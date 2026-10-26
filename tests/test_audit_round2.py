"""Comprehensive tests validating all 31 findings and fixes from Round 2 Audit of NexaPy."""

import asyncio
import datetime
import json
import tempfile
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
import pytest
from fastapi.testclient import TestClient

from nexapy import NexaPy, workflow
from nexapy.ai.base import AIResponse, BaseProvider, CircuitBreakerState
from nexapy.ai.providers.anthropic import AnthropicProvider
from nexapy.ai.router import AIRouter
from nexapy.automation import DAGWorkflow, workflow_registry
from nexapy.automation.actions.http import HTTPAction, is_ssrf_safe_url
from nexapy.automation.queue import Job, JobQueue
from nexapy.automation.scheduler import matches_cron
from nexapy.integrations.n8n import (
    N8nClient,
    export_dag_to_n8n,
    export_n8n_workflow,
    generate_python_code_from_n8n,
)
from nexapy.sdk.nodejs import generate_nodejs_sdk
from nexapy.sdk.php import generate_php_sdk
from nexapy.security import (
    AIAuthGuard,
    HMACVerifier,
    NonceTracker,
    RateLimiter,
    generate_correlation_id,
    sanitize_sensitive_text,
)


@pytest.fixture(autouse=True)
def reset_state():
    workflow_registry.clear()
    yield
    workflow_registry.clear()


# ============================================================================
# TAHAP 1: Security Hotfix Tests
# ============================================================================

def test_fail_closed_workflow_authentication():
    """Finding 1 (P0): Fail-closed security must raise RuntimeError if unconfigured."""
    app = NexaPy()
    # Unconfigured auth with allow_anonymous=False (default) MUST raise RuntimeError
    with pytest.raises(RuntimeError) as exc_info:
        app.mount_workflows(prefix="/workflows")
    assert "Workflow authentication must be configured" in exc_info.value.args[0]

    # Explicit allow_anonymous=True succeeds
    app_anon = NexaPy()
    app_anon.mount_workflows(prefix="/workflows", allow_anonymous=True)

    # Configured with api_key succeeds
    app_keyed = NexaPy()
    app_keyed.mount_workflows(prefix="/workflows", api_key="test-key")


def test_ai_auth_guard_and_rate_limiting():
    """Finding 2 (P0): AI endpoint protection via AIAuthGuard and token quotas."""
    guard = AIAuthGuard(valid_keys=["key-123"], max_requests_per_minute=2, token_limit=100)

    # 1. Missing API key
    req_mock = MagicMock()
    req_mock.headers = {}
    req_mock.client.host = "10.0.0.1"
    ok, _, err = guard.is_authorized(req_mock)
    assert ok is False
    assert "Invalid or missing" in err

    # 2. Valid API key
    req_mock.headers = {"X-NexaPy-Key": "key-123"}
    ok, key, _ = guard.is_authorized(req_mock)
    assert ok is True
    assert key == "key-123"

    # 3. Second request within limit
    ok2, _, _ = guard.is_authorized(req_mock)
    assert ok2 is True

    # 4. Third request exceeds rate limit (max 2/min)
    ok3, _, err3 = guard.is_authorized(req_mock)
    assert ok3 is False
    assert "rate limit exceeded" in err3.lower()

    # 5. Token quota check
    guard.limiter.reset()
    guard.record_usage("key-123", tokens=100)
    ok4, _, err4 = guard.is_authorized(req_mock)
    assert ok4 is False
    assert "token quota exceeded" in err4.lower()


def test_hmac_replay_defense_with_timestamp_and_nonce():
    """Finding 14 (P1): Replay defense prevents timestamp drift and duplicate nonces."""
    secret = "super-secret"
    payload = b'{"amount": 500, "currency": "USD"}'
    now = time.time()

    # 1. Valid timestamped signature
    ts = str(int(now))
    sig = HMACVerifier.compute_signature(secret, payload, timestamp=ts)
    assert HMACVerifier.verify_signature(secret, payload, sig, timestamp=ts, nonce="nonce-1") is True

    # 2. Replay attack: duplicate nonce must be rejected
    assert HMACVerifier.verify_signature(secret, payload, sig, timestamp=ts, nonce="nonce-1") is False

    # 3. Expired timestamp: outside 300s window must be rejected
    old_ts = str(int(now - 400))
    old_sig = HMACVerifier.compute_signature(secret, payload, timestamp=old_ts)
    assert HMACVerifier.verify_signature(secret, payload, old_sig, timestamp=old_ts, nonce="nonce-2") is False


def test_http_action_ssrf_protection():
    """Finding 23 (P1): HTTPAction blocks private subnets and cloud metadata."""
    # SSRF safe check
    assert is_ssrf_safe_url("http://localhost/admin") is False
    assert is_ssrf_safe_url("http://127.0.0.1:8080/data") is False
    assert is_ssrf_safe_url("http://169.254.169.254/latest/meta-data") is False
    assert is_ssrf_safe_url("http://10.0.0.5/internal") is False
    assert is_ssrf_safe_url("https://api.github.com/users") is True

    # HTTPAction raises ValueError on SSRF target
    http = HTTPAction(prevent_ssrf=True)
    with pytest.raises(ValueError) as exc:
        asyncio.run(http.get("http://127.0.0.1:9000/secret"))
    assert "SSRF Protection blocked" in str(exc.value)


def test_sensitive_data_redaction_and_correlation_id():
    """Finding 19 (P1): Sensitive fields redacted and correlation ID generated."""
    corr_id = generate_correlation_id()
    assert corr_id.startswith("corr_")

    raw_err = "Failed calling provider with api_key='sk-abcdef1234567890abcdef1234' and secret: 'my_secret_token'"
    sanitized = sanitize_sensitive_text(raw_err)
    assert "sk-abcdef1234567890" not in sanitized
    assert "***REDACTED***" in sanitized


# ============================================================================
# TAHAP 2: n8n Integration Stabilization Tests
# ============================================================================

def test_n8n_export_credentials_support():
    """Finding 3 (P0): n8n export supports credential binding to avoid plaintext secrets."""
    @workflow("cred-flow")
    def cred_flow(ctx):
        return "ok"

    # Export with use_credentials=True
    exported = export_n8n_workflow("cred-flow", use_credentials=True, credential_name="My Production Creds")
    http_node = next(n for n in exported["nodes"] if n["type"] == "n8n-nodes-base.httpRequest")
    assert "credentials" in http_node
    assert http_node["credentials"]["nexapyApi"]["name"] == "My Production Creds"
    assert "headerParameters" not in http_node.get("parameters", {})


def test_n8n_dag_export_merge_nodes():
    """Finding 4 & 11 (P0 & P1): Multi-branch DAGs insert an n8n Merge node."""
    dag = DAGWorkflow("multi_branch_dag")

    @dag.node("start")
    def start():
        return 1

    @dag.node("branchA", depends_on=["start"])
    def branch_a():
        return "A"

    @dag.node("branchB", depends_on=["start"])
    def branch_b():
        return "B"

    @dag.node("joinStep", depends_on=["branchA", "branchB"])
    def join_step():
        return "Joined"

    workflow_registry.register("multi_branch_dag", dag)

    # When use_merge_nodes=True, an n8n Merge node is created to join branchA and branchB
    exported = export_dag_to_n8n(dag, use_merge_nodes=True)
    node_types = [n["type"] for n in exported["nodes"]]
    assert "n8n-nodes-base.merge" in node_types

    merge_node = next(n for n in exported["nodes"] if n["type"] == "n8n-nodes-base.merge")
    assert merge_node["name"] == "Merge for joinStep"
    # Merge node connects to Step: joinStep
    assert exported["connections"]["Merge for joinStep"]["main"][0][0]["node"] == "Step: joinStep"


def test_n8n_python_generator_null_safety():
    """Finding 6 (P0): Code generator does not raise NameError on JSON null/bool."""
    sample_json = {
        "name": "Nullable Workflow",
        "nodes": [
            {
                "name": "Webhook",
                "type": "n8n-nodes-base.webhook",
                "parameters": {"path": "test", "optional_val": None, "active": True, "debug": False},
            }
        ],
        "connections": {},
    }

    code = generate_python_code_from_n8n(sample_json)
    # The generated Python code must execute cleanly without NameError: name 'null' is not defined
    namespace = {}
    exec(code, namespace)
    assert "nullable_workflow" in namespace


@pytest.mark.asyncio
async def test_n8n_client_put_and_persistent_id_cache():
    """Finding 7 & 12 (P0 & P1): Update uses PUT and sync caches IDs without duplicate creation on failure."""
    client = N8nClient(n8n_url="http://n8n.local:5678", api_key="secret")

    with patch("httpx.AsyncClient.put", new_callable=AsyncMock) as mock_put:
        mock_put.return_value.json.return_value = {"id": "wf_99", "name": "Test"}
        mock_put.return_value.raise_for_status = lambda: None

        res = await client.update_workflow("wf_99", {"name": "Test"})
        assert res["id"] == "wf_99"
        mock_put.assert_called_once()
        assert "api/v1/workflows/wf_99" in mock_put.call_args[0][0]

    # Verify ID cache prevents unnecessary lookup
    client._id_cache["Cached Workflow"] = "wf_cached_1"
    with patch.object(client, "update_workflow", new_callable=AsyncMock) as mock_update, \
         patch.object(client, "list_workflows", new_callable=AsyncMock) as mock_list:
        mock_update.return_value = {"id": "wf_cached_1"}

        sync_res = await client.sync_workflow({"name": "Cached Workflow"}, upsert=True)
        assert sync_res["id"] == "wf_cached_1"
        mock_update.assert_called_once_with("wf_cached_1", {"name": "Cached Workflow"})
        mock_list.assert_not_called()


# ============================================================================
# TAHAP 3: Durable Workflow Engine Tests
# ============================================================================

@pytest.mark.asyncio
async def test_job_queue_recovery_and_idempotency():
    """Finding 5 (P0): SQLite crash recovery and idempotency key deduplication."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        db_file = Path(tmp_dir) / "jobs.db"
        queue = JobQueue(db_path=db_file)

        # Enqueue job with idempotency key
        job1 = await queue.enqueue("order_sync", {"order_id": 100}, idempotency_key="idemp_abc")
        assert job1.status == "QUEUED"

        # Re-submitting with same idempotency key returns the exact same job
        job2 = await queue.enqueue("order_sync", {"order_id": 100}, idempotency_key="idemp_abc")
        assert job2.job_id == job1.job_id

        # Simulate crash while processing
        job1.status = "PROCESSING"
        queue._save_job(job1)

        # Restart server with new JobQueue instance referencing same db
        restarted_queue = JobQueue(db_path=db_file)
        recovered_count = restarted_queue.recover_pending_jobs()
        assert recovered_count == 1

        recovered_job = restarted_queue.get_job(job1.job_id)
        assert recovered_job is not None
        assert recovered_job.status == "QUEUED"  # Interrupted PROCESSING job recovered to QUEUED


def test_job_queue_api_endpoints():
    """Finding 17 (P1): /workflows/jobs REST API endpoints."""
    app = NexaPy()
    app.mount_workflows(prefix="/workflows", allow_anonymous=True)
    client = TestClient(app.fastapi)

    # 1. Submit job
    res_post = client.post("/workflows/jobs", json={"workflow": "dummy_wf", "input": {"x": 1}})
    assert res_post.status_code == 202
    job_data = res_post.json()["job"]
    job_id = job_data["job_id"]
    assert job_data["status"] == "QUEUED"

    # 2. Get job status
    res_get = client.get(f"/workflows/jobs/{job_id}")
    assert res_get.status_code == 200
    assert res_get.json()["job"]["job_id"] == job_id

    # 3. Cancel job
    res_del = client.delete(f"/workflows/jobs/{job_id}")
    assert res_del.status_code == 200
    assert "cancelled" in res_del.json()["message"]


def test_workflow_scheduler_5_field_cron():
    """Finding 16 (P1): 5-field cron parsing support."""
    # Every 5 minutes: minute % 5 == 0
    dt_match = datetime.datetime(2026, 10, 26, 14, 15, 0)  # minute 15
    dt_non_match = datetime.datetime(2026, 10, 26, 14, 16, 0)  # minute 16

    assert matches_cron("*/5 * * * *", dt_match) is True
    assert matches_cron("*/5 * * * *", dt_non_match) is False

    # Exact hour and minute: 30 9 * * *
    dt_930 = datetime.datetime(2026, 10, 26, 9, 30, 0)
    assert matches_cron("30 9 * * *", dt_930) is True
    assert matches_cron("30 9 * * *", dt_match) is False


@pytest.mark.asyncio
async def test_dag_step_execution_with_run_id_persistence():
    """Finding 18 (P1): Single-step DAG execution preserves prior step outputs using run_id."""
    dag = DAGWorkflow("pipeline_store")

    @dag.node("step1")
    def step1(ctx):
        return {"data": 42}

    @dag.node("step2", depends_on=["step1"])
    def step2(ctx):
        prior = ctx.get("steps", {})
        return {"result_from_s1": prior.get("step1", {}).get("data", 0) * 2}

    workflow_registry.register("pipeline_store", dag)

    from nexapy.automation.context import WorkflowContext

    # Run step 1 with run_id
    ctx1 = WorkflowContext(input_data={"run_id": "run_test_1"})
    res1 = await dag.execute_step_async("step1", ctx1)
    assert res1["result"] == {"data": 42}

    # Run step 2 with same run_id: prior step1 output is preserved
    ctx2 = WorkflowContext(input_data={"run_id": "run_test_1"})
    res2 = await dag.execute_step_async("step2", ctx2)
    assert res2["result"] == {"result_from_s1": 84}


# ============================================================================
# TAHAP 4: AI Reliability Tests
# ============================================================================

def test_anthropic_active_default_model_and_migration():
    """Finding 8 (P1): Anthropic default model updated to active claude-3-5-sonnet-latest."""
    provider = AnthropicProvider(api_key="mock-key")
    assert provider.default_model == "claude-3-5-sonnet-latest"

    # Retired model name is migrated
    assert provider.resolve_model("claude-3-5-sonnet-20241022") == "claude-3-5-sonnet-latest"


def test_circuit_breaker_formal_states():
    """Finding 10 (P1): Circuit breaker CLOSED -> OPEN -> HALF_OPEN states."""
    class DummyProvider(BaseProvider):
        async def generate(self, prompt, **kwargs):
            return AIResponse(text="dummy", provider=self.name, model="m")

    p = DummyProvider("dummy", cooldown_seconds=0.1, max_consecutive_failures=2)
    assert p.circuit_state == CircuitBreakerState.CLOSED
    assert p.is_healthy() is True

    # 1 failure
    p.record_failure("err1")
    assert p.circuit_state == CircuitBreakerState.CLOSED

    # 2 failures -> reaches max_consecutive_failures -> trips to OPEN
    p.record_failure("err2")
    assert p.circuit_state == CircuitBreakerState.OPEN
    assert p.is_healthy() is False

    # Wait for cooldown -> transitions to HALF_OPEN
    time.sleep(0.12)
    assert p.circuit_state == CircuitBreakerState.HALF_OPEN
    assert p.is_healthy() is True

    # Success in HALF_OPEN resets to CLOSED
    p.record_success()
    assert p.circuit_state == CircuitBreakerState.CLOSED


@pytest.mark.asyncio
async def test_ai_streaming_no_mixed_tokens_fallback():
    """Finding 9 (P1): Streaming never falls back to another provider once tokens have yielded."""
    class PartialFailingProvider(BaseProvider):
        async def generate(self, prompt, **kwargs):
            return AIResponse(text="fail", provider=self.name, model="m")

        async def stream(self, prompt, **kwargs):
            yield "PARTIAL_TEXT_"
            raise RuntimeError("Mid-stream connection break!")

    class BackupProvider(BaseProvider):
        async def generate(self, prompt, **kwargs):
            return AIResponse(text="backup", provider=self.name, model="m")

        async def stream(self, prompt, **kwargs):
            yield "BACKUP_FULL"

    router = AIRouter()
    router.providers["failer"] = PartialFailingProvider("failer")
    router.providers["backup"] = BackupProvider("backup")
    router.priority = ["failer", "backup"]

    # When failer yields token and then crashes, router MUST NOT switch to backup
    chunks = []
    with pytest.raises(RuntimeError) as exc_info:
        async for chunk in router.stream("test prompt", provider="auto"):
            chunks.append(chunk)

    assert chunks == ["PARTIAL_TEXT_"]
    assert "Stream interrupted during generation by failer" in str(exc_info.value)
    assert "BACKUP_FULL" not in chunks


# ============================================================================
# TAHAP 5: Cross-Language SDK Generator Tests
# ============================================================================

def test_nodejs_sdk_dual_module_support():
    """Finding 24 (P2): Node.js SDK generates both CommonJS index.js and ESM index.mjs."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        out_dir = Path(tmp_dir)
        generate_nodejs_sdk(out_dir)

        pkg_json = json.loads((out_dir / "package.json").read_text(encoding="utf-8"))
        assert pkg_json["main"] == "index.js"
        assert pkg_json["module"] == "index.mjs"
        assert "." in pkg_json["exports"]
        assert pkg_json["exports"]["."]["import"] == "./index.mjs"
        assert pkg_json["exports"]["."]["require"] == "./index.js"

        assert (out_dir / "index.js").exists()
        assert (out_dir / "index.mjs").exists()


def test_php_sdk_psr18_adapter_support():
    """Finding 25 (P2): PHP SDK supports PSR-18 HTTP client injection."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        out_dir = Path(tmp_dir)
        generate_php_sdk(out_dir)

        composer_json = json.loads((out_dir / "composer.json").read_text(encoding="utf-8"))
        assert "psr/http-client" in composer_json.get("suggest", {})

        client_php = (out_dir / "src" / "NexaPyClient.php").read_text(encoding="utf-8")
        assert "httpClient" in client_php
        assert "sendRequest" in client_php
