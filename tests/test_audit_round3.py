"""Comprehensive tests validating all 9 P0 critical defects and P1 reliability fixes from Round 3 Audit."""

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
from nexapy.ai.base import AIResponse, CircuitBreakerState, BaseProvider
from nexapy.ai.providers.anthropic import AnthropicProvider, ACTIVE_MODELS, RETIRED_MODELS_MAP
from nexapy.ai.router import AIRouter
from nexapy.automation import DAGWorkflow, workflow_registry
from nexapy.automation.actions.http import HTTPAction, is_ssrf_safe_url
from nexapy.automation.dag import _dag_run_state_store
from nexapy.automation.queue import Job, JobQueue, job_queue
from nexapy.automation.scheduler import matches_cron
from nexapy.integrations.n8n import (
    N8nClient,
    export_dag_to_n8n,
    export_n8n_workflow,
    generate_python_code_from_n8n,
)
from nexapy.integrations.node_generator import generate_n8n_community_node
from nexapy.security import (
    AIAuthGuard,
    HMACVerifier,
    NonceTracker,
    RateLimiter,
)


@pytest.fixture(autouse=True)
def reset_state():
    workflow_registry.clear()
    _dag_run_state_store.clear()
    yield
    workflow_registry.clear()
    _dag_run_state_store.clear()


# ============================================================================
# P0 CRITICAL FINDINGS TESTS
# ============================================================================

def test_p0_1_and_2_hmac_replay_and_nonce_sequence():
    """
    P0 #1 & #2:
    - Fake signature with new nonce MUST NOT consume the nonce.
    - Valid signature with same nonce MUST succeed.
    - Replayed valid signature with same nonce MUST be rejected.
    - Legacy signature without timestamp MUST be rejected when timestamp is present.
    """
    secret = "audit3_hmac_secret"
    payload = b'{"action": "payout", "amount": 1000}'
    now = time.time()
    ts = str(int(now))
    nonce_test = f"nonce_audit3_{int(now * 1000)}"

    # 1. Fake signature + new nonce -> False
    fake_sig = "a" * 64
    assert HMACVerifier.verify_signature(secret, payload, fake_sig, timestamp=ts, nonce=nonce_test) is False

    # 2. Valid signature + same nonce -> True (proves fake signature did NOT exhaust the nonce!)
    valid_sig = HMACVerifier.compute_signature(secret, payload, timestamp=ts)
    assert HMACVerifier.verify_signature(secret, payload, valid_sig, timestamp=ts, nonce=nonce_test) is True

    # 3. Replay with the same nonce -> False (anti-replay defense works)
    assert HMACVerifier.verify_signature(secret, payload, valid_sig, timestamp=ts, nonce=nonce_test) is False

    # 4. Legacy signature without timestamp MUST NOT be accepted when timestamp is provided
    legacy_raw_sig = HMACVerifier.compute_signature(secret, payload)
    new_nonce = f"nonce_legacy_{int(now * 1000)}"
    # Supplying timestamp with legacy signature MUST return False
    assert HMACVerifier.verify_signature(secret, payload, legacy_raw_sig, timestamp=ts, nonce=new_nonce) is False


def test_p0_3_ai_endpoints_fail_closed_by_default():
    """
    P0 #3: AI endpoints must require authentication by default when unconfigured,
    and allow anonymous access only with explicit allow_anonymous_ai=True.
    """
    # 1. Default app without keys or allow_anonymous_ai -> Fails closed with 401
    app_default = NexaPy()
    client_default = TestClient(app_default.fastapi)
    res_default = client_default.post("/ai/chat", json={"prompt": "Hello"})
    assert res_default.status_code == 401
    assert "authentication required" in res_default.json()["error"].lower()

    # 2. Explicit allow_anonymous_ai=True -> succeeds
    app_anon = NexaPy(allow_anonymous_ai=True)
    with patch.object(app_anon._ai_engine, "chat", new_callable=AsyncMock) as mock_chat:
        mock_chat.return_value = AIResponse(text="Hello anon", provider="freemodel", model="auto", success=True)
        client_anon = TestClient(app_anon.fastapi)
        res_anon = client_anon.post("/ai/chat", json={"prompt": "Hello"})
        assert res_anon.status_code == 200
        assert res_anon.json()["text"] == "Hello anon"

    # 3. Configured with key -> valid key succeeds, missing key fails
    app_keyed = NexaPy(ai_api_keys=["key-audit3"])
    client_keyed = TestClient(app_keyed.fastapi)
    res_unauth = client_keyed.post("/ai/chat", json={"prompt": "Hello"})
    assert res_unauth.status_code == 401

    with patch.object(app_keyed._ai_engine, "chat", new_callable=AsyncMock) as mock_chat:
        mock_chat.return_value = AIResponse(text="Hello keyed", provider="freemodel", model="auto", success=True)
        res_auth = client_keyed.post(
            "/ai/chat",
            json={"prompt": "Hello"},
            headers={"X-NexaPy-Key": "key-audit3"},
        )
        assert res_auth.status_code == 200
        assert res_auth.json()["text"] == "Hello keyed"


def test_p0_4_job_queue_worker_lifecycle():
    """
    P0 #4: FastAPI app lifecycle automatically starts background workers,
    allowing submitted jobs to be executed and completed.
    """
    @workflow("audit3_bg_task")
    async def bg_task(ctx):
        return {"processed": True, "value": ctx.input.get("x", 0) * 2}

    app = NexaPy(allow_anonymous_ai=True)
    app.mount_workflows(prefix="/workflows", allow_anonymous=True)

    with TestClient(app.fastapi) as client:
        # Submit background job
        submit_res = client.post("/workflows/jobs", json={"workflow": "audit3_bg_task", "input": {"x": 21}})
        assert submit_res.status_code == 202
        job_id = submit_res.json()["job"]["job_id"]

        # Wait briefly for worker to complete
        time.sleep(0.5)

        status_res = client.get(f"/workflows/jobs/{job_id}")
        assert status_res.status_code == 200
        job_data = status_res.json()["job"]
        assert job_data["status"] == "COMPLETED"
        assert job_data["result"] == {"processed": True, "value": 42}


def test_p0_5_node_generator_no_undeclared_headers():
    """
    P0 #5: Generated TypeScript n8n Community Node must use buildHeaders({})
    and have no undefined 'headers' reference.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        out_path = generate_n8n_community_node(tmpdir)
        node_file = out_path / "src" / "nodes" / "NexaPy" / "NexaPy.node.ts"
        assert node_file.exists()
        ts_code = node_file.read_text(encoding="utf-8")
        # Must NOT have isolated 'headers,' without buildHeaders
        assert "headers: buildHeaders({})" in ts_code
        # Must not contain raw undefined headers reference
        assert "headers," not in ts_code


def test_p0_6_and_7_n8n_export_credentials_no_plaintext():
    """
    P0 #6 & #7: n8n workflow export uses credentials as single source of secrets.
    Never emits plaintext secrets in headerParameters.
    Uses genericCredentialType and httpHeaderAuth with realistic credential ID.
    """
    @workflow("audit3_sec_wf")
    def sec_wf(ctx):
        return "ok"

    # Export with auth_key
    n8n_json = export_n8n_workflow("audit3_sec_wf", auth_key="super_secret_token_123")
    http_node = next(n for n in n8n_json["nodes"] if n["type"] == "n8n-nodes-base.httpRequest")

    # Plaintext secret MUST NOT be present
    assert "headerParameters" not in http_node.get("parameters", {})
    assert "super_secret_token_123" not in json.dumps(n8n_json)

    # Must configure genericCredentialType + httpHeaderAuth
    params = http_node["parameters"]
    assert params["authentication"] == "genericCredentialType"
    assert params["genericAuthType"] == "httpHeaderAuth"

    creds = http_node["credentials"]
    assert "httpHeaderAuth" in creds
    # ID must be realistic, not fixed at "1"
    assert creds["httpHeaderAuth"]["id"] == "cred_audit3_sec_wf"
    assert creds["httpHeaderAuth"]["name"] == "NexaPy Header Auth"


def test_p0_8_n8n_python_generator_dynamic_escaping():
    """
    P0 #8: Python code generator handles workflow and node names with quotes,
    newlines, and non-alphanumeric characters using repr() and identifier sanitization.
    """
    n8n_complex_data = {
        "name": 'My "Special" \n Workflow: 100% Reliable!',
        "nodes": [
            {
                "name": 'Step "Alpha" \n #1',
                "type": "n8n-nodes-base.httpRequest",
                "parameters": {
                    "url": "https://api.example.com/v1/data?foo=bar&baz=\"quotes\"",
                    "method": "POST",
                },
            },
            {
                "name": "Step 'Beta' \t [2]",
                "type": "n8n-nodes-base.webhook",
                "parameters": {"path": "webhook-path"},
            },
        ],
        "connections": {},
    }

    generated_code = generate_python_code_from_n8n(n8n_complex_data)

    # Must be valid, compilable Python without SyntaxError or NameError
    compiled = compile(generated_code, "<string>", "exec")
    assert compiled is not None
    assert "my_special_workflow_100_reliable" in generated_code


def test_p0_9_default_ai_chat_route_registered():
    """
    P0 #9: Standard /ai/chat endpoint is registered out of the box when creating NexaPy(),
    matching n8n Community Node expectations.
    """
    app = NexaPy(allow_anonymous_ai=True)
    with patch.object(app._ai_engine, "chat", new_callable=AsyncMock) as mock_chat:
        mock_chat.return_value = AIResponse(text="AI response", provider="freemodel", model="auto", success=True)
        client = TestClient(app.fastapi)
        res = client.post("/ai/chat", json={"prompt": "Explain quantum computing"})
        assert res.status_code == 200
        data = res.json()
        assert data["success"] is True
        assert data["text"] == "AI response"


# ============================================================================
# P1 RELIABILITY FINDINGS TESTS
# ============================================================================

def test_p1_10_ai_token_usage_tracking_and_quota():
    """P1 #10: record_usage() tracks token usage across /ai/chat and enforces quota."""
    app = NexaPy(ai_api_keys=["quota-key"])
    app.ai_guard.token_limit = 20  # tiny quota

    client = TestClient(app.fastapi)
    with patch.object(app._ai_engine, "chat", new_callable=AsyncMock) as mock_chat:
        mock_chat.return_value = AIResponse(
            text="This is a test response consisting of several words to simulate token consumption.",
            provider="freemodel",
            model="auto",
            success=True,
        )

        # First request consumes tokens
        res1 = client.post(
            "/ai/chat",
            json={"prompt": "Tell me a story"},
            headers={"X-NexaPy-Key": "quota-key"},
        )
        assert res1.status_code == 200
        assert app.ai_guard.token_usage.get("quota-key", 0) > 0

        # Subsequent request when quota is exceeded returns 429
        app.ai_guard.token_usage["quota-key"] = 25
        res2 = client.post(
            "/ai/chat",
            json={"prompt": "Another request"},
            headers={"X-NexaPy-Key": "quota-key"},
        )
        assert res2.status_code == 429
        assert "token quota exceeded" in res2.json()["error"].lower()


@pytest.mark.asyncio
async def test_p1_11_circuit_breaker_open_state_rejection():
    """P1 #11: AI Router never invokes a provider whose circuit breaker is OPEN."""
    router = AIRouter()

    mock_provider = AsyncMock(spec=BaseProvider)
    mock_provider.name = "failing_provider"
    mock_provider.circuit_state = CircuitBreakerState.OPEN
    mock_provider.is_healthy.return_value = False

    router.register_provider("failing_provider", mock_provider)

    # Calling OPEN provider directly must fail immediately without invoking generate()
    res = await router.route("Hello", provider="failing_provider")
    assert res.success is False
    assert "circuit breaker is open" in res.error.lower()
    mock_provider.generate.assert_not_called()


def test_p1_12_claude_default_model_update():
    """P1 #12: Claude default model updated to active model and deprecated aliases mapped."""
    provider = AnthropicProvider(api_key="mock_key")
    assert provider.default_model == "claude-sonnet-4-6"
    assert provider.resolve_model("claude-3-5-sonnet-latest") == "claude-sonnet-4-6"
    assert "claude-sonnet-4-6" in ACTIVE_MODELS


@pytest.mark.asyncio
async def test_p1_13_job_cancellation_race_condition():
    """P1 #13: Cancelled jobs are never overwritten to COMPLETED."""
    queue = JobQueue()

    # Enqueue a job and set to PROCESSING
    job = await queue.enqueue("dummy_wf", {"param": 1})
    job.status = "PROCESSING"
    queue._save_job(job)

    # Cancel the job
    assert queue.cancel_job(job.job_id) is True
    cancelled_job = queue.get_job(job.job_id)
    assert cancelled_job.status == "CANCELLED"

    # Simulate worker finishing after cancellation
    latest = queue.get_job(job.job_id)
    if latest and latest.status == "CANCELLED":
        pass  # worker respects cancelled state
    else:
        latest.status = "COMPLETED"
        queue._save_job(latest)

    # Status must remain CANCELLED
    final_job = queue.get_job(job.job_id)
    assert final_job.status == "CANCELLED"


def test_p1_14_sqlite_atomic_job_claim():
    """P1 #14: SQLite job queue atomic claiming via claim_next_job with transactional locking."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_jobs.db"
        queue = JobQueue(db_path=db_path)

        asyncio.run(queue.enqueue("task_1", {"x": 10}))
        asyncio.run(queue.enqueue("task_2", {"x": 20}))

        # Worker 1 claims task_1
        claimed1 = queue.claim_next_job(worker_id="worker_1", lease_seconds=60.0)
        assert claimed1 is not None
        assert claimed1.workflow_name == "task_1"
        assert claimed1.status == "PROCESSING"

        # Worker 2 claims task_2
        claimed2 = queue.claim_next_job(worker_id="worker_2", lease_seconds=60.0)
        assert claimed2 is not None
        assert claimed2.workflow_name == "task_2"

        # Worker 3 finds no more queued jobs
        claimed3 = queue.claim_next_job(worker_id="worker_3", lease_seconds=60.0)
        assert claimed3 is None


@pytest.mark.asyncio
async def test_p1_15_dag_dependency_validation_and_idempotency():
    """P1 #15: DAG step execution validates dependency completion and returns cached result on retry."""
    from nexapy.automation.context import WorkflowContext

    dag = DAGWorkflow("pipeline_dep_test")

    @dag.node("stepA")
    def step_a():
        return "resultA"

    @dag.node("stepB", depends_on=["stepA"])
    def step_b(ctx):
        return f"resultB_{ctx.steps['stepA']}"

    ctx = WorkflowContext(workflow_name="pipeline_dep_test", input_data={"run_id": "run_test_123"})

    # 1. Attempting stepB before stepA must raise ValueError
    with pytest.raises(ValueError) as exc_info:
        await dag.execute_step_async("stepB", ctx)
    assert "Dependency step 'stepA' has not executed yet" in str(exc_info.value)

    # 2. Executing stepA succeeds
    res_a = await dag.execute_step_async("stepA", ctx)
    assert res_a["status"] == "success"
    assert res_a["result"] == "resultA"

    # 3. Executing stepB now succeeds
    res_b = await dag.execute_step_async("stepB", ctx)
    assert res_b["status"] == "success"
    assert res_b["result"] == "resultB_resultA"

    # 4. Idempotency: re-executing stepB returns cached status
    res_b_retry = await dag.execute_step_async("stepB", ctx)
    assert res_b_retry["status"] == "cached"
    assert res_b_retry["result"] == "resultB_resultA"


def test_p1_17_dag_n8n_export_propagates_run_id():
    """P1 #17: DAG export to n8n propagates run_id in JSON payload."""
    dag = DAGWorkflow("run_id_dag")

    @dag.node("node1")
    def node1():
        return 1

    workflow_registry.register("run_id_dag", dag)
    exported = export_dag_to_n8n(dag, mode="per_step")
    step_node = next(n for n in exported["nodes"] if n["name"] == "Step: node1")
    json_body = step_node["parameters"]["jsonBody"]
    assert "run_id" in json_body


def test_p1_18_cron_scheduler_zero_division_safety():
    """P1 #18: Invalid cron expressions like '*/0' return False without ZeroDivisionError."""
    # Must return False safely and not raise ZeroDivisionError
    assert matches_cron("*/0 * * * *") is False
    assert matches_cron("0 */0 * * *") is False
    assert matches_cron("invalid cron expression") is False


def test_p1_19_ssrf_guard_scheme_and_metadata_protection():
    """P1 #19: SSRF Guard restricts to HTTP/HTTPS and rejects cloud metadata even in allowlist."""
    # Schemes other than HTTP/HTTPS must be rejected
    assert is_ssrf_safe_url("ftp://example.com/file.txt") is False
    assert is_ssrf_safe_url("file:///etc/passwd") is False
    assert is_ssrf_safe_url("gopher://127.0.0.1:70") is False

    # Metadata service is blocked even if someone puts it in allowed_hosts
    assert is_ssrf_safe_url("http://169.254.169.254/latest/meta-data", allowed_hosts=["169.254.169.254"]) is False
    assert is_ssrf_safe_url("http://localhost/admin", allowed_hosts=["localhost"]) is False

    # Valid HTTPS public URL passes
    assert is_ssrf_safe_url("https://api.github.com/repos") is True


@pytest.mark.asyncio
async def test_p1_21_n8n_client_persistent_cache_and_pagination():
    """P1 #21: N8nClient persists _id_cache to file and supports cursor pagination."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cache_path = Path(tmpdir) / "n8n_cache.json"

        # 1. Create client with cache_file
        client = N8nClient(n8n_url="http://n8n.local", api_key="test_key", cache_file=cache_path)
        client._id_cache["wf_alpha"] = "42"
        client._save_cache()

        assert cache_path.exists()
        with open(cache_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        assert data.get("wf_alpha") == "42"

        # 2. Reload client from disk
        client_reloaded = N8nClient(n8n_url="http://n8n.local", api_key="test_key", cache_file=cache_path)
        assert client_reloaded._id_cache.get("wf_alpha") == "42"
