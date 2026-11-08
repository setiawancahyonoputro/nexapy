"""
Audit Round 8 Comprehensive Regression Tests for NexaPy Framework.
Validates all findings from Audit Teknis Profesional NexaPy V8:
- P0-01: Tenant identity bound to credential, rejects tenant spoofing with 403 TENANT_FORBIDDEN
- P0-02: Strict job ownership check without 'default' tenant bypass (returns 404)
- P0-03: Queue idempotency scoped per tenant; payload hash conflict yields ValueError / 409
- P0-04: n8n export credential options (httpHeaderAuth) and sync safe defaults
- P1-01: DAGStateStore.save_step with claim_token=None cannot overwrite existing completed step
- P1-02: WorkflowScheduler._acquire_execution_lock fails closed on database error (returns False)
- P1-03: n8n rule conversion for seconds and >60m intervals; strict ValueError on invalid cron
- P1-04: n8n Webhook trigger node sets responseMode at parameters root level
- P1-05: Per-step DAG export preserves payload expression ($json.payload || $json)
- P1-07: nexapy init creates secure defaults (allow_anonymous_ai=False, random token)
- P1-09b: /ai/chat route authenticates valid HMAC signatures
"""

import hashlib
import hmac
import os
import sqlite3
import tempfile
import time
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from nexapy import NexaPy
from nexapy.ai import AIResponse
from nexapy.automation.dag import (
    DAGStateStore,
    DAGWorkflow,
    get_default_dag_state_store,
    set_default_dag_state_store,
)
from nexapy.automation import workflow
from nexapy.automation.queue import Job, JobQueue
from nexapy.automation.registry import workflow_registry
from nexapy.automation.scheduler import WorkflowScheduler
from nexapy.cli import init
from nexapy.integrations.n8n import (
    convert_schedule_to_n8n_cron,
    convert_schedule_to_n8n_rule,
    export_dag_to_n8n,
    export_n8n_workflow,
    validate_n8n_workflow,
)
from nexapy.security import AuthenticatedPrincipal


@pytest.fixture(autouse=True)
def clean_workflow_registry():
    workflow_registry.clear()
    yield
    workflow_registry.clear()


# -------------------------------------------------------------------------
# P0-01: Tenant Identity Bound to Credential
# -------------------------------------------------------------------------
def test_v8_p0_01_tenant_credential_binding_and_rejection():
    """Verify that an API key bound to tenant_a rejects X-Tenant-ID: tenant_b with HTTP 403."""
    app = NexaPy()

    @workflow("task_alpha")
    def task_alpha(ctx):
        return {"done": True, "tenant": ctx.security_context.get("tenant_id")}

    app.mount_workflows(
        prefix="/workflows",
        tenant_keys={"key_alpha": "tenant_alpha"},
    )

    client = TestClient(app.fastapi)

    # 1. Valid key with matching tenant
    res_valid = client.post(
        "/workflows/task_alpha",
        json={"data": 1},
        headers={"X-NexaPy-Key": "key_alpha", "X-Tenant-ID": "tenant_alpha"},
    )
    assert res_valid.status_code == 200
    assert res_valid.json()["result"]["tenant"] == "tenant_alpha"

    # 2. Valid key without header (inherits bound tenant)
    res_default = client.post(
        "/workflows/task_alpha",
        json={"data": 1},
        headers={"X-NexaPy-Key": "key_alpha"},
    )
    assert res_default.status_code == 200
    assert res_default.json()["result"]["tenant"] == "tenant_alpha"

    # 3. Valid key with spoofed/unauthorized tenant header -> 403 TENANT_FORBIDDEN
    res_spoof = client.post(
        "/workflows/task_alpha",
        json={"data": 1},
        headers={"X-NexaPy-Key": "key_alpha", "X-Tenant-ID": "tenant_beta"},
    )
    assert res_spoof.status_code == 403
    assert "TENANT_FORBIDDEN" in res_spoof.text


# -------------------------------------------------------------------------
# P0-02: Strict Job Ownership Verification (No 'default' bypass)
# -------------------------------------------------------------------------
def test_v8_p0_02_job_ownership_no_default_bypass():
    """Verify that a job belonging to tenant_a cannot be read or cancelled with X-Tenant-ID: default."""
    app = NexaPy()

    @workflow("task_owned")
    def task_owned(ctx):
        return {"status": "ok"}

    app.mount_workflows(prefix="/workflows", api_key="secret123")
    client = TestClient(app.fastapi)

    # Submit job as tenant_alpha
    sub_res = client.post(
        "/workflows/jobs",
        json={"workflow_name": "task_owned", "input_data": {"val": 1}},
        headers={"X-NexaPy-Key": "secret123", "X-Tenant-ID": "tenant_alpha"},
    )
    assert sub_res.status_code == 202
    job_id = sub_res.json()["job_id"]

    # Read status as tenant_alpha -> 200
    res_alpha = client.get(
        f"/workflows/jobs/{job_id}",
        headers={"X-NexaPy-Key": "secret123", "X-Tenant-ID": "tenant_alpha"},
    )
    assert res_alpha.status_code == 200
    assert res_alpha.json()["job_id"] == job_id

    # Read status with 'default' tenant -> MUST return 404 (not allowed to view another tenant's job)
    res_default = client.get(
        f"/workflows/jobs/{job_id}",
        headers={"X-NexaPy-Key": "secret123", "X-Tenant-ID": "default"},
    )
    assert res_default.status_code == 404

    # Cancel with 'default' tenant -> MUST return 404
    res_cancel = client.post(
        f"/workflows/jobs/{job_id}/cancel",
        headers={"X-NexaPy-Key": "secret123", "X-Tenant-ID": "default"},
    )
    assert res_cancel.status_code == 404


# -------------------------------------------------------------------------
# P0-03: Queue Idempotency Scoped per Tenant & Payload Conflict
# -------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_v8_p0_03_queue_idempotency_scoped_and_payload_conflict():
    """Verify that idempotency is isolated across tenants and detects payload conflicts for the same tenant."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "idempotency.db")
        q = JobQueue(db_path=db_path)

        # 1. Tenant A enqueues with key "order-100"
        job_a = await q.enqueue("process_order", {"amount": 50}, idempotency_key="order-100", tenant_id="tenant_a")
        assert job_a.job_id is not None
        assert job_a.payload_hash is not None

        # 2. Tenant B enqueues with identical key "order-100" -> gets distinct job
        job_b = await q.enqueue("process_order", {"amount": 75}, idempotency_key="order-100", tenant_id="tenant_b")
        assert job_b.job_id != job_a.job_id
        assert job_b.tenant_id == "tenant_b"

        # 3. Tenant A re-submits exact same payload with same key -> returns existing job (idempotent)
        job_a_dup = await q.enqueue("process_order", {"amount": 50}, idempotency_key="order-100", tenant_id="tenant_a")
        assert job_a_dup.job_id == job_a.job_id

        # 4. Tenant A re-submits DIFFERENT payload with same key -> raises ValueError
        with pytest.raises(ValueError) as excinfo:
            await q.enqueue("process_order", {"amount": 999}, idempotency_key="order-100", tenant_id="tenant_a")
        assert "Idempotency conflict" in str(excinfo.value)


def test_v8_p0_03_http_jobs_idempotency_conflict_returns_409():
    """Verify that submitting a conflicting payload for an existing idempotency key returns HTTP 409."""
    app = NexaPy()

    @workflow("billing_job")
    def billing_job(ctx):
        return {"billed": True}

    app.mount_workflows(prefix="/workflows", api_key="secret_bill")
    client = TestClient(app.fastapi)

    # Initial submit
    res1 = client.post(
        "/workflows/jobs",
        json={"workflow_name": "billing_job", "input_data": {"inv": 1}, "idempotency_key": "inv-2026-01"},
        headers={"X-NexaPy-Key": "secret_bill", "X-Tenant-ID": "tenant_corp"},
    )
    assert res1.status_code == 202

    # Second submit with different payload -> 409 Conflict
    res2 = client.post(
        "/workflows/jobs",
        json={"workflow_name": "billing_job", "input_data": {"inv": 999}, "idempotency_key": "inv-2026-01"},
        headers={"X-NexaPy-Key": "secret_bill", "X-Tenant-ID": "tenant_corp"},
    )
    assert res2.status_code == 409
    assert "IDEMPOTENCY_CONFLICT" in res2.text


# -------------------------------------------------------------------------
# P1-01: DAGStateStore Cannot Overwrite Completed Step Without Claim
# -------------------------------------------------------------------------
def test_v8_p1_01_save_step_no_unclaimed_overwrite():
    """Verify that save_step with claim_token=None returns False and cannot overwrite existing step."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "dag_overwrite.db")
        store = DAGStateStore(db_path=db_path)

        # Worker claims and completes a step
        status, _, token = store.claim_or_get_step("t1", "wf_test", "run_1", "step_x")
        assert status == "CLAIMED"
        assert token is not None

        saved = store.save_step("t1", "wf_test", "run_1", "step_x", {"result": "original"}, claim_token=token)
        assert saved is True

        # Now an unauthorized caller attempts to overwrite with claim_token=None
        res = store.save_step("t1", "wf_test", "run_1", "step_x", {"result": "overwritten"}, claim_token=None)
        assert res is False

        # Verify state is unchanged
        row = store.get_step_status("t1", "wf_test", "run_1", "step_x")
        assert row["status"] == "success"
        assert row["result"] == {"result": "original"}


# -------------------------------------------------------------------------
# P1-02: Scheduler Fail-Closed Lock
# -------------------------------------------------------------------------
def test_v8_p1_02_scheduler_lock_fails_closed():
    """Verify that database errors in _acquire_execution_lock fail closed (return False, no uncaught exception)."""
    sched = WorkflowScheduler()

    # Set an invalid database path that cannot be created/opened
    sched.db_path = "invalid_dir/non_existent_subdir/locked.db"
    lock_result = sched._acquire_execution_lock(
        workflow_name="test_job",
        execution_key="key_1",
    )
    assert lock_result is False

    # Mock sqlite3.connect to raise OperationalError
    sched.db_path = ":memory:"
    with patch("sqlite3.connect", side_effect=sqlite3.OperationalError("disk I/O error")):
        res_mock = sched._acquire_execution_lock(
            workflow_name="test_job_mock",
            execution_key="key_2",
        )
        assert res_mock is False


# -------------------------------------------------------------------------
# P1-03: n8n Schedule Rules and Interval Conversions
# -------------------------------------------------------------------------
def test_v8_p1_03_n8n_interval_conversions():
    """Verify native n8n interval rules for seconds and >60m intervals, and strict rejection in cron."""
    # Native rule generator
    rule_30s = convert_schedule_to_n8n_rule("30s")
    assert rule_30s["interval"][0]["secondsInterval"] == 30

    rule_61m = convert_schedule_to_n8n_rule("61m")
    assert rule_61m["interval"][0]["minutesInterval"] == 61

    rule_2h = convert_schedule_to_n8n_rule("2h")
    assert rule_2h["interval"][0]["hoursInterval"] == 2

    # Strict cron rejects sub-minute and >60m intervals
    with pytest.raises(ValueError):
        convert_schedule_to_n8n_cron("30s")

    with pytest.raises(ValueError):
        convert_schedule_to_n8n_cron("61m")

    # Standard valid interval
    assert convert_schedule_to_n8n_cron("15m") == "*/15 * * * *"


# -------------------------------------------------------------------------
# P1-04: n8n Webhook Response Mode Placement
# -------------------------------------------------------------------------
def test_v8_p1_04_n8n_webhook_response_mode_at_root():
    """Verify that responseMode is placed at parameters root level on Webhook trigger node."""
    dag = DAGWorkflow("pipeline_wh")
    workflow_registry.register("pipeline_wh", dag)

    exported = export_n8n_workflow("pipeline_wh", host="http://localhost:8000")
    webhook_node = next(n for n in exported["nodes"] if n["type"] == "n8n-nodes-base.webhook")

    assert "responseMode" in webhook_node["parameters"]
    assert webhook_node["parameters"]["responseMode"] == "onReceived"


# -------------------------------------------------------------------------
# P1-05: Per-Step DAG Export Input Preservation
# -------------------------------------------------------------------------
def test_v8_p1_05_per_step_dag_payload_preservation():
    """Verify that per-step DAG export preserves original payload via ($json.payload || $json)."""
    dag = DAGWorkflow(name="chain_dag")

    @dag.node("step_a")
    def step_a(ctx):
        return {"a": 1}

    @dag.node("step_b", depends_on=["step_a"])
    def step_b(ctx):
        return {"b": 2}

    exported = export_dag_to_n8n(dag, host="http://localhost:8000", mode="per_step")
    step_b_node = next(n for n in exported["nodes"] if n["name"] == "Step: step_b")
    body = step_b_node["parameters"]["jsonBody"]

    assert "($json.payload || $json)" in body


# -------------------------------------------------------------------------
# P0-04: n8n Export Generic Credentials Support
# -------------------------------------------------------------------------
def test_v8_p0_04_n8n_export_credentials():
    """Verify that export_n8n_workflow outputs httpHeaderAuth generic credentials when specified."""
    dag = DAGWorkflow("cred_wf")
    workflow_registry.register("cred_wf", dag)

    exported = export_n8n_workflow(
        "cred_wf",
        host="http://localhost:8000",
        auth_mode="api_key",
        credential_id="cred_abc",
        credential_name="NexaPy Prod Key",
    )
    http_node = next(n for n in exported["nodes"] if n["type"] == "n8n-nodes-base.httpRequest")
    assert "credentials" in http_node
    assert "httpHeaderAuth" in http_node["credentials"]
    assert http_node["credentials"]["httpHeaderAuth"]["id"] == "cred_abc"
    assert http_node["credentials"]["httpHeaderAuth"]["name"] == "NexaPy Prod Key"


# -------------------------------------------------------------------------
# P1-07: nexapy init Secure Defaults
# -------------------------------------------------------------------------
def test_v8_p1_07_init_project_secure_defaults():
    """Verify that nexapy init generates allow_anonymous_ai=False and secure random key."""
    orig_cwd = os.getcwd()
    with tempfile.TemporaryDirectory() as tmpdir:
        os.chdir(tmpdir)
        try:
            init(name="my_safe_app", dev_unsafe=False)
            project_dir = os.path.join(tmpdir, "my_safe_app")

            app_py = os.path.join(project_dir, "app.py")
            env_file = os.path.join(project_dir, ".env")

            assert os.path.exists(app_py)
            assert os.path.exists(env_file)

            app_content = open(app_py, "r", encoding="utf-8").read()
            assert "allow_anonymous_ai=False" in app_content

            env_content = open(env_file, "r", encoding="utf-8").read()
            assert "NEXAPY_API_KEY=" in env_content
            # Ensure key is not empty and has high entropy
            for line in env_content.splitlines():
                if line.startswith("NEXAPY_API_KEY="):
                    key_val = line.split("=", 1)[1].strip()
                    assert len(key_val) >= 32
                    assert key_val != "your_secure_api_key_here"
        finally:
            os.chdir(orig_cwd)


# -------------------------------------------------------------------------
# P1-09b: /ai/chat Route Accepts HMAC Signatures
# -------------------------------------------------------------------------
def test_v8_p1_09b_ai_chat_hmac_authentication():
    """Verify that /ai/chat accepts valid HMAC signature when configured."""
    secret = "super-hmac-secret-1234567890"
    app = NexaPy(hmac_secret=secret, allow_anonymous_ai=False)

    client = TestClient(app.fastapi)

    # 1. Without credentials -> 401
    res_no_auth = client.post("/ai/chat", json={"prompt": "hello"})
    assert res_no_auth.status_code == 401

    # 2. Compute valid HMAC signature
    ts = str(int(time.time()))
    nonce = "testnonce123"
    body_bytes = b'{"prompt": "hello"}'
    body_hash = hashlib.sha256(body_bytes).hexdigest()
    string_to_sign = f"v2\n{ts}\n{nonce}\nPOST\n/ai/chat\n{body_hash}"
    sig = hmac.new(secret.encode("utf-8"), string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()

    headers = {
        "Content-Type": "application/json",
        "X-NexaPy-Signature": f"sha256={sig}",
        "X-NexaPy-Timestamp": ts,
        "X-NexaPy-Nonce": nonce,
    }

    mock_resp = AIResponse(text="mock response", provider="mock", model="mock", success=True)
    with patch.object(app._ai_engine, "chat", new=AsyncMock(return_value=mock_resp)):
        res_hmac = client.post("/ai/chat", content=body_bytes, headers=headers)
        assert res_hmac.status_code == 200
        assert res_hmac.json()["text"] == "mock response"
