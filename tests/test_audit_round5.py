"""
Test Suite for Audit Profesional NexaPy - Versi Terbaru (V5) Findings Verification.
Validates:
- P0-01: HMAC query parameter tampering protection.
- P0-02: Persistent SQLiteNonceStore distributed replay defense across worker instances.
- P0-03: Leased job queue recovery protecting active PROCESSING jobs from duplicate claiming.
- P0-04: DAG parallel execution atomic step claiming ensuring exactly-one execution.
- P0-05: Persistent SQLite DAG state store configuration and cross-instance state isolation.
- P1-01: AI streaming token quota enforcement, cutoff event, and reconciliation.
- P1-02: n8n official schedule trigger interval structure and valid jsonBody expression syntax.
- P1-03: n8n Community Node listWorkflows HMAC signature path and method alignment.
"""

import asyncio
import json
import sqlite3
import tempfile
import time
from typing import Any, Dict
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from nexapy import NexaPy, workflow
from nexapy.automation import workflow_registry
from nexapy.automation.context import WorkflowContext
from nexapy.automation.dag import DAGStateStore, DAGWorkflow
from nexapy.automation.queue import Job, JobQueue
from nexapy.integrations.n8n import export_dag_to_n8n, export_n8n_workflow
from nexapy.integrations.node_generator import generate_n8n_community_node
from nexapy.security import (
    AIAuthGuard,
    HMACVerifier,
    SQLiteNonceStore,
    canonicalize_path,
    get_global_nonce_store,
    set_global_nonce_store,
)


def test_v5_p0_01_hmac_query_parameter_tampering_rejected():
    """
    P0-01: HMAC signature must bind all request parameters including URL query strings.
    Tampering with ?step=validate to ?step=execute must be rejected with HTTP 403.
    """
    workflow_registry.clear()

    payment_dag = DAGWorkflow("payment_v5")

    @payment_dag.node("validate")
    def validate_step(ctx):
        return {"step_executed": "validate"}

    @payment_dag.node("execute")
    def execute_step(ctx):
        return {"step_executed": "execute"}

    workflow_registry.register("payment_v5", payment_dag)

    app = NexaPy()
    secret = "hmac_secret_audit_v5"
    app.mount_workflows(prefix="/workflows", hmac_secret=secret)
    client = TestClient(app.fastapi)

    body_bytes = json.dumps({"amount": 100}).encode("utf-8")
    ts = str(int(time.time()))
    nonce = "nonce_p0_01_a"

    # Client signs for ?step=validate
    valid_sig = HMACVerifier.compute_signature_v2(
        secret=secret,
        timestamp=ts,
        nonce=nonce,
        method="POST",
        path="/workflows/payment_v5?step=validate",
        payload=body_bytes,
    )

    headers = {
        "Content-Type": "application/json",
        "X-NexaPy-Signature": f"sha256={valid_sig}",
        "X-NexaPy-Timestamp": ts,
        "X-NexaPy-Nonce": nonce,
    }

    # 1. Tampered query parameter: attacker changes step to 'execute' using same signature -> 403 Forbidden
    res_tampered = client.post(
        "/workflows/payment_v5?step=execute",
        content=body_bytes,
        headers=headers,
    )
    assert res_tampered.status_code == 403
    assert res_tampered.json().get("success") is False
    assert "authentication" in res_tampered.json().get("error", "").lower()

    # 2. Legitimate request with matching signature -> 200 OK
    res_valid = client.post(
        "/workflows/payment_v5?step=validate",
        content=body_bytes,
        headers=headers,
    )
    assert res_valid.status_code == 200
    assert res_valid.json()["result"]["step"] == "validate"

    # 3. Canonical query parameter sorting check (?b=2&a=1 matches ?a=1&b=2)
    nonce_sort = "nonce_p0_01_sort"
    sig_sorted = HMACVerifier.compute_signature_v2(
        secret=secret,
        timestamp=ts,
        nonce=nonce_sort,
        method="POST",
        path="/workflows/payment_v5?a=1&b=2",
        payload=body_bytes,
    )
    res_sorted = client.post(
        "/workflows/payment_v5?b=2&a=1",
        content=body_bytes,
        headers={
            "Content-Type": "application/json",
            "X-NexaPy-Signature": f"sha256={sig_sorted}",
            "X-NexaPy-Timestamp": ts,
            "X-NexaPy-Nonce": nonce_sort,
        },
    )
    assert res_sorted.status_code == 200


def test_v5_p0_02_sqlite_nonce_store_distributed_replay_defense():
    """
    P0-02: Nonce replay protection must work across multiple worker processes
    using shared persistent SQLite storage.
    """
    with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as tmp:
        db_path = tmp.name

    try:
        # Simulate two separate uvicorn worker instances accessing the same database
        worker_a_store = SQLiteNonceStore(db_path=db_path, ttl_seconds=300.0)
        worker_b_store = SQLiteNonceStore(db_path=db_path, ttl_seconds=300.0)

        # Worker A receives and verifies nonce "worker-a-nonce-1"
        assert worker_a_store.check_and_add("worker-a-nonce-1") is True

        # Worker B receives the same nonce (replay attack) -> MUST return False!
        assert worker_b_store.check_and_add("worker-a-nonce-1") is False

        # Verify has_seen across instances
        assert worker_b_store.has_seen("worker-a-nonce-1") is True

        # Strict HMAC verification with shared SQLite store
        secret = "secret_v5_shared"
        payload = b'{"msg": "distributed"}'
        ts = str(int(time.time()))
        nonce = "nonce_worker_cross"

        sig = HMACVerifier.compute_signature_v2(
            secret=secret,
            timestamp=ts,
            nonce=nonce,
            method="POST",
            path="/api",
            payload=payload,
        )

        # First verification on Worker A -> True
        ok_a = HMACVerifier.verify_signature(
            secret, payload, sig, timestamp=ts, nonce=nonce, method="POST", path="/api",
            enforce_timestamp=True, enforce_nonce=True, nonce_store=worker_a_store
        )
        assert ok_a is True

        # Second verification of exact same signature on Worker B -> False (Replayed!)
        ok_b = HMACVerifier.verify_signature(
            secret, payload, sig, timestamp=ts, nonce=nonce, method="POST", path="/api",
            enforce_timestamp=True, enforce_nonce=True, nonce_store=worker_b_store
        )
        assert ok_b is False
    finally:
        import os
        try:
            os.remove(db_path)
        except Exception:
            pass


def test_v5_p0_03_job_queue_recovery_does_not_reset_active_processing_jobs():
    """
    P0-03: recover_pending_jobs() must NOT reset active jobs being processed
    by another worker instance to QUEUED. Only jobs with expired leases are recovered.
    """
    with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as tmp:
        db_path = tmp.name

    try:
        queue_a = JobQueue(db_path=db_path)
        queue_b = JobQueue(db_path=db_path)

        now = time.time()
        # Insert Job 1: actively PROCESSING by worker A (started 5 seconds ago, lease is 60s)
        active_job = Job(
            job_id="job_active_worker_a",
            workflow_name="wf_test",
            input_data={"n": 1},
            status="PROCESSING",
            created_at=now - 10,
            started_at=now - 5,
            worker_id="worker_a",
            claim_token="token_a_123",
        )
        queue_a._save_job(active_job)

        # Insert Job 2: crashed PROCESSING job (started 120 seconds ago, lease expired)
        crashed_job = Job(
            job_id="job_crashed_past",
            workflow_name="wf_test",
            input_data={"n": 2},
            status="PROCESSING",
            created_at=now - 200,
            started_at=now - 120,
            worker_id="crashed_worker",
            claim_token="token_crashed",
        )
        queue_a._save_job(crashed_job)

        # Worker B starts up and runs recover_pending_jobs
        recovered_count = queue_b.recover_pending_jobs(lease_seconds=60.0)

        # MUST recover ONLY the crashed job, NOT the active job
        assert recovered_count == 1

        # Check active job status in DB: MUST still be PROCESSING!
        job_active_check = queue_b.get_job("job_active_worker_a")
        assert job_active_check.status == "PROCESSING"
        assert job_active_check.worker_id == "worker_a"

        # Check crashed job status in DB: MUST be reset to QUEUED for retry
        job_crashed_check = queue_b.get_job("job_crashed_past")
        assert job_crashed_check.status == "QUEUED"
    finally:
        import os
        try:
            os.remove(db_path)
        except Exception:
            pass


@pytest.mark.asyncio
async def test_v5_p0_04_dag_parallel_requests_execute_handler_exactly_once():
    """
    P0-04: DAG execution must guarantee exactly-one execution per step under parallel requests.
    Two concurrent requests executing the same step run the underlying function exactly once.
    """
    workflow_registry.clear()
    execution_counter = 0

    dag = DAGWorkflow("parallel_dag_v5")

    @dag.node("expensive_step")
    async def expensive_step(ctx: WorkflowContext):
        nonlocal execution_counter
        execution_counter += 1
        await asyncio.sleep(0.15)
        return {"computed_val": 42}

    run_id = f"run_par_{int(time.time())}"
    ctx1 = WorkflowContext(input_data={"run_id": run_id, "tenant_id": "tenant_v5"})
    ctx2 = WorkflowContext(input_data={"run_id": run_id, "tenant_id": "tenant_v5"})

    # Launch both concurrent requests simultaneously
    results = await asyncio.gather(
        dag.execute_step_async("expensive_step", ctx1),
        dag.execute_step_async("expensive_step", ctx2),
    )

    # 1. Both requests must succeed and return the computation result
    assert results[0]["result"] == {"computed_val": 42}
    assert results[1]["result"] == {"computed_val": 42}

    # 2. Crucial audit check: handler function execution count MUST be exactly 1!
    assert execution_counter == 1, f"Handler executed {execution_counter} times! Expected exactly 1."

    # 3. One request was original success, the other was cached
    statuses = {results[0]["status"], results[1]["status"]}
    assert "success" in statuses
    assert "cached" in statuses


def test_v5_p0_05_dag_state_persistence_with_sqlite():
    """
    P0-05: Connect DAG state persistence (DAGStateStore with SQLite) to application runtime.
    State survives across store instances.
    """
    with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as tmp:
        db_path = tmp.name

    try:
        store1 = DAGStateStore(db_path=db_path)
        store1.save_step("tenant_1", "wf_order", "run_100", "step_validate", {"valid": True}, status="success")

        # Simulate fresh server process accessing same database
        store2 = DAGStateStore(db_path=db_path)
        steps = store2.get_run_steps("tenant_1", "wf_order", "run_100")

        assert "step_validate" in steps
        assert steps["step_validate"] == {"valid": True}

        # Multi-tenant isolation verification
        steps_other_tenant = store2.get_run_steps("tenant_2", "wf_order", "run_100")
        assert steps_other_tenant == {}
    finally:
        import os
        try:
            os.remove(db_path)
        except Exception:
            pass


def test_v5_p1_01_ai_streaming_token_limit_enforced_and_cutoff():
    """
    P1-01: AI streaming respects token quota and cuts off mid-stream when quota exceeded.
    """
    app = NexaPy()
    # Configure strict token limit: 3 tokens total
    guard = AIAuthGuard(valid_keys=["key_stream_test"], token_limit=3, allow_anonymous=False)
    app.ai_guard = guard
    client = TestClient(app.fastapi)

    async def mock_streaming_engine(prompt, **kwargs):
        # Generates 10 tokens
        for i in range(10):
            await asyncio.sleep(0.01)
            yield f"tok{i} "

    app._ai_engine.stream = mock_streaming_engine

    # 1. Stream request: prompt has 1 token, limit is 3. Should cutoff after 2 generated tokens.
    res = client.post(
        "/ai/stream",
        json={"prompt": "hi"},
        headers={"X-NexaPy-Key": "key_stream_test"},
    )
    assert res.status_code == 200
    events = res.text.split("\n\n")

    received_text = ""
    error_found = False
    for ev in events:
        if ev.startswith("data: ") and ev != "data: [DONE]":
            try:
                data = json.loads(ev[6:])
                if data.get("text"):
                    received_text += data["text"]
                if data.get("error"):
                    error_found = True
            except Exception:
                pass

    # Should have cutoff and emitted quota exceeded error
    assert error_found is True
    # Recorded tokens in guard must NOT exceed quota ceiling of 3
    assert guard.token_usage.get("key_stream_test", 0) <= 3

    # 2. Subsequent request must be rejected with 429 Token quota exceeded
    res_second = client.post(
        "/ai/stream",
        json={"prompt": "more text"},
        headers={"X-NexaPy-Key": "key_stream_test"},
    )
    assert res_second.status_code == 429
    assert "token quota exceeded" in res_second.json().get("error", "").lower()


def test_v5_p1_02_n8n_schedule_trigger_schema_and_json_body_format():
    """
    P1-02:
    - n8n Schedule Trigger matches official interval array structure.
    - per-step DAG HTTP Request jsonBody uses valid n8n expression format.
    """
    workflow_registry.clear()

    @workflow("audit5_cron_wf", schedule="*/15 * * * *")
    def cron_wf(ctx):
        return "ok"

    n8n_export = export_n8n_workflow("audit5_cron_wf")
    sched_node = next(n for n in n8n_export["nodes"] if n["type"] == "n8n-nodes-base.scheduleTrigger")
    rule = sched_node["parameters"]["rule"]

    # Official n8n schedule trigger structure
    assert "interval" in rule
    assert isinstance(rule["interval"], list)
    assert rule["interval"][0]["field"] == "cronExpression"
    assert rule["interval"][0]["expression"] == "*/15 * * * *"
    # Backward compatibility
    assert rule["cronExpression"] == "*/15 * * * *"

    # DAG per-step jsonBody format check
    dag = DAGWorkflow("dag_format_test")

    @dag.node("step_a")
    def step_a():
        return 1

    dag_export = export_dag_to_n8n(dag, mode="per_step")
    step_node = next(n for n in dag_export["nodes"] if n["name"] == "Step: step_a")
    json_body = step_node["parameters"]["jsonBody"]

    # Expression must start with ={{ and end with }}
    assert json_body.startswith("={{")
    assert json_body.endswith("}}")
    # Must contain step, run_id, and payload
    assert '"step": "step_a"' in json_body
    assert "run_id" in json_body
    assert "payload" in json_body


def test_v5_p1_03_community_node_list_workflows_hmac_headers():
    """
    P1-03: Community Node listWorkflows uses buildHeaders(null, 'GET', '/workflows/_schema').
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        out_path = generate_n8n_community_node(tmpdir)
        node_file = out_path / "src" / "nodes" / "NexaPy" / "NexaPy.node.ts"
        assert node_file.exists()
        ts_code = node_file.read_text(encoding="utf-8")

        # Must call buildHeaders with GET and /workflows/_schema
        assert "buildHeaders(null, 'GET', '/workflows/_schema')" in ts_code
        # Must preserve buildHeaders({}) for backward test compatibility
        assert "headers: buildHeaders({})" in ts_code
