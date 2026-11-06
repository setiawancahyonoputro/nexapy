"""
Audit Round 7 Comprehensive Regression Tests for NexaPy Framework.
Validates all findings from Audit Profesional NexaPy Framework — Versi V7:
- DAG timeout waiter raises TimeoutError and never executes without a claim (P0-01)
- DAGStateStore.save_step requires a valid claim_token and rejects claim_token=None (P0-02)
- Server-authenticated tenant identity protects against spoofed tenant payloads (P0-03)
- JobQueue recovery race protection with conditional atomic updates (P0-04)
- WorkflowScheduler multi-worker atomic leader election lock (P1-05)
- AI streaming token budget reservation and unified ceiling validator (P1-07 / P1-08)
- n8n sub-minute schedule conversion rejection (P1-03)
- n8n DAG export default mode single_call (P1-03)
- n8n workflow JSON schema validation
"""

import asyncio
import os
import sqlite3
import tempfile
import time
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from nexapy import NexaPy
from nexapy.automation.context import WorkflowContext
from nexapy.automation.dag import (
    DAGStateStore,
    DAGWorkflow,
    get_default_dag_state_store,
    set_default_dag_state_store,
)
from nexapy.automation.queue import Job, JobQueue
from nexapy.automation.registry import workflow_registry
from nexapy.automation.scheduler import WorkflowScheduler
from nexapy.integrations.n8n import (
    convert_schedule_to_n8n_cron,
    export_dag_to_n8n,
    export_n8n_workflow,
    validate_n8n_workflow,
)


@pytest.fixture(autouse=True)
def clean_workflow_registry():
    workflow_registry.clear()
    yield
    workflow_registry.clear()


@pytest.mark.asyncio
async def test_v7_dag_timeout_waiter_does_not_execute_without_claim():
    """P0-01: Verifies that after timeout waiting for another worker, execution is NOT performed."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "dag_timeout.db")
        store = DAGStateStore(db_path=db_path)
        orig_store = get_default_dag_state_store()
        set_default_dag_state_store(store)

        try:
            wf = DAGWorkflow(name="wf_timeout_test")
            execution_counter = {"count": 0}

            @wf.node(name="slow_step")
            def slow_step(ctx: WorkflowContext):
                execution_counter["count"] += 1
                return {"done": True}

            # Worker A claims the step and holds it in RUNNING status
            status_a, _, token_a = store.claim_or_get_step("tenant_1", "wf_timeout_test", "run_x", "slow_step", lease_seconds=60.0)
            assert status_a == "CLAIMED"
            assert token_a is not None

            # Worker B attempts to execute the same step while Worker A is still running
            # Patch poll timeout to 0.1s to avoid waiting 30 seconds
            ctx_b = WorkflowContext(
                input_data={"run_id": "run_x"},
                workflow_name="wf_timeout_test",
                security_context={"tenant_id": "tenant_1"},
            )

            clock = [1000.0]
            def fake_time():
                t = clock[0]
                clock[0] += 10.0
                return t

            with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep, \
                 patch("time.time", side_effect=fake_time):
                with pytest.raises(TimeoutError, match="is still being processed by another worker"):
                    await wf.execute_step_async("slow_step", ctx=ctx_b)

            # Node function MUST NOT have been executed by Worker B
            assert execution_counter["count"] == 0, "Waiter must never execute the step without owning the claim"
        finally:
            set_default_dag_state_store(orig_store)


def test_v7_dag_save_step_requires_claim_token_and_rejects_none():
    """P0-02: Verifies save_step requires a valid claim_token and rejects claim_token=None or wrong token."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "dag_save_test.db")
        store = DAGStateStore(db_path=db_path)

        # Worker A claims step
        status, _, token_a = store.claim_or_get_step("tenant_corp", "wf_save", "run_1", "step_a")
        assert status == "CLAIMED"
        assert token_a is not None

        # 1. Calling save_step with claim_token=None MUST return False
        saved_none = store.save_step("tenant_corp", "wf_save", "run_1", "step_a", {"val": "hack"}, claim_token=None)
        assert saved_none is False, "save_step without claim_token must be rejected"

        # 2. Calling save_step with wrong claim_token MUST return False
        saved_wrong = store.save_step("tenant_corp", "wf_save", "run_1", "step_a", {"val": "bad"}, claim_token="wrong_token_123")
        assert saved_wrong is False, "save_step with wrong claim_token must be rejected"

        # Verify step is still RUNNING
        info = store.get_step_status("tenant_corp", "wf_save", "run_1", "step_a")
        assert info["status"] == "RUNNING"
        assert info["claim_token"] == token_a

        # 3. Calling save_step with valid token_a MUST succeed
        saved_valid = store.save_step("tenant_corp", "wf_save", "run_1", "step_a", {"val": "success_val"}, claim_token=token_a)
        assert saved_valid is True

        # Verify final state is success
        info_final = store.get_step_status("tenant_corp", "wf_save", "run_1", "step_a")
        assert info_final["status"] == "success"
        assert info_final["result"] == {"val": "success_val"}

        # 4. Admin API allows explicit administrative state override
        store.admin_set_step("tenant_corp", "wf_save", "run_1", "step_admin", {"admin": True})
        admin_info = store.get_step_status("tenant_corp", "wf_save", "run_1", "step_admin")
        assert admin_info["status"] == "success"
        assert admin_info["result"] == {"admin": True}


def test_v7_tenant_isolation_rejects_untrusted_spoofed_payload():
    """P0-03: Verifies HTTP endpoint rejects spoofed tenant_id in request payload when authenticated."""
    app = NexaPy()
    secret_key = "secret_auth_key_123"
    app.mount_workflows(prefix="/workflows", api_key=secret_key)
    client = TestClient(app.fastapi)

    dag = DAGWorkflow("secure_dag")

    @dag.node("step1")
    def step1(ctx):
        return {"ok": True}

    workflow_registry.register("secure_dag", dag)

    # Client authenticates with valid key and specifies X-Tenant-ID: tenant_alpha,
    # but injects "tenant_id": "tenant_beta_victim" into request JSON
    headers = {
        "X-NexaPy-Key": secret_key,
        "X-Tenant-ID": "tenant_alpha",
    }
    payload = {
        "tenant_id": "tenant_beta_victim",  # Spoofed!
        "amount": 500,
    }

    # 1. Direct workflow endpoint must reject with 403 TENANT_MISMATCH
    resp = client.post("/workflows/secure_dag", headers=headers, json=payload)
    assert resp.status_code == 403
    assert resp.json()["error_code"] == "TENANT_MISMATCH"

    # 2. Async job submission must also reject with 403 TENANT_MISMATCH
    job_payload = {
        "workflow": "secure_dag",
        "input": {"tenant_id": "tenant_beta_victim"},
    }
    resp_job = client.post("/workflows/jobs", headers=headers, json=job_payload)
    assert resp_job.status_code == 403
    assert resp_job.json()["error_code"] == "TENANT_MISMATCH"


def test_v7_queue_recovery_race_conditional_update():
    """P0-04: Verifies recover_pending_jobs does NOT overwrite newly claimed jobs during recovery."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "queue_race.db")
        q_rec = JobQueue(db_path=db_path)
        q_worker = JobQueue(db_path=db_path)

        async def _init_job():
            job = await q_rec.enqueue("dummy_task", {"v": 1})
            return job.job_id

        job_id = asyncio.run(_init_job())

        # Worker A claims job with short lease
        claimed_a = q_rec.claim_job(job_id, worker_id="worker_a", lease_seconds=1.0)
        assert claimed_a is not None
        orig_token = claimed_a.claim_token

        # Expire lease artificially in database
        conn = sqlite3.connect(db_path)
        conn.execute("UPDATE nexapy_jobs SET started_at = ?, lease_expires_at = ? WHERE job_id = ?", (time.time() - 200, time.time() - 100, job_id))
        conn.commit()
        conn.close()

        # Worker B claims the expired job legitimately
        claimed_b = q_worker.claim_job(job_id, worker_id="worker_b", lease_seconds=60.0)
        assert claimed_b is not None
        assert claimed_b.claim_token != orig_token

        # Now simulate recovery running with old lease condition
        # Recovery must check claim_token and see Worker B is actively processing
        recovered = q_rec.recover_pending_jobs(lease_seconds=60.0)
        assert recovered == 0, "Recovery must not steal active job leased by Worker B"

        # Authoritative job status must remain PROCESSING with Worker B
        authoritative = q_worker.get_job(job_id)
        assert authoritative.status == "PROCESSING"
        assert authoritative.worker_id == "worker_b"
        assert authoritative.claim_token == claimed_b.claim_token


def test_v7_n8n_subminute_schedule_rejected():
    """P1-03: Sub-minute intervals like '30s' or '10s' are rejected rather than corrupted to '* * * * *'."""
    with pytest.raises(ValueError, match="Sub-minute interval '30s' cannot be converted"):
        convert_schedule_to_n8n_cron("30s")

    with pytest.raises(ValueError, match="Sub-minute interval '10s' cannot be converted"):
        convert_schedule_to_n8n_cron("10s")

    # Valid minute-based intervals work properly
    assert convert_schedule_to_n8n_cron("5m") == "*/5 * * * *"
    assert convert_schedule_to_n8n_cron("1h") == "0 */1 * * *"


def test_v7_n8n_dag_export_single_call_default():
    """P1-03: DAG export defaults to mode='single_call' for atomic safety."""
    dag = DAGWorkflow("checkout_dag")

    @dag.node("validate")
    def validate_node():
        return True

    @dag.node("charge", depends_on=["validate"])
    def charge_node():
        return True

    workflow_registry.register("checkout_dag", dag)

    # 1. Default mode is single_call
    exported_default = export_dag_to_n8n(dag)
    assert len(exported_default["nodes"]) == 2
    http_node = next(n for n in exported_default["nodes"] if n["type"] == "n8n-nodes-base.httpRequest")
    assert http_node["name"] == "Run DAG checkout_dag"

    # 2. Default export_n8n_workflow for DAG is also single_call
    exported_wf = export_n8n_workflow("checkout_dag")
    assert len(exported_wf["nodes"]) == 2

    # 3. Explicit per_step mode still available
    exported_per_step = export_dag_to_n8n(dag, mode="per_step")
    assert len(exported_per_step["nodes"]) == 3  # Trigger + validate + charge


def test_v7_n8n_workflow_schema_validation():
    """P1-03: n8n workflow JSON validator enforces strict schema rules."""
    # Valid workflow
    valid_wf = {
        "name": "Order Workflow",
        "nodes": [
            {"name": "Start", "type": "n8n-nodes-base.webhook", "parameters": {"path": "start"}},
            {"name": "Action", "type": "n8n-nodes-base.httpRequest", "parameters": {"url": "http://api.org"}},
        ],
        "connections": {
            "Start": {"main": [[{"node": "Action", "type": "main", "index": 0}]]}
        },
    }
    assert validate_n8n_workflow(valid_wf) is True

    # Duplicate node names rejected
    dup_wf = {
        "name": "Dup Workflow",
        "nodes": [
            {"name": "SameName", "type": "n8n-nodes-base.webhook"},
            {"name": "SameName", "type": "n8n-nodes-base.httpRequest"},
        ],
        "connections": {},
    }
    assert validate_n8n_workflow(dup_wf) is False

    # Connection to non-existent node rejected
    broken_conn = {
        "name": "Broken Conn",
        "nodes": [{"name": "NodeA", "type": "n8n-nodes-base.webhook"}],
        "connections": {
            "NodeA": {"main": [[{"node": "GhostNode", "type": "main", "index": 0}]]}
        },
    }
    assert validate_n8n_workflow(broken_conn) is False


def test_v7_ai_stream_token_ceiling_and_budget_reservation():
    """P1-07 / P1-08: AI streaming enforces max_tokens ceiling and reserves output budget upfront."""
    app = NexaPy(ai_api_keys=["ai_key_v7"], token_limit=50)
    client = TestClient(app.fastapi)

    # 1. Upfront budget exceeds quota
    # Requesting max_tokens=100 when token_limit is 50 must be rejected upfront with 429
    resp = client.post(
        "/ai/stream",
        headers={"X-NexaPy-Key": "ai_key_v7"},
        json={"prompt": "Hello world", "max_tokens": 100},
    )
    assert resp.status_code == 429
    assert "token budget exceeded" in resp.json()["error"].lower()


def test_v7_scheduler_multi_instance_leader_election():
    """P1-05: Multi-instance scheduler election lock ensures only one process runs scheduled slot."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "sched_lock.db")
        sched_a = WorkflowScheduler(db_path=db_path)
        sched_b = WorkflowScheduler(db_path=db_path)

        slot_key = "cron_2026-11-06 12:00"

        # Worker A acquires lock for slot
        lock_a = sched_a._acquire_execution_lock("daily_report", slot_key)
        assert lock_a is True

        # Worker B tries to acquire lock for same slot - must be rejected
        lock_b = sched_b._acquire_execution_lock("daily_report", slot_key)
        assert lock_b is False

        # Next slot can be acquired
        next_slot = "cron_2026-11-06 12:01"
        assert sched_b._acquire_execution_lock("daily_report", next_slot) is True

