"""
Audit Round 6 Comprehensive Regression Tests for NexaPy Framework.
Validates all findings from Audit Profesional NexaPy Framework — Versi V6:
- Cron matches month correctly (mo_expr validation)
- DAGStateStore cross-worker synchronization (P0-01)
- DAGStateStore fencing token prevents stale worker overwrite (P0-02)
- JobQueue heartbeat and lease protection against worker theft (P0-04)
- JobQueue terminal cancellation prevents COMPLETED overwrite (P0-05)
- DAG trusted tenant identity from security context overrides untrusted payload
"""

import asyncio
import datetime
import os
import sqlite3
import tempfile
import time
import pytest

from nexapy.automation.scheduler import matches_cron
from nexapy.automation.dag import (
    DAGStateStore,
    DAGWorkflow,
    set_default_dag_state_store,
    get_default_dag_state_store,
)
from nexapy.automation.context import WorkflowContext
from nexapy.automation.queue import JobQueue, Job


def test_v6_cron_matches_month():
    """Verify matches_cron accurately evaluates month field."""
    # Specific month (December = 12)
    dt_jan = datetime.datetime(2026, 1, 1, 12, 0)
    dt_dec = datetime.datetime(2026, 12, 1, 12, 0)
    assert not matches_cron("0 12 * 12 *", dt_jan), "January should not match month 12"
    assert matches_cron("0 12 * 12 *", dt_dec), "December should match month 12"

    # Month range (1-6)
    dt_mar = datetime.datetime(2026, 3, 15, 12, 0)
    dt_aug = datetime.datetime(2026, 8, 15, 12, 0)
    assert matches_cron("0 12 * 1-6 *", dt_mar), "March should match 1-6"
    assert not matches_cron("0 12 * 1-6 *", dt_aug), "August should not match 1-6"

    # Step in month (*/2 matches 1, 3, 5, 7, 9, 11 in 1-based POSIX cron)
    assert matches_cron("0 12 * */2 *", dt_jan), "January should match */2"
    assert not matches_cron("0 12 * */2 *", datetime.datetime(2026, 2, 1, 12, 0)), "February should not match */2"
    assert matches_cron("0 12 * 2-12/2 *", datetime.datetime(2026, 2, 1, 12, 0)), "February should match 2-12/2"


def test_v6_dag_state_store_cross_worker_sync():
    """P0-01: Verify DAGStateStore synchronizes state across worker instances via SQLite."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "dag_sync.db")
        store_a = DAGStateStore(db_path=db_path)
        store_b = DAGStateStore(db_path=db_path)

        # Worker A claims step
        status, cached, token_a = store_a.claim_or_get_step("tenant_1", "wf_sync", "run_1", "step_compute")
        assert status == "CLAIMED"
        assert token_a is not None

        # Worker B reads step status - should be RUNNING
        info_b = store_b.get_step_status("tenant_1", "wf_sync", "run_1", "step_compute")
        assert info_b is not None
        assert info_b["status"] == "RUNNING"

        # Worker A finishes and saves success
        success = store_a.save_step("tenant_1", "wf_sync", "run_1", "step_compute", {"answer": 42}, status="success", claim_token=token_a)
        assert success is True

        # Worker B reads step status without re-instantiation - must see 'success' immediately
        status_b_info = store_b.get_step_status("tenant_1", "wf_sync", "run_1", "step_compute")
        assert status_b_info is not None
        assert status_b_info["status"] == "success", f"Expected 'success', got '{status_b_info['status']}'"
        assert status_b_info["result"] == {"answer": 42}

        # Worker B reads all run steps
        run_steps = store_b.get_run_steps("tenant_1", "wf_sync", "run_1")
        assert "step_compute" in run_steps
        assert run_steps["step_compute"] == {"answer": 42}


def test_v6_dag_fencing_token_prevents_stale_overwrite():
    """P0-02: Verify stale worker cannot overwrite newer worker's results after lease expiry."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "dag_fence.db")
        store_a = DAGStateStore(db_path=db_path)
        store_b = DAGStateStore(db_path=db_path)

        # Worker A claims step
        status_a, _, token_a = store_a.claim_or_get_step("tenant_1", "wf_fence", "run_1", "step_1")
        assert status_a == "CLAIMED"
        assert token_a is not None

        # Simulate Worker A lease expiration in database
        conn = sqlite3.connect(db_path)
        conn.execute(
            "UPDATE nexapy_dag_states SET updated_at = ? WHERE tenant_id = 'tenant_1' AND step_name = 'step_1'",
            (time.time() - 400,),
        )
        conn.commit()
        conn.close()

        # Worker B re-claims the expired step
        status_b, _, token_b = store_b.claim_or_get_step(
            "tenant_1", "wf_fence", "run_1", "step_1", lease_seconds=300
        )
        assert status_b == "CLAIMED"
        assert token_b != token_a

        # Worker B successfully saves its result
        saved_b = store_b.save_step(
            "tenant_1", "wf_fence", "run_1", "step_1",
            {"msg": "new worker result"}, status="success", claim_token=token_b
        )
        assert saved_b is True

        # Worker A attempts late save using stale claim token
        saved_a = store_a.save_step(
            "tenant_1", "wf_fence", "run_1", "step_1",
            {"msg": "old worker result"}, status="success", claim_token=token_a
        )
        assert saved_a is False, "Stale worker save must fail and return False"

        # Verify authoritative state preserved Worker B's result
        final_steps = store_b.get_run_steps("tenant_1", "wf_fence", "run_1")
        assert final_steps["step_1"] == {"msg": "new worker result"}


def test_v6_queue_heartbeat_lease_extension():
    """Verify active worker heartbeats prevent job theft and false recovery."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "queue_lease.db")
        queue_a = JobQueue(db_path=db_path)
        queue_b = JobQueue(db_path=db_path)

        async def _run():
            job = await queue_a.enqueue("dummy_wf", {"x": 1})
            return job.job_id

        job_id = asyncio.run(_run())

        # Worker A claims job with a short lease (1 second)
        claimed_a = queue_a.claim_job(job_id, worker_id="worker_a", lease_seconds=1.0)
        assert claimed_a is not None
        assert claimed_a.claim_token is not None

        # Worker A sends a heartbeat extending lease by 10 seconds
        hb_success = queue_a.heartbeat(
            job_id, worker_id="worker_a", claim_token=claimed_a.claim_token, extension_seconds=10.0
        )
        assert hb_success is True

        # Wait past the original 1.0s lease
        time.sleep(1.2)

        # Worker B tries to claim the job - must be rejected because heartbeat extended lease
        claimed_b = queue_b.claim_job(job_id, worker_id="worker_b", lease_seconds=1.0)
        assert claimed_b is None, "Active job with extended lease must not be stolen by worker B"

        # Worker B recovery also must NOT recover this active job
        recovered = queue_b.recover_pending_jobs(lease_seconds=1.0)
        assert recovered == 0, "Active leased job must not be recovered"


def test_v6_queue_cancellation_is_terminal():
    """Verify job cancellation cannot be overwritten by subsequent COMPLETED finalization."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "queue_cancel.db")
        queue_a = JobQueue(db_path=db_path)
        queue_b = JobQueue(db_path=db_path)

        async def _run():
            job = await queue_a.enqueue("long_task", {"v": 10})
            return job.job_id

        job_id = asyncio.run(_run())

        # Worker A claims job
        claimed_a = queue_a.claim_job(job_id, worker_id="worker_a")
        assert claimed_a is not None

        # Client / Worker B cancels the job
        cancel_success = queue_b.cancel_job(job_id)
        assert cancel_success is True

        cancelled_job = queue_b.get_job(job_id)
        assert cancelled_job.status == "CANCELLED"

        # Worker A completes execution and attempts to finalize job as COMPLETED
        finalized = queue_a._finalize_job(claimed_a, status="COMPLETED", result={"done": True})
        assert finalized is False, "_finalize_job must fail when job is CANCELLED"

        # Confirm authoritative status in SQLite is still CANCELLED
        job_after = queue_b.get_job(job_id)
        assert job_after.status == "CANCELLED"
        assert job_after.error == "Job was cancelled by client."


@pytest.mark.asyncio
async def test_v6_trusted_tenant_identity_override():
    """P0-05: Verify server security_context overrides untrusted payload tenant_id."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "dag_tenant.db")
        store = DAGStateStore(db_path=db_path)
        orig_store = get_default_dag_state_store()
        set_default_dag_state_store(store)

        try:
            wf = DAGWorkflow(name="tenant_wf")

            received_context = {}

            @wf.node(name="step_audit")
            def step_audit(ctx: WorkflowContext):
                received_context["tenant_id"] = (
                    ctx.security_context.get("tenant_id")
                    if isinstance(ctx.security_context, dict)
                    else None
                )
                received_context["input"] = ctx.input
                return {"authenticated": True}

            # Untrusted client sends spoofed tenant_id in input payload,
            # but trusted auth server provides true tenant_id in security_context
            security_ctx = {"tenant_id": "trusted_company_corp", "user_id": "usr_99"}
            client_payload = {"tenant_id": "evil_spoofed_corp", "data": "secret", "run_id": "run_101"}

            ctx = WorkflowContext(
                input_data=client_payload,
                workflow_name="tenant_wf",
                security_context=security_ctx,
            )

            res = await wf.execute_step_async("step_audit", ctx=ctx)

            assert res["status"] == "success"
            assert res["result"] == {"authenticated": True}
            assert res["tenant_id"] == "trusted_company_corp", "Response tenant_id must be trusted server identity"

            # Check DAG store - records must be under trusted tenant
            trusted_steps = store.get_run_steps("trusted_company_corp", "tenant_wf", "run_101")
            assert "step_audit" in trusted_steps
            assert trusted_steps["step_audit"] == {"authenticated": True}

            # Evil spoofed tenant must have no records
            evil_steps = store.get_run_steps("evil_spoofed_corp", "tenant_wf", "run_101")
            assert len(evil_steps) == 0
        finally:
            set_default_dag_state_store(orig_store)
