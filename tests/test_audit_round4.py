"""Comprehensive tests validating all P0 and P1 audit fixes from Round 4 Professional Audit."""

import asyncio
import datetime
import hashlib
import json
import sqlite3
import tempfile
import time
from pathlib import Path
from unittest.mock import AsyncMock, patch
import pytest
from fastapi.testclient import TestClient

from nexapy import NexaPy, workflow
from nexapy.ai.base import AIResponse
from nexapy.ai.router import AIRouter
from nexapy.automation import DAGWorkflow, WorkflowContext, workflow_registry
from nexapy.automation.actions.notification import NotificationAction
from nexapy.automation.dag import DAGStateStore, _dag_run_state_store
from nexapy.automation.queue import Job, JobQueue
from nexapy.automation.scheduler import WorkflowScheduler, matches_cron
from nexapy.integrations.n8n import (
    convert_schedule_to_n8n_cron,
    export_dag_to_n8n,
    export_n8n_workflow,
    import_n8n_workflow,
)
from nexapy.security import (
    AIAuthGuard,
    HMACVerifier,
    NonceTracker,
    RateLimiter,
)


@pytest.fixture(autouse=True)
def clean_env():
    workflow_registry.clear()
    _dag_run_state_store.clear()
    yield
    workflow_registry.clear()
    _dag_run_state_store.clear()


# ============================================================================
# P0-01, P0-02, P0-03: HMAC v2 & REPLAY DEFENSE TESTS
# ============================================================================

def test_p0_01_hmac_v2_nonce_inclusion_and_tampering_rejected():
    """
    P0-01: Nonce MUST be included in signature.
    Same signature with different nonce MUST be rejected.
    """
    secret = "audit4_secret_key"
    payload = b'{"action": "transfer", "amount": 5000}'
    ts = str(int(time.time()))
    nonce_a = "nonce_alpha_1234"
    nonce_b = "nonce_beta_5678"
    method = "POST"
    path = "/workflows/transfer"

    # Compute valid signature with nonce A
    sig_a = HMACVerifier.compute_signature_v2(
        secret=secret,
        timestamp=ts,
        nonce=nonce_a,
        method=method,
        path=path,
        payload=payload,
    )

    # 1. Valid signature + nonce A -> True
    assert HMACVerifier.verify_signature(
        secret=secret,
        payload=payload,
        signature=sig_a,
        timestamp=ts,
        nonce=nonce_a,
        method=method,
        path=path,
        enforce_timestamp=True,
        enforce_nonce=True,
    ) is True

    # 2. Replay with same nonce A -> False (already seen)
    assert HMACVerifier.verify_signature(
        secret=secret,
        payload=payload,
        signature=sig_a,
        timestamp=ts,
        nonce=nonce_a,
        method=method,
        path=path,
        enforce_timestamp=True,
        enforce_nonce=True,
    ) is False

    # 3. Same signature + nonce B -> MUST BE REJECTED (proves nonce is cryptographically bound!)
    assert HMACVerifier.verify_signature(
        secret=secret,
        payload=payload,
        signature=sig_a,
        timestamp=ts,
        nonce=nonce_b,
        method=method,
        path=path,
        enforce_timestamp=True,
        enforce_nonce=True,
    ) is False


def test_p0_01_timestamp_required_empty_nonce_rejected():
    """
    P0-01: When timestamp is provided or enforced, empty/missing nonce MUST be rejected.
    """
    secret = "audit4_secret_key"
    payload = b'{"msg": "ping"}'
    ts = str(int(time.time()))
    sig = HMACVerifier.compute_signature_v2(secret, ts, "nonce123", "POST", "/ping", payload)

    # Missing nonce with enforce_timestamp=True -> False
    assert HMACVerifier.verify_signature(
        secret=secret,
        payload=payload,
        signature=sig,
        timestamp=ts,
        nonce=None,
        enforce_timestamp=True,
    ) is False

    # Empty string nonce -> False
    assert HMACVerifier.verify_signature(
        secret=secret,
        payload=payload,
        signature=sig,
        timestamp=ts,
        nonce="",
        enforce_timestamp=True,
    ) is False


def test_p0_02_workflow_endpoint_enforces_hmac_v2_and_rejects_legacy():
    """
    P0-02: Workflow endpoints require strict HMAC-v2 with timestamp and nonce,
    and reject un-timestamped legacy HMAC signatures.
    """
    secret = "workflow_guard_secret"

    @workflow("secure_payout")
    def secure_payout(ctx):
        return {"paid": True}

    app = NexaPy()
    app.mount_workflows(prefix="/workflows", hmac_secret=secret)
    client = TestClient(app.fastapi)

    payload = {"account": "user1", "amount": 100}
    payload_bytes = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")

    # 1. Legacy signature without timestamp/nonce -> 403 Forbidden
    legacy_sig = hashlib.sha256(secret.encode("utf-8") + payload_bytes).hexdigest()
    res_legacy = client.post(
        "/workflows/secure_payout",
        content=payload_bytes,
        headers={"Content-Type": "application/json", "X-NexaPy-Signature": f"sha256={legacy_sig}"},
    )
    assert res_legacy.status_code == 403

    # 2. Strict HMAC-v2 signature with timestamp & nonce -> 200 OK
    ts = str(int(time.time()))
    nonce = "wf_nonce_test_01"
    v2_sig = HMACVerifier.compute_signature_v2(
        secret=secret,
        timestamp=ts,
        nonce=nonce,
        method="POST",
        path="/workflows/secure_payout",
        payload=payload_bytes,
    )
    res_v2 = client.post(
        "/workflows/secure_payout",
        content=payload_bytes,
        headers={
            "Content-Type": "application/json",
            "X-NexaPy-Signature": f"sha256={v2_sig}",
            "X-NexaPy-Timestamp": ts,
            "X-NexaPy-Nonce": nonce,
        },
    )
    assert res_v2.status_code == 200
    assert res_v2.json()["result"]["paid"] is True


def test_p0_03_notification_action_produces_strict_hmac_v2():
    """
    P0-03: NotificationAction.send_webhook produces standard HMAC-v2 signatures
    which are verified successfully by strict verifiers.
    """
    secret = "strict_webhook_secret"
    payload = {"event": "invoice_paid", "invoice_id": "INV-400"}
    action = NotificationAction()

    captured_url = None
    captured_content = None
    captured_headers = None

    async def mock_post(url, content=None, headers=None, **kwargs):
        nonlocal captured_url, captured_content, captured_headers
        captured_url = url
        captured_content = content
        captured_headers = headers
        mock_res = AsyncMock()
        mock_res.is_success = True
        return mock_res

    with patch("httpx.AsyncClient.post", side_effect=mock_post):
        target_url = "http://my-service.local/webhooks/incoming"
        success = asyncio.run(action.send_webhook(target_url, payload=payload, secret=secret))
        assert success is True

        sig = captured_headers["X-NexaPy-Signature"]
        ts = captured_headers["X-NexaPy-Timestamp"]
        nonce = captured_headers["X-NexaPy-Nonce"]

        # Verify against strict verifier with enforce_timestamp=True, enforce_nonce=True, allow_legacy=False
        is_verified = HMACVerifier.verify_signature(
            secret=secret,
            payload=captured_content,
            signature=sig,
            timestamp=ts,
            nonce=nonce,
            method="POST",
            path="/webhooks/incoming",
            enforce_timestamp=True,
            enforce_nonce=True,
            allow_legacy=False,
        )
        assert is_verified is True


# ============================================================================
# P0-04: MULTI-INSTANCE JOB QUEUE ATOMIC CLAIMING
# ============================================================================

def test_p0_04_multi_instance_job_claiming_concurrency():
    """
    P0-04: Two JobQueue instances sharing the same SQLite database MUST NOT
    execute the same job handler twice. Exactly one worker claims and completes the job.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        db_path = Path(tmp_dir) / "shared_jobs.sqlite"

        # Instance 1 & Instance 2
        queue_1 = JobQueue(db_path=db_path, max_workers=1)
        queue_2 = JobQueue(db_path=db_path, max_workers=1)

        handler_calls = 0

        @workflow("atomic_task")
        def atomic_task(ctx):
            nonlocal handler_calls
            handler_calls += 1
            return {"count": handler_calls}

        async def scenario():
            # Instance 1 enqueues the job
            job = await queue_1.enqueue("atomic_task", {"test": "data"})
            job_id = job.job_id

            # Both instances attempt to claim the job concurrently
            # Simulate worker loop claiming
            claimed_by_1 = queue_1.claim_job(job_id, worker_id="worker_1")
            claimed_by_2 = queue_2.claim_job(job_id, worker_id="worker_2")

            # Exactly one must succeed, the other must receive None
            assert (claimed_by_1 is not None) ^ (claimed_by_2 is not None), (
                "Exactly one instance must win the atomic claim"
            )

            winner_queue = queue_1 if claimed_by_1 is not None else queue_2
            winner_job = claimed_by_1 or claimed_by_2

            # Winner executes the handler
            res = atomic_task(None)
            winner_queue._finalize_job(winner_job, status="COMPLETED", result=res)

            # Assert handler called EXACTLY ONCE
            assert handler_calls == 1

            # Both instances query job status from DB: both see COMPLETED
            job_status_1 = queue_1.get_job(job_id)
            job_status_2 = queue_2.get_job(job_id)

            assert job_status_1 is not None and job_status_1.status == "COMPLETED"
            assert job_status_2 is not None and job_status_2.status == "COMPLETED"
            assert job_status_1.result == {"count": 1}
            assert job_status_2.result == {"count": 1}

        asyncio.run(scenario())


def test_p0_04_idempotency_key_sqlite_unique():
    """
    P1/P0-04: Concurrent enqueue with same idempotency_key returns identical job.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        db_path = Path(tmp_dir) / "idemp_jobs.sqlite"
        q1 = JobQueue(db_path=db_path)
        q2 = JobQueue(db_path=db_path)

        @workflow("payment_charge")
        def payment_charge(ctx):
            return "paid"

        async def run_test():
            job1 = await q1.enqueue("payment_charge", {"charge": 10}, idempotency_key="pay_txn_999")
            job2 = await q2.enqueue("payment_charge", {"charge": 10}, idempotency_key="pay_txn_999")

            assert job1.job_id == job2.job_id
            assert job1.idempotency_key == "pay_txn_999"

        asyncio.run(run_test())


# ============================================================================
# P0-05, P0-06: DAG STATE ISOLATION & PERSISTENCE
# ============================================================================

def test_p0_05_dag_state_isolation_by_tenant_and_workflow():
    """
    P0-05: Two workflows sharing the same step name and same run_id MUST NOT
    collide or return each other's cached state.
    """
    dag_a = DAGWorkflow("account_A")

    @dag_a.node("profile")
    def profile_a(ctx):
        return "A-private"

    dag_b = DAGWorkflow("account_B")

    @dag_b.node("profile")
    def profile_b(ctx):
        return "B-private"

    async def scenario():
        ctx_a = WorkflowContext(input_data={"run_id": "run_collision", "tenant_id": "tenant_1"})
        res_a = await dag_a.execute_step_async("profile", ctx_a)

        assert res_a["status"] == "success"
        assert res_a["result"] == "A-private"
        assert res_a["workflow"] == "account_A"

        # Workflow B with the exact same run_id MUST NOT receive A's result
        ctx_b = WorkflowContext(input_data={"run_id": "run_collision", "tenant_id": "tenant_1"})
        res_b = await dag_b.execute_step_async("profile", ctx_b)

        assert res_b["status"] == "success"  # NOT "cached" with A's data!
        assert res_b["result"] == "B-private"
        assert res_b["workflow"] == "account_B"

    asyncio.run(scenario())


def test_p0_06_dag_state_persistence_across_restarts():
    """
    P0-06: DAG state backed by SQLite survives in-memory cache clear/restart.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        db_path = str(Path(tmp_dir) / "dag_state.sqlite")
        store = DAGStateStore(db_path=db_path)

        store.save_step("tenant_1", "wf_checkout", "run_42", "charge", {"amount": 250})

        # Clear in-memory cache to simulate restart
        store._memory_store.clear()

        # Fetch after restart: recovers from SQLite
        steps = store.get_run_steps("tenant_1", "wf_checkout", "run_42")
        assert "charge" in steps
        assert steps["charge"] == {"amount": 250}


# ============================================================================
# P0-07: N8N SIGNATURE FOR GET REQUESTS
# ============================================================================

def test_p0_07_n8n_get_request_empty_body_hash():
    """
    P0-07: Verifier and client agree that for GET/HEAD or empty requests,
    the body SHA-256 hash is computed on b'' (empty string), NOT '{}'.
    """
    secret = "hmac_key_test"
    ts = str(int(time.time()))
    nonce = "nonce_get_123"
    method = "GET"
    path = "/workflows/_schema"

    empty_sha256 = hashlib.sha256(b"").hexdigest()
    assert empty_sha256 == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

    string_to_sign = HMACVerifier.compute_string_to_sign_v2(
        timestamp=ts,
        nonce=nonce,
        method=method,
        path=path,
        payload=b"",
    )
    assert f"GET\n{path}\n{empty_sha256}" in string_to_sign

    sig = HMACVerifier.compute_signature_v2(secret, ts, nonce, method, path, b"")
    assert HMACVerifier.verify_signature(
        secret, b"", sig, timestamp=ts, nonce=nonce, method=method, path=path, enforce_timestamp=True, enforce_nonce=True
    ) is True


# ============================================================================
# P1: AI TOKEN ACCOUNTING, CEILING & BUDGET RESERVATION
# ============================================================================

def test_p1_ai_token_accounting_and_budget_reservation():
    """
    P1: AIAuthGuard tracks real provider usage tokens and rejects when budget quota exceeded.
    """
    guard = AIAuthGuard(valid_keys=["test-key"], token_limit=100, allow_anonymous=False)

    # 1. Budget reservation under limit succeeds
    ok, res_id, err = guard.reserve_budget("test-key", 50)
    assert ok is True
    assert res_id is not None

    # 2. Budget reservation exceeding remaining limit is rejected
    ok2, res_id2, err2 = guard.reserve_budget("test-key", 60)
    assert ok2 is False
    assert "budget exceeded" in err2.lower()

    # 3. Reconcile with actual provider usage (40 tokens)
    guard.reconcile_budget(res_id, "test-key", actual_tokens=40)
    assert guard.token_usage["test-key"] == 40
    assert len(guard.token_reservations) == 0

    # 4. Now a 50-token request is allowed (40 + 50 <= 100)
    ok3, res_id3, _ = guard.reserve_budget("test-key", 50)
    assert ok3 is True


# ============================================================================
# P1: SCHEDULER POSIX CRON & INTERVAL VALIDATION
# ============================================================================

def test_p1_scheduler_interval_zero_rejected():
    """
    P1: Schedule interval of 0s or 0m must raise ValueError or be rejected.
    """
    scheduler = WorkflowScheduler()
    with pytest.raises(ValueError, match="strictly positive"):
        scheduler._parse_interval_seconds("0s")

    with pytest.raises(ValueError, match="strictly positive"):
        scheduler._parse_interval_seconds("0m")


def test_p1_scheduler_posix_cron_dom_dow_evaluation():
    """
    P1: Standard POSIX cron: when both DOM and DOW are restricted, match if EITHER matches.
    """
    # June 15, 2026 (Monday: DOW=1)
    # Cron: * * 15 * 5 (15th of month OR Friday)
    # Since June 15 matches DOM=15, it must evaluate to True even though Monday != Friday
    dt = datetime.datetime(2026, 6, 15, 12, 0)
    assert matches_cron("* * 15 * 5", dt) is True

    # June 19, 2026 (Friday: DOW=5)
    # Since it is Friday, it matches DOW=5 even though 19 != 15
    dt_fri = datetime.datetime(2026, 6, 19, 12, 0)
    assert matches_cron("* * 15 * 5", dt_fri) is True

    # June 16, 2026 (Tuesday: DOW=2)
    # Neither 16 == 15 nor Tuesday == Friday -> False
    dt_tue = datetime.datetime(2026, 6, 16, 12, 0)
    assert matches_cron("* * 15 * 5", dt_tue) is False


def test_p1_n8n_schedule_interval_conversion():
    """
    P1: Converts human schedule intervals ('5m', '1h', 'every 30m') to valid 5-field cron.
    """
    assert convert_schedule_to_n8n_cron("5m") == "*/5 * * * *"
    assert convert_schedule_to_n8n_cron("every 15m") == "*/15 * * * *"
    assert convert_schedule_to_n8n_cron("1h") == "0 */1 * * *"
    assert convert_schedule_to_n8n_cron("0 8 * * *") == "0 8 * * *"


def test_p1_n8n_import_supported_vs_unsupported_nodes():
    """
    P1: import_n8n_workflow flags unsupported nodes clearly.
    """
    sample_n8n = {
        "name": "Mixed Workflow",
        "nodes": [
            {"name": "Trigger", "type": "n8n-nodes-base.webhook", "parameters": {"path": "in"}},
            {"name": "UnsupportedNode", "type": "n8n-nodes-base.unknownAction", "parameters": {}},
        ],
        "connections": {},
    }
    imported = import_n8n_workflow(sample_n8n)
    assert len(imported["unsupported_nodes"]) == 1
    assert imported["unsupported_nodes"][0]["name"] == "UnsupportedNode"

