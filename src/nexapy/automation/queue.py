"""Persistent Async Job Queue for background workflow execution in NexaPy."""

import asyncio
import hashlib
import json
import sqlite3
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
from .runner import runner


@dataclass
class Job:
    """Represents an asynchronous workflow background job."""

    job_id: str
    workflow_name: str
    input_data: Dict[str, Any] = field(default_factory=dict)
    status: str = "QUEUED"  # QUEUED, PROCESSING, COMPLETED, FAILED, CANCELLED
    result: Optional[Any] = None
    error: Optional[str] = None
    idempotency_key: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    worker_id: Optional[str] = None
    claim_token: Optional[str] = None
    lease_expires_at: Optional[float] = None
    tenant_id: str = "default"
    security_context: Optional[Dict[str, Any]] = None
    payload_hash: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "job_id": self.job_id,
            "workflow_name": self.workflow_name,
            "input_data": self.input_data,
            "status": self.status,
            "result": self.result,
            "error": self.error,
            "idempotency_key": self.idempotency_key,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "worker_id": self.worker_id,
            "claim_token": self.claim_token,
            "lease_expires_at": self.lease_expires_at,
            "tenant_id": self.tenant_id,
            "security_context": self.security_context,
            "payload_hash": self.payload_hash,
        }


class JobQueue:
    """
    Persistent Job Queue for background workflow execution.
    Can operate in-memory or backed by a persistent SQLite database with atomic multi-instance claiming,
    crash recovery, and idempotency guarantees.
    """

    def __init__(self, db_path: Optional[Union[str, Path]] = None, max_workers: int = 4):
        self.db_path = str(db_path) if db_path else None
        self.max_workers = max_workers
        self.worker_id = f"worker_{uuid.uuid4().hex[:8]}"
        self._jobs: Dict[str, Job] = {}
        self._idempotency_map: Dict[str, str] = {}
        self._queue: asyncio.Queue[str] = asyncio.Queue()
        self._worker_tasks: List[asyncio.Task] = []
        self._active_tasks: Dict[str, asyncio.Task] = {}
        self._running = False

        if self.db_path:
            self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        if not self.db_path:
            raise RuntimeError("Database path not configured.")
        # Timeout set to 10 seconds for multi-instance SQLite concurrent access
        return sqlite3.connect(self.db_path, timeout=10.0)

    def _init_db(self) -> None:
        conn = self._get_connection()
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS nexapy_jobs (
                    job_id TEXT PRIMARY KEY,
                    workflow_name TEXT NOT NULL,
                    input_data TEXT,
                    status TEXT NOT NULL,
                    result TEXT,
                    error TEXT,
                    created_at REAL NOT NULL,
                    started_at REAL,
                    completed_at REAL,
                    idempotency_key TEXT,
                    worker_id TEXT,
                    claim_token TEXT,
                    lease_expires_at REAL,
                    tenant_id TEXT,
                    security_context TEXT
                )
                """
            )
            for col, col_type in [
                ("idempotency_key", "TEXT"),
                ("worker_id", "TEXT"),
                ("claim_token", "TEXT"),
                ("lease_expires_at", "REAL"),
                ("tenant_id", "TEXT"),
                ("security_context", "TEXT"),
                ("payload_hash", "TEXT"),
            ]:
                try:
                    conn.execute(f"ALTER TABLE nexapy_jobs ADD COLUMN {col} {col_type}")
                except Exception:
                    pass

            # P0-03: Scope idempotency uniqueness per tenant
            conn.execute("DROP INDEX IF EXISTS idx_nexapy_jobs_idempotency")
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_nexapy_jobs_tenant_idempotency "
                "ON nexapy_jobs(tenant_id, idempotency_key) WHERE idempotency_key IS NOT NULL"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_nexapy_jobs_status_created "
                "ON nexapy_jobs(status, created_at)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_nexapy_jobs_status_lease "
                "ON nexapy_jobs(status, lease_expires_at)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_nexapy_jobs_tenant "
                "ON nexapy_jobs(tenant_id)"
            )
            conn.commit()
        finally:
            conn.close()

    def _save_job(self, job: Job) -> None:
        self._jobs[job.job_id] = job
        if job.idempotency_key:
            eff_tenant = job.tenant_id or "default"
            self._idempotency_map[(eff_tenant, job.idempotency_key)] = (job.job_id, job.payload_hash or "", job.workflow_name)

        if self.db_path:
            conn = self._get_connection()
            try:
                conn.execute(
                    """
                    INSERT INTO nexapy_jobs
                    (job_id, workflow_name, input_data, status, result, error, created_at, started_at, completed_at, idempotency_key, worker_id, claim_token, lease_expires_at, tenant_id, security_context, payload_hash)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(job_id) DO UPDATE SET
                        workflow_name=excluded.workflow_name,
                        input_data=excluded.input_data,
                        status=excluded.status,
                        result=excluded.result,
                        error=excluded.error,
                        started_at=excluded.started_at,
                        completed_at=excluded.completed_at,
                        idempotency_key=excluded.idempotency_key,
                        worker_id=excluded.worker_id,
                        claim_token=excluded.claim_token,
                        lease_expires_at=excluded.lease_expires_at,
                        tenant_id=excluded.tenant_id,
                        security_context=excluded.security_context,
                        payload_hash=excluded.payload_hash
                    """,
                    (
                        job.job_id,
                        job.workflow_name,
                        json.dumps(job.input_data),
                        job.status,
                        json.dumps(job.result) if job.result is not None else None,
                        job.error,
                        job.created_at,
                        job.started_at,
                        job.completed_at,
                        job.idempotency_key,
                        job.worker_id,
                        job.claim_token,
                        job.lease_expires_at,
                        job.tenant_id,
                        json.dumps(job.security_context) if job.security_context is not None else None,
                        job.payload_hash,
                    ),
                )
                conn.commit()
            finally:
                conn.close()

    def recover_pending_jobs(self, lease_seconds: float = 60.0) -> int:
        """
        Recover QUEUED and expired PROCESSING jobs from SQLite on server startup.
        Active PROCESSING jobs currently leased by another worker are NOT touched (P0 Finding V5 & V6).
        Conditional atomic updates guarantee no collision with active or newly claimed jobs (P0-04 Finding V7).
        """
        if not self.db_path:
            return 0

        recovered_count = 0
        conn = self._get_connection()
        try:
            conn.execute("BEGIN IMMEDIATE")
            now = time.time()
            cursor = conn.execute(
                """
                SELECT job_id, workflow_name, input_data, status, result, error, created_at, started_at, completed_at, idempotency_key, worker_id, claim_token, lease_expires_at, tenant_id, security_context
                FROM nexapy_jobs
                WHERE status = 'QUEUED' OR (status = 'PROCESSING' AND (
                    (lease_expires_at IS NOT NULL AND lease_expires_at < ?) OR
                    (lease_expires_at IS NULL AND (started_at IS NULL OR started_at < ?))
                ))
                """,
                (now, now - lease_seconds),
            )
            rows = cursor.fetchall()
            for row in rows:
                job_id = row[0]
                orig_status = row[3]
                orig_token = row[11]

                if orig_status == "PROCESSING":
                    # Conditional atomic reset: only reset if still PROCESSING and holding orig_token
                    if orig_token is not None:
                        cur = conn.execute(
                            """
                            UPDATE nexapy_jobs
                            SET status = 'QUEUED', started_at = NULL, worker_id = NULL, claim_token = NULL, lease_expires_at = NULL
                            WHERE job_id = ? AND status = 'PROCESSING' AND claim_token = ?
                            """,
                            (job_id, orig_token),
                        )
                    else:
                        cur = conn.execute(
                            """
                            UPDATE nexapy_jobs
                            SET status = 'QUEUED', started_at = NULL, worker_id = NULL, claim_token = NULL, lease_expires_at = NULL
                            WHERE job_id = ? AND status = 'PROCESSING' AND claim_token IS NULL
                            """,
                            (job_id,),
                        )
                    if cur.rowcount == 0:
                        # Another worker claimed or updated this job; do NOT recover!
                        continue
                elif orig_status == "QUEUED":
                    # Verify it has not been claimed
                    cur = conn.execute("SELECT 1 FROM nexapy_jobs WHERE job_id = ? AND status = 'QUEUED'", (job_id,))
                    if not cur.fetchone():
                        continue

                job = Job(
                    job_id=job_id,
                    workflow_name=row[1],
                    input_data=json.loads(row[2]) if row[2] else {},
                    status="QUEUED",
                    result=json.loads(row[4]) if row[4] else None,
                    error=row[5],
                    created_at=row[6],
                    started_at=None,
                    completed_at=row[8],
                    idempotency_key=row[9],
                    worker_id=None,
                    claim_token=None,
                    lease_expires_at=None,
                    tenant_id=row[13] if len(row) > 13 and row[13] else "default",
                    security_context=json.loads(row[14]) if len(row) > 14 and row[14] else None,
                )
                self._jobs[job_id] = job
                self._queue.put_nowait(job_id)
                recovered_count += 1
            conn.commit()
            return recovered_count
        except Exception:
            conn.rollback()
            return 0
        finally:
            conn.close()

    async def enqueue(
        self,
        workflow_name: str,
        input_data: Optional[Dict[str, Any]] = None,
        idempotency_key: Optional[str] = None,
        tenant_id: str = "default",
        security_context: Optional[Dict[str, Any]] = None,
    ) -> Job:
        """
        Enqueue a new workflow job for asynchronous processing.
        Idempotency keys are strictly scoped per (tenant_id, idempotency_key) (P0-03).
        Payload conflict detection raises ValueError if key is reused with different inputs.
        """
        eff_tenant = tenant_id or "default"
        payload_bytes = json.dumps(input_data or {}, sort_keys=True).encode("utf-8")
        current_payload_hash = hashlib.sha256(payload_bytes).hexdigest()

        if idempotency_key:
            # Check persistent database first (strictly scoped by tenant_id)
            if self.db_path:
                conn = self._get_connection()
                try:
                    cursor = conn.execute(
                        "SELECT job_id, workflow_name, payload_hash, input_data FROM nexapy_jobs WHERE tenant_id = ? AND idempotency_key = ?",
                        (eff_tenant, idempotency_key),
                    )
                    row = cursor.fetchone()
                    if row:
                        found_id, prev_wf, prev_hash = row[0], row[1], row[2]
                        if prev_wf != workflow_name:
                            raise ValueError(
                                f"Idempotency conflict: key '{idempotency_key}' was previously used for workflow '{prev_wf}', but requested for '{workflow_name}'."
                            )
                        if prev_hash and prev_hash != current_payload_hash:
                            raise ValueError(
                                f"Idempotency conflict: key '{idempotency_key}' was previously used with different payload."
                            )
                        found_job = self.get_job(found_id)
                        if found_job:
                            return found_job
                finally:
                    conn.close()

            # Check in-memory map (strictly scoped by tenant_id)
            mem_entry = self._idempotency_map.get((eff_tenant, idempotency_key))
            if mem_entry:
                existing_id, prev_hash, prev_wf = mem_entry
                if prev_wf != workflow_name:
                    raise ValueError(
                        f"Idempotency conflict: key '{idempotency_key}' was previously used for workflow '{prev_wf}', but requested for '{workflow_name}'."
                    )
                if prev_hash and prev_hash != current_payload_hash:
                    raise ValueError(
                        f"Idempotency conflict: key '{idempotency_key}' was previously used with different payload."
                    )
                existing_job = self.get_job(existing_id)
                if existing_job:
                    return existing_job

        job_id = f"job_{uuid.uuid4().hex[:12]}"
        now = time.time()
        job = Job(
            job_id=job_id,
            workflow_name=workflow_name,
            input_data=input_data or {},
            status="QUEUED",
            idempotency_key=idempotency_key,
            created_at=now,
            tenant_id=eff_tenant,
            security_context=security_context,
            payload_hash=current_payload_hash,
        )

        if self.db_path and idempotency_key:
            conn = self._get_connection()
            try:
                conn.execute("BEGIN IMMEDIATE")
                cursor = conn.execute(
                    "SELECT job_id, workflow_name, payload_hash FROM nexapy_jobs WHERE tenant_id = ? AND idempotency_key = ?",
                    (eff_tenant, idempotency_key),
                )
                row = cursor.fetchone()
                if row:
                    conn.rollback()
                    found_id, prev_wf, prev_hash = row[0], row[1], row[2]
                    if prev_wf != workflow_name or (prev_hash and prev_hash != current_payload_hash):
                        raise ValueError(f"Idempotency conflict: key '{idempotency_key}' was previously used with different parameters.")
                    found = self.get_job(found_id)
                    if found:
                        return found
                conn.execute(
                    """
                    INSERT INTO nexapy_jobs
                    (job_id, workflow_name, input_data, status, result, error, created_at, started_at, completed_at, idempotency_key, worker_id, claim_token, lease_expires_at, tenant_id, security_context, payload_hash)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        job.job_id,
                        job.workflow_name,
                        json.dumps(job.input_data),
                        job.status,
                        None,
                        None,
                        job.created_at,
                        None,
                        None,
                        job.idempotency_key,
                        None,
                        None,
                        None,
                        job.tenant_id,
                        json.dumps(job.security_context) if job.security_context is not None else None,
                        job.payload_hash,
                    ),
                )
                conn.commit()
                self._jobs[job.job_id] = job
                self._idempotency_map[(eff_tenant, idempotency_key)] = (job.job_id, current_payload_hash, workflow_name)
                await self._queue.put(job_id)
                return job
            except sqlite3.IntegrityError:
                conn.rollback()
                cursor = conn.execute(
                    "SELECT job_id, workflow_name, payload_hash FROM nexapy_jobs WHERE tenant_id = ? AND idempotency_key = ?",
                    (eff_tenant, idempotency_key),
                )
                row = cursor.fetchone()
                if row:
                    found_id, prev_wf, prev_hash = row[0], row[1], row[2]
                    if prev_wf != workflow_name or (prev_hash and prev_hash != current_payload_hash):
                        raise ValueError(f"Idempotency conflict: key '{idempotency_key}' was previously used with different parameters.")
                    found = self.get_job(found_id)
                    if found:
                        return found
            finally:
                conn.close()

        self._save_job(job)
        await self._queue.put(job_id)
        return job

    def cancel_job(self, job_id: str) -> bool:
        """Cancel a queued or processing job immediately and terminally."""
        now = time.time()
        if self.db_path:
            conn = self._get_connection()
            try:
                cursor = conn.execute(
                    """
                    UPDATE nexapy_jobs
                    SET status = 'CANCELLED', completed_at = ?, error = 'Job was cancelled by client.'
                    WHERE job_id = ? AND status IN ('QUEUED', 'PROCESSING')
                    """,
                    (now, job_id),
                )
                conn.commit()
                cancelled = cursor.rowcount > 0
            finally:
                conn.close()

            if not cancelled:
                return False

            if job_id in self._jobs:
                self._jobs[job_id].status = "CANCELLED"
                self._jobs[job_id].completed_at = now
                self._jobs[job_id].error = "Job was cancelled by client."

            active_task = self._active_tasks.get(job_id)
            if active_task and not active_task.done():
                active_task.cancel()

            return True

        # In-memory only
        job = self._jobs.get(job_id)
        if not job or job.status in ("COMPLETED", "FAILED", "CANCELLED"):
            return False

        job.status = "CANCELLED"
        job.completed_at = now
        job.error = "Job was cancelled by client."
        self._jobs[job.job_id] = job

        active_task = self._active_tasks.get(job_id)
        if active_task and not active_task.done():
            active_task.cancel()

        return True

    def heartbeat(
        self,
        job_id: str,
        worker_id: Optional[str] = None,
        claim_token: Optional[str] = None,
        extension_seconds: float = 60.0,
    ) -> bool:
        """
        Extend the lease of an actively processing job.
        Guarantees long-running jobs are not reclaimed or recovered by other workers.
        Returns True if lease was extended, False if job was cancelled or claimed by another worker.
        """
        now = time.time()
        new_lease_expires = now + extension_seconds

        if not self.db_path:
            job = self._jobs.get(job_id)
            if not job or job.status != "PROCESSING":
                return False
            if claim_token and job.claim_token != claim_token:
                return False
            if worker_id and job.worker_id != worker_id:
                return False
            job.lease_expires_at = new_lease_expires
            return True

        conn = self._get_connection()
        try:
            sql = "UPDATE nexapy_jobs SET lease_expires_at = ? WHERE job_id = ? AND status = 'PROCESSING'"
            params: List[Any] = [new_lease_expires, job_id]
            if worker_id:
                sql += " AND worker_id = ?"
                params.append(worker_id)
            if claim_token:
                sql += " AND claim_token = ?"
                params.append(claim_token)

            cursor = conn.execute(sql, params)
            conn.commit()
            if cursor.rowcount > 0:
                if job_id in self._jobs:
                    self._jobs[job_id].lease_expires_at = new_lease_expires
                return True
            return False
        except Exception:
            return False
        finally:
            conn.close()

    def claim_job(self, job_id: str, worker_id: str = "worker", lease_seconds: float = 60.0) -> Optional[Job]:
        """
        Atomically claim a specific job by ID if QUEUED or lease expired.
        Guarantees that across multiple instances, only one worker can process the job.
        """
        now = time.time()
        lease_expires = now + lease_seconds

        if not self.db_path:
            job = self._jobs.get(job_id)
            if not job:
                return None
            if job.status == "PROCESSING":
                if job.lease_expires_at is not None and job.lease_expires_at >= now:
                    return None  # Lease still active!
            elif job.status != "QUEUED":
                return None

            job.status = "PROCESSING"
            job.started_at = now
            job.worker_id = worker_id
            job.claim_token = uuid.uuid4().hex
            job.lease_expires_at = lease_expires
            return job

        conn = self._get_connection()
        try:
            conn.execute("BEGIN IMMEDIATE")
            claim_token = uuid.uuid4().hex
            cursor = conn.execute(
                """
                UPDATE nexapy_jobs
                SET status = 'PROCESSING', started_at = ?, worker_id = ?, claim_token = ?, lease_expires_at = ?
                WHERE job_id = ? AND (
                    status = 'QUEUED' OR (status = 'PROCESSING' AND (
                        (lease_expires_at IS NOT NULL AND lease_expires_at < ?) OR
                        (lease_expires_at IS NULL AND (started_at IS NULL OR started_at < ?))
                    ))
                )
                """,
                (now, worker_id, claim_token, lease_expires, job_id, now, now - lease_seconds),
            )
            if cursor.rowcount == 0:
                conn.rollback()
                return None

            cursor = conn.execute(
                "SELECT job_id, workflow_name, input_data, status, result, error, created_at, started_at, completed_at, idempotency_key, worker_id, claim_token, lease_expires_at, tenant_id, security_context, payload_hash "
                "FROM nexapy_jobs WHERE job_id = ?",
                (job_id,),
            )
            row = cursor.fetchone()
            conn.commit()
            if not row:
                return None
            job = Job(
                job_id=row[0],
                workflow_name=row[1],
                input_data=json.loads(row[2]) if row[2] else {},
                status=row[3],
                result=json.loads(row[4]) if row[4] else None,
                error=row[5],
                created_at=row[6],
                started_at=row[7],
                completed_at=row[8],
                idempotency_key=row[9],
                worker_id=row[10],
                claim_token=row[11],
                lease_expires_at=row[12] if len(row) > 12 else None,
                tenant_id=row[13] if len(row) > 13 and row[13] else "default",
                security_context=json.loads(row[14]) if len(row) > 14 and row[14] else None,
                payload_hash=row[15] if len(row) > 15 else None,
            )
            self._jobs[job_id] = job
            return job
        except Exception:
            conn.rollback()
            return None
        finally:
            conn.close()

    def claim_next_job(self, worker_id: str = "worker", lease_seconds: float = 60.0) -> Optional[Job]:
        """
        Atomically claim the next pending QUEUED job or expired lease job in SQLite.
        Uses BEGIN IMMEDIATE to prevent race conditions across multiple instances.
        """
        if not self.db_path:
            return None
        conn = self._get_connection()
        try:
            conn.execute("BEGIN IMMEDIATE")
            now = time.time()
            lease_expires = now + lease_seconds
            claim_token = uuid.uuid4().hex
            cursor = conn.execute(
                """
                SELECT job_id
                FROM nexapy_jobs
                WHERE status = 'QUEUED' OR (status = 'PROCESSING' AND (
                    (lease_expires_at IS NOT NULL AND lease_expires_at < ?) OR
                    (lease_expires_at IS NULL AND (started_at IS NULL OR started_at < ?))
                ))
                ORDER BY created_at ASC LIMIT 1
                """,
                (now, now - lease_seconds),
            )
            row = cursor.fetchone()
            if not row:
                conn.commit()
                return None

            job_id = row[0]
            cursor = conn.execute(
                """
                UPDATE nexapy_jobs
                SET status = 'PROCESSING', started_at = ?, worker_id = ?, claim_token = ?, lease_expires_at = ?
                WHERE job_id = ? AND (
                    status = 'QUEUED' OR (status = 'PROCESSING' AND (
                        (lease_expires_at IS NOT NULL AND lease_expires_at < ?) OR
                        (lease_expires_at IS NULL AND (started_at IS NULL OR started_at < ?))
                    ))
                )
                """,
                (now, worker_id, claim_token, lease_expires, job_id, now, now - lease_seconds),
            )
            if cursor.rowcount == 0:
                conn.rollback()
                return None

            cursor = conn.execute(
                "SELECT job_id, workflow_name, input_data, status, result, error, created_at, started_at, completed_at, idempotency_key, worker_id, claim_token, lease_expires_at, tenant_id, security_context, payload_hash "
                "FROM nexapy_jobs WHERE job_id = ?",
                (job_id,),
            )
            row = cursor.fetchone()
            conn.commit()
            if not row:
                return None
            job = Job(
                job_id=row[0],
                workflow_name=row[1],
                input_data=json.loads(row[2]) if row[2] else {},
                status=row[3],
                result=json.loads(row[4]) if row[4] else None,
                error=row[5],
                created_at=row[6],
                started_at=row[7],
                completed_at=row[8],
                idempotency_key=row[9],
                worker_id=row[10],
                claim_token=row[11],
                lease_expires_at=row[12] if len(row) > 12 else None,
                tenant_id=row[13] if len(row) > 13 and row[13] else "default",
                security_context=json.loads(row[14]) if len(row) > 14 and row[14] else None,
                payload_hash=row[15] if len(row) > 15 else None,
            )
            self._jobs[job_id] = job
            return job
        except Exception:
            conn.rollback()
            return None
        finally:
            conn.close()

    def _finalize_job(self, job: Job, status: str, result: Any = None, error: Optional[str] = None) -> bool:
        """
        Finalize job state atomically, verifying claim token and status = 'PROCESSING'.
        Cancelled jobs will NOT be overwritten by COMPLETED (P0 Finding V6).
        """
        now = time.time()

        if self.db_path:
            conn = self._get_connection()
            try:
                if job.claim_token:
                    cursor = conn.execute(
                        """
                        UPDATE nexapy_jobs
                        SET status = ?, result = ?, error = ?, completed_at = ?
                        WHERE job_id = ? AND claim_token = ? AND status = 'PROCESSING'
                        """,
                        (
                            status,
                            json.dumps(result) if result is not None else None,
                            error,
                            now,
                            job.job_id,
                            job.claim_token,
                        ),
                    )
                    conn.commit()
                    success = cursor.rowcount > 0
                else:
                    cursor = conn.execute(
                        """
                        UPDATE nexapy_jobs
                        SET status = ?, result = ?, error = ?, completed_at = ?
                        WHERE job_id = ? AND status = 'PROCESSING'
                        """,
                        (
                            status,
                            json.dumps(result) if result is not None else None,
                            error,
                            now,
                            job.job_id,
                        ),
                    )
                    conn.commit()
                    success = cursor.rowcount > 0

                if success:
                    job.status = status
                    job.result = result
                    job.error = error
                    job.completed_at = now
                    self._jobs[job.job_id] = job
                    return True
                else:
                    self.get_job(job.job_id)
                    return False
            except Exception:
                return False
            finally:
                conn.close()

        # In-memory only
        in_memory_job = self._jobs.get(job.job_id)
        if not in_memory_job or in_memory_job.status != "PROCESSING":
            return False

        in_memory_job.status = status
        in_memory_job.result = result
        in_memory_job.error = error
        in_memory_job.completed_at = now
        return True

    def get_job(self, job_id: str) -> Optional[Job]:
        """
        Fetch authoritative job state by ID.
        Always queries persistent SQLite first when configured to prevent stale in-memory reads.
        """
        if self.db_path:
            conn = self._get_connection()
            try:
                cursor = conn.execute(
                    "SELECT job_id, workflow_name, input_data, status, result, error, created_at, started_at, completed_at, idempotency_key, worker_id, claim_token, lease_expires_at, tenant_id, security_context, payload_hash "
                    "FROM nexapy_jobs WHERE job_id = ?",
                    (job_id,),
                )
                row = cursor.fetchone()
                if row:
                    job = Job(
                        job_id=row[0],
                        workflow_name=row[1],
                        input_data=json.loads(row[2]) if row[2] else {},
                        status=row[3],
                        result=json.loads(row[4]) if row[4] else None,
                        error=row[5],
                        created_at=row[6],
                        started_at=row[7],
                        completed_at=row[8],
                        idempotency_key=row[9] if len(row) > 9 else None,
                        worker_id=row[10] if len(row) > 10 else None,
                        claim_token=row[11] if len(row) > 11 else None,
                        lease_expires_at=row[12] if len(row) > 12 else None,
                        tenant_id=row[13] if len(row) > 13 and row[13] else "default",
                        security_context=json.loads(row[14]) if len(row) > 14 and row[14] else None,
                        payload_hash=row[15] if len(row) > 15 else None,
                    )
                    self._jobs[job_id] = job
                    if job.idempotency_key:
                        self._idempotency_map[(job.tenant_id or "default", job.idempotency_key)] = (job_id, job.payload_hash or "", job.workflow_name)
                    return job
            finally:
                conn.close()

        if job_id in self._jobs:
            return self._jobs[job_id]

        return None

    async def _worker_loop(self) -> None:
        """Background worker consuming jobs with atomic claim enforcement and periodic heartbeat."""
        while self._running:
            claimed_job: Optional[Job] = None
            job_id_from_queue: Optional[str] = None

            try:
                job_id_from_queue = await asyncio.wait_for(self._queue.get(), timeout=0.5)
            except asyncio.TimeoutError:
                if self.db_path:
                    claimed_job = self.claim_next_job(worker_id=self.worker_id)
            except asyncio.CancelledError:
                break

            if job_id_from_queue:
                claimed_job = self.claim_job(job_id_from_queue, worker_id=self.worker_id)
                self._queue.task_done()

            if not claimed_job:
                continue

            job_id = claimed_job.job_id
            current_task = asyncio.current_task()
            if current_task:
                self._active_tasks[job_id] = current_task

            # Start background heartbeat task during job execution
            async def _heartbeat_worker(jid: str, wid: str, token: Optional[str]) -> None:
                try:
                    while True:
                        await asyncio.sleep(15.0)
                        extended = self.heartbeat(jid, worker_id=wid, claim_token=token, extension_seconds=60.0)
                        if not extended:
                            break
                except asyncio.CancelledError:
                    pass

            hb_task = asyncio.create_task(_heartbeat_worker(job_id, self.worker_id, claimed_job.claim_token))

            try:
                res = await runner.run_async(
                    claimed_job.workflow_name,
                    input_data=claimed_job.input_data,
                    security_context=claimed_job.security_context,
                )

                latest_job = self.get_job(job_id)
                if latest_job and latest_job.status == "CANCELLED":
                    pass
                else:
                    self._finalize_job(claimed_job, status="COMPLETED", result=res)
            except asyncio.CancelledError:
                self._finalize_job(claimed_job, status="CANCELLED", error="Job execution was cancelled.")
            except Exception as err:
                latest_job = self.get_job(job_id)
                if latest_job and latest_job.status == "CANCELLED":
                    pass
                else:
                    self._finalize_job(claimed_job, status="FAILED", error=str(err))
            finally:
                hb_task.cancel()
                try:
                    await hb_task
                except asyncio.CancelledError:
                    pass
                self._active_tasks.pop(job_id, None)

    def start_workers(self) -> None:
        """Start async background worker pool and recover any pending jobs."""
        if self._running:
            return
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return  # Defer worker task creation until event loop is active

        if self.db_path:
            self.recover_pending_jobs()
        self._running = True
        for _ in range(self.max_workers):
            task = asyncio.create_task(self._worker_loop())
            self._worker_tasks.append(task)

    async def stop_workers(self) -> None:
        """Stop worker pool and wait for active tasks."""
        self._running = False
        for task in self._worker_tasks:
            task.cancel()
        if self._worker_tasks:
            await asyncio.gather(*self._worker_tasks, return_exceptions=True)
            self._worker_tasks.clear()


# Global default job queue
job_queue = JobQueue()
