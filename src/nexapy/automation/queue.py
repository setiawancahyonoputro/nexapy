"""Persistent Async Job Queue for background workflow execution in NexaPy."""

import asyncio
import json
import sqlite3
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
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
        }


class JobQueue:
    """
    Persistent Job Queue for background workflow execution.
    Can operate in-memory or backed by a persistent SQLite database with crash recovery and idempotency.
    """

    def __init__(self, db_path: Optional[Union[str, Path]] = None, max_workers: int = 4):
        self.db_path = str(db_path) if db_path else None
        self.max_workers = max_workers
        self._jobs: Dict[str, Job] = {}
        self._idempotency_map: Dict[str, str] = {}
        self._queue: asyncio.Queue[str] = asyncio.Queue()
        self._worker_tasks: List[asyncio.Task] = []
        self._running = False

        if self.db_path:
            self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        if not self.db_path:
            raise RuntimeError("Database path not configured.")
        return sqlite3.connect(self.db_path)

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
                    idempotency_key TEXT
                )
                """
            )
            try:
                conn.execute("ALTER TABLE nexapy_jobs ADD COLUMN idempotency_key TEXT")
            except Exception:
                pass
            conn.commit()
        finally:
            conn.close()

    def _save_job(self, job: Job) -> None:
        self._jobs[job.job_id] = job
        if job.idempotency_key:
            self._idempotency_map[job.idempotency_key] = job.job_id

        if self.db_path:
            conn = self._get_connection()
            try:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO nexapy_jobs
                    (job_id, workflow_name, input_data, status, result, error, created_at, started_at, completed_at, idempotency_key)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    ),
                )
                conn.commit()
            finally:
                conn.close()

    def recover_pending_jobs(self) -> int:
        """
        Recover QUEUED and unfinalized PROCESSING jobs from SQLite on server startup.
        Interrupted PROCESSING jobs are reset to QUEUED for retry.
        """
        if not self.db_path:
            return 0

        recovered_count = 0
        conn = self._get_connection()
        try:
            cursor = conn.execute(
                "SELECT job_id, workflow_name, input_data, status, result, error, created_at, started_at, completed_at, idempotency_key "
                "FROM nexapy_jobs WHERE status IN ('QUEUED', 'PROCESSING')"
            )
            rows = cursor.fetchall()
            for row in rows:
                job_id = row[0]
                status = "QUEUED"  # Reset interrupted PROCESSING jobs back to QUEUED
                job = Job(
                    job_id=job_id,
                    workflow_name=row[1],
                    input_data=json.loads(row[2]) if row[2] else {},
                    status=status,
                    result=json.loads(row[4]) if row[4] else None,
                    error=row[5],
                    created_at=row[6],
                    started_at=row[7],
                    completed_at=row[8],
                    idempotency_key=row[9],
                )
                self._save_job(job)
                self._queue.put_nowait(job_id)
                recovered_count += 1
            return recovered_count
        finally:
            conn.close()

    async def enqueue(
        self,
        workflow_name: str,
        input_data: Optional[Dict[str, Any]] = None,
        idempotency_key: Optional[str] = None,
    ) -> Job:
        """
        Enqueue a new workflow job for asynchronous processing.
        If an idempotency_key is provided and an existing job matches, returns the existing job.
        """
        if idempotency_key:
            # Check in-memory map
            existing_id = self._idempotency_map.get(idempotency_key)
            if existing_id:
                existing_job = self.get_job(existing_id)
                if existing_job:
                    return existing_job

            # Check persistent database
            if self.db_path:
                conn = self._get_connection()
                try:
                    cursor = conn.execute(
                        "SELECT job_id FROM nexapy_jobs WHERE idempotency_key = ?",
                        (idempotency_key,),
                    )
                    row = cursor.fetchone()
                    if row:
                        found_job = self.get_job(row[0])
                        if found_job:
                            return found_job
                finally:
                    conn.close()

        job_id = f"job_{uuid.uuid4().hex[:12]}"
        job = Job(
            job_id=job_id,
            workflow_name=workflow_name,
            input_data=input_data or {},
            status="QUEUED",
            idempotency_key=idempotency_key,
            created_at=time.time(),
        )
        self._save_job(job)
        await self._queue.put(job_id)
        return job

    def cancel_job(self, job_id: str) -> bool:
        """Cancel a queued or processing job."""
        job = self.get_job(job_id)
        if not job:
            return False
        if job.status in ("COMPLETED", "FAILED", "CANCELLED"):
            return False

        job.status = "CANCELLED"
        job.completed_at = time.time()
        job.error = "Job was cancelled by client."
        self._save_job(job)
        return True

    def get_job(self, job_id: str) -> Optional[Job]:
        """Fetch job state by ID."""
        if job_id in self._jobs:
            return self._jobs[job_id]

        if self.db_path:
            conn = self._get_connection()
            try:
                cursor = conn.execute("SELECT * FROM nexapy_jobs WHERE job_id = ?", (job_id,))
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
                    )
                    self._jobs[job_id] = job
                    if job.idempotency_key:
                        self._idempotency_map[job.idempotency_key] = job_id
                    return job
            finally:
                conn.close()
        return None

    async def _worker_loop(self) -> None:
        """Background worker consuming jobs from queue."""
        while self._running:
            try:
                job_id = await asyncio.wait_for(self._queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break

            job = self.get_job(job_id)
            if not job or job.status != "QUEUED":
                self._queue.task_done()
                continue

            job.status = "PROCESSING"
            job.started_at = time.time()
            self._save_job(job)

            try:
                res = await runner.run_async(job.workflow_name, input_data=job.input_data)
                job.status = "COMPLETED"
                job.result = res
            except Exception as err:
                job.status = "FAILED"
                job.error = str(err)
            finally:
                job.completed_at = time.time()
                self._save_job(job)
                self._queue.task_done()

    def start_workers(self) -> None:
        """Start async background worker pool and recover any pending jobs."""
        if self._running:
            return
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
