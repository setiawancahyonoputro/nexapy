"""Execution telemetry tracking and persistence adapters for NexaPy Workflows."""

import json
import sqlite3
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Union


@dataclass
class WorkflowExecutionLog:
    """Record representing a single workflow execution instance."""

    execution_id: str
    workflow_name: str
    status: str  # PENDING, RUNNING, SUCCESS, FAILED
    start_time: float
    end_time: Optional[float] = None
    duration_ms: Optional[float] = None
    input_data: Dict[str, Any] = field(default_factory=dict)
    result: Optional[Any] = None
    error: Optional[str] = None
    steps: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert execution log to clean serializable dict."""
        return {
            "execution_id": self.execution_id,
            "workflow_name": self.workflow_name,
            "status": self.status,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_ms": round(self.duration_ms, 2) if self.duration_ms is not None else None,
            "input_data": self.input_data,
            "result": self.result,
            "error": self.error,
            "steps": self.steps,
        }


class BaseStorageAdapter:
    """Base interface for storing execution telemetry logs."""

    def save(self, log: WorkflowExecutionLog) -> None:
        raise NotImplementedError

    def get(self, execution_id: str) -> Optional[WorkflowExecutionLog]:
        raise NotImplementedError

    def list_recent(self, limit: int = 50) -> List[WorkflowExecutionLog]:
        raise NotImplementedError

    def clear(self) -> None:
        raise NotImplementedError


class MemoryStorageAdapter(BaseStorageAdapter):
    """In-memory telemetry log storage."""

    def __init__(self, max_capacity: int = 1000):
        self.max_capacity = max_capacity
        self._logs: Dict[str, WorkflowExecutionLog] = {}

    def save(self, log: WorkflowExecutionLog) -> None:
        if len(self._logs) >= self.max_capacity and log.execution_id not in self._logs:
            # Remove oldest key
            oldest = next(iter(self._logs))
            del self._logs[oldest]
        self._logs[log.execution_id] = log

    def get(self, execution_id: str) -> Optional[WorkflowExecutionLog]:
        return self._logs.get(execution_id)

    def list_recent(self, limit: int = 50) -> List[WorkflowExecutionLog]:
        logs = list(self._logs.values())
        logs.sort(key=lambda x: x.start_time, reverse=True)
        return logs[:limit]

    def clear(self) -> None:
        self._logs.clear()


class SQLiteStorageAdapter(BaseStorageAdapter):
    """SQLite database backed execution log storage."""

    def __init__(self, db_path: Union[str, Path] = "nexapy_telemetry.db"):
        self.db_path = str(db_path)
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def _init_db(self) -> None:
        conn = self._get_connection()
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS executions (
                    execution_id TEXT PRIMARY KEY,
                    workflow_name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    start_time REAL NOT NULL,
                    end_time REAL,
                    duration_ms REAL,
                    input_data TEXT,
                    result TEXT,
                    error TEXT,
                    steps TEXT
                )
                """
            )
            conn.commit()
        finally:
            conn.close()

    def save(self, log: WorkflowExecutionLog) -> None:
        conn = self._get_connection()
        try:
            conn.execute(
                """
                INSERT OR REPLACE INTO executions
                (execution_id, workflow_name, status, start_time, end_time, duration_ms, input_data, result, error, steps)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    log.execution_id,
                    log.workflow_name,
                    log.status,
                    log.start_time,
                    log.end_time,
                    log.duration_ms,
                    json.dumps(log.input_data),
                    json.dumps(log.result),
                    log.error,
                    json.dumps(log.steps),
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def get(self, execution_id: str) -> Optional[WorkflowExecutionLog]:
        conn = self._get_connection()
        try:
            cursor = conn.execute("SELECT * FROM executions WHERE execution_id = ?", (execution_id,))
            row = cursor.fetchone()
            if not row:
                return None
            return self._row_to_log(row)
        finally:
            conn.close()

    def list_recent(self, limit: int = 50) -> List[WorkflowExecutionLog]:
        conn = self._get_connection()
        try:
            cursor = conn.execute("SELECT * FROM executions ORDER BY start_time DESC LIMIT ?", (limit,))
            rows = cursor.fetchall()
            return [self._row_to_log(row) for row in rows]
        finally:
            conn.close()

    def clear(self) -> None:
        conn = self._get_connection()
        try:
            conn.execute("DELETE FROM executions")
            conn.commit()
        finally:
            conn.close()

    def _row_to_log(self, row: tuple) -> WorkflowExecutionLog:
        return WorkflowExecutionLog(
            execution_id=row[0],
            workflow_name=row[1],
            status=row[2],
            start_time=row[3],
            end_time=row[4],
            duration_ms=row[5],
            input_data=json.loads(row[6]) if row[6] else {},
            result=json.loads(row[7]) if row[7] else None,
            error=row[8],
            steps=json.loads(row[9]) if row[9] else {},
        )


SENSITIVE_KEY_PATTERNS = {
    "password",
    "token",
    "secret",
    "api_key",
    "apikey",
    "authorization",
    "auth",
    "access_token",
    "refresh_token",
    "private_key",
    "credit_card",
    "cvv",
    "ssn",
    "client_secret",
}


def mask_sensitive_data(data: Any, mask_value: str = "********") -> Any:
    """
    Recursively sanitize dictionaries and lists, masking sensitive fields (passwords, tokens, keys).
    """
    if isinstance(data, dict):
        sanitized = {}
        for k, v in data.items():
            k_lower = str(k).lower().replace("-", "_")
            if any(pattern in k_lower for pattern in SENSITIVE_KEY_PATTERNS):
                sanitized[k] = mask_value
            else:
                sanitized[k] = mask_sensitive_data(v, mask_value=mask_value)
        return sanitized
    elif isinstance(data, list):
        return [mask_sensitive_data(item, mask_value=mask_value) for item in data]
    elif isinstance(data, tuple):
        return tuple(mask_sensitive_data(item, mask_value=mask_value) for item in data)
    return data


class ExecutionTracker:
    """Tracker for workflow execution telemetry."""

    def __init__(self, storage: Optional[BaseStorageAdapter] = None):
        self.storage = storage or MemoryStorageAdapter()

    def set_storage(self, storage: BaseStorageAdapter) -> None:
        """Dynamically configure the telemetry storage adapter (e.g. SQLiteStorageAdapter)."""
        self.storage = storage

    def start_execution(self, workflow_name: str, input_data: Dict[str, Any]) -> WorkflowExecutionLog:
        exec_id = f"exec_{uuid.uuid4().hex[:12]}"
        safe_input = mask_sensitive_data(input_data)
        log = WorkflowExecutionLog(
            execution_id=exec_id,
            workflow_name=workflow_name,
            status="RUNNING",
            start_time=time.time(),
            input_data=safe_input,
        )
        self.storage.save(log)
        return log

    def finish_execution(
        self,
        log: WorkflowExecutionLog,
        result: Any = None,
        error: Optional[str] = None,
        steps: Optional[Dict[str, Any]] = None,
    ) -> WorkflowExecutionLog:
        log.end_time = time.time()
        log.duration_ms = (log.end_time - log.start_time) * 1000
        log.result = mask_sensitive_data(result)
        log.error = error
        log.status = "SUCCESS" if error is None else "FAILED"
        if steps is not None:
            log.steps = mask_sensitive_data(steps)
        self.storage.save(log)
        return log


# Global tracker singleton
tracker = ExecutionTracker()

