"""Multi-step DAG (Directed Acyclic Graph) Workflow Execution Engine."""

import asyncio
import inspect
import json
import re
import sqlite3
import threading
import time
import uuid
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union
from .context import WorkflowContext


class DAGNode:
    """Represents a single node step in a DAG Workflow."""

    def __init__(
        self,
        name: str,
        func: Callable[..., Any],
        depends_on: Optional[List[str]] = None,
        description: Optional[str] = None,
        max_retries: int = 0,
        retry_delay: float = 1.0,
        condition: Optional[Callable[[WorkflowContext], bool]] = None,
    ):
        self.name = name
        self.func = func
        self.depends_on = depends_on or []
        self.description = description or func.__doc__ or f"Node {name}"
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.condition = condition

    def __repr__(self) -> str:
        return f"<DAGNode name='{self.name}' depends_on={self.depends_on}>"


class DAGStateStore:
    """
    Isolated state store for DAG workflows.
    Partitions state strictly by (tenant_id, workflow_name, run_id) to prevent cross-workflow
    and cross-tenant state leaks.
    Supports SQLite persistence, TTL expiry, and backward-compatible dict methods.
    """

    def __init__(self, db_path: Optional[str] = None, ttl_seconds: float = 86400.0):
        self.db_path = db_path
        self.ttl_seconds = ttl_seconds
        self._memory_store: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
        self._timestamps: Dict[Tuple[str, str, str], float] = {}
        self._step_records: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
        self._lock = threading.Lock()
        # Backwards compatibility map for direct run_id keying
        self._legacy_map: Dict[str, Dict[str, Any]] = {}
        if self.db_path:
            self._init_db()

    def configure(self, db_path: str, ttl_seconds: float = 86400.0) -> None:
        """Configure SQLite persistence backend for DAG state engine."""
        self.db_path = db_path
        self.ttl_seconds = ttl_seconds
        self._init_db()

    def _init_db(self) -> None:
        if not self.db_path:
            return
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS nexapy_dag_states (
                    tenant_id TEXT NOT NULL,
                    workflow_name TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    step_name TEXT NOT NULL,
                    result TEXT,
                    status TEXT NOT NULL,
                    claim_token TEXT,
                    error TEXT,
                    updated_at REAL NOT NULL,
                    PRIMARY KEY (tenant_id, workflow_name, run_id, step_name)
                )
                """
            )
            for col, col_type in [("claim_token", "TEXT"), ("error", "TEXT")]:
                try:
                    conn.execute(f"ALTER TABLE nexapy_dag_states ADD COLUMN {col} {col_type}")
                except Exception:
                    pass
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_dag_states_lookup "
                "ON nexapy_dag_states(tenant_id, workflow_name, run_id)"
            )
            conn.commit()
        finally:
            conn.close()

    def get_run_steps(self, tenant_id: str, workflow_name: str, run_id: str) -> Dict[str, Any]:
        """Fetch all executed step outputs for a specific tenant, workflow, and run."""
        self._cleanup_expired()
        key = (tenant_id, workflow_name, run_id)

        if self.db_path:
            conn = sqlite3.connect(self.db_path, timeout=10.0)
            try:
                cursor = conn.execute(
                    "SELECT step_name, result FROM nexapy_dag_states "
                    "WHERE tenant_id = ? AND workflow_name = ? AND run_id = ? AND status = 'success'",
                    (tenant_id, workflow_name, run_id),
                )
                rows = cursor.fetchall()
                steps = {r[0]: (json.loads(r[1]) if r[1] is not None else None) for r in rows}
                with self._lock:
                    self._memory_store[key] = steps
                    self._timestamps[key] = time.time()
                return steps.copy()
            finally:
                conn.close()

        with self._lock:
            if key in self._memory_store:
                return self._memory_store[key].copy()

        return {}

    def claim_or_get_step(
        self,
        tenant_id: str,
        workflow_name: str,
        run_id: str,
        step_name: str,
        lease_seconds: float = 60.0,
    ) -> Tuple[str, Any, Optional[str]]:
        """
        Atomically claim a step execution or return existing status.
        Returns:
            - ("CACHED", result, None) if step has already completed successfully.
            - ("RUNNING", None, None) if step is currently being executed with a valid lease.
            - ("CLAIMED", None, token) if caller won the claim to execute this step.
        """
        now = time.time()
        key_run = (tenant_id, workflow_name, run_id)
        step_key = (tenant_id, workflow_name, run_id, step_name)

        if self.db_path:
            conn = sqlite3.connect(self.db_path, timeout=30.0)
            try:
                conn.execute("BEGIN IMMEDIATE")
                cursor = conn.execute(
                    "SELECT result, status, updated_at, claim_token FROM nexapy_dag_states "
                    "WHERE tenant_id = ? AND workflow_name = ? AND run_id = ? AND step_name = ?",
                    (tenant_id, workflow_name, run_id, step_name),
                )
                row = cursor.fetchone()
                if row:
                    res_raw, st, upd_at, existing_token = row[0], row[1], row[2], row[3]
                    if st == "success":
                        conn.commit()
                        cached_val = json.loads(res_raw) if res_raw is not None else None
                        with self._lock:
                            self._step_records[step_key] = {"status": "success", "result": cached_val, "updated_at": upd_at, "claim_token": existing_token}
                            if key_run not in self._memory_store:
                                self._memory_store[key_run] = {}
                            self._memory_store[key_run][step_name] = cached_val
                        return ("CACHED", cached_val, None)
                    if st == "RUNNING" and (now - upd_at) < lease_seconds:
                        conn.commit()
                        return ("RUNNING", None, None)

                # Atomically claim the execution
                token = uuid.uuid4().hex
                conn.execute(
                    """
                    INSERT INTO nexapy_dag_states
                    (tenant_id, workflow_name, run_id, step_name, result, status, claim_token, error, updated_at)
                    VALUES (?, ?, ?, ?, NULL, 'RUNNING', ?, NULL, ?)
                    ON CONFLICT(tenant_id, workflow_name, run_id, step_name) DO UPDATE SET
                        status = 'RUNNING',
                        claim_token = excluded.claim_token,
                        error = NULL,
                        updated_at = excluded.updated_at
                    """,
                    (tenant_id, workflow_name, run_id, step_name, token, now),
                )
                conn.commit()
                with self._lock:
                    self._step_records[step_key] = {"status": "RUNNING", "result": None, "claim_token": token, "updated_at": now}
                return ("CLAIMED", None, token)
            except Exception:
                conn.rollback()
                return ("RUNNING", None, None)
            finally:
                conn.close()

        # Pure in-memory claim
        with self._lock:
            if step_key in self._step_records:
                record = self._step_records[step_key]
                st = record.get("status")
                upd_at = record.get("updated_at", 0)
                if st == "success":
                    return ("CACHED", record.get("result"), None)
                if st == "RUNNING" and (now - upd_at) < lease_seconds:
                    return ("RUNNING", None, None)

            token = uuid.uuid4().hex
            self._step_records[step_key] = {"status": "RUNNING", "result": None, "claim_token": token, "updated_at": now}
            return ("CLAIMED", None, token)

    def get_step_status(
        self,
        tenant_id: str,
        workflow_name: str,
        run_id: str,
        step_name: str,
    ) -> Optional[Dict[str, Any]]:
        """Retrieve authoritative status of a specific step."""
        step_key = (tenant_id, workflow_name, run_id, step_name)

        if self.db_path:
            conn = sqlite3.connect(self.db_path, timeout=10.0)
            try:
                cursor = conn.execute(
                    "SELECT result, status, error, updated_at, claim_token FROM nexapy_dag_states "
                    "WHERE tenant_id = ? AND workflow_name = ? AND run_id = ? AND step_name = ?",
                    (tenant_id, workflow_name, run_id, step_name),
                )
                row = cursor.fetchone()
                if row:
                    res = json.loads(row[0]) if row[0] is not None else None
                    info = {
                        "result": res,
                        "status": row[1],
                        "error": row[2],
                        "updated_at": row[3],
                        "claim_token": row[4],
                    }
                    with self._lock:
                        self._step_records[step_key] = info
                        if row[1] == "success":
                            key_run = (tenant_id, workflow_name, run_id)
                            if key_run not in self._memory_store:
                                self._memory_store[key_run] = {}
                            self._memory_store[key_run][step_name] = res
                    return info
                return None
            finally:
                conn.close()

        with self._lock:
            if step_key in self._step_records:
                return self._step_records[step_key].copy()

        return None

    def save_step(
        self,
        tenant_id: str,
        workflow_name: str,
        run_id: str,
        step_name: str,
        result: Any,
        status: str = "success",
        error: Optional[str] = None,
        claim_token: Optional[str] = None,
    ) -> bool:
        """
        Atomically persist a step execution output with mandatory fencing token check.
        Rejects any attempt to overwrite an actively claimed step without a valid claim_token (P0 Finding V7).
        """
        key = (tenant_id, workflow_name, run_id)
        step_key = (tenant_id, workflow_name, run_id, step_name)
        now = time.time()
        result_json = json.dumps(result) if result is not None else None

        if self.db_path:
            conn = sqlite3.connect(self.db_path, timeout=10.0)
            try:
                conn.execute("BEGIN IMMEDIATE")
                if claim_token is not None:
                    # Fencing token enforcement: ONLY update if status is RUNNING and claim_token matches!
                    cursor = conn.execute(
                        """
                        UPDATE nexapy_dag_states
                        SET result = ?, status = ?, error = ?, updated_at = ?
                        WHERE tenant_id = ? AND workflow_name = ? AND run_id = ? AND step_name = ?
                          AND status = 'RUNNING' AND claim_token = ?
                        """,
                        (result_json, status, error, now, tenant_id, workflow_name, run_id, step_name, claim_token),
                    )
                    conn.commit()
                    if cursor.rowcount == 0:
                        # Fencing check failed: lease expired or preempted!
                        return False
                else:
                    # Without claim_token, check if step is actively claimed; if so, reject overwrite!
                    cursor = conn.execute(
                        "SELECT status, claim_token FROM nexapy_dag_states WHERE tenant_id = ? AND workflow_name = ? AND run_id = ? AND step_name = ?",
                        (tenant_id, workflow_name, run_id, step_name),
                    )
                    row = cursor.fetchone()
                    if row and row[0] == "RUNNING":
                        conn.commit()
                        return False

                    conn.execute(
                        """
                        INSERT INTO nexapy_dag_states
                        (tenant_id, workflow_name, run_id, step_name, result, status, error, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(tenant_id, workflow_name, run_id, step_name) DO UPDATE SET
                            result=excluded.result,
                            status=excluded.status,
                            error=excluded.error,
                            updated_at=excluded.updated_at
                        """,
                        (tenant_id, workflow_name, run_id, step_name, result_json, status, error, now),
                    )
                    conn.commit()
            except Exception:
                conn.rollback()
                return False
            finally:
                conn.close()

        with self._lock:
            if claim_token is not None:
                if step_key not in self._step_records:
                    return False
                existing = self._step_records[step_key]
                if existing.get("claim_token") != claim_token or existing.get("status") != "RUNNING":
                    return False
            else:
                if step_key in self._step_records and self._step_records[step_key].get("status") == "RUNNING":
                    # Actively claimed! Reject unauthenticated overwrite
                    return False

            if key not in self._memory_store:
                self._memory_store[key] = {}
            if status == "success":
                self._memory_store[key][step_name] = result
            self._timestamps[key] = now
            self._step_records[step_key] = {
                "status": status,
                "result": result,
                "error": error,
                "claim_token": claim_token,
                "updated_at": now,
            }

        return True

    def admin_set_step(
        self,
        tenant_id: str,
        workflow_name: str,
        run_id: str,
        step_name: str,
        result: Any,
        status: str = "success",
        error: Optional[str] = None,
    ) -> None:
        """
        Administrative override API to set step state directly outside regular worker claiming.
        """
        key = (tenant_id, workflow_name, run_id)
        step_key = (tenant_id, workflow_name, run_id, step_name)
        now = time.time()
        result_json = json.dumps(result) if result is not None else None

        if self.db_path:
            conn = sqlite3.connect(self.db_path, timeout=10.0)
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    """
                    INSERT INTO nexapy_dag_states
                    (tenant_id, workflow_name, run_id, step_name, result, status, error, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(tenant_id, workflow_name, run_id, step_name) DO UPDATE SET
                        result=excluded.result,
                        status=excluded.status,
                        error=excluded.error,
                        updated_at=excluded.updated_at
                    """,
                    (tenant_id, workflow_name, run_id, step_name, result_json, status, error, now),
                )
                conn.commit()
            finally:
                conn.close()

        with self._lock:
            if key not in self._memory_store:
                self._memory_store[key] = {}
            if status == "success":
                self._memory_store[key][step_name] = result
            self._timestamps[key] = now
            self._step_records[step_key] = {
                "status": status,
                "result": result,
                "error": error,
                "claim_token": None,
                "updated_at": now,
            }

    def _cleanup_expired(self) -> None:
        now = time.time()
        expired_keys = [k for k, ts in self._timestamps.items() if now - ts > self.ttl_seconds]
        for k in expired_keys:
            self._memory_store.pop(k, None)
            self._timestamps.pop(k, None)

        if self.db_path:
            try:
                conn = sqlite3.connect(self.db_path, timeout=5.0)
                conn.execute("DELETE FROM nexapy_dag_states WHERE updated_at < ?", (now - self.ttl_seconds,))
                conn.commit()
                conn.close()
            except Exception:
                pass

    def clear(self) -> None:
        """Clear all stored state."""
        self._memory_store.clear()
        self._timestamps.clear()
        self._legacy_map.clear()
        if self.db_path:
            try:
                conn = sqlite3.connect(self.db_path, timeout=5.0)
                conn.execute("DELETE FROM nexapy_dag_states")
                conn.commit()
                conn.close()
            except Exception:
                pass

    # Dict-like fallback methods for tests
    def setdefault(self, key: Any, default: Any = None) -> Any:
        if isinstance(key, str):
            return self._legacy_map.setdefault(key, default if default is not None else {})
        elif isinstance(key, tuple):
            if key not in self._memory_store:
                self._memory_store[key] = default if default is not None else {}
            return self._memory_store[key]
        return {}

    def __getitem__(self, key: Any) -> Any:
        if isinstance(key, str):
            return self._legacy_map[key]
        return self._memory_store[key]

    def __setitem__(self, key: Any, value: Any) -> None:
        if isinstance(key, str):
            self._legacy_map[key] = value
        else:
            self._memory_store[key] = value
            self._timestamps[key] = time.time()

    def __contains__(self, key: Any) -> bool:
        if isinstance(key, str):
            return key in self._legacy_map
        return key in self._memory_store

    def get(self, key: Any, default: Any = None) -> Any:
        if isinstance(key, str):
            return self._legacy_map.get(key, default)
        return self._memory_store.get(key, default)


_dag_run_state_store = DAGStateStore()


def get_default_dag_state_store() -> DAGStateStore:
    """Get the active global DAG state store."""
    return _dag_run_state_store


def set_default_dag_state_store(store: DAGStateStore) -> None:
    """Set the active global DAG state store."""
    global _dag_run_state_store
    _dag_run_state_store = store


class DAGWorkflow:
    """Directed Acyclic Graph Workflow runner allowing parallel & conditional step execution."""

    def __init__(self, name: str, description: Optional[str] = None, schedule: Optional[str] = None):
        self.name = name
        self.description = description or f"DAG Workflow {name}"
        self.schedule = schedule
        self.nodes: Dict[str, DAGNode] = {}

    def add_node(
        self,
        name: str,
        func: Optional[Callable[..., Any]] = None,
        depends_on: Optional[List[str]] = None,
        description: Optional[str] = None,
        max_retries: int = 0,
        retry_delay: float = 1.0,
        condition: Optional[Callable[[WorkflowContext], bool]] = None,
    ) -> Any:
        """Register a step node into the DAG workflow."""
        def decorator(f: Callable[..., Any]) -> Callable[..., Any]:
            node_name = name or f.__name__
            if node_name in self.nodes:
                raise ValueError(f"DAG Node '{node_name}' already exists in workflow '{self.name}'.")
            
            node = DAGNode(
                name=node_name,
                func=f,
                depends_on=depends_on or [],
                description=description,
                max_retries=max_retries,
                retry_delay=retry_delay,
                condition=condition,
            )
            self.nodes[node_name] = node
            return f

        if func is not None:
            return decorator(func)
        return decorator

    def node(
        self,
        name: str,
        depends_on: Optional[List[str]] = None,
        description: Optional[str] = None,
        max_retries: int = 0,
        retry_delay: float = 1.0,
        condition: Optional[Callable[[WorkflowContext], bool]] = None,
    ) -> Callable:
        """Decorator shortcut to register a node."""
        return self.add_node(
            name=name,
            depends_on=depends_on,
            description=description,
            max_retries=max_retries,
            retry_delay=retry_delay,
            condition=condition,
        )

    def _topological_levels(self) -> List[List[DAGNode]]:
        """
        Group nodes into parallel execution levels using topological sorting.
        Nodes in the same level can execute concurrently.
        """
        in_degree = {name: len(node.depends_on) for name, node in self.nodes.items()}
        graph: Dict[str, List[str]] = {name: [] for name in self.nodes}

        for name, node in self.nodes.items():
            for dep in node.depends_on:
                if dep not in self.nodes:
                    raise ValueError(f"Node '{name}' depends on unknown node '{dep}'")
                graph[dep].append(name)

        levels = []
        current_level = [name for name, deg in in_degree.items() if deg == 0]

        visited_count = 0
        while current_level:
            levels.append([self.nodes[n] for n in sorted(current_level)])
            visited_count += len(current_level)
            next_level = []

            for n in current_level:
                for neighbor in graph[n]:
                    in_degree[neighbor] -= 1
                    if in_degree[neighbor] == 0:
                        next_level.append(neighbor)
            current_level = next_level

        if visited_count < len(self.nodes):
            raise ValueError(f"Cycle detected in DAG Workflow '{self.name}'.")

        return levels

    async def execute_async(self, ctx: WorkflowContext) -> Dict[str, Any]:
        """Execute all nodes in the DAG in parallel topological order."""
        levels = self._topological_levels()
        step_results: Dict[str, Any] = {}
        ctx.set("steps", step_results)

        for level in levels:
            tasks = [self._execute_node(node, ctx, step_results) for node in level]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for node, res in zip(level, results):
                if isinstance(res, Exception):
                    raise res
                step_results[node.name] = res

        return step_results

    async def execute_step_async(self, step_name: str, ctx: WorkflowContext) -> Any:
        """
        Execute a single step node in the DAG workflow by name.
        State is strictly partitioned by (tenant_id, workflow_name, run_id, step_name)
        to prevent cross-workflow results pollution (P0-05 and P0-06).
        """
        if step_name not in self.nodes:
            raise KeyError(
                f"Step '{step_name}' not found in DAG Workflow '{self.name}'. Available steps: {list(self.nodes.keys())}"
            )
        node = self.nodes[step_name]

        # Tenant identity security enforcement (P0 Finding V6 & V7)
        sec_tenant = (
            ctx.security_context.get("tenant_id")
            if isinstance(getattr(ctx, "security_context", None), dict)
            else None
        )
        if sec_tenant:
            # Trusted server security context always takes precedence over untrusted input
            tenant_id = sec_tenant
        else:
            tenant_id = (
                getattr(ctx, "tenant_id", None)
                or (ctx.input.get("tenant_id") if isinstance(ctx.input, dict) else None)
                or "default"
            )

        raw_run_id = (
            (ctx.input.get("run_id") if isinstance(ctx.input, dict) else None)
            or getattr(ctx, "run_id", None)
        )
        if raw_run_id:
            if not isinstance(raw_run_id, str) or not re.match(r"^[a-zA-Z0-9_\-\.]{1,128}$", raw_run_id):
                raise ValueError(f"Invalid run_id format: '{raw_run_id}'. Must be alphanumeric with - _ . (max 128 chars).")
            run_id = raw_run_id
        else:
            run_id = f"run_{uuid.uuid4().hex}"

        # Isolated step results for THIS tenant, THIS workflow, and THIS run_id
        step_results = _dag_run_state_store.get_run_steps(tenant_id, self.name, run_id)
        ctx.set("steps", step_results)

        # Idempotency check: if step already executed in this run for this workflow
        if step_name in step_results:
            return {
                "workflow": self.name,
                "step": step_name,
                "run_id": run_id,
                "tenant_id": tenant_id,
                "result": step_results[step_name],
                "status": "cached",
            }

        # Validate all required dependencies have completed
        for dep in node.depends_on:
            if dep not in step_results:
                raise ValueError(
                    f"Dependency step '{dep}' has not executed yet for step '{step_name}' in run '{run_id}' for workflow '{self.name}'."
                )

        # Atomic claim to prevent duplicate execution across parallel requests (P0 Finding V5, V6 & V7)
        claim_status, cached_res, claim_token = _dag_run_state_store.claim_or_get_step(
            tenant_id, self.name, run_id, step_name, lease_seconds=60.0
        )
        if claim_status == "CACHED":
            return {
                "workflow": self.name,
                "step": step_name,
                "run_id": run_id,
                "tenant_id": tenant_id,
                "result": cached_res,
                "status": "cached",
            }

        if claim_status == "RUNNING":
            # Another concurrent request holds the active lease; await its completion
            poll_start = time.time()
            poll_timeout = 30.0
            while time.time() - poll_start < poll_timeout:
                await asyncio.sleep(0.05)
                info = _dag_run_state_store.get_step_status(tenant_id, self.name, run_id, step_name)
                if info:
                    if info.get("status") == "success":
                        return {
                            "workflow": self.name,
                            "step": step_name,
                            "run_id": run_id,
                            "tenant_id": tenant_id,
                            "result": info.get("result"),
                            "status": "cached",
                        }
                    elif info.get("status") == "FAILED":
                        raise RuntimeError(f"Step '{step_name}' failed in concurrent execution: {info.get('error')}")
            # Timeout reached while waiting for other worker: DO NOT execute without a valid claim
            raise TimeoutError(f"Step '{step_name}' is still being processed by another worker in run '{run_id}'.")

        if claim_status != "CLAIMED" or not claim_token:
            raise RuntimeError(
                f"Cannot execute step '{step_name}': step was not claimed by current worker (status='{claim_status}')."
            )

        try:
            res = await self._execute_node(node, ctx, step_results)
            step_results[step_name] = res
            ctx.set("steps", step_results)
            saved = _dag_run_state_store.save_step(
                tenant_id, self.name, run_id, step_name, res, status="success", claim_token=claim_token
            )
            if not saved:
                raise RuntimeError(
                    f"Step '{step_name}' claim lease expired or was preempted by another worker in run '{run_id}'."
                )
            return {
                "workflow": self.name,
                "step": step_name,
                "run_id": run_id,
                "tenant_id": tenant_id,
                "result": res,
                "status": "success",
            }
        except Exception as exc:
            _dag_run_state_store.save_step(
                tenant_id, self.name, run_id, step_name, None, status="FAILED", error=str(exc), claim_token=claim_token
            )
            raise exc

    async def _execute_node(self, node: DAGNode, ctx: WorkflowContext, step_results: Dict[str, Any]) -> Any:
        """Execute a single node with condition check and retries."""
        if node.condition is not None:
            cond_val = node.condition(ctx)
            if not cond_val:
                return {"status": "skipped", "reason": "Condition returned False"}

        sig = inspect.signature(node.func)
        has_ctx = len(sig.parameters) > 0

        last_error = None
        for attempt in range(node.max_retries + 1):
            try:
                if inspect.iscoroutinefunction(node.func):
                    return await node.func(ctx) if has_ctx else await node.func()
                else:
                    return await asyncio.to_thread(node.func, ctx) if has_ctx else await asyncio.to_thread(node.func)
            except Exception as err:
                last_error = err
                if attempt < node.max_retries:
                    await asyncio.sleep(node.retry_delay)

        raise RuntimeError(f"DAG Node '{node.name}' failed after {node.max_retries + 1} attempts: {last_error}")
