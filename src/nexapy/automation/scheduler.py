import asyncio
import datetime
import logging
import re
import time
from typing import Dict, List, Optional
from .registry import workflow_registry
from .runner import runner

logger = logging.getLogger("nexapy.scheduler")


def _match_cron_field(field_val: int, expr: str, min_val: int, max_val: int) -> bool:
    """Evaluate a single cron field against an integer value safely."""
    expr = expr.strip()
    if expr == "*":
        return True
    if "/" in expr:
        parts = expr.split("/")
        if len(parts) == 2:
            try:
                step = int(parts[1])
                # P1 Finding 18: Reject step <= 0 to prevent ZeroDivisionError
                if step <= 0:
                    return False
                sub_expr = parts[0]
                if sub_expr == "*":
                    start_val, end_val = min_val, max_val
                elif "-" in sub_expr:
                    rparts = sub_expr.split("-")
                    start_val, end_val = int(rparts[0]), int(rparts[1])
                else:
                    start_val, end_val = int(sub_expr), max_val
                return (start_val <= field_val <= end_val) and ((field_val - start_val) % step == 0)
            except (ValueError, ZeroDivisionError):
                return False
    if "," in expr:
        return any(_match_cron_field(field_val, sub.strip(), min_val, max_val) for sub in expr.split(","))
    if "-" in expr:
        parts = expr.split("-")
        if len(parts) == 2:
            try:
                return int(parts[0]) <= field_val <= int(parts[1])
            except ValueError:
                return False
    try:
        return field_val == int(expr)
    except ValueError:
        return False


def matches_cron(cron_str: str, dt: Optional[datetime.datetime] = None) -> bool:
    """
    Evaluate standard 5-field cron expression against datetime safely.
    Fields: minute (0-59), hour (0-23), day (1-31), month (1-12), day_of_week (0-6, Sunday=0 or 7).
    """
    try:
        clean = cron_str.strip()
        fields = clean.split()
        if len(fields) != 5:
            return False

        dt = dt or datetime.datetime.now()
        m_expr, h_expr, d_expr, mo_expr, dow_expr = fields

        if not _match_cron_field(dt.minute, m_expr, 0, 59):
            return False
        if not _match_cron_field(dt.hour, h_expr, 0, 23):
            return False
        if not _match_cron_field(dt.month, mo_expr, 1, 12):
            return False
        # Standard POSIX / Vixie cron behavior:
        # If both DOM and DOW are specified (neither is '*'), match if EITHER matches (OR condition)
        dom_restricted = (d_expr != "*")
        dow_restricted = (dow_expr != "*")
        dom_matches = _match_cron_field(dt.day, d_expr, 1, 31)

        cron_dow = (dt.weekday() + 1) % 7
        if dow_expr == "7":
            dow_expr = "0"
        dow_matches = _match_cron_field(cron_dow, dow_expr, 0, 6)

        if dom_restricted and dow_restricted:
            if not (dom_matches or dow_matches):
                return False
        else:
            if not dom_matches or not dow_matches:
                return False

        return True
    except Exception:
        return False


class WorkflowScheduler:
    """
    In-process scheduler that executes registered workflows matching standard cron
    expressions or human-readable interval schedules (e.g. '10s', '5m', '1h').
    Supports atomic multi-instance leader election across multiple worker processes when backed by SQLite.
    """

    def __init__(self, check_interval: float = 1.0, db_path: Optional[str] = None):
        self.check_interval = check_interval
        self.db_path = db_path
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._last_runs: Dict[str, float] = {}
        self._last_cron_minutes: Dict[str, str] = {}

    def _acquire_execution_lock(self, workflow_name: str, execution_key: str) -> bool:
        """Atomic leader election lock across multiple worker instances."""
        if not self.db_path:
            return True
        import sqlite3
        conn = None
        try:
            conn = sqlite3.connect(self.db_path, timeout=5.0)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS nexapy_scheduler_locks (
                    workflow_name TEXT NOT NULL,
                    execution_key TEXT NOT NULL,
                    locked_at REAL NOT NULL,
                    PRIMARY KEY (workflow_name, execution_key)
                )
                """
            )
            now = time.time()
            # Clean up old locks older than 24 hours
            conn.execute("DELETE FROM nexapy_scheduler_locks WHERE locked_at < ?", (now - 86400,))
            conn.execute(
                "INSERT INTO nexapy_scheduler_locks (workflow_name, execution_key, locked_at) VALUES (?, ?, ?)",
                (workflow_name, execution_key, now),
            )
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False
        except Exception as exc:
            # Fail-closed (P1-02): Never execute without lock on database failure or connection error
            logger.warning(f"Scheduler lock acquisition failed for '{workflow_name}' ({execution_key}): {exc}")
            return False
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    def _parse_interval_seconds(self, schedule_str: str) -> Optional[float]:
        """Parse intervals like '10s', '5m', '1h', or 'every 30s'."""
        clean = schedule_str.lower().replace("every", "").strip()
        match = re.match(r"^(\d+(?:\.\d+)?)\s*(s|sec|m|min|h|hr|d)?$", clean)
        if not match:
            return None
        val, unit = float(match.group(1)), match.group(2) or "s"
        # Validate interval > 0 (P1 Finding 18: Reject 0s/0m to prevent infinite loops)
        if val <= 0:
            raise ValueError(f"Schedule interval must be strictly positive (got '{schedule_str}').")

        if unit in ("s", "sec"):
            return val
        elif unit in ("m", "min"):
            return val * 60
        elif unit in ("h", "hr"):
            return val * 3600
        elif unit in ("d",):
            return val * 86400
        return None

    async def _loop(self) -> None:
        while self._running:
            now = time.time()
            now_dt = datetime.datetime.now()
            current_minute_key = now_dt.strftime("%Y-%m-%d %H:%M")
            workflows = workflow_registry.list_all()

            for wf in workflows:
                schedule = getattr(wf, "schedule", None)
                if not schedule:
                    continue

                # 1. Try interval format first
                interval = self._parse_interval_seconds(schedule)
                if interval is not None:
                    last_run = self._last_runs.get(wf.name, 0.0)
                    if now - last_run >= interval:
                        slot_key = f"interval_{int(now // interval)}"
                        if not self._acquire_execution_lock(wf.name, slot_key):
                            continue
                        self._last_runs[wf.name] = now
                        asyncio.create_task(self._safe_execute(wf.name))
                    continue

                # 2. Try 5-field cron expression
                if len(schedule.strip().split()) == 5:
                    if self._last_cron_minutes.get(wf.name) != current_minute_key:
                        if matches_cron(schedule, now_dt):
                            slot_key = f"cron_{current_minute_key}"
                            if not self._acquire_execution_lock(wf.name, slot_key):
                                continue
                            self._last_cron_minutes[wf.name] = current_minute_key
                            self._last_runs[wf.name] = now
                            asyncio.create_task(self._safe_execute(wf.name))

            await asyncio.sleep(self.check_interval)

    async def _safe_execute(self, workflow_name: str) -> None:
        try:
            await runner.run_async(workflow_name)
        except Exception:
            pass

    def start(self) -> None:
        """Start scheduler in background event loop."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        """Stop scheduler."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None


# Global scheduler singleton
scheduler = WorkflowScheduler()
