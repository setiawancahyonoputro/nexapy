import asyncio
import datetime
import re
import time
from typing import Dict, List, Optional
from .registry import workflow_registry
from .runner import runner


def _match_cron_field(field_val: int, expr: str, min_val: int, max_val: int) -> bool:
    """Evaluate a single cron field against an integer value."""
    expr = expr.strip()
    if expr == "*":
        return True
    if "/" in expr:
        parts = expr.split("/")
        if len(parts) == 2:
            try:
                step = int(parts[1])
                sub_expr = parts[0]
                start_val = min_val if sub_expr == "*" else int(sub_expr)
                return (field_val >= start_val) and ((field_val - start_val) % step == 0)
            except ValueError:
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
    Evaluate standard 5-field cron expression against datetime.
    Fields: minute (0-59), hour (0-23), day (1-31), month (1-12), day_of_week (0-6, Sunday=0 or 7).
    """
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
    if not _match_cron_field(dt.day, d_expr, 1, 31):
        return False
    if not _match_cron_field(dt.month, mo_expr, 1, 12):
        return False

    # Standard cron: 0 = Sunday, 1 = Monday, ..., 6 = Saturday (7 is also Sunday)
    cron_dow = (dt.weekday() + 1) % 7
    if dow_expr == "7":
        dow_expr = "0"
    if not _match_cron_field(cron_dow, dow_expr, 0, 6):
        return False

    return True


class WorkflowScheduler:
    """
    In-process scheduler that executes registered workflows matching standard cron
    expressions or human-readable interval schedules (e.g. '10s', '5m', '1h').
    """

    def __init__(self, check_interval: float = 1.0):
        self.check_interval = check_interval
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._last_runs: Dict[str, float] = {}
        self._last_cron_minutes: Dict[str, str] = {}

    def _parse_interval_seconds(self, schedule_str: str) -> Optional[float]:
        """Parse intervals like '10s', '5m', '1h', or 'every 30s'."""
        clean = schedule_str.lower().replace("every", "").strip()
        match = re.match(r"^(\d+(?:\.\d+)?)\s*(s|sec|m|min|h|hr|d)?$", clean)
        if not match:
            return None
        val, unit = float(match.group(1)), match.group(2) or "s"
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
                        self._last_runs[wf.name] = now
                        asyncio.create_task(self._safe_execute(wf.name))
                    continue

                # 2. Try 5-field cron expression
                if len(schedule.strip().split()) == 5:
                    if self._last_cron_minutes.get(wf.name) != current_minute_key:
                        if matches_cron(schedule, now_dt):
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
