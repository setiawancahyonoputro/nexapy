import asyncio
import tempfile
from pathlib import Path
import pytest
from nexapy import workflow
from nexapy.automation import JobQueue, WorkflowScheduler, workflow_registry


@pytest.mark.asyncio
async def test_job_queue_in_memory_and_sqlite():
    """Verify JobQueue enqueues, executes, and persists job status."""
    workflow_registry.clear()

    @workflow("queue_test_wf")
    async def queue_test_wf(ctx):
        return {"processed": ctx.input.get("x", 0) * 2}

    with tempfile.TemporaryDirectory() as tmp_dir:
        db_path = Path(tmp_dir) / "jobs.db"
        queue = JobQueue(db_path=db_path, max_workers=2)

        # Enqueue job
        job = await queue.enqueue("queue_test_wf", {"x": 21})
        assert job.status == "QUEUED"
        assert job.job_id.startswith("job_")

        # Start workers, let it process
        queue.start_workers()
        await asyncio.sleep(0.5)
        await queue.stop_workers()

        # Check job status after processing
        updated_job = queue.get_job(job.job_id)
        assert updated_job is not None
        assert updated_job.status == "COMPLETED"
        assert updated_job.result == {"processed": 42}

    workflow_registry.clear()


def test_scheduler_interval_parsing():
    """Verify scheduler parses interval strings accurately."""
    sched = WorkflowScheduler()

    assert sched._parse_interval_seconds("30s") == 30.0
    assert sched._parse_interval_seconds("every 10s") == 10.0
    assert sched._parse_interval_seconds("5m") == 300.0
    assert sched._parse_interval_seconds("1h") == 3600.0
    assert sched._parse_interval_seconds("invalid_cron") is None
