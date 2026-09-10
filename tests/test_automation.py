import asyncio
import json
import os
import tempfile
from pathlib import Path
import pytest
from typer.testing import CliRunner
from click.utils import strip_ansi
from nexapy import workflow
from nexapy.automation import (
    WorkflowContext,
    WorkflowRegistry,
    workflow_registry,
    WorkflowRunner,
    runner as default_runner,
)
from nexapy.cli import app

cli_runner = CliRunner()


@pytest.fixture(autouse=True)
def reset_registry():
    """Reset global workflow registry before each test."""
    workflow_registry.clear()
    yield
    workflow_registry.clear()


def test_workflow_context_get_set():
    ctx = WorkflowContext(input_data={"foo": "bar", "num": 42}, workflow_name="test_flow")
    assert ctx.workflow_name == "test_flow"
    assert ctx.input["foo"] == "bar"
    assert ctx.get("foo") == "bar"

    # Fallback to internal state
    ctx.set("step1", "completed")
    assert ctx.get("step1") == "completed"
    assert ctx.get("non_existent", "default") == "default"
    assert "<WorkflowContext workflow='test_flow'" in repr(ctx)


def test_workflow_decorator_and_registry():
    @workflow("hello")
    async def hello_func(ctx):
        return {"message": f"Hello {ctx.get('name', 'world')}"}

    @workflow
    def simple_sync():
        return "sync_result"

    assert len(workflow_registry) == 2

    wf_hello = workflow_registry.get("hello")
    assert wf_hello is not None
    assert wf_hello.name == "hello"

    wf_sync = workflow_registry.get("simple_sync")
    assert wf_sync is not None
    assert wf_sync.name == "simple_sync"

    all_workflows = workflow_registry.list_all()
    assert len(all_workflows) == 2
    assert [w.name for w in all_workflows] == ["hello", "simple_sync"]


def test_decorated_workflow_remains_callable():
    """Verify @workflow returns the original function so it remains directly callable."""
    @workflow("direct_call")
    def direct_call(ctx):
        return f"Hello {ctx.get('name')}"

    assert callable(direct_call)
    ctx = WorkflowContext(input_data={"name": "Direct"}, workflow_name="direct_call")
    result = direct_call(ctx)
    assert result == "Hello Direct"


def test_duplicate_workflow_registration_raises_value_error():
    """Verify registering a duplicate workflow name raises ValueError unless override=True."""
    @workflow("duplicate_test")
    def first_flow(ctx):
        return "first"

    with pytest.raises(ValueError) as exc_info:
        @workflow("duplicate_test")
        def second_flow(ctx):
            return "second"

    assert "Workflow 'duplicate_test' is already registered" in str(exc_info.value)

    # Allowed with explicit override
    workflow_registry.register("duplicate_test", func=lambda ctx: "overridden", override=True)
    assert workflow_registry.get("duplicate_test").func(None) == "overridden"


def test_workflow_runner_async_and_sync():
    @workflow("async_flow")
    async def async_flow(ctx):
        name = ctx.input.get("name", "NexaPy")
        return {"greeting": f"Hello {name}"}

    @workflow("sync_flow")
    def sync_flow(ctx):
        return f"Result: {ctx.input.get('val', 0) * 2}"

    w_runner = WorkflowRunner()

    res_async = w_runner.run("async_flow", input_data={"name": "Alice"})
    assert res_async == {"greeting": "Hello Alice"}

    res_sync = w_runner.run("sync_flow", input_data={"val": 21})
    assert res_sync == "Result: 42"


def test_workflow_runner_unregistered():
    w_runner = WorkflowRunner()
    with pytest.raises(KeyError) as exc_info:
        w_runner.run("non_existent_flow")
    assert "Workflow 'non_existent_flow' is not registered" in str(exc_info.value)


@pytest.mark.asyncio
async def test_runner_run_inside_active_event_loop_raises_runtime_error():
    """Verify runner.run raises RuntimeError when invoked inside an active event loop."""
    @workflow("loop_test")
    def flow():
        return "ok"

    w_runner = WorkflowRunner()
    with pytest.raises(RuntimeError) as exc_info:
        w_runner.run("loop_test")

    assert "runner.run() cannot be used inside an active event loop" in str(exc_info.value)

    # run_async must work inside active event loop
    res = await w_runner.run_async("loop_test")
    assert res == "ok"


def test_cli_workflow_list_empty():
    result = cli_runner.invoke(app, ["workflow", "list"])
    assert result.exit_code == 0
    output = strip_ansi(result.output)
    assert "No workflows registered." in output


def test_cli_workflow_list_with_items():
    @workflow("customer-support")
    async def support(ctx):
        """Customer support handler workflow."""
        return "support"

    @workflow("daily-report")
    def report(ctx):
        """Daily status reporter."""
        return "report"

    result = cli_runner.invoke(app, ["workflow", "list"])
    assert result.exit_code == 0
    output = strip_ansi(result.output)
    assert "Available Workflows" in output
    assert "customer-support" in output
    assert "daily-report" in output


def test_cli_workflow_import_error_propagation():
    """Verify CLI reports import errors cleanly and exits with code 1."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        broken_app = Path(tmp_dir) / "app.py"
        broken_app.write_text("raise RuntimeError('boom during import')", encoding="utf-8")

        cwd = os.getcwd()
        try:
            os.chdir(tmp_dir)
            result = cli_runner.invoke(app, ["workflow", "list"])
            assert result.exit_code == 1
            output = strip_ansi(result.output)
            assert "Failed to load workflows from 'app.py'" in output
            assert "RuntimeError: boom during import" in output
        finally:
            os.chdir(cwd)


def test_cli_workflow_run_success():
    @workflow("greet")
    async def greet(ctx):
        person = ctx.input.get("person", "Developer")
        return {"status": "ok", "message": f"Hello {person}"}

    # Run without input
    res1 = cli_runner.invoke(app, ["workflow", "run", "greet"])
    assert res1.exit_code == 0
    output1 = strip_ansi(res1.output)
    assert "Workflow 'greet' executed successfully" in output1
    assert "Hello Developer" in output1

    # Run with JSON string input
    res2 = cli_runner.invoke(app, ["workflow", "run", "greet", "--input", '{"person":"Bob"}'])
    assert res2.exit_code == 0
    output2 = strip_ansi(res2.output)
    assert "Hello Bob" in output2


def test_cli_workflow_run_with_file_input():
    @workflow("calculate")
    def calc(ctx):
        a = ctx.input.get("a", 0)
        b = ctx.input.get("b", 0)
        return {"sum": a + b}

    with tempfile.TemporaryDirectory() as tmp_dir:
        json_file = Path(tmp_dir) / "data.json"
        json_file.write_text(json.dumps({"a": 15, "b": 27}), encoding="utf-8")

        res = cli_runner.invoke(app, ["workflow", "run", "calculate", "-i", f"@{json_file}"])
        assert res.exit_code == 0
        output = strip_ansi(res.output)
        assert "42" in output


def test_cli_workflow_run_invalid_cases():
    # 1. Unregistered workflow
    res1 = cli_runner.invoke(app, ["workflow", "run", "unknown_workflow"])
    assert res1.exit_code == 1
    assert "is not registered" in strip_ansi(res1.output)

    # 2. Invalid JSON input string
    @workflow("dummy")
    def dummy():
        return "ok"

    res2 = cli_runner.invoke(app, ["workflow", "run", "dummy", "-i", "invalid-json"])
    assert res2.exit_code == 1
    assert "Invalid JSON string" in strip_ansi(res2.output)


@pytest.mark.asyncio
async def test_workflow_ctx_ai_action():
    """Verify ctx.ai(...) action completes prompt via AI router within workflow context."""
    from unittest.mock import AsyncMock, patch
    from nexapy.ai.base import AIResponse

    mock_res = AIResponse(text="Summarized output", provider="freemodel", model="auto", success=True)

    @workflow("ai_summary")
    async def ai_summary(ctx):
        res = await ctx.ai("Summarize text: " + ctx.input["text"])
        return {"summary": res.text, "provider": res.provider}

    with patch("nexapy.ai.router.AI.chat", new_callable=AsyncMock) as mock_chat:
        mock_chat.return_value = mock_res
        w_runner = WorkflowRunner()
        res = await w_runner.run_async("ai_summary", input_data={"text": "Long article text"})
        assert res["summary"] == "Summarized output"
        assert res["provider"] == "freemodel"
        mock_chat.assert_called_once()

