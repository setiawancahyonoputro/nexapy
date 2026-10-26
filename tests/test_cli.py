from typer.testing import CliRunner
from nexapy.cli import app, _is_valid_key
from nexapy import __version__

runner = CliRunner()


def test_cli_version():
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert f"NexaPy {__version__}" in result.stdout


def test_cli_doctor_placeholders_detected():
    assert _is_valid_key("") is False
    assert _is_valid_key("your_freemodel_api_key_here") is False
    assert _is_valid_key("your_gemini_api_key_here") is False
    assert _is_valid_key("valid_api_key_12345") is True


def test_cli_doctor():
    result = runner.invoke(app, ["doctor"])
    assert result.exit_code == 0
    assert "NexaPy Environment Doctor" in result.stdout
    assert "FreeModel Provider" in result.stdout
    assert "Gemini Provider" in result.stdout


def test_cli_workflow_info():
    from nexapy import workflow
    from nexapy.automation import workflow_registry
    from click.utils import strip_ansi

    workflow_registry.clear()

    @workflow("sample-flow", description="Sample workflow info test", schedule="0 12 * * *")
    async def sample_flow(ctx):
        return "sample"

    result = runner.invoke(app, ["workflow", "info", "sample-flow"])
    assert result.exit_code == 0
    output = strip_ansi(result.output)
    assert "Workflow Info: sample-flow" in output
    assert "Sample workflow info test" in output
    assert "0 12 * * *" in output

    workflow_registry.clear()


def test_cli_n8n_export():
    import json
    import tempfile
    from pathlib import Path
    from nexapy import workflow
    from nexapy.automation import workflow_registry
    from click.utils import strip_ansi

    workflow_registry.clear()

    @workflow("export-flow", description="Export test")
    def export_flow():
        return "ok"

    # 1. Export to stdout
    result_stdout = runner.invoke(app, ["n8n", "export", "export-flow"])
    assert result_stdout.exit_code == 0
    assert "NexaPy - export-flow" in result_stdout.output

    # 2. Export to file
    with tempfile.TemporaryDirectory() as tmp_dir:
        out_file = Path(tmp_dir) / "flow.n8n.json"
        res_file = runner.invoke(app, ["n8n", "export", "export-flow", "-o", str(out_file)])
        assert res_file.exit_code == 0
        assert out_file.exists()
        content = json.loads(out_file.read_text(encoding="utf-8"))
        assert content["name"] == "NexaPy - export-flow"

    workflow_registry.clear()

