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
