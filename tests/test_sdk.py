import tempfile
from pathlib import Path
import pytest
from typer.testing import CliRunner
from nexapy.cli import app
from nexapy.sdk import generate_sdk, generate_javascript_sdk, generate_typescript_sdk

runner = CliRunner()


def test_generate_javascript_sdk():
    with tempfile.TemporaryDirectory() as tmp_dir:
        out_path = Path(tmp_dir) / "js_sdk"
        generate_javascript_sdk(out_path, base_url="http://api.example.com")

        client_file = out_path / "client.js"
        assert client_file.exists()
        content = client_file.read_text(encoding="utf-8")

        assert "export class NexaPyClient" in content
        assert "http://api.example.com" in content
        assert "async chat(prompt" in content
        assert "async generate(prompt" in content


def test_generate_typescript_sdk():
    with tempfile.TemporaryDirectory() as tmp_dir:
        out_path = Path(tmp_dir) / "ts_sdk"
        generate_typescript_sdk(out_path, base_url="http://api.example.com")

        client_file = out_path / "client.ts"
        types_file = out_path / "types.ts"
        index_file = out_path / "index.ts"

        assert client_file.exists()
        assert types_file.exists()
        assert index_file.exists()

        types_content = types_file.read_text(encoding="utf-8")
        assert "export interface AIResponse" in types_content
        assert 'provider: "freemodel" | "gemini" | string;' in types_content
        assert "export interface AIChatOptions" in types_content

        client_content = client_file.read_text(encoding="utf-8")
        assert "export class NexaPyClient" in client_content
        assert "Promise<AIResponse>" in client_content
        assert "http://api.example.com" in client_content


def test_generate_sdk_orchestrator_aliases_and_validation():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)

        # JS alias
        js_path = tmp_path / "js_alias"
        generate_sdk("js", js_path)
        assert (js_path / "client.js").exists()

        # TS alias
        ts_path = tmp_path / "ts_alias"
        generate_sdk("ts", ts_path)
        assert (ts_path / "client.ts").exists()

        # Invalid language error
        with pytest.raises(ValueError) as exc_info:
            generate_sdk("python", tmp_path / "py_sdk")
        assert "Unsupported language 'python'" in str(exc_info.value)


def test_cli_sdk_generate_javascript():
    with tempfile.TemporaryDirectory() as tmp_dir:
        out_dir = Path(tmp_dir) / "my_js_sdk"
        result = runner.invoke(app, ["sdk", "generate", "--lang", "js", "--output", str(out_dir)])
        assert result.exit_code == 0
        assert "Successfully generated js SDK" in result.output
        assert (out_dir / "client.js").exists()


def test_cli_sdk_generate_typescript():
    with tempfile.TemporaryDirectory() as tmp_dir:
        out_dir = Path(tmp_dir) / "my_ts_sdk"
        result = runner.invoke(app, ["sdk", "generate", "-l", "typescript", "-o", str(out_dir)])
        assert result.exit_code == 0
        assert "Successfully generated typescript SDK" in result.output
        assert (out_dir / "client.ts").exists()
        assert (out_dir / "types.ts").exists()
        assert (out_dir / "index.ts").exists()


def test_cli_sdk_generate_invalid_language():
    result = runner.invoke(app, ["sdk", "generate", "--lang", "invalid_lang"])
    assert result.exit_code == 1
    assert "Unsupported language 'invalid_lang'" in result.output
