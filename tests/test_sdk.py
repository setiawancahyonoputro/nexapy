import tempfile
from pathlib import Path
import pytest
from fastapi.testclient import TestClient
from typer.testing import CliRunner
from nexapy import NexaPy, __version__
from nexapy.cli import app
from nexapy.sdk import (
    generate_sdk,
    generate_javascript_sdk,
    generate_typescript_sdk,
    generate_react_sdk,
)

runner = CliRunner()


def test_generate_javascript_sdk():
    with tempfile.TemporaryDirectory() as tmp_dir:
        out_path = Path(tmp_dir) / "js_sdk"
        generate_javascript_sdk(out_path, base_url="http://api.example.com", ai_path="/ai/chat")

        client_file = out_path / "client.js"
        assert client_file.exists()
        content = client_file.read_text(encoding="utf-8")

        assert f"v{__version__}" in content
        assert "export class NexaPyClient" in content
        assert "http://api.example.com" in content
        assert "/ai/chat" in content
        assert "AbortController" in content
        assert "try {" in content
        assert "catch (err)" in content


def test_generate_typescript_sdk():
    with tempfile.TemporaryDirectory() as tmp_dir:
        out_path = Path(tmp_dir) / "ts_sdk"
        generate_typescript_sdk(out_path, base_url="http://api.example.com", ai_path="/ai/chat")

        client_file = out_path / "client.ts"
        types_file = out_path / "types.ts"
        index_file = out_path / "index.ts"

        assert client_file.exists()
        assert types_file.exists()
        assert index_file.exists()

        types_content = types_file.read_text(encoding="utf-8")
        assert "export interface AIResponse" in types_content
        assert 'export type AIProvider = "freemodel" | "gemini" | "unknown"' in types_content
        assert "export interface AIChatOptions" in types_content

        client_content = client_file.read_text(encoding="utf-8")
        assert f"v{__version__}" in client_content
        assert "export class NexaPyClient" in client_content
        assert "Promise<AIResponse>" in client_content
        assert "http://api.example.com" in client_content
        assert "/ai/chat" in client_content
        assert "AbortController" in client_content


def test_generate_react_sdk():
    with tempfile.TemporaryDirectory() as tmp_dir:
        out_path = Path(tmp_dir) / "react_sdk"
        generate_react_sdk(out_path, base_url="http://api.example.com", ai_path="/ai/chat")

        assert (out_path / "client.ts").exists()
        assert (out_path / "types.ts").exists()
        assert (out_path / "index.ts").exists()

        react_dir = out_path / "react"
        assert react_dir.exists()
        assert (react_dir / "useNexaPy.ts").exists()
        assert (react_dir / "index.ts").exists()

        hook_content = (react_dir / "useNexaPy.ts").read_text(encoding="utf-8")
        assert f"v{__version__}" in hook_content
        assert "export function useNexaPy" in hook_content
        assert "export interface UseNexaPyReturn" in hook_content
        assert "send: (prompt: string" in hook_content
        assert "data: AIResponse | null" in hook_content
        assert "loading: boolean" in hook_content
        assert "error: string | null" in hook_content
        assert "reset: () => void" in hook_content


def test_sdk_custom_ai_path_option():
    with tempfile.TemporaryDirectory() as tmp_dir:
        out_path = Path(tmp_dir) / "custom_sdk"
        generate_sdk("ts", out_path, base_url="http://localhost:8000", ai_path="/custom-ai-route")

        client_content = (out_path / "client.ts").read_text(encoding="utf-8")
        assert "/custom-ai-route" in client_content


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

        # React alias
        react_path = tmp_path / "react_alias"
        generate_sdk("react", react_path)
        assert (react_path / "react" / "useNexaPy.ts").exists()

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


def test_cli_sdk_generate_typescript_custom_path():
    with tempfile.TemporaryDirectory() as tmp_dir:
        out_dir = Path(tmp_dir) / "my_ts_sdk"
        result = runner.invoke(
            app, ["sdk", "generate", "-l", "typescript", "-o", str(out_dir), "-p", "/my-custom-path"]
        )
        assert result.exit_code == 0
        assert "Successfully generated typescript SDK" in result.output
        client_content = (out_dir / "client.ts").read_text(encoding="utf-8")
        assert "/my-custom-path" in client_content


def test_cli_sdk_generate_react():
    with tempfile.TemporaryDirectory() as tmp_dir:
        out_dir = Path(tmp_dir) / "my_react_sdk"
        result = runner.invoke(app, ["sdk", "generate", "-l", "react", "-o", str(out_dir)])
        assert result.exit_code == 0
        assert "Successfully generated react SDK" in result.output
        assert (out_dir / "react" / "useNexaPy.ts").exists()


def test_cli_sdk_generate_invalid_language():
    result = runner.invoke(app, ["sdk", "generate", "--lang", "invalid_lang"])
    assert result.exit_code == 1
    assert "Unsupported language 'invalid_lang'" in result.output


def test_sdk_fastapi_default_route_alignment():
    """Verify default route registered by NexaPy app matches default route used by generated SDK."""
    nexa = NexaPy()

    @nexa.ai()  # Defaults to /ai/chat
    async def chat(prompt: str):
        from nexapy.ai.base import AIResponse
        return AIResponse(text="Aligned route output", provider="freemodel", model="auto", success=True)

    client = TestClient(nexa.fastapi)
    res = client.post("/ai/chat", json={"prompt": "Test alignment"})
    assert res.status_code == 200
    assert res.json()["text"] == "Aligned route output"
