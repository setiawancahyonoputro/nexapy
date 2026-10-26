import tempfile
from pathlib import Path
import pytest
from nexapy.sdk import (
    generate_sdk,
    generate_php_sdk,
    generate_kotlin_sdk,
    generate_flutter_sdk,
    generate_nodejs_sdk,
)


def test_generate_php_sdk():
    with tempfile.TemporaryDirectory() as tmp_dir:
        out = Path(tmp_dir) / "php"
        generate_php_sdk(out, base_url="http://api.local:8000")
        assert (out / "composer.json").exists()
        assert (out / "src" / "NexaPyClient.php").exists()
        assert (out / "README.md").exists()

        content = (out / "src" / "NexaPyClient.php").read_text(encoding="utf-8")
        assert "class NexaPyClient" in content
        assert "executeWorkflow" in content
        assert "getExecution" in content
        assert "listWorkflows" in content


def test_generate_kotlin_sdk():
    with tempfile.TemporaryDirectory() as tmp_dir:
        out = Path(tmp_dir) / "kotlin"
        generate_kotlin_sdk(out, base_url="http://api.local:8000")
        assert (out / "build.gradle.kts").exists()
        assert (out / "src" / "main" / "kotlin" / "com" / "nexapy" / "NexaPyClient.kt").exists()
        assert (out / "src" / "main" / "kotlin" / "com" / "nexapy" / "Models.kt").exists()


def test_generate_flutter_sdk():
    with tempfile.TemporaryDirectory() as tmp_dir:
        out = Path(tmp_dir) / "flutter"
        generate_flutter_sdk(out, base_url="http://api.local:8000")
        assert (out / "pubspec.yaml").exists()
        assert (out / "lib" / "nexapy_client.dart").exists()

        content = (out / "lib" / "nexapy_client.dart").read_text(encoding="utf-8")
        assert "class NexaPyClient" in content
        assert "class AIResponse" in content
        assert "executeWorkflow" in content


def test_generate_nodejs_sdk():
    with tempfile.TemporaryDirectory() as tmp_dir:
        out = Path(tmp_dir) / "node"
        generate_nodejs_sdk(out, base_url="http://api.local:8000")
        assert (out / "package.json").exists()
        assert (out / "index.js").exists()
        assert (out / "index.d.ts").exists()

        content = (out / "index.js").read_text(encoding="utf-8")
        assert "class NexaPyClient" in content
        assert "executeWorkflow" in content
        assert "getExecution" in content


def test_generate_sdk_orchestrator():
    with tempfile.TemporaryDirectory() as tmp_dir:
        for lang in ["php", "kotlin", "flutter", "nodejs", "typescript", "javascript", "react"]:
            out = Path(tmp_dir) / lang
            res_path = generate_sdk(lang, output_dir=out)
            assert res_path.exists()
            assert len(list(res_path.iterdir())) > 0

    with pytest.raises(ValueError, match="Unsupported language"):
        generate_sdk("cobol")
