"""NexaPy SDK Generator Orchestrator Engine for Multi-Platform Integration."""

from pathlib import Path
from typing import Union
from .javascript import generate_javascript_sdk
from .typescript import generate_typescript_sdk
from .react import generate_react_sdk
from .php import generate_php_sdk
from .kotlin import generate_kotlin_sdk
from .flutter import generate_flutter_sdk
from .nodejs import generate_nodejs_sdk


def generate_sdk(
    lang: str,
    output_dir: Union[str, Path] = "nexapy_sdk",
    base_url: str = "http://localhost:8000",
    ai_path: str = "/ai/chat",
) -> Path:
    """
    Generate client SDK code for the specified language.
    
    Supported languages:
    - 'javascript' ('js')
    - 'typescript' ('ts')
    - 'react' ('react-hooks')
    - 'php'
    - 'kotlin' ('kt')
    - 'flutter' ('dart')
    - 'nodejs' ('node')
    
    Raises ValueError for unsupported languages.
    """
    target_lang = (lang or "").strip().lower()
    path = Path(output_dir)

    if target_lang in ("javascript", "js"):
        generate_javascript_sdk(path, base_url=base_url, ai_path=ai_path)
    elif target_lang in ("typescript", "ts"):
        generate_typescript_sdk(path, base_url=base_url, ai_path=ai_path)
    elif target_lang in ("react", "react-hooks"):
        generate_react_sdk(path, base_url=base_url, ai_path=ai_path)
    elif target_lang in ("php",):
        generate_php_sdk(path, base_url=base_url, ai_path=ai_path)
    elif target_lang in ("kotlin", "kt"):
        generate_kotlin_sdk(path, base_url=base_url, ai_path=ai_path)
    elif target_lang in ("flutter", "dart"):
        generate_flutter_sdk(path, base_url=base_url, ai_path=ai_path)
    elif target_lang in ("nodejs", "node"):
        generate_nodejs_sdk(path, base_url=base_url, ai_path=ai_path)
    else:
        raise ValueError(
            f"Unsupported language '{lang}'. Supported values are: "
            "'javascript', 'typescript', 'react', 'php', 'kotlin', 'flutter', 'nodejs'."
        )

    return path
