"""NexaPy SDK Generator Orchestrator Engine."""

from pathlib import Path
from typing import Union
from .javascript import generate_javascript_sdk
from .typescript import generate_typescript_sdk


def generate_sdk(
    lang: str,
    output_dir: Union[str, Path] = "nexapy_sdk",
    base_url: str = "http://localhost:8000",
) -> Path:
    """
    Generate client SDK code for the specified language.
    
    Supported languages: 'javascript' ('js'), 'typescript' ('ts').
    Raises ValueError for unsupported languages.
    """
    target_lang = (lang or "").strip().lower()
    path = Path(output_dir)

    if target_lang in ("javascript", "js"):
        generate_javascript_sdk(path, base_url=base_url)
    elif target_lang in ("typescript", "ts"):
        generate_typescript_sdk(path, base_url=base_url)
    else:
        raise ValueError(
            f"Unsupported language '{lang}'. Supported values are 'javascript' ('js') and 'typescript' ('ts')."
        )

    return path
