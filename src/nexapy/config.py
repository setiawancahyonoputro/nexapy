import os
from pathlib import Path
from typing import Any, Dict, Optional
import yaml
from dotenv import load_dotenv

ENV_MAPPINGS = {
    "app.name": "NEXAPY_APP_NAME",
    "ai.provider": "NEXAPY_AI_PROVIDER",
    "ai.freemodel.model": "NEXAPY_FREEMODEL_MODEL",
    "ai.gemini.model": "NEXAPY_GEMINI_MODEL",
    "cors.enabled": "NEXAPY_CORS_ENABLED",
}


class Config:
    """NexaPy Configuration manager supporting environment variables and YAML files."""

    def __init__(self, project_root: Optional[Path] = None):
        self.root = project_root or Path.cwd()
        self._env_loaded = False
        self._yaml_config: Dict[str, Any] = {}
        self.load()

    def load(self) -> None:
        """Load .env and framework.yaml configuration if present."""
        env_path = self.root / ".env"
        if env_path.exists():
            load_dotenv(dotenv_path=env_path)
            self._env_loaded = True

        yaml_path = self.root / "framework.yaml"
        if not yaml_path.exists():
            yaml_path = self.root / "framework.yml"

        if yaml_path.exists():
            try:
                with open(yaml_path, "r", encoding="utf-8") as f:
                    self._yaml_config = yaml.safe_load(f) or {}
            except Exception as e:
                print(f"[NexaPy Warning] Failed to parse {yaml_path.name}: {e}")

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get config value.
        Checks explicit key in OS env, mapped NEXAPY_* env, then nested YAML config.
        """
        # 1. Direct OS environment variable check
        env_val = os.getenv(key)
        if env_val is not None:
            return env_val

        # 2. Mapped NEXAPY_* environment variable check
        if key in ENV_MAPPINGS:
            mapped_env = os.getenv(ENV_MAPPINGS[key])
            if mapped_env is not None:
                return mapped_env

        # 3. Check nested YAML config
        keys = key.lower().split(".")
        val: Any = self._yaml_config
        for k in keys:
            if isinstance(val, dict) and k in val:
                val = val[k]
            else:
                return default
        return val


config = Config()
