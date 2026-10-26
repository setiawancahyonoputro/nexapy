"""Transform and mapping action integration for NexaPy Workflows."""

import re
from typing import Any, Dict, List, Optional


class TransformAction:
    """Transformation helper for reshaping JSON data, field filtering, and string template interpolation."""

    @staticmethod
    def pick(data: Dict[str, Any], keys: List[str]) -> Dict[str, Any]:
        """Extract a subset of keys from a dictionary."""
        if not isinstance(data, dict):
            return {}
        return {k: data[k] for k in keys if k in data}

    @staticmethod
    def omit(data: Dict[str, Any], keys: List[str]) -> Dict[str, Any]:
        """Omit specified keys from a dictionary."""
        if not isinstance(data, dict):
            return {}
        omit_set = set(keys)
        return {k: v for k, v in data.items() if k not in omit_set}

    @staticmethod
    def map_fields(data: Dict[str, Any], mapping: Dict[str, str]) -> Dict[str, Any]:
        """
        Rename/map keys in data according to a mapping dictionary {old_key: new_key}.
        Unmapped keys are preserved.
        """
        if not isinstance(data, dict):
            return {}
        res = {}
        for k, v in data.items():
            new_key = mapping.get(k, k)
            res[new_key] = v
        return res

    @staticmethod
    def render_template(template: str, context: Dict[str, Any]) -> str:
        """
        Render a simple string template with {{ variable }} placeholders.
        """
        def replace(match):
            key = match.group(1).strip()
            return str(context.get(key, ""))

        return re.sub(r"\{\{\s*([a-zA-Z0-9_\.]+)\s*\}\}", replace, template)
