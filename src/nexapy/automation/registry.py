"""Global workflow registry."""

from typing import Any, Callable, Dict, List, Optional


class Workflow:
    """Represents a registered workflow definition."""

    def __init__(self, name: str, func: Callable[..., Any], description: Optional[str] = None):
        self.name = name
        self.func = func
        self.description = description or func.__doc__ or f"Workflow {name}"

    def __repr__(self) -> str:
        return f"<Workflow name='{self.name}'>"


class WorkflowRegistry:
    """Registry maintaining active workflows."""

    def __init__(self):
        self._workflows: Dict[str, Workflow] = {}

    def register(self, name: str, func: Callable[..., Any], description: Optional[str] = None) -> Workflow:
        """Register a workflow function under a unique name."""
        wf = Workflow(name=name, func=func, description=description)
        self._workflows[name] = wf
        return wf

    def get(self, name: str) -> Optional[Workflow]:
        """Get a registered workflow by name."""
        return self._workflows.get(name)

    def list_all(self) -> List[Workflow]:
        """Get all registered workflows sorted by name."""
        return sorted(self._workflows.values(), key=lambda w: w.name)

    def clear(self) -> None:
        """Clear all registered workflows (useful for test isolation)."""
        self._workflows.clear()

    def __len__(self) -> int:
        return len(self._workflows)


# Global singleton instance
workflow_registry = WorkflowRegistry()
