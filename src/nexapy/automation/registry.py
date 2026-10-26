import inspect
from typing import Any, Callable, Dict, List, Optional, Union
from .dag import DAGWorkflow


class Workflow:
    """Represents a registered workflow definition."""

    def __init__(
        self,
        name: str,
        func: Callable[..., Any],
        description: Optional[str] = None,
        schedule: Optional[str] = None,
    ):
        if not hasattr(func, "execute_async"):
            sig = inspect.signature(func)
            if len(sig.parameters) > 1:
                raise TypeError(
                    f"Workflow '{name}' function accepts {len(sig.parameters)} parameters, "
                    f"but only 0 or 1 parameter (ctx) is allowed."
                )
        self.name = name
        self.func = func
        self.description = description or getattr(func, "__doc__", None) or f"Workflow {name}"
        self.schedule = schedule
        self.before_hooks: List[Callable] = []
        self.after_hooks: List[Callable] = []
        self.error_hooks: List[Callable] = []

    def before_run(self, hook_func: Callable) -> Callable:
        """Register a pre-execution hook function."""
        self.before_hooks.append(hook_func)
        return hook_func

    def after_run(self, hook_func: Callable) -> Callable:
        """Register a post-execution hook function."""
        self.after_hooks.append(hook_func)
        return hook_func

    def on_error(self, hook_func: Callable) -> Callable:
        """Register an error handling hook function."""
        self.error_hooks.append(hook_func)
        return hook_func

    def __repr__(self) -> str:
        return f"<Workflow name='{self.name}'>"


class WorkflowRegistry:
    """Registry maintaining active workflows."""

    def __init__(self):
        self._workflows: Dict[str, Union[Workflow, DAGWorkflow]] = {}

    def register(
        self,
        name: str,
        func: Union[Callable[..., Any], DAGWorkflow],
        description: Optional[str] = None,
        schedule: Optional[str] = None,
        override: bool = False,
    ) -> Union[Workflow, DAGWorkflow]:
        """Register a workflow function or DAGWorkflow under a unique name."""
        if name in self._workflows and not override:
            raise ValueError(f"Workflow '{name}' is already registered.")

        if isinstance(func, DAGWorkflow):
            wf = func
        else:
            wf = Workflow(name=name, func=func, description=description, schedule=schedule)

        self._workflows[name] = wf
        return wf

    def get(self, name: str) -> Optional[Union[Workflow, DAGWorkflow]]:
        """Get a registered workflow by name."""
        return self._workflows.get(name)

    def list_all(self) -> List[Union[Workflow, DAGWorkflow]]:
        """Get all registered workflows sorted by name."""
        return sorted(self._workflows.values(), key=lambda w: w.name)

    def clear(self) -> None:
        """Clear all registered workflows (useful for test isolation)."""
        self._workflows.clear()

    def __len__(self) -> int:
        return len(self._workflows)


# Global singleton instance
workflow_registry = WorkflowRegistry()
