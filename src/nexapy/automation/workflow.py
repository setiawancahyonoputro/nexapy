"""Workflow decorator and registration interface."""

from typing import Any, Callable, Optional, Union, overload
from .registry import Workflow, workflow_registry


@overload
def workflow(name: str) -> Callable[[Callable[..., Any]], Workflow]:
    ...

@overload
def workflow(func: Callable[..., Any]) -> Workflow:
    ...

def workflow(
    name_or_func: Union[str, Callable[..., Any], None] = None,
    description: Optional[str] = None,
) -> Any:
    """
    Decorator to register a function as a NexaPy workflow.

    Usage:
        @workflow("hello")
        async def hello(ctx):
            return {"message": "Hello from workflow"}

        @workflow
        def my_flow(ctx):
            return "sync result"
    """
    if callable(name_or_func):
        func = name_or_func
        wf_name = func.__name__
        return workflow_registry.register(name=wf_name, func=func, description=description)

    def decorator(func: Callable[..., Any]) -> Workflow:
        wf_name = name_or_func if isinstance(name_or_func, str) and name_or_func.strip() else func.__name__
        return workflow_registry.register(name=wf_name, func=func, description=description)

    return decorator
