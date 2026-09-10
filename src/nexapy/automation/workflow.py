"""Workflow decorator and registration interface."""

from typing import Any, Callable, Optional, Union, overload
from .registry import Workflow, workflow_registry


@overload
def workflow(name: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    ...

@overload
def workflow(func: Callable[..., Any]) -> Callable[..., Any]:
    ...

def workflow(
    name_or_func: Union[str, Callable[..., Any], None] = None,
    description: Optional[str] = None,
) -> Any:
    """
    Decorator to register a function as a NexaPy workflow.
    Returns the original decorated function so it remains directly callable.

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
        workflow_registry.register(name=wf_name, func=func, description=description)
        return func

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        wf_name = name_or_func if isinstance(name_or_func, str) and name_or_func.strip() else func.__name__
        workflow_registry.register(name=wf_name, func=func, description=description)
        return func

    return decorator
