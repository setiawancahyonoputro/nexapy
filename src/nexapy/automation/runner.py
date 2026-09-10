"""Workflow execution runner."""

import asyncio
import inspect
from typing import Any, Dict, Optional, Union
from .context import WorkflowContext
from .registry import Workflow, workflow_registry


class WorkflowRunner:
    """Runner for executing sync and async workflows."""

    async def run_async(
        self,
        workflow_or_name: Union[str, Workflow, Any],
        input_data: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """Execute a workflow asynchronously."""
        if isinstance(workflow_or_name, str):
            wf = workflow_registry.get(workflow_or_name)
            if not wf:
                raise KeyError(f"Workflow '{workflow_or_name}' is not registered.")
        elif isinstance(workflow_or_name, Workflow):
            wf = workflow_or_name
        elif hasattr(workflow_or_name, "func"):
            wf = workflow_or_name
        elif callable(workflow_or_name):
            wf = Workflow(name=getattr(workflow_or_name, "__name__", "anonymous"), func=workflow_or_name)
        else:
            raise ValueError(f"Invalid workflow object or name: {workflow_or_name}")

        ctx = WorkflowContext(input_data=input_data, workflow_name=wf.name)
        func = wf.func

        sig = inspect.signature(func)
        if len(sig.parameters) > 0:
            if inspect.iscoroutinefunction(func):
                result = await func(ctx)
            else:
                result = func(ctx)
        else:
            if inspect.iscoroutinefunction(func):
                result = await func()
            else:
                result = func()

        return result

    def run(
        self,
        workflow_or_name: Union[str, Workflow, Any],
        input_data: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """Synchronous entry point to run a workflow."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            raise RuntimeError(
                "runner.run() cannot be used inside an active event loop. "
                "Use await runner.run_async(...) instead."
            )

        return asyncio.run(self.run_async(workflow_or_name, input_data))


# Singleton runner instance
runner = WorkflowRunner()
