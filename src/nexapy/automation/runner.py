"""Workflow execution runner with telemetry and lifecycle hooks."""

import asyncio
import inspect
from typing import Any, Dict, Optional, Union
from .context import WorkflowContext
from .dag import DAGWorkflow
from .registry import Workflow, workflow_registry
from .telemetry import tracker


class WorkflowRunner:
    """Runner for executing standard and DAG workflows with telemetry, concurrency control, and timeouts."""

    def __init__(self, max_concurrency: int = 50, default_timeout: Optional[float] = 120.0):
        self.max_concurrency = max_concurrency
        self.default_timeout = default_timeout
        self._semaphore: Optional[asyncio.Semaphore] = None

    def _get_semaphore(self) -> asyncio.Semaphore:
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(self.max_concurrency)
        return self._semaphore

    async def run_async(
        self,
        workflow_or_name: Union[str, Workflow, DAGWorkflow, Any],
        input_data: Optional[Dict[str, Any]] = None,
        security_context: Optional[Dict[str, Any]] = None,
        step: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> Any:
        """Execute a workflow asynchronously with concurrency and timeout guards."""
        inp = input_data or {}
        if isinstance(workflow_or_name, str):
            wf = workflow_registry.get(workflow_or_name)
            if not wf:
                raise KeyError(f"Workflow '{workflow_or_name}' is not registered.")
        elif isinstance(workflow_or_name, (Workflow, DAGWorkflow)):
            wf = workflow_or_name
        elif hasattr(workflow_or_name, "func") or hasattr(workflow_or_name, "execute_async"):
            wf = workflow_or_name
        elif callable(workflow_or_name):
            wf = Workflow(name=getattr(workflow_or_name, "__name__", "anonymous"), func=workflow_or_name)
        else:
            raise ValueError(f"Invalid workflow object or name: {workflow_or_name}")

        wf_name = getattr(wf, "name", "anonymous")
        exec_log = tracker.start_execution(wf_name, inp)
        ctx = WorkflowContext(
            input_data=inp,
            workflow_name=wf_name,
            execution_id=exec_log.execution_id,
            security_context=security_context,
        )

        effective_timeout = timeout if timeout is not None else getattr(wf, "timeout", self.default_timeout)

        async def _execute_internal() -> Any:
            # Handle specific DAG step execution if requested
            if step is not None:
                if hasattr(wf, "execute_step_async"):
                    res = await wf.execute_step_async(step, ctx)
                    steps = ctx.state.get("steps", {})
                    tracker.finish_execution(exec_log, result=res, steps=steps)
                    return res
                raise ValueError(f"Step '{step}' requested, but workflow '{wf_name}' does not support step execution.")

            # Handle DAGWorkflow objects
            if isinstance(wf, DAGWorkflow) or hasattr(wf, "execute_async"):
                result = await wf.execute_async(ctx)
                steps = ctx.state.get("steps", {})
                tracker.finish_execution(exec_log, result=result, steps=steps)
                return result

            # Standard function workflow
            func = getattr(wf, "func", wf)

            # Before hooks
            if hasattr(wf, "before_hooks") and wf.before_hooks:
                for hook in wf.before_hooks:
                    if inspect.iscoroutinefunction(hook):
                        await hook(ctx)
                    else:
                        hook(ctx)

            sig = inspect.signature(func)
            if len(sig.parameters) > 0:
                if inspect.iscoroutinefunction(func):
                    result = await func(ctx)
                else:
                    result = await asyncio.to_thread(func, ctx)
            else:
                if inspect.iscoroutinefunction(func):
                    result = await func()
                else:
                    result = await asyncio.to_thread(func)

            # After hooks
            if hasattr(wf, "after_hooks") and wf.after_hooks:
                for hook in wf.after_hooks:
                    if inspect.iscoroutinefunction(hook):
                        await hook(ctx, result)
                    else:
                        hook(ctx, result)

            tracker.finish_execution(exec_log, result=result)
            return result

        async with self._get_semaphore():
            try:
                if effective_timeout is not None and effective_timeout > 0:
                    return await asyncio.wait_for(_execute_internal(), timeout=effective_timeout)
                return await _execute_internal()
            except asyncio.TimeoutError:
                err_msg = f"Workflow '{wf_name}' timed out after {effective_timeout}s."
                tracker.finish_execution(exec_log, error=err_msg)
                raise TimeoutError(err_msg)
            except Exception as err:
                if hasattr(wf, "error_hooks") and wf.error_hooks:
                    for hook in wf.error_hooks:
                        try:
                            if inspect.iscoroutinefunction(hook):
                                await hook(ctx, err)
                            else:
                                hook(ctx, err)
                        except Exception:
                            pass
                tracker.finish_execution(exec_log, error=str(err))
                raise err

    def run(
        self,
        workflow_or_name: Union[str, Workflow, DAGWorkflow, Any],
        input_data: Optional[Dict[str, Any]] = None,
        security_context: Optional[Dict[str, Any]] = None,
        step: Optional[str] = None,
        timeout: Optional[float] = None,
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

        return asyncio.run(self.run_async(workflow_or_name, input_data, security_context, step=step, timeout=timeout))



# Singleton runner instance
runner = WorkflowRunner()
