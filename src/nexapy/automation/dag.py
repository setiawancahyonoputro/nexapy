"""Multi-step DAG (Directed Acyclic Graph) Workflow Execution Engine."""

import asyncio
import inspect
import time
from typing import Any, Callable, Dict, List, Optional, Set, Union
from .context import WorkflowContext


class DAGNode:
    """Represents a single node step in a DAG Workflow."""

    def __init__(
        self,
        name: str,
        func: Callable[..., Any],
        depends_on: Optional[List[str]] = None,
        description: Optional[str] = None,
        max_retries: int = 0,
        retry_delay: float = 1.0,
        condition: Optional[Callable[[WorkflowContext], bool]] = None,
    ):
        self.name = name
        self.func = func
        self.depends_on = depends_on or []
        self.description = description or func.__doc__ or f"Node {name}"
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.condition = condition

    def __repr__(self) -> str:
        return f"<DAGNode name='{self.name}' depends_on={self.depends_on}>"


_dag_run_state_store: Dict[str, Dict[str, Any]] = {}


class DAGWorkflow:
    """Directed Acyclic Graph Workflow runner allowing parallel & conditional step execution."""

    def __init__(self, name: str, description: Optional[str] = None, schedule: Optional[str] = None):
        self.name = name
        self.description = description or f"DAG Workflow {name}"
        self.schedule = schedule
        self.nodes: Dict[str, DAGNode] = {}

    def add_node(
        self,
        name: str,
        func: Optional[Callable[..., Any]] = None,
        depends_on: Optional[List[str]] = None,
        description: Optional[str] = None,
        max_retries: int = 0,
        retry_delay: float = 1.0,
        condition: Optional[Callable[[WorkflowContext], bool]] = None,
    ) -> Any:
        """Register a step node into the DAG workflow."""
        def decorator(f: Callable[..., Any]) -> Callable[..., Any]:
            node_name = name or f.__name__
            if node_name in self.nodes:
                raise ValueError(f"DAG Node '{node_name}' already exists in workflow '{self.name}'.")
            
            node = DAGNode(
                name=node_name,
                func=f,
                depends_on=depends_on or [],
                description=description,
                max_retries=max_retries,
                retry_delay=retry_delay,
                condition=condition,
            )
            self.nodes[node_name] = node
            return f

        if func is not None:
            return decorator(func)
        return decorator

    def node(
        self,
        name: str,
        depends_on: Optional[List[str]] = None,
        description: Optional[str] = None,
        max_retries: int = 0,
        retry_delay: float = 1.0,
        condition: Optional[Callable[[WorkflowContext], bool]] = None,
    ) -> Callable:
        """Decorator shortcut to register a node."""
        return self.add_node(
            name=name,
            depends_on=depends_on,
            description=description,
            max_retries=max_retries,
            retry_delay=retry_delay,
            condition=condition,
        )

    def _topological_levels(self) -> List[List[DAGNode]]:
        """
        Group nodes into parallel execution levels using topological sorting.
        Nodes in the same level can execute concurrently.
        """
        in_degree = {name: len(node.depends_on) for name, node in self.nodes.items()}
        graph: Dict[str, List[str]] = {name: [] for name in self.nodes}

        for name, node in self.nodes.items():
            for dep in node.depends_on:
                if dep not in self.nodes:
                    raise ValueError(f"Node '{name}' depends on unknown node '{dep}'")
                graph[dep].append(name)

        levels = []
        current_level = [name for name, deg in in_degree.items() if deg == 0]

        visited_count = 0
        while current_level:
            levels.append([self.nodes[n] for n in sorted(current_level)])
            visited_count += len(current_level)
            next_level = []

            for n in current_level:
                for neighbor in graph[n]:
                    in_degree[neighbor] -= 1
                    if in_degree[neighbor] == 0:
                        next_level.append(neighbor)
            current_level = next_level

        if visited_count < len(self.nodes):
            raise ValueError(f"Cycle detected in DAG Workflow '{self.name}'.")

        return levels

    async def execute_async(self, ctx: WorkflowContext) -> Dict[str, Any]:
        """Execute all nodes in the DAG in parallel topological order."""
        levels = self._topological_levels()
        step_results: Dict[str, Any] = {}
        ctx.set("steps", step_results)

        for level in levels:
            tasks = [self._execute_node(node, ctx, step_results) for node in level]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for node, res in zip(level, results):
                if isinstance(res, Exception):
                    raise res
                step_results[node.name] = res

        return step_results

    async def execute_step_async(self, step_name: str, ctx: WorkflowContext) -> Any:
        """
        Execute a single step node in the DAG workflow by name.
        Maintains output state across steps when run_id is supplied in input.
        """
        if step_name not in self.nodes:
            raise KeyError(
                f"Step '{step_name}' not found in DAG Workflow '{self.name}'. Available steps: {list(self.nodes.keys())}"
            )
        node = self.nodes[step_name]

        run_id = (ctx.input.get("run_id") if isinstance(ctx.input, dict) else None) or getattr(ctx, "run_id", None)
        if run_id:
            step_results = _dag_run_state_store.setdefault(run_id, {})
            ctx.set("steps", step_results)
        else:
            step_results = ctx.state.get("steps", {})

        res = await self._execute_node(node, ctx, step_results)
        step_results[step_name] = res
        ctx.set("steps", step_results)
        if run_id:
            _dag_run_state_store[run_id] = step_results

        return {
            "workflow": self.name,
            "step": step_name,
            "run_id": run_id,
            "result": res,
            "status": "success",
        }

    async def _execute_node(self, node: DAGNode, ctx: WorkflowContext, step_results: Dict[str, Any]) -> Any:
        """Execute a single node with condition check and retries."""
        if node.condition is not None:
            cond_val = node.condition(ctx)
            if not cond_val:
                return {"status": "skipped", "reason": "Condition returned False"}

        sig = inspect.signature(node.func)
        has_ctx = len(sig.parameters) > 0

        last_error = None
        for attempt in range(node.max_retries + 1):
            try:
                if inspect.iscoroutinefunction(node.func):
                    return await node.func(ctx) if has_ctx else await node.func()
                else:
                    return await asyncio.to_thread(node.func, ctx) if has_ctx else await asyncio.to_thread(node.func)
            except Exception as err:
                last_error = err
                if attempt < node.max_retries:
                    await asyncio.sleep(node.retry_delay)

        raise RuntimeError(f"DAG Node '{node.name}' failed after {node.max_retries + 1} attempts: {last_error}")
