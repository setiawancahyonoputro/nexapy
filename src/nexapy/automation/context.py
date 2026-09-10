"""Workflow execution context."""

from typing import Any, Dict, Optional


class WorkflowContext:
    """Context object passed to workflow handlers containing execution input and state."""

    def __init__(self, input_data: Optional[Dict[str, Any]] = None, workflow_name: str = ""):
        self.input: Dict[str, Any] = input_data or {}
        self.state: Dict[str, Any] = {}
        self.workflow_name: str = workflow_name
        self._ai_action = None

    def get(self, key: str, default: Any = None) -> Any:
        """Get value from input data or internal state."""
        if key in self.input:
            return self.input[key]
        return self.state.get(key, default)

    def set(self, key: str, value: Any) -> None:
        """Set value in internal state."""
        self.state[key] = value

    async def ai(
        self,
        prompt: str,
        provider: Optional[str] = None,
        reasoning: Optional[str] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
    ):
        """Execute AI completion prompt using NexaPy AI Action."""
        if self._ai_action is None:
            from .actions.ai import AIAction
            self._ai_action = AIAction()
        return await self._ai_action(
            prompt,
            provider=provider,
            reasoning=reasoning,
            temperature=temperature,
            max_tokens=max_tokens,
        )

    def __repr__(self) -> str:
        return f"<WorkflowContext workflow='{self.workflow_name}' input={self.input}>"
