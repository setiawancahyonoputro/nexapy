"""Workflow execution context."""

from typing import Any, Dict, Optional, Type, TypeVar
from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)


class WorkflowContext:
    """Context object passed to workflow handlers containing execution input and state."""

    def __init__(
        self,
        input_data: Optional[Dict[str, Any]] = None,
        workflow_name: str = "",
        execution_id: str = "",
        security_context: Optional[Dict[str, Any]] = None,
    ):
        if input_data is not None and not isinstance(input_data, dict):
            raise TypeError("Workflow input must be a dictionary.")
        self.input: Dict[str, Any] = input_data or {}
        self.state: Dict[str, Any] = {}
        self.workflow_name: str = workflow_name
        self.execution_id: str = execution_id
        self.security_context: Dict[str, Any] = security_context or {}

        self._ai_action = None
        self._http_action = None
        self._transform_action = None
        self._notify_action = None

    def get(self, key: str, default: Any = None) -> Any:
        """Get value from input data or internal state."""
        if key in self.input:
            return self.input[key]
        return self.state.get(key, default)

    def set(self, key: str, value: Any) -> None:
        """Set value in internal state."""
        self.state[key] = value

    @property
    def http(self):
        """Access NexaPy HTTP Action interface."""
        if self._http_action is None:
            from .actions.http import HTTPAction
            self._http_action = HTTPAction()
        return self._http_action

    @property
    def transform(self):
        """Access NexaPy Data Transformation Action interface."""
        if self._transform_action is None:
            from .actions.transform import TransformAction
            self._transform_action = TransformAction()
        return self._transform_action

    @property
    def notify(self):
        """Access NexaPy Webhook & Notification Action interface."""
        if self._notify_action is None:
            from .actions.notification import NotificationAction
            self._notify_action = NotificationAction()
        return self._notify_action

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

    async def ai_structured(
        self,
        prompt: str,
        schema: Type[T],
        provider: Optional[str] = None,
    ) -> T:
        """Execute AI completion prompt and return validated Pydantic model response."""
        if self._ai_action is None:
            from .actions.ai import AIAction
            self._ai_action = AIAction()
        return await self._ai_action.chat_structured(prompt, schema=schema, provider=provider)

    def __repr__(self) -> str:
        return f"<WorkflowContext workflow='{self.workflow_name}' execution_id='{self.execution_id}'>"
