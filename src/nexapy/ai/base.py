import time
from abc import ABC, abstractmethod
from typing import Any, AsyncGenerator, Dict, Optional
from pydantic import BaseModel, Field


class AIResponse(BaseModel):
    """Normalized response format across all NexaPy AI providers."""

    text: str = Field(default="", description="The generated text content")
    provider: str = Field(..., description="Provider name (e.g. freemodel, gemini, openai, anthropic, deepseek)")
    model: str = Field(..., description="Model name used for generation")
    success: bool = Field(default=True, description="Whether generation succeeded")
    error: Optional[str] = Field(default=None, description="Error message if generation failed")
    raw: Optional[Dict[str, Any]] = Field(default=None, exclude=True, description="Raw provider response payload")

    def dict_response(self, include_raw: bool = False) -> Dict[str, Any]:
        """Return response formatted as dictionary."""
        data = self.model_dump()
        if include_raw and self.raw is not None:
            data["raw"] = self.raw
        return data


from enum import Enum


class CircuitBreakerState(str, Enum):
    """Formal Circuit Breaker state machine for AI providers."""

    CLOSED = "CLOSED"         # Normal healthy operation
    OPEN = "OPEN"             # In failure cooldown; calls should be avoided
    HALF_OPEN = "HALF_OPEN"   # Cooldown elapsed; trial probe request allowed


class BaseProvider(ABC):
    """Abstract base class for NexaPy AI providers with circuit breaker state machine and streaming."""

    def __init__(self, name: str, cooldown_seconds: float = 30.0, max_consecutive_failures: int = 3):
        self.name = name
        self.cooldown_seconds = cooldown_seconds
        self.max_consecutive_failures = max_consecutive_failures
        self.consecutive_failures: int = 0
        self.last_failure_time: float = 0.0

    @property
    def circuit_state(self) -> CircuitBreakerState:
        """Evaluate and return the current circuit breaker state machine status."""
        if self.consecutive_failures < self.max_consecutive_failures:
            return CircuitBreakerState.CLOSED

        now = time.time()
        if now - self.last_failure_time < self.cooldown_seconds:
            return CircuitBreakerState.OPEN

        return CircuitBreakerState.HALF_OPEN

    @abstractmethod
    async def generate(
        self,
        prompt: str,
        model: Optional[str] = None,
        reasoning: Optional[str] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        **kwargs: Any,
    ) -> AIResponse:
        """Generate text completion from prompt with standardized parameters."""
        pass

    async def stream(
        self,
        prompt: str,
        model: Optional[str] = None,
        reasoning: Optional[str] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        **kwargs: Any,
    ) -> AsyncGenerator[str, None]:
        """Stream generated text chunks. Default implementation yields the full generation response."""
        res = await self.generate(
            prompt,
            model=model,
            reasoning=reasoning,
            temperature=temperature,
            max_tokens=max_tokens,
            **kwargs,
        )
        if res.success and res.text:
            yield res.text
        elif not res.success:
            raise RuntimeError(res.error or f"{self.name} generation failed")

    def is_healthy(self) -> bool:
        """Check if provider is available (CLOSED or HALF_OPEN trial probing)."""
        return self.circuit_state != CircuitBreakerState.OPEN

    def record_success(self) -> None:
        """Record successful invocation and reset circuit breaker to CLOSED."""
        self.consecutive_failures = 0

    def record_failure(self, error: Optional[str] = None) -> None:
        """Record provider failure and update circuit breaker state towards OPEN."""
        self.consecutive_failures += 1
        self.last_failure_time = time.time()
