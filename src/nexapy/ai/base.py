from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field


class AIResponse(BaseModel):
    """Normalized response format across all NexaPy AI providers."""

    text: str = Field(default="", description="The generated text content")
    provider: str = Field(..., description="Provider name (e.g. freemodel, gemini)")
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


class BaseProvider(ABC):
    """Abstract base class for NexaPy AI providers."""

    def __init__(self, name: str):
        self.name = name

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
