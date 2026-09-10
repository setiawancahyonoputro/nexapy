"""NexaPy AI Package - Multi-provider AI router and interfaces."""

from .base import AIResponse, BaseProvider
from .providers.freemodel import FreeModelProvider
from .providers.gemini import GeminiProvider
from .router import AI, AIRouter

__all__ = [
    "AI",
    "AIRouter",
    "AIResponse",
    "BaseProvider",
    "FreeModelProvider",
    "GeminiProvider",
]
