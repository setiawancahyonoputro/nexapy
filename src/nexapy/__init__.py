"""NexaPy Framework - Modern Python web & AI framework."""

__version__ = "0.2.0.dev0"

from .app import NexaPy
from .ai.base import AIResponse
from .ai.router import AI, AIRouter

__all__ = ["NexaPy", "AI", "AIRouter", "AIResponse", "__version__"]
