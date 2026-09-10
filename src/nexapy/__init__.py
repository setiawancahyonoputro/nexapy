"""NexaPy Framework - Modern Python web & AI framework."""

__version__ = "0.2.0.dev0"

from .app import NexaPy
from .ai.base import AIResponse
from .ai.router import AI, AIRouter
from .automation import workflow

__all__ = ["NexaPy", "AI", "AIRouter", "AIResponse", "workflow", "__version__"]
