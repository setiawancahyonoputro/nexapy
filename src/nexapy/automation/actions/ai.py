"""AI action integration for NexaPy Workflows."""

from typing import Optional
from nexapy.ai.base import AIResponse
from nexapy.ai.router import AI


class AIAction:
    """AI Action wrapper bound to NexaPy AI Router."""

    def __init__(self, ai_engine: Optional[AI] = None):
        self._ai_engine = ai_engine or AI()

    async def __call__(
        self,
        prompt: str,
        provider: Optional[str] = None,
        reasoning: Optional[str] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
    ) -> AIResponse:
        """Execute an AI completion prompt using the NexaPy AI Router."""
        return await self._ai_engine.chat(
            prompt,
            provider=provider,
            reasoning=reasoning,
            temperature=temperature,
            max_tokens=max_tokens,
        )
