"""AI action integration for NexaPy Workflows with structured schema parsing."""

import json
import re
from typing import Any, Dict, Optional, Type, TypeVar, Union
from pydantic import BaseModel
from nexapy.ai.base import AIResponse
from nexapy.ai.router import AI

T = TypeVar("T", bound=BaseModel)


class AIAction:
    """AI Action wrapper bound to NexaPy AI Router with structured output support."""

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

    async def chat_structured(
        self,
        prompt: str,
        schema: Type[T],
        provider: Optional[str] = None,
        max_retries: int = 2,
    ) -> T:
        """
        Execute AI completion prompt and parse response into a validated Pydantic model schema.
        Performs self-correction retry if output fails schema validation.
        """
        schema_json = json.dumps(schema.model_json_schema(), indent=2)
        system_prompt = (
            f"{prompt}\n\n"
            f"IMPORTANT: Respond ONLY with valid JSON matching this JSON schema:\n"
            f"```json\n{schema_json}\n```\n"
            f"Do not include any intro, explanation, or markdown backticks outside of valid JSON."
        )

        current_prompt = system_prompt
        for attempt in range(max_retries + 1):
            res = await self._ai_engine.chat(current_prompt, provider=provider)
            if not res.success or not res.text:
                if attempt == max_retries:
                    raise ValueError(f"AI response failed or returned empty text: {res.error}")
                continue

            text = res.text.strip()
            # Clean markdown code blocks if present
            match = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", text)
            if match:
                text = match.group(1).strip()

            try:
                data = json.loads(text)
                return schema.model_validate(data)
            except Exception as parse_err:
                if attempt == max_retries:
                    raise ValueError(
                        f"Failed to parse structured AI output into {schema.__name__}: {parse_err}\nRaw text: {res.text}"
                    )
                current_prompt = (
                    f"{system_prompt}\n\n"
                    f"Your previous response failed validation with error: {parse_err}. "
                    f"Please fix and provide exact JSON adhering to the schema."
                )
