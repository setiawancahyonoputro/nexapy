"""Anthropic Claude AI Provider implementation supporting Messages API and streaming."""

import json
import os
from typing import Any, AsyncGenerator, Dict, Optional
import httpx
from ..base import BaseProvider, AIResponse


ACTIVE_MODELS = {
    "claude-3-5-sonnet-latest",
    "claude-3-7-sonnet-latest",
    "claude-3-5-haiku-latest",
    "claude-3-opus-latest",
}

RETIRED_MODELS_MAP = {
    "claude-3-5-sonnet-20241022": "claude-3-5-sonnet-latest",
}


class AnthropicProvider(BaseProvider):
    """Direct Anthropic API provider for Claude models with active model validation."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        default_model: str = "claude-3-5-sonnet-latest",
        base_url: str = "https://api.anthropic.com/v1",
    ):
        super().__init__(name="anthropic")
        self.api_key = api_key or os.getenv("ANTHROPIC_API_KEY")
        self.default_model = RETIRED_MODELS_MAP.get(default_model, default_model)
        self.base_url = base_url.rstrip("/")

    def resolve_model(self, model: Optional[str]) -> str:
        """Resolve active model name and migrate deprecated aliases."""
        target = model or self.default_model
        return RETIRED_MODELS_MAP.get(target, target)

    async def generate(
        self,
        prompt: str,
        model: Optional[str] = None,
        reasoning: Optional[str] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        **kwargs: Any,
    ) -> AIResponse:
        selected_model = self.resolve_model(model)

        if not self.api_key:
            return AIResponse(
                text="",
                provider=self.name,
                model=selected_model,
                success=False,
                error="ANTHROPIC_API_KEY is not configured",
            )

        url = f"{self.base_url}/messages"
        headers = {
            "Content-Type": "application/json",
            "x-api-key": self.api_key,
            "anthropic-version": "2023-06-01",
        }

        payload: Dict[str, Any] = {
            "model": selected_model,
            "max_tokens": max_tokens or 1024,
            "messages": [{"role": "user", "content": prompt}],
        }
        if temperature is not None:
            payload["temperature"] = temperature

        for k, v in kwargs.items():
            if k not in payload:
                payload[k] = v

        timeout = httpx.Timeout(connect=5.0, read=60.0, write=10.0, pool=5.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            try:
                res = await client.post(url, headers=headers, json=payload)
                res.raise_for_status()
                data = res.json()

                extracted_text = ""
                content_blocks = data.get("content", [])
                for block in content_blocks:
                    if isinstance(block, dict) and block.get("type") == "text":
                        extracted_text += block.get("text", "")

                if not extracted_text.strip():
                    self.record_failure("Provider returned empty text response")
                    return AIResponse(
                        text="",
                        provider=self.name,
                        model=selected_model,
                        success=False,
                        error="Provider returned empty text response",
                        raw=data,
                    )

                self.record_success()
                return AIResponse(
                    text=extracted_text,
                    provider=self.name,
                    model=selected_model,
                    success=True,
                    raw=data,
                )
            except Exception as e:
                self.record_failure(str(e))
                return AIResponse(
                    text="",
                    provider=self.name,
                    model=selected_model,
                    success=False,
                    error=str(e),
                )

    async def stream(
        self,
        prompt: str,
        model: Optional[str] = None,
        reasoning: Optional[str] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        **kwargs: Any,
    ) -> AsyncGenerator[str, None]:
        selected_model = self.resolve_model(model)
        if not self.api_key:
            raise ValueError("ANTHROPIC_API_KEY is not configured")

        url = f"{self.base_url}/messages"
        headers = {
            "Content-Type": "application/json",
            "x-api-key": self.api_key,
            "anthropic-version": "2023-06-01",
        }
        payload: Dict[str, Any] = {
            "model": selected_model,
            "max_tokens": max_tokens or 1024,
            "messages": [{"role": "user", "content": prompt}],
            "stream": True,
        }
        if temperature is not None:
            payload["temperature"] = temperature

        timeout = httpx.Timeout(connect=5.0, read=60.0, write=10.0, pool=5.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            async with client.stream("POST", url, headers=headers, json=payload) as response:
                response.raise_for_status()
                async for line in response.aiter_lines():
                    if not line or not line.startswith("data:"):
                        continue
                    data_str = line[5:].strip()
                    try:
                        event = json.loads(data_str)
                        if event.get("type") == "content_block_delta":
                            delta = event.get("delta", {})
                            if delta.get("type") == "text_delta":
                                yield delta.get("text", "")
                    except Exception:
                        continue
