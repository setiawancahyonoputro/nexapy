"""OpenAI AI Provider implementation supporting GPT-4o, GPT-4, and streaming."""

import json
import os
from typing import Any, AsyncGenerator, Dict, Optional
import httpx
from ..base import BaseProvider, AIResponse


class OpenAIProvider(BaseProvider):
    """Direct OpenAI API provider with full chat and streaming support."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        default_model: str = "gpt-4o-mini",
        base_url: str = "https://api.openai.com/v1",
    ):
        super().__init__(name="openai")
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        self.default_model = default_model
        self.base_url = base_url.rstrip("/")

    async def generate(
        self,
        prompt: str,
        model: Optional[str] = None,
        reasoning: Optional[str] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        **kwargs: Any,
    ) -> AIResponse:
        selected_model = model or self.default_model

        if not self.api_key:
            return AIResponse(
                text="",
                provider=self.name,
                model=selected_model,
                success=False,
                error="OPENAI_API_KEY is not configured",
            )

        url = f"{self.base_url}/chat/completions"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}",
        }

        payload: Dict[str, Any] = {
            "model": selected_model,
            "messages": [{"role": "user", "content": prompt}],
        }
        if temperature is not None:
            payload["temperature"] = temperature
        if max_tokens is not None:
            payload["max_tokens"] = max_tokens
        if reasoning:
            payload["reasoning_effort"] = reasoning

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
                choices = data.get("choices", [])
                if choices:
                    extracted_text = choices[0].get("message", {}).get("content", "") or ""

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
        selected_model = model or self.default_model
        if not self.api_key:
            raise ValueError("OPENAI_API_KEY is not configured")

        url = f"{self.base_url}/chat/completions"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}",
        }
        payload: Dict[str, Any] = {
            "model": selected_model,
            "messages": [{"role": "user", "content": prompt}],
            "stream": True,
        }
        if temperature is not None:
            payload["temperature"] = temperature
        if max_tokens is not None:
            payload["max_tokens"] = max_tokens
        if reasoning:
            payload["reasoning_effort"] = reasoning

        timeout = httpx.Timeout(connect=5.0, read=60.0, write=10.0, pool=5.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            async with client.stream("POST", url, headers=headers, json=payload) as response:
                response.raise_for_status()
                async for line in response.aiter_lines():
                    if not line or not line.startswith("data:"):
                        continue
                    data_str = line[5:].strip()
                    if data_str == "[DONE]":
                        break
                    try:
                        chunk_obj = json.loads(data_str)
                        choices = chunk_obj.get("choices", [])
                        if choices:
                            delta = choices[0].get("delta", {})
                            content = delta.get("content")
                            if content:
                                yield content
                    except Exception:
                        continue
