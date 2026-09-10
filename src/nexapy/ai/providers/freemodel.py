import os
from typing import Any, Dict, Optional
import httpx
from ..base import BaseProvider, AIResponse


class FreeModelProvider(BaseProvider):
    """FreeModel AI Provider implementation (OpenAI compatible)."""

    def __init__(self, api_key: Optional[str] = None, default_model: str = "auto", base_url: str = "https://api.freemodel.dev/v1"):
        super().__init__(name="freemodel")
        self.api_key = api_key or os.getenv("FREEMODEL_API_KEY")
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
        """Send prompt generation request to FreeModel OpenAI-compatible endpoint."""
        selected_model = model or self.default_model

        if not self.api_key:
            return AIResponse(
                text="",
                provider=self.name,
                model=selected_model,
                success=False,
                error="FREEMODEL_API_KEY is not configured",
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

        if reasoning:
            payload["reasoning_effort"] = reasoning
        if temperature is not None:
            payload["temperature"] = temperature
        if max_tokens is not None:
            payload["max_tokens"] = max_tokens

        for k, v in kwargs.items():
            if k not in payload:
                payload[k] = v

        timeout = httpx.Timeout(connect=5.0, read=30.0, write=10.0, pool=5.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            try:
                res = await client.post(url, headers=headers, json=payload)
                res.raise_for_status()
                data = res.json()

                extracted_text = ""
                try:
                    choices = data.get("choices", [])
                    if choices:
                        message = choices[0].get("message", {})
                        extracted_text = message.get("content", "") or ""
                except Exception:
                    extracted_text = ""

                if not extracted_text.strip():
                    return AIResponse(
                        text="",
                        provider=self.name,
                        model=selected_model,
                        success=False,
                        error="Provider returned empty text response",
                        raw=data,
                    )

                return AIResponse(
                    text=extracted_text,
                    provider=self.name,
                    model=selected_model,
                    success=True,
                    raw=data,
                )
            except Exception as e:
                return AIResponse(
                    text="",
                    provider=self.name,
                    model=selected_model,
                    success=False,
                    error=str(e),
                )
