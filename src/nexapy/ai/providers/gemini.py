import os
from typing import Any, Dict, Optional
import httpx
from ..base import BaseProvider, AIResponse


class GeminiProvider(BaseProvider):
    """Google Gemini AI Provider implementation using Interactions API."""

    def __init__(self, api_key: Optional[str] = None, default_model: str = "gemini-3.8-flash"):
        super().__init__(name="gemini")
        self.api_key = api_key or os.getenv("GEMINI_API_KEY")
        self.default_model = default_model

    async def generate(
        self,
        prompt: str,
        model: Optional[str] = None,
        reasoning: Optional[str] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        **kwargs: Any,
    ) -> AIResponse:
        """Send prompt generation request to Google Gemini Interactions API using header authentication."""
        selected_model = model or self.default_model

        if not self.api_key:
            return AIResponse(
                text="",
                provider=self.name,
                model=selected_model,
                success=False,
                error="GEMINI_API_KEY is not configured",
            )

        url = "https://generativelanguage.googleapis.com/v1beta/interactions"
        headers = {
            "Content-Type": "application/json",
            "x-goog-api-key": self.api_key,
        }

        payload: Dict[str, Any] = {
            "model": selected_model,
            "input": prompt,
        }

        generation_config: Dict[str, Any] = {}
        if reasoning:
            generation_config["thinking_level"] = reasoning
        elif temperature is not None and "gemini-3.8" not in selected_model.lower():
            # Omit temperature for Gemini 3.8 Flash per Google migration guide
            generation_config["temperature"] = temperature

        if max_tokens is not None:
            generation_config["max_output_tokens"] = max_tokens

        if "generationConfig" in kwargs and isinstance(kwargs["generationConfig"], dict):
            generation_config.update(kwargs["generationConfig"])
        elif "generation_config" in kwargs and isinstance(kwargs["generation_config"], dict):
            generation_config.update(kwargs["generation_config"])

        if generation_config:
            payload["generation_config"] = generation_config

        timeout = httpx.Timeout(connect=5.0, read=30.0, write=10.0, pool=5.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            try:
                res = await client.post(url, headers=headers, json=payload)
                res.raise_for_status()
                data = res.json()

                extracted_text = ""
                try:
                    # 1. Official Google Interactions API schema parsing
                    steps = data.get("steps", [])
                    for step in steps:
                        content_item = step.get("content")
                        if isinstance(content_item, list):
                            for part in content_item:
                                if isinstance(part, dict) and part.get("text"):
                                    extracted_text += str(part.get("text"))
                                elif isinstance(part, str):
                                    extracted_text += part
                        elif isinstance(content_item, dict):
                            parts = content_item.get("parts", [])
                            for part in parts:
                                if isinstance(part, dict) and part.get("text"):
                                    extracted_text += str(part.get("text"))
                                elif isinstance(part, str):
                                    extracted_text += part

                    # 2. Fallback check for candidates structure
                    if not extracted_text:
                        candidates = data.get("candidates", [])
                        if candidates:
                            parts = candidates[0].get("content", {}).get("parts", [])
                            extracted_text = "".join(str(p.get("text", "")) for p in parts if isinstance(p, dict))
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
