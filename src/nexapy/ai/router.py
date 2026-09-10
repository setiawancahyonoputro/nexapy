import asyncio
from typing import Any, Dict, List, Optional
from .base import AIResponse, BaseProvider
from .providers.freemodel import FreeModelProvider
from .providers.gemini import GeminiProvider
from ..config import config as global_config

TRANSIENT_ERRORS = ("429", "500", "502", "503", "504", "timeout", "connect", "rate limit")


def is_transient_error(error_msg: Optional[str]) -> bool:
    if not error_msg:
        return False
    msg = error_msg.lower()
    return any(err in msg for err in TRANSIENT_ERRORS)


class AIRouter:
    """NexaPy Multi-Provider AI Router supporting FreeModel and Gemini with priority fallback and automatic retries."""

    def __init__(self, config: Optional[Any] = None):
        self.config = config or global_config

        freemodel_key = self.config.get("FREEMODEL_API_KEY")
        freemodel_model = self.config.get("ai.freemodel.model", self.config.get("NEXAPY_FREEMODEL_MODEL", "auto"))

        gemini_key = self.config.get("GEMINI_API_KEY")
        gemini_model = self.config.get("ai.gemini.model", self.config.get("ai.model", self.config.get("NEXAPY_GEMINI_MODEL", "gemini-3.8-flash")))

        self.freemodel_provider = FreeModelProvider(api_key=freemodel_key, default_model=str(freemodel_model))
        self.gemini_provider = GeminiProvider(api_key=gemini_key, default_model=str(gemini_model))

        self.providers: Dict[str, BaseProvider] = {
            "freemodel": self.freemodel_provider,
            "gemini": self.gemini_provider,
        }
        self.default_provider = str(self.config.get("ai.provider", self.config.get("NEXAPY_AI_PROVIDER", "auto")))

        raw_priority = self.config.get("ai.priority", ["freemodel", "gemini"])
        if isinstance(raw_priority, list):
            self.priority: List[str] = [str(p).lower() for p in raw_priority if str(p).lower() in self.providers]
        else:
            self.priority = ["freemodel", "gemini"]

        if not self.priority:
            self.priority = ["freemodel", "gemini"]

    async def route(
        self,
        prompt: str,
        provider: Optional[str] = None,
        reasoning: Optional[str] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        max_retries: int = 1,
        **kwargs: Any,
    ) -> AIResponse:
        """Route prompt through priority fallback chain with limited retries and backoff for transient errors."""
        target = (provider or self.default_provider).lower()
        max_attempts = max(0, max_retries) + 1

        if target == "auto":
            errors: List[str] = []
            for p_name in self.priority:
                p_instance = self.providers.get(p_name)
                if not p_instance:
                    continue

                for attempt in range(max_attempts):
                    res = await p_instance.generate(
                        prompt, reasoning=reasoning, temperature=temperature, max_tokens=max_tokens, **kwargs
                    )
                    if res.success:
                        return res

                    if not is_transient_error(res.error):
                        errors.append(f"{p_name}: {res.error}")
                        break

                    if attempt < max_attempts - 1:
                        await asyncio.sleep(0.5)
                    else:
                        errors.append(f"{p_name} (after {max_attempts} attempts): {res.error}")

            return AIResponse(
                text="",
                provider="auto",
                model="fallback",
                success=False,
                error="Auto routing failed across priority chain. " + " | ".join(errors),
            )

        if target in self.providers:
            p_instance = self.providers[target]
            res: Optional[AIResponse] = None
            for attempt in range(max_attempts):
                res = await p_instance.generate(
                    prompt, reasoning=reasoning, temperature=temperature, max_tokens=max_tokens, **kwargs
                )
                if res.success or not is_transient_error(res.error):
                    return res
                if attempt < max_attempts - 1:
                    await asyncio.sleep(0.5)

            if res is not None:
                return res

        return AIResponse(
            text="",
            provider=target,
            model="unknown",
            success=False,
            error=f"Unsupported provider: '{target}'",
        )


class AI:
    """Simple high-level AI interface for NexaPy."""

    def __init__(self, provider: Optional[str] = None, config: Optional[Any] = None):
        self.router = AIRouter(config=config)
        self.provider = provider

    async def chat(
        self,
        prompt: str,
        provider: Optional[str] = None,
        reasoning: Optional[str] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        **kwargs: Any,
    ) -> AIResponse:
        """Send chat prompt and return AIResponse."""
        target_provider = provider or self.provider
        return await self.router.route(
            prompt, provider=target_provider, reasoning=reasoning, temperature=temperature, max_tokens=max_tokens, **kwargs
        )

    async def generate(
        self,
        prompt: str,
        provider: Optional[str] = None,
        reasoning: Optional[str] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        **kwargs: Any,
    ) -> AIResponse:
        """Alias for chat method."""
        return await self.chat(
            prompt, provider=provider, reasoning=reasoning, temperature=temperature, max_tokens=max_tokens, **kwargs
        )
