import asyncio
from typing import Any, AsyncGenerator, Dict, List, Optional
from .base import AIResponse, BaseProvider, CircuitBreakerState
from .providers.freemodel import FreeModelProvider
from .providers.gemini import GeminiProvider
from .providers.openai import OpenAIProvider
from .providers.anthropic import AnthropicProvider
from .providers.deepseek import DeepSeekProvider
from ..config import config as global_config

TRANSIENT_ERRORS = ("429", "500", "502", "503", "504", "timeout", "connect", "rate limit")


def is_transient_error(error_msg: Optional[str]) -> bool:
    if not error_msg:
        return False
    msg = error_msg.lower()
    return any(err in msg for err in TRANSIENT_ERRORS)


class AIRouter:
    """
    NexaPy Multi-Provider AI Router.
    Supports FreeModel, Google Gemini, OpenAI (GPT), Anthropic (Claude), and DeepSeek
    with dynamic provider registration, circuit breaker health checks, priority fallback,
    automatic retries, and streaming.
    """

    def __init__(self, config: Optional[Any] = None):
        self.config = config or global_config

        freemodel_key = self.config.get("FREEMODEL_API_KEY")
        freemodel_model = self.config.get("ai.freemodel.model", self.config.get("NEXAPY_FREEMODEL_MODEL", "auto"))

        gemini_key = self.config.get("GEMINI_API_KEY")
        gemini_model = self.config.get(
            "ai.gemini.model",
            self.config.get("ai.model", self.config.get("NEXAPY_GEMINI_MODEL", "gemini-3.8-flash")),
        )

        openai_key = self.config.get("OPENAI_API_KEY")
        openai_model = self.config.get("ai.openai.model", "gpt-4o-mini")

        anthropic_key = self.config.get("ANTHROPIC_API_KEY")
        anthropic_model = self.config.get("ai.anthropic.model", "claude-sonnet-4-6")

        deepseek_key = self.config.get("DEEPSEEK_API_KEY")
        deepseek_model = self.config.get("ai.deepseek.model", "deepseek-chat")

        self.freemodel_provider = FreeModelProvider(api_key=freemodel_key, default_model=str(freemodel_model))
        self.gemini_provider = GeminiProvider(api_key=gemini_key, default_model=str(gemini_model))
        self.openai_provider = OpenAIProvider(api_key=openai_key, default_model=str(openai_model))
        self.anthropic_provider = AnthropicProvider(api_key=anthropic_key, default_model=str(anthropic_model))
        self.deepseek_provider = DeepSeekProvider(api_key=deepseek_key, default_model=str(deepseek_model))

        self.providers: Dict[str, BaseProvider] = {
            "freemodel": self.freemodel_provider,
            "gemini": self.gemini_provider,
            "openai": self.openai_provider,
            "anthropic": self.anthropic_provider,
            "deepseek": self.deepseek_provider,
        }

        self.default_provider = str(self.config.get("ai.provider", self.config.get("NEXAPY_AI_PROVIDER", "auto")))

        raw_priority = self.config.get("ai.priority", ["freemodel", "gemini"])
        if isinstance(raw_priority, list):
            self.priority: List[str] = [str(p).lower() for p in raw_priority if str(p).lower() in self.providers]
        else:
            self.priority = ["freemodel", "gemini"]

        if not self.priority:
            self.priority = ["freemodel", "gemini"]

    def register_provider(self, name: str, provider: BaseProvider) -> None:
        """Register a custom or third-party AI provider plugin into the router."""
        clean_name = name.strip().lower()
        self.providers[clean_name] = provider
        if clean_name not in self.priority:
            self.priority.append(clean_name)

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
        """Route prompt through priority fallback chain with circuit breaker health tracking and retries."""
        target = (provider or self.default_provider).lower()
        max_attempts = max(0, max_retries) + 1

        if target == "auto":
            errors: List[str] = []
            # P1 Finding 11: Circuit breaker must NEVER call providers in OPEN state
            candidate_providers = [
                p for p in self.priority
                if p in self.providers and self.providers[p].circuit_state != CircuitBreakerState.OPEN
            ]

            if not candidate_providers:
                return AIResponse(
                    text="",
                    provider="auto",
                    model="circuit_breaker",
                    success=False,
                    error="Auto routing failed: all available AI providers currently have OPEN circuit breakers in cooldown.",
                )

            # Prioritize CLOSED over HALF_OPEN trial probing
            healthy_closed = [p for p in candidate_providers if self.providers[p].circuit_state == CircuitBreakerState.CLOSED]
            half_open = [p for p in candidate_providers if self.providers[p].circuit_state == CircuitBreakerState.HALF_OPEN]
            ordered_candidates = healthy_closed + half_open

            for p_name in ordered_candidates:
                p_instance = self.providers.get(p_name)
                if not p_instance:
                    continue

                # HALF_OPEN permits exactly 1 single controlled trial attempt
                eff_attempts = 1 if p_instance.circuit_state == CircuitBreakerState.HALF_OPEN else max_attempts

                for attempt in range(eff_attempts):
                    res = await p_instance.generate(
                        prompt,
                        reasoning=reasoning,
                        temperature=temperature,
                        max_tokens=max_tokens,
                        **kwargs,
                    )
                    if res.success:
                        p_instance.record_success()
                        return res

                    p_instance.record_failure(res.error)

                    if not is_transient_error(res.error):
                        errors.append(f"{p_name}: {res.error}")
                        break

                    if attempt < eff_attempts - 1:
                        await asyncio.sleep(0.5)
                    else:
                        errors.append(f"{p_name} (after {eff_attempts} attempts): {res.error}")

            return AIResponse(
                text="",
                provider="auto",
                model="fallback",
                success=False,
                error="Auto routing failed across priority chain. " + " | ".join(errors),
            )

        if target in self.providers:
            p_instance = self.providers[target]
            # P1 Finding 11: Reject call immediately if circuit breaker is OPEN
            if p_instance.circuit_state == CircuitBreakerState.OPEN:
                return AIResponse(
                    text="",
                    provider=target,
                    model="circuit_breaker",
                    success=False,
                    error=f"Circuit breaker is OPEN for provider '{target}'. Call rejected during cooldown.",
                )

            eff_attempts = 1 if p_instance.circuit_state == CircuitBreakerState.HALF_OPEN else max_attempts
            res: Optional[AIResponse] = None
            for attempt in range(eff_attempts):
                res = await p_instance.generate(
                    prompt, reasoning=reasoning, temperature=temperature, max_tokens=max_tokens, **kwargs
                )
                if res.success:
                    p_instance.record_success()
                    return res
                p_instance.record_failure(res.error)
                if not is_transient_error(res.error):
                    return res
                if attempt < eff_attempts - 1:
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

    async def stream(
        self,
        prompt: str,
        provider: Optional[str] = None,
        reasoning: Optional[str] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        **kwargs: Any,
    ) -> AsyncGenerator[str, None]:
        """
        Stream completion tokens via priority fallback or target provider.
        CRITICAL: Never fall back to another provider once tokens have begun streaming to the client.
        Never invokes providers in OPEN circuit breaker state.
        """
        target = (provider or self.default_provider).lower()

        if target == "auto":
            candidate_providers = [
                p for p in self.priority
                if p in self.providers and self.providers[p].circuit_state != CircuitBreakerState.OPEN
            ]
            if not candidate_providers:
                raise RuntimeError("Streaming failed: all providers have OPEN circuit breakers in cooldown.")

            healthy_closed = [p for p in candidate_providers if self.providers[p].circuit_state == CircuitBreakerState.CLOSED]
            half_open = [p for p in candidate_providers if self.providers[p].circuit_state == CircuitBreakerState.HALF_OPEN]
            ordered_candidates = healthy_closed + half_open

            errors = []
            for p_name in ordered_candidates:
                p_instance = self.providers[p_name]
                yielded_any = False
                try:
                    async for chunk in p_instance.stream(
                        prompt,
                        reasoning=reasoning,
                        temperature=temperature,
                        max_tokens=max_tokens,
                        **kwargs,
                    ):
                        yielded_any = True
                        yield chunk
                    p_instance.record_success()
                    return
                except Exception as err:
                    p_instance.record_failure(str(err))
                    if yielded_any:
                        # Prevent partial response mixing across providers
                        raise RuntimeError(f"Stream interrupted during generation by {p_name}: {err}")
                    errors.append(f"{p_name}: {err}")
                    continue
            raise RuntimeError("Streaming failed across all fallback providers: " + " | ".join(errors))

        if target in self.providers:
            p_instance = self.providers[target]
            if p_instance.circuit_state == CircuitBreakerState.OPEN:
                raise RuntimeError(f"Circuit breaker is OPEN for provider '{target}'. Streaming rejected during cooldown.")

            async for chunk in p_instance.stream(
                prompt,
                reasoning=reasoning,
                temperature=temperature,
                max_tokens=max_tokens,
                **kwargs,
            ):
                yield chunk
            p_instance.record_success()
            return

        raise ValueError(f"Unsupported provider for streaming: '{target}'")


class AI:
    """Simple high-level AI interface for NexaPy."""

    def __init__(self, provider: Optional[str] = None, config: Optional[Any] = None):
        self.router = AIRouter(config=config)
        self.provider = provider

    def register_provider(self, name: str, provider: BaseProvider) -> None:
        """Register a custom provider plugin into this AI instance."""
        self.router.register_provider(name, provider)

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
            prompt,
            provider=target_provider,
            reasoning=reasoning,
            temperature=temperature,
            max_tokens=max_tokens,
            **kwargs,
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
            prompt,
            provider=provider,
            reasoning=reasoning,
            temperature=temperature,
            max_tokens=max_tokens,
            **kwargs,
        )

    async def stream(
        self,
        prompt: str,
        provider: Optional[str] = None,
        reasoning: Optional[str] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        **kwargs: Any,
    ) -> AsyncGenerator[str, None]:
        """Stream completion tokens asynchronously."""
        target_provider = provider or self.provider
        async for chunk in self.router.stream(
            prompt,
            provider=target_provider,
            reasoning=reasoning,
            temperature=temperature,
            max_tokens=max_tokens,
            **kwargs,
        ):
            yield chunk
