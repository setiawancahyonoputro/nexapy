import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from nexapy.ai import (
    AI,
    AIRouter,
    AIResponse,
    BaseProvider,
    OpenAIProvider,
    AnthropicProvider,
    DeepSeekProvider,
)


@pytest.mark.asyncio
async def test_openai_provider_missing_key():
    provider = OpenAIProvider(api_key="")
    res = await provider.generate("Hello")
    assert res.success is False
    assert "OPENAI_API_KEY is not configured" in res.error


@pytest.mark.asyncio
async def test_openai_provider_success():
    provider = OpenAIProvider(api_key="test_key")
    with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "choices": [{"message": {"content": "OpenAI GPT-4o output"}}]
        }
        mock_response.raise_for_status = lambda: None
        mock_post.return_value = mock_response

        res = await provider.generate("Test prompt")
        assert res.success is True
        assert res.text == "OpenAI GPT-4o output"
        assert res.provider == "openai"


@pytest.mark.asyncio
async def test_anthropic_provider_missing_key():
    provider = AnthropicProvider(api_key="")
    res = await provider.generate("Hello")
    assert res.success is False
    assert "ANTHROPIC_API_KEY is not configured" in res.error


@pytest.mark.asyncio
async def test_anthropic_provider_success():
    provider = AnthropicProvider(api_key="test_anthropic_key")
    with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "content": [{"type": "text", "text": "Claude 3.5 Sonnet output"}]
        }
        mock_response.raise_for_status = lambda: None
        mock_post.return_value = mock_response

        res = await provider.generate("Hello Claude")
        assert res.success is True
        assert res.text == "Claude 3.5 Sonnet output"
        assert res.provider == "anthropic"


@pytest.mark.asyncio
async def test_deepseek_provider_missing_key():
    provider = DeepSeekProvider(api_key="")
    res = await provider.generate("Hello")
    assert res.success is False
    assert "DEEPSEEK_API_KEY is not configured" in res.error


@pytest.mark.asyncio
async def test_deepseek_provider_success():
    provider = DeepSeekProvider(api_key="test_deepseek_key")
    with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "choices": [{"message": {"content": "DeepSeek Chat output"}}]
        }
        mock_response.raise_for_status = lambda: None
        mock_post.return_value = mock_response

        res = await provider.generate("Hello DeepSeek")
        assert res.success is True
        assert res.text == "DeepSeek Chat output"
        assert res.provider == "deepseek"


@pytest.mark.asyncio
async def test_router_dynamic_provider_registration():
    """Verify custom/third-party AI providers can be registered via plugin architecture."""
    router = AIRouter()

    class CustomPluginProvider(BaseProvider):
        def __init__(self):
            super().__init__(name="custom_plugin")

        async def generate(self, prompt, **kwargs):
            return AIResponse(text=f"Custom: {prompt}", provider=self.name, model="custom-v1", success=True)

    plugin = CustomPluginProvider()
    router.register_provider("custom_plugin", plugin)

    assert "custom_plugin" in router.providers
    assert "custom_plugin" in router.priority

    res = await router.route("Hello Plugin", provider="custom_plugin")
    assert res.success is True
    assert res.text == "Custom: Hello Plugin"


@pytest.mark.asyncio
async def test_circuit_breaker_health_tracking():
    """Verify provider health tracking marks failing provider as unhealthy."""
    router = AIRouter()

    class FailingProvider(BaseProvider):
        def __init__(self):
            super().__init__(name="failing_prov", cooldown_seconds=60.0, max_consecutive_failures=2)

        async def generate(self, prompt, **kwargs):
            return AIResponse(text="", provider=self.name, model="m", success=False, error="500 Fatal")

    failing = FailingProvider()
    assert failing.is_healthy() is True

    failing.record_failure("error 1")
    assert failing.is_healthy() is True

    failing.record_failure("error 2")
    # Now reached max_consecutive_failures (2) within cooldown -> unhealthy
    assert failing.is_healthy() is False

    # After success, resets to healthy
    failing.record_success()
    assert failing.is_healthy() is True


@pytest.mark.asyncio
async def test_ai_router_streaming():
    """Verify router streaming yields chunks."""
    router = AIRouter()

    class StreamingMockProvider(BaseProvider):
        def __init__(self):
            super().__init__(name="mock_stream")

        async def generate(self, prompt, **kwargs):
            return AIResponse(text="full text", provider=self.name, model="m", success=True)

        async def stream(self, prompt, **kwargs):
            for token in ["Hello", " ", "world", "!"]:
                yield token

    router.register_provider("mock_stream", StreamingMockProvider())

    tokens = []
    async for chunk in router.stream("Test", provider="mock_stream"):
        tokens.append(chunk)

    assert "".join(tokens) == "Hello world!"
