import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from nexapy.ai import AI, AIRouter, AIResponse, FreeModelProvider, GeminiProvider


@pytest.mark.asyncio
async def test_ai_response_model():
    res = AIResponse(text="Hello", provider="freemodel", model="auto", success=True)
    d = res.dict_response()
    assert d["text"] == "Hello"
    assert d["provider"] == "freemodel"
    assert d["success"] is True


@pytest.mark.asyncio
async def test_freemodel_missing_key():
    provider = FreeModelProvider(api_key="")
    res = await provider.generate("Hello")
    assert res.success is False
    assert "FREEMODEL_API_KEY is not configured" in res.error


@pytest.mark.asyncio
async def test_freemodel_success():
    provider = FreeModelProvider(api_key="test_fm_key")
    with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "choices": [{"message": {"content": "FreeModel response text"}}]
        }
        mock_response.raise_for_status = lambda: None
        mock_post.return_value = mock_response

        res = await provider.generate("Test prompt", temperature=0.7)
        assert res.success is True
        assert res.text == "FreeModel response text"
        assert res.provider == "freemodel"


@pytest.mark.asyncio
async def test_freemodel_empty_text_error():
    provider = FreeModelProvider(api_key="test_fm_key")
    with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"choices": []}
        mock_response.raise_for_status = lambda: None
        mock_post.return_value = mock_response

        res = await provider.generate("Test prompt")
        assert res.success is False
        assert "empty text response" in res.error


@pytest.mark.asyncio
async def test_freemodel_reasoning_param():
    provider = FreeModelProvider(api_key="test_fm_key")
    with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "choices": [{"message": {"content": "Reasoned response"}}]
        }
        mock_response.raise_for_status = lambda: None
        mock_post.return_value = mock_response

        res = await provider.generate("Test prompt", reasoning="high")
        assert res.success is True
        assert res.text == "Reasoned response"


@pytest.mark.asyncio
async def test_freemodel_custom_model_and_url():
    provider = FreeModelProvider(api_key="test_key", default_model="custom-m", base_url="https://custom.api/v1")
    assert provider.default_model == "custom-m"
    assert provider.base_url == "https://custom.api/v1"


@pytest.mark.asyncio
async def test_freemodel_error():
    provider = FreeModelProvider(api_key="test_fm_key")
    with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post:
        mock_post.side_effect = Exception("API Server Error")

        res = await provider.generate("Test prompt")
        assert res.success is False
        assert "API Server Error" in res.error


@pytest.mark.asyncio
async def test_gemini_header_auth():
    provider = GeminiProvider(api_key="test_gemini_key")
    assert provider.api_key == "test_gemini_key"
    assert provider.default_model == "gemini-3.8-flash"


@pytest.mark.asyncio
async def test_gemini_interactions_official_schema():
    """Verify parsing official Google Interactions API schema: steps -> content -> [{type: text, text: ...}]."""
    provider = GeminiProvider(api_key="test_gemini_key")
    with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "steps": [
                {
                    "type": "model_output",
                    "content": [
                        {
                            "type": "text",
                            "text": "Official Gemini Interactions response text"
                        }
                    ]
                }
            ]
        }
        mock_response.raise_for_status = lambda: None
        mock_post.return_value = mock_response

        res = await provider.generate("Test prompt", reasoning="medium")
        assert res.success is True
        assert res.text == "Official Gemini Interactions response text"
        assert res.provider == "gemini"


@pytest.mark.asyncio
async def test_gemini_max_output_tokens_field_name():
    """Verify max_output_tokens key is used in generation_config."""
    provider = GeminiProvider(api_key="test_gemini_key")
    with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "steps": [{"type": "model_output", "content": [{"type": "text", "text": "Ok"}]}]
        }
        mock_response.raise_for_status = lambda: None
        mock_post.return_value = mock_response

        await provider.generate("Test prompt", max_tokens=100)
        call_kwargs = mock_post.call_args[1]
        json_payload = call_kwargs["json"]
        assert "generation_config" in json_payload
        assert json_payload["generation_config"]["max_output_tokens"] == 100
        assert "maxOutputTokens" not in json_payload["generation_config"]


@pytest.mark.asyncio
async def test_gemini_temperature_omitted_for_38_flash():
    """Verify temperature is omitted for Gemini 3.8 Flash per Google guidance."""
    provider = GeminiProvider(api_key="test_gemini_key", default_model="gemini-3.8-flash")
    with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "steps": [{"type": "model_output", "content": [{"type": "text", "text": "Ok"}]}]
        }
        mock_response.raise_for_status = lambda: None
        mock_post.return_value = mock_response

        await provider.generate("Test prompt", temperature=0.9, reasoning="high")
        call_kwargs = mock_post.call_args[1]
        json_payload = call_kwargs["json"]
        gen_cfg = json_payload.get("generation_config", {})
        assert "temperature" not in gen_cfg
        assert gen_cfg.get("thinking_level") == "high"


@pytest.mark.asyncio
async def test_gemini_missing_key():
    provider = GeminiProvider(api_key="")
    res = await provider.generate("Test")
    assert res.success is False
    assert "GEMINI_API_KEY is not configured" in res.error


@pytest.mark.asyncio
async def test_gemini_empty_text_error():
    provider = GeminiProvider(api_key="test_gemini_key")
    with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"steps": []}
        mock_response.raise_for_status = lambda: None
        mock_post.return_value = mock_response

        res = await provider.generate("Test")
        assert res.success is False
        assert "empty text response" in res.error


@pytest.mark.asyncio
async def test_ai_router_unsupported_provider():
    router = AIRouter()
    res = await router.route("Hello", provider="unknown")
    assert res.success is False
    assert "Unsupported provider" in res.error


@pytest.mark.asyncio
async def test_auto_freemodel_to_gemini():
    router = AIRouter()
    router.priority = ["freemodel", "gemini"]

    with patch.object(router.providers["freemodel"], "generate", new_callable=AsyncMock) as mock_fm, \
         patch.object(router.providers["gemini"], "generate", new_callable=AsyncMock) as mock_gemini:

        # FreeModel fails
        mock_fm.return_value = AIResponse(
            text="", provider="freemodel", model="auto", success=False, error="Quota Exceeded"
        )
        # Gemini succeeds
        mock_gemini.return_value = AIResponse(
            text="Gemini fallback response", provider="gemini", model="gemini-3.8-flash", success=True
        )

        res = await router.route("Test prompt", provider="auto")
        assert res.success is True
        assert res.text == "Gemini fallback response"
        assert res.provider == "gemini"


@pytest.mark.asyncio
async def test_auto_gemini_to_freemodel():
    router = AIRouter()
    router.priority = ["gemini", "freemodel"]

    with patch.object(router.providers["gemini"], "generate", new_callable=AsyncMock) as mock_gemini, \
         patch.object(router.providers["freemodel"], "generate", new_callable=AsyncMock) as mock_fm:

        # Gemini fails
        mock_gemini.return_value = AIResponse(
            text="", provider="gemini", model="gemini-3.8-flash", success=False, error="Auth Error"
        )
        # FreeModel succeeds
        mock_fm.return_value = AIResponse(
            text="FreeModel fallback response", provider="freemodel", model="auto", success=True
        )

        res = await router.route("Test prompt", provider="auto")
        assert res.success is True
        assert res.text == "FreeModel fallback response"
        assert res.provider == "freemodel"


@pytest.mark.asyncio
async def test_provider_priority_custom_config():
    class DummyConfig:
        def get(self, key, default=None):
            if key == "ai.priority":
                return ["gemini", "freemodel"]
            return default

    router = AIRouter(config=DummyConfig())
    assert router.priority == ["gemini", "freemodel"]


@pytest.mark.asyncio
async def test_transient_error_retry_logic():
    router = AIRouter()
    with patch.object(router.providers["freemodel"], "generate", new_callable=AsyncMock) as mock_fm:
        # First call fails with transient 503 error, second call succeeds
        mock_fm.side_effect = [
            AIResponse(text="", provider="freemodel", model="auto", success=False, error="503 Service Unavailable"),
            AIResponse(text="Recovered text", provider="freemodel", model="auto", success=True),
        ]

        res = await router.route("Test prompt", provider="freemodel", max_retries=2)
        assert res.success is True
        assert res.text == "Recovered text"
        assert mock_fm.call_count == 2


@pytest.mark.asyncio
async def test_router_zero_retries_safety():
    """Verify max_retries=0 executes safely without UnboundLocalError."""
    router = AIRouter()
    with patch.object(router.providers["freemodel"], "generate", new_callable=AsyncMock) as mock_fm:
        mock_fm.return_value = AIResponse(
            text="", provider="freemodel", model="auto", success=False, error="500 Internal Error"
        )
        res = await router.route("Test prompt", provider="freemodel", max_retries=0)
        assert res.success is False
        assert mock_fm.call_count == 1


@pytest.mark.asyncio
async def test_ai_response_raw_exclusion():
    """Verify raw field is present on AIResponse instance but excluded from JSON dict by default."""
    raw_payload = {"choices": [{"message": {"content": "Hi"}}], "usage": {"total_tokens": 10}}
    res = AIResponse(text="Hi", provider="freemodel", model="auto", success=True, raw=raw_payload)

    assert res.raw == raw_payload

    # Default dict_response excludes raw
    d_default = res.dict_response()
    assert "raw" not in d_default
    assert d_default["text"] == "Hi"

    # dict_response(include_raw=True) includes raw
    d_raw = res.dict_response(include_raw=True)
    assert "raw" in d_raw
    assert d_raw["raw"] == raw_payload


@pytest.mark.asyncio
async def test_router_default_max_retries_one():
    """Verify max_retries=1 (default) performs 1 initial attempt + 1 retry = 2 total attempts."""
    router = AIRouter()
    with patch.object(router.providers["freemodel"], "generate", new_callable=AsyncMock) as mock_fm:
        mock_fm.return_value = AIResponse(
            text="", provider="freemodel", model="auto", success=False, error="503 Service Unavailable"
        )
        res = await router.route("Test prompt", provider="freemodel", max_retries=1)
        assert res.success is False
        assert mock_fm.call_count == 2

