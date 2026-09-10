import pytest
from unittest.mock import AsyncMock, patch
from fastapi.testclient import TestClient
from nexapy import NexaPy, AI, AIResponse


def test_full_app_integration():
    app = NexaPy(title="Integration Test App")
    client = TestClient(app.fastapi)

    # 1. Health check returns 200 OK
    res_health = client.get("/health")
    assert res_health.status_code == 200
    assert res_health.json()["status"] == "ok"


def test_ai_decorator_flow_and_status_codes():
    app = NexaPy()

    @app.ai("/chat")
    async def chat(prompt: str):
        if prompt == "return_custom_response":
            return AIResponse(text="Custom output", provider="custom", model="test-m", success=True)
        if prompt == "modify_prompt":
            return "Enhanced prompt string"
        return None

    client = TestClient(app.fastapi)

    # 1. Missing prompt returns 422 Unprocessable Entity (standard FastAPI Pydantic schema validation)
    res_422 = client.post("/chat", json={})
    assert res_422.status_code == 422

    # 2. Custom AIResponse return returns 200 OK with AIResponse schema
    res_custom = client.post("/chat", json={"prompt": "return_custom_response"})
    assert res_custom.status_code == 200
    assert res_custom.json() == {
        "text": "Custom output",
        "provider": "custom",
        "model": "test-m",
        "success": True,
        "error": None,
    }

    # 3. AI provider failure returns 503 Service Unavailable
    with patch.object(app._ai_engine.router, "route", new_callable=AsyncMock) as mock_route:
        mock_route.return_value = AIResponse(
            text="", provider="freemodel", model="auto", success=False, error="API Key Invalid"
        )
        res_503 = client.post("/chat", json={"prompt": "test"})
        assert res_503.status_code == 503
        assert res_503.json()["success"] is False
        assert res_503.json()["error"] == "API Key Invalid"


@pytest.mark.asyncio
async def test_ai_parameters_forwarding():
    ai = AI()
    with patch.object(ai.router.providers["freemodel"], "generate", new_callable=AsyncMock) as mock_fm:
        mock_fm.return_value = AIResponse(
            text="Mocked output", provider="freemodel", model="auto", success=True
        )

        res = await ai.chat("Hello", provider="freemodel", reasoning="low", temperature=0.8, max_tokens=500)
        assert res.success is True
        mock_fm.assert_called_once_with("Hello", reasoning="low", temperature=0.8, max_tokens=500)
