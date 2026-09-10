import pytest
from fastapi.testclient import TestClient
from nexapy import NexaPy, __version__
from nexapy.app import AIChatRequest


def test_version_single_source():
    assert __version__ == "0.2.0.dev0"
    app = NexaPy()
    assert app.version == __version__


def test_app_health_check():
    app = NexaPy()
    client = TestClient(app.fastapi)
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "framework": "NexaPy",
        "version": __version__,
    }


def test_cors_middleware():
    app = NexaPy(enable_cors=True, cors_origins=["http://localhost:3000"])
    client = TestClient(app.fastapi)
    response = client.options(
        "/health",
        headers={
            "Origin": "http://localhost:3000",
            "Access-Control-Request-Method": "GET",
        },
    )
    assert response.status_code == 200
    assert response.headers.get("access-control-allow-origin") == "http://localhost:3000"


def test_ai_chat_request_schema():
    req = AIChatRequest(prompt="Halo", reasoning="high")
    assert req.prompt == "Halo"
    assert req.reasoning == "high"


def test_ai_route_decorator_post_only_default():
    app = NexaPy()

    @app.ai("/chat")
    async def chat(prompt: str):
        pass

    client = TestClient(app.fastapi)
    # Default is POST-only, GET should return Method Not Allowed (405)
    res_get = client.get("/chat")
    assert res_get.status_code == 405

    # POST with missing prompt returns 422 Unprocessable Entity due to required Pydantic schema
    res_post_err = client.post("/chat", json={})
    assert res_post_err.status_code == 422


def test_ai_route_strict_airesponse_contract():
    app = NexaPy()

    @app.ai("/custom_response")
    async def custom_handler(prompt: str):
        from nexapy.ai.base import AIResponse
        return AIResponse(text="Custom AIResponse output", provider="custom", model="test-m", success=True)

    client = TestClient(app.fastapi)
    res = client.post("/custom_response", json={"prompt": "Hello"})
    assert res.status_code == 200
    data = res.json()
    assert data["text"] == "Custom AIResponse output"
    assert data["provider"] == "custom"
    assert data["model"] == "test-m"
    assert data["success"] is True
    assert "raw" not in data


def test_ai_route_string_prompt_modification():
    app = NexaPy()

    @app.ai("/mod_chat")
    async def mod_chat(prompt: str):
        return f"Modified: {prompt}"

    client = TestClient(app.fastapi)
    # Handler modifies prompt string, route executes chat engine (which fails cleanly without API keys)
    res = client.post("/mod_chat", json={"prompt": "Input text"})
    assert res.status_code in (200, 503)
    data = res.json()
    assert "text" in data
    assert "provider" in data
    assert "success" in data

