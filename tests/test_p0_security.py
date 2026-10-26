import json
import pytest
from unittest.mock import AsyncMock, patch
from fastapi.testclient import TestClient
from nexapy import NexaPy, workflow
from nexapy.automation import mask_sensitive_data, tracker, workflow_registry
from nexapy.automation.actions.notification import NotificationAction
from nexapy.security import HMACVerifier


def test_hmac_webhook_signature_exact_bytes():
    """Verify HMAC signature is computed on the exact byte sequence sent in HTTP request."""
    secret = "my_webhook_secret_key"
    payload = {"order_id": 999, "status": "shipped", "customer": {"name": "Alice"}}

    action = NotificationAction()

    # Capture what was actually sent
    captured_content = None
    captured_headers = None

    async def fake_post(url, content=None, headers=None, **kwargs):
        nonlocal captured_content, captured_headers
        captured_content = content
        captured_headers = headers
        mock_res = AsyncMock()
        mock_res.is_success = True
        return mock_res

    with patch("httpx.AsyncClient.post", side_effect=fake_post):
        success = pytest.importorskip("asyncio").run(
            action.send_webhook("http://example.com/webhook", payload=payload, secret=secret)
        )
        assert success is True
        assert captured_content is not None
        assert captured_headers is not None
        assert "X-NexaPy-Signature" in captured_headers

        sig_header = captured_headers["X-NexaPy-Signature"]
        # Exact signature verification on captured bytes
        is_valid = HMACVerifier.verify_signature(secret, captured_content, sig_header)
        assert is_valid is True, "HMAC signature MUST match exact bytes sent in HTTP request"


def test_sensitive_data_masking():
    """Verify mask_sensitive_data redacts passwords, tokens, API keys, and authorization headers."""
    raw_data = {
        "username": "admin",
        "password": "supersecretpassword123",
        "api_key": "sk-1234567890abcdef",
        "nested": {
            "auth_token": "bearer_jwt_token_here",
            "normal_value": 42,
            "private_key": "-----BEGIN PRIVATE KEY-----",
        },
        "items": [
            {"name": "order1", "credit_card": "4111222233334444"},
            {"name": "order2", "public_info": "ok"},
        ],
    }

    sanitized = mask_sensitive_data(raw_data)

    assert sanitized["username"] == "admin"
    assert sanitized["password"] == "********"
    assert sanitized["api_key"] == "********"
    assert sanitized["nested"]["auth_token"] == "********"
    assert sanitized["nested"]["normal_value"] == 42
    assert sanitized["nested"]["private_key"] == "********"
    assert sanitized["items"][0]["credit_card"] == "********"
    assert sanitized["items"][1]["public_info"] == "ok"


def test_workflow_telemetry_endpoint_requires_auth():
    """Verify /workflows/executions/{id} requires authentication when API key is set."""
    workflow_registry.clear()

    @workflow("guarded_wf")
    def guarded_wf(ctx):
        return {"data": 123}

    app = NexaPy()
    app.mount_workflows(prefix="/workflows", api_key="secret-api-key")
    client = TestClient(app.fastapi)

    # 1. Start execution to get an execution ID
    exec_log = tracker.start_execution("guarded_wf", {"input": "test"})
    exec_id = exec_log.execution_id

    # 2. Access execution telemetry without key -> 401 Unauthorized
    res_unauth = client.get(f"/workflows/executions/{exec_id}")
    assert res_unauth.status_code == 401

    # 3. Access execution telemetry with valid key -> 200 OK
    res_auth = client.get(
        f"/workflows/executions/{exec_id}",
        headers={"X-NexaPy-Key": "secret-api-key"},
    )
    assert res_auth.status_code == 200
    assert res_auth.json()["execution"]["execution_id"] == exec_id

    workflow_registry.clear()


def test_payload_size_limit():
    """Verify payloads exceeding max_payload_bytes return HTTP 413 Payload Too Large."""
    workflow_registry.clear()

    @workflow("echo_large")
    def echo_large(ctx):
        return "ok"

    app = NexaPy()
    # Configure tiny limit for testing: 100 bytes
    app.mount_workflows(prefix="/workflows", allow_anonymous=True, max_payload_bytes=100)
    client = TestClient(app.fastapi)

    # Payload larger than 100 bytes
    large_payload = {"data": "A" * 500}
    res = client.post("/workflows/echo_large", json=large_payload)
    assert res.status_code == 413
    assert "Payload size exceeds limit" in res.json()["error"]

    # Small payload under limit
    small_payload = {"data": "hi"}
    res_small = client.post("/workflows/echo_large", json=small_payload)
    assert res_small.status_code == 200

    workflow_registry.clear()
