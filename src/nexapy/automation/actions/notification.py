"""Notification and webhook dispatching action for NexaPy Workflows."""

from typing import Any, Dict, Optional
import httpx


class NotificationAction:
    """Action helper for sending outgoing webhook alerts or callbacks to n8n / external endpoints."""

    def __init__(self, default_timeout: float = 15.0):
        self.default_timeout = default_timeout

    async def send_webhook(
        self,
        url: str,
        payload: Dict[str, Any],
        headers: Optional[Dict[str, str]] = None,
        secret: Optional[str] = None,
    ) -> bool:
        """
        Send a POST notification payload to a webhook URL.
        Optionally attaches HMAC signature header if secret is provided.
        """
        import json
        import urllib.parse

        parsed_url = urllib.parse.urlparse(url)
        scheme = (parsed_url.scheme or "").lower()
        if scheme not in ("http", "https"):
            return False
        if (parsed_url.hostname or "").lower() in ("169.254.169.254", "metadata.google.internal"):
            return False

        req_headers = {"Content-Type": "application/json", **(headers or {})}
        body_bytes = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")

        if secret:
            import time
            import urllib.parse
            import uuid
            from nexapy.security import HMACVerifier

            ts = str(int(time.time()))
            nonce = uuid.uuid4().hex
            parsed_url = urllib.parse.urlparse(url)
            req_path = f"{parsed_url.path or '/'}?{parsed_url.query}" if parsed_url.query else (parsed_url.path or "/")

            sig = HMACVerifier.compute_signature_v2(
                secret=secret,
                timestamp=ts,
                nonce=nonce,
                method="POST",
                path=req_path,
                payload=body_bytes,
            )
            req_headers["X-NexaPy-Signature"] = f"sha256={sig}"
            req_headers["X-NexaPy-Timestamp"] = ts
            req_headers["X-NexaPy-Nonce"] = nonce

        try:
            async with httpx.AsyncClient(timeout=self.default_timeout) as client:
                res = await client.post(url, content=body_bytes, headers=req_headers)
                return res.is_success
        except Exception:
            return False
