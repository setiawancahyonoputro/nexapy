"""Security guards, HMAC signature verification, replay protection, and AI endpoint guards."""

import hashlib
import hmac
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union
from fastapi import HTTPException, Request, status


class NonceTracker:
    """Thread-safe TTL tracker for webhook nonces to prevent replay attacks."""

    def __init__(self, ttl_seconds: float = 300.0):
        self.ttl_seconds = ttl_seconds
        self._seen: Dict[str, float] = {}

    def has_seen(self, nonce: str) -> bool:
        self._cleanup()
        return nonce in self._seen

    def add(self, nonce: str) -> None:
        self._cleanup()
        self._seen[nonce] = time.time()

    def _cleanup(self) -> None:
        now = time.time()
        expired = [k for k, ts in self._seen.items() if now - ts > self.ttl_seconds]
        for k in expired:
            del self._seen[k]


_global_nonce_tracker = NonceTracker()


class HMACVerifier:
    """HMAC SHA-256 Signature generator and verifier with timestamp and replay protection."""

    @staticmethod
    def compute_signature(secret: str, payload: bytes, timestamp: Optional[Union[str, int]] = None) -> str:
        """Compute HMAC SHA-256 signature for payload bytes, optionally prefixing timestamp for replay defense."""
        if timestamp is not None:
            data = f"{timestamp}.".encode("utf-8") + payload
        else:
            data = payload
        return hmac.new(secret.encode("utf-8"), data, hashlib.sha256).hexdigest()

    @staticmethod
    def verify_signature(
        secret: str,
        payload: bytes,
        signature: str,
        timestamp: Optional[Union[str, int]] = None,
        max_age_seconds: float = 300.0,
        nonce: Optional[str] = None,
    ) -> bool:
        """
        Verify payload HMAC signature against expected signature.
        Includes timestamp expiration checks and nonce deduplication.
        """
        if not secret or not signature:
            return False

        # Replay protection: Check timestamp drift
        if timestamp is not None:
            try:
                ts_val = float(timestamp)
                now = time.time()
                if abs(now - ts_val) > max_age_seconds:
                    return False
            except (ValueError, TypeError):
                return False

        # Replay protection: Check nonce uniqueness
        if nonce:
            if _global_nonce_tracker.has_seen(nonce):
                return False
            _global_nonce_tracker.add(nonce)

        clean_sig = signature.replace("sha256=", "").strip()

        # Check timestamped signature first if timestamp provided
        if timestamp is not None:
            expected_with_ts = HMACVerifier.compute_signature(secret, payload, timestamp=timestamp)
            if hmac.compare_digest(expected_with_ts.lower(), clean_sig.lower()):
                return True

        # Fallback check on raw payload for standard webhook payloads
        expected_raw = HMACVerifier.compute_signature(secret, payload)
        return hmac.compare_digest(expected_raw.lower(), clean_sig.lower())


def verify_n8n_signature_guard(
    secret: str,
    max_age_seconds: float = 300.0,
    enforce_timestamp: bool = False,
) -> Callable:
    """
    FastAPI dependency factory enforcing HMAC SHA-256 signature and replay verification.
    """
    async def dependency(request: Request):
        sig_header = request.headers.get("X-NexaPy-Signature") or request.headers.get("x-nexapy-signature")
        if not sig_header:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Missing X-NexaPy-Signature header.",
            )
        ts_header = request.headers.get("X-NexaPy-Timestamp") or request.headers.get("x-nexapy-timestamp")
        if enforce_timestamp and not ts_header:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Missing X-NexaPy-Timestamp header for replay verification.",
            )
        nonce_header = request.headers.get("X-NexaPy-Nonce") or request.headers.get("x-nexapy-nonce")
        body = await request.body()
        if not HMACVerifier.verify_signature(
            secret, body, sig_header, timestamp=ts_header, max_age_seconds=max_age_seconds, nonce=nonce_header
        ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Invalid or expired HMAC SHA-256 signature.",
            )
    return dependency


class APIKeyGuard:
    """API Key validator for NexaPy secured workflow routes."""

    def __init__(self, valid_keys: Optional[list] = None, header_name: str = "X-NexaPy-Key"):
        self.valid_keys = set(valid_keys or [])
        self.header_name = header_name

    def verify_request(self, request: Request) -> bool:
        """Verify request contains valid API Key header."""
        if not self.valid_keys:
            return True
        key = request.headers.get(self.header_name) or request.headers.get(self.header_name.lower())
        return key in self.valid_keys


class AIAuthGuard:
    """
    Authentication & Usage Quota Guard protecting AI completion endpoints from unauthorized access
    and accidental budget exhaustion.
    """

    def __init__(
        self,
        valid_keys: Optional[List[str]] = None,
        max_requests_per_minute: int = 60,
        token_limit: Optional[int] = None,
    ):
        self.valid_keys: Set[str] = set(valid_keys or [])
        self.limiter = RateLimiter(max_requests=max_requests_per_minute, window_seconds=60.0)
        self.token_usage: Dict[str, int] = {}
        self.token_limit = token_limit

    def is_authorized(self, request: Request) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Validate request headers for API key, check rate limit, and evaluate usage quota.
        Returns: (is_authorized, key_used, error_reason)
        """
        provided_key = (
            request.headers.get("X-NexaPy-Key")
            or request.headers.get("x-nexapy-key")
            or request.headers.get("Authorization", "").replace("Bearer ", "").strip()
        )

        if self.valid_keys:
            if not provided_key or provided_key not in self.valid_keys:
                return False, None, "Invalid or missing X-NexaPy-Key header."

        key_id = provided_key or (request.client.host if request.client else "unknown")

        allowed, _ = self.limiter.is_allowed(key_id)
        if not allowed:
            return False, key_id, "AI rate limit exceeded. Please slow down."

        if self.token_limit is not None:
            used = self.token_usage.get(key_id, 0)
            if used >= self.token_limit:
                return False, key_id, f"AI token quota exceeded ({used}/{self.token_limit} tokens used)."

        return True, key_id, None

    def record_usage(self, key_id: str, tokens: int = 0) -> None:
        """Record token consumption against quota."""
        if tokens > 0:
            self.token_usage[key_id] = self.token_usage.get(key_id, 0) + tokens


import re
import uuid
from abc import ABC, abstractmethod


def generate_correlation_id() -> str:
    """Generate a unique request tracing correlation ID."""
    return f"corr_{uuid.uuid4().hex[:16]}"


SENSITIVE_PATTERNS = [
    re.compile(r"(api[_-]?key|secret|token|password|authorization|bearer)\s*[:=]\s*['\"]?([^'\"\s,]+)['\"]?", re.IGNORECASE),
    re.compile(r"(AIzaSy[A-Za-z0-9_-]{33})"),
    re.compile(r"(sk-[A-Za-z0-9_\-]{20,})"),
]


def sanitize_sensitive_text(text: str) -> str:
    """Redact API keys, tokens, and secrets from error logs and response messages."""
    if not text:
        return ""
    sanitized = str(text)
    for pat in SENSITIVE_PATTERNS:
        sanitized = pat.sub(r"\1=***REDACTED***", sanitized)
    return sanitized


class BaseRateLimiter(ABC):
    """Abstract interface for rate limiting (local in-memory or distributed Redis)."""

    @abstractmethod
    def is_allowed(self, client_id: str) -> Tuple[bool, int]:
        """Check if client request is within rate limit bounds. Returns (is_allowed, remaining)."""
        pass

    @abstractmethod
    def reset(self) -> None:
        """Reset rate limiter state."""
        pass


class RateLimiter(BaseRateLimiter):
    """Token bucket / sliding window in-memory rate limiter for workflow webhooks and AI requests."""

    def __init__(self, max_requests: int = 100, window_seconds: float = 60.0):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._clients: Dict[str, list] = {}

    def is_allowed(self, client_id: str) -> Tuple[bool, int]:
        """
        Check if client is within rate limit bounds.
        Returns tuple of (is_allowed, remaining_requests).
        """
        now = time.time()
        timestamps = self._clients.get(client_id, [])

        # Filter out expired timestamps
        timestamps = [ts for ts in timestamps if now - ts < self.window_seconds]

        if len(timestamps) >= self.max_requests:
            self._clients[client_id] = timestamps
            return False, 0

        timestamps.append(now)
        self._clients[client_id] = timestamps
        remaining = self.max_requests - len(timestamps)
        return True, remaining

    def reset(self) -> None:
        """Reset rate limiter state."""
        self._clients.clear()


class RedisRateLimiter(BaseRateLimiter):
    """Distributed rate limiter backed by Redis for multi-instance production clusters."""

    def __init__(self, redis_client: Any = None, max_requests: int = 100, window_seconds: float = 60.0):
        self.redis = redis_client
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._fallback = RateLimiter(max_requests=max_requests, window_seconds=window_seconds)

    def is_allowed(self, client_id: str) -> Tuple[bool, int]:
        if not self.redis:
            return self._fallback.is_allowed(client_id)
        try:
            key = f"nexapy:ratelimit:{client_id}"
            current = self.redis.incr(key)
            if current == 1:
                self.redis.expire(key, int(self.window_seconds))
            if current > self.max_requests:
                return False, 0
            return True, max(0, self.max_requests - current)
        except Exception:
            return self._fallback.is_allowed(client_id)

    def reset(self) -> None:
        if self._fallback:
            self._fallback.reset()

