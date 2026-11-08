"""Security guards, HMAC signature verification, replay protection, and AI endpoint guards."""

import hashlib
import hmac
import sqlite3
import threading
import time
import urllib.parse
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union
from fastapi import HTTPException, Request, status


@dataclass
class AuthenticatedPrincipal:
    """Server-verified principal identity with tenant binding and authorized scopes."""
    principal_id: str
    tenant_id: str
    scopes: List[str] = field(default_factory=lambda: ["*"])
    allowed_tenants: List[str] = field(default_factory=list)

    def is_tenant_authorized(self, requested_tenant: Optional[str]) -> bool:
        if not requested_tenant or requested_tenant == self.tenant_id:
            return True
        if "*" in self.allowed_tenants:
            return True
        return requested_tenant in self.allowed_tenants

    def to_dict(self) -> Dict[str, Any]:
        return {
            "principal_id": self.principal_id,
            "tenant_id": self.tenant_id,
            "scopes": list(self.scopes),
            "allowed_tenants": list(self.allowed_tenants),
        }


class BaseNonceStore(ABC):
    """Abstract base class for replay attack nonce stores."""

    @abstractmethod
    def check_and_add(self, nonce: str, ttl_seconds: Optional[float] = None, principal: str = "default") -> bool:
        """
        Atomically check if nonce was seen; if not seen, record it and return True.
        If already seen (replayed), return False.
        """
        pass

    @abstractmethod
    def has_seen(self, nonce: str, principal: str = "default") -> bool:
        """Return True if nonce is present and not expired."""
        pass

    @abstractmethod
    def add(self, nonce: str, ttl_seconds: Optional[float] = None, principal: str = "default") -> None:
        """Record a nonce without atomic checking."""
        pass

    @abstractmethod
    def clear(self) -> None:
        """Clear all stored nonces."""
        pass


class MemoryNonceStore(BaseNonceStore):
    """Thread-safe TTL in-memory store for nonces."""

    def __init__(self, ttl_seconds: float = 300.0):
        self.ttl_seconds = ttl_seconds
        self._seen: Dict[Tuple[str, str], float] = {}
        self._lock = threading.RLock()

    def has_seen(self, nonce: str, principal: str = "default") -> bool:
        with self._lock:
            self._cleanup()
            return (principal, nonce) in self._seen

    def check_and_add(self, nonce: str, ttl_seconds: Optional[float] = None, principal: str = "default") -> bool:
        ttl = ttl_seconds or self.ttl_seconds
        with self._lock:
            self._cleanup()
            key = (principal, nonce)
            if key in self._seen:
                return False
            self._seen[key] = time.time() + ttl
            return True

    def add(self, nonce: str, ttl_seconds: Optional[float] = None, principal: str = "default") -> None:
        ttl = ttl_seconds or self.ttl_seconds
        with self._lock:
            self._cleanup()
            self._seen[(principal, nonce)] = time.time() + ttl

    def clear(self) -> None:
        with self._lock:
            self._seen.clear()

    def _cleanup(self) -> None:
        now = time.time()
        expired = [k for k, exp in self._seen.items() if now > exp]
        for k in expired:
            del self._seen[k]


class SQLiteNonceStore(BaseNonceStore):
    """
    Persistent SQLite-backed nonce store providing atomic replay protection across
    multiple uvicorn worker processes and server restarts.
    """

    def __init__(self, db_path: str, ttl_seconds: float = 300.0):
        self.db_path = db_path
        self.ttl_seconds = ttl_seconds
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30.0, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def _init_db(self) -> None:
        conn = self._get_connection()
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS nexapy_nonces (
                    principal TEXT NOT NULL,
                    nonce TEXT NOT NULL,
                    expires_at REAL NOT NULL,
                    PRIMARY KEY (principal, nonce)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_nexapy_nonces_exp ON nexapy_nonces(expires_at)"
            )
            conn.commit()
        finally:
            conn.close()

    def has_seen(self, nonce: str, principal: str = "default") -> bool:
        now = time.time()
        conn = self._get_connection()
        try:
            cursor = conn.execute(
                "SELECT 1 FROM nexapy_nonces WHERE principal = ? AND nonce = ? AND expires_at > ?",
                (principal, nonce, now),
            )
            return cursor.fetchone() is not None
        finally:
            conn.close()

    def check_and_add(self, nonce: str, ttl_seconds: Optional[float] = None, principal: str = "default") -> bool:
        ttl = ttl_seconds or self.ttl_seconds
        now = time.time()
        expires_at = now + ttl
        conn = self._get_connection()
        try:
            # Opportunistically cleanup expired nonces
            try:
                conn.execute("DELETE FROM nexapy_nonces WHERE expires_at < ?", (now,))
            except Exception:
                pass

            try:
                conn.execute(
                    "INSERT INTO nexapy_nonces (principal, nonce, expires_at) VALUES (?, ?, ?)",
                    (principal, nonce, expires_at),
                )
                conn.commit()
                return True
            except sqlite3.IntegrityError:
                conn.rollback()
                return False
        finally:
            conn.close()

    def add(self, nonce: str, ttl_seconds: Optional[float] = None, principal: str = "default") -> None:
        self.check_and_add(nonce, ttl_seconds=ttl_seconds, principal=principal)

    def clear(self) -> None:
        conn = self._get_connection()
        try:
            conn.execute("DELETE FROM nexapy_nonces")
            conn.commit()
        finally:
            conn.close()


class NonceTracker(MemoryNonceStore):
    """
    Thread-safe TTL tracker for webhook nonces to prevent replay attacks.
    Preserves backwards-compatible dict-based attribute access.
    """

    def __init__(self, ttl_seconds: float = 300.0):
        super().__init__(ttl_seconds=ttl_seconds)
        self._legacy_seen: Dict[str, float] = {}

    @property
    def _seen(self) -> Dict[str, float]:  # type: ignore[override]
        return self._legacy_seen

    @_seen.setter
    def _seen(self, value: Dict[str, float]) -> None:
        self._legacy_seen = value

    def has_seen(self, nonce: str, principal: str = "default") -> bool:
        return super().has_seen(nonce, principal) or nonce in self._legacy_seen

    def check_and_add(self, nonce: str, ttl_seconds: Optional[float] = None, principal: str = "default") -> bool:
        with self._lock:
            self._cleanup()
            if nonce in self._legacy_seen:
                return False
            res = super().check_and_add(nonce, ttl_seconds, principal)
            if res:
                self._legacy_seen[nonce] = time.time()
            return res

    def add(self, nonce: str, ttl_seconds: Optional[float] = None, principal: str = "default") -> None:
        with self._lock:
            self._cleanup()
            self._legacy_seen[nonce] = time.time()
            super().add(nonce, ttl_seconds, principal)

    def clear(self) -> None:
        with self._lock:
            self._legacy_seen.clear()
            super().clear()

    def _cleanup(self) -> None:
        super()._cleanup()
        now = time.time()
        expired = [k for k, ts in self._legacy_seen.items() if now - ts > self.ttl_seconds]
        for k in expired:
            del self._legacy_seen[k]


_global_nonce_store: BaseNonceStore = NonceTracker()
_global_nonce_tracker = _global_nonce_store


def get_global_nonce_store() -> BaseNonceStore:
    """Get the active global nonce store (memory or SQLite)."""
    return _global_nonce_store


def set_global_nonce_store(store: BaseNonceStore) -> None:
    """Set the active global nonce store for replay defense."""
    global _global_nonce_store, _global_nonce_tracker
    _global_nonce_store = store
    _global_nonce_tracker = store


def canonicalize_path(path: Optional[str]) -> str:
    """
    Normalize URL path and query parameters for canonical HMAC signature computation.
    Lexicographically sorts query parameters to prevent bypass via parameter reordering
    or query string injection.
    """
    if not path:
        return "/"
    p = path.strip()
    if "?" in p:
        base_path, query_str = p.split("?", 1)
    else:
        base_path, query_str = p, ""

    if not base_path.startswith("/"):
        base_path = "/" + base_path
    if len(base_path) > 1 and base_path.endswith("/"):
        base_path = base_path.rstrip("/")

    if query_str:
        pairs = urllib.parse.parse_qsl(query_str, keep_blank_values=True)
        pairs_sorted = sorted(pairs, key=lambda kv: (kv[0], kv[1]))
        canonical_query = urllib.parse.urlencode(pairs_sorted)
        return f"{base_path}?{canonical_query}" if canonical_query else base_path

    return base_path


class HMACVerifier:
    """HMAC SHA-256 Signature generator and verifier with NexaPy-HMAC-v2 protocol and replay protection."""

    @staticmethod
    def compute_string_to_sign_v2(
        timestamp: Union[str, int],
        nonce: str,
        method: str = "POST",
        path: str = "/",
        payload: bytes = b"",
    ) -> str:
        """Construct the canonical byte payload string for NexaPy-HMAC-v2."""
        canonical_path = canonicalize_path(path)
        payload_bytes = payload if isinstance(payload, bytes) else (payload.encode("utf-8") if isinstance(payload, str) else b"")
        body_hash = hashlib.sha256(payload_bytes).hexdigest()
        return f"v2\n{timestamp}\n{nonce}\n{method.upper()}\n{canonical_path}\n{body_hash}"

    @staticmethod
    def compute_signature_v2(
        secret: str,
        timestamp: Union[str, int],
        nonce: str,
        method: str = "POST",
        path: str = "/",
        payload: bytes = b"",
    ) -> str:
        """Compute standard NexaPy-HMAC-v2 signature using HMAC-SHA256."""
        string_to_sign = HMACVerifier.compute_string_to_sign_v2(
            timestamp=timestamp, nonce=nonce, method=method, path=path, payload=payload
        )
        return hmac.new(secret.encode("utf-8"), string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()

    @staticmethod
    def compute_signature(
        secret: str,
        payload: bytes = b"",
        timestamp: Optional[Union[str, int]] = None,
        nonce: Optional[str] = None,
        method: str = "POST",
        path: str = "/",
        version: str = "v2",
    ) -> str:
        """
        Compute HMAC SHA-256 signature for payload.
        If version is 'v2' and nonce & timestamp are provided, computes NexaPy-HMAC-v2 signature.
        Otherwise falls back to timestamp-prefixed or raw payload signature for legacy compatibility.
        """
        if version == "v2" and nonce is not None and timestamp is not None:
            return HMACVerifier.compute_signature_v2(
                secret=secret,
                timestamp=timestamp,
                nonce=nonce,
                method=method,
                path=path,
                payload=payload,
            )
        if timestamp is not None:
            data = f"{timestamp}.".encode("utf-8") + (payload or b"")
        else:
            data = payload or b""
        return hmac.new(secret.encode("utf-8"), data, hashlib.sha256).hexdigest()

    @staticmethod
    def verify_signature(
        secret: str,
        payload: bytes,
        signature: str,
        timestamp: Optional[Union[str, int]] = None,
        max_age_seconds: float = 300.0,
        nonce: Optional[str] = None,
        method: str = "POST",
        path: str = "/",
        enforce_timestamp: bool = False,
        enforce_nonce: bool = False,
        allow_legacy: bool = True,
        nonce_store: Optional[BaseNonceStore] = None,
    ) -> bool:
        """
        Verify payload HMAC signature against expected signature.
        Supports NexaPy-HMAC-v2 with timestamp, nonce, method, path, and body SHA-256 hash.
        Strict verification:
        - If timestamp is provided or enforce_timestamp is True, timestamp is mandatory and expiration checked.
        - If timestamp is provided or enforce_timestamp is True or enforce_nonce is True, nonce is mandatory.
        - Replay defense: Nonce is checked and atomically recorded only after the signature is verified.
        - If allow_legacy is False or timestamp/nonce is provided, legacy fallback without timestamp is rejected.
        """
        if not secret or not signature:
            return False

        active_nonce_store = nonce_store or _global_nonce_store
        clean_sig = signature.replace("sha256=", "").strip()
        has_timestamp = timestamp is not None and str(timestamp).strip() != ""
        has_nonce = nonce is not None and str(nonce).strip() != ""

        # Check timestamp requirements
        if enforce_timestamp or has_timestamp:
            if not has_timestamp:
                return False
            # P0 Finding: When timestamp is enforced/provided, nonce is mandatory
            if not has_nonce:
                return False
            try:
                ts_val = float(timestamp)
                now = time.time()
                if abs(now - ts_val) > max_age_seconds:
                    return False
            except (ValueError, TypeError):
                return False

            # Strict check against NexaPy-HMAC-v2 protocol
            expected_v2 = HMACVerifier.compute_signature_v2(
                secret=secret,
                timestamp=timestamp,
                nonce=nonce,
                method=method,
                path=path,
                payload=payload,
            )
            if hmac.compare_digest(expected_v2.lower(), clean_sig.lower()):
                if not active_nonce_store.check_and_add(nonce, ttl_seconds=max_age_seconds):
                    return False  # Replayed nonce!
                return True

            if allow_legacy:
                expected_v1 = hmac.new(
                    secret.encode("utf-8"),
                    f"{timestamp}.".encode("utf-8") + (payload or b""),
                    hashlib.sha256,
                ).hexdigest()
                if hmac.compare_digest(expected_v1.lower(), clean_sig.lower()):
                    if has_nonce and not active_nonce_store.check_and_add(nonce, ttl_seconds=max_age_seconds):
                        return False
                    return True

            return False

        if enforce_nonce and not has_nonce:
            return False

        # If timestamp is omitted, only allowed when not enforcing timestamp and legacy is allowed
        if not allow_legacy:
            return False

        expected_raw = HMACVerifier.compute_signature(secret, payload, version="v1")
        if not hmac.compare_digest(expected_raw.lower(), clean_sig.lower()):
            return False

        if has_nonce:
            if not active_nonce_store.check_and_add(nonce, ttl_seconds=max_age_seconds):
                return False

        return True


def verify_n8n_signature_guard(
    secret: str,
    max_age_seconds: float = 300.0,
    enforce_timestamp: bool = True,
    enforce_nonce: bool = True,
    allow_legacy: bool = False,
    nonce_store: Optional[BaseNonceStore] = None,
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
        if enforce_nonce and not nonce_header:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Missing X-NexaPy-Nonce header for replay verification.",
            )
        body = await request.body()
        req_path = f"{request.url.path}?{request.url.query}" if request.url.query else request.url.path
        if not HMACVerifier.verify_signature(
            secret,
            body,
            sig_header,
            timestamp=ts_header,
            max_age_seconds=max_age_seconds,
            nonce=nonce_header,
            method=request.method,
            path=req_path,
            enforce_timestamp=enforce_timestamp,
            enforce_nonce=enforce_nonce,
            allow_legacy=allow_legacy,
            nonce_store=nonce_store,
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


MAX_TOKENS_CEILING = 128000


class AIAuthGuard:
    """
    Authentication & Usage Quota Guard protecting AI completion endpoints from unauthorized access
    and accidental budget exhaustion.
    Supports upfront budget reservation, reconciliation, and ceiling enforcement.
    Fails closed by default when unconfigured unless allow_anonymous is explicitly True.
    """

    def __init__(
        self,
        valid_keys: Optional[List[str]] = None,
        max_requests_per_minute: int = 60,
        token_limit: Optional[int] = None,
        allow_anonymous: bool = False,
    ):
        self.valid_keys: Set[str] = set(valid_keys or [])
        self.limiter = RateLimiter(max_requests=max_requests_per_minute, window_seconds=60.0)
        self.token_usage: Dict[str, int] = {}
        self.token_reservations: Dict[str, Tuple[str, int]] = {}
        self.token_limit = token_limit
        self.allow_anonymous = allow_anonymous

    @property
    def active_reservations(self) -> Dict[str, int]:
        """Aggregate active token reservations by key_id."""
        res: Dict[str, int] = {}
        for kid, tok in self.token_reservations.values():
            res[kid] = res.get(kid, 0) + tok
        return res

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
                if not self.allow_anonymous:
                    return False, None, "Invalid or missing X-NexaPy-Key header."
        else:
            if not self.allow_anonymous:
                return False, None, "AI endpoint authentication required. Configure an API key or enable allow_anonymous_ai=True."

        key_id = provided_key or (request.client.host if request.client else "unknown")

        allowed, _ = self.limiter.is_allowed(key_id)
        if not allowed:
            return False, key_id, "AI rate limit exceeded. Please slow down."

        if self.token_limit is not None:
            used = self.token_usage.get(key_id, 0)
            pending = sum(tok for kid, tok in self.token_reservations.values() if kid == key_id)
            if used + pending >= self.token_limit:
                return False, key_id, f"AI token quota exceeded ({used + pending}/{self.token_limit} tokens used or reserved)."

        return True, key_id, None

    def reserve_budget(self, key_id: str, estimated_tokens: int) -> Tuple[bool, Optional[str], Optional[str]]:
        """Reserve token budget upfront before executing provider request (P1 Finding 10)."""
        if self.token_limit is not None and estimated_tokens > 0:
            used = self.token_usage.get(key_id, 0)
            pending = sum(tok for kid, tok in self.token_reservations.values() if kid == key_id)
            if used + pending + estimated_tokens > self.token_limit:
                return False, None, f"AI token budget exceeded. Required {estimated_tokens}, available {max(0, self.token_limit - used - pending)}."

        reservation_id = f"res_{uuid.uuid4().hex[:12]}"
        self.token_reservations[reservation_id] = (key_id, max(0, estimated_tokens))
        return True, reservation_id, None

    def reconcile_budget(self, reservation_id: Optional[str], key_id: str, actual_tokens: int = 0) -> None:
        """Release reservation and record authoritative token usage."""
        if reservation_id:
            self.token_reservations.pop(reservation_id, None)
        if actual_tokens > 0:
            self.token_usage[key_id] = self.token_usage.get(key_id, 0) + actual_tokens

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


def is_ssrf_safe_url(*args: Any, **kwargs: Any) -> bool:
    """Validate whether URL is safe from SSRF attacks."""
    from nexapy.automation.actions.http import is_ssrf_safe_url as _is_safe
    return _is_safe(*args, **kwargs)

