"""HTTP action integration for NexaPy Workflows using httpx."""

import ipaddress
import socket
from urllib.parse import urlparse
from typing import Any, Dict, List, Optional, Union
import httpx


def is_ssrf_safe_url(
    url: str,
    allowed_hosts: Optional[List[str]] = None,
    allow_private_hosts: bool = False,
) -> bool:
    """
    Validate whether URL is safe from SSRF attacks.
    Enforces HTTP/HTTPS protocol strictly (rejects ftp://, file://, etc.).
    Blocks localhost, loopback, private RFC 1918/4193 subnets, and cloud metadata (169.254.169.254).
    """
    try:
        parsed = urlparse(url)
        # P1 Finding 19: Strictly enforce HTTP/HTTPS protocol
        scheme = (parsed.scheme or "").lower()
        if scheme not in ("http", "https"):
            return False

        hostname = parsed.hostname
        if not hostname:
            return False

        lower_host = hostname.lower()

        # Always block loopback, metadata services, and internal domains
        BLOCKED_HOSTNAMES = {
            "localhost",
            "127.0.0.1",
            "::1",
            "0.0.0.0",
            "metadata.google.internal",
            "169.254.169.254",
        }
        if lower_host in BLOCKED_HOSTNAMES or lower_host.endswith(".local") or lower_host.endswith(".internal"):
            return False

        # If it's an IP literal, validate directly
        try:
            ip = ipaddress.ip_address(lower_host)
            # Cloud metadata and loopback are never safe
            if ip.is_loopback or ip.is_link_local or ip.is_reserved or str(ip) == "169.254.169.254":
                return False
            if ip.is_private and not allow_private_hosts:
                return False
            if allowed_hosts and lower_host not in allowed_hosts:
                return False
            return True
        except ValueError:
            pass

        # If allowed_hosts is specified, verify domain membership
        if allowed_hosts:
            host_match = lower_host in allowed_hosts or any(lower_host.endswith(f".{h.lstrip('.')}") for h in allowed_hosts)
            if not host_match:
                return False

        # DNS resolution check: verify resolved IP is safe
        try:
            addr_info = socket.getaddrinfo(hostname, None)
            for item in addr_info:
                ip_str = item[4][0]
                ip = ipaddress.ip_address(ip_str)
                if ip.is_loopback or ip.is_link_local or ip.is_reserved or str(ip) == "169.254.169.254":
                    return False
                if ip.is_private and not allow_private_hosts:
                    return False
        except Exception:
            # Fallback for mock/offline domains
            if lower_host in ("localhost", "local", "internal", "lan", "home") or "." not in lower_host:
                return False

        return True
    except Exception:
        return False


class HTTPAction:
    """HTTP Action wrapper providing async HTTP client methods for workflows with SSRF guard."""

    def __init__(
        self,
        default_timeout: float = 30.0,
        prevent_ssrf: bool = True,
        allowed_hosts: Optional[List[str]] = None,
    ):
        self.default_timeout = default_timeout
        self.prevent_ssrf = prevent_ssrf
        self.allowed_hosts = allowed_hosts

    def _validate_ssrf(self, url: str) -> None:
        if self.prevent_ssrf and not is_ssrf_safe_url(url, allowed_hosts=self.allowed_hosts):
            raise ValueError(f"SSRF Protection blocked request to host in URL: '{url}'")

    async def request(
        self,
        method: str,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        json: Optional[Any] = None,
        data: Optional[Any] = None,
        timeout: Optional[Union[float, int]] = None,
        **kwargs: Any,
    ) -> httpx.Response:
        """Execute an HTTP request and return the httpx.Response object with SSRF protection."""
        self._validate_ssrf(url)
        req_timeout = timeout if timeout is not None else self.default_timeout
        async with httpx.AsyncClient(timeout=req_timeout) as client:
            response = await client.request(
                method=method.upper(),
                url=url,
                params=params,
                headers=headers,
                json=json,
                data=data,
                **kwargs,
            )
            return response

    async def get(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[Union[float, int]] = None,
        **kwargs: Any,
    ) -> httpx.Response:
        """Execute HTTP GET request."""
        return await self.request("GET", url, params=params, headers=headers, timeout=timeout, **kwargs)

    async def post(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        json: Optional[Any] = None,
        data: Optional[Any] = None,
        timeout: Optional[Union[float, int]] = None,
        **kwargs: Any,
    ) -> httpx.Response:
        """Execute HTTP POST request."""
        return await self.request("POST", url, params=params, headers=headers, json=json, data=data, timeout=timeout, **kwargs)

    async def put(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        json: Optional[Any] = None,
        data: Optional[Any] = None,
        timeout: Optional[Union[float, int]] = None,
        **kwargs: Any,
    ) -> httpx.Response:
        """Execute HTTP PUT request."""
        return await self.request("PUT", url, params=params, headers=headers, json=json, data=data, timeout=timeout, **kwargs)

    async def patch(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        json: Optional[Any] = None,
        data: Optional[Any] = None,
        timeout: Optional[Union[float, int]] = None,
        **kwargs: Any,
    ) -> httpx.Response:
        """Execute HTTP PATCH request."""
        return await self.request("PATCH", url, params=params, headers=headers, json=json, data=data, timeout=timeout, **kwargs)

    async def delete(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[Union[float, int]] = None,
        **kwargs: Any,
    ) -> httpx.Response:
        """Execute HTTP DELETE request."""
        return await self.request("DELETE", url, params=params, headers=headers, timeout=timeout, **kwargs)
