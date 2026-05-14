"""Async fetcher for data.gov.au XLSX/CSV resources and CKAN package metadata.

Two endpoints:
- `fetch_resource(url)`  — pulls a static XLSX/CSV file by URL. Cached as "data".
- `fetch_package(name)`  — CKAN `package_show` for a dataset slug. Cached as "catalog".

data.gov.au is CKAN under the Drupal 11 wrapper. The CKAN API path is
`/data/api/3/action/...`. Public, no auth, no documented rate limit. We send
a courteous User-Agent and dedupe concurrent in-flight requests for the
same URL so a burst of `latest()` calls fans in to one HTTP request.
"""
from __future__ import annotations

import asyncio
import json
import time
from contextvars import ContextVar
from typing import Any

import httpx

from .cache import TTL, Cache, CacheKind

DEFAULT_BASE_URL = "https://data.gov.au"
DEFAULT_TIMEOUT = httpx.Timeout(120.0, connect=15.0)  # ACNC CSV is 14MB; allow time


# ─── stale signal (graceful-degradation reporting per CLAUDE.md dim #4) ─
# When data.gov.au is unreachable, _fetch_cached falls back to the cached
# payload regardless of TTL and records the staleness in this ContextVar.
# Server-side tool wrappers read it after the request chain and set
# DataResponse.stale / .stale_reason. ContextVar (not instance attr) so
# concurrent MCP tool calls each see their own state.
_stale_signal: ContextVar[tuple[bool, str | None]] = ContextVar(
    "ato_mcp_stale_signal", default=(False, None)
)


def reset_stale_signal() -> None:
    """Clear the stale state. Call once at the start of each tool call."""
    _stale_signal.set((False, None))


def get_stale_signal() -> tuple[bool, str | None]:
    """Return (stale, reason) for the most recent fetch chain in this context."""
    return _stale_signal.get()


def _mark_stale(reason: str) -> None:
    """Record that a stale-cache fallback was served this context.

    If multiple fetches in one chain are stale, we keep the FIRST reason
    (it's usually the most informative — the originating upstream failure).
    """
    cur_stale, _ = _stale_signal.get()
    if not cur_stale:
        _stale_signal.set((True, reason))


class ATOAPIError(Exception):
    """Raised when data.gov.au returns non-2xx or the request fails."""


class ATOClient:
    def __init__(
        self,
        cache: Cache | None = None,
        base_url: str = DEFAULT_BASE_URL,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.cache = cache or Cache()
        self._http = httpx.AsyncClient(
            timeout=DEFAULT_TIMEOUT,
            transport=transport,
            headers={
                "User-Agent": "ato-mcp/0.1 (+https://github.com/Bigred97/ato-mcp)",
            },
            follow_redirects=True,
        )
        self._in_flight: dict[str, asyncio.Future[bytes]] = {}
        self._in_flight_lock = asyncio.Lock()

    async def aclose(self) -> None:
        await self._http.aclose()

    async def __aenter__(self) -> ATOClient:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.aclose()

    async def fetch_resource(
        self, url: str, *, kind: CacheKind = "data"
    ) -> bytes:
        """Fetch a static XLSX/CSV file by URL. Cached. In-flight deduped."""
        if not url.startswith(("http://", "https://")):
            raise ATOAPIError(f"Refusing to fetch non-http(s) URL: {url!r}")
        return await self._fetch_cached(url, kind=kind)

    async def fetch_package(self, package_id: str) -> dict[str, Any]:
        """Fetch CKAN package_show for a dataset slug. Returns the result dict.

        `package_id` is the data.gov.au dataset slug (e.g. 'taxation-statistics-2022-23').
        Raises ATOAPIError if the dataset doesn't exist or CKAN returns success=false.
        """
        if "/" in package_id or "?" in package_id or "&" in package_id:
            raise ATOAPIError(f"Bad package id: {package_id!r}")
        url = f"{self.base_url}/data/api/3/action/package_show?id={package_id}"
        body = await self._fetch_cached(url, kind="catalog")
        try:
            payload = json.loads(body.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            raise ATOAPIError(f"CKAN returned non-JSON for {package_id!r}: {e}") from e
        if not payload.get("success"):
            err = payload.get("error", {})
            raise ATOAPIError(f"CKAN error for {package_id!r}: {err}")
        result = payload.get("result")
        if not isinstance(result, dict):
            raise ATOAPIError(f"CKAN result missing for {package_id!r}")
        return result

    async def _fetch_cached(self, url: str, *, kind: CacheKind) -> bytes:
        cached = await self.cache.get(url, ttl=TTL[kind])
        if cached is not None:
            return cached

        async with self._in_flight_lock:
            existing = self._in_flight.get(url)
            if existing is None:
                future: asyncio.Future[bytes] = (
                    asyncio.get_running_loop().create_future()
                )
                self._in_flight[url] = future

        if existing is not None:
            return await existing

        try:
            try:
                resp = await self._http.get(url)
                resp.raise_for_status()
            except (httpx.HTTPStatusError, httpx.RequestError) as e:
                # Graceful degradation: when data.gov.au is unreachable,
                # fall back to the most-recent cached payload (regardless of
                # TTL) rather than raising and breaking the agent's chain
                # of reasoning. The staleness is surfaced via the
                # _stale_signal ContextVar and ends up in DataResponse.stale.
                #
                # Only the data-fetch kinds degrade gracefully. CKAN catalog
                # lookups ("I don't know the URL") fail differently to data
                # fetches ("the data is down") — serving stale CKAN metadata
                # could resolve to a stale resource URL and silently mask
                # ATO renames. Discovery already falls back to the YAML's
                # hard-coded download_url on DiscoveryError, so a clean
                # ATOAPIError here is the right signal.
                if kind != "catalog":
                    fallback = await self.cache.get_stale(url)
                    if fallback is not None:
                        payload, cached_at = fallback
                        age_min = max(0, int((time.time() - cached_at) / 60))
                        if isinstance(e, httpx.HTTPStatusError):
                            upstream = (
                                f"ATO API returned {e.response.status_code}"
                            )
                        else:
                            upstream = (
                                f"ATO API unreachable ({type(e).__name__})"
                            )
                        _mark_stale(
                            f"{upstream} for {url}; serving cached payload "
                            f"from ~{age_min} minute(s) ago"
                        )
                        future.set_result(payload)
                        return payload
                # Genuinely no cache to fall back to (or CKAN failure) —
                # preserve original behaviour.
                if isinstance(e, httpx.HTTPStatusError):
                    raise ATOAPIError(
                        f"data.gov.au returned {e.response.status_code} for {url}"
                    ) from e
                raise ATOAPIError(f"data.gov.au request failed: {e}") from e
            await self.cache.set(url, resp.content, kind=kind)
            future.set_result(resp.content)
            return resp.content
        except BaseException as e:
            if not future.done():
                future.set_exception(e)
            raise
        finally:
            async with self._in_flight_lock:
                self._in_flight.pop(url, None)
