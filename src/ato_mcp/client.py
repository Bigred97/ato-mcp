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
from typing import Any

import httpx

from .cache import TTL, Cache, CacheKind

DEFAULT_BASE_URL = "https://data.gov.au"
DEFAULT_TIMEOUT = httpx.Timeout(120.0, connect=15.0)  # ACNC CSV is 14MB; allow time


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

    async def __aenter__(self) -> "ATOClient":
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
            except httpx.HTTPStatusError as e:
                raise ATOAPIError(
                    f"data.gov.au returned {e.response.status_code} for {url}"
                ) from e
            except httpx.RequestError as e:
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
