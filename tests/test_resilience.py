"""Network-failure resilience tests via respx.

These exercise the error paths in `client.py`:
- 404 → ATOAPIError with helpful message
- 5xx → ATOAPIError
- Connection timeout → ATOAPIError
- Connection refused / DNS failure → ATOAPIError
- Malformed JSON from CKAN package_show → ATOAPIError
- The error propagates up to a clean ValueError at the tool layer
  (not a raw httpx exception).
"""
from __future__ import annotations

import asyncio
from pathlib import Path

import httpx
import pytest
import respx

from ato_mcp.cache import Cache
from ato_mcp.client import ATOAPIError, ATOClient


@pytest.fixture
def fresh_cache(tmp_path: Path) -> Cache:
    """Each resilience test starts with a fresh, isolated cache."""
    return Cache(tmp_path / "cache.db")


@pytest.mark.asyncio
@respx.mock
async def test_fetch_resource_404(fresh_cache: Cache):
    url = "https://data.gov.au/test/file.xlsx"
    respx.get(url).mock(return_value=httpx.Response(404))
    async with ATOClient(cache=fresh_cache) as client:
        with pytest.raises(ATOAPIError, match="404"):
            await client.fetch_resource(url)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_resource_500(fresh_cache: Cache):
    url = "https://data.gov.au/test/file.xlsx"
    respx.get(url).mock(return_value=httpx.Response(503, text="upstream gone"))
    async with ATOClient(cache=fresh_cache) as client:
        with pytest.raises(ATOAPIError, match="503"):
            await client.fetch_resource(url)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_resource_timeout(fresh_cache: Cache):
    url = "https://data.gov.au/test/file.xlsx"
    respx.get(url).mock(side_effect=httpx.ConnectTimeout("timed out"))
    async with ATOClient(cache=fresh_cache) as client:
        with pytest.raises(ATOAPIError, match="request failed"):
            await client.fetch_resource(url)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_resource_dns_failure(fresh_cache: Cache):
    url = "https://data.gov.au/test/file.xlsx"
    respx.get(url).mock(side_effect=httpx.ConnectError("dns lookup failed"))
    async with ATOClient(cache=fresh_cache) as client:
        with pytest.raises(ATOAPIError, match="request failed"):
            await client.fetch_resource(url)


@pytest.mark.asyncio
async def test_fetch_resource_rejects_non_http_url(fresh_cache: Cache):
    """file:// / javascript: / data: URLs must be refused at the boundary."""
    async with ATOClient(cache=fresh_cache) as client:
        for url in (
            "file:///etc/passwd",
            "javascript:alert(1)",
            "data:text/plain,hello",
            "ftp://example.org/file.xlsx",
            "",
        ):
            with pytest.raises(ATOAPIError, match="non-http"):
                await client.fetch_resource(url)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_package_malformed_json(fresh_cache: Cache):
    url_pattern = "https://data.gov.au/data/api/3/action/package_show"
    respx.get(url__startswith=url_pattern).mock(
        return_value=httpx.Response(200, text="<html>not json</html>")
    )
    async with ATOClient(cache=fresh_cache) as client:
        with pytest.raises(ATOAPIError, match="non-JSON"):
            await client.fetch_package("taxation-statistics-2022-23")


@pytest.mark.asyncio
@respx.mock
async def test_fetch_package_success_false(fresh_cache: Cache):
    url_pattern = "https://data.gov.au/data/api/3/action/package_show"
    respx.get(url__startswith=url_pattern).mock(
        return_value=httpx.Response(
            200, json={"success": False, "error": {"message": "not found"}}
        )
    )
    async with ATOClient(cache=fresh_cache) as client:
        with pytest.raises(ATOAPIError, match="CKAN error"):
            await client.fetch_package("does-not-exist")


@pytest.mark.asyncio
async def test_fetch_package_rejects_bad_id_chars(fresh_cache: Cache):
    async with ATOClient(cache=fresh_cache) as client:
        for bad in ("with/slash", "with?question", "with&ampersand"):
            with pytest.raises(ATOAPIError, match="Bad package id"):
                await client.fetch_package(bad)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_resource_cache_hit_does_not_refetch(fresh_cache: Cache):
    url = "https://data.gov.au/test/file.xlsx"
    route = respx.get(url).mock(return_value=httpx.Response(200, content=b"hello"))
    async with ATOClient(cache=fresh_cache) as client:
        assert await client.fetch_resource(url) == b"hello"
        assert await client.fetch_resource(url) == b"hello"
        assert await client.fetch_resource(url) == b"hello"
    # respx records every match — cache should mean only 1 hit
    assert route.call_count == 1


@pytest.mark.asyncio
@respx.mock
async def test_fetch_resource_in_flight_dedup(fresh_cache: Cache):
    """Parallel callers for the same URL → exactly 1 actual HTTP request."""
    url = "https://data.gov.au/test/file.xlsx"

    async def slow_response(request):
        await asyncio.sleep(0.05)
        return httpx.Response(200, content=b"hello")

    route = respx.get(url).mock(side_effect=slow_response)
    async with ATOClient(cache=fresh_cache) as client:
        results = await asyncio.gather(*(client.fetch_resource(url) for _ in range(10)))
    assert all(r == b"hello" for r in results)
    assert route.call_count == 1


# ─── stale-fallback graceful degradation (CLAUDE.md quality dim #4) ──────

async def _prime_stale_cache(
    db_path: Path, url: str, payload: bytes, age_hours: float, kind: str = "data"
) -> None:
    """Put `payload` into the cache as if it was fetched `age_hours` ago.
    Used to test the stale-fallback path: a regular cache.get() with a normal
    TTL will miss this row (because cached_at is older than the TTL window),
    but cache.get_stale() will still return it.
    """
    import time

    import aiosqlite

    from ato_mcp.cache import Cache as _Cache
    cache = _Cache(db_path)
    await cache._ensure_init()
    async with aiosqlite.connect(db_path) as conn:
        await conn.execute(
            "INSERT INTO http_cache (cache_key, payload, cached_at, kind) "
            "VALUES (?, ?, ?, ?) ON CONFLICT(cache_key) DO UPDATE SET "
            "payload=excluded.payload, cached_at=excluded.cached_at",
            (url, payload, time.time() - age_hours * 3600, kind),
        )
        await conn.commit()


@pytest.mark.asyncio
@respx.mock
async def test_stale_fallback_serves_cached_payload_on_5xx(tmp_path: Path):
    """When data.gov.au returns 5xx and we have a cached payload past its
    TTL, serve the cached payload and mark the response as stale. Agents
    continue reasoning rather than crashing."""
    from ato_mcp.client import get_stale_signal, reset_stale_signal

    db_path = tmp_path / "cache.db"
    fixture = (Path(__file__).parent / "fixtures" / "sbb_benchmarks.xlsx").read_bytes()
    url = "https://data.gov.au/data/sbb-benchmarks-2023-24.xlsx"

    # Prime a 14-day-old cache entry — past the 7-day "data" TTL, so cache.get()
    # misses but cache.get_stale() will still return it.
    await _prime_stale_cache(db_path, url, fixture, age_hours=24 * 14)

    reset_stale_signal()
    respx.get(url).mock(return_value=httpx.Response(503, text="Service Unavailable"))

    cache = Cache(db_path)
    async with ATOClient(cache=cache) as client:
        body = await client.fetch_resource(url, kind="data")
        assert body == fixture, "fallback payload must match the primed bytes"
        stale, reason = get_stale_signal()
        assert stale is True, "stale flag must be set after 5xx fallback"
        assert reason and "503" in reason, f"stale_reason should mention the 5xx: {reason}"
        assert "ATO API returned 503" in reason, f"wording must mirror reference: {reason}"
        assert "minute" in reason.lower(), f"stale_reason should report age: {reason}"
        assert "cached payload" in reason, f"stale_reason should mention cached payload: {reason}"


@pytest.mark.asyncio
@respx.mock
async def test_stale_fallback_serves_cached_on_request_error(tmp_path: Path):
    """Same as 5xx test but for httpx.RequestError (DNS / connection refused / etc.)."""
    from ato_mcp.client import get_stale_signal, reset_stale_signal

    db_path = tmp_path / "cache.db"
    fixture = (Path(__file__).parent / "fixtures" / "sbb_benchmarks.xlsx").read_bytes()
    url = "https://data.gov.au/data/sbb-benchmarks-2023-24.xlsx"
    await _prime_stale_cache(db_path, url, fixture, age_hours=24 * 14)

    reset_stale_signal()
    respx.get(url).mock(side_effect=httpx.ConnectError("simulated DNS failure"))

    cache = Cache(db_path)
    async with ATOClient(cache=cache) as client:
        body = await client.fetch_resource(url, kind="data")
        assert body == fixture
        stale, reason = get_stale_signal()
        assert stale is True
        assert reason and "ConnectError" in reason
        assert "unreachable" in reason


@pytest.mark.asyncio
@respx.mock
async def test_raises_when_no_stale_cache_to_fall_back_to(tmp_path: Path):
    """Empty cache + upstream 5xx → still raises ATOAPIError (original behaviour
    when there's nothing to gracefully degrade to)."""
    from ato_mcp.client import reset_stale_signal

    db_path = tmp_path / "cache.db"
    url = "https://data.gov.au/data/sbb-benchmarks-2023-24.xlsx"

    reset_stale_signal()
    respx.get(url).mock(return_value=httpx.Response(503, text="Service Unavailable"))

    cache = Cache(db_path)
    async with ATOClient(cache=cache) as client:
        with pytest.raises(ATOAPIError, match="503"):
            await client.fetch_resource(url, kind="data")


@pytest.mark.asyncio
async def test_cache_get_stale_returns_payload_and_timestamp(tmp_path: Path):
    """Cache.get_stale() returns (payload, cached_at) regardless of TTL —
    the building block for client's stale-fallback path."""
    from datetime import timedelta

    cache = Cache(tmp_path / "cache.db")
    await cache.set("https://example.org/x", b"hello", kind="data")
    # Normal `get` with a tiny TTL should miss
    fresh = await cache.get("https://example.org/x", ttl=timedelta(seconds=0))
    assert fresh is None
    # `get_stale` should return regardless of TTL
    stale = await cache.get_stale("https://example.org/x")
    assert stale is not None
    payload, cached_at = stale
    assert payload == b"hello"
    assert cached_at > 0
    # Non-existent key → None
    miss = await cache.get_stale("https://example.org/missing")
    assert miss is None
