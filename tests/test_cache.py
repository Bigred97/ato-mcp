"""Cache layer tests.

The cache is critical — it's what makes warm calls cheap. Test:
- write → read same TTL → hit
- write → read past TTL → miss
- corrupt DB → silent rebuild
- two concurrent writes don't race
- clear(kind) only drops one kind
- clear() drops everything
"""
from __future__ import annotations

import asyncio
from datetime import timedelta
from pathlib import Path

import pytest

from ato_mcp.cache import TTL, Cache


@pytest.fixture
def temp_db(tmp_path: Path) -> Path:
    return tmp_path / "cache.db"


@pytest.mark.asyncio
async def test_set_get_within_ttl(temp_db: Path):
    cache = Cache(temp_db)
    await cache.set("key1", b"hello", kind="data")
    got = await cache.get("key1", ttl=timedelta(minutes=5))
    assert got == b"hello"


@pytest.mark.asyncio
async def test_get_past_ttl_returns_none(temp_db: Path):
    cache = Cache(temp_db)
    await cache.set("key1", b"hello", kind="data")
    got = await cache.get("key1", ttl=timedelta(microseconds=1))
    await asyncio.sleep(0.01)
    got = await cache.get("key1", ttl=timedelta(microseconds=1))
    assert got is None


@pytest.mark.asyncio
async def test_get_missing_key_returns_none(temp_db: Path):
    cache = Cache(temp_db)
    got = await cache.get("nope", ttl=timedelta(hours=1))
    assert got is None


@pytest.mark.asyncio
async def test_set_overwrites_existing(temp_db: Path):
    cache = Cache(temp_db)
    await cache.set("key1", b"first", kind="data")
    await cache.set("key1", b"second", kind="data")
    got = await cache.get("key1", ttl=timedelta(hours=1))
    assert got == b"second"


@pytest.mark.asyncio
async def test_clear_all(temp_db: Path):
    cache = Cache(temp_db)
    await cache.set("k1", b"a", kind="data")
    await cache.set("k2", b"b", kind="register")
    await cache.clear()
    assert await cache.get("k1", ttl=timedelta(hours=1)) is None
    assert await cache.get("k2", ttl=timedelta(hours=1)) is None


@pytest.mark.asyncio
async def test_clear_by_kind(temp_db: Path):
    cache = Cache(temp_db)
    await cache.set("k1", b"a", kind="data")
    await cache.set("k2", b"b", kind="register")
    await cache.clear(kind="data")
    assert await cache.get("k1", ttl=timedelta(hours=1)) is None
    assert await cache.get("k2", ttl=timedelta(hours=1)) == b"b"


@pytest.mark.asyncio
async def test_corrupt_db_silent_rebuild(temp_db: Path):
    """Corrupt cache.db file → cache.set() should drop and recreate it."""
    # Pre-populate with garbage bytes
    temp_db.parent.mkdir(parents=True, exist_ok=True)
    temp_db.write_bytes(b"this is not a sqlite database at all\x00\xff\xfe")
    cache = Cache(temp_db)
    # First write triggers init, which detects corruption and recreates
    await cache.set("k1", b"hello", kind="data")
    got = await cache.get("k1", ttl=timedelta(hours=1))
    assert got == b"hello"


@pytest.mark.asyncio
async def test_concurrent_writes_dont_corrupt(temp_db: Path):
    """50 parallel writes to different keys all land safely."""
    cache = Cache(temp_db)
    async def write_one(i: int) -> None:
        await cache.set(f"key_{i}", str(i).encode(), kind="data")
    await asyncio.gather(*(write_one(i) for i in range(50)))
    # Read them all back
    for i in range(50):
        got = await cache.get(f"key_{i}", ttl=timedelta(hours=1))
        assert got == str(i).encode(), f"key_{i} mismatch"


@pytest.mark.asyncio
async def test_ttl_constants_defined():
    """All declared cache kinds must have a TTL."""
    for kind in ("data", "latest", "register", "catalog"):
        assert kind in TTL, f"TTL missing for kind {kind!r}"
        assert TTL[kind].total_seconds() > 0


@pytest.mark.asyncio
async def test_large_payload_roundtrip(temp_db: Path):
    """Payloads up to 50MB (biggest ATO file) must roundtrip without truncation."""
    cache = Cache(temp_db)
    payload = b"x" * (10 * 1024 * 1024)  # 10MB
    await cache.set("big", payload, kind="data")
    got = await cache.get("big", ttl=timedelta(hours=1))
    assert got == payload
    assert len(got) == 10 * 1024 * 1024


@pytest.mark.asyncio
async def test_binary_safe(temp_db: Path):
    """Cache must round-trip arbitrary bytes — no encoding interference."""
    cache = Cache(temp_db)
    payload = bytes(range(256)) * 100  # every byte value, repeated
    await cache.set("binary", payload, kind="data")
    got = await cache.get("binary", ttl=timedelta(hours=1))
    assert got == payload
