"""Regression tests for the ACNC_REGISTER OperationalError bug.

Live symptom: `GET /v1/data/ato/ACNC_REGISTER` returned HTTP 502
("Upstream error from 'ato': OperationalError") even though the gateway
already grants this dataset a 45s timeout ceiling. Root cause: `Cache`
opened every `aiosqlite` connection with the library's 5s default
busy-timeout. `server.py` keeps a *thread-local* `ATOClient`/`Cache` per
worker thread, so the in-process `_in_flight` de-dupe in `client.py` does
NOT cover concurrent gateway worker threads racing the same cold/expired
cache key — and `ACNC_REGISTER` is a ~50MB payload. Concurrent writers to
the same `cache.db` serialize on SQLite's single-writer lock; under load
that queue can exceed 5s, raising a bare `sqlite3.OperationalError`
("database is locked" / "disk I/O error") that `Cache.set()`'s
corruption-retry path had no further protection against, so a second
failure propagated uncaught.

Reproduced locally (outside this repo, read-only): 24 concurrent OS
processes writing a 58MB payload to the same key against an unmodified
`Cache` raised `sqlite3.OperationalError` in 3 of 4 runs; the same repro
against a `Cache` with every connection raised to a 30s busy-timeout saw
0 errors across 4 runs.

These tests guard the fix: every `aiosqlite.connect` call site in
`cache.py` must route through `Cache._connect()`, which must pass a
busy-timeout well above the 5s default, and `Cache.set()` must still
recover — not propagate — when the underlying connection raises
`sqlite3.OperationalError` (not just generic corruption bytes).
"""
from __future__ import annotations

import inspect
import re
import sqlite3
from datetime import timedelta
from pathlib import Path

import pytest

from ato_mcp import cache as cache_module
from ato_mcp.cache import Cache, _BUSY_TIMEOUT_MS


@pytest.fixture
def temp_db(tmp_path: Path) -> Path:
    return tmp_path / "cache.db"


def test_busy_timeout_well_above_sqlite_default():
    """The whole point of the fix: raise the busy-timeout well past
    aiosqlite/sqlite3's 5s default, while staying under the gateway's 45s
    ceiling for slow datasets like ACNC_REGISTER."""
    assert _BUSY_TIMEOUT_MS >= 15_000, (
        "busy-timeout must be raised meaningfully above the 5s sqlite3 "
        "default to survive concurrent-writer contention on large "
        "payloads like ACNC_REGISTER"
    )
    assert _BUSY_TIMEOUT_MS <= 40_000, (
        "busy-timeout must stay under the gateway's 45s ceiling for "
        "this dataset, or a locked write could itself exceed the "
        "gateway timeout instead of resolving within it"
    )


def test_all_connects_route_through_connect_helper():
    """Regression guard: every `aiosqlite.connect` call in Cache must go
    through `_connect()` (which sets the busy-timeout). A future call site
    that reverts to raw `aiosqlite.connect(self.db_path)` would silently
    reintroduce the 5s-default bug this file exists to catch."""
    source = inspect.getsource(cache_module)
    raw_connect_calls = re.findall(r"aiosqlite\.connect\(", source)
    # Exactly one raw call site is allowed: inside _connect() itself.
    assert len(raw_connect_calls) == 1, (
        f"expected exactly one raw `aiosqlite.connect(` call (inside "
        f"Cache._connect()), found {len(raw_connect_calls)} — every "
        f"other call site must use `self._connect()` so the busy-timeout "
        f"is always applied"
    )


@pytest.mark.asyncio
async def test_connect_helper_passes_busy_timeout(monkeypatch, temp_db: Path):
    """Cache._connect() must pass a timeout kwarg matching _BUSY_TIMEOUT_MS
    to aiosqlite.connect, on every call site (init, get, get_stale, set,
    clear)."""
    seen_kwargs: list[dict] = []
    real_connect = cache_module.aiosqlite.connect

    def spy_connect(*args, **kwargs):
        seen_kwargs.append(kwargs)
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(cache_module.aiosqlite, "connect", spy_connect)

    cache = Cache(temp_db)
    await cache.set("k1", b"v1", kind="data")
    await cache.get("k1", ttl=timedelta(hours=1))
    await cache.get_stale("k1")
    await cache.clear()

    assert seen_kwargs, "no connect() calls observed"
    for kwargs in seen_kwargs:
        assert "timeout" in kwargs, (
            "aiosqlite.connect() called without a timeout kwarg — the "
            "5s sqlite3 default applies, reintroducing the lock-contention "
            "bug"
        )
        assert kwargs["timeout"] == pytest.approx(_BUSY_TIMEOUT_MS / 1000)


@pytest.mark.asyncio
async def test_set_recovers_from_operational_error_not_just_generic_corruption(
    monkeypatch, temp_db: Path
):
    """The pre-existing corruption tests only exercise garbage-bytes
    corruption (a generic sqlite3.DatabaseError). This test simulates the
    *specific* exception class seen live — sqlite3.OperationalError
    ("database is locked") — raised mid-write, and confirms Cache.set()'s
    self-heal-and-retry path absorbs it exactly as it does file corruption,
    rather than propagating the raw OperationalError to the caller (which
    is what the gateway reported as the live 502)."""
    cache = Cache(temp_db)
    # Prime a real, healthy DB first.
    await cache.set("warm", b"warm-value", kind="register")

    real_connect = cache_module.aiosqlite.connect
    call_count = {"n": 0}

    class _FakeCursorCM:
        def __init__(self, exc):
            self._exc = exc

        async def __aenter__(self):
            raise self._exc

        async def __aexit__(self, *exc_info):
            return False

    class _FakeConn:
        """Raises sqlite3.OperationalError on the first `execute()` call
        only, simulating a lock-contention failure on the very first
        connection attempt inside Cache.set()."""

        async def execute(self, *a, **kw):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise sqlite3.OperationalError("database is locked")
            return _FakeCursorCM(RuntimeError("should not reach here"))

        async def commit(self):
            return None

    class _FakeConnectCM:
        async def __aenter__(self):
            return _FakeConn()

        async def __aexit__(self, *exc_info):
            return False

    def fake_connect_once(*args, **kwargs):
        # Only the very first connect() (Cache.set()'s primary attempt)
        # gets the faulty connection; everything after (schema re-init on
        # corruption-reset, the retry write) uses the real thing so we can
        # assert the end state on disk.
        if call_count["n"] == 0 and not hasattr(fake_connect_once, "_armed_used"):
            fake_connect_once._armed_used = True
            return _FakeConnectCM()
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(cache_module.aiosqlite, "connect", fake_connect_once)

    # Must NOT raise — Cache.set()'s except sqlite3.DatabaseError branch
    # (OperationalError is a subclass) must catch this, reset, and retry.
    await cache.set("after_lock_error", b"recovered-value", kind="register")

    # Restore real connect to verify the retry actually landed on disk.
    monkeypatch.setattr(cache_module.aiosqlite, "connect", real_connect)
    got = await cache.get("after_lock_error", ttl=timedelta(hours=1))
    assert got == b"recovered-value", (
        "Cache.set() did not successfully recover and persist the value "
        "after a simulated sqlite3.OperationalError on the first write "
        "attempt"
    )


@pytest.mark.asyncio
async def test_concurrent_writes_to_same_key_dont_raise(temp_db: Path):
    """Realistic-shape regression check: many concurrent writers racing
    the SAME cache key (as gateway worker threads do for a cold/expired
    ACNC_REGISTER entry) must not raise, even with a several-MB payload.
    This is an in-process approximation of the cross-process repro used to
    find the bug — it won't reliably reproduce the original failure on its
    own (that needed real OS-level process contention), but it guards
    against the busy-timeout fix being silently reverted or narrowed."""
    import asyncio

    cache = Cache(temp_db)
    payload = b"x" * (2 * 1024 * 1024)  # 2MB — keep the test fast

    async def write_same_key(i: int) -> None:
        await cache.set("ACNC_REGISTER_URL", payload, kind="register")

    await asyncio.gather(*(write_same_key(i) for i in range(20)))
    got = await cache.get("ACNC_REGISTER_URL", ttl=timedelta(hours=1))
    assert got == payload
