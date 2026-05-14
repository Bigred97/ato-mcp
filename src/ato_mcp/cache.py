"""SQLite-backed HTTP cache with per-read TTL.

Ported from rba-mcp. Difference: cache kinds are tuned for ATO's cadence:
- "data": annual ATO releases — 7 days is conservative.
- "latest": short freshness window after publication.
- "register": weekly-updated ACNC register — 24 hours.
- "catalog": CKAN package_show metadata — 1 hour.
"""
from __future__ import annotations

import asyncio
import sqlite3
import time
from datetime import timedelta
from pathlib import Path
from typing import Literal

import aiosqlite

CacheKind = Literal["data", "latest", "register", "catalog"]

DEFAULT_DB_PATH = Path.home() / ".ato-mcp" / "cache.db"

TTL: dict[CacheKind, timedelta] = {
    "data": timedelta(days=7),
    "latest": timedelta(hours=6),
    "register": timedelta(hours=24),
    "catalog": timedelta(hours=1),
}

_SCHEMA = """
CREATE TABLE IF NOT EXISTS http_cache (
    cache_key  TEXT PRIMARY KEY,
    payload    BLOB NOT NULL,
    cached_at  REAL NOT NULL,
    kind       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_kind_cached_at ON http_cache(kind, cached_at);
"""


class Cache:
    def __init__(self, db_path: Path = DEFAULT_DB_PATH) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialized = False
        self._init_lock = asyncio.Lock()

    async def _ensure_init(self) -> None:
        if self._initialized:
            return
        async with self._init_lock:
            if self._initialized:
                return
            try:
                await self._init_schema()
            except sqlite3.DatabaseError:
                # Pre-existing cache.db is corrupt — drop and recreate.
                self.db_path.unlink(missing_ok=True)
                await self._init_schema()
            self._initialized = True

    async def _init_schema(self) -> None:
        async with aiosqlite.connect(self.db_path) as conn:
            await conn.execute("PRAGMA journal_mode=WAL")
            await conn.executescript(_SCHEMA)
            await conn.commit()

    async def _reset_for_corruption(self) -> None:
        """Drop the DB file and force re-init on next access. Used when an
        operation raises sqlite3.DatabaseError mid-session — the file may
        have been corrupted by disk failure, partial truncation, or external
        tampering. Resetting is always safe because the cache is a perf
        layer, not a source of truth."""
        self._initialized = False
        self.db_path.unlink(missing_ok=True)

    async def get(self, key: str, ttl: timedelta) -> bytes | None:
        await self._ensure_init()
        cutoff = time.time() - ttl.total_seconds()
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                async with conn.execute(
                    "SELECT payload FROM http_cache WHERE cache_key = ? AND cached_at >= ?",
                    (key, cutoff),
                ) as cur:
                    row = await cur.fetchone()
        except sqlite3.DatabaseError:
            # Mid-session corruption — drop and recreate, then return None
            # (treat as cache miss). Callers re-fetch from origin.
            await self._reset_for_corruption()
            await self._ensure_init()
            return None
        return row[0] if row else None

    async def get_stale(self, key: str) -> tuple[bytes, float] | None:
        """Return cached (payload, cached_at_epoch) regardless of TTL.

        Used by the client as a fallback when data.gov.au is unavailable —
        graceful degradation per CLAUDE.md quality dimension #4. The caller
        computes "how stale" from the timestamp and surfaces it in
        `DataResponse.stale_reason`.
        """
        await self._ensure_init()
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                async with conn.execute(
                    "SELECT payload, cached_at FROM http_cache WHERE cache_key = ?",
                    (key,),
                ) as cur:
                    row = await cur.fetchone()
        except sqlite3.DatabaseError:
            # Mirror .get(): mid-session corruption → drop, recreate, miss.
            await self._reset_for_corruption()
            await self._ensure_init()
            return None
        return (row[0], row[1]) if row else None

    async def set(self, key: str, value: bytes, kind: CacheKind) -> None:
        await self._ensure_init()
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                await conn.execute(
                    """
                    INSERT INTO http_cache (cache_key, payload, cached_at, kind)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(cache_key) DO UPDATE SET
                        payload = excluded.payload,
                        cached_at = excluded.cached_at,
                        kind = excluded.kind
                    """,
                    (key, value, time.time(), kind),
                )
                await conn.commit()
        except sqlite3.DatabaseError:
            # Mid-session corruption — recreate and retry once. If the retry
            # also fails, the disk is genuinely broken and we propagate.
            await self._reset_for_corruption()
            await self._ensure_init()
            async with aiosqlite.connect(self.db_path) as conn:
                await conn.execute(
                    """
                    INSERT INTO http_cache (cache_key, payload, cached_at, kind)
                    VALUES (?, ?, ?, ?)
                    """,
                    (key, value, time.time(), kind),
                )
                await conn.commit()

    async def clear(self, kind: CacheKind | None = None) -> None:
        await self._ensure_init()
        async with aiosqlite.connect(self.db_path) as conn:
            if kind:
                await conn.execute("DELETE FROM http_cache WHERE kind = ?", (kind,))
            else:
                await conn.execute("DELETE FROM http_cache")
            await conn.commit()
