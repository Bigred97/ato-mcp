"""On-disk Parquet cache for parsed DataFrames.

Mirrors `wgea-mcp` / `aihw-mcp`'s parquet_cache module. The in-process
LRU (`_df_cache` in `server.py`) handles warm queries in ~50ms but it's
empty on cold restart — first call after a worker bounce pays the full
pandas/openpyxl parse cost (4-9s for the largest ATO/ACNC files, which
combined with network fetch + JSON serialisation can trip a 20s gateway
budget).

Location: defaults to `~/.ato-mcp/parquet-cache/`, overridable via
`ATO_MCP_PARQUET_CACHE_DIR`.

TTL: 24h, matching ATO's annual/quarterly publish cadence.
"""
from __future__ import annotations

import hashlib
import os
import time
from pathlib import Path
from typing import Any

import pandas as pd

DEFAULT_TTL_SECONDS = 24 * 60 * 60

_ENV_VAR = "ATO_MCP_PARQUET_CACHE_DIR"
_DEFAULT_DIR = Path.home() / ".ato-mcp" / "parquet-cache"


def cache_dir() -> Path:
    override = os.environ.get(_ENV_VAR)
    path = Path(override) if override else _DEFAULT_DIR
    path.mkdir(parents=True, exist_ok=True)
    return path


def _key_to_filename(key: tuple[Any, ...]) -> str:
    payload = repr(key).encode("utf-8")
    return hashlib.sha256(payload).hexdigest() + ".parquet"


def read_if_fresh(
    key: tuple[Any, ...], *, ttl_seconds: int = DEFAULT_TTL_SECONDS
) -> pd.DataFrame | None:
    path = cache_dir() / _key_to_filename(key)
    if not path.is_file():
        return None
    try:
        age = time.time() - path.stat().st_mtime
    except OSError:
        return None
    if age > ttl_seconds:
        return None
    try:
        return pd.read_parquet(path)
    except Exception:
        try:
            path.unlink()
        except OSError:
            pass
        return None


def write(key: tuple[Any, ...], df: pd.DataFrame) -> None:
    target = cache_dir() / _key_to_filename(key)
    tmp = target.with_suffix(".parquet.tmp")
    try:
        df.to_parquet(tmp, engine="pyarrow", compression="snappy", index=False)
        tmp.replace(target)
    except Exception:
        try:
            if tmp.is_file():
                tmp.unlink()
        except OSError:
            pass


def reset_for_tests() -> None:
    d = cache_dir()
    for f in d.glob("*.parquet"):
        try:
            f.unlink()
        except OSError:
            pass
    for f in d.glob("*.parquet.tmp"):
        try:
            f.unlink()
        except OSError:
            pass
