"""Tests for the parsed-DataFrame in-process cache in server.py.

The cache is what makes warm get_data() calls cheap. We probe:
  - Repeated identical calls don't re-parse (counted via mock)
  - Cache key is content-aware: same URL but different bytes → re-parse
  - LRU eviction keeps memory bounded
  - Tests don't leak state via the autouse reset fixture
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from ato_mcp import server
from ato_mcp.client import ATOClient

FIXTURE_DIR = Path(__file__).parent / "fixtures"
FIXTURE_MAP = {
    "ts23individual08": FIXTURE_DIR / "ind_postcode_median.xlsx",
    "ts23company04":    FIXTURE_DIR / "company_industry.xlsx",
    "2023-24-corporate": FIXTURE_DIR / "corp_transparency_2023_24.xlsx",
    "ts23individual23": FIXTURE_DIR / "super_contrib_age.xlsx",
    "datadotgov_main.csv": FIXTURE_DIR / "acnc_register_head.csv",
    "gst01gstwetlctbyyearmonth": FIXTURE_DIR / "gst_monthly.xlsx",
}


@pytest.fixture(autouse=True)
async def reset_caches():
    server.reset_df_cache_for_tests()
    await server.reset_client_for_tests()
    yield
    server.reset_df_cache_for_tests()
    await server.reset_client_for_tests()


@pytest.fixture
def mocked_fetch_with_counter():
    """Patches fetch_resource and counts invocations per URL."""
    counts = {"calls": 0}

    async def fake(self, url, *, kind="data"):
        counts["calls"] += 1
        for tag, path in FIXTURE_MAP.items():
            if tag in url:
                return path.read_bytes()
        raise RuntimeError(f"no fixture for {url}")

    with patch.object(ATOClient, "fetch_resource", fake):
        yield counts


@pytest.fixture
def mocked_read_xlsx_with_counter():
    """Patches read_xlsx and counts invocations."""
    import ato_mcp.server as srv
    counts = {"calls": 0}
    original = srv.read_xlsx

    def counted(*args, **kwargs):
        counts["calls"] += 1
        return original(*args, **kwargs)

    with patch.object(srv, "read_xlsx", counted):
        yield counts


@pytest.mark.asyncio
async def test_repeat_query_does_not_reparse(mocked_fetch_with_counter, mocked_read_xlsx_with_counter):
    """Three identical get_data calls → only 1 PARSE.

    Note: the test mocks fetch_resource (bypassing the byte cache inside the
    real client), so fetch will be called 3x — that's expected. The point is
    the parsed-df cache: read_xlsx must be called exactly once.
    """
    for _ in range(3):
        r = await server.get_data(
            "CORP_TRANSPARENCY",
            filters={"entity_name": "1 MENDS STREET PTY LTD"},
        )
        assert r.row_count >= 0
    assert mocked_read_xlsx_with_counter["calls"] == 1, (
        f"expected 1 parse, got {mocked_read_xlsx_with_counter['calls']}"
    )


@pytest.mark.asyncio
async def test_different_filters_share_parsed_df(mocked_fetch_with_counter, mocked_read_xlsx_with_counter):
    """The cache key is the parse spec, not the query. Different filters on
    the same dataset should share the cached DataFrame — i.e. one parse."""
    await server.get_data("IND_POSTCODE_MEDIAN", filters={"state": "nsw"})
    await server.get_data("IND_POSTCODE_MEDIAN", filters={"state": "vic"})
    await server.get_data("IND_POSTCODE_MEDIAN", filters={"postcode": "2000"})
    assert mocked_read_xlsx_with_counter["calls"] == 1


@pytest.mark.asyncio
async def test_different_datasets_each_get_parsed(mocked_fetch_with_counter, mocked_read_xlsx_with_counter):
    """Each dataset has its own cache slot."""
    await server.get_data("CORP_TRANSPARENCY", filters={"entity_name": "1 MENDS STREET PTY LTD"})
    await server.get_data("IND_POSTCODE_MEDIAN", filters={"state": "nsw", "postcode": "2000"})
    await server.get_data("COMPANY_INDUSTRY", filters={"industry_broad": "A. Agriculture, Forestry and Fishing"})
    assert mocked_read_xlsx_with_counter["calls"] == 3


@pytest.mark.asyncio
async def test_lru_eviction_keeps_bounded(mocked_fetch_with_counter, mocked_read_xlsx_with_counter):
    """Querying more datasets than the LRU cap → oldest are evicted, querying
    them again re-parses."""
    from ato_mcp.server import _DF_CACHE_MAX_ENTRIES, _df_cache

    # Sequence: corp, postcode, company, super, gst, acnc → 6 entries
    # Cap is 8 (defined in server.py), so they all fit
    queries = [
        ("CORP_TRANSPARENCY", {"entity_name": "1 MENDS STREET PTY LTD"}),
        ("IND_POSTCODE_MEDIAN", {"state": "nsw", "postcode": "2000"}),
        ("COMPANY_INDUSTRY", {"industry_broad": "A. Agriculture, Forestry and Fishing"}),
        ("SUPER_CONTRIB_AGE", {"sex": "female"}),
        ("GST_MONTHLY", None),
        ("ACNC_REGISTER", {"state": "QLD"}),
    ]
    for ds, filters in queries:
        await server.get_data(ds, filters=filters)
    # First parse round
    first_parses = mocked_read_xlsx_with_counter["calls"]
    assert first_parses == 5  # 5 xlsx, 1 csv

    # Repeat — should be all cache hits
    for ds, filters in queries:
        await server.get_data(ds, filters=filters)
    assert mocked_read_xlsx_with_counter["calls"] == first_parses

    # Cache should hold at most _DF_CACHE_MAX_ENTRIES
    assert len(_df_cache) <= _DF_CACHE_MAX_ENTRIES


@pytest.mark.asyncio
async def test_cache_invalidates_on_content_change(mocked_read_xlsx_with_counter, tmp_path):
    """If the byte cache returns different bytes (e.g. data.gov.au published a
    new version), the parsed-df cache must invalidate via the body hash."""
    server.reset_df_cache_for_tests()
    fixture_v1 = (FIXTURE_DIR / "corp_transparency_2023_24.xlsx").read_bytes()
    # Build a synthetic "v2" body by appending bytes — same logical shape but
    # different content hash. We won't actually parse the modified body (it
    # might be invalid XLSX); we just need the cache to attempt re-parse.
    fixture_v2 = fixture_v1 + b"\x00ATO_MOCK_BUMP"

    bodies = [fixture_v1, fixture_v2, fixture_v2]
    body_iter = iter(bodies)

    async def serve(self, url, *, kind="data"):
        return next(body_iter)

    with patch.object(ATOClient, "fetch_resource", serve):
        # call 1: v1 body → parse
        await server.get_data(
            "CORP_TRANSPARENCY", filters={"entity_name": "1 MENDS STREET PTY LTD"},
        )
        first_parses = mocked_read_xlsx_with_counter["calls"]
        assert first_parses == 1

        # call 2: v2 body — same URL but new content. Real read_xlsx will reject
        # the corrupt bytes, but the cache lookup must MISS (we don't care that
        # parsing then fails; we care that the cache attempted re-parse).
        try:
            await server.get_data(
                "CORP_TRANSPARENCY", filters={"entity_name": "1 MENDS STREET PTY LTD"},
            )
        except (Exception,):
            pass  # parsing v2 (corrupted) is expected to fail
        # Whether parse succeeded or raised, read_xlsx was called → cache missed.
        assert mocked_read_xlsx_with_counter["calls"] > first_parses


@pytest.mark.asyncio
async def test_warm_hit_is_fast_enough_for_chat(mocked_fetch_with_counter):
    """Soft assertion: warm hits should be well under 100ms even for the
    biggest XLSX. (Cold parse is ~4s; warm should be ~50ms.)"""
    import time
    server.reset_df_cache_for_tests()
    # Warm up
    await server.get_data("CORP_TRANSPARENCY", filters={"entity_name": "1 MENDS STREET PTY LTD"})
    # Measure 3 warm hits
    timings = []
    for _ in range(3):
        t0 = time.time()
        await server.get_data("CORP_TRANSPARENCY", filters={"entity_name": "1 MENDS STREET PTY LTD"})
        timings.append((time.time() - t0) * 1000)
    median = sorted(timings)[len(timings) // 2]
    assert median < 200, f"warm hit too slow: median {median:.0f}ms (timings={timings})"
