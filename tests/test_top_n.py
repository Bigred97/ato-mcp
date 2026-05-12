"""Tests for the top_n convenience tool.

top_n ranks rows by a measure and returns the top (or bottom) N. It's the
most common agent workflow — "show me the top 10 X by Y" — collapsed into
a single server-side call.
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


async def _fake_fetch(self, url, *, kind="data"):
    for tag, path in FIXTURE_MAP.items():
        if tag in url:
            return path.read_bytes()
    raise RuntimeError(f"no fixture for {url}")


@pytest.fixture(autouse=True)
async def reset_caches():
    server.reset_df_cache_for_tests()
    await server.reset_client_for_tests()
    yield
    server.reset_df_cache_for_tests()
    await server.reset_client_for_tests()


@pytest.fixture
def mocked_client():
    with patch.object(ATOClient, "fetch_resource", _fake_fetch):
        yield


@pytest.mark.asyncio
async def test_top_n_default_top_5(mocked_client):
    r = await server.top_n("CORP_TRANSPARENCY", "tax_payable", n=5)
    assert r.row_count == 5
    # Returns the 5 entities with the largest tax_payable
    values = [rec.value for rec in r.records]
    assert values == sorted(values, reverse=True)
    # Top should be in the billions
    assert r.records[0].value > 1_000_000_000


@pytest.mark.asyncio
async def test_top_n_bottom_direction(mocked_client):
    r = await server.top_n("CORP_TRANSPARENCY", "tax_payable", n=5, direction="bottom")
    assert r.row_count == 5
    # Smallest first
    values = [rec.value for rec in r.records]
    assert values == sorted(values)


@pytest.mark.asyncio
async def test_top_n_with_filter(mocked_client):
    """NSW postcodes only — every returned record must satisfy the filter."""
    r = await server.top_n(
        "IND_POSTCODE_MEDIAN",
        "median_taxable_income_2022_23",
        n=10,
        filters={"state": "nsw"},
    )
    assert r.row_count == 10
    assert all(rec.dimensions.get("state") == "NSW" for rec in r.records)
    # Sorted desc
    values = [rec.value for rec in r.records]
    assert values == sorted(values, reverse=True)


@pytest.mark.asyncio
async def test_top_n_caps_at_available_rows(mocked_client):
    """If the dataset has fewer rows than n, return what's available."""
    r = await server.top_n(
        "IND_POSTCODE_MEDIAN",
        "median_taxable_income_2022_23",
        n=10_000,
        filters={"state": "nt"},  # NT has few postcodes
    )
    # Some rows but well below 10k
    assert 1 <= r.row_count < 200


@pytest.mark.asyncio
async def test_top_n_skips_null_values(mocked_client):
    """Some Corporate Transparency entities have blank tax_payable. They must
    not appear in top/bottom rankings."""
    r = await server.top_n("CORP_TRANSPARENCY", "tax_payable", n=20, direction="bottom")
    assert all(rec.value is not None for rec in r.records)
    assert all(rec.value > 0 for rec in r.records)  # actually >= 0; bottom is real numbers, not nulls


@pytest.mark.asyncio
async def test_top_n_envelope_preserved(mocked_client):
    """The DataResponse envelope (unit, ato_url, attribution, etc.) must come
    through unchanged."""
    r = await server.top_n("CORP_TRANSPARENCY", "tax_payable", n=3)
    assert r.unit == "AUD"
    assert r.source == "Australian Taxation Office"
    assert "Creative Commons" in r.attribution
    assert r.ato_url.startswith("https://data.gov.au/")


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_top_n_unknown_dataset_raises():
    with pytest.raises(ValueError, match="not a curated"):
        await server.top_n("DOES_NOT_EXIST", "x", n=5)


@pytest.mark.asyncio
async def test_top_n_rejects_non_string_measure():
    with pytest.raises(ValueError, match="measure is required"):
        await server.top_n("CORP_TRANSPARENCY", "", n=5)


@pytest.mark.asyncio
async def test_top_n_rejects_bad_direction():
    with pytest.raises(ValueError, match="direction must be"):
        await server.top_n(
            "CORP_TRANSPARENCY", "tax_payable", n=5, direction="sideways",  # type: ignore[arg-type]
        )


@pytest.mark.asyncio
async def test_top_n_rejects_n_zero():
    with pytest.raises(ValueError, match=">= 1"):
        await server.top_n("CORP_TRANSPARENCY", "tax_payable", n=0)


@pytest.mark.asyncio
async def test_top_n_rejects_n_bool():
    with pytest.raises(ValueError, match="positive integer"):
        await server.top_n("CORP_TRANSPARENCY", "tax_payable", n=True)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_top_n_rejects_unknown_measure(mocked_client):
    """Unknown measure name → ValueError with hint listing valid measures."""
    with pytest.raises(ValueError, match="Unknown measure"):
        await server.top_n("CORP_TRANSPARENCY", "not_a_measure", n=5)


@pytest.mark.asyncio
async def test_top_n_caches_across_queries(mocked_client):
    """Two top_n calls on the same dataset → only 1 parse via the df cache."""
    import ato_mcp.server as srv
    original_read_xlsx = srv.read_xlsx
    parse_count = {"calls": 0}

    def counted(*args, **kwargs):
        parse_count["calls"] += 1
        return original_read_xlsx(*args, **kwargs)

    with patch.object(srv, "read_xlsx", counted):
        await server.top_n("CORP_TRANSPARENCY", "tax_payable", n=5)
        await server.top_n("CORP_TRANSPARENCY", "tax_payable", n=10)
        await server.top_n("CORP_TRANSPARENCY", "tax_payable", n=20, direction="bottom")
    assert parse_count["calls"] == 1, (
        f"expected 1 parse for 3 top_n calls, got {parse_count['calls']}"
    )
