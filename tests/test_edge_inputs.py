"""Adversarial / fuzz inputs into every public tool.

These probe boundaries the unit-validation tests don't reach: very long
strings, Unicode (emoji, RTL, combining marks), path-traversal attempts,
URL-injection characters in filter values, type confusion (bool vs int,
NaN, infinity), and edge integer values for `limit`.

Goal: every weird input either returns a clean result OR raises a ValueError
with an actionable message. Nothing should crash with a stack trace, a 500,
or silently return wrong data.
"""
from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import patch

import pytest

from ato_mcp import curated, server
from ato_mcp.client import ATOClient


FIXTURE_DIR = Path(__file__).parent / "fixtures"
FIXTURE_MAP = {
    "ind06": FIXTURE_DIR / "corp_transparency_2023_24.xlsx",  # alias
    "ts23individual08": FIXTURE_DIR / "ind_postcode_median.xlsx",
    "ts23company04": FIXTURE_DIR / "company_industry.xlsx",
    "ts23individual23": FIXTURE_DIR / "super_contrib_age.xlsx",
    "2023-24-corporate": FIXTURE_DIR / "corp_transparency_2023_24.xlsx",
    "datadotgov_main.csv": FIXTURE_DIR / "acnc_register_head.csv",
}


async def _fake_fetch(self, url, *, kind="data"):
    for tag, path in FIXTURE_MAP.items():
        if tag in url:
            return path.read_bytes()
    raise RuntimeError(f"no fixture for {url}")


@pytest.fixture
def mocked_client():
    with patch.object(ATOClient, "fetch_resource", _fake_fetch):
        yield


# ---------------------------------------------------------------------------
# search_datasets
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.parametrize("bad_query", [
    None,
    123,
    1.5,
    True,
    [],
    {},
    object(),
    bytes(b"postcode"),
])
async def test_search_datasets_rejects_non_string_query(bad_query):
    with pytest.raises(ValueError):
        await server.search_datasets(bad_query)  # type: ignore[arg-type]


@pytest.mark.asyncio
@pytest.mark.parametrize("ws", ["", "   ", "\t\t", "\n\n", " \r\n "])
async def test_search_datasets_rejects_blank(ws):
    with pytest.raises(ValueError, match="query is required"):
        await server.search_datasets(ws)


@pytest.mark.asyncio
async def test_search_datasets_handles_huge_query():
    huge = "postcode " * 2000  # ~16KB
    r = await server.search_datasets(huge, limit=3)
    assert isinstance(r, list)


@pytest.mark.asyncio
async def test_search_datasets_handles_unicode():
    for q in ["税収", "🏠 postcode", "Tërritørÿ", "𝓟𝓸𝓼𝓽𝓬𝓸𝓭𝓮", "naïve"]:
        r = await server.search_datasets(q, limit=3)
        assert isinstance(r, list)


@pytest.mark.asyncio
async def test_search_datasets_handles_special_chars():
    # Things that would break naive SQL/URL handling
    for q in ["postcode'; DROP TABLE x;--", "<script>alert(1)</script>",
              "../../etc/passwd", "../%2e%2e/passwd", "%00", "\x00postcode"]:
        r = await server.search_datasets(q, limit=3)
        assert isinstance(r, list)


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_limit", [0, -1, -100, False, 1.5, "10", None])
async def test_search_datasets_rejects_bad_limit(bad_limit):
    with pytest.raises(ValueError):
        await server.search_datasets("postcode", limit=bad_limit)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_search_datasets_huge_limit_clipped_by_pydantic():
    # Field(le=50) — pydantic raises if > 50
    from pydantic import ValidationError
    try:
        r = await server.search_datasets("postcode", limit=10**6)
        # If pydantic doesn't trip (maybe we're not in strict-validation path),
        # at least the response should be sane.
        assert len(r) <= len(curated.list_ids())
    except (ValueError, ValidationError):
        pass  # expected


# ---------------------------------------------------------------------------
# describe_dataset
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.parametrize("bad_id", [
    None, 123, 1.5, True, [], {}, b"CORP",
])
async def test_describe_rejects_non_string(bad_id):
    with pytest.raises(ValueError, match="must be a string"):
        await server.describe_dataset(bad_id)  # type: ignore[arg-type]


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_id", [
    "../etc/passwd",
    "CORP/TRANSPARENCY",
    "CORP%20TRANSPARENCY",
    "CORP TRANSPARENCY",
    "corp$transparency",
    "CORP;TRANSPARENCY",
    "CORP\x00TRANSPARENCY",
    "🚀IND_POSTCODE",
    "?dataset=CORP_TRANSPARENCY",
])
async def test_describe_rejects_invalid_chars(bad_id):
    with pytest.raises(ValueError, match="invalid characters"):
        await server.describe_dataset(bad_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("ws_id", ["", "   ", "\t", "\n"])
async def test_describe_rejects_blank(ws_id):
    with pytest.raises(ValueError, match="empty"):
        await server.describe_dataset(ws_id)


@pytest.mark.asyncio
async def test_describe_case_insensitive():
    # Server normalizes to upper
    d_upper = await server.describe_dataset("CORP_TRANSPARENCY")
    d_lower = await server.describe_dataset("corp_transparency")
    d_mixed = await server.describe_dataset("Corp_TransParency")
    d_padded = await server.describe_dataset("  CORP_TRANSPARENCY  ")
    assert d_upper.id == d_lower.id == d_mixed.id == d_padded.id == "CORP_TRANSPARENCY"


@pytest.mark.asyncio
async def test_describe_every_curated_dataset():
    """No dataset should error on describe — they all have valid YAMLs."""
    for dataset_id in curated.list_ids():
        d = await server.describe_dataset(dataset_id)
        assert d.id == dataset_id
        assert d.name
        assert d.description
        assert d.source_url.startswith("https://")
        # At least one measure or dimension
        assert d.dimensions or d.measures


# ---------------------------------------------------------------------------
# get_data
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.parametrize("bad_filters", [
    "not a dict",
    ["state", "nsw"],
    42,
    3.14,
    True,
])
async def test_get_data_rejects_non_dict_filters(bad_filters):
    with pytest.raises(ValueError, match="filters must be a dict"):
        await server.get_data("CORP_TRANSPARENCY", filters=bad_filters)  # type: ignore[arg-type]


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_measures", [
    42, 1.5, True, {"a": "b"}, object(),
])
async def test_get_data_rejects_non_string_measures(bad_measures):
    with pytest.raises(ValueError, match="must be a string or list"):
        await server.get_data("CORP_TRANSPARENCY", measures=bad_measures)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_get_data_rejects_measure_list_with_non_strings():
    with pytest.raises(ValueError, match="must be strings"):
        await server.get_data("CORP_TRANSPARENCY", measures=["total_income", 42])  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_get_data_rejects_empty_string_in_measure_list():
    with pytest.raises(ValueError, match="empty string"):
        await server.get_data("CORP_TRANSPARENCY", measures=["total_income", ""])


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_period", [
    "??", "-1", "abcd", "2024'", "2024;",
    "2024/01", "2024.01", "https://evil/2024",
    "𝟚𝟘𝟚𝟜",  # mathematical digits
])
async def test_get_data_rejects_bad_periods(bad_period):
    with pytest.raises(ValueError, match="invalid format"):
        await server.get_data("CORP_TRANSPARENCY", start_period=bad_period)


@pytest.mark.asyncio
async def test_get_data_strips_period_whitespace():
    """Leading/trailing whitespace on periods is stripped at the boundary —
    treat it as user-friendly normalization, not an error."""
    # Should NOT raise — '2024 ' becomes '2024' after strip
    try:
        await server.get_data("CORP_TRANSPARENCY", start_period="2024 ")
    except ValueError as e:
        # If it fails, it must be for a downstream reason (e.g. dataset has no
        # period dimension), not the strip itself.
        assert "invalid format" not in str(e)


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_format", ["json", "PARQUET", "table", "PROTOBUF", "", " "])
async def test_get_data_rejects_bad_format(bad_format):
    with pytest.raises(ValueError, match="Unknown format"):
        await server.get_data("CORP_TRANSPARENCY", format=bad_format)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_get_data_filter_with_url_injection_chars(mocked_client):
    """Filter values containing &, ?, /, # should still be safe — they go
    into the DataFrame, not the URL."""
    r = await server.get_data(
        "CORP_TRANSPARENCY",
        filters={"entity_name": "BHP IRON ORE?&=/#"},
    )
    # No match, but no crash.
    assert r.row_count == 0


@pytest.mark.asyncio
async def test_get_data_filter_with_huge_value(mocked_client):
    r = await server.get_data(
        "CORP_TRANSPARENCY",
        filters={"entity_name": "X" * 10000},
    )
    assert r.row_count == 0


@pytest.mark.asyncio
async def test_get_data_filter_with_unicode(mocked_client):
    """Unicode filter values must not crash."""
    r = await server.get_data(
        "CORP_TRANSPARENCY",
        filters={"entity_name": "Bürger King 🍔 株式会社"},
    )
    assert r.row_count == 0


@pytest.mark.asyncio
async def test_get_data_empty_filter_dict_returns_all(mocked_client):
    """{} filters should NOT raise — it means 'no filter applied'."""
    r = await server.get_data("CORP_TRANSPARENCY", filters={}, measures="total_income")
    assert r.row_count > 100  # ~4200 entities have total_income


@pytest.mark.asyncio
async def test_get_data_list_filter_one_match_one_miss(mocked_client):
    r = await server.get_data(
        "CORP_TRANSPARENCY",
        filters={"entity_name": ["1 MENDS STREET PTY LTD", "DOES NOT EXIST"]},
        measures="total_income",
    )
    assert r.row_count == 1  # only one match


@pytest.mark.asyncio
async def test_get_data_periods_equal_allowed(mocked_client):
    # start == end is allowed
    r = await server.get_data(
        "CORP_TRANSPARENCY", start_period="2024", end_period="2024",
    )
    assert isinstance(r.row_count, int)


@pytest.mark.asyncio
async def test_get_data_period_swap_caught():
    with pytest.raises(ValueError, match="before start_period"):
        await server.get_data(
            "CORP_TRANSPARENCY", start_period="2025", end_period="2020",
        )


# ---------------------------------------------------------------------------
# latest
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_latest_unknown_dataset_raises():
    with pytest.raises(ValueError, match="not a curated"):
        await server.latest("DOES_NOT_EXIST")


@pytest.mark.asyncio
async def test_latest_passes_validation_through(mocked_client):
    """latest() shares validation with get_data — confirm it fails the same way."""
    with pytest.raises(ValueError, match="filters must be a dict"):
        await server.latest("CORP_TRANSPARENCY", filters="bad")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# list_curated
# ---------------------------------------------------------------------------

def test_list_curated_idempotent():
    ids1 = server.list_curated()
    ids2 = server.list_curated()
    assert ids1 == ids2
    assert ids1 == sorted(ids1)
