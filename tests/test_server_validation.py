"""Server-level validation guards on each MCP tool.

Mirrors abs-mcp / rba-mcp `test_server_validation` — confirms each tool
rejects nonsense input cleanly (with a ValueError carrying a 'Try X' hint)
rather than crashing partway through with an obscure error.
"""
from __future__ import annotations

import pytest

from ato_mcp import server


@pytest.mark.asyncio
async def test_search_datasets_empty_query():
    with pytest.raises(ValueError, match="query is required"):
        await server.search_datasets("")


@pytest.mark.asyncio
async def test_search_datasets_whitespace_query():
    with pytest.raises(ValueError, match="query is required"):
        await server.search_datasets("   ")


@pytest.mark.asyncio
async def test_search_datasets_non_string_query():
    with pytest.raises(ValueError, match="must be a string"):
        await server.search_datasets(123)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_search_datasets_limit_too_small():
    with pytest.raises(ValueError, match=">= 1"):
        await server.search_datasets("postcode", limit=0)


@pytest.mark.asyncio
async def test_search_datasets_limit_is_bool():
    with pytest.raises(ValueError, match="positive integer"):
        await server.search_datasets("postcode", limit=True)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_describe_dataset_unknown_id():
    with pytest.raises(ValueError, match="not a curated"):
        await server.describe_dataset("DOES_NOT_EXIST")


@pytest.mark.asyncio
async def test_describe_dataset_bad_chars():
    with pytest.raises(ValueError, match="invalid characters"):
        await server.describe_dataset("../etc/passwd")


@pytest.mark.asyncio
async def test_describe_dataset_empty_id():
    with pytest.raises(ValueError, match="empty"):
        await server.describe_dataset("")


@pytest.mark.asyncio
async def test_get_data_unknown_id():
    with pytest.raises(ValueError, match="not a curated"):
        await server.get_data("DOES_NOT_EXIST")


@pytest.mark.asyncio
async def test_get_data_filters_must_be_dict():
    with pytest.raises(ValueError, match="filters must be a dict"):
        await server.get_data(
            "CORP_TRANSPARENCY", filters=["state", "nsw"],  # type: ignore[arg-type]
        )


@pytest.mark.asyncio
async def test_get_data_bad_period_format():
    with pytest.raises(ValueError, match="invalid format"):
        await server.get_data("CORP_TRANSPARENCY", start_period="?garbage?")


@pytest.mark.asyncio
async def test_get_data_period_swap():
    with pytest.raises(ValueError, match="before start_period"):
        await server.get_data(
            "CORP_TRANSPARENCY", start_period="2024", end_period="2020",
        )


@pytest.mark.asyncio
async def test_get_data_empty_measures_list():
    with pytest.raises(ValueError, match="empty list"):
        await server.get_data("CORP_TRANSPARENCY", measures=[])


@pytest.mark.asyncio
async def test_get_data_bad_format():
    with pytest.raises(ValueError, match="Unknown format"):
        await server.get_data("CORP_TRANSPARENCY", format="parquet")  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_unknown_dataset_id_suggests_close_match():
    """Quality dim #5: unknown dataset IDs should hint at the closest curated ID
    via difflib's get_close_matches. A typo of CORP_TRANSPARENCY should surface
    that name back to the agent so it can self-correct without a separate
    list_curated() round-trip."""
    with pytest.raises(ValueError) as exc_info:
        await server.describe_dataset("CORP_TRANSPRENCY")  # missing the 'A'
    msg = str(exc_info.value)
    assert "Did you mean" in msg
    assert "CORP_TRANSPARENCY" in msg
    # Same shape on get_data
    with pytest.raises(ValueError) as exc_info2:
        await server.get_data("CORP_TRANSPRENCY")
    assert "Did you mean 'CORP_TRANSPARENCY'" in str(exc_info2.value)


@pytest.mark.asyncio
async def test_unknown_format_suggests_close_match():
    """Quality dim #5: an unknown format like 'recrods' (typo of 'records')
    should suggest the closest valid format and also list all options."""
    with pytest.raises(ValueError) as exc_info:
        await server.get_data("CORP_TRANSPARENCY", format="recrods")  # type: ignore[arg-type]
    msg = str(exc_info.value)
    assert "Did you mean 'records'" in msg
    # And still lists valid options
    assert "records" in msg and "series" in msg and "csv" in msg


@pytest.mark.asyncio
async def test_list_curated_returns_sorted_ids():
    ids = server.list_curated()
    assert ids == sorted(ids)
    assert "CORP_TRANSPARENCY" in ids


# --- Int-year coercion (Wave 1 interop fix) ----------------------------------

def test_validate_period_accepts_int_year():
    """Bare int years are coerced to 'YYYY' string at the boundary."""
    assert server._validate_period(2024, "start_period") == "2024"
    assert server._validate_period(2020, "end_period") == "2020"
    assert server._validate_period(1907, "start_period") == "1907"
    assert server._validate_period(2100, "end_period") == "2100"


def test_validate_period_int_out_of_range_raises_helpful():
    """Out-of-range int years raise with a useful hint, not a TypeError."""
    with pytest.raises(ValueError, match="out of range"):
        server._validate_period(1800, "start_period")
    with pytest.raises(ValueError, match="out of range"):
        server._validate_period(2200, "end_period")
    with pytest.raises(ValueError, match="YYYY"):
        server._validate_period(99, "start_period")


def test_validate_period_rejects_bool_with_hint():
    """bool is a subclass of int but must NOT be coerced silently."""
    with pytest.raises(ValueError, match="bool"):
        server._validate_period(True, "start_period")
