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
async def test_list_curated_returns_sorted_ids():
    ids = server.list_curated()
    assert ids == sorted(ids)
    assert "CORP_TRANSPARENCY" in ids
