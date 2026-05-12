"""End-to-end tests that hit data.gov.au.

Tagged `live` so they don't run by default. Run with:
    pytest -m live
"""
from __future__ import annotations

import pytest

from ato_mcp import curated, server

pytestmark = pytest.mark.live


@pytest.mark.asyncio
async def test_live_corp_transparency_search():
    curated.reset_registry()
    results = await server.search_datasets("corporate tax transparency")
    assert any(s.id == "CORP_TRANSPARENCY" for s in results), (
        f"CORP_TRANSPARENCY missing from results: {[s.id for s in results]}"
    )


@pytest.mark.asyncio
async def test_live_corp_transparency_get_data():
    curated.reset_registry()
    try:
        r = await server.get_data(
            "CORP_TRANSPARENCY",
            filters={"entity_name": "BHP IRON ORE (JIMBLEBAR) PTY LTD"},
        )
        assert r.row_count >= 1
        assert r.unit == "AUD"
        assert r.ato_url.startswith("https://data.gov.au/")
    finally:
        await server.reset_client_for_tests()


@pytest.mark.asyncio
async def test_live_ind_postcode_median_get_data():
    curated.reset_registry()
    try:
        r = await server.get_data(
            "IND_POSTCODE_MEDIAN",
            filters={"state": "nsw", "postcode": "2000"},
            measures="median_taxable_income_2022_23",
        )
        assert r.row_count == 1
        assert r.records[0].dimensions["postcode"] == "2000"
        assert r.records[0].dimensions["state"] == "NSW"
        assert r.records[0].value > 0
    finally:
        await server.reset_client_for_tests()
