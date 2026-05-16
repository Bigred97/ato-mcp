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


@pytest.mark.asyncio
async def test_live_foreign_ownership_ag_land_range_check():
    """Live fetch the AG land foreign-ownership XLSX and sanity-check the
    units + ranges of every measure in the curated YAML.

    Most importantly: verify the `foreign_held_million_ha` column is in
    consistent units with `total_aust_ag_land_million_ha` (both in million
    hectares), so the customer-computed ratio matches `foreign_ownership_pct`.
    Pre-fix this ratio was off by ~1,000,000× because one column was raw Ha
    and the other was million Ha.
    """
    curated.reset_registry()
    try:
        r = await server._get_data_impl(
            "FOREIGN_OWNERSHIP_AG_LAND",
            filters=None,
            measures=None,
            start_period=None,
            end_period=None,
            fmt="records",
        )
        assert r.row_count > 0, "Expected at least one row from AG land register"

        # Group records by year so we can cross-check measures on the same row.
        by_year: dict[str, dict[str, float]] = {}
        for rec in r.records:
            year = rec.dimensions.get("year")
            if year is None:
                continue
            by_year.setdefault(year, {})[rec.measure] = rec.value

        assert by_year, "Expected at least one year with measures populated"

        # Sanity-check every year we got back.
        for year, measures in by_year.items():
            total_mh = measures.get("total_aust_ag_land_million_ha")
            foreign_mh = measures.get("foreign_held_million_ha")
            pct = measures.get("foreign_ownership_pct")

            # Total AU ag land is ~390M ha — should land 350-450 million ha.
            if total_mh is not None:
                assert 350 <= total_mh <= 450, (
                    f"{year}: total_aust_ag_land_million_ha={total_mh} outside "
                    "expected 350-450M ha range"
                )

            # Foreign-held area must be in MILLION hectares (units fix). Raw
            # hectares would be 5e7+; million-Ha values land in 30-80.
            if foreign_mh is not None:
                assert 10 <= foreign_mh <= 100, (
                    f"{year}: foreign_held_million_ha={foreign_mh} not in expected "
                    "10-100M ha range — unit normalisation may have regressed"
                )

            # Foreign-ownership pct should be 5-25%.
            if pct is not None:
                assert 0 <= pct <= 50, (
                    f"{year}: foreign_ownership_pct={pct} outside plausible 0-50% range"
                )

            # The big invariant: million_ha / million_ha ≈ pct/100.
            if total_mh is not None and foreign_mh is not None and pct is not None:
                computed = (foreign_mh / total_mh) * 100
                assert abs(computed - pct) < 1.5, (
                    f"{year}: (foreign_held_million_ha / total_aust_ag_land_million_ha) "
                    f"* 100 = {computed:.2f} but foreign_ownership_pct = {pct:.2f}; "
                    "units across the two area columns are inconsistent"
                )
    finally:
        await server.reset_client_for_tests()


@pytest.mark.asyncio
async def test_live_foreign_ownership_residential_by_country_range_check():
    """Live fetch the residential foreign-ownership Table 9 XLSX and verify
    country counts land in plausible ranges.

    Transposed layout — each row is a country, each period a column. China
    is consistently the highest-count country in this register.
    """
    curated.reset_registry()
    try:
        r = await server._get_data_impl(
            "FOREIGN_OWNERSHIP_RESIDENTIAL_BY_COUNTRY",
            filters=None,
            measures=None,
            start_period=None,
            end_period=None,
            fmt="records",
        )
        assert r.row_count > 0, "Expected at least one row from residential register"

        sample = r.records[0]
        # Every observation must carry a country in its dimensions.
        assert "metric_source_label" in sample.dimensions or sample.measure, (
            "Expected each observation to carry a country label"
        )
        # Period must be present (transposed layout pivots periods into rows).
        assert sample.period is not None, "Expected period on transposed-layout record"

        # Aggregate per country across periods so we can sanity-check.
        by_country: dict[str, list[float]] = {}
        for rec in r.records:
            country = rec.dimensions.get("metric_source_label") or rec.measure
            if country is None:
                continue
            by_country.setdefault(country, []).append(rec.value)

        # China is always the largest by registered-interest count.
        china_key = next(
            (k for k in by_country if "China" in k or "PRC" in k),
            None,
        )
        assert china_key is not None, (
            f"Expected a China-labelled row in the register; got: {list(by_country)[:10]}"
        )
        china_max = max(by_country[china_key])
        assert china_max > 10_000, (
            f"China max registered interests = {china_max}; expected >10,000 in any period"
        )

        # No country should exceed a sanity ceiling (top is China ~22k).
        for country, values in by_country.items():
            for v in values:
                assert 0 <= v <= 100_000, (
                    f"{country}: value {v} outside plausible 0-100,000 count range"
                )
    finally:
        await server.reset_client_for_tests()
