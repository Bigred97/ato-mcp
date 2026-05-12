"""Realistic multi-step customer flows hitting live data.gov.au.

These act as the customer would: start with a vague question, discover the
right dataset, describe it, query it, and confirm the result is meaningful.
Each test is one full Claude-style "agent journey".

Live by default — tagged with `live` so they skip in CI default. Run with:
    pytest -m live
"""
from __future__ import annotations

import pytest

from ato_mcp import curated, server

pytestmark = pytest.mark.live


@pytest.fixture(autouse=True)
async def reset_client():
    await server.reset_client_for_tests()
    yield
    await server.reset_client_for_tests()


@pytest.mark.asyncio
async def test_flow_property_tech_postcode_median_over_time():
    """Customer: 'What's the median income trajectory in postcode 2000?'

    Agent journey:
      1. search_datasets("median income postcode")
      2. describe_dataset → confirm measures include yearly medians
      3. get_data with state+postcode filter + 3 yearly measures
    """
    # Step 1
    results = await server.search_datasets("median income postcode")
    assert any(s.id == "IND_POSTCODE_MEDIAN" for s in results), (
        f"expected IND_POSTCODE_MEDIAN in top results, got {[s.id for s in results]}"
    )

    # Step 2
    detail = await server.describe_dataset("IND_POSTCODE_MEDIAN")
    measure_keys = {m.key for m in detail.measures}
    for required in (
        "median_taxable_income_2003_04",
        "median_taxable_income_2013_14",
        "median_taxable_income_2022_23",
    ):
        assert required in measure_keys, f"missing measure {required}"

    # Step 3
    data = await server.get_data(
        "IND_POSTCODE_MEDIAN",
        filters={"state": "nsw", "postcode": "2000"},
        measures=[
            "median_taxable_income_2003_04",
            "median_taxable_income_2013_14",
            "median_taxable_income_2022_23",
        ],
    )
    assert data.row_count == 3
    by_measure = {r.measure: r.value for r in data.records}
    # Real-world plausibility: 2003 < 2013 < 2022 income (nominal AUD)
    assert by_measure["median_taxable_income_2003_04"] > 5_000
    assert by_measure["median_taxable_income_2003_04"] < by_measure["median_taxable_income_2013_14"]
    assert by_measure["median_taxable_income_2013_14"] < by_measure["median_taxable_income_2022_23"]
    # Every record should carry the attribution
    assert "Creative Commons" in data.attribution
    assert data.ato_url.startswith("https://data.gov.au/")


@pytest.mark.asyncio
async def test_flow_corporate_tax_lookup():
    """Customer: 'How much tax did BHP Iron Ore (Jimblebar) pay?'

    Agent journey:
      1. search_datasets("corporate tax bhp")
      2. describe_dataset → find dimensions
      3. get_data with entity filter
    """
    results = await server.search_datasets("corporate tax transparency")
    assert any(s.id == "CORP_TRANSPARENCY" for s in results)

    detail = await server.describe_dataset("CORP_TRANSPARENCY")
    assert {"entity_name", "income_year"}.issubset({d.key for d in detail.dimensions})

    data = await server.get_data(
        "CORP_TRANSPARENCY",
        filters={"entity_name": "BHP IRON ORE (JIMBLEBAR) PTY LTD"},
    )
    assert data.row_count == 3  # total_income, taxable_income, tax_payable
    by_measure = {r.measure: r.value for r in data.records}
    assert by_measure["total_income"] > 1_000_000_000  # BHP is big
    assert by_measure["taxable_income"] > 1_000_000_000
    assert by_measure["tax_payable"] > 100_000_000


@pytest.mark.asyncio
async def test_flow_compare_multiple_postcodes():
    """Customer: 'Compare 2022-23 median income across postcodes 2000, 2008, 2026, 2031.'

    One multi-value filter call returning 4 rows.
    """
    data = await server.get_data(
        "IND_POSTCODE_MEDIAN",
        filters={"state": "nsw", "postcode": ["2000", "2008", "2026", "2031"]},
        measures="median_taxable_income_2022_23",
    )
    assert data.row_count == 4
    postcodes_returned = {r.dimensions["postcode"] for r in data.records}
    assert postcodes_returned == {"2000", "2008", "2026", "2031"}
    # All values should be plausible incomes
    for r in data.records:
        assert 10_000 < r.value < 1_000_000, (
            f"postcode {r.dimensions['postcode']} value {r.value} out of plausible range"
        )


@pytest.mark.asyncio
async def test_flow_csv_for_spreadsheet_export():
    """Customer: 'Give me NSW postcode medians as CSV so I can paste into Excel.'"""
    data = await server.get_data(
        "IND_POSTCODE_MEDIAN",
        filters={"state": "nsw"},
        measures="median_taxable_income_2022_23",
        format="csv",
    )
    assert data.csv is not None
    lines = data.csv.strip().split("\n")
    # Header + data rows
    assert len(lines) > 100
    # CSV is long-format: measure name appears in the `measure` column, not the header
    assert lines[0].startswith("period,measure,value,unit")
    assert "median_taxable_income_2022_23" in data.csv


@pytest.mark.asyncio
async def test_flow_series_format_for_charting():
    """Customer: 'I want to chart median vs average income for postcode 2026.'"""
    data = await server.get_data(
        "IND_POSTCODE_MEDIAN",
        filters={"state": "nsw", "postcode": "2026"},
        measures=["median_taxable_income_2022_23", "average_taxable_income_2022_23"],
        format="series",
    )
    assert len(data.records) == 2  # two series groups
    measures_in_groups = {g["measure"] for g in data.records}
    assert measures_in_groups == {
        "median_taxable_income_2022_23",
        "average_taxable_income_2022_23",
    }


@pytest.mark.asyncio
async def test_flow_charity_filter_nsw_large():
    """Customer: 'Find every Large charity in NSW.'"""
    data = await server.get_data(
        "ACNC_REGISTER",
        filters={"state": "NSW", "charity_size": "Large"},
        measures="responsible_persons_count",
    )
    assert data.row_count > 50, "expected hundreds of Large NSW charities"
    # Every returned record must satisfy both filters
    for r in data.records:
        assert r.dimensions.get("state") == "NSW"
        assert r.dimensions.get("charity_size") == "Large"


@pytest.mark.asyncio
async def test_flow_unhappy_path_helpful_error():
    """Customer typos a state. Error must guide them to the right answer."""
    with pytest.raises(ValueError, match="Try one of") as exc_info:
        await server.get_data(
            "IND_POSTCODE_MEDIAN", filters={"state": "narnia"},
        )
    assert "nsw" in str(exc_info.value) and "vic" in str(exc_info.value)


@pytest.mark.asyncio
async def test_flow_response_envelope_invariants():
    """Every response carries the metadata an agent needs to cite the source."""
    data = await server.get_data(
        "CORP_TRANSPARENCY",
        filters={"entity_name": "BHP IRON ORE (JIMBLEBAR) PTY LTD"},
    )
    assert data.dataset_id
    assert data.dataset_name
    assert data.source == "Australian Taxation Office"
    assert data.attribution
    assert data.retrieved_at  # datetime
    assert data.ato_url.startswith("https://data.gov.au/")
    assert data.server_version


@pytest.mark.asyncio
async def test_flow_all_curated_datasets_return_data():
    """Sanity: every curated dataset returns SOME data when queried with no
    filters and a single measure. This catches anyone shipping a YAML with
    a broken column map or a stale URL."""
    for dataset_id in curated.list_ids():
        cd = curated.get(dataset_id)
        # Wide layouts: first role=measure column. Transposed layouts: first
        # alias declared in the metric_label column's dimension_values.
        wide_measures = [c.key for c in cd.columns.values() if c.role == "measure"]
        if wide_measures:
            first_measure = wide_measures[0]
        else:
            first_measure = curated.transposed_measure_aliases(cd)[0]
        data = await server.get_data(dataset_id, measures=first_measure)
        assert data.row_count > 0, (
            f"{dataset_id} returned no rows for measure {first_measure!r}"
        )


@pytest.mark.asyncio
async def test_flow_discovery_resolves_real_ckan_url():
    """End-to-end auto-update test: the discovery layer must resolve real
    data.gov.au URLs against live CKAN.

    This is the canary that detects when ATO renames a dataset or resource —
    the kind of breakage that would otherwise only surface when a user runs
    a real query and finds stale data.

    One retry is allowed: data.gov.au occasionally returns transient errors
    under load. A second failure indicates a real regression.
    """
    import asyncio as _asyncio

    from ato_mcp.client import ATOClient
    from ato_mcp.discovery import DiscoveryError, DiscoverySpec, resolve_latest_url

    async def _resolve_with_retry(client, spec, retries: int = 1):
        last_error: Exception | None = None
        for attempt in range(retries + 1):
            try:
                return await resolve_latest_url(client, spec)
            except DiscoveryError as e:
                last_error = e
                if attempt < retries:
                    await _asyncio.sleep(0.5)  # brief backoff for transient blip
        raise last_error  # type: ignore[misc]

    async with ATOClient() as client:
        # 1. Resource-in-fixed-package discovery (CORP_TRANSPARENCY pattern)
        url = await _resolve_with_retry(
            client,
            DiscoverySpec(
                package_id="corporate-transparency",
                resource_name_pattern=r"^\d{4}-\d{2} Report of Entity Tax Information$",
            ),
        )
        assert url.startswith("https://data.gov.au/")
        assert "corporate-transparency" in url or "report-of-entity-tax-information" in url.lower()

        # 2. Latest-package discovery (IND_POSTCODE pattern)
        url = await _resolve_with_retry(
            client,
            DiscoverySpec(
                organization_id="australiantaxationoffice",
                package_id_pattern=r"^taxation-statistics-(\d{4})-\d{2}$",
                resource_name="Individuals - Table 6",
            ),
        )
        assert url.startswith("https://data.gov.au/")
        assert "individual06" in url.lower() or "individual-06" in url.lower()
