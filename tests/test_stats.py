"""Tests for the `stats` MCP tool.

stats() returns aggregate statistics (count/sum/mean/median/min/max/stddev)
for one measure across all rows matching filters. Collapses the
"fetch-all-then-aggregate-locally" workflow into one server call.
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
    "ts23individual15": FIXTURE_DIR / "ato_occupation.xlsx",
    "ts23individual23": FIXTURE_DIR / "super_contrib_age.xlsx",
    "help-statistics":   FIXTURE_DIR / "help_debt.xlsx",
    "datadotgov_main.csv": FIXTURE_DIR / "acnc_register_head.csv",
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
async def test_stats_basic_envelope(mocked_client):
    r = await server.stats(
        "IND_POSTCODE_MEDIAN", "median_taxable_income_2022_23",
        filters={"state": "nsw"},
    )
    # Required envelope fields
    for field in ("dataset_id", "dataset_name", "measure", "unit",
                  "query", "statistics", "source", "attribution",
                  "ato_url", "server_version"):
        assert field in r, f"missing field: {field}"
    assert r["measure"] == "median_taxable_income_2022_23"
    assert r["unit"] == "AUD"


@pytest.mark.asyncio
async def test_stats_aggregates_nsw_postcodes(mocked_client):
    r = await server.stats(
        "IND_POSTCODE_MEDIAN", "median_taxable_income_2022_23",
        filters={"state": "nsw"},
    )
    st = r["statistics"]
    # ~587 NSW postcodes
    assert 500 < st["count"] < 700
    # Distribution sanity: min ~$10-30k (student-heavy postcodes),
    # max ~$80-100k (elite inner suburbs), mean somewhere in between.
    assert 10_000 < st["min"] < 40_000
    assert 80_000 < st["max"] < 150_000
    assert st["min"] < st["mean"] < st["max"]
    assert st["min"] < st["median"] < st["max"]
    # stddev should be positive
    assert st["stddev"] > 0


@pytest.mark.asyncio
async def test_stats_handles_empty_result_set(mocked_client):
    """No matching rows → count == 0 and no other stats are reported."""
    r = await server.stats(
        "CORP_TRANSPARENCY", "tax_payable",
        filters={"entity_name": "DEFINITELY DOES NOT EXIST INC"},
    )
    assert r["statistics"] == {"count": 0}


@pytest.mark.asyncio
async def test_stats_corporate_sector_aggregates(mocked_client):
    """Headline distribution of $100M+ corporate sector tax payable."""
    r = await server.stats("CORP_TRANSPARENCY", "tax_payable")
    st = r["statistics"]
    # ~3,000 entities had non-null tax payable in 2023-24
    assert 2_500 < st["count"] < 4_500
    # Total tax payable from disclosed corporate sector is ~$96B
    assert 60_000_000_000 < st["sum"] < 150_000_000_000


@pytest.mark.asyncio
async def test_stats_skips_null_values(mocked_client):
    """Entities with blank tax_payable shouldn't drag count/mean down."""
    # 4198 total entities, ~3,000 have tax_payable. The rest are blank.
    r = await server.stats("CORP_TRANSPARENCY", "tax_payable")
    assert r["statistics"]["count"] < 4_198  # nulls excluded


@pytest.mark.asyncio
async def test_stats_rejects_empty_measure():
    with pytest.raises(ValueError, match="measure is required"):
        await server.stats("CORP_TRANSPARENCY", "")


@pytest.mark.asyncio
async def test_stats_rejects_unknown_measure(mocked_client):
    with pytest.raises(ValueError, match="Unknown measure"):
        await server.stats("CORP_TRANSPARENCY", "not_a_measure")


@pytest.mark.asyncio
async def test_stats_unknown_dataset_raises():
    with pytest.raises(ValueError, match="not a curated"):
        await server.stats("DOES_NOT_EXIST", "x")


@pytest.mark.asyncio
async def test_stats_reuses_parsed_df_cache(mocked_client):
    """Two stats calls on the same dataset → 1 parse via the cache."""
    import ato_mcp.server as srv
    original = srv.read_xlsx
    parse_count = {"calls": 0}

    def counted(*args, **kwargs):
        parse_count["calls"] += 1
        return original(*args, **kwargs)

    with patch.object(srv, "read_xlsx", counted):
        await server.stats("CORP_TRANSPARENCY", "tax_payable")
        await server.stats("CORP_TRANSPARENCY", "taxable_income")
        await server.stats("CORP_TRANSPARENCY", "total_income")
    assert parse_count["calls"] == 1


@pytest.mark.asyncio
async def test_stats_for_help_debt_time_series(mocked_client):
    """20 years of HELP debt — min should be old, max current; mean ~$60B."""
    r = await server.stats("HELP_DEBT", "total_debt_aud")
    st = r["statistics"]
    # 20 years in source (2005-06 → 2024-25)
    assert 18 <= st["count"] <= 22
    # Latest year ~$125B, oldest ~$17B
    assert st["max"] > 100_000_000_000
    assert st["min"] < 30_000_000_000


@pytest.mark.asyncio
async def test_stats_for_super_contrib(mocked_client):
    """Stats over super contributions by demographic slice."""
    r = await server.stats(
        "SUPER_CONTRIB_AGE", "employer_contrib_total",
        filters={"sex": "female"},
    )
    st = r["statistics"]
    # ~80 cells (10 age × 8 income brackets)
    assert 50 < st["count"] < 110
    assert st["sum"] > 0
    assert st["mean"] > 0


# ---------------------------------------------------------------------------
# group_by tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_stats_group_by_state_returns_8_buckets(mocked_client):
    r = await server.stats(
        "IND_POSTCODE_MEDIAN", "median_taxable_income_2022_23",
        group_by="state",
    )
    assert r["by"] == "state"
    keys = {g["key"] for g in r["groups"]}
    # All 8 Australian states/territories
    assert {"NSW", "VIC", "QLD", "WA", "SA", "TAS", "NT", "ACT"} == keys


@pytest.mark.asyncio
async def test_stats_group_by_state_real_dispersion(mocked_client):
    """ACT (Canberra) has the highest mean postcode median income; TAS the lowest."""
    r = await server.stats(
        "IND_POSTCODE_MEDIAN", "median_taxable_income_2022_23",
        group_by="state",
    )
    by_state = {g["key"]: g["statistics"]["mean"] for g in r["groups"]}
    assert by_state["ACT"] > by_state["NSW"]
    assert by_state["ACT"] > by_state["TAS"]
    # Coarse plausibility floor
    assert all(40_000 < v < 100_000 for v in by_state.values())


@pytest.mark.asyncio
async def test_stats_group_by_sex_shows_gender_pay_gap(mocked_client):
    """Male occupation median income > female occupation median (real, persistent gap)."""
    r = await server.stats(
        "ATO_OCCUPATION", "median_taxable_income",
        group_by="sex",
    )
    by_sex = {g["key"]: g["statistics"]["median"] for g in r["groups"]}
    assert by_sex["Male"] > by_sex["Female"]
    # Gap should be meaningful (>5%) but not absurd (<50%)
    gap = (by_sex["Male"] - by_sex["Female"]) / by_sex["Female"]
    assert 0.05 < gap < 0.5


@pytest.mark.asyncio
async def test_stats_group_by_industry_corporate(mocked_client):
    """Group corp tax_payable by income_year — one bucket per year present."""
    r = await server.stats(
        "CORP_TRANSPARENCY", "tax_payable",
        group_by="income_year",
    )
    assert r["by"] == "income_year"
    # The 2023-24 file should have at least one income year bucket
    assert len(r["groups"]) >= 1
    for g in r["groups"]:
        assert g["statistics"]["count"] > 0


@pytest.mark.asyncio
async def test_stats_group_by_unknown_dim_raises(mocked_client):
    with pytest.raises(ValueError, match="Unknown group_by"):
        await server.stats(
            "CORP_TRANSPARENCY", "tax_payable",
            group_by="alien_dim",
        )


@pytest.mark.asyncio
async def test_stats_group_by_combined_with_filters(mocked_client):
    """Filter on sex=female THEN group by something else — both apply."""
    r = await server.stats(
        "ATO_OCCUPATION", "median_taxable_income",
        filters={"sex": "female"},
        group_by="occupation_group",
    )
    # Every group should now only contain Female rows; each row had a
    # single value (one occupation × Female = one cell), so each group's
    # count is at most a small handful.
    assert r["by"] == "occupation_group"
    assert len(r["groups"]) > 100  # hundreds of distinct occupation groups
    for g in r["groups"][:10]:
        # All groups should have count >= 1
        assert g["statistics"]["count"] >= 1


@pytest.mark.asyncio
async def test_stats_group_by_empty_string_raises():
    with pytest.raises(ValueError, match="group_by must be"):
        await server.stats("CORP_TRANSPARENCY", "tax_payable", group_by="")


@pytest.mark.asyncio
async def test_stats_group_by_caps_at_200_groups(mocked_client):
    """If a group_by would produce more than 200 buckets, truncate cleanly."""
    # postcode in IND_POSTCODE_MEDIAN gives ~2,300 unique groups
    r = await server.stats(
        "IND_POSTCODE_MEDIAN", "median_taxable_income_2022_23",
        group_by="postcode",
    )
    assert len(r["groups"]) == 200
    assert r.get("groups_truncated") is True
    assert r.get("groups_truncated_at") == 200
