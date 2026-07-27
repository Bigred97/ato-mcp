"""Shaping contract tests against real ATO sample files."""
from __future__ import annotations

import pandas as pd
import pytest

from ato_mcp import curated, parsing, shaping
from ato_mcp.curated import CuratedColumn, CuratedDataset


def _parse(cd, body):
    df = parsing.read_xlsx(
        body, sheet=cd.sheet, header_row=cd.header_row, data_start_row=cd.data_start_row,
    )
    dim_cols = [c.source_column for c in cd.columns.values() if c.role == "dimension"]
    return parsing.drop_blank_rows(df, dim_cols)


def test_corp_transparency_unfiltered_full_load(corp_transparency_xlsx):
    cd = curated.get("CORP_TRANSPARENCY")
    df = _parse(cd, corp_transparency_xlsx)
    resp = shaping.build_response(
        cd=cd, df=df, filters={}, measures=None,
        start_period=None, end_period=None, fmt="records", user_query={},
    )
    # 3 measures × ~4198 entities = ~12k records (some blanks for taxable_income/tax_payable)
    assert resp.row_count > 5000
    assert resp.row_count < 14000
    assert resp.unit == "AUD"
    assert resp.dataset_id == "CORP_TRANSPARENCY"


def test_corp_transparency_filter_entity_name(corp_transparency_xlsx):
    cd = curated.get("CORP_TRANSPARENCY")
    df = _parse(cd, corp_transparency_xlsx)
    resp = shaping.build_response(
        cd=cd, df=df,
        filters={"entity_name": "BHP IRON ORE (JIMBLEBAR) PTY LTD"},
        measures=None,
        start_period=None, end_period=None, fmt="records", user_query={},
    )
    # All 3 measures (total_income, taxable_income, tax_payable) for one entity
    assert resp.row_count == 3
    measures = {r.measure for r in resp.records}
    assert measures == {"total_income", "taxable_income", "tax_payable"}


def test_corp_transparency_abn_is_clean_string(corp_transparency_xlsx):
    cd = curated.get("CORP_TRANSPARENCY")
    df = _parse(cd, corp_transparency_xlsx)
    resp = shaping.build_response(
        cd=cd, df=df,
        filters={"entity_name": "BHP IRON ORE (JIMBLEBAR) PTY LTD"},
        measures="total_income",
        start_period=None, end_period=None, fmt="records", user_query={},
    )
    abn = resp.records[0].dimensions["abn"]
    # Must NOT have trailing '.0' from pandas float coercion
    assert "." not in abn, f"ABN should be a clean int-string, got {abn!r}"
    assert abn.isdigit()


def test_postcode_median_state_filter(ind_postcode_median_xlsx):
    cd = curated.get("IND_POSTCODE_MEDIAN")
    df = _parse(cd, ind_postcode_median_xlsx)
    resp = shaping.build_response(
        cd=cd, df=df,
        filters={"state": "nsw"},
        measures="median_taxable_income_2022_23",
        start_period=None, end_period=None, fmt="records", user_query={},
    )
    # Every NSW postcode → one record
    assert resp.row_count > 400
    assert all(r.dimensions.get("state") == "NSW" for r in resp.records)
    assert all(r.measure == "median_taxable_income_2022_23" for r in resp.records)
    assert all(r.unit == "AUD" for r in resp.records)


def test_postcode_median_postcode_filter(ind_postcode_median_xlsx):
    cd = curated.get("IND_POSTCODE_MEDIAN")
    df = _parse(cd, ind_postcode_median_xlsx)
    resp = shaping.build_response(
        cd=cd, df=df,
        filters={"postcode": "2000"},
        measures=[
            "median_taxable_income_2003_04",
            "median_taxable_income_2013_14",
            "median_taxable_income_2022_23",
        ],
        start_period=None, end_period=None, fmt="records", user_query={},
    )
    # One NSW postcode 2000 row × 3 measures
    assert resp.row_count == 3
    # Income should grow over time
    by_measure = {r.measure: r.value for r in resp.records}
    assert (
        by_measure["median_taxable_income_2003_04"]
        < by_measure["median_taxable_income_2013_14"]
        < by_measure["median_taxable_income_2022_23"]
    )


def test_postcode_median_csv_format(ind_postcode_median_xlsx):
    cd = curated.get("IND_POSTCODE_MEDIAN")
    df = _parse(cd, ind_postcode_median_xlsx)
    resp = shaping.build_response(
        cd=cd, df=df,
        filters={"state": "nsw", "postcode": "2000"},
        measures="median_taxable_income_2022_23",
        start_period=None, end_period=None, fmt="csv", user_query={},
    )
    assert resp.csv is not None
    lines = resp.csv.strip().split("\n")
    # Header + one data row
    assert len(lines) >= 2
    assert "median_taxable_income_2022_23" in resp.csv
    assert "42667" in resp.csv  # known value


def test_postcode_median_series_format(ind_postcode_median_xlsx):
    cd = curated.get("IND_POSTCODE_MEDIAN")
    df = _parse(cd, ind_postcode_median_xlsx)
    resp = shaping.build_response(
        cd=cd, df=df,
        filters={"state": "act", "postcode": "2600"},
        measures=["median_taxable_income_2022_23", "average_taxable_income_2022_23"],
        start_period=None, end_period=None, fmt="series", user_query={},
    )
    # Two series groups, one per measure
    assert len(resp.records) == 2
    measures = {g["measure"] for g in resp.records}
    assert measures == {"median_taxable_income_2022_23", "average_taxable_income_2022_23"}
    # Each group has its own unit + observations list
    for g in resp.records:
        assert g["unit"] == "AUD"
        assert isinstance(g["observations"], list)
        assert len(g["observations"]) == 1


def test_company_industry_filter(company_industry_xlsx):
    cd = curated.get("COMPANY_INDUSTRY")
    df = _parse(cd, company_industry_xlsx)
    resp = shaping.build_response(
        cd=cd, df=df,
        filters={"industry_broad": "A. Agriculture, Forestry and Fishing"},
        measures="total_income",
        start_period=None, end_period=None, fmt="records", user_query={},
    )
    # 15 fine-industry rows under Agriculture, Forestry and Fishing
    assert 10 < resp.row_count < 30
    assert all(r.measure == "total_income" for r in resp.records)
    assert all(
        r.dimensions["industry_broad"] == "A. Agriculture, Forestry and Fishing"
        for r in resp.records
    )


def test_super_contrib_age_sex_filter(super_contrib_age_xlsx):
    cd = curated.get("SUPER_CONTRIB_AGE")
    df = _parse(cd, super_contrib_age_xlsx)
    resp = shaping.build_response(
        cd=cd, df=df,
        filters={"sex": "female"},
        measures=["employer_contrib_total", "personal_contrib_total"],
        start_period=None, end_period=None, fmt="records", user_query={},
    )
    # 10 age brackets × 8 income ranges × 2 measures = up to 160 records (some cells empty)
    assert resp.row_count > 100
    assert all(r.dimensions["sex"] == "Female" for r in resp.records)


def test_acnc_register_state_filter(acnc_register_csv):
    cd = curated.get("ACNC_REGISTER")
    df = parsing.read_csv(acnc_register_csv)
    df = parsing.drop_blank_rows(
        df, [c.source_column for c in cd.columns.values() if c.role == "dimension"],
    )
    resp = shaping.build_response(
        cd=cd, df=df,
        filters={"state": "QLD"},
        measures="responsible_persons_count",
        start_period=None, end_period=None, fmt="records", user_query={},
    )
    assert resp.row_count > 0
    assert all(r.dimensions.get("state") == "QLD" for r in resp.records)


def test_acnc_register_limit_truncates_from_front(acnc_register_csv):
    """Regression: period-less register datasets (ACNC_REGISTER) have no
    chronological "latest" — source order is the meaningful order, so
    `limit` truncation must keep the FRONT of the list, unchanged from
    pre-fix behaviour. This guards against a blanket records[-limit:]
    flip (the tail-slice fix for time-series datasets like GST_MONTHLY)
    silently reversing register truncation too.
    """
    cd = curated.get("ACNC_REGISTER")
    df = parsing.read_csv(acnc_register_csv)
    df = parsing.drop_blank_rows(
        df, [c.source_column for c in cd.columns.values() if c.role == "dimension"],
    )
    resp_full = shaping.build_response(
        cd=cd, df=df, filters={}, measures="responsible_persons_count",
        start_period=None, end_period=None, fmt="records", user_query={},
    )
    resp_capped = shaping.build_response(
        cd=cd, df=df, filters={}, measures="responsible_persons_count",
        start_period=None, end_period=None, fmt="records", user_query={},
        limit=5,
    )
    assert resp_full.row_count > 5, "fixture must have more than 5 rows to exercise truncation"
    assert resp_capped.row_count == 5
    # The emit-time shape_cap short-circuits materialisation at limit+1 for
    # memory safety (see build_response), so truncated_at reports that
    # intermediate count rather than the full untruncated row_count — same
    # convention as the existing shape-cap tests (test_shape_cap.py). What
    # matters here is only that truncation was flagged at all.
    assert resp_capped.truncated_at is not None
    assert resp_capped.truncated_at >= 5


def _mixed_wide_cd() -> CuratedDataset:
    """Synthetic wide-layout dataset with BOTH periodic measures (FY-suffixed
    keys, e.g. `count_2019_20`) and a period-less register-style measure —
    the mixed case `_truncate_records` handles explicitly but that no real
    curated dataset happens to exercise. Lets a single `build_response` call
    (the same entrypoint `server.py`'s `get_data`/`latest` use) produce a
    records list containing both periodic and period-less Observations, so
    truncation direction can be checked for each half simultaneously.
    """
    columns = {
        "entity_id": CuratedColumn(key="entity_id", source_column="Entity ID", role="dimension"),
        "count_2019_20": CuratedColumn(key="count_2019_20", source_column="Count 2019-20", unit="Count"),
        "count_2020_21": CuratedColumn(key="count_2020_21", source_column="Count 2020-21", unit="Count"),
        "count_2021_22": CuratedColumn(key="count_2021_22", source_column="Count 2021-22", unit="Count"),
        "register_field": CuratedColumn(key="register_field", source_column="Register Field", unit="Count"),
    }
    return CuratedDataset(
        id="TEST_MIXED",
        name="Test Mixed Register/Time-series",
        description="Synthetic fixture for truncation-direction regression test.",
        source_url="https://example.test/dataset",
        download_url="https://example.test/dataset.csv",
        format="csv",
        sheet=None,
        header_row=1,
        data_start_row=None,
        max_rows=None,
        layout="wide",
        period_coverage=None,
        update_frequency="annual",
        cache_kind="data",
        columns=columns,
        dimension_values={},
    )


def test_truncate_records_periodic_tail_slice_discriminates_from_prefix_front_slice():
    """Regression: for a purely periodic (time-series) dataset, truncation
    must keep the LATEST periods (tail-slice), via the real
    `shaping.build_response` entrypoint `server.py`'s `get_data`/`latest`
    call — not the internal `_truncate_records` helper directly.

    This scenario is deliberately built to DISCRIMINATE old vs new code.
    `build_response`'s emit-time cap for wide layouts short-circuits
    materialisation at `limit + 1` records (see the `shape_cap` comment in
    `build_response`), so with 4 entities x 3 FY measures the raw
    (pre-truncate) set is: 2019-20 x4, 2020-21 x4, 2021-22 x3 (11 total,
    row 4's 2021-22 cell never gets materialised). Post-hoc truncation to
    limit=10 must then drop exactly 1 record:
      - NEW (tail-slice, `_truncate_records`): drops the FIRST item in the
        ascending-sorted list -> entity E1's 2019-20 observation.
      - OLD (pre-fix `records[:limit]` front-slice on the same ascending
        list): drops the LAST item -> entity E3's 2021-22 observation.
    These are different, concrete, individually-identifiable observations,
    so asserting on them fails against the pre-fix front-slice and passes
    only against the tail-slice fix — unlike the all-register fixture this
    replaces, which could not tell the two implementations apart.
    """
    cd = _mixed_wide_cd()
    df = pd.DataFrame(
        {
            "Entity ID": ["E1", "E2", "E3", "E4"],
            "Count 2019-20": [10, 20, 30, 40],
            "Count 2020-21": [11, 21, 31, 41],
            "Count 2021-22": [12, 22, 32, 42],
            "Register Field": [100, 200, 300, 400],
        }
    )
    measures = ["count_2019_20", "count_2020_21", "count_2021_22"]

    resp = shaping.build_response(
        cd=cd, df=df, filters={}, measures=measures,
        start_period=None, end_period=None, fmt="records", user_query={},
        limit=10,
    )
    assert resp.row_count == 10
    seen = {(r.dimensions.get("entity_id"), r.period) for r in resp.records}

    assert ("E1", "2019-20") not in seen, (
        "tail-slice must drop the EARLIEST surplus record (E1's 2019-20); "
        "its presence means truncation regressed to the pre-fix front-slice"
    )
    assert ("E3", "2021-22") in seen, (
        "tail-slice must keep the LATEST period (2021-22) fully, including "
        "E3's observation; its absence means truncation regressed to the "
        "pre-fix front-slice, which would have dropped it instead"
    )
    # Sanity: everything else in the 4x3 grid (minus the one dropped cell,
    # and row 4's un-materialised 2021-22 cell) is present.
    assert len(seen) == 10


def test_truncate_records_mixed_periodic_and_register_front_slice():
    """Regression: when a response mixes periodic and period-less
    (register-style) Observations and the periodic rows fit comfortably
    within `limit`, the leftover budget for period-less rows must still be
    filled from the FRONT (source order), exercising the
    `kept_periodic + kept_register` branch in `_truncate_records` that a
    100%-periodic or 100%-register fixture never reaches. Uses the same
    real `shaping.build_response` entrypoint as production `get_data`/
    `latest`.
    """
    cd = _mixed_wide_cd()
    df = pd.DataFrame(
        {
            "Entity ID": ["E1", "E2", "E3"],
            "Count 2019-20": [10, 20, 30],
            "Count 2020-21": [11, 21, 31],
            "Count 2021-22": [12, 22, 32],
            "Register Field": [100, 200, 300],
        }
    )
    measures = ["count_2019_20", "register_field"]

    resp = shaping.build_response(
        cd=cd, df=df, filters={}, measures=measures,
        start_period=None, end_period=None, fmt="records", user_query={},
        limit=5,
    )
    assert resp.row_count == 5
    periodic_entities = {
        r.dimensions.get("entity_id") for r in resp.records if r.period is not None
    }
    register_entities = {
        r.dimensions.get("entity_id") for r in resp.records if r.period is None
    }
    assert periodic_entities == {"E1", "E2", "E3"}, (
        "all 3 periodic rows fit within limit=5 and must be kept in full, "
        f"got {periodic_entities}"
    )
    assert register_entities == {"E1", "E2"}, (
        "period-less rows must truncate from the FRONT (source order, "
        f"dropping E3, the last row) — got {register_entities}"
    )


def test_unknown_filter_raises(corp_transparency_xlsx):
    cd = curated.get("CORP_TRANSPARENCY")
    df = _parse(cd, corp_transparency_xlsx)
    with pytest.raises(ValueError, match="Unknown filter"):
        shaping.build_response(
            cd=cd, df=df,
            filters={"not_a_dim": "x"}, measures=None,
            start_period=None, end_period=None, fmt="records", user_query={},
        )


def test_empty_list_filter_raises(corp_transparency_xlsx):
    cd = curated.get("CORP_TRANSPARENCY")
    df = _parse(cd, corp_transparency_xlsx)
    with pytest.raises(ValueError, match="empty list"):
        shaping.build_response(
            cd=cd, df=df,
            filters={"entity_name": []}, measures=None,
            start_period=None, end_period=None, fmt="records", user_query={},
        )


def test_latest_on_wide_dataset_does_not_arbitrarily_trim(ind_postcode_median_xlsx):
    """Regression: latest() (last_n=1) on a WIDE dataset with multiple matching
    rows used to trim per-measure to 1 random row. Now it should preserve
    all rows because there's no time dimension to "be latest" on.

    Audit bug #1: trimming when every record has period=None makes no sense —
    `latest()` on a wide-layout dataset should behave like `get_data()`.
    """
    cd = curated.get("IND_POSTCODE_MEDIAN")
    df = parsing.read_xlsx(
        ind_postcode_median_xlsx, sheet=cd.sheet, header_row=cd.header_row,
    )
    df = parsing.drop_blank_rows(
        df, [c.source_column for c in cd.columns.values() if c.role == "dimension"],
    )
    # Without last_n
    resp_full = shaping.build_response(
        cd=cd, df=df,
        filters={"state": "nsw"},
        measures="median_taxable_income_2022_23",
        start_period=None, end_period=None, fmt="records", user_query={},
        last_n=None,
    )
    # With last_n=1 (what latest() passes)
    resp_latest = shaping.build_response(
        cd=cd, df=df,
        filters={"state": "nsw"},
        measures="median_taxable_income_2022_23",
        start_period=None, end_period=None, fmt="records", user_query={},
        last_n=1,
    )
    # The wide layout has no time dimension — latest must NOT trim
    assert resp_latest.row_count == resp_full.row_count, (
        f"latest trimmed wide-layout records: {resp_latest.row_count} vs "
        f"full {resp_full.row_count}"
    )


def test_latest_on_corp_transparency_keeps_all_measures(corp_transparency_xlsx):
    """latest() filter on entity_name with measures=None should return all 3
    measures (total_income, taxable_income, tax_payable) — not 1 arbitrary one."""
    cd = curated.get("CORP_TRANSPARENCY")
    df = parsing.read_xlsx(
        corp_transparency_xlsx, sheet=cd.sheet, header_row=cd.header_row,
    )
    df = parsing.drop_blank_rows(
        df, [c.source_column for c in cd.columns.values() if c.role == "dimension"],
    )
    resp = shaping.build_response(
        cd=cd, df=df,
        filters={"entity_name": "BHP IRON ORE (JIMBLEBAR) PTY LTD"},
        measures=None,
        start_period=None, end_period=None, fmt="records", user_query={},
        last_n=1,
    )
    measures = {r.measure for r in resp.records}
    assert measures == {"total_income", "taxable_income", "tax_payable"}


def test_response_carries_metadata(corp_transparency_xlsx):
    cd = curated.get("CORP_TRANSPARENCY")
    df = _parse(cd, corp_transparency_xlsx)
    resp = shaping.build_response(
        cd=cd, df=df, filters={}, measures="total_income",
        start_period=None, end_period=None, fmt="records", user_query={"x": 1},
    )
    assert resp.dataset_id == "CORP_TRANSPARENCY"
    assert resp.dataset_name
    assert resp.source == "Australian Taxation Office (ATO) + ACNC, via data.gov.au"
    assert "Creative Commons" in resp.attribution
    assert resp.ato_url == cd.source_url
    assert resp.query == {"x": 1}
    assert resp.server_version


def test_data_response_has_source_url_canonical_field(corp_transparency_xlsx):
    """Wave-2 interop: both source_url and ato_url are populated and equal."""
    cd = curated.get("CORP_TRANSPARENCY")
    df = _parse(cd, corp_transparency_xlsx)
    resp = shaping.build_response(
        cd=cd, df=df, filters={}, measures="total_income",
        start_period=None, end_period=None, fmt="records", user_query={},
    )
    assert resp.source_url is not None
    assert resp.source_url == resp.ato_url
    assert resp.source_url == cd.source_url


def test_data_response_source_url_present_on_csv_format(corp_transparency_xlsx):
    """source_url is populated regardless of output format."""
    cd = curated.get("CORP_TRANSPARENCY")
    df = _parse(cd, corp_transparency_xlsx)
    resp = shaping.build_response(
        cd=cd, df=df, filters={}, measures="total_income",
        start_period=None, end_period=None, fmt="csv", user_query={},
    )
    assert resp.source_url == resp.ato_url
    assert resp.source_url.startswith("https://")
