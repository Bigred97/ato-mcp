"""Shaping contract tests against real ATO sample files."""
from __future__ import annotations

import pytest

from ato_mcp import curated, parsing, shaping


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
    assert resp.source == "Australian Taxation Office"
    assert "Creative Commons" in resp.attribution
    assert resp.ato_url == cd.source_url
    assert resp.query == {"x": 1}
    assert resp.server_version
