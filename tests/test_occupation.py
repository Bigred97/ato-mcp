"""Tests for the ATO_OCCUPATION curated dataset.

This is the 8th curated dataset (and 4th XLSX wide-layout). Covers the
"which jobs pay the most" angle which is high-traffic for HR-tech and
career-planning agents.
"""
from __future__ import annotations

import pytest

from ato_mcp import curated, parsing, shaping


def _parse(cd, body):
    df = parsing.read_xlsx(
        body, sheet=cd.sheet, header_row=cd.header_row,
        data_start_row=cd.data_start_row,
    )
    dim_cols = [c.source_column for c in cd.columns.values() if c.role == "dimension"]
    return parsing.drop_blank_rows(df, dim_cols)


def test_yaml_loads_and_has_expected_shape():
    cd = curated.get("ATO_OCCUPATION")
    assert cd is not None
    assert cd.layout == "wide"
    assert cd.sheet == "Table 15A"
    assert cd.header_row == 2
    assert cd.discovery is not None  # auto-discovery is wired
    # Expected measures
    measure_keys = {c.key for c in cd.columns.values() if c.role == "measure"}
    assert {
        "individuals_count",
        "average_taxable_income",
        "median_taxable_income",
        "average_salary_wage",
        "median_salary_wage",
        "average_total_income",
        "median_total_income",
    }.issubset(measure_keys)


def test_full_load_row_count(ato_occupation_xlsx):
    cd = curated.get("ATO_OCCUPATION")
    df = _parse(cd, ato_occupation_xlsx)
    # ~3500 rows: ~1200 occupations × 3 sex categories (Female/Male/Total)
    assert 3000 < len(df) < 4000


def test_sex_filter(ato_occupation_xlsx):
    cd = curated.get("ATO_OCCUPATION")
    df = _parse(cd, ato_occupation_xlsx)
    resp = shaping.build_response(
        cd=cd, df=df, filters={"sex": "female"},
        measures="median_taxable_income",
        start_period=None, end_period=None, fmt="records", user_query={},
    )
    assert resp.row_count > 500  # ~one third of rows
    assert all(r.dimensions["sex"] == "Female" for r in resp.records)


def test_top_n_highest_median_taxable_income(ato_occupation_xlsx):
    """The top earners should include surgeons / cardiologists / specialists —
    classic medical-profession sanity check."""
    cd = curated.get("ATO_OCCUPATION")
    df = _parse(cd, ato_occupation_xlsx)
    resp = shaping.build_response(
        cd=cd, df=df, filters={"sex": "total"},
        measures="median_taxable_income",
        start_period=None, end_period=None, fmt="records", user_query={},
    )
    # Sort by value and look at the top 10
    sorted_records = sorted(
        [r for r in resp.records if r.value is not None],
        key=lambda r: r.value, reverse=True,
    )
    top_10_jobs = [r.dimensions["occupation"].lower() for r in sorted_records[:10]]
    # At least one medical specialist in the top 10
    assert any(
        kw in occ
        for occ in top_10_jobs
        for kw in ("surgeon", "cardiologist", "anaesthetist", "neurosurgeon", "specialist")
    ), f"top 10 didn't include any medical specialist: {top_10_jobs}"
    # Top median should be > $300k (highest-paid medical roles)
    assert sorted_records[0].value > 300_000


def test_ceo_real_value_sanity(ato_occupation_xlsx):
    """CEO median income should be plausible — within an expected range."""
    cd = curated.get("ATO_OCCUPATION")
    df = _parse(cd, ato_occupation_xlsx)
    resp = shaping.build_response(
        cd=cd, df=df,
        filters={"occupation": "111111 Chief executive officer", "sex": "total"},
        measures="median_taxable_income",
        start_period=None, end_period=None, fmt="records", user_query={},
    )
    assert resp.row_count == 1
    value = resp.records[0].value
    # Median CEO taxable income in AU 2022-23 is in the ~$60k-$120k range
    # (driven down by part-time and stay-at-home CEOs of family companies).
    assert 30_000 < value < 200_000


def test_unknown_sex_alias_raises(ato_occupation_xlsx):
    cd = curated.get("ATO_OCCUPATION")
    df = _parse(cd, ato_occupation_xlsx)
    with pytest.raises(ValueError, match="Unknown value"):
        shaping.build_response(
            cd=cd, df=df, filters={"sex": "unisex"},
            measures=None, start_period=None, end_period=None,
            fmt="records", user_query={},
        )


def test_canary_source_columns_match(ato_occupation_xlsx):
    """Every curated source_column must appear in the parsed file — guards
    against ATO changing column-name spelling in a future release."""
    cd = curated.get("ATO_OCCUPATION")
    df = parsing.read_xlsx(ato_occupation_xlsx, sheet=cd.sheet, header_row=cd.header_row)
    missing = [c.source_column for c in cd.columns.values() if c.source_column not in df.columns]
    assert not missing, f"source columns missing: {missing}"
