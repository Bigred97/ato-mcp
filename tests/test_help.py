"""Tests for the HELP_DEBT curated dataset.

HECS / HELP is universally relatable. Sellable angles: edtech, fintech,
budget/savings planners, public-policy research.
"""
from __future__ import annotations

from ato_mcp import curated, parsing, shaping


def _parse(cd, body):
    df = parsing.read_xlsx(
        body, sheet=cd.sheet, header_row=cd.header_row,
        data_start_row=cd.data_start_row, max_rows=cd.max_rows,
    )
    dim_cols = [c.source_column for c in cd.columns.values() if c.role == "dimension"]
    return parsing.drop_blank_rows(df, dim_cols)


def test_help_yaml_loads():
    cd = curated.get("HELP_DEBT")
    assert cd is not None
    assert cd.layout == "wide"
    assert cd.sheet == "Table 1"
    assert cd.discovery is not None
    measure_keys = {c.key for c in cd.columns.values() if c.role == "measure"}
    assert {
        "total_debt_aud",
        "indexation_aud",
        "compulsory_repayments_aud",
        "voluntary_repayments_aud",
    }.issubset(measure_keys)


def test_help_full_load(help_debt_xlsx):
    cd = curated.get("HELP_DEBT")
    df = _parse(cd, help_debt_xlsx)
    # 2005-06 to 2024-25 = 20 years
    assert 15 <= len(df) <= 25


def test_help_latest_year_total_debt_over_100b(help_debt_xlsx):
    """HECS sector should be > $100B in the latest year."""
    cd = curated.get("HELP_DEBT")
    df = _parse(cd, help_debt_xlsx)
    resp = shaping.build_response(
        cd=cd, df=df, filters={"income_year": "2024 - 25"},
        measures="total_debt_aud",
        start_period=None, end_period=None, fmt="records", user_query={},
    )
    assert resp.row_count == 1
    # $125B in 2024-25
    assert 120_000_000_000 < resp.records[0].value < 200_000_000_000


def test_help_debt_grows_year_over_year(help_debt_xlsx):
    """Total HELP debt should grow monotonically in recent years."""
    cd = curated.get("HELP_DEBT")
    df = _parse(cd, help_debt_xlsx)
    resp = shaping.build_response(
        cd=cd, df=df, filters={}, measures="total_debt_aud",
        start_period=None, end_period=None, fmt="records", user_query={},
    )
    by_year = {r.dimensions["income_year"]: r.value for r in resp.records}
    # Latest > 2020-21 > 2015-16
    assert by_year["2024 - 25"] > by_year["2020 - 21"]
    assert by_year["2020 - 21"] > by_year["2015 - 16"]


def test_help_canary_source_columns_match(help_debt_xlsx):
    cd = curated.get("HELP_DEBT")
    df = parsing.read_xlsx(
        help_debt_xlsx, sheet=cd.sheet, header_row=cd.header_row,
        data_start_row=cd.data_start_row,
    )
    missing = [c.source_column for c in cd.columns.values() if c.source_column not in df.columns]
    assert not missing, f"source columns missing: {missing}"


def test_help_repayments_sum_makes_sense(help_debt_xlsx):
    """Voluntary + compulsory repayments together should be a significant
    fraction of the debt pile but never exceed total debt."""
    cd = curated.get("HELP_DEBT")
    df = _parse(cd, help_debt_xlsx)
    resp = shaping.build_response(
        cd=cd, df=df, filters={"income_year": "2024 - 25"},
        measures=["total_debt_aud", "compulsory_repayments_aud", "voluntary_repayments_aud"],
        start_period=None, end_period=None, fmt="records", user_query={},
    )
    values = {r.measure: r.value for r in resp.records}
    repayments = values["compulsory_repayments_aud"] + values["voluntary_repayments_aud"]
    assert repayments > 30_000_000_000  # > $30B combined
    assert repayments < values["total_debt_aud"]  # never more than the debt pile
