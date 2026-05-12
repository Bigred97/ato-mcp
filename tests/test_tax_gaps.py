"""Tests for the TAX_GAPS curated dataset.

The tax gap is ATO's estimate of how much tax is being missed each year.
Sellable: public-policy researchers, journalism, fintech / compliance.
"""
from __future__ import annotations

import pytest

from ato_mcp import curated, parsing, shaping


def _parse(cd, body):
    df = parsing.read_xlsx(
        body, sheet=cd.sheet, header_row=cd.header_row,
        data_start_row=cd.data_start_row, max_rows=cd.max_rows,
    )
    dim_cols = [c.source_column for c in cd.columns.values() if c.role == "dimension"]
    return parsing.drop_blank_rows(df, dim_cols)


def test_tax_gaps_yaml_loads():
    cd = curated.get("TAX_GAPS")
    assert cd is not None
    assert cd.layout == "wide"
    assert cd.discovery is not None
    measure_keys = {c.key for c in cd.columns.values() if c.role == "measure"}
    assert {
        "tax_expected_millions", "gross_gap_millions",
        "net_gap_millions", "net_gap_rate",
    }.issubset(measure_keys)


def test_tax_gaps_full_load(tax_gaps_xlsx):
    cd = curated.get("TAX_GAPS")
    df = _parse(cd, tax_gaps_xlsx)
    # 4 tax types × ~7 years = ~28 rows
    assert 20 < len(df) < 50


def test_tax_gaps_personal_income_largest(tax_gaps_xlsx):
    """Personal income tax gap should be the largest by dollar amount."""
    cd = curated.get("TAX_GAPS")
    df = _parse(cd, tax_gaps_xlsx)
    resp = shaping.build_response(
        cd=cd, df=df, filters={"financial_year": "2022-23"},
        measures="net_gap_millions",
        start_period=None, end_period=None, fmt="records", user_query={},
    )
    by_type = {r.dimensions["tax_type"]: r.value for r in resp.records}
    # Personal income should be biggest
    assert by_type["Personal income tax"] > by_type["Corporate income tax"]
    assert by_type["Personal income tax"] > by_type["Goods and services tax"]
    # Personal gap is in tens of billions ($m units → > 20000)
    assert by_type["Personal income tax"] > 20_000


def test_tax_gaps_personal_gap_growing(tax_gaps_xlsx):
    """Personal income tax gap has been growing over time."""
    cd = curated.get("TAX_GAPS")
    df = _parse(cd, tax_gaps_xlsx)
    resp = shaping.build_response(
        cd=cd, df=df, filters={"tax_type": "personal"},
        measures="net_gap_millions",
        start_period=None, end_period=None, fmt="records", user_query={},
    )
    by_year = {r.dimensions["financial_year"]: r.value for r in resp.records}
    # Latest > earliest
    assert by_year["2022-23"] > by_year["2016-17"]


def test_tax_gaps_rate_under_15_percent(tax_gaps_xlsx):
    """Sanity: net gap rate should never exceed 15% for any tax type / year."""
    cd = curated.get("TAX_GAPS")
    df = _parse(cd, tax_gaps_xlsx)
    resp = shaping.build_response(
        cd=cd, df=df, filters={}, measures="net_gap_rate",
        start_period=None, end_period=None, fmt="records", user_query={},
    )
    for r in resp.records:
        assert 0 < r.value < 0.15, (
            f"net_gap_rate out of range: {r.dimensions} = {r.value}"
        )


def test_tax_gaps_canary_source_columns_match(tax_gaps_xlsx):
    cd = curated.get("TAX_GAPS")
    df = parsing.read_xlsx(tax_gaps_xlsx, sheet=cd.sheet, header_row=cd.header_row)
    missing = [c.source_column for c in cd.columns.values() if c.source_column not in df.columns]
    assert not missing, f"source columns missing: {missing}"


def test_tax_gaps_stats_group_by_tax_type(tax_gaps_xlsx):
    """stats group_by should give 4 buckets (one per tax type), with
    personal income tax having the highest mean."""
    cd = curated.get("TAX_GAPS")
    df = _parse(cd, tax_gaps_xlsx)
    resp = shaping.build_response(
        cd=cd, df=df, filters={}, measures="net_gap_millions",
        start_period=None, end_period=None, fmt="records", user_query={},
    )
    # Build the group_by buckets manually since shaping doesn't, but the
    # server-level test exercises the integration. Here just check the data.
    by_type: dict[str, list[float]] = {}
    for r in resp.records:
        t = r.dimensions["tax_type"]
        by_type.setdefault(t, []).append(r.value)
    means = {t: sum(v) / len(v) for t, v in by_type.items()}
    assert means["Personal income tax"] > means["Corporate income tax"]
    assert means["Personal income tax"] > means["Goods and services tax"]
