"""Tests for the SBB_BENCHMARKS curated dataset.

Small Business Benchmarks is a fintech / accounting / tax-advisor goldmine
— "what's the typical COGS ratio for a bakery?" gets a one-call answer.
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


def test_sbb_yaml_loads():
    cd = curated.get("SBB_BENCHMARKS")
    assert cd is not None
    assert cd.layout == "wide"
    assert cd.discovery is not None
    # Expected measures
    measure_keys = {c.key for c in cd.columns.values() if c.role == "measure"}
    assert {
        "total_expenses_med_min", "total_expenses_med_max",
        "cost_of_sales_med_min", "cost_of_sales_med_max",
    }.issubset(measure_keys)


def test_sbb_full_load(sbb_benchmarks_xlsx):
    cd = curated.get("SBB_BENCHMARKS")
    df = _parse(cd, sbb_benchmarks_xlsx)
    # ~100 industries
    assert 80 < len(df) < 110


def test_sbb_bakery_medium_turnover_known_values(sbb_benchmarks_xlsx):
    """Bakery medium-turnover total expenses should be 0.75–0.86 (verified
    against the published 2023-24 ratios)."""
    cd = curated.get("SBB_BENCHMARKS")
    df = _parse(cd, sbb_benchmarks_xlsx)
    resp = shaping.build_response(
        cd=cd, df=df,
        filters={"business_type": "Bakeries and hot bread shops"},
        measures=["total_expenses_med_min", "total_expenses_med_max"],
        start_period=None, end_period=None, fmt="records", user_query={},
    )
    assert resp.row_count == 2
    by_measure = {r.measure: r.value for r in resp.records}
    assert 0.7 < by_measure["total_expenses_med_min"] < 0.8
    assert 0.8 < by_measure["total_expenses_med_max"] < 0.9


def test_sbb_top_n_highest_expense_ratio(sbb_benchmarks_xlsx):
    """Industries with the highest expected total-expense ratio are
    low-margin retail: fuel, tobacco, grocery, etc. Sanity check."""
    cd = curated.get("SBB_BENCHMARKS")
    df = _parse(cd, sbb_benchmarks_xlsx)
    resp = shaping.build_response(
        cd=cd, df=df, filters={},
        measures="total_expenses_med_min",
        start_period=None, end_period=None, fmt="records", user_query={},
    )
    sorted_records = sorted(
        [r for r in resp.records if r.value is not None],
        key=lambda r: r.value, reverse=True,
    )
    top_5_names = [r.dimensions["business_type"].lower() for r in sorted_records[:5]]
    # At least one of the recognisably low-margin retail categories should
    # appear in the top 5.
    assert any(
        kw in name
        for name in top_5_names
        for kw in ("fuel", "tobacco", "grocery", "liquor", "tyre")
    ), f"top 5 didn't include any low-margin retail: {top_5_names}"


def test_sbb_turnover_range_strings_carry_through(sbb_benchmarks_xlsx):
    """The turnover-range descriptor columns should appear in every record's
    dimensions so an agent knows what "low" / "medium" / "high" mean."""
    cd = curated.get("SBB_BENCHMARKS")
    df = _parse(cd, sbb_benchmarks_xlsx)
    resp = shaping.build_response(
        cd=cd, df=df,
        filters={"business_type": "Bakeries and hot bread shops"},
        measures="total_expenses_med_min",
        start_period=None, end_period=None, fmt="records", user_query={},
    )
    assert resp.row_count == 1
    dims = resp.records[0].dimensions
    # Range descriptors carried through as dimensions
    assert "$" in (dims.get("low_turnover_range") or "")
    assert "$" in (dims.get("medium_turnover_range") or "")
    assert "More than" in (dims.get("high_turnover_range") or "")


def test_sbb_canary_source_columns_match(sbb_benchmarks_xlsx):
    cd = curated.get("SBB_BENCHMARKS")
    df = parsing.read_xlsx(sbb_benchmarks_xlsx, sheet=cd.sheet, header_row=cd.header_row)
    missing = [c.source_column for c in cd.columns.values() if c.source_column not in df.columns]
    assert not missing, f"source columns missing: {missing}"
