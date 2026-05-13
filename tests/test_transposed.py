"""Tests for the transposed-layout code path via GST_MONTHLY.

Transposed datasets have metrics as rows and periods as columns — opposite
of the wide layout. This file probes:
  - All-measures pass through (10 metrics × 48 months = 480 records)
  - Single-measure filter via alias
  - Period range filter (uses _normalize_period to disambiguate
    YYYY-MM month vs YYYY-YY financial year)
  - last_n=1 returns one observation per measure (latest period)
  - CSV / series output formats
  - Whitespace stripping on metric labels ("Net GST " → "Net GST")
  - Unknown measure → helpful error
  - Discovery block present and well-formed
"""
from __future__ import annotations

import pytest

from ato_mcp import curated, parsing, shaping


def _parse(cd, body):
    return parsing.read_xlsx(
        body, sheet=cd.sheet, header_row=cd.header_row,
        data_start_row=cd.data_start_row,
    )


def test_gst_monthly_yaml_loads():
    cd = curated.get("GST_MONTHLY")
    assert cd is not None
    assert cd.layout == "transposed"
    assert cd.metric_label_column == "Unnamed: 0"
    assert cd.unit_column == "Unnamed: 1"
    assert cd.sheet == "Table 1B"
    assert cd.header_row == 2
    assert cd.discovery is not None
    # Spot check some expected metric aliases
    aliases = cd.dimension_values["metric_label"].values
    assert {"net_gst", "gross_gst", "wet_payable", "net_lct"}.issubset(aliases.keys())


def test_gst_monthly_all_measures(gst_monthly_xlsx):
    cd = curated.get("GST_MONTHLY")
    df = _parse(cd, gst_monthly_xlsx)
    resp = shaping.build_response(
        cd=cd, df=df, filters={}, measures=None,
        start_period=None, end_period=None, fmt="records", user_query={},
    )
    # 10 declared measures × 48 months = 480 (the source has no NaN cells)
    assert resp.row_count == 480
    assert resp.unit == "$m"
    # Every record should have a measure that's in our alias set
    aliases = set(cd.dimension_values["metric_label"].values.keys())
    returned_measures = {r.measure for r in resp.records}
    assert returned_measures == aliases


def test_gst_monthly_single_measure_filter(gst_monthly_xlsx):
    cd = curated.get("GST_MONTHLY")
    df = _parse(cd, gst_monthly_xlsx)
    resp = shaping.build_response(
        cd=cd, df=df, filters={}, measures="net_gst",
        start_period=None, end_period=None, fmt="records", user_query={},
    )
    assert resp.row_count == 48
    assert all(r.measure == "net_gst" for r in resp.records)
    assert all(r.unit == "$m" for r in resp.records)


def test_gst_monthly_whitespace_stripped(gst_monthly_xlsx):
    """ATO ships 'Net GST ' (trailing space). The YAML aliases 'Net GST'
    (clean). The shape layer must strip whitespace so they match."""
    cd = curated.get("GST_MONTHLY")
    df = _parse(cd, gst_monthly_xlsx)
    resp = shaping.build_response(
        cd=cd, df=df, filters={}, measures="net_gst",
        start_period=None, end_period=None, fmt="records", user_query={},
    )
    # Source label is exposed via dimensions['metric_source_label'] —
    # confirm it's the clean form after our strip step.
    assert resp.records[0].dimensions["metric_source_label"] == "Net GST"


def test_gst_monthly_period_range_inclusive(gst_monthly_xlsx):
    """start='2023-01' end='2023-06' should give exactly the 6 months of H1 2023.

    This is the regression case where '2023-06' was previously misparsed as
    a financial year (YY=06) — the disambiguation rule says YY 01-12 is a
    month and 13-99 is a financial year.
    """
    cd = curated.get("GST_MONTHLY")
    df = _parse(cd, gst_monthly_xlsx)
    resp = shaping.build_response(
        cd=cd, df=df, filters={}, measures="net_gst",
        start_period="2023-01", end_period="2023-06",
        fmt="records", user_query={},
    )
    assert resp.row_count == 6  # 6 months
    # Confirm period bounds
    periods = sorted({r.period for r in resp.records})
    assert periods[0].startswith("2023-01")
    assert periods[-1].startswith("2023-06")


def test_gst_monthly_latest_one_per_measure(gst_monthly_xlsx):
    cd = curated.get("GST_MONTHLY")
    df = _parse(cd, gst_monthly_xlsx)
    resp = shaping.build_response(
        cd=cd, df=df, filters={}, measures=None,
        start_period=None, end_period=None, fmt="records", user_query={},
        last_n=1,
    )
    # 10 measures × 1 latest = 10
    assert resp.row_count == 10
    measures_returned = {r.measure for r in resp.records}
    assert "net_gst" in measures_returned
    # All "latest" observations should share the same period (last month in the file)
    periods = {r.period for r in resp.records}
    assert len(periods) == 1  # only one period — the most recent month


def test_gst_monthly_csv_format(gst_monthly_xlsx):
    cd = curated.get("GST_MONTHLY")
    df = _parse(cd, gst_monthly_xlsx)
    resp = shaping.build_response(
        cd=cd, df=df, filters={}, measures=["net_gst", "gross_gst"],
        start_period="2024-01", end_period=None,
        fmt="csv", user_query={},
    )
    assert resp.csv is not None
    lines = resp.csv.strip().split("\n")
    assert lines[0].startswith("period,measure,value,unit")
    # 6 months × 2 measures = 12 data rows
    assert len(lines) == 13  # header + 12 data


def test_gst_monthly_series_format(gst_monthly_xlsx):
    cd = curated.get("GST_MONTHLY")
    df = _parse(cd, gst_monthly_xlsx)
    resp = shaping.build_response(
        cd=cd, df=df, filters={}, measures=["net_gst", "gross_gst"],
        start_period="2024-01", end_period=None,
        fmt="series", user_query={},
    )
    assert len(resp.records) == 2
    measures_in_groups = {g["measure"] for g in resp.records}
    assert measures_in_groups == {"net_gst", "gross_gst"}
    for g in resp.records:
        assert g["unit"] == "$m"
        assert len(g["observations"]) == 6


def test_gst_monthly_unknown_measure_lists_alternatives(gst_monthly_xlsx):
    cd = curated.get("GST_MONTHLY")
    df = _parse(cd, gst_monthly_xlsx)
    with pytest.raises(ValueError, match="Unknown measure") as exc_info:
        shaping.build_response(
            cd=cd, df=df, filters={}, measures="not_a_metric",
            start_period=None, end_period=None, fmt="records", user_query={},
        )
    # Error must list valid alternatives, including net_gst
    assert "net_gst" in str(exc_info.value)


def test_period_in_range_end_year_includes_all_months(gst_monthly_xlsx):
    """Regression: end_period='2024' against monthly data must INCLUDE
    every 2024-NN row. A naive string compare excluded them (audit bug #3).
    """
    cd = curated.get("GST_MONTHLY")
    df = _parse(cd, gst_monthly_xlsx)
    resp = shaping.build_response(
        cd=cd, df=df, filters={}, measures="net_gst",
        start_period="2024", end_period="2024",
        fmt="records", user_query={},
    )
    # 2024 only — there should be at least one record (Jan-Jun 2024 in the
    # 2022-23 file, since the GST monthly table goes to 2024-06).
    assert resp.row_count >= 1
    # Every record's period must start with 2024
    for r in resp.records:
        assert r.period.startswith("2024"), f"unexpected period: {r.period}"


def test_period_in_range_end_year_includes_dec(gst_monthly_xlsx):
    """end_period='2023' should include 2023-12 (December 2023)."""
    cd = curated.get("GST_MONTHLY")
    df = _parse(cd, gst_monthly_xlsx)
    resp = shaping.build_response(
        cd=cd, df=df, filters={}, measures="net_gst",
        start_period="2023-12", end_period="2023",
        fmt="records", user_query={},
    )
    # start=2023-12 and end=2023 (whole year) — December 2023 must be in range
    periods = {r.period[:7] for r in resp.records}
    assert "2023-12" in periods, f"got periods: {periods}"


def test_period_normalize_disambiguates_month_vs_financial_year():
    from ato_mcp.shaping import _normalize_period
    # Months: YY <= 12
    assert _normalize_period("2023-01") == "2023-01"
    assert _normalize_period("2023-06") == "2023-06"
    assert _normalize_period("2023-12") == "2023-12"
    # Financial years: YY >= 13
    assert _normalize_period("2022-23") == "2022"
    assert _normalize_period("2022-99") == "2022"
    # Year only
    assert _normalize_period("2024") == "2024"
    # Excel ISO datetime stringified
    assert _normalize_period("2023-06-30 00:00:00") == "2023-06"
    # Garbage
    assert _normalize_period("not a date") is None
    assert _normalize_period("") is None
    assert _normalize_period("2023-00") is None


def test_period_normalize_handles_03_04_correctly():
    """Per the disambiguation rule, '2003-04' suffix 04 <= 12 → month, not financial year."""
    from ato_mcp.shaping import _normalize_period
    assert _normalize_period("2003-04") == "2003-04"


def test_apply_aliases_preserves_period_columns_for_transposed(gst_monthly_xlsx):
    """The biggest transposed-layout fix: _apply_aliases must NOT drop the
    date columns when layout=transposed. They carry the data."""
    cd = curated.get("GST_MONTHLY")
    df = _parse(cd, gst_monthly_xlsx)
    aliased = shaping._apply_aliases(df, cd)
    # Should have 2 alias columns + 48 period columns
    assert "metric_label" in aliased.columns
    assert "unit" in aliased.columns
    period_cols = [c for c in aliased.columns if c not in ("metric_label", "unit")]
    assert len(period_cols) == 48
