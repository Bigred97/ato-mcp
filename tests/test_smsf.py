"""Tests for the SMSF_FUNDS curated dataset + max_rows sub-table extraction.

SMSF Annual Overview is the first dataset that carves a sub-table out of
a multi-section sheet — Table 1 sheet has 6 sub-tables stacked vertically
and we only want sub-table 2 ("Annual SMSF population and assets" at
header_row 13). `max_rows=3` is the schema feature that makes this work.

Also exercises the latest-direction fix: SMSF lists years DESCENDING in
source (2024-25 → 2019-20), and `latest` must still return the newest year.
"""
from __future__ import annotations

import pytest

from ato_mcp import curated, parsing, shaping


def _parse(cd, body):
    return parsing.read_xlsx(
        body, sheet=cd.sheet, header_row=cd.header_row,
        data_start_row=cd.data_start_row, max_rows=cd.max_rows,
    )


def test_smsf_yaml_loads():
    cd = curated.get("SMSF_FUNDS")
    assert cd is not None
    assert cd.layout == "transposed"
    assert cd.sheet == "Table 1"
    assert cd.header_row == 13
    assert cd.data_start_row == 14
    assert cd.max_rows == 3
    assert cd.discovery is not None
    # Three headline metrics
    aliases = cd.dimension_values["metric_label"].values
    assert {"total_smsfs", "total_members", "total_gross_assets_millions"} == set(aliases.keys())


def test_smsf_max_rows_carves_subtable(smsf_annual_overview_xlsx):
    """Without max_rows, read_xlsx would return ~20 rows including data from
    other sub-tables. With max_rows=3, we get just the population-and-assets
    block we curated."""
    cd = curated.get("SMSF_FUNDS")
    df = _parse(cd, smsf_annual_overview_xlsx)
    assert len(df) == 3
    # Metric labels should be the three headline series
    labels = df.iloc[:, 0].astype(str).str.strip().tolist()
    assert set(labels) == {"Total SMSFs", "Total members", "Total gross assets ($m)"}


def test_smsf_full_query_returns_18_observations(smsf_annual_overview_xlsx):
    cd = curated.get("SMSF_FUNDS")
    df = _parse(cd, smsf_annual_overview_xlsx)
    resp = shaping.build_response(
        cd=cd, df=df, filters={}, measures=None,
        start_period=None, end_period=None, fmt="records", user_query={},
    )
    # 3 metrics × 6 financial years = 18 observations
    assert resp.row_count == 18


def test_smsf_real_values_plausible(smsf_annual_overview_xlsx):
    """Spot-check known values from the 2023-24 release."""
    cd = curated.get("SMSF_FUNDS")
    df = _parse(cd, smsf_annual_overview_xlsx)
    resp = shaping.build_response(
        cd=cd, df=df, filters={}, measures="total_smsfs",
        start_period=None, end_period=None, fmt="records", user_query={},
    )
    by_year = {r.period: r.value for r in resp.records}
    # 2023-24 ~614k SMSFs, 2019-20 ~566k. Growing.
    assert 600_000 < by_year["2023-24"] < 700_000
    assert 500_000 < by_year["2019-20"] < 600_000
    assert by_year["2023-24"] > by_year["2019-20"]


def test_smsf_latest_returns_newest_year(smsf_annual_overview_xlsx):
    """Source file orders years DESCENDING (2024-25 first). latest=1 must
    still return the newest year — this is the regression case that drove
    the sort-by-normalized-period fix in shape_transposed."""
    cd = curated.get("SMSF_FUNDS")
    df = _parse(cd, smsf_annual_overview_xlsx)
    resp = shaping.build_response(
        cd=cd, df=df, filters={}, measures="total_smsfs",
        start_period=None, end_period=None, fmt="records", user_query={},
        last_n=1,
    )
    assert resp.row_count == 1
    rec = resp.records[0]
    # Period should be 2024-25 (the latest in the file), NOT 2019-20
    assert rec.period == "2024-25"
    assert rec.value > 640_000  # ~653k


def test_smsf_total_gross_assets_is_trillion_dollar_sector(smsf_annual_overview_xlsx):
    """Sanity: the SMSF sector holds ~$1T in assets as of 2023-24."""
    cd = curated.get("SMSF_FUNDS")
    df = _parse(cd, smsf_annual_overview_xlsx)
    resp = shaping.build_response(
        cd=cd, df=df, filters={}, measures="total_gross_assets_millions",
        start_period=None, end_period=None, fmt="records", user_query={},
    )
    latest = max(resp.records, key=lambda r: r.period or "")
    # Total gross assets are reported in $m. ~$1,000,000m = ~$1 trillion.
    assert latest.value > 900_000   # > $900B = $0.9T
    assert latest.value < 2_000_000  # < $2T sanity ceiling
