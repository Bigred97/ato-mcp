"""Tests for ACNC_AIS_FINANCIALS — per-charity financial details.

Companion to ACNC_REGISTER (which carries identity/address/jurisdiction).
This dataset adds revenue/expense/staff numbers.
"""
from __future__ import annotations

from ato_mcp import curated, parsing, shaping


def _parse(cd, body):
    df = parsing.read_csv(body)
    dim_cols = [c.source_column for c in cd.columns.values() if c.role == "dimension"]
    return parsing.drop_blank_rows(df, dim_cols)


def test_acnc_ais_yaml_loads():
    cd = curated.get("ACNC_AIS_FINANCIALS")
    assert cd is not None
    assert cd.format == "csv"
    assert cd.layout == "wide"
    assert cd.cache_kind == "register"  # weekly cadence
    assert cd.discovery is not None
    measure_keys = {c.key for c in cd.columns.values() if c.role == "measure"}
    for required in (
        "total_revenue", "total_expenses", "net_surplus_deficit",
        "donations_and_bequests", "staff_full_time", "staff_volunteers",
    ):
        assert required in measure_keys, f"missing measure {required}"


def test_acnc_ais_head_sample_loads(acnc_ais_csv):
    cd = curated.get("ACNC_AIS_FINANCIALS")
    df = _parse(cd, acnc_ais_csv)
    # Head sample is a 200KB byte-range slice of the full 35MB CSV. The exact
    # row count depends on how many rows fit, but it should be a substantial
    # alphabetical slice (low-ABN charities).
    assert 100 < len(df) < 500


def test_acnc_ais_large_charities_have_meaningful_revenue(acnc_ais_csv):
    """Large charities should report > $250k in revenue (ACNC threshold)."""
    cd = curated.get("ACNC_AIS_FINANCIALS")
    df = _parse(cd, acnc_ais_csv)
    resp = shaping.build_response(
        cd=cd, df=df, filters={"charity_size": "large"},
        measures="total_revenue",
        start_period=None, end_period=None, fmt="records", user_query={},
    )
    assert resp.row_count > 0
    # Every Large charity should clear the ACNC threshold (~$250k)
    for r in resp.records:
        assert r.value > 250_000, (
            f"{r.dimensions.get('charity_name')}: revenue {r.value} below Large threshold"
        )


def test_acnc_ais_revenue_vs_expenses_sanity(acnc_ais_csv):
    """For most charities, total_revenue and total_expenses are within an
    order of magnitude of each other (charities run roughly break-even)."""
    cd = curated.get("ACNC_AIS_FINANCIALS")
    df = _parse(cd, acnc_ais_csv)
    rev = shaping.build_response(
        cd=cd, df=df, filters={"charity_size": "large"},
        measures="total_revenue",
        start_period=None, end_period=None, fmt="records", user_query={},
    )
    exp = shaping.build_response(
        cd=cd, df=df, filters={"charity_size": "large"},
        measures="total_expenses",
        start_period=None, end_period=None, fmt="records", user_query={},
    )
    by_abn_rev = {r.dimensions["abn"]: r.value for r in rev.records}
    by_abn_exp = {r.dimensions["abn"]: r.value for r in exp.records}
    # For at least half the charities, expenses should be 20%-500% of revenue
    sane = 0
    for abn, r in by_abn_rev.items():
        e = by_abn_exp.get(abn)
        if e is None or r <= 0:
            continue
        ratio = e / r
        if 0.2 <= ratio <= 5.0:
            sane += 1
    assert sane >= len(by_abn_rev) // 2, (
        f"only {sane}/{len(by_abn_rev)} charities had sane revenue/expense ratios"
    )


def test_acnc_ais_canary_source_columns_match(acnc_ais_csv):
    cd = curated.get("ACNC_AIS_FINANCIALS")
    df = parsing.read_csv(acnc_ais_csv)
    missing = [c.source_column for c in cd.columns.values() if c.source_column not in df.columns]
    assert not missing, f"source columns missing: {missing}"


def test_acnc_ais_abn_is_clean_string(acnc_ais_csv):
    cd = curated.get("ACNC_AIS_FINANCIALS")
    df = _parse(cd, acnc_ais_csv)
    resp = shaping.build_response(
        cd=cd, df=df, filters={}, measures="total_revenue",
        start_period=None, end_period=None, fmt="records", user_query={},
    )
    sample_abns = [r.dimensions["abn"] for r in resp.records[:20] if r.dimensions.get("abn")]
    for abn in sample_abns:
        assert "." not in abn, f"ABN has trailing .0: {abn!r}"
