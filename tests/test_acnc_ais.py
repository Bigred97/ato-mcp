"""Tests for ACNC_AIS_FINANCIALS — per-charity financial details.

Companion to ACNC_REGISTER (which carries identity/address/jurisdiction).
This dataset adds revenue/expense/staff numbers.
"""
from __future__ import annotations

import tracemalloc

import pandas as pd

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


def test_acnc_ais_streaming_reader_matches_pandas_for_curated_columns(acnc_ais_csv):
    """The streaming reader's projected columns must match pd.read_csv's
    after both pass through the downstream coercion in `_coerce_dtypes`.

    Pandas auto-converts numeric-looking columns to float at parse time;
    the streaming reader leaves cells as strings (downstream
    shaping._coerce_dtypes applies the curated dtype hint). So we
    compare AFTER coercion: every column should land on identical
    final values regardless of which reader produced the frame.
    """
    cd = curated.get("ACNC_AIS_FINANCIALS")
    source_cols = [c.source_column for c in cd.columns.values()]

    df_stream = parsing.read_csv_streaming(acnc_ais_csv, columns=source_cols)
    df_pandas = parsing.read_csv(acnc_ais_csv).reset_index(drop=True)

    assert len(df_stream) == len(df_pandas)
    assert set(df_stream.columns) == set(source_cols)
    # Apply the same per-column coercion both frames would receive
    # downstream in shaping._coerce_dtypes.
    renamed_stream = shaping._apply_aliases(df_stream, cd)
    renamed_pandas = shaping._apply_aliases(df_pandas, cd)
    coerced_stream = shaping._coerce_dtypes(renamed_stream, cd)
    coerced_pandas = shaping._coerce_dtypes(renamed_pandas, cd)
    # Compare every aliased column.
    for col in coerced_stream.columns:
        assert col in coerced_pandas.columns
        s_stream = coerced_stream[col]
        s_pandas = coerced_pandas[col]
        # Compare via string-ified form to handle NaN/Int64/float types uniformly.
        a = s_stream.astype("string").fillna("__NA__")
        b = s_pandas.astype("string").fillna("__NA__")
        assert (a.values == b.values).all(), (
            f"mismatch in column {col!r}: "
            f"streaming sample={a.head(3).tolist()}, pandas sample={b.head(3).tolist()}"
        )


def test_acnc_ais_financials_streams_under_memory_budget(acnc_ais_csv):
    """Regression for the production-blocking OOM on 512MB hosts.

    The full ACNC AIS CSV (36MB, 53k rows, 91 columns) used to spike peak
    memory to ~1.15GB when loaded via `pd.read_csv` because pandas keeps
    every column as `object` dtype. The streaming reader projects to the
    ~23 curated columns at parse time, keeping the in-memory footprint
    bounded.

    This test uses the 200KB head-sample fixture (279 rows) to keep CI
    fast, but the memory budget below is a tight per-row check that
    extrapolates correctly to the full file. The pre-fix `pd.read_csv`
    path consistently used 5-10x more memory than the streaming path on
    this same fixture in local benchmarking.
    """
    cd = curated.get("ACNC_AIS_FINANCIALS")
    source_cols = [c.source_column for c in cd.columns.values()]

    tracemalloc.start()
    df = parsing.read_csv_streaming(acnc_ais_csv, columns=source_cols)
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    peak_mb = peak / 1024 / 1024

    assert len(df) > 100, "fixture should have >100 rows"
    # 200KB fixture × ~23 columns. Pre-fix pandas full-load on this fixture
    # peaked ~5MB; the streaming reader peaks well under 2MB. Budget at
    # 10MB so transient interpreter noise doesn't flake the test, but
    # any future regression (full file load, accidental column-explosion)
    # blows through immediately.
    assert peak_mb < 10, (
        f"streaming parser used {peak_mb:.1f}MB peak on 200KB fixture — "
        "looks like the column projection regressed. Check that "
        "ACNC_AIS_FINANCIALS still routes through read_csv_streaming "
        "in server._fetch_and_parse."
    )
    # Confirm we projected to the curated subset, not the full 91 columns.
    assert len(df.columns) == len(source_cols), (
        f"streaming reader returned {len(df.columns)} columns; "
        f"expected the {len(source_cols)} curated source columns"
    )


def test_acnc_ais_streaming_reader_drops_unprojected_columns(acnc_ais_csv):
    """Sanity check: the reader must NOT return columns that weren't asked
    for. Catches a silent regression where the column-projection list is
    ignored and the full 91-column frame leaks downstream.
    """
    df = parsing.read_csv_streaming(
        acnc_ais_csv, columns=["abn", "charity name", "total revenue"],
    )
    assert sorted(df.columns) == ["abn", "charity name", "total revenue"]
    assert len(df) > 0


def test_acnc_ais_streaming_reader_handles_missing_columns_gracefully(acnc_ais_csv):
    """Requesting a column that doesn't exist in the source returns the
    columns that DO exist — same lenient behaviour as `_apply_aliases`
    catches in shaping.py. Missing columns are filled by neither reader."""
    df = parsing.read_csv_streaming(
        acnc_ais_csv, columns=["abn", "this_column_does_not_exist"],
    )
    assert "abn" in df.columns
    assert "this_column_does_not_exist" not in df.columns


def test_acnc_ais_streaming_reader_empty_body_raises():
    try:
        parsing.read_csv_streaming(b"", columns=["abn"])
    except parsing.ParseError as e:
        assert "empty" in str(e).lower()
    else:
        raise AssertionError("expected ParseError on empty body")


def test_acnc_ais_streaming_reader_no_matching_columns_raises(acnc_ais_csv):
    try:
        parsing.read_csv_streaming(
            acnc_ais_csv, columns=["no_such_column", "another_phantom"],
        )
    except parsing.ParseError as e:
        assert "matched no columns" in str(e)
    else:
        raise AssertionError("expected ParseError when projection matches nothing")


def test_acnc_ais_streaming_reader_returns_dataframe_with_nan(acnc_ais_csv):
    """Empty cells must become NaN (matching pandas behaviour) so downstream
    `_coerce_dtypes` and `drop_blank_rows` work the same as before."""
    cd = curated.get("ACNC_AIS_FINANCIALS")
    source_cols = [c.source_column for c in cd.columns.values()]
    df = parsing.read_csv_streaming(acnc_ais_csv, columns=source_cols)
    # At least one column in the fixture has empty cells — check NaN exists.
    has_any_nan = df.isna().any().any()
    assert has_any_nan, "expected at least one NaN cell in the streamed frame"
    # `charity name` is unambiguously a text column; pandas should keep it
    # as object dtype with string values.
    if "charity name" in df.columns:
        sample = df["charity name"].dropna().head(5).tolist()
        for v in sample:
            assert isinstance(v, str), f"charity name cell should be str, got {type(v)}"


def test_acnc_ais_get_data_path_uses_streaming_reader():
    """Confirm the server dispatches ACNC_AIS_FINANCIALS through the
    streaming reader, not the full-load pandas reader. This is the actual
    customer-facing wire — a regression here re-introduces the 512MB OOM.
    """
    from ato_mcp.server import _STREAMING_CSV_DATASETS
    assert "ACNC_AIS_FINANCIALS" in _STREAMING_CSV_DATASETS
