"""Tests for the emit-time record cap in shape_wide / shape_transposed.

The cap prevents wide-layout datasets like ACNC_AIS_FINANCIALS from
materialising all 53k entities × 16 measures = 853k Observation objects
(~850 MB Pydantic state) when the customer only wanted the first 5
rows. Without the cap shape_wide produces the full set, build_response
slices to `limit` post-hoc, and the worker OOMs in between.

We exercise:
  - shape_wide stops emitting at max_records
  - shape_transposed stops emitting at max_records
  - build_response derives the right cap from (limit, last_n)
  - latest() with last_n=1 still works (cap doesn't break per-measure trim)
"""
from __future__ import annotations

from typing import Any

import pandas as pd
import pytest

from ato_mcp import curated, shaping


@pytest.fixture(autouse=True)
def reset_registry():
    curated.reset_registry()
    yield
    curated.reset_registry()


def test_shape_wide_short_circuits_at_max_records():
    """Two-measure wide table × 10 rows would emit 20 Observations.
    Capping at 5 must return exactly 5."""
    # IND_POSTCODE has measure columns; use a synthetic frame matching
    # its schema for a unit test that doesn't require parsing.
    cd = curated.get("IND_POSTCODE")
    assert cd is not None
    assert cd.layout == "wide"

    measure_keys = [c.key for c in curated.measure_columns(cd)][:2]

    rows = []
    for i in range(10):
        row = {c.source_column: ("VIC" if i % 2 == 0 else "NSW") for c in cd.columns.values()
               if c.role in ("dimension", "id")}
        # All measure columns get a value
        for c in cd.columns.values():
            if c.role == "measure":
                row[c.source_column] = float(i + 1)
        rows.append(row)
    df = pd.DataFrame(rows)
    # Mimic what _apply_aliases would produce — rename source → key
    renamed = df.rename(columns={c.source_column: c.key for c in cd.columns.values()})

    # Without a cap: 10 rows × 2 measures = 20 Observations.
    no_cap = shaping.shape_wide(renamed, cd, measure_keys)
    assert len(no_cap) == 20

    # With cap=5: short-circuits at exactly 5.
    capped = shaping.shape_wide(renamed, cd, measure_keys, max_records=5)
    assert len(capped) == 5

    # Cap larger than the natural count is a no-op.
    over_cap = shaping.shape_wide(renamed, cd, measure_keys, max_records=1000)
    assert len(over_cap) == 20


def test_shape_wide_cap_returns_first_records_in_iteration_order():
    """The cap returns the FIRST N records, not a random N."""
    cd = curated.get("IND_POSTCODE")
    measure_keys = [c.key for c in curated.measure_columns(cd)][:1]

    rows = []
    for i in range(20):
        row = {c.source_column: f"row{i}" for c in cd.columns.values()
               if c.role in ("dimension", "id")}
        for c in cd.columns.values():
            if c.role == "measure":
                row[c.source_column] = float(i)
        rows.append(row)
    df = pd.DataFrame(rows)
    renamed = df.rename(columns={c.source_column: c.key for c in cd.columns.values()})

    capped = shaping.shape_wide(renamed, cd, measure_keys, max_records=3)
    assert len(capped) == 3
    # Values 0, 1, 2 — first three rows.
    assert [obs.value for obs in capped] == [0.0, 1.0, 2.0]


def test_build_response_uses_limit_as_cap_when_no_last_n():
    """get_data(limit=N) ⇒ shape_cap = min(N, _HARD_MAX_RECORDS).

    For wide layout this means the customer's `limit` short-circuits
    shape_wide; we never materialise more than the user asked for.
    """
    cd = curated.get("IND_POSTCODE")
    rows = [{c.source_column: "VIC" for c in cd.columns.values()
             if c.role in ("dimension", "id")}
            for _ in range(50)]
    for r in rows:
        for c in cd.columns.values():
            if c.role == "measure":
                r[c.source_column] = 1.0
    df = pd.DataFrame(rows)

    # limit=3, last_n=None → shape_cap=3
    resp = shaping.build_response(
        cd=cd, df=df, filters={}, measures=None,
        start_period=None, end_period=None, fmt="records",
        user_query={}, last_n=None, limit=3,
    )
    assert resp.row_count == 3
    # truncated_at reports >= 3 (we hit the cap)
    assert resp.truncated_at is not None
    assert resp.truncated_at >= 3


def test_build_response_uses_hard_max_when_last_n_set():
    """latest() passes last_n=1 ⇒ shape_cap = _HARD_MAX_RECORDS.

    The hard ceiling preserves enough records for the per-measure
    sort+trim to be meaningful. The customer-facing `limit` slice
    still happens post-hoc.
    """
    cd = curated.get("IND_POSTCODE")
    # 5 rows × however-many-measures; shouldn't hit _HARD_MAX
    rows = []
    for i in range(5):
        row = {c.source_column: "VIC" for c in cd.columns.values()
               if c.role in ("dimension", "id")}
        for c in cd.columns.values():
            if c.role == "measure":
                row[c.source_column] = float(i)
        rows.append(row)
    df = pd.DataFrame(rows)

    # latest path: last_n=1, limit=3
    resp = shaping.build_response(
        cd=cd, df=df, filters={}, measures=None,
        start_period=None, end_period=None, fmt="records",
        user_query={}, last_n=1, limit=3,
    )
    assert resp.row_count <= 3  # may be less if no per-measure periods


def test_shape_transposed_short_circuits():
    """Transposed shape also honours max_records.

    Build a minimal GST_MONTHLY-shaped frame: one metric-label row,
    five period columns ⇒ 5 Observations natural, cap at 2 ⇒ 2.
    """
    cd = curated.get("GST_MONTHLY")
    assert cd.layout == "transposed"
    assert cd.metric_label_column is not None

    label_src = cd.metric_label_column
    unit_src = cd.unit_column

    row: dict[str, Any] = {label_src: "Net GST"}
    if unit_src:
        row[unit_src] = "AUD millions"
    for p in ("2024-01", "2024-02", "2024-03", "2024-04", "2024-05"):
        row[p] = 100.0
    df = pd.DataFrame([row])
    # Rename source → key (mimic _apply_aliases) for the curated columns
    rename = {c.source_column: c.key for c in cd.columns.values()
              if c.source_column in df.columns}
    df = df.rename(columns=rename)

    # measures=[] means no measure filter — all label rows shape
    full = shaping.shape_transposed(df, cd, [], None, None)
    assert len(full) == 5

    capped = shaping.shape_transposed(df, cd, [], None, None, max_records=2)
    assert len(capped) == 2


def test_acnc_ais_query_with_synthetic_cap():
    """Smoke: build_response on a synthetic ACNC frame with limit=5
    materialises ~5 records, not the entire frame × measures.

    Without the cap, the 53k × 16 explosion peaks at ~850 MB Pydantic
    state. We assert here only on the row_count + truncated_at signals
    (the memory profile is verified by the live test in
    tests/test_integration.py).
    """
    cd = curated.get("ACNC_AIS_FINANCIALS")
    measure_cols = [c for c in cd.columns.values() if c.role == "measure"]
    dim_cols = [c for c in cd.columns.values() if c.role in ("dimension", "id")]

    # Build a 1000-row synthetic frame. With 16 measures that's 16k
    # potential Observations — easily above limit=5 but below
    # _HARD_MAX_RECORDS=100k, so the cap-from-limit path fires.
    rows = []
    for i in range(1000):
        row = {c.source_column: f"value_{i}" for c in dim_cols}
        for c in measure_cols:
            row[c.source_column] = float(i)
        rows.append(row)
    df = pd.DataFrame(rows)

    resp = shaping.build_response(
        cd=cd, df=df, filters={}, measures=None,
        start_period=None, end_period=None, fmt="records",
        user_query={}, last_n=None, limit=5,
    )
    assert resp.row_count == 5
    assert resp.truncated_at is not None
    assert resp.truncated_at >= 5
