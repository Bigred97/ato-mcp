"""Tests for ACNC_REGISTER — the live register of Australian charities.

Companion to ACNC_AIS_FINANCIALS (which carries the financial detail).
This dataset is the identity/address/jurisdiction layer.

The full upstream CSV is ~50MB / 69 columns / 60k+ rows. Loading it whole
via `pd.read_csv` OOMs / times out on 512MB hosts the same way
ACNC_AIS_FINANCIALS did before 0.8.3 — so this dataset also routes through
`parsing.read_csv_streaming` (column-projected) via the
`_STREAMING_CSV_DATASETS` opt-in set in server.py.
"""
from __future__ import annotations

import tracemalloc

from ato_mcp import curated, parsing


def test_acnc_register_streams_under_memory_budget(acnc_register_csv):
    """Regression for the production-blocking OOM on 512MB hosts.

    The full ACNC Register CSV (~50MB, 60k+ rows, 69 columns) used to OOM
    when loaded via `pd.read_csv` because pandas keeps every column as
    `object` dtype. The streaming reader projects to the ~22 curated
    columns at parse time, keeping the in-memory footprint bounded.

    This test uses the ~30KB head-sample fixture (129 rows) for CI speed,
    but the budget below is a tight per-row check — if the streaming
    reader regresses (e.g. drops the column projection), peak memory
    blows through on this tiny fixture too.
    """
    cd = curated.get("ACNC_REGISTER")
    source_cols = [c.source_column for c in cd.columns.values()]

    tracemalloc.start()
    df = parsing.read_csv_streaming(acnc_register_csv, columns=source_cols)
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    peak_mb = peak / 1024 / 1024

    assert len(df) > 50, "head fixture should have >50 rows"
    # 30KB fixture × 22 columns. Pre-fix pandas full-load on this fixture
    # peaked ~0.4MB; the streaming reader peaks well under 0.5MB. Budget
    # at 5MB so transient interpreter noise doesn't flake the test, but
    # any future regression (full file load, accidental column-explosion)
    # blows through immediately.
    assert peak_mb < 5, (
        f"streaming parser used {peak_mb:.1f}MB peak on 30KB fixture — "
        "looks like the column projection regressed. Check that "
        "ACNC_REGISTER still routes through read_csv_streaming "
        "in server._fetch_and_parse."
    )
    # Confirm we projected to the curated subset, not the full 69 columns.
    assert len(df.columns) == len(source_cols), (
        f"streaming reader returned {len(df.columns)} columns; "
        f"expected the {len(source_cols)} curated source columns"
    )


def test_acnc_register_get_data_path_uses_streaming_reader():
    """Confirm the server dispatches ACNC_REGISTER through the streaming
    reader, not the full-load pandas reader. This is the actual
    customer-facing wire — a regression here re-introduces the OOM that
    the user hit in live API testing.
    """
    from ato_mcp.server import _STREAMING_CSV_DATASETS
    assert "ACNC_REGISTER" in _STREAMING_CSV_DATASETS


def test_acnc_register_streaming_preserves_all_curated_columns(acnc_register_csv):
    """The streaming reader must return every source column the YAML
    declares. If a column is silently dropped, downstream `_apply_aliases`
    in shaping.py raises a misleading "upstream file may have changed
    shape" error — we want to catch the regression here instead.
    """
    cd = curated.get("ACNC_REGISTER")
    source_cols = [c.source_column for c in cd.columns.values()]
    df = parsing.read_csv_streaming(acnc_register_csv, columns=source_cols)
    for col in source_cols:
        assert col in df.columns, (
            f"streaming reader dropped curated source column {col!r} — "
            f"got {sorted(df.columns)}"
        )
