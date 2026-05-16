"""XLSX and CSV parsers for ATO / ACNC resources on data.gov.au.

Two source formats:
  - XLSX (the vast majority of ATO Taxation Statistics tables) — usually one
    "Notes" sheet of preamble + one or more data sheets. Headers land on row 1,
    2, or 3 depending on the table; the curated YAML pins the exact row.
  - CSV (ACNC Register, a few ATO tables) — flat, headers on row 1.

We expose two simple readers. Higher-level coercion (rename to aliases,
melt transposed time series, type-convert columns) happens in `shaping.py`
guided by the curated table spec.

Why pandas: it deals with mixed dtypes, NA values, multi-row blanks, and
trailing-cell whitespace without us having to reinvent any of it. The cost
is the openpyxl read time for the bigger files (~7-8MB → ~1.5s cold load).

For the one outlier — ACNC_AIS_FINANCIALS, a 36MB / 91-column / 50k+ row
CSV — `read_csv_streaming` does a column-projected stdlib `csv.reader`
pass that keeps peak memory bounded (the full pandas load OOMs on 512MB
hosts). See the function docstring for the trade-offs.
"""
from __future__ import annotations

import csv
import zipfile
from collections.abc import Iterable
from io import BytesIO

import pandas as pd


class ParseError(Exception):
    """Raised when an ATO/ACNC resource can't be parsed."""


def read_xlsx(
    body: bytes,
    *,
    sheet: str,
    header_row: int,
    data_start_row: int | None = None,
    max_rows: int | None = None,
) -> pd.DataFrame:
    """Read one sheet from an XLSX as a DataFrame.

    Args:
        body: raw bytes of the .xlsx file.
        sheet: sheet name (must exist).
        header_row: 1-indexed row containing column headers (matches Excel's
            row numbering and the convention used in curated YAMLs).
        data_start_row: 1-indexed first row of data. Defaults to header_row + 1.
            Set this when there are blank/spacer rows between header and data.
        max_rows: cap on data rows returned (None = no limit). Useful when
            tables have trailing footnote rows.

    Returns:
        DataFrame indexed 0..N-1. Column names are the raw header strings
        (renaming to plain-English aliases happens in shaping.py).
    """
    if not body:
        raise ParseError("empty XLSX body")
    if header_row < 1:
        raise ParseError(f"header_row must be 1-indexed (>=1), got {header_row}")

    # pandas header= is 0-indexed; user-facing header_row is 1-indexed.
    pandas_header = header_row - 1

    try:
        df = pd.read_excel(
            BytesIO(body),
            sheet_name=sheet,
            header=pandas_header,
            engine="openpyxl",
        )
    except ValueError as e:
        # pandas raises ValueError("Worksheet named '...' not found")
        raise ParseError(f"sheet {sheet!r} not found in workbook: {e}") from e
    except (KeyError, OSError, zipfile.BadZipFile) as e:
        # openpyxl/zipfile raises BadZipFile on non-zip bodies, KeyError for
        # missing zip entries when truncated, and OSError on IO problems.
        # Wrap so callers see a uniform ParseError instead of arbitrary internals.
        raise ParseError(f"could not parse XLSX (corrupt or truncated body): {e}") from e

    # If data_start_row > header_row + 1 there's a spacer row to drop.
    if data_start_row is not None:
        if data_start_row < header_row + 1:
            raise ParseError(
                f"data_start_row ({data_start_row}) must be > header_row ({header_row})"
            )
        skip_after_header = data_start_row - header_row - 1
        if skip_after_header > 0:
            df = df.iloc[skip_after_header:].reset_index(drop=True)

    if max_rows is not None and len(df) > max_rows:
        df = df.iloc[:max_rows].reset_index(drop=True)

    df.columns = [_normalize_header(c) for c in df.columns]
    return df


def _normalize_header(c):
    """Normalize an XLSX column header.

    ATO headers carry meaning in embedded newlines (`Individuals\\nno.` =
    "Individuals" + units-suffix "no.") so we keep the newline. But we strip
    any padding whitespace around the newline because ATO ships several
    inconsistent variants of the same logical name across tables and years
    (e.g. `"Individuals \\nno."`, `"Individuals\\n no."`, `"Individuals  \\n  no."`).
    Normalizing here means curated YAMLs only ever spell one canonical form.
    """
    if not isinstance(c, str):
        return c
    parts = c.split("\n")
    parts = [p.strip() for p in parts]
    return "\n".join(parts)


def read_csv(body: bytes, *, encoding: str = "utf-8-sig") -> pd.DataFrame:
    """Read a CSV body as a DataFrame.

    ACNC's CSV uses UTF-8 with BOM and standard quoting — pandas handles it
    natively. We pass `low_memory=False` so mixed-dtype columns aren't
    silently coerced to `object` partway through parsing.
    """
    if not body:
        raise ParseError("empty CSV body")
    try:
        df = pd.read_csv(
            BytesIO(body),
            encoding=encoding,
            low_memory=False,
        )
    except UnicodeDecodeError as e:
        raise ParseError(f"CSV decode failed with encoding {encoding!r}: {e}") from e
    except pd.errors.ParserError as e:
        raise ParseError(f"CSV parse failed: {e}") from e

    df.columns = [_normalize_header(c) for c in df.columns]
    return df


def read_csv_streaming(
    body: bytes,
    *,
    columns: Iterable[str],
    encoding: str = "utf-8-sig",
    max_rows: int | None = None,
) -> pd.DataFrame:
    """Read a CSV body projecting to a subset of columns.

    Built for ACNC_AIS_FINANCIALS: a 36MB / 91-column CSV that OOMs on
    512MB hosts when loaded whole via `pd.read_csv`. Most of the source
    columns are unused — the YAML pins ~23 of the 91 — so we project
    those out at parse time via pandas' native `usecols=` argument.
    That skips materialising the 68 unused columns entirely and cuts
    peak parsing memory from ~160MB to ~85MB on the full 36MB file.

    "Streaming" is a slight misnomer — pandas' C engine still buffers
    the file — but it IS column-projected and never builds the full
    91-column frame. Compared to a hand-rolled stdlib `csv.reader`,
    pandas' usecols path is ~2x faster (uses the C parser) and uses
    a fraction of the Python-object overhead per cell.

    Behavioural parity with `read_csv` for the projected columns:
      - Headers are normalised via `_normalize_header` (whitespace-only
        change, preserves embedded newlines).
      - Empty strings become NaN (pandas default `na_values=['']`).
      - Numeric columns are auto-coerced by pandas (same as `read_csv`).

    Args:
        body: raw CSV bytes (UTF-8 with optional BOM).
        columns: iterable of source-column headers to KEEP. Any column
            not in this set is skipped at parse time.
        encoding: text encoding. Default 'utf-8-sig' strips a UTF-8 BOM.
        max_rows: cap on data rows returned (None = no limit).

    Returns:
        DataFrame with only the projected columns, in source-file order.

    Raises:
        ParseError: empty body, unsupported encoding, malformed CSV, or
            no requested column was found in the header row.
    """
    if not body:
        raise ParseError("empty CSV body")

    want_norm: set[str] = {_normalize_header(c) for c in columns}
    if not want_norm:
        raise ParseError("read_csv_streaming requires at least one column to keep")

    # Pre-scan the header line to figure out which requested columns
    # actually exist in the source. `usecols` raises ValueError if any
    # requested name is missing; we'd rather silently drop missing
    # columns so a YAML drift surfaces downstream as the existing
    # `_apply_aliases` ValueError ("expected these columns but they
    # were not in the parsed table: ...") with its actionable hint.
    try:
        first_newline = body.index(b"\n")
    except ValueError as e:
        raise ParseError("CSV has no header row") from e
    try:
        header_line = body[:first_newline].decode(encoding)
    except UnicodeDecodeError as e:
        raise ParseError(
            f"CSV header decode failed with encoding {encoding!r}: {e}"
        ) from e
    # `csv.reader` over a single line just splits on commas with quote
    # handling — cheaper than pulling pandas in for the header alone.
    headers = next(csv.reader([header_line]))
    available_norm_to_raw: dict[str, str] = {
        _normalize_header(h): h for h in headers
    }
    kept_raw = [available_norm_to_raw[n] for n in want_norm if n in available_norm_to_raw]
    if not kept_raw:
        raise ParseError(
            "CSV streaming projection matched no columns; "
            f"requested={sorted(want_norm)} headers={headers[:5]}..."
        )

    try:
        df = pd.read_csv(
            BytesIO(body),
            encoding=encoding,
            usecols=kept_raw,
            nrows=max_rows,
            low_memory=False,
        )
    except UnicodeDecodeError as e:
        raise ParseError(f"CSV decode failed with encoding {encoding!r}: {e}") from e
    except pd.errors.ParserError as e:
        raise ParseError(f"CSV parse failed: {e}") from e

    df.columns = [_normalize_header(c) for c in df.columns]
    return df


def drop_blank_rows(df: pd.DataFrame, key_columns: list[str]) -> pd.DataFrame:
    """Drop rows where every column in `key_columns` is NaN.

    Use this to trim trailing footnote / blank rows that ATO sometimes leaves
    after the data block. We require ALL key columns to be NaN before
    discarding — a single non-null in any key column means the row is real.
    """
    present = [c for c in key_columns if c in df.columns]
    if not present:
        return df
    keep_mask = ~df[present].isna().all(axis=1)
    return df.loc[keep_mask].reset_index(drop=True)
