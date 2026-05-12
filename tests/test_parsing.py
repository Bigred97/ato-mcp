"""Parsing contract tests against real ATO sample files."""
from __future__ import annotations

import pytest

from ato_mcp.parsing import (
    ParseError,
    _normalize_header,
    drop_blank_rows,
    read_csv,
    read_xlsx,
)


def test_read_xlsx_corp_transparency(corp_transparency_xlsx):
    df = read_xlsx(corp_transparency_xlsx, sheet="Income tax details", header_row=1)
    assert "Name" in df.columns
    assert "ABN" in df.columns
    assert "Total income $" in df.columns
    assert len(df) > 1000  # 4,200 entities in the 2023-24 file


def test_read_xlsx_normalizes_newline_padding(ind_postcode_median_xlsx):
    df = read_xlsx(ind_postcode_median_xlsx, sheet="Table 8", header_row=2)
    # Canonical no-padding form
    assert "State/\nTerritory" in df.columns
    assert "Postcode2" in df.columns
    # 2022-23 median taxable income column
    assert any("2022–23" in c and "Median" in c for c in df.columns)


def test_read_xlsx_bad_sheet_raises(corp_transparency_xlsx):
    with pytest.raises(ParseError, match="not found"):
        read_xlsx(corp_transparency_xlsx, sheet="NotASheet", header_row=1)


def test_read_xlsx_bad_header_row_raises(corp_transparency_xlsx):
    with pytest.raises(ParseError, match="1-indexed"):
        read_xlsx(corp_transparency_xlsx, sheet="Income tax details", header_row=0)


def test_read_xlsx_empty_body_raises():
    with pytest.raises(ParseError, match="empty"):
        read_xlsx(b"", sheet="x", header_row=1)


def test_read_csv_acnc(acnc_register_csv):
    df = read_csv(acnc_register_csv)
    assert "ABN" in df.columns
    assert "Charity_Legal_Name" in df.columns
    assert "State" in df.columns
    assert "Charity_Size" in df.columns
    assert len(df) > 50  # head sample has ~129 rows


def test_read_csv_empty_body_raises():
    with pytest.raises(ParseError, match="empty"):
        read_csv(b"")


def test_normalize_header_strips_padding_around_newline():
    assert _normalize_header("Individuals  \n  no.") == "Individuals\nno."
    assert _normalize_header("Individuals\n  no.") == "Individuals\nno."
    assert _normalize_header("Individuals  \nno.") == "Individuals\nno."


def test_normalize_header_preserves_internal_spaces():
    assert _normalize_header("Other sales of goods\n$") == "Other sales of goods\n$"


def test_normalize_header_passthrough_non_string():
    import datetime
    dt = datetime.datetime(2024, 1, 1)
    assert _normalize_header(dt) == dt


def test_drop_blank_rows(corp_transparency_xlsx):
    df = read_xlsx(corp_transparency_xlsx, sheet="Income tax details", header_row=1)
    before = len(df)
    cleaned = drop_blank_rows(df, ["Name", "ABN"])
    # Sample should have minimal blank rows; both should be close
    assert len(cleaned) <= before
    assert len(cleaned) > 1000


def test_drop_blank_rows_no_matching_keys_passthrough(corp_transparency_xlsx):
    df = read_xlsx(corp_transparency_xlsx, sheet="Income tax details", header_row=1)
    out = drop_blank_rows(df, ["nonexistent_col"])
    # No matching key columns → return df unchanged
    assert len(out) == len(df)
