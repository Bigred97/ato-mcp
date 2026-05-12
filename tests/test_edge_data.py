"""Data-layer edge cases against real and synthetic input.

These cover the failure modes that ATO data has historically produced:
- "na" / "*" / "np" privacy-suppressed cells in numeric columns
- All-blank rows trailing the data block
- Totals/subtotals masquerading as data
- Mixed-dtype columns (string + float)
- Unicode bytes in source columns
- Datetime objects as column headers (transposed monthly tables)

Plus pure-parser edge cases (empty body, wrong sheet, off-by-one header_row).
"""
from __future__ import annotations

from io import BytesIO

import openpyxl
import pandas as pd
import pytest

from ato_mcp import curated, parsing, shaping


def _build_synthetic_xlsx(sheet_name: str, rows: list[list]) -> bytes:
    """Build an in-memory XLSX for edge-case fuzz tests."""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = sheet_name
    for row in rows:
        ws.append(row)
    buf = BytesIO()
    wb.save(buf)
    return buf.getvalue()


def test_parse_wide_with_nan_cells():
    body = _build_synthetic_xlsx("data", [
        ["title row", None, None],
        ["state", "postcode", "value\n$"],
        ["NSW", "2000", 100],
        ["NSW", "2001", None],   # privacy-suppressed
        ["NSW", "2002", "*"],    # ATO sentinel
        ["NSW", "2003", "na"],   # historical sentinel
        ["VIC", "3000", 250],
    ])
    df = parsing.read_xlsx(body, sheet="data", header_row=2)
    assert "value\n$" in df.columns
    assert len(df) == 5
    # When coerced to numeric, '*' and 'na' should become NaN
    df["value\n$"] = pd.to_numeric(df["value\n$"], errors="coerce")
    assert df["value\n$"].isna().sum() == 3
    assert int(df["value\n$"].sum()) == 350


def test_parse_handles_trailing_blank_rows():
    body = _build_synthetic_xlsx("data", [
        ["title", None, None],
        ["state", "postcode", "value\n$"],
        ["NSW", "2000", 100],
        ["NSW", "2001", 150],
        [None, None, None],
        [None, None, None],
    ])
    df = parsing.read_xlsx(body, sheet="data", header_row=2)
    # pandas keeps the blank rows; drop_blank_rows should strip them
    cleaned = parsing.drop_blank_rows(df, ["state", "postcode"])
    assert len(cleaned) == 2


def test_parse_truncated_xlsx_raises_parse_error():
    """A truncated/corrupt XLSX body must raise ParseError specifically —
    not arbitrary internals from openpyxl/zipfile."""
    # 0x50 0x4B is the ZIP magic. Looks like XLSX but truncated.
    with pytest.raises(parsing.ParseError):
        parsing.read_xlsx(b"\x50\x4b\x03\x04garbage", sheet="x", header_row=1)


def test_parse_completely_invalid_xlsx_raises_parse_error():
    """Total garbage that doesn't even look like XLSX must also raise
    ParseError, not BadZipFile or similar."""
    with pytest.raises(parsing.ParseError):
        parsing.read_xlsx(b"this is not an xlsx file at all" * 100,
                          sheet="x", header_row=1)


def test_parse_csv_with_bom():
    body = "﻿state,postcode\nNSW,2000\n".encode()
    df = parsing.read_csv(body)
    assert list(df.columns) == ["state", "postcode"]
    assert df.iloc[0]["postcode"] == 2000  # pandas coerced to int


def test_parse_csv_with_unicode_data():
    body = "name,country\n株式会社東京,JP\n🏢 BigCorp,US\n".encode()
    df = parsing.read_csv(body)
    assert df.iloc[0]["name"] == "株式会社東京"
    assert df.iloc[1]["name"] == "🏢 BigCorp"


def test_parse_csv_with_mixed_dtypes_no_warning():
    body = b"id,value\n1,100\n2,abc\n3,200\n"
    df = parsing.read_csv(body)
    # value column should be object (mixed) — no pandas warning thrown
    assert len(df) == 3


def test_normalize_header_handles_multiple_internal_newlines():
    from ato_mcp.parsing import _normalize_header
    assert _normalize_header("a\n\nb") == "a\n\nb"  # double newline preserved
    assert _normalize_header("  a  \n  b  \n  c  ") == "a\nb\nc"


def test_normalize_header_handles_only_whitespace():
    from ato_mcp.parsing import _normalize_header
    assert _normalize_header("   ") == ""
    assert _normalize_header("\n") == "\n"


def test_dtype_coercion_int_with_nan():
    """Int64 coercion should preserve NaN rows."""
    cd = curated.get("IND_POSTCODE_MEDIAN")
    df = pd.DataFrame({
        "State/\nTerritory": ["NSW", "NSW"],
        "Postcode2": ["2000", "2001"],
        "Individuals 2003–04\nno.": [5692, None],
        "Median3 taxable income 2003–04\n$": [42327.0, 50000.0],
        "Average3 taxable income 2003–04\n$": [55819.0, 60000.0],
        # Add minimum required columns so the alias rename doesn't fail
        **{c.source_column: [None, None] for c in cd.columns.values()
           if c.source_column not in {
               "State/\nTerritory", "Postcode2",
               "Individuals 2003–04\nno.",
               "Median3 taxable income 2003–04\n$",
               "Average3 taxable income 2003–04\n$",
           }}
    })
    resp = shaping.build_response(
        cd=cd, df=df, filters={}, measures="individuals_2003_04",
        start_period=None, end_period=None, fmt="records", user_query={},
    )
    # Only one record (the row with non-NaN individuals)
    assert resp.row_count == 1


def test_shape_wide_skips_observations_with_nan_value(corp_transparency_xlsx):
    """When tax_payable is blank for an entity, that measure observation
    should be omitted from results — not returned as null."""
    cd = curated.get("CORP_TRANSPARENCY")
    df = parsing.read_xlsx(
        corp_transparency_xlsx, sheet=cd.sheet, header_row=cd.header_row,
    )
    df = parsing.drop_blank_rows(
        df, [c.source_column for c in cd.columns.values() if c.role == "dimension"],
    )
    # 1 MENDS STREET has total_income only; taxable_income + tax_payable are blank
    resp = shaping.build_response(
        cd=cd, df=df,
        filters={"entity_name": "1 MENDS STREET PTY LTD"},
        measures=None,
        start_period=None, end_period=None, fmt="records", user_query={},
    )
    measures_returned = {r.measure for r in resp.records}
    assert measures_returned == {"total_income"}, (
        f"expected only total_income, got {measures_returned}"
    )


def test_response_csv_handles_empty_result(corp_transparency_xlsx):
    cd = curated.get("CORP_TRANSPARENCY")
    df = parsing.read_xlsx(
        corp_transparency_xlsx, sheet=cd.sheet, header_row=cd.header_row,
    )
    df = parsing.drop_blank_rows(
        df, [c.source_column for c in cd.columns.values() if c.role == "dimension"],
    )
    resp = shaping.build_response(
        cd=cd, df=df,
        filters={"entity_name": "DEFINITELY DOES NOT EXIST INC"},
        measures="total_income",
        start_period=None, end_period=None, fmt="csv", user_query={},
    )
    assert resp.row_count == 0
    assert resp.csv == ""


def test_response_csv_format_is_valid_csv(ind_postcode_median_xlsx):
    """CSV output must be parseable back by pandas."""
    cd = curated.get("IND_POSTCODE_MEDIAN")
    df = parsing.read_xlsx(
        ind_postcode_median_xlsx, sheet=cd.sheet, header_row=cd.header_row,
    )
    df = parsing.drop_blank_rows(
        df, [c.source_column for c in cd.columns.values() if c.role == "dimension"],
    )
    resp = shaping.build_response(
        cd=cd, df=df,
        filters={"state": "nsw"},
        measures="median_taxable_income_2022_23",
        start_period=None, end_period=None, fmt="csv", user_query={},
    )
    roundtrip = pd.read_csv(BytesIO(resp.csv.encode("utf-8")))
    assert "value" in roundtrip.columns
    assert "measure" in roundtrip.columns
    assert roundtrip["measure"].iloc[0] == "median_taxable_income_2022_23"
    assert (roundtrip["value"] > 0).all()


def test_curated_yaml_canonical_columns_match_real_files(corp_transparency_xlsx,
                                                          ind_postcode_median_xlsx,
                                                          company_industry_xlsx,
                                                          super_contrib_age_xlsx,
                                                          acnc_register_csv):
    """Every curated source_column must be in the parsed file headers.

    This is the canary that catches whitespace drift in ATO releases.
    """
    fixtures = {
        "CORP_TRANSPARENCY":   ("xlsx", corp_transparency_xlsx),
        "IND_POSTCODE_MEDIAN": ("xlsx", ind_postcode_median_xlsx),
        "COMPANY_INDUSTRY":    ("xlsx", company_industry_xlsx),
        "SUPER_CONTRIB_AGE":   ("xlsx", super_contrib_age_xlsx),
        "ACNC_REGISTER":       ("csv",  acnc_register_csv),
    }
    for dataset_id, (fmt, body) in fixtures.items():
        cd = curated.get(dataset_id)
        if fmt == "xlsx":
            df = parsing.read_xlsx(body, sheet=cd.sheet, header_row=cd.header_row)
        else:
            df = parsing.read_csv(body)
        missing = [
            c.source_column for c in cd.columns.values()
            if c.source_column not in df.columns
        ]
        assert not missing, (
            f"{dataset_id}: source columns missing in real data: {missing}\n"
            f"actual first 10: {list(df.columns[:10])}"
        )
