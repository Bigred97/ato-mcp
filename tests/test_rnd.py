"""Tests for the RND_INCENTIVE curated dataset.

The R&D Tax Incentive transparency report — every entity that lodged an
R&D claim with their dollar amount. Sellable for fintech / due-diligence /
innovation-policy buyers.
"""
from __future__ import annotations

from ato_mcp import curated, parsing, shaping


def _parse(cd, body):
    df = parsing.read_xlsx(
        body, sheet=cd.sheet, header_row=cd.header_row,
        data_start_row=cd.data_start_row, max_rows=cd.max_rows,
    )
    dim_cols = [c.source_column for c in cd.columns.values() if c.role == "dimension"]
    return parsing.drop_blank_rows(df, dim_cols)


def test_rnd_yaml_loads():
    cd = curated.get("RND_INCENTIVE")
    assert cd is not None
    assert cd.layout == "wide"
    assert cd.discovery is not None
    measure_keys = {c.key for c in cd.columns.values() if c.role == "measure"}
    assert "rnd_expenditure_aud" in measure_keys


def test_rnd_full_load(rnd_incentive_xlsx):
    cd = curated.get("RND_INCENTIVE")
    df = _parse(cd, rnd_incentive_xlsx)
    # ~13,000 entities in the 2022-23 file
    assert 10_000 < len(df) < 20_000


def test_rnd_atlassian_is_top_claimant(rnd_incentive_xlsx):
    """Sanity: Atlassian is publicly known as one of Australia's biggest
    R&D spenders. In 2022-23 they were #1."""
    cd = curated.get("RND_INCENTIVE")
    df = _parse(cd, rnd_incentive_xlsx)
    resp = shaping.build_response(
        cd=cd, df=df, filters={}, measures="rnd_expenditure_aud",
        start_period=None, end_period=None, fmt="records", user_query={},
    )
    sorted_records = sorted(
        [r for r in resp.records if r.value is not None],
        key=lambda r: r.value, reverse=True,
    )
    top_3_names = [r.dimensions["company_name"].lower() for r in sorted_records[:3]]
    assert any("atlassian" in n for n in top_3_names), (
        f"Atlassian missing from top 3: {top_3_names}"
    )
    # Top claim should be > $100M
    assert sorted_records[0].value > 100_000_000


def test_rnd_sector_total_in_billions(rnd_incentive_xlsx):
    """Total R&D claimed across all entities should be in the tens of billions."""
    cd = curated.get("RND_INCENTIVE")
    df = _parse(cd, rnd_incentive_xlsx)
    resp = shaping.build_response(
        cd=cd, df=df, filters={}, measures="rnd_expenditure_aud",
        start_period=None, end_period=None, fmt="records", user_query={},
    )
    total = sum(r.value for r in resp.records if r.value is not None)
    # Australia's annual R&D Tax Incentive claims are well-known to be in the $10–25B range
    assert 8_000_000_000 < total < 30_000_000_000


def test_rnd_abn_is_clean_string(rnd_incentive_xlsx):
    """ABN should come through as digits-only, no trailing .0 from float coercion."""
    cd = curated.get("RND_INCENTIVE")
    df = _parse(cd, rnd_incentive_xlsx)
    resp = shaping.build_response(
        cd=cd, df=df, filters={}, measures="rnd_expenditure_aud",
        start_period=None, end_period=None, fmt="records", user_query={},
    )
    abns = [r.dimensions.get("abn") for r in resp.records[:50] if r.dimensions.get("abn")]
    assert all("." not in abn for abn in abns), f"some ABNs have .0: {[a for a in abns if '.' in a][:3]}"


def test_rnd_canary_source_columns_match(rnd_incentive_xlsx):
    cd = curated.get("RND_INCENTIVE")
    df = parsing.read_xlsx(rnd_incentive_xlsx, sheet=cd.sheet, header_row=cd.header_row)
    missing = [c.source_column for c in cd.columns.values() if c.source_column not in df.columns]
    assert not missing, f"source columns missing: {missing}"
