"""Shared pytest fixtures.

Test fixtures load real ATO sample files from `tests/fixtures/`. These are
small (under 8MB total) so they live alongside the test suite. The 7.9MB
Individuals Table 6 is intentionally not bundled — its parsing is exercised
via the `live` marker tests.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from ato_mcp import curated

FIXTURE_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture(autouse=True)
def reset_curated_registry():
    """Force a fresh load of curated YAMLs before each test."""
    curated.reset_registry()
    yield
    curated.reset_registry()


@pytest.fixture
def fixture_dir() -> Path:
    return FIXTURE_DIR


@pytest.fixture
def corp_transparency_xlsx() -> bytes:
    return (FIXTURE_DIR / "corp_transparency_2023_24.xlsx").read_bytes()


@pytest.fixture
def ind_postcode_median_xlsx() -> bytes:
    return (FIXTURE_DIR / "ind_postcode_median.xlsx").read_bytes()


@pytest.fixture
def company_industry_xlsx() -> bytes:
    return (FIXTURE_DIR / "company_industry.xlsx").read_bytes()


@pytest.fixture
def super_contrib_age_xlsx() -> bytes:
    return (FIXTURE_DIR / "super_contrib_age.xlsx").read_bytes()


@pytest.fixture
def acnc_register_csv() -> bytes:
    """ACNC register CSV — head-only sample (~100 rows) so tests stay fast."""
    return (FIXTURE_DIR / "acnc_register_head.csv").read_bytes()


@pytest.fixture
def gst_monthly_xlsx() -> bytes:
    """GST/WET/LCT monthly collections — transposed-layout fixture."""
    return (FIXTURE_DIR / "gst_monthly.xlsx").read_bytes()


@pytest.fixture
def ato_occupation_xlsx() -> bytes:
    """Individuals by occupation × sex — ANZSCO 6-digit roles."""
    return (FIXTURE_DIR / "ato_occupation.xlsx").read_bytes()


@pytest.fixture
def smsf_annual_overview_xlsx() -> bytes:
    """SMSF Annual Overview — exercises sub-table extraction via max_rows."""
    return (FIXTURE_DIR / "smsf_annual_overview.xlsx").read_bytes()


@pytest.fixture
def sbb_benchmarks_xlsx() -> bytes:
    """ATO Small Business Benchmarks 2023-24 — 100 industries × ratio columns."""
    return (FIXTURE_DIR / "sbb_benchmarks.xlsx").read_bytes()


@pytest.fixture
def help_debt_xlsx() -> bytes:
    """ATO HELP / HECS annual debt statistics."""
    return (FIXTURE_DIR / "help_debt.xlsx").read_bytes()
