"""Discovery module tests.

Discovery is the auto-update path: it resolves a fresh CKAN URL at fetch
time so when ATO publishes a new yearly release, the curated YAML doesn't
need a code change. The contract is strict:

  - On success: return the freshest matching URL.
  - On any failure (network, malformed CKAN, no match, wrong year): raise
    DiscoveryError. Callers MUST fall back to the YAML default.

These tests use respx so they're fast (no live network).
"""
from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest
import respx

from ato_mcp.cache import Cache
from ato_mcp.client import ATOClient
from ato_mcp.discovery import (
    DiscoveryError,
    DiscoverySpec,
    _pick_resource,
    _year_from_text,
    resolve_latest_url,
)


@pytest.fixture
def fresh_cache(tmp_path: Path) -> Cache:
    return Cache(tmp_path / "cache.db")


# ---------------------------------------------------------------------------
# Helper unit tests (no network)
# ---------------------------------------------------------------------------

def test_year_from_text_extracts_highest():
    # 4-digit substring is used as the year; ATO's "YYYY-YY" notation means
    # the start year (2022, 2023) is what we see — that's fine for ranking.
    assert _year_from_text("Taxation Statistics 2022-23") == 2022
    assert _year_from_text("2023-24 Report of Entity Tax Information") == 2023
    # If multiple 4-digit years are present, the highest wins
    assert _year_from_text("Released 2019, covers 2024 data") == 2024
    assert _year_from_text("no year here") is None
    assert _year_from_text("") is None
    assert _year_from_text(None) is None  # type: ignore[arg-type]


def test_pick_resource_exact_name_match():
    resources = [
        {"name": "Individuals - Table 5", "url": "https://a/file.xlsx"},
        {"name": "Individuals - Table 6", "url": "https://a/file6.xlsx"},
        {"name": "Individuals - Table 7", "url": "https://a/file7.xlsx"},
    ]
    spec = DiscoverySpec(package_id="x", resource_name="Individuals - Table 6")
    m = _pick_resource(resources, spec)
    assert m is not None
    assert m["url"] == "https://a/file6.xlsx"


def test_pick_resource_pattern_picks_highest_year():
    resources = [
        {"name": "2021-22 Report of Entity Tax Information", "url": "https://a/2021.xlsx"},
        {"name": "2023-24 Report of Entity Tax Information", "url": "https://a/2023.xlsx"},
        {"name": "2022-23 Report of Entity Tax Information", "url": "https://a/2022.xlsx"},
    ]
    spec = DiscoverySpec(
        package_id="x",
        resource_name_pattern=r"^\d{4}-\d{2} Report of Entity Tax Information$",
    )
    m = _pick_resource(resources, spec)
    assert m is not None
    assert m["url"] == "https://a/2023.xlsx"


def test_pick_resource_no_match_returns_none():
    resources = [{"name": "Other Resource", "url": "https://a/other.xlsx"}]
    spec = DiscoverySpec(package_id="x", resource_name="Missing One")
    assert _pick_resource(resources, spec) is None


def test_pick_resource_skips_non_dict_entries():
    resources = [
        "not a dict",  # type: ignore[list-item]
        None,
        {"name": "Right One", "url": "https://a/file.xlsx"},
    ]
    spec = DiscoverySpec(package_id="x", resource_name="Right One")
    m = _pick_resource(resources, spec)
    assert m is not None
    assert m["url"] == "https://a/file.xlsx"


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_resolve_requires_package_id_or_pattern(fresh_cache: Cache):
    async with ATOClient(cache=fresh_cache) as client:
        with pytest.raises(DiscoveryError, match="package_id"):
            await resolve_latest_url(
                client,
                DiscoverySpec(resource_name="x"),
            )


@pytest.mark.asyncio
async def test_resolve_requires_resource_name_or_pattern(fresh_cache: Cache):
    async with ATOClient(cache=fresh_cache) as client:
        with pytest.raises(DiscoveryError, match="resource_name"):
            await resolve_latest_url(
                client,
                DiscoverySpec(package_id="x"),
            )


@pytest.mark.asyncio
async def test_resolve_package_pattern_requires_org_id(fresh_cache: Cache):
    async with ATOClient(cache=fresh_cache) as client:
        with pytest.raises(DiscoveryError, match="organization_id"):
            await resolve_latest_url(
                client,
                DiscoverySpec(
                    package_id_pattern=r"^foo-(\d{4})$",
                    resource_name="x",
                ),
            )


# ---------------------------------------------------------------------------
# Happy paths via respx
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@respx.mock
async def test_resolve_with_exact_package_id_and_name(fresh_cache: Cache):
    respx.get(
        url__startswith="https://data.gov.au/data/api/3/action/package_show",
    ).mock(
        return_value=httpx.Response(200, json={
            "success": True,
            "result": {
                "name": "corporate-transparency",
                "resources": [
                    {"name": "2021-22 Report", "url": "https://x/2021.xlsx"},
                    {"name": "2023-24 Report of Entity Tax Information", "url": "https://x/2023.xlsx"},
                    {"name": "2022-23 Report", "url": "https://x/2022.xlsx"},
                ],
            },
        })
    )
    async with ATOClient(cache=fresh_cache) as client:
        url = await resolve_latest_url(
            client,
            DiscoverySpec(
                package_id="corporate-transparency",
                resource_name="2023-24 Report of Entity Tax Information",
            ),
        )
    assert url == "https://x/2023.xlsx"


@pytest.mark.asyncio
@respx.mock
async def test_resolve_with_package_pattern_picks_latest_year(fresh_cache: Cache):
    # First call: package_search returns multiple packages
    respx.get(
        url__startswith="https://data.gov.au/data/api/3/action/package_search",
    ).mock(
        return_value=httpx.Response(200, json={
            "success": True,
            "result": {
                "results": [
                    {"name": "taxation-statistics-2020-21"},
                    {"name": "taxation-statistics-2022-23"},
                    {"name": "taxation-statistics-2021-22"},
                    {"name": "other-ato-dataset"},  # should be ignored
                ],
            },
        })
    )
    # Second call: package_show for the chosen package
    respx.get(
        url__startswith="https://data.gov.au/data/api/3/action/package_show",
    ).mock(
        return_value=httpx.Response(200, json={
            "success": True,
            "result": {
                "name": "taxation-statistics-2022-23",
                "resources": [
                    {"name": "Individuals - Table 6", "url": "https://x/t6.xlsx"},
                ],
            },
        })
    )
    async with ATOClient(cache=fresh_cache) as client:
        url = await resolve_latest_url(
            client,
            DiscoverySpec(
                organization_id="australiantaxationoffice",
                package_id_pattern=r"^taxation-statistics-(\d{4})-\d{2}$",
                resource_name="Individuals - Table 6",
            ),
        )
    assert url == "https://x/t6.xlsx"


# ---------------------------------------------------------------------------
# Failure paths — every one MUST raise DiscoveryError (callers fall back).
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@respx.mock
async def test_resolve_404_raises_discovery_error(fresh_cache: Cache):
    respx.get(
        url__startswith="https://data.gov.au/data/api/3/action/package_show",
    ).mock(return_value=httpx.Response(404))
    async with ATOClient(cache=fresh_cache) as client:
        with pytest.raises(DiscoveryError):
            await resolve_latest_url(
                client,
                DiscoverySpec(
                    package_id="missing-pkg",
                    resource_name="anything",
                ),
            )


@pytest.mark.asyncio
@respx.mock
async def test_resolve_no_matching_resource_raises(fresh_cache: Cache):
    respx.get(
        url__startswith="https://data.gov.au/data/api/3/action/package_show",
    ).mock(
        return_value=httpx.Response(200, json={
            "success": True,
            "result": {
                "name": "corporate-transparency",
                "resources": [{"name": "Other Resource", "url": "https://x/other.xlsx"}],
            },
        })
    )
    async with ATOClient(cache=fresh_cache) as client:
        with pytest.raises(DiscoveryError, match="no resource matched"):
            await resolve_latest_url(
                client,
                DiscoverySpec(
                    package_id="corporate-transparency",
                    resource_name="Missing Resource",
                ),
            )


@pytest.mark.asyncio
@respx.mock
async def test_resolve_no_matching_package_pattern_raises(fresh_cache: Cache):
    respx.get(
        url__startswith="https://data.gov.au/data/api/3/action/package_search",
    ).mock(
        return_value=httpx.Response(200, json={
            "success": True,
            "result": {"results": [{"name": "other-org-dataset"}]},
        })
    )
    async with ATOClient(cache=fresh_cache) as client:
        with pytest.raises(DiscoveryError, match="no package matched"):
            await resolve_latest_url(
                client,
                DiscoverySpec(
                    organization_id="australiantaxationoffice",
                    package_id_pattern=r"^taxation-statistics-(\d{4})-\d{2}$",
                    resource_name="Anything",
                ),
            )


@pytest.mark.asyncio
@respx.mock
async def test_resolve_malformed_url_raises(fresh_cache: Cache):
    respx.get(
        url__startswith="https://data.gov.au/data/api/3/action/package_show",
    ).mock(
        return_value=httpx.Response(200, json={
            "success": True,
            "result": {
                "name": "x",
                "resources": [{"name": "Right One", "url": "file:///etc/passwd"}],
            },
        })
    )
    async with ATOClient(cache=fresh_cache) as client:
        with pytest.raises(DiscoveryError, match="invalid url"):
            await resolve_latest_url(
                client,
                DiscoverySpec(package_id="x", resource_name="Right One"),
            )


# ---------------------------------------------------------------------------
# Server-side fallback: discovery failure must NOT break get_data.
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_server_falls_back_to_yaml_url_when_discovery_fails():
    """When CKAN is unreachable but the cache has the YAML-pinned URL,
    get_data should still succeed."""
    # This is exercised end-to-end by the customer_flows live tests;
    # the unit-level invariant is that _resolve_download_url returns
    # cd.download_url on DiscoveryError (see server.py).
    from ato_mcp.server import _resolve_download_url
    from ato_mcp import curated as cmod
    cmod.reset_registry()
    cd = cmod.get("CORP_TRANSPARENCY")
    # Make a fake client whose fetch_resource will always raise — i.e.
    # the discovery network path fails — and confirm we still get the
    # YAML download_url back as a fallback.
    cache = Cache(Path("/tmp/ato-mcp-discovery-fallback-test.db"))
    cache.db_path.unlink(missing_ok=True)
    async with ATOClient(cache=cache) as client:
        # No respx mock → real network calls would fail in CI without it,
        # but we don't need to make the call: we just need the discovery
        # path to error. Patch fetch_package to always raise.
        from unittest.mock import patch
        from ato_mcp.client import ATOAPIError
        async def boom(*a, **kw):
            raise ATOAPIError("mocked failure")
        with patch.object(ATOClient, "fetch_package", boom), \
             patch.object(ATOClient, "_fetch_cached", boom):
            url = await _resolve_download_url(cd, client)
    assert url == cd.download_url
    cache.db_path.unlink(missing_ok=True)
