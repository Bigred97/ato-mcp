"""Auto-discovery of the latest data.gov.au resource URL for a curated dataset.

When ATO publishes Taxation Statistics 2023-24 next year, the corresponding
data.gov.au package slug changes from `taxation-statistics-2022-23` to
`taxation-statistics-2023-24`, and every resource gets a fresh GUID-based
URL. Without discovery, the hard-coded YAML `download_url` would silently
keep serving the previous year's data.

The discovery layer fixes this by resolving the URL at query time:

  1. The curated YAML declares an optional `discovery:` block.
  2. At fetch time, `resolve_latest_url(client, spec)` walks CKAN to find
     the freshest matching resource.
  3. If discovery succeeds, the resolved URL replaces the YAML default.
  4. If discovery fails (network down, ATO renamed something, no match),
     the YAML's hard-coded `download_url` is used as a safe fallback.

Discovery results are cached for 1 hour via the existing cache layer
(`kind="catalog"`), so resolving once per session is the common case.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from .client import ATOAPIError, ATOClient


@dataclass(frozen=True)
class DiscoverySpec:
    """How to find the right resource on data.gov.au.

    Exactly one of `package_id` / `package_id_pattern` is required.
    Exactly one of `resource_name` / `resource_name_pattern` is required.

    Args:
        package_id: exact CKAN package slug (e.g. "corporate-transparency").
            Use this for datasets where every yearly release lives as a
            resource inside the same package.
        package_id_pattern: regex with one numeric capture group, applied
            to the org's package slugs. The capture is interpreted as a
            year and the highest-year match wins. Example:
            r"^taxation-statistics-(\\d{4})-\\d{2}$" — picks
            "taxation-statistics-2023-24" over "taxation-statistics-2022-23".
        organization_id: required when using package_id_pattern. The CKAN
            org slug to enumerate ("australiantaxationoffice", "acnc").
        resource_name: exact name match against `resource["name"]`.
        resource_name_pattern: regex match against `resource["name"]`.
            If multiple match, the one with the highest year (extracted
            from any 4-digit sequence in the name) wins.
    """
    package_id: str | None = None
    package_id_pattern: str | None = None
    organization_id: str | None = None
    resource_name: str | None = None
    resource_name_pattern: str | None = None


class DiscoveryError(Exception):
    """Raised when discovery cannot resolve a URL.

    Callers should catch this and fall back to the curated YAML's
    `download_url`. Discovery should never break a query — its job is to
    upgrade staleness, not introduce failure modes.
    """


_YEAR_IN_TEXT = re.compile(r"(\d{4})")


async def resolve_latest_url(client: ATOClient, spec: DiscoverySpec) -> str:
    """Resolve the freshest matching resource URL.

    Raises DiscoveryError on any failure path; callers fall back to the
    curated YAML's hard-coded URL in that case.
    """
    if not spec.package_id and not spec.package_id_pattern:
        raise DiscoveryError("DiscoverySpec needs package_id or package_id_pattern")
    if not spec.resource_name and not spec.resource_name_pattern:
        raise DiscoveryError("DiscoverySpec needs resource_name or resource_name_pattern")

    package_id = (
        spec.package_id
        if spec.package_id
        else await _resolve_latest_package_id(client, spec)
    )

    try:
        package = await client.fetch_package(package_id)
    except ATOAPIError as e:
        raise DiscoveryError(f"failed to fetch package {package_id!r}: {e}") from e

    resources = package.get("resources") or []
    if not isinstance(resources, list):
        raise DiscoveryError(f"package {package_id!r}: malformed resources field")

    match = _pick_resource(resources, spec)
    if match is None:
        raise DiscoveryError(
            f"package {package_id!r}: no resource matched "
            f"name={spec.resource_name!r} pattern={spec.resource_name_pattern!r}"
        )
    url = match.get("url")
    if not isinstance(url, str) or not url.startswith(("http://", "https://")):
        raise DiscoveryError(
            f"package {package_id!r}: resource has invalid url {url!r}"
        )
    return url


async def _resolve_latest_package_id(
    client: ATOClient, spec: DiscoverySpec
) -> str:
    if not spec.organization_id:
        raise DiscoveryError(
            "package_id_pattern requires organization_id (e.g. 'australiantaxationoffice')"
        )
    assert spec.package_id_pattern is not None  # checked above

    # Use the CKAN search endpoint via fetch_package's machinery — we share
    # the catalog cache TTL, so repeated lookups are cheap.
    url = (
        f"{client.base_url}/data/api/3/action/package_search"
        f"?fq=organization:{spec.organization_id}&rows=200&fl=name"
    )
    try:
        body = await client._fetch_cached(url, kind="catalog")  # type: ignore[attr-defined]
    except ATOAPIError as e:
        raise DiscoveryError(f"failed to list packages: {e}") from e

    try:
        import json
        payload = json.loads(body.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        raise DiscoveryError(f"package_search returned non-JSON: {e}") from e

    if not payload.get("success"):
        raise DiscoveryError(f"package_search failed: {payload.get('error')}")

    results = (payload.get("result") or {}).get("results") or []
    pattern = re.compile(spec.package_id_pattern)
    matches: list[tuple[int, str]] = []
    for entry in results:
        name = entry.get("name") if isinstance(entry, dict) else None
        if not isinstance(name, str):
            continue
        m = pattern.match(name)
        if not m:
            continue
        # First numeric capture group → year
        year_str = m.group(1) if m.groups() else None
        try:
            year = int(year_str) if year_str else 0
        except (TypeError, ValueError):
            year = 0
        matches.append((year, name))

    if not matches:
        raise DiscoveryError(
            f"no package matched pattern {spec.package_id_pattern!r} "
            f"for org {spec.organization_id!r}"
        )
    matches.sort(key=lambda t: -t[0])  # highest year first
    return matches[0][1]


def _pick_resource(resources: list[dict[str, Any]], spec: DiscoverySpec) -> dict | None:
    """Return the best-matching resource dict, or None."""
    candidates: list[tuple[int, dict]] = []
    for res in resources:
        if not isinstance(res, dict):
            continue
        name = res.get("name") or ""
        if not isinstance(name, str):
            continue
        if spec.resource_name is not None:
            if name == spec.resource_name:
                candidates.append((_year_from_text(name) or 0, res))
        elif spec.resource_name_pattern is not None:
            if re.search(spec.resource_name_pattern, name):
                candidates.append((_year_from_text(name) or 0, res))
    if not candidates:
        return None
    candidates.sort(key=lambda t: -t[0])
    return candidates[0][1]


def _year_from_text(s: str) -> int | None:
    """Extract the highest 4-digit year from a string, if any."""
    matches = _YEAR_IN_TEXT.findall(s or "")
    if not matches:
        return None
    try:
        years = [int(m) for m in matches]
    except ValueError:
        return None
    return max(years)
