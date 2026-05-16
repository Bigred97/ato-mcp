"""FastMCP server entrypoint for ato-mcp.

Five tools, mirroring abs-mcp and rba-mcp so an agent that uses all three
gets a uniform shape:

  - search_datasets     — fuzzy search curated ATO/ACNC datasets
  - describe_dataset    — show columns, filters, allowed values for one dataset
  - get_data            — query a dataset with filters / measures / period
  - latest              — shortcut: last N observations (same query shape)
  - list_curated        — enumerate the curated dataset IDs

The MCP shape stays plain-English: users pass `{"state": "nsw"}` instead of
ATO's verbose source column header. Curated YAMLs do the translation.
"""
from __future__ import annotations

import asyncio
import difflib
import hashlib
import re
from collections import OrderedDict
from typing import Annotated, Any, Literal

import pandas as pd
from fastmcp import FastMCP
from pydantic import Field

from . import catalog, curated
from .client import ATOAPIError, ATOClient, get_stale_signal, reset_stale_signal
from .discovery import DiscoveryError, DiscoverySpec, resolve_latest_url
from .models import ColumnDetail, DataResponse, DatasetDetail, DatasetSummary, Observation
from .parsing import drop_blank_rows, read_csv, read_csv_streaming, read_xlsx
from .shaping import build_response

# CSV datasets where pd.read_csv() OOMs on small-RAM hosts. We swap them
# onto the column-projected streaming reader. Keep this list narrow —
# default behaviour stays full-load pandas for every other CSV.
#
# - ACNC_AIS_FINANCIALS: 36MB / 91 columns / 50k+ rows. Full pd.read_csv()
#   peaks at ~1.15GB; projected to ~23 curated columns it peaks <100MB.
_STREAMING_CSV_DATASETS = frozenset({"ACNC_AIS_FINANCIALS"})

# Curated IDs are uppercase letters + digits + underscore.
_DATASET_ID_PATTERN = re.compile(r"^[A-Z][A-Z0-9_]*$")
# Period strings: YYYY, YYYY-MM, YYYY-YY (financial year), or compound up to YYYY-MM-DD.
_PERIOD_PATTERN = re.compile(r"^[0-9-]{4,10}$")
_VALID_FORMATS = {"records", "series", "csv"}

mcp = FastMCP("ato-mcp")

_client: ATOClient | None = None
_client_lock = asyncio.Lock()

# Parsed-DataFrame cache. The byte cache already short-circuits the network,
# but pandas/openpyxl still re-parses bytes on every warm call — for the
# 7.9MB IND_POSTCODE that's ~4s of pure CPU. We cache the post-parse,
# post-drop_blank_rows DataFrame in-process so repeat queries land in ~50ms.
# Bounded LRU; eviction keeps memory under ~150-300MB across all entries.
_DF_CACHE_MAX_ENTRIES = 8
_df_cache: OrderedDict[tuple, pd.DataFrame] = OrderedDict()
_df_cache_lock = asyncio.Lock()


def reset_df_cache_for_tests() -> None:
    """Drop the parsed-DataFrame cache. Tests use this to start from clean."""
    _df_cache.clear()


def _suggest_dataset_id(bad: str) -> str:
    """Return a 'Did you mean X?' fragment for an unknown dataset ID.

    Uses difflib to find the closest curated ID (case-insensitive). Returns
    an empty string when no close match exists so the caller can append it
    unconditionally.
    """
    if not isinstance(bad, str) or not bad.strip():
        return ""
    candidates = curated.list_ids()
    norm = bad.strip().upper()
    matches = difflib.get_close_matches(norm, candidates, n=1, cutoff=0.6)
    if not matches:
        return ""
    return f"Did you mean {matches[0]!r}? "


async def _get_client() -> ATOClient:
    global _client
    async with _client_lock:
        if _client is None:
            _client = ATOClient()
        return _client


async def reset_client_for_tests() -> None:
    """Drop the cached client. Tests that span event loops must clear it."""
    global _client
    if _client is not None:
        try:
            await _client.aclose()
        except Exception:
            pass
        _client = None


def _normalize_dataset_id(dataset_id: Any) -> str:
    if not isinstance(dataset_id, str):
        raise ValueError(
            f"dataset_id must be a string, got {type(dataset_id).__name__}. "
            "Try search_datasets() or list_curated() to discover IDs."
        )
    norm = dataset_id.strip().upper()
    if not norm:
        raise ValueError(
            "dataset_id is empty. Try list_curated() to see available IDs."
        )
    if not _DATASET_ID_PATTERN.match(norm):
        raise ValueError(
            f"dataset_id {dataset_id!r} contains invalid characters — "
            "ato-mcp IDs use uppercase letters, digits, and underscores "
            "(e.g. 'IND_POSTCODE', 'COMPANY_INDUSTRY')."
        )
    return norm


def _validate_filters(filters: Any) -> dict[str, Any]:
    if filters is None:
        return {}
    if isinstance(filters, str):
        import json as _json
        try:
            filters = _json.loads(filters)
        except _json.JSONDecodeError as exc:
            raise ValueError(
                f"filters must be a JSON object, got invalid JSON string: {exc}. "
                "Example: {\"state\": \"nsw\", \"postcode\": \"2000\"}."
            ) from exc
    if not isinstance(filters, dict):
        raise ValueError(
            f"filters must be a dict, got {type(filters).__name__}. "
            "Example: {'state': 'nsw', 'postcode': '2000'}."
        )
    return filters


def _validate_period(value: Any, field_name: str) -> str | None:
    if value is None:
        return None
    # LLM clients routinely send JSON ints (e.g. {"start_period": 2024}). Coerce
    # 4-digit ints in a realistic year range to the canonical "YYYY" string at the
    # boundary so we don't surface a confusing type error downstream.
    if isinstance(value, bool):
        # bool is a subclass of int; reject it explicitly before the int branch.
        raise ValueError(
            f"{field_name} must be a string or int year, got bool. "
            f"Try {field_name}='2024' (year), '2024-06' (month), '2022-23' (FY), "
            "or 2024 (int year)."
        )
    if isinstance(value, int):
        if 1900 <= value <= 2100:
            value = str(value)
        else:
            raise ValueError(
                f"{field_name} integer {value} out of range. "
                f"For year-only periods pass a 4-digit year like 2024, or use string "
                f"forms 'YYYY' (e.g. '2024'), 'YYYY-MM' (e.g. '2024-06'), or "
                f"'YYYY-YY' (ATO FY, e.g. '2022-23'). Try {field_name}='2024'."
            )
    if not isinstance(value, str):
        raise ValueError(
            f"{field_name} must be a string or int year, got {type(value).__name__}. "
            "Try a year ('2024' or 2024), a month ('2024-06'), or an ATO financial "
            "year ('2022-23'). Example: "
            "get_data('GST_MONTHLY', start_period='2024', end_period='2024-06')."
        )
    s = value.strip()
    if not s:
        return None
    if not _PERIOD_PATTERN.match(s):
        raise ValueError(
            f"{field_name} {value!r} has invalid format. "
            "Use 'YYYY' (e.g. '2024'), 'YYYY-MM' (e.g. '2024-06'), or "
            "an ATO financial year like '2022-23'. Did you mean "
            f"{s[:4] if s[:4].isdigit() else '2024'!r}? "
            "Example: get_data('GST_MONTHLY', start_period='2024-01', end_period='2024-06')."
        )
    return s


def _validate_measures(measures: Any) -> str | list[str] | None:
    if measures is None:
        return None
    if isinstance(measures, str):
        s = measures.strip()
        if not s:
            raise ValueError(
                "measures is empty. Pass a measure key like 'median_taxable_income', "
                "or omit `measures` to return all curated measures."
            )
        return s
    if isinstance(measures, list):
        if not measures:
            raise ValueError(
                "measures is an empty list. Pass at least one measure, "
                "or omit `measures` to return all."
            )
        out: list[str] = []
        for m in measures:
            if not isinstance(m, str):
                raise ValueError(
                    f"measures list entries must be strings, got {type(m).__name__}. "
                    "Try a measure key from describe_dataset(). "
                    "Example: measures=['total_income', 'tax_payable']."
                )
            s = m.strip()
            if not s:
                raise ValueError(
                    "measures list contains an empty string. "
                    "Try a measure key from describe_dataset() — "
                    "e.g. measures=['total_income', 'tax_payable'] — "
                    "or omit `measures` to return all."
                )
            out.append(s)
        return out
    raise ValueError(
        f"measures must be a string or list of strings, got {type(measures).__name__}. "
        "Try describe_dataset(dataset_id) to discover valid measure keys. "
        "Example: measures='total_income' or measures=['total_income', 'tax_payable']."
    )


async def _resolve_download_url(cd: curated.CuratedDataset, client: ATOClient) -> str:
    """If the curated YAML declares a discovery block, try to resolve a fresh
    URL via CKAN. On any failure, silently fall back to the YAML default —
    discovery upgrades staleness; it must not introduce new failure modes.
    """
    if not cd.discovery:
        return cd.download_url
    try:
        spec = DiscoverySpec(
            package_id=cd.discovery.get("package_id"),
            package_id_pattern=cd.discovery.get("package_id_pattern"),
            organization_id=cd.discovery.get("organization_id"),
            resource_name=cd.discovery.get("resource_name"),
            resource_name_pattern=cd.discovery.get("resource_name_pattern"),
        )
        return await resolve_latest_url(client, spec)
    except DiscoveryError:
        return cd.download_url


async def _fetch_and_parse(cd: curated.CuratedDataset, *, kind: str = "data"):
    """Download the dataset's primary resource and parse it into a DataFrame.

    The parsed DataFrame is cached in-process keyed by (url, parse-spec, body
    content hash). The hash makes the cache content-aware: if the byte cache
    serves stale bytes that get refreshed, the hash differs and we re-parse.
    """
    client = await _get_client()
    url = await _resolve_download_url(cd, client)
    try:
        body = await client.fetch_resource(url, kind=kind)  # type: ignore[arg-type]
    except ATOAPIError as e:
        raise ValueError(
            f"Could not fetch dataset {cd.id} from data.gov.au. ({e})"
        ) from e

    # Content-aware cache key. We can't hash the whole body on every warm call
    # (sha256 over 8MB is ~30ms — defeats the perf benefit), so we use a
    # 3-part signature: total byte length + hash of head + hash of tail. Same
    # length AND same head AND same tail = same file in practice (XLSX is a
    # zip; appending or truncating shifts both length and tail hash).
    head = body[:8192]
    tail = body[-2048:] if len(body) > 8192 else b""
    body_sig = hashlib.sha256(head + tail).digest()
    cache_key = (
        url, cd.format, cd.sheet, cd.header_row, cd.data_start_row,
        len(body), body_sig,
    )

    async with _df_cache_lock:
        cached = _df_cache.get(cache_key)
        if cached is not None:
            _df_cache.move_to_end(cache_key)
            return cached

    if cd.format == "csv":
        if cd.id in _STREAMING_CSV_DATASETS:
            # Column-projected streaming reader — drops unused columns at
            # parse time so a 36MB / 91-col CSV doesn't bloat a DataFrame
            # to >1GB. See parsing.read_csv_streaming for details.
            source_cols = [c.source_column for c in cd.columns.values()]
            df = read_csv_streaming(
                body, columns=source_cols, max_rows=cd.max_rows,
            )
        else:
            df = read_csv(body)
    else:
        if cd.sheet is None:
            raise ValueError(
                f"Dataset {cd.id!r} declares format='xlsx' but has no sheet name. "
                "Fix the curated YAML."
            )
        df = read_xlsx(
            body,
            sheet=cd.sheet,
            header_row=cd.header_row,
            data_start_row=cd.data_start_row,
            max_rows=cd.max_rows,
        )
    # Trim trailing blank rows where every dimension is NaN.
    dim_source_cols = [c.source_column for c in cd.columns.values() if c.role == "dimension"]
    if dim_source_cols:
        df = drop_blank_rows(df, dim_source_cols)

    async with _df_cache_lock:
        _df_cache[cache_key] = df
        _df_cache.move_to_end(cache_key)
        while len(_df_cache) > _DF_CACHE_MAX_ENTRIES:
            _df_cache.popitem(last=False)

    return df


@mcp.tool
async def search_datasets(
    query: Annotated[
        str,
        Field(
            description=(
                "Free-text search query. Matches against dataset IDs, names, "
                "descriptions, and curated search keywords. Case-insensitive."
            ),
            examples=[
                "postcode tax",
                "company industry",
                "charity register",
                "corporate tax",
                "gst collections",
                "super contributions",
            ],
        ),
    ],
    limit: Annotated[
        int,
        Field(
            description="Maximum number of results to return, ranked by relevance.",
            examples=[5, 10],
            ge=1,
            le=50,
        ),
    ] = 10,
) -> list[DatasetSummary]:
    """Fuzzy-search the curated ATO/ACNC dataset catalog.

    All datasets ship hand-curated in v0.1: personal tax by postcode, company
    tax by industry, corporate tax transparency, GST collections, super
    contributions by age, the ACNC charity register, and more.

    Examples:
        # Find the dataset that gives tax stats by postcode
        results = await search_datasets("postcode tax")
        # → [{id: 'IND_POSTCODE', name: 'Individuals by Postcode', ...}]

        # Discover what's available on charities
        results = await search_datasets("charity")

    Returns:
        List of DatasetSummary (id, name, description, update_frequency,
        is_curated), ranked by relevance.
    """
    if not isinstance(query, str):
        raise ValueError(
            f"query must be a string, got {type(query).__name__}. "
            "Try 'postcode', 'company', 'charity', 'gst', or 'super'."
        )
    if not query.strip():
        raise ValueError(
            "query is required. Try 'postcode', 'company', 'charity', "
            "'gst', 'super', or any other ATO topic."
        )
    if isinstance(limit, bool) or not isinstance(limit, int):
        raise ValueError(
            f"limit must be a positive integer, got {limit!r} ({type(limit).__name__}). "
            "Try limit=10 (default) or any int between 1 and 50. "
            "Example: search_datasets('postcode', limit=5)."
        )
    if limit < 1:
        raise ValueError(
            f"limit must be >= 1, got {limit}. "
            "Try limit=10 (default) or any int between 1 and 50. "
            "Example: search_datasets('postcode', limit=5)."
        )
    return catalog.search(query, limit=limit)


@mcp.tool
async def describe_dataset(
    dataset_id: Annotated[
        str,
        Field(
            description=(
                "Curated dataset ID. Use search_datasets() to discover or "
                "list_curated() to enumerate. Case-insensitive."
            ),
            examples=[
                "IND_POSTCODE",
                "COMPANY_INDUSTRY",
                "CORP_TRANSPARENCY",
                "ACNC_REGISTER",
                "GST_MONTHLY",
            ],
        ),
    ],
) -> DatasetDetail:
    """Describe a dataset's filterable dimensions, returnable measures, units, and source.

    Use this before calling get_data on a new dataset — it tells you the
    valid filter keys ('state', 'postcode', 'industry'), the valid filter
    values ('nsw', 'vic'), the measure aliases ('median_taxable_income'),
    and the canonical source URL.

    Returns:
        DatasetDetail with id, name, description, period_coverage, list of
        dimensions, list of measures (each with key, source_column, unit,
        description), and source_url + download_url.
    """
    norm_id = _normalize_dataset_id(dataset_id)
    cd = curated.get(norm_id)
    if cd is None:
        suggestion = _suggest_dataset_id(dataset_id)
        raise ValueError(
            f"Dataset {dataset_id!r} is not a curated ato-mcp dataset. "
            f"{suggestion}"
            "Try list_curated() to see all available IDs, or "
            "search_datasets('keyword') to fuzzy-find by topic."
        )
    dims_out = [
        ColumnDetail(
            key=c.key,
            source_column=c.source_column,
            description=c.description,
            unit=c.unit,
            role=c.role,
        )
        for c in cd.columns.values()
        if c.role in ("dimension", "id")
    ]
    measures_out = [
        ColumnDetail(
            key=c.key,
            source_column=c.source_column,
            description=c.description,
            unit=c.unit,
            role=c.role,
        )
        for c in cd.columns.values()
        if c.role == "measure"
    ]
    return DatasetDetail(
        id=cd.id,
        name=cd.name,
        description=cd.description,
        is_curated=True,
        update_frequency=cd.update_frequency,
        period_coverage=cd.period_coverage,
        dimensions=dims_out,
        measures=measures_out,
        source_url=cd.source_url,
        download_url=cd.download_url,
    )


async def _get_data_impl(
    dataset_id: str,
    filters: Any,
    measures: Any,
    start_period: Any,
    end_period: Any,
    fmt: Any,
    last_n: int | None = None,
) -> DataResponse:
    # Reset the graceful-degradation flag at the start of each tool call so
    # we only report staleness introduced by THIS call's fetches.
    reset_stale_signal()
    norm_id = _normalize_dataset_id(dataset_id)
    cd = curated.get(norm_id)
    if cd is None:
        suggestion = _suggest_dataset_id(dataset_id)
        raise ValueError(
            f"Dataset {dataset_id!r} is not a curated ato-mcp dataset. "
            f"{suggestion}"
            "Try list_curated() to see all available IDs, or "
            "search_datasets('keyword') to fuzzy-find by topic."
        )
    filters_d = _validate_filters(filters)
    measures_v = _validate_measures(measures)
    start_v = _validate_period(start_period, "start_period")
    end_v = _validate_period(end_period, "end_period")
    if fmt is None:
        fmt_norm = "records"
    elif isinstance(fmt, str):
        fmt_norm = fmt.lower()
    else:
        raise ValueError(
            f"format must be a string, got {type(fmt).__name__}. "
            f"Valid options: {sorted(_VALID_FORMATS)}. "
            "Try format='records' (default), format='series', or format='csv'."
        )
    if fmt_norm not in _VALID_FORMATS:
        suggestion = difflib.get_close_matches(fmt_norm, sorted(_VALID_FORMATS), n=1, cutoff=0.5)
        hint = f"Did you mean {suggestion[0]!r}? " if suggestion else ""
        raise ValueError(
            f"Unknown format {fmt!r}. Valid options: {sorted(_VALID_FORMATS)}. "
            f"{hint}"
            "Try format='records' (default), format='series', or format='csv'."
        )
    if start_v and end_v and start_v > end_v:
        raise ValueError(
            f"end_period ({end_v}) is before start_period ({start_v}). "
            "Try swapping them."
        )

    user_query: dict[str, Any] = {}
    if filters_d:
        user_query["filters"] = dict(filters_d)
    if measures_v is not None:
        user_query["measures"] = measures_v
    if start_v:
        user_query["start_period"] = start_v
    if end_v:
        user_query["end_period"] = end_v

    df = await _fetch_and_parse(cd, kind=cd.cache_kind)  # type: ignore[arg-type]
    resp = build_response(
        cd=cd,
        df=df,
        filters=filters_d,
        measures=measures_v,
        start_period=start_v,
        end_period=end_v,
        fmt=fmt_norm,
        user_query=user_query,
        last_n=last_n,
    )
    # If any fetch in the chain served a stale-cache fallback because
    # data.gov.au was unreachable, propagate it to the response.
    stale, reason = get_stale_signal()
    if stale:
        resp.stale = True
        resp.stale_reason = reason
    return resp


@mcp.tool
async def get_data(
    dataset_id: Annotated[
        str,
        Field(
            description="Curated dataset ID. Use search_datasets() / list_curated().",
            examples=["IND_POSTCODE", "COMPANY_INDUSTRY", "ACNC_REGISTER"],
        ),
    ],
    filters: Annotated[
        dict[str, Any] | None,
        Field(
            description=(
                "Dimension filters. Keys are plain-English aliases from the dataset's "
                "describe_dataset response. Values are matched against the source data; "
                "pass a list to OR across values. Examples: "
                "{'state': 'nsw'}, {'postcode': '2000'}, "
                "{'industry_broad': ['A', 'B']}."
            ),
            examples=[
                {"state": "nsw"},
                {"postcode": "2000"},
                {"state": ["nsw", "vic"], "taxable_status": "taxable"},
                {"industry_broad": "A. Agriculture, Forestry and Fishing"},
            ],
        ),
    ] = None,
    measures: Annotated[
        str | list[str] | None,
        Field(
            description=(
                "Which measure(s) to return. Plain-English keys from describe_dataset. "
                "Omit to return all measures."
            ),
            examples=[
                "median_taxable_income",
                ["median_taxable_income", "average_taxable_income"],
                "total_income",
            ],
        ),
    ] = None,
    start_period: Annotated[
        str | int | None,
        Field(
            description=(
                "Inclusive start period for transposed time-series datasets "
                "(GST_MONTHLY etc). Ignored for wide single-year tables. "
                "Format: 'YYYY' or 'YYYY-MM' or ATO FY 'YYYY-YY'. "
                "Bare int years like 2020 are coerced to '2020' automatically."
            ),
            examples=["2020", "2020-07", "2023-24", 2020],
        ),
    ] = None,
    end_period: Annotated[
        str | int | None,
        Field(
            description="Inclusive end period. Same format as start_period.",
            examples=["2024", "2024-12", 2024],
        ),
    ] = None,
    format: Annotated[
        Literal["records", "series", "csv"],
        Field(
            description=(
                "Response shape. 'records' (default): flat list of observations. "
                "'series': grouped by measure. 'csv': pandas CSV string in `csv` field."
            ),
            examples=["records", "series", "csv"],
        ),
    ] = "records",
) -> DataResponse:
    """Query a curated ATO/ACNC dataset and return observations.

    Examples:
        # Median taxable income in postcode 2000 (Sydney CBD), 2022-23
        resp = await get_data(
            "IND_POSTCODE_MEDIAN",
            filters={"state": "nsw", "postcode": "2000"},
            measures="median_taxable_income_2022_23",
        )

        # All registered charities in NSW with size = "large"
        resp = await get_data(
            "ACNC_REGISTER",
            filters={"state": "NSW", "charity_size": "Large"},
            measures=["total_gross_income", "total_employees"],
        )

        # 2023-24 corporate tax payable for entities with total income > $1B
        resp = await get_data("CORP_TRANSPARENCY", filters={"income_year": "2023-24"})

    Returns:
        DataResponse with records (or csv), unit, period bounds, row_count,
        source URL, and CC-BY attribution.
    """
    return await _get_data_impl(
        dataset_id, filters, measures, start_period, end_period, format
    )


@mcp.tool
async def latest(
    dataset_id: Annotated[
        str,
        Field(
            description="Curated dataset ID.",
            examples=["GST_MONTHLY", "CORP_TRANSPARENCY", "IND_POSTCODE"],
        ),
    ],
    filters: Annotated[
        dict[str, Any] | None,
        Field(
            description="Same filter shape as get_data. Useful for narrowing to one entity.",
            examples=[
                {"postcode": "2000"},
                {"entity_name": "BHP GROUP LIMITED"},
            ],
        ),
    ] = None,
    measures: Annotated[
        str | list[str] | None,
        Field(
            description="Same as get_data.",
            examples=["net_gst", "tax_payable"],
        ),
    ] = None,
) -> DataResponse:
    """Return the most recent observation(s) per measure for a dataset.

    For transposed time-series tables (GST_MONTHLY etc.) this trims to the
    most-recent period. For wide single-year tables (IND_POSTCODE etc.) it
    returns the same shape as get_data — there is only one period in those
    tables to begin with.

    Examples:
        # Latest monthly net GST nationally
        resp = await latest("GST_MONTHLY", measures="net_gst")
    """
    return await _get_data_impl(
        dataset_id, filters, measures, None, None, "records", last_n=1
    )


@mcp.tool
async def top_n(
    dataset_id: Annotated[
        str,
        Field(
            description="Curated dataset ID. Use search_datasets() / list_curated().",
            examples=["CORP_TRANSPARENCY", "IND_POSTCODE_MEDIAN", "COMPANY_INDUSTRY"],
        ),
    ],
    measure: Annotated[
        str,
        Field(
            description=(
                "Plain-English measure key to rank by. Use describe_dataset() "
                "to see available measures."
            ),
            examples=["total_income", "median_taxable_income_2022_23", "tax_payable"],
        ),
    ],
    n: Annotated[
        int,
        Field(
            description="How many top (or bottom) rows to return.",
            ge=1,
            le=500,
            examples=[5, 10, 20, 50],
        ),
    ] = 10,
    filters: Annotated[
        dict[str, Any] | None,
        Field(
            description="Optional dimension filters, same shape as get_data.",
            examples=[
                {"state": "nsw"},
                {"income_year": "2023-24"},
                {"industry_broad": "C. Manufacturing"},
            ],
        ),
    ] = None,
    direction: Annotated[
        Literal["top", "bottom"],
        Field(
            description=(
                "'top' returns the N rows with the LARGEST measure values "
                "(highest tax payable, biggest population, etc.). 'bottom' "
                "returns the SMALLEST."
            ),
            examples=["top", "bottom"],
        ),
    ] = "top",
) -> DataResponse:
    """Return the N rows with the largest (or smallest) value of a measure.

    This is the most common agent workflow: "show me the top 10 X by Y".
    Without this tool, an agent would call get_data, receive the full table,
    and then sort/slice locally — wasting tokens and turns. top_n does the
    rank server-side and returns only the requested rows.

    Examples:
        # Top 10 corporate taxpayers in 2023-24
        top_n("CORP_TRANSPARENCY", "tax_payable", n=10)

        # 20 NSW postcodes with the highest median income (2022-23)
        top_n("IND_POSTCODE_MEDIAN", "median_taxable_income_2022_23",
              filters={"state": "nsw"}, n=20)

        # 5 lowest-income postcodes in QLD
        top_n("IND_POSTCODE_MEDIAN", "median_taxable_income_2022_23",
              filters={"state": "qld"}, n=5, direction="bottom")

    Returns:
        DataResponse with at most `n` records, sorted by `measure` value
        in the requested direction. Other fields (period, unit, attribution)
        match a regular get_data call.
    """
    # Validate inputs that pydantic's runtime can't enforce strictly when
    # called directly (Literal/ge/le are type-checker-only in some paths).
    if not isinstance(measure, str) or not measure.strip():
        raise ValueError(
            "measure is required and must be a non-empty string. "
            "Use describe_dataset() to see available measure keys."
        )
    if isinstance(n, bool) or not isinstance(n, int):
        raise ValueError(
            f"n must be a positive integer, got {n!r} ({type(n).__name__}). "
            "Try n=10 (default) or any int between 1 and 500. "
            "Example: top_n('CORP_TRANSPARENCY', 'tax_payable', n=20)."
        )
    if n < 1:
        raise ValueError(
            f"n must be >= 1, got {n}. "
            "Try n=10 (default) or any int between 1 and 500. "
            "Example: top_n('CORP_TRANSPARENCY', 'tax_payable', n=20)."
        )
    if direction not in ("top", "bottom"):
        suggestion = difflib.get_close_matches(
            str(direction).lower() if isinstance(direction, str) else "",
            ["top", "bottom"], n=1, cutoff=0.4,
        )
        hint = f"Did you mean {suggestion[0]!r}? " if suggestion else ""
        raise ValueError(
            f"direction must be 'top' or 'bottom', got {direction!r}. "
            f"{hint}"
            "Try direction='top' for largest values, direction='bottom' for smallest."
        )

    # Run a full get_data first, then rank + slice. The parsed-DataFrame cache
    # means this is essentially free after the first hit.
    full = await _get_data_impl(
        dataset_id, filters, measure, None, None, "records", last_n=None,
    )
    # Filter out null values, sort, slice
    valid = [r for r in full.records if isinstance(r, Observation) and r.value is not None]
    valid.sort(key=lambda r: r.value, reverse=(direction == "top"))
    top = valid[:n]
    # Preserve the response envelope; replace records and row_count
    return full.model_copy(update={"records": top, "row_count": len(top)})


_STATS_MAX_GROUPS = 200


def _summarise(values: list[float]) -> dict[str, float | int]:
    """Compute the stats payload over a list of non-null numeric values."""
    if not values:
        return {"count": 0}
    import statistics as _stats
    n = len(values)
    s_mean = sum(values) / n
    s_var = sum((v - s_mean) ** 2 for v in values) / n if n > 1 else 0.0
    return {
        "count": n,
        "sum": round(sum(values), 2),
        "mean": round(s_mean, 2),
        "median": round(_stats.median(values), 2),
        "min": round(min(values), 2),
        "max": round(max(values), 2),
        "stddev": round(s_var ** 0.5, 4),
    }


@mcp.tool
async def stats(
    dataset_id: Annotated[
        str,
        Field(
            description="Curated dataset ID. Use search_datasets() / list_curated().",
            examples=["IND_POSTCODE_MEDIAN", "CORP_TRANSPARENCY", "ATO_OCCUPATION"],
        ),
    ],
    measure: Annotated[
        str,
        Field(
            description=(
                "The measure key to aggregate over. Use describe_dataset() "
                "to see available measures."
            ),
            examples=["median_taxable_income_2022_23", "tax_payable", "total_income"],
        ),
    ],
    filters: Annotated[
        dict[str, Any] | None,
        Field(
            description="Optional dimension filters — same shape as get_data.",
            examples=[
                {"state": "nsw"},
                {"industry_broad": "C. Manufacturing"},
                {"sex": "female"},
            ],
        ),
    ] = None,
    group_by: Annotated[
        str | None,
        Field(
            description=(
                "Optional dimension key to partition rows by. When set, returns "
                "per-group statistics instead of a single aggregate. Caps at "
                "200 groups to keep responses bounded — exceeding the cap returns "
                "the first 200 groups by row order and sets a `groups_truncated` "
                "flag in the response."
            ),
            examples=["state", "sex", "income_year", "industry_broad"],
        ),
    ] = None,
) -> dict[str, Any]:
    """Aggregate statistics (count, sum, mean, median, min, max, stddev) for
    one measure across all rows matching filters. Optionally grouped.

    Without `group_by`: returns one stats payload over all matching rows.
    With `group_by`: returns per-group stats — much more powerful for
    "distribution X by Y" queries that would otherwise require N filtered
    calls.

    Examples:
        # Single aggregate over NSW postcodes
        stats("IND_POSTCODE_MEDIAN", "median_taxable_income_2022_23",
              filters={"state": "nsw"})
        # → {statistics: {count: 587, mean: 55017, median: 53484, ...}}

        # Stats grouped by state — one call instead of 8
        stats("IND_POSTCODE_MEDIAN", "median_taxable_income_2022_23",
              group_by="state")
        # → {by: "state", groups: [
        #     {key: "ACT", statistics: {...}},
        #     {key: "NSW", statistics: {...}},
        #     ...
        # ]}

        # Tax payable per income year across the corporate sector
        stats("CORP_TRANSPARENCY", "tax_payable", group_by="income_year")

    Returns:
        Without group_by: dict with `statistics` field.
        With group_by:    dict with `by` and `groups` fields; each group
                          carries `key`, `statistics`, plus the same envelope
                          metadata (dataset_id, unit, attribution, etc.).
    """
    if not isinstance(measure, str) or not measure.strip():
        raise ValueError(
            "measure is required and must be a non-empty string. "
            "Use describe_dataset() to see available measure keys."
        )
    if group_by is not None and (not isinstance(group_by, str) or not group_by.strip()):
        raise ValueError(
            "group_by must be a non-empty string naming a dimension "
            "(or None / omitted). See describe_dataset() for valid dimensions."
        )

    # Reuse the normal data path so all filter / measure / dtype / validation
    # work happens for free and the parsed-DataFrame cache is shared.
    resp = await _get_data_impl(
        dataset_id, filters, measure, None, None, "records", last_n=None,
    )

    if group_by is None:
        values: list[float] = [
            r.value
            for r in resp.records
            if isinstance(r, Observation) and r.value is not None
        ]
        return {
            "dataset_id": resp.dataset_id,
            "dataset_name": resp.dataset_name,
            "measure": measure,
            "unit": resp.unit,
            "query": resp.query,
            "statistics": _summarise(values),
            "source": resp.source,
            "attribution": resp.attribution,
            "ato_url": resp.ato_url,
            "server_version": resp.server_version,
        }

    # group_by path: validate the column exists on the dataset, then bucket
    cd = curated.get(_normalize_dataset_id(dataset_id))
    if cd is None:
        # _get_data_impl above would have raised already, but defend anyway
        suggestion = _suggest_dataset_id(dataset_id)
        raise ValueError(
            f"Dataset {dataset_id!r} is not a curated ato-mcp dataset. "
            f"{suggestion}"
            "Try list_curated() to see all available IDs, or "
            "search_datasets('keyword') to fuzzy-find by topic."
        )
    valid_dim_keys = {c.key for c in cd.columns.values() if c.role in ("dimension", "id")}
    if group_by not in valid_dim_keys:
        valid_sorted = sorted(valid_dim_keys)
        suggestion = difflib.get_close_matches(group_by, valid_sorted, n=1, cutoff=0.6)
        hint = f"Did you mean {suggestion[0]!r}? " if suggestion else ""
        more = "..." if len(valid_sorted) > 10 else ""
        raise ValueError(
            f"Unknown group_by {group_by!r} for dataset {cd.id!r}. "
            f"{hint}"
            f"Valid options: {', '.join(valid_sorted[:10])}{more}. "
            f"Try describe_dataset({cd.id!r}) for full dimension details."
        )

    buckets: dict[str, list[float]] = {}
    bucket_order: list[str] = []
    for r in resp.records:
        if not isinstance(r, Observation) or r.value is None:
            continue
        key = r.dimensions.get(group_by)
        if key is None:
            continue
        key_s = str(key)
        if key_s not in buckets:
            buckets[key_s] = []
            bucket_order.append(key_s)
        buckets[key_s].append(r.value)

    truncated = False
    if len(bucket_order) > _STATS_MAX_GROUPS:
        bucket_order = bucket_order[:_STATS_MAX_GROUPS]
        truncated = True

    groups_out = [
        {"key": k, "statistics": _summarise(buckets[k])}
        for k in bucket_order
    ]
    out = {
        "dataset_id": resp.dataset_id,
        "dataset_name": resp.dataset_name,
        "measure": measure,
        "unit": resp.unit,
        "query": resp.query,
        "by": group_by,
        "groups": groups_out,
        "source": resp.source,
        "attribution": resp.attribution,
        "ato_url": resp.ato_url,
        "server_version": resp.server_version,
    }
    if truncated:
        out["groups_truncated"] = True
        out["groups_truncated_at"] = _STATS_MAX_GROUPS
    return out


@mcp.tool
def list_curated() -> list[str]:
    """List every curated dataset ID in this version of ato-mcp.

    These are the datasets where get_data accepts plain-English filter keys
    and returns aliased, well-typed measure columns. Each ID is documented
    via describe_dataset.

    Returns:
        Sorted list of dataset IDs.
    """
    return curated.list_ids()


def main() -> None:
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
