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
import hashlib
import re
from collections import OrderedDict
from typing import Annotated, Any, Literal

import pandas as pd
from fastmcp import FastMCP
from pydantic import Field

from . import catalog, curated
from .client import ATOAPIError, ATOClient
from .discovery import DiscoveryError, DiscoverySpec, resolve_latest_url
from .models import DataResponse, DatasetDetail, DatasetSummary, ColumnDetail, Observation
from .parsing import drop_blank_rows, read_csv, read_xlsx
from .shaping import build_response

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
    if not isinstance(filters, dict):
        raise ValueError(
            f"filters must be a dict, got {type(filters).__name__}. "
            "Example: {'state': 'nsw', 'postcode': '2000'}."
        )
    return filters


def _validate_period(value: Any, field_name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(
            f"{field_name} must be a string like '2024' or '2024-06', "
            f"got {type(value).__name__}."
        )
    s = value.strip()
    if not s:
        return None
    if not _PERIOD_PATTERN.match(s):
        raise ValueError(
            f"{field_name} {value!r} has invalid format. "
            "Use 'YYYY' (e.g. '2024'), 'YYYY-MM' (e.g. '2024-06'), or "
            "an ATO financial year like '2022-23'."
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
                    f"measures list entries must be strings, got {type(m).__name__}."
                )
            s = m.strip()
            if not s:
                raise ValueError("measures list contains an empty string.")
            out.append(s)
        return out
    raise ValueError(
        f"measures must be a string or list of strings, got {type(measures).__name__}."
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
            f"limit must be a positive integer, got {limit!r} ({type(limit).__name__})."
        )
    if limit < 1:
        raise ValueError(f"limit must be >= 1, got {limit}.")
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
        raise ValueError(
            f"Dataset {dataset_id!r} is not a curated ato-mcp dataset. "
            "Try list_curated() to see available IDs."
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
    norm_id = _normalize_dataset_id(dataset_id)
    cd = curated.get(norm_id)
    if cd is None:
        raise ValueError(
            f"Dataset {dataset_id!r} is not a curated ato-mcp dataset. "
            "Try list_curated() to see available IDs."
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
            f"Valid options: {sorted(_VALID_FORMATS)}"
        )
    if fmt_norm not in _VALID_FORMATS:
        raise ValueError(
            f"Unknown format {fmt!r}. Valid options: {sorted(_VALID_FORMATS)}"
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
    return build_response(
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
        str | None,
        Field(
            description=(
                "Inclusive start period for transposed time-series datasets "
                "(GST_MONTHLY etc). Ignored for wide single-year tables. "
                "Format: 'YYYY' or 'YYYY-MM'."
            ),
            examples=["2020", "2020-07", "2023-24"],
        ),
    ] = None,
    end_period: Annotated[
        str | None,
        Field(
            description="Inclusive end period. Same format as start_period.",
            examples=["2024", "2024-12"],
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
            f"n must be a positive integer, got {n!r} ({type(n).__name__})."
        )
    if n < 1:
        raise ValueError(f"n must be >= 1, got {n}.")
    if direction not in ("top", "bottom"):
        raise ValueError(
            f"direction must be 'top' or 'bottom', got {direction!r}."
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
