"""Hand-curated metadata for the top-N ATO/ACNC datasets.

Each YAML under `data/curated/` describes one queryable table:
- where to fetch it (data.gov.au resource URL)
- how to parse it (sheet name, header row, layout)
- which columns are dimensions (filterable) vs measures (returned values)
- plain-English aliases for ATO's verbose column names
- which filter values are accepted, what they mean
- search keywords folded into the fuzzy search haystack

The translator turns a user's plain-English `filters={...}` and
`measures=[...]` request into instructions the shaping layer can apply
to the parsed DataFrame.
"""
from __future__ import annotations

import difflib
from dataclasses import dataclass
from importlib import resources
from pathlib import Path
from typing import Literal

import yaml
from aus_identity import (
    is_valid_postcode,
    normalize_state,
    postcode_to_state,
)


# Dim names whose values are state/region references. When `translate_filter_value`
# encounters a value on one of these dims it tries `aus_identity` first so
# "NSW", "nsw", "New South Wales", "AU-VIC", "Tassie", and 4-digit postcodes
# all resolve to the curated alias (`nsw` / `NSW`).
_STATE_LIKE_DIM_NAMES = frozenset({"state", "region", "state_territory"})

Layout = Literal["wide", "transposed"]


@dataclass(frozen=True)
class CuratedColumn:
    """One column in the source table that's exposed to users."""
    key: str                                 # plain-English alias (e.g. "median_taxable_income")
    source_column: str                       # exact XLSX/CSV column header
    description: str | None = None
    unit: str | None = None                  # "AUD", "Persons", "Count", "Per cent"
    role: str = "measure"                    # "dimension" | "measure" | "id"
    dtype: str | None = None                 # optional pandas coercion: "int", "float", "string"


@dataclass(frozen=True)
class CuratedDimensionValues:
    """Allowed values for a dimension, plus their canonical labels.

    Used for state/territory codes, taxable-status enums, industry codes, etc.
    `None` means free-form (e.g. postcode, ABN) — anything goes.
    """
    values: dict[str, str] | None = None     # alias -> source value


@dataclass(frozen=True)
class CuratedDataset:
    """One curated dataset (a single queryable view)."""
    id: str
    name: str
    description: str
    source_url: str                          # the data.gov.au dataset page
    download_url: str                        # direct XLSX/CSV resource URL (fallback if discovery fails)
    format: Literal["xlsx", "csv"]
    sheet: str | None                        # XLSX sheet name; None for CSV
    header_row: int                          # 1-indexed
    data_start_row: int | None               # optional override (defaults to header_row + 1)
    max_rows: int | None                     # cap on data rows read — used to carve out a sub-table from a multi-section sheet (SMSF Annual Overview)
    layout: Layout                           # "wide" = entities-as-rows; "transposed" = years-as-cols
    period_coverage: str | None              # e.g. "2022-23" or "2003-04 to 2022-23"
    update_frequency: str | None             # "annual", "weekly", "irregular"
    cache_kind: str                          # "data" | "register"
    columns: dict[str, CuratedColumn]        # keyed by alias
    dimension_values: dict[str, CuratedDimensionValues]  # keyed by alias (column key)
    search_keywords: tuple[str, ...] = ()
    # For transposed tables: which column header carries the metric label,
    # and what unit column to read alongside (typically column B).
    metric_label_column: str | None = None
    unit_column: str | None = None
    # Optional auto-discovery spec: when present, the server resolves the
    # current download URL via CKAN at fetch time so new yearly releases
    # land without a YAML edit. See discovery.py.
    discovery: dict | None = None


_REGISTRY: dict[str, CuratedDataset] | None = None


def _yaml_dir() -> Path:
    try:
        ref = resources.files("ato_mcp").joinpath("data/curated")
        if ref.is_dir():
            return Path(str(ref))
    except (ModuleNotFoundError, AttributeError):
        pass
    here = Path(__file__).resolve().parent / "data" / "curated"
    if here.is_dir():
        return here
    raise FileNotFoundError("Could not locate ato_mcp/data/curated/")


def _parse_column(key: str, raw: dict) -> CuratedColumn:
    if not isinstance(raw, dict):
        raise ValueError(f"Column {key!r} must be a mapping, got {type(raw).__name__}")
    if "source_column" not in raw:
        raise ValueError(f"Column {key!r} missing required field 'source_column'")
    return CuratedColumn(
        key=key,
        source_column=str(raw["source_column"]),
        description=raw.get("description"),
        unit=raw.get("unit"),
        role=str(raw.get("role", "measure")),
        dtype=raw.get("dtype"),
    )


def _parse_dimension_values(raw: dict | None) -> CuratedDimensionValues:
    if raw is None:
        return CuratedDimensionValues(values=None)
    if not isinstance(raw, dict):
        raise ValueError(f"dimension_values entry must be a mapping, got {type(raw).__name__}")
    return CuratedDimensionValues(values={str(k): str(v) for k, v in raw.items()})


def _load_one(path: Path) -> CuratedDataset:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"{path.name}: top-level must be a mapping")

    columns: dict[str, CuratedColumn] = {}
    for key, col_raw in (raw.get("columns") or {}).items():
        columns[key] = _parse_column(key, col_raw)

    dim_values: dict[str, CuratedDimensionValues] = {}
    for key, val_raw in (raw.get("dimension_values") or {}).items():
        dim_values[key] = _parse_dimension_values(val_raw)

    fmt = str(raw.get("format", "xlsx")).lower()
    if fmt not in ("xlsx", "csv"):
        raise ValueError(f"{path.name}: format must be 'xlsx' or 'csv', got {fmt!r}")

    layout = str(raw.get("layout", "wide")).lower()
    if layout not in ("wide", "transposed"):
        raise ValueError(f"{path.name}: layout must be 'wide' or 'transposed', got {layout!r}")

    discovery_raw = raw.get("discovery")
    if discovery_raw is not None and not isinstance(discovery_raw, dict):
        raise ValueError(f"{path.name}: discovery must be a mapping if provided")

    return CuratedDataset(
        id=str(raw["id"]),
        name=str(raw["name"]),
        description=str(raw.get("description", "")),
        source_url=str(raw["source_url"]),
        download_url=str(raw["download_url"]),
        format=fmt,  # type: ignore[arg-type]
        sheet=raw.get("sheet"),
        header_row=int(raw.get("header_row", 1)),
        data_start_row=raw.get("data_start_row"),
        max_rows=raw.get("max_rows"),
        layout=layout,  # type: ignore[arg-type]
        period_coverage=raw.get("period_coverage"),
        update_frequency=raw.get("update_frequency"),
        cache_kind=str(raw.get("cache_kind", "data")),
        columns=columns,
        dimension_values=dim_values,
        search_keywords=tuple(raw.get("search_keywords") or ()),
        metric_label_column=raw.get("metric_label_column"),
        unit_column=raw.get("unit_column"),
        discovery=discovery_raw,
    )


def _load_all() -> dict[str, CuratedDataset]:
    out: dict[str, CuratedDataset] = {}
    for path in sorted(_yaml_dir().glob("*.yaml")):
        cd = _load_one(path)
        if cd.id in out:
            raise ValueError(f"Duplicate curated id {cd.id!r} (from {path.name})")
        out[cd.id] = cd
    return out


def get(dataset_id: str) -> CuratedDataset | None:
    global _REGISTRY
    if _REGISTRY is None:
        _REGISTRY = _load_all()
    return _REGISTRY.get(dataset_id.upper())


def list_ids() -> list[str]:
    global _REGISTRY
    if _REGISTRY is None:
        _REGISTRY = _load_all()
    return sorted(_REGISTRY.keys())


def list_all() -> list[CuratedDataset]:
    global _REGISTRY
    if _REGISTRY is None:
        _REGISTRY = _load_all()
    return [_REGISTRY[k] for k in sorted(_REGISTRY.keys())]


def reset_registry() -> None:
    """For tests."""
    global _REGISTRY
    _REGISTRY = None


def dimension_columns(cd: CuratedDataset) -> list[CuratedColumn]:
    """All columns flagged role == 'dimension'."""
    return [c for c in cd.columns.values() if c.role == "dimension"]


def measure_columns(cd: CuratedDataset) -> list[CuratedColumn]:
    """All columns flagged role == 'measure'."""
    return [c for c in cd.columns.values() if c.role == "measure"]


def id_columns(cd: CuratedDataset) -> list[CuratedColumn]:
    """All columns flagged role == 'id'."""
    return [c for c in cd.columns.values() if c.role == "id"]


def _aus_identity_pass_through(dim_key: str, user_value: str) -> str:
    """When a state-shaped dim has no curated enum, still normalise via
    aus_identity so postcodes route to state codes and lowercase / full
    names canonicalise. Returns the original value if it can't be
    normalised (existing free-form behaviour preserved)."""
    if dim_key not in _STATE_LIKE_DIM_NAMES:
        return user_value
    s = user_value.strip()
    if s.isdigit() and is_valid_postcode(s):
        try:
            return postcode_to_state(s)
        except ValueError:
            return user_value
    try:
        return normalize_state(s)
    except ValueError:
        return user_value


def _normalise_state_like(
    dim_key: str, user_value: str, alias_to_canonical: dict[str, str]
) -> str | None:
    """Try `aus_identity` normalisation when the user value is state-shaped.

    Returns the source-column value to use, or `None` to fall back to the
    existing "Did you mean?" suggestion path.
    """
    if dim_key not in _STATE_LIKE_DIM_NAMES:
        return None
    s = user_value.strip()
    if s.isdigit() and is_valid_postcode(s):
        try:
            code = postcode_to_state(s)
        except ValueError:
            return None
    else:
        try:
            code = normalize_state(s)
        except ValueError:
            return None
    # Resolve canonical state code back to the alias/value the YAML uses.
    # Aliases are typically lowercase (`nsw`); canonical values uppercase
    # (`NSW`). Try direct uppercase match first, then case-insensitive.
    if code in alias_to_canonical:
        return alias_to_canonical[code]
    lower = code.lower()
    if lower in alias_to_canonical:
        return alias_to_canonical[lower]
    # As a last resort, scan values (in case the source-column form
    # differs from the canonical short code — e.g. "AU-NSW" or full name).
    for v in alias_to_canonical.values():
        if v.upper() == code:
            return v
    return None


def translate_filter_value(
    cd: CuratedDataset, dim_key: str, user_value: str
) -> str:
    """Translate a user-supplied dimension value to the value stored in the source column.

    If the dim has an enumerated `dimension_values` map, the user can pass either
    a plain-English alias (e.g. 'nsw') or the raw source value (e.g. 'NSW' or
    'New South Wales') — both resolve. If the dim is free-form (no enum), the
    raw value passes through.

    State-shaped filters (`state`, `region`, `state_territory`) accept the
    full canonical menu via `aus_identity`: short codes (`NSW`/`nsw`/`Nsw`),
    full names (`New South Wales`), ISO 3166-2 (`AU-NSW`), aliases
    (`Tassie`), and 4-digit postcodes (`2000` → NSW, `2600` → ACT).
    """
    dv = cd.dimension_values.get(dim_key)
    if dv is None or dv.values is None:
        # Free-form state-shaped dims (rare) still benefit from postcode
        # routing: a user passing "2000" gets back "NSW" automatically.
        return _aus_identity_pass_through(dim_key, user_value)
    if user_value in dv.values:
        return dv.values[user_value]
    # Maybe the user already passed the canonical value.
    if user_value in dv.values.values():
        return user_value
    # Cross-source normalisation via aus_identity (state names, postcodes).
    normalised = _normalise_state_like(dim_key, user_value, dv.values)
    if normalised is not None:
        return normalised
    valid = sorted(dv.values.keys())
    # Look in both aliases and canonical values for a close match.
    haystack = valid + sorted(set(dv.values.values()))
    suggestion = difflib.get_close_matches(user_value, haystack, n=1, cutoff=0.6)
    hint = f"Did you mean {suggestion[0]!r}? " if suggestion else ""
    more = "..." if len(valid) > 10 else ""
    raise ValueError(
        f"Unknown value {user_value!r} for filter {dim_key!r} on dataset {cd.id!r}. "
        f"{hint}"
        f"Valid options: {', '.join(valid[:10])}{more}. "
        f"Try describe_dataset({cd.id!r}) to see all allowed values."
    )


def transposed_measure_aliases(cd: CuratedDataset) -> list[str]:
    """For a transposed-layout dataset, return the list of alias keys that
    label rows of the metric_label_column. These act as the dataset's
    'available measures' since transposed tables don't have measure columns.
    """
    if cd.layout != "transposed" or cd.metric_label_column is None:
        return []
    label_col = cd.metric_label_column
    for c in cd.columns.values():
        if c.source_column == label_col:
            dv = cd.dimension_values.get(c.key)
            if dv and dv.values is not None:
                return list(dv.values.keys())
            break
    return []


def resolve_measure_keys(
    cd: CuratedDataset, requested: str | list[str] | None
) -> list[str]:
    """Translate a user's measures= request into a list of measure keys.

    - None  → all measure columns (subject to a soft default cap at the
      caller's discretion).
    - "foo" → ["foo"] (validated)
    - ["foo", "bar"] → ["foo", "bar"] (validated)
    Raw source column names also pass through if they match a measure column.

    For transposed-layout datasets without explicit role=measure columns,
    the metric_label_column's dimension_values aliases double as the
    available measure keys.
    """
    measure_keys = [c.key for c in measure_columns(cd)]
    if not measure_keys:
        measure_keys = transposed_measure_aliases(cd)
    if requested is None:
        return measure_keys
    items: list[str]
    if isinstance(requested, str):
        items = [requested]
    elif isinstance(requested, list):
        if not requested:
            raise ValueError(
                "measures filter is an empty list. "
                "Pass at least one measure, or omit `measures` to return all. "
                f"Try describe_dataset({cd.id!r}) to see available measures."
            )
        items = [str(x) for x in requested]
    else:
        sample_keys = sorted(measure_keys)[:3]
        example = (
            f"measures={sample_keys[0]!r}" if sample_keys else "measures='total_income'"
        )
        raise ValueError(
            f"measures must be a string or list of strings, got {type(requested).__name__}. "
            f"Try describe_dataset({cd.id!r}) to discover valid measure keys. "
            f"Example: {example}."
        )

    source_to_key = {c.source_column: c.key for c in cd.columns.values() if c.role == "measure"}
    valid_keys = set(measure_keys)
    valid_sorted = sorted(valid_keys)
    out: list[str] = []
    for v in items:
        v_str = v.strip()
        if not v_str:
            more = "..." if len(valid_sorted) > 10 else ""
            raise ValueError(
                f"Empty measure key. "
                f"Valid options: {', '.join(valid_sorted[:10])}{more}. "
                f"Try describe_dataset({cd.id!r}) for full measure details."
            )
        if v_str in valid_keys:
            out.append(v_str)
        elif v_str in source_to_key:
            out.append(source_to_key[v_str])
        else:
            # Look across both alias keys and raw source columns for a close match.
            haystack = valid_sorted + sorted(source_to_key.keys())
            suggestion = difflib.get_close_matches(v_str, haystack, n=1, cutoff=0.6)
            hint = f"Did you mean {suggestion[0]!r}? " if suggestion else ""
            valid_hint = (
                ", ".join(valid_sorted[:10])
                if valid_keys else "(none — dataset has no curated measures)"
            )
            more = "..." if len(valid_keys) > 10 else ""
            raise ValueError(
                f"Unknown measure {v!r} for dataset {cd.id!r}. "
                f"{hint}"
                f"Valid options: {valid_hint}{more}. "
                f"Try describe_dataset({cd.id!r}) for full measure details."
            )
    # Dedupe while preserving order.
    seen: set[str] = set()
    return [k for k in out if not (k in seen or seen.add(k))]
