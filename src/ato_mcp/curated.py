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

from dataclasses import dataclass, field
from importlib import resources
from pathlib import Path
from typing import Literal

import yaml


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


def translate_filter_value(
    cd: CuratedDataset, dim_key: str, user_value: str
) -> str:
    """Translate a user-supplied dimension value to the value stored in the source column.

    If the dim has an enumerated `dimension_values` map, the user can pass either
    a plain-English alias (e.g. 'nsw') or the raw source value (e.g. 'NSW' or
    'New South Wales') — both resolve. If the dim is free-form (no enum), the
    raw value passes through.
    """
    dv = cd.dimension_values.get(dim_key)
    if dv is None or dv.values is None:
        return user_value
    if user_value in dv.values:
        return dv.values[user_value]
    # Maybe the user already passed the canonical value.
    if user_value in dv.values.values():
        return user_value
    valid = sorted(dv.values.keys())
    raise ValueError(
        f"Unknown value {user_value!r} for filter {dim_key!r} on dataset {cd.id!r}. "
        f"Try one of: {', '.join(valid[:15])}"
        + ("..." if len(valid) > 15 else "")
    )


def resolve_measure_keys(
    cd: CuratedDataset, requested: str | list[str] | None
) -> list[str]:
    """Translate a user's measures= request into a list of measure keys.

    - None  → all measure columns (subject to a soft default cap at the
      caller's discretion).
    - "foo" → ["foo"] (validated)
    - ["foo", "bar"] → ["foo", "bar"] (validated)
    Raw source column names also pass through if they match a measure column.
    """
    measure_keys = [c.key for c in measure_columns(cd)]
    if requested is None:
        return measure_keys
    items: list[str]
    if isinstance(requested, str):
        items = [requested]
    elif isinstance(requested, list):
        if not requested:
            raise ValueError(
                f"measures filter is an empty list. "
                "Pass at least one measure, or omit `measures` to return all."
            )
        items = [str(x) for x in requested]
    else:
        raise ValueError(
            f"measures must be a string or list of strings, got {type(requested).__name__}."
        )

    source_to_key = {c.source_column: c.key for c in cd.columns.values() if c.role == "measure"}
    valid_keys = set(measure_keys)
    out: list[str] = []
    for v in items:
        v_str = v.strip()
        if not v_str:
            raise ValueError(
                f"Empty measure key. Try one of: {', '.join(sorted(valid_keys)[:15])}"
            )
        if v_str in valid_keys:
            out.append(v_str)
        elif v_str in source_to_key:
            out.append(source_to_key[v_str])
        else:
            raise ValueError(
                f"Unknown measure {v!r} for dataset {cd.id!r}. "
                f"Try one of: {', '.join(sorted(valid_keys)[:15])}"
                + ("..." if len(valid_keys) > 15 else "")
            )
    # Dedupe while preserving order.
    seen: set[str] = set()
    return [k for k in out if not (k in seen or seen.add(k))]
