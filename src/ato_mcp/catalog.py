"""Fuzzy search and listing across the curated dataset registry.

Unlike abs-mcp (which calls SDMX dataflow listings) or rba-mcp (which has a
static F-table registry), ato-mcp ships with N curated datasets hand-picked
for sellable value. The catalog surface is intentionally small in v0.1 — we
expose only the curated set. Future versions can grow this to discover
arbitrary ATO datasets via CKAN.
"""
from __future__ import annotations

from rapidfuzz import fuzz, process

from . import curated as curated_mod
from .models import DatasetSummary


def list_summaries() -> list[DatasetSummary]:
    """All curated datasets as DatasetSummary objects."""
    out: list[DatasetSummary] = []
    for cd in curated_mod.list_all():
        out.append(
            DatasetSummary(
                id=cd.id,
                name=cd.name,
                description=cd.description,
                update_frequency=cd.update_frequency,
                is_curated=True,
            )
        )
    return out


def search(query: str, limit: int = 10) -> list[DatasetSummary]:
    """Fuzzy-search curated datasets by id, name, description, and search_keywords.

    Two-pool ranker (mirrors abs-mcp's design):
      * High-signal pool = id + name + curated.search_keywords —
        scored with token_set_ratio. Token-strict so a query like
        'charity revenue' doesn't fuzzy-match 'foreign ownership' just
        because both descriptions mention 'Australia' or 'annual'.
      * Description pool = the YAML description text — scored with
        WRatio (preserves typo tolerance for free-text queries) and
        capped at DESCRIPTION_CAP so long boilerplate prose can't
        dominate.

    Final score = token_set(name/keywords) + min(WRatio(description),
    DESCRIPTION_CAP), clamped to 100. The previous WRatio-only ranker
    collapsed unrelated datasets to identical ~57 scores because their
    descriptions all contained common terms like 'Australia', 'data',
    'annual'; token_set_ratio fixes that by requiring real word overlap.
    """
    if not query.strip():
        raise ValueError(
            "query is required. Try 'postcode', 'company', 'charity', "
            "'gst', 'super', or any other ATO topic."
        )
    summaries = list_summaries()
    if not summaries:
        return []

    # Description contribution caps at 30 — well below a clean
    # high-signal token-set match (100). Tuned to keep curated
    # keyword/name matches reliably above any description-only hit.
    DESCRIPTION_CAP = 30

    keyword_lookup = {cd.id: " ".join(cd.search_keywords) for cd in curated_mod.list_all()}

    scored: list[tuple[float, float, int]] = []  # (final, high, idx)
    query_lc = query.lower()
    for i, s in enumerate(summaries):
        high_str = f"{s.id} {s.name} {keyword_lookup.get(s.id, '')}".lower()
        desc_str = (s.description or "").lower()
        high = fuzz.token_set_ratio(query_lc, high_str)
        desc_raw = fuzz.WRatio(query_lc, desc_str) if desc_str else 0
        desc = min(desc_raw, DESCRIPTION_CAP)
        final = min(high + desc * 0.5, 100.0)  # half-weight desc on top of full-weight high
        scored.append((final, high, i))

    scored.sort(key=lambda t: (-t[0], -t[1]))
    return [
        summaries[idx].model_copy(update={"relevance": round(float(final), 1)})
        for final, _high, idx in scored[:limit]
    ]
