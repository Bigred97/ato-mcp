"""Fuzzy search and listing across the curated dataset registry.

Unlike abs-mcp (which calls SDMX dataflow listings) or rba-mcp (which has a
static F-table registry), ato-mcp ships with N curated datasets hand-picked
for sellable value. The catalog surface is intentionally small in v0.1 — we
expose only the curated set. Future versions can grow this to discover
arbitrary ATO datasets via CKAN.
"""
from __future__ import annotations

from rapidfuzz import fuzz

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

    # Three-pool ranker (matches apra/aihw/asic/rba design):
    # - id+name token_set_ratio = PRIMARY discriminator
    # - keywords broaden recall at KEYWORD_WEIGHT (under name strength)
    # - description capped at DESCRIPTION_CAP, half weight
    # - PHRASE_BONUS when query is contiguous substring of keyword
    #   haystack
    # - proportional scaling against leader's raw — no pre-sort clamp
    DESCRIPTION_CAP = 30
    KEYWORD_WEIGHT = 0.4
    PHRASE_BONUS = 15

    keyword_lookup = {cd.id: " ".join(cd.search_keywords) for cd in curated_mod.list_all()}
    query_lc = query.lower()
    candidates: list[tuple[float, float, int]] = []
    for i, s in enumerate(summaries):
        name_str = f"{s.id} {s.name}".lower()
        kw_str = f"{name_str} {keyword_lookup.get(s.id, '')}".lower()
        desc_str = (s.description or "").lower()
        name_high = fuzz.token_set_ratio(query_lc, name_str)
        kw_high = fuzz.token_set_ratio(query_lc, kw_str)
        desc_raw = fuzz.WRatio(query_lc, desc_str) if desc_str else 0
        desc = min(desc_raw, DESCRIPTION_CAP)
        phrase = PHRASE_BONUS if query_lc and query_lc in kw_str else 0
        raw_adjusted = name_high + kw_high * KEYWORD_WEIGHT + desc * 0.3 + phrase
        candidates.append((raw_adjusted, name_high, i))

    candidates.sort(key=lambda t: (-t[0], -t[1]))
    top_pool = candidates[:limit]
    out: list[DatasetSummary] = []
    if top_pool:
        leader_adj = top_pool[0][0]
        scale_ref = max(leader_adj, 100.0)
        for raw, _name_high, idx in top_pool:
            rel = round(max(0.0, (raw / scale_ref) * 100.0), 1)
            out.append(summaries[idx].model_copy(update={"relevance": rel}))
    return out
