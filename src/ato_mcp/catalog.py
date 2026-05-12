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

    Score order is by RapidFuzz WRatio. The whole index is curated so no
    bonus reranking is needed (the rba/abs `+25 curated bonus` is irrelevant here).
    """
    if not query.strip():
        raise ValueError(
            "query is required. Try 'postcode', 'company', 'charity', "
            "'gst', 'super', or any other ATO topic."
        )
    summaries = list_summaries()
    if not summaries:
        return []
    # Build the haystack including search_keywords from the curated YAML.
    keyword_lookup = {cd.id: " ".join(cd.search_keywords) for cd in curated_mod.list_all()}
    haystack = {
        i: f"{s.id} {s.name} {s.description or ''} {keyword_lookup.get(s.id, '')}"
        for i, s in enumerate(summaries)
    }
    matches = process.extract(
        query, haystack, scorer=fuzz.WRatio, limit=max(limit, len(summaries))
    )
    ordered = sorted(matches, key=lambda m: -m[1])
    return [summaries[idx] for _hay, _score, idx in ordered[:limit]]
