# ato-mcp

Sister MCP in the Australian Public Data stack. See `../CLAUDE.md` for
portfolio-wide conventions; this file captures repo-specific details
plus the cross-sister discipline.

## Source

| | |
|--|--|
| Source agency | Australian Taxation Office (ATO) + ACNC, via data.gov.au |
| Source URL | https://data.gov.au/dataset/?organisation=australian-taxation-office |
| Data format | XLSX (Excel) via CKAN-discovered URLs on data.gov.au |
| Licence | CC-BY 3.0 Australia |
| Licence URL | https://creativecommons.org/licenses/by/3.0/au/ |
| Python module | `ato_mcp` |
| PyPI package | `ato-mcp` |
| GitHub | https://github.com/Bigred97/ato-mcp |

## Curated datasets (14)

IND_POSTCODE · IND_POSTCODE_MEDIAN · COMPANY_INDUSTRY · CORP_TRANSPARENCY · SUPER_CONTRIB_AGE · ACNC_REGISTER · ATO_OCCUPATION · SBB_BENCHMARKS · SMSF_FUNDS · GST_MONTHLY · TAX_GAPS · RND_INCENTIVE · ACNC_AIS_FINANCIALS · (+1)

## Repo-specific module set

Required (every sister): `server.py`, `models.py`, `curated.py`, `client.py`, `cache.py`, `shaping.py`, `data/curated/*.yaml`

Repo-specific extras:
- `parsing.py — XLSX reader with header normalisation (`\n` whitespace stripping)`
- `discovery.py — CKAN auto-discovery; each YAML's `discovery:` block resolves freshest data.gov.au resource URL at fetch time`
- `catalog.py — search ranking across 14 curated datasets`

## Repo-specific gotchas

- Taxation Statistics releases annually (~June); each YAML's CKAN discovery resolves the latest release without manual URL updates.
- Column-header normalisation in `parsing._normalize_header` strips whitespace around embedded `\n` — ATO inconsistencies vary by release.
- GST_MONTHLY exercises the transposed-layout code path (months as rows, not columns).

---

## The 5-tool surface (uniform across sisters — non-negotiable)

1. `search_*(query, limit)` — fuzzy search across known datasets/tables/locations
2. `describe_*(id)` — schema + filter values + source URL
3. `get_data(id, filters, start_period, end_period, format)` — query
4. `latest(id, filters)` — current snapshot (caps to `limit` for register data)
5. `list_curated()` — enumerate supported IDs

Every parameter uses `Annotated[Type, Field(description=..., examples=[...])]`.
This is the Glama Tool Definition Quality requirement — non-negotiable.

## Trust contract (every DataResponse carries)

```
source             "Australian Taxation Office (ATO) + ACNC, via data.gov.au"
source_url         https://data.gov.au/dataset/?organisation=australian-taxation-office
attribution        full CC-BY 3.0 Australia attribution string with licence URL
retrieved_at       UTC timestamp
server_version     importlib.metadata.version("ato-mcp")
stale              True when serving cached fallback after upstream error
stale_reason       human-readable when stale=True
truncated_at       int | None — set when latest() caps a large response
```

## The 5 quality dimensions (audit every release against these)

1. **Semantic Clarity** — verb-noun tool names, Annotated[Field] with examples, rich docstrings (Examples + When to use + Returns blocks), `pattern=` constraints where IDs have known shapes
2. **Data Pruning** — <10k tokens for typical responses, `latest()` caps register dumps via `limit` + `truncated_at`, no leaked SDMX/Excel boilerplate dims in records
3. **Cross-Agency Joining** — uniform period format conventions (YYYY / YYYY-MM / YYYY-Q1 / YYYY-S1 / YYYY-MM-DD); standardise on ASGS, postcode, ABN, ANZSIC where the data supports it
4. **Reliability + Caching** — SQLite cache TTLs (15min latest / 1h data / 24h catalogue / 7d structure), self-heal on `sqlite3.DatabaseError`, **graceful degradation**: when upstream fails, fall back to last cached payload and set `stale=True, stale_reason="..."` rather than raising
5. **Deterministic Error Handling** — every `ValueError` carries a "Try X" / "Did you mean X?" / "Valid options: ..." hint that suggests the correction, not just describes the rejection

## Test taxonomy

Required: `test_cache.py`, `test_curated.py`, `test_server_validation.py`, `test_shaping.py`, `test_integration.py` (live, `@pytest.mark.live`)
Recommended: `test_client.py`, `test_mcp_protocol.py`, `test_discovery.py`, `test_resilience.py`, `test_edge_inputs.py`, `test_concurrency.py`

Zero-flake bar: full unit suite must run 10× consecutively green before tagging a release.

## Release workflow (Trusted Publishing via OIDC, no API tokens in CI)

```
1. Bump version in pyproject.toml (semver)
2. Update CHANGELOG.md (latest entry at top, semver headings)
3. uv run pytest × 10 — zero flakes
4. git commit -am "X.Y.Z: <one-line reason>"
5. git tag -a vX.Y.Z -m "X.Y.Z: <reason>"
6. git push origin main vX.Y.Z
7. release.yml fires → builds → OIDC publish → PyPI
```

PyPI new-project rate limit: 5/day per account; not an issue for existing
projects (only counts NEW package names).

## Anti-patterns — DO NOT do these

- Don't add a 6th tool; uniform 5-tool surface is the brand
- Don't add new top-level dependencies beyond what other sisters use (httpx, pydantic, fastmcp, aiosqlite, rapidfuzz, pyyaml, + parsing-library if needed)
- Don't bundle large XLSX/CSV fixtures in the wheel; cache at runtime
- Don't ship without 10 consecutive zero-flake pytest runs
- Don't echo PyPI tokens / PATs in tool output, commit messages, or CHANGELOG
- Don't classify a slow source API (>2s cold) as a bug; only flag >10s or actual errors
- Don't widen scope mid-audit-loop; loops are fix-only

## Common operations

```bash
cd .                                                       # in the repo
uv sync --extra dev                                        # install deps
uv run pytest                                              # unit tests
uv run pytest -m live                                      # live tests too
uvx --refresh --from ato-mcp==<ver> python -c "..."         # smoke a published wheel
```
