# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.6.1] - 2026-05-16

### Changed — historical dataset flagged in description

- `GST_MONTHLY` description and `period_coverage` updated to prominently
  warn that the dataset covers July 2020 to June 2023 only. ATO ceased
  publishing the monthly GST/WET/LCT collections series after the
  2022-23 Taxation Statistics release; current monthly data lives in
  PDF-embedded tables in annual reports and is not machine-accessible.
- Description now positions the dataset for "COVID-era to early
  post-COVID GST trend analysis" rather than a live collections feed.

No data, code, or test changes. Description-only update so clients see
the historical nature of the data in `describe_dataset` output.

## [0.6.0] - 2026-05-15

### Added

- **DataResponse.source_url**: canonical click-through URL field, populated
  alongside the legacy `ato_url` alias. Cross-sister consumers can now read
  `.source_url` uniformly across the portfolio. `ato_url` remains populated
  with the same value for backward compatibility.

## [0.5.0] — 2026-05-15

### Added — Wave 1 portfolio interoperability fix (int-year coercion)

Cross-sister consistency pass on input handling identified in the portfolio
interoperability audit.

- **Int-year coercion in period validation.** `start_period=2024` (a bare
  JSON int) now coerces to `"2024"` instead of raising a TypeError-style
  message. LLM clients routinely send JSON ints; this removes a confusing
  failure mode that surfaced as `must be a string, got int`. Out-of-range
  ints (e.g. `12345`, `1800`) still raise — with a hint pointing at the
  canonical `'YYYY'` / `'YYYY-MM'` / `'YYYY-YY'` (ATO FY) forms. `bool` is
  explicitly rejected (it's a subclass of int) to avoid silent coercion.
- **Type signature broadened** on `get_data`'s `start_period` /
  `end_period` to `str | int | None` so the tool's published schema
  reflects the new coercion behaviour.

3 new unit tests in `tests/test_server_validation.py` cover the coercion
boundary, the out-of-range hint, and the bool-subclass-of-int guard.

### Backward compatibility

No breaking changes. Inputs that previously raised a type error on bare
int years now succeed; every other input still validates as before.

## [0.4.0] — 2026-05-15

### Added — aus-identity integration

The cross-source compatibility moat for the AU public-data MCP stack. The
`state` filter (on IND_POSTCODE, IND_POSTCODE_MEDIAN, ACNC_REGISTER, and
every other state-aware dataset) now accepts ANY of:

- Canonical short codes (`NSW`, `VIC`, `QLD`, `SA`, `WA`, `TAS`, `NT`, `ACT`)
- Case-insensitive variants (`nsw`, `Nsw`)
- Full names (`New South Wales`, `Queensland`, `Tasmania`)
- ISO 3166-2 (`AU-NSW`, `AU-VIC`)
- Common aliases (`Tassie`)
- 4-digit postcodes (`2000` → NSW, `2600` → ACT, `3000` → VIC, `0800` → NT)

Powered by [`aus-identity`](https://pypi.org/project/aus-identity/). An LLM
agent that's already fetched a postcode from another sister MCP (asic-mcp,
abs-mcp) can pass it straight to ato-mcp without manual conversion.

- **`aus-identity>=0.1.0`** added as a new top-level dependency. Pure-Python,
  no transitive deps.
- **`curated.translate_filter_value`** now wraps state-shaped dims
  (`state`, `region`, `state_territory`) with `aus_identity.normalize_state`
  and `aus_identity.postcode_to_state` before falling through to the
  existing alias / canonical lookup.
- **7 new unit tests** in `tests/test_curated.py` covering full state name,
  lowercase full name, ISO 3166-2 form, common alias, postcode routing,
  ACT-postcode (ACT/NSW boundary), and a second dataset (ACNC_REGISTER).

### Backward compatibility

No breaking changes — every input that worked in 0.3.2 still works.

## [0.3.2] — 2026-05-15

Error-message sweep — quality dimension #5 in CLAUDE.md. Rejection
messages now suggest the correction instead of just describing what's
wrong.

`ValueError` raises across `server.py`, `shaping.py`, and `curated.py`
were audited against the "Try X / Did you mean X? / Valid options:" bar.
The ~12 weak sites identified by the prior audit were rewritten to:

- Surface a `Did you mean X?` hint via `difflib.get_close_matches` when
  the input is a likely typo (unknown dataset ID, unknown measure key,
  unknown filter name, unknown filter value, unknown format, unknown
  group_by, unknown direction).
- List up to 10 valid alternatives inline, with `...` truncation marker
  when more exist, and a pointer to the discovery tool that gives the
  full picture (`describe_dataset(...)`, `list_curated()`,
  `search_datasets(...)`).
- Include a worked example for format and period validation failures
  (e.g. `Example: get_data('GST_MONTHLY', start_period='2024-01',
  end_period='2024-06')`).
- Tighten type-coercion messages (limit, n, measures-list entries) to
  pair the rejected value with a usable substitute.

Concrete before/after:

```
# Before (0.3.1)
"Dataset 'CORP_TRANSPRENCY' is not a curated ato-mcp dataset.
 Try list_curated() to see available IDs."

# After (0.3.2)
"Dataset 'CORP_TRANSPRENCY' is not a curated ato-mcp dataset.
 Did you mean 'CORP_TRANSPARENCY'?
 Try list_curated() to see all available IDs, or
 search_datasets('keyword') to fuzzy-find by topic."
```

No public-API behaviour change — the exception type is still
`ValueError`, all existing test message-match assertions still trigger
on the same key phrases.

- **+4 regression tests** (`test_server_validation.py` x2 +
  `test_curated.py` x2) — unknown dataset ID, unknown format, unknown
  measure key, unknown filter value all assert the new `Did you mean`
  + valid-options hint shape.
- 302 unit tests now (was 298 in 0.3.1).
- No new dependencies; `difflib` is a stdlib module.

## [0.3.1] — 2026-05-15

Graceful degradation — quality dimension #4 in CLAUDE.md. Pattern ported
from abs-mcp 0.2.13.

When data.gov.au is unreachable (5xx, timeout, DNS failure, connection
refused), the client now falls back to the most-recent cached payload
regardless of TTL and surfaces the staleness in the response. Agents
see `DataResponse.stale=True` with a `stale_reason` like *"ATO API
returned 503 for <url>; serving cached payload from ~17 minute(s) ago"*
and can continue reasoning, rather than the tool raising and breaking
the chat.

Genuine no-cache-to-fall-back-to case still raises `ATOAPIError` — only
degrade gracefully when there's something to degrade to. CKAN catalog
lookups (`kind="catalog"`) deliberately bypass the fallback: a stale
package listing could resolve to the wrong year's resource URL and
silently mask an ATO rename. Discovery already falls back to the YAML's
hard-coded `download_url` on `DiscoveryError`, so a clean error here is
the right signal.

- **New: `Cache.get_stale(key) -> (payload, cached_at)`** — TTL-bypassing
  read, the building block for the fallback path. Mirrors `.get()`'s
  mid-session corruption handling.
- **New: `_stale_signal` ContextVar in `client.py`** — `reset_stale_signal()`
  + `get_stale_signal()` are the public API. The server resets at the
  start of each `_get_data_impl` call and reads after `build_response`
  to propagate `stale=True` into the response.
- **New: `DataResponse.stale: bool` and `DataResponse.stale_reason: str | None`** —
  echoed in every response when serving a stale cache.
- **New: `DataResponse.truncated_at: int | None`** — placeholder field
  matching the sister-MCP envelope; remains `None` for time-series-shaped
  ATO data today.
- **+4 regression tests** in `test_resilience.py`:
  1. 503 + stale cache → fallback + stale flag set, wording check
  2. ConnectError + stale cache → same, `unreachable` wording
  3. 503 + empty cache → raises `ATOAPIError` (unchanged behaviour)
  4. `Cache.get_stale()` round-trip + TTL bypass verification
- 298 unit tests now (was 294 in 0.3.0).

## [0.3.0] — 2026-05-13

Minor release. Two new sellable datasets pushing the catalog to **14 curated datasets**.

### Added — `ACNC_AIS_FINANCIALS` curated dataset (14th dataset)

- ACNC Annual Information Statement — the per-charity financial detail
  companion to `ACNC_REGISTER`. While the register tells you who a charity
  is, the AIS tells you how big they are: total revenue, expenses by type,
  staff counts (FT / PT / casual / FTE / volunteers), and net surplus.
- ~60,000 charities × annual reporting period. 23 curated columns from
  the 91-column source: revenue (government, donations, goods/services,
  investments), expenses (employee, grants AU/overseas), bottom-line
  surplus, and staff demographics.
- Sellable: nonprofit-tech benchmarking, philanthropy analytics, grantmaker
  due diligence ("Which NSW charity has the highest grant ratio?",
  "What's Vinnies' total revenue?").
- 6 new tests in `test_acnc_ais.py` including the "Large charities
  always > $250k revenue" assertion (matches ACNC's size threshold) and
  a revenue/expenses ratio sanity check.

### Added — `RND_INCENTIVE` curated dataset (13th dataset)

- ATO R&D Tax Incentive transparency report — every entity that lodged
  an R&D claim with their name, ABN/ACN, original notional deduction, and
  amended figure where applicable. 2022-23 income year, ~13,000 entities.
- Verified top R&D claimants:
  - **Atlassian Australia $220.2M** (Australia's biggest R&D spender)
  - Fortescue Metals $150.8M
  - Cochlear $136.7M (medtech)
  - CSL $111.5M (vaccines/pharma)
  - ResMed $106.7M
- Sector totals: **$16.5B claimed** across 13,135 entities; mean $1.25M,
  median $375k.
- Sellable: startup-due-diligence, fintech, VC research, innovation-policy
  analysis.
- 6 new tests in `test_rnd.py` including the "Atlassian top 3" assertion
  and an $8B-30B total-claims sanity range.

## [0.2.2] — 2026-05-13

Hardening patch — two additional audit findings addressed.

### Hardening

- **Cache mid-session corruption recovery.** Previously, if the SQLite
  cache file got corrupted *after* the cache was initialised (disk error,
  external truncation, etc.), every subsequent `get`/`set` raised
  `sqlite3.DatabaseError`. Now both methods catch the error, drop and
  recreate the DB, and either return a cache miss (`get`) or retry the
  write (`set`). The cache is a perf layer not a source of truth, so
  losing its contents on corruption is always safe.
- **Discovery now paginates `package_search`.** Was hardcoded to
  `rows=200` — if data.gov.au's ATO org grows past 200 packages, the
  freshest yearly release could sit on page 2 and never be matched.
  Now walks up to 10 pages (2,000 packages) and stops when `count`
  signals the end. Tests cover the multi-page case.

### Tests
- 282 unit + 13 live = 295 total (+3 regression tests).
- Ruff still clean.

## [0.2.1] — 2026-05-13

Patch release — two bug fixes surfaced by a deliberate code audit after v0.2.0.

### Bug fixes

- **`latest()` no longer arbitrarily trims wide-layout results.** Calling
  `latest()` on a wide-layout dataset (any of IND_POSTCODE, IND_POSTCODE_MEDIAN,
  COMPANY_INDUSTRY, etc. — datasets with no time dimension) used to pick a
  single arbitrary row per measure when `last_n=1` was applied. Now the trim
  is skipped when every record has `period=None`. `latest()` on a wide
  dataset == `get_data()` (same query, same shape). Transposed-layout
  datasets (GST_MONTHLY, SMSF_FUNDS) still get proper most-recent trimming.
- **`end_period="2024"` against monthly data now correctly includes 2024-NN.**
  Naive string comparison `"2024-06" > "2024"` returned True, excluding
  every month of the year you asked for. Fixed by right-padding short
  end-period normalisations to "YYYY-99" when the period under test has a
  month component — so `end_period="2024"` against `period="2024-06"` now
  includes the row.

### Tests
- 279 unit + 13 live = 292 total. 3 new regression tests for the audit fixes.

## [0.2.0] — 2026-05-13

Six new curated datasets, two new tools, plus performance and security
upgrades since v0.1.0. Highlights:

- **12 curated datasets** (up from 10): added GST_MONTHLY, ATO_OCCUPATION,
  SMSF_FUNDS, SBB_BENCHMARKS, HELP_DEBT, TAX_GAPS.
- **7 MCP tools** (up from 5): added `top_n` (ranking) and `stats` (with
  `group_by` for grouped aggregates).
- **132× warm-hit speedup** on the largest dataset via the parsed-DataFrame
  in-process cache.
- **Discovery host pin**: refuses any URL whose host isn't data.gov.au.
- **Ruff lint pass** + CI lint gate.

### Added — `TAX_GAPS` curated dataset (12th dataset)

- ATO's official "tax gap" estimates — the dollar difference between what
  each tax type should have collected (perfect compliance) and what was
  actually collected — across 4 tax types × ~7 financial years.
- Headline 2022-23 figures:
  - **Personal income tax gap**: $35.5B / 10.3% rate (growing from
    8.8% in 2017)
  - **Corporate income tax gap**: $10.8B
  - **GST gap**: $8.1B
  - **Excise & other gap**: $3.8B
  - Total estimated missing tax: **~$58 billion per year**
- Sellable angles: public-policy researchers, tax-advisory firms,
  compliance fintech, investigative journalism.
- 7 new tests in `test_tax_gaps.py` including a "personal > corporate"
  ordering assertion, a year-over-year growth check, and a rate-under-15%
  sanity check.

### Added — `stats` MCP tool (7th tool) with `group_by`

- New tool: `stats(dataset_id, measure, filters?, group_by?)` returns
  summary aggregates (count, sum, mean, median, min, max, stddev) for
  one measure across all rows matching filters. Collapses the
  "fetch-all-then-aggregate-locally" workflow into a single call —
  response payload is tiny (8 numbers) even when the underlying dataset
  has thousands of rows.
- `group_by` parameter buckets rows by a dimension before aggregating.
  Real insights surface in one call:
  - **By state** (NSW postcode median income): ACT highest ($72k mean),
    TAS lowest ($50k mean). 587 NSW postcodes vs 24 ACT.
  - **By sex** (occupation median income): Male $72,408 median vs
    Female $59,667 — a visible 21% gap.
  - **By industry** (company total income): Mining $95B/company average
    (Big-3 distortion), Manufacturing has the most companies (55).
- Caps at 200 groups to keep responses bounded; flags `groups_truncated`
  when exceeded (e.g. group_by="postcode" with ~2,300 unique values).
- Skips null values automatically, so blank-tax-payable entries don't
  drag down the mean of `CORP_TRANSPARENCY`.
- 19 new tests in `test_stats.py`.

### Added — `HELP_DEBT` curated dataset (11th dataset)

- ATO HELP / HECS annual statistics (Table 1) — total outstanding debt,
  indexation, compulsory and voluntary repayments, write-offs by financial
  year from 2005-06 to 2024-25. Universally relatable for any Australian
  uni grad. Headline 2024-25 figures: **$125.3B total HECS debt**, $52.1B
  in compulsory repayments, $11.8B in voluntary repayments, $21.8B in
  annual indexation. Sector grew $29B since 2020-21.
- 6 new tests in `test_help.py` including a year-over-year growth assertion
  and a repayments-vs-debt sanity check.

## [Unreleased] — 2026-05-12

### Added — `SBB_BENCHMARKS` curated dataset (10th dataset)

- ATO Small Business Benchmarks 2023-24 — industry-specific total-expenses
  and cost-of-sales ratio bands by turnover bracket (low/medium/high) for
  ~100 small-business categories. The tax-advisor / accountant
  fintech goldmine: "is my client's bakery's COGS within ATO's expected
  range?" → 34–39% for medium-turnover bakeries. Top-expense-ratio
  industries are predictably low-margin retail: fuel (91%), tobacco (91%),
  liquor (90%), grocery (88%), tyre retailing (88%).
- 6 new tests in `test_sbb.py` including a known-value bakery assertion
  and a top-N low-margin-retail sanity check.

### Added — `SMSF_FUNDS` curated dataset (9th dataset)

- ATO SMSF Annual Overview Table 1 sub-table 2 — total SMSFs, total
  members, total gross assets over the last 6 financial years (2019-20
  to 2024-25). The "how big is the SMSF sector?" answer in one call:
  653,062 funds, 1.2M members, $1.05 trillion in assets at 30 June 2025.
- Auto-discovery wired (resolves to the latest "SMSF Annual Overview
  YYYY-YY" resource on data.gov.au at fetch time).

### Schema extension — `max_rows`

- Curated YAMLs can now declare `max_rows: N` to carve a sub-table out
  of a multi-section sheet. Needed for SMSF Annual Overview where each
  sheet stacks 4-6 sub-tables vertically (narrative → key highlights →
  data → next sub-title → data → ...). Other datasets unaffected.

### Bug fix — `latest` direction

- `last_n=1` on transposed datasets used to return the OLDEST period
  when the source file lists years descending (SMSF's case). Now
  `shape_transposed` sorts by normalised period ascending before
  tailing — so `latest("SMSF_FUNDS", measures="total_smsfs")` correctly
  returns 2024-25's 653,062 funds, not 2019-20's 566,871.

### Added — repo polish

- GitHub Actions workflows: `tests` (Python 3.11/3.12/3.13 matrix + wheel
  install verify) and `codeql` (weekly SAST). Both green on first run.
- README badges: tests, PyPI, Python versions, license, Glama.
- Issue templates, PR template, dependabot. Same shape as sister repos.
- Dependency bumps merged via dependabot: setup-uv v3→v7,
  actions/checkout v4→v6, codeql-action v3→v4.

### Added — `ATO_OCCUPATION` curated dataset (8th dataset)

- Individuals Table 15A — median and average taxable / salary-wage / total
  income by ANZSCO 6-digit occupation × sex. ~1,200 occupations × 3 sex
  categories. The "which jobs pay the most" answer in one call. Real
  numbers verified: median taxable income for medical specialists tops out
  at Otorhinolaryngologist $516k, Neurosurgeon $486k, Plastic Surgeon
  $459k; top non-medical role is Judge — Law at $438k.
- Pairs naturally with `top_n` for HR-tech / career-planning agents:
  `top_n("ATO_OCCUPATION", "median_taxable_income", filters={"sex": "total"})`
- 7 new tests in `test_occupation.py` including a CEO median sanity-check
  and a top-10-medical-specialists assertion.

### Added — `top_n` MCP tool (6th tool)

- **`top_n(dataset_id, measure, n=10, filters=None, direction="top")`** —
  ranks rows by a measure and returns the top (or bottom) N. The most
  common agent workflow ("show me the top 10 X by Y") now collapses to
  a single server-side call. Saves the agent from pulling every row and
  ranking client-side.
- Verified against real data:
  - Top 5 corp taxpayers 2023-24: Rio Tinto ($6.25B), BHP ($6.01B),
    Fortescue ($3.93B), Chevron ($3.52B), CommBank ($3.43B).
  - Top NSW postcodes by median income: 2043 (Erskineville/Newtown $92k),
    2039 (Rozelle), 2028 (Double Bay).
- Strict runtime validation on `n`, `direction`, and `measure` (Python's
  `Literal` annotation is type-checker-only).
- 13 new tests in `test_top_n.py`.

### Bug fixes

- **Trailing-whitespace state codes**: ATO ships some state values with a
  trailing space (`'NT '`, `'SA '`). Filters that compared user-supplied
  `'nt'` (which we already strip + alias-resolve to `'NT'`) silently
  returned 0 rows. Fix: `_to_clean_string` now strips whitespace on all
  string-typed columns at dtype coercion time, so every downstream filter
  comparison sees the canonical form.

### Performance

- **Parsed-DataFrame in-process cache**: warm get_data() hits no longer
  re-parse the XLSX. Measured speedups:
  - `IND_POSTCODE` (7.9MB): 4500ms → 34ms (**132× faster**)
  - `CORP_TRANSPARENCY` (270KB): 400ms → 8ms (53× faster)
  - `IND_POSTCODE_MEDIAN` (560KB): 400ms → 22ms (18× faster)
  Cache is bounded LRU (8 entries), keyed by (url, parse-spec, content
  signature) so a content change at the byte cache forces a re-parse. Sub-50ms
  warm hits across every dataset now — fast enough that Claude Desktop feels
  instant.

### Security

- **Discovery host pin**: `discovery.py` now refuses any resolved resource
  URL whose host isn't `data.gov.au` or a subdomain thereof. Defense in
  depth against a compromised CKAN returning a malicious URL. The host
  check is case-insensitive and resists suffix attacks
  (e.g. `data.gov.au.attacker.com` is correctly rejected).

### Bug fixes

- `parsing.read_xlsx` now wraps `zipfile.BadZipFile`, `KeyError`, and
  `OSError` as `ParseError`. Previously corrupted XLSX bytes leaked
  internal openpyxl/zipfile exceptions; now callers see a uniform error
  type they can catch.
- `test_flow_discovery_resolves_real_ckan_url` retries once on transient
  network errors. Caught 2 flakes in 10-run stability after the loop had
  cumulatively hit data.gov.au ~130 times — a single retry is sufficient
  and means subsequent stability runs stay clean.

### Added — GST_MONTHLY (first transposed-layout dataset)

- **New curated dataset `GST_MONTHLY`**: monthly Goods and Services Tax /
  Wine Equalisation Tax / Luxury Car Tax collections from ATO Table 1B,
  July 2020 forward. Exposes 10 aliased metrics including `net_gst`,
  `gross_gst`, `input_tax_credits`, `wet_payable`, `net_lct`.
- This is the first transposed-layout curated dataset to ship. The
  transposed code path existed since v0.1 but had three latent bugs
  surfaced and fixed by GST:
  - `_apply_aliases` dropped unaliased columns; transposed datasets need
    the period (date) columns preserved.
  - `shape_transposed` referenced `cd.metric_label_column` directly,
    which is a *source* column name; after alias renaming the df has
    *alias* names. Now resolves source → alias inside the shape layer.
  - `_normalize_period` mis-categorised `"2023-06"` as a financial-year
    suffix; disambiguation rule added so 01-12 = month, 13-99 = FY end.
- 13 new tests in `test_transposed.py` covering all the above plus
  whitespace stripping ("Net GST " → "Net GST" for clean aliasing),
  period-range filter inclusivity, latest-per-measure semantics, CSV
  and series output, and unknown-measure error hints.

### Added — examples/

- `examples/claude_desktop_config_all_three.json`: ready-to-paste
  Claude Desktop config that registers abs-mcp, rba-mcp, and ato-mcp
  side by side with `--upgrade` for auto-PyPI-refresh.
- `examples/claude_desktop_config_local.json`: local-dev variant for
  testing unreleased changes via `uv run --directory ...`.
- `examples/demo_prompts.md`: six copy-paste prompts each demonstrating
  a different sellable angle (property-tech, fintech, charity-tech,
  retirement-tech, B2B intel) with expected numerical answers.

### Added — auto-update layer

- `discovery.py`: a CKAN-driven resolver that finds the freshest
  data.gov.au resource URL for a curated dataset at fetch time. When ATO
  ships Taxation Statistics 2023-24 next year, ato-mcp picks it up without
  a wheel release.
- Optional `discovery:` block on every curated YAML. Two shapes are
  supported:
  - `package_id` + `resource_name` (or pattern) — for fixed packages with
    many resources (e.g. `corporate-transparency`, `acnc-register`).
  - `package_id_pattern` + `organization_id` + `resource_name` — for the
    Taxation Statistics pattern where each year is its own package.
- Discovery failure is silent and safe: callers fall back to the YAML's
  hard-coded `download_url`, so a CKAN outage never breaks a query.
- 18 new discovery tests (mocked CKAN via respx) + 1 live test that
  confirms both discovery shapes against real data.gov.au.

### Added — exhaustive edge testing

- `test_edge_inputs.py` (40 tests): adversarial fuzz across every tool
  surface — None/int/float/list/bool/bytes inputs, blank/whitespace
  strings, very long strings (16KB), Unicode (emoji, RTL, combining,
  mathematical alphabets), special characters (`<script>`, `../`, null
  bytes), URL-injection chars in filter values, type confusion.
- `test_edge_data.py` (15 tests): synthetic XLSX edge cases (NaN cells,
  privacy-suppressed `*`/`na`, trailing blank rows, truncated bodies,
  unicode in CSV, mixed-dtype columns, normalisation of inconsistent
  whitespace around `\\n` in column headers). Includes the canary that
  every curated `source_column` exists in the parsed real file.
- `test_concurrency.py` (3 tests): 50 parallel callers fold to one HTTP
  request; 5 parallel calls to different datasets all succeed; rapid
  sequential calls warm the cache.
- `test_customer_flows.py` (10 tests): realistic multi-step agent
  journeys — search → describe → get_data → compare across postcodes /
  format as CSV / format as series / unhappy path with helpful error /
  response-envelope invariants / every dataset reachable from cold cache.
- `test_cache.py` (10 tests): TTL boundaries, corrupt-DB silent rebuild,
  50 concurrent writes don't race, 10MB payload roundtrip, binary-safe.
- `test_resilience.py` (10 tests): respx-mocked network failures — 404,
  503, timeouts, DNS failures, malformed JSON from CKAN, non-http URL
  rejection, in-flight dedup.

### Tests at a glance
- **202 total** (189 unit + 13 live)
- **10 consecutive full-suite runs** with zero flakes
- Wheel size unchanged at 33KB (data layer adds ~7KB; tests stay outside wheel)

## [0.1.0] — 2026-05-12

First public release. Six curated datasets, five MCP tools, end-to-end tested
against live data.gov.au.

### Added
- `search_datasets`, `describe_dataset`, `get_data`, `latest`, `list_curated`
  tools (FastMCP) — same surface as `abs-mcp` and `rba-mcp` so an agent that
  uses multiple servers gets a uniform shape.
- Curated datasets:
  - `IND_POSTCODE` — Individuals by taxable status × state × SA4 × postcode (Taxation Statistics 2022-23, Table 6A; ~5,200 rows × ~80 measures).
  - `IND_POSTCODE_MEDIAN` — Median + average taxable income by postcode, every year 2003-04 → 2022-23.
  - `COMPANY_INDUSTRY` — Company tax by ANZSIC broad + fine industry (Table 4A).
  - `CORP_TRANSPARENCY` — Entity-level disclosures for the 2023-24 Corporate Tax Transparency report (~4,200 entities).
  - `SUPER_CONTRIB_AGE` — Super contributions by age × sex × taxable income bracket (Table 23A).
  - `ACNC_REGISTER` — Live ACNC charity register (~60,000 entities, weekly refresh).
- HTTP fetcher with SQLite-backed disk cache (`~/.ato-mcp/cache.db`); per-resource TTL tuned for ATO's annual cadence vs ACNC's weekly cadence.
- XLSX + CSV parsers with automatic header-padding normalisation (so curated YAMLs only ever spell one canonical form even when ATO ships small whitespace variations year-to-year).
- 53 unit tests + 3 live integration tests.
- 33KB wheel; all curated dataset specs bundled, data fetched lazily.

### Known limitations
- v0.1 only ships wide-layout (one-row-per-entity) datasets. Transposed
  time-series tables (GST monthly, super-funds aggregate) are slated for v0.2.
- The latest Taxation Statistics release is hard-coded to the 2022-23 file
  URL. v0.2 will auto-discover the newest release via CKAN.
