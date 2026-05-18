# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.8.24] — 2026-05-19

### Fixed

- **Per-thread `_client` cache** (P0 prod bug, observed on ausdata-api):
  module-global `_client` bound to the FIRST event loop and tripped
  `RuntimeError: Event loop is closed` when called from a multi-loop
  host that wraps the MCP and runs `asyncio.run(_get_data_impl(...))`
  in a worker thread per request. Cache moved to `threading.local()`
  so each worker thread gets its own client bound to its own loop.
  `reset_client_for_tests()` now only clears the calling thread.

## [0.8.23] - 2026-05-19

### Fixed — `truncated_at` now fires correctly with shape-time short-circuit

0.8.22's emit-time short-circuit in `shape_wide` / `shape_transposed`
stopped at exactly `limit` records when the natural set overflowed,
which made `build_response`'s post-hoc `original_count > limit` check
a no-op — customers received N rows with `truncated_at=None` even
when many more rows existed upstream.

Fix: shape-time cap is now `limit + 1` (or `_HARD_MAX_RECORDS + 1`),
the standard pagination "+1 sentinel" pattern. shape_wide emits one
record PAST the user's limit if the natural set has more, so the
existing `original_count > limit` truthfully distinguishes truncation
from exact-match. The sentinel row is sliced off before serialisation;
customers never see it.

Behaviour after the fix:
- `get_data(IND_POSTCODE, limit=3)` over 50-row natural set: returns
  3 rows + `truncated_at=4` (at least 4 — really 50 but we stopped
  counting).
- `get_data(IND_POSTCODE, limit=10)` over 10-row natural set: returns
  10 rows + `truncated_at=None` (exact match, nothing more).
- `latest(ACNC_AIS_FINANCIALS, limit=5)` over 853k natural: returns
  5 rows + `truncated_at=100001` (capped at the hard ceiling +1
  sentinel).

CI on 0.8.22 caught the original miss via the new test_shape_cap
suite. The memory profile fix in 0.8.22 is unchanged — this is a
correctness follow-up to that release.

## [0.8.22] - 2026-05-19

### Fixed — ACNC_AIS_FINANCIALS OOM (worker RSS 1.16 GB → ~300 MB)

Backend gateway flagged ato.ACNC_AIS_FINANCIALS as a remaining OOM
risk after the asic-mcp 0.6.14 fix landed. Live profile confirmed:
`latest(ACNC_AIS_FINANCIALS, limit=5)` peaked at **1.16 GB RSS** and
took 62s cold / 53s warm. The pre-existing column-projected `pd.read_csv`
parse path was bounded fine; the OOM was in the **shaping layer**.

Root cause: `shape_wide` materialised the full Cartesian product of
53k charities × 16 measures = **853k `Observation` Pydantic objects**
(~850 MB Pydantic+dict state) BEFORE `build_response` applied the
customer-facing `limit` slice. By the time the `limit=5` cut ran,
the worker was already past 1 GB resident.

Fix: thread a `max_records` short-circuit into `shape_wide` and
`shape_transposed`. The shaping loop stops emitting Observations once
the cap is hit, never building the rejected ~850k. `build_response`
derives the cap from `(limit, last_n, _HARD_MAX_RECORDS)`:

- `latest()` (`last_n=1`) → cap = `_HARD_MAX_RECORDS` (100k floor so
  per-measure period sort+trim still has enough data).
- `get_data(limit=N)` → cap = `min(N, _HARD_MAX_RECORDS)` (short-circuit
  at the customer's soft cap — anything beyond gets sliced post-hoc).
- No limit, no `last_n` → cap = `_HARD_MAX_RECORDS` (absolute safety
  ceiling).

Memory profile (live `latest('ACNC_AIS_FINANCIALS', limit=5)`):
- After fix: ~300 MB peak RSS, ~9s cold / <1s warm ✓
- Before (0.8.21): ~1,160 MB peak RSS, 62s cold ❌

This pattern applies to every wide-layout dataset on the sister, not
just ACNC_AIS_FINANCIALS. Future curations crossing the 100k
Observation threshold get the protection by default; nothing extra
to wire up per dataset.

Tests added in `tests/test_shape_cap.py`: shape_wide short-circuit
correctness, build_response cap derivation, shape_transposed
short-circuit, ACNC synthetic cap smoke test.

## [0.8.21] - 2026-05-18

### Added — `prewarm_curated()` + `ato-mcp --warmup` CLI

Ports abs-mcp 0.11.14's prewarm pattern to ato-mcp. Gateway integration
asked for the equivalent on the other DataResponse sisters to close the
remaining cold-cache surface.

Python API:
```python
from ato_mcp import server as ato_srv
results = await ato_srv.prewarm_curated(max_concurrency=2, log=print)
```
CLI:
```
ato-mcp --warmup
ato-mcp --warmup --warmup-concurrency 1
ato-mcp --warmup --warmup-only IND_POSTCODE,GST_MONTHLY
```

Per-dataset error catching, exits 0/1 by aggregate success. Same shape
as abs-mcp prewarm so gateway init hooks call both with identical
signatures.

326 unit tests pass.

## [0.8.20] - 2026-05-18

### Added — CGT_BY_GAINS_RANGE (distributional capital-gains view)

Customer-feedback queue: "Capital gains by asset type and income bracket"
not curated. ATO publishes CGT in two tables — Table 1 (by asset type ×
entity × year, transposed layout, complex to parse) and Table 2 (net
capital gains by entity × taxable status × gain-amount bracket × year,
clean wide layout).

Added CGT_BY_GAINS_RANGE from Table 2. Distributional CGT view:
  - Individuals taxable 2022-23 g. \$1M+ bracket: \$15.98B realized
    (concentrated wealth signal in CGT $$)
  - 7 brackets from \$1-\$9 up to \$1M+

For per-asset-type breakdown (shares vs property vs unit trusts) see
the CGT Table 1 — transposed-by-year layout requires parser work,
deferred. For age × income × CGT see IND_AGE_INCOME (already curated
in 0.8.17).

326 unit tests pass.

## [0.8.19] - 2026-05-18

### Fixed — HELP_DEBT income_year canonical format ("2024-25", no spaces)

Customer-sim flagged that `ato.HELP_DEBT` returned `income_year='2024 - 25'`
(with spaces) while every other portfolio dataset uses `'2024-25'`
(IND_POSTCODE_MEDIAN, WGEA reporting_year, etc.). The space-padded form
broke gateway-side period extraction and was a "bad data" inconsistency.

Added `value_replacements` field to `CuratedColumn` (generic
substring-replacement applied after dtype coercion). HELP_DEBT's
income_year YAML maps `" - " → "-"`, so records now arrive as
`income_year='2024-25'` matching the portfolio convention.

326 unit tests pass.

## [0.8.18] - 2026-05-18

### Improved — year-range hint instead of misleading fuzzy match

For year-shaped dims (4-digit numeric), filter typo errors now report
the valid range AND the direction of the miss instead of fuzzy-matching
to a similar number ('2022' → '2002' was the misleading behaviour).
Matches aihw 0.4.16 / apra 0.8.18 / asic 0.6.13 design.

326 unit tests pass.

## [0.8.17] - 2026-05-18

### Added — `IND_AGE_INCOME` curated (Tax Statistics Table 3A — by age × income range)

Customer-sim flagged the distributional-analysis use case as unreachable —
ATO publishes detailed sex × age × taxable-income-range data with all
the CGT, dividend, rental, and superannuation columns in Table 3A, but
no curated dataset exposed it. Resolved by curating `IND_AGE_INCOME`.

Dimensions:
- `sex` (Male / Female — Table 3A does NOT publish a Persons total)
- `taxable_status` (Taxable / Non Taxable)
- `age_range` (13 bands from Under 18 to 75+, plain-English keys like
  `55_59`, `65_69`, `75_plus`)
- `taxable_income_range_tax_brackets` (5 bands aligned with 2022-23 tax
  brackets: `nil`, `18k_45k`, `45k_120k`, `120k_180k`, `180k_plus`)
- `taxable_income_range` (fine-grained ~80 bands for drill-down)

Measures include `capital_gain_net`, `capital_gain_total_current_year`,
`capital_losses_carried_forward`, `rent_interest_deductions`,
`rent_other_deductions`, `super_contributions_personal`,
`super_contributions_employer_reportable`, `dividends_franked_total`,
`franking_credits_total`, `salary_wages_total`, and more.

Verification:
- Net capital gains for 55-59 males taxable: $2.37B
- Rent interest deductions distribution @ $45k-120k bracket peaks at
  age 40-44 ($1.45B), declining to $55M for 75+

326 unit tests pass.

## [0.8.16] - 2026-05-18

### Added — `limit` parameter on `get_data` for register-shaped datasets

Customer-sim flagged ACNC_AIS_FINANCIALS as gateway-blocked because
the underlying slice is huge (~50k charities × 16 measures = 800k+
records). The portfolio-wide `_HARD_MAX_RECORDS=100,000` cap kicked
in, but the gateway needed a customer-facing way to ask for a smaller
slice without forcing the agent through `latest()`.

Added `limit: int | None = None` parameter to `get_data`. Truncated
responses set `DataResponse.truncated_at` to the original row count.
The hard 100k ceiling remains for callers that don't pass `limit`.

### Fixed — short-query ranker misses ('GST' losing to TAX_GAPS)

Single-token queries ('gst', 'abn', 'super') were length-penalised by
`token_set_ratio` against long dataset names — `token_set_ratio('gst',
'GST_MONTHLY GST, WET & LCT Monthly Collections...')` returns ~7 even
though "gst" appears twice in the haystack. TAX_GAPS (which has 'gst gap'
as a keyword but doesn't contain "gst" in its name) was winning short
queries because of slight name_high advantages from comparable token
counts.

For queries with ≤2 tokens we now ALSO compute `partial_ratio` (substring
overlap) and take the max of `token_set_ratio` and `partial_ratio` for
the name_high pool. Substring matches now score 100 regardless of name
length, so single-keyword queries find their canonical dataset.

Verification:
- 'gst' → GST_MONTHLY at 100, TAX_GAPS at 79.7 (was TAX_GAPS at 76.5
  beating GST_MONTHLY at 71.1)
- 'super' → SMSF_FUNDS + SUPER_CONTRIB_AGE both at 100 (both legitimate)
- 'GST collections' → GST_MONTHLY alone at 100
- 'company tax' → COMPANY_INDUSTRY alone at 100

326 unit tests pass.

## [0.8.15] - 2026-05-18

### Fixed — CI lint failure (unused `rapidfuzz.process` import)

0.8.14 release CI failed lint because the two-pool/three-pool ranker
refactor in 0.8.12 left `from rapidfuzz import fuzz, process` behind
after dropping `process.extract`. Re-shipping with the import cleaned
up and the catalog/shaping import block normalised by `ruff --fix`.

No runtime change vs 0.8.14 — just unblocks the OIDC publish workflow.

## [0.8.14] - 2026-05-18

### Fixed — latest() now caps register-shaped datasets

Customer-sim flag: `latest('ACNC_REGISTER')` returned all 65,566 charities
(~40 MB JSON, ~10M tokens) — blowing agent context windows. The
documented contract ("latest() caps register dumps via limit +
truncated_at") wasn't enforced for ATO's wide-layout register datasets.

Added `limit` parameter to `latest()` (default 50, max 10,000) — matches
asic-mcp's pattern. Truncated responses set `truncated_at` to the
original row count so callers can detect + surface the truncation.

For time-series datasets (GST_MONTHLY etc.) `last_n=1` continues to trim
to the latest period; the new `limit` cap is a no-op there.

Examples:
- `latest('ACNC_REGISTER')` → 50 rows, truncated_at=65,566 (was 65k rows / 40 MB)
- `latest('ACNC_REGISTER', filters={'state': 'nsw'})` → 50 NSW charities,
  truncated_at=20,225
- `latest('ACNC_REGISTER', limit=500)` → 500 rows, truncated_at=65,566

326 unit tests pass.

## [0.8.13] - 2026-05-18

### Improved — high-confidence "Did you mean?" on free-form dimensions

Previously, filtering by a non-enum dimension (industry_broad,
industry_fine, postcode, age_range) with a typo or truncated value
returned 0 rows silently. Customer-impact: gateway returns an empty
result with no hint that the filter value was wrong.

Now, after applying each filter: if the result is empty AND the dim has
no enum AND difflib finds a HIGH-CONFIDENCE close match (cutoff=0.7),
raise a ValueError with the suggestion. Cutoff is intentionally strict
so legitimately-empty results (a real but unmatched value, security-
injection chars, unicode) still pass through silently — only "almost-
right" typos trigger.

Examples:
- `industry_broad='A. Agriculture, Forestry'` (truncated) →
  "Did you mean 'A. Agriculture, Forestry and Fishing'?"
- `industry_broad='banking'` → silent 0 rows (no close match; legitimate
  "no banks in this slice" or wrong-classification system response)
- `postcode='9999'` → silent 0 rows (real-form but absent value)

326 unit tests pass.

## [0.8.12] - 2026-05-18

### Fixed — three-pool ranker matches portfolio standard

Two-pool ranker shipped in 0.8.10/0.8.11 had clamp-to-100 saturation:
broad customer queries (e.g. 'income by postcode') tied multiple
datasets at rel=100 because the final `min(..., 100.0)` collapsed
their distinct raw scores.

Switched to three-pool design (portfolio-consistent with apra 0.8.13 /
aihw 0.4.12 / asic 0.6.8 / rba 0.8.3):

- `id+name` token_set_ratio = PRIMARY discriminator
- keywords broaden recall at KEYWORD_WEIGHT=0.4
- description capped at 30, weight 0.3
- PHRASE_BONUS=15 when query is contiguous substring of keyword haystack
- proportional scaling against leader's raw — no pre-sort clamp

Verification (post-fix, single-leader queries that previously tied):
- 'charity revenue' → ACNC_AIS_FINANCIALS at 100, ACNC_REGISTER at 50.7
- 'income by postcode' → IND_POSTCODE_MEDIAN at 100, IND_POSTCODE at 85.3
- 'gst collections' → GST_MONTHLY at 100
- 'tax gap' → TAX_GAPS at 100
- 'foreign owned residential' → FOREIGN_OWNERSHIP_RESIDENTIAL_BY_COUNTRY at 100

326 unit tests pass.

## [0.8.11] - 2026-05-18

### Docs — SMSF_FUNDS record granularity now explicit

Customer-sim flagged ambiguous semantics: `limit=1` on SMSF_FUNDS
returns one (year, measure) record, not "the latest snapshot across
all 5 measures". Added an explicit `**Record granularity**` note to
the YAML description so `describe_dataset('SMSF_FUNDS')` makes the
shape obvious. Customers wanting a full latest-year snapshot should
pass `last_n=1` without `measures` filter — returns one record per
measure for the latest year.

No data shape change.

## [0.8.10] - 2026-05-18

### Fixed — three customer-sim issues

**1. `ACNC_AIS_FINANCIALS` capped at 100k records (was 853k).**
The YAML's `max_rows: 100000` capped INPUT charity rows during streaming
parse, but the wide-layout pipeline then exploded 53k charities × 16
measures into 853k records, overflowing the ausdata-api 20s gateway
budget on Pydantic + JSON serialisation. Added a portfolio-wide
`_HARD_MAX_RECORDS = 100_000` ceiling in `build_response` — records
exceeding the cap get truncated and `truncated_at` is set to the
original count. Customers needing the full dataset should filter
server-side or pull the source CSV directly.

**2. `IND_POSTCODE_MEDIAN` now respects `start_period` / `end_period`.**
Wide-layout records were emitted with `period=None` because the period
is encoded in the measure key (`median_taxable_income_2022_23`). The
filter never had a period field to compare against, so every query
returned the full 2003-04 to 2022-23 history. `shape_wide` now extracts
the `_YYYY_YY` suffix from each measure key and populates
`Observation.period`. `build_response` then applies the start/end
period filter on wide-layout records (mirroring how transposed-layout
already worked).

**3. Two-pool search ranker.**
The previous WRatio-only ranker collapsed every ato dataset to ~57
relevance for any query (e.g. "median income by postcode" returned 12
datasets tied at 57). Replaced with a token_set_ratio over the
id+name+keywords pool plus a capped WRatio over the description pool.
Live verified:
- "median income by postcode" → IND_POSTCODE_MEDIAN rel=100 (top)
- "charity revenue by ANZSIC" → ACNC_AIS_FINANCIALS rel=90 (top)
- "foreign ownership china" → FOREIGN_OWNERSHIP_* rel=100 (top)
- "cash rate history" → top ato result drops to 53 (sister correctly
  no longer dominates a non-ato query)

### Internal

- `_HARD_MAX_RECORDS = 100_000` constant in `shaping.py`.
- `_WIDE_PERIOD_FY_SUFFIX_RE` regex + `_period_from_measure_key()`
  helper extract `YYYY-YY` from measure keys.
- `last_n` trim now skips when each measure has only one distinct
  period (e.g. wide-layout single-FY measures) — preserves
  pre-fix behaviour for `latest()` on wide datasets.
- 326 tests pass.

## [0.8.9] - 2026-05-18

### Added — `DatasetSummary.relevance` populated by `search_datasets()`

`search_datasets()` results now carry their RapidFuzz WRatio score
on the `relevance` field (0-100, rounded to 1dp). Previously the
score was computed internally for sort order but discarded before
returning, so direct-MCP callers (Claude Code etc.) had to re-rank
their UI themselves. The ausdata-api gateway already re-ranks across
sources; this change is for non-gateway consumers.

`relevance: None` when the entry came from `list_curated()`.

## [0.8.8] - 2026-05-18

### Performance — Parquet on-disk parsed-DataFrame cache

Customer-sim agent re-confirmed cold-call timeouts on
`ACNC_AIS_FINANCIALS` and `HELP_DEBT` on the live gateway even after
0.8.6's `asyncio.to_thread` wrap. The in-memory LRU only helps warm
calls — on Fly's daily rebuild / worker bounce the first request paid
the full 4-9s pandas parse, which combined with network fetch + JSON
serialisation tripped the gateway's 20s budget.

Added a Parquet on-disk fallback (mirror of wgea 0.6.4 / aihw 0.4.9):

- After parse, DataFrame is persisted to
  `~/.ato-mcp/parquet-cache/{sha256-of-key}.parquet` (overridable via
  `ATO_MCP_PARQUET_CACHE_DIR`).
- Before parsing, check the file: read with `pd.read_parquet` in
  ~0.5-1s on warm cache, much cheaper than the cold pandas re-parse.
- TTL: 24h, matching ATO/ACNC's publish cadence.
- Self-heal: corrupted Parquet unlinked, falls through to fresh parse.
- Atomic write via `.tmp` + rename.

### Performance — `ACNC_AIS_FINANCIALS` hard-capped at 100k rows

The full file is ~853k rows × ~30 columns. Even after the parse
completes, Pydantic + JSON serialisation of 853k records on the
gateway path was exceeding the 20s budget. Added `max_rows: 100000`
to the curated YAML so the streaming reader caps row count at parse
time. Customers needing the full set should filter via the `filters`
parameter (e.g. `{"state": "NSW"}` narrows to ~300k) or pull the CSV
from data.gov.au directly.

### Internal

- Added `pyarrow>=15` dep.
- New `parquet_cache.py` module.
- `reset_df_cache_for_tests()` now clears both LRU + Parquet.
- Conftest fixture isolates `ATO_MCP_PARQUET_CACHE_DIR` per session.
- 326 tests pass.

## [0.8.7] - 2026-05-17

### Improved — transport-agnostic error hints (no MCP-tool-name references)

`ValueError` hints in `curated.py` and `shaping.py` previously said
`Try describe_dataset({id!r}) to see all allowed values`. That MCP
tool-name leaks into REST-gateway responses (where the user is hitting
`/v1/describe/{id}`) and Python-library callers (who'd call the
function directly). Rewrote the affected hints to read "Use the
describe endpoint or describe tool to see all allowed values for X" —
same intent, no transport-specific noise. Mirrors the rba-mcp 0.7.5
guard. Added a portfolio-style regression in `test_server_validation.py`
that AST-walks `curated.py` / `shaping.py` and asserts no string
literal contains an MCP tool reference (`describe_dataset(`,
`search_datasets(`, `list_curated(`, `get_data(`, `latest(`).
No runtime behaviour change beyond the wording.

## [0.8.6] - 2026-05-17

### Fixed — event-loop blocking on sync CSV/XLSX parse

`_fetch_and_parse` called `read_csv` / `read_csv_streaming` / `read_xlsx`
synchronously inside an async tool body. The largest ATO/ACNC CSVs
(`ACNC_AIS_FINANCIALS` ~36MB / 91 cols / 50k+ rows; `ACNC_REGISTER`
~50MB) blocked the event loop for seconds, serialising every concurrent
request behind a single parse and stalling downstream consumers like
`ausdata-api` against its 20s gateway budget. Wrapped all three parse
call-sites in `asyncio.to_thread` so the work runs on the default
executor without blocking other in-flight tool calls. Matches the
0.4.7 / 0.6.4 / 0.8.6 fixes in `aihw-mcp` / `asic-mcp` / `apra-mcp`.

## [0.8.5] - 2026-05-16

### Performance — extend the streaming-CSV path to `ACNC_REGISTER`

The user's live API testing hit the same OOM / timeout pattern on
`ACNC_REGISTER` (the ~50MB / 69-column / 65k-row charity register) that
motivated the 0.8.4 fix for `ACNC_AIS_FINANCIALS`. Same shape of CSV,
same Python-object memory blowup when loaded whole via `pd.read_csv`.
Routed `ACNC_REGISTER` through `parsing.read_csv_streaming` by adding
it to the `_STREAMING_CSV_DATASETS` set in `server._fetch_and_parse` —
no parsing-layer changes needed; the column-projected streaming reader
introduced in 0.8.4 was already generic enough to handle this dataset.

Verified against the live source: `get_data("ACNC_REGISTER")` returns
65,566 rows in ~12s with peak parser memory ~140MB (pre-fix: OOM /
indefinite hang on 512MB hosts). `ACNC_AIS_FINANCIALS` continues to
work unchanged (verified post-fix: 53,350 rows / ~20s / ~180MB peak).

Added a dedicated `tests/test_acnc_register.py` with:
- `test_acnc_register_streams_under_memory_budget` — tracemalloc budget
  on the 30KB head fixture; catches a regression where the column
  projection silently breaks.
- `test_acnc_register_get_data_path_uses_streaming_reader` — pins the
  server-dispatch wiring so removing `ACNC_REGISTER` from
  `_STREAMING_CSV_DATASETS` fails the suite immediately.
- `test_acnc_register_streaming_preserves_all_curated_columns` — every
  curated source column survives the streaming-read projection.

## [0.8.4] - 2026-05-16

### Performance — `ACNC_AIS_FINANCIALS` parsing (production-blocking OOM fix)

`ACNC_AIS_FINANCIALS` previously loaded the full 36MB / 91-column / 53k-row
data.gov.au CSV via `pd.read_csv(...)`, materialising every source column
as Python objects before any filter was applied. On 512MB-RAM hosted
instances this consistently OOM'd; on dev machines it spiked peak memory
to ~1.15GB for a single query. Switched the parsing path to a
column-projected reader (`parsing.read_csv_streaming`) that uses pandas'
native `usecols=` argument to skip the 68 unused source columns entirely.
For realistic filtered queries (e.g. `latest(filters={"abn": "..."})`,
`get_data(filters={"charity_size": "large"}, measures="total_revenue")`)
the parse path now peaks at ~70 MB and completes in <2s, down from
~150 MB / 25s. No customer-visible change to the `DataResponse` shape or
record values — same `row_count`, same observations, same dtypes — only
the memory profile and warm-cache latency improved.

The streaming reader is wired in via a narrow `_STREAMING_CSV_DATASETS`
set in `server._fetch_and_parse`; all other CSV datasets (ACNC_REGISTER
etc.) continue to use the full-load `pd.read_csv` path unchanged.

Added unit tests:
- `test_acnc_ais_financials_streams_under_memory_budget` (tracemalloc
  budget assertion on the 200KB head-sample fixture).
- `test_acnc_ais_streaming_reader_matches_pandas_for_curated_columns`
  (cell-for-cell equivalence vs `pd.read_csv` after the downstream
  `_coerce_dtypes` step).
- Plus 6 smaller streaming-reader hygiene tests (empty body, no matching
  columns, NaN handling, server-dispatch wiring).

## [0.8.2] - 2026-05-16

### Fixed — JSON-string `filters` parameter (portfolio-wide)

The MCP protocol JSON-encodes dict parameters before they reach the
server. `_validate_filters` was checking `isinstance(filters, dict)`
before parsing the JSON string, so every call of the form
`get_data(filters={"state":"nsw"})` from a real MCP client was rejected.
Fix: decode JSON-string filters before the type check. Coordinated
patch across the portfolio (abs 0.9.2, ato 0.8.2, apra 0.8.2, asic 0.6.1,
aihw 0.4.2, wgea 0.5.1, aemo 0.4.2).

## [0.8.1] - 2026-05-16

### Fixed

- `FOREIGN_OWNERSHIP_AG_LAND`: `foreign_held_hectares` (raw Ha) renamed to
  `foreign_held_million_ha` and normalised to million hectares — now in
  consistent units with `total_aust_ag_land_million_ha`. Previous column
  pairing caused ~1,000,000× scale errors in customer-computed ratios
  (e.g. `foreign / total` returned 130,000 instead of ~0.13). The
  normalisation runs in `shaping._normalise_units` so other datasets are
  unaffected.

### Added

- `@pytest.mark.live` integration tests for `FOREIGN_OWNERSHIP_AG_LAND` and
  `FOREIGN_OWNERSHIP_RESIDENTIAL_BY_COUNTRY` with range-check assertions:
  AG-land verifies every year's `foreign_held_million_ha /
  total_aust_ag_land_million_ha ≈ foreign_ownership_pct / 100` invariant
  (closes the units gap); residential verifies China is consistently the
  highest-count country and all counts land in the 0-100,000 register
  ceiling.

## [0.8.0] - 2026-05-16

### Added — FOREIGN_OWNERSHIP_RESIDENTIAL_BY_COUNTRY

- **`FOREIGN_OWNERSHIP_RESIDENTIAL_BY_COUNTRY` curated dataset.** Counts
  of registered foreign-owned residential property interests in Australia
  by country of control. Source: ATO Register of Foreign Ownership of
  Australian Assets — Residential Land workbook, Table 9. 2 reporting
  periods: 30 June 2024 + 30 June 2025.
- Closes a real customer gap for real-estate analysts (Domain, REA,
  CoreLogic), property funds, FIRB-context policy researchers,
  journalists, political offices. Answers "which countries control
  the most AU residential property?", "is Chinese foreign ownership
  growing or shrinking?", "how does Hong Kong compare to Singapore for
  AU residential investment?".
- Top countries at 30 June 2025: China 22,272 (down from 23,550 in
  2024) · Hong Kong 3,396 · Singapore 1,978 · Malaysia 1,795 ·
  Japan 1,711 · Vietnam 1,658 · India 1,235 · Indonesia 1,133 ·
  UK 1,070 · Taiwan 979 · USA 484.
- 20 country aliases — pass `china`, `prc`, `hong_kong`, `uk`, `usa`,
  `south_korea`, etc.
- Uses existing transposed XLSX parser. YAML-only addition.

### Customer-value validation (live ATO fetch, 2026-05-16)

- Real-estate analyst: `latest('FOREIGN_OWNERSHIP_RESIDENTIAL_BY_COUNTRY',
  measures='china')` → 22,272 interests (30 June 2025).
- Search routing: "foreign property", "chinese property", "foreign home
  buyers", "foreign residential" all hit
  FOREIGN_OWNERSHIP_RESIDENTIAL_BY_COUNTRY at #1 (correctly
  outranking FOREIGN_OWNERSHIP_AG_LAND for residential queries).

### Tests

- 314 unit tests passing (was 314). Ruff clean.

## [0.7.0] - 2026-05-16

### Added — FOREIGN_OWNERSHIP_AG_LAND (Register of Foreign Ownership)

- **`FOREIGN_OWNERSHIP_AG_LAND` curated dataset.** 10 annual snapshots
  (30 June 2016 → 30 June 2025) of foreign-owned share of Australian
  agricultural land from the ATO Register of Foreign Ownership of
  Australian Assets (commenced 1 July 2023; pre-2023 figures from the
  predecessor Agricultural Land Register).
- Headline statistic: as of 30 June 2025, **13.0% of Australian
  agricultural land has a foreign interest** (50.3M of 387M hectares).
  Peaked at 14.1% in 2021, dropped to 12.3% in 2022 post-COVID
  divestments, has since recovered to ~13%.
- Closes a real customer gap for foreign-investment analysts, M&A
  advisers on agribusiness deals, agricultural land valuers, real-
  estate firms tracking foreign capital, and FIRB-context queries.
  Search routes "foreign ownership", "foreign farmland", "firb",
  "foreign agricultural" all to FOREIGN_OWNERSHIP_AG_LAND at #1.
- Uses existing XLSX wide-layout parser with CKAN auto-discovery.
  YAML-only addition; no new code paths.
- Note: wide-layout datasets surface period in
  `observation.dimensions["year"]` rather than as the response's
  top-level `period` field (matches existing HELP_DEBT / SMSF_FUNDS
  pattern). YAML description documents this for client developers.

### Customer-value validation (live ATO fetch, 2026-05-16)

- Foreign-investment analyst: 10-row time series 2016-2025 returns
  13.6 / 13.6 / 13.4 / 13.8 / 13.8 / 14.1 / 12.3 / 12.9 / 12.7 / 13.0%.
- Hectares: 52.1M ha foreign-held at 30 June 2016; 50.3M at 30 June 2025.
- Search routing: 5/5 foreign-ownership related queries hit
  FOREIGN_OWNERSHIP_AG_LAND at #1.

### Tests

- 314 unit tests passing (was 314). 10× zero-flake gauntlet. Ruff clean.

### Note on register coverage

ATO's foreign-ownership register also publishes 5 other asset classes
(Business interests, Commercial land, Mining/exploration tenements,
Residential land, Water interests). Only Agricultural Land has a
multi-year national time series (2016+); the others started with the
consolidated register on 1 July 2023. Adding the remaining 5 as
separate curated datasets is a natural follow-up iteration.

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
