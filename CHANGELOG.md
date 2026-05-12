# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased] — 2026-05-12

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
