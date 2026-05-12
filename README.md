# ato-mcp

[![tests](https://github.com/Bigred97/ato-mcp/actions/workflows/test.yml/badge.svg)](https://github.com/Bigred97/ato-mcp/actions/workflows/test.yml)
[![PyPI](https://img.shields.io/pypi/v/ato-mcp.svg)](https://pypi.org/project/ato-mcp/)
[![Python](https://img.shields.io/pypi/pyversions/ato-mcp.svg)](https://pypi.org/project/ato-mcp/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Glama MCP server quality](https://glama.ai/mcp/servers/Bigred97/ato-mcp/badges/score.svg)](https://glama.ai/mcp/servers/Bigred97/ato-mcp)

**MCP server for Australian Taxation Office statistics.** Plain-English access to personal tax by postcode, company tax by industry, corporate tax transparency for every $100M+ entity, super contributions by age, salary by occupation, monthly GST collections, and the live ACNC charity register — all from a single `uvx` command.

```text
"What's the median taxable income in postcode 2000?"
"How much tax did BHP pay last year?"
"Which industries have the highest gross income?"
"How many Large charities are there in NSW?"
"What's the average super contribution for under-30s in the top tax bracket?"
```

Sister to [abs-mcp](https://github.com/Bigred97/abs-mcp) (Australian Bureau of Statistics), [rba-mcp](https://github.com/Bigred97/rba-mcp) (Reserve Bank of Australia), and [au-weather-mcp](https://github.com/Bigred97/au-weather-mcp) (Australian weather via Open-Meteo + BOM). The four together cover the macro / regulator / tax / climate layer of Australian official data.

---

## Install

```bash
# Run on demand via uvx (recommended)
uvx --upgrade ato-mcp

# Or install permanently
pip install ato-mcp
```

### Claude Desktop

Add to `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "ato": { "command": "uvx", "args": ["--upgrade", "ato-mcp"] }
  }
}
```

> **Why `--upgrade`?** `uvx ato-mcp` (without the flag) uses whatever wheel is cached and never adopts new PyPI releases on its own. `--upgrade` makes uvx check PyPI on each launch and pull a newer release if one exists. To verify which version is currently serving you, look at the `server_version` field on any `DataResponse`.

### Claude Code / Cursor

```bash
claude mcp add ato --command uvx --args -- --upgrade ato-mcp
```

## Auto-updating data

Beyond the wheel-level `--upgrade`, the server has a second auto-update path **inside** the data layer: when ATO publishes Taxation Statistics 2023-24 next year, ato-mcp resolves the new resource URL via [data.gov.au's CKAN API](https://data.gov.au/data/api/3/action/package_show) at fetch time and uses the freshest match. Hard-coded YAML URLs are the safe fallback if discovery fails. You do **not** need to wait for a new wheel release to get new yearly data — just delete `~/.ato-mcp/cache.db` to force a refresh, or wait for the 7-day TTL to expire.

---

## What it exposes

Six tools, all plain-English in, structured out:

| Tool                | Purpose                                                       |
|---------------------|---------------------------------------------------------------|
| `search_datasets`   | Fuzzy-search the curated catalog by keyword                   |
| `describe_dataset`  | List a dataset's filterable dimensions and returnable measures |
| `get_data`          | Query with `filters`, `measures`, period range, output format |
| `latest`            | Last observation per measure (shortcut)                       |
| `top_n`             | Rank rows by a measure, return top (or bottom) N              |
| `list_curated`      | Enumerate the curated dataset IDs                             |

Every response is the same shape — `dataset_id`, `dataset_name`, `query`, `period`, `unit`, `row_count`, `records`, `ato_url`, `attribution`, `server_version` — across every curated dataset.

---

## Curated datasets (10 in v0.1)

| ID                    | What it is                                                                          | Period             | Coverage                  |
|-----------------------|-------------------------------------------------------------------------------------|--------------------|---------------------------|
| `IND_POSTCODE`        | Personal tax stats by taxable status × state × SA4 × postcode (~5,200 postcodes)    | 2022-23            | 80+ measures              |
| `IND_POSTCODE_MEDIAN` | Median & average taxable income by postcode, every year                             | 2003-04 → 2022-23  | 21 yearly measures        |
| `COMPANY_INDUSTRY`    | Company tax by ANZSIC broad + fine industry                                         | 2022-23            | 216 industry cells        |
| `CORP_TRANSPARENCY`   | Entity-level tax disclosure for $100M+ corporations (name, ABN, income, tax)        | 2023-24            | ~4,200 entities           |
| `SUPER_CONTRIB_AGE`   | Super contributions by age × sex × taxable income bracket                            | 2022-23            | Employer/personal/other   |
| `ACNC_REGISTER`       | Live register of every Australian charity (ABN, size, jurisdiction, beneficiaries)   | Current (weekly)   | ~60,000 entities          |
| `GST_MONTHLY`         | Monthly GST / WET / LCT collections (gross GST, input tax credits, net GST, etc.)   | 2020-07 → 2024-06  | 10 metrics × 48 months    |
| `ATO_OCCUPATION`      | Median/average income (taxable, salary/wage, total) by ANZSCO occupation × sex      | 2022-23            | ~1,200 jobs × 7 measures  |
| `SMSF_FUNDS`          | SMSF sector size — total funds, total members, total gross assets (trillion-$ sector) | 2019-20 → 2024-25  | 3 metrics × 6 years       |
| `SBB_BENCHMARKS`      | Industry total-expense + COGS ratio bands by turnover bracket (~100 industries) | 2023-24            | 12 measures × 100 industries |

Adding a new dataset is a single YAML drop into `src/ato_mcp/data/curated/` — see [CONTRIBUTING.md](CONTRIBUTING.md).

---

## Example queries (paste into Claude)

**Property-tech**: *"For postcodes 2000, 2008, 2026, and 2031 in NSW, give me the median taxable income across every available year so I can compare trajectories."*

**Corporate tax**: *"Get the total income, taxable income, and tax payable for BHP IRON ORE (JIMBLEBAR) PTY LTD."*

**Industry analysis**: *"Which fine industry codes under 'C. Manufacturing' have the highest total income, and how many companies are in each?"*

**Charity/non-profit tech**: *"Find every charity in NSW with size 'Large' that operates_in_VIC = Y."*

**Retirement planning**: *"What's the average personal super contribution for males aged 30-39 in the $120,001–$180,000 bracket?"*

Each prompt resolves to one `get_data` call. The response includes the source URL so the agent can cite it back.

---

## Architecture

Same shape as the sister packages — `client → cache → parsing → shaping → server`:

- **`client.py`** wraps `httpx` with a SQLite-backed disk cache (per-resource TTL).
- **`parsing.py`** reads XLSX (via `openpyxl`/`pandas`) and CSV (via `pandas`). Header rows + sheet names live in the curated YAML so future format quirks are a YAML edit, not a code change.
- **`curated.py`** loads dataset specs from `data/curated/*.yaml` — each one declares its dimensions, measures, dimension value enums, source/download URLs, format, and parse layout.
- **`shaping.py`** transforms the parsed DataFrame into `DataResponse` (records / series / csv).
- **`server.py`** is the FastMCP entrypoint — six tools, full input validation with helpful "Try X" hints on error.

Cache lives under `~/.ato-mcp/cache.db`. Data on data.gov.au refreshes once a year (ATO) or weekly (ACNC), and the TTLs are tuned for that.

---

## Attribution

Data sourced from the Australian Taxation Office and the Australian Charities and Not-for-profits Commission, both via [data.gov.au](https://data.gov.au/). Licensed under [Creative Commons Attribution 3.0 Australia (CC BY 3.0 AU)](https://creativecommons.org/licenses/by/3.0/au/). The MCP server is MIT-licensed; the data carries the upstream CC-BY 3.0 AU licence, which is echoed in every response's `attribution` field.

---

## Sister packages

- [abs-mcp](https://github.com/Bigred97/abs-mcp) — ABS census and economic statistics (unemployment, CPI, GDP, population, building approvals)
- [rba-mcp](https://github.com/Bigred97/rba-mcp) — RBA statistical tables (cash rate, FX rates, mortgage rates, money market)
- **ato-mcp** — this one. Tax, super, and charity registers.
- [au-weather-mcp](https://github.com/Bigred97/au-weather-mcp) — Australian weather via Open-Meteo + BOM. 21 curated locations + postcode/place-name lookup, current observations, 16-day forecasts, 80yr historical archive.

All four are designed to compose: an agent can ask for "unemployment + cash rate + median income + climate" in postcode 2000 and one shot fans out across four MCPs.

---

## Roadmap (next iterations)

- v0.2: `GST_MONTHLY` transposed time series; multi-year `CORP_TRANSPARENCY`; `ATO_OCCUPATION` (salary by occupation code)
- v0.3: hosted version with [x402](https://x402.org/) per-call paywall; programmatic SEO pages
- v0.4: listing on MCPay + Apify; paid tier for high-volume agent users

[CHANGELOG](CHANGELOG.md) tracks every release.

---

## Development

```bash
git clone https://github.com/Bigred97/ato-mcp.git
cd ato-mcp
uv venv
uv pip install -e ".[dev]"
pytest                  # 53 unit tests, ~7s
pytest -m live          # 3 integration tests against data.gov.au, ~3s
```

Issues, ideas, and contributions welcome: [github.com/Bigred97/ato-mcp/issues](https://github.com/Bigred97/ato-mcp/issues).
