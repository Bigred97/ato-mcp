# Demo prompts

Copy-paste any of these into Claude Desktop (or any MCP client with `ato-mcp` enabled). Each prompt forces a tool call against a real ATO/ACNC dataset and returns a concrete, verifiable answer.

All values below were verified live against `data.gov.au` on 2026-05-12. If your screenshot disagrees with the values shown, either the tool wasn't called or data.gov.au shipped a new release (the auto-discovery layer should resolve the freshest data, so re-running should work).

---

## 1. Corporate tax — high-recognition entity

> What did BHP Iron Ore (Jimblebar) Pty Ltd pay in Australian corporate income tax for 2023-24? Show me total income, taxable income, and tax payable side by side.

**Expected**: `total_income $10.26B`, `taxable_income $7.04B`, `tax_payable $2.11B`. Tool: `ato:get_data` on `CORP_TRANSPARENCY` with `entity_name` filter.

## 1b. Top corporate taxpayers (uses `top_n` directly)

> Who paid the most corporate income tax in Australia in 2023-24? Give me the top 10 entities ranked by tax payable.

**Expected**: Rio Tinto ($6.25B), BHP ($6.01B), Fortescue ($3.93B), Chevron ($3.52B), CommBank ($3.43B), ... Tool: `ato:top_n("CORP_TRANSPARENCY", "tax_payable", n=10)`. One call, ranked server-side.

---

## 2. Property-tech — postcode income trajectory

> Pull the median taxable income for postcode 2000 (Sydney CBD) across every available year — 2003-04, 2013-14, 2019-20, 2020-21, 2021-22, and 2022-23 — and plot it. Note real growth in nominal AUD.

**Expected**: 2003-04 ~$12,133 → 2022-23 ~$42,667. Monotonically increasing nominal series. Tool: `ato:get_data` on `IND_POSTCODE_MEDIAN`.

---

## 3. Suburb comparison — ranked bar chart

> Compare the 2022-23 median taxable income across these Sydney postcodes: 2000, 2008 (Pyrmont), 2026 (Bondi), 2027 (Darling Point), 2031 (Randwick). Rank them highest to lowest and show me the gap between top and bottom.

**Expected**: 5 records. 2027 (Darling Point) at the top, low-density CBD postcodes lower. Tool: `ato:get_data` with multi-value postcode filter.

## 3b. Top 20 NSW postcodes by income (uses `top_n`)

> What are the 20 highest-earning postcodes in NSW by 2022-23 median taxable income? Rank them.

**Expected**: Inner-west and eastern-suburbs Sydney dominate. 2043 (Erskineville/Newtown ~$92k), 2039 (Rozelle ~$90k), 2028 (Double Bay ~$89k), 2061 (Kirribilli), 2062 (Cremorne)... Tool: `ato:top_n("IND_POSTCODE_MEDIAN", "median_taxable_income_2022_23", n=20, filters={"state": "nsw"})`.

---

## 4. Nonprofit-tech — charity finder

> Find every charity in NSW with size "Large" that also operates in Victoria. Tell me how many there are, then show the first 10 by name with their postcode and whether they're a registered Public Benevolent Institution.

**Expected**: ~hundreds of Large NSW-registered charities, of which a meaningful subset also operate in VIC. Tool: `ato:get_data` on `ACNC_REGISTER` with combined filters.

---

## 5. B2B / industry intel — company-tax-by-industry

> Which broad ANZSIC industries have the highest total reported income on 2022-23 company tax returns? Rank the top 5 and show how many companies are in each segment.

**Expected**: Financial Services, Mining, Manufacturing typically lead. Tool: `ato:get_data` on `COMPANY_INDUSTRY`. Claude will fetch all rows then sort/rank locally.

---

## 5b. HR-tech / career-planning — top-earning jobs

> Use the ATO occupation data to find the top 10 highest-paid occupations in Australia by median taxable income in 2022-23. Show me the occupation, the median, and how many people work in each.

**Expected**: Otorhinolaryngologist $516k, Neurosurgeon $486k, Plastic Surgeon $459k, Ophthalmologist $458k, Urologist $450k, Cardiologist $449k, Judge $438k, Anaesthetist $425k. Tool: `ato:top_n("ATO_OCCUPATION", "median_taxable_income", n=10, filters={"sex": "total"})`.

## 5c. Gender pay gap by occupation

> What's the median taxable-income gap between female and male software programmers in Australia? Pull both rows from the ATO occupation dataset and compute the percentage difference.

**Expected**: Software-programmer males earn more than females (typical ~10-25% gap nationwide). Tool: `ato:get_data("ATO_OCCUPATION", filters={"occupation": "261313 Software engineer"})`.

## 6. Retirement-tech — super contributions slice

> For Australian females aged 30 to 39 earning between $120,001 and $180,000, what was the average employer super contribution per person in 2022-23? Pull the totals and the headcount and do the division.

**Expected**: One row per (age × sex × income bracket) — Claude divides total $ by count to derive the per-person average. Tool: `ato:get_data` on `SUPER_CONTRIB_AGE`.

## 6b. SMSF sector size

> How big is the Australian self-managed super fund sector right now? Show me total funds, members, and assets for the latest year, plus how the sector has grown since 2019-20.

**Expected**: 2024-25 latest: **653,062 SMSFs**, 1.2M members, **$1.05 trillion** in assets. Grown $29B since 2019-20. Tool: `ato:get_data` on `SMSF_FUNDS`.

---

## 7. HECS / HELP — student debt

> What's the total outstanding HECS/HELP debt in Australia, and how much has been repaid by graduates voluntarily vs compulsorily in the latest year?

**Expected**: 2024-25 — total HELP debt **$125.3B**, compulsory repayments **$52.1B**, voluntary repayments **$11.8B** (from 9.17M voluntary repayments). Tool: `ato:get_data` on `HELP_DEBT` filtered to 2024-25.

## 7b. HECS debt growth trajectory (uses `stats`)

> What's the range, mean, and median of total HELP debt over the last 20 years?

**Expected**: Min $17B (2005-06), max $125B (2024-25), mean ~$63B. Tool: `ato:stats("HELP_DEBT", "total_debt_aud")` — one call, eight numbers.

---

## 8. Tax-advisor / accountant — Small Business Benchmarks

> What's the typical total-expenses-to-income ratio for a medium-turnover bakery in Australia? My client's bakery is at 78% — is that within ATO's expected range?

**Expected**: Medium-turnover bakeries: 75–86% total expenses, 34–39% COGS. 78% is well inside the band. Tool: `ato:get_data` on `SBB_BENCHMARKS` filtered to "Bakeries and hot bread shops".

## 8b. Highest-margin industries

> Which small-business industries have the LOWEST total-expense ratios (so highest gross margins)? Rank them.

**Expected**: Professional services, architectural services, IT consulting typically lead. Tool: `ato:top_n("SBB_BENCHMARKS", "total_expenses_med_min", n=10, direction="bottom")`.

---

## 9. Public-policy / journalism — tax gaps

> How much tax does the ATO estimate is going uncollected in Australia each year? Break it down by tax type for 2022-23 and rank by dollar amount.

**Expected**: Personal income tax **$35.5B** (10.3% gap rate), Corporate income tax $10.8B, GST $8.1B, Excise $3.8B — total **~$58 billion / year** estimated missing. Tool: `ato:get_data` on `TAX_GAPS` filtered to 2022-23.

## 9b. Has the personal income tax gap been growing? (uses `stats group_by`)

> Trace the trend in the personal income tax gap rate over time. Has it gone up or down since 2017?

**Expected**: Rate climbed from 8.8% (2017-18) to 10.3% (2022-23). Tool: `ato:get_data("TAX_GAPS", filters={"tax_type": "personal"})` or `ato:stats("TAX_GAPS", "net_gap_rate", group_by="financial_year", filters={"tax_type": "personal"})`.

---

## 10. Aggregation queries (showcase `stats`)

> Across all NSW postcodes, what's the mean, median, min, and max of 2022-23 median taxable income?

**Expected**: 587 postcodes, mean $55k, median $53.5k, min $17.9k (student-heavy), max $92k (inner-west / eastern suburbs). Tool: `ato:stats("IND_POSTCODE_MEDIAN", "median_taxable_income_2022_23", filters={"state": "nsw"})`.

## 10b. Same query, but grouped by state (showcase `group_by`)

> Compare the distribution of postcode-level median taxable incomes across every state and territory. Which state has the highest mean? The lowest? How dispersed is each?

**Expected**: ACT highest mean ($72k, 24 postcodes), TAS lowest ($50k, 97 postcodes), NSW the largest sample (587). Tool: `ato:stats("IND_POSTCODE_MEDIAN", "median_taxable_income_2022_23", group_by="state")` — one call, all 8 states' distributions.

---

## Multi-server combos

Once you also have [abs-mcp](https://github.com/Bigred97/abs-mcp) and [rba-mcp](https://github.com/Bigred97/rba-mcp) installed, you can fan out across all three:

> For Sydney postcode 2000 in 2022-23: get the median taxable income (ato), the latest unemployment rate for Greater Sydney (abs), and the current RBA cash rate (rba). Summarise what these three numbers say about the financial profile of the area.

Claude disambiguates with `ato:`, `abs:`, `rba:` prefixes, so a single user message can produce three parallel tool calls.

---

## Troubleshooting

- **Tool not called / vague answer**: the MCP server isn't installed or not enabled. Check Claude Desktop's tool panel for `ato` next to `abs` and `rba`. If not present: verify your config file (see `examples/claude_desktop_config_all_three.json`), then **fully quit Claude Desktop (Cmd+Q)** and reopen — Claude Desktop writes its own state to the config periodically, so closing the window isn't enough.
- **"Could not fetch dataset … from data.gov.au"**: data.gov.au or your network had a hiccup. Retry; the cache is forgiving and warm hits don't go to the network.
- **Numbers look stale**: the dataset cache TTL is 7 days. Delete `~/.ato-mcp/cache.db` to force a refresh, or wait for the auto-update layer to detect a new yearly release via CKAN.
