# Contributing to ato-mcp

Thanks for considering a contribution. This is an indie open-source project — every PR is read.

## Quick start

```bash
git clone https://github.com/Bigred97/ato-mcp.git
cd ato-mcp
uv sync --extra dev
uv pip install -e .

# Unit tests (no network)
uv run pytest

# Live integration tests (hits data.gov.au)
uv run pytest -m live
```

## What kind of contribution helps?

| Most welcome | Be cautious |
|---|---|
| Bug fixes (with a regression test) | Adding new tools to the MCP surface |
| New curated datasets (one YAML per dataset in `src/ato_mcp/data/curated/`) | Refactors that touch >3 modules |
| Better error messages with actionable hints | Changes that break the public response shape |
| Docs / README improvements | Pulling in new dependencies |
| Performance fixes (with a benchmark) | Changes to the YAML schema |

## Adding a curated dataset

1. Find the dataset on [data.gov.au](https://data.gov.au/data/organization/australiantaxationoffice). Note the dataset slug (e.g. `taxation-statistics-2022-23`) and the specific resource (e.g. `Individuals - Table 6`).
2. Fetch the resource metadata via CKAN: `curl https://data.gov.au/data/api/3/action/package_show?id={slug} | jq` and find the resource's `url` field.
3. Download a copy and inspect headers:
   ```python
   import openpyxl
   wb = openpyxl.load_workbook("file.xlsx", read_only=True, data_only=True)
   for sn in wb.sheetnames:
       ws = wb[sn]
       for row in ws.iter_rows(max_row=5, values_only=True):
           print(row)
   ```
   Identify the data sheet, the 1-indexed header row, and the canonical column names. Note: ATO column headers often have embedded newlines (`Individuals\nno.`) — `parsing._normalize_header` strips padding around the newline so canonical YAML form is `"Individuals\nno."`.
4. Hand-write the YAML under `src/ato_mcp/data/curated/{ID}.yaml`. `CORP_TRANSPARENCY.yaml` is the simplest reference (header_row 1, flat tabular); `IND_POSTCODE.yaml` covers the multi-dimension case.
5. Run the smoke test to confirm column mappings match:
   ```bash
   PYTHONPATH=src uv run python -c "
   from pathlib import Path
   from ato_mcp import curated, parsing, shaping
   cd = curated.get('YOUR_ID')
   df = parsing.read_xlsx(Path('/path/to/file.xlsx').read_bytes(), sheet=cd.sheet, header_row=cd.header_row)
   missing = [c.source_column for c in cd.columns.values() if c.source_column not in df.columns]
   print('missing:' if missing else 'all columns match', missing)
   "
   ```
6. Add a test fixture in `tests/fixtures/` and write a test in `tests/test_shaping.py`.
7. Run `uv run pytest -m live` and confirm green.

## PR checklist

- [ ] All tests pass (`uv run pytest -m "not live"` minimum; `uv run pytest -m live` if you touched curation or the network path)
- [ ] New code has tests
- [ ] No new dependencies (or they're justified in the PR body)
- [ ] CHANGELOG.md updated under the Unreleased section
- [ ] If you changed default behaviour, the README "Example queries" still produces the documented values
- [ ] CC-BY 3.0 AU attribution still surfaces in `DataResponse.attribution`

## Style

- Python 3.11+, `from __future__ import annotations` at file top
- Pydantic v2 models — use `Field(default_factory=...)` for mutable defaults
- Docstrings in module-level summary; functions only when non-obvious
- No comments restating the code; comments explain *why*

## Filing bugs

Use the bug-report issue template. Bugs filed via the template get triaged within a week; freeform issues may sit longer.

## Discussions vs Issues

- **Issue**: bug, feature request, security report
- **Discussion**: question, idea you're not sure about, sharing how you're using the package

## Code of conduct

This project follows the [Contributor Covenant](CODE_OF_CONDUCT.md). Be kind.
