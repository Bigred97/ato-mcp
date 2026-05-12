# Security Policy

## Supported versions

Only the latest minor release of `ato-mcp` is supported with security fixes. Older versions should be upgraded.

| Version | Supported |
|---|---|
| latest 0.1.x | ✅ |
| < 0.1.0 | ❌ |

## Reporting a vulnerability

**Do not file public GitHub issues for security vulnerabilities.**

Privately report via GitHub's [Security Advisories](https://github.com/Bigred97/ato-mcp/security/advisories/new) flow, or email `hvass97@gmail.com` with subject `[ato-mcp security]`.

Include:
- A clear description of the vulnerability
- A reproducer (minimal MCP call sequence or input that triggers it)
- The version of `ato-mcp` you tested against (`ato_mcp.__version__`)
- Your suggested fix, if you have one

You'll get an acknowledgement within 72 hours. Critical issues will be fixed and a patch release published within 7 days; lower-severity issues within 30 days. You'll be credited in the release notes unless you ask otherwise.

## Threat model

`ato-mcp` runs locally as an MCP stdio subprocess of your MCP client (Claude Desktop, Cursor, etc.). It:

- Reads no local files except its own SQLite cache at `~/.ato-mcp/cache.db`.
- Makes outbound HTTPS requests only to `https://data.gov.au/`.
- Has URL-injection guards on every user-supplied identifier (dataset IDs, period strings, package slugs).
- Does not execute arbitrary code from untrusted input.

The most realistic attack surfaces are:
- A malformed dataset ID escaping the regex guard and reaching the URL path (mitigated by `_DATASET_ID_PATTERN`).
- A crafted CKAN package_show response containing a malicious download URL (mitigated by `fetch_resource` rejecting any URL whose scheme isn't `http`/`https`, and by the curated YAML pinning the exact URL for each dataset — CKAN isn't trusted to supply URLs for curated datasets).
- A compromised `pip install` chain (mitigated by the standard PyPI signing and the MIT-licensed open-source repo).
- A crafted XLSX that exploits a vulnerability in openpyxl (mitigated by keeping openpyxl pinned to a recent version and by `read_xlsx` raising on parse failure rather than silently continuing).

If you find an attack vector outside this list, please report it.
