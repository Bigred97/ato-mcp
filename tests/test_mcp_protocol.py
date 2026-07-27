"""Real MCP-transport protocol tests via `fastmcp.Client`.

Unlike the rest of the suite (which calls the async tool functions directly
with native Python objects), these tests round-trip arguments through
FastMCP's actual JSON-RPC argument validation — the same Pydantic gate a
real MCP client (Claude, another agent, curl against a hosted transport)
triggers. This is the boundary that a direct-call unit test cannot exercise:
MCP clients commonly send structured parameters like `filters` as a
JSON-encoded STRING (not a native dict), and FastMCP validates the incoming
argument against the tool's declared parameter type *before* the function
body ever runs.

Regression test for: `filters: Annotated[dict[str, Any] | None, ...]` on
`get_data`/`latest`/`top_n`/`stats` rejected a JSON-string `filters` argument
with a raw Pydantic `dict_type` error at the transport boundary, even though
`_validate_filters` in `server.py` already handles strings correctly — that
helper was dead code for any client sending `filters` as JSON text, because
the string never survived long enough to reach it. The fix widens the
parameter type to `dict[str, Any] | str | None` so the string reaches the
function body, where `_validate_filters` does the real parsing.
"""
from __future__ import annotations

import pytest
from fastmcp import Client
from fastmcp.exceptions import ToolError

from ato_mcp.server import mcp


@pytest.mark.asyncio
async def test_get_data_accepts_json_string_filters_over_mcp_transport():
    """A JSON-encoded string `filters` argument must clear the MCP transport
    boundary and reach `server.py`'s own validation/business logic — not be
    rejected by Pydantic's dict-only schema before the function body runs.

    Uses an unknown dataset_id so the call fails deterministically (no
    network / fixture data needed) via `_get_data_impl`'s own
    "not a curated ato-mcp dataset" ValueError. Pre-fix, the string `filters`
    argument never gets that far: FastMCP's strict `dict[str, Any] | None`
    schema rejects it first with a `dict_type` validation error, and the
    resulting ToolError never mentions "not a curated" at all.
    """
    async with Client(mcp) as client:
        with pytest.raises(ToolError) as excinfo:
            await client.call_tool(
                "get_data",
                {"dataset_id": "DOES_NOT_EXIST", "filters": '{"state": "nsw"}'},
            )
    message = str(excinfo.value)
    assert "not a curated" in message, (
        "Expected the call to clear MCP transport validation and fail on "
        "server.py's own dataset-id check, not a Pydantic schema rejection "
        f"of the string filters argument. Got: {message!r}"
    )
    assert "dict_type" not in message
    assert "Input should be a valid dictionary" not in message


@pytest.mark.asyncio
async def test_latest_accepts_json_string_filters_over_mcp_transport():
    async with Client(mcp) as client:
        with pytest.raises(ToolError) as excinfo:
            await client.call_tool(
                "latest",
                {"dataset_id": "DOES_NOT_EXIST", "filters": '{"postcode": "2000"}'},
            )
    message = str(excinfo.value)
    assert "not a curated" in message
    assert "dict_type" not in message


@pytest.mark.asyncio
async def test_top_n_accepts_json_string_filters_over_mcp_transport():
    async with Client(mcp) as client:
        with pytest.raises(ToolError) as excinfo:
            await client.call_tool(
                "top_n",
                {
                    "dataset_id": "DOES_NOT_EXIST",
                    "measure": "tax_payable",
                    "filters": '{"state": "nsw"}',
                },
            )
    message = str(excinfo.value)
    assert "not a curated" in message
    assert "dict_type" not in message


@pytest.mark.asyncio
async def test_stats_accepts_json_string_filters_over_mcp_transport():
    async with Client(mcp) as client:
        with pytest.raises(ToolError) as excinfo:
            await client.call_tool(
                "stats",
                {
                    "dataset_id": "DOES_NOT_EXIST",
                    "measure": "tax_payable",
                    "filters": '{"state": "nsw"}',
                },
            )
    message = str(excinfo.value)
    assert "not a curated" in message
    assert "dict_type" not in message


@pytest.mark.asyncio
async def test_get_data_malformed_json_string_filters_gives_clean_hint():
    """A malformed JSON string should surface `_validate_filters`'s own
    "invalid JSON string" hint over MCP transport, not a schema error —
    proving the string reaches the helper rather than being rejected upstream.
    """
    async with Client(mcp) as client:
        with pytest.raises(ToolError) as excinfo:
            await client.call_tool(
                "get_data",
                {"dataset_id": "CORP_TRANSPARENCY", "filters": "{not valid json"},
            )
    message = str(excinfo.value)
    assert "invalid JSON string" in message
