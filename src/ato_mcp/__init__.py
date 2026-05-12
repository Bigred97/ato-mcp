"""ato-mcp — MCP server for Australian Taxation Office statistics."""
from __future__ import annotations

try:
    from importlib.metadata import version as _v
    __version__ = _v("ato-mcp")
except Exception:
    __version__ = "0.0.0+unknown"
