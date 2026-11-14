# Copyright (c) 2026 Kenneth Stott
# Canary: 7d2f4b19-83e6-4c0a-a5d1-6e9b0c3f2a74
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Measure the tool definitions Provisa's MCP server sends a client, and Polly's tool list.

A client is sent every tool's definition (name, description, input schema, and whatever else
``tools/list`` carries) on every model call. This builds the server in-process (no server, no
container), takes ``list_tools()`` and serializes each tool exactly as ``tools/list`` does, then
counts characters. Tokens are estimated as ``ceil(chars / 4)``.

    python scripts/mcp_tool_size.py           # the MCP server's tools
    python scripts/mcp_tool_size.py --polly   # Polly's tool list (chat.py), same rule
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
from types import SimpleNamespace
from typing import Any

CHARS_PER_TOKEN = 4


def estimate_tokens(chars: int) -> int:
    """The estimate rule: one token per four characters, rounded up."""
    return math.ceil(chars / CHARS_PER_TOKEN)


def _serialized(tool: Any) -> str:
    # tools/list sends ListToolsResult serialized by alias with None fields left out.
    return json.dumps(
        tool.model_dump(by_alias=True, mode="json", exclude_none=True), separators=(",", ":")
    )


async def mcp_tool_sizes() -> dict[str, int]:
    """Each MCP server tool's serialized size in characters, by name."""
    from provisa.api.mcp.server import build_mcp_server

    # Building the server reads nothing from the state: the tools bind it for their calls.
    mcp = build_mcp_server(SimpleNamespace())
    return {t.name: len(_serialized(t)) for t in await mcp.list_tools()}


def polly_tool_sizes() -> dict[str, int]:
    """Each of Polly's tools' size in characters, by name, as chat.py sends it to the model."""
    from provisa.api.mcp.chat import _TOOLS

    return {t["name"]: len(json.dumps(t, separators=(",", ":"))) for t in _TOOLS}


def report(title: str, sizes: dict[str, int]) -> str:
    total = sum(sizes.values())
    lines = [
        f"{title}: {len(sizes)} tools, {total:,} chars, ~{estimate_tokens(total):,} tokens "
        f"(chars / {CHARS_PER_TOKEN}, rounded up)",
    ]
    width = max(map(len, sizes))
    for name, size in sorted(sizes.items(), key=lambda kv: (-kv[1], kv[0])):
        lines.append(f"  {name:<{width}}  {size:>6,}  ~{estimate_tokens(size):>5,}")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Measure the MCP tool definitions a client is sent."
    )
    parser.add_argument("--polly", action="store_true", help="measure Polly's tool list instead")
    args = parser.parse_args()
    if args.polly:
        print(report("Polly (chat.py _TOOLS)", polly_tool_sizes()))
    else:
        print(report("MCP server tools/list", asyncio.run(mcp_tool_sizes())))


if __name__ == "__main__":
    main()
