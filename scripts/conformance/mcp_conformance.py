# Copyright (c) 2026 Kenneth Stott
# Canary: 2b8d6f43-1e7a-4c9b-8d2e-4f6a1c3b9e07
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Provisa's MCP server against the MCP conformance suite and the MCP specification (REQ-2167).

Two parts, one report:

- the public suite: @modelcontextprotocol/conformance (npm, MIT), installed at a pinned version,
  never vendored, its server scenarios run by the workflow against Provisa's Streamable HTTP
  endpoint with ``--output-dir``. Each scenario of its active suite is a case: passed when it wrote
  at least one check and none is FAILURE (the suite's own count: SUCCESS and FAILURE decide,
  WARNING and INFO do not); failed otherwise, and failed when it wrote no result at all.
- the model's tools, which the suite's scenarios do not reach (they call the suite's reference
  server's own fixtures): cases derived from the MCP specification's lifecycle, tools, ping and
  authorization sections, run here through the MCP Python SDK.

Every case is passed, skipped by design, a documented limit or failed (REQ-2167, amended
2026-11-23); the run is
judged by baseline_gate.py against scripts/conformance/expected/mcp.json.

Usage: mcp_conformance.py <mcp url> <suite results dir> <suite version> <out.json> <out.md>
"""

# Requirements: REQ-2167

from __future__ import annotations

import asyncio
import json
import re
import sys
from collections.abc import Awaitable, Callable
from datetime import timedelta
from pathlib import Path
from typing import Any

import httpx
from jsonschema import Draft202012Validator
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client
from mcp.shared.exceptions import McpError
from mcp.types import LATEST_PROTOCOL_VERSION

from surface_cases import Case, CaseFailed, expect, report, run, write

SUITE = "@modelcontextprotocol/conformance server scenarios and MCP specification scenarios"
SUITE_FEATURE = "MCP conformance suite"
#: The suite's active server scenarios at its pinned version (0.1.16): every server scenario but
#: the pending json-schema-2020-12 and server-sse-polling. A version bump re-reads this list from
#: the suite (`conformance list --server`) and the total in expected/mcp.json with it.
ACTIVE = (
    "server-initialize",
    "logging-set-level",
    "ping",
    "completion-complete",
    "tools-list",
    "tools-call-simple-text",
    "tools-call-image",
    "tools-call-audio",
    "tools-call-embedded-resource",
    "tools-call-mixed-content",
    "tools-call-with-logging",
    "tools-call-error",
    "tools-call-with-progress",
    "tools-call-sampling",
    "tools-call-elicitation",
    "elicitation-sep1034-defaults",
    "server-sse-multiple-streams",
    "elicitation-sep1330-enums",
    "resources-list",
    "resources-read-text",
    "resources-read-binary",
    "resources-templates-read",
    "resources-subscribe",
    "resources-unsubscribe",
    "prompts-list",
    "prompts-get-simple",
    "prompts-get-with-args",
    "prompts-get-embedded-resource",
    "prompts-get-with-image",
    "dns-rebinding-protection",
)
#: The suite's result directory: <scenario>-<ISO time with : and . as ->, under server-.
RESULT_DIR = re.compile(r"^server-(?P<scenario>.+)-\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}-\d{3}Z$")
TIMEOUT = timedelta(seconds=120)


def suite_results(results: Path) -> dict[str, list[dict]]:
    """Each scenario's checks, as the suite wrote them (the latest run of a scenario)."""
    found: dict[str, tuple[str, list[dict]]] = {}
    for checks in sorted(results.glob("*/checks.json")):
        named = RESULT_DIR.match(checks.parent.name)
        if named is None:
            continue
        scenario = named.group("scenario")
        found[scenario] = (checks.parent.name, json.loads(checks.read_text()))
    return {scenario: entry[1] for scenario, entry in found.items()}


def suite_check(checks: list[dict] | None) -> Callable[[], None]:
    """A scenario's case: what its checks say."""

    def check() -> None:
        expect(checks is not None, "the suite wrote no result for the scenario")
        assert checks is not None
        expect(bool(checks), "the scenario wrote no check")
        failed = [c for c in checks if c.get("status") == "FAILURE"]
        expect(
            not failed,
            "; ".join(
                f"{c.get('id')}: {c.get('errorMessage') or c.get('description')}" for c in failed
            ),
        )

    return check


def suite_cases(results: Path) -> list[Case]:
    got = suite_results(results)
    return [Case(SUITE_FEATURE, scenario, suite_check(got.get(scenario))) for scenario in ACTIVE]


Step = Callable[[ClientSession, Any], Awaitable[None]]


class Mcp:
    """Provisa's MCP endpoint, reached over the loopback as its local principal (REQ-1105)."""

    def __init__(self, url: str) -> None:
        self.url = url

    def session(self, step: Step) -> Callable[[], None]:
        """A case whose check opens a session, initializes it, and runs ``step``."""

        async def go() -> None:
            seconds = TIMEOUT.total_seconds()
            async with httpx.AsyncClient(timeout=seconds) as client:
                async with streamable_http_client(self.url, http_client=client) as (read, send, _):
                    async with ClientSession(read, send, read_timeout_seconds=TIMEOUT) as session:
                        initialized = await session.initialize()
                        await step(session, initialized)

        return lambda: asyncio.run(go())

    @staticmethod
    async def initialize(_: ClientSession, initialized: Any) -> None:
        version = initialized.protocolVersion
        expect(bool(version), "initialize named no protocol version")
        expect(
            version <= LATEST_PROTOCOL_VERSION,
            f"protocol version {version} is newer than the client's {LATEST_PROTOCOL_VERSION}",
        )
        expect(bool(initialized.serverInfo.name), "serverInfo has no name")
        expect(initialized.capabilities.tools is not None, "no tools capability declared")

    @staticmethod
    async def ping(session: ClientSession, _: Any) -> None:
        await session.send_ping()

    @staticmethod
    async def tools_listed(session: ClientSession, _: Any) -> None:
        tools = (await session.list_tools()).tools
        expect(bool(tools), "tools/list answered no tool")
        bad = [t.name for t in tools if not t.name or t.inputSchema.get("type") != "object"]
        expect(not bad, f"tools without a name or an object inputSchema: {bad}")

    @staticmethod
    async def schemas_valid(session: ClientSession, _: Any) -> None:
        bad = []
        for tool in (await session.list_tools()).tools:
            try:
                Draft202012Validator.check_schema(tool.inputSchema)
            except Exception as exc:  # noqa: BLE001 - the schema error is the finding
                bad.append(f"{tool.name}: {str(exc).splitlines()[0]}")
        expect(not bad, f"inputSchemas that are not JSON Schema: {bad[:5]}")

    @staticmethod
    async def call_tool(session: ClientSession, _: Any) -> None:
        result = await session.call_tool("describe_role", {})
        text = " ".join(getattr(c, "text", "") for c in result.content)
        expect(not result.isError, f"describe_role answered an error: {text[:300]}")
        expect(bool(result.content), "describe_role answered no content")

    @staticmethod
    async def unknown_tool(session: ClientSession, _: Any) -> None:
        try:
            result = await session.call_tool("no_such_tool_x", {})
        except McpError:
            return
        raise CaseFailed(
            "an unknown tool was answered with a tool result"
            f" (isError={result.isError}), not a JSON-RPC error"
        )

    @staticmethod
    async def invalid_arguments(session: ClientSession, _: Any) -> None:
        try:
            result = await session.call_tool("describe_table", {"schema": 7})
        except McpError:
            return
        expect(result.isError, "arguments outside the inputSchema were answered as a success")

    def invalid_bearer_401(self) -> None:
        body = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": LATEST_PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {"name": "provisa-conformance", "version": "1"},
            },
        }
        got = httpx.post(
            self.url,
            json=body,
            headers={
                "authorization": "Bearer not-a-token",
                "accept": "application/json, text/event-stream",
            },
            timeout=TIMEOUT.total_seconds(),
        )
        expect(got.status_code == 401, f"an invalid bearer answered {got.status_code}")
        expect("www-authenticate" in got.headers, "a 401 without a WWW-Authenticate header")


def model_cases(m: Mcp) -> list[Case]:
    lc, tl, ut, au = "Lifecycle", "Tools", "Utilities", "Authorization"
    return [
        Case(
            lc,
            "initialize names a version, serverInfo and the tools capability",
            m.session(m.initialize),
        ),
        Case(ut, "ping is answered with an empty result", m.session(m.ping)),
        Case(
            tl,
            "tools/list gives each tool a name and an object inputSchema",
            m.session(m.tools_listed),
        ),
        Case(tl, "every tool's inputSchema is a valid JSON Schema", m.session(m.schemas_valid)),
        Case(
            tl, "tools/call of a model tool answers content, not an error", m.session(m.call_tool)
        ),
        Case(tl, "tools/call of an unknown tool is a JSON-RPC error", m.session(m.unknown_tool)),
        Case(
            tl,
            "tools/call with arguments outside the inputSchema is an error",
            m.session(m.invalid_arguments),
        ),
        Case(au, "an invalid bearer token is answered 401 with a challenge", m.invalid_bearer_401),
    ]


def main(argv: list[str]) -> int:
    url, results, version, out_json, out_md = argv
    outcomes = run([*suite_cases(Path(results)), *model_cases(Mcp(url))])
    doc = report("mcp", SUITE, version, outcomes)
    return write(doc, "MCP", Path(out_json), Path(out_md))


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
