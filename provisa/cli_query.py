# Copyright (c) 2026 Kenneth Stott
# Canary: fe803fd5-12d7-4627-9f66-0bd70abb9b63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""``provisa query`` and ``provisa explain``: a statement in any language, against a running
instance (REQ-2222).

``query`` goes through the same front door a client uses, so what it answers is what a client
gets. ``explain`` calls ``POST /data/explain``. Nothing is decided here: the role's rights, the
SQL and the refusal are the server's. There is no interactive mode: pgwire and Bolt give psql and
cypher-shell.

THE CONTRACT WITH A SCRIPT is one JSON document on stdout, keys sorted, and the exit code:

* ``0`` -- answered, or refused with the code ``--expect-refused`` names.
* ``1`` -- refused: the instance declined the statement (HTTP 4xx). Its code is in the document
  and on stderr.
* ``2`` -- the command line is wrong.
* ``3`` -- ``--expect-refused`` was not met: the statement was answered, or refused under another
  code.
* ``4`` -- failed: the instance could not be reached, or answered 5xx.

``--expect-refused`` takes the code, never bare: a statement with a typo in it is declined too,
and a bare assertion would pass on it.
"""

# Requirements: REQ-2222

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

OK = 0
REFUSED = 1
USAGE = 2
EXPECTATION_FAILED = 3
FAILED = 4

LANGUAGES = ("sql", "graphql", "cypher", "sparql", "grpc", "rest", "mcp")
_MCP_PROTOCOL = "2025-03-26"


class _UsageError(Exception):
    pass


@dataclass(frozen=True)
class _Answer:
    """A front door's answer, as a script reads it."""

    status: int
    body: Any
    code: str | None = None  # set when the door declined or failed


def _headers(args: argparse.Namespace) -> dict[str, str]:
    token = args.token or os.environ.get("PROVISA_API_TOKEN", "")
    headers = {"X-Provisa-Role": args.role}
    if args.org:
        headers["X-Org-Provisa"] = args.org
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _send(
    url: str, method: str, data: bytes | None, headers: dict[str, str], timeout: int
) -> tuple[int, bytes, dict[str, str]]:
    req = urllib.request.Request(url, method=method, data=data, headers=headers)
    try:
        # B310 wants the scheme pinned: the URL is the operator's own --api argument.
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # nosec B310
            return resp.status, resp.read(), dict(resp.headers.items())
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read(), dict(exc.headers.items())


def _decoded(raw: bytes) -> Any:
    text = raw.decode(errors="replace")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {"detail": text}


def _answer_of(status: int, raw: bytes) -> _Answer:
    body = _decoded(raw)
    if status < 400:
        errors = body.get("errors") if isinstance(body, dict) else None
        if not errors:
            return _Answer(status, body)
        # GraphQL over HTTP answers a request it declined as 200 with `errors`.
        first = errors[0]
        code = (first.get("extensions") or {}).get("code") or "graphql.error"
        return _Answer(400, {"detail": first.get("message"), "errors": errors}, code)
    code = body.get("code") if isinstance(body, dict) else None
    return _Answer(status, body, code if isinstance(code, str) else f"http.{status}")


def _api(args: argparse.Namespace) -> str:
    return (args.api or os.environ.get("PROVISA_API_URL", "http://127.0.0.1:8000")).rstrip("/")


def _json_call(
    args: argparse.Namespace, method: str, path: str, body: dict[str, Any] | None
) -> _Answer:
    headers = _headers(args)
    data = None
    if body is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(body).encode()
    status, raw, _ = _send(f"{_api(args)}{path}", method, data, headers, args.timeout)
    return _answer_of(status, raw)


def _sparql_call(args: argparse.Namespace, statement: str) -> _Answer:
    headers = {
        **_headers(args),
        "Content-Type": "application/sparql-query",
        "Accept": "application/sparql-results+json",
    }
    status, raw, _ = _send(
        f"{_api(args)}/data/sparql", "POST", statement.encode(), headers, args.timeout
    )
    return _answer_of(status, raw)


def _rest_path(statement: str) -> str:
    path = "/" + statement.lstrip("/")
    return path if path.startswith("/data/rest/") else f"/data/rest{path}"


def _mcp_payload(raw: bytes) -> dict[str, Any]:
    """The JSON-RPC message of an MCP answer: the body, or the last event of an event stream."""
    text = raw.decode(errors="replace")
    events = [ln[5:].strip() for ln in text.splitlines() if ln.startswith("data:")]
    return json.loads(events[-1] if events else text)


def _mcp_call(args: argparse.Namespace, tool: str, arguments: dict[str, Any]) -> _Answer:
    """One tool call over Streamable HTTP: initialize, the initialized notice, then the call."""
    url = args.mcp_url or os.environ.get("PROVISA_MCP_URL")
    if not url:
        raise _UsageError("mcp needs --mcp-url or PROVISA_MCP_URL")
    headers = {
        **_headers(args),
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
    }

    def post(message: dict[str, Any]) -> tuple[int, bytes, dict[str, str]]:
        data = json.dumps({"jsonrpc": "2.0", **message}).encode()
        return _send(url, "POST", data, headers, args.timeout)

    status, raw, sent = post(
        {
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": _MCP_PROTOCOL,
                "capabilities": {},
                "clientInfo": {"name": "provisa-cli", "version": "1"},
            },
        }
    )
    if status >= 400:
        return _answer_of(status, raw)
    session = next((v for k, v in sent.items() if k.lower() == "mcp-session-id"), None)
    if session:
        headers["Mcp-Session-Id"] = session
    post({"method": "notifications/initialized"})
    status, raw, _ = post(
        {
            "id": 2,
            "method": "tools/call",
            # A tool names its role in its arguments (provisa/api/mcp/server.py).
            "params": {"name": tool, "arguments": {"role": args.role, **arguments}},
        }
    )
    if status >= 400:
        return _answer_of(status, raw)
    message = _mcp_payload(raw)
    if "error" in message:
        error = message["error"]
        return _Answer(400, error, f"mcp.{error.get('code')}")
    result = message["result"]
    if result.get("isError"):
        text = " ".join(c.get("text", "") for c in result.get("content", []))
        return _Answer(403, {"detail": text}, "mcp.tool_refused")
    return _Answer(status, result.get("structuredContent", result))


def _query_answer(args: argparse.Namespace, statement: str, arguments: dict[str, Any]) -> _Answer:
    language = args.language
    if language == "sql":
        return _json_call(args, "POST", "/data/sql", {"sql": statement})
    if language == "graphql":
        return _json_call(
            args, "POST", "/data/graphql", {"query": statement, "variables": arguments}
        )
    if language == "cypher":
        return _json_call(args, "POST", "/data/cypher", {"query": statement, "params": arguments})
    if language == "sparql":
        return _sparql_call(args, statement)
    if language == "grpc":
        return _json_call(args, "POST", f"/data/grpc/{quote(statement, safe='')}", arguments)
    if language == "rest":
        return _json_call(args, "GET", _rest_path(statement), None)
    return _mcp_call(args, statement, arguments)


def _explain_answer(args: argparse.Namespace, statement: str, arguments: dict[str, Any]) -> _Answer:
    return _json_call(
        args,
        "POST",
        "/data/explain",
        {
            "language": args.language,
            "statement": statement,
            "arguments": arguments,
            "analyze": args.analyze,
        },
    )


def _statement(args: argparse.Namespace) -> str:
    if args.file:
        with open(args.file, encoding="utf-8") as handle:
            return handle.read()
    if args.statement == "-":
        return sys.stdin.read()
    if args.statement is None:
        raise _UsageError("give the statement, - for stdin, or --file")
    return args.statement


def _arguments(args: argparse.Namespace) -> dict[str, Any]:
    if args.arguments is None:
        return {}
    try:
        parsed = json.loads(args.arguments)
    except json.JSONDecodeError as exc:
        raise _UsageError(f"--arguments is not JSON: {exc}") from exc
    if not isinstance(parsed, dict):
        raise _UsageError("--arguments is a JSON object")
    return parsed


def _emit(document: dict[str, Any]) -> None:
    print(json.dumps(document, sort_keys=True, default=str))


def _report(answer: _Answer, expected: str | None) -> int:
    """The document and the exit code for ``answer``, given the refusal expected of it."""
    if answer.code is None:
        _emit({"result": answer.body, "status": "ok"})
        if expected is None:
            return OK
        print(f"expected refusal {expected}, but the statement was answered", file=sys.stderr)
        return EXPECTATION_FAILED
    detail = answer.body.get("detail") if isinstance(answer.body, dict) else answer.body
    failed = answer.status >= 500
    _emit(
        {
            "code": answer.code,
            "detail": detail,
            "http_status": answer.status,
            "status": "failed" if failed else "refused",
        }
    )
    print(f"{'failed' if failed else 'refused'}: {answer.code}: {detail}", file=sys.stderr)
    if failed:
        return FAILED
    if expected is None:
        return REFUSED
    if answer.code == expected:
        return OK
    print(f"expected refusal {expected}, refused under {answer.code}", file=sys.stderr)
    return EXPECTATION_FAILED


def _run(args: argparse.Namespace, call: Any) -> int:
    try:
        if args.person:
            # REQ-2219: no instance applies a person's entitlements yet, and a run that named one
            # and got the role's answer would be read as that person's.
            raise _UsageError(
                "--person: acting as a person is not available yet, no instance applies entitlements"
            )
        answer = call(args, _statement(args), _arguments(args))
    except _UsageError as exc:
        print(f"provisa {args.command}: {exc}", file=sys.stderr)
        return USAGE
    except (urllib.error.URLError, TimeoutError) as exc:
        reason = getattr(exc, "reason", exc)
        _emit({"code": "client.unreachable", "detail": str(reason), "status": "failed"})
        print(f"failed: client.unreachable: {reason}", file=sys.stderr)
        return FAILED
    return _report(answer, args.expect_refused)


def _cmd_query(args: argparse.Namespace) -> int:
    from provisa import cli_changed

    problem = cli_changed.usage_problem(args)
    if problem is not None:
        print(f"provisa query: {problem}", file=sys.stderr)
        return USAGE
    if args.changed:
        return cli_changed.run(args)
    if args.language is None:
        # No statement and no --changed: there is no interactive mode to fall into.
        print("provisa query: give the language and the statement, or --changed", file=sys.stderr)
        raise SystemExit(USAGE)
    return _run(args, _query_answer)


def _cmd_explain(args: argparse.Namespace) -> int:
    return _run(args, _explain_answer)


def _add_common(parser: argparse.ArgumentParser, *, language_optional: bool = False) -> None:
    parser.add_argument("language", choices=LANGUAGES, nargs="?" if language_optional else None)
    parser.add_argument(
        "statement",
        nargs="?",
        default=None,
        help="The text (sql, graphql, cypher, sparql), the type name (grpc), the path and query "
        "string (rest) or the tool (mcp); - reads stdin",
    )
    parser.add_argument("--file", default=None, help="Read the statement from this file")
    parser.add_argument(
        "--arguments",
        default=None,
        metavar="JSON",
        help="GraphQL variables, Cypher parameters, the gRPC request or the MCP tool arguments",
    )
    parser.add_argument("--role", required=True, help="The role the statement runs as")
    parser.add_argument("--org", default=None, help="The organization (X-Org-Provisa)")
    parser.add_argument("--person", default=None, help="The person, once entitlements apply")
    parser.add_argument(
        "--expect-refused",
        default=None,
        metavar="CODE",
        help="Exit 0 only if the statement is refused with this code",
    )
    parser.add_argument("--api", default=None, help="API URL (default: $PROVISA_API_URL)")
    parser.add_argument("--token", default=None, help="Bearer token (default: $PROVISA_API_TOKEN)")
    parser.add_argument("--mcp-url", default=None, help="MCP URL (default: $PROVISA_MCP_URL)")
    parser.add_argument(
        "--timeout", type=int, default=300, help="Request timeout in seconds (default: 300)"
    )


def register(sub: Any) -> None:
    """Add ``query`` and ``explain`` to the CLI's subcommands."""
    query = sub.add_parser("query", help="Run a statement in any language, as a client would")
    _add_common(query, language_optional=True)
    query.add_argument(
        "--changed",
        action="store_true",
        help="Run the statement files of --dir that read a table changed since --state, or a "
        "view that depends on one (needs --org)",
    )
    query.add_argument(
        "--state", default=None, help="The earlier configuration, as provisa export wrote it"
    )
    query.add_argument(
        "--dir", default=None, help="The statement files; the extension names the language"
    )
    query.set_defaults(func=_cmd_query)
    explain = sub.add_parser("explain", help="Show the SQL a statement in any language becomes")
    _add_common(explain)
    explain.add_argument(
        "--analyze", action="store_true", help="Run the statement and report real timings"
    )
    explain.set_defaults(func=_cmd_explain)
