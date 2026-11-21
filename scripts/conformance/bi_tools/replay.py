# Copyright (c) 2026 Kenneth Stott
# Canary: d4a07c19-6e3b-4f82-91c5-5b8e2f0a7d63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Replay the statements BI tools and psql send to browse a schema against PostgreSQL 17 and
Provisa, and compare the answers (REQ-2204).

Each tool's stored statements (bi_tools/<tool>.yaml, written by capture.py) run in one session per
server, in the order the tool first sent them, each with the bind parameters it was sent with. The
tokens a capture put in place of the fixture's OIDs, the database's name and the tool's user are
resolved on each server first, by name (an object a token names that a server does not have leaves
the statement unrun there). Each answer is compared as the pgwire catalog check compares a catalog
relation (pgwire_catalog_types.py, REQ-2167): the columns the server describes (name, order, type
OID and typmod), or, where PostgreSQL refuses the statement, whether Provisa refuses it with the
same SQLSTATE and message. Catalog content is not compared: it is the role's governed view of the
model, a deliberate difference stated once (REQ-2167, PGWIRE CATALOG AND TYPES), and the two
servers number their objects differently.

Each statement is one case: passed; skipped by design, when every difference is one a group of
by_design/pgwire.yaml names (its feature is ``<tool> catalog replay``, its scenario ``#<id> <sql>``);
failed; or not tested, when PostgreSQL itself does not have an object a token names (the stored
statements and the fixture have drifted apart). A tool with no stored statements is not tested.
The run fails on a failed case, on any tool.

Usage:
  replay.py replay --pg <dsn> --provisa <dsn> --out-json <f> --out-md <f>
  replay.py docs <report.json>          # the tools' section of docs/conformance/pgwire.md
"""

# Requirements: REQ-2204

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

import by_design  # noqa: E402
import pgwire_catalog_types as catalog  # noqa: E402

#: The tools, in the order the page lists them.
TOOLS = ("psql", "dbeaver", "metabase", "grafana")
TITLES = {"psql": "psql", "dbeaver": "DBeaver", "metabase": "Metabase", "grafana": "Grafana"}

DOCS = HERE.parents[2] / "docs" / "conformance" / "pgwire.md"
BEGIN = "<!-- bi-tools-results:begin -->"
END = "<!-- bi-tools-results:end -->"

TOKEN = re.compile(r"\{\{(oid:(pg_[a-z_]+):([^}]*)|database|user)\}\}")

#: How each token resolves on a server, by the name it carries ({schema}, {name}).
_BY_NAME = {
    "pg_class": "SELECT c.oid FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n "
    "ON n.oid = c.relnamespace WHERE n.nspname = {schema} AND c.relname = {name}",
    "pg_type": "SELECT t.oid FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n "
    "ON n.oid = t.typnamespace WHERE n.nspname = {schema} AND t.typname = {name}",
    "pg_constraint": "SELECT k.oid FROM pg_catalog.pg_constraint k JOIN pg_catalog.pg_namespace n "
    "ON n.oid = k.connamespace WHERE n.nspname = {schema} AND k.conname = {name}",
    "pg_namespace": "SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = {name}",
    "pg_database": "SELECT oid FROM pg_catalog.pg_database WHERE datname = current_database()",
    "pg_authid": "SELECT oid FROM pg_catalog.pg_roles WHERE rolname = current_user",
}


def _literal(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def resolver_sql(token: str) -> str:
    """The query answering ``token`` (its text between the braces) on a server."""
    if token == "database":
        return "SELECT current_database()"
    if token == "user":
        return "SELECT current_user"
    _, kind, name = token.split(":", 2)
    schema, _, rel = name.rpartition(".")
    return _BY_NAME[kind].format(schema=_literal(schema), name=_literal(rel or name))


def tokens_of(statement: dict[str, Any]) -> set[str]:
    """Every token a stored statement carries, in its SQL or its text parameters."""
    texts = [statement["sql"]] + [
        p["value"]
        for p in statement.get("params") or []
        if p.get("format", 0) == 0 and p["value"] is not None
    ]
    return {m.group(1) for text in texts for m in TOKEN.finditer(text)}


def resolve(server: catalog.Server, tokens: set[str]) -> dict[str, str | None]:
    """Each token's value on ``server``: None where the server has no such object."""
    values: dict[str, str | None] = {}
    for token in sorted(tokens):
        answer = server.run(resolver_sql(token))
        row = answer.get("row") if answer["ok"] else None
        values[token] = row[0].decode() if row and row[0] is not None else None
    return values


def bound(
    statement: dict[str, Any], values: dict[str, str | None]
) -> tuple[str, list[tuple[int, bytes | None, int]]]:
    """The statement's SQL and parameters with each token replaced by its value."""

    def fill(text: str) -> str:
        return TOKEN.sub(lambda m: values[m.group(1)] or "", text)

    params: list[tuple[int, bytes | None, int]] = []
    for p in statement.get("params") or []:
        fmt = p.get("format", 0)
        if p["value"] is None:
            params.append((p["type"], None, fmt))
        elif fmt == 1:
            params.append((p["type"], bytes.fromhex(p["value"]), 1))
        else:
            params.append((p["type"], fill(p["value"]).encode(), 0))
    return fill(statement["sql"]), params


def _recover(server: catalog.Server) -> None:
    """A statement that failed inside a transaction block aborts it: end it, as the tool would."""
    from psycopg import pq

    if server.conn.pgconn.transaction_status == pq.TransactionStatus.INERROR:
        server.run("ROLLBACK")


def run_statement(
    server: catalog.Server, statement: dict[str, Any], values: dict[str, str | None]
) -> dict[str, Any]:
    sql, params = bound(statement, values)
    answer = server.run(sql, params=params)
    _recover(server)
    return answer


def compare(want: dict[str, Any], got: dict[str, Any], names: dict[int, str]) -> list[str]:
    """The differences of Provisa's answer from PostgreSQL's, as the catalog check finds them:
    a refusal against PostgreSQL's (pgwire_catalog_types.same_refusal), else the described
    columns (pgwire_catalog_types._compare_columns). None is a match."""
    if not want["ok"]:
        if catalog.same_refusal(want, got):
            return []
        said = "answers" if got["ok"] else f"refuses: {got.get('primary') or got['error']}"
        return [f"PostgreSQL refuses: {want.get('primary') or want['error']}; Provisa {said}"]
    if not got["ok"]:
        return [f"Provisa error: {got['error'].splitlines()[0] if got['error'] else ''}"]
    return catalog._compare_columns(want["fields"], got["fields"], names)


def scenario(statement: dict[str, Any], limit: int = 160) -> str:
    """A statement's case name: its id and its SQL on one line."""
    sql = " ".join(statement["sql"].split())
    return f"#{statement['id']} " + (sql if len(sql) <= limit else sql[: limit - 3] + "...")


def replay_tool(
    doc: dict[str, Any], pg: catalog.Server, provisa: catalog.Server, names: dict[int, str]
) -> dict[str, Any]:
    """One tool's statements replayed on both servers: each case's outcome and the counts."""
    feature = f"{doc['tool']} catalog replay"
    statements = doc["statements"]
    tokens = set().union(*(tokens_of(s) for s in statements)) if statements else set()
    on_pg, on_provisa = resolve(pg, tokens), resolve(provisa, tokens)
    cases: list[dict[str, Any]] = []
    for statement in statements:
        needed = tokens_of(statement)
        case = {"id": statement["id"], "feature": feature, "scenario": scenario(statement)}
        missing_pg = sorted(t for t in needed if on_pg[t] is None)
        missing_provisa = sorted(t for t in needed if on_provisa[t] is None)
        if missing_pg:
            cases.append(
                {
                    **case,
                    "outcome": "not_tested",
                    "reason": "PostgreSQL has no " + ", ".join(missing_pg),
                }
            )
            continue
        want = run_statement(pg, statement, on_pg)
        if missing_provisa:
            cases.append(
                {
                    **case,
                    "outcome": "failed",
                    "reason": "names what Provisa's catalog does not have: "
                    + ", ".join(missing_provisa),
                }
            )
            continue
        got = run_statement(provisa, statement, on_provisa)
        diffs = compare(want, got, names)
        cases.append(
            {**case, "outcome": "failed" if diffs else "passed", "reason": "; ".join(diffs)}
        )
    return tool_outcomes(doc, cases)


def tool_outcomes(doc: dict[str, Any], cases: list[dict[str, Any]]) -> dict[str, Any]:
    """The tool's four counts: a failed case a by-design group of by_design/pgwire.yaml names is
    skipped by design (REQ-2167, amended 2026-10-28)."""
    unmet = [
        {"feature": c["feature"], "scenario": c["scenario"], "reason": c["reason"]}
        for c in cases
        if c["outcome"] == "failed"
    ]
    failures, excluded = by_design.classify(unmet, by_design.groups("pgwire"))
    skipped = {c["case"] for g in excluded for c in g["cases"]}
    for c in cases:
        if c["outcome"] == "failed" and f"{c['feature']}::{c['scenario']}" in skipped:
            c["outcome"] = "by_design"
    passed = sum(1 for c in cases if c["outcome"] == "passed")
    return {
        "tool": doc["tool"],
        "version": doc["version"],
        "captured": doc["captured"],
        "method": doc["method"],
        "browse": doc.get("browse") or [],
        "statements": len(cases),
        **by_design.outcomes(passed, excluded, failures),
        "not_tested": sum(1 for c in cases if c["outcome"] == "not_tested"),
        "cases": cases,
    }


def not_captured(tool: str) -> dict[str, Any]:
    """A tool with no stored statements: not tested."""
    return {
        "tool": tool,
        "version": None,
        "captured": None,
        "method": "not captured",
        "browse": [],
        "statements": 0,
        **by_design.outcomes(0, [], []),
        "not_tested": 0,
        "cases": [],
    }


def load(tool: str, directory: Path = HERE) -> dict[str, Any] | None:
    import yaml

    path = directory / f"{tool}.yaml"
    return yaml.safe_load(path.read_text()) if path.exists() else None


def replay(pg_dsn: str, provisa_dsn: str, directory: Path = HERE) -> dict[str, Any]:
    names = catalog._type_names(catalog.Server(pg_dsn))
    tools: dict[str, Any] = {}
    for tool in TOOLS:
        doc = load(tool, directory)
        if doc is None:
            tools[tool] = not_captured(tool)
            continue
        # One fresh session per tool and server, as the tool opens its own.
        tools[tool] = replay_tool(doc, catalog.Server(pg_dsn), catalog.Server(provisa_dsn), names)
    pg = catalog.Server(pg_dsn)
    return {
        "surface": "pgwire-bi-tools",
        "postgres_version": pg.rows("SHOW server_version")[0][0],
        "failed": sum(t["failed"] for t in tools.values()),
        "tools": tools,
    }


# --- report ----------------------------------------------------------------------------------


def _cell(text: Any) -> str:
    return " ".join(str(text).split()).replace("|", "\\|")


def _counts_row(t: dict[str, Any]) -> str:
    title = TITLES[t["tool"]]
    if t["version"] is None:
        return f"| {title} | — | — | not captured | 0 | 0 | 0 | 0 | not tested |"
    return (
        f"| {title} | {_cell(t['version'])} | {t['captured']} | {_cell(t['method'])} "
        f"| {t['statements']} | {t['passed']} | {t['by_design']} | {t['failed']} "
        f"| {t['not_tested']} |"
    )


def markdown(report: dict[str, Any], requirement: bool = False) -> str:
    """The per-tool results: the counts table, then each tool's failed, skipped-by-design and
    not-tested statements. ``requirement``: the job summary's form, naming each group's."""
    tools = [report["tools"][t] for t in TOOLS]
    out = [
        f"## BI tools' catalog statements (PostgreSQL {report['postgres_version']})",
        "",
        "Each statement a tool sent to browse the schema, replayed on PostgreSQL and on Provisa; "
        "the columns each answer describes (name, order, type, typmod) are compared, or the "
        "refusal where PostgreSQL refuses. Catalog content is not compared: it is the role's "
        "governed view of the model.",
        "",
        "| Tool | Version | Captured | Method | Statements | Passed | Skipped by design | Failed "
        "| Not tested |",
        "| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: |",
        *(_counts_row(t) for t in tools),
        "",
    ]
    for t in tools:
        title = TITLES[t["tool"]]
        failed = [c for c in t["cases"] if c["outcome"] == "failed"]
        untested = [c for c in t["cases"] if c["outcome"] == "not_tested"]
        if failed:
            out += [f"<details><summary>{title}: failed ({len(failed)})</summary>", ""]
            out += ["| Statement | Difference |", "| --- | --- |"]
            out += [f"| `{_cell(c['scenario'])}` | {_cell(c['reason'])} |" for c in failed]
            out += ["", "</details>", ""]
        if t["by_design_groups"]:
            out += [
                f"<details><summary>{title}: skipped by design ({t['by_design']})</summary>",
                "",
            ]
            out += by_design.table(t["by_design_groups"], requirement=requirement)
            out += ["", "</details>", ""]
        if untested:
            out += [f"<details><summary>{title}: not tested ({len(untested)})</summary>", ""]
            out += ["| Statement | Why |", "| --- | --- |"]
            out += [f"| `{_cell(c['scenario'])}` | {_cell(c['reason'])} |" for c in untested]
            out += ["", "</details>", ""]
    return "\n".join(out)


def fill(page: str, block: str) -> str:
    """``page`` with ``block`` between its bi-tools-results markers."""
    if BEGIN not in page or END not in page:
        raise ValueError("the page has no bi-tools-results markers to put the results between")
    start, end = page.index(BEGIN), page.index(END) + len(END)
    return page[:start] + f"{BEGIN}\n{block}\n{END}" + page[end:]


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    r = sub.add_parser("replay")
    r.add_argument("--pg", required=True)
    r.add_argument("--provisa", required=True)
    r.add_argument("--out-json", required=True, type=Path)
    r.add_argument("--out-md", required=True, type=Path)
    d = sub.add_parser("docs")
    d.add_argument("report", type=Path)
    d.add_argument("--page", type=Path, default=DOCS)
    args = parser.parse_args(argv)
    if args.command == "docs":
        report = json.loads(args.report.read_text())
        args.page.write_text(fill(args.page.read_text(), markdown(report)))
        print(f"{args.page}: BI tools' catalog statements")
        return 0
    report = replay(args.pg, args.provisa)
    args.out_json.write_text(json.dumps(report, indent=2, default=str) + "\n")
    args.out_md.write_text(markdown(report, requirement=True) + "\n")
    for t in report["tools"].values():
        print(
            f"{TITLES[t['tool']]}: {t['statements']} statements: {t['passed']} passed, "
            f"{t['by_design']} skipped by design, {t['failed']} failed, "
            f"{t['not_tested']} not tested" + ("" if t["version"] else " (not captured)")
        )
    if report["failed"]:
        print(
            f"::error::BI tools' catalog statements: {report['failed']} failed that no by-design "
            "group of scripts/conformance/by_design/pgwire.yaml excludes",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
