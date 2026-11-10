# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""``provisa query --changed --state FILE --dir DIR``: run the statement files that read a table
changed since the state file, or one that depends on it (REQ-2222).

The state file is a configuration, as ``provisa export`` writes one. It is compared with what the
instance holds for ``--org`` now: a table is changed when it was added, removed or edited. What
depends on a changed table is every view that reads it, directly or through another view, found
from each view's parsed SQL in either configuration, so a view that a change removed still counts.

Which tables a statement reads is the server's answer, from ``POST /data/explain``, so nothing here
parses a statement. A statement explain refuses is reported as refused and not run: a statement
reading a removed table is exactly what a branch needs to hear about.

A file's language is its extension. ``.grpc`` and ``.mcp`` files are a JSON object with
``statement`` and ``arguments``; ``--arguments`` applies to the other files.

The output is one JSON document and the exit codes are ``provisa query``'s: ``0`` when every
selected statement was answered, ``1`` when any was refused or a configuration cannot be used,
``2`` for a wrong command line or an unusable statement file, and ``4`` when any failed. A failure
outranks a refusal.
"""

# Requirements: REQ-2222

from __future__ import annotations

import argparse
import json
import urllib.error
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote

import yaml

from provisa.cli_config import _comparable, _read, _Refused, _report, _validated, _yaml_problem
from provisa.cli_query import (
    FAILED,
    OK,
    REFUSED,
    USAGE,
    _Answer,
    _answer_of,
    _api,
    _arguments,
    _emit,
    _explain_answer,
    _headers,
    _query_answer,
    _send,
    _UsageError,
)

_LANGUAGE_OF = {
    ".sql": "sql",
    ".graphql": "graphql",
    ".gql": "graphql",
    ".cypher": "cypher",
    ".cql": "cypher",
    ".sparql": "sparql",
    ".rq": "sparql",
    ".rest": "rest",
    ".grpc": "grpc",
    ".mcp": "mcp",
}
_JSON_FILES = ("grpc", "mcp")
_SECTION = "tables"


@dataclass(frozen=True)
class _Model:
    """One configuration, as far as selection needs it."""

    items: dict[str, dict[str, Any]]  # each table, comparable, by its identity
    tables: dict[str, Any]  # each table of the model, by the same identity
    metrics: dict[str, str]  # each metric's expression, by its name


@dataclass(frozen=True)
class _StatementFile:
    file: str  # relative to --dir
    language: str
    statement: str
    arguments: dict[str, Any]


def _address(table: Any) -> str:
    return f"{table.source_id}/{table.schema_name}/{table.table_name}"


def _model(raw: dict[str, Any], code: str) -> _Model:
    """``raw`` with its metric-composed views compiled to SQL, as a load compiles them."""
    from provisa.core.config_loader import _expand_view_metrics
    from provisa.core.store_config import item_identity

    config, _ = _validated(raw, code)
    try:
        _expand_view_metrics(config)
    except ValueError as exc:
        raise _Refused(code, str(exc)) from exc
    return _Model(
        _comparable(raw, code)[_SECTION],
        {
            item_identity(
                _SECTION, t.model_dump(mode="json", by_alias=True, exclude_defaults=True)
            ): t
            for t in config.tables
        },
        {m.name: m.expression for m in config.metrics},
    )


def _changed(then: _Model, now: _Model) -> set[str]:
    """The address of each table added, edited or removed between two configurations."""
    entry = _report({_SECTION: now.items}, {_SECTION: then.items})["sections"].get(_SECTION)
    if entry is None:
        return set()
    current = [*entry["add"], *(c["item"] for c in entry["change"])]
    return {_address(now.tables[i]) for i in current} | {
        _address(then.tables[i]) for i in entry["instance_only"]
    }


def _names(models: Iterable[_Model]) -> dict[str, set[str]]:
    """Each name a view's SQL may call a table by, lower-cased, with the tables it can mean. A bare
    name two tables share means both: selection may run too much, never too little."""
    from provisa.compiler.naming import apply_sql_name, domain_to_sql_name

    names: dict[str, set[str]] = {}
    for model in models:
        for table in model.tables.values():
            address = _address(table)
            domain = domain_to_sql_name(table.domain_id)
            said = {table.table_name, apply_sql_name(table.table_name)}
            if table.alias:
                said |= {table.alias, apply_sql_name(table.alias)}
            keys = {f"{table.schema_name}.{table.table_name}"}
            for name in said:
                keys |= {name, f"{domain}.{name}"}
            for key in keys:
                names.setdefault(key.lower(), set()).add(address)
    return names


def _metric_reads(
    metric: str, expressions: list[str], view: str, names: dict[str, set[str]], code: str
) -> set[str]:
    """The tables behind ``metrics.<metric>`` in a view's SQL, which is expanded only when the
    view is read, so the SQL itself never names them."""
    from provisa.compiler.metric_expand import metric_reference_tables

    reads: set[str] = set()
    for expression in expressions:
        try:
            referenced = metric_reference_tables(metric, expression)
        except ValueError as exc:
            raise _Refused(code, f"view {view}: {exc}") from exc
        for table in referenced:
            reads |= names.get(table.lower(), set())
    return reads


def _view_reads(
    sql: str,
    view: str,
    names: dict[str, set[str]],
    metrics: dict[str, list[str]],
    code: str,
) -> set[str]:
    """The tables a view's SQL names, from its parsed form. Resolved as a statement's references
    are (``naming.table_id_of``): by the qualified name, then the bare one."""
    import sqlglot
    import sqlglot.expressions as exp
    from sqlglot.errors import SqlglotError

    from provisa.compiler.metric_expand import METRICS_SCHEMA

    try:
        tree = sqlglot.parse_one(sql, read="postgres")
    except SqlglotError as exc:
        # The parser's message quotes the SQL, which is not this report's to repeat.
        raise _Refused(code, f"view {view} does not parse") from exc
    cte_names = {cte.alias.lower() for cte in tree.find_all(exp.CTE)}
    reads: set[str] = set()
    for table in tree.find_all(exp.Table):
        name = table.name.lower()
        if not table.db and name in cte_names:
            continue
        if table.db == METRICS_SCHEMA:
            reads |= _metric_reads(table.name, metrics.get(table.name, []), view, names, code)
            continue
        found = names.get(f"{table.db.lower()}.{name}") if table.db else None
        reads |= found or names.get(name, set())
    return reads


def _affected(changed: set[str], then: _Model, now: _Model, code: str) -> set[str]:
    """``changed`` and every view that reads one of them, however many views away."""
    names = _names((then, now))
    metrics: dict[str, list[str]] = {}
    for model in (then, now):
        for name, expression in model.metrics.items():
            metrics.setdefault(name, []).append(expression)
    readers: dict[str, set[str]] = {}
    for model in (then, now):
        for table in model.tables.values():
            if table.view_sql:
                view = _address(table)
                for read in _view_reads(table.view_sql, view, names, metrics, code):
                    readers.setdefault(read, set()).add(view)
    affected = set(changed)
    frontier = list(changed)
    while frontier:
        for view in readers.get(frontier.pop(), set()) - affected:
            affected.add(view)
            frontier.append(view)
    return affected


def _instance_config(args: argparse.Namespace) -> dict[str, Any] | _Answer:
    """What the instance holds for the org. The export is the org's, not the statements' role's,
    so the request names no role."""
    headers = {k: v for k, v in _headers(args).items() if k != "X-Provisa-Role"}
    url = f"{_api(args)}/admin/orgs/{quote(args.org, safe='')}/config-export"
    status, raw, _ = _send(url, "GET", None, headers, args.timeout)
    if status >= 400:
        return _answer_of(status, raw)
    try:
        return yaml.safe_load(raw) or {}
    except yaml.YAMLError as exc:
        raise _Refused("config.instance_invalid", _yaml_problem(exc)) from exc


def _read_statement(path: Path, root: Path, arguments: dict[str, Any]) -> _StatementFile:
    file = str(path.relative_to(root))
    language = _LANGUAGE_OF[path.suffix.lower()]
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise _UsageError(f"cannot read {file}: {exc}") from exc
    if language not in _JSON_FILES:
        return _StatementFile(file, language, text.strip(), arguments)
    try:
        document = json.loads(text)
        return _StatementFile(file, language, document["statement"], document.get("arguments", {}))
    except (json.JSONDecodeError, KeyError, TypeError) as exc:
        raise _UsageError(f"{file}: a {language} file is JSON with a statement") from exc


def _statement_files(args: argparse.Namespace) -> list[_StatementFile]:
    root = Path(args.dir)
    if not root.is_dir():
        raise _UsageError(f"--dir: {args.dir} is not a directory")
    arguments = _arguments(args)
    return [
        _read_statement(path, root, arguments)
        for path in sorted(root.rglob("*"))
        if path.is_file() and path.suffix.lower() in _LANGUAGE_OF
    ]


def _declined(answer: _Answer) -> dict[str, Any]:
    detail = answer.body.get("detail") if isinstance(answer.body, dict) else answer.body
    return {
        "code": answer.code,
        "detail": detail,
        "outcome": "failed" if answer.status >= 500 else "refused",
    }


def _run_file(args: argparse.Namespace, item: _StatementFile, affected: set[str]) -> dict[str, Any]:
    """One statement file's entry in the document: explained, then run if it is selected."""
    each = argparse.Namespace(**{**vars(args), "language": item.language, "analyze": False})
    entry: dict[str, Any] = {"file": item.file, "language": item.language}
    explained = _explain_answer(each, item.statement, item.arguments)
    if explained.code is not None:
        return {**entry, **_declined(explained)}
    reads = {t["address"] for s in explained.body["statements"] for t in s["tables"]}
    entry["tables"] = sorted(reads)
    if not reads & affected:
        return {**entry, "outcome": "skipped"}
    answer = _query_answer(each, item.statement, item.arguments)
    if answer.code is not None:
        return {**entry, **_declined(answer)}
    return {**entry, "outcome": "ok", "result": answer.body}


def _selection(then: _Model, held: dict[str, Any]) -> tuple[set[str], set[str]]:
    code = "config.instance_invalid"
    now = _model(held, code)
    changed = _changed(then, now)
    return changed, _affected(changed, then, now, code)


def usage_problem(args: argparse.Namespace) -> str | None:
    """What is wrong with how ``--changed`` and its flags were given, if anything."""
    if not args.changed:
        return "--state and --dir go with --changed" if args.state or args.dir else None
    if args.language or args.statement or args.file:
        return "--changed takes its statements from --dir, not a statement"
    if args.expect_refused:
        return "--expect-refused names one statement's refusal; --changed runs many"
    if not args.state or not args.dir or not args.org:
        return "--changed needs --state, --dir and --org"
    return None


def _worst(entries: list[dict[str, Any]]) -> tuple[str, int]:
    outcomes = {entry["outcome"] for entry in entries}
    if "failed" in outcomes:
        return "failed", FAILED
    return ("refused", REFUSED) if "refused" in outcomes else ("ok", OK)


def _refusal(refused: _Refused) -> int:
    _emit({"code": refused.code, "detail": refused.detail, "status": "refused"})
    return REFUSED


def run(args: argparse.Namespace) -> int:
    """``provisa query --changed``: the exit code, with the one document on stdout."""
    import sys

    try:
        if args.person:
            raise _UsageError(
                "--person: acting as a person is not available yet, no instance applies entitlements"
            )
        files = _statement_files(args)
        then = _model(_read(args.state), "config.invalid")
        held = _instance_config(args)
        if isinstance(held, _Answer):
            _emit({**_declined(held), "status": _declined(held)["outcome"]})
            return FAILED if held.status >= 500 else REFUSED
        changed, affected = _selection(then, held)
        entries = [_run_file(args, item, affected) for item in files]
    except _UsageError as exc:
        print(f"provisa query: {exc}", file=sys.stderr)
        return USAGE
    except _Refused as refused:
        return _refusal(refused)
    except (urllib.error.URLError, TimeoutError) as exc:
        reason = getattr(exc, "reason", exc)
        _emit({"code": "client.unreachable", "detail": str(reason), "status": "failed"})
        print(f"failed: client.unreachable: {reason}", file=sys.stderr)
        return FAILED
    status, exit_code = _worst(entries)
    _emit(
        {
            "affected": sorted(affected),
            "changed": sorted(changed),
            "statements": entries,
            "status": status,
        }
    )
    return exit_code
