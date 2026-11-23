# Copyright (c) 2026 Kenneth Stott
# Canary: 3571b420-4466-44e3-96cc-2fdb8299a1ff
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A remote GraphQL endpoint as a built-in code source (REQ-2267 part 2, v0).

The code source's ``@discover`` (provisa/graphql_remote/graphql_source.py) generates its tables,
queries and commands from the remote's introspected schema, through the mapping the
``graphql_remote`` source registers with (:func:`provisa.graphql_remote.mapper.map_schema`), and
REQ-1986's grouping by row type:

- a root field that requires no argument (a plain list) is a table, its row type the columns the
  mapping gives it and its inputs the arguments it takes (each optional);
- a root field that requires an argument is a query on the plain list of its row type, where one
  exists; otherwise it is a table of its own, its required arguments required inputs;
- a mutation is a command; one that answers with a row of a generated table returns that table's
  rows.

A table the remote fetches by a list of keys (:func:`provisa.graphql_remote.by_keys.key_fetches`:
``where: {key: {_in: [...]}}``, or ``nodes(ids: [...])``) declares a loader (REQ-2100): it takes
the key column as an input, and a join's keys are fetched a batch at a time in one request.

Each item is read as the source reads it today: a table or query through
:func:`provisa.graphql_remote.executor.execute_remote` (a connection page by page), a command as
the mutation, its answer selected as its table's columns.

Names: a table, query, column and input is named as the ``graphql_remote`` registration names it
(its sql name), so the code source's catalog matches it; each input is mapped back to the remote
argument it is. A command keeps its mutation field's name, as source operations do (REQ-1924).
"""

# Requirements: REQ-2267, REQ-1986, REQ-1865, REQ-2100
from __future__ import annotations

import json
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from provisa.compiler.naming import apply_sql_name
from provisa.graphql_remote.by_keys import IDS, WHERE_IN, key_fetches
from provisa.graphql_remote.mapper import (
    _build_required_args,
    _find_type,
    _gql_type_string,
    _unwrap_type,
    map_schema,
)

#: The code source's entry module and the configuration written beside it.
ENTRY = "graphql_source"
CONFIG_FILE = "graphql_source.json"
_TEMPLATE = Path(__file__).with_name(f"{ENTRY}.py")

#: The fields the configuration must give; :func:`write_code` and :func:`load_config` refuse
#: one without any of them by name.
CONFIG_FIELDS = (
    "url",
    "auth",
    "namespace",
    "schema",
    "max_object_depth",
    "max_list_depth",
    "max_list_items",
    "max_rows",
    "loader_batch",
    "field_overrides",
)

#: The mapper's column type, as the Python type a generated row's field declares.
PYTHON_TYPE: dict[str, Any] = {
    "text": str,
    "integer": int,
    "numeric": float,
    "boolean": bool,
    "jsonb": Any,
}


class ConfigRefused(ValueError):
    """The code source's configuration lacks a field it must give."""


@dataclass(frozen=True)
class Column:
    """A generated row's field: its sql name, the mapper's type, its description and what it
    selects on the remote (``executor.column_selection``)."""

    name: str
    type: str
    description: str | None
    selection: str


@dataclass(frozen=True)
class Argument:
    """A remote argument as an input or parameter: its sql name, the remote's own, its GraphQL
    type (for a variable's declaration), the mapper's type and whether the remote requires it."""

    name: str
    remote: str
    gql_type: str
    type: str
    required: bool


@dataclass(frozen=True)
class Read:
    """How a root field is read: the table spec ``execute_remote`` takes (field, row path,
    pagination) and the arguments it may be given."""

    field_name: str
    rows_path: list[str] | None
    pagination: dict | None
    arguments: tuple[Argument, ...]

    def required_args(self) -> list[dict]:
        """Every argument as ``execute_remote`` declares the ones a read gives a value for."""
        return [{"name": a.remote, "gql_type": a.gql_type} for a in self.arguments]


@dataclass(frozen=True)
class Table:
    """A generated table: its name, row columns, how it is read, and its loader key (the column
    its rows are fetched by a list at a time, with how) or None."""

    name: str
    gql_type: str
    description: str | None
    columns: tuple[Column, ...]
    read: Read
    key: str | None = None
    key_fetch: dict | None = None


@dataclass(frozen=True)
class Query:
    """A generated query: a parameterised read of ``table``'s row type."""

    name: str
    table: str
    description: str | None
    read: Read


@dataclass(frozen=True)
class Command:
    """A generated command: a mutation, its arguments, and the table whose row it answers with
    (None: it answers with one value, of the mapper's type ``returns``)."""

    name: str
    description: str | None
    arguments: tuple[Argument, ...]
    table: str | None
    returns: str


@dataclass(frozen=True)
class Plan:
    """Everything the code source generates, by name, and how each is read."""

    config: dict
    tables: dict[str, Table] = field(default_factory=dict)
    queries: dict[str, Query] = field(default_factory=dict)
    commands: dict[str, Command] = field(default_factory=dict)
    relationships: tuple[dict, ...] = ()


# -- configuration --------------------------------------------------------------------------------


def _checked(config: dict) -> dict:
    missing = [f for f in CONFIG_FIELDS if f not in config]
    if missing:
        raise ConfigRefused(f"the GraphQL code source's configuration lacks {', '.join(missing)}")
    return config


def write_code(code_dir: Path, config: dict) -> Path:
    """The code source's code in ``code_dir``: its entry module, and ``config`` beside it (the
    remote's url, auth, namespace, introspected schema and mapping limits)."""
    _checked(config)
    code_dir.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(_TEMPLATE, code_dir / f"{ENTRY}.py")
    (code_dir / CONFIG_FILE).write_text(json.dumps(config))
    return code_dir


def load_config(code_dir: Path) -> dict:
    """The configuration :func:`write_code` wrote in ``code_dir``."""
    return _checked(json.loads((code_dir / CONFIG_FILE).read_text()))


# -- the plan -------------------------------------------------------------------------------------


def _arguments(table: dict) -> tuple[Argument, ...]:
    required = [(a, True) for a in table.get("required_args") or []]
    optional = [(a, False) for a in table.get("optional_args") or []]
    return tuple(
        Argument(apply_sql_name(a["name"]), a["name"], a["gql_type"], a["provisa_type"], req)
        for a, req in required + optional
    )


def _columns(table: dict) -> tuple[Column, ...]:
    from provisa.graphql_remote.executor import column_selection

    return tuple(
        Column(apply_sql_name(c["name"]), c["type"], c.get("description"), column_selection(c))
        for c in table["columns"]
    )


def _read(table: dict) -> Read:
    return Read(
        table["field_name"], table.get("rows_path"), table.get("pagination"), _arguments(table)
    )


def _loader_key(schema: dict, table: dict, inputs: set[str]) -> tuple[str | None, dict | None]:
    """The column ``table``'s rows are fetched by, a list at a time, and how: ``id`` where it is
    one, else the first such column. None when there is none, or its name is an input already."""
    fetches = key_fetches(schema, table)
    if not fetches:
        return None, None
    key = "id" if "id" in fetches else next(iter(fetches))
    if key in inputs:
        return None, None
    return key, fetches[key]


def _mutation_fields(schema: dict) -> list[dict]:
    root = (schema.get("mutationType") or {}).get("name")
    found = _find_type(schema.get("types") or [], root) if root else None
    return (found or {}).get("fields") or []


def _command(mutation: dict, by_type: dict[str, str]) -> Command:
    from provisa.graphql_remote.mapper import _build_args_list, _gql_to_provisa_type

    required = {a["name"] for a in _build_required_args(mutation)}
    declared = {a["name"]: a["type"] for a in mutation.get("args") or []}
    arguments = tuple(
        Argument(
            apply_sql_name(a["name"]),
            a["name"],
            _gql_type_string(declared[a["name"]]),
            a["type"],
            a["name"] in required,
        )
        for a in _build_args_list(mutation)
    )
    kind, returned = _unwrap_type(mutation["type"])
    table = by_type.get(returned) if kind == "OBJECT" else None
    return Command(
        mutation["name"],
        mutation.get("description"),
        arguments,
        table,
        _gql_to_provisa_type(mutation["type"]),
    )


def plan(config: dict) -> Plan:
    """What the code source generates from ``config``'s schema (see the module's docstring)."""
    _checked(config)
    schema = config["schema"]
    mapped, _, relationships = map_schema(
        schema,
        config["namespace"],
        "",
        "",
        max_object_depth=config["max_object_depth"],
        max_list_depth=config["max_list_depth"],
        max_list_items=config["max_list_items"],
        field_overrides=config["field_overrides"],
    )
    out = Plan(config, relationships=tuple(relationships))
    plain: dict[str, str] = {}  # each row type's first plain list
    for t in mapped:
        if not t.get("required_args"):
            plain.setdefault(t["gql_type_name"], t["sql_name"])
    for t in mapped:
        name = t["sql_name"]
        if t.get("required_args") and t["gql_type_name"] in plain:
            out.queries[name] = Query(
                name, plain[t["gql_type_name"]], t.get("description"), _read(t)
            )
            continue
        read = _read(t)
        key, fetch = (None, None)
        if not t.get("required_args"):
            key, fetch = _loader_key(schema, t, {a.name for a in read.arguments})
        out.tables[name] = Table(
            name, t["gql_type_name"], t.get("description"), _columns(t), read, key, fetch
        )
    by_type: dict[str, str] = {}
    for table in out.tables.values():
        by_type.setdefault(table.gql_type, table.name)
    for mutation in _mutation_fields(schema):
        if (config["field_overrides"] or {}).get(mutation["name"], "").lower() == "query":
            continue  # REQ-597: reclassified as a read; the mapping offers it as one
        out.commands[mutation["name"]] = _command(mutation, by_type)
    return out


# -- reads ----------------------------------------------------------------------------------------


def _variables(read: Read, given: dict[str, Any]) -> dict[str, Any]:
    """The remote variables for the inputs ``given`` (by sql name); an input not an argument of
    ``read`` (a loader's key) is not one."""
    remote = {a.name: a.remote for a in read.arguments}
    return {remote[k]: v for k, v in given.items() if k in remote and v is not None}


async def _read_rows(config: dict, read: Read, columns: tuple[Column, ...], given: dict) -> list:
    from provisa.graphql_remote.executor import execute_remote

    answer = await execute_remote(
        url=config["url"],
        auth=config["auth"],
        field_name=read.field_name,
        columns=[c.selection for c in columns],
        variables=_variables(read, given) or None,
        required_args=read.required_args() or None,
        limit=config["max_list_items"],
        pagination=read.pagination,
        rows_path=read.rows_path,
        max_rows=config["max_rows"],
    )
    return [r for r in answer.rows if isinstance(r, dict)]


async def _rows_by_keys(config: dict, table: Table, values: list) -> list[dict]:
    """The rows of ``table`` whose key is one of ``values``, in one request."""
    from provisa.graphql_remote.executor import execute_remote

    fetch = table.key_fetch
    assert fetch is not None and table.key is not None  # only a table with a loader key
    selections = [c.selection for c in table.columns]
    if fetch["style"] == WHERE_IN:
        variables = {fetch["arg"]: {fetch["key"]: {"_in": values}}}
    else:
        assert fetch["style"] == IDS
        variables = {fetch["arg"]: values}
        if fetch["on"] is not None:
            selections = [f"... on {fetch['on']} {{ {' '.join(selections)} }}"]
    answer = await execute_remote(
        url=config["url"],
        auth=config["auth"],
        field_name=fetch["field"],
        columns=selections,
        variables=variables,
        required_args=[{"name": fetch["arg"], "gql_type": fetch["arg_type"]}],
    )
    # A missing id answers null; an id of another type of the union, an object without the key.
    return [r for r in answer.rows if isinstance(r, dict) and table.key in r]


async def read_table(plan: Plan, name: str, inputs: dict, keys: dict | None) -> Any:
    """The table ``name``'s rows for ``inputs``; for a loader's batch (``keys``: ``{hash: key}``),
    each key's rows by its hash, from one request."""
    table = plan.tables[name]
    if keys is not None:
        wanted: dict[str, Any] = {}
        for hashed, key in keys.items():
            if key.get(table.key) is None:
                raise ValueError(f"{name}: a loader key gives no {table.key}: {key!r}")
            wanted[hashed] = key[table.key]
        rows = await _rows_by_keys(plan.config, table, list(dict.fromkeys(wanted.values())))
        by_value: dict[Any, list[dict]] = {}
        for row in rows:
            by_value.setdefault(row[table.key], []).append(row)
        return {hashed: by_value.get(value, []) for hashed, value in wanted.items()}
    if table.key is not None and inputs.get(table.key) is not None:
        return await _rows_by_keys(plan.config, table, [inputs[table.key]])
    return await _read_rows(plan.config, table.read, table.columns, inputs)


async def run_query(plan: Plan, name: str, arguments: dict) -> list[dict]:
    """The query ``name``'s rows: its root field read with ``arguments``, selected as the
    columns of the table whose rows it returns."""
    query = plan.queries[name]
    return await _read_rows(plan.config, query.read, plan.tables[query.table].columns, arguments)


def _selection(schema: dict, type_ref: dict) -> str:
    from provisa.executor.source_operation import _ANSWER_DEPTH, _selection as answer_selection

    return answer_selection(schema, type_ref, _ANSWER_DEPTH)


def command_document(plan: Plan, name: str, given: list[str]) -> str:
    """The mutation ``name`` called with the arguments ``given`` (remote names), each a variable
    of its declared type; its answer selected as its table's columns, else as a source
    operation's answer is (REQ-1924)."""
    command = plan.commands[name]
    declared = {a.remote: a.gql_type for a in command.arguments}
    passed = [a for a in given if a in declared]
    head = f"({', '.join(f'${a}: {declared[a]}' for a in passed)})" if passed else ""
    call = f"({', '.join(f'{a}: ${a}' for a in passed)})" if passed else ""
    if command.table is not None:
        answer = "{ " + " ".join(c.selection for c in plan.tables[command.table].columns) + " }"
    else:
        mutation = next(f for f in _mutation_fields(plan.config["schema"]) if f["name"] == name)
        answer = _selection(plan.config["schema"], mutation["type"])
    return f"mutation{head} {{ {name}{call}{f' {answer}' if answer else ''} }}"


async def run_command(plan: Plan, name: str, arguments: dict) -> Any:
    """The mutation ``name`` called with ``arguments``: its table's rows, else its answer."""
    import httpx

    from provisa.graphql_remote.introspect import _build_headers
    from provisa.graphql_remote.executor import _safe_json

    command = plan.commands[name]
    remote = {a.name: a.remote for a in command.arguments}
    variables = {remote[k]: v for k, v in arguments.items() if v is not None}
    document = command_document(plan, name, list(variables))
    headers = {"Content-Type": "application/json", **_build_headers(plan.config["auth"])}
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            plan.config["url"], json={"query": document, "variables": variables}, headers=headers
        )
    resp.raise_for_status()
    body = _safe_json(resp)
    if body.get("errors"):
        raise ValueError(f"{name}: the remote refused the mutation: {body['errors']}")
    answer = (body.get("data") or {}).get(name)
    if command.table is None:
        return answer
    rows = answer if isinstance(answer, list) else [answer]
    return [r for r in rows if isinstance(r, dict)]
