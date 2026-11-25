# Copyright (c) 2026 Kenneth Stott
# Canary: 3b7e1d24-9c5a-4f08-a6e2-8d41f0c7b593
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An OpenAPI source as a built-in Python code source (REQ-2267 part 2, v0).

The code is Provisa's own: :func:`write_code` writes a directory holding the source's spec, its
address, auth and headers -- its credential as the declared secret it is read from at each call,
never its value (provisa/codesource/credentials.py) -- and an entry module whose ``@discover`` function wraps the
existing mapping (:func:`provisa.openapi.mapper.parse_spec`) and its row-type grouping (REQ-1986):

- every GET that answers rows and requires no parameter (a plain list) is a generated table, its
  row type the response's columns and its inputs the operation's optional parameters;
- a GET that requires a parameter and answers the rows of a type a plain list answers is a
  generated query on that plain list's table (the first plain list of the type, as
  ``_function_table.plain_list_returned`` picks it); one whose type no plain list answers, or whose
  spec names no type, is a generated table of its own, its required parameters required inputs;
- a GET that looks up a list of ids (its one required parameter an array of the row's key, named
  as the key's plural: ``ids`` for ``id``, ``petIds`` for ``petId``) is also its plain list's
  table's loader (REQ-2100): a join's keys are sent to it in one call, in batches of the
  parameter's ``maxItems`` where the spec declares one;
- every other operation is a generated command, called as ``source_operation.call_operation``
  calls it (its path parameters, then ``body``).

Each read is today's: the table's endpoint is built as ``register_openapi_endpoint`` builds it for
a table registered with the paging the spec suggests (``openapi_operation``, ``endpoint_columns``,
``default_params_from_spec``), its inputs are sent as the parameters a statement binds are, and its
rows are those ``call_api`` and ``answer_rows`` give; an answer cut at its ``max_pages`` carries
the warning ``handle_api_query`` gives it (provisa_services.warn).

The code runs under Provisa's own Python (:func:`host_command`, :func:`read_code`): it imports
Provisa, which no organisation's code environment holds.
"""

# Requirements: REQ-2267, REQ-1986, REQ-2100
from __future__ import annotations

import hashlib
import json
import keyword
import re
import sys
import typing
from dataclasses import dataclass, field
from functools import cached_property
from pathlib import Path
from types import SimpleNamespace
from typing import Any

if typing.TYPE_CHECKING:
    from provisa.api_source.models import ApiEndpoint, ApiSource
    from provisa.codesource.manifest import Manifest
    from provisa.codesource.worker import HostCommand
    from provisa.openapi.mapper import OpenAPIMutation, OpenAPIQuery

#: The entry module :func:`write_code` writes, and the files it reads.
ENTRY = "provisa_openapi_source"
SPEC_FILE = "spec.json"
SOURCE_FILE = "source.json"

_ENTRY_TEXT = '''\
"""An OpenAPI source as a code source, written by Provisa (provisa/openapi/code_source.py)."""

from pathlib import Path

from provisa_services import Discovered

from provisa.openapi.code_source import OpenAPICode

_code = OpenAPICode.load(Path(__file__).parent)


def discover() -> Discovered:
    """@discover"""
    return _code.discover(serve_table, serve_query, serve_command)


async def serve_table(name, inputs, context):
    return await _code.serve_table(name, inputs, context)


async def serve_query(name, arguments):
    return await _code.serve_query(name, arguments)


async def serve_command(name, arguments):
    return await _code.serve_command(name, arguments)
'''

# An OpenAPI column's type (openapi.register) as the Python type of a field.
_PYTHON_TYPE: dict[str, Any] = {
    "string": str,
    "integer": int,
    "number": float,
    "boolean": bool,
    "jsonb": Any,
}

#: A generated field: its name (the column's or parameter's), Python type, whether it is
#: required, and its description.
FieldSpec = tuple[str, Any, bool, "str | None"]

#: The plural of a key the spec looks a list of up by: ``ids`` / ``petIds`` / ``pet_ids``.
_PLURAL_KEY = re.compile(r"^(?P<key>(?:.*[_a-z])?[iI]d)s$")


@dataclass(frozen=True)
class Lookup:  # REQ-2100
    """A GET that looks up a list of ids: the table's loader. ``parameter`` is the array query
    parameter the ids are sent in, ``key`` the row's field (and the table's input) each id is,
    ``batch`` the most ids one call takes (the parameter's ``maxItems``; None when unbounded)."""

    operation: OpenAPIQuery
    parameter: str
    key: str
    batch: int | None


@dataclass(frozen=True)
class GeneratedTable:
    """A generated table: the operation it is read from, and its loader where it has one."""

    operation: OpenAPIQuery
    lookup: Lookup | None = None


@dataclass(frozen=True)
class GeneratedQuery:
    """A generated query: the parameterised operation, and the table whose rows it returns."""

    operation: OpenAPIQuery
    table: str


@dataclass(frozen=True)
class Plan:
    """What the spec generates, by name."""

    tables: dict[str, GeneratedTable] = field(default_factory=dict)
    queries: dict[str, GeneratedQuery] = field(default_factory=dict)
    commands: dict[str, OpenAPIMutation] = field(default_factory=dict)


def _parameters(q: OpenAPIQuery) -> list[dict]:
    return [*q.path_params, *q.query_params]


def _requires(q: OpenAPIQuery) -> bool:
    return any(p["required"] for p in _parameters(q))


def _columns(q: OpenAPIQuery) -> list[dict]:
    from provisa.openapi.register import _schema_to_columns

    return _schema_to_columns(q.response_schema)


def _array_items(spec: dict, q: OpenAPIQuery, name: str) -> int | None:
    """The ``maxItems`` the spec declares for ``q``'s query parameter ``name``; None for none."""
    from provisa.openapi.mapper import operation_parameters

    for p in operation_parameters(spec, q.path, q.method.lower()):
        if p.get("in") == "query" and p.get("name") == name:
            schema = p.get("schema") or p  # Swagger 2.0 declares it on the parameter itself
            found = schema.get("maxItems")
            return int(found) if found is not None else None
    raise LookupError(f"operation {q.operation_id!r} has no query parameter {name!r}")


def _lookup(spec: dict, q: OpenAPIQuery, table: OpenAPIQuery) -> Lookup | None:
    """``q`` as ``table``'s loader, when it looks up a list of ids (see the module); else None."""
    if not q.is_list or q.path_params:
        return None
    required = [p for p in q.query_params if p["required"]]
    if len(required) != 1 or required[0]["type"] != "array":
        return None
    parameter = required[0]["name"]
    plural = _PLURAL_KEY.match(parameter)
    if plural is None:
        return None
    key = plural.group("key")
    if key not in {c["name"] for c in _columns(table)}:
        return None
    if key in {p["name"] for p in _parameters(table)}:
        return None  # the plain list takes an input of the key's name: it is not the key
    return Lookup(q, parameter, key, _array_items(spec, q, parameter))


def plan_spec(spec: dict) -> Plan:
    """What ``spec`` generates (the module's rules)."""
    from provisa.openapi.mapper import parse_spec

    queries, mutations = parse_spec(spec)
    plan = Plan()
    # REQ-1924: a GET whose response declares no row schema has no row type; it is generated as
    # the command that reads (parse_spec offers it as both).
    reads = [q for q in queries if not q.columns_declared]
    plain: dict[str, OpenAPIQuery] = {}
    for q in reads:
        if _requires(q):
            continue
        plan.tables[q.operation_id] = GeneratedTable(q)
        if q.returns_type is not None:
            plain.setdefault(q.returns_type, q)
    for q in reads:
        if not _requires(q):
            continue
        listed = plain.get(q.returns_type) if q.returns_type is not None else None
        if listed is None:
            plan.tables[q.operation_id] = GeneratedTable(q)
            continue
        plan.queries[q.operation_id] = GeneratedQuery(q, listed.operation_id)
        table = plan.tables[listed.operation_id]
        lookup = None if table.lookup is not None else _lookup(spec, q, listed)
        if lookup is not None:
            plan.tables[listed.operation_id] = GeneratedTable(listed, lookup)
    for m in mutations:
        plan.commands[m.operation_id] = m
    return plan


# -- the generated types ----------------------------------------------------------------------------


def _attribute(name: str, taken: set[str]) -> str:
    """A Python attribute name for the field ``name`` (its alias), unique among ``taken``."""
    base = re.sub(r"\W", "_", name) or "field"
    if base[0].isdigit() or keyword.iskeyword(base) or base.startswith("_"):
        base = f"f_{base}"
    attribute, n = base, 1
    while attribute in taken:
        n += 1
        attribute = f"{base}_{n}"
    taken.add(attribute)
    return attribute


def _model(name: str, fields: typing.Sequence[FieldSpec]) -> type:
    """A Pydantic model named ``name`` whose fields, by alias, are ``(alias, type, required,
    description)``: the reader names each column or parameter by its alias (python/reader.py)."""
    from pydantic import ConfigDict, Field, create_model

    taken: set[str] = set()
    defined: dict[str, Any] = {}
    for alias, kind, required, description in fields:
        hint = kind if required or kind is Any else kind | None
        default = ... if required else None
        defined[_attribute(alias, taken)] = (
            hint,
            Field(default, alias=alias, description=description),
        )
    return create_model(  # pyright: ignore[reportCallIssue]
        name, __config__=ConfigDict(populate_by_name=True), **defined
    )


def _type_name(text: str, suffix: str) -> str:
    return re.sub(r"\W", "_", text) + suffix


def _row_type(table: str, q: OpenAPIQuery) -> type:
    return _model(
        _type_name(table, "Row"),
        [(c["name"], _PYTHON_TYPE[c["type"]], False, c.get("description")) for c in _columns(q)],
    )


def _parameter_type(
    name: str, q: OpenAPIQuery, suffix: str, extra: typing.Sequence[FieldSpec] = ()
) -> type | None:
    from provisa.openapi.register import _openapi_to_provisa_type

    fields: list[FieldSpec] = [
        (p["name"], _PYTHON_TYPE[_openapi_to_provisa_type(p.get("type"))], p["required"], None)
        for p in _parameters(q)
    ]
    fields.extend(extra)
    return _model(_type_name(name, suffix), fields) if fields else None


# -- the code ---------------------------------------------------------------------------------------


@dataclass
class OpenAPICode:
    """One OpenAPI source's code: its spec, and the address, auth and headers it is called with.
    The auth's credential names the source's declared secret (provisa/codesource/credentials.py),
    read from Provisa's secrets service at each call."""

    source_id: str
    spec: dict
    base_url: str
    auth: dict | None = None
    headers: dict[str, str] = field(default_factory=dict)

    @classmethod
    def load(cls, directory: Path) -> OpenAPICode:
        source = json.loads((directory / SOURCE_FILE).read_text())
        spec = json.loads((directory / SPEC_FILE).read_text())
        return cls(
            source_id=source["source_id"],
            spec=spec,
            base_url=source["base_url"],
            auth=source["auth"],
            headers=source["headers"],
        )

    @cached_property
    def plan(self) -> Plan:
        return plan_spec(self.spec)

    def source(self) -> ApiSource:
        """The source a call is made through, its credential read now (REQ-2267): a rotated
        secret is the next call's."""
        from provisa.api_source.models import ApiSource, ApiSourceType
        from provisa.api_source.openapi_endpoint import api_auth
        from provisa.codesource.credentials import resolved

        return ApiSource(
            id=self.source_id,
            type=ApiSourceType.openapi,
            base_url=self.base_url,
            auth=api_auth(resolved(self.auth)),  # pyright: ignore[reportArgumentType]
            headers=self.headers,
        )

    # -- discovery ----------------------------------------------------------------------------------

    def discover(self, table_handler, query_handler, command_handler) -> Any:  # a Discovered
        from provisa_services import (  # pyright: ignore[reportMissingImports]
            Discovered,
            DiscoveredCommand,
            DiscoveredQuery,
            DiscoveredTable,
        )

        from provisa.executor.source_operation import BODY_ARGUMENT

        plan = self.plan
        tables = []
        for name, table in plan.tables.items():
            q = table.operation
            lookup = table.lookup
            key: tuple[FieldSpec, ...] = ()
            if lookup is not None:
                kind = next(c["type"] for c in _columns(q) if c["name"] == lookup.key)
                key = ((lookup.key, _PYTHON_TYPE[kind], False, None),)
            tables.append(
                DiscoveredTable(
                    name,
                    row=_row_type(name, q),
                    inputs=_parameter_type(name, q, "Inputs", key),
                    description=q.summary or "",
                    loader=lookup is not None,
                    loader_batch=None if lookup is None else lookup.batch,
                )
            )
        queries = [
            DiscoveredQuery(
                name,
                table=query.table,
                parameters=_parameter_type(name, query.operation, "Parameters"),
                description=query.operation.summary or "",
            )
            for name, query in plan.queries.items()
        ]
        commands = []
        for name, m in plan.commands.items():
            fields: list[FieldSpec] = [(p, str, True, None) for p in _path_parameters(m.path)]
            if m.input_schema is not None:
                fields.append((BODY_ARGUMENT, Any, True, None))
            commands.append(
                DiscoveredCommand(
                    name,
                    parameters=_model(_type_name(name, "Parameters"), fields) if fields else None,
                    returns=Any,
                    description=m.summary or "",
                )
            )
        return Discovered(
            tables=tables,
            queries=queries,
            commands=commands,
            table_handler=table_handler if tables else None,
            query_handler=query_handler if queries else None,
            command_handler=command_handler if commands else None,
        )

    # -- reads --------------------------------------------------------------------------------------

    def endpoint(self, table: str, q: OpenAPIQuery) -> ApiEndpoint:
        """The endpoint ``q`` is read through, as ``register_openapi_endpoint`` writes it for the
        table ``table`` registered with the paging the spec suggests."""
        from provisa.api_source.models import ApiEndpoint
        from provisa.api_source.openapi_endpoint import (
            default_params_from_spec,
            endpoint_columns,
            openapi_operation,
        )

        match = openapi_operation(self.spec, self.source_id, q.operation_id, q.pagination)
        return ApiEndpoint(
            source_id=self.source_id,
            path=match.path if match.server is None else match.server.rstrip("/") + match.path,
            method="GET",
            table_name=table,
            columns=endpoint_columns(match),  # pyright: ignore[reportArgumentType]
            default_params=default_params_from_spec(self.spec, match.path),
            pagination=q.pagination,
            response_root=match.rows_field,
        )

    async def _rows(self, endpoint: ApiEndpoint, given: dict) -> list[dict]:
        import provisa_services  # pyright: ignore[reportMissingImports] -- on the host's path

        from provisa.api_source.caller import answer_cut_warning, answer_rows, call_api

        # As handle_api_query: the endpoint's defaults under what the read binds.
        params = {**endpoint.default_params, **given}
        source = self.source()
        answer = await call_api(
            endpoint,
            params,
            base_url=source.base_url,
            auth=source.auth,
            source_headers=source.headers,
        )
        rows, cut = answer_rows(endpoint, answer)
        if cut is not None:
            # A cut answer (max_pages reached) is the rows read, with the warning
            # handle_api_query gives it (REQ-1350), carried by the call's answer (REQ-2267).
            warning = answer_cut_warning(endpoint.table_name, cut)
            provisa_services.warn(warning.code, warning.message, **warning.params)
        return rows

    async def serve_table(self, name: str, inputs: dict, context: Any) -> Any:
        table = self.plan.tables[name]
        if context.keys is not None:
            return await self._load(name, table, context.keys)
        lookup = table.lookup
        if lookup is not None and lookup.key in inputs:
            # The key given as an input reads the one row through the lookup, its only input.
            others = sorted(set(inputs) - {lookup.key})
            if others:
                raise ValueError(
                    f"table {name}: its key {lookup.key} is read by {lookup.operation.operation_id}"
                    f", which takes none of {', '.join(others)}"
                )
            found = await self._looked_up(name, lookup, [inputs[lookup.key]])
            return found.get(inputs[lookup.key], [])
        return await self._rows(self.endpoint(name, table.operation), inputs)

    async def _looked_up(self, name: str, lookup: Lookup, ids: list) -> dict[Any, list[dict]]:
        """The rows ``lookup`` answers for ``ids``, by the id each carries."""
        endpoint = self.endpoint(name, lookup.operation)
        by_id: dict[Any, list[dict]] = {}
        for row in await self._rows(endpoint, {lookup.parameter: ids}):
            by_id.setdefault(row.get(lookup.key), []).append(row)
        return by_id

    async def _load(self, name: str, table: GeneratedTable, keys: dict[str, dict]) -> dict:
        """REQ-2100: a loader call. Every key's id sent to the table's lookup in one call (the
        host gives at most its batch), each key answered with the rows carrying its id."""
        lookup = table.lookup
        if lookup is None:
            raise ValueError(f"table {name} has no loader")
        for key in keys.values():
            others = sorted(set(key) - {lookup.key})
            if others or lookup.key not in key:
                raise ValueError(
                    f"table {name}: a join's key gives {', '.join(sorted(key)) or 'nothing'}, and "
                    f"its loader {lookup.operation.operation_id} looks up {lookup.key} alone"
                )
        ids = list(dict.fromkeys(key[lookup.key] for key in keys.values()))
        found = await self._looked_up(name, lookup, ids)
        return {hashed: found.get(key[lookup.key], []) for hashed, key in keys.items()}

    async def serve_query(self, name: str, arguments: dict) -> list[dict]:
        query = self.plan.queries[name]
        return await self._rows(self.endpoint(query.table, query.operation), arguments)

    async def serve_command(self, name: str, arguments: dict) -> Any:
        from provisa.executor.source_operation import call_operation

        # The command's call as source_operation makes it, from what a process holding the source
        # holds for it: its spec and its source.
        state = SimpleNamespace(
            source_types={self.source_id: "openapi"},
            openapi_specs={self.source_id: {"spec": self.spec}},
            api_sources={self.source_id: self.source()},
        )
        return await call_operation(state, self.source_id, name, arguments)


def _path_parameters(path: str) -> list[str]:
    return re.findall(r"\{([^}]+)\}", path)


# -- the code's directory, and how Provisa runs it --------------------------------------------------


def _credential_field(auth: dict | None) -> str | None:
    """The field of ``auth`` (as registered, api_auth's form) its credential is in; None when it
    has none."""
    from provisa.api_source.openapi_endpoint import AUTH_SECRET

    if not auth or auth.get("type", "none") == "none":
        return None
    return AUTH_SECRET[auth["type"]]


def declared_secrets(source_id: str, auth: dict | None) -> dict[str, str]:
    """REQ-2267: the ``secrets`` the source's mapping declares for its credential, the reference
    as registered; refused by name when the credential is not a reference."""
    from provisa.codesource.credentials import held_as_reference

    return held_as_reference(auth, _credential_field(auth), f"source {source_id}")[1]


def write_code(
    directory: Path,
    *,
    source_id: str,
    spec: dict,
    base_url: str,
    auth: dict | None = None,
    headers: dict[str, str] | None = None,
) -> str:
    """Write the source's code into ``directory``; its content hash. Its credential is written as
    the declared secret it is read from (:func:`declared_secrets`), never as a value or a
    reference."""
    from provisa.codesource.credentials import held_as_reference

    held, _ = held_as_reference(auth, _credential_field(auth), f"source {source_id}")
    directory.mkdir(parents=True, exist_ok=True)
    source = {
        "source_id": source_id,
        "base_url": base_url,
        "auth": held,
        "headers": dict(headers or {}),
    }
    files = {
        f"{ENTRY}.py": _ENTRY_TEXT,
        SPEC_FILE: json.dumps(spec),  # its own order: the order of its columns
        SOURCE_FILE: json.dumps(source, sort_keys=True),
    }
    digest = hashlib.sha256()
    for name, text in sorted(files.items()):
        (directory / name).write_text(text)
        digest.update(name.encode() + b"\0" + text.encode() + b"\0")
    return digest.hexdigest()


#: The environment the reader and the host get beside PATH (as provisa/python/source.py's).
_PYTHON_ENV = {"PYTHONDONTWRITEBYTECODE": "1", "PYTHONUNBUFFERED": "1"}


def read_code(directory: Path) -> Manifest:
    """The code's tables, queries and commands, read by Provisa's Python reader under Provisa's
    own Python; refused with every problem by name."""
    from provisa.codesource.manifest import PYTHON_RULES, read_manifest
    from provisa.codesource.worker import private_environment
    from provisa.python.source import READER, run_reader

    argv = [sys.executable, "-I", str(READER), str(directory), ENTRY]
    return read_manifest(run_reader(argv, private_environment(**_PYTHON_ENV)), PYTHON_RULES)


def host_command(directory: Path, manifest: Manifest) -> HostCommand:
    """How Provisa's own Python runs the host on the code."""
    from provisa.codesource.manifest import discovered_handlers
    from provisa.codesource.worker import HostCommand, private_environment
    from provisa.python.source import HOST

    return HostCommand(
        argv=lambda address: [sys.executable, "-I", str(HOST), address, str(directory), ENTRY],
        env=private_environment(**_PYTHON_ENV),
        cwd=directory,
        discovered=discovered_handlers(manifest),
    )
