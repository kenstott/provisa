# Copyright (c) 2026 Kenneth Stott
# Canary: 2f9c61d8-3a47-4e1b-b7d0-91ce4a6f3e25
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2226: a code source's @command takes a governed table as an argument, and a function tagged
@query is identical to one tagged @command except that its kind is query, not mutation. It declares
its input and output columns, so it is a lineage node, and a mutation is never composed in a view."""

# Requirements: REQ-2226

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.codesource import commands
from provisa.codesource.manifest import (
    GO_RULES,
    NODE_RULES,
    PYTHON_RULES,
    ManifestRefused,
    Named,
    TableOf,
    manifest_from_json,
    manifest_json,
    read_manifest,
)
from provisa.codesource.mutations import bindings
from provisa.executor.function_dispatch import dispatch_function
from provisa.executor.source_operation import refuse_writes_in_definition
from provisa.lineage.graph import build_column_graph

ALL_RULES = [NODE_RULES, PYTHON_RULES, GO_RULES]
ORDER = {"kind": "ref", "name": "Order"}
TYPES = {
    "Order": {
        "name": "Order",
        "fields": [
            {
                "name": "id",
                "optional": False,
                "type": {"kind": "number", "integer": True},
                "description": "",
            },
            {"name": "region", "optional": False, "type": {"kind": "string"}, "description": ""},
        ],
    },
    "Score": {
        "name": "Score",
        "fields": [
            {"name": "region", "optional": False, "type": {"kind": "string"}, "description": ""},
            {"name": "score", "optional": False, "type": {"kind": "number"}, "description": ""},
        ],
    },
}
SCORES = {"kind": "array", "element": {"kind": "ref", "name": "Score"}}


def _parameter(name: str, kind: dict, optional: bool = False) -> dict:
    return {"name": name, "optional": optional, "rest": False, "type": kind}


def _table(row: dict | None = None) -> dict:
    return {"kind": "table_ref", "row": row or ORDER}


def _raw(
    tag: str, parameters: list[dict], returns: dict | None = None, name: str = "score"
) -> dict:
    return {
        "exports": [
            {
                "name": name,
                "tags": [tag],
                "tag_arguments": [],
                "description": "",
                "signatures": [{"parameters": parameters, "returns": returns or SCORES}],
            }
        ],
        "types": TYPES,
    }


@pytest.mark.parametrize("rules", ALL_RULES)
@pytest.mark.parametrize(("tag", "kind"), [("command", "mutation"), ("query", "query")])
def test_in_each_language_a_query_is_of_kind_query_and_a_command_a_mutation(rules, tag, kind):
    export = read_manifest(_raw(tag, [_parameter("orders", _table())]), rules).export("score")
    assert commands.command_kind(export) == kind


@pytest.mark.parametrize("tag", ["command", "query"])
def test_a_table_argument_declares_its_input_and_output_columns(tag):
    manifest = read_manifest(_raw(tag, [_parameter("orders", _table())]))
    export = manifest.export("score")
    assert export.parameters[0].shape == TableOf(Named("Order"))
    arguments, outputs = commands.contract(manifest, export)
    assert arguments == [
        {
            "name": "orders",
            "type": "text",
            "arg_kind": "table_ref",
            "columns": [{"name": "id", "type": "bigint"}, {"name": "region", "type": "text"}],
        }
    ]
    assert outputs == [{"name": "region", "type": "text"}, {"name": "score", "type": "double"}]


def test_a_query_and_a_command_differ_only_in_their_kind():
    parameters = [_parameter("orders", _table()), _parameter("floor", {"kind": "number"})]
    query = read_manifest(_raw("query", parameters))
    command = read_manifest(_raw("command", parameters))
    assert commands.contract(query, query.export("score")) == commands.contract(
        command, command.export("score")
    )
    assert query.export("score").parameters == command.export("score").parameters


def test_a_table_argument_survives_being_kept_with_a_built_source():
    manifest = read_manifest(_raw("query", [_parameter("orders", _table())]))
    kept = manifest_from_json(manifest_json(manifest))
    assert kept.export("score") == manifest.export("score")


@pytest.mark.parametrize("row", [{"kind": "any"}, {"kind": "record"}])
def test_a_table_argument_with_a_loose_row_type_is_refused_by_name(row):
    with pytest.raises(ManifestRefused, match=r"score\(orders\).*input columns"):
        read_manifest(_raw("command", [_parameter("orders", _table(row))]))


def test_an_optional_table_argument_is_refused_by_name():
    with pytest.raises(ManifestRefused, match=r"score\(orders\).*required"):
        read_manifest(_raw("command", [_parameter("orders", _table(), optional=True)]))


def test_a_table_argument_on_anything_but_a_command_or_query_is_refused_by_name():
    raw = _raw("columns", [_parameter("orders", _table())])
    with pytest.raises(ManifestRefused, match=r"@command or a @query only"):
        read_manifest(raw)


def test_a_query_binds_to_no_write_of_a_code_table():
    rows = {"kind": "array", "element": ORDER}
    raw = _raw("query", [_parameter("rows", rows)], name="add")
    raw["exports"].append(
        {
            "name": "orders",
            "tags": ["table"],
            "tag_arguments": [],
            "description": "",
            "signatures": [{"parameters": [], "returns": rows}],
        }
    )
    assert bindings(read_manifest(raw), "orders", ["id"]) == {}


def test_a_query_tagged_as_a_tables_write_is_refused_by_name():
    raw = _raw("query", [_parameter("rows", SCORES)])
    raw["exports"][0]["tag_arguments"] = [{"tag": "insert", "text": "orders"}]
    with pytest.raises(ManifestRefused, match=r"@insert binds a command"):
        read_manifest(raw)


# -- the call -----------------------------------------------------------------------------------


class _Worker:
    def __init__(self, answer) -> None:
        self.answer = answer
        self.calls: list[tuple] = []

    def command(self, export, args, names, *, schema=None):
        self.calls.append((export, args, names, schema))
        return self.answer


def _registered(monkeypatch, tag: str, answer) -> tuple[_Worker, dict]:
    manifest = read_manifest(
        _raw(
            tag,
            [_parameter("orders", _table()), _parameter("floor", {"kind": "number"}, True)],
        )
    )
    worker = _Worker(answer)
    prepared = SimpleNamespace(manifest=manifest, worker=lambda: worker, remote=None)

    async def _prepare(_state, _source_id, _function):
        return prepared

    monkeypatch.setattr(commands, "_prepared", _prepare)
    arguments, outputs = commands.contract(manifest, manifest.export("score"))
    return worker, {
        "name": "score",
        "function_name": "score",
        "source_id": "code1",
        "impl_kind": commands.CODE_COMMAND,
        "kind": commands.command_kind(manifest.export("score")),
        "arguments": arguments,
        "output_columns": outputs,
    }


class _Tokens:
    async def mint(self, _call) -> str:
        return "provisa_call_test"

    async def retire(self, _token) -> None:
        return None


def _listening(monkeypatch) -> SimpleNamespace:
    """A state whose minted-token listener runs, with its store stood in for."""
    from provisa.core import settings_registry
    from provisa.pgwire import minted

    monkeypatch.setattr(minted, "MintedTokenStore", lambda _db: _Tokens())
    monkeypatch.setattr(settings_registry, "value", {"server.minted_pgwire_port": 6543}.__getitem__)
    return SimpleNamespace(
        admin_db=None,
        _minted_pgwire_server=SimpleNamespace(ssl_ctx=None),
        meta_qualifiers={},
        org_id="org1",
    )


@pytest.mark.asyncio
async def test_a_call_gives_the_function_the_table_reference_and_its_declared_columns(monkeypatch):
    rows = [{"region": "emea", "score": 0.5}]
    worker, fn = _registered(monkeypatch, "query", rows)
    answered = await dispatch_function(
        fn, {"a0": "main.public.orders"}, _listening(monkeypatch), "analyst"
    )
    assert answered == rows
    from provisa.codesource.worker import ABSENT

    export, args, names, schema = worker.calls[0]
    assert (export, names) == ("score", ["orders", "floor"])
    assert args[1:] == [ABSENT]
    given = {k: v for k, v in args[0].items() if k != "connection"}
    assert given == {
        "kind": "table_ref",
        "ref": "main.public.orders",
        "columns": ["id", "region"],
        "sql": 'SELECT "id", "region" FROM main.public.orders',
    }
    assert schema is not None


@pytest.mark.asyncio
async def test_rows_off_the_declared_output_contract_fail_loud(monkeypatch):
    from fastapi import HTTPException

    _, fn = _registered(monkeypatch, "query", [{"region": "emea"}])
    with pytest.raises(HTTPException):
        await dispatch_function(
            fn, {"a0": "main.public.orders"}, _listening(monkeypatch), "analyst"
        )


# -- composition and lineage --------------------------------------------------------------------


def _function(kind: str) -> dict:
    return {"name": "score", "impl_kind": commands.CODE_COMMAND, "kind": kind}


def test_a_view_may_call_a_query_and_may_not_call_a_mutation():
    sql = "SELECT * FROM score('main.public.orders') AS s"
    refuse_writes_in_definition(sql, {"score": _function("query")}, "view v")
    with pytest.raises(ValueError, match="score"):
        refuse_writes_in_definition(sql, {"score": _function("mutation")}, "view v")


def test_a_views_lineage_runs_from_the_input_table_through_the_command_to_its_outputs():
    manifest = read_manifest(_raw("query", [_parameter("orders", _table())]))
    arguments, outputs = commands.contract(manifest, manifest.export("score"))
    command = {
        "name": "score",
        "impl_kind": commands.CODE_COMMAND,
        "kind": "query",
        "arguments": arguments,
        "output_columns": outputs,
    }
    graph = build_column_graph(
        "SELECT s.score FROM score('main.public.orders') AS s", commands={"score": command}
    )
    sources = {n.id for n in graph.nodes.values() if n.kind == "source"}
    assert sources == {"main.public.orders.id", "main.public.orders.region"}
    into_score = [e for e in graph.edges if e.target == "s.score"]
    assert {e.source for e in into_score} >= sources
    assert all(e.ops[0].name == "score" for e in into_score if e.source in sources)
