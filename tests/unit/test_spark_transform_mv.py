# Copyright (c) 2026 Kenneth Stott
# Canary: 9a3d61f4-2c87-4b5e-a0f9-7e14c6b8d235
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A view's table transform run on Spark (REQ-2225, amended 2026-11-23 and 2026-11-25).

A Python code source's ``@query`` that runs on a Spark source's session takes a governed table
(``TableRef[Clip]``, REQ-2226) and returns rows. Registered, it is a code command like any code
source's, so a view over its call is landed and read only from its stored table, and its argument
table is its input: a write to that table marks it stale, and the event graph refreshes it when
that table lands.
tests/integration/test_spark_connector_e2e.py reads such a @query on a one-node Spark, the table
read over Arrow Flight (REQ-2270)."""

# Requirements: REQ-2225, REQ-2226, REQ-2270

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.codesource import commands
from provisa.codesource.manifest import PYTHON_RULES, read_manifest
from provisa.events.nodes import lineage_graph, source_node, view_node
from provisa.mv.models import MVDefinition, MVStatus, TableIdentity
from provisa.mv.readable_inputs import read_table_names
from provisa.mv.registry import MVRegistry
from provisa.mv.view_inputs import read_tables
from provisa.mv.view_read import lands_stored_read

#: What the reader reports for the Python source's
#: ``def clips_on_the_cluster(clips: TableRef[Clip]) -> list[Clip]`` tagged ``@query``.
_CLIP = {
    "name": "Clip",
    "fields": [
        {
            "name": "id",
            "optional": False,
            "type": {"kind": "number", "integer": True},
            "description": "",
        },
        {"name": "region", "optional": False, "type": {"kind": "string"}, "description": ""},
    ],
}
_RAW = {
    "exports": [
        {
            "name": "clips_on_the_cluster",
            "tags": ["query"],
            "tag_arguments": [],
            "description": "The clips the caller may read, as a DataFrame on the cluster.",
            "signatures": [
                {
                    "parameters": [
                        {
                            "name": "clips",
                            "optional": False,
                            "rest": False,
                            "type": {"kind": "table_ref", "row": {"kind": "ref", "name": "Clip"}},
                        }
                    ],
                    "returns": {"kind": "array", "element": {"kind": "ref", "name": "Clip"}},
                }
            ],
        }
    ],
    "types": {"Clip": _CLIP},
}
_SQL = "SELECT c.id, c.region FROM clips_on_the_cluster('ledger.clips') AS c"


def _command() -> dict:
    manifest = read_manifest(_RAW, PYTHON_RULES)
    export = manifest.export("clips_on_the_cluster")
    assert export is not None
    arguments, outputs = commands.contract(manifest, export)
    return {
        "name": "clips_on_the_cluster",
        "function_name": "clips_on_the_cluster",
        "source_id": "spark_jobs",
        "impl_kind": commands.CODE_COMMAND,
        "kind": commands.command_kind(export),
        "arguments": arguments,
        "output_columns": outputs,
    }


def _commands() -> dict[str, dict]:
    return {"clips_on_the_cluster": _command()}


def _state(registry: MVRegistry | None = None) -> SimpleNamespace:
    row = {
        "id": 7,
        "source_id": "ledger_pg",
        "schema_name": "public",
        "table_name": "clips",
        "domain_id": "ledger",
        "alias": None,
        "view_sql": None,
    }
    return SimpleNamespace(
        tables=[row],
        source_catalogs={"ledger_pg": "ledger_pg"},
        view_context=None,
        mv_registry=registry if registry is not None else MVRegistry(),
        tracked_functions=_commands(),
    )


def _view() -> MVDefinition:
    return MVDefinition(
        id="view-spark_clips",
        source_tables=[],
        target_catalog="mat_store",
        target_schema="mat",
        target_table="mv_spark_clips",
        sql=_SQL,
        semantic_sql=_SQL,
        view_table="spark_clips",
        read_tables=read_table_names(_SQL, _commands()),
        refresh_interval=3600,
        status=MVStatus.FRESH,
    )


def test_the_spark_query_is_a_code_command_taking_its_table_as_a_table_argument():
    command = _command()
    assert command["kind"] == "query"
    (clips,) = command["arguments"]
    assert clips["arg_kind"] == "table_ref"
    assert [c["name"] for c in clips["columns"]] == ["id", "region"]
    assert [c["name"] for c in command["output_columns"]] == ["id", "region"]


def test_a_view_over_its_call_is_landed_and_read_only_from_its_stored_table():
    assert lands_stored_read(_SQL, {}, _commands())


def test_a_view_over_no_call_of_it_is_not_landed_for_it():
    assert not lands_stored_read("SELECT id, region FROM ledger.clips", {}, _commands())


def test_the_views_input_is_the_table_the_call_is_given():
    assert read_tables(_view(), _state()) == [TableIdentity("ledger_pg", "public", "clips")]


def test_a_write_to_the_argument_table_marks_the_view_stale():
    registry = MVRegistry()
    registry.register(_view())
    assert registry.mark_stale("clips") == ["view-spark_clips"]
    kept = registry.get("view-spark_clips")
    assert kept is not None
    assert kept.status == MVStatus.STALE


def test_a_write_to_another_table_leaves_the_view_fresh():
    registry = MVRegistry()
    registry.register(_view())
    assert registry.mark_stale("orders") == []
    kept = registry.get("view-spark_clips")
    assert kept is not None
    assert kept.status == MVStatus.FRESH


def test_the_event_graph_refreshes_the_view_when_its_argument_table_lands():
    registry = MVRegistry()
    view = _view()
    registry.register(view)
    graph = lineage_graph([view], _state(registry))
    assert graph == {view_node(view): {source_node("ledger_pg", "public", "clips")}}


def test_a_view_calling_it_with_no_table_is_refused_naming_the_command():
    sql = "SELECT c.id FROM clips_on_the_cluster(t.name) AS c, ledger.clips AS t"
    with pytest.raises(ValueError, match="clips_on_the_cluster"):
        read_table_names(sql, _commands())


# --- a reader is judged by the read the call makes of its table (REQ-2226) -------------------
#
# A view over a code command's call is read from its stored table; its definition is judged for
# the reader, each table argument as the read the call's code is given (minted.read_statement).
# A table in a domain the reader does not reach is in none of the reader's compiled contexts, so
# only that read's column grants can refuse it -- as they refuse the call's own read when it runs.
# Any code command's call, whether or not its code runs on a Spark source's session.


def _outside_the_readers_domains(argument: str, columns: list[str] | None = None):
    """test_code_resolver_read's reader over the bridge, its call given ``argument`` for the
    clips table, which is in a domain the reader does not reach -- so, as a real build of its
    surface leaves it, in none of its compiled context's tables."""
    from tests.unit.test_code_resolver_read import _MATCH_REGION, _state

    state = _state(
        definition=f"SELECT * FROM match_region('orders', '{argument}') AS m",
        clips_domain="media",
        domain_access=["sales"],
    )
    state.contexts["analyst"].tables.pop("clips")
    command = state.tracked_functions["match_region"]
    command["arguments"] = [
        dict(_MATCH_REGION["arguments"][0]),
        {"name": "clips", "arg_kind": "table_ref", "columns": [{"name": c} for c in columns or []]},
    ]
    return state


@pytest.mark.asyncio
@pytest.mark.parametrize("argument", ["clips", "media.clips", "sales.clips"])
@pytest.mark.parametrize("columns", [["id", "region"], []])
async def test_a_reader_outside_the_argument_tables_domain_is_refused_the_view(
    monkeypatch, argument, columns
):
    from tests.unit.test_code_resolver_read import _READ_BRIDGE, _refused

    refused = await _refused(
        monkeypatch, _READ_BRIDGE, _outside_the_readers_domains(argument, columns)
    )
    assert "V003" in refused or "V006" in refused or "V000" in refused, refused


@pytest.mark.asyncio
@pytest.mark.parametrize("columns", [["id", "region"], []])
async def test_the_view_is_refused_in_the_words_the_calls_read_is_refused(monkeypatch, columns):
    from provisa.pgwire.minted import read_statement
    from tests.unit.test_code_resolver_read import _READ_BRIDGE, _refused

    stored = await _refused(
        monkeypatch, _READ_BRIDGE, _outside_the_readers_domains("media.clips", columns)
    )
    live = await _refused(
        monkeypatch,
        read_statement("media.clips", columns),
        _outside_the_readers_domains("media.clips", columns),
    )
    assert stored == live, (stored, live)


@pytest.mark.asyncio
async def test_a_column_the_command_reads_that_the_reader_cannot_see_refuses_the_view(
    monkeypatch,
):
    """In the reader's domains, but one declared input column hidden from it."""
    from tests.unit.test_code_resolver_read import _MATCH_REGION, _READ_BRIDGE, _refused, _state

    state = _state(hidden={2: {"secret"}})
    state.tracked_functions["match_region"]["arguments"] = [
        dict(_MATCH_REGION["arguments"][0]),
        {
            "name": "clips",
            "arg_kind": "table_ref",
            "columns": [{"name": "id"}, {"name": "secret"}],
        },
    ]
    refused = await _refused(monkeypatch, _READ_BRIDGE, state)
    assert "V003" in refused and "secret" in refused, refused


@pytest.mark.asyncio
async def test_a_reader_who_may_read_every_column_the_command_reads_reads_the_view(monkeypatch):
    from tests.unit.test_code_resolver_read import _MATCH_REGION, _READ_BRIDGE, _govern, _state

    state = _state(hidden={2: {"secret"}})
    state.tracked_functions["match_region"]["arguments"] = [
        dict(_MATCH_REGION["arguments"][0]),
        {"name": "clips", "arg_kind": "table_ref", "columns": [{"name": "id"}, {"name": "region"}]},
    ]
    governed = await _govern(monkeypatch, _READ_BRIDGE, state)
    assert "order_clips" in governed


# --- the view's lineage over its lowered SQL -------------------------------------------------
#
# A registered view's build SQL is its definition lowered to the engine's (app_rebuild,
# compile_view_sql_to_physical), which prints a function call upper-cased. Its lineage is read
# from that SQL: the call is still the command's, so a landing of its argument table reaches it.


def _lowered_view() -> MVDefinition:
    from provisa.api.app_rebuild import compile_view_sql_to_physical
    from provisa.compiler.sql_gen import CompilationContext

    view = _view()
    view.sql = compile_view_sql_to_physical(_SQL, CompilationContext())
    assert "CLIPS_ON_THE_CLUSTER" in view.sql, view.sql  # the case the lowering prints
    return view


def test_the_event_graph_reads_the_argument_table_from_the_lowered_sql():
    registry = MVRegistry()
    view = _lowered_view()
    registry.register(view)
    graph = lineage_graph([view], _state(registry))
    assert graph == {view_node(view): {source_node("ledger_pg", "public", "clips")}}


def test_a_landing_of_the_argument_table_is_told_to_the_view():
    from provisa.events import supervisor

    registry = MVRegistry()
    view = _lowered_view()
    registry.register(view)
    dependents = supervisor.dependents_of(lineage_graph([view], _state(registry)))
    assert dependents(source_node("ledger_pg", "public", "clips")) == [view_node(view)]


def test_the_views_input_is_read_from_the_lowered_sql():
    assert read_tables(_lowered_view(), _state()) == [TableIdentity("ledger_pg", "public", "clips")]


def test_a_change_function_finds_the_call_in_the_lowered_sql():
    from provisa.mv.change_refresh import code_call

    found = code_call(_lowered_view().sql, _commands())
    assert found is not None
    command, tables = found
    assert command["name"] == "clips_on_the_cluster"
    assert tables == ("ledger.clips",)
