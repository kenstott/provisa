# Copyright (c) 2026 Kenneth Stott
# Canary: 9a3d61f4-2c87-4b5e-a0f9-7e14c6b8d235
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A view's table transform run on Spark (REQ-2225, amended 2026-11-23).

The Spark connector's script declares a ``@query`` taking a governed table (``TableRef[Clip]``,
REQ-2226) and returning rows. Registered, it is a code command like any code source's, so a view
over its call is landed and read only from its stored table, and its argument table is its input:
a write to that table marks it stale, and the event graph refreshes it when that table lands.
tests/integration/test_spark_transform_mv_e2e.py builds it on a one-node Spark, the table read
over Arrow Flight (REQ-2270)."""

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

#: What the connector's host reports for the script's
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
