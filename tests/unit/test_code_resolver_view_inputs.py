# Copyright (c) 2026 Kenneth Stott
# Canary: 3c1e8a52-6f0d-4b7e-9a21-5d84c0f7b913
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A view over a code resolver's call reads the tables the call is given.

A code resolver is a code ``@query`` taking the source and the target table as table parameters
(REQ-2226) and returning one row per match. A view over its call names those tables only as the
call's string arguments, so the view used to read nothing: a change to either table neither marked
it stale nor moved its input token, and its landed matches were never recomputed. Its inputs are
now the call's table arguments, resolved like the tables a FROM names."""

# Requirements: REQ-2249, REQ-2226

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.mv.models import MVDefinition, MVStatus, TableIdentity
from provisa.mv.readable_inputs import read_table_names
from provisa.mv.registry import MVRegistry
from provisa.mv.view_inputs import call_table_refs, read_tables, resolve_view_inputs

_MATCH = {
    "name": "match_stores",
    "impl_kind": "code_command",
    "kind": "query",
    "arguments": [
        {"name": "stores", "type": "String", "arg_kind": "table_ref"},
        {"name": "radius", "type": "Float", "arg_kind": "column_value"},
        {"name": "regions", "type": "String", "arg_kind": "table_ref"},
    ],
}
_COMMANDS = {"match_stores": _MATCH}
_SQL = "SELECT m.store_id, m.region_id, m.score FROM match_stores('retail.stores', 5.0, 'geo.regions') AS m"


def _row(table_id: int, source: str, schema: str, table: str) -> dict:
    return {
        "id": table_id,
        "source_id": source,
        "schema_name": schema,
        "table_name": table,
        "domain_id": None,
        "alias": None,
        "view_sql": None,
    }


def _state() -> SimpleNamespace:
    return SimpleNamespace(
        tables=[_row(1, "shop", "retail", "stores"), _row(2, "maps", "geo", "regions")],
        source_catalogs={"shop": "shop", "maps": "maps"},
        view_context=None,
        mv_registry=SimpleNamespace(get_enabled=lambda: []),
        tracked_functions=_COMMANDS,
    )


def _view(sql: str = _SQL) -> MVDefinition:
    return MVDefinition(
        id="view-store_regions",
        source_tables=[],
        target_catalog="mat_store",
        target_schema="mat",
        target_table="mv_store_regions",
        sql=sql,
        view_table="store_regions",
        read_tables=read_table_names(sql, _COMMANDS),
        status=MVStatus.FRESH,
    )


def test_a_resolver_calls_table_arguments_are_what_it_reads():
    assert call_table_refs(_SQL, _COMMANDS) == [("retail", "stores"), ("geo", "regions")]


def test_a_call_of_no_registered_command_reads_nothing():
    assert call_table_refs(_SQL, {}) == []


def test_a_view_over_the_call_resolves_both_tables_as_its_inputs():
    resolved = resolve_view_inputs(_view(), _state())
    assert [r.table_id for r in resolved] == [1, 2]


def test_the_refresh_asks_both_tables_for_their_version():
    assert read_tables(_view(), _state()) == [
        TableIdentity("shop", "retail", "stores"),
        TableIdentity("maps", "geo", "regions"),
    ]


@pytest.mark.parametrize("changed", ["stores", "regions"])
def test_a_write_to_either_table_marks_the_landed_view_stale(changed):
    registry = MVRegistry()
    registry.register(_view())
    assert registry.mark_stale(changed) == ["view-store_regions"]
    assert registry.get("view-store_regions").status == MVStatus.STALE


def test_a_table_argument_that_is_not_a_written_table_name_is_refused_naming_the_command():
    sql = "SELECT * FROM match_stores(t.name, 5.0, 'geo.regions') AS m, retail.stores t"
    with pytest.raises(ValueError, match="match_stores: its table argument 'stores'"):
        call_table_refs(sql, _COMMANDS)
