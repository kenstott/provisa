# Copyright (c) 2026 Kenneth Stott
# Canary: 7a2c9e14-3b58-4f0d-86a1-c4e2d9b07f53
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A code resolver set: one resolver picked, kept as objects of kinds that already exist -- a
bridge view over the resolver's call, a junction relationship through it (Cypher's one edge) and
two ordinary relationships to and from it (GraphQL's two levels) -- saved by each kind's own
mutation, marked with the set, and changed or retired only through the set."""

# Requirements: REQ-2249, REQ-2248

from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Any, cast

import pytest

from provisa.api.admin import code_resolver
from provisa.api.admin.types import CodeResolverInput, MutationResult

#: The request a mutation is called with: the set reads nothing of it here.
_INFO = cast(Any, SimpleNamespace())

_RESOLVER = {
    "name": "match_region",
    "impl_kind": "code_command",
    "kind": "query",
    "arguments": [
        {"name": "orders", "type": "String", "arg_kind": "table_ref", "columns": []},
        {"name": "clips", "type": "String", "arg_kind": "table_ref", "columns": []},
    ],
    "output_columns": [
        {"name": "order_id", "type": "bigint"},
        {"name": "clip_id", "type": "bigint"},
        {"name": "score", "type": "double"},
    ],
}


def _table(table_id: int, name: str, readers: list[str], **more) -> dict:
    row = {
        "id": table_id,
        "table_name": name,
        "alias": None,
        "domain_id": "fx",
        "retired": False,
        "code_resolver": None,
        "columns": [{"column_name": "id", "visible_to": readers}],
    }
    row.update(more)
    return row


def _input(**more) -> CodeResolverInput:
    fields = {
        "id": "order_clips",
        "resolver": "match_region",
        "source_table": "orders",
        "source_column": "id",
        "source_match": "order_id",
        "target_table": "clips",
        "target_column": "id",
        "target_match": "clip_id",
    }
    fields.update(more)
    return CodeResolverInput(**fields)


class _Model:
    """The registered tables, the calls each kind's mutation received, and what the set marked
    and discarded."""

    def __init__(self) -> None:
        self.tables = {
            "orders": _table(1, "orders", ["admin", "analyst"]),
            "clips": _table(2, "clips", ["admin", "analyst", "guest"]),
        }
        self.calls: list[tuple[str, object]] = []
        self.refuse_edge: str | None = None


@pytest.fixture
def model(monkeypatch) -> _Model:
    from provisa.api.admin import capabilities, domain_guard, schema_helpers, schema_mutation
    from provisa.api.admin import schema_mutation_ops as ops
    from provisa.api import app
    from provisa.core.repositories import relationship as rel_repo
    from provisa.core.repositories import table as table_repo

    m = _Model()
    monkeypatch.setattr(app.state, "tracked_functions", {"match_region": _RESOLVER}, raising=False)

    class _Conn:
        async def execute_core(self, statement):
            m.calls.append(("mark", str(statement)))

        @asynccontextmanager
        async def transaction(self):
            yield

    class _Pool:
        @asynccontextmanager
        async def acquire(self):
            yield _Conn()

    async def _pool():
        return _Pool()

    async def _find(conn, name):
        return m.tables.get(name)

    async def _get(conn, table_id):
        return next((t for t in m.tables.values() if t["id"] == table_id), None)

    async def _register(info, view):
        m.calls.append(("register", view))
        m.tables[view.table_name] = _table(3, view.table_name, [])
        return MutationResult(success=True, message="registered (id=3)")

    async def _save_edge(info, edge, *, creating):
        m.calls.append(("edge", edge))
        ok = edge.id != m.refuse_edge
        return MutationResult(success=ok, message="saved" if ok else "refused")

    class _Single:
        async def update_table(self, info, view):
            m.calls.append(("update", view))
            return MutationResult(success=True, message="updated")

    async def _remove_where(conn, *where):
        m.calls.append(("discard_edges", str(where[0])))

    async def _discard(conn, table_id):
        m.calls.append(("discard_bridge", table_id))

    monkeypatch.setattr(schema_helpers, "_get_pool", _pool)
    monkeypatch.setattr(table_repo, "find_by_table_name", _find)
    monkeypatch.setattr(table_repo, "get", _get)
    monkeypatch.setattr(table_repo, "discard", _discard)
    monkeypatch.setattr(rel_repo, "remove_where", _remove_where)
    monkeypatch.setattr(ops, "register_table", _register)
    monkeypatch.setattr(schema_mutation, "_save_relationship_as_caller", _save_edge)
    monkeypatch.setattr(schema_mutation, "_single", _Single)
    from provisa.security import rights

    # The roles a key is served to: here, its grant as written.
    monkeypatch.setattr(
        rights,
        "served_roles",
        lambda roles, chains, domain, column: frozenset(column["visible_to"]),
    )
    monkeypatch.setattr(app.state, "roles", {}, raising=False)
    monkeypatch.setattr(app.state, "role_chains", {}, raising=False)
    monkeypatch.setattr(capabilities, "require_domain", lambda info, domain: None)
    monkeypatch.setattr(domain_guard, "require_domains", lambda info, domains: None)
    return m


def _kinds(m: _Model) -> list[str]:
    return [kind for kind, _ in m.calls]


async def test_a_new_set_saves_the_bridge_then_its_relationships_then_marks_it(model):
    """REQ-1921: the bridge is live on registration: nothing releases it afterwards."""
    result = await code_resolver.save(_INFO, _input(), creating=True)
    assert result.success, result.message
    assert _kinds(model) == ["register", "edge", "edge", "edge", "mark", "mark"]
    view = model.calls[0][1]
    assert view.table_name == "order_clips" and view.schema_name == "views"
    assert view.view_sql == "SELECT * FROM match_region('fx.orders', 'fx.clips') AS m"
    assert view.materialize is True
    # A match is visible to the roles that see both keys: guest sees only the clips' key.
    assert {c.name: c.visible_to for c in view.columns} == {
        "order_id": ["admin", "analyst"],
        "clip_id": ["admin", "analyst"],
        "score": ["admin", "analyst"],
    }
    junction, matches, to_target = (model.calls[i][1] for i in (1, 2, 3))
    # One edge from source to target through the bridge, named by a fixed label: Cypher's.
    assert (junction.id, junction.source_table_id, junction.target_table_id) == (
        "order_clips",
        "orders",
        "clips",
    )
    assert (junction.via_table, junction.via_source_column, junction.via_target_column) == (
        "order_clips",
        "order_id",
        "clip_id",
    )
    assert (junction.via_label_source, junction.alias) == ("fixed", "ORDER_CLIPS")
    assert not junction.disable_cypher
    # The two levels GraphQL nests, left out of Cypher, where the bridge is no node.
    assert matches.disable_cypher and to_target.disable_cypher
    assert (matches.id, matches.source_table_id, matches.target_table_id) == (
        "order_clips_matches",
        "orders",
        "order_clips",
    )
    assert (matches.source_column, matches.target_column, matches.cardinality) == (
        "id",
        "order_id",
        "one-to-many",
    )
    assert matches.graphql_alias == "order_clips"
    assert (to_target.id, to_target.source_table_id, to_target.target_table_id) == (
        "order_clips_target",
        "order_clips",
        "clips",
    )
    assert (to_target.source_column, to_target.target_column, to_target.cardinality) == (
        "clip_id",
        "id",
        "many-to-one",
    )
    assert model.calls[-1][1] == (3, False)


async def test_a_resolver_whose_rows_lack_a_key_is_refused_with_the_reason_and_nothing_saved(
    model,
):
    result = await code_resolver.save(_INFO, _input(target_match="region_id"), creating=True)
    assert not result.success
    assert result.code == "schema.code_resolver_refused"
    assert "its rows carry no 'region_id'" in result.message
    assert model.calls == []


async def test_a_key_no_role_sees_on_both_ends_is_refused(model):
    model.tables["clips"] = _table(2, "clips", ["guest"])
    result = await code_resolver.save(_INFO, _input(), creating=True)
    assert not result.success and result.code == "schema.code_resolver_no_reader"
    assert model.calls == []


async def test_an_id_that_is_not_a_table_name_is_refused(model):
    result = await code_resolver.save(_INFO, _input(id="Order Clips"), creating=True)
    assert not result.success and result.code == "schema.code_resolver_bad_id"


async def test_a_save_refused_part_way_leaves_nothing_of_the_set(model):
    model.refuse_edge = "order_clips_target"
    result = await code_resolver.save(_INFO, _input(), creating=True)
    assert not result.success
    assert _kinds(model) == [
        "register",
        "edge",
        "edge",
        "edge",
        "discard_edges",
        "discard_bridge",
    ]
    assert model.calls[-1][1] == 3


async def test_a_change_saves_the_three_objects_through_their_updates(model):
    model.tables["order_clips"] = _table(
        3, "order_clips", [], code_resolver="order_clips", schema_name="views"
    )
    result = await code_resolver.save(_INFO, _input(refresh_interval=60), creating=False)
    assert result.success, result.message
    assert _kinds(model) == ["update", "edge", "edge", "edge", "mark", "mark"]
    assert model.calls[0][1].mv_refresh_interval == 60


async def test_a_change_to_a_set_that_does_not_exist_is_refused(model):
    result = await code_resolver.save(_INFO, _input(), creating=False)
    assert not result.success and result.code == "schema.code_resolver_not_found"


def test_an_object_of_a_set_is_refused_a_change_made_apart_from_it():
    held = code_resolver.held_by_set(
        {"code_resolver": "order_clips"}, "relationship", "order_clips_target"
    )
    assert held is not None and held.code == "schema.code_resolver_part"
    assert "change or retire it there" in held.message
    assert code_resolver.held_by_set({"code_resolver": None}, "table", "orders") is None
    assert code_resolver.held_by_set(None, "table", "orders") is None


def test_the_set_being_saved_may_change_its_own_objects():
    with code_resolver._changing("order_clips"):
        assert code_resolver.held_by_set({"code_resolver": "order_clips"}, "table", "x") is None
        assert code_resolver.held_by_set({"code_resolver": "other"}, "table", "x") is not None
