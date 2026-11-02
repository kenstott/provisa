# Copyright (c) 2026 Kenneth Stott
# Canary: 6bcc4a47-21c3-40b7-b14c-743d3ab9bd65
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2113: a single-file CSV or Parquet source's table is set up with it, as a draft, when the
create form (or Polly) asks: named after the source, typed from its file as Register Table
proposes it, every role seeing every column. When it cannot be, the source stands and why is
said."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.api.admin import schema_query
from provisa.api.admin.schema_mutation import Mutation, _register_draft_table
from provisa.api.admin.types import AvailableColumnType, MutationResult, SourceInput


@pytest.fixture
def registered(monkeypatch):
    from provisa.api.app import state

    seen: list = []

    async def _columns(source_id, schema_name, table_name):
        assert (schema_name, table_name) == ("main", source_id)
        return [
            AvailableColumnType(name="item", data_type="varchar", comment=None),
            AvailableColumnType(name="on_hand", data_type="integer", comment="How many"),
            AvailableColumnType(name="lastCounted", data_type="date", comment=None),
        ]

    async def _register(self, info, table):
        seen.append(table)
        return MutationResult(success=True, message="ok", code="schema.table_registered")

    monkeypatch.setattr(schema_query, "resolve_available_columns_metadata", _columns)
    monkeypatch.setattr(Mutation, "register_table", _register)
    monkeypatch.setattr(state, "roles", {"analyst": object(), "admin": object()}, raising=False)
    return seen


async def test_the_draft_table_is_named_and_typed_from_the_file(registered):
    made = SourceInput(
        id="supplies", type="parquet", register_table_draft=True, table_domain_id="shelter"
    )
    assert await _register_draft_table(SimpleNamespace(context={}), made) is None
    (table,) = registered
    assert (table.source_id, table.domain_id, table.schema_name, table.table_name) == (
        "supplies",
        "shelter",
        "main",
        "supplies",
    )
    # As the Register Table form gives a new table: every role, domain scope, no mask, and a
    # snake_case alias only where the name is not one.
    assert [
        (c.name, c.alias, c.data_type, c.visible_to, c.scope, c.masks) for c in table.columns
    ] == [
        ("item", None, "varchar", ["admin", "analyst"], "domain", []),
        ("on_hand", None, "integer", ["admin", "analyst"], "domain", []),
        ("lastCounted", "last_counted", "date", ["admin", "analyst"], "domain", []),
    ]


async def test_without_a_domain_or_for_another_type_the_source_stands_and_says_why(registered):
    info = SimpleNamespace(context={})
    no_domain = SourceInput(id="s", type="csv", register_table_draft=True)
    said = await _register_draft_table(info, no_domain)
    assert said is not None and said.success and said.code == "schema.source_created_without_table"
    assert "no domain" in said.params["reason"]
    sqlite = SourceInput(id="db", type="sqlite", register_table_draft=True, table_domain_id="d")
    said = await _register_draft_table(info, sqlite)
    assert said is not None and said.success and "single table" in said.params["reason"]
    assert registered == []


def test_no_mutation_resolver_reaches_through_self():
    """Strawberry runs a Mutation resolver with `self` as the schema's root value, which the admin
    schema never sets: `self._register_draft_table` raised "'NoneType' object has no attribute
    '_register_draft_table'" on every createSource with registerTableDraft."""
    import ast
    import inspect

    from provisa.api.admin import schema_mutation

    tree = ast.parse(inspect.getsource(schema_mutation.Mutation))
    through_self = sorted(
        {
            n.attr
            for n in ast.walk(tree)
            if isinstance(n, ast.Attribute)
            and isinstance(n.value, ast.Name)
            and n.value.id == "self"
        }
    )
    assert through_self == []


def _publishing(monkeypatch, register):
    """Publish a new source with ``register`` standing in for its draft table: the rebuilds run."""
    from provisa.api.admin import schema_mutation

    rebuilds: list[str] = []

    async def _rebuild():
        rebuilds.append("rebuild")

    monkeypatch.setattr(schema_mutation, "_rebuild_schemas", _rebuild)
    monkeypatch.setattr(schema_mutation, "_register_draft_table", register)
    return schema_mutation._publish_new_source, rebuilds


async def test_a_source_saved_with_its_draft_table_rebuilds_the_schemas_once(monkeypatch):
    """REQ-2113, REQ-2006: a rebuild takes 14-20 s on a demo sandbox while the tier builds, and the
    table's registration ends in one: the source's own before it was paid for twice on every Save
    (79 s, 2026-11-01). The table is typed from the file, which needs no rebuild."""

    async def _registered(info, input):
        return None

    publish, rebuilds = _publishing(monkeypatch, _registered)
    made = SourceInput(id="s", type="parquet", register_table_draft=True, table_domain_id="d")
    assert await publish(SimpleNamespace(context={}), made) is None
    assert rebuilds == []  # the registration's own stands for it: stubbed here


async def test_a_source_without_a_draft_table_is_rebuilt_for(monkeypatch):
    async def _never(info, input):
        raise AssertionError("no draft table was asked for")

    publish, rebuilds = _publishing(monkeypatch, _never)
    assert await publish(SimpleNamespace(context={}), SourceInput(id="s", type="csv")) is None
    assert rebuilds == ["rebuild"]


async def test_a_source_whose_draft_table_was_not_set_up_is_rebuilt_for(monkeypatch):
    said = MutationResult(success=True, message="m", code="schema.source_created_without_table")

    async def _without(info, input):
        return said

    publish, rebuilds = _publishing(monkeypatch, _without)
    made = SourceInput(id="s", type="csv", register_table_draft=True)
    assert await publish(SimpleNamespace(context={}), made) is said
    assert rebuilds == ["rebuild"]


async def test_a_draft_table_that_raised_leaves_the_source_rebuilt_for(monkeypatch):
    async def _raises(info, input):
        raise RuntimeError("the file could not be read")

    publish, rebuilds = _publishing(monkeypatch, _raises)
    made = SourceInput(id="s", type="csv", register_table_draft=True, table_domain_id="d")
    with pytest.raises(RuntimeError):
        await publish(SimpleNamespace(context={}), made)
    assert rebuilds == ["rebuild"]
