# Copyright (c) 2026 Kenneth Stott
# Canary: 4b7d2c19-8e3a-4f61-b0c5-6a9e1d3f7c24
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1415: tag ids ride in the bracketed description suffix, on every catalog surface.

The suffix is a human-facing mirror built by one formatter, so pg_description, GraphQL
introspection and the Flight/MCP catalog show the same text. It stays unambiguous because a tag
id and a tag parameter value are slugs from the moment they are created.
"""

# Requirements: REQ-1320, REQ-1415

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.api.admin.schema_mutation import Mutation
from provisa.api.admin.types import TagInput, TagParamValueInput
from provisa.api.flight.catalog import build_catalog_tables_from_context
from provisa.core.modeling_tags import (
    append_modeling_tag,
    attach_tags,
    column_comment,
    is_tag_slug,
    tags_by_object,
)

_ASSIGNMENTS = [
    ("natural_language", "column", 2, "name"),
    ("entity:customer", "column", 2, "name"),
    ("pii", "table", 2, None),
    ("deprecated", "table", 1, None),
    ("technical", "source", None, None),
    ("pii", "relationship", None, None),
]


def test_tags_follow_the_role_in_one_suffix():
    assert (
        append_modeling_tag("Customer master", "dimension", "scd2", ["entity", "pii"])
        == "Customer master [dimension, scd2, entity, pii]"
    )


def test_tags_alone_make_a_suffix_when_no_role_is_set():
    assert append_modeling_tag("Notes", None, None, ["natural_language"]) == (
        "Notes [natural_language]"
    )
    assert append_modeling_tag(None, None, None, ["pii"]) == "[pii]"


def test_history_without_a_role_is_not_listed():
    assert append_modeling_tag("Notes", None, "scd2", ["pii"]) == "Notes [pii]"


def test_a_tagged_column_lists_its_tables_role_and_its_own_tags():
    assert column_comment("Customer name", "dimension", ["entity", "natural_language"]) == (
        "Customer name [dimension, entity, natural_language]"
    )


def test_an_untagged_column_keeps_its_description_as_written():
    assert column_comment("Customer name", "dimension", []) == "Customer name"
    assert column_comment(None, "dimension", []) == ""


def test_assignments_are_indexed_by_table_and_column_in_one_order():
    by_table, by_column = tags_by_object(_ASSIGNMENTS)
    assert by_table == {1: ["deprecated"], 2: ["pii"]}
    assert by_column == {(2, "name"): ["entity:customer", "natural_language"]}


def test_attach_tags_puts_the_ids_on_every_table_and_column_row():
    tables = [
        {"id": 1, "columns": [{"column_name": "id"}]},
        {"id": 2, "columns": [{"column_name": "name"}, {"column_name": "id"}]},
    ]
    attach_tags(tables, _ASSIGNMENTS)
    assert tables[0]["tags"] == ["deprecated"]
    assert tables[0]["columns"][0]["tags"] == []
    assert tables[1]["tags"] == ["pii"]
    assert tables[1]["columns"][0]["tags"] == ["entity:customer", "natural_language"]


@pytest.mark.parametrize("value", ["pii", "natural_language", "gdpr-art-9", "tier2"])
def test_a_slug_is_accepted(value):
    assert is_tag_slug(value)


@pytest.mark.parametrize("value", ["a,b", "a]b", "[a", "A", "a b", "", "a:b", "é"])
def test_anything_else_is_not_a_slug(value):
    assert not is_tag_slug(value)


# -- the surfaces -------------------------------------------------------------------------------


def test_graphql_type_and_field_descriptions_carry_the_tags():
    from tests.unit.test_sql_gen_aggregate import _FACT_TABLES, _fact_schema_and_ctx

    tables = [{**t, "columns": [dict(c) for c in t["columns"]]} for t in _FACT_TABLES]
    tables[1]["columns"][1]["description"] = "Customer name"
    attach_tags(tables, _ASSIGNMENTS)
    schema, _ = _fact_schema_and_ctx(tables=tables)
    assert schema.type_map["Orders"].description == "Order fact rows [fact, deprecated]"
    customers = schema.type_map["Customers"]
    assert customers.description == "Customer dimension [dimension, scd2, pii]"
    assert customers.fields["name"].description == (
        "Customer name [dimension, entity:customer, natural_language]"
    )
    assert customers.fields["id"].description is None


def test_pg_description_carries_the_tags():
    import duckdb

    from provisa.pgwire.catalog_populate import _populate_pg_description

    db = duckdb.connect()
    db.execute(
        "CREATE TABLE _pg_description (objoid INTEGER, classoid INTEGER, objsubid INTEGER, "
        "description VARCHAR)"
    )
    idx = SimpleNamespace(
        ns_map={},
        tables=[("sales", "customers", "customers", 2, 9002)],
        col_attnum={(9002, "name"): 2},
        metric_oids={},
    )
    raw = [
        {
            "id": 2,
            "description": "Customer dimension",
            "modeling_role": "dimension",
            "modeling_history": "scd2",
            "columns": [{"column_name": "name", "description": "Customer name"}],
        }
    ]
    attach_tags(raw, _ASSIGNMENTS)
    _populate_pg_description(db, idx, raw)
    rows = dict(db.execute("SELECT objsubid, description FROM _pg_description").fetchall())
    db.close()
    assert rows == {
        0: "Customer dimension [dimension, scd2, pii]",
        2: "Customer name [dimension, entity:customer, natural_language]",
    }


def test_flight_catalog_carries_the_tags():
    column = SimpleNamespace(
        column_name="name", data_type="varchar", is_nullable=True, description="Customer name"
    )
    tinfo = SimpleNamespace(
        table_id=2,
        domain_id="sales",
        description="Customer dimension",
        modeling_role="dimension",
        modeling_history="scd2",
        columns=[column],
    )
    assignments = [
        SimpleNamespace(tag_id=t, object_type=o, table_id=i, column_name=c)
        for t, o, i, c in _ASSIGNMENTS
    ]
    from tests.role_state import RoleState

    state = RoleState(
        contexts={"analyst": SimpleNamespace(table_map={"customers": tinfo})},
        config=SimpleNamespace(tag_assignments=assignments),
    )
    table = build_catalog_tables_from_context(state)[0]
    assert table.description == "Customer dimension [dimension, scd2, pii]"
    assert table.columns[0].description == (
        "Customer name [dimension, entity:customer, natural_language]"
    )


# -- the constraint is enforced where a tag is created -------------------------------------------


def _info(monkeypatch):
    import provisa.api.app as app_mod
    from tests.unit.gate_identity import grant

    return grant(monkeypatch, "table_registration", state=app_mod.state)[0]


@pytest.mark.parametrize("bad_id", ["a,b", "a]b", "[a", "Has Space", "UPPER"])
async def test_a_user_tag_id_that_is_not_a_slug_is_refused(monkeypatch, bad_id):
    result = await Mutation().create_tag(
        _info(monkeypatch), TagInput(id=bad_id, applies_to=["table"])
    )
    assert not result.success
    assert result.code == "schema.tag_id_not_slug"


@pytest.mark.parametrize("bad_value", ["a,b", "a]b", "Has Space"])
async def test_a_parameter_value_that_is_not_a_slug_is_refused(monkeypatch, bad_value):
    result = await Mutation().upsert_tag_param_value(
        _info(monkeypatch), TagParamValueInput(tag_id="entity", value=bad_value)
    )
    assert not result.success
    assert result.code == "schema.tag_param_value_not_slug"
