# Copyright (c) 2026 Kenneth Stott
# Canary: 5d1c8e72-3f4b-4a96-b0e1-7c2a9d6f4b38
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1961 (amended 2026-10-28, VARCHAR AND CHAR KEEP THEIR TYPE): a PostgreSQL source column of
type varchar, varchar(n), char(n) or bpchar registers as text, keeping its native type as a
hidden attribute of the column -- the attribute REQ-1965 keeps a native array's in. Such a column
is no nested column: nothing that reads the attribute for an array, a struct or a map treats it
as one."""

# Requirements: REQ-1961, REQ-1965

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.api.admin import _table_ops, schema_query
from provisa.api.admin.introspect import _pg_column_typing
from provisa.api.admin.types import AvailableColumnType
from provisa.core.ir_types import ColumnTyping
from provisa.core.models import Column


@pytest.mark.parametrize(
    ("data_type", "udt_name", "formatted"),
    [
        ("character varying", "varchar", "character varying"),
        ("character varying", "varchar", "character varying(20)"),
        ("character", "bpchar", "character(5)"),
        ("character", "bpchar", "bpchar"),
    ],
)
def test_registration_keeps_a_postgresql_character_columns_native_type(
    data_type, udt_name, formatted
):
    typing = _pg_column_typing("postgresql", data_type, udt_name, formatted, "b")
    assert typing == ColumnTyping("text", native_type=formatted, native_platform="postgresql")


def test_a_kind_listing_no_formatted_type_keeps_the_types_name():
    # The wire kinds read from information_schema alone (_PG_WIRE_COLUMN_TYPES_SQL): no length.
    typing = _pg_column_typing("cockroachdb", "character varying", "varchar", None, None)
    assert typing == ColumnTyping("text", native_type="varchar", native_platform="cockroachdb")


def test_a_text_column_carries_no_native_type():
    assert _pg_column_typing("postgresql", "text", "text", "text", "b") == ColumnTyping("text")


def _schema_input(columns: list[dict]):
    from provisa.compiler.schema_types import SchemaInput

    table = {"schema_name": "public", "table_name": "people", "columns": columns}
    return SchemaInput(
        tables=[table],
        relationships=[],
        column_types={},
        naming_rules=[],
        role={},
        domains=[],
    )


def test_a_character_column_is_no_nested_column_to_the_compiler():
    from provisa.compiler.context import _nested_columns

    si = _schema_input(
        [
            {"column_name": "name", "native_type": "character varying(20)", "array_depth": 0},
            {
                "column_name": "tags",
                "native_type": "text[]",
                "array_depth": 1,
                "element_type": "text",
            },
        ]
    )
    nested = _nested_columns(si, {})
    assert set(nested[("public", "people")]) == {"tags"}


def test_a_character_column_is_landed_as_itself():
    from provisa.transpiler.native_json import source_projection

    columns = [
        Column(
            name="name",
            data_type="text",
            native_type="character(5)",
            native_platform="postgresql",
            visible_to=[],
        )
    ]
    assert source_projection(columns, "postgres") == '"name"'


def test_a_character_column_is_typed_by_graphql_as_its_model_type():
    from provisa.compiler.type_map import nested_column_graphql_type

    assert nested_column_graphql_type({"native_type": "character varying(20)"}) is None


_INPUT = SimpleNamespace(
    source_id="people-pg", schema_name="public", table_name="people", query_template=None
)


class _Pool:
    def acquire(self):
        return self

    async def __aenter__(self):
        return object()

    async def __aexit__(self, *exc):
        return False


def _source(monkeypatch, listed: list[AvailableColumnType]) -> None:
    async def _meta(source_id, schema_name, table_name):
        return listed

    async def _stored(_conn, _source_id, _schema, _table):
        return {}

    monkeypatch.setattr(schema_query, "resolve_available_columns_metadata", _meta)
    monkeypatch.setattr("provisa.core.repositories.table.stored_column_types", _stored)


def _name() -> AvailableColumnType:
    return AvailableColumnType(
        name="name",
        data_type="text",
        comment=None,
        native_type="character varying(20)",
        native_platform="postgresql",
    )


async def test_a_character_column_registered_as_text_records_its_native_type(monkeypatch):
    _source(monkeypatch, [_name()])
    columns = [Column(name="name", data_type="text", visible_to=[])]

    assert await _table_ops._settle_nested_columns(_Pool(), _INPUT, columns) is None

    name = columns[0]
    assert (name.array_depth, name.element_type, name.native_type, name.native_platform) == (
        0,
        None,
        "character varying(20)",
        "postgresql",
    )


async def test_a_character_column_registered_as_another_type_carries_no_native_type(monkeypatch):
    # Not refused: the column is no JSON column. It is the type it was given, and its native
    # type no longer describes it.
    _source(monkeypatch, [_name()])
    columns = [Column(name="name", data_type="integer", visible_to=[])]

    assert await _table_ops._settle_nested_columns(_Pool(), _INPUT, columns) is None

    assert columns[0].native_type is None


@pytest.mark.parametrize("native_type", ["address", "json_cols_c5f2a959.address", "address[]"])
def test_a_composite_column_is_still_landed_as_json(native_type):
    """A named composite's type is recorded by its name, which states no shape (REQ-1965): it is
    nested all the same, and only a character type's recorded name is not."""
    from provisa.core.native_types import natively_nested
    from provisa.transpiler.native_json import source_projection

    assert natively_nested(native_type)
    column = Column(
        name="ship_to",
        data_type="json",
        native_type=native_type,
        native_platform="postgresql",
        visible_to=[],
    )
    assert source_projection([column], "postgres") != '"ship_to"'


@pytest.mark.parametrize(
    "native_type", ["varchar", "character varying(20)", "character(5)", "bpchar", "char(3)"]
)
def test_a_character_type_is_no_nested_type(native_type):
    from provisa.core.native_types import natively_nested

    assert not natively_nested(native_type)
    assert natively_nested(f"{native_type}[]")
