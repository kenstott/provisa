# Copyright (c) 2026 Kenneth Stott
# Canary: 3e8b51d7-2c4f-4a96-b0e1-7f5d9c2a6b48
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The relation a remote GraphQL table is read as holds every column the model registers for it.

The process holds a remote source's tables as its endpoint's schema reads them (the graphql-demo
source is introspected at boot): the shelter's ``Schedule`` type has ``employee``, an object of
the registered ``Employee`` type, and no ``employee_id`` field. The model registers
``schedules.employee_id`` with ``gql_selection: "employee_id: employee { id }"``. The relation was
built from the endpoint's fields alone, so it held no ``employee_id``, and every read of the
registered column -- discovery's sample of it first -- failed in the engine: ``Binder Error:
Values list "t" does not have a column named "employee_id"``.
"""

# Requirements: REQ-308, REQ-413

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from provisa.api.data.materialization import _mat_gql_remote_table
from provisa.graphql_remote.executor import RemoteAnswer
from tests.unit.test_materialization_api import _statement_hot

pytestmark = pytest.mark.unit

# The Schedule type as the endpoint's schema reads it: ``employee`` points at one Employee.
_INTROSPECTED = {
    "name": "schedules",
    "sql_name": "schedules",
    "field_name": "schedules",
    "gql_type_name": "Schedule",
    "columns": [
        {"name": "id", "type": "integer"},
        {"name": "employee", "type": "jsonb", "gql_object_type": "Employee", "gql_is_list": False},
        {"name": "dayOfWeek", "type": "text"},
    ],
}

# The registered schedules table: its employee id is read through the employee object.
_REGISTERED = {
    "table_name": "schedules",
    "source_id": "graphql-demo",
    "cache_ttl": 300,
    "columns": [
        {"column_name": "id", "data_type": "integer"},
        {
            "column_name": "employee_id",
            "data_type": "integer",
            "gql_selection": "employee_id: employee { id }",
        },
        {"column_name": "day_of_week", "data_type": "varchar"},
    ],
}


def _state():
    return SimpleNamespace(
        graphql_remote_sources={
            "graphql-demo": {
                "source_id": "graphql-demo",
                "tables": [
                    _INTROSPECTED,
                    {"sql_name": "employees", "gql_type_name": "Employee", "columns": []},
                ],
            }
        },
        org_id="default",
        federation_engine=SimpleNamespace(
            materialize_store_dsn=lambda: "sqlite:///x.db", cache_catalog=lambda: "cat"
        ),
        config=SimpleNamespace(graphql_remote=SimpleNamespace(max_list_items=100, max_rows=10000)),
    )


async def test_the_relation_holds_a_registered_column_the_endpoint_names_another_way():
    asked: list[list[str]] = []

    async def remote(url, auth, field_name, columns, *args, **kwargs):
        asked.append(list(columns))
        return RemoteAnswer([{"id": 1, "employee_id": 7, "day_of_week": "Monday"}])

    inline: dict = {}
    with patch("provisa.graphql_remote.executor.execute_remote", new=remote):
        await _mat_gql_remote_table(
            "schedules",
            {"source_id": "graphql-demo", "url": "https://example.test/graphql"},
            _INTROSPECTED,
            _state(),
            _statement_hot(),
            500,
            {},
            inline,
            registration=_REGISTERED,
        )
    assert "employee_id: employee { id }" in asked[0]
    assert "employee_id" in inline["schedules"].column_names
    assert inline["schedules"].rows == [{"id": 1, "employee_id": 7, "day_of_week": "Monday"}]


_CONFIGS = ("config/provisa.yaml", "config/provisa-install.yaml", "demo/named/perf/config.yaml")


def _demo_type_fields(root_field: str) -> set[str] | None:
    """The fields of the type the demo GraphQL server's root field ``root_field`` answers."""
    from demo.graphql_server import server as demo

    field = demo.schema._schema.query_type.fields.get(root_field)
    if field is None:
        return None
    of = field.type
    while hasattr(of, "of_type"):
        of = of.of_type
    return set(of.fields)


@pytest.mark.parametrize("path", _CONFIGS)
def test_every_registered_graphql_demo_column_is_a_field_or_says_how_it_is_read(path):
    """A column a config registers for the graphql-demo source is a field of its type on the demo
    server (by its GraphQL name), or carries the ``gql_selection`` that reads it: a column that is
    neither is in no relation, and every read of it fails. config/provisa.yaml registered
    ``schedules.employee_id`` with no selection while ``Schedule`` holds only ``employee``."""
    from pathlib import Path

    import yaml

    from provisa.compiler.naming import apply_gql_name

    cfg = yaml.safe_load(Path(path).read_text())
    checked = 0
    for table in cfg["tables"]:
        if table.get("source_id") != "graphql-demo":
            continue
        fields = _demo_type_fields(apply_gql_name(table["table"]))
        assert fields is not None, f"{path}: {table['table']} is no root field of the demo server"
        for column in table["columns"]:
            if column.get("native_filter_type") or column.get("gql_selection"):
                continue
            checked += 1
            assert apply_gql_name(column["name"]) in fields, (
                f"{path}: {table['table']}.{column['name']} is no field of the demo server's "
                f"type and has no gql_selection: {sorted(fields)}"
            )
    assert checked, f"{path}: no graphql-demo column was checked"
