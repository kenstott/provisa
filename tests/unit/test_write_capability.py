# Copyright (c) 2026 Kenneth Stott
# Canary: 5b2c7e48-1d93-4f06-a8e5-3c9f0d7a2b61
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Which data writes a table's source can take, decided in one place (executor/write_capability.py)
and applied before any right is checked."""

from __future__ import annotations

import pytest
import sqlglot

from provisa.compiler.stage2 import GovernanceContext
from provisa.compiler.write_admission import WriteNotSupported, admit_rows, admit_write
from provisa.executor.write_capability import table_write_ops


@pytest.mark.parametrize(
    "source_type, offered",
    [
        ("postgresql", {"insert", "update", "delete"}),
        ("clickhouse", {"insert"}),
        ("neo4j", set()),
        ("mongodb", set()),
        ("graphql_remote", set()),
        ("openapi", set()),
        ("grpc_remote", set()),
        ("stripe", set()),
    ],
)
def test_a_source_type_offers_what_it_can_carry(source_type, offered):
    assert table_write_ops({"table_name": "t"}, source_type, None) == offered


def test_a_view_takes_no_writes():
    assert table_write_ops({"view_sql": "SELECT 1"}, "postgresql", None) == frozenset()
    assert table_write_ops({"table_name": "v"}, None, None) == frozenset()


def _gov(write_ops: set[str]) -> GovernanceContext:
    gov = GovernanceContext()
    gov.role_id = "writer"
    gov.can_write = True
    gov.table_map = {"orders": 1, "sales.orders": 1}
    gov.all_columns = {1: [("id", "integer"), ("region", "varchar")]}
    gov.writable_columns = {1: frozenset({"id", "region"})}
    gov.write_ops = {1: frozenset(write_ops)}
    return gov


@pytest.mark.parametrize(
    "sql, operation",
    [
        ("INSERT INTO sales.orders (id, region) VALUES (1, 'east')", "insert"),
        ("UPDATE sales.orders SET region = 'west' WHERE id = 1", "update"),
        ("DELETE FROM sales.orders WHERE id = 1", "delete"),
    ],
)
def test_a_table_whose_source_takes_no_writes_refuses_each_by_name(sql, operation):
    # e.g. a Neo4j-backed table: refused whatever the role holds, before any right is checked.
    with pytest.raises(WriteNotSupported) as refused:
        admit_write(sqlglot.parse_one(sql, read="postgres"), _gov(set()))
    assert (refused.value.table, refused.value.operation) == ("orders", operation)
    assert f"'orders' does not take {operation.upper()}" in str(refused.value)


def test_an_insert_only_table_refuses_update_and_admits_insert():
    gov = _gov({"insert"})
    admit_write(
        sqlglot.parse_one("INSERT INTO sales.orders (id, region) VALUES (1, 'e')", read="postgres"),
        gov,
    )
    with pytest.raises(WriteNotSupported, match="does not take UPDATE"):
        admit_write(sqlglot.parse_one("UPDATE sales.orders SET region = 'w'", read="postgres"), gov)


def test_the_refusal_comes_before_the_rights():
    gov = _gov(set())
    gov.can_write = False
    with pytest.raises(WriteNotSupported):
        admit_write(sqlglot.parse_one("DELETE FROM sales.orders", read="postgres"), gov)


def test_a_bulk_load_is_an_insert():
    with pytest.raises(WriteNotSupported, match="does not take INSERT"):
        admit_rows(_gov({"update", "delete"}), 1, "orders", ["id"])
    admit_rows(_gov({"insert"}), 1, "orders", ["id"])


def test_a_landed_replica_is_no_write_route_to_its_source():
    # The engine's land connector for a replicated type declares write support for its own store;
    # writing the replica would not write the source, so it is no route (executor/writable.py).
    from provisa.federation.engine import build_duckdb_engine

    engine = build_duckdb_engine().complete_reach()
    assert table_write_ops({"table_name": "t"}, "neo4j", engine) == frozenset()
    assert table_write_ops({"table_name": "t"}, "postgresql", engine) == {
        "insert",
        "update",
        "delete",
    }
