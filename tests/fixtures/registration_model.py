# Copyright (c) 2025 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Factory functions for building registration model test fixtures."""


def make_source(id: str = "sales-pg", type: str = "postgresql", **kwargs) -> dict:
    return {
        "id": id,
        "type": type,
        "host": kwargs.get("host", "localhost"),
        "port": kwargs.get("port", 5432),
        "database": kwargs.get("database", "provisa"),
        "username": kwargs.get("username", "provisa"),
        "password": kwargs.get("password", "provisa"),
        **kwargs,
    }


def make_table(
    source_id: str = "sales-pg",
    domain_id: str = "sales-analytics",
    schema: str = "public",
    table: str = "orders",
    governance: str = "pre-approved",
    columns: list[dict] | None = None,
) -> dict:
    return {
        "source_id": source_id,
        "domain_id": domain_id,
        "schema": schema,
        "table": table,
        "governance": governance,
        "columns": columns or [
            {"name": "id", "visible_to": ["admin", "analyst"]},
            {"name": "customer_id", "visible_to": ["admin", "analyst"]},
            {"name": "amount", "visible_to": ["admin"]},
        ],
    }


def make_relationship(
    id: str = "orders-to-customers",
    source_table_id: str = "orders",
    target_table_id: str = "customers",
    source_column: str = "customer_id",
    target_column: str = "id",
    cardinality: str = "many-to-one",
) -> dict:
    return {
        "id": id,
        "source_table_id": source_table_id,
        "target_table_id": target_table_id,
        "source_column": source_column,
        "target_column": target_column,
        "cardinality": cardinality,
    }


def make_role(
    id: str = "analyst",
    capabilities: list[str] | None = None,
    domain_access: list[str] | None = None,
) -> dict:
    return {
        "id": id,
        "capabilities": capabilities or ["query_development"],
        "domain_access": domain_access or ["sales-analytics"],
    }
