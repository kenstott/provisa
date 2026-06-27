# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""pytest-bdd step implementations for REQ-735 — Cassandra Connector."""

from __future__ import annotations

import os

import pytest
from pytest_bdd import given, parsers, scenario, then, when

from provisa.cassandra.source import (
    CQL_TYPE_TO_TRINO,
    discover_schema,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data() -> dict:
    """Shared state dictionary passed between Given/When/Then steps."""
    return {}


# ---------------------------------------------------------------------------
# Scenario binding
# ---------------------------------------------------------------------------


@scenario(
    "../../features/req_735.feature",
    "REQ-735 default behaviour",
)
def test_req_735_default_behaviour():
    pass


# ---------------------------------------------------------------------------
# Step definitions
# ---------------------------------------------------------------------------


@given("a Cassandra table with partition and clustering keys")
def given_cassandra_table_with_partition_and_clustering_keys(shared_data: dict):
    """Set up a realistic keyspace metadata dict that mirrors what Cassandra driver
    would return for a table with a mixed set of CQL column types, including
    collection types, and both partition and clustering key columns."""
    keyspace_metadata = {
        "columns": [
            {"name": "user_id", "type": "uuid"},
            {"name": "event_time", "type": "timestamp"},
            {"name": "session_id", "type": "timeuuid"},
            {"name": "username", "type": "text"},
            {"name": "score", "type": "bigint"},
            {"name": "active", "type": "boolean"},
            {"name": "rating", "type": "float"},
            {"name": "tags", "type": "list<text>"},
            {"name": "properties", "type": "map<text,text>"},
            {"name": "visited", "type": "set<text>"},
            {"name": "age", "type": "int"},
            {"name": "ip_address", "type": "inet"},
        ],
        "partition_keys": ["user_id"],
        "clustering_keys": ["event_time", "session_id"],
    }
    shared_data["keyspace_metadata"] = keyspace_metadata


@when("the adapter discovers the schema")
def when_adapter_discovers_schema(shared_data: dict):
    """Invoke discover_schema with the prepared keyspace metadata."""
    keyspace_metadata = shared_data["keyspace_metadata"]
    columns = discover_schema(keyspace_metadata)
    shared_data["columns"] = columns


@then("CQL column types are mapped to Trino types and key columns are annotated")
def then_cql_types_mapped_and_keys_annotated(shared_data: dict):
    """Assert that every column has the correct Trino type, that collection types
    are normalised to VARCHAR, and that partition / clustering key flags are set."""
    columns: list[dict] = shared_data["columns"]
    keyspace_metadata: dict = shared_data["keyspace_metadata"]

    # Build lookup by column name for easy assertion
    col_by_name = {c["name"]: c for c in columns}

    # ------------------------------------------------------------------ #
    # Verify all input columns are present in the output
    # ------------------------------------------------------------------ #
    for raw_col in keyspace_metadata["columns"]:
        assert raw_col["name"] in col_by_name, (
            f"Column '{raw_col['name']}' missing from discover_schema output"
        )

    # ------------------------------------------------------------------ #
    # Scalar type mappings
    # ------------------------------------------------------------------ #
    expected_trino_types = {
        "user_id": "UUID",
        "event_time": "TIMESTAMP",
        "session_id": "UUID",
        "username": "VARCHAR",
        "score": "BIGINT",
        "active": "BOOLEAN",
        "rating": "REAL",
        "age": "INTEGER",
        "ip_address": "VARCHAR",
    }
    for col_name, expected_type in expected_trino_types.items():
        actual_type = col_by_name[col_name]["type"]
        assert actual_type == expected_type, (
            f"Column '{col_name}': expected Trino type '{expected_type}', got '{actual_type}'"
        )

    # ------------------------------------------------------------------ #
    # Collection types must be normalised to VARCHAR
    # ------------------------------------------------------------------ #
    for col_name in ("tags", "properties", "visited"):
        actual_type = col_by_name[col_name]["type"]
        assert actual_type == "VARCHAR", (
            f"Collection column '{col_name}': expected 'VARCHAR', got '{actual_type}'"
        )

    # ------------------------------------------------------------------ #
    # Partition key annotation
    # ------------------------------------------------------------------ #
    partition_keys = set(keyspace_metadata["partition_keys"])
    for col_name in partition_keys:
        col_def = col_by_name[col_name]
        assert col_def.get("partitionKey") is True, (
            f"Column '{col_name}' should have partitionKey=True"
        )

    # ------------------------------------------------------------------ #
    # Clustering key annotation
    # ------------------------------------------------------------------ #
    clustering_keys = set(keyspace_metadata["clustering_keys"])
    for col_name in clustering_keys:
        col_def = col_by_name[col_name]
        assert col_def.get("clusteringKey") is True, (
            f"Column '{col_name}' should have clusteringKey=True"
        )

    # ------------------------------------------------------------------ #
    # Non-key columns must NOT carry partition / clustering key flags
    # ------------------------------------------------------------------ #
    all_key_columns = partition_keys | clustering_keys
    for col in columns:
        if col["name"] not in all_key_columns:
            assert "partitionKey" not in col, (
                f"Column '{col['name']}' is not a partition key but has partitionKey flag"
            )
            assert "clusteringKey" not in col, (
                f"Column '{col['name']}' is not a clustering key but has clusteringKey flag"
            )

    # ------------------------------------------------------------------ #
    # cqlType is preserved on each column
    # ------------------------------------------------------------------ #
    raw_types = {c["name"]: c["type"] for c in keyspace_metadata["columns"]}
    for col in columns:
        assert "cqlType" in col, f"Column '{col['name']}' is missing 'cqlType' field"
        assert col["cqlType"] == raw_types[col["name"]], (
            f"Column '{col['name']}' cqlType mismatch: "
            f"expected '{raw_types[col['name']]}', got '{col['cqlType']}'"
        )

    # ------------------------------------------------------------------ #
    # Sanity-check the global CQL_TYPE_TO_TRINO mapping for key entries
    # ------------------------------------------------------------------ #
    assert CQL_TYPE_TO_TRINO["text"] == "VARCHAR"
    assert CQL_TYPE_TO_TRINO["bigint"] == "BIGINT"
    assert CQL_TYPE_TO_TRINO["timestamp"] == "TIMESTAMP"
    assert CQL_TYPE_TO_TRINO["uuid"] == "UUID"
    assert CQL_TYPE_TO_TRINO["list"] == "VARCHAR"
    assert CQL_TYPE_TO_TRINO["map"] == "VARCHAR"
    assert CQL_TYPE_TO_TRINO["set"] == "VARCHAR"
