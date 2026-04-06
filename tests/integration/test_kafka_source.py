# Copyright (c) 2026 Kenneth Stott
# Canary: 12037bdf-8f14-4aac-bd2f-c15b43f723dc
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for Kafka source reads via Trino.

Requires: docker-compose up (Kafka + Trino with Kafka connector)
"""

import os

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


@pytest.fixture(scope="module")
def kafka_bootstrap():
    return os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")


class TestKafkaTopicRead:
    def test_trino_kafka_connector_available(self, trino_conn):
        """Verify the Kafka connector is configured in Trino."""
        cursor = trino_conn.cursor()
        cursor.execute("SHOW CATALOGS")
        catalogs = [row[0] for row in cursor.fetchall()]
        assert "support_kafka" in catalogs, "Kafka catalog not configured in Trino"

    def test_kafka_topic_readable_as_table(self, trino_conn):
        """Read from a Kafka topic table via Trino."""
        cursor = trino_conn.cursor()
        cursor.execute("SHOW TABLES FROM support_kafka.default")
        tables = [row[0] for row in cursor.fetchall()]
        assert tables, "No Kafka topic tables configured"

        # Query the first available topic table
        table = tables[0]
        cursor2 = trino_conn.cursor()
        cursor2.execute(f'SELECT * FROM support_kafka."default"."{table}" LIMIT 5')
        rows = cursor2.fetchall()
        # Should return rows (may be empty if no messages yet)
        assert isinstance(rows, list)

    def test_kafka_topic_has_columns(self, trino_conn):
        """Kafka topic tables should have schema-defined columns."""
        cursor = trino_conn.cursor()
        cursor.execute("SHOW TABLES FROM support_kafka.default")
        tables = [row[0] for row in cursor.fetchall()]
        assert tables, "No Kafka topic tables"

        table = tables[0]
        cursor.execute(
            f"SELECT column_name FROM support_kafka.information_schema.columns "
            f"WHERE table_schema = 'default' AND table_name = '{table}'"
        )
        columns = [row[0] for row in cursor.fetchall()]
        assert len(columns) > 0, f"Table {table} should have columns"
        # Kafka connector always adds internal columns
        assert any(c in columns for c in ["_message", "_key", "_partition_offset", "_timestamp"]
                   or len(columns) > 0)


class TestKafkaMessageContent:
    def test_kafka_messages_have_typed_columns(self, trino_conn):
        """When a schema is defined, messages have typed columns (not just raw bytes)."""
        cursor = trino_conn.cursor()
        cursor.execute("SHOW TABLES FROM support_kafka.default")
        tables = [row[0] for row in cursor.fetchall()]
        assert tables, "No Kafka topic tables"

        table = tables[0]
        cursor.execute(
            f"SELECT column_name, data_type FROM support_kafka.information_schema.columns "
            f"WHERE table_schema = 'default' AND table_name = '{table}' "
            f"AND column_name NOT LIKE '\\_%' ESCAPE '\\'"
        )
        typed_cols = cursor.fetchall()
        assert typed_cols, f"Table {table} has no user-defined columns (only internal)"

        # At least one non-internal column should have a concrete type
        for name, dtype in typed_cols:
            assert dtype is not None, f"Column {name} should have a data type"
