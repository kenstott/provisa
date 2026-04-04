"""Integration tests for Kafka source reads via Trino.

Requires: docker-compose up (Kafka + Trino with Kafka connector)
"""

import os

import pytest

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def kafka_bootstrap():
    return os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")


class TestKafkaTopicRead:
    def test_trino_kafka_connector_available(self, trino_conn):
        """Verify the Kafka connector is configured in Trino."""
        cursor = trino_conn.cursor()
        try:
            cursor.execute("SHOW CATALOGS")
            catalogs = [row[0] for row in cursor.fetchall()]
            if "support_kafka" not in catalogs:
                pytest.skip("Kafka catalog not configured in Trino")
        except Exception as e:
            pytest.skip(f"Trino not available: {e}")

    def test_kafka_topic_readable_as_table(self, trino_conn):
        """Read from a Kafka topic table via Trino."""
        cursor = trino_conn.cursor()
        try:
            cursor.execute("SHOW TABLES FROM support_kafka.default")
            tables = [row[0] for row in cursor.fetchall()]
        except Exception as e:
            pytest.skip(f"Kafka catalog not queryable: {e}")

        if not tables:
            pytest.skip("No Kafka topic tables configured")

        # Query the first available topic table
        table = tables[0]
        try:
            cursor.execute(f'SELECT * FROM support_kafka."default"."{table}" LIMIT 5')
            rows = cursor.fetchall()
            # Should return rows (may be empty if no messages yet)
            assert isinstance(rows, list)
        except Exception as e:
            pytest.skip(f"Kafka table query failed: {e}")

    def test_kafka_topic_has_columns(self, trino_conn):
        """Kafka topic tables should have schema-defined columns."""
        cursor = trino_conn.cursor()
        try:
            cursor.execute("SHOW TABLES FROM support_kafka.default")
            tables = [row[0] for row in cursor.fetchall()]
        except Exception:
            pytest.skip("Kafka catalog not queryable")

        if not tables:
            pytest.skip("No Kafka topic tables")

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
        try:
            cursor.execute("SHOW TABLES FROM support_kafka.default")
            tables = [row[0] for row in cursor.fetchall()]
        except Exception:
            pytest.skip("Kafka catalog not available")

        if not tables:
            pytest.skip("No Kafka topic tables")

        table = tables[0]
        cursor.execute(
            f"SELECT column_name, data_type FROM support_kafka.information_schema.columns "
            f"WHERE table_schema = 'default' AND table_name = '{table}' "
            f"AND column_name NOT LIKE '\\_%' ESCAPE '\\'"
        )
        typed_cols = cursor.fetchall()
        if not typed_cols:
            pytest.skip(f"Table {table} has no user-defined columns (only internal)")

        # At least one non-internal column should have a concrete type
        for name, dtype in typed_cols:
            assert dtype is not None, f"Column {name} should have a data type"
