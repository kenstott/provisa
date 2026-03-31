# Copyright (c) 2025 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for Trino dynamic catalog creation/deletion."""

import os

import pytest

from provisa.core.catalog import catalog_exists, create_catalog, drop_catalog
from provisa.core.models import Source

pytestmark = pytest.mark.integration


@pytest.fixture
def test_source() -> Source:
    return Source(
        id="test-dynamic",
        type="postgresql",
        host=os.environ.get("TRINO_PG_HOST", "postgres"),  # Docker network hostname
        port=5432,
        database=os.environ.get("PG_DATABASE", "provisa"),
        username=os.environ.get("PG_USER", "provisa"),
        password="${env:PG_PASSWORD}",
    )


@pytest.fixture(autouse=True)
def _cleanup_catalog(trino_conn, test_source):
    """Ensure dynamic catalog is removed after test."""
    yield
    try:
        drop_catalog(trino_conn, test_source.id)
    except Exception:
        pass


class TestCatalogManagement:
    def test_create_catalog(self, trino_conn, test_source):
        password = os.environ.get("PG_PASSWORD", "provisa")
        create_catalog(trino_conn, test_source, password)
        assert catalog_exists(trino_conn, test_source.id)

    def test_created_catalog_can_query(self, trino_conn, test_source):
        password = os.environ.get("PG_PASSWORD", "provisa")
        create_catalog(trino_conn, test_source, password)

        catalog_name = test_source.catalog_name
        cur = trino_conn.cursor()
        cur.execute(f'SELECT COUNT(*) FROM "{catalog_name}".public.customers')
        result = cur.fetchone()
        assert result[0] == 20

    def test_created_catalog_information_schema(self, trino_conn, test_source):
        password = os.environ.get("PG_PASSWORD", "provisa")
        create_catalog(trino_conn, test_source, password)

        catalog_name = test_source.catalog_name
        cur = trino_conn.cursor()
        cur.execute(
            f"""
            SELECT column_name FROM "{catalog_name}".information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'orders'
            """
        )
        columns = [r[0] for r in cur.fetchall()]
        assert "id" in columns
        assert "customer_id" in columns

    def test_drop_catalog(self, trino_conn, test_source):
        password = os.environ.get("PG_PASSWORD", "provisa")
        create_catalog(trino_conn, test_source, password)
        assert catalog_exists(trino_conn, test_source.id)
        drop_catalog(trino_conn, test_source.id)
        assert not catalog_exists(trino_conn, test_source.id)

    def test_create_catalog_idempotent(self, trino_conn, test_source):
        password = os.environ.get("PG_PASSWORD", "provisa")
        create_catalog(trino_conn, test_source, password)
        create_catalog(trino_conn, test_source, password)
        assert catalog_exists(trino_conn, test_source.id)

    def test_drop_nonexistent_catalog(self, trino_conn):
        """DROP IF NOT EXISTS should not raise."""
        drop_catalog(trino_conn, "nonexistent-catalog")
