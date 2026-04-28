# Copyright (c) 2026 Kenneth Stott
# Canary: e3f4a5b6-c7d8-9012-cdef-123456789012
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for core.catalog: Trino catalog management helpers."""

import pytest
from unittest.mock import MagicMock, call

from provisa.core.catalog import (
    _build_catalog_properties,
    _escape_sql_string,
    _to_catalog_name,
    _validate_identifier,
    analyze_source_tables,
    catalog_exists,
    create_catalog,
    drop_catalog,
)
from provisa.core.models import Source


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_source(id="pg1", type="postgresql", host="db.local", port=5432,
                 database="mydb", username="user", password="p", **kwargs):
    return Source(
        id=id,
        type=type,
        host=host,
        port=port,
        database=database,
        username=username,
        password=password,
        **kwargs,
    )


def _make_table(source_id="pg1", schema_name="public", table_name="orders"):
    """Return a minimal object duck-typing provisa.core.models.Table."""
    tbl = MagicMock()
    tbl.source_id = source_id
    tbl.schema_name = schema_name
    tbl.table_name = table_name
    return tbl


def _make_trino_conn(catalogs=None):
    """Return a mock Trino connection whose cursor executes expected queries."""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    if catalogs is not None:
        cursor.fetchall.return_value = [[c] for c in catalogs]
    else:
        cursor.fetchall.return_value = []
    return conn, cursor


# ---------------------------------------------------------------------------
# _validate_identifier
# ---------------------------------------------------------------------------


class TestValidateIdentifier:
    def test_valid_simple(self):
        assert _validate_identifier("pg1") == "pg1"

    def test_valid_underscores(self):
        assert _validate_identifier("sales_pg_prod") == "sales_pg_prod"

    def test_valid_mixed_case(self):
        assert _validate_identifier("MyConnector") == "MyConnector"

    def test_starts_with_number_rejected(self):
        with pytest.raises(ValueError, match="Invalid Trino identifier"):
            _validate_identifier("1bad")

    def test_hyphens_rejected(self):
        with pytest.raises(ValueError, match="Invalid Trino identifier"):
            _validate_identifier("bad-id")

    def test_spaces_rejected(self):
        with pytest.raises(ValueError, match="Invalid Trino identifier"):
            _validate_identifier("bad id")

    def test_empty_string_rejected(self):
        with pytest.raises(ValueError, match="Invalid Trino identifier"):
            _validate_identifier("")


# ---------------------------------------------------------------------------
# _escape_sql_string
# ---------------------------------------------------------------------------


class TestEscapeSqlString:
    def test_no_quotes_unchanged(self):
        assert _escape_sql_string("hunter2") == "hunter2"

    def test_single_quote_escaped(self):
        assert _escape_sql_string("it's") == "it''s"

    def test_multiple_quotes(self):
        assert _escape_sql_string("a'b'c") == "a''b''c"

    def test_empty_string(self):
        assert _escape_sql_string("") == ""


# ---------------------------------------------------------------------------
# _to_catalog_name
# ---------------------------------------------------------------------------


class TestToCatalogName:
    def test_plain_id_unchanged(self):
        assert _to_catalog_name("pg1") == "pg1"

    def test_hyphens_replaced(self):
        assert _to_catalog_name("sales-pg-prod") == "sales_pg_prod"

    def test_underscores_preserved(self):
        assert _to_catalog_name("sales_pg") == "sales_pg"

    def test_invalid_result_raises(self):
        # A name that, even after hyphen replacement, begins with a digit is invalid
        with pytest.raises(ValueError):
            _to_catalog_name("1numeric-start")


# ---------------------------------------------------------------------------
# _build_catalog_properties
# ---------------------------------------------------------------------------


class TestBuildCatalogProperties:
    def test_postgresql_jdbc_props(self):
        src = _make_source(id="pg1", type="postgresql")
        props = _build_catalog_properties(src, "mypassword")
        assert "connection-url" in props
        assert "jdbc:postgresql" in props["connection-url"]
        assert props["connection-user"] == "user"
        assert props["connection-password"] == "mypassword"

    def test_mysql_jdbc_props(self):
        src = _make_source(id="my1", type="mysql", port=3306)
        props = _build_catalog_properties(src, "pw")
        assert "jdbc:mysql" in props["connection-url"]

    def test_sqlserver_jdbc_props(self):
        src = _make_source(id="ss1", type="sqlserver", port=1433)
        props = _build_catalog_properties(src, "pw")
        assert "jdbc:sqlserver" in props["connection-url"]
        assert "databaseName=mydb" in props["connection-url"]

    def test_mongodb_props(self):
        src = _make_source(id="mg1", type="mongodb", port=27017)
        props = _build_catalog_properties(src, "pw")
        assert "mongodb.connection-url" in props
        assert "mongodb://" in props["mongodb.connection-url"]

    def test_mongodb_with_credentials(self):
        src = _make_source(id="mg1", type="mongodb", port=27017, username="admin")
        props = _build_catalog_properties(src, "secretpw")
        assert "admin:secretpw@" in props["mongodb.connection-url"]

    def test_mongodb_schema_collection(self):
        src = _make_source(id="mg1", type="mongodb", port=27017)
        props = _build_catalog_properties(src, "pw")
        assert props["mongodb.schema-collection"] == "_schema"

    def test_cassandra_props(self):
        src = _make_source(id="cs1", type="cassandra", port=9042)
        props = _build_catalog_properties(src, "pw")
        assert "cassandra.contact-points" in props
        assert props["cassandra.contact-points"] == "db.local"
        assert props["cassandra.native-protocol-port"] == "9042"

    def test_duckdb_returns_empty_props(self):
        src = _make_source(id="dk1", type="duckdb", port=0)
        props = _build_catalog_properties(src, "pw")
        # duckdb has no JDBC prefix → empty props
        assert props == {}

    def test_password_escaped_in_props(self):
        src = _make_source(id="pg1", type="postgresql")
        props = _build_catalog_properties(src, "p'word")
        # Password is stored as-is in the dict (escaping happens in SQL building)
        assert props["connection-password"] == "p'word"


# ---------------------------------------------------------------------------
# catalog_exists
# ---------------------------------------------------------------------------


class TestCatalogExists:
    def test_catalog_present(self):
        conn, cursor = _make_trino_conn(catalogs=["pg1", "tpch", "system"])
        assert catalog_exists(conn, "pg1") is True

    def test_catalog_absent(self):
        conn, cursor = _make_trino_conn(catalogs=["tpch", "system"])
        assert catalog_exists(conn, "pg1") is False

    def test_show_catalogs_called(self):
        conn, cursor = _make_trino_conn(catalogs=[])
        catalog_exists(conn, "pg1")
        cursor.execute.assert_called_once_with("SHOW CATALOGS")

    def test_hyphenated_source_id_normalised(self):
        """catalog_exists converts hyphens to underscores before comparing."""
        conn, cursor = _make_trino_conn(catalogs=["sales_pg", "system"])
        # The function normalises the name internally
        assert catalog_exists(conn, "sales_pg") is True


# ---------------------------------------------------------------------------
# create_catalog
# ---------------------------------------------------------------------------


class TestCreateCatalog:
    def test_skips_if_catalog_exists(self):
        src = _make_source(id="pg1", type="postgresql")
        conn, cursor = _make_trino_conn(catalogs=["pg1"])
        create_catalog(conn, src, "pw")
        # SHOW CATALOGS called (for existence check), but no CREATE CATALOG
        executed_sqls = [str(c) for c in cursor.execute.call_args_list]
        assert not any("CREATE CATALOG" in s for s in executed_sqls)

    def test_creates_catalog_when_absent(self):
        src = _make_source(id="pg1", type="postgresql")
        conn, cursor = _make_trino_conn(catalogs=[])  # catalog absent
        create_catalog(conn, src, "pw")
        executed_sqls = " ".join(str(c) for c in cursor.execute.call_args_list)
        assert "CREATE CATALOG" in executed_sqls

    def test_create_catalog_contains_connector(self):
        src = _make_source(id="pg1", type="postgresql")
        conn, cursor = _make_trino_conn(catalogs=[])
        create_catalog(conn, src, "pw")
        create_call = next(
            str(c) for c in cursor.execute.call_args_list if "CREATE CATALOG" in str(c)
        )
        assert "postgresql" in create_call

    def test_create_catalog_skips_empty_props_source(self):
        """DuckDB produces no Trino connector props — catalog creation skipped."""
        src = _make_source(id="dk1", type="duckdb", port=0)
        conn, cursor = _make_trino_conn(catalogs=[])
        create_catalog(conn, src, "pw")
        executed_sqls = " ".join(str(c) for c in cursor.execute.call_args_list)
        assert "CREATE CATALOG" not in executed_sqls

    def test_catalog_creation_failure_does_not_raise(self):
        """Exception during CREATE CATALOG must be swallowed (logged as warning)."""
        src = _make_source(id="pg1", type="postgresql")
        conn, cursor = _make_trino_conn(catalogs=[])
        cursor.execute.side_effect = [None, Exception("connection refused")]
        cursor.fetchall.side_effect = [[], Exception("connection refused")]
        # Should not raise
        create_catalog(conn, src, "pw")

    def test_sql_string_escaping_in_catalog_create(self):
        """Passwords with single quotes are escaped in the SQL."""
        src = _make_source(id="pg1", type="postgresql")
        conn, cursor = _make_trino_conn(catalogs=[])
        create_catalog(conn, src, "p'word")
        # Grab the actual SQL string from the CREATE CATALOG call
        create_sql = next(
            c.args[0]
            for c in cursor.execute.call_args_list
            if "CREATE CATALOG" in c.args[0]
        )
        # The escaped password appears as p''word in the raw SQL
        assert "p''word" in create_sql


# ---------------------------------------------------------------------------
# drop_catalog
# ---------------------------------------------------------------------------


class TestDropCatalog:
    def test_drop_executes_sql(self):
        conn, cursor = _make_trino_conn()
        drop_catalog(conn, "pg1")
        cursor.execute.assert_called_once_with("DROP CATALOG IF EXISTS pg1")

    def test_drop_normalises_hyphens(self):
        conn, cursor = _make_trino_conn()
        drop_catalog(conn, "sales_pg")
        executed = cursor.execute.call_args[0][0]
        assert "sales_pg" in executed

    def test_drop_calls_fetchall(self):
        conn, cursor = _make_trino_conn()
        drop_catalog(conn, "pg1")
        cursor.fetchall.assert_called_once()


# ---------------------------------------------------------------------------
# analyze_source_tables
# ---------------------------------------------------------------------------


class TestAnalyzeSourceTables:
    def test_analyze_called_for_matching_tables(self):
        src = _make_source(id="pg1", type="postgresql")
        tables = [
            _make_table(source_id="pg1", schema_name="public", table_name="orders"),
            _make_table(source_id="pg1", schema_name="public", table_name="customers"),
        ]
        conn, cursor = _make_trino_conn()
        analyze_source_tables(conn, src, tables)
        executed_sqls = " ".join(str(c) for c in cursor.execute.call_args_list)
        assert "ANALYZE pg1.public.orders" in executed_sqls
        assert "ANALYZE pg1.public.customers" in executed_sqls

    def test_analyze_skips_other_source_tables(self):
        src = _make_source(id="pg1", type="postgresql")
        tables = [
            _make_table(source_id="pg1", table_name="orders"),
            _make_table(source_id="other_src", table_name="items"),
        ]
        conn, cursor = _make_trino_conn()
        analyze_source_tables(conn, src, tables)
        executed_sqls = " ".join(str(c) for c in cursor.execute.call_args_list)
        assert "orders" in executed_sqls
        assert "items" not in executed_sqls

    def test_analyze_failure_does_not_raise(self):
        """ANALYZE failures must be swallowed — connector may not support it."""
        src = _make_source(id="pg1", type="postgresql")
        tables = [_make_table(source_id="pg1")]
        conn, cursor = _make_trino_conn()
        cursor.fetchall.side_effect = Exception("ANALYZE not supported")
        # Should not raise
        analyze_source_tables(conn, src, tables)

    def test_analyze_empty_table_list(self):
        src = _make_source(id="pg1", type="postgresql")
        conn, cursor = _make_trino_conn()
        analyze_source_tables(conn, src, [])
        cursor.execute.assert_not_called()

    def test_catalog_name_used_in_analyze_sql(self):
        src = _make_source(id="sales-pg", type="postgresql")
        tables = [_make_table(source_id="sales-pg", schema_name="public", table_name="orders")]
        conn, cursor = _make_trino_conn()
        analyze_source_tables(conn, src, tables)
        executed_sqls = " ".join(str(c) for c in cursor.execute.call_args_list)
        assert "sales_pg.public.orders" in executed_sqls
