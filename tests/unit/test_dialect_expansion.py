# Copyright (c) 2026 Kenneth Stott
# Canary: e3b7a912-d05c-4f8a-9b3e-7c1d5e2f0a84
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for direct-route dialect expansion (Phase AA1, REQ-229).

Verifies that all expanded source types have correct SQLGlot dialect mappings
and that transpile() produces valid output for each.
"""

import pytest

from provisa.core.models import SOURCE_TO_DIALECT, SourceType
from provisa.transpiler.transpile import transpile

# The eight new dialects added in Phase AA1 (REQ-229)
_NEW_DIALECTS = {
    "clickhouse",
    "mariadb",
    "redshift",
    "databricks",
    "hive",
    "druid",
    "exasol",
    "singlestore",
}

_SIMPLE_SELECT = 'SELECT "id", "amount" FROM "public"."orders"'
_WHERE_SELECT = 'SELECT "id" FROM "public"."orders" WHERE "region" = \'us-east\''
_LIMIT_SELECT = 'SELECT "id" FROM "public"."orders" LIMIT 100 OFFSET 0'


class TestSourceToDialectMapping:
    def test_all_new_source_types_have_dialect(self):
        """Every Phase AA1 source type must be in SOURCE_TO_DIALECT."""
        for src_type in _NEW_DIALECTS:
            assert src_type in SOURCE_TO_DIALECT, (
                f"Source type {src_type!r} missing from SOURCE_TO_DIALECT"
            )

    def test_clickhouse_dialect(self):
        assert SOURCE_TO_DIALECT["clickhouse"] == "clickhouse"

    def test_mariadb_maps_to_mysql_dialect(self):
        # MariaDB is MySQL wire-compatible; SQLGlot uses mysql dialect
        assert SOURCE_TO_DIALECT["mariadb"] == "mysql"

    def test_redshift_dialect(self):
        assert SOURCE_TO_DIALECT["redshift"] == "redshift"

    def test_databricks_dialect(self):
        assert SOURCE_TO_DIALECT["databricks"] == "databricks"

    def test_hive_dialect(self):
        assert SOURCE_TO_DIALECT["hive"] == "hive"

    def test_druid_dialect(self):
        assert SOURCE_TO_DIALECT["druid"] == "druid"

    def test_exasol_dialect(self):
        assert SOURCE_TO_DIALECT["exasol"] == "exasol"

    def test_singlestore_dialect(self):
        assert SOURCE_TO_DIALECT["singlestore"] in ("singlestore", "mysql")

    def test_existing_dialects_unchanged(self):
        """Phase AA1 must not break existing dialect mappings."""
        assert SOURCE_TO_DIALECT["postgresql"] == "postgres"
        assert SOURCE_TO_DIALECT["mysql"] == "mysql"
        assert SOURCE_TO_DIALECT["snowflake"] == "snowflake"
        assert SOURCE_TO_DIALECT["bigquery"] == "bigquery"
        assert SOURCE_TO_DIALECT["duckdb"] == "duckdb"

    def test_source_type_enum_covers_new_types(self):
        """SourceType enum must include all new dialect source types."""
        enum_values = {m.value for m in SourceType}
        for src in _NEW_DIALECTS:
            assert src in enum_values, f"SourceType enum missing {src!r}"


class TestTranspileClickhouse:
    def test_simple_select(self):
        sql = transpile(_SIMPLE_SELECT, "clickhouse")
        assert "SELECT" in sql
        assert "orders" in sql

    def test_where_clause(self):
        sql = transpile(_WHERE_SELECT, "clickhouse")
        assert "WHERE" in sql
        assert "region" in sql.lower() or "region" in sql

    def test_limit_offset(self):
        sql = transpile(_LIMIT_SELECT, "clickhouse")
        assert "LIMIT" in sql


class TestTranspileMariaDB:
    def test_simple_select(self):
        sql = transpile(_SIMPLE_SELECT, SOURCE_TO_DIALECT["mariadb"])
        assert "SELECT" in sql
        assert "orders" in sql

    def test_where_clause(self):
        sql = transpile(_WHERE_SELECT, SOURCE_TO_DIALECT["mariadb"])
        assert "WHERE" in sql


class TestTranspileRedshift:
    def test_simple_select(self):
        sql = transpile(_SIMPLE_SELECT, "redshift")
        assert "SELECT" in sql

    def test_limit_offset(self):
        sql = transpile(_LIMIT_SELECT, "redshift")
        assert "LIMIT" in sql


class TestTranspileDatabricks:
    def test_simple_select(self):
        sql = transpile(_SIMPLE_SELECT, "databricks")
        assert "SELECT" in sql
        assert "orders" in sql

    def test_where_clause(self):
        sql = transpile(_WHERE_SELECT, "databricks")
        assert "WHERE" in sql


class TestTranspileHive:
    def test_simple_select(self):
        sql = transpile(_SIMPLE_SELECT, "hive")
        assert "SELECT" in sql

    def test_limit(self):
        sql = transpile(_LIMIT_SELECT, "hive")
        assert "LIMIT" in sql


class TestTranspileDruid:
    def test_simple_select(self):
        sql = transpile(_SIMPLE_SELECT, "druid")
        assert "SELECT" in sql

    def test_where_clause(self):
        sql = transpile(_WHERE_SELECT, "druid")
        assert "WHERE" in sql


class TestTranspileExasol:
    def test_simple_select(self):
        sql = transpile(_SIMPLE_SELECT, "exasol")
        assert "SELECT" in sql

    def test_where_clause(self):
        sql = transpile(_WHERE_SELECT, "exasol")
        assert "WHERE" in sql


class TestTranspileSinglestore:
    def test_simple_select(self):
        dialect = SOURCE_TO_DIALECT["singlestore"]
        sql = transpile(_SIMPLE_SELECT, dialect)
        assert "SELECT" in sql

    def test_where_clause(self):
        dialect = SOURCE_TO_DIALECT["singlestore"]
        sql = transpile(_WHERE_SELECT, dialect)
        assert "WHERE" in sql


class TestTranspileAllNewDialects:
    """Parametric smoke test — each new dialect must produce non-empty output."""

    @pytest.mark.parametrize("source_type", sorted(_NEW_DIALECTS))
    def test_select_produces_output(self, source_type):
        dialect = SOURCE_TO_DIALECT[source_type]
        sql = transpile(_SIMPLE_SELECT, dialect)
        assert sql.strip(), f"transpile produced empty output for {source_type!r}"

    @pytest.mark.parametrize("source_type", sorted(_NEW_DIALECTS))
    def test_where_produces_output(self, source_type):
        dialect = SOURCE_TO_DIALECT[source_type]
        sql = transpile(_WHERE_SELECT, dialect)
        assert "WHERE" in sql

    @pytest.mark.parametrize("source_type", sorted(_NEW_DIALECTS))
    def test_limit_produces_output(self, source_type):
        dialect = SOURCE_TO_DIALECT[source_type]
        sql = transpile(_LIMIT_SELECT, dialect)
        assert "LIMIT" in sql
