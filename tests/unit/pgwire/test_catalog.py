# Copyright (c) 2026 Kenneth Stott
# Canary: e5f6a7b8-c9d0-1234-ef01-456789012345
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for pgwire catalog proxy (Phase 1)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from unittest.mock import MagicMock

import pytest

from provisa.pgwire.catalog import answer, classify


@dataclass
class _TableMeta:
    table_id: int
    field_name: str
    catalog_name: str
    schema_name: str
    table_name: str
    domain_id: str = ""
    source_id: str = ""
    type_name: str = ""
    source_type: str = ""
    original_table_name: str = ""
    display_name: str = ""
    column_presets: dict = field(default_factory=dict)


@dataclass
class _ColMeta:
    column_name: str
    data_type: str
    is_nullable: bool


def _make_state(tables: dict, col_types: dict) -> Any:
    ctx = MagicMock()
    ctx.tables = tables
    state = MagicMock()
    state.contexts = {"testrole": ctx}
    state.schema_build_cache = {"column_types": col_types}
    return state


class TestClassify:
    def test_set_intercepted(self):
        assert classify("SET search_path TO public") == "INTERCEPT"

    def test_set_case_insensitive(self):
        assert classify("set client_encoding = 'UTF8'") == "INTERCEPT"

    def test_show_intercepted(self):
        assert classify("SHOW server_version") == "INTERCEPT"

    def test_begin_intercepted(self):
        assert classify("BEGIN") == "INTERCEPT"

    def test_commit_intercepted(self):
        assert classify("COMMIT") == "INTERCEPT"

    def test_rollback_intercepted(self):
        assert classify("ROLLBACK") == "INTERCEPT"

    def test_information_schema_intercepted(self):
        assert classify("SELECT * FROM information_schema.tables") == "INTERCEPT"

    def test_pg_catalog_intercepted(self):
        assert classify("SELECT * FROM pg_catalog.pg_namespace") == "INTERCEPT"

    def test_plain_select_passthrough(self):
        assert classify("SELECT 1") == "PASS_THROUGH"

    def test_user_table_passthrough(self):
        assert classify("SELECT id FROM dogs") == "PASS_THROUGH"

    def test_current_setting_intercepted(self):
        assert classify("SELECT current_setting('server_version')") == "INTERCEPT"


class TestAnswerSetTxn:
    def _empty_state(self):
        return _make_state({}, {})

    def test_set_returns_empty_result(self):
        result = answer("SET search_path TO public", "testrole", self._empty_state())
        assert result.rows == []
        assert result.column_names == []

    def test_begin_returns_empty_result(self):
        result = answer("BEGIN", "testrole", self._empty_state())
        assert result.rows == []
        assert result.column_names == []

    def test_commit_returns_empty_result(self):
        result = answer("COMMIT", "testrole", self._empty_state())
        assert result.rows == []

    def test_rollback_returns_empty_result(self):
        result = answer("ROLLBACK", "testrole", self._empty_state())
        assert result.rows == []


class TestAnswerShow:
    def _empty_state(self):
        return _make_state({}, {})

    def test_show_server_version(self):
        result = answer("SHOW server_version", "testrole", self._empty_state())
        assert result.column_names == ["server_version"]
        assert len(result.rows) == 1
        assert "provisa" in result.rows[0][0]

    def test_show_all(self):
        result = answer("SHOW ALL", "testrole", self._empty_state())
        assert result.column_names == ["name", "setting"]
        assert len(result.rows) > 0


class TestAnswerInformationSchema:
    def _state_with_table(self):
        tm = _TableMeta(
            table_id=1,
            field_name="dogs",
            catalog_name="provisa",
            schema_name="public",
            table_name="dogs",
        )
        col_types = {
            1: [
                _ColMeta("id", "integer", False),
                _ColMeta("name", "varchar", True),
                _ColMeta("breed", "varchar", True),
            ]
        }
        return _make_state({"dogs": tm}, col_types)

    def test_is_tables_returns_row(self):
        state = self._state_with_table()
        result = answer("SELECT * FROM information_schema.tables", "testrole", state)
        names = [r[2] for r in result.rows]
        assert "dogs" in names

    def test_is_columns_returns_columns(self):
        state = self._state_with_table()
        result = answer("SELECT * FROM information_schema.columns", "testrole", state)
        col_names = [r[3] for r in result.rows]
        assert "id" in col_names
        assert "name" in col_names

    def test_is_schemata_returns_schema(self):
        state = self._state_with_table()
        result = answer("SELECT * FROM information_schema.schemata", "testrole", state)
        schemas = [r[1] for r in result.rows]
        assert "public" in schemas

    def test_is_columns_where_table(self):
        state = self._state_with_table()
        result = answer(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'dogs'",
            "testrole",
            state,
        )
        col_names = [r[0] for r in result.rows]
        assert "id" in col_names


class TestAnswerPgCatalog:
    def _state_with_table(self):
        tm = _TableMeta(
            table_id=2,
            field_name="cats",
            catalog_name="provisa",
            schema_name="public",
            table_name="cats",
        )
        col_types = {2: [_ColMeta("id", "bigint", False)]}
        return _make_state({"cats": tm}, col_types)

    def test_pg_namespace(self):
        state = self._state_with_table()
        result = answer("SELECT * FROM pg_catalog.pg_namespace", "testrole", state)
        ns_names = [r[1] for r in result.rows]
        assert "pg_catalog" in ns_names
        assert "public" in ns_names

    def test_pg_class(self):
        state = self._state_with_table()
        result = answer("SELECT * FROM pg_catalog.pg_class", "testrole", state)
        rel_names = [r[1] for r in result.rows]
        assert "cats" in rel_names

    def test_pg_attribute(self):
        state = self._state_with_table()
        result = answer("SELECT * FROM pg_catalog.pg_attribute", "testrole", state)
        att_names = [r[1] for r in result.rows]
        assert "id" in att_names

    def test_pg_type(self):
        state = self._state_with_table()
        result = answer("SELECT * FROM pg_catalog.pg_type", "testrole", state)
        assert len(result.rows) > 0
        type_names = [r[1] for r in result.rows]
        assert "int4" in type_names

    def test_pg_database(self):
        state = self._state_with_table()
        result = answer("SELECT * FROM pg_catalog.pg_database", "testrole", state)
        db_names = [r[1] for r in result.rows]
        assert "provisa" in db_names

    def test_pg_settings(self):
        state = self._state_with_table()
        result = answer("SELECT * FROM pg_catalog.pg_settings", "testrole", state)
        assert len(result.rows) > 0

    def test_pg_roles_contains_role(self):
        state = self._state_with_table()
        result = answer("SELECT * FROM pg_catalog.pg_roles", "testrole", state)
        rolnames = [r[1] for r in result.rows]
        assert "testrole" in rolnames

    def test_join_pg_class_pg_namespace(self):
        state = self._state_with_table()
        result = answer(
            """SELECT c.relname, n.nspname
               FROM pg_catalog.pg_class c
               JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname = 'public'""",
            "testrole",
            state,
        )
        rel_names = [r[0] for r in result.rows]
        assert "cats" in rel_names

    def test_pg_get_expr_rewritten_to_null(self):
        state = self._state_with_table()
        result = answer(
            "SELECT pg_get_expr(adbin, adrelid) FROM pg_catalog.pg_attrdef",
            "testrole",
            state,
        )
        assert result.rows == [] or all(r[0] is None for r in result.rows)

    def test_pg_constraint_column_types_include_integerarray(self):
        """answer() must return column_types for pg_constraint even when 0 rows.

        DBeaver's Statement Describe step runs the constraint query with a fake
        conrelid (no rows returned). ProvisaQueryResult must still send the
        correct OID (1007, int4[]) for conkey/confkey rather than OID 25 (text).
        This requires column_types to carry DuckDB's schema-level type info.
        """
        state = self._state_with_table()
        result = answer(
            "SELECT c.oid, c.* FROM pg_catalog.pg_constraint AS c WHERE c.conrelid = NULL",
            "testrole",
            state,
        )
        assert result.column_types is not None, "column_types must be populated from DuckDB cursor"
        col_idx = {n: i for i, n in enumerate(result.column_names)}
        assert "conkey" in col_idx, f"conkey missing from columns: {result.column_names}"
        assert "confkey" in col_idx, f"confkey missing from columns: {result.column_names}"
        assert result.column_types[col_idx["conkey"]] == "INTEGER[]", (
            f"conkey must be INTEGER[], got {result.column_types[col_idx['conkey']]}"
        )
        assert result.column_types[col_idx["confkey"]] == "INTEGER[]", (
            f"confkey must be INTEGER[], got {result.column_types[col_idx['confkey']]}"
        )

    def test_provisaqueryresult_uses_column_types_for_empty_rows(self):
        """ProvisaQueryResult must emit INTEGERARRAY for conkey even with 0 rows."""
        from buenavista.core import BVType
        from provisa.pgwire.server import ProvisaQueryResult

        state = self._state_with_table()
        result = answer(
            "SELECT c.oid, c.* FROM pg_catalog.pg_constraint AS c WHERE c.conrelid = NULL",
            "testrole",
            state,
        )
        assert result.rows == [], "0 rows expected for non-existent conrelid"
        qr = ProvisaQueryResult(result)
        col_idx = {result.column_names[i]: i for i in range(len(result.column_names))}
        assert "conkey" in col_idx, f"conkey missing: {result.column_names}"
        _, bvtype = qr.column(col_idx["conkey"])
        assert bvtype == BVType.INTEGERARRAY, f"Expected INTEGERARRAY for conkey, got {bvtype}"


def _make_pet_store_state() -> Any:
    """Multi-table, multi-schema context mirroring pet_store: meta.pets FK→ meta.registered_tables."""
    from provisa.compiler.sql_gen import TableMeta, JoinMeta as _JoinMeta

    pets = MagicMock(spec=TableMeta)
    pets.table_id = 10
    pets.field_name = "meta__pets"
    pets.catalog_name = "provisa"
    pets.schema_name = "meta"
    pets.table_name = "pets"
    pets.domain_id = "meta"
    pets.type_name = "Pet"
    pets.display_name = "pets"

    reg = MagicMock(spec=TableMeta)
    reg.table_id = 11
    reg.field_name = "meta__registeredTables"
    reg.catalog_name = "provisa"
    reg.schema_name = "meta"
    reg.table_name = "registered_tables"
    reg.domain_id = "meta"
    reg.type_name = "RegisteredTable"
    reg.display_name = "registeredTables"

    col_types = {
        10: [
            _ColMeta("id", "integer", False),
            _ColMeta("name", "varchar", True),
            _ColMeta("species", "varchar", True),
            _ColMeta("registered_table_id", "integer", True),
        ],
        11: [
            _ColMeta("id", "integer", False),
            _ColMeta("name", "varchar", True),
        ],
    }
    fk = MagicMock(spec=_JoinMeta)
    fk.source_column = "registered_table_id"
    fk.target_column = "id"
    fk.source_column_type = "integer"
    fk.target_column_type = "integer"
    fk.target = reg
    fk.cardinality = "many-to-one"
    fk.source_constant = None
    fk.source_expr = None

    ctx = MagicMock()
    ctx.tables = {"pets": pets, "registered_tables": reg}
    ctx.joins = {("Pet", "registered_table"): fk}
    ctx.pk_columns = {10: ["id"], 11: ["id"]}
    state = MagicMock()
    state.contexts = {"testrole": ctx}
    state.schema_build_cache = {"column_types": col_types, "tables": []}
    return state


class TestDBeaverERDiagram:
    """Simulate the exact queries DBeaver fires to render an ER diagram.

    DBeaver flow:
    1. pg_namespace → resolve 'meta' → ns_oid
    2. pg_class WHERE relnamespace=ns_oid → table OIDs
    3. pg_attribute JOIN pg_class WHERE relnamespace=ns_oid → columns per table
    4. pg_constraint WHERE conrelid=pets_oid → FK rows with conkey/confkey
    """

    @pytest.fixture
    def state(self):
        return _make_pet_store_state()

    def _pets_oid(self, state):
        result = answer("SELECT oid, relname FROM pg_catalog.pg_class", "testrole", state)
        return next(r[0] for r in result.rows if r[1] == "pets")

    def _meta_ns_oid(self, state):
        result = answer("SELECT oid, nspname FROM pg_catalog.pg_namespace", "testrole", state)
        return next(r[0] for r in result.rows if r[1] == "meta")

    def test_meta_schema_in_pg_namespace(self, state):
        result = answer("SELECT oid, nspname FROM pg_catalog.pg_namespace", "testrole", state)
        names = [r[1] for r in result.rows]
        assert "meta" in names, f"meta missing from pg_namespace: {names}"

    def test_pg_class_returns_snake_case_names(self, state):
        result = answer("SELECT relname FROM pg_catalog.pg_class", "testrole", state)
        names = [r[0] for r in result.rows]
        assert "pets" in names
        assert "registered_tables" in names
        assert "registeredTables" not in names, "camelCase leaked into pg_class.relname"

    def test_pg_attribute_returns_columns_for_pets(self, state):
        pets_oid = self._pets_oid(state)
        result = answer(
            f"SELECT attname, attnum FROM pg_catalog.pg_attribute"
            f" WHERE attrelid={pets_oid} AND attnum>0 AND attisdropped=false",
            "testrole", state,
        )
        col_names = [r[0] for r in result.rows]
        assert "id" in col_names, f"id missing: {col_names}"
        assert "name" in col_names, f"name missing: {col_names}"
        assert "registered_table_id" in col_names, f"registered_table_id missing: {col_names}"
        assert len(col_names) == 4, f"expected 4 columns, got {col_names}"

    def test_bulk_er_diagram_query_returns_columns(self, state):
        """DBeaver bulk query: pg_attribute JOIN pg_class WHERE relnamespace=meta_oid."""
        meta_oid = self._meta_ns_oid(state)
        result = answer(
            f"""SELECT c.oid, a.attname, a.atttypid, a.attnum
                FROM pg_catalog.pg_attribute a, pg_catalog.pg_class c
                WHERE a.attrelid=c.oid AND c.relnamespace={meta_oid}
                AND a.attnum>0 AND NOT a.attisdropped
                ORDER BY c.oid, a.attnum""",
            "testrole", state,
        )
        assert len(result.rows) == 6, f"expected 6 cols (4 pets + 2 reg), got {len(result.rows)}: {result.rows}"
        col_names = [r[1] for r in result.rows]
        assert "id" in col_names
        assert "registered_table_id" in col_names

    def test_pg_constraint_returns_fk_for_pets(self, state):
        pets_oid = self._pets_oid(state)
        result = answer(
            f"SELECT oid, conname, contype, conrelid, confrelid, conkey, confkey"
            f" FROM pg_catalog.pg_constraint WHERE conrelid={pets_oid}",
            "testrole", state,
        )
        fk_rows = [r for r in result.rows if r[2] == "f"]
        assert len(fk_rows) >= 1, f"no FK rows for pets (oid={pets_oid}): {result.rows}"
        fk = fk_rows[0]
        assert fk[4] is not None, "confrelid (FK target OID) must be set"
        assert fk[5] is not None and len(fk[5]) > 0, f"conkey must be non-empty: {fk[5]}"
        assert fk[6] is not None and len(fk[6]) > 0, f"confkey must be non-empty: {fk[6]}"
        assert fk[5][0] > 0, f"conkey attnum must be positive, got {fk[5]}"
        assert fk[6][0] > 0, f"confkey attnum must be positive, got {fk[6]}"

    def test_fk_conkey_matches_registered_table_id_attnum(self, state):
        """conkey must point to registered_table_id's attnum (4th col → 4)."""
        pets_oid = self._pets_oid(state)
        attr_result = answer(
            f"SELECT attname, attnum FROM pg_catalog.pg_attribute"
            f" WHERE attrelid={pets_oid} AND attnum>0 ORDER BY attnum",
            "testrole", state,
        )
        attnum_map = {r[0]: r[1] for r in attr_result.rows}
        expected_attnum = attnum_map["registered_table_id"]

        con_result = answer(
            f"SELECT conkey FROM pg_catalog.pg_constraint WHERE conrelid={pets_oid} AND contype='f'",
            "testrole", state,
        )
        assert len(con_result.rows) >= 1
        conkey = con_result.rows[0][0]
        assert conkey[0] == expected_attnum, (
            f"conkey[0]={conkey[0]} must equal registered_table_id attnum={expected_attnum}"
        )
