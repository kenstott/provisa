# Copyright (c) 2026 Kenneth Stott
# Canary: 9e4b7a26-1d3f-4c85-b6e2-8a0f5d73c1e9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1653 / REQ-1652: the Snowflake-native landing terminal -- replica DDL/DML, the per-source
view, and the keys the landed model carries -- driven through a scripted DBAPI cursor."""

from __future__ import annotations

import pytest
from typing import Any

from provisa.federation import snowflake_store as store
from provisa.federation.landed_keys import ForeignKeyEdge

pytestmark = pytest.mark.unit

_LANDING = "_landing"
_PETS = (_LANDING, "mat", "pet-store-sqlite__pet_store__pets")
_VISITS = (_LANDING, "mat", "pet-store-sqlite__pet_store__pet_visits")
_PETS_VIEW = ("pet_store_sqlite", "pet_store", "pets")
_VISITS_VIEW = ("pet_store_sqlite", "pet_store", "pet_visits")
_COLS = [("id", "integer"), ("name", "text"), ("meta", "json")]


class _Cursor:
    """Scripted cursor: ``answers`` maps a SQL prefix to the rows (and description) it returns."""

    def __init__(self, answers: dict[str, tuple[list[tuple], list[str]]] | None = None):
        self.sql: list[str] = []
        self.params: list[Any] = []
        self.many: list[tuple[str, list]] = []
        self.description: list[tuple[str, ...]] = []
        self._rows: list[tuple] = []
        self._answers = answers or {}

    def execute(self, sql, params=None):
        self.sql.append(sql)
        self.params.append(params)
        self._rows, cols = [], []
        for prefix, (rows, columns) in self._answers.items():
            if sql.startswith(prefix) or prefix in sql:
                self._rows, cols = rows, columns
                break
        self.description = [(c,) for c in cols]

    def executemany(self, sql, seq):
        self.many.append((sql, list(seq)))

    def fetchall(self):
        return list(self._rows)

    def close(self):
        pass


def test_ddl_type_maps_ir_and_raises_on_unknown():
    assert store.ddl_type("bigint") == "NUMBER(38,0)"
    assert store.ddl_type("json") == "VARIANT"
    with pytest.raises(ValueError):
        store.ddl_type("geometry_of_doom")


def test_create_ddl_carries_the_declared_primary_key():
    ddl = store.create_ddl(_PETS, _COLS, ("id",))
    assert ddl.startswith(
        'CREATE TABLE IF NOT EXISTS "_landing"."mat"."pet-store-sqlite__pet_store__pets" ('
    )
    assert '"id" NUMBER(38,0), "name" VARCHAR, "meta" VARIANT' in ddl
    assert 'CONSTRAINT "provisa_pk_pet_store_sqlite__pet_store__pets" PRIMARY KEY ("id")' in ddl
    assert "PRIMARY KEY" not in store.create_ddl(_PETS, _COLS)


def test_reconcile_creates_namespace_and_table_when_absent():
    cur = _Cursor({"SELECT column_name": ([], ["column_name"])})
    assert (
        store.reconcile_snowflake_native(cur, parts=_PETS, columns=_COLS, pk_columns=["id"])
        == "created"
    )
    assert cur.sql[0] == 'CREATE DATABASE IF NOT EXISTS "_landing"'
    assert cur.sql[1] == 'CREATE SCHEMA IF NOT EXISTS "_landing"."mat"'
    assert cur.sql[-1].startswith("CREATE TABLE IF NOT EXISTS")


def test_reconcile_keeps_a_matching_table_and_recreates_on_column_or_key_drift():
    matching = {
        "SELECT column_name": ([("id",), ("name",), ("meta",)], ["column_name"]),
        "SHOW PRIMARY KEYS": ([("id", 1)], ["column_name", "key_sequence"]),
    }
    cur = _Cursor(matching)
    assert (
        store.reconcile_snowflake_native(cur, parts=_PETS, columns=_COLS, pk_columns=["id"])
        == "kept"
    )
    assert not any(s.startswith("DROP TABLE") for s in cur.sql)
    # REQ-1651: a key-less table with a now-declared key is drift, exactly as a column change is
    cur = _Cursor({**matching, "SHOW PRIMARY KEYS": ([], ["column_name", "key_sequence"])})
    assert (
        store.reconcile_snowflake_native(cur, parts=_PETS, columns=_COLS, pk_columns=["id"])
        == "recreated"
    )
    assert any(s.startswith("DROP TABLE IF EXISTS") for s in cur.sql)
    cur = _Cursor(matching)
    assert (
        store.reconcile_snowflake_native(cur, parts=_PETS, columns=_COLS[:2], pk_columns=["id"])
        == "recreated"
    )


def test_land_replace_deletes_then_inserts_with_json_parsed():
    cur = _Cursor()
    rows = [{"id": 1, "name": "Rex", "meta": {"k": "v"}}, {"id": 2, "name": "Tom", "meta": None}]
    store.land_snowflake_native(cur, parts=_PETS, columns=_COLS, rows=rows, shape="replace")
    # one atomic multi-row INSERT OVERWRITE (no separate DELETE: two concurrent replaces must not
    # interleave), JSON parsed in the projection over VALUES (executemany's rewrite refuses
    # INSERT ... SELECT, and VALUES cannot hold PARSE_JSON)
    assert len(cur.sql) == 1
    assert cur.sql[0] == (
        'INSERT OVERWRITE INTO "_landing"."mat"."pet-store-sqlite__pet_store__pets" '
        '("id", "name", "meta") SELECT column1, column2, PARSE_JSON(column3) FROM VALUES '
        "(%s, %s, %s), (%s, %s, %s)"
    )
    assert cur.params[0] == [1, "Rex", '{"k": "v"}', 2, "Tom", None]
    assert cur.many == []


def test_land_replace_with_no_rows_empties_the_replica():
    cur = _Cursor()
    store.land_snowflake_native(cur, parts=_PETS, columns=_COLS, rows=[], shape="replace")
    assert cur.sql == ['DELETE FROM "_landing"."mat"."pet-store-sqlite__pet_store__pets"']


def test_land_append_does_not_delete_and_cdc_is_refused():
    cur = _Cursor()
    store.land_snowflake_native(cur, parts=_PETS, columns=_COLS, rows=[{"id": 3}], shape="append")
    assert len(cur.sql) == 1 and cur.sql[0].startswith("INSERT INTO")
    assert cur.many == []
    with pytest.raises(NotImplementedError):
        store.land_snowflake_native(cur, parts=_PETS, columns=_COLS, rows=[], shape="cdc")


def test_expose_view_creates_when_absent_replaces_on_recreate_or_stale_body_else_keeps():
    body_ok = 'create or replace secure view "pets" as SELECT * FROM "_landing"."mat"."pet-store-sqlite__pet_store__pets"'
    body_stale = 'create or replace view "pets" as SELECT * FROM "landing"."mat"."pets"'
    # absent → create
    cur = _Cursor({"SELECT view_definition": ([], ["view_definition"])})
    store.expose_view(cur, view=_PETS_VIEW, replica=_PETS, replace=False)
    assert cur.sql[-1].startswith(
        'CREATE OR REPLACE SECURE VIEW "pet_store_sqlite"."pet_store"."pets"'
    )
    # present and reading this replica → left alone (its tags and comments survive)
    cur = _Cursor({"SELECT view_definition": ([(body_ok,)], ["view_definition"])})
    store.expose_view(cur, view=_PETS_VIEW, replica=_PETS, replace=False)
    assert not any(s.startswith("CREATE OR REPLACE") for s in cur.sql)
    # present but pointing elsewhere (the Sep-6 layout's dead "landing" database) → replaced
    cur = _Cursor({"SELECT view_definition": ([(body_stale,)], ["view_definition"])})
    store.expose_view(cur, view=_PETS_VIEW, replica=_PETS, replace=False)
    assert cur.sql[-1].startswith("CREATE OR REPLACE SECURE VIEW")
    # replica recreated → replaced regardless
    cur = _Cursor({"SELECT view_definition": ([(body_ok,)], ["view_definition"])})
    store.expose_view(cur, view=_PETS_VIEW, replica=_PETS, replace=True)
    assert cur.sql[-1].startswith("CREATE OR REPLACE SECURE VIEW")


def _targets():
    return {
        "pet-store-sqlite.pet_store.pets": store.KeyTarget(_PETS, _PETS_VIEW, ("id",)),
        "pet-store-sqlite.pet_store.pet_visits": store.KeyTarget(_VISITS, _VISITS_VIEW, ("id",)),
    }


def _edge():
    return ForeignKeyEdge(
        "provisa_fk_visits_pet",
        "pet-store-sqlite.pet_store.pet_visits",
        ("pet_id",),
        "pet-store-sqlite.pet_store.pets",
        ("id",),
    )


def test_constraint_statements_add_keys_and_are_idempotent():
    stmts = store.constraint_statements(_targets(), [_edge()], {}, {})
    assert stmts == [
        'ALTER TABLE "_landing"."mat"."pet-store-sqlite__pet_store__pet_visits" ADD CONSTRAINT '
        '"provisa_pk_pet_store_sqlite__pet_store__pet_visits" PRIMARY KEY ("id")',
        'ALTER TABLE "_landing"."mat"."pet-store-sqlite__pet_store__pets" ADD CONSTRAINT '
        '"provisa_pk_pet_store_sqlite__pet_store__pets" PRIMARY KEY ("id")',
        'ALTER TABLE "_landing"."mat"."pet-store-sqlite__pet_store__pet_visits" ADD CONSTRAINT '
        '"provisa_fk_visits_pet" FOREIGN KEY ("pet_id") REFERENCES '
        '"_landing"."mat"."pet-store-sqlite__pet_store__pets" ("id")',
    ]
    # everything already there → nothing
    assert (
        store.constraint_statements(
            _targets(),
            [_edge()],
            {_PETS: ("id",), _VISITS: ("id",)},
            {_VISITS: frozenset({"provisa_fk_visits_pet"})},
        )
        == []
    )


def test_constraint_statements_replace_a_differing_key_and_withdraw_stale_owned_keys():
    stmts = store.constraint_statements(
        _targets(),
        [],
        {_PETS: ("name",), _VISITS: ("id",)},
        {_VISITS: frozenset({"provisa_fk_gone", "dba_added"})},
    )
    assert (
        stmts[0]
        == 'ALTER TABLE "_landing"."mat"."pet-store-sqlite__pet_store__pets" DROP PRIMARY KEY'
    )
    assert 'PRIMARY KEY ("id")' in stmts[1]
    assert stmts[2] == (
        'ALTER TABLE "_landing"."mat"."pet-store-sqlite__pet_store__pet_visits" '
        'DROP CONSTRAINT "provisa_fk_gone"'
    )
    assert not any("dba_added" in s for s in stmts)


def test_key_tag_statements_mirror_keys_onto_views_and_unset_stale_ones():
    existing: dict[store.Parts, store.ObjectTags] = {
        _PETS_VIEW: store.ObjectTags(
            table={},
            columns={
                "id": {"PRIMARY_KEY": "1", "FOREIGN_KEY": "x"},
                "name": {"VISIBILITY_RESTRICTED": "y"},
            },
        )
    }
    stmts = store.key_tag_statements(_LANDING, _targets(), [_edge()], existing)
    assert stmts[:3] == [
        'CREATE SCHEMA IF NOT EXISTS "_landing"."PROVISA_GOVERNANCE"',
        'CREATE TAG IF NOT EXISTS "_landing"."PROVISA_GOVERNANCE"."PRIMARY_KEY"',
        'CREATE TAG IF NOT EXISTS "_landing"."PROVISA_GOVERNANCE"."FOREIGN_KEY"',
    ]
    assert (
        'ALTER VIEW "pet_store_sqlite"."pet_store"."pets" MODIFY COLUMN "id" '
        'SET TAG "_landing"."PROVISA_GOVERNANCE"."PRIMARY_KEY" = \'1\''
    ) in stmts
    assert (
        'ALTER VIEW "pet_store_sqlite"."pet_store"."pet_visits" MODIFY COLUMN "pet_id" '
        'SET TAG "_landing"."PROVISA_GOVERNANCE"."FOREIGN_KEY" = '
        '\'"pet_store_sqlite"."pet_store"."pets"."id"\''
    ) in stmts
    # the stale FOREIGN_KEY on pets.id goes; a non-key tag is never touched
    assert (
        'ALTER VIEW "pet_store_sqlite"."pet_store"."pets" MODIFY COLUMN "id" '
        'UNSET TAG "_landing"."PROVISA_GOVERNANCE"."FOREIGN_KEY"'
    ) in stmts
    assert not any("VISIBILITY_RESTRICTED" in s for s in stmts)


def test_key_tag_statements_emit_nothing_without_a_view_or_a_key():
    no_view = {"x": store.KeyTarget(_PETS, None, ("id",))}
    assert store.key_tag_statements(_LANDING, no_view, [], {}) == []
    no_key = {"x": store.KeyTarget(_PETS, _PETS_VIEW, ())}
    assert store.key_tag_statements(_LANDING, no_key, [], {}) == []


def test_reconcile_keys_native_reads_state_once_per_object_then_applies():
    answers = {
        "SHOW PRIMARY KEYS": ([], ["column_name", "key_sequence"]),
        "SHOW IMPORTED KEYS": ([], ["fk_name"]),
        "tag_references_all_columns": ([], ["column_name", "tag_name"]),
        "SELECT comment FROM": ([], ["comment"]),
        "SELECT column_name, comment": ([], ["column_name", "comment"]),
        # every object exists (the existence probe reads information_schema.columns)
        "SELECT column_name FROM": ([("id",)], ["column_name"]),
    }
    cur = _Cursor(answers)
    applied = store.reconcile_metadata_native(
        cur, tags_database=_LANDING, targets=_targets(), edges=[_edge()]
    )
    ddl = [s for s in cur.sql if s.startswith(("ALTER", "CREATE"))]
    assert applied == len(ddl) == 3 + 3 + 3  # 2 PK + 1 FK, 3 tag headers, 2 PK tags + 1 FK tag
    assert sum(s.startswith("SHOW PRIMARY KEYS") for s in cur.sql) == 2


def test_comment_statements_write_descriptions_to_replica_and_view_and_respect_extended_comments():
    targets = {
        "pet-store-sqlite.pet_store.pets": store.KeyTarget(
            _PETS, _PETS_VIEW, ("id",), "Pet inventory", {"id": "Pet id", "name": "Pet name"}
        )
    }
    existing = {
        # the view already carries the description plus a governance note the export appended
        _PETS_VIEW: ("Pet inventory\n\nGovernance: masked", {"id": "Pet id", "name": "old"}),
        _PETS: ("", {}),
    }
    stmts = store.comment_statements(targets, existing)
    assert stmts == [
        'ALTER TABLE "_landing"."mat"."pet-store-sqlite__pet_store__pets" SET COMMENT = \'Pet inventory\'',
        'ALTER TABLE "_landing"."mat"."pet-store-sqlite__pet_store__pets" MODIFY COLUMN "id" COMMENT \'Pet id\'',
        'ALTER TABLE "_landing"."mat"."pet-store-sqlite__pet_store__pets" MODIFY COLUMN "name" COMMENT \'Pet name\'',
        'ALTER VIEW "pet_store_sqlite"."pet_store"."pets" ALTER COLUMN "name" COMMENT \'Pet name\'',
    ]
    # an empty description never erases; a matching one is left alone
    quiet = {"x": store.KeyTarget(_PETS, None, (), "", {})}
    assert store.comment_statements(quiet, {_PETS: ("anything", {})}) == []


def test_comment_statements_escape_quotes():
    targets = {"x": store.KeyTarget(_PETS, None, (), "Owner's pets", {})}
    assert store.comment_statements(targets, {})[0].endswith("SET COMMENT = 'Owner''s pets'")


def test_model_tag_statements_set_reason_valued_tags_on_replica_and_view_and_withdraw_unassigned():
    targets = {
        "pet-store-sqlite.pet_store.pets": store.KeyTarget(
            _PETS,
            _PETS_VIEW,
            ("id",),
            tags=(("gold", "Curated"),),
            column_tags={"name": (("pii", "pii"),)},
        )
    }
    existing = {
        _PETS_VIEW: store.ObjectTags(
            table={"GOLD": "Curated", "DEPRECATED": "old"},  # DEPRECATED no longer assigned
            columns={"name": {"PII": "stale value", "VISIBILITY_RESTRICTED": "keep"}},
        )
    }
    stmts = store.model_tag_statements(
        _LANDING, targets, existing, frozenset({"gold", "pii", "deprecated"})
    )
    assert stmts[:3] == [
        'CREATE SCHEMA IF NOT EXISTS "_landing"."PROVISA_GOVERNANCE"',
        'CREATE TAG IF NOT EXISTS "_landing"."PROVISA_GOVERNANCE"."GOLD"',
        'CREATE TAG IF NOT EXISTS "_landing"."PROVISA_GOVERNANCE"."PII"',
    ]
    body = stmts[3:]
    # replica: nothing existed → both tags set
    assert (
        'ALTER TABLE "_landing"."mat"."pet-store-sqlite__pet_store__pets" '
        'SET TAG "_landing"."PROVISA_GOVERNANCE"."GOLD" = \'Curated\''
    ) in body
    assert (
        'ALTER TABLE "_landing"."mat"."pet-store-sqlite__pet_store__pets" MODIFY COLUMN "name" '
        'SET TAG "_landing"."PROVISA_GOVERNANCE"."PII" = \'pii\''
    ) in body
    # view: GOLD matches (no statement), DEPRECATED withdrawn, PII value corrected, other tag kept
    assert not any('"pets" SET TAG "_landing"."PROVISA_GOVERNANCE"."GOLD"' in s for s in body)
    assert (
        'ALTER VIEW "pet_store_sqlite"."pet_store"."pets" UNSET TAG "_landing"."PROVISA_GOVERNANCE"."DEPRECATED"'
        in body
    )
    assert (
        'ALTER VIEW "pet_store_sqlite"."pet_store"."pets" ALTER COLUMN "name" '
        'SET TAG "_landing"."PROVISA_GOVERNANCE"."PII" = \'pii\''
    ) in body
    assert not any("VISIBILITY_RESTRICTED" in s for s in body)


def test_merge_upserts_by_key_with_one_prepared_statement():
    cur = _Cursor()
    rows = [{"id": 1, "name": "Rex", "meta": None}]
    store.merge_snowflake_native(cur, parts=_PETS, columns=_COLS, rows=rows, pk_columns=["id"])
    sql, params = cur.many[0]
    assert sql.startswith('MERGE INTO "_landing"."mat"."pet-store-sqlite__pet_store__pets" t USING')
    assert (
        'ON t."id" = s."id"' in sql and 'UPDATE SET t."name" = s."name", t."meta" = s."meta"' in sql
    )
    assert params == [[1, "Rex", None]]
    with pytest.raises(ValueError):
        store.merge_snowflake_native(cur, parts=_PETS, columns=_COLS, rows=rows, pk_columns=[])


def test_reconcile_metadata_skips_an_object_that_does_not_exist_yet():
    # an MV whose first refresh has not run: no columns in information_schema → nothing applied,
    # and the edge that references it waits with it
    answers = {"SELECT column_name FROM": ([], ["column_name"])}
    cur = _Cursor(answers)
    applied = store.reconcile_metadata_native(
        cur, tags_database=_LANDING, targets=_targets(), edges=[_edge()]
    )
    assert applied == 0
    assert not any(s.startswith("ALTER") for s in cur.sql)


class _RefusingCursor(_Cursor):
    def execute(self, sql, params=None):
        super().execute(sql, params)
        if "check_definition" in sql:
            raise RuntimeError("Failure during expansion of view")


def test_reconcile_metadata_keeps_going_past_a_refused_statement():
    answers = {
        "SHOW PRIMARY KEYS": ([], ["column_name", "key_sequence"]),
        "SHOW IMPORTED KEYS": ([], ["fk_name"]),
        "tag_references_all_columns": ([], ["level", "column_name", "tag_name", "tag_value"]),
        "SELECT comment FROM": ([], ["comment"]),
        "SELECT column_name, comment": ([], ["column_name", "comment"]),
        "SELECT column_name FROM": ([("id",)], ["column_name"]),
    }
    cur = _RefusingCursor(answers)
    targets = {
        "a": store.KeyTarget(
            ("_landing", "mat", "a"), None, ("id",), "", {"check_definition": "x"}
        ),
        "b": store.KeyTarget(("_landing", "mat", "b"), None, ("id",), "B table", {}),
    }
    applied = store.reconcile_metadata_native(
        cur, tags_database="_landing", targets=targets, edges=[]
    )
    ddl = [s for s in cur.sql if s.startswith("ALTER")]
    # 2 PRIMARY KEYs + b's comment succeed; a's refused column comment does not stop b
    assert applied == 3 and len(ddl) == 4
    assert any("SET COMMENT = 'B table'" in s for s in ddl)
