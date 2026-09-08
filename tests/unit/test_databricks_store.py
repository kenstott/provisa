# Copyright (c) 2026 Kenneth Stott
# Canary: 8eddaa46-1603-4be0-a6c4-cd6f6404ff3b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-987: the Databricks materialization write face. Driver-free — a fake cursor records the SQL so
the DDL/type mapping, catalog+schema creation, replace/append shapes, and bulk (non-per-row) insert
are pinned without a live warehouse. The live land→query round-trip is exercised in integration."""

from __future__ import annotations

import json

import pytest

from provisa.federation import databricks_store
from provisa.federation.databricks_store import (
    ObjectTags,
    comment_statements,
    constraint_statements,
    reconcile_metadata_native,
    tag_key,
    tag_statements,
    COPY_INTO_ROW_THRESHOLD,
    DatabricksStage,
    _ddl_type,
    land_databricks_native,
    reconcile_databricks_native,
)

COLS = [("id", "bigint"), ("s", "text"), ("amt", "numeric"), ("j", "json")]


class _FakeCursor:
    """Records executed SQL (+ params); answers information_schema column probes from a settable set."""

    def __init__(self):
        self.sql: list[tuple[str, list | None]] = []
        self._existing: list[str] = []
        self._pk: list[str] = []
        self._fks: list[str] = []
        self._comments: tuple[str, dict[str, str]] = ("", {})
        self._tags: tuple[dict[str, str], dict[str, dict[str, str]]] = ({}, {})
        self._last = ""

    def execute(self, sql, params=None):
        self.sql.append((sql, params))
        self._last = sql

    def fetchall(self):
        q = self._last
        if "information_schema.key_column_usage" in q:
            return [(c,) for c in self._pk]
        if "constraint_type = 'FOREIGN KEY'" in q:
            return [(n,) for n in self._fks]
        if "SELECT comment FROM" in q:
            return [(self._comments[0],)]
        if "SELECT column_name, comment FROM" in q:
            return list(self._comments[1].items())
        if "information_schema.table_tags" in q:
            return list(self._tags[0].items())
        if "information_schema.column_tags" in q:
            return [(c, k, v) for c, tags in self._tags[1].items() for k, v in tags.items()]
        if "SELECT 1 FROM" in q:
            return [(1,)] if self._existing else []
        return [(c,) for c in self._existing]

    def joined(self) -> str:
        return " | ".join(s for s, _ in self.sql)


def test_ddl_type_maps_ir_and_raises_on_unknown():
    assert _ddl_type("bigint") == "BIGINT"
    assert _ddl_type("text") == "STRING"
    assert _ddl_type("json") == "STRING"
    assert _ddl_type("numeric") == "DECIMAL(38,9)"
    assert _ddl_type("timestamptz") == "TIMESTAMP"  # native spelling normalizes through to_ir
    # A type outside the IR vocabulary raises (via to_ir) rather than silently widening.
    with pytest.raises(ValueError, match="not in the IR vocabulary"):
        _ddl_type("geography")


def test_reconcile_creates_catalog_schema_and_table():
    cur = _FakeCursor()
    assert (
        reconcile_databricks_native(
            cur, catalog="sales_db", schema="public", table="orders", columns=COLS
        )
        == "created"
    )
    j = cur.joined()
    assert "CREATE CATALOG IF NOT EXISTS `sales_db`" in j
    assert "CREATE SCHEMA IF NOT EXISTS `sales_db`.`public`" in j
    assert "CREATE TABLE IF NOT EXISTS `sales_db`.`public`.`orders`" in j
    assert "USING DELTA" in j


def test_reconcile_keeps_matching_and_recreates_on_drift():
    cur = _FakeCursor()
    cur._existing = ["id", "s", "amt", "j"]
    assert (
        reconcile_databricks_native(cur, catalog="c", schema="s", table="t", columns=COLS) == "kept"
    )
    cur2 = _FakeCursor()
    cur2._existing = ["id", "s"]  # drift
    assert (
        reconcile_databricks_native(cur2, catalog="c", schema="s", table="t", columns=COLS)
        == "recreated"
    )
    assert "DROP TABLE IF EXISTS `c`.`s`.`t`" in cur2.joined()


def test_land_replace_truncates_then_bulk_inserts():
    cur = _FakeCursor()
    land_databricks_native(
        cur,
        catalog="c",
        schema="s",
        table="t",
        columns=COLS,
        rows=[
            {"id": 1, "s": "a", "amt": 10, "j": json.dumps({"k": 1})},
            {"id": 2, "s": "b", "amt": 20, "j": None},
        ],
    )
    j = cur.joined()
    assert "TRUNCATE TABLE `c`.`s`.`t`" in j  # replace = full refresh
    inserts = [(s, p) for s, p in cur.sql if s.startswith("INSERT INTO")]
    assert len(inserts) == 1  # ONE bulk multi-row INSERT, never a per-row loop (REQ-987)
    sql, params = inserts[0]
    assert params is not None
    assert sql.count("(?, ?, ?, ?)") == 2  # two value tuples in one statement
    assert len(params) == 8  # 2 rows × 4 columns, flattened


def test_land_append_does_not_truncate():
    cur = _FakeCursor()
    land_databricks_native(
        cur,
        catalog="c",
        schema="s",
        table="t",
        columns=COLS,
        rows=[{"id": 3, "s": "c", "amt": 30, "j": None}],
        change_signal="poll",
        watermark_column="id",
    )
    assert "TRUNCATE" not in cur.joined()  # append amends, never truncates


def test_land_coerces_dict_json_to_text():
    cur = _FakeCursor()
    land_databricks_native(
        cur,
        catalog="c",
        schema="s",
        table="t",
        columns=COLS,
        rows=[{"id": 1, "s": "a", "amt": 1, "j": {"k": 2}}],  # dict, not str
    )
    _, params = [(s, p) for s, p in cur.sql if s.startswith("INSERT INTO")][0]
    assert params is not None
    assert params[3] == '{"k": 2}'  # dict re-serialized to JSON text for the STRING column


def _fake_stage() -> DatabricksStage:
    return DatabricksStage(
        root_url="r2://b@acct.r2.cloudflarestorage.com/stage/",
        endpoint_url="https://acct.r2.cloudflarestorage.com",
        credential={"access_key_id": "k", "secret_access_key": "s", "account_id": "a"},
        uc_host="host",
        uc_token="tok",
    )


def test_land_large_batch_uses_copy_into(monkeypatch):
    # Stub the object upload + UC credential install so the gate is exercised without R2/Databricks.
    staged: dict = {}

    def fake_stage_parquet(stage, key, arrow_table):
        staged["url"] = stage.root_url + key
        staged["rows"] = arrow_table.num_rows
        return staged["url"]

    monkeypatch.setattr(databricks_store, "_stage_parquet", fake_stage_parquet)
    monkeypatch.setattr(databricks_store, "ensure_external_link", lambda *a, **k: "loc")
    monkeypatch.setattr(databricks_store, "_unstage", lambda *a, **k: None)

    cur = _FakeCursor()
    rows = [{"id": i, "s": "x", "amt": i, "j": None} for i in range(COPY_INTO_ROW_THRESHOLD)]
    land_databricks_native(
        cur, catalog="c", schema="s", table="t", columns=COLS, rows=rows, stage=_fake_stage()
    )

    j = cur.joined()
    assert "COPY INTO `c`.`s`.`t`" in j  # bulk COPY-INTO for a batch at/above threshold (REQ-990)
    assert "FILEFORMAT = PARQUET" in j
    assert not any(s.startswith("INSERT INTO") for s, _ in cur.sql)  # never the row INSERT
    assert "TRUNCATE TABLE `c`.`s`.`t`" in j  # replace shape still truncates first
    assert staged["rows"] == COPY_INTO_ROW_THRESHOLD  # the whole batch was staged as Parquet


def test_land_small_batch_uses_insert_even_with_stage(monkeypatch):
    # A stage is configured but the batch is below threshold → the multi-row INSERT (REQ-990 tiny write).
    monkeypatch.setattr(
        databricks_store,
        "_stage_parquet",
        lambda *a, **k: pytest.fail("COPY path taken for tiny batch"),
    )
    cur = _FakeCursor()
    rows = [{"id": 1, "s": "a", "amt": 10, "j": None}]  # 1 row << threshold
    land_databricks_native(
        cur, catalog="c", schema="s", table="t", columns=COLS, rows=rows, stage=_fake_stage()
    )

    j = cur.joined()
    assert "COPY INTO" not in j
    inserts = [s for s, _ in cur.sql if s.startswith("INSERT INTO")]
    assert len(inserts) == 1  # one multi-row INSERT for the tiny write


def test_land_no_rows_creates_but_no_insert():
    cur = _FakeCursor()
    land_databricks_native(cur, catalog="c", schema="s", table="t", columns=COLS, rows=[])
    assert not any(s.startswith("INSERT INTO") for s, _ in cur.sql)
    assert "CREATE TABLE IF NOT EXISTS" in cur.joined()


if __name__ == "__main__":
    pytest.main([__file__, "-q"])


# -- landed metadata (REQ-1657) -------------------------------------------------------------------

from provisa.federation.landed_keys import ForeignKeyEdge, KeyTarget  # noqa: E402

_PETS = ("pet_store_sqlite", "pet_store", "pets")
_VISITS = ("pet_store_sqlite", "pet_store", "pet_visits")


def _targets(**overrides) -> dict[str, KeyTarget]:
    base = {
        "pets": KeyTarget(
            replica=_PETS,
            view=None,
            primary_key=("id",),
            description="Pet inventory",
            column_descriptions={"name": "Pet name"},
            tags=(("gold", "Curated"),),
            column_tags={"name": (("pii", "pii"),)},
        ),
        "visits": KeyTarget(replica=_VISITS, view=None, primary_key=("id",)),
    }
    base.update(overrides)
    return base


_EDGE = ForeignKeyEdge("provisa_fk_visits_pet", "visits", ("pet_id",), "pets", ("id",))


def test_create_ddl_declares_not_null_key_columns_and_an_inline_primary_key():
    cur = _FakeCursor()
    reconcile_databricks_native(
        cur, catalog="c", schema="s", table="t", columns=COLS, pk_columns=["id"]
    )
    assert (
        "CREATE TABLE IF NOT EXISTS `c`.`s`.`t` (`id` BIGINT NOT NULL, `s` STRING, "
        "`amt` DECIMAL(38,9), `j` STRING, CONSTRAINT `provisa_pk_t` PRIMARY KEY (`id`)) USING DELTA"
        in cur.joined()
    )


def test_reconcile_recreates_when_the_primary_key_drifted():
    cur = _FakeCursor()
    cur._existing = ["id", "s", "amt", "j"]
    cur._pk = ["s"]
    outcome = reconcile_databricks_native(
        cur, catalog="c", schema="s", table="t", columns=COLS, pk_columns=["id"]
    )
    assert outcome == "recreated"


def test_tag_key_avoids_unity_catalog_reserved_characters():
    assert tag_key("PII") == "provisa_governance:pii"
    assert tag_key("cost.center=x") == "provisa_governance:cost_center_x"


def test_constraint_statements_add_missing_keys_with_not_null_first():
    stmts = constraint_statements(_targets(), [_EDGE], {}, {})
    assert stmts == [
        "ALTER TABLE `pet_store_sqlite`.`pet_store`.`pets` ALTER COLUMN `id` SET NOT NULL",
        "ALTER TABLE `pet_store_sqlite`.`pet_store`.`pets` ADD CONSTRAINT `provisa_pk_pets` "
        "PRIMARY KEY (`id`)",
        "ALTER TABLE `pet_store_sqlite`.`pet_store`.`pet_visits` ALTER COLUMN `id` SET NOT NULL",
        "ALTER TABLE `pet_store_sqlite`.`pet_store`.`pet_visits` ADD CONSTRAINT "
        "`provisa_pk_pet_visits` PRIMARY KEY (`id`)",
        "ALTER TABLE `pet_store_sqlite`.`pet_store`.`pet_visits` ADD CONSTRAINT "
        "`provisa_fk_visits_pet` FOREIGN KEY (`pet_id`) "
        "REFERENCES `pet_store_sqlite`.`pet_store`.`pets` (`id`)",
    ]


def test_constraint_statements_are_idempotent_and_withdraw_orphaned_owned_keys():
    pks = {_PETS: ("id",), _VISITS: ("id",)}
    fks = {_VISITS: frozenset({"provisa_fk_visits_pet", "provisa_fk_gone", "customer_fk"})}
    assert constraint_statements(_targets(), [_EDGE], pks, fks) == [
        "ALTER TABLE `pet_store_sqlite`.`pet_store`.`pet_visits` DROP CONSTRAINT `provisa_fk_gone`"
    ]


def test_replacing_a_primary_key_cascades_and_re_adds_the_edges_into_it():
    pks = {_PETS: ("name",), _VISITS: ("id",)}
    fks = {_VISITS: frozenset({"provisa_fk_visits_pet"})}
    stmts = constraint_statements(_targets(), [_EDGE], pks, fks)
    assert stmts[0] == "ALTER TABLE `pet_store_sqlite`.`pet_store`.`pets` DROP PRIMARY KEY CASCADE"
    assert stmts[-1].startswith(
        "ALTER TABLE `pet_store_sqlite`.`pet_store`.`pet_visits` ADD CONSTRAINT "
        "`provisa_fk_visits_pet`"
    )


def test_comment_statements_write_only_what_differs():
    existing = {_PETS: ("Pet inventory -- extended by a catalog", {"name": ""})}
    assert comment_statements(_targets(), existing) == [
        "ALTER TABLE `pet_store_sqlite`.`pet_store`.`pets` ALTER COLUMN `name` COMMENT 'Pet name'"
    ]
    assert comment_statements(_targets(), {}) == [
        "COMMENT ON TABLE `pet_store_sqlite`.`pet_store`.`pets` IS 'Pet inventory'",
        "ALTER TABLE `pet_store_sqlite`.`pet_store`.`pets` ALTER COLUMN `name` COMMENT 'Pet name'",
    ]


def test_tag_statements_set_missing_and_unset_known_unassigned():
    existing = {
        _PETS: ObjectTags(
            table={"provisa_governance:silver": "x"},
            columns={"name": {"provisa_governance:pii": "pii"}},
        )
    }
    assert tag_statements(_targets(), existing, frozenset({"gold", "silver", "pii"})) == [
        "ALTER TABLE `pet_store_sqlite`.`pet_store`.`pets` SET TAGS "
        "('provisa_governance:gold' = 'Curated')",
        "ALTER TABLE `pet_store_sqlite`.`pet_store`.`pets` UNSET TAGS ('provisa_governance:silver')",
    ]


def test_reconcile_metadata_skips_absent_tables_and_survives_a_refused_statement(caplog):
    cur = _FakeCursor()
    cur._existing = ["id", "name"]

    refused = {"COMMENT ON TABLE"}
    real_execute = cur.execute

    def execute(sql, params=None):
        real_execute(sql, params)
        if any(sql.startswith(r) for r in refused):
            raise RuntimeError("refused")

    cur.execute = execute
    applied = reconcile_metadata_native(cur, targets=_targets(), edges=[_EDGE])
    joined = cur.joined()
    assert "ADD CONSTRAINT `provisa_fk_visits_pet`" in joined
    assert "COMMENT ON TABLE" in joined
    assert applied == len([s for s, _ in cur.sql if s.startswith(("ALTER", "COMMENT"))]) - 1
    assert "refused" in caplog.text
