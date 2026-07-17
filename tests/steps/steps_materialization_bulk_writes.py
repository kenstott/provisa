# Copyright (c) 2026 Kenneth Stott
# Canary: 590d1e41-f4f2-4f15-829a-655a268b358c
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
"""pytest-bdd steps for REQ-990 — materialization writes use bulk/columnar ingest, gated explicitly."""

from __future__ import annotations

import pytest
from pytest_bdd import given, scenarios, then, when

from provisa.federation import databricks_store
from provisa.federation.databricks_store import (
    COPY_INTO_ROW_THRESHOLD,
    DatabricksStage,
    land_databricks_native,
)

COLS = [("id", "bigint"), ("s", "text"), ("amt", "numeric"), ("j", "json")]


def _stage() -> DatabricksStage:
    return DatabricksStage(
        root_url="r2://b@acct.r2.cloudflarestorage.com/stage/",
        endpoint_url="https://acct.r2.cloudflarestorage.com",
        credential={"access_key_id": "k", "secret_access_key": "s", "account_id": "a"},
        uc_host="host",
        uc_token="tok",
    )


class _FakeCursor:
    def __init__(self):
        self.sql: list[tuple[str, list | None]] = []
        self._existing: list[str] = []

    def execute(self, sql, params=None):
        self.sql.append((sql, params))

    def fetchall(self):
        return [(c,) for c in self._existing]

    def joined(self) -> str:
        return " | ".join(s for s, _ in self.sql)


@pytest.fixture
def shared_data():
    return {}


@given("a materialization target declaring bulk-COPY/columnar ingest support")
def given_bulk_capable_target(shared_data, monkeypatch):
    staged: dict = {}

    def fake_stage_parquet(stage, key, arrow_table):
        staged["rows"] = arrow_table.num_rows
        return stage.root_url + key

    monkeypatch.setattr(databricks_store, "_stage_parquet", fake_stage_parquet)
    monkeypatch.setattr(databricks_store, "ensure_external_link", lambda *a, **k: "loc")
    monkeypatch.setattr(databricks_store, "_unstage", lambda *a, **k: None)
    shared_data["staged"] = staged


@when("a batch at or above the bulk threshold is landed")
def when_bulk_batch_landed(shared_data):
    cur = _FakeCursor()
    rows = [{"id": i, "s": "x", "amt": i, "j": None} for i in range(COPY_INTO_ROW_THRESHOLD)]
    land_databricks_native(
        cur, catalog="c", schema="s", table="t", columns=COLS, rows=rows, stage=_stage()
    )
    shared_data["cur"] = cur


@then("the bulk/columnar ingest path is used, never row-by-row INSERT")
def then_bulk_path_used(shared_data):
    j = shared_data["cur"].joined()
    assert "COPY INTO `c`.`s`.`t`" in j  # capability-gated bulk path
    assert not any(s.startswith("INSERT INTO") for s, _ in shared_data["cur"].sql)
    assert shared_data["staged"]["rows"] == COPY_INTO_ROW_THRESHOLD


@given("a target without bulk support or a tiny write")
def given_tiny_write(shared_data, monkeypatch):
    # Even with a stage configured, a tiny batch must NOT take the COPY path.
    monkeypatch.setattr(
        databricks_store,
        "_stage_parquet",
        lambda *a, **k: pytest.fail("COPY path taken for a tiny write"),
    )
    shared_data["rows"] = [{"id": 1, "s": "a", "amt": 10, "j": None}]


@when("the batch is landed")
def when_tiny_batch_landed(shared_data):
    cur = _FakeCursor()
    land_databricks_native(
        cur,
        catalog="c",
        schema="s",
        table="t",
        columns=COLS,
        rows=shared_data["rows"],
        stage=_stage(),
    )
    shared_data["cur"] = cur


@then("row INSERT is used, capability-gated and explicit, never a silent fallback")
def then_insert_gated_explicit(shared_data):
    j = shared_data["cur"].joined()
    assert (
        "COPY INTO" not in j
    )  # the tiny-write branch is chosen explicitly, not a silent bulk miss
    inserts = [s for s, _ in shared_data["cur"].sql if s.startswith("INSERT INTO")]
    assert len(inserts) == 1


scenarios("../features/REQ-990.feature")
