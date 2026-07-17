# Copyright (c) 2026 Kenneth Stott
# Canary: 8f4f70fc-3cdc-4de6-aa2b-a39ac27b5d00
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
"""pytest-bdd steps for REQ-987 — Databricks bulk (columnar) writes, never a per-row INSERT loop."""

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


@given("the Databricks federation engine advertising ROWS/ARROW/ARROW_STREAM")
def given_databricks_engine(shared_data):
    from types import SimpleNamespace

    from provisa.federation.engine import build_databricks_engine
    from provisa.federation.runtime import EngineCapability, EngineRuntime

    state = SimpleNamespace(trino_conn=object(), flight_client=None, source_pools=None)
    caps = set(EngineRuntime(build_databricks_engine(), state).capabilities)
    assert {
        EngineCapability.ROWS,
        EngineCapability.ARROW,
        EngineCapability.ARROW_STREAM,
    } <= caps


@when("a batch at or above COPY_INTO_ROW_THRESHOLD is landed into the store")
def when_large_batch_landed(shared_data, monkeypatch):
    staged: dict = {}

    def fake_stage_parquet(stage, key, arrow_table):
        staged["rows"] = arrow_table.num_rows
        return stage.root_url + key

    monkeypatch.setattr(databricks_store, "_stage_parquet", fake_stage_parquet)
    monkeypatch.setattr(databricks_store, "ensure_external_link", lambda *a, **k: "loc")
    monkeypatch.setattr(databricks_store, "_unstage", lambda *a, **k: None)

    cur = _FakeCursor()
    rows = [{"id": i, "s": "x", "amt": i, "j": None} for i in range(COPY_INTO_ROW_THRESHOLD)]
    land_databricks_native(
        cur, catalog="c", schema="s", table="t", columns=COLS, rows=rows, stage=_stage()
    )
    shared_data["cur"] = cur
    shared_data["staged_rows"] = staged.get("rows")


@then("the batch is staged as Parquet and ingested via COPY INTO, never a per-row INSERT loop")
def then_copy_into_not_per_row(shared_data):
    j = shared_data["cur"].joined()
    assert "COPY INTO `c`.`s`.`t`" in j
    assert "FILEFORMAT = PARQUET" in j
    assert not any(s.startswith("INSERT INTO") for s, _ in shared_data["cur"].sql)
    assert shared_data["staged_rows"] == COPY_INTO_ROW_THRESHOLD


@given("a batch below the threshold")
def given_small_batch(shared_data, monkeypatch):
    monkeypatch.setattr(
        databricks_store,
        "_stage_parquet",
        lambda *a, **k: pytest.fail("COPY path taken for a tiny batch"),
    )
    shared_data["rows"] = [{"id": 1, "s": "a", "amt": 10, "j": None}]


@when("it is landed")
def when_small_batch_landed(shared_data):
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


@then("a single multi-row INSERT is used, never row-by-row")
def then_single_multirow_insert(shared_data):
    inserts = [s for s, _ in shared_data["cur"].sql if s.startswith("INSERT INTO")]
    assert len(inserts) == 1  # one bulk multi-row INSERT, not a per-row loop


scenarios("../features/REQ-987.feature")
