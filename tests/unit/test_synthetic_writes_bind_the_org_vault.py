# Copyright (c) 2026 Kenneth Stott
# Canary: 8472e647-87d5-42ae-9d4c-076ae3567c19
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A generated table and a closeness sample land in the engine's store with the vault of the org
their sources are registered in bound (REQ-1695, REQ-1942): the store's target attaches every
registered source, resolving each one's ${secret:...}. The testdata demo template's generation
failed with "Cannot resolve ${secret:SHOP_DB_PASSWORD}: no organization is bound"."""

# Requirements: REQ-1695, REQ-1942

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from provisa.core import secrets_store
from provisa.core.request_context import reset_current_org, set_current_org
from provisa.core.secrets import resolve_secrets
from provisa.synthetic import run

pytestmark = pytest.mark.unit

_REF = "${secret:SHOP_DB_PASSWORD}"


@pytest.fixture
def landed(monkeypatch: pytest.MonkeyPatch) -> dict:
    """The store's target and the copy, each resolving the shop source's password as the attach
    walk does; what each one resolved."""
    from provisa.federation import data_replicator, replica_builds, replica_parties, residency

    seen: dict = {}
    source = SimpleNamespace(id="shop", password=_REF)

    async def model_row(state, key):
        return source, SimpleNamespace(columns=[]), [source]

    async def decrypted(admin_db, org_id, owner_id):
        return {"SHOP_DB_PASSWORD": f"pw-of-{org_id}"}

    class Job:
        async def run(self, progress):
            seen["copy"] = resolve_secrets(_REF)
            return SimpleNamespace(rows_copied=3)

    def replica_target(state, **_):
        seen["target"] = resolve_secrets(_REF)
        return object()

    def read_catalog(state):
        seen["catalog"] = resolve_secrets(_REF)
        return "store"

    monkeypatch.setattr(replica_builds, "_model_row", model_row)
    monkeypatch.setattr(secrets_store, "_decrypted", decrypted)
    monkeypatch.setattr(residency, "resolve_landing_args", lambda s, t, platform: SimpleNamespace())
    monkeypatch.setattr(replica_parties, "StoreReadingEngine", lambda backend, state: object())
    monkeypatch.setattr(data_replicator, "data_replicator", lambda *a, **k: Job())
    monkeypatch.setattr(run, "generation_sql", lambda plan, dialect, children: "SELECT 1")
    monkeypatch.setattr(run, "replace", lambda args, **k: args)
    backend = SimpleNamespace(
        dialect="duckdb", replica_target=replica_target, replica_read_catalog=read_catalog
    )
    seen["state"] = SimpleNamespace(
        federation_engine=SimpleNamespace(engine=SimpleNamespace(backend=backend, name="duckdb")),
        admin_db=object(),
        active_org_id="sbxtpltestdata",
    )
    return seen


def _bound_to(org: str, work):
    async def go():
        token = set_current_org(org)
        try:
            return await work
        finally:
            reset_current_org(token)

    return asyncio.run(go())


_REG = {"source_id": "shop", "schema_name": "public", "table_name": "orders", "parameters": {}}


def test_a_generated_table_lands_with_its_orgs_vault_bound(landed):
    planned = SimpleNamespace(plan=SimpleNamespace(name="orders"))
    rows = _bound_to("sbxtpltestdata", run._write_table(landed["state"], "syn", planned, _REG))
    assert rows == 3
    assert landed["target"] == landed["copy"] == "pw-of-sbxtpltestdata"
    assert secrets_store.bound_org_id() is None  # bound for the landing alone


def test_a_closeness_sample_lands_with_its_orgs_vault_bound(landed, monkeypatch):
    from provisa.profiler import run as profiler_run

    async def governed(sql):
        return ["a"], [("x",), ("y",)]

    monkeypatch.setattr(profiler_run, "_governed", governed)
    monkeypatch.setattr(run, "_exposer", lambda state: lambda table, name: name)
    monkeypatch.setattr(run, "sample_read_sql", lambda table, specs: "SELECT a")
    monkeypatch.setattr(run, "sample_name", lambda reg: "orders_sample")
    planned = SimpleNamespace(table=SimpleNamespace(pgwire_name="orders", name="orders"))
    col = SimpleNamespace(name="a", family="text", sql_type="text")
    address, _cols = _bound_to(
        "sbxtpltestdata", run._land_sample(landed["state"], "syn", planned, _REG, [col])
    )
    assert address == '"store"."syn"."orders_sample"'
    assert landed["target"] == landed["copy"] == landed["catalog"] == "pw-of-sbxtpltestdata"
