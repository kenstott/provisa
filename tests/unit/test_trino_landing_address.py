# Copyright (c) 2026 Kenneth Stott
# Canary: 3b3c1a52-2f0a-4c4f-9b06-7a9c1d5f7e21
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1912 (and REQ-1443): on Trino a replica lives in the store's replicas schema, like on every
engine, and a read is addressed to it there.

Trino reads the Postgres materialization store through its ``provisa_admin`` catalog. A source it
cannot read in place — a data-quality checker's scan results, an API's fetched pages, a sqlite
file — is served from its replica, and so is a source the operator floors. Every one of them has
the same address rule: the org's replicas schema, under ``<source>__<schema>__<table>``. No
source's replica sits at its registered address, where the source's live relation would be.

The reconcile is DDL-only, and it must run: a poll node probes its table's watermark BEFORE the
first land, so an unresolvable relation would fail the probe and prevent the land that would have
created it."""

# Requirements: REQ-1912, REQ-1443

from __future__ import annotations

import itertools

from types import SimpleNamespace

import pytest

from provisa.federation.backend import TrinoBackend
from provisa.federation.engine import build_trino_engine
from tests.helpers import no_engine_store, no_promoted_tables


@pytest.fixture(autouse=True)
def _bound_to_acme(bind_org):
    """The work is acme's, the org the test state serves, bound as its entrypoint binds it (REQ-1266)."""
    bind_org("acme")


def _rcol(name, data_type: str | None = "bigint", pk=False, nf=None):
    return {
        "column_name": name,
        "data_type": data_type,
        "is_primary_key": pk,
        "native_filter_type": nf,
    }


_IDS = itertools.count(1)


def _rtbl(sid, schema, tname, cols):
    return {
        # every registry row carries its registered id (db_queries.fetch_tables)
        "id": next(_IDS),
        "source_id": sid,
        "schema_name": schema,
        "table_name": tname,
        "columns": cols,
        # the per-table overrides every registry row carries (None = inherit the source's)
        "replicate": None,
        "load_protected": None,
    }


def _src(sid, stype):
    return SimpleNamespace(
        id=sid,
        type=SimpleNamespace(value=stype),
        change_signal="ttl",
        replicate=None,
        load_protected=False,
    )


class _FakeConn:
    async def __aenter__(self):
        return object()

    async def __aexit__(self, *a):
        return False


def _state(cfg, registered, monkeypatch):
    async def _fetch_tables(_conn):
        return registered

    monkeypatch.setattr("provisa.api.admin.db_queries.fetch_tables", _fetch_tables)
    # REQ-1939: no synthetic dataset is generated in this model.
    monkeypatch.setattr("provisa.synthetic.datasets.generated_tables", no_synthetic_tables)
    monkeypatch.setattr("provisa.federation.replica_state.promotion", no_promoted_tables)
    monkeypatch.setattr("provisa.federation.replica_builds.store_identity", no_engine_store)

    async def _source_rows(_conn):
        # REQ-1674, REQ-1919: the registry view reads the sources the model store holds.
        return [
            {
                "id": src.id,
                "type": src.type.value,
                "change_signal": src.change_signal,
                "replicate": src.replicate,
                "load_protected": src.load_protected,
                "password_ref": "",
            }
            for src in cfg.sources
        ]

    monkeypatch.setattr("provisa.core.repositories.source.list_all", _source_rows)
    return SimpleNamespace(
        config=cfg,
        model_db=(_one_db := SimpleNamespace(acquire=lambda: _FakeConn())),
        tenant_db=_one_db,
        org_id="acme",
    )


def _backend():
    return TrinoBackend(build_trino_engine())


_STATE = SimpleNamespace(org_id="acme")


@pytest.fixture(autouse=True)
def _a_postgres_store(monkeypatch):
    backend = _backend()
    monkeypatch.setattr(type(backend.engine), "materialize_store", lambda _self: "postgresql:///x")


def _address(source_id: str, schema_name: str, table_name: str) -> tuple[str, str]:
    address = _backend().replica_address(
        _STATE, source_id=source_id, schema_name=schema_name, table_name=table_name
    )
    return address.schema, address.table


class TestTheReplicaAddress:
    def test_an_adapter_produced_source_is_replicated_into_the_replicas_schema(self):
        assert _address("dq-checker", "quality", "pets_scan") == (
            "org_acme_replicas",
            "dq-checker__quality__pets_scan",
        )

    def test_a_sqlite_source_is_replicated_into_the_replicas_schema(self):
        """REQ-1660: a sqlite file is read by its connector and replicated like any other fetched
        source; its replica is not at the registered address."""
        assert _address("inquiries_sqlite", "default", "inquiries") == (
            "org_acme_replicas",
            "inquiries_sqlite__default__inquiries",
        )

    def test_an_engine_scannable_source_follows_the_same_rule(self):
        assert _address("warehouse_pg", "public", "orders") == (
            "org_acme_replicas",
            "warehouse_pg__public__orders",
        )

    def test_no_source_type_has_an_address_rule_of_its_own(self):
        """The rule reads the source's id and its table's names only."""
        import inspect

        params = inspect.signature(TrinoBackend.replica_address).parameters
        assert "source_type" not in params


@pytest.mark.asyncio
async def test_reconcile_converges_the_store_table_at_that_address(monkeypatch):
    calls: list[dict] = []

    async def _reconcile_table(dsn, *, schema, table, columns, pk_columns):
        del dsn
        calls.append(
            {"schema": schema, "table": table, "columns": columns, "pk_columns": pk_columns}
        )

    monkeypatch.setattr("provisa.federation.store_writer.reconcile_table", _reconcile_table)
    backend = _backend()
    cfg = SimpleNamespace(
        sources=[_src("dq-checker", "great_expectations"), _src("pg", "postgresql")], tables=[]
    )
    registered = [
        _rtbl(
            "dq-checker",
            "quality",
            "pets_scan",
            [_rcol("check_name", "text", pk=True), _rcol("passed", "boolean")],
        ),
        _rtbl("pg", "default", "users", [_rcol("id", "bigint", pk=True)]),  # ATTACH → not landed
    ]

    reconciled = await backend.reconcile_landed_tables(_state(cfg, registered, monkeypatch))

    assert reconciled == [("dq-checker", "pets_scan")]
    assert calls == [
        {
            "schema": "org_acme_replicas",
            "table": "dq-checker__quality__pets_scan",
            "columns": [("check_name", "text"), ("passed", "boolean")],
            "pk_columns": ["check_name"],
        }
    ]


def test_a_checker_source_is_reachable_by_replication_and_has_no_catalog_of_its_own():
    """Trino declares it reaches a checker by fetching (so its results are replicated), and
    registers no catalog for it: there is nothing live to attach, and its replica is read through
    the store's own catalog."""
    from provisa.core import catalog
    from provisa.federation.connector_base import LIVE_IN_PLACE
    from provisa.federation.trino_connectors import build_trino_connectors

    by_type = {c.source_type: c for c in build_trino_connectors()}
    for stype in ("great_expectations", "soda"):
        assert by_type[stype].mechanism not in LIVE_IN_PLACE

        class _Conn:
            def cursor(self):
                raise AssertionError(f"a catalog statement was issued for a {stype} source")

        source = SimpleNamespace(id="dq-checker", type=SimpleNamespace(value=stype))
        catalog.create_catalog(_Conn(), source, "", catalog_name="dq_checker")  # type: ignore[arg-type]


async def no_synthetic_tables(_conn):
    return {}
