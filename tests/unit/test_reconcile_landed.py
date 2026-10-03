# Copyright (c) 2026 Kenneth Stott
# Canary: c8450386-959f-4308-a730-155f9c00b413
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-846/932: the schema-currency controller — reconcile_landed_tables converges the store's
replica of every table served from one, at its replica address (REQ-1912), and skips still-untyped
ones. Nothing is created at the table's registered name.

Drives off the design-time REGISTERED tables (control plane: semantic sql names + resolved types),
not the raw YAML — so the test feeds the registered shape through a fake ``fetch_tables``."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.federation.duckdb_backend import DuckDBBackend
from provisa.federation.engine import build_duckdb_engine
from tests.helpers import no_engine_store, no_promoted_tables


def _rcol(name, data_type: str | None = "bigint", pk=False, nf=None):
    return {
        "column_name": name,
        "data_type": data_type,
        "is_primary_key": pk,
        "native_filter_type": nf,
    }


def _rtbl(sid, tname, cols):
    return {
        "source_id": sid,
        "schema_name": "default",
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


class _FakeRuntime:
    def __init__(self):
        self.landed: list = []

    def attach_source(self, source):  # exercised by _attach_registered — no-op record
        pass

    async def reconcile_replica(self, *, schema, table, columns, pk_columns=None):
        self.landed.append((schema, table, columns, pk_columns))
        return "created"

    def ensure_materialize_attached(self) -> str:
        return "mat_store"


class _FakeConn:
    async def __aenter__(self):
        return object()

    async def __aexit__(self, *a):
        return False


def _fake_tenant_db():
    return SimpleNamespace(acquire=lambda: _FakeConn())


def _state(cfg, registered, monkeypatch):
    async def _fetch_tables(_conn):
        return registered

    monkeypatch.setattr("provisa.api.admin.db_queries.fetch_tables", _fetch_tables)
    monkeypatch.setattr("provisa.federation.replica_state.promotion", no_promoted_tables)
    monkeypatch.setattr("provisa.federation.replica_builds.store_identity", no_engine_store)

    async def _no_ui_sources(_conn):  # REQ-1674: the registry view also lists UI-created sources
        return []

    monkeypatch.setattr("provisa.core.repositories.source.list_all", _no_ui_sources)
    return SimpleNamespace(config=cfg, tenant_db=_fake_tenant_db(), org_id="acme")


@pytest.mark.asyncio
async def test_reconciles_only_materialized_and_skips_untyped(monkeypatch):
    backend = DuckDBBackend(build_duckdb_engine())
    rt = _FakeRuntime()
    backend._runtime = rt  # inject fake runtime (skip real duckdb build)
    cfg = SimpleNamespace(sources=[_src("api", "openapi"), _src("pg", "postgresql")], tables=[])
    registered = [
        _rtbl("api", "events", [_rcol("id", "bigint", pk=True), _rcol("status", "text")]),
        _rtbl("pg", "users", [_rcol("id", "bigint", pk=True)]),  # ATTACH → VIRTUAL, not landed
        _rtbl("api", "bad", [_rcol("id", None)]),  # MATERIALIZED but untyped → skipped
        # FULLY PARAMETERIZED (every column a native-filter arg) → a function, no snapshot at
        # all → never materialized.
        _rtbl("api", "one", [_rcol("_nf_key", "text", nf="query_param")]),
        # REQ-1742: a MIX of native-filter arg + real data columns (e.g. grpc_remote's map_proto
        # adding a synthetic "_nf_limit" column alongside a method's genuine output columns) DOES
        # land — just the real columns, with the parameter column filtered out first.
        _rtbl(
            "api",
            "mixed",
            [_rcol("_nf_key", "text", nf="query_param"), _rcol("val", "text")],
        ),
    ]
    reconciled = await backend.reconcile_landed_tables(_state(cfg, registered, monkeypatch))

    assert reconciled == [("api", "events"), ("api", "mixed")]
    # REQ-1912: each at its replica address — the org's replicas schema, the one replica name
    assert rt.landed == [
        (
            "org_acme_replicas",
            "api__default__events",
            [("id", "bigint"), ("status", "text")],
            ["id"],
        ),
        ("org_acme_replicas", "api__default__mixed", [("val", "text")], []),
    ]


@pytest.mark.asyncio
async def test_no_config_is_noop():
    backend = DuckDBBackend(build_duckdb_engine())
    backend._runtime = _FakeRuntime()
    assert await backend.reconcile_landed_tables(SimpleNamespace()) == []


class _KeyedRuntime(_FakeRuntime):
    """A store that holds informational constraints: exposes the key hook (REQ-1652)."""

    def __init__(self):
        super().__init__()
        self.plans: list = []

    async def reconcile_landed_metadata(self, plan):
        self.plans.append(plan)
        return len(plan.edges) + sum(1 for t in plan.tables.values() if t.primary_key)


@pytest.mark.asyncio
async def test_keys_converge_with_the_tables_from_registration_and_relationships(monkeypatch):
    # REQ-1652: keys are part of the landed model's shape. The reconcile hands the runtime a plan
    # built from the registered keys and the relationships, for every landed table -- no Data
    # Product filter -- and withholds a key the store cannot hold.
    backend = DuckDBBackend(build_duckdb_engine())
    rt = _KeyedRuntime()
    backend._runtime = rt
    cfg = SimpleNamespace(sources=[_src("api", "openapi")], tables=[])
    registered = [
        {"id": 1, **_rtbl("api", "pets", [_rcol("id", "bigint", pk=True), _rcol("breed", "text")])},
        {
            "id": 2,
            **_rtbl("api", "visits", [_rcol("id", "bigint", pk=True), _rcol("pet_id", "bigint")]),
        },
    ]
    rels = [
        {
            "id": "visits-pet",
            "source_table_id": 2,
            "target_table_id": 1,
            "source_column": "pet_id",
            "target_column": "id",
            "cardinality": "many-to-one",
            "via_table_id": None,
        },
        {
            "id": "breed-link",
            "source_table_id": 2,
            "target_table_id": 1,
            "source_column": "breed",
            "target_column": "breed",
            "cardinality": "many-to-one",
            "via_table_id": None,
        },
    ]

    async def _rels(_conn):
        return rels

    registered[0]["description"] = "Pets for sale"
    registered[0]["columns"][1]["description"] = "Breed of the pet"
    monkeypatch.setattr("provisa.core.repositories.relationship.list_all", _rels)
    reconciled = await backend.reconcile_landed_tables(_state(cfg, registered, monkeypatch))

    assert reconciled == [("api", "pets"), ("api", "visits")]
    plan = rt.plans[0]
    assert set(plan.tables) == {"api.default.pets", "api.default.visits"}
    # REQ-1654: the descriptions ride the same plan, for every landed table
    assert plan.tables["api.default.pets"].description == "Pets for sale"
    assert plan.tables["api.default.pets"].column_descriptions == {"breed": "Breed of the pet"}
    assert [
        (e.holder, e.holder_columns, e.referenced, e.referenced_columns) for e in plan.edges
    ] == [("api.default.visits", ("pet_id",), "api.default.pets", ("id",))]
    # REQ-1912: the plan carries each replica's store address; none is at its registered name
    assert plan.store_parts == {
        "api.default.pets": ("mat_store", "org_acme_replicas", "api__default__pets"),
        "api.default.visits": ("mat_store", "org_acme_replicas", "api__default__visits"),
    }
    assert plan.withheld[0][0] == "provisa_fk_breed_link"
    assert "not api.default.pets's primary key" in plan.withheld[0][1]


class _PublishingRuntime(_KeyedRuntime):
    """A store whose catalog export shares a view of each replica (Snowflake)."""

    def __init__(self):
        super().__init__()
        self.views: list = []

    async def publish_replica_view(self, *, view_schema, view_table, schema, table, replace):
        self.views.append(((view_schema, view_table), (schema, table), replace))


@pytest.mark.asyncio
async def test_a_published_view_of_a_replica_lives_in_the_export_schema(monkeypatch):
    # REQ-1912: the view a store publishes over a replica for its catalog export is in a schema
    # of its own — not at the table's registered address ("default"."pets"), not in the replicas
    # schema a read is addressed to — and the metadata plan names that same address for it.
    backend = DuckDBBackend(build_duckdb_engine())
    rt = _PublishingRuntime()
    backend._runtime = rt
    cfg = SimpleNamespace(sources=[_src("api", "openapi")], tables=[])
    registered = [{"id": 1, **_rtbl("api", "pets", [_rcol("id", "bigint", pk=True)])}]

    async def _rels(_conn):
        return []

    monkeypatch.setattr("provisa.core.repositories.relationship.list_all", _rels)
    await backend.reconcile_landed_tables(_state(cfg, registered, monkeypatch))

    assert rt.views == [
        (
            ("org_acme_export", "api__default__pets"),
            ("org_acme_replicas", "api__default__pets"),
            False,
        )
    ]
    plan = rt.plans[0]
    assert plan.view_parts == {
        "api.default.pets": ("mat_store", "org_acme_export", "api__default__pets")
    }
    from provisa.federation.landed_keys import plan_targets

    target = plan_targets(plan)["api.default.pets"]
    assert target.view == ("mat_store", "org_acme_export", "api__default__pets")
    assert target.replica == ("mat_store", "org_acme_replicas", "api__default__pets")


@pytest.mark.asyncio
async def test_a_catalog_export_is_handed_the_addresses_the_reconcile_published(monkeypatch):
    # REQ-1912: the export addresses a replica-served table by the same rule, over the same
    # tables, as the reconcile publishes views for — so it finds what was created.
    from provisa.federation.replica_routing import export_view_addresses

    backend = DuckDBBackend(build_duckdb_engine())
    rt = _PublishingRuntime()
    backend._runtime = rt
    cfg = SimpleNamespace(sources=[_src("api", "openapi"), _src("pg", "postgresql")], tables=[])
    registered = [
        {"id": 1, **_rtbl("api", "pets", [_rcol("id", "bigint", pk=True)])},
        {"id": 2, **_rtbl("pg", "orders", [_rcol("id", "bigint", pk=True)])},  # read live
    ]

    async def _rels(_conn):
        return []

    monkeypatch.setattr("provisa.core.repositories.relationship.list_all", _rels)
    state = _state(cfg, registered, monkeypatch)
    state.federation_engine = SimpleNamespace(engine=backend.engine)
    await backend.reconcile_landed_tables(state)

    handed = await export_view_addresses(state)

    assert handed == {("api", "default", "pets"): ("org_acme_export", "api__default__pets")}
    assert [view for view, _replica, _replace in rt.views] == list(handed.values())


@pytest.mark.asyncio
async def test_a_runtime_that_publishes_no_view_has_none_in_its_plan(monkeypatch):
    backend = DuckDBBackend(build_duckdb_engine())
    rt = _KeyedRuntime()
    backend._runtime = rt
    cfg = SimpleNamespace(sources=[_src("api", "openapi")], tables=[])
    registered = [{"id": 1, **_rtbl("api", "pets", [_rcol("id", "bigint", pk=True)])}]

    async def _rels(_conn):
        return []

    monkeypatch.setattr("provisa.core.repositories.relationship.list_all", _rels)
    await backend.reconcile_landed_tables(_state(cfg, registered, monkeypatch))

    from provisa.federation.landed_keys import plan_targets

    assert rt.plans[0].view_parts == {}
    assert plan_targets(rt.plans[0])["api.default.pets"].view is None


@pytest.mark.asyncio
async def test_a_runtime_without_the_key_hook_gets_no_key_plan(monkeypatch):
    # An enforcing store (DuckDB/Postgres): a FOREIGN KEY would refuse every REPLACE land, so the
    # tables reconcile and the keys stay out, by design.
    backend = DuckDBBackend(build_duckdb_engine())
    backend._runtime = _FakeRuntime()
    cfg = SimpleNamespace(sources=[_src("api", "openapi")], tables=[])
    registered = [{"id": 1, **_rtbl("api", "pets", [_rcol("id", "bigint", pk=True)])}]
    assert await backend.reconcile_landed_tables(_state(cfg, registered, monkeypatch)) == [
        ("api", "pets")
    ]
