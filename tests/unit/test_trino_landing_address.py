# Copyright (c) 2026 Kenneth Stott
# Canary: 3b3c1a52-2f0a-4c4f-9b06-7a9c1d5f7e21
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1443: on Trino the landed replica has to live where the compiler reads it.

Trino's catalog for an adapter-produced source is PG-backed — it resolves
``<catalog>.<schema>.<table>`` straight to a relation in the Postgres materialization store, with no
engine-side view layer to redirect a mangled ``mat`` name. So an adapter-produced source (a
data-quality checker's scan results, an API's fetched pages) lands AT its registered address, while
an engine-scannable source keeps the internal ``mat`` name — landing that one at its registered
address would point the read and the write at one relation.

The reconcile is DDL-only, and it must run: a poll node probes its table's watermark BEFORE the
first land, so an unresolvable relation would fail the probe and prevent the land that would have
created it."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.federation.backend import TrinoBackend
from provisa.federation.engine import build_trino_engine


def _rcol(name, data_type: str | None = "bigint", pk=False, nf=None):
    return {
        "column_name": name,
        "data_type": data_type,
        "is_primary_key": pk,
        "native_filter_type": nf,
    }


def _rtbl(sid, schema, tname, cols):
    return {"source_id": sid, "schema_name": schema, "table_name": tname, "columns": cols}


def _src(sid, stype):
    return SimpleNamespace(id=sid, type=SimpleNamespace(value=stype), change_signal="ttl")


class _FakeConn:
    async def __aenter__(self):
        return object()

    async def __aexit__(self, *a):
        return False


def _state(cfg, registered, monkeypatch):
    async def _fetch_tables(_conn):
        return registered

    monkeypatch.setattr("provisa.api.admin.db_queries.fetch_tables", _fetch_tables)
    return SimpleNamespace(config=cfg, tenant_db=SimpleNamespace(acquire=lambda: _FakeConn()))


def _backend():
    return TrinoBackend(build_trino_engine())


class TestTheLandingAddress:
    def test_an_adapter_produced_source_lands_at_its_registered_address(self):
        assert _backend().landing_target(
            store_schema="mat",
            source_id="dq-checker",
            source_type="great_expectations",
            schema_name="quality",
            table_name="pets_scan",
        ) == ("quality", "pets_scan")

    def test_an_engine_scannable_source_keeps_the_internal_name(self):
        """sqlite is mirrored into Postgres at registration, so its physical address already holds
        the rows the engine reads — landing there would make the node read and write one table."""
        assert _backend().landing_target(
            store_schema="mat",
            source_id="inquiries_sqlite",
            source_type="sqlite",
            schema_name="default",
            table_name="inquiries",
        ) == ("mat", "inquiries_sqlite__default__inquiries")

    def test_the_enum_member_reads_the_same_as_the_bare_string(self):
        assert _backend().landing_target(
            store_schema="mat",
            source_id="dq-checker",
            source_type=SimpleNamespace(value="soda"),
            schema_name="quality",
            table_name="pets_scan",
        ) == ("quality", "pets_scan")


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
    monkeypatch.setattr(type(backend.engine), "materialize_store", lambda _self: "postgresql:///x")
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
            "schema": "quality",
            "table": "pets_scan",
            "columns": [("check_name", "text"), ("passed", "boolean")],
            "pk_columns": ["check_name"],
        }
    ]


def test_a_checker_source_gets_a_trino_catalog():
    """Without a connector, create_catalog skips the source and the catalog name the compiler emits
    resolves to nothing — the CATALOG_NOT_FOUND this fixes."""
    from provisa.federation.trino_connectors import build_trino_connectors

    by_type = {c.source_type: c for c in build_trino_connectors()}
    for stype in ("great_expectations", "soda"):
        assert by_type[stype].trino_connector == "postgresql"
