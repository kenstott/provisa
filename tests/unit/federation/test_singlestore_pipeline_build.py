# Copyright (c) 2026 Kenneth Stott
# Canary: ebdaefc0-6d07-4a33-b416-f914207b5a10
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-990/REQ-848 PIPELINE_LAND: a whole-table replica of an S3 file on a SingleStore store is
built by a one-shot pipeline into the build table, then swapped in; a failed pipeline is torn down
and the build abandoned, never relayed; a dropped replica takes its pipeline and link with it."""

from __future__ import annotations

import asyncio

import pytest

from provisa.federation import singlestore_pipeline as sp
from provisa.federation import singlestore_pipeline_build as spb
from provisa.federation.replica_target import RENAME_PAIR, ROWS_IN_TRANSACTION

_HINTS = {"access_key_id": "AKIA1", "secret_access_key": "s3cr3t", "region": "us-east-1"}


class _Source:
    type = "parquet"
    federation_hints = _HINTS


class _Target:
    """The replica target's build-connection surface, recording what the build runs."""

    def __init__(self, *, fail_on: str | None = None, replace_method: str = ROWS_IN_TRANSACTION):
        self.schema = "org_a_replicas"
        self.table = "s__default__t"
        self.build_table_name = "build__t"
        self.replace_method = replace_method
        self.calls: list = []
        self._fail_on = fail_on

    async def begin(self) -> None:
        self.calls.append("begin")

    async def run_on_build(self, statements, *, tolerate=()):
        self.calls.append(("run", list(statements), tuple(tolerate)))
        if self._fail_on and any(self._fail_on in s for s in statements):
            raise RuntimeError("1933: Cannot get source metadata for pipeline")

    async def build_row_count(self) -> int:
        self.calls.append("count")
        return 42

    async def swap(self) -> None:
        self.calls.append("swap")

    async def abort(self) -> None:
        self.calls.append("abort")


def _build(target: _Target, origin: sp.LandOrigin, **kwargs):
    return asyncio.run(
        spb.build_by_pipeline(
            source=_Source(), origin=origin, target=target, columns=["id", "name"], **kwargs
        )
    )


_PARQUET = sp.LandOrigin("parquet", "s3://bucket/k.parquet", "parquet")


def test_parquet_build_loads_the_build_table_by_pipeline_then_swaps():
    target = _Target()
    outcome = _build(target, _PARQUET)

    schema, table = target.schema, target.table
    pipeline, link = sp.pipeline_name(schema, table), sp.link_name(schema, table)
    finish = sp.teardown(schema, pipeline, link=link)
    assert target.calls == [
        "begin",
        (
            "run",
            [
                finish[0],
                sp.s3_link_ddl(schema, link, _HINTS),
                sp.file_pipeline_ddl(
                    schema=schema,
                    pipeline=pipeline,
                    link=link,
                    origin=_PARQUET,
                    into_table="build__t",
                    columns=["id", "name"],
                ),
                sp.start_foreground(schema, pipeline),
            ],
            (),
        ),
        "count",
        ("run", finish, ()),
        "swap",
    ]
    assert (outcome.rows_copied, outcome.method) == (42, "store_pipeline")


def test_csv_build_maps_the_file_header(monkeypatch):
    monkeypatch.setattr(spb, "csv_header", lambda location, hints: ["name", "id"])
    target = _Target()
    _build(target, sp.LandOrigin("csv", "s3://bucket/k.csv", "csv"))
    statements = target.calls[1][1]
    assert statements[2].endswith("IGNORE 1 LINES (`name`, `id`)")


def test_a_failed_pipeline_is_torn_down_and_the_build_abandoned_never_relayed():
    target = _Target(fail_on="FOREGROUND")
    with pytest.raises(RuntimeError, match="Cannot get source metadata"):
        _build(target, _PARQUET)
    schema, table = target.schema, target.table
    finish = sp.teardown(schema, sp.pipeline_name(schema, table), link=sp.link_name(schema, table))
    assert target.calls[-2:] == [("run", finish, (sp.NO_SUCH_LINK,)), "abort"]
    assert "swap" not in target.calls


@pytest.mark.parametrize(
    ("target", "origin", "kwargs", "message"),
    [
        (_Target(), _PARQUET, {"region_filter": "region = 'eu'"}, "needs a region filter"),
        (
            _Target(),
            sp.LandOrigin("iceberg", "s3://b/warehouse/t", "iceberg"),
            {},
            "enable_iceberg_ingest",
        ),
        (_Target(replace_method=RENAME_PAIR), _PARQUET, {}, "unkeyed build table"),
    ],
)
def test_refusals_are_named_before_anything_runs(target, origin, kwargs, message):
    with pytest.raises(sp.PipelineRefused, match=message):
        _build(target, origin, **kwargs)
    assert target.calls == []


class _Cursor:
    def __init__(self, raw) -> None:
        self._raw = raw

    def execute(self, sql: str) -> None:
        self._raw.executed.append(sql)
        if sql.startswith("DROP LINK"):
            exc = RuntimeError("No link with name exists")
            exc.errno = sp.NO_SUCH_LINK  # type: ignore[attr-defined]
            raise exc

    def close(self) -> None:
        pass


class _Raw:
    def __init__(self) -> None:
        self.executed: list[str] = []
        self.commits = 0

    def cursor(self) -> _Cursor:
        return _Cursor(self)

    def commit(self) -> None:
        self.commits += 1


def test_dropping_a_singlestore_replica_drops_its_pipeline_and_link_absent_or_not(monkeypatch):
    from contextlib import contextmanager

    from sqlalchemy import create_engine

    from provisa.federation.replica_target import SqlAlchemyStoreTarget

    raw = _Raw()
    conn = type("_Conn", (), {"connection": type("_F", (), {"driver_connection": raw})()})()

    @contextmanager
    def _ctx():
        yield conn

    target = SqlAlchemyStoreTarget(
        create_engine("singlestoredb://u:p@localhost:1/d"),
        schema="org_a_replicas",
        table="s__default__t",
        columns=[("id", "integer")],
        pk_columns=["id"],
    )
    target._sa = type("_Sa", (), {"begin": staticmethod(_ctx), "connect": staticmethod(_ctx)})()
    monkeypatch.setattr(target, "_drop_if_present", lambda c, name: None)

    target._drop()  # the absent link (error 2332) is passed over

    assert raw.executed == sp.teardown(
        "org_a_replicas",
        sp.pipeline_name("org_a_replicas", "s__default__t"),
        link=sp.link_name("org_a_replicas", "s__default__t"),
    )
    assert raw.commits == 1


def _routing_state(monkeypatch, path: str, seen: dict):
    from contextlib import asynccontextmanager
    from types import SimpleNamespace

    from provisa.federation import replica_builds, replica_state
    from provisa.federation.data_replicator import BuildOutcome
    from provisa.federation.engine import FederationEngine, build_sqlalchemy_engine

    monkeypatch.setattr(
        FederationEngine, "materialize_store", lambda self: "singlestoredb://u:p@h:3306/db"
    )

    @asynccontextmanager
    async def _held(*args, **kwargs):
        yield SimpleNamespace()

    async def _record_started(conn, key, **kwargs):
        seen["record"] = kwargs

    async def _build(**kwargs):
        seen["build"] = kwargs
        return BuildOutcome(rows_copied=3, method="store_pipeline")

    monkeypatch.setattr("provisa.federation.source_vault.org_vault", _held)
    monkeypatch.setattr("provisa.events.land_lock.land_lock", _held)
    monkeypatch.setattr(replica_state, "record_started", _record_started)
    monkeypatch.setattr(spb, "build_by_pipeline", _build)

    bare = build_sqlalchemy_engine("singlestoredb://u:p@h:3306/db")
    backend = SimpleNamespace(replica_target=lambda state, **kw: "the-target")
    monkeypatch.setattr(FederationEngine, "backend", property(lambda self: backend))
    state = SimpleNamespace(
        federation_engine=SimpleNamespace(engine=bare),
        tenant_db=SimpleNamespace(acquire=_held),
    )
    source = SimpleNamespace(id="s", type="parquet", path=path, federation_hints=_HINTS)
    args = SimpleNamespace(columns=[("id", "bigint")], pk_columns=["id"])
    address = SimpleNamespace(schema="org_a_replicas", table="s__default__t")
    return replica_builds, state, source, args, address


def test_a_singlestore_engine_builds_an_s3_file_by_pipeline(monkeypatch):
    seen: dict = {}
    replica_builds, state, source, args, address = _routing_state(
        monkeypatch, "s3://b/k.parquet", seen
    )
    outcome = asyncio.run(
        replica_builds._build_by_pipeline(
            state, ("s", "default", "t"), source, None, [source], args, address
        )
    )
    assert outcome is not None and outcome.rows_copied == 3
    assert seen["record"] == {"method": "store_pipeline", "load_kind": "bulk_stream"}
    assert seen["build"]["target"] == "the-target"
    assert seen["build"]["origin"] == sp.LandOrigin("parquet", "s3://b/k.parquet", "parquet")


def test_a_local_file_on_a_singlestore_engine_keeps_the_relay(monkeypatch):
    seen: dict = {}
    replica_builds, state, source, args, address = _routing_state(
        monkeypatch, "/data/k.parquet", seen
    )
    outcome = asyncio.run(
        replica_builds._build_by_pipeline(
            state, ("s", "default", "t"), source, None, [source], args, address
        )
    )
    assert outcome is None and seen == {}
