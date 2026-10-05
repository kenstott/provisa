# Copyright (c) 2026 Kenneth Stott
# Canary: c15789cd-37b4-4a09-be60-0199d776ee5c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-990: with SingleStore as the engine's own store, every bulk write face streams one LOAD DATA
LOCAL INFILE — the replica build's batch write and the engine's own REPLACE/APPEND land — never
SQLAlchemy's executemany. A refused LOCAL INFILE raises by name."""

from __future__ import annotations

from collections.abc import Iterable

import pytest
from sqlalchemy import create_engine

from provisa.federation.replica_target import LOAD_SINGLESTORE_INFILE, SqlAlchemyStoreTarget

_COLUMNS = [("id", "integer"), ("name", "varchar"), ("payload", "json")]
_ROWS = [
    {"id": 1, "name": "a\tb", "payload": '{"k": 1}'},
    {"id": 2, "name": None, "payload": None},
]


class _Raw:
    """The singlestoredb client connection: ``query`` with an in-memory infile stream."""

    def __init__(self, refuse: str | None = None) -> None:
        self.statements: list[str] = []
        self.streamed = b""
        self.commits = 0
        self._refuse = refuse

    def query(self, sql: str, infile_stream: Iterable[bytes] = ()) -> None:
        self.statements.append(sql)
        if self._refuse:
            raise RuntimeError(self._refuse)
        self.streamed += b"".join(infile_stream)

    def commit(self) -> None:
        self.commits += 1


class _NoExecutemany:
    """A SQLAlchemy connection that fails the test if anything is sent through ``execute``."""

    def __init__(self, raw: _Raw, dialect) -> None:
        self.connection = type("_Fairy", (), {"driver_connection": raw})()
        self.dialect = dialect

    def execute(self, *args, **kwargs):  # pragma: no cover — reaching it is the failure
        raise AssertionError("a SingleStore bulk write went through executemany")

    def commit(self) -> None:
        pass


def _built_target(raw: _Raw) -> SqlAlchemyStoreTarget:
    from provisa.federation.materialize_exec import _json_columns, temporal_columns

    engine = create_engine("singlestoredb://u:p@localhost:1/d")  # opens no connection
    target = SqlAlchemyStoreTarget(
        engine,
        schema="org_a_replicas",
        table="s__public__t",
        columns=_COLUMNS,
        pk_columns=["id"],
    )
    target._build_table = target._core_table(target._build, keyed=False)
    target._json = _json_columns(target._build_table)
    target._temporal = temporal_columns(_COLUMNS)
    target._conn = _NoExecutemany(raw, engine.dialect)
    return target


def test_replica_build_batch_streams_load_data_into_the_build_table():
    raw = _Raw()
    target = _built_target(raw)
    assert target.load_method == LOAD_SINGLESTORE_INFILE

    target._write(_ROWS)

    assert raw.statements == [
        f"LOAD DATA LOCAL INFILE ':stream:' INTO TABLE `org_a_replicas`.`{target._build}` "
        "(`id`, `name`, `payload`)"
    ]
    # Tab escaped, NULL is \N, the JSON text parsed then re-serialized once (not double-encoded).
    assert raw.streamed == b'1\ta\\tb\t{"k": 1}\n2\t\\N\t\\N\n'
    assert raw.commits == 1


def test_replica_build_streams_load_data_whatever_driver_the_connected_dialect_names():
    # Before it connects the singlestoredb dialect names no driver; once connected it names the
    # wire protocol ("mysql", seen live). The load is the dialect's either way.
    engine = create_engine("singlestoredb://u:p@localhost:1/d")
    for driver in ("", "mysql"):
        engine.dialect.driver = driver
        target = SqlAlchemyStoreTarget(
            engine, schema="org_a_replicas", table="t", columns=_COLUMNS, pk_columns=["id"]
        )
        assert target.load_method == LOAD_SINGLESTORE_INFILE


def test_replica_build_refused_local_infile_raises_by_name():
    target = _built_target(_Raw(refuse="Loading local data is disabled; local_infile is off"))
    with pytest.raises(RuntimeError, match="LOAD DATA LOCAL INFILE is not permitted"):
        target._write(_ROWS)


def test_engine_land_bulk_insert_streams_load_data_on_singlestore():
    from provisa.federation.materialize_exec import build_table
    from provisa.federation.sqlalchemy_runtime import _sync_bulk_insert

    engine = create_engine("singlestoredb://u:p@localhost:1/d")
    table = build_table("db", "t", _COLUMNS, ("id",), dialect_name="singlestoredb")
    raw = _Raw()

    _sync_bulk_insert(
        _NoExecutemany(raw, engine.dialect), table, [{"id": 7, "name": "", "payload": None}]
    )

    assert raw.statements == [
        "LOAD DATA LOCAL INFILE ':stream:' INTO TABLE `db`.`t` (`id`, `name`, `payload`)"
    ]
    assert raw.streamed == b"7\t\t\\N\n"  # empty string stays distinct from NULL


def test_engine_runtime_enables_local_infile_on_a_singlestore_store(monkeypatch):
    import sqlalchemy

    from provisa.federation.sqlalchemy_runtime import SqlAlchemyFederationRuntime

    seen: dict[str, dict] = {}

    class _Engine:
        def raw_connection(self):
            return object()

    def _create_engine(url, **kwargs):
        seen[str(url).split(":", 1)[0]] = kwargs
        return _Engine()

    monkeypatch.setattr(sqlalchemy, "create_engine", _create_engine)
    SqlAlchemyFederationRuntime(url="singlestoredb://u:p@h:3306/db")
    SqlAlchemyFederationRuntime(url="mysql+pymysql://u:p@h:3306/db")

    assert seen["singlestoredb"]["connect_args"] == {"local_infile": True}
    assert seen["mysql+pymysql"]["connect_args"] == {}
