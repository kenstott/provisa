# Copyright (c) 2026 Kenneth Stott
# Canary: 7e3a5b19-c2d4-4f68-8a91-0d6b2e4c7f35
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1998 (MOTHERDUCK LANDS THROUGH ITS POSTGRES ENDPOINT): a MotherDuck replica lands through
MotherDuck's own Postgres-wire endpoint with the source's token, read by Provisa's existing
Postgres client (psycopg), a bounded batch at a time, into Arrow of the registered types."""

# Requirements: REQ-1998

from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import pyarrow as pa
import pytest

from provisa.events import motherduck_reader
from provisa.events.motherduck_reader import (
    connection_settings,
    make_motherduck_loader,
    select_sql,
)
from provisa.executor.drivers.postgresql import stream_rows

TOKEN = "md-token-that-must-not-leak"
HOST = "pg.us-east-1-aws.motherduck.com"


def _source(**kw):
    base = dict(
        id="md",
        type=SimpleNamespace(value="motherduck"),
        host=HOST,
        port=0,
        database="analytics",
        password=TOKEN,
    )
    return SimpleNamespace(**{**base, **kw})


def _table():
    return SimpleNamespace(
        schema_name="main",
        table_name="orders",
        columns=[
            SimpleNamespace(name="id", native_filter_type=None),
            SimpleNamespace(name="placed", native_filter_type=None),
            SimpleNamespace(name="_nf_q", native_filter_type="query_arg"),
        ],
        landing_filter=None,
    )


def test_the_endpoint_is_signed_in_to_with_the_sources_token_over_verified_tls():
    settings = connection_settings(_source())
    assert settings["host"] == HOST and settings["port"] == 5432
    assert settings["user"] == "postgres" and settings["password"] == TOKEN
    assert settings["dbname"] == "analytics"
    assert settings["sslmode"] == "verify-full" and settings["sslrootcert"] == "system"
    assert settings["options"] == "--attach_mode=single"


def test_the_token_is_resolved_with_the_standard_resolution(monkeypatch):
    monkeypatch.setenv("J866_MD_TOKEN", TOKEN)
    assert connection_settings(_source(password="${env:J866_MD_TOKEN}"))["password"] == TOKEN


@pytest.mark.parametrize(
    ("field", "said"),
    [
        ("host", "Postgres endpoint.*pg.<region>-aws.motherduck.com"),
        ("password", "token"),
        ("database", "name the MotherDuck database"),
    ],
)
def test_a_source_missing_what_the_endpoint_needs_is_refused_by_name(field, said):
    with pytest.raises(ValueError, match=f"'md'.*{said}") as refused:
        connection_settings(_source(**{field: ""}))
    assert TOKEN not in str(refused.value)


def test_the_read_names_the_registered_columns_quoted():
    sql = select_sql(_table(), ["id", 'we"ird'])
    assert sql.as_string(None) == 'SELECT "id", "we""ird" FROM "main"."orders"'


class _Cursor:
    def __init__(self, rows, names) -> None:
        self._rows = rows
        self.description = [SimpleNamespace(name=n) for n in names]
        self.streamed: list[tuple] = []

    def stream(self, sql, size):
        self.streamed.append((sql, size))
        yield from self._rows


class _Connection:
    def __init__(self, cursor) -> None:
        self._cursor = cursor
        self.closed = False

    def cursor(self):
        return self._cursor

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.closed = True


def _connect(monkeypatch, rows, names=("id", "placed")):
    import psycopg

    opened: list[dict] = []
    cursor = _Cursor(rows, names)
    connection = _Connection(cursor)

    def connect(**settings):
        opened.append(settings)
        return connection

    monkeypatch.setattr(psycopg, "connect", connect)
    return opened, cursor, connection


def test_rows_stream_off_the_connection_a_bounded_batch_at_a_time(monkeypatch):
    rows = [(i, date(2024, 1, i)) for i in range(1, 6)]
    opened, cursor, connection = _connect(monkeypatch, rows)
    settings = connection_settings(_source())
    batches = list(stream_rows(settings, select_sql(_table(), ["id", "placed"]), 2))
    assert [len(b) for b in batches] == [2, 2, 1]
    assert batches[0][0] == {"id": 1, "placed": date(2024, 1, 1)}
    assert cursor.streamed[0][1] == 2
    assert opened == [settings] and connection.closed


@pytest.mark.asyncio
async def test_a_replica_build_reads_arrow_of_the_registered_types(monkeypatch):
    _connect(monkeypatch, [(1, date(2024, 1, 1)), (2, None)])
    source = make_motherduck_loader().replica_source(
        _source(), _table(), [("id", "bigint"), ("placed", "date")]
    )
    table = pa.Table.from_batches([b async for b in source.batches(1000)])
    assert table.schema.types == [pa.int64(), pa.date32()]
    assert table.to_pylist() == [
        {"id": 1, "placed": date(2024, 1, 1)},
        {"id": 2, "placed": None},
    ]


@pytest.mark.asyncio
async def test_a_whole_load_reads_the_registered_data_columns(monkeypatch):
    monkeypatch.setattr(motherduck_reader, "LOAD_BATCH_ROWS", 1)
    _, cursor, _ = _connect(monkeypatch, [(1, date(2024, 1, 1)), (2, date(2024, 1, 2))])
    rows = await make_motherduck_loader()(_source(), _table())
    assert rows == [{"id": 1, "placed": date(2024, 1, 1)}, {"id": 2, "placed": date(2024, 1, 2)}]
    assert cursor.streamed[0][0].as_string(None) == 'SELECT "id", "placed" FROM "main"."orders"'


def test_the_loader_is_wired_where_the_engine_cannot_attach_motherduck():
    from provisa.events.app_wiring import build_adapter_loaders
    from provisa.federation.connector import Mechanism

    state = SimpleNamespace(config=SimpleNamespace(sources=[]))
    landed = SimpleNamespace(
        engine=SimpleNamespace(
            connectors={
                "motherduck": SimpleNamespace(mechanism=Mechanism.FETCH, reads_in_place=False)
            }
        )
    )
    assert "motherduck" in build_adapter_loaders(state, landed)
    attached = SimpleNamespace(
        engine=SimpleNamespace(
            connectors={
                "motherduck": SimpleNamespace(mechanism=Mechanism.ATTACH_RW, reads_in_place=True)
            }
        )
    )
    assert "motherduck" not in build_adapter_loaders(state, attached)
