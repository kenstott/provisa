# Copyright (c) 2026 Kenneth Stott
# Canary: 9d2f4b71-3e85-4c06-b8a1-6f0e2c47d935
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""ClickHouse reads a dev-mode code source live through its pgwire sidecar (REQ-2267 part 3):
the engine reaches a dev-mode source by its code-host attach and a built one by its type's land
reach, since it cannot present the TLS identity a built host accepts; a built source's table is
landed; and the runtime drops what it mounted for a source when the source's sidecar stops for
good, so the next attach dials the sidecar as it is then. A real ClickHouse read of a running
sidecar is in tests/unit/test_code_pgwire_sidecar.py."""

# Requirements: REQ-2267
from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.codesource import attach
from provisa.core.models import SourceType
from provisa.federation.clickhouse_connectors import ClickHouseCodeHostConnector
from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime
from provisa.federation.connector_base import Mechanism
from provisa.federation.engine import build_clickhouse_engine
from provisa.federation.replica_address import ReplicaRoutes

pytestmark = pytest.mark.unit


def _source(mode: str):
    return SimpleNamespace(
        id="scores-src",
        type=SourceType("python"),
        mapping={"mode": mode},
        federation_hints={},
    )


def test_clickhouse_attaches_a_dev_source_and_lands_a_built_one():
    engine = build_clickhouse_engine()
    code_host = engine.scan_connectors["python"]
    assert isinstance(code_host, ClickHouseCodeHostConnector)
    assert engine.connector_for_source(_source("dev")) is code_host
    landed = engine.connector_for_source(_source("built"))
    assert landed is engine.connectors["python"]
    assert landed.mechanism in (Mechanism.DIRECT, Mechanism.FETCH)  # its rows, landed


def _state(mode: str):
    connector = ClickHouseCodeHostConnector("python")
    return SimpleNamespace(
        source_types={"scores-src": "python"},
        runtime_sources={"scores-src": {"mapping": {"mode": mode}}},
        federation_engine=SimpleNamespace(
            engine=SimpleNamespace(scan_connectors={"python": connector})
        ),
        replica_routes=ReplicaRoutes(floored={}),
        tables=[],
    )


_TABLE = {
    "id": 4,
    "source_id": "scores-src",
    "schema_name": "default",
    "table_name": "scores",
    "cache_ttl": None,
    "columns": [],
}


def test_a_dev_sources_table_is_attached_on_clickhouse_and_a_built_ones_landed():
    assert attach.attached(_state("dev"), _TABLE)
    assert not attach.attached(_state("built"), _TABLE)


class _Backend:
    def __init__(self) -> None:
        self.commands: list[str] = []

    def command(self, sql: str) -> None:
        self.commands.append(sql)


def _runtime(backend: _Backend) -> ClickHouseFederationRuntime:
    rt = ClickHouseFederationRuntime(backend)  # type: ignore[arg-type] -- a recording backend
    details = {
        "attach_ddl": [
            'CREATE DATABASE IF NOT EXISTS "ch_org_acme__scores_src" ENGINE = PostgreSQL('
            "'127.0.0.1:15433', 'code', 'provisa', '', 'public')"
        ],
        "local_schema": "ch_org_acme__scores_src",
    }
    rt._engine = SimpleNamespace(  # type: ignore[assignment] -- resolves to ``details``
        resolve=lambda source: SimpleNamespace(details=details)
    )
    return rt


def _table(source_id: str, table: str):
    return SimpleNamespace(
        id=source_id,
        catalog=f"org_acme__{source_id.replace('-', '_')}",
        schema_name="default",
        table_name=table,
    )


def test_the_runtime_drops_what_it_mounted_for_a_source_and_mounts_it_again():
    backend = _Backend()
    rt = _runtime(backend)
    rt.attach_source(_table("scores-src", "scores"))
    rt.attach_source(_table("scores-src", "runs"))
    rt.attach_source(_table("other-src", "x"))
    backend.commands.clear()
    rt.drop_source("scores-src")
    assert backend.commands == [
        'DROP VIEW IF EXISTS "org_acme__scores_src_default"."runs"',
        'DROP VIEW IF EXISTS "org_acme__scores_src_default"."scores"',
        'DROP DATABASE IF EXISTS "ch_org_acme__scores_src"',
    ]
    backend.commands.clear()
    rt.drop_source("scores-src")  # dropped already: nothing left to drop
    assert backend.commands == []
    # Attached again, its database is mounted again: the sidecar as it is now.
    rt.attach_source(_table("scores-src", "scores"))
    assert any(c.startswith('CREATE DATABASE IF NOT EXISTS "ch_org') for c in backend.commands)


# -- what the sidecar answers ClickHouse's PostgreSQL engine with (pgfront, pgcatalog) ----------


@pytest.fixture
def pgfront(monkeypatch):
    """pgfront as a host or a sidecar loads it: its directory first on the path."""
    import importlib
    from pathlib import Path

    from provisa.codehost import front

    monkeypatch.syspath_prepend(str(Path(front.__file__).parent))
    return importlib.import_module("pgfront")


@pytest.mark.parametrize(
    "options, fmt",
    [
        (None, 0),  # ClickHouse's: no options, text
        ("FORMAT text", 0),
        ('FORMAT "binary"', 1),  # DuckDB's postgres scanner
        ("FORMAT binary", 1),
        ("binary", 1),
        ("FORMAT csv", None),  # not answered
    ],
)
def test_a_copy_is_answered_in_the_format_its_options_ask_for(pgfront, options, fmt):
    assert pgfront._copy_format(options) == fmt


def test_a_text_copy_line_escapes_its_fields_and_writes_a_null_as_backslash_n(pgfront):
    import struct

    fields = [b"plain", None, b"a\tb\nc\\d\re"]
    body = struct.pack("!h", len(fields)) + b"".join(
        struct.pack("!i", -1) if f is None else struct.pack("!i", len(f)) + f for f in fields
    )
    assert pgfront._text_line(body) == b"plain\t\\N\ta\\tb\\nc\\\\d\\re\n"


def test_the_catalog_answers_current_setting_and_format_type_as_postgresql_does():
    import duckdb

    from provisa.codehost import pgcatalog

    db = duckdb.connect()
    names = {
        "orders": {
            "columns": [
                {"name": "id", "type": "bigint"},
                {"name": "score", "type": "double"},
                {"name": "ok", "type": "boolean"},
                {"name": "at", "type": "timestamp"},
                {"name": "tags", "type": "list"},
            ],
            "parameters": [],
        }
    }
    pgcatalog.build(db, names, ("public",))

    def answer(sql: str) -> list[tuple]:
        return db.execute(
            pgcatalog.rewrite(pgcatalog.parse(sql), ("public",)).sql("duckdb")
        ).fetchall()

    assert answer("SELECT current_setting('server_version_num')::int") == [(160000,)]
    assert answer(
        "SELECT attname, format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid = "
        "(SELECT oid FROM pg_class WHERE relname = 'orders') ORDER BY attnum"
    ) == [
        ("id", "bigint"),
        ("score", "double precision"),
        ("ok", "boolean"),
        ("at", "timestamp with time zone"),
        ("tags", "text[]"),
    ]
