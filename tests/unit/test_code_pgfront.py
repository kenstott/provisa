# Copyright (c) 2026 Kenneth Stott
# Canary: 0411303f-1f71-4320-a7fa-a8942641cc6e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A built host's PostgreSQL face (REQ-2100): the front end, with the Python host program as its
worker, answers a PostgreSQL client over TLS that presents the paired Provisa's certificate: the
catalog lists each table function with its declared columns and parameters, the planner's row
estimate is the count last answered, and a read is the function called with its parameters and
pushed values, the statement applied to its rows. A client with another certificate is refused."""

# Requirements: REQ-2100
from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
import time
from pathlib import Path
from types import SimpleNamespace

import psycopg
import pytest

from provisa.codehost import front
from provisa.codesource import remote
from provisa.codesource.manifest import PYTHON_RULES, read_manifest
from provisa.codesource.names import name_tables
from provisa.python.source import HOST, READER

pytestmark = pytest.mark.unit

_CODE = textwrap.dedent(
    '''
    from dataclasses import dataclass


    @dataclass
    class Order:
        id: int
        customerName: str
        asked: str


    def orders(n: int = 3, customerName: str | None = None) -> list[Order]:
        """Orders.

        @table
        """
        asked = f"n={n};customerName={customerName}"
        return [Order(i, "acme" if i % 2 else "zeta", asked) for i in range(n)]
    '''
)


@pytest.fixture
def host(tmp_path):
    """A front end on a free port, paired; its port and the paired identity's files."""
    code = tmp_path / "code"
    code.mkdir()
    (code / "shop.py").write_text(_CODE)
    done = subprocess.run(
        [sys.executable, "-I", str(READER), str(code), "shop"], capture_output=True, text=True
    )
    assert done.returncode == 0, done.stderr
    names = tmp_path / "names.json"
    names.write_text(json.dumps(name_tables(read_manifest(json.loads(done.stdout), PYTHON_RULES))))
    port = 0
    worker = [sys.executable, "-I", str(HOST), "{fd}", str(code), "shop"]
    process = subprocess.Popen(  # noqa: S603 -- the test's own interpreter and Provisa's front end
        [sys.executable, front.__file__, str(port), str(tmp_path / "state"), str(names), *worker],
        env={**os.environ, "PROVISA_CODE_HASH": "h1"},
        stderr=subprocess.PIPE,
        text=True,
    )
    assert process.stderr is not None
    pairing = None
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        line = process.stderr.readline()
        if line.startswith("pairing code: "):
            pairing = line.removeprefix("pairing code: ").strip()
        if line.startswith("remote host listening on "):
            # Bound by the host itself (port 0): no other test can take it first.
            port = int(line.rsplit(" ", 1)[1])
            break
    assert pairing is not None
    identity = remote.new_identity()
    remote.pair("127.0.0.1", port, pairing, identity)
    cert, key = tmp_path / "provisa.crt", tmp_path / "provisa.key"
    cert.write_text(identity.certificate)
    key.write_text(identity.key)
    key.chmod(0o600)
    yield port, cert, key
    process.terminate()
    process.wait(timeout=10)


def _dsn(port: int, cert: Path, key: Path) -> str:
    return (
        f"host=127.0.0.1 port={port} dbname=code user=provisa sslmode=require "
        f"sslcert={cert} sslkey={key}"
    )


def _rows(conn, sql: str) -> list[tuple]:
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()


def test_the_catalog_lists_each_function_with_its_declared_columns_and_parameters(host):
    with psycopg.connect(_dsn(*host), autocommit=True) as conn:
        assert _rows(
            conn,
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = 'orders' ORDER BY ordinal_position",
        ) == [
            ("id", "bigint"),
            ("customerName", "text"),
            ("asked", "text"),
            ("_nf_n", "bigint"),
            ("_nf_customerName", "text"),
        ]


def test_a_read_calls_the_function_with_its_parameters_and_pushes_and_applies_the_rest(host):
    with psycopg.connect(_dsn(*host), autocommit=True) as conn:
        assert _rows(
            conn,
            "SELECT id, asked FROM public.orders WHERE _nf_n = 5 AND \"customerName\" = 'acme' "
            "ORDER BY id DESC",
        ) == [(3, "n=5;customerName=acme"), (1, "n=5;customerName=acme")]


def test_the_row_estimate_is_the_count_last_answered(host):
    with psycopg.connect(_dsn(*host), autocommit=True) as conn:
        estimate = "SELECT reltuples FROM pg_catalog.pg_class WHERE relname = 'orders'"
        assert _rows(conn, estimate) == [(-1.0,)]
        _rows(conn, "SELECT id FROM orders WHERE _nf_n = 7")
        assert _rows(conn, estimate) == [(7.0,)]


def test_trinos_statistics_queries_read_the_row_count_and_no_column_statistics(host):
    with psycopg.connect(_dsn(*host), autocommit=True) as conn:
        _rows(conn, "SELECT id FROM orders WHERE _nf_n = 2")
        assert _rows(
            conn,
            "SELECT reltuples FROM pg_class WHERE relnamespace = (SELECT oid FROM pg_namespace "
            "WHERE nspname = 'public') AND relname = 'orders'",
        ) == [(2.0,)]
        assert (
            _rows(
                conn,
                "SELECT attname, null_frac, n_distinct, avg_width FROM pg_stats "
                "WHERE schemaname = 'public' AND tablename = 'orders'",
            )
            == []
        )


def test_the_functions_are_tables_of_the_schemas_the_connection_names(host):
    port, cert, key = host
    dsn = _dsn(port, cert, key) + " options='-c provisa.schemas=default,scores'"
    with psycopg.connect(dsn, autocommit=True) as conn:
        assert _rows(
            conn,
            "SELECT table_schema FROM information_schema.tables WHERE table_name = 'orders' "
            "ORDER BY 1",
        ) == [("default",), ("scores",)]
        assert _rows(conn, 'SELECT count(*) FROM "default".orders WHERE _nf_n = 2') == [(2,)]
        assert _rows(conn, "SELECT current_schema()") == [("default",)]


def test_a_cursor_hands_out_its_rows_as_they_are_fetched(host):
    with psycopg.connect(_dsn(*host), autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("START TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            cur.execute(
                "DECLARE c1 CURSOR FOR SELECT id FROM public.orders WHERE _nf_n = 5 ORDER BY id"
            )
            cur.execute("FETCH 2 FROM c1")
            assert cur.fetchall() == [(0,), (1,)]
            cur.execute("FETCH FROM c1")
            assert cur.fetchall() == [(2,)]
            cur.execute("FETCH ALL IN c1")
            assert cur.fetchall() == [(3,), (4,)]
            cur.execute("CLOSE c1")
            cur.execute("COMMIT")


def test_a_client_without_the_paired_certificate_is_refused(host, tmp_path):
    port, _cert, _key = host
    other = remote.new_identity()
    cert, key = tmp_path / "other.crt", tmp_path / "other.key"
    cert.write_text(other.certificate)
    key.write_text(other.key)
    key.chmod(0o600)
    with pytest.raises(psycopg.OperationalError):
        psycopg.connect(_dsn(port, cert, key), connect_timeout=5)
    with pytest.raises(psycopg.OperationalError):
        psycopg.connect(
            f"host=127.0.0.1 port={port} dbname=code user=provisa sslmode=require",
            connect_timeout=5,
        )


def test_duckdb_attaches_the_host_and_reads_a_function(host):
    import duckdb

    port, cert, key = host
    db = duckdb.connect()
    db.execute("LOAD postgres")
    dsn = _dsn(port, cert, key) + " options='-c provisa.schemas=default'"
    db.execute(f"ATTACH '{dsn.replace("'", "''")}' AS shop (TYPE postgres, READ_ONLY)")
    assert db.execute(
        'SELECT id FROM shop."default".orders WHERE _nf_n = 4 AND id >= 2 ORDER BY id'
    ).fetchall() == [(2,), (3,)]
    assert db.execute(
        'SELECT "customerName", count(*) FROM shop."default".orders WHERE _nf_n = 5 GROUP BY 1 '
        "ORDER BY 1"
    ).fetchall() == [("acme", 2), ("zeta", 3)]


def test_the_duckdb_engines_own_attach_verifies_and_reads_the_host(host, monkeypatch, tmp_path):
    import duckdb

    from provisa.federation.codehost_connectors import DuckDBCodeHostConnector, Prepared

    port, cert, key = host
    monkeypatch.setattr("provisa.core.request_context.require_current_org", lambda: "acme")
    connector = DuckDBCodeHostConnector("python")
    # The host's own certificate, as connect records it: the root the engine verifies it by.
    connector.remember(
        "acme",
        Prepared(
            str(cert),
            str(key),
            "",
            roots={"shop-src": str(tmp_path / "state" / "host.crt")},
            schemas={"shop-src": "default"},
        ),
    )
    source = SimpleNamespace(
        id="shop-src",
        mapping={
            "mode": "built",
            "remote": {"host": "127.0.0.1", "port": port, "pin": "p", "manifest": {"exports": []}},
        },
    )
    db = duckdb.connect()
    db.execute("LOAD postgres")
    db.execute(connector.details(source)["attach"])
    assert db.execute(
        'SELECT id FROM "_src_shop-src"."default".orders WHERE _nf_n = 3 ORDER BY id'
    ).fetchall() == [(0,), (1,), (2,)]
    # Another certificate as the root: not the host pinned, so the attach is refused.
    other = tmp_path / "other.crt"
    other.write_text(remote.new_identity().certificate)
    connector.remember(
        "acme",
        Prepared(
            str(cert), str(key), "", roots={"shop-src": str(other)}, schemas={"shop-src": "default"}
        ),
    )
    refused = duckdb.connect()
    refused.execute("LOAD postgres")
    with pytest.raises(duckdb.Error):
        refused.execute(connector.details(source)["attach"])
