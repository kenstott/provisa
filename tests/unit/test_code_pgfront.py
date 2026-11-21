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

BOUNDS = {
    "memory_mb": 256,
    "threads": 2,
    "spill_cap_mb": 1024,
}  # the host's DuckDB, given at pairing

_CODE = textwrap.dedent(
    '''
    from dataclasses import dataclass

    from provisa_services import ReadContext


    @dataclass
    class Order:
        id: int
        customerName: str
        asked: str


    @dataclass
    class OrderInputs:
        n: int = 3


    def orders(inputs: OrderInputs, context: ReadContext) -> list[Order]:
        """Orders.

        @table
        """
        filters = ",".join(f"{f.column} {f.op} {f.value}" for f in context.filters)
        asked = f"n={inputs.n};filters={filters}"
        return [Order(i, "acme" if i % 2 else "zeta", asked) for i in range(inputs.n)]
    '''
)


def _image_frontlib(tmp_path: Path) -> Path:
    """The front end's library as a code image lays it out (image_build._front): buenavista's
    modules, and provisa._encoders as image_build._encoders takes it from the Linux provisa
    wheel -- here a wheel of the module built for this machine."""
    import shutil
    import zipfile

    import buenavista

    import provisa._encoders as encoders
    from provisa.codesource import image_build

    lib = tmp_path / "frontlib"
    shutil.copytree(Path(buenavista.__file__).parent, lib / "buenavista")
    house = tmp_path / "wheels"
    house.mkdir()
    module = Path(encoders.__file__).resolve()
    arch = "arm64" if os.uname().machine in ("arm64", "aarch64") else "amd64"
    wheel = house / f"provisa-1.0.0-cp312-cp312-manylinux_2_28_{image_build._WHEEL_ARCH[arch]}.whl"
    with zipfile.ZipFile(wheel, "w") as w:
        w.write(module, f"provisa/{module.name}")
    plan = SimpleNamespace(cache_root=tmp_path, version="1.0.0")
    for entry in image_build._encoders(plan, arch):  # type: ignore[arg-type]
        at = lib / Path(entry.path).relative_to(f"{image_build.ROOT}/frontlib")
        if entry.directory:
            at.mkdir(parents=True, exist_ok=True)
        else:
            at.write_bytes(entry.data or b"")
    return lib


def _start_host(tmp_path: Path, frontlib: Path | None):
    """A front end on a free port, paired; its port and the paired identity's files. With
    ``frontlib``, it runs as in a code image: isolated (no PYTHONPATH), its libraries there."""
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
    if frontlib is None:
        python, env = [sys.executable], {**os.environ, "PROVISA_CODE_HASH": "h1"}
    else:
        python = [sys.executable, "-I"]
        env = {
            **{k: v for k, v in os.environ.items() if k != "PYTHONPATH"},
            "PROVISA_CODE_HASH": "h1",
            "PROVISA_FRONT_LIB": str(frontlib),
        }
    process = subprocess.Popen(  # noqa: S603 -- the test's own interpreter and Provisa's front end
        [
            *python,
            front.__file__,
            str(port),
            str(tmp_path / "state"),
            str(tmp_path / "spill"),
            str(names),
            *worker,
        ],
        env=env,
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
    remote.pair("127.0.0.1", port, pairing, identity, BOUNDS)
    cert, key = tmp_path / "provisa.crt", tmp_path / "provisa.key"
    cert.write_text(identity.certificate)
    key.write_text(identity.key)
    key.chmod(0o600)
    return process, (port, cert, key)


@pytest.fixture
def host(tmp_path):
    """A front end on a free port, paired; its port and the paired identity's files."""
    process, served = _start_host(tmp_path, None)
    yield served
    process.terminate()
    process.wait(timeout=10)


@pytest.fixture
def image_host(tmp_path):
    """The front end as a code image runs it: its encoders from the image's provisa wheel."""
    process, served = _start_host(tmp_path, _image_frontlib(tmp_path))
    yield served
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
        ]


def test_a_read_gives_the_function_its_inputs_and_its_context_and_applies_the_rest(host):
    with psycopg.connect(_dsn(*host), autocommit=True) as conn:
        assert _rows(
            conn,
            "SELECT id, asked FROM public.orders WHERE _nf_n = 5 AND \"customerName\" = 'acme' "
            "ORDER BY id DESC",
        ) == [(3, "n=5;filters=customerName eq acme"), (1, "n=5;filters=customerName eq acme")]


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
    monkeypatch.setattr("provisa.federation.codehost_connectors._PREPARED", {})
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


def test_in_a_code_images_layout_the_front_end_sends_rows_through_the_wheels_encoders(image_host):
    """REQ-2194: a code image carries provisa._encoders from the provisa wheel for its
    architecture, and the front end's DataRows are written by it, in text and in binary."""
    with psycopg.connect(_dsn(*image_host), autocommit=True) as conn:
        query = 'SELECT id, "customerName" FROM public.orders WHERE _nf_n = 3 ORDER BY id'
        assert _rows(conn, query) == [(0, "zeta"), (1, "acme"), (2, "zeta")]
        with conn.cursor(binary=True) as cur:
            cur.execute(query)
            assert cur.fetchall() == [(0, "zeta"), (1, "acme"), (2, "zeta")]


def test_in_a_code_images_layout_duckdb_reads_the_host_through_binary_copy(image_host):
    """DuckDB's postgres scanner reads a table with COPY ... TO STDOUT (FORMAT binary), which the
    front end writes from the encoder's binary DataRows."""
    import duckdb

    port, cert, key = image_host
    db = duckdb.connect()
    db.execute("LOAD postgres")
    dsn = _dsn(port, cert, key)
    db.execute(f"ATTACH '{dsn.replace("'", "''")}' AS shop (TYPE postgres, READ_ONLY)")
    assert db.execute(
        'SELECT id, "customerName" FROM shop.public.orders WHERE _nf_n = 3 ORDER BY id'
    ).fetchall() == [(0, "zeta"), (1, "acme"), (2, "zeta")]
