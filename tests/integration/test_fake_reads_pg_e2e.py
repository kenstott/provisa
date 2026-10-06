# Copyright (c) 2026 Kenneth Stott
# Canary: a796c712-7075-4976-a490-63e3b8854768
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Faked reads on the pg engine (REQ-1494): PostgreSQL's fake functions are PL/Python calling the
same Python as the embedded DuckDB engine, so the digest and every fake method agree with it; a
governed statement's faked projection runs there; the key, read from the key directory by its
fingerprint, never reaches the server's log."""

from __future__ import annotations

import datetime as dt
import secrets
import subprocess
import time
import uuid
from pathlib import Path

import psycopg2
import pytest

from provisa.compiler import stage2
from provisa.compiler.stage2 import GovernanceContext, apply_governance
from provisa.fakes import digest as digest_mod
from provisa.fakes.digest import digest, fingerprint
from provisa.fakes.duckdb_functions import fake_method
from provisa.fakes.methods import UNSUPPORTED, method_names
from provisa.fakes.pg_functions import FUNCTIONS_SQL
from provisa.security.masking import MaskingRule, MaskType
from provisa.transpiler.transpile import rewrite_json_object_to_build_object, transpile

pytestmark = [pytest.mark.integration]

_ROOT = Path(__file__).resolve().parents[2]
_IMAGE = "provisa-pg-plpython-test:16"


def _docker(*args: str) -> str:
    return subprocess.run(["docker", *args], capture_output=True, text=True, check=True).stdout


@pytest.fixture(scope="module")
def key(tmp_path_factory):
    value = secrets.token_bytes(32)
    directory = tmp_path_factory.mktemp("fake-keys", numbered=True)
    (directory / f"{fingerprint(value)}.key").write_text(value.hex())
    directory.chmod(0o755)
    return value, directory


@pytest.fixture(scope="module")
def pg(key):
    _docker(
        "build",
        "-q",
        "-t",
        _IMAGE,
        "-f",
        str(_ROOT / "docker/pg-plpython-test.Dockerfile"),
        str(_ROOT / "docker"),
    )
    name = f"provisa-pg-plpython-{uuid.uuid4().hex[:8]}"
    _docker(
        "run", "-d", "--rm", "--name", name, "-p", "127.0.0.1::5432",
        "-e", "POSTGRES_PASSWORD=provisa",
        "-e", "PROVISA_FAKE_KEY_DIR=/keys",
        "-e", "PYTHONPATH=/provisa",
        "-v", f"{_ROOT}:/provisa:ro",
        "-v", f"{key[1]}:/keys:ro",
        _IMAGE,
    )  # fmt: skip
    try:
        port = int(_docker("port", name, "5432/tcp").strip().rsplit(":", 1)[1])
        dsn = f"postgresql://postgres:provisa@127.0.0.1:{port}/postgres"
        deadline = time.time() + 60
        while True:
            try:
                con = psycopg2.connect(dsn)
                break
            except psycopg2.OperationalError:
                if time.time() > deadline:
                    raise
                time.sleep(1)
        con.autocommit = True
        cur = con.cursor()
        cur.execute("CREATE EXTENSION IF NOT EXISTS plpython3u")
        cur.execute(FUNCTIONS_SQL)
        yield con, name
        con.close()
    finally:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)


def _one(con, sql: str, params=()):
    cur = con.cursor()
    cur.execute(sql, params)
    return cur.fetchone()[0]


def test_postgres_computes_the_published_digest(pg, key):
    con, _ = pg
    value, _dir = key
    fp = fingerprint(value)
    assert _one(con, "SELECT provisa_digest(%s, %s)", (fp, "ann@example.com")) == digest(
        value, "ann@example.com"
    )
    with pytest.raises(psycopg2.Error, match="holds no fake key 0123456789abcdef"):
        _one(con, "SELECT provisa_digest('0123456789abcdef', 'x')")


def test_every_method_computes_as_the_embedded_engine_does(pg):
    con, _ = pg
    declarable = [m for m in method_names() if m not in UNSUPPORTED]
    cur = con.cursor()
    for seed in (1, 987654321):
        cur.execute(
            "SELECT m, provisa_fake_method(m, '{}', %s) FROM unnest(%s::text[]) AS t(m)",
            (seed, declarable),
        )
        for method, value in cur.fetchall():
            assert value == fake_method(method, "{}", seed), method


def test_every_method_is_a_function_of_the_digest_alone_on_postgres(pg):
    """REQ-1494 (determinism): the same digest gives the same value at two reads a second apart."""
    con, _ = pg
    declarable = [m for m in method_names() if m not in UNSUPPORTED]
    sql = (
        "SELECT m, d, provisa_fake_method(m, '{}', d) FROM unnest(%s::text[]) AS t(m) "
        "CROSS JOIN unnest(ARRAY[1, -7, 4611686018427387904]::bigint[]) AS s(d)"
    )
    cur = con.cursor()
    cur.execute(sql, (declarable,))
    first = {(m, d): v for m, d, v in cur.fetchall()}
    time.sleep(1.1)  # the clock moves past a second: a method reading it would differ
    cur.execute(sql, (declarable,))
    second = {(m, d): v for m, d, v in cur.fetchall()}
    assert len(first) == 3 * len(declarable)
    assert {k for k in first if first[k] != second[k]} == set()


def test_a_governed_faked_read_runs_on_postgres(pg, key, monkeypatch):
    con, _ = pg
    value, _dir = key
    monkeypatch.setattr(digest_mod, "_key", value)
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS people")
    cur.execute("CREATE TABLE people (id int, email text, joined timestamp, renewed timestamp)")
    cur.execute(
        "INSERT INTO people SELECT i, 'p' || (i % 40) || '@real.example', '2020-01-01', "
        "'2020-01-02' FROM generate_series(1, 60) AS i"
    )
    gov = GovernanceContext(role_id="analyst")
    gov.table_map = {"public.people": 1}
    gov.all_columns = {
        1: [
            ("id", "integer"),
            ("email", "varchar"),
            ("joined", "timestamp"),
            ("renewed", "timestamp"),
        ]
    }
    gov.visible_columns = {1: None}
    for name, decl, dtype in [
        ("email", "email()", "varchar"),
        ("joined", "uniform(min='2024-01-01', max='2024-06-30')", "timestamp"),
        ("renewed", "after(joined, 10 to 20 days)", "timestamp"),
    ]:
        gov.masking_rules[(1, name)] = (MaskingRule(MaskType.fake, fake=decl), dtype)
    stage2._bind_fakes(gov)
    governed = apply_governance("SELECT id, email, joined, renewed FROM public.people", gov)
    # The pg engine's physical transpile (PgBackend.transpile_physical).
    cur.execute(rewrite_json_object_to_build_object(transpile(governed, "postgres")))
    rows = cur.fetchall()
    by_id = {r[0]: r[1] for r in rows}
    assert not set(by_id.values()) & {f"p{i}@real.example" for i in range(40)}
    assert by_id[1] == by_id[41] and len(set(by_id.values())) == 40
    for _id, _email, joined, renewed in rows:
        assert dt.datetime(2024, 1, 1) <= joined <= dt.datetime(2024, 6, 30)
        assert dt.timedelta(days=10) <= renewed - joined <= dt.timedelta(days=20)


def test_the_key_never_reaches_the_server_log(pg, key):
    _, name = pg
    log = subprocess.run(["docker", "logs", name], capture_output=True, text=True).stderr
    assert log, "the server's log was not read"
    assert key[0].hex() not in log and key[0].hex().upper() not in log
