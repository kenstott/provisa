# Copyright (c) 2026 Kenneth Stott
# Canary: 46da310b-04fc-4eae-abad-0a48cbd487b5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Faked reads on the Trino engine (REQ-1494): a governed statement's faked projection, transpiled
for Trino, runs there under the platform key the engine holds, and every use of a faked column
reads the same fake -- the same statement the server sends."""

from __future__ import annotations

import datetime as dt
import os
import secrets
import uuid
from pathlib import Path

import pytest
import sqlalchemy as sa
import trino

from provisa.compiler import stage2
from provisa.compiler.stage2 import GovernanceContext, apply_governance
from provisa.core.catalog import create_catalog
from provisa.core.models import Source, SourceType
from provisa.fakes import digest as digest_mod
from provisa.security.masking import MaskingRule, MaskType
from provisa.transpiler.transpile import transpile_to_trino

pytestmark = [pytest.mark.integration]

_TABLE = f"fake_read_people_{uuid.uuid4().hex[:8]}"
_COLUMNS = [
    ("id", "integer"),
    ("email", "varchar"),
    ("joined", "timestamp"),
    ("renewed", "timestamp"),
    ("region", "varchar"),
]
_FAKES = {
    "email": "email()",
    "joined": "uniform(min='2024-01-01', max='2024-06-30')",
    "renewed": "after(joined, 10 to 20 days)",
}


@pytest.fixture(scope="module")
def table():
    url = (
        f"postgresql+psycopg://{os.environ.get('PG_USER', 'provisa')}:"
        f"{os.environ.get('PG_PASSWORD', 'provisa')}@{os.environ.get('PG_HOST', 'localhost')}:"
        f"{os.environ.get('PG_PORT', '5432')}/{os.environ.get('PG_DATABASE', 'provisa')}"
    )
    engine = sa.create_engine(url, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        conn.execute(
            sa.text(
                f"CREATE TABLE public.{_TABLE} (id integer, email text, joined timestamp, "
                "renewed timestamp, region text)"
            )
        )
        for i in range(1, 61):
            conn.execute(
                sa.text(
                    f"INSERT INTO public.{_TABLE} VALUES (:i, :e, '2020-01-01', '2020-01-02', :r)"
                ),
                {"i": i, "e": f"p{i % 40}@real.example", "r": "east" if i % 2 else "west"},
            )
    source = Source(
        id="sales-pg",
        type=SourceType.postgresql,
        host=os.environ.get("PG_HOST", "localhost"),
        port=int(os.environ.get("PG_PORT", "5432")),
        database=os.environ.get("PG_DATABASE", "provisa"),
        username=os.environ.get("PG_USER", "provisa"),
        password=os.environ.get("PG_PASSWORD", "provisa"),
    )
    conn = trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "localhost"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user="test",
    )
    create_catalog(conn, source, resolved_password=source.password)
    conn.close()
    yield _TABLE
    with engine.connect() as conn:
        conn.execute(sa.text(f"DROP TABLE public.{_TABLE}"))
    engine.dispose()


@pytest.fixture
def key(monkeypatch):
    value = secrets.token_bytes(32)
    monkeypatch.setattr(digest_mod, "_key", value)
    path = Path(os.environ["PROVISA_FAKE_KEY_DIR"]) / f"{digest_mod.fingerprint(value)}.key"
    path.write_text(value.hex())
    yield value
    path.unlink()


def _gov() -> GovernanceContext:
    gov = GovernanceContext(role_id="analyst")
    gov.table_map = {f"public.{_TABLE}": 1}
    gov.all_columns = {1: list(_COLUMNS)}
    gov.visible_columns = {1: None}
    for name, decl in _FAKES.items():
        gov.masking_rules[(1, name)] = (
            MaskingRule(MaskType.fake, fake=decl),
            dict(_COLUMNS)[name],
        )
    stage2._bind_fakes(gov)
    return gov


def _run(trino_conn, sql: str) -> list[tuple]:
    governed = apply_governance(sql, _gov())
    cur = trino_conn.cursor()
    cur.execute(transpile_to_trino(governed))
    return cur.fetchall()


def test_trino_reads_one_fake_per_real_value(trino_conn, table, key):
    rows = _run(trino_conn, f"SELECT id, email FROM sales_pg.public.{table}")
    by_id = dict(rows)
    real = {f"p{i}@real.example" for i in range(40)}
    assert not set(by_id.values()) & real
    assert by_id[1] == by_id[41] and len(set(by_id.values())) == 40


def test_trino_filters_and_relative_fakes_read_the_faked_projection(trino_conn, table, key):
    ((email,),) = _run(trino_conn, f"SELECT email FROM sales_pg.public.{table} WHERE id = 3")
    ids = _run(
        trino_conn, f"SELECT id FROM sales_pg.public.{table} WHERE email = '{email}' ORDER BY id"
    )
    assert [r[0] for r in ids] == [3, 43]
    for joined, renewed in _run(trino_conn, f"SELECT joined, renewed FROM sales_pg.public.{table}"):
        assert dt.datetime(2024, 1, 1) <= joined <= dt.datetime(2024, 6, 30)
        assert dt.timedelta(days=10) <= renewed - joined <= dt.timedelta(days=20)
