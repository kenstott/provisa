# Copyright (c) 2026 Kenneth Stott
# Canary: 5d1f7a39-2c84-4e60-9b17-a3e6c0d4f82b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: a statement over a real PostgreSQL source calls no function that reads or acts on
that server (REQ-001).

A role that may read one table of a PostgreSQL source sends, over pgwire and over SQL on HTTP,
statements calling ``ts_stat('SELECT ...')``, ``pg_read_file``, ``nextval``, ``lo_from_bytea``,
``set_config`` and ``provisa_fake_method``. Each is refused by the validator (V007) before it is
routed. The source says so itself: a sequence the statements would advance, directly or through
the text ``ts_stat`` runs, is never called, and no large object is made.
"""

# Requirements: REQ-001

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
import uuid

import psycopg
import pytest
import sqlalchemy as sa

from tests.integration.worker_boot_harness import WorkerBoot

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = int(os.environ.get("PG_PORT", "5432"))
_PG_USER = os.environ.get("PG_USER", "provisa")
_PG_PASSWORD = os.environ.get("PG_PASSWORD", "provisa")
_ROLE = "org_admin"
_LARGE_OBJECT = 424243
_META_REFUSAL = "has no form over the meta domain"
_ADVANCES = "SELECT nextval(''public.h7_seq'')::text::tsvector"


@pytest.fixture(scope="module")
def launch_role():
    """A database role of this test's own, with a password no refusal can contain. The server's
    source credential is matched by central redaction in every text a client is told (REQ-1557,
    REQ-2087), and the shared database's password is "provisa", the prefix of the product's own
    function names: under it the refusal of ``provisa_fake_method`` is told as
    ``[REDACTED]_fake_method``. A superuser, as the shared role is: the launch makes its own
    database, control plane and sequence."""
    role, password = f"h7_{uuid.uuid4().hex[:10]}", uuid.uuid4().hex
    admin = sa.create_engine(
        f"postgresql+psycopg://{_PG_USER}:{_PG_PASSWORD}@{_PG_HOST}:{_PG_PORT}/provisa",
        isolation_level="AUTOCOMMIT",
    )
    with admin.connect() as conn:
        conn.execute(sa.text(f"CREATE ROLE {role} LOGIN SUPERUSER PASSWORD '{password}'"))
    try:
        yield role, password
    finally:
        with admin.connect() as conn:
            conn.execute(sa.text(f"DROP ROLE {role}"))
        admin.dispose()


@pytest.fixture(scope="module")
def server(launch_role):
    role, password = launch_role
    boot = WorkerBoot(
        1,
        pg_host=_PG_HOST,
        pg_port=_PG_PORT,
        pg_user=role,
        pg_password=password,
        env={"PROVISA_REDIRECT_ENABLED": "false"},
    )
    boot.create_database()
    try:
        with _source(boot) as conn:
            conn.execute(sa.text("CREATE SEQUENCE public.h7_seq"))
        boot.start()
        boot.wait_all_ready(timeout=300)
        yield boot
    finally:
        boot.cleanup()


def _source(boot):
    """The source's own database, as its owner: what a statement that reached it would change."""
    return sa.create_engine(boot.url, isolation_level="AUTOCOMMIT").connect()


def _http(boot, statement: str) -> tuple[bool, str]:
    req = urllib.request.Request(
        f"http://127.0.0.1:{boot.ports['http']}/data/sql",
        data=json.dumps({"sql": statement}).encode(),
        headers={"Content-Type": "application/json", "x-provisa-role": _ROLE},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return resp.status == 200, resp.read().decode()
    except urllib.error.HTTPError as exc:
        return False, f"{exc.code} {exc.read().decode()}"


def _pgwire(boot, statement: str) -> tuple[bool, str]:
    with psycopg.connect(
        host="127.0.0.1",
        port=boot.ports["pgwire"],
        user=_ROLE,
        password="provisa",
        dbname="provisa",
        autocommit=True,
        connect_timeout=30,
    ) as conn:
        try:
            return True, str(conn.execute(statement.encode()).fetchall())
        except psycopg.Error as exc:
            return False, str(exc)


_SURFACES = {"http": _http, "pgwire": _pgwire}

#: Each over the one table the role reads, so the statement is routed to that table's source.
_REFUSED = {
    "ts_stat_from": (
        "ts_stat",
        f"SELECT o.id FROM sales.orders o WHERE EXISTS (SELECT 1 FROM ts_stat('{_ADVANCES}'))",
    ),
    "ts_stat_qualified": (
        "ts_stat",
        f"SELECT o.id, (SELECT count(*) FROM pg_catalog.\"ts_stat\"('{_ADVANCES}')) AS n "
        "FROM sales.orders o",
    ),
    "pg_read_file": ("pg_read_file", "SELECT o.id, pg_read_file('PG_VERSION') FROM sales.orders o"),
    "pg_read_file_qualified": (
        "pg_read_file",
        "SELECT o.id FROM sales.orders o WHERE PG_CATALOG.PG_READ_FILE('PG_VERSION') <> ''",
    ),
    "nextval": ("nextval", "SELECT o.id, nextval('public.h7_seq') FROM sales.orders o"),
    "lo_from_bytea": (
        "lo_from_bytea",
        f"SELECT o.id, lo_from_bytea({_LARGE_OBJECT}, '\\x61'::bytea) FROM sales.orders o",
    ),
    "pg_sleep": ("pg_sleep", "SELECT o.id FROM sales.orders o WHERE pg_sleep(0) IS NULL"),
    "provisa_fake_method": (
        "provisa_fake_method",
        "SELECT o.id, provisa_fake_method('file_path', '{}', 1, 1) FROM sales.orders o",
    ),
}


@pytest.mark.parametrize("surface", list(_SURFACES))
@pytest.mark.parametrize("case", list(_REFUSED))
def test_a_function_that_reads_or_acts_on_the_source_is_refused(server, surface, case):
    name, statement = _REFUSED[case]
    accepted, said = _SURFACES[surface](server, statement)
    assert not accepted, said
    assert name in said, said
    # pgwire addresses a statement naming pg_catalog to its catalog route, whose own refusal by
    # name comes first (metadata_functions.MetadataFunctionRefused); every other is the validator's.
    assert "V007" in said or _META_REFUSAL in said, said


@pytest.mark.parametrize("surface", list(_SURFACES))
def test_a_function_that_runs_sql_as_text_is_refused_with_no_table(server, surface):
    accepted, said = _SURFACES[surface](server, f"SELECT * FROM ts_stat('{_ADVANCES}')")
    assert not accepted, said
    assert "ts_stat" in said, said


def test_a_setting_is_not_set_on_the_sources_connection_over_http(server):
    """S17: over HTTP the statement went to the source's pooled connection as written. pgwire
    answers set_config itself and sets nothing (pgwire.catalog_route)."""
    accepted, said = _http(
        server, "SELECT o.id, set_config('provisa.h7', 'on', false) FROM sales.orders o"
    )
    assert not accepted, said
    assert "V007" in said and "set_config" in said, said


@pytest.mark.parametrize("surface", list(_SURFACES))
def test_an_ordinary_function_over_the_same_table_is_served(server, surface):
    accepted, said = _SURFACES[surface](
        server, "SELECT o.id, lower(o.region) AS r, length(o.region) AS n FROM sales.orders o"
    )
    assert accepted, said


def test_nothing_reached_the_source(server):
    """Last in the file: after every refused statement, the source is as it was made."""
    with _source(server) as conn:
        called = conn.execute(sa.text("SELECT is_called FROM public.h7_seq")).scalar_one()
        objects = conn.execute(
            sa.text("SELECT count(*) FROM pg_catalog.pg_largeobject_metadata WHERE oid = :oid"),
            {"oid": _LARGE_OBJECT},
        ).scalar_one()
    assert called is False, "a refused statement advanced the source's sequence"
    assert objects == 0, "a refused statement made a large object on the source"
