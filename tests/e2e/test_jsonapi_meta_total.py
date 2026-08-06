# Copyright (c) 2026 Kenneth Stott
# Canary: 5892a646-c714-47f1-9540-a5ab30a42b6e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1197 e2e: JSON:API ``meta.total`` is computed by a COUNT(*) aggregate pushed
down to the engine, never by materializing the full matching set in Python.

Live round trip: HTTP request → JSON:API router → the single governed pipeline
(_govern_and_route_compiled + _execute_plan) → the e2e stack's Postgres. Push-down is
proven from the ENGINE side: Postgres statement logging is switched on for the test and
the ``SELECT COUNT(*) AS total FROM (...) AS _provisa_count`` wrapper must appear in the
database's own log — a Python-side count would never send that statement. Correctness is
proven against a direct SQL count of the same rows, with the response page holding fewer
rows than the total (the full set never crossed the wire inline).
"""

import datetime
import os
import subprocess

import psycopg2
import pytest
from httpx import ASGITransport, AsyncClient

from tests.itest_stack import E2E_PROJECT

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio]

_JSONAPI_ACCEPT = "application/vnd.api+json"


def _pg_conn():
    """Connect to the DATA-source Postgres (the e2e stack's own instance).

    The sample config's sales-pg source reads ${env:PG_PORT}, so this is the server the
    governed count executes against and the container whose logs carry the marker —
    NOT the platform control plane, which a full session parks on the itest stack."""
    return psycopg2.connect(
        host=os.environ.get("PG_HOST", "localhost"),
        port=int(os.environ.get("PG_PORT", "5432")),
        dbname="provisa",
        user="provisa",
        password="provisa",
    )


def _pg_scalar(sql: str):
    with _pg_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchone()[0]


def _postgres_container_id() -> str:
    """The e2e stack's postgres container, located by its compose project label."""
    out = subprocess.run(
        [
            "docker",
            "ps",
            "-q",
            "--filter",
            f"label=com.docker.compose.project={E2E_PROJECT}",
            "--filter",
            "label=com.docker.compose.service=postgres",
        ],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    assert out, f"no postgres container found for compose project {E2E_PROJECT}"
    return out.splitlines()[0]


def _pg_log_since(container_id: str, since_iso: str) -> str:
    proc = subprocess.run(
        ["docker", "logs", "--since", since_iso, container_id],
        capture_output=True,
        text=True,
        check=True,
    )
    return proc.stdout + proc.stderr


@pytest.fixture(scope="module")
async def client():
    os.environ.setdefault("PG_PASSWORD", "provisa")

    from provisa.api.app import create_app

    app = create_app()

    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            yield c


def _alter_log_statement(sql: str) -> None:
    # ALTER SYSTEM refuses to run inside a transaction block, and psycopg2's connection
    # context manager wraps statements in one — plain connection, autocommit on.
    conn = _pg_conn()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql)
            cur.execute("SELECT pg_reload_conf()")
    finally:
        conn.close()


@pytest.fixture(scope="module")
def statement_logging():
    """Turn on Postgres statement logging for the duration of the module, then reset."""
    _alter_log_statement("ALTER SYSTEM SET log_statement = 'all'")
    yield
    _alter_log_statement("ALTER SYSTEM RESET log_statement")


class TestMetaTotalCountPushdown:
    async def test_total_is_engine_counted_not_materialized(self, client, statement_logging):
        """meta.total equals the engine's count while the page carries fewer rows, and the
        _provisa_count COUNT(*) wrapper is observed at the database — the engine computed
        the cardinality; Python only ever saw one scalar row."""
        expected_total = _pg_scalar("SELECT COUNT(*) FROM public.orders")
        assert expected_total > 2, "fixture orders table must hold more rows than the page"

        container = _postgres_container_id()
        since = datetime.datetime.now(datetime.timezone.utc).isoformat()

        resp = await client.get(
            "/data/jsonapi/sales-analytics/orders",
            params={"page[number]": "1", "page[size]": "2"},
            headers={"Accept": _JSONAPI_ACCEPT, "X-Provisa-Role": "org_admin"},
        )
        assert resp.status_code == 200, resp.text
        doc = resp.json()

        # Contract: the total is the full governed cardinality, the page is not.
        assert doc["meta"]["total"] == expected_total
        assert len(doc["data"]) == 2

        # Push-down evidence from the engine's own statement log: the COUNT(*) wrapper
        # executed AT the database. A Python-side count would fetch plain rows instead
        # and this marker would never reach Postgres.
        log = _pg_log_since(container, since)
        count_lines = [ln for ln in log.splitlines() if "_provisa_count" in ln]
        assert count_lines, "COUNT(*) _provisa_count wrapper never reached the engine"
        assert any("COUNT" in ln.upper() for ln in count_lines)

    async def test_total_reflects_the_governed_filter(self, client, statement_logging):
        """The count query carries the same filters as the data query — meta.total is the
        filtered cardinality, not the table's."""
        expected_filtered = _pg_scalar(
            "SELECT COUNT(*) FROM public.orders WHERE region = 'us-east'"
        )
        expected_all = _pg_scalar("SELECT COUNT(*) FROM public.orders")
        assert 0 < expected_filtered < expected_all

        resp = await client.get(
            "/data/jsonapi/sales-analytics/orders",
            params={"page[number]": "1", "page[size]": "1", "filter[region]": "us-east"},
            headers={"Accept": _JSONAPI_ACCEPT, "X-Provisa-Role": "org_admin"},
        )
        assert resp.status_code == 200, resp.text
        doc = resp.json()
        assert doc["meta"]["total"] == expected_filtered
        assert len(doc["data"]) == 1
