# Copyright (c) 2026 Kenneth Stott
# Canary: fea7c6e4-32a9-4738-a85d-5e3a672e949c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Faked reads on a real server (REQ-1494, REQ-1942), on the embedded engine (the Trino engine's
faked reads are tests/integration/test_fake_reads_trino_e2e.py): in a Test (fake) environment a
column declaring a fake shows the fake to every role, and prod never fakes; filters, grouping and
matching work on the fakes; a relative fake follows the faked column it names."""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Any

import pytest
import sqlalchemy as sa

from tests.integration.worker_boot_harness import WorkerBoot, _config

pytestmark = [pytest.mark.integration]

_ROLES = ["org_admin", "analyst"]
_ROWS = 60


def _col(name: str, data_type: str, **extra) -> dict:
    return {"name": name, "data_type": data_type, "visible_to": _ROLES, **extra}


def _faked(decl: str) -> dict:
    return {"fake": decl}


#: The Test (fake) environment the faked reads are made in (REQ-1942).
_QA = "qa"


@pytest.fixture(scope="module", params=["duckdb"])
def server(request):
    pg_host = os.environ.get("PG_HOST", "localhost")
    pg_port = int(os.environ.get("PG_PORT", "5432"))
    boot = WorkerBoot(
        1,
        pg_host=pg_host,
        pg_port=pg_port,
        engine=request.param,
        env={"PROVISA_REDIRECT_ENABLED": "false"},
    )
    base = _config(pg_host, pg_port, boot.database)
    boot._extra_config = {
        "sources": base["sources"],
        "tables": [
            {
                "source_id": "sales-pg",
                "domain_id": "sales",
                "schema": "public",
                "table": "people",
                "columns": [
                    _col("id", "integer", is_primary_key=True),
                    _col("email", "varchar", **_faked("email()")),
                    _col("tier", "varchar", **_faked("categories((gold, silver))")),
                    _col(
                        "joined",
                        "timestamp",
                        **_faked("uniform(min='2024-01-01', max='2024-06-30')"),
                    ),
                    _col("renewed", "timestamp", **_faked("after(joined, 10 to 20 days)")),
                    _col("region", "varchar"),
                    # Measured at model build (REQ-1494): from the table, there being no profile.
                    _col("status", "varchar", **_faked("categories()")),
                    _col("vip", "boolean", **_faked("bool()")),
                    _col("opened", "timestamp"),
                    _col("closed", "timestamp", **_faked("after(opened)")),
                ],
            }
        ],
        "roles": [
            {
                "id": "analyst",
                "capabilities": ["query_development", "full_results"],
                "domain_access": ["*"],
            }
        ],
    }
    boot.create_database()
    try:
        engine = sa.create_engine(boot.url, isolation_level="AUTOCOMMIT")
        with engine.connect() as conn:
            conn.execute(
                sa.text(
                    "CREATE TABLE public.people (id integer PRIMARY KEY, email text, tier text, "
                    "joined timestamp, renewed timestamp, region text, status text, vip boolean, "
                    "opened timestamp, closed timestamp)"
                )
            )
            for i in range(1, _ROWS + 1):
                conn.execute(
                    sa.text(
                        "INSERT INTO public.people VALUES (:i, :e, 'bronze', '2020-01-01', "
                        "'2020-01-02', :r, :s, :v, '2021-03-01', "
                        "TIMESTAMP '2021-03-01' + make_interval(days => :d))"
                    ),
                    {
                        "i": i,
                        "e": f"p{i % 40}@real.example",
                        "r": "east" if i % 2 else "west",
                        "s": "open" if i % 3 else "closed",
                        "v": i % 4 == 0,
                        "d": i % 5 + 1,
                    },
                )
        engine.dispose()
        boot.start()
        boot.wait_all_ready(timeout=300)
        status, body = _call(
            boot,
            "POST",
            f"/admin/orgs/{boot.org_id}/environments",
            {"name": _QA, "data_mode": "test_fake"},
        )
        assert status == 200, body
        yield boot
    finally:
        boot.cleanup()


def _call(boot, method: str, path: str, body: Any = None, env: str | None = None):
    headers = {"Content-Type": "application/json", "x-provisa-role": "org_admin"}
    if env is not None:
        headers["x-provisa-env"] = env
    req = urllib.request.Request(
        f"http://127.0.0.1:{boot.ports['http']}{path}",
        data=None if body is None else json.dumps(body).encode(),
        headers=headers,
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=300) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, {"error": exc.read().decode()}


def _sql(boot, sql: str, role: str, env: str | None = _QA) -> list[dict]:
    headers = {"Content-Type": "application/json", "x-provisa-role": role}
    if env is not None:
        headers["x-provisa-env"] = env
    req = urllib.request.Request(
        f"http://127.0.0.1:{boot.ports['http']}/data/sql",
        data=json.dumps({"sql": sql}).encode(),
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=300) as resp:
            body: Any = json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        raise AssertionError(exc.read().decode()) from exc
    return body["data"]["sql"]


def test_every_role_reads_fakes_in_test_fake_and_prod_never_fakes(server):
    real = {f"p{i}@real.example" for i in range(40)}
    query = "SELECT id, email FROM sales.people ORDER BY id"
    for role in ("analyst", "org_admin"):
        faked = _sql(server, query, role)
        assert not {r["email"] for r in faked} & real, role
        by_id = {r["id"]: r["email"] for r in faked}
        assert by_id[1] == by_id[41] and len(set(by_id.values())) == 40
        # REQ-1942: prod is always real.
        assert {r["email"] for r in _sql(server, query, role, env=None)} <= real


def test_filters_grouping_and_matching_work_on_the_fakes(server):
    (row,) = _sql(server, "SELECT email FROM sales.people WHERE id = 3", "analyst")
    ids = _sql(
        server, f"SELECT id FROM sales.people WHERE email = '{row['email']}' ORDER BY id", "analyst"
    )
    assert [r["id"] for r in ids] == [3, 43]
    tiers = _sql(server, "SELECT tier, COUNT(*) AS n FROM sales.people GROUP BY tier", "analyst")
    assert {r["tier"] for r in tiers} <= {"gold", "silver"}
    # Matching through the fake: rows 41..60 hold the real emails of rows 1..20, so their fakes
    # match those 20 rows and themselves.
    matched = _sql(
        server,
        "SELECT COUNT(*) AS n FROM sales.people WHERE email IN "
        "(SELECT email FROM sales.people WHERE id > 40)",
        "analyst",
    )
    assert matched[0]["n"] == 40


def test_a_relative_fake_follows_the_faked_column(server):
    import datetime as dt

    rows = _sql(server, "SELECT joined, renewed FROM sales.people", "analyst")
    for r in rows:
        joined = dt.datetime.fromisoformat(str(r["joined"]).replace("Z", ""))
        renewed = dt.datetime.fromisoformat(str(r["renewed"]).replace("Z", ""))
        assert dt.datetime(2024, 1, 1) <= joined <= dt.datetime(2024, 6, 30)
        assert dt.timedelta(days=10) <= renewed - joined <= dt.timedelta(days=20)


def test_measured_fakes_read_from_the_table_at_model_build(server):
    import datetime as dt

    rows = _sql(server, "SELECT status, vip, opened, closed FROM sales.people", "analyst")
    assert {r["status"] for r in rows} <= {"open", "closed"}
    assert {r["vip"] for r in rows} <= {True, False}
    for r in rows:
        opened = dt.datetime.fromisoformat(str(r["opened"]).replace("Z", ""))
        closed = dt.datetime.fromisoformat(str(r["closed"]).replace("Z", ""))
        # The measured difference runs from 1 to 5 days.
        assert dt.timedelta(days=1) <= closed - opened <= dt.timedelta(days=5)
