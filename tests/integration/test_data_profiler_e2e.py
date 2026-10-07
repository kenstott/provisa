# Copyright (c) 2026 Kenneth Stott
# Canary: d17e4a92-6b3c-4f85-a0e1-3c9b7f2d5a68
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Data Profiler on a real server (REQ-1934).

The run reads as the org admin through the one governed pipeline, so the org admin's own rules shape
what is profiled: its row rule keeps a region out of the profile and a column masked even to it is
profiled as the mask. Result tables are registered through the admin surface with the defaults the
profiler's catalog derives from the profiled table's rules, so a masked column's values reach only
the roles that see it unmasked. The runs view is made safe for its viewer on the server.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Any

import pytest
from tests.helpers import PROFILER_RUN_DEFAULTS, registered_id, release_field
import sqlalchemy as sa

from tests.integration.worker_boot_harness import WorkerBoot, _config

pytestmark = [pytest.mark.integration]

_ROWS = [
    (1, "east", "ann@example.com", "s1", 10),
    (2, "west", "bob@example.com", "s2", 20),
    (3, "east", "cy@example.com", "s3", 30),
    (4, "north", "dee@example.com", "s4", 40),
]
_READERS = ["org_admin", "analyst", "auditor"]
# The result tables this test registers, by kind.
_KINDS = ("runs", "columns", "top_values", "shapes")


def _col(name: str, data_type: str, visible_to: list[str], **extra) -> dict:
    return {"name": name, "data_type": data_type, "visible_to": visible_to, **extra}


@pytest.fixture(scope="module")
def server():
    pg_host = os.environ.get("PG_HOST", "localhost")
    pg_port = int(os.environ.get("PG_PORT", "5432"))
    base = _config(pg_host, pg_port, "unused")
    orders = base["tables"][0]
    orders["profiler_source_id"] = "profiler"
    orders["columns"] = [
        _col("id", "integer", _READERS, is_primary_key=True),
        _col("region", "varchar", _READERS),
        # Masked to the analyst; the org admin and the auditor see it unmasked.
        _col(
            "email",
            "varchar",
            _READERS,
            mask_type="constant",
            mask_value="***",
            unmasked_to=["org_admin", "auditor"],
        ),
        # Masked even to the org admin, and seen by it alone: profiled as the mask.
        _col("secret", "varchar", ["org_admin"], mask_type="constant", mask_value="***"),
        _col("amount", "integer", _READERS),
    ]
    boot = WorkerBoot(
        1,
        pg_host=pg_host,
        pg_port=pg_port,
        extra_config={
            "tables": [orders],
            # org_admin is the reserved administrative role (REQ-1349): not declared here.
            "roles": [
                {
                    "id": role,
                    "capabilities": ["query_development", "full_results"],
                    "domain_access": ["*"],
                }
                for role in ("analyst", "auditor", "outsider")
            ],
            # The org admin's own row rule: the profile describes only the rows it may read.
            "rls_rules": [
                {"table_id": "orders", "role_id": "org_admin", "filter": "region <> 'north'"}
            ],
        },
        env={"PROVISA_REDIRECT_ENABLED": "false"},
    )
    # The harness names the source's database only once it has chosen one.
    boot._extra_config["sources"] = _config(pg_host, pg_port, boot.database)["sources"] + [
        {
            "id": "profiler",
            "type": "data_profiler",
            "mapping": {"cron": "0 3 * * *", **PROFILER_RUN_DEFAULTS},
        }
    ]
    boot.create_database()
    try:
        engine = sa.create_engine(boot.url, isolation_level="AUTOCOMMIT")
        with engine.connect() as conn:
            conn.execute(
                sa.text(
                    "ALTER TABLE public.orders ADD COLUMN email varchar, "
                    "ADD COLUMN secret varchar, ADD COLUMN amount integer"
                )
            )
            conn.execute(sa.text("DELETE FROM public.orders"))
            for row in _ROWS:
                conn.execute(
                    sa.text("INSERT INTO public.orders VALUES (:i, :r, :e, :s, :a)"),
                    dict(zip(["i", "r", "e", "s", "a"], row)),
                )
        engine.dispose()
        boot.start()
        boot.wait_all_ready(timeout=300)
        yield boot
    finally:
        boot.cleanup()


def _call(boot, role: str, method: str, path: str, body: dict | None = None) -> tuple[int, Any]:
    req = urllib.request.Request(
        f"http://127.0.0.1:{boot.ports['http']}{path}",
        data=None if body is None else json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "x-provisa-role": role},
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=300) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, {"error": exc.read().decode()}


def _sql(boot, role: str, sql: str) -> tuple[int, Any]:
    status, body = _call(boot, role, "POST", "/data/sql", {"sql": sql})
    return status, body["data"]["sql"] if status == 200 and "data" in body else body


def _graphql(boot, query: str, variables: dict) -> dict:
    status, body = _call(
        boot, "org_admin", "POST", "/admin/graphql", {"query": query, "variables": variables}
    )
    assert status == 200 and "errors" not in body, body
    return body["data"]


def _register(boot, entry: dict) -> None:
    """Register one result table with the catalog's defaults, as the form submits them."""
    result = _graphql(
        boot,
        "mutation($input: TableInput!) { registerTable(input: $input) { success message } }",
        {
            "input": {
                "sourceId": "profiler",
                "domainId": "sales",
                "schemaName": "default",
                "tableName": entry["tableName"],
                "columns": [
                    {
                        "name": c["name"],
                        "dataType": c["dataType"],
                        "visibleTo": c["visibleTo"],
                        "unmaskedTo": c["unmaskedTo"],
                        "maskType": c["maskType"],
                    }
                    for c in entry["columns"]
                ],
            }
        },
    )
    assert result["registerTable"]["success"], result
    # REQ-1921: a table registered through the admin starts as a draft; the owner releases it.
    released = _graphql(
        boot,
        f"mutation {{ {release_field(registered_id(result['registerTable']['message']))} "
        "{ success message } }",
        {},
    )
    assert released["setTableDraft"]["success"], released
    for rule in entry["rowRules"]:
        result = _graphql(
            boot,
            "mutation($input: RLSRuleInput!) { upsertRlsRule(input: $input) { success message } }",
            {
                "input": {
                    "tableId": entry["tableName"],
                    "roleId": rule["roleId"],
                    "filterExpr": rule["filter"],
                }
            },
        )
        assert result["upsertRlsRule"]["success"], result


@pytest.fixture(scope="module")
def profiled(server) -> dict:
    status, body = _call(server, "org_admin", "POST", "/admin/profilers/profiler/run")
    assert status == 200, body
    assert [o["error"] for o in body] == [None], body
    status, catalog = _call(server, "org_admin", "GET", "/admin/profilers/profiler/catalog")
    assert status == 200, catalog
    [member] = catalog
    names = {}
    for entry in member["tables"]:
        if entry["kind"] in _KINDS:
            _register(server, entry)
            names[entry["kind"]] = entry["tableName"]
    return {"boot": server, "member_id": member["memberId"], "names": names}


def test_the_run_reads_as_the_org_admin_and_is_kept_as_history(profiled):
    boot, runs = profiled["boot"], profiled["names"]["runs"]
    status, rows = _sql(boot, "org_admin", f"SELECT row_count, status FROM sales.{runs}")
    assert status == 200, rows
    # The org admin's row rule keeps the north row out of what it reads, so out of the profile.
    assert [(r["row_count"], r["status"]) for r in rows] == [(3, "succeeded")]


def test_the_org_admins_own_masks_shape_what_is_profiled(profiled):
    boot, columns = profiled["boot"], profiled["names"]["columns"]
    status, rows = _sql(
        boot,
        "org_admin",
        f"SELECT column_name, distinct_count FROM sales.{columns} "
        "WHERE column_name IN ('email', 'secret')",
    )
    assert status == 200, rows
    # email is unmasked to the org admin: three distinct real values. secret is masked even to
    # the org admin, so its profile describes the mask: one value.
    assert {r["column_name"]: r["distinct_count"] for r in rows} == {"email": 3, "secret": 1}


def test_a_masked_columns_values_reach_only_the_roles_that_see_it_unmasked(profiled):
    boot, top = profiled["boot"], profiled["names"]["top_values"]
    sql = (
        f"SELECT column_name, value FROM sales.{top} WHERE kind = 'top' "
        "AND column_name IN ('email', 'amount', 'secret')"
    )
    status, analyst = _sql(boot, "analyst", sql)
    assert status == 200, analyst
    assert {r["column_name"] for r in analyst} == {"amount"}
    status, auditor = _sql(boot, "auditor", sql)
    assert status == 200, auditor
    assert ("email", "ann@example.com") in {(r["column_name"], r["value"]) for r in auditor}
    assert "secret" not in {r["column_name"] for r in auditor}

    shapes = profiled["names"]["shapes"]
    status, rows = _sql(boot, "analyst", f"SELECT DISTINCT column_name FROM sales.{shapes}")
    assert status == 200, rows
    assert "email" not in {r["column_name"] for r in rows}


def test_the_columns_table_shows_a_restricted_reader_shape_but_no_values(profiled):
    boot, columns = profiled["boot"], profiled["names"]["columns"]
    sql = (
        f"SELECT column_name, null_share, min_value, max_value FROM sales.{columns} "
        "ORDER BY column_name"
    )
    status, analyst = _sql(boot, "analyst", sql)
    assert status == 200, analyst
    by = {r["column_name"]: r for r in analyst}
    # secret is not visible to the analyst: its description is withheld.
    assert set(by) == {"id", "region", "email", "amount"}
    assert by["email"]["null_share"] == 0.0 and by["email"]["min_value"] is None
    status, auditor = _sql(boot, "auditor", sql)
    assert status == 200, auditor
    assert {r["column_name"]: r["min_value"] for r in auditor}["amount"] == "10.0"


def test_a_role_that_cannot_read_the_profiled_table_reads_no_profile(profiled):
    boot, runs = profiled["boot"], profiled["names"]["runs"]
    status, body = _sql(boot, "outsider", f"SELECT row_count FROM sales.{runs}")
    assert status != 200, body


def test_the_runs_view_is_safe_for_its_viewer(profiled):
    boot, member_id = profiled["boot"], profiled["member_id"]
    status, runs = _call(boot, "analyst", "GET", f"/admin/tables/{member_id}/profile-runs")
    assert status == 200, runs
    run_id = runs[0]["run_id"]
    path = f"/admin/tables/{member_id}/profile-runs/{run_id}"
    status, analyst = _call(boot, "analyst", "GET", path)
    assert status == 200, analyst
    cols = {r["column_name"]: r for r in analyst["columns"]}
    assert "secret" not in cols
    assert cols["email"]["min_value"] is None and cols["email"]["distinct_ratio"] == 1.0
    assert cols["amount"]["min_value"] == "10.0"
    assert "email" not in {r["column_name"] for r in analyst["top_values"]}
    status, auditor = _call(boot, "auditor", "GET", path)
    assert status == 200, auditor
    assert "email" in {r["column_name"] for r in auditor["top_values"]}
