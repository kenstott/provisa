# Copyright (c) 2026 Kenneth Stott
# Canary: e8f0700f-3d86-4988-9e2c-0c33411e0032
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An environment created as Inherit copies each source's connection from its parent and reads
through the copy; one created as Unbound reads through none (REQ-1491, REQ-1529, REQ-1538,
REQ-1942), on a real server."""

# Requirements: REQ-1491, REQ-1529, REQ-1538, REQ-1942

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from typing import Any

import pytest

from tests.integration.worker_boot_harness import WorkerBoot

pytestmark = [pytest.mark.integration]


@pytest.fixture(scope="module")
def boot():
    b = WorkerBoot(
        1,
        pg_host=os.environ.get("PG_HOST", "localhost"),
        pg_port=int(os.environ.get("PG_PORT", "5432")),
        env={"PROVISA_REDIRECT_ENABLED": "false"},
    )
    # REQ-1942: a pii column with no fake, so Test (fake) is refused naming it.
    b._extra_config = {
        # REQ-1942: a table a Reversible environment can keep mutations of -- its key declared,
        # its columns writable by org_admin.
        "tables": [
            {
                "source_id": "sales-pg",
                "domain_id": "sales",
                "schema": "public",
                "table": "orders",
                "columns": [
                    {
                        "name": "id",
                        "data_type": "integer",
                        "is_primary_key": True,
                        "visible_to": ["org_admin", "analyst"],
                        "writable_by": ["org_admin"],
                    },
                    {
                        "name": "region",
                        "data_type": "varchar",
                        "visible_to": ["org_admin", "analyst"],
                        "writable_by": ["org_admin"],
                    },
                ],
            }
        ],
        "tag_assignments": [
            {
                "tag_id": "pii",
                "object_type": "column",
                "table_ref": "sales-pg.public.orders",
                "column_name": "region",
            }
        ],
    }
    b.create_database()
    try:
        b.start()
        b.wait_all_ready(timeout=300)
        yield b
    finally:
        b.cleanup()


def _call(b, method: str, path: str, body: Any = None, env: str | None = None):
    headers = {"Content-Type": "application/json", "x-provisa-role": "org_admin"}
    if env is not None:
        headers["x-provisa-env"] = env
    req = urllib.request.Request(
        f"http://127.0.0.1:{b.ports['http']}{path}",
        data=None if body is None else json.dumps(body).encode(),
        headers=headers,
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=300) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, {"error": exc.read().decode()}


def _environment(b, name: str, data_mode: str) -> None:
    status, body = _call(
        b,
        "POST",
        f"/admin/orgs/{b.org_id}/environments",
        {"name": name, "data_mode": data_mode},
    )
    assert status == 200, body


def _orders(b, env: str):
    return _call(b, "POST", "/data/sql", {"sql": "SELECT COUNT(*) AS n FROM sales.orders"}, env)


def _write(b, env: str):
    return _call(
        b, "POST", "/data/sql", {"sql": "INSERT INTO sales.orders VALUES (9, 'north')"}, env
    )


def test_an_inheriting_environment_reads_the_real_table_through_its_copied_connection(boot):
    before = len(boot.log_text())
    _environment(boot, "branch", "inherit")
    status, body = _orders(boot, "branch")
    since = boot.log_text()[before:]
    assert status == 200, (body, since[-4000:])
    assert body["data"]["sql"] == [{"n": 2}]
    # Its pool dialled the coordinates copied from prod.
    assert "direct pool for 'sales-pg'" not in since, since[-4000:]


def test_recopy_from_parent_restores_a_cleared_connection(boot):
    """REQ-1942: after creation the copy is the environment's own: clearing it leaves the source
    unreadable there, and Re-copy from parent copies prod's connection again."""
    before = len(boot.log_text())
    _environment(boot, "recopied", "inherit")
    path = f"/admin/orgs/{boot.org_id}/environments/recopied/sources"
    status, body = _call(boot, "PUT", f"{path}/sales-pg/binding", {"binding": "unbound"})
    assert status == 200, body
    status, body = _orders(boot, "recopied")
    assert status != 200, body
    status, body = _call(boot, "POST", f"{path}/recopy", {})
    assert status == 200 and "sales-pg" in body["sources"], body
    status, body = _orders(boot, "recopied")
    assert status == 200 and body["data"]["sql"] == [{"n": 2}], body
    # Each change of the environment's data drops its runtime and builds another, while passes
    # detached from the first build (job wiring, replica convergence) are still running for it.
    # Those are left to the next runtime: none of them is an error (the server once logged three
    # tracebacks here, "no runtime built for environment 'recopied'").
    time.sleep(5)  # the detached passes of the builds above have run by now
    since = boot.log_text()[before:]
    for failure in (
        "no runtime built",
        "did not converge",
        "poll-job wiring failed",
        "background task org-lifecycle",
    ):
        assert failure not in since, since[-4000:]


def test_an_unbound_environment_reads_through_no_binding_of_prods(boot):
    before = len(boot.log_text())
    _environment(boot, "base", "unbound")
    status, body = _orders(boot, "base")
    assert status != 200, (body, boot.log_text()[before:][-6000:])


def test_a_new_environments_mutations_are_refused_naming_it(boot):
    """REQ-1942: a new environment's mutation handling is Refused."""
    _environment(boot, "readonly", "inherit")
    status, body = _write(boot, "readonly")
    assert status != 200, body
    assert "'readonly'" in json.dumps(body) and "Refused" in json.dumps(body), body


def test_a_direct_mutation_never_writes_through_a_copied_connection(boot):
    _environment(boot, "direct", "inherit")
    status, body = _call(
        boot,
        "PATCH",
        f"/admin/orgs/{boot.org_id}/environments/direct/data",
        {"mutation_handling": "direct"},
    )
    assert status == 200, body
    status, body = _write(boot, "direct")
    assert status != 200, body
    assert "Direct" in json.dumps(body) and "sales-pg" in json.dumps(body), body
    status, rows = _orders(boot, None)
    assert rows["data"]["sql"] == [{"n": 2}]  # prod's rows are unchanged


def test_test_fake_is_refused_while_a_pii_column_has_no_fake(boot):
    status, body = _call(
        boot,
        "POST",
        f"/admin/orgs/{boot.org_id}/environments",
        {"name": "faked", "data_mode": "test_fake"},
    )
    assert status == 422, body
    assert "orders.region" in body["error"], body


def test_the_detail_shows_each_sources_binding_and_a_change_of_keys_asks_to_confirm(boot):
    _environment(boot, "detailed", "unbound")
    status, detail = _call(boot, "GET", f"/admin/orgs/{boot.org_id}/environments/detailed/detail")
    assert status == 200, detail
    assert detail["parent"] == "prod" and detail["data_mode"] == "unbound"
    assert detail["mutation_handling"] == "refused"
    assert {"id": "sales-pg", "type": "postgresql", "binding": "unbound"} in detail["sources"]
    assert detail["test_data"]["sensitive_without_fake"] == ["orders.region"]
    status, body = _call(
        boot,
        "PATCH",
        f"/admin/orgs/{boot.org_id}/environments/detailed/data",
        {"data_mode": "test_synthetic"},
    )
    assert status == 409 and "confirm" in json.dumps(body).lower(), body
    status, body = _call(
        boot,
        "POST",
        f"/admin/orgs/{boot.org_id}/environments/detailed/sources/recopy",
        {"sources": ["sales-pg"]},
    )
    assert status == 200 and body["sources"] == ["sales-pg"], body
    status, detail = _call(
        boot, "GET", f"/admin/orgs/{boot.org_id}/environments/detailed/detail", None
    )
    assert status == 200, detail
    assert {"id": "sales-pg", "type": "postgresql", "binding": "copied"} in detail["sources"]
    status, rows = _orders(boot, "detailed")
    assert status == 200 and rows["data"]["sql"] == [{"n": 2}], rows


def _rows(b, env: str | None) -> list[dict]:
    status, body = _call(
        b, "POST", "/data/sql", {"sql": "SELECT id, region FROM sales.orders ORDER BY id"}, env
    )
    assert status == 200, body
    return body["data"]["sql"]


def _sql(b, env: str, sql: str) -> None:
    status, body = _call(b, "POST", "/data/sql", {"sql": sql}, env)
    assert status == 200, body


def test_a_reversible_environment_keeps_its_mutations_and_resets_to_its_baseline(boot):
    """REQ-1942: under Reversible the parent's rows are never changed; the environment reads them
    with its kept mutations applied, and Reset mutations returns it to them."""
    prod = _rows(boot, None)
    _environment(boot, "kept", "inherit")
    status, body = _call(
        boot,
        "PATCH",
        f"/admin/orgs/{boot.org_id}/environments/kept/data",
        {"mutation_handling": "reversible"},
    )
    assert status == 200, body
    _sql(boot, "kept", "INSERT INTO sales.orders (id, region) VALUES (9, 'north')")
    _sql(boot, "kept", "UPDATE sales.orders SET region = 'south' WHERE id = 1")
    _sql(boot, "kept", "DELETE FROM sales.orders WHERE id = 2")
    assert _rows(boot, "kept") == [{"id": 1, "region": "south"}, {"id": 9, "region": "north"}]
    assert _rows(boot, None) == prod  # the parent's real rows are untouched
    status, detail = _call(boot, "GET", f"/admin/orgs/{boot.org_id}/environments/kept/detail")
    assert status == 200 and sum(detail["kept_mutations"].values()) == 3, detail
    status, body = _call(
        boot, "POST", f"/admin/orgs/{boot.org_id}/environments/kept/mutations/reset"
    )
    assert status == 200 and len(body["tables"]) == 1, body
    assert _rows(boot, "kept") == prod


def test_a_lane_repointed_to_its_own_database_reads_it_through_the_engine(boot):
    """REQ-1942 on DuckDB: an Unbound lane binds the source to a connection of its own -- here the
    same database reached another way, so the engine attaches it apart from prod's -- and a read
    the engine serves (its kept mutations apply) reads through that attach."""
    _environment(boot, "repointed", "unbound")
    pg_port = int(os.environ.get("PG_PORT", "5432"))
    status, body = _call(
        boot,
        "PUT",
        f"/admin/orgs/{boot.org_id}/environments/repointed/sources/sales-pg/binding",
        {
            "binding": "own",
            "host": "127.0.0.1",
            "port": pg_port,
            "database": boot.database,
            "username": "provisa",
            "password": os.environ.get("PG_PASSWORD", "provisa"),
        },
    )
    assert status == 200, body
    status, body = _call(
        boot,
        "PATCH",
        f"/admin/orgs/{boot.org_id}/environments/repointed/data",
        {"mutation_handling": "reversible"},
    )
    assert status == 200, body
    _sql(boot, "repointed", "INSERT INTO sales.orders (id, region) VALUES (7, 'east')")
    assert [r["id"] for r in _rows(boot, "repointed")] == [1, 2, 7]
    assert [r["id"] for r in _rows(boot, None)] == [1, 2]


def test_a_kept_truncate_hides_the_rows_before_it_and_reset_restores_them(boot):
    """REQ-1942: under Reversible a TRUNCATE is one marker: TRUNCATE then INSERT reads as just the
    inserts, and Reset mutations brings the parent's rows back."""
    prod = _rows(boot, None)
    _environment(boot, "emptied", "inherit")
    status, body = _call(
        boot,
        "PATCH",
        f"/admin/orgs/{boot.org_id}/environments/emptied/data",
        {"mutation_handling": "reversible"},
    )
    assert status == 200, body
    _sql(boot, "emptied", "INSERT INTO sales.orders (id, region) VALUES (8, 'early')")
    _sql(boot, "emptied", "TRUNCATE TABLE sales.orders")
    assert _rows(boot, "emptied") == []
    _sql(boot, "emptied", "INSERT INTO sales.orders (id, region) VALUES (9, 'north')")
    assert _rows(boot, "emptied") == [{"id": 9, "region": "north"}]
    assert _rows(boot, None) == prod  # the parent's real rows are untouched
    status, body = _call(
        boot, "POST", f"/admin/orgs/{boot.org_id}/environments/emptied/mutations/reset"
    )
    assert status == 200, body
    assert _rows(boot, "emptied") == prod


def test_a_kept_merge_writes_what_each_clause_would(boot):
    """REQ-1942: under Reversible a MERGE is kept as the versions its clauses would write."""
    prod = _rows(boot, None)
    _environment(boot, "merged", "inherit")
    status, body = _call(
        boot,
        "PATCH",
        f"/admin/orgs/{boot.org_id}/environments/merged/data",
        {"mutation_handling": "reversible"},
    )
    assert status == 200, body
    _sql(
        boot,
        "merged",
        "MERGE INTO sales.orders AS t USING (VALUES (1, 'south'), (5, 'new')) AS s(id, region) "
        "ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET region = s.region "
        "WHEN NOT MATCHED THEN INSERT (id, region) VALUES (s.id, s.region)",
    )
    assert _rows(boot, "merged") == [
        {"id": 1, "region": "south"},
        {"id": 2, "region": "west"},
        {"id": 5, "region": "new"},
    ]
    assert _rows(boot, None) == prod
