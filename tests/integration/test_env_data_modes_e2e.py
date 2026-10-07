# Copyright (c) 2026 Kenneth Stott
# Canary: e8f0700f-3d86-4988-9e2c-0c33411e0032
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An environment created as Inherit reads through its parent's bindings; one created as Unbound
reads through none (REQ-1491, REQ-1529, REQ-1538, REQ-1942), on a real server: a created
environment's sources carry no connection, and a statement routed to a source directly runs in the
inheriting environment on the connection prod bound."""

# Requirements: REQ-1491, REQ-1529, REQ-1538, REQ-1942

from __future__ import annotations

import json
import os
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


def test_an_inheriting_environment_reads_the_real_table_through_its_parents_binding(boot):
    before = len(boot.log_text())
    _environment(boot, "branch", "inherit")
    status, body = _orders(boot, "branch")
    since = boot.log_text()[before:]
    assert status == 200, (body, since[-4000:])
    assert body["data"]["sql"] == [{"n": 2}]
    # Its pool dialled prod's coordinates, not the stripped row's.
    assert "direct pool for 'sales-pg'" not in since, since[-4000:]


def test_an_unbound_environment_reads_through_no_binding_of_prods(boot):
    _environment(boot, "base", "unbound")
    status, body = _orders(boot, "base")
    assert status != 200, body


def test_a_new_environments_mutations_are_refused_naming_it(boot):
    """REQ-1942: a new environment's mutation handling is Refused."""
    _environment(boot, "readonly", "inherit")
    status, body = _write(boot, "readonly")
    assert status != 200, body
    assert "'readonly'" in json.dumps(body) and "Refused" in json.dumps(body), body


def test_a_direct_mutation_never_writes_through_an_inherited_source(boot):
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
    assert detail["test_data"]["pii_without_fake"] == ["orders.region"]
    status, body = _call(
        boot,
        "PATCH",
        f"/admin/orgs/{boot.org_id}/environments/detailed/data",
        {"data_mode": "test_synthetic"},
    )
    assert status == 409 and "confirm" in json.dumps(body).lower(), body
    status, body = _call(
        boot,
        "PUT",
        f"/admin/orgs/{boot.org_id}/environments/detailed/sources/sales-pg/binding",
        {"binding": "inherited"},
    )
    assert status == 200, body
    status, rows = _orders(boot, "detailed")
    assert status == 200 and rows["data"]["sql"] == [{"n": 2}], rows
