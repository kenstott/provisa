# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E (REQ-1919, DEMO ORGANISATIONS ARE THEIR CONFIG, maintainer ruling 2026-11-23): a restart of
the default demo replaces what its configuration declares and keeps what the admin registered.

The smoke check's isolated instance of the default demo (PROVISA_DEMO, embedded PostgreSQL control
plane, DuckDB engine), as ``start-ui-install.sh --demo`` runs it. Two tables of the demo's own
PostgreSQL source are registered through the admin: one in a domain the admin creates, one in a
domain the configuration declares. After a restart both are still registered and read. Then the
configuration drops a column of one of its own tables, and after the next restart that table is
as the configuration says, while the admin's tables are still there."""

# Requirements: REQ-1919

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml

from tests.smoke import test_main_is_startable as smoke
from tests.smoke.test_main_is_startable import REPO, Instance

# isolated: the instance is the smoke check's, which writes the demo's files and owns its ports.
pytestmark = [pytest.mark.integration, pytest.mark.isolated, pytest.mark.local_platform]

_ROLE = "org_admin"
_HEADERS = {"X-Provisa-Role": _ROLE}
_SOURCE = "smoke-pg"
_ADMIN_DOMAIN = "kept"
# A domain of the demo's own configuration.
_CONFIG_DOMAIN = "pet-store"
_config_path: dict[str, Path] = {}


@pytest.fixture(scope="module")
def instance(tmp_path_factory) -> Iterator[Instance]:
    """The smoke check's instance of the default demo, on a copy of its configuration this test
    can change between restarts."""
    from _pytest.monkeypatch import MonkeyPatch

    home = tmp_path_factory.mktemp("demo_keeps_admin_tables")
    config = yaml.safe_load((REPO / smoke.CONFIG).read_text())
    config["includes"] = [str(REPO / "config" / "provisa-install.yaml")]
    path = home / "config.yaml"
    path.write_text(yaml.safe_dump(config))
    _config_path["path"] = path
    mp = MonkeyPatch()
    mp.setattr(smoke, "CONFIG", str(path))
    try:
        yield from smoke.instance.__wrapped__()
    finally:
        mp.undo()


def _log_tail(inst: Instance) -> str:
    logs = sorted(inst.log_dir.glob("backend*.log"), key=lambda p: p.stat().st_mtime)
    return logs[-1].read_text()[-6000:]


def _gql(inst: Instance, query: str, variables: dict | None = None) -> dict[str, Any]:
    r = httpx.post(
        f"{inst.url}/admin/graphql",
        json={"query": query, "variables": variables or {}},
        headers=_HEADERS,
        timeout=300,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert "errors" not in body, body
    return body["data"]


def _rows(inst: Instance, sql: str) -> list[dict]:
    r = httpx.post(f"{inst.url}/data/sql", json={"sql": sql}, headers=_HEADERS, timeout=120)
    assert r.status_code == 200, f"{r.text[:2000]}\n{_log_tail(inst)}"
    body = r.json()
    assert "errors" not in body, f"{body}\n{_log_tail(inst)}"
    return body["data"]["sql"]


def _register(inst: Instance, table: str, domain: str) -> None:
    outcomes = _gql(
        inst,
        "mutation($inputs: [TableInput!]!) { registerTables(inputs: $inputs) "
        "{ tableName result { success message } } }",
        {
            "inputs": [
                {
                    "sourceId": _SOURCE,
                    "domainId": domain,
                    "schemaName": "public",
                    "tableName": table,
                    "columns": [
                        {"name": "id", "visibleTo": [_ROLE], "dataType": "integer"},
                        {"name": "label", "visibleTo": [_ROLE], "dataType": "varchar"},
                    ],
                }
            ]
        },
    )["registerTables"]
    assert all(o["result"]["success"] for o in outcomes), json.dumps(outcomes)


def _assert_admin_tables_read(inst: Instance) -> None:
    a = _rows(inst, 'SELECT id, label FROM "kept"."smoke_unregistered_a" ORDER BY id')
    assert [(r["id"], r["label"]) for r in a] == [(1, "a1"), (2, "a2")]
    b = _rows(inst, 'SELECT id, label FROM "pet_store"."smoke_unregistered_b" ORDER BY id')
    assert [(r["id"], r["label"]) for r in b] == [(1, "b1")]


def test_a_restart_keeps_the_admin_s_tables_and_replaces_the_config_s(instance: Instance):
    created = _gql(
        instance,
        "mutation($input: DomainInput!) { createDomain(input: $input) { success message } }",
        {"input": {"id": _ADMIN_DOMAIN}},
    )["createDomain"]
    assert created["success"], created
    _register(instance, "smoke_unregistered_a", _ADMIN_DOMAIN)
    _register(instance, "smoke_unregistered_b", _CONFIG_DOMAIN)
    _assert_admin_tables_read(instance)

    instance.restart_backend()
    _assert_admin_tables_read(instance)
    accounts = _rows(instance, 'SELECT * FROM "pet_store"."smoke_accounts" ORDER BY id')
    assert set(accounts[0]) == {"id", "owner"}, accounts

    # The configuration drops a column of its own table: the next start replaces the table.
    path = _config_path["path"]
    config = yaml.safe_load(path.read_text())
    (declared,) = [t for t in config["tables"] if t["table"] == "smoke_accounts"]
    declared["columns"] = [c for c in declared["columns"] if c["name"] != "owner"]
    path.write_text(yaml.safe_dump(config))

    instance.restart_backend()
    accounts = _rows(instance, 'SELECT * FROM "pet_store"."smoke_accounts" ORDER BY id')
    assert set(accounts[0]) == {"id"}, accounts
    _assert_admin_tables_read(instance)
