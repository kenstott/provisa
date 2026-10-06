# Copyright (c) 2026 Kenneth Stott
# Canary: 4a9e2c17-8b53-4f60-9d21-3c7e0b5a8f14
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The model catalog's tables are read-only on every data surface, on a real server.

The meta domain publishes the org's own catalog — roles, registered tables, their columns,
commands, tags, glossary — as views over the control-plane tables, so it can be queried. A write
through one of them would change the model without the model store (its dependency refusals,
origin, the rights a role may be granted), so no data surface takes one: SQL refuses it naming
the table and the operation, and GraphQL offers no mutation for any of them."""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request

import pytest
import sqlalchemy as sa

from tests.integration.worker_boot_harness import WorkerBoot, _config

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = int(os.environ.get("PG_PORT", "5432"))


@pytest.fixture(scope="module")
def server():
    base = _config(_PG_HOST, _PG_PORT, "unused")
    boot = WorkerBoot(
        1,
        pg_host=_PG_HOST,
        pg_port=_PG_PORT,
        extra_config={
            "tables": base["tables"],
            # org_admin is the reserved administrative role (REQ-1349): not declared.
            "roles": [
                {"id": "analyst", "capabilities": ["query_development"], "domain_access": ["*"]},
            ],
        },
        env={"PROVISA_REDIRECT_ENABLED": "false"},
    )
    boot.create_database()
    try:
        boot.start()
        boot.wait_all_ready(timeout=300)
        yield boot
    finally:
        boot.cleanup()


def _post(boot, path: str, body: dict) -> tuple[int, dict]:
    req = urllib.request.Request(
        f"http://127.0.0.1:{boot.ports['http']}{path}",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "x-provisa-role": "org_admin"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())


def _store(boot, sql: str) -> list[tuple]:
    """Read the control plane directly, from the schema that holds the org's catalog views."""
    engine = sa.create_engine(boot.url, isolation_level="AUTOCOMMIT")
    try:
        with engine.connect() as conn:
            schema = conn.execute(
                sa.text(
                    "SELECT table_schema FROM information_schema.views "
                    "WHERE table_name = 'roles_meta' LIMIT 1"
                )
            ).scalar_one()
            return [tuple(r) for r in conn.execute(sa.text(sql.format(schema=schema)))]
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    "sql",
    [
        "UPDATE meta.roles SET capabilities = '[\"cross_org\"]' WHERE id = 'analyst'",
        "UPDATE meta.registered_tables SET description = 'changed' WHERE table_name = 'orders'",
        "DELETE FROM meta.roles WHERE id = 'analyst'",
        "INSERT INTO meta.roles (id, capabilities) VALUES ('injected', '[]')",
    ],
)
def test_sql_takes_no_write_on_the_catalog(server, sql):
    status, body = _post(server, "/data/sql", {"sql": sql})
    assert status == 400 and body.get("code") == "data.write_not_supported", (status, body)
    roles = dict(_store(server, "SELECT id, capabilities::text FROM {schema}.roles"))
    assert "injected" not in roles and "cross_org" not in roles["analyst"], roles
    described = _store(
        server, "SELECT description FROM {schema}.registered_tables WHERE table_name = 'orders'"
    )
    assert described != [("changed",)], described


def test_graphql_offers_no_mutation_on_the_catalog(server):
    status, body = _post(
        server, "/data/graphql", {"query": "{ __schema { mutationType { fields { name } } } }"}
    )
    assert status == 200, body
    mutation_type = body["data"]["__schema"]["mutationType"]
    fields = [f["name"] for f in (mutation_type or {}).get("fields") or []]
    assert not [f for f in fields if f.startswith(("m__", "o__"))], fields


def _admin(boot, query: str) -> dict:
    status, body = _post(boot, "/admin/graphql", {"query": query})
    assert status == 200, body
    return body


def test_a_column_grant_on_the_catalog_does_not_open_it_to_writes(server):
    """An admin naming a role in a catalog column's writable_by (the admin API allows the edit)
    still leaves the catalog read-only: the grant is not a write route into the model."""
    status, body = _post(
        server,
        "/data/sql",
        {
            "sql": "SELECT schema_name FROM meta.registered_tables "
            "WHERE domain_id = 'meta' AND table_name = 'roles'"
        },
    )
    assert status == 200, body
    schema = body["data"]["sql"][0]["schema_name"]
    granted = _admin(
        server,
        f"""
        mutation {{
            updateTable(input: {{
                sourceId: "provisa-admin", domainId: "meta", schemaName: "{schema}",
                tableName: "roles",
                columns: [
                    {{ name: "id", dataType: "varchar", visibleTo: ["org_admin"], writableBy: ["org_admin"] }},
                    {{ name: "parent_role_id", dataType: "varchar", visibleTo: ["org_admin"], writableBy: ["org_admin"] }},
                    {{ name: "org_id", dataType: "varchar", visibleTo: ["org_admin"], writableBy: ["org_admin"] }},
                    {{ name: "capabilities", dataType: "json", visibleTo: ["org_admin"], writableBy: ["org_admin"] }},
                    {{ name: "tenant_id", dataType: "varchar", visibleTo: ["org_admin"], writableBy: ["org_admin"] }},
                    {{ name: "domain_id", dataType: "varchar", visibleTo: ["org_admin"], writableBy: ["org_admin"] }}
                ]
            }}) {{ success message }}
        }}
        """,
    )
    status, body = _post(
        server,
        "/data/sql",
        {"sql": "UPDATE meta.roles SET capabilities = '[\"cross_org\"]' WHERE id = 'analyst'"},
    )
    gql = _post(
        server,
        "/data/graphql",
        {
            "query": 'mutation { m__updateRoles(set: {capabilities: ["cross_org"]}, '
            'where: {id: {eq: "analyst"}}) { affected_rows } }'
        },
    )
    import psycopg

    try:
        with psycopg.connect(
            host="127.0.0.1",
            port=server.ports["pgwire"],
            user="org_admin",
            password="provisa",
            dbname="provisa",
            autocommit=True,
            connect_timeout=30,
        ) as conn:
            pg = conn.execute(
                "UPDATE meta.roles SET capabilities = '[\"cross_org\"]' WHERE id = 'analyst'"
            ).rowcount
    except psycopg.Error as exc:
        pg = str(exc)
    roles = dict(_store(server, "SELECT id, capabilities::text FROM {schema}.roles"))
    assert "cross_org" not in roles["analyst"], (granted, status, body, gql, pg, roles)
    assert status == 400 and body.get("code") == "data.write_not_supported", (gql, pg, status, body)
