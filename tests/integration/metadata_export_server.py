# Copyright (c) 2026 Kenneth Stott
# Canary: 6c1e9a47-2b58-4f03-9d71-8e4a0c5f2b16
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A real Provisa server publishing its metadata through the production path (REQ-2119).

Every metadata-export end-to-end test publishes the same way an admin does: a Provisa server
reads two tables of the test stack's Postgres, registered as the two members of a data product;
the target is saved through ``PUT /admin/metadata-export`` and the snapshot published through
``POST /admin/metadata-export/publish`` -- the one publish the drain and the reconcile also run
(``publishing.publish_snapshot``). No adapter is constructed by the test and no transport is
faked: what the target holds afterwards is what production would have sent it.

The model carries what the adapters map: a stewarded domain, a data product with every ODPS
field, two member tables with column descriptions, a masked column, a column hidden from a role,
an RLS rule and an approved relationship.
"""

# Requirements: REQ-2119

from __future__ import annotations

import os
import tempfile
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
import yaml

from provisa.core.db import SEEDED_ROLE_IDS, STANDARD_ROLES
from provisa.security.rights import ORG_ADMIN_ROLE

SCHEMA = "mdx_e2e"
SOURCE = "mdx-wh"
DOMAIN = "sales"
PRODUCT = "customer_360"
PRODUCT_FIELDS = {
    "id": PRODUCT,
    "domain_id": DOMAIN,
    "name": "Customer 360",
    "owner_role": "data_owner",
    "team_role": "analyst",
    "purpose": "One view of each customer and their orders",
    "limitations": "Orders before 2020 are not loaded",
    "usage": "Join orders to customers on customer_id",
    "version": "1.2.0",
    "status": "active",
    "sla": "Refreshed hourly",
    "support": "Ask in the data team's channel",
    "support_contact": "data-team@example.com",
    "custom_properties": {"cost_center": "CC-42"},
}
MASK_PATTERN = r"(\d{3})-(\d{2})-(\d{4})"
RLS_FILTER = "region = 'eu'"
ORDERS_AMOUNT_DESCRIPTION = "Order total in cents"
MEMBER_TTL = 3600
GLOSSARY_TERM = "customer"
GLOSSARY_DEFINITION = "A person or company that has placed at least one order."
# REQ-2141/REQ-1389: a Class term grounded in the orders table. Beside it, GLOSSARY_TERM is the
# Property orders.customer_id derives; customers has no Class, so its key customers.id grounds
# that Property too. Each catalog receives three assignments: orders, and the two columns.
CLASS_TERM = "order"
CLASS_DEFINITION = "A customer's request to buy, placed once."
# A view over orders, a member too: its column lineage is what a target's lineage holds.
# The roles a catalog's governance tags name: the model's, and every standard role (REQ-2155) that
# reaches a domain -- org_admin among them, platform_admin (which reaches none) not. A column read
# by the model's roles alone is visibility-restricted for the standard ones.
MODEL_ROLES = ["analyst", "data_owner", "admin"]
AUDIENCE = sorted({*MODEL_ROLES, *(r for r in SEEDED_ROLE_IDS if STANDARD_ROLES[r].domain_access)})

VIEW = "order_totals"
VIEW_SQL = f'SELECT o.id AS id, o.amount * 2 AS net FROM "{DOMAIN}"."orders" o'


def _pg_env(name: str) -> str:
    return os.environ[name]


async def seed_member_tables() -> None:
    """The two member tables, in the test stack's Postgres."""
    import asyncpg

    conn = await asyncpg.connect(
        host=_pg_env("PG_HOST"),
        port=int(_pg_env("PG_PORT")),
        user=_pg_env("PG_USER"),
        password=_pg_env("PG_PASSWORD"),
        database=_pg_env("PG_DATABASE"),
        timeout=15,
    )
    try:
        await conn.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
        await conn.execute(f"CREATE SCHEMA {SCHEMA}")
        await conn.execute(f"CREATE TABLE {SCHEMA}.customers (id int PRIMARY KEY, name text)")
        await conn.execute(
            f"CREATE TABLE {SCHEMA}.orders (id int PRIMARY KEY, customer_id int, "
            "amount numeric, region text, ssn text, margin numeric)"
        )
        await conn.execute(f"INSERT INTO {SCHEMA}.customers VALUES (1, 'Ada'), (2, 'Grace')")
        await conn.execute(
            f"INSERT INTO {SCHEMA}.orders VALUES "
            "(10, 1, 1250, 'eu', '123-45-6789', 300), (11, 2, 900, 'us', '987-65-4321', 120)"
        )
    finally:
        await conn.close()


def model_config(
    engine: str,
    *,
    with_view: bool = True,
    product: str = PRODUCT,
    domain: str = DOMAIN,
    product_name: str = PRODUCT_FIELDS["name"],
) -> dict[str, Any]:
    """The server's configuration: the governed model every target is published from. A target
    that shares the members' physical objects (Snowflake Horizon, Dataplex) takes the product
    without the view, which is no object of the engine's: two members, as its scenario reads.
    ``product`` is the data product's id: a test publishing into a shared cloud account names it
    for the run (provisa_e2e_*). ``domain`` is the members' domain: a catalog that files a product
    under a domain of its own name (Purview) is given the one that exists there. ``product_name``
    is its name: a catalog read back by name is given the run's own."""
    roles = MODEL_ROLES
    # Every role, the org's own admin among them: the tests read as org_admin, the role the
    # server's default assignment gives.
    everyone = AUDIENCE
    config = {
        "federation_engine": engine,
        "auth": {
            "provider": "none",
            "assignments_source": "provisa",
            "default_assignments": [{"domain_id": "*", "role_id": "org_admin"}],
        },
        "naming": {"domain_prefix": False, "rules": []},
        "cache": {"enabled": False},
        "domains": [
            {"id": domain, "description": "Sales domain", "steward": "data_owner"},
            # Unstewarded on purpose: a catalog must show the governance gap.
            {"id": "lab", "description": "Lab domain"},
        ],
        "sources": [
            {
                "id": SOURCE,
                "type": "postgresql",
                "description": "Sales warehouse",
                "host": "${env:PG_HOST}",
                "port": "${env:PG_PORT}",
                "database": "${env:PG_DATABASE}",
                "username": "${env:PG_USER}",
                "password": "${env:PG_PASSWORD}",
            }
        ],
        # REQ-2155: a standard role (analyst) is every org's already; a config declares only its own.
        "roles": [
            {"id": r, "description": f"{r} role.", "capabilities": [], "domain_access": ["*"]}
            for r in roles
            if r not in SEEDED_ROLE_IDS
        ],
        "data_products": [
            {**PRODUCT_FIELDS, "id": product, "domain_id": domain, "name": product_name}
        ],
        "tables": [
            {
                "source_id": SOURCE,
                "domain_id": domain,
                "schema_name": SCHEMA,
                "table_name": "customers",
                "description": "Customers of the shop",
                # REQ-1907: a member the engine lands (Snowflake, BigQuery) is re-read on its own
                # cache_ttl; it is also the member's freshness in its contract.
                "cache_ttl": MEMBER_TTL,
                "product_id": product,
                "columns": [
                    {"name": "id", "data_type": "integer", "visible_to": everyone},
                    {
                        "name": "name",
                        "data_type": "text",
                        "visible_to": everyone,
                        "description": "Full name",
                    },
                ],
            },
            {
                "source_id": SOURCE,
                "domain_id": domain,
                "schema_name": SCHEMA,
                "table_name": "orders",
                "description": "Orders placed",
                "class_term": CLASS_TERM,
                # REQ-1907: a member the engine lands (Snowflake, BigQuery) is re-read on its own
                # cache_ttl; it is also the member's freshness in its contract.
                "cache_ttl": MEMBER_TTL,
                "product_id": product,
                "columns": [
                    {
                        "name": "id",
                        "data_type": "integer",
                        "visible_to": everyone,
                        "is_primary_key": True,
                    },
                    {"name": "customer_id", "data_type": "integer", "visible_to": everyone},
                    {
                        "name": "amount",
                        "data_type": "numeric",
                        "visible_to": everyone,
                        "description": ORDERS_AMOUNT_DESCRIPTION,
                    },
                    {"name": "region", "data_type": "text", "visible_to": everyone},
                    {
                        "name": "ssn",
                        "data_type": "text",
                        # The masked column's audience is the org's roles: masked for all but
                        # admin, as the catalogs' restricted and exempt roles show it. The org's
                        # admin reads it too, so a catalog published as it receives the column
                        # (REQ-2152).
                        "visible_to": everyone,
                        "masks": [
                            {"to": ["admin"], "mask": None},
                            {
                                "mask": {
                                    "type": "regex",
                                    "pattern": MASK_PATTERN,
                                    "replace": "XXX-XX-\\3",
                                },
                            },
                        ],
                    },
                    # Restricted: the seeded roles do not read it. The org's admin does, so a
                    # catalog published as it receives the column (REQ-2152).
                    {
                        "name": "margin",
                        "data_type": "numeric",
                        "visible_to": ["admin", ORG_ADMIN_ROLE],
                    },
                ],
            },
            {
                "source_id": "__derived__",
                "domain_id": domain,
                "schema_name": "views",
                "table_name": VIEW,
                "description": "Each order's net total",
                "product_id": product,
                "view_sql": VIEW_SQL.replace(f'"{DOMAIN}"', f'"{domain}"'),
                "columns": [
                    {"name": "id", "data_type": "integer", "visible_to": everyone},
                    {"name": "net", "data_type": "numeric", "visible_to": everyone},
                ],
            },
        ],
        "glossary_terms": [
            {"name": GLOSSARY_TERM, "definition": GLOSSARY_DEFINITION, "domains": [domain]},
            {"name": CLASS_TERM, "definition": CLASS_DEFINITION, "domains": [domain]},
        ],
        "relationships": [
            {
                "id": "orders-to-customers",
                "source_table_id": "orders",
                "target_table_id": "customers",
                "source_column": "customer_id",
                "target_column": "id",
                "cardinality": "many-to-one",
            }
        ],
        "rls_rules": [
            {
                "table_id": "orders",
                "role_id": "analyst",
                "filter": RLS_FILTER,
                "description": "Analysts see EU orders.",
            }
        ],
    }
    if not with_view:
        config["tables"] = [t for t in config["tables"] if t["table_name"] != VIEW]
    return config


async def entitle(client: httpx.AsyncClient, org_id: str) -> None:
    """Set the org's tier to the one metadata export needs, as a platform administrator does in
    Admin > Orgs (REQ-1066): stored with the org, read by the feature check."""
    response = await client.put(f"/admin/orgs/{org_id}/tier", json={"tier": "premium"})
    assert response.status_code == 200, response.text


@asynccontextmanager
async def published_server(
    org_id: str,
    *,
    engine: str = "trino",
    env: dict[str, str] | None = None,
    with_view: bool = True,
    product: str = PRODUCT,
    domain: str = DOMAIN,
    product_name: str = PRODUCT_FIELDS["name"],
) -> AsyncGenerator[httpx.AsyncClient]:
    """A started Provisa server for ``org_id`` with the model above, as an HTTP client."""
    from tests.integration.isolated_server import IsolatedServer, drop_org_schema

    await seed_member_tables()
    with tempfile.TemporaryDirectory() as tmp:
        config = Path(tmp) / "provisa.yaml"
        config.write_text(
            yaml.safe_dump(
                model_config(
                    engine,
                    with_view=with_view,
                    product=product,
                    domain=domain,
                    product_name=product_name,
                ),
                sort_keys=False,
            )
        )
        server = IsolatedServer(org_id, engine=engine, config=str(config), env=env)
        server.start()
        try:
            async with httpx.AsyncClient(base_url=server.base_url, timeout=300.0) as client:
                await entitle(client, org_id)
                yield client
        finally:
            server.stop_process()
            await drop_org_schema(org_id)


# A target's settings that are the export's own; the rest are its catalog's (REQ-2122).
EXPORT_SETTINGS = ("datazone_project_id", "delta_share", "role")
#: The role each catalog is published as (REQ-2152): what it is sent is what that role reads. The
#: org's admin reads every column of the model, so every catalog receives all of it.
PUBLISH_ROLE = ORG_ADMIN_ROLE


async def org_domains(client: httpx.AsyncClient) -> list[str]:
    """Every domain of the org, as the admin API lists them."""
    answer = await client.post("/admin/graphql", json={"query": "{ domains { id } }"})
    assert answer.status_code == 200, answer.text
    return [d["id"] for d in answer.json()["data"]["domains"]]


async def save_targets(client: httpx.AsyncClient, targets: list[dict[str, Any]]) -> None:
    """Connect each target's catalog for every domain of the org in the Catalogs panel, and export
    to each, both as an admin does (REQ-2122). Each target is named by its ``id``."""
    cleared = await client.put("/admin/metadata-export", json={"enabled": False, "targets": []})
    assert cleared.status_code == 200, cleared.text
    domains = await org_domains(client)
    catalogs = [
        {**{k: v for k, v in t.items() if k not in EXPORT_SETTINGS}, "domains": domains}
        for t in targets
    ]
    connected = await client.put("/admin/catalogs", json={"catalogs": catalogs})
    assert connected.status_code == 200, connected.text
    exports = [
        {"catalog": t["id"], "role": PUBLISH_ROLE, **{k: t[k] for k in EXPORT_SETTINGS if k in t}}
        for t in targets
    ]
    saved = await client.put("/admin/metadata-export", json={"enabled": True, "targets": exports})
    assert saved.status_code == 200, saved.text


def refused(outcome: dict[str, Any]) -> str:
    """A publish outcome's errors, each in full: a non-string assertion message is shortened."""
    return "\n".join(f"{e['asset']}: {e['message']}" for e in outcome["errors"])


async def publish(client: httpx.AsyncClient, target: dict[str, Any]) -> dict[str, Any]:
    """Save ``target`` as the org's one export target and publish now, both as an admin does;
    what the publish reports for that target. The target is named after its provider."""
    await save_targets(client, [{"id": target["provider"], **target}])
    published = await client.post("/admin/metadata-export/publish")
    assert published.status_code == 200, published.text
    (outcome,) = published.json()["targets"]
    return outcome
