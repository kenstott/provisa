# Copyright (c) 2026 Kenneth Stott
# Canary: 0248ac31-036a-48c3-824a-c4b5264f10a5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The standard roles' rows (REQ-2155) and the tenancy-dependent rights (REQ-1337): the model
store's writes of the roles table at schema init and boot. Kept apart from ``role`` so that
``provisa.core.db`` reaches them without importing the dependency guard."""

from __future__ import annotations

from typing import TYPE_CHECKING

from provisa.core.schema_org import roles

if TYPE_CHECKING:
    from provisa.core.database import Connection


def assert_standard_roles(sync_conn, schema: str) -> None:  # REQ-2155
    """Write every standard role into ``schema`` exactly as :data:`STANDARD_ROLES` defines it, on
    every init_schema: a standard role is Provisa's and locked, so its row is re-asserted rather
    than seeded once -- a right Provisa adds reaches every org and environment on its next start.
    The sandbox's demonstrated list is part of its definition (REQ-1602)."""
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    from provisa.core.db import STANDARD_ROLES, sandbox_demonstrated

    sync_conn.exec_driver_sql(f'SET LOCAL search_path TO "{schema}"')
    rows = [
        {
            "id": role_id,
            "description": spec.description,
            "capabilities": list(spec.capabilities),
            "parents": list(spec.parents),
            "demonstrated": sandbox_demonstrated() if role_id == "sandbox" else [],
            "domain_access": list(spec.domain_access),
            "org_id": None,
        }
        for role_id, spec in STANDARD_ROLES.items()
    ]
    stmt = pg_insert(roles).values(rows)
    sync_conn.execute(
        stmt.on_conflict_do_update(
            index_elements=["id"],
            set_={
                c: stmt.excluded[c]
                for c in ("description", "capabilities", "parents", "demonstrated", "domain_access")
            },
        )
    )
    from provisa.core.repositories.retired_roles import drop_retired_role_ids

    drop_retired_role_ids(sync_conn)  # REQ-1297, after platform_admin is written above


async def apply_tenancy_grants(conn: "Connection", *, multitenancy: bool) -> None:  # REQ-1337
    """Grant or withdraw the tenancy-dependent rights on the roles of the schema ``conn`` is set
    to (db.apply_tenancy_role_grants): ``platform_settings`` is org_admin's only in a
    single-tenant deployment.

    cross_org is withdrawn in BOTH modes -- org authority is confined to the org being acted in,
    so no role but platform_admin holds it however the deployment is configured, and holding it
    is what marks a role control-plane."""
    await conn.execute(
        "UPDATE roles SET capabilities = COALESCE("
        "  (SELECT jsonb_agg(v) FROM jsonb_array_elements(capabilities) v"
        "   WHERE v <> '\"cross_org\"'::jsonb), '[]'::jsonb)"
        " WHERE id <> 'platform_admin' AND capabilities ? 'cross_org'"
    )
    if multitenancy:
        await conn.execute(
            "UPDATE roles SET capabilities = COALESCE("
            "  (SELECT jsonb_agg(v) FROM jsonb_array_elements(capabilities) v"
            "   WHERE v <> '\"platform_settings\"'::jsonb), '[]'::jsonb)"
            " WHERE id = 'org_admin' AND capabilities ? 'platform_settings'"
        )
    else:
        await conn.execute(
            "UPDATE roles SET capabilities = capabilities || '[\"platform_settings\"]'::jsonb"
            " WHERE id = 'org_admin' AND NOT capabilities ? 'platform_settings'"
        )
