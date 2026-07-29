# Copyright (c) 2026 Kenneth Stott
# Canary: 3de609ff-6421-4f6e-9d77-5c7c93e20416
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The deployment's own tenancy, as a queryable dataset in the root org (REQ-1301).

A platform_admin asked "who runs org acme, and when did it appear" had to drop to SQL against the
admin schema, because the org registry is control-plane state reachable only through admin
endpoints. This builds a read-only SQL view in the root org's schema — one row per org per
org_admin — so the same question is answerable through the query, graph and API surfaces every
other dataset is.

Two facts have to be joined and they live in different places: the org row is admin-plane
(``orgs`` + ``user_profiles``), while who holds org_admin is tenant-plane, one
``org_<id>.user_role_assignments`` table per org. The view therefore names each org's schema
explicitly and is regenerated whenever the set of orgs changes (create, delete). Between
regenerations it still reads live rows — it is a view, not a snapshot.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from provisa.core.database import Database

log = logging.getLogger(__name__)

VIEW_NAME = "org_registry"

# The columns the view exposes, in order. Named here so the seeding path and the tests agree on one
# list rather than each restating the SELECT.
VIEW_COLUMNS = (
    "org_id",
    "org_name",
    "provisioning_state",
    "created_at",
    "org_admin_user_id",
    "org_admin_email",
    "org_admin_display_name",
)


class RegistryViewUnavailable(RuntimeError):
    """The org registry cannot be exposed as a view on this deployment.

    Raised, never swallowed into a no-op: the view is REQ-1301's whole deliverable, so a caller that
    asked for it has to learn it did not happen and why. The seeding path catches this to log the
    reason and continue, because a deployment whose planes live in separate databases is a supported
    topology and must still boot.
    """


def root_schema_name(tenant_db: "Database") -> str:
    """The schema the view is built in: the one the root org's own connection is scoped to.

    Read from the handle rather than composed as ``org_<id>`` so the view always lands where that
    org's tables actually are — the schema name is the tenant plane's fact, and this module is not
    the place that decides it.
    """
    if not tenant_db.search_path:
        raise RegistryViewUnavailable(
            "the root org's connection carries no search_path, so the schema to build the registry "
            "view in is unknown"
        )
    return tenant_db.search_path


async def _admin_schema_name(admin_db: "Database") -> str:
    """The schema the admin-plane tables sit in, as the tenant connection would have to name them.

    The platform plane is deliberately unscoped (``bring_up_platform`` passes no ``search_path``),
    so its tables land in whatever schema that connection resolves first — ``public`` on a stock
    deployment, a test schema under a harness. Ask the connection rather than assuming.
    """
    if admin_db.search_path:
        return admin_db.search_path
    async with admin_db.acquire() as conn:
        schema = await conn.fetchval("SELECT current_schema()")
    if not schema:
        raise RegistryViewUnavailable(
            "the platform plane's connection resolves no current schema, so its tables cannot be "
            "named from the tenant plane"
        )
    return str(schema)


def build_view_sql(*, root_schema: str, admin_schema: str, org_schemas: dict[str, str]) -> str:
    """The CREATE OR REPLACE VIEW statement.

    ``org_schemas`` maps org id to the schema holding that org's ``user_role_assignments``. An org
    whose schema is missing (still provisioning, or provisioning failed) is simply absent from the
    admin-holder union — it keeps its registry row, with no org_admin, which is the true state.
    """
    if org_schemas:
        holders = " UNION ALL ".join(
            f"SELECT {_quote_literal(org_id)} AS org_id, user_id FROM {schema}.user_role_assignments"
            f" WHERE role_id = 'org_admin'"
            for org_id, schema in sorted(org_schemas.items())
        )
    else:
        # No org has a provisioned schema yet. The typed empty relation keeps the view's column
        # types identical to the populated form, so the registered column metadata never shifts.
        holders = "SELECT NULL::text AS org_id, NULL::text AS user_id WHERE false"
    return (
        f"CREATE OR REPLACE VIEW {root_schema}.{VIEW_NAME} AS\n"
        f"SELECT o.id AS org_id,\n"
        f"       o.name AS org_name,\n"
        f"       o.provisioning_state AS provisioning_state,\n"
        f"       o.created_at AS created_at,\n"
        f"       a.user_id AS org_admin_user_id,\n"
        f"       p.email AS org_admin_email,\n"
        f"       p.display_name AS org_admin_display_name\n"
        f"FROM {admin_schema}.orgs o\n"
        f"LEFT JOIN ({holders}) a ON a.org_id = o.id\n"
        f"LEFT JOIN {admin_schema}.user_profiles p ON p.user_id = a.user_id"
    )


def _quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


async def refresh_org_registry_view(*, tenant_db: "Database", admin_db: "Database") -> str:
    """(Re)create the registry view in the root org's schema and return the SQL that made it.

    Raises :class:`RegistryViewUnavailable` when the topology cannot support it: a non-PostgreSQL
    plane, or planes in separate databases (the tenant connection cannot see the admin tables, and
    PostgreSQL has no cross-database view).
    """
    if tenant_db.dialect != "postgresql" or admin_db.dialect != "postgresql":
        raise RegistryViewUnavailable(
            f"the registry view is PostgreSQL DDL; planes are tenant={tenant_db.dialect} "
            f"admin={admin_db.dialect}"
        )
    admin_schema = await _admin_schema_name(admin_db)
    root_schema = root_schema_name(tenant_db)

    async with admin_db.acquire() as conn:
        rows = await conn.fetch("SELECT id FROM orgs")
    org_ids = [r[0] for r in rows]

    async with tenant_db.acquire() as conn:
        visible = await conn.fetchval(f"SELECT to_regclass('{admin_schema}.orgs')")
        if visible is None:
            raise RegistryViewUnavailable(
                f"the tenant plane cannot see {admin_schema}.orgs — the two control planes are in "
                f"separate databases, which PostgreSQL cannot join in a view"
            )
        org_schemas: dict[str, str] = {}
        for org_id in org_ids:
            schema = f"org_{org_id}"
            present = await conn.fetchval(
                f"SELECT to_regclass('{schema}.user_role_assignments')"
            )
            if present is not None:
                org_schemas[org_id] = schema
        sql = build_view_sql(
            root_schema=root_schema, admin_schema=admin_schema, org_schemas=org_schemas
        )
        # The view's column list changes shape between the empty and populated forms only in the
        # subquery, but PostgreSQL still refuses CREATE OR REPLACE across a column-type change, so
        # drop first. The view holds no data, and it is rebuilt in the same statement batch.
        await conn.execute(f"DROP VIEW IF EXISTS {root_schema}.{VIEW_NAME}")
        await conn.execute(sql)
    log.info("org registry view %s.%s rebuilt over %d org(s)", root_schema, VIEW_NAME, len(org_ids))
    return sql
