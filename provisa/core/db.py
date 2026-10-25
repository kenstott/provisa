# Copyright (c) 2026 Kenneth Stott
# Canary: ad492cac-4438-4e3a-88d8-315e26a58491
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Control-plane org/tenant bootstrap: role hardening and schema seeding."""

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from sqlalchemy.schema import CreateSchema

from provisa.core.environments import org_schema
from provisa.core.models import BUILT_IN_SOURCE_IDS

if TYPE_CHECKING:
    from provisa.core.database import Database


async def create_org_role(
    conn: Any, org_id: str, env: str | None = None
) -> None:  # REQ-699, REQ-889
    """Create a PG role scoped to org_<org_id> schema for physical multi-tenant isolation.

    Defense-in-depth hardening on the control plane's PostgreSQL (REQ-889, REQ-2014): Provisa
    governance roles are a Provisa-layer concept living in metadata tables, never a DB role system.
    """
    _validate_org_id(org_id)
    # REQ-1488: the role stays org-level — an environment multiplies the model, not the tenant — so
    # each of the org's environment schemas is granted to the one role the org already has.
    schema_name = org_schema(org_id, env)
    role_name = f"role_{org_id}"
    await conn.execute(
        f"DO $$ BEGIN CREATE ROLE {role_name}; EXCEPTION WHEN duplicate_object THEN NULL; END $$"
    )
    await conn.execute(f'GRANT USAGE, CREATE ON SCHEMA "{schema_name}" TO {role_name}')


def _validate_org_id(org_id: str) -> None:
    import re

    if not re.fullmatch(r"[a-zA-Z0-9_]+", org_id):
        raise ValueError(f"org_id must be alphanumeric/underscore only, got: {org_id!r}")


# Default domain rows seeded by schema.sql; FK targets other tenant rows depend
# on (domain_id='' must always resolve). Re-seeded on the portable path.
# REQ-1386: ops ships with org_admin as steward (REQ-609 — never PENDING). A demo's own domains
# (the pet store's pet-store and shelter) are its config's, never seeded: a seeded one appeared in
# every org and every other demo.
_SEED_DOMAINS: tuple[tuple[str, str, str | None], ...] = (
    ("", "No domain", None),
    ("meta", "System metadata", None),
    ("ops", "Operational telemetry", "org_admin"),
)

# REQ-2155: Provisa's standard roles, as provisa/core/standard_roles.yaml defines them (the one
# source; the file says what each is and why). Standard roles are LOCKED: init_schema re-asserts every
# one on each run (assert_standard_roles), and the admin surfaces refuse to edit or delete one
# (roles.standard_locked); an organisation extends a standard role only with a child role.


@dataclass(frozen=True)
class StandardRole:  # REQ-2155
    parents: tuple[str, ...]
    capabilities: tuple[str, ...]
    #: REQ-2080: what the role is for, as an access review reads it.
    description: str
    #: Where its capabilities apply; the role's own, never inherited.
    domain_access: tuple[str, ...]


def _load_standard_roles() -> dict[str, StandardRole]:
    from pathlib import Path

    import yaml

    raw = yaml.safe_load((Path(__file__).parent / "standard_roles.yaml").read_text())
    return {
        role_id: StandardRole(
            tuple(spec["parents"]),
            tuple(spec["capabilities"]),
            spec["description"],
            tuple(spec["domain_access"]),
        )
        for role_id, spec in raw.items()
    }


STANDARD_ROLES: dict[str, StandardRole] = _load_standard_roles()


def _standard_rights(role_id: str) -> frozenset[str]:
    spec = STANDARD_ROLES[role_id]
    return frozenset(spec.capabilities).union(*(_standard_rights(p) for p in spec.parents))


#: Each standard role's rights once its ancestors' are folded in, by id.
STANDARD_ROLE_RIGHTS: dict[str, frozenset[str]] = {
    rid: _standard_rights(rid) for rid in STANDARD_ROLES
}


def sandbox_demonstrated() -> list[str]:  # REQ-1602, REQ-1608
    """The rights the sandbox is SHOWN but does not hold: what org_admin holds that it does not,
    so its surfaces stay on the page, disabled and badged -- all but user_management, whose page
    would imply a visitor could confer roles or admit people (REQ-1608)."""
    return sorted(
        STANDARD_ROLE_RIGHTS["org_admin"] - STANDARD_ROLE_RIGHTS["sandbox"] - {"user_management"}
    )


#: What org_admin holds and the sandbox never does (REQ-2155): managing environments or their data,
#: conferring roles, and the org glossary's override. A demo role carrying one is refused.
SANDBOX_WITHHELD: frozenset[str] = (
    STANDARD_ROLE_RIGHTS["org_admin"] - STANDARD_ROLE_RIGHTS["sandbox"]
)


def standard_role_dicts() -> list[dict]:
    """The standard roles as role rows (id, capabilities, parents), for resolving a role defined
    outside a model store -- a demo's -- whose parents are standard roles."""
    return [
        {"id": rid, "capabilities": list(spec.capabilities), "parents": list(spec.parents)}
        for rid, spec in STANDARD_ROLES.items()
    ]


def fold_config_grants(tables: list[dict], config_roles: list[dict]) -> None:
    """Fold the column grants of ``tables`` over each role's ancestors -- the standard roles and
    ``config_roles``, a configuration's own -- as the runtime build does
    (security.inheritance.expand_column_grants): a column granted to data_steward is org_admin's.
    For a schema built from a configuration file, outside a model store (REQ-2155)."""
    from provisa.security.inheritance import expand_column_grants, role_chains

    own = [
        {
            "id": r["id"],
            "capabilities": r.get("capabilities") or [],
            "parents": r.get("parents") or [],
        }
        for r in config_roles
    ]
    expand_column_grants(tables, role_chains([*standard_role_dicts(), *own]))


# REQ-1919: what the deployment seeds into every model store. A seeded role is a standard role and
# is never deleted; none of these counts as the org's own catalog.
SEEDED_ROLE_IDS: frozenset[str] = frozenset(STANDARD_ROLES)
SEEDED_DOMAIN_IDS: frozenset[str] = frozenset(domain_id for domain_id, _d, _s in _SEED_DOMAINS)
#: The built-in sources and the demo's GraphQL source (provisa.api.app_startup).
SEEDED_SOURCE_IDS: frozenset[str] = BUILT_IN_SOURCE_IDS | {"graphql-demo"}


_SCHEMA_NAME = re.compile(r"^[A-Za-z0-9_]+$")

# The advisory lock every creator of control-plane tables holds: schema.sql (init_schema), the
# audit initializer, and the metadata reconcile (add_missing_columns).
SCHEMA_LOCK_KEY = 7337


def _validate_schema_name(schema: str) -> None:
    if not _SCHEMA_NAME.match(schema):
        raise ValueError(f"not a schema name: {schema!r}")


def _create_table_postgres(sync_conn, table) -> None:
    """CREATE ``table`` in the connection's current schema with every JSON column as JSONB — what
    ``schema.sql`` and the tables' own PostgreSQL DDL declare. Built from a copy, so the shared
    metadata (which the portable backends create from as it is) is not changed."""
    from sqlalchemy import MetaData
    from sqlalchemy.dialects.postgresql import JSONB

    scratch = MetaData()
    # The whole metadata is copied so the table's foreign keys resolve in the copy.
    copies = {t.name: t.to_metadata(scratch) for t in table.metadata.tables.values()}
    copy = copies[table.name]
    for column in copy.columns:
        if column.type.compile(sync_conn.dialect).upper() == "JSON":
            column.type = JSONB()
    copy.create(sync_conn, checkfirst=True)


def add_missing_columns(sync_conn, tables, schema: str | None = None) -> None:
    """Additive schema reconciliation: CREATE any metadata table absent from the live database, and
    ADD COLUMN any metadata column absent from a live table.

    V1 ships no migrations, so the SQLAlchemy metadata IS the schema's source of truth — but
    ``create_all`` skips tables that already exist, so a table (or a column on an existing table)
    added to the metadata after a database's initial creation never reaches it. This closes that
    gap for every plane: the portable tenant bootstrap (previously masked by boot eagerly copying a
    fresh SQLite file every time, which always saw a truly empty database and let ``create_all``
    create everything from scratch — no longer true now that boot persists the file, so an existing
    SQLite deployment hits the exact same gap Postgres always had), the platform registry, and —
    scoped by ``schema`` to one ``org_<id>`` schema — the PostgreSQL tenant plane, where
    ``schema.sql``'s ``ADD COLUMN IF NOT EXISTS`` blocks had to be hand-written for every new column
    and a forgotten one broke every upgrade at startup (cloud-dev: ``column "body_encoding" does not
    exist``; REQ-1742: ``provisa_sources`` added to schema_org.py's metadata but never to
    schema.sql's raw DDL, so it was never created for a Postgres deployment at all). Additive only:
    drops and type changes stay out of scope.

    A missing table is created WHERE ``schema`` SAYS and TYPED AS THE PLANE DECLARES IT. On
    PostgreSQL the reconciling connection is scoped to ``schema`` for its transaction
    (``SET LOCAL search_path``): a pooled engine connection carries no org search_path, and an
    unqualified CREATE on it lands in the role's default schema (``public``), which no org-scoped
    reader ever looks in. Its JSON columns are created JSONB, the rule the ADD COLUMN branch below
    applies: a table with a later initializer of its own (``query_audit_log``,
    ``audit.query_log.init_audit_schema``) is then already there as that initializer declares it,
    and what is built on it (``ops_table_usage`` unnests ``table_ids`` with jsonb functions) works.
    A created table has every metadata column, so it needs no column diffing this pass.
    """
    from sqlalchemy import inspect as _inspect

    postgres = sync_conn.dialect.name == "postgresql"
    if postgres:
        # One reconcile at a time, and never beside schema.sql (init_schema holds the same key
        # while it runs): a newly provisioned org is initialized by its provisioning AND by the
        # first request that reaches it, and two transactions that each find a table missing
        # both CREATE it — the second fails on the catalog's unique index. Taken before the
        # inspection below, held to this transaction's end, so the second sees what the first
        # created.
        sync_conn.exec_driver_sql(f"SELECT pg_advisory_xact_lock({SCHEMA_LOCK_KEY})")
    if schema and postgres:
        _validate_schema_name(schema)
        # Transaction-scoped: nothing of it stays on the pooled connection.
        sync_conn.exec_driver_sql(f'SET LOCAL search_path TO "{schema}"')
    inspector = _inspect(sync_conn)
    existing_tables = set(inspector.get_table_names(schema=schema))
    qualify = (lambda name: f'"{schema}"."{name}"') if schema else (lambda name: f'"{name}"')
    for table in tables:
        if table.name not in existing_tables:
            if postgres:
                _create_table_postgres(sync_conn, table)
            else:
                table.create(sync_conn, checkfirst=True)
            continue
        live = {c["name"] for c in inspector.get_columns(table.name, schema=schema)}
        for column in table.columns:
            if column.name in live:
                continue
            ddl_type = column.type.compile(sync_conn.dialect)
            if sync_conn.dialect.name == "postgresql" and ddl_type.upper() == "JSON":
                ddl_type = "JSONB"  # schema.sql declares every JSON column as JSONB
            default = ""
            if column.server_default is not None:
                arg = getattr(column.server_default, "arg", None)
                literal = str(getattr(arg, "text", arg))
                # Quote unless it is already a SQL literal (number, quoted string, bool).
                bare = literal.strip()
                is_sql_literal = (
                    bare.startswith("'")
                    or bare.replace(".", "", 1).isdigit()
                    or bare.lower() in ("true", "false", "null")
                )
                default = f" DEFAULT {bare}" if is_sql_literal else f" DEFAULT '{bare}'"
            sync_conn.exec_driver_sql(
                f'ALTER TABLE {qualify(table.name)} ADD COLUMN "{column.name}" {ddl_type}{default}'
            )


def assert_standard_roles(sync_conn, schema: str) -> None:  # REQ-2155
    """Write every standard role into ``schema`` exactly as :data:`STANDARD_ROLES` defines it, on
    every init_schema: a standard role is Provisa's and locked, so its row is re-asserted rather
    than seeded once -- a right Provisa adds reaches every org and environment on its next start.
    The sandbox's demonstrated list is part of its definition (REQ-1602)."""
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    from provisa.core.schema_org import roles

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


async def init_schema(
    pool: "Database",
    schema_sql: str,
    org_id: str = "default",
    env: str | None = None,
    *,
    region: str | None = None,
) -> None:
    """Execute schema SQL scoped to org_<org_id> schema (REQ-697) — the one ``region`` names, in
    that region's store (REQ-1922: its state and record), else the org's model.

    The control plane is PostgreSQL (REQ-2014): the raw ``schema.sql`` script runs inside an
    ``org_<id>`` schema."""
    _validate_org_id(org_id)
    schema_name = org_schema(org_id, env, region=region)
    async with pool.acquire() as conn:
        # This branch is PostgreSQL-only (non-PG returned above); the advisory lock is taken through
        # the abstraction so no PG-specific lock SQL appears here.
        async with conn.advisory_lock(SCHEMA_LOCK_KEY):
            await conn.execute_core(CreateSchema(schema_name, if_not_exists=True))
            await conn.execute_core(
                CreateSchema(
                    org_schema(org_id, env, "_mv_cache", region=region), if_not_exists=True
                )
            )
            await conn.execute(f'SET search_path TO "{schema_name}"')
            # schema_sql is a multi-statement script (DO $$ blocks). Raw asyncpg
            # runs it natively; the control-plane Database shim auto-detects the
            # multi-statement case and routes to the raw driver.
            from provisa.core import model_change

            async with model_change.layout():  # REQ-1524: the layout is not a model change
                await conn.execute(schema_sql)
    # Whatever schema.sql's hand-written ADD COLUMN blocks missed, the metadata supplies: a
    # column added to schema_org reaches an org schema created before it (see add_missing_columns).
    engine = getattr(pool, "engine", None)
    if engine is not None:
        from provisa.core import schema_org

        with engine.begin() as sa_conn:
            add_missing_columns(sa_conn, schema_org.metadata.sorted_tables, schema_name)
            assert_standard_roles(sa_conn, schema_name)  # REQ-2155
            # REQ-1914: the stamp rows and the triggers that advance them, once every table exists.
            from provisa.core import config_stamp

            config_stamp.install(
                sa_conn,
                config_stamp.TENANT_TABLES,
                schema_name,
                advanced=config_stamp.TENANT_ADVANCED,
            )


async def apply_tenancy_role_grants(  # REQ-1337
    pool: "Database", org_id: str, *, multitenancy: bool, env: str | None = None
) -> None:
    """Assert the tenancy-dependent role grants: ``platform_settings`` and ``cross_org``.

    Every other right of a standard role is its definition, re-asserted by init_schema
    (:func:`assert_standard_roles`, REQ-2155). The deployment-wide settings surface (federation
    engine, cache storage, encryption, auth provider, the config file, query-engine lifecycle) is
    gated by the ``platform_settings`` RIGHT, never by a role name. Which roles hold that right is
    the only thing tenancy decides:

    * single-tenant -- the org administrator IS the deployment operator, so org_admin holds it;
    * multitenant -- an org administers only its own data plane, so org_admin must NOT hold it and
      the grant is withdrawn (a deployment flipped to multitenant keeps no stale right).

    REQ-1623: ``env`` names the environment whose roles are being asserted, because an environment
    is a schema holding its OWN copy of the roles table (REQ-1488).
    """
    _validate_org_id(org_id)
    async with pool.acquire() as conn:
        await conn.execute(f'SET search_path TO "{org_schema(org_id, env)}"')  # REQ-1623
        # REQ-1337: cross_org is withdrawn in BOTH modes -- org authority is confined to the org
        # being acted in, so no role but platform_admin holds it however the deployment is
        # configured, and holding it is what marks a role control-plane.
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
