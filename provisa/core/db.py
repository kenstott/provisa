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
from typing import TYPE_CHECKING, Any

from sqlalchemy import insert, select, update
from sqlalchemy.schema import CreateSchema

from provisa.core.environments import org_schema
from provisa.core.models import BUILT_IN_SOURCE_IDS

if TYPE_CHECKING:
    from provisa.core.database import Database


async def create_org_role(
    conn: Any, org_id: str, env: str | None = None
) -> None:  # REQ-699, REQ-889
    """Create a PG role scoped to org_<org_id> schema for physical multi-tenant isolation.

    PostgreSQL-only hardening — a NO-OP on every other control-plane backend (REQ-889). Provisa
    governance roles are a Provisa-layer concept living in metadata tables, never a DB role system;
    this only adds defense-in-depth when Postgres is the control plane. Embedded/single-tenant
    homes have no role system to harden, so the metadata home stays portable.
    """
    _validate_org_id(org_id)
    # Default to postgresql for a raw asyncpg connection (which has no capabilities wrapper).
    dialect = getattr(getattr(conn, "capabilities", None), "dialect", "postgresql")
    if dialect != "postgresql":
        return
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
# REQ-1386: ops ships with org_admin as steward (REQ-609 — never PENDING).
_SEED_DOMAINS: tuple[tuple[str, str, str | None], ...] = (
    ("", "No domain", None),
    ("meta", "System metadata", None),
    ("ops", "Operational telemetry", "org_admin"),
    ("shelter", "Animal shelter staff and breed management", None),
)

# REQ-1266/REQ-1297/REQ-1597: the six system template roles, mirrored from schema.sql's seed (lines
# 623-712) so non-PostgreSQL (SQLite/portable) deployments reach parity with PostgreSQL/SaaS
# deployments on role seeding. schema.sql is PG-only DDL and cannot be shared verbatim, so this is
# a second literal by necessity, not by choice; keep the two in sync on any capability change.
# platform_settings/cross_org are intentionally absent from org_admin here — those are
# tenancy-conditional and asserted by apply_tenancy_role_grants after seeding, on every dialect.
# Named rather than inlined below because REQ-1597's sandbox role is defined by SUBTRACTING from it,
# and a second copy of the list is a second thing to remember on every capability change.
_ORG_ADMIN_CAPABILITIES: list[str] = [
    "source_registration",
    "table_registration",
    "create_relationship",
    "create_view",
    "approve_view",
    "approve_relationship",
    "access_config",
    "user_management",
    "masking_config",
    "column_grant",
    "view_governance",
    "query_development",
    "full_results",
    "write",
    "usage",
    "org_settings",
    "observability",
    # REQ-1573: the two environment rights. Creating and deleting an environment spends the
    # org's plan ceiling and drops a schema; being served by one other than prod is working
    # somewhere that is not production. org_admin and developer hold both; analyst and
    # modeler hold neither.
    "environment_management",
    "environment_switch",
    # REQ-1942: changing an environment's data choices -- its data mode, its sources' bindings,
    # read-only or read-write, its synthetic settings -- including creating it reading real data.
    # org_admin ALONE: a developer switching an environment to Test (fake) with no fakes declared
    # would expose real data.
    "environment_data",
    # REQ-1943: revealing or hiding a sensitive column -- its sensitive tags, the Sensitive data
    # option on a tag, its role masks, fake, synthetic rule and column grants. org_admin by
    # default, meant to be granted to a data steward; no developer holds it.
    "sensitive_data",
    # REQ-1590: the glossary's two rights. Reading a term is not administering the org, so
    # every seeded role reads; curation stays with the roles that own the model.
    "glossary_read",
    "glossary_rw",
    # REQ-1592: the org's glossary owner — curates any term whatever its domains and
    # whoever authored it. org_admin ALONE: it is the override that makes the enterprise
    # scope and the author lock safe, so a term whose authors have all left, or one scoped
    # to the whole org, still has someone who can maintain it.
    "org_glossary_rw",
    # REQ-1634: data products' two rights. Curating the catalog (create/delete) stays with
    # org_admin, on the same footing as table_registration; reading it is granted separately
    # below to analyst/developer/modeler.
    "data_product_read",
    "data_product_rw",
]

# REQ-1597: the rights sandbox does NOT inherit from org_admin -- see the seed entry below.
# REQ-1602 originally denied org_settings/observability wholesale, on the theory that they were
# narrow org-wide surfaces. In practice REQ-1349 made org_settings the single right nearly every
# org-scoped admin page is gated on (cache, AI models, import, tags, secrets, scheduler, requests,
# org-engine, billing, domains -- see adminNavCapabilities.test.ts), so denying it took out most of
# the Admin tab, not the few surfaces this list intended. The design (per REQ-1597/REQ-1598) is that
# a sandbox visitor can do everything the product does except reach past the environment it was
# minted in and write back to the shared sample sources it points at -- so the denylist narrows to
# exactly that: leaving/managing environments, conferring roles, and overriding the org's own
# glossary terms. Viewing settings and telemetry is no longer withheld.
_SANDBOX_DENIED: frozenset[str] = frozenset(
    {
        "environment_switch",
        "environment_management",
        "user_management",
        "org_glossary_rw",
    }
)

# What the reconcile seam subtracts when it re-derives ``sandbox`` from the org_admin row it has
# just re-asserted: REQ-1597's denylist, plus ``platform_settings``, which is not an org_admin
# right but the tenancy answer the seam writes onto org_admin afterwards (single-tenant only) and
# which a visitor never holds.
_SANDBOX_REDERIVE_DENIED: frozenset[str] = _SANDBOX_DENIED | {"platform_settings"}

_SEED_ROLES: tuple[tuple[str, list[str]], ...] = (
    ("org_admin", _ORG_ADMIN_CAPABILITIES),
    (
        "analyst",
        ["usage", "query_development", "glossary_read", "data_product_read"],
    ),  # REQ-1590, REQ-1634
    (
        "developer",
        [
            "query_development",
            "create_view",
            "create_relationship",
            "full_results",
            "write",
            "usage",
            "environment_management",  # REQ-1573
            "environment_switch",  # REQ-1573
            "glossary_read",  # REQ-1590
            "data_product_read",  # REQ-1634
        ],
    ),
    # REQ-1297: modeler is the only system role holding ignore_relationships — the discovery role
    # that determines the model by joining across relations the catalog does not yet cover.
    (
        "modeler",
        [
            "query_development",
            "create_relationship",
            "create_view",
            "ignore_relationships",
            "full_results",
            "usage",
            # REQ-1590: modeler is the model-curation role, so it curates the glossary too.
            "glossary_read",
            "glossary_rw",
            "data_product_read",  # REQ-1634
        ],
    ),
    # REQ-1944: data_steward owns a domain's governance and nothing operational; its edits are
    # checked against the domain of what they change. Must match schema.sql's seed row.
    (
        "data_steward",
        [
            "access_config",
            "column_grant",
            "masking_config",
            "sensitive_data",
            "glossary_read",
            "glossary_rw",
            "data_product_read",
            "data_product_rw",
            "usage",
            "query_development",
            "full_results",
            "view_governance",
        ],
    ),
    # REQ-1597: sandbox is what a "Try it Out" invitation confers. It is org_admin's capability list
    # minus a DENYLIST, rather than a list built up from analyst, because the point of the sandbox is
    # that a stranger can do everything the product does — register a source, model it, govern it,
    # query it, write to it — inside an environment that expires. Enumerating what they may do would
    # make every new capability invisible to them until someone remembered to add it here; taking
    # away is the direction that stays correct.
    #
    # Four rights are withheld, each because it reaches something the environment does not contain:
    # environment_switch would leave the sandbox (REQ-1596 pins the membership to it, and the pin
    # would be pointless against a role that could name another); environment_management would spend
    # the org's plan ceiling and can drop another environment's schemas; user_management would let a
    # visitor confer roles or admit more people; org_glossary_rw is the override over terms the org's
    # own people authored.
    ("sandbox", sorted(set(_ORG_ADMIN_CAPABILITIES) - _SANDBOX_DENIED)),
    # REQ-1327/REQ-1337: the two platform rights and nothing else — no data capability, and no
    # capability that stands in for one.
    ("platform_admin", ["platform_settings", "cross_org"]),
)

# REQ-1602/REQ-1608: rights a role is SHOWN but does not hold. Three of the sandbox's four withheld
# rights stay on the page, disabled and badged as belonging to the production system -- hiding them
# would make the sandbox look like a smaller product rather than the same one with the org's controls
# held back. `user_management` is the exception (REQ-1608): letting a sandbox visitor see a page that
# implies they could confer roles or admit people, even inertly, misrepresents what the role can ever
# do here, so /team stays a hard NotAuthorized instead of a demonstration.
# Every other role's absence of a right means the same thing it always did -- nothing to show.
_DEMONSTRATED_ROLES: dict[str, list[str]] = {
    "sandbox": sorted(_SANDBOX_DENIED - {"user_management"})
}

# REQ-1919: what the deployment seeds into every model store. A seeded role is a system role and
# is never deleted; none of these counts as the org's own catalog.
SEEDED_ROLE_IDS: frozenset[str] = frozenset(role_id for role_id, _caps in _SEED_ROLES)
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


async def _init_schema_portable(pool: "Database") -> None:
    """Bootstrap the tenant plane from portable SQLAlchemy metadata.

    ``schema.sql`` is PostgreSQL-only DDL (SERIAL/JSONB/DO $$/advisory locks) and
    does not parse on SQLite/MySQL. The ``schema_org`` metadata is the dialect-
    neutral mirror; ``create_all`` emits per-dialect DDL. Org isolation is the
    default schema on these single-tenant backends (no ``search_path``)."""
    from provisa.core import schema_org
    from provisa.core.schema_org import domains, roles

    with pool.engine.begin() as conn:
        schema_org.metadata.create_all(conn)
        # ``create_all`` skips tables that already exist, so a column added to the metadata never
        # reaches an existing SQLite/MySQL file — the portable equivalent of schema.sql's
        # ALTER ... ADD COLUMN IF NOT EXISTS blocks.
        add_missing_columns(conn, schema_org.metadata.sorted_tables)
        # REQ-1914: the stamp rows and the triggers that advance them, once every table exists.
        from provisa.core import config_stamp

        config_stamp.install(
            conn, config_stamp.TENANT_TABLES, advanced=config_stamp.TENANT_ADVANCED
        )
    async with pool.acquire() as conn:
        for domain_id, description, steward in _SEED_DOMAINS:
            result = await conn.execute_core(select(domains.c.id).where(domains.c.id == domain_id))
            if result.fetchone() is None:
                await conn.execute_core(
                    insert(domains).values(id=domain_id, description=description, steward=steward)
                )
        for role_id, capabilities in _SEED_ROLES:
            demonstrated = _DEMONSTRATED_ROLES.get(role_id, [])
            result = await conn.execute_core(select(roles.c.id).where(roles.c.id == role_id))
            if result.fetchone() is None:
                await conn.execute_core(
                    insert(roles).values(
                        id=role_id,
                        capabilities=capabilities,
                        demonstrated=demonstrated,
                        # A role holding the cross-org right is the control plane, which reaches
                        # no data domain (REQ-1337).
                        domain_access=[] if "cross_org" in capabilities else ["*"],
                        org_id=None,
                    )
                )
            elif demonstrated:
                # REQ-1602: the demonstrated list is part of the role's definition, and the seed
                # above cannot reach a row an earlier release already created -- the same seam the
                # tenancy grants re-assert their rights through.
                await conn.execute_core(
                    roles.update().where(roles.c.id == role_id).values(demonstrated=demonstrated)
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

    PostgreSQL runs the raw ``schema.sql`` script inside an ``org_<id>`` schema.
    Non-PG backends bootstrap from portable ``schema_org`` metadata instead."""
    _validate_org_id(org_id)
    # A raw asyncpg pool (no Database shim) has no .dialect and is always PostgreSQL — run the
    # native schema.sql path. Only the portable SQLAlchemy Database routes non-PG backends.
    if getattr(pool, "dialect", "postgresql") != "postgresql":
        await _init_schema_portable(pool)
        return
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
            # REQ-1914: the stamp rows and the triggers that advance them, once every table exists.
            from provisa.core import config_stamp

            config_stamp.install(
                sa_conn,
                config_stamp.TENANT_TABLES,
                schema_name,
                advanced=config_stamp.TENANT_ADVANCED,
            )


async def _apply_tenancy_role_grants_portable(pool: "Database", *, multitenancy: bool) -> None:
    """Portable (SQLite/non-PG) mirror of ``apply_tenancy_role_grants``'s tenancy-conditional UPDATEs.

    Same rights, same rules, run through SQLAlchemy Core instead of PG jsonb operators since the
    portable ``roles.capabilities`` column round-trips as a plain Python list."""
    from provisa.core.schema_org import roles

    async with pool.acquire() as conn:
        result = await conn.execute_core(select(roles.c.id, roles.c.capabilities))
        for role_id, capabilities in result.fetchall():
            caps = set(capabilities or [])
            changed = False
            if role_id != "platform_admin" and "cross_org" in caps:
                caps.discard("cross_org")
                changed = True
            # REQ-1573: the two environment rights, held by org_admin and developer alike. Same
            # reason as the org_admin block below: the seed cannot add a right to a role row an
            # earlier release already created.
            if role_id in ("org_admin", "developer"):
                for right in ("environment_management", "environment_switch"):
                    if right not in caps:
                        caps.add(right)
                        changed = True
            # REQ-1590: the glossary's two rights — every system role reads, org_admin and modeler
            # curate. Same seam and same reason as the environment rights above.
            if role_id in ("org_admin", "analyst", "developer", "modeler"):
                if "glossary_read" not in caps:
                    caps.add("glossary_read")
                    changed = True
            if role_id in ("org_admin", "modeler"):
                if "glossary_rw" not in caps:
                    caps.add("glossary_rw")
                    changed = True
            # REQ-1942: org_admin alone changes an environment's data choices.
            if role_id == "org_admin" and "environment_data" not in caps:
                caps.add("environment_data")
                changed = True
            # REQ-1943: and, by default, alone reveals or hides a sensitive column.
            if role_id == "org_admin" and "sensitive_data" not in caps:
                caps.add("sensitive_data")
                changed = True
            # REQ-1592: org_admin alone owns the org's glossary — see the seed table above.
            if role_id == "org_admin" and "org_glossary_rw" not in caps:
                caps.add("org_glossary_rw")
                changed = True
            # REQ-1634: data products' two rights — org_admin curates, org_admin/analyst/
            # developer/modeler read. Same seam and same reason as the glossary rights above.
            if role_id in ("org_admin", "analyst", "developer", "modeler"):
                if "data_product_read" not in caps:
                    caps.add("data_product_read")
                    changed = True
            if role_id == "org_admin" and "data_product_rw" not in caps:
                caps.add("data_product_rw")
                changed = True
            if role_id == "org_admin":
                for right in ("org_settings", "observability"):
                    if right not in caps:
                        caps.add(right)
                        changed = True
                if multitenancy and "platform_settings" in caps:
                    caps.discard("platform_settings")
                    changed = True
                elif not multitenancy and "platform_settings" not in caps:
                    caps.add("platform_settings")
                    changed = True
            if changed:
                await conn.execute_core(
                    update(roles).where(roles.c.id == role_id).values(capabilities=sorted(caps))
                )
        # REQ-1597: sandbox is org_admin minus a denylist, and the seed cannot reach a sandbox row an
        # earlier release created -- so a right added to org_admin above (data_product_read among
        # them) never reached an existing sandbox row, and the visitor's derived org_admin below
        # then re-read the stale list. Re-derived here, from the org_admin row as re-asserted above.
        admin_row = (
            await conn.execute_core(select(roles.c.capabilities).where(roles.c.id == "org_admin"))
        ).fetchone()
        if admin_row is not None:
            await conn.execute_core(
                update(roles)
                .where(roles.c.id == "sandbox")
                .values(capabilities=sorted(set(admin_row[0] or []) - _SANDBOX_REDERIVE_DENIED))
            )
        # REQ-1624: the derived roles are re-read LAST, exactly as on PostgreSQL -- see the comment
        # at the end of apply_tenancy_role_grants for what re-asserting over a subtraction did.
        derived = (
            await conn.execute_core(
                select(roles.c.id, roles.c.defined_from).where(roles.c.defined_from.isnot(None))
            )
        ).fetchall()
        for role_id, source in derived:
            if role_id == source:
                continue
            row = (
                await conn.execute_core(
                    select(roles.c.capabilities, roles.c.demonstrated).where(roles.c.id == source)
                )
            ).fetchone()
            if row is None:
                raise ValueError(
                    f"role {role_id!r} is defined from {source!r}, which this schema does not have"
                )
            await conn.execute_core(
                update(roles)
                .where(roles.c.id == role_id)
                .values(capabilities=row[0], demonstrated=row[1])
            )


async def apply_tenancy_role_grants(  # REQ-1337
    pool: "Database", org_id: str, *, multitenancy: bool, env: str | None = None
) -> None:
    """Assert the tenancy-dependent role grants: ``platform_settings`` and ``cross_org``.

    Also re-asserts the two tenancy-INDEPENDENT org_admin rights (``org_settings``,
    ``observability``, REQ-1349). The schema.sql seed uses ``ON CONFLICT (id) DO NOTHING`` and so
    cannot add a right to an org_admin row an earlier release already created; this is the seam
    where an existing deployment picks them up, on the next ``init_schema``.

    The deployment-wide settings surface (federation engine, cache storage, encryption, auth
    provider, the config file, query-engine lifecycle) is gated by the ``platform_settings`` RIGHT,
    never by a role name. Which roles hold that right is the only thing tenancy decides:

    * single-tenant — the org administrator IS the deployment operator, so org_admin holds it;
    * multitenant — an org administers only its own data plane, so org_admin must NOT hold it and
      the grant is withdrawn (a deployment flipped to multitenant keeps no stale right).

    Runs on every ``init_schema``, so the seed re-asserts the mode's grant rather than depending on
    when the schema was first created. platform_admin always holds it (seeded in schema.sql).

    REQ-1623: ``env`` names the environment whose roles are being asserted, because an environment
    is a schema holding its OWN copy of the roles table (REQ-1488). Fixed at ``org_<id>`` this
    re-asserted prod's rights every time a non-prod environment's runtime was built — a write into
    prod from an environment, leaving the environment's own roles carrying whatever the copy held.
    """
    _validate_org_id(org_id)
    if getattr(pool, "dialect", "postgresql") != "postgresql":
        await _apply_tenancy_role_grants_portable(pool, multitenancy=multitenancy)
        return
    async with pool.acquire() as conn:
        await conn.execute(f'SET search_path TO "{org_schema(org_id, env)}"')  # REQ-1623
        # REQ-1337: cross_org is withdrawn in BOTH modes — org authority is confined to the org
        # being acted in, so org_admin never holds it however the deployment is configured. Only
        # platform_admin carries it (schema.sql), and holding it is what marks a role control-plane.
        await conn.execute(
            "UPDATE roles SET capabilities = COALESCE("
            "  (SELECT jsonb_agg(v) FROM jsonb_array_elements(capabilities) v"
            "   WHERE v <> '\"cross_org\"'::jsonb), '[]'::jsonb)"
            " WHERE id <> 'platform_admin' AND capabilities ? 'cross_org'"
        )
        # REQ-1349: org-scoped admin rights, granted to org_admin in BOTH tenancy modes. They name
        # surfaces that are always the org's own — its AI/NL provider overrides, domains, scheduled
        # tasks, creation requests, and its read-only performance views — so no tenancy condition
        # applies. Re-asserted here because the seed cannot update a pre-existing role row.
        for right in ("org_settings", "observability"):
            await conn.execute(
                "UPDATE roles SET capabilities = capabilities || "
                f"'[\"{right}\"]'::jsonb"
                f" WHERE id = 'org_admin' AND NOT capabilities ? '{right}'"
            )
        # REQ-1573: environments are their own right rather than a facet of org_settings — a
        # developer manages and switches them while holding no org settings at all, and an analyst
        # holds neither. Both roles carry both rights; nothing here names analyst or modeler.
        for right in ("environment_management", "environment_switch"):
            await conn.execute(
                "UPDATE roles SET capabilities = capabilities || "
                f"'[\"{right}\"]'::jsonb"
                f" WHERE id IN ('org_admin', 'developer') AND NOT capabilities ? '{right}'"
            )
        # REQ-1942, REQ-1943: an environment's data choices and a sensitive column's hiding are
        # org_admin's by default.
        for right in ("environment_data", "sensitive_data"):
            await conn.execute(
                "UPDATE roles SET capabilities = capabilities || "
                f"'[\"{right}\"]'::jsonb"
                f" WHERE id = 'org_admin' AND NOT capabilities ? '{right}'"
            )
        # REQ-1590: the glossary's two rights, on the same terms as the seed — every system role
        # reads, and curation stays with the roles that own the model. Re-asserted for the same
        # reason as the rights above: an org whose role rows predate REQ-1590 keeps them otherwise,
        # which hides the glossary nav link and 403s the surface for its own org_admin.
        for role_ids, right in (
            (("org_admin", "analyst", "developer", "modeler"), "glossary_read"),
            (("org_admin", "modeler"), "glossary_rw"),
            # REQ-1592: org_admin alone owns the org's glossary — see the seed table above.
            (("org_admin",), "org_glossary_rw"),
            # REQ-1634: data products' two rights — same seam as the glossary pair above.
            (("org_admin", "analyst", "developer", "modeler"), "data_product_read"),
            (("org_admin",), "data_product_rw"),
        ):
            id_list = ", ".join(f"'{r}'" for r in role_ids)
            await conn.execute(
                "UPDATE roles SET capabilities = capabilities || "
                f"'[\"{right}\"]'::jsonb"
                f" WHERE id IN ({id_list}) AND NOT capabilities ? '{right}'"
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
        # REQ-1597: sandbox is org_admin minus a denylist, and the seed's ON CONFLICT DO NOTHING
        # cannot reach a sandbox row an earlier release created -- so every right the blocks above
        # add to org_admin (data_product_read among them) was missing from an existing sandbox row,
        # and the visitor's derived org_admin (re-read below) inherited the stale list. Re-derived
        # here, from org_admin as just re-asserted, so the subtraction stays the only author.
        denied = ", ".join(f"'\"{r}\"'::jsonb" for r in sorted(_SANDBOX_REDERIVE_DENIED))
        await conn.execute(
            "UPDATE roles t SET capabilities = COALESCE("
            "  (SELECT jsonb_agg(v ORDER BY v) FROM jsonb_array_elements(s.capabilities) v"
            f"   WHERE v NOT IN ({denied})), '[]'::jsonb)"
            " FROM roles s WHERE s.id = 'org_admin' AND t.id = 'sandbox'"
        )
        # REQ-1624: LAST, and after every re-assertion above. A role whose `defined_from` names
        # another is DERIVED from it in this schema -- the sandbox visitor's `org_admin`, which
        # REQ-1597 defines by subtraction from org_admin and env_copy.adopt_role_definition applies
        # in the visitor's own environment. Every block above re-asserts org_admin's rights into
        # whatever schema is being asserted, environment schemas included, so the subtraction was
        # given back on the visitor's next runtime build: a sandbox visitor recovered
        # environment_management and reached the org's environments surface, other visitors'
        # environments and all. Re-reading the definition here is what makes the withholding hold.
        await conn.execute(
            "UPDATE roles t SET capabilities = s.capabilities, demonstrated = s.demonstrated"
            " FROM roles s WHERE t.defined_from = s.id AND t.id <> s.id"
        )
