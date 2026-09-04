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

from typing import TYPE_CHECKING, Any

from sqlalchemy import insert, select, update
from sqlalchemy.schema import CreateSchema

from provisa.core.environments import org_schema

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
    await conn.execute(f"GRANT USAGE, CREATE ON SCHEMA {schema_name} TO {role_name}")


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
    # REQ-1590: the glossary's two rights. Reading a term is not administering the org, so
    # every seeded role reads; curation stays with the roles that own the model.
    "glossary_read",
    "glossary_rw",
    # REQ-1592: the org's glossary owner — curates any term whatever its domains and
    # whoever authored it. org_admin ALONE: it is the override that makes the enterprise
    # scope and the author lock safe, so a term whose authors have all left, or one scoped
    # to the whole org, still has someone who can maintain it.
    "org_glossary_rw",
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

_SEED_ROLES: tuple[tuple[str, list[str]], ...] = (
    ("org_admin", _ORG_ADMIN_CAPABILITIES),
    ("analyst", ["usage", "query_development", "glossary_read"]),  # REQ-1590
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
    ("platform_admin", ["admin", "superadmin", "platform_settings", "cross_org"]),
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


def add_missing_columns(sync_conn, tables) -> None:
    """Additive column reconciliation: ADD COLUMN any metadata column absent from a live table.

    V1 ships no migrations, so the SQLAlchemy metadata IS the schema's source of truth — but
    ``create_all`` skips tables that already exist, so a column added to the metadata never reaches
    a database created before it. This closes that gap for the metadata-driven planes (the portable
    tenant bootstrap and the platform registry). Additive only: drops and type changes stay out of
    scope, as they do on the PostgreSQL ``schema.sql`` path.
    """
    from sqlalchemy import inspect as _inspect

    inspector = _inspect(sync_conn)
    existing_tables = set(inspector.get_table_names())
    for table in tables:
        if table.name not in existing_tables:
            continue
        live = {c["name"] for c in inspector.get_columns(table.name)}
        for column in table.columns:
            if column.name in live:
                continue
            ddl_type = column.type.compile(sync_conn.dialect)
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
                f'ALTER TABLE {table.name} ADD COLUMN "{column.name}" {ddl_type}{default}'
            )


async def _init_schema_portable(pool: "Database") -> None:
    """Bootstrap the tenant plane from portable SQLAlchemy metadata.

    ``schema.sql`` is PostgreSQL-only DDL (SERIAL/JSONB/DO $$/advisory locks) and
    does not parse on SQLite/MySQL. The ``schema_org`` metadata is the dialect-
    neutral mirror; ``create_all`` emits per-dialect DDL. Org isolation is the
    default schema on these single-tenant backends (no ``search_path``)."""
    from provisa.core import schema_org
    from provisa.core.schema_org import domains, roles

    async with pool.engine.begin() as conn:
        await conn.run_sync(schema_org.metadata.create_all)
        # ``create_all`` skips tables that already exist, so a column added to the metadata never
        # reaches an existing SQLite/MySQL file — the portable equivalent of schema.sql's
        # ALTER ... ADD COLUMN IF NOT EXISTS blocks.
        await conn.run_sync(add_missing_columns, schema_org.metadata.sorted_tables)
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
                        domain_access=["*"],
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
    pool: "Database", schema_sql: str, org_id: str = "default", env: str | None = None
) -> None:
    """Execute schema SQL scoped to org_<org_id> schema (REQ-697).

    PostgreSQL runs the raw ``schema.sql`` script inside an ``org_<id>`` schema.
    Non-PG backends bootstrap from portable ``schema_org`` metadata instead."""
    _validate_org_id(org_id)
    # A raw asyncpg pool (no Database shim) has no .dialect and is always PostgreSQL — run the
    # native schema.sql path. Only the portable SQLAlchemy Database routes non-PG backends.
    if getattr(pool, "dialect", "postgresql") != "postgresql":
        await _init_schema_portable(pool)
        return
    schema_name = org_schema(org_id, env)
    async with pool.acquire() as conn:
        # This branch is PostgreSQL-only (non-PG returned above); the advisory lock is taken through
        # the abstraction so no PG-specific lock SQL appears here.
        async with conn.advisory_lock(7337):
            await conn.execute_core(CreateSchema(schema_name, if_not_exists=True))
            await conn.execute_core(
                CreateSchema(org_schema(org_id, env, "_mv_cache"), if_not_exists=True)
            )
            await conn.execute(f"SET search_path TO {schema_name}")
            # schema_sql is a multi-statement script (DO $$ blocks). Raw asyncpg
            # runs it natively; the control-plane Database shim auto-detects the
            # multi-statement case and routes to the raw driver.
            await conn.execute(schema_sql)


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
            # REQ-1592: org_admin alone owns the org's glossary — see the seed table above.
            if role_id == "org_admin" and "org_glossary_rw" not in caps:
                caps.add("org_glossary_rw")
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
        await conn.execute(f"SET search_path TO {org_schema(org_id, env)}")  # REQ-1623
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
        # REQ-1590: the glossary's two rights, on the same terms as the seed — every system role
        # reads, and curation stays with the roles that own the model. Re-asserted for the same
        # reason as the rights above: an org whose role rows predate REQ-1590 keeps them otherwise,
        # which hides the glossary nav link and 403s the surface for its own org_admin.
        for role_ids, right in (
            (("org_admin", "analyst", "developer", "modeler"), "glossary_read"),
            (("org_admin", "modeler"), "glossary_rw"),
            # REQ-1592: org_admin alone owns the org's glossary — see the seed table above.
            (("org_admin",), "org_glossary_rw"),
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
