# Copyright (c) 2026 Kenneth Stott
# Canary: f3c5f955-837c-47bc-82e9-742ab45c2384
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SQLAlchemy Core metadata for the **platform control plane**.

Also referred to as the admin data model. This is the global registry shared
across all tenants/orgs; it must live in a single logical location (not
duplicated per-org schema). Its counterpart is the **tenant control plane**
(per-org), defined in ``provisa/core/schema_org.py``.

Contents (shared across all orgs, single logical location):

- Org registry and membership: ``orgs``, ``user_profiles``,
  ``user_org_memberships``, ``local_users``, ``org_invites``
- SaaS billing: the plan/limit/Lemon-Squeezy/KMS columns on ``orgs`` (REQ-1355 —
  the org is the billing subject; there is no separate ``tenants`` row), plus the
  per-org encrypted config in ``org_config``

Mirrors the post-migration shape of the corresponding tables in
``provisa/core/schema.sql`` and ``provisa/api/billing/org_db.py`` with
portable types (see ``provisa/core/schema_org.py`` for the type mapping).

Cross-model references to the per-org ``roles`` table (``org_invites.role_id``)
are kept as plain columns, not ForeignKeys, since the org model may live in a
separate schema/engine.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import (
    JSON,
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    LargeBinary,
    MetaData,
    PrimaryKeyConstraint,
    Table,
    Text,
    UniqueConstraint,
    Uuid,
    false,
    func,
    select,
    true,
)

if TYPE_CHECKING:
    from provisa.core.database import Database

metadata = MetaData()


orgs = Table(
    "orgs",
    metadata,
    Column("id", Text, primary_key=True),
    Column("name", Text, nullable=False),
    Column("created_by", Text),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    # REQ-1266: async provisioning lifecycle. server_default="ready" keeps every pre-existing
    # org queryable (V1 re-runs create_all, no migration); a self-service create inserts
    # "provisioning" and a background task flips it to "ready" or "failed" (+ provisioning_error,
    # persisted not swallowed).
    Column("provisioning_state", Text, nullable=False, server_default="ready"),
    Column("provisioning_error", Text),
    # REQ-1266: whether this org was seeded with the shared demo config. The per-request org
    # router reads it to rebuild the org's data-plane runtime after a process restart (registry
    # is in-memory, no TTL) — a demo org reloads the demo sources, an empty org stays empty.
    Column("seeded_demo", Boolean, nullable=False, server_default=false()),
    # REQ-1268: optional email-address rule (a regular expression, e.g. "@acme\\.com$"). When set,
    # a user redeeming an invite to join this org must have an authenticated email that matches, or
    # the join is rejected. NULL means no restriction (any email may join). Validated as a
    # compilable regex at org-creation time.
    Column("email_rule", Text),
    # REQ-1269: when true, a newly authenticated user whose email matches email_rule (or any user
    # if email_rule is NULL) is auto-granted membership with auto_join_role — no invite needed.
    # When false (default), joining requires an explicit invite. auto_join_role is the tenant-plane
    # role granted on auto-join (e.g. a low-privilege "analyst"); it must exist in the org schema.
    Column("auto_join", Boolean, nullable=False, server_default=false()),
    Column("auto_join_role", Text),
    # REQ-1043/REQ-1067/REQ-1244: when true this org runs on its OWN federation-engine instance
    # (a dedicated Trino coordinator / per-org embedded engine) instead of the shared/pooled
    # engine. Chosen at org creation (pre-billing surface: the onboarding create-org checkbox);
    # the org-runtime builder reads it to bind a dedicated EngineRuntime.
    Column("isolated_engine", Boolean, nullable=False, server_default=false()),
    # REQ-1450: which shared shard a pooled-lane org queries. The shared lane is not one
    # coordinator but a set of them ("shared_1", "shared_2", …), each a Deployment on its own node
    # pool; the org row is what says which. Meaningless when isolated_engine is true — that org has
    # a coordinator of its own and no shard to be placed on. server_default is the first shard, so
    # an org that predates sharding is on it rather than on nothing (V1 re-runs create_all).
    Column("shard", Text, nullable=False, server_default="shared_1"),
    # REQ-1412: an org may instead point its federation at a coordinator IT operates (bring your
    # own engine). Set means EXTERNAL: the org's runtime binds a terminal at this host/port rather
    # than the shared coordinator or a SaaS-dedicated one. NULL means the mode is decided by
    # isolated_engine (true = isolated, false = shared) — the three modes are derived from these
    # two columns, never stored twice.
    Column("external_engine_host", Text),
    Column("external_engine_port", Integer),
    # REQ-1418: which engine KIND the org's own coordinator is. NULL means the deployment's kind
    # (PROVISA_ENGINE / persisted federation_engine) — the case every shared-lane and isolated org
    # is in. An external org may instead run a kind of its own: the value is an _ENGINE_BUILDERS
    # key, so an org can point Provisa at Databricks, Snowflake, BigQuery, ClickHouse, Fabric,
    # Synapse or any SQLAlchemy URL while the deployment stays on Trino.
    Column("engine_kind", Text),
    # REQ-1418: the DSN for a URL-addressed engine kind (databricks:// snowflake:// …), encrypted
    # at rest with the same process-wide service as api_sources.auth (REQ-686) because it carries
    # the org's warehouse token. Host/port kinds (trino-byo) use the two columns above instead —
    # which of the two an org fills is decided by the chosen kind's ENGINE_REGISTRY config_fields,
    # never by sniffing the value.
    Column("engine_url_enc", LargeBinary),
    # REQ-1355: the org IS the billing subject. These columns were the ``tenants`` table, whose
    # UUID pk duplicated the org and forced every billing call site to carry a second identifier.
    # The externally-visible billing key is now the org slug.
    Column("plan", Text, nullable=False, server_default="trial"),
    Column("source_limit", Integer, nullable=False, server_default="2"),
    Column("ls_customer_id", Text),  # Lemon Squeezy customer id (REQ-1075)
    Column("ls_subscription_id", Text),  # Lemon Squeezy subscription id (REQ-1075)
    # Nullable, unlike the NOT NULL ``tenants.kms_key_arn``: an org created through onboarding
    # exists before billing initializes it, and there is no key to invent for it (REQ-693 rejects
    # client-side decrypt when this is unset, which is the correct state for such an org).
    Column("kms_key_arn", Text),
)

user_profiles = Table(
    "user_profiles",
    metadata,
    Column("user_id", Text, primary_key=True),
    Column("email", Text),
    Column("display_name", Text),
    # User-owned profile details (REQ-1266). display_name/email mirror the IdP token on every
    # request (_upsert_profile); given_name/family_name are entered by the user via PATCH
    # /auth/profile and are NEVER overwritten by the IdP mirror (excluded from its update_columns),
    # because Firebase/OIDC ID tokens do not carry a first/last split.
    Column("given_name", Text),
    Column("family_name", Text),
    Column("provider", Text),
    Column("last_seen", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

user_org_memberships = Table(
    "user_org_memberships",
    metadata,
    Column("user_id", Text, nullable=False),
    Column("org_id", Text, ForeignKey("orgs.id", ondelete="CASCADE"), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    PrimaryKeyConstraint("user_id", "org_id"),
)

# REQ-1306: a deliberate departure must not be undone by the next sign-in. When a user leaves an
# org whose auto_join rule still matches their email, the resolver would re-add them immediately;
# this row records the refusal. It is keyed to the (user, org) pair, so it suppresses only that
# org's auto-join and only for that person. An invitation or an explicit add clears it — those are
# affirmative acts, unlike a rule match.
org_auto_join_optouts = Table(
    "org_auto_join_optouts",
    metadata,
    Column("user_id", Text, nullable=False),
    Column("org_id", Text, ForeignKey("orgs.id", ondelete="CASCADE"), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    PrimaryKeyConstraint("user_id", "org_id"),
)

local_users = Table(
    "local_users",
    metadata,
    Column("id", Text, primary_key=True),
    Column("username", Text, nullable=False, unique=True),
    Column("password_hash", Text, nullable=False),
    Column("email", Text),
    Column("display_name", Text),
    Column("roles", JSON, nullable=False, default=list),
    Column("attributes", JSON, nullable=False, default=dict),
    Column("is_active", Boolean, nullable=False, server_default=true()),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

# REQ-1266: single-administrator bootstrap (limited Firebase/IdP mode). A fixed single-row
# lock (id always 1) that atomically records which authenticated user_id claimed the sole
# super-admin slot. The first login INSERTs id=1 (first writer wins the race — see
# AuthMiddleware bootstrap gate); every later, unrecorded user is denied. No multi-org, no
# second admin. The row IS the claim; the CheckConstraint forbids a second slot.
superadmin_bootstrap = Table(
    "superadmin_bootstrap",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=False),
    Column("user_id", Text, nullable=False),
    Column("claimed_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    CheckConstraint("id = 1", name="superadmin_bootstrap_singleton"),
)

org_invites = Table(
    "org_invites",
    metadata,
    Column("token", Text, primary_key=True),
    Column("org_id", Text, ForeignKey("orgs.id", ondelete="CASCADE"), nullable=False),
    Column("role_id", Text),  # cross-model ref -> org.roles
    # REQ-1287: the invitee's email, when the inviter addressed the invite to a person rather than
    # minting a shareable link. Nullable because a link invite is addressed to nobody. This is what
    # lets a just-authenticated user with no membership be TOLD they have a pending invitation
    # instead of having to already possess the token.
    Column("email", Text),
    Column("created_by", Text, nullable=False),
    Column("expires_at", DateTime(timezone=True), nullable=False),
    Column("used_at", DateTime(timezone=True)),
    Column("used_by", Text),
)

# REQ-1263: personal access tokens — the long-lived credential every non-browser protocol
# accepts. A token is shown once at issuance and stored only as a SHA-256 hash, so a registry
# read cannot recover a working credential; ``token_hash`` is the primary key because lookup is
# always by presented secret. ``prefix`` is the token's leading public characters, kept solely so
# the owner can tell their tokens apart in the UI — it is not a credential.
#
# The row carries its own org and role rather than deriving them from the owner at validation
# time: a token is a scoped grant, so narrowing or revoking it must not require touching the
# user, and a user's later membership changes must not silently widen a token already issued.
personal_access_tokens = Table(
    "personal_access_tokens",
    metadata,
    Column("token_hash", Text, primary_key=True),
    Column("prefix", Text, nullable=False),
    Column("name", Text, nullable=False),
    Column("user_id", Text, nullable=False),
    Column("org_id", Text, ForeignKey("orgs.id", ondelete="CASCADE"), nullable=False),
    Column("role_id", Text),  # cross-model ref -> org.roles; null means the owner's resolved role
    Column("scopes", JSON, nullable=False, default=list),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    # Null expiry is a non-expiring token — an explicit choice at issuance, not an absent value.
    Column("expires_at", DateTime(timezone=True)),
    Column("last_used_at", DateTime(timezone=True)),
    # Revocation is a tombstone rather than a DELETE so an audit of a compromised token still
    # resolves its owner and issuance time after the credential stops working.
    Column("revoked_at", DateTime(timezone=True)),
)

# REQ-1394: SCRAM-SHA-256 verifiers, the credential PostgreSQL clients negotiate over pgwire.
#
# A separate table from ``local_users`` rather than another column on it, because the two are
# derived at the same moment but answer different questions and have different lifetimes: the
# bcrypt hash proves a password to the HTTP surface, the verifier lets pgwire prove knowledge
# without either side transmitting one. A user has a verifier only from the first time they set a
# password after SCRAM is turned on — a bcrypt hash cannot be converted — so its absence is normal
# and must not make the user row look incomplete.
#
# The verifier is stored in PostgreSQL's own pg_authid spelling,
# ``SCRAM-SHA-256$<iterations>:<b64 salt>$<b64 StoredKey>:<b64 ServerKey>``. It is not a password
# equivalent: a reader of this table cannot authenticate with what it holds.
scram_credentials = Table(
    "scram_credentials",
    metadata,
    Column("user_id", Text, primary_key=True),
    # pgwire's startup packet names the user and nothing else, so lookup is by username.
    Column("username", Text, nullable=False, unique=True),
    Column("verifier", Text, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

# Per-org encrypted configuration — formerly ``tenant_config``, keyed by the ``tenants`` UUID.
# REQ-1355: org == tenant, so the key is the org slug and the billing columns it used to carry
# live on ``orgs`` itself.
org_config = Table(
    "org_config",
    metadata,
    Column("id", Uuid, primary_key=True),
    Column("org_id", Text, ForeignKey("orgs.id", ondelete="CASCADE"), nullable=False),
    Column("entity_type", Text, nullable=False),
    Column("entity_id", Text, nullable=False),
    Column("encrypted_dek", LargeBinary, nullable=False),
    Column("ciphertext", LargeBinary, nullable=False),
    Column("iv", LargeBinary, nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    UniqueConstraint("org_id", "entity_type", "entity_id"),
)


# The org/user/invite registry. ``org_config`` is bootstrapped separately by the billing
# module, so it is excluded here.
REGISTRY_TABLES = [
    orgs,
    user_profiles,
    user_org_memberships,
    org_auto_join_optouts,
    local_users,
    org_invites,
    superadmin_bootstrap,
    personal_access_tokens,
    scram_credentials,
]


async def init_registry_schema(db: "Database", org_id: str) -> None:  # REQ-696, REQ-1286
    """Create the platform registry tables on the platform control-plane engine.

    Uses portable SQLAlchemy metadata (dialect-appropriate DDL) so the platform
    control plane can be backed by any SQLAlchemy URI, not just PostgreSQL. The
    tenant control plane keeps its raw ``schema.sql`` bootstrap (PostgreSQL).

    Seeds the default org (the on-prem/single-tenant namespace), which was previously
    seeded by ``schema.sql`` when the registry lived per-org. REQ-1286: *org_id* is the
    control plane's resolved org id — the same value that names the tenant schema
    ``org_<id>``. Seeding a different literal here strands the registry row on an org
    whose schema does not exist, and every org-runtime resolution for it then fails."""
    async with db.engine.begin() as conn:
        await conn.run_sync(lambda sc: metadata.create_all(sc, tables=REGISTRY_TABLES))
        # V1 no-migrations: the metadata is the registry's schema, but ``create_all`` skips tables
        # that already exist, so a column added here never reaches a deployment whose registry
        # predates it (REQ-1412's external-engine columns are the case that surfaced it).
        from provisa.core.db import add_missing_columns

        await conn.run_sync(add_missing_columns, REGISTRY_TABLES)
    async with db.acquire() as conn:
        result = await conn.execute_core(select(orgs.c.id).where(orgs.c.id == org_id))
        if result.scalar() is None:
            # Insert-if-absent (DO NOTHING): seed the default org idempotently.
            # REQ-1296: seeded_demo is true because the bootstrap org is built from the deployment's
            # own config at every startup — the demo sources, domains and views land in it before the
            # first sign-in completes. A false here would make a rebuilt runtime come back empty and
            # hand the platform admin the blank deployment this requirement exists to prevent.
            await conn.upsert(
                orgs,
                {"id": org_id, "name": "Enterprise", "seeded_demo": True},
                index_elements=["id"],
                update_columns=[],
            )
        else:
            # An org row predating REQ-1296 carries seeded_demo=false (the column default). The
            # bootstrap org is always demo-seeded, so correct it rather than leave the registry
            # disagreeing with what startup actually built.
            await conn.execute_core(
                orgs.update().where(orgs.c.id == org_id).values(seeded_demo=True)
            )
