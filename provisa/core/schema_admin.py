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
- Per-org encrypted config: ``org_config``

The SaaS billing facts are NOT here. The plan/limit/Lemon-Squeezy/KMS columns on ``orgs`` and the
``org_usage_hour`` meter belong to the commercial plugin, which attaches them to this module's
``metadata`` and ``REGISTRY_TABLES`` when it loads (``provisa.core.commerce``). A deployment without
the plugin has no billing subject and no meter, which is the correct shape for the open-source and
demo distributions.

Mirrors the post-migration shape of the corresponding tables in
``provisa/core/schema.sql`` with portable types (see ``provisa/core/schema_org.py``
for the type mapping).

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
    ForeignKeyConstraint,
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
    text,
    true,
)

if TYPE_CHECKING:
    from provisa.core.database import Database

metadata = MetaData()


# REQ-1476: the provisioning_state a commercial deployment registers an org in before its
# subscription exists. It holds the id and the creator's membership and nothing else — no schema, no
# engine, no demo seed — so an org in this state is not somewhere anyone can work: it is excluded
# from session bindings, from /auth/me's memberships and from auto-join.
AWAITING_CHECKOUT = "awaiting_checkout"


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
    # REQ-1048: the org's OWN materialization store (bring-your-own storage), encrypted at rest
    # like engine_url_enc above and for the same reason — it is a credential to a system the
    # platform does not own. Set means every MV output and landed table for this org is written
    # there, on the org's bill, and its footprint is neither metered nor capped by REQ-1046. NULL
    # means the org materializes into the platform's store, where the tier quota applies.
    Column("storage_url_enc", LargeBinary),
    # REQ-1486: the org's own light branding — a JSON document of display_name, primary_color,
    # accent_color, welcome_message and invite_message, written only through
    # provisa.core.org_branding.validate_branding. NULL means the org set none and the product's
    # own presentation stands. The logo travels beside it as bytes rather than a URL: the sign-in
    # page must render it before any session exists, and a tenant-supplied remote URL would make
    # that page fetch from a host the platform does not control.
    Column("branding", Text),
    Column("branding_logo", LargeBinary),
    Column("branding_logo_media_type", Text),
    # REQ-1527: where this org's environment repository (REQ-1524) is mirrored, and where the
    # projection's outcome is reported. Both NULL is the ordinary case and the airgapped one: the
    # org commits, merges and approves with no remote and no CI at all. The remote is stored as
    # written, secret references included (${env:GIT_TOKEN}), and resolved only at push time, so
    # the token a push needs never enters the control plane.
    Column("repo_remote", Text),
    Column("repo_status_webhook", Text),
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
    # REQ-1478: how the membership came about, and whether the member has been told about it. A
    # person can be joined to an org by an email-rule match or by an administrator — neither is an
    # act they performed — so the membership carries what to say when they next sign in. NULL
    # joined_via is a membership written before this column existed; it explains nothing and is
    # announced as nothing.
    Column("joined_via", Text),
    Column("acknowledged_at", DateTime(timezone=True)),
    # REQ-1596: the ONE environment this membership may be served by, or NULL for the ordinary
    # membership that may be served by any environment its role permits. A pinned member naming no
    # environment is served this one rather than prod, and naming a different one is refused — which
    # is what makes a sandbox visitor's whole session live inside the environment minted for them
    # instead of in the org's production data. Nullable because a pin is the exception: every
    # membership written before an open invite existed, and every ordinary member, has none.
    Column("env_name", Text),
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
    # REQ-1594: an invite is redeemable ``max_uses`` times, and ``uses`` counts how many of those
    # have been spent. ``max_uses`` defaults to 1, which is the addressed invitation every existing
    # caller mints — so the ordinary invite is still burnt by its first redeemer, expressed as a
    # count rather than as a timestamp. NULL max_uses is unlimited: the "Try it Out" link on the
    # marketing site cannot know in advance how many people will click it.
    Column("uses", Integer, nullable=False, server_default=text("0")),
    Column("max_uses", Integer, server_default=text("1")),
    # ``used_at``/``used_by`` are the LAST redemption, not the only one. Kept as scalars because the
    # account-deletion tombstone (auth_router) rewrites used_by, and because a single-use invite --
    # still the common case -- has exactly one, so nothing about the addressed invite changed.
    Column("used_at", DateTime(timezone=True)),
    Column("used_by", Text),
    # REQ-1595: what the redeemer is given to work in. ``none`` is the ordinary invitation: the
    # person joins the org and is served by it as any member is. ``per_visitor`` mints a fresh
    # environment for each redemption, expiring ``env_ttl_seconds`` later, which is the sandbox --
    # the visitor gets real machinery and a real model, and nothing they do outlives the hour.
    # ``shared`` seats every redeemer in the ONE environment ``env_name`` names, which is the
    # branded read-only data portal an org publishes to people it has never met.
    #
    # Three values rather than a sandbox boolean because the portal and the sandbox differ only in
    # these fields; a boolean would have to be joined by a second one later.
    Column("env_policy", Text, nullable=False, server_default=text("'none'")),
    Column("env_ttl_seconds", Integer),
    Column("env_name", Text),
    CheckConstraint(
        "env_policy IN ('none', 'per_visitor', 'shared')", name="ck_org_invites_env_policy"
    ),
    # A per_visitor environment with no TTL would never be reaped and would accumulate one schema
    # per click, so the TTL is not optional there; a shared environment with no name names nothing.
    CheckConstraint(
        "(env_policy <> 'per_visitor' OR env_ttl_seconds IS NOT NULL) AND "
        "(env_policy <> 'shared' OR env_name IS NOT NULL)",
        name="ck_org_invites_env_policy_fields",
    ),
    CheckConstraint(
        "uses >= 0 AND (max_uses IS NULL OR max_uses >= 1)", name="ck_org_invites_uses"
    ),
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


# REQ-1488: the environments an org holds. An environment is physically a schema (REQ-1488) and
# the schema is what holds its model, so this row is deliberately only what the schema CANNOT hold:
# the plan ceiling's countable unit, the expiry that reaps it, whether a merge into it needs an
# approval, and whether its repository projection has fallen behind. Per-source boundness
# (REQ-1491) is per SOURCE and lives in the org schema beside the source; the environment a
# credential addresses (REQ-1503) lives on the credential. Neither is duplicated here.
#
# ``prod`` HAS A ROW. REQ-1487 gives prod to the org at creation rather than by a load, so
# provisioning writes its row with the schema, the role and the stores — an org whose prod is
# absent from this table would be an org whose environments cannot be counted and whose prod
# cannot be protected. It is the one row REQ-1488's create-by-loading rule does not write.
environments = Table(
    "environments",
    metadata,
    Column("org_id", Text, ForeignKey("orgs.id", ondelete="CASCADE"), primary_key=True),
    # Validated by provisa.core.environments.validate_env_name against THIS org's id before
    # anything is provisioned (REQ-1523) — the length a name may reach depends on the id it is
    # suffixed onto, so the check cannot live in a column type.
    Column("name", Text, primary_key=True),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("created_by", Text),
    # REQ-1523: opt-in. NULL means permanent — an environment is never reaped for being idle,
    # because a quiet pre-prod is not an abandoned one. prod can carry none.
    Column("expires_at", DateTime(timezone=True)),
    # REQ-1600: how long this environment may go UNUSED before it is reaped, when its lifetime is
    # measured from the last request that was served by it rather than from its creation. NULL is
    # REQ-1523's fixed deadline: `expires_at` stands where it was written and nothing moves it. A
    # value makes `expires_at` a sliding one — `provisa.api.env_routing` pushes it out by this many
    # seconds each time the environment serves — which is what an environment handed to a visitor
    # by an invitation needs: an hour of quiet ends it, an hour of work does not.
    Column("idle_ttl_seconds", Integer),
    # REQ-1504: a merge into a protected environment waits for an approval by someone other than
    # the requester. prod is protected once the org has more than one member; any environment can
    # be protected by an org_admin.
    Column("protected", Boolean, nullable=False, server_default=false()),
    # REQ-1524: the repository is a projection, and a failed commit must not fail the change it
    # observes. A change whose commit did not land sets this instead; rebuilding re-serializes
    # every carried class and clears it.
    Column("drifted", Boolean, nullable=False, server_default=false()),
    # REQ-1529: the base this environment branched from, or NULL when it IS a base. A base is what
    # an org_admin creates, binds with its own credentials and grants membership in; a branch is
    # what a member creates from one, and it reaches the base's sources by reference rather than
    # holding a copy of where they point (REQ-1491). Self-referential FK: a branch cannot name an
    # environment of another org, and a base cannot be dropped while a branch still resolves
    # through it, which RESTRICT enforces rather than a check somebody has to remember.
    # REQ-1543: WHERE THE ENVIRONMENT IS in its own history, and where an undo departed from.
    # ``deployed_sha`` is the commit whose tree the environment's model equals -- written by the
    # write-through that committed it and by the deploy that applied it, and NOT the same as the
    # branch tip once an undo has happened. ``redo_sha`` is the position the first undo of a run
    # departed from, so a redo can step forward along the path back to it; it is cleared by any
    # deploy or write-through that is not itself an undo, because a new edit makes the abandoned
    # future unreachable in intent. Neither column can lose work: both name commits that stay in
    # the object store and stay deployable by sha.
    Column("deployed_sha", Text),
    # WHERE THIS ENVIRONMENT'S OWN LINE BEGINS: the last commit that belongs to the environment it
    # was created from. A branch is seeded at its source's tip so the two share an object history
    # and ordinary git can move between them, which means the commits below that point are the
    # SOURCE's -- trees this environment never held. An undo that walked into them would deploy a
    # model the environment was never running, so it stops here instead: one change made after a
    # branch is created gives exactly one undo. NULL for an environment with nothing behind it,
    # which is prod.
    Column("origin_sha", Text),
    Column("redo_sha", Text),
    Column("branched_from", Text),
    ForeignKeyConstraint(
        ["org_id", "branched_from"],
        ["environments.org_id", "environments.name"],
        ondelete="RESTRICT",
        name="environments_branched_from_fkey",
    ),
)


# REQ-1504: a proposed merge from one environment of an org into another, held as a ROW because the
# approver is by definition someone other than the requester and is therefore not present when the
# request is made. An ephemeral confirmation would force the approval into the requester's own
# session, which is the one arrangement the requirement forbids.
#
# It sits beside ``environments`` rather than inside an org schema for the same reason that table
# does: the request names TWO environments, so a row inside either one of them would be a row only
# half its subject can see.
#
# The report is stored as it was READ. REQ-1504 makes a request whose source has moved on STALE
# rather than applicable, and staleness is only decidable against the report the approver actually
# saw — recomputing it at approval time would silently approve a different merge than the one
# reviewed.
env_merge_requests = Table(
    "env_merge_requests",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("org_id", Text, ForeignKey("orgs.id", ondelete="CASCADE"), nullable=False),
    # Where the model comes from and where it is going. A MERGE names an environment within the
    # same org_id -- a merge across orgs is never a legal operation (REQ-1524), so one org_id
    # covers both. A LOAD (REQ-1496) names a git ref instead, and pins the sha that ref resolved to
    # at request time: an approver approves a TREE, and a branch that moves afterwards is a
    # different tree that has not been approved.
    Column("source_env", Text),
    Column("source_ref", Text),
    Column("source_sha", Text),
    Column("target_env", Text, nullable=False),
    Column("requested_by", Text, nullable=False),
    Column("requested_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    # The requester's own words. Together with ``report`` these ARE the review (REQ-1504): the
    # report is the squash, at object granularity rather than line granularity.
    Column("message", Text, nullable=False, server_default=""),
    # REQ-1490's report as the approver read it: added, changed, removed, left alone.
    Column("report", JSON, nullable=False),
    # Whether the request asked for removals. Carried on the row because it changes what applying
    # does, and an approver approves a specific operation rather than a direction.
    Column("removals", Boolean, nullable=False, server_default=false()),
    # REQ-1542: whether applying this merge also RETIRES the environment it came from -- its
    # schemas, its row and its branch. Carried on the row for the same reason ``removals`` is: an
    # approver approves a specific operation, and "merge this" and "merge this and end the feature
    # environment" are two of them.
    Column("retire_source", Boolean, nullable=False, server_default=false()),
    # REQ-1549: and whether it also deletes the source's branch ON THE REMOTE. A separate column
    # from ``retire_source`` because it is a separate ask (REQ-1546): the remote copy is what
    # survives a lost volume, so ending an environment locally never implies ending it there.
    Column("retire_remote", Boolean, nullable=False, server_default=false()),
    # requested -> approved | rejected, and approved -> applied. ``stale`` is derived rather than
    # written: a request is stale when re-planning no longer produces the stored report.
    Column("state", Text, nullable=False, server_default="requested"),
    Column("decided_by", Text),
    Column("decided_at", DateTime(timezone=True)),
    Column("decision_note", Text),
    Column("applied_at", DateTime(timezone=True)),
    # A load also carries whether it SEEDS -- REQ-1539's creation-only classes -- because that
    # changes what applying does, exactly as ``removals`` does for a merge.
    Column("seed", Boolean, nullable=False, server_default=false()),
    CheckConstraint(
        "state IN ('requested', 'approved', 'rejected', 'applied')",
        name="ck_env_merge_requests_state",
    ),
    # Exactly one source. A row naming both would leave applying to a preference, and a row naming
    # neither describes nothing.
    CheckConstraint(
        "(source_env IS NULL) <> (source_ref IS NULL)",
        name="ck_env_merge_requests_one_source",
    ),
    CheckConstraint(
        "(source_ref IS NULL) = (source_sha IS NULL)",
        name="ck_env_merge_requests_ref_pinned",
    ),
)


# REQ-1466: the deployment-wide maintenance notice. One row, id ``current``, written only by a
# holder of ``platform_settings`` and read by every signed-in client so a planned outage — the
# engine-cluster topology switch is the one that forces it, since flipping
# ``var.engine_cluster_mode`` REPLACES the cluster — reads as scheduled work rather than as the
# product being broken. Registry-resident rather than an env var because it is turned on and off
# while the control plane is running, and it must survive the restart it often accompanies.
platform_notice = Table(
    "platform_notice",
    metadata,
    Column("id", Text, primary_key=True),
    Column("active", Boolean, nullable=False, server_default=false()),
    # NULL means the deployment's standard wording, composed by the API from ``ends_at``. A
    # platform_admin overrides it only when there is something specific to say.
    Column("message", Text, nullable=True),
    # When the work is expected to be over. NULL means "no estimate", which the banner says in as
    # many words rather than inventing one.
    Column("ends_at", DateTime(timezone=True), nullable=True),
    Column("started_at", DateTime(timezone=True), nullable=True),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("updated_by", Text, nullable=True),
)


# REQ-1557, REQ-1558: the org's own secrets, when no central secrets service is connected. One
# row is one NAME the org can write ``${secret:NAME}`` against; ``value`` is the envelope blob
# (REQ-685), so the authority to read it is the encryption master key the process holds rather
# than any key of this table's own -- a registry copied without that key yields nothing. There is
# deliberately no plaintext column and no read path back out: a value goes in and is only ever
# resolved (REQ-1558).
# REQ-1560: the OWNER is part of the key, not a permission checked around it. ``owner_id`` is the
# user whose personal vault holds the row, or ``ORG_OWNER`` ("*") for the org vault every member
# shares. Two people may therefore each hold a GIT_TOKEN of their own, and ${user:GIT_TOKEN}
# resolves to whichever of them is acting -- there is no way to write down someone else's secret.
secrets_store = Table(
    "secrets_store",
    metadata,
    Column("org_id", Text, ForeignKey("orgs.id", ondelete="CASCADE"), primary_key=True),
    Column("owner_id", Text, primary_key=True),
    Column("name", Text, primary_key=True),
    Column("value", LargeBinary, nullable=False),
    # What the secret is for, in the org's own words. Never the value, and never derived from it.
    Column("description", Text, nullable=True),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("updated_by", Text, nullable=True),
)


# REQ-1574: the org's OWN encryption key -- a RING, one row per key the org has ever set. The
# active key is the row with ``retired_at IS NULL``; the rest stay so that a payload wrapped under
# one still decrypts, which is what makes rotation immediate rather than a re-encryption of
# everything stored. ``wrapped_key`` is the org's 32-byte master key wrapped by the DEPLOYMENT's
# encryption service (REQ-685), for the same reason secrets_store holds an envelope blob: a copy of
# this table without the deployment master key is not the org's key.
# There is NO plaintext column and NO read path back out. ``fingerprint`` -- the first 16 hex of
# SHA-256 over the raw key -- is what an operator is shown instead: enough to tell which key is
# active and whether a key they just set is the one they meant, and nothing about the key itself.
org_encryption_keys = Table(
    "org_encryption_keys",
    metadata,
    Column("org_id", Text, ForeignKey("orgs.id", ondelete="CASCADE"), primary_key=True),
    # Stamped into every envelope blob this key wraps (REQ-1574), so it is ASCII and short.
    Column("key_id", Text, primary_key=True),
    Column("wrapped_key", LargeBinary, nullable=False),
    Column("fingerprint", Text, nullable=False),
    # Whether the org supplied the key material or asked the server to generate it. Recorded
    # because it is the difference between a key the org can also hold elsewhere and one that
    # exists only here.
    Column("supplied", Boolean, nullable=False, server_default=false()),
    # REQ-1574: this entry adopts blobs written before the org held a ring (envelope v1, which
    # names no key). True on at most one row per org -- the first key set for an org that already
    # had data, which was written under the deployment key and is decrypted by it thereafter.
    Column("adopts_unkeyed", Boolean, nullable=False, server_default=false()),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("created_by", Text),
    # NULL is the ACTIVE key. Exactly one row per org has it.
    Column("retired_at", DateTime(timezone=True)),
)


# The org/user/invite registry. ``org_config`` is bootstrapped separately by the billing
# module, so it is excluded here.
# REQ-1576: what the mail transport actually did. A configured transport says nothing about
# whether mail is arriving -- the failures that matter (a rejected key, an expired relay
# credential, an unverified sender domain) exist only at send time -- so every attempt is written
# here, failures included, and the Email settings page reports the counts and the last failure out
# of this table rather than out of the config. Registry-resident because it is platform-wide: the
# transport belongs to the deployment, not to any org, even though a row usually names the org
# whose invitation was being sent.
mail_events = Table(
    "mail_events",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("sent_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    # The transport this attempt went through, by registry key. Kept per row rather than read
    # from config at display time, because the question an operator asks after switching
    # transports is which of them was failing.
    Column("provider", Text, nullable=False),
    # What was being sent: "invite" (REQ-1330) or "test" (the operator's own check).
    Column("kind", Text, nullable=False),
    Column("recipient", Text, nullable=False),
    Column("org_id", Text, nullable=True),
    Column("succeeded", Boolean, nullable=False),
    # The transport's own words on failure, kept verbatim -- an operator fixes a rejected sender
    # identity from the provider's message, not from a rephrasing of it. NULL on success.
    Column("error", Text, nullable=True),
    Column("requested_by", Text, nullable=True),
)


REGISTRY_TABLES = [
    orgs,
    org_encryption_keys,
    user_profiles,
    user_org_memberships,
    org_auto_join_optouts,
    local_users,
    org_invites,
    superadmin_bootstrap,
    environments,
    env_merge_requests,
    org_config,
    secrets_store,
    personal_access_tokens,
    scram_credentials,
    platform_notice,
    mail_events,
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
        # REQ-1487: every org has prod from its creation, the bootstrap org included. Written here
        # rather than left to the first environment request, because an org absent from this table
        # is an org whose environments cannot be counted against its plan ceiling (REQ-1523).
        await conn.upsert(
            environments,
            {"org_id": org_id, "name": "prod"},
            index_elements=["org_id", "name"],
            update_columns=[],
        )
