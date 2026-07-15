# Copyright (c) 2026 Kenneth Stott
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
- SaaS billing (formerly the raw ``platform`` schema): ``tenants``,
  ``tenant_config``

Mirrors the post-migration shape of the corresponding tables in
``provisa/core/schema.sql`` and ``provisa/api/billing/tenant_db.py`` with
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
)

user_profiles = Table(
    "user_profiles",
    metadata,
    Column("user_id", Text, primary_key=True),
    Column("email", Text),
    Column("display_name", Text),
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

org_invites = Table(
    "org_invites",
    metadata,
    Column("token", Text, primary_key=True),
    Column("org_id", Text, ForeignKey("orgs.id", ondelete="CASCADE"), nullable=False),
    Column("role_id", Text),  # cross-model ref -> org.roles
    Column("created_by", Text, nullable=False),
    Column("expires_at", DateTime(timezone=True), nullable=False),
    Column("used_at", DateTime(timezone=True)),
    Column("used_by", Text),
)

# SaaS billing — formerly the raw PG ``platform`` schema in tenant_db.py.
tenants = Table(
    "tenants",
    metadata,
    Column("id", Uuid, primary_key=True),
    Column("kms_key_arn", Text, nullable=False),
    Column("ls_customer_id", Text),  # Lemon Squeezy customer id (REQ-1015)
    Column("plan", Text, nullable=False, server_default="trial"),
    Column("source_limit", Integer, nullable=False, server_default="2"),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

tenant_config = Table(
    "tenant_config",
    metadata,
    Column("id", Uuid, primary_key=True),
    Column("tenant_id", Uuid, ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False),
    Column("entity_type", Text, nullable=False),
    Column("entity_id", Text, nullable=False),
    Column("encrypted_dek", LargeBinary, nullable=False),
    Column("ciphertext", LargeBinary, nullable=False),
    Column("iv", LargeBinary, nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    UniqueConstraint("tenant_id", "entity_type", "entity_id"),
)


# The org/user/invite registry. ``tenants``/``tenant_config`` are bootstrapped
# separately by the billing module (``platform`` PG schema), so they are excluded
# here to avoid creating a duplicate copy in the default schema.
REGISTRY_TABLES = [orgs, user_profiles, user_org_memberships, local_users, org_invites]


async def init_registry_schema(db: "Database") -> None:  # REQ-696
    """Create the platform registry tables on the platform control-plane engine.

    Uses portable SQLAlchemy metadata (dialect-appropriate DDL) so the platform
    control plane can be backed by any SQLAlchemy URI, not just PostgreSQL. The
    tenant control plane keeps its raw ``schema.sql`` bootstrap (PostgreSQL).

    Seeds the default ``root`` org (the on-prem/single-tenant namespace), which
    was previously seeded by ``schema.sql`` when the registry lived per-org."""
    async with db.engine.begin() as conn:
        await conn.run_sync(lambda sc: metadata.create_all(sc, tables=REGISTRY_TABLES))
    async with db.acquire() as conn:
        result = await conn.execute_core(select(orgs.c.id).where(orgs.c.id == "root"))
        if result.scalar() is None:
            # Insert-if-absent (DO NOTHING): seed the default root org idempotently.
            await conn.upsert(
                orgs, {"id": "root", "name": "Enterprise"}, index_elements=["id"], update_columns=[]
            )
