# Copyright (c) 2026 Kenneth Stott
# Canary: e3be41a2-c96e-43d4-8ccc-1c9a86fd3066
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Platform control-plane bring-up.

The platform control plane is the global registry (orgs, users, memberships,
invites) plus SaaS billing, shared across all orgs and never org-scoped. It is a
separate SQLAlchemy engine/pool from the tenant control plane and may be backed
by any SQLAlchemy URI (not necessarily PostgreSQL). See REQ-837, REQ-839.
"""

from __future__ import annotations

from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_admin import init_registry_schema


async def bring_up_platform(url: str, *, pool_size: int, pool_min: int) -> Database:
    """Build the platform-plane ``Database`` from *url* and initialise its schema
    (org/user/invite registry + SaaS billing). Unscoped — no ``search_path``."""
    db = Database(
        create_engine_from_url(url, pool_size=pool_size, max_overflow=max(pool_size - pool_min, 0)),
        name="platform",
    )
    await init_registry_schema(db)
    from provisa.api.billing.tenant_db import init_billing_schema

    await init_billing_schema(db)
    return db
