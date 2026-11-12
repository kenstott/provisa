# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The tenant-plane tables every authenticated request reads or writes in the bound org.

AuthMiddleware._upsert_profile, on each sign-in: reads the caller's role assignments (roles,
user_role_assignments), mirrors the identity (user_directory, REQ-1439) and what the identity
provider said (user_idp_attributes, REQ-1383), and mirrors the token's entitlement lists and
attaches grants waiting on its email (entitlements, entitlement_expiries, entitlement_pending,
REQ-2219). A test that lays out a partial tenant schema starts from this list, so a table the
sign-in path comes to need is added here once.
"""

from __future__ import annotations

from provisa.core.schema_org import (
    entitlement_expiries,
    entitlement_pending,
    entitlements,
    roles,
    user_directory,
    user_idp_attributes,
    user_role_assignments,
)

SIGN_IN_TABLES = [
    roles,
    user_role_assignments,
    user_directory,
    user_idp_attributes,
    entitlements,
    entitlement_expiries,
    entitlement_pending,
]
