# Copyright (c) 2026 Kenneth Stott
# Canary: b64c9be1-34e4-4d11-a658-bdaf66cb9789
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Cache key generation for query results (REQ-078).

Key includes query SQL, parameters, role_id, and RLS context values.
Two users with different RLS filters get different cache entries.
"""

from __future__ import annotations

import hashlib
import json


def cache_key(
    sql: str,
    params: list,
    role_id: str,
    rls_rules: dict[int, str],
) -> str:
    """Generate a deterministic cache key from query + security context.

    Args:
        sql: The compiled SQL string.
        params: Positional parameters.
        role_id: The requesting role.
        rls_rules: Active RLS rules (table_id → filter expression) for this role.

    Returns:
        SHA-256 hex digest cache key.

    Raises:
        ValueError: If rls_rules is non-empty but empty string values exist
                    (indicates unresolved RLS context — security defect).
    """
    for table_id, expr in rls_rules.items():
        if not expr or not expr.strip():
            raise ValueError(
                f"RLS rule for table {table_id} has empty filter expression. "
                f"Cannot generate cache key with unresolved RLS context."
            )

    # Build a canonical representation for hashing
    key_parts = {
        "sql": sql,
        "params": params,
        "role_id": role_id,
        "rls": {str(k): v for k, v in sorted(rls_rules.items())},
    }
    canonical = json.dumps(key_parts, sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
