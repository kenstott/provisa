# Copyright (c) 2026 Kenneth Stott
# Canary: bc25fcba-4c6b-478e-bedd-2a0ad3ba364f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Embedding-column governance (REQ-426).

Similarity search runs over the same governed columns as any other query: a role may
only search an embedding column it is permitted to see and that is not masked for it.
A masked embedding (the role is not in unmasked_to) cannot be searched — searching
masked vectors would leak the very data masking protects.
"""

from __future__ import annotations

# Requirements: REQ-426, REQ-652


class EmbeddingAccessError(PermissionError):
    """A role attempted a similarity search on an embedding column it cannot access."""


def can_search_embedding(role_id: str, column) -> bool:  # REQ-426, REQ-652
    """Whether ``role_id`` may run a similarity search on this embedding column (REQ-426).

    Requires column visibility, and — for masked/sensitive columns — that the role is
    in ``unmasked_to``. Mirrors the visibility + masking gate used elsewhere in Stage 2.
    """
    visible_to = getattr(column, "visible_to", []) or []
    if role_id not in visible_to:
        return False
    mask_type = getattr(column, "mask_type", None)
    if mask_type:
        unmasked_to = getattr(column, "unmasked_to", []) or []
        if role_id not in unmasked_to:
            return False
    return True


def assert_search_allowed(role_id: str, column) -> None:  # REQ-426, REQ-652
    """Raise EmbeddingAccessError if the role may not search this embedding column."""
    if not can_search_embedding(role_id, column):
        raise EmbeddingAccessError(
            f"role {role_id!r} may not run similarity search on embedding column "
            f"{getattr(column, 'name', '?')!r}"
        )
