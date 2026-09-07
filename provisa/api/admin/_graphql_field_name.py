# Copyright (c) 2026 Kenneth Stott
# Canary: 6c9e7c2a-3ab4-4d5a-9c9f-7f6b7cbb2d1a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1634: look up a table's root query field name in the already-compiled data-plane GraphQL
schema (state.table_path_maps), rather than re-deriving provisa.compiler.naming.generate_name's
domain-wide uniqueness algorithm here or client-side. That algorithm needs every sibling table
name in the domain to resolve collisions, so a partial/local reimplementation would drift."""

from __future__ import annotations


def resolve_graphql_field_name(*, domain_id: str, schema_name: str, table_name: str) -> str | None:
    """Return the gql field name for (domain_id, schema_name, table_name), or None if no role's
    compiled schema currently exposes it (startup, or every role's data capabilities exclude it)."""
    from provisa.api.app import state

    for path_map in state.table_path_maps.values():
        for field_name, info in path_map.items():
            if (
                info["domain_id"] == domain_id
                and info["schema_name"] == schema_name
                and info["table_name"] == table_name
            ):
                return field_name
    return None
