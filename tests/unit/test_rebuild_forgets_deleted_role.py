# Copyright (c) 2026 Kenneth Stott
# Canary: d1382ac8-ff02-4dc9-9140-23ba7dbd8368
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A schema rebuild forgets a role that was deleted (REQ-042).

The role registry and the per-role maps are kept across rebuilds of a live runtime. Both role
delete paths rebuild; the rebuild must then drop the role from the runtime, or the deleted role
keeps its rights and its schema until the process restarts.
"""

# Requirements: REQ-042, REQ-1918

from __future__ import annotations

import provisa.api.app as appmod

# --- the rebuild: a role that is gone leaves nothing behind in the runtime -----------------------


def test_a_rebuild_forgets_a_role_that_was_deleted(monkeypatch):
    """The registry and the per-role maps are kept across rebuilds of a live runtime. A role
    deleted since the last build must leave them, or it keeps its rights (capability resolution
    reads ``state.roles``) and its schema for whoever still names it."""
    from provisa.api.app_loaders import _build_and_register_schemas
    from provisa.compiler.introspect import ColumnMetadata
    from provisa.core import domain_policy

    monkeypatch.setattr(domain_policy, "single_domain", lambda: False)
    state = appmod.state
    surfaces = ("schemas", "contexts", "rls_contexts", "table_path_maps", "proto_files")
    for name in ("roles", *surfaces):
        monkeypatch.setattr(state, name, {}, raising=False)
    monkeypatch.setattr(state, "graphql_remote_sources", {}, raising=False)
    monkeypatch.setattr(state, "source_types", {"pg": "postgresql"}, raising=False)
    monkeypatch.setattr(state, "source_catalogs", {"pg": "pg"}, raising=False)

    def _build(role_ids: list[str]) -> None:
        _build_and_register_schemas(
            roles=[
                {"id": r, "capabilities": ["usage"], "domain_access": ["sales"]} for r in role_ids
            ],
            tables=[
                {
                    "id": 1,
                    "source_id": "pg",
                    "domain_id": "sales",
                    "schema_name": "public",
                    "table_name": "orders",
                    "governance": "pre-approved",
                    "columns": [{"column_name": "id", "visible_to": ["*"]}],
                    "write_ops": ["delete", "insert", "update"],
                }
            ],
            relationships=[],
            col_types_converted={
                1: [ColumnMetadata(column_name="id", data_type="integer", is_nullable=False)]
            },
            naming_rules=[],
            domains=[{"id": "sales"}],
            domain_prefix=False,
            kafka_physical={},
            tracked_functions=[],
            tracked_webhooks=[],
            gql_object_cols={},
            rls_rules=[],
            metrics=[],
        )

    _build(["kept", "loose"])
    assert {"kept", "loose"} <= set(state.roles) and "loose" in state.schemas

    _build(["kept"])  # ``loose`` was deleted; the same runtime is rebuilt
    assert "loose" not in state.roles
    for name in surfaces:
        assert "loose" not in getattr(state, name), name
    assert "kept" in state.roles and "kept" in state.schemas
