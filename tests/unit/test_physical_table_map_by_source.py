# Copyright (c) 2026 Kenneth Stott
# Canary: 5c1f7a2e-93d4-4b6e-a8f0-2d7e61b4c9a3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A user-source table named like a control-plane table reads the user's table.

The physical table map rewrites only the control plane's own registrations (source
``provisa-admin``) to their meta, ontology and ops views. A table of the same name on any other
source is read as registered: a user table ``requests`` is never answered from ``requests_ops``.
"""

# Requirements: REQ-884, REQ-2216

from __future__ import annotations

import pytest

from provisa.api.role_surface import role_surface
from provisa.core.models import ADMIN_SOURCE_ID

_ROLE = "analyst"

# Each name the map rewrites for the control plane, and the entitlement tables.
_SHADOWED = [
    "requests",
    "query_audit_log",
    "replica_build_log",
    "registered_tables",
    "roles",
    "_terms",
    "entitlements",
    "entitlement_expiries",
]


def _table(table_id: int, source_id: str, domain_id: str, schema: str, name: str) -> dict:
    return {
        "id": table_id,
        "source_id": source_id,
        "domain_id": domain_id,
        "schema_name": schema,
        "table_name": name,
        "governance": "pre-approved",
        "columns": [{"column_name": "id", "visible_to": [_ROLE]}],
        "write_ops": [],
        "write_returns_rows": True,
        "write_refused_forms": [],
    }


def _build(monkeypatch, tables: list[dict], domains: list[str]):
    import provisa.api.app as appmod
    from provisa.api import app_loaders
    from provisa.compiler.introspect import ColumnMetadata
    from provisa.core import domain_policy

    monkeypatch.setattr(domain_policy, "single_domain", lambda: False)
    state = appmod.state
    for name in ("roles", "schemas", "contexts", "rls_contexts", "table_path_maps", "proto_files"):
        monkeypatch.setattr(state, name, {}, raising=False)
    monkeypatch.setattr(state, "graphql_remote_sources", {}, raising=False)
    monkeypatch.setattr(
        state, "source_types", {"pg": "postgresql", ADMIN_SOURCE_ID: "postgresql"}, raising=False
    )
    monkeypatch.setattr(
        state, "source_catalogs", {"pg": "pg", ADMIN_SOURCE_ID: "provisa_admin"}, raising=False
    )
    app_loaders._build_and_register_schemas(
        roles=[
            {
                "id": _ROLE,
                "description": "Test role.",
                "capabilities": ["usage"],
                "domain_access": domains,
            }
        ],
        tables=tables,
        relationships=[],
        col_types_converted={
            t["id"]: [ColumnMetadata(column_name="id", data_type="integer", is_nullable=False)]
            for t in tables
        },
        naming_rules=[],
        domains=[{"id": d} for d in domains],
        domain_prefix=False,
        kafka_physical={},
        tracked_functions=[],
        tracked_webhooks=[],
        gql_object_cols={},
        rls_rules=[],
        metrics=[],
        withdrawn_tables=[],
        withdrawn_relationships=[],
    )
    return state


def _surface(state):
    surface = role_surface(state, _ROLE)
    assert surface is not None
    return surface


def _physical(state, table_id: int) -> tuple[str, str, str]:
    ctx = _surface(state).context
    meta = next(m for m in ctx.tables.values() if m.table_id == table_id)
    return meta.source_id, meta.catalog_name, meta.table_name


@pytest.mark.parametrize("name", _SHADOWED)
def test_a_user_table_named_like_a_control_plane_table_reads_the_users_table(
    monkeypatch, name: str
) -> None:
    state = _build(monkeypatch, [_table(1, "pg", "sales", "public", name)], ["sales"])
    assert _physical(state, 1) == ("pg", "pg", name)


@pytest.mark.parametrize("name", _SHADOWED)
def test_a_compiled_query_of_the_user_table_names_the_users_table(monkeypatch, name: str) -> None:
    from provisa.compiler.parser import parse_query
    from provisa.compiler.sql_gen import compile_query

    state = _build(monkeypatch, [_table(1, "pg", "sales", "public", name)], ["sales"])
    surface = _surface(state)
    field = next(f for f, m in surface.context.tables.items() if m.table_id == 1)
    doc = parse_query(surface.schema, f"{{ {field} {{ id }} }}", ctx=surface.context)
    (compiled,) = compile_query(doc, surface.context)
    sql = compiled.sql
    assert f'"{name}"' in sql
    for view in ("_ops", "_meta", "_ontology"):
        assert f'"{name}{view}"' not in sql


@pytest.mark.parametrize(
    ("name", "view"),
    [
        ("requests", "requests_ops"),
        ("query_audit_log", "query_audit_log_ops"),
        ("registered_tables", "registered_tables_meta"),
        ("roles", "roles_meta"),
        ("_terms", "_terms_ontology"),
    ],
)
def test_the_control_planes_own_table_reads_its_view(monkeypatch, name: str, view: str) -> None:
    # The map is keyed by source and name, not domain: the control plane's table is placed in a
    # domain the role reaches without the meta and ops domains' own capabilities.
    state = _build(monkeypatch, [_table(1, ADMIN_SOURCE_ID, "sales", "public", name)], ["sales"])
    assert _physical(state, 1) == (ADMIN_SOURCE_ID, "provisa_admin", view)


def test_the_map_is_keyed_by_source_and_table(monkeypatch) -> None:
    state = _build(monkeypatch, [_table(1, "pg", "sales", "public", "requests")], ["sales"])
    physical = state.role_build_inputs["physical_table_map"]
    assert physical[(ADMIN_SOURCE_ID, "requests")] == "requests_ops"
    assert all(isinstance(k, tuple) and len(k) == 2 for k in physical)
    assert ("pg", "requests") not in physical
