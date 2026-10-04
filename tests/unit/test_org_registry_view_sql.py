# Copyright (c) 2026 Kenneth Stott
# Canary: e8a15f03-9c47-4b2d-b6e0-3d7f1a2c8c54
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1301: the root org's registry view is one read-only row per org per org_admin."""

from __future__ import annotations

from provisa.core.org_registry_view import VIEW_COLUMNS, VIEW_NAME, build_view_sql


def test_the_view_exposes_id_name_state_creation_time_and_admin_contact():
    sql = build_view_sql(root_schema="org_root", admin_schema="public", org_schemas={})
    assert sql.startswith(f"CREATE OR REPLACE VIEW org_root.{VIEW_NAME} AS")
    for expected in (
        "o.id AS org_id",
        "o.name AS org_name",
        "o.provisioning_state AS provisioning_state",
        "o.created_at AS created_at",
        "p.email AS org_admin_email",
        "p.display_name AS org_admin_display_name",
    ):
        assert expected in sql
    assert len(VIEW_COLUMNS) == 7


def test_org_admins_are_read_from_each_orgs_own_schema():
    sql = build_view_sql(
        root_schema="org_root",
        admin_schema="public",
        org_schemas={"acme": "org_acme", "beta": "org_beta"},
    )
    assert "FROM org_acme.user_role_assignments WHERE role_id = 'org_admin'" in sql
    assert "FROM org_beta.user_role_assignments WHERE role_id = 'org_admin'" in sql
    assert "'acme' AS org_id" in sql


def test_an_org_without_a_provisioned_schema_keeps_its_row_with_no_admin():
    sql = build_view_sql(root_schema="org_root", admin_schema="public", org_schemas={})
    assert "FROM public.orgs o" in sql
    assert "LEFT JOIN (SELECT NULL::text AS org_id, NULL::text AS user_id WHERE false) a" in sql


def test_the_view_is_read_only_ddl_over_the_control_plane():
    sql = build_view_sql(root_schema="org_root", admin_schema="public", org_schemas={"a": "org_a"})
    assert "INSERT" not in sql.upper() and "UPDATE" not in sql.upper()
