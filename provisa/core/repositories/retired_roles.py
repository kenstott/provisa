# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The retired role ids (REQ-1297): the model store's one write that removes them."""

from __future__ import annotations


def drop_retired_role_ids(sync_conn) -> None:  # REQ-1297
    """The role ids 'admin' and 'superadmin' are retired. Assignments naming them are carried to
    platform_admin, then the rows are dropped, so nothing resolves them afterward. An INSERT-select
    rather than an UPDATE: a user may already hold platform_admin for the same domain, and
    user_role_assignments is UNIQUE (user_id, role_id, domain_id). Deleting the roles rows cascades
    their assignments away, so the carry-over comes first. Runs on ``sync_conn`` in the schema it is
    set to, once platform_admin is written (db.assert_standard_roles)."""
    sync_conn.exec_driver_sql(
        "INSERT INTO user_role_assignments (user_id, role_id, domain_id) "
        "SELECT DISTINCT user_id, 'platform_admin', domain_id FROM user_role_assignments "
        "WHERE role_id IN ('admin', 'superadmin') "
        "ON CONFLICT (user_id, role_id, domain_id) DO NOTHING"
    )
    sync_conn.exec_driver_sql("DELETE FROM roles WHERE id IN ('admin', 'superadmin')")
