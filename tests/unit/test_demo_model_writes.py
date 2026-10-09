# Copyright (c) 2026 Kenneth Stott
# Canary: 2d8f6b13-a40e-4c97-b5d2-9e1c7f0a3684
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The demo model lets the org's admin write a prospective adopter (REQ-663, REQ-868).

A column is writable only by the roles its ``writable_by`` names; one that names none is
writable by nobody. The demo model named none anywhere, so nothing in the demo could be written
by anyone and the Cypher write spec (CREATE / SET / DELETE on ``Users``, REQ-670) was refused
403. Its ``pet-store`` ``users`` table now names ``org_admin`` on every column.

The statements below are the SQL the Cypher write path lowers those three forms to; they are
admitted in the governance stage every surface shares, from the model file as it is shipped.
"""

# Requirements: REQ-663, REQ-868, REQ-670

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from provisa.compiler.rls import RLSContext
from provisa.compiler.sql_gen import CompilationContext, TableMeta
from provisa.compiler.stage2 import apply_governance, build_governance_context
from provisa.compiler.write_admission import ColumnNotWritable, WriteNotAdmitted

_WRITES = [
    "INSERT INTO users (id, name, email) VALUES (99981, 'E2E Cypher', 'e2e-cypher@example.com')",
    "UPDATE users SET name = 'E2E Cypher Updated' WHERE id = 99981",
    "DELETE FROM users WHERE id = 99981",
]


def _users() -> dict:
    """The demo model's ``users`` table, as the governance stage is handed a table."""
    config = yaml.safe_load(
        (Path(__file__).resolve().parents[2] / "config" / "provisa-install.yaml").read_text()
    )
    (table,) = [t for t in config["tables"] if t["table"] == "users"]
    return {
        "id": 1,
        "domain_id": table["domain_id"],
        "source_id": table["source_id"],
        "schema_name": table["schema"],
        "table_name": "users",
        "columns": [
            {
                "column_name": c["name"],
                "visible_to": c.get("visible_to") or [],
                "writable_by": c.get("writable_by") or [],
                "native_filter_type": None,
            }
            for c in table["columns"]
        ],
        "write_ops": ["insert", "update", "delete"],
        "write_returns_rows": True,
        "write_refused_forms": [],
    }


def _governance(role_id: str, capabilities: list[str]):
    table = _users()
    ctx = CompilationContext()
    ctx.tables = {
        "users": TableMeta(
            table_id=1,
            field_name="users",
            type_name="Users",
            source_id=table["source_id"],
            catalog_name="c",
            schema_name=table["schema_name"],
            table_name="users",
            domain_id=table["domain_id"],
        )
    }
    role = {"id": role_id, "domain_access": ["*"], "capabilities": capabilities}
    return build_governance_context(role_id, RLSContext.empty(), {}, ctx, [table], role)


def test_every_column_of_users_names_org_admin_as_a_writer():
    """A DELETE removes whole rows and needs every column writable, so all five."""
    columns = _users()["columns"]
    assert len(columns) == 5
    assert all(c["writable_by"] == ["org_admin"] for c in columns), columns


@pytest.mark.parametrize("statement", _WRITES)
def test_org_admin_inserts_updates_and_deletes_a_user(statement):
    governed = apply_governance(statement, _governance("org_admin", ["write"]))
    assert governed.split()[0] == statement.split()[0]
    assert "users" in governed


@pytest.mark.parametrize("statement", _WRITES)
def test_a_role_the_columns_do_not_name_is_still_refused(statement):
    """The grant is to org_admin: the demo's analyst, even holding the write right, is not a
    writer of any column."""
    with pytest.raises(ColumnNotWritable, match="analyst"):
        apply_governance(statement, _governance("analyst", ["write"]))


def test_the_write_right_is_still_required():
    with pytest.raises(WriteNotAdmitted, match="does not hold the 'write' right"):
        apply_governance(_WRITES[0], _governance("org_admin", []))
