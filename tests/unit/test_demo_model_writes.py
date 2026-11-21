# Copyright (c) 2026 Kenneth Stott
# Canary: 2d8f6b13-a40e-4c97-b5d2-9e1c7f0a3684
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The demo model lets the org's admin write a prospective adopter (REQ-2074, REQ-868).

A role writes a table only through its write grants (REQ-2074). The demo model granted none
anywhere, so nothing in the demo could be written by anyone and the Cypher write spec (CREATE /
SET / DELETE on ``Users``, REQ-670) was refused 403. Its ``pet-store`` ``users`` table grants
``org_admin`` insert and update over every column, and delete.

The statements below are the SQL the Cypher write path lowers those three forms to; they are
admitted in the governance stage every surface shares, from the model file as it is shipped.
"""

# Requirements: REQ-2074, REQ-868, REQ-670

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from provisa.compiler.rls import RLSContext
from provisa.compiler.sql_gen import CompilationContext, TableMeta
from provisa.compiler.stage2 import apply_governance, build_governance_context
from provisa.compiler.write_admission import WriteNotAdmitted
from provisa.core.db import fold_config_grants

_WRITES = [
    "INSERT INTO users (id, name, email) VALUES (99981, 'E2E Cypher', 'e2e-cypher@example.com')",
    "UPDATE users SET name = 'E2E Cypher Updated' WHERE id = 99981",
    "DELETE FROM users WHERE id = 99981",
]


def _config() -> dict:
    return yaml.safe_load(
        (Path(__file__).resolve().parents[2] / "config" / "provisa-install.yaml").read_text()
    )


def _users(*, folded: bool = True) -> dict:
    """The demo model's ``users`` table, as the governance stage is handed a table: its column
    grants folded over each role's ancestors, as the runtime build folds them (REQ-2155)."""
    config = _config()
    (table,) = [t for t in config["tables"] if t["table"] == "users"]
    users = {
        "id": 1,
        "domain_id": table["domain_id"],
        "source_id": table["source_id"],
        "schema_name": table["schema"],
        "table_name": "users",
        "columns": [
            {
                "column_name": c["name"],
                "visible_to": c.get("visible_to") or [],
                "native_filter_type": None,
            }
            for c in table["columns"]
        ],
        "write_grants": [
            {"rows": None, "check": None, "presets": [], "columns": [], **g}
            for g in table.get("write_grants") or []
        ],
        "write_ops": ["insert", "update", "delete"],
        "write_returns_rows": True,
        "write_refused_forms": [],
    }
    if folded:
        fold_config_grants([users], config.get("roles") or [])
    return users


def _governance(role_id: str, capabilities: list[str], *, folded: bool = True):
    table = _users(folded=folded)
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


def test_users_grants_org_admin_every_write_over_every_column():
    users = _users()
    every = {c["column_name"] for c in users["columns"]}
    assert len(every) == 5
    grants = {g["operation"]: g for g in users["write_grants"] if g["role"] == "org_admin"}
    assert set(grants) == {"insert", "update", "delete"}
    assert set(grants["insert"]["columns"]) == set(grants["update"]["columns"]) == every


@pytest.mark.parametrize("statement", _WRITES)
def test_org_admin_inserts_updates_and_deletes_a_user(statement):
    governed = apply_governance(statement, _governance("org_admin", ["write"]))
    assert governed.split()[0] == statement.split()[0]
    assert "users" in governed


@pytest.mark.parametrize("column", ["id", "name"])
def test_org_admin_is_served_what_it_writes_through_data_steward(column):
    """REQ-039: a column a role sets or filters a write on is one it is served. The model grants
    ``users.id`` and ``users.name`` to data_steward, never to org_admin by name; org_admin is
    served them because it inherits data_steward, and its write over them is admitted."""
    (raw,) = [t for t in _config()["tables"] if t["table"] == "users"]
    granted = next(c["visible_to"] for c in raw["columns"] if c["name"] == column)
    assert "data_steward" in granted
    assert "org_admin" not in granted
    gov = _governance("org_admin", ["write"])
    served = gov.visible_columns[1]
    assert served is None or column in served  # None: every column is served
    statement = f"UPDATE users SET {column} = {column} WHERE id = 99981"
    assert apply_governance(statement, gov).split()[0] == "UPDATE"
    # Without the inheritance fold the grant names no role org_admin is: the write is refused.
    with pytest.raises(PermissionError, match="V003"):
        apply_governance(statement, _governance("org_admin", ["write"], folded=False))


@pytest.mark.parametrize("statement", _WRITES)
def test_a_role_with_no_grant_is_still_refused(statement):
    """The grants are org_admin's: the demo's analyst, even holding the write right, holds
    none."""
    with pytest.raises(WriteNotAdmitted, match="role 'analyst' holds no"):
        apply_governance(statement, _governance("analyst", ["write"]))


def test_the_write_right_is_still_required():
    with pytest.raises(WriteNotAdmitted, match="does not hold the 'write' right"):
        apply_governance(_WRITES[0], _governance("org_admin", []))
