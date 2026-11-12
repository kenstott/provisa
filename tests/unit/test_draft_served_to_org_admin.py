# Copyright (c) 2026 Kenneth Stott
# Canary: 5e1d7c3a-92b4-4f60-8a1e-c4d2b7f90e35
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1921, A DRAFT IS SERVED TO ORG_ADMIN: a draft is read as a served table is by a role that
holds org_admin. A table's Associations read the table's own row of the model table
registered_tables (its class term) through the data surface as the acting role; right after a
registration that row is a draft's."""

# Requirements: REQ-1921

from __future__ import annotations

import pytest

from provisa.api._meta_views import _META_TABLE_VIEWS
from provisa.core import schema_org

_SCHEMA = "org_default"


@pytest.fixture(scope="module")
def plane(embedded_postgres, control_plane_template):
    from tests.pg_control_plane import clone

    held = clone(embedded_postgres, control_plane_template)
    engine = held.engine(f"{held.url}?options=-csearch_path%3D{_SCHEMA},public")
    t = schema_org
    with engine.begin() as conn:
        conn.execute(t.sources.insert().values(id="src", type="postgresql"))
        conn.execute(t.domains.insert().values(id="pet_store", description="Pets"))
        for table_id, name, draft in ((1, "served", False), (2, "drafted", True)):
            conn.execute(
                t.registered_tables.insert().values(
                    id=table_id,
                    source_id="src",
                    domain_id="pet_store",
                    schema_name="public",
                    table_name=name,
                    draft=draft,
                )
            )
        conn.exec_driver_sql(_META_TABLE_VIEWS["registered_tables"].replace("%", "%%"))
    yield engine
    held.close()


def test_the_model_table_holds_a_drafts_row(plane):
    """The model table a draft's class term is read from holds the draft's row: which roles read
    it is the statement's governance, not the view's."""
    with plane.begin() as conn:
        ids = [r[0] for r in conn.exec_driver_sql("SELECT id FROM registered_tables_meta")]
    assert sorted(ids) == [1, 2]


# The governance a statement over the model table is given, by role: which rows it reads.
_RT = 1


def _governed(role_id: str, domain_access: list[str], chain: list[str]):
    from provisa.compiler.sql_types import CompilationContext, TableMeta
    from provisa.compiler.stage2 import build_governance_context
    from provisa.security.rights import META_DOMAIN_ID

    ctx = CompilationContext()
    ctx.tables["registered_tables"] = TableMeta(
        table_id=_RT,
        field_name="registered_tables",
        type_name="RegisteredTables",
        source_id="s",
        catalog_name="main",
        schema_name="public",
        table_name="registered_tables_meta",
        domain_id=META_DOMAIN_ID,
    )
    # REQ-2154: the served table's column names its reader.
    column = {"column_name": "id", "visible_to": [role_id]}
    served = [{"id": 1, "domain_id": "pet_store", "columns": [column]}]
    rls = type("Rls", (), {"rules": {}, "write_rules": {}})()
    # As the build holds it: with the roles it acts as (REQ-2188).
    role = {
        "id": role_id,
        "capabilities": [],
        "domain_access": domain_access,
        "acting_chain": chain,
    }
    return build_governance_context(role_id, rls, {}, ctx, served, role)


def _rows_read(plane, gov) -> list[int]:
    rule = gov.rls_rules.get(_RT)
    where = "" if rule is None else f" WHERE {rule.sql(dialect='postgres')}"
    with plane.begin() as conn:
        return sorted(
            r[0] for r in conn.exec_driver_sql(f"SELECT id FROM registered_tables_meta{where}")
        )


_EVERY = ["*", "meta", "ops", "ontology"]

# A built role's acting chain: itself and its ancestors, or a meta role's members and theirs.
_HOLDING = [
    ("org_admin", _EVERY, ["org_admin"]),
    # A role inheriting from org_admin through its parents (REQ-2155).
    ("steward", ["pet_store"], ["steward", "org_admin"]),
    # The meta role a person holding several roles acts as ("Role: All").
    (
        "meta:analyst+data_product_owner+org_admin",
        _EVERY,
        ["analyst", "data_product_owner", "org_admin"],
    ),
    # A scoped meta role: the draft beside the rows of its scope.
    ("meta:analyst+org_admin", ["pet_store"], ["analyst", "org_admin"]),
]
_NOT_HOLDING = [
    ("analyst", _EVERY, ["analyst", "viewer"]),
    ("meta:analyst+data_product_owner", _EVERY, ["analyst", "data_product_owner"]),
    ("analyst", ["pet_store"], ["analyst"]),
    ("org_admin_helper", ["pet_store"], ["org_admin_helper"]),
]


@pytest.mark.parametrize(("role_id", "domain_access", "chain"), _HOLDING)
def test_a_role_holding_org_admin_reads_a_drafts_row(plane, role_id, domain_access, chain):
    """The Associations' class-term read (registered_tables, id eq the draft's) finds its row."""
    assert _rows_read(plane, _governed(role_id, domain_access, chain)) == [1, 2]


@pytest.mark.parametrize(("role_id", "domain_access", "chain"), _NOT_HOLDING)
def test_no_other_role_reads_a_drafts_row(plane, role_id, domain_access, chain):
    assert _rows_read(plane, _governed(role_id, domain_access, chain)) == [1]


@pytest.mark.parametrize(("role_id", "domain_access", "chain"), _HOLDING + _NOT_HOLDING)
def test_holding_org_admin_is_org_admin_inherited_or_a_members(role_id, domain_access, chain):
    from provisa.security.rights import holds_org_admin

    role = {"id": role_id, "domain_access": domain_access, "acting_chain": chain}
    assert holds_org_admin(role) == ((role_id, domain_access, chain) in _HOLDING)
