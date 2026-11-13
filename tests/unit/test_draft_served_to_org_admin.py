# Copyright (c) 2026 Kenneth Stott
# Canary: 5e1d7c3a-92b4-4f60-8a1e-c4d2b7f90e35
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1921, A DRAFT IS SERVED TO ORG_ADMIN (decided 2026-11-12): a draft is read as a served table
is by a role holding table registration in the draft's domain -- the right a draft's preview
needs -- held as rights are: by the role, a role it inherits from, or a member of a meta role,
each paired with the domains of the role carrying it. org_admin holds it everywhere and reads
every draft; a domain steward reads its domains' drafts; an analyst reads none. A table's
Associations read the table's own row of the model table registered_tables (its class term)
through the data surface as the acting role; right after a registration that row is a draft's."""

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
        conn.execute(t.domains.insert().values(id="finance", description="Money"))
        for table_id, name, draft, domain in (
            (1, "served", False, "pet_store"),
            (2, "drafted", True, "pet_store"),
            (3, "ledger", True, "finance"),
        ):
            conn.execute(
                t.registered_tables.insert().values(
                    id=table_id,
                    source_id="src",
                    domain_id=domain,
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
    assert sorted(ids) == [1, 2, 3]


# The governance a statement over the model table is given, by role: which rows it reads.
_RT = 1


#: The roles as the build registers them: each with its own rights and the domains it reaches.
_EVERY = ["*", "meta", "ops", "ontology"]
_REG = "table_registration"
_ROLES = {
    "org_admin": {"id": "org_admin", "capabilities": [_REG, "usage"], "domain_access": _EVERY},
    "pet_steward": {"id": "pet_steward", "capabilities": [_REG], "domain_access": ["pet_store"]},
    "analyst": {"id": "analyst", "capabilities": ["usage"], "domain_access": _EVERY},
    "finance_reader": {
        "id": "finance_reader",
        "capabilities": ["usage"],
        "domain_access": ["finance"],
    },
    "child": {"id": "child", "capabilities": ["usage"], "domain_access": ["pet_store"]},
}


def _built(role_id: str, chain: list[str]) -> tuple[dict, dict]:
    """The role as the build holds it, beside the registry: a role its own record with its acting
    chain (REQ-2188); a meta role carrying no right of its own, reaching its members' union."""
    own = _ROLES.get(role_id, {"id": role_id, "capabilities": [], "domain_access": _EVERY})
    role = {**own, "acting_chain": chain}
    return role, {**_ROLES, role_id: role}


def _governed(role_id: str, chain: list[str]):
    from provisa.compiler.sql_types import CompilationContext, TableMeta
    from provisa.compiler.stage2 import build_governance_context
    from provisa.security.rights import META_DOMAIN_ID, draft_reach

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
    # As the build holds it: with the roles it acts as (REQ-2188); a meta role reaches the union.
    role, roles = _built(role_id, chain)
    return build_governance_context(
        role_id, rls, {}, ctx, served, role, draft_reach=lambda: draft_reach(role, roles)
    )


def _rows_read(plane, gov) -> list[int]:
    rule = gov.rls_rules.get(_RT)
    where = "" if rule is None else f" WHERE {rule.sql(dialect='postgres')}"
    with plane.begin() as conn:
        return sorted(
            r[0] for r in conn.exec_driver_sql(f"SELECT id FROM registered_tables_meta{where}")
        )


# A built role's acting chain: itself and its ancestors, or a meta role's members and theirs; and
# the rows of registered_tables_meta it reads (1 is served; 2 a pet_store draft, 3 a finance one).
_CASES = [
    ("org_admin", ["org_admin"], [1, 2, 3]),
    ("pet_steward", ["pet_steward"], [1, 2]),
    ("analyst", ["analyst"], [1]),
    # A role inheriting the right from its parent (REQ-2155), in the parent's domains.
    ("child", ["child", "org_admin"], [1, 2, 3]),
    ("child", ["child", "pet_steward"], [1, 2]),
    # The meta role a person holding several roles acts as ("Role: All").
    ("meta:analyst+org_admin", ["analyst", "org_admin"], [1, 2, 3]),
    ("meta:analyst+pet_steward", ["analyst", "pet_steward"], [1, 2]),
    ("meta:analyst+finance_reader", ["analyst", "finance_reader"], [1]),
    # The right is paired with its own role's domains: finance comes with no right (REQ-1944).
    ("meta:finance_reader+pet_steward", ["finance_reader", "pet_steward"], [1, 2]),
]


@pytest.mark.parametrize(("role_id", "chain", "rows"), _CASES)
def test_a_drafts_row_is_read_where_the_role_registers_tables(plane, role_id, chain, rows):
    """The Associations' class-term read (registered_tables, id eq the draft's) finds its row."""
    assert _rows_read(plane, _governed(role_id, chain)) == rows


@pytest.mark.parametrize(("role_id", "chain", "rows"), _CASES)
def test_registering_tables_in_a_drafts_domain_is_the_one_answer(role_id, chain, rows):
    from provisa.security.rights import registers_tables_in

    role, roles = _built(role_id, chain)
    assert registers_tables_in(role, roles, "pet_store") == (2 in rows)
    assert registers_tables_in(role, roles, "finance") == (3 in rows)
