# Copyright (c) 2026 Kenneth Stott
# Canary: 7e0c3a58-2b94-4d61-8f17-a5c9d2e6b043
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Where nothing is public, the one served-set rule answers as the old copies did (REQ-1959).

Which columns a role is served used to be written out separately in the schema build, in SQL
governance, in the catalog's row scope and in ``security/visibility.py``. They are one function
now (``security.rights.column_served``). This holds the consolidation to "no answer changes
where no column is public or restricted": each old copy is kept here as a reference function
and compared, for every role, table and column of the product's demo model and of a
multi-domain model with the lockdown and catalog domains, against what the code computes today.

One answer changes by intent: the catalog's row scope, which went by domain reach alone, is now
the tables a role is served a column of (see the row-scope cases below).

Where the old copies disagreed WITH EACH OTHER, the test says which answer the one rule takes:

- ``security/visibility.py`` read ``visible_to`` literally (an empty list was nobody, ``*`` was
  a role named "*", no lockdown domain). It was a helper no request path called; the schema
  build's reading is the product's, and the helper now gives it.
- SQL governance decided a column by its grant alone, with no domain condition: a table outside
  a role's domains had "visible" columns that only the direct-read check (V001) and the
  relationship guard kept it from. The one rule requires reach (or a public column), so such a
  table now has no visible column for the role — what the schema build always answered.
"""

# Requirements: REQ-1959, REQ-039, REQ-1132, REQ-1133

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.rls import RLSContext
from provisa.compiler.schema_gen import _build_visible_tables
from provisa.compiler.schema_types import SchemaInput
from provisa.compiler.sql_gen import CompilationContext, TableMeta
from provisa.compiler.stage2 import build_governance_context
from provisa.security.rights import compute_meta_row_scope
from provisa.security.visibility import visible_tables

_LOCKDOWN = {"ops"}
_IMPLICIT = {"meta"}


# --- the old copies, as they were ----------------------------------------------------------------


def _old_grant(role_id: str, domain: str, column: dict) -> bool:
    """The column rule the schema build and SQL governance each spelled out."""
    granted = column["visible_to"]
    return (not granted and domain not in _LOCKDOWN) or "*" in granted or role_id in granted


def _old_schema_served(role: dict, tables: list[dict]) -> dict[int, list[str]]:
    """compiler/schema_gen.py _build_visible_tables, before: reach, then the grant."""
    reach = set(role["domain_access"])
    out = {}
    for table in tables:
        if "*" not in reach and table["domain_id"] not in reach | _IMPLICIT:
            continue
        columns = [
            c["column_name"]
            for c in table["columns"]
            if _old_grant(role["id"], table["domain_id"], c) and not c.get("native_filter_type")
        ]
        if columns:
            out[table["id"]] = columns
    return out


def _old_sql_visible(role: dict, table: dict) -> set[str]:
    """compiler/stage2.py build_governance_context, before: the grant alone, no reach."""
    return {
        c["column_name"] for c in table["columns"] if _old_grant(role["id"], table["domain_id"], c)
    }


def _old_meta_scope(role: dict, tables: list[dict]) -> set[int] | None:
    """security/rights.py compute_meta_row_scope, before (no relationships)."""
    reach = role["domain_access"]
    if "*" in reach or "meta" in reach:
        return None
    return {t["id"] for t in tables if t["domain_id"] in reach}


def _old_literal_visible(role: dict, tables: list[dict]) -> dict[int, list[str]]:
    """security/visibility.py visible_tables, before: reach, then ``role in visible_to``."""
    reach = set(role["domain_access"])
    out = {}
    for table in tables:
        if "*" not in reach and table["domain_id"] not in reach:
            continue
        columns = [c["column_name"] for c in table["columns"] if role["id"] in c["visible_to"]]
        if columns:
            out[table["id"]] = columns
    return out


# --- the models ----------------------------------------------------------------------------------


def _demo_model() -> tuple[list[dict], list[dict]]:
    """The product's demo config: its tables and roles as the schema build is handed them."""
    config = yaml.safe_load(
        (Path(__file__).resolve().parents[2] / "config" / "provisa-install.yaml").read_text()
    )
    tables = [
        {
            "id": table_id,
            "domain_id": t["domain_id"],
            "source_id": t["source_id"],
            "schema_name": t["schema"],
            "table_name": t["table"],
            "columns": [
                {
                    "column_name": c["name"],
                    "visible_to": list(c.get("visible_to") or []),
                    "native_filter_type": c.get("native_filter_type"),
                    **({"scope": c["scope"]} if "scope" in c else {}),
                }
                for c in t.get("columns") or []
            ],
            "write_ops": [],
            "write_returns_rows": True,
            "write_refused_forms": [],
        }
        for table_id, t in enumerate(config["tables"], start=1)
    ]
    domains = sorted({t["domain_id"] for t in tables})
    roles = [
        {"id": r["id"], "domain_access": list(r["domain_access"]), "capabilities": []}
        for r in config["roles"]
    ]
    roles += [
        {"id": "org_admin", "domain_access": ["*"], "capabilities": []},
        {"id": "nobody", "domain_access": [], "capabilities": []},
        *({"id": f"only_{d}", "domain_access": [d], "capabilities": []} for d in domains),
    ]
    return tables, roles


def _column(name, visible_to=(), **extra):
    return {
        "column_name": name,
        "visible_to": list(visible_to),
        "native_filter_type": None,
        **extra,
    }


def _table(table_id, domain, name, columns):
    return {
        "id": table_id,
        "domain_id": domain,
        "source_id": "pg",
        "schema_name": "public",
        "table_name": name,
        "columns": columns,
        "write_ops": [],
        "write_returns_rows": True,
        "write_refused_forms": [],
    }


def _multi_domain_model() -> tuple[list[dict], list[dict]]:
    """Two business domains, the lockdown domain and the catalog domain; open, starred, granted
    and parameter columns; a role per reach, and a meta-role of two of them."""
    tables = [
        _table(1, "sales", "orders", [_column("id"), _column("total", ["seller"])]),
        _table(
            2, "sales", "lookup", [_column("v", ["*"]), _column("k", native_filter_type="path")]
        ),
        _table(3, "hr", "staff", [_column("id"), _column("salary", ["hr_reader"])]),
        _table(4, "ops", "audit", [_column("id"), _column("who", ["auditor"])]),
        _table(5, "meta", "registered_tables", [_column("id"), _column("table_name")]),
    ]
    meta_id = "meta:hr_reader+seller"
    for table in tables:
        for column in table["columns"]:
            # What making the meta-role does: a grant naming a member names it.
            if {"seller", "hr_reader"} & set(column["visible_to"]):
                column["visible_to"].append(meta_id)
    roles = [
        {"id": "seller", "domain_access": ["sales"], "capabilities": []},
        {"id": "hr_reader", "domain_access": ["hr"], "capabilities": []},
        {"id": "auditor", "domain_access": ["ops"], "capabilities": []},
        {"id": "cataloguer", "domain_access": ["meta"], "capabilities": []},
        {"id": "everything", "domain_access": ["*"], "capabilities": []},
        {"id": "nothing", "domain_access": [], "capabilities": []},
        {"id": meta_id, "domain_access": ["hr", "sales"], "capabilities": []},
    ]
    return tables, roles


_MODELS = {"demo config": _demo_model(), "multi-domain": _multi_domain_model()}
# In the demo config every column carries an explicit grant, so the two roles this file adds
# that reach one domain and are named in no grant lose that domain's tables (18 + 9). No role
# the demo config itself declares loses a table.
_REMOVED_PAIRS = {"demo config": 27, "multi-domain": 0}
_CASES = [
    pytest.param(tables, role, id=f"{model}/{role['id']}")
    for model, (tables, roles) in _MODELS.items()
    for role in roles
]


def test_the_models_publish_and_restrict_nothing():
    """The premise: the equivalence is about models where no column is public or restricted."""
    for tables, _roles in _MODELS.values():
        scopes = {c.get("scope", "domain") for t in tables for c in t["columns"]}
        assert scopes == {"domain"}, scopes
    assert sum(len(t["columns"]) for t in _MODELS["demo config"][0]) > 100


def _schema_input(tables: list[dict], role: dict) -> SchemaInput:
    return SchemaInput(
        tables=tables,
        relationships=[],
        column_types={
            t["id"]: [
                ColumnMetadata(column_name=c["column_name"], data_type="varchar", is_nullable=True)
                for c in t["columns"]
            ]
            for t in tables
        },
        naming_rules=[],
        role=role,
        domains=[{"id": d} for d in sorted({t["domain_id"] for t in tables})],
    )


# --- the comparisons ------------------------------------------------------------------------------


@pytest.mark.parametrize("tables, role", _CASES)
def test_the_schema_build_serves_what_it_served(tables, role):
    served = {
        info.table_id: [c["column_name"] for c in info.visible_columns]
        for info in _build_visible_tables(_schema_input(tables, role))
    }
    assert served == _old_schema_served(role, tables)


@pytest.mark.parametrize("tables, role", _CASES)
def test_sql_governance_serves_what_it_served_within_reach(tables, role):
    ctx = CompilationContext()
    ctx.tables = {
        f"t{t['id']}": TableMeta(
            table_id=t["id"],
            field_name=f"t{t['id']}",
            type_name=f"T{t['id']}",
            source_id=t["source_id"],
            catalog_name="c",
            schema_name=t["schema_name"],
            table_name=t["table_name"],
            domain_id=t["domain_id"],
        )
        for t in tables
    }
    gov = build_governance_context(role["id"], RLSContext.empty(), {}, ctx, tables, role)
    assert gov.public_tables == frozenset(), "nothing is published"
    reach = set(role["domain_access"])
    for table in tables:
        if table["domain_id"] == "meta":
            continue  # the catalog domain has its own tiered rule, untouched
        visible = gov.visible_columns[table["id"]]
        got = {c["column_name"] for c in table["columns"]} if visible is None else set(visible)
        if "*" in reach or table["domain_id"] in reach:
            assert got == _old_sql_visible(role, table), table["table_name"]
        else:
            # The one place the old copies disagreed on a role's own answer: SQL governance
            # judged a column by its grant alone, the schema build required reach. The rule
            # requires reach; outside it (and with nothing published) no column is visible.
            assert got == set(), table["table_name"]


def _narrowed_meta_scope(role: dict, tables: list[dict]) -> set[int] | None:
    """The catalog row scope as intended: of the tables in a domain the role reaches, those it is
    served at least one column of — the schema build's answer, not domain reach alone."""
    old = _old_meta_scope(role, tables)
    if old is None:
        return None
    return old & set(_old_schema_served(role, tables))


@pytest.mark.parametrize("tables, role", _CASES)
def test_the_catalog_row_scope_is_the_tables_the_role_is_served(tables, role):
    """The ONE intended change of the consolidation. The row scope went by domain reach alone:
    a role that reached a domain was shown the catalog rows of a table every column of which was
    granted to other roles. It is now the tables the role is served a column of."""
    scope = compute_meta_row_scope(role, tables, [])
    assert scope == _narrowed_meta_scope(role, tables)
    old = _old_meta_scope(role, tables)
    assert scope is None if old is None else scope <= old, "never wider than it was"


def test_how_many_role_table_pairs_the_narrowing_removes():
    """Stated, so a change to the fixture models or the rule shows up here: the (role, table)
    pairs that were in the row scope by domain reach and are served no column."""
    removed = {}
    for model, (tables, roles) in _MODELS.items():
        removed[model] = sorted(
            (role["id"], table_id)
            for role in roles
            if (old := _old_meta_scope(role, tables)) is not None
            for table_id in old - set(_old_schema_served(role, tables))
        )
    assert {model: len(pairs) for model, pairs in removed.items()} == _REMOVED_PAIRS, removed


@pytest.mark.parametrize("tables, role", _CASES)
def test_the_visibility_helper_now_answers_as_the_schema_build(tables, role):
    """The helper read grants literally; where that differed from the schema build it was the
    helper that was wrong (an open column is everyone's, ``*`` is everyone, ops needs a grant).
    Where the two already agreed — a column carrying the role's explicit grant, outside the
    lockdown domain — nothing changed: it is still listed."""
    by_table = {t["id"]: t for t in tables}
    listed = {
        t["id"]: {c["column_name"] for c in t["columns"]} for t in visible_tables(tables, role)
    }
    # The helper has no notion of the implicitly reachable catalog domain or of parameter
    # columns; apart from those two it is the schema build's answer.
    reach = set(role["domain_access"])
    expected = {
        table_id: columns
        for table_id, columns in _old_schema_served(role, tables).items()
        if "*" in reach or by_table[table_id]["domain_id"] in reach
    }
    data = {
        table_id: [
            c["column_name"]
            for c in by_table[table_id]["columns"]
            if c["column_name"] in columns and not c.get("native_filter_type")
        ]
        for table_id, columns in listed.items()
    }
    assert {i: c for i, c in data.items() if c} == expected
    for table_id, columns in _old_literal_visible(role, tables).items():
        if by_table[table_id]["domain_id"] in _LOCKDOWN:
            continue
        assert set(columns) <= listed.get(table_id, set()), by_table[table_id]["table_name"]


def test_a_table_granted_wholly_to_other_roles_is_not_in_a_reaching_roles_catalog():
    """The narrowing, directly: both roles reach hr; only the one granted a column of
    ``payroll`` has its catalog rows. A 1-hop neighbour is still discovered through a
    relationship from a table the role is served."""
    tables = [
        _table(1, "hr", "staff", [_column("id")]),
        _table(2, "hr", "payroll", [_column("amount", ["payroll_clerk"])]),
    ]
    reader = {"id": "hr_reader", "domain_access": ["hr"], "capabilities": []}
    clerk = {"id": "payroll_clerk", "domain_access": ["hr"], "capabilities": []}
    assert compute_meta_row_scope(reader, tables, []) == {1}
    assert compute_meta_row_scope(clerk, tables, []) == {1, 2}
    edge = [{"source_table_id": 1, "target_table_id": 2}]
    assert compute_meta_row_scope(reader, tables, edge) == {1, 2}
    hidden = [{"source_table_id": 1, "target_table_id": 2, "hide_target_meta": True}]
    assert compute_meta_row_scope(reader, tables, hidden) == {1}
