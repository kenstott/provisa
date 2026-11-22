# Copyright (c) 2026 Kenneth Stott
# Canary: f5c0f000-7f96-46b2-ac29-cc625e76ae82
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The W3C SPARQL fixture's model builds a compilation context (REQ-1929): a relationship never
hides a column, so a relationship whose field would be one of its source table's columns has a
field of its own, and the SPARQL predicate still reaches it through its relationship type."""

# Requirements: REQ-1929, REQ-2167

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest
from rdflib import Graph

from provisa.compiler import naming as _naming
from provisa.compiler.context import build_context
from provisa.compiler.schema_gen import SchemaInput
from provisa.compiler.introspect import ColumnMetadata

CONFORMANCE = Path(__file__).resolve().parents[2] / "scripts" / "conformance"
sys.path.insert(0, str(CONFORMANCE))  # the scripts' sibling imports


def _load(name: str):
    spec = importlib.util.spec_from_file_location(name, CONFORMANCE / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module  # a dataclass resolves its module by name
    spec.loader.exec_module(module)
    return module


_load("pgwire_catalog_types")
_load("sparql_w3c_manifest")
fixtures = _load("sparql_w3c_fixtures")

EX = "http://example.org/"


def schema_input(f) -> SchemaInput:
    """``f``'s tables and relationships as the model store hands them to a role's build: each
    table under an id, each relationship's tables by those ids, the dataset's role reading all."""
    entries = fixtures.table_entries(f)
    ids = {e["table"]: n for n, e in enumerate(entries, start=1)}
    tables = [
        {
            "id": ids[e["table"]],
            "source_id": e["source_id"],
            "domain_id": e["domain_id"],
            "schema_name": e["schema"],
            "table_name": e["table"],
            "alias": e["alias"],
            "columns": [
                {"column_name": c["name"], "visible_to": c["visible_to"]} for c in e["columns"]
            ],
            "write_ops": [],
            "write_returns_rows": False,
            "write_refused_forms": [],
        }
        for e in entries
    ]
    column_types = {
        ids[e["table"]]: [
            ColumnMetadata(column_name=c["name"], data_type=c["data_type"], is_nullable=True)
            for c in e["columns"]
        ]
        for e in entries
    }
    relationships = [
        {
            **{k: v for k, v in r.items() if k != "via_table"},
            "source_table_id": ids[r["source_table_id"]],
            "target_table_id": ids[r["target_table_id"]],
            "via_table_id": ids[r["via_table"]],
        }
        for r in fixtures.relationship_entries(f)
    ]
    return SchemaInput(
        tables=tables,
        relationships=relationships,
        column_types=column_types,
        naming_rules=[],
        role={"id": f.role, "capabilities": [], "domain_access": [f.domain]},
        domains=[{"id": f.domain, "description": f.domain}],
    )


@pytest.fixture(autouse=True)
def _deployment_naming():
    """The instance's conventions: the deployment's defaults (its model names none)."""
    _naming.configure()
    yield
    _naming.configure()


def _adapt(ident: str, turtle: str):
    g = Graph()
    g.parse(data=f"@prefix : <{EX}> .\n{turtle}", format="turtle")
    return fixtures.adapt(ident, g)


def test_a_relationship_numbered_like_a_column_of_its_table_builds():
    # :name is column d013_p0 of the subjects' table and :knows is relationship type D013_P0:
    # named for its type, the relationship was field d013P0 of that table, the column's field.
    f = _adapt("d013", ":a :name 'Ann' ; :knows :b . :b :name 'Bob' .")
    assert f.refusal is None
    assert [c.name for t in f.tables for c in t.columns] == ["d013_p0"]
    assert [e.rel_type for e in f.edges] == ["D013_P0"]
    ctx = build_context(schema_input(f))
    fields = {field for (_type, field) in ctx.joins}
    assert fields and not fields & {"d013P0", "d013_p0"}


def test_the_predicate_still_names_the_relationship_type():
    f = _adapt("d013", ":a :name 'Ann' ; :knows :b . :b :name 'Bob' .")
    ctx = build_context(schema_input(f))
    (join,) = ctx.joins.values()
    assert join.cypher_alias == "D013_P0"
    assert f.predicates[f"{EX}knows"] == f"{fixtures.BASE}/vocab/rel/D013_P0"
