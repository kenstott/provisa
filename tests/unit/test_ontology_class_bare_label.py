# Copyright (c) 2026 Kenneth Stott
# Canary: 7612c286-9dd2-49db-898c-4cb9cb54e6c0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An ontology Class table never takes a model table's bare label (REQ-2163, REQ-2176): where a
model table and a Class share a name, the bare label is the model table's on every surface -- a
read, a write and the session's classification alike -- and the Class is reached by its qualified
label (Ontology:Customer). A Class no model table shares a name with keeps its bare label."""

# Requirements: REQ-2163, REQ-2176

from __future__ import annotations

import pytest

from provisa.compiler.ontology_classes import class_table_id, class_tables
from tests.unit.test_ontology_classes import (
    CUSTOMER,
    ORDER,
    _GROUNDINGS,
    _REFS,
    _TERMS,
    _col,
    _tables,
)

pytestmark = pytest.mark.unit

MODEL_CUSTOMER = 4


@pytest.fixture
def label_map(monkeypatch):
    """The analyst's label map over the sales and crm tables, a sales table named customer, and
    the Classes customer (grounded in customers and clients) and order (in orders)."""
    import provisa.api.app as appmod
    from provisa.api import app_loaders
    from provisa.api.role_surface import role_surface
    from provisa.compiler.introspect import ColumnMetadata
    from provisa.core import domain_policy
    from provisa.gql.label_map import CypherLabelMap

    monkeypatch.setattr(domain_policy, "single_domain", lambda: False)
    state = appmod.state
    for name in ("roles", "schemas", "contexts", "rls_contexts", "table_path_maps", "proto_files"):
        monkeypatch.setattr(state, name, {}, raising=False)
    monkeypatch.setattr(state, "graphql_remote_sources", {}, raising=False)
    monkeypatch.setattr(
        state, "source_types", {"pg": "postgresql", "__derived__": "postgresql"}, raising=False
    )
    monkeypatch.setattr(state, "source_catalogs", {"pg": "pg"}, raising=False)
    tables = _tables() + [
        {
            "id": MODEL_CUSTOMER,
            "source_id": "pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "customer",
            "columns": [_col("id", "integer"), _col("name", "varchar")],
        }
    ]
    for t in tables:
        t.update(write_ops=[], write_returns_rows=False, write_refused_forms=[])
        t["governance"] = "pre-approved"
    rows, refused = class_tables(tables, _GROUNDINGS, _TERMS, _REFS)
    assert refused == {}
    for r in rows:
        r.update(governance="pre-approved", write_ops=[], write_returns_rows=False)
        r["write_refused_forms"] = []
    tables = [*tables, *rows]
    app_loaders._build_and_register_schemas(
        roles=[
            {
                "id": "analyst",
                "description": "t",
                "capabilities": ["usage"],
                "domain_access": ["sales", "crm", "ontology"],
            }
        ],
        tables=tables,
        relationships=[],
        col_types_converted={
            t["id"]: [
                ColumnMetadata(
                    column_name=c["column_name"], data_type=c["data_type"], is_nullable=True
                )
                for c in t["columns"]
            ]
            for t in tables
        },
        naming_rules=[],
        domains=[{"id": "sales"}, {"id": "crm"}, {"id": "ontology"}],
        domain_prefix=False,
        kafka_physical={},
        tracked_functions=[],
        tracked_webhooks=[],
        gql_object_cols={},
        rls_rules=[],
        metrics=[],
        draft_tables=[],
    )
    ctx = role_surface(state, "analyst").context
    return CypherLabelMap.from_schema(
        ctx, domain_access=["sales", "crm", "ontology"], all_tables=tables
    )


def _type_of(label_map, table_id: int) -> str:
    (type_name,) = [t for t, nm in label_map.nodes.items() if nm.table_id == table_id]
    return type_name


def _resolved(label_map, *labels: str) -> str | None:
    from provisa.gql.translator import _Translator

    from provisa.cypher.parser import parse_cypher

    return _Translator(parse_cypher("RETURN 1"), label_map, {})._resolve_node_type(list(labels))[0]


def test_the_bare_label_is_the_model_tables(label_map):
    model = _type_of(label_map, MODEL_CUSTOMER)
    assert label_map.nodes_by_table["Customer"] == [model]
    assert _resolved(label_map, "Customer") == model


def test_the_qualified_label_reads_the_class(label_map):
    assert _resolved(label_map, "Ontology", "Customer") == _type_of(
        label_map, class_table_id(CUSTOMER)
    )


def test_a_write_by_the_bare_label_goes_to_the_model_table(label_map):
    from provisa.cypher.write_translator import WriteTranslator, parse_cypher_write

    sql = WriteTranslator(label_map).translate(
        parse_cypher_write("CREATE (n:Customer {id: 1, name: 'Ada'})")
    )
    assert '"customer"' in sql and "INSERT INTO" in sql and '"ontology"' not in sql


def test_the_session_classifies_the_bare_label_as_the_model_table(label_map):
    from provisa.ephemeral.classify import resolve

    assert resolve(["Customer"], label_map).exact
    assert resolve(["Customer"], label_map).models == (_type_of(label_map, MODEL_CUSTOMER),)


def test_a_class_no_model_table_shares_a_name_with_keeps_its_bare_label(label_map):
    order = _type_of(label_map, class_table_id(ORDER))
    assert _resolved(label_map, "Order") == order
