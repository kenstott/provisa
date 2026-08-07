# Copyright (c) 2026 Kenneth Stott
# Canary: 9b4d7e02-3c6f-4a1b-8e5d-0f2a6c9b3e77
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1387: the term graph in the published snapshot.

The data_product filter rule extends to terms: a ref publishes only when its column
does, a rooted term whose refs are all withheld is withheld with them, an abstract
term publishes on its edges alone, and an edge needs both endpoints published.
"""

# Requirements: REQ-1387

from __future__ import annotations

from provisa.api.metadata_export.builder import build_snapshot
from provisa.core.models import Column, Domain, ProvisaConfig, Source, SourceType, Table


def _table(table_name: str, *, data_product: bool = True, columns: list[str] | None = None):
    return Table(
        source_id="wh",
        domain_id="sales",
        schema_name="public",
        table_name=table_name,
        data_product=data_product,
        columns=[
            Column(name=c, data_type="integer", visible_to=["admin"])
            for c in (columns or ["id"])
        ],
    )


def _config(tables: list[Table]) -> ProvisaConfig:
    return ProvisaConfig(
        sources=[Source(id="wh", type=SourceType.postgresql, description="warehouse")],
        domains=[Domain(id="sales", description="Sales", steward="data-steward")],
        tables=tables,
        roles=[],
    )


def _glossary() -> dict:
    return {
        "terms": [
            {"id": 1, "name": "customer", "definition": "buyer", "is_abstract": False,
             "deprecated": False},
            {"id": 2, "name": "shadow", "definition": None, "is_abstract": False,
             "deprecated": False},
            {"id": 3, "name": "party", "definition": None, "is_abstract": True,
             "deprecated": False},
        ],
        "refs": [
            {"term_id": 1, "column_name": "cust_id", "source_id": "wh",
             "schema_name": "public", "table_name": "orders"},
            {"term_id": 2, "column_name": "hidden_id", "source_id": "wh",
             "schema_name": "public", "table_name": "staging"},
        ],
        "edges": [
            {"from_term_id": 1, "to_term_id": 3, "rel_type": "KIND_OF"},
            {"from_term_id": 2, "to_term_id": 3, "rel_type": "KIND_OF"},
        ],
        "experts": [{"term_id": 1, "user_id": "alice", "kind": "expert"}],
    }


def test_terms_follow_the_data_product_filter():
    config = _config(
        [
            _table("orders", columns=["cust_id"]),
            _table("staging", data_product=False, columns=["hidden_id"]),
        ]
    )
    snapshot = build_snapshot(
        config, org_id="acme", dialect="postgres", glossary=_glossary()
    )
    names = {t.name for t in snapshot.glossary_terms}
    # 'shadow' is rooted only in the withheld table, so it is withheld with it; the
    # abstract term publishes on its edges alone.
    assert names == {"customer", "party"}
    customer = next(t for t in snapshot.glossary_terms if t.name == "customer")
    assert [r.parts for r in customer.refs] == [("wh", "public", "orders", "cust_id")]
    assert customer.experts == ("alice",)
    assert customer.semantic_uri == "provisa://acme/terms/customer"
    # Only the edge with both endpoints published survives.
    assert [(e.from_term_id, e.to_term_id, e.rel_type) for e in snapshot.glossary_edges] == [
        (1, 3, "KIND_OF")
    ]


def test_snapshot_without_glossary_is_empty_not_absent():
    config = _config([_table("orders")])
    snapshot = build_snapshot(config, org_id="acme", dialect="postgres")
    assert snapshot.glossary_terms == []
    assert snapshot.glossary_edges == []
