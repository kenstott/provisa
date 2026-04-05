# Copyright (c) 2026 Kenneth Stott
# Canary: 260f4059-71eb-457a-a196-346f0c4454c4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Tests for discovery prompt builder."""

from provisa.discovery.collector import DiscoveryInput, TableMeta
from provisa.discovery.prompt import build_prompt


def _make_table(table_id, schema, name, domain, columns):
    return TableMeta(
        table_id=table_id,
        source_id="src1",
        domain_id=domain,
        schema_name=schema,
        table_name=name,
        columns=[{"name": c, "type": "integer"} for c in columns],
        sample_values=[{c: "1" for c in columns}],
    )


def test_prompt_includes_table_metadata():
    t = _make_table(1, "public", "orders", "sales", ["id", "customer_id"])
    di = DiscoveryInput(tables=[t], existing_relationships=[], rejected_pairs=[])
    prompt = build_prompt(di)
    assert "public.orders" in prompt
    assert "id (integer)" in prompt
    assert "customer_id (integer)" in prompt


def test_prompt_includes_multiple_tables_for_domain_scope():
    t1 = _make_table(1, "public", "orders", "sales", ["id", "customer_id"])
    t2 = _make_table(2, "public", "customers", "sales", ["id", "name"])
    di = DiscoveryInput(tables=[t1, t2], existing_relationships=[], rejected_pairs=[])
    prompt = build_prompt(di)
    assert "public.orders" in prompt
    assert "public.customers" in prompt


def test_existing_relationships_excluded_from_prompt():
    t = _make_table(1, "public", "orders", "sales", ["id"])
    rel = {
        "source_table_id": 1,
        "source_column": "customer_id",
        "target_table_id": 2,
        "target_column": "id",
        "cardinality": "many-to-one",
    }
    di = DiscoveryInput(tables=[t], existing_relationships=[rel], rejected_pairs=[])
    prompt = build_prompt(di)
    assert "Already Existing Relationships" in prompt
    assert "source_column=customer_id" in prompt


def test_rejected_relationships_excluded_from_prompt():
    t = _make_table(1, "public", "orders", "sales", ["id"])
    rej = {
        "source_table_id": 1,
        "source_column": "status_id",
        "target_table_id": 3,
        "target_column": "id",
    }
    di = DiscoveryInput(tables=[t], existing_relationships=[], rejected_pairs=[rej])
    prompt = build_prompt(di)
    assert "Previously Rejected" in prompt
    assert "source_column=status_id" in prompt


def test_cross_domain_prompt_includes_tables_from_multiple_domains():
    t1 = _make_table(1, "public", "orders", "sales", ["id"])
    t2 = _make_table(2, "public", "products", "inventory", ["id"])
    t3 = _make_table(3, "public", "shipments", "logistics", ["id"])
    di = DiscoveryInput(tables=[t1, t2, t3], existing_relationships=[], rejected_pairs=[])
    prompt = build_prompt(di)
    assert "domain=sales" in prompt
    assert "domain=inventory" in prompt
    assert "domain=logistics" in prompt
