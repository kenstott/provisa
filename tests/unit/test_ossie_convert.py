# Copyright (c) 2026 Kenneth Stott
# Canary: a74444cb-9dad-4d91-903a-f9298d873b0f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1316: Apache Ossie interchange boundary converter (export/import/round-trip)."""

import json

import pytest
import yaml

from provisa.core.models import (
    Column,
    Domain,
    Metric,
    ProvisaConfig,
    Relationship,
    Source,
    Table,
    UniqueConstraint,
)
from provisa.ossie.convert import build_ossie_model, ossie_yaml, parse_ossie_model


def _config() -> ProvisaConfig:
    return ProvisaConfig(
        sources=[Source(id="pg1", type="postgresql")],
        domains=[Domain(id="sales")],
        roles=[],
        tables=[
            Table(
                source_id="pg1",
                domain_id="sales",
                schema_name="public",
                table_name="orders",
                description="Order fact table",
                modeling_role="fact",
                modeling_history="snapshot",
                columns=[
                    Column(name="id", data_type="bigint", visible_to=["*"], is_primary_key=True),
                    Column(name="customer_id", data_type="bigint", visible_to=["*"]),
                    Column(name="amount", data_type="numeric(10,2)", visible_to=["*"]),
                    Column(name="ordered_at", data_type="timestamp", visible_to=["*"]),
                ],
                unique_constraints=[
                    UniqueConstraint(name="orders_nk", columns=["customer_id", "ordered_at"])
                ],
            ),
            Table(
                source_id="pg1",
                domain_id="sales",
                schema_name="public",
                table_name="customers",
                columns=[
                    Column(name="id", data_type="bigint", visible_to=["*"], is_primary_key=True),
                    Column(
                        name="name",
                        data_type="varchar(255)",
                        visible_to=["*"],
                        description="Customer display name",
                    ),
                ],
            ),
        ],
        relationships=[
            Relationship(
                id="orders_customer",
                source_table_id="orders",
                target_table_id="customers",
                source_column="customer_id",
                target_column="id",
                cardinality="many-to-one",
                alias="PLACED_BY",
            )
        ],
        metrics=[
            Metric(
                name="total_revenue",
                expression="SUM(orders.amount)",
                datatype="number",
                description="Total order revenue",
                ai_context="Sum of all order amounts; the headline revenue figure.",
            )
        ],
    )


# ── export ────────────────────────────────────────────────────────────────────


def test_export_document_shape():
    doc = build_ossie_model(_config())
    assert doc["version"] == "0.2.0.dev0"
    assert len(doc["semantic_model"]) == 1
    model = doc["semantic_model"][0]
    assert model["name"] == "sales"  # first domain id
    assert [d["name"] for d in model["datasets"]] == ["orders", "customers"]


def test_export_dataset_source_keys_and_fields():
    model = build_ossie_model(_config())["semantic_model"][0]
    orders = model["datasets"][0]
    assert orders["source"] == "pg1.public.orders"
    assert orders["primary_key"] == ["id"]
    assert orders["unique_keys"] == [["customer_id", "ordered_at"]]
    assert orders["description"] == "Order fact table"
    fields = {f["name"]: f for f in orders["fields"]}
    assert fields["id"]["expression"] == {"dialects": [{"dialect": "ANSI_SQL", "expression": "id"}]}
    assert fields["id"]["datatype"] == "integer"
    assert fields["amount"]["datatype"] == "number"  # numeric(10,2) → number
    customers = model["datasets"][1]
    cfields = {f["name"]: f for f in customers["fields"]}
    assert cfields["name"]["datatype"] == "string"  # varchar(255) → string
    assert cfields["name"]["description"] == "Customer display name"


def test_export_time_column_gets_is_time_dimension():
    model = build_ossie_model(_config())["semantic_model"][0]
    fields = {f["name"]: f for f in model["datasets"][0]["fields"]}
    assert fields["ordered_at"]["dimension"] == {"is_time": True}
    assert "dimension" not in fields["amount"]


def test_export_unknown_datatype_passes_through_verbatim():
    cfg = _config()
    cfg.tables[1].columns[1].data_type = "hstore"
    model = build_ossie_model(cfg)["semantic_model"][0]
    cfields = {f["name"]: f for f in model["datasets"][1]["fields"]}
    assert cfields["name"]["datatype"] == "hstore"


def test_export_modeling_role_custom_extension():  # REQ-1320
    model = build_ossie_model(_config())["semantic_model"][0]
    orders, customers = model["datasets"]
    exts = orders["custom_extensions"]
    assert len(exts) == 1
    assert exts[0]["vendor_name"] == "provisa"
    assert json.loads(exts[0]["data"]) == {
        "modeling_role": "fact",
        "modeling_history": "snapshot",
    }
    assert "custom_extensions" not in customers  # no role set → no slot


def test_export_relationship_shape():
    model = build_ossie_model(_config())["semantic_model"][0]
    assert model["relationships"] == [
        {
            "name": "PLACED_BY",  # alias wins over id
            "from": "orders",
            "to": "customers",
            "from_columns": ["customer_id"],
            "to_columns": ["id"],
        }
    ]


def test_export_metric_verbatim():
    model = build_ossie_model(_config())["semantic_model"][0]
    assert model["metrics"] == [
        {
            "name": "total_revenue",
            "expression": {
                "dialects": [{"dialect": "ANSI_SQL", "expression": "SUM(orders.amount)"}]
            },
            "datatype": "number",
            "description": "Total order revenue",
            "ai_context": "Sum of all order amounts; the headline revenue figure.",
        }
    ]


def test_export_unresolvable_relationship_is_hard_error():
    cfg = _config()
    cfg.relationships[0].target_table_id = "nonexistent"
    with pytest.raises(ValueError, match="nonexistent"):
        build_ossie_model(cfg)


# ── yaml ──────────────────────────────────────────────────────────────────────


def test_ossie_yaml_valid_and_deterministic():
    cfg = _config()
    text1 = ossie_yaml(cfg)
    text2 = ossie_yaml(cfg)
    assert text1 == text2  # deterministic
    assert yaml.safe_load(text1) == build_ossie_model(cfg)


# ── import / round-trip ───────────────────────────────────────────────────────


def test_parse_round_trips_export():
    doc = build_ossie_model(_config())
    imp = parse_ossie_model(doc)
    assert imp.model_name == "sales"

    orders = imp.tables[0]
    assert orders["table_name"] == "orders"
    assert orders["schema_name"] == "public"
    assert orders["source_id"] == "pg1"
    assert orders["primary_key"] == ["id"]
    assert orders["unique_keys"] == [["customer_id", "ordered_at"]]
    cols = {c["name"]: c for c in orders["columns"]}
    assert cols["id"]["is_primary_key"] is True
    assert cols["amount"]["is_primary_key"] is False
    assert cols["amount"]["datatype"] == "number"
    # REQ-1320: modeling metadata restored from the provisa custom_extensions slot.
    assert orders["modeling_role"] == "fact"
    assert orders["modeling_history"] == "snapshot"
    assert "modeling_role" not in imp.tables[1]

    assert imp.relationships == [
        {
            "name": "PLACED_BY",
            "from": "orders",
            "to": "customers",
            "from_columns": ["customer_id"],
            "to_columns": ["id"],
        }
    ]
    assert imp.metrics == [
        {
            "name": "total_revenue",
            "expression": "SUM(orders.amount)",
            "datatype": "number",
            "description": "Total order revenue",
            "ai_context": "Sum of all order amounts; the headline revenue figure.",
        }
    ]


def test_parse_missing_semantic_model_names_path():
    with pytest.raises(ValueError, match=r"\$\.semantic_model"):
        parse_ossie_model({"version": "0.2.0.dev0"})


def test_parse_dataset_without_source_names_path():
    doc = build_ossie_model(_config())
    del doc["semantic_model"][0]["datasets"][1]["source"]
    with pytest.raises(ValueError, match=r"semantic_model\[0\]\.datasets\[1\]\.source"):
        parse_ossie_model(doc)


def test_parse_metric_without_expression_names_path():
    doc = build_ossie_model(_config())
    del doc["semantic_model"][0]["metrics"][0]["expression"]
    with pytest.raises(ValueError, match=r"semantic_model\[0\]\.metrics\[0\]\.expression"):
        parse_ossie_model(doc)


def test_parse_non_mapping_document_rejected():
    with pytest.raises(ValueError, match="mapping"):
        parse_ossie_model(["not", "a", "doc"])
