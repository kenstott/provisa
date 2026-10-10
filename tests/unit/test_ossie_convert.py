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
from provisa.ossie.convert import (
    DATATYPES,
    OSSIE_SPEC_COMMIT,
    OssieExportRefused,
    build_ossie_model,
    ossie_yaml,
    parse_ossie_model,
)


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
                datatype="numeric",
                description="Total order revenue",
                ai_context="Sum of all order amounts; the headline revenue figure.",
            )
        ],
    )


# ── export ────────────────────────────────────────────────────────────────────


def _schema() -> dict:
    """Ossie's own schema, as tests/fixtures/ossie/SOURCE.txt says where it is from."""
    from pathlib import Path

    fixtures = Path(__file__).resolve().parents[1] / "fixtures" / "ossie"
    assert OSSIE_SPEC_COMMIT in (fixtures / "SOURCE.txt").read_text().replace("\n", "")
    return json.loads((fixtures / "ossie-schema.json").read_text())


def _schema_problems(doc: dict) -> list[str]:
    import jsonschema

    validator = jsonschema.Draft202012Validator(_schema())
    return sorted(f"{'/'.join(map(str, e.path))}: {e.message}" for e in validator.iter_errors(doc))


def test_the_export_is_valid_against_ossies_own_schema():
    assert _schema_problems(build_ossie_model(_config())) == []


def test_the_export_with_types_outside_the_vocabulary_is_still_valid():
    cfg = _config()
    cfg.tables[1].columns[1].data_type = "hstore"
    cfg.metrics[0].datatype = "money"
    assert _schema_problems(build_ossie_model(cfg)) == []


def test_the_schema_refuses_the_shape_provisa_used_to_write():
    """What the schema check is worth: the wrapper and the old type names are both refused."""
    doc = build_ossie_model(_config())
    wrapped = {"version": doc["version"], "semantic_model": [dict(doc, version=None)]}
    assert any("semantic_model" in problem for problem in _schema_problems(wrapped))
    doc["datasets"][0]["fields"][0]["datatype"] = "integer"
    assert any("'integer' is not one of" in problem for problem in _schema_problems(doc))


def test_export_document_shape():
    doc = build_ossie_model(_config())
    assert list(doc) == ["version", "name", "datasets", "relationships", "metrics"]
    assert doc["version"] == "0.2.0.dev0"
    assert doc["name"] == "sales"  # first domain id
    assert "semantic_model" not in doc
    assert [d["name"] for d in doc["datasets"]] == ["orders", "customers"]


def test_a_model_with_no_table_is_refused_by_name():
    cfg = _config()
    cfg.tables, cfg.relationships = [], []
    with pytest.raises(OssieExportRefused, match="needs at least one dataset"):
        build_ossie_model(cfg)


def test_export_dataset_source_keys_and_fields():
    orders = build_ossie_model(_config())["datasets"][0]
    assert orders["source"] == "pg1.public.orders"
    assert orders["primary_key"] == ["id"]
    assert orders["unique_keys"] == [["customer_id", "ordered_at"]]
    assert orders["description"] == "Order fact table"
    fields = {f["name"]: f for f in orders["fields"]}
    assert fields["id"]["expression"] == {"dialects": [{"dialect": "ANSI_SQL", "expression": "id"}]}
    assert fields["id"]["datatype"] == "Integer"
    assert fields["amount"]["datatype"] == "Decimal"  # numeric(10,2)
    customers = build_ossie_model(_config())["datasets"][1]
    cfields = {f["name"]: f for f in customers["fields"]}
    assert cfields["name"]["datatype"] == "String"  # varchar(255)
    assert cfields["name"]["description"] == "Customer display name"


@pytest.mark.parametrize(
    ("source_type", "datatype"),
    [
        ("varchar(255)", "String"),
        ("uuid", "String"),
        ("bigint", "Integer"),
        ("numeric(10,2)", "Decimal"),
        ("double precision", "Float"),
        ("boolean", "Boolean"),
        ("date", "Date"),
        ("time", "Time"),
        ("timestamp", "DateTime"),
        ("timestamp with time zone", "DateTimeTz"),
        ("timestamptz", "DateTimeTz"),
    ],
)
def test_a_source_type_is_written_as_its_ossie_datatype(source_type, datatype):
    cfg = _config()
    cfg.tables[1].columns[1].data_type = source_type
    field = build_ossie_model(cfg)["datasets"][1]["fields"][1]
    assert field["datatype"] == datatype and datatype in DATATYPES
    assert "custom_extensions" not in field


def test_export_time_column_gets_is_time_dimension():
    fields = {f["name"]: f for f in build_ossie_model(_config())["datasets"][0]["fields"]}
    assert fields["ordered_at"]["datatype"] == "DateTime"
    assert fields["ordered_at"]["dimension"] == {"is_time": True}
    assert "dimension" not in fields["amount"]


def test_a_type_outside_the_vocabulary_is_opaque_and_keeps_its_name_in_the_provisa_slot():
    cfg = _config()
    cfg.tables[1].columns[1].data_type = "hstore"
    field = build_ossie_model(cfg)["datasets"][1]["fields"][1]
    assert field["datatype"] == "Opaque"
    assert field["custom_extensions"] == [
        {"vendor_name": "provisa", "data": json.dumps({"data_type": "hstore"})}
    ]


def test_a_column_with_no_type_recorded_states_no_datatype():
    cfg = _config()
    cfg.tables[1].columns[1].data_type = None
    field = build_ossie_model(cfg)["datasets"][1]["fields"][1]
    assert "datatype" not in field and "custom_extensions" not in field


def test_export_modeling_role_custom_extension():  # REQ-1320
    orders, customers = build_ossie_model(_config())["datasets"]
    exts = orders["custom_extensions"]
    assert len(exts) == 1
    assert exts[0]["vendor_name"] == "provisa"
    assert json.loads(exts[0]["data"]) == {
        "modeling_role": "fact",
        "modeling_history": "snapshot",
    }
    assert "custom_extensions" not in customers  # no role set → no slot


def test_export_relationship_shape():
    assert build_ossie_model(_config())["relationships"] == [
        {
            "name": "PLACED_BY",  # alias wins over id
            "from": "orders",
            "to": "customers",
            "from_columns": ["customer_id"],
            "to_columns": ["id"],
        }
    ]


def test_a_relationship_over_several_columns_lists_each_column():
    cfg = _config()
    cfg.relationships[0].source_column = "customer_id, ordered_at"
    cfg.relationships[0].target_column = "id,name"
    (rel,) = build_ossie_model(cfg)["relationships"]
    assert (rel["from_columns"], rel["to_columns"]) == (
        ["customer_id", "ordered_at"],
        ["id", "name"],
    )


def test_export_metric():
    assert build_ossie_model(_config())["metrics"] == [
        {
            "name": "total_revenue",
            "expression": {
                "dialects": [{"dialect": "ANSI_SQL", "expression": "SUM(orders.amount)"}]
            },
            "datatype": "Decimal",  # the metric's own type, numeric
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
    # Ossie's Decimal carries no precision, so none is proposed.
    assert (cols["id"]["datatype"], cols["amount"]["datatype"]) == ("bigint", "decimal")
    assert cols["ordered_at"]["datatype"] == "timestamp"
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
            "datatype": "decimal",
            "description": "Total order revenue",
            "ai_context": "Sum of all order amounts; the headline revenue figure.",
        }
    ]


def test_an_opaque_type_comes_back_as_the_type_the_provisa_slot_names():
    cfg = _config()
    cfg.tables[1].columns[1].data_type = "hstore"
    imp = parse_ossie_model(build_ossie_model(cfg))
    assert imp.tables[1]["columns"][1]["datatype"] == "hstore"


def test_an_opaque_type_from_another_tool_proposes_no_type():
    doc = build_ossie_model(_config())
    doc["datasets"][1]["fields"][1]["datatype"] = "Opaque"
    assert parse_ossie_model(doc).tables[1]["columns"][1]["datatype"] is None


def test_a_datatype_outside_ossies_vocabulary_is_refused_by_name_and_path():
    doc = build_ossie_model(_config())
    doc["datasets"][0]["fields"][2]["datatype"] = "number"
    with pytest.raises(ValueError, match=r"\$\.datasets\[0\]\.fields\[2\]\.datatype is 'number'"):
        parse_ossie_model(doc)


def test_the_wrapped_shape_is_refused_saying_it_predates_the_flat_one():
    doc = build_ossie_model(_config())
    wrapped = {"version": doc["version"], "semantic_model": [doc]}
    with pytest.raises(ValueError, match="predates Ossie's flat document shape"):
        parse_ossie_model(wrapped)


def test_another_version_is_refused_by_name():
    doc = build_ossie_model(_config())
    doc["version"] = "0.1.1"
    with pytest.raises(
        ValueError, match=r"\$\.version is '0\.1\.1'; this reads Ossie 0\.2\.0\.dev0"
    ):
        parse_ossie_model(doc)


def test_a_document_without_a_name_or_datasets_names_what_is_missing():
    with pytest.raises(ValueError, match=r"missing \$\.name"):
        parse_ossie_model({"version": "0.2.0.dev0"})
    with pytest.raises(ValueError, match=r"missing \$\.datasets"):
        parse_ossie_model({"version": "0.2.0.dev0", "name": "m"})
    with pytest.raises(ValueError, match="at least one dataset"):
        parse_ossie_model({"version": "0.2.0.dev0", "name": "m", "datasets": []})


def test_an_ai_context_given_as_an_object_is_read_by_its_instructions():
    doc = build_ossie_model(_config())
    doc["metrics"][0]["ai_context"] = {"instructions": "Headline revenue", "synonyms": ["sales"]}
    assert parse_ossie_model(doc).metrics[0]["ai_context"] == "Headline revenue"


def test_a_document_another_ossie_tool_wrote_is_read():
    """The flat shape with members Provisa does not write: a label, a dimension that is not
    time, a root description, another vendor's extension, a relationship over two columns."""
    doc = {
        "version": "0.2.0.dev0",
        "name": "semantic_model",
        "description": "From a dbt semantic manifest",
        "datasets": [
            {
                "name": "orders",
                "source": "pg1.public.orders",
                "primary_key": ["id"],
                "fields": [
                    {
                        "name": "id",
                        "label": "Order",
                        "expression": {"dialects": [{"dialect": "ANSI_SQL", "expression": "id"}]},
                    },
                    {
                        "name": "amount",
                        "datatype": "Float",
                        "dimension": {"is_time": False},
                        "expression": {
                            "dialects": [{"dialect": "ANSI_SQL", "expression": "amount"}]
                        },
                    },
                ],
                "custom_extensions": [{"vendor_name": "DBT", "data": "{}"}],
            }
        ],
        "relationships": [
            {
                "name": "self",
                "from": "orders",
                "to": "orders",
                "from_columns": ["a", "b"],
                "to_columns": ["c", "d"],
            }
        ],
    }
    assert _schema_problems(doc) == []
    imp = parse_ossie_model(doc)
    assert imp.model_name == "semantic_model"
    assert [(c["name"], c["datatype"]) for c in imp.tables[0]["columns"]] == [
        ("id", None),
        ("amount", "double"),
    ]
    assert "modeling_role" not in imp.tables[0]
    assert imp.relationships[0]["from_columns"] == ["a", "b"]
    assert imp.metrics == []


def test_parse_dataset_without_source_names_path():
    doc = build_ossie_model(_config())
    del doc["datasets"][1]["source"]
    with pytest.raises(ValueError, match=r"\$\.datasets\[1\]\.source"):
        parse_ossie_model(doc)


def test_parse_metric_without_expression_names_path():
    doc = build_ossie_model(_config())
    del doc["metrics"][0]["expression"]
    with pytest.raises(ValueError, match=r"\$\.metrics\[0\]\.expression"):
        parse_ossie_model(doc)


def test_parse_non_mapping_document_rejected():
    with pytest.raises(ValueError, match="mapping"):
        parse_ossie_model(["not", "a", "doc"])
