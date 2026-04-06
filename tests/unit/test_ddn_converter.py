# Copyright (c) 2026 Kenneth Stott
# Canary: 8d4e5f6a-7b8c-9012-def0-123456789012
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for Hasura DDN HML -> Provisa config conversion."""

from __future__ import annotations

import io
import sys
import tempfile
from pathlib import Path

import pytest
import yaml

from provisa.core.models import ProvisaConfig
from provisa.ddn.mapper import convert_hml
from provisa.ddn.models import (
    DDNAggregateExpression,
    DDNFieldMapping,
    DDNMetadata,
    DDNModel,
    DDNObjectType,
    DDNRelationship,
    DDNTypeMapping,
    DDNTypePermission,
)
from provisa.ddn.parser import parse_hml_dir
from provisa.import_shared.warnings import WarningCollector

pytestmark = [pytest.mark.asyncio(loop_scope="session")]


# ---------------------------------------------------------------------------
# Inline HML fixture content
# ---------------------------------------------------------------------------

_ORDER_HML = """
kind: ObjectType
definition:
  name: Order
  fields:
    - name: id
      type: Int
    - name: customerId
      type: Int
    - name: totalAmount
      type: Float
    - name: region
      type: String
  dataConnectorTypeMapping:
    - dataConnectorName: my_pg
      dataConnectorObjectType: orders
      fieldMapping:
        id:
          column:
            name: id
        customerId:
          column:
            name: customer_id
        totalAmount:
          column:
            name: total_amount
        region:
          column:
            name: region
---
kind: Model
definition:
  name: Orders
  objectType: Order
  source:
    dataConnectorName: my_pg
    collection: orders
  graphql:
    selectMany:
      queryRootField: orders
    selectUniques:
      - queryRootField: order
        uniqueIdentifier:
          - id
  aggregateExpression: OrderAgg
---
kind: TypePermissions
definition:
  typeName: Order
  permissions:
    - role: analyst
      output:
        allowedFields:
          - id
          - customerId
          - totalAmount
    - role: manager
      output:
        allowedFields:
          - id
          - customerId
          - totalAmount
          - region
---
kind: AggregateExpression
definition:
  name: OrderAgg
  operand:
    object:
      aggregatedType: Order
      aggregatableFields:
        - fieldName: totalAmount
          enableAggregationFunctions:
            - name: sum
            - name: avg
  count:
    enable: true
    enableDistinct: true
"""

_CUSTOMER_HML = """
kind: ObjectType
definition:
  name: Customer
  fields:
    - name: id
      type: Int
    - name: name
      type: String
    - name: email
      type: String
  dataConnectorTypeMapping:
    - dataConnectorName: my_pg
      dataConnectorObjectType: customers
      fieldMapping:
        id:
          column:
            name: id
        name:
          column:
            name: name
        email:
          column:
            name: email
---
kind: Model
definition:
  name: Customers
  objectType: Customer
  source:
    dataConnectorName: my_pg
    collection: customers
  graphql:
    selectMany:
      queryRootField: customers
---
kind: Relationship
definition:
  name: orders
  sourceType: Customer
  target:
    model:
      name: Orders
      relationshipType: Array
  mapping:
    - source:
        fieldPath:
          - id
      target:
        fieldPath:
          - customerId
"""

_CONNECTOR_HML = """
kind: DataConnectorLink
definition:
  name: my_pg
  url:
    singleUrl:
      value: http://localhost:8100
"""


def _build_ddn_dir(tmp_path: Path) -> Path:
    """Write a minimal DDN HML project layout to a temp directory."""
    hml_dir = tmp_path / "ddn_project"
    sg_dir = hml_dir / "app" / "metadata"
    sg_dir.mkdir(parents=True)

    (sg_dir / "orders.hml").write_text(_ORDER_HML, encoding="utf-8")
    (sg_dir / "customers.hml").write_text(_CUSTOMER_HML, encoding="utf-8")
    (sg_dir / "connector.hml").write_text(_CONNECTOR_HML, encoding="utf-8")
    return hml_dir


# ---------------------------------------------------------------------------
# Programmatic metadata builders
# ---------------------------------------------------------------------------

def _minimal_metadata() -> DDNMetadata:
    """Build a DDNMetadata object in-memory without touching the filesystem."""
    meta = DDNMetadata()

    ot = DDNObjectType(
        name="Order",
        subgraph="app",
        fields={"id": "Int", "customerId": "Int", "totalAmount": "Float"},
        type_mappings=[
            DDNTypeMapping(
                connector_name="my_pg",
                source_type="orders",
                field_mappings=[
                    DDNFieldMapping(graphql_field="customerId", column="customer_id"),
                    DDNFieldMapping(graphql_field="totalAmount", column="total_amount"),
                    DDNFieldMapping(graphql_field="id", column="id")],
            )
        ],
    )
    meta.object_types.append(ot)

    model = DDNModel(
        name="Orders",
        subgraph="app",
        object_type="Order",
        connector_name="my_pg",
        collection="orders",
        aggregate_expression="OrderAgg",
    )
    meta.models.append(model)

    meta.type_permissions.append(
        DDNTypePermission(
            type_name="Order",
            subgraph="app",
            role="analyst",
            allowed_fields=["id", "totalAmount"],
        )
    )

    agg = DDNAggregateExpression(
        name="OrderAgg",
        subgraph="app",
        operand_type="Order",
        count_enabled=True,
        count_distinct=True,
        aggregatable_fields={"totalAmount": ["sum", "avg"]},
    )
    meta.aggregate_expressions.append(agg)
    meta.subgraphs.add("app")
    return meta


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestDDNConverter:
    async def test_converts_models_to_tables(self, tmp_path):
        """DDN Model definitions become Provisa table registrations."""
        hml_dir = _build_ddn_dir(tmp_path)
        collector = WarningCollector()
        metadata = parse_hml_dir(hml_dir, collector)
        config = convert_hml(metadata, collector)

        table_names = [t.table_name for t in config.tables]
        assert "orders" in table_names
        assert "customers" in table_names

    async def test_converts_relationships_to_provisa(self, tmp_path):
        """DDN Relationship becomes a Provisa relationship with correct cardinality."""
        hml_dir = _build_ddn_dir(tmp_path)
        collector = WarningCollector()
        metadata = parse_hml_dir(hml_dir, collector)
        config = convert_hml(metadata, collector)

        assert len(config.relationships) >= 1
        rel = config.relationships[0]
        assert "customers" in rel.source_table_id
        assert "orders" in rel.target_table_id
        assert rel.cardinality == "one-to-many"

    async def test_field_mapping_resolved(self):
        """DDN field mapping resolves GraphQL field names to physical column names."""
        meta = _minimal_metadata()
        collector = WarningCollector()
        config = convert_hml(meta, collector)

        orders_table = next(t for t in config.tables if t.table_name == "orders")
        col_names = {c.name for c in orders_table.columns}
        # Physical columns should appear (not GraphQL aliases)
        assert "customer_id" in col_names
        assert "total_amount" in col_names
        # id has identity mapping
        assert "id" in col_names
        # GraphQL alias stored on column
        customer_col = next(c for c in orders_table.columns if c.name == "customer_id")
        assert customer_col.alias == "customerId"

    async def test_aggregate_preserved(self):
        """DDN aggregate annotations are preserved in table description."""
        meta = _minimal_metadata()
        collector = WarningCollector()
        config = convert_hml(meta, collector)

        orders_table = next(t for t in config.tables if t.table_name == "orders")
        assert orders_table.description is not None
        desc = orders_table.description
        assert "count" in desc or "aggregates" in desc

    async def test_output_valid_provisa_config(self, tmp_path):
        """Conversion output passes ProvisaConfig Pydantic validation."""
        hml_dir = _build_ddn_dir(tmp_path)
        collector = WarningCollector()
        metadata = parse_hml_dir(hml_dir, collector)
        config = convert_hml(metadata, collector)

        raw = config.model_dump(by_alias=True)
        validated = ProvisaConfig.model_validate(raw)
        assert len(validated.tables) == len(config.tables)

    async def test_cli_produces_yaml_output(self, tmp_path):
        """DDN CLI run produces valid YAML that round-trips through ProvisaConfig."""
        hml_dir = _build_ddn_dir(tmp_path)

        from provisa.ddn.cli import main  # noqa: PLC0415

        captured = io.StringIO()
        old_stdout = sys.stdout
        sys.stdout = captured
        try:
            rc = main([str(hml_dir)])
        finally:
            sys.stdout = old_stdout

        assert rc == 0
        output_text = captured.getvalue()
        assert output_text.strip()

        loaded = yaml.safe_load(output_text)
        assert isinstance(loaded, dict)
        assert "tables" in loaded

        validated = ProvisaConfig.model_validate(loaded)
        assert len(validated.tables) >= 2

    async def test_type_permissions_produce_roles(self):
        """TypePermissions create role entries in the output config."""
        meta = _minimal_metadata()
        collector = WarningCollector()
        config = convert_hml(meta, collector)

        role_ids = {r.id for r in config.roles}
        assert "analyst" in role_ids

    async def test_connector_becomes_source(self, tmp_path):
        """DataConnectorLink is parsed and becomes a Provisa source."""
        hml_dir = _build_ddn_dir(tmp_path)
        collector = WarningCollector()
        metadata = parse_hml_dir(hml_dir, collector)
        config = convert_hml(metadata, collector)

        source_ids = [s.id for s in config.sources]
        assert any("my_pg" in sid or "my_pg" == sid for sid in source_ids)
