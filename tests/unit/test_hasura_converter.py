# Copyright (c) 2026 Kenneth Stott
# Canary: 7c3d4e5f-6a7b-8901-cdef-012345678901
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for Hasura v2 metadata YAML -> Provisa config conversion."""

from __future__ import annotations

import io
import sys
import tempfile
from pathlib import Path

import pytest
import yaml

from provisa.core.models import ProvisaConfig
from provisa.hasura_v2.mapper import convert_metadata
from provisa.hasura_v2.parser import parse_metadata_dir
from provisa.import_shared.warnings import WarningCollector

pytestmark = [pytest.mark.asyncio(loop_scope="session")]


# ---------------------------------------------------------------------------
# Inline fixtures — minimal Hasura v2 metadata YAML
# ---------------------------------------------------------------------------

TABLES_YAML = """
- table:
    name: orders
    schema: public
  select_permissions:
    - role: analyst
      permission:
        columns:
          - id
          - amount
          - region
          - customer_id
          - created_at
        filter: {}
    - role: manager
      permission:
        columns: "*"
        filter:
          region:
            _eq: "us-east"
  insert_permissions:
    - role: admin
      permission:
        columns:
          - amount
          - region
          - customer_id
        check: {}
  object_relationships:
    - name: customer
      using:
        foreign_key_constraint_on: customer_id
  array_relationships: []
  configuration:
    custom_name: Order
    custom_root_fields:
      select: orders
      select_by_pk: order

- table:
    name: customers
    schema: public
  select_permissions:
    - role: analyst
      permission:
        columns:
          - id
          - name
          - email
        filter: {}
  array_relationships:
    - name: orders
      using:
        foreign_key_constraint_on:
          table:
            name: orders
            schema: public
          column: customer_id
"""

ACTIONS_YAML = """
actions:
  - name: place_order
    definition:
      kind: synchronous
      handler: https://api.example.com/place_order
      type: mutation
      arguments:
        - name: product_id
          type: Int
        - name: quantity
          type: Int
      output_type: PlaceOrderResponse
    permissions:
      - role: analyst
"""


def _build_metadata_dir(tmp_path: Path, tables_yaml: str = TABLES_YAML,
                         actions_yaml: str | None = ACTIONS_YAML) -> Path:
    """Write a minimal Hasura metadata directory to a temp directory."""
    md_dir = tmp_path / "metadata"
    md_dir.mkdir()
    (md_dir / "tables.yaml").write_text(tables_yaml, encoding="utf-8")
    if actions_yaml:
        (md_dir / "actions.yaml").write_text(actions_yaml, encoding="utf-8")
    return md_dir


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestHasuraConverter:
    async def test_converts_tracked_tables(self, tmp_path):
        """Tracked Hasura tables become Provisa table registrations."""
        md_dir = _build_metadata_dir(tmp_path)
        collector = WarningCollector()
        metadata = parse_metadata_dir(md_dir, collector)

        assert len(metadata.sources) == 1
        source = metadata.sources[0]
        table_names = [t.name for t in source.tables]
        assert "orders" in table_names
        assert "customers" in table_names

        config = convert_metadata(metadata, collector)
        prov_table_names = [t.table_name for t in config.tables]
        assert "orders" in prov_table_names
        assert "customers" in prov_table_names

    async def test_converts_relationships(self, tmp_path):
        """Object and array relationships are converted to Provisa relationships."""
        md_dir = _build_metadata_dir(tmp_path)
        collector = WarningCollector()
        metadata = parse_metadata_dir(md_dir, collector)
        config = convert_metadata(metadata, collector)

        rel_ids = [r.id for r in config.relationships]
        # object relationship: orders.customer
        assert any("customer" in rid for rid in rel_ids)
        # array relationship: customers.orders
        assert any("orders" in rid for rid in rel_ids)

        # Check cardinalities
        cardinalities = {r.id: r.cardinality for r in config.relationships}
        obj_rel = next(r for r in config.relationships if "customer" in r.id)
        arr_rel = next(r for r in config.relationships if "orders" in r.id and "customers" in r.source_table_id)
        assert obj_rel.cardinality == "many-to-one"
        assert arr_rel.cardinality == "one-to-many"

    async def test_converts_permissions(self, tmp_path):
        """select_permissions become Provisa roles with correct column visibility."""
        md_dir = _build_metadata_dir(tmp_path)
        collector = WarningCollector()
        metadata = parse_metadata_dir(md_dir, collector)
        config = convert_metadata(metadata, collector)

        role_ids = {r.id for r in config.roles}
        assert "analyst" in role_ids
        assert "manager" in role_ids
        assert "admin" in role_ids

        # analyst should have read capability
        analyst = next(r for r in config.roles if r.id == "analyst")
        assert "read" in analyst.capabilities

        # admin (has insert_permissions) should have write capability
        admin = next(r for r in config.roles if r.id == "admin")
        assert "write" in admin.capabilities

        # Columns for orders visible to analyst
        orders_table = next(t for t in config.tables if t.table_name == "orders")
        analyst_cols = [c for c in orders_table.columns if "analyst" in c.visible_to]
        analyst_col_names = {c.name for c in analyst_cols}
        assert "id" in analyst_col_names
        assert "amount" in analyst_col_names

    async def test_converts_rls_filter(self, tmp_path):
        """Hasura row-level filter on manager role becomes a Provisa RLS rule."""
        md_dir = _build_metadata_dir(tmp_path)
        collector = WarningCollector()
        metadata = parse_metadata_dir(md_dir, collector)
        config = convert_metadata(metadata, collector)

        manager_rls = [r for r in config.rls_rules if r.role_id == "manager"]
        assert len(manager_rls) == 1
        rls = manager_rls[0]
        assert "region" in rls.filter
        assert "us-east" in rls.filter

    async def test_output_is_valid_provisa_config(self, tmp_path):
        """Conversion output passes ProvisaConfig Pydantic validation."""
        md_dir = _build_metadata_dir(tmp_path)
        collector = WarningCollector()
        metadata = parse_metadata_dir(md_dir, collector)
        config = convert_metadata(metadata, collector)

        raw = config.model_dump(by_alias=True)
        validated = ProvisaConfig.model_validate(raw)
        assert len(validated.tables) == len(config.tables)
        assert len(validated.roles) == len(config.roles)

    async def test_cli_produces_yaml_output(self, tmp_path):
        """CLI run produces valid YAML output that round-trips through ProvisaConfig."""
        md_dir = _build_metadata_dir(tmp_path)

        from provisa.hasura_v2.cli import main  # noqa: PLC0415

        captured = io.StringIO()
        old_stdout = sys.stdout
        sys.stdout = captured
        try:
            rc = main([str(md_dir)])
        finally:
            sys.stdout = old_stdout

        assert rc == 0
        output_text = captured.getvalue()
        assert output_text.strip()

        loaded = yaml.safe_load(output_text)
        assert isinstance(loaded, dict)
        assert "tables" in loaded
        assert "sources" in loaded

        # Round-trip validation
        validated = ProvisaConfig.model_validate(loaded)
        assert len(validated.tables) >= 2

    async def test_converts_action_as_webhook(self, tmp_path):
        """HTTP-handler actions become Provisa webhook entries."""
        md_dir = _build_metadata_dir(tmp_path)
        collector = WarningCollector()
        metadata = parse_metadata_dir(md_dir, collector)
        config = convert_metadata(metadata, collector)

        webhook_names = [w.name for w in config.webhooks]
        assert "place_order" in webhook_names

        wh = next(w for w in config.webhooks if w.name == "place_order")
        assert wh.url == "https://api.example.com/place_order"
        assert "analyst" in wh.visible_to

    async def test_metadata_without_permissions_still_converts(self, tmp_path):
        """Tables without permissions produce empty columns list, no errors."""
        plain_tables = """
- table:
    name: products
    schema: public
"""
        md_dir = _build_metadata_dir(tmp_path, tables_yaml=plain_tables, actions_yaml=None)
        collector = WarningCollector()
        metadata = parse_metadata_dir(md_dir, collector)
        config = convert_metadata(metadata, collector)

        assert any(t.table_name == "products" for t in config.tables)
