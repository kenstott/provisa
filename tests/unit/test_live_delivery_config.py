# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Unit tests for per-table live delivery config: validation + DB reconcile.

Covers REQ-565 amendments — poll routes through Trino, CDC is restricted to
source types with a real push provider, and admin-persisted live config drives
the engine via reconcile.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from provisa.core.config_loader import _validate_table_live_delivery


def _cfg(*, delivery, watermark_column=None, source_type="postgresql"):
    live = SimpleNamespace(delivery=delivery, watermark_column=watermark_column)
    table = SimpleNamespace(table_name="orders", source_id="s1", live=live)
    source = SimpleNamespace(id="s1", type=source_type)
    return SimpleNamespace(tables=[table], sources=[source])


class TestValidateLiveDelivery:
    def test_poll_without_watermark_raises(self):
        with pytest.raises(ValueError, match="requires watermark_column"):
            _validate_table_live_delivery(_cfg(delivery="poll", watermark_column=None))

    def test_poll_with_watermark_ok(self):
        _validate_table_live_delivery(
            _cfg(delivery="poll", watermark_column="updated_at", source_type="snowflake")
        )

    @pytest.mark.parametrize("stype", ["postgresql", "debezium", "kafka", "mongodb"])
    def test_cdc_supported_source_types_ok(self, stype):
        _validate_table_live_delivery(_cfg(delivery="cdc", source_type=stype))

    @pytest.mark.parametrize("stype", ["mysql", "oracle", "snowflake", "sqlite"])
    def test_cdc_unsupported_source_types_raise(self, stype):
        with pytest.raises(ValueError, match="cdc not supported"):
            _validate_table_live_delivery(_cfg(delivery="cdc", source_type=stype))


class TestReconcileLiveEngine:
    @pytest.mark.asyncio
    async def test_builds_trino_qualified_poll_specs(self):
        from provisa.api import app as app_mod

        rows = [
            {
                "source_id": "sales-db",
                "schema_name": "public",
                "table_name": "orders",
                "live": {
                    "delivery": "poll",
                    "watermark_column": "updated_at",
                    "poll_interval": 20,
                    "outputs": [
                        {
                            "type": "kafka",
                            "topic": "orders",
                            "bootstrap_servers": "k:9092",
                            "key_column": "id",
                        }
                    ],
                },
            },
            # cdc rows are handled by providers, not the poll engine → excluded
            {
                "source_id": "pg",
                "schema_name": "public",
                "table_name": "events",
                "live": {"delivery": "cdc", "watermark_column": "ts"},
            },
        ]
        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=rows)
        engine = MagicMock()

        with patch.object(app_mod, "state", SimpleNamespace(live_engine=engine)):
            await app_mod._reconcile_live_engine(conn)

        engine.reconcile.assert_called_once()
        specs = engine.reconcile.call_args.args[0]
        assert len(specs) == 1
        spec = specs[0]
        assert spec.query_id == "sales-db.orders"
        # catalog = sanitized source id (hyphen → underscore), Trino-qualified
        assert spec.sql == 'SELECT * FROM sales_db."public"."orders"'
        assert spec.watermark_column == "updated_at"
        assert spec.poll_interval == 20
        assert spec.kafka_outputs == [
            {"bootstrap_servers": "k:9092", "topic": "orders", "key_column": "id"}
        ]

    @pytest.mark.asyncio
    async def test_no_engine_is_noop(self):
        from provisa.api import app as app_mod

        conn = AsyncMock()
        with patch.object(app_mod, "state", SimpleNamespace(live_engine=None)):
            await app_mod._reconcile_live_engine(conn)
        conn.fetch.assert_not_called()


class TestRepoUpsertSerializesLive:
    @pytest.mark.asyncio
    async def test_live_persisted_as_json(self):
        import json

        from provisa.core.models import Column, LiveDeliveryConfig, LiveOutputConfig, Table
        from provisa.core.repositories import table as table_repo

        live = LiveDeliveryConfig(
            query_id="s1.orders",
            watermark_column="updated_at",
            poll_interval=15,
            delivery="poll",
            outputs=[LiveOutputConfig(type="kafka", topic="orders", bootstrap_servers="k:9092")],
        )
        tbl = Table(
            source_id="s1",
            domain_id="default",
            schema_name="public",
            table_name="orders",
            columns=[Column(name="id", visible_to=["analyst"])],
            live=live,
        )
        conn = AsyncMock()
        conn.fetchval = AsyncMock(return_value=1)
        conn.execute = AsyncMock(return_value=None)

        await table_repo.upsert(conn, tbl)

        live_arg = conn.fetchval.call_args.args[-1]
        assert json.loads(live_arg) == live.model_dump()

    @pytest.mark.asyncio
    async def test_live_none_persists_null(self):
        from provisa.core.models import Column, Table
        from provisa.core.repositories import table as table_repo

        tbl = Table(
            source_id="s1",
            domain_id="default",
            schema_name="public",
            table_name="orders",
            columns=[Column(name="id", visible_to=["analyst"])],
            live=None,
        )
        conn = AsyncMock()
        conn.fetchval = AsyncMock(return_value=1)
        conn.execute = AsyncMock(return_value=None)

        await table_repo.upsert(conn, tbl)

        assert conn.fetchval.call_args.args[-1] is None


class TestAdminLiveMapping:
    def test_input_to_model_to_row_type_roundtrip(self):
        from provisa.api.admin import schema as admin_schema
        from provisa.api.admin.types import LiveDeliveryConfigInput, LiveOutputConfigInput

        inp = LiveDeliveryConfigInput(
            query_id="s1.orders",
            watermark_column="updated_at",
            poll_interval=25,
            delivery="poll",
            outputs=[
                LiveOutputConfigInput(
                    type="kafka", topic="orders", key_column="id", bootstrap_servers="k:9092"
                )
            ],
        )
        model = admin_schema._live_model_from_input(inp)
        assert model is not None
        # Persisted shape → GraphQL output type
        out = admin_schema._live_type_from_row(model.model_dump())
        assert out is not None
        assert out.query_id == "s1.orders"
        assert out.watermark_column == "updated_at"
        assert out.poll_interval == 25
        assert out.delivery == "poll"
        assert len(out.outputs) == 1
        assert out.outputs[0].type == "kafka"
        assert out.outputs[0].bootstrap_servers == "k:9092"

    def test_none_input_and_row_map_to_none(self):
        from provisa.api.admin import schema as admin_schema

        assert admin_schema._live_model_from_input(None) is None
        assert admin_schema._live_type_from_row(None) is None
