# Copyright (c) 2026 Kenneth Stott
# Canary: e66bec32-27a9-498a-a10e-f9ba89ab99be
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Unit tests for per-table live delivery config: validation + DB reconcile.

Covers REQ-565/813/814 — ``strategy`` selects the delta-capture mechanism
(poll|native|debezium|kafka), capability-gated by source type; poll routes
through Trino; debezium/kafka inherit transport from the source cdc block; and
admin-persisted live config drives the engine via reconcile.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from provisa.core.config_loader import _validate_table_live_delivery


def _cfg(
    *,
    strategy,
    watermark_column=None,
    source_type="postgresql",
    kafka=None,
    source_cdc=None,
):
    live = SimpleNamespace(strategy=strategy, watermark_column=watermark_column, kafka=kafka)
    table = SimpleNamespace(table_name="orders", source_id="s1", live=live)
    source = SimpleNamespace(id="s1", type=source_type, cdc=source_cdc)
    return SimpleNamespace(tables=[table], sources=[source])


class TestValidateLiveDelivery:
    def test_poll_without_watermark_raises(self):
        with pytest.raises(ValueError, match="live.strategy=poll requires watermark_column"):
            _validate_table_live_delivery(_cfg(strategy="poll", watermark_column=None))

    def test_poll_with_watermark_ok(self):
        # poll is allowed on any federated SQL source.
        _validate_table_live_delivery(
            _cfg(strategy="poll", watermark_column="updated_at", source_type="snowflake")
        )

    def test_native_on_postgres_ok(self):
        _validate_table_live_delivery(_cfg(strategy="native", source_type="postgresql"))

    def test_native_on_mongodb_ok(self):
        _validate_table_live_delivery(_cfg(strategy="native", source_type="mongodb"))

    def test_debezium_on_postgres_with_cdc_ok(self):
        _validate_table_live_delivery(
            _cfg(strategy="debezium", source_type="postgresql", source_cdc=object())
        )

    def test_debezium_on_postgres_without_cdc_raises(self):
        with pytest.raises(ValueError, match="requires source-level cdc transport"):
            _validate_table_live_delivery(
                _cfg(strategy="debezium", source_type="postgresql", source_cdc=None)
            )

    def test_kafka_without_params_raises(self):
        with pytest.raises(ValueError, match="requires a kafka params block"):
            _validate_table_live_delivery(
                _cfg(strategy="kafka", source_type="postgresql", kafka=None, source_cdc=object())
            )

    @pytest.mark.parametrize("stype", ["snowflake", "sqlite", "mongodb"])
    def test_debezium_unsupported_source_types_raise(self, stype):
        with pytest.raises(ValueError, match="not supported for source type"):
            _validate_table_live_delivery(_cfg(strategy="debezium", source_type=stype))

    def test_native_unsupported_on_rdbms_raises(self):
        # A non-PG RDBMS has no native push mechanism → only poll/debezium/kafka.
        with pytest.raises(ValueError, match="not supported for source type"):
            _validate_table_live_delivery(_cfg(strategy="native", source_type="mysql"))


class TestReconcileLiveEngine:
    @pytest.mark.asyncio
    async def test_builds_trino_qualified_poll_specs(self):
        from provisa.api import app as app_mod
        from provisa.api import app_rebuild

        # REQ-932: reconcile keys off change_signal (legacy live.strategy read through) + the
        # top-level watermark_column.
        rows = [
            {
                "source_id": "sales-db",
                "schema_name": "public",
                "table_name": "orders",
                "change_signal": None,  # inherit; legacy live.strategy=poll → ttl (a poll signal)
                "watermark_column": "updated_at",
                "live": {
                    "strategy": "poll",
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
            # debezium rows are handled by providers, not the poll engine → excluded
            {
                "source_id": "pg",
                "schema_name": "public",
                "table_name": "events",
                "change_signal": None,
                "watermark_column": "ts",
                "live": {"strategy": "debezium", "watermark_column": "ts"},
            },
        ]
        conn = AsyncMock()
        _result = MagicMock()
        _result.fetchall.return_value = [MagicMock(_mapping=r) for r in rows]
        conn.execute_core = AsyncMock(return_value=_result)
        engine = MagicMock()

        with patch.object(app_mod, "state", SimpleNamespace(live_engine=engine)):
            await app_rebuild._reconcile_live_engine(conn)

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
        from provisa.api import app_rebuild

        conn = AsyncMock()
        with patch.object(app_mod, "state", SimpleNamespace(live_engine=None)):
            await app_rebuild._reconcile_live_engine(conn)
        conn.fetch.assert_not_called()


class TestRepoUpsertSerializesLive:
    @pytest.mark.asyncio
    async def test_live_persisted_as_json(self):
        from provisa.core.models import Column, LiveDeliveryConfig, LiveOutputConfig, Table
        from provisa.core.repositories import table as table_repo

        live = LiveDeliveryConfig(
            query_id="s1.orders",
            watermark_column="updated_at",
            poll_interval=15,
            strategy="poll",
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
        conn.upsert_returning = AsyncMock(return_value=1)
        _empty = MagicMock()
        _empty.fetchall.return_value = []  # no pre-existing column types to preserve
        conn.execute_core = AsyncMock(return_value=_empty)

        await table_repo.upsert(conn, tbl)

        # The JSON `live` column takes the Python dict directly (SQLAlchemy serializes per dialect) —
        # no manual json.dumps. Assert the value passed to the Core upsert.
        values = conn.upsert_returning.await_args.args[1]
        assert values["live"] == live.model_dump()

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
        conn.upsert_returning = AsyncMock(return_value=1)
        _empty = MagicMock()
        _empty.fetchall.return_value = []  # no pre-existing column types to preserve
        conn.execute_core = AsyncMock(return_value=_empty)

        await table_repo.upsert(conn, tbl)

        values = conn.upsert_returning.await_args.args[1]
        assert values["live"] is None


class TestAdminLiveMapping:
    def test_input_to_model_to_row_type_roundtrip(self):
        from provisa.api.admin._live_mappers import live_model_from_input
        from provisa.api.admin._row_mappers import _live_type_from_row
        from provisa.api.admin.types import LiveDeliveryConfigInput, LiveOutputConfigInput

        inp = LiveDeliveryConfigInput(
            query_id="s1.orders",
            watermark_column="updated_at",
            poll_interval=25,
            strategy="poll",
            outputs=[
                LiveOutputConfigInput(
                    type="kafka", topic="orders", key_column="id", bootstrap_servers="k:9092"
                )
            ],
        )
        model = live_model_from_input(inp)
        assert model is not None
        # Persisted shape → GraphQL output type
        out = _live_type_from_row(model.model_dump())
        assert out is not None
        assert out.query_id == "s1.orders"
        assert out.watermark_column == "updated_at"
        assert out.poll_interval == 25
        assert out.strategy == "poll"
        assert len(out.outputs) == 1
        assert out.outputs[0].type == "kafka"
        assert out.outputs[0].bootstrap_servers == "k:9092"

    def test_none_input_and_row_map_to_none(self):
        from provisa.api.admin._live_mappers import live_model_from_input
        from provisa.api.admin._row_mappers import _live_type_from_row

        assert live_model_from_input(None) is None
        assert _live_type_from_row(None) is None
