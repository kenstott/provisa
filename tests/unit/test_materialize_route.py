# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1194/REQ-1195: the ONE IR-level materialize/redirect route.

The redirect/materialize decision is a directive on the governed plan, executed by the single
_execute_plan terminal, so every transport inherits it. These tests pin the terminal behaviour and
the sink-tier selection rule without a live engine or object store.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from provisa.executor.redirect import Delivery, RedirectConfig, run_materialize


def _cfg(endpoint_url: str = "http://minio:9000") -> RedirectConfig:
    return RedirectConfig(
        enabled=True,
        threshold=1000,
        bucket="provisa-results",
        endpoint_url=endpoint_url,
        access_key="k",
        secret_key="s",
        ttl=3600,
    )


class TestExecutePlanMaterializeTerminal:
    """A stamped plan carrying a materialize directive runs the sink terminal, not a row terminal."""

    @pytest.mark.asyncio
    async def test_materialize_branch_precedes_route_dispatch(self, monkeypatch):
        from provisa.pgwire._pipeline import _Plan, _execute_plan, _mint_stamp
        from provisa.transpiler.router import Route

        handle = {"sink": "object-store", "redirect_url": "http://x/f.parquet", "row_count": 42}

        async def _fake_run_materialize(state, physical_sql, delivery):
            assert physical_sql == "SELECT 1"
            assert isinstance(delivery, Delivery)
            return handle

        monkeypatch.setattr(_pipeline_redirect_module(), "run_materialize", _fake_run_materialize)

        # Route is DIRECT — materialize must still win, proving the terminal is route-independent.
        plan = _Plan(
            route=Route.DIRECT,
            sql="SELECT 1",
            source_id="s",
            dialect="postgres",
            physical_sql="SELECT 1",
            materialize=Delivery(output_format="parquet", config=_cfg()),
            stamp=_mint_stamp(),
        )
        state = MagicMock()
        result = await _execute_plan(plan, state)
        assert result.redirect == handle
        assert result.rows == []
        state.federation_engine.execute_native.assert_not_called()

    @pytest.mark.asyncio
    async def test_ungoverned_materialize_plan_rejected(self):
        from provisa.pgwire._pipeline import _Plan, _execute_plan
        from provisa.transpiler.router import Route

        plan = _Plan(
            route=Route.ENGINE,
            sql="SELECT 1",
            source_id="s",
            dialect="postgres",
            physical_sql="SELECT 1",
            materialize=Delivery(output_format="parquet", config=_cfg()),
            stamp=None,
        )
        with pytest.raises(PermissionError):
            await _execute_plan(plan, MagicMock())


class TestSinkTierSelection:
    """REQ-1195: object-store tier when the engine can write the format natively to a store."""

    @pytest.mark.asyncio
    async def test_object_store_tier(self, monkeypatch):
        import provisa.executor.redirect as rd

        async def _fake_presign(prefix, config):
            return f"http://minio/{prefix}/f.parquet"

        async def _noop_cleanup(prefix, config, delay_seconds=None):
            return None

        monkeypatch.setattr(rd, "presign_ctas_result", _fake_presign)
        monkeypatch.setattr(rd, "schedule_s3_cleanup", _noop_cleanup)

        state = MagicMock()
        state.engine_conn = object()
        state.federation_engine.ctas_redirect.return_value = {
            "table_name": "r_abc",
            "s3_prefix": "s3a://provisa-results/results/abc",
            "row_count": 7,
        }
        handle = await run_materialize(
            state, "SELECT 1", Delivery(output_format="parquet", config=_cfg())
        )
        assert handle["sink"] == "object-store"
        assert handle["row_count"] == 7
        assert handle["redirect_url"].endswith("f.parquet")
        assert handle["content_type"] == "application/vnd.apache.parquet"

    @pytest.mark.asyncio
    async def test_no_engine_uses_local_tier(self):
        # No connected engine → engine can't CTAS natively → local/HTTP tier (follow-on, REQ-1191).
        state = MagicMock()
        state.engine_conn = None
        with pytest.raises(NotImplementedError):
            await run_materialize(
                state, "SELECT 1", Delivery(output_format="parquet", config=_cfg())
            )

    @pytest.mark.asyncio
    async def test_non_native_format_uses_local_tier(self):
        # CSV is not engine-native → falls to the local tier regardless of a connected engine.
        state = MagicMock()
        state.engine_conn = object()
        with pytest.raises(NotImplementedError):
            await run_materialize(
                state, "SELECT 1", Delivery(output_format="csv", config=_cfg())
            )


def _pipeline_redirect_module():
    # _execute_plan imports run_materialize from provisa.executor.redirect at call time; patch there.
    import provisa.executor.redirect as rd

    return rd
