# Copyright (c) 2026 Kenneth Stott
# Canary: ce341f4e-0d36-4279-aec3-075183c061ca
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""BDD steps for REQ-814 — provider selection dispatches on live.strategy.

Exercises the real dispatch path (_resolve_provider_type + get_provider). Only
the tenant_db object is a stand-in; provider *selection* is production logic.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from pytest_bdd import given, scenarios, then, when

from provisa.subscriptions.pg_provider import PgNotificationProvider

scenarios("../features/REQ-814.feature")


@given("a PostgreSQL table with live.strategy=native", target_fixture="r814")
def _r814_given() -> dict:
    return {
        "source_type": "postgresql",
        "source_id": "pg1",
        "tbl_meta": SimpleNamespace(live=SimpleNamespace(strategy="native")),
        # REQ-931: legacy strategy resolution reads state.config.sources for the change_signal.
        "state": SimpleNamespace(
            cdc_sources={}, tenant_db=MagicMock(), config=SimpleNamespace(sources=[])
        ),
    }


@when("get_provider() is called")
def _r814_when(r814: dict) -> None:
    from provisa.api.data.subscribe import _build_provider_config, _resolve_provider_type
    from provisa.subscriptions.registry import get_provider

    stype, sid = r814["source_type"], r814["source_id"]
    meta, state = r814["tbl_meta"], r814["state"]
    ptype = _resolve_provider_type(stype, sid, meta, state)
    r814["provider_type"] = ptype
    r814["provider"] = get_provider(
        ptype, _build_provider_config(ptype, sid, "orders", meta, state)
    )
    # Same source_type, strategy=debezium -> different provider, proving strategy drives dispatch.
    meta_deb = SimpleNamespace(live=SimpleNamespace(strategy="debezium"))
    r814["debezium_type"] = _resolve_provider_type(stype, sid, meta_deb, state)


@then("PgNotificationProvider is instantiated")
def _r814_then_pg(r814: dict) -> None:
    assert isinstance(r814["provider"], PgNotificationProvider)


@then("the source_type is not used to dispatch")
def _r814_then_strategy_driven(r814: dict) -> None:
    assert r814["provider_type"] == "postgresql"
    assert r814["debezium_type"] == "debezium"
