# Copyright (c) 2026 Kenneth Stott
# Canary: 2b7e5c9a-4f1d-4e83-a6b2-9d0c3e7f1a58
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The native column reader types a source only when the engine has no live connector for it."""

# Requirements: REQ-840, REQ-1672

from types import SimpleNamespace

from provisa.api.admin.schema_query import _engine_reaches_live
from provisa.federation.connector_base import Mechanism


def _conn(mechanism: Mechanism) -> SimpleNamespace:
    return SimpleNamespace(mechanism=mechanism)


def _state(connectors: dict) -> SimpleNamespace:
    return SimpleNamespace(
        federation_engine=SimpleNamespace(engine=SimpleNamespace(connectors=connectors))
    )


def test_a_type_with_a_live_connector_is_reached_live():
    assert _engine_reaches_live(
        _state({"elasticsearch": _conn(Mechanism.ATTACH_RW)}), "elasticsearch"
    )
    assert _engine_reaches_live(_state({"hive_s3": _conn(Mechanism.SCAN)}), "hive_s3")


def test_a_type_without_a_connector_is_not_reached_live():
    assert not _engine_reaches_live(
        _state({"postgresql": _conn(Mechanism.ATTACH_RW)}), "elasticsearch"
    )


def test_a_type_the_engine_only_lands_is_not_reached_live():
    """complete_reach gives a native engine a FETCH land connector for every type it does not
    attach, so membership alone would switch the REQ-1672 native reader off on DuckDB."""
    assert not _engine_reaches_live(
        _state({"elasticsearch": _conn(Mechanism.FETCH)}), "elasticsearch"
    )


def test_the_real_engines_agree():
    """Trino attaches elasticsearch live; DuckDB only lands it."""
    import os

    from provisa.federation.engine import build_engine

    prior = os.environ.get("PROVISA_ENGINE")
    try:
        os.environ["PROVISA_ENGINE"] = "trino"
        assert _engine_reaches_live(
            SimpleNamespace(federation_engine=SimpleNamespace(engine=build_engine())),
            "elasticsearch",
        )
        os.environ["PROVISA_ENGINE"] = "duckdb"
        assert not _engine_reaches_live(
            SimpleNamespace(federation_engine=SimpleNamespace(engine=build_engine())),
            "elasticsearch",
        )
    finally:
        if prior is None:
            os.environ.pop("PROVISA_ENGINE", None)
        else:
            os.environ["PROVISA_ENGINE"] = prior


async def test_hive_s3_is_listed_through_an_engine_that_reads_it_live(monkeypatch):
    """REQ-1672/REQ-1730: hive_s3's native lister dials the S3 endpoint the source declares, the
    engine-visible address (http://minio:9000 in the Trino lane), which the app cannot reach. Under
    Trino the schema picker came back empty; its schemas are listed through the engine's catalog."""
    from provisa.api.admin.schema_query import _engine_lists

    async def _attached(_state, _source_id):
        return None

    monkeypatch.setattr("provisa.api.admin.introspect.unattached_source", _attached)
    trino = _state({"hive_s3": _conn(Mechanism.SCAN)})
    assert await _engine_lists(trino, "lake", "hive_s3")
    duckdb = _state({"hive_s3": _conn(Mechanism.FETCH)})
    assert not await _engine_lists(duckdb, "lake", "hive_s3")
    assert not await _engine_lists(_state({"pinot": _conn(Mechanism.SCAN)}), "olap", "pinot")


async def test_a_floored_hive_s3_source_is_listed_by_the_app(monkeypatch):
    from provisa.api.admin.schema_query import _engine_lists

    async def _floored(_state, source_id):
        return SimpleNamespace(id=source_id)

    monkeypatch.setattr("provisa.api.admin.introspect.unattached_source", _floored)
    assert not await _engine_lists(_state({"hive_s3": _conn(Mechanism.SCAN)}), "lake", "hive_s3")
