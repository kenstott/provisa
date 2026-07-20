# Copyright (c) 2026 Kenneth Stott
# Canary: 7c3d2a90-8b44-4f18-9d05-4e6a0f4f1c95
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1143: plain-English refresh-policy summary, derived per (source, table, engine)."""

from __future__ import annotations

from provisa.core.models import Column, Source, SourceType, Table
from provisa.federation.engine import build_duckdb_engine, build_trino_engine
from provisa.federation.policy_summary import Serving, describe_refresh_policy


def _src(sid: str, type_: SourceType, **kw) -> Source:
    return Source(id=sid, type=type_, host="h", port=1, database="d", username="u", **kw)


def _tbl(sid: str, **kw) -> Table:
    return Table(
        source_id=sid,
        domain_id="dom",
        table="t",
        schema="s",
        columns=[Column(name="id", data_type="integer", visible_to=["*"], is_primary_key=True)],
        **kw,
    )


def test_load_protected_scheduled_summary():
    s = _src("pg", SourceType.postgresql, load_protected=True, off_peak_window="01:00-03:00")
    r = describe_refresh_policy(s, _tbl("pg"), build_trino_engine())
    assert r.serving is Serving.SCHEDULED
    assert "01:00–03:00 UTC" in r.text
    assert "queries never touch the source" in r.text
    assert r.warning is None


def test_scheduled_summary_lists_all_gates():
    s = _src(
        "pg",
        SourceType.postgresql,
        load_protected=True,
        off_peak_window="01:00-03:00",
        cache_ttl=3600,
        change_signal="ttl_probe",
    )
    r = describe_refresh_policy(s, _tbl("pg"), build_trino_engine())
    assert "during 01:00–03:00 UTC" in r.text
    assert "at most every 1h" in r.text
    assert "only when the source has changed" in r.text


def test_lazy_read_through_cache_summary():
    s = _src("pg", SourceType.postgresql, prefer_materialized=True, cache_ttl=300)
    r = describe_refresh_policy(s, _tbl("pg"), build_trino_engine())
    assert r.serving is Serving.CACHE
    assert "5m" in r.text and r.warning is None


def test_reachable_default_is_live():
    r = describe_refresh_policy(
        _src("pg", SourceType.postgresql), _tbl("pg"), build_trino_engine()
    )
    assert r.serving is Serving.LIVE and r.warning is None


def test_prefer_materialized_no_policy_on_reachable_warns_inert_live():
    s = _src("pg", SourceType.postgresql, prefer_materialized=True)
    r = describe_refresh_policy(s, _tbl("pg"), build_trino_engine())
    assert r.serving is Serving.LIVE
    assert r.warning is not None and "no effect" in r.warning


def test_prefer_materialized_no_policy_on_unreachable_warns_frozen():
    s = _src("api", SourceType.openapi, base_url="http://x")
    s.prefer_materialized = True
    r = describe_refresh_policy(s, _tbl("api"), build_trino_engine())
    assert r.serving is Serving.FROZEN
    assert r.warning is not None and "never refreshed" in r.warning
    assert "never refreshes" in r.text


def test_unreachable_no_prefer_inherits_global_ttl_cache():
    # openapi is not live-reachable and not prefer_materialized; with no explicit cache_ttl it still
    # refetches on the global response-cache TTL, so it is CACHE, not FROZEN (REQ-1143 accuracy fix).
    s = _src("api", SourceType.openapi, base_url="http://x")
    r = describe_refresh_policy(s, _tbl("api"), build_trino_engine(), default_ttl=300)
    assert r.serving is Serving.CACHE
    assert "5m" in r.text and r.warning is None


def test_unreachable_no_prefer_caching_disabled_is_frozen():
    s = _src("api", SourceType.openapi, base_url="http://x")
    r = describe_refresh_policy(s, _tbl("api"), build_trino_engine(), default_ttl=0)
    assert r.serving is Serving.FROZEN
    assert "caching disabled" in r.text


def test_reachability_is_engine_specific():
    # csv SCANs live on DuckDB (file_native) but is unreachable-live on a Trino build without a csv
    # connector — the SAME source's summary differs per engine (REQ-1143/REQ-826).
    s = _src("c", SourceType.csv, path="/c.csv")
    s.prefer_materialized = True
    on_duck = describe_refresh_policy(s, _tbl("c"), build_duckdb_engine())
    on_trino = describe_refresh_policy(s, _tbl("c"), build_trino_engine())
    assert on_duck.serving is Serving.LIVE  # inert flag, served live (SCAN)
    assert on_trino.serving is Serving.FROZEN  # no csv connector → not live → frozen
