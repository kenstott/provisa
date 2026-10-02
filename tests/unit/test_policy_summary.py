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
    s = _src("pg", SourceType.postgresql, replicate=0, cache_ttl=300)
    r = describe_refresh_policy(s, _tbl("pg"), build_trino_engine())
    assert r.serving is Serving.CACHE
    assert "5m" in r.text and r.warning is None


def test_reachable_default_is_live():
    r = describe_refresh_policy(_src("pg", SourceType.postgresql), _tbl("pg"), build_trino_engine())
    assert r.serving is Serving.LIVE and r.warning is None


def test_always_on_a_reachable_source_reads_the_replica_with_or_without_a_cadence():
    # REQ-826: Always is a guarantee on every engine — a source the engine could read live is
    # still served from its replica.
    s = _src("pg", SourceType.postgresql, replicate=0, change_signal="probe")
    r = describe_refresh_policy(s, _tbl("pg"), build_trino_engine())
    assert r.serving is Serving.CACHE and r.warning is None
    assert "reads come from the replica" in r.text
    assert "refreshed only when the source reports a change" in r.text


def test_never_on_a_reachable_source_is_live():
    s = _src("pg", SourceType.postgresql, replicate=-1)
    r = describe_refresh_policy(s, _tbl("pg"), build_trino_engine())
    assert r.serving is Serving.LIVE and "never replicated" in r.text


def test_never_where_the_engine_cannot_read_the_source_in_place_says_it_is_replicated():
    # Never is best effort: an API source is reached only through its replica, and the panel
    # text says so instead of repeating the setting.
    s = _src("api", SourceType.openapi, base_url="http://x", replicate=-1)
    r = describe_refresh_policy(s, _tbl("api"), build_trino_engine(), default_ttl=300)
    assert r.serving is Serving.CACHE
    assert "Never cannot apply here" in r.text and "best effort" in r.text
    assert "reads come from the replica" in r.text and "5m" in r.text


def test_a_hot_threshold_is_live_until_the_table_is_busy_then_replicated():
    s = _src("pg", SourceType.postgresql, cache_ttl=300)
    below = describe_refresh_policy(s, _tbl("pg", replicate=500), build_trino_engine())
    assert below.serving is Serving.LIVE
    assert "until it passes 500 governed statements per interval" in below.text
    assert "best effort" in below.text
    past = describe_refresh_policy(
        s, _tbl("pg", replicate=500), build_trino_engine(), promoted=True
    )
    assert past.serving is Serving.CACHE and "reads come from the replica" in past.text


def test_a_table_value_wins_over_its_sources():
    s = _src("pg", SourceType.postgresql, replicate=500, cache_ttl=300)
    r = describe_refresh_policy(s, _tbl("pg", replicate=0), build_trino_engine())
    assert r.serving is Serving.CACHE and "5m" in r.text


def test_always_on_a_source_the_engine_only_replicates_reads_the_replica():
    s = _src("api", SourceType.openapi, base_url="http://x", cache_ttl=600)
    s.replicate = 0
    r = describe_refresh_policy(s, _tbl("api"), build_trino_engine())
    assert r.serving is Serving.CACHE
    assert "reads come from the replica" in r.text and "10m" in r.text


def test_unreachable_no_prefer_inherits_global_ttl_cache():
    # openapi is not live-reachable and not replicate; with no explicit cache_ttl it still
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
    # airport (DuckDB's own airport community extension, REQ-899) attaches live on DuckDB but has
    # no Trino connector at all (still materializable there, so FROZEN rather than raising) — csv
    # no longer fits this case since Trino gained a real csv connector (matching parquet/
    # delta_lake/iceberg's Hive-metastore-backed lake connectors).
    s = _src("c", SourceType.airport, path="/c.csv")
    on_duck = describe_refresh_policy(s, _tbl("c"), build_duckdb_engine(), default_ttl=0)
    on_trino = describe_refresh_policy(s, _tbl("c"), build_trino_engine(), default_ttl=0)
    assert on_duck.serving is Serving.LIVE  # read in place (ATTACH)
    assert on_trino.serving is Serving.FROZEN  # no airport connector → not live → a snapshot
