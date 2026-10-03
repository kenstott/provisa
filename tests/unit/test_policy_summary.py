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
from provisa.federation.policy_summary import (
    HotView,
    PolicySummary,
    Serving,
    describe_refresh_policy,
)


def _describe(source, table, engine, default_ttl: int = 300, **standing) -> PolicySummary:
    """The summary under the deployment's default Hot replication settings (100 statements per
    60 s, 10,000,000 rows); ``standing`` is where the table stands (HotView's other fields)."""
    return describe_refresh_policy(
        source, table, engine, default_ttl, hot=HotView(100, 60, 10_000_000, **standing)
    )


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
    r = _describe(s, _tbl("pg"), build_trino_engine())
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
    r = _describe(s, _tbl("pg"), build_trino_engine())
    assert "during 01:00–03:00 UTC" in r.text
    assert "at most every 1h" in r.text
    assert "only when the source has changed" in r.text


def test_lazy_read_through_cache_summary():
    s = _src("pg", SourceType.postgresql, replicate=0, cache_ttl=300)
    r = _describe(s, _tbl("pg"), build_trino_engine())
    assert r.serving is Serving.CACHE
    assert "5m" in r.text and r.warning is None


def test_reachable_default_is_live():
    r = _describe(_src("pg", SourceType.postgresql), _tbl("pg"), build_trino_engine())
    assert r.serving is Serving.LIVE and r.warning is None


def test_always_on_a_reachable_source_reads_the_replica_with_or_without_a_cadence():
    # REQ-826: Always is a guarantee on every engine — a source the engine could read live is
    # still served from its replica.
    s = _src("pg", SourceType.postgresql, replicate=0, change_signal="probe")
    r = _describe(s, _tbl("pg"), build_trino_engine())
    assert r.serving is Serving.CACHE and r.warning is None
    assert "reads come from the replica" in r.text
    assert "refreshed only when the source reports a change" in r.text


def test_never_on_a_reachable_source_is_live():
    s = _src("pg", SourceType.postgresql, replicate=-1)
    r = _describe(s, _tbl("pg"), build_trino_engine())
    assert r.serving is Serving.LIVE and "never replicated" in r.text


def test_never_where_the_engine_cannot_read_the_source_in_place_says_it_is_replicated():
    # Never is best effort: an API source is reached only through its replica, and the panel
    # text says so instead of repeating the setting.
    s = _src("api", SourceType.openapi, base_url="http://x", replicate=-1)
    r = _describe(s, _tbl("api"), build_trino_engine(), default_ttl=300)
    assert r.serving is Serving.CACHE
    assert "Never cannot apply here" in r.text and "best effort" in r.text
    assert "reads come from the replica" in r.text and "5m" in r.text


def test_a_hot_threshold_shows_both_numbers_while_live():
    s = _src("pg", SourceType.postgresql, cache_ttl=300)
    below = _describe(s, _tbl("pg", replicate=500), build_trino_engine())
    assert below.serving is Serving.LIVE and below.warning is None
    assert below.text == (
        "Live — read directly from the source. Replicated once it passes 500 governed "
        "statements per 1m (best effort), and back to live below 250."
    )


def test_a_default_table_is_judged_against_the_global_threshold():
    s = _src("pg", SourceType.postgresql, cache_ttl=300)
    r = _describe(s, _tbl("pg"), build_trino_engine())
    assert r.serving is Serving.LIVE
    assert "Replicated once it passes 100 governed statements per 1m" in r.text
    assert "back to live below 50" in r.text


def test_a_promoted_table_is_live_while_its_replica_is_built_then_replicated():
    s = _src("pg", SourceType.postgresql, cache_ttl=300)
    building = _describe(s, _tbl("pg", replicate=500), build_trino_engine(), promoted=True)
    assert building.serving is Serving.LIVE
    assert building.text == (
        "Live — it passed 500 governed statements per 1m and its replica is being built; reads "
        "stay live until the replica exists."
    )
    served = _describe(
        s, _tbl("pg", replicate=500), build_trino_engine(), promoted=True, serving=True
    )
    assert served.serving is Serving.CACHE
    assert "reads come from the replica" in served.text
    assert "It passed 500 governed statements per 1m; it returns to live below 250." in served.text


def test_embedded_redis_with_several_workers_says_the_table_stays_live():
    s = _src("pg", SourceType.postgresql, cache_ttl=300)
    said = (
        "Hot promotion needs shared Redis when more than one worker serves requests; this "
        "table stays live."
    )
    default = _describe(s, _tbl("pg"), build_trino_engine(), runs=False)
    assert default.serving is Serving.LIVE and said in default.text
    assert default.warning is None  # nothing the operator set is without effect
    chosen = _describe(s, _tbl("pg", replicate=500), build_trino_engine(), runs=False)
    assert said in chosen.text and chosen.warning == said  # the Hot-500 they chose has no effect


def test_a_table_with_no_replication_clock_says_why_it_is_not_replicated_when_busy():
    from provisa.federation.replica_hot import NO_CLOCK

    s = _src("pg", SourceType.postgresql)
    r = _describe(s, _tbl("pg"), build_trino_engine(), skipped=NO_CLOCK)
    assert r.serving is Serving.LIVE
    assert "neither it nor its source declares a Cache TTL" in r.text


def test_a_table_over_the_size_ceiling_says_why_it_is_not_replicated_when_busy():
    from provisa.federation.replica_hot import TOO_LARGE

    s = _src("pg", SourceType.postgresql, cache_ttl=300)
    r = _describe(s, _tbl("pg", replicate=500), build_trino_engine(), skipped=TOO_LARGE)
    assert r.serving is Serving.LIVE
    assert "it holds more than 10000000 rows (replication.hot_max_rows)" in r.text
    assert r.warning is not None


def test_a_table_the_hot_tier_manages_says_it_is_not_also_replicated_when_busy():
    from provisa.federation.replica_hot import HOT_TIER

    s = _src("pg", SourceType.postgresql, cache_ttl=300)
    r = _describe(s, _tbl("pg"), build_trino_engine(), skipped=HOT_TIER)
    assert r.serving is Serving.LIVE
    assert "the hot tier keeps it in Redis, and a table lives in one tier" in r.text


def test_a_table_with_no_whole_copy_says_why_it_is_not_replicated_when_busy():
    from provisa.federation.replica_hot import NOT_WHOLE

    s = _src("pg", SourceType.postgresql, cache_ttl=300)
    r = _describe(s, _tbl("pg"), build_trino_engine(), skipped=NOT_WHOLE)
    assert r.serving is Serving.LIVE
    assert "it is read by its parameters or row by row, so it has no whole copy" in r.text


def test_an_odd_threshold_states_its_half_exactly():
    s = _src("pg", SourceType.postgresql, cache_ttl=300)
    r = _describe(s, _tbl("pg", replicate=25), build_trino_engine())
    assert "back to live below 12.5" in r.text


def test_a_table_value_wins_over_its_sources():
    s = _src("pg", SourceType.postgresql, replicate=500, cache_ttl=300)
    r = _describe(s, _tbl("pg", replicate=0), build_trino_engine())
    assert r.serving is Serving.CACHE and "5m" in r.text


def test_always_on_a_source_the_engine_only_replicates_reads_the_replica():
    s = _src("api", SourceType.openapi, base_url="http://x", cache_ttl=600)
    s.replicate = 0
    r = _describe(s, _tbl("api"), build_trino_engine())
    assert r.serving is Serving.CACHE
    assert "reads come from the replica" in r.text and "10m" in r.text


def test_unreachable_no_prefer_inherits_global_ttl_cache():
    # openapi is not live-reachable and not replicate; with no explicit cache_ttl it still
    # refetches on the global response-cache TTL, so it is CACHE, not FROZEN (REQ-1143 accuracy fix).
    s = _src("api", SourceType.openapi, base_url="http://x")
    r = _describe(s, _tbl("api"), build_trino_engine(), default_ttl=300)
    assert r.serving is Serving.CACHE
    assert "5m" in r.text and r.warning is None


def test_unreachable_no_prefer_caching_disabled_is_frozen():
    s = _src("api", SourceType.openapi, base_url="http://x")
    r = _describe(s, _tbl("api"), build_trino_engine(), default_ttl=0)
    assert r.serving is Serving.FROZEN
    assert "caching disabled" in r.text


def test_reachability_is_engine_specific():
    # airport (DuckDB's own airport community extension, REQ-899) attaches live on DuckDB but has
    # no Trino connector at all (still materializable there, so FROZEN rather than raising) — csv
    # no longer fits this case since Trino gained a real csv connector (matching parquet/
    # delta_lake/iceberg's Hive-metastore-backed lake connectors).
    s = _src("c", SourceType.airport, path="/c.csv")
    on_duck = _describe(s, _tbl("c"), build_duckdb_engine(), default_ttl=0)
    on_trino = _describe(s, _tbl("c"), build_trino_engine(), default_ttl=0)
    assert on_duck.serving is Serving.LIVE  # read in place (ATTACH)
    assert on_trino.serving is Serving.FROZEN  # no airport connector → not live → a snapshot
