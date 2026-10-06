# Copyright (c) 2026 Kenneth Stott
# Canary: 80bad1f9-0171-4384-af99-607696bcbe66
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The count Hot-N replication is decided on (REQ-826): which statements count a table, the
window the count is taken over, and when a deployment decides promotion at all."""

# Requirements: REQ-826, REQ-1920

from __future__ import annotations

import uuid
from types import SimpleNamespace

import pytest

from provisa.federation.replica_hot import (
    HotCounts,
    batch_hits,
    count_scope,
    counted_tables,
    counts_toward_hot,
    promotion_runs,
)

pytestmark = pytest.mark.unit

_INTERVAL = 60


class _Clock:
    def __init__(self, at: float) -> None:
        self.at = at

    def __call__(self) -> float:
        return self.at


@pytest.fixture
def scope() -> str:
    # The embedded store is one per process: each test counts in an org of its own.
    return count_scope(f"org-{uuid.uuid4().hex}", "prod")


def _statement(scope: str, table_ids, *, route: str | None = "engine", status_code: int = 200):
    return SimpleNamespace(
        hot_scope=scope, table_ids=tuple(table_ids), route=route, status_code=status_code
    )


# -- what counts -----------------------------------------------------------------------------------


def test_a_statement_counts_a_table_once_however_many_times_it_reads_it(scope):
    # a self-join, or a table read by two fields of one request: its id is resolved twice
    assert counted_tables([7, 7, 9, 7]) == frozenset({7, 9})
    assert batch_hits([_statement(scope, [7, 7, 9])]) == {(scope, 7): 1, (scope, 9): 1}
    # two statements are two
    assert batch_hits([_statement(scope, [7]), _statement(scope, [7, 7])]) == {(scope, 7): 2}


def test_a_response_answered_from_the_response_cache_does_not_count(scope):
    """It reached no data, so it put no load on the source — what the threshold is about."""
    assert counts_toward_hot("cache", 200) is False
    assert batch_hits([_statement(scope, [7], route="cache")]) == {}
    for route in ("direct", "engine", "api"):
        assert counts_toward_hot(route, 200) is True


def test_a_statement_governance_refused_does_not_count(scope):
    """A refusal reaches neither the cache nor the data: it has no route."""
    assert counts_toward_hot(None, 403) is False
    assert batch_hits([_statement(scope, [7], route=None, status_code=403)]) == {}


def test_a_statement_that_failed_does_not_count(scope):
    assert counts_toward_hot("engine", 500) is False
    assert batch_hits([_statement(scope, [7], status_code=500)]) == {}


def test_a_name_that_is_not_a_registered_table_is_not_counted():
    assert counted_tables([7, "pg_catalog.pg_class"]) == frozenset({7})


def test_table_ids_resolved_on_the_writer_thread_are_counted(scope):
    record = SimpleNamespace(
        hot_scope=scope, table_ids=lambda: (7, 9), route="direct", status_code=200
    )
    assert batch_hits([record]) == {(scope, 7): 1, (scope, 9): 1}


# -- the window ------------------------------------------------------------------------------------


def test_counts_add_up_within_the_interval(scope):
    clock = _Clock(_INTERVAL * 1000 + 1)
    counts = HotCounts(None, clock=clock)
    counts.add({(scope, 7): 3, (scope, 9): 1}, _INTERVAL)
    clock.at += 10
    counts.add({(scope, 7): 2}, _INTERVAL)
    assert counts.counts(scope, [7, 9, 11], _INTERVAL) == {7: 5.0, 9: 1.0, 11: 0.0}


def test_the_count_does_not_drop_to_zero_at_a_bucket_boundary(scope):
    """40 statements late in one bucket: just after the boundary nearly all of them are still
    inside the last interval; half an interval later, half; a full interval later, none."""
    start = _INTERVAL * 2000
    clock = _Clock(start + _INTERVAL - 1)
    counts = HotCounts(None, clock=clock)
    counts.add({(scope, 7): 40}, _INTERVAL)
    clock.at = start + _INTERVAL  # the boundary
    assert counts.counts(scope, [7], _INTERVAL)[7] == pytest.approx(40.0)
    clock.at = start + _INTERVAL * 1.5
    assert counts.counts(scope, [7], _INTERVAL)[7] == pytest.approx(20.0)
    clock.at = start + _INTERVAL * 2
    assert counts.counts(scope, [7], _INTERVAL)[7] == 0.0


def test_a_count_expires_on_its_own(scope):
    """Derived state (REQ-1920): a bucket is kept for the interval it is current in and the one
    it is the previous of, then it is gone — nothing sweeps it."""
    clock = _Clock(_INTERVAL * 3000)
    counts = HotCounts(None, clock=clock)
    counts.add({(scope, 7): 1}, _INTERVAL)
    keys = counts._redis().keys(f"provisa:replica_hot:{scope}:7:*")
    assert len(keys) == 1
    assert 0 < counts._redis().ttl(keys[0]) <= 2 * _INTERVAL


def test_each_org_environment_counts_its_own_tables():
    org = f"org-{uuid.uuid4().hex}"
    prod, dev = count_scope(org, "prod"), count_scope(org, "dev")
    counts = HotCounts(None, clock=_Clock(_INTERVAL * 4000))
    counts.add({(prod, 7): 5}, _INTERVAL)
    assert counts.counts(prod, [7], _INTERVAL) == {7: 5.0}
    assert counts.counts(dev, [7], _INTERVAL) == {7: 0.0}


def test_an_empty_batch_touches_no_store():
    counts = HotCounts("redis://127.0.0.1:1/0")  # nothing listens there
    counts.add({}, _INTERVAL)
    assert counts.counts("any", [], _INTERVAL) == {}


# -- whether promotion is decided at all -----------------------------------------------------------


def test_with_a_shared_redis_promotion_runs_whatever_the_worker_count():
    shared = HotCounts("redis://redis.internal:6379/0")
    assert shared.shared is True
    assert promotion_runs(shared, workers=1) is True
    assert promotion_runs(shared, workers=8) is True


def test_with_the_embedded_redis_promotion_runs_for_a_single_worker():
    """The demo, a small single-node install: the one process sees every statement."""
    embedded = HotCounts(None)
    assert embedded.shared is False
    assert promotion_runs(embedded, workers=1) is True


def test_with_the_embedded_redis_and_several_workers_promotion_does_not_run():
    """Each worker counts only its own share of the traffic: no count is the table's."""
    assert promotion_runs(HotCounts(None), workers=2) is False


# -- the decision ----------------------------------------------------------------------------------

from provisa.core.models import Source, SourceType  # noqa: E402
from provisa.federation.engine import build_engine  # noqa: E402
from provisa.federation.replica_hot import (  # noqa: E402
    DEMOTE,
    NO_CLOCK,
    PROMOTE,
    HotCandidate,
    hot_candidates,
    judge,
    threshold_of,
)

_ENGINE = build_engine("trino")
_DEFAULT = 100


def _pg(sid: str = "pg", **settings) -> Source:
    """A source Trino reads in place."""
    base = dict(host="h", port=5432, database="d", username="u", cache_ttl=60)
    base.update(settings)
    return Source(id=sid, type=SourceType.postgresql, **base)


def _reg(table_id: int, name: str, source_id: str = "pg", **settings) -> SimpleNamespace:
    row = {
        "id": table_id,
        "source_id": source_id,
        "schema_name": "public",
        "table_name": name,
        "replicate": None,
        "load_protected": None,
        "change_signal": None,
        "region": None,  # REQ-1921: it names no region
        "cache_ttl": None,
        "columns": [SimpleNamespace(name="id", native_filter_type=None)],
        "row_materialize": False,
    }
    row.update(settings)
    return SimpleNamespace(**row)


def test_promotion_at_the_threshold_and_demotion_below_half_of_it():
    assert judge(99, 100, promoted=False) is None
    assert judge(100, 100, promoted=False) == PROMOTE
    assert judge(250, 100, promoted=False) == PROMOTE
    # promoted: it stays until its traffic falls below half the threshold
    assert judge(100, 100, promoted=True) is None
    assert judge(50, 100, promoted=True) is None
    assert judge(49.9, 100, promoted=True) == DEMOTE
    assert judge(0, 100, promoted=True) == DEMOTE


def test_a_table_hovering_at_its_threshold_is_not_promoted_and_demoted_over_and_over():
    promoted = False
    changes = 0
    for count in (100, 99, 101, 98, 100, 60, 100, 51):
        verdict = judge(count, 100, promoted=promoted)
        if verdict is not None:
            promoted = verdict == PROMOTE
            changes += 1
    assert (promoted, changes) == (True, 1)


def test_the_threshold_is_the_tables_own_n_else_its_sources_else_the_default():
    assert threshold_of(_pg(), _reg(1, "t"), _DEFAULT) == _DEFAULT
    assert threshold_of(_pg(), _reg(1, "t", replicate=500), _DEFAULT) == 500
    assert threshold_of(_pg(replicate=50), _reg(1, "t"), _DEFAULT) == 50
    assert threshold_of(_pg(replicate=50), _reg(1, "t", replicate=1000), _DEFAULT) == 1000


def test_never_always_and_load_protected_tables_are_not_judged():
    for settings in ({"replicate": -1}, {"replicate": 0}, {"load_protected": True}):
        assert threshold_of(_pg(), _reg(1, "t", **settings), _DEFAULT) is None
    assert threshold_of(_pg(load_protected=True), _reg(1, "t"), _DEFAULT) is None


def test_candidates_are_the_threshold_tables_of_sources_the_engine_reads_in_place():
    sources = {
        "pg": _pg(),
        "floored": _pg("floored", replicate=0),
        "api": Source(id="api", type=SourceType.openapi, path="https://x.test/spec.json"),
    }
    registered = [
        _reg(1, "default_table"),
        _reg(2, "hot_500", replicate=500),
        _reg(3, "never", replicate=-1),
        _reg(4, "always", replicate=0),
        _reg(5, "protected", load_protected=True),
        _reg(6, "of_a_floored_source", "floored", replicate=500),
        _reg(7, "of_a_source_reached_only_by_replica", "api"),
        _reg(8, "already_promoted", replicate=50),
    ]
    promoted = frozenset({("pg", "public", "already_promoted")})
    candidates, skipped = hot_candidates(registered, sources, promoted, _ENGINE, _DEFAULT)
    assert candidates == [
        HotCandidate(("pg", "public", "default_table"), 1, _DEFAULT, False),
        HotCandidate(("pg", "public", "hot_500"), 2, 500, False),
        HotCandidate(("pg", "public", "already_promoted"), 8, 50, True),
    ]
    assert skipped == {}


def test_a_default_table_with_no_replication_clock_is_skipped_and_the_reason_is_reported():
    """REQ-1907: a ttl change signal and no cache_ttl on the table or its source — a replica of
    it could never be judged stale. A table left at Default was never asked for a clock at save."""
    source = _pg(cache_ttl=None)
    registered = [_reg(1, "no_clock"), _reg(2, "own_clock", cache_ttl=30)]
    candidates, skipped = hot_candidates(registered, {"pg": source}, frozenset(), _ENGINE, _DEFAULT)
    assert [c.table_id for c in candidates] == [2]
    assert skipped == {("pg", "public", "no_clock"): NO_CLOCK}


def test_a_table_the_redis_hot_tier_manages_is_not_judged():
    """REQ-241: a table lives in at most one tier, and the hot tier wins."""
    from provisa.federation.replica_hot import HOT_TIER

    registered = [_reg(1, "currencies", cache_ttl=30), _reg(2, "orders", cache_ttl=30)]
    candidates, skipped = hot_candidates(
        registered,
        {"pg": _pg()},
        frozenset(),
        _ENGINE,
        _DEFAULT,
        hot_tier=frozenset({1}),  # currencies, by its table id
    )
    assert [c.table_id for c in candidates] == [2]
    assert skipped == {("pg", "public", "currencies"): HOT_TIER}


def test_candidates_are_read_from_rows_as_the_registry_view_builds_them():
    """The evaluation reads ``registry_view.registered_tables``. This builds its rows with the
    view's own builder from control-plane rows, so the fields the decision reads (id, the
    replica's key, replicate, load protection, change signal, cache_ttl) are the ones the view
    really provides — a stand-in row with a field the view lacks would pass and prove nothing."""
    from provisa.federation.registry_view import _build_registered_tables

    def _control_plane_row(table_id: int, name: str, **settings) -> dict:
        row = {
            "id": table_id,
            "source_id": "pg",
            "schema_name": "public",
            "table_name": name,
            "columns": [],
            "cache_ttl": None,
            "role_ttl": {},
            "pagination": None,  # REQ-318: the table sets no paging
            "replicate": None,
            "load_protected": None,
            "row_materialize": False,
            "dq_contract": None,
            "alias": None,
            "change_signal": None,
            "region": None,  # REQ-1921: it names no region
            # REQ-1919: the row carries the landing settings; the file is never read for them.
            "live": None,
            "watermark_column": None,
            "probe_type": None,
        }
        row.update(settings)
        return row

    rows = _build_registered_tables(
        [
            _control_plane_row(1, "orders", replicate=500, cache_ttl=30),
            _control_plane_row(2, "items"),
            _control_plane_row(3, "audit", replicate=-1),
        ],
    )
    candidates, skipped = hot_candidates(rows, {"pg": _pg()}, frozenset(), _ENGINE, _DEFAULT)
    assert candidates == [
        HotCandidate(("pg", "public", "orders"), 1, 500, False),
        HotCandidate(("pg", "public", "items"), 2, _DEFAULT, False),
    ]
    assert skipped == {}


def test_a_table_with_no_whole_copy_to_build_is_not_judged_and_the_reason_is_kept():
    """The one rule for what a build may copy whole (``replica_converge.whole_copy``): a table
    read by a parameter column is a function of its arguments, with no whole to replicate."""
    from provisa.federation.replica_hot import NOT_WHOLE

    by_parameter = _reg(1, "pet_by_id")
    by_parameter.columns = [
        SimpleNamespace(name="id", native_filter_type=None),
        SimpleNamespace(name="petId", native_filter_type="path"),
    ]
    candidates, skipped = hot_candidates(
        [by_parameter, _reg(2, "orders")], {"pg": _pg()}, frozenset(), _ENGINE, _DEFAULT
    )
    assert [c.table_id for c in candidates] == [2]
    assert skipped == {("pg", "public", "pet_by_id"): NOT_WHOLE}
