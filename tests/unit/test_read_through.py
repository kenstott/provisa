# Copyright (c) 2026 Kenneth Stott
# Canary: 2d9c4a71-6b08-4e75-9f12-3c7a0d4f9c02
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-847: read-through / write-back discipline."""

from __future__ import annotations

from provisa.federation.read_through import ReadOutcome, plan_mutation, resolve_read


def _read(**kw) -> ReadOutcome:
    base = dict(pull_ok=False, cache_fresh=False, cache_has_data=False, stale_policy_allows=False)
    base.update(kw)
    return resolve_read(**base)


# ---- read discipline --------------------------------------------------------


def test_successful_pull_serves_fresh():
    assert _read(pull_ok=True) is ReadOutcome.SERVE_FRESH


def test_fresh_cache_serves_without_pull():
    assert _read(pull_ok=False, cache_fresh=True) is ReadOutcome.SERVE_FRESH


def test_pull_failure_no_data_is_hard_error():
    assert _read(pull_ok=False, cache_fresh=False, cache_has_data=False) is ReadOutcome.HARD_ERROR


def test_pull_failure_stale_data_without_policy_is_hard_error():
    # Stale data exists but no explicit stale policy → HARD ERROR, never silent stale.
    assert (
        _read(pull_ok=False, cache_has_data=True, stale_policy_allows=False)
        is ReadOutcome.HARD_ERROR
    )


def test_pull_failure_stale_data_with_explicit_policy_serves_stale():
    assert (
        _read(pull_ok=False, cache_has_data=True, stale_policy_allows=True)
        is ReadOutcome.SERVE_STALE
    )


def test_stale_policy_without_data_is_still_hard_error():
    # Policy allows stale, but there is nothing cached → still a hard error.
    assert _read(cache_has_data=False, stale_policy_allows=True) is ReadOutcome.HARD_ERROR


# ---- write discipline -------------------------------------------------------


def test_mutation_targets_upstream_and_invalidates():
    plan = plan_mutation("orders_cache")
    assert plan.target == "upstream"  # never the cache
    assert plan.invalidate == "orders_cache"
