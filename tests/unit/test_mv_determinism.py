# Copyright (c) 2026 Kenneth Stott
# Canary: 9b1d3f5a-7c2e-4860-8a4d-1f3e5b7c9a0d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-879: MV determinism check for the distributed consistency tier.

Pure SQL analysis — no I/O. A distributed (per-instance) MV must be deterministic
or its per-instance copies never converge; the check rejects volatile functions and
unordered LIMITs. Shared MVs (single coordinated copy) are exempt.
"""

from __future__ import annotations

import pytest

from provisa.mv.determinism import check_view_determinism, validate_mv_consistency


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT a, b FROM t WHERE x = 1 ORDER BY a",
        "SELECT count(*) AS c, region FROM t GROUP BY region",
        "SELECT a FROM t ORDER BY a LIMIT 10",
        "SELECT o.id, c.name FROM orders o JOIN customers c ON o.cid = c.id",
    ],
)
def test_deterministic_views_pass(sql):
    ok, reason = check_view_determinism(sql)
    assert ok is True and reason == ""


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT now() AS ts, a FROM t",
        "SELECT current_timestamp AS ts FROM t",
        "SELECT random() AS r FROM t",
        "SELECT gen_random_uuid() AS id FROM t",
        "SELECT a FROM t LIMIT 10",  # unordered LIMIT
    ],
)
def test_non_deterministic_views_rejected(sql):
    ok, reason = check_view_determinism(sql)
    assert ok is False and reason


def test_unparseable_view_is_not_deterministic():
    ok, reason = check_view_determinism(">>> not sql <<<")
    assert ok is False and "parse" in reason


# --- validate_mv_consistency (the registration gate) ---------------------------


def test_shared_tier_skips_the_check():
    # Even a non-deterministic view is allowed for a shared MV (single coordinated copy).
    validate_mv_consistency("shared", "SELECT now() FROM t")


def test_distributed_deterministic_allowed():
    validate_mv_consistency("distributed", "SELECT a FROM t ORDER BY a")


def test_distributed_non_deterministic_rejected():
    with pytest.raises(ValueError, match="never converge"):
        validate_mv_consistency("distributed", "SELECT now() AS ts FROM t")


def test_join_pattern_mv_no_view_sql_is_exempt():
    # A join-pattern MV has no explicit view_sql — deterministic by construction.
    validate_mv_consistency("distributed", None)
