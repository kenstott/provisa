# Copyright (c) 2026 Kenneth Stott
# Canary: 71f0c2ad-9e3b-4b58-a6d4-5c8e20b1f9e6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""``/debug/logs``'s ``pattern`` comes from the request: it runs on the bounded matcher, refused by
name when it could backtrack catastrophically and cut off, refused by name, past its step limit."""

# Requirements: REQ-2087

from __future__ import annotations

import time

import pytest

from provisa.api.debug_log_filter import lines_matching
from provisa.api.errors import ApiError


def test_a_pattern_filters_the_lines_it_matches():
    lines = ["auth ok for alice", "route /data", "auth failed for bob"]
    assert lines_matching(r"auth \w+", lines) == ["auth ok for alice", "auth failed for bob"]


@pytest.mark.parametrize("pattern", [r"(a+)+$", r"(\w+\s?)*$", r"(a)\1"])
def test_a_pattern_that_could_stall_is_refused_by_name(pattern):
    with pytest.raises(ApiError) as caught:
        lines_matching(pattern, ["a" * 40 + "!"])
    assert caught.value.status_code == 400
    assert caught.value.code == "debug_logs.pattern_refused"


def test_one_search_is_linear_in_the_line():
    # One search never rescans: a line that defeats a backtracking engine is one pass here.
    started = time.monotonic()
    assert lines_matching(r"x|a(?:.*b)?c", ["a" * 20000]) == []
    assert time.monotonic() - started < 5


def test_a_pattern_past_its_step_limit_is_refused_by_name(monkeypatch):
    from provisa.security import bounded_pattern

    # One search stays inside its limit by construction (two steps per instruction per
    # character, under STEP_FACTOR); a limit of none shows what a reader is told past it.
    monkeypatch.setattr(bounded_pattern, "STEP_FACTOR", 0)
    with pytest.raises(ApiError) as caught:
        lines_matching(r"auth", ["auth ok"])
    assert caught.value.code == "debug_logs.pattern_exceeded"
    assert caught.value.params["length"] == len("auth ok")


def test_the_route_filters_through_the_bounded_matcher():
    import inspect

    from provisa.api import debug_logs as route_module

    source = inspect.getsource(route_module)
    route = source[source.index('"/debug/logs"') :][:1500]
    assert "lines_matching(pattern" in route
    assert "re.search(pattern" not in route
