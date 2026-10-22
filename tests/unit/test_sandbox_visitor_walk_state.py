# Copyright (c) 2026 Kenneth Stott
# Canary: 2b5583b2-cb26-4d8a-8033-878617f3ee04
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2006: the visitor's walk (tests/integration/test_sandbox_visitor_walk.py) sends each step in
the role, environment and time travel the steps before it chose, as provisa-ui/src/tour/demoTour.ts
sets them: a role picked on the SQL explorer is its run-as and reaches only its statements, one
picked elsewhere is the app's, a set of roles goes to the explorer, an environment reaches every
request after it, and time travel reaches only statements."""

# Requirements: REQ-2006, REQ-2066

from __future__ import annotations

import httpx
import pytest

from tests.integration import test_sandbox_visitor_walk as walk_module

pytestmark = pytest.mark.unit

_TOKEN = {"Authorization": "Bearer t", "x-org-provisa": "sbx1"}


def _walk() -> walk_module.Walk:
    walk = object.__new__(walk_module.Walk)
    walk.tier = None
    walk.token = _TOKEN
    walk.app_role = walk.all_roles = "analyst,sandbox"
    walk.sql_role = None
    walk.page = None
    walk.env = None
    walk.as_of = None
    walk.as_recorded = None
    walk.answers = {}
    return walk


def test_the_first_request_acts_as_every_role_the_visitor_holds():
    walk = _walk()
    assert walk.headers(sql=True)["X-Provisa-Role"] == "analyst,sandbox"
    assert walk.headers(sql=False)["X-Provisa-Role"] == "analyst,sandbox"
    assert "x-provisa-env" not in walk.headers(sql=False)


def test_a_role_picked_on_the_sql_explorer_reaches_only_its_statements():
    walk = _walk()
    walk.choose({"key": "a", "route": "/sql", "role": "analyst"})
    assert walk.headers(sql=True)["X-Provisa-Role"] == "analyst"
    assert walk.headers(sql=False)["X-Provisa-Role"] == "analyst,sandbox"


def test_a_role_picked_elsewhere_is_the_app_s_and_all_is_every_role():
    walk = _walk()
    walk.choose({"key": "a", "route": "/graphql", "role": "analyst"})
    assert walk.headers(sql=False)["X-Provisa-Role"] == "analyst"
    assert walk.headers(sql=True)["X-Provisa-Role"] == "analyst"
    walk.choose({"key": "b", "role": "all"})
    assert walk.headers(sql=False)["X-Provisa-Role"] == "analyst,sandbox"


def test_a_set_of_roles_is_the_sql_explorer_s_run_as():
    walk = _walk()
    walk.choose({"key": "a", "route": "/graphql", "role": "reviewer,region__eu"})
    assert walk.headers(sql=True)["X-Provisa-Role"] == "reviewer,region__eu"
    assert walk.headers(sql=False)["X-Provisa-Role"] == "analyst,sandbox"


def test_an_environment_reaches_every_request_after_it():
    walk = _walk()
    walk.choose({"key": "a", "env": "dev"})
    assert walk.headers(sql=True)["x-provisa-env"] == "dev"
    assert walk.headers(sql=False)["x-provisa-env"] == "dev"


def test_time_travel_reaches_only_statements():
    walk = _walk()
    walk.choose({"key": "a", "as_of": "2026-01-01T00:00:00Z"})
    walk.answers["earlier"] = [{"recorded_at": "2026-02-01T00:00:00Z"}]
    walk.choose(
        {"key": "b", "as_recorded_from": {"step": "earlier", "row": 0, "column": "recorded_at"}}
    )
    sql = walk.headers(sql=True)
    assert sql["X-Provisa-As-Of"] == "2026-01-01T00:00:00Z"
    assert sql["X-Provisa-As-Recorded"] == "2026-02-01T00:00:00Z"
    other = walk.headers(sql=False)
    assert "X-Provisa-As-Of" not in other and "X-Provisa-As-Recorded" not in other


def test_every_published_demo_is_a_parameter():
    names = walk_module.published_demos()
    assert "testdata" in names and "weblogs" in names
    # A regions-tier demo is walked like any other published one, whichever node serves it.
    assert "retail" in names and "banking" in names
    assert "perf" not in names  # published: false
    assert "groupexposure" in names  # published once its card was taken from its real answer


_CLOSE = 'SELECT c.category FROM "provisioning"."view_close_by_category" AS c'


def _refused(view: str) -> httpx.Response:
    return httpx.Response(
        503,
        json={
            "detail": f"the view {view!r} can be read only within its read window",
            "code": "data.view_outside_load_window",
            "params": {"view": view, "opens_at": "2026-10-20T02:00+00:00"},
        },
    )


def test_a_step_reading_a_view_outside_its_read_window_is_answered_by_the_refusal():
    """REQ-2066: outside its window the page shows the refusal naming the view (the provisioning
    tour's close step is written for it), so the walk takes it as the step's answer."""
    said, answer = walk_module._answered("close (sql)", _refused("view_close_by_category"), _CLOSE)
    assert answer is None
    assert said == "close (sql): refused outside view_close_by_category's read window"


def test_a_refusal_for_a_view_the_step_does_not_read_fails_it():
    with pytest.raises(AssertionError):
        walk_module._answered("close (sql)", _refused("view_order_margins"), _CLOSE)


def test_a_refusal_off_the_sql_explorer_fails_the_step():
    with pytest.raises(AssertionError):
        walk_module._answered("close (graphql)", _refused("view_close_by_category"))
