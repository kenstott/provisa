# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: a demo version taken down from the shared Elasticsearch leaves no mark that says it
is still loaded (REQ-2049). The node's shared instance is 8.13.0, as the test stack's is: it
refuses a prefix query on _id, and the delete_by_query that once cleared a version's marks so was
refused unread -- the reviews demo then came back "ready" over indices its linger had dropped, and
every query of it was a 404."""

# Requirements: REQ-2049

from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path

import httpx
import pytest

pytestmark = [pytest.mark.integration, pytest.mark.requires_elasticsearch]

_REPO = Path(__file__).resolve().parents[2]


def _module():
    spec = importlib.util.spec_from_file_location(
        "demo_stores", _REPO / "scripts" / "demo_stores.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module  # its dataclasses look their module up
    spec.loader.exec_module(module)
    return module


ds = _module()
SUFFIX = ds.namespace("esdropcheck", "v1")
NS = SUFFIX.lstrip("_")
OTHER = ds.namespace("esdropother", "v1").lstrip("_")


def _base() -> str:
    return f"http://localhost:{int(os.environ['ELASTICSEARCH_PORT'])}"


class _StackShared(ds.Shared):
    """The test stack's Elasticsearch in place of the node's shared one: each command the shared
    instance would run in its container runs here against the stack's published port."""

    def exec(self, kind, *cmd, stdin=None):
        assert kind == "elasticsearch" and stdin is None
        return self.runner.run([c.replace("http://localhost:9200", _base()) for c in cmd])


def _shared():
    return _StackShared(ds.Runner(), "unused", "unused", {})


@pytest.fixture()
def loaded():
    with httpx.Client(timeout=60) as c:
        # As the node's shared instance runs (docker-compose.shared.yml): a version's indices
        # are dropped by a wildcard on its suffix.
        c.put(
            f"{_base()}/_cluster/settings",
            json={"transient": {"action.destructive_requires_name": False}},
        ).raise_for_status()
        c.delete(f"{_base()}/reviews{SUFFIX}")
        c.put(f"{_base()}/reviews{SUFFIX}").raise_for_status()
        for mark in (f"{NS}:elasticsearch", f"{OTHER}:elasticsearch"):
            c.put(
                f"{_base()}/{ds._ES_MARKS}/_doc/{mark}?refresh=true", json={"loaded": True}
            ).raise_for_status()
    yield
    with httpx.Client(timeout=60) as c:
        c.delete(f"{_base()}/reviews{SUFFIX}")
        c.delete(f"{_base()}/{ds._ES_MARKS}/_doc/{OTHER}:elasticsearch?refresh=true")
        c.put(
            f"{_base()}/_cluster/settings",
            json={"transient": {"action.destructive_requires_name": None}},
        ).raise_for_status()


@pytest.mark.usefixtures("loaded")
def test_this_elasticsearch_refuses_a_prefix_query_on_id():
    """Why the marks are deleted by id: the query the take-down once sent is refused."""
    resp = httpx.post(
        f"{_base()}/{ds._ES_MARKS}/_delete_by_query?refresh=true&conflicts=proceed",
        json={"query": {"prefix": {"_id": f"{NS}:"}}},
        timeout=60,
    )
    assert resp.status_code == 400, resp.text


@pytest.mark.usefixtures("loaded")
def test_a_version_taken_down_loses_its_indices_and_its_marks_and_no_other():
    ds.es_drop(_shared(), SUFFIX)
    with httpx.Client(timeout=60) as c:
        assert c.get(f"{_base()}/reviews{SUFFIX}").status_code == 404
        assert c.get(f"{_base()}/{ds._ES_MARKS}/_doc/{NS}:elasticsearch").status_code == 404
        assert c.get(f"{_base()}/{ds._ES_MARKS}/_doc/{OTHER}:elasticsearch").status_code == 200
