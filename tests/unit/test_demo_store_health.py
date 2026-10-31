# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A demo whose store is red or down is named as failed at its open, not partway through its tour
(REQ-2006; cloud-dev, 2026-10-31: past Elasticsearch's high watermark the reviews index was red,
its version still marked ready, and the tour's search answered 503). Engine releases the running
Provisa does not read leave the runtime-deps cache: six of them held 13 GB of a 49 GB disk."""

# Requirements: REQ-2006, REQ-2049

from __future__ import annotations

import asyncio
import socket
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from provisa.api import demo_sandbox
from provisa.api.errors import ApiError
from provisa.core import demo_bundles
from provisa.runtime_deps import pgwire_bundles

pytestmark = pytest.mark.unit

_RUN = """
services:
  search:
    image: docker.elastic.co/elasticsearch/elasticsearch:8.13.0
    labels:
      com.provisa.demo.role: search
      com.provisa.demo.host_env: REVIEWS_ES_HOST
      com.provisa.demo.port_env: REVIEWS_ES_PORT
      com.provisa.demo.ns_env: REVIEWS_ES_NS
  graph:
    image: neo4j:5
    labels:
      com.provisa.demo.host_env: REVIEWS_NEO4J_HOST
      com.provisa.demo.port_env: REVIEWS_NEO4J_PORT
  loader:
    image: busybox
"""
_ENV = (
    "REVIEWS_ES_HOST=es\nREVIEWS_ES_PORT=9200\nREVIEWS_ES_NS=_reviews_6ac065b6\n"
    "REVIEWS_NEO4J_HOST=neo\nREVIEWS_NEO4J_PORT=7474\n"
)


@pytest.fixture
def bundle(tmp_path) -> Path:
    (tmp_path / "docker-compose.run.yml").write_text(_RUN)
    (tmp_path / "stores.env").write_text(_ENV)
    return tmp_path


@pytest.fixture
def stores(monkeypatch):
    """The node's stores as the tier reaches them: ``down`` the hosts refusing a connection, and
    what Elasticsearch answers its health with."""
    state = SimpleNamespace(down=set(), status="yellow", es_code=200, asked=[], reached=[])

    def connect(address, timeout):
        state.reached.append(address)
        if address[0] in state.down:
            raise ConnectionRefusedError(address)
        return SimpleNamespace(close=lambda: None)

    def get(url, timeout):
        state.asked.append(url)
        return httpx.Response(
            state.es_code, json={"status": state.status}, request=httpx.Request("GET", url)
        )

    monkeypatch.setattr(socket, "create_connection", connect)
    monkeypatch.setattr(demo_bundles.httpx, "get", get)
    return state


def test_healthy_stores_are_reported_as_none(bundle, stores):
    assert demo_bundles.unhealthy_stores(bundle) == []
    assert stores.asked == ["http://es:9200/_cluster/health/*_reviews_6ac065b6"]
    assert ("neo", 7474) in stores.reached


def test_a_demo_running_no_stores_has_none_unhealthy(tmp_path, stores):
    assert demo_bundles.unhealthy_stores(tmp_path) == []


def test_a_red_index_is_named(bundle, stores):
    stores.status = "red"
    assert demo_bundles.unhealthy_stores(bundle) == ["search: its data is red"]


def test_a_search_store_answering_an_error_is_named(bundle, stores):
    stores.es_code = 503
    assert demo_bundles.unhealthy_stores(bundle) == ["search: answered 503"]


def test_a_store_that_refuses_a_connection_is_named(bundle, stores):
    stores.down = {"neo"}
    said = demo_bundles.unhealthy_stores(bundle)
    assert len(said) == 1 and said[0].startswith("graph: not reachable")


@pytest.fixture
def served(monkeypatch, bundle):
    monkeypatch.setenv("PROVISA_DEMO_BUNDLES", "reviews")
    demo = SimpleNamespace(name="reviews", title="Reviews", directory=bundle, stores_ready=True)

    async def follow(_name):
        return None

    async def template(_demo):
        return None

    monkeypatch.setattr(demo_sandbox, "follow_latest", follow)
    monkeypatch.setattr(demo_sandbox, "published_demo", lambda _name: demo)
    monkeypatch.setattr(demo_sandbox, "_template_ready", template)
    return demo


def test_an_open_of_a_demo_whose_store_is_red_fails_by_name_at_once(served, stores, caplog):
    stores.status = "red"
    with pytest.raises(ApiError) as refused:
        asyncio.run(demo_sandbox._ready_demo("reviews"))
    assert refused.value.status_code == 503
    assert refused.value.code == "sandbox.demo_failed"
    assert refused.value.detail == "Reviews could not be prepared; try again in a few minutes"
    assert "es:9200" not in refused.value.detail
    assert "search: its data is red" in caplog.text  # the operator's


def test_an_open_of_a_healthy_demo_goes_on(served, stores):
    assert asyncio.run(demo_sandbox._ready_demo("reviews")) is served


def test_a_demo_served_from_its_directory_is_not_asked(served, stores, monkeypatch):
    """It runs its stores itself."""
    monkeypatch.delenv("PROVISA_DEMO_BUNDLES")
    stores.status = "red"
    assert asyncio.run(demo_sandbox._ready_demo("reviews")) is served


# -- the runtime-deps cache --------------------------------------------------------------------


def _releases(root: Path, *names: str) -> None:
    for name in names:
        (root / name / "pgwire-file" / "bin").mkdir(parents=True)


def test_only_the_engine_releases_read_stay_in_the_cache(tmp_path, monkeypatch):
    monkeypatch.setattr(pgwire_bundles, "CONNECTOR_RELEASE", {"govdata": "engine-v0.122.0"})
    current = pgwire_bundles.RELEASE_TAG
    _releases(tmp_path, "engine-v0.109.0", "engine-v0.119.4", "engine-v0.122.0", current)
    (tmp_path / "code").mkdir()
    (tmp_path / "code-bundles").mkdir()
    (tmp_path / "reasoner").mkdir()
    gone = pgwire_bundles.prune_releases(tmp_path)
    assert gone == ["engine-v0.109.0", "engine-v0.119.4"]
    assert sorted(p.name for p in tmp_path.iterdir()) == sorted(
        ["code", "code-bundles", "reasoner", "engine-v0.122.0", current]
    )


def test_an_empty_cache_is_left_as_it_is(tmp_path):
    assert pgwire_bundles.prune_releases(tmp_path / "none") == []


def test_the_command_prunes_the_cache_it_is_run_for(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("PROVISA_RUNTIME_DEPS_CACHE", str(tmp_path))
    _releases(tmp_path, "engine-v0.109.0")
    assert demo_bundles._main(["prune-engines"]) == 0
    assert not (tmp_path / "engine-v0.109.0").exists()
    assert "engine-v0.109.0" in capsys.readouterr().out
