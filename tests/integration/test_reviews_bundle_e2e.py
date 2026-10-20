# Copyright (c) 2026 Kenneth Stott
# Canary: c5f9e79b-98a6-49ba-ab5f-c0ea842ae4b7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: the reviews demo, built into its bundle once and run from it (REQ-2049, REQ-2030).

The ``test`` instance. The bundle is built as demo-bundles.yml builds it (scripts/demo_bundle.py
build: the three stores and the seeder run once, on ports of the build's own; MongoDB is dumped
to an archive, Elasticsearch to a snapshot, Neo4j to a database dump) and published to a registry
of this test's own. Then, with nothing of the build left running, the CLI's way
(``start-ui-install.sh --demo reviews``): the bundle is pulled, its run-time compose file brings up
the three stores loaded from those dumps, read only, and a server on the bundle's config.yaml
walks the demo's tour -- every source and relationship it names is there, and every statement,
Cypher query, REST call and example it runs answers."""

# Requirements: REQ-2049, REQ-2030

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import httpx
import pytest
import yaml

from provisa.core.demo_bundles import store_namespaces
from tests.integration.test_weblogs_bundle_e2e import _shared, _shared_running, walk_tour

pytestmark = [pytest.mark.integration]

_REPO = Path(__file__).resolve().parents[2]
_SCRIPT = _REPO / "scripts" / "demo_bundle.py"
_ROLE = "org_admin"


@pytest.fixture(scope="module")
def published(tmp_path_factory):
    """The reviews bundle, built and published to a registry of this test's own."""
    from tests.integration.demo_registry import DemoRegistry

    out = tmp_path_factory.mktemp("reviews_bundle")
    done = subprocess.run(
        [sys.executable, str(_SCRIPT), "build", "reviews", "--out", str(out), "--timeout", "900"],
        capture_output=True,
        text=True,
        timeout=2400,
    )
    print(done.stdout[-4000:])
    assert done.returncode == 0, (done.stdout[-4000:], done.stderr[-8000:])
    (bundle,) = out.glob("reviews-*.tar.gz")
    registry = DemoRegistry()
    registry.start()
    try:
        print(registry.push(bundle))
        yield registry
    finally:
        registry.stop()


def _reviews_containers() -> list[str]:
    listed = subprocess.run(
        [
            "docker",
            "ps",
            "-a",
            "--filter",
            "label=com.provisa.demo=reviews",
            "--format",
            '{{.Names}} {{.Label "com.provisa.demo.role"}}',
        ],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    return [line for line in listed.split("\n") if line.strip()]


class RunTime:
    """The bundle's run-time compose file, under a project of this test's own."""

    def __init__(self, directory: Path) -> None:
        from tests.port_lease import lease_ports

        self.directory = directory
        self.mongo, self.es, self.neo4j = lease_ports(3)
        self.project = f"reviews-bundle-run-{os.getpid()}"
        self.ports = {
            "REVIEWS_MONGO_PORT": str(self.mongo),
            "REVIEWS_ES_PORT": str(self.es),
            "REVIEWS_NEO4J_HTTP_PORT": str(self.neo4j),
        }

    def compose(self, *args: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            ["docker", "compose", "-p", self.project]
            + ["-f", str(self.directory / "docker-compose.run.yml"), *args],
            env={**os.environ, **self.ports},
            capture_output=True,
            text=True,
            check=True,
            timeout=900,
        )


def _post(server, path: str, body: dict) -> httpx.Response:
    return httpx.post(
        f"{server.base_url}{path}", json=body, headers={"X-Provisa-Role": _ROLE}, timeout=300
    )


def _walk_the_rest(server, demo: Path) -> list[str]:
    """The tour's steps walk_tour does not run: its Cypher and its REST call."""
    walked = []
    for step in yaml.safe_load((demo / "tour.yaml").read_text())["steps"]:
        if "cypher" in step:
            resp = _post(server, "/data/cypher", {"query": step["cypher"]})
            assert resp.status_code == 200 and resp.json()["rows"], (step, resp.text)
            walked.append(f"{step['key']}: cypher -> {len(resp.json()['rows'])} rows")
        if "api" in step:
            method, path = step["api"].split(" ", 1)
            resp = httpx.request(
                method, f"{server.base_url}{path}", headers={"X-Provisa-Role": _ROLE}, timeout=300
            )
            assert resp.status_code == 200 and resp.json()["data"], (step, resp.text)
            walked.append(f"{step['key']}: api -> {len(resp.json()['data'])} rows")
    return walked


def _writes_are_refused(run: RunTime) -> None:
    """Each store is read only where the demo runs: the reader cannot write the catalog, the
    review index is closed to writes, and the graph database is read only."""
    from pymongo import MongoClient
    from pymongo.errors import OperationFailure

    reader = MongoClient(
        f"mongodb://reviews_reader:reviews_reader@127.0.0.1:{run.mongo}/", directConnection=True
    )
    try:
        assert reader["catalog"]["products"].count_documents({}) == 200
        with pytest.raises(OperationFailure):
            reader["catalog"]["products"].insert_one({"product_id": 999})
    finally:
        reader.close()
    es = httpx.put(f"http://127.0.0.1:{run.es}/reviews/_doc/x", json={"text": "x"}, timeout=60)
    assert es.status_code == 403, es.text
    neo = httpx.post(
        f"http://127.0.0.1:{run.neo4j}/db/neo4j/tx/commit",
        json={"statements": [{"statement": "CREATE (:Product {product_id: 999})"}]},
        timeout=60,
    )
    assert neo.json()["errors"], neo.text


def test_the_cli_pulls_the_bundle_and_walks_the_tour_from_its_stores(published, tmp_path_factory):
    registry = published
    assert not _reviews_containers(), "the build left containers"
    home = tmp_path_factory.mktemp("reviews_cli")
    pulled = registry.pull("reviews", home / "demos")
    assert pulled.returncode == 0, pulled.stderr
    demo = Path(pulled.stdout.strip())

    run = RunTime(demo)
    run.compose("up", "-d", "--wait")
    server = None
    try:
        roles = sorted(c.split()[-1] for c in _reviews_containers())
        assert roles == ["document", "graph", "search"], roles  # the stores alone: no seeder
        _writes_are_refused(run)
        from tests.integration.isolated_server import IsolatedServer

        server = IsolatedServer(
            "reviewsbundle",
            engine="duckdb",
            config=str(demo / "config.yaml"),
            control_plane="own",
            env={
                "PROVISA_NAMED_DEMO_DIR": str(demo),
                **run.ports,
                # Its own stores, as start-ui-install.sh --demo runs them: no suffix.
                **store_namespaces(demo),
                "PROVISA_DATA_DIR": str(home / "provisa"),
            },
        )
        server.start(timeout=300)
        try:
            walked = walk_tour(server, demo) + _walk_the_rest(server, demo)
        except AssertionError:
            print(server.dump_stderr_debug()[-6000:])
            raise
        print("\n".join(walked))
        # Every step but those a page alone shows: a table's detail panel, the graph grouped.
        steps = yaml.safe_load((demo / "tour.yaml").read_text())["steps"]
        assert len(walked) == len([s for s in steps if not {"table", "group_by"} & s.keys()])
    finally:
        if server is not None:
            server.stop_process()
        run.compose("down", "-v", "--remove-orphans")


def test_a_visitor_sandbox_of_reviews_reads_the_shared_stores(published, tmp_path_factory):
    """The sandbox tier's node: the demo's MongoDB catalog and Elasticsearch index loaded into the
    node's shared instances under its version's names, its Neo4j graph its version's own
    container; a visitor's sandbox reads each (REQ-2049)."""
    import time

    from tests.integration.isolated_server import IsolatedServer
    from tests.integration.test_demo_sandboxes_e2e import _JWT_SECRET, _deployment_config

    registry = published
    work = tmp_path_factory.mktemp("reviews_tier")
    bundles = work / "data" / "demo-bundles"
    synced = registry.sync("reviews", bundles)
    print(synced.stdout, synced.stderr)
    assert synced.returncode == 0, (synced.stdout, synced.stderr)
    version = (bundles / "reviews" / "ready").read_text().strip()
    stores = dict(
        line.split("=", 1)
        for line in (bundles / "reviews" / version / "stores.env").read_text().split()
    )
    print(stores)
    suffix = stores["REVIEWS_MONGO_NS"]
    assert suffix.startswith("_reviews_") and stores["REVIEWS_ES_NS"] == suffix, stores
    assert _shared_running(registry) == {"mongo", "elasticsearch"}
    databases = _shared(
        registry,
        bundles,
        *("exec", "-T", "mongo", "mongosh", "--quiet", "-u", "root", "-p", "root"),
        *("--authenticationDatabase", "admin"),
        "--eval",
        "db.adminCommand({listDatabases: 1}).databases.forEach(d => print(d.name))",
    ).stdout.split()
    assert f"catalog{suffix}" in databases and "catalog" not in databases, databases
    indices = _shared(
        registry,
        bundles,
        *("exec", "-T", "elasticsearch", "curl", "-s"),
        "http://localhost:9200/_cat/indices?h=index",
    ).stdout.split()
    assert f"reviews{suffix}" in indices, indices
    project = f"provisa-demo-reviews-{version}"
    own = subprocess.run(
        ["docker", "compose", "-p", project, "ps", "--services"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.split()
    assert own == ["neo4j"], own

    config = work / "config.yaml"
    _deployment_config(config)
    tier = IsolatedServer(
        "reviewstiere2e",
        engine="duckdb",
        config=str(config),
        auth={"provider": "basic", "jwt_secret": _JWT_SECRET, "assignments_source": "provisa"},
        env={
            "PROVISA_SANDBOX_TIER": "true",
            "PROVISA_DEMO_BUNDLES": "reviews",
            "PROVISA_DEMO_REGISTRY": registry.url,
            "PROVISA_DATA_DIR": str(work / "data"),
        },
    )
    tier.start(timeout=300)
    try:
        deadline = time.monotonic() + 900
        while True:
            opened = httpx.post(f"{tier.base_url}/public/demos/reviews", timeout=600)
            if opened.status_code == 200:
                break
            assert opened.json().get("code") == "sandbox.demo_not_ready", (
                opened.text,
                tier.dump_stderr_debug()[-6000:],
            )
            assert time.monotonic() < deadline, tier.dump_stderr_debug()[-6000:]
            time.sleep(3)
        sandbox = opened.json()
        headers = {
            "Authorization": f"Bearer {sandbox['token']}",
            "x-org-provisa": sandbox["org_id"],
        }
        for table, store in (
            ("products", "MongoDB"),
            ("reviews", "Elasticsearch"),
            ("customers", "Neo4j"),
        ):
            rows = httpx.post(
                f"{tier.base_url}/data/sql",
                json={"sql": f'SELECT COUNT(*) AS n FROM "store"."{table}"'},
                headers=headers,
                timeout=300,
            )
            assert rows.status_code == 200, (store, rows.text, tier.dump_stderr_debug()[-6000:])
            assert rows.json()["data"]["sql"][0]["n"] > 0, (store, rows.text)
    finally:
        tier.stop_process()
        subprocess.run(["docker", "compose", "-p", project, "down", "-v"], check=False)
