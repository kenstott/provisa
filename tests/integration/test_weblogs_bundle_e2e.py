# Copyright (c) 2026 Kenneth Stott
# Canary: 784a3a4f-a848-4500-8ac3-a33435158e09
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: the weblogs demo, built into its bundle once and run from it (REQ-2049).

The ``test`` instance. The bundle is built as demo-bundles.yml builds it (scripts/demo_bundle.py
build: the shop, nginx and the seeder run once, on ports of the build's own) and published to a
registry of this test's own. Then, with nothing of the build left running:

- the CLI's way (``start-ui-install.sh --demo weblogs``): the bundle is pulled, its run-time
  compose file brings up the shop's database alone, loaded from the bundle's dump, and a server on
  the bundle's config.yaml walks the demo's tour -- every source, relationship, glossary term and
  role it names is there, and every example query and statement it runs answers with rows;
- the demo webpage's way: a sandbox tier pulling weblogs from the registry opens a visitor's
  sandbox, which reads the demo's views from its replicas."""

# Requirements: REQ-2049, REQ-2006, REQ-2010

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest
import yaml

from provisa.core.demo_bundles import store_namespaces
from tests.integration.test_demo_sandboxes_e2e import _JWT_SECRET, _deployment_config

pytestmark = [pytest.mark.integration]

_REPO = Path(__file__).resolve().parents[2]
_SCRIPT = _REPO / "scripts" / "demo_bundle.py"
_ROLE = "org_admin"


@pytest.fixture(scope="module")
def published(tmp_path_factory):
    """The weblogs bundle, built and published to a registry of this test's own."""
    from tests.integration.demo_registry import DemoRegistry

    out = tmp_path_factory.mktemp("weblogs_bundle")
    done = subprocess.run(
        [sys.executable, str(_SCRIPT), "build", "weblogs", "--out", str(out), "--timeout", "900"],
        capture_output=True,
        text=True,
        timeout=2400,
    )
    print(done.stdout[-4000:])
    assert done.returncode == 0, (done.stdout[-4000:], done.stderr[-8000:])
    (bundle,) = out.glob("weblogs-*.tar.gz")
    registry = DemoRegistry()
    registry.start()
    try:
        print(registry.push(bundle))
        yield registry, bundle.name.removeprefix("weblogs-").removesuffix(".tar.gz")
    finally:
        registry.stop()


def _weblogs_containers() -> list[str]:
    return subprocess.run(
        [
            "docker",
            "ps",
            "-a",
            "--filter",
            "label=com.provisa.demo=weblogs",
            "--format",
            '{{.Names}} {{.Label "com.provisa.demo.role"}}',
        ],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.split("\n")


class RunTime:
    """The bundle's run-time compose file, under a project of this test's own."""

    def __init__(self, directory: Path) -> None:
        from tests.port_lease import lease_ports

        self.directory = directory
        (self.pg_port,) = lease_ports(1)
        self.project = f"weblogs-bundle-run-{os.getpid()}"
        self.env = {**os.environ, "WEBLOGS_PG_PORT": str(self.pg_port)}

    def compose(self, *args: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            [
                "docker",
                "compose",
                "-p",
                self.project,
                "-f",
                str(self.directory / "docker-compose.run.yml"),
                *args,
            ],
            env=self.env,
            capture_output=True,
            text=True,
            check=True,
            timeout=600,
        )


def _admin(server, query: str) -> dict:
    resp = httpx.post(
        f"{server.base_url}/admin/graphql",
        json={"query": query},
        headers={"X-Provisa-Role": _ROLE},
        timeout=60,
    )
    assert resp.status_code == 200 and not resp.json().get("errors"), resp.text
    return resp.json()["data"]


def _rows(server, path: str, body: dict) -> dict:
    resp = httpx.post(
        f"{server.base_url}{path}",
        json={**body, "role": _ROLE},
        headers={"X-Provisa-Role": _ROLE},
        timeout=180,
    )
    assert resp.status_code == 200, (body, resp.text)
    answer = resp.json()
    assert not answer.get("errors") and answer.get("data"), (body, resp.text)
    return answer["data"]


def walk_tour(server, demo: Path) -> list[str]:
    """Every step of the demo's tour, done through the API its page calls; what each step did."""
    steps = yaml.safe_load((demo / "tour.yaml").read_text())["steps"]
    sources = {s["id"] for s in _admin(server, "{ sources { id } }")["sources"]}
    aliases = {r["alias"] for r in _admin(server, "{ relationships { alias } }")["relationships"]}
    roles = {r["id"] for r in _admin(server, "{ roles { id } }")["roles"]}
    walked = []
    for step in steps:
        if "source" in step:
            assert step["source"] in sources, step
            walked.append(f"{step['key']}: source {step['source']}")
        if "relationship" in step:
            assert step["relationship"] in aliases, step
            walked.append(f"{step['key']}: relationship {step['relationship']}")
        if "example" in step:
            text = (demo / "queries" / f"{step['example']}.graphql").read_text()
            data = _rows(server, "/data/graphql", {"query": text})
            assert all(data.values()), (step, data)
            walked.append(
                f"{step['key']}: example {step['example']} -> {len(next(iter(data.values())))} rows"
            )
        if "sql" in step:
            rows = _rows(server, "/data/sql", {"sql": step["sql"]})["sql"]
            assert rows, step
            walked.append(f"{step['key']}: sql -> {len(rows)} rows")
        if "term" in step:
            terms = httpx.get(
                f"{server.base_url}/admin/glossary/terms",
                params={"q": step["term"], "include_deprecated": "false"},
                headers={"X-Provisa-Role": _ROLE},
                timeout=60,
            )
            assert terms.status_code == 200, terms.text
            assert step["term"] in {t["name"] for t in terms.json()}, (step, terms.text)
            walked.append(f"{step['key']}: term {step['term']}")
        if "role" in step:
            assert step["role"] == "all" or step["role"] in roles, step
            walked.append(f"{step['key']}: role {step['role']}")
    return walked


def test_the_cli_pulls_the_bundle_and_walks_the_tour_with_only_its_database(
    published, tmp_path_factory
):
    registry, version = published
    assert not [c for c in _weblogs_containers() if c.strip()], "the build left containers"
    home = tmp_path_factory.mktemp("weblogs_cli")
    pulled = registry.pull("weblogs", home / "demos")
    assert pulled.returncode == 0, pulled.stderr
    demo = Path(pulled.stdout.strip())
    assert demo == home / "demos" / "weblogs" / version
    pinned = registry.pull(f"weblogs@{version}", home / "demos")
    assert Path(pinned.stdout.strip()) == demo

    run = RunTime(demo)
    run.compose("up", "-d", "--wait")
    server = None
    try:
        roles = sorted(c.split()[-1] for c in _weblogs_containers() if c.strip())
        assert roles == ["rdb"], roles  # the database alone: no shop, nginx or seeder
        from tests.integration.isolated_server import IsolatedServer

        server = IsolatedServer(
            "weblogsbundle",
            engine="duckdb",
            config=str(demo / "config.yaml"),
            control_plane="own",
            env={
                "PROVISA_NAMED_DEMO_DIR": str(demo),
                "WEBLOGS_PG_PORT": str(run.pg_port),
                # Its own database, as start-ui-install.sh --demo runs it: no suffix.
                **store_namespaces(demo),
                "PROVISA_LOGS_ALLOWED_ROOTS": str(demo / "logs"),
                "PROVISA_DATA_DIR": str(home / "provisa"),
            },
        )
        server.start(timeout=300)
        deadline = time.monotonic() + 300
        while True:
            try:
                walked = walk_tour(server, demo)
                break
            except AssertionError:
                # The logs land after the server is up (their first read); the tour waits for them.
                if time.monotonic() > deadline:
                    print(server.dump_stderr_debug()[-6000:])
                    raise
                time.sleep(5)
        print("\n".join(walked))
    finally:
        if server is not None:
            server.stop_process()
        run.compose("down", "-v", "--remove-orphans")


def _shared(registry, bundles: Path, *args: str) -> subprocess.CompletedProcess:
    """The node's shared stores (packaging/linux/demo-stores/docker-compose.shared.yml), as the
    registry's sync runs them."""
    from tests.integration.demo_registry import _SHARED

    return subprocess.run(
        ["docker", "compose", "-p", registry.stores_project, "-f", str(_SHARED), *args],
        env={**os.environ, "DEMO_BUNDLES_DIR": str(bundles)},
        capture_output=True,
        text=True,
        timeout=300,
        check=True,
    )


def _shared_running(registry) -> set[str]:
    """The shared stores running for the registry's sync."""
    listed = subprocess.run(
        ["docker", "ps", "--filter", f"label=com.docker.compose.project={registry.stores_project}"]
        + ["--format", '{{.Label "com.docker.compose.service"}}'],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.split()
    return set(listed)


def test_a_visitor_sandbox_of_weblogs_reads_the_bundle(published, tmp_path_factory):
    registry, version = published
    work = tmp_path_factory.mktemp("weblogs_tier")
    from tests.integration.isolated_server import IsolatedServer

    # The node's side first: the bundle pulled and offered, and none of its stores started -- the
    # node starts a demo's stores only once the tier asks for it.
    bundles = work / "data" / "demo-bundles"
    synced = registry.sync("weblogs", bundles, requested=False)
    print(synced.stdout, synced.stderr)
    assert synced.returncode == 0, (synced.stdout, synced.stderr)
    assert (bundles / "weblogs" / "pulled").read_text().strip() == version
    assert not (bundles / "weblogs" / "ready").exists()
    assert _shared_running(registry) == set(), "a store started before the demo was asked for"
    project = f"provisa-demo-weblogs-{version}"

    config = work / "config.yaml"
    _deployment_config(config)
    tier = IsolatedServer(
        "weblogstiere2e",
        engine="duckdb",
        config=str(config),
        auth={"provider": "basic", "jwt_secret": _JWT_SECRET, "assignments_source": "provisa"},
        env={
            "PROVISA_SANDBOX_TIER": "true",
            "PROVISA_DEMO_BUNDLES": "weblogs",
            "PROVISA_DEMO_REGISTRY": registry.url,
            "PROVISA_DATA_DIR": str(work / "data"),
            "PROVISA_LOGS_ALLOWED_ROOTS": str(work / "data" / "demo-bundles"),
        },
    )
    tier.start(timeout=300)
    try:
        # The first open asks for the demo: it is being prepared until the node has its stores up.
        first = httpx.post(f"{tier.base_url}/public/demos/weblogs", timeout=600)
        assert first.json().get("code") == "sandbox.demo_not_ready", first.text
        assert (bundles / "weblogs" / "requested").exists()
        # The node's path unit runs the sync when the tier asks (bundles/.wanted): run it here.
        synced = registry.sync("weblogs", bundles, requested=False)
        assert synced.returncode == 0, (synced.stdout, synced.stderr)
        assert (bundles / "weblogs" / "ready").read_text().strip() == version
        stores = (bundles / "weblogs" / version / "stores.env").read_text()
        print(stores)
        suffix = dict(line.split("=", 1) for line in stores.split())["WEBLOGS_PG_NS"]
        assert suffix.startswith("_weblogs_"), stores
        # Its database is the shared Postgres's shop<suffix>, the only store the node started.
        assert _shared_running(registry) == {"postgres"}
        listed = _shared(
            registry,
            bundles,
            *("exec", "-T", "postgres", "psql", "-U", "postgres", "-Atc"),
            "SELECT datname FROM pg_database",
        ).stdout.split()
        assert f"shop{suffix}" in listed, listed
        deadline = time.monotonic() + 600
        while True:
            opened = httpx.post(f"{tier.base_url}/public/demos/weblogs", timeout=600)
            if opened.status_code == 200:
                break
            assert opened.json().get("code") == "sandbox.demo_not_ready", (
                opened.text,
                tier.dump_stderr_debug()[-6000:],
            )
            assert time.monotonic() < deadline, tier.dump_stderr_debug()[-6000:]
            time.sleep(3)
        sandbox = opened.json()
        rows = httpx.post(
            f"{tier.base_url}/data/sql",
            json={
                "sql": 'SELECT path, page_ms FROM "shop"."view_slowest_pages" ORDER BY page_ms DESC LIMIT 5'
            },
            headers={
                "Authorization": f"Bearer {sandbox['token']}",
                "x-org-provisa": sandbox["org_id"],
            },
            timeout=180,
        )
        assert rows.status_code == 200, rows.text
        assert len(rows.json()["data"]["sql"]) == 5, rows.text
        served = (work / "data" / "demo-bundles" / "weblogs" / "ready").read_text().strip()
        assert served == version
    finally:
        tier.stop_process()
        subprocess.run(["docker", "compose", "-p", project, "down", "-v"], check=False)
