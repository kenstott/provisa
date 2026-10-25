# Copyright (c) 2026 Kenneth Stott
# Canary: db59ba60-c05d-4604-a1f1-851c1783ee89
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: the sandbox tier serves each demo's published bundle, a newer one with no redeploy
(REQ-2049).

The ``test`` instance: a registry of this test's own (tests/integration/demo_registry.py), the
node's sync (scripts/demo_stores_sync.sh) run here as the node runs it, and a sandbox tier serving
the demo ``alpha`` (PROVISA_DEMO_BUNDLES). The sync pulls a published version and marks it ready;
a visitor's sandbox reads the demo's own source live, from that version's bundle. A version
published while the tier runs is asked for when the next sandbox is opened -- which is served the
ready version meanwhile -- and served from the first sandbox opened after the sync has it ready,
while a sandbox opened before keeps reading its own version."""

# Requirements: REQ-2049, REQ-2006

from __future__ import annotations

import sqlite3
import subprocess
import time

import httpx
import pytest

from tests.integration.test_demo_bundle_build_e2e import build, published_demo
from tests.integration.test_demo_sandboxes_e2e import _JWT_SECRET, _deployment_config

pytestmark = [pytest.mark.integration, pytest.mark.local_platform]

_ORG = "bundletiere2e"


@pytest.fixture(scope="module")
def registry():
    from tests.integration.demo_registry import DemoRegistry

    reg = DemoRegistry()
    reg.start()
    yield reg
    reg.stop()


def _publish(registry, named, out, commit: str, rows: list[str]) -> str:
    """Build alpha with ``rows`` and publish it; the version published."""
    source = named / "alpha" / "animals.sqlite"
    db = sqlite3.connect(source)
    db.execute("DELETE FROM animals")
    db.executemany("INSERT INTO animals VALUES (?, ?)", list(enumerate(rows, start=1)))
    db.commit()
    db.close()
    bundle = build(named, "alpha", out, commit)
    registry.push(bundle)
    return bundle.name.removeprefix("alpha-").removesuffix(".tar.gz")


def _sync(work, registry) -> subprocess.CompletedProcess:
    """The node's sync, for the demo alpha, into the tier's data directory."""
    return registry.sync("alpha", work / "data" / "demo-bundles")


def _synced(work, registry) -> str:
    done = _sync(work, registry)
    assert done.returncode == 0, (done.stdout, done.stderr)
    return (work / "data" / "demo-bundles" / "alpha" / "ready").read_text().strip()


def _with_a_source_of_its_own(config, work) -> None:
    """The tier's deployment configuration with a source and table of its own, as cloud.provisa.dev's
    has (provisa-install.yaml's dq-checker and pet store): a demo org on the tier is described by its
    own registry, never by this file -- a template's replica build once walked this file's sources
    against the template's catalog names and failed (KeyError: 'dq-checker')."""
    import sqlite3

    import yaml

    db = sqlite3.connect(work / "deployment.sqlite")
    db.execute("CREATE TABLE IF NOT EXISTS notes (id INTEGER PRIMARY KEY, body TEXT)")
    db.commit()
    db.close()
    raw = yaml.safe_load(config.read_text())
    raw["sources"] = [
        {"id": "deploy-db", "type": "sqlite", "path": str(work / "deployment.sqlite")},
        {
            # The demo deployment's quality-scan source (provisa-install.yaml), of a type a store
            # registers without an engine catalog.
            "id": "dq-checker",
            "type": "great_expectations",
            "mapping": {
                "database": "provisa",
                "host": "localhost",
                "password": "provisa",
                "port": 5439,
                "user": "org_admin",
            },
        },
    ]
    raw["tables"] = [
        {
            "source_id": "deploy-db",
            "domain_id": "ops",
            "schema": "main",
            "table": "notes",
            "columns": [
                {"name": "id", "data_type": "integer", "visible_to": ["org_admin"]},
                {"name": "body", "data_type": "varchar", "visible_to": ["org_admin"]},
            ],
        }
    ]
    config.write_text(yaml.safe_dump(raw))


def _tier(work, registry):
    from tests.integration.isolated_server import IsolatedServer

    config = work / "config.yaml"
    _deployment_config(config)
    _with_a_source_of_its_own(config, work)
    return IsolatedServer(
        _ORG,
        engine="duckdb",
        config=str(config),
        auth={"provider": "basic", "jwt_secret": _JWT_SECRET, "assignments_source": "provisa"},
        env={
            "PROVISA_SANDBOX_TIER": "true",
            "PROVISA_DEMO_BUNDLES": "alpha",
            "PROVISA_DEMO_REGISTRY": registry.url,
            "PROVISA_DATA_DIR": str(work / "data"),
        },
    )


_opened: list[str] = []


@pytest.fixture(autouse=True)
def _close_what_the_test_opened():
    """The session's control plane is shared by every module's tier: a sandbox left open here
    counts against the next tier's sandbox.max_concurrent. Each test's are removed after it."""
    yield
    import os

    from sqlalchemy import create_engine, delete

    from provisa.core.schema_admin import demo_sandboxes

    if not _opened:
        return
    engine = create_engine(os.environ["PLATFORM_DATABASE_URL"])
    try:
        with engine.begin() as conn:
            conn.execute(delete(demo_sandboxes).where(demo_sandboxes.c.org_id.in_(_opened)))
    finally:
        engine.dispose()
        _opened.clear()


def _open(tier) -> dict:
    deadline = time.monotonic() + 240
    while True:
        opened = httpx.post(f"{tier.base_url}/public/demos/alpha", timeout=300)
        if opened.status_code == 200:
            _opened.append(opened.json()["org_id"])
            return opened.json()
        assert opened.json().get("code") == "sandbox.demo_not_ready", (
            opened.text,
            tier.dump_stderr_debug()[-6000:],
        )
        assert time.monotonic() < deadline, tier.dump_stderr_debug()[-6000:]
        time.sleep(2)


def _names(tier, sandbox: dict) -> list[str]:
    rows = httpx.post(
        f"{tier.base_url}/data/sql",
        json={"sql": "SELECT name FROM animals"},
        headers={"Authorization": f"Bearer {sandbox['token']}", "x-org-provisa": sandbox["org_id"]},
        timeout=120,
    )
    assert rows.status_code == 200, rows.text
    return sorted(r["name"] for r in rows.json()["data"]["sql"])


def test_a_newer_version_is_asked_for_served_once_ready_and_an_open_sandbox_keeps_its_own(
    tmp_path_factory, registry
):
    work = tmp_path_factory.mktemp("bundle_tier")
    named = work / "named"
    published_demo(named, "alpha")
    first = _publish(registry, named, work / "out1", "a" * 40, ["ant", "ape"])
    assert _synced(work, registry) == first

    tier = _tier(work, registry)
    tier.start(timeout=240)
    try:
        before = _open(tier)
        assert _names(tier, before) == ["ant", "ape"]

        published_demo(work / "named2", "alpha")
        second = _publish(registry, work / "named2", work / "out2", "b" * 40, ["asp", "auk", "ant"])
        assert second != first

        meanwhile = _open(tier)  # asks for the demo, and is served the ready version
        assert _names(tier, meanwhile) == ["ant", "ape"]
        bundles = work / "data" / "demo-bundles"
        # The tier asks only; the node's sync pulls the latest published (f87058c07).
        assert (bundles / "alpha" / "requested").read_text().strip() == "requested"
        assert not (bundles / "alpha" / "wanted").exists()
        assert (bundles / ".wanted").exists()  # what the node's path unit watches

        assert _synced(work, registry) == second
        after = _open(tier)
        assert _names(tier, after) == ["ant", "asp", "auk"]
        assert _names(tier, before) == ["ant", "ape"]  # opened before: its own version, live
        in_use = set((bundles / "alpha" / "in-use").read_text().split())
        assert in_use == {first, second}
    finally:
        tier.stop_process()


def test_a_bundle_that_needs_a_newer_provisa_is_refused_by_name(tmp_path_factory, registry):
    work = tmp_path_factory.mktemp("bundle_newer")
    named = work / "named"
    published_demo(named, "alpha", requires="99.0.0")
    bundle = build(named, "alpha", work / "out", "c" * 40)
    registry.push(bundle)

    pulled = registry.pull("alpha", work / "demos")
    assert pulled.returncode == 2
    assert "needs Provisa 99.0.0 or newer; this is Provisa" in pulled.stderr
    assert not (work / "demos" / "alpha").exists() or not any((work / "demos" / "alpha").iterdir())
    synced = _sync(work, registry)  # the node refuses it too, and marks nothing ready
    assert synced.returncode != 0
    assert "needs Provisa 99.0.0 or newer" in synced.stderr
    assert not (work / "data" / "demo-bundles" / "alpha" / "ready").exists()


def test_a_sandbox_is_shown_the_example_queries_of_the_bundle_it_was_opened_from(
    tmp_path_factory, registry
):
    """REQ-2006/REQ-2049: the demo's example queries ride on /setup/status (REQ-2009). The sandbox
    tier serves many demos, so it answers with those of the sandbox's own demo, from its bundle."""
    work = tmp_path_factory.mktemp("bundle_examples")
    named = work / "named"
    demo = published_demo(named, "alpha")
    (demo / "queries").mkdir()
    (demo / "queries" / "1-animals.graphql").write_text(
        "# Every animal\nquery { animals { name } }\n"
    )
    _publish(registry, named, work / "out", "d" * 40, ["ant", "ape"])
    _synced(work, registry)
    tier = _tier(work, registry)
    tier.start(timeout=240)
    try:
        sandbox = _open(tier)
        status = httpx.get(
            f"{tier.base_url}/setup/status",
            headers={
                "Authorization": f"Bearer {sandbox['token']}",
                "x-org-provisa": sandbox["org_id"],
            },
            timeout=60,
        )
        assert status.status_code == 200, status.text
        body = status.json()
        assert body["named_demo"] == "alpha", body
        assert body["example_queries"] == [
            {"name": "1-animals", "query": "# Every animal\nquery { animals { name } }\n"}
        ]
    finally:
        tier.stop_process()


def test_a_visitor_reads_what_the_demo_shows_its_admin_and_runs_as_its_own_roles(
    tmp_path_factory, registry
):
    """A demo names its own roles (org_admin, a keeper), never the sandbox role. Its visitor holds
    the sandbox role, which sees what org_admin sees (REQ-1597), and the demo's own roles, so its
    tour can run as each (weblogs: analyst; test_weblogs_bundle_e2e, run 38596957161)."""
    import yaml

    work = tmp_path_factory.mktemp("bundle_roles")
    named = work / "named"
    demo = published_demo(named, "alpha")
    config = yaml.safe_load((demo / "config.yaml").read_text())
    config["roles"] = [
        {
            "description": "Test role.",
            "id": "keeper",
            "capabilities": ["query_development"],
            "domain_access": ["zoo"],
        }
    ]
    for column in config["tables"][0]["columns"]:
        column["visible_to"] = (
            ["org_admin", "keeper"] if column["name"] == "name" else ["org_admin"]
        )
    (demo / "config.yaml").write_text(yaml.safe_dump(config))
    _publish(registry, named, work / "out", "e" * 40, ["ant", "ape"])
    _synced(work, registry)
    tier = _tier(work, registry)
    tier.start(timeout=240)
    try:
        sandbox = _open(tier)
        assert _names(tier, sandbox) == ["ant", "ape"]
        as_keeper = httpx.post(
            f"{tier.base_url}/data/sql",
            json={"sql": "SELECT name FROM animals"},
            headers={
                "Authorization": f"Bearer {sandbox['token']}",
                "x-org-provisa": sandbox["org_id"],
                "X-Provisa-Role": "keeper",
            },
            timeout=120,
        )
        assert as_keeper.status_code == 200, as_keeper.text
        assert sorted(r["name"] for r in as_keeper.json()["data"]["sql"]) == ["ant", "ape"]
        as_admin = httpx.post(
            f"{tier.base_url}/data/sql",
            json={"sql": "SELECT name FROM animals"},
            headers={
                "Authorization": f"Bearer {sandbox['token']}",
                "x-org-provisa": sandbox["org_id"],
                "X-Provisa-Role": "org_admin",
            },
            timeout=120,
        )
        assert as_admin.status_code == 403, as_admin.text  # REQ-1597: never org_admin itself
    finally:
        tier.stop_process()


def test_a_server_started_on_a_bundle_in_an_older_format_refuses_it_by_name(tmp_path):
    """The maintainer's local-dev: started on weblogs 202610141838-c48b72e45, built before tour.yaml
    named a landing, every /setup/status failed (KeyError 'landing') and the UI never loaded. Such
    a bundle is refused when the server starts, by name, saying to pull the latest."""
    import json

    import yaml

    from tests.integration.isolated_server import IsolatedServer
    from tests.integration.test_demo_sandboxes_e2e import _demo

    _demo(tmp_path / "named", "weblogs", published=True)
    old = tmp_path / "named" / "weblogs"
    (old / "bundle.json").write_text(
        json.dumps(
            {
                "demo": "weblogs",
                "version": "202610141838-c48b72e45",
                "commit": "c" * 40,
                "requires_provisa": "0.1.0",
            }
        )
    )
    (old / "tour.yaml").write_text(yaml.safe_dump({"steps": []}))
    server = IsolatedServer(
        "oldformate2e",
        engine="duckdb",
        config=str(old / "config.yaml"),
        control_plane="own",
        env={"PROVISA_NAMED_DEMO_DIR": str(old), "PROVISA_DATA_DIR": str(tmp_path / "data")},
    )
    with pytest.raises(Exception):  # noqa: B017 - the start fails; what it says is checked below
        server.start(timeout=120)
    try:
        log = server.dump_stderr_debug()
        assert (
            "demo weblogs 202610141838-c48b72e45 is in demo format 1; this Provisa reads format 2: "
            "pull its latest (start-ui-install.sh --demo weblogs)"
        ) in log, log[-4000:]
    finally:
        server.stop_process()
