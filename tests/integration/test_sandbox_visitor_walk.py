# Copyright (c) 2026 Kenneth Stott
# Canary: 2cf56750-7be5-4bf3-af13-69ae6bc33298
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: a visitor's path through each published demo, served from its bundle (REQ-2006, REQ-2049,
REQ-2065).

The ``test`` instance. For each demo whose demo.yaml says ``published: true``: its bundle, built as
demo-bundles.yml builds it, is published to a registry of this test's own; the node's sync pulls it
and brings up its run-time stores; a sandbox tier serving only that demo starts. Then, as the demos
page and the tour do it:

- the visitor opens a sandbox through the public route (``POST /public/demos/<name>``), taking a
  place;
- each tour step that runs something runs as the visitor, with the visitor's token, through the
  transport its page uses, in the role, environment and time travel the steps before it chose (as
  provisa-ui/src/tour/demoTour.ts sets them), and answers with rows (or, reading a materialized
  view outside its read window, with the refusal naming it, as the page shows then);
- the tab closes it (``POST /sandboxes/closing``): once the grace has passed it is deleted, its place
  is free again, and its token reaches nothing.

A demo that cannot be opened, or a step whose query fails or answers nothing, fails its parameter,
by name, before the bundle is published (demo-bundles.yml runs this test on the bundle it built).
"""

# Requirements: REQ-2006, REQ-2049, REQ-2065

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml

from tests.integration.test_demo_sandboxes_e2e import _JWT_SECRET, _deployment_config

# local_platform: the tier's control plane is the local platform's, no shared stack (only the
# demo's own stores run in Docker). demo_walk: each demo's bundle is built and walked on its own, in
# demo-bundles.yml and scripts/test-all's opt-in lane, never in a default lane.
pytestmark = [pytest.mark.integration, pytest.mark.local_platform, pytest.mark.demo_walk]

_REPO = Path(__file__).resolve().parents[2]
_NAMED = _REPO / "demo" / "named"
_SCRIPT = _REPO / "scripts" / "demo_bundle.py"
_CLOUD_TIER_CONFIG = _REPO / "config" / "provisa-install-base.yaml"
#: A directory of bundles already built from this checkout (demo-bundles.yml sets it to the one it
#: just built, so the bundle it publishes is the one walked here). A demo with no bundle there is
#: built by this test, as that workflow builds it.
_BUILT = "PROVISA_TEST_DEMO_BUNDLES_DIR"


def published_demos() -> list[str]:
    return sorted(
        spec.parent.name
        for spec in _NAMED.glob("*/demo.yaml")
        if yaml.safe_load(spec.read_text()).get("published") is True
    )


def _bundle(demo: str, out: Path) -> Path:
    """The demo's bundle: the one built for this run when there is one, else built now."""
    built = os.environ.get(_BUILT)
    if built:
        found = sorted(Path(built).glob(f"{demo}-*.tar.gz"))
        if found:
            assert len(found) == 1, f"{built} holds {len(found)} bundles of {demo}: {found}"
            return found[0]
    done = subprocess.run(
        [sys.executable, str(_SCRIPT), "build", demo, "--out", str(out), "--timeout", "1500"],
        capture_output=True,
        text=True,
        timeout=3600,
    )
    print(done.stdout[-4000:])
    assert done.returncode == 0, (demo, done.stdout[-4000:], done.stderr[-8000:])
    (bundle,) = out.glob(f"{demo}-*.tar.gz")
    return bundle


@pytest.fixture(params=published_demos())
def tier(request, tmp_path_factory):
    """A sandbox tier serving the demo ``request.param`` from its bundle, as the cloud's node does:
    the bundle in a registry of this test's own, pulled by the node's sync, its stores up."""
    from tests.integration.demo_registry import DemoRegistry
    from tests.integration.isolated_server import IsolatedServer

    demo = request.param
    work = tmp_path_factory.mktemp(f"visitor_{demo}")
    bundle = _bundle(demo, work / "built")
    version = bundle.name.removeprefix(f"{demo}-").removesuffix(".tar.gz")
    registry = DemoRegistry()
    registry.start()
    project = f"provisa-demo-{demo}-{version}"
    server = None
    try:
        print(registry.push(bundle))
        bundles = work / "data" / "demo-bundles"
        synced = registry.sync(demo, bundles)
        print(synced.stdout, synced.stderr)
        assert synced.returncode == 0, (demo, synced.stdout, synced.stderr)
        assert (bundles / demo / "ready").read_text().strip() == version
        config = work / "config.yaml"
        _deployment_config(config)
        # The deployment's naming is the one every org's schema is named by: the cloud's sandbox
        # tier runs on config/provisa-install-base.yaml (first-launch.sh), whose domain prefix the
        # demos' example queries and Cypher labels are written against (c__viewChurnFeatures).
        served = yaml.safe_load(config.read_text())
        served["naming"] = yaml.safe_load(_CLOUD_TIER_CONFIG.read_text())["naming"]
        config.write_text(yaml.safe_dump(served))
        server = IsolatedServer(
            f"visitor{demo}",
            engine="duckdb",
            config=str(config),
            auth={"provider": "basic", "jwt_secret": _JWT_SECRET, "assignments_source": "provisa"},
            env={
                "PROVISA_SANDBOX_TIER": "true",
                "PROVISA_DEMO_BUNDLES": demo,
                "PROVISA_DEMO_REGISTRY": registry.url,
                "PROVISA_DATA_DIR": str(work / "data"),
                "PROVISA_LOGS_ALLOWED_ROOTS": str(bundles),
            },
        )
        server.demo = demo
        server.directory = bundles / demo / version
        server.start(timeout=600)
        yield server
    finally:
        if server is not None:
            server.stop_process()
        subprocess.run(["docker", "compose", "-p", project, "down", "-v"], check=False)
        registry.stop()


def _open_sandbox(tier) -> dict:
    """What the demos page sends when a visitor opens the demo: a sandbox of the visitor's own,
    once its template is ready."""
    deadline = time.monotonic() + 1800
    while True:
        opened = httpx.post(f"{tier.base_url}/public/demos/{tier.demo}", timeout=600)
        if opened.status_code == 200:
            return opened.json()
        if not (
            opened.status_code == 503 and opened.json().get("code") == "sandbox.demo_not_ready"
        ):
            raise _failed(tier, f"opening it answered {opened.status_code}: {opened.text}")
        if _status(tier)["state"] == "failed":
            raise _failed(tier, "its template could not be prepared")
        if time.monotonic() > deadline:
            raise _failed(tier, "its template was never ready")
        time.sleep(5)


def _failed(tier, why: str) -> AssertionError:
    """A failure of ``tier``'s demo, its server's whole log kept beside the run's results (the
    workflow uploads PROVISA_TEST_LOG_DIR) and its end in the message, unshortened."""
    text = tier.dump_stderr_debug()
    log = Path(os.environ.get("PROVISA_TEST_LOG_DIR", tier.directory.parent)) / (
        f"visitor-walk-{tier.demo}-server.log"
    )
    log.parent.mkdir(parents=True, exist_ok=True)
    log.write_text(text)
    return AssertionError(f"{tier.demo}: {why}\nserver log {log}; its end:\n{text[-6000:]}")


def _status(tier) -> dict:
    status = httpx.get(f"{tier.base_url}/public/demos/{tier.demo}/status", timeout=60)
    assert status.status_code == 200, status.text
    return status.json()


def _places_free(tier) -> int:
    return _status(tier)["places_free"]


def _all_roles(tier, token: dict[str, str]) -> str:
    """The visitor's "Role: All", as the page's role picker sends it (AuthContext.tsx): every role
    the visitor is assigned, those reaching only the control plane last, the rest by id."""
    me = httpx.get(f"{tier.base_url}/auth/me", headers=token, timeout=60)
    assert me.status_code == 200, me.text
    assigned = {a["role_id"] for a in me.json()["assignments"]}
    listed = httpx.post(
        f"{tier.base_url}/admin/graphql",
        json={"query": "{ roles { id capabilities } }"},
        headers=token,
        timeout=60,
    )
    assert listed.status_code == 200 and not listed.json().get("errors"), listed.text
    roles = [r for r in listed.json()["data"]["roles"] if r["id"] in assigned]
    assert roles, (assigned, listed.text)
    roles.sort(key=lambda r: ("cross_org" in r["capabilities"], r["id"]))
    return ",".join(r["id"] for r in roles)


def _answered(what: str, resp: httpx.Response, sql: str | None = None) -> tuple[str, Any]:
    """A step's request, answered as its page shows it: with rows, or a spec or schema -- or, for a
    step reading a materialized view outside its read window, the refusal naming that view, which
    is what the page shows then (REQ-2066; the provisioning tour's close step)."""
    if resp.status_code == 503 and sql is not None:
        refused = resp.json()
        view = refused.get("params", {}).get("view")
        if refused.get("code") == "data.view_outside_load_window" and view and view in sql:
            return f"{what}: refused outside {view}'s read window", None
    assert resp.status_code == 200, (what, resp.status_code, resp.text[:2000])
    body = resp.json()
    if isinstance(body, dict) and "data" in body:
        assert not body.get("errors"), (what, resp.text[:2000])
        data = body["data"]
        if isinstance(data, list):  # a JSON:API document: its resources
            assert data, (what, resp.text[:2000])
            return f"{what}: {len(data)} rows", data
        assert data and all(data.values()), (what, resp.text[:2000])
        counts = [len(v) if isinstance(v, list) else 1 for v in data.values()]
        return f"{what}: {sum(counts)} rows", data
    assert body, (what, resp.text[:2000])
    return f"{what}: {len(body) if isinstance(body, list) else 'answered'}", body


class Walk:
    """The visitor's page state through the tour, as demoTour.ts keeps it: the page the tour is on,
    the role the app acts as (the header's picker), the SQL explorer's run-as role and time travel,
    and the environment every request after an ``env`` step is served from."""

    def __init__(self, tier, token: dict[str, str]) -> None:
        self.tier = tier
        self.token = token
        self.app_role = _all_roles(tier, token)
        self.all_roles = self.app_role
        self.sql_role: str | None = None
        self.page: str | None = None
        self.env: str | None = None
        self.as_of: str | None = None
        self.as_recorded: str | None = None
        self.answers: dict[str, list[dict]] = {}

    def _role(self, chosen: str) -> str:
        return self.all_roles if chosen == "all" else chosen

    def choose(self, step: dict) -> None:
        self.page = step.get("route", self.page)
        if "role" in step:
            if "," in step["role"] or self.page == "/sql":
                self.sql_role = self._role(step["role"])
            else:
                self.app_role = self._role(step["role"])
        if "env" in step:
            self.env = step["env"]
        if "as_of" in step:
            self.as_of = step["as_of"]
        if "as_recorded" in step:
            self.as_recorded = step["as_recorded"]
        if "as_recorded_from" in step:
            taken = step["as_recorded_from"]
            self.as_recorded = str(self.answers[taken["step"]][taken["row"]][taken["column"]])

    def headers(self, *, sql: bool) -> dict[str, str]:
        sent = {
            **self.token,
            "X-Provisa-Role": (self.sql_role or self.app_role) if sql else self.app_role,
        }
        if self.env is not None:
            sent["x-provisa-env"] = self.env
        if sql and self.as_of is not None:
            sent["X-Provisa-As-Of"] = self.as_of
        if sql and self.as_recorded is not None:
            sent["X-Provisa-As-Recorded"] = self.as_recorded
        return sent

    def send(
        self, what: str, method: str, path: str, body: dict | None, *, sql: str | None = None
    ) -> Any:
        # A step reading what the sandbox lands after it opens (a demo's log files, first read on
        # their first query) waits for it, as the visitor's page does.
        deadline = time.monotonic() + 300
        while True:
            resp = httpx.request(
                method,
                f"{self.tier.base_url}{path}",
                json=body,
                headers=self.headers(sql=sql is not None),
                timeout=300,
            )
            try:
                said, answer = _answered(what, resp, sql)
                self.walked.append(said)
                return answer
            except AssertionError as unanswered:
                if time.monotonic() > deadline:
                    raise _failed(self.tier, str(unanswered)) from unanswered
                time.sleep(5)

    def run(self, directory: Path) -> list[str]:
        from provisa.api.demo_warmup import _EXPLORERS, _is_subscription
        from provisa.core.demo import demo_tour, example_queries

        examples = {q["name"]: q["query"] for q in example_queries(directory)}
        self.walked: list[str] = []
        for step in demo_tour(directory):
            self.choose(step)
            key = step["key"]
            if "sql" in step:
                answer = self.send(
                    f"{key} (sql)", "POST", "/data/sql", {"sql": step["sql"]}, sql=step["sql"]
                )
                if answer is not None:  # None: refused outside a view's read window (_answered)
                    self.answers[key] = answer["sql"]
            if "cypher" in step:
                self.send(f"{key} (cypher)", "POST", "/data/cypher", {"query": step["cypher"]})
            if "example" in step and not _is_subscription(examples[step["example"]]):
                query = {"query": examples[step["example"]]}
                self.send(f"{key} (graphql)", "POST", "/data/graphql", query)
            for kind in ("api", "jsonapi"):
                if kind in step:
                    method, _, path = (
                        step[kind].partition(" ") if kind == "api" else ("GET", "", step[kind])
                    )
                    self.send(f"{key} ({kind})", method, path, None)
        for method, path, body in _EXPLORERS:
            self.send(path, method, path, body)
        return self.walked


def test_a_visitor_opens_the_demo_walks_its_tour_and_closes_it(tier):
    from provisa.api.demo_sandbox import CLOSE_GRACE_S

    free = _places_free(tier)
    sandbox = _open_sandbox(tier)
    assert _places_free(tier) == free - 1
    token = {"Authorization": f"Bearer {sandbox['token']}", "x-org-provisa": sandbox["org_id"]}

    walked = Walk(tier, token).run(tier.directory)
    print("\n".join(walked))
    from provisa.core.demo import demo_tour

    steps = {s["key"] for s in demo_tour(tier.directory)}
    spec = yaml.safe_load((tier.directory / "demo.yaml").read_text())
    if spec.get("tour") == "core":
        # Toured by the product's core tour (no tour.yaml of its own): the explorers are walked.
        assert not steps, f"{tier.demo} is toured by the core tour and has a tour of its own"
    else:
        ran = any(line.split(" ", 1)[0] in steps for line in walked)
        assert ran, f"{tier.demo}'s tour runs nothing"

    # What sandboxClosing.ts sends as the visitor's tab unloads.
    closed = httpx.post(f"{tier.base_url}/sandboxes/closing", headers=token, timeout=60)
    assert closed.status_code == 204, closed.text
    # Deleted once the grace has passed with no request to it (the reaper runs each minute).
    deadline = time.monotonic() + CLOSE_GRACE_S + 180
    while _places_free(tier) != free:
        if time.monotonic() > deadline:
            raise _failed(tier, "the closed sandbox was not deleted")
        time.sleep(10)
    gone = httpx.post(
        f"{tier.base_url}/data/sql", json={"sql": "SELECT 1"}, headers=token, timeout=60
    )
    assert 400 <= gone.status_code < 500, gone.text  # refused, not served and not a crash
