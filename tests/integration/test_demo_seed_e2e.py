# Copyright (c) 2026 Kenneth Stott
# Canary: ace50662-27bd-487d-8006-d261dfc070d9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: a demo is seeded when its bundle is built, and a deployment needs none of its sources
(REQ-2006).

Everything here is the ``test`` instance on the local test platform. A seeding run of the sandbox
tier (``PROVISA_DEMO_SEED_EXPORT=true``, the bundle build's step) lands the demo's template from
its own source -- a SQLite file -- and writes the demo's seed beside its demo.yaml. The source is
then deleted. A sandbox tier started afterwards, on a data directory of its own (a new
deployment's volume), lands the template from the seed, and a visitor's sandbox reads the demo's
rows: nothing read the source, which no longer exists."""

# Requirements: REQ-2006

from __future__ import annotations

import json
import time

import httpx
import pytest

from tests.integration.test_demo_bundle_build_e2e import replicated
from tests.integration.test_demo_sandboxes_e2e import _JWT_SECRET, _ROWS, _demo, _deployment_config

pytestmark = [pytest.mark.integration, pytest.mark.local_platform]

_ORG = "seedboote2e"


def _tier(work, demos, data, *, seeding: bool):
    from tests.integration.isolated_server import IsolatedServer

    config = work / "config.yaml"
    _deployment_config(config)
    return IsolatedServer(
        _ORG,
        engine="duckdb",
        config=str(config),
        # Each tier its own control plane: the release's build and the new deployment share
        # nothing but the demo's directory. A template another test recorded ready in the shared
        # plane, under the same demo name, was served to them as theirs (release 510).
        control_plane="own",
        auth={"provider": "basic", "jwt_secret": _JWT_SECRET, "assignments_source": "provisa"},
        env={
            "PROVISA_SANDBOX_TIER": "true",
            "PROVISA_NAMED_DEMOS_DIR": str(demos),
            "PROVISA_DATA_DIR": str(data),
            "PROVISA_DEMO_SEED_EXPORT": "true" if seeding else "false",
        },
    )


def test_a_demo_seeded_at_release_serves_its_sandboxes_with_its_source_gone(tmp_path_factory):
    work = tmp_path_factory.mktemp("demo_seed")
    demos = work / "named"
    _demo(demos, "alpha", published=True)
    replicated(demos / "alpha")  # what a seed carries: a source served from its replica
    record = demos / "alpha" / "seeded" / "replicas.json"

    release = _tier(work, demos, work / "release-data", seeding=True)
    release.start(timeout=240)
    try:
        deadline = time.monotonic() + 180
        while not record.exists():
            assert time.monotonic() < deadline, release.dump_stderr_debug()[-6000:]
            time.sleep(2)
    finally:
        release.stop_process()
    seed = json.loads(record.read_text())
    assert [r["table_name"] for r in seed["replicas"]] == ["animals"]
    assert (demos / "alpha" / "seeded" / "replicas.duckdb").is_file()

    (demos / "alpha" / "animals.sqlite").unlink()  # the source is gone

    deployed = _tier(work, demos, work / "deployed-data", seeding=False)
    deployed.start(timeout=240)
    try:
        deadline = time.monotonic() + 180
        while True:
            opened = httpx.post(f"{deployed.base_url}/public/demos/alpha", timeout=240)
            if opened.status_code == 200:
                break
            assert opened.json().get("code") == "sandbox.demo_not_ready", (
                opened.text,
                deployed.dump_stderr_debug(),
            )
            assert time.monotonic() < deadline, deployed.dump_stderr_debug()[-6000:]
            time.sleep(2)
        sandbox = opened.json()
        rows = httpx.post(
            f"{deployed.base_url}/data/sql",
            json={"sql": "SELECT name FROM animals"},
            headers={
                "Authorization": f"Bearer {sandbox['token']}",
                "x-org-provisa": sandbox["org_id"],
            },
            timeout=120,
        )
        assert rows.status_code == 200, rows.text
        assert sorted(r["name"] for r in rows.json()["data"]["sql"]) == sorted(_ROWS["alpha"])
    finally:
        deployed.stop_process()
