# Copyright (c) 2026 Kenneth Stott
# Canary: d2f23db9-9151-4b04-80a9-935588ceaa6f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: a demo's Test (synthetic) environment is generated once and carried like its replicas
(REQ-1942, REQ-2053).

Everything here is the ``test`` instance on the local test platform. The demo's setup.yaml declares
dev in Test (synthetic). A seeding run of the sandbox tier (the bundle build's step) builds the
demo's template: its prod is profiled, dev is generated from those runs, and the generated rows and
their records -- the dataset, the generation and its report -- are written beside the replicas'
seed. The demo's source is then deleted. A sandbox tier started afterwards adopts the template's
dev from the seed, and a visitor's sandbox is given it: its dev reads generated rows, none of them
a real name, and carries the report comparing them with the profile they were drawn from."""

# Requirements: REQ-1942, REQ-2053

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path

import httpx
import pytest
import yaml

from tests.integration.test_demo_sandboxes_e2e import _JWT_SECRET, _deployment_config

pytestmark = [pytest.mark.integration, pytest.mark.local_platform]

_ORG = "synthenve2e"
_SPECIES = ("cat", "dog", "owl")
_NAMES = [f"Animal {n:02d}" for n in range(1, 41)]
_PROFILER = {
    "cron": "0 2 * * *",
    "low_cardinality_max": 100,
    "drift_window": 7,
    "drift_season": "none",
    "drift_distance": 3.0,
    "drift_slope": 3.0,
    "drift_ks": 0.2,
    "drift_psi": 0.25,
    "correlation_max_columns": 20,
    "joint_max_distinct": 20,
    "category_max_columns": 10,
    "classification_sample": 500,
}


def _demo(root: Path) -> Path:
    folder = root / "gamma"
    folder.mkdir(parents=True)
    db = sqlite3.connect(folder / "animals.sqlite")
    db.execute(
        "CREATE TABLE animals (id INTEGER PRIMARY KEY, name TEXT, species TEXT, age INTEGER)"
    )
    db.executemany(
        "INSERT INTO animals VALUES (?, ?, ?, ?)",
        [(i, name, _SPECIES[i % 3], 1 + i % 12) for i, name in enumerate(_NAMES, start=1)],
    )
    db.commit()
    db.close()
    (folder / "demo.yaml").write_text(
        yaml.safe_dump({"title": "The gamma demo", "description": "gamma", "published": True})
    )
    readers = ["sandbox", "org_admin"]
    config = {
        "federation_engine": "duckdb",
        "auth": {"provider": "none", "assignments_source": "provisa"},
        "naming": {"domain_prefix": False, "rules": []},
        "domains": [{"id": "zoo", "description": "animals"}],
        "roles": [],
        "sources": [
            {
                "id": "zoo-db",
                "type": "sqlite",
                "path": "${env:PROVISA_NAMED_DEMO_DIR}/animals.sqlite",
                # Served from its replica, as a bundle's seed carries one: the source is deleted
                # once the release has run.
                "replicate": 0,
                "change_signal": "signal",
            },
            {"id": "zoo-profiler", "type": "data_profiler", "mapping": _PROFILER},
        ],
        "tables": [
            {
                "source_id": "zoo-db",
                "domain_id": "zoo",
                "schema": "main",
                "table": "animals",
                "profiler_source_id": "zoo-profiler",
                "columns": [
                    {
                        "name": "id",
                        "data_type": "integer",
                        "is_primary_key": True,
                        "visible_to": readers,
                    },
                    {
                        "name": "name",
                        "data_type": "varchar",
                        "fake": "name()",
                        "visible_to": readers,
                    },
                    {"name": "species", "data_type": "varchar", "visible_to": readers},
                    {"name": "age", "data_type": "integer", "visible_to": readers},
                ],
            }
        ],
        "relationships": [],
    }
    (folder / "config.yaml").write_text(yaml.safe_dump(config))
    (folder / "setup.yaml").write_text(
        yaml.safe_dump({"steps": [{"environment": {"name": "dev", "data_mode": "test_synthetic"}}]})
    )
    return folder


def _tier(work: Path, demos: Path, data: Path, *, seeding: bool):
    from tests.integration.isolated_server import IsolatedServer

    config = work / "config.yaml"
    _deployment_config(config)
    return IsolatedServer(
        _ORG,
        engine="duckdb",
        config=str(config),
        auth={"provider": "basic", "jwt_secret": _JWT_SECRET, "assignments_source": "provisa"},
        env={
            "PROVISA_SANDBOX_TIER": "true",
            "PROVISA_NAMED_DEMOS_DIR": str(demos),
            "PROVISA_DATA_DIR": str(data),
            "PROVISA_DEMO_SEED_EXPORT": "true" if seeding else "false",
        },
    )


def _open(tier) -> dict:
    deadline = time.monotonic() + 300
    while True:
        opened = httpx.post(f"{tier.base_url}/public/demos/gamma", timeout=240)
        if opened.status_code == 200:
            return opened.json()
        assert opened.json().get("code") == "sandbox.demo_not_ready", (
            opened.text,
            tier.dump_stderr_debug()[-8000:],
        )
        assert time.monotonic() < deadline, tier.dump_stderr_debug()[-8000:]
        time.sleep(2)


def _headers(sandbox: dict, env: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {sandbox['token']}",
        "x-org-provisa": sandbox["org_id"],
        "x-provisa-env": env,
    }


def test_a_synthetic_environment_is_generated_once_and_given_to_each_sandbox(tmp_path_factory):
    work = tmp_path_factory.mktemp("demo_synthetic_env")
    demos = work / "named"
    folder = _demo(demos)
    record = folder / "seeded" / "synthetic.json"

    release = _tier(work, demos, work / "release-data", seeding=True)
    release.start(timeout=240)
    try:
        deadline = time.monotonic() + 300
        while not record.exists():
            assert time.monotonic() < deadline, release.dump_stderr_debug()[-8000:]
            time.sleep(2)
    finally:
        release.stop_process()
    held = json.loads(record.read_text())["environments"]["dev"]
    [dataset] = held["records"]["synthetic_datasets"]
    assert (dataset["status"], dataset["report_error"]) == ("generated", None), dataset
    assert held["records"]["synthetic_report"], "the generation's report is carried"
    assert (folder / "seeded" / "synthetic.duckdb").is_file()

    (folder / "animals.sqlite").unlink()  # nothing after the release reads the source

    deployed = _tier(work, demos, work / "deployed-data", seeding=False)
    deployed.start(timeout=240)
    try:
        sandbox = _open(deployed)
        rows = httpx.post(
            f"{deployed.base_url}/data/sql",
            json={"sql": "SELECT name, species, age FROM animals"},
            headers=_headers(sandbox, "dev"),
            timeout=120,
        )
        assert rows.status_code == 200, (rows.text, deployed.dump_stderr_debug()[-8000:])
        generated = rows.json()["data"]["sql"]
        assert generated
        assert not {r["name"] for r in generated} & set(_NAMES), "no real name is in dev"
        assert len(generated) == len(_NAMES), "as many rows as production's profile counted"
        assert not {r["species"] for r in generated} & set(_SPECIES), "no real value is in dev"
        report = httpx.get(
            f"{deployed.base_url}/admin/synthetic-datasets/model/report",
            headers=_headers(sandbox, "dev"),
            timeout=120,
        )
        assert report.status_code == 200, report.text
        assert report.json(), "the sandbox's dev carries the report of its generation"
    finally:
        deployed.stop_process()
