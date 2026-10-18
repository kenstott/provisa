# Copyright (c) 2026 Kenneth Stott
# Canary: 7b60c575-2b66-4472-87f3-d73437b1ad70
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: a demo's bundle is built with its data landed in it (scripts/demo_bundle.py build,
REQ-2049).

The ``test`` instance: the script, run as demo-bundles.yml runs it, on a named-demo directory of
this test's own -- one published demo over a SQLite file, which has no data stores to bring up --
copies the demo, starts its own embedded control plane and a sandbox tier in seeding mode, and
packs the copy with its seed. The demo's own directory is left as it was."""

# Requirements: REQ-2049, REQ-2006

from __future__ import annotations

import gzip
import io
import json
import re
import subprocess
import sys
import tarfile
from pathlib import Path

import pytest
import yaml

from tests.integration.test_demo_sandboxes_e2e import _demo

pytestmark = [pytest.mark.integration, pytest.mark.local_platform]

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "demo_bundle.py"


def published_demo(named: Path, name: str, requires: str = "0.1.0") -> Path:
    _demo(named, name, published=True)
    spec = named / name / "demo.yaml"
    spec.write_text(spec.read_text() + f"requires_provisa: {requires}\n")
    return named / name


def replicated(demo: Path) -> Path:
    """``demo`` with its source served from a replica (``replicate: 0``), as a SaaS-account
    source's is: what a bundle's seed carries. A demo's own sources are read live instead."""
    config = yaml.safe_load((demo / "config.yaml").read_text())
    for source in config["sources"]:
        source["replicate"] = 0
        source["change_signal"] = "signal"  # landed once, refreshed only when told
    (demo / "config.yaml").write_text(yaml.safe_dump(config))
    return demo


def build(named: Path, name: str, out: Path, commit: str) -> Path:
    done = subprocess.run(
        [
            sys.executable,
            str(_SCRIPT),
            "build",
            name,
            "--named-dir",
            str(named),
            "--out",
            str(out),
            "--commit",
            commit,
            "--timeout",
            "300",
        ],
        capture_output=True,
        text=True,
        timeout=600,
    )
    # A string, not a tuple: pytest shortens a tuple's repr to a line, which hid the traceback of
    # the build's failing step (CI run 39147851796).
    assert done.returncode == 0, (
        f"build exited {done.returncode}\n--- stdout ---\n{done.stdout[-4000:]}"
        f"\n--- stderr ---\n{done.stderr[-12000:]}"
    )
    (bundle,) = out.glob(f"{name}-*.tar.gz")
    return bundle


def _json(tar: tarfile.TarFile, name: str) -> dict:
    member = tar.extractfile(name)
    assert member is not None, name
    return json.load(member)


def test_a_bundle_holds_its_demo_and_the_seed_landed_from_it(tmp_path):
    named = tmp_path / "named"
    demo = replicated(published_demo(named, "alpha"))
    before = sorted(p.name for p in demo.iterdir())
    commit = "0123456789abcdef0123456789abcdef01234567"
    bundle = build(named, "alpha", tmp_path / "out", commit)

    assert re.fullmatch(r"alpha-\d{12}-012345678\.tar\.gz", bundle.name)
    with tarfile.open(fileobj=io.BytesIO(gzip.decompress(bundle.read_bytes()))) as tar:
        names = set(tar.getnames())
        manifest = _json(tar, "bundle.json")
        record = _json(tar, "seeded/replicas.json")
    assert {"config.yaml", "demo.yaml", "animals.sqlite", "seeded/replicas.duckdb"} <= names
    assert manifest == {
        "commit": commit,
        "demo": "alpha",
        "format": 2,
        "requires_provisa": "0.1.0",
        "version": bundle.name.removeprefix("alpha-").removesuffix(".tar.gz"),
    }
    assert [r["table_name"] for r in record["replicas"]] == ["animals"]
    assert sorted(p.name for p in demo.iterdir()) == before  # built in a copy


def test_a_demo_of_its_own_sources_only_has_no_seed(tmp_path):
    """REQ-2049: a demo's own sources are read live where it runs, so nothing is landed for it."""
    named = tmp_path / "named"
    published_demo(named, "alpha")
    bundle = build(named, "alpha", tmp_path / "out", "1" * 40)
    with tarfile.open(fileobj=io.BytesIO(gzip.decompress(bundle.read_bytes()))) as tar:
        names = set(tar.getnames())
    assert {"config.yaml", "demo.yaml", "animals.sqlite", "bundle.json"} <= names
    assert not [n for n in names if n.startswith("seeded/")]


def test_a_demo_that_does_not_say_which_provisa_it_needs_is_not_built(tmp_path):
    named = tmp_path / "named"
    _demo(named, "alpha", published=True)
    done = subprocess.run(
        [
            sys.executable,
            str(_SCRIPT),
            "build",
            "alpha",
            "--named-dir",
            str(named),
            "--out",
            str(tmp_path),
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert done.returncode != 0
    assert "requires_provisa" in done.stderr
    assert yaml.safe_load((named / "alpha" / "demo.yaml").read_text())["published"] is True
