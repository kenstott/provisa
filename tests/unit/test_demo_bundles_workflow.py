# Copyright (c) 2026 Kenneth Stott
# Canary: d0dcca86-a4a1-43e2-b4e2-8369385404ac
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""demo-bundles.yml: a demo changed on main is rebuilt and published alone, and nothing of it
goes into a release (REQ-2049)."""

# Requirements: REQ-2049

from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS = ROOT / ".github" / "workflows"


def _load(name: str) -> dict:
    return yaml.safe_load((WORKFLOWS / name).read_text())


def test_a_push_to_main_touching_a_demo_or_a_dispatch_naming_one_starts_it():
    on = _load("demo-bundles.yml")[True]
    assert on["push"] == {"branches": ["main"], "paths": ["demo/named/**"]}
    assert on["workflow_dispatch"]["inputs"]["demo"]["required"] is True


def test_each_demo_is_built_by_the_action_and_published_with_the_runs_own_token():
    workflow = _load("demo-bundles.yml")
    assert workflow["permissions"] == {"contents": "read"}
    job = workflow["jobs"]["bundle"]
    assert job["permissions"] == {"contents": "read", "packages": "write"}
    assert job["strategy"]["matrix"]["demo"] == "${{ fromJSON(needs.plan.outputs.demos) }}"
    steps = job["steps"]
    build = next(s for s in steps if s.get("uses") == "./.github/actions/demo-bundle")
    assert build["with"] == {"demo": "${{ matrix.demo }}"}
    publish = next(s for s in steps if "demo_bundle.py push" in s.get("run", ""))
    assert publish["env"]["PROVISA_DEMO_REGISTRY_TOKEN"] == "${{ secrets.GITHUB_TOKEN }}"
    assert publish["env"]["REGISTRY"] == "https://ghcr.io/${{ github.repository_owner }}"


def test_the_plan_builds_only_the_published_demos_a_push_changed():
    run = _load("demo-bundles.yml")["jobs"]["plan"]["steps"][-1]["run"]
    assert 'git diff --name-only "$BEFORE" "$GITHUB_SHA" -- demo/named/' in run
    assert "grep -qx 'published: true'" in run


def test_no_release_or_image_build_carries_a_demo():
    for path in WORKFLOWS.glob("*.yml"):
        if path.name in ("demo-bundles.yml", "ci-leaf-check.yml"):
            continue
        text = path.read_text()
        assert "demo_bundle.py" not in text and "demo-bundle" not in text, path.name
    assert "demo/named" not in (ROOT / "Dockerfile").read_text()
