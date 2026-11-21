# Copyright (c) 2026 Kenneth Stott
# Canary: 11b39e7a-52b3-4791-b6a7-b81d9fa5197e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The pip-audit scans of uv.lock see every index the lock uses, and audit every package.

pip-audit installs the exported pins in a venv of its own from PyPI unless told otherwise, so a
package the lock takes from another [[tool.uv.index]] (Linux torch==2.15.0+cpu from pytorch-cpu)
fails the install. PyPI's JSON API skips local versions with exit 0, so the scans query OSV and
pass the report through scripts/ci/pip_audit_gate.py, which fails on any skipped dependency.
"""

from __future__ import annotations

import importlib.util
import json
import tomllib
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[2]

# (workflow, job) of every pip-audit run over the uv.lock export.
LOCK_AUDITS = [
    (".github/workflows/release-packages.yml", "provisa-sbom"),
    (".github/workflows/cve-audit.yml", "pip-audit"),
]


def _lock_indexes() -> set[str]:
    pyproject = tomllib.loads((REPO / "pyproject.toml").read_text())
    return {index["url"] for index in pyproject["tool"]["uv"]["index"]}


def _job(workflow: str, job: str) -> dict:
    return yaml.safe_load((REPO / workflow).read_text())["jobs"][job]


def _audit_run(workflow: str, job: str) -> str:
    runs = [s["run"] for s in _job(workflow, job)["steps"] if "pip-audit " in s.get("run", "")]
    assert len(runs) == 1, f"{workflow} {job}: expected one pip-audit step"
    return runs[0]


def _gate():
    spec = importlib.util.spec_from_file_location(
        "pip_audit_gate", REPO / "scripts/ci/pip_audit_gate.py"
    )
    assert spec is not None and spec.loader is not None
    gate = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(gate)
    return gate


def test_every_lock_audit_is_listed():
    """Every workflow step that pip-audits the uv.lock export is one of LOCK_AUDITS."""
    found = set()
    for path in sorted((REPO / ".github/workflows").glob("*.yml")):
        for name, job in (yaml.safe_load(path.read_text()).get("jobs") or {}).items():
            for step in job.get("steps", []):
                if "pip-audit " in step.get("run", "") and "requirements-audit.txt" in step["run"]:
                    found.add((str(path.relative_to(REPO)), name))
    assert found == set(LOCK_AUDITS)


def test_the_lock_takes_linux_torch_from_an_extra_index():
    assert "https://download.pytorch.org/whl/cpu" in _lock_indexes()
    assert 'version = "2.15.0+cpu"' in (REPO / "uv.lock").read_text()


@pytest.mark.parametrize(("workflow", "job"), LOCK_AUDITS)
def test_the_lock_audit_sees_every_index_the_lock_uses(workflow, job):
    tokens = _audit_run(workflow, job).split()
    extra = {tokens[i + 1] for i, t in enumerate(tokens) if t == "--extra-index-url"}
    assert extra == _lock_indexes()
    assert "--index-url" not in tokens, "PyPI stays the primary index, as in the lock"


@pytest.mark.parametrize(("workflow", "job"), LOCK_AUDITS)
def test_the_lock_audit_queries_osv_and_gates_its_report(workflow, job):
    run = _audit_run(workflow, job)
    tokens = run.split()
    assert tokens[tokens.index("-s") + 1] == "osv", "PyPI's JSON API skips local versions"
    assert "-f json" in run and "set -o pipefail" in run
    report = run.split("tee ", 1)[1].split()[0]
    assert f"scripts/ci/pip_audit_gate.py {report}" in run
    assert "|| true" not in run and "continue-on-error" not in str(_job(workflow, job))


def test_the_gate_fails_a_skipped_dependency():
    report = {
        "dependencies": [
            {"name": "numpy", "version": "2.3.0", "vulns": []},
            {"name": "torch", "skip_reason": "Dependency not found on PyPI: torch (2.15.0+cpu)"},
        ],
        "fixes": [],
    }
    problems = _gate().violations(report)
    assert len(problems) == 1 and problems[0].startswith("torch: skipped")


def test_the_gate_fails_an_empty_report():
    assert _gate().violations({"dependencies": [], "fixes": []})


def test_the_gate_passes_a_fully_audited_report(tmp_path):
    report = {"dependencies": [{"name": "torch", "version": "2.15.0+cpu", "vulns": []}]}
    path = tmp_path / "report.json"
    path.write_text(json.dumps(report))
    assert _gate().main([str(path)]) == 0
