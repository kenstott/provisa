# Copyright (c) 2026 Kenneth Stott
# Canary: 5a639aeb-f12f-4a3a-a8ed-c8d41f1c1054
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A demo source's primer is told the compose prefix its source was started under.

demo/sources/provision.py starts a source as compose project ``<prefix>-<name>`` and then runs
the source's prime.py. The exasol and firebird primers find their container by that project,
read from PROVISA_DEMO_PREFIX -- which the provisioner never set, so under any prefix but the
default the primer looked in the wrong project: the UI e2e core lane on v0.1.0-alpha.477 failed
its exasol case on "no running exasol container for compose project 'provisa-demo-exasol'"
after starting it as provisa-s2q-exasol."""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SOURCES = REPO / "demo" / "sources"


def _provision():
    spec = importlib.util.spec_from_file_location("demo_provision", SOURCES / "provision.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_the_primer_runs_with_the_prefix_its_source_was_started_under(monkeypatch):
    provision = _provision()
    ran: list[tuple[list[str], dict]] = []

    def _run(cmd, *, check, env, **_kw):
        ran.append((list(cmd), dict(env)))

    monkeypatch.setattr(provision.subprocess, "run", _run)
    provision.up(["exasol"], "provisa-s2q-exasol", None, {"PATH": "/usr/bin"}, None)

    composed = [c for c, _ in ran if c[:2] == ["docker", "compose"]]
    assert composed and composed[0][3] == "provisa-s2q-exasol-exasol"
    (primed,) = [(c, e) for c, e in ran if c[-1].endswith("exasol/prime.py")]
    assert primed[1]["PROVISA_DEMO_PREFIX"] == "provisa-s2q-exasol"
    assert primed[1]["PATH"] == "/usr/bin"


def test_every_primer_that_finds_its_container_by_project_reads_that_prefix():
    by_project = [
        p
        for p in sorted(SOURCES.glob("*/prime.py"))
        if "com.docker.compose.project" in p.read_text()
    ]
    assert {p.parent.name for p in by_project} >= {"exasol", "firebird"}
    for primer in by_project:
        assert re.search(r'os\.environ\.get\(\s*"PROVISA_DEMO_PREFIX"', primer.read_text()), primer
