# Copyright (c) 2026 Kenneth Stott
# Canary: 7c1e5a52-3b0d-4f0e-9a76-2d4f1c8e9b13
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""No conformance workflow publishes a host port inside Linux's ephemeral range.

Run 43665085771 (Flight SQL conformance) failed before any test with "failed to bind host port
0.0.0.0:55432/tcp: address already in use": 55432 lies in 32768-60999, where the kernel hands out
client socket ports, so a client can already hold it when the service container starts."""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

WORKFLOWS = sorted(
    (Path(__file__).resolve().parents[2] / ".github" / "workflows").glob("conformance-*.yml")
)
EPHEMERAL = range(32768, 61000)


def _host_port(spec: object) -> int | None:
    """The host port of one `ports:` entry, or None when the host port is left to Docker."""
    parts = str(spec).split("/")[0].split(":")
    if len(parts) == 1:
        return None
    host = parts[-2]
    if not re.fullmatch(r"\d+", host):
        raise AssertionError(f"unparsed host port in {spec!r}")
    return int(host)


def _published(doc: dict) -> list[tuple[str, object]]:
    found = []
    for job_name, job in (doc.get("jobs") or {}).items():
        for svc_name, svc in (job.get("services") or {}).items():
            for spec in svc.get("ports") or []:
                found.append((f"{job_name}.services.{svc_name}", spec))
    return found


def test_conformance_workflows_found():
    assert len(WORKFLOWS) >= 7


@pytest.mark.parametrize("path", WORKFLOWS, ids=lambda p: p.name)
def test_no_published_host_port_in_ephemeral_range(path):
    published = _published(yaml.safe_load(path.read_text()))
    bad = [
        (where, spec)
        for where, spec in published
        if (port := _host_port(spec)) is not None and port in EPHEMERAL
    ]
    assert not bad, f"{path.name} publishes host ports in 32768-60999: {bad}"


@pytest.mark.parametrize("path", WORKFLOWS, ids=lambda p: p.name)
def test_no_other_publication_form_in_ephemeral_range(path):
    """`docker run -p` / `--publish` in a run step is held to the same range."""
    text = path.read_text()
    for m in re.finditer(r"(?:-p|--publish)[ =](?:[\d.]+:)?(\d+):\d+", text):
        assert int(m.group(1)) not in EPHEMERAL, f"{path.name}: {m.group(0)}"


@pytest.mark.parametrize("path", WORKFLOWS, ids=lambda p: p.name)
def test_pg_port_env_matches_published_port(path):
    text = path.read_text()
    published = {_host_port(s) for _, s in _published(yaml.safe_load(text))} - {None}
    env_ports = set(re.findall(r'CONFORMANCE_PG_PORT: "(\d+)"', text))
    assert env_ports <= {str(p) for p in published}, (env_ports, published)
    for url_port in re.findall(r"@127\.0\.0\.1:(\d+)/conformance", text):
        assert int(url_port) in published
