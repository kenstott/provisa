# Copyright (c) 2026 Kenneth Stott
# Canary: 9bce1c36-6fbc-4d58-9f1a-506f48c5efea
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The UI lanes' fixed ports are kept out of Linux's ephemeral range before Playwright runs.

The e2e's demo sources publish fixed host ports (33030-39200) inside the range Linux hands out
for outbound connections (32768-60999). With four backends connecting out, one connection held
37117 as its local port when the MongoDB container was to bind it: "failed to bind host port for
0.0.0.0:37117 ... address already in use", and the provisioning job ran no test (run 40185713445).
ui-core-prepare.sh reserves the band so no outbound connection is given one of them.

The Trino-backed lanes (trino-stack-prepare.sh) publish the same kind of ports and had no
reservation: an outbound connection held 36920 when the druid coordinator was to bind it, and the
druid source-to-query spec failed both attempts (release/v0.1.0-alpha.510, run 44446383071)."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
LANES = REPO / "scripts" / "ci" / "lanes"
PREPARES = [LANES / "ui-core-prepare.sh", LANES / "trino-stack-prepare.sh"]
_EPHEMERAL = range(32768, 61000)


def _fixed_ports() -> set[int]:
    sources = [
        *(REPO / "provisa-ui" / "e2e").glob("*.ts"),
        REPO / "provisa-ui" / "playwright.config.ts",
    ]
    found: set[int] = set()
    for source in sources:
        text = source.read_text()
        for m in re.finditer(r"_PORT[A-Z_]* = (\d{4,5})\b|PORT[A-Z_]*\s*\?\?\s*(\d{4,5})", text):
            found.add(int(m.group(1) or m.group(2)))
    return found


def _reserved(prepare: Path) -> set[int]:
    m = re.search(r"net\.ipv4\.ip_local_reserved_ports=(\S+)", prepare.read_text())
    assert m, f"{prepare.name} reserves no ports"
    ports: set[int] = set()
    for part in m.group(1).strip("\"'").split(","):
        low, _, high = part.partition("-")
        ports.update(range(int(low), int(high or low) + 1))
    return ports


@pytest.mark.parametrize("prepare", PREPARES, ids=lambda p: p.name)
def test_every_fixed_e2e_port_in_the_ephemeral_range_is_reserved(prepare: Path):
    exposed = {p for p in _fixed_ports() if p in _EPHEMERAL}
    assert {37117, 36920} <= exposed  # the ports the scan is about
    reserved = _reserved(prepare)
    assert exposed <= reserved, sorted(exposed - reserved)


@pytest.mark.parametrize("prepare", PREPARES, ids=lambda p: p.name)
def test_the_reservation_is_made_before_the_lane_command_on_linux_only(prepare: Path):
    script = prepare.read_text()
    line = next(row for row in script.splitlines() if "ip_local_reserved_ports" in row)
    assert line.lstrip().startswith("sudo sysctl -w ")
    assert 'uname -s)" = Linux' in script
