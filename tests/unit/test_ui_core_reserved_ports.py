# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The core UI lane's fixed ports are kept out of Linux's ephemeral range before Playwright runs.

The e2e's demo sources publish fixed host ports (33030-39200) inside the range Linux hands out
for outbound connections (32768-60999). With four backends connecting out, one connection held
37117 as its local port when the MongoDB container was to bind it: "failed to bind host port for
0.0.0.0:37117 ... address already in use", and the provisioning job ran no test (run 40185713445).
ui-core-prepare.sh reserves the band so no outbound connection is given one of them."""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
PREPARE = REPO / "scripts" / "ci" / "lanes" / "ui-core-prepare.sh"
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


def _reserved() -> set[int]:
    m = re.search(r"net\.ipv4\.ip_local_reserved_ports=(\S+)", PREPARE.read_text())
    assert m, "ui-core-prepare.sh reserves no ports"
    ports: set[int] = set()
    for part in m.group(1).strip("\"'").split(","):
        low, _, high = part.partition("-")
        ports.update(range(int(low), int(high or low) + 1))
    return ports


def test_every_fixed_e2e_port_in_the_ephemeral_range_is_reserved():
    exposed = {p for p in _fixed_ports() if p in _EPHEMERAL}
    assert 37117 in exposed  # the port the scan is about
    assert exposed <= _reserved(), sorted(exposed - _reserved())


def test_the_reservation_is_made_before_the_lane_command_on_linux_only():
    script = PREPARE.read_text()
    line = next(row for row in script.splitlines() if "ip_local_reserved_ports" in row)
    assert line.lstrip().startswith("sudo sysctl -w ")
    assert 'uname -s)" = Linux' in script
