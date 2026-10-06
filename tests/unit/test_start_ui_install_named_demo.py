# Copyright (c) 2026 Kenneth Stott
# Canary: d206cc73-73c7-4967-af08-9f40613d962d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""start-ui-install.sh and named demos: the option parsing, and that a named demo reseeds by
default (REQ-1858).

The script is never run. Its option-parsing loop, which has no side effect beyond assigning
variables, is cut out and run in a bash of its own; the reseed step is checked as text, in order.
"""

# Requirements: REQ-1858

from __future__ import annotations

import subprocess
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / "start-ui-install.sh"
TEXT = SCRIPT.read_text()


def _parse(*args: str) -> dict[str, str] | tuple[int, str]:
    """Run only the option-parsing loop on ``args``; the variables it sets, or (exit code, output)
    when it refuses an option."""
    start = TEXT.index("KEEP_DOCKER=false")
    end = TEXT.index('if [ -n "$IDP" ]')
    program = (
        "set -euo pipefail\n"
        + TEXT[start:end]
        + '\necho "KEEP_DATA=$KEEP_DATA"; echo "DEMO=$DEMO"; echo "DEMO_NAME=$DEMO_NAME";'
        + ' echo "SOURCES=${SOURCES[*]-}"\n'
    )
    done = subprocess.run(
        ["bash", "-c", program, "start-ui-install.sh", *args], capture_output=True, text=True
    )
    if done.returncode != 0:
        return done.returncode, done.stdout
    return dict(line.split("=", 1) for line in done.stdout.splitlines())


def test_a_named_demo_reseeds_unless_told_to_keep_its_data() -> None:
    assert _parse("--demo", "perf")["KEEP_DATA"] == "false"
    kept = _parse("--demo", "perf", "--keep-data")
    assert kept["KEEP_DATA"] == "true"
    assert kept["DEMO_NAME"] == "perf"


def test_keep_data_does_not_become_a_demo_name() -> None:
    parsed = _parse("--demo", "--keep-data")
    assert parsed["DEMO"] == "true"
    assert parsed["DEMO_NAME"] == ""
    assert parsed["KEEP_DATA"] == "true"


def test_demo_with_no_name_is_the_standard_demo_and_a_name_is_a_named_one() -> None:
    assert _parse("--demo")["DEMO_NAME"] == ""
    assert _parse("--demo", "--source=redis")["DEMO_NAME"] == ""
    assert _parse("--demo", "--source=redis")["SOURCES"] == "redis"
    assert _parse("--demo", "retail", "--source=redis")["DEMO_NAME"] == "retail"


def test_an_unknown_option_prints_the_usage_with_keep_data() -> None:
    code, output = _parse("--bogus")  # type: ignore[misc]
    assert code == 1
    assert "Unknown option: --bogus" in output
    assert "--keep-data" in output


def test_the_stack_is_torn_down_and_its_data_removed_before_it_comes_up() -> None:
    block = TEXT[TEXT.index('if [ -n "$DEMO_NAME" ]; then\n  _NAMED_DIR') :]
    reset = block.index('if [ "$KEEP_DATA" = false ]; then')
    down = block.index('docker compose -f "$_NAMED_DIR/docker-compose.yml" down -v')
    remove = block.index('rm -rf "${_NAMED_DIR:?}/data"')
    up = block.index('docker compose -f "$_NAMED_DIR/docker-compose.yml" up -d --build')
    seeder = block.index('docker compose -f "$_NAMED_DIR/docker-compose.yml" up seeder')
    assert reset < down < remove < up < seeder
