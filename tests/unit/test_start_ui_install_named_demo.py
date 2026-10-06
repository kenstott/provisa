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

import yaml

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


def _choose(tmp_path: Path, *files: str) -> tuple[int, str, str]:
    """Run only the block that picks a named demo's config, over a demo directory holding
    ``files``: (exit code, output, whether the config is a whole one)."""
    (tmp_path / "demo" / "named" / "x").mkdir(parents=True)
    for name in files:
        (tmp_path / "demo" / "named" / "x" / name).write_text("{}\n")
    start = TEXT.index("_NAMED_WHOLE=false")
    end = TEXT.index('LOG_DIR="$SCRIPT_DIR/.logs"')
    program = (
        f'SCRIPT_DIR="{tmp_path}"\nDEMO_NAME=x\n'
        + TEXT[start:end]
        + '\necho "WHOLE=$_NAMED_WHOLE"\n'
    )
    done = subprocess.run(["bash", "-c", program], capture_output=True, text=True)
    return done.returncode, done.stdout, done.stdout.split("WHOLE=")[-1].strip()


def test_a_named_demo_with_a_config_yaml_uses_it_whole(tmp_path: Path) -> None:
    code, _out, whole = _choose(tmp_path, "config.yaml")
    assert (code, whole) == (0, "true")


def test_a_named_demo_with_only_a_fragment_overlays_the_standard_config(tmp_path: Path) -> None:
    code, _out, whole = _choose(tmp_path, "fragment.yaml")
    assert (code, whole) == (0, "false")


def test_a_named_demo_with_neither_is_refused_by_name(tmp_path: Path) -> None:
    code, out, _ = _choose(tmp_path)
    assert code == 1
    assert "--demo x has no config.yaml" in out


def test_a_named_demo_with_both_is_refused_by_name(tmp_path: Path) -> None:
    code, out, _ = _choose(tmp_path, "config.yaml", "fragment.yaml")
    assert code == 1
    assert "--demo x has both config.yaml and fragment.yaml" in out


def test_a_whole_config_is_the_config_before_any_source_overlays_it() -> None:
    chosen = TEXT.index('export PROVISA_CONFIG="demo/named/$DEMO_NAME/config.yaml"')
    sources = TEXT.index(
        '_SRC_WRAPPER="${PROVISA_HOME:-$HOME/.provisa}/demo/provisa-with-sources.yaml"'
    )
    assert chosen < sources


def test_perf_is_a_whole_config_with_its_own_and_the_standard_demos_sources() -> None:
    perf = SCRIPT.parent / "demo" / "named" / "perf"
    assert not (perf / "fragment.yaml").exists()
    ids = {src["id"] for src in yaml.safe_load((perf / "config.yaml").read_text())["sources"]}
    assert {"bench-postgresql", "bench-neo4j", "pet-store-sqlite"} <= ids
