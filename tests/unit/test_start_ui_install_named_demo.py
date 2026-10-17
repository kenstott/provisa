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

# Requirements: REQ-1858, REQ-2049

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
        f'SCRIPT_DIR="{tmp_path}"\nDEMO_NAME=x\n_DEMO_DIR="{tmp_path}/demo/named/x"\n'
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
    chosen = TEXT.index('export PROVISA_CONFIG="$_DEMO_DIR/config.yaml"')
    sources = TEXT.index(
        '_SRC_WRAPPER="${PROVISA_HOME:-$HOME/.provisa}/demo/provisa-with-sources.yaml"'
    )
    assert chosen < sources


def test_perf_is_a_whole_config_with_its_own_and_the_standard_demos_sources() -> None:
    perf = SCRIPT.parent / "demo" / "named" / "perf"
    assert not (perf / "fragment.yaml").exists()
    ids = {src["id"] for src in yaml.safe_load((perf / "config.yaml").read_text())["sources"]}
    assert {"bench-postgresql", "bench-neo4j", "pet-store-sqlite"} <= ids


def _graphql_demo_enabled(demo: bool, demo_name: str, env_value: str | None) -> str:
    """Run only the launcher's choice of GRAPHQL_DEMO_ENABLED; the value it exports."""
    start = TEXT.index("# The pet store's GraphQL demo source is registered into the standard demo")
    end = TEXT.index("export PROVISA_DEMO=")
    preset = f"GRAPHQL_DEMO_ENABLED={env_value}\n" if env_value is not None else ""
    program = (
        "set -euo pipefail\n"
        + preset
        + f"DEMO={'true' if demo else 'false'}\nDEMO_NAME={demo_name}\n"
        + TEXT[start:end]
        + '\necho "$GRAPHQL_DEMO_ENABLED"\n'
    )
    done = subprocess.run(["bash", "-c", program], capture_output=True, text=True, check=True)
    return done.stdout.strip()


def test_the_pet_store_graphql_demo_is_the_standard_demos_alone() -> None:
    """A named demo is what its own config lists: the pet store's GraphQL demo source, and its
    shelter domain, are registered into the standard demo only -- whatever .env says."""
    assert _graphql_demo_enabled(True, "", None) == "true"
    assert _graphql_demo_enabled(True, "weblogs", None) == "false"
    assert _graphql_demo_enabled(True, "weblogs", "true") == "false"
    assert _graphql_demo_enabled(False, "", None) == "false"


def _where(tmp_path: Path, *args: str, published: bool = True) -> tuple[int, str]:
    """Run only the block that decides where a named demo comes from, after parsing ``args``,
    with a stand-in for the bundle puller that records its arguments and prints a directory:
    (exit code, output)."""
    demo = tmp_path / "demo" / "named" / "weblogs"
    demo.mkdir(parents=True)
    (demo / "demo.yaml").write_text(f"title: Web shop logs\npublished: {str(published).lower()}\n")
    pulled = tmp_path / "pulled" / "weblogs" / "202610141530-abcdef012"
    pulled.mkdir(parents=True)
    python = tmp_path / ".venv" / "bin" / "python"
    python.parent.mkdir(parents=True)
    python.write_text(f'#!/usr/bin/env bash\necho "$*" > "{tmp_path}/pull-args"\necho "{pulled}"\n')
    python.chmod(0o755)
    parse = TEXT[TEXT.index("KEEP_DOCKER=false") : TEXT.index('if [ -n "$IDP" ]')]
    where = TEXT[TEXT.index('_DEMO_VERSION=""') : TEXT.index("_NAMED_WHOLE=false")]
    program = (
        "set -euo pipefail\n"
        + parse
        + f'SCRIPT_DIR="{tmp_path}"\nPROVISA_HOME="{tmp_path}/home"\n'
        + where
        + '\necho "DIR=$_DEMO_DIR BUNDLED=$_DEMO_BUNDLED NAME=$DEMO_NAME"\n'
    )
    done = subprocess.run(["bash", "-c", program, "x", *args], capture_output=True, text=True)
    return done.returncode, done.stdout


def test_a_published_demo_is_pulled_as_its_latest_bundle(tmp_path: Path) -> None:
    code, out = _where(tmp_path, "--demo", "weblogs")
    assert code == 0, out
    assert f"DIR={tmp_path}/pulled/weblogs/202610141530-abcdef012 BUNDLED=true NAME=weblogs" in out
    args = (tmp_path / "pull-args").read_text().split()
    assert args[1:4] == ["pull", "weblogs", "--into"] and args[4] == f"{tmp_path}/home/demos"


def test_a_named_version_is_pulled_as_that_version(tmp_path: Path) -> None:
    code, out = _where(tmp_path, "--demo", "weblogs@202610141530-abcdef012")
    assert code == 0, out
    assert "BUNDLED=true NAME=weblogs" in out
    assert (tmp_path / "pull-args").read_text().split()[2] == "weblogs@202610141530-abcdef012"


def test_from_source_runs_the_checkouts_demo(tmp_path: Path) -> None:
    code, out = _where(tmp_path, "--demo", "weblogs", "--from-source")
    assert code == 0, out
    assert f"DIR={tmp_path}/demo/named/weblogs BUNDLED=false" in out
    assert not (tmp_path / "pull-args").exists()


def test_an_unpublished_demo_runs_from_the_checkout(tmp_path: Path) -> None:
    code, out = _where(tmp_path, "--demo", "weblogs", published=False)
    assert code == 0, out
    assert "BUNDLED=false" in out


def test_a_version_and_from_source_together_are_refused(tmp_path: Path) -> None:
    code, out = _where(tmp_path, "--demo", "weblogs@202610141530-abcdef012", "--from-source")
    assert code == 1
    assert "Choose one" in out


def test_a_bundles_logs_are_never_emptied_and_its_builders_never_run() -> None:
    block = TEXT[TEXT.index('if [ -n "$DEMO_NAME" ]; then\n  _NAMED_DIR') :]
    assert 'if [ "$KEEP_DATA" = false ] && [ "$_DEMO_BUNDLED" = false ]; then' in block
    bundled = block[block.index("# Only what reading the bundle needs") : block.index("elif [ -f")]
    assert 'docker compose -f "$_NAMED_DIR/docker-compose.run.yml" up -d --wait' in bundled
    assert "seeder" not in bundled and "docker-compose.yml" not in bundled


def _stop_others(tmp_path: Path, own: str | None, running: list[str]) -> tuple[int, list[str]]:
    """Run only stop_other_demo_stores, with a stand-in for docker: ``running`` the compose
    projects it lists, ``own`` the name this demo's compose file gives (None: it has none). The
    projects it was told to take down, in order."""
    import json

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    downs = tmp_path / "downs"
    docker = bin_dir / "docker"
    docker.write_text(
        "#!/usr/bin/env bash\n"
        'case "$*" in\n'
        f"  *'ls -a --format json'*) echo '{json.dumps([{'Name': n} for n in running])}' ;;\n"
        f"  *'config --format json'*) echo '{json.dumps({'name': own})}' ;;\n"
        f'  *down*) echo "$3" >> "{downs}" ;;\n'
        '  *) echo "unexpected docker $*" >&2; exit 9 ;;\n'
        "esac\n"
    )
    docker.chmod(0o755)
    compose = tmp_path / "docker-compose.run.yml"
    if own is not None:
        compose.write_text("services: {}\n")
    start = TEXT.index("stop_other_demo_stores() {")
    end = TEXT.index("\n}\n", start) + 3
    program = (
        "set -euo pipefail\nDEMO_NAME=weblogs\n"
        + TEXT[start:end]
        + f'stop_other_demo_stores "{compose}"\n'
    )
    done = subprocess.run(
        ["bash", "-c", program],
        capture_output=True,
        text=True,
        env={"PATH": f"{bin_dir}:/usr/bin:/bin"},
    )
    return done.returncode, downs.read_text().split() if downs.exists() else []


def test_starting_a_demo_stops_every_other_demos_stores(tmp_path: Path) -> None:
    """The maintainer's report: the checkout-built weblogs stack and sonnets were still up, and the
    bundled weblogs could not bind 28432. Only this demo's own project is left, and a bundle build's
    stack is not a demo's stores."""
    code, downs = _stop_others(
        tmp_path,
        "provisa-demo-weblogs-run",
        [
            "provisa-demo-weblogs",
            "provisa-demo-weblogs-run",
            "provisa-demo-sonnets",
            "provisa-demo-bundle-weblogs-4242",
            "compose",
        ],
    )
    assert code == 0
    assert downs == ["provisa-demo-weblogs", "provisa-demo-sonnets"]


def test_a_demo_with_no_stores_stops_every_demos_stores(tmp_path: Path) -> None:
    code, downs = _stop_others(tmp_path, None, ["provisa-demo-weblogs-run", "provisa-demo-perf"])
    assert code == 0
    assert downs == ["provisa-demo-weblogs-run", "provisa-demo-perf"]


def test_both_ways_a_demo_starts_stop_the_others_first() -> None:
    block = TEXT[TEXT.index('if [ -n "$DEMO_NAME" ]; then\n  _NAMED_DIR') :]
    bundled = block.index('stop_other_demo_stores "$_NAMED_DIR/docker-compose.run.yml"')
    checkout = block.index('stop_other_demo_stores "$_NAMED_DIR/docker-compose.yml"')
    assert bundled < block.index('docker compose -f "$_NAMED_DIR/docker-compose.run.yml" up -d')
    assert checkout < block.index(
        'docker compose -f "$_NAMED_DIR/docker-compose.yml" up -d --build'
    )
    # No volume is removed: a later --keep-data start of that demo still has its data.
    function = TEXT[TEXT.index("stop_other_demo_stores() {") :]
    function = function[: function.index("\n}\n")]
    assert 'docker compose -p "$project" down\n' in function + "\n"
    assert "-v" not in function.split("docker compose -p")[1].split("\n")[0]


def test_a_named_demo_in_regions_runs_a_headless_node_per_other_region() -> None:
    """REQ-2035: demo.yaml `regions` -- the visitor's node is its region's node, and each headless
    region gets an API-only node of its own on the same control plane, its protocol servers off or
    moved, stopped with the rest."""
    assert '_BACKEND_ENV+=( PROVISA_REGION="$DEMO_VISITOR_REGION" )' in TEXT
    headless = TEXT[TEXT.index("start_headless_nodes() {") :]
    headless = headless[: headless.index("\n}\n")]
    assert 'PROVISA_REGION="$_r"' in headless
    for off in ("PROVISA_PGWIRE_PORT=0", "PROVISA_MCP_PORT=0", "PROVISA_BOLT_PORT=0"):
        assert off in headless
    assert '--port "$((PROVISA_API_PORT + _i))"' in headless
    # REQ-2068: a captive PostgreSQL per region, never one store shared by both.
    assert "provisa.core.control_plane_pg start" in TEXT
    assert 'data/region-pg/$_r"' in TEXT
    assert 'export "PROVISA_DEMO_${_rid_upper}_PG_URL=postgresql+psycopg://' in TEXT
    assert "PROVISA_DEMO_REGIONS_PG_URL" not in TEXT
    cleanup = TEXT[TEXT.index("cleanup() {") :]
    assert '"${HEADLESS_PIDS[@]}"' in cleanup[: cleanup.index("\n}\n")]


def test_the_regions_of_a_demo_are_read_from_its_demo_yaml(tmp_path: Path) -> None:
    (tmp_path / "demo.yaml").write_text("regions:\n  visitor: us\n  headless: [eu]\n")
    block = TEXT[TEXT.index("  _DEMO_REGIONS=") :]
    block = block[: block.index('\n  if [ -n "$_DEMO_REGIONS" ]')]
    program = (
        f'SCRIPT_DIR="{SCRIPT.parent}"\n_DEMO_DIR="{tmp_path}"\n{block}\necho "$_DEMO_REGIONS"\n'
    )
    done = subprocess.run(["bash", "-c", program], capture_output=True, text=True)
    assert done.stdout.strip() == "us eu", done.stderr
