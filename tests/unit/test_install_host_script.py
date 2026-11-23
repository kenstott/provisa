# Copyright (c) 2026 Kenneth Stott
# Canary: 4925d283-4555-446d-9ad6-52042795ad57
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""scripts/install-host.sh (REQ-2269): argument handling, asset selection and the files it renders.

The script is sourced (main runs only when it is executed) and each function is called on its own
under ``bash -euo pipefail``, as the script runs it. The rendered config is validated by
``ProvisaConfig``, and the systemd unit's ExecStart is parsed by ``provisa``'s own argument parser,
so a flag the CLI does not take fails here."""

from __future__ import annotations

import configparser
import os
import re
import shlex
import subprocess
from pathlib import Path

import pytest
import yaml

_ROOT = Path(__file__).parents[2]
_SCRIPT = _ROOT / "scripts" / "install-host.sh"
_TAG = "v0.1.0-alpha.511"
_X86_ASSETS = [
    "provisa-0.1.0a511-cp312-cp312-manylinux_2_28_x86_64.whl",
    "provisa_notebook-0.1.0a511-py3-none-any.whl",
    "provisa-trino-plugins-v0.1.0-alpha.511.tar.gz",
    "provisa_duckdb_ext-0.2.2-py3-none-manylinux_2_28_x86_64.whl",
]
# A release's listing, trimmed: the other platforms and unrelated assets are present too.
_RELEASE_LISTING = _X86_ASSETS + [
    "provisa-0.1.0a511-cp312-cp312-manylinux_2_28_aarch64.whl",
    "provisa-0.1.0a511-cp312-cp312-macosx_11_0_arm64.whl",
    "provisa_duckdb_ext-0.2.2-py3-none-manylinux_2_28_aarch64.whl",
    "provisa_duckdb_ext-0.2.2-py3-none-manylinux_2_28_x86_64.whl.publish.attestation",
    "provisa_client-0.1.0a511-py3-none-any.whl",
    "Provisa-v0.1.0-alpha.511-linux-x86_64.AppImage",
]


def _bash(body: str, stdin: str = "") -> subprocess.CompletedProcess[str]:
    """Source the script, then run ``body``."""
    script = f"source {shlex.quote(str(_SCRIPT))}\n{body}"
    return subprocess.run(
        ["bash", "-euo", "pipefail", "-c", script],
        input=stdin,
        env={"PATH": os.environ["PATH"]},
        capture_output=True,
        text=True,
    )


def _ok(body: str, stdin: str = "") -> str:
    done = _bash(body, stdin)
    assert done.returncode == 0, done.stderr
    return done.stdout


def _refused(body: str, stdin: str = "") -> str:
    done = _bash(body, stdin)
    assert done.returncode != 0, done.stdout
    return done.stderr


def _parsed(*args: str) -> dict[str, str]:
    quoted = " ".join(shlex.quote(a) for a in args)
    out = _ok(
        f"parse_args {quoted}\n"
        "for v in IH_VERSION IH_SSH IH_ASSETS_DIR IH_REPO IH_PREFIX IH_DATA_DIR IH_CONFIG_DIR "
        "IH_BIND IH_API_PORT IH_UI_PORT IH_TRINO_PORT IH_TRINO_HEAP_GB IH_TRINO_IMAGE "
        'IH_HEALTH_TIMEOUT IH_HELP; do printf "%s=%s\\n" "$v" "${!v}"; done\n'
        'printf "REMOTE=%s\\n" "${IH_REMOTE_ARGS[*]:-}"'
    )
    return dict(line.split("=", 1) for line in out.splitlines())


# ── Arguments ────────────────────────────────────────────────────────────────


def test_defaults_install_under_opt_with_trino_on_8080():
    got = _parsed("--version", _TAG)
    assert got["IH_VERSION"] == _TAG
    assert got["IH_SSH"] == ""
    assert got["IH_REPO"] == "kenstott/provisa"
    assert got["IH_PREFIX"] == "/opt/provisa"
    assert got["IH_DATA_DIR"] == "/var/lib/provisa"
    assert got["IH_CONFIG_DIR"] == "/etc/provisa"
    assert got["IH_BIND"] == "0.0.0.0"
    assert (got["IH_API_PORT"], got["IH_UI_PORT"], got["IH_TRINO_PORT"]) == ("8000", "3000", "8080")
    assert got["IH_TRINO_IMAGE"] == "trinodb/trino:481"
    assert got["REMOTE"] == f"--version {_TAG}"


def test_every_option_is_read_and_ssh_is_not_passed_to_the_remote_side():
    got = _parsed(
        "--ssh", "admin@db1",
        "--version", "v0.2.0",
        "--repo", "acme/provisa",
        "--prefix", "/srv/provisa",
        "--data-dir", "/srv/data",
        "--config-dir", "/srv/etc",
        "--bind", "10.0.0.5",
        "--api-port", "9000",
        "--ui-port", "9001",
        "--trino-port", "9080",
        "--trino-heap-gb", "12",
        "--trino-image", "trinodb/trino:482",
        "--health-timeout", "60",
    )  # fmt: skip
    assert got["IH_SSH"] == "admin@db1"
    assert got["IH_VERSION"] == "v0.2.0"
    assert got["IH_REPO"] == "acme/provisa"
    assert got["IH_PREFIX"] == "/srv/provisa"
    assert got["IH_DATA_DIR"] == "/srv/data"
    assert got["IH_CONFIG_DIR"] == "/srv/etc"
    assert got["IH_BIND"] == "10.0.0.5"
    assert (got["IH_API_PORT"], got["IH_UI_PORT"], got["IH_TRINO_PORT"]) == ("9000", "9001", "9080")
    assert got["IH_TRINO_HEAP_GB"] == "12"
    assert got["IH_TRINO_IMAGE"] == "trinodb/trino:482"
    assert got["IH_HEALTH_TIMEOUT"] == "60"
    assert "--ssh" not in got["REMOTE"] and "admin@db1" not in got["REMOTE"]
    assert got["REMOTE"].startswith("--version v0.2.0 --repo acme/provisa --prefix /srv/provisa")


def test_help_needs_no_version():
    assert _parsed("--help")["IH_HELP"] == "true"


@pytest.mark.parametrize(
    ("args", "message"),
    [
        ((), "--version is required"),
        (("--version", "latest"), "--version must be a release tag"),
        (("--version", "0.1.0"), "--version must be a release tag"),
        (("--version", _TAG, "--frobnicate"), "unknown argument '--frobnicate'"),
        (("--version",), "--version needs a value"),
        (("--version", _TAG, "--api-port", "http"), "--api-port must be a port number"),
        (("--version", _TAG, "--ui-port", "70000"), "--ui-port must be a port number"),
        (("--version", _TAG, "--trino-port", "0"), "--trino-port must be a port number"),
        (("--version", _TAG, "--ui-port", "8000"), "--api-port and --ui-port must differ"),
        (("--version", _TAG, "--trino-port", "3000"), "--trino-port must differ"),
        (("--version", _TAG, "--trino-heap-gb", "0"), "--trino-heap-gb must be a whole number"),
        (
            ("--version", _TAG, "--health-timeout", "soon"),
            "--health-timeout must be a whole number",
        ),
        (("--version", _TAG, "--prefix", "opt/provisa"), "must be absolute paths"),
        (("--version", _TAG, "--data-dir", "data"), "must be absolute paths"),
        (("--version", _TAG, "--repo", "provisa"), "--repo must be OWNER/NAME"),
        (("--version", _TAG, "--ssh", "h", "--assets-dir", "/a"), "--assets-dir names a directory"),
    ],
)
def test_bad_arguments_are_refused_naming_the_problem(args, message):
    quoted = " ".join(shlex.quote(a) for a in args)
    assert message in _refused(f"parse_args {quoted}")


# ── Executed vs sourced ─────────────────────────────────────────────────────


def test_executed_with_help_prints_usage():
    done = subprocess.run(["bash", str(_SCRIPT), "--help"], capture_output=True, text=True)
    assert done.returncode == 0, done.stderr
    assert "Usage: install-host.sh --version <tag>" in done.stdout


def test_executed_without_a_version_fails_before_touching_anything():
    done = subprocess.run(["bash", str(_SCRIPT)], capture_output=True, text=True)
    assert done.returncode == 1
    assert "--version is required" in done.stderr


def test_executed_from_stdin_runs_main_as_over_ssh():
    done = subprocess.run(
        ["bash", "-s", "--", "--help"], stdin=_SCRIPT.open(), capture_output=True, text=True
    )
    assert done.returncode == 0, done.stderr
    assert "Usage: install-host.sh" in done.stdout


def test_sourcing_runs_nothing():
    assert _ok("true") == ""


def test_the_script_parses():
    done = subprocess.run(["bash", "-n", str(_SCRIPT)], capture_output=True, text=True)
    assert done.returncode == 0, done.stderr


# ── SSH ──────────────────────────────────────────────────────────────────────


def test_remote_command_runs_the_script_under_sudo_with_each_argument_intact():
    cmd = _ok('remote_command --version v1.0.0 --bind "10.0.0.1" --trino-image "a b\'c"').strip()
    assert cmd.startswith("sudo bash -s -- ")
    rest = cmd.removeprefix("sudo bash -s -- ")
    # The remote login shell word-splits the command: each argument comes back as it was given.
    out = subprocess.run(
        ["bash", "-c", f'set -- {rest}; printf "%s\\n" "$@"'], capture_output=True, text=True
    ).stdout
    assert out.splitlines() == [
        "--version",
        "v1.0.0",
        "--bind",
        "10.0.0.1",
        "--trino-image",
        "a b'c",
    ]


# ── Release assets ───────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("tag", "pep440"),
    [
        ("v0.1.0-alpha.511", "0.1.0a511"),
        ("v1.2.3-beta.4", "1.2.3b4"),
        ("v1.2.3-rc.10", "1.2.3rc10"),
        ("v1.2.3", "1.2.3"),
    ],
)
def test_the_wheel_version_is_the_tag_in_pep_440(tag, pep440):
    assert _ok(f"pep440_version {tag}").strip() == pep440


def test_x86_64_selects_the_release_wheels_and_plugins():
    got = _ok(f"select_assets {_TAG} x86_64", "\n".join(_RELEASE_LISTING) + "\n").split()
    assert sorted(got) == sorted(_X86_ASSETS)


def test_aarch64_selects_the_arm_wheels():
    got = _ok(f"select_assets {_TAG} aarch64", "\n".join(_RELEASE_LISTING) + "\n").split()
    assert "provisa-0.1.0a511-cp312-cp312-manylinux_2_28_aarch64.whl" in got
    assert "provisa_duckdb_ext-0.2.2-py3-none-manylinux_2_28_aarch64.whl" in got
    assert not [n for n in got if "x86_64" in n]


def test_a_missing_asset_is_named():
    listing = [n for n in _RELEASE_LISTING if not n.startswith("provisa-trino-plugins")]
    err = _refused(f"select_assets {_TAG} x86_64", "\n".join(listing) + "\n")
    assert f"release {_TAG} is missing asset(s): provisa-trino-plugins-{_TAG}.tar.gz" in err


def test_a_missing_duckdb_ext_wheel_is_named():
    listing = [n for n in _RELEASE_LISTING if not n.startswith("provisa_duckdb_ext")]
    err = _refused(f"select_assets {_TAG} x86_64", "\n".join(listing) + "\n")
    assert "provisa_duckdb_ext-<version>-py3-none-manylinux_2_28_x86_64.whl" in err


def test_two_duckdb_ext_wheels_for_one_platform_are_refused():
    listing = _RELEASE_LISTING + ["provisa_duckdb_ext-0.2.3-py3-none-manylinux_2_28_x86_64.whl"]
    err = _refused(f"select_assets {_TAG} x86_64", "\n".join(listing) + "\n")
    assert "more than one provisa_duckdb_ext wheel" in err


def test_an_unsupported_machine_is_refused():
    assert "unsupported machine 'riscv64'" in _refused("wheel_platform_tag riscv64")


def test_download_urls_are_the_releases_own():
    assert _ok(f"release_download_url kenstott/provisa {_TAG} x.whl").strip() == (
        f"https://github.com/kenstott/provisa/releases/download/{_TAG}/x.whl"
    )
    assert _ok(f"release_api_url kenstott/provisa {_TAG}").strip() == (
        f"https://api.github.com/repos/kenstott/provisa/releases/tags/{_TAG}"
    )


def test_asset_names_match_the_release_workflow():
    action = (_ROOT / ".github/actions/release-metadata/action.yml").read_text()
    assert 'plugins_name=provisa-trino-plugins-${TAG}.tar.gz"' in action
    assert _ok(f"plugins_asset_name {_TAG}").strip() == f"provisa-trino-plugins-{_TAG}.tar.gz"
    wheels = (_ROOT / ".github/workflows/release-packages.yml").read_text()
    assert "tag: manylinux_2_28_x86_64" in wheels and "tag: manylinux_2_28_aarch64" in wheels


def test_the_trino_image_is_the_one_the_stacks_run():
    compose = yaml.safe_load((_ROOT / "docker-compose.core.yml").read_text())
    assert (
        _ok('printf "%s" "$INSTALL_HOST_DEFAULT_TRINO_IMAGE"')
        == compose["services"]["trino"]["image"]
    )


# ── Rendered files ───────────────────────────────────────────────────────────


def test_the_config_selects_trino_and_provisa_accepts_it():
    from provisa.core.models import ProvisaConfig

    raw = yaml.safe_load(_ok("render_config_yaml 9080"))
    config = ProvisaConfig.model_validate(raw)
    assert config.federation_engine == "trino"
    assert config.federation_engine_host == "127.0.0.1"
    assert config.federation_engine_port == 9080
    # No auth block: the setup wizard runs on the first visit.
    assert "auth" not in raw


def test_the_env_file_pins_the_engine_and_names_the_config():
    lines = _ok("render_env_file /srv/etc /srv/data 9080").splitlines()
    env = dict(line.split("=", 1) for line in lines if line and not line.startswith("#"))
    assert env == {
        "PROVISA_CONFIG": "/srv/etc/provisa.yaml",
        "PROVISA_ENGINE": "trino",
        "TRINO_HOST": "127.0.0.1",
        "TRINO_PORT": "9080",
        "PROVISA_TRINO_ETC_DIR": "/srv/data/trino",
        "PROVISA_FAKE_KEY_DIR": "/srv/data/fake-key",
    }


def _unit() -> configparser.ConfigParser:
    parser = configparser.ConfigParser(interpolation=None, strict=True)
    parser.optionxform = str  # type: ignore[assignment,method-assign]
    parser.read_string(
        _ok("render_systemd_unit /srv/provisa /srv/etc /srv/data 10.0.0.5 9000 9001")
    )
    return parser


def test_the_unit_runs_the_current_release_as_the_service_user():
    unit = _unit()
    assert unit["Unit"]["Requires"] == "docker.service"
    assert "docker.service" in unit["Unit"]["After"]
    service = unit["Service"]
    assert service["User"] == "provisa" and service["Group"] == "provisa"
    assert service["EnvironmentFile"] == "/srv/etc/provisa.env"
    assert service["WorkingDirectory"] == "/srv/data"
    assert service["Restart"] == "on-failure"
    assert unit["Install"]["WantedBy"] == "multi-user.target"


def test_the_unit_execstart_is_a_command_provisa_accepts():
    from provisa.cli import build_parser

    argv = shlex.split(_unit()["Service"]["ExecStart"])
    assert argv[0] == "/srv/provisa/current/bin/provisa"
    args = build_parser().parse_args(argv[1:])
    assert args.command == "run"
    assert (args.host, args.api_port, args.ui_port) == ("10.0.0.5", 9000, 9001)
    assert args.no_browser is True
    assert args.data_dir == "/srv/data/native"
    assert args.demo is False


def _properties(text: str) -> dict[str, str]:
    return dict(
        line.split("=", 1) for line in text.splitlines() if line and not line.startswith("#")
    )


def test_trino_is_one_coordinator_with_dynamic_catalogs_on_the_chosen_port():
    props = _properties(_ok("render_trino_config_properties 9080"))
    assert props["coordinator"] == "true"
    assert props["node-scheduler.include-coordinator"] == "true"
    assert props["http-server.http.port"] == "9080"
    assert props["discovery.uri"] == "http://localhost:9080"
    assert props["catalog.management"] == "dynamic"
    assert props["internal-communication.shared-secret"] == "${ENV:PROVISA_TRINO_SHARED_SECRET}"
    # Fault-tolerant execution needs the exchange manager the script also writes.
    assert props["retry-policy"] == "TASK"
    exchange = _properties(_ok("render_trino_exchange_properties"))
    assert exchange["exchange-manager.name"] == "filesystem"
    assert _properties(_ok("render_trino_node_properties"))["node.data-dir"] == "/data/trino"


def test_trino_heap_is_the_chosen_size_and_loads_no_agent():
    jvm = _ok("render_trino_jvm_config 12").splitlines()
    assert "-Xmx12G" in jvm
    # The repo's jvm.config names an OpenTelemetry agent jar this host does not have.
    assert not [line for line in jvm if line.startswith("-javaagent")]


def test_trino_runs_on_the_host_network_with_every_plugin_mounted():
    args = _ok(
        "trino_run_args /srv/etc /srv/data /srv/provisa/trino/plugins trinodb/trino:481 "
        "trino-file provisa-functions"
    ).splitlines()
    assert args[-1] == "trinodb/trino:481"
    assert args[:7] == [
        "-d",
        "--name",
        "provisa-trino",
        "--restart",
        "unless-stopped",
        "--network",
        "host",
    ]
    assert args[args.index("--env-file") + 1] == "/srv/etc/trino/trino.env"
    mounts = [args[i + 1] for i, a in enumerate(args) if a == "-v"]
    for plugin in ("trino-file", "provisa-functions"):
        assert f"/srv/provisa/trino/plugins/{plugin}:/usr/lib/trino/plugin/{plugin}:ro" in mounts
    for f in ("config.properties", "jvm.config", "node.properties", "exchange-manager.properties"):
        assert f"/srv/etc/trino/{f}:/etc/trino/{f}:ro" in mounts
    for side in ("kafka", "kafka-client", "codehost-client", "redis"):
        assert f"/srv/data/trino/{side}:/etc/trino/{side}:ro" in mounts
    assert "/srv/data/fake-key:/etc/provisa/fake-key:ro" in mounts


def test_trino_side_dirs_are_the_ones_the_stack_mounts():
    compose = yaml.safe_load((_ROOT / "docker-compose.core.yml").read_text())
    mounted = {
        m.rsplit(":", 2)[1].removeprefix("/etc/trino/")
        for m in compose["services"]["trino"]["volumes"]
        if re.match(r"\$\{PROVISA_TRINO_ETC_HOST:-\./trino\}/[a-z-]+:/etc/trino/[a-z-]+:ro$", m)
    }
    assert set(_ok('printf "%s" "$INSTALL_HOST_TRINO_SIDE_DIRS"').split()) == mounted


def test_no_plugin_directories_is_refused():
    assert "no Trino plugin directories" in _refused("trino_run_args /e /d /p img")


def test_plugin_dirs_lists_only_directories_holding_a_jar(tmp_path):
    (tmp_path / "trino-file").mkdir()
    (tmp_path / "trino-file" / "trino-file.jar").write_bytes(b"")
    (tmp_path / "empty").mkdir()
    (tmp_path / "README").write_text("x")
    assert _ok(f"plugin_dirs {shlex.quote(str(tmp_path))}").split() == ["trino-file"]


# ── Health check ─────────────────────────────────────────────────────────────

_STUBS = """
IH_HEALTH_TIMEOUT=1 IH_TRINO_PORT=8080 IH_API_PORT=8000 IH_UI_PORT=3000 IH_BIND=0.0.0.0 IH_VERSION=v1.0.0
docker() { [ "$HC_DOCKER" = up ] && echo true || echo false; }
systemctl() { [ "$HC_SERVICE" = up ]; }
curl() {
  local url="${*: -1}"
  case "$url" in
    */v1/info) [ "$HC_TRINO" = up ] && echo '{"starting":false}' ;;
    */v1/query) [ "$HC_TRINO" = up ] && [ "$HC_ENGINE" = trino ] && echo '[{"user":"provisa"}]' ;;
    */ready) [ "$HC_SERVICE" = up ] && echo '{"status":"ready"}' ;;
    */health) [ "$HC_SERVICE" = up ] && echo '{"status":"ok"}' ;;
    */) [ "$HC_SERVICE" = up ] ;;
  esac
}
"""


def _health(**state: str) -> subprocess.CompletedProcess[str]:
    env = {"HC_DOCKER": "up", "HC_SERVICE": "up", "HC_TRINO": "up", "HC_ENGINE": "trino"} | state
    exports = "".join(f"export {k}={v}\n" for k, v in env.items())
    return _bash(exports + _STUBS + "health_check")


def test_a_healthy_install_passes():
    done = _health()
    assert done.returncode == 0, done.stderr
    assert "health check passed" in done.stderr


def test_a_stopped_service_is_named():
    done = _health(HC_SERVICE="down")
    assert done.returncode == 1
    assert "systemd service provisa is not active" in done.stderr
    assert "/ready did not report ready" in done.stderr
    assert "Trino container" not in done.stderr


def test_a_stopped_trino_is_named():
    done = _health(HC_DOCKER="down", HC_TRINO="down")
    assert done.returncode == 1
    assert "Trino container provisa-trino is not running" in done.stderr
    assert "/v1/info did not report started" in done.stderr
    assert "systemd service" not in done.stderr


def test_provisa_not_using_trino_is_named():
    done = _health(HC_ENGINE="duckdb")
    assert done.returncode == 1
    assert "Provisa is not using Trino as its federation engine" in done.stderr
    assert done.stderr.count("\n  - ") == 1
