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
]
# A release's listing, trimmed: the other platforms and unrelated assets are present too. Every
# version part of provisa_duckdb_ext (REQ-294) is there, one wheel per part and platform.
_RELEASE_LISTING = _X86_ASSETS + [
    "provisa_duckdb_ext-0.3.0.1-py3-none-manylinux_2_28_x86_64.whl",
    "provisa_duckdb_ext-0.3.0.2-py3-none-manylinux_2_28_x86_64.whl",
    "provisa-0.1.0a511-cp312-cp312-manylinux_2_28_aarch64.whl",
    "provisa-0.1.0a511-cp312-cp312-macosx_11_0_arm64.whl",
    "provisa_duckdb_ext-0.3.0.1-py3-none-manylinux_2_28_aarch64.whl",
    "provisa_duckdb_ext-0.3.0.2-py3-none-manylinux_2_28_aarch64.whl",
    "provisa_duckdb_ext-0.3.0.1-py3-none-manylinux_2_28_x86_64.whl.publish.attestation",
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
        'IH_HEALTH_TIMEOUT IH_PG_PORT IH_MINIO_PORT IH_ZAYCHIK_PORT IH_HELP; do printf "%s=%s\\n" "$v" "${!v}"; done\n'
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
    assert (got["IH_PG_PORT"], got["IH_MINIO_PORT"], got["IH_ZAYCHIK_PORT"]) == (
        "5432",
        "9000",
        "8480",
    )
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
        "--pg-port", "6432",
        "--minio-port", "9100",
        "--zaychik-port", "9480",
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
    assert (got["IH_PG_PORT"], got["IH_MINIO_PORT"], got["IH_ZAYCHIK_PORT"]) == (
        "6432",
        "9100",
        "9480",
    )
    assert got["REMOTE"].endswith("--pg-port 6432 --minio-port 9100 --zaychik-port 9480")
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
        (("--version", _TAG, "--pg-port", "pg"), "--pg-port must be a port number"),
        (("--version", _TAG, "--minio-port", "0"), "--minio-port must be a port number"),
        (("--version", _TAG, "--zaychik-port", "99999"), "--zaychik-port must be a port number"),
        (("--version", _TAG, "--pg-port", "8080"), "--pg-port must differ from --api-port"),
        (("--version", _TAG, "--api-port", "9000"), "--minio-port must differ from --api-port"),
        (("--version", _TAG, "--zaychik-port", "3000"), "--zaychik-port must differ"),
        (
            ("--version", _TAG, "--minio-port", "5432"),
            "--pg-port, --minio-port and --zaychik-port must differ",
        ),
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
    assert not [n for n in got if "x86_64" in n]


def test_a_missing_asset_is_named():
    listing = [n for n in _RELEASE_LISTING if not n.startswith("provisa-trino-plugins")]
    err = _refused(f"select_assets {_TAG} x86_64", "\n".join(listing) + "\n")
    assert f"release {_TAG} is missing asset(s): provisa-trino-plugins-{_TAG}.tar.gz" in err


def test_duckdb_ext_parts_are_left_to_the_stage_command():
    # provisa_duckdb_ext is one project split across version parts (REQ-294): two versions of one
    # project cannot share a virtualenv, and the stage command fetches every part at first start
    # (provisa.runtime_deps.stage_assets), as every installer does. Release 513's dry run refused
    # its two parts here.
    got = _ok(f"select_assets {_TAG} x86_64", "\n".join(_RELEASE_LISTING) + "\n").split()
    assert not [n for n in got if n.startswith("provisa_duckdb_ext")]


def test_a_release_without_duckdb_ext_wheels_still_installs():
    listing = [n for n in _RELEASE_LISTING if not n.startswith("provisa_duckdb_ext")]
    got = _ok(f"select_assets {_TAG} x86_64", "\n".join(listing) + "\n").split()
    assert sorted(got) == sorted(_X86_ASSETS)


def test_a_refused_selection_stops_the_install(tmp_path):
    # Release 513's dry run (--assets-dir) printed select_assets' refusal and installed anyway:
    # install_local read the selection through a command substitution, which bash runs without
    # errexit, so fetch_assets copied what it had selected and returned success.
    assets = tmp_path / "assets"
    assets.mkdir()
    listing = [n for n in _RELEASE_LISTING if not n.startswith("provisa-trino-plugins")]
    for name in listing:
        (assets / name).write_bytes(b"")
    done = _bash(
        f"IH_PREFIX={shlex.quote(str(tmp_path / 'prefix'))} IH_VERSION={_TAG} IH_REPO=o/r "
        f"IH_ASSETS_DIR={shlex.quote(str(assets))}\n"
        "uname() { echo x86_64; }\n"
        "preflight() { :; }\n"
        # macOS's find has no -printf, and its bash 3.2 no mapfile: stand-ins keep the test about
        # the selection on every machine.
        f"find() {{ printf '%s\\n' {' '.join(listing)}; }}\n"
        "mapfile() { cat >/dev/null; }\n"
        "ensure_user() { echo installing anyway; }\n"
        "install_local"
    )
    assert done.returncode != 0
    assert "installing anyway" not in done.stdout
    assert f"is missing asset(s): provisa-trino-plugins-{_TAG}.tar.gz" in done.stderr


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


def test_the_installed_host_smoke_config_loads_and_runs_on_the_installs_engine(
    tmp_path, monkeypatch
):
    # REQ-1989, REQ-2269: the installed-host smoke run gives the service the demo plus the smoke's
    # own sources, on the engine the install's provisa.yaml names. The demo's file names duckdb, and
    # an include never overrides a scalar the including file sets, so the two must load as one.
    from provisa.core.config_loader import parse_config_dict, read_config_with_includes
    from tests.smoke.installed_config import write_installed_smoke_config

    # What the smoke harness names for its seeded PostgreSQL source.
    for key, value in {"SMOKE_PG_HOST": "/run/pg", "SMOKE_PG_PORT": "5433"}.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("SMOKE_PG_PASSWORD", "smoke")
    installed = yaml.safe_load(_ok("render_config_yaml 9080"))
    raw = read_config_with_includes(write_installed_smoke_config(tmp_path, installed))
    config = parse_config_dict(raw)
    assert (
        config.federation_engine,
        config.federation_engine_host,
        config.federation_engine_port,
    ) == ("trino", "127.0.0.1", 9080)
    demo = yaml.safe_load((_ROOT / "config" / "provisa-install.yaml").read_text())
    assert {s.id for s in config.sources} >= {s["id"] for s in demo["sources"]} | {
        "smoke-parquet",
        "smoke-pg",
    }
    assert ("smoke-pg", "smoke_accounts") in {(t.source_id, t.table_name) for t in config.tables}


def test_the_env_file_names_the_config_and_leaves_the_engine_to_it():
    # REQ-2269: the config's federation_engine wins over the launch profile's default, so the env
    # file sets no PROVISA_ENGINE.
    text = _ok("render_env_file /srv/etc /srv/data 9080 6432 pgpw 9100 miniopw 9480")
    assert "PROVISA_ENGINE=" not in text
    lines = text.splitlines()
    env = dict(line.split("=", 1) for line in lines if line and not line.startswith("#"))
    pg_url = "postgresql+psycopg://provisa:pgpw@127.0.0.1:6432/provisa"
    assert env == {
        "PROVISA_CONFIG": "/srv/etc/provisa.yaml",
        "ORG_ID": "root",
        "TRINO_HOST": "127.0.0.1",
        "TRINO_PORT": "9080",
        "PROVISA_TRINO_ETC_DIR": "/srv/data/trino",
        "PROVISA_FAKE_KEY_DIR": "/srv/data/fake-key",
        # REQ-2269 (amended 2026-11-26): the install's own PostgreSQL over TCP, so Trino's JDBC
        # catalogs reach it, and `provisa run` uses it instead of the embedded one.
        "PLATFORM_DATABASE_URL": pg_url,
        "TENANT_DATABASE_URL": pg_url,
        # REQ-2269: Trino's materializations land in that same PostgreSQL.
        "PROVISA_MATERIALIZE_URL": "postgresql://provisa:pgpw@127.0.0.1:6432/provisa",
        "PROVISA_OTEL_S3_ENDPOINT": "http://127.0.0.1:9100",
        "PROVISA_OTEL_S3_ACCESS_KEY": "provisa",
        "PROVISA_OTEL_S3_SECRET_KEY": "miniopw",
        "PROVISA_REDIRECT_ENDPOINT": "http://127.0.0.1:9100",
        "PROVISA_REDIRECT_BUCKET": "provisa-results",
        "PROVISA_REDIRECT_ACCESS_KEY": "provisa",
        "PROVISA_REDIRECT_SECRET_KEY": "miniopw",
        "ZAYCHIK_HOST": "127.0.0.1",
        "ZAYCHIK_PORT": "9480",
    }


def test_the_env_file_urls_are_ones_the_control_plane_reads():
    from sqlalchemy import make_url

    from provisa.core.trino_system_catalogs import _pg_parts

    text = _ok("render_env_file /e /d 8080 5432 pw 9000 mpw 8480")
    env = dict(line.split("=", 1) for line in text.splitlines() if "=" in line)
    url = make_url(env["TENANT_DATABASE_URL"])
    # A host Trino dials, not the embedded server's unix socket.
    assert _pg_parts(url) == ("127.0.0.1", 5432, "provisa", "provisa", "pw")
    assert url.get_backend_name() == "postgresql" and url.get_driver_name() == "psycopg"


def test_the_env_file_lands_trinos_materializations_in_the_installs_postgresql(
    tmp_path, monkeypatch
):
    # REQ-2269: Trino's materialize store is the install's own PostgreSQL. `provisa run` applies
    # the native profile beneath the env file, and that profile names an embedded DuckDB store,
    # which Trino refuses at startup; the env file names the store so the profile's is not taken.
    from sqlalchemy import make_url

    from provisa.cli import _apply_embedded_env
    from provisa.federation.engine import build_trino_engine

    text = _ok(f"render_env_file {tmp_path} /d 8080 5432 pw 9000 mpw 8480")
    env = dict(line.split("=", 1) for line in text.splitlines() if "=" in line)
    for key in ("PROVISA_ENGINE", "PROVISA_MATERIALIZE_URL", "PROVISA_ENGINE_URL"):
        monkeypatch.delenv(key, raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    # Extensions staged already: the launch stages nothing here.
    monkeypatch.setenv("PROVISA_DUCKDB_EXT_DIR", str(tmp_path / "ext"))
    _apply_embedded_env(tmp_path / "native")

    store = make_url(os.environ["PROVISA_MATERIALIZE_URL"])
    tenant = make_url(env["TENANT_DATABASE_URL"])
    assert store.get_backend_name() in build_trino_engine()._supported_materialize_stores
    assert (store.host, store.port, store.database, store.username, store.password) == (
        tenant.host,
        tenant.port,
        tenant.database,
        tenant.username,
        tenant.password,
    )


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


def test_the_exchange_spool_is_a_host_directory_trino_owns(tmp_path):
    # Release 513's dry run: a named volume's root is root's, Trino runs as uid 1000, and the first
    # query Provisa ran at boot failed "AccessDeniedException: /data/provisa/exchange/...".
    args = _ok(
        "trino_run_args /srv/etc /srv/data /srv/provisa/trino/plugins trinodb/trino:481 trino-file"
    ).splitlines()
    mounts = [args[i + 1] for i, a in enumerate(args) if a == "-v"]
    assert [m for m in mounts if m.split(":")[1] == "/data/provisa/exchange"] == [
        "/srv/data/trino-exchange:/data/provisa/exchange"
    ]
    calls = tmp_path / "install.calls"
    _ok(
        f'install() {{ printf "%s\\n" "$*" >> {shlex.quote(str(calls))}; }}\n'
        "ensure_trino_exchange_dir /srv/data"
    )
    assert calls.read_text().splitlines() == ["-d -m 0700 -o 1000 -g 1000 /srv/data/trino-exchange"]


def test_the_exchange_owner_is_the_uid_the_stack_gives_trino():
    compose = (_ROOT / "docker-compose.core.yml").read_text()
    uid = _ok('printf "%s" "$INSTALL_HOST_TRINO_UID"')
    assert f"chown -R {uid}:{uid} /data/provisa/exchange" in compose
    exchange = _ok("render_trino_exchange_properties")
    assert "exchange.base-directories=/data/provisa/exchange" in exchange


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
IH_PG_PORT=5432 IH_MINIO_PORT=9000 IH_ZAYCHIK_PORT=8480
docker() {
  local name="${*: -1}"
  case "$*" in
    *Health*) case " ${HC_CORE_DOWN:-} " in *" $name "*) echo unhealthy ;; *) echo healthy ;; esac ;;
    *) [ "$HC_DOCKER" = up ] && echo true || echo false ;;
  esac
}
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
    exports = "".join(f"export {k}={shlex.quote(v)}\n" for k, v in env.items())
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


def test_an_unhealthy_core_service_is_named():
    done = _health(HC_CORE_DOWN="provisa-minio provisa-zaychik")
    assert done.returncode == 1
    assert "MinIO container provisa-minio is not healthy" in done.stderr
    assert "Zaychik container provisa-zaychik is not healthy" in done.stderr
    assert "PostgreSQL" not in done.stderr
    assert done.stderr.count("\n  - ") == 2


# ── Core services beside Trino (REQ-2269, amended 2026-11-26) ─────────────────


def _core() -> dict:
    return yaml.safe_load((_ROOT / "docker-compose.core.yml").read_text())["services"]


def _lines(body: str) -> list[str]:
    return _ok(body).splitlines()


def _option(args: list[str], flag: str) -> list[str]:
    return [args[i + 1] for i, a in enumerate(args) if a == flag]


def _health_flags(args: list[str]) -> dict[str, str]:
    return {
        key: _option(args, f"--health-{key}")[0]
        for key in ("interval", "timeout", "retries", "start-period")
    }


def _compose_health(service: str) -> dict[str, str]:
    check = _core()[service]["healthcheck"]
    return {
        "interval": check["interval"],
        "timeout": check["timeout"],
        "retries": str(check["retries"]),
        "start-period": check["start_period"],
    }


def test_the_core_images_are_the_ones_the_stacks_run():
    core = _core()
    for var, service in (
        ("INSTALL_HOST_PG_IMAGE", "postgres"),
        ("INSTALL_HOST_MINIO_IMAGE", "minio"),
        ("INSTALL_HOST_MINIO_CLIENT_IMAGE", "minio-init"),
    ):
        assert _ok(f'printf "%s" "${var}"') == core[service]["image"], var


def test_postgres_runs_as_the_stack_runs_it_published_on_loopback_only():
    args = _lines("postgres_run_args /srv/etc /srv/data 6432 pgvector/pgvector:pg16-trixie")
    assert _option(args, "--name") == ["provisa-postgres"]
    assert _option(args, "-p") == ["127.0.0.1:6432:5432"]
    assert "--network" not in args
    assert _option(args, "--env-file") == ["/srv/etc/core/postgres.env"]
    assert _option(args, "-v") == ["/srv/data/postgres:/var/lib/postgresql/data"]
    assert _option(args, "--restart") == ["unless-stopped"]
    image = args.index("pgvector/pgvector:pg16-trixie")
    assert args[image + 1 :] == _core()["postgres"]["command"].split()
    # The stack's own health check, as the container's shell runs it ($$ is compose's escape).
    compose = _core()["postgres"]["healthcheck"]
    assert compose["test"][0] == "CMD-SHELL"
    assert _option(args, "--health-cmd") == [compose["test"][1].replace("$$", "$")]
    assert _health_flags(args) == _compose_health("postgres")


def test_the_postgres_env_is_the_stacks_database_and_role():
    env = dict(line.split("=", 1) for line in _lines("render_postgres_env s3cret"))
    compose = _core()["postgres"]["environment"]
    assert set(env) == set(compose)
    assert env["POSTGRES_DB"] == compose["POSTGRES_DB"]
    assert env["POSTGRES_USER"] == compose["POSTGRES_USER"]
    assert env["POSTGRES_PASSWORD"] == "s3cret"


def test_minio_runs_as_the_stack_runs_it_published_on_loopback_only():
    args = _lines("minio_run_args /srv/etc /srv/data 9100 bitnamilegacy/minio:latest")
    assert _option(args, "--name") == ["provisa-minio"]
    assert _option(args, "-p") == ["127.0.0.1:9100:9000"]
    assert "--network" not in args
    assert _option(args, "--env-file") == ["/srv/etc/core/minio.env"]
    compose = _core()["minio"]
    (volume,) = compose["volumes"]
    assert _option(args, "-v") == ["/srv/data/minio:" + volume.split(":", 1)[1]]
    image = args.index("bitnamilegacy/minio:latest")
    assert args[image + 1 :] == shlex.split(compose["command"])
    assert _option(args, "--health-cmd") == [" ".join(compose["healthcheck"]["test"][1:])]
    assert _health_flags(args) == _compose_health("minio")
    env = dict(line.split("=", 1) for line in _lines("render_minio_env pw"))
    assert set(env) == set(compose["environment"])
    assert env == {"MINIO_ROOT_USER": "provisa", "MINIO_ROOT_PASSWORD": "pw"}


def test_minio_data_is_owned_by_the_uid_the_image_runs_as():
    # docker-compose.core.yml: Bitnami's image runs as non-root uid 1001.
    assert "uid 1001" in (_ROOT / "docker-compose.core.yml").read_text()
    assert _ok('printf "%s" "$INSTALL_HOST_MINIO_UID"') == "1001"


def test_the_buckets_are_the_ones_minio_init_creates():
    args = _lines("minio_buckets_args /srv/etc 9100 bitnamilegacy/minio-client:latest")
    assert args[:7] == [
        "--rm",
        "--network",
        "host",
        "--env-file",
        "/srv/etc/core/minio.env",
        "--entrypoint",
        "/bin/sh",
    ]
    assert args[7:9] == ["bitnamilegacy/minio-client:latest", "-c"]
    script = args[9]
    assert script.startswith(
        'mc alias set local http://127.0.0.1:9100 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"'
    )
    made = re.findall(r"mc mb --ignore-existing local/(\S+)", script)
    compose = _core()["minio-init"]["entrypoint"]
    wanted = [
        b.replace("${PROVISA_OTEL_BUCKET:-provisa-otel}", "provisa-otel")
        for b in re.findall(r"mc mb --ignore-existing local/(\S+)", compose)
    ]
    assert made == wanted


def test_zaychik_runs_as_the_stack_runs_it_beside_trino_on_loopback():
    args = _lines("zaychik_run_args 9080 9480 provisa/zaychik:local")
    assert _option(args, "--name") == ["provisa-zaychik"]
    assert _option(args, "--network") == ["host"]
    assert "-p" not in args
    env = dict(e.split("=", 1) for e in _option(args, "-e"))
    compose = _core()["zaychik"]
    assert set(env) == set(compose["environment"])
    assert env["TF_TRINO_HOST"] == "127.0.0.1" and env["TF_TRINO_PORT"] == "9080"
    assert env["TF_FLIGHT_HOST"] == "127.0.0.1" and env["TF_FLIGHT_PORT"] == "9480"
    for key in ("TF_TRINO_SSL", "TF_FLIGHT_SSL", "TF_FLIGHT_AUTH_TYPE", "TF_FLIGHT_BATCH_SIZE"):
        assert env[key] == str(compose["environment"][key]), key
    assert env["TF_TRINO_ALLOWED_SERVERS"] == ""
    (health,) = _option(args, "--health-cmd")
    assert health == "timeout 2 bash -c '</dev/tcp/127.0.0.1/9480' || exit 1"
    compose_health = _compose_health("zaychik")
    assert _health_flags(args) == compose_health
    assert args[-1] == "provisa/zaychik:local"


def test_the_zaychik_image_is_the_releases_own():
    # A dry run's assets: the image release-prebuilt.yml builds into the core images, on either
    # machine.
    prebuilt = (_ROOT / ".github/workflows/release-prebuilt.yml").read_text()
    for machine in ("x86_64", "aarch64"):
        local = _ok(f"zaychik_image kenstott/provisa v0.1.0-alpha.513 /assets {machine}").strip()
        assert local == "provisa/zaychik:local"
    for platform in ("linux/amd64", "linux/arm64"):
        assert f"docker build --platform {platform} -t {local} ./zaychik" in prebuilt
        assert (
            f"docker save --platform {platform} {local} | gzip -9 > images/zaychik-local.tar.gz"
            in prebuilt
        )
    assert _ok('printf "%s" "$INSTALL_HOST_ZAYCHIK_IMAGE_FILE"') == "zaychik-local.tar.gz"
    # A published release on x86_64: the image release-publish.yml pushes for its version.
    publish = yaml.safe_load((_ROOT / ".github/workflows/release-publish.yml").read_text())
    push = next(
        s["with"]
        for s in publish["jobs"]["publish-zaychik-image"]["steps"]
        if s.get("uses", "").startswith("docker/build-push-action")
    )
    assert (
        push["tags"]
        == "ghcr.io/${{ github.repository_owner }}/provisa-zaychik:${{ inputs.version }}"
    )
    published = _ok("zaychik_image acme/provisa v0.1.0-alpha.513 '' x86_64").strip()
    assert published == "ghcr.io/acme/provisa-zaychik:v0.1.0-alpha.513"
    # REQ-2269 (amended 2026-11-26): that pushed image is amd64 only, so a published aarch64
    # install runs the one in the release's core-images asset, never a registry's.
    assert push["platforms"] == "linux/amd64"
    for machine in ("aarch64", "arm64"):
        out = _ok(f"zaychik_image acme/provisa v0.1.0-alpha.513 '' {machine}").strip()
        assert out == "provisa/zaychik:local"


def test_the_core_images_asset_is_named_as_the_release_names_it():
    action = (_ROOT / ".github/actions/release-metadata/action.yml").read_text()
    assert 'core_images_amd64_name=provisa-core-images-amd64-${TAG}.zip"' in action
    assert 'core_images_name=provisa-core-images-${TAG}.tar.gz"' in action
    assert _ok(f"core_images_asset_name {_TAG} x86_64").strip() == (
        f"provisa-core-images-amd64-{_TAG}.zip"
    )
    assert _ok(f"core_images_asset_name {_TAG} aarch64").strip() == (
        f"provisa-core-images-{_TAG}.tar.gz"
    )
    assert "unsupported machine" in _refused(f"core_images_asset_name {_TAG} riscv64")


_LOAD_STUBS = """
IH_REPO=kenstott/provisa IH_VERSION=v0.1.0-alpha.511
uname() { [ "$1" = -m ] && echo "$MACHINE" || command uname "$@"; }
docker() {
  case "$1" in
    load) cat > "$LOADED" ;;
    *) echo "unexpected: docker $*" >&2; return 1 ;;
  esac
}
"""


def test_a_dry_run_loads_zaychik_from_the_core_images_zip(tmp_path):
    import zipfile

    assets = tmp_path / "assets"
    assets.mkdir()
    with zipfile.ZipFile(assets / f"provisa-core-images-amd64-{_TAG}.zip", "w") as z:
        z.writestr("zaychik-local.tar.gz", b"the zaychik image")
        z.writestr("provisa-local.tar.gz", b"another image")
    loaded = tmp_path / "loaded"
    out = _ok(
        f"export LOADED={shlex.quote(str(loaded))} MACHINE=x86_64\n{_LOAD_STUBS}"
        f"IH_ASSETS_DIR={shlex.quote(str(assets))}\n"
        f'load_zaychik_image {shlex.quote(str(tmp_path / "dest"))}\nprintf "%s" "$IH_ZAYCHIK_IMAGE"'
    )
    assert out == "provisa/zaychik:local"
    assert loaded.read_bytes() == b"the zaychik image"


def test_a_dry_run_without_the_core_images_is_refused_naming_them(tmp_path):
    err = _refused(
        f"export LOADED=/dev/null MACHINE=x86_64\n{_LOAD_STUBS}"
        f"IH_ASSETS_DIR={shlex.quote(str(tmp_path))}\nload_zaychik_image {shlex.quote(str(tmp_path))}"
    )
    assert f"lacks provisa-core-images-amd64-{_TAG}.zip" in err


def _prebuilt_jobs() -> dict:
    return yaml.safe_load((_ROOT / ".github/workflows/release-prebuilt.yml").read_text())["jobs"]


def _step(job: dict, name: str) -> dict:
    return next(s for s in job["steps"] if s.get("name") == name)


def _arm64_core_images(tmp_path: Path, image: bytes) -> Path:
    """The arm64 core-images asset, built as release-prebuilt.yml builds it: pull-images-core saves
    the images into images/, uploads that directory as docker-images-core, and package-core-images
    downloads it into core-images/ and tars it with the step's own command."""
    jobs = _prebuilt_jobs()
    core = jobs["pull-images-core"]
    build = _step(core, "Pull core images and build zaychik")["run"]
    assert "docker build --platform linux/arm64 -t provisa/zaychik:local ./zaychik" in build
    assert (
        "docker save --platform linux/arm64 provisa/zaychik:local | gzip -9 > "
        "images/zaychik-local.tar.gz"
    ) in build
    upload = _step(core, "Upload core image tarballs")["with"]
    assert (upload["name"], upload["path"]) == ("docker-images-core", "images/")
    package = jobs["package-core-images"]
    download = _step(package, "Download core image tarballs")["with"]
    assert download["name"] == "docker-images-core"
    staged = tmp_path / download["path"]
    staged.mkdir(parents=True)
    (staged / "zaychik-local.tar.gz").write_bytes(image)
    (staged / "provisa-local.tar.gz").write_bytes(b"another image")
    name = f"provisa-core-images-{_TAG}.tar.gz"
    command = _step(package, "Package as single tarball")["run"].replace(
        "${{ needs.metadata.outputs.core_images_name }}", name
    )
    subprocess.run(["bash", "-euo", "pipefail", "-c", command], cwd=tmp_path, check=True)
    return tmp_path / name


def test_the_arm64_core_images_asset_carries_the_zaychik_image_an_aarch64_dry_run_loads(tmp_path):
    asset = _arm64_core_images(tmp_path, b"the arm64 zaychik image")
    assets = tmp_path / "assets"
    assets.mkdir()
    asset.rename(assets / asset.name)
    loaded = tmp_path / "loaded"
    out = _ok(
        f"export LOADED={shlex.quote(str(loaded))} MACHINE=aarch64\n{_LOAD_STUBS}"
        f"IH_ASSETS_DIR={shlex.quote(str(assets))}\n"
        f'load_zaychik_image {shlex.quote(str(tmp_path / "dest"))}\nprintf "%s" "$IH_ZAYCHIK_IMAGE"'
    )
    assert out == "provisa/zaychik:local"
    assert loaded.read_bytes() == b"the arm64 zaychik image"


_CURL_STUB = """
curl() {
  local out="" url=""
  while [ $# -gt 0 ]; do
    case "$1" in
      -o) out="$2"; shift ;;
      -*) ;;
      *) url="$1" ;;
    esac
    shift
  done
  printf '%s\\n' "$url" >> "$FETCHED"
  cp "$SERVED" "$out"
}
"""


def test_a_published_aarch64_install_loads_zaychik_from_the_releases_core_images(tmp_path):
    # REQ-2269 (amended 2026-11-26): the release's own core-images asset, downloaded; no registry
    # pull (the _LOAD_STUBS docker refuses anything but load).
    served = _arm64_core_images(tmp_path, b"the arm64 zaychik image")
    dest = tmp_path / "dest"
    dest.mkdir()
    loaded, fetched = tmp_path / "loaded", tmp_path / "fetched"
    out = _ok(
        f"export LOADED={shlex.quote(str(loaded))} MACHINE=aarch64 "
        f"FETCHED={shlex.quote(str(fetched))} SERVED={shlex.quote(str(served))}\n"
        f"{_LOAD_STUBS}{_CURL_STUB}IH_ASSETS_DIR=''\n"
        f'load_zaychik_image {shlex.quote(str(dest))}\nprintf "%s" "$IH_ZAYCHIK_IMAGE"'
    )
    assert out == "provisa/zaychik:local"
    assert loaded.read_bytes() == b"the arm64 zaychik image"
    assert fetched.read_text().splitlines() == [
        f"https://github.com/kenstott/provisa/releases/download/{_TAG}/provisa-core-images-{_TAG}.tar.gz"
    ]
    # The asset is the size of every core image; only Zaychik is taken from it, so it is not kept.
    assert list(dest.iterdir()) == []


def test_a_published_aarch64_install_names_a_failed_core_images_download(tmp_path):
    err = _refused(
        f"export LOADED=/dev/null MACHINE=aarch64\n{_LOAD_STUBS}"
        "curl() { return 22; }\nIH_ASSETS_DIR=''\n"
        f"load_zaychik_image {shlex.quote(str(tmp_path))}"
    )
    assert f"download of provisa-core-images-{_TAG}.tar.gz from release {_TAG} failed" in err


def test_a_published_x86_64_install_pulls_the_releases_zaychik_image(tmp_path):
    pulled = tmp_path / "pulled"
    out = _ok(
        f"export PULLED={shlex.quote(str(pulled))} MACHINE=x86_64\n{_LOAD_STUBS}"
        'docker() { [ "$1" = pull ] && printf "%s" "$2" > "$PULLED"; }\n'
        "curl() { echo unexpected curl >&2; return 1; }\nIH_ASSETS_DIR=''\n"
        f'load_zaychik_image {shlex.quote(str(tmp_path))}\nprintf "%s" "$IH_ZAYCHIK_IMAGE"'
    )
    assert out == f"ghcr.io/kenstott/provisa-zaychik:{_TAG}"
    assert pulled.read_text() == out


def test_secrets_are_generated_once_and_kept(tmp_path):
    cfg = shlex.quote(str(tmp_path))
    show = 'ensure_core_secrets {cfg}\nprintf "%s %s" "$IH_PG_PASSWORD" "$IH_MINIO_PASSWORD"'
    first = _ok(show.format(cfg=cfg)).split()
    assert len(first) == 2 and all(re.fullmatch(r"[A-Za-z0-9]{40,}", p) for p in first)
    assert first[0] != first[1]
    # Re-running the install (an upgrade) keeps them.
    assert _ok(show.format(cfg=cfg)).split() == first
    secrets = tmp_path / "secrets.env"
    assert secrets.stat().st_mode & 0o777 == 0o600
    assert secrets.read_text() == (
        f"PROVISA_PG_PASSWORD={first[0]}\nPROVISA_MINIO_ROOT_PASSWORD={first[1]}\n"
    )


def test_a_secret_missing_from_an_earlier_install_is_added_and_the_rest_kept(tmp_path):
    (tmp_path / "secrets.env").write_text("PROVISA_PG_PASSWORD=kept\n")
    out = _ok(
        f"ensure_core_secrets {shlex.quote(str(tmp_path))}\n"
        'printf "%s %s" "$IH_PG_PASSWORD" "$IH_MINIO_PASSWORD"'
    )
    pg, minio = out.split()
    assert pg == "kept" and minio and minio != "kept"


def test_the_install_starts_the_core_services_before_trino_and_zaychik_after():
    body = _SCRIPT.read_text().split("install_local() {", 1)[1].split("\n}\n", 1)[0]
    steps = [line.strip() for line in body.splitlines() if line.strip()]
    order = [s for s in steps if s.startswith(("start_", "write_files", "load_zaychik_image"))]
    assert order == [
        'load_zaychik_image "$assets"',
        "write_files",
        "start_core",
        "start_trino",
        "start_zaychik",
        "start_provisa",
    ]


def test_trino_reads_the_installs_files_directory_at_the_path_provisa_names_it(tmp_path):
    # Release 513's installed-host smoke: a Parquet source's directory is a host path Provisa
    # hands Trino as-is (pgwire_replica: the adapter's "directory" operand), and Trino runs in
    # Docker, so a file outside every mount failed "Directory does not exist" and the query
    # "Schema 'default' does not exist". The install's files directory is mounted at its own path.
    from tests.smoke.installed_config import installed_files_dir

    files = installed_files_dir(Path("/srv/data"))
    args = _ok(
        "trino_run_args /srv/etc /srv/data /srv/provisa/trino/plugins trinodb/trino:481 trino-file"
    ).splitlines()
    mounts = [args[i + 1] for i, a in enumerate(args) if a == "-v"]
    assert f"{files}:{files}:ro" in mounts
    calls = tmp_path / "install.calls"
    _ok(
        f'install() {{ printf "%s\\n" "$*" >> {shlex.quote(str(calls))}; }}\n'
        "ensure_files_dir /srv/data"
    )
    assert calls.read_text().splitlines() == [f"-d -m 0755 -o provisa -g provisa {files}"]
