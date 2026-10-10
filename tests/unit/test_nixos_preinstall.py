# Copyright (c) 2026 Kenneth Stott
# Canary: 56c9ba59-b28d-4aa1-95c0-99172db75260
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Provisa's installers on NixOS (the site says "Linux & NixOS, one installer for both").

A NixOS host needs settings only its own configuration can make (packaging/nixos/preinstall.nix,
which records its version on the host). Every installer makes ONE check for them
(packaging/nixos/preinstall-check.sh): install.sh and the Linux AppImage's first launch source
it. Where the settings are missing it hands the user the settings file, names the steps and
stops, having installed nothing. Docker is asked for only where what is being installed runs
under Docker.

The download a visitor gets is the AppImage, so it has to START on a host with none of the
settings: its runtime is packed static, and the build fails if it is not.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest

_ROOT = Path(__file__).parents[2]
_CHECK = _ROOT / "packaging/nixos/preinstall-check.sh"
_SETTINGS = _ROOT / "packaging/nixos/preinstall.nix"

# What a script that sources the check provides, and a host described by four values: the
# marker file's content (empty: no file), the user's groups, whether the Docker daemon answers,
# and whether a user is there to answer.
_HARNESS = r"""
set -euo pipefail
PROVISA_HOME="$1"; MARKER="$2"; GROUPS_OF_USER="$3"; DAEMON="$4"; NON_INTERACTIVE="$5"
DOCKER_NEEDED="$6"
BOLD=""; CYAN=""; NC=""
ok()   { printf "[ok] %s\n" "$*"; }
warn() { printf "[warn] %s\n" "$*"; }
. "$CHECK"
NIXOS_PREINSTALL_MARKER="$(dirname "$PROVISA_HOME")/marker"
if [ -n "$MARKER" ]; then printf "%s\n" "$MARKER" > "$NIXOS_PREINSTALL_MARKER"; fi
id() { if [ "$1" = -nG ]; then echo "$GROUPS_OF_USER"; else echo alice; fi; }
docker() { [ "$DAEMON" = up ]; }
nixos_preinstall "$SETTINGS" "$DOCKER_NEEDED"
echo "INSTALL PROCEEDS"
"""


def _check(tmp_path, *, marker="1", groups="users", daemon="down", interactive=False, docker):
    home = tmp_path / "home" / ".provisa"
    home.parent.mkdir()
    done = subprocess.run(
        [
            "bash",
            "-c",
            _HARNESS,
            "harness",
            str(home),
            marker,
            groups,
            daemon,
            "false" if interactive else "true",
            "true" if docker else "false",
        ],
        env={"CHECK": str(_CHECK), "SETTINGS": str(_SETTINGS), "PATH": "/usr/bin:/bin"},
        input="",
        capture_output=True,
        text=True,
    )
    written = sorted(p.name for p in home.iterdir()) if home.exists() else []
    return done.returncode, done.stdout + done.stderr, written, home


def test_a_host_without_the_settings_is_handed_them_and_told_the_steps(tmp_path):
    code, out, written, home = _check(tmp_path, marker="", docker=False)
    assert code == 1 and "INSTALL PROCEEDS" not in out
    assert f"sudo cp {home}/nixos-preinstall.nix /etc/nixos/provisa.nix" in out
    assert "add ./provisa.nix to imports" in out
    assert "sudo nixos-rebuild switch" in out
    assert "run this installer again" in out
    # The settings file is the only thing written, and it is the one the repository holds.
    assert written == ["nixos-preinstall.nix"]
    assert (home / "nixos-preinstall.nix").read_bytes() == _SETTINGS.read_bytes()


def test_an_install_that_needs_no_docker_is_not_asked_for_it(tmp_path):
    """The AppImage's default is the native tier. A user outside the docker group, on a host
    whose daemon is not even running, has everything that install needs."""
    code, out, written, _home = _check(tmp_path, groups="users wheel", daemon="down", docker=False)
    assert code == 0 and "INSTALL PROCEEDS" in out
    assert "the settings Provisa needs are in place" in out
    assert "docker" not in out.lower() and written == []


def test_a_host_without_the_settings_is_not_told_about_docker_it_does_not_need(tmp_path):
    _code, out, _written, _home = _check(tmp_path, marker="", docker=False)
    assert "extraGroups" not in out


def test_a_docker_install_names_the_group_step_for_the_user(tmp_path):
    code, out, written, _home = _check(tmp_path, groups="users wheel", daemon="up", docker=True)
    assert code == 1 and "INSTALL PROCEEDS" not in out
    assert 'users.users.alice.extraGroups = [ "docker" ];' in out
    assert "sudo nixos-rebuild switch" in out
    # The settings are there: they are not handed out again.
    assert "sudo cp" not in out and written == []


def test_a_docker_install_proceeds_with_the_settings_the_group_and_the_daemon(tmp_path):
    code, out, written, _home = _check(tmp_path, groups="users docker", daemon="up", docker=True)
    assert code == 0 and "INSTALL PROCEEDS" in out and written == []


def test_a_docker_install_stops_while_the_daemon_does_not_answer(tmp_path):
    code, out, _written, _home = _check(tmp_path, groups="users docker", daemon="down", docker=True)
    assert code == 1 and "sudo nixos-rebuild switch" in out
    assert "extraGroups" not in out  # the user is in the group already


def test_settings_of_another_version_are_missing_settings(tmp_path):
    code, out, written, _home = _check(tmp_path, marker="0", docker=False)
    assert code == 1 and "sudo cp" in out and written == ["nixos-preinstall.nix"]


@pytest.mark.parametrize("interactive", [True, False])
def test_only_a_user_who_can_answer_is_asked(tmp_path, interactive):
    """A closed stdin answers as Enter does; a non-interactive run is not asked at all."""
    code, out, _written, _home = _check(tmp_path, marker="", interactive=interactive, docker=False)
    assert code == 1
    assert ("Press Enter to list the steps" in out) is interactive
    assert "sudo nixos-rebuild switch" in out


# --- one check, two installers ----------------------------------------------------------------------


def test_the_check_expects_the_version_the_settings_record():
    (recorded,) = re.findall(
        r'environment\.etc\."provisa/nixos-preinstall"\.text = "(\d+)\\n";', _SETTINGS.read_text()
    )
    (expected,) = re.findall(r'^NIXOS_PREINSTALL_VERSION="(\d+)"$', _CHECK.read_text(), re.M)
    assert recorded == expected


def test_the_check_reads_the_file_the_settings_write():
    assert 'NIXOS_PREINSTALL_MARKER="/etc/provisa/nixos-preinstall"' in _CHECK.read_text()


def test_both_installers_make_the_one_check():
    installer = (_ROOT / "install.sh").read_text()
    first_launch = (_ROOT / "packaging/linux/first-launch.sh").read_text()
    for script in (installer, first_launch):
        assert "nixos_preinstall() {" not in script  # neither has a check of its own
        assert "NIXOS_PREINSTALL_VERSION=" not in script
    assert '. "${SCRIPT_DIR}/packaging/nixos/preinstall-check.sh"' in installer
    # install.sh's services run under docker compose: it always asks for Docker.
    assert 'nixos_preinstall "${SCRIPT_DIR}/packaging/nixos/preinstall.nix" true' in installer
    assert '. "${APPDIR}/nixos/preinstall-check.sh"' in first_launch


def test_first_launch_checks_before_it_asks_or_writes_and_asks_for_docker_only_on_that_tier():
    first_launch = (_ROOT / "packaging/linux/first-launch.sh").read_text()
    main = first_launch[first_launch.index("\nmain() {") :]
    settings = main.index('nixos_preinstall "${APPDIR}/nixos/preinstall.nix" false')
    resolve = main.index("resolve_deployment   #")
    docker = main.index('nixos_preinstall "${APPDIR}/nixos/preinstall.nix" true')
    assert settings < resolve < docker
    assert 'if is_nixos && [ "$NEEDS_DOCKER" = true ]; then' in main[resolve:docker]
    # Nothing of the install is set up before the first check.
    for step in ("setup_native_venv", "start_docker", "write_config", "install_cli"):
        assert main.index(step) > docker


def test_on_nixos_the_docker_tier_uses_the_system_daemon_the_settings_enable():
    first_launch = (_ROOT / "packaging/linux/first-launch.sh").read_text()
    assert (
        "if [ -e /etc/NIXOS ]; then\n"
        '  DOCKER_MODE="${PROVISA_DOCKER_MODE:-system}"\n'
        "else\n"
        '  DOCKER_MODE="${PROVISA_DOCKER_MODE:-bundled}"\n'
        "fi"
    ) in first_launch
    assert "virtualisation.docker.enable = true;" in _SETTINGS.read_text()


# --- the AppImage ------------------------------------------------------------------------------------


def test_the_appimage_stops_when_its_first_launch_refuses(tmp_path):
    """AppRun went on to run the command whatever first launch said. Run for real, with a first
    launch that stops as the NixOS check does: the command is not reached."""
    appdir = tmp_path / "AppDir"
    appdir.mkdir()
    (appdir / "AppRun").write_text((_ROOT / "packaging/linux/AppRun").read_text())
    (appdir / "first-launch.sh").write_text("#!/usr/bin/env bash\necho steps; exit 1\n")
    (appdir / "provisa-cli").write_text("#!/usr/bin/env bash\necho COMMAND RAN\n")
    for name in ("AppRun", "first-launch.sh", "provisa-cli"):
        (appdir / name).chmod(0o755)
    home = tmp_path / "home"
    home.mkdir()
    done = subprocess.run(
        ["bash", str(appdir / "AppRun"), "status"],
        env={"HOME": str(home), "PATH": "/usr/bin:/bin"},
        capture_output=True,
        text=True,
    )
    assert done.returncode == 1 and "steps" in done.stdout and "COMMAND RAN" not in done.stdout

    (appdir / "first-launch.sh").write_text("#!/usr/bin/env bash\nexit 0\n")
    done = subprocess.run(
        ["bash", str(appdir / "AppRun"), "status"],
        env={"HOME": str(home), "PATH": "/usr/bin:/bin"},
        capture_output=True,
        text=True,
    )
    assert done.returncode == 0 and "COMMAND RAN" in done.stdout


def test_the_appimage_carries_the_check_and_the_settings():
    build = (_ROOT / "packaging/linux/build-appimage.sh").read_text()
    assert (
        'cp "${REPO_ROOT}/packaging/nixos/preinstall-check.sh" "${APPDIR}/nixos/preinstall-check.sh"'
        in build
    )
    assert (
        'cp "${REPO_ROOT}/packaging/nixos/preinstall.nix"      "${APPDIR}/nixos/preinstall.nix"'
        in build
    )


def test_the_appimage_is_packed_with_the_static_runtime_and_the_build_checks_it():
    build = (_ROOT / "packaging/linux/build-appimage.sh").read_text()
    assert "github.com/AppImage/appimagetool/releases/download/" in build
    assert "AppImageKit/releases" not in build  # the retired tool packs a dynamic runtime
    assert "libfuse2" not in build.replace("no libfuse2", "")  # nor is it a build prerequisite
    packed = build.index('"$APPIMAGETOOL" "$APPDIR" "${OUT_DIR}/Provisa.AppImage"')
    assert build.index('require_static_runtime "${OUT_DIR}/Provisa.AppImage"') > packed


@pytest.mark.parametrize(
    ("headers", "code"),
    [
        ("  LOAD   0x000000 0x400000\n  GNU_STACK 0x0\n", 0),
        (
            "  PHDR   0x40\n  INTERP 0x2a8\n      [Requesting program interpreter: /lib64/ld-linux-x86-64.so.2]\n",
            1,
        ),
    ],
    ids=["static", "dynamic"],
)
def test_a_build_whose_runtime_names_a_loader_fails(headers, code):
    """The function itself, with the program headers a static and a dynamic runtime print."""
    build = (_ROOT / "packaging/linux/build-appimage.sh").read_text()
    start = build.index("require_static_runtime() {")
    function = build[start : build.index("\n}\n", start) + 3]
    script = (
        'ok() { echo "$*"; }; err() { echo "$*" >&2; }\n'
        'readelf() { printf "%s" "$HEADERS"; }\n' + function + "require_static_runtime x\n"
    )
    done = subprocess.run(
        ["bash", "-c", script],
        env={"HEADERS": headers, "PATH": "/usr/bin:/bin"},
        capture_output=True,
        text=True,
    )
    assert done.returncode == code, done.stderr
    assert ("will not start on NixOS" in done.stderr) is (code == 1)
