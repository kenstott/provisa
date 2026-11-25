# Copyright (c) 2026 Kenneth Stott
# Canary: 695fa456-619f-4daa-a5a6-ccf2c0fb677a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The repository's root flake: how a Nix user gets Provisa.

Its NixOS module is for users' hosts: a host flake imports inputs.provisa.nixosModules.provisa and
sets services.provisa.enable, and a systemd service runs the flake's package as an unprivileged
service user. It offers the Linux release as packages.<system>.default and apps.<system>.default,
so `nix run github:kenstott/provisa` starts Provisa; and devShells.default is the toolchain for
working on Provisa. The NixOS workflow evaluates and checks it, builds a host that enables the
module and checks its service in a VM, and runs the package on stock NixOS. Nix is not on every machine the unit suite runs on, so what is checked
here is the text: the pins, the outputs and the workflow's jobs.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

_ROOT = Path(__file__).parents[2]
_FLAKE = (_ROOT / "flake.nix").read_text()
_NIXOS = _ROOT / "packaging" / "nixos"
_PACKAGE = (_NIXOS / "package.nix").read_text()
_HOST = (_NIXOS / "host" / "flake.nix").read_text()
_WORKFLOW = yaml.safe_load((_ROOT / ".github" / "workflows" / "nixos.yml").read_text())
_NIXPKGS = re.compile(r'inputs\.nixpkgs\.url = "github:NixOS/nixpkgs/([0-9a-f]{40})";')


def test_the_root_flake_pins_the_nixpkgs_commit_of_the_nixos_flake():
    (root,) = _NIXPKGS.findall(_FLAKE)
    (nixos,) = _NIXPKGS.findall((_NIXOS / "flake.nix").read_text())
    assert root == nixos


_MODULE = (_NIXOS / "module.nix").read_text()


def test_the_root_flake_exports_the_user_host_module_not_the_ci_host():
    assert "nixosModules.provisa = import ./packaging/nixos/module.nix { inherit self; };" in _FLAKE
    assert "configuration.nix" not in _FLAKE
    # The CI host stays in packaging/nixos for CI only: no flake exports it as a module.
    assert "nixosModules.provisa =" not in (_NIXOS / "flake.nix").read_text()


def test_the_module_runs_the_flakes_package_as_an_unprivileged_service_user():
    assert "default = self.packages.${pkgs.stdenv.hostPlatform.system}.default;" in _MODULE
    assert "config = mkIf cfg.enable {" in _MODULE
    for option in (
        "enable = mkEnableOption",
        "port = mkOption",
        "uiPort = mkOption",
        "dataDir = mkOption",
    ):
        assert option in _MODULE, option
    assert "isSystemUser = true;" in _MODULE
    assert 'User = "provisa";' in _MODULE
    # No sudo user, no source checkout, no Docker.
    for absent in ("wheel", "sudo", "isNormalUser", "/home/provisa", ".venv", "docker"):
        assert absent not in _MODULE, absent


def test_the_module_carries_the_libraries_the_preinstall_settings_name():
    assert "preinstall = import ./preinstall.nix { inherit pkgs; };" in _MODULE
    assert "inherit (preinstall.programs.nix-ld) libraries;" in _MODULE
    # The marker the release's first launch checks for those settings.
    assert 'preinstall.environment.etc."provisa/nixos-preinstall".text' in _MODULE


def test_the_service_starts_the_release_and_lives_while_its_servers_do():
    assert '"$provisa" start' in _MODULE
    assert 'read -r -a pids < "$home/.native.pid"' in _MODULE
    assert "while kill -0" in _MODULE
    assert 'ExecStop = "${lib.getExe cfg.package} stop";' in _MODULE
    assert 'Restart = "on-failure";' in _MODULE
    # The ports are written into the config.yaml the first launch made.
    assert "s/^api_port:.*/api_port: ${toString cfg.port}/" in _MODULE
    assert "s/^ui_port:.*/ui_port: ${toString cfg.uiPort}/" in _MODULE


def test_the_root_flake_offers_the_package_the_app_and_the_shell():
    assert "callPackage ./packaging/nixos/package.nix" in _FLAKE
    assert "callPackage ./packaging/nixos/dev-shell.nix" in _FLAKE
    for output in ("packages = ", "apps = ", "devShells = "):
        assert output in _FLAKE, output
    assert 'type = "app";' in _FLAKE
    # With no command, the app starts Provisa.
    assert 'if [ "$#" -eq 0 ]; then set -- start; fi' in _FLAKE


def test_the_package_is_a_pinned_release_asset_run_in_an_fhs_environment():
    (version,) = re.findall(r'version = "(\d+\.\d+\.\d+-alpha\.\d+)";', _PACKAGE)
    assert (
        "https://github.com/kenstott/provisa/releases/download/v${version}/"
        "Provisa-v${version}-linux-x86_64.AppImage"
    ) in _PACKAGE
    assert re.search(r'hash = "sha256-[A-Za-z0-9+/]{43}=";', _PACKAGE)
    assert "appimageTools.wrapType2" in _PACKAGE
    # The libraries the sandbox provides are the ones the NixOS settings give nix-ld.
    assert "(import ./preinstall.nix { inherit pkgs; }).programs.nix-ld.libraries" in _PACKAGE
    # `provisa start` returns with its services running in the background.
    assert "unsharePid = false;" in _PACKAGE and "dieWithParent = false;" in _PACKAGE


def test_the_host_flake_imports_and_enables_the_module_from_the_root_flake():
    assert 'inputs.provisa.url = "github:kenstott/provisa";' in _HOST
    assert 'inputs.nixpkgs.follows = "provisa/nixpkgs";' in _HOST
    assert "provisa.nixosModules.provisa" in _HOST
    assert "services.provisa.enable = true;" in _HOST
    # Its CI guest logs in as a user of its own: the service's user has no login.
    assert '"${provisa}/packaging/nixos/vm-guest.nix"' in _HOST
    assert "users.users.ci = {" in _HOST


def _runs(job: str) -> list[str]:
    return [str(s.get("run", "")) for s in _WORKFLOW["jobs"][job]["steps"]]


def test_the_nixos_workflow_checks_the_flake_and_builds_the_importing_host():
    runs = _runs("flake")
    assert any(r.strip() == "nix flake check --print-build-logs" for r in runs)
    (build,) = [r for r in runs if "packaging/nixos/host#" in r]
    assert "nixosConfigurations.host.config.system.build.toplevel" in build
    assert '--override-input provisa "git+file://$GITHUB_WORKSPACE"' in build


def test_the_nixos_workflow_runs_the_flake_on_stock_nixos_until_provisa_answers():
    steps = _WORKFLOW["jobs"]["flake-run"]["steps"]
    (vm,) = [s for s in steps if s.get("uses") == "./.github/actions/nixos-vm"]
    assert vm["with"]["system"] == "bare"
    assert "packaging/nixos/vm-run 'packaging/nixos/ci/flake-runs.sh'" in _runs("flake-run")
    script = (_NIXOS / "ci" / "flake-runs.sh").read_text()
    assert "nix_ run --print-build-logs . <" in script
    assert "http://127.0.0.1:8000/health" in script


def test_the_nixos_workflow_boots_the_host_and_checks_the_modules_service():
    steps = _WORKFLOW["jobs"]["flake-module"]["steps"]
    (vm,) = [s for s in steps if s.get("uses") == "./.github/actions/nixos-vm"]
    assert vm["with"] == {"flake": "packaging/nixos/host", "system": "ci", "user": "ci"}
    assert "packaging/nixos/vm-run 'packaging/nixos/ci/module-runs.sh'" in _runs("flake-module")
    script = (_NIXOS / "ci" / "module-runs.sh").read_text()
    assert "systemctl is-active provisa" in script
    assert "http://127.0.0.1:8000/health" in script
    assert "id -nG provisa" in script


def test_the_vm_action_builds_a_host_flake_against_this_checkout():
    action = yaml.safe_load((_ROOT / ".github" / "actions" / "nixos-vm" / "action.yml").read_text())
    assert set(action["inputs"]) == {"flake", "system", "user"}
    (build,) = [s["run"] for s in action["runs"]["steps"] if s.get("name") == "Build the VM"]
    assert '--override-input provisa "git+file://$GITHUB_WORKSPACE"' in build
    assert (
        "path:$GITHUB_WORKSPACE/${{ inputs.flake }}#nixosConfigurations.${{ inputs.system }}"
        in build
    )
