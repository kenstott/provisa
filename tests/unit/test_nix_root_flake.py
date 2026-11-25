# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The repository's root flake: how a Nix user gets Provisa.

It re-exports the NixOS module, so a host flake imports inputs.provisa.nixosModules.provisa; it
offers the Linux release as packages.<system>.default and apps.<system>.default, so `nix run
github:kenstott/provisa` starts Provisa; and devShells.default is the toolchain for working on
Provisa. The NixOS workflow evaluates and checks it, builds a host that imports the module, and
runs it on stock NixOS. Nix is not on every machine the unit suite runs on, so what is checked
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


def test_the_root_flake_reexports_the_module_the_nixos_flake_exports():
    module = "nixosModules.provisa = ./packaging/nixos/configuration.nix;"
    assert module in _FLAKE
    assert "nixosModules.provisa = ./configuration.nix;" in (_NIXOS / "flake.nix").read_text()


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


def test_the_host_flake_imports_the_module_from_the_root_flake():
    assert 'inputs.provisa.url = "github:kenstott/provisa";' in _HOST
    assert 'inputs.nixpkgs.follows = "provisa/nixpkgs";' in _HOST
    assert "provisa.nixosModules.provisa" in _HOST


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
