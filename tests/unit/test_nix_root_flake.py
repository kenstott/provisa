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
    # No sudo user, no source checkout, and the module neither enables Docker nor joins its group.
    for absent in (
        "wheel",
        "sudo",
        "isNormalUser",
        "/home/provisa",
        ".venv",
        "virtualisation.docker",
        "extraGroups",
    ):
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
    # The app is a file of its own that takes the package, so CI runs the same app on its build.
    assert (
        "callPackage ./packaging/nixos/app.nix {\n            provisa = self.packages.${system}.default;"
        in _FLAKE
    )
    # With no command, the app starts Provisa.
    app = (_NIXOS / "app.nix").read_text()
    assert 'if [ "$#" -eq 0 ]; then set -- start; fi' in app
    assert 'exec ${lib.getExe provisa} "$@"' in app


def test_the_package_is_a_pinned_release_asset_run_in_an_fhs_environment():
    # The AppImage is an argument whose default is the pinned release asset: a user's flake gets
    # that release, and CI passes the AppImage its commit builds.
    (version,) = re.findall(r'releaseVersion \? "(\d+\.\d+\.\d+-alpha\.\d+)",', _PACKAGE)
    assert version
    assert "releaseAppImage ? fetchurl {" in _PACKAGE
    assert (
        "https://github.com/kenstott/provisa/releases/download/v${releaseVersion}/"
        "Provisa-v${releaseVersion}-linux-x86_64.AppImage"
    ) in _PACKAGE
    assert re.search(r'hash = "sha256-[A-Za-z0-9+/]{43}=";', _PACKAGE)
    assert "version = releaseVersion;" in _PACKAGE
    assert "src = releaseAppImage;" in _PACKAGE
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
    assert 'nix_ run --print-build-logs "${flake[@]}" <' in script
    assert "http://127.0.0.1:8000/health" in script


def test_nix_run_runs_from_the_home_directory_not_the_checkout():
    """vm-run starts in the checkout. The release's first launch checks its install with
    `python3 -c "import provisa._encoders.pgwire"`, which from there imported the checkout's
    provisa/ source tree, with no compiled encoders, and failed (run 45680556710). A user runs
    `nix run github:kenstott/provisa` from wherever they are; the leg runs it from home, the flake
    named by the checkout's path."""
    script = (_NIXOS / "ci" / "flake-runs.sh").read_text()
    body = script.split("rm -rf ~/.provisa", 1)
    assert 'checkout="$PWD"' in body[0] and "\ncd ~\n" in body[0]
    assert " . " not in body[1] and 'nix_ run "${flake[@]}" -- status' in body[1]


_THIS_COMMIT = (_NIXOS / "this-commit" / "flake.nix").read_text()
_OVERRIDE = (
    ".default.override {\n"
    '{indent}releaseVersion = "0-this-commit";\n'
    "{indent}releaseAppImage = ./Provisa.AppImage;\n"
)


def test_nix_run_runs_the_root_flakes_app_on_the_appimage_this_commit_builds():
    """The pinned release lacks what this commit fixes, so a leg on it tests the past, not what
    ships. The leg's flake is the root flake's package with the commit's AppImage (put beside it by
    the workflow) passed for the release, and the root flake's own app.nix on that package."""
    assert 'inputs.provisa.url = "github:kenstott/provisa";' in _THIS_COMMIT
    assert _OVERRIDE.replace("{indent}", " " * 8) in _THIS_COMMIT
    assert 'callPackage "${provisa}/packaging/nixos/app.nix" { provisa = package; }' in _THIS_COMMIT
    assert "apps.${system}.default = {" in _THIS_COMMIT
    script = (_NIXOS / "ci" / "flake-runs.sh").read_text()
    assert '"path:$checkout/packaging/nixos/this-commit"' in script
    assert '--override-input provisa "path:$checkout"' in script
    assert '[ ! -f "$checkout/packaging/nixos/this-commit/Provisa.AppImage" ]' in script


def test_the_module_legs_guest_runs_the_appimage_this_commit_builds():
    """The `ci` host the workflow boots sets the service's package to the root flake's package on
    the commit's AppImage; `host`, the system a user's host mirrors, keeps the module's default,
    the pinned release, which the flake job's build of it fetches and checks against its hash."""
    host, ci = _HOST.split("nixosConfigurations.ci =", 1)
    assert "services.provisa.package" not in host and "Provisa.AppImage" not in host
    assert (
        "services.provisa.package = provisa.packages.x86_64-linux"
        + _OVERRIDE.replace("{indent}", " " * 14)
    ) in ci


def test_the_service_has_a_runtime_directory_of_its_own():
    """A system service gets no login session, so no XDG_RUNTIME_DIR: platformdirs picked
    /tmp/runtime-<uid>, refused it as owned by another uid, and the embedded control plane did not
    start (run 45680556710). The unit makes /run/provisa for the service user and names it."""
    assert 'XDG_RUNTIME_DIR = "/run/provisa";' in _MODULE
    assert 'RuntimeDirectory = "provisa";' in _MODULE
    assert 'RuntimeDirectoryMode = "0700";' in _MODULE


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
    # path: (not git+file://) avoids `revCount is not available` on GitHub's shallow-clone runners.
    # git+file:// requires a full git history to compute revCount; path: hashes the directory tree.
    # REQ-1900: the NixOS CI workflow must pass the module service job (run 45637595821).
    assert '--override-input provisa "path:$GITHUB_WORKSPACE"' in build
    assert (
        "path:$GITHUB_WORKSPACE/${{ inputs.flake }}#nixosConfigurations.${{ inputs.system }}"
        in build
    )


def test_the_guests_store_writes_go_to_its_disk_not_to_memory():
    """A NixOS VM's writable store is a tmpfs of half its memory by default; `nix run` filled it
    unpacking the AppImage and stopped with "No space left on device" (run 45720472876)."""
    guest = (_NIXOS / "vm-guest.nix").read_text()
    assert "writableStoreUseTmpfs = false;" in guest


def test_the_guest_has_the_memory_and_swap_of_an_ubuntu_runner():
    """The kernel killed clickhouse-server for memory in core 2/21, beside Trino's 5 GB heap (run
    45940557737); an Ubuntu runner has 16 GB and 4 GB of swap. The guest's 12 GB and 8 GB of swap
    make the same 20 GB."""
    guest = (_NIXOS / "vm-guest.nix").read_text()
    assert "memorySize = 12288;" in guest
    swap = guest.split("virtualisation.vmVariant.swapDevices = [", 1)[1].split("];", 1)[0]
    assert re.search(r"^\s*size = 8192;$", swap, flags=re.M)


def test_the_package_names_a_runtime_directory_its_sandbox_accepts():
    """Inside the FHS sandbox's user namespace every root-owned directory reads as uid 65534, so
    platformdirs refused XDG_RUNTIME_DIR under /run and its /tmp fallback, and the embedded
    PostgreSQL did not start (run 45720472876, module leg). The caller's runtime directory is
    bound at the sandbox's top level and named there; a caller without one is refused by name."""
    assert '--bind "$XDG_RUNTIME_DIR" /xdg-runtime' in _PACKAGE
    assert "export XDG_RUNTIME_DIR=/xdg-runtime" in _PACKAGE
    pre = _PACKAGE.split("extraPreBwrapCmds = ''", 1)[1].split("'';", 1)[0]
    assert '[ ! -d "$XDG_RUNTIME_DIR" ]' in pre and "exit 1" in pre


def test_the_settings_give_nix_ld_the_embedded_postgresqls_clickhouse_library():
    """pg_clickhouse.so loads liblz4.so.1, which no library in the settings provided (run
    45720472876: the PostgreSQL engine's ClickHouse reads)."""
    settings = (_NIXOS / "preinstall.nix").read_text()
    libraries = settings.split("libraries = with pkgs; [", 1)[1].split("];", 1)[0]
    assert re.search(r"^\s*lz4$", libraries, flags=re.M)


def test_the_settings_give_nix_ld_the_harfbuzz_its_pango_needs():
    """node-canvas bundles an older libharfbuzz.so.0; the settings' pango bound to it and the graph
    renderer failed on hb_ot_color_has_paint (run 45903556292)."""
    settings = (_NIXOS / "preinstall.nix").read_text()
    libraries = settings.split("libraries = with pkgs; [", 1)[1].split("];", 1)[0]
    assert re.search(r"^\s*pango$", libraries, flags=re.M)
    assert re.search(r"^\s*harfbuzz$", libraries, flags=re.M)


def _trusted_interfaces(nix: str) -> list[str]:
    (block,) = re.findall(r"networking\.firewall\.trustedInterfaces = \[(.*?)\];", nix, re.S)
    return re.findall(r'"([^"]+)"', block)


def test_the_ci_host_lets_a_container_dial_a_server_on_the_host():
    """Trino in a compose network dials a code source's sidecar on the host; the NixOS firewall
    dropped it (run 45903556292). Docker's bridges are trusted on the CI host."""
    ci_host = (_NIXOS / "configuration.nix").read_text()
    assert _trusted_interfaces(ci_host) == ["docker0", "br-+"]
    assert "trustedInterfaces" not in (_NIXOS / "preinstall.nix").read_text()


def test_the_module_trusts_the_docker_bridges_as_the_ci_host_does():
    """REQ-1900 (maintainer 2026-11-26): a user's host trusts docker0 and the compose br-*
    bridges, so the federation engine in its compose network reaches the sidecars on the host;
    the module sets it whether or not openFirewall opens the API and UI ports."""
    ci_host = (_NIXOS / "configuration.nix").read_text()
    assert _trusted_interfaces(_MODULE) == _trusted_interfaces(ci_host) == ["docker0", "br-+"]
    config = _MODULE.split("config = mkIf cfg.enable {", 1)[1]
    assert re.search(r"^    networking\.firewall\.trustedInterfaces = \[", config, re.M)
