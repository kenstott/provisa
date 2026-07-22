# Copyright (c) 2026 Kenneth Stott
# Canary: c1b1e57b-bbf5-4b15-887d-06372a1630af
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
"""Structural tests for the Desktop Installation cluster.

REQ-976 (Docker/VMs only when needed), REQ-977 (local-first add-on resolution),
REQ-978 (demo OFF by default + ?tour=1), REQ-979 (native tier bundles a standalone
Python runtime, no container images), REQ-1005 (Windows native first-launch next-steps
guidance). These reqs are realized in the installer/first-launch scripts, so the wired
behavior is asserted against the real script text (the grep-based style of
test_infra_requirements.py).
"""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent.parent
PKG = REPO_ROOT / "packaging"


# ---------------------------------------------------------------------------
# REQ-976: Docker/VMs provisioned only when needed — engine==trino, OR
#          obs demo on Docker, OR demo on Docker. Otherwise fully native.
# ---------------------------------------------------------------------------


class TestREQ976DockerOnlyWhenNeeded:
    """REQ-976"""

    def test_macos_first_launch_needs_docker_decision_covers_three_triggers(self):
        # REQ-976 — NEEDS_DOCKER defaults false and flips only on the three triggers.
        content = (PKG / "macos" / "first-launch.sh").read_text()
        assert "NEEDS_DOCKER=false" in content, "native default must be no Docker"
        assert '[ "$DEPLOY_ENGINE" = "trino" ] && NEEDS_DOCKER=true' in content
        assert '[ "$OBS_MODE" = "docker" ] && NEEDS_DOCKER=true' in content
        assert 'DEMO_MODE" = "docker" ]; } && NEEDS_DOCKER=true' in content

    def test_macos_native_path_selects_native_runtime(self):
        # REQ-976 — NEEDS_DOCKER false -> runtime "native" (Python venv, no Docker);
        # true -> runtime "docker" (user's own Docker). No Lima/VM tier exists.
        content = (PKG / "macos" / "first-launch.sh").read_text()
        assert 'if [ "${NEEDS_DOCKER:-false}" = false ]; then' in content
        assert 'runtime="native"' in content
        assert 'runtime="docker"' in content
        assert 'runtime="lima"' not in content, "no Lima VM runtime tier"
        assert "limactl" not in content, "macOS first-launch must not drive Lima (limactl)"
        assert "runtime: ${runtime}" in content
        assert "image_source: ${IMAGE_SOURCE:-build}" in content


# ---------------------------------------------------------------------------
# REQ-977: heavy add-ons resolved local-first — installer-adjacent dir,
#          this script's dir, ~/Downloads, mounted volumes, then GitHub release.
# ---------------------------------------------------------------------------


class TestREQ977LocalFirstResolution:
    """REQ-977"""

    def test_macos_acquire_addon_resolution_order_is_local_first(self):
        # REQ-977 — the offline candidates must be searched before any download.
        content = (PKG / "macos" / "first-launch.sh").read_text()
        assert "acquire_addon()" in content
        i_adjacent = content.index('"$(dirname "$BUNDLE_DIR")/${filename}"')
        i_script = content.index('"${SCRIPT_DIR}/${filename}"')
        i_downloads = content.index('"${HOME}/Downloads/${filename}"')
        i_volumes = content.index('"/Volumes/"*"/${filename}"')
        i_github = content.index("releases/download")
        assert i_adjacent < i_script < i_downloads < i_volumes < i_github, (
            "add-on resolution must be local-first, GitHub release last"
        )

    def test_macos_acquire_addon_download_is_gated_and_last(self):
        # REQ-977 — GitHub download happens only when nothing staged AND selected;
        # airgapped enterprises pre-stage the tarball beside the installer.
        content = (PKG / "macos" / "first-launch.sh").read_text()
        assert 'if [ -n "$src" ]; then' in content  # extract offline when found
        assert "airgap" in content.lower()


# ---------------------------------------------------------------------------
# REQ-978: demo optional + OFF by default; when installed the launcher opens
#          the UI at ?tour=1 to auto-start the guided tour.
# ---------------------------------------------------------------------------


class TestREQ978DemoOffByDefaultTour:
    """REQ-978"""

    def test_demo_defaults_off_across_installers(self):
        # REQ-978 — every installer defaults PROVISA_INSTALL_DEMO to n / false.
        assert (
            'INSTALL_DEMO="${PROVISA_INSTALL_DEMO:-n}"'
            in (PKG / "macos" / "first-launch.sh").read_text()
        )
        assert (
            'INSTALL_DEMO="${PROVISA_INSTALL_DEMO:-n}"'
            in (PKG / "linux" / "first-launch.sh").read_text()
        )
        assert 'INSTALL_DEMO="${PROVISA_INSTALL_DEMO:-n}"' in (REPO_ROOT / "install.sh").read_text()
        native = (PKG / "windows" / "first-launch-native.ps1").read_text()
        assert "$env:PROVISA_INSTALL_DEMO -match '^(y|Y|true)'" in native
        assert "else { 'false' }" in native

    def test_macos_launcher_opens_tour_when_demo(self):
        # REQ-978 — the SwiftUI launcher opens ?tour=1 only when the demo is installed.
        swift = (
            PKG
            / "macos"
            / "ProvisaLauncher"
            / "Sources"
            / "ProvisaLauncher"
            / "Views"
            / "Setup"
            / "SetupWizardView.swift"
        ).read_text()
        assert 'config.installDemo ? "\\(base)/?tour=1" : base' in swift

    def test_windows_native_cli_opens_tour_when_demo(self):
        # REQ-978 — the native Windows CLI opens ?tour=1 only when demo=true.
        ps = (PKG / "windows" / "provisa-native.ps1").read_text()
        assert '$Demo) { "http://localhost:$UiPort/?tour=1" }' in ps


# ---------------------------------------------------------------------------
# REQ-979: native tier bundles a standalone Python runtime (python-build-standalone
#          + provisa wheel + duckdb/pg_duckdb + aiosqlite); base ships no images.
# ---------------------------------------------------------------------------


class TestREQ979NativeStandaloneRuntime:
    """REQ-979"""

    def test_windows_base_installer_bundles_standalone_python_no_images(self):
        # REQ-979 — build-sfx.ps1 pulls python-build-standalone, pip-installs provisa,
        # and ships no OVA/VirtualBox/Trino container images.
        base = (PKG / "windows" / "build-sfx.ps1").read_text().lower()
        assert "python-build-standalone" in base
        assert "aiosqlite" in base
        for token in ("vboxmanage", ".ova", "provisa-runtime.ova", "trinosrc"):
            assert token not in base, f"native base installer must not bundle {token}"

    def test_windows_native_first_launch_stages_runtime_no_docker(self):
        # REQ-979 — first-launch stages the runtime to %USERPROFILE%\.provisa\runtime.
        native = (PKG / "windows" / "first-launch-native.ps1").read_text()
        assert "'.provisa'" in native and "'runtime'" in native
        assert "python.exe" in native
        assert "no Docker" in native.lower() or "no docker" in native.lower()


# ---------------------------------------------------------------------------
# REQ-1005: Windows native first-launch presents next-steps guidance that Trino,
#           observability, and demo are NOT part of the native tier and are added
#           via the layered Container -> Obs -> Demo installers.
# ---------------------------------------------------------------------------


class TestREQ1005NativeNextSteps:
    """REQ-1005"""

    def test_native_first_launch_shows_next_steps(self):
        # REQ-1005 — a next-steps block runs after setup completes.
        native = (PKG / "windows" / "first-launch-native.ps1").read_text()
        assert "function Show-NextSteps" in native
        assert "Show-NextSteps\n" in native, "Show-NextSteps must be invoked in Main"

    def test_native_next_steps_names_all_three_missing_tiers(self):
        # REQ-1005 — guidance names Trino, observability, and demo as not-in-native.
        native = (PKG / "windows" / "first-launch-native.ps1").read_text().lower()
        assert "not part of the native tier" in native
        assert "trino" in native
        assert "observability" in native
        assert "demo" in native

    def test_native_next_steps_names_layered_installers_in_order(self):
        # REQ-1005 — Container installer first (initializes federation engine),
        # then Obs (requires container tier), then Demo (requires Core + Obs).
        native = (PKG / "windows" / "first-launch-native.ps1").read_text()
        block = native[native.index("function Show-NextSteps") :]
        i_container = block.index("Container installer")
        i_obs = block.index("Obs installer")
        i_demo = block.index("Demo installer")
        assert i_container < i_obs < i_demo, "layered installers must be presented in tier order"
        assert "install-container.ps1" in native or "Provisa-Container-" in block
        assert "Requires the container tier" in block
        assert "Requires Core + Obs" in block
