# Copyright (c) 2026 Kenneth Stott
# Canary: 623440ee-fa31-4f58-a818-0d2aecd2b039
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""deploy-cloud-release.sh installs a release on cloud.provisa.dev from its own artifacts (REQ-1989).

The source hot-patch (deploy-cloud.sh) pushed this checkout's tree into a node whose image was
older, and failed whenever the two disagreed (a module the old image's Python could not find). A
release deploy runs the node's own installer for a tag and carries no source at all.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[2]
_SCRIPT = _REPO / "scripts" / "deploy-cloud-release.sh"
_LIB = _REPO / "scripts" / "cloud-node-lib.sh"
_HOTPATCH = _REPO / "scripts" / "deploy-cloud.sh"


def _function(name: str) -> str:
    text = _SCRIPT.read_text()
    found = re.search(rf"^{name}\(\) \{{\n.*?^\}}\n", text, re.S | re.M)
    assert found is not None, f"deploy-cloud-release.sh has no {name}"
    return found.group(0)


def _steps() -> list[str]:
    """The top-level calls the script makes, in order, after its last function."""
    text = _SCRIPT.read_text()
    tail = text[text.rindex("\n}\n") + 3 :]
    return re.findall(r"^([a-z_]+)(?: [^\n]*)?$", tail, re.M)


def _node_installer(tmp_path: Path) -> str:
    out = tmp_path / "node-install.sh"
    done = subprocess.run(
        ["bash", "-c", f'{_function("write_node_installer")}\nwrite_node_installer "$1"', "_", out],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert done.returncode == 0, done.stderr
    return out.read_text()


@pytest.mark.parametrize(
    "args", [[], ["latest"], ["0.1.0-alpha.513"], ["--check-only"], ["--check-only", "latest"]]
)
def test_a_release_tag_is_required(args):
    done = subprocess.run(["bash", str(_SCRIPT), *args], capture_output=True, text=True, timeout=30)
    assert done.returncode == 2
    assert "usage:" in done.stderr


def test_the_steps_run_in_order():
    steps = _steps()
    order = [
        "check_release",
        "wake_node",
        "wait_for_stack",
        "install_release",
        "recreate_stack",
        "reset_state",
        "verify",
    ]
    assert [s for s in steps if s in order] == order, steps
    reset = steps.index("reset_state")
    for check in (
        "verify_api",
        "verify_demo",
        "verify_engine",
        "verify_org_base",
        "verify_commerce",
    ):
        assert steps.index(check) > reset, (check, steps)


def test_the_engine_and_commerce_settings_are_written_before_the_recreate():
    steps = _steps()
    install, recreate = steps.index("install_release"), steps.index("recreate_stack")
    assert install < steps.index("push_engine_env") < recreate
    assert install < steps.index("push_commerce_env") < recreate
    # The running container is read only once the recreate has brought it up.
    assert recreate < steps.index("preflight_commerce")
    assert steps.count("preflight_engine") == 2
    assert recreate < len(steps) - 1 - steps[::-1].index("preflight_engine")


def test_the_release_engine_images_are_the_tag():
    assert 'push_engine_env "$TAG"' in _SCRIPT.read_text()


def test_no_source_is_pushed():
    # Comments name the hot-patch's steps to say why they are absent; calls are what count.
    text = re.sub(r"^\s*#.*$", "", _SCRIPT.read_text(), flags=re.M)
    for push in (
        "build_ui",
        "build_api",
        "build_cfg",
        "build_deps",
        "push_ui",
        "push_api",
        "push_cfg",
        "push_deps",
        "push_overlay",
        "stage_runtime_deps",
        "push_app",
        "push_cp_compose",
    ):
        assert not re.search(rf"\b{push}\b", text), push
    assert "deploy-cloud.sh" not in text
    # Nothing from the checkout is packed for the node: the one file sent is the installer
    # this script writes.
    # The one tar extracts the tag's tree here, for the render; nothing is packed.
    assert re.findall(r"\btar\s[^\n]*", text) == ['tar -x -C "$src" \\']
    assert re.findall(r"scp_node ([^ ]+)", text) == ['"$STAGE/node-install.sh"']


def test_the_installer_runs_the_rendered_startup_script_not_the_metadata_copy(tmp_path):
    # The instance's metadata holds the startup-script of whatever terraform apply last ran (491
    # when 513 was deployed: a .zip asset 513 does not ship, from the repo before its move). The
    # release's installer is rendered from the tag's own terraform/gcp-saas instead.
    installer = _node_installer(tmp_path)
    assert installer.rstrip().endswith('bash "$dir/startup.sh"')
    done = subprocess.run(["bash", "-n"], input=installer, capture_output=True, text=True)
    assert done.returncode == 0, done.stderr
    for text in (installer, _SCRIPT.read_text()):
        assert "computeMetadata" not in text
        assert "perl -pe" not in text


def test_the_installer_clears_what_would_stop_or_shadow_the_install(tmp_path):
    installer = _node_installer(tmp_path)
    run = installer.index('bash "$dir/startup.sh"')
    for cleared in (
        'rm -f "$overlay_ext"',
        "rm -f /root/.provisa/images/.version",
        "rm -f /var/lib/provisa-startup.sha256",
    ):
        assert installer.index(cleared) < run, cleared


def test_the_render_plans_the_tags_module_and_applies_nothing():
    body = _function("render_startup")
    assert 'git -C "$REPO" archive "$TAG"' in body
    plan = body[body.index("terraform plan") : body.index("-out=coordinator.tfplan")]
    for flag in (
        "-refresh=false",
        "-lock=false",
        "-target=google_compute_instance.coordinator",
        '"provisa_version=$TAG"',
        '"github_repo=$GITHUB_REPO"',
    ):
        assert flag in plan, flag
    # The plan is the render. Nothing changes the deployment's state or the instance's metadata.
    text = re.sub(r"^\s*#.*$", "", _SCRIPT.read_text(), flags=re.M)
    assert "terraform apply" not in text
    assert "add-metadata" not in text
    # The deployment's state is copied, never used in place.
    assert 'cd "$CLOUD_TF_DIR"' not in text
    assert 'install -m 600 "$f" "$tf/"' in body
    # Only an in-place update renders known values.
    assert "update | no-op) ;;" in body


def test_the_rendered_script_reaches_the_node_without_a_tmp_copy():
    body = _function("install_release")
    assert "umask 077 && cat > $RELEASE_DIR/startup.sh" in body
    assert '< "$STARTUP"' in body


def test_the_render_and_asset_check_come_before_the_node_is_touched():
    steps = _steps()
    assert steps.index("check_release") < steps.index("render_startup")
    assert steps.index("render_startup") < steps.index("check_startup_assets")
    assert steps.index("check_startup_assets") < steps.index("wake_node")
    text = _SCRIPT.read_text()
    check_only = text[text.index('if [ -n "$CHECK_ONLY" ]') :]
    assert check_only.index("exit 0") < check_only.index("wake_node")


_TAG = "v0.1.0-alpha.513"
_REPO_NAME = "simpleishard-llc/provisa"
_BASE = f"https://github.com/{_REPO_NAME}/releases/download/{_TAG}"
_MANIFEST = f"provisa-core-images-amd64-{_TAG}.sha256"
_PARTS = [
    f"provisa-core-image-provisa-amd64-{_TAG}.tar.gz.part1",
    f"provisa-core-image-provisa-amd64-{_TAG}.tar.gz.part2",
    f"provisa-core-image-redis-amd64-{_TAG}.tar.gz",
]
_NAMED = [
    f"Provisa-{_TAG}-linux-x86_64.AppImage",
    _MANIFEST,
    f"provisa-trino-plugins-{_TAG}.tar.gz",
]


def _startup(base: str = _BASE, extra: str = "") -> str:
    return (
        "#!/bin/bash\n"
        f'gh_base="{base}"\n'
        f'curl -fSL "$gh_base/{_NAMED[0]}" -o /opt/Provisa.AppImage\n'
        f'curl -fSL "$gh_base/{_MANIFEST}" -o /opt/{_MANIFEST}\n'
        f"for asset in $(awk '{{print $2}}' /opt/{_MANIFEST}); do\n"
        '  curl -fSL "$gh_base/$asset" -o "/opt/$asset"\n'
        "done\n"
        f'curl -fSL "$gh_base/{_NAMED[2]}" -o /opt/{_NAMED[2]}\n'
        f"{extra}"
        f'export PROVISA_VERSION="{_TAG}"\n'
    )


def _check_assets(
    tmp_path: Path, startup: str, assets: list[str], manifest: list[str] | None = None
) -> subprocess.CompletedProcess[str]:
    """check_startup_assets against a release whose assets are `assets`; `gh` serves its manifest."""
    stage = tmp_path / "stage"
    stage.mkdir()
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    published = tmp_path / "published"
    published.mkdir()
    listed = manifest if manifest is not None else _PARTS
    (published / _MANIFEST).write_text("".join(f"{'0' * 64}  {name}\n" for name in listed))
    # gh release download <tag> --repo <r> --pattern <name> --dir <dir> --clobber
    gh = bin_dir / "gh"
    gh.write_text(
        "#!/bin/bash\n"
        'while [ $# -gt 0 ]; do case "$1" in --pattern) p="$2"; shift ;; --dir) d="$2"; shift ;;'
        " esac; shift; done\n"
        f'cp "{published}/$p" "$d/$p"\n'
    )
    gh.chmod(0o755)
    script = tmp_path / "startup.sh"
    script.write_text(startup)
    listing = tmp_path / "assets"
    listing.write_text("".join(f"{a}\n" for a in assets))
    return subprocess.run(
        [
            "bash",
            "-c",
            f'{_function("check_startup_assets")}\ncheck_startup_assets "$@"',
            "_",
            str(script),
            str(listing),
            _REPO_NAME,
            _TAG,
        ],
        capture_output=True,
        text=True,
        timeout=30,
        env={"PATH": f"{bin_dir}:/usr/bin:/bin", "STAGE": str(stage)},
    )


def test_every_download_of_a_complete_release_passes(tmp_path):
    done = _check_assets(tmp_path, _startup(), _NAMED + _PARTS)
    assert done.returncode == 0, done.stderr
    # The manifest's own download plus the AppImage, the plugins and every listed part.
    assert f"{len(_NAMED) + len(_PARTS)} downloads" in done.stdout


def test_a_named_asset_the_release_lacks_is_refused_by_name(tmp_path):
    done = _check_assets(tmp_path, _startup(), [_NAMED[0], _MANIFEST, *_PARTS])
    assert done.returncode == 1
    assert f"release {_TAG} has no asset {_NAMED[2]}" in done.stderr


def test_a_manifest_entry_the_release_lacks_is_refused_by_name(tmp_path):
    done = _check_assets(tmp_path, _startup(), _NAMED + _PARTS[:1] + _PARTS[2:])
    assert done.returncode == 1
    assert f"has no asset {_PARTS[1]} (listed in {_MANIFEST})" in done.stderr


def test_a_missing_manifest_is_refused_by_name(tmp_path):
    done = _check_assets(tmp_path, _startup(), [_NAMED[0], _NAMED[2], *_PARTS])
    assert done.returncode == 1
    assert f"has no asset {_MANIFEST}" in done.stderr


def test_the_old_zip_layout_is_refused_by_name(tmp_path):
    # What the 491 metadata copy fetched for 513 (deploy-release-513b.log: curl 404).
    zip_name = f"provisa-core-images-amd64-{_TAG}.zip"
    startup = (
        f'gh_base="{_BASE}"\n'
        f'curl -fSL "$gh_base/{_NAMED[0]}" -o /opt/Provisa.AppImage\n'
        f'curl -fSL "$gh_base/{zip_name}" -o /opt/{zip_name}\n'
    )
    done = _check_assets(tmp_path, startup, _NAMED + _PARTS)
    assert done.returncode == 1
    assert f"has no asset {zip_name}" in done.stderr


def test_a_download_from_another_repository_is_refused(tmp_path):
    old = f"https://github.com/kenstott/provisa/releases/download/{_TAG}"
    done = _check_assets(tmp_path, _startup(base=old), _NAMED + _PARTS)
    assert done.returncode == 1
    assert f"downloads from {old}, not {_BASE}" in done.stderr


def test_a_release_url_outside_gh_base_is_refused(tmp_path):
    extra = f'curl -fSL "{_BASE}/other.tar.gz" -o /opt/other.tar.gz\n'
    done = _check_assets(tmp_path, _startup(extra=extra), _NAMED + _PARTS)
    assert done.returncode == 1
    assert "a release URL other than gh_base" in done.stderr


def test_a_download_name_it_cannot_resolve_is_refused(tmp_path):
    extra = 'curl -fSL "$gh_base/$other" -o /opt/other\n'
    done = _check_assets(tmp_path, _startup(extra=extra), _NAMED + _PARTS)
    assert done.returncode == 1
    assert "$gh_base/$other, a name this check cannot resolve" in done.stderr


def test_a_script_with_no_downloads_is_refused(tmp_path):
    done = _check_assets(tmp_path, f'gh_base="{_BASE}"\n', _NAMED + _PARTS)
    assert done.returncode == 1
    assert "downloads nothing" in done.stderr


def test_the_install_runs_detached_and_is_checked_after():
    body = _function("install_release")
    assert "systemd-run --unit $INSTALL_UNIT" in body
    assert body.index("systemd-run") < body.index("$SENTINEL") < body.index("$IMAGES_MARKER")


def test_wake_hits_the_site_until_running_then_waits_for_ssh():
    body = _function("wake_node")
    assert '"$SITE/health"' in body
    assert body.index("RUNNING") < body.index("ssh_node true")
    assert "instances start" not in body  # the front door wakes it, as a visitor's hit does


def test_the_stack_is_awaited_before_its_environment_is_read():
    steps = _steps()
    assert steps.index("wait_for_stack") < steps.index("preflight_engine")
    body = _function("wait_for_stack")
    assert (
        body.index("google-startup-scripts")
        < body.index("$API_CONTAINER")
        < body.index("verify_api")
    )


def test_the_hot_patch_still_sources_the_shared_library():
    text = _HOTPATCH.read_text()
    assert '. "$REPO/scripts/cloud-node-lib.sh"' in text
    lib = _LIB.read_text()
    for name in ("ssh_node", "preflight_engine", "reset_state", "verify_demo", "verify_engine"):
        assert re.search(rf"^{name}\(\) \{{", lib, re.M), name
        assert not re.search(rf"^{name}\(\) \{{", text, re.M), name


@pytest.mark.parametrize("path", [_SCRIPT, _LIB], ids=lambda p: p.name)
def test_no_check_stops_reading_a_pipe_early(path):
    # Under pipefail `producer | grep -q` fails whenever the producer was still writing.
    early = [
        line
        for line in path.read_text().splitlines()
        if re.search(r"\|\s*grep\s+-[A-Za-z]*q", line)
    ]
    assert not early, early


@pytest.mark.parametrize("path", [_SCRIPT, _LIB], ids=lambda p: p.name)
def test_the_scripts_parse(path):
    done = subprocess.run(["bash", "-n", str(path)], capture_output=True, text=True)
    assert done.returncode == 0, done.stderr


# --- the release's engine images reach the installer (the 513 deploy, 2026-11-28) ---------------
#
# The render planned the tag's module with the deployment's terraform.tfvars, whose engine_image
# and zaychik_image are whatever the last apply pinned (ghcr.io/kenstott/...:alpha.391/344). The
# rendered installer exported those, the installer's stack booted on them, and boot's
# converge_boot_shard rolled the shard to the 391 engine: the release's own images never ran.

_ENGINE = f"ghcr.io/simpleishard-llc/provisa-trino-engine:{_TAG}"
_ZAYCHIK = f"ghcr.io/simpleishard-llc/provisa-zaychik:{_TAG}"


def test_the_render_pins_the_release_engine_images():
    body = _function("render_startup")
    plan = body[body.index("terraform plan") : body.index("-out=coordinator.tfplan")]
    assert '"engine_image=$IMAGE_REGISTRY/provisa-trino-engine:$TAG"' in plan
    assert '"zaychik_image=$IMAGE_REGISTRY/provisa-zaychik:$TAG"' in plan
    # And the rendered script is checked to carry them, by the function the tests below run.
    assert 'check_startup_images "$STARTUP" "$TAG"' in body


def _check_images(tmp_path: Path, exports: str) -> subprocess.CompletedProcess[str]:
    script = tmp_path / "startup.sh"
    script.write_text(_startup(extra=exports))
    return subprocess.run(
        [
            "bash",
            "-c",
            f"IMAGE_REGISTRY=ghcr.io/simpleishard-llc\n{_function('check_startup_images')}\n"
            'check_startup_images "$@"',
            "_",
            str(script),
            _TAG,
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )


def test_a_rendered_installer_on_the_release_images_passes(tmp_path):
    done = _check_images(
        tmp_path,
        f'    export PROVISA_ENGINE_IMAGE="{_ENGINE}"\n    export PROVISA_ZAYCHIK_IMAGE="{_ZAYCHIK}"\n',
    )
    assert done.returncode == 0, done.stderr


@pytest.mark.parametrize(
    ("exports", "named"),
    [
        (
            '    export PROVISA_ENGINE_IMAGE="ghcr.io/kenstott/provisa-trino-engine:v0.1.0-alpha.391"\n'
            f'    export PROVISA_ZAYCHIK_IMAGE="{_ZAYCHIK}"\n',
            "PROVISA_ENGINE_IMAGE",
        ),
        (
            f'    export PROVISA_ENGINE_IMAGE="{_ENGINE}"\n'
            '    export PROVISA_ZAYCHIK_IMAGE="ghcr.io/kenstott/provisa-zaychik:v0.1.0-alpha.344"\n',
            "PROVISA_ZAYCHIK_IMAGE",
        ),
        (f'    export PROVISA_ZAYCHIK_IMAGE="{_ZAYCHIK}"\n', "PROVISA_ENGINE_IMAGE"),
    ],
    ids=["old-engine", "old-zaychik", "no-engine"],
)
def test_a_rendered_installer_on_other_images_is_refused_by_name(tmp_path, exports, named):
    done = _check_images(tmp_path, exports)
    assert done.returncode != 0
    assert named in done.stderr, done.stderr


# --- every exit names its reason (the 513 deploy exited 1 after "== commerce env", silently) -----


def test_an_unexpected_failure_names_the_command_and_line(tmp_path):
    text = _SCRIPT.read_text()
    assert re.search(r"^set -Eeuo pipefail$", text, re.M)
    trap = re.search(r"^trap '[^\n]*' ERR$", text, re.M)
    assert trap is not None, "deploy-cloud-release.sh sets no ERR trap"
    done = subprocess.run(
        [
            "bash",
            "-c",
            f"set -Eeuo pipefail\n{trap.group(0)}\n"
            'step() { local v; v="$(sed -n p /nonexistent/.env 2>/dev/null | tail -1)"; }\n'
            "step\necho unreachable\n",
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert done.returncode != 0
    assert "unreachable" not in done.stdout
    assert "/nonexistent/.env" in done.stderr, done.stderr
    assert "line" in done.stderr, done.stderr
