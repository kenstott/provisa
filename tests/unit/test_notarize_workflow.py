# Copyright (c) 2026 Kenneth Stott
# Canary: 3b6f0c52-7d1e-4a8b-9c40-5e2a1f7d6b83
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""macOS notarization is its own job, rerunnable alone.

On v0.1.0-alpha.508 (run 42951512721) the Obs DMG build hit its 45-minute limit while notarytool
waited 44 minutes on Apple's notary queue, and rerunning it rebuilt the DMG. The DMG build jobs
build and sign; the notarize job notarizes and staples every DMG under its own limit; and the
notarize workflow can be dispatched on its own for a version.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest
import yaml

_REPO = Path(__file__).resolve().parents[2]
_WORKFLOWS = _REPO / ".github/workflows"
_BUILD_SCRIPTS = (
    "packaging/macos/build-dmg.sh",
    "packaging/macos/build-dmg-obs.sh",
    "packaging/macos/build-dmg-demo.sh",
)
_BUILD_JOBS = {
    "build-macos-core": "macos-core",
    "build-macos-container": "macos-container",
    "build-macos-obs": "macos-obs",
    "build-macos-demo": "macos-demo",
}
_SECRETS = ("APPLE_NOTARYTOOL_APPLE_ID", "APPLE_NOTARYTOOL_PASSWORD", "APPLE_NOTARYTOOL_TEAM_ID")
# Apple's queue took 44 minutes for the Obs DMG of alpha.508; the build limit was 45.
_BUILD_LIMIT = 45


def _load(name: str) -> dict:
    data = yaml.safe_load((_WORKFLOWS / name).read_text())
    # PyYAML reads the bare key `on` as True.
    data["on"] = data.pop(True)
    return data


def _is_upload(step: dict) -> bool:
    return str(step.get("uses", "")).startswith("actions/upload-artifact")


@pytest.mark.parametrize("script", _BUILD_SCRIPTS)
def test_a_dmg_build_script_does_not_notarize(script):
    lines = (_REPO / script).read_text().splitlines()
    code = "\n".join(line for line in lines if not line.lstrip().startswith("#"))
    for word in ("notarytool", "stapler", "staple_retry", "notary-lib", "notarize"):
        assert word not in code, f"{script} still uses {word}"


@pytest.mark.parametrize("job_id", _BUILD_JOBS)
def test_a_dmg_build_job_does_not_notarize_and_hands_its_dmg_on_unnotarized(job_id):
    job = _load("release-macos.yml")["jobs"][job_id]
    text = yaml.safe_dump(job["steps"])
    for secret in _SECRETS:
        assert secret not in text, f"{job_id} still gets {secret}"
    assert "notarize" not in text.lower().replace("-unnotarized", "")
    uploads = [s for s in job["steps"] if _is_upload(s)]
    assert [u["with"]["name"] for u in uploads] == [f"{_BUILD_JOBS[job_id]}-unnotarized"]
    # Not the artifact the release attaches: that one is the notarized DMG's.
    assert not uploads[0]["with"]["name"].endswith("-dist")


def test_the_unnotarized_dmgs_outlive_a_notarization_that_is_run_again_later():
    jobs = _load("release-macos.yml")["jobs"]
    for job_id in _BUILD_JOBS:
        for step in filter(_is_upload, jobs[job_id]["steps"]):
            assert step["with"]["retention-days"] >= 3


def test_the_release_macos_leg_runs_the_notarize_workflow_after_every_dmg_build():
    job = _load("release-macos.yml")["jobs"]["notarize"]
    assert job["uses"] == "./.github/workflows/notarize-macos.yml"
    assert set(_BUILD_JOBS) <= set(job["needs"])
    assert job["with"]["version"] == "${{ inputs.version }}"
    assert set(job["secrets"]) == set(_SECRETS)
    for secret in _SECRETS:
        assert job["secrets"][secret] == f"${{{{ secrets.{secret} }}}}"


def test_the_notarize_job_notarizes_and_staples_every_dmg_under_its_own_limit():
    job = _load("notarize-macos.yml")["jobs"]["notarize"]
    assert job["runs-on"].startswith("macos")
    # Longer than the build jobs' limit that cancelled the notarization.
    assert job["timeout-minutes"] > _BUILD_LIMIT
    # Every DMG: one matrix leg per artifact the build jobs hand on.
    legs = job["strategy"]["matrix"]["include"]
    assert {leg["artifact"] for leg in legs} == set(_BUILD_JOBS.values())
    assert {leg["dmg"] for leg in legs} == {
        "${{ needs.metadata.outputs.dmg_name }}",
        "${{ needs.metadata.outputs.dmg_container_name }}",
        "${{ needs.metadata.outputs.dmg_obs_name }}",
        "${{ needs.metadata.outputs.dmg_demo_name }}",
    }
    assert job["strategy"]["fail-fast"] is False
    steps = {s["name"]: s for s in job["steps"]}
    download = steps["Download the unnotarized DMG"]["with"]
    assert download["name"] == "${{ matrix.artifact }}-unnotarized"
    notarize = steps["Notarize and staple"]
    assert "packaging/macos/notarize-dmg.sh" in notarize["run"]
    assert '"dmg/${{ matrix.dmg }}"' in notarize["run"]
    assert set(notarize["env"]) == set(_SECRETS)
    for secret in _SECRETS:
        assert notarize["env"][secret] == f"${{{{ secrets.{secret} }}}}"
    # What the release attaches: the *-dist artifact, now the notarized DMG.
    upload = steps["Upload notarized DMG artifact"]["with"]
    assert upload["name"] == "${{ matrix.artifact }}-dist"
    assert upload["path"] == "dmg/${{ matrix.dmg }}"


def test_core_and_container_notarize_the_app_then_build_the_dmg_from_it_then_notarize_the_dmg():
    job = _load("notarize-macos.yml")["jobs"]["notarize"]
    legs = {leg["artifact"]: leg for leg in job["strategy"]["matrix"]["include"]}
    assert legs["macos-core"]["app_edition"] == "core"
    assert legs["macos-container"]["app_edition"] == "container"
    assert legs["macos-obs"]["app_edition"] == ""
    assert legs["macos-demo"]["app_edition"] == ""
    assert legs["macos-core"]["built_dmg"] == "Provisa.dmg"
    assert legs["macos-container"]["built_dmg"] == "Provisa-Container.dmg"
    names = [s["name"] for s in job["steps"]]
    assert names.index("Notarize and staple the app") < names.index(
        "Build the DMG from the stapled app"
    )
    assert names.index("Build the DMG from the stapled app") < names.index("Notarize and staple")
    steps = {s["name"]: s for s in job["steps"]}
    app = steps["Notarize and staple the app"]
    assert app["if"] == "${{ matrix.app_edition != '' }}"
    assert "notarize-dmg.sh --app packaging/macos/Provisa.app" in app["run"]
    assert 'tar -xf "dmg/stage-${{ matrix.app_edition }}.tar"' in app["run"]
    assert set(app["env"]) == set(_SECRETS)
    build = steps["Build the DMG from the stapled app"]
    assert build["if"] == app["if"]
    assert "build-dmg.sh dmg" in build["run"]
    assert build["env"] == {"PROVISA_EDITION": "${{ matrix.app_edition }}"}
    assert (
        'mv "packaging/macos/dist/${{ matrix.built_dmg }}" "dmg/${{ matrix.dmg }}"' in build["run"]
    )
    # The step that creates the DMG holds no notarization secret.
    assert not any(secret in str(build) for secret in _SECRETS)


@pytest.mark.parametrize(
    ("job_id", "edition"), [("build-macos-core", "core"), ("build-macos-container", "container")]
)
def test_the_core_and_container_build_jobs_hand_on_the_signed_app_not_a_dmg(job_id, edition):
    job = _load("release-macos.yml")["jobs"][job_id]
    runs = [s["run"] for s in job["steps"] if "run" in s]
    assert any("packaging/macos/build-dmg.sh stage" in r for r in runs)
    assert not any(".dmg" in r for r in runs)
    (upload,) = [s for s in job["steps"] if _is_upload(s)]
    assert upload["with"]["path"] == f"packaging/macos/dist/stage-{edition}.tar"
    assert job["timeout-minutes"] == _BUILD_LIMIT


def test_build_dmg_stages_the_app_and_the_native_payload_and_makes_the_dmg_in_a_later_step():
    source = (_REPO / "packaging/macos/build-dmg.sh").read_text()
    assert 'STAGE_TAR="${OUT_DIR}/stage-${EDITION}.tar"' in source
    assert "members+=(native-payload/runtime)" in source
    assert "stage|dmg|all) ;;" in source
    main = source.split("main() {")[1]
    # `stage` stops before the DMG; `dmg` skips the build and signing.
    assert main.index("stage_app") < main.index("create_dmg")
    assert 'if [ "$step" != "dmg" ]; then' in main
    assert 'if [ "$step" = "stage" ]; then' in main


def test_the_notarize_script_waits_less_than_its_job_limit_and_staples_through_the_retry():
    job = _load("notarize-macos.yml")["jobs"]["notarize"]
    source = (_REPO / "packaging/macos/notarize-dmg.sh").read_text()
    wait = int(source.split("NOTARY_TIMEOUT_MINUTES:-")[1].split("}")[0])
    assert wait < job["timeout-minutes"]
    assert "xcrun notarytool submit" in source
    assert "staple_retry " in source
    assert "xcrun stapler staple" not in source


def test_the_notarize_workflow_can_be_dispatched_on_its_own_for_a_version():
    workflow = _load("notarize-macos.yml")
    dispatch = workflow["on"]["workflow_dispatch"]["inputs"]
    assert dispatch["version"]["required"] is True
    assert dispatch["run_id"]["default"] == ""
    assert dispatch["attach"]["default"] is False
    # And called by the release leg.
    call = workflow["on"]["workflow_call"]
    assert call["inputs"]["version"]["required"] is True
    assert set(call["secrets"]) == set(_SECRETS)
    # Nothing is built: no step of the workflow runs a DMG build script.
    assert "build-dmg" not in yaml.safe_dump(workflow["jobs"])
    # The release metadata, not a hand-typed name, says which DMGs a version has.
    meta = workflow["jobs"]["metadata"]["steps"][-1]
    assert meta["uses"] == "./.github/actions/release-metadata"
    assert meta["with"]["version"] == "${{ inputs.version }}"


def test_a_manual_run_reads_the_dmgs_of_the_run_that_built_them():
    steps = {s["name"]: s for s in _load("notarize-macos.yml")["jobs"]["notarize"]["steps"]}
    download = steps["Download the unnotarized DMG"]["with"]
    assert download["run-id"] == "${{ inputs.run_id || github.run_id }}"
    assert download["github-token"] == "${{ github.token }}"
    attach = steps["Attach the notarized DMG to the release"]
    assert attach["if"] == "${{ github.event_name == 'workflow_dispatch' && inputs.attach }}"


def test_the_notarize_workflow_never_prints_a_secret():
    text = (_WORKFLOWS / "notarize-macos.yml").read_text()
    script = (_REPO / "packaging/macos/notarize-dmg.sh").read_text()
    # Secrets reach the script only as env of the one step, and nothing echoes them.
    assert text.count("secrets.APPLE_NOTARYTOOL_PASSWORD") == 1
    for line in script.splitlines():
        if line.lstrip().startswith(("info", "ok", "err", "echo", "printf")):
            assert "APPLE_NOTARYTOOL_PASSWORD" not in line


def _run_script(
    tmp_path: Path, env: dict[str, str], submit_exit: int = 0, target: str = "Provisa.dmg"
):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    calls = tmp_path / "calls"
    calls.write_text("")
    fake = bin_dir / "xcrun"
    fake.write_text(
        "#!/bin/bash\n"
        f'echo "$*" >> {calls}\n'
        'case "$1 $2" in\n'
        f'  "notarytool submit") [ {submit_exit} = 0 ] && echo \'{{"id": "abc-123"}}\'; '
        f"exit {submit_exit} ;;\n"
        '  "notarytool info") echo \'{"status": "Accepted"}\' ;;\n'
        '  "notarytool log") echo \'{"issues": null}\' ;;\n'
        '  "stapler staple") echo "The staple and validate action worked!" ;;\n'
        "esac\n"
    )
    fake.chmod(0o755)
    (bin_dir / "sleep").write_text("#!/bin/sh\nexit 0\n")
    (bin_dir / "sleep").chmod(0o755)
    # ditto is macOS's; the fake writes the zip the real one would.
    (bin_dir / "ditto").write_text(f'#!/bin/bash\necho "ditto $*" >> {calls}\n: > "${{@: -1}}"\n')
    (bin_dir / "ditto").chmod(0o755)
    path = tmp_path / target
    if target.endswith(".app"):
        path.mkdir()
        args = ["--app", str(path)]
    else:
        path.write_bytes(b"dmg")
        args = [str(path)]
    proc = subprocess.run(
        ["bash", str(_REPO / "packaging/macos/notarize-dmg.sh"), *args],
        capture_output=True,
        text=True,
        env={"PATH": f"{bin_dir}:/usr/bin:/bin", **env},
    )
    return proc, calls.read_text().splitlines()


_CREDS = {
    "APPLE_NOTARYTOOL_APPLE_ID": "dev@example.com",
    "APPLE_NOTARYTOOL_PASSWORD": "s3cret-app-password",
    "APPLE_NOTARYTOOL_TEAM_ID": "TEAM123",
}


def test_the_script_submits_waits_reads_the_log_then_staples_the_dmg(tmp_path):
    proc, calls = _run_script(tmp_path, _CREDS)
    assert proc.returncode == 0, proc.stderr
    verbs = [" ".join(c.split()[:2]) for c in calls]
    assert verbs == ["notarytool submit", "notarytool info", "notarytool log", "stapler staple"]
    assert calls[0].split()[2].endswith("Provisa.dmg")
    assert calls[-1].endswith("Provisa.dmg")
    assert "s3cret-app-password" not in proc.stdout + proc.stderr


def test_the_script_fails_when_the_dmg_is_missing(tmp_path):
    proc = subprocess.run(
        ["bash", str(_REPO / "packaging/macos/notarize-dmg.sh"), str(tmp_path / "none.dmg")],
        capture_output=True,
        text=True,
        env={"PATH": "/usr/bin:/bin", **_CREDS},
    )
    assert proc.returncode == 1
    assert "DMG not found" in proc.stderr


def test_the_script_fails_when_notarytool_submit_fails(tmp_path):
    proc, calls = _run_script(tmp_path, _CREDS, submit_exit=1)
    assert proc.returncode == 1
    assert [c for c in calls if c.startswith("stapler")] == []
    assert "s3cret-app-password" not in proc.stdout + proc.stderr


def test_the_script_skips_without_the_notarization_account_and_staples_nothing(tmp_path):
    proc, calls = _run_script(tmp_path, {})
    assert proc.returncode == 0, proc.stderr
    assert "skipping notarization" in proc.stdout
    assert calls == []


def test_the_script_zips_submits_and_staples_an_app_bundle(tmp_path):
    proc, calls = _run_script(tmp_path, _CREDS, target="Provisa.app")
    assert proc.returncode == 0, proc.stderr
    assert calls[0].startswith("ditto -c -k --keepParent ")
    verbs = [" ".join(c.split()[:2]) for c in calls[1:]]
    assert verbs == ["notarytool submit", "notarytool info", "notarytool log", "stapler staple"]
    # The zip is what is submitted; the bundle itself is what is stapled.
    assert calls[1].split()[2].endswith("-notarize.zip")
    assert calls[-1].endswith("Provisa.app")
    assert "s3cret-app-password" not in proc.stdout + proc.stderr


def test_the_script_fails_when_the_app_bundle_is_missing(tmp_path):
    proc = subprocess.run(
        ["bash", str(_REPO / "packaging/macos/notarize-dmg.sh"), "--app", str(tmp_path / "x.app")],
        capture_output=True,
        text=True,
        env={"PATH": "/usr/bin:/bin", **_CREDS},
    )
    assert proc.returncode == 1
    assert "App bundle not found" in proc.stderr
