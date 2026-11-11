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
    # Core also hands on the DuckDB extensions asset: no code Apple notarizes (it never rides in a
    # DMG), so the release attaches it as built.
    extensions = ["macos-duckdb-extensions-dist"] if job_id == "build-macos-core" else []
    assert [u["with"]["name"] for u in uploads] == [
        f"{_BUILD_JOBS[job_id]}-unnotarized",
        *extensions,
    ]
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
    # The handoff is verified right after the untar and before anything is submitted: the .app and,
    # for Core, the native runtime the DMG carries beside it (verify_signed in build-dmg.sh).
    run = app["run"]
    verify = 'PROVISA_EDITION="${{ matrix.app_edition }}" packaging/macos/build-dmg.sh verify'
    assert run.index("tar -xf") < run.index(verify) < run.index("notarize-dmg.sh --app")
    assert "does not verify after the handoff" in run
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
    (upload,) = [
        s for s in job["steps"] if _is_upload(s) and s["with"]["name"].endswith("-unnotarized")
    ]
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


# ── The native runtime the Core DMG carries beside the .app (run 43018393659) ──────────────────
# Apple rejected the Core DMG of alpha.509: its hidden runtime/ held some 500 Mach-O files that were
# not Developer ID signed with a secure timestamp. The stage step signs them all; the notarize job
# verifies them after the handoff, naming each that fails.

_THIN = b"\xcf\xfa\xed\xfe" + b"\x00" * 28
_FAT = b"\xca\xfe\xba\xbe\x00\x00\x00\x02" + b"\x00" * 24
_JAVA_CLASS = b"\xca\xfe\xba\xbe\x00\x00\x00\x34" + b"\x00" * 24


#: Every kind of DuckDB extension file the runtime holds: a core wheel's blob, provisa-duckdb-ext's
#: blob with its .info, and a named repository's extension with its trust anchor.
_EXTENSIONS = {
    "lib/site-packages/duckdb_extension_httpfs/extensions/v2.0.0/httpfs.duckdb_extension": _THIN,
    "lib/site-packages/provisa_duckdb_ext/_ext/v2.0.0/osx_arm64/mongo.duckdb_extension": _THIN,
    "lib/site-packages/provisa_duckdb_ext/_ext/v2.0.0/osx_arm64/mongo.duckdb_extension.info": b"i",
    "lib/site-packages/provisa_duckdb_ext/_ext/v2.0.0/osx_arm64/repositories/query_farm/"
    "airport.duckdb_extension": _THIN,
    "lib/site-packages/provisa_duckdb_ext/_ext/v2.0.0/osx_arm64/repositories/query_farm/"
    "query_farm.duckdb_extension.repo.json": b"{}",
}


def _payload(tmp_path: Path, extensions: bool = False) -> Path:
    """A build-dmg.sh beside a native-payload/runtime holding each kind of file it meets; with
    ``extensions``, the DuckDB extension files too (as pip installs them, before the split)."""
    macos = tmp_path / "macos"
    runtime = macos / "native-payload/runtime"
    (macos / "Provisa.app/Contents/MacOS").mkdir(parents=True)
    for rel, body in {
        "bin/python3.12": _THIN,
        "lib/libpython3.12.dylib": _THIN,
        "lib/site-packages/zstandard/backend_c.cpython-312-darwin.so": _THIN,
        "lib/site-packages/fat/_fat.abi3.so": _FAT,
        "lib/site-packages/jpype/Foo.class": _JAVA_CLASS,
        "lib/site-packages/zstandard/__init__.py": b"import os\n",
        **(_EXTENSIONS if extensions else {}),
    }.items():
        (runtime / rel).parent.mkdir(parents=True, exist_ok=True)
        (runtime / rel).write_bytes(body)
    (runtime / "bin/python3").symlink_to("python3.12")
    for name in ("build-dmg.sh", "entitlements.plist"):
        (macos / name).write_bytes((_REPO / "packaging/macos" / name).read_bytes())
    return macos


def _fake_tools(tmp_path: Path, unsigned: str = "") -> Path:
    """codesign logs each call; `-dv` reports a secure timestamp, and any file whose path contains
    ``unsigned`` fails --verify. `file` calls python3.12 an executable, everything else a bundle."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "codesign").write_text(
        "#!/bin/bash\n"
        f'echo "$*" >> {tmp_path}/calls\n'
        'last="${*: -1}"\n'
        f'if [ -n "{unsigned}" ] && [[ "$last" == *"{unsigned}"* ]]; then exit 1; fi\n'
        'if [ "$1" = "-dv" ]; then printf "Identifier=x\\nTimestamp=Nov 11, 2026\\n" >&2; fi\n'
        "exit 0\n"
    )
    (bin_dir / "file").write_text(
        '#!/bin/bash\n[[ "$2" == *python3.12 ]] && echo "Mach-O 64-bit executable arm64" '
        '|| echo "Mach-O 64-bit bundle arm64"\n'
    )
    for tool in ("codesign", "file"):
        (bin_dir / tool).chmod(0o755)
    (tmp_path / "calls").write_text("")
    return bin_dir


def _build_dmg(macos: Path, bin_dir: Path, program: str) -> subprocess.CompletedProcess:
    """Run ``program`` in bash with build-dmg.sh's functions defined (its main is not run)."""
    lib = macos / "lib.sh"
    lib.write_text((macos / "build-dmg.sh").read_text().replace('\nmain "$@"\n', "\n"))
    return subprocess.run(
        ["bash", "-c", f'set -euo pipefail\nsource "{lib}"\n{program}'],
        capture_output=True,
        text=True,
        env={"PATH": f"{bin_dir}:/usr/bin:/bin", "PROVISA_EDITION": "core"},
    )


def test_macho_files_finds_every_mach_o_by_magic_and_nothing_else(tmp_path):
    macos = _payload(tmp_path, extensions=True)
    runtime = macos / "native-payload/runtime"
    result = _build_dmg(macos, _fake_tools(tmp_path), f'macho_files "{runtime}"')
    assert result.returncode == 0, result.stderr
    found = {Path(line).relative_to(runtime).as_posix() for line in result.stdout.split()}
    assert found == {
        "bin/python3.12",
        "lib/libpython3.12.dylib",
        "lib/site-packages/zstandard/backend_c.cpython-312-darwin.so",
        "lib/site-packages/fat/_fat.abi3.so",
        *(rel for rel in _EXTENSIONS if rel.endswith(".duckdb_extension")),
    }


def test_the_native_runtime_is_signed_like_the_app_with_executables_last(tmp_path):
    macos = _payload(tmp_path, extensions=True)
    result = _build_dmg(
        macos,
        _fake_tools(tmp_path),
        "APPLE_DEVELOPER_ID='Developer ID Application: X'\n"
        "split_duckdb_extensions\nsign_native_payload",
    )
    assert result.returncode == 0, result.stderr
    calls = (tmp_path / "calls").read_text().splitlines()
    flags = "--force --sign Developer ID Application: X --options runtime --timestamp"
    assert all(call.startswith(flags) for call in calls), calls
    signed = " ".join(calls)
    for rel in ("libpython3.12.dylib", "backend_c.cpython-312-darwin.so", "_fat.abi3.so"):
        assert rel in signed
    # codesign refuses a DuckDB-signed extension: none is left in the runtime to pass to it.
    assert "duckdb_extension" not in signed
    # The executable is signed after the libraries it loads, with the .app's entitlements.
    assert "python3.12" in calls[-1]
    assert f"--entitlements {macos}/entitlements.plist" in calls[-1]
    assert not any("--entitlements" in call for call in calls[:-1])


def test_the_build_signs_the_native_runtime_before_the_stage_tar_carries_it():
    source = (_REPO / "packaging/macos/build-dmg.sh").read_text()
    main = source.split("main() {")[1]
    assert main.index("sign_app") < main.index("sign_native_payload") < main.index("stage_app")
    # One signing routine: every codesign_retry call goes through sign_code.
    code = "\n".join(line for line in source.splitlines() if not line.lstrip().startswith("#"))
    assert code.count("codesign_retry ") == 1
    assert (
        'codesign_retry --force --sign "${APPLE_DEVELOPER_ID}" --options runtime --timestamp'
        in code
    )


def test_the_handoff_verify_passes_when_every_mach_o_is_signed(tmp_path):
    macos = _payload(tmp_path)
    result = _build_dmg(macos, _fake_tools(tmp_path), "main verify")
    assert result.returncode == 0, result.stderr
    calls = (tmp_path / "calls").read_text()
    assert f"--verify --deep --strict {macos}/Provisa.app" in calls
    assert "-R=anchor apple generic" in calls
    assert "backend_c.cpython-312-darwin.so" in calls
    # No DuckDB extension is in the runtime, so none is listed apart.
    assert "duckdb_extension" not in calls + result.stdout + result.stderr
    assert "Not signable" not in (_REPO / "packaging/macos/build-dmg.sh").read_text()


def test_the_handoff_verify_fails_and_names_a_duckdb_extension_left_in_the_runtime(tmp_path):
    macos = _payload(tmp_path, extensions=True)
    result = _build_dmg(macos, _fake_tools(tmp_path), "main verify")
    assert result.returncode == 1
    for rel in _EXTENSIONS:
        assert (
            f"native-payload/runtime/{rel} is a DuckDB extension: it ships in "
            "provisa-duckdb-extensions-dev-macOS-arm64.tar.gz, not the DMG." in result.stderr
        )


def test_the_handoff_verify_fails_and_names_an_unsigned_runtime_binary(tmp_path):
    macos = _payload(tmp_path)
    result = _build_dmg(macos, _fake_tools(tmp_path, unsigned="backend_c"), "main verify")
    assert result.returncode == 1
    assert (
        "native-payload/runtime/lib/site-packages/zstandard/backend_c.cpython-312-darwin.so "
        "is not Developer ID signed with a secure timestamp after the handoff." in result.stderr
    )


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
    # Nothing is rebuilt: the only build script it runs makes a DMG from the stapled .app.
    jobs = yaml.safe_dump(workflow["jobs"])
    assert "build-dmg.sh stage" not in jobs
    assert "build-dmg-obs" not in jobs
    assert "build-dmg-demo" not in jobs
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
    # Secrets reach the script only as env of the two notarizing steps, and nothing echoes them.
    assert text.count("secrets.APPLE_NOTARYTOOL_PASSWORD") == 2  # the app step, the DMG step
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


# ── The DuckDB extensions leave the Core DMG for a release asset of their own ───────────────────
# codesign refuses them (DuckDB's signature follows the Mach-O image), and Apple rejected the DMG
# that held them unsigned: "The signature of the binary is invalid" (maintainer decision 2026-11-11,
# "Separate release asset").

_ASSET = "provisa-duckdb-extensions-dev-macOS-arm64.tar.gz"


def test_the_split_moves_every_duckdb_extension_file_into_a_checksummed_asset(tmp_path):
    import hashlib
    import tarfile

    macos = _payload(tmp_path, extensions=True)
    runtime = macos / "native-payload/runtime"
    result = _build_dmg(macos, _fake_tools(tmp_path), "split_duckdb_extensions")
    assert result.returncode == 0, result.stderr
    # The runtime the DMG carries holds no DuckDB extension file, and keeps everything else.
    assert not [p for p in runtime.rglob("*") if ".duckdb_extension" in p.name]
    assert (runtime / "lib/site-packages/zstandard/backend_c.cpython-312-darwin.so").is_file()
    # The asset holds each at its path under the runtime, with its content.
    asset = macos / "dist" / _ASSET
    with tarfile.open(asset) as tar:
        members = {m.name.removeprefix("./"): tar.extractfile(m).read() for m in tar if m.isfile()}
    assert members == _EXTENSIONS
    # Its sha256 is published beside it and pinned in the .app, which is signed after this.
    digest = hashlib.sha256(asset.read_bytes()).hexdigest()
    assert (macos / "dist" / f"{_ASSET}.sha256").read_text() == f"{digest}  {_ASSET}\n"
    pin = macos / "Provisa.app/Contents/Resources/duckdb-extensions.sha256"
    assert pin.read_text() == f"{digest}  {_ASSET}\n"


def test_the_split_fails_when_the_runtime_holds_no_duckdb_extension(tmp_path):
    macos = _payload(tmp_path)
    result = _build_dmg(macos, _fake_tools(tmp_path), "split_duckdb_extensions")
    assert result.returncode == 1
    assert "No DuckDB extension in the native runtime" in result.stderr


def test_the_core_dmg_is_not_made_from_a_runtime_holding_a_duckdb_extension(tmp_path):
    macos = _payload(tmp_path, extensions=True)
    (macos / "native-payload/runtime/bin/python3.12").chmod(0o755)
    result = _build_dmg(macos, _fake_tools(tmp_path), "create_dmg")
    assert result.returncode == 1
    assert "is a DuckDB extension: it ships in" in result.stderr
    assert not (macos / "dist/Provisa.dmg").exists()


def test_the_core_build_splits_the_extensions_out_before_anything_is_signed():
    main = (_REPO / "packaging/macos/build-dmg.sh").read_text().split("main() {")[1]
    order = [
        "bundle_native_payload",
        "split_duckdb_extensions",
        "sign_app",
        "sign_native_payload",
        "stage_app",
    ]
    assert [main.index(step) for step in order] == sorted(main.index(step) for step in order)
