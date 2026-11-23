# Copyright (c) 2026 Kenneth Stott
# Canary: 500ff5b2-40d2-4338-984e-bc70a8a73e6b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""deploy-cloud.sh installs the release's Python dependency set on every node it pushes code to.

511 reached cloud-dev as code importing packages the node's image did not have (lemminflect,
rdflib, ...), and the coordinator and the sandbox VM crash-looped. scripts/deploy_deps.py exports
the set as the Dockerfile installs it and fingerprints it; push_deps rebuilds a node's image only
when the image's recorded fingerprint differs, saves it over the tarball first-launch reloads on
every boot, recreates the stack on it, and refuses the deploy before the restart when the image
does not satisfy the set or the app does not import."""

from __future__ import annotations

import importlib.util
import re
import subprocess
import sys
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[2]
_SCRIPT = _REPO / "scripts" / "deploy-cloud.sh"
_HELPER = _REPO / "scripts" / "deploy_deps.py"

_spec = importlib.util.spec_from_file_location("deploy_deps", _HELPER)
assert _spec is not None and _spec.loader is not None
deploy_deps = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(deploy_deps)

_FP = "a" * 64
_LOCKED = """\
-e ./provisa-notebook
lemminflect==0.2.3
nvidia-cublas-cu12==12.4.5.8 ; sys_platform == 'linux'
pixeltable-pgserver==0.5.1 ; platform_machine != 'aarch64' or sys_platform != 'linux'
torch==2.15.0 ; sys_platform != 'linux'
torch==2.15.0+cpu ; sys_platform == 'linux'
triton==3.1.0 ; sys_platform == 'linux'
"""


def _function(name: str) -> str:
    text = _SCRIPT.read_text()
    start = text.index(f"\n{name}() {{") + 1
    return text[start : text.index("\n}\n", start) + 3]


def _assignment(name: str) -> str:
    match = re.search(rf"^{name}=.*$", _SCRIPT.read_text(), re.MULTILINE)
    assert match is not None, name
    return match.group(0)


# --- the set and its fingerprint ---------------------------------------------------------------


def test_extras_are_the_dockerfiles_default():
    dockerfile = (_REPO / "Dockerfile").read_text()
    declared = re.search(r"^ARG PROVISA_EXTRAS=(\S+)$", dockerfile, re.MULTILINE)
    assert declared is not None
    assert deploy_deps.dockerfile_extras(dockerfile) == declared.group(1).split(",")


def test_a_dockerfile_without_the_extras_arg_is_refused():
    with pytest.raises(ValueError, match="PROVISA_EXTRAS"):
        deploy_deps.dockerfile_extras("FROM python:3.12\n")


def test_the_fingerprint_follows_pyproject_lock_and_extras():
    base = deploy_deps.fingerprint(b"pyproject", b"lock", ["firebase", "vector"])
    assert re.fullmatch(r"[0-9a-f]{64}", base)
    assert base == deploy_deps.fingerprint(b"pyproject", b"lock", ["firebase", "vector"])
    assert base != deploy_deps.fingerprint(b"pyproject2", b"lock", ["firebase", "vector"])
    assert base != deploy_deps.fingerprint(b"pyproject", b"lock2", ["firebase", "vector"])
    assert base != deploy_deps.fingerprint(b"pyproject", b"lock", ["firebase"])
    # Parts are hashed separately: moving bytes across the boundary is a different set.
    assert deploy_deps.fingerprint(b"ab", b"c", []) != deploy_deps.fingerprint(b"a", b"bc", [])


def test_the_export_splits_as_the_dockerfile_does():
    torch, requirements = deploy_deps.split_locked(_LOCKED)
    assert torch == "torch==2.15.0+cpu"
    assert requirements == [
        "lemminflect==0.2.3",
        "pixeltable-pgserver==0.5.1 ; platform_machine != 'aarch64' or sys_platform != 'linux'",
    ]


def test_an_export_without_the_linux_cpu_torch_is_refused():
    with pytest.raises(ValueError, match="torch"):
        deploy_deps.split_locked("lemminflect==0.2.3\ntorch==2.15.0\n")


def test_stage_writes_the_layer_inputs(tmp_path, monkeypatch):
    calls: list[list[str]] = []

    def fake_run(cmd, **_kwargs):
        calls.append(cmd)
        return subprocess.CompletedProcess(cmd, 0, stdout=_LOCKED, stderr="")

    monkeypatch.setattr(deploy_deps.subprocess, "run", fake_run)
    out = tmp_path / "deps"
    fp = deploy_deps._stage(_REPO, out)

    extras = deploy_deps.dockerfile_extras((_REPO / "Dockerfile").read_text())
    assert fp == deploy_deps.fingerprint(
        (_REPO / "pyproject.toml").read_bytes(), (_REPO / "uv.lock").read_bytes(), extras
    )
    assert calls[0][:2] == ["uv", "export"] and "--frozen" in calls[0]
    assert [calls[0][i + 1] for i, a in enumerate(calls[0]) if a == "--extra"] == extras
    assert (out / "fingerprint").read_text() == fp + "\n"
    assert (out / "torch.txt").read_text() == "torch==2.15.0+cpu\n"
    assert "lemminflect==0.2.3" in (out / "requirements.txt").read_text().splitlines()
    assert (out / "Dockerfile").read_text() == deploy_deps.layer_dockerfile()
    assert (out / "deploy_deps.py").read_text() == _HELPER.read_text()


def test_the_layer_installs_the_set_and_records_its_fingerprint():
    layer = deploy_deps.layer_dockerfile()
    assert layer.startswith("ARG BASE\nFROM ${BASE}\n")
    assert "--index-url https://download.pytorch.org/whl/cpu" in layer
    assert "pip install --no-cache-dir --no-deps -r /tmp/provisa-deps/requirements.txt" in layer
    assert f"cp /tmp/provisa-deps/fingerprint {deploy_deps.RECORD_PATH}" in layer


# --- the per-node plan -------------------------------------------------------------------------


def test_a_node_whose_image_records_the_same_set_is_current():
    assert deploy_deps.plan(_FP, _FP) == "current"


@pytest.mark.parametrize("have", ["", "b" * 64])
def test_a_node_with_no_record_or_another_set_installs(have):
    assert deploy_deps.plan(_FP, have) == "install"


def test_a_malformed_wanted_fingerprint_is_refused():
    with pytest.raises(ValueError, match="fingerprint"):
        deploy_deps.plan("", "")


# --- the check inside the image ----------------------------------------------------------------


def test_unmet_names_missing_and_wrong_versions_and_skips_markers_that_do_not_apply():
    installed = {"lemminflect": "0.2.3", "python-docx": "1.1.0", "rdflib": "7.0.0"}
    missing = deploy_deps.unmet(
        [
            "lemminflect==0.2.3",
            "python_docx==1.1.0",  # canonical name match
            "rdflib==7.1.0",
            "statsmodels==0.14.6",
            "pixeltable-pgserver==0.5.1 ; sys_platform != 'linux'",
        ],
        installed,
        lambda marker: marker != "sys_platform != 'linux'",
    )
    assert missing == [
        "rdflib==7.1.0 (installed: 7.0.0)",
        "statsmodels==0.14.6 (installed: none)",
    ]


def test_an_unpinned_line_is_refused():
    with pytest.raises(ValueError, match="pinned"):
        deploy_deps.unmet(["lemminflect>=0.2"], {}, lambda _marker: True)


def test_check_against_this_interpreter(tmp_path):
    import pytest as installed_pytest

    good = tmp_path / "good.txt"
    good.write_text(f"pytest=={installed_pytest.__version__}\nnope==1.0 ; python_version < '3'\n")
    bad = tmp_path / "bad.txt"
    bad.write_text("provisa-no-such-distribution==1.0\n")
    run = [sys.executable, str(_HELPER), "check"]
    assert subprocess.run([*run, str(good)], capture_output=True, text=True).returncode == 0
    failed = subprocess.run([*run, str(bad)], capture_output=True, text=True)
    assert failed.returncode == 1
    assert "unmet: provisa-no-such-distribution==1.0 (installed: none)" in failed.stderr


# --- the shell wiring --------------------------------------------------------------------------


def test_the_script_and_the_helper_name_the_same_record():
    assert _assignment("DEPS_RECORD") == f"DEPS_RECORD={deploy_deps.RECORD_PATH}"


def test_the_saved_tarball_is_the_one_first_launch_reloads():
    # first-launch.sh load_images: staged="${PROVISA_HOME}/images"; the release names the app's
    # tarball provisa-local.tar.gz (release-prebuilt.yml).
    assert (
        'local staged="${PROVISA_HOME}/images"'
        in (_REPO / "packaging" / "linux" / "first-launch.sh").read_text()
    )
    assert (
        "images/provisa-local.tar.gz"
        in (_REPO / ".github" / "workflows" / "release-prebuilt.yml").read_text()
    )
    assert (
        _assignment("IMAGE_TARBALL") == "IMAGE_TARBALL=/root/.provisa/images/provisa-local.tar.gz"
    )


def _arms() -> dict[str, str]:
    text = _SCRIPT.read_text()
    case = text[text.index('case "$TARGET" in\n  ui)') :]
    return dict(re.findall(r"^  ([a-z-]+)\)\n(?:    #.*\n)*    (.*) ;;$", case, re.MULTILINE))


def test_every_arm_that_pushes_code_installs_the_set_before_its_restart():
    arms = _arms()
    pushing = {name for name, steps in arms.items() if "push_api;" in steps}
    assert pushing == {"api", "all", "patch"}
    for name in pushing:
        steps = [s.strip() for s in re.split(r"[;{}|]+", arms[name])]
        assert steps.index("build_deps") > steps.index("build_api"), name
        assert steps.index("push_api") < steps.index("push_deps") < steps.index("restart"), name
    for name in set(arms) - pushing:
        assert "push_deps" not in arms[name], name


def test_the_sandbox_vm_installs_the_set_with_the_package_before_its_reset():
    body = _function("deploy_sandbox_vm")
    assert '[ -f "$STAGE/api.tgz" ] && { push_api; push_plugins; push_deps; }' in body
    assert body.index("push_deps") < body.index("reset_sandbox_state") < body.index("restart")


def _run_push_deps(
    tmp_path: Path, *, have: str, check_rc: int = 0, import_rc: int = 0
) -> tuple[subprocess.CompletedProcess, str]:
    stage = tmp_path / "stage"
    (stage / "deps").mkdir(parents=True)
    (stage / "deps.tgz").write_text("tgz")
    log = tmp_path / "ssh.log"
    script = "\n".join(
        [
            "set -euo pipefail",
            f"REPO={_REPO}",
            f"STAGE={stage}",
            "NODE=test-node",
            "API_CONTAINER=compose-provisa-1",
            f"DEPS_WANT={_FP}",
            f"LOG={log}",
            f"HAVE='{have}'",
            f"CHECK_RC={check_rc}",
            f"IMPORT_RC={import_rc}",
            _assignment("DEPS_RECORD"),
            _assignment("DEPS_DIR"),
            _assignment("IMAGE_TARBALL"),
            'DEPS_RECREATE=""',
            'scp_node() { echo "scp $1 $2" >> "$LOG"; }',
            "ssh_node() {",
            '  echo "$1" >> "$LOG"',
            '  case "$1" in',
            '    *"docker inspect"*) echo "provisa/provisa:local" ;;',
            '    *"--entrypoint cat"*) [ -n "$HAVE" ] || return 1; echo "$HAVE" ;;',
            '    *"deploy_deps.py check"*) return "$CHECK_RC" ;;',
            '    *"import provisa.api.app"*) return "$IMPORT_RC" ;;',
            "    *) return 0 ;;",
            "  esac",
            "}",
            _function("verify_deps"),
            _function("push_deps"),
            "push_deps",
            'echo "RECREATE=$DEPS_RECREATE"',
        ]
    )
    path = f"{Path(sys.executable).parent}:/usr/bin:/bin"
    result = subprocess.run(
        ["bash", "-c", script], env={"PATH": path}, capture_output=True, text=True
    )
    return result, log.read_text() if log.exists() else ""


def test_a_node_without_the_set_gets_it_built_saved_and_recreated(tmp_path):
    result, log = _run_push_deps(tmp_path, have="")
    assert result.returncode == 0, result.stderr
    assert "scp " + str(tmp_path / "stage" / "deps.tgz") + " /tmp/provisa-deps.tgz" in log
    assert (
        "docker build --quiet --build-arg BASE=provisa/provisa:local -t provisa/provisa:local "
        "/tmp/provisa-deps" in log
    )
    assert (
        "docker save provisa/provisa:local | gzip > /root/.provisa/images/provisa-local.tar.gz.new"
        in log
    )
    assert log.index("docker build") < log.index("deploy_deps.py check")
    assert "import provisa.api.app" in log
    assert "RECREATE=test-node" in result.stdout


def test_a_node_whose_image_holds_the_set_is_only_verified(tmp_path):
    result, log = _run_push_deps(tmp_path, have=_FP)
    assert result.returncode == 0, result.stderr
    assert "docker build" not in log and "docker save" not in log
    assert "deploy_deps.py check /deps/requirements.txt" in log
    assert "import provisa.api.app" in log
    assert "RECREATE=\n" in result.stdout


def test_an_image_missing_a_locked_package_fails_the_deploy(tmp_path):
    result, log = _run_push_deps(tmp_path, have=_FP, check_rc=1)
    assert result.returncode == 1
    assert "does not hold the release's set" in result.stderr
    assert "RECREATE" not in result.stdout


def test_an_app_that_does_not_import_fails_the_deploy(tmp_path):
    result, log = _run_push_deps(tmp_path, have=_FP, import_rc=1)
    assert result.returncode == 1
    assert "provisa.api.app does not import" in result.stderr


@pytest.mark.parametrize(
    ("recreate", "expected"),
    [("test-node", "systemctl restart provisa"), ("", "docker restart compose-provisa-1")],
)
def test_restart_recreates_a_node_whose_image_was_rebuilt(recreate, expected):
    script = "\n".join(
        [
            "set -euo pipefail",
            "NODE=test-node",
            f'DEPS_RECREATE="{recreate}"',
            'node_containers() { echo "compose-provisa-1"; }',
            'ssh_node() { echo "ssh: $1"; }',
            _function("restart"),
            "restart",
            'echo "after=$DEPS_RECREATE"',
        ]
    )
    result = subprocess.run(["bash", "-c", script], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert f"ssh: sudo {expected}" in result.stdout
    assert "after=\n" in result.stdout
