# Copyright (c) 2026 Kenneth Stott
# Canary: 9bc9fc02-feb6-491e-b747-d0a4a8ef31e9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""deploy-cloud.sh refuses a hosted node whose billing settings are missing (REQ-1562, REQ-1564).

The preflight reads the RUNNING container's environment: the eight Stripe price ids, the webhook
secret, and the secret and publishable key STRIPE_MODE selects. A blank value is a missing value.
Names are printed, values never are. first-launch.sh forwards the same settings into the
container, since its compose base forwards nothing it does not name.

The function is run here as the script runs it, with ssh_node replaced by a local shell."""

# Requirements: REQ-1562, REQ-1564

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

_REPO = Path(__file__).resolve().parents[2]
_SCRIPT = _REPO / "scripts" / "deploy-cloud.sh"
_LIB = _REPO / "scripts" / "cloud-node-lib.sh"
_FIRST_LAUNCH = _REPO / "packaging" / "linux" / "first-launch.sh"

_PRICES = {
    "STRIPE_PRICE_STARTER": "price_starter",
    "STRIPE_PRICE_PRO_S": "price_pro_s",
    "STRIPE_PRICE_PRO_M": "price_pro_m",
    "STRIPE_PRICE_PRO_L": "price_pro_l",
    "STRIPE_PRICE_EGRESS_STARTER": "price_egress_starter",
    "STRIPE_PRICE_EGRESS_PRO_S": "price_egress_pro_s",
    "STRIPE_PRICE_EGRESS_PRO_M": "price_egress_pro_m",
    "STRIPE_PRICE_EGRESS_PRO_L": "price_egress_pro_l",
}
_SECRET = "whsec_not_a_real_secret"
_LIVE = {"STRIPE_SECRET_KEY": "sk_live_not_real", "STRIPE_PUBLISHABLE_KEY": "pk_live_not_real"}
_TEST = {
    "STRIPE_TEST_SECRET_KEY": "sk_test_not_real",
    "STRIPE_TEST_PUBLISHABLE_KEY": "pk_test_not_real",
}

# Every variable the commercial plugin reads, except STRIPE_BASE_URL, which points it at a test
# stub and has no production value.
_PLUGIN_SETTINGS = {
    *_PRICES,
    "STRIPE_WEBHOOK_SECRET",
    "STRIPE_MODE",
    *_LIVE,
    *_TEST,
}


def _source() -> str:
    """deploy-cloud.sh with the node library it sources (its shared functions live there)."""
    return _LIB.read_text() + _SCRIPT.read_text()


def _function(name: str) -> str:
    text = _source()
    start = text.index(f"{name}() {{")
    return text[start : text.index("\n}\n", start) + 3]


def _keys_block(name: str) -> str:
    """The `NAME="..."` assignment, which spans continuation lines."""
    text = _source()
    match = re.search(rf'^{name}="[^"]*"$', text, re.MULTILINE)
    assert match, name
    return match.group(0)


def _preflight(tmp_path: Path, env: dict[str, str]) -> subprocess.CompletedProcess:
    dump = tmp_path / "printenv.txt"
    dump.write_text("".join(f"{k}={v}\n" for k, v in {"PATH": "/bin", **env}.items()))
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    # `sudo docker exec <container> printenv` answers the container's environment.
    (bin_dir / "sudo").write_text(f'#!/bin/bash\ncat "{dump}"\n')
    (bin_dir / "sudo").chmod(0o755)
    return subprocess.run(
        [
            "bash",
            "-c",
            "\n".join(
                [
                    "set -euo pipefail",
                    "API_CONTAINER=compose-provisa-1",
                    'ssh_node() { bash -c "$1"; }',
                    _keys_block("COMMERCE_KEYS"),
                    _function("commerce_mode_keys"),
                    _function("preflight_commerce"),
                    "preflight_commerce",
                ]
            ),
        ],
        env={"PATH": f"{bin_dir}:/usr/bin:/bin:/opt/homebrew/bin:/usr/local/bin"},
        capture_output=True,
        text=True,
    )


def _secrets(env: dict[str, str]) -> list[str]:
    return [v for k, v in env.items() if k != "STRIPE_MODE"]


@pytest.mark.parametrize(
    "env",
    [
        {**_PRICES, "STRIPE_WEBHOOK_SECRET": _SECRET, **_LIVE},
        {**_PRICES, "STRIPE_WEBHOOK_SECRET": _SECRET, "STRIPE_MODE": "live", **_LIVE},
        {**_PRICES, "STRIPE_WEBHOOK_SECRET": _SECRET, "STRIPE_MODE": " Test ", **_TEST},
    ],
    ids=["live-by-default", "live", "test"],
)
def test_a_configured_node_passes_and_no_value_is_printed(tmp_path, env):
    result = _preflight(tmp_path, env)
    assert result.returncode == 0, result.stderr
    assert "STRIPE_PRICE_STARTER=<set>" in result.stdout
    assert "STRIPE_WEBHOOK_SECRET=<set>" in result.stdout
    for value in _secrets(env):
        assert value not in result.stdout + result.stderr


@pytest.mark.parametrize("missing", [*_PRICES, "STRIPE_WEBHOOK_SECRET"])
@pytest.mark.parametrize("blank", [False, True], ids=["absent", "blank"])
def test_each_price_and_the_webhook_secret_is_asserted(tmp_path, missing, blank):
    env = {**_PRICES, "STRIPE_WEBHOOK_SECRET": _SECRET, **_LIVE}
    if blank:
        env[missing] = " "
    else:
        del env[missing]
    result = _preflight(tmp_path, env)
    assert result.returncode == 1
    assert f"missing commerce settings: {missing}" in result.stderr


def test_test_mode_names_the_sandbox_keys_even_with_the_live_ones_set(tmp_path):
    # REQ-1564's scenario: STRIPE_MODE is test and STRIPE_TEST_SECRET_KEY is empty.
    env = {
        **_PRICES,
        "STRIPE_WEBHOOK_SECRET": _SECRET,
        "STRIPE_MODE": "test",
        **_LIVE,
        "STRIPE_TEST_SECRET_KEY": "",
    }
    result = _preflight(tmp_path, env)
    assert result.returncode == 1
    assert "STRIPE_TEST_SECRET_KEY(STRIPE_MODE=test)" in result.stderr
    assert "STRIPE_TEST_PUBLISHABLE_KEY(STRIPE_MODE=test)" in result.stderr
    assert "STRIPE_SECRET_KEY(" not in result.stderr


def test_live_mode_names_the_publishable_key_it_cannot_mount_checkout_without(tmp_path):
    env = {
        **_PRICES,
        "STRIPE_WEBHOOK_SECRET": _SECRET,
        **_TEST,
        "STRIPE_SECRET_KEY": "sk_live_not_real",
    }
    result = _preflight(tmp_path, env)
    assert result.returncode == 1
    assert "STRIPE_PUBLISHABLE_KEY(STRIPE_MODE=<unset>)" in result.stderr
    assert "STRIPE_SECRET_KEY(" not in result.stderr


def test_every_commerce_setting_the_deploy_reads_is_a_stripe_one():
    names = (_keys_block("COMMERCE_KEYS") + _keys_block("COMMERCE_CREDENTIAL_KEYS")).split('"')[
        1::2
    ]
    keys = " ".join(names).replace("\\", " ").split()
    assert set(keys) == _PLUGIN_SETTINGS
    for fn in ("push_commerce_env", "preflight_commerce"):
        assert "printenv | grep -E '^STRIPE_'" in _function(fn)


def test_first_launch_forwards_exactly_the_plugins_settings():
    text = _FIRST_LAUNCH.read_text()
    forwarded = set(re.findall(r"^\s+(STRIPE_[A-Z_]+): \"\\\$\{\1:-\}\"$", text, re.MULTILINE))
    assert forwarded == _PLUGIN_SETTINGS


def _target_steps(target: str) -> list[str]:
    """The function calls a deploy target runs, in order, from the script's case arm."""
    text = _source()
    arm = re.search(
        rf"^  {re.escape(target)}\)\n(.*?)\n    ;;$|^  {re.escape(target)}\)\n(.*?) ;;$",
        text,
        re.MULTILINE | re.DOTALL,
    )
    assert arm, target
    body = arm.group(1) or arm.group(2)
    calls = [line for line in body.splitlines() if not line.strip().startswith("#")]
    return [
        s.strip()
        for s in " ".join(calls).replace("{", " ").replace("}", " ").split(";")
        if s.strip()
    ]


@pytest.mark.parametrize("target", ["api", "cfg", "all", "patch"])
def test_the_preflight_reads_the_container_after_the_compose_that_forwards_the_settings(target):
    # The container's environment is what the compose file names. A node set up before a billing
    # setting existed has a compose that does not forward it, so asserting the container before
    # the current compose is pushed refuses every such node however its provisa.env is set.
    steps = _target_steps(target)
    assert "push_cp_compose" in steps, steps
    compose = steps.index("push_cp_compose")
    preflight = steps.index("preflight_commerce")
    assert steps.index("push_commerce_env") < compose < preflight, steps
    for code in ("push_api", "push_plugins", "push_cfg"):
        if code in steps:
            assert preflight < steps.index(code), (code, steps)


def test_a_container_environment_larger_than_a_pipe_buffer_passes(tmp_path):
    # Each key is matched as soon as its line is read. A reader that stops at the first match
    # while a producer is still writing makes the producer's next write fail with a broken pipe,
    # which pipefail turns into "missing". 200 KB of settings after the matched lines is more than
    # any pipe buffer holds, so a check fed through such a pipe fails on every run, not by timing.
    padding = {f"STRIPE_ZZ_PADDING_{i:05d}": "v" for i in range(10_000)}
    env = {**_PRICES, "STRIPE_WEBHOOK_SECRET": _SECRET, **_LIVE, **padding}
    result = _preflight(tmp_path, env)
    assert result.returncode == 0, result.stderr[-2000:]
    assert "Broken pipe" not in result.stderr
    assert "STRIPE_PRICE_STARTER=<set>" in result.stdout


def test_no_check_in_the_deploy_stops_reading_a_pipe_early():
    # `producer | grep -q` exits at the first match, and under pipefail the producer's broken
    # pipe fails the check whenever it was still writing. Every match reads a variable instead.
    lines = _source().splitlines()
    early = [
        f"{n}: {line.strip()}"
        for n, line in enumerate(lines, 1)
        if re.search(r"\|\s*grep\s+-[A-Za-z]*q", line)
    ]
    assert not early, early


# --- push_commerce_env reads the deployment's .env, and refuses by name when it cannot -----------
#
# The 513 release deploy ran from a job worktree, which has no .env. A key the container lacked
# sent push_commerce_env to `sed ... "$REPO/.env" 2>/dev/null | tail -1`; under pipefail the missing
# file failed the assignment and `set -e` ended the deploy with status 1 and no message, before the
# recreate, reset and verify ever ran.


def _push(tmp_path: Path, container: dict[str, str], env_file: Path) -> subprocess.CompletedProcess:
    dump = tmp_path / "printenv.txt"
    dump.write_text("".join(f"{k}={v}\n" for k, v in {"PATH": "/bin", **container}.items()))
    calls = tmp_path / "calls.txt"
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "sudo").write_text(f'#!/bin/bash\ncat "{dump}"\n')
    (bin_dir / "sudo").chmod(0o755)
    return subprocess.run(
        [
            "bash",
            "-c",
            "\n".join(
                [
                    "set -euo pipefail",
                    "API_CONTAINER=compose-provisa-1",
                    f"REPO_ENV={env_file}",
                    'ssh_node() { case "$1" in *printenv*) bash -c "$1" ;;'
                    f' *) echo "ssh $1" >> {calls} ;; esac; }}',
                    f'set_node_env() {{ echo "set $1" >> {calls}; }}',
                    _keys_block("COMMERCE_KEYS"),
                    _keys_block("COMMERCE_CREDENTIAL_KEYS"),
                    _function("repo_env_value"),
                    _function("push_commerce_env"),
                    "push_commerce_env",
                ]
            ),
        ],
        env={"PATH": f"{bin_dir}:/usr/bin:/bin:/opt/homebrew/bin:/usr/local/bin"},
        capture_output=True,
        text=True,
    )


_COMPLETE = {**_PRICES, "STRIPE_WEBHOOK_SECRET": _SECRET, "STRIPE_MODE": "live", **_LIVE, **_TEST}


def test_a_key_the_container_lacks_with_no_env_file_is_refused_by_name(tmp_path):
    container = {k: v for k, v in _COMPLETE.items() if k != "STRIPE_TEST_SECRET_KEY"}
    missing = tmp_path / "worktree" / ".env"
    done = _push(tmp_path, container, missing)
    assert done.returncode != 0
    assert "STRIPE_TEST_SECRET_KEY" in done.stderr, done.stderr
    assert str(missing) in done.stderr, done.stderr


def test_a_key_the_container_lacks_is_written_from_the_env_file(tmp_path):
    container = {k: v for k, v in _COMPLETE.items() if k != "STRIPE_TEST_SECRET_KEY"}
    env_file = tmp_path / ".env"
    env_file.write_text(f"OTHER=1\nSTRIPE_TEST_SECRET_KEY={_TEST['STRIPE_TEST_SECRET_KEY']}\n")
    done = _push(tmp_path, container, env_file)
    assert done.returncode == 0, done.stderr
    calls = (tmp_path / "calls.txt").read_text()
    assert "set STRIPE_TEST_SECRET_KEY" in calls
    assert "systemctl restart provisa" in calls
    assert _TEST["STRIPE_TEST_SECRET_KEY"] not in done.stdout + done.stderr


def test_a_complete_container_needs_no_env_file(tmp_path):
    done = _push(tmp_path, _COMPLETE, tmp_path / "absent" / ".env")
    assert done.returncode == 0, done.stderr
    assert "up to date" in done.stdout


def test_the_env_file_is_the_primary_checkouts_not_the_worktrees():
    # A job worktree has no .env: the deployment's keys live in the primary checkout's, the same
    # checkout CLOUD_TF_DIR reads the deployment's terraform state from.
    lib = _LIB.read_text()
    assert re.search(
        r"^REPO_ENV=\"\$\{REPO_ENV:-\$\(git -C \"\$REPO\" worktree list --porcelain \| "
        r"sed -n '1s/\^worktree //p'\)/\.env\}\"$",
        lib,
        re.M,
    ), "cloud-node-lib.sh does not name the primary checkout's .env"
    assert '"$REPO/.env"' not in lib
