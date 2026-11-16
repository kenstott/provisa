# Copyright (c) 2026 Kenneth Stott
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


def _function(name: str) -> str:
    text = _SCRIPT.read_text()
    start = text.index(f"{name}() {{")
    return text[start : text.index("\n}\n", start) + 3]


def _keys_block(name: str) -> str:
    """The `NAME="..."` assignment, which spans continuation lines."""
    text = _SCRIPT.read_text()
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


def test_no_lemon_squeezy_setting_is_read_or_asserted():
    text = _SCRIPT.read_text().upper()
    assert "LEMONSQUEEZY" not in text


def test_first_launch_forwards_exactly_the_plugins_settings():
    text = _FIRST_LAUNCH.read_text()
    forwarded = set(re.findall(r"^\s+(STRIPE_[A-Z_]+): \"\\\$\{\1:-\}\"$", text, re.MULTILINE))
    assert forwarded == _PLUGIN_SETTINGS
    assert "LEMONSQUEEZY" not in text.upper()
