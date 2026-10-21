# Copyright (c) 2026 Kenneth Stott
# Canary: 5293f267-614a-4983-b260-b70c7f34824c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2006: the hosted-sandbox page names each limit with its value, and the values are the
settings' own shipped defaults -- the page cannot drift from the settings that enforce them."""

# Requirements: REQ-2006

from __future__ import annotations

import re
from pathlib import Path

from provisa.core.settings_catalog_sandbox import DECLARED

_PAGE = Path(__file__).resolve().parents[2] / "docs" / "hosted-sandbox.md"


def _defaults() -> dict[str, int]:
    return {s.key: int(s.default) for s in DECLARED if s.card == "limits"}


def _row(setting: str) -> str:
    for line in _PAGE.read_text().splitlines():
        if line.startswith("|") and f"`{setting}`" in line:
            return line
    raise AssertionError(f"the page names no row for {setting}")


def test_the_memory_and_disk_limits_are_the_settings_defaults():
    defaults = _defaults()
    for key in ("sandbox.memory_limit", "sandbox.disk_limit"):
        megabytes = int(re.search(r"(\d+) MB", _row(key)).group(1))  # type: ignore[union-attr]
        assert megabytes * 1024 * 1024 == defaults[key], key


def test_the_inactivity_limit_is_the_settings_default():
    hours = int(re.search(r"after (\d+) hours?", _row("sandbox.idle_ttl")).group(1))  # type: ignore[union-attr]
    assert hours * 3600 == _defaults()["sandbox.idle_ttl"]


def test_the_cap_is_the_settings_default():
    count = int(re.search(r"\| (\d+) sandboxes", _row("sandbox.max_concurrent")).group(1))  # type: ignore[union-attr]
    assert count == _defaults()["sandbox.max_concurrent"]


def test_the_demo_databases_linger_is_the_settings_default():
    minutes = int(re.search(r"for (\d+) minutes", _row("sandbox.demo_stores_linger")).group(1))  # type: ignore[union-attr]
    assert minutes * 60 == _defaults()["sandbox.demo_stores_linger"]


def test_every_sandbox_setting_is_on_the_page():
    for setting in DECLARED:
        _row(setting.key)
