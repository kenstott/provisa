# Copyright (c) 2026 Kenneth Stott
# Canary: 3b7c1d55-58f7-4d0e-9f5b-6e0e8c5a2b17
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""No replace mode, and no worker that removes (REQ-1229, REQ-1919).

A configuration is a one-time seed: an apply adds and updates and removes nothing, so there is
nothing for a replace mode to decide and nothing a primary worker alone may do. Which node seeds
the store is decided by the store (``config_loader.is_seeded``), not by ``PROVISA_ROLE``, and
``PROVISA_CONFIG_REPLACE`` is read by nothing.
"""

# Requirements: REQ-1229, REQ-1919

from __future__ import annotations

import inspect
from pathlib import Path

from provisa.core import config_loader

_REPO = Path(__file__).resolve().parents[2]


def test_no_worker_role_decides_what_a_config_does():
    assert not hasattr(config_loader, "is_primary_worker")
    readers = [
        str(path.relative_to(_REPO))
        for path in (_REPO / "provisa" / "core").rglob("*.py")
        if "PROVISA_ROLE" in path.read_text(encoding="utf-8")
    ]
    assert readers == []


def test_nothing_reads_the_replace_flag_any_more():
    assert not hasattr(config_loader, "config_replace_mode")
    readers = [
        str(path.relative_to(_REPO))
        for path in (_REPO / "provisa").rglob("*.py")
        if "PROVISA_CONFIG_REPLACE" in path.read_text(encoding="utf-8")
    ]
    assert readers == []


def test_an_apply_or_a_seed_takes_no_replace_argument():
    for function in (config_loader.apply_config, config_loader.seed_config):
        assert "replace" not in inspect.signature(function).parameters
    assert not hasattr(config_loader, "load_config")
    assert not hasattr(config_loader, "load_config_from_yaml")
