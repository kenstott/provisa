# Copyright (c) 2026 Kenneth Stott
# Canary: 62c70f18-6693-4790-bad8-8de972558fa2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The single-writer invariant and the end of replace mode (REQ-1229, REQ-1919).

Only the primary's config load removes what the file no longer declares; a worker started with
``PROVISA_ROLE=secondary`` only upserts. There is no replace mode any more: what a load removes is
decided by the origin of what is stored, so ``PROVISA_CONFIG_REPLACE`` is read by nothing.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from provisa.core import config_loader
from provisa.core.config_loader import is_primary_worker

_REPO = Path(__file__).resolve().parents[2]


class TestWhichWorkerIsThePrimary:
    def test_a_worker_is_the_primary_by_default(self) -> None:
        assert is_primary_worker({}) is True

    @pytest.mark.parametrize("role", ["primary", "PRIMARY", " worker "])
    def test_any_role_but_secondary_is_the_primary(self, role: str) -> None:
        assert is_primary_worker({"PROVISA_ROLE": role}) is True

    @pytest.mark.parametrize("role", ["secondary", "SECONDARY", " Secondary "])
    def test_a_secondary_is_not(self, role: str) -> None:
        assert is_primary_worker({"PROVISA_ROLE": role}) is False


def test_nothing_reads_the_replace_flag_any_more():
    assert not hasattr(config_loader, "config_replace_mode")
    readers = [
        str(path.relative_to(_REPO))
        for path in (_REPO / "provisa").rglob("*.py")
        if "PROVISA_CONFIG_REPLACE" in path.read_text(encoding="utf-8")
    ]
    assert readers == []


def test_a_load_takes_no_replace_argument():
    import inspect

    for function in (config_loader.load_config, config_loader.load_config_from_yaml):
        assert "replace" not in inspect.signature(function).parameters
