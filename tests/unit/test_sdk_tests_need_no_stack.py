# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Python SDK's tests touch no service: a run of them with tests/unit provisions no Docker
stack (a run that did crashed its xdist workers)."""

# Requirements: REQ-1984
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.conftest import _needs_shared_stack

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]


def _item(path: Path):
    return SimpleNamespace(path=path, get_closest_marker=lambda name: None)


def test_sdk_tests_with_unit_tests_need_no_stack():
    items = [
        _item(ROOT / "tests" / "unit" / "test_x.py"),
        _item(ROOT / "provisa-sdk" / "python" / "tests" / "test_typing.py"),
    ]
    assert _needs_shared_stack(items) is False


def test_integration_tests_still_need_the_stack():
    assert _needs_shared_stack([_item(ROOT / "tests" / "integration" / "test_x.py")]) is True
