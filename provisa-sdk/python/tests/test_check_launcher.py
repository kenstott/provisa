# Copyright (c) 2026 Kenneth Stott
# Canary: 7a90e3b5-1c46-4d82-b5f7-0e8d29c61a43
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Python SDK's check launcher runs the matching server's ``code check`` through uvx."""

# Requirements: REQ-1984
from __future__ import annotations

from importlib.metadata import version

import pytest

from provisa_sdk import __main__ as launcher

pytestmark = pytest.mark.unit


def test_check_runs_uvx_provisa_at_the_sdks_own_version():
    assert launcher.command(["check", "proj", "--json"]) == [
        "uvx",
        f"provisa=={version('provisa-sdk')}",
        "code",
        "check",
        "proj",
        "--json",
    ]


@pytest.mark.parametrize("args", [[], ["build"]])
def test_anything_but_check_is_a_usage_error(args):
    with pytest.raises(SystemExit):
        launcher.command(args)
