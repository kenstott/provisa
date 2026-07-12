# Copyright (c) 2026 Kenneth Stott
# Canary: a3b4c5d6-e7f8-9a0b-c1d2-e3f4a5b6c7d8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""pytest configuration for BDD step definition files.

Applies the ``bdd`` marker to every pytest-bdd scenario collected from
``steps_*.py`` modules so that ``-m bdd`` selects them.
"""

import pytest


def pytest_collection_modifyitems(items: list) -> None:
    """Mark every item from a steps_*.py module with the ``bdd`` marker."""
    bdd_mark = pytest.mark.bdd
    for item in items:
        module = getattr(item, "module", None)
        if module is None:
            continue
        name = getattr(module, "__name__", "") or ""
        # Match tests.steps.steps_* or just steps_*
        if name.startswith("tests.steps.steps_") or name.startswith("steps_"):
            item.add_marker(bdd_mark, append=False)
