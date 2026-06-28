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

Also re-exports all pytest-bdd step fixtures from generated_stubs so that
every steps_*.py module can resolve stub steps without importing generated_stubs
directly.  pytest-bdd v8 registers step decorators as fixtures in the *calling
module's* local namespace; copying those fixture objects into this conftest's
namespace makes them available to every test in the directory via normal
pytest fixture discovery.
"""

import pytest

# Import stubs first so the @given/@when/@then decorators run and populate
# generated_stubs.__dict__ with fixture objects.
import tests.steps.generated_stubs as _stubs  # noqa: F401  # type: ignore[import]

# Re-export every pytest fixture defined in generated_stubs into this
# conftest's global namespace.  pytest scans conftest.__dict__ for FixtureDef
# objects, so placing them here makes them available to all tests in the
# tests/steps/ directory tree without requiring each steps_*.py to import
# generated_stubs individually.
import _pytest.fixtures as _pf

_g = globals()
for _name, _obj in vars(_stubs).items():
    if isinstance(_obj, _pf.FixtureFunctionDefinition):
        _g[_name] = _obj


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
