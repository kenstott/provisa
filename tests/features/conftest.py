# Copyright (c) 2026 Kenneth Stott
# Canary: f1b2c3d4-e5f6-7a8b-9c0d-e1f2a3b4c5d6
#
# This source code is licensed under the Business Source License 1.1
"""pytest-bdd configuration for generated feature files.

Scenarios and their step definitions are collected directly from
``tests/steps/steps_*.py`` (see ``python_files`` / ``testpaths`` in pyproject.toml);
each step module calls ``scenarios(...)`` for its own feature. This directory holds
only ``.feature`` files, which pytest does not collect on their own, so no scenario
registration belongs here.

Registering scenarios at this conftest's import time was both redundant — the same
step modules are already collected from ``tests/steps`` — and unsafe: when
``tests/features/`` is passed as a direct CLI argument this conftest loads before
pytest-bdd's ``pytest_configure`` populates ``CONFIG_STACK``, so any module-level
``scenarios()`` call raised ``IndexError``.
"""

import sys
from pathlib import Path

# Make step definitions importable when this directory is targeted directly.
sys.path.insert(0, str(Path(__file__).parent.parent))
