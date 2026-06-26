# Copyright (c) 2026 Kenneth Stott
# Canary: f1b2c3d4-e5f6-7a8b-9c0d-e1f2a3b4c5d6
#
# This source code is licensed under the Business Source License 1.1
"""pytest-bdd configuration for generated feature files.

All .feature files in this directory are auto-collected. Step definitions live in
tests/steps/. Unimplemented steps call pytest.skip — CI sees them as skipped rather
than failing, giving visibility without blocking.
"""

import sys
from pathlib import Path

# Make step definitions importable
sys.path.insert(0, str(Path(__file__).parent.parent))

from pytest_bdd import scenarios  # type: ignore[import]

# Import stub step definitions so pytest-bdd can register them
import tests.steps.generated_stubs  # type: ignore[import]  # noqa: F401

# Collect every .feature file in this directory
scenarios(".")
