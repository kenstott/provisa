# Copyright (c) 2026 Kenneth Stott
# Canary: 1cbbf391-7348-4ad8-869a-6d3a78c79fa9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Demo-mode signal (REQ-1220). PROVISA_DEMO is exported by every demo launch path:
``provisa run --demo``, scripts/provisa, packaging first-launch, start-ui*.sh."""

from __future__ import annotations

import os


def is_demo() -> bool:
    """True when the deployment was launched in demo mode (PROVISA_DEMO=1/true/yes)."""
    return os.environ.get("PROVISA_DEMO", "").lower() in ("1", "true", "yes")
