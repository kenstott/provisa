# Copyright (c) 2026 Kenneth Stott
# Canary: b17a9c68-e3ad-4f7c-8c12-9b3d8a0e47c1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Offline, airgapped trial + license subsystem (REQ-1135–1139).

Ties together first-use anchors (REQ-1135), a rollback-resistant trial clock (REQ-1136), the
post-trial nag (REQ-1137), and offline Ed25519 license verification/application (REQ-1138/1139).
Provisa never phones home: every check is local and time-based.
"""

from __future__ import annotations

from provisa.licensing.license import License, apply_license, load_license, verify_license
from provisa.licensing.machine_id import stable_machine_id
from provisa.licensing.nag import NagRateLimiter, nag_message
from provisa.licensing.state import LicensingState, evaluate

__all__ = [
    "License",
    "LicensingState",
    "NagRateLimiter",
    "apply_license",
    "evaluate",
    "load_license",
    "nag_message",
    "stable_machine_id",
    "verify_license",
]
