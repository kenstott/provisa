# Copyright (c) 2026 Kenneth Stott
# Canary: 4a9f2c81-7d36-4e15-9b0b-2a6e3c7f41da
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Stable machine id (REQ-1135, REQ-1139).

Derived from hardware/OS identifiers — NOT the application install directory — so it survives a
reinstall and a fresh working copy: the same machine yields the same id. It is a stable fingerprint,
not a secret; it scopes a license to the machine it was issued for and namespaces the first-use
anchors. Fully offline: no network, no phone-home.
"""

from __future__ import annotations

import hashlib
import platform
import uuid


def stable_machine_id() -> str:
    """A stable, reinstall-surviving machine fingerprint (REQ-1139).

    Combines the OS/arch, the node name, and the primary interface MAC (``uuid.getnode``) into a
    SHA-256 hex digest. These are hardware/OS-level and independent of the install directory, so the
    id is identical before and after a reinstall on the same machine."""
    parts = [
        platform.system(),
        platform.machine(),
        platform.node(),
        f"{uuid.getnode():012x}",  # primary NIC MAC (or a stable per-boot fallback)
    ]
    raw = "|".join(parts).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:32]
