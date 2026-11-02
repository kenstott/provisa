# Copyright (c) 2026 Kenneth Stott
# Canary: 98e8363d-f9f6-41b7-8776-651e9abc21ce
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What each mode opens for the planes at launch (REQ-1916)."""

# Requirements: REQ-1916

from __future__ import annotations

import os
from typing import Any

from provisa.core import process_mode


def start_plane() -> Any:
    """A control node's plan listener, started on its ``--plane-port``; a data node's check that
    it holds the deployment key and its control node answers (``LaunchRefused`` otherwise). None
    for a node in ``every`` mode, which plans for itself, and for a data node."""
    if process_mode.mode() == process_mode.CONTROL:
        from provisa.planes import signing
        from provisa.planes.control_server import start
        from provisa.planes.facade import PLANE_PORT_ENV
        from provisa.core.process_region import LaunchRefused

        try:
            signing._key()  # noqa: SLF001 - derived now, so a node without the key does not start
        except RuntimeError as exc:
            raise LaunchRefused(f"a control node signs plans: {exc}") from exc
        return start(int(os.environ[PLANE_PORT_ENV]))
    if process_mode.plans_remotely():
        from provisa.planes.facade import require_control

        require_control()
    return None
