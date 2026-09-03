# Copyright (c) 2026 Kenneth Stott
# Canary: 8e1f4c2a-9b3d-4e6f-a7c8-5d0e9b1f2a3c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Global kill switch for the desktop dev instance (start-ui-install.sh --demo/--native).

POST /admin/lifecycle/shutdown launches scripts/stop-all-services.sh detached, then returns —
the script survives the backend process it's about to kill because it runs in its own session
(start_new_session=True), same as the process-group teardown start-ui-install.sh already runs
on its own restart. Refused outside a native/desktop launch (PROVISA_REDIS_EMBEDDED=1, set only
by --demo/--native): there is no host-process "backend"/"UI dev server" pair to kill under
Docker or in a cloud deployment, and killing this process there would take down a shared engine
shard rather than a lone desktop instance.
"""

from __future__ import annotations

import logging
import os
import subprocess
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from provisa.api.admin._platform_guard import require_platform_settings
from provisa.api.errors import ApiError

log = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/lifecycle", tags=["admin"])

_REPO_ROOT = Path(__file__).resolve().parents[3]
_STOP_SCRIPT = _REPO_ROOT / "scripts" / "stop-all-services.sh"


def _is_desktop_runtime() -> bool:
    """True only for a --demo/--native launch (desktop_profile.py's own no-Docker signal)."""
    return os.environ.get("PROVISA_REDIS_EMBEDDED", "") == "1"


@router.post("/shutdown")
async def shutdown(request: Request) -> JSONResponse:
    """Kill every Provisa process on this host: uvicorn backend + Vite UI dev server."""
    require_platform_settings(request)

    if not _is_desktop_runtime():
        raise ApiError(
            409,
            "lifecycle.not_desktop_runtime",
            "The global kill switch only runs against a --demo/--native desktop launch.",
        )
    if not _STOP_SCRIPT.exists():
        raise ApiError(500, "lifecycle.stop_script_missing", f"{_STOP_SCRIPT} not found")

    log.warning("Admin-triggered shutdown: launching %s", _STOP_SCRIPT)
    subprocess.Popen(  # noqa: S603 — fixed, non-shell argv; not user input
        [str(_STOP_SCRIPT)],
        start_new_session=True,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return JSONResponse(content={"status": "stopping"})
