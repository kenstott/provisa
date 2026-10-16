# Copyright (c) 2026 Kenneth Stott
# Canary: 20667f3c-7d46-446c-9a72-d68b9a8d0c5b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Which process serves a sandbox org (REQ-2006).

A demo sandbox is an org of its own, on an in-process DuckDB engine pinned to a file in the org's
folder (REQ-1992). Exactly one process may ever open that file, and every request for the sandbox
must reach that process: a DuckDB file is locked by the process that opens it, and the deployment's
API runs several uvicorn workers that share one listening socket, so the kernel, not Provisa, picks
which worker answers. The sandboxes therefore have a tier of their own: one single-worker process
(``PROVISA_SANDBOX_TIER=true``), reached through the UI server by the org's host (ui_server.py).

The tier is named by the org id. A sandbox's id begins ``sbx`` and a demo's template org's begins
``sbxtpl``; an ordinary org may not take either prefix (orgs_router._validate_new_org_id). Each
process builds runtimes for its own tier's orgs only (app._build_org_runtime), so a request that
reaches the other tier is refused by name rather than opening the file a second time.
"""

# Requirements: REQ-2006, REQ-1992, REQ-1309

from __future__ import annotations

import secrets

from provisa.api.errors import ApiError
from provisa.core.org_ids import (
    SANDBOX_PREFIX,
    TEMPLATE_PREFIX,
    is_demo_sandbox,
    is_sandbox_tier_org,
    is_template_org,
)

__all__ = [
    "SANDBOX_PREFIX",
    "TEMPLATE_PREFIX",
    "is_demo_sandbox",
    "is_sandbox_tier_org",
    "is_template_org",
    "new_sandbox_org_id",
    "require_this_tier",
    "serves_sandboxes",
    "template_org_id",
]


def template_org_id(demo: str) -> str:
    """The template org of the named demo ``demo`` (demo/named/<demo>/)."""
    return f"{TEMPLATE_PREFIX}{demo}"


def new_sandbox_org_id() -> str:
    """A fresh sandbox org id: ``sbx`` and 12 random hex digits (``t`` is not one, so a sandbox's
    id never begins as a template's) -- ``sbxr`` and them on the regions sandbox tier (REQ-2068),
    so the UI server routes it there by its id."""
    from provisa.api.demo_region_seed import regional
    from provisa.core.org_ids import REGIONS_SANDBOX_PREFIX

    prefix = REGIONS_SANDBOX_PREFIX if regional() else SANDBOX_PREFIX
    return f"{prefix}{secrets.token_hex(6)}"


def serves_sandboxes() -> bool:
    """Whether this process is the sandbox tier. Set by the deployment (the control plane's
    provisa-sandbox service, packaging/linux/first-launch.sh), never per request."""
    from provisa.core.org_ids import serves_sandbox_tier

    return serves_sandbox_tier()


def require_this_tier(org_id: str) -> None:
    """Refuse to serve ``org_id`` here when it belongs to the other tier.

    421 Misdirected Request: the request was well formed and the org exists, but this process does
    not serve it -- a sandbox opened here would be its DuckDB file opened by a second process."""
    sandbox = is_sandbox_tier_org(org_id)
    if sandbox == serves_sandboxes():
        return
    raise ApiError(
        421,
        "sandbox.wrong_tier",
        (
            f"Org {org_id!r} is a demo sandbox, served by the sandbox tier only"
            if sandbox
            else f"Org {org_id!r} is not a demo sandbox; this process serves demo sandboxes only"
        ),
        org=org_id,
    )
