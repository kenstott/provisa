# Copyright (c) 2026 Kenneth Stott
# Canary: 883c0718-fafd-4982-946d-e713e8763837
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What the Sources form needs to know of a source kind before anything is saved.

Each fact is a rule the server owns; the form asks rather than keep its own list of kinds:

- ``cache_ttl_required``: a source of the kind must state its Cache TTL itself (REQ-1907,
  ``_landing_ttl.source_states_the_clock``);
- ``sentinel_path``: a source of the kind can have a sentinel marker (REQ-1148,
  ``events.sentinel_probe.applies_to``);
- ``tables_land``: every table of a source of the kind is served from a replica under the
  engine bound here, so a table registered under a ttl signal needs a Cache TTL, its own or
  its source's (REQ-1907, ``_landing_ttl.kind_lands``);
- ``live_only``: the kind is live-only -- its tables are read live, never replicated, so no
  replication setting is offered (REQ-2000, ``core.source_registry.is_live_only``).

And, of every kind at once, the reach the form's type list is gated and annotated by (REQ-947):
the engine serving the bound org and what each engine reads live or from a replica -- none of
the engine settings, which are the platform's (``/admin/federation-engine``).
"""

# Requirements: REQ-1907, REQ-1148, REQ-2000
from __future__ import annotations

from fastapi import APIRouter, Request

from provisa.api.admin._landing_ttl import kind_lands, source_states_the_clock
from provisa.api.admin.capabilities import require_capability_request
from provisa.core.source_registry import is_live_only
from provisa.events import sentinel_probe

router = APIRouter(prefix="/admin/source-kinds", tags=["admin"])


@router.get("")
async def source_reach(request: Request) -> dict:
    """The engine serving the bound org, and each engine's reach faces (REQ-947): every kind it
    reads in place (``live_source_types``) and every kind it reaches at all
    (``reachable_source_types``)."""
    require_capability_request(request, "source_registration")
    from provisa.api.app import state
    from provisa.federation.engine import engine_registry

    return {
        "engine": state.federation_engine.selected_key,
        "engines": [
            {
                "key": e["key"],
                "label": e["label"],
                "live_source_types": e["live_source_types"],
                "reachable_source_types": e["reachable_source_types"],
            }
            for e in engine_registry()
        ],
    }


@router.get("/{kind}")
async def source_kind(request: Request, kind: str) -> dict:
    require_capability_request(request, "source_registration")
    return {
        "cache_ttl_required": source_states_the_clock(kind),
        "sentinel_path": sentinel_probe.applies_to(kind),
        "tables_land": kind_lands(kind),
        "live_only": is_live_only(kind),
    }
