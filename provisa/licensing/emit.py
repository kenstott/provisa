# Copyright (c) 2026 Kenneth Stott
# Canary: d39c1e8a-05cf-4b9e-8e14-1b5f0c2a69e3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""One nag-emission seam every protocol surface shares (REQ-1137).

The app evaluates licensing once at startup and installs the resulting :class:`LicensingState` here
via :func:`set_state`. Each surface then calls :func:`nag_for_connection(connection_id)` at the point
it can attach an out-of-band, non-fatal notice (pgwire NoticeResponse, Bolt SUCCESS metadata, Flight
app_metadata, gRPC trailing metadata, REST header + warnings[], MCP notifications/message). The helper
returns the nag text at most once per connection/session (shared rate limiter) when the trial has
expired and no valid license is present, else None. It never touches the result body or gates
anything — a None means "emit nothing".
"""

from __future__ import annotations

from provisa.licensing.nag import NagRateLimiter
from provisa.licensing.state import LicensingState

_state: LicensingState | None = None
_rate_limiter = NagRateLimiter()


def set_state(state: LicensingState | None) -> None:
    """Install the evaluated licensing state (called once at startup) and reset the rate limiter."""
    global _state, _rate_limiter
    _state = state
    _rate_limiter = NagRateLimiter()


def current_state() -> LicensingState | None:
    return _state


def should_nag() -> bool:
    """Whether the post-trial nag is currently active (trial expired, no valid license)."""
    return _state is not None and _state.should_nag


def nag_for_connection(connection_id: str) -> str | None:
    """The nag text for ``connection_id`` the first time only, or None (REQ-1137).

    Returns None when not nagging or when this connection was already nagged — so a surface emits the
    notice at most once per connection/session and never on every message."""
    if not should_nag() or _state is None:
        return None
    if not _rate_limiter.should_emit(connection_id):
        return None
    return _state.nag_text
