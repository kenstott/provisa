# Copyright (c) 2026 Kenneth Stott
# Canary: 9f5e7a46-c18b-4d5a-8a10-7f1b6e8c25af
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The post-trial license nag — message + per-connection rate limit (REQ-1137).

After the trial expires with no valid license, a self-contained nag is surfaced on every access
surface through each protocol's NON-FATAL out-of-band notice channel only (pgwire NoticeResponse,
Bolt SUCCESS-metadata, Flight app_metadata, gRPC trailing metadata, REST header + warnings[], MCP
notifications/message). It NEVER modifies the result body, row stream, or any schema-typed field, and
NEVER gates or degrades functionality — it is purely informational. Emission is rate-limited to once
per session/connection.

The message states the license is FREE and used only to gather usage information (Provisa collects no
telemetry), that the trial has elapsed, the fields to send, the registration link + request email,
the machine id, and how to apply the received file.
"""

from __future__ import annotations

_REGISTRATION_URL = "https://provisa.dev/register"
_REQUEST_EMAIL = "license@provisa.dev"


def nag_message(machine_id: str) -> str:
    """The full self-contained nag text (REQ-1137). Identical across every surface."""
    return (
        "Your Provisa trial period has elapsed. A license is FREE — it exists only so we can gather "
        "basic usage information, because Provisa collects NO telemetry and never phones home. "
        "Functionality is NOT affected or degraded.\n"
        f"To get a license, register at {_REGISTRATION_URL} or email {_REQUEST_EMAIL} with: "
        "company, position/title, role, first name, last name, email, phone (optional).\n"
        f"Include your machine ID: {machine_id}\n"
        "Then apply the license with `provisa license apply <file>`, upload it in Settings → License, "
        "or place it at ~/.provisa/license.json."
    )


class NagRateLimiter:  # REQ-1137
    """Once-per-connection nag gate. A surface calls ``should_emit(conn_id)`` before emitting; it
    returns True at most once per distinct connection/session id, so a client is nagged once, not on
    every message."""

    def __init__(self) -> None:
        self._seen: set[str] = set()

    def should_emit(self, connection_id: str) -> bool:
        if connection_id in self._seen:
            return False
        self._seen.add(connection_id)
        return True
