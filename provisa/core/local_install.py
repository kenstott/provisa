# Copyright (c) 2026 Kenneth Stott
# Canary: 2e7a9c41-5b3d-4f08-8a16-d0c4e9f7b215
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A local install's public address (REQ-2135, A LOCAL INSTALL SETS THE PUBLIC URL).

A local start (start-ui-install.sh, the desktop app, ``provisa run``) names the address its server
listens on, ``http://<host>:<port>``, in PROVISA_LOCAL_ADDRESS. The boot's once-per-launch work
stores it as server.public_url when that is unset, as the operator setting, so it shows in
Deployment settings and can be changed there. It is an install step, not a read-time fallback: a
hosted or cluster start names no local address, stores nothing, and SPARQL refuses until the
operator sets the field.
"""

# Requirements: REQ-2135

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING

from provisa.core import settings_registry

if TYPE_CHECKING:
    from provisa.core.database import Database

LOCAL_ADDRESS_ENV = "PROVISA_LOCAL_ADDRESS"
_KEY = "server.public_url"

log = logging.getLogger(__name__)


def local_address() -> str | None:
    """The address a local start's server listens on, or None when this is no local start."""
    return os.environ.get(LOCAL_ADDRESS_ENV) or None


def set_public_url(db: "Database") -> None:
    """Store the local start's address as server.public_url, when nothing sets that field."""
    address = local_address()
    if address is None:
        return
    if settings_registry.resolve(_KEY).source != "default":
        return
    settings_registry.store(db, {_KEY: address}, updated_by="local install")
    log.info("local install: %s set to %s, the address the server listens on", _KEY, address)
