# Copyright (c) 2026 Kenneth Stott
# Canary: ada49807-92cc-4439-a7ed-d20524b5cae1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The platform fake key (REQ-1494): the guarded secret setting ``fakes.key``, created at the
platform's first start, the same for every node and region.

Created from the deployment's key when the deployment provides one (``PROVISA_FAKE_KEY``, hex --
the Helm chart's Secret, which it also mounts into the engine), else at random. Once stored, a
deployment key that differs refuses the start: the engine would compute every fake under another
key.

Each process holds the key for its embedded engine (``provisa.fakes.digest.set_key``). For an
engine running apart, the process writes it to ``<PROVISA_FAKE_KEY_DIR>/<fingerprint>.key`` when
that directory is named -- the directory the deployment mounts into the engine -- so platforms
sharing one engine each keep their own key file.
"""

# Requirements: REQ-1494

from __future__ import annotations

import os
import secrets
from pathlib import Path
from typing import TYPE_CHECKING

from provisa.fakes.digest import fingerprint

if TYPE_CHECKING:
    from provisa.core.database import Database

SETTING = "fakes.key"
DEPLOYMENT_KEY_ENV = "PROVISA_FAKE_KEY"
KEY_DIR_ENV = "PROVISA_FAKE_KEY_DIR"

_KEY_BYTES = 32


class FakeKeyMismatch(RuntimeError):
    """The deployment's fake key is not the platform's."""


def ensure(db: "Database") -> bytes:
    """The platform fake key, created now when the platform has none; written to the engine's key
    directory when one is named."""
    from provisa.core import deployment_settings, settings_registry

    deployed = os.environ.get(DEPLOYMENT_KEY_ENV, "").strip().lower() or None
    held = settings_registry.stored_value(SETTING)
    if held is None:
        # REQ-1494 (G1): the platform creates its key at its first start -- the deployment's when
        # it provides one, else a new random key.
        created = deployed if deployed is not None else secrets.token_hex(_KEY_BYTES)
        sealed = settings_registry._to_store(  # noqa: SLF001 -- the store's own sealing
            settings_registry.setting(SETTING), created
        )
        deployment_settings.create(db, SETTING, sealed, updated_by="platform")
        held = settings_registry.stored_value(SETTING)
    assert held is not None  # stored above, by this process or another
    key = bytes.fromhex(held)
    if deployed is not None and deployed != held:
        raise FakeKeyMismatch(
            f"the deployment's fake key ({fingerprint(bytes.fromhex(deployed))}) is not the "
            f"platform's ({fingerprint(key)}): the engine would compute every fake under another key"
        )
    directory = os.environ.get(KEY_DIR_ENV)
    if directory:
        _export(Path(directory) / f"{fingerprint(key)}.key", held)
    return key


def _export(path: Path, hex_key: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp.write_text(hex_key)
    # Readable by the engine, which runs as its own user; the directory is the deployment's to
    # keep from anyone else (a mounted secret in a cluster, a private volume elsewhere).
    tmp.chmod(0o644)
    tmp.replace(path)
