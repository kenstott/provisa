# Copyright (c) 2026 Kenneth Stott
# Canary: 5f0d7b28-3e61-4a94-8c17-b2e6d9a4f351
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A test that stores secrets leaves the vault and its key record as it found them.

The secrets suites run as a deployment with a key of their own, on a control plane every other
suite of the session shares. A secret left behind was written under that key; once the module
puts the environment back, every later server of the session is a worker with no key facing a
vault that holds secrets, and refuses to start (``VaultKeyError``) — correctly. So a module
records what the vault and the deployment's key record hold before it writes, and puts exactly
that back when it is done."""

from __future__ import annotations

from contextlib import contextmanager

from sqlalchemy import tuple_
from sqlalchemy.engine import Engine

from provisa.core.schema_admin import deployment_encryption_key, secrets_store

_KEY = (secrets_store.c.org_id, secrets_store.c.owner_id, secrets_store.c.name)


def vault_snapshot(engine: Engine) -> tuple[set[tuple], list[dict]]:
    """What the vault holds (which secrets, not their values) and the deployment's key record."""
    with engine.connect() as conn:
        held = {tuple(row) for row in conn.execute(secrets_store.select().with_only_columns(*_KEY))}
        record = [dict(row._mapping) for row in conn.execute(deployment_encryption_key.select())]
    return held, record


def restore_vault(engine: Engine, snapshot: tuple[set[tuple], list[dict]]) -> None:
    """Remove every secret stored since ``snapshot`` and put the key record back as it was."""
    held, record = snapshot
    with engine.begin() as conn:
        now = {tuple(row) for row in conn.execute(secrets_store.select().with_only_columns(*_KEY))}
        added = sorted(now - held)
        if added:
            conn.execute(secrets_store.delete().where(tuple_(*_KEY).in_(added)))
        conn.execute(deployment_encryption_key.delete())
        if record:
            conn.execute(deployment_encryption_key.insert(), record)
        left = {tuple(row) for row in conn.execute(secrets_store.select().with_only_columns(*_KEY))}
        assert left <= held, f"secrets left in the vault: {sorted(left - held)}"


@contextmanager
def vault_left_as_found(engine: Engine):
    snapshot = vault_snapshot(engine)
    try:
        yield
    finally:
        restore_vault(engine, snapshot)


def vault_set_aside(engine: Engine) -> tuple[list[dict], list[dict]]:
    """Take every secret and the deployment's key record out of the vault, returning them.

    For a module that runs as a deployment with a key of its own: the boot decrypts the org's
    vault under the key it holds (REQ-1919), and what other modules stored was written under the
    session's key."""
    with engine.begin() as conn:
        secrets = [dict(row._mapping) for row in conn.execute(secrets_store.select())]
        record = [dict(row._mapping) for row in conn.execute(deployment_encryption_key.select())]
        conn.execute(secrets_store.delete())
        conn.execute(deployment_encryption_key.delete())
    return secrets, record


def vault_put_back(engine: Engine, saved: tuple[list[dict], list[dict]]) -> None:
    """Replace the vault and the key record with what :func:`vault_set_aside` took out."""
    secrets, record = saved
    with engine.begin() as conn:
        conn.execute(secrets_store.delete())
        conn.execute(deployment_encryption_key.delete())
        if secrets:
            conn.execute(secrets_store.insert(), secrets)
        if record:
            conn.execute(deployment_encryption_key.insert(), record)
