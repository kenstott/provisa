# Copyright (c) 2026 Kenneth Stott
# Canary: 4f7c1d90-8b2e-4a63-95d1-6e0837bca42f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An environment's materialized replicas live somewhere the environment owns (REQ-1622).

THE HOLE THIS CLOSES. A landed replica is addressed by store DSN, schema, and a table named
``{source_id}__{schema}__{table}`` (``backend.py``). The DSN is per-ORG, the schema was the literal
``mat`` / ``main``, and the table name is derived from the source id -- which every environment
shares, because a source's identity is the same identity in every environment. Nothing in that
address named the environment, so two environments of one org landing the same source wrote the same
rows, and an environment that was going to be deleted wrote into the replica prod reads.

THE ADDRESS CARRIES THE ENVIRONMENT, IN ONE OF TWO PLACES. An author who wants a whole separate
store per environment writes ``${scope:ENV}`` into the store DSN itself (REQ-1622's templating), and
the resolved DSN then names the environment -- there is nothing left to qualify, so the schema stays
as it was. An author who does not is given the qualification instead: the schema becomes
``mat_env_<name>``, one namespace per environment inside the one store.

A SCHEMA-LESS STORE HAS NO SECOND OPTION. SQLite has exactly one namespace, so a non-prod
environment on a SQLite store that was not templated has no way to be addressed apart from prod's
and is refused. The refusal names the template, which is the fix -- it is not a case that can be
made to work by choosing a different schema, and landing anyway would write prod's file.

WHY EVERY NON-PROD ENVIRONMENT AND NOT ONLY THE EPHEMERAL ONES. Two long-lived environments
clobbering each other's replicas is the same defect with a slower clock. Ephemerality is what makes
the cleanup mandatory (REQ-1620's rule: nothing an expiring environment writes may outlive it), not
what makes the separation correct.
"""

# Requirements: REQ-1622, REQ-1620, REQ-1487

from __future__ import annotations

import logging
from urllib.parse import urlparse

from provisa.core.environments import PROD, is_env_name

log = logging.getLogger(__name__)

#: The prefix an environment's own store schema is built from. ``mat`` alone stays prod's, so a
#: schema this module created is recognizable by name -- which is what lets the retire path drop one
#: without the risk of dropping the shared one.
ENV_SCHEMA_PREFIX = "mat_env_"


class StoreNotIsolable(Exception):
    """A non-prod environment's replicas had nowhere of their own to land."""

    def __init__(self, env: str, dsn_scheme: str) -> None:
        self.env = env
        super().__init__(
            f"environment {env!r} materializes into a {dsn_scheme!r} store, which has a single "
            f"namespace, and the store's own address does not name the environment -- so its "
            f"replicas would land on top of {PROD}'s. Give the store URL a per-environment address "
            f"with the ${{scope:ENV}} template (REQ-1622), e.g. "
            f"materialize_store_url: sqlite:///.../store_${{scope:ENV}}.db"
        )


def _scheme(dsn: str) -> str:
    return urlparse(dsn).scheme.split("+", 1)[0]


def _schema_capable(dsn: str) -> bool:
    """Whether the store at ``dsn`` has namespaces to put an environment in. SQLite does not."""
    return _scheme(dsn) != "sqlite"


def dsn_names_env(dsn: str, env: str) -> bool:
    """Whether ``dsn`` was templated with ``${scope:ENV}`` and so already addresses ``env`` alone.

    Substring, because that is exactly what the template produced: the author wrote the environment
    into the address and this reads it back out. ``prod`` is not asked -- the prod store is the one
    every untemplated address resolves to, so a DSN that happens to contain the word is not evidence
    of anything.
    """
    return env != PROD and env in dsn


def store_schema(dsn: str, env: str) -> str:
    """The schema ``env``'s landed replicas live in within the store at ``dsn``.

    ``prod`` keeps the store's own default -- ``mat`` where there are schemas, ``main`` where there
    are not -- because prod's replicas are where they have always been and moving them would orphan
    every one already landed. A non-prod environment gets its own namespace, unless the DSN already
    gave it one.
    """
    default = "mat" if _schema_capable(dsn) else "main"
    if env == PROD or dsn_names_env(dsn, env):
        return default
    if not _schema_capable(dsn):
        raise StoreNotIsolable(env, _scheme(dsn))
    if not is_env_name(env):
        raise ValueError(f"{env!r} is not an environment name")
    return f"{ENV_SCHEMA_PREFIX}{env}"


async def drop_env_store(dsn: str, env: str) -> str | None:
    """Remove what ``env`` owned in the store at ``dsn``. Returns the schema dropped, or None.

    Called from the one retire door. Refuses to act unless the schema it computed is one this module
    named (``mat_env_*``): a DSN that already addressed the environment is the AUTHOR's store, whose
    lifetime is the author's to state, and ``prod``'s ``mat`` is never a thing an environment may
    drop. So the only schema this deletes is one that exists because an environment existed.
    """
    schema = store_schema(dsn, env)
    if not schema.startswith(ENV_SCHEMA_PREFIX):
        return None
    from sqlalchemy import text

    from provisa.federation.store_writer import store_connection

    async with store_connection(dsn) as conn:
        await conn.execute_core(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
    log.info("Environment %r dropped its materialization schema %s", env, schema)
    return schema
