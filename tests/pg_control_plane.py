# Copyright (c) 2026 Kenneth Stott
# Canary: b6a39e99-7b82-4b5c-b068-5d7138a1960b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Control planes for tests (REQ-2014): the control plane is PostgreSQL only, so a test that needs
one gets a database of its own on the session's embedded PostgreSQL (tests/embedded_pg.py).

Each worker builds one template database once -- the platform registry (``init_registry_schema``)
and org ``default``'s schema (``init_schema`` over schema.sql), as a deployment lays them out --
and every test gets a fresh database cloned from it (``CREATE DATABASE ... TEMPLATE``), dropped
when the test ends. Cloning copies files; no DDL runs per test.
"""

from __future__ import annotations

import asyncio
import itertools
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

    from provisa.core.database import Database
    from tests.embedded_pg import LocalPostgres


@dataclass(frozen=True)
class SessionPostgres:
    """The PostgreSQL the session exported for its stack (PG_HOST, PG_PORT, PG_USER, PG_PASSWORD:
    the docker stack's, or the local platform's): what an integration or steps test clones its
    control plane on. The same face as tests/embedded_pg.py's ``LocalPostgres``."""

    host: str
    port: int
    user: str
    password: str

    @classmethod
    def from_env(cls) -> SessionPostgres:
        return cls(
            os.environ["PG_HOST"],
            int(os.environ["PG_PORT"]),
            os.environ["PG_USER"],
            os.environ["PG_PASSWORD"],
        )

    def url(self, database: str, *, driver: str = "psycopg") -> str:
        return (
            f"postgresql+{driver}://{self.user}:{self.password}@{self.host}:{self.port}/{database}"
        )

    def dsn(self, database: str = "postgres") -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{database}"


SCHEMA_SQL = (
    Path(__file__).resolve().parent.parent / "provisa" / "core" / "schema.sql"
).read_text()
TEMPLATE = f"cp_template_{os.getpid()}"  # one per process: a server may be shared
ORG = "default"

_serial = itertools.count()


def _admin(pg: LocalPostgres | SessionPostgres, *statements: str) -> None:
    """Run database-level statements (no transaction: CREATE/DROP DATABASE refuse one)."""
    import psycopg

    with psycopg.connect(pg.dsn(), autocommit=True) as conn:
        for statement in statements:
            conn.execute(statement)  # type: ignore[arg-type]


def build_template(pg: LocalPostgres | SessionPostgres) -> str:
    """The worker's template database, laid out as a deployment lays out its control plane."""
    from provisa.core.database import Database, create_engine_from_url
    from provisa.core.db import init_schema
    from provisa.core.schema_admin import init_registry_schema

    _admin(
        pg, f'DROP DATABASE IF EXISTS "{TEMPLATE}" WITH (FORCE)', f'CREATE DATABASE "{TEMPLATE}"'
    )
    engine = create_engine_from_url(pg.url(TEMPLATE))
    try:
        asyncio.run(init_registry_schema(Database(engine, name="admin"), ORG))
        asyncio.run(init_schema(Database(engine, name="layout"), SCHEMA_SQL, org_id=ORG))
    finally:
        engine.dispose()  # a template with an open connection cannot be cloned
    return TEMPLATE


@dataclass
class ControlPlane:
    """A test's own control plane: a database cloned from the template.

    ``platform`` is the platform registry's handle; ``org()`` an org's handle (search_path
    ``org_<id>``), its tables the template's for org ``default``."""

    pg: LocalPostgres | SessionPostgres
    name: str
    _engines: list[Engine] = field(default_factory=list)

    @property
    def url(self) -> str:
        return self.pg.url(self.name)

    def engine(self, url: str | None = None, **kwargs: Any) -> Engine:
        from provisa.core.database import create_engine_from_url

        engine = create_engine_from_url(url or self.url, **kwargs)
        self._engines.append(engine)
        return engine

    @property
    def platform(self) -> Database:
        from provisa.core.database import Database

        return Database(self.engine(), name="admin")

    def org(self, org_id: str = ORG, *, holds: str | None = None) -> Database:
        from provisa.core.database import Database
        from provisa.core.environments import org_schema

        schema = org_schema(org_id)
        # Every connection starts in the org's schema -- a raw one off ``db.engine`` too, not only
        # those ``acquire`` scopes -- so a test writing through the engine reaches the org's tables.
        return Database(
            self.engine(f"{self.url}?options=-csearch_path%3D{schema}"),
            name=f"org-{holds or 'model'}",
            search_path=schema,
            holds=holds,
        )

    def close(self) -> None:
        for engine in self._engines:
            engine.dispose()
        _admin(self.pg, f'DROP DATABASE IF EXISTS "{self.name}" WITH (FORCE)')


def clone(pg: LocalPostgres | SessionPostgres, template: str) -> ControlPlane:
    name = f"cp_{os.getpid()}_{next(_serial)}"
    _admin(pg, f'CREATE DATABASE "{name}" TEMPLATE "{template}"')
    return ControlPlane(pg, name)


_built: dict[tuple[str, int], str] = {}


def fresh_control_plane() -> ControlPlane:
    """A control plane cloned on the session's PostgreSQL, for a helper that is not a fixture (a
    server a test boots, a world a steps file builds): the process's template is built on the
    first call. The caller closes it (``ControlPlane.close``)."""
    server = SessionPostgres.from_env()
    key = (server.host, server.port)
    if key not in _built:
        _built[key] = build_template(server)
    return clone(server, _built[key])
