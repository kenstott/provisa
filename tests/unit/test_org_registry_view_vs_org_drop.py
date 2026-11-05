# Copyright (c) 2026 Kenneth Stott
# Canary: 3f1c9a52-7b0e-4d6a-9e21-8c4d5b7a1f60
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The root org's registry view is rebuilt over every org's schema, and an org's schema is dropped
when the org goes (REQ-1301, REQ-701). On the hosted demo tier a sandbox opening rebuilt the view
while the reaper was removing a closed one: the reaper dropped the closed sandbox's schema after the
rebuild had found it and before its CREATE VIEW, which then failed with UndefinedTable -- and the
open answered 500 ProgrammingError. The rebuild and the drop must not overlap.

Nor may the drop land between the rebuild and what reads the view it made: the drop's CASCADE takes
the view with it, since the view reads the dropped schema. A profiling sandbox opening granted the
view to the client role under a second hold of the lock, after a reap had dropped a closed sandbox's
schema in between, and its GRANT failed with UndefinedTable on org_default.org_registry.

The control plane is modelled here: a set of schemas, a CREATE VIEW that fails on a schema that is
gone, and advisory locks that block across threads as the server's do. Each side runs on a thread
and event loop of its own, as each request does."""

# Requirements: REQ-1301, REQ-701

from __future__ import annotations

import asyncio
import re
import threading
from contextlib import asynccontextmanager, contextmanager

from provisa.core import db as core_db
from provisa.core.org_provisioning import deprovision_org
from provisa.core.org_registry_view import VIEW_COLUMNS, refresh_org_registry_view

_ROOT = "default"
_DOOMED = "sbxclosed"
_REAPER = "reaper"


class UndefinedTable(Exception):
    pass


class _Server:
    def __init__(self, schemas: set[str], orgs: list[str]) -> None:
        self.schemas = set(schemas)
        self.orgs = list(orgs)
        self.locks: dict[int, threading.Lock] = {}
        self.guard = threading.Lock()
        self.events: list[str] = []
        # Set once the rebuild has found the doomed org's schema; the reaper starts then.
        self.found = threading.Event()
        self.dropped = threading.Event()
        # The schemas the registry view reads, or None while there is no view.
        self.view: set[str] | None = None
        # Set once the view is made.
        self.made = threading.Event()

    def lock(self, key: int) -> threading.Lock:
        with self.guard:
            return self.locks.setdefault(key, threading.Lock())


class _Conn:
    def __init__(self, server: _Server) -> None:
        self.server = server

    async def fetch(self, sql: str, *args):
        assert sql == "SELECT id FROM orgs"
        return [(o,) for o in self.server.orgs]

    async def fetchval(self, sql: str, *args):
        if sql == "SELECT current_schema()":
            return "public"
        name = re.fullmatch(r"SELECT to_regclass\('(\w+)\.(\w+)'\)", sql)
        assert name, sql
        schema, table = name.groups()
        if schema == "public":
            return f"{schema}.{table}"
        present = schema in self.server.schemas
        if present and schema == f"org_{_DOOMED}":
            self.server.found.set()
            # The window the reaper hit: give its drop the chance to land before the CREATE VIEW.
            self.server.dropped.wait(timeout=1.0)
        return f"{schema}.{table}" if present else None

    async def execute(self, sql: str, *args):
        if sql.startswith("DROP SCHEMA IF EXISTS "):
            schema = sql.split()[4]
            self.server.schemas.discard(schema)
            # CASCADE: a view reading the dropped schema goes with it.
            if self.server.view is not None and schema in self.server.view:
                self.server.view = None
            if schema == f"org_{_DOOMED}":
                self.server.events.append("dropped")
                self.server.dropped.set()
        elif sql.startswith("CREATE OR REPLACE VIEW"):
            for schema in re.findall(r"FROM (\w+)\.user_role_assignments", sql):
                if schema not in self.server.schemas:
                    raise UndefinedTable(
                        f'relation "{schema}.user_role_assignments" does not exist'
                    )
            self.server.view = set(re.findall(r"FROM (\w+)\.user_role_assignments", sql))
            self.server.events.append("view")
            self.server.made.set()
        elif sql.startswith("DROP VIEW IF EXISTS "):
            self.server.view = None
        elif re.match(r"GRANT SELECT ON \S+\.org_registry ", sql):
            self._the_view()
            self.server.events.append("grant")
        return "OK"

    def _the_view(self) -> None:
        if self.server.view is None:
            raise UndefinedTable('relation "org_default.org_registry" does not exist')

    async def upsert_returning(self, *args, **kwargs):
        return 1

    async def upsert(self, *args, **kwargs):
        return None

    async def reflect_columns(self, table: str, schema: str | None = None):
        self._the_view()
        return [{"column_name": c, "data_type": "text"} for c in VIEW_COLUMNS]

    @asynccontextmanager
    async def transaction(self):
        yield self

    @asynccontextmanager
    async def advisory_lock(self, key: int):
        lock = self.server.lock(key)
        if self.server.made.is_set() and threading.current_thread().name != _REAPER:
            # Taken again after the view is made, with a reaper waiting for it: the reaper gets it
            # first, as the server may hand it.
            self.server.dropped.wait(timeout=2.0)
        lock.acquire()
        try:
            yield self
        finally:
            lock.release()


class _Engine:
    @contextmanager
    def begin(self):
        yield None


class _Database:
    dialect = "postgresql"

    def __init__(self, server: _Server, search_path: str | None) -> None:
        self.server = server
        self.search_path = search_path
        self.engine = _Engine()

    @asynccontextmanager
    async def acquire(self):
        yield _Conn(self.server)


def test_a_reap_cannot_drop_an_orgs_schema_while_the_registry_view_is_rebuilt(monkeypatch):
    monkeypatch.setattr(core_db, "add_missing_columns", lambda *a, **k: None)
    doomed_schemas = {f"org_{_DOOMED}"}
    server = _Server({f"org_{_ROOT}", *doomed_schemas}, [_ROOT, _DOOMED])
    model_db = _Database(server, f"org_{_ROOT}")
    admin_db = _Database(server, None)
    failures: list[BaseException] = []

    def _rebuild() -> None:
        try:
            asyncio.run(refresh_org_registry_view(model_db=model_db, admin_db=admin_db))
        except BaseException as exc:  # noqa: BLE001 -- reported by the assertion below
            failures.append(exc)
            server.found.set()

    def _reap() -> None:
        server.found.wait(timeout=5)
        # The reaper has deleted the org's row already; its schemas go next.
        server.orgs.remove(_DOOMED)
        try:
            asyncio.run(deprovision_org(_Database(server, None), _DOOMED))
        except BaseException as exc:  # noqa: BLE001 -- reported by the assertion below
            failures.append(exc)

    threads = [threading.Thread(target=_rebuild), threading.Thread(target=_reap, name=_REAPER)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    assert not failures, failures
    # The rebuild that found the schema finished with it in place; the drop came after.
    assert server.events == ["view", "dropped"]
    assert f"org_{_DOOMED}" not in server.schemas


def test_a_reap_cannot_take_the_registry_view_away_before_it_is_granted(monkeypatch):
    from provisa.api import startup_seed
    from provisa.api.admin.orgs_router import _refresh_root_registry_view

    monkeypatch.setattr(core_db, "add_missing_columns", lambda *a, **k: None)
    server = _Server({f"org_{_ROOT}", f"org_{_DOOMED}"}, [_ROOT, _DOOMED])
    monkeypatch.setattr(startup_seed.state, "model_db", _Database(server, f"org_{_ROOT}"))
    monkeypatch.setattr(startup_seed.state, "admin_db", _Database(server, None))
    failures: list[BaseException] = []

    def _open() -> None:
        # The open's own call (demo_sandbox._build), which binds the root org on its thread.
        try:
            asyncio.run(_refresh_root_registry_view())
        except BaseException as exc:  # noqa: BLE001 -- reported by the assertion below
            failures.append(exc)
        finally:
            server.made.set()

    def _reap() -> None:
        server.made.wait(timeout=5)
        # The reaper has deleted the org's row already; its schemas go next.
        server.orgs.remove(_DOOMED)
        try:
            asyncio.run(deprovision_org(_Database(server, None), _DOOMED))
        except BaseException as exc:  # noqa: BLE001 -- reported by the assertion below
            failures.append(exc)

    threads = [threading.Thread(target=_open), threading.Thread(target=_reap, name=_REAPER)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    assert not failures, failures
    # The view was granted while it stood; the drop, and the cascade with it, came after.
    assert server.events == ["view", "grant", "dropped"]
