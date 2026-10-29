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
from provisa.core.org_registry_view import refresh_org_registry_view

_ROOT = "default"
_DOOMED = "sbxclosed"


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
            if schema == f"org_{_DOOMED}":
                self.server.events.append("dropped")
                self.server.dropped.set()
        elif sql.startswith("CREATE OR REPLACE VIEW"):
            for schema in re.findall(r"FROM (\w+)\.user_role_assignments", sql):
                if schema not in self.server.schemas:
                    raise UndefinedTable(
                        f'relation "{schema}.user_role_assignments" does not exist'
                    )
            self.server.events.append("view")
        return "OK"

    @asynccontextmanager
    async def advisory_lock(self, key: int):
        lock = self.server.lock(key)
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

    threads = [threading.Thread(target=_rebuild), threading.Thread(target=_reap)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    assert not failures, failures
    # The rebuild that found the schema finished with it in place; the drop came after.
    assert server.events == ["view", "dropped"]
    assert f"org_{_DOOMED}" not in server.schemas
