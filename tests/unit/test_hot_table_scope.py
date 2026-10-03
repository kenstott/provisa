# Copyright (c) 2026 Kenneth Stott
# Canary: 418690f2-6cfb-487c-83ce-0b2d33ce0847
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A hot table's rows belong to one org, one environment and one model (REQ-230, REQ-595, REQ-1914).

The hot tier keeps the rows of small lookup tables in the process and in Redis and substitutes
them into queries. One manager serves the whole process and one Redis serves every process, and
both were keyed by the bare table name: two orgs — or prod and a branch, or two sources — with a
table of the same name read each other's rows, and a table pointed at another relation kept
serving the old one's.

The registry is now kept per org and environment, holds only rows loaded under the model the
runtime currently has, and is keyed by the registered table's id; the Redis key carries that scope
and the id. A statement substitutes the hot rows of the tables it reads — the ids the pipeline
resolved for it — so two sources' ``customers`` are each hot, each served to the statements that
read that one.
"""

# Requirements: REQ-230, REQ-232, REQ-595, REQ-1914, REQ-1529, REQ-1674

from __future__ import annotations

import pytest

from provisa.cache import hot_tables
from provisa.cache.hot_tables import HOT_PREFIX, HotTableCandidate, HotTableManager

ACME_ROWS = [{"id": 1, "name": "acme customer"}]
GLOBEX_ROWS = [{"id": 1, "name": "globex customer"}]


class _Acting:
    """Where the request is acting, and the model that runtime loaded."""

    def __init__(self) -> None:
        self.place = "acme"
        self.stamp: int | None = 7

    def __call__(self) -> tuple[str, int | None]:
        return self.place, self.stamp


@pytest.fixture
def acting(monkeypatch) -> _Acting:
    where = _Acting()
    monkeypatch.setattr(hot_tables, "_scope_parts", where)
    return where


@pytest.fixture
async def manager(acting):
    mgr = HotTableManager(redis_url=None, auto_threshold=100, max_rows=1000)
    await mgr._connect()
    await mgr._redis.flushall()
    yield mgr
    await mgr._redis.flushall()
    await mgr.close()


CUSTOMERS = 11  # the registered id of pg's public.customers
OTHER_CUSTOMERS = 12  # warehouse's public.customers: same name, another table


async def _load(
    mgr: HotTableManager,
    rows,
    *,
    table_id: int = CUSTOMERS,
    catalog: str = "pg",
    schema: str = "public",
) -> None:
    await mgr._store_rows(table_id, "customers", rows, "id", catalog, schema)


async def _keys(mgr: HotTableManager) -> list[str]:
    return sorted(await mgr._redis.keys(HOT_PREFIX + "*"))


# --- two orgs ------------------------------------------------------------------------------------


async def test_two_orgs_with_a_same_named_table_never_read_each_others_rows(manager, acting):
    await _load(manager, ACME_ROWS)
    assert manager.is_hot(CUSTOMERS) and await manager.get_rows(CUSTOMERS) == ACME_ROWS

    acting.place = "globex"
    assert not manager.is_hot(CUSTOMERS)
    assert manager.get_entry(CUSTOMERS) is None
    assert await manager.get_rows(CUSTOMERS) == []
    assert CUSTOMERS not in manager.managed_tables()
    assert manager.snapshot() == []

    await _load(manager, GLOBEX_ROWS)
    assert await manager.get_rows(CUSTOMERS) == GLOBEX_ROWS
    acting.place = "acme"
    assert await manager.get_rows(CUSTOMERS) == ACME_ROWS
    assert manager.get_entry(CUSTOMERS).rows == ACME_ROWS


async def test_another_worker_of_another_org_does_not_read_the_blob(manager, acting):
    """Two processes share one Redis. The blob one org's worker wrote is not at the key another
    org's worker reads."""
    await _load(manager, ACME_ROWS)
    other = HotTableManager(redis_url=None, auto_threshold=100, max_rows=1000)
    acting.place = "globex"
    await other._store_rows(CUSTOMERS, "customers", GLOBEX_ROWS, "id", "pg", "public")
    assert await other.get_rows(CUSTOMERS) == GLOBEX_ROWS
    acting.place = "acme"
    assert await manager.get_rows(CUSTOMERS) == ACME_ROWS
    assert len(await _keys(manager)) == 2
    await other.close()


async def test_a_blob_is_keyed_by_the_table_id_not_its_name(manager, acting):
    await _load(manager, ACME_ROWS)
    (key,) = await _keys(manager)
    assert key == HOT_PREFIX + f"acme:m7:t{CUSTOMERS}:blob"


# --- an environment ------------------------------------------------------------------------------


async def test_a_branch_does_not_read_prods_rows(manager, acting):
    await _load(manager, ACME_ROWS)
    acting.place = "acme_env_staging"
    assert not manager.is_hot(CUSTOMERS) and await manager.get_rows(CUSTOMERS) == []


# --- the model -----------------------------------------------------------------------------------


async def test_a_table_pointed_elsewhere_does_not_serve_the_old_rows(manager, acting):
    """The model changes (the table now reads another relation) and the runtime reloads at the
    next stamp: what was loaded under the previous model is not hot any more."""
    manager.register_candidate(HotTableCandidate(CUSTOMERS, "customers", "id", "pg", "public"))
    await _load(manager, ACME_ROWS)

    acting.stamp = 8
    assert not manager.is_hot(CUSTOMERS)
    assert manager.get_entry(CUSTOMERS) is None
    assert await manager.get_rows(CUSTOMERS) == []
    # It is still a candidate, so the next small read of it makes it hot again with the rows
    # that read returned.
    assert CUSTOMERS in manager.managed_tables()
    await manager.maybe_promote(CUSTOMERS, [(1, "current row")], ["id", "name"])
    assert await manager.get_rows(CUSTOMERS) == [{"id": 1, "name": "current row"}]


async def test_invalidating_a_table_removes_its_blob_and_only_in_the_acting_org(manager, acting):
    await _load(manager, ACME_ROWS)
    acting.place = "globex"
    await _load(manager, GLOBEX_ROWS)
    await manager.invalidate(CUSTOMERS)
    assert not manager.is_hot(CUSTOMERS)
    assert await _keys(manager) == [HOT_PREFIX + f"acme:m7:t{CUSTOMERS}:blob"]
    acting.place = "acme"
    assert await manager.get_rows(CUSTOMERS) == ACME_ROWS


# --- two sources in one model --------------------------------------------------------------------


async def test_two_sources_same_named_tables_are_each_hot_with_their_own_rows(manager, acting):
    """Two sources both hold ``public.customers``. Each is hot under its own id, and a statement
    is given the rows of the one it reads."""
    await _load(manager, ACME_ROWS, table_id=CUSTOMERS, catalog="pg")
    await _load(manager, GLOBEX_ROWS, table_id=OTHER_CUSTOMERS, catalog="warehouse")
    assert await manager.get_rows(CUSTOMERS) == ACME_ROWS
    assert await manager.get_rows(OTHER_CUSTOMERS) == GLOBEX_ROWS
    assert manager.entries_for([CUSTOMERS])["customers"].rows == ACME_ROWS
    assert manager.entries_for([OTHER_CUSTOMERS, 99])["customers"].rows == GLOBEX_ROWS
    assert len(await _keys(manager)) == 2


async def test_a_statement_reading_both_substitutes_neither(manager, acting):
    """A statement that reads both carries the name twice: substitution goes by name in its SQL,
    which cannot say which reference is which, so neither is substituted for that statement."""
    await _load(manager, ACME_ROWS, table_id=CUSTOMERS, catalog="pg")
    await _load(manager, GLOBEX_ROWS, table_id=OTHER_CUSTOMERS, catalog="warehouse")
    assert manager.entries_for([CUSTOMERS, OTHER_CUSTOMERS]) == {}
    assert manager.is_hot(CUSTOMERS) and manager.is_hot(OTHER_CUSTOMERS)


async def test_a_statement_is_given_only_the_tables_it_reads(manager, acting):
    await _load(manager, ACME_ROWS)
    assert manager.entries_for([]) == {}
    assert manager.entries_for([OTHER_CUSTOMERS]) == {}


# --- the scope is the runtime's ------------------------------------------------------------------


def test_the_scope_is_read_from_the_acting_runtime():
    import provisa.api.app as appmod
    from provisa.core.request_context import current_org

    runtime = appmod.state._active_runtime()
    held = runtime.model_stamp
    runtime.model_stamp = 4321
    token = current_org.set("acme")
    try:
        assert hot_tables._scope_parts() == ("acme", 4321)
    finally:
        current_org.reset(token)
        runtime.model_stamp = held


async def test_rows_a_caller_hands_over_are_held_under_their_table(manager, acting):
    """A caller that has just fetched a table's rows hands them to the tier with ``hold``; two
    sources' same-named tables are held apart."""
    from provisa.cache.hot_tables import HotTableEntry

    def entry(table_id: int, catalog: str, rows: list[dict]) -> HotTableEntry:
        return HotTableEntry(
            table_id, "customers", catalog, "public", "id", rows=rows, column_names=["id"]
        )

    manager.hold(entry(CUSTOMERS, "pg", ACME_ROWS))
    manager.hold(entry(OTHER_CUSTOMERS, "warehouse", GLOBEX_ROWS))
    assert manager.get_entry(CUSTOMERS).rows == ACME_ROWS
    assert manager.get_entry(OTHER_CUSTOMERS).rows == GLOBEX_ROWS

    acting.place = "globex"  # another org holds its own
    assert manager.get_entry(CUSTOMERS) is None


def test_nothing_outside_the_manager_writes_its_registry():
    import pathlib
    import re

    root = pathlib.Path(hot_tables.__file__).resolve().parents[1]
    offenders = [
        f"{path.relative_to(root)}:{n}"
        for path in sorted(root.rglob("*.py"))
        if path.name != "hot_tables.py"
        for n, line in enumerate(path.read_text().splitlines(), 1)
        if re.search(
            r"\._hot_tables\s*(\[[^\]]*\]\s*=[^=]|\.(pop|clear|update|setdefault)\()", line
        )
    ]
    assert offenders == []


# --- a write, boot, and what one statement holds -------------------------------------------------


class _Engine:
    """Answers the reload's SELECT with ``rows``, recording what it was asked."""

    def __init__(self, rows: list[tuple], columns: list[str]) -> None:
        self.rows, self.columns, self.sent = rows, columns, []

    def engine_physical(self, sql: str) -> str:
        return sql

    async def execute_engine(self, sql: str, *args, **kwargs):
        from provisa.executor.result import QueryResult

        self.sent.append(sql)
        return QueryResult(rows=self.rows, column_names=self.columns)


async def test_a_write_reloads_the_table_with_the_key_it_was_hot_under(manager, acting):
    """A write drops the table's hot rows and loads them again — with its own key and address,
    not a guessed ``id`` column."""
    await manager._store_rows(CUSTOMERS, "customers", ACME_ROWS, "customer_no", "pg", "crm")
    engine = _Engine([(1, "changed")], ["customer_no", "name"])
    await manager.refresh_after_write(engine, CUSTOMERS)
    assert engine.sent == ['SELECT * FROM "pg"."crm"."customers"']
    entry = manager.get_entry(CUSTOMERS)
    assert entry.pk_column == "customer_no"
    assert entry.rows == [{"customer_no": 1, "name": "changed"}]
    # A table that is not hot is left alone.
    await manager.refresh_after_write(engine, OTHER_CUSTOMERS)
    assert manager.get_entry(OTHER_CUSTOMERS) is None and len(engine.sent) == 1


def _hot_settings(monkeypatch) -> None:
    from provisa.core import settings_registry

    values = {
        "cache.enabled": True,
        "hot_tables.auto_threshold": 100,
        "hot_tables.max_bytes": 10_000_000,
        "hot_tables.max_rows": 1000,
        "hot_tables.refresh_interval": 300,
    }
    monkeypatch.setattr(settings_registry, "value", lambda key: values[key])
    monkeypatch.setattr("provisa.core.redis_location.redis_url", lambda: None)


async def test_boot_keeps_two_sources_same_named_tables_apart(monkeypatch, acting):
    """Two config tables named ``customers`` on two sources, both declared hot: each is loaded
    under its own registered id with its own source's rows."""
    _hot_settings(monkeypatch)
    raw = {
        "sources": [{"id": "pg", "type": "postgresql"}, {"id": "wh", "type": "postgresql"}],
        "tables": [
            {"source_id": "pg", "schema": "public", "table": "customers", "hot": True},
            {"source_id": "wh", "schema": "public", "table": "customers", "hot": True},
        ],
    }
    registered = [
        {"id": CUSTOMERS, "source_id": "pg", "schema_name": "public", "table_name": "customers"},
        {
            "id": OTHER_CUSTOMERS,
            "source_id": "wh",
            "schema_name": "public",
            "table_name": "customers",
        },
    ]

    class _PerCatalog(_Engine):
        async def execute_engine(self, sql: str, *args, **kwargs):
            from provisa.executor.result import QueryResult

            self.sent.append(sql)
            name = "pg row" if sql.startswith('SELECT * FROM "pg"') else "wh row"
            return QueryResult(rows=[(1, name)], column_names=["id", "name"])

    mgr = await hot_tables.init_hot_tables(raw, _PerCatalog([], []), registered)
    try:
        assert mgr.get_entry(CUSTOMERS).rows == [{"id": 1, "name": "pg row"}]
        assert mgr.get_entry(OTHER_CUSTOMERS).rows == [{"id": 1, "name": "wh row"}]
    finally:
        await mgr.close()


async def test_boot_refuses_a_config_table_that_is_not_registered(monkeypatch, acting):
    _hot_settings(monkeypatch)
    raw = {"tables": [{"source_id": "pg", "schema": "public", "table": "ghost", "hot": True}]}
    with pytest.raises(ValueError, match=r"config table pg/public.ghost is not registered"):
        await hot_tables.init_hot_tables(raw, _Engine([], []), [])


def test_a_statement_holds_fetched_rows_under_the_table_it_reads(acting):
    """Rows a statement fetched for ``customers`` are held under the id of the ``customers`` it
    reads; when it reads two tables of that name, they are substituted into it alone."""
    from types import SimpleNamespace

    from provisa.api.data.materialization import _StatementHot
    from provisa.cache.values_cte import InlineRows

    held: dict = {}
    manager = SimpleNamespace(
        hold=lambda e: held.__setitem__(e.table_id, e), entries_for=lambda _ids: {}
    )
    state = SimpleNamespace(
        tables=[
            {"id": CUSTOMERS, "table_name": "customers"},
            {"id": OTHER_CUSTOMERS, "table_name": "customers"},
        ]
    )
    kw = {"catalog": "c", "schema": "s", "pk_column": "id", "column_names": ["id"]}

    one = _StatementHot(manager, state, [OTHER_CUSTOMERS])
    one.hold("customers", rows=GLOBEX_ROWS, **kw)
    assert list(held) == [OTHER_CUSTOMERS] and held[OTHER_CUSTOMERS].rows == GLOBEX_ROWS

    both = _StatementHot(manager, state, [CUSTOMERS, OTHER_CUSTOMERS])
    assert isinstance(both.hold("customers", rows=ACME_ROWS, **kw), InlineRows)
    assert list(held) == [OTHER_CUSTOMERS]
