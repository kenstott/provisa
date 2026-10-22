# Copyright (c) 2026 Kenneth Stott
# Canary: 8ce03dc0-c82b-4c52-8d43-bea87b27f9e4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A demo's replicas, landed once and copied everywhere else (REQ-2006).

A demo is seeded when its bundle is built (REQ-2049), never at a visitor's request and never on
the deployment: the bundle build lands the demo's template once, against the demo's own sources,
and writes the template's replicas out as the demo's SEED -- ``seeded/replicas.duckdb`` beside its
demo.yaml, and ``seeded/replicas.json``, the record of each replica. The bundle carries the seed.

When the sandbox tier starts, a template whose replicas are not landed ADOPTS the seed: the
tables are copied into the template's own store and recorded as built for that store and for the
template's model as this deployment has it. A visitor's sandbox is then a copy of the template's
replicas in turn (demo_sandbox.py). None of these copies reads a demo's source: the template's and
the sandboxes' sources replicate always and are refreshed only on a signal (demo_templates), so a
deployment needs none of a demo's databases running.

Each copy's record carries the definition its own convergence computes -- the address a replica
is written at names the org, and the source's identity is the deployment's -- so nothing the copy
records is rebuilt for being recorded under another org's or another build's digest.

A demo's bitemporal views (REQ-1162) keep history no rebuild from its sources can give back: each
version its build's setup appended, as its sources changed. Their append logs travel the same way
-- written out by the bundle build after the setup (``seeded/views.duckdb`` and
``seeded/views.json``), copied into the template's store when its runtime is built, and from the
template into each sandbox's. A refresh over sources unchanged since then appends nothing, so the
history is the vault's own wherever the demo runs.

The records of the refreshes that built that history (REQ-862: ``ops.mv_refresh_lineage``, what
each was built from) travel with it -- written out by the build beside the append logs
(``seeded/refresh_lineage.parquet``) and landed as the template's, then each sandbox's, wherever
the history is copied. They are the records of those builds, carried, not new ones: their times
and traces are as recorded, so a time read from them falls where it fell against that history.
"""

# Requirements: REQ-2006, REQ-1992, REQ-1920

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

#: The seed of a published demo, beside its demo.yaml.
SEED_DIR = "seeded"
SEED_STORE = "replicas.duckdb"
SEED_RECORD = "replicas.json"

#: A demo's bitemporal views' append logs, beside its seed (REQ-1162).
VIEWS_STORE = "views.duckdb"
VIEWS_RECORD = "views.json"
#: The refreshes that built them, as each refresh's record has it (REQ-862).
REFRESHES_STORE = "refresh_lineage.parquet"
_VIEWS_SCHEMA = "views"

#: The fields of a replica's record a seed carries; every other one is the copy's own.
_SEED_FIELDS = (
    "source_id",
    "schema_name",
    "table_name",
    "rows_copied",
    "completed_rows",
    "content_hash",
    "built_columns",
    "build_method",
    "load_kind",
)


def store_identity(org_id: str, store: str | None = None) -> str:
    """What a replica built by ``org_id``'s own engine records as its store: the build's own
    reading (replica_builds.store_identity) of the engine an org of the sandbox tier runs, landing
    in its embedded file -- or in ``store``, its region's (REQ-2068, demo_region_seed)."""
    from types import SimpleNamespace

    from provisa.core.request_context import reset_current_org, set_current_org
    from provisa.federation.engine import build_engine, embedded_store_dsn
    from provisa.federation.replica_builds import store_identity as build_identity

    engine = build_engine("duckdb")
    engine.pin_materialize_store(store if store is not None else embedded_store_dsn(org_id))
    # Read as the build reads it: with org_id bound, not whichever org the caller is bound to --
    # the regions tier's boot org runs an engine of its own, the template org does not.
    token = set_current_org(org_id)
    try:
        return build_identity(SimpleNamespace(federation_engine=SimpleNamespace(engine=engine)))
    finally:
        reset_current_org(token)


def _store_path(org_id: str) -> str:
    from provisa.federation.engine import embedded_store_dsn

    return embedded_store_dsn(org_id).removeprefix("duckdb:///")


def _replicas_schema(org_id: str) -> str:
    from provisa.core.environments import org_schema
    from provisa.federation.replica_address import REPLICAS_SUFFIX

    return org_schema(org_id, None, REPLICAS_SUFFIX)


def _views_schema(org_id: str) -> str:
    from provisa.core.environments import org_schema
    from provisa.federation.replica_address import MVS_SUFFIX

    return org_schema(org_id, None, MVS_SUFFIX)


def bitemporal_view_tables(config: Any) -> list[str]:
    """The stored tables of a configuration's bitemporal materialized views (REQ-1162): each one's
    append log, by name."""
    from provisa.core.models import DERIVED_SOURCE_ID

    return sorted(
        f"mv_{table.table_name}"
        for table in config.tables
        if table.source_id == DERIVED_SOURCE_ID and table.materialize and table.is_bitemporal
    )


def view_mv_id(table: str) -> str:
    """The materialized view whose append log is the stored table ``table`` (``mv_<table name>``)."""
    return "view-" + table.removeprefix("mv_")


def _refreshes_path(demo_directory: Path) -> Path:
    return demo_directory / SEED_DIR / REFRESHES_STORE


def land_refreshes(demo_directory: Path, org_id: str) -> int:
    """Land the demo's carried refresh records (REQ-862) as ``org_id``'s, where its views' history
    has just been copied; how many. A bundle built before they were carried has none."""
    import pyarrow.parquet as pq

    from provisa.observability import native_traces

    path = _refreshes_path(demo_directory)
    if not path.exists():
        return 0
    rows = pq.read_table(path).cast(native_traces.TRACES_SCHEMA).to_pylist()
    native_traces.land_records(native_traces.traces_dir(), rows, org_id)
    return len(rows)


def _state_db(org_id: str) -> Any:
    """``org_id``'s STATE handle (REQ-1920), scoped to its schema, with no runtime needed."""
    from provisa.api.admin.orgs_router import _pool
    from provisa.core import process_region
    from provisa.core.database import Database
    from provisa.core.environments import org_schema
    from provisa.core.regions import DEFAULT_REGION

    if process_region.region() != DEFAULT_REGION:
        # A region keeps its replicas' record in a store of its own (REQ-1922): the regions
        # sandbox tier's demos are copied by demo_region_seed, never through this handle.
        raise RuntimeError("a region's demo record is kept in its own store (demo_region_seed)")
    return Database(_pool().engine, name="org-state", search_path=org_schema(org_id), holds="state")


async def definitions(
    model_org: str, keys: list[tuple[str, str, str]], address_org: str
) -> dict[tuple[str, str, str], str]:
    """Each replica's definition as ``address_org``'s convergence computes it -- the convergence's
    own computation (replica_converge.wanted_shapes) over ``model_org``'s model, at
    ``address_org``'s replica address. A table whose definition cannot be computed is refused by
    name: its replica could not be recorded as built for it."""
    from provisa.api.app import ensure_org_runtime
    from provisa.core.request_context import reset_current_org, set_current_org

    await ensure_org_runtime(model_org)
    token = set_current_org(model_org)
    try:
        return await _bound_definitions(model_org, keys, address_org)
    finally:
        reset_current_org(token)


async def _bound_definitions(
    model_org: str, keys: list[tuple[str, str, str]], address_org: str
) -> dict[tuple[str, str, str], str]:
    """:func:`definitions`, with ``model_org``'s runtime built and bound already."""
    from provisa.api.app import state
    from provisa.federation.replica_converge import wanted_shapes

    shapes = await wanted_shapes(state, set(keys), address_org=address_org)
    missing = [".".join(key) for key in keys if key not in shapes]
    if missing:
        raise RuntimeError(f"{model_org} has no buildable definition for {', '.join(missing)}")
    return {key: shapes[key][0] for key in keys}


def _as_built(
    rows: list[dict], org_id: str, defined: dict, now: datetime, store: str | None = None
) -> list[dict]:
    built = store_identity(org_id, store)
    return [
        {
            **{field: row[field] for field in _SEED_FIELDS},
            "built_store": built,
            "definition_hash": defined[(row["source_id"], row["schema_name"], row["table_name"])],
            "completed_at": now,
            "build_state": "idle",
        }
        for row in rows
    ]


def _key(row: dict) -> tuple[str, str, str]:
    return (row["source_id"], row["schema_name"], row["table_name"])


async def copy_to_sandbox(template_org: str, org_id: str) -> None:
    """The template's landed replicas, into a sandbox's own store and its own record. Each
    template digest is checked first: a replica not built from the template's model as this
    deployment has it is refused by name rather than handed to a visitor."""
    from provisa.api import demo_region_seed
    from provisa.federation import replica_state
    from provisa.federation.materialize_broker import copy_schema

    if demo_region_seed.regional():
        # REQ-2068: each region's replicas and views, in that region's stores.
        await demo_region_seed.copy_to_sandbox(template_org, org_id)
        return

    await asyncio.to_thread(
        copy_schema,
        _store_path(template_org),
        _replicas_schema(template_org),
        _store_path(org_id),
        _replicas_schema(org_id),
    )
    async with _state_db(template_org).acquire() as conn:
        rows = await replica_state.completed_rows(conn)
    keys = [_key(row) for row in rows]
    own = await definitions(template_org, keys, template_org)
    for row in rows:
        if own[_key(row)] != row["definition_hash"]:
            raise RuntimeError(
                f"the {template_org} replica of {'.'.join(_key(row))} was not built from the "
                "template's model; the template is landed again at the sandbox tier's next start"
            )
    copied = _as_built(
        rows, org_id, await definitions(template_org, keys, org_id), datetime.now(UTC)
    )
    async with _state_db(org_id).acquire() as conn:
        await replica_state.record_copied(conn, copied)


async def copy_views_to_sandbox(
    template_org: str, org_id: str, tables: list[str], demo_directory: Path
) -> None:
    """The template's bitemporal views' append logs (``tables``), into a sandbox's own store: the
    sandbox keeps their history, and its first refresh over the demo's unchanged sources appends
    nothing (REQ-1162). The refreshes that built it are landed as the sandbox's (REQ-862)."""
    from provisa.api import demo_region_seed
    from provisa.federation.materialize_broker import copy_schema

    if demo_region_seed.regional():
        return  # REQ-2068: copied with each region's replicas (copy_to_sandbox)

    await asyncio.to_thread(
        copy_schema,
        _store_path(template_org),
        _views_schema(template_org),
        _store_path(org_id),
        _views_schema(org_id),
        tables,
    )
    await asyncio.to_thread(land_refreshes, demo_directory, org_id)


def seed_paths(demo_directory: Path) -> tuple[Path, Path]:
    """Where a demo's seed is: its replicas' store and their record."""
    seed = demo_directory / SEED_DIR
    return seed / SEED_STORE, seed / SEED_RECORD


def views_seed_paths(demo_directory: Path) -> tuple[Path, Path]:
    """Where a demo's bitemporal views' append logs are: their store and their record."""
    seed = demo_directory / SEED_DIR
    return seed / VIEWS_STORE, seed / VIEWS_RECORD


def export_views(org_id: str, demo_directory: Path, tables: list[str]) -> None:
    """Write ``org_id``'s bitemporal views' append logs (``tables``) out beside the demo's seed (the
    bundle build, after its setup). One the org has not built fails the build by name: the bundle
    would serve that view with no history."""
    from provisa.federation.materialize_broker import copy_schema

    store, record = views_seed_paths(demo_directory)
    store.parent.mkdir(parents=True, exist_ok=True)
    store.unlink(missing_ok=True)
    copied = copy_schema(
        _store_path(org_id), _views_schema(org_id), str(store), _VIEWS_SCHEMA, tables
    )
    missing = sorted(set(tables) - set(copied))
    if missing:
        raise RuntimeError(f"these bitemporal views were never built: {', '.join(missing)}")
    # REQ-862: and the records of the refreshes that built them, landed by this process's run.
    import pyarrow as pa
    import pyarrow.parquet as pq

    from provisa.observability import native_traces

    refreshes = native_traces.refresh_records(
        native_traces.traces_dir(), [view_mv_id(t) for t in copied]
    )
    path = _refreshes_path(demo_directory)
    path.unlink(missing_ok=True)
    if refreshes:
        pq.write_table(pa.Table.from_pylist(refreshes, schema=native_traces.TRACES_SCHEMA), path)
    record.write_text(
        json.dumps(
            {"schema": _VIEWS_SCHEMA, "tables": copied, "refreshes": len(refreshes)}, indent=2
        )
        + "\n"
    )


def adopt_views(template_org: str, demo_directory: Path) -> bool:
    """Copy the demo's bitemporal views' append logs into the template's store when its bundle
    carries them and the store does not hold them yet. Run within the template's runtime build,
    before its views' refreshes are scheduled, so its first refresh appends to the history rather
    than starting one. Whether they were copied."""
    from provisa.api import demo_region_seed
    from provisa.federation.materialize_broker import copy_schema

    if demo_region_seed.regional():
        return False  # REQ-2068: landed with each region's seed (adopt_seed)

    store, record = views_seed_paths(demo_directory)
    if not record.exists():
        return False
    tables = json.loads(record.read_text())["tables"]
    if set(tables) <= set(_held_rows(template_org, tables, _views_schema(template_org))):
        return False
    copy_schema(
        str(store), _VIEWS_SCHEMA, _store_path(template_org), _views_schema(template_org), tables
    )
    land_refreshes(demo_directory, template_org)  # REQ-862: the refreshes that built it
    return True


async def export_seed(template_org: str, demo_directory: Path) -> None:
    """Write the template's landed replicas out as the demo's seed (the bundle build)."""
    from provisa.api import demo_region_seed
    from provisa.federation import replica_state
    from provisa.federation.materialize_broker import copy_schema

    if demo_region_seed.regional():
        await demo_region_seed.export_seed(template_org, demo_directory)
        return

    store, record = seed_paths(demo_directory)
    store.parent.mkdir(parents=True, exist_ok=True)
    store.unlink(missing_ok=True)
    tables = await asyncio.to_thread(
        copy_schema,
        _store_path(template_org),
        _replicas_schema(template_org),
        str(store),
        _replicas_schema(template_org),
    )
    async with _state_db(template_org).acquire() as conn:
        rows = await replica_state.completed_rows(conn)
    record.write_text(
        json.dumps(
            {
                "schema": _replicas_schema(template_org),
                "tables": tables,
                "replicas": [{field: row[field] for field in _SEED_FIELDS} for row in rows],
            },
            indent=2,
            default=str,
        )
        + "\n"
    )


async def landed_here(template_org: str) -> int:
    """How many of the template's replicas are landed in THIS deployment's store: recorded as
    built for it, and held by it with the rows recorded (a table's shape is re-created empty from
    its record when the runtime is built, so a table that is there is not yet one that is landed:
    a data directory replaced since keeps the records and loses the rows)."""
    from provisa.api import demo_region_seed
    from provisa.federation import replica_state
    from provisa.federation.replica_address import replica_table_name

    if demo_region_seed.regional():
        return await demo_region_seed.landed_here(template_org)

    async with _state_db(template_org).acquire() as conn:
        rows = await replica_state.completed_rows(conn)
    built = store_identity(template_org)
    expected = {
        replica_table_name(r["source_id"], r["schema_name"], r["table_name"]): r["completed_rows"]
        for r in rows
        if r["built_store"] == built
    }
    held = await asyncio.to_thread(
        _held_rows, template_org, list(expected), _replicas_schema(template_org)
    )
    return sum(1 for name, count in expected.items() if held.get(name) == count)


def _held_rows(org_id: str, tables: list[str], schema: str) -> dict[str, int]:
    """The rows each of ``tables`` holds in ``schema`` of ``org_id``'s store; a table not there is
    left out."""
    from provisa.federation.materialize_broker import get_broker

    store = _store_path(org_id)
    if not Path(store).exists():
        return {}
    broker = get_broker(store)
    present = set(broker.tables(schema))
    return {
        name: int(broker.execute(f'SELECT count(*) FROM mat_store."{schema}"."{name}"')[0][0])
        for name in tables
        if name in present
    }


def store_counts(org_id: str) -> dict[str, dict[str, int]]:
    """The rows each replica and each view of ``org_id`` holds in its store file, read without
    its runtime: ``{"replicas": {table: rows}, "views": {table: rows}}``. A store not on disk
    holds none."""
    from provisa.federation.materialize_broker import get_broker

    store = _store_path(org_id)
    out: dict[str, dict[str, int]] = {"replicas": {}, "views": {}}
    if not Path(store).exists():
        return out
    broker = get_broker(store)
    for kind, schema in (("replicas", _replicas_schema(org_id)), ("views", _views_schema(org_id))):
        out[kind] = {
            name: int(broker.execute(f'SELECT count(*) FROM mat_store."{schema}"."{name}"')[0][0])
            for name in broker.tables(schema)
        }
    return out


async def adopt_seed(template_org: str, demo_directory: Path) -> bool:
    """Land the template's replicas from the demo's seed, when its bundle carries one: copied into
    the template's store, and recorded as built for it, over anything recorded before. Run within
    the template's runtime build (app._build_org_runtime), before its replica convergence is
    wired, so convergence finds them built and asks for no build -- a build would read a source
    the deployment does not run. Whether there was a seed."""
    from provisa.api import demo_region_seed
    from provisa.federation import replica_state
    from provisa.federation.materialize_broker import copy_schema

    if demo_region_seed.regional():
        return await demo_region_seed.adopt_seed(template_org, demo_directory)

    store, record = seed_paths(demo_directory)
    if not record.exists():
        return False
    seed = json.loads(record.read_text())
    await asyncio.to_thread(
        copy_schema,
        str(store),
        seed["schema"],
        _store_path(template_org),
        _replicas_schema(template_org),
    )
    rows = seed["replicas"]
    keys = [_key(row) for row in rows]
    defined = await _bound_definitions(template_org, keys, template_org)
    adopted = _as_built(rows, template_org, defined, datetime.now(UTC))
    async with _state_db(template_org).acquire() as conn:
        await replica_state.record_adopted(conn, adopted)
    return True
