# Copyright (c) 2026 Kenneth Stott
# Canary: 4e1a6d02-9d7c-4d84-9f0e-2f5a1c3b7e60
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The full registration→query path for a connector-backed source, as a reusable harness.

Why this exists
---------------
Every ``tests/integration/test_*_source_e2e.py`` module proves the same narrow thing: that
``provisa.core.catalog.create_catalog`` produces a Trino catalog the coordinator can answer a
``SELECT`` from, over a raw ``trino.dbapi`` cursor using a three-part physical
``catalog.schema.table`` reference. That establishes reachability and nothing else — it is the
plumbing under the product, not the product.

The settled query pipeline (``_govern_and_route``/``_execute_plan`` in ``provisa/pgwire/_pipeline.py``)
never sees that shape. It *rejects* it: ``_reject_physical_source_refs`` raises ``PermissionError``
on a three-part physical ref, and only a registered semantic ``domain.table`` is accepted. So a
source can pass its ``*_source_e2e.py`` test and still be unusable, because the two steps between
reachability and a served query are untested:

  1. **Registration.** ``registerTable`` with columns that carry no ``data_type`` routes through
     ``_ensure_source_column_types`` (``provisa/api/admin/_table_ops.py:214``), which introspects
     ``<catalog>.information_schema.columns`` — plus ``table_constraints``/``key_column_usage`` for
     primary keys — through the engine. Connector catalogs vary wildly in what they expose there;
     a connector that answers ``SELECT`` fine but has a thin or absent ``information_schema``
     produces zero resolved types, and registration is *refused* (``schema.column_types_unresolved``).
     Registration is where a connector source realistically dies, and no test covered it.

  2. **Catalog-name agreement.** ``createSource`` records ``state.source_catalogs[id]`` from
     ``input.database or source_to_catalog(id)`` (``schema_mutation.py:378-381``), while the
     catalog is physically created by ``create_catalog`` from ``_to_catalog_name(source.id)``
     alone (``provisa/core/catalog.py:116``). Those two agree only when ``database`` is empty or
     already equals the source id. When they disagree the source registers cleanly and every
     query then fails the unknown-catalog gate (``_pipeline.py:535-559``). A raw-cursor test
     cannot see this: it names the physical catalog itself.

These helpers drive the real GraphQL mutations and the real ``/data/sql`` endpoint against an
in-process app bound to the isolated stack, so both steps are exercised for every source type.

Failures are always assertions with the server's own response text attached — never a skip. A
source whose container is missing is a provisioning failure for the marker to handle, and a
source that cannot register is a defect to fix, not a condition to tolerate.

Every connector-only type with a container is covered. One of them, **kafka**, does not use these
helpers: it never registers from a ``Source`` row (``TrinoKafkaConnector.details`` returns ``{}``),
so there is nothing for ``createSource`` to drive. Its catalog and its semantic table are both
produced by config load from a ``kafka_sources[]`` entry, and ``test_kafka_pipeline_e2e.py`` drives
that path instead — ending, as every module here does, at a ``"domain"."table"`` query through
``/data/sql``.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from collections.abc import Callable
from typing import Any

import pytest_asyncio
from httpx import ASGITransport, AsyncClient

# The role every test in this bucket registers for and queries as. It must be a role the app
# actually seeds (config/provisa.yaml:7 makes org_admin the default identity) — a role name that
# does not resolve produces an empty role dict, so stage2 (compiler/stage2.py:140) sees neither the
# ADMIN capability nor a matching role_id in visible_to, and every column comes back V003
# "not visible to this role". "admin" is not one of the seeded roles.
_ROLE = "org_admin"
_VISIBLE_TO_LITERAL = '["org_admin", "analyst"]'


@pytest_asyncio.fixture(loop_scope="session")
async def connector_client():
    """An in-process app bound to the isolated stack's Trino coordinator.

    ``build_engine`` (``provisa/federation/engine.py:1266``) defaults to the embedded DuckDB engine,
    and DuckDB has no Trino connectors at all — a connector-only source registers against it and
    then every reference to its catalog fails ``Catalog "…" does not exist``. Pinning
    ``PROVISA_ENGINE=trino`` is therefore part of the definition of this test bucket, not a tuning
    knob: these source types exist only as Trino catalogs. ``TRINO_HOST``/``TRINO_PORT`` are already
    in the environment from ``_allocate_itest_ports`` and win over config
    (``provisa/federation/engine.py:1238``), so the app lands on the isolated coordinator.

    Setting the variable is not sufficient on its own: ``AppState.__init__`` calls ``build_engine``
    once, at import of ``provisa.api.app`` (``app.py:278``), which in a pytest session has already
    happened by the time any fixture runs. So the engine is rebuilt here explicitly — the same
    two lines startup uses for a dedicated per-org engine (``app.py:860-861``) — and both the env
    pin and the previous engine are restored on teardown, leaving the rest of the session on the
    default engine.
    """
    import provisa.api.app as app_mod
    from provisa.federation.engine import build_engine
    from provisa.federation.runtime import EngineRuntime

    prior_env = os.environ.get("PROVISA_ENGINE")
    os.environ["PROVISA_ENGINE"] = "trino"
    os.environ.setdefault("PG_PASSWORD", "provisa")
    prior_engine = app_mod.state.federation_engine
    app_mod.state.federation_engine = EngineRuntime(build_engine(), app_mod.state)
    app_mod.state.federation_engine.bind_terminal()
    try:
        app = app_mod.create_app()
        async with app.router.lifespan_context(app):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test", timeout=180) as c:
                yield c
    finally:
        app_mod.state.federation_engine = prior_engine
        if prior_env is None:
            os.environ.pop("PROVISA_ENGINE", None)
        else:
            os.environ["PROVISA_ENGINE"] = prior_env


async def _gql(client: AsyncClient, query: str, field: str) -> dict[str, Any]:
    """POST an admin GraphQL mutation and return its MutationResult payload.

    Raises on a transport error, a GraphQL ``errors`` array, or ``success: false`` — each with the
    raw response body, because the server's message names the actual cause (an unresolved column
    type, a failed connection validation) and a bare assertion would discard it.
    """
    resp = await client.post("/admin/graphql", json={"query": query})
    assert resp.status_code == 200, f"{field}: HTTP {resp.status_code}: {resp.text}"
    body = resp.json()
    assert "errors" not in body, f"{field}: GraphQL errors: {resp.text}"
    result = body["data"][field]
    assert result["success"], f"{field} failed: {result.get('message')!r} (full: {resp.text})"
    return result


async def create_domain(client: AsyncClient, domain_id: str) -> None:
    await _gql(
        client,
        f'mutation {{ createDomain(input: {{ id: "{domain_id}", description: "connector e2e" }}) '
        "{ success message } }",
        "createDomain",
    )


async def create_source(
    client: AsyncClient,
    *,
    source_id: str,
    source_type: str,
    host: str,
    port: int,
    database: str = "",
    username: str = "",
    password: str = "",
    path: str = "",
    mapping: dict[str, Any] | None = None,
) -> None:
    """Register the source through the real ``createSource`` mutation.

    This is the seam the raw-cursor tests skip. It persists the source, records
    ``state.source_types``/``source_catalogs``, and provisions the catalog on the bound engine via
    ``_register_source_on_engine``. Note that that last step logs and swallows its exception
    (``schema_common.py:233-236``), so a catalog that fails to create still yields
    ``success: true`` here — which is precisely why the caller must go on to register a table and
    run a query rather than trusting this return.

    ``host``/``port`` are the values *Trino* uses to reach the source, so they are compose service
    names on the stack's private network, never the host-published ephemeral ports the test process
    itself uses for seeding.

    ``path`` is the object-store URL of a file-shaped source (parquet/csv). It replaces host/port
    for a source the engine reaches by location rather than by connection — a warehouse engine's
    external link (Synapse OPENROWSET over ADLS) is the case that needs it.
    """
    mapping_arg = ""
    if mapping is not None:
        # Embed as a GraphQL string literal; json.dumps twice — once for the mapping object, once
        # to escape it as a literal — so quotes inside the mapping survive.
        mapping_arg = f", mappingJson: {json.dumps(json.dumps(mapping))}"
    path_arg = f", path: {json.dumps(path)}" if path else ""
    await _gql(
        client,
        f"""
        mutation {{
            createSource(input: {{
                id: "{source_id}",
                type: "{source_type}",
                host: "{host}",
                port: {port},
                database: "{database}",
                username: "{username}",
                password: {json.dumps(password)}
                {path_arg}
                {mapping_arg}
            }}) {{ success message }}
        }}
        """,
        "createSource",
    )


async def register_table(
    client: AsyncClient,
    *,
    source_id: str,
    domain_id: str,
    schema_name: str,
    table_name: str,
    alias: str,
    columns: list[str],
    column_types: dict[str, str] | None = None,
) -> None:
    """Register a table with UNTYPED columns, forcing source introspection.

    Passing ``visibleTo`` but no ``dataType`` is the deliberate part: it takes the
    ``_ensure_source_column_types`` branch, which resolves every type from the source's
    ``information_schema`` through the engine and refuses the registration if any column's type
    cannot be resolved. Supplying types here would make the test pass while proving nothing about
    whether the connector is actually introspectable — the exact gap this harness closes.

    ``column_types`` opts out of that, and only one source shape may use it: a warehouse *engine*
    external link, where the object the types would be read from does not exist yet. For a Synapse
    OPENROWSET source the view is created by ``NativeEngineBackend._attach_registered``
    (``provisa/federation/native_backend.py:76``) from ``state.tables`` — i.e. only after this
    registration lands — so ``resolve_available_columns_metadata`` (``schema_query.py:864``) would
    be introspecting an object that registration itself is the precondition for. Steward-assigned
    types are the designed answer for that case (``types.py:363-366``). Never pass this for a
    connector source: there it would hide the introspection failure this harness exists to catch.
    """
    types = column_types or {}
    cols = ", ".join(
        f'{{ name: "{c}", visibleTo: {_VISIBLE_TO_LITERAL}'
        + (f', dataType: "{types[c]}"' if c in types else "")
        + " }"
        for c in columns
    )
    await _gql(
        client,
        f"""
        mutation {{
            registerTable(input: {{
                sourceId: "{source_id}",
                domainId: "{domain_id}",
                schemaName: "{schema_name}",
                tableName: "{table_name}",
                alias: "{alias}",
                columns: [{cols}]
            }}) {{ success message }}
        }}
        """,
        "registerTable",
    )


async def rebuild_schemas(client: AsyncClient) -> None:
    """Rebuild the compilation context so the new table enters the governance table_map.

    Without this the table is persisted but absent from ``gov_ctx.table_map``, and the pipeline
    rejects the query as ``V000`` (not accessible for role) rather than routing it.
    """
    await _gql(client, "mutation { rebuildSchemas { success message } }", "rebuildSchemas")


async def query_semantic(
    client: AsyncClient, sql: str, *, role: str = _ROLE
) -> list[dict[str, Any]]:
    """Run semantic SQL through ``/data/sql`` — the settled pipeline, end to end.

    The caller must pass a two-part ``"domain"."alias"`` reference. A three-part physical
    ``catalog.schema.table`` is what the old raw-cursor tests used and is rejected here by design.
    """
    resp = await client.post("/data/sql", json={"sql": sql, "role": role})
    assert resp.status_code == 200, f"/data/sql {sql!r}: HTTP {resp.status_code}: {resp.text}"
    body = resp.json()
    assert "data" in body, f"/data/sql {sql!r}: no data in {resp.text}"
    return body["data"]["sql"]


async def assert_registration_and_query(
    client: AsyncClient,
    *,
    source_id: str,
    source_type: str,
    host: str,
    port: int,
    domain_id: str,
    schema_name: str,
    table_name: str,
    columns: list[str],
    expected_rows: list[dict[str, Any]],
    order_by: str,
    database: str = "",
    username: str = "",
    password: str = "",
    path: str = "",
    column_types: dict[str, str] | None = None,
    mapping: dict[str, Any] | None = None,
    alias: str | None = None,
    settle_seconds: float = 0,
    seed_after_create_source: Callable[[str], None] | None = None,
) -> None:
    """Drive the whole path and assert the served rows.

    One call covers: createSource → createDomain → registerTable (types introspected from the
    source) → rebuildSchemas → semantic SELECT through ``_govern_and_route``/``_execute_plan``.

    ``settle_seconds`` re-runs the *final* query until the rows match, for sources whose write
    path is eventually consistent (Splunk indexes asynchronously and generates data-model
    summaries on its own schedule; Druid hands off segments). It re-runs only the query — every
    mutation before it is still expected to succeed on the first attempt, so a registration
    failure surfaces immediately instead of being retried into a timeout. At the default of 0 the
    query runs exactly once.

    ``seed_after_create_source`` is called with the physical catalog name once the source exists
    and before the table is registered. Hive and hive_s3 need it: their storage has no table until
    one is written *through* the catalog, so the data cannot be seeded before the catalog is
    created, and registration cannot run before the data is there.
    """
    alias = alias or table_name
    await create_source(
        client,
        source_id=source_id,
        source_type=source_type,
        host=host,
        port=port,
        database=database,
        username=username,
        password=password,
        path=path,
        mapping=mapping,
    )
    if seed_after_create_source is not None:
        # source_to_catalog only maps '-' → '_'; every id in this bucket is already hyphen-free,
        # so the catalog name is the source id (see the module docstring on why the two must not
        # be allowed to diverge here).
        seed_after_create_source(source_id)
    await create_domain(client, domain_id)
    await register_table(
        client,
        source_id=source_id,
        domain_id=domain_id,
        schema_name=schema_name,
        table_name=table_name,
        alias=alias,
        columns=columns,
        column_types=column_types,
    )
    await rebuild_schemas(client)

    projection = ", ".join(f'"{c}"' for c in columns)
    sql = f'SELECT {projection} FROM "{domain_id}"."{alias}" ORDER BY "{order_by}"'
    deadline = time.monotonic() + settle_seconds
    while True:
        rows = await query_semantic(client, sql)
        if rows == expected_rows or time.monotonic() >= deadline:
            break
        await asyncio.sleep(5)
    assert rows == expected_rows, f"served rows {rows!r} != expected {expected_rows!r}"
