# Copyright (c) 2026 Kenneth Stott
# Canary: 2abaebb8-8834-456d-9d6c-755efe5fcfa4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A live ServiceNow source end to end (REQ-1954): created and its tables registered through the
admin GraphQL, read through the settled pipeline with a filter and a limit, and written through
GraphQL mutations on both write routes:

- ``incident``, bound to no import set: its insert, update and delete are written to the table;
- ``sys_user``, bound to the import set of one of the instance's transform maps into it: its
  insert goes through the instance's Import Set API; its delete is written to the table.

The DuckDB engine reads the source by attaching its own server; see ``test_servicenow_source_e2e``
for the server alone. It reads SERVICENOW_INSTANCE_URL, SERVICENOW_USERNAME and
SERVICENOW_PASSWORD. Every record it writes is named ``provisa-itest-`` plus a random suffix and is
deleted by that name whether the test passes or fails, and read back gone; it touches no record it
did not create. The staging rows an import-set insert leaves are the run's too and are deleted by
the same name (REQ-254).
"""

from __future__ import annotations

import json
import os
from typing import Any

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from tests.integration.connector_source_harness import (
    _gql,
    create_domain,
    query_semantic,
    rebuild_schemas,
)
from tests.conftest import as_deployment_org
from tests.helpers import registered_id, release_mutation, write_grants_mutation
from tests.integration.live_teardown import none_remain, run_every_async, run_name

pytestmark = [
    pytest.mark.integration,
    pytest.mark.requires_servicenow,
    pytest.mark.requires_warehouse,
    pytest.mark.asyncio(loop_scope="session"),
]

_SOURCE_ID = "servicenow-pipeline"
_SCHEMA = "servicenow_pipeline"
_DOMAIN = "snow_e2e"
_ROLE = "org_admin"
_VISIBLE_TO = '["org_admin"]'


@pytest_asyncio.fixture(loop_scope="session", scope="module")
async def client():
    """An in-process app on the DuckDB engine, which attaches the source's own server."""
    import provisa.api.app as app_mod
    from provisa.federation.engine import build_engine
    from provisa.federation.runtime import EngineRuntime

    prior_env = os.environ.get("PROVISA_ENGINE")
    os.environ["PROVISA_ENGINE"] = "duckdb"
    # The engine is the deployment org's, set as its boot sets it (REQ-1266).
    with as_deployment_org():
        prior_engine = app_mod.state.federation_engine
        app_mod.state.federation_engine = EngineRuntime(build_engine(), app_mod.state)
        app_mod.state.federation_engine.bind_terminal()
    try:
        app = app_mod.create_app()
        async with app.router.lifespan_context(app):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test", timeout=600) as c:
                yield c
    finally:
        with as_deployment_org():
            app_mod.state.federation_engine = prior_engine
        if prior_env is None:
            os.environ.pop("PROVISA_ENGINE", None)
        else:
            os.environ["PROVISA_ENGINE"] = prior_env


async def _admin(client: AsyncClient, query: str) -> dict[str, Any]:
    resp = await client.post("/admin/graphql", json={"query": query})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "errors" not in body, resp.text
    return body["data"]


async def _data(client: AsyncClient, query: str) -> dict[str, Any]:
    resp = await client.post(
        "/data/graphql", json={"query": query}, headers={"x-provisa-role": _ROLE}
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "errors" not in body, resp.text
    return body["data"]


async def _register(
    client: AsyncClient, table: str, columns: list[str], import_set: str | None = None
) -> None:
    cols = ", ".join(f'{{ name: "{c}", visibleTo: {_VISIBLE_TO} }}' for c in columns)
    bound = f", importSet: {json.dumps(import_set)}" if import_set else ""
    registered = await _gql(
        client,
        f"""
        mutation {{
            registerTable(input: {{
                sourceId: "{_SOURCE_ID}", domainId: "{_DOMAIN}", schemaName: "{_SCHEMA}",
                tableName: "{table}", alias: "{table}", columns: [{cols}]{bound}
            }}) {{ success message }}
        }}
        """,
        "registerTable",
    )
    table_id = registered_id(registered["message"])
    # The tests write both tables: the role may write every registered column (REQ-2074).
    await _gql(client, write_grants_mutation(table_id, [_ROLE], columns), "setTableWriteGrants")
    await _gql(client, release_mutation(table_id), "setTableDraft")


async def _mutation(client: AsyncClient, verb: str, table: str) -> str:
    """The data GraphQL mutation field that ``verb``s ``table`` (insertIncident, updateSysUser...)."""
    data = await _data(client, "{ __schema { mutationType { fields { name } } } }")
    names = [f["name"] for f in data["__schema"]["mutationType"]["fields"]]
    wanted = verb + "".join(part.capitalize() for part in table.split("_"))
    (field,) = [n for n in names if n.split("__")[-1] == wanted]
    return field


@pytest_asyncio.fixture(loop_scope="session", scope="module")
async def registered(client):
    """The source, ``incident`` unbound and ``sys_user`` bound to an import set, released and in
    the schema; the import set is the first of the instance's transform maps into sys_user."""
    await create_domain(client, _DOMAIN)
    await _gql(
        client,
        f"""
        mutation {{
            createSource(input: {{
                id: "{_SOURCE_ID}", type: "servicenow",
                host: {json.dumps(os.environ["SERVICENOW_INSTANCE_URL"])}, port: 0, database: "",
                username: {json.dumps(os.environ["SERVICENOW_USERNAME"])},
                password: {json.dumps(os.environ["SERVICENOW_PASSWORD"])}
            }}) {{ success message }}
        }}
        """,
        "createSource",
    )
    maps = (
        await _admin(
            client,
            f'{{ availableImportSets(sourceId: "{_SOURCE_ID}", schemaName: "{_SCHEMA}", '
            'tableName: "sys_user") { name importSet } }',
        )
    )["availableImportSets"]
    assert maps, "the instance has no active transform map into sys_user"
    import_set = maps[0]["importSet"]
    await _register(client, "incident", ["sys_id", "number", "short_description", "active"])
    await _register(client, "sys_user", ["sys_id", "user_name", "first_name", "email"], import_set)
    await rebuild_schemas(client)
    return import_set


def _own_name() -> str:
    return run_name("provisa-itest", "-")


def _email(name: str) -> str:
    """The email of user ``name``: the instance's transform map into sys_user coalesces on it."""
    return f"{name}@example.invalid"


async def test_a_registered_table_is_read_through_a_filter_and_a_limit(client, registered):
    del registered
    rows = await query_semantic(
        client,
        f'SELECT "number", "active" FROM "{_DOMAIN}"."incident" WHERE "active" = TRUE LIMIT 5',
    )
    assert 0 < len(rows) <= 5
    assert all(r["number"].startswith("INC") and r["active"] for r in rows)


async def _incidents_named(client: AsyncClient, name: str) -> list[dict[str, Any]]:
    return await query_semantic(
        client,
        f'SELECT "sys_id", "short_description" FROM "{_DOMAIN}"."incident" '
        f"WHERE \"short_description\" LIKE '{name}%'",
    )


async def test_an_unbound_tables_insert_update_and_delete_are_written_to_it(client, registered):
    del registered
    name = _own_name()
    insert = await _mutation(client, "insert", "incident")
    update = await _mutation(client, "update", "incident")
    delete = await _mutation(client, "delete", "incident")
    try:
        written = await _data(
            client,
            f'mutation {{ {insert}(input: {{shortDescription: "{name}"}}) {{ affected_rows }} }}',
        )
        assert written[insert]["affected_rows"] == 1
        assert [r["short_description"] for r in await _incidents_named(client, name)] == [name]
        await _data(
            client,
            f'mutation {{ {update}(where: {{shortDescription: {{eq: "{name}"}}}}, '
            f'set: {{shortDescription: "{name} changed"}}) {{ affected_rows }} }}',
        )
        assert [r["short_description"] for r in await _incidents_named(client, name)] == [
            f"{name} changed"
        ]
    finally:
        await _data(
            client,
            f'mutation {{ {delete}(where: {{shortDescription: {{like: "{name}%"}}}}) '
            "{ affected_rows } }",
        )
        none_remain(f"incident {name}", await _incidents_named(client, name))


async def test_a_bound_tables_insert_goes_through_its_import_set(client, registered, staging):
    del registered
    name = _own_name()
    insert = await _mutation(client, "insert", "sys_user")
    try:
        written = await _data(
            client,
            f'mutation {{ {insert}(input: {{userName: "{name}", firstName: "Provisa", '
            f'email: "{_email(name)}"}}) {{ affected_rows }} }}',
        )
        assert written[insert]["affected_rows"] == 1
        # The record the transform wrote into sys_user.
        (row,) = await _users_named(client, name)
        assert row["sys_id"]
    finally:
        await _remove_user_and_staging(client, staging, name)


async def _users_named(client: AsyncClient, name: str) -> list[dict[str, Any]]:
    return await query_semantic(
        client, f'SELECT "sys_id" FROM "{_DOMAIN}"."sys_user" WHERE "user_name" = \'{name}\''
    )


@pytest_asyncio.fixture(loop_scope="session", scope="module")
async def staging(client, registered) -> dict[str, str]:
    """sys_user's import-set staging table, registered so a test deletes the staging rows its
    insert left (REQ-254): its name, and its columns that carry the user name and the email."""
    import_set = registered
    columns = [
        c["name"]
        for c in (
            await _admin(
                client,
                f'{{ availableColumnsMetadata(sourceId: "{_SOURCE_ID}", schemaName: "{_SCHEMA}", '
                f'tableName: "{import_set}") {{ name }} }}',
            )
        )["availableColumnsMetadata"]
        if not c["name"].startswith("sys_")
    ]
    user_field = next(c for c in columns if "user_name" in c or "user_id" in c)
    email_field = next(c for c in columns if "email" in c)
    await _register(client, import_set, ["sys_id", user_field, email_field])
    await rebuild_schemas(client)
    return {"table": import_set, "user": user_field, "email": email_field}


async def _staged_rows(client: AsyncClient, staging: dict[str, str], name: str) -> list[str]:
    rows = await query_semantic(
        client,
        f'SELECT "sys_id" FROM "{_DOMAIN}"."{staging["table"]}" '
        f"WHERE \"{staging['user']}\" = '{name}' OR \"{staging['email']}\" = '{_email(name)}'",
    )
    return [r["sys_id"] for r in rows]


async def _delete_by_sys_id(client: AsyncClient, table: str, sys_ids: list[str]) -> None:
    delete = await _mutation(client, "delete", table)
    for sys_id in sys_ids:
        await _data(
            client,
            f'mutation {{ {delete}(where: {{sysId: {{eq: "{sys_id}"}}}}) {{ affected_rows }} }}',
        )


async def _remove_user_and_staging(client: AsyncClient, staging: dict[str, str], name: str) -> None:
    """Delete the run's sys_user record and the staging rows its import-set insert left, each by
    the run's name; every delete attempted, then each read back empty (REQ-254)."""

    async def users() -> None:
        await _delete_by_sys_id(
            client, "sys_user", [r["sys_id"] for r in await _users_named(client, name)]
        )
        none_remain(f"sys_user {name}", await _users_named(client, name))

    async def staged() -> None:
        await _delete_by_sys_id(client, staging["table"], await _staged_rows(client, staging, name))
        none_remain(f"{staging['table']} rows of {name}", await _staged_rows(client, staging, name))

    await run_every_async(
        [(f"delete sys_user {name}", users), (f"delete staging rows of {name}", staged)]
    )


# -- the instance's own APIs, as commands ---------------------------------------------------------
#
# The source's companion OpenAPI source (<id>-api) offers the APIs added on its page: here the
# Service Catalog API, and the import set of sys_user's first transform map as a typed insert.

_COMPANION = f"{_SOURCE_ID}-api"


@pytest_asyncio.fixture(loop_scope="session", scope="module")
async def offered(client, registered):
    """The companion's offered operations (name -> "[METHOD] path"), once the Service Catalog API
    and sys_user's import set are added."""
    import_set = registered
    for mutation, field in (
        (
            f'mutation {{ setServicenowApi(sourceId: "{_SOURCE_ID}", namespace: "sn_sc", '
            'name: "Service Catalog API", added: true) { success message } }',
            "setServicenowApi",
        ),
        (
            f'mutation {{ setServicenowImportSet(sourceId: "{_SOURCE_ID}", tableName: "sys_user", '
            f'importSet: "{import_set}", added: true) {{ success message }} }}',
            "setServicenowImportSet",
        ),
    ):
        await _gql(client, mutation, field)
    listed = await _admin(
        client,
        f'{{ availableFunctions(sourceId: "{_COMPANION}", schemaName: "openapi") '
        "{ name comment } }",
    )
    return {f["name"]: f["comment"] for f in listed["availableFunctions"]}


def _operation(offered: dict[str, str], method: str, path: str) -> str:
    """The operation offered as ``method path`` exactly: its comment is that, then its summary."""
    said_as = f"[{method}] {path}"
    (name,) = [
        n for n, said in offered.items() if said == said_as or said.startswith(f"{said_as} — ")
    ]
    return name


def _result(answer: list[dict[str, Any]]) -> Any:
    """What a ServiceNow REST answer holds: the instance wraps each in ``result``, and a command
    answers an object as its one row."""
    (row,) = answer
    return row["result"]


async def _register_command(client: AsyncClient, name: str, operation: str) -> str:
    """Register ``operation`` as a command of the companion; the data GraphQL field it runs as:
    a query field for an operation that reads, a mutation field otherwise."""
    resp = await client.post(
        "/admin/actions/functions",
        json={
            "name": name,
            "sourceId": _COMPANION,
            "schemaName": "openapi",
            "functionName": operation,
            "visibleTo": [_ROLE],
            "domainId": _DOMAIN,
        },
    )
    assert resp.status_code == 200, resp.text
    await rebuild_schemas(client)
    data = await _data(
        client,
        "{ __schema { queryType { fields { name } } mutationType { fields { name } } } }",
    )
    (field,) = [
        f["name"]
        for root in ("queryType", "mutationType")
        for f in data["__schema"][root]["fields"]
        if f["name"].split("__")[-1] == name
    ]
    return field


async def test_an_import_set_row_goes_through_its_transform_map(
    client, registered, offered, staging
):
    """The typed insert's fields are the staging table's columns; the answer is each row's
    transform result. The record the transform wrote and the staging row are deleted by the
    run's name after, however the test ends."""
    import_set = registered
    field = await _register_command(
        client, "snowImportUser", _operation(offered, "POST", f"/api/now/import/{import_set}")
    )
    name = _own_name()
    try:
        answer = await _data(
            client,
            f'mutation {{ {field}(body: {{ {staging["user"]}: "{name}", '
            f'{staging["email"]}: "{_email(name)}" }}) }}',
        )
        rows = _result(answer[field])
        assert rows and rows[0]["status"] in ("inserted", "updated"), rows
        assert rows[0]["table"] == "sys_user"
    finally:
        await _remove_user_and_staging(client, staging, name)


async def _orderable_without_variables(
    client: AsyncClient, detail: str, items: list[dict[str, Any]]
) -> dict[str, Any]:
    """The first catalog item none of whose variables is mandatory: the instance refuses an order
    that leaves one unset ("Mandatory Variables are required", run 39444269086), and which items
    ask for what is the instance's own catalog."""
    for item in items:
        if not item.get("sys_id"):
            continue
        answer = await _data(client, f'query {{ {detail}(sys_id: "{item["sys_id"]}") }}')
        variables = _result(answer[detail]).get("variables") or []
        if not any(v.get("mandatory") for v in variables):
            return item
    raise AssertionError("the instance's catalog has no item that orders without its variables")


async def _remove_order(client: AsyncClient, marker: str, ordered_sys_id: list[str]) -> None:
    """Delete what the run's order created -- its catalog tasks, requested items and the request
    -- found by the request's sys_id and by the run's marker on it; every delete attempted, then
    each read back empty (REQ-254)."""
    requests = list(ordered_sys_id)
    marked = await query_semantic(
        client,
        f'SELECT "sys_id" FROM "{_DOMAIN}"."sc_request" '
        f"WHERE \"special_instructions\" = '{marker}'",
    )
    requests += [r["sys_id"] for r in marked if r["sys_id"] not in requests]

    async def children(table: str) -> list[str]:
        if not requests:
            return []
        listed = ", ".join(f"'{r}'" for r in requests)
        rows = await query_semantic(
            client, f'SELECT "sys_id" FROM "{_DOMAIN}"."{table}" WHERE "request" IN ({listed})'
        )
        return [r["sys_id"] for r in rows]

    async def remove(table: str) -> None:
        await _delete_by_sys_id(client, table, await children(table))
        none_remain(f"{table} rows of the run's order", await children(table))

    async def remove_requests() -> None:
        await _delete_by_sys_id(client, "sc_request", requests)
        if requests:
            listed = ", ".join(f"'{r}'" for r in requests)
            left = await query_semantic(
                client,
                f'SELECT "sys_id" FROM "{_DOMAIN}"."sc_request" WHERE "sys_id" IN ({listed})',
            )
            none_remain("the run's sc_request", left)

    await run_every_async(
        [
            ("delete the order's sc_task rows", lambda: remove("sc_task")),
            ("delete the order's sc_req_item rows", lambda: remove("sc_req_item")),
            ("delete the order's sc_request", remove_requests),
        ]
    )


async def test_a_service_catalog_item_is_ordered_and_its_request_removed(
    client, registered, offered
):
    """A Service Catalog order through the instance's own API: the first item that orders with
    no variables, once. The request carries the run's marker; it, its requested items and their
    tasks are removed through the Table API as registered tables, however the test ends."""
    del registered
    items = await _register_command(
        client, "snowCatalogItems", _operation(offered, "GET", "/api/sn_sc/servicecatalog/items")
    )
    detail = await _register_command(
        client,
        "snowCatalogItem",
        _operation(offered, "GET", "/api/sn_sc/servicecatalog/items/{sys_id}"),
    )
    order = await _register_command(
        client,
        "snowOrderNow",
        _operation(offered, "POST", "/api/sn_sc/servicecatalog/items/{sys_id}/order_now"),
    )
    # Everything the teardown needs is registered before anything is ordered.
    await _register(client, "sc_request", ["sys_id", "number", "special_instructions"])
    await _register(client, "sc_req_item", ["sys_id", "request"])
    await _register(client, "sc_task", ["sys_id", "request"])
    await rebuild_schemas(client)
    update = await _mutation(client, "update", "sc_request")
    listed = await _data(client, f"query {{ {items} }}")
    item = await _orderable_without_variables(client, detail, _result(listed[items]))
    marker = _own_name()
    ordered_sys_id: list[str] = []
    try:
        ordered = await _data(
            client,
            f'mutation {{ {order}(sys_id: "{item["sys_id"]}", body: {{ sysparm_quantity: 1 }}) }}',
        )
        request = _result(ordered[order])
        ordered_sys_id.append(request["sys_id"])
        await _data(
            client,
            f'mutation {{ {update}(where: {{sysId: {{eq: "{request["sys_id"]}"}}}}, '
            f'set: {{specialInstructions: "{marker}"}}) {{ affected_rows }} }}',
        )
        assert request["number"].startswith("REQ")
    finally:
        await _remove_order(client, marker, ordered_sys_id)
