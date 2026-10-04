# Copyright (c) 2026 Kenneth Stott
# Canary: a1b2c3d4-e5f6-7890-abcd-ef1234567890
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Admin REST endpoints for tracked DB functions and webhooks (REQ-205-211)."""

# Requirements: REQ-004, REQ-062, REQ-205, REQ-206, REQ-207, REQ-208, REQ-209, REQ-210, REQ-211, REQ-245, REQ-253, REQ-304, REQ-305, REQ-306, REQ-434

from __future__ import annotations

import logging

from fastapi import APIRouter, Request
from pydantic import BaseModel
from sqlalchemy import func, select, update

from provisa.api.errors import ApiError
from provisa.core.schema_org import tracked_functions, tracked_webhooks
from provisa.api.admin.capabilities import require_capability_request

log = logging.getLogger(__name__)
router = APIRouter(prefix="/admin/actions", tags=["admin", "actions"])


def _args_to_ui(raw: list[dict] | None) -> list[dict]:
    """Project stored function arguments to the UI shape (REQ-885: argKind; REQ-1159: dataset columns)."""
    out = []
    for a in raw or []:
        arg = {"name": a["name"], "type": a["type"], "argKind": a.get("arg_kind", "column_value")}
        if a.get("columns"):  # REQ-1159: per-dataset input column contract
            arg["columns"] = a["columns"]
        out.append(arg)
    return out


def _args_from_ui(raw: list[dict]) -> list[dict]:
    """Normalize UI arguments to the model shape (argKind → arg_kind), for persistence."""
    out = []
    for a in raw:
        norm = {
            "name": a["name"],
            "type": a["type"],
            "arg_kind": a.get("argKind") or a.get("arg_kind") or "column_value",
        }
        cols = a.get("columns")  # REQ-1159: per-dataset input column contract [{name,type}]
        if cols:
            norm["columns"] = cols
        out.append(norm)
    return out


def _row_to_function(row: dict) -> dict:
    return {
        "name": row["name"],
        "sourceId": row["source_id"],
        "schemaName": row["schema_name"],
        "functionName": row["function_name"],
        "returns": row["returns"],
        "arguments": _args_to_ui(row["arguments"]),
        "visibleTo": list(row["visible_to"] or []),
        "domainId": row["domain_id"],
        "description": row.get("description"),
        "kind": row.get("kind", "mutation"),
        "productId": row.get("product_id"),  # REQ-1634
        "returnSchema": row.get("return_schema"),
        "outputColumns": row.get("output_columns"),  # REQ-1159: IR-typed output dataset contract
        # REQ-885: implementation kind + swappable binding, decoupled from addressing.
        "implKind": row.get("impl_kind", "source_procedure"),
        "binding": row.get("binding") or {},
        "materialize": bool(row.get("materialize", False)),
        "requiresApproval": bool(row.get("requires_approval", False)),  # REQ-1924
        "writesTable": row.get("writes_table"),  # REQ-1924, REQ-871
    }


def _row_to_webhook(row: dict) -> dict:
    return {
        "name": row["name"],
        "url": row["url"],
        "method": row["method"],
        "timeoutMs": row["timeout_ms"],
        "returns": row.get("returns"),
        "inlineReturnType": row["inline_return_type"] or [],
        "arguments": row["arguments"] or [],
        "visibleTo": list(row["visible_to"] or []),
        "domainId": row["domain_id"],
        "description": row.get("description"),
        "kind": row.get("kind", "mutation"),
    }


@router.get("")
async def list_actions(request: Request):  # REQ-205, REQ-209
    """Return all tracked functions and webhooks."""
    require_capability_request(request, "table_registration")
    from provisa.api.app import state

    if state.model_db is None:
        raise ApiError(503, "actions.database_not_connected", "Database not connected")

    from provisa.core.repositories import creation_request as cr_repo

    async with state.model_db.acquire() as conn:
        fn_result = await conn.execute_core(
            select(tracked_functions).order_by(tracked_functions.c.name)
        )
        fn_rows = fn_result.fetchall()
        wh_result = await conn.execute_core(
            select(tracked_webhooks).order_by(tracked_webhooks.c.name)
        )
        wh_rows = wh_result.fetchall()

        # REQ-209: a webhook reaches GraphQL only once its latest creation_request is 'executed'.
        # Surface that gate to the Commands page so a registered-but-unapproved webhook is not
        # mistaken for live (the schema loader applies the same gate in app_loaders).
        webhooks = []
        for r in wh_rows:
            wh = _row_to_webhook(dict(r._mapping))
            wh["approved"] = await cr_repo.latest_status(conn, "webhook", wh["name"]) == "executed"
            webhooks.append(wh)

    return {
        "functions": [_row_to_function(dict(r._mapping)) for r in fn_rows],
        "webhooks": webhooks,
    }


class FunctionInput(BaseModel):  # REQ-205, REQ-206, REQ-304, REQ-305, REQ-306
    name: str
    sourceId: str = ""
    schemaName: str = "public"
    functionName: str = ""
    returns: str = ""
    arguments: list[dict] = []
    visibleTo: list[str] = []
    domainId: str = ""
    description: str | None = None
    kind: str = "mutation"
    productId: str | None = None  # REQ-1634
    returnSchema: dict | None = None
    # REQ-1159: canonical IR-typed output dataset contract [{name,type}]; returnSchema is its projection.
    outputColumns: list[dict] | None = None
    # REQ-885: implementation-kind dimension + swappable binding + identity model.
    implKind: str = "source_procedure"
    binding: dict = {}
    materialize: bool = False
    requiresApproval: bool = False  # REQ-1924
    writesTable: str | None = None  # REQ-1924, REQ-871: "schema.table" of the table it writes


class WebhookInput(BaseModel):  # REQ-209, REQ-210, REQ-211
    name: str
    url: str = ""
    method: str = "POST"
    timeoutMs: int = 5000
    returns: str | None = None
    inlineReturnType: list[dict] = []
    arguments: list[dict] = []
    visibleTo: list[str] = []
    domainId: str = ""
    description: str | None = None
    kind: str = "mutation"


async def _as_source_operation(body: FunctionInput) -> None:
    """REQ-1924: a command registered on a remote source is one of the source's write
    operations, called as it is. What it is follows from the operation the source offers -- its
    kind, the schema it is registered under, its arguments, each passed through as a JSON value
    -- and not from what the form sent. An operation the source does not offer is refused."""
    from provisa.api.app import state
    from provisa.executor.source_operation import OPERATION_SCHEMA, offered_operation

    source_type = (getattr(state, "source_types", None) or {}).get(body.sourceId, "")
    if source_type not in OPERATION_SCHEMA:
        return
    if source_type == "openapi":
        from provisa.api.admin.schema_helpers import _ensure_openapi_spec

        await _ensure_openapi_spec(body.sourceId)
    operation = await offered_operation(state, body.sourceId, body.functionName)
    body.implKind = "source_operation"
    body.kind = "mutation"
    body.schemaName = OPERATION_SCHEMA[source_type]
    body.returns = ""
    body.binding = {}
    body.materialize = False
    body.arguments = [{"name": a, "type": "json"} for a in operation.arguments]


def _check_written_table(body: FunctionInput) -> None:
    """REQ-1924, REQ-871: the table a command is registered as writing is a registered table of
    the command's own source."""
    from provisa.api.app import state
    from provisa.executor.source_operation import written_table

    if body.writesTable is None:
        return
    if written_table(state, body.sourceId, body.writesTable) is None:
        raise ApiError(
            422,
            "actions.written_table_not_registered",
            f"{body.writesTable!r} is not a registered table of source {body.sourceId!r}",
            source_id=body.sourceId,
            table=body.writesTable,
        )


def _saved_domain(request: Request, name: str, domain_id: str) -> str:  # REQ-1531
    """The domain a command or webhook is being saved into: it must name one, and the caller
    must reach it."""
    from provisa.api.admin.capabilities import require_domain_request
    from provisa.core import domain_policy

    try:
        resolved = domain_policy.command_domain_id(domain_id, name)
    except ValueError as refused:
        raise ApiError(422, "actions.domain_required", str(refused), name=name) from refused
    require_domain_request(request, resolved)
    return resolved


@router.post("/functions")
async def create_function(
    request: Request,
    body: FunctionInput,
):  # REQ-205, REQ-206, REQ-207, REQ-208, REQ-253, REQ-304
    """Create a tracked DB function."""
    require_capability_request(request, "table_registration")
    body.domainId = _saved_domain(request, body.name, body.domainId)
    await _as_source_operation(body)
    _check_written_table(body)
    from provisa.api.app import state
    from provisa.core.models import DatasetColumn, Function, FunctionArgument
    from provisa.core.repositories import function as function_repo

    if state.model_db is None:
        raise ApiError(503, "actions.database_not_connected", "Database not connected")

    func = Function(
        name=body.name,
        source_id=body.sourceId,
        schema_name=body.schemaName,
        function_name=body.functionName,
        returns=body.returns,
        arguments=[FunctionArgument(**a) for a in _args_from_ui(body.arguments)],
        visible_to=body.visibleTo,
        domain_id=body.domainId,
        description=body.description,
        kind=body.kind,
        product_id=body.productId,  # REQ-1634
        impl_kind=body.implKind,
        binding=body.binding,
        materialize=body.materialize,
        requires_approval=body.requiresApproval,  # REQ-1924
        writes_table=body.writesTable,  # REQ-1924, REQ-871
        # REQ-1159: the wire model carries the raw [{name,type}] the UI posts (the update path
        # below writes it straight into a JSON column); the domain model wants the typed contract.
        output_columns=(
            [DatasetColumn(**c) for c in body.outputColumns]
            if body.outputColumns is not None
            else None
        ),
    )
    # return_schema is a JSON column — pass the Python object directly (no double-encoding).
    async with state.model_db.acquire() as _conn:
        await function_repo.upsert_function(
            _conn, func, return_schema=body.returnSchema, origin="admin"
        )

    log.info("Saved tracked function %s", body.name)
    from provisa.api.app import _rebuild_schemas

    await _rebuild_schemas()
    return {"success": True, "name": body.name}


@router.put("/functions/{name}")
async def update_function(
    request: Request, name: str, body: FunctionInput
):  # REQ-205, REQ-253, REQ-304
    """Update a tracked DB function by name."""
    require_capability_request(request, "table_registration")
    body.domainId = _saved_domain(request, name, body.domainId)
    await _as_source_operation(body)
    _check_written_table(body)
    from provisa.api.app import state

    if state.model_db is None:
        raise ApiError(503, "actions.database_not_connected", "Database not connected")

    from provisa.core.repositories import data_product as data_product_repo

    async with state.model_db.acquire() as conn:
        # REQ-1531: moving it needs the domain it is moved out of as well.
        await _require_its_domain(request, conn, tracked_functions, name)
        if body.productId is not None:
            # REQ-1634: same domain-membership gate as function_repo.upsert_function; the
            # update path writes tracked_functions directly and must not bypass it.
            product = await data_product_repo.get(conn, body.productId)
            if product is None:
                raise ApiError(
                    422,
                    "actions.data_product_not_found",
                    f"data product {body.productId!r} does not exist",
                )
            if product["domain_id"] != body.domainId:
                raise ApiError(
                    422,
                    "actions.data_product_domain_mismatch",
                    f"command {name} is in domain {body.domainId!r} but data product "
                    f"{body.productId!r} belongs to domain {product['domain_id']!r}",
                )
        result = await conn.execute_core(
            update(tracked_functions)
            .where(tracked_functions.c.name == name)
            .values(
                source_id=body.sourceId,
                schema_name=body.schemaName,
                function_name=body.functionName,
                returns=body.returns,
                arguments=_args_from_ui(body.arguments),
                visible_to=body.visibleTo,
                domain_id=body.domainId,
                description=body.description,
                kind=body.kind,
                product_id=body.productId,  # REQ-1634
                return_schema=body.returnSchema,
                output_columns=body.outputColumns,  # REQ-1159
                impl_kind=body.implKind,
                binding=body.binding,
                materialize=body.materialize,
                requires_approval=body.requiresApproval,  # REQ-1924
                writes_table=body.writesTable,  # REQ-1924, REQ-871
                updated_at=func.now(),
            )
        )

    if (result.rowcount or 0) == 0:
        raise ApiError(404, "actions.function_not_found", f"Function '{name}' not found", name=name)

    log.info("Updated tracked function %s", name)
    from provisa.api.app import _rebuild_schemas

    await _rebuild_schemas()
    return {"success": True, "name": name}


async def _require_its_domain(request: Request, conn, table, name: str) -> None:  # REQ-1531
    """Gate an act on a command or webhook named by ``name`` on the domain it sits in.

    Every command and webhook is saved into a domain. A stored row that names none predates
    that rule: no role is shown it (``actions_schema``), and there is no domain to hold for it.
    A name with no row is the caller's not-found, answered by the act itself.
    """
    from provisa.api.admin.capabilities import require_domain_request

    row = (
        await conn.execute_core(select(table.c.domain_id).where(table.c.name == name))
    ).fetchone()
    if row is not None and row[0]:
        require_domain_request(request, row[0])


@router.delete("/functions/{name}")
async def delete_function(request: Request, name: str):  # REQ-205, REQ-253
    """Delete a tracked DB function by name."""
    require_capability_request(request, "table_registration")
    from provisa.api.app import state

    if state.model_db is None:
        raise ApiError(503, "actions.database_not_connected", "Database not connected")

    async with state.model_db.acquire() as conn:
        await _require_its_domain(request, conn, tracked_functions, name)
        from provisa.core.repositories import function as function_repo

        deleted = await function_repo.delete_function(conn, name)

    if not deleted:
        raise ApiError(404, "actions.function_not_found", f"Function '{name}' not found", name=name)

    log.info("Deleted tracked function %s", name)
    from provisa.api.app import _rebuild_schemas

    await _rebuild_schemas()
    return {"success": True, "name": name}


@router.post("/webhooks")
async def create_webhook(
    request: Request, body: WebhookInput
):  # REQ-209, REQ-210, REQ-211, REQ-253, REQ-434
    """Create a tracked webhook."""
    require_capability_request(request, "table_registration")
    body.domainId = _saved_domain(request, body.name, body.domainId)
    from provisa.api.app import state

    if state.model_db is None:
        raise ApiError(503, "actions.database_not_connected", "Database not connected")

    from provisa.core.repositories import creation_request as cr_repo

    async with state.model_db.acquire() as conn:
        await conn.upsert(
            tracked_webhooks,
            {
                "origin": "admin",  # REQ-1919: written when the row is created
                "name": body.name,
                "url": body.url,
                "method": body.method,
                "timeout_ms": body.timeoutMs,
                "returns": body.returns,
                "inline_return_type": body.inlineReturnType,
                "arguments": body.arguments,
                "visible_to": body.visibleTo,
                "domain_id": body.domainId,
                "description": body.description,
                "kind": body.kind,
                "updated_at": func.now(),
            },
            index_elements=["name"],
            update_columns=[
                "url",
                "method",
                "timeout_ms",
                "returns",
                "inline_return_type",
                "arguments",
                "visible_to",
                "domain_id",
                "description",
                "kind",
                "updated_at",
            ],
        )
        # REQ-209: a webhook is exposed only after a steward approves it. Approval is tracked
        # via the creation_requests queue — a webhook is approved when its most recent
        # "webhook" request is executed. Registering or editing enqueues a fresh pending
        # request, so any edit resets approval until re-approved.
        request_id = await cr_repo.create(
            conn,
            "webhook",
            "webhook_registration",
            {"name": body.name},
            None,
        )

    log.info("Saved tracked webhook %s (pending approval, request #%s)", body.name, request_id)
    from provisa.api.app import _rebuild_schemas

    await _rebuild_schemas()
    return {
        "success": True,
        "name": body.name,
        "approved": False,
        "creationRequestId": request_id,
        "message": (
            f"Webhook {body.name!r} registered — awaiting a steward holding "
            "'webhook_registration' to approve it before it is exposed."
        ),
    }


@router.put("/webhooks/{name}")
async def update_webhook(request: Request, name: str, body: WebhookInput):  # REQ-209, REQ-253
    """Update a tracked webhook by name."""
    require_capability_request(request, "table_registration")
    body.domainId = _saved_domain(request, name, body.domainId)
    from provisa.api.app import state

    if state.model_db is None:
        raise ApiError(503, "actions.database_not_connected", "Database not connected")

    async with state.model_db.acquire() as conn:
        # REQ-1531: moving it needs the domain it is moved out of as well.
        await _require_its_domain(request, conn, tracked_webhooks, name)
        result = await conn.execute_core(
            update(tracked_webhooks)
            .where(tracked_webhooks.c.name == name)
            .values(
                url=body.url,
                method=body.method,
                timeout_ms=body.timeoutMs,
                returns=body.returns,
                inline_return_type=body.inlineReturnType,
                arguments=body.arguments,
                visible_to=body.visibleTo,
                domain_id=body.domainId,
                description=body.description,
                kind=body.kind,
                updated_at=func.now(),
            )
        )

    if (result.rowcount or 0) == 0:
        raise ApiError(404, "actions.webhook_not_found", f"Webhook '{name}' not found", name=name)

    log.info("Updated tracked webhook %s", name)
    from provisa.api.app import _rebuild_schemas

    await _rebuild_schemas()
    return {"success": True, "name": name}


@router.delete("/webhooks/{name}")
async def delete_webhook(request: Request, name: str):  # REQ-209, REQ-253
    """Delete a tracked webhook by name."""
    require_capability_request(request, "table_registration")
    from provisa.api.app import state

    if state.model_db is None:
        raise ApiError(503, "actions.database_not_connected", "Database not connected")

    async with state.model_db.acquire() as conn:
        await _require_its_domain(request, conn, tracked_webhooks, name)
        from provisa.core.repositories import function as function_repo

        deleted = await function_repo.delete_webhook(conn, name)

    if not deleted:
        raise ApiError(404, "actions.webhook_not_found", f"Webhook '{name}' not found", name=name)

    log.info("Deleted tracked webhook %s", name)
    from provisa.api.app import _rebuild_schemas

    await _rebuild_schemas()
    return {"success": True, "name": name}


class TestActionInput(BaseModel):  # REQ-004, REQ-062, REQ-245
    actionType: str  # "function" or "webhook"
    name: str
    role_id: str | None = None  # REQ-245: governance role selector


def _test_endpoints_enabled() -> bool:
    """REQ-004: developer test endpoints are opt-in and MUST NOT be exposed in production.

    Disabled unless ``PROVISA_ENABLE_TEST_ENDPOINTS`` is explicitly truthy, mirroring the
    opt-in pattern used for other non-production features (e.g. ``allow_simple_auth``).
    """
    import os

    return os.environ.get("PROVISA_ENABLE_TEST_ENDPOINTS", "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


@router.post("/test")
async def test_action(request: Request, body: TestActionInput):  # REQ-004, REQ-062, REQ-245
    """Run a no-arg test invocation of a tracked function or webhook, as a role.

    The role is required and is one the caller holds, or the caller holds ``access_config`` (the
    rule compileQuery follows). The call is then the real one: the same command admission,
    approval, record, dispatch and governed rows every surface gets (``invoke_command``), with
    what governance applied reported beside the rows.
    """
    require_capability_request(request, "table_registration")
    if not _test_endpoints_enabled():
        raise ApiError(
            404,
            "actions.test_endpoint_disabled",
            "Test endpoint is disabled (set PROVISA_ENABLE_TEST_ENDPOINTS to enable in non-production).",
        )

    from provisa.api.app import state

    if state.model_db is None:
        raise ApiError(503, "actions.database_not_connected", "Database not connected")

    if body.actionType not in ("function", "webhook"):
        raise ApiError(
            400,
            "actions.unknown_action_type",
            f"Unknown actionType '{body.actionType}'",
            action_type=body.actionType,
        )
    if not body.role_id:
        raise ApiError(
            422,
            "actions.test_role_required",
            "role_id is required: a test call runs as a role, governed as that role's own call",
            field="role_id",
        )
    from provisa.api.admin.capabilities import require_inspectable_role_request

    require_inspectable_role_request(request, body.role_id)

    registry = state.tracked_functions if body.actionType == "function" else state.tracked_webhooks
    if body.name not in registry:
        kind = "Function" if body.actionType == "function" else "Webhook"
        raise ApiError(
            404,
            f"actions.{body.actionType}_not_found",
            f"{kind} '{body.name}' not found",
            name=body.name,
        )

    from provisa.api.data.action_exec import invoke_command

    rows, enforcement = await invoke_command(body.name, {}, state, body.role_id)
    return {
        "rows": rows,
        "enforcement": (
            enforcement.as_dict()
            if enforcement is not None
            else {
                "role_used": body.role_id,
                "note": "The command declares no output columns; nothing to govern.",
            }
        ),
    }
