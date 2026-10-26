# Copyright (c) 2026 Kenneth Stott
# Canary: 6b3d9c17-4a82-4e56-9f01-2c7a0d4f8b62
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Shared, surface-agnostic executor for registered tracked functions (REQ-872, REQ-869).

Authorizes by contract (REQ-869) then hands off to the extensible-function dispatcher
(REQ-885), which routes on ``impl_kind`` and emits a non-bypassable invocation trace
(REQ-886). ``source_procedure`` is the original REQ-205–208 stored-procedure path.
"""

from __future__ import annotations

import httpx

from provisa.api.data.action_governance import ActionEnforcement, govern_action_rows
from provisa.api.errors import ApiError
from provisa.core.secrets import resolve_secrets
from provisa.executor.function_dispatch import dispatch_function
from provisa.security.rights import require_role
from provisa.security.mutation_authz import (
    CommandNotFound,
    MutationNotPermitted,
    admit_command as _admit_command,
    command_reachable,
)


def unknown_command(name: str) -> ApiError:
    """The one answer for a command that is not there for the caller: unregistered, not assigned
    to the role, or outside its domains. The same on every surface."""
    return ApiError(404, "functions.unknown_command", f"Unknown command: {name!r}", name=name)


def admit_command(command: dict, state, role_id: str | None, name: str) -> dict:
    """The one command admission (security/mutation_authz.admit_command) rendered as the API's
    answer: not found for a command the role may not use, 403 for a mutation without the write
    right. Returns the acting role."""
    role = state.roles.get(role_id) if role_id is not None else None
    try:
        _admit_command(command, role, name)
    except CommandNotFound as exc:
        raise unknown_command(name) from exc
    except MutationNotPermitted as exc:
        raise ApiError(
            403,
            "authz.mutation_not_permitted",
            str(exc),
            field_name=exc.field_name,
            reason=exc.reason,
        ) from exc
    assert role is not None  # admit_command refuses a call without one
    return role


def _record_call(name: str, args: dict, state, role_id: str) -> None:
    """The record notes the call: the command, the role, and the NAMES of its arguments (never
    their values)."""
    import time

    from provisa.audit.context import current_audit_identity
    from provisa.audit.pipeline import PendingAudit, enqueue_audit

    identity = current_audit_identity()
    if identity is None:
        return
    enqueue_audit(
        PendingAudit(
            user_id=identity.user_id,
            surface=identity.surface,
            role_id=role_id,
            query_text=f"CALL {name}({', '.join(args)})",
            table_ids=[],
            started=time.monotonic(),
            model_stamp=state.model_stamp,
            enforced={"command": {"name": name, "arguments": list(args)}},
        ),
        200,
        state,
        route="command",
    )


async def _run_recorded(fn: dict, args: dict, state, role_id: str) -> list[dict]:  # REQ-2001
    """A statement command's run and its provenance record: enough to prove what produced the
    rows -- the statement as registered, its hash and contract version, the bound argument
    values, the models it embedded with (with their versions), the caller, its start and end,
    its outcome, the tables it read, the table it wrote and the rows affected. Written once the
    run ends, however it ends; append-only, as every audit row. No copy of the data is kept."""
    import time

    from provisa.audit.context import current_audit_identity
    from provisa.audit.pipeline import PendingAudit, enqueue_audit
    from provisa.executor.function_dispatch import _bind_arg_names
    from provisa.executor.statement_command import RUN_FACTS

    identity = current_audit_identity()
    contract = fn["contract"]
    values = _bind_arg_names(fn, args)
    started = time.monotonic()
    token = RUN_FACTS.set(None)
    status = 500
    try:
        rows = await dispatch_function(fn, args, state, role_id)
        status = 200
        return rows
    except ApiError as exc:
        status = exc.status_code
        raise
    finally:
        facts = RUN_FACTS.get()
        RUN_FACTS.reset(token)
        if identity is not None:
            enqueue_audit(
                PendingAudit(
                    user_id=identity.user_id,
                    surface=identity.surface,
                    role_id=role_id,
                    query_text=fn["binding"]["sql"],
                    table_ids=[],
                    started=started,
                    model_stamp=state.model_stamp,
                    enforced={
                        "command": {
                            "name": fn["name"],
                            "kind": "statement",
                            "statement_hash": contract["statement_hash"],
                            "contract_version": contract["version"],
                            "arguments": {k: _recorded(v) for k, v in values.items()},
                            "reads": list(contract["reads"]),
                            "writes": contract["writes"],
                            "embed": facts["embed"] if facts is not None else [],
                        }
                    },
                ),
                status,
                state,
                route="command",
                row_count=facts["rows"] if facts is not None else None,
            )


def _recorded(value):
    """An argument value as the record holds it: JSON-safe, as given."""
    import json

    try:
        json.dumps(value)
    except TypeError:
        return str(value)
    return value


def usable_commands(
    state, role_id: str, *, webhooks: bool = True, language: str = "sql"
) -> dict[str, dict]:
    """The commands ``role_id`` may call, by their command binding in ``language`` (REQ-2168):
    assigned to it, in a domain it reaches, and offered in the language. A surface that
    recognizes a command by its name in the statement (SQL, Cypher) recognizes only these, so a
    command the role may not use reads exactly like a name that was never registered."""
    from provisa.core.command_bindings import bound
    from provisa.security.mutation_authz import command_reachable

    role = (getattr(state, "roles", None) or {}).get(role_id)
    if role is None:
        return {}
    pools = [getattr(state, "tracked_functions", None) or {}]
    if webhooks:
        pools.append(getattr(state, "tracked_webhooks", None) or {})
    return bound(
        (
            command
            for pool in pools
            for command in pool.values()
            if command_reachable(command, role)
        ),
        language,
    )


def command_named(state, role_id: str | None, language: str, name: str) -> str:
    """The command ``name`` calls in ``language`` for ``role_id`` -- its registered name -- or the
    one unknown-command answer when no command the role may use is bound to it there."""
    if role_id is None:
        raise unknown_command(name)
    command = usable_commands(state, role_id, language=language).get(name)
    if command is None:
        raise unknown_command(name)
    return command["name"]


def list_visible_commands(state, role_id: str | None, language: str) -> list[dict]:
    """Every registered command visible to ``role_id``, as ordered metadata dicts (REQ-1156).

    The one discovery path every surface (MCP, Arrow Flight, gRPC, Cypher/Bolt) projects, so a
    command registered once is listable on all of them — not only invocable. ``visible_to``
    filtering matches the REST/OpenAPI surface exactly (openapi_spec.py): an empty ``visible_to``
    means visible to every role. Each entry carries the two orthogonal dimensions the req names:
    ``kind`` (query vs mutation) and ``set_returning`` (``return_schema`` or ``returns =
    "schema.table"`` -> table-valued). Aliased duplicates (the domain-prefixed keys added in
    app_loaders) collapse to one entry per command name.

    ``role_id`` None means the broadest, role-agnostic catalog view (every command), matching the
    Flight table catalog; a concrete role filters by ``visible_to`` (empty ``visible_to`` = every
    role), matching the REST/OpenAPI surface.
    """
    from provisa.core.command_bindings import binding_name

    out: list[dict] = []
    seen: set[str] = set()
    # Functions AND webhooks are both governed commands (REQ-872): a webhook is a scalar-argument
    # HTTP mutation. Both project into the one catalog every surface reads, so a webhook is
    # discoverable (and invocable) on SQL/Cypher/gRPC/REST/MCP, not only as a GraphQL mutation.
    for fn in [
        *(getattr(state, "tracked_functions", {}) or {}).values(),
        *(getattr(state, "tracked_webhooks", {}) or {}).values(),
    ]:
        name = fn.get("name")
        if not name or name in seen:
            continue
        if role_id is not None and not command_reachable(fn, require_role(state.roles, role_id)):
            continue
        seen.add(name)
        # REQ-2168: listed by its command binding in the surface's language; one excluded from
        # the language is not listed there.
        called = binding_name(fn, language)
        if called is None:
            continue
        out.append(
            {
                "name": called,
                "command": name,
                "domain": fn.get("domain_id", "") or "",
                "kind": fn.get("kind", "mutation"),
                "set_returning": bool(
                    fn.get("return_schema") or fn.get("returns") or fn.get("inline_return_type")
                ),
                "arguments": [
                    {"name": a.get("name"), "type": a.get("type", "String")}
                    for a in (fn.get("arguments") or [])
                    if a.get("name")
                ],
                "description": fn.get("description", "") or "",
            }
        )
    return sorted(out, key=lambda c: (c["domain"], c["name"]))


def _admitted(name: str, state, role_id: str | None) -> dict:
    command = (getattr(state, "tracked_functions", None) or {}).get(name) or (
        getattr(state, "tracked_webhooks", None) or {}
    ).get(name)
    if command is None:
        raise unknown_command(name)
    admit_command(command, state, role_id, name)
    return command


def _signature(name: str, declared: list[dict]) -> str:
    args = ", ".join(f"{a['name']} :: {str(a.get('type', 'String')).upper()}" for a in declared)
    return f"{name}({args})"


def bind_named_args(name: str, given: dict, state, role_id: str | None) -> dict:
    """Named argument values (GraphQL, gRPC) in ``name``'s declared order — the order a
    positional call binds them. Admitted first (nothing told of a command the caller may not
    use); then an argument not given, or one the command does not declare, is refused by name."""
    command = _admitted(name, state, role_id)
    declared = [a for a in command.get("arguments") or [] if a.get("name")]
    names = [a["name"] for a in declared]
    signature = _signature(name, declared)
    unknown = [k for k in given if k not in names]
    if unknown:
        raise ApiError(
            400,
            "functions.argument_not_declared",
            f"{signature}: argument {unknown[0]!r} is not one it declares",
            signature=signature,
            argument=unknown[0],
        )
    missing = [n for n in names if n not in given]
    if missing:
        raise ApiError(
            400,
            "functions.argument_missing",
            f"{signature}: argument {missing[0]!r} was not given",
            signature=signature,
            argument=missing[0],
        )
    return {n: given[n] for n in names}


def bind_command_args(name: str, values: list, state, role_id: str | None) -> dict:
    """Positional argument ``values`` bound to ``name``'s declared arguments, for a surface that
    passes them by position (Cypher CALL). The command is admitted first, so a caller learns
    nothing of a command it may not use; then a count that does not match its signature is
    refused, naming the command and the signature."""
    command = _admitted(name, state, role_id)
    declared = [a for a in command.get("arguments") or [] if a.get("name")]
    if len(values) != len(declared):
        raise ApiError(
            400,
            "functions.argument_count",
            f"{_signature(name, declared)} takes {len(declared)} argument(s); {len(values)} given",
            signature=_signature(name, declared),
            count=len(declared),
            given=len(values),
        )
    return {a["name"]: v for a, v in zip(declared, values, strict=True)}


async def invoke_tracked_function(name: str, args: dict, state, role_id: str | None) -> list[dict]:
    """The one path every surface routes through to invoke a registered function.

    Every surface (GraphQL, SQL, pgwire, Cypher, Bolt, gRPC, MCP, REST): the one command
    admission (:func:`admit_command`), approval where declared, the call recorded, then dispatch
    by implementation kind (REQ-885) with a mandatory invocation trace (REQ-886); the after-write
    step when it declares ``writes_table``; the returned rows as the role may see them. ``args``
    is an ordered dict of argument values.
    """
    rows, _enforcement = await invoke_command(name, args, state, role_id)
    return rows


async def invoke_command(
    name: str, args: dict, state, role_id: str | None
) -> tuple[list[dict], ActionEnforcement | None]:
    """:func:`invoke_tracked_function`, also answering what governance applied to the rows
    (``None`` when the command declares no column contract). The admin test call reports it."""
    fn = state.tracked_functions.get(name)
    if fn:
        role = admit_command(fn, state, role_id, name)
        assert role_id is not None
        if fn.get("requires_approval"):
            await _require_approval(fn, args, state, role_id, role)
        if fn.get("impl_kind") == "statement":
            rows = await _run_recorded(fn, args, state, role_id)  # REQ-2001
            return await govern_action_rows(rows, fn, role_id, state)
        _record_call(name, args, state, role_id)
        rows = await dispatch_function(fn, args, state, role_id)
        # A statement command's write passed the pipeline's own write terminal, which already
        # did what follows a write to its table (REQ-2001).
        if fn.get("writes_table") and fn.get("impl_kind") != "statement":
            await _table_was_written(fn, state)
        return await govern_action_rows(rows, fn, role_id, state)
    # A webhook is a governed command too (REQ-872): every surface routes here, so a webhook is
    # invocable beyond GraphQL. Kept a distinct path because a webhook is a scalar-argument HTTP
    # POST — the function dispatcher rejects scalar-only external calls (they can't batch).
    if name in (getattr(state, "tracked_webhooks", None) or {}):
        return await _invoke_webhook(name, args, state, role_id)
    reason = state.undefined_commands.get(name)
    if reason is not None:
        # REQ-1942: a command of a source bound to a synthetic store is not defined in a Test (synthetic)
        # environment; the call is refused saying why, on every surface.
        raise ApiError(409, "functions.not_defined_synthetic", reason, name=name)
    raise unknown_command(name)


def _webhook_body(wh: dict, args: dict) -> dict:
    """Map args to the webhook's declared argument names → the JSON request body.

    The SQL surfaces pass positional args (``a0``, ``a1`` …); the GraphQL path passes them keyed by
    name. Bind positional args to the declared names so the body is correct on every surface.
    """
    declared = [a["name"] for a in wh.get("arguments") or [] if a.get("name")]
    keys = list(args)
    if declared and keys and all(k == f"a{i}" for i, k in enumerate(keys)):
        return {declared[i]: v for i, v in enumerate(args.values()) if i < len(declared)}
    return dict(args)


async def invoke_tracked_webhook(name: str, args: dict, state, role_id: str | None) -> list[dict]:
    """Invoke a registered webhook (a governed HTTP-POST mutation) — the one shared webhook path.

    The same admission and record as a function, then POSTs the argument body to the webhook's
    URL and normalizes the response to a list of row dicts; the rows as the role may see them.
    """
    rows, _enforcement = await _invoke_webhook(name, args, state, role_id)
    return rows


async def _invoke_webhook(
    name: str, args: dict, state, role_id: str | None
) -> tuple[list[dict], ActionEnforcement | None]:
    wh = (getattr(state, "tracked_webhooks", None) or {}).get(name)
    if not wh:
        raise unknown_command(name)
    role = admit_command(wh, state, role_id, name)
    assert role_id is not None
    if wh.get("requires_approval"):
        # REQ-209: approved before the webhook is called, exactly as a command is (REQ-1924).
        await _require_approval(wh, args, state, role_id, role)
    _record_call(name, args, state, role_id)
    timeout = wh["timeout_ms"] / 1000
    async with httpx.AsyncClient(timeout=timeout) as client:
        # The URL is kept as written; an environment or secret reference in it is resolved here.
        url = resolve_secrets(wh["url"])
        resp = await client.request(wh["method"].upper(), url, json=_webhook_body(wh, args))
    body = resp.json()
    rows = body if isinstance(body, list) else [body]
    return await govern_action_rows(rows, wh, role_id, state)


async def _table_was_written(fn: dict, state) -> None:
    """REQ-1924, REQ-871: the command wrote the table it was registered as writing; what follows
    any write to that table follows this one."""
    from provisa.api.data.table_written import after_table_written
    from provisa.executor.source_operation import written_table

    table = written_table(state, fn["source_id"], fn["writes_table"])
    if table is None:
        raise ApiError(
            500,
            "functions.written_table_missing",
            f"command {fn['name']!r} writes {fn['writes_table']!r}, which is not registered",
            name=fn["name"],
            table=fn["writes_table"],
        )
    await after_table_written(
        state,
        table_id=table["id"],
        table_name=table["table_name"],
        source_id=fn["source_id"],
    )


async def _require_approval(fn: dict, args: dict, state, role_id: str | None, role) -> None:
    """REQ-1924: a command that needs approval runs only when the deployment's approval hook
    (REQ-203) approves the call -- who calls it, as which role, with what arguments. A
    deployment with no hook cannot approve one, so the call is refused."""
    from provisa.auth.approval_hook import ApprovalRequest
    from provisa.core.request_context import session_vars_for

    hook = getattr(state, "approval_hook", None)
    if hook is None:
        raise ApiError(
            403,
            "functions.approval_unavailable",
            f"command {fn['name']!r} needs approval and no approval hook is configured",
            name=fn["name"],
        )
    verdict = await hook.evaluate(
        ApprovalRequest(
            user=role_id or "",
            roles=[role_id] if role_id else [],
            tables=[],
            columns=[],
            operation="command",
            session_vars=session_vars_for(role),
            command=fn["name"],
            arguments=args,
        )
    )
    if not verdict.approved:
        raise ApiError(
            403,
            "functions.approval_denied",
            f"Approval denied for {fn['name']!r}: {verdict.reason}",
            name=fn["name"],
            reason=str(verdict.reason),
        )
