# Copyright (c) 2026 Kenneth Stott
# Canary: 3a9d6c17-8b40-4e52-9f61-2c7a0d4f8b95
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Cypher ``CALL <registeredFn>(args) YIELD ...`` binding to the shared executor (REQ-872).

A ``CALL <command>(…)`` is read by the one Cypher command-call reader (cypher/command_call.py,
shared with Bolt), admitted and bound by the shared executor, and shaped by its YIELD and
RETURN. Kept out of cypher_router so that surface stays within its size/complexity budget.
"""

from __future__ import annotations

import re as _re

from fastapi.responses import JSONResponse

# REQ-572: the schema procedures answered from the label map, each with the one column it yields.
_PROCEDURES = {
    "db.labels": "label",
    "db.relationshiptypes": "relationshipType",
    "db.propertykeys": "propertyKey",
}

# REQ-1156: a command-listing procedure so an HTTP Cypher client discovers registered commands
# (name/signature) — parity with the Bolt SHOW PROCEDURES surface. dbms.procedures is the Neo4j name.
_COMMANDS_PROC_RE = _re.compile(
    r"^\s*CALL\s+(?:dbms\.procedures|provisa\.commands)\s*\(\s*\)\s*(?:YIELD\b.*)?$", _re.IGNORECASE
)


def _command_signature(cmd: dict) -> str:
    args = ", ".join(
        f"{a['name']} :: {str(a.get('type', 'String')).upper()}" for a in cmd["arguments"]
    )
    ret = "LIST OF MAP" if cmd["set_returning"] else "MAP"
    return f"{cmd['name']}({args}) :: ({ret})"


def _procedure_call(query: str):
    """The schema procedure ``query`` calls, standing alone and taking no arguments, with its
    YIELD and RETURN (``CALL db.labels() YIELD label RETURN label``); else None."""
    from provisa.cypher.command_call import CommandCallRefused, read_call

    try:
        call = read_call(query, {})
    except CommandCallRefused:
        return None
    if call is None or call.values or call.name.lower() not in _PROCEDURES:
        return None
    return call


def _detect_procedure(query: str) -> str | None:
    call = _procedure_call(query)
    return call.name.lower() if call is not None else None


def _procedure_rows(proc: str, label_map) -> list[dict]:
    """The rows of a Neo4j-compatible schema procedure, from the label map (REQ-572)."""
    if proc == "db.labels":
        all_labels: set[str] = set()
        for nm in label_map.nodes.values():
            if nm.domain_label:
                all_labels.add(nm.domain_label)
            all_labels.add(nm.table_label)
        return [{"label": lbl} for lbl in sorted(all_labels)]
    if proc == "db.relationshiptypes":
        return [
            {"relationshipType": r.rel_type}
            for r in sorted(label_map.relationships.values(), key=lambda x: x.rel_type)
        ]
    # proc == "db.propertykeys"
    keys: set[str] = set()
    for nm in label_map.nodes.values():
        keys.update(nm.properties.keys())
    return [{"propertyKey": k} for k in sorted(keys)]


def _handle_procedure(proc: str, label_map) -> JSONResponse:
    """Return schema-inspection results for Neo4j-compatible CALL procedures."""
    return JSONResponse(
        content={"columns": [_PROCEDURES[proc]], "rows": _procedure_rows(proc, label_map)}
    )


async def intercept_precompile(body, state, role_id, label_map) -> JSONResponse | None:
    """Pre-parse dispatch: Neo4j schema procedures then REQ-872 registered-function CALLs.

    Returns a response when the query is one of these, else None (fall through to compile).
    """
    if _COMMANDS_PROC_RE.match(body.query.strip()):
        from provisa.api.data.action_exec import list_visible_commands

        cmds = list_visible_commands(state, role_id, "cypher")
        rows = [
            {"name": c["name"], "description": c["description"], "signature": _command_signature(c)}
            for c in cmds
        ]
        return JSONResponse(content={"columns": ["name", "description", "signature"], "rows": rows})
    from provisa.api.errors import ApiError
    from provisa.cypher.command_call import CommandCallRefused, parse_command_call, project

    procedure = _procedure_call(body.query)
    if procedure is not None:
        proc = procedure.name.lower()
        try:
            cols, shaped = project(
                procedure, _procedure_rows(proc, label_map), columns=[_PROCEDURES[proc]]
            )
        except CommandCallRefused as exc:
            raise ApiError(400, "cypher.command_call_refused", str(exc), reason=str(exc)) from exc
        return JSONResponse(content={"columns": cols, "rows": shaped})

    try:
        call = parse_command_call(body.query, body.params or {})
    except CommandCallRefused as exc:
        raise ApiError(400, "cypher.command_call_refused", str(exc), reason=str(exc)) from exc
    if call is None:
        return None
    from provisa.api.data.action_exec import (
        bind_command_args,
        command_named,
        invoke_tracked_function,
    )

    # REQ-2168: the name is the command's Cypher command binding.
    name = command_named(state, role_id, "cypher", call.name)
    args = bind_command_args(name, call.values, state, role_id)
    rows = await invoke_tracked_function(name, args, state, role_id)
    try:
        cols, shaped = project(call, rows)
    except CommandCallRefused as exc:
        raise ApiError(400, "cypher.command_call_refused", str(exc), reason=str(exc)) from exc
    return JSONResponse(content={"columns": cols, "rows": shaped})
