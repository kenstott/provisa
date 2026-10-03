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

_PROC_RE = _re.compile(
    r"^\s*CALL\s+(db\.labels|db\.relationshipTypes|db\.propertyKeys)\s*\(\s*\)\s*$", _re.IGNORECASE
)

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


def _detect_procedure(query: str) -> str | None:
    m = _PROC_RE.match(query.strip())
    return m.group(1).lower() if m else None


def _handle_procedure(proc: str, label_map) -> JSONResponse:
    """Return schema-inspection results for Neo4j-compatible CALL procedures."""
    if proc == "db.labels":
        all_labels: set[str] = set()
        for nm in label_map.nodes.values():
            if nm.domain_label:
                all_labels.add(nm.domain_label)
            all_labels.add(nm.table_label)
        rows = [{"label": lbl} for lbl in sorted(all_labels)]
        return JSONResponse(content={"columns": ["label"], "rows": rows})
    if proc == "db.relationshiptypes":
        rows = [
            {"relationshipType": r.rel_type}
            for r in sorted(label_map.relationships.values(), key=lambda x: x.rel_type)
        ]
        return JSONResponse(content={"columns": ["relationshipType"], "rows": rows})
    # proc == "db.propertykeys"
    keys: set[str] = set()
    for nm in label_map.nodes.values():
        keys.update(nm.properties.keys())
    rows = [{"propertyKey": k} for k in sorted(keys)]
    return JSONResponse(content={"columns": ["propertyKey"], "rows": rows})


async def intercept_precompile(body, state, role_id, label_map) -> JSONResponse | None:
    """Pre-parse dispatch: Neo4j schema procedures then REQ-872 registered-function CALLs.

    Returns a response when the query is one of these, else None (fall through to compile).
    """
    if _COMMANDS_PROC_RE.match(body.query.strip()):
        from provisa.api.data.action_exec import list_visible_commands

        cmds = list_visible_commands(state, role_id)
        rows = [
            {"name": c["name"], "description": c["description"], "signature": _command_signature(c)}
            for c in cmds
        ]
        return JSONResponse(content={"columns": ["name", "description", "signature"], "rows": rows})
    proc = _detect_procedure(body.query)
    if proc is not None:
        return _handle_procedure(proc, label_map)
    from provisa.api.errors import ApiError
    from provisa.cypher.command_call import CommandCallRefused, parse_command_call, project

    try:
        call = parse_command_call(body.query, body.params or {})
    except CommandCallRefused as exc:
        raise ApiError(400, "cypher.command_call_refused", str(exc), reason=str(exc)) from exc
    if call is None:
        return None
    from provisa.api.data.action_exec import bind_command_args, invoke_tracked_function

    args = bind_command_args(call.name, call.values, state, role_id)
    rows = await invoke_tracked_function(call.name, args, state, role_id)
    try:
        cols, shaped = project(call, rows)
    except CommandCallRefused as exc:
        raise ApiError(400, "cypher.command_call_refused", str(exc), reason=str(exc)) from exc
    return JSONResponse(content={"columns": cols, "rows": shaped})
