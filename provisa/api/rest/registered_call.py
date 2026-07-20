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

Detects a registered tracked-function CALL, coerces its positional arguments, routes to
the one shared ``invoke_tracked_function`` executor (which enforces per-mutation
writable_by), and projects YIELD columns. Kept out of cypher_router so that surface stays
within its size/complexity budget.
"""

from __future__ import annotations

import re as _re

from fastapi.responses import JSONResponse

_MIN_QUOTED_LEN = 2  # a quoted literal needs at least the two surrounding quote chars
_YIELD_ALIAS_TOKENS = 3  # "col AS alias" splits into three tokens

_REGISTERED_CALL_RE = _re.compile(
    r"^\s*CALL\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\((.*?)\)"
    r"\s*(?:YIELD\s+(.+?))?\s*(?:RETURN\s+.+)?\s*$",
    _re.IGNORECASE | _re.DOTALL,
)

_PROC_RE = _re.compile(
    r"^\s*CALL\s+(db\.labels|db\.relationshipTypes|db\.propertyKeys)\s*\(\s*\)\s*$", _re.IGNORECASE
)

# REQ-1150: a command-listing procedure so an HTTP Cypher client discovers registered commands
# (name/signature) — parity with the Bolt SHOW PROCEDURES surface. dbms.procedures is the Neo4j name.
_COMMANDS_PROC_RE = _re.compile(
    r"^\s*CALL\s+(?:dbms\.procedures|provisa\.commands)\s*\(\s*\)\s*(?:YIELD\b.*)?$", _re.IGNORECASE
)


def _command_signature(cmd: dict) -> str:
    args = ", ".join(f"{a['name']} :: {str(a.get('type', 'String')).upper()}" for a in cmd["arguments"])
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


def _split_call_args(args_str: str) -> list[str]:
    """Split a CALL argument list on top-level commas, respecting quotes."""
    out: list[str] = []
    buf: list[str] = []
    quote: str | None = None
    for ch in args_str:
        if quote:
            buf.append(ch)
            if ch == quote:
                quote = None
        elif ch in "'\"":
            quote = ch
            buf.append(ch)
        elif ch == ",":
            out.append("".join(buf))
            buf = []
        else:
            buf.append(ch)
    if "".join(buf).strip():
        out.append("".join(buf))
    return out


def _parse_call_literal(raw: str, params: dict):
    """Coerce one CALL argument token to a value ($param, string, number, bool, null)."""
    raw = raw.strip()
    if raw.startswith("$"):
        return params.get(raw[1:])
    if len(raw) >= _MIN_QUOTED_LEN and raw[0] in "'\"" and raw[-1] == raw[0]:
        return raw[1:-1]
    low = raw.lower()
    if low in ("true", "false"):
        return low == "true"
    if low in ("null", "none"):
        return None
    try:
        return int(raw)
    except ValueError:
        pass
    try:
        return float(raw)
    except ValueError:
        return raw


def detect_registered_call(
    query: str, state, params: dict
) -> tuple[str, dict, list[tuple[str, str]]] | None:
    """Detect ``CALL <registeredFn>(args) [YIELD cols]`` (REQ-872).

    Returns (function name, ordered positional args dict, YIELD (source, alias) pairs) when the
    name is a registered tracked function, else None. YIELD is optional; ``col AS alias`` supported.
    """
    fns = getattr(state, "tracked_functions", None)
    if not isinstance(fns, dict):
        return None
    m = _REGISTERED_CALL_RE.match(query.strip())
    if m is None:
        return None
    name = m.group(1)
    if name not in fns:
        return None
    args: dict = {}
    for i, tok in enumerate(_split_call_args(m.group(2) or "")):
        if tok.strip():
            args[f"a{i}"] = _parse_call_literal(tok, params)
    yields: list[tuple[str, str]] = []
    if m.group(3):
        for part in m.group(3).split(","):
            seg = part.strip().split()
            if not seg:
                continue
            src = seg[0]
            alias = seg[2] if len(seg) >= _YIELD_ALIAS_TOKENS and seg[1].lower() == "as" else src
            yields.append((src, alias))
    return name, args, yields


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
    reg = detect_registered_call(body.query, state, body.params)
    if reg is not None:
        return await handle_registered_call(reg[0], reg[1], reg[2], state, role_id)
    return None


async def handle_registered_call(name, args, yields, state, role_id) -> JSONResponse:  # REQ-872
    """Invoke a registered function via the shared executor and project YIELD columns."""
    from provisa.api.data.action_exec import invoke_tracked_function

    rows = await invoke_tracked_function(name, args, state, role_id)
    if yields:
        cols = [alias for _src, alias in yields]
        rows = [{alias: r.get(src) for src, alias in yields} for r in rows]
    else:
        cols = list(rows[0].keys()) if rows else []
    return JSONResponse(content={"columns": cols, "rows": rows})
