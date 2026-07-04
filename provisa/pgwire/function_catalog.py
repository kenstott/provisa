# Copyright (c) 2026 Kenneth Stott
# Canary: 7f2c9a04-3b81-4d67-9e52-1a6c8d0f4b73
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Projection of registered tracked_functions into the pgwire SQL catalog (REQ-872).

The tracked_functions registry is the single source of truth; this projects it into
the pgwire emulation's pg_proc and information_schema.routines/parameters so a SQL
client (psql \\df, DBeaver, Explore) can DISCOVER registered functions. Invocation and
per-mutation writable_by authorization are enforced by the shared executor at call time,
not by catalog visibility.
"""

from __future__ import annotations

_FN_TYPE_TO_SQL = {
    "text": "character varying",
    "string": "character varying",
    "varchar": "character varying",
    "integer": "integer",
    "int": "integer",
    "bigint": "bigint",
    "float": "double precision",
    "number": "double precision",
    "numeric": "numeric",
    "boolean": "boolean",
    "jsonb": "jsonb",
    "json": "json",
    "uuid": "uuid",
    "timestamp": "timestamp without time zone",
    "date": "date",
}

_PUBLIC_NS_OID = 2200  # pg_namespace oid of the public schema
_FN_OID_BASE = 40000  # starting synthetic oid for projected functions


def _fn_sql_type(provisa_type: str | None) -> str:
    return _FN_TYPE_TO_SQL.get((provisa_type or "").lower(), "character varying")


def _fn_visible(fn: dict, role_id: str) -> bool:
    """A tracked function is discoverable when unrestricted or the role is granted visibility."""
    visible_to = fn.get("visible_to") or []
    return not visible_to or role_id in visible_to


def _dedupe_functions(fns: dict) -> list[dict]:
    """One entry per underlying function (the registry keys it by bare name + prefixed alias)."""
    seen: set = set()
    unique: list[dict] = []
    for fn in fns.values():
        key = fn.get("id") if fn.get("id") is not None else fn.get("name")
        if key in seen:
            continue
        seen.add(key)
        unique.append(fn)
    return unique


def populate_functions(db, state, role_id: str) -> None:  # REQ-872
    """Project role-visible tracked_functions into pg_proc / routines / parameters."""
    fns = getattr(state, "tracked_functions", None)
    if not isinstance(fns, dict):
        return
    oid = _FN_OID_BASE
    for fn in sorted(_dedupe_functions(fns), key=lambda f: f.get("name") or ""):
        if not _fn_visible(fn, role_id):
            continue
        name = fn.get("name") or ""
        args = fn.get("arguments") or []
        set_returns = bool(fn.get("return_schema"))
        ret_sql = "record" if set_returns else _fn_sql_type(fn.get("returns"))
        specific = f"{name}_{oid}"

        db.execute(
            "INSERT INTO _pg_proc (oid, proname, pronamespace, proowner, prokind, "
            "prosecdef, proleakproof, proisstrict, proretset, provolatile, proparallel, "
            "pronargs, pronargdefaults, prorettype, prosrc) "
            "VALUES (?, ?, ?, 10, 'f', false, false, false, ?, 'v', 'u', ?, 0, 0, ?)",
            [oid, name, _PUBLIC_NS_OID, set_returns, len(args), name],
        )
        db.execute(
            "INSERT INTO _is_routines (specific_catalog, specific_schema, specific_name, "
            "routine_catalog, routine_schema, routine_name, routine_type, data_type, "
            "routine_body, external_language, is_deterministic, sql_data_access, security_type) "
            "VALUES ('provisa', 'public', ?, 'provisa', 'public', ?, 'FUNCTION', ?, "
            "'EXTERNAL', 'EXTERNAL', 'NO', 'MODIFIES', 'INVOKER')",
            [specific, name, ret_sql],
        )
        for pos, arg in enumerate(args, 1):
            db.execute(
                "INSERT INTO _is_parameters (specific_catalog, specific_schema, specific_name, "
                "ordinal_position, parameter_mode, is_result, parameter_name, data_type) "
                "VALUES ('provisa', 'public', ?, ?, 'IN', 'NO', ?, ?)",
                [specific, pos, arg.get("name"), _fn_sql_type(arg.get("type"))],
            )
        oid += 1
