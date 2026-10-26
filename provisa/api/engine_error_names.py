# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a client is told of an error: redacted, and in the model's names (REQ-2087, REQ-2152).

The engine runs a statement lowered to physical addresses -- a source's catalog, its schema and
its table -- and its error quotes them: DuckDB's Binder Error carries a ``LINE n:`` excerpt of the
physical SQL. A physical address is source configuration, and the caller wrote model names, so
:func:`client_text` -- the one text every transport answers a client's error with (the HTTP error
middleware, pgwire, Flight, MCP, gRPC) -- writes each as the model names it and drops the excerpt.
Logs, traces and the audit record keep the physical names: they are the operator's.
"""

# Requirements: REQ-2087, REQ-2152

from __future__ import annotations

import re

#: An engine's excerpt of the statement it ran: ``LINE n: ...`` and the caret line under it.
_EXCERPT = re.compile(r"\n+LINE \d+:[^\n]*(?:\n[ \t]*\^)?")
#: The same inside a JSON string: newlines escaped, and the string ending at an unescaped quote.
_EXCERPT_JSON = re.compile(r"(?:\\n)+LINE \d+:.*?(?:\\n[ \t]*\^|(?=(?<!\\)\"))")


def client_text(text: str, org_id: str | None = None) -> str:
    """``text`` as a client is told it: in the model's names (:func:`model_named`), then
    redacted. ``org_id`` is the org the request acted in; when the transport does not say, the
    org bound now."""
    from provisa.core.request_context import current_org
    from provisa.security.redaction import redact

    return redact(model_named(text, org_id if org_id is not None else current_org.get()))


def model_named(text: str, org_id: str | None) -> str:
    """``text`` with each physical address of a table (its source's catalog, schema and table, as
    a statement is lowered to it) written as the model names the table, quoted, unquoted or as a
    JSON body carries it, and with an engine's excerpt of the statement it ran dropped: truncated,
    the excerpt can begin inside a physical address, and its caret counts characters of SQL the
    caller never wrote.

    The addresses are ``org_id``'s tables. With no org known (a transport whose error leaves after
    its org binding ends), every org's: an address two orgs name differently is not rewritten,
    the excerpt being dropped all the same."""
    text = _EXCERPT.sub("", _EXCERPT_JSON.sub("", text))
    named = _names(org_id)
    if not named:
        return text
    found = re.compile(
        r"(?<![\w.])(?:"
        + "|".join(re.escape(k) for k in sorted(named, key=len, reverse=True))
        + r")(?![\w])"
    )
    return found.sub(lambda m: named[m.group(0)], text)


def _names(org_id: str | None) -> dict[str, str]:
    """Each physical address, in its three spellings, → the model's name for it."""
    from provisa.api.app import state
    from provisa.compiler.sql_rewrite import semantic_ref

    registry = state.org_registry
    if org_id is not None:
        runtime = registry.get(org_id)
        runtimes = [] if runtime is None else [runtime]
    else:
        runtimes = [r for o in registry.all_org_ids() if (r := registry.get(o)) is not None]
    named: dict[str, str] = {}
    conflicting: set[str] = set()
    for runtime in runtimes:
        for ctx in runtime.contexts.values():
            for meta in ctx.tables.values():
                model = semantic_ref(meta)  # "domain"."table"
                parts = (meta.catalog_name, meta.schema_name, meta.table_name)
                for physical, said in (
                    (".".join(f'"{p}"' for p in parts), model),
                    (".".join(f'\\"{p}\\"' for p in parts), model.replace('"', '\\"')),
                    (".".join(parts), model),
                ):
                    if named.setdefault(physical, said) != said:
                        conflicting.add(physical)
    return {k: v for k, v in named.items() if k not in conflicting}
