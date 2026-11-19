# Copyright (c) 2026 Kenneth Stott
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The domains an AI-assisted request works within (REQ-2253).

An AI-assisted feature -- the MCP chat's tool calls -- runs within the domains selected in the
domain filter, already intersected with the role's access (``provisa.nl.scope.selected_scope``).
That scope is bound here for the duration of one tool call, and every read the call makes asks it:
the catalog the tools list and describe, and the pipeline, which refuses a statement reading a
table of a domain outside it.

``None`` is no selection: the role's own surface, unchanged. It is what every request outside an
AI-assisted call sees, since nothing binds a scope for it.
"""

# Requirements: REQ-2253

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Generator, Iterable

# REQ-2253: unbound is no selection (None) -- every request that is not an AI-assisted call keeps
# the role's own surface, so the default is the design's "no narrowing", not a stand-in value.
_SELECTED: ContextVar[frozenset[str] | None] = ContextVar("provisa_selected_domains", default=None)


@contextmanager
def within(scope: frozenset[str] | None) -> Generator[None]:
    """Bind ``scope`` (raw domain ids, or None for no selection) for the block."""
    token = _SELECTED.set(scope)
    try:
        yield
    finally:
        _SELECTED.reset(token)


def current() -> frozenset[str] | None:
    """The bound scope, or None when no selection narrows this request."""
    return _SELECTED.get()


def admits(domain_id: str) -> bool:
    """Whether ``domain_id`` (a raw domain id) is inside the bound scope."""
    scope = _SELECTED.get()
    return scope is None or domain_id in scope


def refuse_outside(state: Any, table_ids: Iterable[int]) -> None:
    """Refuse a statement reading a registered table of a domain outside the bound scope.

    ``table_ids`` are the registered tables the statement reads, as the pipeline resolved them;
    their rows are ``state.tables``, read only when a scope is bound."""
    scope = _SELECTED.get()
    if scope is None:
        return
    ids = set(table_ids)
    for row in state.tables:
        if row["id"] in ids and row["domain_id"] not in scope:
            raise PermissionError(
                f"Table {row['table_name']!r} is in domain {row['domain_id']!r}, outside the "
                "domains selected in the domain filter"
            )
