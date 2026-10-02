# Copyright (c) 2026 Kenneth Stott
# Canary: 3d7b1f58-9c2e-4a64-b8f1-0e5a7c93d2b6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Per-reader staleness tolerance (REQ-1907).

A table carries ``role_ttl``, an operator-set map of role -> TTL seconds. A reader's effective TTL
on a table is always ``max(cache_ttl, role_ttl(role))``: the table's resolved ``cache_ttl`` (its
own, else its source's -- never the global response-cache ``default_ttl``, which is not a landing
clock) is the operator's floor, and a role entry can only relax it. Only a table whose change
signal needs no clock (pushed, probe, trigger) may declare no ``cache_ttl``; it takes 0 as its
floor (REQ-1907, amended 2026-09-30). A ``ttl`` / ``ttl_probe`` table that lands with no
``cache_ttl`` is a configuration error (:func:`require_landing_ttl`, :func:`lands_from_config`). The effective TTL is one half of the replication gate;
the table's freshness check is the other (``query_residency.is_stale_of``).

``table`` / ``source`` are any objects carrying ``cache_ttl`` (and ``role_ttl`` on the table) — the
config models and the registry view's rows alike."""

# Requirements: REQ-1907

from __future__ import annotations

from typing import Any


# Change signals whose refresh cadence IS cache_ttl (REQ-930): with no cache_ttl they have no clock.
CLOCKED_SIGNALS = frozenset({"ttl", "ttl_probe"})


def declared_cache_ttl(table: Any, source: Any) -> int | None:
    """The landing cache_ttl the operator declared: the table's own, else its source's; None =
    none. The global response-cache default_ttl never applies here (REQ-1907, amended 2026-09-30)."""
    return table.cache_ttl if table.cache_ttl is not None else source.cache_ttl


def lands_from_config(
    *,
    materialize: bool,
    row_materialize: bool,
    table_replicate: int | None,
    source_replicate: int | None,
    table_load_protected: bool | None,
    source_load_protected: bool,
) -> bool:
    """True when the operator's settings say the table is replicated: materialize,
    row_materialize, a source floored as a whole (its load_protected or ``replicate: 0`` — it has
    no live attach, so every table of it is replica-served whatever the table says), or a
    resolved (table's own, else source's) load_protected or ``replicate`` that names replication
    — always (0) or once busy (N > 0) (``core.replicate.may_replicate``). Default (not set) and
    Never (-1) do not: a table replicated only because the bound engine cannot read its source in
    place is not known until the read."""
    from provisa.core.replicate import floor_of, may_replicate

    if materialize or row_materialize:
        return True
    if floor_of(source_replicate, bool(source_load_protected), promoted=False) is not None:
        return True
    replicate = table_replicate if table_replicate is not None else source_replicate
    protected = table_load_protected if table_load_protected is not None else source_load_protected
    return may_replicate(replicate, bool(protected))


def missing_landing_ttl(
    table_signal: str | None, source_signal: str, table_ttl: int | None, source_ttl: int | None
) -> str | None:
    """The error for a ttl / ttl_probe change signal with no table or source cache_ttl, else None
    (REQ-1907, amended 2026-09-30). Shared by config load, admin save and the read path; the
    no-TTL freshness signals (probe, native, debezium, kafka, signal) need no cache_ttl."""
    signal = table_signal if table_signal is not None else source_signal
    if signal in CLOCKED_SIGNALS and table_ttl is None and source_ttl is None:
        return (
            f"change_signal {signal!r} is TTL-based and the table lands, so add a cache_ttl to the "
            "table or its source (REQ-1907); the global response-cache default_ttl is not a "
            "landing TTL"
        )
    return None


def require_landing_ttl(table: Any, source: Any) -> None:
    """Raise when the table's change signal is ttl / ttl_probe and neither it nor its source
    declares a cache_ttl -- such a table has no refresh clock, so a read cannot judge it. Called
    only for a table the read actually lands (after the direct-attach decision)."""
    err = missing_landing_ttl(
        table.change_signal, source.change_signal, table.cache_ttl, source.cache_ttl
    )
    if err is not None:
        raise ValueError(f"table {table.table_name!r} (source {source.id!r}): {err}")


def floor_ttl(table: Any, source: Any) -> int:
    """The operator's floor: the declared cache_ttl, or 0 when none is declared (REQ-1907 mandates
    the 0 floor; only a change-fed / probed / triggered table may declare none, and its freshness
    check reports it fresh on the read path)."""
    declared = declared_cache_ttl(table, source)
    return declared if declared is not None else 0


def role_ttl(table: Any, source: Any, role: str | None) -> int:
    """The table's entry for ``role``. REQ-1907 mandates the lookup default: a role the table
    does not list (and a caller with no reader role, e.g. a background refresh) uses the table's
    cache_ttl, else its source's (never the global response-cache default_ttl)."""
    entries: dict[str, int] = table.role_ttl
    if role is not None and role in entries:
        return entries[role]
    return floor_ttl(table, source)


def effective_ttl(table: Any, source: Any, role: str | None) -> int:
    """``max(cache_ttl, role_ttl(role))`` — the operator's floor always wins."""
    return max(floor_ttl(table, source), role_ttl(table, source, role))


def max_accepted_ttl(table: Any, source: Any) -> int:
    """The longest any reader of this table accepts — the row cache's reap horizon."""
    entries: dict[str, int] = table.role_ttl
    return max([floor_ttl(table, source), *entries.values()])
