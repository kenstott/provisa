# Copyright (c) 2026 Kenneth Stott
# Canary: 8c2f4e61-7a93-4d05-b1e8-3f6a9d2c7b14
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What an audit row records about how a statement was governed and answered.

A row names the model in force (the REQ-1914 stamp) and what was enforced on the statement, so
that what a person was shown can be checked against the rules that applied when they were shown
it. The summary is taken from the statement's own governance — the same object the governance
stage applied — once, where it was decided:

* ``row_filters``: per table, the role's row filter as governed (session terms unresolved) and
  the NAMES of the session variables it reads. Never their values: those are per-person data.
* ``masks``: per table, each masked column and the kind of mask applied.
* ``row_cap`` / ``table_caps`` / ``sample``: the row limits applied to a read (a write has none).
* ``write``: for a data write, the table written, the columns written, and whether the role's
  row filter governed it (narrowed the rows it touched and checked the rows it left).

``data_age`` is taken at the terminal, from how the rows were answered: a response-cache hit
gives the entry's age, a read as of a time gives that time, a live read gives nothing (current
at ``logged_at``)."""

from __future__ import annotations

import re
import threading
from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from provisa.compiler.stage2 import GovernanceContext

_SESSION_TERM = re.compile(r"current_setting\(\s*'provisa\.([A-Za-z0-9_]+)'\s*\)", re.IGNORECASE)


def _mask_kind(rule: Any) -> str:
    kind = getattr(rule, "mask_type", None)
    if kind is None:
        raise TypeError(f"a masking rule without a mask type: {rule!r}")
    return str(getattr(kind, "value", kind))


def enforced_summary(
    gov: "GovernanceContext",
    table_ids: Iterable[int] | None,
    tree: Any = None,
) -> dict[str, Any]:
    """What ``gov`` enforced on a statement over ``table_ids`` (None: every table it governs);
    ``tree`` is the statement, read only to describe a write."""
    tids = None if table_ids is None else set(table_ids)

    def _in(tid: int) -> bool:
        return tids is None or tid in tids

    row_filters = {
        str(tid): {
            "filter": text,
            "session_variables": sorted({m.group(1) for m in _SESSION_TERM.finditer(text)}),
        }
        for tid, text in sorted(gov.rls_rules.items())
        if _in(tid)
    }
    masks: dict[str, dict[str, str]] = {}
    for (tid, column), (rule, _dtype) in sorted(gov.masking_rules.items()):
        if _in(tid):
            masks.setdefault(str(tid), {})[column] = _mask_kind(rule)

    # The columns the role could see in each table read. Dropped from the row when its model
    # commit is proven (the commit holds them); kept when it is not (provisa/audit/writer.py).
    visible = {
        str(tid): sorted(cols if cols is not None else (c for c, _ in gov.all_columns.get(tid, [])))
        for tid, cols in sorted(gov.visible_columns.items())
        if _in(tid)
    }
    summary: dict[str, Any] = {
        "row_filters": row_filters,
        "masks": masks,
        "visible_columns": visible,
    }
    write = _write_summary(tree, gov) if tree is not None else None
    if write is not None:
        summary["write"] = write
    else:
        summary["row_cap"] = gov.limit_ceiling
        summary["table_caps"] = {
            str(tid): cap for tid, cap in sorted(gov.table_ceilings.items()) if _in(tid)
        }
        summary["sample"] = gov.sample_size
    return summary


def _write_summary(tree: Any, gov: "GovernanceContext") -> dict[str, Any] | None:
    from provisa.compiler.write_admission import is_write, written_columns, written_table_id

    if not is_write(tree):
        return None
    table_id = written_table_id(tree, gov)
    return {
        "table": table_id,
        "columns": sorted(written_columns(tree, gov, table_id)),
        "row_filter": table_id in gov.rls_rules,
    }


# The summaries GraphQL rows were given, kept for the governance inputs in force: every request of
# a role between two model builds has the same inputs, and a governance build per row put a
# whole-model build on the audit writer's thread for every record, holding the GIL in a burst each
# batch. The inputs are replaced, never changed in place, when a build or a meta-role changes
# them, so their identities name them. Only the current inputs are kept: the model's (masking
# rules, tables, relationships) for every role, then each role's own (row filters, compilation
# context, role); a row with other inputs replaces them, so a replaced model is never held.
_KEPT_PER_ROLE = 4096  # distinct table sets one role's requests read
_kept_model: tuple = ()
_kept: dict[str, tuple[tuple, dict[tuple[int, ...], dict]]] = {}
_kept_lock = threading.Lock()


def _same(a: tuple, b: tuple) -> bool:
    return len(a) == len(b) and all(x is y for x, y in zip(a, b))


def _kept_summaries(role_id: str, model: tuple, own: tuple) -> dict[tuple[int, ...], dict]:
    """The role's kept summaries for these inputs, emptied of any kept for others."""
    global _kept_model
    with _kept_lock:
        if not _same(_kept_model, model):
            _kept_model = model
            _kept.clear()
        entry = _kept.get(role_id)
        if entry is None or not _same(entry[0], own):
            entry = _kept[role_id] = (own, {})
        return entry[1]


def enforced_for_request(state: Any, role_id: str, ctx: Any) -> Callable[[tuple[int, ...]], dict]:
    """For an audit row whose statement's governance object is not at hand (a GraphQL request,
    whose fields were governed one by one): the role's governance as it stands NOW — every input
    captured here, on the request thread — built when the row is written, over the tables the
    request read."""
    from provisa.compiler.rls import RLSContext
    from provisa.compiler.stage2 import build_governance_context
    from provisa.security.rights import require_role

    rls = state.rls_contexts.get(role_id)
    masking_rules = state.masking_rules
    tables = getattr(state, "tables", [])
    relationships = getattr(state, "relationships", None)
    role = require_role(state.roles, role_id)

    def _summary(table_ids: tuple[int, ...]) -> dict:
        kept = _kept_summaries(role_id, (masking_rules, tables, relationships), (rls, ctx, role))
        summary = kept.get(table_ids)
        if summary is not None:
            return summary
        gov = build_governance_context(
            role_id,
            rls if rls is not None else RLSContext.empty(),
            masking_rules,
            ctx,
            tables,
            role=role,
            relationships=relationships,
        )
        summary = enforced_summary(gov, table_ids)
        with _kept_lock:
            if len(kept) >= _KEPT_PER_ROLE:
                kept.clear()
            kept[table_ids] = summary
        return summary

    return _summary


def data_age(plan: Any, cache_entry: Any) -> dict[str, Any] | None:
    """How old the rows a plan was answered with are, as far as the terminal can see: the age of
    the response-cache entry it was served from (``cache_entry``, None when it was not), a read
    as of a stated time; None for a live read."""
    age: dict[str, Any] = {}
    if cache_entry is not None:
        age["cache_age_seconds"] = cache_entry.age_seconds
    as_of = getattr(plan, "cache_as_of", None)
    if as_of is not None:
        age["as_of"] = as_of
    return age or None
