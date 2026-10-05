# Copyright (c) 2026 Kenneth Stott
# Canary: 5a8d2f61-9c34-4e07-b1f8-3e6a0d9c7b25
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a view reference becomes when a role reads it.

One rule, on every surface: through any view, materialized or not, a reader sees only what it
could see in the view's inputs.

* The view's SQL is expanded in place of the reference, with the reader's rules — row filters,
  masks, column visibility — applied to every table it reads (``stage2.govern_fragment``).
* A materialized view is built whole. Its stored rows are read only by a role whose rules on
  none of the view's inputs are narrower than "everything"; any other role reads the governed
  expansion, never the stored rows.
* The view table's own declared rules apply on top, in both cases.
* A view that names another region (REQ-1921) is read from the copy that region keeps, by every
  reader, with the view's own rules on top: its inputs are not read here.

This is the only place a view's body is chosen for a reader; the pipeline's expansion sites and
the GraphQL endpoint's call it."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from provisa.compiler.stage2 import GovernanceContext


def _input_names(sql: str) -> set[str]:
    from provisa.events.lineage import extract_inputs  # noqa: PLC0415

    return {name.rsplit(".", 1)[-1] for name in extract_inputs(sql, "postgres")}


def _narrowed(table_id: int, gov: "GovernanceContext") -> bool:
    """Whether the role's rules on ``table_id`` are narrower than "everything"."""
    if table_id in gov.rls_rules:
        return True
    if any(tid == table_id for tid, _ in gov.masking_rules):
        return True
    visible = gov.visible_columns.get(table_id)
    if visible is None:
        return False
    return any(column not in visible for column, _ in gov.all_columns.get(table_id, []))


def sees_everything(
    view: str,
    view_sql_map: dict[str, str],
    gov: "GovernanceContext",
    _seen: frozenset = frozenset(),
) -> bool:
    """Whether the role may read ``view``'s stored rows: none of the tables the view reads —
    directly, or through another view — carries a rule narrower than "everything" for it. An
    input that is neither a registered table nor a view cannot be judged, so it does not qualify."""
    if view in _seen:
        return False
    for name in _input_names(view_sql_map[view]):
        if name in view_sql_map:
            if not sees_everything(name, view_sql_map, gov, _seen | {view}):
                return False
            continue
        table_id = gov.table_map.get(name)
        if table_id is None or _narrowed(table_id, gov):
            return False
    return True


def _stored_rows(view: str, state: Any) -> str | None:
    """A read of ``view``'s stored rows, when it is materialized and its build is fresh."""
    from provisa.mv.models import MVStatus  # noqa: PLC0415

    mv = state.mv_registry.get(f"view-{view}")
    if mv is None or not mv.enabled or mv.status != MVStatus.FRESH or not mv.target_table:
        return None
    return f'SELECT * FROM "{mv.target_catalog}"."{mv.target_schema}"."{mv.target_table}"'


def _home_copy(view: str, state: Any) -> str | None:
    """REQ-1921, A VIEW MAY NAME A REGION: a read of the copy another region keeps of ``view`` —
    it names that region, which alone builds it — whoever reads it and whatever they could see in
    its inputs; None for a view this region builds. Whether that copy is built, and its store
    attached, is the statement's residency (``query_residency.read_home_views``)."""
    from provisa.federation.query_residency import home_view_read  # noqa: PLC0415

    mv = state.mv_registry.get(f"view-{view}")
    return None if mv is None else home_view_read(state, mv)


def _with_own_rules(view: str, body: str, gov: "GovernanceContext") -> str:
    """``body`` under the rules declared on the view's own table (its row filter, masks and
    column visibility), when it has any."""
    from provisa.compiler.stage2 import govern_fragment  # noqa: PLC0415
    from provisa.compiler.view_expand import _expand_one  # noqa: PLC0415

    table_id = gov.table_map.get(view)
    if table_id is None or not _narrowed(table_id, gov):
        return body
    own = govern_fragment(f"SELECT * FROM {view}", gov)
    return _expand_one(own, view, body)


def view_bodies(
    sql: str,
    view_sql_map: dict[str, str],
    state: Any,
    gov: "GovernanceContext",
) -> dict[str, str]:
    """For each view ``sql`` — a statement ALREADY governed — references (directly or through
    another view), what replaces the reference for the role ``gov`` was built for: its stored
    rows, or its SQL with the role's rules on every table it reads; the view table's own rules
    on top."""
    from provisa.compiler.stage2 import govern_fragment  # noqa: PLC0415

    bodies: dict[str, str] = {}
    pending = [sql]
    while pending:
        text = pending.pop()
        for view, view_sql in view_sql_map.items():
            if view in bodies or view not in text:
                continue
            stored = _stored_rows(view, state)
            home = _home_copy(view, state)
            if home is not None:
                body = home  # its region's copy, with the view's own rules on it below
            elif stored is not None and sees_everything(view, view_sql_map, gov):
                body = stored
            else:
                body = govern_fragment(view_sql, gov)
                pending.append(view_sql)
            bodies[view] = _with_own_rules(view, body, gov)
    return bodies


def unnarrowed_view_bodies(sql: str, view_sql_map: dict[str, str], state: Any) -> dict[str, str]:
    """For a read that acts for no role (an MV refresh builds a view whole), what replaces each
    view ``sql`` references, directly or through another view: by the same rule as
    :func:`view_bodies` with nothing narrowed — its stored rows when it is materialized and its
    build is fresh, else its own SQL."""
    bodies: dict[str, str] = {}
    pending = [sql]
    while pending:
        text = pending.pop()
        for view, view_sql in view_sql_map.items():
            if view in bodies or view not in text:
                continue
            stored = _home_copy(view, state) or _stored_rows(view, state)
            if stored is None:
                bodies[view] = view_sql
                pending.append(view_sql)
            else:
                bodies[view] = stored
    return bodies


def split_for_whole_statement_governance(
    sql: str, view_sql_map: dict[str, str], state: Any, gov: "GovernanceContext"
) -> tuple[dict[str, str], dict[str, str]]:
    """For a caller that validates and governs the whole statement AFTER expanding views (the
    GraphQL endpoint): ``(before, after)``.

    ``before``: the views to expand first, as their own SQL — the statement's validation and
    governance then reach the tables inside, with the view table's own rules already on top.
    ``after``: the views read from their stored rows, to expand once the statement has been
    validated and governed — until then each is the registered table it is, so the statement's
    governance applies the view table's own rules to it, and its store address (which is no
    registered table) never meets the validator."""
    before: dict[str, str] = {}
    after: dict[str, str] = {}
    pending = [sql]
    while pending:
        text = pending.pop()
        for view, view_sql in view_sql_map.items():
            if view in before or view in after or view not in text:
                continue
            stored = _stored_rows(view, state)
            if stored is not None and sees_everything(view, view_sql_map, gov):
                after[view] = stored
            else:
                before[view] = _with_own_rules(view, view_sql, gov)
                pending.append(view_sql)
    return before, after
