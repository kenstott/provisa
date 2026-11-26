# Copyright (c) 2026 Kenneth Stott
# Canary: 8d54bcd7-d0a4-4714-9c33-7ac376f763a5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Classification suggestions and the steward's decisions on them (REQ-2147).

A run tests each column's sampled values against the enabled kinds (``classify``) and records each
kind a column reached the threshold of as a ``classifications`` result row, with the decision in
force at the run's time. The decisions are stored in ``profiler_classifications``: an accepted
suggestion has linked the column to the kind's glossary Property term (a key column's meaning is its
Class term, so it is not moved) and, for a sensitive kind, marked it pii; a rejected one is not
suggested again for that column and kind. Withdrawing a decision makes the suggestion new again; it
does not undo what accepting did, which the glossary and the tags own from then on.

The kinds are the org's (REQ-2147, amended 2026-11-25): the shipped catalogue as the org changed it
and the org's own custom kinds, held in the org's ``classification`` setting
(``provisa.core.org_settings``) with the kinds it disabled, which a run tests none of. The setting
is part of the model, so it moves with the model across environments.
"""

# Requirements: REQ-2147

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any

from sqlalchemy import delete, select

from provisa.profiler.classify import Kind, Match, custom_id, kind_of, org_catalogue

DECISION_STATUSES: tuple[str, ...] = ("accepted", "rejected")
SETTING_KEY = "classification"  # the org setting holding the org's kinds (KindSettings)
KEY_COLUMN_NOTE = "a key column's meaning is its Class term"


# -- the org's kinds -------------------------------------------------------------------------------
#
# The org's ``classification`` setting holds what the org made of the kinds (REQ-2147, amended
# 2026-11-25): ``disabled``, the kinds it does not test; ``changed``, per shipped kind the settings
# it changed (CHANGEABLE); ``custom``, its own kinds. The setting holds only what differs from the
# shipped catalogue: a part the org has not touched is not stored (and no setting at all is the
# shipped catalogue, every kind enabled), so an absent part is an empty one by design.


@dataclass(frozen=True)
class KindSettings:
    disabled: frozenset[str]
    changed: dict[str, dict[str, Any]]  # shipped kind id -> {setting: value}
    custom: tuple[dict[str, Any], ...]  # stored custom kind entries, in the order they were made

    @property
    def kinds(self) -> tuple[Kind, ...]:
        """Every kind of the org, enabled or not."""
        return org_catalogue(self.changed, self.custom)

    @property
    def enabled(self) -> tuple[Kind, ...]:
        return tuple(k for k in self.kinds if k.id not in self.disabled)


SHIPPED = KindSettings(frozenset(), {}, ())


def settings_of(overrides: dict[str, Any]) -> KindSettings:
    """The org's kind settings, from its setting overrides; a setting naming an unknown kind, or
    holding a kind that does not load, is refused by name."""
    setting = overrides.get(SETTING_KEY)
    if setting is None:
        return SHIPPED
    unknown_parts = set(setting) - {"disabled", "changed", "custom"}
    if unknown_parts:
        raise ValueError(f"the org's classification setting holds unknown {sorted(unknown_parts)}")
    # REQ-2147: only what the org changed is stored (see above), so an absent part is empty by
    # design, not a value missing.
    settings = KindSettings(
        frozenset(setting.get("disabled", ())),
        dict(setting.get("changed", {})),
        tuple(setting.get("custom", ())),
    )
    unknown = settings.disabled - {k.id for k in settings.kinds}
    if unknown:
        raise ValueError(
            f"the org's classification setting disables unknown kinds {sorted(unknown)}"
        )
    return settings


def setting_of(settings: KindSettings) -> dict[str, Any] | None:
    """The org setting holding ``settings``, checked: only its parts that differ from the shipped
    catalogue; None (the row removed) when nothing does."""
    known = {k.id for k in settings.kinds}  # loads every kind: one that does not is refused here
    unknown = settings.disabled - known
    if unknown:
        raise ValueError(f"unknown classification kinds {sorted(unknown)}")
    out: dict[str, Any] = {}
    if settings.disabled:
        out["disabled"] = sorted(settings.disabled)
    if settings.changed:
        out["changed"] = {k: dict(v) for k, v in sorted(settings.changed.items())}
    if settings.custom:
        out["custom"] = [dict(e) for e in settings.custom]
    return out or None


def disabled_of(overrides: dict[str, Any]) -> frozenset[str]:
    """The kinds the org disabled, from its setting overrides; unknown ids are refused by name."""
    return settings_of(overrides).disabled


async def read_settings(model_db: Any) -> KindSettings:
    from provisa.core.org_settings import read_org_overrides

    return settings_of(await read_org_overrides(model_db))


async def org_kinds_on(conn: Any) -> dict[str, Kind]:
    """Every kind of the org by id, read on ``conn`` (a caller already holding a connection)."""
    from provisa.core.schema_org import org_settings

    result = await conn.execute_core(
        select(org_settings.c.value).where(org_settings.c.key == SETTING_KEY)
    )
    rows = result.fetchall()
    overrides = {SETTING_KEY: rows[0][0]} if rows else {}
    return {k.id: k for k in settings_of(overrides).kinds}


async def org_kinds(model_db: Any) -> tuple[Kind, ...]:
    """Every kind of the org, enabled or not: the shipped ones as it changed them, and its own."""
    return (await read_settings(model_db)).kinds


async def enabled_kinds(model_db: Any) -> tuple[Kind, ...]:
    return (await read_settings(model_db)).enabled


def kind_doc(kind: Kind, enabled: bool) -> dict[str, Any]:
    """A kind as the kinds' page and Polly read it (REQ-2147): a shipped kind's test is its
    catalogue name, a custom kind's its pattern, check and ignored characters."""
    return {
        "id": kind.id,
        "label": kind.label,
        "custom": kind.custom,
        "sensitive": kind.sensitive,
        "test": kind.test,
        "pattern": kind.pattern if kind.custom else None,
        "check": kind.check if kind.custom else None,
        "ignore": kind.ignore if kind.custom else None,
        "families": list(kind.families),
        "threshold": kind.threshold,
        "term": kind.term,
        "maskPreset": kind.mask_preset,
        "enabled": enabled,
    }


# -- changing the org's kinds ----------------------------------------------------------------------


def with_enabled(settings: KindSettings, enabled: set[str]) -> KindSettings:
    """``settings`` with exactly ``enabled`` of its kinds enabled."""
    ids = {k.id for k in settings.kinds}
    unknown = enabled - ids
    if unknown:
        raise KeyError(f"unknown classification kinds {sorted(unknown)}")
    return replace(settings, disabled=frozenset(ids - enabled))


def with_shipped_change(
    settings: KindSettings, kind_id: str, change: dict[str, Any]
) -> KindSettings:
    """``settings`` with the shipped kind ``kind_id``'s settings (CHANGEABLE) set to ``change``
    over its shipped ones. A change back to every shipped value is no change, and is not stored."""
    shipped = kind_of(kind_id)
    merged = {**settings.changed.get(kind_id, {}), **change}
    differs = {k: v for k, v in merged.items() if getattr(shipped, k) != v}
    changed = (
        {**settings.changed, kind_id: differs}
        if differs
        else {k: v for k, v in settings.changed.items() if k != kind_id}
    )
    _mask_known(merged.get("mask_preset"))
    return _loaded(replace(settings, changed=changed))


def _loaded(settings: KindSettings) -> KindSettings:
    """``settings``, once every kind of it loads: a kind that does not is refused by name."""
    settings.kinds  # noqa: B018 -- loading is the check
    return settings


def _mask_known(mask_preset: str | None) -> None:
    from provisa.security.mask_presets import PRESETS

    if mask_preset is not None and mask_preset not in {p["id"] for p in PRESETS}:
        raise ValueError(f"unknown mask preset {mask_preset!r}")


CUSTOM_FIELDS = (
    "label",
    "pattern",
    "check",
    "ignore",
    "threshold",
    "sensitive",
    "mask_preset",
    "term",
)


def with_custom(settings: KindSettings, entry: dict[str, Any]) -> tuple[KindSettings, str]:
    """``settings`` with a new custom kind of ``entry`` (CUSTOM_FIELDS); and its id, made from its
    name. A name whose id another kind has is refused by name."""
    missing = set(CUSTOM_FIELDS) - set(entry)
    if missing:
        raise ValueError(f"a custom classification kind needs {sorted(missing)}")
    kind_id = custom_id(entry["label"])
    if kind_id in {k.id for k in settings.kinds}:
        raise ValueError(f"a classification kind named {entry['label']!r} exists ({kind_id})")
    _mask_known(entry["mask_preset"])
    stored = {"id": kind_id, **{f: entry[f] for f in CUSTOM_FIELDS}}
    return _loaded(replace(settings, custom=(*settings.custom, stored))), kind_id


def with_custom_change(
    settings: KindSettings, kind_id: str, change: dict[str, Any]
) -> KindSettings:
    """``settings`` with the custom kind ``kind_id`` changed by ``change`` (some of
    CUSTOM_FIELDS). Its id is kept when its name changes: decisions on it are recorded by id."""
    unknown = set(change) - set(CUSTOM_FIELDS)
    if unknown:
        raise ValueError(f"a custom classification kind has no {sorted(unknown)}")
    held = [e for e in settings.custom if e["id"] == kind_id]
    if not held:
        raise KeyError(f"unknown custom classification kind {kind_id!r}")
    updated = {**held[0], **change}
    _mask_known(updated["mask_preset"])
    return _loaded(
        replace(
            settings,
            custom=tuple(updated if e["id"] == kind_id else e for e in settings.custom),
        )
    )


def without_custom(settings: KindSettings, kind_id: str) -> KindSettings:
    """``settings`` without the custom kind ``kind_id``; a shipped kind is not deleted."""
    if not any(e["id"] == kind_id for e in settings.custom):
        kind_of(kind_id)  # a shipped kind is refused as one, an unknown one by name
        raise ValueError(f"classification kind {kind_id!r} is shipped: it is disabled, not deleted")
    return replace(
        settings,
        disabled=settings.disabled - {kind_id},
        custom=tuple(e for e in settings.custom if e["id"] != kind_id),
    )


async def forget_kind(conn: Any, kind_id: str) -> None:
    """Remove every decision on, and hint of, a deleted kind: they are about a kind the model no
    longer has. What accepting did (a term link, a pii mark) stays, as withdrawing a decision
    leaves it."""
    from provisa.core.schema_org import profiler_classifications as pc
    from provisa.core.schema_org import profiler_sensitive_hints as ph

    await conn.execute_core(delete(pc).where(pc.c.kind == kind_id))
    await conn.execute_core(delete(ph).where(ph.c.kind == kind_id))


# -- decisions -------------------------------------------------------------------------------------


async def decisions(conn: Any, table_id: int) -> dict[tuple[str, str], dict]:
    """Every decision on the table's columns, by (physical column, kind)."""
    from provisa.core.schema_org import profiler_classifications as pc

    result = await conn.execute_core(select(pc).where(pc.c.table_id == table_id))
    return {(r.physical_column, r.kind): dict(r._mapping) for r in result.fetchall()}


@dataclass(frozen=True)
class Suggestion:
    column: str  # as the org admin reads it
    physical: str
    match: Match


def result_rows(
    table_name: str, suggestions: list[Suggestion], decided: dict[tuple[str, str], dict]
) -> list[dict]:
    """``classifications`` result rows (without the run key)."""
    out = []
    for s in suggestions:
        decision = decided.get((s.physical, s.match.kind.id))
        out.append(
            {
                "column_name": s.column,
                "physical_column": s.physical,
                "kind": s.match.kind.id,
                "sensitive": s.match.kind.sensitive,
                "values_tested": s.match.tested,
                "values_passed": s.match.passed,
                "share": s.match.share,
                "threshold": s.match.kind.threshold,
                "evidence": s.match.evidence(table_name, s.column),
                "status": "suggested" if decision is None else decision["status"],
            }
        )
    return out


async def record(
    conn: Any,
    table_id: int,
    physical: str,
    kind: Kind,
    *,
    status: str,
    evidence: str,
    run_id: str,
    by: str | None,
) -> None:
    from provisa.core.schema_org import profiler_classifications as pc

    if status not in DECISION_STATUSES:
        raise ValueError(f"a decision is one of {DECISION_STATUSES}, got {status!r}")
    values = {
        "table_id": table_id,
        "physical_column": physical,
        "kind": kind.id,  # a kind of the org's (org_kinds): its caller found it by id
        "status": status,
        "evidence": evidence,
        "run_id": run_id,
        "decided_by": by,
    }
    await conn.upsert(
        pc,
        values,
        index_elements=["table_id", "physical_column", "kind"],
        update_columns=["status", "evidence", "run_id", "decided_by"],
    )


async def forget(conn: Any, table_id: int, physical: str, kind_id: str) -> bool:
    """Withdraw a decision: the suggestion is new again. False when there is none."""
    from provisa.core.schema_org import profiler_classifications as pc

    result = await conn.execute_core(
        delete(pc).where(
            (pc.c.table_id == table_id)
            & (pc.c.physical_column == physical)
            & (pc.c.kind == kind_id)
        )
    )
    return bool(result.rowcount)


# -- accepting -------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Accepted:
    term: str | None  # the Property term the column now means; None for a key column
    term_note: str | None  # why the column was not linked, where it was not
    pii: bool  # whether the column was marked pii
    mask_preset: str | None  # the mask the kind offers, for a sensitive kind
    tag_changes: Any  # what the term move did to the column's inherited tags (REQ-2145)


class PiiRefused(Exception):
    """The pii tag could not be put on the column; ``result`` is the tag rules' refusal."""

    def __init__(self, result: Any) -> None:
        super().__init__(result.message)
        self.result = result


async def accept_effects(
    conn: Any,
    table_id: int,
    physical: str,
    kind: Kind,
    evidence: str,
    *,
    confirm_tag_loss: tuple[str, ...] = (),
    carry_tags: bool = False,
) -> Accepted:
    """Link the column to the kind's Property term and, for a sensitive kind, mark it pii, in the
    caller's transaction. A key column keeps its Class term (REQ-2141) and is marked pii alone.
    Raises the glossary's and the tags' own refusals."""
    from provisa.api.admin.schema_mutation import store_tag_assignment
    from provisa.core.models import TagAssignment
    from provisa.core.repositories import glossary as glossary_repo
    from provisa.core.repositories import tag as tag_repo
    from provisa.core.term_tags import inherited_snapshot, tag_changes_of_moves

    term: str | None = None
    note: str | None = None
    changes: Any = None
    if await glossary_repo.is_key_column(conn, table_id, physical):
        note = KEY_COLUMN_NOTE
    else:
        domain = await glossary_repo._table_domain(conn, table_id)  # noqa: SLF001
        term_id = await glossary_repo._find_or_create_term(conn, kind.term, domain)  # noqa: SLF001
        before = await inherited_snapshot(conn, {table_id})
        moved = await glossary_repo.move_ref(conn, table_id, physical, term_id)
        if moved is None:
            raise ValueError(f"column {physical!r} of table {table_id} has no glossary ref")
        changes = await tag_changes_of_moves(
            conn,
            before,
            {table_id},
            confirm_tag_loss=confirm_tag_loss,
            carry_tags=carry_tags,
        )
        term = kind.term
    if kind.sensitive:
        tag_row = await tag_repo.get(conn, "pii")
        assert tag_row is not None, "pii is a system tag"
        refused = await store_tag_assignment(
            conn,
            TagAssignment(
                tag_id="pii",
                object_type="column",
                table_id=table_id,
                column_name=physical,
                reason=evidence,
            ),
            tag_row,
        )
        if refused is not None:
            raise PiiRefused(refused)
    return Accepted(term, note, kind.sensitive, kind.mask_preset, changes)
