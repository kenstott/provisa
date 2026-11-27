# Copyright (c) 2026 Kenneth Stott
# Canary: 5c2e8a17-4b9d-4f63-a1e0-7d3b9c6f2e84
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The column rule keyed on a tag or a classification (REQ-2278): the ONE evaluator a table
column and a command output column are both governed through.

A rule names a tag or a classification kind and one role, and either hides every column carrying
the key from that role (and every role descending from it) or masks it for them. It names no
column: a column comes under it when it is tagged or classified, and leaves it when the tag or the
classification is taken off.

A column's own grant (``visible_to``) and mask list still apply; where a rule applies too, the
stricter result holds -- hidden over masked, masked over plain:

* hide: the column's grant loses every entry a hidden role holds, so the one column rule
  (:func:`provisa.security.rights.column_served`) serves it to them no longer.
* mask: the rule's entry is added to the column's mask list as a *floor* entry
  (:data:`FLOOR`). :func:`provisa.security.masking.resolve` reads the column's own entries first;
  only a reader its own list leaves unmasked reads it through the floor. A floor entry never makes
  a reader masked that no rule names, and never unmasks one the column's own list masks.
"""

# Requirements: REQ-2278

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Literal

#: Marks a mask entry a column rule added to a column's list (REQ-2278); the column's own entries
#: are read before any of them. Never stored: the entries are added as the runtime is built.
FLOOR = "floor"

HIDE = "hide"
MASK = "mask"

#: The kinds of key a rule is keyed on, as a column's keys name them.
TAG = "tag"
CLASSIFICATION = "classification"

#: What a column is, as a rule's reach names it: ``("column", table id, column name)`` for a table
#: column, ``("command_column", command name, column name)`` for a command's output column.
Target = tuple[str, Any, str]
Key = tuple[str, str]


@dataclass(frozen=True)
class ColumnRule:
    """One rule: on the columns carrying ``key`` (a (kind, id) pair), ``role_id`` -- and every role
    descending from it -- is hidden the column, or reads it through ``mask``. A mask rule with no
    ``mask`` reads the column completely masked."""

    id: int
    key: Key
    role_id: str
    action: Literal["hide", "mask"]
    mask: dict | None = None

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> "ColumnRule":
        key = (TAG, row["tag_id"]) if row.get("tag_id") else (CLASSIFICATION, row["classification"])
        return cls(
            id=row["id"],
            key=key,
            role_id=row["role_id"],
            action=row["action"],
            mask=row.get("mask"),
        )


def tag_keys(tag_id: str) -> tuple[Key, ...]:
    """The keys a column carrying ``tag_id`` is under: the tag, and for a parameterized tag
    (``entity:customer``) its base too, so a rule on ``entity`` reaches every entity column."""
    from provisa.core.models import base_tag_id

    base = base_tag_id(tag_id)
    return ((TAG, tag_id),) if base == tag_id else ((TAG, tag_id), (TAG, base))


def ruled_columns(
    keys: Mapping[Target, Iterable[Key]], rules: Iterable[ColumnRule]
) -> dict[Target, tuple[ColumnRule, ...]]:
    """Each column ``keys`` names (its tags and classifications) with the rules that reach it;
    a column no rule reaches is left out."""
    by_key: dict[Key, list[ColumnRule]] = {}
    for rule in rules:
        by_key.setdefault(rule.key, []).append(rule)
    out: dict[Target, tuple[ColumnRule, ...]] = {}
    for target, held in keys.items():
        found = tuple(
            sorted(
                {r for k in set(held) for r in by_key.get(k, ())},
                key=lambda r: r.id,
            )
        )
        if found:
            out[target] = found
    return out


def _hidden_from(entry: str, hidden: frozenset[str], chains: Mapping[str, list[str]]) -> bool:
    """Whether a grant entry is held only by readers a hide rule names: a part of it is a hidden
    role, or descends from one."""
    from provisa.security.inheritance import grant_parts

    return any(hidden & set(chains.get(part, [part])) for part in grant_parts(entry))


def hidden_grant(
    visible_to: list[str] | None,
    rules: Iterable[ColumnRule],
    chains: Mapping[str, list[str]],
) -> list[str] | None:
    """A column's grant with every entry a hide rule reaches taken out: the rule's role, and each
    role whose chain holds it. The column's grant decides who else is served it, as before. A
    column with no grant list (``None``, a command's public shape) keeps None: the caller reads
    such a column's grant from elsewhere and narrows that."""
    hidden = frozenset(r.role_id for r in rules if r.action == HIDE)
    if not hidden or visible_to is None:
        return visible_to
    return [e for e in visible_to if not _hidden_from(e, hidden, chains)]


def floor_entries(rules: Iterable[ColumnRule]) -> list[dict]:
    """The mask rules of ``rules`` as floor entries of a mask list, in rule order."""
    return [{"to": [r.role_id], "mask": r.mask, FLOOR: True} for r in rules if r.action == MASK]


def govern_column(
    column: dict, rules: Iterable[ColumnRule], chains: Mapping[str, list[str]]
) -> dict:
    """``column`` (a dict with ``visible_to`` and ``masks``) as the rules reaching it leave it: a
    copy with its grant narrowed by the hide rules and the mask rules added to its mask list as
    floor entries. The one evaluator a table column and a command output column go through."""
    ruled = list(rules)
    if not ruled:
        return column
    return {
        **column,
        "visible_to": hidden_grant(column.get("visible_to"), ruled, chains),
        "masks": [*(column.get("masks") or []), *floor_entries(ruled)],
    }


def hide_ruled_tables(
    tables: Iterable[dict],
    ruled: Mapping[Target, tuple[ColumnRule, ...]],
    chains: Mapping[str, list[str]],
) -> None:
    """Narrow, in place, the grant of every table column a hide rule reaches. Run on the build's
    own copies, after the grants are folded down the role chains, so a role granted the column
    through an ancestor is hidden it too."""
    if not ruled:
        return
    for t in tables:
        for column in t.get("columns") or []:
            rules = ruled.get(("column", t["id"], column["column_name"]))
            if rules:
                column["visible_to"] = hidden_grant(column.get("visible_to"), rules, chains)
