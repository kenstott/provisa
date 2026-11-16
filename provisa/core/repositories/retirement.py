# Copyright (c) 2026 Kenneth Stott
# Canary: e18dddf3-32c3-4ba5-973d-13677f18c437
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Deleting a model object retires it (REQ-2248).

A table, column, relationship, tag or metric that is deleted keeps its row, its name and its file
in the model repository, marked retired with why, who and when -- the shape a glossary term's
retirement takes (``retired``, ``status_reason``, ``status_by``, ``status_at``;
:func:`provisa.core.repositories.glossary.retire_term`). Who is the person the change commits as
(:func:`provisa.core.model_change.actor`); why is the caller's, when it gives one.

A retired source or domain is the exception (maintainer 2026-11-15): its id is its name, and a
retired one leaves it free for reuse. Its row is kept under a key of its own (:func:`retired_key`)
with the id it had in ``retired_id``, and its restore is refused while a live object holds that
id (:class:`NameHeld`).

A retired name otherwise stays taken: the store refuses to write a second object at it
(:class:`RetiredNameTaken`), since an object at that name would otherwise be the retired one
brought back by a write that did not ask for it.
"""

# Requirements: REQ-2248

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import func

from provisa.core import model_change


class RetiredNameTaken(Exception):
    """A write that would put an object at the name of a retired one (REQ-2248). Its message
    names the retired object and says it can be restored."""

    def __init__(self, kind: str, name: str) -> None:
        self.kind = kind
        self.name = name
        super().__init__(
            f"the name {name!r} belongs to a retired {kind}; restore that {kind} to use it again"
        )


def stamp_of(table: Any) -> list[Any]:
    """The retirement columns of ``table`` (a model table carrying them): whether it is retired,
    why, by whom and when."""
    return [table.c.retired, table.c.status_reason, table.c.status_by, table.c.status_at]


def cleared() -> dict[str, Any]:
    """The column values that restore a retired row (REQ-2248): its retirement cleared."""
    return {"retired": False, "status_reason": None, "status_by": None, "status_at": None}


def values(reason: str | None) -> dict[str, Any]:
    """The column values that retire a row now, as the acting person, for ``reason``."""
    text = reason.strip() if reason is not None else None
    return {
        "retired": True,
        "status_reason": text or None,
        "status_by": model_change.actor(),
        "status_at": func.now(),
    }


def stamped(reason: str | None, by: str | None) -> dict[str, Any]:
    """:func:`values` as plain data, for a row written whole (a column re-inserted retired, a
    model file a merge retires) rather than updated in place."""
    text = reason.strip() if reason is not None else None
    return {
        "retired": True,
        "status_reason": text or None,
        "status_by": by,
        "status_at": datetime.now(UTC),
    }


class NameHeld(Exception):
    """A restore of a retired object whose name a live object now holds (REQ-2248). Its message
    names both."""

    def __init__(self, kind: str, name: str, holder: str) -> None:
        self.kind = kind
        self.name = name
        self.holder = holder
        super().__init__(
            f"the retired {kind} {name!r} cannot be restored: {holder} holds that name now"
        )


#: What separates a retired source's or domain's id from the rest of the key it is kept under.
#: A source id is letters, digits, hyphens and underscores, so no live id holds it.
RETIRED_MARK = "~retired~"


def retired_key(live_id: str) -> str:
    """The key a retired source or domain is kept under (REQ-2248): its id, marked, and unique to
    this retirement, so the id is free and the same id can be retired again."""
    return f"{live_id}{RETIRED_MARK}{uuid4().hex[:12]}"
