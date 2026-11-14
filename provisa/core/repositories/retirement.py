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

A retired name stays taken: the store refuses to write a second object at it
(:class:`RetiredNameTaken`), since an object at that name would otherwise be the retired one
brought back by a write that did not ask for it.
"""

# Requirements: REQ-2248

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import func

from provisa.core import model_change


class RetiredNameTaken(Exception):
    """A write that would put an object at the name of a retired one (REQ-2248)."""

    def __init__(self, kind: str, name: str) -> None:
        self.kind = kind
        self.name = name
        super().__init__(f"the {kind} name {name!r} is held by a retired {kind}")


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
