# Copyright (c) 2026 Kenneth Stott
# Canary: 0d41cae7-b456-4d39-abd9-0237b3b4ceac
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The ``replicate`` setting (REQ-826): when a table's reads come from its replica.

One integer per table, inherited from its source when the table has none:

* not set — Default: the global threshold applies (the table is replicated once it passes that
  many governed statements per interval);
* ``-1`` — Never: read live wherever a live path exists; replicated only where replication is the
  sole way the engine reaches the source;
* ``N > 0`` — Hot-N: replicated once it passes N governed statements per interval;
* ``0`` — Always: reads come from the replica.

Only Always is a guarantee. Never and the Hot values are best effort. ``load_protected`` (REQ-1141)
also puts reads on the replica, and contradicts Never.

This module holds the value rules only — what a value means, how a table resolves its own, and
whether the operator's settings put a table's reads on its replica. Which tables an ENGINE serves
from a replica also depends on what the engine can read in place
(``provisa.federation.replica_routing``).
"""

# Requirements: REQ-826, REQ-1141, REQ-030

from __future__ import annotations

from typing import Any

#: Read live wherever a live path exists.
NEVER = -1
#: Reads come from the replica.
ALWAYS = 0

#: The setting names an operator-floor refusal reports (REQ-030).
REPLICATE = "replicate"
LOAD_PROTECTED = "load_protected"


def check_replicate(value: int | None) -> int | None:
    """``value`` when it is a legal ``replicate``: not set, -1, 0, or a threshold above 0."""
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(
            f"replicate must be an integer (-1 never, 0 always, N > 0 once the table passes N "
            f"statements per interval) or not set; got {value!r}"
        )
    if value < NEVER:
        raise ValueError(
            f"replicate must be -1 (never), 0 (always), a threshold above 0, or not set; "
            f"got {value}"
        )
    return value


def _own(holder: Any, name: str) -> Any:
    return holder[name] if isinstance(holder, dict) else getattr(holder, name)


def resolved_replicate(source: Any, table: Any) -> int | None:
    """The table's ``replicate``: its own value, else its source's (None when neither is set)."""
    own = _own(table, "replicate")
    return own if own is not None else _own(source, "replicate")


def resolved_load_protected(source: Any, table: Any) -> bool:
    """Whether the table is load-protected: its own value, else its source's (REQ-1141)."""
    own = _own(table, "load_protected")
    return bool(own) if own is not None else bool(_own(source, "load_protected"))


def contradiction(replicate: int | None, load_protected: bool) -> str | None:
    """The error for ``load_protected`` with Never, else None. A load-protected table is never
    read live; Never asks for it to be read live wherever it can be."""
    if load_protected and replicate == NEVER:
        return (
            "load_protected with replicate -1 (never) is contradictory: a load-protected table "
            "is never read live. Remove one of the two settings (REQ-826)"
        )
    return None


def floor_of(replicate: int | None, load_protected: bool, *, promoted: bool) -> str | None:
    """The operator setting that puts reads on the replica, or None when they may be live.

    ``load_protected`` and Always each do. A table under a threshold (Default or Hot-N) does once
    it is ``promoted`` — it passed its threshold and its replica serves it. Never does not."""
    if load_protected:
        return LOAD_PROTECTED
    if replicate == ALWAYS:
        return REPLICATE
    if promoted and (replicate is None or replicate > ALWAYS):
        return REPLICATE
    return None


def may_replicate(replicate: int | None, load_protected: bool) -> bool:
    """Whether an operator setting SAYS this table is to be replicated — now (load_protected,
    Always) or once it is busy (Hot-N). Default (not set) and Never are not such a statement.
    The tables REQ-1907's replication-clock rule judges at config load and at save."""
    return load_protected or (replicate is not None and replicate >= ALWAYS)
