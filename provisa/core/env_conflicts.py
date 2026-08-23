# Copyright (c) 2026 Kenneth Stott
# Canary: 8f41d2b6-73ae-4c09-9e15-3d6b8a4f21c7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""WHICH OF THE TARGET'S OWN WORK A MERGE IS ABOUT TO OVERWRITE (REQ-1555).

A merge carries the source's model onto the target by identity: every object the source has, the
target gets. That rule is right for the object the source changed and WRONG, silently, for the
object somebody changed in the target -- the merge overwrites it, reports it as an ordinary
``changed``, and nothing distinguishes it from the case the operator was asking for. Two models
differing is not information about who differed.

A third input is what separates them, and it is the commit both lines last held: an object that
differs from the base on BOTH sides was edited twice, independently, and one of those edits is
about to disappear. That is a conflict, and a conflict is worth knowing about whether or not
anybody decides to stop for it -- it says two people were working on the same object without
knowing it, which is a fact about the org, not about this merge.

MEASURED IN PATHS, not rows. A path is the unit the model is versioned in (REQ-1526), the unit a
deploy's delta already speaks in, and the unit a person reads; the surrogate keys the rows carry
belong to one schema and mean nothing in the other. The comparison is therefore between three
projections of the same shape: the source's schema, the target's schema, and the tree at the base.

REPORTED, NOT REFUSED. The merge still applies, and the source still wins -- that is what a merge
into a target is. What changes is that the report names every object whose other edit it just
carried away, so the person approving the merge (REQ-1504) and the audit record afterwards both
say so.
"""

# Requirements: REQ-1490, REQ-1526, REQ-1539, REQ-1555

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from provisa.core.env_classes import SEEDED_AT_CREATION

if TYPE_CHECKING:
    from provisa.core.database import Connection

#: What one side did to an object, relative to the commit both lines last held.
CHANGED = "changed"
REMOVED = "removed"
ADDED = "added"


@dataclass(frozen=True)
class Conflict:
    """One object two lines edited independently, and what each of them did to it."""

    path: str
    source: str
    target: str

    def as_dict(self) -> dict[str, Any]:
        return {"path": self.path, "source": self.source, "target": self.target}


def _side(base: dict[str, dict], side: dict[str, dict], path: str) -> str | None:
    """What ``side`` did to ``path`` since ``base``, or None if it did nothing."""
    was, now = base.get(path), side.get(path)
    if was == now:
        return None
    if was is None:
        return ADDED
    if now is None:
        return REMOVED
    return CHANGED


def compare(
    base: dict[str, dict[str, Any]],
    source: dict[str, dict[str, Any]],
    target: dict[str, dict[str, Any]],
) -> list[Conflict]:
    """Every object both lines moved away from ``base`` differently.

    An object both sides changed INTO THE SAME THING is not a conflict -- two people arriving at
    one answer is agreement, and a merge that stopped for it would be stopping for nothing. An
    object only one side touched is not a conflict either, whichever side that was: the source's
    edit is what the merge is carrying, and the target's edit is one the source never contradicts.
    """
    found = []
    for path in sorted(set(base) | set(source) | set(target)):
        if source.get(path) == target.get(path):
            continue
        src, tgt = _side(base, source, path), _side(base, target, path)
        if src is not None and tgt is not None:
            found.append(Conflict(path, src, tgt))
    return found


async def detect(
    conn: "Connection",
    org_id: str,
    source_env: str | None,
    target_env: str | None,
    src_schema: str,
    dst_schema: str,
) -> tuple[str | None, list[Conflict]]:
    """The base the two environments last shared, and what they each did to it since.

    Returns ``(None, [])`` when the lines share NO ancestor -- two environments rooted separately
    rather than branched from one another. The caller reports that as a question that could not be
    asked, never as a clean merge: an empty list under a base of None means nothing was compared.

    Both projections are read on the caller's connection, inside the caller's transaction, so what
    the report names is what the apply in that same transaction acts on.
    """
    from provisa.core.env_project import project
    from provisa.core.env_repo import files_at, merge_base, tip
    from provisa.core.environments import PROD

    source_tip = tip(org_id, source_env or PROD)
    target_tip = tip(org_id, target_env or PROD)
    if source_tip is None or target_tip is None:
        # A line that has not started holds no commits, so there is nothing for the other to have
        # last shared with it. REQ-1543 gives every environment a baseline, so this is the window
        # before that has run rather than a state an environment rests in.
        return None, []
    base_sha = merge_base(org_id, source_tip, target_tip)
    if base_sha is None:
        return None, []

    from provisa.core.env_deploy import PROJECTED, table_of
    from provisa.core.env_files import load as load_files

    # Only what a merge CARRIES can be conflicted by one. The seeded classes are the environment's
    # own answer to who may do what and never travel (REQ-1539), so the two lines differing there
    # is not two edits colliding -- it is the design.
    scope = PROJECTED - SEEDED_AT_CREATION

    def carried(tree: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
        return {p: b for p, b in tree.items() if table_of(p) in scope}

    base = carried(load_files(files_at(org_id, base_sha)))
    source = carried(await project(conn, src_schema))
    target = carried(await project(conn, dst_schema))
    return base_sha, compare(base, source, target)
