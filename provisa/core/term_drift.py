# Copyright (c) 2026 Kenneth Stott
# Canary: c43a8e17-52d6-4f09-b7e3-1f6d90a2c58e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Ontology quality report's drift (REQ-1386, DRIFT FROM THE MODEL'S HISTORY): the terms that
lost or gained grounding since the previous period, computed from the environment's repository
(REQ-1524 commits) rather than read from a view, since the glossary stores no grounding history.

The previous period is the last full calendar month. Its start, the first of last month UTC, picks
the base: the latest commit at or before it on the branch's first-parent line. The base's tree and
the tip's are graded with :func:`provisa.core.glossary.term_grounding`, the computation the report's
own grounding states in SQL, and compared. With no commit by the cutoff there is nothing to compare,
which is no history and never zero drift.

A commit's tree holds no surrogate ids, so a term's identity across commits is its name: a rename
is one term lost and one term new.
"""

# Requirements: REQ-1386, REQ-1387

from __future__ import annotations

import sys
from datetime import UTC, datetime
from typing import Any

from provisa.core.glossary import GROUNDED, GROUNDED_PENDING, NOT_GROUNDED, term_grounding

#: A grade's height: a move down is a loss, a move up a gain, so a term whose every path to data
#: comes to run through a draft has lost grounding though it still reaches data.
_RANK = {NOT_GROUNDED: 0, GROUNDED_PENDING: 1, GROUNDED: 2}


def period_cutoff(now: datetime) -> datetime:
    """The start of the previous period for ``now``: the first of last month, UTC."""
    utc = now.astimezone(UTC)
    year, month = (utc.year - 1, 12) if utc.month == 1 else (utc.year, utc.month - 1)
    return datetime(year, month, 1, tzinfo=UTC)


def graded(files: dict[str, str]) -> dict[str, str]:
    """Each term of a commit's tree, by name, with its grounding: the tree's terms, edges and refs
    handed to the same ``term_grounding`` the live model is graded with."""
    from provisa.core import env_files
    from provisa.core.env_project import KIND_DIRS

    tree = env_files.load(files)
    term_dir = f"{KIND_DIRS['glossary_terms']}/"
    ids = {path: tid for tid, path in enumerate(p for p in tree if p.startswith(term_dir))}
    terms = [{**tree[path], "id": tid} for path, tid in ids.items()]
    edges = [
        (ids[path], ids[edge["to"]], edge["rel_type"])
        for path in ids
        for edge in tree[path].get("edges", [])
    ]
    rooted = {
        ids[ref["term"]]
        for body in tree.values()
        for ref in (*body.get("glossary", []), *body.get("class_term", []))
    }
    grounding = term_grounding(terms, edges, rooted)
    return {t["name"]: grounding[t["id"]] for t in terms}


def _change(before: str | None, after: str | None) -> str | None:
    """What moved between two grades of one name, ``None`` when nothing did. A term absent from the
    base is ``new`` whatever it holds; one absent from the tip holds nothing."""
    if before is None:
        return "new"
    was = _RANK[before]
    now = 0 if after is None else _RANK[after]
    return "lost" if now < was else "gained" if now > was else None


def compare(before: dict[str, str], after: dict[str, str]) -> list[dict[str, Any]]:
    """The terms whose grounding moved between two grades, by name. A term absent from ``before``
    or ``after`` has ``None`` there."""
    rows = []
    for name in sorted(before.keys() | after.keys()):
        was, now = before.get(name), after.get(name)
        change = _change(was, now)
        if change is not None:
            rows.append({"name": name, "before": was, "after": now, "change": change})
    return rows


def counted(rows: list[dict[str, Any]]) -> dict[str, int]:
    """How many terms lost grounding, gained it, and are new."""
    return {kind: sum(r["change"] == kind for r in rows) for kind in ("lost", "gained", "new")}


def drift(org_id: str, env: str, now: datetime | None = None) -> dict[str, Any]:
    """``env``'s drift since the previous period: the ``cutoff``, the ``base`` commit it is
    measured from, and the moved ``terms`` with their ``counts``. ``has_history`` is False, with
    ``base``, ``terms`` and ``counts`` None, when the branch holds no commit by the cutoff."""
    from provisa.core import env_repo

    cutoff = period_cutoff(datetime.now(UTC) if now is None else now)
    tip = env_repo.tip(org_id, env)
    # The whole line, unpaged: a base past a page's end would read as no history.
    line = [] if tip is None else env_repo.history(org_id, tip, limit=sys.maxsize)
    eligible = [c for c in line if c["committed_at"] <= cutoff.timestamp()]
    if tip is None or not eligible:
        return {
            "has_history": False,
            "cutoff": cutoff.isoformat(),
            "base": None,
            "terms": None,
            "counts": None,
        }
    base = max(eligible, key=lambda c: c["committed_at"])
    rows = compare(
        graded(env_repo.files_at(org_id, base["sha"])), graded(env_repo.files_at(org_id, tip))
    )
    return {
        "has_history": True,
        "cutoff": cutoff.isoformat(),
        "base": {
            "sha": base["sha"],
            "committed_at": datetime.fromtimestamp(base["committed_at"], UTC).isoformat(),
        },
        "terms": rows,
        "counts": counted(rows),
    }
