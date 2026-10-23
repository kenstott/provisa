# Copyright (c) 2026 Kenneth Stott
# Canary: 8b2f6d14-3e7a-4c91-a5d0-6f9e2c1b7a38
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a retired term applied to (REQ-2144): its column and table refs as the environment's
repository held them at the model-store commit before its retirement (REQ-1524 commits). A
retirement moves them to the successor, so the commit history is their record; nothing else
stores them."""

# Requirements: REQ-2144

from __future__ import annotations

from typing import Any

#: How far back the history is searched for the retirement.
_HISTORY_LIMIT = 10_000


def _names_the_retirement(message: str, term_id: int) -> bool:
    """Whether a commit's message names ``term_id``'s retirement, as model_change writes it: the
    message itself, or one ``- `` line of a change of several."""
    marker = f"retire glossary term {term_id}"
    return any(line.strip() in (marker, f"- {marker}") for line in message.splitlines())


def applied_history(org_id: str, env: str, term_id: int, term_name: str) -> dict[str, Any] | None:
    """The refs ``term_id`` held at the commit before the one that retired it -- ``columns``
    (table path and column) and ``tables`` (table paths) -- with both commits; None when the
    environment's history holds no retirement of it."""
    from provisa.core import env_files, env_repo
    from provisa.core.env_project import KIND_DIRS

    retired_in = next(
        (
            c["sha"]
            for c in env_repo.history(org_id, env, limit=_HISTORY_LIMIT)
            if _names_the_retirement(c["message"], term_id)
        ),
        None,
    )
    if retired_in is None:
        return None
    before = env_repo.parent_of(org_id, retired_in)
    if before is None:
        return {"retired_in": retired_in, "as_of": None, "columns": [], "tables": []}
    tree = env_files.load(env_repo.files_at(org_id, before))
    term_path = env_files.kind_path(KIND_DIRS["glossary_terms"], term_name)
    columns: list[dict[str, str]] = []
    tables: list[str] = []
    for path, body in sorted(tree.items()):
        for ref in body.get("glossary", []):
            if ref["term"] == term_path:
                columns.append({"table": path, "column": ref["column_name"]})
        if any(ref["term"] == term_path for ref in body.get("class_term", [])):
            tables.append(path)
    return {"retired_in": retired_in, "as_of": before, "columns": columns, "tables": tables}
