# Copyright (c) 2026 Kenneth Stott
# Canary: b37e0c91-4a2d-4c68-8f15-6d09e2c7a143
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What dbt's ``run_results.json`` says of one invocation (REQ-2228, REQ-858).

Each invocation writes the outcome of the nodes it ran, and only those: a ``dbt run --select x``
says nothing of anything else, so a document adds to what is known and never replaces it.
"""

# Requirements: REQ-2228, REQ-858

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime

#: The step of a node's timing during which dbt built it.
_EXECUTE = "execute"
#: The status dbt gives a model, snapshot or seed it built.
SUCCESS = "success"
#: What dbt builds, by the leading part of a node's unique id: what a run's outcome can date.
_BUILT_KINDS = frozenset({"model", "snapshot", "seed"})


class NotRunResults(ValueError):
    """The document given is not a dbt run results file."""


@dataclass(frozen=True)
class NodeResult:
    unique_id: str
    status: str
    #: When dbt finished executing the node; None for a node it did not execute.
    completed_at: datetime | None

    @property
    def builds(self) -> bool:
        """Whether this is a node dbt builds, and not a test or a macro's operation."""
        return self.unique_id.split(".", 1)[0] in _BUILT_KINDS


@dataclass(frozen=True)
class RunResults:
    generated_at: datetime
    results: tuple[NodeResult, ...]


def _when(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _completed(result: dict) -> datetime | None:
    step = next((t for t in result.get("timing") or [] if t.get("name") == _EXECUTE), None)
    return None if step is None or not step.get("completed_at") else _when(step["completed_at"])


def parse_run_results(document: str | bytes | dict) -> RunResults:
    """The outcomes ``document``, a dbt ``run_results.json``, declares; refused when it is none."""
    try:
        raw = json.loads(document) if isinstance(document, str | bytes) else document
    except ValueError as exc:
        raise NotRunResults(f"not dbt run results: {exc}") from exc
    metadata = raw.get("metadata") if isinstance(raw, dict) else None
    if not isinstance(metadata, dict) or "/run-results/" not in str(
        metadata.get("dbt_schema_version", "")
    ):
        raise NotRunResults("not dbt run results: it declares no run results schema version")
    return RunResults(
        generated_at=_when(metadata["generated_at"]),
        results=tuple(
            NodeResult(r["unique_id"], r["status"], _completed(r)) for r in raw.get("results") or []
        ),
    )
