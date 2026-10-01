# Copyright (c) 2026 Kenneth Stott
# Canary: 3f9a6c27-8d41-4e5b-a2c8-7b0e1d4f9a63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Keeping a statement's governed plan so a repeat does not rebuild it (REQ-1877).

Parsing, validating and governing a statement is a pure function of the statement, the role, the
acting person, the session variables RLS resolves against and the role's governance objects. Every
surface therefore keeps the result the same way, in the org's one ``compiled_query_cache``
(per-org, in-memory, TTL-evicted, bounded):

- the raw-SQL stage (``_pipeline.govern_statement``): pgwire, Flight SQL, /data/sql, MCP;
- the compiled stage (``_pipeline._govern_and_route_compiled_planned``): Cypher over HTTP,
  Bolt and Flight, gRPC, REST, JSON:API, GraphQL over Flight, NL;
- the GraphQL endpoint (``api.data.graphql_plan``), which also keeps the parse and compile.

One :class:`PlanSlot` per request holds the rules they share:

- the key carries the stage, ``schema_boot_id`` + ``schema_version`` (bumped by every schema,
  masking, RLS, relationship and role change), the role, the acting person and a digest of the
  stage's own inputs (statement text, session variables, ...). The store is per org.
- a kept plan answers only while every governance object it was built from is still the SAME
  object (identity, not equality): a rebuild swaps those objects before it bumps
  ``schema_version``, so the generation alone would let a plan built from the old objects answer
  under the new number.
- nothing is kept while a schema rebuild is in progress, or when the generation moved between the
  lookup and the keep: the state the plan read was changing.

What a stage must not keep (a write, a statement that runs a registered command, an approval-hook
deployment) is that stage's own rule, applied before it calls :meth:`PlanSlot.keep`."""

# Requirements: REQ-1877

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from provisa.audit.context import current_audit_identity


def _rebuild_in_progress() -> bool:
    from provisa.api.app import _rebuild_schemas_lock

    return _rebuild_schemas_lock.locked()


def governance_anchors(state: Any, role_id: str) -> tuple[Any, ...]:
    """The objects governance reads for a role."""
    return (
        state.contexts[role_id],
        state.rls_contexts.get(role_id),
        state.roles.get(role_id),
        state.masking_rules,
        state.tables,
    )


def acting_role_set() -> tuple[str, ...]:
    """The roles the caller is acting as beyond the one the statement runs as (REQ-1620, "Role:
    All"), in a canonical order — empty for a single role. Domain access is the union over that
    set (``security.rights.effective_domain_access_role``), so it is an input of governance."""
    from provisa.core.request_context import current_role_claims

    claims = current_role_claims.get()
    return tuple(sorted(set(claims))) if claims and len(claims) > 1 else ()


def plan_key(state: Any, stage: str, role_id: str, *body: Any) -> str:
    """``stage`` + generation + role + acting-role set + person verbatim; ``body`` (unbounded,
    caller-authored) hashed. Two stages never share a key, so neither can read the other's value.
    The acting-role set is in the key because a statement admitted under a wider set's domain
    access must never be served to a narrower one."""
    identity = current_audit_identity()
    digest = hashlib.sha256(
        json.dumps(body, sort_keys=True, default=str, separators=(",", ":")).encode()
    ).hexdigest()
    return "\x00".join(
        [
            stage,
            str(state.schema_boot_id),
            str(state.schema_version),
            role_id,
            ",".join(acting_role_set()),
            identity.user_id if identity is not None else "",
            digest,
        ]
    )


@dataclass(frozen=True)
class _Kept:
    value: Any
    anchors: tuple[Any, ...]


class PlanSlot:
    """One request's place in the plan store: look the plan up, or keep the one it builds."""

    def __init__(
        self,
        state: Any,
        stage: str,
        role_id: str,
        *body: Any,
        extra_anchors: tuple[Any, ...] = (),
    ) -> None:
        self._state = state
        self._key = plan_key(state, stage, role_id, *body)
        self._anchors = (*governance_anchors(state, role_id), *extra_anchors)
        self._generation = (state.schema_boot_id, state.schema_version)
        self._rebuilding_at_start = _rebuild_in_progress()

    def cached(self) -> Any:
        """The kept plan, or None when there is none or its governance objects were replaced."""
        kept = self._state.compiled_query_cache.get(self._key)
        if not isinstance(kept, _Kept) or len(kept.anchors) != len(self._anchors):
            return None
        if not all(a is b for a, b in zip(kept.anchors, self._anchors)):
            return None
        return kept.value

    def keep(self, value: Any) -> None:
        state = self._state
        if self._rebuilding_at_start or _rebuild_in_progress():
            return
        if (state.schema_boot_id, state.schema_version) != self._generation:
            return
        state.compiled_query_cache.put(self._key, _Kept(value, self._anchors))
