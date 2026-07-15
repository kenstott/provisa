# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The two INDEPENDENT operator-declared MV outcomes (REQ-965).

An MV declares TWO decoupled things, not one:

1. a PERSISTENCE outcome — a single choice of how the recomputed result is applied to the MV's OWN
   store table: ``replace`` / ``append`` / ``upsert`` (executed by land_replace / land_append /
   apply_cdc in ``materialize_exec``).
2. a SET of EMIT outcomes — per fire the MV may emit ANY, ALL, or NONE of {replace, append, delta}
   downstream, each shape ROUTED only to the dependents that consume it. Emission is DEMAND-DRIVEN
   (pay-per-consumer): a shape is produced only if a dependent subscribes to it, so delta's diff
   cost is paid ONLY when delta has a subscriber.

The two axes are decoupled along both cost and semantics (persist=upsert may emit=delta;
persist=replace may emit=append). The system NEVER infers a shape nor silently downgrades: an
undeclared/invalid outcome, or emit=delta / persist=upsert without a PK, is an EXPLICIT ERROR — never
a silent fallback (REQ-964).
"""

from __future__ import annotations

# Persistence outcomes (the single axis): how the compute result is applied to the MV's own store.
PERSIST_REPLACE = "replace"
PERSIST_APPEND = "append"
PERSIST_UPSERT = "upsert"
PERSIST_OUTCOMES = frozenset({PERSIST_REPLACE, PERSIST_APPEND, PERSIST_UPSERT})

# Emit outcomes (the set axis): the downstream-facing shapes. warn/error are emitted through their
# own channels (REQ-957), not part of the declared data-shape set.
EMIT_REPLACE = "replace"
EMIT_APPEND = "append"
EMIT_DELTA = "delta"
EMIT_OUTCOMES = frozenset({EMIT_REPLACE, EMIT_APPEND, EMIT_DELTA})

# A PK is REQUIRED for these outcomes — there is no row identity to upsert/diff without it.
_PERSIST_NEEDS_PK = frozenset({PERSIST_UPSERT})
_EMIT_NEEDS_PK = frozenset({EMIT_DELTA})


def validate_persist(persist: str) -> str:
    """The persistence outcome, validated. Raises on an undeclared/invalid value (never a fallback)."""
    if persist not in PERSIST_OUTCOMES:
        raise ValueError(
            f"invalid persistence outcome {persist!r}; expected one of {sorted(PERSIST_OUTCOMES)}"
        )
    return persist


def validate_emit(emit: set[str] | frozenset[str]) -> frozenset[str]:
    """The declared emit-outcome SET, validated. An unknown shape is an explicit error. An empty set
    is valid (emit NONE — a terminal MV that persists but tells no one)."""
    bad = set(emit) - EMIT_OUTCOMES
    if bad:
        raise ValueError(
            f"invalid emit outcome(s) {sorted(bad)}; expected a subset of {sorted(EMIT_OUTCOMES)}"
        )
    return frozenset(emit)


def require_pk(persist: str, emit: set[str] | frozenset[str], pk_columns: list[str] | None) -> None:
    """Fail loud when a declared outcome on EITHER axis needs a PK and none is derivable (REQ-965 /
    REQ-970): persist=upsert or emit=delta requires row identity. Never silently downgrades to
    replace — that would hide churn and break provability."""
    if pk_columns:
        return
    needs = set()
    if persist in _PERSIST_NEEDS_PK:
        needs.add(f"persist={persist}")
    for shape in emit:
        if shape in _EMIT_NEEDS_PK:
            needs.add(f"emit={shape}")
    if needs:
        raise ValueError(
            f"{sorted(needs)} require a primary key for row identity, but none is declared or "
            f"derivable — refusing to silently emit/persist a coarser shape (REQ-965)"
        )


def resolve_emitted(
    declared: set[str] | frozenset[str], subscribed: set[str] | frozenset[str]
) -> list[str]:
    """DEMAND-DRIVEN emit resolution (REQ-965): the shapes actually produced this fire = the declared
    set INTERSECTED with the shapes a downstream dependent subscribes to. A declared shape with no
    subscriber is NOT produced (pay-per-consumer — delta's diff cost is paid only when delta has a
    consumer). Returned in a stable (sorted) order."""
    return sorted(validate_emit(declared) & set(subscribed))
