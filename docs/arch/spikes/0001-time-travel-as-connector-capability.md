# SPIKE 0001 — Time travel as a connector capability

Status: Open (timebox: one design pass; deliverable is a decision, not code)
Requirement: REQ-1366 (proposed). Related: REQ-467, REQ-825, REQ-826, REQ-827,
REQ-841.

Spikes live in this directory and follow the ADR shell (`../adr/`): numbered file,
Status header, Context, then Question / Findings / Outcome instead of a Decision. A
spike closes by producing or amending a requirement, or by recording why not.

## Context

Time travel today is a source-type special case. REQ-467 hardwires it to
Iceberg/Delta via `TIME_TRAVEL_SOURCES` (`provisa/core/source_registry.py`), and the
compiler both gates and emits it as a Trino-only string suffix
(`provisa/compiler/sql_gen.py:394-412`, `FOR TIMESTAMP/VERSION AS OF`). The
connector `Capability` surface (`provisa/federation/connector_base.py:70`) carries
predicate/join/aggregate pushdown and `write` — no time-travel trait — so the PLAN
stage cannot see or route on it.

The federation-engine abstraction (REQ-825/840/841) defines engines by capability
profile per (engine, source_type). Time travel is a capability of exactly that
shape: whether `as_of` works depends on the source keeping history AND the
engine's connector being able to address it, and the MATERIALIZED strategy
(REQ-826) destroys native history because the copy is one frozen snapshot.

## Question

Can time travel be generalized from a compiler special case to a connector
capability — and is it merely a gate, or operational?

## Findings

Three escalating degrees, each feasible at a different cost:

1. **Gate.** Add `time_travel` to `Capability`; replace the `TIME_TRAVEL_SOURCES`
   check. Wrinkle: capability is per (engine, source_type) and the compiler is
   engine-blind, so the gate moves from compile to the PLAN stage — where REQ-825
   already puts engine-aware decisions. Trivial.

2. **Connector-owned rendering.** `as_of` becomes a neutral IR attribute; stage-4
   codegen asks the connector to render it. The mechanism differs structurally per
   engine, not just syntactically: Trino is a table-suffix clause; DuckDB Iceberg
   is a scan-function argument (`iceberg_scan(..., snapshot_from_timestamp=)`)
   that normally lives in view DDL, so a per-query `as_of` needs an inline
   table-ref substitution, not a clause append. Rides the REQ-825 REMAINING
   codegen-to-stage-4 refactor. Moderate.

3. **Synthesized time travel.** For MATERIALIZED sources, retain N snapshots in
   the materialization store instead of replace-style lands
   (`provisa/federation/store_writer.py` `land_replace`/`land_ctas`) and resolve
   `as_of` to a retained snapshot in the PLAN prep phase. The capability becomes
   three-valued: `native | synthesized | none`. Provisa then grants time travel to
   sources that never had it (REST APIs, MongoDB) — a semantic-layer feature no
   single engine offers. Nearly free on an Iceberg-backed store (snapshot history
   is native; add a retention knob); a Postgres-backed store would need versioned
   tables plus retention/vacuum machinery, so degree 3 is scoped to the Iceberg
   store backend only.

Cross-cutting: governance-parity conformance (REQ-827) must cover `as_of` —
RLS/masking over historical rows must diff identically per engine, and synthesized
snapshots are governed at read time (stage 3 governs whatever relation is read),
never at capture time; otherwise a role change would leak pre-change data.

## Outcome

Pending. Expected close: degrees 1+2 land with the REQ-825 pipeline work; degree 3
gets a scoped design on the Iceberg store backend.
