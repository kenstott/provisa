# Copyright (c) 2026 Kenneth Stott
# Canary: 7f3e9b05-2c84-4d1a-96e7-e0b4a8d5c217
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A relationship discovery run (REQ-413): the kinds asked, the model chosen, a source set and a
target set; its request count before it runs; its work, reported as each request returns.

The run's model defaults to the model assigned to relationship inference and may be any model of a
vendor with a key, for that run only (the assignment is never changed). Its limits (REQ-464) size
the windows. Foreign keys are read from the source tables' own constraints; semantics are asked of
the model. Suggestions are settled (:func:`provisa.discovery.direction.settle`) and stored as each
request returns, so they appear while the run goes on.
"""

# Requirements: REQ-413, REQ-464, REQ-1789

from __future__ import annotations

import copy
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, cast

from provisa.discovery import candidates as candidates_repo
from provisa.discovery import gather
from provisa.discovery.analyzer import RelationshipCandidate
from provisa.discovery.collector import collect_fk_for_tables
from provisa.discovery.columns import TableFacts
from provisa.discovery.direction import (
    Edge,
    Kind,
    Known,
    Outcome,
    Proposal,
    Sampled,
    from_answer,
    settle,
)
from provisa.discovery.runner import MIN_CONFIDENCE, Control, Tally, execute
from provisa.discovery.windows import Request, count_by_group, plan

if TYPE_CHECKING:
    from provisa.core.database import Connection

#: The operation whose assigned model a run uses unless it names another.
OPERATION = "relationship_inference"
SYSTEM_PROMPT = "You are a data analyst."


@dataclass(frozen=True)
class RunSpec:
    """What a run is asked for. ``*_tables`` None: every table of the chosen domains. With
    ``same_as_source`` the target set is the source set."""

    kinds: frozenset[Kind]
    source_domains: tuple[str, ...]
    source_tables: tuple[int, ...] | None
    target_domains: tuple[str, ...] = ()
    target_tables: tuple[int, ...] | None = None
    same_as_source: bool = True
    #: (vendor, model) for this run only; None: the model assigned to relationship inference.
    model: tuple[str, str] | None = None


@dataclass
class ChosenModel:
    vendor: str
    model: str
    #: Why it cannot run (its vendor has no key); None when it can.
    problem: str | None
    input_tokens: int
    output_tokens: int
    limits_source: str
    client: Any

    @property
    def label(self) -> str:
        return f"{self.vendor}:{self.model}"

    def describe(self) -> dict[str, Any]:
        return {
            "vendor": self.vendor,
            "model": self.model,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "limits_source": self.limits_source,
        }


def choose_model(
    config: dict[str, Any], api_keys: dict[str, str], choice: tuple[str, str] | None
) -> ChosenModel:
    """The run's model: the one assigned to relationship inference, or ``choice`` for this run
    only, in a copy of the resolved config so the assignment is never changed."""
    from provisa.llm.client import ProvisaLLMClient

    cfg = config
    if choice is not None:
        cfg = copy.deepcopy(config)
        cfg.setdefault("ai_models", {})[OPERATION] = {"vendor": choice[0], "model": choice[1]}
    client = ProvisaLLMClient(OPERATION, config=cfg, api_keys=api_keys)
    limits = client.limits()
    return ChosenModel(
        client.vendor,
        client.model,
        client.key_problem(),
        limits.input_tokens,
        limits.output_tokens,
        limits.source,
        client,
    )


@dataclass
class Prepared:
    source: list[TableFacts]
    #: None: the target set is the source set.
    target: list[TableFacts] | None
    k: int
    requests: list[Request]

    @property
    def tables(self) -> dict[int, TableFacts]:
        out = {t.table_id: t for t in self.source}
        out.update({t.table_id: t for t in self.target or []})
        return out

    @property
    def source_ids(self) -> set[int]:
        return {t.table_id for t in self.source}

    @property
    def target_ids(self) -> set[int]:
        return self.source_ids if self.target is None else {t.table_id for t in self.target}


async def table_sets(conn: "Connection", spec: RunSpec) -> tuple[list[int], list[int] | None]:
    """The ids of the source set and of the target set (None: the same as the source)."""
    source = await gather.scope_table_ids(conn, spec.source_domains, spec.source_tables)
    if spec.same_as_source:
        return source, None
    target = await gather.scope_table_ids(conn, spec.target_domains, spec.target_tables)
    return source, target


async def prepare(
    state: Any,
    spec: RunSpec,
    model: ChosenModel | None,
    on_table: Callable[[int, int], Awaitable[None]] | None = None,
) -> Prepared:
    """The run's tables with their facts, and, when semantics is asked, its requests;
    ``on_table(read, total)`` is told after each table is read."""
    async with state.model_db.acquire() as _conn:
        conn = cast("Connection", _conn)
        source_ids, target_ids = await table_sets(conn, spec)
        ids = set(source_ids) | set(target_ids or [])
        facts = {
            t.table_id: t for t in await gather.table_facts(state, conn, sorted(ids), on_table)
        }
    source = [facts[i] for i in source_ids]
    target = None if target_ids is None else [facts[i] for i in target_ids]
    if model is None:
        return Prepared(source, target, 0, [])
    k, requests = plan(source, target, model.input_tokens, model.output_tokens)
    return Prepared(source, target, k, requests)


def count(prepared: Prepared, model: ChosenModel) -> dict[str, Any]:
    """The request count before Run (REQ-413), by group, for the chosen model."""
    return {
        "requests": len(prepared.requests),
        "by_group": count_by_group(prepared.requests),
        "suggestions_per_request": prepared.k,
        "model": model.describe(),
        "source_tables": len(prepared.source),
        "target_tables": len(prepared.target if prepared.target is not None else prepared.source),
    }


def _candidate(p: Proposal) -> RelationshipCandidate:
    return RelationshipCandidate(
        source_table_id=p.start[0],
        source_column=p.start[1],
        target_table_id=p.end[0],
        target_column=p.end[1],
        cardinality="many-to-one",
        confidence=p.confidence,
        reasoning=p.reasoning,
        suggested_name=p.suggested_name,
    )


@dataclass
class Publisher:
    """Keeps the stored suggestions of one run equal to what it shows."""

    pool: Any
    scope: str
    stored: dict[Edge, int] = field(default_factory=dict)

    async def publish(self, shown: list[Proposal]) -> None:
        wanted = {p.edge: p for p in shown}
        fresh = [p for e, p in wanted.items() if e not in self.stored]
        gone = [e for e in self.stored if e not in wanted]
        async with self.pool.acquire() as _conn:
            conn = cast("Connection", _conn)
            if gone:
                await candidates_repo.withdraw(conn, [self.stored.pop(e) for e in gone])
            if fresh:
                ids = await candidates_repo.store_candidates(
                    conn, [_candidate(p) for p in fresh], self.scope
                )
                self.stored.update({p.edge: i for p, i in zip(fresh, ids, strict=True)})


ProgressSink = Callable[[dict[str, Any]], Awaitable[None]]
StopCheck = Callable[[], Awaitable[bool]]


def _fk_proposals(found: list[RelationshipCandidate]) -> list[Proposal]:
    return [
        from_answer(
            "foreign_keys",
            (c.source_table_id, c.source_column),
            (c.target_table_id, c.target_column),
            "many-to-one",
            1.0,
            c.reasoning,
            c.suggested_name,
        )
        for c in found
    ]


def _caching(read: Callable[[TableFacts, str], Awaitable[Sampled]]):
    cache: dict[tuple[int, str], Sampled] = {}

    async def cached(table: TableFacts, column: str) -> Sampled:
        key = (table.table_id, column)
        if key not in cache:
            cache[key] = await read(table, column)
        return cache[key]

    return cached


def _semantics_report(tally: Tally, outcome: Outcome) -> dict[str, Any]:
    shown = sum(1 for p in outcome.kept if p.kind == "semantics")
    if shown:
        empty = None
    elif tally.proposed == 0 and tally.unreadable:
        empty = "unreadable"
    elif tally.proposed == 0:
        empty = "none_proposed"
    elif tally.below_threshold and tally.below_threshold + tally.invalid == tally.proposed:
        empty = "below_threshold"
    else:
        empty = "filtered"
    return {"suggested": shown, "empty_reason": empty, **tally.as_dict()}


async def _refiner(state: Any) -> Callable[[list[Proposal]], Awaitable[list[Proposal]]] | None:
    """REQ-1789: with a Jev key configured, each suggestion's confidence is Jev's calibrated score;
    those it drops fell below the threshold."""
    from provisa.core.org_secrets import resolve_jev_api_key

    jev_key = await resolve_jev_api_key(state.model_db)
    if not jev_key:
        return None
    from provisa.jev.decisions import refine_relationship_confidence

    async def refine(found: list[Proposal]) -> list[Proposal]:
        by_edge = {p.edge: p for p in found}
        refined = await refine_relationship_confidence(
            [_candidate(p) for p in found], jev_key, min_confidence=MIN_CONFIDENCE
        )
        out = []
        for c in refined:
            p = by_edge[(c.source_table_id, c.source_column, c.target_table_id, c.target_column)]
            out.append(
                Proposal(
                    p.kind, p.start, p.end, p.written, c.confidence, p.reasoning, p.suggested_name
                )
            )
        return out

    return refine


async def perform(
    state: Any,
    spec: RunSpec,
    model: ChosenModel | None,
    prepared: Prepared,
    *,
    scope: str,
    progress: ProgressSink,
    stop_asked: StopCheck,
) -> dict[str, Any]:
    """Run the kinds asked over ``prepared``; ``progress`` is told after every step, and the run
    stops after the request in flight once ``stop_asked`` says so. The result says what each
    kind found and what was removed."""
    pool = state.model_db
    async with pool.acquire() as _conn:
        conn = cast("Connection", _conn)
        known: Known = await gather.known(conn)
        fk_found: list[RelationshipCandidate] = []
        if "foreign_keys" in spec.kinds:
            fk_found = await collect_fk_for_tables(
                state.source_pools, state.source_types, conn, prepared.source_ids
            )
    fk = _fk_proposals(fk_found)
    publisher = Publisher(pool, scope)
    read_sample = _caching(gather.uniqueness_reader(state))
    tables = prepared.tables

    async def settled(semantic: list[Proposal]) -> Outcome:
        outcome = await settle(
            [*fk, *semantic], tables, prepared.source_ids, prepared.target_ids, known, read_sample
        )
        await publisher.publish(outcome.kept)
        return outcome

    def report(tally: Tally | None, outcome: Outcome) -> dict[str, Any]:
        out: dict[str, Any] = {
            "suggestions": len(outcome.kept),
            "removed": outcome.removed.as_dict(),
            "decided_by": dict(outcome.decided_by),
            "stored_ids": sorted(publisher.stored.values()),
        }
        if "foreign_keys" in spec.kinds:
            out["foreign_keys"] = {
                "found": len(fk_found),
                "suggested": sum(1 for p in outcome.kept if p.kind == "foreign_keys"),
            }
        if tally is not None:
            out["semantics"] = _semantics_report(tally, outcome)
        if model is not None:
            out["model"] = model.describe()
        return out

    outcome = await settled([])
    await progress(report(Tally(total=len(prepared.requests)) if model else None, outcome))
    if model is None:
        return report(None, outcome)

    class _Control(Control):
        async def stop_asked(self) -> bool:
            return await stop_asked()

        async def returned(self, tally: Tally, proposals: list[Proposal]) -> None:
            nonlocal outcome
            outcome = await settled(proposals)
            await progress(report(tally, outcome))

    def ask(prompt: str, max_tokens: int) -> Any:
        return model.client.complete_sync_detailed(prompt, SYSTEM_PROMPT, max_tokens)

    tally, proposals = await execute(
        prepared.requests,
        prepared.k,
        model.output_tokens,
        ask,
        _Control(),
        refine=await _refiner(state),
    )
    outcome = await settled(proposals)
    return report(tally, outcome)
