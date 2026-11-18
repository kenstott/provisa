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

The count before Run (:func:`count`) reads no table and asks no source: it is computed from the
model store alone and is an upper bound on the requests the run makes
(:func:`provisa.discovery.windows.bound`).

The run's model defaults to the model assigned to relationship inference and may be any model of a
vendor with a key, for that run only (the assignment is never changed). Its limits (REQ-464) size
the windows. Foreign keys are read from the source tables' own constraints; semantics are asked of
the model. Suggestions are settled (:func:`provisa.discovery.direction.settle`) and stored as each
request returns, so they appear while the run goes on.
"""

# Requirements: REQ-413, REQ-464, REQ-1789

from __future__ import annotations

import asyncio
import copy
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any, TypeVar, cast

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
    reduced_tables,
    settle,
)
from provisa.discovery.measure import Measurer, governed_reader
from provisa.discovery.proposals import BELOW_THRESHOLD, Recorded
from provisa.discovery.runner import Control, KeyValues, Rescore, Tally, execute
from provisa.discovery.windows import Request, bound, plan

if TYPE_CHECKING:
    from provisa.core.database import Connection

#: The operation whose assigned model a run uses unless it names another.
OPERATION = "relationship_inference"
SYSTEM_PROMPT = "You are a data analyst."

#: While a request is in flight the run says this often that it is still going: a request may take
#: longer (provisa.llm.client, ``call_timeout_s``) than a run may go without moving on
#: (provisa.discovery.runs.STALLED_AFTER).
STILL_GOING_EVERY_S = 60.0

_T = TypeVar("_T")


async def while_saying_still_going(
    work: Awaitable[_T], still_going: Callable[[], Awaitable[None]]
) -> _T:
    """``work``'s result; ``still_going`` is told every :data:`STILL_GOING_EVERY_S` until it ends.
    When ``still_going`` raises (the run has ended), ``work`` is left and the error is raised."""
    task = asyncio.ensure_future(work)
    try:
        while True:
            done, _ = await asyncio.wait({task}, timeout=STILL_GOING_EVERY_S)
            if done:
                return task.result()
            await still_going()
    finally:
        task.cancel()


@dataclass(frozen=True)
class KeyValueOverlap:
    """Key value overlap's settings for a run (REQ-413, amended 2026-11-16): the semantic pairs
    whose model confidence is at ``cutoff`` or more and under the run's threshold are measured, each
    column read for at most ``sample_size`` distinct values."""

    cutoff: float
    sample_size: int


@dataclass(frozen=True, kw_only=True)
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
    #: The role of the person who started the run: each proposed pair's values are read as it
    #: (:mod:`provisa.discovery.measure`).
    role: str
    #: Key value overlap, chosen only with semantics; None: not chosen, and no pair is measured.
    key_values: KeyValueOverlap | None
    #: The tick threshold of the person who started the run, taken when it started and recorded
    #: with it (REQ-413, amended 2026-11-18): the model is told it, a proposal at it or above is
    #: kept unmeasured, and the key value overlap band runs from the cutoff up to it.
    threshold: float


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

    def sizes(self) -> dict[str, int]:
        return sizes(self.source, self.target)


def sizes(source: list[TableFacts], target: list[TableFacts] | None) -> dict[str, int]:
    """A run's sets as the count and the result give them: the tables of each, and how many of
    the run's tables are never read (REQ-1986: each requires a parameter and is not kept for
    good, so it takes part on its registration alone)."""
    tables = {t.table_id: t for t in [*source, *(target or [])]}
    return {
        "source_tables": len(source),
        "target_tables": len(source if target is None else target),
        "tables_not_read": sum(1 for t in tables.values() if t.reduced),
    }


def _left_out(named: tuple[int, ...] | None, kept: list[TableFacts]) -> int:
    """How many of the tables chosen by name a set left out; none were when none were named."""
    return 0 if named is None else len(set(named)) - len(kept)


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


#: What the count's ``requests`` are (REQ-413): the most a run of the scope makes, never fewer
#: than it makes. The count reads no table, so what a run reads can only take requests away.
UPPER_BOUND = "upper_bound"


async def count(state: Any, spec: RunSpec, model: ChosenModel | None) -> dict[str, Any]:
    """The request count before Run (REQ-413), by group, for the chosen model: an upper bound,
    from the model store alone. No table is read, classified or sampled and no source is asked;
    with no model (foreign keys alone) there is no request to count."""
    async with state.model_db.acquire() as _conn:
        conn = cast("Connection", _conn)
        source_ids, target_ids = await table_sets(conn, spec)
        ids = set(source_ids) | set(target_ids or [])
        facts = {t.table_id: t for t in await gather.registered_facts(state, conn, sorted(ids))}
    source = [facts[i] for i in source_ids]
    target = None if target_ids is None else [facts[i] for i in target_ids]
    k, by_group = (
        (0, {}) if model is None else bound(source, target, model.input_tokens, model.output_tokens)
    )
    return {
        "requests": sum(by_group.values()),
        "requests_are": UPPER_BOUND,
        "by_group": by_group,
        "suggestions_per_request": k,
        "model": None if model is None else model.describe(),
        **sizes(source, target),
    }


def _candidate(p: Proposal) -> RelationshipCandidate:
    return RelationshipCandidate(
        source_table_id=p.start[0],
        source_column=p.start[1],
        target_table_id=p.end[0],
        target_column=p.end[1],
        cardinality=p.cardinality,
        confidence=p.rank,
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
    """What semantics did, and when it suggests none, why. Every request failing is not the model
    proposing none: it is ``requests_failed`` (``unreadable`` when each answer was unreadable)."""
    shown = sum(1 for p in outcome.kept if p.kind == "semantics")
    if shown:
        empty = None
    elif tally.done and len(tally.failed) == tally.done and tally.unreadable < tally.done:
        empty = "requests_failed"
    elif tally.proposed == 0 and tally.unreadable:
        empty = "unreadable"
    elif tally.proposed == 0:
        empty = "none_proposed"
    elif tally.below_threshold and tally.below_threshold + tally.invalid == tally.proposed:
        empty = BELOW_THRESHOLD
    else:
        empty = "filtered"
    return {"suggested": shown, "empty_reason": empty, **tally.as_dict()}


async def _refiner(state: Any, tables: dict[int, TableFacts]) -> Rescore | None:
    """REQ-1789: with a Jev key configured, each proposal is also scored by Jev, on what the run
    knows of its tables and columns; Jev's score is shown beside the model's."""
    from provisa.core.org_secrets import JEV, org_ai_key_or_none

    jev_key = await org_ai_key_or_none(state.model_db, JEV)
    if not jev_key:
        return None
    from provisa.discovery.rescoring import jev_scores

    async def refine(found: list[Proposal]) -> list[float | None] | None:
        return await jev_scores(found, tables, jev_key)

    return refine


def _key_values(
    state: Any, spec: RunSpec, tables: dict[int, TableFacts], known: Known
) -> KeyValues | None:
    """The run's key-value measure when Key value overlap is chosen: each band pair's values read
    as the person who started the run, at most the sample size of them per column. A pair already
    registered or decided is never measured as a new one (REQ-413)."""
    if spec.key_values is None:
        return None
    measurer = Measurer(
        governed_reader(state, spec.role),
        tables,
        decided=known.existing | known.rejected,
        bound=spec.key_values.sample_size,
    )
    return KeyValues(measurer.all, spec.key_values.cutoff)


#: Every proposal a run has read so far, to be kept with it.
RecordSink = Callable[[list[Recorded]], Awaitable[None]]


async def perform(
    state: Any,
    spec: RunSpec,
    model: ChosenModel | None,
    prepared: Prepared,
    *,
    scope: str,
    progress: ProgressSink,
    stop_asked: StopCheck,
    record: RecordSink,
) -> dict[str, Any]:
    """Run the kinds asked over ``prepared``; ``progress`` is told after every step, and the run
    stops after the request in flight once ``stop_asked`` says so. The result says what each
    kind found and what was removed. ``record`` is given every proposal read so far, shown or
    left out with why, each time the suggestions are settled."""
    pool = state.model_db
    async with pool.acquire() as _conn:
        conn = cast("Connection", _conn)
        known: Known = await gather.known(conn)
        fk_found: list[RelationshipCandidate] = []
        if "foreign_keys" in spec.kinds:
            fk_found = await collect_fk_for_tables(
                state.source_pools, state.source_types, conn, prepared.source_ids
            )
    tables = prepared.tables
    # Declared foreign keys are the first check and are never measured (REQ-413, amended
    # 2026-11-16): only semantic pairs in the band are (:func:`provisa.discovery.runner.judged`).
    fk = _fk_proposals(fk_found)
    publisher = Publisher(pool, scope)
    read_sample = _caching(gather.uniqueness_reader(state))

    def registered(read: Recorded) -> Recorded:
        """``read``, said to be already registered or rejected where it is: a proposal left out
        as an answer was read (the model rated it low, say) may be one of a relationship that
        exists or a pair a reviewer rejected, and is then not one a reviewer can still decide."""
        p = read.proposal
        if p is None:
            return read
        reverse = (p.end[0], p.end[1], p.start[0], p.start[1])
        if p.edge in known.existing or reverse in known.existing:
            return replace(read, left_out="existing")
        # REQ-413 (amended 2026-11-16): a pair marked not to be recommended again keeps that
        # decision, whatever this run's answer rated it.
        if p.edge in known.rejected or reverse in known.rejected:
            return replace(read, left_out="rejected")
        return read

    async def settled(semantic: list[Proposal], unread: list[Recorded]) -> Outcome:
        outcome = await settle(
            [*fk, *semantic], tables, prepared.source_ids, prepared.target_ids, known, read_sample
        )
        await publisher.publish(outcome.kept)
        await record(
            [
                *(Recorded(p.kind, None, p) for p in outcome.kept),
                *(registered(r) for r in unread),
                *(Recorded(o.proposal.kind, o.reason, o.proposal) for o in outcome.left_out),
            ]
        )
        return outcome

    def report(tally: Tally | None, outcome: Outcome) -> dict[str, Any]:
        out: dict[str, Any] = {
            "suggestions": len(outcome.kept),
            "removed": outcome.removed.as_dict(),
            # The run's sets: what it compared, so what it did not is plain.
            **prepared.sizes(),
            "target_same_as_source": prepared.target is None,
            "source_domains": sorted({t.domain_id for t in prepared.source}),
            "target_domains": sorted({t.domain_id for t in prepared.target or prepared.source}),
            # Tables chosen by name that the run left out (a system domain's, one out of service).
            "source_left_out": _left_out(spec.source_tables, prepared.source),
            "target_left_out": (
                0 if spec.same_as_source else _left_out(spec.target_tables, prepared.target or [])
            ),
            "decided_by": dict(outcome.decided_by),
            "stored_ids": sorted(publisher.stored.values()),
            # REQ-1986: suggestions with a table that is never read, so no values were compared.
            "on_reduced_information": sum(1 for p in outcome.kept if reduced_tables(p, tables)),
        }
        if "foreign_keys" in spec.kinds:
            out["foreign_keys"] = {
                "found": len(fk_found),
                "suggested": sum(1 for p in outcome.kept if p.kind == "foreign_keys"),
            }
        if tally is not None:
            out["semantics"] = _semantics_report(tally, outcome)
        if spec.key_values is not None:
            # REQ-413 (amended 2026-11-16): the run's summary states Key value overlap's settings.
            out["key_values"] = {
                "cutoff": spec.key_values.cutoff,
                "sample_size": spec.key_values.sample_size,
            }
        if model is not None:
            out["model"] = model.describe()
        return out

    outcome = await settled([], [])
    latest = report(
        Tally(total=len(prepared.requests), threshold=spec.threshold) if model else None, outcome
    )
    await progress(latest)
    if model is None:
        return report(None, outcome)

    class _Control(Control):
        async def stop_asked(self) -> bool:
            return await stop_asked()

        async def returned(self, tally: Tally, proposals: list[Proposal]) -> None:
            nonlocal outcome, latest
            outcome = await settled(proposals, tally.left_out)
            latest = report(tally, outcome)
            await progress(latest)

    def ask(prompt: str, max_tokens: int) -> Any:
        return model.client.complete_sync_detailed(prompt, SYSTEM_PROMPT, max_tokens)

    async def in_flight(call: Callable[..., Any], *args: Any) -> Any:
        return await while_saying_still_going(
            asyncio.to_thread(call, *args), lambda: progress(latest)
        )

    tally, proposals = await execute(
        prepared.requests,
        prepared.k,
        model.output_tokens,
        ask,
        _Control(),
        threshold=spec.threshold,
        key_values=_key_values(state, spec, tables, known),
        refine=await _refiner(state, tables),
        to_thread=in_flight,
    )
    outcome = await settled(proposals, tally.left_out)
    return report(tally, outcome)
