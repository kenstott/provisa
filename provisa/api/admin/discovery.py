# Copyright (c) 2026 Kenneth Stott
# Canary: 1971faf0-92e4-46f1-8075-f8a18f370534
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Admin API router for LLM relationship discovery."""

# Requirements: REQ-018, REQ-167, REQ-413, REQ-612, REQ-2253

from __future__ import annotations

import asyncio
import logging as _logging
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Annotated, Any, Literal, cast

from fastapi import APIRouter, Query, Request
from pydantic import BaseModel, ConfigDict, Field

from provisa.api.admin import discovery_dimension, join_through
from provisa.api.app import state
from provisa.api.errors import ApiError
from provisa.core.relationship_naming import RelationshipNameTaken
from provisa.discovery import candidates as candidates_repo
from provisa.discovery import gather
from provisa.discovery import proposals as proposals_repo
from provisa.discovery import runs as runs_repo
from provisa.discovery import threshold as threshold_repo
from provisa.discovery.direction import Kind
from provisa.discovery.run import (
    ChosenModel,
    KeyValueOverlap,
    RunSpec,
    choose_model,
    count,
    perform,
    prepare,
    table_sets,
)
from provisa.discovery.windows import NoRoomError
from provisa.otel_compat import get_tracer as _get_tracer
from provisa.otel_compat import stage as _stage
from provisa.api.admin.capabilities import require_capability_request, selected_domains_request
from provisa.api.engine_error_names import client_text

if TYPE_CHECKING:
    from provisa.core.database import Connection

_tracer = _get_tracer(__name__)

router = APIRouter(prefix="/admin/discover")

# REQ-2253: the navbar filter's selection, as repeated ``domains=`` parameters
# (capabilities.selected_domains_request). Annotated so a handler awaited directly defaults to
# None, not to the Query marker.
SelectedDomains = Annotated[list[str] | None, Query()]

#: The kinds of discovery a run can be asked for: foreign keys read from the sources' own
#: constraints, and semantics inferred by the model assigned to relationship inference.
DiscoveryKind = Literal["foreign_keys", "semantics"]

#: What a run the dialog starts can look for: the kinds, and Key value overlap, the measure of the
#: semantic pairs in the band between its cutoff and the threshold (REQ-413, amended 2026-11-16),
#: chosen only with semantics.
RunKind = Literal["foreign_keys", "semantics", "key_values"]
KEY_VALUES = "key_values"

#: Where a model and its key are set, as the admin UI names it.
WHERE_TO_SET = "Admin, AI Models"


class DiscoverRequest(BaseModel):
    """A discovery done while the caller waits, as a run (REQ-413): ``table`` compares the tables
    of the table's domain, ``domain`` those of the domain, ``cross-domain`` those of every
    domain. A field the route does not read is refused, never ignored; chosen domains or tables
    are a run's (``RunScope``, ``POST /admin/discover/runs``)."""

    model_config = ConfigDict(extra="forbid")

    scope: Literal["table", "domain", "cross-domain"]
    kinds: list[DiscoveryKind] = Field(min_length=1)
    table_id: int | None = None
    domain_id: str | None = None


class RejectRequest(BaseModel):
    reason: str


class AcceptRequest(BaseModel):
    name: str | None = None


class AcceptEach(BaseModel):
    """One proposal to accept, with the name its relationship takes."""

    id: int
    name: str | None = None


_log = _logging.getLogger(__name__)


@dataclass
class SemanticModel:  # REQ-413
    """The model assigned to relationship inference in the acting org, and why it cannot run
    (``problem``), judged by that model's own key; None when it can."""

    vendor: str
    model: str
    problem: str | None
    config: dict[str, Any]
    api_keys: dict[str, str]

    @property
    def label(self) -> str:
        return f"{self.vendor}:{self.model}"


async def semantic_model(pool: Any) -> SemanticModel:  # REQ-413
    """The acting org's model for relationship inference, with its config and keys resolved the
    way every org-bound model call resolves them."""
    from provisa.core.org_secrets import read_org_api_keys
    from provisa.core.org_settings import resolve_org_config
    from provisa.llm.client import ProvisaLLMClient

    config = await resolve_org_config(pool)
    api_keys = await read_org_api_keys(pool)
    client = ProvisaLLMClient("relationship_inference", config=config, api_keys=api_keys)
    return SemanticModel(client.vendor, client.model, client.key_problem(), config, api_keys)


def _semantics_availability(model: SemanticModel) -> dict[str, Any]:
    return {
        "available": model.problem is None,
        "model": model.label,
        "reason": None if model.problem is None else "no_key",
    }


@router.get("/kinds")
async def discovery_kinds(request: Request):  # REQ-413
    """Which kinds of discovery can run: foreign keys always; semantics when the model assigned to
    relationship inference has its key."""
    require_capability_request(request, "create_relationship")
    pool = state.model_db
    assert pool is not None
    return {
        "foreign_keys": {"available": True},
        "semantics": _semantics_availability(await semantic_model(pool)),
    }


class RunScope(BaseModel):  # REQ-413
    """A run's kinds, model and sets, as the dialog sends them. Tables None: every table of the
    chosen domains. ``model`` None: the model assigned to relationship inference."""

    kinds: list[RunKind] = Field(min_length=1)
    source_domains: list[str] = Field(default_factory=list)
    source_tables: list[int] | None = None
    target_same_as_source: bool = True
    target_domains: list[str] = Field(default_factory=list)
    target_tables: list[int] | None = None
    vendor: str | None = None
    model: str | None = None
    #: The score from which the run's proposals start ticked, the person's Tick from as set in the
    #: dialog (REQ-413, amended 2026-11-18): the model is told it, a proposal at it or above is
    #: kept unmeasured, and the key value overlap band runs up to it.
    threshold: float = Field(ge=0.0, le=1.0)
    #: The distinct sample values read and shown to the model of each compared column.
    samples_per_column: int = Field(ge=1)
    #: Key value overlap's settings, given exactly when it is chosen: the measure cutoff, the
    #: sample size (the most non-null values read of each column), the share of a source's values
    #: in a key target at which the pair is a key relationship, and the most a pair scores when
    #: neither side is a key.
    measure_cutoff: float | None = Field(default=None, ge=0.0, le=1.0)
    sample_size: int | None = Field(default=None, ge=1)
    contained_at: float | None = Field(default=None, ge=0.0, le=1.0)
    shared_ceiling: float | None = Field(default=None, ge=0.0, le=1.0)

    def key_values(self) -> KeyValueOverlap | None:
        """Key value overlap as asked: refused without semantics, whose pairs it measures, and
        refused unless its settings are given exactly when it is chosen."""
        given = (self.measure_cutoff, self.sample_size, self.contained_at, self.shared_ceiling)
        if KEY_VALUES not in self.kinds:
            if any(v is not None for v in given):
                raise ApiError(
                    400,
                    "discovery.key_values_settings_unchosen",
                    "Key value overlap's settings are given only with Key value overlap",
                )
            return None
        if "semantics" not in self.kinds:
            raise ApiError(
                400,
                "discovery.key_values_needs_semantics",
                "Key value overlap measures the pairs Semantic proposes: choose Semantic with it",
            )
        if (
            self.measure_cutoff is None
            or self.sample_size is None
            or self.contained_at is None
            or self.shared_ceiling is None
        ):
            raise ApiError(
                400,
                "discovery.key_values_settings_missing",
                "Key value overlap needs its measure cutoff, sample size, key containment share "
                "and shared value ceiling",
            )
        return KeyValueOverlap(
            self.measure_cutoff, self.sample_size, self.contained_at, self.shared_ceiling
        )

    def spec(self, role: str, within: frozenset[str] | None) -> RunSpec:
        """The run asked for, by the person acting as ``role``: its pairs are measured as it, and
        no table outside ``within`` (the selected domains as the person may reach them, REQ-2253;
        None: no narrowing) is read or proposed."""
        if (self.vendor is None) != (self.model is None):
            raise ApiError(
                400,
                "discovery.model_incomplete",
                "A chosen model names both its vendor and its model",
            )
        return RunSpec(
            kinds=frozenset(cast("Kind", k) for k in self.kinds if k != KEY_VALUES),
            source_domains=tuple(self.source_domains),
            source_tables=None if self.source_tables is None else tuple(self.source_tables),
            target_domains=tuple(self.target_domains),
            target_tables=None if self.target_tables is None else tuple(self.target_tables),
            same_as_source=self.target_same_as_source,
            model=None if self.vendor is None or self.model is None else (self.vendor, self.model),
            role=role,
            key_values=self.key_values(),
            threshold=self.threshold,
            samples_per_column=self.samples_per_column,
            within=within,
        )


async def _run_model(spec: RunSpec) -> ChosenModel | None:  # REQ-413, REQ-464
    """The run's model when semantics is asked, refused when its vendor has no key."""
    if "semantics" not in spec.kinds:
        return None
    from provisa.core.org_secrets import read_org_api_keys
    from provisa.core.org_settings import resolve_org_config

    pool = state.model_db
    assert pool is not None
    chosen = choose_model(await resolve_org_config(pool), await read_org_api_keys(pool), spec.model)
    if chosen.problem is not None:
        raise ApiError(
            409,
            "discovery.semantics_unavailable",
            f"Semantics cannot run: {chosen.label} has no key. Set one under {WHERE_TO_SET}.",
            model=chosen.label,
            where=WHERE_TO_SET,
        )
    return chosen


@router.get("/models")
async def discovery_models(request: Request):  # REQ-413, REQ-464
    """The run's Model select: the model assigned to relationship inference with its limits, and
    every vendor with a key configured for the organization. Anyone who may run discovery may
    read it."""
    require_capability_request(request, "create_relationship")
    from provisa.core.org_secrets import LLM_VENDORS, read_org_api_keys
    from provisa.core.org_settings import resolve_org_config

    pool = state.model_db
    assert pool is not None
    config = await resolve_org_config(pool)
    keys = await read_org_api_keys(pool)
    assigned = choose_model(config, keys, None)
    endpoints = [
        str(ep["id"]) for ep in (config.get("ai_endpoints", []) or []) if ep.get("enabled", True)
    ]
    vendors = [
        v
        for v in sorted(LLM_VENDORS) + endpoints
        if choose_model(config, keys, (v, assigned.model)).problem is None
    ]
    return {
        "assigned": {**assigned.describe(), "has_key": assigned.problem is None},
        "vendors": vendors,
        "semantics_available": bool(vendors),
    }


@router.get("/vendors/{vendor}/models")
async def discovery_vendor_models(request: Request, vendor: str):  # REQ-413
    """The models ``vendor`` serves, for the run's Model select; ``vendor`` must have a key."""
    require_capability_request(request, "create_relationship")
    from provisa.api.admin.ai_models_router import _run_model_listing
    from provisa.core.org_secrets import (
        endpoint_address,
        endpoint_key,
        org_ai_key,
        read_org_api_keys,
    )
    from provisa.core.org_settings import resolve_org_config
    from provisa.llm.vendor_models import (
        VENDOR_MODEL_APIS,
        fetch_endpoint_models,
        fetch_vendor_models,
    )

    pool = state.model_db
    assert pool is not None
    if vendor in VENDOR_MODEL_APIS:
        # The key the vendor's calls are made with; refused when there is none (REQ-1395).
        key = await org_ai_key(pool, vendor)
        models = await _run_model_listing(vendor, lambda: fetch_vendor_models(vendor, key))
        return {"vendor": vendor, "models": models}
    config = await resolve_org_config(pool)
    endpoint = {ep["id"]: ep for ep in (config.get("ai_endpoints", []) or [])}.get(vendor)
    if endpoint is None or not endpoint.get("enabled", True):
        raise ApiError(
            404,
            "discovery.vendor_unknown",
            f"{vendor} lists no models here; enter the model name",
            vendor=vendor,
        )
    # The key and address the endpoint's calls are made with (REQ-1395, REQ-1557, REQ-1790).
    key = await endpoint_key(await read_org_api_keys(pool), endpoint)
    address = endpoint_address(endpoint)
    models = await _run_model_listing(
        vendor, lambda: fetch_endpoint_models(endpoint["style"], address, key)
    )
    return {"vendor": vendor, "models": models}


@router.get("/limits")
async def discovery_model_limits(request: Request, vendor: str, model: str):  # REQ-464
    """One vendor/model's input and output limits in tokens, shown beside the Model select."""
    require_capability_request(request, "create_relationship")
    from provisa.core.org_settings import resolve_org_config
    from provisa.llm.limits import describe

    pool = state.model_db
    assert pool is not None
    return describe(vendor, model, await resolve_org_config(pool))


def _refuse_empty(
    side: str, resolved: list[int], named: list[int] | tuple[int, ...] | None
) -> None:
    """A set that resolves to no table is refused, saying why: chosen tables none of which
    discovery reads (gather.scope_table_ids leaves out a table of a system domain and one out of
    service), or chosen domains that hold none it reads."""
    if resolved:
        return
    if named is not None:
        chosen = len(set(named))
        raise ApiError(
            400,
            f"discovery.{side}_tables_not_read",
            f"None of the {chosen} chosen {side} tables is read: discovery leaves out a table of "
            "a system domain or one out of service",
            count=chosen,
        )
    raise ApiError(
        400,
        f"discovery.{side}_domains_empty",
        f"The chosen {side} domains hold no table discovery reads",
    )


async def _refuse_outside(
    conn: "Connection",
    side: str,
    resolved: list[int],
    named: tuple[int, ...] | None,
    within: frozenset[str] | None,
) -> None:
    """REQ-2253: a set whose chosen tables all lie outside the selected domains is refused,
    naming them; none is read."""
    if resolved or named is None or within is None:
        return
    outside = await gather.outside_selected_domains(conn, named, within)
    if outside:
        raise ApiError(
            400,
            f"discovery.{side}_tables_outside_domains",
            f"The chosen {side} tables are outside the selected domains: {', '.join(outside)}",
            tables=outside,
            names=", ".join(outside),
        )


async def _require_tables(spec: RunSpec) -> None:  # REQ-413, REQ-2253
    """Refuse, before anything runs, a scope whose source or target resolves to no table."""
    pool = state.model_db
    assert pool is not None
    async with pool.acquire() as _conn:
        conn = cast("Connection", _conn)
        source, target = await table_sets(conn, spec)
        await _refuse_outside(conn, "source", source, spec.source_tables, spec.within)
        if target is not None:
            await _refuse_outside(conn, "target", target, spec.target_tables, spec.within)
    _refuse_empty("source", source, spec.source_tables)
    if target is not None:
        _refuse_empty("target", target, spec.target_tables)


@router.post("/runs/count")
async def count_run(
    request: Request, body: RunScope, domains: SelectedDomains = None
):  # REQ-413, REQ-2253
    """Before Run: at most how many calls the run would make, and the table pairs they list, for the chosen
    kinds, model and sets. Computed from the model store alone: no table is read, no source is
    asked and no model is called, so it can be asked again on every change of the scope. It is
    an upper bound (``requests_are``): the run reads the tables and may make fewer. ``domains``
    is the navbar filter's selection: no table outside it is counted."""
    require_capability_request(request, "create_relationship")
    spec = body.spec(_acting(request), selected_domains_request(request, domains))
    model = await _run_model(spec)
    await _require_tables(spec)
    try:
        return await count(state, spec, model)
    except NoRoomError as exc:
        raise ApiError(
            422,
            "discovery.model_has_no_room",
            f"The model cannot hold a request: {exc}",
            model=None if model is None else model.label,
            reason=str(exc),
        ) from exc


def _acting(request: Request) -> str:
    """The role the request runs as (REQ-273): a run's pairs are measured as it."""
    from provisa.api.acting_role import acting_role
    from provisa.profiler.run import PROFILE_ROLE

    # REQ-273: with no role established and none named, a request runs as the org admin.
    return acting_role(request, request.headers.get("X-Provisa-Role"), None, PROFILE_ROLE)


def _caller(request: Request) -> str | None:
    identity = getattr(request.state, "identity", None)
    return None if identity is None else getattr(identity, "user_id", None)


class _RunEnded(Exception):
    """The run's row ended while it ran (it was given up on): the worker stops."""


async def run_discovery(run_id: int, spec: RunSpec, model: ChosenModel | None) -> None:  # REQ-413
    """Do a run in the background, its row kept as it goes: progress after each step, Stop read
    before each request; a run that cannot finish is marked failed, naming why."""
    pool = state.model_db
    assert pool is not None

    async def progress(report: dict[str, Any]) -> None:
        async with pool.acquire() as _conn:
            moved_on = await runs_repo.record_progress(cast("Connection", _conn), run_id, report)
        if not moved_on:
            raise _RunEnded

    async def reading(read: int, total: int) -> None:
        await progress({"phase": "reading", "tables_read": read, "tables_total": total})

    async def stop_asked() -> bool:
        async with pool.acquire() as _conn:
            return await runs_repo.stop_asked(cast("Connection", _conn), run_id)

    async def record(read: list[proposals_repo.Recorded]) -> None:
        async with pool.acquire() as _conn:
            await proposals_repo.record(cast("Connection", _conn), run_id, read)

    try:
        prepared = await prepare(state, spec, model, reading)
        report = await perform(
            state,
            spec,
            model,
            prepared,
            scope=f"run:{run_id}",
            progress=progress,
            stop_asked=stop_asked,
            record=record,
        )
        async with pool.acquire() as _conn:
            await runs_repo.finish(cast("Connection", _conn), run_id, report)
    except _RunEnded:
        _log.info("discovery run %s stopped: its row has ended", run_id)
    except asyncio.CancelledError:  # the process is shutting down (shutdown_background)
        async with pool.acquire() as _conn:
            await runs_repo.fail(cast("Connection", _conn), run_id, runs_repo.SHUT_DOWN)
        raise
    except Exception as exc:  # the run is marked failed with what stopped it, never left running
        _log.exception("discovery run %s failed", run_id)
        async with pool.acquire() as _conn:
            await runs_repo.fail(cast("Connection", _conn), run_id, str(exc) or type(exc).__name__)


def _launch(run_id: int, spec: RunSpec, model: ChosenModel | None) -> None:
    """Run in the background: a run outlives the request that started it."""
    from provisa.core.connection_loop import spawn_background

    spawn_background(run_discovery(run_id, spec, model), name="discovery-run")


@router.post("/runs")
async def start_run(
    request: Request, body: RunScope, domains: SelectedDomains = None
):  # REQ-413, REQ-2253
    """Start a run in the background; answered at once with the run, running. ``domains`` is the
    navbar filter's selection: no table outside it is read or proposed."""
    require_capability_request(request, "create_relationship")
    spec = body.spec(_acting(request), selected_domains_request(request, domains))
    model = await _run_model(spec)
    await _require_tables(spec)
    pool = state.model_db
    assert pool is not None
    async with pool.acquire() as _conn:
        conn = cast("Connection", _conn)
        run_id = await runs_repo.start(
            conn,
            scope=body.model_dump(),
            model=None if model is None else model.label,
            started_by=_caller(request),
        )
        await _remember(conn, request, body)
        started = await runs_repo.run(conn, run_id)
    assert started is not None
    _launch(run_id, spec, model)
    return {"run": started.view()}


async def _source_domains(conn: "Connection", body: DiscoverRequest) -> list[str]:
    """The domains a discovery request's scope compares the tables of."""
    from sqlalchemy import select

    from provisa.core.schema_org import registered_tables as rt

    if body.scope == "table":
        if body.table_id is None:
            raise ApiError(400, "discovery.table_id_required", "table_id required for table scope")
        domain = (
            await conn.execute_core(select(rt.c.domain_id).where(rt.c.id == body.table_id))
        ).scalar()
        if domain is None:
            raise ApiError(
                404,
                "discovery.table_not_found",
                f"Table {body.table_id} is not registered",
                table_id=body.table_id,
            )
        return [str(domain)]
    if body.scope == "domain":
        if body.domain_id is None:
            raise ApiError(
                400, "discovery.domain_id_required", "domain_id required for domain scope"
            )
        return [body.domain_id]
    found = await conn.execute_core(select(rt.c.domain_id).distinct().order_by(rt.c.domain_id))
    return [str(r[0]) for r in found.fetchall()]


@router.post("/relationships")
async def trigger_discovery(
    request: Request, body: DiscoverRequest, domains: SelectedDomains = None
):  # REQ-018, REQ-167, REQ-413, REQ-612, REQ-2253
    """A discovery run done while the caller waits: started, performed and stored as a run the
    dialog starts (its proposals kept, Jev's scores beside the model's), and answered with the
    run and what each kind found. ``domains`` is the navbar filter's selection: no table outside
    it is read or proposed."""
    require_capability_request(request, "create_relationship")
    pool = state.model_db
    assert pool is not None
    with _stage(_tracer, "admin.discovery") as span:
        async with pool.acquire() as _conn:
            conn = cast("Connection", _conn)
            domains = await _source_domains(conn, body)
            # REQ-413 (amended 2026-11-18): a run started here, with no dialog, takes the
            # settings the person last set in it.
            kept = await threshold_repo.read_settings(conn, _person(request))
        scope = RunScope(
            kinds=list[RunKind](body.kinds),
            source_domains=domains,
            threshold=kept.threshold,
            samples_per_column=kept.samples_per_column,
        )
        spec = scope.spec(_acting(request), selected_domains_request(request, domains))
        model = await _run_model(spec)
        async with pool.acquire() as _conn:
            run_id = await runs_repo.start(
                cast("Connection", _conn),
                scope=scope.model_dump(),
                model=None if model is None else model.label,
                started_by=_caller(request),
            )
        await run_discovery(run_id, spec, model)
        async with pool.acquire() as _conn:
            ended = await runs_repo.run(cast("Connection", _conn), run_id)
        assert ended is not None
        if ended.status != runs_repo.DONE:
            raise ApiError(
                502,
                "discovery.run_failed",
                f"Discovery run {run_id} failed: {ended.error}",
                run_id=run_id,
                error=ended.error,
            )
        report = ended.report
        assert report is not None, f"discovery run {run_id} finished with no report"
        span.set_attribute("admin.source_count", int(report["suggestions"]))
        return {
            "run": ended.view(),
            "candidates_found": report["suggestions"],
            "stored_ids": report["stored_ids"],
            **{kind: report[kind] for kind in ("foreign_keys", "semantics") if kind in report},
        }


async def _held(conn: "Connection", run_id: int) -> runs_repo.RunRow:
    await runs_repo.give_up_on_left_behind(conn, runs_repo.this_launch())
    found = await runs_repo.run(conn, run_id)
    if found is None:
        raise ApiError(
            404, "discovery.run_not_found", f"Discovery run {run_id} not found", run_id=run_id
        )
    return found


@router.get("/runs")
async def list_runs(request: Request, limit: int = 10, mine: bool = False):  # REQ-413
    """The latest runs, newest first: the dialog finds a run still going when it opens. ``mine``:
    only the runs the caller started, whose latest a new run's fields start from."""
    require_capability_request(request, "create_relationship")
    pool = state.model_db
    assert pool is not None
    async with pool.acquire() as _conn:
        conn = cast("Connection", _conn)
        await runs_repo.give_up_on_left_behind(conn, runs_repo.this_launch())
        found = await runs_repo.recent(
            conn, max(1, min(limit, 100)), _caller(request) if mine else runs_repo.ANYONE
        )
    return {"runs": [r.view() for r in found]}


@router.get("/runs/{run_id}")
async def get_run(request: Request, run_id: int):  # REQ-413
    """A run's progress: requests done of the total, suggestions so far, what each kind found."""
    require_capability_request(request, "create_relationship")
    pool = state.model_db
    assert pool is not None
    async with pool.acquire() as _conn:
        found = await _held(cast("Connection", _conn), run_id)
    return {"run": found.view()}


@router.post("/runs/{run_id}/stop")
async def stop_run(request: Request, run_id: int):  # REQ-413
    """Stop a run after the request in flight; what it found is kept."""
    require_capability_request(request, "create_relationship")
    pool = state.model_db
    assert pool is not None
    async with pool.acquire() as _conn:
        conn = cast("Connection", _conn)
        found = await _held(conn, run_id)
        if not await runs_repo.ask_stop(conn, run_id, _caller(request)):
            raise ApiError(
                409,
                "discovery.run_ended",
                f"Discovery run {run_id} has already ended: {found.status}",
                run_id=run_id,
                status=found.status,
            )
        asked = await runs_repo.run(conn, run_id)
    assert asked is not None
    return {"run": asked.view()}


@router.get("/runs/{run_id}/proposals")
async def list_proposals(request: Request, run_id: int):  # REQ-413
    """Every proposal a run read and no reviewer has decided, the highest score first: its
    confidence and reasoning, why it is not among the run's suggestions where it is not, and
    whether it can be decided. ``threshold``: the score from which a row starts ticked."""
    require_capability_request(request, "create_relationship")
    pool = state.model_db
    assert pool is not None
    async with pool.acquire() as _conn:
        conn = cast("Connection", _conn)
        held = await _held(conn, run_id)
        kept = await threshold_repo.read_settings(conn, _person(request))
        return {
            "proposals": await proposals_repo.listed(conn, state, run_id),
            "threshold": kept.threshold,
            # The person's filter on the Score column, applied when the results open
            # (REQ-413, amended 2026-11-18); None when they cleared it.
            "score_filter": kept.score_filter,
            # The score the run keeps a suggestion at: the threshold it was started with
            # (REQ-413, amended 2026-11-18); a proposal's reason below_threshold is against it,
            # whatever the person's threshold is now.
            "kept_at": held.scope["threshold"],
        }


def _person(request: Request) -> str:
    """The signed-in person whose threshold is read and kept."""
    caller = _caller(request)
    if caller is None:
        raise ApiError(401, "auth.required", "Authentication required")
    return caller


class ThresholdBody(BaseModel):  # REQ-413
    """The score from which a run's proposals start ticked, from 0 to 1."""

    threshold: float = Field(ge=0.0, le=1.0)


async def _remember(conn: "Connection", request: Request, body: RunScope) -> None:
    """Keep the settings the run's person set in the dialog (REQ-413, amended 2026-11-18) for
    their next runs: their threshold always; with semantics, the samples per column and Key value
    overlap, chosen or not, and, chosen, its settings. A run without semantics, where those are not
    offered, leaves them as they were; Key value overlap unchosen keeps its settings last set."""
    person = _person(request)
    last = await threshold_repo.read_settings(conn, person)
    samples, kv = last.samples_per_column, last.key_values
    if "semantics" in body.kinds:
        samples = body.samples_per_column
        asked = body.key_values()
        kv = (
            replace(kv, chosen=False)
            if asked is None
            else threshold_repo.KeyValueSettings(
                True, asked.cutoff, asked.sample_size, asked.contained_at, asked.shared_ceiling
            )
        )
    await threshold_repo.keep_settings(
        conn,
        person,
        replace(last, threshold=body.threshold, samples_per_column=samples, key_values=kv),
    )


@router.get("/settings")
async def get_settings(request: Request):  # REQ-413
    """The signed-in person's settings: the run settings, which prefill the discover dialog, and
    the Score filter the results open with; and the defaults Reset to defaults restores."""
    require_capability_request(request, "create_relationship")
    pool = state.model_db
    assert pool is not None
    async with pool.acquire() as _conn:
        kept = await threshold_repo.read_settings(cast("Connection", _conn), _person(request))
    return {**kept.as_dict(), "defaults": threshold_repo.DEFAULTS.as_dict()}


@router.get("/threshold")
async def get_threshold(request: Request):  # REQ-413
    """The signed-in person's threshold: the score from which a run's proposals start ticked."""
    require_capability_request(request, "create_relationship")
    pool = state.model_db
    assert pool is not None
    async with pool.acquire() as _conn:
        kept = await threshold_repo.read_settings(cast("Connection", _conn), _person(request))
    return {"threshold": kept.threshold}


@router.put("/threshold")
async def put_threshold(request: Request, body: ThresholdBody):  # REQ-413
    """Keep the threshold set in the dialog as the signed-in person's own."""
    require_capability_request(request, "create_relationship")
    pool = state.model_db
    assert pool is not None
    async with pool.acquire() as _conn:
        conn = cast("Connection", _conn)
        person = _person(request)
        last = await threshold_repo.read_settings(conn, person)
        kept = await threshold_repo.keep_settings(
            conn, person, replace(last, threshold=body.threshold)
        )
    return {"threshold": kept.threshold}


class ScoreFilter(BaseModel):  # REQ-413
    """A filter on a run's Score column, as the list's column filter holds it: an operator and
    its operands, in percent."""

    op: Literal["eq", "ne", "lt", "le", "gt", "ge", "between", "isEmpty", "notEmpty", "in"]
    a: str | None = None
    b: str | None = None
    values: list[str] | None = None


class ScoreFilterBody(BaseModel):  # REQ-413
    """The Score filter a person set in a run's results; None: they cleared it."""

    filter: ScoreFilter | None


@router.put("/score-filter")
async def put_score_filter(request: Request, body: ScoreFilterBody):  # REQ-413
    """Keep the Score filter set in a run's results as the signed-in person's own."""
    require_capability_request(request, "create_relationship")
    pool = state.model_db
    assert pool is not None
    spec = None if body.filter is None else body.filter.model_dump(exclude_none=True)
    async with pool.acquire() as _conn:
        conn = cast("Connection", _conn)
        person = _person(request)
        last = await threshold_repo.read_settings(conn, person)
        kept = await threshold_repo.keep_settings(conn, person, replace(last, score_filter=spec))
    return {"filter": kept.score_filter}


class ApplyRequest(BaseModel):
    """What a reviewer decided in a run's result: the proposals to accept, each with the name its
    relationship takes, and those not to recommend again. A proposal in neither stays undecided."""

    accept: list[AcceptEach] = Field(default_factory=list)
    not_again: list[int] = Field(default_factory=list)


@router.post("/runs/{run_id}/apply")
async def apply_decisions(request: Request, run_id: int, body: ApplyRequest):  # REQ-413, REQ-612
    """Accept the proposals listed and mark the others listed not to be recommended again, each
    in a transaction of its own: one that fails is reported with its reason and leaves the
    others decided. Refused while the run is still going (it writes its rows as it goes)."""
    require_capability_request(request, "create_relationship")
    pool = state.model_db
    assert pool is not None
    results: list[dict[str, Any]] = []
    async with pool.acquire() as _conn:
        conn = cast("Connection", _conn)
        found = await _held(conn, run_id)
        if found.status == runs_repo.RUNNING:
            raise ApiError(
                409,
                "discovery.run_still_going",
                f"Discovery run {run_id} is still going; decide its proposals once it has ended",
                run_id=run_id,
            )
        if set(e.id for e in body.accept) & set(body.not_again):
            raise ApiError(
                400,
                "discovery.decided_both_ways",
                "A proposal is accepted or not recommended again, not both",
            )
        for each in body.accept:
            try:
                if await proposals_repo.junction_of(conn, run_id, each.id) is not None:
                    # REQ-1586: a junction suggestion joins its two ends through the junction.
                    relationship_id = await join_through.accept_proposal(
                        request, conn, run_id, each.id, each.name
                    )
                elif await proposals_repo.shared_pair(conn, run_id, each.id):
                    # REQ-413: a many-to-many pair is accepted as a shared dimension.
                    relationship_id = await discovery_dimension.accept(
                        request, conn, run_id, each.id, each.name
                    )
                else:
                    relationship_id = await proposals_repo.accept(conn, run_id, each.id, each.name)
            except Exception as exc:  # reported per proposal, the rest go on
                _log.warning("Apply: accepting proposal %s failed: %s", each.id, exc)
                results.append(
                    {"id": each.id, "action": "accept", "ok": False, "error": client_text(str(exc))}
                )
                continue
            results.append(
                {"id": each.id, "action": "accept", "ok": True, "relationship_id": relationship_id}
            )
        for proposal_id in body.not_again:
            try:
                await proposals_repo.not_again(conn, run_id, proposal_id)
            except Exception as exc:  # reported per proposal, the rest go on
                _log.warning("Apply: marking proposal %s failed: %s", proposal_id, exc)
                results.append(
                    {
                        "id": proposal_id,
                        "action": "not_again",
                        "ok": False,
                        "error": client_text(str(exc)),
                    }
                )
                continue
            results.append({"id": proposal_id, "action": "not_again", "ok": True})
    return {"results": results}


@router.get("/candidates")
async def list_candidates(request: Request):  # REQ-612
    """List pending relationship candidates."""
    require_capability_request(request, "create_relationship")
    pool = state.model_db
    assert pool is not None
    async with pool.acquire() as _conn:
        return await candidates_repo.list_pending(cast("Connection", _conn), state)


@router.post("/candidates/{candidate_id}/accept")
async def accept_candidate(
    request: Request, candidate_id: int, body: AcceptRequest | None = None
):  # REQ-612
    """Accept a relationship candidate."""
    require_capability_request(request, "create_relationship")
    pool = state.model_db
    assert pool is not None
    async with pool.acquire() as _conn:
        try:
            return await candidates_repo.accept(
                cast("Connection", _conn), candidate_id, body.name if body else None
            )
        except RelationshipNameTaken as exc:  # REQ-415: a typed name that is taken
            raise ApiError(
                409, exc.code, str(exc), kind=exc.kind, name=exc.name, held_by=exc.held_by
            ) from exc


@router.post("/candidates/{candidate_id}/reject")
async def reject_candidate(request: Request, candidate_id: int, body: RejectRequest):  # REQ-612
    """Reject a relationship candidate."""
    require_capability_request(request, "create_relationship")
    pool = state.model_db
    assert pool is not None
    async with pool.acquire() as _conn:
        await candidates_repo.reject(cast("Connection", _conn), candidate_id, body.reason)
    return {"status": "rejected"}


@router.get("/candidates/rejected/count")
async def rejected_count(request: Request):
    """Count rejected candidates."""
    require_capability_request(request, "create_relationship")
    pool = state.model_db
    assert pool is not None
    async with pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM relationship_candidates WHERE status = 'rejected'"
        )
    return {"count": count}


@router.delete("/candidates/rejected")
async def clear_rejections(request: Request):
    """Delete all rejected candidates."""
    require_capability_request(request, "create_relationship")
    pool = state.model_db
    assert pool is not None
    async with pool.acquire() as _conn:
        count = await candidates_repo.clear_rejections(cast("Connection", _conn))
    return {"deleted": count}
