# Copyright (c) 2026 Kenneth Stott
# Canary: 2422fca0-95f2-4f59-a6e7-e0a6e0c58241
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The seam between Provisa and its commercial plugin.

Metering, tier ceilings, plans and the merchant-of-record integration are SaaS operations, not
product behaviour. They live in a separate distribution (``provisa_commercial``) that only the
hosted deployment installs, so the open-source wheel and the demo build ship neither the pricing
model nor the code that charges for it.

Every core call site goes through this module and NOTHING here fails when the plugin is absent:
the meter records nothing, no tier caps resolve, no billing routes mount, no trial sweep is
scheduled. That is the correct behaviour for a self-hosted deployment — there is no subscription to
enforce and no invoice to produce — and it is why the seam returns "no caps apply" rather than a
default set of ceilings. Guessing a tier for a deployment that has no billing subject would impose
a paywall on software the customer already owns.

Load order matters in one place: :func:`load` must run before the control-plane registry schema is
initialised, because the plugin attaches its columns and its meter table to that registry's
metadata at import time. ``bring_up_platform`` calls it there.
"""

# Requirements: REQ-1044, REQ-1046, REQ-1355, REQ-1452, REQ-1454, REQ-1455

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from provisa.executor.result import ResultStream

log = logging.getLogger(__name__)

_PLUGIN: Any = None
_LOADED = False


def load() -> Any:
    """Import the commercial plugin, or return None when it is not installed.

    Resolved once per process. An ImportError is the ordinary state of the open-source and demo
    distributions, so it is recorded at debug and never raised; any OTHER exception from the
    plugin's import is a broken commercial install and propagates, because a hosted deployment that
    silently ran unmetered would be billing nobody.
    """
    global _PLUGIN, _LOADED
    if _LOADED:
        return _PLUGIN
    try:
        import provisa_commercial
    except ImportError:
        _PLUGIN = None
        log.debug("commercial plugin not installed — billing, metering and tier caps are off")
    else:
        _PLUGIN = provisa_commercial
        log.info("commercial plugin loaded: billing, metering and tier caps are active")
    _LOADED = True
    return _PLUGIN


def enabled() -> bool:
    """Whether this deployment is a commercial one."""
    return load() is not None


def reset_for_tests() -> None:
    """Drop the memoized resolution so a test can install or remove the plugin mid-process."""
    global _PLUGIN, _LOADED
    _PLUGIN, _LOADED = None, False


# --- metering -------------------------------------------------------------------------------- #


async def meter_op(pool: Any, org_id: str) -> None:
    """Meter one submitted statement against ``org_id``'s current billing bucket.

    Called from the ONE audit seam, after its ``pending is None`` guard, so the meter inherits the
    audit's definition of a user query exactly and no protocol can be added that executes governed
    SQL and records nothing.
    """
    plugin = load()
    if plugin is None:
        return
    await plugin.record_op(pool, org_id)


async def meter_egress(pool: Any, org_id: str, n_bytes: int) -> None:
    """Meter ``n_bytes`` delivered to ``org_id``'s clients against its current billing bucket.

    Called from the transport write seams (``provisa.core.egress``) rather than the audit seam,
    because egress is the count of what actually left the socket and a streaming result is
    finalized before it is drained. REQ-1452 bills this line separately from the active hour, and
    REQ-1455 bounds the trial on it.
    """
    plugin = load()
    if plugin is None:
        return
    await plugin.record_egress(pool, org_id, n_bytes)


# --- tier ceilings --------------------------------------------------------------------------- #


async def caps_for_org(state: Any, org_id: str | None) -> tuple[Any, str] | None:
    """The ``(caps, plan)`` in force for ``org_id``, or None when no tier applies.

    None on a self-hosted deployment (no plugin) and on a plugin deployment whose control plane
    holds no ``orgs`` row for the id — in both cases there is no subscription, so there is no
    ceiling to impose.
    """
    plugin = load()
    if plugin is None:
        return None
    return await plugin.caps_for_org(state, org_id)


async def storage_cap_for_org(state: Any, org_id: str | None) -> tuple[int, str] | None:
    """The ``(max_bytes, plan)`` storage allowance for ``org_id``, or None when none applies.

    Separate from :func:`caps_for_org` because the two ceilings answer different questions: a tier
    cap bounds what ONE statement may cost and is attached to every plan, while this bounds what an
    org accumulates on the operator's disk across every materialization it has ever run. Sharing a
    resolver would tie a standing quota's lifetime to a single query's.

    None on a self-hosted deployment: the operator owns the disk and there is nobody to bill for
    it (REQ-1046).
    """
    plugin = load()
    if plugin is None:
        return None
    return await plugin.storage_cap_for_org(state, org_id)


async def meter_storage(pool: Any, org_id: str, n_bytes: int) -> None:
    """Record ``org_id``'s current platform-storage footprint against its billing bucket.

    A LEVEL, not an increment: storage is what the org occupies right now, so each observation
    replaces the last rather than adding to it — unlike ops and egress, which are counts of events
    that happened. REQ-1049 requires the measurement from day one whether or not the deployment
    bills on it, because a quota that was never measured cannot be sized, priced or policed.
    """
    plugin = load()
    if plugin is None:
        return
    await plugin.record_storage(pool, org_id, n_bytes)


def tier_session_hints(caps: Any) -> dict[str, str]:
    """The engine session properties that enforce ``caps``'s scan-side ceilings."""
    plugin = load()
    if plugin is None:
        return {}
    return plugin.tier_session_hints(caps)


def translate_engine_error(exc: BaseException, caps: Any, plan: str) -> Exception | None:
    """The tier restatement of an engine-side ceiling kill, or None when ``exc`` is unrelated."""
    plugin = load()
    if plugin is None:
        return None
    return plugin.translate_engine_error(exc, caps, plan)


def enforce_output_cap[T: "ResultStream"](result: T, caps: Any, plan: str) -> T:
    """Bound ``result`` at the tier's egress ceiling — a rejection, never a truncation.

    Generic in the result's own type rather than ``ResultStream`` in and out: a result over the
    ceiling raises, and one under it comes back as the very object handed in, so the caller keeps
    the concrete result it built (``QueryResult`` on the buffered terminal) instead of the widened
    protocol.
    """
    plugin = load()
    if plugin is None:
        return result
    return plugin.enforce_output_cap(result, caps, plan)


async def require_lane_entitlement(state: Any, org_id: str | None, mode: str) -> None:
    """Refuse a federation-engine lane the org's plan does not include (REQ-1412).

    The isolated lane is a coordinator the PLATFORM runs for one org, so on a hosted deployment it
    belongs to a tier. No-op without the plugin: a self-hosted deployment operates its own engines
    and has no subscription to check.
    """
    plugin = load()
    if plugin is None:
        return
    await plugin.require_lane(state, org_id, mode)


async def engine_size_for_org(state: Any, org_id: str | None) -> Any:
    """The plan-fixed size of ``org_id``'s dedicated engine, or None when its plan does not set one.

    REQ-1449 sells the isolated lane in fixed sizes, so on a hosted deployment the org's plan — not
    a deployment-wide environment variable — says how large its coordinator is. The object carries
    ``pod_cpu``, ``pod_memory_gib`` and ``query_max_memory_gb``; the provisioner reads those three
    and nothing else, so the plan vocabulary stays inside the plugin.

    None without the plugin, and None for an org whose plan sells no engine but which an operator
    put on the isolated lane anyway. Both are deployments that size their engines themselves, and
    the provisioner uses its own settings for them.
    """
    plugin = load()
    if plugin is None:
        return None
    return await plugin.engine_size_for_org(state, org_id)


async def lane_entitled(state: Any, org_id: str | None, mode: str) -> bool:
    """Whether ``org_id`` may select the ``mode`` lane. True without the plugin — an unbilled
    deployment gates nothing."""
    plugin = load()
    if plugin is None:
        return True
    return await plugin.lane_entitled(state, org_id, mode)


async def source_limit_for_org(state: Any, org_id: str | None) -> tuple[int, str] | None:
    """The ``(max_sources, plan)`` a plan admits for ``org_id``, or None when nothing bounds it.

    REQ-1513: Starter admits 10 sources and Pro admits 100, and those are the numbers the Billing
    page prints on the plan cards. None on a self-hosted deployment and for an id with no ``orgs``
    row: an operator running their own Provisa registers as many sources as their own systems have,
    and imposing a ceiling there would paywall software the customer already owns.
    """
    plugin = load()
    if plugin is None:
        return None
    return await plugin.source_limit_for_org(state, org_id)


async def environment_limit_for_org(state: Any, org_id: str | None) -> tuple[int, str] | None:
    """The ``(max_environments, plan)`` a plan admits for ``org_id``, or None when nothing bounds it.

    REQ-1523: an environment is a schema of its own (REQ-1488), so an unbounded count is unbounded
    schemas — the short-lived per-developer environments an org actually creates accumulate until
    nothing reaps them. The ceiling counts ``prod`` with the rest: it is a schema like any other,
    and a plan admitting one environment admits the one every org already has.

    None on a self-hosted deployment and for an id with no ``orgs`` row, for the reason
    :func:`source_limit_for_org` returns None there — no subscription, so no ceiling.
    """
    plugin = load()
    if plugin is None:
        return None
    try:
        return await plugin.environment_limit_for_org(state, org_id)
    except AttributeError:
        return None


# --- trial eligibility ----------------------------------------------------------------------- #


async def bind_member_to_org_trial(pool: Any, org_id: str, email: str | None) -> None:
    """Record that ``email`` has used the free trial, if the org they just joined is on one.

    Called from every seam where a person becomes a member of an org they did not create — invite
    redemption at registration, invite redemption by an existing account, and REQ-1269 auto-join.
    The trial belongs to the org, so everyone working inside a trialling org has had their free
    evaluation; recording it is what stops an org from minting fresh trials by inviting alternate
    accounts that later go off and create orgs of their own.

    No-op without the plugin: a self-hosted deployment has no trial to spend.
    """
    plugin = load()
    if plugin is None:
        return
    await plugin.bind_member_to_trial(pool, org_id, email)


# --- platform-owned orgs ----------------------------------------------------------------------- #


async def entitle_starter(pool: Any, org_id: str) -> None:
    """Put a PLATFORM-OWNED org on the Starter plan, with no purchase behind it (REQ-1598).

    The sandbox org is the deployment's own: nobody buys it, so there is no subscription to set its
    plan and the trial an org row otherwise opens at would expire the one org that must never stop
    answering. Its ceilings are Starter's -- what a visitor tries is the plan the platform sells.

    No-op without the plugin: a self-hosted deployment has no plans, so its orgs are already
    uncapped.
    """
    plugin = load()
    if plugin is None:
        return
    await plugin.entitle_starter(pool, org_id)


# --- org checkout gate ----------------------------------------------------------------------- #


async def sweep_org_reservations(conn) -> None:
    """Release org reservations whose checkout window has closed (REQ-1476).

    A reservation exists only where the org is sold, so both the state and its expiry are the
    plugin's policy; core calls this at the head of the create path so an expired reservation frees
    its id before the next creator is told the name is taken. No-op without the plugin — a
    self-hosted deployment provisions on create and never reserves.
    """
    plugin = load()
    if plugin is None:
        return
    await plugin.release_expired_reservations(conn)


# --- wiring ---------------------------------------------------------------------------------- #


def include_routes(app: Any) -> None:
    """Mount the billing API, if this deployment has one."""
    plugin = load()
    if plugin is None:
        return
    plugin.include_routes(app)


def schedule_jobs(scheduler: Any) -> None:
    """Register the plugin's scheduled billing work (the REQ-1455 trial sweep)."""
    plugin = load()
    if plugin is None:
        return
    plugin.schedule_jobs(scheduler)
