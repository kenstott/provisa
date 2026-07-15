# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The injector action (REQ-940/982) — what a scheduled poll job runs for one node.

A poll node registers a job with the embedded scheduler (APScheduler) at its cadence; each fire
calls ``check_node``: probe the source for its current token, compare it to the persisted baseline,
and — only when it differs (or the source cannot produce a token, degrading to the TTL cadence) — post
the node's change event and fan it out to its dependents. Push nodes (native/debezium/kafka) use a
true listener that posts directly, not this. The event kind comes from the node's probe_type
(REQ-982): ``watermark`` → ``append`` (insert the delta), ``hash``/``count``/``none`` → ``replace``;
falls back to the ``change_signal`` shape when no probe_type is given. Unchanged → no event, so no
wasted downstream work; the REQ-855 baseline token is persisted for the next comparison.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import Any

from provisa.core.change_signal import APPEND, CDC, REPLACE, select_landing_shape
from provisa.events import queue, probes

# REQ-968: the three forced-regen scopes. SOURCE = regen the root(s) and let lineage fan-out cascade
# forward (the natural, always-correct form). NODE = regen a specific derived node WITHOUT re-landing
# its sources (for a changed SQL def). WINDOW = regen one sealed calendar-addressable period.
REGEN_SCOPES = frozenset({"source", "node", "window"})

# landing shape → the event kind posted onto the bus.
_SHAPE_TO_EVENT = {CDC: "delta", APPEND: "append", REPLACE: "replace"}

# A probe returns the source's CURRENT opaque token (or None when it cannot produce one this call —
# a capability signal that degrades the node to its TTL cadence, treated as changed). check_node owns
# the equality compare against the persisted baseline (REQ-855/982) — Provisa never interprets a token.
Probe = Callable[[], Awaitable["str | None"]]


async def check_node(
    conn: Any,
    *,
    node: str,
    change_signal: str,
    watermark_column: str | None,
    probe: Probe,
    dependents: list[str],
    probe_type: str | None = None,
) -> int | None:
    """Run the injector action for one poll ``node``: probe for the current token, compare it to the
    persisted baseline, and if it differs (or is None → TTL degrade) post the change event (shape from
    ``probe_type``, else ``change_signal``) + fan it out to ``dependents``; then persist the new token.
    Returns the event id, or None when the token matched the baseline (the guard against no-op events
    rippling the DAG)."""
    token = await probe()
    prior = await queue.get_node_state(conn, node)
    prior_token = prior["probe_token"] if prior else None
    # A real token equal to the baseline → unchanged. A None token has no capability → do not gate
    # (degrade to the TTL cadence: re-fetch, and let the REQ-981 output hash suppress an idle ripple).
    if token is not None and token == prior_token:
        return None
    shape = (
        probes.probe_shape(probe_type)
        if probe_type is not None
        else select_landing_shape(change_signal, watermark_column)
    )
    event_type = _SHAPE_TO_EVENT[shape]
    event_id = await queue.post_event(
        conn, source_table=node, event_type=event_type, payload={"token": token}
    )
    await queue.fan_out(conn, event_id, dependents)
    if token is not None:
        await queue.set_node_state(conn, node, probe_token=token)
    return event_id


async def force_regen(
    conn: Any,
    *,
    scope: str,
    node: str,
    reason: str,
    window_id: str | None = None,
    as_of: datetime | None = None,
) -> int:
    """REQ-968 forced regen / replay: recompute ``node`` ON DEMAND, bypassing the token/no-op gate
    (REQ-958/981). Posts ONE synthetic ``replace`` event marked ``{forced, reason, scope, ...}`` and
    fans the work item to ``node`` itself, so the node's processor claims it, recomputes REGARDLESS of
    change (the content-hash output gate is skipped), still runs preprocess (REQ-957) and honors the
    output outcomes (REQ-965), then re-posts its OWN change — the normal forward cascade fans to the
    dependents in topological order.

    Scope drives WHAT is posted:
    - ``source`` — ``node`` is a root source: re-land it forced; its re-post cascades to every MV.
    - ``node``   — ``node`` is a derived MV: recompute it WITHOUT re-landing its sources (a changed
      SQL def), cascading to dependents.
    - ``window`` — regen one sealed period: ``window_id`` (the calendar address, REQ-962) is MANDATORY
      and ``as_of`` (a deterministic instant INSIDE that window) pegs the exact-period recompute.

    An unknown ``scope``, a missing ``reason`` (forced events MUST carry a why/audit reason, REQ-967),
    or ``window`` scope without a ``window_id`` FAILS LOUD — never a silent default."""
    if scope not in REGEN_SCOPES:
        raise ValueError(f"unknown regen scope {scope!r}; expected one of {sorted(REGEN_SCOPES)}")
    if not (reason and reason.strip()):
        raise ValueError("REQ-968: a forced regen MUST carry a reason (audit why-tag)")
    if scope == "window" and not window_id:
        raise ValueError("REQ-968: window-scope regen requires a window_id (the calendar address)")
    payload: dict[str, Any] = {"forced": True, "reason": reason, "scope": scope}
    if window_id is not None:
        payload["window_id"] = window_id
    if as_of is not None:
        payload["as_of"] = as_of.isoformat()
    event_id = await queue.post_event(
        conn, source_table=node, event_type="replace", payload=payload
    )
    await queue.fan_out(conn, event_id, [node])
    return event_id
