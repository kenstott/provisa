# Copyright (c) 2026 Kenneth Stott
# Canary: 682f1d6b-5647-4ad7-98e4-d2a1c97217c9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A role's LLM budget (REQ-1174, amended 2026-10-27: "LLM TOKEN BUDGETS ARE THE ROLE'S" and
"BUDGETS ARE ROLLING WINDOWS COUNTED IN THE CONTROL PLANE").

A role may cap what one Polly turn or one LLM service call uses (``llm_call_cap``), and set any of
four rolling windows -- an hour, a day, a week, a month (30 days) -- each a limit in tokens or USD
(``llm_windows``), USD priced by ``polly.model_prices`` (REQ-2012). A budget never resets: a window
holds what was spent within it now.

Usage is counted once per org and role, whoever acts as it: an operator who wants one person's
budget gives that person a role of their own. It is kept in the org's control plane
(``role_llm_usage``) as per-minute buckets (an hour), per-hour buckets (a week) and per-day buckets
(31 days); each window is a sum over the buckets of its grain. A call is admitted before it runs
(:func:`admit`) and its usage debited after (:func:`debit`). A call acting as several roles is held
to the most generous: it runs when any of them has room, and is counted against the one with the
most room. A refusal names the window and when its oldest spend leaves it.
"""

# Requirements: REQ-1174, REQ-2012, REQ-2105

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

Unit = Literal["tokens", "usd"]

#: Each window: how long it is, and the grain of the buckets it sums.
WINDOWS: dict[str, tuple[timedelta, str]] = {
    "hour": (timedelta(hours=1), "minute"),
    "day": (timedelta(days=1), "hour"),
    "week": (timedelta(days=7), "hour"),
    "month": (timedelta(days=30), "day"),
}
#: Each grain: a bucket's length, and how long its buckets are kept.
GRAINS: dict[str, tuple[timedelta, timedelta]] = {
    "minute": (timedelta(minutes=1), timedelta(hours=1)),
    "hour": (timedelta(hours=1), timedelta(days=7)),
    "day": (timedelta(days=1), timedelta(days=31)),
}


@dataclass(frozen=True)
class Limit:
    amount: float
    unit: Unit

    def said(self, value: float) -> str:
        return f"${value:,.4f}" if self.unit == "usd" else f"{int(value):,} tokens"


@dataclass(frozen=True)
class Budget:
    """What a role may spend: per call, and in each window it sets."""

    call_cap: Limit | None = None
    windows: dict[str, Limit] = field(default_factory=dict)

    @property
    def priced(self) -> bool:
        """Whether a limit is in USD: such a role uses only models with a price."""
        limits = [*self.windows.values(), *([self.call_cap] if self.call_cap else [])]
        return any(limit.unit == "usd" for limit in limits)


@dataclass(frozen=True)
class Spend:
    tokens: int = 0
    usd: float = 0.0

    def of(self, unit: Unit) -> float:
        return self.usd if unit == "usd" else self.tokens


class BudgetRefused(PermissionError):
    """A call a role's LLM budget does not admit; the message names the window or cap."""


def _limit(raw: Mapping[str, Any] | None) -> Limit | None:
    if not raw:
        return None
    return Limit(float(raw["amount"]), raw["unit"])


def budget_of(role: Mapping[str, Any]) -> Budget | None:
    """The role's LLM budget (its ``rate_limit``'s ``llm_call_cap`` and ``llm_windows``), or
    None: it sets none."""
    limits = role.get("rate_limit") or {}
    cap = _limit(limits.get("llm_call_cap"))
    windows = {
        name: limit
        for name in WINDOWS
        if (limit := _limit((limits.get("llm_windows") or {}).get(name))) is not None
    }
    return None if cap is None and not windows else Budget(cap, windows)


def bucket_of(at: datetime, grain: str) -> datetime:
    """The start of the ``grain`` bucket ``at`` falls in."""
    if grain == "minute":
        return at.replace(second=0, microsecond=0)
    if grain == "hour":
        return at.replace(minute=0, second=0, microsecond=0)
    return at.replace(hour=0, minute=0, second=0, microsecond=0)


@dataclass(frozen=True)
class WindowUse:
    """A window's limit, what is spent in it now, and when its oldest spend leaves it (None:
    nothing is spent in it)."""

    window: str
    limit: Limit
    used: float
    frees_at: datetime | None

    @property
    def full(self) -> bool:
        return self.used >= self.limit.amount

    @property
    def room(self) -> float:
        """What is left of the window, as a share of its limit."""
        return max(0.0, 1.0 - self.used / self.limit.amount) if self.limit.amount else 0.0


def window_use(
    buckets: Iterable[tuple[str, datetime, int, float]], budget: Budget, now: datetime
) -> list[WindowUse]:
    """Each window the budget sets, summed over ``buckets`` -- (grain, start, tokens, usd) -- whose
    start is within it."""
    held = list(buckets)
    out = []
    for name, limit in budget.windows.items():
        length, grain = WINDOWS[name]
        since = now - length
        inside = [
            (start, tokens, usd) for g, start, tokens, usd in held if g == grain and start > since
        ]
        used = sum(usd if limit.unit == "usd" else tokens for _, tokens, usd in inside)
        spent = [start for start, tokens, usd in inside if (usd if limit.unit == "usd" else tokens)]
        out.append(WindowUse(name, limit, used, min(spent) + length if spent else None))
    return out


def _when(at: datetime) -> str:
    return at.astimezone(UTC).strftime("%Y-%m-%d %H:%M UTC")


def refusal(role_id: str, use: WindowUse) -> str:
    """Why ``role_id``'s full window ``use`` refuses a call."""
    said = f"the {use.window}" if use.window != "month" else "the month (30 days)"
    frees = f"; its oldest spend leaves it at {_when(use.frees_at)}" if use.frees_at else ""
    return (
        f"role {role_id!r} has spent its LLM budget for {said}: "
        f"{use.limit.said(use.used)} of {use.limit.said(use.limit.amount)}{frees}"
    )


def allowance(role_id: str, budget: Budget, uses: Iterable[WindowUse], price: Any) -> int | None:
    """The most output tokens one call may still use: the smaller of the role's per-call cap and
    the room left in each of its windows -- a USD limit in tokens at the model's output price
    (``price``, a polly_usage.Price; None for an unpriced model, which a role with a USD limit
    never calls). None: no limit holds the call's output (each is in USD, and the model's output
    is free). Refused naming the limit when it leaves room for no token at all."""
    left = [(budget.call_cap.amount, budget.call_cap, "per call")] if budget.call_cap else []
    left += [(u.limit.amount - u.used, u.limit, f"for the {u.window}") for u in uses]
    tokens = [
        (
            int(amount if limit.unit == "tokens" else amount * 1_000_000 / price.output),
            limit,
            amount,
            where,
        )
        for amount, limit, where in left
        if limit.unit == "tokens" or price.output
    ]
    if not tokens:
        return None
    most, limit, amount, where = min(tokens, key=lambda t: t[0])
    if most < 1:
        raise BudgetRefused(
            f"role {role_id!r} has {limit.said(amount)} left of its LLM budget {where}: room "
            "for no output token of this model"
        )
    return most


def over_cap(role_id: str, budget: Budget | None, spend: Spend) -> str | None:
    """Why ``spend``, what one call has used so far, passes the role's per-call cap; None if not."""
    if budget is None or budget.call_cap is None:
        return None
    cap = budget.call_cap
    if spend.of(cap.unit) <= cap.amount:
        return None
    return (
        f"role {role_id!r} caps one LLM call at {cap.said(cap.amount)}; "
        f"this one used {cap.said(spend.of(cap.unit))}"
    )


# -- the control plane ------------------------------------------------------------------------------


async def _buckets(model_db: Any, role_id: str, now: datetime) -> list[tuple]:
    from sqlalchemy import or_, select

    from provisa.core.schema_org import role_llm_usage as t

    kept = or_(
        *(
            (t.c.grain == grain) & (t.c.bucket > now - keep)
            for grain, (_length, keep) in GRAINS.items()
        )
    )
    async with model_db.acquire() as conn:
        result = await conn.execute_core(
            select(t.c.grain, t.c.bucket, t.c.tokens, t.c.usd).where(
                (t.c.role_id == role_id) & kept
            )
        )
        return [tuple(r) for r in result.fetchall()]


async def usage(model_db: Any, role_id: str, budget: Budget, now: datetime) -> list[WindowUse]:
    """Each of the role's windows as it stands at ``now``."""
    return window_use(await _buckets(model_db, role_id, now), budget, now)


@dataclass(frozen=True)
class Admitted:
    """The role a call is counted against, and its budget (None: it sets none)."""

    role_id: str
    budget: Budget | None
    #: Its windows as they stood when the call was admitted.
    uses: tuple[WindowUse, ...] = ()


async def admit(
    model_db: Any,
    roles: Mapping[str, Mapping[str, Any]],
    acting: Iterable[str],
    *,
    priced: bool,
    now: datetime | None = None,
) -> Admitted:
    """The role, of those ``acting``, a call is counted against: the most generous -- one with no
    budget, else the one whose fullest window has the most room. Refused by name when none has
    room, naming the window whose oldest spend leaves soonest. ``priced``: the call's model has a
    price; a role with a USD limit uses only such models."""
    now = now or datetime.now(UTC)
    best: tuple[float, str, Budget, list[WindowUse]] | None = None
    blocked: list[tuple[datetime, str]] = []
    unpriced: list[str] = []
    for role_id in acting:
        budget = budget_of(roles[role_id])
        if budget is None:
            return Admitted(role_id, None)
        if model_db is None:
            raise BudgetRefused(
                f"role {role_id!r} has an LLM budget, which is counted in the org's control "
                "plane, and this call is bound to no org"
            )
        if budget.priced and not priced:
            unpriced.append(role_id)
            continue
        uses = await usage(model_db, role_id, budget, now)
        full = [u for u in uses if u.full]
        if full:
            soonest = min(full, key=lambda u: u.frees_at or now)
            blocked.append((soonest.frees_at or now, refusal(role_id, soonest)))
            continue
        room = min((u.room for u in uses), default=1.0)
        if best is None or room > best[0]:
            best = (room, role_id, budget, uses)
    if best is not None:
        return Admitted(best[1], best[2], tuple(best[3]))
    if blocked:
        raise BudgetRefused(min(blocked)[1])
    raise BudgetRefused(
        f"role {unpriced[0]!r} has an LLM budget in USD, so it uses only models with a "
        "polly.model_prices entry, and this model has none"
    )


async def debit(model_db: Any, role_id: str, spend: Spend, now: datetime | None = None) -> None:
    """Count ``spend`` against ``role_id`` in each grain's bucket for now, and drop the buckets no
    window reads any longer."""
    from sqlalchemy import delete

    from provisa.core.schema_org import role_llm_usage as t

    now = now or datetime.now(UTC)
    async with model_db.acquire() as conn:
        for grain, (_length, keep) in GRAINS.items():
            await conn.upsert(
                t,
                {
                    "role_id": role_id,
                    "grain": grain,
                    "bucket": bucket_of(now, grain),
                    "tokens": spend.tokens,
                    "usd": spend.usd,
                },
                index_elements=["role_id", "grain", "bucket"],
                set_extra={"tokens": t.c.tokens + spend.tokens, "usd": t.c.usd + spend.usd},
            )
            await conn.execute_core(
                delete(t).where((t.c.grain == grain) & (t.c.bucket <= now - keep))
            )


# -- a call held to its roles' budget -------------------------------------------------------------


def spend_of(usage: Any, model: str) -> Spend:
    """What a call's ``usage`` (polly_usage.Usage) spends: every token it read or wrote, and its
    cost at the model's price (none for a model with no price)."""
    from provisa.api.mcp import polly_usage

    tokens = (
        usage.input_tokens
        + usage.output_tokens
        + usage.cache_read_tokens
        + usage.cache_write_tokens
    )
    price = polly_usage.prices().get(model)
    return Spend(tokens, 0.0 if price is None else polly_usage.cost_usd(usage, price))


class Guard:
    """One Polly turn's or LLM service call's hold on the budget of the roles it acts as: admitted
    before its first model call (:meth:`admit`), held to the admitting role's per-call cap as its
    usage grows (:meth:`over_cap`), and debited with what it used (:meth:`debit`)."""

    def __init__(
        self, model_db: Any, roles: Mapping[str, Mapping[str, Any]], acting: Iterable[str]
    ):
        self._model_db = model_db
        self._roles = roles
        self._acting = tuple(acting)
        self.admitted: Admitted | None = None

    async def admit(self, model: str) -> None:
        from provisa.api.mcp import polly_usage

        self.admitted = await admit(
            self._model_db,
            self._roles,
            self._acting,
            priced=model in polly_usage.prices(),
        )

    def allowance(self, model: str) -> int | None:
        """The most output tokens the call may use (:func:`allowance`); None when the role it was
        admitted as sets no budget, or none of its limits holds this model's output."""
        from provisa.api.mcp import polly_usage

        assert self.admitted is not None, "a call is admitted before it uses anything"
        if self.admitted.budget is None:
            return None
        return allowance(
            self.admitted.role_id,
            self.admitted.budget,
            self.admitted.uses,
            polly_usage.prices().get(model),
        )

    def over_cap(self, usage: Any, model: str) -> str | None:
        assert self.admitted is not None, "a call is admitted before it uses anything"
        return over_cap(self.admitted.role_id, self.admitted.budget, spend_of(usage, model))

    async def debit(self, usage: Any, model: str) -> None:
        """Count what the call used against the role it was admitted as -- when that role has a
        budget to hold it to; a role with none counts nothing."""
        assert self.admitted is not None, "a call is admitted before it uses anything"
        if self.admitted.budget is not None:
            await debit(self._model_db, self.admitted.role_id, spend_of(usage, model))


def guard(state: Any, role_id: str, model_db: Any) -> Guard:
    """The guard of a call acting as ``role_id`` -- a meta-role acting as each of its members --
    in the org whose control plane is ``model_db``."""
    from provisa.security.meta_role import acting_roles

    return Guard(model_db, state.roles, acting_roles(state, role_id))
