# Copyright (c) 2026 Kenneth Stott
# Canary: c5a81e07-6f2d-4b93-8e14-2d7f0b9a63c1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The model a scan asks, under the org's AI settings and the role's LLM budget (REQ-2179).

The model is the one the org's AI Models settings name for ``glossary_term_finding`` (the org's
overrides on the deployment's, provisa.core.org_settings.resolve_org_config), called with the
org's own vendor keys. Every call is held to the LLM budget of the role the steward acts as
(REQ-1174, provisa.security.llm_budget): admitted before it runs, its output limited to what the
role has room for, and what it used debited after. A refusal names the window that is full and
ends the scan. A scan that could not run is refused before it starts (:func:`for_role`): no model
named, or no key for it; a table scan is refused too when the role's budget has no room for
the rows it would send (:meth:`Held.require_room`, REQ-2202). Every other failure of a call ends
the scan naming the model. An answer cut off at its output limit is counted like any other and raised as
:class:`~provisa.term_finding.scan.AnswerCutOff`, for the scan to decide what to do about it.

What the scan sends is redacted as everything Provisa sends out is (provisa.security.redaction).
"""

# Requirements: REQ-2179, REQ-1174, REQ-2202

from __future__ import annotations

import math
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

from provisa.llm.client import LLMEmptyResponseError, LLMTimeoutError
from provisa.term_finding.scan import AnswerCutOff, ScanFailed

#: The ``ai_models`` entry that names the scan's model.
OPERATION = "glossary_term_finding"
SYSTEM = "You are a data catalog assistant helping a data steward build a business glossary."
#: Where the scan's model and its vendor's key are set, as the admin navigation names it.
WHERE_TO_SET = "Admin, AI Models"

Call = Callable[[str, int], Awaitable[str]]
#: About how many characters of text one token holds: what a scan's prompts are reckoned at
#: before it starts (:meth:`Held.require_room`), no model having counted them yet.
CHARS_PER_TOKEN = 4


class ModelRefused(ScanFailed):
    """A scan refused before it starts: its model cannot run. The message says where to set it."""


class OverBudget(ScanFailed):
    """A scan refused before it starts: the role's LLM budget has no room for what it would send
    (REQ-2202). The message names the limit."""


def usage_of(vendor: str, reported: Any) -> Any:
    """The vendor's reported usage (aisuite's CompletionUsage) as polly_usage.Usage. aisuite's
    Anthropic provider reports input tokens without the cached ones; the OpenAI-shaped vendors
    count the cached ones within ``prompt_tokens``."""
    from provisa.api.mcp import polly_usage

    if vendor == "anthropic":
        details = getattr(reported, "prompt_tokens_details", None)
        cached = (getattr(details, "cached_tokens", None) or 0) if details is not None else 0
        return polly_usage.Usage(
            input_tokens=reported.prompt_tokens,
            output_tokens=reported.completion_tokens,
            cache_read_tokens=cached,
            calls=1,
        )
    return polly_usage.from_openai(reported)


def _failure(exc: Exception, name: str) -> str:
    """What stopped a call to the model ``name``, in plain words."""
    if isinstance(exc, LLMTimeoutError):
        return str(exc)
    if getattr(exc, "status_code", None) in (401, 403):
        return f"the vendor refused the API key for {name}; set a working one under {WHERE_TO_SET}"
    return f"the model {name} failed: {exc}"


class Held:
    """A call to ``client`` (provisa.llm.client.ProvisaLLMClient) held to ``budget``
    (llm_budget.Guard). ``name`` is the model it asks; ``limited_to`` is what the role's budget
    last held a call's output to, in tokens, when it held one to less than the scan asked for."""

    def __init__(self, client: Any, budget: Any, org_id: str | None):
        self._client = client
        self._budget = budget
        self._org_id = org_id
        self.name = f"{client.vendor}:{client.model}"
        self.limited_to: int | None = None

    async def __call__(self, prompt: str, max_tokens: int) -> str:
        from provisa.security.llm_budget import BudgetRefused
        from provisa.security.redaction import redact

        client, budget = self._client, self._budget
        try:
            await budget.admit(client.model)
            allowed = budget.allowance(client.model)
        except BudgetRefused as exc:
            raise ScanFailed(str(exc)) from None
        if allowed is not None:
            if allowed <= 0:
                raise ScanFailed("the role's LLM budget has no room left for another call")
            if allowed < max_tokens:
                self.limited_to = max_tokens = allowed
        try:
            answered = await client.complete_detailed(
                redact(prompt, self._org_id),
                system=SYSTEM,
                max_tokens=max_tokens,
                # Reasoning off: finding and resolving terms is reading, and a model that reasons
                # by default spends the output limit before any answer. Demo bundles 41930549595:
                # anthropic/claude-haiku-5.5 on OpenRouter (default_effort medium) used all of
                # 4096, then 8192, tokens and returned no text. A vendor that cannot be told
                # fails the call by name (provisa.llm.client.ReasoningNotControllable).
                reasoning_off=True,
            )
        except LLMEmptyResponseError as exc:
            if not exc.answer.cut_off:
                raise ScanFailed(redact(_failure(exc, self.name), self._org_id)) from exc
            answered = exc.answer
        except Exception as exc:
            raise ScanFailed(redact(_failure(exc, self.name), self._org_id)) from exc
        if answered.usage is None:
            if budget.admitted is not None and budget.admitted.budget is not None:
                raise ScanFailed(
                    f"{answered.vendor}:{answered.model} reported no usage, so the call cannot "
                    "be counted against the role's LLM budget"
                )
        else:
            await budget.debit(usage_of(answered.vendor, answered.usage), answered.model)
        if answered.cut_off:
            raise AnswerCutOff(
                f"{answered.vendor}:{answered.model} stopped at its limit of {max_tokens} output "
                "tokens"
            )
        return answered.text

    async def require_room(self, prompts: Sequence[str], what: str) -> None:
        """Refuse (:class:`OverBudget`) a scan of ``what`` whose ``prompts`` -- what it sends at
        the least -- the role's LLM budget has no room for: the largest of them over its per-call
        cap, or all of them over what is left of a window. Reckoned in tokens at
        :data:`CHARS_PER_TOKEN`, and a USD limit at the model's input price. A role that sets no
        budget is not held to one."""
        from provisa.api.mcp import polly_usage
        from provisa.security.llm_budget import BudgetRefused

        budget, model = self._budget, self._client.model
        try:
            await budget.admit(model)
        except BudgetRefused as exc:
            raise OverBudget(str(exc)) from None
        admitted = budget.admitted
        if admitted is None or admitted.budget is None or not prompts:
            return
        sizes = [math.ceil(len(p) / CHARS_PER_TOKEN) for p in prompts]
        price = polly_usage.prices().get(model)

        def spend(limit: Any, tokens: int) -> float:
            if limit.unit == "tokens":
                return tokens
            return 0.0 if price is None else tokens * price.input / 1_000_000

        role, held = admitted.role_id, admitted.budget
        cap = held.call_cap
        if cap is not None and spend(cap, max(sizes)) > cap.amount:
            raise OverBudget(
                f"scanning {what} sends about {cap.said(spend(cap, max(sizes)))} in one call, and "
                f"role {role!r} caps one LLM call at {cap.said(cap.amount)}"
            )
        for use in admitted.uses:
            needed, left = spend(use.limit, sum(sizes)), use.limit.amount - use.used
            if needed > left:
                said = f"the {use.window}" if use.window != "month" else "the month (30 days)"
                raise OverBudget(
                    f"scanning {what} sends at least about {use.limit.said(needed)} to the model, "
                    f"and role {role!r} has {use.limit.said(max(left, 0.0))} left of its LLM "
                    f"budget for {said}"
                )


def require_named(config: dict[str, Any]) -> None:
    """Refuse unless the resolved ``config`` names the scan's model."""
    named = (config.get("ai_models") or {}).get(OPERATION)
    if not (named.get("model") if isinstance(named, dict) else named):
        raise ModelRefused(
            f"no AI model is set for finding terms: choose one under {WHERE_TO_SET}, Find terms"
        )


async def for_role(state: Any, role: str) -> Held:
    """The scan's model call for ``role`` in the bound org, refused (:class:`ModelRefused`) when
    no model is named for it or its vendor has no key."""
    from provisa.core.org_secrets import read_org_api_keys
    from provisa.core.org_settings import resolve_org_config
    from provisa.core.request_context import current_org
    from provisa.llm.client import ProvisaLLMClient
    from provisa.security import llm_budget

    model_db = state.model_db
    config = await resolve_org_config(model_db)
    require_named(config)
    client = ProvisaLLMClient(OPERATION, config=config, api_keys=await read_org_api_keys(model_db))
    problem = client.key_problem()
    if problem is not None:
        raise ModelRefused(
            f"{client.vendor}:{client.model} cannot run: {problem}. Set one under {WHERE_TO_SET}"
        )
    return Held(client, llm_budget.guard(state, role, model_db), current_org.get())
