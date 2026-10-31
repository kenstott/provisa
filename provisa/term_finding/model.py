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
named, or no key for it. Every other failure of a call ends the scan naming the model.

What the scan sends is redacted as everything Provisa sends out is (provisa.security.redaction).
"""

# Requirements: REQ-2179, REQ-1174

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from provisa.llm.client import LLMTimeoutError
from provisa.term_finding.scan import ScanFailed

#: The ``ai_models`` entry that names the scan's model.
OPERATION = "glossary_term_finding"
SYSTEM = "You are a data catalog assistant helping a data steward build a business glossary."
#: Where the scan's model and its vendor's key are set, as the admin navigation names it.
WHERE_TO_SET = "Admin, AI Models"

Call = Callable[[str, int], Awaitable[str]]


class ModelRefused(ScanFailed):
    """A scan refused before it starts: its model cannot run. The message says where to set it."""


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
                redact(prompt, self._org_id), system=SYSTEM, max_tokens=max_tokens
            )
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
        return answered.text


def require_named(config: dict[str, Any]) -> None:
    """Refuse unless the resolved ``config`` names the scan's model."""
    named = (config.get("ai_models") or {}).get(OPERATION)
    if not (named.get("model") if isinstance(named, dict) else named):
        raise ModelRefused(
            "no AI model is set for finding terms in a document: choose one under "
            f"{WHERE_TO_SET}, Find terms in a document"
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
