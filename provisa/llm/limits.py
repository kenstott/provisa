# Copyright (c) 2026 Kenneth Stott
# Canary: 5e2c8a41-9d7b-4f3e-a6c0-1b8f2d7e4c93
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A model's limits, in tokens: the most it reads in one request (input) and the longest answer it
writes (output).

Three places answer, in order: the organization's own figure for that vendor and model (set on the
AI models page, stored at ``ai_models.model_limits.<vendor>.<model>``); the vendor's published
figure, from :data:`KNOWN_LIMITS`; and, for a model in neither, :data:`DEFAULT_LIMITS`. Any
vendor/model pair is answered, whether or not a task is assigned to it, so a caller that lets the
user pick a model for one run sizes its requests the same way.

Text is converted to tokens at one fixed ratio, :data:`CHARS_PER_TOKEN` characters per token, for
every model (:func:`tokens_of`).
"""

# Requirements: REQ-464

from __future__ import annotations

import math
import re
from typing import Any, Literal, NamedTuple

#: Characters per token, one ratio for every model (REQ-464: no per-model ratio).
CHARS_PER_TOKEN = 3

Source = Literal["override", "known", "default"]


class Limits(NamedTuple):
    """A model's input and output limits in tokens, and where they came from."""

    input_tokens: int
    output_tokens: int
    source: Source


# REQ-464 mandates this pair for a model Provisa has no figure for: the requirement names it as
# the value shown, marked as the default, until an administrator sets one. It is the stated
# default, not a guess at the model's real limits.
DEFAULT_LIMITS = (32_000, 4_096)

#: The vendors' published limits, as (input tokens, output tokens), by vendor and model id. A model
#: id here also matches its dated snapshots: the id followed by ``-YYYYMMDD`` or ``-YYYY-MM-DD``
#: (:func:`known_limits`). Only figures read from the vendor's own documentation or SDK are listed;
#: a model whose figures could not be confirmed is left out and takes :data:`DEFAULT_LIMITS`.
KNOWN_LIMITS: dict[str, dict[str, tuple[int, int]]] = {
    # Source: Anthropic's model overview as shipped with its API documentation (the claude-api
    # reference, "Current models" table, cached 2026-11-03): context window and max output.
    # "1M" is 1,000,000 tokens; "128K" 128,000; "200K" 200,000; "64K" 64,000.
    "anthropic": {
        "claude-fable-5-2": (1_000_000, 128_000),
        "claude-mythos-5-2": (1_000_000, 128_000),
        "claude-fable-5-1": (1_000_000, 128_000),
        "claude-mythos-5-1": (1_000_000, 128_000),
        "claude-fable-5": (1_000_000, 128_000),
        "claude-mythos-5": (1_000_000, 128_000),
        "claude-opus-5-5": (1_000_000, 128_000),
        "claude-opus-5": (1_000_000, 128_000),
        "claude-opus-4-8": (1_000_000, 128_000),
        "claude-opus-4-7": (1_000_000, 128_000),
        "claude-opus-4-6": (1_000_000, 128_000),
        "claude-sonnet-5-5": (1_000_000, 128_000),
        "claude-sonnet-5": (1_000_000, 128_000),
        "claude-sonnet-4-6": (1_000_000, 128_000),
        "claude-haiku-5-5": (1_000_000, 128_000),
        "claude-haiku-4-5": (200_000, 64_000),  # snapshot claude-haiku-4-5-20251001
    },
    # Source: OpenAI's model pages, developers.openai.com/api/docs/models/<id>, read 2026-11-15:
    # "context window" and "max output tokens" as printed there.
    "openai": {
        "gpt-5": (400_000, 128_000),  # snapshot gpt-5-2025-08-07
        "gpt-5-mini": (400_000, 128_000),  # snapshot gpt-5-mini-2025-08-07
        "gpt-4.1": (1_047_576, 32_768),  # snapshot gpt-4.1-2025-04-14
        "gpt-4.1-mini": (1_047_576, 32_768),  # snapshot gpt-4.1-mini-2025-04-14
        "gpt-4o": (128_000, 16_384),  # snapshots gpt-4o-2024-08-06, gpt-4o-2024-11-20
        "gpt-4o-mini": (128_000, 16_384),  # snapshot gpt-4o-mini-2024-07-18
    },
}

# A dated snapshot of a model id: "-20251001" or "-2024-08-06" at the end.
_SNAPSHOT = re.compile(r"-(\d{8}|\d{4}-\d{2}-\d{2})$")


def tokens_of(text: str) -> int:
    """How many tokens ``text`` counts as: its length in characters over :data:`CHARS_PER_TOKEN`,
    rounded up."""
    return math.ceil(len(text) / CHARS_PER_TOKEN)


def known_limits(vendor: str, model: str) -> tuple[int, int] | None:
    """The vendor's published (input, output) limits for ``model``, or None when Provisa has none.
    ``model`` matches an id in :data:`KNOWN_LIMITS` exactly, or as that id's dated snapshot."""
    table = KNOWN_LIMITS.get(vendor)
    if table is None:
        return None
    if model in table:
        return table[model]
    return table.get(_SNAPSHOT.sub("", model))


def override_limits(vendor: str, model: str, config: dict[str, Any]) -> tuple[int, int] | None:
    """The organization's own (input, output) limits for ``vendor``/``model`` in ``config`` (a
    resolved configuration, with ``ai_models.model_limits``), or None when it has set none."""
    stored = ((config.get("ai_models") or {}).get("model_limits") or {}).get(vendor) or {}
    entry = stored.get(model)
    if not isinstance(entry, dict):
        return None
    return int(entry["input_tokens"]), int(entry["output_tokens"])


def limits_for(vendor: str, model: str, config: dict[str, Any]) -> Limits:
    """``vendor``/``model``'s limits: the organization's override in ``config``, else the vendor's
    published figure, else :data:`DEFAULT_LIMITS`; ``source`` says which."""
    own = override_limits(vendor, model, config)
    if own is not None:
        return Limits(own[0], own[1], "override")
    known = known_limits(vendor, model)
    if known is not None:
        return Limits(known[0], known[1], "known")
    return Limits(DEFAULT_LIMITS[0], DEFAULT_LIMITS[1], "default")


def assignment(choice: Any) -> tuple[str, str] | None:
    """The (vendor, model) an ``ai_models`` role assignment names, read as
    :class:`provisa.llm.client.ProvisaLLMClient` reads it: the string form is an Anthropic model;
    the dict form names its vendor (Anthropic when absent). None when it names no model."""
    if isinstance(choice, str):
        return ("anthropic", choice) if choice.strip() else None
    if isinstance(choice, dict) and choice.get("model"):
        return str(choice.get("vendor") or "anthropic"), str(choice["model"])
    return None


def describe(vendor: str, model: str, config: dict[str, Any]) -> dict[str, Any]:
    """``vendor``/``model``'s limits as the AI models page shows them: the limits in force and
    their source, and the figure clearing an override returns to (the published one, or the
    default when there is none)."""
    limits = limits_for(vendor, model, config)
    known = known_limits(vendor, model)
    base = known if known is not None else DEFAULT_LIMITS
    return {
        "vendor": vendor,
        "model": model,
        "input_tokens": limits.input_tokens,
        "output_tokens": limits.output_tokens,
        "source": limits.source,
        "base_input_tokens": base[0],
        "base_output_tokens": base[1],
        "base_source": "known" if known is not None else "default",
    }
