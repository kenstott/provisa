# Copyright (c) 2026 Kenneth Stott
# Canary: 07d30ea0-328e-4dc3-b508-4f2068f20bc7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The demo sandboxes' limits (REQ-2006): how many may be open at once, and what each may use.

A demo sandbox is an org of its own on a single-process DuckDB engine (provisa/api/sandbox_tier.py).
Each limit is refused by name when reached, and a named sandbox's visitor is told every one of them
with its value, read from here (the sandbox notice and the hosted-sandbox docs page)."""

# Requirements: REQ-2006, REQ-1913

from __future__ import annotations

from provisa.core.settings_registry import Setting

DECLARED: list[Setting] = [
    Setting(
        key="sandbox.max_concurrent",
        card="limits",
        type="int",
        effect="live",  # read when a sandbox is opened
        # REQ-2006: past it, a new visitor is asked to try again shortly. Sized from the
        # measurement of the cloud coordinator with every published demo open.
        req="REQ-2006",
        env="PROVISA_SANDBOX_MAX_CONCURRENT",
        default=10,
        min=1,
        unit="sandboxes",
    ),
    Setting(
        key="sandbox.template_builds_at_once",
        card="limits",
        type="int",
        effect="restart",  # read when the tier starts building its demos' templates
        # REQ-2006: every template at once kept a 2-vCPU tier too busy to serve its visitors.
        req="REQ-2006",
        env="PROVISA_SANDBOX_TEMPLATE_BUILDS_AT_ONCE",
        default=2,
        min=1,
        unit="builds",
    ),
    Setting(
        key="sandbox.memory_limit",
        card="limits",
        type="int",
        effect="live",  # set on each sandbox's engine when it is built
        req="REQ-2006",
        env="PROVISA_SANDBOX_MEMORY_LIMIT",
        default=256 * 1024 * 1024,
        min=32 * 1024 * 1024,
        unit="bytes",
    ),
    Setting(
        key="sandbox.disk_limit",
        card="limits",
        type="int",
        effect="live",  # checked before a sandbox writes its store or takes an upload
        # REQ-2006: the sandbox's whole folder -- its DuckDB file and its uploads.
        req="REQ-2006",
        env="PROVISA_SANDBOX_DISK_LIMIT",
        default=512 * 1024 * 1024,
        min=16 * 1024 * 1024,
        unit="bytes",
    ),
    Setting(
        key="sandbox.idle_ttl",
        card="limits",
        type="int",
        effect="live",  # read when a sandbox is opened and on each request it serves
        # REQ-2006, REQ-1600: a sandbox no request has reached for this long is deleted.
        req="REQ-2006",
        env="PROVISA_SANDBOX_IDLE_TTL",
        default=3600,
        min=300,
        unit="seconds",
    ),
    # REQ-2006: the AI models a sandbox's visitor uses -- Polly and every completion role (column
    # and table descriptions, relationship discovery, natural-language SQL, table selection), all
    # through one OpenAI-wire endpoint. Off unless both the endpoint and its key are set. Never an
    # embedding model: a sandbox has none (provisa/core/sandbox_ai.py).
    Setting(
        key="sandbox.ai_base_url",
        card="mcp",
        type="str",
        effect="live",  # read whenever a sandbox's configuration is resolved
        req="REQ-2006",
        env="PROVISA_SANDBOX_AI_BASE_URL",
        default=None,
        nullable=True,  # unset: no AI in sandboxes
    ),
    Setting(
        key="sandbox.ai_model",
        card="mcp",
        type="str",
        effect="live",
        req="REQ-2006",
        env="PROVISA_SANDBOX_AI_MODEL",
        # The maintainer's choice, through OpenRouter (2026-10-14).
        default="anthropic/claude-haiku-5.5",
    ),
    Setting(
        key="sandbox.ai_api_key",
        card="mcp",
        type="str",
        effect="live",
        req="REQ-2006",
        env="PROVISA_SANDBOX_AI_API_KEY",
        # A ${secret:NAME} reference to the key, stored in the deployment org's vault and resolved
        # there for every sandbox (never in a sandbox's own vault, which holds nothing).
        default=None,
        nullable=True,  # unset: no AI in sandboxes
        credential=True,
    ),
]
