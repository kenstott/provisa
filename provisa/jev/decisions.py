# Copyright (c) 2026 Kenneth Stott
# Canary: 001e1a89-79ca-426e-ad73-2bf6ed1516cb
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Optional Jev-assisted confidence scoring for generative-LLM glossary proposals.

Glossary relationship-type proposals (provisa.api.admin.glossary_router) run a generative LLM to
PROPOSE candidates; this module runs a second Jev pass to SCORE what was proposed with a
calibrated confidence, gating the generating model's own self-reported confidence. Relationship
discovery asks Jev through provisa.discovery.rescoring, with the run's evidence.

Only called when a Jev key resolves (provisa.core.org_secrets.org_ai_key_or_none) — the
caller decides whether to invoke this module at all, so its absence is never a silent
fallback here. A Jev call that itself fails (network, HTTP error) IS handled inside these
functions: this is a refinement layered on an already-succeeded generative pass, not a
dependency that pass needs to succeed, so a failure here is logged and the untouched
proposals are returned rather than losing the whole batch.
"""

from __future__ import annotations

import logging
from typing import Any

log = logging.getLogger(__name__)


async def refine_glossary_edges(
    proposals: list[dict], terms_by_name: dict[str, dict], api_key: str, min_confidence: float
) -> list[dict]:
    """Re-score each ``{"from", "to", "rel_type"}`` proposal via a Jev noul question, dropping
    proposals whose Jev-scored plausibility falls below ``min_confidence``.

    ``terms_by_name`` supplies each term's definition for the question's state (name -> row
    dict with a ``definition`` key); a name absent from it (already-filtered upstream) just
    yields an empty definition in the prompt.
    """
    if not proposals:
        return proposals

    from provisa.jev.client import evaluate

    questions = []
    for i, p in enumerate(proposals):
        from_name = str(p.get("from") or "")
        to_name = str(p.get("to") or "")
        from_def = (terms_by_name.get(from_name) or {}).get("definition", "")
        to_def = (terms_by_name.get(to_name) or {}).get("definition", "")
        questions.append(
            {
                "id": str(i),
                "type": "noul",
                "instructions": (
                    f"Is it correct that business-glossary term '{from_name}' ({from_def}) "
                    f"has relationship {p.get('rel_type')} to term '{to_name}' ({to_def})?"
                ),
                "criteria": {
                    "true": "a correct, meaningful relationship worth recording",
                    "false": "incorrect, or too speculative to record",
                },
            }
        )
    try:
        result = await evaluate(api_key, {"proposal_count": len(proposals)}, questions)
    except Exception:
        log.warning(
            "Jev glossary relationship scoring failed; keeping unscored proposals", exc_info=True
        )
        return proposals

    answers: dict[str, Any] = result.get("answers", {})
    out: list[dict] = []
    for i, p in enumerate(proposals):
        answer = answers.get(str(i))
        if answer is not None and float(answer.get("noul", 1.0)) < min_confidence:
            log.info("Jev dropped glossary relationship proposal below threshold: %s", p)
            continue
        out.append(p)
    return out
