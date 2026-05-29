# Copyright (c) 2026 Kenneth Stott
# Canary: a1b2c3d4-e5f6-7890-bcde-f01234567890
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Multi-vendor LLM abstraction layer using aisuite."""

from __future__ import annotations

import logging
from typing import Optional

log = logging.getLogger(__name__)

# Default config when operation key is absent from yaml
_DEFAULTS: dict[str, dict] = {
    "table_description": {"vendor": "anthropic", "model": "claude-haiku-4-5-20251001"},
    "column_description": {"vendor": "anthropic", "model": "claude-haiku-4-5-20251001"},
    "relationship_inference": {"vendor": "anthropic", "model": "claude-haiku-4-5-20251001"},
    "sql_generation": {"vendor": "anthropic", "model": "claude-opus-4-6"},
    "table_selection": {"vendor": "anthropic", "model": "claude-haiku-4-5-20251001"},
}


class ProviasLLMClient:
    """Vendor-agnostic LLM client backed by aisuite.

    Reads vendor/model from config `ai_models.<operation>` section.
    Supports a fallback vendor/model tried on primary failure.
    """

    def __init__(self, operation: str = "column_description") -> None:
        from provisa.api.admin._config_io import read_config

        cfg = read_config()
        ai_models = cfg.get("ai_models", {})
        op_cfg = ai_models.get(operation, {})

        # Support both legacy string format ("claude-haiku-4-5-20251001") and
        # new dict format ({"vendor": "anthropic", "model": "..."})
        if isinstance(op_cfg, str):
            self._vendor = "anthropic"
            self._model = op_cfg
            self._fallback_vendor: Optional[str] = None
            self._fallback_model: Optional[str] = None
        elif isinstance(op_cfg, dict):
            defaults = _DEFAULTS.get(operation, {"vendor": "anthropic", "model": "claude-haiku-4-5-20251001"})
            self._vendor = op_cfg.get("vendor") or defaults["vendor"]
            self._model = op_cfg.get("model") or defaults["model"]
            fallback = op_cfg.get("fallback")
            if isinstance(fallback, dict):
                self._fallback_vendor = fallback.get("vendor")
                self._fallback_model = fallback.get("model")
            else:
                self._fallback_vendor = None
                self._fallback_model = None
        else:
            defaults = _DEFAULTS.get(operation, {"vendor": "anthropic", "model": "claude-haiku-4-5-20251001"})
            self._vendor = defaults["vendor"]
            self._model = defaults["model"]
            self._fallback_vendor = None
            self._fallback_model = None

    def _make_aisuite_model_id(self, vendor: str, model: str) -> str:
        return f"{vendor}:{model}"

    def _build_messages(self, prompt: str, system: str) -> list[dict]:
        messages = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})
        return messages

    def _complete_sync(self, vendor: str, model: str, prompt: str, system: str, max_tokens: int) -> str:
        import aisuite as ai

        client = ai.Client()
        model_id = self._make_aisuite_model_id(vendor, model)
        messages = self._build_messages(prompt, system)
        response = client.chat.completions.create(
            model=model_id,
            messages=messages,
            max_tokens=max_tokens,
        )
        return response.choices[0].message.content.strip()

    def complete_sync(self, prompt: str, system: str = "You are a helpful assistant.", max_tokens: int = 1024) -> str:
        """Complete a prompt synchronously.

        Tries primary vendor/model first; on failure tries fallback if configured.
        Returns plain text response.
        """
        try:
            result = self._complete_sync(self._vendor, self._model, prompt, system, max_tokens)
            return result
        except Exception as primary_exc:
            log.warning(
                "LLM primary vendor=%s model=%s failed: %s",
                self._vendor, self._model, primary_exc,
            )
            if self._fallback_vendor and self._fallback_model:
                try:
                    result = self._complete_sync(
                        self._fallback_vendor, self._fallback_model, prompt, system, max_tokens
                    )
                    return result
                except Exception as fallback_exc:
                    log.warning(
                        "LLM fallback vendor=%s model=%s failed: %s",
                        self._fallback_vendor, self._fallback_model, fallback_exc,
                    )
                    raise fallback_exc
            raise primary_exc

    async def complete(
        self,
        prompt: str,
        system: str = "You are a helpful assistant.",
        max_tokens: int = 1024,
    ) -> str:
        """Complete a prompt asynchronously.

        Tries primary vendor/model first; on failure tries fallback if configured.
        Returns plain text response.
        """
        import asyncio

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            lambda: self.complete_sync(prompt, system, max_tokens),
        )
