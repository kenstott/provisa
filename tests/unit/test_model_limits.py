# Copyright (c) 2026 Kenneth Stott
# Canary: 3a9f6d12-7c4e-4b8a-9e15-c2d0f8a6b417
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""A model's input and output limits (provisa.llm.limits): the vendor's published figure, the
default pair for a model Provisa does not know, the organization's override, and the fixed
characters-per-token ratio.

Requirements: REQ-464.
"""

from typing import Any

import pytest

from provisa.core.models import AIModelsConfig
from provisa.llm.limits import (
    DEFAULT_LIMITS,
    Limits,
    describe,
    known_limits,
    limits_for,
    tokens_of,
)

NO_CONFIG: dict = {}


def _with_override(vendor: str, model: str, input_tokens: int, output_tokens: int) -> dict:
    return {
        "ai_models": {
            "model_limits": {
                vendor: {model: {"input_tokens": input_tokens, "output_tokens": output_tokens}}
            }
        }
    }


class TestKnownModel:
    def test_a_known_model_takes_the_vendors_published_figures(self):
        assert limits_for("anthropic", "claude-haiku-4-5", NO_CONFIG) == Limits(
            200_000, 64_000, "known"
        )

    def test_a_dated_snapshot_takes_its_models_figures(self):
        assert limits_for("anthropic", "claude-haiku-4-5-20251001", NO_CONFIG) == Limits(
            200_000, 64_000, "known"
        )
        assert limits_for("openai", "gpt-4o-2024-08-06", NO_CONFIG) == Limits(
            128_000, 16_384, "known"
        )

    def test_a_known_prefix_does_not_cover_another_model(self):
        # gpt-4o-mini has its own entry; a name that only starts with a known id is not that model.
        assert known_limits("openai", "gpt-4o-audio-preview") is None
        assert known_limits("anthropic", "claude-opus-4-8-experimental") is None

    def test_the_same_id_under_another_vendor_is_not_known(self):
        assert limits_for("groq", "gpt-4o", NO_CONFIG).source == "default"


class TestUnknownModel:
    def test_an_unknown_model_takes_the_default_pair(self):
        limits = limits_for("ollama", "llama3:8b", NO_CONFIG)
        assert (limits.input_tokens, limits.output_tokens) == (32_000, 4_096)
        assert limits.source == "default"
        assert DEFAULT_LIMITS == (32_000, 4_096)

    def test_an_unknown_vendor_takes_the_default_pair(self):
        assert limits_for("my-gateway", "anything", NO_CONFIG) == Limits(32_000, 4_096, "default")


class TestOverride:
    def test_an_override_wins_over_the_known_figure(self):
        cfg = _with_override("anthropic", "claude-opus-4-8", 150_000, 8_000)
        assert limits_for("anthropic", "claude-opus-4-8", cfg) == Limits(150_000, 8_000, "override")

    def test_an_override_wins_over_the_default(self):
        cfg = _with_override("ollama", "llama3:8b", 8_192, 2_048)
        assert limits_for("ollama", "llama3:8b", cfg) == Limits(8_192, 2_048, "override")

    def test_an_override_for_one_model_leaves_others_alone(self):
        cfg = _with_override("anthropic", "claude-opus-4-8", 150_000, 8_000)
        assert limits_for("anthropic", "claude-opus-4-6", cfg).source == "known"

    def test_a_cleared_override_returns_to_the_known_figure(self):
        cfg = _with_override("anthropic", "claude-opus-4-8", 150_000, 8_000)
        cfg["ai_models"]["model_limits"]["anthropic"].pop("claude-opus-4-8")
        assert limits_for("anthropic", "claude-opus-4-8", cfg) == Limits(
            1_000_000, 128_000, "known"
        )


class TestAnyModel:
    def test_a_model_no_task_is_assigned_to_is_answered(self):
        # The config assigns only Haiku; the helper still answers for an OpenAI model and for an
        # override on a model no task uses.
        cfg: dict[str, Any] = {"ai_models": {"sql_generation": "claude-haiku-4-5-20251001"}}
        assert limits_for("openai", "gpt-4.1", cfg) == Limits(1_047_576, 32_768, "known")
        cfg["ai_models"]["model_limits"] = {
            "mistral": {"mistral-large-latest": {"input_tokens": 128_000, "output_tokens": 4_000}}
        }
        assert limits_for("mistral", "mistral-large-latest", cfg) == Limits(
            128_000, 4_000, "override"
        )

    def test_describe_names_the_figure_a_cleared_override_returns_to(self):
        cfg = _with_override("anthropic", "claude-haiku-4-5", 100_000, 1_000)
        shown = describe("anthropic", "claude-haiku-4-5", cfg)
        assert shown["source"] == "override"
        assert (shown["base_input_tokens"], shown["base_output_tokens"]) == (200_000, 64_000)
        assert shown["base_source"] == "known"
        unknown = describe("ollama", "llama3:8b", NO_CONFIG)
        assert (unknown["base_input_tokens"], unknown["base_output_tokens"]) == (32_000, 4_096)
        assert unknown["base_source"] == "default"


class TestTokensOf:
    @pytest.mark.parametrize(
        ("text", "tokens"),
        [("", 0), ("a", 1), ("abc", 1), ("abcd", 2), ("abcdef", 2), ("x" * 3_001, 1_001)],
    )
    def test_three_characters_per_token_rounded_up(self, text, tokens):
        assert tokens_of(text) == tokens


class TestConfigShape:
    def test_ai_models_config_holds_overrides(self):
        cfg = AIModelsConfig.model_validate(
            {"model_limits": {"openai": {"gpt-4o": {"input_tokens": 1, "output_tokens": 2}}}}
        )
        assert cfg.model_limits["openai"]["gpt-4o"].output_tokens == 2

    def test_ai_models_config_refuses_a_limit_of_zero(self):
        with pytest.raises(ValueError):
            AIModelsConfig.model_validate(
                {"model_limits": {"openai": {"gpt-4o": {"input_tokens": 0, "output_tokens": 2}}}}
            )


class TestClientLimits:
    def test_the_client_answers_its_assigned_models_limits_with_the_orgs_override(self):
        from provisa.llm.client import ProvisaLLMClient

        cfg = _with_override("openai", "gpt-4o", 64_000, 2_000)
        cfg["ai_models"]["sql_generation"] = {"vendor": "openai", "model": "gpt-4o"}
        cfg["ai_models"]["table_selection"] = {"vendor": "openai", "model": "gpt-4o-mini"}
        assert ProvisaLLMClient("sql_generation", config=cfg).limits() == Limits(
            64_000, 2_000, "override"
        )
        assert ProvisaLLMClient("table_selection", config=cfg).limits() == Limits(
            128_000, 16_384, "known"
        )
