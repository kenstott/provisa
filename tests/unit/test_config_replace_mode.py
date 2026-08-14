# Copyright (c) 2026 Kenneth Stott
# Canary: cf59ecc9-f721-4fe0-bda3-6ae653fd23fd
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for the single-writer cluster replace-mode gate (REQ-1229).

Source coverage:
  - provisa/core/config_loader.py — config_replace_mode

REQ-1229: only the primary node may destructively modify the config row store.
PROVISA_ROLE=secondary forces load_config replace=OFF regardless of
PROVISA_CONFIG_REPLACE, so a secondary can never wipe primary-registered rows.
On the primary, replace is opt-in via PROVISA_CONFIG_REPLACE.
"""

import pytest

from provisa.core.config_loader import config_replace_mode


class TestSecondaryNeverReplaces:
    """PROVISA_ROLE=secondary hard-disables replace mode."""

    @pytest.mark.parametrize("replace_flag", ["1", "true", "yes", "TRUE", "Yes"])
    def test_secondary_ignores_replace_flag(self, replace_flag: str) -> None:
        env = {"PROVISA_ROLE": "secondary", "PROVISA_CONFIG_REPLACE": replace_flag}
        assert config_replace_mode(env) is False

    @pytest.mark.parametrize("role", ["secondary", "SECONDARY", " secondary ", "Secondary"])
    def test_secondary_role_is_case_and_whitespace_insensitive(self, role: str) -> None:
        env = {"PROVISA_ROLE": role, "PROVISA_CONFIG_REPLACE": "true"}
        assert config_replace_mode(env) is False


class TestPrimaryOptIn:
    """On the primary, replace is off by default and opt-in via PROVISA_CONFIG_REPLACE."""

    def test_default_role_is_primary_and_replace_defaults_off(self) -> None:
        assert config_replace_mode({}) is False

    def test_explicit_primary_replace_defaults_off(self) -> None:
        assert config_replace_mode({"PROVISA_ROLE": "primary"}) is False

    @pytest.mark.parametrize("replace_flag", ["1", "true", "yes", "TRUE", "Yes"])
    def test_primary_replace_opt_in(self, replace_flag: str) -> None:
        env = {"PROVISA_CONFIG_REPLACE": replace_flag}
        assert config_replace_mode(env) is True

    @pytest.mark.parametrize("replace_flag", ["", "0", "false", "no", "off"])
    def test_primary_non_truthy_flag_stays_off(self, replace_flag: str) -> None:
        env = {"PROVISA_CONFIG_REPLACE": replace_flag}
        assert config_replace_mode(env) is False

    def test_unknown_role_is_treated_as_primary(self) -> None:
        env = {"PROVISA_ROLE": "worker", "PROVISA_CONFIG_REPLACE": "true"}
        assert config_replace_mode(env) is True
