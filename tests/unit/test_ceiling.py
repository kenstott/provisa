# Copyright (c) 2025 Kenneth Stott
# Canary: 3688be0f-e573-4176-9358-fec28a05b935
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for ceiling enforcement."""

import pytest

from provisa.registry.ceiling import CeilingViolationError, check_ceiling


APPROVED = "{ orders { id amount region } }"


class TestCeiling:
    def test_exact_match_passes(self):
        check_ceiling(APPROVED, "{ orders { id amount region } }")

    def test_fewer_columns_passes(self):
        check_ceiling(APPROVED, "{ orders { id amount } }")

    def test_single_column_passes(self):
        check_ceiling(APPROVED, "{ orders { id } }")

    def test_extra_column_rejected(self):
        with pytest.raises(CeilingViolationError, match="secret"):
            check_ceiling(APPROVED, "{ orders { id amount secret } }")

    def test_extra_nested_field_rejected(self):
        with pytest.raises(CeilingViolationError, match="name"):
            check_ceiling(APPROVED, "{ orders { id customers { name } } }")

    def test_additional_filter_passes(self):
        """Client can add WHERE filters (restricting within ceiling)."""
        check_ceiling(
            APPROVED,
            '{ orders(where: { region: { eq: "us" } }) { id amount } }',
        )

    def test_error_detail_lists_fields(self):
        with pytest.raises(CeilingViolationError) as exc_info:
            check_ceiling(APPROVED, "{ orders { id extra1 extra2 } }")
        assert "extra1" in str(exc_info.value)
        assert "extra2" in str(exc_info.value)
