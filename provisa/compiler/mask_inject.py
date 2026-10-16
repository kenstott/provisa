# Copyright (c) 2026 Kenneth Stott
# Canary: e07027a2-1bdb-4f34-a9eb-2c124971ab05
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Per-role column masking rules (REQ-087).

The rules are applied to a statement by the governed pipeline (compiler/stage2): a masked
column is its mask wherever the statement references it (REQ-740).
"""

# Requirements: REQ-040, REQ-263, REQ-264

from __future__ import annotations

from provisa.security.masking import MaskingRule


# Key: (table_id, role_id) → {column_name: (MaskingRule, data_type)}
MaskingRules = dict[tuple[int, str], dict[str, tuple[MaskingRule, str]]]
