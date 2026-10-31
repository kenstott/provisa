# Copyright (c) 2026 Kenneth Stott
# Canary: a4d1e8f6-3b57-4c02-9e71-60f2c8db5e14
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The statements the JSON and Range families answer, by the overload each proves. What PostgreSQL
17 answered to each is in tests/unit/pgwire/pg17_json_range_answers.py."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

NESTED = """'{"b":[1,{"z":null,"y":2.50}],"a":"x\\ty","c":1e2}'"""

#: overload -> statements.
CORPUS: dict[str, list[str]] = {}
