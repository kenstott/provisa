# Copyright (c) 2026 Kenneth Stott
# Canary: 61f7d2a8-c04e-4b39-8e1d-a95c37e0b6d4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What PostgreSQL 17 (postgres:17.11, UTF8, en_US.utf8) answered for text_search_corpus: statement
-> ("ok", type oid, rows) or ("err", SQLSTATE, message). Written by record_pg17_text_search.py,
never by hand."""

# Requirements: REQ-2167, REQ-2171
# fmt: off

from __future__ import annotations

from typing import Any

RECORDED: dict[str, tuple[Any, Any, Any]] = {
}
