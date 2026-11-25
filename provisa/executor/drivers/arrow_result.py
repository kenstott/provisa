# Copyright (c) 2026 Kenneth Stott
# Canary: 82e3f77c-218e-4452-96b4-8d6c55e34354
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A DIRECT driver's result read through its source's Arrow interface (REQ-2194).

A warehouse whose client returns Arrow (Snowflake's fetch_arrow_all, Databricks'
``fetchall_arrow``, BigQuery's ``to_arrow``) hands its table to the executor as an
:class:`ArrowResult`: an encoder reads the record batches, and Python rows are built from the
table only when a consumer that takes rows reads them (``arrow_table_rows``).
"""

# Requirements: REQ-2194

from __future__ import annotations

from typing import Any

from provisa.core.ir_arrow import column_values
from provisa.executor.result import ArrowResult


def arrow_table_rows(data: Any) -> list[tuple]:  # noqa: ANN401 — a pyarrow Table or RecordBatch
    """The rows of a source's Arrow table, each value as the IR holds it (``column_values``)."""
    return list(zip(*(column_values(c) for c in data.columns)))


def source_arrow_result(table: Any) -> ArrowResult:  # noqa: ANN401 — a pyarrow Table
    """``table`` as the executor's Arrow result; no row is built here."""
    return ArrowResult(table, to_rows=arrow_table_rows)
