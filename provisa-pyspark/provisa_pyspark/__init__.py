# Copyright (c) 2026 Kenneth Stott
# Canary: 8576e43c-c1e0-47e8-834c-6a932dcb509e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Read governed Provisa tables into Spark (REQ-2270).

``ProvisaDataSource`` is imported on first use: the options, type mapping and Flight modules need
only pyarrow, so they import where pyspark is not installed.
"""

# Requirements: REQ-2270

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from provisa_pyspark.datasource import ProvisaDataSource

__all__ = ["ProvisaDataSource"]


def __getattr__(name: str) -> Any:
    if name == "ProvisaDataSource":
        from provisa_pyspark.datasource import ProvisaDataSource

        return ProvisaDataSource
    raise AttributeError(f"module 'provisa_pyspark' has no attribute {name!r}")
