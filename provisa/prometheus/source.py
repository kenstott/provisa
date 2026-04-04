# Copyright (c) 2025 Kenneth Stott
# Canary: c9d0e1f2-a3b4-5678-9012-345678c01237
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Prometheus source mapping — metric-to-table with label columns (REQ-250).

Each Prometheus metric becomes a table. Labels become dimension columns.
Value + timestamp are fixed columns. default_range injects a time window
filter at query time.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

log = logging.getLogger(__name__)


@dataclass
class PrometheusTableConfig:
    """Table mapped from a Prometheus metric."""

    name: str
    metric: str
    labels_as_columns: list[str] = field(default_factory=list)
    value_column: str = "value"
    default_range: str = "1h"


@dataclass
class PrometheusSourceConfig:
    """Prometheus source connection + table mappings."""

    id: str
    url: str = "http://localhost:9090"
    tables: list[PrometheusTableConfig] = field(default_factory=list)


def generate_catalog_properties(config: PrometheusSourceConfig) -> dict[str, str]:
    """Generate Trino Prometheus connector catalog properties."""
    return {
        "connector.name": "prometheus",
        "prometheus.uri": config.url,
    }


def generate_table_definitions(config: PrometheusSourceConfig) -> list[dict]:
    """Generate table definition entries for each configured metric.

    Each metric produces a table with:
    - A timestamp column
    - A value column (named per config)
    - One VARCHAR column per label
    """
    definitions = []
    for table in config.tables:
        columns = [
            {"name": "timestamp", "type": "TIMESTAMP"},
            {"name": table.value_column, "type": "DOUBLE"},
        ]
        for label in table.labels_as_columns:
            columns.append({"name": label, "type": "VARCHAR"})

        entry = {
            "tableName": table.name,
            "metric": table.metric,
            "defaultRange": table.default_range,
            "columns": columns,
        }
        definitions.append(entry)
    return definitions


def discover_schema(
    metric_metadata: dict, metric_name: str
) -> list[dict]:
    """Infer columns from Prometheus metric metadata.

    Args:
        metric_metadata: Dict with keys:
            - ``labels``: list of label names for the metric
            - ``type``: metric type (counter, gauge, histogram, summary)
        metric_name: The metric name.

    Returns:
        List of column definition dicts.
    """
    labels = metric_metadata.get("labels", [])
    metric_type = metric_metadata.get("type", "gauge")

    columns = [
        {"name": "timestamp", "type": "TIMESTAMP"},
        {"name": "value", "type": "DOUBLE"},
    ]

    for label in sorted(labels):
        if label == "__name__":
            continue
        columns.append({"name": label, "type": "VARCHAR"})

    # Histogram/summary metrics have extra columns
    if metric_type == "histogram":
        columns.append({"name": "le", "type": "VARCHAR"})
    elif metric_type == "summary":
        columns.append({"name": "quantile", "type": "VARCHAR"})

    return columns
