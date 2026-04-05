# Copyright (c) 2026 Kenneth Stott
# Canary: 211ec041-48f7-4c5e-b4cb-1a37261f7f6d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Provider registry: maps source_type to NotificationProvider class."""

from __future__ import annotations

import logging
from typing import Any

from provisa.subscriptions.base import NotificationProvider

log = logging.getLogger(__name__)

# Source types that support native LISTEN/NOTIFY
_PG_TYPES = {"postgresql"}

# Source types using MongoDB change streams
_MONGO_TYPES = {"mongodb"}

# Source types using Kafka consumer
_KAFKA_TYPES = {"kafka"}

# All other SQL-ish sources fall back to polling
_POLLING_TYPES = {
    "mysql", "singlestore", "mariadb", "sqlserver", "oracle", "duckdb",
    "snowflake", "bigquery", "databricks", "redshift", "clickhouse",
    "elasticsearch", "pinot", "druid", "exasol", "delta_lake", "iceberg",
    "hive", "cassandra", "redis", "kudu", "accumulo", "google_sheets",
    "prometheus",
}


def get_provider(source_type: str, config: dict[str, Any]) -> NotificationProvider:
    """Instantiate the appropriate provider for *source_type*.

    ``config`` keys vary by provider:
      - pg: ``pool`` (asyncpg Pool)
      - mongo: ``database`` (motor Database)
      - kafka: ``bootstrap_servers``, ``group_id``
      - polling: ``pool``, ``poll_interval``, ``soft_delete_column``
    """
    if source_type in _PG_TYPES:
        from provisa.subscriptions.pg_provider import PgNotificationProvider

        return PgNotificationProvider(pool=config["pool"])

    if source_type in _MONGO_TYPES:
        from provisa.subscriptions.mongo_provider import MongoNotificationProvider

        return MongoNotificationProvider(database=config["database"])

    if source_type in _KAFKA_TYPES:
        from provisa.subscriptions.kafka_provider import KafkaNotificationProvider

        return KafkaNotificationProvider(
            bootstrap_servers=config["bootstrap_servers"],
            group_id=config.get("group_id", "provisa-subscriptions"),
        )

    if source_type in _POLLING_TYPES:
        from provisa.subscriptions.polling_provider import PollingNotificationProvider

        return PollingNotificationProvider(
            pool=config["pool"],
            poll_interval=config.get("poll_interval", 5.0),
            soft_delete_column=config.get("soft_delete_column"),
        )

    raise ValueError(f"No subscription provider for source_type={source_type!r}")
