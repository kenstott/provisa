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

# Requirements: REQ-258, REQ-260, REQ-261, REQ-282, REQ-285, REQ-336, REQ-338, REQ-339, REQ-340, REQ-341, REQ-342, REQ-343, REQ-344

# Source types that support native LISTEN/NOTIFY
_PG_TYPES = {"postgresql"}

# Source types using MongoDB change streams
_MONGO_TYPES = {"mongodb"}

# Source types using Kafka consumer
_KAFKA_TYPES = {"kafka"}

# Debezium CDC sources (MySQL, SQL Server, Oracle, PostgreSQL via Debezium)
_DEBEZIUM_TYPES = {"debezium"}

# Ingest sources — watermark polling via SQLAlchemy (Phase AS)
_INGEST_TYPES = {"ingest"}

# WebSocket feed sources — connect to external WS URL, receive pushed events
_WEBSOCKET_TYPES = {"websocket"}

# RSS/Atom feed sources — poll HTTP feed, watermark by pubDate/updated
_RSS_TYPES = {"rss"}

# GovData — Calcite JDBC adapter over US government datasets (JPype, polling)
_GOVDATA_TYPES = {"govdata"}

# All other SQL-ish sources fall back to polling
_POLLING_TYPES = {
    "mysql",
    "singlestore",
    "mariadb",
    "sqlserver",
    "oracle",
    "duckdb",
    "snowflake",
    "bigquery",
    "databricks",
    "redshift",
    "clickhouse",
    "elasticsearch",
    "pinot",
    "druid",
    "exasol",
    "delta_lake",
    "iceberg",
    "hive",
    "cassandra",
    "redis",
    "kudu",
    "accumulo",
    "google_sheets",
    "prometheus",
}


def get_provider(
    source_type: str, config: dict[str, Any]
) -> NotificationProvider:  # REQ-258, REQ-260, REQ-261, REQ-285
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

    if source_type in _INGEST_TYPES:
        from provisa.ingest.provider import IngestPollingProvider

        return IngestPollingProvider(  # REQ-336
            engine=config["engine"],
            poll_interval=config.get("poll_interval", 5.0),
        )

    if source_type in _WEBSOCKET_TYPES:
        from provisa.subscriptions.websocket_provider import WebSocketNotificationProvider

        return WebSocketNotificationProvider(  # REQ-338, REQ-339, REQ-340, REQ-341
            url=config["url"],
            subscribe_payload=config.get("subscribe_payload"),
            event_path=config.get("event_path"),
            reconnect_interval=config.get("reconnect_interval", 5.0),
        )

    if source_type in _RSS_TYPES:
        from provisa.subscriptions.rss_provider import RSSNotificationProvider

        return RSSNotificationProvider(  # REQ-342, REQ-343, REQ-344
            url=config["url"],
            poll_interval=config.get("poll_interval", 300.0),
        )

    if source_type in _DEBEZIUM_TYPES:
        from provisa.subscriptions.debezium_provider import DebeziumNotificationProvider

        return DebeziumNotificationProvider(  # REQ-261
            bootstrap_servers=config["bootstrap_servers"],
            topic_prefix=config["topic_prefix"],
            database=config["database"],
            consumer_group_id=config.get("consumer_group_id", "provisa-debezium"),
            schema_registry_url=config.get("schema_registry_url"),
            source_type=config.get("source_type", "postgresql"),
        )

    if source_type in _POLLING_TYPES:
        from provisa.subscriptions.polling_provider import PollingNotificationProvider

        return PollingNotificationProvider(  # REQ-260, REQ-282, REQ-283, REQ-285
            pool=config["pool"],
            poll_interval=config.get("poll_interval", 5.0),
            watermark_column=config.get("watermark_column", "updated_at"),
        )

    if source_type in _GOVDATA_TYPES:
        from provisa.subscriptions.govdata_provider import GovDataPollingProvider

        return GovDataPollingProvider(
            sources=config["sources"],
            table=config["table"],
            watermark_column=config["watermark_column"],
            api_key=config["api_key"],
            schema=config.get("schema", ""),
            poll_interval=config.get("poll_interval", 30.0),
        )

    raise ValueError(f"No subscription provider for source_type={source_type!r}")
