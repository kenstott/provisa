# Copyright (c) 2026 Kenneth Stott
# Canary: 395be238-a909-4a2b-b727-71a8b4ca2b4f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A ServiceNow source's connection settings (REQ-1954), read by both of its readers: the pgwire
server's model (``pgwire_replica``) and the Trino catalog (``trino_connectors``).

The adapter reads each table through the instance's Table API. It requests only the selected
columns (``sysparm_fields``) and sends a filter (``sysparm_query``) only in a form verified against
a live instance; every other filter, and every sort and limit, is applied to the rows it reads. The
instance's whole table list is offered; a table's columns are read only when it is selected for
registration."""

# Requirements: REQ-1954

from __future__ import annotations

from typing import Any

from provisa.core.secrets import resolve_secrets
from provisa.federation.connector_config import MissingConnectorConfig

INSTANCE_URL_SCHEME = "https://"

#: The table the adapter serves in the source's schema beside the instance's own: one row per table
#: of the instance, its name, its label and its theme (its package's display name, "(no package)"
#: for none). The Register Table list is read from it, so listing the tables reads no table's
#: columns. The leading underscore keeps it apart from every ServiceNow table name.
TABLE_LIST = "_servicenow_tables"

#: The table the adapter serves for the instance's transform maps: ``(name, source_table,
#: target_table, active)``, read for one ``target_table`` at a time. A map's source_table is the
#: import set (staging table) a table bound to it takes its inserts through.
TRANSFORM_MAP_LIST = "_servicenow_transform_maps"

#: The table the adapter serves for one table's columns: ``(table_name, column_name, data_type,
#: ordinal_position)``, read for one ``table_name`` at a time (it refuses a scan of every table),
#: each data_type the PostgreSQL type the registered table will have.
COLUMN_LIST = "_servicenow_columns"


def servicenow_instance_url(source: Any) -> str:
    """The instance's address, from ``base_url`` or ``host`` with its secret reference resolved.
    It must be the full ``https://…`` URL; a missing one, or one without the scheme, is a config
    error naming the field. The scheme is never added for the steward."""
    who = f"servicenow source {source.id!r}"
    raw = source.base_url or source.host
    url = resolve_secrets(raw) if raw else ""
    if not url:
        raise MissingConnectorConfig(f"{who}: requires instanceUrl (the instance's address)")
    if not url.startswith(INSTANCE_URL_SCHEME) or url == INSTANCE_URL_SCHEME:
        raise MissingConnectorConfig(
            f"{who}: instanceUrl must be the full {INSTANCE_URL_SCHEME}… address "
            f"(for example https://acme.service-now.com), got {url!r}"
        )
    return url.rstrip("/")


def servicenow_settings(source: Any) -> dict:
    """The adapter's settings for ``source``: its instance URL, the credential entered for it,
    each resolved at use, the tables registered from it and the import sets those bound to one
    take their inserts through (``served_tables``).
    A missing username or password is a config error naming it."""
    who = f"servicenow source {source.id!r}"
    username = resolve_secrets(source.username) if source.username else ""
    password = resolve_secrets(source.password, credential=True) if source.password else ""
    if not username:
        raise MissingConnectorConfig(f"{who}: requires the username")
    if not password:
        raise MissingConnectorConfig(f"{who}: requires the password")
    settings: dict[str, str | list[str] | dict[str, str]] = {
        "instanceUrl": servicenow_instance_url(source),
        "authType": "basic",
        "username": username,
        "password": password,
    }
    # Only the registered tables are served as tables; with none, the server serves its table
    # and column lists alone.
    from provisa.federation.served_tables import served_import_sets, served_tables

    tables = served_tables(source.id)
    if tables:
        settings["tables"] = list(tables)
    # A table bound to an import set takes its inserts through that set's transform map.
    import_sets = served_import_sets(source.id)
    if import_sets:
        settings["importSet"] = import_sets
    return settings
