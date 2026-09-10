# Copyright (c) 2026 Kenneth Stott
# Canary: a2f607a5-46d3-4bee-a094-3aaaf9ed6483
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The registered sources and tables a landing path drives off (REQ-1674).

The config file is not the registry. A source created through the Sources page and a table
registered through Register Table live in the control plane and never in ``state.config``, so a
landing path that read ``config.sources``/``config.tables`` — the event-loop wiring, the query-time
residency check, the pre-read land — saw none of them: the query reached the landed-replica name
before anything had landed it. These two readers give every landing path the same view: the
control plane's rows, with the config's per-source/per-table settings (passwords are secret refs
only the config carries; change signal, cadence, live block) laid over them where the config
declares the same id.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from provisa.core.models import BUILT_IN_SOURCE_IDS, Source


def _source_from_row(row: dict) -> Source:
    """A Source model from a control-plane row: the row's columns that are model fields, set.

    REQ-1695: including its password, which the row carries as the ``password_ref`` reference.
    """
    from provisa.core.repositories.source import source_from_row

    return source_from_row(row)


async def registered_sources(state: Any, conn: Any | None = None) -> list[Source]:  # REQ-1674
    """Every registered source: the config's Source where the config declares the id (it carries
    the operator's settings), else the control-plane row -- which since REQ-1695 carries its own
    password reference too. Built-in sources (provisa-admin, provisa-otel, the derived-view source)
    are never landed and stay out."""
    from provisa.core.repositories import source as source_repo

    config = getattr(state, "config", None)
    by_id: dict[str, Source] = {s.id: s for s in (getattr(config, "sources", None) or [])}
    db = getattr(state, "tenant_db", None)
    if db is None:
        return list(by_id.values())
    if conn is not None:
        rows = await source_repo.list_all(conn)
    else:
        async with db.acquire() as _conn:
            rows = await source_repo.list_all(_conn)
    for row in rows:
        sid = row["id"]
        if sid in by_id or sid in BUILT_IN_SOURCE_IDS:
            continue
        by_id[sid] = _source_from_row(row)
    return list(by_id.values())


async def registered_tables(state: Any, conn: Any | None = None) -> list[Any]:  # REQ-1674
    """Every registered table, in the shape the landing paths read: the control plane's semantic
    sql name and resolved column types, with the config table's landing settings (live block, change
    signal, watermark, cadence, probe) where the config declares the same source + table."""
    from provisa.api.admin.db_queries import fetch_tables
    from provisa.compiler.naming import apply_sql_name

    config = getattr(state, "config", None)
    cfg_by = {
        (t.source_id, apply_sql_name(t.table_name)): t
        for t in (getattr(config, "tables", None) or [])
    }
    db = getattr(state, "tenant_db", None)
    if db is None:
        return []
    if conn is not None:
        registered = await fetch_tables(conn)
    else:
        async with db.acquire() as _conn:
            registered = await fetch_tables(_conn)
    out: list[Any] = []
    for rt in registered:
        cfg = cfg_by.get((rt["source_id"], rt["table_name"]))
        out.append(
            SimpleNamespace(
                source_id=rt["source_id"],
                schema_name=rt["schema_name"],
                table_name=rt["table_name"],
                columns=[
                    SimpleNamespace(
                        name=c["column_name"],
                        data_type=c["data_type"],
                        is_primary_key=c["is_primary_key"],
                        native_filter_type=c["native_filter_type"],
                    )
                    for c in rt["columns"]
                ],
                live=getattr(cfg, "live", None),
                change_signal=getattr(cfg, "change_signal", None),
                watermark_column=getattr(cfg, "watermark_column", None),
                cache_ttl=getattr(cfg, "cache_ttl", None),
                probe_type=getattr(cfg, "probe_type", None),  # REQ-982
                # REQ-1443: a checker table's rows are the results of running its contract, so
                # the registered contract rides with the table into make_dq_loader.
                dq_contract=rt["dq_contract"],
            )
        )
    return out
