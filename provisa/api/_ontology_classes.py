# Copyright (c) 2026 Kenneth Stott
# Canary: e2c14479-f53c-4c14-beb8-6477c4e57106
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The schema build's Class tables (REQ-2163): generated over the tables a build serves, from the
glossary's groundings and bindings, and served as views of the ontology schema
(provisa.compiler.ontology_classes). Nothing is stored: they are made again by every build."""

# Requirements: REQ-2163

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from sqlalchemy import select

if TYPE_CHECKING:
    from provisa.core.database import Connection

log = logging.getLogger(__name__)


async def register_class_tables(conn: "Connection", tables: list[dict]) -> None:
    """Add the model's Class tables to ``tables`` and their reads to the view map. A Class that
    cannot be offered is refused by itself, logged and kept with why; the rest are served."""
    from provisa.api.app import state
    from provisa.compiler.ontology_classes import class_tables, whole_body
    from provisa.core.schema_org import glossary_term_refs, glossary_terms, registered_tables

    groundings = {
        r.id: r.class_term_id
        for r in (
            await conn.execute_core(
                select(registered_tables.c.id, registered_tables.c.class_term_id).where(
                    registered_tables.c.class_term_id.is_not(None)
                )
            )
        ).fetchall()
    }
    terms = {
        r.id: r.name
        for r in (
            await conn.execute_core(
                select(glossary_terms.c.id, glossary_terms.c.name).where(
                    glossary_terms.c.retired.is_(False)
                )
            )
        ).fetchall()
    }
    refs = [
        (r.term_id, r.table_id, r.column_name)
        for r in (
            await conn.execute_core(
                select(
                    glossary_term_refs.c.term_id,
                    glossary_term_refs.c.table_id,
                    glossary_term_refs.c.column_name,
                )
            )
        ).fetchall()
    ]
    rows, refused = class_tables(tables, groundings, terms, refs)
    for term_id, reason in refused.items():
        log.error("ontology Class %d is not offered: %s", term_id, reason)
    tables.extend(rows)
    state.class_tables = {row["table_name"]: row for row in rows}
    state.refused_classes = refused
    state.class_bodies = {}
    for row in rows:
        state.view_sql_map[row["table_name"]] = whole_body(row)
