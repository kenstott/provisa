# Copyright (c) 2026 Kenneth Stott
# Canary: 7337be0b-6667-4a2c-bdb9-6d594d79490d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""OTel span attributes derived from governed semantic SQL.

The ops `queries` report reads the `otel.signals.traces` table, whose table_name/domain_id/
role_id/query_text columns are populated from the `provisa.*` span attributes listed in
``provisa.observability.ops_schema.TRACE_ATTR_COLS``. A query span carries those attributes
only when the executor is handed this dict, so every governed execution mints one here.
"""

from __future__ import annotations

import sqlglot
import sqlglot.expressions as _sge


def span_attrs_from_semantic_sql(
    semantic_sql: str,
    role_id: str,
    query_text: str | None = None,
    no_table_label: str = "cypher",
) -> dict[str, str]:
    """Extract normalized table names from semantic SQL for OTel span attributes.

    Semantic SQL uses domain_to_sql_name(domain_id) as schema, e.g. pet_store.pets.
    sqlglot Table nodes expose .db (schema/domain) and .name (table).

    ``no_table_label`` names the surface for a statement with no parsable FROM table, so the
    ops report shows where a table-less query came from instead of an empty cell.
    """
    tables: set[str] = set()
    domains: set[str] = set()
    try:
        parsed = sqlglot.parse_one(semantic_sql, dialect="postgres")
        # Walk only the primary FROM clause — LATERAL joins (ops/meta traversal) live in
        # parsed.args["joins"] and must be excluded so provisa.table is a single root
        # table name rather than a comma list that never matches the exact-match join condition.
        # sqlglot 30 renamed the FROM arg to "from_"; unpinned dependency, both keys are read
        # for the same reason provisa/compiler/sql_validator.py does.
        from_node = parsed.args.get("from_") or parsed.args.get("from")
        if from_node:
            for tbl in from_node.find_all(_sge.Table):
                db = tbl.db
                name = tbl.name
                if db:
                    tables.add(f"{db}.{name}")
                    domains.add(db)
                elif name:
                    tables.add(name)
    except Exception:
        pass
    attrs: dict[str, str] = {
        "provisa.table": ", ".join(sorted(tables)) or no_table_label,
        "provisa.domain": ", ".join(sorted(domains)) or no_table_label,
        "provisa.role": role_id,
    }
    if query_text is not None:
        attrs["provisa.query_text"] = query_text
    return attrs
