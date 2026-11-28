# Copyright (c) 2026 Kenneth Stott
# Canary: 03ed831b-6ea8-4979-b457-ab0269f92ca0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A stand-in model names table for a unit test: the tables SQL serves, each by its logical
``schema.table`` name, as the model build's names table holds them (REQ-194, REQ-2214)."""

from __future__ import annotations

from provisa.compiler.names_table import NamesTable, NameRow, build_names_table


def served_names(*refs: str) -> NamesTable:
    """A names table serving each ``schema.table`` of ``refs`` as a table on SQL."""
    rows = []
    for table_id, ref in enumerate(refs, start=1):
        schema, table = ref.split(".")
        rows.append(
            NameRow(
                "table",
                str(table_id),
                "sql",
                table,
                table,
                f"pg.public.{table}",
                domain_id=schema,
                table_id=table_id,
                scope=("sql", schema),
            )
        )
    return build_names_table(rows)
