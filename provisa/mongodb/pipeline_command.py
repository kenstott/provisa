# Copyright (c) 2026 Kenneth Stott
# Canary: ada82b9a-1220-4d15-9462-8e26a825c887
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A query command that runs a MongoDB aggregation pipeline (REQ-1988).

The command's ``binding`` is the native query, ``{"inputCollection": ..., "pipeline": [...]}``,
as a pipeline table's is (REQ-1980); its arguments are the command's arguments, each typed, and a
``"{{name}}"`` placeholder names one. A call converts each argument to its type and binds it into
the pipeline as a BSON value -- never into its text -- and returns the documents the pipeline
gives: as JSON, since their shape may differ from document to document, or checked against the
command's ``output_columns`` when it declares them (the dispatcher's output contract). A command
that writes ($out, $merge) is refused, as a pipeline table is.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

IMPL_KIND = "source_query"


async def run_command(state: Any, fn: dict, args: dict) -> list[dict]:
    """Run the pipeline command ``fn`` with ``args`` (by argument name) and return its rows."""
    from bson import json_util

    from provisa.core.parameter_values import convert
    from provisa.federation.registry_view import registered_sources
    from provisa.mongodb.fetch import MongoConnection, aggregate_row_batches
    from provisa.mongodb.native_query import bind, check, native_query_text

    name = fn["name"]
    declared = fn.get("arguments") or []
    query = check(name, native_query_text(fn["binding"]), {a["name"] for a in declared})
    values = {
        a["name"]: (
            convert(args.get(a["name"]), a["type"], table=name, parameter=a["name"]),
            a["type"],
        )
        for a in declared
    }
    pipeline = bind(query, values)
    found = [s for s in await registered_sources(state) if s.id == fn["source_id"]]
    if not found or found[0].type.value != "mongodb":
        raise ValueError(f"Command {name!r}: its source {fn['source_id']!r} is no MongoDB source")
    source = found[0]
    from provisa.core.secrets import resolve_secrets

    conn = MongoConnection.from_source(source)
    # Resolved here, at use, as its connection's host and credentials are (MongoConnection).
    database = resolve_secrets(source.database) if source.database else fn["schema_name"]

    def _run() -> list[dict]:
        return [
            doc
            for batch in aggregate_row_batches(
                conn, database, query.input_collection, pipeline, 1000
            )
            for doc in batch
        ]

    docs = await asyncio.to_thread(_run)
    # Relaxed Extended JSON: a document's ObjectId, dates and decimals as JSON spells them.
    return [
        json.loads(json_util.dumps(doc, json_options=json_util.RELAXED_JSON_OPTIONS))
        for doc in docs
    ]
