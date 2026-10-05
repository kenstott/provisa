# Copyright (c) 2026 Kenneth Stott
# Canary: af5a8be4-4e4a-43fb-a731-9937386ce07a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A whole-table replica built by a SingleStore PIPELINE (REQ-990, REQ-848 PIPELINE_LAND).

The store loads the file itself: the build table is created as for any build, a one-shot pipeline
loads the origin into it (``START PIPELINE … FOREGROUND``), the pipeline and its credential link are
dropped, and the build table is swapped in. No row passes through Provisa. A refresh does the same
with a new pipeline, because a pipeline remembers the files it has already loaded.

A pipeline that fails raises SingleStore's own error after its pipeline and link are dropped and the
build is abandoned; it is never retried through the relay.
"""

from __future__ import annotations

import csv
import io
from typing import Any

from provisa.federation import singlestore_pipeline as sp
from provisa.federation.data_replicator import BuildOutcome, Method

#: How much of a CSV file is read to find its header line.
_HEADER_PROBE_BYTES = 65_536


def csv_header(location: str, hints: dict) -> list[str]:
    """The header line of the CSV file at ``location`` (``s3://bucket/key``), read with the source's
    resolved S3 hints. Only the head of the file is read; the rows are loaded by the pipeline."""
    from pyarrow import fs as pafs

    s3 = pafs.S3FileSystem(
        access_key=hints.get("access_key_id"),
        secret_key=hints.get("secret_access_key"),
        region=hints.get("region"),
        endpoint_override=hints.get("endpoint") or None,
    )
    with s3.open_input_stream(location.split("://", 1)[1]) as stream:
        head = stream.read(_HEADER_PROBE_BYTES).decode("utf-8-sig")
    first = head.splitlines()[0] if head else ""
    if not first:
        raise sp.PipelineRefused(f"{location} has no header line")
    return next(csv.reader(io.StringIO(first)))


async def build_by_pipeline(
    *,
    source: Any,
    origin: sp.LandOrigin,
    target: Any,
    columns: list[str],
    region_filter: str | None = None,
) -> BuildOutcome:
    """Build the replica ``target`` addresses from ``origin`` through a one-shot pipeline.

    ``target`` is the store's replica target (``SqlAlchemyStoreTarget``), replacing rows in one
    transaction from an unkeyed build table, so the pipeline needs no duplicate-key policy.
    ``columns`` are the replica's column names. The caller binds the org's vault (``org_vault``)
    so the source's secret references resolve."""
    from provisa.core.secrets import resolve_secrets_in_dict
    from provisa.federation.replica_target import ROWS_IN_TRANSACTION

    sp.refuse_region_filter(region_filter, table=f"{target.schema}.{target.table}")
    if origin.source_type == "iceberg":
        raise sp.PipelineRefused(
            f"iceberg on S3 ({origin.location}) lands into SingleStore only on a workspace that "
            f"enables enable_iceberg_ingest, and its catalog configuration is not modelled yet"
        )
    if target.replace_method != ROWS_IN_TRANSACTION:
        raise sp.PipelineRefused(
            f"a SingleStore pipeline loads an unkeyed build table; this store replaces by "
            f"{target.replace_method}"
        )
    hints = resolve_secrets_in_dict(dict(getattr(source, "federation_hints", None) or {}))
    schema = target.schema
    pipeline = sp.pipeline_name(schema, target.table)
    link = sp.link_name(schema, target.table)
    link_ddl = sp.s3_link_ddl(schema, link, hints)
    header = csv_header(origin.location, hints) if origin.format == "csv" else None
    pipe_ddl = sp.file_pipeline_ddl(
        schema=schema,
        pipeline=pipeline,
        link=link,
        origin=origin,
        into_table=target.build_table_name,
        columns=columns,
        csv_header=header,
    )
    finish = sp.teardown(schema, pipeline, link=link)

    await target.begin()
    try:
        # A pipeline left by a build that died is dropped first (its name is the replica's); the
        # link is created OR REPLACED for the same reason.
        await target.run_on_build(
            [finish[0], link_ddl, pipe_ddl, sp.start_foreground(schema, pipeline)]
        )
        rows = await target.build_row_count()
        await target.run_on_build(finish)
    except BaseException:
        try:
            await target.run_on_build(finish, tolerate=(sp.NO_SUCH_LINK,))
        finally:
            await target.abort()
        raise
    await target.swap()
    return BuildOutcome(rows_copied=rows, method=Method.STORE_PIPELINE.value)
