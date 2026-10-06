# Copyright (c) 2026 Kenneth Stott
# Canary: 3b7e1d52-8c4a-4f09-a6e1-5d2c9b80f417
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""One registered table, read and saved the way the table editor does it.

The editor reads a table through the admin ``tables`` query and saves it through ``updateTable``,
which takes the WHOLE ``TableInput``. A Polly tool that changes one attribute of a table -- a
column's fake, the profiler it belongs to -- does the same: it reads the table through the query's
own row mapper, rebuilds the full input the way ``buildTableUpdateInput``
(provisa-ui/src/pages/tables/helpers.ts) does, changes the one attribute and saves through the
``update_table`` mutation itself, so every check that save runs (capability, the fake check, the
profiler membership rules, dependency guards) runs here too.

Beyond what the editor sends, the input carries the stored fields the read type exposes and the
editor leaves out (load protection, its off-peak window, the modeling role and history, a column's
epoch unit, a Kafka output's role). The editor's own edit form holds those through other controls;
a single-attribute save from here must not reset them to their defaults.
"""

# Requirements: REQ-1934, REQ-1494

from __future__ import annotations

import types
from typing import Any

from sqlalchemy import select


async def read_table(table_id: int) -> Any:
    """The table as the admin ``tables`` query returns it (a ``RegisteredTableType``)."""
    from provisa.api.admin.schema_helpers import _fetch_table_with_columns, _get_pool
    from provisa.core.models import DERIVED_SOURCE_ID
    from provisa.core.schema_org import registered_tables

    pool = await _get_pool()
    async with pool.acquire() as conn:
        row = (
            await conn.execute_core(
                select(registered_tables).where(registered_tables.c.id == table_id)
            )
        ).fetchone()
        if row is None:
            raise ValueError(f"table {table_id} is not registered")
        others = await conn.execute_core(
            select(
                registered_tables.c.source_id,
                registered_tables.c.domain_id,
                registered_tables.c.schema_name,
                registered_tables.c.table_name,
                registered_tables.c.alias,
            ).where(registered_tables.c.source_id != DERIVED_SOURCE_ID)
        )
        all_tables = [dict(r._mapping) for r in others.fetchall()]
        # can_deploy_to_db is not part of the input; the table is read without that check.
        return await _fetch_table_with_columns(conn, dict(row._mapping), all_tables, False)


def _live_input(live: Any) -> Any:
    from provisa.api.admin.types import (
        LiveDeliveryConfigInput,
        LiveKafkaParamsInput,
        LiveOutputConfigInput,
    )

    if live is None:
        return None
    kafka = (
        LiveKafkaParamsInput(
            topic=live.kafka.topic, format=live.kafka.format, key_column=live.kafka.key_column
        )
        if live.strategy == "kafka" and live.kafka is not None
        else None
    )
    return LiveDeliveryConfigInput(
        strategy=live.strategy,
        watermark_column=live.watermark_column,
        poll_interval=live.poll_interval,
        kafka=kafka,
        query_id=live.query_id,
        outputs=[
            LiveOutputConfigInput(
                type=o.type,
                topic=o.topic,
                key_column=o.key_column,
                bootstrap_servers=o.bootstrap_servers,
                role=o.role,
            )
            for o in live.outputs
        ],
    )


def _column_input(c: Any) -> Any:
    from provisa.api.admin.types import ColumnInput

    return ColumnInput(
        name=c.column_name,
        visible_to=list(c.visible_to),
        writable_by=list(c.writable_by),
        unmasked_to=list(c.unmasked_to),
        mask_type=c.mask_type or None,
        mask_pattern=c.mask_pattern or None,
        mask_replace=c.mask_replace or None,
        mask_value=c.mask_value or None,
        mask_precision=c.mask_precision or None,
        alias=c.alias or None,
        description=c.description or None,
        data_type=c.data_type or None,
        path=c.path or None,
        native_filter_type=c.native_filter_type or None,
        is_primary_key=c.is_primary_key,
        is_foreign_key=c.is_foreign_key,
        is_alternate_key=c.is_alternate_key,
        scope=c.scope,
        epoch_unit=c.epoch_unit,
        fake=c.fake or None,
        fake_stable=c.fake_stable,
        synthetic_rule=c.synthetic_rule or None,
    )


def table_input(t: Any) -> Any:
    """The full ``TableInput`` that saves ``t`` back unchanged (helpers.ts buildTableUpdateInput)."""
    from provisa.api.admin.types import ColumnPresetInput, TableInput, UniqueConstraintInput

    return TableInput(
        source_id=t.source_id,
        domain_id=t.domain_id,
        schema_name=t.schema_name,
        table_name=t.table_name,
        alias=t.alias or None,
        description=t.description or None,
        watermark_column=t.watermark_column or None,
        change_signal=t.change_signal or None,
        probe_query=t.probe_query or None,
        probe_type=t.probe_type or None,
        view_sql=t.view_sql or None,
        dq_contract=t.dq_contract or None,
        profiler_source_id=t.profiler_source_id or None,
        query_template=t.query_template or None,
        file_glob=t.file_glob or None,
        source_file_column=t.source_file_column or None,
        load_protected=t.load_protected,
        off_peak_window=t.off_peak_window,
        off_peak_tz=t.off_peak_tz,
        materialize=t.materialize,
        mv_refresh_interval=t.mv_refresh_interval,
        mv_debounce_quiet=t.mv_debounce_quiet,
        mv_debounce_max_delay=t.mv_debounce_max_delay,
        push_debounce_quiet=t.push_debounce_quiet,
        push_debounce_max_delay=t.push_debounce_max_delay,
        mv_consistency=t.mv_consistency,
        mv_preprocess=t.mv_preprocess or None,
        mv_bitemporal_mode=t.mv_bitemporal_mode or None,
        mv_bitemporal_key=list(t.mv_bitemporal_key),
        mv_persist=t.mv_persist,
        # REQ-970: the MV row-identity key IS the table's primary key, as the editor derives it.
        mv_primary_key=[c.column_name for c in t.columns if c.is_primary_key],
        mv_incremental=t.mv_incremental,
        mv_calendar=t.mv_calendar or None,
        mv_grain=t.mv_grain or None,
        mv_allowed_lateness=t.mv_allowed_lateness,
        mv_expected_events=t.mv_expected_events,
        mv_business_day_grain=t.mv_business_day_grain,
        modeling_role=t.modeling_role,
        modeling_history=t.modeling_history,
        # REQ-1443 clause 10: a checker table's product is derived and refused on the row.
        product_id=None if t.dq_contract else t.stored_product_id,
        enable_aggregates=t.enable_aggregates,
        enable_group_by=t.enable_group_by,
        live=_live_input(t.live),
        column_presets=[
            ColumnPresetInput(
                column=p.column,
                source=p.source,
                name=p.name,
                value=p.value,
                data_type=p.data_type,
            )
            for p in t.column_presets
        ],
        unique_constraints=[
            UniqueConstraintInput(name=u.name, columns=list(u.columns))
            for u in t.unique_constraints
        ],
        columns=[_column_input(c) for c in t.columns],
    )


async def save_table(request: Any, table_input_: Any) -> dict:
    """Save through the ``updateTable`` mutation the editor calls; a refusal raises its message."""
    from provisa.api.admin.schema_mutation import Mutation

    info = types.SimpleNamespace(context={"request": request})
    # pyright mistypes strawberry.mutation-decorated methods' call signature (see
    # tools.register_table_now).
    result = await Mutation().update_table(info, table_input_)  # pyright: ignore[reportCallIssue]
    if not result.success:
        raise ValueError(result.message)
    return {
        "message": result.message,
        # REQ-1919: e.g. a config-origin table was edited, and the next config load re-applies it.
        "warnings": [w.message for w in result.warnings],
    }
