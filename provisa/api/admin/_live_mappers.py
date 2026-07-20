# Copyright (c) 2026 Kenneth Stott
# Canary: 4b6b9c56-68fd-47f8-be86-c55348492b7e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Admin GraphQL input → core live-config model mapping (REQ-565, REQ-813)."""

from __future__ import annotations


def table_model_from_input(inp, columns, presets, alias):  # REQ-929, REQ-982
    """Convert a TableInput into a core Table model — shared by register/update (identical but for
    ``alias``, which the caller resolves). Keeps the change-detection axes (change_signal, probe_query,
    probe_type, watermark_column) mapped in one place."""
    from provisa.core.models import Table as TableModel
    from provisa.core.models import UniqueConstraint as UniqueConstraintModel

    unique_constraints = [
        UniqueConstraintModel(name=uc.name, columns=list(uc.columns))
        for uc in getattr(inp, "unique_constraints", [])
    ]  # REQ-1093

    return TableModel(
        source_id=inp.source_id,
        domain_id=inp.domain_id,
        schema_name=inp.schema_name,
        table_name=inp.table_name,
        alias=alias,
        description=inp.description,
        columns=columns,
        unique_constraints=unique_constraints,  # REQ-1093
        watermark_column=inp.watermark_column,
        change_signal=inp.change_signal,
        probe_query=inp.probe_query,
        probe_type=inp.probe_type,
        load_protected=inp.load_protected,  # REQ-1141
        off_peak_window=inp.off_peak_window,  # REQ-1141
        off_peak_tz=inp.off_peak_tz,  # REQ-1141
        column_presets=presets,
        view_sql=inp.view_sql or None,
        materialize=inp.materialize,
        mv_refresh_interval=inp.mv_refresh_interval,
        mv_debounce_quiet=inp.mv_debounce_quiet,  # REQ-963
        mv_debounce_max_delay=inp.mv_debounce_max_delay,  # REQ-963
        mv_consistency=inp.mv_consistency,  # REQ-879
        mv_preprocess=inp.mv_preprocess,  # REQ-957
        mv_bitemporal_mode=inp.mv_bitemporal_mode,  # REQ-1162
        mv_bitemporal_key=list(inp.mv_bitemporal_key),  # REQ-1162
        mv_persist=inp.mv_persist,  # REQ-965
        mv_primary_key=list(inp.mv_primary_key),  # REQ-970
        mv_incremental=inp.mv_incremental,  # REQ-969
        data_product=inp.data_product,
        enable_aggregates=inp.enable_aggregates,
        enable_group_by=inp.enable_group_by,
        live=live_model_from_input(inp.live),
    )


def live_model_from_input(inp):  # REQ-565, REQ-813
    """Convert a LiveDeliveryConfigInput into a LiveDeliveryConfig model (None when unset)."""
    if inp is None:
        return None
    from provisa.core.models import LiveDeliveryConfig, LiveKafkaParams, LiveOutputConfig

    return LiveDeliveryConfig(
        strategy=inp.strategy,
        watermark_column=inp.watermark_column,
        poll_interval=inp.poll_interval,
        kafka=(
            LiveKafkaParams(
                topic=inp.kafka.topic,
                format=inp.kafka.format,
                key_column=inp.kafka.key_column,
            )
            if inp.kafka is not None
            else None
        ),
        query_id=inp.query_id,
        outputs=[
            LiveOutputConfig(
                type=o.type,
                topic=o.topic,
                key_column=o.key_column,
                bootstrap_servers=o.bootstrap_servers,
            )
            for o in inp.outputs
        ],
    )
