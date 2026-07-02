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
