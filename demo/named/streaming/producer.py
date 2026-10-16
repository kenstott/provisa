# Copyright (c) 2026 Kenneth Stott
# Canary: 9a906516-5ff0-4977-9c96-59332d5f873c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
#
"""The streaming demo's live order feed: the `producer` service publishes a few orders a second
to the Kafka topic `orders`, one JSON object a message, for as long as it runs. Each order names
a product of the catalog (db/init.sql) and the region it was placed in; some regions buy more
than others, so the busiest regions change slowly and the answer has a shape."""

from __future__ import annotations

import asyncio
import json
import os
import random
import uuid
from datetime import datetime, timezone

from aiokafka import AIOKafkaProducer

BOOTSTRAP = os.environ["ORDERS_BOOTSTRAP"]
TOPIC = "orders"
PER_SECOND = float(os.environ.get("ORDERS_PER_SECOND", "4"))

# How often each region orders, relative to the others; drifted a little every minute.
_WEIGHTS = {
    "na-east": 5.0,
    "na-west": 4.0,
    "eu-west": 3.5,
    "eu-central": 2.5,
    "apac": 3.0,
    "latam": 1.5,
}
_PRODUCTS = [1, 2, 3, 4, 5, 6, 7, 8]
_PRODUCT_WEIGHTS = [6, 2, 2, 1, 5, 3, 2, 3]


def _order() -> dict:
    regions = list(_WEIGHTS)
    return {
        "order_id": uuid.uuid4().hex,
        "product_id": random.choices(_PRODUCTS, _PRODUCT_WEIGHTS)[0],
        "region_id": random.choices(regions, [_WEIGHTS[r] for r in regions])[0],
        "quantity": random.choice([1, 1, 1, 2, 2, 3]),
        "ordered_at": datetime.now(timezone.utc).isoformat(),
    }


def _drift() -> None:
    for region in _WEIGHTS:
        _WEIGHTS[region] = max(0.5, _WEIGHTS[region] * random.uniform(0.85, 1.15))


async def main() -> None:
    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP)
    await producer.start()
    print(f"producing about {PER_SECOND:g} orders a second to {TOPIC} on {BOOTSTRAP}", flush=True)
    try:
        tick = 0
        while True:
            await producer.send_and_wait(TOPIC, json.dumps(_order()).encode())
            tick += 1
            if tick % int(60 * PER_SECOND) == 0:
                _drift()
            await asyncio.sleep(random.expovariate(PER_SECOND))
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
