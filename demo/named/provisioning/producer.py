# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
#
"""The provisioning demo's feeds: the `producer` service publishes three topics, one JSON object a
message, for as long as it runs.

  orders     each order: its id, product, quantity, the price it sold at and when. A month of
             orders is published first (ORDERS_HISTORY of them), then a few a second.
  prices     each product's list price; one changes every PRICE_EVERY seconds.
  inventory  each product's stock on hand; one is counted every INVENTORY_EVERY seconds.

Prices and stock are keyed by product: the latest message for a product is its current value."""

from __future__ import annotations

import asyncio
import json
import os
import random
import uuid
from datetime import datetime, timedelta, timezone

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition

BOOTSTRAP = os.environ["FEEDS_BOOTSTRAP"]
HISTORY = int(os.environ.get("ORDERS_HISTORY", "200000"))
PER_SECOND = float(os.environ.get("ORDERS_PER_SECOND", "5"))
PRICE_EVERY = float(os.environ.get("PRICE_EVERY", "20"))
INVENTORY_EVERY = float(os.environ.get("INVENTORY_EVERY", "3"))

# Each product (db/init.sql), its list price to start from, and how often it sells.
_PRODUCTS = {
    "BN-ETH": (24.00, 9),
    "BN-COL": (21.00, 10),
    "BN-SUM": (22.50, 6),
    "BN-DEC": (26.00, 3),
    "BN-ESP": (19.50, 12),
    "EQ-GRD": (149.00, 2),
    "EQ-KTL": (59.00, 2),
    "EQ-DRP": (24.00, 4),
    "EQ-SCL": (45.00, 3),
    "MR-MUG": (14.00, 5),
    "MR-TOT": (12.00, 3),
    "SV-CLS": (65.00, 1),
    "SV-SUB": (9.00, 4),
}
_price = {sku: price for sku, (price, _) in _PRODUCTS.items()}
_on_hand = {sku: random.randint(40, 400) for sku in _PRODUCTS}


def _order(at: datetime) -> dict:
    skus = list(_PRODUCTS)
    sku = random.choices(skus, [_PRODUCTS[s][1] for s in skus])[0]
    # A sale may be discounted from the list price: up to 15% off, a third of the time.
    discount = random.choice([1.0, 1.0, random.uniform(0.85, 1.0)])
    return {
        "order_id": uuid.uuid4().hex,
        "sku": sku,
        "quantity": random.choice([1, 1, 1, 2, 2, 3]),
        "unit_price": round(_price[sku] * discount, 2),
        "ordered_at": at.isoformat(),
    }


async def _send(producer: AIOKafkaProducer, topic: str, value: dict) -> None:
    await producer.send(topic, json.dumps(value).encode())


async def _history(producer: AIOKafkaProducer) -> None:
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=30)
    step = (now - start) / max(HISTORY, 1)
    for i in range(HISTORY):
        await _send(producer, "orders", _order(start + step * i))
    await producer.flush()
    print(f"published {HISTORY} orders of the last 30 days", flush=True)


async def _orders_published() -> bool:
    """Whether the broker already holds orders: a restarted producer publishes the month of history
    once, never a second month of different orders."""
    consumer = AIOKafkaConsumer(bootstrap_servers=BOOTSTRAP)
    await consumer.start()
    try:
        partitions = await consumer.partitions_for_topic("orders") or set()
        ends = await consumer.end_offsets([TopicPartition("orders", p) for p in partitions])
        return any(ends.values())
    finally:
        await consumer.stop()


async def _orders(producer: AIOKafkaProducer) -> None:
    while True:
        await _send(producer, "orders", _order(datetime.now(timezone.utc)))
        await asyncio.sleep(random.expovariate(PER_SECOND))


async def _prices(producer: AIOKafkaProducer) -> None:
    while True:
        await asyncio.sleep(PRICE_EVERY)
        sku = random.choice(list(_PRODUCTS))
        _price[sku] = round(_price[sku] * random.uniform(0.95, 1.05), 2)
        await _send(producer, "prices", _price_message(sku))


async def _inventory(producer: AIOKafkaProducer) -> None:
    while True:
        await asyncio.sleep(INVENTORY_EVERY)
        sku = random.choice(list(_PRODUCTS))
        _on_hand[sku] = max(0, _on_hand[sku] + random.randint(-25, 30))
        await _send(producer, "inventory", _stock_message(sku))


def _price_message(sku: str) -> dict:
    return {"sku": sku, "list_price": _price[sku], "changed_at": _now()}


def _stock_message(sku: str) -> dict:
    return {"sku": sku, "on_hand": _on_hand[sku], "counted_at": _now()}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


async def main() -> None:
    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP, linger_ms=50)
    await producer.start()
    try:
        for sku in _PRODUCTS:  # every product's price and stock, before any order names it
            await _send(producer, "prices", _price_message(sku))
            await _send(producer, "inventory", _stock_message(sku))
        if not await _orders_published():
            await _history(producer)
        print(f"producing about {PER_SECOND:g} orders a second on {BOOTSTRAP}", flush=True)
        await asyncio.gather(_orders(producer), _prices(producer), _inventory(producer))
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
