# Copyright (c) 2026 Kenneth Stott
# Canary: f42b25cf-3a3e-4828-ac6e-e6fa13e503d3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Python source's code, as an organisation would write it: no import of Provisa's, tags in
docstrings, types in hints (REQ-1993). The same API as the Node fixture's. A table or stream takes
its inputs as one object (REQ-2100), and none here asks for its read context."""

from __future__ import annotations

import asyncio
import enum
import socket
import time
import urllib.error
import urllib.request
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal

print("importing orders")  # what code prints never reaches the protocol


class Tier(enum.Enum):
    GOLD = "gold"
    SILVER = "silver"


@dataclass
class Line:
    sku: str
    qty: int


@dataclass
class Order:
    id: int
    placed: datetime
    due: date
    total: Decimal
    tier: Tier
    lines: list[Line]
    note: str | None = None


@dataclass
class OrdersInputs:
    customer_id: str
    since: datetime | None = None


def orders(inputs: OrdersInputs) -> list[Order]:
    """Orders of one customer.

    @table
    """
    customer_id, since = inputs.customer_id, inputs.since
    note = since.isoformat() if since else None
    placed = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    return [
        Order(
            2**60,
            placed,
            date(2026, 1, 9),
            Decimal("12.50"),
            Tier.GOLD,
            [Line(customer_id, 2)],
            note,
        )
    ]


@dataclass
class ManyInputs:
    count: int


def many(inputs: ManyInputs) -> list[dict]:
    """Rows of no declared shape: typed by a preview.

    @table
    """
    return [{"i": i} for i in range(inputs.count)]


@dataclass
class PlacedInputs:
    region: str


async def placed(inputs: PlacedInputs) -> AsyncIterator[Order]:
    """Orders as they are placed.

    @stream
    """
    region = inputs.region
    n = 0
    while True:
        yield Order(
            n, datetime.now(timezone.utc), date.today(), Decimal(n), Tier.SILVER, [], region
        )
        n += 1
        await asyncio.sleep(0.01)


def ticks() -> Iterator[dict]:
    """@stream"""
    n = 0
    while True:
        yield {"tick": n}
        n += 1
        time.sleep(0.01)


async def forever() -> AsyncIterator[dict]:
    """@stream"""
    await asyncio.Event().wait()
    yield {}


def cancel_order(order_id: int) -> dict:
    """@command"""
    return {"cancelled": order_id}


def boom() -> list[dict]:
    """@table"""
    raise LookupError("no such customer")


def outside() -> list[dict]:
    """@table"""
    with open("/etc/hosts") as hosts:
        return [{"length": len(hosts.read())}]


@dataclass
class FetchedInputs:
    url: str


def fetched(inputs: FetchedInputs) -> list[dict]:
    """@table"""
    try:
        with urllib.request.urlopen(inputs.url, timeout=10) as response:
            return [{"status": response.status, "body": response.read().decode()}]
    except urllib.error.HTTPError as refused:
        return [{"status": refused.code, "body": refused.read().decode()}]


@dataclass
class DirectInputs:
    host: str
    port: int


def direct(inputs: DirectInputs) -> list[dict]:
    """@table"""
    with socket.create_connection((inputs.host, inputs.port), timeout=5):
        return [{"connected": True}]


@dataclass
class GreetInputs:
    name: str
    greeting: str = "hello"
    punctuation: str = "!"


def greet(inputs: GreetInputs) -> list[dict]:
    """@table"""
    return [{"text": f"{inputs.greeting} {inputs.name}{inputs.punctuation}"}]


@dataclass
class Address:
    street: str
    city: str
    country: str


def clean_address(raw_address: list[str]) -> list[Address]:
    """A batch column transform (REQ-2131): each raw address of the batch, as its parts.

    @columns
    """
    out = []
    for raw in raw_address:
        street, city, country = (part.strip() for part in raw.split(","))
        out.append(Address(street, city.title(), country.upper()))
    return out


def short_address(raw_address: list[str]) -> list[Address]:
    """One result too few for its batch: refused naming it.

    @columns
    """
    return clean_address(raw_address)[:-1]


def helper() -> int:
    return 1
