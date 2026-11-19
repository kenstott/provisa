# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""trade_match: one allocation's four records -- the order system's allocation, the execution
system's fills, the broker's confirmation and the custodian's settlement instruction -- matched,
and each break between them named.

A leg that has not arrived is a break of its own (no_confirmation, no_instruction), and the legs
it would have been compared on are not compared. A net amount is compared only when quantity and
price agree: a net amount off because the quantity or the price is off is that break, not two."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

#: How far the broker's price may sit from the average execution price, as a fraction of it.
PRICE_TOLERANCE = 0.0001
#: How far the broker's net amount may sit from the custodian's, in the trade's currency.
NET_TOLERANCE = 0.01


@dataclass
class Match:
    trade_ref: str
    status: str  # matched, or break
    break_type: str | None  # empty when matched
    detail: str | None  # what differs, in words; empty when matched


@dataclass
class TradeLegs:
    #: The allocation's reference, which the broker's confirmation and the custodian's
    #: instruction both carry.
    trade_ref: str
    #: The order system's side: BUY or SELL.
    side: str
    #: The quantity allocated to the fund.
    quantity: float
    #: The settlement date the order system expects.
    settle_date: date
    #: The average price of the order's fills, from the execution system.
    exec_price: float
    #: The custodian and safekeeping account the fund's standing instructions name for the market.
    ssi_custodian: str
    ssi_account: str
    #: The broker's confirmation; absent while it has not arrived.
    confirm_side: str | None = None
    confirm_quantity: float | None = None
    confirm_price: float | None = None
    confirm_net: float | None = None
    confirm_settle_date: date | None = None
    confirm_custodian: str | None = None
    confirm_account: str | None = None
    #: The custodian's settlement instruction; absent while it has not been sent.
    instruction_quantity: float | None = None
    instruction_net: float | None = None
    instruction_settle_date: date | None = None


def _qty(value: float) -> str:
    return f"{value:,.0f}"


def _confirmation_breaks(legs: TradeLegs) -> list[tuple[str, str]]:
    """The breaks between the allocation and the broker's confirmation, which has arrived."""
    found: list[tuple[str, str]] = []
    quantity, price = legs.confirm_quantity, legs.confirm_price
    settle = legs.confirm_settle_date
    assert quantity is not None and price is not None and settle is not None
    if legs.confirm_side != legs.side:
        found.append(("side", f"allocated {legs.side}, confirmed {legs.confirm_side}"))
    if quantity != legs.quantity:
        found.append(("quantity", f"allocated {_qty(legs.quantity)}, confirmed {_qty(quantity)}"))
    if abs(price - legs.exec_price) > PRICE_TOLERANCE * legs.exec_price:
        found.append(("price", f"executed at {legs.exec_price:.4f}, confirmed at {price:.4f}"))
    if settle != legs.settle_date:
        found.append(
            (
                "settlement_date",
                f"expected {legs.settle_date.isoformat()}, confirmed {settle.isoformat()}",
            )
        )
    if (legs.confirm_custodian, legs.confirm_account) != (legs.ssi_custodian, legs.ssi_account):
        found.append(
            (
                "ssi",
                f"standing instructions {legs.ssi_custodian} {legs.ssi_account}, "
                f"confirmed to {legs.confirm_custodian} {legs.confirm_account}",
            )
        )
    return found


def _instruction_breaks(legs: TradeLegs) -> list[tuple[str, str]]:
    """The breaks between the allocation and the custodian's instruction, which has been sent."""
    found: list[tuple[str, str]] = []
    quantity, settle = legs.instruction_quantity, legs.instruction_settle_date
    assert quantity is not None and settle is not None
    if quantity != legs.quantity:
        found.append(
            (
                "instruction_quantity",
                f"allocated {_qty(legs.quantity)}, instructed {_qty(quantity)}",
            )
        )
    if settle != legs.settle_date:
        found.append(
            (
                "instruction_settlement_date",
                f"expected {legs.settle_date.isoformat()}, instructed {settle.isoformat()}",
            )
        )
    return found


def breaks(legs: TradeLegs) -> list[tuple[str, str]]:
    """Each break between the legs, as (break type, what differs), in a fixed order."""
    found: list[tuple[str, str]] = []
    confirmed = legs.confirm_quantity is not None
    instructed = legs.instruction_quantity is not None
    if not confirmed:
        found.append(("no_confirmation", "the broker has sent no confirmation"))
    if not instructed:
        found.append(("no_instruction", "the custodian holds no settlement instruction"))
    if confirmed:
        found += _confirmation_breaks(legs)
    if instructed:
        found += _instruction_breaks(legs)
    priced = not any(kind in ("quantity", "price") for kind, _ in found)
    confirm_net, instruction_net = legs.confirm_net, legs.instruction_net
    if priced and confirm_net is not None and instruction_net is not None:
        if abs(confirm_net - instruction_net) > NET_TOLERANCE:
            found.append(
                (
                    "net_amount",
                    f"instructed {instruction_net:,.2f}, confirmed {confirm_net:,.2f}",
                )
            )
    return found


def trade_match(inputs: TradeLegs) -> list[Match]:
    """One allocation matched across the order, execution, broker and custodian systems: a row a
    break, naming it and what differs, or one matched row when nothing does.

    @table
    """
    found = breaks(inputs)
    if not found:
        return [Match(inputs.trade_ref, "matched", None, None)]
    return [Match(inputs.trade_ref, "break", kind, detail) for kind, detail in found]
