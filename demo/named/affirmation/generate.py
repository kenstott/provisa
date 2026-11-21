# Copyright (c) 2026 Kenneth Stott
# Canary: 5da7be3c-ec73-4010-80c8-8be147bb623d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Write the affirmation demo's databases, db/10-*.sql to db/50-*.sql, from the trades below:

    .venv/bin/python3 demo/named/affirmation/generate.py

Larkspur Asset Management, a fictional asset manager, traded ten orders on 2026-11-18 through
three fictional brokers and allocated them to its three funds. Each system holds its own record
of every allocation: the order system the allocation, the execution system the fills, the broker
its confirmation and the custodian its settlement instruction. Seven allocations carry one seeded
break each (BREAKS); the others agree in every system."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

DB = Path(__file__).parent / "db"
TRADE_DATE = "2026-11-18"
#: The settlement date each market expects: T+1 in the US, T+2 in the EU markets.
SETTLE = {"US": "2026-11-19", "DE": "2026-11-20", "FR": "2026-11-20", "NL": "2026-11-20"}
CURRENCY = {"US": "USD", "DE": "EUR", "FR": "EUR", "NL": "EUR"}
#: The global custodian's sub-custodian BIC in each market (fictional).
CUSTODIAN = {"US": "MRDNUS33", "DE": "MRDNDEFF", "FR": "MRDNFRPP", "NL": "MRDNNL2A"}
FUNDS = ("GEF-01", "USG-02", "EUI-03")


@dataclass(frozen=True)
class Order:
    order_id: str
    isin: str
    security: str
    side: str
    market: str
    broker: str
    trader: str
    fills: tuple[tuple[int, float], ...]  # (quantity, price) of each fill
    allocations: tuple[tuple[str, int], ...]  # (fund, quantity) of each allocation

    @property
    def quantity(self) -> int:
        return sum(q for q, _ in self.fills)

    @property
    def avg_price(self) -> float:
        return round(sum(q * p for q, p in self.fills) / self.quantity, 4)


ORDERS = (
    Order(
        "O-1001",
        "US0378331005",
        "Apple Inc.",
        "BUY",
        "US",
        "HRBR",
        "Priya Nair",
        ((18000, 228.41), (12000, 228.47)),
        (("GEF-01", 18000), ("USG-02", 12000)),
    ),
    Order(
        "O-1002",
        "US5949181045",
        "Microsoft Corp.",
        "SELL",
        "US",
        "NFCM",
        "Tom Becker",
        ((7000, 512.10), (5000, 512.02)),
        (("GEF-01", 12000),),
    ),
    Order(
        "O-1003",
        "US67066G1040",
        "NVIDIA Corp.",
        "BUY",
        "US",
        "HRBR",
        "Priya Nair",
        ((25000, 186.30), (15000, 186.36)),
        (("USG-02", 25000), ("GEF-01", 15000)),
    ),
    Order(
        "O-1004",
        "US4781601046",
        "Johnson & Johnson",
        "BUY",
        "US",
        "OSTB",
        "Tom Becker",
        ((15000, 158.92),),
        (("USG-02", 15000),),
    ),
    Order(
        "O-1005",
        "DE0007164600",
        "SAP SE",
        "BUY",
        "DE",
        "OSTB",
        "Lena Vogt",
        ((12000, 241.15), (8000, 241.05)),
        (("EUI-03", 12000), ("GEF-01", 8000)),
    ),
    Order(
        "O-1006",
        "DE0007236101",
        "Siemens AG",
        "SELL",
        "DE",
        "NFCM",
        "Lena Vogt",
        ((8000, 201.40),),
        (("EUI-03", 8000),),
    ),
    Order(
        "O-1007",
        "FR0000120271",
        "TotalEnergies SE",
        "BUY",
        "FR",
        "OSTB",
        "Lena Vogt",
        ((15000, 57.82), (10000, 57.80)),
        (("EUI-03", 15000), ("GEF-01", 10000)),
    ),
    Order(
        "O-1008",
        "NL0010273215",
        "ASML Holding N.V.",
        "BUY",
        "NL",
        "HRBR",
        "Lena Vogt",
        ((3000, 702.60),),
        (("GEF-01", 3000),),
    ),
    Order(
        "O-1009",
        "US46625H1005",
        "JPMorgan Chase & Co.",
        "BUY",
        "US",
        "NFCM",
        "Tom Becker",
        ((12000, 247.55), (8000, 247.61)),
        (("GEF-01", 12000), ("USG-02", 8000)),
    ),
    Order(
        "O-1010",
        "US1912161007",
        "Coca-Cola Co.",
        "SELL",
        "US",
        "OSTB",
        "Priya Nair",
        ((30000, 69.14),),
        (("USG-02", 30000),),
    ),
)

#: The seeded break of each allocation that carries one, by its trade reference.
BREAKS = {
    "A-1002-1": "price",  # the broker confirms 15 cents (about 3 bp) off the average price
    "A-1003-2": "quantity",  # the broker confirms 1,500 shares for 15,000
    "A-1004-1": "no_confirmation",  # the broker has not confirmed
    "A-1005-2": "ssi",  # the broker delivers to the fund's closed account
    "A-1006-1": "settlement_date",  # the broker confirms T+1 in a T+2 market
    "A-1007-2": "no_instruction",  # the custodian was never instructed
    "A-1008-1": "net_amount",  # the broker charges 10 bp of commission for 5
}


def account(fund: str, market: str, number: int = 1) -> str:
    return f"{fund.replace('-', '')}-{market}-{number:04d}"


def commission(market: str, quantity: int, price: float, rate_bp: float = 5.0) -> float:
    """A cent a share in the US; a rate of the gross in the EU markets."""
    if market == "US":
        return round(quantity * 0.01, 2)
    return round(quantity * price * rate_bp / 10000, 2)


def net(side: str, quantity: int, price: float, fee: float) -> float:
    gross = quantity * price
    return round(gross + fee if side == "BUY" else gross - fee, 2)


def _sql(value: str | int | float | None) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, (int, float)):
        return repr(value)
    return "'" + str(value).replace("'", "''") + "'"


def _insert(table: str, rows: list[tuple[str | int | float | None, ...]]) -> str:
    body = ",\n".join("  (" + ", ".join(_sql(v) for v in row) + ")" for row in rows)
    return f"INSERT INTO {table} VALUES\n{body};\n"


_HEADER = """-- Copyright (c) 2026 Kenneth Stott
--
-- This source code is licensed under the Business Source License 1.1
-- found in the LICENSE file in the root directory of this source tree.
--
-- NOTICE: Use of this software for training artificial intelligence or
-- machine learning models is strictly prohibited without explicit written
-- permission from the copyright holder.
--
-- Written by generate.py; edit the trades there, not here.
"""


def _grants(writes: str = "") -> str:
    return (
        "GRANT USAGE ON SCHEMA public TO provisa;\n"
        "GRANT SELECT ON ALL TABLES IN SCHEMA public TO provisa;\n" + writes
    )


def oms() -> str:
    orders = [
        (
            o.order_id,
            o.isin,
            o.security,
            o.side,
            o.quantity,
            o.market,
            CURRENCY[o.market],
            o.broker,
            o.trader,
            TRADE_DATE,
        )
        for o in ORDERS
    ]
    allocations = [
        (ref, o.order_id, fund, o.market, qty, SETTLE[o.market])
        for o, ref, fund, qty in allocations_of()
    ]
    return (
        _HEADER
        + "-- The order management system: each order and its allocations to the funds.\n"
        + "\\connect affirmation_oms\n\n"
        + "CREATE TABLE orders (order_id text PRIMARY KEY, isin text NOT NULL, security text NOT NULL,\n"
        + "  side text NOT NULL CHECK (side IN ('BUY', 'SELL')), quantity integer NOT NULL,\n"
        + "  market text NOT NULL, currency text NOT NULL, broker_code text NOT NULL,\n"
        + "  trader text NOT NULL, trade_date date NOT NULL);\n"
        + "CREATE TABLE allocations (trade_ref text PRIMARY KEY,\n"
        + "  order_id text NOT NULL REFERENCES orders, fund_account text NOT NULL,\n"
        + "  market text NOT NULL, quantity integer NOT NULL, settle_date date NOT NULL);\n\n"
        + _insert("orders", orders)
        + _insert("allocations", allocations)
        + _grants()
    )


def ems() -> str:
    fills = [
        (
            f"X-{o.order_id[2:]}-{i}",
            o.order_id,
            qty,
            price,
            f"{TRADE_DATE} {14 + i}:{10 * i:02d}:00",
        )
        for o in ORDERS
        for i, (qty, price) in enumerate(o.fills, 1)
    ]
    return (
        _HEADER
        + "-- The execution management system: each fill of each order.\n"
        + "\\connect affirmation_ems\n\n"
        + "CREATE TABLE executions (exec_id text PRIMARY KEY, order_id text NOT NULL,\n"
        + "  quantity integer NOT NULL, price numeric(12, 4) NOT NULL,\n"
        + "  executed_at timestamp NOT NULL);\n\n"
        + _insert("executions", fills)
        + _grants()
    )


def allocations_of() -> list[tuple[Order, str, str, int]]:
    """Each allocation: its order, its trade reference, its fund and its quantity."""
    return [
        (o, f"A-{o.order_id[2:]}-{i}", fund, qty)
        for o in ORDERS
        for i, (fund, qty) in enumerate(o.allocations, 1)
    ]


Value = str | int | float


def confirmation(o: Order, ref: str, fund: str, qty: int) -> dict[str, Value] | None:
    """The broker's confirmation of an allocation, its seeded break in it; None when unsent."""
    seeded = BREAKS.get(ref)
    if seeded == "no_confirmation":
        return None
    price = round(o.avg_price + 0.15, 4) if seeded == "price" else o.avg_price
    quantity = 1500 if seeded == "quantity" else qty
    fee = commission(o.market, quantity, price, 10.0 if seeded == "net_amount" else 5.0)
    return {
        "side": o.side,
        "quantity": quantity,
        "price": price,
        "commission": fee,
        "net_amount": net(o.side, quantity, price, fee),
        "settle_date": TRADE_DATE[:-2] + "19" if seeded == "settlement_date" else SETTLE[o.market],
        "custodian_bic": CUSTODIAN[o.market],
        "safekeeping_account": account(fund, o.market, 9 if seeded == "ssi" else 1),
    }


def instruction(o: Order, ref: str, qty: int) -> dict[str, Value] | None:
    """The custodian's instruction for an allocation; None when it was never sent."""
    if BREAKS.get(ref) == "no_instruction":
        return None
    fee = commission(o.market, qty, o.avg_price)
    return {
        "quantity": qty,
        "net_amount": net(o.side, qty, o.avg_price, fee),
        "settle_date": SETTLE[o.market],
    }


def trade_legs(o: Order, ref: str, fund: str, qty: int) -> dict[str, Value | date]:
    """An allocation's four records as the legs view reads them: what trade_match is given."""
    legs: dict[str, Value | date] = {
        "trade_ref": ref,
        "side": o.side,
        "quantity": float(qty),
        "settle_date": date.fromisoformat(SETTLE[o.market]),
        "exec_price": sum(q * p for q, p in o.fills) / o.quantity,
        "ssi_custodian": CUSTODIAN[o.market],
        "ssi_account": account(fund, o.market),
    }
    c = confirmation(o, ref, fund, qty)
    if c is not None:
        legs |= {
            "confirm_side": c["side"],
            "confirm_quantity": float(c["quantity"]),
            "confirm_price": c["price"],
            "confirm_net": c["net_amount"],
            "confirm_settle_date": date.fromisoformat(str(c["settle_date"])),
            "confirm_custodian": c["custodian_bic"],
            "confirm_account": c["safekeeping_account"],
        }
    i = instruction(o, ref, qty)
    if i is not None:
        legs |= {
            "instruction_quantity": float(i["quantity"]),
            "instruction_net": i["net_amount"],
            "instruction_settle_date": date.fromisoformat(str(i["settle_date"])),
        }
    return legs


def broker() -> str:
    rows = []
    for o, ref, fund, qty in allocations_of():
        c = confirmation(o, ref, fund, qty)
        if c is None:
            continue
        rows.append(
            (
                f"C-{o.broker}-{ref[2:]}",
                ref,
                o.broker,
                *c.values(),
                f"{TRADE_DATE} 19:{len(rows):02d}:00",
            )
        )
    return (
        _HEADER
        + "-- The brokers' confirmations, as they arrive through the matching platform: each\n"
        + "-- carries the allocation's reference, the broker's price, commission and net amount,\n"
        + "-- and the settlement instructions the broker will deliver to.\n"
        + "\\connect affirmation_broker\n\n"
        + "CREATE TABLE confirmations (confirm_id text PRIMARY KEY, trade_ref text NOT NULL UNIQUE,\n"
        + "  broker_code text NOT NULL, side text NOT NULL, quantity integer NOT NULL,\n"
        + "  price numeric(12, 4) NOT NULL, commission numeric(14, 2) NOT NULL,\n"
        + "  net_amount numeric(16, 2) NOT NULL, settle_date date NOT NULL,\n"
        + "  custodian_bic text NOT NULL, safekeeping_account text NOT NULL,\n"
        + "  received_at timestamp NOT NULL);\n\n"
        + _insert("confirmations", rows)
        + _grants()
    )


def custody() -> str:
    rows = []
    for o, ref, fund, qty in allocations_of():
        i = instruction(o, ref, qty)
        if i is None:
            continue
        rows.append((f"I-{ref[2:]}", ref, fund, o.side, *i.values(), "pending"))
    return (
        _HEADER
        + "-- The custodian's settlement instructions, as Larkspur sent them: what each fund\n"
        + "-- expects to receive or deliver, against what cash, on what day.\n"
        + "\\connect affirmation_custody\n\n"
        + "CREATE TABLE instructions (instruction_id text PRIMARY KEY, trade_ref text NOT NULL UNIQUE,\n"
        + "  fund_account text NOT NULL, side text NOT NULL, quantity integer NOT NULL,\n"
        + "  net_amount numeric(16, 2) NOT NULL, settle_date date NOT NULL,\n"
        + "  status text NOT NULL);\n\n"
        + _insert("instructions", rows)
        + _grants()
    )


def desk() -> str:
    ssi = [
        (fund, market, CUSTODIAN[market], account(fund, market))
        for fund in FUNDS
        for market in ("US", "DE", "FR", "NL")
    ]
    rules = [
        (
            "US",
            1,
            26 * 60,
            "affirmed by 9:00 pm New York time on trade date (DTC, T+1)",
            1.25,
            "a day's funding of the failed amount at 4.5% a year",
        ),
        (
            "DE",
            2,
            22 * 60,
            "confirmed by 11:00 pm Frankfurt time on trade date (T+2)",
            1.0,
            "CSDR cash penalty for a liquid share",
        ),
        (
            "FR",
            2,
            22 * 60,
            "confirmed by 11:00 pm Paris time on trade date (T+2)",
            1.0,
            "CSDR cash penalty for a liquid share",
        ),
        (
            "NL",
            2,
            22 * 60,
            "confirmed by 11:00 pm Amsterdam time on trade date (T+2)",
            1.0,
            "CSDR cash penalty for a liquid share",
        ),
    ]
    return (
        _HEADER
        + "-- The middle office's own database: the funds' standing settlement instructions, each\n"
        + "-- market's affirmation cutoff and what a failed settlement costs there, the desk's\n"
        + "-- clock, and the affirmation log the affirm_matched command writes.\n"
        + "\\connect affirmation_desk\n\n"
        + "CREATE TABLE ssi (fund_account text NOT NULL, market text NOT NULL,\n"
        + "  custodian_bic text NOT NULL, safekeeping_account text NOT NULL,\n"
        + "  PRIMARY KEY (fund_account, market));\n"
        + "-- cutoff_minutes: the cutoff, in minutes after midnight UTC starting the trade date.\n"
        + "CREATE TABLE settlement_rules (market text PRIMARY KEY, settlement_cycle_days integer NOT NULL,\n"
        + "  cutoff_minutes integer NOT NULL, cutoff_rule text NOT NULL,\n"
        + "  penalty_bp_per_day numeric(6, 2) NOT NULL, penalty_basis text NOT NULL);\n"
        + "-- The time the demo's desk is at: the cutoffs count down from it.\n"
        + "CREATE TABLE desk_clock (as_at timestamp PRIMARY KEY);\n"
        + "CREATE TABLE affirmation_log (trade_ref text PRIMARY KEY, affirmed_at timestamp NOT NULL,\n"
        + "  affirmed_by text NOT NULL);\n\n"
        + _insert("ssi", ssi)
        + _insert("settlement_rules", rules)
        + f"INSERT INTO desk_clock VALUES ('{TRADE_DATE} 21:15:00');\n"
        + _grants("GRANT INSERT ON affirmation_log TO provisa;\n")
    )


def main() -> None:
    for name, text in (
        ("10-oms.sql", oms()),
        ("20-ems.sql", ems()),
        ("30-broker.sql", broker()),
        ("40-custody.sql", custody()),
        ("50-desk.sql", desk()),
    ):
        (DB / name).write_text(text)


if __name__ == "__main__":
    main()
