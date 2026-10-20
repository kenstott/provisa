"""Write supplies.parquet: the animal shelter's supplies inventory by location, the file the demo
adds as a Parquet source (REQ-2113), published at https://provisa.dev/demo-data/supplies.parquet.

    .venv/bin/python3 site/demo-data/make_supplies.py

Each location is one the demo GraphQL server's schedules name (demo/graphql_server/server.py), so
relationship discovery can propose supplies.location -> schedules.location. Seeded: the same rows
on every run, some of them below their reorder level."""

from __future__ import annotations

import random
from datetime import date, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

HERE = Path(__file__).parent
COUNTED = date(2026, 10, 19)

# (item, category, unit, unit cost in USD, typical stock)
_ITEMS = {
    "kibble, adult": ("food", "15 kg bag", 54.90, 24),
    "kibble, puppy": ("food", "10 kg bag", 47.50, 12),
    "kibble, senior": ("food", "10 kg bag", 51.20, 8),
    "cat food, dry": ("food", "7 kg bag", 38.20, 18),
    "cat food, wet": ("food", "can", 1.35, 240),
    "cat food, kitten": ("food", "3 kg bag", 22.80, 10),
    "raw meat, beef": ("food", "kg", 9.80, 160),
    "raw meat, chicken": ("food", "kg", 5.60, 140),
    "raw meat, rabbit": ("food", "kg", 12.30, 60),
    "raw meat, horse": ("food", "kg", 8.40, 120),
    "hay, timothy": ("food", "bale", 21.00, 10),
    "hay, alfalfa": ("food", "bale", 24.50, 6),
    "flea meds": ("medical", "dose", 14.75, 40),
    "flea meds, small animal": ("medical", "dose", 8.90, 20),
    "litter, clumping": ("cleaning", "18 kg bag", 16.30, 30),
    "bedding, straw": ("cleaning", "bale", 11.50, 16),
    "bedding, fleece": ("cleaning", "blanket", 7.25, 40),
    "bedding, paper": ("cleaning", "pack", 9.60, 25),
    "disinfectant, quaternary": ("cleaning", "5 L jug", 32.00, 8),
    "disinfectant, bleach": ("cleaning", "4 L jug", 6.40, 12),
    "disinfectant, enzymatic": ("cleaning", "1 L bottle", 13.90, 14),
}
_AT = {
    "Dog Kennels": [
        "kibble, adult",
        "kibble, puppy",
        "kibble, senior",
        "flea meds",
        "bedding, straw",
        "bedding, fleece",
        "disinfectant, quaternary",
        "disinfectant, bleach",
        "disinfectant, enzymatic",
    ],
    "Domestic Cat Wing": [
        "cat food, dry",
        "cat food, wet",
        "cat food, kitten",
        "litter, clumping",
        "flea meds",
        "bedding, fleece",
        "bedding, paper",
        "disinfectant, quaternary",
        "disinfectant, bleach",
        "disinfectant, enzymatic",
    ],
    "Large Carnivore Block A": [
        "raw meat, beef",
        "raw meat, chicken",
        "bedding, straw",
        "disinfectant, quaternary",
        "disinfectant, bleach",
        "disinfectant, enzymatic",
    ],
    "Large Carnivore Block B": [
        "raw meat, horse",
        "raw meat, rabbit",
        "bedding, straw",
        "disinfectant, quaternary",
        "disinfectant, bleach",
        "disinfectant, enzymatic",
    ],
    "Small Animal Room": [
        "hay, timothy",
        "hay, alfalfa",
        "flea meds, small animal",
        "bedding, paper",
        "bedding, fleece",
        "disinfectant, quaternary",
        "disinfectant, bleach",
        "disinfectant, enzymatic",
    ],
}
# Items stocked in several areas, set so the question "should we shift supplies between
# locations?" has an answer: one area well above its reorder level while another is below it.
# (location, item) -> (on_hand, reorder_level).
_SHARED = {
    ("Dog Kennels", "disinfectant, quaternary"): (40, 10),
    ("Domestic Cat Wing", "disinfectant, quaternary"): (3, 10),
    ("Large Carnivore Block A", "disinfectant, quaternary"): (12, 10),
    ("Large Carnivore Block B", "disinfectant, quaternary"): (4, 10),
    ("Small Animal Room", "disinfectant, quaternary"): (11, 10),
    ("Small Animal Room", "bedding, paper"): (30, 10),
    ("Domestic Cat Wing", "bedding, paper"): (2, 10),
    ("Dog Kennels", "flea meds"): (6, 16),
    ("Domestic Cat Wing", "flea meds"): (45, 16),
}
# Items only one area uses (each carnivore block its own meats), below their reorder level: short,
# with nowhere to move them from.
_SHORT = {
    ("Large Carnivore Block A", "raw meat, beef"),
    ("Small Animal Room", "hay, timothy"),
}


def rows(rng: random.Random) -> list[dict]:
    out = []
    for location, items in _AT.items():
        for item in items:
            category, unit, cost, typical = _ITEMS[item]
            reorder = max(2, round(typical * 0.4))
            if (location, item) in _SHARED:
                on_hand, reorder = _SHARED[(location, item)]
            elif (location, item) in _SHORT:
                on_hand = rng.randint(max(0, reorder // 3), reorder - 1)
            else:
                # Above the reorder level, but not by enough to spare: twice it at most.
                on_hand = rng.randint(reorder + 1, reorder * 2)
            out.append(
                {
                    "item": item,
                    "category": category,
                    "location": location,
                    "on_hand": on_hand,
                    "unit": unit,
                    "reorder_level": reorder,
                    "unit_cost": cost,
                    "last_counted": COUNTED - timedelta(days=rng.randint(0, 6)),
                }
            )
    return out


SCHEMA = pa.schema(
    [
        ("item", pa.string()),
        ("category", pa.string()),
        ("location", pa.string()),
        ("on_hand", pa.int32()),
        ("unit", pa.string()),
        ("reorder_level", pa.int32()),
        ("unit_cost", pa.float64()),
        ("last_counted", pa.date32()),
    ]
)


def main() -> None:
    table = pa.Table.from_pylist(rows(random.Random(2113)), schema=SCHEMA)
    pq.write_table(table, HERE / "supplies.parquet", compression="zstd")
    below = sum(1 for r in table.to_pylist() if r["on_hand"] < r["reorder_level"])
    print(f"{table.num_rows} rows, {below} below their reorder level")


if __name__ == "__main__":
    main()
