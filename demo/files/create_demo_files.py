#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: 28d71c2e-788e-431a-8bc0-413ddddf8530
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Create demo Parquet and SQLite files for file-based source demo."""

import sqlite3
from pathlib import Path

HERE = Path(__file__).parent


def create_products_parquet() -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    data = {
        "id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
        "sku": [
            "WIDGET-A", "WIDGET-B", "GADGET-X", "GADGET-Y", "TOOL-1",
            "TOOL-2", "PART-100", "PART-200", "BUNDLE-S", "BUNDLE-M",
            "BUNDLE-L", "ADDON-A", "ADDON-B", "SERVICE-M", "SERVICE-Y",
        ],
        "name": [
            "Widget Alpha", "Widget Beta", "Gadget X", "Gadget Y", "Power Tool 1",
            "Power Tool 2", "Spare Part 100", "Spare Part 200", "Starter Bundle",
            "Medium Bundle", "Large Bundle", "Add-on A", "Add-on B",
            "Monthly Service", "Yearly Service",
        ],
        "category": [
            "Widgets", "Widgets", "Gadgets", "Gadgets", "Tools",
            "Tools", "Parts", "Parts", "Bundles", "Bundles",
            "Bundles", "Add-ons", "Add-ons", "Services", "Services",
        ],
        "price": [
            9.99, 14.99, 49.99, 79.99, 199.99,
            249.99, 4.99, 7.49, 29.99, 59.99,
            99.99, 19.99, 24.99, 9.99, 89.99,
        ],
        "stock": [500, 350, 120, 80, 45, 30, 1000, 750, 200, 150, 75, 400, 300, 0, 0],
        "active": [
            True, True, True, True, True,
            True, True, True, True, True,
            True, True, True, True, True,
        ],
    }

    arrays = {
        "id": pa.array(data["id"], type=pa.int32()),
        "sku": pa.array(data["sku"], type=pa.string()),
        "name": pa.array(data["name"], type=pa.string()),
        "category": pa.array(data["category"], type=pa.string()),
        "price": pa.array(data["price"], type=pa.float64()),
        "stock": pa.array(data["stock"], type=pa.int32()),
        "active": pa.array(data["active"], type=pa.bool_()),
    }
    table = pa.table(arrays)
    pq.write_table(table, HERE / "products.parquet")
    print("Created products.parquet")


def create_orders_sqlite() -> None:
    db_path = HERE / "orders.sqlite"
    db_path.unlink(missing_ok=True)
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            unit_price REAL NOT NULL,
            total REAL NOT NULL,
            status TEXT NOT NULL,
            order_date TEXT NOT NULL
        );

        INSERT INTO orders VALUES
            (1,  1, 1, 2,  9.99,  19.98, 'delivered', '2024-01-10'),
            (2,  2, 3, 1, 49.99,  49.99, 'delivered', '2024-01-12'),
            (3,  3, 5, 1,199.99, 199.99, 'delivered', '2024-01-15'),
            (4,  1, 2, 3, 14.99,  44.97, 'delivered', '2024-01-20'),
            (5,  4, 7, 5,  4.99,  24.95, 'delivered', '2024-02-01'),
            (6,  5, 9, 1, 29.99,  29.99, 'shipped',   '2024-02-05'),
            (7,  6, 1, 1,  9.99,   9.99, 'delivered', '2024-02-10'),
            (8,  7, 4, 2, 79.99, 159.98, 'delivered', '2024-02-14'),
            (9,  8, 6, 1,249.99, 249.99, 'pending',   '2024-02-20'),
            (10, 9, 8, 4,  7.49,  29.96, 'delivered', '2024-03-01'),
            (11,10, 3, 1, 49.99,  49.99, 'delivered', '2024-03-05'),
            (12,11, 2, 2, 14.99,  29.98, 'shipped',   '2024-03-10'),
            (13,12, 5, 1,199.99, 199.99, 'pending',   '2024-03-15'),
            (14,13,10, 1, 59.99,  59.99, 'delivered', '2024-03-20'),
            (15,14,11, 1, 99.99,  99.99, 'cancelled', '2024-03-25');
    """)
    conn.commit()
    conn.close()
    print("Created orders.sqlite")


if __name__ == "__main__":
    create_products_parquet()
    create_orders_sqlite()
    print("Done.")
