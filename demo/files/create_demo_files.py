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
            amount REAL NOT NULL,
            status TEXT NOT NULL,
            order_date TEXT NOT NULL,
            region TEXT NOT NULL
        );

        INSERT INTO orders VALUES
            (1,  1, 1, 2,  9.99,  19.98, 'delivered', '2024-01-10', 'us-east'),
            (2,  2, 3, 1, 49.99,  49.99, 'delivered', '2024-01-12', 'us-west'),
            (3,  3, 5, 1,199.99, 199.99, 'delivered', '2024-01-15', 'eu-west'),
            (4,  1, 2, 3, 14.99,  44.97, 'delivered', '2024-01-20', 'us-east'),
            (5,  4, 7, 5,  4.99,  24.95, 'delivered', '2024-02-01', 'apac'),
            (6,  5, 9, 1, 29.99,  29.99, 'shipped',   '2024-02-05', 'us-west'),
            (7,  6, 1, 1,  9.99,   9.99, 'delivered', '2024-02-10', 'us-east'),
            (8,  7, 4, 2, 79.99, 159.98, 'delivered', '2024-02-14', 'eu-west'),
            (9,  8, 6, 1,249.99, 249.99, 'pending',   '2024-02-20', 'apac'),
            (10, 9, 8, 4,  7.49,  29.96, 'delivered', '2024-03-01', 'us-east'),
            (11,10, 3, 1, 49.99,  49.99, 'delivered', '2024-03-05', 'us-west'),
            (12,11, 2, 2, 14.99,  29.98, 'shipped',   '2024-03-10', 'eu-west'),
            (13,12, 5, 1,199.99, 199.99, 'pending',   '2024-03-15', 'us-east'),
            (14,13,10, 1, 59.99,  59.99, 'delivered', '2024-03-20', 'apac'),
            (15,14,11, 1, 99.99,  99.99, 'cancelled', '2024-03-25', 'us-west'),
            (16, 1, 3, 2, 49.99,  99.98, 'delivered', '2024-04-01', 'us-east'),
            (17, 2, 1, 1,  9.99,   9.99, 'delivered', '2024-04-03', 'eu-west'),
            (18, 3, 8, 3,  7.49,  22.47, 'shipped',   '2024-04-05', 'us-west'),
            (19, 5, 5, 1,199.99, 199.99, 'delivered', '2024-04-08', 'apac'),
            (20, 6, 2, 2, 14.99,  29.98, 'delivered', '2024-04-10', 'us-east'),
            (21, 7, 9, 1, 29.99,  29.99, 'pending',   '2024-04-12', 'eu-west'),
            (22, 8, 4, 1, 79.99,  79.99, 'delivered', '2024-04-15', 'us-east'),
            (23, 9, 7, 4,  4.99,  19.96, 'delivered', '2024-04-18', 'us-west'),
            (24,10, 6, 1,249.99, 249.99, 'shipped',   '2024-04-20', 'apac'),
            (25,11, 1, 3,  9.99,  29.97, 'delivered', '2024-04-22', 'us-east'),
            (26,12, 3, 1, 49.99,  49.99, 'delivered', '2024-04-25', 'eu-west'),
            (27,13, 2, 2, 14.99,  29.98, 'cancelled', '2024-04-28', 'us-west'),
            (28,14,10, 1, 59.99,  59.99, 'delivered', '2024-05-01', 'apac'),
            (29, 1, 5, 1,199.99, 199.99, 'delivered', '2024-05-03', 'us-east'),
            (30, 2,11, 1, 99.99,  99.99, 'pending',   '2024-05-05', 'eu-west');
    """)
    conn.commit()
    conn.close()
    print("Created orders.sqlite")


def create_inquiries_sqlite() -> None:
    # Customer inquiries — users table + inquiries linked by user_id
    db_path = HERE / "inquiries.sqlite"
    db_path.unlink(missing_ok=True)
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT NOT NULL UNIQUE,
            phone TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE inquiries (
            id INTEGER PRIMARY KEY,
            pet_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL REFERENCES users(id),
            inquiry_type TEXT NOT NULL,
            message TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'new',
            submitted_at TEXT NOT NULL
        );

        -- 10 prospective adopters
        INSERT INTO users VALUES
            (1,  'Alice Nguyen',  'alice@example.com',  '555-0101', '2025-10-01 09:00:00'),
            (2,  'Bob Martinez',  'bob@example.com',    '555-0102', '2025-10-03 10:15:00'),
            (3,  'Carol White',   'carol@example.com',  '555-0103', '2025-10-05 14:30:00'),
            (4,  'David Kim',     'david@example.com',  '555-0104', '2025-10-07 11:00:00'),
            (5,  'Eva Brown',     'eva@example.com',    '555-0105', '2025-10-09 16:45:00'),
            (6,  'Frank Lee',     'frank@example.com',  '555-0106', '2025-10-12 08:20:00'),
            (7,  'Grace Chen',    'grace@example.com',  '555-0107', '2025-10-14 13:10:00'),
            (8,  'Hank Patel',    'hank@example.com',   '555-0108', '2025-10-16 09:55:00'),
            (9,  'Iris Jordan',   'iris@example.com',   '555-0109', '2025-10-18 15:30:00'),
            (10, 'Jay Singh',     'jay@example.com',    '555-0110', '2025-10-20 10:00:00');

        -- pet_ids 1-7: Cat 1, Cat 2, Dog 1, Lion 1, Lion 2, Lion 3, Rabbit 1
        INSERT INTO inquiries VALUES
            (1,  1, 1, 'adoption', 'Is Cat 1 still available?',              'converted', '2025-10-02 10:00:00'),
            (2,  1, 3, 'adoption', 'We would love to adopt Cat 1.',          'closed',    '2025-10-06 11:30:00'),
            (3,  1, 5, 'general',  'What is Cat 1 temperament?',             'open',      '2025-10-10 09:15:00'),
            (4,  1, 7, 'adoption', 'Interested in Cat 1 for our family.',    'new',       '2025-10-15 14:00:00'),
            (5,  1, 9, 'general',  'Does Cat 1 get along with dogs?',        'pending',   '2025-10-19 16:30:00'),
            (6,  2, 2, 'adoption', 'How old is Cat 2?',                      'open',      '2025-10-04 09:00:00'),
            (7,  2, 4, 'adoption', 'Cat 2 looks adorable, still available?', 'pending',   '2025-10-08 12:00:00'),
            (8,  2, 6, 'general',  'Cat 2 indoor or outdoor?',               'new',       '2025-10-13 10:45:00'),
            (9,  2, 8, 'adoption', 'Would like to schedule a visit for Cat 2.','open',    '2025-10-17 13:30:00'),
            (10, 3, 1, 'adoption', 'Dog 1 — is a deposit required?',         'pending',   '2025-10-03 11:00:00'),
            (11, 3, 3, 'general',  'Dog 1 house-trained?',                   'open',      '2025-10-07 14:30:00'),
            (12, 3, 5, 'adoption', 'Would love to meet Dog 1.',              'converted', '2025-10-11 09:00:00'),
            (13, 3, 7, 'general',  'What breed is Dog 1?',                   'closed',    '2025-10-15 11:15:00'),
            (14, 3,10, 'adoption', 'Dog 1 — energy level?',                  'new',       '2025-10-21 08:45:00'),
            (15, 4, 2, 'general',  'Lion 1 — is this a rescue?',             'open',      '2025-10-05 10:30:00'),
            (16, 4, 6, 'adoption', 'Serious inquiry about Lion 1.',          'pending',   '2025-10-14 15:00:00'),
            (17, 4, 9, 'general',  'Lion 1 diet requirements?',              'new',       '2025-10-20 09:30:00'),
            (18, 5, 4, 'adoption', 'Lion 2 — do you have a waiting list?',   'open',      '2025-10-09 13:00:00'),
            (19, 5, 6, 'general',  'Lion 2 — habitat needs?',                'pending',   '2025-10-14 16:45:00'),
            (20, 5, 8, 'adoption', 'Very interested in Lion 2.',             'new',       '2025-10-17 14:00:00'),
            (21, 5,10, 'general',  'Lion 2 age and health status?',          'open',      '2025-10-21 10:15:00'),
            (22, 6, 1, 'adoption', 'Lion 3 — how do I apply?',               'closed',    '2025-10-02 16:00:00'),
            (23, 6, 3, 'general',  'Lion 3 socialization history?',          'open',      '2025-10-07 09:00:00'),
            (24, 6, 5, 'adoption', 'Interested in Lion 3.',                  'pending',   '2025-10-11 11:30:00'),
            (25, 6, 7, 'general',  'Lion 3 — estimated weight?',             'new',       '2025-10-16 15:30:00'),
            (26, 7, 2, 'adoption', 'Rabbit 1 — available now?',              'converted', '2025-10-04 14:00:00'),
            (27, 7, 4, 'general',  'Rabbit 1 spayed/neutered?',              'open',      '2025-10-09 10:00:00'),
            (28, 7, 8, 'adoption', 'Would like info on Rabbit 1.',           'new',       '2025-10-18 12:45:00');
    """)
    conn.commit()
    conn.close()
    print("Created inquiries.sqlite")


if __name__ == "__main__":
    create_products_parquet()
    create_orders_sqlite()
    create_inquiries_sqlite()
    print("Done.")
