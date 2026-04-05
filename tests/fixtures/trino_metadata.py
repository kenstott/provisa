# Copyright (c) 2026 Kenneth Stott
# Canary: ac809c4f-62bd-4c22-9a9f-d611b2aa67c3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Mock Trino INFORMATION_SCHEMA responses for unit tests."""

ORDERS_COLUMNS = [
    {"column_name": "id", "data_type": "integer", "is_nullable": "NO"},
    {"column_name": "customer_id", "data_type": "integer", "is_nullable": "NO"},
    {"column_name": "product_id", "data_type": "integer", "is_nullable": "NO"},
    {"column_name": "amount", "data_type": "decimal(10,2)", "is_nullable": "NO"},
    {"column_name": "quantity", "data_type": "integer", "is_nullable": "NO"},
    {"column_name": "region", "data_type": "varchar(50)", "is_nullable": "NO"},
    {"column_name": "status", "data_type": "varchar(20)", "is_nullable": "NO"},
    {"column_name": "created_at", "data_type": "timestamp", "is_nullable": "NO"},
]

CUSTOMERS_COLUMNS = [
    {"column_name": "id", "data_type": "integer", "is_nullable": "NO"},
    {"column_name": "name", "data_type": "varchar(100)", "is_nullable": "NO"},
    {"column_name": "email", "data_type": "varchar(200)", "is_nullable": "NO"},
    {"column_name": "region", "data_type": "varchar(50)", "is_nullable": "NO"},
    {"column_name": "created_at", "data_type": "timestamp", "is_nullable": "NO"},
]

PRODUCTS_COLUMNS = [
    {"column_name": "id", "data_type": "integer", "is_nullable": "NO"},
    {"column_name": "name", "data_type": "varchar(200)", "is_nullable": "NO"},
    {"column_name": "price", "data_type": "decimal(10,2)", "is_nullable": "NO"},
    {"column_name": "category", "data_type": "varchar(100)", "is_nullable": "NO"},
    {"column_name": "created_at", "data_type": "timestamp", "is_nullable": "NO"},
]

ALL_TABLES = {
    "orders": ORDERS_COLUMNS,
    "customers": CUSTOMERS_COLUMNS,
    "products": PRODUCTS_COLUMNS,
}
