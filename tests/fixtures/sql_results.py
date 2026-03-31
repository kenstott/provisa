# Copyright (c) 2025 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Sample SQL result row sets for serializer tests."""

from decimal import Decimal

FLAT_ORDERS = {
    "columns": ["id", "amount", "status"],
    "rows": [
        (1, Decimal("19.99"), "completed"),
        (2, Decimal("99.98"), "completed"),
        (3, Decimal("29.99"), "completed"),
    ],
    "expected_json": {
        "data": {
            "orders": [
                {"id": 1, "amount": 19.99, "status": "completed"},
                {"id": 2, "amount": 99.98, "status": "completed"},
                {"id": 3, "amount": 29.99, "status": "completed"},
            ]
        }
    },
}

JOINED_ORDER_CUSTOMER = {
    "columns": ["t0.id", "t0.amount", "t1.name", "t1.email"],
    "rows": [
        (1, Decimal("19.99"), "Alice Johnson", "alice@example.com"),
        (2, Decimal("99.98"), "Alice Johnson", "alice@example.com"),
        (3, Decimal("29.99"), "Bob Smith", "bob@example.com"),
    ],
    "expected_json": {
        "data": {
            "orders": [
                {
                    "id": 1,
                    "amount": 19.99,
                    "customers": {"name": "Alice Johnson", "email": "alice@example.com"},
                },
                {
                    "id": 2,
                    "amount": 99.98,
                    "customers": {"name": "Alice Johnson", "email": "alice@example.com"},
                },
                {
                    "id": 3,
                    "amount": 29.99,
                    "customers": {"name": "Bob Smith", "email": "bob@example.com"},
                },
            ]
        }
    },
}

NULL_RELATIONSHIP = {
    "columns": ["t0.id", "t0.amount", "t1.name"],
    "rows": [
        (1, Decimal("19.99"), "Alice Johnson"),
        (2, Decimal("99.98"), None),
    ],
    "expected_json": {
        "data": {
            "orders": [
                {"id": 1, "amount": 19.99, "customers": {"name": "Alice Johnson"}},
                {"id": 2, "amount": 99.98, "customers": None},
            ]
        }
    },
}
