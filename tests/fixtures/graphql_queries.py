# Copyright (c) 2025 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Sample GraphQL queries and expected SQL output pairs for compiler tests."""

SIMPLE_SELECT = {
    "graphql": """
        {
            orders {
                id
                amount
                status
            }
        }
    """,
    "expected_sql": 'SELECT "id", "amount", "status" FROM "public"."orders"',
}

FILTERED_SELECT = {
    "graphql": """
        {
            orders(where: { region: { eq: "us-east" } }) {
                id
                amount
            }
        }
    """,
    "expected_sql": (
        'SELECT "id", "amount" FROM "public"."orders" WHERE "region" = $1'
    ),
    "params": ["us-east"],
}

NESTED_RELATIONSHIP = {
    "graphql": """
        {
            orders {
                id
                amount
                customers {
                    name
                    email
                }
            }
        }
    """,
    "expected_sql": (
        'SELECT "t0"."id", "t0"."amount", "t1"."name", "t1"."email"'
        ' FROM "public"."orders" "t0"'
        ' LEFT JOIN "public"."customers" "t1" ON "t0"."customer_id" = "t1"."id"'
    ),
}

PAGINATED_SELECT = {
    "graphql": """
        {
            orders(limit: 10, offset: 20, order_by: [{ field: CREATED_AT, direction: DESC }]) {
                id
                amount
            }
        }
    """,
    "expected_sql": (
        'SELECT "id", "amount" FROM "public"."orders"'
        ' ORDER BY "created_at" DESC LIMIT 10 OFFSET 20'
    ),
}
