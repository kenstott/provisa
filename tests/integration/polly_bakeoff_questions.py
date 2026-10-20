# Copyright (c) 2026 Kenneth Stott
# Canary: 6a1f3c84-9d27-4b50-8e16-2c7b9a4d0f53
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Polly bake-off's questions, each with the SQL whose rows are its right answers (REQ-2012).

Each named demo has its own questions (QUESTIONS, by demo name), asked on that demo and graded
over its tables: the weblogs demo's by tests/integration/test_polly_bakeoff_e2e.py. A demo's tour
may ask Polly any of its own (tests/unit/test_named_demo_tour.py), and every grading query names
its demo's tables and views as they register (tests/unit/test_polly_bakeoff_questions.py)."""

# Requirements: REQ-2012

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Question:
    text: str
    # The SQL whose rows are the right answers: a reply is right when it names any of them (a tie
    # for "the most" makes each tied value right).
    answer_sql: str


# The weblogs demo's own question, the one its tour answers in SQL and over REST.
DEMO_QUESTION = "Which pages are slowest, and which SQL makes them slow?"

WEBLOGS = (
    Question(
        "How many customers does the shop have?", 'SELECT COUNT(*) AS a FROM "shop"."customers"'
    ),
    Question("How many orders have been placed?", 'SELECT COUNT(*) AS a FROM "shop"."orders"'),
    Question(
        "How many requests are in the web access log?",
        'SELECT COUNT(*) AS a FROM "shop"."access_log"',
    ),
    Question(
        "How many requests were answered with a server error (a 5xx status)?",
        'SELECT COUNT(*) AS a FROM "shop"."access_log" WHERE status >= 500',
    ),
    Question(
        "Which product category has the most products?",
        'SELECT category AS a FROM "shop"."products" GROUP BY category '
        'HAVING COUNT(*) = (SELECT MAX(n) FROM (SELECT COUNT(*) AS n FROM "shop"."products" '
        "GROUP BY category) AS c)",
    ),
    Question(
        "What is the name of the most expensive product?",
        'SELECT name AS a FROM "shop"."products" '
        'WHERE price = (SELECT MAX(price) FROM "shop"."products")',
    ),
    Question(
        "Which city has the most customers?",
        'SELECT city AS a FROM "shop"."customers" GROUP BY city '
        'HAVING COUNT(*) = (SELECT MAX(n) FROM (SELECT COUNT(*) AS n FROM "shop"."customers" '
        "GROUP BY city) AS c)",
    ),
    Question(
        "Which page path was requested most often?",
        'SELECT path AS a FROM "shop"."access_log" GROUP BY path '
        'HAVING COUNT(*) = (SELECT MAX(n) FROM (SELECT COUNT(*) AS n FROM "shop"."access_log" '
        "GROUP BY path) AS c)",
    ),
    Question(
        "How many distinct customers have placed at least one order?",
        'SELECT COUNT(DISTINCT customer_id) AS a FROM "shop"."orders"',
    ),
    Question(
        "Which database table did the shop's SQL statements touch most often?",
        'SELECT table_name AS a FROM "shop"."view_slow_statements_by_table" WHERE statements = '
        '(SELECT MAX(statements) FROM "shop"."view_slow_statements_by_table")',
    ),
    # The demo's own question: right when the reply names the slowest page, on average.
    Question(
        DEMO_QUESTION,
        'SELECT path AS a FROM "shop"."view_slowest_pages" GROUP BY path '
        "ORDER BY AVG(page_ms) DESC LIMIT 1",
    ),
)

# The insider demo's post-review questions (REQ-2056): why the top case was flagged, who was over
# the wall for its deal, and what the customer-data sender read before sending.
_TOP_DEAL_CASE = (
    'SELECT deal_id FROM "bank"."view_review_queue" WHERE deal_id IS NOT NULL '
    "ORDER BY score DESC LIMIT 1"
)
INSIDER = (
    Question(
        "Who sent the highest-scoring deal message in the review queue, and which company is it "
        "about?",
        'SELECT sender AS a FROM "bank"."view_review_queue" WHERE deal_id IS NOT NULL AND score = '
        '(SELECT MAX(score) FROM "bank"."view_review_queue" WHERE deal_id IS NOT NULL)',
    ),
    Question(
        "Who was brought over the wall for the deal behind the highest-scoring deal message in "
        "the review queue?",
        'SELECT e.name AS a FROM "bank"."deal_team" AS t JOIN "bank"."employee" AS e '
        f"ON e.employee_id = t.employee_id WHERE t.deal_id = ({_TOP_DEAL_CASE})",
    ),
    Question(
        "How many customer records did the sender of the highest-scoring customer-data message "
        "read in the week before sending it?",
        'SELECT SUM(a.records_read) AS a FROM "bank"."customer_access" AS a '
        'JOIN "bank"."message" AS m ON m.sender_id = a.employee_id '
        'WHERE m.message_id = (SELECT message_id FROM "bank"."view_review_queue" '
        "WHERE deal_id IS NULL ORDER BY score DESC LIMIT 1) "
        "AND a.accessed_at BETWEEN m.sent_at - INTERVAL '7 days' AND m.sent_at",
    ),
)

# The cloudpolicy demo's questions: the security lead's review of the disk-encryption policy.
CLOUDPOLICY = (
    Question(
        "Which attached disks break the policy's disk encryption control, and which section "
        "requires it?",
        'SELECT resource_name AS a FROM "cloudpolicy"."view_cmek_violations" '
        "UNION ALL SELECT '4.2'",
    ),
    Question(
        "Which disks without a customer-managed key are covered by an exception that is still "
        "in date?",
        'SELECT resource_name AS a FROM "cloudpolicy"."view_daily_review" '
        "WHERE status = 'covered by an exception'",
    ),
    Question(
        "Which incidents cite a policy section other than the control they are about?",
        'SELECT number AS a FROM "cloudpolicy"."view_incident_controls" '
        "WHERE finding <> 'cites its control'",
    ),
    Question(
        "Which controls of the policy does no check enforce?",
        'SELECT section AS a FROM "cloudpolicy"."view_unchecked_controls"',
    ),
    Question(
        "Which incidents name a disk the cloud inventory does not have?",
        'SELECT i.number AS a FROM "cloudpolicy"."incident" AS i '
        'LEFT JOIN "cloudpolicy"."storage_resources" AS d ON d.resource_name = i.cmdb_ci__display '
        "WHERE d.resource_name IS NULL",
    ),
)

# Each named demo's questions, by its name (its directory under demo/named).
QUESTIONS: dict[str, tuple[Question, ...]] = {
    "weblogs": WEBLOGS,
    "insider": INSIDER,
    "cloudpolicy": CLOUDPOLICY,
}
