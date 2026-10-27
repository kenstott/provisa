# Copyright (c) 2026 Kenneth Stott
# Canary: fb2162e8-7731-43bc-acf6-4acd1d11c497
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Neo4j Python driver's examples, their setup and cleanup adapted to the job's fixture model
(REQ-2167, "setup that creates objects is adapted, not excluded").

Only setup and cleanup change, never what an example tests:

- ``MATCH (_) DETACH DELETE _``, an example's cleanup, deletes the Person nodes the examples write:
  the examples write no other node, and a person's relationships are junction rows that cascade
  with it (fixtures.py).
- A MERGE that, in a graph each example empties first, can only create is the CREATE it does, and
  a MERGE of a company the fixture seeds is its MATCH.

Each rewrite names the text it replaces and how often it occurs, so a driver release that changes
an example fails here, by name, instead of running the example unadapted.

Usage: adapt.py <the driver checkout's tests/integration>
"""

# Requirements: REQ-2167

from __future__ import annotations

import sys
from pathlib import Path

CLEANUP = '"MATCH (_) DETACH DELETE _"'
PERSONS_DELETED = '"MATCH (_:Person) WHERE _.id IS NOT NULL DELETE _"'

#: (file under tests/integration, text, its replacement, how often the text occurs)
REWRITES: list[tuple[str, str, str, int]] = [
    *(
        (f"examples/{name}.py", CLEANUP, PERSONS_DELETED, count)
        for name, count in (
            ("test_autocommit_transaction_example", 1),
            ("test_pass_bookmarks_example", 2),
            ("test_read_write_transaction_example", 2),
            ("test_result_consume_example", 2),
            ("test_result_retain_example", 2),
            ("test_session_example", 2),
            ("test_transaction_function_example", 1),
            ("test_transaction_metadata_config_example", 1),
            ("test_transaction_timeout_config_example", 1),
        )
    ),
    (
        "examples/test_pass_bookmarks_example.py",
        '"MERGE (a)-[:KNOWS]->(b)"',
        '"CREATE (a)-[:KNOWS]->(b)"',
        1,
    ),
    (
        "examples/test_result_retain_example.py",
        '"MERGE (com:Company {name: $company_name}) "',
        '"MATCH (com:Company {name: $company_name}) "',
        1,
    ),
    (
        "examples/test_result_retain_example.py",
        '"MERGE (emp)-[:WORKS_FOR]->(com)"',
        '"CREATE (emp)-[:WORKS_FOR]->(com)"',
        1,
    ),
]


def adapt(tests: Path) -> None:
    for name, text, replacement, count in REWRITES:
        path = tests / name
        source = path.read_text()
        found = source.count(text)
        if found != count:
            raise SystemExit(
                f"{name}: {text} occurs {found} times, where the adaptation expects {count}"
            )
        path.write_text(source.replace(text, replacement))
        print(f"{name}: {text} -> {replacement} ({count})")


if __name__ == "__main__":
    adapt(Path(sys.argv[1]))
