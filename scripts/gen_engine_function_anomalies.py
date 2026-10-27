# Copyright (c) 2026 Kenneth Stott
# Canary: 7c4e2a91-5b38-4f60-9d17-2e8a6c3b4f05
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Write the known anomalies of the shipped SQL expression functions (the APOC set, REQ-2168) into
docs/engines.md: each function an engine's dialect cannot write, computed from the templates, never
typed. A call of one on that engine is refused by name when the statement is compiled.

    .venv/bin/python3 scripts/gen_engine_function_anomalies.py          # rewrite the section
    .venv/bin/python3 scripts/gen_engine_function_anomalies.py --check  # exit 1 when it is stale
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
DOC = REPO / "docs" / "engines.md"
BEGIN = "<!-- engine-function-anomalies:begin -->"
END = "<!-- engine-function-anomalies:end -->"

# Each federation engine (federation.engine.engine_kinds) by the SQL dialect it is written in.
ENGINE_DIALECTS: dict[str, str] = {
    "bigquery": "bigquery",
    "clickhouse": "clickhouse",
    "clickhouse-server": "clickhouse",
    "cockroachdb": "postgres",
    "databricks": "databricks",
    "duckdb": "duckdb",
    "exasol": "exasol",
    "fabric": "tsql",
    "greenplum": "postgres",
    "mariadb": "mysql",
    "motherduck": "duckdb",
    "mssql": "tsql",
    "mysql": "mysql",
    "opengauss": "postgres",
    "oracle": "oracle",
    "pg": "postgres",
    "redshift": "redshift",
    "singlestore": "singlestore",
    "snowflake": "snowflake",
    "synapse": "tsql",
    "teradata": "teradata",
    "tidb": "mysql",
    "trino": "trino",
    "trino-byo": "trino",
    "yugabytedb": "postgres",
}


def anomalies() -> list[tuple[str, str]]:
    """(engine, function) for each shipped function the engine's dialect cannot write."""
    sys.path.insert(0, str(REPO))
    from provisa.core.builtin_commands import declared
    from provisa.core.command_bindings import offered_name
    from provisa.core.sql_expression import writable

    found = []
    for engine, dialect in sorted(ENGINE_DIALECTS.items()):
        for fn in declared():
            if fn["impl_kind"] == "sql_expression" and not writable(fn, dialect):
                found.append((engine, offered_name(fn, "cypher")))
    return found


def section() -> str:
    rows = "\n".join(f"| `{engine}` | `{fn}` |" for engine, fn in anomalies())
    return f"{BEGIN}\n| Engine | Function not available |\n| --- | --- |\n{rows}\n{END}"


def main(argv: list[str]) -> int:
    text = DOC.read_text()
    start, end = text.index(BEGIN), text.index(END) + len(END)
    current = text[start:end]
    wanted = section()
    if "--check" in argv:
        return 0 if current == wanted else 1
    DOC.write_text(text[:start] + wanted + text[end:])
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
