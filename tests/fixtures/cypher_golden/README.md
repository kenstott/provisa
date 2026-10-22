# Cypher golden SQL

Captured from 3c3aff532, the last commit before the graph compiler was split (REQ-2135), by
`scripts/cypher_golden.py`. It holds every Cypher translation the unit tests made there, and every
tour, perf-bench and UI e2e Cypher statement compiled against its model's label map.

`entries.jsonl` has one translation per line: where it came from, its Cypher text, the label map
and parameters it was given (under `blobs/`), and the SQL, parameter order and graph-variable
kinds it produced, or the refusal it raised. `tests/unit/test_cypher_golden_sql.py` replays each
one and requires the same result, byte for byte. A difference is a regression unless it is
approved query by query.
