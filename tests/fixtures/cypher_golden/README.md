# Cypher golden SQL

Captured from 3c3aff532, the last commit before the graph compiler was split (REQ-2135), by
`scripts/cypher_golden.py`. It holds every Cypher translation the unit tests made there, and every
tour, perf-bench and UI e2e Cypher statement compiled against its model's label map.

`entries.jsonl` has one translation per line: where it came from, its Cypher text, the label map
and parameters it was given (under `blobs/`), and the SQL, parameter order and graph-variable
kinds it produced, or the refusal it raised. `tests/unit/test_cypher_golden_sql.py` replays each
one and requires the same result, byte for byte. A difference is a regression unless it is
approved query by query.

`approved.jsonl` holds the translations whose result was changed on purpose, each approved query by
query: captured from the checkout that makes the change, with `capture-approved-tests` (a test's
translations) or `capture-approved` (one statement over a model), with the approval recorded in
the entry. An entry an approval replaces is removed from `entries.jsonl`.

Capture with `PYTHONPATH` set to the checkout being captured: the shared venv's editable install
extends `provisa` with another checkout's modules, whose code would otherwise be recorded.
