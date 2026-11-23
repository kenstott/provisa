# SPARQL 1.1 Conformance

**Suite:** W3C SPARQL 1.1 test suite (query, update, result formats).

**Pass rate:** Not yet measured. The Conformance (SPARQL) workflow runs the full suite in CI when it is dispatched for a release. A test that fails and is not on `scripts/conformance/expected/sparql.json` fails the run, and so does a listed test that passes.

## How the suite runs

Provisa has no triple store, so each test's data file is declared as fixture tables before the run: a table per class, a column per literal predicate, a relationship per resource predicate. The test's own query then runs against them. Its constant IRIs are mapped to the fixture's and the answer's mapped back. A variable predicate gets one `FILTER`, keeping it from the key column and class every table adds. Nothing else of the query changes. Solutions compare as multisets, with blank nodes matched one to one and numbers compared by value. A dataset the tables cannot state faithfully counts its tests as failed, with the reason.

## Deliberate differences

### Writes

Graph management is refused by name, by design. `DROP`, `CLEAR`, `LOAD`, `CREATE`, `COPY`, `MOVE` and `ADD` act on named graphs, and Provisa has none: the model holds tables, not graphs. An update containing one of these operations is refused whole, and its tests count as skipped by design. To move rows, write to the tables.

SPARQL Update's data operations (`INSERT DATA`, `DELETE DATA`, `DELETE`/`INSERT ... WHERE`) are planned on the write path Cypher uses, so a SPARQL write is governed the same way a Cypher write is. Until then they are refused by name, and their tests count as failed.

### Vocabulary fixed by the model

New classes and properties belong in the model as tables, columns, or ontology terms. Add the term there to make it available.

Unbounded property path queries (`?s :p* ?o`) are answered within the server's depth limit, which defaults to 10 hops. A result cut at the limit includes a warning. Set `graph.depth_limit` to raise it.

### Add-ins

Custom SPARQL functions beyond the W3C built-ins are unavailable inline. Where a function is commonly needed, Provisa offers it as a command. For others, use a `SERVICE` federation clause to call your own SPARQL endpoint.

## Tools

| Tool | Version | Date | Method |
| --- | --- | --- | --- |
| Protégé | — | — | Covered by driver (SPARQL over HTTP) |
| Stardog Studio | — | — | Covered by driver (SPARQL over HTTP) |
| Apache Jena (API) | — | — | Not yet verified |
| GraphDB | — | — | Not yet verified |
