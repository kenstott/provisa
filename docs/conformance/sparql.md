# SPARQL 1.1 Conformance

**Suite:** W3C SPARQL 1.1 test suite (query, update, result formats). TBD: version recorded from CI.

**Pass rate:** TBD: generated from CI.

## How the suite runs

Provisa has no triple store, so each test's data file is declared as fixture tables before the run: a table per class, a column per literal predicate, a relationship per resource predicate. The test's own query then runs against them. Its constant IRIs are mapped to the fixture's and the answer's mapped back; nothing else of the query changes. Solutions compare as multisets, with blank nodes matched one to one and numbers compared by value. A dataset the tables cannot state faithfully counts its tests as failed, with the reason.

## Deliberate differences

### Writes

This endpoint answers queries. Rows are written through governed table writes, so SPARQL Update is refused by name. An update acting on a named graph (`ADD`, `COPY`, `MOVE`, `CLEAR`, `DROP`, `CREATE`) needs structure the model lacks: the model holds tables, not graphs. Move the rows with a governed write instead.

### Vocabulary fixed by the model

New classes and properties belong in the model as tables, columns, or ontology terms. Add the term there to make it available.

Unbounded property path queries (`?s :p* ?o`) are answered within a depth limit. TBD: exact limit.

### Add-ins

Custom SPARQL functions beyond the W3C built-ins are unavailable inline. Where a function is commonly needed, Provisa offers it as a command. For others, use a `SERVICE` federation clause to call your own SPARQL endpoint.

## Tools

| Tool | Version | Date | Method |
| --- | --- | --- | --- |
| Protégé | TBD | TBD | Covered by driver (SPARQL over HTTP) |
| Stardog Studio | TBD | TBD | Covered by driver (SPARQL over HTTP) |
| Apache Jena (API) | TBD | TBD | TBD: replay or manual |
| GraphDB | TBD | TBD | TBD: replay or manual |
