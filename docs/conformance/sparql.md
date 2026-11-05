# SPARQL 1.1 Conformance

**Suite:** W3C SPARQL 1.1 test suite (query, update, result formats).

**Pass rate:** The W3C SPARQL 1.1 test suite does not run in CI. An in-process run is the current measure.

## Deliberate differences

### Writes

SPARQL Update is supported where the update maps onto the model through governed table writes. An update needing structure the model lacks is refused with a message naming what is missing.

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
