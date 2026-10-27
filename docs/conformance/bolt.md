# Bolt Conformance

**Suite:** neo4j-python-driver integration tests. TBD: version recorded from CI.

**Pass rate:** TBD: generated from CI.

The official Neo4j Python driver's own integration tests (`tests/integration` of github.com/neo4j/neo4j-python-driver, Apache-2.0), at a pinned release tag, run against Provisa's Bolt endpoint. The driver's tests assume an empty Neo4j database they may write to; a test that creates its own labels fails here, because a Provisa graph's labels and relationship types are its model's.

## Deliberate differences

### Writes

Writes are supported where the write maps onto the model through governed table writes. A write needing structure the model lacks is refused by name. `BEGIN`/`COMMIT`/`ROLLBACK` are a no-op by design.

### Vocabulary fixed by the model

Server-side procedures that mutate labels or create schema elements are not supported. Declare the structure in the model.

### Add-ins

APOC and GDS procedures delivered over Bolt are not available inline. TBD: specific cases from CI.

## Tools

| Tool | Version | Date | Method |
| --- | --- | --- | --- |
| Neo4j Browser | TBD | TBD | Covered by driver (Neo4j JavaScript driver) |
| Neo4j Bloom | TBD | TBD | Covered by driver (Neo4j JavaScript driver) |
| DBeaver (Bolt) | TBD | TBD | TBD: replay or manual |
