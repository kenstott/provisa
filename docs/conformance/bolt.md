# Bolt Conformance

**Suite:** Neo4j driver test kit. TBD: version recorded from CI.

**Pass rate:** TBD: generated from CI.

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
