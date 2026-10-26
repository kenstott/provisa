# Cypher Conformance

**Suite:** openCypher TCK. TBD: version recorded from CI.

**Pass rate:** TBD: generated from CI.

## Deliberate differences

### Writes

`CREATE`, `MERGE`, `SET`, and `DELETE` are supported where the write maps onto the model through governed table writes. A write needing structure the model lacks is refused by name.

### Vocabulary fixed by the model

Dynamic labels and relationship types (e.g. `MATCH (n:$label)`) are not supported. Labels and relationship types are fixed by the model and must be declared there. This is a structural characteristic, not a limitation to be worked around.

### Add-ins

The APOC calls Cypher tools and agent frameworks make are offered as commands and functions: `apoc.meta.data`, `apoc.meta.schema` and `apoc.schema.nodes` answer from the role's graph, the common `apoc.text`, `apoc.coll`, `apoc.map`, `apoc.date` and `apoc.convert` functions push down with the query, and `apoc.path.expand` and `apoc.path.subgraphNodes` walk the graph as variable-length patterns. See [The APOC set Provisa ships](../commands.md#the-apoc-set-provisa-ships). Procedures that act outside the model (`apoc.create.*`, `apoc.periodic.iterate`, `apoc.load.*`) are not offered; for those, run them on your own Neo4j instance against data it reads from Provisa over Bolt. GDS algorithms are not available inline.

### Scenarios that build their own graph

TCK scenarios that create their graph (`CREATE` in their setup, a named TCK graph, a test procedure) are not runnable: a Provisa graph's labels and relationship types are its model's. The pass rate counts the scenarios that run.

## Tools

| Tool | Version | Date | Method |
| --- | --- | --- | --- |
| Neo4j Browser | TBD | TBD | Covered by driver (Neo4j JavaScript driver) |
| Neo4j Bloom | TBD | TBD | Covered by driver (Neo4j JavaScript driver) |
| neo4j-driver (Python) | TBD | TBD | Covered by driver |
| neo4j-driver (Java) | TBD | TBD | Covered by driver |
