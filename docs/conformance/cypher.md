# Cypher Conformance

**Suite:** openCypher TCK. TBD: version recorded from CI.

**Pass rate:** TBD: generated from CI.

## Deliberate differences

### Writes

`CREATE`, `MERGE`, `SET`, and `DELETE` are supported where the write maps onto the model through governed table writes. A write needing structure the model lacks is refused by name.

### Vocabulary fixed by the model

Dynamic labels and relationship types (e.g. `MATCH (n:$label)`) are not supported. Labels and relationship types are fixed by the model and must be declared there. This is a structural characteristic, not a limitation to be worked around.

### Add-ins

APOC procedures and GDS algorithms are not available inline. Where a procedure is commonly needed, Provisa offers it as a command. For others, use `apoc.bolt.load` or `apoc.bolt.execute` from your own Neo4j instance querying Provisa over Bolt, so your store runs the procedure and Provisa provides the data.

Real tools and agent frameworks interact through Bolt. Conformance entries for specific APOC calls reflect those captured as replays, covering what real callers use, not the full APOC catalogue.

## Tools

| Tool | Version | Date | Method |
| --- | --- | --- | --- |
| Neo4j Browser | TBD | TBD | Covered by driver (Neo4j JavaScript driver) |
| Neo4j Bloom | TBD | TBD | Covered by driver (Neo4j JavaScript driver) |
| neo4j-driver (Python) | TBD | TBD | Covered by driver |
| neo4j-driver (Java) | TBD | TBD | Covered by driver |
