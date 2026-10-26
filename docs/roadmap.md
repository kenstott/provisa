# Roadmap

This roadmap shows where Provisa is headed. Groupings and timing reflect current plans and may shift as priorities develop. Items listed here are things a user will see—internal engineering work is not shown.

## 2027-Q1

### Connector catalog

- Process columns in batch with a Python or Node function, reducing round-trip overhead on large tables.
- Attach an OWL reasoner to a SPARQL source so Provisa reads inferred triples alongside the asserted ones.

### Glossary, tags and ontology

- Filter the ontology graph with SPARQL directly from the Ontology page controls, or open it as a query.
- Glossary terms derive their OWL type automatically from what they are grounded in—no manual classification.
- See a glossary term's related terms and column bindings as a small interactive graph in its detail view.
- Export your governed graph as Turtle or N-Triples and load it into any triple store you already run.
- Join your Jena, GraphDB or Stardog triples with Provisa's governed data live, with no copy needed.

### Catalog and discovery

- State the business meaning of each data quality check in plain language, not just its technical predicate.
- Define input expectations and output promises for a data product, with check results tracked over time.
- Declare a versioned data contract per output port so consumers know exactly what they can rely on.
- A product's owner sees health and expectations; a consumer sees the contract, SLA and latest check results.

### Roles and capabilities

- Start from built-in analyst, steward and operator roles and extend them, instead of building access from scratch.
- See a role's effective rights—the composition of its scope, capabilities and grants—in one panel.
- See every role that can read, write or mask a column, with the inheritance chain that gave each role access.

### Approvals and query limits

- Refer a pending data registration to another domain steward for confirmation, keeping the request live.

### Federation and routing

- Export server logs with secrets and physical addresses redacted, ready to attach to a support email.
- Add a new data source, query engine or secrets vault by writing a driver, using the same kit the built-ins use.

### Change events and streams

- A preflight check can call Provisa's own services—LLM, vectors, cache, data quality—before a source refresh.

## 2027-Q2

### SPARQL

- Write back to your model through SPARQL Update, with governance enforced on every triple change.
