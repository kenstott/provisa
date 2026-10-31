# RDF 1.1 Syntax Conformance

**Suite:** W3C rdf-tests (Turtle, N-Triples). TBD: version recorded from CI.

**Pass rate:** TBD: generated from CI.

## Deliberate differences

### Writes

RDF syntax is used for data import and export. A document that maps onto the model through governed table writes is accepted; one needing structure the model lacks is refused with a message naming what is missing.

### Vocabulary fixed by the model

Classes and properties in an imported RDF document are resolved against the model. Terms not in the model are refused. Add the term first, then import.

### Add-ins

No add-ins apply to RDF syntax conformance.

## Tools

| Tool | Version | Date | Method |
| --- | --- | --- | --- |
| Apache Jena (RDF parser) | TBD | TBD | TBD: replay or manual |
| rdflib (Python) | TBD | TBD | TBD: replay or manual |
