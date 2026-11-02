# RDF 1.1 Syntax Conformance

**Suite:** W3C rdf-tests (Turtle, N-Triples).

**Pass rate:** The W3C rdf-tests suite does not run in CI. An in-process run is the current measure.

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
| Apache Jena (RDF parser) | — | — | Not yet verified |
| rdflib (Python) | — | — | Not yet verified |
