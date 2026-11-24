# JSON:API Conformance

JSON:API publishes no server conformance suite, so the run checks scenarios taken from the JSON:API 1.1 specification: content negotiation, document structure, fetching data, query parameters and errors.

**Suite:** JSON:API 1.1 specification scenarios.

**Pass rate:** written from the first run of conformance-jsonapi.yml.

## Deliberate differences

### Writes

POST, PATCH, and DELETE are supported where the write maps onto the model through governed table writes. A write needing structure the model lacks is refused by name.

### Vocabulary fixed by the model

The JSON:API schema is generated from the model. A resource type or relationship the model does not declare is not present in the schema.

### Add-ins

No standard JSON:API add-ins apply here.

## Tools

| Tool | Version | Date | Method |
| --- | --- | --- | --- |
| Postman | — | — | Not yet verified |
