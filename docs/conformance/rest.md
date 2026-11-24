# REST / OpenAPI Conformance

No public conformance suite exists for a REST server built from its own OpenAPI document, so the run checks scenarios taken from OpenAPI 3.1 and RFC 9110: the served document (validated by openapi-spec-validator), each declared read's status, Content-Type and body against the document, and HTTP status rules.

**Suite:** OpenAPI 3.1 and RFC 9110 scenarios.

**Pass rate:** written from the first run of conformance-rest.yml.

## Deliberate differences

### Writes

POST, PUT, PATCH, and DELETE are supported where the write maps onto the model through governed table writes. A write needing structure the model lacks is refused by name.

### Vocabulary fixed by the model

The OpenAPI schema is generated from the model. A path or field the model does not declare is not present in the schema.

### Add-ins

No standard REST add-ins apply here.

## Tools

| Tool | Version | Date | Method |
| --- | --- | --- | --- |
| Postman | — | — | Not yet verified |
| Insomnia | — | — | Not yet verified |
