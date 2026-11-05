# REST / OpenAPI Conformance

No public REST conformance suite runs in CI today.

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
