# GraphQL Conformance

**Suite:** graphql-http compliance audit. TBD: version recorded from CI.

**Pass rate:** TBD: generated from CI.

## Deliberate differences

### Writes

Mutations are supported where the write maps onto the model through governed table writes. A mutation needing structure the model lacks is refused by name.

### Vocabulary fixed by the model

The GraphQL schema is generated from the model. Types, fields, and arguments reflect exactly what the model declares. A type or field the model does not declare is not present in the schema.

### Add-ins

No standard GraphQL add-ins apply here. Custom directives are part of the model, not added inline.

## Tools

| Tool | Version | Date | Method |
| --- | --- | --- | --- |
| Apollo Studio | TBD | TBD | TBD: replay or manual |
| Insomnia | TBD | TBD | TBD: replay or manual |
| Postman | TBD | TBD | TBD: replay or manual |
