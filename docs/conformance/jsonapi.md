# JSON:API Conformance

TBD: suite to be chosen.

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
| Postman | TBD | TBD | TBD: replay or manual |
