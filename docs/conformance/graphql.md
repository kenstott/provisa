# GraphQL Conformance

**Suite:** graphql-http server audit 1.22.4.

**Pass rate:** 100.0% (13 of 13) of the requirements (MUST); recommendations met 22 of 47.

<!-- conformance-results:begin -->
## Failing cases

<details><summary>Recommendations (SHOULD) (4)</summary>

| Case | Reason |
| --- | --- |
| 22EB SHOULD accept application/graphql-response+json and match the content-type | Response header content-type does not contain application/graphql-response+json |
| 572B SHOULD use 200 status code on document parsing failure when accepting application/json | Response status code is not 200 |
| FDE2 SHOULD use 200 status code on document validation failure when accepting application/json | Response status code is not 200 |
| 7B9B SHOULD use a status code of 200 on variable coercion failure when accepting application/json | Response status code is not 200 |

</details>

<details><summary>Recommendations (MAY) (21)</summary>

| Case | Reason |
| --- | --- |
| 5A70 MAY accept application/x-www-form-urlencoded formatted GET requests | Response status code is not 200 |
| A5BF MAY use 400 status code when request body is missing on POST | Response status code is not 400 |
| LKJ0 MAY use 400 status code on object {query} parameter | Response status code is not 400 |
| LKJ1 MAY use 400 status code on number {query} parameter | Response status code is not 400 |
| LKJ2 MAY use 400 status code on boolean {query} parameter | Response status code is not 400 |
| LKJ3 MAY use 400 status code on array {query} parameter | Response status code is not 400 |
| 6C00 MAY use 400 status code on object {operationName} parameter | Response status code is not 400 |
| 6C01 MAY use 400 status code on number {operationName} parameter | Response status code is not 400 |
| 6C02 MAY use 400 status code on boolean {operationName} parameter | Response status code is not 400 |
| 6C03 MAY use 400 status code on array {operationName} parameter | Response status code is not 400 |
| 4760 MAY use 400 status code on string {variables} parameter | Response status code is not 400 |
| 4761 MAY use 400 status code on number {variables} parameter | Response status code is not 400 |
| 4762 MAY use 400 status code on boolean {variables} parameter | Response status code is not 400 |
| 4763 MAY use 400 status code on array {variables} parameter | Response status code is not 400 |
| D6D5 MAY allow URL-encoded JSON string {variables} parameter in GETs when accepting application/graphql-response+json | Response status code is not 200 |
| 6A70 MAY allow URL-encoded JSON string {variables} parameter in GETs when accepting application/json | Response status code is not 200 |
| 58B0 MAY use 400 status code on string {extensions} parameter | Response status code is not 400 |
| 58B1 MAY use 400 status code on number {extensions} parameter | Response status code is not 400 |
| 58B2 MAY use 400 status code on boolean {extensions} parameter | Response status code is not 400 |
| 58B3 MAY use 400 status code on array {extensions} parameter | Response status code is not 400 |
| BCF8 MAY use 400 status code on JSON parsing failure | Response status code is not 400 |

</details>

<!-- conformance-results:end -->

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
