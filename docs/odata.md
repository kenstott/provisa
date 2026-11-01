# OData

## Assumptions to test

- Excel and Power BI read the service document and `$metadata`, then the entity set.
- Excel and Power BI send `$select`, `$filter`, `$orderby`, `$top`, `$skip` and `$count`.
- Excel and Power BI follow `@odata.nextLink`.
- Excel and Power BI sign in with Basic or a bearer token.
- An unauthenticated request gets 401 with `WWW-Authenticate`.
- Every response carries `OData-Version: 4.0`.
- JSON responses are `application/json;odata.metadata=minimal`.
- The clients accept an absolute `@odata.nextLink` built from `X-Forwarded-Proto`, `X-Forwarded-Host` or `Host`.
- The clients accept an entity type with no `Key` for a table whose key the role cannot see or that has none.
- The clients accept a referential constraint on single-valued navigation properties only.
- The clients accept every non-key property as nullable.
- The clients accept a timestamp with no offset as UTC in `Edm.DateTimeOffset`.
- The clients accept a 501 for `$filter`, `$orderby` and `$expand` until those are served.
- The clients accept a page of 5000 rows, and do not need `Prefer: odata.maxpagesize` honoured.
- The clients accept minimal JSON whatever their `Accept` header asks for.

## The service

The service is at `/data/odata` on the HTTP port, so it has no port or startup line of its own.

| Request | Answer |
|---|---|
| `GET /data/odata/` | The service document: the role's entity sets |
| `GET /data/odata/$metadata` | The role's CSDL |
| `GET /data/odata/{set}` | A page of the entity set |
| `GET /data/odata/{set}/$count` | The role's row count, as plain text |

## The model

`$metadata` is generated from the surface the role is served through, as its gRPC proto is. A table or column the role cannot see is not in it, and asking for one is answered as for a name that does not exist.

| Model | OData |
|---|---|
| Table | Entity set, named by the table's GraphQL root field |
| Column | Property, named by its GraphQL field |
| Primary key | Key |
| Relationship | Navigation property |
| Column pair of a to-one relationship | Referential constraint |

A navigation property appears only where the role can see the target table. A constraint appears only where the role can see both columns. A relationship through a junction or an expression has none.

A table that can only be read with arguments, such as an API source with a required parameter, has no entity set.

| Column type | Edm type |
|---|---|
| `tinyint` | `Edm.SByte` |
| `smallint` | `Edm.Int16` |
| `integer` | `Edm.Int32` |
| `bigint` | `Edm.Int64` |
| `real` | `Edm.Single` |
| `double` | `Edm.Double` |
| `decimal(p,s)` | `Edm.Decimal`, with its precision and scale |
| `boolean` | `Edm.Boolean` |
| `date` | `Edm.Date` |
| `time` | `Edm.TimeOfDay` |
| `timestamp`, `timestamptz` | `Edm.DateTimeOffset` |
| `uuid` | `Edm.Guid` |
| Binary types | `Edm.Binary` |
| `interval`, JSON, arrays and every other type | `Edm.String` |

An interval is a string because `Edm.Duration` cannot hold months.

## Reads

A read accepts `$select`, `$top`, `$skip`, `$count` and `$format=json`. It becomes one statement through the governed pipeline, so row rules, masks, column grants, the role's row ceiling and the audit record apply as on every transport. `$count=true` runs a second statement through the same pipeline.

Rows are read in key order, or with no key in the order of every sortable property, so pages do not overlap. A page holds at most 5000 rows, and `@odata.nextLink` leads to the next one. A role's row ceiling ends the feed: the last page has no link, and no row past the ceiling is served.

`$filter`, `$orderby`, `$expand`, `$search`, `$apply`, `$compute` and `$levels` are answered 501 rather than ignored. Any other `$` option is answered 400, and a `$format` other than JSON is answered 406.
