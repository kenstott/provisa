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
- The clients accept a 501 for a `$filter` function or an `$orderby` expression the service does not serve, and for `$expand=*`.
- The clients expand with `$expand` and follow `@odata.nextLink` in a nested collection.
- The clients accept a 501 for a list past its bound whose parent has no key the role can address.
- The clients send the date and time literals OData defines, and do not compare a date or timestamp with a string.
- The clients accept a 403 for a filter on a masked column.
- The clients accept a page of 5000 rows, and do not need `Prefer: odata.maxpagesize` honoured.
- The clients accept minimal JSON whatever their `Accept` header asks for.

## The service

The service is at `/data/odata` on the HTTP port, so it has no port of its own. It has its line in the startup log and its row on the admin System health table, as the other HTTP surfaces do, and the OData page under Explore gives its address to copy.

Excel and Power BI send no `X-Provisa-Role`, so a read is as the role the sign-in resolves to. Under `assignments_source: claims` that is the first `role_mapping` rule it matches, else `default_role`, whether or not the sign-in holds that role. A deployment serving more than one role needs a rule for each.

| Request | Answer |
|---|---|
| `GET /data/odata/` | The service document: the role's entity sets |
| `GET /data/odata/$metadata` | The role's CSDL |
| `GET /data/odata/{set}` | A page of the entity set |
| `GET /data/odata/{set}/$count` | The role's row count, as plain text, of the rows a `$filter` meets when it has one |
| `GET /data/odata/{set}({key})/{navigation}` | A page of one row's to-many navigation property |

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

A read accepts `$filter`, `$orderby`, `$select`, `$expand`, `$top`, `$skip`, `$count` and `$format=json`. It becomes one statement through the governed pipeline, so row rules, masks, column grants, the role's row ceiling and the audit record apply as on every transport. `$count=true` runs a second statement through the same pipeline, and counts the rows `$filter` meets.

Rows are read in `$orderby` order, then in key order, or with no key in the order of every sortable property, so pages do not overlap. A page holds at most 5000 rows, and `@odata.nextLink` leads to the next one. A role's row ceiling ends the feed: the last page has no link, and no row past the ceiling is served.

`@odata.nextLink` keeps `$filter` and `$orderby` as the client sent them.

`$search`, `$apply`, `$compute` and `$levels` are answered 501 rather than ignored. Any other `$` option is answered 400, and a `$format` other than JSON is answered 406.

A request runs under `limits.request_timeouts.odata`, or `limits.request_timeout` when that has no value.

## Filtering

`$filter` is parsed into a tree, then written as the `WHERE` of the read's one statement. `validate_sql` and `apply_governance` read it as they read any `WHERE`.

| Form | Served |
|---|---|
| Comparison | `eq`, `ne`, `gt`, `ge`, `lt`, `le` |
| Logical | `and`, `or`, `not`, parentheses |
| Membership | `in`, as in `id in (1, 2)`; a listed `null` also matches null |
| String | `contains`, `startswith`, `endswith`, `tolower`, `toupper`, `length` |
| Date | `year`, `month`, `day` |
| Literal | Strings, with a quote doubled; numbers; `true`, `false`, `null`; dates; times; timestamps with an offset; guids |

`contains`, `startswith` and `endswith` take a string literal as their second argument, and `%` and `_` in it are those characters.

| What it reads | How it compares |
|---|---|
| `x eq null`, `x ne null` | `IS NULL`, `IS NOT NULL` |
| `x ne 'a'` on a null | True, as OData says |
| `a eq b` on two nulls | True, as OData says |
| `not (x gt 1)` on a null | True: the comparison is false |
| A timestamp with an offset | The instant, in UTC; a timestamp with no offset is read as UTC |
| A date against a timestamp | Midnight UTC of that date |

A string, date, time or guid is a bound value. A number or boolean is a literal the parser read as one. A name is looked up in the role's model, so no text from the request reaches the statement.

A column the role cannot see is answered 400, as for a name that does not exist. A value of the wrong type for what it is compared with is answered 400, as is text that is no filter. An expression may nest 64 levels and hold 1000 terms.

| Not served, answered 501 naming it |
|---|
| Any other function, such as `substring` or `hour` |
| `add`, `sub`, `mul`, `div`, `divby`, `mod`, `has` |
| A path, such as `customer/name`, and `any` or `all` over one |
| `duration'...'`, `binary'...'` and qualified names |
| A `contains`, `startswith` or `endswith` searching for anything but a string literal |
| A column served as `Edm.String` whose type is JSON, an array or an interval |

### Masked columns

A filter on a masked column is refused, as on every transport. The statement fails `validate_sql` with V005 and the client gets 403 naming the column. Nothing runs. Otherwise a caller could recover the real value one comparison at a time.

A faked column is the exception, also as on every transport. Its table is read through its faked projection, so a filter on it compares the fake the role is shown, and the real value matches nothing.

## Ordering

`$orderby` takes properties, each with `asc` or `desc`, and sorts asc by default. A null sorts first ascending and last descending, as OData says. A property named twice is answered 400, and a function or a path is answered 501.

A masked column sorts by the value the role is shown, so the real values' order does not show through. A column the role cannot see is answered 400.

## Expanding

`$expand` nests a row's related rows under its navigation property, in the read's one statement. The related rows are compiled as GraphQL compiles a relationship, as a subquery per field, so a parent stays one row whatever it expands. The subquery is read by `validate_sql` and `apply_governance` as any SELECT, so the related table's row rules, masks and column grants hold in it. A relationship into a table the role cannot see is answered 400, as for a name that does not exist.

| Option in an expansion | To-many | To-one |
|---|---|---|
| `$select` | Served | Served |
| `$expand` | Served | Served |
| `$filter` | Served, as at the root | 400 |
| `$orderby` | Served, as at the root | 400 |
| `$top`, `$skip` | Served | 400 |
| `$count` | 501 | 501 |
| `$levels`, `$search`, `$apply`, `$compute` | 501 | 501 |

`$format` in an expansion is answered 400. Options are separated by `;`, as in `orders($filter=id gt 1;$select=id;$top=5)`. Expansions nest 4 levels. `*`, a path such as `customer/orders`, and `/$ref` or `/$count` are answered 501.

A filter on a masked column of the related table is refused as at the root, with 403 and nothing run. An order on one sorts by the value the role is shown.

A to-one property with no related row is `null`. A to-many property with none is `[]`. A table replicated row by row is joined rather than aggregated, so expanding a to-one property into one is answered 501.

### Bounded lists

An expanded list holds at most 1000 rows per parent. A longer one carries `orders@odata.nextLink` beside it, leading to `{set}({key})/orders` with the expansion's own options. That read is the same statement for one parent, so its row rules and masks are the same, and it pages and nests as an expansion does. It takes no `$count`. A single-valued navigation property is not read by path, and is answered 501.

A role's row ceiling holds a list as it holds a read: a ceiling below the bound ends the list with no link.

A nextLink needs the parent's key. A list past its bound under a parent with no key, or whose key the role reads masked, is answered 501 naming it, rather than cut short with nothing to say so. A list within its bound is served under any parent. Ordering within a list is the database's: it is exact on PostgreSQL, where the list is built from sorted rows, and an engine that does not keep that order keeps a page's rows but not their order in it.
