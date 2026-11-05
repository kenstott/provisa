# Azure Cosmos DB for NoSQL as a source (REQ-1982)

Status: approved 2026-10-12 with the five recommendations of section 10 (see its decisions); being built.

The requirement as given: "Azure Cosmos DB for NoSQL is a source type." This note says what that takes and where it reuses the MongoDB work. That work is the document reader, the parameters of REQ-1986, the pipeline tables of REQ-1980 and the query commands of REQ-1988.

## 1. The source

Cosmos DB for NoSQL stores JSON documents in **containers**, inside a **database**, inside an **account**.

- Every container has a partition key path (for example `/customerId`). A query that names a partition key value reads one partition; a query that does not reads them all (cross-partition).
- The query language is a SQL dialect over the documents: `SELECT c.id, c.total FROM c WHERE c.status = @status`. It takes **named parameters** (`@status`) natively. A parameter's value is sent beside the query text, never inside it.
- Reads cost request units (RU). A throttled read returns 429; the SDK retries it.
- The Python SDK is `azure-cosmos` (`CosmosClient`), and it is not yet a dependency. It is the only reader: Trino, DuckDB, ClickHouse and the Postgres engine have no Cosmos connector.

## 2. Model

**Source type `cosmosdb`.** The source fields:

| Field | Holds |
|---|---|
| `host` | The account endpoint, `https://<account>.documents.azure.com:443/` |
| `database` | The Cosmos database |
| `password` | The account key, as a secret reference, resolved at use |
| `federation_hints.auth` | `key` (default) or `entra`, see section 7 |

**A table is one of two things**, the same split as MongoDB after REQ-1980:

- **Container table.** `table_name` is the container. Its rows are the container's documents, projected onto the declared columns. A column's `path` reads a nested field (`address.city`).
- **Query table.** The table holds a Cosmos SQL query in `query_template`. Each `@name` in the query is a parameter of the table, and its value is bound as a query parameter.

**Parameters (REQ-1986).** A query table's `@name` parameters are parameter columns `_nf_<name>`. They need a value for how they reach the source, and there are two choices:

- **(a)** A new kind, `query_arg` (an argument of the table's native query).
- **(b)** Reuse MongoDB's `pipeline_arg`, renamed to the general `query_arg`, for both MongoDB and Cosmos.

See section 10, decision 1. Either way the steward's required switch and the one enforcement rule apply as they are.

A container table can also expose its **partition key** as an optional parameter. A steward can then make it required, so that no read of the table fans out across every partition. See section 10, decision 2.

## 3. Reading: engine reach

Cosmos is an adapter-fetched source on every engine (`_MATERIALIZE_ONLY`, never `engine_attaches`).

- **Container table, whole.** The native loader runs `container.query_items("SELECT * FROM c", enable_cross_partition_query=True)` page by page, projects each document onto the columns, and lands the rows. This is the same `SourceRowLoader` path MongoDB uses off Trino, and a replica of the table reads the same way.
- **Container table with a partition key parameter.** The loader passes `partition_key=<value>`: one partition is read, and the landed rows are kept under that value.
- **Keyed fetch (`row_materialize`).** `container.read_item(id, partition_key)` is a point read, the cheapest Cosmos read (1 RU). A table whose key is `id` plus its partition key gets the keyed loader, as MongoDB's `$in` fetch does.
- **Query table.** The materialization step runs the query with its parameters, checks the documents against the output columns, and lands them under the arguments given. This is the path of REQ-1980: `_mat_pipeline_table` becomes a native-query branch with one reader per source type.

## 4. Binding and types

A Cosmos query parameter is a JSON value, so the REQ-1980 converter (`provisa/core/parameter_values.py`) is reused as it is. It converts strictly to the declared type and refuses anything else. Cosmos then needs only a JSON encoding of the result:

| Converted value | Sent as |
|---|---|
| int, float, bool, str | itself |
| Decimal | a number (Cosmos has no decimal type, so the precision is that of a double) |
| date, datetime | an ISO 8601 string (Cosmos keeps dates as strings) |
| UUID | its string |
| list (`array(T)`) | a JSON array, e.g. a query vector |
| json | the JSON value |

**Output checking.** `pipeline_rows.project` checks the output and needs one addition: a column declared `date` or `timestamp` reads an ISO 8601 string. Cosmos stores no date type, so a date column is a string field that holds a date. A string that does not parse fails the read and names the column, as any other mismatch does.

## 5. Schema discovery

A container has no schema, so Register Table previews it. The preview reads up to N documents (`SELECT TOP 20 * FROM c`) and suggests the columns with `pipeline_rows.infer_columns`, the same inference as the MongoDB pipeline preview:
- Nested objects are flattened to `a_b` with `path`.
- Arrays become json.

A query table previews its query with sample parameter values, as a MongoDB pipeline table does. The Register Table form gains a Cosmos variant of `QueryTableFields`, with the query editor and the parameter rows.

Cosmos adds system fields to every document (`_rid`, `_self`, `_etag`, `_attachments`, `_ts`). The preview leaves them out, except `_ts` (the last write, in epoch seconds), which can be the watermark. See section 6.

## 6. Change signal

- **`ttl`:** a poll, as for every source.
- **`native`:** the Cosmos change feed (pull model). It lists the documents changed since a continuation token, in each partition. As for MongoDB change streams (REQ-1861), a table on the native signal gets a listener that invalidates or refreshes what is held of it. Deletes appear only in the "all versions and deletes" mode, which needs continuous backup on the account. With the default "latest version" mode a delete is not seen, so the replica keeps a deleted document until its next full refresh. See section 10, decision 3.
- **Watermark.** `_ts` is a watermark the account maintains, so a container table can declare it and take an append-shaped refresh.

## 7. Authentication

- **Account key (default).** The key goes in `password` as a secret reference, resolved at use. It is never written into a string another reach reads, because no other reach exists.
- **Microsoft Entra ID.** A service principal (tenant, client id, client secret as a secret reference) gives a `ClientSecretCredential`. This is the setup most organizations require, because account keys are often disabled. It needs fields on the Sources form, as the Microsoft 365 source has. See section 10, decision 4.

The Sources form gets a Cosmos section: endpoint, database, auth choice and its fields. Every setting has a UI, as the rule requires.

## 8. Search: vector and full text

Cosmos NoSQL offers vector search (`VectorDistance(c.embedding, @vector)` with a vector index policy on the container) and full-text search (`FullTextContains`, `FullTextScore`, `ORDER BY RANK`, and hybrid `RRF`). These are plain Cosmos SQL, so a **query table** serves them with no further mechanism:

```sql
SELECT TOP 10 c.id, c.title, VectorDistance(c.embedding, @query_vector) AS score
FROM c
ORDER BY VectorDistance(c.embedding, @query_vector)
```

Here `query_vector` is an `array(double)` parameter, which a steward makes required. It is joinable and governed like any table. A **query command** (REQ-1988's `source_query` kind, extended to `cosmosdb`) serves a query whose output has no stable shape.

## 9. Test plan

**Unit tests** (`tests/unit`):
- Source fields and the auth choice.
- Parameter binding as query parameters; a value holding SQL syntax stays a value.
- JSON encoding of every converted type.
- Projection of documents, including ISO date strings, and refusal of a bad one.
- Schema inference with system fields left out.
- Partition key routing (`partition_key` passed when the parameter is given).
- Keyed fetch as point reads.
- Registration refusals: a query parameter undeclared or unused; a `query_template` on a container that also names a partition key parameter it does not use.

**Integration and e2e** (CI only, isolated container on a leased port):
- The Cosmos DB Linux emulator (`mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:vnext-preview`) with a seeded container.
- Container table read, joined to another table, on DuckDB.
- Query table with parameters over SQL and GraphQL.
- Missing required parameter refused.
- Partition key parameter reads one partition (RU charge checked).
- Change feed invalidation.
- A grant and a mask applied to Cosmos rows.

**Search tests need a spike first.** Whether the vnext emulator serves vector and full-text queries is unverified. If it does not, those tests need a real account. There are no Azure credentials here; the warehouse credentials in `.env` are Snowflake, Databricks, BigQuery and Fabric. See section 10, decision 5.

## 10. Decisions (maintainer, 2026-10-12)

1. **How a query parameter reaches the source.** `query_arg`, MongoDB's `pipeline_arg` renamed, shared by MongoDB pipelines, Cosmos queries and Node functions later.
2. **The partition key of a container table:** an optional parameter, which a steward can make required.
3. **Deletes on the native change signal:** the change feed's latest-version mode first; a deleted document is seen at the next full refresh, and the Sources form says so.
4. **Entra ID authentication:** a service principal, in the first build.
5. **Search tests:** a spike on whether the vnext emulator serves vector and full-text search comes before those tests are written.
