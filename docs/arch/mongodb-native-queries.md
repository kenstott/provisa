# MongoDB native queries (REQ-1980)

Status: approved 2026-10-12 with the three open points of section 9 decided; built as REQ-1985 (Atlas connections), REQ-1986 (table parameters) and REQ-1980 (pipeline tables).

Target: Hasura DDN's MongoDB native queries of the collection representation. Those are pipelines over an input collection, including Atlas `$search`, `$searchMeta` and `$vectorSearch`. They act as sources in relational queries, with typed, validated arguments that are bound into the pipeline.

- **Shape A** (this note): a table registered from a pipeline.
- **Shape B** (after A): a query command for a pipeline whose output has no stable schema. It is covered briefly at the end.

## 1. What exists today

| Piece | Where | Reused for |
|---|---|---|
| Native MongoDB reader (pymongo; `find` only) | `provisa/mongodb/fetch.py` | Gains an `aggregate` reader |
| Query-API tables: a table whose rows a stored query produces (Cypher, SPARQL) | `Table.query_template` (`core/models.py`), `registered_tables.query_template` (`core/schema_org.py:308`), `api/admin/_query_api_registration.py`, `core/config_loader.py:451` | Storage, the config and registration checks, and the preview-then-register form flow |
| Parameter columns (`_nf_<name>`, `Column.native_filter_type`) | `core/models.py:845` | Pipeline arguments |
| Argument extraction from SQL: top-level AND-ed `_nf_x = literal` or `= $N` | `compiler/nf_extractor.py:72` (`extract_nf_args`); `pgwire/_pipeline.py:1631` | Unchanged, plus one extension (section 4) |
| GraphQL field arguments from parameter columns | `compiler/schema_gen.py:562` (`_build_native_filter_args`); `compiler/sql_gen.py:502` | Unchanged, plus the NonNull rule (section 3) |
| Rows landed in the engine before the query runs, in a cache table keyed by argument values | `api/data/materialization.py:850` (`_materialize_api_to_engine_cache`); `api_source/engine_cache.py:132` (`cache_table_name`) | One new branch |
| Missing-argument refusal | `materialization.py:924-958` (graphql_remote `required_args` → `dropped_tables`), raised by `nf_extractor.apply_dropped_tables` | The same reason text and raise path |
| Parameter columns excluded from data columns, rights, replicas and the profiler | `security/rights.py:333-373`, `federation/replica_routing.py:211`, `profiler/source.py:180` | Unchanged, plus one profiler check |
| Vector dimension check | `vector/query.py:61` (`validate_vector_dimensions`), `Column.embedding_model` | Vector arguments |

Facts found while reading that bear on the design:

- `native_filter_required` is stored, but **nothing enforces it at query time** on any source kind.
  - GraphQL NonNull comes from `native_filter_type == "path_param"` alone (`schema_gen.py:584`).
  - Only graphql_remote refuses a missing argument; it does so through its own `required_args`.
  - OpenAPI skips the table and logs (`materialization.py:728`).
  - gRPC does not check at all.

  This design enforces the flag for `query_arg` only. The general gap is a separate defect and is not fixed here.
- The comment at `core/models.py:845` lists only `path_param | query_param`. In fact gRPC also writes `grpc_input`.
- `limit_arg`, `offset_arg` and `cursor_arg` are graphql_remote pagination keys. They are not parameter-column types.
- MongoDB tables route `ENGINE` (`transpiler/router.py:55`). Neo4j tables also route `ENGINE`; their rows are landed by the materialization step before routing. A pipeline table follows the neo4j path.

## 2. Model

No new table and no new column on `registered_tables`.

**`Table.query_template`**: on a MongoDB source it holds the native query as one Extended JSON document. The shape follows DDN's native-query file:

```json
{
  "inputCollection": "movies",
  "pipeline": [
    {"$vectorSearch": {"index": "plot_vec", "path": "plot_embedding",
                       "queryVector": "{{query_vector}}", "numCandidates": 200,
                       "limit": "{{k}}"}},
    {"$project": {"_id": 0, "movie_id": 1, "title": 1,
                  "score": {"$meta": "vectorSearchScore"}}}
  ]
}
```

- `table_name` is the served name. `schema_name` is the database, as for collection tables.
- `inputCollection` is required. A database-level pipeline (`db.aggregate`, e.g. `$documents`) is out of scope for Shape A.
- `_QUERY_API_TYPES` and `QUERY_API_TYPES` gain `mongodb`. On MongoDB, `query_template` is **optional**: without it the table is a collection table, as today. With it the table is a pipeline table. The neo4j and sparql rule ("required") is unchanged.

**Arguments.** One parameter column per argument.
- Name `_nf_<arg>`.
- `native_filter_type = "query_arg"`.
- `native_filter_required` set per argument (true or false, never None, for this type).
- `data_type` is the argument's type: an IR type, or `array(<IR scalar>)`.
- Optional `embedding_model` on an `array(double)` argument fixes its dimension.

**Validation at registration and at config load** (fail loud; `code` values follow the `schema.*` pattern):
- Every `{{name}}` placeholder names a declared argument, and every declared argument appears at least once.
- A placeholder is a whole JSON string value. A placeholder inside a longer string (`"title: {{q}}"`) is refused, because that would be splicing.
- The document parses as Extended JSON (`bson.json_util.loads`), and `pipeline` is a list of single-key stage objects.
- Pipeline tables have no `change_signal = native` (change streams watch a collection, not a pipeline's output) and no `row_materialize` (there is no key to fetch by).

## 3. Arguments: enforcement and binding

**Getting values (unchanged mechanism).**
- SQL clients write `WHERE similar_movies._nf_query_vector = $1 AND similar_movies._nf_k = 10`, the same as for an OpenAPI path parameter.
- GraphQL clients write `similar_movies(query_vector: [...], k: 10) { ... }`.
- Both arrive in `nf_args`.

**Required arguments.**
- In `_materialize_api_to_engine_cache`, the new branch checks every `query_arg` with `native_filter_required = true`.
- A missing one goes to `dropped_tables` with the existing reason text ("requires filter(s) [...] — add a WHERE clause with the required parameter(s)"). `apply_dropped_tables` raises it for a lone table, and a UNION sweep (a graph count) drops the branch, as for graphql_remote.
- In GraphQL, `_build_native_filter_args` makes the argument NonNull when `native_filter_type == "path_param"`, or when it is `"query_arg"` and required. Other types keep today's rule.

**Optional arguments.** An absent optional argument binds BSON `null`. A pipeline that must behave differently without it says so itself (`$ifNull`, `$cond`). There is no other default, no fallback and no stage pruning.

**Binding.** The pipeline is parsed once into Python objects. A walk replaces each placeholder node with the converted value, and pymongo encodes the result as BSON. The text is never re-formatted, so a value can only ever be a value. A string argument that holds `{"$gt": ""}` stays the literal string.

**Type conversion.** Strict: a value that does not convert is refused, naming the table, the argument and the expected type, and nothing runs at the source.

| Argument `data_type` | Accepted input | BSON value |
|---|---|---|
| `text` | string | string |
| `smallint`, `integer` | integer (or a numeric string from SQL) | int32 |
| `bigint` | integer | int64 (`Int64`) |
| `float`, `double` | number | double |
| `numeric` | number or numeric string | Decimal128 |
| `boolean` | bool (`true`/`false` text from SQL) | bool |
| `date` | ISO date | date (UTC midnight) |
| `timestamp` | ISO timestamp | date (UTC) |
| `uuid` | UUID text | Binary subtype 4 |
| `json` | JSON text or object, read as Extended JSON | the document (so `{"$oid": "..."}` → ObjectId) |
| `array(T)` | a JSON array text, a pgwire array parameter, or an SQL `ARRAY[...]` literal; each element converted as `T` | array |

- **Vectors.** `array(double)`, plus `embedding_model` when one is set: the length is checked with `validate_vector_dimensions`. A `$vectorSearch` `queryVector` receives a BSON array of doubles.
- **ObjectId arguments.** These need no new IR type: a `json` argument takes `{"$oid": "..."}`.

**One extractor change.** `extract_nf_args` accepts literals and `$N` only. It gains an `ARRAY[<numeric literals>]` value (SQL vector literals). A string literal holding a JSON array, and a bound array parameter, already reach it as values.

## 4. Routing and engine reach

**Reader.** `fetch.aggregate_row_batches(conn, database, input_collection, pipeline, columns, batch_rows)` runs `collection.aggregate(pipeline, batchSize=...)` in a thread and yields batches like `iter_row_batches`. It is the only reader of a pipeline table. The Trino and ClickHouse MongoDB connectors cannot run pipelines, so a pipeline table never reaches them.

**Materialization branch.** One new `elif` in `_materialize_api_to_engine_cache`, plus the same lookup in `would_materialize_optimize`:
1. Look up the pipeline table and check the required arguments.
2. Bind the values and run the reader.
3. Validate the rows (section 5).
4. Land the rows with `land_api_cache` under `cache_table_name(source_id, "pipeline:" + table_name, args)`, with the table's `cache_ttl`.

`rewrite_all_from_cache` then points the query at the cache table. This works the same way under DuckDB, Trino and the warehouse engines, because the engine only ever sees a landed table.

**Joins.** A pipeline table joins like any API table: the engine joins the landed rows. A join that feeds an argument from another table's column (a lateral, per-key fill) is out of scope for Shape A. The argument must be a constant in the statement.

**Collection tables are unchanged.** On Trino they still go through `TrinoMongoConnector`; on other engines, through the native loader.

**A pipeline with no arguments.** Its cache key is constant, so it behaves as a view with a TTL. It is replicable: replica routing's `_data_columns` keeps every column. The replica source for a MongoDB table (`make_mongodb_loader()._replica_source`) branches on `query_template` and reads through `aggregate_row_batches`, never `find`. A pipeline table with arguments is a function of its arguments and is not replicated (`replica_routing.py:211`, unchanged).

**Atlas connections.** `MongoConnection` connects by host and port with `directConnection=True`. That reaches `mongodb-atlas-local` (single node) and self-hosted servers. A real Atlas cluster needs a `mongodb+srv://` URI with TLS: under REQ-1985 the source's host may hold a `mongodb://` or `mongodb+srv://` connection string, which every reach uses.

## 5. Output columns: discovery and validation

**Declared.** The registering user writes the columns: names and IR types, as for any table.

**Discovered.** A preview runs the pipeline once with sample argument values and returns up to five documents plus inferred columns. It is the admin GraphQL query `mongoPipelinePreview(sourceId, database, nativeQuery, args)`, the same contract as `neo4jPreview`, with failures returned as `error`.
- Inference uses `mongodb/source.py:discover_schema`.
  - Nested objects are flattened to `a_b` columns with `path` set.
  - Arrays map to `json`. The IR has no array column type; a `float[]` output such as an embedding is served as `json`.
- The form refuses to submit before a preview has typed the columns, as for neo4j. The sample arguments are not stored.
- Columns are stored on the table like any other. Later runs never re-infer them.

**Validation on the way out.** Each document is projected to the declared columns, reading `path` where it is set.
- A missing field is null.
- A value that does not convert to the declared type fails the query, naming the table, the column and the BSON type it found. Rows are never partly served.
- Extra fields are ignored.

This is the same contract as `Function.output_columns` (REQ-1159). `ObjectId` is read as its hex string (`fetch._coerce`); dates as timestamps.

## 6. Surfaces and governance

The table is a registered table, so governance needs no new code.
- Column `visible_to`, masks, row rules and domain all apply to the landed rows in the engine.
- Parameter columns are not data. They publish nothing, carry no grant and are never served as output (`rights.py:333`, `schema_gen.py:163`).
- The pipeline reads whatever the source's credential can reach, including `$lookup` or `$unionWith` into other collections. What a role sees is governed at the table's output, as for a neo4j Cypher table.

**Surfaces:**
- GraphQL root field with arguments.
- SQL over pgwire, Flight SQL and JDBC (the `nf_args` path is shared).
- Cypher: a label over the table, with `_nf_` conditions kept in WHERE (`cypher/translator_helpers.py:420`).
- REST, JSON:API and gRPC: these are generated from the same schema. I have not checked that each one passes parameter-column arguments. The test plan covers each.

**Profiler.** It refuses a table that has a required argument. `profiler/source.py:180` extends its `path_param` check to required `query_arg`s. A table with optional arguments only is profiled with every argument null.

**Writes.** A pipeline table is read-only: registration refuses a pipeline containing `$out` or `$merge` anywhere, nested pipelines included.

## 7. Shape B (after A, outline only)

A `Function` with:
- `kind = "query"` and a new `impl_kind = "source_query"`;
- `binding = {"inputCollection", "pipeline"}`;
- `arguments` as `FunctionArgument`s, typed with the same table and bound by the same binder.

With no `output_columns`, it returns a list of JSON documents, so its GraphQL return is a list of the JSON scalar and it has no SQL surface. With `output_columns`, it validates as in section 5. Command assignment and domain rules are the existing ones (`security/mutation_authz.admit_command`). It has its own requirement when it starts.

## 8. Test plan

**Unit tests** (`tests/unit`, no container):
- Binder: placeholders are replaced as values. A string holding operator syntax stays a string. A placeholder inside a longer string, an undeclared placeholder and an unused argument are refused at registration.
- Conversion: every row of the table in section 3, including Decimal128, Binary UUID, Extended JSON `$oid`, and `array(double)` with and without `embedding_model` (dimension mismatch refused).
- Required arguments: a missing one is refused for a lone table, and its branch is dropped in a UNION. GraphQL NonNull follows `native_filter_required`.
- `extract_nf_args` with `ARRAY[...]`.
- Output validation: missing field → null; wrong type → error naming the column; `path` projection.
- Config and registration: `query_template` accepted on mongodb, still required on neo4j and sparql, and the incompatible settings in section 2 refused.
- Materialization branch: cache key per argument set; `would_materialize_optimize` agrees with it.

**Integration and e2e** (CI only, `heavy.sh` lane; never run locally):
- An isolated compose project on unique, unexposed ports with `mongodb/mongodb-atlas-local`.
- The test creates a search index and a vector search index, seeds `movies` (text and a small embedding dimension) and `reviews` in Postgres, and waits for the indexes to be queryable.
- Per engine (DuckDB and Trino):
  - a `$search` table and a `$searchMeta` table;
  - a `$vectorSearch` table joined in SQL to `reviews`;
  - the same tables over GraphQL with field arguments;
  - a missing required argument (refused);
  - a malicious string argument (no effect);
  - a column mask and a row rule applied to pipeline output;
  - a no-argument pipeline served as a view.
- Surfaces: one call each over REST, JSON:API, gRPC, Flight SQL and Cypher.
- UI: the Register Table form on a MongoDB source with a pipeline, and a preview with sample arguments.

## 9. Decisions (maintainer, 2026-10-12)

1. **Where the input collection is stored:** inside `query_template` (`inputCollection`).
2. **`$out` and `$merge`:** refused at registration.
3. **Atlas SRV and TLS connections:** built first, as REQ-1985. The source's host holds the connection string; there is no new field.
4. **Arguments fed by a join:** left out of Shape A.
5. **`native_filter_required` for every source kind:** enforced by one shared check, under the wider parameter model of REQ-1986 (filed as #239). A table's parameters are required by the source or by a steward; pipeline arguments are parameters like any other, so the argument model of section 2 is REQ-1986's: `query_arg` columns, optional unless a steward requires them.
