# Commands

A command is a registered, governed function that brings external computation under Provisa's
governance, audit, and lineage system. Where the federation engine handles SQL natively, a command
is the seam for computation it cannot express: an enrichment microservice, a Python model, a shell
script, a native database stored procedure. Register it once; every client surface — GraphQL,
pgwire SQL, REST, Arrow Flight, gRPC, Bolt/Cypher — can invoke it with identical governance
.

The key distinction: a command is a **governed RPC**, not ad-hoc ETL. Its inputs and outputs are
declared, typed, validated, traced, and wired into lineage. An ungoverned curl call or subprocess
is none of those things.

## Implementation kinds

Six `impl_kind` values are supported:

| `impl_kind` | Transport |
| --- | --- |
| `source_procedure` | Native stored procedure on a registered source |
| `source_operation` | A write operation of an OpenAPI, remote GraphQL or gRPC source, passed through as is (see [A remote source's write operation](#a-remote-sources-write-operation)) |
| `script` | Local subprocess fed JSON on stdin, reads JSON from stdout |
| `http` | HTTP/S endpoint; JSON request body, JSON response |
| `grpc` | gRPC unary; proto-less JSON bridge |
| `python` | In-process Python callable (`module:attr`) |

Addressing (the catalog `name` and `function_name`) is decoupled from `binding` (transport and
location). Swap the binding and the command's governance, lineage, and caller contracts stay
unchanged.

## Argument kinds

Each argument declares an `arg_kind`:

| `arg_kind` | Behavior |
| --- | --- |
| `column_value` | Scalar; passed directly in the request payload |
| `table_ref` | Lazy; Provisa passes the relation reference as-is; the service fetches the data |
| `result_set` | Eager; Provisa materializes the referenced relation and sends its rows |

`http` and `grpc` commands **must** declare at least one `table_ref` or `result_set` argument.
An external command receiving only scalar arguments would be invoked once per row, which defeats
batching. The dispatcher rejects this configuration at call time (422).

A command that returns a set (declared via `output_columns` and `return_schema`) is a
table-valued function. Use it in a `FROM` clause or a `JOIN`.

## The dataset contract

Each `table_ref` or `result_set` argument may declare an **input column contract**: an ordered,
IR-typed list of columns in `FunctionArgument.columns`. The command itself declares an
**output column contract** in `Function.output_columns`.

Both contracts are validated fail-loud at every invocation:

- **Input (result_set only):** after materialization, Provisa validates the rows against the
  declared columns. Extra fields, missing fields, and wrong types all raise HTTP 422.

- **Output:** rows returned by the command are validated against `output_columns` before they
  reach the caller.
- **Narrow projection:** when an input contract is declared, the materialization query projects
  **only those columns** (`SELECT "id", "region" FROM ...`) rather than `SELECT *`.

### The IR type vocabulary

Contract column types use the canonical IR type system, not GraphQL scalars or
source-native spellings. The valid names are:

`smallint` `integer` `bigint` `text` `boolean` `float` `double` `numeric`
`date` `timestamp` `time` `uuid` `bytea` `json`

Common aliases resolve automatically (`varchar` → `text`, `int4` → `integer`, `jsonb` → `json`,
etc.).

`return_schema` is the **GraphQL projection** of `output_columns`, not the source of truth.
Declare `output_columns` for validation and lineage; add `return_schema` for GraphQL type
generation.

## Authoring a command

### Config file

```yaml
functions:
  - name: enrich_orders
    description: Enrich orders inline — deterministic score + region label
    domain_id: sales-analytics
    kind: query
    impl_kind: python
    source_id: ""
    function_name: enrich_orders
    returns: ""
    binding:
      callable: demo.py_functions:enrich_orders
    arguments:
      - name: input
        type: String
        arg_kind: result_set
        columns:
          - {name: id, type: integer}   # narrow input contract
          - {name: region, type: text}
    visible_to: [admin]
    output_columns:
      - {name: id, type: integer}
      - {name: score, type: double}
      - {name: region_label, type: text}
    return_schema:
      type: array
      items:
        type: object
        properties:
          id: {type: integer}
          score: {type: number}
          region_label: {type: string}
```

The gRPC variant (`enrich_grpc_set`) follows the same pattern but specifies `impl_kind: grpc`
and a `binding` with `target` and `method` keys instead of `callable`:

```yaml
  - name: enrich_grpc_set
    impl_kind: grpc
    binding:
      target: ${env:DEMO_GRPC_TARGET:-localhost:50071}
      method: /provisa.demo.Enrich/EnrichRows
    arguments:
      - name: input
        type: String
        arg_kind: result_set
        columns:
          - {name: id, type: integer}
          - {name: region, type: text}
    output_columns:
      - {name: id, type: integer}
      - {name: embedding, type: text}
      - {name: geo, type: text}
```

### Admin UI

The command form in **Settings → Commands** includes a per-dataset input-columns editor (one row
per declared column, with an IR type selector) and an output-columns editor. Save the form to
register or update the command without a config reload.

## Inline composition

Commands may appear **inside** a larger SQL statement — joined, sub-queried, or projected. You
are not limited to `SELECT * FROM fn(args)`. The exception is a remote source's write operation, which is called on its own (see [Why it cannot be composed](#why-it-cannot-be-composed)).

```sql
-- Enrich the orders relation and join the result back inline.
SELECT o.id, o.amount, e.score, e.region_label
FROM   orders o
JOIN   enrich_orders('main.public.orders') e ON o.id = e.id
WHERE  e.score > 0.8;
```

Before governance, validation, or routing runs, the pipeline detects registered command calls,
executes each through the shared governed executor (so the I/O contract and identity model apply
exactly as for a direct call), and rewrites the call site to a typed local relation.

Substitution is size-adaptive: up to 1,000 rows the result inlines as a typed `VALUES` list;
above that threshold it registers as a named local relation in the engine.

A localized statement routes normally. Single-source queries stay on the source; only genuinely
cross-source queries go to the federation engine.

## A remote source's write operation

An OpenAPI, remote GraphQL or gRPC source offers write operations. Registering one as a command makes it callable, governed, and audited from every surface. Adding the source registers none of them; you register the ones you want, one at a time, the way you register tables. The registration is the curation.

What a source offers:

| Source type | Operations offered | Operation name |
| --- | --- | --- |
| `openapi` | Every non-GET operation in the spec | The `operationId` |
| `graphql_remote` | Every field of the remote `Mutation` type | The field name |
| `grpc_remote` | Every method classified as a mutation | `Service.Method` |

### Register one

1. Open **Model → Commands** and add a command.
2. Pick the remote source. The form switches to an operation picker, listing what the source offers.
3. Pick the operation, a domain, and the roles that may call it.
4. Optionally switch on **Requires approval** and pick the **Writes table**.

Through the admin GraphQL API, `availableFunctions(sourceId, schemaName)` lists the operations. The schema name is `openapi`, `graphql` or `grpc_remote`, by source type.

Everything else follows from the operation, not from what the form sends. `_as_source_operation` overwrites these fields:

```python
body.implKind = "source_operation"
body.kind = "mutation"
body.schemaName = OPERATION_SCHEMA[source_type]
body.returns = ""
body.binding = {}
body.materialize = False
body.arguments = [{"name": a, "type": "json"} for a in operation.arguments]
```

Every argument is typed `json`. The operation's arguments are its OpenAPI path parameters (plus `body` when the operation takes a request body), its GraphQL mutation arguments, or its gRPC request fields. An operation the source does not offer is refused with 422, `functions.operation_not_offered`.

### Calling it

Provisa does not shape, type or check the input. Each argument goes to the remote unchanged, with the source's credential, and the remote's answer comes back unchanged. Provisa governs who may call, in which domain, whether approval is needed, and records the call.

How the remote receives the arguments:

- **OpenAPI.** Path parameters fill the path template. `body` is the JSON request body. Every other argument goes on the query string.
- **GraphQL.** Arguments are sent as typed variables, each declared with the type the remote schema gives it. The mutation asks back the answer's scalar and enum fields, and those of objects inside it down to two levels.
- **gRPC.** The arguments become the request message of the `Service.Method`.

The answer is the command's rows: an object is one row, a list of objects is its rows, anything else is one row `{"result": ...}`.

On GraphQL the command is a mutation field and its answer is the JSON scalar.

```graphql
mutation {
  createIssue(input: {repositoryId: "R_kgDO...", title: "Crash on save"})
}
```

On the SQL surfaces (pgwire and the others that pass SQL) write each argument as a JSON literal in a string. `'{"title": "x"}'` is an object, `'"text"'` a string, `'3'` a number. A literal that is not valid JSON fails with 422, `functions.json_argument_invalid`.

```sql
SELECT * FROM create_issue('{"repositoryId": "R_kgDO...", "title": "Crash on save"}');
```

On REST, POST a JSON object of arguments to `/data/rest/{domain}/commands/{command}`. A `json` argument is documented in the generated spec as any value.

```bash
curl -X POST https://acme.provisa.org/data/rest/engineering/commands/create_issue \
  -H "Content-Type: application/json" \
  -d '{"input": {"repositoryId": "R_kgDO...", "title": "Crash on save"}}'
```

### Refusals

The remote's refusal comes back as the remote stated it.

| Remote | Provisa answers |
| --- | --- |
| Refuses the call (HTTP 4xx, GraphQL `errors`, a refused gRPC call) | 422, `functions.remote_refused`, carrying `remote_status` and `answer` |
| Fails (HTTP 5xx) | 502, `functions.remote_refused` |

Whether the source's credential may perform the operation is the remote's to say, when the operation is called. Provisa cannot try a write at registration without performing it.

### Approval

Switch on **Requires approval** and each call is put to the deployment's approval hook before it runs. It runs only when the hook approves.

- No hook configured: 403, `functions.approval_unavailable`.
- Hook denies: 403, `functions.approval_denied`, with the hook's reason.

The hook receives the caller, the role, the command name and its arguments. See [ABAC Approval Hook](security.md#abac-approval-hook). The flag is stored as `Function.requires_approval`, and the check applies to any command that sets it.

### Writes table

Pick the table the operation writes in **Writes table**, as `schema.table`. It must be a registered table of the command's own source, or the save is refused with 422, `actions.written_table_not_registered`. The setting is optional.

After each call the remote accepts, Provisa treats it as a write to that table. It drops the table's cached responses, marks the materialized views over it stale, emits the change event, runs the commands that run when it changes, and reloads the table when it is held hot.

When the table is read from its whole-table replica (the operator's settings put it there, or the engine cannot read its source in place), a build of the replica is asked for after the call, with the reason `write`. Readers keep the old replica until the new one swaps in. A table replicated row by row, or one with a parameter column, has no whole replica to rebuild.

### Why it cannot be composed

A write operation is an action, not a transform of data. A view or materialized view that held one would perform the write each time it was read or refreshed. So the call stands alone: `SELECT * FROM create_issue(...)` on its own runs it, and the same call inside a larger statement is refused, wherever it sits -- in a join, a subquery or a projection.

A view or materialized view whose definition calls one is refused when it is saved. A definition that does not parse is refused too while any write operation is registered, since it cannot be shown to call none.

```text
command 'create_issue' writes to its source and is called on its own: it cannot be composed in a query, a view or a materialized view
```

A source operation is also not a lineage node: lineage is read from the SQL of views and queries, where a command appears as a node, and no saved definition can call a source operation.

## Commands and lineage

Because every command declares its input and output columns, column-level lineage **closes across
the opaque command boundary**. The lineage engine applies a taint closure: each declared output
column derives from every declared input column.

**The actionable consequence:** the width of your input contract determines the precision of that
closure. A narrow input — only the columns the command actually needs — produces a tight,
readable lineage cone. Declaring every column in the source relation fans in widely across every
output, which is still sound (no lineage is lost) but blurs traceability.

**Rule of thumb:** pass the minimum projection the command needs, and return only derived columns
(not echoed-through inputs unchanged). This keeps the taint cone accurate.

See [Lineage](lineage.md) for how command nodes appear in the DAG and how to read them.

## Egress allowlist

`http` and `grpc` commands call external endpoints. Every target host must appear on the
deployment's `udf_egress_allowlist`. Loopback (`localhost`, `127.0.0.1`, `::1`) is always
permitted. An absent allowlist denies all external egress with HTTP 403 — there is no silent
default.

## Invocation tracing

Every invocation emits a trace regardless of outcome. The trace includes the command name,
transport kind, identity model (DEFINER or INVOKER), input relation references, role id, and
output cardinality. The dispatcher emits the trace — no `impl_kind` can bypass it.

## CLI: provisa metadata export

`provisa metadata export` is a shell-tier job, not a governed RPC. It triggers the running
server's on-demand metadata publish by posting to
`/admin/metadata-export/publish` — the same endpoint the Admin tab's **Publish now** button
calls.

Use it to drive timed exports from cron or CI when the configured `reconcile_cron` schedule is
not granular enough:

```bash
provisa metadata export --api https://acme.provisa.org --token "$PROVISA_API_TOKEN"
```

Exit 0 = full publish. Exit 1 = partial publish or connection failure.

For the full flag reference, auth options, multitenancy host naming, and a cron example, see
[Metadata Export — From the command line](metadata-export.md#from-the-command-line).

Commands appear in each environment's git projection. See [Environments](environments.md) for how a command and its tag assignments survive merge and pull.
