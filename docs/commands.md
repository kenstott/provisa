# Commands

A command is a registered, governed function that brings external computation under Provisa's
governance, audit, and lineage system. Where the federation engine handles SQL natively, a command
is the seam for computation it cannot express: an enrichment microservice, a Python model, a shell
script, a native database stored procedure. Register it once; every client surface — GraphQL,
pgwire SQL, REST, Arrow Flight, gRPC, Bolt/Cypher — can invoke it with identical governance
(REQ-885, REQ-1156). [tool-verified: function_dispatch.py module docstring + REQ-885 in requirements.md]

The key distinction: a command is a **governed RPC**, not ad-hoc ETL. Its inputs and outputs are
declared, typed, validated, traced, and wired into lineage. An ungoverned curl call or subprocess
is none of those things.

## Implementation kinds

Six `impl_kind` values are supported [tool-verified: `_EXECUTORS` dict in `provisa/executor/function_dispatch.py`]:

| `impl_kind` | Transport |
| --- | --- |
| `source_procedure` | Native stored procedure on a registered source |
| `source_operation` | A write operation of an OpenAPI, remote GraphQL or gRPC source, passed through as is (see [A remote source's write operation](#a-remote-sources-write-operation-req-1924)) |
| `script` | Local subprocess fed JSON on stdin, reads JSON from stdout |
| `http` | HTTP/S endpoint; JSON request body, JSON response |
| `grpc` | gRPC unary; proto-less JSON bridge |
| `python` | In-process Python callable (`module:attr`) |

Addressing (the catalog `name` and `function_name`) is decoupled from `binding` (transport and
location). Swap the binding and the command's governance, lineage, and caller contracts stay
unchanged. [tool-verified: Function model in models.py:710-750]

## Argument kinds

Each argument declares an `arg_kind` [tool-verified: FunctionArgument.arg_kind in models.py:691-700]:

| `arg_kind` | Behavior |
| --- | --- |
| `column_value` | Scalar; passed directly in the request payload |
| `table_ref` | Lazy; Provisa passes the relation reference as-is; the service fetches the data |
| `result_set` | Eager; Provisa materializes the referenced relation and sends its rows |

`http` and `grpc` commands **must** declare at least one `table_ref` or `result_set` argument.
An external command receiving only scalar arguments would be invoked once per row, which defeats
batching. The dispatcher rejects this configuration at call time (422). [tool-verified:
`_reject_rowwise_external` in function_dispatch.py:322-344]

A command that returns a set (declared via `output_columns` and `return_schema`) is a
table-valued function. Use it in a `FROM` clause or a `JOIN`. [inferred from models.py:744-748
and command_localize.py:52-63]

## The dataset contract (REQ-1159)

Each `table_ref` or `result_set` argument may declare an **input column contract**: an ordered,
IR-typed list of columns in `FunctionArgument.columns`. The command itself declares an
**output column contract** in `Function.output_columns`. [tool-verified: DatasetColumn model in
models.py:675-683, Function.output_columns in models.py:748]

Both contracts are validated fail-loud at every invocation:

- **Input (result_set only):** after materialization, Provisa validates the rows against the
  declared columns. Extra fields, missing fields, and wrong types all raise HTTP 422.
  [tool-verified: `_validate_against` called in `_prepare_args` at function_dispatch.py:243-248]
- **Output:** rows returned by the command are validated against `output_columns` before they
  reach the caller. [tool-verified: function_dispatch.py:488-490]
- **Narrow projection:** when an input contract is declared, the materialization query projects
  **only those columns** (`SELECT "id", "region" FROM ...`) rather than `SELECT *`.
  [tool-verified: `_materialize_relation` at function_dispatch.py:155-177, col_names passed
  to projection at line 171]

### The IR type vocabulary

Contract column types use the canonical IR type system (REQ-846), not GraphQL scalars or
source-native spellings. The valid names are [tool-verified: `_IR_TO_SA` keys in ir_types.py:45-63]:

`smallint` `integer` `bigint` `text` `boolean` `float` `double` `numeric`
`date` `timestamp` `time` `uuid` `bytea` `json`

Common aliases resolve automatically (`varchar` → `text`, `int4` → `integer`, `jsonb` → `json`,
etc.). [tool-verified: `_ALIASES` dict in ir_types.py:67-90]

`return_schema` is the **GraphQL projection** of `output_columns`, not the source of truth.
Declare `output_columns` for validation and lineage; add `return_schema` for GraphQL type
generation. [tool-verified: models.py:744-748, comment "return_schema is its GraphQL projection"]

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

[tool-verified: sample_config.yaml enrich_orders block]

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

[tool-verified: config/provisa.yaml enrich_grpc_set block]

### Admin UI

The command form in **Settings → Commands** includes a per-dataset input-columns editor (one row
per declared column, with an IR type selector) and an output-columns editor. Save the form to
register or update the command without a config reload. [inferred from CommandFormFields.tsx]

## Inline composition (REQ-1159)

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
[tool-verified: `_localize_inline_commands` in _pipeline.py:145-163 and localize_commands in
command_localize.py:178-222]

Substitution is size-adaptive: up to 1,000 rows the result inlines as a typed `VALUES` list;
above that threshold it registers as a named local relation in the engine.
[tool-verified: `_DEFAULT_VALUES_MAX_ROWS = 1000` in command_localize.py:49, path at lines 211-216]

A localized statement routes normally. Single-source queries stay on the source; only genuinely
cross-source queries go to the federation engine. [tool-verified: _pipeline.py:304 comment
"REQ-1159: a localized statement carries an inline local relation..."]

## A remote source's write operation (REQ-1924)

An OpenAPI, remote GraphQL or gRPC source offers write operations. Registering one as a command makes it callable, governed, and audited from every surface. Adding the source registers none of them; you register the ones you want, one at a time, the way you register tables. The registration is the curation. [tool-verified: `provisa/executor/source_operation.py` module docstring; `provisa/api/admin/schema_common.py` `remote_source_counts`, `"mutations": 0`]

What a source offers [tool-verified: `provisa/executor/source_operation.py` `offered_operations`]:

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

[tool-verified: `provisa-ui/src/components/navGroups.ts` (`/commands` in the Model group); `provisa-ui/src/pages/commands/CommandFormFields.tsx` `isSourceOperation`, `command-requires-approval-switch`, `writesTable`; `provisa/api/admin/schema_query.py` `available_functions` ("Listing them registers none")]

Through the admin GraphQL API, `availableFunctions(sourceId, schemaName)` lists the operations. The schema name is `openapi`, `graphql` or `grpc_remote`, by source type. [tool-verified: `OPERATION_SCHEMA` in `source_operation.py`; `available_functions` returns `[]` when the schema name does not match the source type]

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

[tool-verified: `provisa/api/admin/actions_router.py` `_as_source_operation`]

Every argument is typed `json`. The operation's arguments are its OpenAPI path parameters (plus `body` when the operation takes a request body), its GraphQL mutation arguments, or its gRPC request fields. [tool-verified: `_openapi_operations`, `_graphql_operations`, `_grpc_operations` in `source_operation.py`] An operation the source does not offer is refused with 422, `functions.operation_not_offered`. [tool-verified: `offered_operation`]

### Calling it

Provisa does not shape, type or check the input. Each argument goes to the remote unchanged, with the source's credential, and the remote's answer comes back unchanged. Provisa governs who may call, in which domain, whether approval is needed, and records the call. [tool-verified: `source_operation.py` module docstring]

How the remote receives the arguments:

- **OpenAPI.** Path parameters fill the path template. `body` is the JSON request body. Every other argument goes on the query string. [tool-verified: `_call_openapi`]
- **GraphQL.** Arguments are sent as typed variables, each declared with the type the remote schema gives it. The mutation asks back the answer's scalar and enum fields, and those of objects inside it down to two levels. [tool-verified: `mutation_document`, `_selection`, `_ANSWER_DEPTH = 2`]
- **gRPC.** The arguments become the request message of the `Service.Method`. [tool-verified: `_call_grpc`]

The answer is the command's rows: an object is one row, a list of objects is its rows, anything else is one row `{"result": ...}`. [tool-verified: `_rows`]

On GraphQL the command is a mutation field and its answer is the JSON scalar. [tool-verified: `provisa/compiler/actions_schema.py` (`gql_return = JSONScalar` for `source_operation`; `kind` defaults to `"mutation"`)]

```graphql
mutation {
  createIssue(input: {repositoryId: "R_kgDO...", title: "Crash on save"})
}
```

[inferred: argument names are those of the remote operation; the example call was not run]

On the SQL surfaces (pgwire and the others that pass SQL) write each argument as a JSON literal in a string. `'{"title": "x"}'` is an object, `'"text"'` a string, `'3'` a number. A literal that is not valid JSON fails with 422, `functions.json_argument_invalid`. [tool-verified: `_json_arguments_from_sql` in `function_dispatch.py`]

```sql
SELECT * FROM create_issue('{"repositoryId": "R_kgDO...", "title": "Crash on save"}');
```

[inferred: the first-argument shape follows `_json_arguments_from_sql`; the statement was not run, and the command's argument list is the operation's]

On REST, POST a JSON object of arguments to `/data/rest/{domain}/commands/{command}`. A `json` argument is documented in the generated spec as any value. [tool-verified: `provisa/api/rest/openapi_spec.py` `cmd_path = f"/{cmd_domain}/commands/{cmd_name}"`, `_arg_type_to_openapi` (`"json"` returns `{}`)]

```bash
curl -X POST https://acme.provisa.org/data/rest/engineering/commands/create_issue \
  -H "Content-Type: application/json" \
  -d '{"input": {"repositoryId": "R_kgDO...", "title": "Crash on save"}}'
```

[inferred: host, domain and command name are placeholders; not run]

### Refusals

The remote's refusal comes back as the remote stated it. [tool-verified: `_refused` in `source_operation.py`]

| Remote | Provisa answers |
| --- | --- |
| Refuses the call (HTTP 4xx, GraphQL `errors`, a refused gRPC call) | 422, `functions.remote_refused`, carrying `remote_status` and `answer` |
| Fails (HTTP 5xx) | 502, `functions.remote_refused` |

Whether the source's credential may perform the operation is the remote's to say, when the operation is called. Provisa cannot try a write at registration without performing it. [tool-verified: REQ-1924 CREDENTIAL AT CALL amendment in `docs/arch/requirements.yaml`; no credential check in `_as_source_operation`]

### Approval

Switch on **Requires approval** and each call is put to the deployment's approval hook before it runs. It runs only when the hook approves. [tool-verified: `provisa/api/data/action_exec.py` `_require_approval`]

- No hook configured: 403, `functions.approval_unavailable`.
- Hook denies: 403, `functions.approval_denied`, with the hook's reason.

The hook receives the caller, the role, the command name and its arguments. See [ABAC Approval Hook](security.md#abac-approval-hook). The flag is stored as `Function.requires_approval`, and the check applies to any command that sets it. [tool-verified: `action_exec.py` `if fn.get("requires_approval")`]

### Writes table

Pick the table the operation writes in **Writes table**, as `schema.table`. It must be a registered table of the command's own source, or the save is refused with 422, `actions.written_table_not_registered`. The setting is optional. [tool-verified: `_check_written_table` in `actions_router.py`; `written_table` in `source_operation.py`]

After each call the remote accepts, Provisa treats it as a write to that table. It drops the table's cached responses, marks the materialized views over it stale, emits the change event, runs the table's sinks, and reloads the table when it is held hot. [tool-verified: `provisa/api/data/table_written.py` `after_table_written`]

Replicas are not refreshed by the call. That waits on a way to ask for a replica refresh, which is not built; until then a replica refreshes on its own schedule. [tool-verified: REQ-1924 WRITTEN TABLE amendment; no replica call in `after_table_written`]

### Why it cannot be composed

A write operation is an action, not a transform of data. A view or materialized view that held one would perform the write each time it was read or refreshed. So the call stands alone: `SELECT * FROM create_issue(...)` on its own runs it, and the same call inside a larger statement is refused, wherever it sits -- in a join, a subquery or a projection. [tool-verified: `provisa/pgwire/_pipeline.py` `_refuse_composed_mutators`; `provisa/executor/source_operation.py` `writes_called_in`]

A view or materialized view whose definition calls one is refused when it is saved. A definition that does not parse is refused too while any write operation is registered, since it cannot be shown to call none. [tool-verified: `refuse_writes_in_definition` in `source_operation.py`, called from `provisa/api/admin/_table_ops.py` `_build_columns_for_input` (views) and `provisa/api/admin/schema_common.py` (materialized views)]

```text
command 'create_issue' writes to its source and is called on its own: it cannot be composed in a query, a view or a materialized view (REQ-1924)
```

A source operation is also not a lineage node: lineage is read from the SQL of views and queries, where a command appears as a node, and no saved definition can call a source operation. [tool-verified: `provisa/lineage/graph.py` (`kind="command"` for a call in the SQL); `refuse_writes_in_definition`]

## Commands and lineage

Because every command declares its input and output columns, column-level lineage **closes across
the opaque command boundary**. The lineage engine applies a taint closure: each declared output
column derives from every declared input column. [tool-verified: `_splice_commands` in graph.py:223-242]

**The actionable consequence:** the width of your input contract determines the precision of that
closure. A narrow input — only the columns the command actually needs — produces a tight,
readable lineage cone. Declaring every column in the source relation fans in widely across every
output, which is still sound (no lineage is lost) but blurs traceability.

**Rule of thumb:** pass the minimum projection the command needs, and return only derived columns
(not echoed-through inputs unchanged). This keeps the taint cone accurate. [inferred from
_splice_commands behavior in graph.py and _materialize_relation narrow-projection in function_dispatch.py:161]

See [Lineage](lineage.md) for how command nodes appear in the DAG and how to read them.

## Egress allowlist

`http` and `grpc` commands call external endpoints. Every target host must appear on the
deployment's `udf_egress_allowlist`. Loopback (`localhost`, `127.0.0.1`, `::1`) is always
permitted. An absent allowlist denies all external egress with HTTP 403 — there is no silent
default. [tool-verified: `_check_egress` in function_dispatch.py:292-311]

## Invocation tracing (REQ-886)

Every invocation emits a trace regardless of outcome. The trace includes the command name,
transport kind, identity model (DEFINER or INVOKER), input relation references, role id, and
output cardinality. The dispatcher emits the trace — no `impl_kind` can bypass it.
[tool-verified: `udf_invocation_trace` context in dispatch_function:475-492]

## CLI: provisa metadata export

`provisa metadata export` is a shell-tier job, not a governed RPC. It triggers the running
server's on-demand metadata publish (REQ-1072/REQ-1074) by posting to
`/admin/metadata-export/publish` — the same endpoint the Admin tab's **Publish now** button
calls. [tool-verified: `_cmd_metadata_export` in provisa/cli.py:272-310]

Use it to drive timed exports from cron or CI when the configured `reconcile_cron` schedule is
not granular enough:

```bash
provisa metadata export --api https://acme.provisa.org --token "$PROVISA_API_TOKEN"
```

Exit 0 = full publish. Exit 1 = partial publish or connection failure.

For the full flag reference, auth options, multitenancy host naming, and a cron example, see
[Metadata Export — From the command line](metadata-export.md#from-the-command-line).


Commands appear in each environment's git projection. See [Environments](environments.md) for how a command and its tag assignments survive merge and pull.
