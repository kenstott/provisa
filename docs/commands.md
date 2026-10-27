# Commands

A command is a registered, governed function that brings outside computation under Provisa's governance, audit and lineage. The federation engine handles SQL natively. A command covers the computation SQL can't express, such as an enrichment microservice, a Python model, a shell script or a native database stored procedure. You register it once, and every client surface (GraphQL, pgwire SQL, REST, Arrow Flight, gRPC, Bolt/Cypher) can call it under identical governance.

Commands are one of the ways to write business logic in Provisa. See [Business Logic in Provisa](business-logic.md).

A command is a **governed RPC**. Its inputs and outputs are declared, typed, validated, traced and wired into lineage. An ad-hoc ETL step, an ungoverned curl call or a subprocess has none of that.

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

`script` and `python` are host code: they run on the server's own host, and a deployment allows
them only when its operator turns host code on (see [Host code](#host-code)).

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

An argument may declare a `default`. A call on any surface may leave that argument out, and it
takes the default; an argument without one is required, and a call missing it is refused naming
it. The default is checked against the argument's type when the command is saved.

```yaml
    arguments:
      - name: config
        type: json
        default: {}
```

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

Contract column types use the canonical IR type names:

`smallint` `integer` `bigint` `text` `boolean` `float` `double` `numeric`
`date` `timestamp` `time` `uuid` `bytea` `json`

Common aliases resolve automatically (`varchar` → `text`, `int4` → `integer`, `jsonb` → `json`,
etc.).

`return_schema` is the **GraphQL projection** of `output_columns`. The source of truth is `output_columns`, so declare it for validation and lineage, and add `return_schema` for GraphQL type generation.

## Authoring a command

### Config file

This example is a `python` command, so it loads only where host code is allowed (see
[Host code](#host-code)).

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

Commands can appear **inside** a larger SQL statement, where you join, subquery or project them; `SELECT * FROM fn(args)` is only the simplest form. A remote source's write operation is the exception, because it's called on its own (see [Why it cannot be composed](#why-it-cannot-be-composed)).

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

Everything else follows from the operation, whatever the form sends. Provisa overwrites these fields:

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

A write operation is an action. It doesn't transform data, and a view or materialized view that held one would perform the write each time it was read or refreshed. So the call stands alone. `SELECT * FROM create_issue(...)` on its own runs it, and the same call inside a larger statement is refused wherever it sits, whether in a join, a subquery or a projection.

A view or materialized view whose definition calls one is refused when it is saved. A definition that does not parse is refused too while any write operation is registered, since it cannot be shown to call none.

```text
command 'create_issue' writes to its source and is called on its own: it cannot be composed in a query, a view or a materialized view
```

A source operation isn't a lineage node either. Lineage is read from the SQL of views and queries, where a command appears as a node, and no saved definition can call a source operation.

## Commands and lineage

Because every command declares its input and output columns, column-level lineage **closes across
the opaque command boundary**. The lineage engine applies a taint closure: each declared output
column derives from every declared input column.

**The actionable consequence:** the width of your input contract sets the precision of that closure. A narrow input, holding only the columns the command needs, produces a tight, readable lineage cone. Declaring every column of the source relation fans in widely across every output. That's still sound, since no lineage is lost, but it blurs traceability.

**Rule of thumb:** pass the minimum projection the command needs, and return only derived columns. Inputs echoed through unchanged widen the taint cone, so leave them out.

See [Lineage](lineage.md) for how command nodes appear in the DAG and how to read them.

## Egress allowlist

`http` and `grpc` commands call external endpoints. Every target host must appear on the deployment's `udf_egress_allowlist`. Loopback (`localhost`, `127.0.0.1`, `::1`) is always permitted. Without an allowlist, all external egress is denied with HTTP 403, and there's no silent default.

## Host code

A `python` command calls a `module:attr` inside the server's process, and a `script` command runs a
subprocess on the server's host. Either one runs with everything the server can reach: its memory,
its credentials, its files and its network. Code that is malicious, buggy or greedy can read any of
it, starve the server or take it down. The command's model entry holds only a reference to the code
(`module:attr`, or the script's path), so the code itself doesn't travel with the model when the
model is promoted to another environment.

The deployment setting `commands.allow_host_code` governs both kinds. It is off by default. While
it is off:

- registering a command of either kind is refused, whether from the admin API or from a config file;
- updating a command to either kind is refused;
- calling an existing command of either kind is refused, with HTTP 403 and the code
  `functions.host_code_refused`.

The setting belongs to the operator, never to an organization. It is on the Deployment settings page
under Security, and can also be set in the config file or the environment:

```yaml
server:
  commands:
    allow_host_code: true
```

```bash
PROVISA_COMMANDS_ALLOW_HOST_CODE=true
```

[Code sources](code-sources.md) are the governed alternative: their code runs in a sandbox, is
pinned to a version, and is promoted with the model.

## Invocation tracing

Every invocation emits a trace, whatever the outcome. The trace holds the command name, transport kind, identity model (DEFINER or INVOKER), input relation references, role id and output cardinality. The dispatcher emits it, so no `impl_kind` can skip it.

## CLI: provisa metadata export

`provisa metadata export` is a shell-tier job and runs outside the governed RPC path. It triggers the running server's on-demand metadata publish by posting to `/admin/metadata-export/publish`, the endpoint the Admin tab's **Publish now** button calls.

Use it to drive timed exports from cron or CI when the configured `reconcile_cron` schedule is
not granular enough:

```bash
provisa metadata export --api https://acme.provisa.org --token "$PROVISA_API_TOKEN"
```

Exit 0 = full publish. Exit 1 = partial publish or connection failure.

For the full flag reference, auth options, multitenancy host naming, and a cron example, see
[Metadata Export — From the command line](metadata-export.md#from-the-command-line).

Commands appear in each environment's git projection. See [Environments](environments.md) for how a command and its tag assignments survive merge and pull.

## Creating command bindings for a library you already use

### What a command binding is

A command binding is the name a command or function uses in a specific language. Every command gets a generated default binding in each surface Provisa exposes: SQL, Cypher, GraphQL, REST, MCP, SPARQL and the others. Rename a binding in one language, or exclude the command from a language entirely, and you are making an explicit declaration. Omit a language from the declaration, and the command appears there under its generated name.

A SPARQL binding is accepted and checked like the others, but the SPARQL endpoint does not call commands yet, so it has no effect there.

SQL is the one language you cannot exclude. Every other surface compiles to SQL, so a command targeted only to Cypher still has a SQL binding.

A variant that returns a different result shape is its own command. When APOC's `apoc.text.join` returns a plain string but your general `text_join` command returns a row set, those are two commands with two implementations. Producing results in the shape the caller expects is the command developer's responsibility. A command binding is a name. Arguments, governance and grants are declared once on the command, and every binding shares them.

One term to keep separate: a term binding attaches an ontology term to a column, visible on the column's **Bound columns** tab. That is a different mechanism. Always write "command binding" in full.

### How much work is it?

That depends on where the library runs.

| Library | Command kind | Effort |
| --- | --- | --- |
| A pure SQL scalar expression | SQL expression template | Minutes |
| A Python library | `python` command | Small |
| Anything callable from the command line | `script` command | Small |
| An existing service in another language | `http` or `grpc` command | Moderate — you run the service |
| A plugin tied to another runtime (e.g., a Neo4j extension in Java) | Register that system as a Provisa source and query it in place | Depends |

One worked example per row.

**SQL expression template.** APOC's `apoc.text.join(list, sep)` joins a list to a string. The SQL equivalent is `array_to_string`. Declare it as a function with a SQL expression template, give the Cypher command binding its APOC name, and let the transpiler handle the rest:

```yaml
functions:
  - name: text_join
    kind: query
    impl_kind: sql_expression
    binding:
      sql: "array_to_string({list}, {sep})"
    arguments:
      - name: list
        type: String
      - name: sep
        type: String
    language_bindings:
      cypher:
        name: apoc.text.join
```

The transpiler rewrites `{list}` and `{sep}` to each engine's dialect when the function is saved. A broken template fails at save, not at call time.

**Python library.** Suppose you want `langdetect` accessible as a governed function. Point a `python` command at a callable in your codebase:

```yaml
functions:
  - name: detect_language
    kind: query
    impl_kind: python
    binding:
      callable: myapp.text_functions:detect_language
    arguments:
      - name: text
        type: String
    output_columns:
      - {name: language, type: text}
      - {name: confidence, type: double}
```

`python` commands run in-process on the server host. The operator must turn host code on first (see [Host code](#host-code)).

**Script command.** `pdfinfo` is a CLI tool. A `script` command wraps it:

```yaml
functions:
  - name: pdf_info
    kind: query
    impl_kind: script
    binding:
      argv: ["pdfinfo", "/mnt/files/{file_path}"]
    arguments:
      - name: file_path
        type: String
    output_columns:
      - {name: pages, type: integer}
      - {name: title, type: text}
```

Like `python`, `script` commands require the operator's `allow_host_code` setting.

**HTTP or gRPC service.** A sentiment scoring model sits behind an HTTP endpoint. Register it as an `http` command and add its host to the egress allowlist:

```yaml
functions:
  - name: score_sentiment
    kind: query
    impl_kind: http
    binding:
      url: https://ml.internal/sentiment
      method: POST
    arguments:
      - name: reviews
        type: String
        arg_kind: result_set
        columns:
          - {name: id, type: integer}
          - {name: body, type: text}
    output_columns:
      - {name: id, type: integer}
      - {name: sentiment, type: text}
      - {name: score, type: double}
```

**Another runtime.** When a procedure is embedded in another system — for example, a graph traversal written as a Neo4j extension in Java — the straightforward path is to register that system as a Provisa source and query it in place. Provisa federates the results with the rest of your model without copying data.

### Governance

Every command runs under its own grants. The caller must hold rights to both the domain and the command. A function declared as a SQL expression template is different: it runs inside the governed query rather than as a separate call, carrying exactly the same row-level security as the rest of the statement. Either way, a third-party library calling an emulated function never reads rows the caller's role cannot see.

### The APOC set Provisa ships

Cypher tools and agent frameworks call APOC before they write Cypher. LangChain's Neo4j integration, for example, reads the graph's schema with `apoc.meta.data`. Provisa ships the APOC calls these tools make as command declarations on this same mechanism, in `provisa/core/apoc_commands.yaml`. Each is bound in Cypher to its APOC name, offered in SQL under its own name, and excluded from GraphQL, REST, MCP and SPARQL.

| Kind | APOC names | How it runs |
| --- | --- | --- |
| Introspection | `apoc.meta.data`, `apoc.meta.schema`, `apoc.schema.nodes` | A `builtin` command, answered from the acting role's label map |
| Functions | `apoc.text.join`, `capitalize`, `decapitalize`, `lpad`, `rpad`, `indexOf`; `apoc.coll.sum`, `max`, `min`, `contains`, `toSet`, `union`, `flatten`; `apoc.map.fromLists`; `apoc.date.currentTimestamp`; `apoc.convert.toString`, `toInteger`, `toFloat`, `toBoolean`, `toJson` | A `sql_expression` function, pushed down with the query |
| Traversal | `apoc.path.expand`, `apoc.path.subgraphNodes` | Compiled to the variable-length pattern they walk, the same recursive SQL |

A `builtin` command runs code that ships with Provisa, named in its binding:

```yaml
functions:
  - name: apoc_meta_data
    kind: query
    impl_kind: builtin
    binding:
      callable: apoc.meta.data
    arguments:
      - name: config
        type: json
    language_bindings:
      cypher:
        name: apoc.meta.data
      graphql:
        exclude: true
      rest:
        exclude: true
      mcp:
        exclude: true
      sparql:
        exclude: true
```

An argument may declare a default, so a call can leave it out: `apoc.meta.data()` with no argument takes its default configuration, as older LangChain versions call it.

A command called inside a larger Cypher statement names the columns it yields, and the rest of the statement reads them. This is the statement LangChain sends:

```cypher
CALL apoc.meta.data({sample: $SAMPLE})
YIELD label, other, elementType, type, property
WHERE NOT type = 'RELATIONSHIP' AND elementType = 'node'
  AND NOT label IN $EXCLUDED_LABELS
WITH label AS nodeLabel, collect({property: property, type: type}) AS properties
RETURN {label: nodeLabel, properties: properties} AS output
```

The governance model does the filtering. A role that can read `Employees` but not `Payroll` sees `Employees` and its relationships in the result. `Payroll` is absent, and any Cypher LangChain generates next cannot reference it. Counts in the answer are null: a schema read never scans the data.

The path procedures take a start node the statement has matched with a label, a label the walk ends at and a bounded depth. A relationship type is walked out (`TYPE>`), in (`<TYPE`), or either way when the walk ends at the start's own label:

```cypher
MATCH (p:Person {name: 'Ada'})
CALL apoc.path.subgraphNodes(p, {relationshipFilter: 'KNOWS>', labelFilter: '+Person', maxLevel: 3})
YIELD node
RETURN DISTINCT node.name
```

A namespaced function no function is bound to, such as `apoc.util.sleep`, is refused by name. The procedures that act outside the model, such as `apoc.create.*`, `apoc.periodic.iterate` and `apoc.load.*`, are listed on the Cypher Conformance page with their workarounds.
