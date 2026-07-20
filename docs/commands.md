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

Five `impl_kind` values are supported [tool-verified: `_EXECUTORS` dict in function_dispatch.py:420-426]:

| `impl_kind` | Transport |
|---|---|
| `source_procedure` | Native stored procedure on a registered source |
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
|---|---|
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
are not limited to `SELECT * FROM fn(args)`.

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
