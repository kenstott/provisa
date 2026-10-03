# 命令

命令是一个已注册的、受治理的函数，它把外部计算纳入 Provisa 的治理、审计与血缘体系。联邦引擎原生处理 SQL，而命令则是它无法表达的那部分计算的接缝：一个数据增强微服务、一个 Python 模型、一段 shell 脚本、一个数据库原生的存储过程。注册一次；每一个客户端界面——GraphQL、pgwire SQL、REST、Arrow Flight、gRPC、Bolt/Cypher——都能以完全相同的治理去调用它（REQ-885、REQ-1156）。[tool-verified: function_dispatch.py module docstring + REQ-885 in requirements.md]

关键区别在于：命令是一次**受治理的 RPC**，而不是临时凑的 ETL。它的输入和输出经过声明、定型、校验、追踪，并接入血缘。一次不受治理的 curl 调用或子进程一样都不占。

## 实现种类

支持六种 `impl_kind` 取值 [tool-verified: `_EXECUTORS` dict in `provisa/executor/function_dispatch.py`]：

| `impl_kind` | 传输方式 |
| --- | --- |
| `source_procedure` | 已注册数据源上的原生存储过程 |
| `source_operation` | OpenAPI、远程 GraphQL 或 gRPC 来源的写入操作，原样透传（见[远程来源的写入操作](#a-remote-sources-write-operation-req-1924)） |
| `script` | 本地子进程，从 stdin 喂入 JSON，从 stdout 读取 JSON |
| `http` | HTTP/S 终结点；JSON 请求体，JSON 响应 |
| `grpc` | gRPC 一元调用；无 proto 的 JSON 桥接 |
| `python` | 进程内的 Python 可调用对象（`module:attr`） |

寻址（目录中的 `name` 与 `function_name`）与 `binding`（传输方式与位置）是解耦的。换掉 binding，命令的治理、血缘和调用方契约都保持不变。[tool-verified: Function model in models.py:710-750]

## 参数种类

每个参数都声明一个 `arg_kind` [tool-verified: FunctionArgument.arg_kind in models.py:691-700]：

| `arg_kind` | 行为 |
| --- | --- |
| `column_value` | 标量；直接在请求负载中传递 |
| `table_ref` | 惰性；Provisa 原样传递关系引用，由服务自行取数 |
| `result_set` | 急切；Provisa 物化被引用的关系并发送其行 |

`http` 和 `grpc` 命令**必须**至少声明一个 `table_ref` 或 `result_set` 参数。一个只收到标量参数的外部命令会被逐行调用一次，那就毁掉了批处理。分发器在调用时拒绝这种配置（422）。[tool-verified: `_reject_rowwise_external` in function_dispatch.py:322-344]

返回集合的命令（通过 `output_columns` 和 `return_schema` 声明）是表值函数。可在 `FROM` 子句或 `JOIN` 中使用它。[inferred from models.py:744-748 and command_localize.py:52-63]

## 数据集契约（REQ-1159）

每个 `table_ref` 或 `result_set` 参数都可以声明一份**输入列契约**：`FunctionArgument.columns` 中一份有序的、按 IR 定型的列清单。命令自身则在 `Function.output_columns` 中声明一份**输出列契约**。[tool-verified: DatasetColumn model in models.py:675-683, Function.output_columns in models.py:748]

两份契约在每一次调用时都会以失败即报的方式校验：

- **输入（仅 result_set）：** 物化之后，Provisa 会依据所声明的列校验这些行。多出的字段、缺失的字段和类型不符都会引发 HTTP 422。
  [tool-verified: `_validate_against` called in `_prepare_args` at function_dispatch.py:243-248]
- **输出：** 命令返回的行在到达调用方之前，会依据 `output_columns` 校验。
  [tool-verified: function_dispatch.py:488-490]
- **窄投影：** 声明了输入契约后，物化查询**只投影那些列**（`SELECT "id", "region" FROM ...`），而不是 `SELECT *`。
  [tool-verified: `_materialize_relation` at function_dispatch.py:155-177, col_names passed
  to projection at line 171]

### IR 类型词汇表

契约中的列类型使用规范的 IR 类型体系（REQ-846），而不是 GraphQL 标量或数据源原生的写法。有效名称为 [tool-verified: `_IR_TO_SA` keys in ir_types.py:45-63]：

`smallint` `integer` `bigint` `text` `boolean` `float` `double` `numeric`
`date` `timestamp` `time` `uuid` `bytea` `json`

常见别名会自动解析（`varchar` → `text`、`int4` → `integer`、`jsonb` → `json` 等）。[tool-verified: `_ALIASES` dict in ir_types.py:67-90]

`return_schema` 是 `output_columns` 的 **GraphQL 投影**，不是事实来源。为校验和血缘声明 `output_columns`；为生成 GraphQL 类型再加上 `return_schema`。[tool-verified: models.py:744-748, comment "return_schema is its GraphQL projection"]

## 编写命令

### 配置文件

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

gRPC 变体（`enrich_grpc_set`）遵循同样的模式，只是指定 `impl_kind: grpc`，并且 `binding` 用 `target` 和 `method` 键代替 `callable`：

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

### 管理 UI

**设置 → 命令**中的命令表单包含一个按数据集划分的输入列编辑器（每个已声明的列一行，配有 IR 类型选择器）和一个输出列编辑器。保存表单即可注册或更新命令，无需重新加载配置。[inferred from CommandFormFields.tsx]

## 内联组合（REQ-1159）

命令可以出现在更大的 SQL 语句**内部**——被联接、被作为子查询、或被投影。你不必局限于 `SELECT * FROM fn(args)`。例外是远程来源的写入操作，它只能单独调用（见[为何不能被组合](#why-it-cannot-be-composed)）。

```sql
-- Enrich the orders relation and join the result back inline.
SELECT o.id, o.amount, e.score, e.region_label
FROM   orders o
JOIN   enrich_orders('main.public.orders') e ON o.id = e.id
WHERE  e.score > 0.8;
```

在治理、校验或路由运行之前，管道会检测出已注册的命令调用，经由共享的受治理执行器逐一执行（因此 I/O 契约和身份模型的适用方式与直接调用完全一致），并把调用点改写为一个已定型的本地关系。
[tool-verified: `_localize_inline_commands` in _pipeline.py:145-163 and localize_commands in
command_localize.py:178-222]

替换是随规模自适应的：不超过 1,000 行时，结果以已定型的 `VALUES` 列表内联；超过该阈值则在引擎中注册为具名的本地关系。
[tool-verified: `_DEFAULT_VALUES_MAX_ROWS = 1000` in command_localize.py:49, path at lines 211-216]

本地化后的语句照常路由。单源查询留在数据源上；只有真正的跨源查询才会走联邦引擎。[tool-verified: _pipeline.py:304 comment
"REQ-1159: a localized statement carries an inline local relation..."]

## 远程来源的写入操作（REQ-1924） {: #a-remote-sources-write-operation-req-1924 }

OpenAPI、远程 GraphQL 或 gRPC 来源会提供写入操作。将其中一个注册为命令后，便可从所有界面调用，并受治理、被审计。添加来源不会注册其中任何一个；你要逐个注册所需的操作，方式与注册数据表相同。注册即为策展。 [tool-verified: `provisa/executor/source_operation.py` module docstring; `provisa/api/admin/schema_common.py` `remote_source_counts`, `"mutations": 0`]

来源提供的内容 [tool-verified: `provisa/executor/source_operation.py` `offered_operations`]：

| 来源类型 | 提供的操作 | 操作名称 |
| --- | --- | --- |
| `openapi` | 规范中的每个非 GET 操作 | `operationId` |
| `graphql_remote` | 远程 `Mutation` 类型的每个字段 | 字段名称 |
| `grpc_remote` | 每个被归类为 mutation 的方法 | `Service.Method` |

### 注册一个 {: #register-one }

1. 打开 **建模 → 命令** 并添加一个命令。
2. 选择远程来源。表单会切换为操作选择器，列出该来源所提供的操作。
3. 选择操作、一个域，以及可以调用它的角色。
4. 可选：开启 **需要审批**，并填写 **写入表** 字段。

[tool-verified: `provisa-ui/src/components/navGroups.ts` (`/commands` in the Model group); `provisa-ui/src/pages/commands/CommandFormFields.tsx` `isSourceOperation`, `command-requires-approval-switch`, `writesTable`; `provisa/api/admin/schema_query.py` `available_functions` ("Listing them registers none")]

通过管理 GraphQL API，`availableFunctions(sourceId, schemaName)` 会列出这些操作。模式名称依来源类型而定，为 `openapi`、`graphql` 或 `grpc_remote`。 [tool-verified: `OPERATION_SCHEMA` in `source_operation.py`; `available_functions` returns `[]` when the schema name does not match the source type]

其余一切均由操作决定，而非由表单所发送的内容决定。`_as_source_operation` 会覆盖以下字段：

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

每个参数的类型均为 `json`。操作的参数即其 OpenAPI 路径参数（操作带请求体时另加 `body`）、其 GraphQL mutation 参数，或其 gRPC 请求字段。 [tool-verified: `_openapi_operations`, `_graphql_operations`, `_grpc_operations` in `source_operation.py`] 来源未提供的操作会被拒绝，返回 422，`functions.operation_not_offered`。 [tool-verified: `offered_operation`]

### 调用 {: #calling-it }

Provisa 不会对输入进行整形、类型化或检查。每个参数都原样连同来源的凭据发往远程服务，远程服务的应答也原样返回。Provisa 负责治理谁可以调用、在哪个域中调用、是否需要审批，并记录该调用。 [tool-verified: `source_operation.py` module docstring]

远程服务如何接收参数：

- **OpenAPI.** 路径参数填入路径模板。`body` 是 JSON 请求体。其余每个参数都放在查询字符串中。 [tool-verified: `_call_openapi`]
- **GraphQL.** 参数以类型化变量发送，每个变量均按远程模式给定的类型声明。mutation 会回取应答中的标量字段和枚举字段，以及其内部对象的这类字段，最多深入两层。 [tool-verified: `mutation_document`, `_selection`, `_ANSWER_DEPTH = 2`]
- **gRPC.** 这些参数成为 `Service.Method` 的请求消息。 [tool-verified: `_call_grpc`]

应答即命令的行：一个对象是一行，对象列表即其各行，其他任何内容都是一行 `{"result": ...}`。 [tool-verified: `_rows`]

在 GraphQL 中，命令是一个 mutation 字段，其应答是 JSON 标量。 [tool-verified: `provisa/compiler/actions_schema.py` (`gql_return = JSONScalar` for `source_operation`; `kind` defaults to `"mutation"`)]

```graphql
mutation {
  createIssue(input: {repositoryId: "R_kgDO...", title: "Crash on save"})
}
```

[inferred: argument names are those of the remote operation; the example call was not run]

在 SQL 界面（pgwire 及其他传递 SQL 的界面）上，将每个参数写成字符串中的 JSON 字面量。`'{"title": "x"}'` 是对象，`'"text"'` 是字符串，`'3'` 是数字。不是有效 JSON 的字面量会失败，返回 422，`functions.json_argument_invalid`。 [tool-verified: `_json_arguments_from_sql` in `function_dispatch.py`]

```sql
SELECT * FROM create_issue('{"repositoryId": "R_kgDO...", "title": "Crash on save"}');
```

[inferred: the first-argument shape follows `_json_arguments_from_sql`; the statement was not run, and the command's argument list is the operation's]

在 REST 上，向 `/data/rest/{domain}/commands/{command}` 以 POST 发送由参数组成的 JSON 对象。`json` 参数在生成的规范中被记录为任意值。 [tool-verified: `provisa/api/rest/openapi_spec.py` `cmd_path = f"/{cmd_domain}/commands/{cmd_name}"`, `_arg_type_to_openapi` (`"json"` returns `{}`)]

```bash
curl -X POST https://acme.provisa.org/data/rest/engineering/commands/create_issue \
  -H "Content-Type: application/json" \
  -d '{"input": {"repositoryId": "R_kgDO...", "title": "Crash on save"}}'
```

[inferred: host, domain and command name are placeholders; not run]

### 拒绝 {: #refusals }

远程服务的拒绝按其原样返回。 [tool-verified: `_refused` in `source_operation.py`]

| 远程服务 | Provisa 应答 |
| --- | --- |
| 拒绝该调用（HTTP 4xx、GraphQL `errors`、被拒绝的 gRPC 调用） | 422，`functions.remote_refused`，附带 `remote_status` 和 `answer` |
| 失败（HTTP 5xx） | 502, `functions.remote_refused` |

来源的凭据能否执行该操作，由远程服务在调用该操作时决定。Provisa 无法在注册时试写而不真正执行它。 [tool-verified: REQ-1924 CREDENTIAL AT CALL amendment in `docs/arch/requirements.yaml`; no credential check in `_as_source_operation`]

### 审批 {: #approval }

开启 **需要审批** 后，每次调用在运行前都会提交给部署的审批钩子。仅当钩子批准时才会运行。 [tool-verified: `provisa/api/data/action_exec.py` `_require_approval`]

- 未配置钩子：403，`functions.approval_unavailable`。
- 钩子拒绝：403，`functions.approval_denied`，附带钩子给出的原因。

钩子会收到调用者、角色、命令名称及其参数。参见[ABAC批准钩子](security.md#abac-hook)。该标志存储为 `Function.requires_approval`，该检查适用于任何设置了它的命令。 [tool-verified: `action_exec.py` `if fn.get("requires_approval")`]

### 写入表 {: #writes-table }

在 **写入表** 字段中填写该操作所写入的数据表，格式为 `schema.table`。它必须是该命令自身来源下的已注册数据表，否则保存会被拒绝，返回 422，`actions.written_table_not_registered`。该设置可选。 [tool-verified: `_check_written_table` in `actions_router.py`; `written_table` in `source_operation.py`]

远程服务每接受一次调用，Provisa 就将其视为对该数据表的一次写入。它会丢弃该表的缓存应答，将基于它的物化视图标记为过期，发出变更事件，运行该表的 sink，并在该表被保持为热状态时重新加载它。 [tool-verified: `provisa/api/data/table_written.py` `after_table_written`]

调用不会刷新副本。这有待一种请求刷新副本的方式，而该方式尚未构建；在此之前，副本按其自身的计划刷新。 [tool-verified: REQ-1924 WRITTEN TABLE amendment; no replica call in `after_table_written`]

### 为何不能被组合 {: #why-it-cannot-be-composed }

写入操作是一个动作，而不是对数据的转换。包含它的视图或物化视图会在每次被读取或刷新时执行该写入。因此该调用必须单独存在：单独的 `SELECT * FROM create_issue(...)` 会运行它，而同一调用出现在更大的语句内则会被拒绝，无论位于何处——联接、子查询或投影中。 [tool-verified: `provisa/pgwire/_pipeline.py` `_refuse_composed_mutators`; `provisa/executor/source_operation.py` `writes_called_in`]

若视图或物化视图的定义调用了它，保存时会被拒绝。只要注册了任何写入操作，无法解析的定义也会被拒绝，因为无法证明它没有调用任何一个。 [tool-verified: `refuse_writes_in_definition` in `source_operation.py`, called from `provisa/api/admin/_table_ops.py` `_build_columns_for_input` (views) and `provisa/api/admin/schema_common.py` (materialized views)]

```text
command 'create_issue' writes to its source and is called on its own: it cannot be composed in a query, a view or a materialized view (REQ-1924)
```

来源操作也不是血缘节点：血缘读取自视图和查询的 SQL，命令在其中表现为节点，而任何已保存的定义都无法调用来源操作。 [tool-verified: `provisa/lineage/graph.py` (`kind="command"` for a call in the SQL); `refuse_writes_in_definition`]

## 命令与血缘

由于每个命令都声明了自己的输入列和输出列，列级血缘得以**跨越不透明的命令边界闭合**。血缘引擎会施加一次污点闭包：每个已声明的输出列都派生自每个已声明的输入列。[tool-verified: `_splice_commands` in graph.py:223-242]

**由此带来的实际后果：** 输入契约的宽度决定了那次闭包的精度。窄输入——只包含命令确实需要的列——产出一个紧凑、可读的血缘锥。把源关系中的每一列都声明进去，则会在每个输出上大幅扇入，这依然是可靠的（不会丢失任何血缘），但会模糊可追溯性。

**经验法则：** 传入命令所需的最小投影，并且只返回派生出来的列（不要把原样透传的输入也带回来）。这样能让污点锥保持准确。[inferred from
_splice_commands behavior in graph.py and _materialize_relation narrow-projection in function_dispatch.py:161]

命令节点如何出现在 DAG 中以及如何解读它们，参见[血缘](lineage.md)。

## 出口允许列表

`http` 和 `grpc` 命令会调用外部终结点。每一个目标主机都必须出现在该部署的 `udf_egress_allowlist` 中。回环地址（`localhost`、`127.0.0.1`、`::1`）始终被放行。允许列表缺失时，所有外部出口一律以 HTTP 403 拒绝——不存在悄悄生效的默认值。[tool-verified: `_check_egress` in function_dispatch.py:292-311]

## 调用追踪（REQ-886）

无论结果如何，每一次调用都会发出一条追踪记录。追踪内容包括命令名称、传输种类、身份模型（DEFINER 或 INVOKER）、输入关系引用、角色 id 以及输出基数。追踪由分发器发出——没有哪种 `impl_kind` 能绕过它。
[tool-verified: `udf_invocation_trace` context in dispatch_function:475-492]

## CLI：provisa metadata export

`provisa metadata export` 是一个 shell 层的作业，而不是受治理的 RPC。它通过向 `/admin/metadata-export/publish` 发送 POST 请求，触发正在运行的服务器按需发布元数据（REQ-1072/REQ-1074）——与管理选项卡上**立即发布**按钮所调用的终结点相同。[tool-verified: `_cmd_metadata_export` in provisa/cli.py:272-310]

当配置的 `reconcile_cron` 计划粒度不够时，可用它从 cron 或 CI 驱动定时导出：

```bash
provisa metadata export --api https://acme.provisa.org --token "$PROVISA_API_TOKEN"
```

退出码 0 = 完整发布。退出码 1 = 部分发布或连接失败。

完整的标志参考、认证选项、多租户主机命名以及一个 cron 示例，参见[元数据导出——从命令行](metadata-export.md#from-the-command-line)。


命令会出现在每个环境的 git 投影中。命令及其标签分配如何在合并与拉取中留存，参见[环境](environments.md)。
