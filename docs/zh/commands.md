# 命令

命令是一个已注册、受治理的函数，它将外部计算纳入 Provisa 的治理、审计和血缘体系。联邦查询引擎原生处理 SQL，而命令则是它无法表达的计算之间的接缝：一个增强型微服务、一个 Python 模型、一段 shell 脚本、一个数据库原生的存储过程。注册一次；每个客户端表面——GraphQL、pgwire SQL、REST、Arrow Flight、gRPC、Bolt/Cypher——都能以相同的治理方式调用它（REQ-885, REQ-1156）。[tool-verified: function_dispatch.py module docstring + REQ-885 in requirements.md]

关键区别在于：命令是一次**受治理的 RPC**，而非临时的 ETL。它的输入和输出都是被声明、类型化、经过校验、可追踪，并接入血缘体系的。一次不受治理的 curl 调用或子进程都不具备这些特性。

## 实现类型

支持五种 `impl_kind` 取值 [tool-verified: `_EXECUTORS` dict in function_dispatch.py:420-426]：

| `impl_kind` | 传输方式 |
| --- | --- |
| `source_procedure` | 已注册数据源上的原生存储过程 |
| `script` | 本地子进程，通过 stdin 输入 JSON，从 stdout 读取 JSON |
| `http` | HTTP/S 端点；JSON 请求体，JSON 响应 |
| `grpc` | gRPC 一元调用；无 proto 的 JSON 桥接 |
| `python` | 进程内 Python 可调用对象（`module:attr`） |

寻址方式（目录中的 `name` 和 `function_name`）与 `binding`（传输方式和位置）是解耦的。更换 binding 后，命令的治理、血缘和调用方契约保持不变。[tool-verified: Function model in models.py:710-750]

## 参数类型

每个参数都声明一个 `arg_kind` [tool-verified: FunctionArgument.arg_kind in models.py:691-700]：

| `arg_kind` | 行为 |
| --- | --- |
| `column_value` | 标量；直接在请求负载中传递 |
| `table_ref` | 惰性；Provisa 按原样传递关系引用；由服务方获取数据 |
| `result_set` | 立即求值；Provisa 物化被引用的关系并发送其行数据 |

`http` 和 `grpc` 命令**必须**声明至少一个 `table_ref` 或 `result_set` 参数。一个只接收标量参数的外部命令会被逐行调用一次，这会破坏批处理。调度器会在调用时拒绝这种配置（422）。[tool-verified: `_reject_rowwise_external` in function_dispatch.py:322-344]

一个返回集合（通过 `output_columns` 和 `return_schema` 声明）的命令就是一个表值函数。可以在 `FROM` 子句或 `JOIN` 中使用它。[inferred from models.py:744-748 and command_localize.py:52-63]

## 数据集契约（REQ-1159）

每个 `table_ref` 或 `result_set` 参数都可以声明一个**输入列契约**：在 `FunctionArgument.columns` 中一份有序的、经过 IR 类型标注的列列表。命令本身在 `Function.output_columns` 中声明一个**输出列契约**。[tool-verified: DatasetColumn model in models.py:675-683, Function.output_columns in models.py:748]

两个契约在每次调用时都会以快速失败的方式进行校验：

- **输入（仅 result_set）：** 物化完成后，Provisa 会对照声明的列校验行数据。多余字段、缺失字段和类型错误都会抛出 HTTP 422。[tool-verified: `_validate_against` called in `_prepare_args` at function_dispatch.py:243-248]
- **输出：** 命令返回的行数据在到达调用方之前，会对照 `output_columns` 进行校验。[tool-verified: function_dispatch.py:488-490]
- **窄投影：** 当声明了输入契约时，物化查询只投影**这些列**（`SELECT "id", "region" FROM ...`），而非 `SELECT *`。[tool-verified: `_materialize_relation` at function_dispatch.py:155-177, col_names passed to projection at line 171]

### IR 类型词汇表

契约列类型使用规范的 IR 类型系统（REQ-846），而非 GraphQL 标量或数据源原生拼写。合法的名称有 [tool-verified: `_IR_TO_SA` keys in ir_types.py:45-63]：

`smallint` `integer` `bigint` `text` `boolean` `float` `double` `numeric`
`date` `timestamp` `time` `uuid` `bytea` `json`

常见别名会自动解析（`varchar` → `text`，`int4` → `integer`，`jsonb` → `json`，等等）。[tool-verified: `_ALIASES` dict in ir_types.py:67-90]

`return_schema` 是 `output_columns` 的 **GraphQL 投影**，而非事实来源。声明 `output_columns` 用于校验和血缘；添加 `return_schema` 用于生成 GraphQL 类型。[tool-verified: models.py:744-748, comment "return_schema is its GraphQL projection"]

## 编写一个命令

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

gRPC 变体（`enrich_grpc_set`）遵循相同模式，但指定 `impl_kind: grpc`，并使用带 `target` 和 `method` 键的 `binding`，而非 `callable`：

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

### 管理界面

**设置 → 命令**中的命令表单包含一个按数据集的输入列编辑器（每个声明的列一行，带 IR 类型选择器）和一个输出列编辑器。保存表单即可注册或更新命令，无需重新加载配置。[inferred from CommandFormFields.tsx]

## 内联组合（REQ-1159）

命令可以出现在**更大的** SQL 语句**内部**——被联结、子查询或投影。您不局限于 `SELECT * FROM fn(args)`。

```sql
-- Enrich the orders relation and join the result back inline.
SELECT o.id, o.amount, e.score, e.region_label
FROM   orders o
JOIN   enrich_orders('main.public.orders') e ON o.id = e.id
WHERE  e.score > 0.8;
```

在治理、校验或路由运行之前，流水线会检测已注册的命令调用，通过共享的受治理执行器执行每一个（因此 I/O 契约和身份模型与直接调用完全一致），并将调用点重写为一个类型化的本地关系。[tool-verified: `_localize_inline_commands` in _pipeline.py:145-163 and localize_commands in command_localize.py:178-222]

替换方式是按规模自适应的：结果在 1,000 行以内时以类型化的 `VALUES` 列表内联；超过该阈值时则在引擎中注册为一个命名的本地关系。[tool-verified: `_DEFAULT_VALUES_MAX_ROWS = 1000` in command_localize.py:49, path at lines 211-216]

一条本地化后的语句会正常路由。单数据源查询留在数据源上；只有真正跨数据源的查询才会发往联邦查询引擎。[tool-verified: _pipeline.py:304 comment "REQ-1159: a localized statement carries an inline local relation..."]

## 命令与血缘

由于每个命令都声明了其输入和输出列，列级血缘能够**跨越这个不透明的命令边界闭合**。血缘引擎应用了一个污点闭包：每个声明的输出列都派生自每个声明的输入列。[tool-verified: `_splice_commands` in graph.py:223-242]

**可操作的结果是：** 输入契约的宽度决定了该闭包的精度。一个窄输入——只包含命令实际需要的列——会产生一个紧凑、可读的血缘锥。声明源关系中的每一列会使血缘在每个输出上都广泛扇入，这在正确性上依然是可靠的（不会丢失血缘），但会模糊可追溯性。

**经验法则：** 只传递命令所需的最小投影，并只返回派生列（而非原样回传未变的输入列）。这能让污点锥保持准确。[inferred from _splice_commands behavior in graph.py and _materialize_relation narrow-projection in function_dispatch.py:161]

关于命令节点如何出现在 DAG 中以及如何解读它们，参见[血缘](lineage.md)。

## 出站白名单

`http` 和 `grpc` 命令会调用外部端点。每个目标主机都必须出现在部署环境的 `udf_egress_allowlist` 中。回环地址（`localhost`、`127.0.0.1`、`::1`）始终被允许。缺失白名单会以 HTTP 403 拒绝所有外部出站——不存在静默的默认放行。[tool-verified: `_check_egress` in function_dispatch.py:292-311]

## 调用追踪（REQ-886）

无论结果如何，每次调用都会产生一条追踪记录。该追踪记录包含命令名称、传输类型、身份模型（DEFINER 或 INVOKER）、输入关系引用、角色 ID 和输出基数。调度器负责发出该追踪记录——没有任何 `impl_kind` 能够绕过它。[tool-verified: `udf_invocation_trace` context in dispatch_function:475-492]

## CLI：provisa metadata export

`provisa metadata export` 是 shell 层的作业，而非受治理的 RPC。它通过向
`/admin/metadata-export/publish` 发送 POST 请求，触发运行中服务器的按需元数据发布（REQ-1072/REQ-1074）——
与管理页签中**立即发布**按钮调用的是同一个端点。[tool-verified: `_cmd_metadata_export` in provisa/cli.py:272-310]

当配置的 `reconcile_cron` 调度粒度不够时，可用它从 cron 或 CI 驱动定时导出：

```bash
provisa metadata export --api https://acme.provisa.org --token "$PROVISA_API_TOKEN"
```

退出码 0 = 完整发布。退出码 1 = 部分发布或连接失败。

完整的参数说明、认证选项、多租户主机命名以及 cron 示例，参见
[元数据导出——从命令行](metadata-export.md#from-the-command-line)。
