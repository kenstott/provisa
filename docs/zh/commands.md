# 命令

命令是一个已注册的、受治理的函数，它把外部计算纳入 Provisa 的治理、审计与血缘体系。联邦引擎原生处理 SQL，而命令则是它无法表达的那部分计算的接缝：一个数据增强微服务、一个 Python 模型、一段 shell 脚本、一个数据库原生的存储过程。注册一次；每一个客户端界面——GraphQL、pgwire SQL、REST、Arrow Flight、gRPC、Bolt/Cypher——都能以完全相同的治理去调用它（REQ-885、REQ-1156）。[tool-verified: function_dispatch.py module docstring + REQ-885 in requirements.md]

关键区别在于：命令是一次**受治理的 RPC**，而不是临时凑的 ETL。它的输入和输出经过声明、定型、校验、追踪，并接入血缘。一次不受治理的 curl 调用或子进程一样都不占。

## 实现种类

支持五种 `impl_kind` 取值 [tool-verified: `_EXECUTORS` dict in function_dispatch.py:420-426]：

| `impl_kind` | 传输方式 |
| --- | --- |
| `source_procedure` | 已注册数据源上的原生存储过程 |
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

命令可以出现在更大的 SQL 语句**内部**——被联接、被作为子查询、或被投影。你不必局限于 `SELECT * FROM fn(args)`。

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
