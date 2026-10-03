# 远程模式

远程模式来源将外部 API——GraphQL（含 GitHub）、gRPC 或 REST（OpenAPI）——连接至 Provisa 语义层。添加来源不会注册任何数据表。来源会提供数据表，由数据管家通过“Register Table”选择器注册所需的每一张表；该注册即为策展（curation）步骤。（REQ-308、REQ-316、REQ-322）已注册的数据表是一级的 Provisa 数据表。（REQ-308、REQ-316、REQ-325）所有治理规则、查询接口及安全层均会自动应用。（REQ-310、REQ-319、REQ-328）远程服务永远不会看到 Provisa 的治理规则。（REQ-310、REQ-319、REQ-328）

---

## 三种来源类型

### GraphQL 远程模式（REQ-307–313）

**如何添加来源。** 向 `/admin/sources/graphql-remote` 发送 POST 请求，附上端点 URL、命名空间及可选的认证信息。Provisa 会对远程端点发起标准的 `__schema` 内省查询，以确认端点及凭据。（REQ-307）[tool-verified: `provisa/graphql_remote/introspect.py:47–59`]

添加来源不会注册任何数据表或 command。每种远程来源类型在添加和刷新时都返回相同的计数：`tables`（已注册或已更新至最新的数据表；添加时为 0）、`available_tables`（可提供的数据表）、`mutations`（始终为 0）及 `available_mutations`（可提供的 command）。[tool-verified: `provisa/api/admin/schema_common.py` `remote_source_counts`; `provisa/api/admin/graphql_remote_router.py` `register_graphql_remote_source`]

**注册数据表。** 打开 Tables，再打开 Register Table，选择来源及模式 `graphql`，然后选择所需的数据表和列。通过管理端 GraphQL API：`availableTables(sourceId, schemaName)` 列出可提供的数据表，`availableColumns` 列出某张数据表的列，`registerTable(input: TableInput)` 按所选的列注册一张数据表。已注册的数据表随即受到治理。（REQ-308）[tool-verified: `provisa/api/admin/introspect.py` `_native_tables_graphql` (`if schema_name != "graphql": return []`), `provisa/api/admin/_graphql_table_registration.py` `offered_tables`, `offered_columns`, `columns_to_register`][tool-verified: `provisa/api/admin/schema_query.py` `available_tables`, `available_columns`; `registerTable` is from the task brief, not read]

已注册数据表的读取方式（根字段、行路径、必填参数、分页参数）存储于 `sources.mapping["tables"]`，因此重启后的进程无需向远程服务查询其模式即可读取。[tool-verified: `_graphql_table_registration.py` `TABLE_SPECS_KEY = "tables"`, `remember_table`]

```json
{
  "source_id": "petstore-gql",
  "url": "https://api.example.com/graphql",
  "namespace": "petstore",
  "domain_id": "veterinary",
  "auth": { "type": "bearer", "token": "..." },
  "cache_ttl": 300,
  "field_overrides": { "createPet": "query" },
  "relationships": [
    { "source_table": "petstore__pets", "source_column": "owner_id",
      "target_table": "owners__users", "target_column": "id" }
  ]
}
```

认证选项：`none`、`bearer`（Authorization 头）、`basic`（Base64 编码的用户名:密码）。（REQ-307）[tool-verified: `provisa/graphql_remote/introspect.py:36–45`]

**字段覆盖。** `field_overrides` 是一个 `{fieldName: "query" | "mutation"}` 映射表，于内省后应用，其优先于结构性分类。只有 query 类型的字段可重新分类为 mutation；mutation 类型的字段在 GraphQL 中没有覆盖路径。（REQ-531）[tool-verified: `provisa/graphql_remote/mapper.py`]

**注册时的关系。** `relationships` 于注册时声明数据表之间的外键/主键连接路径，并存储为手动声明的关系（没有 `remote_managed` 标志）。刷新时，自动检测的关系（带有 `remote_managed: True` 的）会重新执行并可能改变；手动声明的关系则不受影响。（REQ-554）[tool-verified: `provisa/api/admin/graphql_remote_router.py`]

**来源提供的内容。** 远程 `Query` 类型上每个返回对象或对象列表的字段都会作为数据表提供，单对象字段下的每个 Relay 连接亦然（见下文）。注册某个已提供的数据表即使其成为数据表。远程 `Mutation` 类型上的每个字段都是一个可提供的命令，计入 `available_mutations`；添加来源不会注册其中任何一个。将所需的注册为命令；参见[远程来源的写入操作](commands.md#a-remote-sources-write-operation-req-1924)。（REQ-308、REQ-1924） [tool-verified: `provisa/graphql_remote/mapper.py:243–278`, `graphql_remote_router.py` `register_graphql_remote_source` (`"functions": 0`)]

**数据表命名。** 数据表命名为 `{namespace}__{field_name}`。以命名空间 `petstore` 及查询字段 `pets` 为例：数据表名称为 `petstore__pets`。（REQ-312）[tool-verified: `provisa/graphql_remote/mapper.py:250`]

**Relay 连接。** 许多 API 以 Relay 连接返回列表：一个对象，带有 `nodes`（或 `edges { node }`）以及 `pageInfo`。Provisa 将连接映射为其节点的数据表，并逐页读取。（REQ-308、REQ-309）[tool-verified: `provisa/graphql_remote/mapper.py` `_is_connection`, `_map_connection_table`]

- 返回连接的根字段（`securityAdvisories`）会成为其节点的一张数据表。
- 根字段所返回的单个对象上的连接会成为独立的数据表。该数据表沿用根字段的必填参数。对于 `repository(owner, name)` 及 `Repository` 上的连接 `issues`，数据表为 `repositoryIssues`，在命名空间 `gh` 下的 SQL 名称为 `gh__repository_issues`。可通过 `_nf_owner` 和 `_nf_name` 列对其筛选：`WHERE _nf_owner = 'acme' AND _nf_name = 'widgets'`。
- 连接绝不会成为列。否则每一行都要为其类型所拥有的每个连接携带一次由远程逐行计算的读取。
- 仅当连接字段接受 `first` 和 `after`、从而可逐页读取时，它才是数据表。需要自身参数的连接不是数据表。联合类型（union）的连接，以及返回列表的根字段之下的任何连接，也都不是。

[tool-verified: `provisa/graphql_remote/mapper.py` `_map_connection_table`, `_map_child_connection_tables`; `tests/unit/test_graphql_remote_relay.py` `test_child_connection_table_takes_the_root_fields_arguments`]

**类型映射（REQ-308）。** 标量字段会直接映射至 Provisa 类型。OBJECT 字段则按目标类型是否受治理而分为两种情况（见下方"受治理数据表"）。[tool-verified: `provisa/graphql_remote/mapper.py:14–36`, `provisa/api/data/endpoint.py:655–671`, `provisa/compiler/schema_gen.py:481–485`]

| GraphQL 类型 | Provisa 类型 |
| --- | --- |
| `String` | `text` |
| `ID` | `text` |
| `Int` | `integer` |
| `Float` | `numeric` |
| `Boolean` | `boolean` |
| OBJECT（未受治理的内嵌类型，例如 `ContactInfo`） | `jsonb` blob 列 |
| OBJECT（受治理的目标类型） | 完全从 SDL 及提取中排除 |
| 任何 ENUM | `jsonb` |
| 自定义标量 | `text`（回退值） |

**受治理数据表。** 若 GQL 类型在远程模式中以 `Query` 的根字段形式出现，即属受治理类型。`_collect_queryable_types` 会于注册期间收集这些类型，并优先选取没有必填参数的字段，使其可作为联接目标进行批量提取。[tool-verified: `provisa/graphql_remote/mapper.py:395–413`]

当受治理数据表上的 OBJECT 类型字段指向另一个受治理类型时，该字段会同时受三项规则约束 [tool-verified: `provisa/api/data/endpoint.py:655–671`, `provisa/compiler/schema_gen.py:481–485`]：

1. **从 GQL 提取中排除**——提取父数据表的行数据时，不会请求该字段。
2. **从 SDL 中排除**——该字段不会出现在生成模式中的父类型上。
3. **仅可通过已声明的关系访问**——数据管家必须在两个已物化的受治理数据表之间注册 JOIN。若无此关系，该字段纯粹缺失；并无 blob 回退方案。

无法作为根 Query 字段访问的 OBJECT 类型（例如 `ContactInfo` 或 `Address` 等内嵌类型）遵循不同的规则：它们会以 `jsonb` blob 列的形式提取，并于 SDL 中呈现为嵌套对象字段。子字段可通过 SQL 中的 `-->>` 提取访问。

**需要参数的字段不是列。** 带有必填参数的字段无法直接选取，因此会被排除在数据表的列及嵌套选择之外。[tool-verified: `provisa/graphql_remote/mapper.py` `_build_columns`, `_build_gql_field_selection`]

**必填参数。** 当根查询字段带有非空值、无默认值的参数时，这些参数会成为数据表上的 `native_filter_type: query_param` 字段（于注入时加上 `_nf_` 前缀）。执行器会将其作为 GraphQL 变量传递。（REQ-555）[tool-verified: `provisa/graphql_remote/mapper.py:110–120`, `provisa/api/app.py:1280–1303`]

**自动检测的关系。** Provisa 会扫描每张已注册数据表中 OBJECT 类型的列。当所引用的 GQL 类型同样是同一来源中已注册的数据表，且该关系所依附的列属于已注册的列时，该关系即被存储。尚未注册的数据表则不会获得关系。[tool-verified: `_graphql_table_registration.py` `sync_detected_relationships`]多对一关系会依据命名惯例推断来源列与目标列（来源类型上的 `breedName` → 目标类型 `Breed` 上的 `name`）。一对多（LIST）字段会产生列引用为空的关系——外键位于目标一侧。（REQ-554）[tool-verified: `provisa/graphql_remote/mapper.py:162–202`]

**Mutation。** mutation 字段会被逐个注册为 `source_operation` 种类的命令。其参数的类型均为 `json`，并作为类型化变量传给远程服务；应答即远程服务返回的 JSON，没有 `return_schema`。参见[远程来源的写入操作](commands.md#a-remote-sources-write-operation-req-1924)。（REQ-1924） [tool-verified: `provisa/api/admin/actions_router.py` `_as_source_operation` (`body.returns = ""`, arguments typed `json`); `provisa/executor/source_operation.py` `_call_graphql`]

**刷新。** 向 `/admin/sources/graphql-remote/{id}/refresh` 发送 POST 请求。此操作会重新对远程模式进行内省，并使已注册的数据表与之保持一致。它不会添加任何数据表或列：模式新增的数据表或列仍处于可提供状态，模式已删去的列则会被移除。已有的治理规则（RLS、脱敏）将予以保留。（REQ-311）[tool-verified: `provisa/api/admin/graphql_remote_router.py` `refresh_graphql_remote_source`; `_graphql_table_registration.py` `refreshed_registered_tables`: "a column the schema has lost is gone; one it has gained is on offer and is not added"]

**限制。**

- 标量及 ENUM 类型的根查询字段（返回类型非 OBJECT）会成为受跟踪的函数，而非虚拟数据表。其 `return_schema` 为单一字段 `value`，类型为对应的标量类型。[tool-verified: `provisa/graphql_remote/mapper.py:254–279`]
- 对象嵌套结构于注册时会解析至 `graphql_remote.max_object_depth`（默认值：5）。远程获取的选择集与子字段元数据均构建至此深度；超出限制的字段不会被获取，也无法用于 SQL 提取。沿任一路径，同一类型只会进入一次：类型已在向下路径上的字段会被排除，因此类型相互引用的模式每个类型只遍历一次，而不是每个深度层级遍历一次。（REQ-556）[tool-verified: `provisa/graphql_remote/mapper.py` `_build_gql_field_selection`, `tests/unit/test_graphql_remote_relay.py` `test_a_type_is_entered_once_along_a_path`]
- LIST 类型的嵌套 OBJECT 字段（例如 `breed.awards: [Award]`）会被纳入获取选择集，最多嵌套 `graphql_remote.max_list_depth` 层（默认值：2）。在此限制内，列表会作为父列上的 `jsonb` 数组获取。当列表字段声明了 `first` 参数（Relay、PostGraphile、pg_graphql）或 `limit` 参数（Hasura）时，选择集会将其作为 `first: N` 或 `limit: N` 传入，其中 N 为 `graphql_remote.max_list_items`（默认值：100）。两者都未声明的列表字段不会获得参数，因为远程服务会拒绝字段未声明的参数。超出 `max_list_depth` 后，该 LIST 字段将被完全排除，以防止数据无限膨胀。在 SQL 中，数组可通过 `json_array_elements(column_name)` 或使用 `->>` 的索引提取来访问。若列表的元素类型自带根查询，应改为将其注册为独立数据表并建立关系——联接路径更高效，且可绕过 blob。（REQ-556）[tool-verified: `provisa/graphql_remote/mapper.py` `_list_limit_arg`, `_build_gql_field_selection`; `tests/unit/test_graphql_remote_relay.py` `test_a_plain_list_takes_no_first_and_a_list_that_declares_first_gets_it`]
- 对于 SQL 查询，未受治理的 OBJECT 类型字段会从远程来源完整提取（所有子字段至配置深度为止），并以 `jsonb` 形式缓存。SQL 中对子字段的访问是通过对 blob 进行 `->>` 提取来处理；远程请求不会限缩为 SQL 查询所选取的字段。当列表的元素类型没有根查询，且 blob 表示法不敷使用时，应直接以 GraphQL SDL 编写查询——Provisa 会忠实地重现 GQL 字段选择，令远程来源仅接收到确切请求的字段。[tool-verified: `provisa/compiler/sql_gen.py:1332–1368`]
- 若远程服务器因需要子字段选择而拒绝某个 OBJECT 类型字段（在 `gql_selection` 可用时理应不会发生此情况），执行器会移除该等字段后重试一次，以确保标量字段仍可正常返回。此规则适用于从根字段读取的数据表。连接数据表不走此路径。[tool-verified: `provisa/graphql_remote/executor.py` `execute_remote` (`for attempt in range(2)`), `_execute_connection`]

**分页读取。** 连接数据表通过游标读取。每一页请求 `first: N, after: $pageCursor` 及 `pageInfo { hasNextPage endCursor }`，读取会沿 `endCursor` 继续，直至远程服务报告没有下一页。（REQ-309）[tool-verified: `provisa/graphql_remote/executor.py` `_connection_query`, `_execute_connection`]

| 设置 | 默认值 | 作用 |
| --- | --- | --- |
| `graphql_remote.max_list_items` | `100` | 每页行数。[tool-verified: `provisa/api/data/materialization.py` passes `limit=max_items` to `execute_remote`] |
| `graphql_remote.max_rows` | `10000` | 对一张连接数据表的一次读取最多取得的行数。达到该值的读取会停止并记录一条警告。[tool-verified: `provisa/core/models.py` `GraphQLRemoteConfig`] |

```yaml
graphql_remote:
  max_list_items: 100
  max_rows: 10000
```

有两种响应会使执行器重试：

- **页面过重。** 当远程服务返回 502 或 504 时，会以一半大小重新请求同一页，最小降至一行。[tool-verified: `_PAGE_TOO_HEAVY = (502, 504)`, `page_size = max(1, page_size // 2)`]
- **带等待时间的速率限制。** 当远程服务返回 403 或 429，且 `Retry-After` 不超过 120 秒时，执行器会等待相应时间后重新发送请求，最多三次。没有 `Retry-After`，或要求更长等待时间的拒绝，会作为错误抛出。此规则适用于每一次读取，无论是否为连接。[tool-verified: `_post`, `_RETRY_AFTER_STATUSES`, `_RETRY_AFTER_ATTEMPTS`, `_RETRY_AFTER_MAX_SECONDS`]

响应中的任何其他错误都会使读取失败，除非来源类型另有规定（见下文 GitHub）。父级返回 null 的连接没有任何行。[tool-verified: `_accept_row_field_errors`, `_execute_connection`]

---

### GitHub（REQ-1923）

GitHub 是一种普通的来源类型。其 API 为 GraphQL，因此其数据表的行为如上所述，包括 `gh__repository_issues` 这类连接数据表。[tool-verified: `provisa/graphql_remote/brands.py` `BRANDS["github"]`]

**添加来源。**

1. 打开 Sources，添加类型为 **GitHub** 的来源。
2. 输入 GitHub 访问令牌。可选择输入命名空间，即数据表名称的前缀；默认值为 `gh`。
3. 保存。Provisa 会向 GitHub 校验该令牌。被 GitHub 拒绝的令牌会使添加失败，并附上 GitHub 的消息。

添加来源不会注册任何数据表。[tool-verified: `provisa/api/admin/graphql_remote_router.py` `_register_branded_source` (`"tables": 0`, `verify_query="query { viewer { login } }"`)]

**注册数据表。** 打开 Tables，再打开 Register Table。选择 GitHub 来源，选择模式 `graphql`，然后选择所需的数据表。GitHub 提供的每张数据表都会列出；注册即由您决定公开哪些。[tool-verified: `provisa/api/admin/_graphql_table_registration.py` `offered_tables`][inferred: picker labels and the `graphql` schema name from the task brief; the UI strings were not read]

**令牌范围（scope）。** 注册数据表时，Provisa 会用您的令牌向 GitHub 校验一次。

- 令牌范围未涵盖的字段会被排除在数据表之外。结果会列出每个被排除的字段：`Left out, because the source's credential may not read them: projectsV2`。[tool-verified: `provisa/api/admin/schema_mutation_ops.py`]
- 令牌完全无法读取的数据表会被拒绝，并附上 GitHub 给出的原因：`GitHub does not let this source's credential read gh__repository_issues: ...`。[tool-verified: `provisa/api/admin/_table_ops.py` `_branded_columns_for_input`]

**令牌无权查看的行。** 对于令牌无权在某一特定行中查看的字段（例如无推送权限的仓库的协作者），GitHub 返回 `FORBIDDEN`；对于仅存在于组织所有的仓库的字段，则返回 `NOT_ORG_OWNED_REPO`。该字段在该行中为 null，其余读取照常进行，Provisa 会记录一条警告。针对数据表本身的错误会使读取失败。[tool-verified: `provisa/graphql_remote/brands.py` `error_policy`, `provisa/graphql_remote/executor.py` `_accept_row_field_errors`]

**过重的页面。** 当 GitHub 因某一页的计算成本过高而返回 `RESOURCE_LIMITS_EXCEEDED` 时，会以一半大小重新请求该页。[tool-verified: `brands.py` `overload`, `executor.py` `_execute_connection`]

**嵌套对象。** GitHub 数据表使用自身的嵌套深度 0（此来源类型的 `max_object_depth=0`），而不是 `graphql_remote.max_object_depth`。嵌套对象列仅以其自身的标量字段选取；其内部的对象显示为 `__typename`。[tool-verified: `brands.py`]

**令牌存储。** 令牌存入密钥库，来源行保留一个引用，因此重启后会重新读取该来源，无需重新输入令牌。[tool-verified: `provisa/api/admin/graphql_remote_router.py` `_persist_source` docstring: "The credential goes to the org's vault and the row carries the reference"]

**运作方式（面向运维人员）。** GitHub 的模式随 Provisa 一同发布，因此添加来源不会发起内省调用，大型模式在注册时也不产生任何成本。数据表在注册时才从中逐个映射。刷新端点会拒绝此来源类型；新的 GitHub 模式会随 Provisa 版本发布而到来。[tool-verified: `brands.py` module docstring, `brand_schema`; REQ-1923 "there is no refresh" in `docs/arch/requirements.yaml` REQ-1875 supersession note][tool-verified: refresh handler returns code `graphql_remote.branded_source_not_refreshed`]

---

### GitLab（REQ-1923）

GitLab 是一种普通的来源类型，其添加与注册方式与 GitHub 相同：添加类型为 **GitLab** 的来源并提供访问令牌，然后从模式 `graphql` 中注册所需的数据表。数据表名称的默认前缀为 `gl`。该来源连接 `gitlab.com`。[tool-verified: `provisa/graphql_remote/brands.py` `BRANDS["gitlab"]`]

**选择列。** GitLab 会为每个查询定价，并拒绝成本过高的查询：匿名调用方为 200 点，带令牌为 250 点。选取全部列的宽数据表会超出此价格，因此请注册仅含所需列的 GitLab 数据表。[tool-verified: live against gitlab.com 2026-10-02, `project.issues` with all 63 columns answered "Query has complexity of 1733, which exceeds max complexity of 200"; with 14 chosen columns it registered and read]

- 注册数据表时，Provisa 会向 GitLab 询问一次，看其是否会在读取所用的页面大小下处理该选择。如果 GitLab 回应查询过于复杂或过大，该数据表不会被注册，结果会附上 GitLab 的消息：`Table 'gl__project_issues' was not registered with the columns selected: Query has complexity of 1733, which exceeds max complexity of 200. Choose fewer columns.`[tool-verified: `provisa/graphql_remote/probe.py` `QueryTooComplex`; `provisa/api/admin/_table_ops.py` code `schema.table_too_complex`]
- 一列的成本取决于其种类。普通值约为一点；嵌套对象列的成本是其许多倍。舍弃嵌套对象列最能节省成本。[tool-verified: live, five scalar columns scored 26 at 100 rows a page; two small object columns added 18]
- 页面大小是价格的一部分。它即 `graphql_remote.max_list_items`。[tool-verified: live, the same five columns scored 15 at 5 rows a page and 26 at 100]

**令牌校验。** GitLab 对无法识别的令牌返回空结果，而不是错误。Provisa 将其视为被拒绝的令牌，不会添加该来源。[tool-verified: `provisa/api/admin/graphql_remote_router.py` `_verify_live_auth`]

---

### gRPC 远程模式（REQ-322–329）

**如何添加来源。** 向 `/admin/grpc-remote/register` 发送 POST 请求，附上服务器地址、`.proto` 文件的路径或 URL，以及可选的 TLS 配置。添加来源不会注册任何数据表。

```json
{
  "source_id": "orders-grpc",
  "proto_path": "https://api.example.com/orders.proto",
  "server_address": "grpc.example.com:443",
  "namespace": "orders",
  "domain_id": "commerce",
  "tls": true,
  "cache_ttl": 300,
  "method_overrides": { "CreateOrder": "query" },
  "relationships": [
    { "source_table": "orders__OrderService__ListOrders", "source_column": "customer_id",
      "target_table": "customers__CustomerService__GetCustomer", "target_column": "id" }
  ]
}
```

Provisa 会提取 proto 文件，以纯文本解析器（解析时不依赖任何外部 proto 依赖项）进行解析，通过 `grpc_tools.protoc` 编译 Python stub，并打开一个持续存在的 `grpc.aio.Channel`。（REQ-322）[tool-verified: `provisa/grpc_remote/loader.py:99–128`, `provisa/grpc_remote/loader.py:166–214`, `provisa/api/admin/grpc_remote_router.py:80–104`]

Proto 文件也可为本地路径。常见类型（`google/protobuf/timestamp.proto`）的导入路径会于注册时存储，并于刷新时重复使用。（REQ-329）[tool-verified: `provisa/grpc_remote/loader.py:135–159`]

**来源提供的内容。** Proto 中的每个 `rpc` 方法均会依优先顺序使用三项信号分类为 query 或 mutation：（REQ-323）[tool-verified: `provisa/grpc_remote/mapper.py`]

1. **注册载荷中的 `method_overrides`**——`{"MethodName": "query"}` 或 `{"MethodName": "mutation"}` 优先于其他一切。
2. **`server_streaming: true`**——服务器发送消息流；恒为虚拟数据表（除非输出为标量）。
3. **输出消息带有重复的消息类型字段**——例如 `ListOrdersResponse { repeated Order items; }` 会被视为列表包装并成为虚拟数据表。重复的标量字段（例如 `repeated string tags`）不会触发此规则——它们是单一实体的数组属性，并非行数据来源。

不符合以上任何信号的方法（返回单一实体消息的一元 RPC，或任何标量输出）会成为受跟踪的函数。

**注册数据表。** 每个 query 方法都会作为一张数据表提供，名称为 `{namespace}__{Service}__{Method}`，位于选择器模式 `grpc_remote` 之下。通过 Register Table 选择器注册所需的数据表（`availableTables`、`availableColumns`、`registerTable`，与 GraphQL 来源相同），并选择响应列。请求字段会成为 `_nf_*` 原生过滤列，且始终包含在内。（REQ-322）[tool-verified: `provisa/api/admin/introspect.py` `_native_tables_grpc` (`if schema_name != "grpc_remote": return []`), `provisa/api/admin/grpc_remote_router.py` `query_table_name`, `query_columns`, `_register_schema` (`if table_name not in registered: continue`), `provisa/api/admin/_table_ops.py` `_grpc_columns_for_input`]

Mutation 方法是可提供的命令，计入 `available_mutations`；添加来源不会记录其中任何一个。gRPC mutation 在“命令”页面注册：先选择来源，再选择方法，方法名为 `Service.Method`；该命令的种类为 `source_operation`。参见[远程来源的写入操作](commands.md#a-remote-sources-write-operation-req-1924)。（REQ-1924） [tool-verified: `provisa/executor/source_operation.py` `grpc_operation_name`, `_grpc_operations`; `provisa/api/admin/actions_router.py` `_as_source_operation`]

**数据表命名。** 默认名称为 `{namespace}__{ServiceName}__{MethodName}`。若无命名空间，服务名称与方法名称会直接连接。任何已注册的数据表均可指定 `alias`；一旦设置，该别名将于各处使用（查询、SDL、关系）。自动生成的名称为注册键，永远不会改变。（REQ-322）[tool-verified: `provisa/core/repositories/table.py:129–134`]

**类型映射（REQ-324）。** Proto 标量类型与 SQL 类型的映射如下。[tool-verified: `provisa/grpc_remote/mapper.py:31–47`]

| Proto 类型 | SQL 类型 |
| --- | --- |
| `string`、`bytes` | `text` |
| `int32` / `uint32` / `sint32` / `fixed32` / `sfixed32` | `integer` |
| `int64` / `uint64` / `sint64` / `fixed64` / `sfixed64` | `bigint` |
| `float` | `real` |
| `double` | `numeric` |
| `bool` | `boolean` |
| `repeated <T>` | `jsonb` |
| 嵌套消息 | `jsonb` |
| Enum | `text` |

**注册时的关系。** `relationships` 的运作方式与 GQL 适配器相同——声明外键/主键连接路径，并存储为手动声明的关系（没有 `remote_managed` 标志）。刷新时，这些关系会保持不变。（REQ-554）[tool-verified: `provisa/api/admin/grpc_remote_router.py:93–109`]

**Query 方法（REQ-325）。** 输出消息的字段会成为数据表列。输入消息的字段既会成为传递至远程调用的 GraphQL 参数，*同时*也会注册为以 `_nf_` 为前缀、`native_filter_type: "grpc_input"` 的字段——此机制与 GQL 及 OpenAPI 用于原生过滤器注入的机制相同。（REQ-555）[tool-verified: `provisa/api/admin/grpc_remote_router.py:207–213`]

**嵌套消息的子字段。** 对于 query 方法，深度 0（直接输出列）的非重复消息类型字段，其子字段会解析多一层并存储为 `ColumnDef` 上的 `object_fields`。此元数据用于 SQL 中的 `jsonb` 子字段提取及模式文档。超出深度 1 的嵌套字段不会递归展开。（REQ-556）[tool-verified: `provisa/grpc_remote/mapper.py:111–128`]

服务器流式方法会先将所有流式消息收集成列表，再返回行数据。（REQ-325）[tool-verified: `provisa/grpc_remote/executor.py:86–119`]

**Mutation 方法（REQ-326）。** 已注册的 mutation 方法是一个命令，其参数即输入消息的字段，类型均为 `json`，并原样透传。远程服务的应答以行的形式返回；被拒绝的调用为 422，`functions.remote_refused`。参见[远程来源的写入操作](commands.md#a-remote-sources-write-operation-req-1924)。（REQ-1924） [tool-verified: `provisa/executor/source_operation.py` `_grpc_operations`, `_call_grpc`, `_refused`]

**通道管理。** 每个已注册来源会有一个 `grpc.aio.Channel`，存储于应用程序状态中并于后续请求重复使用。刷新时，旧通道会在新通道打开前关闭。（REQ-327）[tool-verified: `provisa/api/admin/grpc_remote_router.py:107–117`]

**刷新。** 向 `/admin/grpc-remote/refresh/{source_id}` 发送 POST 请求。此操作会从已存储的路径重新加载 proto，重新编译存根（stub），并使已注册的数据表与 proto 保持一致，沿用各数据表注册时所用的列。它不会注册任何新数据表；新增到 proto 的 query 方法仍处于可提供状态。也可向 `/admin/grpc-remote/{source_id}/proto` 发送 PUT 请求并附上新的 `proto_text`，以内联方式更新 proto。（REQ-329）[tool-verified: `provisa/api/admin/grpc_remote_router.py` `refresh_grpc_remote_source`, `_load_and_register` and `put_grpc_proto` (both pass `registered=await registered_query_tables(conn, source_id)`)]

**限制。**

- 对象子字段提取仅支持一层深度。超出深度 1 的嵌套消息字段不会递归展开。（REQ-556）[tool-verified: `provisa/grpc_remote/mapper.py:111–128`]

---

### OpenAPI / REST（REQ-314–321）

**如何添加来源。** 向 `/admin/openapi/register` 发送 POST 请求，附上来源 ID 及规范（从本地文件或 URL 加载）。规范会被解析并随来源保存；不会注册任何数据表或 command。响应返回 `tables: 0` 与 `mutations: 0`，可提供的数量则在 `available_tables` 与 `available_mutations` 中。（REQ-314）[tool-verified: `provisa/openapi/loader.py:30–55`, `provisa/api/admin/openapi_router.py` `_load_and_register` docstring: "Tables and functions are NOT auto-registered here. Users register them individually via the Register Table / Register Action UI."]

**注册数据表。** 通过 Register Table 选择器注册所需的每个 GET 操作（`availableTables`、`availableColumns`、`registerTable`），并选择列。每个非 GET 操作则在“命令”页面逐个注册为命令，由 `availableFunctions` 列出；参见[远程来源的写入操作](commands.md#a-remote-sources-write-operation-req-1924)。`PUT /admin/openapi/spec/{source_id}` 会存储手工编辑的规范，不注册任何内容，并返回 `available_tables` 与 `available_mutations`。（REQ-316） [tool-verified: `provisa/api/admin/openapi_router.py` `put_openapi_spec`; `provisa/api/admin/schema_query.py` `available_functions` ("returns non-GET operations")] [tool-verified: `provisa/api/admin/_table_ops.py` `_build_columns_for_input`; a registered OpenAPI table is read through the operation in the stored spec, `provisa/api/data/materialization.py` (`state.openapi_specs`)]

**注册载荷。** `/admin/openapi/register` 端点除了 `source_id`、`spec_path` 等字段外，还接受两个额外字段：

```json
{
  "operation_overrides": { "createPet": "query", "listOrders": "mutation" },
  "relationships": [
    { "source_table": "pets__listPets", "source_column": "owner_id",
      "target_table": "owners__listOwners", "target_column": "id" }
  ]
}
```

**来源提供的内容。** 规范中的每个 GET 操作都会作为数据表提供，除非其响应模式是标量类型（`string`、`number`、`boolean`、`integer`）——返回标量的 GET 操作则是只有单个 `value` 列的函数。每个非 GET 操作（POST、PUT、PATCH、DELETE）都会作为命令提供，以其 `operationId` 命名。注册后，它接受该操作的路径参数，以及用于请求体的 `body` 参数，类型均为 `json`；其余任何参数都放在查询字符串中。参见[远程来源的写入操作](commands.md#a-remote-sources-write-operation-req-1924)。（REQ-316、REQ-317、REQ-1924） [tool-verified: `provisa/executor/source_operation.py` `_openapi_operations`, `_call_openapi`]

分类优先顺序：`operation_overrides`（载荷）优先于 `x-provisa-kind`（规范扩展），而 `x-provisa-kind` 又优先于 GET 启发式规则。`operation_overrides` 为推荐的覆盖途径；`x-provisa-kind` 则适用于须由规范本身承载分类信息的情况。（REQ-408）[tool-verified: `provisa/openapi/mapper.py:192–203`]

**注册时的关系。** `relationships` 的运作方式与其他适配器相同——存储为手动声明的关系，并于刷新时予以保留。（REQ-554）[tool-verified: `provisa/api/admin/openapi_router.py:103–108`]

**数据表命名。** 数据表使用操作的 `operationId`。若未定义 `operationId`，Provisa 会将 `{method}_{path}` 转为 slug。别名的推导方式为移除开头的动词片段并将名词转为单数（`findPetsByStatus` → `pet_by_status`）。（REQ-557）[tool-verified: `provisa/openapi/register.py:39–56`]

**类型映射。** JSON Schema 类型与 Provisa 类型的映射如下。[tool-verified: `provisa/openapi/register.py:59–70`]

| JSON Schema 类型 | Provisa 类型 |
| --- | --- |
| `string` | `string` |
| `integer` | `integer` |
| `number` | `number` |
| `boolean` | `boolean` |
| `array` | `jsonb` |
| `object` | `jsonb` |

**作为原生过滤器字段的参数。** 尚未属于响应字段的路径及查询参数，会成为 `native_filter_type` 设为 `path_param` 或 `query_param`、并以 `_nf_` 为前缀的字段。当参数名称与响应字段名称相符时，该参数的元数据会并入已有的字段项，而非另建重复项。（REQ-555）[tool-verified: `provisa/openapi/register.py:116–122`, `provisa/openapi/register.py:172–196`]

**响应模式的解析。** 映射器会依序检查 `responses.200`、`responses.2xx`，再检查 `responses.default`。数组类型的响应会展开至其元素模式。`$ref` 引用会解析至一层深度。（REQ-316）[tool-verified: `provisa/openapi/mapper.py:83–101`]

**对象子字段。** 带有 `type: object` 且自身具有 `properties` 的响应属性，会存储为该字段上的 `object_fields`。这些子字段于 SDL 中可见，并用于查询中的 `jsonb` 提取。（REQ-556）[tool-verified: `provisa/openapi/register.py:87–96`]

**响应缓存（REQ-318）。** GET 操作的结果会由 `pg_cache.py` 缓存于 PostgreSQL 中。每种请求参数组合均拥有其专属的 `_params_hash` 分组。当 TTL 到期时，特定哈希值的行数据会被替换。带路径参数的端点（`/pets/{id}`）会跳过初始批量提取——缓存数据表会先创建为空以供模式内省之用，再依主键于请求到达时逐步填充。[tool-verified: `provisa/openapi/pg_cache.py:181–234`, `provisa/openapi/pg_cache.py:307–360`]

**刷新（REQ-321）。** 向 `/admin/openapi/refresh/{source_id}` 发送 POST 请求。通过 `_load_and_register` 重新解析规范，该函数不注册任何内容：它不会添加任何数据表或列。已有的治理规则将予以保留。[tool-verified: `provisa/api/admin/openapi_router.py` `refresh_openapi_source`, `_load_and_register`]已注册的表保留其列；它通过刷新后 spec 中的操作读取。 [tool-verified: `provisa/api/admin/openapi_router.py` `_load_and_register` (replaces `state.openapi_specs[source_id]`), `provisa/api/data/materialization.py`]

**限制。**

- 对象子字段提取仅支持一层深度。`object_fields` 中嵌套的属性不会递归展开。（REQ-556）[tool-verified: `provisa/openapi/register.py:87–96`]
- 请求头及 Cookie 参数会被忽略；只有 `path` 及 `query` 参数会被注册。（REQ-555）[tool-verified: `provisa/openapi/mapper.py:144–158`]
- 规范层级的 `$ref` 解析对于属性模式仅支持一层深度；深层嵌套的组件引用可能无法解析。[tool-verified: `provisa/openapi/mapper.py:51–60`]

---

## 注册远程数据表的影响

从任何远程模式来源注册的数据表，均为一级的 Provisa 数据表。在运行时，它与本地连接的关系型数据表在待遇上并无任何区别。（REQ-308、REQ-313）

**查询接口。** 该数据表可立即通过 GraphQL、SQL（pgwire 或直接连接）、Cypher（GQL）、JSON:API 及 Arrow Flight 进行查询。（REQ-001、REQ-267、REQ-345、REQ-257、REQ-051）由于远程数据表没有目录，模式生成过程会为其合成 `ColumnMetadata`——类型映射是于模式构建时应用的。（REQ-602）[tool-verified: `provisa/api/app.py:1367–1386`]

**安全模型。** 所有五层治理规则均适用：

1. 域访问控制——数据表的 `domain_id` 决定哪些角色可以查看它。（REQ-039）[tool-verified: `provisa/compiler/schema_gen.py:1064–1076`]
2. 行级安全（RLS）——不论接口为何，数据表上设置的行过滤器均会注入每项查询中。（REQ-040、REQ-041）
3. 字段可见性——每个字段的 `visible_to` 列表控制按角色而定的字段暴露。（REQ-039）
4. 字段脱敏——脱敏规则于治理流程的第二阶段应用。（REQ-040、REQ-263）
5. 谓词防护——已脱敏的字段会于 WHERE 及 HAVING 子句中被拒绝。（REQ-603）

针对远程数据表的即席查询仅依用户本身的权限予以允许——访问方式统一以权限为基础（数据表/字段权限加上已批准的关系），并无按数据表而异的治理模式。（REQ-001、REQ-003）

**关系治理（V002）。** 针对远程数据表的 JOIN 条件——当通过 SQL 或 Cypher 查询时——必须符合一项已注册并已批准的关系。（REQ-604）由于 SDL 定义的关系依设计已预先批准，GraphQL 查询会跳过 V002 检查。详见 [docs/security.md](security.md#v002)。

**OBJECT 类型字段。** 当字段映射至未受治理的内嵌 GQL OBJECT 或 OpenAPI 对象类型时，其 Provisa 类型为 `jsonb`。该字段会存储完整的嵌套 JSON blob。当声明了子字段（`gql_object_fields` 或 `object_fields`）时，`gql_object_columns` 映射表会于模式构建时填充。当查询选取这些子字段时，SQL 生成器会使用此映射表发出 `->>` 提取表达式。[tool-verified: `provisa/api/app.py:1305–1315`, `provisa/compiler/schema_gen.py:80–82`]

**作为原生过滤器参数的必填参数。** 带有非空值、无默认值参数的根查询字段，会为已注册数据表注入额外字段。这些字段带有 `native_filter_type: query_param`。Cypher 转译器会将 `WHERE n.id = $val` 重写为 `WHERE n._nf_id = $val`，而 GraphQL 执行器则会将其识别为要传递至远程端点的变量。（REQ-555）[tool-verified: `provisa/api/app.py:1280–1303`]

---

## 建立覆盖性关系的影响

当数据管家于两个远程数据表之间（或于一个远程数据表与一个本地数据表之间）注册一项关系时，该关系即成为查询时所使用的联接路径。

**联接如何取得优先。** 于查询编译阶段，Provisa 会通过已注册的关系解析联接路径。该关系的 `source_column` 及 `target_column` 会成为生成 SQL 中的联接条件。联接会取代原本针对已连接类型所需的、按数据表逐一发出的远程调用。

**原始 blob 永远不会于 SQL 中暴露。** `petstore__pets` 上的 `breed` 字段无法于 SQL 查询中作为原始 jsonb 值选取。当 `petstore__pets` 与 `petstore__breeds` 之间已注册一项关系时，SQL 查询会经由联接解析——`SELECT breed.name FROM petstore__pets` 是通过外键联接解析，而非通过 blob。若未注册任何关系，但该字段带有已声明的子字段（`gql_object_fields`），则 SQL 中对子字段的引用会被重写为对已存储 blob 的 `->>` 提取。此路径仅适用于未受治理的内嵌类型——受治理目标类型的字段完全从 SDL 中排除，并无 blob 可供提取。原始 blob 本身永远不会以裸字段值的形式输出。[tool-verified: `provisa/compiler/sql_gen.py:1156`, `tests/unit/test_sql_gen.py:TestGqlJsonBlobExtraction`]

于 GraphQL SDL 中，未受治理的内嵌 OBJECT 字段会被定型为该嵌套对象类型。至于它究竟是于运行时通过联接或通过 blob 提取来提供服务，属于实现细节——两种情况下的 SDL 形状均相同。当子类型被注册为其独立数据表（因而成为受治理类型）时，五层治理规则会独立应用于其上：其自身的 RLS 规则、字段可见性、脱敏规则、谓词防护及域访问控制。（REQ-039、REQ-040、REQ-041、REQ-263）Blob 提取则会绕过此机制——子项数据会以预先内嵌的形式随父行数据一并到达，并仅受父数据表规则的治理。将子项注册为数据表并建立关系，是对子类型实现精细治理的途径。

**关系上的 `graphql_alias`。** `graphql_alias` 字段会为关系于父类型上暴露的 SDL 字段命名。若缺省，其名称会依目标数据表的 `field_name` 及该关系的基数，通过 `rel_field_name(target.field_name, cardinality)` 推导而来。（REQ-605）[tool-verified: `provisa/compiler/schema_gen.py:1050`]

**联接路径上的 V002。** 凡经由 SQL 及 Cypher 遍历该关系的查询，均须受 V002 关系治理规范。该关系必须已注册并获批准，方可允许进行联接。（REQ-604）通过 SDL 关系字段进行的 GraphQL 遍历则恒为预先批准。[tool-verified: `docs/security.md:41–54`]

**remote-managed 标志。** 于 GraphQL 远程模式注册期间自动检测的关系，会以 `remote_managed: True` 存储。（REQ-554）[tool-verified: `provisa/graphql_remote/mapper.py:199`] 这是一个元数据标记，并不会改变治理行为。

---

## 仅供类型定义的行为

并非远程模式中的每种类型都必须成为可查询的数据表。

当 `SchemaInput` 上设置了 `root_table_ids` 时，ID 不在该集合中的数据表会从生成 SDL 的根查询字段中排除。它们仍会以 GraphQL 类型的形式存在，并可通过具有根条目的数据表上的关系字段加以访问。（REQ-601）[tool-verified: `provisa/compiler/schema_gen.py:1062–1069`]

相同机制也适用于按域过滤的模式构建：位于角色无法访问的域中的数据表，仅属类型定义——其类型定义存在于 SDL 中以供关系遍历之用，但不会为其生成任何根查询字段。（REQ-039）[tool-verified: `provisa/compiler/schema_gen.py:1068–1076`]

仅供类型定义的数据表具备以下特性：

- 没有根查询字段——客户端无法直接按名称查询它。
- 可通过具有根条目的数据表上的关系字段加以访问。
- 仍会于模式内省中以具名类型的形式出现。
- 当通过关系访问数据时，仍会应用所有治理规则。（REQ-039、REQ-040）

只有在数据表的注册被完全删除时，才会从模式中完全移除——包括其类型定义。将数据表标记为仅供类型定义（通过从 `root_table_ids` 中移除其 ID，或按域访问权进行过滤）并不会移除该类型。

此设计让数据管家能够公开可导航的对象图，其中部分类型仅可通过遍历访问，而非独立查询。
