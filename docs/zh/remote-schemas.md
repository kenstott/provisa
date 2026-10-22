# 远程模式 {: #remote-schemas }

远程模式数据源把外部 API（GraphQL（包括 GitHub）、gRPC 或 REST（OpenAPI））接入 Provisa 模型。添加数据源不会注册任何表。数据源提供可选的表，由数据管家在 Register Table 选择器中逐个注册所需的表；这个注册就是筛选整理的步骤。已注册的表是完整的 Provisa 表，所以每条治理规则、每种查询接口和每个安全层都会自动作用于它。远程服务永远看不到 Provisa 的治理规则。

---

## 三种来源类型 {: #three-source-types }

### GraphQL 远程模式 {: #graphql-remote-schema }

**如何添加来源。** 向 `/admin/sources/graphql-remote` 发送 POST 请求，附上端点 URL、命名空间及可选的认证信息。Provisa 会对远程端点发起标准的 `__schema` 内省查询，以确认端点及凭据。

添加来源不会注册任何数据表或 command。每种远程来源类型在添加和刷新时都返回相同的计数：`tables`（已注册或已更新至最新的数据表；添加时为 0）、`available_tables`（可提供的数据表）、`mutations`（始终为 0）及 `available_mutations`（可提供的 command）。

**注册数据表。** 打开 Tables，再打开 Register Table，选择来源及模式 `graphql`，然后选择所需的数据表和列。通过管理端 GraphQL API：`availableTables(sourceId, schemaName)` 列出可提供的数据表，`availableColumns` 列出某张数据表的列，`registerTable(input: TableInput)` 按所选的列注册一张数据表。已注册的数据表随即受到治理。

已注册数据表的读取方式（根字段、行路径、必填参数、分页参数）存储于 `sources.mapping["tables"]`，因此重启后的进程无需向远程服务查询其模式即可读取。

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

认证选项：`none`、`bearer`（Authorization 头）、`basic`（Base64 编码的用户名:密码）。

**字段覆盖。** `field_overrides` 是一个 `{fieldName: "query" | "mutation"}` 映射表，于内省后应用，其优先于结构性分类。只有 query 类型的字段可重新分类为 mutation；mutation 类型的字段在 GraphQL 中没有覆盖路径。

**注册时的关系。** `relationships` 于注册时声明数据表之间的外键/主键连接路径，并存储为手动声明的关系（没有 `remote_managed` 标志）。刷新时，自动检测的关系（带有 `remote_managed: True` 的）会重新执行并可能改变；手动声明的关系则不受影响。

**来源提供的内容。** 远程 `Query` 类型上每个返回对象或对象列表的字段都会作为数据表提供，单对象字段下的每个 Relay 连接亦然（见下文）。注册某个已提供的数据表即使其成为数据表。远程 `Mutation` 类型上的每个字段都是一个可提供的命令，计入 `available_mutations`；添加来源不会注册其中任何一个。将所需的注册为命令；参见[远程来源的写入操作](commands.md#a-remote-sources-write-operation)。

**数据表命名。** 数据表命名为 `{namespace}__{field_name}`。以命名空间 `petstore` 及查询字段 `pets` 为例：数据表名称为 `petstore__pets`。

**Relay 连接。** 许多 API 以 Relay 连接返回列表：一个对象，带有 `nodes`（或 `edges { node }`）以及 `pageInfo`。Provisa 将连接映射为其节点的数据表，并逐页读取。

- 返回连接的根字段（`securityAdvisories`）会成为其节点的一张数据表。
- 根字段所返回的单个对象上的连接会成为独立的数据表。该数据表沿用根字段的必填参数。对于 `repository(owner, name)` 及 `Repository` 上的连接 `issues`，数据表为 `repositoryIssues`，在命名空间 `gh` 下的 SQL 名称为 `gh__repository_issues`。可通过 `_nf_owner` 和 `_nf_name` 列对其筛选：`WHERE _nf_owner = 'acme' AND _nf_name = 'widgets'`。
- 连接绝不会成为列。否则每一行都要为其类型所拥有的每个连接携带一次由远程逐行计算的读取。
- 仅当连接字段接受 `first` 和 `after`、从而可逐页读取时，它才是数据表。需要自身参数的连接不是数据表。联合类型（union）的连接，以及返回列表的根字段之下的任何连接，也都不是。

**类型映射。** 标量字段会直接映射至 Provisa 类型。OBJECT 字段则按目标类型是否受治理而分为两种情况（见下方"受治理数据表"）。

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

**受治理数据表。** 若 GQL 类型在远程模式中以 `Query` 的根字段形式出现，即属受治理类型。`_collect_queryable_types` 会于注册期间收集这些类型，并优先选取没有必填参数的字段，使其可作为联接目标进行批量提取。

当受治理数据表上的 OBJECT 类型字段指向另一个受治理类型时，该字段会同时受三项规则约束：

1. **从 GQL 提取中排除**——提取父数据表的行数据时，不会请求该字段。
2. **从 SDL 中排除**——该字段不会出现在生成模式中的父类型上。
3. **仅可通过已声明的关系访问**——数据管家必须在两个已物化的受治理数据表之间注册 JOIN。若无此关系，该字段纯粹缺失；并无 blob 回退方案。

无法作为根 Query 字段访问的 OBJECT 类型（例如 `ContactInfo` 或 `Address` 等内嵌类型）遵循不同的规则：它们会以 `jsonb` blob 列的形式提取，并于 SDL 中呈现为嵌套对象字段。子字段可通过 SQL 中的 `->>` 提取访问。

**需要参数的字段不是列。** 带有必填参数的字段无法直接选取，因此会被排除在数据表的列及嵌套选择之外。

**必填参数。** 当根查询字段带有非空值、无默认值的参数时，这些参数会成为数据表上的 `native_filter_type: query_param` 字段（于注入时加上 `_nf_` 前缀）。执行器会将其作为 GraphQL 变量传递。

**自动检测关系。** Provisa 会扫描每张已注册表中 OBJECT 类型的列。当某列引用的 GQL 类型也是同一数据源中已注册的表，并且承载该关系的列属于已注册的列时，就会保存该关系。尚未注册的表不会获得关系。多对一关系根据命名约定推断源列和目标列（源类型上的 `breedName` → 目标类型 `Breed` 上的 `name`）。一对多（LIST）字段生成的关系列引用为空，因为外键位于目标一侧。

**Mutation。** mutation 字段会被逐个注册为 `source_operation` 种类的命令。其参数的类型均为 `json`，并作为类型化变量传给远程服务；应答即远程服务返回的 JSON，没有 `return_schema`。参见[远程来源的写入操作](commands.md#a-remote-sources-write-operation)。

**刷新。** 向 `/admin/sources/graphql-remote/{id}/refresh` 发送 POST 请求。此操作会重新对远程模式进行内省，并使已注册的数据表与之保持一致。它不会添加任何数据表或列：模式新增的数据表或列仍处于可提供状态，模式已删去的列则会被移除。已有的治理规则（RLS、脱敏）将予以保留。

**限制。**

- 标量及 ENUM 类型的根查询字段（返回类型非 OBJECT）会成为受跟踪的函数，而非虚拟数据表。其 `return_schema` 为单一字段 `value`，类型为对应的标量类型。
- 对象嵌套结构于注册时会解析至 `graphql_remote.max_object_depth`（默认值：5）。远程获取的选择集与子字段元数据均构建至此深度；超出限制的字段不会被获取，也无法用于 SQL 提取。沿任一路径，同一类型只会进入一次：类型已在向下路径上的字段会被排除，因此类型相互引用的模式每个类型只遍历一次，而不是每个深度层级遍历一次。
- LIST 类型的嵌套 OBJECT 字段（例如 `breed.awards: [Award]`）会被纳入获取选择集，最多嵌套 `graphql_remote.max_list_depth` 层（默认值：2）。在此限制内，列表会作为父列上的 `jsonb` 数组获取。当列表字段声明了 `first` 参数（Relay、PostGraphile、pg_graphql）或 `limit` 参数（Hasura）时，选择集会将其作为 `first: N` 或 `limit: N` 传入，其中 N 为 `graphql_remote.max_list_items`（默认值：100）。两者都未声明的列表字段不会获得参数，因为远程服务会拒绝字段未声明的参数。超出 `max_list_depth` 后，该 LIST 字段将被完全排除，以防止数据无限膨胀。在 SQL 中，数组可通过 `json_array_elements(column_name)` 或使用 `->>` 的索引提取来访问。若列表的元素类型自带根查询，应改为将其注册为独立数据表并建立关系——联接路径更高效，且可绕过 blob。
- 对于 SQL 查询，未受治理的 OBJECT 类型字段会从远程来源完整提取（所有子字段至配置深度为止），并以 `jsonb` 形式缓存。SQL 中对子字段的访问是通过对 blob 进行 `->>` 提取来处理；远程请求不会限缩为 SQL 查询所选取的字段。当列表的元素类型没有根查询，且 blob 表示法不敷使用时，应直接以 GraphQL SDL 编写查询。Provisa 会忠实地重现 GQL 字段选择，令远程来源仅接收到确切请求的字段。
- 若远程服务器因需要子字段选择而拒绝某个 OBJECT 类型字段（在 `gql_selection` 可用时理应不会发生此情况），执行器会移除该等字段后重试一次，以确保标量字段仍可正常返回。此规则适用于从根字段读取的数据表。连接数据表不走此路径。

**分页读取。** 连接数据表通过游标读取。每一页请求 `first: N, after: $pageCursor` 及 `pageInfo { hasNextPage endCursor }`，读取会沿 `endCursor` 继续，直至远程服务报告没有下一页。

| 设置 | 默认值 | 作用 |
| --- | --- | --- |
| `graphql_remote.max_list_items` | `100` | 每页行数。 |
| `graphql_remote.max_rows` | `10000` | 对一张连接数据表的一次读取最多取得的行数。达到该值的读取会停止并记录一条警告。 |

```yaml
graphql_remote:
  max_list_items: 100
  max_rows: 10000
```

有两种响应会使执行器重试：

- **页面过重。** 当远程服务返回 502 或 504 时，会以一半大小重新请求同一页，最小降至一行。
- **带等待时间的速率限制。** 当远程服务返回 403 或 429，且 `Retry-After` 不超过 120 秒时，执行器会等待相应时间后重新发送请求，最多三次。没有 `Retry-After`，或要求更长等待时间的拒绝，会作为错误抛出。此规则适用于每一次读取，无论是否为连接。

响应中的任何其他错误都会使读取失败，除非来源类型另有规定（见下文 GitHub）。父级返回 null 的连接没有任何行。

---

### GitHub {: #github }

GitHub 是一种普通的来源类型。其 API 为 GraphQL，因此其数据表的行为如上所述，包括 `gh__repository_issues` 这类连接数据表。

**添加来源。**

1. 打开 Sources，添加类型为 **GitHub** 的来源。
2. 输入 GitHub 访问令牌。可选择输入命名空间，即数据表名称的前缀；默认值为 `gh`。
3. 保存。Provisa 会向 GitHub 校验该令牌。被 GitHub 拒绝的令牌会使添加失败，并附上 GitHub 的消息。

添加来源不会注册任何数据表。

**注册数据表。** 打开 Tables，再打开 Register Table。选择 GitHub 来源，选择模式 `graphql`，然后选择所需的数据表。GitHub 提供的每张数据表都会列出；注册即由您决定公开哪些。

**令牌范围（scope）。** 注册数据表时，Provisa 会用您的令牌向 GitHub 校验一次。

- 令牌范围未涵盖的字段会被排除在数据表之外。结果会列出每个被排除的字段：`Left out, because the source's credential may not read them: projectsV2`。
- 令牌完全无法读取的数据表会被拒绝，并附上 GitHub 给出的原因：`GitHub does not let this source's credential read gh__repository_issues: ...`。

**令牌无权查看的行。** 对于令牌无权在某一特定行中查看的字段（例如无推送权限的仓库的协作者），GitHub 返回 `FORBIDDEN`；对于仅存在于组织所有的仓库的字段，则返回 `NOT_ORG_OWNED_REPO`。该字段在该行中为 null，其余读取照常进行，Provisa 会记录一条警告。针对数据表本身的错误会使读取失败。

**过重的页面。** 当 GitHub 因某一页的计算成本过高而返回 `RESOURCE_LIMITS_EXCEEDED` 时，会以一半大小重新请求该页。

**嵌套对象。** GitHub 表使用自己的嵌套深度 0（此数据源类型的 `max_object_depth=0`），而不是 `graphql_remote.max_object_depth`。嵌套对象列只选取其自身的标量字段，其中的对象显示为 `__typename`。

**令牌存储。** 令牌存入密钥库，来源行保留一个引用，因此重启后会重新读取该来源，无需重新输入令牌。

**运作方式（面向运维人员）。** GitHub 的模式随 Provisa 一同发布，因此添加来源不会发起内省调用，大型模式在注册时也不产生任何成本。数据表在注册时才从中逐个映射。刷新端点会拒绝此来源类型；新的 GitHub 模式会随 Provisa 版本发布而到来。

---

### GitLab {: #gitlab }

GitLab 是一种普通的来源类型，其添加与注册方式与 GitHub 相同：添加类型为 **GitLab** 的来源并提供访问令牌，然后从模式 `graphql` 中注册所需的数据表。数据表名称的默认前缀为 `gl`。该来源连接 `gitlab.com`。

**选择列。** GitLab 会为每个查询定价，并拒绝成本过高的查询：匿名调用方为 200 点，带令牌为 250 点。选取全部列的宽数据表会超出此价格，因此请注册仅含所需列的 GitLab 数据表。

- 注册数据表时，Provisa 会向 GitLab 询问一次，看其是否会在读取所用的页面大小下处理该选择。如果 GitLab 回应查询过于复杂或过大，该数据表不会被注册，结果会附上 GitLab 的消息：`Table 'gl__project_issues' was not registered with the columns selected: Query has complexity of 1733, which exceeds max complexity of 200. Choose fewer columns.`
- 一列的成本取决于其种类。普通值约为一点；嵌套对象列的成本是其许多倍。舍弃嵌套对象列最能节省成本。
- 页面大小是价格的一部分。它即 `graphql_remote.max_list_items`。

**令牌检查。** GitLab 对无法识别的令牌返回空结果，而不是报错。Provisa 会把这视为令牌被拒绝，不添加该数据源。

---

### gRPC 远程模式 {: #grpc-remote-schema }

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

Provisa 会提取 proto 文件，以纯文本解析器（解析时不依赖任何外部 proto 依赖项）进行解析，通过 `grpc_tools.protoc` 编译 Python stub，并打开一个持续存在的 `grpc.aio.Channel`。

Proto 文件也可为本地路径。常见类型（`google/protobuf/timestamp.proto`）的导入路径会于注册时存储，并于刷新时重复使用。

**来源提供的内容。** Proto 中的每个 `rpc` 方法均会依优先顺序使用三项信号分类为 query 或 mutation：

1. **注册载荷中的 `method_overrides`**——`{"MethodName": "query"}` 或 `{"MethodName": "mutation"}` 优先于其他一切。
2. **`server_streaming: true`**——服务器发送消息流；恒为虚拟数据表（除非输出为标量）。
3. **输出消息带有重复的消息类型字段**——例如 `ListOrdersResponse { repeated Order items; }` 会被视为列表包装并成为虚拟数据表。重复的标量字段（例如 `repeated string tags`）不会触发此规则，因为它们是单一实体的数组属性，并非行数据来源。

不符合以上任何信号的方法（返回单一实体消息的一元 RPC，或任何标量输出）会成为受跟踪的函数。

**注册数据表。** 每个 query 方法都会作为一张数据表提供，名称为 `{namespace}__{Service}__{Method}`，位于选择器模式 `grpc_remote` 之下。通过 Register Table 选择器注册所需的数据表（`availableTables`、`availableColumns`、`registerTable`，与 GraphQL 来源相同），并选择响应列。请求字段会成为 `_nf_*` 原生过滤列，且始终包含在内。

Mutation 方法是可提供的命令，计入 `available_mutations`；添加来源不会记录其中任何一个。gRPC mutation 在“命令”页面注册：先选择来源，再选择方法，方法名为 `Service.Method`；该命令的种类为 `source_operation`。参见[远程来源的写入操作](commands.md#a-remote-sources-write-operation)。

**数据表命名。** 默认名称为 `{namespace}__{ServiceName}__{MethodName}`。若无命名空间，服务名称与方法名称会直接连接。任何已注册的数据表均可指定 `alias`；一旦设置，该别名将于各处使用（查询、SDL、关系）。自动生成的名称为注册键，永远不会改变。

**类型映射。** Proto 标量类型与 SQL 类型的映射如下。

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

**注册时的关系。** `relationships` 的作用与 GQL 适配器相同：声明 FK/PK 联接路径，保存为手工声明的关系（没有 `remote_managed` 标记）。刷新时它们保持不变。

**Query 方法。** 输出消息的字段会成为数据表列。输入消息的字段既会成为传递至远程调用的 GraphQL 参数，*同时*也会注册为以 `_nf_` 为前缀、`native_filter_type: "grpc_input"` 的字段，此机制与 GQL 及 OpenAPI 用于原生过滤器注入的机制相同。

**嵌套消息的子字段。** 对于 query 方法，深度 0（直接输出列）的非重复消息类型字段，其子字段会解析多一层并存储为 `ColumnDef` 上的 `object_fields`。此元数据用于 SQL 中的 `jsonb` 子字段提取及模式文档。超出深度 1 的嵌套字段不会递归展开。

服务器流式方法会先将所有流式消息收集成列表，再返回行数据。

**Mutation 方法。** 已注册的 mutation 方法是一个命令，其参数即输入消息的字段，类型均为 `json`，并原样透传。远程服务的应答以行的形式返回；被拒绝的调用为 422，`functions.remote_refused`。参见[远程来源的写入操作](commands.md#a-remote-sources-write-operation)。

**通道管理。** 每个已注册来源会有一个 `grpc.aio.Channel`，存储于应用程序状态中并于后续请求重复使用。刷新时，旧通道会在新通道打开前关闭。

**刷新。** 向 `/admin/grpc-remote/refresh/{source_id}` 发送 POST 请求。此操作会从已存储的路径重新加载 proto，重新编译存根（stub），并使已注册的数据表与 proto 保持一致，沿用各数据表注册时所用的列。它不会注册任何新数据表；新增到 proto 的 query 方法仍处于可提供状态。也可向 `/admin/grpc-remote/{source_id}/proto` 发送 PUT 请求并附上新的 `proto_text`，以内联方式更新 proto。

**限制。**

- 对象子字段提取仅支持一层深度。超出深度 1 的嵌套消息字段不会递归展开。

---

### OpenAPI / REST {: #openapi-rest }

**如何添加来源。** 向 `/admin/openapi/register` 发送 POST 请求，附上来源 ID 及规范（从本地文件或 URL 加载）。规范会被解析并随来源保存；不会注册任何数据表或 command。响应返回 `tables: 0` 与 `mutations: 0`，可提供的数量则在 `available_tables` 与 `available_mutations` 中。

**注册数据表。** 通过 Register Table 选择器注册所需的每个 GET 操作（`availableTables`、`availableColumns`、`registerTable`），并选择列。每个非 GET 操作则在“命令”页面逐个注册为命令，由 `availableFunctions` 列出；参见[远程来源的写入操作](commands.md#a-remote-sources-write-operation)。`PUT /admin/openapi/spec/{source_id}` 会存储手工编辑的规范，不注册任何内容，并返回 `available_tables` 与 `available_mutations`。

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

**数据源提供什么。** 规范中的每个 GET 操作都作为表提供，除非其响应模式是标量类型（`string`、`number`、`boolean`、`integer`）；返回标量的 GET 是只有一个 `value` 列的函数。其他每个操作（POST、PUT、PATCH、DELETE）都作为命令提供，以其 `operationId` 命名。注册后，它接受操作的路径参数和一个用于请求体的 `body` 参数，类型均为 `json`，其他参数放在查询字符串中。参见[远程来源的写入操作](commands.md#a-remote-sources-write-operation)。

分类优先顺序：`operation_overrides`（载荷）优先于 `x-provisa-kind`（规范扩展），而 `x-provisa-kind` 又优先于 GET 启发式规则。`operation_overrides` 为推荐的覆盖途径；`x-provisa-kind` 则适用于须由规范本身承载分类信息的情况。

**注册时的关系。** `relationships` 的作用与其他适配器相同：保存为手工声明的关系，刷新时保留。

**数据表命名。** 数据表使用操作的 `operationId`。若未定义 `operationId`，Provisa 会将 `{method}_{path}` 转为 slug。别名的推导方式为移除开头的动词片段并将名词转为单数（`findPetsByStatus` → `pet_by_status`）。

**类型映射。** JSON Schema 类型与 Provisa 类型的映射如下。

| JSON Schema 类型 | Provisa 类型 |
| --- | --- |
| `string` | `string` |
| `integer` | `integer` |
| `number` | `number` |
| `boolean` | `boolean` |
| `array` | `jsonb` |
| `object` | `jsonb` |

**参数作为原生过滤列。** 尚未成为响应字段的路径参数和查询参数会变成列，其 `native_filter_type` 为 `path_param` 或 `query_param`，并带 `_nf_` 前缀。当参数与某个响应字段同名时，其元数据会合并到该列的条目中，不会产生重复。

**响应模式的解析。** 映射器会依序检查 `responses.200`、`responses.2xx`，再检查 `responses.default`。数组类型的响应会展开至其元素模式。`$ref` 引用会解析至一层深度。

**对象子字段。** 带有 `type: object` 且自身具有 `properties` 的响应属性，会存储为该字段上的 `object_fields`。这些子字段于 SDL 中可见，并用于查询中的 `jsonb` 提取。

**响应缓存。** GET 操作的结果会由 `pg_cache.py` 缓存于 PostgreSQL 中。每种请求参数组合均拥有其专属的 `_params_hash` 分组。当 TTL 到期时，特定哈希值的行数据会被替换。带路径参数的端点（`/pets/{id}`）会跳过初始批量提取。缓存数据表会先创建为空以供模式内省之用，再依主键于请求到达时逐步填充。

**刷新。** 向 `/admin/openapi/refresh/{source_id}` 发送 POST 请求。通过 `_load_and_register` 重新解析规范，该函数不注册任何内容：它不会添加任何数据表或列。已有的治理规则将予以保留。已注册的表保留其列；它通过刷新后 spec 中的操作读取。

**限制。**

- 对象子字段提取仅支持一层深度。`object_fields` 中嵌套的属性不会递归展开。
- 请求头及 Cookie 参数会被忽略；只有 `path` 及 `query` 参数会被注册。
- 规范层级的 `$ref` 解析对于属性模式仅支持一层深度；深层嵌套的组件引用可能无法解析。

---

## 注册远程数据表的影响 {: #impact-of-registering-a-remote-table }

从任何远程模式来源注册的数据表，都是普通的 Provisa 数据表。在运行时，它与本地连接的关系型数据表在待遇上并无任何区别。

**查询接口。** 该数据表可立即通过 GraphQL、SQL（pgwire 或直接连接）、Cypher（GQL）、JSON:API 及 Arrow Flight 进行查询。由于远程数据表没有目录，模式生成过程会为其合成 `ColumnMetadata`，类型映射是于模式构建时应用的。

**安全模型。** 所有五层治理规则均适用：

1. 域访问控制——数据表的 `domain_id` 决定哪些角色可以查看它。
2. 行级安全（RLS）——不论接口为何，数据表上设置的行过滤器均会注入每项查询中。
3. 字段可见性——每个字段的 `visible_to` 列表控制按角色而定的字段暴露。
4. 字段脱敏——脱敏规则于治理流程的第二阶段应用。
5. 谓词防护——已脱敏的字段会于 WHERE 及 HAVING 子句中被拒绝。

对远程表的即席查询只依据用户自己的权限放行。各处的访问都遵循同一套基于权限的模型（表和列的权限加上已批准的关系），没有按表设置的治理模式。

**关系治理（V002）。** 在 SQL 或 Cypher 查询中，针对远程表的 JOIN 条件必须与已注册并获批准的关系相符。GraphQL 查询会跳过 V002 检查，因为 SDL 中定义的关系按设计已获批准。参见 [docs/security.md](security.md#relationship-governance-v002)。

**OBJECT 类型字段。** 当字段映射至未受治理的内嵌 GQL OBJECT 或 OpenAPI 对象类型时，其 Provisa 类型为 `jsonb`。该字段会存储完整的嵌套 JSON blob。当声明了子字段（`gql_object_fields` 或 `object_fields`）时，`gql_object_columns` 映射表会于模式构建时填充。当查询选取这些子字段时，SQL 生成器会使用此映射表发出 `->>` 提取表达式。

**作为原生过滤器参数的必填参数。** 带有非空值、无默认值参数的根查询字段，会为已注册数据表注入额外字段。这些字段带有 `native_filter_type: query_param`。Cypher 转译器会将 `WHERE n.id = $val` 重写为 `WHERE n._nf_id = $val`，而 GraphQL 执行器则会将其识别为要传递至远程端点的变量。

---

## 建立覆盖性关系的影响 {: #impact-of-creating-a-covering-relationship }

当数据管家于两个远程数据表之间（或于一个远程数据表与一个本地数据表之间）注册一项关系时，该关系即成为查询时所使用的联接路径。

**联接如何取得优先。** 于查询编译阶段，Provisa 会通过已注册的关系解析联接路径。该关系的 `source_column` 及 `target_column` 会成为生成 SQL 中的联接条件。联接会取代原本针对已连接类型所需的、按数据表逐一发出的远程调用。

**原始 blob 永远不会在 SQL 中暴露。** 在 SQL 查询中无法把 `petstore__pets` 的 `breed` 列作为原始 jsonb 值选出。一旦在 `petstore__pets` 和 `petstore__breeds` 之间注册了关系，SQL 查询就会走联接：`SELECT breed.name FROM petstore__pets` 通过外键联接解析。如果没有注册关系，但该列声明了子字段（`gql_object_fields`），对子字段的 SQL 引用会改写为针对已存 blob 的 `->>` 提取。这条路径只适用于未受治理的内联类型；目标受治理的字段完全不出现在 SDL 中，也没有可提取的 blob。原始 blob 本身永远不会作为裸列值返回。

在 GraphQL SDL 中，未受治理的内联 OBJECT 字段的类型就是嵌套对象类型。执行时由联接还是由 blob 提取来提供数据，并不影响 SDL 的形状。一旦子类型注册为独立的表并因此受治理，五个治理层都会独立作用于它：它自己的 RLS 规则、列可见性、脱敏规则、谓词守卫和领域访问控制。blob 提取会绕过这一切，因为子数据嵌在父行中一起到达，只受父表规则约束。要对子类型做细粒度治理，请把它注册为表并创建关系。

**关系上的 `graphql_alias`。** `graphql_alias` 字段会为关系于父类型上暴露的 SDL 字段命名。若缺省，其名称会依目标数据表的 `field_name` 及该关系的基数，通过 `rel_field_name(target.field_name, cardinality)` 推导而来。

**联接路径上的 V002。** 凡经由 SQL 及 Cypher 遍历该关系的查询，均须受 V002 关系治理规范。该关系必须已注册并获批准，方可允许进行联接。通过 SDL 关系字段进行的 GraphQL 遍历则恒为预先批准。

**remote-managed 标志。** 于 GraphQL 远程模式注册期间自动检测的关系，会以 `remote_managed: True` 存储。 这是一个元数据标记，并不会改变治理行为。

---

## 仅供类型定义的行为 {: #type-def-only-behavior }

并非远程模式中的每种类型都必须成为可查询的数据表。

当 `SchemaInput` 上设置了 `root_table_ids` 时，ID 不在该集合中的数据表会从生成 SDL 的根查询字段中排除。它们仍会以 GraphQL 类型的形式存在，并可通过具有根条目的数据表上的关系字段加以访问。

按领域过滤构建模式时，同样的机制也适用。角色无权访问的领域中的表只保留类型定义：其类型定义存在于 SDL 中以便遍历关系，但不会为它们生成根查询字段。

仅供类型定义的数据表具备以下特性：

- 没有根查询字段，因此客户端无法直接按名称查询它。
- 可通过具有根条目的数据表上的关系字段加以访问。
- 仍会于模式内省中以具名类型的形式出现。
- 当通过关系访问数据时，仍会应用所有治理规则。

只有删除表的注册时，该表（包括类型定义）才会从模式中完全移除。把表变成只保留类型定义（从 `root_table_ids` 中移除其 ID，或通过领域访问过滤）并不会移除该类型。

这样，数据管家就能公开可导航的对象图，其中某些类型只能通过遍历到达，无法单独查询。
