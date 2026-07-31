# 远程模式

远程模式来源将外部 API——GraphQL、gRPC 或 REST（OpenAPI）——连接至 Provisa 语义层。注册后，外部 API 的操作即成为一级的 Provisa 数据表及函数。（REQ-308、REQ-316、REQ-325）所有治理规则、查询接口及安全层均会自动应用。（REQ-310、REQ-319、REQ-328）远程服务永远不会看到 Provisa 的治理规则。（REQ-310、REQ-319、REQ-328）

---

## 三种来源类型

### GraphQL 远程模式（REQ-307–313）

**如何注册。** 向 `/admin/sources/graphql-remote` 发送 POST 请求，带上端点 URL、命名空间及可选的认证设置。Provisa 会向远程端点发出标准的 `__schema` 内省查询。（REQ-307）[tool-verified: `provisa/graphql_remote/introspect.py:47–59`]

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

**自动发现的内容。** 远程 `Query` 类型上每个返回 OBJECT 的字段都会成为虚拟数据表。远程 `Mutation` 类型上每个字段都会成为受跟踪的函数。（REQ-308）[tool-verified: `provisa/graphql_remote/mapper.py:243–278`]

**数据表命名。** 数据表命名为 `{namespace}__{field_name}`。以命名空间 `petstore` 及查询字段 `pets` 为例：数据表名称为 `petstore__pets`。（REQ-312）[tool-verified: `provisa/graphql_remote/mapper.py:250`]

**类型映射（REQ-308）。** 标量字段会直接映射至 Provisa 类型。OBJECT 字段则按目标类型是否受治理而分为两种情况（见下方"受治理数据表"）。[tool-verified: `provisa/graphql_remote/mapper.py:14–36`、`provisa/api/data/endpoint.py:655–671`、`provisa/compiler/schema_gen.py:481–485`]

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

当受治理数据表上的 OBJECT 类型字段指向另一个受治理类型时，该字段会同时受三项规则约束 [tool-verified: `provisa/api/data/endpoint.py:655–671`、`provisa/compiler/schema_gen.py:481–485`]：

1. **从 GQL 提取中排除**——提取父数据表的行数据时，不会请求该字段。
2. **从 SDL 中排除**——该字段不会出现在生成模式中的父类型上。
3. **仅可通过已声明的关系访问**——数据管家必须在两个已物化的受治理数据表之间注册 JOIN。若无此关系，该字段纯粹缺失；并无 blob 回退方案。

无法作为根 Query 字段访问的 OBJECT 类型（例如 `ContactInfo` 或 `Address` 等内嵌类型）遵循不同的规则：它们会以 `jsonb` blob 列的形式提取，并于 SDL 中呈现为嵌套对象字段。子字段可通过 SQL 中的 `-->>` 提取访问。

**必填参数。** 当根查询字段带有非空值、无默认值的参数时，这些参数会成为数据表上的 `native_filter_type: query_param` 字段（于注入时加上 `_nf_` 前缀）。执行器会将其作为 GraphQL 变量传递。（REQ-555）[tool-verified: `provisa/graphql_remote/mapper.py:110–120`、`provisa/api/app.py:1280–1303`]

**自动检测的关系。** Provisa 会扫描每个数据表中 OBJECT 类型的字段。当被引用的 GQL 类型也已于同一来源中注册为数据表时，即会产生一项关系。多对一关系会依命名约定推断来源及目标列（来源类型上的 `breedName` → 目标类型 `Breed` 上的 `name`）。一对多（LIST）字段所产生的关系，其列引用为空——外键位于目标一方。（REQ-554）[tool-verified: `provisa/graphql_remote/mapper.py:162–202`]

**Mutation。** Mutation 字段会产生受跟踪的函数，其参数类型依 mutation 的参数映射而来，`return_schema` 则依 mutation 的返回类型推导。（REQ-308）[tool-verified: `provisa/graphql_remote/mapper.py:261–278`]

**刷新。** 向 `/admin/sources/graphql-remote/{id}/refresh` 发送 POST 请求。此操作会重新对远程模式进行内省，并更新数据表及函数的注册信息。已有的治理规则（RLS、脱敏）将予以保留。（REQ-311）[tool-verified: `provisa/api/admin/graphql_remote_router.py:217–257`]

**限制。**

- 标量及 ENUM 类型的根查询字段（返回类型非 OBJECT）会成为受跟踪的函数，而非虚拟数据表。其 `return_schema` 为单一字段 `value`，类型为对应的标量类型。[tool-verified: `provisa/graphql_remote/mapper.py:254–279`]
- 对象嵌套结构于注册时会解析至 `graphql_remote.max_object_depth`（默认值：5）的深度。远程提取的字段选择及子字段元数据均会构建至此深度；超出限制的字段不会被提取，也无法用于 SQL 提取。（REQ-556）[tool-verified: `provisa/graphql_remote/mapper.py:38–52`]
- LIST 类型的嵌套 OBJECT 字段（例如 `breed.awards: [Award]`）会于提取选择中纳入，直至 `graphql_remote.max_list_depth` 个嵌套层级（默认值：2）。于此限制内，列表会以 `jsonb` 数组的形式提取至父字段，而 GQL 选择会注入 `first: N`（N 为 `graphql_remote.max_list_items`，默认值：100），以限制数组大小。超出 `max_list_depth` 时，该 LIST 字段会完全被排除，以防止数据无限膨胀。在 SQL 中，可通过 `json_array_elements(column_name)` 或以 `->>` 进行索引提取来访问该数组。若列表的元素类型本身具有根查询，建议将其另行注册为独立数据表并建立关系——联接路径效率更高，也可绕过 blob。（REQ-556）[tool-verified: `provisa/graphql_remote/mapper.py:43–70`]
- 对于 SQL 查询，未受治理的 OBJECT 类型字段会从远程来源完整提取（所有子字段至配置深度为止），并以 `jsonb` 形式缓存。SQL 中对子字段的访问是通过对 blob 进行 `->>` 提取来处理；远程请求不会限缩为 SQL 查询所选取的字段。当列表的元素类型没有根查询，且 blob 表示法不敷使用时，应直接以 GraphQL SDL 编写查询——Provisa 会忠实地重现 GQL 字段选择，令远程来源仅接收到确切请求的字段。[tool-verified: `provisa/compiler/sql_gen.py:1332–1368`]
- 若远程服务器因需要子字段选择而拒绝某个 OBJECT 类型字段（在 `gql_selection` 可用时理应不会发生此情况），执行器会移除该等字段后重试一次，以确保标量字段仍可正常返回。[tool-verified: `provisa/graphql_remote/executor.py:76–80`]

---

### gRPC 远程模式（REQ-322–329）

**如何注册。** 向 `/admin/grpc-remote/register` 发送 POST 请求，带上服务器地址、`.proto` 文件的路径或 URL，以及可选的 TLS 设置。

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

Provisa 会提取 proto 文件，以纯文本解析器（解析时不依赖任何外部 proto 依赖项）进行解析，通过 `grpc_tools.protoc` 编译 Python stub，并打开一个持续存在的 `grpc.aio.Channel`。（REQ-322）[tool-verified: `provisa/grpc_remote/loader.py:99–128`、`provisa/grpc_remote/loader.py:166–214`、`provisa/api/admin/grpc_remote_router.py:80–104`]

Proto 文件也可为本地路径。常见类型（`google/protobuf/timestamp.proto`）的导入路径会于注册时存储，并于刷新时重复使用。（REQ-329）[tool-verified: `provisa/grpc_remote/loader.py:135–159`]

**自动发现的内容。** Proto 中的每个 `rpc` 方法均会依优先顺序使用三项信号分类为 query 或 mutation：（REQ-323）[tool-verified: `provisa/grpc_remote/mapper.py`]

1. **注册载荷中的 `method_overrides`**——`{"MethodName": "query"}` 或 `{"MethodName": "mutation"}` 优先于其他一切。
2. **`server_streaming: true`**——服务器发送消息流；恒为虚拟数据表（除非输出为标量）。
3. **输出消息带有重复的消息类型字段**——例如 `ListOrdersResponse { repeated Order items; }` 会被视为列表包装并成为虚拟数据表。重复的标量字段（例如 `repeated string tags`）不会触发此规则——它们是单一实体的数组属性，并非行数据来源。

不符合以上任何信号的方法（返回单一实体消息的一元 RPC，或任何标量输出）会成为受跟踪的函数。

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

**Mutation 方法（REQ-326）。** 输入消息的字段会成为 mutation 输入参数。输出消息的模式则会成为 `return_schema`。[tool-verified: `provisa/grpc_remote/executor.py:122–143`]

**通道管理。** 每个已注册来源会有一个 `grpc.aio.Channel`，存储于应用程序状态中并于后续请求重复使用。刷新时，旧通道会在新通道打开前关闭。（REQ-327）[tool-verified: `provisa/api/admin/grpc_remote_router.py:107–117`]

**刷新。** 向 `/admin/grpc-remote/refresh/{source_id}` 发送 POST 请求。此操作会从已存储的路径重新加载 proto、重新编译 stub，并重新注册数据表及函数。另外，也可向 `/admin/grpc-remote/{source_id}/proto` 发送 PUT 请求，并附上新的 `proto_text` 以内联方式更新 proto。（REQ-329）[tool-verified: `provisa/api/admin/grpc_remote_router.py:241–268`、`provisa/api/admin/grpc_remote_router.py:300–358`]

**限制。**

- 对象子字段提取仅支持一层深度。超出深度 1 的嵌套消息字段不会递归展开。（REQ-556）[tool-verified: `provisa/grpc_remote/mapper.py:111–128`]

---

### OpenAPI / REST（REQ-314–321）

**如何注册。** 调用 `auto_register_openapi_source`，并带上来源标识符、已解析的规范及连接元数据。此规范可从本地文件或 URL 加载。（REQ-314）[tool-verified: `provisa/openapi/loader.py:30–55`、`provisa/openapi/register.py:249–264`]

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

**自动发现的内容。** 规范中每个 GET 操作都会成为虚拟数据表，除非其响应模式属标量类型（`string`、`number`、`boolean`、`integer`）——返回标量的 GET 操作则会成为带有单一 `value` 字段的受跟踪函数。每个非 GET 操作（POST、PUT、PATCH、DELETE）都会成为受跟踪的函数。（REQ-316、REQ-317）

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

**作为原生过滤器字段的参数。** 尚未属于响应字段的路径及查询参数，会成为 `native_filter_type` 设为 `path_param` 或 `query_param`、并以 `_nf_` 为前缀的字段。当参数名称与响应字段名称相符时，该参数的元数据会并入已有的字段项，而非另建重复项。（REQ-555）[tool-verified: `provisa/openapi/register.py:116–122`、`provisa/openapi/register.py:172–196`]

**响应模式的解析。** 映射器会依序检查 `responses.200`、`responses.2xx`，再检查 `responses.default`。数组类型的响应会展开至其元素模式。`$ref` 引用会解析至一层深度。（REQ-316）[tool-verified: `provisa/openapi/mapper.py:83–101`]

**对象子字段。** 带有 `type: object` 且自身具有 `properties` 的响应属性，会存储为该字段上的 `object_fields`。这些子字段于 SDL 中可见，并用于查询中的 `jsonb` 提取。（REQ-556）[tool-verified: `provisa/openapi/register.py:87–96`]

**响应缓存（REQ-318）。** GET 操作的结果会由 `pg_cache.py` 缓存于 PostgreSQL 中。每种请求参数组合均拥有其专属的 `_params_hash` 分组。当 TTL 到期时，特定哈希值的行数据会被替换。带路径参数的端点（`/pets/{id}`）会跳过初始批量提取——缓存数据表会先创建为空以供模式内省之用，再依主键于请求到达时逐步填充。[tool-verified: `provisa/openapi/pg_cache.py:181–234`、`provisa/openapi/pg_cache.py:307–360`]

**刷新（REQ-321）。** 重新解析规范并再次调用 `auto_register_openapi_source`。已有的治理规则会予以保留；注册信息会以 ON CONFLICT upsert 方式更新。[tool-verified: `provisa/openapi/register.py:249–264`]

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

**OBJECT 类型字段。** 当字段映射至未受治理的内嵌 GQL OBJECT 或 OpenAPI 对象类型时，其 Provisa 类型为 `jsonb`。该字段会存储完整的嵌套 JSON blob。当声明了子字段（`gql_object_fields` 或 `object_fields`）时，`gql_object_columns` 映射表会于模式构建时填充。当查询选取这些子字段时，SQL 生成器会使用此映射表发出 `->>` 提取表达式。[tool-verified: `provisa/api/app.py:1305–1315`、`provisa/compiler/schema_gen.py:80–82`]

**作为原生过滤器参数的必填参数。** 带有非空值、无默认值参数的根查询字段，会为已注册数据表注入额外字段。这些字段带有 `native_filter_type: query_param`。Cypher 转译器会将 `WHERE n.id = $val` 重写为 `WHERE n._nf_id = $val`，而 GraphQL 执行器则会将其识别为要传递至远程端点的变量。（REQ-555）[tool-verified: `provisa/api/app.py:1280–1303`]

---

## 建立覆盖性关系的影响

当数据管家于两个远程数据表之间（或于一个远程数据表与一个本地数据表之间）注册一项关系时，该关系即成为查询时所使用的联接路径。

**联接如何取得优先。** 于查询编译阶段，Provisa 会通过已注册的关系解析联接路径。该关系的 `source_column` 及 `target_column` 会成为生成 SQL 中的联接条件。联接会取代原本针对已连接类型所需的、按数据表逐一发出的远程调用。

**原始 blob 永远不会于 SQL 中暴露。** `petstore__pets` 上的 `breed` 字段无法于 SQL 查询中作为原始 jsonb 值选取。当 `petstore__pets` 与 `petstore__breeds` 之间已注册一项关系时，SQL 查询会经由联接解析——`SELECT breed.name FROM petstore__pets` 是通过外键联接解析，而非通过 blob。若未注册任何关系，但该字段带有已声明的子字段（`gql_object_fields`），则 SQL 中对子字段的引用会被重写为对已存储 blob 的 `->>` 提取。此路径仅适用于未受治理的内嵌类型——受治理目标类型的字段完全从 SDL 中排除，并无 blob 可供提取。原始 blob 本身永远不会以裸字段值的形式输出。[tool-verified: `provisa/compiler/sql_gen.py:1156`、`tests/unit/test_sql_gen.py:TestGqlJsonBlobExtraction`]

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
