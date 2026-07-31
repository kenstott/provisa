# 安全模型

Provisa在所有查询语言（GraphQL、SQL、Cypher）及所有传输方式（REST、gRPC、Arrow Flight、JDBC、WebSocket）上，均实施多层次的安全模型。（REQ-001、REQ-266）治理统一应用——不存在任何可绕过治理的查询路径。（REQ-002、REQ-266）

各层按顺序应用。每个请求必须先通过每一层，才会评估下一层。

## 分层模型

### 第0层——内省过滤

呈现给某个角色的架构 (Schema) 及目录，只包含其`domain_access`清单中的数据表，以及通过逐列`visible_to`规则的列。（REQ-039）角色权限之外的对象，在发现阶段即不可见——无法查询、无法自动补全，也无法推断其存在。（REQ-039）此规则适用于GraphQL架构、SQL目录以及查询编辑器的架构浏览器。（REQ-039、REQ-363）

参阅[架构可见性](#_10)。

### 第1层——公开访问

没有`domain_access`限制的域中的数据表，无需额外配置即可供所有已通过身份验证的用户查看。对于真正公开的数据，完全没有阻力。

### 第2层——域访问

每个角色都有一份`domain_access`域ID清单。凡涉及该等域以外数据表的查询，均会在执行前被拒绝。（REQ-038、REQ-039）这是粗粒度的所有权边界——无论SQL如何编写，人力资源角色都无法访问财务数据表。（REQ-002）

参阅[权限模型](#_3)。

### 第3层——行级安全

域访问获确认后，系统会在执行时，将按数据表、按角色设定的`WHERE`谓词注入每个`SELECT`语句中。（REQ-041、REQ-263）该等谓词是针对原始数据进行评估的。即使使用`SELECT *`，查询共享订单表的区域经理也只会看到其所属区域的数据行。（REQ-264）

参阅[行级安全 (RLS)](#rls)。

### 第4层——列可见性及脱敏

`visible_to`清单中不包括请求角色的列，会从查询结果中剥离。（REQ-040、REQ-263）设有脱敏规则的列，其值会在结果离开服务器前被替换——方式包括正则表达式编修、常量替换或截断。（REQ-263）脱敏适用于所有查询语言及输出格式。（REQ-263）

参阅[列权限模型](#_5)及[列级脱敏](#_11)。

### 第5层——谓词防护

被脱敏的列会在`WHERE`及`HAVING`子句中被拒绝使用。（REQ-263）若无此防护，即使输出结果已脱敏，调用方仍可通过在筛选条件中进行二分查找，推断出未脱敏的值。此项拒绝会在查询解析阶段（执行前）强制执行。（REQ-531）

### 关系治理（V002）

SQL中的JOIN条件，必须匹配数据表之间已登记并获批准的关系。（REQ-001）未经批准的join会被拒绝。每个关系均附有人类可读的原因及描述——为用户及自主代理提供指引，说明某遍历路径存在的原因。这属于治理策略，而非硬性的安全边界：无论join结构如何，第2至5层依然有效，因此刻意的规避行为，并不会使角色接触到其原本无法通过两次独立查询获取的数据。规避尝试会被记录并可供审计。

**绕过机制**——只有在以下两项独立条件同时成立时，才可绕过V002：

1. **角色标志**——角色定义中的`relationship_guard: false`（默认值：`true`）。[tool-verified: `provisa/core/models.py:349`]
2. **按查询退出**——SQL中包含`--relationship-guard=false`注释。[tool-verified: `provisa/compiler/params.py:80`]

两者必须同时具备。仅靠角色标志无法绕过V002；仅靠注释也无法绕过V002。

**GraphQL路径**——对于GraphQL查询，V002会被无条件跳过。SDL中定义的关系，按设计已预先获批准；该项检查属于多余，因此不会执行。[tool-verified: `provisa/api/data/endpoint.py:468`]

**SQL及Cypher路径**——V002默认处于启用状态。`endpoint_dev.py`及`cypher_router.py`均会在调用`validate_sql`之前，执行两项条件的检查。[tool-verified: `provisa/api/data/endpoint_dev.py:127`、`provisa/api/rest/cypher_router.py:260`]

**pgwire路径**——与SQL相同的两项条件检查。`--relationship-guard=false`注释会在执行前从查询中剥离；不会传递到数据库。[tool-verified: `provisa/pgwire/_pipeline.py:60`]

---

这些层次会相互组合。同时具备域访问、RLS及脱敏列的角色，其五项约束会同时生效。新增数据源、列或关系，无需逐一更新所有规则——每一层均独立配置，并会自动应用于任何涉及受治理对象的查询。

---

## 权限模型

各项能力独立分配，并可通过`parent_role_id`实现可选的角色层级结构。`admin`授予全部能力。（REQ-042）

| 能力 | 说明 |
| ----------- | ------------- |
| `source_registration` | 注册数据源 |
| `table_registration` | 注册数据表、列 |
| `create_relationship` | 定义外键关系 |
| `access_config` | 配置RLS、脱敏 |
| `query_development` | 执行查询 |
| `write` | 调用已注册的变更操作（粗粒度控制；参阅“变更操作授权”） |
| `full_results` | 绕过采样限制 |
| `ignore_relationships` | 绕过关系治理（V002） |
| `admin` | 超级用户——授予全部能力 |

### 角色继承

角色可通过`parent_role_id`，从父角色继承能力及域访问权。（REQ-215）层级结构会在启动时被展平——子角色会将父角色的能力及域访问权，与自身的合并。（REQ-215）

```yaml
roles:
  - id: basic_user
    capabilities: [query_development]
    domain_access: [public]
  - id: analyst
    capabilities: [full_results]
    domain_access: [sales, analytics]
    parent_role_id: basic_user   # inherits query_development + public domain
```

## 列权限模型

每个列均设有由四个字段组成的权限模型，用于按角色控制读取、写入及脱敏访问权。（REQ-042、REQ-249）

### 三级可见性

| 级别 | 条件 | 结果 |
| ------ | ----------- | -------- |
| **隐藏** | 角色不在`visible_to`中 | 列不会出现在GraphQL SDL中 |
| **已脱敏** | 角色在`visible_to`中、设有脱敏规则、角色不在`unmasked_to`中 | 列可见，但SQL中数据已脱敏 |
| **未脱敏** | 角色同时在`visible_to`及`unmasked_to`中（或没有脱敏规则） | 完整读取访问权 |

### 写入权限

| 字段 | 空白表示 | 用途 |
| ------- | ------------ | --------- |
| `visible_to` | 所有角色均可读取 | 控制谁可以看到该列（已脱敏或未脱敏） |
| `unmasked_to` | 没有角色可以看到未脱敏的值 | 控制谁可以绕过脱敏 |
| `writable_by` | 没有角色可以写入 | 控制谁可以进行变更 (INSERT/UPDATE) |

写入权限会在变更操作管道中强制执行。不在`writable_by`中的角色，尝试写入受限列时会收到403错误。（REQ-033、REQ-034）

### 示例

```yaml
columns:
  - name: email
    visible_to: [admin, analyst, viewer]
    writable_by: [admin]
    unmasked_to: [admin]
    mask_type: regex
    mask_pattern: "(.).*@"
    mask_replace: "$1***@"
  - name: salary
    visible_to: [admin, hr]
    writable_by: [hr]
    unmasked_to: [admin, hr]
    mask_type: constant
    mask_value: "0"
  - name: created_at
    visible_to: []           # all can read
    writable_by: []          # nobody can write (auto-set)
```

在此示例中：

- `email`：admin可以看到`alice@example.com`并可编辑；analyst/viewer则会看到`a***@example.com`
- `salary`：admin及hr可以看到真实值；hr可以编辑；其余所有角色完全看不到此列
- `created_at`：所有人均可读取，任何人都不可写入

## 变更操作授权

已注册的变更操作（远程GraphQL、OpenAPI、gRPC、Hasura）须经过两项独立检查。（REQ-867、REQ-868）角色只有在同时具备全局`write`能力，并列于该项变更操作的`writable_by`清单中时，才可调用该操作。（REQ-868）空白的`writable_by`即代表默认拒绝——任何角色均不可调用。（REQ-867）

变更操作按合约分类为写入操作，而非按调用方的声明而定。（REQ-869）若`SELECT`语句引用了属于变更操作类型的函数，会被提升为写入操作，并须经过相同的两重把关检查，因此调用方无法通过将变更操作伪装为读取操作而绕过限制。（REQ-869）将某项变更操作重新分类为读取安全，须具备`access_config`能力，并会被记录为治理决定；并无按请求逐次退出的选项。（REQ-870）

## 架构可见性

按角色划分的GraphQL架构，会隐藏未经授权的内容：（REQ-039）

- **域访问**：角色只会看到其`domain_access`域内的数据表（`"*"` = 全部）（REQ-039）
- **列可见性**：对某角色而言不在`visible_to`中的列，会从SDL中省略（REQ-039）
- 未经授权的数据表／列，不会出现在架构中（REQ-039）

## 行级安全 (RLS)

按数据表、按角色注入SQL WHERE子句。此项操作在编译之后、执行之前进行。（REQ-041、REQ-263）

```yaml
rls_rules:
  - table_id: orders
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"
```

筛选条件会以AND方式并入查询的WHERE子句中。此机制同时适用于查询及变更操作 (UPDATE/DELETE)。（REQ-035、REQ-041）

## 列级脱敏

脱敏设置只需为每个列定义一次——这是列本身的属性，而非角色的属性。`unmasked_to`字段控制哪些角色可以绕过脱敏。（REQ-249）

| 脱敏类型 | 支持的类型 | SQL表达式 |
| ----------- | ---------------- | ---------------- |
| `regex` | 字符串 (varchar, char, text) | `REGEXP_REPLACE(col, pattern, replace)` |
| `constant` | 任意类型 | 字面量值 (NULL、0、自定义) |
| `truncate` | 日期／时间戳 | `DATE_TRUNC(precision, col)` |

脱敏会被下推到SQL SELECT投影中——由数据库直接返回已脱敏的数据。（REQ-263）对于已脱敏的角色而言，未脱敏的数据绝不会经网络传输。（REQ-263）已脱敏的列也会在`WHERE`及`HAVING`子句中被屏蔽（第5层谓词防护），以防止通过筛选推断出未脱敏的值。（REQ-263、REQ-531）

## 采样

除非具备`full_results`能力，否则所有角色看到的均为经采样的结果（默认：100行）。（REQ-554）可通过`PROVISA_SAMPLE_SIZE`环境变量控制。（REQ-554）

## 审计日志

任何涉及域资产的查询，均会记录在只可追加的`query_audit_log`中。（REQ-596、REQ-613）每行会捕获`tenant_id`、`user_id`、`role_id`、查询文本的SHA-256哈希值、`table_ids`、`source`、`status_code`、`duration_ms`及`logged_at`。（REQ-596）查询文本绝不会以原文存储——只会存储其哈希值。（REQ-596）

该日志在数据库层面属于只可追加：PostgreSQL规则会阻止`DELETE`及`UPDATE`。（REQ-596、REQ-613）两个索引——`(tenant_id, logged_at)`及`(user_id, logged_at)`——支持按租户范围及按用户的时间范围合规查询。（REQ-596、REQ-613）

启用加密后，查询文本哈希值所在的列会以加密方式存储，并只会在获授权的管理员读取时解密。（REQ-689）

## 速率限制

按角色设置的速率限制，会在`provisa.yaml`中配置：包括每秒最大请求数、最大并发SSE订阅数，以及最大并发Arrow Flight流数。（REQ-369）该等限制会在编译或执行之前，在API层强制执行；超出限制的请求会被拒绝，并返回HTTP 429及`Retry-After`标头。（REQ-369）

自然语言查询服务（`POST /query/nl`）另设有独立限制，通过`nl.rate_limit`（每分钟、每角色的请求数）控制。超出限制的请求会在调用任何LLM之前被拒绝。（REQ-370）

速率限制的状态存储于Redis（`cache.redis_url`）中，以滑动窗口计数器方式运作——并无按实例存储的状态——因此限制会在所有水平扩展的Provisa实例之间保持一致。（REQ-371）

## 身份验证

可插拔的身份验证提供程序：（REQ-120）

| 提供程序 | 令牌类型 | 使用场景 |
| ---------- | ----------- | ---------- |
| `none` | X-Provisa-Role标头 | 开发 |
| `firebase` | Firebase ID令牌 | 生产环境 |
| `keycloak` | Keycloak JWT | 企业版 |
| `oauth` | OIDC JWT | PingFed、Okta、Azure AD、Auth0 |
| `simple` | bcrypt + JWT | 测试 |

角色映射：通过可配置的规则，将身份声明映射至Provisa角色。（REQ-120）`assignments_source`字段控制角色分配的来源：`claims`会从JWT令牌的声明中读取（默认值）；`provisa`则会从Provisa内部的分配存储中读取。（REQ-551）

在`provisa.yaml`中配置的超级用户（用户名加上来自环境密钥的密码），无论配置何种提供程序，均一律获授予admin角色及全部能力——这是用于初始配置的引导路径。（REQ-125）

## ABAC批准钩子 (Hook)

可选的外部策略钩子，会在查询执行前触发。（REQ-203）配置此项功能后，Provisa会调用您的策略引擎，并传递用户身份、角色、数据表、列及操作类型。响应结果会决定该查询是否继续执行。（REQ-203）

### 适用范围

只有当查询涉及已设置范围的数据表或数据源时，该钩子才会触发——其余情况则完全没有额外开销。（REQ-204）

| 配置 | 效果 |
| -------- | -------- |
| `auth.approval_hook.scope: all` | 每个查询均会触发此钩子 |
| `sources[].approval_hook: true` | 该数据源上的所有数据表均会触发此钩子 |
| `tables[].approval_hook: true` | 该数据表会触发此钩子 |

### 协议

支持三种传输方式：（REQ-246）

| 类型 | 使用场景 | 配置字段 |
| ------ | ---------- | ------------- |
| `webhook` | 任何支持HTTP的策略服务（OPA、自定义） | `url` |
| `unix_socket` | 位于同一台机器上的OPA或策略边车 (sidecar) | `socket_path` + `url` |
| `grpc` | 同址部署、高吞吐量的策略服务 | `url` (host:port) |

gRPC传输方式采用`provisa/auth/approval.proto`中定义的`provisa.auth.ApprovalService`合约。请在您的策略引擎中实现此服务：（REQ-246）

```proto
service ApprovalService {
  rpc Evaluate (ApprovalRequest) returns (ApprovalResponse);
}

message ApprovalRequest {
  string user = 1;
  repeated string roles = 2;
  repeated string tables = 3;
  repeated string columns = 4;
  string operation = 5;
}

message ApprovalResponse {
  bool approved = 1;
  string reason = 2;
}
```

gRPC通道属于持久化连接——每个Provisa实例使用一条通道，并会在所有对该钩子端点的调用中被重复使用。（REQ-555）

### 请求／响应

三种传输方式均携带相同的载荷：（REQ-246）

| 字段 | 类型 | 说明 |
| ------- | ------ | ------------- |
| `user` | string | 已通过身份验证的用户身份 |
| `roles` | string[] | 用户的Provisa角色 |
| `tables` | string[] | 查询中引用的数据表ID |
| `columns` | string[] | 查询中选取的列 |
| `operation` | string | `"query"`或`"mutation"` |

webhook及Unix socket传输方式均以JSON交换数据。响应必须包含`approved`（布尔值），并可选择性包含`reason`（字符串）。（REQ-246）

### 超时及回退处理

```yaml
auth:
  approval_hook:
    type: grpc          # webhook | grpc | unix_socket
    url: "localhost:50051"
    timeout_ms: 500     # default 5000
    fallback: deny      # allow | deny — applied on timeout or error
    scope: ""           # "" = use per-table/per-source flags; "all" = every query
```

发生超时或传输错误时，会应用`fallback`策略。（REQ-247）熔断器 (circuit breaker)（默认：连续失败5次后开启，30秒后转为半开状态）可防止因钩子端点响应缓慢而引发的级联故障。（REQ-556）

### 配置示例

```yaml
auth:
  approval_hook:
    type: webhook
    url: "http://opa.internal:8181/v1/data/provisa/allow"
    timeout_ms: 300
    fallback: deny

sources:
  - id: analytics_pg
    approval_hook: true   # all tables on this source require hook approval

tables:
  - id: salary_data
    approval_hook: true   # this table always requires hook approval
```

## 密钥

凭据使用`${env:VAR_NAME}`语法，并在运行时解析。（REQ-557）密码绝不会存储在配置数据库中。（REQ-557）
