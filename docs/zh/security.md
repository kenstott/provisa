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

**绕过机制**——V002 有两种绕过方式。第一种是一项功能：持有 `ignore_relationships` 的角色可以跨目录未涵盖的关系进行联接。在预置的系统角色中只有 `modeler` 持有它——这是负责确定模型而非强制执行模型的探索角色。（REQ-1297）`analyst` 并不持有。[tool-verified: `provisa/core/db.py:84`]

第二种是需同时成立两项条件的退出机制：

1. **角色标志**——角色定义中的`relationship_guard: false`（默认值：`true`）。[tool-verified: `provisa/core/models.py:349`]
2. **按查询退出**——SQL中包含`--relationship-guard=false`注释。[tool-verified: `provisa/compiler/params.py:80`]

仅靠角色标志无法绕过V002；仅靠注释也无法绕过V002。

**高安全模式将该防护固定。**在 `security.mode: high` 下两种绕过均不适用：`ignore_relationships` 被忽略，`relationship_guard: false` 被忽略，且每个联接都必须存在于已批准的关系目录中。（REQ-693）这是刻意的冗余——即便某个生产角色被误授予该功能，它仍然无法突破模型。[tool-verified: `provisa/pgwire/_pipeline.py:377`]

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
| `ignore_relationships` | 绕过关系治理（V002）。在系统角色中仅由 `modeler` 持有，且在高安全模式下被完全忽略 |
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
| `basic` | bcrypt 本地账户 + JWT | 自包含部署 |
| `firebase` | Firebase ID令牌 | 生产环境 |
| `keycloak` | Keycloak JWT | 企业版 |
| `oauth` | OIDC JWT | PingFed、Okta、Azure AD、Auth0 |
| `simple` | bcrypt + JWT | 测试 |

角色映射：通过可配置的规则，将身份声明映射至Provisa角色。（REQ-120）`assignments_source`字段控制角色分配的来源：`claims`会从JWT令牌的声明中读取（默认值）；`provisa`则会从Provisa内部的分配存储中读取。（REQ-551）

在`provisa.yaml`中配置的超级用户（用户名加上来自环境密钥的密码），无论配置何种提供程序，均一律获授予admin角色及全部能力——这是用于初始配置的引导路径。（REQ-125）

### 接口与凭据

每个接口都通过同一套提供程序契约进行认证，因此在一个接口上可用的凭据，只要协议能够承载，就在所有接口上可用。（REQ-124、REQ-1263）本表是唯一参考；各接口文档不再重复。

| 接口 | 密码 | 提供程序令牌 | 个人访问令牌 | 客户端证书（mTLS） |
| --------- | ---------- | ---------------- | ----------------------- | --------------------------- |
| HTTP（REST、JSON:API、GraphQL） | `Authorization: Basic` | `Authorization: Bearer` | `Authorization: Bearer` | 经由终止代理 |
| pgwire | 密码字段（明文或 SCRAM） | 密码字段，OIDC 部署 | 密码字段 | 是 |
| Bolt | `basic` 方案 | `bearer` 方案 | `bearer` 方案 | 是 |
| Arrow Flight | — | 握手或票据负载中的 `token` | 同上 | 是 |
| gRPC | — | `authorization` 元数据 | `authorization` 元数据 | 是 |
| MCP | — | `Authorization: Bearer` | `Authorization: Bearer` | 经由终止代理 |

单元格为 `—` 之处，表示该协议没有可与密码配对的用户名字段；这些情形由令牌形式覆盖。pgwire 则是镜像情形：启动包只有一个密钥字段且没有方案，因此密钥*是什么*决定了采用哪种方法——PAT 由其前缀识别，当配置的提供程序为令牌提供程序时该密钥按 bearer 令牌读取，其余一律视为密码。选择只做一次——被选定校验器拒绝的凭据不会再拿去试另一个。

该矩阵由 `tests/unit/test_auth_surface_conformance.py` 强制执行，它驱动每个接口真实的校验入口，并在新增接口而未添加对应行时失败。

### 个人访问令牌

PAT 是用户为无法完成交互式登录的客户端——脚本、BI 工具、驱动程序——铸造的长期 bearer 密钥。（REQ-1263）它自带组织与角色，且每个接口都通过同一个校验器解析它，因此任何接口都无需知道 PAT 为何物。

其传输形式为 `provisa_pat_` 后跟 43 个 URL 安全的 base64 字符。正是这个前缀把呈交的密钥路由到令牌存储而非身份提供程序，也使泄漏的令牌可在日志与代码库中被 grep 检出。

- **存储**——仅保留密钥的 SHA-256。密钥本身仅在创建时显示一次，且无法找回。列表中携带显示前缀与生命周期时间戳，绝不会是可用凭据。
- **签发与吊销**——`POST /auth/tokens`、`GET /auth/tokens`、`DELETE /auth/tokens/{token_hash}`，以及管理界面中用户自身资料页上的自助区域。铸造与吊销凭据是令牌持有者本人的行为。
- **归属**——通过校验的 PAT 解析为其所有者的账户：用户 id、电子邮件与显示名称。因此在 PAT 之下写入的审计行或使用报表指向的是人，而非凭据。该人的哪一个令牌参与了操作则单独记录于 `raw_claims["token_name"]`。
- **过期**——令牌可携带过期时间；已过期的令牌在校验时被拒。删除用户的成员资格会连同吊销其令牌。

### pgwire 上的 SCRAM-SHA-256

在 `basic` 提供程序下，设置 `auth.scram: true` 会让 pgwire 通告 SASL（认证码 10）并使用 `SCRAM-SHA-256` 机制，从而以证明密码取代发送密码。（REQ-1394）不提供通道绑定（`SCRAM-SHA-256-PLUS`）。

SCRAM 需要一个 RFC 5802 验证器，而它无法从 bcrypt 哈希推导得出。只要密码以明文经过——注册、登录、修改密码、管理员重置——就会写入一个验证器，因此开启 SCRAM 的部署会随着用户下一次认证逐步收集验证器，而每位用户的首次 SCRAM 连接紧随其下一次输入密码之后。对尚无验证器的用户，会以与真实交换无法区分的模拟交换作答，因此线路上不会泄露谁已完成迁移。

### 双向 TLS

客户端证书验证把第一道检查移到 TLS 握手：没有部署方 CA 签名证书的调用方永远到不了凭据层。（REQ-1228）它可用于 pgwire、Bolt、gRPC 与 Arrow Flight——这四种自行终止 TLS 的传输。

| 变量 | 含义 |
| ---------- | --------- |
| `PROVISA_MTLS_CLIENT_CA` | 允许签发客户端证书的 CA 的 PEM 包 |
| `PROVISA_MTLS_MODE` | `required`（设置 CA 后的默认值）或 `optional` |
| `PROVISA_MTLS_BIND_PRINCIPAL` | 为真时，证书的 common name 必须与该连接随后认证所用的用户名相同 |

各协议的覆盖设置沿用与 TLS 设置相同的命名。没有任何东西靠推断：设置了模式却未设置 CA 会拒绝启动，无法识别的模式也会拒绝启动，而不会被读作最接近的安全取值——一个自以为要求客户端证书而实际并未要求的部署，处境比启动失败的部署更糟。

### 登录限流

猜测密码与协议无关：同一个账户可以经由 HTTP、pgwire 与 Bolt 被反复轰击。因此计数器位于凭据校验层，而非任何单一接口，这样在任何地方触发的锁定都会处处生效。（REQ-1393）

它默认开启——五分钟内五次失败会将该主体锁定十五分钟——并在 `auth.login_throttle` 下调整。被锁定的主体在凭据被检查之前就已遭拒，而一次成功认证会清空该主体的历史。

键是协议所携带的 principal。仅支持 bearer 的接口不携带 principal，因此键是凭据自身的摘要；这样阻止的是同一个坏令牌被无限重放。该存储按进程划分，因此运行多个 API worker 的部署每个 worker 最多允许 `max_attempts` 次——限流是对猜测的刹车，不是分布式配额。

### 在传输协议上指定组织

在多租户下，组织通过主机名寻址：`acme.provisa.dev` 即组织 `acme`。在 HTTP 上该名称随 `Host` 标头到达。pgwire 或 Bolt 客户端不发送此类标头，但它确实会在 TLS ClientHello 中发送所拨的主机名，Provisa 便从中读取组织。（REQ-1234）客户端无需任何改动——连接到 `acme.provisa.dev` 即可。

主机名是一项请求，而非授予。它抵达的是与 `Host` 标头相同的解析器，该解析器会拒绝任何认证 principal 既非成员、也不持有跨组织权限的组织。拨向你并无成员资格的主机名，触及不到任何数据。以 IP 地址连接的客户端不发送主机名，仅从 principal 解析其组织——在单组织部署中，每条连接都是如此。

gRPC、Arrow Flight 与 MCP 把证书交给不暴露主机名回调的库；这些传输改用 `x-provisa-org` 元数据标头来指定组织。

## 高安全模式

`provisa.yaml` 中的 `security.mode: high` 主张一项保证：Provisa 后端绝不处理明文数据。（REQ-693）每个重要的列都在源端加密，只有持有解密密钥的客户端才能读取。这项保证带来的后果，部署方必须提前规划。

**该模式的作用：**

- **数据端点要求出示客户端解密的凭证。**`/data/` 之下的一切都返回 403，除非调用方带上 `X-Provisa-KMS-Key` 标头——这是配置为本地解密的 JDBC 或 Python 客户端的标记。浏览器或明文 REST 消费方不带此类密钥，会被拒绝。该关卡是对整棵树的默认拒绝：明天新增的路由在其发布当天即受管控，豁免则必须逐一论证。
- **模式元数据端点保持开放。**`/data/sdl`、`/data/introspection`、`/data/schema-version`、`/data/domains`、`/data/proto` 与 `/data/compile` 不返回行数据，而客户端在能够连接之前必须先读取模式——包括哪些字段带 `@encrypted`。
- **gRPC 与 Arrow Flight 在同一凭证要求下继续服务。**它们正是执行加密的客户端实际使用的传输；关闭它们会让高安全部署失去所有传输协议。在其中任一上的数据调用都必须以调用元数据携带同样的 KMS 密钥。
- **pgwire、Bolt 与 MCP 不会启动。**三者都没有能够承载解密上下文的逐连接握手：pgwire 行集与 Cypher 结果在线路上都是明文，而 MCP 工具调用会把结果以文本交给模型。为其中任一配置的端口在启动时会被拒绝而非提供服务。
- **关系防护无法绕过。**`ignore_relationships` 与 `relationship_guard: false` 均被忽略；参见[关系治理](#v002)。

**如何确认部署处于该模式：**启动日志会指明它；不带 KMS 密钥的 `/data/sql` 请求会以 403 应答并给出提及 REQ-693 的消息；pgwire、Bolt 与 MCP 端口未在监听。

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

完整的密钥服务——保管库、引用语法与提供程序——参见[密钥](secrets.md)。
