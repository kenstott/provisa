# 安全模型 {: #security-model }

Provisa 在所有查询语言（GraphQL、SQL、Cypher）和所有传输（REST、gRPC、Arrow Flight、JDBC、WebSocket）上执行分层安全模型。（REQ-001、REQ-266） 治理统一适用——不存在绕过它的查询路径。（REQ-002、REQ-266）

各层按顺序应用。请求必须通过每一层，下一层才会被评估。

## 分层模型 {: #layered-model }

### 第0层——内省过滤 {: #layer-0-introspection-filtering }

呈现给角色的 schema 和目录只包含其 `domain_access` 列表中的表，以及通过每列 `visible_to` 规则的列。（REQ-039） 角色访问范围之外的对象在发现时不可见——无法查询、自动补全或推断其存在。（REQ-039） 这适用于 GraphQL schema、SQL 目录以及查询编辑器中的 schema 浏览器。（REQ-039、REQ-363）

参见[架构可见性](#schema-visibility)。

### 第1层——公开访问 {: #layer-1-public-access }

以 `public` 范围发布的列会提供给组织中的每个角色，包括其 `domain_access` 未覆盖该表所在域的角色；这样的角色只能看到带有公开列的该表。在注册表的人明确说明之前，没有任何内容是公开的。（REQ-1959） 参见[公开列](#public-columns)。

### 第2层——域访问 {: #layer-2-domain-access }

每个角色携带一个由域 id 组成的 `domain_access` 列表。涉及这些域之外的表的查询会在执行前被拒绝。（REQ-038、REQ-039） 这是粗粒度的所有权边界——无论 SQL 如何编写，HR 角色都无法触及财务表。（REQ-002）

参见[权限模型](#rights-model)。

### 第3层——行级安全 {: #layer-3-row-level-security }

确认域访问之后，按表、按角色的 `WHERE` 谓词会在执行时注入每一个 `SELECT`。（REQ-041、REQ-263） 谓词针对数据源数据求值。查询共享订单表的区域经理即使使用 `SELECT *`，也只能看到其所在区域的行。（REQ-264）

参见[行级安全 (RLS)](#row-level-security-rls)。

### 第4层——列可见性及脱敏 {: #layer-4-column-visibility-and-masking }

`visible_to` 列表不包含请求角色的列会从查询输出中移除。（REQ-040、REQ-263） 带有脱敏规则的列，其值会在结果离开服务器之前被替换——通过正则编辑、常量替换或截断。（REQ-263） 脱敏适用于所有查询语言和输出格式。（REQ-263）

参见[列权限模型](#column-permission-model)和[列级脱敏](#column-level-masking)。

### 第5层——谓词防护 {: #layer-5-predicate-guard }

被脱敏的列在 `WHERE` 和 `HAVING` 子句中会被拒绝。（REQ-263） 否则，即使输出已脱敏，调用者也可以在过滤条件中通过二分查找推断出未脱敏的值。拒绝在解析查询时、执行之前发生。（REQ-531）

### 关系治理（V002） {: #relationship-governance-v002 }

SQL 的 JOIN 条件必须与表之间一个已注册且已批准的关系相匹配。（REQ-001） 未经批准的联接会被拒绝。每个关系都带有人类可读的原因和描述——为用户和自主智能体说明该遍历路径为何存在。这是一项治理策略，而不是硬性的安全边界：第 2–5 层的执行与联接结构无关，因此有意的绕过不会暴露该角色用两条独立查询无法获得的数据。绕过尝试会被记录并可审计。

**绕过机制**——V002 可以通过两种方式绕过。第一种是能力：持有 `ignore_relationships` 的角色可以联接目录未覆盖的关系。预置的系统角色中只有 `modeler` 拥有它——这是一个探索角色，其职责是定义模型，而不是执行模型。（REQ-1297） `analyst` 没有它。

第二种是双条件例外，两个条件必须同时满足：

1. **角色标志**——角色定义中的 `relationship_guard: false`（默认：`true`）。
2. **查询内例外**——SQL 包含注释 `--relationship-guard=false`。

仅有角色标志不会绕过 V002；仅有注释也不会绕过 V002。

**高安全模式锁定防护。** 在 `security.mode: high` 下，任何绕过都不起作用：`ignore_relationships` 被忽略，`relationship_guard: false` 被忽略，每个联接都必须存在于已批准关系目录中。（REQ-693） 这是有意的冗余——被误授予该能力的生产角色仍然无法跳出模型。

**GraphQL 路径**——GraphQL 查询总是跳过 V002。SDL 中定义的关系本质上已预先批准；该检查是冗余的，不会应用。

**SQL 和 Cypher 路径**——V002 默认启用。`endpoint_dev.py` 和 `cypher_router.py` 都会在调用 `validate_sql` 之前应用双条件检查。

**pgwire 路径**——与 SQL 相同的双条件检查。注释 `--relationship-guard=false` 会在执行前从查询中剥离；它不会到达数据库。

---

这些层相互叠加。对于同时具有域访问、行级安全和脱敏列的角色，所有五项限制会同时生效。添加数据源、列或关系不需要更新每条规则——每一层独立配置，并自动应用于任何触及受治理对象的查询。

---

## 权限模型 {: #rights-model }

可独立分配的能力，可通过 `parent_role_id` 构成可选的角色层级。`admin` 授予一切。（REQ-042）

| 能力 | 说明 |
| ----------- | ------------- |
| `source_registration` | 注册数据源 |
| `table_registration` | 注册表和列 |
| `create_relationship` | 定义外键关系 |
| `access_config` | 配置行级安全和脱敏 |
| `query_development` | 运行查询 |
| `write` | 调用已注册的变更（粗粒度控制；参见变更操作授权） |
| `full_results` | 绕过采样限制 |
| `ignore_relationships` | 绕过关系治理 (V002)。系统角色中只有 `modeler` 拥有它，在高安全模式下会被完全忽略 |
| `admin` | 超级用户——授予一切 |

### 角色继承 {: #role-inheritance }

角色可以通过 `parent_role_id`（在配置中设置，或在安全页面上设为“继承自”）从一个父角色继承。（REQ-215） 继承链在启动时展开：子角色获得其祖先的能力与域访问的并集、每一项指名某个祖先的列、指标、函数和 webhook 授权，以及祖先按表的行级安全规则，子角色优先——链中最近一个对某表有规则的角色提供该表的谓词，为子角色保存的规则会替换父角色对该表的规则。（REQ-1677）

```yaml
roles:
  - id: basic_user
    description: Queries the public domain.
    capabilities: [query_development]
    domain_access: [public]
  - id: analyst
    description: Analyses sales; reads full results.
    capabilities: [full_results]
    domain_access: [sales, analytics]
    parent_role_id: basic_user   # inherits query_development + public domain
```
## 角色：授予角色、派生角色和元角色 {: #roles-granted-derived-and-meta }

三种角色在 Provisa 的访问模型中协同工作。角色页面把前两种一起列出；元角色从不列出。

### 授予角色 {: #granted-roles }

授予角色直接分配给用户。它携带权利：能力、域访问、脱敏条目、行过滤。继承（`parent_role_id`）在启动时把祖先的权利折叠进子角色。权利只做加法，没有减法。要限制读取者，请授予一个同时指名角色和条件的复合条目，或把一条脱敏条目放在宽松条目之前。（REQ-215、REQ-1677）

### 派生角色 {: #derived-roles }

派生角色来自用户的属性。组织声明哪些属性会成为角色（`derived_roles`），每个都带有描述。对于每个已声明的属性 `a`，若用户的属性为其提供了值 `v`，则用户拥有父角色 `a` 及其值角色 `a__v`：`region = eu` 得到 `region` 和 `region__eu`。没有值，就两个都没有。

属性的解析很重要。组织自己关于成员的记录优先于身份提供程序给出的内容：两者指明同一属性时，使用组织的值，提供程序的值被标记为已覆盖。

派生角色不携带权利，也从不手动分配。授权列表、脱敏或行规则指名它们的方式与指名授予角色完全相同；请求在 `X-Provisa-Role` 中指名它们并以其身份执行。（REQ-2057）

在角色页面上，每个已声明的属性都是一个标记为*派生*的父角色，其值角色随着其值的出现列在它下面。**+ 角色 → 由属性派生**用于声明一个。它的面板显示该属性、其描述、迄今见到的值，以及指名它或其某个值角色的每条脱敏、行规则和授权。它没有授权控件。值角色的描述是自动生成的：“当用户的 region 为 eu 时拥有”。（REQ-2078、REQ-2079）

在配置中以列表形式声明它们：

```yaml
derived_roles:
- attribute: region
  description: The subsidiary a member works for; region__eu marks EU staff.
```

想想 JWT：它的 claim 是关于这个人的事实。派生角色把选定的事实变成角色，这样授权就能针对它们，而不必为每种组合创建一个角色。

### 元角色 {: #meta-roles }

当请求指名多个角色时，Provisa 会构建一个临时元角色，其标识为 `meta:<ids sorted, joined by +>`。它是所指名集合中每个角色的唯一子角色。整个管道随后像对待任何单一角色一样以它运行。

权利是每个成员权利的并集。行过滤是每个成员所允许过滤的 OR；若读取某表的任何成员没有过滤，元角色也没有——没有过滤意味着所有行。元角色从不保存、列出或导出。模型变更时，它会根据其成员重新构建。（REQ-1620）

每一行的列按照其行过滤允许该行的角色进行脱敏和显示：在这些角色中，采用对该列限制最少的读取方式，未允许该行的角色对其读取方式没有任何影响。脱敏在读取表的地方生效，先于任何连接或聚合，因此聚合读取的是脱敏后的值。写入以同样方式检查：只有当授予该列的角色允许某行时，该列才会在该行上被写入。（REQ-1620）

审计记录指明每个执行中的角色，而不是元角色 id。由于元角色从不保存，它没有自己的描述：凡是显示描述的地方，它的描述就是其成员的描述，每个 id 各带自己的描述。（REQ-2080）

### 描述 {: #descriptions }

每个角色和每条行规则都说明自己的用途。访问审查读取谁可以看到什么以及为什么。没有人能解释的角色或规则，也就没有人能审查，因此描述是必需的：描述为空的角色、派生父角色或行规则会被配置加载、管理 API 和 UI 拒绝，并给出原因。角色列表和行规则列表显示每条描述，其详情面板显示并编辑描述。（REQ-2080、REQ-2081）

一个角色只有一个描述，而不是每个域一个。如果一个通用角色没有描述它在某个特定域中的职责，就说明该域需要一个自己的角色，通常是通用角色的子角色（`parent_role_id`），它继承通用角色的权利，并描述它在那里做什么。

### 请求如何指名角色 {: #how-a-request-names-roles }

客户端在 `X-Provisa-Role` 中发送一个角色或逗号分隔的集合。所指名的每个角色都必须为用户所持有。未分配给该身份的角色会被点名拒绝。

权利和数据只跟随请求所执行的角色：持有一个角色不等于以它执行。一个角色就是该角色；两个或更多角色以它们的元角色执行。如果执行角色会带上所持有的每个角色的权利，它就没有意义，因此请求获得最小权限，审计记录准确指明实际执行的内容。

被指名的派生角色以限定符身份进入。限定符没有自己的权利、域访问、脱敏或行过滤，因此它不是元角色的权利成员：作为没有脱敏的成员，它会解除每一列的脱敏；作为不读取任何表的成员，它会终结任何行过滤的 OR。它只进入授权：指名 `region__eu` 的授权或脱敏条目，会触达同时指名该限定符和一个授予角色的请求。（REQ-2057）

MCP 代理从不超出赋予它的角色行事。远程代理只能指名其令牌主体所持有的角色。本地 stdio 代理只能指名 `mcp.role`（`PROVISA_MCP_ROLE`）中固定的集合内的角色，未指名任何角色时即以该集合行事；任何其他角色都会被点名拒绝。（REQ-2075）

### `+` 的含义 {: #what-means }

`+` 出现在两个地方，各有不同含义。

**在授权条目中。** `visible_to` 或 `to` 列表中的 `analyst+region__eu` 表示读取者同时持有全部——`analyst` 与 `region__eu` 同时持有。不是复合的条目就是一个角色。列表条目之间的规则是任一：读取者持有任一条目即拥有该授权。内部为 AND，之间为 OR。

**在元角色 id 中。** `meta:analyst+engineer` 是集合 {analyst, engineer} 的元角色在服务端的标识。它的权利是两者的并集。客户端从不直接指名元角色 id；它指名自己持有的角色，由服务器构建元角色。

### 将 ABAC 列规则化为简单的 RBAC {: #abac-column-rules-as-plain-rbac }

基于属性的策略问：“具有这些属性的这个用户能看到这一列吗？”RBAC 问：“哪些角色能看到这一列？”复合授权和派生角色把它变成同一个问题。

“仅对欧盟的分析师解除这个电子邮件的脱敏”就是脱敏条目 `{to: [analyst+region__eu], mask: null}`。任何同时持有 `analyst` 角色和 `region__eu` 派生角色的读取者都能读取未脱敏的该列。无需策略语言。无需为每种组合创建角色。

其代价是以计算代替枚举。小型角色目录降低了维护成本，但“谁能看到这个欧盟电子邮件？”变成了一个计算出的答案：复合条目会对照每个用户的分配和 claim 求值。Provisa 可以双向回答，因为模型是一份定义。列编辑器中的访问检查（`POST /admin/masks/check`）会为选定的角色集合运行这一求值。

### 好处 {: #benefits }

- ABAC 列规则成为 RBAC 授权条目。
- 小型角色目录，而不是每种组合一个角色。
- 一份定义协调每个区域的规则。
- 每个请求都是最小权限和精确的审计记录。

### 指南 {: #best-practices }

- 在组织允许的范围内尽量保持角色目录小巧。
- 在同一维度内继承（资历、密级）。跨独立维度组合（角色 + 区域、角色 + 部门）。
- 把最具体的脱敏条目放在最前——即使读取者持有后面的条目，前面的条目也优先。
- 在组合投入生产之前，用 `POST /admin/masks/check` 检查实际访问。

## 列权限模型 {: #column-permission-model }

每一列都有一个权限模型，按角色控制读取、写入和脱敏。（REQ-042、REQ-249）

### 三级可见性 {: #three-tier-visibility }

| 级别 | 条件 | 结果 |
| ------ | ----------- | -------- |
| **隐藏** | 角色不在 `visible_to` 中 | 该列不在 schema 和目录中 |
| **脱敏** | 角色在 `visible_to` 中；它所持有的第一条脱敏条目有 `mask` | 列可见，输出中的值被替换 |
| **未脱敏** | 角色在 `visible_to` 中；它所持有的第一条脱敏条目没有 `mask`，或它没有任何条目 | 完全读取权限 |

### 公开列 {: #public-columns }

每一列都有一个 `scope`，在模型文件、通过管理 API 或在表页面上设置。三种范围按其服务对象排序：`restricted`，然后 `domain`，然后 `public`。表的编辑者（表所在域的 `table_registration`）可以收窄列的范围。放宽它——`restricted` 到 `domain` 或 `public`，`domain` 到 `public`——会把该列提供给更多角色，并需要其授权所需的权利：表所在域的 `column_grant` 或 `access_config`。正在注册的列以 `domain` 为基准衡量，因此将其注册为 `public` 属于放宽。对于带敏感标签的列，任何范围变更都需要 `sensitive_data`。被拒绝的变更会指明列和权利。（REQ-1959、REQ-1943、REQ-1944）

| 范围 | 提供给 |
| ------- | ----------- |
| `domain`（默认） | 覆盖该表所在域的角色，按 `visible_to` 所述。空的 `visible_to` 即每个这样的角色。 |
| `public` | 同样这些角色，以及未覆盖该表所在域的角色。 |
| `restricted` | 仅 `visible_to` 所指名的角色。空的 `visible_to` 即没有人。 |

发布一列只为该列解除域条件，仅此而已：

- 对于域外的角色，表只带着其公开列提供。表的其他列，以及不发布任何内容的表，都不在它的 schema、目录和 SQL 中。
- 列自身的 `visible_to` 仍决定提供给谁：只授予一个角色的公开列只对该角色公开。
- 脱敏、行过滤、行数上限和关系防护的适用方式与域内相同。角色没有行过滤时，已发布的列不经过滤读取；表或其所在域上的行过滤仍然适用。
- 关系在其两端的列都被提供时才被提供。
- API 表的参数列（路径或查询参数）不是数据，也不发布任何内容。
- `ops` 域要求逐列显式授权（REQ-1133）；无论范围如何，其中任何内容都不会跨域提供。

该规则在每个界面上都相同，因为每个界面都读取角色编译后的 schema：GraphQL、SQL（HTTP、pgwire、Flight、JDBC）、Cypher 和 Bolt、gRPC、REST 和 JSON:API、Flight 和 MCP 目录，以及管理列表。不是这三个值之一的范围会在保存列时被拒绝。

### 写入授权 {: #write-grants }

读取一列由 `visible_to` 控制（为空表示所有角色都可以读取）。写入按表由 `write_grants` 控制，而不是按列。表默认没有任何授权，因此任何角色都不能写入它。（REQ-2074）

写入授权声明某个角色通过一种操作（`insert`、`update` 或 `delete`）可以向某张表写入什么：

| 字段 | 用途 |
| ------- | --------- |
| `columns` | `INSERT` 可以列出、`UPDATE` 可以设置的列 |
| `rows` | `UPDATE` 或 `DELETE` 可以触及的行（SQL 谓词） |
| `check` | `INSERT` 写入的每一行、或 `UPDATE` 留下的每一行必须满足的条件（SQL 谓词） |
| `presets` | 授权自己设置的列，来自会话变量、字面量或写入时间。语句不得提供这些列。 |

省略 `rows` 或 `check` 时，授权采用该角色在这张表上的读取行过滤器，因此角色写入的就是它能读到的行。`"TRUE"` 表示所有行。

同一个准入机制决定每个界面上的每次写入，顺序如下：角色持有 `write` 权利；持有表上该操作的授权；语句写入的每一列都在授权中；添加预设；`rows` 以 AND 并入 `WHERE`；在向数据源发送任何内容之前，对新行评估 `check`。拒绝信息会指明表、角色和操作。（REQ-2074）

更宽的语句需要更宽的授权。`TRUNCATE` 需要覆盖所有行的 `delete` 授权。`MERGE` 需要覆盖所有行的 `insert` 和 `update` 授权。`INSERT ... ON CONFLICT DO UPDATE` 需要覆盖所有行且无预设的 `update` 授权。`COPY` 需要无预设的 `insert` 授权。

GraphQL 只为角色持有授权的操作提供变更字段，字段带有授权的列。子角色继承父角色的授权，自己的授权优先。元角色持有其成员授权的折叠结果：列的并集、`rows` 和 `check` 的 OR，以及只保留所有成员共有的预设。在管理界面中，表的**写入**部分会列出并编辑它的授权。

### 示例 {: #example }

```yaml
tables:
  - source_id: hr-pg
    domain_id: people
    schema: public
    table: employees
    columns:
      - name: id
        visible_to: []                # all can read
      - name: email
        visible_to: [admin, analyst, viewer]
        masks:
          - to: [admin]               # admin sees raw value (no mask)
          - to: ["*"]                 # everyone else sees it masked
            mask: {type: regex, pattern: "(.).*@", replace: "$1***@"}
      - name: salary
        visible_to: [admin, hr]
        masks:
          - to: [admin, hr]           # admin and hr see real value
          - to: ["*"]                 # all other visible roles see 0
            mask: {type: constant, value: 0}
      - name: created_at
        visible_to: []                # all can read
    write_grants:
      - role: admin
        operation: update
        columns: [email]
        rows: "TRUE"
      - role: hr
        operation: update
        columns: [salary]
      - role: hr
        operation: insert
        columns: [email, salary]
        presets:
          - {column: created_at, source: now}
```

在此示例中：

- `email`：admin 看到 `alice@example.com`，并可在任意行更新；analyst 和 viewer 看到 `a***@example.com`，不能写入
- `salary`：admin 和 hr 看到真实值；hr 可以更新它在 hr 能读到的行上的值，因为授权没有声明 `rows`；其他可见角色看到 `0`
- `created_at`：所有人都可读取，没有角色提供它，hr 的插入会把它设置为写入时间

## 变更操作授权 {: #mutation-authorization }

已注册的变更（远程 GraphQL、OpenAPI、gRPC、Hasura）由两项相互独立的检查把关。(REQ-868) 角色只有同时持有全局 `write` 能力，并且该命令通过 `visible_to` 分配给它（列表为空表示分配给所有角色），才可以调用变更。(REQ-868) 命令没有 `writable_by` 列表；配置会按名称拒绝它。[tool-verified: `provisa/security/mutation_authz.py` `command_assigned`、`admit_command`；`provisa/core/models.py` `_one_role_list`]

变更按契约而非调用者的声明被归类为写入。（REQ-869） 引用变更类函数的 `SELECT` 会被提升为写入，并接受相同的双重门检查，因此调用者无法把变更伪装成读取来调用。（REQ-869） 将变更重新归类为读取安全需要 `access_config` 能力，并作为治理决定记录；没有按请求的例外。（REQ-870）

## 架构可见性 {: #schema-visibility }

每个角色的 GraphQL schema 会隐藏未授权内容：（REQ-039）

- **域访问**：角色只能看到其 `domain_access` 域中的表（`"*"` = 全部）（REQ-039）
- **列可见性**：不在角色 `visible_to` 中的列会从 SDL 中排除（REQ-039）
- 未授权的表/列不会出现在 schema 中（REQ-039）

## 行级安全 (RLS) {: #row-level-security-rls }

按表、按角色注入 SQL WHERE 子句。在编译之后、执行之前应用。（REQ-041、REQ-263）

```yaml
rls_rules:
  - table_id: orders
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"
    description: Analysts read only their own region's orders.
```
过滤条件以 AND 追加到查询的 WHERE 子句。适用于查询和变更（UPDATE/DELETE）。（REQ-035、REQ-041）

`current_setting('provisa.<name>')` 项从请求所绑定的会话变量解析：来自已验证身份的 `user_id`、来自执行角色的 `role`，以及令牌中每个标量 claim，以其小写名称绑定，连字符换为下划线。Hasura 会话变量保留自己的名称：`X-Hasura-User-Id` 是 `hasura_user_id`，读自令牌的 `x-hasura-user-id` claim（在顶层，或在 Hasura claims 对象内，如 `https://hasura.io/jwt/claims`），或作为属性 `hasura_user_id` 记录在成员名下。因此导入的基于 `X-Hasura-User-Id` 的 Hasura 过滤会读取 `provisa.hasura_user_id`。`provisa.user_id` 始终是已登录的身份，任何 claim 都不会替换它。角色配置的 `session_vars` 是基础常量；请求的绑定叠加在其上。从另一个角色继承的角色，会把祖先的常量放在自己的常量之下，每个名称取最近角色的值。`user_id`、`role`、`region` 和 `org` 由请求或节点绑定，不能配置为常量。在不安全模式下，标头 `x-provisa-session-<name>` 绑定 `<name>`。任何地方都未绑定的变量解析为 NULL，不匹配任何行。（REQ-1682）

## 列级脱敏 {: #column-level-masking }

脱敏以每列一个有序列表的形式配置。读取者持有的第一条条目决定其看到的内容。（REQ-2059、REQ-263）

| 脱敏类型 | 支持的列类型 | SQL 表达式 |
| ----------- | ----------------------- | --------------- |
| `regex` | 字符串（varchar、char、text） | `REGEXP_REPLACE(col, pattern, replace)` |
| `constant` | 任意 | 字面量值（`NULL`、`0`、自定义字符串） |
| `truncate` | 日期/时间戳 | `DATE_TRUNC(precision, col)` |

每条条目都有一个 `to` 列表，指名其针对的读取者，以及一个可选的 `mask`。没有 `mask` 的条目对这些读取者表示“不脱敏”。`to` 中的 `*` 匹配任何读取者。

脱敏表达式被下推到 SQL SELECT 投影中。数据库返回脱敏后的值；对于脱敏角色，未脱敏数据从不经过网络传输。（REQ-263） 脱敏列在 `WHERE` 和 `HAVING` 子句中也被阻止（第 5 层谓词防护），以防止通过过滤推断值。（REQ-263、REQ-531）

**脱敏覆盖列的每一个名称，但不覆盖关联列。** 按别名或物理名称读取的列以相同方式脱敏。脱敏不会隐藏已批准关系与角色可见列连接的键：连接比较的是真实值，因此关联列会显示被脱敏键的内容。请同时对关联列脱敏，或不要为该角色批准此关系。（REQ-263、REQ-019）

**没有条目，就不脱敏。** 不持有任何条目的读取者读取未脱敏的列。当每个读取者都应受到某种脱敏时，请以一条 `*` 条目结束列表。列表没有 `*` 条目时，列编辑器会发出警告（`unlisted_unmasked`）。

**按顺序拒绝。** 即使读取者还持有后面一条更宽松的条目，前面的条目也优先。要让承包商无论还有哪些其他角色都始终看到脱敏的薪资，请把承包商条目放在不脱敏条目之前。授予角色从不失去父角色通过继承给予的内容，但在列表中位置靠前的脱敏条目会先适用——这就是这里限制的方式。

**元角色。** 元角色的链包括每个成员的角色、它们的祖先以及所有被指名的派生角色。授予任何成员的条目都会触达元角色。另一个成员持有的、排在其前面的条目仍然先适用。

**访问检查。** `POST /admin/masks/check` 接受一个脱敏列表和可选的角色集合，返回对这些角色适用的条目，以及任何读取者都无法触达的条目。受 `masking_config` 保护。

```yaml
# email column: analyst+region__eu sees raw; everyone else sees masked
masks:
  - to: [analyst+region__eu]          # composite: analyst AND in EU region
  - to: ["*"]                         # all others
    mask: {type: regex, pattern: "(.).*@", replace: "$1***@"}

# hire_date column: hr sees raw; all other visible roles see year only
masks:
  - to: [hr]
  - to: ["*"]
    mask: {type: truncate, precision: year}
```
## 治理操作响应 {: #governing-action-responses }

被跟踪函数或 webhook 的响应像表的行一样受治理。（REQ-1679） 操作返回的行被绑定为一个关系，其列是该操作声明的输出契约（函数的 `output_columns`、webhook 的 `inline_return_type`，或函数数组型 `return_schema` 的对象属性），并且该关系通过带有这些行的 VALUES CTE，经过与每次表读取相同的治理阶段。（REQ-1679） 行级安全、脱敏或可见性都不会为操作重新实现。

有三件事适用，顺序与表相同。（REQ-1679）

- 行过滤：为该操作按名称保存的角色行级安全规则（带 `actionName` 的 `upsertRlsRule`），或者在操作没有自己的规则时，操作所在域的域规则。保存时会像表规则一样对照契约检查谓词。（REQ-1676）
- 列可见性：声明了 `visible_to` 的契约列会对列表未指名的角色移除；未声明任何内容的列属于该操作的公开形态并保留。
- 脱敏：契约列携带 `masks`，即与表列相同的有序脱敏列表。（REQ-2059）

角色继承为三者解析继承链。（REQ-1677） 返回声明契约之外的列的响应会以 502 被拒绝，而不是不受治理地通过。未声明列契约的操作返回标量，没有可绑定的内容；它按原样返回。管理端的测试调用会报告所应用的过滤、排除和脱敏，受治理的语句会记录在查询审计日志中。

## 采样 {: #sampling }

除非拥有 `full_results` 能力，所有角色看到的都是采样结果（默认：100 行）。（REQ-554） 由环境变量 `PROVISA_SAMPLE_SIZE` 控制。（REQ-554）

## 审计日志 {: #audit-logging }

每个触及域资产的查询都会记录到 `query_audit_log`。（REQ-596、REQ-613） 审计日志属于组织，而不属于平台：平台自己的组织、用户和邀请注册表不保存审计记录。（REQ-1922） 每个组织的 `query_audit_log` 是该组织自己的 schema 中的一张表，位于该组织为其在所选每个区域中的记录所指定的存储中。未声明任何区域的组织，将该 schema 与其模型一起保存在部署的控制平面数据库中。

每一行记录 `tenant_id`（组织）、`user_id`、`role_id`、查询文本的 SHA-256 哈希、加密后的查询文本本身（REQ-689）、`table_ids` 和 `table_names`、`source`、`status_code`、`duration_ms`、路由和行数、对该语句实施了哪些强制措施，以及 `logged_at`。组织设置了密钥时，查询文本用组织的密钥加密，否则用部署的密钥加密（REQ-1574）。未配置加密提供程序的部署，以未加密形式存储。该文本从不暴露在可查询的运维表中，只在授权的管理读取中解密。

该日志在数据库层面是仅追加的：PostgreSQL 规则将针对 `query_audit_log` 的 `DELETE` 和 `UPDATE` 变为空操作。（REQ-596、REQ-613） 两个索引——`(tenant_id, logged_at)` 和 `(user_id, logged_at)`——支持按组织和按用户在时间范围内的合规查询。（REQ-596、REQ-613）

保留期：行会被无限期保留。Provisa 没有保留期设置，也从不清除审计日志；由于 `DELETE` 是空操作，删除旧行意味着由运维人员在 Provisa 之外删除或归档该表的数据。

## 速率限制 {: #rate-limiting }

每个角色的速率限制在 `provisa.yaml` 中配置：每秒最大查询数、最大并发 SSE 订阅数和最大并发 Arrow Flight 流数。（REQ-369） 限制在 API 层、编译或执行之前执行；超出限制的请求以 HTTP 429 和 `Retry-After` 标头被拒绝。（REQ-369）

自然语言查询服务（`POST /query/nl`）通过 `nl.rate_limit`（每个角色每分钟请求数）有单独的限制。超出限制的请求在调用任何 LLM 之前被拒绝。（REQ-370）

速率限制状态以滑动窗口计数器保存在 Redis（`cache.redis_url`）中——不在单个实例内保存状态——因此限制在所有水平扩展的 Provisa 实例上执行。（REQ-371）

## 查询复杂度上限 {: #query-complexity-limit }

速率限制约束角色发送多少请求。复杂度上限约束单条语句可以请求什么。（REQ-1174）

每条语句都有一个复杂度分数：它读取的关系、它的联接、它选择的列（`*` 按其代表的列计数）以及嵌套在其中的查询。从远程 API 数据源（OpenAPI、远程 GraphQL、远程 gRPC）读取的关系计为 10，因为每次读取都会消耗远程系统为该数据源所有用户设定的预算。

上限是角色 `rate_limit` 中的 `max_query_complexity`。组织范围的设置 `limits.max_query_complexity` 是每个角色的上限：角色可以设置更低的上限，更高的上限不生效。默认两者都未设置。

```yaml
roles:
  - id: analyst
    rate_limit:
      max_query_complexity: 200
```
分数在查询解析之后、治理或执行之前测量，因此同一个上限适用于语句可能到达的每个接口：GraphQL、HTTP 上的 SQL、pgwire、Arrow Flight、gRPC、Cypher、JSON:API 和 MCP。超出上限的语句会被拒绝，并附上其分数、上限以及它所请求的内容。HTTP 接口应答 413。该拒绝作为策略拒绝记录。

## 身份验证 {: #authentication }

可插拔的身份验证提供程序：（REQ-120）

| 提供程序 | 凭据 | 场景 |
| ---------- | ----------- | ---------- |
| `none` | X-Provisa-Role 标头 | 仅用于开发 |
| `basic` | 本地账户（bcrypt）+ 会话 JWT | 自托管部署 |
| `firebase` | Firebase ID 令牌 | 生产 |
| `keycloak` | Keycloak JWT | 企业 |
| `oauth` / `oidc` | OIDC JWT | PingFed、Okta、Azure AD、Auth0 |
| `ldap` | 目录用户名和密码（LDAP 或 Active Directory） | 企业目录（REQ-1265） |
| `saml` | SAML 2.0 签名断言，然后是会话 JWT | 企业单点登录（REQ-1265） |
| `simple` | bcrypt + JWT | 测试 |

`ldap` 采用先搜索后绑定的方式让用户登录：Provisa 以服务账户绑定，找到用户名所指向的那一个条目，再以该条目和用户提交的密码进行绑定，由目录来校验密码。用户所属的组成为其角色。每次会话令牌返回时，都会重新读取账户及其组，因此从目录或从某个组中移除的用户，会在令牌过期之前失去相应访问权限。`ldaps://` 和 StartTLS 均受支持。

`saml` 是由 SP 发起的 Web Browser SSO。响应的 XML 签名依据标识提供程序的元数据验证，claim 只从已签名的元素中读取。Issuer、Audience、ACS URL、有效期和 `InResponseTo` 均会检查。未经请求（由 IdP 发起）的响应、加密断言、带 DOCTYPE 的文档、多个断言和 SHA-1 签名均被拒绝。SAML 会话不会再向标识提供程序复核：在那里被移除的用户，在会话令牌过期之前仍保留访问权限。

角色映射：身份 claim → 通过可配置规则映射为 Provisa 角色。（REQ-120） `assignments_source` 字段控制角色分配的来源：`claims` 从 JWT 令牌 claim 中读取（默认），`provisa` 从 Provisa 内部的分配存储中读取。（REQ-551） 多租户部署（`multitenancy: true`）按组织自行分配角色，因此必须使用 `assignments_source: provisa`：使用 `claims` 时它不会启动，会导致这一组合的设置变更也会被拒绝。单租户部署可以使用任一选项。

在 `provisa.yaml` 中配置的超级用户（用户名加来自环境密钥的密码）无论配置了哪个提供程序，总是获得 admin 角色和所有能力——这是初始设置的引导路径。（REQ-125）

### 接口与凭据 {: #surfaces-and-credentials }

每个接口都通过同一个提供程序契约进行身份验证，因此在一个接口上可用的凭据，在协议能携带它的所有接口上都可用。（REQ-124、REQ-1263） 此表是唯一的参考；各接口的文档不再重复。

| 接口 | 密码 | 提供程序令牌 | 个人访问令牌 | 客户端证书 (mTLS) |
| --------- | ---------- | ---------------- | ----------------------- | --------------------------- |
| HTTP（REST、JSON:API、GraphQL） | `Authorization: Basic` | `Authorization: Bearer` | `Authorization: Bearer` | 通过终止代理 |
| pgwire | 密码字段（明文或 SCRAM） | 密码字段，OIDC 部署 | 密码字段 | 是 |
| Bolt | `basic` 方案 | `bearer` 方案 | `bearer` 方案 | 是 |
| Arrow Flight | — | 握手或 ticket 负载中的 `token`；`list_flights`、`get_flight_info` 和 `get_schema` 使用调用标头 `authorization: Bearer` | 同上 | 是 |
| gRPC | — | `authorization` 元数据 | `authorization` 元数据 | 是 |
| Airport（DuckDB `airport` 扩展，可选） | — | `authorization: Bearer`（密钥的 `auth_token`） | 同上 | — |
| MCP | — | `Authorization: Bearer` | `Authorization: Bearer` | 通过终止代理 |

DuckDB `airport` 扩展唯一的身份验证选项是其密钥的 bearer `auth_token`，因此 DuckDB 客户端在那里出示提供程序令牌或个人访问令牌，并以其身份解析到的角色执行。能够设置调用标头的客户端，可以在 `x-provisa-role` 中请求一个已持有的角色或逗号分隔的集合。（REQ-1120、REQ-1263）

在这些接口上，拒绝都会报告其类别。缺失、被拒绝或不可用的凭据是身份验证失败：HTTP 401、gRPC `UNAUTHENTICATED`、Arrow Flight 和 Airport `Unauthenticated`。有效凭据请求以身份不持有的角色执行，是授权失败，并会指明该角色：HTTP 403、gRPC `PERMISSION_DENIED`、Arrow Flight 和 Airport `Unauthorized`。（REQ-273、REQ-1263） 有一个例外位于 Arrow 客户端库而非服务器：Flight `get_schema` 调用的拒绝会以带有服务器原因、但不带其类别的通用 Arrow 错误到达 Python 客户端；`list_flights`、`get_flight_info` 和 `do_get` 会传递类别。

单元格为 `—` 之处，协议不携带可以附加密码的用户名字段；令牌形式覆盖了这种情况。pgwire 则相反：启动包中只有一个密钥字段且没有方案，因此方法根据密钥*是什么*来选择：PAT 按前缀识别；当配置的提供程序是令牌提供程序时，密钥作为 bearer 令牌读取；其他一切都是密码。选择只做一次——被所选验证器拒绝的凭据不会再由另一个验证器检查。

该矩阵由 `tests/unit/test_auth_surface_conformance.py` 执行，它调用每个接口真实的身份验证入口点，若新增接口而没有对应的行则失败。

### 个人访问令牌 {: #personal-access-tokens }

PAT 是用户为无法完成交互式登录的客户端（脚本、BI 工具、驱动）创建的长期 bearer 密钥。（REQ-1263） 它携带自己的组织和角色，每个接口都通过同一个验证器解析它，因此任何接口都不需要知道 PAT 是什么。

线上形式为 `provisa_pat_` 后接 43 个 URL 安全的 base64 字符。该前缀将出示的密钥路由到令牌存储而不是身份提供程序，并使泄露的令牌能在日志和代码仓库中被找到。

- **存储**——只保存密钥的 SHA-256。密钥本身只在创建时显示一次，且无法恢复。列表携带显示前缀和生命周期时间戳，但从不携带有效凭据。
- **签发与吊销**——`POST /auth/tokens`、`GET /auth/tokens`、`DELETE /auth/tokens/{token_hash}`，以及管理界面中用户自己个人资料里的自助区域。创建和吊销凭据是令牌所有者的操作。
- **归属**——经过验证的 PAT 解析为其所有者的账户：用户 id、电子邮件和显示名称。因此在 PAT 下写入的审计行或用量报告指明的是人，而不是凭据。该人的哪个令牌在执行，会单独放在 `raw_claims["token_name"]` 中传递。
- **过期**——令牌可以设置过期时间；过期的令牌在验证时被拒绝。移除用户的成员资格会一并吊销其令牌。

### pgwire 上的 SCRAM-SHA-256 {: #scram-sha-256-on-pgwire }

使用 `basic` 提供程序时，设置 `auth.scram: true` 会让 pgwire 以 `SCRAM-SHA-256` 机制提供 SASL（身份验证代码 10），从而证明密码而不是发送密码。（REQ-1394） 不提供通道绑定（`SCRAM-SHA-256-PLUS`）。

SCRAM 需要 RFC 5802 验证器，它无法从 bcrypt 哈希推导出来。每当密码以明文经过时——注册、登录、修改密码、管理员重置——都会写入验证器，因此启用 SCRAM 的部署会在用户下一次进行身份验证时收集验证器，每个用户的第一次 SCRAM 连接发生在其下一次输入密码之后。尚无验证器的用户会得到一次与真实交换无法区分的模拟交换，因此网络不会暴露谁已经迁移。

### 双向 TLS {: #mutual-tls }

客户端证书验证把第一道检查移到 TLS 握手中：没有由部署的 CA 签发的证书的调用者，永远到达不了凭据层。（REQ-1228） 它在 pgwire、Bolt、gRPC 和 Arrow Flight 上可用——这四种传输自行终止其 TLS。

| 变量 | 含义 |
| ---------- | --------- |
| `PROVISA_MTLS_CLIENT_CA` | 允许签发客户端证书的 CA 的 PEM 包 |
| `PROVISA_MTLS_MODE` | `required`（一旦设置 CA 即为默认）或 `optional` |
| `PROVISA_MTLS_BIND_PRINCIPAL` | 为 true 时，证书的通用名必须与连接随后进行身份验证所用的用户名相匹配 |

按协议的覆盖遵循与 TLS 设置相同的命名。不做任何推断：没有 CA 而设置了模式会拒绝启动，无法识别的模式也会拒绝启动，而不是被读作最安全的相邻值——一个以为自己要求客户端证书但实际并未要求的部署，比一个不启动的部署处境更糟。

### TLS 版本 {: #tls-versions }

Provisa 终结 TLS 的每个监听器（HTTP API 和 UI、pgwire、Bolt、MCP、gRPC 和 Arrow Flight）都接受 TLS 1.2 和 TLS 1.3，并拒绝 TLS 1.0 和 1.1。Provisa 自己不设置协议版本或密码套件列表。HTTP、pgwire 和 Bolt 监听器使用 Python `ssl` 模块的服务器默认值（最低 TLS 1.2）。gRPC 和 Arrow Flight 使用 gRPC 的默认值（最低 TLS 1.2）。这些版本不可配置。

### 登录限流 {: #login-throttling }

密码猜测与协议无关：同一个账户可以通过 HTTP、pgwire 和 Bolt 受到攻击。因此计数器位于凭据验证层，而不是任何单个接口上，从而在任何地方触发的锁定在所有地方都有效。（REQ-1393）

它默认启用——五分钟内失败五次会将主体锁定十五分钟——并可在 `auth.login_throttle` 中配置。被锁定的主体会在凭据被检查之前就被拒绝，成功的身份验证会清除该主体的历史。

键是协议所携带的主体。仅 bearer 的接口不携带主体，因此键是凭据本身的摘要；这可防止无限制地重放错误的令牌。存储在进程内，因此有多个 API worker 的部署允许每个 worker 最多 `max_attempts` 次——该限流是减慢猜测，而不是分布式配额。

### 指定组织 {: #naming-the-org }

在多租户模式下，每个请求都指名其目标组织。只属于一个组织并不等于指名了它：未指名任何组织的请求会被拒绝，拒绝会说明应发送什么。（REQ-1235） 各接口指名组织的方式如下：

| 接口 | 客户端如何指名组织 |
| ------- | -------------------------- |
| HTTP（界面、REST、GraphQL） | 组织自己的主机名 `acme.provisa.dev`，或在控制平面主机上使用 `X-Org-Provisa` 标头 |
| pgwire | 数据库名（`psql -d acme`、BI 工具的数据库字段），或通过 TLS 拨号的组织自己的主机名 |
| Bolt | 数据库名 `<org>.provisa_<role>`（或 `<org>.provisa_ops_<role>`），或通过 TLS 拨号的组织自己的主机名 |
| Arrow Flight | ticket 中的 `"org"` |
| gRPC | `x-provisa-org` 元数据标头 |
| MCP（HTTP 传输） | 组织自己的主机名或 `X-Org-Provisa` 标头 |
| Python 客户端 | `ProvisaClient`、`connect()` 和 `adbc_connect()` 中的 `org=`；SQLAlchemy URL 中的 `?org=` |

通过 TLS 拨号主机名的 pgwire 客户端会在 TLS ClientHello 中发送该主机名，Provisa 从中读取组织（REQ-1234）。当主机名和数据库名都指名了组织且二者不同时，连接会被拒绝；二者都不优先。默认使用的数据库名 `provisa` 不指名组织，连接到某组织的 BI 工具会把
该组织显示为其数据库。在 Bolt 上，`SHOW DATABASES` 为用户所属的每个组织
列出一个 `<org>.provisa_<role>`；在多租户模式下，单独的 `provisa_<role>` 会被拒绝，会话保持在
它最先指名的组织中。

名称是请求，而不是授权。在任何地方它都会到达同一个解析器，该解析器会拒绝已验证主体不是其成员、且对其没有跨组织权利的组织。

个人访问令牌为一个组织签发，并自行指名该组织。它只开放该组织：指名另一个组织的请求会被拒绝，即使令牌所有者同时属于两者。

单租户部署没有需要指名的组织。在那里，没有任何接口为此读取主机名、数据库名或标头。

## 跨组织的密钥 {: #secrets-across-organizations }

组织的密钥只由该组织读取。在 Provisa 自己的存储中，每个密钥都保存在其组织之下。在承载多个组织的部署的中央服务中（Vault、AWS Secrets Manager、GCP Secret Manager、Azure Key Vault），每次读取都在读取方组织的前缀内进行，会离开该前缀的引用会被拒绝。（REQ-1557） 前缀以及将已有密钥迁移到前缀下的步骤，参见[密钥](secrets.md#one-organizations-secrets-in-a-central-service)。

## 跨组织的遥测 {: #telemetry-across-organizations }

在许多组织共享的部署上，运维信号（`"ops"."traces"`，在 Trino 引擎上还有旁边的指标和日志）是所有组织的工作都写入的同一个存储。每个 span 记录启动它所服务的组织，保存为 `tenant_id`；对这些信号的每次读取只返回读取方组织的行，与角色无关。在任何组织的工作之外启动的 span（例如服务器启动时）不属于任何组织，也没有组织会读到它。平台管理员在任何组织中都没有数据访问权，也不通过 Provisa 读取其中任何一个。(REQ-1349, REQ-1327)

运维信号在两种引擎上都会无限期保留，与审计日志一样：压缩会重写存放它们的文件，但不删除任何行。在 Trino 上，它们是 `otel` 目录的 Iceberg 表。在原生引擎（DuckDB）上，服务器把自身的追踪记录以 Parquet 文件写入其遥测文件夹，引擎以同一名称 `"ops"."traces"` 读取它们，并在其上提供刷新血缘视图 `"ops"."mv_refresh_lineage"`；两者都按同一条按组织划分的规则读取。(REQ-862)

信号在其存储中以明文保存。需要在一个部署内部比按组织划分更强边界的工作负载，运行在专用部署上；高度敏感或受监管的工作负载则自托管。

## 部署加固 {: #deployment-hardening }

### 存储凭据与存储的监听位置 {: #store-credentials-and-where-the-stores-listen }

内置存储没有默认凭据。PostgreSQL 控制平面、Redis、MinIO、Grafana 管理员账户以及 Trino 集群的共享密钥，各自使用为该部署生成的值：

- 启动器（`provisa start`、桌面版与服务器安装程序）生成一次并保存在 `~/.provisa/store-credentials.env`（权限 600）。环境中已有的值按原样使用；使用主节点存储的从节点，在没有主节点的值时拒绝启动。
- Terraform 模块用 `random_password` 生成它们，并把相同的值交给每个节点。它们保存在 Terraform 状态中，因此应像保护密码本身一样保护该状态。
- Helm chart 不设置任何值。它需要你创建一个 Secret，由 `credentials.existingSecret` 指定，包含 `pg-password`、`redis-password`、`minio-access-key`、`minio-secret-key`、`grafana-admin-password` 和 `trino-shared-secret`。

Redis 要求提供密码，PostgreSQL 角色的密码在每次启动时重新设置，使此前创建的卷跟随部署当前的密码。存储、联邦查询引擎和遥测服务在同一个地址 `PROVISA_STORE_BIND_IP` 上发布端口：单独节点上为 `127.0.0.1`，从节点连接主节点时为主节点的私有地址。Grafana 管理员账户使用生成的密码；compose 堆栈允许匿名访问者查看仪表板，而 Helm chart 不接纳匿名访问者。

### 传输加密 {: #transport-encryption }

Provisa 终结的每个监听器都提供 TLS（参见 [TLS 版本](#tls-versions)）。服务器节点使用 `PROVISA_TLS_CERT`/`PROVISA_TLS_KEY` 中的证书，或生成自签名证书。Helm chart 默认提供 TLS（`tls.enabled: true`），证书来自 `tls.secretName` 指定的 `kubernetes.io/tls` Secret。

### 嵌入式 PostgreSQL {: #the-embedded-postgresql }

桌面和原生层以嵌入方式运行 PostgreSQL。在 macOS 和 Linux 上，它监听其数据目录内的 unix 套接字（权限 700），只有拥有该目录的账户才能访问。通过 TCP（Windows 上，监听 127.0.0.1）时，每个连接都必须提供为该数据目录生成并保存在其中的密码（`provisa.password`，权限 600）。它从不以 root 身份启动。

### 联邦查询引擎（Trino） {: #the-federation-engine-trino }

- **节点之间。** 协调器与其 worker 使用为部署生成的共享密钥（`internal-communication.shared-secret`）相互认证。加密它们之间的流量是运营者的可选项，默认关闭：关闭时，流量保持在部署的私有网络或集群之内。当合规或零信任要求部署内部也须加密时再开启。Trino 文档称它会使 shuffle 密集型查询（分布式连接、聚合、窗口函数）变慢 10% 到 100% 以上。在 compose 部署上，它是设置 `engine.trino_internal_tls`（Deployment settings → Engine），旁边是引擎网络和协调器的固定地址，并在下次启动时生效。在 Helm chart 下，它是值 `trino.internalTls`；chart 生成节点使用的 CA 和证书，在升级之间保留它们，并按每个节点的 pod DNS 名称寻址。托管平台的引擎是单节点协调器，没有节点间流量，并拒绝该选项。（REQ-2072）
- **假脱机数据。** 容错执行用为每个查询生成、查询结束即丢弃的密钥，加密它假脱机的交换数据（`fault-tolerant-execution.exchange-encryption-enabled=true`，显式设置）。不使用旧式的溢写到磁盘。
- **同一引擎上的多个组织。** 每个组织的 catalog 属于它自己，以该组织命名，每个组织的语句在它自己的资源组中运行。
- **目录及其中的凭据只存在于引擎内存中。** Provisa 在运行时创建每个 Trino 目录，每个目录保存其数据源的连接属性，包括密码。Provisa 运行的每个引擎都把它们保存在内存中（`catalog.store=memory`）：不会有目录写入引擎磁盘，也不会从文件挂载目录。重启的协调器回来时没有任何目录；每个 Provisa 进程会在下一次查询时发现，引擎监视器则在一分钟内发现，并重新创建它所服务的目录。带 SASL 凭据的 Kafka 数据源需要一个文件：Trino 的 Kafka 连接器只从文件读取客户端设置，因此 Provisa 把它们（包括密码）写入 `kafka-client/<catalog>.properties`。该文件只有 Trino 的用户可读（0700 目录中的 0600 模式）。在容器栈中，该目录是 API 与引擎共享的内存卷。在托管平台的引擎上，它是引擎自己的 Kubernetes Secret，保存在节点内存中，只有 Trino 的用户组可读（0440 模式）。无论哪种方式，该文件都不会落到磁盘上。在托管引擎上，Kafka 数据源必须指定模式注册表：主题列只由模型声明的数据源在保存或应用时会被按名称拒绝，因为它的表描述需要一个无法交给运行中引擎的目录。启动栈时会删除早期版本留在磁盘上的目录文件和 Kafka 客户端文件。

### 共享部署上的数据源 {: #sources-on-a-shared-deployment }

在承载多个组织的部署上，组织的数据源只能访问公网地址：绝不能访问平台的私有网络，那里有它的控制平面、缓存和对象存储。该检查在保存数据源时进行（无论通过 UI、管理 API 还是配置应用），并在每次连接和每次 HTTP 重定向时再次进行。演示沙箱的数据源还被限定在检查时的地址。部署自己的组织（其数据源由运营者配置）不受限制。

## 静态加密与组织自有密钥 {: #encryption-at-rest-and-the-organizations-own-keys }

使用组织自有密钥的加密是可用的，在组织开启之前处于关闭状态。除非组织管理员创建了密钥，否则不会有任何内容在组织的密钥下加密。

**组织加密密钥**（REQ-1574）包裹组织的密钥、其加密列及其审计查询文本。组织管理员在 **Admin → Org Secrets → Org Key** 下创建它，或使用 `PUT /admin/org-encryption`：不带请求体时 Provisa 生成该密钥且从不显示，带 `key_b64` 请求体则提供你自己的 32 字节。`GET /admin/org-encryption` 只报告其指纹。没有组织密钥时，组织的数据用部署的密钥（`provisa.yaml` 中的 `encryption.provider`）包裹。未配置加密提供方的部署，会以未加密形式保存加密列和审计查询文本；密钥绝不以明文保存（参见[密钥](secrets.md#provisas-built-in-store)）。

**组织的文件夹**（REQ-1992）存放其上传和落地的文件、其引擎的溢写文件及其临时文件。组织管理员在 **Admin → Org Secrets → File folder encryption** 下开启其加密，或使用 `PUT /admin/org-files`（`{"encrypted": true, "key_reference": "${secret:NAME}"}`）。`POST /admin/org-files/generate-key` 一次性返回新的 256 位密钥，供存入保管库。密钥保留在组织自己的保管库中（AWS Secrets Manager、GCP Secret Manager、Azure Key Vault 或 HashiCorp Vault）：Provisa 只保存引用，gocryptfs 用该密钥使文件夹中的文件在磁盘上保持加密。这需要 FUSE，即带有 `/dev/fuse` 的 Linux 主机或容器；在 macOS 上该设置会被拒绝。

在通常以自有引擎运行一个组织的自托管部署上，同时开启两者即用该组织的密钥加密其密钥、文件、溢写文件和临时文件：

- 组织运行的 DuckDB 引擎溢写到组织的文件夹。
- 行过滤器获取的行和上传的代码暂存在组织的文件夹中。
- Trino 的容错执行用为每个查询生成、查询结束即丢弃的密钥，加密假脱机的交换数据（`fault-tolerant-execution.exchange-encryption-enabled=true`）。Provisa 不开启 Trino 旧式的溢写到磁盘。
- Trino 把组织的目录连同其凭据保存在内存中，不向磁盘写入任何目录（`catalog.store=memory`）；Kafka 数据源的 SASL 客户端设置位于只有 Trino 用户可读的文件中，在容器栈中位于内存（参见[联邦引擎](#the-federation-engine-trino)）。
- PostgreSQL 联邦引擎把临时文件写在该 PostgreSQL 服务器保存它们的位置，处于服务器自身的存储加密之下，而不是组织的文件夹中。

### 共享与专用托管平台 {: #shared-and-dedicated-hosted-platforms }

共享托管平台在逻辑上隔离各组织。一个部署服务所有组织，每个组织的数据通过治理、引擎上自己的 schema、catalog 和资源组、自己的密钥、假脱机引擎数据的每查询密钥，以及拒绝其数据源访问平台私有地址，而始终归其自己所有。建议用于试验和小型企业。专用托管平台在物理上隔离：拥有自己的机器、网络和存储，服务一个客户。对于高度敏感或受监管的工作负载，请在你自己的边界内自托管 Provisa，由你自己的运维人员掌握每一个密钥和每一台主机。托管平台目前没有 SOC 2 报告。我们正在评估 SOC 2，并会提供进展。如果你需要 SOC 2，我们建议自托管。

### 浏览器中的 Python {: #python-in-the-browser }

Explore 的 Python 和 Notebook 标签页在访问者的浏览器中运行 Python。它发出的每次读取都以已登录用户的身份、带着其当前角色、组织和环境发送到服务器，并与来自任何其他客户端的同一语句完全一样地受到治理。

## 高安全模式 {: #high-security-mode }

`provisa.yaml` 中的 `security.mode: high` 声明一项保证：Provisa 后端从不处理明文数据。（REQ-693） 每个有意义的列都在数据源处加密，只有持有解密密钥的客户端才能读取它。这项保证有一些部署必须为之规划的后果。

**该模式做什么：**

- **数据端点要求客户端解密的证明。** `/data/` 下的一切都返回 403，除非调用者出示 `X-Provisa-KMS-Key` 标头——这是配置为本地解密的 JDBC 或 Python 客户端的标志。明文的浏览器或 REST 使用者不携带这样的密钥，因此被拒绝。该控制是对整棵树的默认拒绝：明天新增的路由在发布当天就受到保护，例外需要正当理由。
- **schema 元数据端点保持开放。** `/data/sdl`、`/data/introspection`、`/data/schema-version`、`/data/domains`、`/data/proto` 和 `/data/compile` 不返回行数据，而客户端必须先读取 schema——包括哪些字段是 `@encrypted`——才能连接。
- **gRPC 和 Arrow Flight 继续提供服务，要求相同的证明。** 它们是加密客户端实际使用的传输；关闭它们会让高安全部署没有线协议可用。两者上的数据调用都必须以调用元数据携带同一个 KMS 密钥。
- **pgwire、Bolt 和 MCP 不会启动。** 这三者都没有能携带解密上下文的逐连接握手：pgwire 的行集和 Cypher 结果以明文传输，MCP 的工具调用把结果作为文本交给模型。为其中任何一个配置的端口都会在启动时被拒绝，而不是提供服务。
- **关系防护无法绕过。** `ignore_relationships` 和 `relationship_guard: false` 都被忽略；参见[关系治理](#relationship-governance-v002)。

**如何验证部署处于该模式：** 启动日志会指明它，不带 KMS 密钥的 `/data/sql` 查询会以 403 应答并附上指名 REQ-693 的消息，pgwire、Bolt 和 MCP 端口不在监听。

## ABAC批准钩子 (Hook) {: #abac-approval-hook }

在查询执行之前触发的可选外部策略钩子。（REQ-203） 配置后，Provisa 会带着用户身份、角色、表、列和操作调用你的策略引擎。应答决定查询是否继续。（REQ-203） 以**需要批准**注册的命令在每次调用前都会交给同一个钩子；参见[命令调用](#command-calls)。（REQ-1924）

### 适用范围 {: #scoping }

钩子只在查询触及启用了它的表或数据源时触发——其他一切都没有开销。（REQ-204）

| 配置 | 效果 |
| -------- | -------- |
| `auth.approval_hook.scope: all` | 每个查询都调用钩子 |
| `sources[].approval_hook: true` | 该数据源的所有表都调用钩子 |
| `tables[].approval_hook: true` | 该表调用钩子 |

### 协议 {: #protocols }

支持三种传输：（REQ-246）

| 类型 | 场景 | 配置字段 |
| ------ | ---------- | ------------- |
| `webhook` | 任何 HTTP 策略服务（OPA、自定义） | `url` |
| `unix_socket` | 同一主机上的 OPA 或策略 sidecar | `socket_path` + `url` |
| `grpc` | 就近的高性能策略服务 | `url` (host:port) |

gRPC 传输使用 `provisa/auth/approval.proto` 中定义的 `provisa.auth.ApprovalService` 契约。在你的策略引擎中实现此服务：（REQ-246）

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
  map<string, string> session_vars = 6;
  string command = 7;         // a command call's name; empty for a query
  string arguments_json = 8;  // a command call's arguments as a JSON object
}

message ApprovalResponse {
  bool approved = 1;
  string reason = 2;
}
```
gRPC 通道是持久的——每个 Provisa 实例一个通道，在对该钩子端点的所有调用中复用。（REQ-555）

### 请求／响应 {: #request-response }

三种传输都携带相同的负载：（REQ-246）

| 字段 | 类型 | 说明 |
| ------- | ------ | ------------- |
| `user` | string | 已验证用户的身份 |
| `roles` | string[] | 用户的 Provisa 角色 |
| `tables` | string[] | 查询引用的表 id |
| `columns` | string[] | 查询中选择的列 |
| `operation` | string | `"query"` 或 `"mutation"`；命令调用为 `"command"` |
| `command` | string | 命令调用时的命令名称；查询时为空 |
| `arguments` | object | 调用命令所用的参数；查询时为空 |

webhook 和 Unix socket 传输交换 JSON，以 `command` 和 `arguments` 为键。在 gRPC 上，参数以 JSON 文本 `arguments_json` 传递。响应必须包含 `approved`（bool），并可选包含 `reason`（string）。（REQ-246）

### 命令调用 {: #command-calls }

设置了 `requires_approval`（命令表单上的**需要批准**开关）的命令，在任何界面上每次调用之前都会交给钩子。只有钩子批准时它才运行。请求携带 `operation: "command"`、命令名称及其参数；`tables` 和 `columns` 为空。（REQ-1924） [tool-verified: `provisa/api/data/action_exec.py` `_require_approval` (`operation="command"`, `tables=[]`, `columns=[]`, `command=fn["name"]`, `arguments=args`)]

```json
{
  "user": "analyst",
  "roles": ["analyst"],
  "tables": [],
  "columns": [],
  "operation": "command",
  "command": "create_issue",
  "arguments": {"input": {"title": "Crash on save"}}
}
```
[inferred: the payload also carries `session_vars`; values shown are placeholders]

两种拒绝，均为 403：

- `functions.approval_unavailable`：未配置钩子。与查询不同，需要批准的命令在没有钩子时从不放行。
- `functions.approval_denied`：钩子应答了 `approved: false`。原因在消息中。

### 超时及回退处理 {: #timeout-and-fallback }

```yaml
auth:
  approval_hook:
    type: grpc          # webhook | grpc | unix_socket
    url: "localhost:50051"
    timeout_ms: 500     # default 5000
    fallback: deny      # allow | deny — applied on timeout or error
    scope: ""           # "" = use per-table/per-source flags; "all" = every query
```
超时或传输错误时，适用 `fallback` 策略。（REQ-247） 熔断器（默认：连续 5 次错误后断开，30 秒后半开）可防止缓慢的钩子端点引发级联故障。（REQ-556）

### 配置示例 {: #configuration-example }

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
## 密钥 {: #secrets }

凭据使用 `${env:VAR_NAME}` 语法，在运行时解析。（REQ-557） 密码从不保存在配置数据库中。（REQ-557）

关于密钥服务的完整说明——存储、引用语法和提供程序——参见[密钥](secrets.md)。
