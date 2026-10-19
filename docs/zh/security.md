# 安全模型 {: #security-model }

每条查询都经过同一套治理，无论它用哪种语言编写（GraphQL、SQL、Cypher），也无论它经由哪条途径到达（REST、gRPC、Arrow Flight、JDBC、WebSocket）。没有任何绕过它的路径。

检查按顺序进行。请求必须先通过一项，才会进入下一项。

## 分层模型 {: #layered-model }

### 第 0 层 — 内省过滤 {: #layer-0-introspection-filtering }

一个角色只能看到其 `domain_access` 覆盖的表，以及其 `visible_to` 规则允许的列。其余一切对这个角色来说都不存在：无法查询，无法自动补全，也无法猜出它的存在。GraphQL schema、SQL 目录和查询编辑器的 schema 浏览器显示的都是同一个收窄后的视图。

参见[Schema 可见性](#schema-visibility)。

### 第 1 层 — 公开访问 {: #layer-1-public-access }

以 `public` 作用域发布的列会提供给组织中的每个角色，即使该角色的 `domain_access` 并不覆盖这张表的域。这样的角色能看到这张表，但只有其中的公开列。在注册这张表的人作出决定之前，没有任何东西是公开的。参见[公开列](#public-columns)。

### 第 2 层 — 域访问 {: #layer-2-domain-access }

每个角色都有一个由域 ID 组成的 `domain_access` 列表，触及其他域中表的查询会在执行前被拒绝。这是粗粒度的归属边界。无论 SQL 怎么写，人力资源角色都碰不到财务表。

参见[权限模型](#rights-model)。

### 第 3 层 — 行级安全 {: #layer-3-row-level-security }

确认域访问之后，角色对每张表的行过滤条件会作为 `WHERE` 谓词加入每个 `SELECT`，并针对原始数据求值。区域经理即使对共享的订单表执行 `SELECT *`，拿到的也只有自己区域的行。

参见[行级安全 (RLS)](#row-level-security-rls)。

### 第 4 层 — 列可见性与掩码 {: #layer-4-column-visibility-and-masking }

如果某列的 `visible_to` 列表不包含您的角色，该列会从输出中去掉。被掩码的列在离开服务器之前，其值会被正则替换、常量替换或截断所取代。这在所有查询语言和所有输出格式中都成立。

参见[列权限模型](#column-permission-model)和[列级掩码](#column-level-masking)。

### 第 5 层 — 谓词保护 {: #layer-5-predicate-guard }

您不能按被掩码的列过滤：`WHERE` 和 `HAVING` 在解析查询时就会拒绝它。否则，调用方可以通过二分查找一个过滤条件接一个过滤条件地还原真实值，即使它看到的每个结果都是掩码后的。

### 关系治理 (V002) {: #relationship-governance-v002 }

SQL 中的连接必须沿着已注册并已批准的关系进行，其他任何连接都会被拒绝。每个关系都带有原因和描述，向人和智能体说明这条路径为什么存在。请把它看作治理策略，而不是安全边界。无论连接怎么写，第 2 至第 5 层都照样生效，所以绕过这项检查也不会暴露该角色用两条独立查询读不到的任何内容。绕过的尝试会被记录。

绕过 V002 有两种方式。第一种是一项能力：持有 `ignore_relationships` 的角色可以连接目录未覆盖的关系。在预置角色中，只有 `modeler` 拥有它，因为它的工作是把模型梳理出来；`analyst` 没有。

第二种需要两个条件同时满足：

1. **角色标志**：角色定义中的 `relationship_guard: false`（默认是 `true`）。
2. **按查询退出**：SQL 中的注释 `--relationship-guard=false`。

只有其中一个时什么也不会发生。

**高安全模式会锁定这项检查。** 在 `security.mode: high` 下，两种方式都不起作用。`ignore_relationships` 和 `relationship_guard: false` 都会被忽略，每个连接都必须在已批准的目录中。这种冗余是有意为之：误被授予该能力的生产角色依然无法跳出模型。

**GraphQL** 跳过 V002。它的关系来自模型，因此已经获得批准。

**SQL 和 Cypher** 默认应用 V002，并允许上述两个条件的退出方式。

**pgwire** 与 SQL 应用相同的检查。注释 `--relationship-guard=false` 会在查询到达数据库之前被去掉。

---

各层是叠加的。一个同时具有域访问、行过滤和掩码列的角色，这些约束会同时生效。添加数据源、列或关系不需要逐条修改规则，因为每一层都单独配置，并适用于任何触及其所管辖对象的查询。

---

## 权限模型 {: #rights-model }

可独立分配的能力，可通过 `parent_role_id` 构成可选的角色层级。`admin` 授予一切。

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

角色可以通过 `parent_role_id`（在配置中设置，或在安全页面上设为“继承自”）从一个父角色继承。继承链在启动时展开：子角色获得其祖先的能力与域访问的并集、每一项指名某个祖先的列、指标、函数和 webhook 授权，以及祖先按表的行级安全规则，子角色优先。链中最近一个对某表有规则的角色提供该表的谓词，为子角色保存的规则会替换父角色对该表的规则。

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

授予角色直接分配给用户。它携带权利：能力、域访问、脱敏条目、行过滤。继承（`parent_role_id`）在启动时把祖先的权利折叠进子角色。权利只做加法，没有减法。要限制读取者，请授予一个同时指名角色和条件的复合条目，或把一条脱敏条目放在宽松条目之前。

### 到期与未使用的授予 {: #expiring-and-unused-grants }

Provisa 持有的授予可以结束。设置了到期时间的授予从该时刻起，在每个请求中都不再被持有。角色可以声明 `inactivity_days`：若某个该角色的授予超过这一天数未被任何请求依赖（从其最后一次使用与创建时间中较晚者起算），该授予即被移除。来自身份提供商声明或角色映射的角色，其时限由提供商决定，请在那里设置到期。派生角色和元角色永不到期。

当某个角色单独服务一个请求时，或者在请求以多个角色身份行事、且该角色被授予了语句所读取的表时，该请求依赖该角色。以“全部”身份行事并不会把每个持有的角色都计为已使用。授予的最后一次使用每天最多记录一次。

每 15 分钟，每个组织的清理任务会删除已到期和未使用的授予，并在管理审计记录中将每一项记为 `role.expired` 或 `role.removed_inactive`。凡配置了外发邮件的部署（包括只有一个组织的部署），持有人和组织管理员会在授予结束前 `auth.role_grant_notice_days` 天（默认 7 天）收到邮件，授予被移除时管理员也会收到邮件；未配置邮件时，审计记录即为凭证。无论用户的邮件偏好如何，这些通知都会发送。

### 派生角色 {: #derived-roles }

派生角色来自用户的属性。组织声明哪些属性会成为角色（`derived_roles`），每个都带有描述。对于每个已声明的属性 `a`，若用户的属性为其提供了值 `v`，则用户拥有父角色 `a` 及其值角色 `a__v`：`region = eu` 得到 `region` 和 `region__eu`。没有值，就两个都没有。

属性的解析很重要。组织自己关于成员的记录优先于身份提供程序给出的内容：两者指明同一属性时，使用组织的值，提供程序的值被标记为已覆盖。

派生角色不携带权利，也从不手动分配。授权列表、脱敏或行规则指名它们的方式与指名授予角色完全相同；请求在 `X-Provisa-Role` 中指名它们并以其身份执行。

在角色页面上，每个已声明的属性都是一个标记为*派生*的父角色，其值角色随着其值的出现列在它下面。**+ 角色 → 由属性派生**用于声明一个。它的面板显示该属性、其描述、迄今见到的值，以及指名它或其某个值角色的每条脱敏、行规则和授权。它没有授权控件。值角色的描述是自动生成的：“当用户的 region 为 eu 时拥有”。

在配置中以列表形式声明它们：

```yaml
derived_roles:
- attribute: region
  description: The subsidiary a member works for; region__eu marks EU staff.
```

想想 JWT：它的 claim 是关于这个人的事实。派生角色把选定的事实变成角色，这样授权就能针对它们，而不必为每种组合创建一个角色。

### 元角色 {: #meta-roles }

当请求指名多个角色时，Provisa 会构建一个临时元角色，其标识为 `meta:<ids sorted, joined by +>`。它是所指名集合中每个角色的唯一子角色。整个管道随后像对待任何单一角色一样以它运行。

权利是每个成员权利的并集。行过滤是每个成员所允许过滤的 OR；若读取某表的任何成员没有过滤，元角色也没有。没有过滤意味着所有行。元角色从不保存、列出或导出。模型变更时，它会根据其成员重新构建。

每一行的列按照其行过滤允许该行的角色进行脱敏和显示：在这些角色中，采用对该列限制最少的读取方式，未允许该行的角色对其读取方式没有任何影响。脱敏在读取表的地方生效，先于任何连接或聚合，因此聚合读取的是脱敏后的值。写入以同样方式检查：只有当授予该列的角色允许某行时，该列才会在该行上被写入。

审计记录指明每个执行中的角色，而不是元角色 id。由于元角色从不保存，它没有自己的描述：凡是显示描述的地方，它的描述就是其成员的描述，每个 id 各带自己的描述。

### 描述 {: #descriptions }

每个角色和每条行规则都说明自己的用途。访问审查读取谁可以看到什么以及为什么。没有人能解释的角色或规则，也就没有人能审查，因此描述是必需的：描述为空的角色、派生父角色或行规则会被配置加载、管理 API 和 UI 拒绝，并给出原因。角色列表和行规则列表显示每条描述，其详情面板显示并编辑描述。

一个角色只有一个描述，而不是每个域一个。如果一个通用角色没有描述它在某个特定域中的职责，就说明该域需要一个自己的角色，通常是通用角色的子角色（`parent_role_id`），它继承通用角色的权利，并描述它在那里做什么。

### 请求如何指名角色 {: #how-a-request-names-roles }

客户端在 `X-Provisa-Role` 中发送一个角色或逗号分隔的集合。所指名的每个角色都必须为用户所持有。未分配给该身份的角色会被点名拒绝。

权利和数据只跟随请求所执行的角色：持有一个角色不等于以它执行。一个角色就是该角色；两个或更多角色以它们的元角色执行。如果执行角色会带上所持有的每个角色的权利，它就没有意义，因此请求获得最小权限，审计记录准确指明实际执行的内容。

被指名的派生角色以限定符身份进入。限定符没有自己的权利、域访问、脱敏或行过滤，因此它不是元角色的权利成员：作为没有脱敏的成员，它会解除每一列的脱敏；作为不读取任何表的成员，它会终结任何行过滤的 OR。它只进入授权：指名 `region__eu` 的授权或脱敏条目，会触达同时指名该限定符和一个授予角色的请求。

MCP 代理从不超出赋予它的角色行事。远程代理只能指名其令牌主体所持有的角色。本地 stdio 代理只能指名 `mcp.role`（`PROVISA_MCP_ROLE`）中固定的集合内的角色，未指名任何角色时即以该集合行事；任何其他角色都会被点名拒绝。

代理通过 MCP 工具 `describe_role` 询问自己以哪个角色执行。答案是该角色及其用途（元角色则附上其每个成员），以及代理可以切换到的角色，每个都附有其描述。答案从不包含行规则、脱敏、授权或条件。Provisa 从不告诉代理结果为何被缩小，结果也不带任何“已过滤”标记：角色的数据就是它的全部视图，而说明缺少了什么的理由会诱使人去探查它。

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
- 把最具体的脱敏条目放在最前。即使读取者持有后面的条目，前面的条目也优先。
- 在组合投入生产之前，用 `POST /admin/masks/check` 检查实际访问。

## 列权限模型 {: #column-permission-model }

每一列都有一个权限模型，按角色控制读取、写入和脱敏。

### 三级可见性 {: #three-tier-visibility }

| 级别 | 条件 | 结果 |
| ------ | ----------- | -------- |
| **隐藏** | 角色不在 `visible_to` 中 | 该列不在 schema 和目录中 |
| **脱敏** | 角色在 `visible_to` 中；它所持有的第一条脱敏条目有 `mask` | 列可见，输出中的值被替换 |
| **未脱敏** | 角色在 `visible_to` 中；它所持有的第一条脱敏条目没有 `mask`，或它没有任何条目 | 完全读取权限 |

### 公开列 {: #public-columns }

每一列都有一个 `scope`，在模型文件、通过管理 API 或在表页面上设置。三种范围按其服务对象排序：`restricted`，然后 `domain`，然后 `public`。表的编辑者（表所在域的 `table_registration`）可以收窄列的范围。放宽它——`restricted` 到 `domain` 或 `public`，`domain` 到 `public`——会把该列提供给更多角色，并需要其授权所需的权利：表所在域的 `column_grant` 或 `access_config`。正在注册的列以 `domain` 为基准衡量，因此将其注册为 `public` 属于放宽。对于带敏感标签的列，任何范围变更都需要 `sensitive_data`。被拒绝的变更会指明列和权利。

| 范围 | 提供给 |
| ------- | ----------- |
| `domain`（默认） | 覆盖该表所在域的角色，按 `visible_to` 所述。空的 `visible_to` 即每个这样的角色。|
| `public` | 同样这些角色，以及未覆盖该表所在域的角色。|
| `restricted` | 仅 `visible_to` 所指名的角色。空的 `visible_to` 即没有人。|

发布一列只为该列解除域条件，仅此而已：

- 对于域外的角色，表只带着其公开列提供。表的其他列，以及不发布任何内容的表，都不在它的 schema、目录和 SQL 中。
- 列自身的 `visible_to` 仍决定提供给谁：只授予一个角色的公开列只对该角色公开。
- 脱敏、行过滤、行数上限和关系防护的适用方式与域内相同。角色没有行过滤时，已发布的列不经过滤读取；表或其所在域上的行过滤仍然适用。
- 关系在其两端的列都被提供时才被提供。
- API 表的参数列（路径或查询参数）不是数据，也不发布任何内容。
- `ops` 域要求逐列显式授权；无论范围如何，其中任何内容都不会跨域提供。

该规则在每个界面上都相同，因为每个界面都读取角色编译后的 schema：GraphQL、SQL（HTTP、pgwire、Flight、JDBC）、Cypher 和 Bolt、gRPC、REST 和 JSON:API、Flight 和 MCP 目录，以及管理列表。不是这三个值之一的范围会在保存列时被拒绝。

### 写入授权 {: #write-grants }

读取一列由 `visible_to` 控制（为空表示所有角色都可以读取）。写入按表由 `write_grants` 控制，而不是按列。表默认没有任何授权，因此任何角色都不能写入它。

写入授权声明某个角色通过一种操作（`insert`、`update` 或 `delete`）可以向某张表写入什么：

| 字段 | 用途 |
| ------- | --------- |
| `columns` | `INSERT` 可以列出、`UPDATE` 可以设置的列 |
| `rows` | `UPDATE` 或 `DELETE` 可以触及的行（SQL 谓词） |
| `check` | `INSERT` 写入的每一行、或 `UPDATE` 留下的每一行必须满足的条件（SQL 谓词） |
| `presets` | 授权自己设置的列，来自会话变量、字面量或写入时间。语句不得提供这些列。|

省略 `rows` 或 `check` 时，授权采用该角色在这张表上的读取行过滤器，因此角色写入的就是它能读到的行。`"TRUE"` 表示所有行。

同一个准入机制决定每个界面上的每次写入，顺序如下：角色持有 `write` 权利；持有表上该操作的授权；语句写入的每一列都在授权中；添加预设；`rows` 以 AND 并入 `WHERE`；在向数据源发送任何内容之前，对新行评估 `check`。拒绝信息会指明表、角色和操作。

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

已注册的变更操作（远程 GraphQL、OpenAPI、gRPC、Hasura）要通过两项检查。角色必须持有全局能力 `write`，且该命令必须通过 `visible_to` 分配给它；空列表表示分配给所有角色。命令没有 `writable_by` 列表，配置中出现这样的列表会被按名称拒绝。

一个操作是不是写入，由它的契约决定，而不是由调用方说了算。调用变更类函数的 `SELECT` 会被当作写入，并经过同样的两项检查，所以您无法把变更操作伪装成读取来运行。将某个变更操作标记为读取安全需要 `access_config` 能力，并会作为治理决定记录下来。任何请求都不能自行豁免。

## 架构可见性 {: #schema-visibility }

每个角色都有自己的 GraphQL schema。只会出现其 `domain_access` 中各个域的表（`"*"` 表示全部），某列若不在该角色的 `visible_to` 中，也不会出现在 SDL 里。角色读不到的，它也看不到。

## 行级安全 (RLS) {: #row-level-security-rls }

行过滤条件是针对一张表、一个角色的 `WHERE` 谓词，在编译之后、执行之前加入。

```yaml
rls_rules:
  - table_id: orders
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"
    description: Analysts read only their own region's orders.
```

过滤条件以 AND 并入查询的 `WHERE` 子句，读取时如此，`UPDATE` 和 `DELETE` 时也是如此。

过滤条件通过 `current_setting('provisa.<name>')` 读取请求。请求会把 `user_id` 绑定为已登录的身份，把 `role` 绑定为当前行事的角色，并绑定令牌中的每个标量声明（claim），名称转为小写，连字符改为下划线。`provisa.user_id` 始终是已登录的身份，任何声明都无法替换它。

Hasura 的会话变量保留原名。`X-Hasura-User-Id` 即 `hasura_user_id`，取自令牌的 `x-hasura-user-id` 声明（位于顶层，或位于 Hasura 声明对象内，例如 `https://hasura.io/jwt/claims`），或取自为该成员记录的 `hasura_user_id` 属性。因此，导入的基于 `X-Hasura-User-Id` 的 Hasura 过滤条件读取的是 `provisa.hasura_user_id`。

请求的值之下是角色配置的 `session_vars`，请求会覆盖它们。继承的角色会在自己的常量之下取得祖先的常量，同名时以最近的角色为准。`user_id`、`role`、`region` 和 `org` 来自请求或节点，不能配置为常量。在不安全模式下，`x-provisa-session-<name>` 请求头会绑定 `<name>`。哪里都没有绑定的变量为 NULL，基于它的过滤条件不会匹配任何行。

角色常量的作用方向与派生角色相反。派生角色把成员持有的值变成一个角色。角色常量则给以该角色发出的每个请求一个值，行规则读取它的方式与读取声明相同。常量可以是 `${env:NAME}` 或 `${secret:NAME}` 引用，在查询用到它时，由与其他引用相同的解析器解析。模型保存的是引用，从不保存值；解析不出任何内容的引用会使查询失败，并指出该常量。解析之后，这个值就是查询的一部分：Provisa 自己的记录受集中涂黑保护，但联邦数据源自己的日志不在 Provisa 的控制范围内。

## 列级脱敏 {: #column-level-masking }

脱敏以每列一个有序列表的形式配置。读取者持有的第一条条目决定其看到的内容。

| 脱敏类型 | 支持的列类型 | SQL 表达式 |
| ----------- | ----------------------- | --------------- |
| `regex` | 字符串（varchar、char、text） | `REGEXP_REPLACE(col, pattern, replace)` |
| `constant` | 任意 | 字面量值（`NULL`、`0`、自定义字符串） |
| `truncate` | 日期/时间戳 | `DATE_TRUNC(precision, col)` |

每条条目都有一个 `to` 列表，指名其针对的读取者，以及一个可选的 `mask`。没有 `mask` 的条目对这些读取者表示“不脱敏”。`to` 中的 `*` 匹配任何读取者。

脱敏表达式被下推到 SQL SELECT 投影中。数据库返回脱敏后的值；对于脱敏角色，未脱敏数据从不经过网络传输。脱敏列在 `WHERE` 和 `HAVING` 子句中也被阻止（第 5 层谓词防护），以防止通过过滤推断值。

**脱敏覆盖列的每一个名称，但不覆盖关联列。** 按别名或物理名称读取的列以相同方式脱敏。脱敏不会隐藏已批准关系与角色可见列连接的键：连接比较的是真实值，因此关联列会显示被脱敏键的内容。请同时对关联列脱敏，或不要为该角色批准此关系。

**没有条目，就不脱敏。** 不持有任何条目的读取者读取未脱敏的列。当每个读取者都应受到某种脱敏时，请以一条 `*` 条目结束列表。列表没有 `*` 条目时，列编辑器会发出警告（`unlisted_unmasked`）。

**按顺序拒绝。** 即使读取者还持有后面一条更宽松的条目，前面的条目也优先。要让承包商无论还有哪些其他角色都始终看到脱敏的薪资，请把承包商条目放在不脱敏条目之前。授予角色从不失去父角色通过继承给予的内容，但在列表中位置靠前的脱敏条目会先适用。这就是这里限制的方式。

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

被跟踪函数或 webhook 返回的内容，与表的行一样受治理。Provisa 把返回的行绑定为一个关系，其列就是该操作声明的输出：函数为 `output_columns`，webhook 为 `inline_return_type`，或函数数组型 `return_schema` 的对象属性。随后，这个关系在一个保存这些行的 VALUES CTE 上，经过与任何表读取相同的治理阶段，因此行安全、掩码和可见性无需为操作重新实现。

有三条规则，按与表相同的顺序应用：

- **行过滤。** 角色按操作名称为该操作保存的 RLS 规则（带 `actionName` 的 `upsertRlsRule`），若操作没有自己的规则，则用操作所在域的规则。谓词在保存时会对照契约检查，与表规则一样。
- **列可见性。** 契约中声明了 `visible_to` 的列，对列表未包含的任何角色都会去掉。没有声明的列属于操作的公开形状，会保留。
- **掩码。** 契约列带有 `masks`，与表列使用同一种有序列表。

角色继承会为这三条规则解析继承链。如果响应返回了声明契约之外的列，调用会以 502 失败，而不是让它不受治理地通过。没有声明任何列的操作返回一个标量，没有东西可绑定，按原样返回。管理端的测试调用会报告应用了哪些过滤、排除和掩码，受治理的语句会写入查询审计日志。

## 采样 {: #sampling }

每个角色得到的都是结果的样本，默认 100 行，除非它拥有 `full_results` 能力。用环境变量 `PROVISA_SAMPLE_SIZE` 设置样本大小。

## 审计日志 {: #audit-logging }

每条触及域资产的查询都会写入 `query_audit_log`。日志属于组织，而不属于平台：平台自己的组织、用户和邀请登记中不保存任何审计记录。每个组织的日志是其 schema 中的一张表，位于该组织为其所选每个区域的记录指定的存储中。没有声明区域的组织会把它保存在部署的控制平面数据库中，与其模型放在一起。

一行记录 `tenant_id`（组织）、`user_id`、`role_id`、查询文本的 SHA-256 哈希、查询文本本身（加密）、`table_ids` 和 `table_names`、`source`、`status_code`、`duration_ms`、路由和行数、对该语句实施了什么，以及 `logged_at`。如果组织设置了自己的密钥，文本就用它加密，否则用部署的密钥。没有配置加密提供方的部署会以明文保存。无论哪种情况，它都不会出现在可查询的运维表中，只在授权的管理读取时才会解密。

日志在数据库层面只能追加：PostgreSQL 规则把对 `query_audit_log` 的 `DELETE` 和 `UPDATE` 变成空操作。两个索引 `(tenant_id, logged_at)` 和 `(user_id, logged_at)` 让按组织、按用户在某个时间范围内的合规查询保持快速。

行会一直保留。Provisa 没有保留期设置，也从不清理日志；由于 `DELETE` 不起作用，要清除旧行，只能由运维人员在 Provisa 之外删除或归档该表的数据。

## 涂黑 {: #redaction }

Provisa 对自身工作所做的记录，在写入任何地方之前，都要先经过一份涂黑规则列表。与某条规则匹配的文本会被替换为 `[REDACTED:<rule>]`，其中写明所匹配的规则。

这份列表由四部分组成：

- **内置模式。** 通过 Luhn 校验的卡号、美国社会安全号码、bearer 令牌、Provisa 访问令牌、AWS、GCP、Azure、GitHub 和 Slack 的密钥，以及私钥。它们只是一组起步规则，并非保证。
- **部署的规则**，在每个组织中生效。修改它们需要 `platform_settings` 权限。
- **组织自己的规则。** 修改它们需要 `org_settings` 权限。
- **自动规则**，覆盖组织的密钥：Provisa 自身存储为该组织保存的每个值，组织的模型和设置中提到的每个 `${secret:NAME}`（在任何密钥后端上），以及每个 `${secret:…}` 或 `${user:…}` 值，从其首次解析起即生效。无需任何规则点名。

一条规则可以是一个具名的正则表达式，也可以是一个具名的 `${secret:NAME}` / `${env:NAME}` 引用，后者匹配该引用解析得到的值。无法解析的引用会按名称以 `ERROR` 级别记入日志，并显示为未解析；其余规则照常生效。少于四个字符的值不会被匹配。

Provisa 无从知晓每一种敏感格式。内置模式覆盖常见形式；组织为自己的格式添加模式，例如员工编号、账户标识或内部主机名。涂黑尽力而为：以任何规则都未描述的形式写出的值会原样通过。

这些规则位于 **组织密钥** 页面的 **涂黑规则** 下。该页面列出内置模式、部署的规则（持有 `platform_settings` 时可编辑）、组织的规则、自动规则所涵盖的密钥（仅显示名称），以及每个无法解析的引用。规则在服务器启动时和组织的运行时构建时加载，并在模型或设置变更时、以及组织密钥被存储或删除时重新加载。

这份列表作用于 Provisa 写出的每个出口：

- 每个 span，以一份涂黑后的副本交给每个导出器，以及最近 span 视图；
- Provisa 的 OpenTelemetry 日志管道中的每条日志记录，以及日志处理器写出的每一行，包括标准输出和容器日志；
- 审计日志中的查询文本，在计算哈希和加密之前进行涂黑，因此文本及其哈希都不会泄露被匹配的值；以及管理记录的详情；
- 客户端通过 HTTP、pgwire、gRPC、Arrow Flight 和 MCP 收到的错误消息。

遥测数据和客户端错误在写入时并不归属于某一个组织，因此会按所有组织的规则一并涂黑。有两类错误不经涂黑直接通过：大于 1 MiB 的 HTTP 错误响应体，以及在开始发送之后才失败的 Arrow Flight 流的错误。

联邦数据源保留自己的日志，Provisa 既不写入也无法触及。数据源对 Provisa 发送给它的语句所做的记录，由该数据源负责保护。

## 通过 MCP 进行统计 {: #statistics-over-mcp }

MCP 统计工具（`describe`、`correlate`、`regress`、`compare`、`forecast`）以智能体的角色，通过受治理流水线运行智能体定义的 SQL，因此行规则、脱敏和涂黑都作用于它们所分析的内容。统计工具只返回结果，因此分析背后的行永远不会进入模型的上下文：默认即数据最小化。

限制智能体所能获知内容的是访问控制。要让身份信息不在智能体可及范围内，只向其角色授予不识别任何人的列；能读取某列的角色就能对其聚合，而对小分组的聚合仍可能识别出个人。

## 敏感列覆盖 {: #sensitive-column-coverage }

meta 域中的 `sensitive_coverage` 视图列出组织的每个敏感列，附上其声明的虚构值和合成规则，以及状态：`covered`、`no_fake`，或当 Provisa 的某项充分性检查未通过时为 `inadequate`。这些检查查找：让真实值保持不变的虚构值、种类与列中内容不符的虚构值，以及读取另一敏感列真实值的虚构值或规则。数据管家可在每个环境的详情中看到没有虚构值的列；在每个敏感列都有虚构值之前，Test (fake) 环境拒绝提供数据。检查和视图的说明见[构建测试环境](test-data.md#coverage-and-adequacy-checks)。

## 速率限制 {: #rate-limiting }

按角色的限制在 `provisa.yaml` 中设置：每秒请求数、并发 SSE 订阅数和并发 Arrow Flight 流数。它们在 API 层检查，早于任何编译或执行；超出限制的请求会收到 HTTP 429 和 `Retry-After` 响应头。

自然语言查询（`POST /query/nl`）有自己的限制 `nl.rate_limit`，单位是每个角色每分钟的请求数。超出的请求会在调用任何 LLM 之前被拒绝。

计数以滑动窗口的形式保存在 Redis（`cache.redis_url`）中，不在单个实例上保存状态，因此无论运行多少个 Provisa 实例，限制都同样生效。

## 查询复杂度上限 {: #query-complexity-limit }

速率限制约束角色发送多少请求。复杂度上限约束单条语句可以请求什么。

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

可插拔的身份验证提供程序：

| 提供程序 | 凭据 | 场景 |
| ---------- | ----------- | ---------- |
| `none` | X-Provisa-Role 标头 | 仅用于开发 |
| `basic` | 本地账户（bcrypt）+ 会话 JWT | 自托管部署 |
| `firebase` | Firebase ID 令牌 | 生产 |
| `keycloak` | Keycloak JWT | 企业 |
| `oauth` / `oidc` | OIDC JWT | PingFed、Okta、Azure AD、Auth0 |
| `ldap` | 目录用户名和密码（LDAP 或 Active Directory） | 企业目录 |
| `saml` | SAML 2.0 签名断言，然后是会话 JWT | 企业单点登录 |
| `simple` | bcrypt + JWT | 测试 |

`ldap` 采用先搜索后绑定的方式让用户登录：Provisa 以服务账户绑定，找到用户名所指向的那一个条目，再以该条目和用户提交的密码进行绑定，由目录来校验密码。用户所属的组成为其角色。每次会话令牌返回时，都会重新读取账户及其组，因此从目录或从某个组中移除的用户，会在令牌过期之前失去相应访问权限。`ldaps://` 和 StartTLS 均受支持。

`saml` 是由 SP 发起的 Web Browser SSO。响应的 XML 签名依据标识提供程序的元数据验证，claim 只从已签名的元素中读取。Issuer、Audience、ACS URL、有效期和 `InResponseTo` 均会检查。未经请求（由 IdP 发起）的响应、加密断言、带 DOCTYPE 的文档、多个断言和 SHA-1 签名均被拒绝。SAML 会话不会再向标识提供程序复核：在那里被移除的用户，在会话令牌过期之前仍保留访问权限。

角色映射：身份 claim → 通过可配置规则映射为 Provisa 角色。`assignments_source` 字段控制角色分配的来源：`claims` 从 JWT 令牌 claim 中读取（默认），`provisa` 从 Provisa 内部的分配存储中读取。多租户部署（`multitenancy: true`）按组织自行分配角色，因此必须使用 `assignments_source: provisa`：使用 `claims` 时它不会启动，会导致这一组合的设置变更也会被拒绝。单租户部署可以使用任一选项。

在 `provisa.yaml` 中配置的超级用户（用户名加来自环境密钥的密码）无论配置了哪个提供程序，总是获得 admin 角色和所有能力，这是初始设置的引导路径。

### 接口与凭据 {: #surfaces-and-credentials }

每个接口都通过同一个提供方验证调用方，因此一份凭据在一个接口上能用，在所有接口上都能用，只要协议有地方承载它。

| 接口 | 密码 | 提供程序令牌 | 个人访问令牌 | 客户端证书 (mTLS) |
| --------- | ---------- | ---------------- | ----------------------- | --------------------------- |
| HTTP（REST、JSON:API、GraphQL） | `Authorization: Basic` | `Authorization: Bearer` | `Authorization: Bearer` | 通过终止代理 |
| pgwire | 密码字段（明文或 SCRAM） | 密码字段，OIDC 部署 | 密码字段 | 是 |
| Bolt | `basic` 方案 | `bearer` 方案 | `bearer` 方案 | 是 |
| Arrow Flight | — | 握手或 ticket 负载中的 `token`；`list_flights`、`get_flight_info` 和 `get_schema` 使用调用标头 `authorization: Bearer` | 同上 | 是 |
| gRPC | — | `authorization` 元数据 | `authorization` 元数据 | 是 |
| Airport（DuckDB `airport` 扩展，可选） | — | `authorization: Bearer`（密钥的 `auth_token`） | 同上 | — |
| MCP | — | `Authorization: Bearer` | `Authorization: Bearer` | 通过终止代理 |

DuckDB 的 `airport` 扩展只能发送其密钥中的 bearer `auth_token`。DuckDB 客户端在那里填入提供方令牌或个人访问令牌，并以其身份解析到的角色行事。任何能设置调用请求头的客户端，都可以在 `x-provisa-role` 中请求自己的某个角色，或以逗号分隔的一组角色。

每个接口都会告诉您收到的是哪一类拒绝。缺失、被拒或不可用的凭据属于认证拒绝：HTTP 401、gRPC `UNAUTHENTICATED`、Arrow Flight 和 Airport `Unauthenticated`。有效凭据请求以其身份并不持有的角色行事，属于授权拒绝，并会指出该角色：HTTP 403、gRPC `PERMISSION_DENIED`、Arrow Flight 和 Airport `Unauthorized`。有一个例外来自 Arrow 客户端库，而不是服务器。Flight 的 `get_schema` 被拒绝时，Python 客户端看到的是一个通用的 Arrow 错误，带有服务器给出的原因，但没有拒绝的类别；`list_flights`、`get_flight_info` 和 `do_get` 会带上类别。

表中的 `—` 表示该协议没有可与密码配对的用户名字段，由令牌形式覆盖。pgwire 是相反的情况。它的启动包只有一个密钥字段，没有方案，因此由密钥本身决定方式：个人访问令牌按前缀识别；当配置的提供方签发令牌时，密钥按 bearer 令牌读取；其余一律视为密码。这个选择只做一次，被所选验证器拒绝的凭据不会再交给另一个验证器尝试。

### 个人访问令牌 {: #personal-access-tokens }

个人访问令牌是一种长期有效的 bearer 密钥，供无法完成交互式登录的客户端使用：脚本、BI 工具、驱动程序。它自带组织和角色，每个接口都用同一个验证器检查它，因此没有哪个接口需要知道它是什么。

在传输中，它的形式是 `provisa_pat_` 后跟 43 个 URL 安全的 base64 字符。正是这个前缀把提交的密钥送往令牌存储，而不是身份提供方，也让泄露的令牌在日志和代码库中很容易被找到。

只保存密钥的 SHA-256。您只在创建时看到一次密钥，之后无法找回；您的令牌列表只显示展示用前缀和时间，绝不显示可用的凭据。用 `POST /auth/tokens`、`GET /auth/tokens` 和 `DELETE /auth/tokens/{token_hash}`，或在管理界面中您个人资料的令牌部分，创建、列出和吊销您自己的令牌。

有效令牌会解析到其所有者的账号，带有用户 ID、电子邮件和显示名称，因此以令牌写下的审计行或用量报告指名的是人，而不是凭据。具体是该用户的哪个令牌在行事，单独记录在 `raw_claims["token_name"]` 中。令牌可以带有过期时间，过期后会被拒绝；移除用户的成员身份也会一并吊销其令牌。

### pgwire 上的 SCRAM-SHA-256 {: #scram-sha-256-on-pgwire }

使用 `basic` 提供程序时，设置 `auth.scram: true` 会让 pgwire 以 `SCRAM-SHA-256` 机制提供 SASL（身份验证代码 10），从而证明密码而不是发送密码。不提供通道绑定（`SCRAM-SHA-256-PLUS`）。

SCRAM 需要 RFC 5802 验证器，它无法从 bcrypt 哈希推导出来。每当密码以明文经过时（注册、登录、修改密码、管理员重置）都会写入验证器，因此启用 SCRAM 的部署会在用户下一次进行身份验证时收集验证器，每个用户的第一次 SCRAM 连接发生在其下一次输入密码之后。尚无验证器的用户会得到一次与真实交换无法区分的模拟交换，因此网络不会暴露谁已经迁移。

### 双向 TLS {: #mutual-tls }

客户端证书验证把第一道检查移到 TLS 握手中：没有由部署的 CA 签发的证书的调用者，永远到达不了凭据层。它在 pgwire、Bolt、gRPC 和 Arrow Flight 上可用：这四种传输自行终止其 TLS。

| 变量 | 含义 |
| ---------- | --------- |
| `PROVISA_MTLS_CLIENT_CA` | 允许签发客户端证书的 CA 的 PEM 包 |
| `PROVISA_MTLS_MODE` | `required`（一旦设置 CA 即为默认）或 `optional` |
| `PROVISA_MTLS_BIND_PRINCIPAL` | 为 true 时，证书的通用名必须与连接随后进行身份验证所用的用户名相匹配 |

按协议的覆盖遵循与 TLS 设置相同的命名。不做任何推断：没有 CA 而设置了模式会拒绝启动，无法识别的模式也会拒绝启动，而不是被读作最安全的相邻值。一个以为自己要求客户端证书但实际并未要求的部署，比一个不启动的部署处境更糟。

### TLS 版本 {: #tls-versions }

Provisa 终结 TLS 的每个监听器（HTTP API 和 UI、pgwire、Bolt、MCP、gRPC 和 Arrow Flight）都接受 TLS 1.2 和 TLS 1.3，并拒绝 TLS 1.0 和 1.1。Provisa 自己不设置协议版本或密码套件列表。HTTP、pgwire 和 Bolt 监听器使用 Python `ssl` 模块的服务器默认值（最低 TLS 1.2）。gRPC 和 Arrow Flight 使用 gRPC 的默认值（最低 TLS 1.2）。这些版本不可配置。

### 登录限流 {: #login-throttling }

密码猜测与协议无关：同一个账户可以通过 HTTP、pgwire 和 Bolt 受到攻击。因此计数器位于凭据验证层，而不是任何单个接口上，从而在任何地方触发的锁定在所有地方都有效。

它默认启用（五分钟内失败五次会将主体锁定十五分钟）并可在 `auth.login_throttle` 中配置。被锁定的主体会在凭据被检查之前就被拒绝，成功的身份验证会清除该主体的历史。

键是协议所携带的主体。仅 bearer 的接口不携带主体，因此键是凭据本身的摘要；这可防止无限制地重放错误的令牌。存储在进程内，因此有多个 API worker 的部署允许每个 worker 最多 `max_attempts` 次。该限流是减慢猜测，而不是分布式配额。

### 角色探测 {: #role-probing }

指名调用方并不持有的角色，会在每个接口上被视为已检测到的高严重性安全事件：HTTP API、MCP、pgwire、Bolt、Arrow Flight、airport Flight 服务和 gRPC。合法的客户端只指名分配给它的角色；指名其他角色的客户端不是出了故障，就是在探测。

请求被拒绝。调用方只会得到 "not permitted"，不会得知该角色是否存在；有连接或会话的地方，连接或会话随之结束：pgwire 连接、Bolt 会话、gRPC 调用、MCP 会话。例外是使用个人访问令牌以外任何凭据的 HTTP API：浏览器在那里出示其会话或其身份提供方的令牌，因此拒绝信息仍会指名该角色。

其余处理取决于凭据和传输方式：

| 凭据 | 传输方式 | 暂停对象 |
|---|---|---|
| 演示沙盒访客的令牌 | HTTP API | 无 |
| 个人访问令牌 | 任意 | 该令牌 |
| 由外部身份提供方签发的令牌 | MCP、pgwire、Bolt、Flight、gRPC | 该主体对组织的令牌访问 |
| 由外部身份提供方签发的令牌 | HTTP API | 无 |
| 由本部署自行校验的会话或密码 | 任意 | 无 |
| 本地 MCP 会话（无 bearer） | MCP | 无；事件写入运维方日志 |

HTTP API 之所以例外，是因为浏览器出示的 bearer 令牌与 API 客户端相同，服务器无法区分两者；而标签页仍指名用户已失去的角色（在管理员更改其角色后，或切换组织后）是常见情况。MCP 及其他传输方式从来不是浏览器会话。在那里，事件会像其他事件一样被拒绝、记录并通知；只是不执行暂停。演示沙盒访客的令牌是个人访问令牌，但它是访客浏览器标签页唯一的凭据，因此在 HTTP API 上按会话对待：拒绝并记录，从不暂停。

被暂停的令牌会像已吊销的令牌一样被拒绝。令牌访问被暂停的主体，在该组织中通过 MCP、pgwire、Bolt、Flight 和 gRPC 的请求都会被拒绝，但仍可登录界面。只有管理员能解除暂停；暂停不会因时间到期而解除。团队 → 已暂停的访问 会列出每项暂停及其起因事件，并可将其解除（`GET /admin/orgs/{org}/suspensions`、`POST /admin/orgs/{org}/suspensions/lift`）。

每个事件都会写入组织的管理日志（操作 `security.role_probe`），包括主体、凭据 ID、客户端地址、传输方式、端点或工具、所指名的角色、所持有的角色、时间，以及该主体最近经过审计的语句的 ID。语句文本保留在查询审计日志中，需具备 `view_governance` 权限才能读取。

在部署会发送邮件的情况下，组织管理员会收到邮件。设置了 `security.webhook_url` 时，事件还会以 JSON 发送到运维方的安全 webhook。离开组织的内容包括主体、凭据 ID、传输方式、端点、所指名的角色、时间和严重性，并附有指向管理视图的链接；不包含任何语句，也不包含所持有的角色。无法送达的通知会被记入日志，且绝不会撤销拒绝或暂停。

### 指定组织 {: #naming-the-org }

在多租户模式下，每个请求都指名其目标组织。只属于一个组织并不等于指名了它：未指名任何组织的请求会被拒绝，拒绝会说明应发送什么。各接口指名组织的方式如下：

| 接口 | 客户端如何指名组织 |
| ------- | -------------------------- |
| HTTP（界面、REST、GraphQL） | 组织自己的主机名 `acme.provisa.dev`，或在控制平面主机上使用 `X-Org-Provisa` 标头 |
| pgwire | 数据库名（`psql -d acme`、BI 工具的数据库字段），或通过 TLS 拨号的组织自己的主机名 |
| Bolt | 数据库名 `<org>.provisa_<role>`（或 `<org>.provisa_ops_<role>`），或通过 TLS 拨号的组织自己的主机名 |
| Arrow Flight | ticket 中的 `"org"` |
| gRPC | `x-provisa-org` 元数据标头 |
| MCP（HTTP 传输） | 组织自己的主机名或 `X-Org-Provisa` 标头 |
| Python 客户端 | `ProvisaClient`、`connect()` 和 `adbc_connect()` 中的 `org=`；SQLAlchemy URL 中的 `?org=` |

通过 TLS 拨号主机名的 pgwire 客户端会在 TLS ClientHello 中发送该主机名，Provisa 从中读取组织。当主机名和数据库名都指名了组织且二者不同时，连接会被拒绝；二者都不优先。默认使用的数据库名 `provisa` 不指名组织，连接到某组织的 BI 工具会把
该组织显示为其数据库。在 Bolt 上，`SHOW DATABASES` 为用户所属的每个组织
列出一个 `<org>.provisa_<role>`；在多租户模式下，单独的 `provisa_<role>` 会被拒绝，会话保持在
它最先指名的组织中。

名称是请求，而不是授权。在任何地方它都会到达同一个解析器，该解析器会拒绝已验证主体不是其成员、且对其没有跨组织权利的组织。

个人访问令牌为一个组织签发，并自行指名该组织。它只开放该组织：指名另一个组织的请求会被拒绝，即使令牌所有者同时属于两者。

单租户部署没有需要指名的组织。在那里，没有任何接口为此读取主机名、数据库名或标头。

## 跨组织的密钥 {: #secrets-across-organizations }

组织的密钥只由该组织读取。在 Provisa 自己的存储中，每个密钥都保存在其组织之下。在承载多个组织的部署的中央服务中（Vault、AWS Secrets Manager、GCP Secret Manager、Azure Key Vault），每次读取都在读取方组织的前缀内进行，会离开该前缀的引用会被拒绝。前缀以及将已有密钥迁移到前缀下的步骤，参见[密钥](secrets.md#one-organizations-secrets-in-a-central-service)。

## 跨组织的遥测 {: #telemetry-across-organizations }

在许多组织共享的部署上，运维信号（`"ops"."traces"`，在 Trino 引擎上还有旁边的指标和日志）是所有组织的工作都写入的同一个存储。每个 span 记录启动它所服务的组织，保存为 `tenant_id`；对这些信号的每次读取只返回读取方组织的行，与角色无关。在任何组织的工作之外启动的 span（例如服务器启动时）不属于任何组织，也没有组织会读到它。平台管理员在任何组织中都没有数据访问权，也不通过 Provisa 读取其中任何一个。

运维信号在两种引擎上都会无限期保留，与审计日志一样：压缩会重写存放它们的文件，但不删除任何行。在 Trino 上，它们是 `otel` 目录的 Iceberg 表。在原生引擎（DuckDB）上，服务器把自身的追踪记录以 Parquet 文件写入其遥测文件夹，引擎以同一名称 `"ops"."traces"` 读取它们，并在其上提供刷新血缘视图 `"ops"."mv_refresh_lineage"`；两者都按同一条按组织划分的规则读取。

信号在其存储中以明文保存。需要在一个部署内部比按组织划分更强边界的工作负载，运行在专用部署上；高度敏感或受监管的工作负载则自托管。

## 跨组织的后台工作 {: #background-work-across-organizations }

服务多个组织的进程，会以各组织自己的身份运行其后台工作。每个组织的变更处理（落地数据源变更并刷新其上视图的事件循环）、副本构建以及由此触发的表触发器，都属于该组织自己的运行时：变更由其所属组织的作业处理，使用该组织的地址和凭据读取其数据源，并写入该组织的存储。组织的每个环境都持有自己的 PostgreSQL 逻辑复制槽和发布，按组织、环境和数据源命名，因此两个在同一数据库上为数据源取相同名称的组织，各自读取自己的变更。

## 数据驻留 {: #data-residency }

在带有区域的部署中，表可以将一个区域指定为其归属区域，其个人数据在那里存储和处理。

- 只有归属区域构建和刷新该表的副本。其他每个区域都就地读取该副本，读者的行规则和列规则在归属区域中应用，因此离开该区域的只有读者被允许看到的内容。
- 归属区域的副本尚未构建，或其存储无法访问时，会拒绝读取（`query.home_region_unavailable`）。绝不会改为从另一个区域实时读取数据源。
- 草稿只在其所在之处预览。指定了区域的草稿表，只在该区域的节点上为其负责人的预览而读取，在其他地方被拒绝；预览落地的副本保留在那里，等待其发布。任何区域持有的某个表（无论草稿还是已发布）的副本，若该表的归属已改为另一个区域，则会被停用，随后删除。
- 设置、更改或移除表的区域，除了通过其域拥有该表之外，还需要涉及区域的 `data_residency` 权限。迁移可以经过草稿，使该表在此期间不在任何地方提供服务。

基于应答节点所在区域（`current_setting('provisa.region')`）和成员的 `region` 属性编写的行规则与列规则，决定哪些内容在区域之间流动。[区域](regions.md) 完整介绍区域、归属指定以及这些规则。

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

- **节点之间。** 协调器与其 worker 使用为部署生成的共享密钥（`internal-communication.shared-secret`）相互认证。加密它们之间的流量是运营者的可选项，默认关闭：关闭时，流量保持在部署的私有网络或集群之内。当合规或零信任要求部署内部也须加密时再开启。Trino 文档称它会使 shuffle 密集型查询（分布式连接、聚合、窗口函数）变慢 10% 到 100% 以上。在 compose 部署上，它是设置 `engine.trino_internal_tls`（Deployment settings → Engine），旁边是引擎网络和协调器的固定地址，并在下次启动时生效。在 Helm chart 下，它是值 `trino.internalTls`；chart 生成节点使用的 CA 和证书，在升级之间保留它们，并按每个节点的 pod DNS 名称寻址。托管平台的引擎是单节点协调器，没有节点间流量，并拒绝该选项。
- **假脱机数据。** 容错执行用为每个查询生成、查询结束即丢弃的密钥，加密它假脱机的交换数据（`fault-tolerant-execution.exchange-encryption-enabled=true`，显式设置）。不使用旧式的溢写到磁盘。
- **同一引擎上的多个组织。** 每个组织的 catalog 属于它自己，以该组织命名，每个组织的语句在它自己的资源组中运行。
- **目录及其中的凭据只存在于引擎内存中。** Provisa 在运行时创建每个 Trino 目录，每个目录保存其数据源的连接属性，包括密码。Provisa 运行的每个引擎都把它们保存在内存中（`catalog.store=memory`）：不会有目录写入引擎磁盘，也不会从文件挂载目录。重启的协调器回来时没有任何目录；每个 Provisa 进程会在下一次查询时发现，引擎监视器则在一分钟内发现，并重新创建它所服务的目录。带 SASL 凭据的 Kafka 数据源需要一个文件：Trino 的 Kafka 连接器只从文件读取客户端设置，因此 Provisa 把它们（包括密码）写入 `kafka-client/<catalog>.properties`。该文件只有 Trino 的用户可读（0700 目录中的 0600 模式）。在容器栈中，该目录是 API 与引擎共享的内存卷。在托管平台的引擎上，它是引擎自己的 Kubernetes Secret，保存在节点内存中，只有 Trino 的用户组可读（0440 模式）。无论哪种方式，该文件都不会落到磁盘上。在托管引擎上，Kafka 数据源必须指定模式注册表：主题列只由模型声明的数据源在保存或应用时会被按名称拒绝，因为它的表描述需要一个无法交给运行中引擎的目录。启动栈时会删除早期版本留在磁盘上的目录文件和 Kafka 客户端文件。

### 共享部署上的数据源 {: #sources-on-a-shared-deployment }

在承载多个组织的部署上，组织的数据源只能访问公网地址：绝不能访问平台的私有网络，那里有它的控制平面、缓存和对象存储。该检查在保存数据源时进行（无论通过 UI、管理 API 还是配置应用），并在每次连接和每次 HTTP 重定向时再次进行。演示沙箱的数据源还被限定在检查时的地址。部署自己的组织（其数据源由运营者配置）不受限制。

## 静态加密与组织自有密钥 {: #encryption-at-rest-and-the-organizations-own-keys }

使用组织自有密钥的加密是可用的，在组织开启之前处于关闭状态。除非组织管理员创建了密钥，否则不会有任何内容在组织的密钥下加密。

**组织加密密钥**包裹组织的密钥、其加密列及其审计查询文本。组织管理员在 **Admin → Org Secrets → Org Key** 下创建它，或使用 `PUT /admin/org-encryption`：不带请求体时 Provisa 生成该密钥且从不显示，带 `key_b64` 请求体则提供你自己的 32 字节。`GET /admin/org-encryption` 只报告其指纹。没有组织密钥时，组织的数据用部署的密钥（`provisa.yaml` 中的 `encryption.provider`）包裹。未配置加密提供方的部署，会以未加密形式保存加密列和审计查询文本；密钥绝不以明文保存（参见[密钥](secrets.md#provisas-built-in-store)）。

**组织的文件夹**存放其上传和落地的文件、其引擎的溢写文件及其临时文件。组织管理员在 **Admin → Org Secrets → File folder encryption** 下开启其加密，或使用 `PUT /admin/org-files`（`{"encrypted": true, "key_reference": "${secret:NAME}"}`）。`POST /admin/org-files/generate-key` 一次性返回新的 256 位密钥，供存入保管库。密钥保留在组织自己的保管库中（AWS Secrets Manager、GCP Secret Manager、Azure Key Vault 或 HashiCorp Vault）：Provisa 只保存引用，gocryptfs 用该密钥使文件夹中的文件在磁盘上保持加密。这需要 FUSE，即带有 `/dev/fuse` 的 Linux 主机或容器；在 macOS 上该设置会被拒绝。

在通常以自有引擎运行一个组织的自托管部署上，同时开启两者即用该组织的密钥加密其密钥、文件、溢写文件和临时文件：

- 组织运行的 DuckDB 引擎溢写到组织的文件夹。
- 行过滤器获取的行和上传的代码暂存在组织的文件夹中。
- Trino 的容错执行用为每个查询生成、查询结束即丢弃的密钥，加密假脱机的交换数据（`fault-tolerant-execution.exchange-encryption-enabled=true`）。Provisa 不开启 Trino 旧式的溢写到磁盘。
- Trino 把组织的目录连同其凭据保存在内存中，不向磁盘写入任何目录（`catalog.store=memory`）；Kafka 数据源的 SASL 客户端设置位于只有 Trino 用户可读的文件中，在容器栈中位于内存（参见[联邦引擎](#the-federation-engine-trino)）。
- PostgreSQL 联邦引擎把临时文件写在该 PostgreSQL 服务器保存它们的位置，处于服务器自身的存储加密之下，而不是组织的文件夹中。

### 共享与专用托管平台 {: #shared-and-dedicated-hosted-platforms }

共享托管平台在逻辑上隔离各组织。一个部署服务所有组织，每个组织的数据通过治理、引擎上自己的 schema、catalog 和资源组、自己的密钥、假脱机引擎数据的每查询密钥，以及拒绝其数据源访问平台私有地址，而始终归其自己所有。建议用于试验和小型企业。专用托管平台在物理上隔离：拥有自己的机器、网络和存储，服务一个客户。对于高度敏感或受监管的工作负载，请在你自己的边界内自托管 Provisa，由你自己的运维人员掌握每一个密钥和每一台主机。托管平台没有 SOC 2 报告。如果你需要 SOC 2，我们建议自托管。

### 浏览器中的 Python {: #python-in-the-browser }

Explore 的 Python 和 Notebook 标签页在访问者的浏览器中运行 Python。它发出的每次读取都以已登录用户的身份、带着其当前角色、组织和环境发送到服务器，并与来自任何其他客户端的同一语句完全一样地受到治理。

## 高安全模式 {: #high-security-mode }

`provisa.yaml` 中的 `security.mode: high` 声明一项保证：Provisa 后端从不处理明文数据。每个有意义的列都在数据源处加密，只有持有解密密钥的客户端才能读取它。这项保证有一些部署必须为之规划的后果。

**该模式做什么：**

- **数据端点要求客户端解密的证明。** `/data/` 下的一切都返回 403，除非调用者出示 `X-Provisa-KMS-Key` 标头，这是配置为本地解密的 JDBC 或 Python 客户端的标志。明文的浏览器或 REST 使用者不携带这样的密钥，因此被拒绝。该控制是对整棵树的默认拒绝：明天新增的路由在发布当天就受到保护，例外需要正当理由。
- **schema 元数据端点保持开放。** `/data/sdl`、`/data/introspection`、`/data/schema-version`、`/data/domains`、`/data/proto` 和 `/data/compile` 不返回行数据，而客户端必须先读取 schema（包括哪些字段是 `@encrypted`）才能连接。
- **gRPC 和 Arrow Flight 继续提供服务，要求相同的证明。** 它们是加密客户端实际使用的传输；关闭它们会让高安全部署没有线协议可用。两者上的数据调用都必须以调用元数据携带同一个 KMS 密钥。
- **pgwire、Bolt 和 MCP 不会启动。** 这三者都没有能携带解密上下文的逐连接握手：pgwire 的行集和 Cypher 结果以明文传输，MCP 的工具调用把结果作为文本交给模型。为其中任何一个配置的端口都会在启动时被拒绝，而不是提供服务。
- **关系防护无法绕过。** `ignore_relationships` 和 `relationship_guard: false` 都被忽略；参见[关系治理](#relationship-governance-v002)。

**如何验证部署处于该模式：** 启动日志会指明它，不带 KMS 密钥的 `/data/sql` 查询会以 403 应答并附上指名高安全模式的消息，pgwire、Bolt 和 MCP 端口不在监听。

## ABAC批准钩子 (Hook) {: #abac-approval-hook }

在查询执行之前触发的可选外部策略钩子。配置后，Provisa 会带着用户身份、角色、表、列和操作调用你的策略引擎。应答决定查询是否继续。以**需要批准**注册的命令在每次调用前都会交给同一个钩子；参见[命令调用](#command-calls)。

### 适用范围 {: #scoping }

钩子只在查询触及启用了它的表或数据源时触发。其他一切都没有开销。

| 配置 | 效果 |
| -------- | -------- |
| `auth.approval_hook.scope: all` | 每个查询都调用钩子 |
| `sources[].approval_hook: true` | 该数据源的所有表都调用钩子 |
| `tables[].approval_hook: true` | 该表调用钩子 |

### 协议 {: #protocols }

支持三种传输：

| 类型 | 场景 | 配置字段 |
| ------ | ---------- | ------------- |
| `webhook` | 任何 HTTP 策略服务（OPA、自定义） | `url` |
| `unix_socket` | 同一主机上的 OPA 或策略 sidecar | `socket_path` + `url` |
| `grpc` | 就近的高性能策略服务 | `url` (host:port) |

gRPC 传输使用 `provisa/auth/approval.proto` 中定义的 `provisa.auth.ApprovalService` 契约。在你的策略引擎中实现此服务：

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

gRPC 通道是持久的：每个 Provisa 实例一个通道，在对该钩子端点的所有调用中复用。

### 请求／响应 {: #request-response }

三种传输都携带相同的负载：

| 字段 | 类型 | 说明 |
| ------- | ------ | ------------- |
| `user` | string | 已验证用户的身份 |
| `roles` | string[] | 用户的 Provisa 角色 |
| `tables` | string[] | 查询引用的表 id |
| `columns` | string[] | 查询中选择的列 |
| `operation` | string | `"query"` 或 `"mutation"`；命令调用为 `"command"` |
| `command` | string | 命令调用时的命令名称；查询时为空 |
| `arguments` | object | 调用命令所用的参数；查询时为空 |

webhook 和 Unix socket 传输交换 JSON，以 `command` 和 `arguments` 为键。在 gRPC 上，参数以 JSON 文本 `arguments_json` 传递。响应必须包含 `approved`（bool），并可选包含 `reason`（string）。

### 命令调用 {: #command-calls }

设置了 `requires_approval`（命令表单上的**需要批准**开关）的命令，在任何界面上每次调用之前都会交给钩子。只有钩子批准时它才运行。请求携带 `operation: "command"`、命令名称及其参数；`tables` 和 `columns` 为空。

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

超时或传输错误时，适用 `fallback` 策略。熔断器（默认：连续 5 次错误后断开，30 秒后半开）可防止缓慢的钩子端点引发级联故障。

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

凭据使用 `${env:VAR_NAME}` 语法，在运行时解析。密码从不保存在配置数据库中。

关于密钥服务的完整说明（存储、引用语法和提供程序）参见[密钥](secrets.md)。
