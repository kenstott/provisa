# 从 Hasura v2 迁移到 Provisa {: #migrating-from-hasura-v2-to-provisa }

## 前提条件 {: #prerequisites }

1. 一个正在运行的 Hasura v2 实例（v2.x），并已导出元数据。
2. 以两种布局之一导出元数据：

   **CLI 的元数据目录：**

   ```bash
   hasura metadata export --endpoint http://localhost:8080
   ```

   这会创建一个 `metadata/` 目录。`databases/databases.yaml` 列出每个数据源的类型
   和连接，并通过 `!include` 索引指向其表和函数；
   `actions.yaml`、`cron_triggers.yaml`、`inherited_roles.yaml`、`remote_schemas.yaml` 以及其余
   文件位于其旁。较早的扁平布局（根目录下的 `tables.yaml`）
   同样可以转换。

   **元数据 API 的单一文档：**

   ```bash
   curl -X POST http://localhost:8080/v1/metadata \
     -H "X-Hasura-Admin-Secret: <secret>" \
     -d '{"type":"export_metadata","args":{}}' > metadata.json
   ```

   裸对象和 `{resource_version, metadata}` 信封都可使用。管理端导入
   （Admin、Import Hasura Config）使用此形式；下面的命令行转换器
   读取目录。
3. 已安装 `provisa` 包的 Python 3.11+。

## CLI 用法 {: #cli-usage }

```bash
python -m provisa.hasura_v2 <metadata-dir> -o provisa.yaml
```

### 参数 {: #arguments }

| 参数 | 是否必需 | 说明 |
| ---------- | ---------- | ------------- |
| `metadata_dir` | 是 | 已导出的 Hasura v2 元数据目录的路径 |

### 选项 {: #options }

| 选项 | 默认值 | 说明 |
| -------- | --------- | ------------- |
| `-o, --output FILE` | stdout | 输出 YAML 文件的路径 |
| `--source-overrides FILE` | 无 | 包含按数据源覆盖连接配置的 YAML 文件 |
| `--domain-map KEY=VAL ...` | 无 | 架构到域的映射（例如 `public=core hr=people`） |
| `--auth-env-file FILE` | 无 | 包含 JWT/admin-secret 身份验证配置的 `.env` 文件路径 |
| `--dry-run` | 关闭 | 仅解析并验证，不写入输出 |

### 数据源覆盖文件 {: #source-overrides-file }

一个以数据源名称为键的 YAML 文件，包含要覆盖的连接属性：

```yaml
default:
  host: prod-db.example.com
  port: 5432
  database: myapp
  username: provisa_user
  password: "${env:PROD_DB_PASSWORD}"
```

### 身份验证环境文件 {: #auth-env-file }

一个 `.env` 风格的文件，包含要转换的 Hasura 身份验证配置。转换器执行以下映射：

- 带 `jwk_url` 的 JWT -> Provisa `provider: oauth`。
- JWT 的 `claims_map` -> Provisa `role_mapping[]`。
- Admin secret -> Provisa `superuser`。
- Webhook 身份验证 -> 系统会发出警告（Provisa 无对应功能）。

## 报告 {: #the-report }

转换以一份分为三部分的报告结束：

1. **原样带过来。** 对象在 Provisa 中有对应物，转换后含义不变。
2. **映射到更好的 Provisa 功能。** 每一项都会说明为什么 Provisa 功能做得更好。
3. **需手工重做。** 每一项都会说明该怎么做。这些就是警告。

命令行转换器在仍有需重做的内容时，把报告输出到 stderr，标题为 `Carried as`、`Mapped to` 和 `Redo by hand`，各带一个计数。管理端导入随预览返回这三个列表。

## 功能对等表 {: #feature-parity-matrix }

| Hasura v2 功能 | 部分 | Provisa 对应项 | 说明 |
| --- | --- | --- | --- |
| **数据源**（postgres、mysql、mssql、bigquery、citus） | 原样带过来 | `sources[]` | 类型已映射：pg/postgres -> postgresql，mssql -> sqlserver。明文写出的连接 URL 解析为 host/port/database/username/password。连接池大小保留。从环境变量读取的 URL 不会被猜测（见下文）。 |
| **表**（已跟踪的表） | 原样带过来 | `tables[]` | 保留 schema 和表名。`source_id` 关联到数据源。 |
| **自定义表名**（`custom_name`、`custom_root_fields.select`） | 原样带过来 | `tables[].alias` | 取 `select`、`select_by_pk`、`custom_name` 中第一个非空值。 |
| **自定义列名**（`column_config`，较旧的 `custom_column_names`） | 原样带过来 | `columns[].alias` | 每一列的自定义名称成为它的别名。 |
| **其他根字段名**（`select_by_pk`、`select_aggregate`、`insert` 等） | 映射 | 表的唯一字段 | 按名称报告。按键读取是对表的过滤，聚合是它的 `_aggregate` 字段，写入是它的变更。 |
| **select 权限**（列、过滤器） | 原样带过来 | `columns[].visible_to[]`、`rls_rules[]` | 列列表成为 `visible_to`；支持 `*`。过滤器转为 SQL；会话变量转为 `current_setting('provisa.<name>')`。 |
| **穿过 relationship 的过滤器** | 原样带过来 | `rls_rules[]` 中的 `EXISTS` 子查询 | 规则沿 relationship 到达它的表。 |
| **insert、update 和 delete 权限** | 原样带过来 | `tables[].write_grants[]` | 每个角色和操作一条授权：列来自 `columns`，`rows` 来自 `filter`，`check` 来自 `check`，预设来自 `set`。角色获得 `write` 权利。 |
| **admin 角色** | 原样带过来 | `org_admin` | 持有覆盖所有列和行的每项授权，并在每个导入的列上被指名。 |
| **Object relationship** | 原样带过来 | `relationships[]`，`cardinality: many-to-one` | 已批准。GraphQL 字段是 Hasura relationship 的名称。 |
| **Array relationship** | 原样带过来 | `relationships[]`，`cardinality: one-to-many` | 已批准。GraphQL 字段是 Hasura relationship 的名称。 |
| **继承角色**（一个角色） | 原样带过来 | `roles[].parent_role_id` | 该角色成为父角色。 |
| **继承角色**（多个角色） | 映射 | 持有它们授权并集的角色 | Provisa 角色只有一个父角色，因此并集被明确写在该角色上。 |
| **Computed field** | 重做 | 视图列 | 导出指明函数，但不含其函数体。报告给出需要添加的 `SELECT`。 |
| **已跟踪的函数** | 原样带过来，否则重做 | `functions[]` 命令 | 预览从数据库读取签名。读不到时，在 Commands 页面添加该函数。 |
| **Action** | 映射 | 调用处理程序的命令 | 处理程序 URL 中的 `{{VAR}}` 写作 `${env:VAR}`，在命令被调用时解析。 |
| **Cron 触发器** | 原样带过来 | `scheduled_triggers[]` | cron 表达式、URL、payload 和 enabled 标志。重试不会被带过来（警告）。 |
| **Event 触发器** | 映射 | 针对表变更的 `scheduled_triggers[]` | 任何变更时每批变更触发一次。重试不会被带过来（警告）。 |
| **Remote schema** | 映射 | 落地为表的 `sources[]`（`graphql_remote`） | 保留名称、URL、请求头和身份验证；根字段成为受治理的表。 |
| **枚举表** | 原样带过来 | 已创建表 | 不带过 `is_enum` 标志（Provisa 中无对应项）。 |
| **Allow list、query collection、REST 端点** | 重做 | 无 | 出现在合并导出中时会被报告；不会转换。 |

### Relationship {: #relationships }

- 每个 relationship 的 GraphQL 字段是 Hasura 名称。表上已有该名称的字段时，relationship 采用 Provisa 给它的名称，报告会发出警告。
- 声明同一个 join 的两个 relationship 合并为一个。报告会说明哪个并入了哪个。
- 在多个列对上连接的 relationship 会被警告且不会创建，因为 Provisa 关系只在一个列对上连接。穿过它的行规则仍会转换。
- 仅由外键列声明的 object relationship 通过反向的 array relationship 解析；没有时会被警告。

### 写入授权 {: #write-grants }

一条授权声明某个角色通过一种操作可以向某张表写入什么。空的 `filter` 或 `check` 表示所有行（`TRUE`）。`set` 值 `X-Hasura-User-Id` 成为 `hasura_user_id` 的 `session` 预设；其他任何值成为 `literal` 预设。预设列不会同时出现在授权的列中。

### Action、已跟踪的函数和触发器 {: #actions-tracked-functions-and-triggers }

- **Action** 成为调用其处理程序的命令，对其权限所指名的角色可见（未指名任何角色时只对 `org_admin` 可见）。异步 action 会被警告：命令等待处理程序的应答。
- 当预览能从 PostgreSQL 数据源读取**已跟踪的函数**的参数和返回表时，它成为命令。离线时，以及对于缺失、被重载或返回导入未注册的表的任何函数，它转入重做。
- **Event 触发器** 在其表的每次变更时触发，每批一次，并发送表和已变更的行，而不是每行一个带旧值和新值的事件。监听少于全部三种操作的触发器会被警告。

## 转换后步骤 {: #post-conversion-steps }

1. **审阅报告。** 从 `Redo by hand` 开始。
2. **配置数据源连接。** 导出明文写出的连接按原样转换。从环境变量读取的连接以空值到达。使用 `--source-overrides`，或直接编辑输出。
3. **核对域分配。** 没有 `--domain-map` 时，所有表都落在 `default`。
   使用 `--domain-map public=core analytics=reporting` 将 schema 分配到域。
4. **检查行规则。** 过滤器会转换为 SQL。请审阅复杂的布尔
   表达式（嵌套的 `_and`/`_or`/`_exists`）以及穿过 relationship 的过滤器。
5. **重做所列内容。** Computed field、预览无法读取的已跟踪函数、
   触发器重试和 webhook 身份验证没有自动转换。
6. **设置身份验证。** 如果你的 Hasura 实例使用 JWT/webhook 身份验证，请创建一个
   身份验证环境文件，并用 `--auth-env-file` 重新运行。
7. **传入会话变量。** 配置身份提供方，使令牌携带报告在“映射到”下列出的每个会话变量（例如 `x-hasura-user-id`，在顶层或 Hasura claims 对象内），或按成员将其记录为属性。
8. **测试。** 启动 Provisa 服务器，并针对你的数据源验证查询。

## 常见问题与故障排查 {: #common-issues-and-troubleshooting }

### 连接来自环境变量 {: #connection-comes-from-an-environment-variable }

如果数据源的 `database_url` 是环境变量引用（`{"from_env": "PG_URL"}`），
其值不在导出中。转换器不会猜测任何值：该数据源的 host、数据库、
用户名和密码为空，报告会把它列在 `Redo by hand` 之下。
在导入的数据源设置中（或通过 `--source-overrides`）输入连接，然后再次
转换。

### 通配符列 {: #wildcard-columns }

当某项权限授予 `columns: "*"` 时，转换器会创建一个通配符列条目。转换后，
您可能希望通过检查实际的数据库架构，将其替换为明确的列列表。

### 事件触发器的保真度 {: #event-trigger-fidelity }

Event 触发器变为一个针对其表变更的触发器，调用同一个 webhook。任何变更时它都按
每批变更触发一次，无论变更是经由 Provisa 还是直接写入数据库，并发送表和
已变更的行。Hasura 的每行旧值和新值、按操作过滤以及重试都不会被带过来；
报告会针对适用的每一项发出警告。
让处理程序只处理它应当处理的行，如果漏掉一次调用会有影响，请在处理程序中重试。

### 缺失角色 {: #missing-roles }

角色从表权限、action 权限和继承角色中收集。如果某个角色存在于 Hasura 中，
但不出现在其中任何一处，它就不会出现在输出里。

### 自定义根字段 {: #custom-root-fields }

`select` 和 `select_by_pk` 根字段名给出表别名。其他自定义根字段
（`select_aggregate`、`insert`、`update`、`delete`）按名称报告：Provisa 把
一张表作为一个字段提供，聚合和写入分别是它的 `_aggregate` 字段和它的变更。

## 示例 {: #example }

转换一个典型的 Hasura v2 项目，其中两个架构映射到不同的域：

```bash
# Export metadata from Hasura
hasura metadata export --endpoint http://localhost:8080

# Convert with domain mapping and source overrides
python -m provisa.hasura_v2 metadata/ \
  -o provisa.yaml \
  --domain-map public=core hr=people \
  --source-overrides overrides.yaml \
  --auth-env-file auth.env

# Dry run first to check for warnings
python -m provisa.hasura_v2 metadata/ --dry-run
```

输出结构（节选）：

```yaml
sources:
  - id: default
    type: postgresql
    host: prod-db.example.com
    port: 5432
    database: myapp
domains:
  - id: core
  - id: people
tables:
  - source_id: default
    domain_id: core
    schema_name: public
    table_name: users
    columns:
      - name: id
        visible_to: [org_admin, user]
      - name: email
        visible_to: [org_admin, user]
    write_grants:
      - role: user
        operation: update
        columns: [email]
        rows: "id = current_setting('provisa.hasura_user_id')"
        check: "TRUE"
      - role: org_admin
        operation: insert
        columns: [id, email]
        check: "TRUE"
      - role: org_admin
        operation: update
        columns: [id, email]
        rows: "TRUE"
        check: "TRUE"
      - role: org_admin
        operation: delete
        rows: "TRUE"
roles:
  - id: user
    capabilities: [query_development, write]
    domain_access: ["*"]
rls_rules:
  - table_id: users
    role_id: user
    filter: "id = current_setting('provisa.hasura_user_id')"
relationships:
  - id: orders.user
    source_table_id: orders
    target_table_id: users
    source_column: user_id
    target_column: id
    cardinality: many-to-one
    graphql_alias: user
```
