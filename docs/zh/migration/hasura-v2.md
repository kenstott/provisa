# 从 Hasura v2 迁移到 Provisa

## 前提条件

1. 一个正在运行的 Hasura v2 实例（v2.x），并已导出元数据。
2. 使用 Hasura CLI 导出元数据：

   ```bash
   hasura metadata export --endpoint http://localhost:8080
   ```

   这将创建一个 `metadata/` 目录，其中包含 `sources.yaml`、`actions.yaml`、
   `cron_triggers.yaml`、`inherited_roles.yaml`、`remote_schemas.yaml` 等文件。
3. Python 3.11+，并已安装 `provisa` 包。

## CLI 用法

```bash
python -m provisa.hasura_v2 <metadata-dir> -o provisa.yaml
```

### 参数

| 参数 | 是否必需 | 说明 |
| ---------- | ---------- | ------------- |
| `metadata_dir` | 是 | 已导出的 Hasura v2 元数据目录的路径 |

### 选项

| 选项 | 默认值 | 说明 |
| -------- | --------- | ------------- |
| `-o, --output FILE` | stdout | 输出 YAML 文件的路径 |
| `--source-overrides FILE` | 无 | 包含按数据源覆盖连接配置的 YAML 文件 |
| `--domain-map KEY=VAL ...` | 无 | 架构到域的映射（例如 `public=core hr=people`） |
| `--auth-env-file FILE` | 无 | 包含 JWT/admin-secret 身份验证配置的 `.env` 文件路径 |
| `--dry-run` | 关闭 | 仅解析并验证，不写入输出 |

### 数据源覆盖文件

一个以数据源名称为键的 YAML 文件，包含要覆盖的连接属性：

```yaml
default:
  host: prod-db.example.com
  port: 5432
  database: myapp
  username: provisa_user
  password: "${env:PROD_DB_PASSWORD}"
```

### 身份验证环境文件

一个 `.env` 风格的文件，包含要转换的 Hasura 身份验证配置。转换器执行以下映射：

- 带 `jwk_url` 的 JWT -> Provisa `provider: oauth`。
- JWT 的 `claims_map` -> Provisa `role_mapping[]`。
- Admin secret -> Provisa `superuser`。
- Webhook 身份验证 -> 系统会发出警告（Provisa 无对应功能）。

## 功能对等表

| Hasura v2 功能 | Provisa 对应项 | 说明 |
| --- | --- | --- |
| **数据源**（postgres、mysql、mssql、bigquery、citus） | `sources[]` | 类型映射：pg/postgres -> postgresql，mssql -> sqlserver。连接 URL 会被解析为 host/port/database/username/password。连接池设置会被保留。 |
| **表**（已跟踪的表） | `tables[]` | 架构和表名会被保留。`source_id` 链接到对应的数据源。 |
| **自定义表名**（`custom_name`、`custom_root_fields.select`） | `tables[].alias` | 取 `select`、`select_by_pk`、`custom_name` 中第一个非空值。 |
| **自定义列名** | `columns[].alias` | 将 `custom_column_names` 字典映射为列别名。 |
| **查询权限**（列、过滤条件） | `columns[].visible_to[]`、`rls_rules[]` | 列列表会转换为 `visible_to`。支持通配符（`*`）列。过滤条件会通过 `bool_expr_to_sql` 转换为 SQL。 |
| **插入/更新权限**（列） | `columns[].writable_by[]` | 列列表会转换为 `writable_by`。角色会被升级为具有 `write` 能力。 |
| **删除权限** | 角色能力升级 | 角色获得 `write` 能力。没有按表的删除映射。 |
| **对象关系** | `relationships[]`，`cardinality: many-to-one` | 列映射会被保留。 |
| **数组关系** | `relationships[]`，`cardinality: one-to-many` | 列映射会被保留。 |
| **计算字段** | `functions[]` | 映射为一个 Function，其 `returns` 指向父表的 ID。 |
| **已跟踪的函数** | `functions[]` | `exposed_as` 默认值为 mutation。架构会被保留。 |
| **Actions**（存储过程处理程序） | `functions[]` | 当由存储过程支持时，会转换为 Function 配置。 |
| **Actions**（webhook 处理程序） | 不转换 | 系统会发出警告，包含处理程序的 URL。 |
| **Cron 触发器** | 不转换 | 系统会发出警告。（运行时存在计划触发器，但转换器不会对其进行映射。） |
| **事件触发器** | 不转换 | 系统会发出警告。（运行时存在事件触发器，但转换器不会对其进行映射。） |
| **继承角色** | `roles[].parent_role_id` | `role_set` 中的第一个角色成为父角色。所有子角色都会被创建。 |
| **远程架构** | `sources[]`（`graphql_remote`） | 注册为 `graphql_remote` 数据源。名称、URL、标头和身份验证配置会被保留。 |
| **枚举表** | 创建表 | `is_enum` 标志不会被带入（Provisa 无对应功能）。 |
| **允许列表** | 跳过 | 元数据模型中不存在该项。 |

## 转换后步骤

1. **检查输出的 YAML。** 确认数据源、表和角色是否正确。
2. **配置数据源连接。** 转换器会解析连接 URL，但在解析失败时默认使用 `localhost`。
   请使用 `--source-overrides`，或直接编辑输出结果。
3. **验证域分配。** 如果未使用 `--domain-map`，所有表都会归入 `default`。
   请使用 `--domain-map public=core analytics=reporting` 将架构分配给域。
4. **检查 RLS 规则。** 过滤条件会被转换为近似的 SQL。复杂的布尔表达式
   （嵌套的 `_and`/`_or`/`_exists`）应人工审查。
5. **查看警告。** 转换器会在 stderr 打印警告摘要，列出其无法映射的功能
   （事件触发器、cron 触发器、基于 webhook 的 actions）。
6. **设置身份验证。** 如果您的 Hasura 实例使用 JWT/webhook 身份验证，请创建身份验证环境文件，
   并使用 `--auth-env-file` 重新运行。
7. **测试。** 启动 Provisa 服务器，并针对您的数据源验证查询。

## 常见问题与故障排查

### 连接 URL 未被解析

如果数据源的 `database_url` 是一个环境变量引用（`{"from_env": "PG_URL"}`），转换器
无法在转换时解析它。该数据源将带有占位符值（`host: localhost`、`database: default`）。
请使用 `--source-overrides` 修复。

### 通配符列

当某项权限授予 `columns: "*"` 时，转换器会创建一个通配符列条目。转换后，
您可能希望通过检查实际的数据库架构，将其替换为明确的列列表。

### 事件触发器的保真度

事件触发器会连同 `operations` 和 `webhook_url` 一起转换，但 Hasura 特有的传递保证
（恰好一次、重新传递）在 Provisa 中没有直接对应项。请查看 `event_triggers` 部分，
并相应地配置您的 webhook 基础设施。

### 缺失角色

角色仅从权限条目中收集。如果某个角色存在于 Hasura 中，但在任何表或 action
上都没有权限，则不会出现在输出结果中。

### 自定义根字段

只有 `select` 和 `select_by_pk` 根字段用于表别名。其他自定义根字段
（`select_aggregate`、`insert`、`update`、`delete`）不会被映射。

## 示例

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

输出结构：

```yaml
sources:
  - id: default
    type: postgresql
    host: prod-db.example.com
    port: 5432
    database: myapp
    ...
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
        visible_to: [user, admin]
      - name: email
        visible_to: [admin]
        writable_by: [admin]
    alias: Users
roles:
  - id: admin
    capabilities: [read, write]
    domain_access: ["*"]
  - id: user
    capabilities: [read]
    domain_access: ["*"]
rls_rules:
  - table_id: default.public.users
    role_id: user
    filter: "id = x-hasura-user-id"
relationships:
  - id: default.public.orders.user
    source_table_id: default.public.orders
    target_table_id: default.public.users
    source_column: user_id
    target_column: id
    cardinality: many-to-one
```
