# 从 Hasura DDN (v3) 迁移到 Provisa

## 前提条件

1. 一个包含 HML 文件（扩展名为 `.hml`）的 Hasura DDN 项目。
   DDN 项目通常具有如下目录结构：

   ```text
   my-ddn-project/
     app/
       subgraph1/
         models/
           MyModel.hml
         commands/
           MyCommand.hml
       subgraph2/
         ...
     globals/
       ...
   ```

2. Python 3.11+ 并已安装 `provisa` 软件包。

## CLI 用法

```bash
python -m provisa.ddn <hml-dir> -o provisa.yaml
```

### 参数

| 参数 | 是否必需 | 说明 |
| ---------- | ---------- | ------------- |
| `hml_dir` | 是 | DDN HML 项目目录的路径（会递归扫描其中的 `.hml` 文件） |

### 选项

| 选项 | 默认值 | 说明 |
| -------- | --------- | ------------- |
| `-o, --output FILE` | stdout | 输出 YAML 文件的路径 |
| `--source-overrides FILE` | 无 | 包含各数据源连接覆盖设置的 YAML 文件 |
| `--domain-map KEY=VAL ...` | 无 | Subgraph 到 domain 的映射关系（例如 `app=core analytics=reporting`） |
| `--dry-run` | 关闭 | 仅解析并验证，不写入输出 |

### 数据源覆盖文件

一个以连接器（connector）名称为键的 YAML 文件（经过 ID 清理后：空格、点号、斜杠
会转换为下划线），并包含连接属性：

```yaml
my_pg_connector:
  host: prod-db.example.com
  port: 5432
  database: chinook
  username: provisa_user
  password: "${env:PROD_DB_PASSWORD}"
```

## 功能对照表

| DDN 类型 | Provisa 对应项 | 备注 |
| --- | --- | --- |
| **DataConnectorLink** | `sources[]` | 数据源类型根据连接器 URL 推断（postgres、mysql、mssql、mongo、clickhouse、snowflake、bigquery）。连接详情默认使用占位符；使用 `--source-overrides` 设置实际值。 |
| **ObjectType** | `tables[]` 上的列定义 | 字段（field）转换为列（column）。`dataConnectorTypeMapping.fieldMapping` 将 GraphQL 字段名解析为物理列名。 |
| **Model** | `tables[]` | 每个 Model 生成一张表。`source_id` 来自连接器，`table_name` 来自 collection。`graphql_type_name` 转换为 `alias`。Subgraph（以及由此得出的 `domain_id`）根据文件所在目录推断：即项目根目录下的第一层目录名。 |
| **Relationship** | `relationships[]` | Object 类型 -> `many-to-one`，Array 类型 -> `one-to-many`。字段映射通过查找物理列来解析。 |
| **TypePermissions** | `columns[].visible_to[]` | `allowedFields` 决定哪些角色可以看到每一列。 |
| **ModelPermissions** | `rls_rules[]` | 过滤谓词转换为 SQL WHERE 子句。支持 `_eq`、`_neq`、`_gt`、`_lt`、`_gte`、`_lte`、`_in`、`_nin`、`_like`、`_is_null`、`_and`、`_or`、`_not`。会话变量引用保留为 `${x-hasura-...}`。 |
| **Command** | `functions[]` | 函数和存储过程都会被映射。参数、返回类型及 GraphQL 根字段名均予以保留。`domain_id` 根据 subgraph 设置。 |
| **AggregateExpression** | 附属文件 `provisa-aggregates.yaml` | Count、count_distinct 及各字段的聚合函数会保留在附属文件中，并转换为 Provisa 的聚合配置。 |
| **BooleanExpressionType** | 跳过（静默处理） | DDN 内部用于过滤；无需直接对应到 Provisa。 |
| **AuthConfig** | 跳过（静默处理） | DDN 的身份验证配置不会被映射；请单独配置 Provisa 的身份验证。 |
| **ScalarType** | 跳过 | 会输出带计数的警告。 |
| **GraphqlConfig** | 跳过 | 会输出带计数的警告。 |
| **CompatibilityConfig** | 跳过 | 会输出带计数的警告。 |
| **其他无法识别的类型** | 跳过 | 会按类型输出带计数的警告。 |

## 核心概念：GraphQL 字段到物理列的解析

DDN 通过 ObjectType 上的 `dataConnectorTypeMapping`，将 GraphQL 架构（字段名）
与物理数据库架构（列名）分离开来。转换器会：

1. 读取每个 ObjectType 的类型映射中的 `fieldMapping` 条目。
2. 构建一个查找表：`{graphql_field_name -> physical_column_name}`。
3. 对于没有显式映射的字段，假定字段名与列名相同。
4. 在构建列、关系（relationship）及 RLS 过滤表达式时使用该查找表。

这意味着输出的 `provisa.yaml` 会在 `columns[].name` 中使用**物理列名**，
并在名称不同的情况下，将 `columns[].alias` 设置为 GraphQL 字段名。

## 转换后步骤

1. **检查输出的 YAML。** 验证数据源、表以及列映射。
2. **配置数据源连接。** 连接器仅提供用于类型检测的 URL 提示。
   实际的主机、端口、数据库及凭据必须通过
   `--source-overrides` 提供，或直接编辑输出文件。
3. **验证 domain 分配。** Subgraph 名称根据目录结构推断
   （即项目根目录下的第一层目录名）。若未使用 `--domain-map`，每个
   subgraph 名称会直接成为 domain ID。可使用 `--domain-map` 对其重命名。
4. **检查 RLS 规则。** DDN 的过滤谓词会转换为近似的 SQL 语句。
   支持嵌套布尔逻辑（`_and`/`_or`/`_not`），但涉及跨关系遍历的复杂
   过滤条件可能需要人工审查。
5. **检查聚合配置。** 聚合表达式会写入附属文件
   `provisa-aggregates.yaml`，并转换为 Provisa 的聚合配置。
6. **检查警告信息。** 转换器会在 stderr 输出摘要，列出被跳过的
   DDN 类型，以及任何引用了未知 ObjectType 的模型。
7. **进行测试。** 启动 Provisa 服务器，并针对你的数据源验证查询结果。

## 常见问题与故障排查

### 数据源类型检测失败

连接器 URL 会以启发式方式（heuristically）进行判断（查找诸如 "postgres"、
"mysql"、"mongo" 之类的关键字）。如果 URL 不包含可识别的关键字，
数据源将默认使用 `postgresql`。可通过 `--source-overrides` 覆盖。

### Model 缺少 ObjectType

如果某个 Model 引用的 ObjectType 名称在任何 `.hml` 文件中都找不到，
该表会被跳过并输出警告。请确保扫描目录中包含所有 HML 文件。

### Subgraph 发现

Subgraph 根据目录结构推断：项目根目录下的第一层目录名
被视为 subgraph 名称。HML 文档内部的 `subgraph` 字段不会被使用。
位于 `globals/` 目录下的文件会被归入 `globals` subgraph，
并会从 domain 发现中排除。

### 关系来源解析

关系（relationship）引用一个 `source_type`（ObjectType 名称）和一个 `target_model`
（Model 名称）。如果没有任何 Model 使用指定的 ObjectType，该关系会被静默跳过。

### 随处可见的列别名

如果你的 DDN 项目大量使用 `fieldMapping`，可以预期输出中大多数列都会带有
`alias`。这是正确的行为——`name` 是物理列，`alias` 则是应用程序
所使用的 GraphQL 名称。

### 聚合表达式

聚合表达式会保留在与输出文件一同写入的附属文件 `provisa-aggregates.yaml`
中，并转换为 Provisa 的聚合配置。它们不会存储在表的
`description` 中。

## 示例：转换一个 Chinook DDN 项目

```bash
# Convert the DDN project
python -m provisa.ddn ./chinook-ddn/ \
  -o provisa.yaml \
  --domain-map app=music \
  --source-overrides overrides.yaml

# Dry run to check warnings first
python -m provisa.ddn ./chinook-ddn/ --dry-run
```

输出结构：

```yaml
sources:
  - id: chinook_pg
    type: postgresql
    host: prod-db.example.com
    port: 5432
    database: chinook
    ...
domains:
  - id: music
tables:
  - source_id: chinook_pg
    domain_id: music
    schema_name: public
    table_name: Album
    columns:
      - name: AlbumId
        visible_to: [admin, user]
      - name: Title
        visible_to: [admin, user]
      - name: ArtistId
        visible_to: [admin, user]
    alias: Albums
  - source_id: chinook_pg
    domain_id: music
    schema_name: public
    table_name: Artist
    columns:
      - name: artist_id
        visible_to: [admin, user]
        alias: ArtistId
      - name: artist_name
        visible_to: [admin, user]
        alias: Name
    alias: Artists
roles:
  - id: admin
    capabilities: [read]
    domain_access: ["*"]
  - id: user
    capabilities: [read]
    domain_access: ["*"]
relationships:
  - id: chinook_pg.public.Album.Artist
    source_table_id: chinook_pg.public.Album
    target_table_id: chinook_pg.public.Artist
    source_column: ArtistId
    target_column: artist_id
    cardinality: many-to-one
functions:
  - name: GetTopTracks
    source_id: chinook_pg
    schema_name: public
    function_name: get_top_tracks
    returns: Track
    domain_id: music
    description: "DDN function"
```
