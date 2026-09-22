# 从 Hasura 导入

Provisa 可以将现有的 Hasura 元数据转换为 Provisa 的 `config.yaml`，保留已跟踪的表、关系、权限和远程架构。

## 交互式导入（管理 → 导入 Hasura 配置）

管理界面运行相同的转换器，因此导入无需 shell 访问权限，也无需配置文件的往返操作。需要 `org_settings` 能力；导入落入会话当前操作的组织中。

1. **上传。** 选择一个压缩的 Hasura v2 元数据目录、一个压缩的 DDN 项目、一份合并的元数据导出文件（`.yaml`/`.json`，包括元数据 API 返回的 `{resource_version, metadata}` 信封），或单个 `.hml` 文件。除非上传内容存在歧义，否则将格式保持为*自动检测*。
2. **映射域**（可选）。每一对将一个 v2 架构或一个 DDN 子图映射到一个 Provisa 域；未映射的内容保留其原始名称。
3. **转换并预览。** 服务器进行转换，并返回统计数、转换器警告和生成的配置。此步骤不会写入任何内容。
4. **审查并编辑。** 配置可就地编辑——连接详情、域名称、角色名称。你所应用的正是所展示的内容。
5. **应用。** *替换现有语义层*会删除配置中不存在的每一个数据源、表、角色和规则；若不勾选，导入会合并到组织已有的内容中。应用会加载配置并重建组织的架构。

端点：`POST /admin/import/hasura/preview` 和 `POST /admin/import/hasura/apply`。

---

## Hasura v2

### 导出元数据

从 Hasura 控制台或 CLI：

```bash
hasura metadata export --output metadata.yaml
```

或使用 Hasura API：

```bash
curl -X POST http://localhost:8080/v1/metadata \
  -H "X-Hasura-Admin-Secret: <secret>" \
  -d '{"type":"export_metadata","args":{}}' \
  > metadata.json
```

### 转换

v2 转换器读取一个 Hasura 元数据**目录**（`hasura metadata export` 生成的布局，或扁平的 `tables.yaml` / `actions.yaml` 布局），并写出一个 Provisa 配置：

```bash
python -m provisa.hasura_v2 ./metadata -o config.yaml
```

省略 `-o` 会将配置写入标准输出。

标志：

| 标志 | 用途 |
| ------ | --------- |
| `-o`, `--output` | 输出 YAML 路径（默认：标准输出） |
| `--source-overrides` | 包含按数据源连接覆盖项（主机、端口、凭据）的 YAML 文件 |
| `--domain-map` | 以 `SCHEMA=DOMAIN` 对表示的架构到域的映射 |
| `--auth-env-file` | 包含身份验证配置的 `.env` 文件；转换 JWT/JWK、管理员密钥和声明映射 |
| `--dry-run` | 解析并校验，但不写出输出 |

### 转换内容

| Hasura 概念 | Provisa 对应物 |
| --------------- | ------------------- |
| 已跟踪表 | `tables[]`，带 `publish: true` |
| 对象关系 | `relationships[]`，`cardinality: many-to-one`。仅由外键列声明（`foreign_key_constraint_on: artist_id`）的关系在导出中未命名目标；转换器通过反向的数组关系来解析它，若没有反向关系则丢弃该关系并给出 `[relationships]` 警告。(REQ-1680) |
| 数组关系 | `relationships[]`，`cardinality: one-to-many` |
| 查询权限 | 角色可见性 + 行级安全过滤器。会话变量项（`X-Hasura-User-Id`）变为 `current_setting('provisa.user_id')`，该值在查询时从身份的用户 id 和声明中绑定。(REQ-1682) |
| 列权限 | `visible_to` / `writable_by` |
| 插入/更新/删除权限 | 变更操作 `writable_by` + 行级安全 |
| 远程架构 | `graphql_remote` 数据源注册，加上按角色 SDL 暴露的每个 Query 根字段各一张落地表；一列对每个其 SDL 暴露该列的角色可见，非空的根参数变为 `_nf_` 原生过滤列，嵌套字段会在警告中被命名。(REQ-1681) |
| 计算字段 | `functions[]` 条目，`kind: query` |

### 导入标签页上的连接与域

导出按环境变量为其数据库命名，因此在第一次转换后，标签页会列出每个 SQL 数据源及转换猜测出的连接信息。填写主机、端口、数据库、用户名和密码后再次转换；只有你更改过的字段会作为数据源覆盖项被传递。域行覆盖上传内容所携带的每个架构、子图和远程架构；每一行都是一个对组织现有域的选择器，也接受输入一个自定义名称，若不匹配任何现有域则标记为「新域」。除非勾选替换复选框，否则应用会合并到组织已有的内容中。(REQ-1687)

### 类型在预览时来自数据源

Hasura 导出为列命名时不带类型，而没有权限的已跟踪表则不带任何列名。预览使用你提供的数据源连接运行，因此它会读取每个可达 SQL 数据源的 `information_schema.columns`：每个无类型的列都会得到该数据源类型映射到 IR 词汇后的类型，而没有列的表会获取该数据源拥有的每一列，且仅对 `org_admin` 可见，因为 Hasura 未将其暴露给任何其他角色。预览无法访问的数据源会被报告为 `[sources]` 警告，其列在应用前会保持无类型，等待你自行完成。(REQ-1691, REQ-1684)

### 局限性

- **操作（Actions）** 自动转换：HTTP 处理程序操作变为 `webhooks[]` 变更；具有非 HTTP（数据库）处理程序的操作变为 `functions[]` 占位符，并发出警告要求审查该处理程序
- **事件触发器** 转换为按表的 `event_triggers` 配置（操作、Webhook URL、重试策略），并发出说明保真度有限的警告
- **远程架构** 转换为 `graphql_remote` 数据源条目，并按角色权限 SDL 落地为表；没有权限的远程架构不会落地任何内容，因为导出内容中没有其他关于其形态的说明 (REQ-1681)
- **自定义 SQL 函数** 需要审查——简单情形会转换为 `functions[]` 条目，复杂情形需要手动处理
- **定时触发器** 转换为 `scheduler` 配置条目，保留 cron 表达式和启用标志

---

## Hasura DDN（v3）

### 定位 HML 项目

DDN 转换器直接读取由 `.hml` 文件组成的 DDN 项目**目录**——无需超图（supergraph）构建步骤。项目根目录下的第一级目录名会被作为子图名称；`globals/` 下的文件会被分配到 `globals` 子图。

### 转换

```bash
python -m provisa.ddn ./my-ddn-project -o config.yaml
```

省略 `-o` 会将配置写入标准输出。

标志：

| 标志 | 用途 |
| ------ | --------- |
| `-o`, `--output` | 输出 YAML 路径（默认：标准输出） |
| `--source-overrides` | 包含按数据源连接覆盖项的 YAML 文件 |
| `--domain-map` | 以 `SUBGRAPH=DOMAIN` 对表示的子图到域的映射 |
| `--aggregates-output` | 聚合表达式旁挂文件的输出路径（默认：`<output>-aggregates.yaml`） |
| `--dry-run` | 解析并校验，但不写出输出 |

`AggregateExpression` 元数据会保留在一个旁挂的 `*-aggregates.yaml` 文件中。

### 转换内容

| DDN 概念 | Provisa 对应物 |
| ------------ | ------------------- |
| 子图模型 | 某数据源下的 `tables[]` |
| 关系 | `relationships[]` |
| 权限规则 | 行级安全过滤器 |
| 命令 | Webhook 变更或视图 |
| 连接器 | 带连接详情的数据源条目 |

### 局限性

- **Lambda 连接器**（TypeScript/Python 函数）需要手动设置 Webhook
- **生命周期插件** 没有直接对应物
- **DDN 身份验证模式** 映射到 Provisa 的身份验证提供程序，但 JWT 声明路径可能需要调整

---

## 导入之后

1. 审查生成的 `config.yaml`——留意转换器给出的 `warnings`
2. 校验连接凭据（转换器使用占位值）
3. 启动 Provisa 并确认表出现在浏览器中
4. 运行你现有的 GraphQL 查询——该架构对常见模式是兼容的
5. 在启用生产治理之前，通过管理 API 或 UI 提交查询以供审批
