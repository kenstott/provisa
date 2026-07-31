# 从 Hasura 导入

Provisa 可以将现有的 Hasura 元数据转换为 Provisa 的 `config.yaml`，并保留已跟踪的表、关系、权限和远程架构。

## Hasura v2

### 导出元数据

在你的 Hasura 控制台或 CLI 中：

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

v2 转换器读取一个 Hasura 元数据**目录**（由 `hasura metadata export` 生成的目录结构，或扁平的 `tables.yaml` / `actions.yaml` 结构），并写出一个 Provisa 配置：

```bash
python -m provisa.hasura_v2 ./metadata -o config.yaml
```

省略 `-o` 会将配置写到标准输出。

标志：

| 标志 | 用途 |
| ------ | --------- |
| `-o`, `--output` | 输出 YAML 路径（默认：标准输出） |
| `--source-overrides` | 包含各数据源连接覆盖项（主机、端口、凭据）的 YAML 文件 |
| `--domain-map` | 以 `SCHEMA=DOMAIN` 键值对表示的架构到域映射 |
| `--auth-env-file` | 包含身份验证配置的 `.env` 文件；转换 JWT/JWK、admin secret 和 claims 映射 |
| `--dry-run` | 仅解析和校验，不写出输出 |

### 转换内容

| Hasura 概念 | Provisa 对应项 |
| --------------- | ------------------- |
| 已跟踪的表 | 带 `publish: true` 的 `tables[]` |
| 对象关系 | 带 `cardinality: many-to-one` 的 `relationships[]` |
| 数组关系 | 带 `cardinality: one-to-many` 的 `relationships[]` |
| 查询（select）权限 | 角色可见性 + RLS 过滤器 |
| 列权限 | `visible_to` / `writable_by` |
| 插入/更新/删除权限 | 变更的 `writable_by` + RLS |
| 远程架构 | `graphql_remote` 数据源注册 |
| 计算字段 | 带 `kind: query` 的 `functions[]` 条目 |

### 局限性

- **Actions** 自动转换：HTTP-handler 的 action 转换为 `webhooks[]` 变更；使用非 HTTP（数据库）handler 的 action 转换为 `functions[]` 占位符，并发出警告以提示审查该 handler
- **Event triggers** 转换为按表配置的 `event_triggers`（操作、webhook URL、重试策略），并发出警告说明保真度有限
- **Remote schemas** 转换为 `graphql_remote` 数据源条目
- **自定义 SQL 函数** 需要人工审查——简单情形会转换为 `functions[]` 条目，复杂情形需要手动处理
- **Cron 触发器** 转换为 `scheduler` 配置条目，保留 cron 表达式和启用标志

---

## Hasura DDN（v3）

### 定位 HML 项目

DDN 转换器直接读取由 `.hml` 文件构成的 DDN 项目**目录**——不需要 supergraph 构建步骤。项目根目录下的第一级目录名会被作为子图（subgraph）名称；`globals/` 下的文件会被归入 `globals` 子图。

### 转换

```bash
python -m provisa.ddn ./my-ddn-project -o config.yaml
```

省略 `-o` 会将配置写到标准输出。

标志：

| 标志 | 用途 |
| ------ | --------- |
| `-o`, `--output` | 输出 YAML 路径（默认：标准输出） |
| `--source-overrides` | 包含各数据源连接覆盖项的 YAML 文件 |
| `--domain-map` | 以 `SUBGRAPH=DOMAIN` 键值对表示的子图到域映射 |
| `--aggregates-output` | 聚合表达式旁车文件的输出路径（默认：`<output>-aggregates.yaml`） |
| `--dry-run` | 仅解析和校验，不写出输出 |

`AggregateExpression` 元数据会被保留在旁车文件 `*-aggregates.yaml` 中。

### 转换内容

| DDN 概念 | Provisa 对应项 |
| ------------ | ------------------- |
| 子图模型 | 数据源下的 `tables[]` |
| 关系 | `relationships[]` |
| 权限规则 | RLS 过滤器 |
| Command | Webhook 变更或视图 |
| Connector | 带连接详情的数据源条目 |

### 局限性

- **Lambda connector**（TypeScript/Python 函数）需要手动设置 webhook
- **Lifecycle plugin** 没有直接对应项
- **DDN 身份验证模式** 会映射到 Provisa 的身份验证提供方，但 JWT claim 路径可能需要调整

---

## 导入之后

1. 检查生成的 `config.yaml`——注意转换器给出的 `warnings`
2. 验证连接凭据（转换器使用占位值）
3. 启动 Provisa，确认表出现在 Explorer 中
4. 运行你现有的 GraphQL 查询——该架构对常见模式兼容
5. 在启用生产治理之前，通过 Admin API 或 UI 提交查询以供审批
