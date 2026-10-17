# 从 Hasura 导入 {: #importing-from-hasura }

Provisa 可以将现有的 Hasura 元数据转换为 Provisa 的 `config.yaml`，保留已跟踪的表、关系、权限和远程架构。

## 交互式导入（管理 → 导入 Hasura 配置） {: #interactive-import-admin-import-hasura-config }

管理界面运行相同的转换器，因此导入无需 shell 访问权限，也无需配置文件的往返操作。需要 `org_settings` 能力；导入落入会话当前操作的组织中。

1. **上传。** 选择一个压缩的 Hasura v2 元数据目录、一个压缩的 DDN 项目、一份合并的元数据导出文件（`.yaml`/`.json`，包括元数据 API 返回的 `{resource_version, metadata}` 信封），或单个 `.hml` 文件。除非上传内容存在歧义，否则将格式保持为*自动检测*。
2. **映射域**（可选）。每一对将一个 v2 架构或一个 DDN 子图映射到一个 Provisa 域；未映射的内容保留其原始名称。
3. **转换并预览。** 服务器进行转换，并返回计数、转换报告（参见[报告](#the-report)）和生成的配置。此步骤不会写入任何内容。
4. **审查并编辑。** 配置可就地编辑——连接详情、域名称、角色名称。你所应用的正是所展示的内容。
5. **应用。** *应用到本组织*会加载配置并重建组织的架构。导入始终是合并到组织已有的内容中。

端点：`POST /admin/import/hasura/preview` 和 `POST /admin/import/hasura/apply`。

---

## Hasura v2

### 导出元数据 {: #export-metadata }

两种导出布局都可以转换。

**CLI 的元数据目录。** `hasura metadata export` 会写出一个目录。`databases/databases.yaml` 列出每个数据源的类型和连接，并通过 `!include` 索引指向其表和函数：

```bash
hasura metadata export
```

```yaml
# metadata/databases/databases.yaml
- name: default
  kind: postgres
  configuration:
    connection_info:
      database_url:
        from_env: HASURA_GRAPHQL_DATABASE_URL
  tables: "!include default/tables/tables.yaml"
  functions: "!include default/functions/functions.yaml"
```

扁平布局（根目录下的 `tables.yaml` 和 `actions.yaml`）同样可以转换。

**元数据 API 的单一文档。** 元数据 API 以单个对象返回整个导出，其中包含目录布局无处存放的各数据源 `configuration`：

```bash
curl -X POST http://localhost:8080/v1/metadata \
  -H "X-Hasura-Admin-Secret: <secret>" \
  -d '{"type":"export_metadata","args":{}}' \
  > metadata.json
```

裸对象和 `{resource_version, metadata}` 信封都可接受。管理端导入使用此形式（参见[交互式导入](#interactive-import-admin-import-hasura-config)）；命令行转换器只读取目录。

### 转换 {: #convert }

命令行转换器读取 Hasura 元数据**目录**（`hasura metadata export` 生成的布局，或扁平的 `tables.yaml` / `actions.yaml` 布局），并写出 Provisa 配置：

```bash
python -m provisa.hasura_v2 ./metadata -o config.yaml
```

省略 `-o` 则将配置写到 stdout。如果还有需要手工重做的内容，报告会输出到 stderr。

标志：

| 标志 | 用途 |
| ------ | --------- |
| `-o`, `--output` | 输出 YAML 路径（默认：stdout） |
| `--source-overrides` | 按数据源覆盖连接的 YAML 文件（host、port、凭据），以数据源名称为键 |
| `--domain-map` | 以 `SCHEMA=DOMAIN` 对表示的 schema 到域的映射 |
| `--auth-env-file` | 含身份验证配置的 `.env` 文件；转换 JWT/JWK、管理员密钥和 claims 映射 |
| `--dry-run` | 仅解析并验证，不写出输出 |

### 报告 {: #the-report }

每次转换都会把发现的内容分为三部分。请按此顺序阅读。

1. **原样带过来。** 对象在 Provisa 中有直接对应物，转换后含义不变：数据源、表、已批准的关系、select 权限、cron 触发器。
2. **映射到更好的 Provisa 功能。** Provisa 有比 Hasura 功能做得更好的功能，于是对象变成该功能。每一项都会说明原因。event 触发器、action、remote schema、额外的根字段名，以及继承自多个角色的角色都归入此类。
3. **需手工重做。** 对象在 Provisa 中没有对应物，或导出缺少转换所需的内容。每一项都会说明该怎么做。这些就是警告。

预览以列表形式返回这三部分。命令行转换器把它们输出到 stderr，标题为 `Carried as`、`Mapped to` 和 `Redo by hand`，各带一个计数。第三部分为空的转换即告完成。

### 转换内容 {: #what-gets-converted }

| Hasura 概念 | 部分 | Provisa 结果 |
| --------------- | ---- | -------------- |
| 数据源（`kind`、`connection_info`） | 原样带过来 | `sources[]`。导出中明文写出的连接会转换为 host、port、数据库和凭据。从环境变量读取的连接不会被猜测（参见[连接与域](#connections-and-domains-on-the-import-tab)）。 |
| 已跟踪的表 | 原样带过来 | 一张已注册的表。其自定义名称（或其 `select` 根字段）成为表别名。 |
| 自定义列名（`column_config`，或较旧的 `custom_column_names`） | 原样带过来 | 列的 `alias`。 |
| `select` 之外的根字段名（`select_by_pk`、`select_aggregate`、`insert` 等） | 映射 | 按名称报告。Provisa 把一张表作为一个字段提供：按键读取是对它的过滤，聚合是它的 `_aggregate` 字段，写入是它的变更。请确认应用的查询采用这种形态。 |
| Object 和 array relationship | 原样带过来 | 一个已批准的关系（object 对应 `many-to-one`，array 对应 `one-to-many`），其 GraphQL 字段是 Hasura relationship 的名称。不同的情形见下文。（REQ-1680） |
| select 权限 | 原样带过来 | 列权限（`visible_to`）和一条行规则。带会话变量的项（`X-Hasura-User-Id`）变为 `current_setting('provisa.user_id')`，请求在查询时根据身份的用户 id 和 claims 绑定它。（REQ-1682） |
| 穿过 relationship 的行规则 | 原样带过来 | 对关联表的 `EXISTS` 子查询。 |
| insert、update 和 delete 权限 | 原样带过来 | 表上的写入授权：每个角色和操作一条。`columns` 成为授权的列，`filter` 成为其 `rows`，`check` 成为其 `check`，`set` 成为其预设。 |
| 继承自一个角色的角色 | 原样带过来 | 该角色作为 `parent_role_id`。 |
| 继承自多个角色的角色 | 映射 | 持有它们授权并集的角色，并集被明确写出：Provisa 角色只有一个父角色。 |
| Computed field | 重做 | 基于该表的视图中的一列。 |
| 已跟踪的函数 | 原样带过来 | 一个命令；见下文。否则重做。 |
| Action | 映射 | 调用 action 处理程序的命令。 |
| Cron 触发器 | 原样带过来 | 一个计划触发器。 |
| Event 触发器 | 映射 | 一个针对表变更的触发器。 |
| Remote schema | 映射 | 一个 `graphql_remote` 数据源，其根字段落地为受治理的表。（REQ-1681） |

relationship 的情形：

- **冲突。** 表上已有与 relationship 同名的字段时，relationship 采用 Provisa 给它的名称，报告会发出警告。如果应用需要另一个名称，请在 Relationships 页面重命名。
- **同一个 join 声明两次。** 第二个声明了已声明过的 join 的 relationship 会并入第一个。报告在“原样带过来”一类中说明。
- **多个列对。** Provisa 关系只在一个列对上连接。在多个列对上连接的 relationship 会被警告且不会创建，但穿过它的行规则会被带过来。
- **仅外键列。** 以 `foreign_key_constraint_on: artist_id` 声明的 object relationship 在导出中不指明目标。转换器通过反向的 array relationship 解析它，没有时发出警告。

写入授权详情：

- 空的 `filter` 或 `check` 表示所有行，写作 `TRUE`。
- 属于会话变量的 `set` 值（`X-Hasura-User-Id`）成为对应会话变量的 `session` 预设（`X-Hasura-User-Id` 成为 `user_id`）。其他任何值成为 `literal` 预设。预设列不会同时出现在授权的列中。
- Hasura 隐含的 admin 持有每一项权限。它成为角色 `org_admin`，持有覆盖所有列和所有行的 insert、update 和 delete 授权，并在每个导入的列上被指名。（REQ-1684）
- 每个具有 insert、update 或 delete 权限的角色还会获得 `write` 权利。

命令详情：

- **已跟踪的函数。** 导出指明函数及调用它的角色，但不包含它的参数和返回内容。预览从数据库的目录读取这两者，并注册一个带有所发现签名的命令。数据源无法到达、不是 PostgreSQL，或函数缺失、被重载，或返回导入未注册的表的行时，该函数转入重做。命令行转换器从不连接数据库，因此它发现的每个已跟踪函数都转入重做。
- **Action。** 命令以 `POST` 调用 action 的处理程序。处理程序 URL 中的 `{{VAR}}` 写作 `${env:VAR}`，在命令被调用时解析。命令对 action 权限所指名的角色可见；未指名任何角色时只对 `org_admin` 可见。异步 action 会被警告：其命令等待处理程序的应答。导出不含其字段的返回类型会被警告。
- **Event 触发器。** 触发器在表的每次变更时触发，每批变更一次，并发送表和已变更的行。它不会按行带着旧值和新值触发。监听少于全部三种操作的 Hasura 触发器会被警告，因为 Provisa 触发器对所有操作触发。重试不会被带过来：带 `num_retries` 的 Hasura 触发器会被警告，因为 Provisa 触发器只调用一次。如果漏掉一次调用会有影响，请在处理程序中重试。
- **Cron 触发器。** cron 表达式、URL、payload 和 enabled 标志会被带过来。重试不会被带过来，并以同样方式警告。

合并后的文档还包含转换器没有目标的部分：`query_collections`、`allowlist`、`rest_endpoints`、`network` 和 `opentelemetry`。出现其中任何一项时，报告会把它列在需重做的内容中。目录读取器从不打开它们的文件。

### 导入标签页上的连接与域 {: #connections-and-domains-on-the-import-tab }

导出明文写出的连接按原样转换。当导出从环境变量读取数据库 URL（`database_url: {from_env: HASURA_GRAPHQL_DATABASE_URL}`）时，变量的值不在导出中，因此转换不会猜测任何值。该数据源会以空的连接字段注册，报告会要求重做：在导入的数据源设置中输入该数据源的 host、port、数据库、用户和密码，然后再次转换。只有你修改过的字段会随之传递，作为数据源覆盖。域行涵盖上传内容携带的每个 schema、subgraph 和 remote schema；每一行是对组织现有域的选择器，也接受键入的名称，不匹配任何现有域时标记为“新域”。应用会合并到组织已有的内容中。（REQ-1687）

### 类型在预览时来自数据源 {: #types-come-from-the-source-at-preview }

Hasura 导出为列命名时不带类型，而没有权限的已跟踪表则不带任何列名。预览使用你提供的数据源连接运行，因此它会读取每个可达 SQL 数据源的 `information_schema.columns`：每个无类型的列都会得到该数据源类型映射到 IR 词汇后的类型，而没有列的表会获取该数据源拥有的每一列，且仅对 `org_admin` 可见，因为 Hasura 未将其暴露给任何其他角色。预览无法访问的数据源会被报告为 `[sources]` 警告，其列在应用前会保持无类型，等待你自行完成。(REQ-1691, REQ-1684)

### 局限性 {: #limitations }

- **Computed field** 需手工重做：导出指明了数据库函数但没有定义它。把它添加为基于该表的视图中的一列，或在应用中计算。
- **Remote schema** 根据角色权限 SDL 落地为表。没有权限的 remote schema 不会落地任何内容，因为导出没有其他关于其形态的陈述。（REQ-1681）
- **已跟踪的函数** 需要在预览时访问数据库来读取其签名；离线时，在 Commands 页面重做。
- **event 和 cron 触发器的重试** 不会被带过来。

---

## Hasura DDN（v3） {: #hasura-ddn-v3 }

### 定位 HML 项目 {: #locate-the-hml-project }

DDN 转换器直接读取由 `.hml` 文件组成的 DDN 项目**目录**——无需超图（supergraph）构建步骤。项目根目录下的第一级目录名会被作为子图名称；`globals/` 下的文件会被分配到 `globals` 子图。

### 转换 {: #convert_1 }

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

### 转换内容 {: #what-gets-converted_1 }

| DDN 概念 | Provisa 对应物 |
| ------------ | ------------------- |
| 子图模型 | 某数据源下的 `tables[]` |
| 关系 | `relationships[]` |
| 权限规则 | 行级安全过滤器 |
| 命令 | Webhook 变更或视图 |
| 连接器 | 带连接详情的数据源条目 |

### 局限性 {: #limitations_1 }

- **Lambda 连接器**（TypeScript/Python 函数）需要手动设置 Webhook
- **生命周期插件** 没有直接对应物
- **DDN 身份验证模式** 映射到 Provisa 的身份验证提供程序，但 JWT 声明路径可能需要调整

---

## 导入之后 {: #after-import }

1. 审阅生成的 `config.yaml` 和报告 —— 从 `Redo by hand` 的条目开始
2. 核实连接凭据。导出原本从环境变量读取的连接会以空值到达；报告会指明它
3. 启动 Provisa 并确认表出现在浏览器中
4. 运行你现有的 GraphQL 查询——该架构对常见模式是兼容的
5. 在启用生产治理之前，通过管理 API 或 UI 提交查询以供审批
