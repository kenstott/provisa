# 密钥 {: #secrets }

**名称存进去。值永远不会出来。**

没有任何 API 终结点会返回已存储的密钥值。UI 中也没有"显示"按钮。丢失了某个值的人只能替换它——那与当初创建它的调用相同，走同一个表单。这不是一项策略决定：读取路径在代码中根本不存在。（REQ-1558）

---

## 引用语法 {: #reference-syntax }

凡是 Provisa 解析凭据的地方，以下三种引用形式均有效：

| 形式 | 解析来源 | 谁可以使用 |
| ------ | -------------- | --------------- |
| `${env:VAR_NAME}` | 服务器进程的环境 | 仅限部署配置 |
| `${secret:NAME}` | 组织保管库——由全体成员共享 | 任何接受凭据引用的字段 |
| `${user:NAME}` | 执行操作者的个人保管库 | 任何接受凭据引用的字段 |

在多租户部署中，请将平台凭据保存在密钥服务中，而不是环境变量中。租户管理员可以在自己的凭据字段中写入 `${env:...}`，因此进程环境中的每个值都在每个租户的可及范围之内。只在环境中保留密钥后端自身的凭据。

解析全程失败即关闭。未知的提供程序名称、未设置的名称以及无法访问的后端都会引发错误。无法解析的引用绝不会被悄悄替换为空字符串。（REQ-1557）[tool-verified: `provisa/core/secrets.py:92-117`]

### 名称格式 {: #name-format }

密钥名称必须匹配 `[A-Za-z_][A-Za-z0-9_]*`——字母、数字和下划线，以字母或下划线开头。这一约束出于实际考虑：`${secret:NAME}` 由引用语法解析，它一直读到结束的 `}`。含有花括号、空格或冒号的名称会产生一个被解析成别的东西的引用。[tool-verified: `provisa/core/secrets_store.py:61`]

---

## 两个保管库，一项服务 {: #two-vaults-one-service }

每个组织都有两个保管库。两者都位于同一个密钥服务内部。（REQ-1560）

**组织保管库**——组织管理员存放在这里的凭据是共享的。每个引用 `${secret:DATABASE_TOKEN}` 的成员拿到的都是同一个值。它适用于*组织*所拥有的凭据：共享的数据库密码、服务账号密钥、部署令牌。读写组织保管库需要 `org_settings` 能力。

**个人保管库**——存放在这里的凭据只属于一个人。当两个人各自持有一个 `GIT_TOKEN` 时，`${user:GIT_TOKEN}` 解析为其中正在执行操作的那个人的值。同一段引用文本交给每个人的都是他自己的凭据。什么都没有存的人得到的是错误，而不是别人的值。个人保管库不受任何能力管控——持有自己的凭据不是管理员授予的特权。而且没有任何请求语法可以指名他人的保管库。[tool-verified: `provisa/api/admin/secrets_router.py:86-103`]

作用域是引用的一部分，而不是围绕它的权限。`${secret:NAME}` 和 `${user:NAME}` 绝不会互相代答。

---

## 选择密钥服务 {: #choosing-a-secrets-service }

**管理 → 安全 → 密钥服务。** 该面板对持有 `platform_settings` 能力的人可见。构建版本已知的每一个后端都会列出，无论其 SDK 是否已安装。变灰的行会告诉你缺少哪个 Python 包——面板会点名它，而不是干脆隐藏该选项。

随附五个后端：

| 键 | 标签 | 需要 |
| ----- | ------- | ------- |
| `provisa` | Provisa（内置，加密） | 无；这是默认项 |
| `hashicorp_vault` | HashiCorp Vault (KV v2) | `hvac` |
| `aws_secrets_manager` | AWS Secrets Manager | `boto3` |
| `gcp_secret_manager` | Google Secret Manager | `google-cloud-secret-manager` |
| `azure_key_vault` | Azure Key Vault（密钥） | `azure-keyvault-secrets` |

[tool-verified: `provisa/core/secrets_registry.py:161-299`]

选择过程失败即关闭：未知或不可用的后端会在启动时引发错误，而不是悄悄回退到另一个。（REQ-1557）

### 后端自身的凭据 {: #the-backends-own-credential }

中心化后端的连接凭据属于进程配置。它只来自 `${env:...}`——绝不来自 `${secret:...}`。一个把自身凭据存在自己内部的密钥服务是打不开的，因此信任链按设计终止于宿主环境。注册表强制执行这一点：后端规格上的任何配置值都会在构造后端之前以 `providers=("env",)` 解析。[tool-verified: `provisa/core/secrets_registry.py:128-141`]

示例——`provisa.yaml` 中的 Vault 配置：

```yaml
secrets:
  provider: hashicorp_vault
  hashicorp_vault:
    url: https://vault.internal:8200
    token: ${env:VAULT_TOKEN}   # process env only — never ${secret:...}
    mount: secret
```

### 中心化服务与内置存储 {: #central-service-vs-built-in }

配置了中心化服务后，Provisa 从中读取，但不向其写入。创建和删除条目由中心化服务自己掌管——这些操作属于它自己的工具链。密钥页面会说明这一点，并且不提供创建按钮。（REQ-1557）

当内置的 `provisa` 后端处于启用状态时，密钥页面完全可写：可以从 UI 或通过 API 创建、替换和删除。

---

### 组织在中央服务中的密钥 {: #one-organizations-secrets-in-a-central-service }

承载多个组织并读取中央服务的部署，会为每个组织在该组织的前缀内读取服务。引用保持为短名称（`${secret:db#password}`）；前缀在读取时添加。（REQ-1557） [tool-verified: `provisa/core/secrets_providers.py`, `org_namespace`]

| 服务 | 组织 `acme` 的密钥 `db` 存放位置 |
|---|---|
| HashiCorp Vault (KV v2) | `orgs/acme/db` |
| AWS Secrets Manager | `orgs/acme/db` |
| GCP Secret Manager | `org-acme--db` |
| Azure Key Vault | `o4-acme-db`（`o`、组织 id 的长度、将 `_` 写作 `-` 的 id，然后是名称） |

在以下情况下引用会被点名拒绝：

- 其名称会离开前缀：以 `/` 开头、包含 `..` 段或反斜杠，或在名称是扁平的 GCP 或 Azure 上包含 `/`；
- 添加前缀后其名称对该服务来说过长（GCP 255 个字符，Azure 127 个）；这在保存引用时即被拒绝，并指明限制；
- 读取它的操作未绑定任何组织。

单组织部署按引用的原样读取，不加前缀：它所连接的服务属于该组织，无需迁移任何内容。

**迁移到前缀下。** 在多组织部署中，组织引用的每个密钥都必须位于该组织的前缀之下。使用服务自身的工具将每个密钥复制过去，然后把引用缩短为前缀内的名称（存放在 `orgs/acme/db` 的 `${secret:teams/data/db}` 变为 `${secret:db}`）。密钥未迁移的引用会以“未找到”失败；不会从它原来的位置读取。

## Provisa 的内置存储 {: #provisas-built-in-store }

未配置中心化服务时的默认项。`secrets_store` 中的每一行都保存一个加密的信封 blob——`value` 列是二进制而非文本，解密密钥存在进程环境中，而不是数据库里。一份没有该部署主密钥的控制平面副本，拿到的只有密文，别无他物。（REQ-1558）

加密从来不是可选项。当没有配置进程级加密密钥时，存储会回退到本地密钥链。如果宿主没有密钥链可以保管密钥，存储会拒绝写入，而不是以明文保存该值。[tool-verified: `provisa/core/secrets_store.py:130-159`]

**存储结构** [tool-verified: `provisa/core/schema_admin.py:493-505`]：

| 列 | 类型 | 用途 |
| -------- | ------ | --------- |
| `org_id` | Text | 拥有此密钥的组织 |
| `owner_id` | Text | 组织保管库为 `"*"`；个人保管库为用户 id |
| `name` | Text | 引用名称 |
| `value` | LargeBinary | 加密的信封 blob |
| `description` | Text | 该密钥的用途——绝不从值推导得出 |
| `updated_by` | Text | 最后设置它的人 |

已创建自有加密密钥的组织，其密钥由该密钥包裹；否则由部署的密钥包裹。参见[静态加密](security.md#encryption-at-rest-and-the-organizations-own-keys)。

任何列表查询都不会选取 `value` 列。[tool-verified: `provisa/core/secrets_store.py:214-235`]

---

## API 终结点 {: #api-endpoints }

所有路由都位于 `/admin/orgs/{org_id}` 之下。组织保管库需要该组织中的 `org_settings`。个人保管库不需要任何能力——所有者是从已认证的身份读取的；没有任何请求参数可以指名他人的保管库。

| 方法 | 路径 | 作用 |
| -------- | ------ | ------------- |
| `GET` | `/secrets` | 列出组织保管库的名称和引用 |
| `PUT` | `/secrets/{name}` | 创建或替换一个组织密钥 |
| `DELETE` | `/secrets/{name}` | 删除一个组织密钥 |
| `GET` | `/my-secrets` | 列出调用者个人的名称和引用 |
| `PUT` | `/my-secrets/{name}` | 创建或替换调用者的一个密钥 |
| `DELETE` | `/my-secrets/{name}` | 删除调用者的一个密钥 |

每个响应都会返回元数据——名称、说明、`updated_at`、`updated_by` 以及可直接粘贴的 `reference` 字符串——但绝不返回值。`PUT` 请求体携带 `value`（必填）和 `description`（可选）。替换与创建是同一个调用：名称就是身份，而不是另设一个 ID。

每一次写入都会记入审计日志。日志条目会写明操作者和密钥名称。值不会被记录，连它的长度也不会。[tool-verified: `provisa/api/admin/secrets_router.py:106-117`]

---

## `${secret:NAME}` 在哪里解析 {: #where-secretname-resolves }

解析发生在一个上下文绑定的操作内部，而不是在导入时或启动时。存储在该操作开始时读取并解密该组织的密钥一次，并在操作期间将该映射保存在 `ContextVar` 中。在绑定的操作之外，`${secret:NAME}` 会引发错误。（REQ-1557）[tool-verified: `provisa/core/secrets_store.py:269-290`]

有两处调用点建立该绑定：

**Git 远程操作。** 当某个组织的仓库远程 URL 含有 `${secret:...}` 或 `${user:...}` 引用时——例如嵌入在 URL 中的推送令牌——环境路由会在该 git 调用周围同时绑定组织保管库和执行操作用户的个人保管库。`${user:GIT_TOKEN}` 这种形式意味着一次提交落在推送它的那个人的凭据之下，而不是共享的服务账号。[tool-verified: `provisa/api/admin/environments_router.py:1263`]

**AI 供应商 API 密钥读取。** 当 Provisa 读取某个组织的 LLM 供应商密钥、而该密钥以 `${secret:NAME}` 引用形式存储时，`bound_to_request_org` 会为该请求建立组织保管库。引用在送出的路上被解析；引用文本本身绝不会发送给供应商。（REQ-1580）[tool-verified: `provisa/core/org_secrets.py:76-79`]

---

## 以密钥引用形式保存的组织 AI 供应商密钥 {: #org-ai-vendor-keys-as-secret-references }

组织的 AI 供应商密钥（Anthropic、OpenAI 及其他）可以存成 `${secret:NAME}` 引用，而不是字面密钥。（REQ-1580）

先把密钥存入组织保管库：

```
PUT /admin/orgs/{org_id}/secrets/OPENAI_KEY
{ "value": "sk-...", "description": "OpenAI production key" }
```

然后把组织的 AI 配置设为引用它：

```
vendor key field → ${secret:OPENAI_KEY}
```

该引用以加密形式存放在 `org_secrets` 中。查询时 Provisa 会对照组织保管库解析 `${secret:OPENAI_KEY}`，并把字面密钥交给供应商 SDK。轮换保管库中的条目会立即生效——组织设置那一侧无需改动配置。[tool-verified: `provisa/core/org_secrets.py:64-79`]

---

## 部署自身的存储凭据 {: #the-deployments-own-store-credentials }

Provisa 运行的各存储的密码（PostgreSQL、Redis、MinIO、Grafana 管理员账户和 Trino 集群的共享密钥）在两个保管库中都不算密钥。它们按部署生成并由部署保管：保存在 `~/.provisa/store-credentials.env`（权限 600）、Terraform 状态，或 Helm chart 的 `credentials.existingSecret` 指定的 Kubernetes Secret 中。参见[部署加固](security.md#deployment-hardening)。

---

## 平台管理员的访问权 {: #platform-admin-access }

操作控制平面的平台管理员无法读取任何组织的密钥值。`org_settings` 守卫明确拒绝 `cross_org` 和平台旁路：管理一个组织的生命周期不等于读取该组织保管的凭据。服务器独立于 UI 强制执行这一点。（REQ-1361）[tool-verified: `provisa/api/admin/secrets_router.py:53-83`]

---

## 另请参阅 {: #see-also }

- [安全模型](security.md) —— 分层访问控制、身份认证与审计日志
- [配置参考](configuration.md) —— 进程级凭据的 `${env:VAR}` 语法
