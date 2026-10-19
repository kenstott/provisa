# 密钥 {: #secrets }

**名称存进去。值永远不会出来。**

没有任何 API 端点会返回已存储密钥的值，也没有任何界面提供“显示”按钮。丢失了值的人只能替换它，用的就是创建它时的同一个调用和同一个表单。这不是策略上的选择：代码里根本不存在读取路径。

---

## 引用语法 {: #reference-syntax }

凡是 Provisa 解析凭据的地方，以下三种引用形式均有效：

| 形式 | 解析来源 | 谁可以使用 |
| ------ | -------------- | --------------- |
| `${env:VAR_NAME}` | 服务器进程的环境 | 仅限部署配置 |
| `${secret:NAME}` | 组织保管库——由全体成员共享 | 任何接受凭据引用的字段 |
| `${user:NAME}` | 执行操作者的个人保管库 | 任何接受凭据引用的字段 |

在多租户部署中，请把平台凭据放在密钥服务中，不要放在环境变量里。租户管理员可以在自己的凭据字段里写入 `${env:...}`，所以进程环境中的任何值都在每个租户的可及范围内。只把密钥后端自身的凭据留在环境中。

解析全程失败即关闭。未知的提供程序名称、未设置的名称以及无法访问的后端都会引发错误。无法解析的引用绝不会被悄悄替换为空字符串。

### 名称格式 {: #name-format }

密钥名称必须匹配 `[A-Za-z_][A-Za-z0-9_]*`：字母、数字和下划线，以字母或下划线开头。原因很实际：`${secret:NAME}` 由引用语法解析，它会一直读到右花括号 `}`，所以名称中带有花括号、空格或冒号，就会被解析成别的东西。

---

## 两个保管库，一项服务 {: #two-vaults-one-service }

每个组织都有两个保管库。两者都位于同一个密钥服务内部。

**组织保管库。** 组织管理员存放在这里的凭据是共享的，所以每个引用 `${secret:DATABASE_TOKEN}` 的成员拿到的都是同一个值。它用于*组织*所拥有的凭据，例如共享的数据库密码、服务账号密钥或部署令牌。读写组织保管库需要 `org_settings` 能力。

**个人保管库。** 存放在这里的凭据只属于一个人。当两个人各自有一个 `GIT_TOKEN` 时，`${user:GIT_TOKEN}` 会解析为当前操作者的那个，所以同一段引用文本会给每个人各自的凭据。什么都没存的人会得到错误，永远不会拿到别人的值。个人保管库不受任何能力限制，因为持有自己的凭据不是管理员授予的特权；也没有任何请求语法可以指定别人的保管库。

作用范围是引用本身的一部分，外面没有包一层权限。`${secret:NAME}` 和 `${user:NAME}` 永远不会互相代替。

---

## 选择密钥服务 {: #choosing-a-secrets-service }

**管理 → 安全 → 密钥服务。** 持有 `platform_settings` 能力的人都能看到这个面板。它列出该版本已知的所有后端，无论其 SDK 是否已安装；灰显的行会说明缺少哪个 Python 包，而不是把选项藏起来。

随附五个后端：

| 键 | 标签 | 需要 |
| ----- | ------- | ------- |
| `provisa` | Provisa（内置，加密） | 无；这是默认项 |
| `hashicorp_vault` | HashiCorp Vault (KV v2) | `hvac` |
| `aws_secrets_manager` | AWS Secrets Manager | `boto3` |
| `gcp_secret_manager` | Google Secret Manager | `google-cloud-secret-manager` |
| `azure_key_vault` | Azure Key Vault（密钥） | `azure-keyvault-secrets` |

选择采用失败即关闭的方式：未知或不可用的后端会让启动报错停止，Provisa 从不悄悄改用别的后端。

### 后端自身的凭据 {: #the-backends-own-credential }

中央后端的连接凭据属于进程配置，只能来自 `${env:...}`。`${secret:...}` 无法提供它，因为一个把自身凭据存放在自己里面的密钥服务永远打不开。信任链按设计终止于主机环境。Provisa 在构建后端之前，会从环境中解析该后端的每个配置值。

例如，`provisa.yaml` 中的 Vault 配置：

```yaml
secrets:
  provider: hashicorp_vault
  hashicorp_vault:
    url: https://vault.internal:8200
    token: ${env:VAULT_TOKEN}   # process env only — never ${secret:...}
    mount: secret
```

### 中心化服务与内置存储 {: #central-service-vs-built-in }

配置了中央服务时，Provisa 只从中读取，从不写入。创建和删除条目归该服务自己的工具负责，所以“密钥”页面会说明这一点，也不提供创建按钮。

启用内置的 `provisa` 后端时，“密钥”页面完全可写。你可以在界面中或通过 API 创建、替换和删除密钥。

---

### 组织在中央服务中的密钥 {: #one-organizations-secrets-in-a-central-service }

承载多个组织并读取中央服务的部署，会为每个组织在该组织的前缀内读取服务。引用保持为短名称（`${secret:db#password}`）；前缀在读取时添加。

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

未配置中央服务时默认使用它。`secrets_store` 中的每一行都存放一个加密的信封数据块。`value` 列是二进制的，解密密钥位于进程环境中，在数据库之外。一份没有该部署主密钥的控制平面副本，只有密文，别无他物。

加密从来不是可选项。未配置进程级加密密钥时，存储会使用本地钥匙串。如果主机没有可存放密钥的钥匙串，存储会拒绝写入，值永远不会以明文保存。

**存储结构**：

| 列 | 类型 | 用途 |
| -------- | ------ | --------- |
| `org_id` | Text | 拥有此密钥的组织 |
| `owner_id` | Text | 组织保管库为 `"*"`；个人保管库为用户 id |
| `name` | Text | 引用名称 |
| `value` | LargeBinary | 加密的信封 blob |
| `description` | Text | 该密钥的用途——绝不从值推导得出 |
| `updated_by` | Text | 最后设置它的人 |

已创建自有加密密钥的组织，其密钥由该密钥包裹；否则由部署的密钥包裹。参见[静态加密](security.md#encryption-at-rest-and-the-organizations-own-keys)。

任何列表查询都不会选取 `value` 列。

---

## API 终结点 {: #api-endpoints }

所有路由都位于 `/admin/orgs/{org_id}` 之下。组织保管库需要该组织中的 `org_settings`。个人保管库不需要任何能力，因为所有者取自已认证的身份，也没有任何请求参数可以指定别人的保管库。

| 方法 | 路径 | 作用 |
| -------- | ------ | ------------- |
| `GET` | `/secrets` | 列出组织保管库的名称和引用 |
| `PUT` | `/secrets/{name}` | 创建或替换一个组织密钥 |
| `DELETE` | `/secrets/{name}` | 删除一个组织密钥 |
| `GET` | `/my-secrets` | 列出调用者个人的名称和引用 |
| `PUT` | `/my-secrets/{name}` | 创建或替换调用者的一个密钥 |
| `DELETE` | `/my-secrets/{name}` | 删除调用者的一个密钥 |

每个响应都返回元数据，从不返回值：名称、描述、`updated_at`、`updated_by`，以及可粘贴的 `reference` 字符串。`PUT` 请求体携带 `value`（必填）和 `description`（可选）。替换和创建是同一个调用，因为名称就是标识，没有单独的 ID。

每次写入都会记入审计日志。日志条目写明操作者和密钥名称。值和值的长度都不会记录。

---

## `${secret:NAME}` 在哪里解析 {: #where-secretname-resolves }

解析发生在绑定的操作内部。导入时和启动时都不会解析任何东西。存储在操作开始时读取并解密该组织的密钥一次，并在操作期间一直持有这份映射。在绑定的操作之外，`${secret:NAME}` 会报错。

有两处调用点建立该绑定：

**Git 远程操作。** 组织代码仓库的远程 URL 可以包含 `${secret:...}` 或 `${user:...}` 引用，例如嵌在 URL 中的推送令牌。环境路由器随后会围绕这次 git 调用，同时绑定组织保管库和当前操作用户的个人保管库。使用 `${user:GIT_TOKEN}` 形式时，提交会记在推送者本人的凭据之下，不涉及任何共享的服务账号。

**读取 AI 供应商 API 密钥。** 当 Provisa 读取组织的 LLM 供应商密钥，而该密钥以 `${secret:NAME}` 引用存储时，它会为这次请求绑定组织保管库。引用在出站时解析，引用文本本身永远不会发给供应商。

---

## 以密钥引用形式保存的组织 AI 供应商密钥 {: #org-ai-vendor-keys-as-secret-references }

组织的 AI 供应商密钥（Anthropic、OpenAI 及其他）可以存成 `${secret:NAME}` 引用，而不是字面密钥。

先把密钥存入组织保管库：

```
PUT /admin/orgs/{org_id}/secrets/OPENAI_KEY
{ "value": "sk-...", "description": "OpenAI production key" }
```

然后把组织的 AI 配置设为引用它：

```
vendor key field → ${secret:OPENAI_KEY}
```

引用以加密形式存放在 `org_secrets` 中。查询时，Provisa 依据组织保管库解析 `${secret:OPENAI_KEY}`，并把实际密钥交给供应商的 SDK。轮换保管库条目会立即生效，无需修改组织设置中的配置。

---

## 部署自身的存储凭据 {: #the-deployments-own-store-credentials }

Provisa 运行的各存储的密码（PostgreSQL、Redis、MinIO、Grafana 管理员账户和 Trino 集群的共享密钥）在两个保管库中都不算密钥。它们按部署生成并由部署保管：保存在 `~/.provisa/store-credentials.env`（权限 600）、Terraform 状态，或 Helm chart 的 `credentials.existingSecret` 指定的 Kubernetes Secret 中。参见[部署加固](security.md#deployment-hardening)。

---

## 平台管理员的访问权 {: #platform-admin-access }

运营控制平面的平台管理员无法读取任何组织的密钥值。`org_settings` 守卫会直接拒绝 `cross_org` 和平台绕过，因为管理组织的生命周期并不赋予读取该组织凭据的权限。服务器自行强制执行这一点，与界面显示什么无关。

---

## 另请参阅 {: #see-also }

- [安全模型](security.md) —— 分层访问控制、身份认证与审计日志
- [配置参考](configuration.md) —— 进程级凭据的 `${env:VAR}` 语法
