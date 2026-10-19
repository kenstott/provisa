# 包注册表

Node 和 Python 数据源会安装自己的代码及其依赖。**Admin → 包注册表**决定它们从哪里安装：组织的 npm 注册表、PyPI 索引和 Git 主机。组织管理员只需填写一次，组织内的每次 Node 和 Python 安装都会使用它们。公共 npm 注册表和公共 PyPI 无需填写。

每个字段保存所输入的内容：一个值，或 `${secret:npm_token}`、`${env:NPM_TOKEN}` 这样的引用，每次安装时在组织自己的保管库中解析。解析不出任何内容的引用会中止安装，并指出注册表和字段。Provisa 从不写入保管库，也从不自拟密钥名称。参见[密钥](secrets.md)。

---

## npm 注册表 {: #npm-registries }

| 字段 | 内容 |
|---|---|
| 注册表 URL | 注册表的地址，例如 `https://artifactory.example.com/artifactory/api/npm/npm/` |
| 作用域 | 例如 `@acme`，该作用域的包从此注册表获取。没有则留空 |
| 令牌或 user:password | 访问令牌，或 `user:password` |
| 作为每次安装的镜像 | 没有自身作用域的包都通过此注册表安装。每个组织一个 |
| 证书颁发机构 (PEM) | 参见[私有证书颁发机构](#a-private-certificate-authority) |

令牌以 `_authToken` 写入安装的 `.npmrc`，并作为 bearer 令牌发送。`user:password`（例如 Artifactory 用户及其 API 密钥）以 `_auth`（base64）写入，并以 Basic 方式发送。两者都只发送到注册表自己的主机。

## PyPI 索引 {: #pypi-indexes }

| 字段 | 内容 |
|---|---|
| 索引 URL | 索引的 simple-index 地址，例如 `https://artifactory.example.com/artifactory/api/pypi/pypi/simple/` |
| 令牌或 user:password | 访问令牌（以用户 `__token__` 发送），或 `user:password` |
| 作为每次安装的默认索引 | 所有 Python 包都从此索引安装，而不是公共 PyPI。每个组织一个 |
| 证书颁发机构 (PEM) | 参见[私有证书颁发机构](#a-private-certificate-authority) |

索引及其凭据只通过安装自己的环境传给 uv，从不写入文件。

## Git 主机 {: #git-hosts }

Git 主机条目涵盖获取数据源代码或其 Git 依赖的仓库。

| 字段 | 内容 |
|---|---|
| 主机 | 主机，服务器有端口和路径前缀时一并填写：`github.com`、`gitlab.example.com`、`bitbucket.example.com:7990/bitbucket` |
| 令牌或 user:token | 令牌，或 `user:token` |
| 证书颁发机构 (PEM) | 参见[私有证书颁发机构](#a-private-certificate-authority) |

仓库 URL 会匹配以其主机、端口和路径前缀开头的条目。多个条目匹配时，最长的胜出：`bitbucket.example.com:7990/bitbucket/scm/data` 的条目涵盖该项目，`bitbucket.example.com:7990/bitbucket` 的条目涵盖其余部分。

单独的令牌以用户 `x-access-token` 发送。其他提供商需要用户名，请输入 `user:token`：

| 提供商 | 用户 |
|---|---|
| GitHub | `x-access-token`，即默认值：只输入令牌 |
| GitLab | 任意非空用户名 |
| Bitbucket Cloud | 仓库或工作区访问令牌用 `x-token-auth`；API 令牌用您的用户名或电子邮件 |
| Bitbucket Data Center | 您的用户名；项目或仓库令牌用 `x-token-auth` |

## 私有证书颁发机构 {: #a-private-certificate-authority }

位于您自己的证书颁发机构之后的注册表、索引或 Git 服务器（例如您网络内的 Artifactory 代理）需要信任该机构。将其 PEM 证书粘贴到条目的**证书颁发机构 (PEM)**字段中，或填写指向包含它的密钥的引用。证书受公共信任的服务器请留空。不含 PEM 证书的值在保存条目时会被拒绝；解析不出证书的引用在安装运行时会被拒绝。

该机构与公共信任的机构一起被信任，绝不取代它们：

- Provisa 通过该条目进行的获取（包的元数据和 tarball、wheel、Git 仓库）都会用它验证服务器。
- 安装期间，uv 通过 `SSL_CERT_FILE` 读取它，npm 和 Bun 通过安装自己的 `.npmrc` 中的 `cafile` 读取，Deno 通过 `DENO_CERT`，Git 通过 `GIT_SSL_CAINFO`。

这些设置指向同一个文件，其中包含公共信任的机构和每个条目自己的机构。该文件写入安装的临时目录，并随之删除，因此安装之外的任何程序都不会读取它。
