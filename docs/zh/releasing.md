# 版本发布 {: #releasing }

发布通过推送（push）git 标签触发。标签名称决定发布渠道。

## 标签命名约定 {: #tag-conventions }

| 标签格式 | 渠道 | GitHub Release 类型 |
| ----------- | --------- | ------------------- |
| `v1.2.3-alpha.1` | alpha | Pre-release |
| `v1.2.3-beta.1` | beta | Pre-release |
| `v1.2.3-rc.1` | rc | Pre-release |
| `v1.2.3` | stable | Latest release |

## 创建发布 {: #creating-a-release }

```bash
# Alpha
git tag v1.2.3-alpha.1 && git push origin v1.2.3-alpha.1

# Beta
git tag v1.2.3-beta.1 && git push origin v1.2.3-beta.1

# Release candidate
git tag v1.2.3-rc.1 && git push origin v1.2.3-rc.1

# Stable
git tag v1.2.3 && git push origin v1.2.3
```

CI 工作流（`build-dmg.yml`，名为"Build Provisa Packages"）会在任何 `v*` 标签上触发，并执行以下工作，其中大部分并行运行：

1. **Resolve release metadata** — 从标签后缀检测渠道，推导 PEP 440 版本和资产名称
2. **Download / package Trino plugins** — 拉取 Calcite Trino 连接器并打包为 tarball
3. **Pull core / obs / demo Docker images** — 保存服务镜像的 tarball（arm64，另加 amd64 core 供 Windows 容器层使用）
4. **Build macOS Core / Obs / Demo DMGs** — 在 `macos-14`（Apple Silicon）上运行，离线环境（airgapped）
5. **Build Linux AppImage** — core，离线环境
6. **Build Windows Core installer** — 原生、内嵌 Python、无需 Docker
7. **Build Windows Container-tier installer** — WSL2 + Trino，按需获取镜像（无需 VirtualBox/OVA）
8. **Build JDBC driver** — Maven shaded JAR
9. **Build and test Python client**，然后 **Publish to PyPI**
10. **Publish GitHub Release** — 上传全部资产，为 alpha/beta/rc 设置 pre-release 标志

## 发布资产 {: #release-assets }

每次发布都会发布以下资产，全部附加到 GitHub Release（wheel 也会同步发布到 PyPI）：

| 资产 | 平台／用途 |
| ------- | ---------------- |
| `Provisa-<tag>-macOS.dmg` | macOS Core（Apple Silicon，离线环境） |
| `Provisa-Runtime-<tag>-macOS.dmg` | macOS 原生 Python 运行时（与 Core 一起挂载） |
| `Provisa-Obs-<tag>-macOS.dmg` | macOS 可观测性扩展 |
| `Provisa-Demo-<tag>-macOS.dmg` | macOS 演示扩展（需要 Obs） |
| `Provisa-<tag>-linux-x86_64.AppImage` | Linux x86_64 core（离线环境） |
| `Provisa-<tag>-windows-x64.exe` | Windows x64 原生安装程序（内嵌 Python，无需 Docker） |
| `Provisa-Container-<tag>-windows-x64.exe` | Windows x64 容器层升级（WSL2 + Trino） |
| `provisa-jdbc-<tag>.jar` | JDBC 驱动程序 — Tableau、PowerBI、DBeaver |
| `provisa_client-<pep440>-py3-none-any.whl` | Python 客户端（同时发布于 PyPI） |
| `provisa-core-images-<tag>.tar.gz` | Core Services 镜像 tarball（arm64，离线环境） |
| `provisa-core-images-amd64-<tag>.zip` | Core Services 镜像（amd64，Windows 容器层／离线环境） |
| `provisa-obs-images-<tag>.tar.gz` | Observability Stack 镜像（可选） |
| `provisa-demo-images-<tag>.tar.gz` | Demo Data Pack 镜像（可选） |
| `provisa-trino-plugins-<tag>.tar.gz` | Coordination Engine 连接器（SharePoint、Splunk、File） |

Python 客户端版本会自动转换为 PEP 440 格式：
`v0.1.0-alpha.1` → `0.1.0a1`，`v0.1.0-beta.1` → `0.1.0b1`，`v0.1.0-rc.1` → `0.1.0rc1`。

## PyPI 发布设置（一次性） {: #pypi-publishing-setup-one-time }

`.github/workflows/build-dmg.yml` 中的 `publish-pypi`（`provisa-client`）和 `publish-provisa-pypi`（`provisa`）任务通过 PyPI 可信发布（trusted publishing）进行发布：由任务的 GitHub OIDC 令牌（`id-token: write`）完成身份验证，不读取任何 API 令牌。请在 pypi.org 上将本仓库和 `build-dmg.yml` 注册为每个项目的可信发布者（**Your projects → Manage → Publishing**）。此后每次打标签都会自动发布。

## 签名与供应链 {: #signing-and-supply-chain }

发布工作流构建时，每个发布工件所带有的内容：

| 工件 | 签名 |
| -------- | --------- |
| PyPI 上的 `provisa` 和 `provisa-client` | 每个文件一份经 Sigstore 签名的 PEP 740 证明（`attestations: true`），可在 pypi.org 上验证 |
| PyPI 上的 `provisa-pg-ext`、`provisa-duckdb-ext` | 每个文件一份经 Sigstore 签名的 PEP 740 证明 |
| JDBC 驱动 `.jar` | Sigstore 构建来源证明（`actions/attest-build-provenance`） |
| macOS `.dmg` 安装程序 | Developer ID 代码签名和 Apple 公证 |
| Windows `.exe` 安装程序 | 未签名 |
| Linux `.AppImage` | 未签名 |

任何操作系统上的签名安装方式都是 `pip install provisa`：带有 Sigstore 证明的 PyPI 软件包。需要在 Linux 或 Windows 上获得签名工件的审阅者，请使用 pip 安装。配置了代码签名证书时，Windows 构建会使用 Authenticode 签名（`WINDOWS_CERT_PFX_BASE64`、`WINDOWS_CERT_PFX_PASSWORD`、`WINDOWS_CERT_TIMESTAMP_URL`，由 `packaging/windows/build-sfx.ps1` 读取）；目前未配置，因此以未签名形式发布。

依赖项和 CVE 关卡：

- Python 依赖项在 `uv.lock` 中带哈希锁定；CI 使用 `uv sync --frozen` 安装它们。
- `CVE Audit` 工作流（`.github/workflows/cve-audit.yml`）在每次推送到 `main`、每个拉取请求以及每周，对锁定的集合运行 `pip-audit`。
- 发布时，`provisa` 和 `provisa-client` 的构建各自生成一份 CycloneDX SBOM，并对所发布的内容运行 `pip-audit`。JDBC 驱动使用 `grype --fail-on medium` 扫描。任何发现都会使任务失败，且发布不会发布任何内容：PyPI 上传和 GitHub 发布都会等待这些任务。SBOM 和 CVE 报告会附加到每个发布上。
- 桌面安装程序在发布运行器上从源代码构建 `provisa` wheel，并在构建时用 `pip` 解析其依赖项，而不是从 `uv.lock` 解析；发布 CVE 关卡扫描的是锁定的集合，而不是安装程序捆绑的依赖项。
- 有一项有文档记录的例外被忽略：PYSEC-2026-4114 / CVE-2026-49265（服务器端 oauthlib PKCE；Provisa 仅在客户端使用 oauthlib）。
- `packaging/pypi/requirements-embedded.lock` 是 `provisa[embedded]` 的哈希固定解析结果，供离线（airgap）镜像使用（参见 `packaging/pypi/MIRROR-CHECKLIST.md`）。它是手动重新生成的，而不是每次发布都生成。

## 所需的仓库密钥 {: #required-repository-secrets }

请在 **Settings → Secrets → Actions** 中配置以下内容：

| 密钥 | 所需用途 | 说明 |
| -------- | ------------- | ------------- |
| `APPLE_CERT_P12_BASE64` | 签名构建 | Base64 编码的 `.p12` 证书文件（见下文） |
| `APPLE_CERT_P12_PASSWORD` | 签名构建 | 从 Keychain Access 导出 `.p12` 时设置的密码 |
| `APPLE_DEVELOPER_ID` | 签名构建 | 完整证书名称：`Developer ID Application: Your Name (TEAMID)` |
| `APPLE_NOTARYTOOL_APPLE_ID` | 公证构建 | Apple ID 电子邮件地址 |
| `APPLE_NOTARYTOOL_PASSWORD` | 公证构建 | 来自 appleid.apple.com 的应用程序专用密码（不是你的登录密码） |
| `APPLE_NOTARYTOOL_TEAM_ID` | 公证构建 | 10 个字符的 Apple Team ID |

没有这些密钥的构建仍能成功完成，但会生成未签名／未公证的 DMG（用户将看到 Gatekeeper 警告）。

## 导出 .p12 证书 {: #exporting-the-p12-certificate }

1. 打开 **Keychain Access** → 登录钥匙串 → **My Certificates**
2. 查找 **Developer ID Application: Your Name (TEAMID)** — 展开以确认私钥嵌套在其下方
3. 同时选中证书及其私钥 → 右键点击 → **Export 2 Items** → 另存为 `.p12` → 设置一个强密码
4. 进行 Base64 编码并复制到剪贴板：

   ```bash
   base64 -i YourCert.p12 | pbcopy
   ```

5. 将其粘贴为 `APPLE_CERT_P12_BASE64` 的值；将 `APPLE_CERT_P12_PASSWORD` 设置为步骤 3 中的密码

## 查找你的证书名称 {: #finding-your-certificate-name }

```bash
security find-identity -v -p codesigning | grep "Developer ID Application"
```

复制引号中的完整字符串。这个字符串就是 `APPLE_DEVELOPER_ID` 的值。

## 删除错误的标签 {: #deleting-a-bad-tag }

```bash
git tag -d v1.2.3-alpha.1
git push origin :refs/tags/v1.2.3-alpha.1
```

然后在界面中删除对应的 GitHub Release，再重新打标签。
