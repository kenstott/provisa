# 版本发布

发布通过推送（push）git 标签触发。标签名称决定发布渠道。

## 标签命名约定

| 标签格式 | 渠道 | GitHub Release 类型 |
| ----------- | --------- | ------------------- |
| `v1.2.3-alpha.1` | alpha | Pre-release |
| `v1.2.3-beta.1` | beta | Pre-release |
| `v1.2.3-rc.1` | rc | Pre-release |
| `v1.2.3` | stable | Latest release |

## 创建发布

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

## 发布资产

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

## PyPI 发布设置（一次性）

1. 从 `~/.pypirc` 复制你的 API 令牌（`pypi.org` 对应的 `pypi-...` 值）
2. 在 **Settings → Secrets → Actions** 中将其添加为名为 `PYPI_API_TOKEN` 的仓库密钥

之后每次打标签，`publish-pypi` 任务都会自动发布。

## 所需的仓库密钥

请在 **Settings → Secrets → Actions** 中配置以下内容：

| 密钥 | 所需用途 | 说明 |
| -------- | ------------- | ------------- |
| `PYPI_API_TOKEN` | PyPI 发布 | 来自 `~/.pypirc` 的 API 令牌（以 `pypi-` 开头） |
| `APPLE_CERT_P12_BASE64` | 签名构建 | Base64 编码的 `.p12` 证书文件（见下文） |
| `APPLE_CERT_P12_PASSWORD` | 签名构建 | 从 Keychain Access 导出 `.p12` 时设置的密码 |
| `APPLE_DEVELOPER_ID` | 签名构建 | 完整证书名称：`Developer ID Application: Your Name (TEAMID)` |
| `APPLE_NOTARYTOOL_APPLE_ID` | 公证构建 | Apple ID 电子邮件地址 |
| `APPLE_NOTARYTOOL_PASSWORD` | 公证构建 | 来自 appleid.apple.com 的应用程序专用密码（不是你的登录密码） |
| `APPLE_NOTARYTOOL_TEAM_ID` | 公证构建 | 10 个字符的 Apple Team ID |

没有这些密钥的构建仍能成功完成，但会生成未签名／未公证的 DMG（用户将看到 Gatekeeper 警告）。

## 导出 .p12 证书

1. 打开 **Keychain Access** → 登录钥匙串 → **My Certificates**
2. 查找 **Developer ID Application: Your Name (TEAMID)** — 展开以确认私钥嵌套在其下方
3. 同时选中证书及其私钥 → 右键点击 → **Export 2 Items** → 另存为 `.p12` → 设置一个强密码
4. 进行 Base64 编码并复制到剪贴板：

   ```bash
   base64 -i YourCert.p12 | pbcopy
   ```

5. 将其粘贴为 `APPLE_CERT_P12_BASE64` 的值；将 `APPLE_CERT_P12_PASSWORD` 设置为步骤 3 中的密码

## 查找你的证书名称

```bash
security find-identity -v -p codesigning | grep "Developer ID Application"
```

复制引号中的完整字符串——这就是 `APPLE_DEVELOPER_ID` 的值。

## 删除错误的标签

```bash
git tag -d v1.2.3-alpha.1
git push origin :refs/tags/v1.2.3-alpha.1
```

然后在界面中删除对应的 GitHub Release，再重新打标签。
