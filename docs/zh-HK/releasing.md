# 版本發佈

發佈由推送（push）git 標籤觸發。標籤名稱決定發佈渠道。

## 標籤命名慣例

| 標籤格式 | 渠道 | GitHub Release 類型 |
| ----------- | --------- | ------------------- |
| `v1.2.3-alpha.1` | alpha | Pre-release |
| `v1.2.3-beta.1` | beta | Pre-release |
| `v1.2.3-rc.1` | rc | Pre-release |
| `v1.2.3` | stable | Latest release |

## 建立發佈

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

CI 工作流程（`build-dmg.yml`，名為「Build Provisa Packages」）會在任何 `v*` 標籤上觸發，並執行以下大部分可並行執行的工作：

1. **Resolve release metadata** — 從標籤後綴偵測渠道，推導 PEP 440 版本及資源名稱
2. **Download / package Trino plugins** — 拉取 Calcite Trino 連接器並打包成 tarball
3. **Pull core / obs / demo Docker images** — 保存服務映像的 tarball（arm64，另加 amd64 core 供 Windows 容器層使用）
4. **Build macOS Core / Obs / Demo DMGs** — 在 `macos-14`（Apple Silicon）上執行，離線環境（airgapped）
5. **Build Linux AppImage** — core，離線環境
6. **Build Windows Core installer** — 原生、內嵌 Python、免 Docker
7. **Build Windows Container-tier installer** — WSL2 + Trino，按需擷取映像（免 VirtualBox/OVA）
8. **Build JDBC driver** — Maven shaded JAR
9. **Build and test Python client**，然後 **Publish to PyPI**
10. **Publish GitHub Release** — 上載全部資源，為 alpha/beta/rc 設定 pre-release 標記

## 發佈資源

每次發佈都會發佈以下資源，全部附加到 GitHub Release（wheel 亦會同步發佈至 PyPI）：

| 資源 | 平台／用途 |
| ------- | ---------------- |
| `Provisa-<tag>-macOS.dmg` | macOS Core（Apple Silicon，離線環境） |
| `Provisa-Runtime-<tag>-macOS.dmg` | macOS 原生 Python runtime（與 Core 一併掛載） |
| `Provisa-Obs-<tag>-macOS.dmg` | macOS 可觀測性擴充功能 |
| `Provisa-Demo-<tag>-macOS.dmg` | macOS 示範擴充功能（需要 Obs） |
| `Provisa-<tag>-linux-x86_64.AppImage` | Linux x86_64 core（離線環境） |
| `Provisa-<tag>-windows-x64.exe` | Windows x64 原生安裝程式（內嵌 Python，免 Docker） |
| `Provisa-Container-<tag>-windows-x64.exe` | Windows x64 容器層升級（WSL2 + Trino） |
| `provisa-jdbc-<tag>.jar` | JDBC 驅動程式 — Tableau、PowerBI、DBeaver |
| `provisa_client-<pep440>-py3-none-any.whl` | Python 客戶端（同時發佈於 PyPI） |
| `provisa-core-images-<tag>.tar.gz` | Core Services 映像 tarball（arm64，離線環境） |
| `provisa-core-images-amd64-<tag>.zip` | Core Services 映像（amd64，Windows 容器層／離線環境） |
| `provisa-obs-images-<tag>.tar.gz` | Observability Stack 映像（選用） |
| `provisa-demo-images-<tag>.tar.gz` | Demo Data Pack 映像（選用） |
| `provisa-trino-plugins-<tag>.tar.gz` | Coordination Engine 連接器（SharePoint、Splunk、File） |

Python 客戶端版本會自動轉換為 PEP 440 格式：
`v0.1.0-alpha.1` → `0.1.0a1`，`v0.1.0-beta.1` → `0.1.0b1`，`v0.1.0-rc.1` → `0.1.0rc1`。

## PyPI 發佈設定（一次性）

1. 從 `~/.pypirc` 複製你的 API 權杖（`pypi.org` 對應的 `pypi-...` 值）
2. 在 **Settings → Secrets → Actions** 中將其新增為名為 `PYPI_API_TOKEN` 的存放庫密鑰

之後每次打標籤時，`publish-pypi` 工作都會自動發佈。

## 所需的存放庫密鑰

請在 **Settings → Secrets → Actions** 中設定以下項目：

| 密鑰 | 所需用途 | 說明 |
| -------- | ------------- | ------------- |
| `PYPI_API_TOKEN` | PyPI 發佈 | 來自 `~/.pypirc` 的 API 權杖（以 `pypi-` 開頭） |
| `APPLE_CERT_P12_BASE64` | 簽署建置 | Base64 編碼的 `.p12` 憑證檔案（見下文） |
| `APPLE_CERT_P12_PASSWORD` | 簽署建置 | 從 Keychain Access 匯出 `.p12` 時設定的密碼 |
| `APPLE_DEVELOPER_ID` | 簽署建置 | 完整憑證名稱：`Developer ID Application: Your Name (TEAMID)` |
| `APPLE_NOTARYTOOL_APPLE_ID` | 公證建置 | Apple ID 電郵地址 |
| `APPLE_NOTARYTOOL_PASSWORD` | 公證建置 | 來自 appleid.apple.com 的應用程式專用密碼（並非你的登入密碼） |
| `APPLE_NOTARYTOOL_TEAM_ID` | 公證建置 | 10 個字元的 Apple Team ID |

沒有這些密鑰的建置仍可成功完成，但會產生未簽署／未公證的 DMG（使用者將會看到 Gatekeeper 警告）。

## 匯出 .p12 憑證

1. 開啟 **Keychain Access** → 登入鑰匙圈 → **My Certificates**
2. 尋找 **Developer ID Application: Your Name (TEAMID)** — 展開以確認私密金鑰嵌套於其下
3. 同時選取憑證及其私密金鑰 → 按右鍵 → **Export 2 Items** → 儲存為 `.p12` → 設定一個強密碼
4. 進行 Base64 編碼並複製到剪貼簿：

   ```bash
   base64 -i YourCert.p12 | pbcopy
   ```

5. 將其貼上作為 `APPLE_CERT_P12_BASE64` 的值；將 `APPLE_CERT_P12_PASSWORD` 設定為步驟 3 中的密碼

## 尋找你的憑證名稱

```bash
security find-identity -v -p codesigning | grep "Developer ID Application"
```

複製引號中的完整字串——這就是 `APPLE_DEVELOPER_ID` 的值。

## 刪除錯誤的標籤

```bash
git tag -d v1.2.3-alpha.1
git push origin :refs/tags/v1.2.3-alpha.1
```

然後在介面中刪除對應的 GitHub Release，再重新打標籤。
