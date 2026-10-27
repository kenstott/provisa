# 版本發佈 {: #releasing }

發佈由推送（push）git 標籤觸發。標籤名稱決定發佈渠道。

## 標籤命名慣例 {: #tag-conventions }

| 標籤格式 | 渠道 | GitHub Release 類型 |
| ----------- | --------- | ------------------- |
| `v1.2.3-alpha.1` | alpha | Pre-release |
| `v1.2.3-beta.1` | beta | Pre-release |
| `v1.2.3-rc.1` | rc | Pre-release |
| `v1.2.3` | stable | Latest release |

## 建立發佈 {: #creating-a-release }

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

## 發佈資源 {: #release-assets }

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

## PyPI 發佈設定（一次性） {: #pypi-publishing-setup-one-time }

`.github/workflows/build-dmg.yml` 中的 `publish-pypi`（`provisa-client`）及 `publish-provisa-pypi`（`provisa`）工作透過 PyPI 可信任發佈（trusted publishing）進行發佈：由工作的 GitHub OIDC 權杖（`id-token: write`）完成身分驗證，不會讀取任何 API 權杖。請在 pypi.org 上將本存放庫及 `build-dmg.yml` 註冊為每個專案的可信任發佈者（**Your projects → Manage → Publishing**）。此後每次打標籤都會自動發佈。

## 簽署與供應鏈 {: #signing-and-supply-chain }

發佈工作流程建置時，每個發佈構件所帶有的內容：

| 構件 | 簽章 |
| -------- | --------- |
| PyPI 上的 `provisa` 及 `provisa-client` | 每個檔案一份經 Sigstore 簽署的 PEP 740 證明（`attestations: true`），可在 pypi.org 上驗證 |
| PyPI 上的 `provisa-pg-ext`、`provisa-duckdb-ext` | 每個檔案一份經 Sigstore 簽署的 PEP 740 證明 |
| JDBC 驅動程式 `.jar` | Sigstore 建置來源證明（`actions/attest-build-provenance`） |
| macOS `.dmg` 安裝程式 | Developer ID 程式碼簽章及 Apple 公證 |
| Windows `.exe` 安裝程式 | 未簽署 |
| Linux `.AppImage` | 未簽署 |

任何作業系統上的已簽署安裝方式都是 `pip install provisa`：帶有 Sigstore 證明的 PyPI 套件。需要在 Linux 或 Windows 上取得已簽署構件的審閱者，請使用 pip 安裝。設定了程式碼簽署憑證時，Windows 建置會以 Authenticode 簽署（`WINDOWS_CERT_PFX_BASE64`、`WINDOWS_CERT_PFX_PASSWORD`、`WINDOWS_CERT_TIMESTAMP_URL`，由 `packaging/windows/build-sfx.ps1` 讀取）；目前並未設定，因此以未簽署形式發佈。

相依套件與 CVE 關卡：

- Python 相依套件在 `uv.lock` 中以雜湊鎖定；CI 以 `uv sync --frozen` 安裝它們。
- `CVE Audit` 工作流程（`.github/workflows/cve-audit.yml`）在每次推送至 `main`、每個提取要求以及每週，對鎖定的集合執行 `pip-audit`。
- 發佈時，`provisa` 及 `provisa-client` 的建置各自產生一份 CycloneDX SBOM，並對所發佈的內容執行 `pip-audit`。JDBC 驅動程式以 `grype --fail-on medium` 掃描。任何發現都會令工作失敗，而發佈不會發佈任何內容：PyPI 上傳及 GitHub 發佈都會等待這些工作。SBOM 及 CVE 報告會附加至每個發佈。
- 桌面安裝程式在發佈執行器上從原始碼建置 `provisa` wheel，並在建置時以 `pip` 解析其相依套件，而不是從 `uv.lock` 解析；發佈 CVE 關卡掃描的是鎖定的集合，而不是安裝程式所內附的相依套件。
- `packaging/pypi/requirements-embedded.lock` 是 `provisa[embedded]` 的雜湊固定解析結果，供離線（airgap）鏡像使用（參見 `packaging/pypi/MIRROR-CHECKLIST.md`）。它是手動重新產生的，而不是每次發佈都產生。

## 所需的存放庫密鑰 {: #required-repository-secrets }

請在 **Settings → Secrets → Actions** 中設定以下項目：

| 密鑰 | 所需用途 | 說明 |
| -------- | ------------- | ------------- |
| `APPLE_CERT_P12_BASE64` | 簽署建置 | Base64 編碼的 `.p12` 憑證檔案（見下文） |
| `APPLE_CERT_P12_PASSWORD` | 簽署建置 | 從 Keychain Access 匯出 `.p12` 時設定的密碼 |
| `APPLE_DEVELOPER_ID` | 簽署建置 | 完整憑證名稱：`Developer ID Application: Your Name (TEAMID)` |
| `APPLE_NOTARYTOOL_APPLE_ID` | 公證建置 | Apple ID 電郵地址 |
| `APPLE_NOTARYTOOL_PASSWORD` | 公證建置 | 來自 appleid.apple.com 的應用程式專用密碼（並非你的登入密碼） |
| `APPLE_NOTARYTOOL_TEAM_ID` | 公證建置 | 10 個字元的 Apple Team ID |

沒有這些密鑰的建置仍可成功完成，但會產生未簽署／未公證的 DMG（使用者將會看到 Gatekeeper 警告）。

## 匯出 .p12 憑證 {: #exporting-the-p12-certificate }

1. 開啟 **Keychain Access** → 登入鑰匙圈 → **My Certificates**
2. 尋找 **Developer ID Application: Your Name (TEAMID)**，展開以確認私密金鑰嵌套於其下
3. 同時選取憑證及其私密金鑰 → 按右鍵 → **Export 2 Items** → 儲存為 `.p12` → 設定一個強密碼
4. 進行 Base64 編碼並複製到剪貼簿：

   ```bash
   base64 -i YourCert.p12 | pbcopy
   ```

5. 將其貼上作為 `APPLE_CERT_P12_BASE64` 的值；將 `APPLE_CERT_P12_PASSWORD` 設定為步驟 3 中的密碼

## 尋找你的憑證名稱 {: #finding-your-certificate-name }

```bash
security find-identity -v -p codesigning | grep "Developer ID Application"
```

複製引號中的完整字串。這個字串就是 `APPLE_DEVELOPER_ID` 的值。

## 刪除錯誤的標籤 {: #deleting-a-bad-tag }

```bash
git tag -d v1.2.3-alpha.1
git push origin :refs/tags/v1.2.3-alpha.1
```

然後在介面中刪除對應的 GitHub Release，再重新打標籤。
