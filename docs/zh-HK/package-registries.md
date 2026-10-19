# 套件登錄庫

Node 和 Python 資料來源會安裝自己的程式碼及其相依套件。**Admin → 套件登錄庫**決定它們從哪裏安裝：機構的 npm 登錄庫、PyPI 索引和 Git 主機。機構管理員只需填寫一次，機構內每次 Node 和 Python 安裝都會使用它們。公開 npm 登錄庫和公開 PyPI 毋須填寫。

每個欄位保存所輸入的內容：一個值，或 `${secret:npm_token}`、`${env:NPM_TOKEN}` 這類參照，每次安裝時在機構自己的保管庫中解析。解析不出任何內容的參照會中止安裝，並指出登錄庫和欄位。Provisa 從不寫入保管庫，也從不自擬機密名稱。參見[機密](secrets.md)。

---

## npm 登錄庫 {: #npm-registries }

| 欄位 | 內容 |
|---|---|
| 登錄庫 URL | 登錄庫的位址，例如 `https://artifactory.example.com/artifactory/api/npm/npm/` |
| 範圍 | 例如 `@acme`，該範圍的套件從此登錄庫取得。沒有則留空 |
| 權杖或 user:password | 存取權杖，或 `user:password` |
| 作為每次安裝的鏡像 | 沒有自身範圍的套件都經此登錄庫安裝。每個機構一個 |
| 憑證授權單位 (PEM) | 參見[私有憑證授權單位](#a-private-certificate-authority) |

權杖以 `_authToken` 寫入安裝的 `.npmrc`，並作為 bearer 權杖傳送。`user:password`（例如 Artifactory 使用者及其 API 金鑰）以 `_auth`（base64）寫入，並以 Basic 方式傳送。兩者都只傳送到登錄庫自己的主機。

## PyPI 索引 {: #pypi-indexes }

| 欄位 | 內容 |
|---|---|
| 索引 URL | 索引的 simple-index 位址，例如 `https://artifactory.example.com/artifactory/api/pypi/pypi/simple/` |
| 權杖或 user:password | 存取權杖（以使用者 `__token__` 傳送），或 `user:password` |
| 作為每次安裝的預設索引 | 所有 Python 套件都從此索引安裝，而非公開 PyPI。每個機構一個 |
| 憑證授權單位 (PEM) | 參見[私有憑證授權單位](#a-private-certificate-authority) |

索引及其憑證只經安裝自己的環境傳給 uv，從不寫入檔案。

## Git 主機 {: #git-hosts }

Git 主機項目涵蓋取得資料來源程式碼或其 Git 相依套件的儲存庫。

| 欄位 | 內容 |
|---|---|
| 主機 | 主機，伺服器有連接埠和路徑前綴時一併填寫：`github.com`、`gitlab.example.com`、`bitbucket.example.com:7990/bitbucket` |
| 權杖或 user:token | 權杖，或 `user:token` |
| 憑證授權單位 (PEM) | 參見[私有憑證授權單位](#a-private-certificate-authority) |

儲存庫 URL 會配對以其主機、連接埠和路徑前綴開頭的項目。多個項目配對時，最長的勝出：`bitbucket.example.com:7990/bitbucket/scm/data` 的項目涵蓋該專案，`bitbucket.example.com:7990/bitbucket` 的項目涵蓋其餘部分。

單獨的權杖以使用者 `x-access-token` 傳送。其他供應商需要使用者名稱，請輸入 `user:token`：

| 供應商 | 使用者 |
|---|---|
| GitHub | `x-access-token`，即預設值：只輸入權杖 |
| GitLab | 任何非空白的使用者名稱 |
| Bitbucket Cloud | 儲存庫或工作區存取權杖用 `x-token-auth`；API 權杖用您的使用者名稱或電郵 |
| Bitbucket Data Center | 您的使用者名稱；專案或儲存庫權杖用 `x-token-auth` |

## 私有憑證授權單位 {: #a-private-certificate-authority }

位於您自己的憑證授權單位之後的登錄庫、索引或 Git 伺服器（例如您網絡內的 Artifactory 代理）需要信任該單位。將其 PEM 憑證貼到項目的**憑證授權單位 (PEM)**欄位，或填寫指向包含它的機密的參照。憑證受公開信任的伺服器請留空。不含 PEM 憑證的值在儲存項目時會被拒絕；解析不出憑證的參照在安裝執行時會被拒絕。

該單位與公開信任的單位一併受信任，絕不取代它們：

- Provisa 經該項目進行的擷取（套件的中繼資料和 tarball、wheel、Git 儲存庫）都會以它驗證伺服器。
- 安裝期間，uv 經 `SSL_CERT_FILE` 讀取它，npm 和 Bun 經安裝自己的 `.npmrc` 中的 `cafile` 讀取，Deno 經 `DENO_CERT`，Git 經 `GIT_SSL_CAINFO`。

這些設定指向同一個檔案，其中包含公開信任的單位和每個項目自己的單位。該檔案寫入安裝的暫存目錄，並隨之刪除，因此安裝以外的任何程式都不會讀取它。
