# 密鑰 {: #secrets }

**名稱進得去，值永遠出不來。**

沒有任何 API 端點會傳回已儲存密鑰的值，亦沒有任何介面提供「顯示」按鈕。遺失了值的人只能取代它，用的就是建立它時的同一個呼叫及同一個表單。這不是政策上的選擇：程式碼裏根本不存在讀取路徑。

---

## 引用語法 {: #reference-syntax }

在 Provisa 解析憑證的任何位置，以下三種引用形式皆有效：

| 形式 | 解析來源 | 誰可以使用 |
| ------ | -------------- | --------------- |
| `${env:VAR_NAME}` | 伺服器行程的環境 | 僅限部署設定 |
| `${secret:NAME}` | 組織保管庫——由全體成員共用 | 任何接受憑證引用的欄位 |
| `${user:NAME}` | 操作者的個人保管庫 | 任何接受憑證引用的欄位 |

在多租戶部署中，請把平台憑證放在密鑰服務中，不要放在環境變數裏。租戶管理員可以在自己的憑證欄位裏寫入 `${env:...}`，所以程序環境中的任何值都在每個租戶的可及範圍內。只把密鑰後端自身的憑證留在環境中。

整條解析路徑一律失效即關閉。未知的提供者名稱、未設定的名稱、無法連通的後端，三者都會引發錯誤。無法解析的引用絕不會被靜默替換成空字串。

### 名稱格式 {: #name-format }

密鑰名稱必須符合 `[A-Za-z_][A-Za-z0-9_]*`：字母、數字及底線，以字母或底線開頭。原因很實際：`${secret:NAME}` 由參照語法剖析，它會一直讀到右花括號 `}`，所以名稱中帶有花括號、空格或冒號，就會被剖析成別的東西。

---

## 兩個保管庫，一項服務 {: #two-vaults-one-service }

每個組織都有兩個保管庫，兩者同處於一項密鑰服務之內。

**組織保管庫。** 組織管理員存放在這裏的憑證是共用的，所以凡引用 `${secret:DATABASE_TOKEN}` 的成員，取得的都是同一個值。它用於*組織*本身擁有的憑證，例如共用的資料庫密碼、服務帳戶金鑰或部署權杖。讀寫組織保管庫需要 `org_settings` 能力。

**個人保管庫。** 存放在這裏的憑證只屬於一個人。當兩個人各自有一個 `GIT_TOKEN` 時，`${user:GIT_TOKEN}` 會解析為目前操作者的那個，所以同一段參照文字會給每個人各自的憑證。甚麼都沒存的人會得到錯誤，永遠不會拿到別人的值。個人保管庫不受任何能力限制，因為持有自己的憑證不是管理員授予的特權；亦沒有任何請求語法可以指定別人的保管庫。

作用範圍是參照本身的一部分，外面沒有包一層權限。`${secret:NAME}` 及 `${user:NAME}` 永遠不會互相代替。

---

## 選擇密鑰服務 {: #choosing-a-secrets-service }

**管理 → 安全 → 密鑰服務。** 持有 `platform_settings` 能力的人都能看到這個面板。它列出該版本已知的所有後端，無論其 SDK 是否已安裝；灰顯的列會說明缺少哪個 Python 套件，而不是把選項藏起來。

隨附五個後端：

| 索引鍵 | 標籤 | 需要 |
| ----- | ------- | ------- |
| `provisa` | Provisa（內建，已加密） | 無；此為預設 |
| `hashicorp_vault` | HashiCorp Vault (KV v2) | `hvac` |
| `aws_secrets_manager` | AWS Secrets Manager | `boto3` |
| `gcp_secret_manager` | Google Secret Manager | `google-cloud-secret-manager` |
| `azure_key_vault` | Azure Key Vault（密鑰） | `azure-keyvault-secrets` |

選擇採用失敗即關閉的方式：未知或不可用的後端會令啟動報錯停止，Provisa 從不悄悄改用別的後端。

### 後端自己的憑證 {: #the-backends-own-credential }

中央後端的連線憑證屬於程序設定，只能來自 `${env:...}`。`${secret:...}` 無法提供它，因為一個把自身憑證存放在自己裏面的密鑰服務永遠打不開。信任鏈按設計終止於主機環境。Provisa 在建構後端之前，會從環境中解析該後端的每個設定值。

例如，`provisa.yaml` 中的 Vault 設定：

```yaml
secrets:
  provider: hashicorp_vault
  hashicorp_vault:
    url: https://vault.internal:8200
    token: ${env:VAULT_TOKEN}   # process env only — never ${secret:...}
    mount: secret
```

### 中央服務與內建服務之別 {: #central-service-vs-built-in }

設定了中央服務時，Provisa 只從中讀取，從不寫入。建立及刪除項目歸該服務自己的工具負責，所以「密鑰」頁面會說明這一點，亦不提供建立按鈕。

啟用內置的 `provisa` 後端時，「密鑰」頁面完全可寫。你可以在介面中或透過 API 建立、取代及刪除密鑰。

---

### 組織在中央服務中的密鑰 {: #one-organizations-secrets-in-a-central-service }

承載多個組織並讀取中央服務的部署，會為每個組織在該組織的前綴之內讀取服務。參照維持為短名稱（`${secret:db#password}`）；前綴在讀取時加上。

| 服務 | 組織 `acme` 的密鑰 `db` 存放位置 |
|---|---|
| HashiCorp Vault (KV v2) | `orgs/acme/db` |
| AWS Secrets Manager | `orgs/acme/db` |
| GCP Secret Manager | `org-acme--db` |
| Azure Key Vault | `o4-acme-db`（`o`、組織 id 的長度、把 `_` 寫作 `-` 的 id，然後是名稱） |

在以下情況，參照會被點名拒絕：

- 其名稱會離開前綴：以 `/` 開頭、包含 `..` 段或反斜線，或在名稱為扁平的 GCP 或 Azure 上包含 `/`；
- 加上前綴後，其名稱對該服務而言過長（GCP 255 個字元，Azure 127 個）；這會在儲存參照時被拒絕，並指出上限；
- 讀取它的操作沒有綁定任何組織。

單一組織的部署按參照的原樣讀取，不加前綴：它所連接的服務屬於該組織，無需遷移任何內容。

**遷移至前綴之下。** 在多組織部署中，組織參照的每個密鑰都必須位於該組織的前綴之下。用服務本身的工具把每個密鑰複製過去，然後把參照縮短為前綴之內的名稱（存放於 `orgs/acme/db` 的 `${secret:teams/data/db}` 變成 `${secret:db}`）。密鑰尚未遷移的參照會以「找不到」失敗；它不會從原本的位置讀取。

## Provisa 的內建儲存區 {: #provisas-built-in-store }

未設定中央服務時預設使用它。`secrets_store` 中的每一列都存放一個加密的信封資料塊。`value` 欄是二進位的，解密金鑰位於程序環境中，在資料庫之外。一份沒有該部署主金鑰的控制平面副本，只有密文，別無他物。

加密從來不是可選項。未設定程序級加密金鑰時，儲存會使用本機鑰匙圈。如果主機沒有可存放金鑰的鑰匙圈，儲存會拒絕寫入，值永遠不會以明文儲存。

**儲存形狀**：

| 欄位 | 類型 | 用途 |
| -------- | ------ | --------- |
| `org_id` | Text | 擁有此密鑰的組織 |
| `owner_id` | Text | 組織保管庫為 `"*"`；個人保管庫為使用者 id |
| `name` | Text | 引用名稱 |
| `value` | LargeBinary | 加密的信封 blob |
| `description` | Text | 這個密鑰的用途——絕不從值推導而來 |
| `updated_by` | Text | 最後設定它的人 |

已建立自有加密金鑰的組織，其密鑰由該金鑰包裹；否則由部署的金鑰包裹。參見[靜態加密](security.md#encryption-at-rest-and-the-organizations-own-keys)。

任何列表查詢都不會選取 `value` 欄位。

---

## API 端點 {: #api-endpoints }

所有路由都位於 `/admin/orgs/{org_id}` 之下。組織保管庫需要該組織中的 `org_settings`。個人保管庫不需要任何能力，因為擁有者取自已認證的身分，亦沒有任何請求參數可以指定別人的保管庫。

| 方法 | 路徑 | 作用 |
| -------- | ------ | ------------- |
| `GET` | `/secrets` | 列出組織保管庫的名稱與引用 |
| `PUT` | `/secrets/{name}` | 建立或取代一個組織密鑰 |
| `DELETE` | `/secrets/{name}` | 刪除一個組織密鑰 |
| `GET` | `/my-secrets` | 列出呼叫者個人的名稱與引用 |
| `PUT` | `/my-secrets/{name}` | 建立或取代呼叫者的一個密鑰 |
| `DELETE` | `/my-secrets/{name}` | 刪除呼叫者的一個密鑰 |

每個回應都傳回中繼資料，從不傳回值：名稱、描述、`updated_at`、`updated_by`，以及可貼上的 `reference` 字串。`PUT` 請求主體攜帶 `value`（必填）及 `description`（選填）。取代與建立是同一個呼叫，因為名稱就是識別，沒有獨立的 ID。

每次寫入都會記入稽核日誌。日誌項目寫明操作者及密鑰名稱。值及值的長度都不會記錄。

---

## `${secret:NAME}` 在哪裡解析 {: #where-secretname-resolves }

解析發生在綁定的操作內部。匯入時及啟動時都不會解析任何東西。儲存在操作開始時讀取並解密該組織的密鑰一次，並在操作期間一直持有這份對應。在綁定的操作之外，`${secret:NAME}` 會報錯。

有兩處呼叫點會建立這項繫結：

**Git 遠端操作。** 組織程式碼儲存庫的遠端 URL 可以包含 `${secret:...}` 或 `${user:...}` 參照，例如嵌在 URL 中的推送權杖。環境路由器隨後會圍繞這次 git 呼叫，同時綁定組織保管庫及目前操作使用者的個人保管庫。使用 `${user:GIT_TOKEN}` 形式時，提交會記在推送者本人的憑證之下，不涉及任何共用的服務帳戶。

**讀取 AI 供應商 API 金鑰。** 當 Provisa 讀取組織的 LLM 供應商金鑰，而該金鑰以 `${secret:NAME}` 參照儲存時，它會為這次請求綁定組織保管庫。參照在出站時解析，參照文字本身永遠不會發給供應商。

---

## 組織 AI 供應商金鑰作為密鑰引用 {: #org-ai-vendor-keys-as-secret-references }

組織的 AI 供應商金鑰（Anthropic、OpenAI 及其他）可以存成 `${secret:NAME}` 引用，而非字面上的金鑰。

先把金鑰存進組織保管庫：

```
PUT /admin/orgs/{org_id}/secrets/OPENAI_KEY
{ "value": "sk-...", "description": "OpenAI production key" }
```

然後把組織的 AI 設定指向它：

```
vendor key field → ${secret:OPENAI_KEY}
```

參照以加密形式存放在 `org_secrets` 中。查詢時，Provisa 依據組織保管庫解析 `${secret:OPENAI_KEY}`，並把實際金鑰交給供應商的 SDK。輪換保管庫項目會即時生效，無須修改組織設定中的設定。

---

## 部署本身的儲存區憑據 {: #the-deployments-own-store-credentials }

Provisa 執行的各儲存區的密碼（PostgreSQL、Redis、MinIO、Grafana 管理員帳戶與 Trino 叢集的共用密鑰）在兩個保管庫中都不算密鑰。它們按部署產生並由部署保管：保存於 `~/.provisa/store-credentials.env`（權限 600）、Terraform 狀態，或 Helm chart 的 `credentials.existingSecret` 所指定的 Kubernetes Secret 中。參見[部署加固](security.md#deployment-hardening)。

---

## 平台管理員的存取 {: #platform-admin-access }

營運控制平面的平台管理員無法讀取任何組織的密鑰值。`org_settings` 守衛會直接拒絕 `cross_org` 及平台繞過，因為管理組織的生命週期並不賦予讀取該組織憑證的權限。伺服器自行強制執行這一點，與介面顯示甚麼無關。

---

## 另見 {: #see-also }

- [安全模型](security.md)——分層存取控制、身份驗證與審計記錄
- [設定參考](configuration.md)——行程層級憑證的 `${env:VAR}` 語法
