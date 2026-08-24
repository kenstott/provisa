# 密鑰

**名稱進得去，值永遠出不來。**

沒有任何 API 端點會回傳已儲存的密鑰值，UI 上也沒有「顯示」按鈕。遺失了某個值的人只能重新設定它——那與當初建立它的是同一個呼叫，經同一個表單完成。這並非政策上的取捨：程式碼裡根本不存在讀取路徑。（REQ-1558）

---

## 引用語法

在 Provisa 解析憑證的任何位置，以下三種引用形式皆有效：

| 形式 | 解析來源 | 誰可以使用 |
| ------ | -------------- | --------------- |
| `${env:VAR_NAME}` | 伺服器行程的環境 | 僅限部署設定 |
| `${secret:NAME}` | 組織保管庫——由全體成員共用 | 任何接受憑證引用的欄位 |
| `${user:NAME}` | 操作者的個人保管庫 | 任何接受憑證引用的欄位 |

整條解析路徑一律失效即關閉。未知的提供者名稱、未設定的名稱、無法連通的後端，三者都會引發錯誤。無法解析的引用絕不會被靜默替換成空字串。（REQ-1557）[tool-verified: `provisa/core/secrets.py:92-117`]

### 名稱格式

密鑰名稱必須符合 `[A-Za-z_][A-Za-z0-9_]*`——字母、數字與底線，且以字母或底線開頭。這項限制出於實務考量：`${secret:NAME}` 由引用語法剖析，而它會一直讀到結尾的 `}`。名稱若含大括號、空格或冒號，剖析出來的就會是另一回事。[tool-verified: `provisa/core/secrets_store.py:61`]

---

## 兩個保管庫，一項服務

每個組織都有兩個保管庫，兩者同處於一項密鑰服務之內。（REQ-1560）

**組織保管庫**——組織管理員存放在這裡的憑證是共用的。凡引用 `${secret:DATABASE_TOKEN}` 的成員，取得的都是同一個值。此處適合放*組織*本身擁有的憑證：共用的資料庫密碼、服務帳戶金鑰、部署權杖。讀寫組織保管庫都需要 `org_settings` 能力。

**個人保管庫**——存放在這裡的憑證只屬於一個人。當兩個人各自持有一個 `GIT_TOKEN` 時，`${user:GIT_TOKEN}` 會解析成當下操作者的那一個。同一段引用文字，交到每個人手上的都是他自己的憑證。什麼都沒存過的人得到的是錯誤，而不是別人的值。個人保管庫不受任何能力管控——持有自己的憑證，不是由管理員授予的特權。而且請求語法中也沒有任何辦法指名他人的保管庫。[tool-verified: `provisa/api/admin/secrets_router.py:86-103`]

範圍是引用本身的一部分，而不是套在它外面的權限。`${secret:NAME}` 與 `${user:NAME}` 永遠不會互相作答。

---

## 選擇密鑰服務

**管理 → 安全 → 密鑰服務。** 持有 `platform_settings` 能力的人可以看到這個面板。凡此組建認得的後端一律列出，不論 SDK 是否已安裝。灰底的一列會告訴你缺的是哪個 Python 套件——面板會把它點名，而不是索性把該選項藏起來。

隨附五個後端：

| 索引鍵 | 標籤 | 需要 |
| ----- | ------- | ------- |
| `provisa` | Provisa（內建，已加密） | 無；此為預設 |
| `hashicorp_vault` | HashiCorp Vault (KV v2) | `hvac` |
| `aws_secrets_manager` | AWS Secrets Manager | `boto3` |
| `gcp_secret_manager` | Google Secret Manager | `google-cloud-secret-manager` |
| `azure_key_vault` | Azure Key Vault（密鑰） | `azure-keyvault-secrets` |

[tool-verified: `provisa/core/secrets_registry.py:161-299`]

選取一律失效即關閉：未知或不可用的後端會在啟動時引發錯誤，而不是靜默改用另一個。（REQ-1557）

### 後端自己的憑證

中央後端的連線憑證屬於行程設定。它只來自 `${env:...}`——絕不來自 `${secret:...}`。連線憑證放在自己裡面的密鑰服務打不開，因此信任鏈按設計終止於主機環境。註冊表會強制這一點：後端規格上的任何設定值，都會先以 `providers=("env",)` 解析，然後才建構該後端。[tool-verified: `provisa/core/secrets_registry.py:128-141`]

範例——`provisa.yaml` 中的 Vault 設定：

```yaml
secrets:
  provider: hashicorp_vault
  hashicorp_vault:
    url: https://vault.internal:8200
    token: ${env:VAULT_TOKEN}   # process env only — never ${secret:...}
    mount: secret
```

### 中央服務與內建服務之別

設定了中央服務之後，Provisa 只從它讀取，不向它寫入。建立與刪除項目由中央服務自己作主——那些操作屬於它自己的工具鏈。密鑰頁面會如實說明，並且不提供建立按鈕。（REQ-1557）

內建的 `provisa` 後端啟用時，密鑰頁面完全可寫：在 UI 或透過 API 建立、取代與刪除皆可。

---

## Provisa 的內建儲存區

未設定中央服務時的預設值。`secrets_store` 的每一行都存著一個加密的信封 blob——`value` 欄位是二進位而非文字，而解密金鑰放在行程環境裡，不在資料庫裡。一份沒有該部署主金鑰的控制平面副本，握有的只是密文，別無他物。（REQ-1558）

加密從不是可選項。當行程層級未設定加密金鑰時，儲存區會退而使用本機鑰匙圈。若主機沒有鑰匙圈可放金鑰，儲存區寧可拒絕寫入，也不會把值以明文存下。[tool-verified: `provisa/core/secrets_store.py:130-159`]

**儲存形狀** [tool-verified: `provisa/core/schema_admin.py:493-505`]：

| 欄位 | 類型 | 用途 |
| -------- | ------ | --------- |
| `org_id` | Text | 擁有此密鑰的組織 |
| `owner_id` | Text | 組織保管庫為 `"*"`；個人保管庫為使用者 id |
| `name` | Text | 引用名稱 |
| `value` | LargeBinary | 加密的信封 blob |
| `description` | Text | 這個密鑰的用途——絕不從值推導而來 |
| `updated_by` | Text | 最後設定它的人 |

任何列表查詢都不會選取 `value` 欄位。[tool-verified: `provisa/core/secrets_store.py:214-235`]

---

## API 端點

所有路由都位於 `/admin/orgs/{org_id}` 之下。組織保管庫需要在該組織內持有 `org_settings`。個人保管庫不需任何能力——擁有者是從已驗證的身分讀出來的；請求參數中沒有任何辦法指名他人的保管庫。

| 方法 | 路徑 | 作用 |
| -------- | ------ | ------------- |
| `GET` | `/secrets` | 列出組織保管庫的名稱與引用 |
| `PUT` | `/secrets/{name}` | 建立或取代一個組織密鑰 |
| `DELETE` | `/secrets/{name}` | 刪除一個組織密鑰 |
| `GET` | `/my-secrets` | 列出呼叫者個人的名稱與引用 |
| `PUT` | `/my-secrets/{name}` | 建立或取代呼叫者的一個密鑰 |
| `DELETE` | `/my-secrets/{name}` | 刪除呼叫者的一個密鑰 |

每個回應都會傳回中繼資料——名稱、說明、`updated_at`、`updated_by`，以及可直接貼上的 `reference` 字串——但永不傳回值。`PUT` 主體帶有 `value`（必填）與 `description`（選填）。取代與建立是同一個呼叫：名稱即身分，並沒有另一個獨立的 ID。

每一次寫入都會記入審計記錄。記錄項目會寫明操作者與密鑰名稱，但不記錄值，連它的長度也不記。[tool-verified: `provisa/api/admin/secrets_router.py:106-117`]

---

## `${secret:NAME}` 在哪裡解析

解析發生在一次繫結了脈絡的操作之內，而非匯入時或啟動時。儲存區在該操作開始時把組織的密鑰讀出並解密一次，然後把這份對應表放在一個 `ContextVar` 裡，供該操作全程使用。不在已繫結的操作之內，`${secret:NAME}` 就會引發錯誤。（REQ-1557）[tool-verified: `provisa/core/secrets_store.py:269-290`]

有兩處呼叫點會建立這項繫結：

**Git 遠端操作。** 當組織的存放庫遠端 URL 含有 `${secret:...}` 或 `${user:...}` 引用時——例如嵌在 URL 裡的推送權杖——環境路由器會在那次 git 呼叫外圍同時繫結組織保管庫與操作者的個人保管庫。`${user:GIT_TOKEN}` 這種形式意味著提交會落在推送者本人的憑證之下，而不是某個共用的服務帳戶。[tool-verified: `provisa/api/admin/environments_router.py:1263`]

**AI 供應商 API 金鑰的讀取。** 當 Provisa 讀取某組織的 LLM 供應商金鑰，而該金鑰是以 `${secret:NAME}` 引用形式存放時，`bound_to_request_org` 會為該請求建立組織保管庫的繫結。引用在送出的路上被解析；引用文字本身絕不會送到供應商那邊。（REQ-1580）[tool-verified: `provisa/core/org_secrets.py:76-79`]

---

## 組織 AI 供應商金鑰作為密鑰引用

組織的 AI 供應商金鑰（Anthropic、OpenAI 及其他）可以存成 `${secret:NAME}` 引用，而非字面上的金鑰。（REQ-1580）

先把金鑰存進組織保管庫：

```
PUT /admin/orgs/{org_id}/secrets/OPENAI_KEY
{ "value": "sk-...", "description": "OpenAI production key" }
```

然後把組織的 AI 設定指向它：

```
vendor key field → ${secret:OPENAI_KEY}
```

該引用會加密存放於 `org_secrets`。查詢時 Provisa 會對組織保管庫解析 `${secret:OPENAI_KEY}`，並把字面上的金鑰交給供應商 SDK。輪替保管庫項目即時生效——組織設定那一側毋須任何設定變更。[tool-verified: `provisa/core/org_secrets.py:64-79`]

---

## 平台管理員的存取

操作控制平面的平台管理員，讀不到任何組織的密鑰值。`org_settings` 關卡明確拒絕 `cross_org` 以及平台繞道：管理一個組織的生命週期，不等於讀取該組織保存的憑證。伺服器獨立於 UI 強制這一點。（REQ-1361）[tool-verified: `provisa/api/admin/secrets_router.py:53-83`]

---

## 另見

- [安全模型](security.md)——分層存取控制、身份驗證與審計記錄
- [設定參考](configuration.md)——行程層級憑證的 `${env:VAR}` 語法
