# 安全模型
Provisa 在每一種查詢語言（GraphQL、SQL、Cypher）與每一種傳輸（REST、gRPC、Arrow Flight、JDBC、WebSocket）上，都強制執行一套多層次的安全模型。（REQ-001、REQ-266）治理一律統一套用——沒有任何一條查詢路徑可以繞過它。（REQ-002、REQ-266）

各層依序套用。一個請求必須通過每一層，下一層才會被評估。

## 分層模型
### 第 0 層——內省過濾
呈現給某個角色的結構描述與目錄，只包含其 `domain_access` 清單中的表，以及通過各欄位 `visible_to` 規則的欄位。（REQ-039）落在角色存取範圍之外的物件，在探索階段就是不可見的——它們無法被查詢、無法自動完成，也無法被推斷出存在。（REQ-039）這適用於 GraphQL 結構描述、SQL 目錄，以及查詢編輯器的結構描述瀏覽器。（REQ-039、REQ-363）

見 [結構描述可見性](#schema-visibility)。

### 第 1 層——公開存取
位於沒有 `domain_access` 限制之網域中的表，對所有已驗證的身份都可見，毋須任何額外設定。對真正公開的數據零阻力。

### 第 2 層——網域存取
每個角色帶有一份網域 ID 的 `domain_access` 清單。觸及這些網域之外之表的查詢，會在執行前被拒絕。（REQ-038、REQ-039）這是粗粒度的所有權邊界——不論 SQL 怎麼寫，HR 角色都碰不到財務的表。（REQ-002）

見 [權限模型](#rights-model)。

### 第 3 層——行層級安全
確認網域存取之後，按表、按角色的 `WHERE` 謂詞會在執行時注入每一個 `SELECT`。（REQ-041、REQ-263）這些謂詞針對原始數據求值。查詢一張共用訂單表的區域經理，即使下 `SELECT *` 也只看到自己區域的行。（REQ-264）

見 [行層級安全（RLS）](#row-level-security-rls)。

### 第 4 層——欄位可見性與遮罩
`visible_to` 清單中不含請求角色的欄位，會從查詢輸出中剝除。（REQ-040、REQ-263）帶有遮罩規則的欄位，其值會在結果離開伺服器之前被替換——正規表示式塗銷、常數替換或截斷。（REQ-263）遮罩在所有查詢語言與輸出格式中皆適用。（REQ-263）

見 [欄位權限模型](#column-permission-model) 與 [欄位層級遮罩](#column-level-masking)。

### 第 5 層——謂詞防護
被遮罩的欄位會從 `WHERE` 與 `HAVING` 子句中拒絕。（REQ-263）沒有這一層，呼叫方即使輸出被遮罩，仍可在篩選中以二分搜尋推斷出未遮罩的值。拒絕是在查詢剖析時、執行之前強制執行的。（REQ-531）

### 關係治理（V002） {#relationship-governance-v002}
SQL 中的 JOIN 條件，必須符合表與表之間一項已註冊、已批准的關係。（REQ-001）未批准的 join 會被拒絕。每項關係帶有一段人類可讀的理由與說明——為使用者與自主代理雙方，說明某條走訪路徑為何存在。這是治理政策，不是硬性的安全邊界：不論 join 結構如何，第 2 至 5 層都成立，因此蓄意的規避並不會揭露該角色透過兩次分開的查詢所觸及不到的數據。規避的嘗試會被記錄並可供審計。

**繞過機制**——V002 有兩種繞過方式。第一種是一項能力：持有 `ignore_relationships` 的角色，可跨目錄未涵蓋的關係進行 join。在已播種的系統角色中只有 `modeler` 持有它——那是負責決定模型、而非強制執行模型的探索角色。（REQ-1297）`analyst` 並不持有。[tool-verified: `provisa/core/db.py:84`]

第二種是一項雙條件退出，兩者必須同時為真：

1. **角色旗標**——角色定義上的 `relationship_guard: false`（預設：`true`）。[tool-verified: `provisa/core/models.py:349`]
2. **逐查詢退出**——SQL 中含有註解 `--relationship-guard=false`。[tool-verified: `provisa/compiler/params.py:80`]

單有角色旗標不會繞過 V002；單有註解也不會繞過 V002。

**高安全模式把防護釘死。** 在 `security.mode: high` 之下，兩種繞過都不適用：`ignore_relationships` 被忽略、`relationship_guard: false` 被忽略，且每一個 join 都必須存在於已批准的關係目錄中。（REQ-693）這是刻意的冗餘——一個誤獲該能力的正式環境角色，仍然無法衝出模型之外。[tool-verified: `provisa/pgwire/_pipeline.py:377`]

**GraphQL 路徑**——對 GraphQL 查詢，V002 無條件略過。SDL 定義的關係按設計即為預先批准；該檢查是多餘的，因而不套用。[tool-verified: `provisa/api/data/endpoint.py:468`]

**SQL 與 Cypher 路徑**——V002 預設啟用。`endpoint_dev.py` 與 `cypher_router.py` 都在呼叫 `validate_sql` 之前套用該雙條件檢查。[tool-verified: `provisa/api/data/endpoint_dev.py:127`, `provisa/api/rest/cypher_router.py:260`]

**pgwire 路徑**——與 SQL 相同的雙條件檢查。`--relationship-guard=false` 註解會在執行前從查詢中剝除；它不會抵達資料庫。[tool-verified: `provisa/pgwire/_pipeline.py:60`]

---

這些層會組合起來。一個帶有網域存取、RLS 與遮罩欄位的角色，五項約束同時生效。加入新的數據來源、欄位或關係，毋須更新每一條規則——每一層都獨立設定，並自動套用於任何觸及受治理物件的查詢。

---

## 權限模型 {#rights-model}
各項能力獨立指派，並可經 `parent_role_id` 選用角色階層。`admin` 授予全部。（REQ-042）

| 能力 | 說明 |
| ----------- | ------------- |
| `source_registration` | 註冊數據來源 |
| `table_registration` | 註冊表、欄位 |
| `create_relationship` | 定義外部索引鍵關係 |
| `access_config` | 設定 RLS、遮罩 |
| `query_development` | 執行查詢 |
| `write` | 調用已註冊的變更操作（粗粒度閘門；見「變更操作授權」） |
| `full_results` | 繞過取樣上限 |
| `ignore_relationships` | 繞過關係治理（V002）。系統角色中僅 `modeler` 持有，且在高安全模式下完全被忽略 |
| `admin` | Superuser——授予全部 |

### 角色繼承
角色可經 `parent_role_id` 從上層角色繼承能力與網域存取。（REQ-215）該階層在啟動時被攤平——子角色把上層角色的能力與網域存取，併入自己的。（REQ-215）

```yaml
roles:
  - id: basic_user
    capabilities: [query_development]
    domain_access: [public]
  - id: analyst
    capabilities: [full_results]
    domain_access: [sales, analytics]
    parent_role_id: basic_user   # inherits query_development + public domain
```

## 欄位權限模型 {#column-permission-model}
每個欄位有一個四欄位的權限模型，按角色控制讀取、寫入與遮罩存取。（REQ-042、REQ-249）

### 三層可見性
| 層級 | 條件 | 結果 |
| ------ | ----------- | -------- |
| **隱藏** | 角色不在 `visible_to` 中 | 欄位不出現於 GraphQL SDL |
| **已遮罩** | 角色在 `visible_to` 中、有遮罩規則、角色不在 `unmasked_to` 中 | 欄位可見但數據在 SQL 中被遮罩 |
| **未遮罩** | 角色在 `visible_to` 中 且 角色在 `unmasked_to` 中（或無遮罩規則） | 完整讀取存取 |

### 寫入權限
| 欄位 | 留空的意思 | 用途 |
| ------- | ------------ | --------- |
| `visible_to` | 所有角色都可讀 | 控制誰看得見該欄位（遮罩或未遮罩） |
| `unmasked_to` | 沒有角色看得到未遮罩的值 | 控制誰繞過遮罩 |
| `writable_by` | 沒有角色可寫 | 控制誰可變更（INSERT/UPDATE） |

寫入權限在變更操作管線中強制執行。不在 `writable_by` 中的角色，嘗試寫入受限欄位時會收到 403 錯誤。（REQ-033、REQ-034）

### 範例
```yaml
columns:
  - name: email
    visible_to: [admin, analyst, viewer]
    writable_by: [admin]
    unmasked_to: [admin]
    mask_type: regex
    mask_pattern: "(.).*@"
    mask_replace: "$1***@"
  - name: salary
    visible_to: [admin, hr]
    writable_by: [hr]
    unmasked_to: [admin, hr]
    mask_type: constant
    mask_value: "0"
  - name: created_at
    visible_to: []           # all can read
    writable_by: []          # nobody can write (auto-set)
```

在這個例子中：

- `email`：admin 看到 `alice@example.com` 並可編輯；analyst／viewer 看到 `a***@example.com`
- `salary`：admin 與 hr 看到真實值；hr 可編輯；所有其他角色根本看不到該欄位
- `created_at`：所有人都可讀，沒有人可寫

## 變更操作授權
已註冊的變更操作（遠端 GraphQL、OpenAPI、gRPC、Hasura）由兩項獨立檢查管束。（REQ-867、REQ-868）角色必須同時持有全域 `write` 能力，並出現在該變更操作的 `writable_by` 清單中，才可調用它。（REQ-868）`writable_by` 留空即為預設拒絕——沒有角色能調用它。（REQ-867）

變更操作按合約歸類為寫入，而非按呼叫方的宣告。（REQ-869）引用某個變更操作類函式的 `SELECT` 會被提升為寫入，並適用同樣的雙閘門檢查，因此呼叫方無法把變更操作偽裝成讀取來調用它。（REQ-869）把變更操作重新歸類為讀取安全，需要 `access_config` 能力，並被記錄為一項治理決定；沒有逐請求的退出方式。（REQ-870）

## 結構描述可見性 {#schema-visibility}
按角色的 GraphQL 結構描述會隱藏未授權的內容：（REQ-039）

- **網域存取**：角色只看到其 `domain_access` 網域中的表（`"*"` = 全部）（REQ-039）
- **欄位可見性**：對某角色而言不在 `visible_to` 中的欄位，會從 SDL 中略去（REQ-039）
- 未授權的表／欄位不會出現在結構描述中（REQ-039）

## 行層級安全（RLS） {#row-level-security-rls}
按表、按角色注入 SQL WHERE 子句。於編譯之後、執行之前套用。（REQ-041、REQ-263）

```yaml
rls_rules:
  - table_id: orders
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"
```

該篩選器會以 AND 併入查詢的 WHERE 子句。查詢與變更操作（UPDATE/DELETE）皆適用。（REQ-035、REQ-041）

## 欄位層級遮罩 {#column-level-masking}
遮罩每欄位定義一次——它是欄位的屬性，不是角色的屬性。`unmasked_to` 欄位控制哪些角色可繞過它。（REQ-249）

| 遮罩類型 | 支援的型別 | SQL 運算式 |
| ----------- | ---------------- | ---------------- |
| `regex` | 字串（varchar、char、text） | `REGEXP_REPLACE(col, pattern, replace)` |
| `constant` | 任意 | 字面值（NULL、0、自訂） |
| `truncate` | 日期／時間戳記 | `DATE_TRUNC(precision, col)` |

遮罩被下推進 SQL SELECT 投影——由資料庫傳回已遮罩的數據。（REQ-263）對被遮罩的角色而言，未遮罩的數據絕不會經過線路。（REQ-263）被遮罩的欄位同時也被擋在 `WHERE` 與 `HAVING` 子句之外（第 5 層謂詞防護），以防止經由篩選推斷出未遮罩的值。（REQ-263、REQ-531）

## 取樣
除非具備 `full_results` 能力，否則所有角色看到的都是取樣結果（預設：100 行）。（REQ-554）經 `PROVISA_SAMPLE_SIZE` 環境變數控制。（REQ-554）

## 審計記錄
每一個觸及網域資產的查詢，都被記錄到僅可附加的 `query_audit_log`。（REQ-596、REQ-613）每一行擷取 `tenant_id`、`user_id`、`role_id`、查詢文字的 SHA-256 雜湊值、`table_ids`、`source`、`status_code`、`duration_ms` 與 `logged_at`。（REQ-596）查詢文字絕不逐字儲存——只存其雜湊值。（REQ-596）

該記錄在資料庫層級即為僅可附加：PostgreSQL 規則擋住 `DELETE` 與 `UPDATE`。（REQ-596、REQ-613）兩個索引——`(tenant_id, logged_at)` 與 `(user_id, logged_at)`——支援租用戶範圍與按使用者的時間範圍合規查詢。（REQ-596、REQ-613）

啟用加密時，查詢文字雜湊欄位以加密形式儲存，僅在授權的管理讀取時解密。（REQ-689）

## 速率限制
按角色的速率限制在 `provisa.yaml` 中設定：每秒最大請求數、最大並行 SSE 訂閱數，以及最大並行 Arrow Flight 串流數。（REQ-369）限制在 API 層、於編譯或執行之前強制執行；超出限制的請求以 HTTP 429 及 `Retry-After` 標頭拒絕。（REQ-369）

NL 查詢服務（`POST /query/nl`）經 `nl.rate_limit`（每角色每分鐘請求數）另有獨立的限制。超出限制的請求，在任何 LLM 呼叫發生之前即被拒絕。（REQ-370）

速率限制狀態以滑動視窗計數器的形式存放在 Redis（`cache.redis_url`）——沒有逐執行個體的狀態——因此限制在所有水平擴展的 Provisa 執行個體之間都成立。（REQ-371）

## 身份驗證
可插拔的驗證提供者：（REQ-120）

| 提供者 | 權杖類型 | 使用情境 |
| ---------- | ----------- | ---------- |
| `none` | X-Provisa-Role 標頭 | 開發 |
| `basic` | bcrypt 本機帳戶 + JWT | 自足式部署 |
| `firebase` | Firebase ID 權杖 | 正式環境 |
| `keycloak` | Keycloak JWT | 企業 |
| `oauth` | OIDC JWT | PingFed、Okta、Azure AD、Auth0 |
| `simple` | bcrypt + JWT | 測試 |

角色對應：身份宣告 → 經可設定的規則對應到 Provisa 角色。（REQ-120）`assignments_source` 欄位控制角色指派從何而來：`claims` 從 JWT 權杖宣告中讀取（預設），`provisa` 則從 Provisa 內部的指派儲存區讀取。（REQ-551）

在 `provisa.yaml` 中設定的 superuser（使用者名稱加上取自環境密鑰的密碼），不論設定了哪個提供者，一律取得 admin 角色與所有能力——這是初次設定的引導路徑。（REQ-125）

### 各介面與憑證
每個介面都經同一份提供者合約驗證，因此凡協定載得動之處，能在其中一個介面上用的憑證，在全部介面上都能用。（REQ-124、REQ-1263）此表是唯一的參照；各介面文件不再重述。

| 介面 | 密碼 | 提供者權杖 | 個人存取權杖 | 用戶端憑證（mTLS） |
| --------- | ---------- | ---------------- | ----------------------- | --------------------------- |
| HTTP（REST、JSON:API、GraphQL） | `Authorization: Basic` | `Authorization: Bearer` | `Authorization: Bearer` | 經終止代理 |
| pgwire | 密碼欄位（明文或 SCRAM） | 密碼欄位，OIDC 部署 | 密碼欄位 | 是 |
| Bolt | `basic` 方案 | `bearer` 方案 | `bearer` 方案 | 是 |
| Arrow Flight | — | 交握或票證負載中的 `token` | 同上 | 是 |
| gRPC | — | `authorization` 中繼資料 | `authorization` 中繼資料 | 是 |
| MCP | — | `Authorization: Bearer` | `Authorization: Bearer` | 經終止代理 |

某格為 `—` 之處，表示該協定沒有可與密碼配對的使用者名稱欄位；權杖形式已涵蓋它。pgwire 是鏡像的情形：啟動封包只有一個密鑰欄位、沒有方案，因此該密鑰*是什麼*決定了方法——PAT 由其前綴辨識，設定的提供者為權杖提供者時該密鑰讀作 bearer 權杖，其餘則為密碼。這個選擇只做一次——所選驗證器拒絕的憑證，不會再拿去試另一個。

此矩陣由 `tests/unit/test_auth_surface_conformance.py` 強制執行，它驅動每個介面真正的驗證進入點，並在加入新介面卻沒有對應資料行時失敗。

### 個人存取權杖
PAT 是一份長生命週期的 bearer 密鑰，由使用者為無法完成互動式登入的用戶端鑄造——腳本、BI 工具、驅動程式。（REQ-1263）它帶有自己的組織與角色，且每個介面都經同一個驗證器解析它，因此沒有任何介面需要知道 PAT 是什麼。

線路形式為 `provisa_pat_` 後接 43 個 url-safe base64 字元。該前綴決定了被呈交的密鑰路由到權杖儲存區而非身份提供者，也讓外洩的權杖在記錄與程式碼儲存庫中可被 grep 出來。

- **儲存**——只保留密鑰的 SHA-256。密鑰本身在建立時恰好顯示一次，之後無法復原。清單帶有顯示前綴與生命週期時間戳記，絕不是可用的憑證。
- **簽發與撤銷**——`POST /auth/tokens`、`GET /auth/tokens`、`DELETE /auth/tokens/{token_hash}`，以及管理 UI 中使用者個人檔案上的自助區段。鑄造與撤銷憑證是權杖持有人自己的行為。
- **歸屬**——通過驗證的 PAT 解析到其擁有者的帳戶：使用者 id、電郵與顯示名稱。因此以 PAT 寫下的審計資料行或使用報表指名的是那個人，不是那份憑證。是那個人的哪一個權杖動作，另行記在 `raw_claims["token_name"]` 中。
- **到期**——權杖可帶有到期時間；已到期的權杖在驗證時被拒。移除某使用者的成員資格，會連同撤銷他的各個權杖。

### pgwire 上的 SCRAM-SHA-256
在 `basic` 提供者之下，設定 `auth.scram: true` 會讓 pgwire 通告 SASL（驗證代碼 10）並帶 `SCRAM-SHA-256` 機制，因此密碼是被證明而非被送出。（REQ-1394）不提供通道繫結（`SCRAM-SHA-256-PLUS`）。

SCRAM 需要一份 RFC 5802 驗證子，而它無法由 bcrypt 雜湊推導出來。每當密碼以明文經過時，就會寫入一份驗證子——註冊、登入、更改密碼、管理員重設——因此開啟 SCRAM 的部署，會隨著使用者下次驗證而逐步收集驗證子，而每位使用者的首次 SCRAM 連線，緊接在他下一次輸入密碼之後。尚無驗證子的使用者，會得到一次與真實交握無法區分的模擬交握，因此線路不會透露誰已遷移。

### 相互 TLS
用戶端憑證驗證把第一道檢查移到 TLS 交握：沒有由該部署 CA 簽署之憑證的呼叫方，根本抵達不了憑證層。（REQ-1228）它可用於 pgwire、Bolt、gRPC 與 Arrow Flight——這四種自行終止 TLS 的傳輸。

| 變數 | 意義 |
| ---------- | --------- |
| `PROVISA_MTLS_CLIENT_CA` | 獲准簽署用戶端憑證之 CA 的 PEM 套組 |
| `PROVISA_MTLS_MODE` | `required`（設定 CA 後的預設）或 `optional` |
| `PROVISA_MTLS_BIND_PRINCIPAL` | 為 true 時，憑證的一般名稱必須等於該連線隨後所驗證的使用者名稱 |

各協定的覆寫遵循與 TLS 設定相同的命名方式。沒有任何東西是推斷出來的：設了模式卻沒有 CA 會拒絕啟動，無法辨識的模式也會拒絕啟動，而不是被讀成最安全的鄰近值——一個以為自己要求用戶端憑證、實際上並沒有的部署，處境比一個啟動失敗的部署更差。

### 登入節流
密碼猜測與協定無關：同一個帳戶可被 HTTP、pgwire 與 Bolt 一起猛敲。因此計數器活在憑證驗證層，而不在任何單一介面上，所以在任何一處掙得的鎖定，處處都被強制執行。（REQ-1393）

它預設開啟——五分鐘內五次失敗，該主體被鎖十五分鐘——並在 `auth.login_throttle` 之下調校。被鎖定的主體在憑證被檢視之前就被拒絕，而一次成功的驗證會清除該主體的歷史。

索引鍵是協定所載的主體。純 bearer 的介面不載有主體，因此索引鍵是憑證本身的摘要；它擋下的是同一份壞權杖被無限次重放。該儲存區是逐處理程序的，因此執行多個 API 工作處理程序的部署，每個工作處理程序各允許 `max_attempts` 次——節流是對猜測的剎車，不是一份分散式配額。

### 在線路協定上定址某個組織
在多租用戶之下，組織以主機名稱定址：`acme.provisa.dev` 即組織 `acme`。經 HTTP 時該名稱在 `Host` 標頭中抵達。pgwire 或 Bolt 用戶端不送這種標頭，但它會在 TLS ClientHello 中送出它所撥接的主機名稱，Provisa 就從那裡讀取組織。（REQ-1234）用戶端毫無變動——連到 `acme.provisa.dev` 就是全部所需。

主機名稱是一項請求，不是一項授予。它抵達的是 `Host` 標頭所抵達的同一個解析器，而該解析器會拒絕任何已驗證主體既非其成員、亦不持有跨組織權利的組織。撥接一個你沒有成員資格的主機名稱，抵達不了任何數據。以 IP 位址連線的用戶端不送主機名稱，僅由主體解析其組織——單一組織部署上的每一條連線都是如此。

gRPC、Arrow Flight 與 MCP 把憑證交給不公開主機名稱回呼的程式庫；那些傳輸改以 `x-provisa-org` 中繼資料標頭指名組織。

## 高安全模式
`provisa.yaml` 中的 `security.mode: high` 主張一項保證：Provisa 後端絕不處理明文數據。（REQ-693）每一個要緊的欄位都在數據來源處加密，且只有持有解密金鑰的用戶端讀得到它。這項保證帶來部署必須事先規劃的後果。

**此模式做什麼：**

- **數據端點要求用戶端解密的證明。** 除非呼叫方呈交 `X-Provisa-KMS-Key` 標頭——那是已設定為本地解密之 JDBC 或 Python 用戶端的標記——否則 `/data/` 之下的一切都回 403。瀏覽器或明文的 REST 消費方不帶這種金鑰，因而被拒。該閘門是覆蓋整棵樹的預設拒絕：明天新增的路由在出貨當日即受管束，而豁免必須被論證。
- **結構描述中繼資料端點保持開放。** `/data/sdl`、`/data/introspection`、`/data/schema-version`、`/data/domains`、`/data/proto` 與 `/data/compile` 不傳回任何行數據，而用戶端在能連線之前，就必須讀取結構描述——包括哪些欄位是 `@encrypted`。
- **gRPC 與 Arrow Flight 繼續服務，適用同一份證明。** 它們是加密用戶端實際使用的傳輸；關掉它們會讓高安全部署沒有任何線路協定可用。這兩者上的數據呼叫，都必須以呼叫中繼資料載有同一把 KMS 金鑰。
- **pgwire、Bolt 與 MCP 不啟動。** 這三者都沒有可承載解密脈絡的逐連線交握：pgwire 的行集與 Cypher 結果在線路上是明文，而 MCP 工具呼叫把結果以文字交給模型。三者中任一設定了連接埠，都會在啟動時被拒而非被服務。
- **關係防護無法被繞過。** `ignore_relationships` 與 `relationship_guard: false` 都被忽略；見 [關係治理](#relationship-governance-v002)。

**驗證某個部署處於此模式：** 啟動記錄會指名它，一個不帶 KMS 金鑰的 `/data/sql` 請求會回 403 並附一則指名 REQ-693 的訊息，而 pgwire、Bolt 與 MCP 連接埠不在監聽。

## ABAC 批准掛鉤
一個選用的外部政策掛鉤，在查詢執行之前觸發。（REQ-203）設定之後，Provisa 會帶著使用者身份、角色、表、欄位與操作，呼叫出去到你的政策引擎。回應決定該查詢是否繼續。（REQ-203）

### 範圍界定
掛鉤只在查詢觸及受範圍界定的表或數據來源時觸發——其餘一切零開銷。（REQ-204）

| 設定 | 效果 |
| -------- | -------- |
| `auth.approval_hook.scope: all` | 每個查詢都觸發掛鉤 |
| `sources[].approval_hook: true` | 該數據來源上的所有表都觸發掛鉤 |
| `tables[].approval_hook: true` | 該表觸發掛鉤 |

### 各項協定
支援三種傳輸：（REQ-246）

| 類型 | 使用情境 | 設定欄位 |
| ------ | ---------- | ------------- |
| `webhook` | 任何具備 HTTP 能力的政策服務（OPA、自訂） | `url` |
| `unix_socket` | 同一台機器上的 OPA 或政策 sidecar | `socket_path` + `url` |
| `grpc` | 同址部署的高輸送量政策服務 | `url`（host:port） |

gRPC 傳輸使用定義於 `provisa/auth/approval.proto` 的 `provisa.auth.ApprovalService` 合約。請在你的政策引擎中實作此服務：（REQ-246）

```proto
service ApprovalService {
  rpc Evaluate (ApprovalRequest) returns (ApprovalResponse);
}

message ApprovalRequest {
  string user = 1;
  repeated string roles = 2;
  repeated string tables = 3;
  repeated string columns = 4;
  string operation = 5;
}

message ApprovalResponse {
  bool approved = 1;
  string reason = 2;
}
```

gRPC 通道是持續性的——每個 Provisa 執行個體一條通道，對該掛鉤端點的所有呼叫共用。（REQ-555）

### 請求／回應
三種傳輸都載有相同的負載：（REQ-246）

| 欄位 | 類型 | 說明 |
| ------- | ------ | ------------- |
| `user` | string | 已驗證的使用者身份 |
| `roles` | string[] | 使用者的 Provisa 角色 |
| `tables` | string[] | 查詢中引用的表 ID |
| `columns` | string[] | 查詢中選取的欄位 |
| `operation` | string | `"query"` 或 `"mutation"` |

webhook 與 Unix socket 傳輸交換 JSON。回應必須包含 `approved`（bool），並可選填 `reason`（string）。（REQ-246）

### 逾時與退路
```yaml
auth:
  approval_hook:
    type: grpc          # webhook | grpc | unix_socket
    url: "localhost:50051"
    timeout_ms: 500     # default 5000
    fallback: deny      # allow | deny — applied on timeout or error
    scope: ""           # "" = use per-table/per-source flags; "all" = every query
```

逾時或傳輸錯誤時，套用 `fallback` 政策。（REQ-247）斷路器（預設：連續 5 次失敗後開啟，30 秒後半開）可防止緩慢的掛鉤端點造成連鎖故障。（REQ-556）

### 設定範例
```yaml
auth:
  approval_hook:
    type: webhook
    url: "http://opa.internal:8181/v1/data/provisa/allow"
    timeout_ms: 300
    fallback: deny

sources:
  - id: analytics_pg
    approval_hook: true   # all tables on this source require hook approval

tables:
  - id: salary_data
    approval_hook: true   # this table always requires hook approval
```

## 密鑰
憑證使用 `${env:VAR_NAME}` 語法，於執行階段解析。（REQ-557）密碼絕不儲存在設定資料庫中。（REQ-557）

完整的密鑰服務——保管庫、參照語法與各提供者——見 [密鑰](secrets.md)。
