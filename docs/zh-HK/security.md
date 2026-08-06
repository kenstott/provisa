# 安全模型

Provisa在所有查詢語言（GraphQL、SQL、Cypher）及所有傳輸方式（REST、gRPC、Arrow Flight、JDBC、WebSocket）上，均實施多層次的安全模型。（REQ-001、REQ-266）治理措施會統一應用——不存在任何可繞過治理的查詢路徑。（REQ-002、REQ-266）

各層按順序應用。每個請求必須先通過每一層，才會評估下一層。

## 分層模型

### 第0層——內省過濾

呈現給某個角色的結構描述 (Schema) 及目錄，只包含其`domain_access`清單中的資料表，以及通過逐欄`visible_to`規則的欄位。（REQ-039）角色權限以外的物件，在發現階段即屬不可見——無法查詢、無法自動完成，亦無法推斷其存在。（REQ-039）此規則適用於GraphQL結構描述、SQL目錄，以及查詢編輯器的結構描述瀏覽器。（REQ-039、REQ-363）

參閱[結構描述可見性](#_10)。

### 第1層——公開存取

沒有`domain_access`限制的網域中的資料表，無需額外設定即可供所有已通過驗證的身份查看。對於真正公開的數據，完全沒有障礙。

### 第2層——網域存取

每個角色都有一份`domain_access`網域ID清單。凡涉及該等網域以外資料表的查詢，均會在執行前遭拒絕。（REQ-038、REQ-039）這是粗粒度的擁有權邊界——無論SQL如何撰寫，人力資源角色都無法存取財務資料表。（REQ-002）

參閱[權限模型](#_3)。

### 第3層——行級安全

網域存取獲確認後，系統會在執行時，將按資料表、按角色設定的`WHERE`謂詞注入每個`SELECT`陳述式中。（REQ-041、REQ-263）該等謂詞是針對原始數據進行評估的。即使使用`SELECT *`，查詢共用訂單資料表的區域經理也只會看到其所屬區域的資料列。（REQ-264）

參閱[行級安全 (RLS)](#rls)。

### 第4層——欄位可見性及遮罩

`visible_to`清單中不包括請求角色的欄位，會從查詢結果中移除。（REQ-040、REQ-263）設有遮罩規則的欄位，其值會在結果離開伺服器前遭取代——方式包括正規表達式編修、常數取代或截斷。（REQ-263）遮罩適用於所有查詢語言及輸出格式。（REQ-263）

參閱[欄位權限模型](#_5)及[欄位層級遮罩](#_11)。

### 第5層——謂詞防護

遭遮罩的欄位會在`WHERE`及`HAVING`子句中被拒絕使用。（REQ-263）若無此防護，即使輸出結果已遮罩，呼叫方仍可透過在篩選條件中進行二分搜尋，推斷出未遮罩的值。此項拒絕會在查詢剖析階段（執行前）強制執行。（REQ-531）

### 關係治理（V002）

SQL中的JOIN條件，必須符合資料表之間已登記並獲批核的關係。（REQ-001）未經批核的join會遭拒絕。每個關係均附有人類可讀的原因及描述——為使用者及自主代理提供指引，說明某遍歷路徑存在的原因。此屬治理政策，而非硬性的安全邊界：無論join結構如何，第2至5層依然有效，因此刻意的規避行為，並不會使角色接觸到其原本無法透過兩個獨立查詢取得的數據。規避的嘗試會被記錄並可供審計。

**繞過機制**——V002 有兩種繞過方式。第一種是一項功能：持有 `ignore_relationships` 的角色可以跨目錄未涵蓋的關係進行聯接。在預置的系統角色中只有 `modeler` 持有它——這是負責確定模型而非強制執行模型的探索角色。（REQ-1297）`analyst` 並不持有。[tool-verified: `provisa/core/db.py:84`]

第二種是需同時成立兩項條件的退出機制：

1. **角色標記**——角色定義中的`relationship_guard: false`（預設值：`true`）。[tool-verified: `provisa/core/models.py:349`]
2. **按查詢退出**——SQL中包含`--relationship-guard=false`註解。[tool-verified: `provisa/compiler/params.py:80`]

單靠角色標記並不能繞過V002；單靠註解亦不能繞過V002。

**高安全模式將該防護固定。**在 `security.mode: high` 之下兩種繞過均不適用：`ignore_relationships` 被忽略，`relationship_guard: false` 被忽略，而每個聯接都必須存在於已批核的關係目錄之中。（REQ-693）這是刻意的冗餘——即使某個生產角色被誤授予該功能，它仍然無法突破模型。[tool-verified: `provisa/pgwire/_pipeline.py:377`]

**GraphQL路徑**——對於GraphQL查詢，V002一律會被略過。SDL中定義的關係，按設計已預先獲批核；此項檢查屬多餘，故不會執行。[tool-verified: `provisa/api/data/endpoint.py:468`]

**SQL及Cypher路徑**——V002預設為啟用狀態。`endpoint_dev.py`及`cypher_router.py`均會在呼叫`validate_sql`前，執行兩項條件的檢查。[tool-verified: `provisa/api/data/endpoint_dev.py:127`、`provisa/api/rest/cypher_router.py:260`]

**pgwire路徑**——與SQL相同的兩項條件檢查。`--relationship-guard=false`註解會在執行前從查詢中移除；不會傳送至資料庫。[tool-verified: `provisa/pgwire/_pipeline.py:60`]

---

這些層次會相互組合。同時具備網域存取、RLS及遮罩欄位的角色，其五項限制會同時生效。新增數據來源、欄位或關係，無需逐一更新所有規則——每一層均獨立設定，並會自動套用於任何涉及受治理物件的查詢。

---

## 權限模型

各項能力獨立指派，並可透過`parent_role_id`實現可選的角色階層。`admin`授予全部能力。（REQ-042）

| 能力 | 說明 |
| ----------- | ------------- |
| `source_registration` | 登記數據來源 |
| `table_registration` | 登記資料表、欄位 |
| `create_relationship` | 定義外部索引鍵關係 |
| `access_config` | 設定RLS、遮罩 |
| `query_development` | 執行查詢 |
| `write` | 呼叫已登記的變更操作（粗粒度控制；參閱「變更操作授權」） |
| `full_results` | 繞過取樣限制 |
| `ignore_relationships` | 繞過關係治理（V002）。在系統角色之中僅由 `modeler` 持有，且在高安全模式下被完全忽略 |
| `admin` | 超級使用者——授予全部能力 |

### 角色繼承

角色可透過`parent_role_id`，從父角色繼承能力及網域存取權。（REQ-215）階層會在啟動時扁平化——子角色會將父角色的能力及網域存取權，與自身的合併。（REQ-215）

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

## 欄位權限模型

每個欄位均設有四個欄位所組成的權限模型，用以控制各角色的讀取、寫入及遮罩存取權。（REQ-042、REQ-249）

### 三級可見性

| 級別 | 條件 | 結果 |
| ------ | ----------- | -------- |
| **隱藏** | 角色不在`visible_to`中 | 欄位不會出現在GraphQL SDL中 |
| **已遮罩** | 角色在`visible_to`中、設有遮罩規則、角色不在`unmasked_to`中 | 欄位可見，但SQL中數據已遮罩 |
| **未遮罩** | 角色同時在`visible_to`及`unmasked_to`中（或沒有遮罩規則） | 完整讀取存取權 |

### 寫入權限

| 欄位 | 空白代表 | 用途 |
| ------- | ------------ | --------- |
| `visible_to` | 所有角色均可讀取 | 控制誰可看到該欄位（已遮罩或未遮罩） |
| `unmasked_to` | 沒有角色可看到未遮罩的值 | 控制誰可繞過遮罩 |
| `writable_by` | 沒有角色可寫入 | 控制誰可作出變更 (INSERT/UPDATE) |

寫入權限會在變更操作管線中強制執行。不在`writable_by`中的角色，嘗試寫入受限欄位時會收到403錯誤。（REQ-033、REQ-034）

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

在此範例中：

- `email`：admin可看到`alice@example.com`並可編輯；analyst/viewer則會看到`a***@example.com`
- `salary`：admin及hr可看到真實值；hr可編輯；其餘所有角色完全看不到此欄位
- `created_at`：所有人均可讀取，任何人都不可寫入

## 變更操作授權

已登記的變更操作（遠端GraphQL、OpenAPI、gRPC、Hasura）須經過兩項獨立檢查。（REQ-867、REQ-868）角色只有在同時具備全域`write`能力，並列於該項變更操作的`writable_by`清單中，方可呼叫該操作。（REQ-868）空白的`writable_by`即代表預設拒絕——任何角色均不可呼叫。（REQ-867）

變更操作按合約分類為寫入操作，而非按呼叫方的聲明而定。（REQ-869）若`SELECT`陳述式引用了屬變更操作類型的函式，會被提升為寫入操作，並須經過相同的兩重把關檢查，因此呼叫方無法透過將變更操作偽裝為讀取操作而繞過限制。（REQ-869）將某項變更操作重新分類為讀取安全，須具備`access_config`能力，並會被記錄為治理決定；並無按請求逐次退出的選項。（REQ-870）

## 結構描述可見性

按角色劃分的GraphQL結構描述，會隱藏未經授權的內容：（REQ-039）

- **網域存取**：角色只會看到其`domain_access`網域內的資料表（`"*"` = 全部）（REQ-039）
- **欄位可見性**：對某角色而言不在`visible_to`中的欄位，會從SDL中省略（REQ-039）
- 未經授權的資料表／欄位，不會出現在結構描述中（REQ-039）

## 行級安全 (RLS)

按資料表、按角色注入SQL WHERE子句。此項操作於編譯後、執行前進行。（REQ-041、REQ-263）

```yaml
rls_rules:
  - table_id: orders
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"
```

篩選條件會以AND方式併入查詢的WHERE子句中。此機制同時適用於查詢及變更操作 (UPDATE/DELETE)。（REQ-035、REQ-041）

## 欄位層級遮罩

遮罩設定只需為每個欄位定義一次——這是欄位本身的屬性，而非角色的屬性。`unmasked_to`欄位控制哪些角色可繞過遮罩。（REQ-249）

| 遮罩類型 | 支援的類型 | SQL運算式 |
| ----------- | ---------------- | ---------------- |
| `regex` | 字串 (varchar, char, text) | `REGEXP_REPLACE(col, pattern, replace)` |
| `constant` | 任何類型 | 常值 (NULL、0、自訂) |
| `truncate` | 日期／時間戳記 | `DATE_TRUNC(precision, col)` |

遮罩會被下推至SQL SELECT投影中——由資料庫直接傳回已遮罩的數據。（REQ-263）對於遭遮罩的角色而言，未遮罩的數據絕不會經網絡傳輸。（REQ-263）遭遮罩的欄位亦會在`WHERE`及`HAVING`子句中遭封鎖（第5層謂詞防護），以防止透過篩選推斷出未遮罩的值。（REQ-263、REQ-531）

## 取樣

除非具備`full_results`能力，否則所有角色看到的均為經取樣的結果（預設：100列）。（REQ-554）可透過`PROVISA_SAMPLE_SIZE`環境變數控制。（REQ-554）

## 審計記錄

任何涉及網域資產的查詢，均會記錄於只可新增的`query_audit_log`中。（REQ-596、REQ-613）每列會擷取`tenant_id`、`user_id`、`role_id`、查詢文字的SHA-256雜湊值、`table_ids`、`source`、`status_code`、`duration_ms`及`logged_at`。（REQ-596）查詢文字絕不會以原文儲存——只會儲存其雜湊值。（REQ-596）

該記錄在資料庫層面屬只可新增：PostgreSQL規則會封鎖`DELETE`及`UPDATE`。（REQ-596、REQ-613）兩個索引——`(tenant_id, logged_at)`及`(user_id, logged_at)`——支援按租用戶範圍及按使用者的時間範圍合規查詢。（REQ-596、REQ-613）

啟用加密後，查詢文字雜湊值一欄會以加密方式儲存，並只會在獲授權的管理員讀取時解密。（REQ-689）

## 速率限制

按角色設定的速率限制，會於`provisa.yaml`中設定：包括每秒最大請求數、最大同時進行的SSE訂閱數，以及最大同時進行的Arrow Flight串流數。（REQ-369）該等限制會在編譯或執行之前，於API層強制執行；超出限制的請求會遭拒絕，並回傳HTTP 429及`Retry-After`標頭。（REQ-369）

自然語言查詢服務（`POST /query/nl`）另設有獨立限制，透過`nl.rate_limit`（每分鐘、每角色的請求數）控制。超出限制的請求會在呼叫任何LLM之前遭拒絕。（REQ-370）

速率限制的狀態儲存於Redis（`cache.redis_url`）中，以滑動視窗計數器方式運作——並無按執行個體儲存的狀態——因此限制會在所有水平擴展的Provisa執行個體之間保持一致。（REQ-371）

## 身份驗證

可插拔的身份驗證提供者：（REQ-120）

| 提供者 | 權杖類型 | 使用案例 |
| ---------- | ----------- | ---------- |
| `none` | X-Provisa-Role標頭 | 開發 |
| `basic` | bcrypt 本機帳戶 + JWT | 自給自足的部署 |
| `firebase` | Firebase ID權杖 | 生產環境 |
| `keycloak` | Keycloak JWT | 企業版 |
| `oauth` | OIDC JWT | PingFed、Okta、Azure AD、Auth0 |
| `simple` | bcrypt + JWT | 測試 |

角色對應：透過可設定的規則，將身份聲明對應至Provisa角色。（REQ-120）`assignments_source`欄位控制角色指派的來源：`claims`會從JWT權杖的聲明中讀取（預設值）；`provisa`則會從Provisa內部的指派儲存區中讀取。（REQ-551）

於`provisa.yaml`中設定的超級使用者（使用者名稱連同來自環境密鑰的密碼），無論設定何種提供者，均一律獲授予admin角色及全部能力——此為初始設定所用的啟動路徑。（REQ-125）

### 介面與憑證

每個介面都經同一套提供者契約進行認證，因此在一個介面上可用的憑證，只要協定能夠承載，就在所有介面上可用。（REQ-124、REQ-1263）本表為唯一參考；各介面文件不再重複。

| 介面 | 密碼 | 提供者權杖 | 個人存取權杖 | 用戶端憑證（mTLS） |
| --------- | ---------- | ---------------- | ----------------------- | --------------------------- |
| HTTP（REST、JSON:API、GraphQL） | `Authorization: Basic` | `Authorization: Bearer` | `Authorization: Bearer` | 經由終止代理 |
| pgwire | 密碼欄位（明文或 SCRAM） | 密碼欄位，OIDC 部署 | 密碼欄位 | 是 |
| Bolt | `basic` 方案 | `bearer` 方案 | `bearer` 方案 | 是 |
| Arrow Flight | — | 交握或票證酬載中的 `token` | 同上 | 是 |
| gRPC | — | `authorization` 中繼資料 | `authorization` 中繼資料 | 是 |
| MCP | — | `Authorization: Bearer` | `Authorization: Bearer` | 經由終止代理 |

儲存格為 `—` 之處，表示該協定沒有可與密碼配對的使用者名稱欄位；這些情形由權杖形式覆蓋。pgwire 則是鏡像情形：啟動封包只有一個密鑰欄位而沒有方案，因此密鑰*是甚麼*決定了採用哪種方法——PAT 由其前綴識別，當所設定的提供者為權杖提供者時該密鑰按 bearer 權杖讀取，其餘一律視為密碼。選擇只做一次——被選定驗證器拒絕的憑證不會再拿去試另一個。

該矩陣由 `tests/unit/test_auth_surface_conformance.py` 強制執行，它驅動每個介面真實的驗證入口，並在新增介面而未加入對應列時失敗。

### 個人存取權杖

PAT 是使用者為無法完成互動式登入的用戶端——指令稿、BI 工具、驅動程式——鑄造的長期 bearer 密鑰。（REQ-1263）它自帶組織與角色，而每個介面都經同一個驗證器解析它，因此任何介面都毋須知道 PAT 為何物。

其傳輸形式為 `provisa_pat_` 後接 43 個 URL 安全的 base64 字元。正是這個前綴把呈交的密鑰導向權杖儲存而非身分提供者，也使外洩的權杖可在紀錄與程式碼庫中被 grep 檢出。

- **儲存**——只保留密鑰的 SHA-256。密鑰本身僅在建立時顯示一次，且無法取回。清單中帶有顯示前綴與生命週期時間戳記，絕不會是可用憑證。
- **簽發與撤銷**——`POST /auth/tokens`、`GET /auth/tokens`、`DELETE /auth/tokens/{token_hash}`，以及管理介面中使用者自身個人檔案頁上的自助區域。鑄造與撤銷憑證是權杖持有者本人的行為。
- **歸屬**——通過驗證的 PAT 解析為其擁有者的帳戶：使用者 id、電郵與顯示名稱。因此在 PAT 之下寫入的稽核列或使用報表指向的是人，而非憑證。該人的哪一個權杖參與了操作則另行記錄於 `raw_claims["token_name"]`。
- **逾期**——權杖可帶有到期時間；已逾期的權杖在驗證時遭拒。刪除使用者的成員資格會連同撤銷其權杖。

### pgwire 上的 SCRAM-SHA-256

在 `basic` 提供者之下，設定 `auth.scram: true` 會令 pgwire 通告 SASL（認證碼 10）並使用 `SCRAM-SHA-256` 機制，從而以證明密碼取代傳送密碼。（REQ-1394）不提供通道繫結（`SCRAM-SHA-256-PLUS`）。

SCRAM 需要一個 RFC 5802 驗證器，而它無法從 bcrypt 雜湊推導得出。只要密碼以明文經過——註冊、登入、更改密碼、管理員重設——就會寫入一個驗證器，因此開啟 SCRAM 的部署會隨著使用者下一次認證逐步收集驗證器，而每位使用者的首次 SCRAM 連線緊接在其下一次輸入密碼之後。對尚無驗證器的使用者，會以與真實交換無法區分的模擬交換作答，因此線路上不會洩露誰已完成遷移。

### 雙向 TLS

用戶端憑證驗證把第一道檢查移到 TLS 交握：沒有部署方 CA 簽署憑證的呼叫方永遠到不了憑證層。（REQ-1228）它可用於 pgwire、Bolt、gRPC 與 Arrow Flight——這四種自行終止 TLS 的傳輸。

| 變數 | 含義 |
| ---------- | --------- |
| `PROVISA_MTLS_CLIENT_CA` | 允許簽發用戶端憑證的 CA 的 PEM 套件 |
| `PROVISA_MTLS_MODE` | `required`（設定 CA 後的預設值）或 `optional` |
| `PROVISA_MTLS_BIND_PRINCIPAL` | 為真時，憑證的 common name 必須與該連線隨後認證所用的使用者名稱相同 |

各協定的覆寫設定沿用與 TLS 設定相同的命名。沒有任何東西靠推斷：設定了模式卻未設定 CA 會拒絕啟動，無法辨識的模式亦會拒絕啟動，而不會被讀作最接近的安全取值——一個自以為要求用戶端憑證而實際並無要求的部署，處境比啟動失敗的部署更差。

### 登入節流

猜測密碼與協定無關：同一個帳戶可以經由 HTTP、pgwire 與 Bolt 被反覆轟炸。因此計數器位於憑證驗證層，而非任何單一介面，這樣在任何地方觸發的鎖定都會處處生效。（REQ-1393）

它預設開啟——五分鐘內五次失敗會將該主體鎖定十五分鐘——並在 `auth.login_throttle` 之下調整。被鎖定的主體在憑證被檢查之前就已遭拒，而一次成功認證會清空該主體的歷史。

鍵是協定所攜帶的 principal。僅支援 bearer 的介面不攜帶 principal，因此鍵是憑證自身的摘要；這樣阻止的是同一個壞權杖被無限重放。該儲存按行程劃分，因此執行多個 API worker 的部署每個 worker 最多允許 `max_attempts` 次——節流是對猜測的煞車，不是分散式配額。

### 在傳輸協定上指定組織

在多租戶之下，組織經主機名稱定址：`acme.provisa.dev` 即組織 `acme`。在 HTTP 上該名稱隨 `Host` 標頭抵達。pgwire 或 Bolt 用戶端不傳送此類標頭，但它確實會在 TLS ClientHello 中傳送所撥的主機名稱，Provisa 便從中讀取組織。（REQ-1234）用戶端毋須任何改動——連線至 `acme.provisa.dev` 即可。

主機名稱是一項請求，而非授予。它抵達的是與 `Host` 標頭相同的解析器，該解析器會拒絕任何認證 principal 既非成員、亦不持有跨組織權限的組織。撥向你並無成員資格的主機名稱，觸及不到任何資料。以 IP 位址連線的用戶端不傳送主機名稱，只從 principal 解析其組織——在單組織部署中，每條連線都是如此。

gRPC、Arrow Flight 與 MCP 把憑證交給不外露主機名稱回呼的程式庫；這些傳輸改用 `x-provisa-org` 中繼資料標頭來指定組織。

## 高安全模式

`provisa.yaml` 中的 `security.mode: high` 主張一項保證：Provisa 後端絕不處理明文資料。（REQ-693）每個重要的欄都在來源端加密，只有持有解密密鑰的用戶端才能讀取。這項保證帶來的後果，部署方必須提前規劃。

**該模式的作用：**

- **資料端點要求出示用戶端解密的憑證。**`/data/` 之下的一切都回傳 403，除非呼叫方帶上 `X-Provisa-KMS-Key` 標頭——這是設定為本機解密的 JDBC 或 Python 用戶端的標記。瀏覽器或明文 REST 消費方不帶此類密鑰，會遭拒絕。該關卡是對整棵樹的預設拒絕：明天新增的路由在其發佈當日即受管控，豁免則必須逐一論證。
- **模式中繼資料端點保持開放。**`/data/sdl`、`/data/introspection`、`/data/schema-version`、`/data/domains`、`/data/proto` 與 `/data/compile` 不回傳列資料，而用戶端在能夠連線之前必須先讀取模式——包括哪些欄位帶 `@encrypted`。
- **gRPC 與 Arrow Flight 在同一憑證要求之下繼續服務。**它們正是執行加密的用戶端實際使用的傳輸；關閉它們會令高安全部署失去所有傳輸協定。在其中任一上的資料呼叫都必須以呼叫中繼資料攜帶同樣的 KMS 密鑰。
- **pgwire、Bolt 與 MCP 不會啟動。**三者都沒有能夠承載解密脈絡的逐連線交握：pgwire 列集與 Cypher 結果在線路上都是明文，而 MCP 工具呼叫會把結果以文字交給模型。為其中任一設定的連接埠在啟動時會遭拒絕而非提供服務。
- **關係防護無法繞過。**`ignore_relationships` 與 `relationship_guard: false` 均被忽略；參見[關係治理](#relationship-governance-v002)。

**如何確認部署處於該模式：**啟動紀錄會指明它；不帶 KMS 密鑰的 `/data/sql` 請求會以 403 回應並給出提及 REQ-693 的訊息；pgwire、Bolt 與 MCP 連接埠未在監聽。

## ABAC批核掛勾 (Hook)

可選的外部政策掛勾，會在查詢執行前觸發。（REQ-203）當設定此項功能後，Provisa會呼叫您的政策引擎，並傳送使用者身份、角色、資料表、欄位及操作類型。回應結果會決定該查詢是否繼續執行。（REQ-203）

### 適用範圍

只有當查詢涉及設定了範圍的資料表或來源時，該掛勾才會觸發——其餘情況則完全沒有額外負擔。（REQ-204）

| 設定 | 效果 |
| -------- | -------- |
| `auth.approval_hook.scope: all` | 每個查詢均會觸發此掛勾 |
| `sources[].approval_hook: true` | 該來源上的所有資料表均會觸發此掛勾 |
| `tables[].approval_hook: true` | 該資料表會觸發此掛勾 |

### 協定

支援三種傳輸方式：（REQ-246）

| 類型 | 使用案例 | 設定欄位 |
| ------ | ---------- | ------------- |
| `webhook` | 任何支援HTTP的政策服務（OPA、自訂） | `url` |
| `unix_socket` | 位於同一部機器上的OPA或政策旁路容器 (sidecar) | `socket_path` + `url` |
| `grpc` | 同址部署、高吞吐量的政策服務 | `url` (host:port) |

gRPC傳輸方式採用`provisa/auth/approval.proto`中定義的`provisa.auth.ApprovalService`合約。請在您的政策引擎中實作此服務：（REQ-246）

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

gRPC通道屬持續性——每個Provisa執行個體使用一條通道，並會在所有對該掛勾端點的呼叫中重複使用。（REQ-555）

### 請求／回應

三種傳輸方式均承載相同的酬載：（REQ-246）

| 欄位 | 類型 | 說明 |
| ------- | ------ | ------------- |
| `user` | string | 已通過驗證的使用者身份 |
| `roles` | string[] | 使用者的Provisa角色 |
| `tables` | string[] | 查詢中引用的資料表ID |
| `columns` | string[] | 查詢中選取的欄位 |
| `operation` | string | `"query"`或`"mutation"` |

webhook及Unix socket傳輸方式均以JSON交換資料。回應必須包含`approved`（布林值），並可選擇性包含`reason`（字串）。（REQ-246）

### 逾時及後備處理

```yaml
auth:
  approval_hook:
    type: grpc          # webhook | grpc | unix_socket
    url: "localhost:50051"
    timeout_ms: 500     # default 5000
    fallback: deny      # allow | deny — applied on timeout or error
    scope: ""           # "" = use per-table/per-source flags; "all" = every query
```

發生逾時或傳輸錯誤時，會套用`fallback`政策。（REQ-247）斷路器 (circuit breaker)（預設：連續失敗5次後開啟，30秒後轉為半開狀態）可防止因掛勾端點反應緩慢而引致的連鎖故障。（REQ-556）

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

憑證使用`${env:VAR_NAME}`語法，並於執行階段解析。（REQ-557）密碼絕不會儲存於設定資料庫中。（REQ-557）
