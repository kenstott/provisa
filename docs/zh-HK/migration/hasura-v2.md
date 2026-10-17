# 從 Hasura v2 遷移至 Provisa {: #migrating-from-hasura-v2-to-provisa }

## 前提條件 {: #prerequisites }

1. 一個正在運行的 Hasura v2 實例（v2.x），並已匯出中繼資料。
2. 以兩種佈局之一匯出中繼資料：

   **CLI 的中繼資料目錄：**

   ```bash
   hasura metadata export --endpoint http://localhost:8080
   ```

   這會建立一個 `metadata/` 目錄。`databases/databases.yaml` 列出每個數據來源的類型
   和連線，並透過 `!include` 索引指向其資料表和函式；
   `actions.yaml`、`cron_triggers.yaml`、`inherited_roles.yaml`、`remote_schemas.yaml` 以及其餘
   檔案位於其旁。較早的扁平佈局（根目錄下的 `tables.yaml`）
   同樣可以轉換。

   **中繼資料 API 的單一文件：**

   ```bash
   curl -X POST http://localhost:8080/v1/metadata \
     -H "X-Hasura-Admin-Secret: <secret>" \
     -d '{"type":"export_metadata","args":{}}' > metadata.json
   ```

   裸物件和 `{resource_version, metadata}` 信封都可使用。管理端匯入
   （Admin、Import Hasura Config）使用此形式；下面的命令列轉換器
   讀取目錄。
3. 已安裝 `provisa` 套件的 Python 3.11+。

## CLI 用法 {: #cli-usage }

```bash
python -m provisa.hasura_v2 <metadata-dir> -o provisa.yaml
```

### 引數 {: #arguments }

| 引數 | 必要 | 說明 |
| ---------- | ---------- | ------------- |
| `metadata_dir` | 是 | 已匯出的 Hasura v2 中繼資料目錄路徑 |

### 選項 {: #options }

| 選項 | 預設值 | 說明 |
| -------- | --------- | ------------- |
| `-o, --output FILE` | stdout | 輸出 YAML 檔案的路徑 |
| `--source-overrides FILE` | 無 | 包含每個數據來源連線覆寫的 YAML 檔案 |
| `--domain-map KEY=VAL ...` | 無 | 結構描述 (Schema) 到網域的對應（例如 `public=core hr=people`） |
| `--auth-env-file FILE` | 無 | 包含 JWT／admin-secret 驗證設定的 `.env` 檔案路徑 |
| `--dry-run` | 關閉 | 僅解析並驗證，不寫入輸出 |

### 數據來源覆寫檔案 {: #source-overrides-file }

一個以數據來源名稱作為索引鍵的 YAML 檔案，包含要覆寫的連線屬性：

```yaml
default:
  host: prod-db.example.com
  port: 5432
  database: myapp
  username: provisa_user
  password: "${env:PROD_DB_PASSWORD}"
```

### 驗證環境檔案 {: #auth-env-file }

一個 `.env` 格式的檔案，內含要轉換的 Hasura 驗證設定。轉換工具會進行以下對應：

- 帶有 `jwk_url` 的 JWT -> Provisa `provider: oauth`。
- JWT 的 `claims_map` -> Provisa `role_mapping[]`。
- Admin secret -> Provisa `superuser`。
- Webhook 驗證 -> 系統會發出警告（Provisa 沒有對應項目）。

## 報告 {: #the-report }

轉換以一份分為三部分的報告結束：

1. **原樣帶過來。** 物件在 Provisa 中有對應物，轉換後含義不變。
2. **對應到更好的 Provisa 功能。** 每一項都會說明為什麼 Provisa 功能做得更好。
3. **需手動重做。** 每一項都會說明該怎麼做。這些就是警告。

命令列轉換器在仍有需重做的內容時，把報告輸出到 stderr，標題為 `Carried as`、`Mapped to` 和 `Redo by hand`，各帶一個計數。管理端匯入隨預覽返回這三個清單。

## 功能對等表 {: #feature-parity-matrix }

| Hasura v2 功能 | 部分 | Provisa 對應項 | 說明 |
| --- | --- | --- | --- |
| **數據來源**（postgres、mysql、mssql、bigquery、citus） | 原樣帶過來 | `sources[]` | 類型已對應：pg/postgres -> postgresql，mssql -> sqlserver。明文寫出的連線 URL 解析為 host/port/database/username/password。連線池大小保留。從環境變數讀取的 URL 不會被猜測（見下文）。 |
| **資料表**（已追蹤的資料表） | 原樣帶過來 | `tables[]` | 保留 schema 和資料表名。`source_id` 關聯到數據來源。 |
| **自訂資料表名**（`custom_name`、`custom_root_fields.select`） | 原樣帶過來 | `tables[].alias` | 取 `select`、`select_by_pk`、`custom_name` 中第一個非空值。 |
| **自訂欄位名**（`column_config`，較舊的 `custom_column_names`） | 原樣帶過來 | `columns[].alias` | 每個欄位的自訂名稱成為它的別名。 |
| **其他根欄位名**（`select_by_pk`、`select_aggregate`、`insert` 等） | 對應 | 資料表的唯一欄位 | 按名稱報告。按鍵讀取是對資料表的篩選，聚合是它的 `_aggregate` 欄位，寫入是它的變更。 |
| **select 權限**（欄位、篩選器） | 原樣帶過來 | `columns[].visible_to[]`、`rls_rules[]` | 欄位清單成為 `visible_to`；支援 `*`。篩選器轉為 SQL；工作階段變數轉為 `current_setting('provisa.<name>')`。 |
| **穿過 relationship 的篩選器** | 原樣帶過來 | `rls_rules[]` 中的 `EXISTS` 子查詢 | 規則沿 relationship 到達它的資料表。 |
| **insert、update 和 delete 權限** | 原樣帶過來 | `tables[].write_grants[]` | 每個角色和操作一條授權：欄位來自 `columns`，`rows` 來自 `filter`，`check` 來自 `check`，預設來自 `set`。角色獲得 `write` 權利。 |
| **admin 角色** | 原樣帶過來 | `org_admin` | 持有涵蓋所有欄位和行的每項授權，並在每個匯入的欄位上被指名。 |
| **Object relationship** | 原樣帶過來 | `relationships[]`，`cardinality: many-to-one` | 已批准。GraphQL 欄位是 Hasura relationship 的名稱。 |
| **Array relationship** | 原樣帶過來 | `relationships[]`，`cardinality: one-to-many` | 已批准。GraphQL 欄位是 Hasura relationship 的名稱。 |
| **繼承角色**（一個角色） | 原樣帶過來 | `roles[].parent_role_id` | 該角色成為父角色。 |
| **繼承角色**（多個角色） | 對應 | 持有它們授權聯集的角色 | Provisa 角色只有一個父角色，因此聯集被明確寫在該角色上。 |
| **Computed field** | 重做 | 檢視欄位 | 匯出指明函式，但不含其函式本體。報告給出需要新增的 `SELECT`。 |
| **已追蹤的函式** | 原樣帶過來，否則重做 | `functions[]` 指令 | 預覽從資料庫讀取簽章。讀不到時，在 Commands 頁面新增該函式。 |
| **Action** | 對應 | 呼叫處理程式的指令 | 處理程式 URL 中的 `{{VAR}}` 寫作 `${env:VAR}`，在指令被呼叫時解析。 |
| **Cron 觸發器** | 原樣帶過來 | `scheduled_triggers[]` | cron 表達式、URL、payload 和 enabled 旗標。重試不會被帶過來（警告）。 |
| **Event 觸發器** | 對應 | 針對資料表變更的 `scheduled_triggers[]` | 任何變更時每批變更觸發一次。重試不會被帶過來（警告）。 |
| **Remote schema** | 對應 | 落地為資料表的 `sources[]`（`graphql_remote`） | 保留名稱、URL、標頭和身分驗證；根欄位成為受治理的資料表。 |
| **列舉資料表** | 原樣帶過來 | 已建立資料表 | 不帶過 `is_enum` 旗標（Provisa 中無對應項）。 |
| **Allow list、query collection、REST 端點** | 重做 | 無 | 出現在合併匯出中時會被報告；不會轉換。 |

### Relationship {: #relationships }

- 每個 relationship 的 GraphQL 欄位是 Hasura 名稱。資料表上已有該名稱的欄位時，relationship 採用 Provisa 給它的名稱，報告會發出警告。
- 宣告同一個 join 的兩個 relationship 合併為一個。報告會說明哪個併入了哪個。
- 在多個欄位對上連接的 relationship 會被警告且不會建立，因為 Provisa 關係只在一個欄位對上連接。穿過它的行規則仍會轉換。
- 僅由外鍵欄位宣告的 object relationship 透過反向的 array relationship 解析；沒有時會被警告。

### 寫入授權 {: #write-grants }

一條授權聲明某個角色透過一種操作可以向某張資料表寫入什麼。空的 `filter` 或 `check` 表示所有行（`TRUE`）。`set` 值 `X-Hasura-User-Id` 成為 `user_id` 的 `session` 預設；其他任何值成為 `literal` 預設。預設欄位不會同時出現在授權的欄位中。

### Action、已追蹤的函式和觸發器 {: #actions-tracked-functions-and-triggers }

- **Action** 成為呼叫其處理程式的指令，對其權限所指名的角色可見（未指名任何角色時只對 `org_admin` 可見）。非同步 action 會被警告：指令等待處理程式的回應。
- 當預覽能從 PostgreSQL 數據來源讀取**已追蹤的函式**的參數和返回資料表時，它成為指令。離線時，以及對於缺失、被多載或返回匯入未註冊的資料表的任何函式，它轉入重做。
- **Event 觸發器** 在其資料表的每次變更時觸發，每批一次，並發送資料表和已變更的行，而不是每行一個帶舊值和新值的事件。監聽少於全部三種操作的觸發器會被警告。

## 轉換後步驟 {: #post-conversion-steps }

1. **審閱報告。** 從 `Redo by hand` 開始。
2. **設定數據來源連線。** 匯出明文寫出的連線按原樣轉換。從環境變數讀取的連線以空值到達。使用 `--source-overrides`，或直接編輯輸出。
3. **核對網域分配。** 沒有 `--domain-map` 時，所有資料表都落在 `default`。
   使用 `--domain-map public=core analytics=reporting` 將 schema 分配到網域。
4. **檢查行規則。** 篩選器會轉換為 SQL。請審閱複雜的布林
   表達式（巢狀的 `_and`/`_or`/`_exists`）以及穿過 relationship 的篩選器。
5. **重做所列內容。** Computed field、預覽無法讀取的已追蹤函式、
   觸發器重試和 webhook 身分驗證沒有自動轉換。
6. **設定身分驗證。** 如果你的 Hasura 實例使用 JWT/webhook 身分驗證，請建立一個
   身分驗證環境檔案，並用 `--auth-env-file` 重新運行。
7. **測試。** 啟動 Provisa 伺服器，並針對你的數據來源驗證查詢。

## 常見問題及疑難排解 {: #common-issues-and-troubleshooting }

### 連線來自環境變數 {: #connection-comes-from-an-environment-variable }

如果數據來源的 `database_url` 是環境變數引用（`{"from_env": "PG_URL"}`），
其值不在匯出中。轉換器不會猜測任何值：該數據來源的 host、資料庫、
使用者名稱和密碼為空，報告會把它列在 `Redo by hand` 之下。
在匯入的數據來源設定中（或透過 `--source-overrides`）輸入連線，然後再次
轉換。

### 萬用字元欄位 {: #wildcard-columns }

當某項權限授予 `columns: "*"` 時，轉換工具會建立單一的萬用字元欄位項目。轉換後，
您可能想透過檢查實際的資料庫結構描述，將其取代為明確的欄位清單。

### 事件觸發器的保真度 {: #event-trigger-fidelity }

Event 觸發器變為一個針對其資料表變更的觸發器，呼叫同一個 webhook。任何變更時它都按
每批變更觸發一次，無論變更是經由 Provisa 還是直接寫入資料庫，並發送資料表和
已變更的行。Hasura 的每行舊值和新值、按操作篩選以及重試都不會被帶過來；
報告會針對適用的每一項發出警告。
讓處理程式只處理它應當處理的行，如果漏掉一次呼叫會有影響，請在處理程式中重試。

### 缺失角色 {: #missing-roles }

角色從資料表權限、action 權限和繼承角色中收集。如果某個角色存在於 Hasura 中，
但不出現在其中任何一處，它就不會出現在輸出裡。

### 自訂根欄位 {: #custom-root-fields }

`select` 和 `select_by_pk` 根欄位名給出資料表別名。其他自訂根欄位
（`select_aggregate`、`insert`、`update`、`delete`）按名稱報告：Provisa 把
一張資料表作為一個欄位提供，聚合和寫入分別是它的 `_aggregate` 欄位和它的變更。

## 範例 {: #example }

轉換一個典型的 Hasura v2 專案，當中有兩個結構描述對應至不同網域：

```bash
# Export metadata from Hasura
hasura metadata export --endpoint http://localhost:8080

# Convert with domain mapping and source overrides
python -m provisa.hasura_v2 metadata/ \
  -o provisa.yaml \
  --domain-map public=core hr=people \
  --source-overrides overrides.yaml \
  --auth-env-file auth.env

# Dry run first to check for warnings
python -m provisa.hasura_v2 metadata/ --dry-run
```

輸出結構（節選）：

```yaml
sources:
  - id: default
    type: postgresql
    host: prod-db.example.com
    port: 5432
    database: myapp
domains:
  - id: core
  - id: people
tables:
  - source_id: default
    domain_id: core
    schema_name: public
    table_name: users
    columns:
      - name: id
        visible_to: [org_admin, user]
      - name: email
        visible_to: [org_admin, user]
    write_grants:
      - role: user
        operation: update
        columns: [email]
        rows: "id = current_setting('provisa.user_id')"
        check: "TRUE"
      - role: org_admin
        operation: insert
        columns: [id, email]
        check: "TRUE"
      - role: org_admin
        operation: update
        columns: [id, email]
        rows: "TRUE"
        check: "TRUE"
      - role: org_admin
        operation: delete
        rows: "TRUE"
roles:
  - id: user
    capabilities: [query_development, write]
    domain_access: ["*"]
rls_rules:
  - table_id: users
    role_id: user
    filter: "id = current_setting('provisa.user_id')"
relationships:
  - id: orders.user
    source_table_id: orders
    target_table_id: users
    source_column: user_id
    target_column: id
    cardinality: many-to-one
    graphql_alias: user
```
