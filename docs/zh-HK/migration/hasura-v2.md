# 從 Hasura v2 遷移至 Provisa

## 先決條件

1. 一個正在執行的 Hasura v2 執行個體（v2.x），並已匯出中繼資料。
2. 使用 Hasura CLI 匯出中繼資料：

   ```bash
   hasura metadata export --endpoint http://localhost:8080
   ```

   這會建立一個 `metadata/` 目錄，當中包含 `sources.yaml`、`actions.yaml`、
   `cron_triggers.yaml`、`inherited_roles.yaml`、`remote_schemas.yaml` 等。
3. Python 3.11+，並已安裝 `provisa` 套件。

## CLI 用法

```bash
python -m provisa.hasura_v2 <metadata-dir> -o provisa.yaml
```

### 引數

| 引數 | 必要 | 說明 |
| ---------- | ---------- | ------------- |
| `metadata_dir` | 是 | 已匯出的 Hasura v2 中繼資料目錄路徑 |

### 選項

| 選項 | 預設值 | 說明 |
| -------- | --------- | ------------- |
| `-o, --output FILE` | stdout | 輸出 YAML 檔案的路徑 |
| `--source-overrides FILE` | 無 | 包含每個數據來源連線覆寫的 YAML 檔案 |
| `--domain-map KEY=VAL ...` | 無 | 結構描述 (Schema) 到網域的對應（例如 `public=core hr=people`） |
| `--auth-env-file FILE` | 無 | 包含 JWT／admin-secret 驗證設定的 `.env` 檔案路徑 |
| `--dry-run` | 關閉 | 僅解析並驗證，不寫入輸出 |

### 數據來源覆寫檔案

一個以數據來源名稱作為索引鍵的 YAML 檔案，包含要覆寫的連線屬性：

```yaml
default:
  host: prod-db.example.com
  port: 5432
  database: myapp
  username: provisa_user
  password: "${env:PROD_DB_PASSWORD}"
```

### 驗證環境檔案

一個 `.env` 格式的檔案，內含要轉換的 Hasura 驗證設定。轉換工具會進行以下對應：

- 帶有 `jwk_url` 的 JWT -> Provisa `provider: oauth`。
- JWT 的 `claims_map` -> Provisa `role_mapping[]`。
- Admin secret -> Provisa `superuser`。
- Webhook 驗證 -> 系統會發出警告（Provisa 沒有對應項目）。

## 功能對等表

| Hasura v2 功能 | Provisa 對等項目 | 備註 |
| --- | --- | --- |
| **數據來源**（postgres、mysql、mssql、bigquery、citus） | `sources[]` | 類型對應：pg/postgres -> postgresql，mssql -> sqlserver。連線 URL 會被解析為 host/port/database/username/password。連線池設定會被保留。 |
| **資料表**（已追蹤的資料表） | `tables[]` | 結構描述及資料表名稱會被保留。`source_id` 會連結至對應的數據來源。 |
| **自訂資料表名稱**（`custom_name`、`custom_root_fields.select`） | `tables[].alias` | 取 `select`、`select_by_pk`、`custom_name` 中第一個非空值。 |
| **自訂欄位名稱** | `columns[].alias` | 將 `custom_column_names` 字典對應至欄位別名。 |
| **選取權限**（欄位、篩選條件） | `columns[].visible_to[]`、`rls_rules[]` | 欄位清單會轉換為 `visible_to`。支援萬用字元（`*`）欄位。篩選條件會透過 `bool_expr_to_sql` 轉換為 SQL。 |
| **插入／更新權限**（欄位） | `columns[].writable_by[]` | 欄位清單會轉換為 `writable_by`。角色會被升級為擁有 `write` 能力。 |
| **刪除權限** | 角色能力升級 | 角色會取得 `write` 能力。沒有逐資料表的刪除對應。 |
| **物件關聯** | `relationships[]`，`cardinality: many-to-one` | 欄位對應會被保留。 |
| **陣列關聯** | `relationships[]`，`cardinality: one-to-many` | 欄位對應會被保留。 |
| **運算欄位** | `functions[]` | 對應至一個 Function，其 `returns` 指向父資料表的 ID。 |
| **已追蹤的函式** | `functions[]` | `exposed_as` 預設為 mutation。結構描述會被保留。 |
| **Actions**（儲存程序處理常式） | `functions[]` | 若由儲存程序支援，會轉換為 Function 設定。 |
| **Actions**（Webhook 處理常式） | 不會轉換 | 系統會發出警告，並包含處理常式的 URL。 |
| **Cron 觸發程序** | 不會轉換 | 系統會發出警告。（執行階段有排程觸發程序，但轉換工具不會對應。） |
| **事件觸發程序** | 不會轉換 | 系統會發出警告。（執行階段有事件觸發程序，但轉換工具不會對應。） |
| **繼承角色** | `roles[].parent_role_id` | `role_set` 中的第一個角色會成為上層角色。所有子角色都會被建立。 |
| **遠端結構描述** | `sources[]`（`graphql_remote`） | 會註冊為 `graphql_remote` 數據來源。名稱、URL、標頭及驗證設定會被保留。 |
| **列舉資料表** | 建立資料表 | `is_enum` 旗標不會被帶入（Provisa 沒有對應項目）。 |
| **允許清單** | 略過 | 中繼資料模型中沒有此項目。 |

## 轉換後步驟

1. **檢查輸出的 YAML。** 確認數據來源、資料表及角色是否正確。
2. **設定數據來源連線。** 轉換工具會解析連線 URL，但在解析失敗時會預設使用 `localhost`。
   請使用 `--source-overrides`，或直接編輯輸出結果。
3. **驗證網域指派。** 若未使用 `--domain-map`，所有資料表都會落入 `default`。
   請使用 `--domain-map public=core analytics=reporting` 將結構描述指派至網域。
4. **檢查 RLS 規則。** 篩選條件會被轉換為近似的 SQL。複雜的布林運算式
   （巢狀的 `_and`/`_or`/`_exists`）應人手檢查。
5. **檢視警告。** 轉換工具會在 stderr 印出警告摘要，列出其無法對應的功能
   （事件觸發程序、cron 觸發程序、以 webhook 為基礎的 actions）。
6. **設定驗證。** 若您的 Hasura 執行個體使用 JWT／webhook 驗證，請建立驗證環境檔案，
   並使用 `--auth-env-file` 重新執行。
7. **測試。** 啟動 Provisa 伺服器，並針對您的數據來源驗證查詢。

## 常見問題及疑難排解

### 連線 URL 未被解析

若數據來源的 `database_url` 是環境變數參照（`{"from_env": "PG_URL"}`），轉換工具在
轉換時無法解析。該數據來源會有預留位置值（`host: localhost`、`database: default`）。
請使用 `--source-overrides` 修正。

### 萬用字元欄位

當某項權限授予 `columns: "*"` 時，轉換工具會建立單一的萬用字元欄位項目。轉換後，
您可能想透過檢查實際的資料庫結構描述，將其取代為明確的欄位清單。

### 事件觸發程序的準確度

事件觸發程序會連同 `operations` 及 `webhook_url` 一併轉換，但 Hasura 特有的傳遞保證
（恰好一次、重新傳遞）在 Provisa 中沒有直接對應項目。請檢視 `event_triggers` 一節，
並相應地設定您的 webhook 基礎架構。

### 缺少角色

角色只會從權限項目中收集。若某個角色存在於 Hasura 中，但在任何資料表或 action
上都沒有權限，則不會出現在輸出結果中。

### 自訂根欄位

只有 `select` 及 `select_by_pk` 根欄位會用作資料表的別名。其他自訂根欄位
（`select_aggregate`、`insert`、`update`、`delete`）不會被對應。

## 範例

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

輸出結構：

```yaml
sources:
  - id: default
    type: postgresql
    host: prod-db.example.com
    port: 5432
    database: myapp
    ...
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
        visible_to: [user, admin]
      - name: email
        visible_to: [admin]
        writable_by: [admin]
    alias: Users
roles:
  - id: admin
    capabilities: [read, write]
    domain_access: ["*"]
  - id: user
    capabilities: [read]
    domain_access: ["*"]
rls_rules:
  - table_id: default.public.users
    role_id: user
    filter: "id = x-hasura-user-id"
relationships:
  - id: default.public.orders.user
    source_table_id: default.public.orders
    target_table_id: default.public.users
    source_column: user_id
    target_column: id
    cardinality: many-to-one
```
