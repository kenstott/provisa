# 從 Hasura DDN (v3) 遷移至 Provisa

## 先決條件

1. 一個包含 HML 檔案（副檔名 `.hml`）的 Hasura DDN 專案。
   DDN 專案通常具有以下目錄結構：
   ```
   my-ddn-project/
     app/
       subgraph1/
         models/
           MyModel.hml
         commands/
           MyCommand.hml
       subgraph2/
         ...
     globals/
       ...
   ```
2. Python 3.11 或以上版本，並已安裝 `provisa` 套件。

## CLI 使用方法

```bash
python -m provisa.ddn <hml-dir> -o provisa.yaml
```

### 參數

| 參數 | 是否必要 | 說明 |
|----------|----------|-------------|
| `hml_dir` | 是 | DDN HML 專案目錄的路徑（會遞迴掃描 `.hml` 檔案） |

### 選項

| 選項 | 預設值 | 說明 |
|--------|---------|-------------|
| `-o, --output FILE` | stdout | 輸出 YAML 檔案的路徑 |
| `--source-overrides FILE` | 無 | 包含各數據來源連線覆寫設定的 YAML 檔案 |
| `--domain-map KEY=VAL ...` | 無 | Subgraph 對應到 domain 的映射（例如 `app=core analytics=reporting`） |
| `--dry-run` | 關閉 | 只進行解析及驗證，不寫入輸出 |

### 數據來源覆寫檔案

一個以連接器（connector）名稱為索引鍵的 YAML 檔案（經過 ID 淨化後：空格、句號、斜線
會轉換為底線），並包含連線屬性：

```yaml
my_pg_connector:
  host: prod-db.example.com
  port: 5432
  database: chinook
  username: provisa_user
  password: "${env:PROD_DB_PASSWORD}"
```

## 功能對應表

| DDN 類型 | Provisa 對應項目 | 備註 |
|---|---|---|
| **DataConnectorLink** | `sources[]` | 數據來源類型會根據連接器的 URL 推斷（postgres、mysql、mssql、mongo、clickhouse、snowflake、bigquery）。連線詳情預設為佔位符；使用 `--source-overrides` 設定實際數值。 |
| **ObjectType** | `tables[]` 上的欄位定義 | 欄位（field）會轉換成資料表欄（column）。`dataConnectorTypeMapping.fieldMapping` 會將 GraphQL 欄位名稱對應至實際的資料表欄名稱。 |
| **Model** | `tables[]` | 每個 Model 會產生一個資料表。`source_id` 來自連接器，`table_name` 來自 collection。`graphql_type_name` 會變成 `alias`。Subgraph（以及因此的 `domain_id`）是根據檔案所在的目錄推斷：即專案根目錄下的第一層目錄名稱。 |
| **Relationship** | `relationships[]` | Object 類型 -> `many-to-one`，Array 類型 -> `one-to-many`。欄位映射會透過查找實際資料表欄來解析。 |
| **TypePermissions** | `columns[].visible_to[]` | `allowedFields` 決定哪些角色可以看到每個資料表欄。 |
| **ModelPermissions** | `rls_rules[]` | 篩選條件（filter predicate）會轉換為 SQL WHERE 子句。支援 `_eq`、`_neq`、`_gt`、`_lt`、`_gte`、`_lte`、`_in`、`_nin`、`_like`、`_is_null`、`_and`、`_or`、`_not`。工作階段變數（session variable）的引用會保留為 `${x-hasura-...}`。 |
| **Command** | `functions[]` | 函式（function）及程序（procedure）皆會被映射。引數、回傳類型及 GraphQL 根欄位名稱均會保留。`domain_id` 會根據 subgraph 設定。 |
| **AggregateExpression** | `provisa-aggregates.yaml` 附屬檔案 | Count、count_distinct 及各欄位的聚合函式會保留在附屬檔案中，並轉換為 Provisa 的聚合設定。 |
| **BooleanExpressionType** | 略過（不作提示） | DDN 內部用於篩選；毋須直接對應至 Provisa。 |
| **AuthConfig** | 略過（不作提示） | DDN 的驗證設定不會被映射；請另行設定 Provisa 的驗證機制。 |
| **ScalarType** | 略過 | 會顯示帶數量的警告。 |
| **GraphqlConfig** | 略過 | 會顯示帶數量的警告。 |
| **CompatibilityConfig** | 略過 | 會顯示帶數量的警告。 |
| **其他未能識別的類型** | 略過 | 會按類型顯示帶數量的警告。 |

## 核心概念：GraphQL 欄位對應至實際資料表欄

DDN 透過 ObjectType 上的 `dataConnectorTypeMapping`，將 GraphQL 結構描述（欄位名稱）
與實際數據庫結構描述（資料表欄名稱）分開。轉換工具會：

1. 讀取每個 ObjectType 類型映射中的 `fieldMapping` 項目。
2. 建立一個對照表：`{graphql_field_name -> physical_column_name}`。
3. 對於沒有明確映射的欄位，假設欄位名稱與資料表欄名稱相同。
4. 在建構資料表欄、關聯（relationship）及 RLS 篩選運算式時使用此對照表。

這代表輸出的 `provisa.yaml` 會在 `columns[].name` 使用**實際資料表欄名稱**，
並在名稱不同時，將 `columns[].alias` 設為 GraphQL 欄位名稱。

## 轉換後步驟

1. **檢查輸出的 YAML。** 驗證數據來源、資料表及欄位映射。
2. **設定數據來源連線。** 連接器僅提供 URL 提示以作類型偵測之用。
   實際的主機、連接埠、資料庫及憑證必須透過
   `--source-overrides` 提供，或直接編輯輸出檔案。
3. **驗證 domain 指派。** Subgraph 名稱是根據目錄結構推斷
   （即專案根目錄下的第一層目錄名稱）。若未使用 `--domain-map`，每個
   subgraph 名稱會直接成為 domain ID。可使用 `--domain-map` 為其重新命名。
4. **檢查 RLS 規則。** DDN 的篩選條件會轉換為近似的 SQL 語句。
   支援巢狀布林邏輯（`_and`/`_or`/`_not`），但複雜的
   跨關聯篩選可能需要人手覆核。
5. **檢查聚合設定。** 聚合運算式會寫入附屬檔案
   `provisa-aggregates.yaml`，並轉換為 Provisa 的聚合設定。
6. **檢查警告。** 轉換工具會在 stderr 輸出摘要，列出略過的
   DDN 類型，以及任何引用未知 ObjectType 的模型。
7. **測試。** 啟動 Provisa 伺服器，並針對你的數據來源驗證查詢。

## 常見問題及疑難排解

### 數據來源類型偵測失敗

連接器的 URL 會以啟發式方式（heuristically）判斷（尋找如「postgres」、
「mysql」、「mongo」等關鍵字）。若 URL 未包含可識別的關鍵字，
數據來源預設會使用 `postgresql`。可透過 `--source-overrides` 覆寫。

### Model 缺少 ObjectType

若某個 Model 引用的 ObjectType 名稱未能在任何 `.hml` 檔案中找到，
該資料表會被略過，並顯示警告。請確保所有 HML 檔案均已包含在
所掃描的目錄中。

### Subgraph 偵測

Subgraph 是根據目錄結構推斷：專案根目錄下的第一層目錄名稱
會被視為 subgraph 名稱。HML 文件內的 `subgraph` 欄位並不會被使用。
位於 `globals/` 目錄下的檔案會被歸入 `globals` subgraph，
並會被排除於 domain 偵測之外。

### 關聯來源解析

關聯會引用一個 `source_type`（ObjectType 名稱）及一個 `target_model`（Model
名稱）。若沒有任何 Model 使用指定的 ObjectType，該關聯會被靜默略過。

### 隨處可見的資料表欄別名

若你的 DDN 專案廣泛使用 `fieldMapping`，可預期大部分資料表欄在輸出中
都會有 `alias`。這是正常的行為 —— `name` 是實際的資料表欄，
`alias` 則是應用程式所使用的 GraphQL 名稱。

### 聚合運算式

聚合運算式會保留在與輸出檔案並列寫入的附屬檔案
`provisa-aggregates.yaml` 中，並轉換為 Provisa 的聚合設定。它們並不會
儲存在資料表的 `description` 中。

## 範例：轉換 Chinook DDN 專案

```bash
# Convert the DDN project
python -m provisa.ddn ./chinook-ddn/ \
  -o provisa.yaml \
  --domain-map app=music \
  --source-overrides overrides.yaml

# Dry run to check warnings first
python -m provisa.ddn ./chinook-ddn/ --dry-run
```

輸出結構：

```yaml
sources:
  - id: chinook_pg
    type: postgresql
    host: prod-db.example.com
    port: 5432
    database: chinook
    ...
domains:
  - id: music
tables:
  - source_id: chinook_pg
    domain_id: music
    schema_name: public
    table_name: Album
    columns:
      - name: AlbumId
        visible_to: [admin, user]
      - name: Title
        visible_to: [admin, user]
      - name: ArtistId
        visible_to: [admin, user]
    alias: Albums
  - source_id: chinook_pg
    domain_id: music
    schema_name: public
    table_name: Artist
    columns:
      - name: artist_id
        visible_to: [admin, user]
        alias: ArtistId
      - name: artist_name
        visible_to: [admin, user]
        alias: Name
    alias: Artists
roles:
  - id: admin
    capabilities: [read]
    domain_access: ["*"]
  - id: user
    capabilities: [read]
    domain_access: ["*"]
relationships:
  - id: chinook_pg.public.Album.Artist
    source_table_id: chinook_pg.public.Album
    target_table_id: chinook_pg.public.Artist
    source_column: ArtistId
    target_column: artist_id
    cardinality: many-to-one
functions:
  - name: GetTopTracks
    source_id: chinook_pg
    schema_name: public
    function_name: get_top_tracks
    returns: Track
    domain_id: music
    description: "DDN function"
```
