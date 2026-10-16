# 管理 GraphQL API 參考 {: #admin-graphql-api-reference }

管理 GraphQL API 是 Provisa 的設定平面。它是管理 Web 應用為每一項管理操作所呼叫的 API——建立數據來源、註冊資料表、定義關係、設定 RLS 規則，以及塑造該模型的其他一切。

**掛載點：** `POST /admin/graphql`

這與 `/data/graphql` 處的數據平面 API 不同。數據平面為註冊網域上的最終使用者查詢提供服務，並由 `/data/sdl` 處的 SDL 描述。管理 API 則設定該結構描述的樣子以及誰可以看到什麼。

---

## UI 如何與該 API 通信 {: #how-the-ui-talks-to-this-api }

管理 Web 應用使用 Apollo Client，指向 `${API_BASE}/admin/graphql`。[tool-verified: `provisa-ui/src/apolloClient.ts:19`]

每個請求都攜帶一個 Bearer 權杖（每次呼叫時從身分驗證提供者重新取得）、在多租戶情況下攜帶一個 `X-Org-Id` 標頭，以及在為分支環境提供服務時攜帶一個 `X-Env` 標頭。[tool-verified: `provisa-ui/src/apolloClient.ts:24-42`]

該結構描述由兩個 `@strawberry.type` 類別組裝而成——來自 `schema_query.py` 的 `Query` 和來自 `schema_mutation.py` 的 `Mutation`——並包裹在一個 `ModelCommitExtension` 中，該擴充會針對目前環境分支記錄每一次變更（REQ-1524）。[tool-verified: `provisa/api/admin/schema.py:44`]

---

## 授權 {: #authorization }

**開發模式：** 當未設定身分驗證且每個請求都以匿名主體身份到達時，所有能力檢查都會被跳過。這使本機安裝無需身分驗證設定即可正常工作。[tool-verified: `provisa/api/admin/capabilities.py:98-99`]

**能力關卡：** 生產部署會強制執行命名能力。每個欄位所需的具體權限都在行內註明。在沒有所需能力的情況下呼叫變更會引發 `PermissionError`。平台管理員角色可繞過所有能力檢查（REQ-1297）。[tool-verified: `provisa/api/admin/capabilities.py:80-110`]

**網域關卡：** 若干變更還會檢查物件所屬的網域。一個作用網域限定為 `sales` 的呼叫者不能將資料表註冊到 `finance` 中、為其排隊一條 RLS 規則，或建立一個源資料表位於其不持有的網域中的關係（REQ-1530、REQ-1531）。檢視受到更進一步的約束：檢視 SQL 讀取的每一張資料表都必須在呼叫者的網域範圍內，因為自由手寫的 SQL 否則會讓成員存取其作用網域之外的數據。[tool-verified: `provisa/api/admin/domain_guard.py:1-133`]

**角色繼承：** 父角色的能力會被子角色繼承（REQ-1677）。`createRole` 和 `deleteRole` 會拒絕形成迴圈，並阻止刪除有繼承者的角色。

---

## 通用傳回類型 {: #common-return-type }

大多數變更傳回 `MutationResult`。[tool-verified: `provisa/api/admin/types.py:1181-1188`]

```graphql
type MutationResult {
  success: Boolean!
  message: String!
  code: String          # stable i18n key, e.g. "schema.source_created"
  params: JSON          # key/value pairs for client-side localization (REQ-1350)
}
```

當變更失敗時，`success` 為 `false`，`message` 攜帶英文原因。`code` 是一個穩定識別符，UI 使用它來渲染本地化訊息。

---

## 查詢 {: #queries }

### 數據來源 {: #sources }

#### `sources → [SourceType!]!`

所有已註冊的數據來源。[tool-verified: `provisa/api/admin/schema_query.py:327-331`]

```graphql
query {
  sources {
    id type host port database username dialect
    cacheEnabled cacheTtl replicate
    loadProtected offPeakWindow offPeakTz
    gqlNamingConvention path allowedDomains
    description mappingJson federationHintsJson
    changeSignal passwordRef
    cdc {
      bootstrapServers topicPrefix consumerGroupId
      schemaRegistryUrl schemaRegistryAuth schemaRegistryUsername schemaRegistryPassword
      schemaRegistryToken schemaRegistryClientCert schemaRegistryClientKey schemaRegistryCa
    }
  }
}
```

`passwordRef` 是指向組織保管庫的 `${secret:NAME}` 引用——絕不是字面憑證。[tool-verified: `provisa/api/admin/types.py:105`]

#### `source(id: String!) → SourceType`

按 ID 查找單一數據來源。未找到時傳回 `null`。[tool-verified: `provisa/api/admin/schema_query.py:334-339`]

#### `availableSchemas(sourceId: String!) → [String!]!`

數據來源中可見的結構描述，已過濾掉 Provisa 內部的結構描述。優先使用原生內省；當數據來源類型沒有直接連線池時，退回到引擎目錄。[tool-verified: `provisa/api/admin/schema_query.py:628-667`]

#### `availableTables(sourceId: String!, schemaName: String = "public") → [AvailableTableType!]!`

數據來源某個結構描述中的資料表及其註釋。對於 OpenAPI 數據來源，傳回回應為陣列或分頁包裝器的 GET 操作。對於 GraphQL 數據來源，傳回傳回清單的查詢欄位。對於 gRPC，傳回伺服器串流 RPC。[tool-verified: `provisa/api/admin/schema_query.py:669-731`]

#### `availableColumns(sourceId: String!, schemaName: String!, tableName: String!) → [String!]!`

引擎目錄中某張資料表的欄位名。對於 govdata 數據來源，使用單獨的解析器。[tool-verified: `provisa/api/admin/schema_query.py:845-866`]

#### `availableColumnsMetadata(sourceId: String!, schemaName: String!, tableName: String!) → [AvailableColumnType!]!`

帶有數據類型、註釋、原生過濾器類型和主鍵旗標的欄位名。對於 OpenAPI 數據來源，會根據操作的回應結構描述和參數推匯出結構。[tool-verified: `provisa/api/admin/schema_query.py:869-876`]

#### `availableFunctions(sourceId: String!, schemaName: String = "openapi") → [AvailableTableType!]!`

OpenAPI 數據來源的非 GET 操作（POST、PUT、PATCH、DELETE）。對於非 OpenAPI 數據來源傳回空清單。[tool-verified: `provisa/api/admin/schema_query.py:822-843`]

#### `crawlSource(path, depth, pattern, recursive, simpleLinks, sameDomain, excludePattern) → CrawlResultType`

在建立數據來源之前，預覽檔案連接器爬取將會發現的內容——檔案、資料表和欄位。僅限 HTTP 的設定（`simpleLinks`、`sameDomain`、`excludePattern`）在本機、S3、FTP 和 SFTP 根目錄下會被忽略。（REQ-1785）[tool-verified: `provisa/api/admin/schema_query.py:733-790`]

#### `suggestTableAlias(tableName: String!, domainId: String!, sourceId: String!) → String!`

傳回在 `domainId` 中從 `sourceId` 註冊 `tableName` 時應使用的別名。當不存在衝突時傳回一個普通的 snake-case 別名，當有效名稱已被同一網域中的另一數據來源佔用時傳回一個帶數據來源前綴的別名（`sqlite_b_orders`）。[tool-verified: `provisa/api/admin/schema_query.py:879-922`]

---

### 資料表 {: #tables }

#### `tables → [RegisteredTableType!]!`

所有已註冊的資料表，各自帶有完整的欄位清單。回應中的欄位可見性遵從呼叫者的 `table_registration` 能力——`canDeployToDb` 取決於呼叫者是否持有該權限。（REQ-016、REQ-021、REQ-042）[tool-verified: `provisa/api/admin/schema_query.py:502-536`]

每個 `RegisteredTableType` 都暴露計算得出的子欄位：

- **`refreshPolicySummary → RefreshPolicySummaryType`** —— 以純文字形式呈現的有效重新整理/服務策略，由伺服器端從引擎所使用的同一個規劃器解析派生。啟動期間傳回 `null`。（REQ-1143）[tool-verified: `provisa/api/admin/types.py:319-327`]
- **`graphqlFieldName → String`** —— 該資料表在已編譯的數據平面結構描述中的欄位名稱，使數據產品面板無需重新實作命名演算法即可建構可執行的範例。（REQ-1634）[tool-verified: `provisa/api/admin/types.py:330-339`]
- **`dqDataset → String`** —— 該資料表作為一個數據品質契約數據集，採用檢查器所掃描的形式。（REQ-1443）[tool-verified: `provisa/api/admin/types.py:374-387`]
- **`productId → String`** —— 該資料表所屬的數據產品。DQ 檢查器資料表繼承其契約所掃描資料表的產品。（REQ-1634）[tool-verified: `provisa/api/admin/types.py:342-372`]

#### `refreshPolicyPreview(...) → RefreshPolicySummaryType`

為*草稿*（尚未儲存）的資料表設定項預覽有效的重新整理/服務摘要，使表單頂部的摘要隨欄位變化而更新，而不持久化任何內容。派生方式與上面的 `refreshPolicySummary` 相同。（REQ-1143）[tool-verified: `provisa/api/admin/schema_query.py:947-979`]

參數：`sourceId`、`domainId`、`schemaName`、`tableName`、`cacheTtl`、`replicate`、`loadProtected`、`offPeakWindow`、`offPeakTz`、`changeSignal`。

#### `columnDependents(tableId: String!, renamed: [String!], removed: [String!]) → [ColumnDependentsType!]!`

待處理的別名重命名或欄位刪除將破壞的構件。僅供參考——管理 UI 會在儲存前顯示此資訊，由管理員決定。必須在*儲存之前*呼叫，因為依賴項是針對該欄位目前所持有的暴露名稱編寫的。（REQ-1484）[tool-verified: `provisa/api/admin/schema_query.py:1301-1331`]

---

### 關係 {: #relationships }

#### `relationships → [RelationshipType!]!`

所有使用者定義的關係（不包括自動產生的 `gql_auto__` 條目以及 ERD 使用的合成 `meta:%` 條目）。[tool-verified: `provisa/api/admin/schema_query.py:539-569`]

#### `allRelationships → [RelationshipType!]!`

與 `relationships` 相同，但包含 `meta:%` 合成條目。供圖形 ERD 使用，它需要顯示每一條邊，包括資料表與中繼資料註冊資料表之間的隱式 `HAS_TABLE` 連結。[tool-verified: `provisa/api/admin/schema_query.py:572-601`]

每個 `RelationshipType` 暴露：

- **`autoSuggested → Boolean`** —— 該關係是否由外鍵分析建議（`id` 以 `fk__` 開頭）。[tool-verified: `provisa/api/admin/types.py:523-525`]
- **`physicalName → String`** —— 該關係在 SQL 和 gRPC 平面上的名稱（`?include=` 參數）。由伺服器端派生；用戶端不得音譯 GraphQL 別名。（REQ-471、REQ-1417）[tool-verified: `provisa/api/admin/types.py:527-536`]

---

### 網域、角色與使用者 {: #domains-roles-users }

#### `domains → [DomainType!]!`

目前組織租戶資料庫中的所有網域。租戶資料庫在結構描述層面是隔離的，因此某個組織管理員的網域清單只包含其組織的行。（REQ-021、REQ-042、REQ-1293）[tool-verified: `provisa/api/admin/schema_query.py:342-357`]

#### `roles → [RoleType!]!`

呼叫者可見的角色。管理員可以看到每個角色；非管理員只能看到沒有 `org_id` 的角色，或屬於其組織的角色。（REQ-042、REQ-059、REQ-060、REQ-215）[tool-verified: `provisa/api/admin/schema_query.py:603-618`]

#### `resolveOwners(refs: [String!]!) → [UserSummaryType!]!`

將角色 ID 或使用者 ID 解析為具體使用者。用於將 `DataProduct.ownerRole`、`Domain.steward` 和 `Column.visibleTo` 展開為人類可讀的清單。未知引用會原樣回顯，以便 UI 顯示原始 ID 而不是空白。[tool-verified: `provisa/api/admin/schema_query.py:388-444`]

---

### RLS 規則 {: #rls-rules }

#### `rlsRules → [RLSRuleType!]!`

所有行層級安全規則。底層儲存庫會在邊界處解密 `filterExpr`。（REQ-041、REQ-402、REQ-686）[tool-verified: `provisa/api/admin/schema_query.py:621-625`]

---

### 數據產品 {: #data-products }

#### `dataProducts → [DataProductType!]!`

所有數據產品。需要 `data_product_read` 能力。（REQ-1634）[tool-verified: `provisa/api/admin/schema_query.py:360-386`]

---

### 標籤 {: #tags }

#### `tags → [TagType!]!`

所有標籤定義，包括每個標籤允許的參數值。（REQ-1373、REQ-1467）[tool-verified: `provisa/api/admin/schema_query.py:447-475`]

#### `tagAssignments → [TagAssignmentType!]!`

跨數據來源、資料表、欄位和關係的所有標籤分配。（REQ-1377）[tool-verified: `provisa/api/admin/schema_query.py:478-499`]

---

### 具體化檢視 {: #materialized-views }

#### `mvList → [MVType!]!`

所有具體化檢視及其執行時狀態：啟用/停用、上次重新整理時間戳記、資料欄位數和上次錯誤。[tool-verified: `provisa/api/admin/schema_query.py:927-944`]

---

### 快取 {: #cache }

#### `cacheStats → CacheStatsType`

快取統計資訊。設定了 Redis 時傳回帶有完整執行指標的 `storeType: "redis"`；使用內嵌 fakeredis 儲存時傳回 `storeType: "memory"`；未設定快取時傳回 `storeType: "noop"`。[tool-verified: `provisa/api/admin/schema_query.py:1106-1140`]

#### `cacheTableStats → [CacheTableStatType!]!`

每張資料表已快取條目的計數。未設定快取儲存時為空。[tool-verified: `provisa/api/admin/schema_query.py:1143-1148`]

#### `hotTables → [HotTableStatType!]!`

Provisa 正在保留副本的資料表，分為兩個層級：`hot`（鏡像到回應儲存中以供 JOIN 內聯）和 `warm`（作為 Iceberg 副本落地）。一張資料表最多處於一個層級中（REQ-241）。[tool-verified: `provisa/api/admin/schema_query.py:1151-1175`]

#### `materializeStoreInfo → MaterializeStoreInfoType`

持久化物化儲存的身份資訊：引擎名稱、儲存 DSN 引用、具體化檢視數量，以及該儲存是否為執行個體本機儲存（如 DuckDB 或 SQLite 這類本機檔案，意味著負載平衡器背後的每個執行個體都保留自己的副本）。[tool-verified: `provisa/api/admin/schema_query.py:1178-1189`]

---

### 系統健康狀態 {: #system-health }

#### `systemHealth → SystemHealthType`

引擎連線狀態、工作程序池計數、中繼資料資料庫連線池狀態、快取模式，以及每個協定監聽器（pgwire、gRPC、Arrow Flight、Bolt）的存活狀態。[tool-verified: `provisa/api/admin/schema_query.py:1194-1198`]

```graphql
query {
  systemHealth {
    engineConnected engineWorkerCount engineActiveWorkers
    metadataPoolSize metadataPoolFree metadataDialect
    cacheMode cacheConnected mvRefreshLoopRunning
    protocols { name status port }
  }
}
```

---

### 排程工作 {: #scheduled-tasks }

#### `scheduledTasks → [ScheduledTaskType!]!`

來自設定的排程觸發器及其執行時狀態。每個條目攜帶其 cron 運算式、`kind`（`webhook` 或 `sql`）、目前是否啟用、上次執行時間戳記（本版本中始終為 `null`——由排程器追蹤），以及來自 APScheduler 的下次計劃執行時間。[tool-verified: `provisa/api/admin/schema_query.py:1203-1245`]

---

### 數據品質 {: #data-quality }

#### `dqContractParse(checker: String!, contractText: String!) → DqContractType`

將原始契約文字解析為建構面板中可編輯的行。每次編輯都會呼叫；解析失敗會以 `error` 形式傳回，而不是作為 GraphQL 錯誤傳回，因為操作員輸入過程中出現半成品文字是正常情況。（REQ-1443 第 7 條）[tool-verified: `provisa/api/admin/schema_query.py:1007-1022`]

#### `dqCheckCatalog(checker: String!, dataset: String!) → DqCheckCatalogType`

`checker` 提供的檢查項，限定在 `dataset` 的欄位範圍內。該數據集是契約的觀測目標，其解析方式與掃描器的解析方式相同——因此所提供的檢查項與檢查器實際會看到的欄位相匹配。（REQ-1443 第 7 條）[tool-verified: `provisa/api/admin/schema_query.py:1025-1050`]

#### `dqCheckDefinition(checker: String!, check: DqCheckBuildInput!) → DqCheckDefinitionType`

來自面板編輯器的某一項檢查的文字。放在伺服器端是因為該方言只有一種實作；透過建構器建立的檢查和手工輸入的檢查必須無法區分。（REQ-1443 第 7 條）[tool-verified: `provisa/api/admin/schema_query.py:1053-1075`]

#### `dqContractBuild(checker: String!, dataset: String!, checks: [DqCheckInput!]!) → DqContractTextType`

將編輯後的檢查行序列化回契約文字。是 `dqContractParse` 的逆操作。放在伺服器端出於同樣的原因：面板不能產生檢查器會拒絕的文字。（REQ-1443 第 7 條）[tool-verified: `provisa/api/admin/schema_query.py:1078-1101`]

---

### 數據來源預覽 {: #source-previews }

#### `neo4jPreview(sourceId: String!, cypher: String!) → QueryPreviewType`

預覽 Neo4j 數據來源上的 Cypher 投影：最多五行以及註冊將攜帶的欄位類型。失敗會以 `error` 形式傳回。（REQ-1670）[tool-verified: `provisa/api/admin/schema_query.py:984-992`]

#### `sparqlPreview(sourceId: String!, query: String!) → QueryPreviewType`

預覽 SPARQL 數據來源上的 SPARQL SELECT：最多五行，所有欄位均為文字形式。（REQ-1683）[tool-verified: `provisa/api/admin/schema_query.py:995-1002`]

---

### Kaggle

#### `kaggleTokenValid(token: String!) → Boolean!`

針對 Kaggle API 的即時檢查。只有當權杖透過身分驗證時才傳回 `true`。為 Kaggle 數據來源表單中的權杖關卡步驟提供支援。（REQ-1783）[tool-verified: `provisa/api/admin/schema_query.py:793-799`]

#### `kaggleDatasets(token: String!, query: String = "", page: Int = 1) → [KaggleDatasetType!]!`

搜尋 Kaggle 完整的公共數據集目錄。受權杖限制。（REQ-1783）[tool-verified: `provisa/api/admin/schema_query.py:802-819`]

---

### 日曆 {: #calendars }

#### `calendars → [CalendarType!]!`

所有已註冊的快照邊界日曆版本。為快照計劃設定選擇器提供數據，並確認週期性具體化檢視可以引用哪些日曆。（REQ-962）[tool-verified: `provisa/api/admin/schema_query.py:218-242`]

---

### 指標 {: #metrics }

#### `metrics → [MetricType!]!`

所有受治理的指標定義。源自事實資料表的指標攜帶 `fromFact`。（REQ-1317、REQ-1320）[tool-verified: `provisa/api/admin/schema_query.py:245-264`]

---

### 結構描述版本 {: #schema-version }

#### `schemaVersion → String!`

目前結構描述狀態（網域、資料表 ID、關係 ID）的 SHA-256 雜湊值。Apollo 用戶端從 `X-Schema-Version` 回應標頭中讀取此值，並在其推進時重新取得所有使用中的查詢。[tool-verified: `provisa/api/admin/schema_query.py:291-324`]

---

### AI 助手 {: #ai-helpers }

#### `generateTableDescription(tableId: String!) → String!`

使用設定的 LLM 為一張已註冊的資料表產生一到兩句話的描述。請先儲存該資料表；對未儲存的資料表呼叫此介面會傳回一條說明性訊息。[tool-verified: `provisa/api/admin/schema_query.py:1250-1298`]

#### `generateColumnDescription(tableId: String!, columnName: String!) → String!`

使用設定的 LLM 為單一欄位產生一句話的描述。[tool-verified: `provisa/api/admin/schema_query.py:1334-1383`]

---

### 建立請求 {: #creation-requests }

#### `creationRequests → [CreationRequestType!]!`

待處理的建立請求，對持有相應建立能力的呼叫者可見。當沒有 `create_relationship` 或 `create_view` 的成員提交一項需要權利持有者批准的請求時使用。關係請求向其 `create_relationship` 權利覆蓋其源資料表或目標資料表所在網域的使用者顯示，也向提出請求的使用者顯示。（REQ-434、REQ-063、REQ-1948） [tool-verified: `provisa/api/admin/schema_query.py:371-420`]

---

## 變更 {: #mutations }

### 數據來源 {: #sources_1 }

#### `createSource(input: SourceInput!) → MutationResult`

註冊新的數據來源。已存在的 id 會在定義檢查之後、準備或寫入任何內容之前被拒絕（`schema.source_id_taken`）；數據來源透過 `updateSource` 修改。儲存前測試連線——被拒絕的數據來源不會在儲存中留下記錄。將憑證存入組織的密鑰庫並記錄引用；明文從不進入資料庫。（REQ-012、REQ-013） 需要 `source_registration` 能力。[tool-verified: `provisa/api/admin/schema_mutation.py:616-781`]

#### `updateSource(input: SourceInput!) → MutationResult`

更新已有數據來源的連線參數、描述和設定，然後在其綁定的引擎上重新準備它、重建 schema 並協調其已載入的資料表，使在一個引擎下儲存的數據來源能由之後切換到的引擎提供服務（REQ-1730）。對檔案/SharePoint 數據來源斷開並重新掛載 pgwire 端點，使路徑變更立即生效。需要 `source_registration`。[tool-verified: `provisa/api/admin/schema_mutation.py:922-1073`]

#### `deleteSource(id: String!) → MutationResult`

移除一個數據來源及其保管庫條目。刪除引擎目錄並重建結構描述。[tool-verified: `provisa/api/admin/schema_mutation.py:1101-1132`]

#### `renameSource(oldId: String!, newId: String!) → MutationResult`

重命名數據來源 id。由另一個數據來源擁有的新 id 會被拒絕（`schema.source_id_taken`），且不寫入任何內容；將數據來源重命名為其自身的 id 不會改變任何內容。[tool-verified: `provisa/core/repositories/source.py` `rename`]

#### `updateSourceCache(sourceId: String!, cacheEnabled: Boolean!, cacheTtl: Int) → MutationResult`

為一個數據來源啟用或停用查詢結果快取，並以秒為單位設定 TTL。[tool-verified: `provisa/api/admin/schema_mutation.py:2327-2350`]

#### `updateSourceReplicate(sourceId: String!, replicate: Int) → MutationResult`

設定數據來源的資料表何時從其副本提供服務：`null` Default、`-1` Never、大於 0 的 `N`——一旦資料表在一個間隔內超過 N 條受治理語句、`0` Always。沒有自身值的資料表繼承它。如果會把 `load_protected` 與 `-1` 組合，或讓它所複製的 `ttl` 資料表沒有 `cache_ttl`，則被拒絕。需要 `source_registration`。（REQ-826、REQ-1907） [tool-verified: `provisa/api/admin/schema_mutation.py` `update_source_replicate`]

#### `updateSourceLoadProtection(sourceId, loadProtected, offPeakWindow, offPeakTz) → MutationResult`

將一個數據來源標記為受負載保護（僅按計劃重新整理）。至少需要一個關卡——非高峰時段窗口、快取 TTL 節奏，或探測式變更信號——否則呼叫失敗。（REQ-1141）[tool-verified: `provisa/api/admin/schema_mutation.py:2431-2480`]

#### `updateSourceNaming(sourceId: String!, gqlNamingConvention: String) → MutationResult`

設定按數據來源的 GraphQL 命名約定。[tool-verified: `provisa/api/admin/schema_mutation.py:2595-2619`]

#### `updateSourceAllowedDomains(sourceId: String!, allowedDomains: [String!]!) → MutationResult`

設定哪些網域可以使用某數據來源（空清單 = 不受限制）。需要 `source_registration`。（REQ-1531）[tool-verified: `provisa/api/admin/schema_mutation.py:2622-2658`]

#### `stageKaggleDataset(token, owner, ref, idPrefix) → KaggleStageResultType`

下載並解壓一個 Kaggle 數據集到本機磁碟。傳回暫存目錄路徑；呼叫者隨後建立一個指向該目錄的 `files` 類型數據來源。攜帶 SQLite 的壓縮包會被整體拒絕。需要 `source_registration`。（REQ-1780–1782）[tool-verified: `provisa/api/admin/schema_mutation.py:784-824`]

#### `refreshKaggleSource(sourceId: String!, token: String!) → MutationResult`

原地重新取得一個源自 Kaggle 的數據來源的數據集。如果 Kaggle 上沒有比磁碟上更新的內容，則跳過下載。需要 `source_registration`。（REQ-1787）[tool-verified: `provisa/api/admin/schema_mutation.py:827-920`]

#### `refreshSourceStatistics(sourceId: String!) → MutationResult`

對某數據來源的所有已註冊資料表執行 `ANALYZE`。改善聯邦查詢的連線順序和廣播決策。（REQ-276）[tool-verified: `provisa/api/admin/schema_mutation.py:2944-3008`]

---

### 資料表 {: #tables_1 }

#### `registerTable(input: TableInput!) → MutationResult`

將一張新資料表（或檢視）註冊到某個網域中。需要 `table_registration` 能力以及目標網域的成員身份。缺少 `create_relationship` 能力而提交檢視的呼叫者，會被加入待權限持有者批准的建立請求佇列。（REQ-013、REQ-016、REQ-252、REQ-434）[tool-verified: `provisa/api/admin/schema_mutation.py:1714-1718`]

#### `updateTable(input: TableInput!) → MutationResult`

更新一張現有資料表的別名、描述、欄位中繼資料、具體化檢視設定和即時投遞設定。（REQ-016、REQ-020）需要 `table_registration`。[tool-verified: `provisa/api/admin/schema_mutation.py:1858-1995`]

#### `deleteTable(id: Int!) → MutationResult`

刪除一張已註冊的資料表。在刪除前查找該資料表所屬的網域以進行網域關卡檢查。（REQ-1531）[tool-verified: `provisa/api/admin/schema_mutation.py:1998-2029`]

#### `updateTableCache(tableId: Int!, cacheTtl: Int) → MutationResult`

覆蓋單張資料表的快取 TTL。[tool-verified: `provisa/api/admin/schema_mutation.py:2353-2376`]

#### `updateTableReplicate(tableId: Int!, replicate: Int) → MutationResult`

設定單張資料表何時從其副本提供服務。`null` = 繼承數據來源的值。取值與拒絕情形同 `updateSourceReplicate`。需要 `table_registration`。（REQ-826、REQ-1907） [tool-verified: `provisa/api/admin/schema_mutation.py` `update_table_replicate`]

#### `updateTableLoadProtection(tableId, loadProtected, offPeakWindow, offPeakTz) → MutationResult`

覆蓋單張資料表的負載保護設定。`loadProtected` 為 `null` 時繼承數據來源預設值。驗證有效的（資料表 → 數據來源）關卡組合。（REQ-1141）[tool-verified: `provisa/api/admin/schema_mutation.py:2483-2566`]

#### `updateTableNaming(tableId: Int!, gqlNamingConvention: String) → MutationResult`

設定按資料表的 GraphQL 命名約定。[tool-verified: `provisa/api/admin/schema_mutation.py:2661-2685`]

#### `deployViewToDb(tableId: Int!) → MutationResult`

將一個虛擬 Provisa 檢視提升為其底層原生數據來源上的真實資料庫檢視。[tool-verified: `provisa/api/admin/schema_mutation.py:3058-3060`]

#### `forceRegen(tableId: Int!, reason: String!) → MutationResult`

按需重新計算一張資料表的落地行，繞過正常的變更關卡。`reason` 是必需的審計註釋。對於即時聯邦資料表（沒有落地行）會被拒絕。（REQ-968）[tool-verified: `provisa/api/admin/schema_mutation.py:2689-2782`]

#### `invalidateFileSource(tableId: Int!) → MutationResult`

強制一張 SQLite 檔案連接器資料表在下一次存取時從磁碟重新同步。[tool-verified: `provisa/api/admin/schema_mutation.py:2877-2880`]

#### `registerEntity(input: EntityInput!) → MutationResult`

註冊維度/中心實體的語法糖。降級為一個（在歷史化時為雙時態的）具體化檢視，並呼叫 `registerTable`。（REQ-1164）[tool-verified: `provisa/api/admin/schema_mutation.py:1721-1725`]

#### `registerFact(input: FactInput!) → MutationResult`

註冊星型結構描述事實資料表的語法糖。降級為一個聚合具體化檢視，建立維度關係，並將事實度量自動註冊為受治理的指標。（REQ-1164、REQ-1320）[tool-verified: `provisa/api/admin/schema_mutation.py:1728-1776`]

---

### 關係 {: #relationships_1 }

#### `createRelationship(input: RelationshipInput!) → MutationResult`

建立關係。檢查看的是源資料表所在的網域（而非目標資料表）。跨網域的邊以 `needsReview: true` 儲存。沒有 `create_relationship` 的呼叫者會作為建立請求排隊。junction（多對多）邊要求鍵清單長度相同。在寫入任何內容之前點名拒絕：已存在的 id（`schema.relationship_id_taken`）；無法解析到任何已註冊資料表或解析到多張資料表的資料表名稱（`schema.relationship_unknown_source`、`_target`、`_junction`、`schema.relationship_ambiguous_table`）；已被同一源資料表的另一個關係佔用的 Cypher 或 GraphQL 別名（`schema.relationship_alias_taken`、`schema.relationship_graphql_alias_taken`）。資料表以其虛擬名稱指定：有別名時即為其別名。（REQ-019、REQ-020、REQ-366、REQ-434） [tool-verified: `provisa/api/admin/schema_mutation.py` `_save_relationship_as_caller`, `save_relationship`]

#### `updateRelationship(input: RelationshipInput!) → MutationResult`

以與 `createRelationship` 相同的檢查修改已有關係。不屬於任何關係的 id 會被拒絕（`schema.relationship_not_found`）。修改需要該權利本身；它從不作為請求排隊。（REQ-019、REQ-020） [tool-verified: `provisa/api/admin/schema_mutation.py` `_save_relationship_as_caller`]

#### `deleteRelationship(id: String!) → MutationResult`

按 ID 刪除一個關係並重建結構描述。[tool-verified: `provisa/api/admin/schema_mutation.py:2303-2322`]

---

### 網域 {: #domains }

#### `createDomain(input: DomainInput!) → MutationResult`

建立網域。已存在的 id 會被拒絕（`schema.domain_id_taken`）。保留的段名（`tables`、`relationships` 及其他 URI 路徑段）會被拒絕，通配符字面量 `*` 也會被拒絕。需要 `org_settings` 能力。（REQ-021、REQ-1531） [tool-verified: `provisa/api/admin/schema_mutation.py:1135-1189`]

#### `deleteDomain(id: String!) → MutationResult`

刪除一個網域。需要 `org_settings`。（REQ-1531）[tool-verified: `provisa/api/admin/schema_mutation.py:1192-1213`]

#### `updateGqlNamingConvention(convention: String!) → MutationResult`

設定全局 GraphQL 命名約定，併為所有角色重建結構描述。只接受已識別的約定名稱。（REQ-253、REQ-416）[tool-verified: `provisa/api/admin/schema_mutation.py:2571-2592`]

---

### 角色 {: #roles }

#### `createRole(input: RoleInput!) → MutationResult`

建立一個角色，帶有能力、網域存取、可選的請求限制和可選的父角色。驗證父角色存在且父鏈中沒有環。定義檢查之後，已存在的 id 會被拒絕（`schema.role_id_taken`）。需要 `user_management`。（REQ-042、REQ-1531、REQ-1677） [tool-verified: `provisa/api/admin/schema_mutation.py:1657-1712`]

#### `updateRole(input: RoleInput!) → MutationResult`

以與 `createRole` 相同的檢查修改已有角色。不屬於任何角色的 id 會被拒絕（`schema.role_not_found`）。重新定義角色是在它所覆蓋的每個網域中的操作。（REQ-042、REQ-1531） [tool-verified: `provisa/api/admin/schema_mutation.py` `_save_role`]

#### `deleteRole(id: String!) → MutationResult`

刪除一個角色。如果其他角色繼承自它則失敗——請先重新指定它們的父級。需要 `user_management`。（REQ-1531、REQ-1677）[tool-verified: `provisa/api/admin/schema_mutation.py:2032-2063`]

---

### RLS 規則 {: #rls-rules_1 }

#### `upsertRlsRule(input: RLSRuleInput!) → MutationResult`

建立或更新一條行層級安全規則。過濾運算式會在儲存時針對目標資料表或網域的欄位進行驗證，因此管理員無法查詢的規則會連同原因一起被拒絕，而不是在查詢時悄然失敗。需要 `masking_config`。（REQ-041、REQ-402、REQ-1531、REQ-1676）[tool-verified: `provisa/api/admin/schema_mutation.py:2066-2136`]

目標是互斥的：為資料表級規則設定 `tableId`，為網域級規則設定 `domainId`，或為追蹤的函數/webhook 設定 `actionName`。（REQ-1679）

#### `deleteRlsRule(roleId, tableId, domainId, actionName) → MutationResult`

刪除一條 RLS 規則。需要 `masking_config`。（REQ-1531）[tool-verified: `provisa/api/admin/schema_mutation.py:2139-2178`]

---

### 數據產品 {: #data-products_1 }

#### `createDataProduct(input: DataProductInput!) → MutationResult`

建立數據產品。已存在的 id 會被拒絕（`schema.data_product_id_taken`）。需要 `data_product_rw` 能力。（REQ-1634） [tool-verified: `provisa/api/admin/schema_mutation.py:1216-1259`]

#### `updateDataProduct(input: DataProductInput!) → MutationResult`

修改已有數據產品。不屬於任何產品的 id 會被拒絕（`schema.data_product_not_found`）。需要 `data_product_rw`。（REQ-1634） [tool-verified: `provisa/api/admin/schema_mutation.py` `_save_data_product`]

#### `deleteDataProduct(id: String!) → MutationResult`

刪除一個數據產品。需要 `data_product_rw`。（REQ-1634）[tool-verified: `provisa/api/admin/schema_mutation.py:1262-1285`]

---

### 標籤 {: #tags_1 }

#### `createTag(input: TagInput!) → MutationResult`

建立標籤定義。已存在的 id 會被拒絕（`schema.tag_id_taken`）。系統標籤和派生標籤不能被重新定義。`appliesTo` 必須是 `["source", "table", "column", "relationship", "command"]` 的非空子集。（REQ-1373、REQ-1375） [tool-verified: `provisa/api/admin/schema_mutation.py:1288-1361`]

#### `updateTag(input: TagInput!) → MutationResult`

以與 `createTag` 相同的檢查修改已有標籤定義。不屬於任何標籤的 id 會被拒絕（`schema.tag_not_found`）。（REQ-1373） [tool-verified: `provisa/api/admin/schema_mutation.py` `_save_tag`]

#### `deleteTag(id: String!) → MutationResult`

刪除一個標籤。拒絕刪除系統標籤和派生標籤。（REQ-1373）[tool-verified: `provisa/api/admin/schema_mutation.py:1364-1393`]

#### `assignTag(input: TagAssignmentInput!) → MutationResult`

將一個標籤分配給數據來源、資料表、欄位、關係或命令。強制執行該標籤的欄位策略（`reason_policy`、`expires_policy`），並且——對於參數化標籤——根據標籤允許的清單驗證參數值。（REQ-1376、REQ-1377）[tool-verified: `provisa/api/admin/schema_mutation.py:1396-1527`]

#### `unassignTag(input: TagAssignmentInput!) → MutationResult`

移除一個標籤分配。（REQ-1377）[tool-verified: `provisa/api/admin/schema_mutation.py:1530-1564`]

#### `upsertTagParamValue(input: TagParamValueInput!) → MutationResult`

為一個參數化標籤添加或重新描述一個允許的參數值。允許值清單是封閉的：每次分配都必須命名其中的一個值。（REQ-1467）[tool-verified: `provisa/api/admin/schema_mutation.py:1567-1616`]

#### `deleteTagParamValue(tagId: String!, value: String!) → MutationResult`

移除一個允許的值。只要仍有分配攜帶該值，就會被拒絕，因為那些分配將命名一個該清單已不再接受的類型。（REQ-1467）[tool-verified: `provisa/api/admin/schema_mutation.py:1619-1655`]

---

### 指標 {: #metrics_1 }

#### `createMetric(input: MetricInput!) → MutationResult`

建立受治理的指標定義。已存在的名稱會被拒絕（`schema.metric_name_taken`）。運算式必須能被 sqlglot 解析，且至少包含一個聚合函數。會重新建立所有引用此指標的、由指標組成的檢視。需要 `table_registration`。（REQ-1317、REQ-1318） [tool-verified: `provisa/api/admin/schema_mutation.py:1779-1831`]

#### `updateMetric(input: MetricInput!) → MutationResult`

以與 `createMetric` 相同的檢查替換已有指標的定義。不屬於任何指標的名稱會被拒絕（`schema.metric_not_found`）。儲存需要新運算式和被替換定義的網域。（REQ-1317、REQ-1531） [tool-verified: `provisa/api/admin/schema_mutation.py` `_save_metric`]

#### `deleteMetric(name: String!) → MutationResult`

刪除一個受治理的指標。重建結構描述。需要 `table_registration`。（REQ-1317）[tool-verified: `provisa/api/admin/schema_mutation.py:1834-1856`]

---

### 日曆 {: #calendars_1 }

#### `createCalendar(input: CalendarInput!) → MutationResult`

建立或替換一個帶版本的快照邊界日曆。透過在持久化之前構造記憶體中的 `Calendar` 來驗證——在未知的基礎系統、錯誤的時區或錯誤的財年錨點上會失敗。（REQ-962）[tool-verified: `provisa/api/admin/schema_mutation.py:525-579`]

#### `deleteCalendar(name: String!) → MutationResult`

刪除一個日曆（所有版本）。當任何具體化檢視引用它時會被拒絕。（REQ-962）[tool-verified: `provisa/api/admin/schema_mutation.py:582-613`]

---

### 具體化檢視 {: #materialized-views_1 }

#### `refreshMv(mvId: String!) → MutationResult`

觸發一次具體化檢視的手動重新整理。當具體化檢視一致性模式為 `shared` 時會在整個叢集範圍內協調。（REQ-133、REQ-158、REQ-879）[tool-verified: `provisa/api/admin/schema_mutation.py:2787-2813`]

#### `toggleMv(mvId: String!, enabled: Boolean!) → MutationResult`

啟用或停用一個具體化檢視。[tool-verified: `provisa/api/admin/schema_mutation.py:2816-2839`]

---

### 快取 {: #cache_1 }

#### `purgeCache → MutationResult`

清除所有快取的查詢結果。[tool-verified: `provisa/api/admin/schema_mutation.py:2844-2858`]

#### `purgeCacheByTable(tableId: Int!) → MutationResult`

清除單張資料表的快取結果。[tool-verified: `provisa/api/admin/schema_mutation.py:2861-2875`]

---

### 排程工作 {: #scheduled-tasks_1 }

#### `createScheduledTask(id, name, cron, kind, webhookName, argsJson, sql) → MutationResult`

建立一個排程觸發器——webhook 呼叫或 SQL 語句——並在 APScheduler 中即時註冊它。`kind` 為 `"webhook"` 或 `"sql"`。（REQ-1003、REQ-1004）[tool-verified: `provisa/api/admin/schema_mutation.py:2923-2936`]

#### `deleteScheduledTask(taskId: String!) → MutationResult`

從設定和即時排程器中移除一個排程觸發器。（REQ-1003）[tool-verified: `provisa/api/admin/schema_mutation.py:2939-2941`]

#### `toggleScheduledTask(taskId: String!, enabled: Boolean!) → MutationResult`

在設定檔中啟用或停用一個排程工作。[tool-verified: `provisa/api/admin/schema_mutation.py:2885-2920`]

---

### 數據品質 {: #data-quality_1 }

#### `dryRunDqContract(sourceId: String!, contractText: String!) → DqDryRunType`

針對即時表執行一個契約，並傳回結果而不落地任何內容。之所以是一個變更而不是查詢，是因為它會產生一次真實的掃描成本。它所驗證的是數據集識別符是否能解析為操作員所指的受治理的資料表。（REQ-1443 第 7 條）[tool-verified: `provisa/api/admin/schema_mutation.py:463-487`]

#### `runDqCheckNow(schemaName: String!, tableName: String!) → MutationResult`

立即觸發某個檢查器資料表的輪詢任務。以正常方式落地行，因此結果會持久化，並且 DQ 歷史會顯示新的掃描。（REQ-1443）[tool-verified: `provisa/api/admin/schema_mutation.py:490-522`]

---

### 結構描述維護 {: #schema-maintenance }

#### `rebuildSchemas → MutationResult`

從資料庫狀態重建記憶體中的結構描述。在外部資料庫發生變更後很有用。[tool-verified: `provisa/api/admin/schema_mutation.py:456-461`]

---

### 建立請求 {: #creation-requests_1 }

#### `approveCreationRequest(requestId: Int!) → MutationResult`

批准一項建立請求：規則、拒絕情形和管理記錄記錄與 `POST /admin/creation-requests/{id}/approve` 相同。請求者不能批准自己的請求，同一使用者的批准只計一次。使請求計數完成的批准會建立請求所要求的內容；若由它完成建立，`params.status` 為 `executed`，否則為 `pending`，`params.waitingOn` 列出關係請求仍在等待的網域。（REQ-434、REQ-063、REQ-1948） [tool-verified: `provisa/api/admin/schema_mutation.py` `approve_creation_request`]

#### `executeCreationRequest(requestId: Int!) → MutationResult`

透過直接變更所用的同一程式碼，執行已獲得批准的建立請求——關係、檢視、資料表、數據來源或 webhook。在請求所需的批准（一個，關係為兩個）由請求者以外的使用者給出之前，以 `requests.approvals_incomplete` 拒絕；關係請求在其涉及的每個網域都有批准者之前，還會以 `requests.waiting_on_domains` 拒絕。批准透過 `POST /admin/creation-requests/{id}/approve` 給出，使計數完成的批准會執行建立，因此此變更用於重試失敗的建立。失敗的建立會讓請求保持待處理，並以其自身的錯誤代碼應答。（REQ-434、REQ-063、REQ-1948） [tool-verified: `provisa/api/admin/schema_mutation.py` `execute_creation_request`, `perform_request_creation`; `provisa/api/admin/creation_requests_router.py`]

#### `rejectCreationRequest(requestId: Int!, reason: String!) → MutationResult`

以可操作的原因從佇列中拒絕一項請求。`reason` 為必填。關係請求可由任何本可以批准它的使用者拒絕。其他任何類型的請求，其請求者可以撤回；這會作為撤回記入管理記錄。（REQ-434、REQ-063、REQ-1948） [tool-verified: `provisa/api/admin/schema_mutation.py:3064-3106`]

---

### 查詢編譯 {: #query-compilation }

#### `compileQuery(input: CompileQueryInput!) → [CompileQueryResult!]!`

針對某個角色的結構描述編譯一個數據平面 GraphQL 查詢，並傳回完整的路由決策：語義 SQL、引擎 SQL、直接 SQL、路由、強制執行中繼資料（已應用的 RLS 過濾器、被排除的欄位、已應用的遮罩），以及編譯後的 Cypher。查詢中每個根欄位傳回一個結果。（REQ-161）[tool-verified: `provisa/api/admin/schema_mutation.py:3011-3055`]

```graphql
mutation {
  compileQuery(input: {
    role: "analyst"
    query: "{ orders { id customer_id total } }"
  }) {
    sql
    semanticSql
    engineSql
    route
    routeReason
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      maskingApplied
    }
  }
}
```

`CompileQueryInput` 欄位：

| 欄位 | 類型 | 說明 |
|-------|------|-------------|
| `query` | `String!` | 要編譯的數據平面 GraphQL 查詢 |
| `role` | `String!` | 要針對哪個角色的結構描述編譯 |
| `variables` | `JSON` | 變數綁定 |
| `flatSql` | `Boolean` | 傳回單一扁平化的 SQL 字串，而不是語義/引擎一對 |
| `flatCypher` | `Boolean` | 扁平化 Cypher 輸出 |
| `nodeOnlyCypher` | `Boolean` | 產生僅含節點的 Cypher（不含邊模式） |

---

## 關鍵輸入類型 {: #key-input-types }

### `SourceInput`

[tool-verified: `provisa/api/admin/types.py:590-611`]

| 欄位 | 類型 | 說明 |
|-------|------|-------|
| `id` | `String!` | 數據來源識別符 |
| `type` | `String!` | 連接器類型（例如 `postgres`、`files`、`openapi`） |
| `host` | `String` | |
| `port` | `Int` | |
| `database` | `String` | |
| `username` | `String` | |
| `password` | `String` | 明文或 `${secret:NAME}` 引用 |
| `path` | `String` | 檔案/CSV 數據來源的檔案系統路徑 |
| `federationHintsJson` | `String` | 用於數據儲存庫附加項的 JSON 物件（Snowflake 儲存庫/角色、Databricks http_path） |
| `changeSignal` | `String` | `ttl` \| `probe` \| `ttl_probe`（REQ-929） |
| `loadProtected` | `Boolean` | 僅按計劃重新整理（REQ-1141） |
| `offPeakWindow` | `String` | `HH:MM-HH:MM` 維護窗口 |
| `offPeakTz` | `String` | IANA 時區 |
| `cdc` | `SourceCdcConfigInput` | Kafka CDC 傳輸設定（REQ-824），包括如何存取其 schema registry（REQ-1951；參見 [Schema registry 身分驗證](subscriptions.md#schema-registry-authentication)）。`schemaRegistryPassword` 和 `schemaRegistryToken` 以密鑰庫引用傳回，從不以憑證傳回。 |

### `TableInput`

[tool-verified: `provisa/api/admin/types.py:745-800`]

核心的資料表註冊輸入。基礎欄位之外的關鍵欄位：

| 欄位 | 說明 |
|-------|-------|
| `materialize` | 在物化儲存中落地一份副本 |
| `mvRefreshInterval` | 重新整理間隔（秒） |
| `mvPersist` | `replace` \| `append` \| `upsert`（REQ-965） |
| `mvIncremental` | 增量維護（REQ-969） |
| `mvBitemporalMode` | 雙時態資料表的 `snapshot` \| `delta`（REQ-1162） |
| `mvCalendar` | 快照日曆名稱（REQ-962） |
| `mvGrain` | 快照粒度：`daily`、`weekly`、`monthly`、`annual`，或自定義的 `3WE` / `LFR`（REQ-962） |
| `viewSql` | 派生檢視的 SQL |
| `viewMetrics` | 聲明式的、由指標組成的檢視規範——與 `viewSql` 互斥（REQ-1318） |
| `dqContract` | YAML/JSON 數據品質契約文字（REQ-1443） |
| `queryTemplate` | Neo4j 資料表的 Cypher（REQ-1670） |
| `live` | SSE/Kafka 推送的即時投遞設定（REQ-565、REQ-813） |
| `discover` | 在註冊時從即時數據來源推斷欄位（REQ-252） |

### `RelationshipInput`

[tool-verified: `provisa/api/admin/types.py:804-826`]

| 欄位 | 說明 |
|-------|-------|
| `id` | 關係識別符 |
| `sourceTableId` | 虛擬資料表名稱（若已設定則為別名，否則為資料表名稱） |
| `targetTableId` | 虛擬資料表名稱；對於計算得出的關係為空 |
| `sourceColumn` | 源端的連線欄位 |
| `targetColumn` | 目標端的連線欄位 |
| `cardinality` | `one-to-one` \| `one-to-many` \| `many-to-one` \| `many-to-many` |
| `alias` | Cypher 邊標籤（例如 `WORKS_FOR`） |
| `graphqlAlias` | 源類型上的 GraphQL 欄位名稱 |
| `viaTable` | 多對多邊的聯結資料表名稱（REQ-1586） |
| `recordCandidate` | 同時寫入一條 `accepted` 狀態的 relationship_candidates 行 |

---

## 範例：註冊一張資料表 {: #example-registering-a-table }

```graphql
mutation {
  registerTable(input: {
    sourceId: "sales-pg"
    domainId: "sales"
    schemaName: "public"
    tableName: "orders"
    alias: "orders"
    columns: [
      { name: "id", visibleTo: ["public"], isPrimaryKey: true }
      { name: "customer_id", visibleTo: ["public"] }
      { name: "total", visibleTo: ["public"] }
    ]
  }) {
    success
    message
    code
  }
}
```

## 範例：建立一條 RLS 規則 {: #example-creating-an-rls-rule }

```graphql
mutation {
  upsertRlsRule(input: {
    tableId: "orders"
    roleId: "regional-analyst"
    filterExpr: "region = '{{user.region}}'"
  }) {
    success
    message
  }
}
```
