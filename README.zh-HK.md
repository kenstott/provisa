# Provisa

**接上你的資料庫。以 GraphQL、gRPC、SQL 或 MCP 查詢——經任何 API 或協定——5 分鐘內完成。**

Provisa 在跨你各個數據來源的聯結結果之上，提供每一個 API 介面（REST、GraphQL、SQL、gRPC、MCP 等）。它做得到，是因為它是一個**主動語意層**：對你整片數據領地的單一份定義——涵蓋各數據來源上的每一個網域、關係與政策，只不納入源頭系統本身——而這份定義既營運這片領地，也治理它。這份定義不是引擎可以參考的文件；它**就是**引擎。已註冊的網域與關係是唯一合法的聯結路徑，而存取政策被編譯進每一份查詢計劃。一個模型，三份工作：

- **定義**——網域、資料行與關係只宣告一次。那份宣告就是每個消費端所見的結構描述，也是任何查詢可走的唯一一組聯結路徑。
- **強制執行**——資料列層級安全、資料行遮罩、資料行可見性與查詢核准，在執行路徑上就地套用。沒有查詢能不經過它們就觸及數據，因此涵蓋是靠構造而完備，不是靠勤勉。
- **審計**——因為每一次請求都走同一條受治理的路徑，誰查了什麼、以什麼角色、對照哪份政策，都被一致地記錄。分散式追蹤、指標與記錄檔本身，也與你的業務數據並列註冊為可查詢的表。

一個受治理的核心，服務每一種語言與傳輸。以 **GraphQL、Cypher 或 SQL** 查詢；經 **pgwire、Bolt、gRPC、REST、Arrow Flight 或 JDBC** 消費。每一種查詢語言都降階為單一份中介表示，治理在那裡注入一次——因此政策不可能在語言之間漂移——而那份 IR 在輸出時再重新鎖定各數據來源的原生方言。加一種語言，是在共用核心之上加一個新前端，不是加一具新引擎。

這片領地既是分析式的，也是交易式的。跨來源讀取經聯邦層扇出；寫入與單一來源讀取直接路由到數據來源驅動程式——受同等治理，但具交易性且低於 100 毫秒。Arrow Flight 資料行式串流內建其中。

整個模型建於少數幾個原語之上——網域、關係、角色與政策。詞彙小，因此定義易於理解，也易於評估與審計：你可以讀完整組政策，然後知道它做什麼。Provisa 是一個輕量查詢編譯器，不是坐在數據路徑上的執行階段。它把一次請求轉成原生查詢、路由它們，然後讓開——這正是這片領地跑得動的原因。

這樣的設計支援兩種使用方式，而它們並不互斥：

- **作為現代化的鷹架**——把你的領地建模，讓 Provisa 為每個數據來源產生原生 SQL，然後擷取那些 SQL 並直接在目標系統中採用。Provisa 是過渡層，不是永久相依項。
- **作為長駐的政策強制執行基礎設施**——讓它留在原處，作為每一次查詢都走的受治理路徑，使定義、強制執行與審計，在這片領地存在多久就統一多久。

## 聯邦模型

整個模型歸結為兩份契約與兩項政策：數據來源歸約為單一型別系統之上的 2-D 表，查詢歸約為一份類 SQL 的 IR，可達性決定什麼被即時查詢、什麼被具體化，而一套鮮度策略治理每一份具體化副本與推導數據集。數據形狀進來，查詢形狀進來，治理落在聯結處，原生查詢出去。本節其餘部分逐件說明。

這個模型建立在一個歸約之上：每個數據來源都被表述為一組建於單一、泛化型別系統上的二維表。那就是一個數據來源要加入這片領地必須滿足的契約，而且對所有數據來源都是同一份契約。有些本來就合身——一張 MySQL 或 PostgreSQL 的表**就是**一個具型別的 2-D 關聯。有些經一次投影後合身：一份 GraphQL 查詢結果攤平之後就是一張表。有些與這個形狀格格不入——SPARQL 三元組儲存、Neo4j——但仍然行得通，因為使用者提供一份結果集為表格式的查詢；那份查詢就是轉接器。無論數據來源是什麼，這片領地看見的只有資料列、資料行與泛化型別，別無其他。接入一種新的數據來源，是去滿足那一份契約——有時需要一步人為介入——而不是寫一套訂製整合。

那個歸約在查詢側有一個孿生。SQL——跨其所有方言與怪癖——本質上就是對 2-D 數據集做分析的語言，這使得一種類 SQL 的形式成為查詢的天然通用目標。所以每一次請求，無論以哪種語言抵達，第一步就被降階為那份中介表示。有些降得乾淨——SQL 本身，甚至 GraphQL；有些很難——Cypher 的路徑與圖形語意要花真功夫——但全都做得到。在任何其他事情發生之前，先把每一次請求收攏進一份 IR，正是治理得以在恰好一個地方、對一種形式套用的原因，與它從哪種語言進來無關。

在那兩種一致形狀之上——表格式數據來源與單一查詢形式——這裡的聯邦同時意指即時查詢與倉儲——即一具像 Trino 的即時查詢引擎所涵蓋的同一片幅度，加上這類引擎所倚賴的具體化。把兩者統一起來的概念是**可達性**：對任一數據來源而言，引擎能否就地查詢它，還是必須先把它的數據具體化到某個可查詢之處？可達性把這片領地分割成即時查詢的部分與先複製一份的部分。

多數資料庫本來就帶有某種即時連結的概念——DuckDB `ATTACH`、PostgreSQL `postgres_fdw`、Databricks 外部連結。所以多數資料庫都能在某種程度上充當聯邦引擎。沒有一個是全面的：各自觸及一組特定數據來源並把其餘具體化，卻沒有一份說明何者屬何者的統一交代。這個模型把可達性明確化來補上那道缺口——逐數據來源定義一組方法，說明引擎能即時觸及什麼，並由排除法得出什麼必須被具體化。

剩下的是鮮度：對每個不可達的數據來源，它的具體化副本必須有多新？實務上這歸約為一小組策略——按需、按排程、按變更訊號（CDC、水位、快照）或釘住。逐數據來源選一個，就是整套鮮度政策。

分析數據集——推導表、彙總、轉換的輸出——收攏進同一個形狀。它們同樣必須以 IR 表述，而正因為如此，族系不是一套要另行維護的系統：自每個源頭系統到最終輸出的路徑，**就是**產生它的那份 IR，端到端可讀。建置它們把鮮度問題往外推一層——這個數據集是按排程重新整理、只在前置條件達成後才重新整理、以近即時方式持續重新整理，還是作為一份釘住的歷史快照？表達如何以及何時建置一個數據集的方式，同樣是那一小組可枚舉的選項，因此一個推導數據集所帶的建置政策，用的詞彙與一份數據來源副本完全相同。

維度模型是一項直接應用。一個星型結構描述的事實表與維度表，與其他任何分析數據集無異——一個維度是一份一致化、去重後的投影；一張事實表是一次聯結與彙總歸約到某個粒度——各自帶著自己的建置與鮮度政策。緩慢變化維度不需要特殊機制：一份釘住快照就是 Type 2 歷史，一次排程重建就是 Type 1。而且因為該結構描述定義於 IR 之中，而非實體綁定到某一個倉庫的表，同一組事實與維度定義可以重新鎖定目標——具體化到 Oracle、到 Databricks，或在一具 MPP 引擎之上維持虛擬——而毋須重新建模。模型產生星型結構描述；它不把它鎖死在某一具引擎上。

Data Vault 以同樣方式合身，只是早一層。它的樞紐是去重後的業務索引鍵數據集，它的連結是它們之間已註冊的關係，而它的衛星是僅附加、帶時間戳記的屬性數據集——即歷史記錄。一個衛星不過是走變更訊號鮮度策略的推導數據集：載入日期加 hashdiff 就是套用在描述性屬性上的 CDC，而僅附加歷史就是釘住快照策略。時間點表與橋接表是為查詢效能而建的進一步推導數據集。所以一個原始 vault 就是 IR 中的一組分析數據集，而一個星型結構描述是它之上的一次投影——兩者都是產生出來的，兩者都可跨引擎移植。這個模型不做的，是決定方法論：什麼成為樞紐、衛星的粒度、拆分策略。那些仍屬建模選擇；一旦做定，它們以可移植的 IR 存在，而不是焊死在某一個倉庫上的 ETL。

兩種模式都經**兩個一級捷徑**宣告，而非手寫檢視——即每個星型結構描述與 Data Vault 所賴以構成的原語，且保持方法論中立：

- **`entity`**——一份具索引鍵、去重、可選擇歷史化的數據來源投影。宣告一個實體索引鍵、屬性與一種歷史模式；Provisa 把它降階為一個具體化檢視，而當要求歷史時，降階為一個**雙時態 MV**（`scd2` → delta，`snapshot` → snapshot）。一個構造同時服務 Kimball **維度**（SCD1/SCD2）與 Data Vault 的**樞紐＋衛星**。
- **`fact`**——一次連往實體索引鍵的聯結，歸約到一個宣告的粒度，帶彙總量值。Provisa 把它降階為一個彙總 MV，加上通往那些實體的已註冊關係。一個構造同時服務星型**事實表**與 Data Vault 的**連結**（一個無量值的事實就是一個純索引鍵集連結）。

因為這個降階是純粹的——一份 `entity`／`fact` 規格恰好變成一位建模者原本要手寫的那些 MV、雙時態與關係定義——所以倉庫從頭到尾都是 IR，跨引擎重新鎖定目標而毋須重新建模。在管理 UI 中（一個供實體與事實使用的 **Model** 表單）或經管理 API（`registerEntity` / `registerFact`）宣告一個倉庫；模型**產生** Kimball 星型或 Data Vault，它不強加其一。

### 時間旅行

時間旅行是個簡單的想法——保留一列資料的每一個版本而非覆寫它，好讓你能問這份數據在過去任一時刻**曾經是**什麼。有別的是各引擎能多有效率地做到這件事，而這正是 Provisa 把它做成具體化檢視**定義**的一項屬性、而非儲存引擎的一項屬性的原因（REQ-1162）。宣告一次；它在任何具體化後端上都行得通。

讓它保持可移植的規則是**僅附加**：一個版本一旦寫入，就絕不被更新或刪除。以回寫一個「有效至」日期來讓一列資料退場——那個慣常的雙時態手法——需要 UPDATE，而許多引擎在聯邦儲存之上做不到（或做不便宜），所以 Provisa 不那樣做。取而代之，每一次重新整理都**附加**，而「時間 T 當下生效的是哪個版本」在讀取時自那份不可變記錄推導出來。附加恰好有兩種方式：

- **快照**——附加整份新鮮數據集，蓋上這次重新整理的系統時間戳記。不做差異比對；在每具引擎上都正確；儲存量每次重新整理增長一份完整副本。
- **Delta**——只附加變更的部分，加上已移除索引鍵的墓碑標記。這份 delta 由**引擎計算**（在 `INSERT … SELECT` 內的反聯結），絕不在 Provisa 中逐列摺疊。較小，且需要一個實體索引鍵。

系統時間（Provisa 記錄某個版本的時刻）以這種方式管理；有效時間（某項事實在業務上為真的時刻）由該檢視自己的 SELECT 提供並被保留。能提供更多的引擎——原生 Iceberg 快照、一個維護較少資料列的 MERGE——可以在同一份宣告背後被鎖定為效率目標；僅附加路徑是那條在任何地方都正確的底線。

讀取是透明的。針對一個雙時態 MV 的普通查詢，預設自附加記錄重建**當下**狀態；要在時間中旅行，送出一個 `X-Provisa-As-Of: <timestamp>` 標頭，整份查詢就會以這片領地在那一刻的樣子作答——在每一種基材上語意相同。可在管理 UI 中（一個 **Time Travel** 控制項：關閉／快照／delta 加上一個實體索引鍵）或經管理 API，為任一具體化檢視開啟它。

可達性加鮮度是一個通用的數據聯邦模型：一份定義，說明什麼是即時的、什麼被具體化，以及每份副本保持多新——獨立於任何單一引擎的觸及範圍。結果是擺脫專有鎖定。模型是可移植的；這片領地不會被今天恰好觸及最多數據來源的那家供應商俘虜。

## 功能

### 查詢介面

這些是你撰寫查詢所用的語言與結構化 API。各有自己的語法與語意；治理（RLS、遮罩、資料行可見性、關係強制執行）在它們之上一致套用，與由哪一種 wire 協定送達無關。

- **GraphQL**——逐角色的結構描述，帶欄位層級可見性、篩選、游標式分頁與彙總查詢（`count`、`sum`、`avg`、`min`、`max`）。受結構描述約束於已註冊關係——靠構造即結構有效，是通往一個正確簡單查詢的最快路徑。內含 Apollo APQ：查詢被雜湊並在伺服器端註冊；後續呼叫經 HTTP GET 只送出雜湊值，使回應可被 CDN 快取而毋須任何用戶端變更。低於可設定資料列門檻的查詢對照表，會以列舉類型呈現。
- **SQL**——在聯邦數據之上的完整 SQL；不受約束，比 GraphQL 更具表達力。寫標準 SQL——相關子查詢等等一應俱全——它就原封不動地跨數據來源執行。單一來源查詢完全繞過聯邦層（低於 100 毫秒）。
- **Cypher**——建於同一份聯邦結構描述之上的圖形查詢語言。把關係當作圖形邊來走訪；聯集數據來源；變長路徑。治理與 GraphQL 和 SQL 完全相同地套用。
- **gRPC 模型 API**——自已註冊結構描述自動產生 `.proto`；逐表的具型別查詢與插入 RPC，串流回應。與 GraphQL 同義地由結構描述驅動——註冊模型就是契約，protobuf 是 wire 編碼。與 Arrow Flight（那是一種資料行式串流傳輸）不同，這是一個完整的逐表查詢介面。
- **JSON:API**——位於 `/data/jsonapi/{table}` 的結構化查詢 API，設計上僅限 HTTP。支援 JSON:API 1.1：稀疏欄位集（`fields[table]=col1,col2`）、篩選運算式（`filter[field][op]=value`）、複合文件（`include=relation`）與排序。它不是一種通用查詢語言——一次查詢一張表，用標準化的篩選語法而非臨時查詢字串。
- **查詢語言探索器**——寫一份 GraphQL 查詢，就在側邊面板看見即時的**語意 SQL** 與 **Cypher** 翻譯；複製其一，或直接跳進 SQL 或圖形編輯器。一種實用的工作流程是先用 GraphQL 勾勒查詢片段，再把產生的 SQL 縫進複雜的檢視或報表。

探索器把一份 GraphQL 查詢與它即時的 SQL 和 Cypher 翻譯並排顯示：

![查詢語言探索器](docs/images/query-explorer.png)

同一份聯邦結構描述可作為一張即時圖形探索——網域與節點標籤、關係類型與變長走訪：

![圖形視覺化](docs/images/graph-view.png)

### 查詢組合工具

這些工具幫助你以上述語言撰寫查詢——它們本身不是查詢語言。

- **自然語言查詢**——由 Claude 驅動的 NL→SQL/Cypher/GraphQL 管線。以日常英文描述你想要什麼；管線以你選定的語言產出一份查詢，並在執行前提供一個互動式驗證迴圈。

![自然語言查詢](docs/images/natural-language.png)

### Wire 協定

這些是連線協定。SQL、GraphQL 與 Cypher 都乘載其上——wire 協定的選擇不改變查詢介面或治理行為。

- **pgwire**——任何 PostgreSQL 用戶端（psql、DBeaver、DataGrip、asyncpg、SQLAlchemy、pandas `read_sql`）都在連接埠 5439 上連線，就如同它是一台 Postgres 伺服器。只接受 SQL。完整治理管線照樣套用。`pg_catalog` 與 `information_schema` 自記憶體內目錄作答，因此結構描述瀏覽器毋須聯邦來回即可運作。TLS 可選。
- **Bolt（Neo4j）**——任何 Neo4j 用戶端（Neo4j Browser、Bloom、官方驅動程式）經 Bolt 協定連線，並針對聯邦圖形執行 Cypher。使用者持有的每個角色都呈現為一個 `provisa_<role>` 資料庫。與其他每一種傳輸同樣的治理。TLS 可選。
- **Arrow Flight**——經 gRPC 的高吞吐量資料行式串流；接受 GraphQL 或 SQL 作為查詢輸入。結果集無上限，無伺服器端具體化，毋須另建基礎設施。
- **JDBC**——以 `approved` 或 `catalog` 模式整合 BI 工具（Tableau、Power BI、DBeaver）。
- **WebSocket / SSE**——訂閱：近即時變更事件；後端：PG 原生、MongoDB 原生、CDC、輪詢。同樣經 Kafka 呈現。

### 數據來源

- **53 種數據來源類型**——PostgreSQL、MySQL、MongoDB、Cassandra、Elasticsearch、Neo4j、SPARQL 三元組儲存、Kafka、Google Sheets 等等，都經單一個 API；圖形與 RDF 數據來源是一級公民，不是轉接器
- **智慧路由**——單一來源查詢繞過聯邦（低於 100 毫秒）；多來源查詢經聯邦層路由——自備叢集，或使用內嵌工作處理程序
- **API 數據來源**——把 REST、GraphQL、gRPC、WebSocket 或 RSS 端點註冊為可查詢的表；內含 SPARQL 輔助工具；跨 API 數據來源與關聯式數據來源的聯邦聯結透明運作
- **遠端結構描述自省**——指向任何 GraphQL、OpenAPI 或 gRPC 端點；已載明的操作自動呈現為可查詢的表、圖形節點與邊，並在其上套用完整治理
- **檔案數據來源**——CSV、Parquet 與 SQLite 檔案作為可查詢的表；支援本機路徑與遠端物件儲存（`s3://`、`ftp://`、`sftp://`）
- **Kafka 整合**——主題作為唯讀表；查詢結果作為 Kafka 接收端
- **排程觸發器**——Cron 與間隔觸發器（APScheduler），可觸發 webhook、變更操作或 Kafka 接收端發佈
- **聯邦效能提示**——SQL 註解路由提示可覆寫自動路由決定

![數據來源](docs/images/data-sources.png)

數據來源、檔案與遠端端點可自 UI 註冊為受治理的表：

![表註冊](docs/images/table-registration.png)

### 安全與治理

- **資料列層級安全**——逐表、逐角色的 WHERE 子句注入
- **資料行遮罩**——逐資料行遮罩（regex、常數、截斷），可依角色略過
- **資料行預設值**——在插入／更新時注入的伺服器端靜態值或工作階段變數值；不在變更操作輸入類型中呈現
- **寫入權限**——逐資料行的變更操作存取控制（`writable_by`）
- **繼承角色**——角色遞迴地自父角色繼承 RLS、可見性與遮罩
- **受追蹤函式與 webhook**——DB 函式與外送 webhook 以具型別回傳形狀呈現為 GraphQL 變更操作
- **ABAC 核准掛鉤**——執行前授權掛鉤；webhook、gRPC 或 unix_socket 傳輸；逐表、逐數據來源或全域範圍；備援政策可設定
- **可插拔驗證**——Firebase、Keycloak、OAuth 2.0、simple（測試用）

![安全角色](docs/images/security-roles.png)

### 交付與效能

- **作為已記錄轉換的具體化檢視**——一個 MV 擷取產生它的那次轉換：它的聯結形狀或 SQL、它據以建置的逐數據來源輸入訊號（Iceberg 快照、RDB 水位），以及註冊時的一次決定性檢查。因為轉換被記錄下來，查詢（或子運算式）可被透明地改寫到一個新鮮的 MV 之上——結構式聯結樣式比對並支援部分比對，因此一個只涵蓋部分聯結的 MV 仍然適用，其餘聯結會被保留
- **熱表內嵌**——小型且頻繁聯結的查詢對照表，直接在查詢計劃中以 VALUES CTE 內嵌，消除維度數據的跨來源來回
- **查詢快取**——依角色＋RLS 分割的 Redis 結果快取；內含 APQ 雜湊快取
- **可觀測性即數據**——分散式追蹤、指標與記錄檔經 OpenTelemetry 收集，壓實成 S3 上的 Iceberg，並自動註冊為聯邦結構描述中可查詢的表（`traces`、`metrics`、`logs`、`queries`）；用 SQL、GraphQL 或 Cypher 與你的業務數據並排查詢它們——把一張 `customers` 表聯結到 `queries` 表，就能看出誰跑了什麼、跑了多久

### 管理與整合

- **管理 API**——位於 `/admin/graphql` 的 GraphQL；設定上傳／下載、關係編輯、查詢核准
- **報表檢視器**——`/admin/reports` 列出內建的 ops 網域管理檢視，以及任何已註冊的自訂報表；需要 `observability` 能力
- **表預覽**——每張已註冊的表都有一個伺服器分頁的受治理數據檢視器，帶下推篩選、多層群組依據與 CSV 匯出
- **GraphQL Voyager**——以實體關係圖呈現的互動式、依角色設限結構描述視覺化
- **LLM 關係探索**——由 Claude 驅動的外部索引鍵候選建議
- **Python 用戶端**——`pip install provisa-client`；GraphQL/SQL → DataFrame，Arrow Flight → pyarrow Table，SQLAlchemy 方言，ADBC 支援
- **數據擷取**——把 JSON 事件數據推入平台的 HTTP 端點
- **Hasura v2 / DDN 匯入**——把 Hasura v2 中繼數據或 DDN supergraph YAML 轉換為 Provisa 設定
- **Apollo Federation**——把 Provisa 呈現為一個 Apollo Federation v2 子圖

以實體關係圖視覺化的依角色設限結構描述（GraphQL Voyager）：

![結構描述 Voyager](docs/images/schema-voyager.png)

關係被註冊、核准，並作為唯一合法的 JOIN 路徑強制執行：

![關係](docs/images/relationships.png)

## 安全模型

這裡正是「就在每次查詢已經要走的那條路徑上」不再只是一句口號之處。Provisa 跨每一種查詢語言（GraphQL、SQL、Cypher）與每一種傳輸（REST、gRPC、Arrow Flight、JDBC、pgwire、Bolt、WebSocket）強制執行一套多層安全模型。治理一致地套用——不存在繞過它的查詢路徑。涵蓋是靠構造而完備，不是靠勤勉：加入一個數據來源、資料行或關係，每一層都自動套用到它，沒有什麼需要記得去註冊。

各層依序套用。一次請求必須通過每一層，下一層才會被評估。

### 第 0 層——自省篩選

呈現給某個角色的結構描述與目錄，只包含其 `domain_access` 清單中的表，以及通過逐資料行 `visible_to` 規則的資料行。在角色存取範圍之外的物件，在探索時就不可見——它們無法被查詢、被自動完成，也無法被推斷存在。這適用於 GraphQL 結構描述、SQL 目錄，以及查詢編輯器的結構描述瀏覽器。

### 第 1 層——公開存取

位於沒有 `domain_access` 限制之網域中的表，對所有已驗證身分可見，毋須額外設定。對真正公開的數據零阻力。

### 第 2 層——網域存取

每個角色帶著一份網域 ID 的 `domain_access` 清單。觸及那些網域以外之表的查詢，會在執行前被拒絕。這是粗粒度的所有權邊界——一個 HR 角色無論 SQL 怎麼寫，都觸及不到財務表。

### 第 3 層——資料列層級安全

在網域存取獲確認之後，逐表、逐角色的 `WHERE` 述詞會在執行時注入每一份 `SELECT`。這些述詞針對原始數據求值。一位區域經理查詢一張共用的訂單表，即使下 `SELECT *`，也只看見自己區域的資料列。

### 第 4 層——資料行可見性與遮罩

`visible_to` 清單不含請求角色的資料行，會自查詢輸出中剝除。帶遮罩規則的資料行，其值在結果離開伺服器之前被替換——regex 遮蔽、常數替換或截斷。遮罩在所有查詢語言與輸出格式中都套用。

### 第 5 層——述詞防護

被遮罩的資料行不得出現在 `WHERE` 與 `HAVING` 子句中。若無此層，即使輸出被遮罩，呼叫端也能在篩選中二分搜尋而推斷出未遮罩的值。拒絕在查詢剖析時就強制執行，早於執行。

### 關係治理

SQL 中的 JOIN 條件，必須符合表與表之間一項已註冊、已核准的關係。未經核准的聯結會被拒絕。每項關係都帶一段供人閱讀的理由與說明——向使用者與自主代理說明某條走訪路徑為何存在的指引。這是治理政策，不是一道硬性安全邊界：無論聯結結構如何，第 2–5 層都成立，因此蓄意規避不會揭露該角色透過兩次分開的查詢也觸及不到的數據。規避嘗試會被記錄且可審計。

---

這些層彼此組合。一個同時具備網域存取、RLS 與遮罩資料行的角色，五項約束同時生效。加入一個新的數據來源、資料行或關係，毋須更新每一條規則——各層獨立設定，並自動套用到任何觸及受治理物件的查詢。

### macOS

1. 下載 [Provisa-macOS.dmg](https://provisa.dev/dl/macos)（一律為最新版本）
2. 把 **Provisa.app** 拖到 `/Applications`，然後按兩下啟動
3. 首次啟動會完成一次性設定（約 2 分鐘，毋須網際網路）
4. 打開終端機：

```bash
provisa start   # start all services
provisa open    # open the UI in your browser
```

### Linux

1. 下載 [Provisa-linux-x86_64.AppImage](https://provisa.dev/dl/linux)（一律為最新版本）
2. 賦予它執行權限並執行——首次啟動會完成一次性設定（毋須網際網路）：

```bash
chmod +x Provisa-*-linux-x86_64.AppImage
./Provisa-*-linux-x86_64.AppImage
provisa start && provisa open
```

### Windows

1. 下載 [Provisa-windows-x64.exe](https://provisa.dev/dl/windows)（一律為最新版本）
2. 執行安裝程式——毋須管理員權限
3. 自「開始」功能表開啟 **Provisa First Launch**——完成一次性設定（約 5 分鐘，毋須網際網路）
4. 打開一個新的終端機：

```bash
provisa start
```

### 第一個查詢

在本機開發中（`PROVISA_MODE=test`），毋須憑證。在生產環境中，以 Bearer 權杖驗證——角色會自動自其中抽出。

```bash
# Local dev — no auth required, role defaults to admin
curl -X POST http://localhost:8001/data/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "{ orders { id amount region } }"}'

# Ad-hoc SQL works the same way
curl -X POST http://localhost:8001/data/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT id, amount, region FROM orders"}'

# Production — authenticate with a Bearer token; role is derived from the token
curl -X POST https://provisa.example.com/data/graphql \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ orders { id amount region } }"}'
```

### JDBC（Tableau、DBeaver、Power BI）

下載 [provisa-jdbc.jar](https://provisa.dev/dl/jdbc)（一律為最新版本），並把它加入你的 BI 工具驅動程式路徑。

```text
jdbc:provisa://localhost:8815
```

以你的 Provisa 使用者名稱與密碼驗證——伺服器指派你的角色。

- **`catalog` 模式**——完整結構描述可見；搭配目錄工具使用（Collibra、Atlan、DBeaver）

Tableau 與 Power BI 的設定步驟見 [docs/integrations.md](docs/integrations.md)。

### PostgreSQL Wire 協定（pgwire）

Provisa 在連接埠 5439 上說 PostgreSQL wire 協定。任何能連上 Postgres 的用戶端都能連上 Provisa——毋須驅動程式、毋須轉接器、毋須改動既有工具。

**PostgreSQL 使用者名稱選定 Provisa 角色。** 在 `provider: none`（信任模式）下，密碼被忽略，任何已設定的角色名稱都可作為使用者名稱接受——以 `analyst`、`admin` 或任何角色連線，即可看見該角色所見的受治理數據檢視。在 `provider: simple` 下，密碼經 bcrypt 驗證。其他提供者（`firebase`、`keycloak`、`oauth`）不支援 pgwire。

```bash
# psql — connect as analyst role
psql -h localhost -p 5439 -U analyst

# psql — connect as admin role
psql -h localhost -p 5439 -U admin

# asyncpg (Python) — role = username, password ignored in trust mode
conn = await asyncpg.connect(host="localhost", port=5439, user="analyst", password="x")
rows = await conn.fetch("SELECT id, amount FROM orders WHERE region = 'west'")

# SQLAlchemy
engine = create_engine("postgresql+psycopg2://analyst:x@localhost:5439/provisa")

# pandas
df = pd.read_sql("SELECT * FROM orders", engine)
```

所有查詢都跑過完整治理管線——網域存取、RLS、遮罩與述詞防護的套用方式，與 GraphQL 和 REST 完全相同。結構描述瀏覽器（DBeaver、DataGrip、pgAdmin）開箱即用：`pg_catalog` 與 `information_schema` 查詢自一份限定於該角色網域存取的記憶體內目錄作答，因此使用者只看見他們獲准查詢的表與資料行。

DataGrip 經 pgwire 瀏覽受治理結構描述及其外部索引鍵圖——毋須驅動程式、毋須轉接器：

![以 pgwire 在 DataGrip 中的 Provisa](docs/images/pgwire-datagrip.png)

設定 `PROVISA_PGWIRE_CERT` 與 `PROVISA_PGWIRE_KEY` 即啟用 TLS。連接埠可經 `PROVISA_PGWIRE_PORT` 設定（預設 `5439`）。

### Bolt（Neo4j Wire 協定）

Provisa 同樣說 Neo4j 的 **Bolt** 協定，因此圖形原生工具可直接連線，並針對聯邦圖形執行 Cypher——毋須匯出，毋須另一套圖形資料庫。把 **Neo4j Browser** 或 **Bloom** 指向 Provisa，即可在套用同樣治理（網域存取、RLS、遮罩）的前提下跨數據來源走訪關係。

Neo4j Browser 針對 Provisa 執行 Cypher——節點標籤、關係類型與屬性索引鍵，全部直接來自已註冊的結構描述：

![以 Bolt 在 Neo4j Browser 中的 Provisa](docs/images/bolt-neo4j-browser.png)

設定 `PROVISA_BOLT_PORT` 即啟用它（Neo4j 的預設值是 `7687`）。TLS 以 `PROVISA_BOLT_CERT` 與 `PROVISA_BOLT_KEY` 啟用。已驗證使用者持有的每個 Provisa 角色，都呈現為一個可選取的 `provisa_<role>` 資料庫（上圖中的 `provisa_admin` 選擇器）——選定其一會把工作階段收窄到該角色的網域權限；使用者永遠不可能超出他們持有的角色。

### Python 用戶端

```bash
pip install provisa-client                       # core
pip install "provisa-client[pandas]"             # + DataFrame support
pip install "provisa-client[sqlalchemy]"         # + SQLAlchemy dialect
pip install "provisa-client[adbc]"               # + ADBC over Arrow Flight
```

```python
from provisa_client import ProvisaClient, connect

# GraphQL → DataFrame
client = ProvisaClient("http://localhost:8001", username="alice", password="secret")
df = client.query_df("{ orders { id amount region } }")

# SQL → DataFrame
df = client.query_df("SELECT id, amount, region FROM orders WHERE region = 'west'")

# Arrow Flight → pyarrow Table (high-throughput columnar)
table = client.flight("{ orders { id amount region } }")

# DB-API 2.0 (PEP 249) — GraphQL or SQL, detected automatically
with connect("http://localhost:8001", username="alice", password="secret") as conn:
    cur = conn.cursor()

    # GraphQL
    cur.execute("{ orders { id amount region } }")
    rows = cur.fetchall()

    # SQL (routed through governance engine — RLS and masking applied)
    cur.execute("SELECT id, amount FROM orders WHERE region = %s", ("west",))
    rows = cur.fetchall()

# SQLAlchemy dialect — provisa+http:// or provisa+https://
from sqlalchemy import create_engine, text
import pandas as pd

engine = create_engine("provisa+http://alice:secret@localhost:8001")

# pandas read_sql — GraphQL or SQL
df = pd.read_sql("{ orders { id amount region } }", engine)
df = pd.read_sql("SELECT id, amount, region FROM orders WHERE region = 'west'", engine)

# raw execute
with engine.connect() as conn:
    rows = conn.execute(text("SELECT id, amount FROM orders")).fetchall()

# role + mode URL parameters (mode=catalog for arbitrary SQL)
engine = create_engine(
    "provisa+http://alice:secret@localhost:8001?role=analyst&mode=catalog"
)

# ADBC — Arrow-native streaming via Flight
from provisa_client.adbc import adbc_connect
with adbc_connect("http://localhost:8001", user="alice", password="secret") as conn:
    with conn.cursor() as cur:
        cur.execute("{ orders { id amount } }")
        table = cur.fetch_arrow_table()
```

完整參照見 [docs/python-client.md](docs/python-client.md)。

## 文件

| 主題 | 文件 |
| --- | --- |
| 開發者快速上手（自原始碼執行） | [docs/quickstart.md](docs/quickstart.md) |
| 完整 YAML 設定參照 | [docs/configuration.md](docs/configuration.md) |
| 端點參照（GraphQL、REST、Flight、gRPC） | [docs/api-reference.md](docs/api-reference.md) |
| 系統設計與元件圖 | [docs/architecture.md](docs/architecture.md) |
| 安全模型（RLS、遮罩、驗證） | [docs/security.md](docs/security.md) |
| 密鑰儲存與 `${secret:NAME}` 參照 | [docs/secrets.md](docs/secrets.md) |
| 業務詞彙表與詞條策展 | [docs/glossary.md](docs/glossary.md) |
| 環境（dev / staging / prod） | [docs/environments.md](docs/environments.md) |
| 支援的數據來源類型 | [docs/sources.md](docs/sources.md) |
| SSE 訂閱 | [docs/subscriptions.md](docs/subscriptions.md) |
| JDBC、BI 工具、Arrow Flight 用戶端、Apollo Federation | [docs/integrations.md](docs/integrations.md) |
| Python 用戶端（`provisa-client`） | [docs/python-client.md](docs/python-client.md) |
| 管理 API | [docs/admin.md](docs/admin.md) |
| 部署（Docker Compose、Kubernetes、macOS） | [docs/deployment.md](docs/deployment.md) |
| Hasura v2 / DDN 匯入 | [docs/import.md](docs/import.md) |
| 發佈流程（alpha/beta/stable 標籤） | [docs/releasing.md](docs/releasing.md) |

## 規模設定

Provisa 內含一具供多來源查詢使用的內建聯邦引擎。首次啟動時你選定一個 RAM 預算；Provisa 會自動推導本機聯邦工作處理程序的數量。

| 主機 RAM | 工作處理程序 | 典型工作負載 |
| --- | --- | --- |
| < 24 GB | 0 | 開發、單一來源查詢、小團隊 |
| 24–47 GB | 1 | 小團隊、中等程度的跨來源查詢 |
| 48–95 GB | 2 | 部門級部署、BI 與筆記本混合使用 |
| 96 GB+ | 4 | 大型部門、高併發聯邦 |

工作處理程序數量可隨時變更：編輯 `~/.provisa/config.yaml`（`federation_workers: N`）並執行 `provisa restart`。設為 `0` 即以僅協調模式執行（單節點）。

### 擴展到單機以外

**水平擴展**——在一個負載平衡器後方執行多個 Provisa 執行個體。每個執行個體都是一套功能完整的系統。所有執行個體必須指向同一個設定 DB（在次要機器上設定 `CONFIG_DB_HOST`），並可選擇指向一個共用的 Redis 執行個體（`REDIS_URL`）以取得統一快取。多數查詢會透明地分散；非常大的跨來源聯結可能超出單一執行個體的資源，需要更大的機器或一個外部聯邦叢集。

**共用 Redis**——在每個執行個體上設定 `REDIS_URL` 指向一個外部 Redis。共用 Redis 意味著來自一個執行個體的快取項目對所有執行個體都可用，提升整個叢集的命中率。

**自備聯邦叢集**——把 Provisa 指向一個既有的外部聯邦叢集，取代內嵌工作處理程序。建議用於大規模或雲端部署；設定方式見 [docs/deployment.md](docs/deployment.md)。

## 授權

Business Source License 1.1（未經修改，依 MariaDB 的 Licensor 承諾）。每個
已發佈版本在其公開發佈的第 4 週年轉為 Change License（GPL v2.0 或
更新版本）；當前與近期的程式碼維持在 BSL 之下。
超出 Additional Use Grant 門檻（少於 100 名員工／承包商，且前一年度營收
低於 100 萬美元）的生產使用，需要商業授權。見 [LICENSE](LICENSE)。

Licensor 不同意將本作品用於 AI/ML 訓練。見
[NOTICE](NOTICE)、[ai.txt](ai.txt) 與 [robots.txt](robots.txt)。商業或
AI 訓練授權請洽：<kennethstott@gmail.com>
