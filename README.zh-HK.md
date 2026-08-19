# Provisa

**連接你的資料庫。以 GraphQL、gRPC、SQL 或 MCP 查詢——透過任何 API 或協定——只需 5 分鐘。**

Provisa 在你所有來源的聯合結果之上，提供每一種 API 介面（REST、GraphQL、SQL、gRPC、MCP 等等）。它之所以能做到這一點，是因為它是一個**主動語義層 (active semantic layer)**：一份針對你的數據體系 (data estate) 的單一定義——涵蓋你所有來源的每一個網域、關聯及政策，僅排除來源系統本身——它既運作這個體系，也治理這個體系。這份定義不是引擎可以參考的文件；它*就是*引擎。已註冊的網域及關聯是唯一合法的 join 路徑，而存取政策會被編譯進每一個查詢計劃之中。一個模型，三項工作：

- **定義 (Define)**——網域、欄位及關聯只需宣告一次。該宣告即是每個使用方所見的結構描述 (schema)，也是任何查詢可以採取的唯一一組 join 路徑。
- **強制執行 (Enforce)**——行級安全 (row-level security)、欄位遮罩、欄位可視性及查詢批核，均在執行路徑上內嵌套用。沒有任何查詢能繞過它們而觸及數據，因此涵蓋範圍是由結構保證的完整性，而非仰賴人手謹慎。
- **稽核 (Audit)**——由於每一項請求都經過同一條受治理的路徑，誰在何種角色下查詢了什麼、依據哪一項政策，都會被統一記錄下來。分散式追蹤 (trace)、指標及記錄本身也會被註冊為可查詢的資料表，與你的業務數據並列。

一個受治理的核心，服務每一種語言及傳輸方式。以 **GraphQL、Cypher 或 SQL** 查詢；透過 **pgwire、Bolt、gRPC、REST、Arrow Flight 或 JDBC** 使用。每一種查詢語言都會降轉 (lower) 為單一的中介表示 (IR)，治理邏輯只在此處注入一次——因此政策不會在不同語言之間出現落差——而該 IR 在輸出時會重新對應至各個來源的原生方言。新增一種語言，是在共享核心之上新增一個前端，而非新增一個引擎。

這個數據體系既是分析型的，也是交易型的。跨來源讀取會透過聯邦層扇出；寫入及單一來源讀取則直接路由至來源驅動程式——受到同等的治理，但屬於交易型且延遲低於 100 毫秒。Arrow Flight 欄式串流是內建功能。

整個模型是由少數幾個基本構件組成——網域、關聯、角色及政策。詞彙量小，因此這份定義易於理解，也易於評估與稽核：你可以直接讀懂政策集並知道它做了什麼。Provisa 是一個輕量級查詢編譯器，而非坐落在數據路徑上的執行期系統。它把一項請求轉換為原生查詢、路由該查詢，然後功成身退——這正是這個數據體系效能良好的原因。

這樣的設計支援兩種使用方式，而且兩者並不互斥：

- **作為現代化的鷹架**——為你的數據體系建立模型，讓 Provisa 為每個來源產生原生 SQL，然後擷取該 SQL，直接在目標系統中採用。Provisa 是過渡層，而非永久性的依賴。
- **作為永久性的政策強制執行基礎設施**——將其保留原地，作為每一項查詢都會經過的受治理路徑，讓定義、強制執行及稽核在該數據體系存在期間持續保持一致。

## 聯邦模型

整個模型可以歸結為兩份合約及兩項政策：來源化約為單一型別系統之上的二維資料表，查詢化約為單一的類 SQL IR，可達性 (reachability) 決定什麼是即時查詢、什麼是先具體化，而新鮮度策略則治理每一份具體化副本及衍生數據集。數據形狀輸入、查詢形狀輸入、在 join 處治理、原生查詢輸出。本節其餘部分將逐一說明每個環節。

該模型建立在一個化約之上：每一個來源都被表示為單一、通用型別系統之上的一組二維資料表集合。這是一個來源要加入該數據體系必須符合的合約，而且對所有來源都是同一份合約。有些來源本身已經符合——一個 MySQL 或 PostgreSQL 資料表*本身就是*一個具型別的二維關聯。有些來源經投影後即可符合：一個 GraphQL 查詢結果，一經攤平，即成為一個資料表。有些來源在形狀上並不相符——SPARQL 三元組儲存庫、Neo4j——但依然可行，因為使用者提供的查詢，其結果集是表格式的；該查詢本身就是轉接器。無論來源為何，該數據體系所見的只有資料列、欄位及通用型別，僅此而已。導入一種新類型的來源，就是去符合這唯一一份合約，有時需要人手介入的一個步驟，而不是撰寫一套訂製整合。

那個化約在查詢面有一個對應版本。SQL——涵蓋其所有方言及怪癖——本質上就是針對二維資料集進行分析的語言，這使得類 SQL 的形式成為查詢的自然通用目標。因此，每一項請求，無論以何種語言送達，第一步都會被降轉為這個中介表示。有些語言降轉得很乾淨——SQL 本身，甚至 GraphQL 也是；有些則相當困難——Cypher 的路徑及圖形語義需要真正下功夫——但全部都可以做到。在任何其他事情發生之前，先把每一項請求匯入單一 IR，正是讓治理能夠只在一個地方、對一種形式套用的關鍵，無論它是從哪一種語言送達的。

在這兩種統一形狀——表格式來源及單一查詢形式——之上，這裡所指的聯邦同時涵蓋即時查詢與數據倉儲——即像 Trino 這類即時查詢引擎所涵蓋的範圍，再加上此類引擎所依賴的具體化功能。將兩者統一起來的概念是**可達性 (reachability)**：對任何一個來源而言，引擎能否就地查詢它，還是必須先將其數據具體化到某個可查詢的地方？可達性把整個數據體系劃分為「即時查詢」與「先複製」兩部分。

大多數資料庫本身已經具備某種即時連結的概念——DuckDB 的 `ATTACH`、PostgreSQL 的 `postgres_fdw`、Databricks 的外部連結。因此大多數資料庫都能在某種程度上充當聯邦引擎。但沒有一個是全面的：每一種都只能觸及一組特定的來源，其餘則需要具體化，而且沒有單一的說明能界定何者屬於哪一類。這個模型把這個缺口補上——透過為每個來源明確定義一組方法，說明引擎能即時觸及什麼，而經由排除法，得知什麼必須被具體化。

剩下的問題是新鮮度：對每一個不可即時觸及的來源而言，它的具體化副本需要多新？實務上這化約為一組不多的策略：按需求、按排程、按變更信號（CDC、水位標記、快照），或釘選 (pinned)。為每個來源選擇其一，即構成整份新鮮度政策。

分析型數據集——衍生資料表、彙總資料、轉換的輸出——也可以摺疊進同一個形狀之中。它們同樣必須以 IR 表示，而正因如此，血緣 (lineage) 不必是另一個需要維護的獨立系統：從每一個來源系統到最終輸出的路徑，*就是*產生該輸出的那個 IR，端到端可讀。建構這些數據集，會把新鮮度問題往後退一步再提出——這個數據集是按排程重新整理，還是只在其前置條件滿足時才重新整理，或是以近乎即時的方式持續刷新，抑或作為一份釘選的歷史快照？表達如何、何時建構一個數據集的方式，與來源副本所用的是同一組不多的可枚舉集合，因此一個衍生數據集所承載的建構政策，用的正是來源副本相同的詞彙。

維度模型是一個直接的應用。星型結構描述 (star schema) 的事實表與維度表，就跟其他分析型數據集一樣——一個維度是一個經一致化、去重的投影；一個事實表則是化約至某個粒度 (grain) 的 join 與彙總——各自帶有自己的建構與新鮮度政策。緩慢變化的維度 (slowly changing dimension) 不需要特別的機制：釘選快照即是 Type 2 歷史記錄，排程重建即是 Type 1。而且，因為該結構描述是以 IR 定義，而非實體綁定在某一個數據倉儲的資料表上，同一組事實表與維度表定義可以重新對應——具體化於 Oracle、具體化於 Databricks，或在某個 MPP 引擎上維持虛擬狀態——而不需要重新建模。這個模型會產生星型結構描述；它不會把你鎖死在某一個引擎上。

Data Vault 也以同樣的方式相容，只是早一個層次。它的 hub 是去重的業務索引鍵數據集，它的 link 是已註冊在這些數據集之間的關聯，而它的 satellite 則是僅供插入、帶有時間戳記的屬性數據集——即歷史記錄。一個 satellite 只不過是採用變更信號新鮮度策略的一個衍生數據集：載入日期 (load-date) 加上 hashdiff，正是 CDC 套用在描述性屬性上；而僅供插入的歷史記錄，正是釘選快照策略。時點 (point-in-time) 表及橋接 (bridge) 表則是為了查詢效能而建構的進一步衍生數據集。因此，一個原始 vault 就是 IR 中的一組分析型數據集，而星型結構描述則是由其投影而來——兩者都是產生出來的，兩者都能跨引擎移植。這個模型不會替你決定方法論：什麼應該成為一個 hub、一個 satellite 的粒度、拆分策略。這些仍然是建模上的決策；一旦決定，它們就會以可移植的 IR 形式存在，而不是焊死在某一個數據倉儲上的 ETL。

這兩種模式都是透過**兩個一級 (first-class) 捷徑**來宣告的，而不是手寫檢視——這是每一個星型結構描述及 Data Vault 所賴以建構的基本構件，且保持方法論中立：

- **`entity`**——一個來源的具索引鍵、去重、可選具歷史化的投影。宣告一個實體索引鍵、屬性及一種歷史模式；Provisa 會將其降轉為一個具體化檢視，若要求歷史記錄，則降轉為一個**雙時態 (bitemporal) MV**（`scd2` → delta，`snapshot` → snapshot）。單一構件同時服務 Kimball 的**維度** (SCD1/SCD2) 及 Data Vault 的 **hub + satellite**。
- **`fact`**——對實體索引鍵的一個 join，化約至一個已宣告的粒度，並附有彙總量度。Provisa 會將其降轉為一個彙總 MV，加上對應這些實體的已註冊關聯。單一構件同時服務星型的**事實表**及 Data Vault 的 **link**（沒有量度的事實即是純粹的索引鍵集合 link）。

由於這個降轉過程是純粹的——一份 `entity`/`fact` 規格，會精確地變成建模人員原本要手寫的那個 MV、雙時態及關聯定義——因此這個數據倉儲徹底由 IR 構成，並能跨引擎重新對應而不需重新建模。在管理介面中宣告一個數據倉儲（一份用於實體及事實的 **Model** 表單），或透過管理 API（`registerEntity` / `registerFact`）宣告；這個模型是*產生*出 Kimball 星型或 Data Vault，而不是強加其一。

### 時光回溯 (Time travel)

時光回溯是一個簡單的概念——保留每一列的每一個版本，而不是覆寫它，這樣你就能查詢數據在過去任何一個時刻的樣貌。不同之處在於各個引擎能以多高的效率做到這一點，這正是為什麼 Provisa 把它做成具體化檢視**定義**的一個屬性，而不是儲存引擎的屬性（REQ-1162）。宣告一次；它就能在任何具備具體化能力的後端上運作。

讓它得以移植的規則是**僅供附加 (append-only)**：一個版本一旦寫入，就永不更新或刪除。透過寫回一個「有效至 (valid-to)」日期來淘汰一列——這是常見的雙時態手法——需要一個 UPDATE，而許多引擎在聯邦式存放區之上無法以低成本（甚至完全無法）執行此操作，所以 Provisa 不這樣做。取而代之，每一次重新整理都會**附加**，而「在時間點 T 生效的是哪個版本」則是在讀取時從不可變的記錄推導出來。附加的方式恰好有兩種：

- **Snapshot**——附加整份最新數據集，並蓋上本次重新整理的系統時間戳記。無需比對差異；在任何引擎上皆正確；儲存空間隨每次重新整理增加一整份副本。
- **Delta**——只附加變更的部分，加上已移除索引鍵的墓碑標記 (tombstone)。此差異是**由引擎計算**的（在一個 `INSERT … SELECT` 內部進行反連接 (anti-join)），絕不會在 Provisa 中逐列摺算。體積較小，且需要一個實體索引鍵。

系統時間（Provisa 記錄某個版本的時刻）以這種方式管理；有效時間（一項事實在業務上成立的時刻）則由該檢視自身的 SELECT 提供並保留。提供更多能力的引擎——原生 Iceberg 快照、維護較少資料列的 MERGE——可以在同一個宣告之下被鎖定以取得效率；僅供附加路徑則是在任何地方都正確的底線。

讀取是透明的。針對一個雙時態 MV 的一般查詢，預設會從附加記錄重建**目前**狀態；若要進行時光回溯，送出一個 `X-Provisa-As-Of: <timestamp>` 標頭，整項查詢就會依該數據體系在那個時刻的狀態作答——在任何底層上語義都相同。可在管理介面中為任何具體化檢視開啟此功能（一個**Time Travel** 控制項：off / snapshot / delta，加上一個實體索引鍵），或透過管理 API 開啟。

可達性加上新鮮度，構成一個通用的數據聯邦模型：一份定義說明什麼是即時的、什麼是具體化的，以及每份副本要維持多新——不受限於任何單一引擎的觸及範圍。其結果是擺脫專有廠商鎖定。這個模型是可移植的；這個數據體系不會被目前恰好能觸及最多來源的那個供應商所俘虜。

## 功能特色

### 查詢介面

這些是你用來撰寫查詢的語言及結構化 API。每一種都有自己的語法及語義；治理（RLS、遮罩、欄位可視性、關聯強制執行）在所有這些介面上均一致套用，無論由哪一種線路協定 (wire protocol) 傳遞。

- **GraphQL**——按角色劃分的結構描述，具備欄位層級可視性、篩選、以游標為基礎的分頁，以及彙總查詢（`count`、`sum`、`avg`、`min`、`max`）。結構受限於已註冊的關聯——因結構而天生有效，是撰寫正確簡單查詢最快的路徑。內建 Apollo APQ：查詢會被雜湊並在伺服端註冊；後續呼叫僅以 HTTP GET 傳送該雜湊值，讓回應可被 CDN 快取，且無需任何用戶端變更。低於可設定資料列數門檻的查閱資料表會以列舉 (enum) 型別公開。
- **SQL**——針對聯邦數據的完整 SQL；不受約束，比 GraphQL 更具表達力。撰寫標準 SQL——包括相關子查詢在內——即可原樣跨來源執行。單一來源查詢會完全繞過聯邦層（延遲低於 100 毫秒）。
- **Cypher**——在同一套聯邦結構描述之上的圖形查詢語言。以圖形邊來遍歷關聯；跨來源聯集；可變長度路徑。治理與 GraphQL 及 SQL 完全相同。
- **gRPC model API**——由已註冊結構描述自動產生 `.proto`；每個資料表附有具型別的查詢及插入 RPC，並支援串流回應。以與 GraphQL 相同的意義做到結構描述驅動——註冊模型即是合約，protobuf 只是線路編碼。與 Arrow Flight（一種欄式串流傳輸）不同，這是一套完整的逐資料表查詢介面。
- **JSON:API**——位於 `/data/jsonapi/{table}` 的結構化查詢 API，依設計僅限 HTTP。支援 JSON:API 1.1：稀疏欄位集 (`fields[table]=col1,col2`)、篩選運算式 (`filter[field][op]=value`)、複合文件 (`include=relation`) 及排序。這不是通用查詢語言——一次只查詢一個資料表，使用標準化篩選語法，而非隨意組成的查詢字串。
- **Query Language Explorer**——撰寫一段 GraphQL 查詢，並在側邊面板中即時檢視 **Semantic SQL** 及 **Cypher** 的翻譯結果；可複製其一，或直接跳到 SQL 或 Graph 編輯器。一個實用的工作流程是先在 GraphQL 中草擬查詢片段，再把產生的 SQL 拼接進複雜的檢視或報表中。

Explorer 會將一段 GraphQL 查詢與其即時的 SQL 及 Cypher 翻譯並列顯示：

![Query Language Explorer](docs/images/query-explorer.png)

同一套聯邦結構描述也能以即時圖形方式探索——網域及節點標籤、關聯類型，以及可變長度遍歷：

![Graph Visualization](docs/images/graph-view.png)

### 查詢組合工具

這些工具協助你以上述語言撰寫查詢——它們本身並非查詢語言。

- **自然語言查詢**——由 Claude 驅動的 NL→SQL/Cypher/GraphQL 管線。以純白話英文描述你想要什麼；該管線會以你所選的語言產生一段查詢，並在執行前提供互動式驗證迴圈。

![Natural Language Query](docs/images/natural-language.png)

### 線路協定 (Wire Protocols)

這些是連線協定。SQL、GraphQL 及 Cypher 均搭載於其上——線路協定的選擇不會改變查詢介面或治理行為。

- **pgwire**——任何 PostgreSQL 用戶端（psql、DBeaver、DataGrip、asyncpg、SQLAlchemy、pandas 的 `read_sql`）皆可在連接埠 5439 連線，如同連上一台 Postgres 伺服器。僅接受 SQL。完整治理管線適用。`pg_catalog` 及 `information_schema` 由記憶體內建目錄作答，讓結構描述瀏覽器無需進行聯邦往返即可運作。TLS 為選用項目。
- **Bolt (Neo4j)**——任何 Neo4j 用戶端（Neo4j Browser、Bloom、官方驅動程式）皆可透過 Bolt 協定連線，並針對聯邦圖形執行 Cypher。使用者所持有的每個角色，均以 `provisa_<role>` 資料庫的形式呈現。治理方式與其他任何傳輸方式相同。TLS 為選用項目。
- **Arrow Flight**——透過 gRPC 進行的高輸送量欄式串流；接受 GraphQL 或 SQL 作為查詢輸入。結果集不受限，無需伺服端具體化，也不需要額外基礎設施。
- **JDBC**——以 `approved` 或 `catalog` 模式與 BI 工具整合（Tableau、Power BI、DBeaver）。
- **WebSocket / SSE**——訂閱功能：近乎即時的變更事件；後端支援 PG 原生、MongoDB 原生、CDC、輪詢。也透過 Kafka 公開。

### 數據來源

- **53 種來源類型**——PostgreSQL、MySQL、MongoDB、Cassandra、Elasticsearch、Neo4j、SPARQL 三元組儲存庫、Kafka、Google Sheets 等等，透過單一 API 存取；圖形及 RDF 來源屬於一級公民，而非轉接器
- **智慧路由**——單一來源查詢會繞過聯邦（延遲低於 100 毫秒）；多來源查詢則經由聯邦層路由——可自帶叢集，或使用內嵌的工作程序
- **API 來源**——將 REST、GraphQL、gRPC、WebSocket 或 RSS 端點註冊為可查詢的資料表；內附 SPARQL 輔助工具；跨 API 來源與關聯式來源的聯邦式 join 可透明運作
- **遠端結構描述內省 (introspection)**——指向任何 GraphQL、OpenAPI 或 gRPC 端點；已記載的操作會自動公開為可查詢的資料表、圖形節點及邊，並在其上完整套用治理
- **檔案來源**——CSV、Parquet 及 SQLite 檔案作為可查詢的資料表；支援本機路徑及遠端物件儲存 (`s3://`、`ftp://`、`sftp://`)
- **Kafka 整合**——主題 (topic) 作為唯讀資料表；查詢結果作為 Kafka sink
- **排程觸發器**——Cron 及間隔觸發器 (APScheduler)，觸發 webhook、mutation 或 Kafka sink 發佈
- **聯邦效能提示**——以 SQL 註解形式的路由提示，可覆寫自動路由決策

![Data Sources](docs/images/data-sources.png)

來源、檔案及遠端端點皆可從使用者介面註冊為受治理的資料表：

![Table Registration](docs/images/table-registration.png)

### 安全與治理

- **行級安全 (Row-level security)**——按資料表、按角色注入 WHERE 子句
- **欄位遮罩**——按欄位遮罩（regex、常數、截斷），並支援以角色為基礎的略過
- **欄位預設集**——在插入/更新時注入伺服端靜態或工作階段變數值；不會在 mutation 輸入型別中公開
- **寫入權限**——逐欄位的 mutation 存取控制 (`writable_by`)
- **繼承角色**——角色會遞迴繼承父角色的 RLS、可視性及遮罩設定
- **已追蹤的函式與 webhook**——資料庫函式及外送 webhook，以具型別回傳形狀公開為 GraphQL mutation
- **ABAC 批核掛鉤 (approval hook)**——執行前授權掛鉤；webhook、gRPC 或 unix_socket 傳輸方式；範圍可為按資料表、按來源或全域；具備可設定的回退政策
- **可插拔身分驗證**——Firebase、Keycloak、OAuth 2.0、simple（用於測試）

![Security Roles](docs/images/security-roles.png)

### 交付與效能

- **作為已記錄轉換的具體化檢視**——一個 MV 會捕捉產生它的轉換：其 join 形狀或 SQL、建構它所依據的各來源輸入信號（Iceberg 快照、關聯式資料庫水位標記），以及註冊時的決定性 (determinism) 檢查。由於該轉換被記錄下來，查詢（或子運算式）會被透明地改寫至一個最新的 MV 之上——採結構化 join 模式比對，並支援部分比對，因此即使一個 MV 只涵蓋部分 join，仍然適用，其餘 join 則予以保留
- **熱門資料表內嵌**——經常被 join 的小型查閱資料表，會以 VALUES CTE 的形式直接內嵌於查詢計劃中，省去維度數據的跨來源往返
- **查詢快取**——按角色及 RLS 分割的 Redis 結果快取；已包含 APQ 雜湊快取
- **可觀測性即數據**——分散式追蹤、指標及記錄透過 OpenTelemetry 收集，壓縮進 S3 上的 Iceberg，並自動註冊為聯邦結構描述中可查詢的資料表（`traces`、`metrics`、`logs`、`queries`）；可用 SQL、GraphQL 或 Cypher 查詢它們，並與你的業務數據並列——把一個 `customers` 資料表 join 到 `queries` 資料表，即可看到誰執行了什麼查詢、花了多久時間

### 管理與整合

- **管理 API**——位於 `/admin/graphql` 的 GraphQL；設定上傳/下載、關聯編輯、查詢批核
- **報表檢視器**——`/admin/reports` 列出內建的 ops 網域管理檢視，以及任何已註冊的自訂報表；需要 `observability` 能力
- **資料表預覽**——每一個已註冊的資料表都有一個伺服端分頁的受治理數據檢視器，具備下推式篩選、多層級 group-by 及 CSV 匯出
- **GraphQL Voyager**——以實體關聯圖形式呈現互動式、按角色範圍限定的結構描述視覺化
- **LLM 關聯發現**——由 Claude 驅動的外部索引鍵候選建議
- **Python 用戶端**——`pip install provisa-client`；GraphQL/SQL → DataFrame、Arrow Flight → pyarrow Table、SQLAlchemy 方言、ADBC 支援
- **數據擷取**——用於將 JSON 事件數據推送進平台的 HTTP 端點
- **Hasura v2 / DDN 匯入**——將 Hasura v2 中繼資料或 DDN supergraph YAML 轉換為 Provisa 設定
- **Apollo Federation**——將 Provisa 公開為一個 Apollo Federation v2 子圖 (subgraph)

以實體關聯圖 (GraphQL Voyager) 呈現的按角色範圍限定結構描述視覺化：

![Schema Voyager](docs/images/schema-voyager.png)

關聯經過註冊、批核，並被強制執行為唯一合法的 JOIN 路徑：

![Relationships](docs/images/relationships.png)

## 安全模型

這正是「治理已在每一項查詢的既有路徑上」不再只是一句口號的地方。Provisa 在每一種查詢語言（GraphQL、SQL、Cypher）及每一種傳輸方式（REST、gRPC、Arrow Flight、JDBC、pgwire、Bolt、WebSocket）上，強制執行一套多層級的安全模型。治理是均一套用的——不存在任何能繞過它的查詢路徑。涵蓋範圍是由結構保證的完整性，而非仰賴人手謹慎：新增一個來源、欄位或關聯，每一層都會自動套用於其上，無需記得手動註冊。

各層依序套用。一項請求必須先通過某一層，下一層才會被評估。

### 第 0 層——內省 (introspection) 篩選

呈現給某個角色的結構描述及目錄，僅包含其 `domain_access` 清單中的資料表，以及通過逐欄位 `visible_to` 規則的欄位。超出角色存取範圍的物件在探索時即不可見——無法被查詢、自動完成，或推斷其存在。此規則適用於 GraphQL 結構描述、SQL 目錄，以及查詢編輯器的結構描述瀏覽器。

### 第 1 層——公開存取

屬於沒有 `domain_access` 限制之網域的資料表，對所有已驗證身分皆可見，無需額外設定。對於真正公開的數據，摩擦為零。

### 第 2 層——網域存取

每個角色都帶有一份網域 ID 的 `domain_access` 清單。一項觸及該清單以外資料表的查詢，會在執行前被拒絕。這是粗粒度的擁有權邊界——無論 SQL 怎麼寫，一個 HR 角色都無法觸及財務資料表。

### 第 3 層——行級安全

確認網域存取後，逐資料表、逐角色的 `WHERE` 判斷式會在執行時注入每一項 `SELECT`。這些判斷式是針對原始數據求值的。一位區域經理查詢一個共享的 orders 資料表時，即使是 `SELECT *`，也只會看到自己所屬區域的資料列。

### 第 4 層——欄位可視性與遮罩

`visible_to` 清單中不含請求角色的欄位，會從查詢輸出中被剔除。帶有遮罩規則的欄位，其值會在結果離開伺服器之前被取代——regex 遮蔽、常數取代或截斷。遮罩適用於所有查詢語言及輸出格式。

### 第 5 層——判斷式防護

已遮罩的欄位會被拒絕出現在 `WHERE` 及 `HAVING` 子句中。若無此防護，即使輸出已被遮罩，呼叫方仍可透過在篩選條件中對其進行二元搜尋，推斷出未遮罩的值。此拒絕是在查詢剖析階段、執行之前強制執行的。

### 關聯治理

SQL 中的 JOIN 條件，必須符合資料表之間一項已註冊、已批核的關聯。未經批核的 join 會被拒絕。每一項關聯都帶有人類可讀的原因及描述——為使用者及自主代理人 (agent) 說明為何存在這條遍歷路徑的指引。這是治理政策，而非硬性的安全邊界：無論 join 結構為何，第 2 至 5 層都依然成立，因此刻意規避此規則並不會讓角色取得原本無法透過兩次獨立查詢觸及的數據。規避嘗試會被記錄並可供稽核。

---

這些層是可組合的。一個同時具有網域存取、RLS 及遮罩欄位的角色，會同時啟用全部五項限制。新增一個數據來源、欄位或關聯，不需要更新每一條規則——每一層都是獨立設定的，並會自動套用於任何觸及受治理物件的查詢。

### macOS

1. 下載 [Provisa-macOS.dmg](https://provisa.dev/dl/macos)（永遠是最新版本）
2. 將 **Provisa.app** 拖曳至 `/Applications`，然後按兩下啟動
3. 首次啟動會完成一次性設定（約 2 分鐘，不需要網際網路連線）
4. 開啟終端機：

```bash
provisa start   # start all services
provisa open    # open the UI in your browser
```

### Linux

1. 下載 [Provisa-linux-x86_64.AppImage](https://provisa.dev/dl/linux)（永遠是最新版本）
2. 賦予其執行權限並執行——首次啟動會完成一次性設定（不需要網際網路連線）：

```bash
chmod +x Provisa-*-linux-x86_64.AppImage
./Provisa-*-linux-x86_64.AppImage
provisa start && provisa open
```

### Windows

1. 下載 [Provisa-windows-x64.exe](https://provisa.dev/dl/windows)（永遠是最新版本）
2. 執行安裝程式——不需要管理員權限
3. 從開始功能表開啟 **Provisa First Launch**——完成一次性設定（約 5 分鐘，不需要網際網路連線）
4. 開啟一個新的終端機：

```bash
provisa start
```

### 第一次查詢

在本機開發環境中 (`PROVISA_MODE=test`)，不需要任何憑證。在正式環境中，則以 Bearer 權杖進行身分驗證——角色會自動由其中擷取。

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

下載 [provisa-jdbc.jar](https://provisa.dev/dl/jdbc)（永遠是最新版本），並將其加入你的 BI 工具驅動程式路徑。

```text
jdbc:provisa://localhost:8815
```

以你的 Provisa 使用者名稱及密碼進行身分驗證——伺服器會指派你的角色。

- **`catalog` 模式**——完整結構描述可見；適用於目錄工具（Collibra、Atlan、DBeaver）

Tableau 及 Power BI 的設定步驟，請參閱 [docs/integrations.md](docs/integrations.md)。

### PostgreSQL 線路協定 (pgwire)

Provisa 在連接埠 5439 上使用 PostgreSQL 線路協定。任何能連上 Postgres 的用戶端，都能連上 Provisa——不需要驅動程式，不需要轉接器，也不需要改動既有工具。

**PostgreSQL 使用者名稱決定 Provisa 角色。**在 `provider: none`（信任模式）下，密碼會被忽略，任何已設定的角色名稱皆可作為使用者名稱被接受——以 `analyst`、`admin` 或任何角色連線，即可看到該角色所受治理的數據檢視。在 `provider: simple` 下，密碼會以 bcrypt 進行驗證。其他驗證提供者（`firebase`、`keycloak`、`oauth`）不支援透過 pgwire 使用。

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

所有查詢都經過完整的治理管線——網域存取、RLS、遮罩及判斷式防護，套用方式與 GraphQL 及 REST 完全相同。結構描述瀏覽器（DBeaver、DataGrip、pgAdmin）開箱即用：`pg_catalog` 及 `information_schema` 查詢由一個範圍限定於該角色網域存取的記憶體內建目錄作答，因此使用者只會看到自己獲准查詢的資料表及欄位。

DataGrip 透過 pgwire 瀏覽受治理的結構描述及其外部索引鍵圖——不需要驅動程式，不需要轉接器：

![Provisa in DataGrip over pgwire](docs/images/pgwire-datagrip.png)

透過設定 `PROVISA_PGWIRE_CERT` 及 `PROVISA_PGWIRE_KEY` 可啟用 TLS。連接埠可透過 `PROVISA_PGWIRE_PORT` 設定（預設為 `5439`）。

### Bolt (Neo4j 線路協定)

Provisa 也支援 Neo4j 的 **Bolt** 協定，因此圖形原生工具可以直接連線，並針對聯邦圖形執行 Cypher——不需要匯出，也不需要另外的圖形資料庫。將 **Neo4j Browser** 或 **Bloom** 指向 Provisa，即可在套用相同治理（網域存取、RLS、遮罩）的情況下跨來源遍歷關聯。

Neo4j Browser 對 Provisa 執行 Cypher——節點標籤、關聯類型及屬性鍵，全部直接來自已註冊的結構描述：

![Provisa in Neo4j Browser over Bolt](docs/images/bolt-neo4j-browser.png)

透過設定 `PROVISA_BOLT_PORT` 啟用（Neo4j 的預設值為 `7687`）。TLS 以 `PROVISA_BOLT_CERT` 及 `PROVISA_BOLT_KEY` 啟用。已驗證使用者所持有的每一個 Provisa 角色，都會以一個可選取的 `provisa_<role>` 資料庫呈現（如上方的 `provisa_admin` 選取項目）——選取其一，即可將工作階段收窄至該角色的網域權限；使用者永遠無法超出自己所持有的角色範圍。

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

完整參考請見 [docs/python-client.md](docs/python-client.md)。

## 文件

| 主題 | 文件 |
| --- | --- |
| 開發人員快速入門（從原始碼執行） | [docs/quickstart.md](docs/quickstart.md) |
| 完整 YAML 設定參考 | [docs/configuration.md](docs/configuration.md) |
| 端點參考（GraphQL、REST、Flight、gRPC） | [docs/api-reference.md](docs/api-reference.md) |
| 系統設計與元件圖 | [docs/architecture.md](docs/architecture.md) |
| 安全模型（RLS、遮罩、身分驗證） | [docs/security.md](docs/security.md) |
| 支援的來源類型 | [docs/sources.md](docs/sources.md) |
| SSE 訂閱 | [docs/subscriptions.md](docs/subscriptions.md) |
| JDBC、BI 工具、Arrow Flight 用戶端、Apollo Federation | [docs/integrations.md](docs/integrations.md) |
| Python 用戶端 (`provisa-client`) | [docs/python-client.md](docs/python-client.md) |
| 管理 API | [docs/admin.md](docs/admin.md) |
| 部署（Docker Compose、Kubernetes、macOS） | [docs/deployment.md](docs/deployment.md) |
| Hasura v2 / DDN 匯入 | [docs/import.md](docs/import.md) |
| 發佈流程（alpha/beta/stable 標籤） | [docs/releasing.md](docs/releasing.md) |

## 規模設定

Provisa 內建一個聯邦引擎，用於多來源查詢。首次啟動時，你選擇一個 RAM 預算；Provisa 會自動推算本機聯邦工作程序的數量。

| 主機 RAM | 工作程序 | 典型工作負載 |
| --- | --- | --- |
| < 24 GB | 0 | 開發、單一來源查詢、小型團隊 |
| 24–47 GB | 1 | 小型團隊、中等程度的跨來源查詢 |
| 48–95 GB | 2 | 部門級部署、混合 BI 及筆記本使用 |
| 96 GB+ | 4 | 大型部門、大量並行聯邦查詢 |

可隨時透過編輯 `~/.provisa/config.yaml`（`federation_workers: N`）並執行 `provisa restart` 來變更工作程序數量。設為 `0` 即僅以協調模式運作（單一節點）。

### 擴展至單一主機之外

**水平擴展 (Horizontal scale-out)**——在負載平衡器之後執行多個 Provisa 執行個體。每個執行個體都是一套功能完整的系統。所有執行個體都必須指向同一個設定資料庫（在次要主機上設定 `CONFIG_DB_HOST`），並可選擇性地指向一個共享的 Redis 執行個體 (`REDIS_URL`)，以取得統一的快取。大多數查詢會透明地分散處理；非常大型的跨來源 join 可能超出單一執行個體的資源，需要更大的主機或一個外部聯邦叢集。

**共享 Redis**——在每個執行個體上設定 `REDIS_URL`，指向一個外部 Redis。共享 Redis 意味著來自一個執行個體的快取項目，其他所有執行個體皆可使用，從而提升叢集整體的命中率。

**自帶聯邦叢集**——將 Provisa 指向一個既有的外部聯邦叢集，而非使用內嵌工作程序。建議用於大規模或雲端部署；設定方式請參閱 [docs/deployment.md](docs/deployment.md)。

## 授權條款

Business Source License 1.1（未經修改，依照 MariaDB 的 Licensor 承諾條款）。每一個
已發佈的版本，會在其公開發佈日的第 4 週年時轉換為 Change License（GPL v2.0 或
更新版本）；現行及近期的程式碼則保持在 BSL 之下。
在 Additional Use Grant 門檻之上的正式環境使用（未滿 100 名
員工/承包人員，且前一年營收低於 100 萬美元）需要商業
授權。詳見 [LICENSE](LICENSE)。

Licensor 不同意將本作品用於 AI/ML 訓練。詳見
[NOTICE](NOTICE)、[ai.txt](ai.txt) 及 [robots.txt](robots.txt)。如需商業授權
或 AI 訓練授權，請聯絡：<kennethstott@gmail.com>
