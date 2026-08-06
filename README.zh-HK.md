# Provisa

**連接您的資料庫。以 GraphQL、gRPC、SQL 或 MCP——透過任何 API 或通訊協定——在 5 分鐘內查詢。**

Provisa 在您各數據來源的聯合結果之上，提供每一種 API 介面（REST、GraphQL、SQL、gRPC、MCP 及更多）。之所以能做到這一點，是因為它是一個**主動語意層**：您整個數據估產的單一定義——涵蓋每一個領域、關係及原則，橫跨您所有數據來源，唯獨排除來源系統本身——同時操作及治理該估產。此定義並非引擎可供參考的文件；它*就是*引擎本身。已註冊的領域及關係是唯一合法的連接（join）路徑，而存取原則會編譯進每一個查詢計劃之中。一個模型，三項工作：

- **定義**——領域、欄位及關係只需宣告一次。此宣告即為每個使用者所見的結構描述 (Schema)，亦是任何查詢可採用的唯一連接路徑集。
- **強制執行**——行級安全、欄位遮罩、欄位可見性及查詢核准會於執行路徑上內嵌套用。沒有任何查詢能繞過這些機制而觸及數據，因此覆蓋範圍是透過架構設計達致完整，而非靠人手審慎達成。
- **稽核**——由於每個請求都經由同一條受治理路徑，何人、以何角色、依據何種原則查詢了甚麼，均會被統一記錄。分散式追蹤、指標及記錄本身亦註冊為可查詢的資料表，與您的業務數據並列。

一個受治理的核心服務所有語言及傳輸方式。以 **GraphQL、Cypher 或 SQL** 查詢；透過 **pgwire、Bolt、gRPC、REST、Arrow Flight 或 JDBC** 使用。每種查詢語言均會降維（lower）至單一中介表示法 (IR)，治理於此僅注入一次——因此原則不會在不同語言之間出現漂移——而該 IR 在輸出時會重新導向至各數據來源的原生方言。新增一種語言只是在共用核心上加裝新的前端，而非新增一個引擎。

此估產同時涵蓋分析型與交易型工作負載。跨數據來源的讀取會透過聯邦層扇出；寫入及單一數據來源的讀取則直接路由至該數據來源驅動程式——治理方式相同，但屬於交易型且延遲低於 100 毫秒。內建 Arrow Flight 欄式串流。

整個模型由少數幾個基本元素構成——領域、關係、角色及原則。詞彙精簡，因此定義易於理解，亦簡單易於評估及稽核：您可以閱讀原則集並知悉其作用。Provisa 是一個輕量的查詢編譯器，而非駐留於數據路徑上的執行環境。它將請求轉換為原生查詢、路由該查詢，然後不再介入——這正是此估產能維持效能的原因。

此設計支援兩種使用方式，且兩者並不互斥：

- **作為現代化的鷹架**——為您的估產建模，讓 Provisa 為每個數據來源產生原生 SQL，然後擷取該 SQL 並直接於目標系統中採用。Provisa 是過渡層，而非永久依存項目。
- **作為永久性、強制執行原則的基礎設施**——保留此系統作為每個查詢所經之受治理路徑，令定義、強制執行及稽核在估產存續期間保持一致。

## 聯邦模型

整個模型可歸結為兩項契約及兩項原則：數據來源化約為單一型別系統上的二維資料表，查詢化約為單一的類 SQL IR，可達性 (reachability) 決定何者即時查詢、何者具體化 (materialize)，而新鮮度策略則管治每一份具體化副本及衍生數據集。輸入數據形狀、輸入查詢形狀、於連接處治理、輸出原生查詢。本節其餘部分將逐一說明各個部分。

此模型立足於一項化約：每個數據來源均以單一通用型別系統上的二維資料表集合來表示。這是數據來源要加入此估產所須符合的契約，且對所有數據來源一視同仁。有些數據來源本身已然符合——一個 MySQL 或 PostgreSQL 資料表*本身即*是一個有型別的二維關係。有些經投影後即可符合：一個 GraphQL 查詢結果，經扁平化後即為一個資料表。有些則與此形狀格格不入——SPARQL 三元組儲存庫、Neo4j——但仍可運作，因為使用者提供的查詢其結果集是表格式的；該查詢本身即為配接器。無論數據來源為何，此估產所見的只有列、欄及通用型別，別無其他。導入一種新型態的數據來源，只是符合這唯一的契約，有時需要人手介入的步驟，而非撰寫一次性的整合程式。

該項化約在查詢端亦有其對應。SQL——涵蓋其所有方言及怪癖——本質上就是針對二維數據集進行分析的語言，這使得類 SQL 的形式成為查詢的自然通用目標。因此每個請求，無論以何種語言送達，第一步便會降維至該中介表示法。有些降維過程順暢——SQL 本身，甚至 GraphQL 亦然；有些則頗費工夫——Cypher 的路徑及圖形語意需要相當的工程——但均可實現。將每個請求先漏斗匯聚至單一 IR，方能令治理僅在單一形式、單一地點套用，無論其源頭語言為何。

在這兩種統一形狀——表格式數據來源及單一查詢形式——之上，聯邦在此意指涵蓋即時查詢與數據倉儲——即像 Trino 這類即時查詢引擎所涵蓋的範圍，再加上這類引擎所倚賴的具體化。統一這兩者的概念便是**可達性**：對任何數據來源而言，引擎能否原地查詢，抑或其數據必須先具體化至某個可查詢之處？可達性將此估產劃分為即時查詢部分及先行複製部分。

大多數資料庫本身已具備某種即時連結的概念——DuckDB `ATTACH`、PostgreSQL `postgres_fdw`、Databricks 外部連結。因此大多數資料庫在某程度上均可充當聯邦引擎。但沒有一個是全面的：每一個都只能觸及特定一組數據來源，其餘則需具體化，且沒有單一說法能清楚交代何者屬於哪一類。此模型透過令可達性明確化來填補此缺口——為每個數據來源定義一組方法，說明引擎可即時觸及的範圍，以及依此推斷須具體化的部分。

餘下的便是新鮮度：對於每個不可即時觸及的數據來源，其具體化副本須維持何等新近程度？實務上這化約為一小組策略——按需、按排程、按變更訊號（CDC、水位標記、快照），或釘選固定。為每個數據來源選定其一，即構成完整的新鮮度原則。

分析型數據集——衍生資料表、彙總結果、轉換的輸出——亦納入同一形狀之中。它們同樣須以 IR 表示，正因如此，血緣並非另一套須另行維護的系統：由每個來源系統至最終輸出的路徑，*即是*產生該輸出的 IR，端到端皆可讀取。建構此類數據集，將新鮮度的問題向後推進一步——該數據集是按排程重新整理、僅於其前置條件滿足時、以近乎即時的方式持續更新，抑或作為釘選的歷史快照？表達如何及何時建構數據集的方式，與表達數據來源副本時同屬一組精簡、可列舉的詞彙，因此衍生數據集所帶的建構原則，與數據來源副本所用的詞彙完全一致。

維度模型正是一項直接應用。星型結構描述的事實及維度資料表，與其他分析型數據集無異——維度是一項經整合、去重複的投影；事實資料表則是連接與彙總化約至某一粒度的結果——每一項均帶有自身的建構及新鮮度原則。緩慢變化維度無須任何特別機制：釘選快照即為 Type 2 歷史記錄，排程重建即為 Type 1。且由於此結構描述是以 IR 定義，而非實體綁定至某一數據倉儲的資料表，同一套事實及維度定義可重新導向——具體化於 Oracle、Databricks，或於 MPP 引擎上保持虛擬——而無須重新建模。此模型會產生星型結構描述；它不會將其鎖定於某一引擎。

Data Vault 同樣適用此模式，只是提早一層。其 hub 是去重複的業務鍵數據集，其 link 是它們之間已註冊的關係，而其 satellite 則是僅供插入、附時間戳記的屬性數據集——即歷史記錄。satellite 只是採用變更訊號新鮮度策略的一種衍生數據集：載入日期加上雜湊差異即是套用於描述性屬性的 CDC，而僅供插入的歷史記錄即是釘選快照策略。時間點 (point-in-time) 及橋接資料表則是為查詢效能而建構的進一步衍生數據集。因此原始的 vault 是 IR 中的一組分析型數據集，而星型結構描述則是其上的一項投影——兩者皆為產生所得，且均可跨引擎移植。此模型不會替您決定方法論：何者成為 hub、satellite 的粒度、拆分策略。這些仍屬建模抉擇；一旦決定，便以可移植的 IR 形式存在，而非焊死於某一數據倉儲的 ETL。

這兩種模式均透過**兩項一級捷徑**宣告，而非手寫檢視 (view)——這是建構每個星型結構描述及 Data Vault 所用的基本元素，且保持方法論中立：

- **`entity`**——一個帶鍵、去重複、可選擇具歷史記錄的數據來源投影。宣告一個實體鍵、屬性及一個歷史模式；Provisa 會將其降維為具體化檢視，若要求歷史記錄，則降維為**雙時態具體化檢視**（`scd2` → delta、`snapshot` → snapshot）。單一構造同時服務 Kimball 的**維度**（SCD1/SCD2）及 Data Vault 的 **hub + satellite**。
- **`fact`**——連接至實體鍵、化約至宣告粒度、帶有彙總量度的連接。Provisa 會將其降維為彙總具體化檢視，加上與各實體之間已註冊的關係。單一構造同時服務星型結構描述的**事實資料表**及 Data Vault 的 **link**（不帶量度的事實即為純粹的鍵集 link）。

由於此降維過程是純粹的——一項 `entity`/`fact` 規格恰好等同於建模人員原本須手寫的具體化檢視、雙時態及關係定義——因此該數據倉儲從上到下皆為 IR，可跨引擎重新導向而無須重新建模。可於管理介面（供實體及事實使用的**模型**表單）或透過管理 API（`registerEntity` / `registerFact`）宣告數據倉儲；此模型是*產生* Kimball 星型結構或 Data Vault，而非強加其一。

### 時光回溯

時光回溯是一個簡單的概念——保留每一列的每一個版本，而非覆寫它，如此便能查詢數據在過去任一時刻的*原貌*。各引擎能否高效做到這一點才是關鍵差異所在，這正是 Provisa 將其設為具體化檢視**定義**的一項屬性，而非儲存引擎的屬性（REQ-1162）。只需宣告一次；便可於任何具備具體化能力的後端上運作。

維持其可移植性的規則是**僅供附加**：一個版本一旦寫入，便永不更新或刪除。以寫回一個「有效至」日期來淘汰一列——這是常見的雙時態技巧——需要一次 UPDATE，而許多引擎難以（甚至無法）於聯邦儲存區上廉價執行此操作，因此 Provisa 不採此法。取而代之，每次重新整理均會**附加**，而「在時間 T 生效的是哪個版本」則於讀取時自不可變的記錄中推導而得。附加的方式恰有兩種：

- **快照**——附加整份最新數據集，並蓋上此次重新整理的系統時間戳記。無須比對差異；於任何引擎上皆正確；每次重新整理的儲存空間均增長一整份副本。
- **差異**——僅附加已變更的部分，加上已移除鍵的墓碑標記 (tombstone)。此差異是**由引擎計算**的（於 `INSERT … SELECT` 內部進行反連接），絕不在 Provisa 中逐列摺算。體積較小，且須要一個實體鍵。

系統時間（Provisa 記錄某版本的時刻）以此方式管理；有效時間（某項事實於業務上為真的時刻）則由該檢視本身的 SELECT 提供並予以保留。能提供更多功能的引擎——原生 Iceberg 快照、能維持較少列數的 MERGE——可在同一宣告之下，針對其目標作效能最佳化；僅供附加的路徑則是於任何基礎架構上皆正確的底線。

讀取是透明的。對一個雙時態具體化檢視發出的一般查詢，預設會由附加記錄中重建**目前**狀態；如要回溯時間，只需傳送 `X-Provisa-As-Of: <timestamp>` 標頭，整個查詢便會以估產於該時刻的狀態作答——於任何基礎架構上語意皆相同。可於管理介面（**時光回溯**控制項：關閉 / 快照 / 差異，加上一個實體鍵）或透過管理 API，為任何具體化檢視啟用此功能。

可達性加上新鮮度，構成一套通用的數據聯邦模型：一項定義說明何者為即時、何者為具體化，以及每份副本維持何等新鮮度——不受限於任何單一引擎的觸及範圍。其成果是擺脫專有技術鎖定。此模型可移植；此估產不受制於任何供應商當下聯邦技術所能觸及的數據來源多寡。

## 功能

### 查詢介面

這些是您撰寫查詢所用的語言及結構化 API。每一種均有其自身的語法及語意；治理機制（行級安全、遮罩、欄位可見性、關係強制執行）於所有查詢介面上均一致套用，不論由哪種線路通訊協定傳遞。

- **GraphQL**——依角色劃分的結構描述，具備欄位級可見性、篩選、以游標分頁及彙總查詢（`count`、`sum`、`avg`、`min`、`max`）。受限於已註冊關係之結構描述——結構上藉由架構設計即為有效，是撰寫正確簡單查詢最快的途徑。內建 Apollo APQ：查詢會被雜湊並於伺服端註冊；後續呼叫僅透過 HTTP GET 傳送該雜湊值，令回應可被 CDN 快取，且客戶端無須任何變更。低於可設定列數門檻的查閱資料表會以列舉 (enum) 型別公開。
- **SQL**——聯邦數據上的完整 SQL；不受限制，比 GraphQL 更具表達力。撰寫標準 SQL——包括相關子查詢等——即可跨數據來源不加修改地執行。單一數據來源的查詢會完全繞過聯邦層（延遲低於 100 毫秒）。
- **Cypher**——建基於同一套聯邦結構描述之上的圖形查詢語言。將關係作為圖形邊 (edge) 進行遍歷；聯集數據來源；可變長度路徑。治理方式與 GraphQL 及 SQL 完全一致。
- **gRPC model API**——自已註冊結構描述自動產生的 `.proto`；每個資料表均有型別化的查詢及插入 RPC，並支援串流回應。與 GraphQL 同樣是結構描述驅動——註冊模型即為契約，protobuf 只是線路編碼方式。與 Arrow Flight（一種欄式串流傳輸）不同，這是完整的、逐資料表的查詢介面。
- **JSON:API**——位於 `/data/jsonapi/{table}` 的結構化查詢 API，設計上僅限 HTTP。支援 JSON:API 1.1：稀疏欄位集 (`fields[table]=col1,col2`)、篩選表達式 (`filter[field][op]=value`)、複合文件 (`include=relation`) 及排序。並非通用查詢語言——一次查詢一個資料表，使用標準化篩選語法，而非任意查詢字串。
- **查詢語言探索工具**——撰寫一則 GraphQL 查詢，並於側邊面板即時檢視其**語意 SQL** 及 **Cypher** 翻譯；可複製任一者，或直接跳轉至 SQL 或圖形編輯器。實務上的常見流程是先以 GraphQL 草擬查詢片段，再將所得的 SQL 拼接至複雜的檢視或報表中。

此探索工具將一則 GraphQL 查詢連同其即時 SQL 及 Cypher 翻譯並列顯示：

![Query Language Explorer](docs/images/query-explorer.png)

同一套聯邦結構描述亦可作為即時圖形進行探索——領域及節點標籤、關係型別，以及可變長度遍歷：

![Graph Visualization](docs/images/graph-view.png)

### 查詢組合工具

這些工具協助您以上述語言撰寫查詢——它們本身並非查詢語言。

- **自然語言查詢**——由 Claude 驅動的 NL→SQL/Cypher/GraphQL 管線。以純文字英文描述您所需的內容；此管線會以您所選的語言產生查詢，並於執行前提供互動式驗證迴圈。

![Natural Language Query](docs/images/natural-language.png)

### 線路通訊協定

這些是連線通訊協定。SQL、GraphQL 及 Cypher 均搭載於其上——線路通訊協定的選擇不會改變查詢介面或治理行為。

- **pgwire**——任何 PostgreSQL 客戶端（psql、DBeaver、DataGrip、asyncpg、SQLAlchemy、pandas `read_sql`）均可於連接埠 5439 上連線，猶如連接至一部 Postgres 伺服器。僅接受 SQL。完整治理管線適用。`pg_catalog` 及 `information_schema` 由記憶體內目錄作答，使結構描述瀏覽工具無須經聯邦往返即可運作。TLS 為選用項目。
- **Bolt (Neo4j)**——任何 Neo4j 客戶端（Neo4j Browser、Bloom、官方驅動程式）均可透過 Bolt 通訊協定連線，並對聯邦圖形執行 Cypher。使用者所持有的每個角色均會呈現為一個 `provisa_<role>` 資料庫。治理方式與其他所有傳輸方式相同。TLS 為選用項目。
- **Arrow Flight**——透過 gRPC 提供高吞吐量欄式串流；接受 GraphQL 或 SQL 作為查詢輸入。結果集無上限，無伺服端具體化，無須另行架設基礎設施。
- **JDBC**——以 `approved` 或 `catalog` 模式整合 BI 工具（Tableau、Power BI、DBeaver）。
- **WebSocket / SSE**——訂閱：近乎即時的變更事件；後端支援：PG 原生、MongoDB 原生、CDC、輪詢。亦透過 Kafka 公開。

### 數據來源

- **46 種數據來源型別**——PostgreSQL、MySQL、MongoDB、Cassandra、Elasticsearch、Neo4j、SPARQL 三元組儲存庫、Kafka、Google Sheets 等，均透過單一 API 存取；圖形及 RDF 數據來源屬一級公民，而非配接器
- **智能路由**——單一數據來源的查詢會繞過聯邦（延遲低於 100 毫秒）；多數據來源的查詢則經聯邦層路由——可自備叢集，亦可使用內嵌工作節點
- **API 數據來源**——將 REST、GraphQL、gRPC、WebSocket 或 RSS 端點註冊為可查詢的資料表；內建 SPARQL 輔助工具；跨 API 數據來源與關聯式數據來源的聯邦連接可無縫運作
- **遠端結構描述內省**——指向任何 GraphQL、OpenAPI 或 gRPC 端點；有文件記載的操作會自動公開為可查詢的資料表、圖形節點及邊，並套用完整治理
- **檔案數據來源**——將 CSV、Parquet 及 SQLite 檔案作為可查詢的資料表；支援本機路徑及遠端物件儲存空間（`s3://`、`ftp://`、`sftp://`）
- **Kafka 整合**——主題作為唯讀資料表；查詢結果可作為 Kafka sink
- **排程觸發器**——透過 Cron 及間隔觸發器（APScheduler）觸發 webhook、變異 (mutation) 或 Kafka sink 發佈
- **聯邦效能提示**——SQL 註解式路由提示，可覆寫自動路由決策

![Data Sources](docs/images/data-sources.png)

數據來源、檔案及遠端端點均可由使用者介面註冊為受治理資料表：

![Table Registration](docs/images/table-registration.png)

### 安全性與治理

- **行級安全**——依資料表、依角色注入 WHERE 子句
- **欄位遮罩**——依欄位遮罩（正規表示式、常數、截斷），並支援依角色略過
- **欄位預設值**——伺服端靜態值或工作階段變數值，於新增/更新時注入；不會於變異輸入型別中公開
- **寫入權限**——依欄位的變異存取控制 (`writable_by`)
- **繼承角色**——角色可遞迴地由母角色繼承行級安全、可見性及遮罩設定
- **受追蹤函式及 webhook**——資料庫函式及外送 webhook 公開為具型別回傳形狀的 GraphQL 變異
- **ABAC 核准掛鉤**——執行前授權掛鉤；支援 webhook、gRPC 或 unix_socket 傳輸；範圍可為資料表級、數據來源級或全域；可設定備援原則
- **可插拔驗證**——Firebase、Keycloak、OAuth 2.0、simple（測試用）

![Security Roles](docs/images/security-roles.png)

### 交付與效能

- **作為已記錄轉換的具體化檢視**——一個具體化檢視會擷取產生它的轉換過程：其連接形狀或 SQL、建構它所依據的各數據來源輸入訊號（Iceberg 快照、關聯式資料庫水位標記），以及註冊時的確定性檢查。由於此轉換過程已被記錄，查詢（或其子表達式）便可透明地重寫至一個最新的具體化檢視——採結構化連接模式比對，並支援局部比對，因此即使某具體化檢視僅涵蓋部分連接，仍可套用，其餘連接則予以保留
- **熱門資料表內嵌**——經常連接的小型查閱資料表，會以 VALUES CTE 形式直接內嵌於查詢計劃中，省卻維度數據的跨數據來源往返
- **查詢快取**——依角色及行級安全劃分的 Redis 結果快取；內含 APQ 雜湊快取
- **可觀測性即數據**——分散式追蹤、指標及記錄經 OpenTelemetry 收集，壓縮至 S3 上的 Iceberg，並自動註冊為聯邦結構描述中可查詢的資料表（`traces`、`metrics`、`logs`、`queries`）；可以 SQL、GraphQL 或 Cypher 查詢，並與您的業務數據並列——將 `customers` 資料表連接至 `queries` 資料表，即可查看何人執行了何種查詢及所耗時間

### 管理與整合

- **管理 API**——位於 `/admin/graphql` 的 GraphQL；設定上傳/下載、關係編輯、查詢核准
- **報表檢視器**——`/admin/reports` 列出內建的 ops 域管理檢視以及所有已註冊的自訂報表；需要 `observability` 功能
- **表格預覽**——每張已註冊的表格都配有伺服器端分頁的受治理資料檢視器，支援下推篩選、多層分組與 CSV 匯出
- **GraphQL Voyager**——依角色劃分、以實體關係圖形式呈現的互動式結構描述視覺化
- **LLM 關係探索**——由 Claude 驅動的外部索引鍵候選建議
- **Python 客戶端**——`pip install provisa-client`；GraphQL/SQL → DataFrame、Arrow Flight → pyarrow Table、SQLAlchemy 方言、ADBC 支援
- **數據擷取**——用於將 JSON 事件數據推送至平台的 HTTP 端點
- **Hasura v2 / DDN 匯入**——將 Hasura v2 中介資料或 DDN supergraph YAML 轉換為 Provisa 設定
- **Apollo Federation**——將 Provisa 公開為一個 Apollo Federation v2 子圖

依角色劃分的結構描述，以實體關係圖形式視覺化呈現（GraphQL Voyager）：

![Schema Voyager](docs/images/schema-voyager.png)

關係經註冊、核准，並強制執行為唯一合法的 JOIN 路徑：

![Relationships](docs/images/relationships.png)

## 安全模型

正是在此處，「位於每個查詢原本就必經的路徑上」不再只是一句口號。Provisa 於每一種查詢語言（GraphQL、SQL、Cypher）及每一種傳輸方式（REST、gRPC、Arrow Flight、JDBC、pgwire、Bolt、WebSocket）上，均強制執行多層次安全模型。治理是一致套用的——沒有任何查詢路徑能繞過它。覆蓋範圍是透過架構設計達致完整，而非靠人手審慎達成：新增一個數據來源、欄位或關係，每一層均會自動套用其上，無須記得另行註冊。

各層依序套用。一項請求須先通過每一層，方能進入下一層評估。

### 第 0 層——內省過濾

呈現給某角色的結構描述及目錄，僅包含其 `domain_access` 清單中的資料表，以及通過逐欄位 `visible_to` 規則的欄位。角色存取範圍以外的物件，於探索階段即不可見——無法被查詢、自動完成，或被推斷其存在。此規則適用於 GraphQL 結構描述、SQL 目錄及查詢編輯器的結構描述瀏覽工具。

### 第 1 層——公開存取

未設定 `domain_access` 限制之領域中的資料表，對所有已驗證身分均可見，無須額外設定。真正公開的數據，摩擦為零。

### 第 2 層——領域存取

每個角色均帶有一份領域 ID 的 `domain_access` 清單。觸及該清單以外資料表的查詢，會於執行前遭拒。這是粗粒度的擁有權邊界——不論 SQL 撰寫方式為何，人力資源角色均無法觸及財務資料表。

### 第 3 層——行級安全

確認領域存取後，依資料表、依角色的 `WHERE` 判詞會於執行時注入每一則 `SELECT`。這些判詞針對原始數據求值。一位地區經理查詢共用的訂單資料表時，即使執行 `SELECT *`，亦僅會看到其所屬地區的資料列。

### 第 4 層——欄位可見性及遮罩

帶有 `visible_to` 清單且不含請求角色的欄位，會自查詢輸出中剔除。帶有遮罩規則的欄位，其值會在離開伺服器前被取代——正規表示式遮蔽、常數取代或截斷。遮罩適用於所有查詢語言及輸出格式。

### 第 5 層——判詞防護

已遮罩的欄位會被拒絕出現於 `WHERE` 及 `HAVING` 子句中。若無此機制，即使輸出已遮罩，呼叫方仍可透過於篩選條件中進行二分搜尋，推斷出未遮罩的值。此拒絕於查詢剖析階段即強制執行，早於執行之前。

### 關係治理

SQL 中的 JOIN 條件必須符合資料表之間已註冊、已核准的關係。未經核准的連接會遭拒。每項關係均帶有可供人閱讀的原因及描述——為使用者及自主代理程式說明該遍歷路徑存在的緣由。這是治理原則，而非硬性安全邊界：無論連接結構為何，第 2 至 5 層均一律有效，因此蓄意規避亦不會暴露該角色本來無法透過兩次獨立查詢觸及的數據。規避嘗試會被記錄並可供稽核。

---

這些層次會相互組合。一個同時擁有領域存取、行級安全及遮罩欄位限制的角色，會同時啟用全部五項約束。新增數據來源、欄位或關係，並不需要更新每一條規則——每一層均獨立設定，並自動套用於任何觸及受治理物件的查詢。

### macOS

1. 下載 [Provisa-macOS.dmg](https://provisa.dev/dl/macos)（恆為最新版本）
2. 將 **Provisa.app** 拖曳至 `/Applications`，然後雙擊啟動
3. 首次啟動會完成一次性設定（約 2 分鐘，無須連接網際網路）
4. 開啟終端機：

```bash
provisa start   # start all services
provisa open    # open the UI in your browser
```

### Linux

1. 下載 [Provisa-linux-x86_64.AppImage](https://provisa.dev/dl/linux)（恆為最新版本）
2. 賦予其可執行權限並執行——首次啟動會完成一次性設定（無須連接網際網路）：

```bash
chmod +x Provisa-*-linux-x86_64.AppImage
./Provisa-*-linux-x86_64.AppImage
provisa start && provisa open
```

### Windows

1. 下載 [Provisa-windows-x64.exe](https://provisa.dev/dl/windows)（恆為最新版本）
2. 執行安裝程式——無須管理員權限
3. 於開始功能表開啟 **Provisa First Launch**——完成一次性設定（約 5 分鐘，無須連接網際網路）
4. 開啟新的終端機：

```bash
provisa start
```

### 第一則查詢

於本機開發環境中（`PROVISA_MODE=test`），無須任何憑證。於生產環境中，則以 Bearer 權杖進行驗證——角色會自動由權杖中擷取。

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

下載 [provisa-jdbc.jar](https://provisa.dev/dl/jdbc)（恆為最新版本），並將其新增至您 BI 工具的驅動程式路徑。

```text
jdbc:provisa://localhost:8815
```

以您的 Provisa 使用者名稱及密碼進行驗證——伺服器會指派您的角色。

- **`catalog` 模式**——完整結構描述可見；供目錄工具（Collibra、Atlan、DBeaver）使用

Tableau 及 Power BI 的設定步驟，請參閱 [docs/integrations.md](docs/integrations.md)。

### PostgreSQL 線路通訊協定 (pgwire)

Provisa 於連接埠 5439 上使用 PostgreSQL 線路通訊協定。任何能連接至 Postgres 的客戶端，均可連接至 Provisa——無須驅動程式、無須配接器、無須變更既有工具。

**PostgreSQL 使用者名稱決定 Provisa 角色。**於 `provider: none`（信任模式）下，密碼會被忽略，且任何已設定的角色名稱均會被接受作為使用者名稱——以 `analyst`、`admin` 或任何角色連線，即可看到該角色所受治理的數據視圖。於 `provider: simple` 下，密碼會以 bcrypt 驗證。其他提供者（`firebase`、`keycloak`、`oauth`）於 pgwire 上不受支援。

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

所有查詢均經完整治理管線執行——領域存取、行級安全、遮罩及判詞防護，套用方式與 GraphQL 及 REST 完全相同。結構描述瀏覽工具（DBeaver、DataGrip、pgAdmin）開箱即用：`pg_catalog` 及 `information_schema` 查詢由記憶體內目錄作答，範圍限於該角色的領域存取，因此使用者僅會看到其獲准查詢的資料表及欄位。

DataGrip 透過 pgwire 瀏覽受治理結構描述及其外部索引鍵圖表——無須驅動程式、無須配接器：

![Provisa in DataGrip over pgwire](docs/images/pgwire-datagrip.png)

透過設定 `PROVISA_PGWIRE_CERT` 及 `PROVISA_PGWIRE_KEY` 即可啟用 TLS。連接埠可透過 `PROVISA_PGWIRE_PORT` 設定（預設為 `5439`）。

### Bolt (Neo4j 線路通訊協定)

Provisa 亦支援 Neo4j 的 **Bolt** 通訊協定，因此圖形原生工具可直接連線，並對聯邦圖形執行 Cypher——無須匯出，亦無須另立圖形資料庫。將 **Neo4j Browser** 或 **Bloom** 指向 Provisa，即可跨數據來源遍歷關係，並套用相同治理機制（領域存取、行級安全、遮罩）。

Neo4j Browser 透過 Bolt 對 Provisa 執行 Cypher——節點標籤、關係型別及屬性鍵均直接來自已註冊結構描述：

![Provisa in Neo4j Browser over Bolt](docs/images/bolt-neo4j-browser.png)

透過設定 `PROVISA_BOLT_PORT` 即可啟用（Neo4j 的預設值為 `7687`）。TLS 則以 `PROVISA_BOLT_CERT` 及 `PROVISA_BOLT_KEY` 啟用。已驗證使用者所持有的每個 Provisa 角色，均會呈現為一個可選取的 `provisa_<role>` 資料庫（上圖中的 `provisa_admin` 選項）——選取其一即可將工作階段限縮至該角色的領域權限;使用者永遠無法超越其所持有的角色範圍。

### Python 客戶端

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

完整參考，請參閱 [docs/python-client.md](docs/python-client.md)。

## 文件

| 主題 | 文件 |
| --- | --- |
| 開發人員快速入門（從原始碼執行） | [docs/quickstart.md](docs/quickstart.md) |
| 完整 YAML 設定參考 | [docs/configuration.md](docs/configuration.md) |
| 端點參考（GraphQL、REST、Flight、gRPC） | [docs/api-reference.md](docs/api-reference.md) |
| 系統設計及元件對照 | [docs/architecture.md](docs/architecture.md) |
| 安全模型（行級安全、遮罩、驗證） | [docs/security.md](docs/security.md) |
| 支援的數據來源型別 | [docs/sources.md](docs/sources.md) |
| SSE 訂閱 | [docs/subscriptions.md](docs/subscriptions.md) |
| JDBC、BI 工具、Arrow Flight 客戶端、Apollo Federation | [docs/integrations.md](docs/integrations.md) |
| Python 客戶端（`provisa-client`） | [docs/python-client.md](docs/python-client.md) |
| 管理 API | [docs/admin.md](docs/admin.md) |
| 部署（Docker Compose、Kubernetes、macOS） | [docs/deployment.md](docs/deployment.md) |
| Hasura v2 / DDN 匯入 | [docs/import.md](docs/import.md) |
| 發佈流程（alpha/beta/stable 標籤） | [docs/releasing.md](docs/releasing.md) |

## 容量規劃

Provisa 內建一個聯邦引擎，供多數據來源查詢使用。首次啟動時，您可選擇一個記憶體 (RAM) 預算，Provisa 會自動推算本機聯邦工作節點的數量。

| 主機記憶體 | 工作節點 | 典型工作負載 |
| --- | --- | --- |
| < 24 GB | 0 | 開發、單一數據來源查詢、小型團隊 |
| 24–47 GB | 1 | 小型團隊、中度跨數據來源查詢 |
| 48–95 GB | 2 | 部門級部署、混合 BI 及筆記本用途 |
| 96 GB 以上 | 4 | 大型部門、繁重的並行聯邦查詢 |

工作節點數量可隨時變更——編輯 `~/.provisa/config.yaml`（`federation_workers: N`）並執行 `provisa restart` 即可。設為 `0` 則僅執行協調（單節點模式）。

### 擴展至單一主機以外

**水平擴展**——於負載平衡器後方執行多個 Provisa 執行個體。每個執行個體均為一套完整運作的系統。所有執行個體必須指向同一個設定資料庫（於次要主機上設定 `CONFIG_DB_HOST`），並可選擇性地共用同一個 Redis 執行個體（`REDIS_URL`）以取得統一快取。大多數查詢均可透明分散處理；極大型的跨數據來源連接可能超出單一執行個體的資源上限，需要更大型的主機或外部聯邦叢集。

**共用 Redis**——於每個執行個體上設定 `REDIS_URL`，指向一個外部 Redis。共用 Redis 意指來自其中一個執行個體的快取項目，可供叢集內所有執行個體使用，藉此提升整體命中率。

**自備聯邦叢集**——將 Provisa 指向現有的外部聯邦叢集，而非使用內嵌工作節點。建議用於大規模或雲端部署；設定方式請參閱 [docs/deployment.md](docs/deployment.md)。

## 授權條款

Business Source License 1.1（未經修改，依 MariaDB 之授權人契約）。每個
已發佈版本，於其公開發佈週年的第 4 年會轉換為 Change License（GPL v2.0 或
更新版本）；目前及近期程式碼則維持在 BSL 之下。
超出附加使用授予門檻（少於 100 名員工/承包商，且上一年度營收低於 100 萬
美元）的生產環境使用，須取得商業授權。詳見 [LICENSE](LICENSE)。

授權人不同意將本著作用於 AI/ML 訓練。詳見
[NOTICE](NOTICE)、[ai.txt](ai.txt) 及 [robots.txt](robots.txt)。如需商業
或 AI 訓練授權：<kennethstott@gmail.com>
</content>
