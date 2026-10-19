# Provisa

**接上你的資料庫。以 GraphQL、gRPC、SQL 或 MCP 查詢——經任何 API 或協定——5 分鐘內完成。**

Provisa 在您的全部數據之上提供一個 API。把它指向您的資料庫及業務應用程式，描述它們如何互相關聯，然後透過 REST、GraphQL、SQL、gRPC、MCP 等方式查詢連接後的結果。

這之所以可行，是因為一切都來自同一個模型：您的網域、資料表之間的關係，以及作用在它們之上的政策，涵蓋所有數據來源。Provisa 依據這個模型運作，也依據它實施治理，所以沒有任何需要同步的東西。已註冊的關係是查詢唯一可以進行的連接，存取政策會編譯進每一個查詢計劃。由這一個模型帶來四件事：

- **聯邦。** 73 個連接器可以連到 85 種以上的數據來源，其中包括 Salesforce、ServiceNow 及 Stripe。支撐您業務運作的 SaaS 系統會作為資料表出現在您的資料庫旁邊，就地讀取，不需要先建立管道把數據送進數據倉庫。以 SQL、GraphQL 或 Cypher 查詢它們，可經由九種協定。
- **指標。** 指標是有名稱、受治理的聚合。只需定義一次，任何工具、任何語言來問，得到的都是同一個定義。
- **本體。** 業務詞彙表是一張由概念、概念定義以及概念之間關係構成的圖。Provisa 把它對應到保存這些值的資料表及欄位上，因此能夠回答關於您數據的問題。一個概念在直接或經由其他概念連到數據之前，始終是草稿。詞彙表說明一個概念是甚麼；指標說明一個數字怎樣計算。
- **治理。** 六層檢查會編譯進每個查詢計劃，涵蓋每種語言、每種協定；每項請求都以同樣的方式記錄：誰查詢了甚麼、以甚麼角色、依據甚麼政策。追蹤、指標及日誌亦作為可查詢的資料表，註冊在您的業務數據旁邊。

您可以用 **GraphQL、Cypher 或 SQL** 提問，透過 **pgwire、Bolt、gRPC、REST、Arrow Flight 或 JDBC** 連接。每種查詢語言都會先降為同一種中間表示，治理就在那裏施加，只施加一次，因此政策不會在不同語言之間出現偏差。輸出時，這種形式會被翻譯成各數據來源自己的方言。新增一種查詢語言，就是在同一個核心上新增一個前端。

讀取及寫入都經過它。跨數據來源的查詢透過聯邦層分發。寫入，以及只涉及單一數據來源的讀取，會直接交給該數據來源的驅動程式，治理方式相同，通常在 100 毫秒內返回。內置 Arrow Flight 欄式串流傳輸。

模型的詞彙很少：網域、關係、角色及政策。這令它易讀、易稽核。讀一遍政策集，就知道它做甚麼。Provisa 本身是一個輕量的查詢編譯器，而不是擋在數據路徑中間的執行環境。它把請求轉成原生查詢，路由出去，然後讓開，所以整套數據始終保持高速。

它有兩種用法，可以並用：

- **用於現代化改造。** 為您的數據建模，讓 Provisa 為每個數據來源產生原生 SQL，再把這些 SQL 拿到目標系統中採用。Provisa 是過渡期的橋樑，之後可以不再使用。
- **用於長期治理。** 把它保留為每項查詢都要經過的路徑，讓定義、執行及稽核在數據存在的整段期間始終在一起。

## 試用示範 {: #try-a-demo }

最快的方式是在瀏覽器中打開線上示範，無需安裝任何東西：[provisa.dev/demos](https://provisa.dev/demos)。每個示範都會打開一個附導覽的私人沙盒。

在本存放庫的檢出副本中，你可以在自己的機器上執行任何示範：

```bash
./start-ui-install.sh --list-demos     # the demos you can run, and which are published
./start-ui-install.sh --demo weblogs   # start one, with its tour
```

已發佈的示範以現成的套件拉取；其他示範從 `demo/named/<name>/` 執行。所有選項見 [docs/sales-engineer-demos.md](docs/sales-engineer-demos.md)。

## 聯邦模型 {: #the-federation-model }

整個設計靠四個想法支撐。每個數據來源都歸結為資料表。每個查詢都歸結為一種類似 SQL 的形式。可達性決定哪些內容即時查詢、哪些先複製。新鮮度策略則令每份副本及每個衍生數據集保持在需要的新鮮程度。

先說數據來源。每個數據來源都表示為一組建立在同一套共用類型系統上的二維資料表，這是數據來源加入整套數據所要滿足的唯一契約。MySQL 或 PostgreSQL 的資料表本來就是這樣的資料表。GraphQL 查詢結果展平之後亦是。SPARQL 三元組儲存及 Neo4j 看起來完全不像資料表，但您提供一個結果為表格的查詢，這個查詢就成了配接器。無論是甚麼數據來源，系統看到的都是資料列、欄位及類型。接入一種新的數據來源，就是滿足這一個契約，有時需要一點人手幫助，而不是另寫一套度身訂造的整合。

查詢亦同樣處理。SQL 無論有多少方言，都是在資料表上做分析的語言，所以類似 SQL 的形式是天然的共同目標。因此，每項請求無論以甚麼語言到來，都會先降為這種形式。SQL 及 GraphQL 降起來很容易。Cypher 的路徑及圖語義需要真正下工夫，但亦能做到。由於一切在其他處理之前都先經過同一種形式，無論請求用甚麼語言，治理都只在一個地方施加。

有了表格化的數據來源及單一的查詢形式，聯邦就能同時涵蓋即時查詢及數據倉庫：既包括 Trino 這類即時查詢引擎能做的，亦包括這類引擎所倚賴的實體化。**可達性**把兩者連在一起。引擎能就地查詢某個數據來源嗎，還是必須先把它的數據複製到可查詢的地方？答案把整套數據分成即時讀取的部分及複製的部分。

大部分資料庫已經有某種即時連結：DuckDB 的 `ATTACH`、PostgreSQL 的 `postgres_fdw`、Databricks 的外部連結。所以大部分資料庫在一定程度上都能充當聯邦引擎。但沒有一個能涵蓋一切。每個都只能連到特定的一組數據來源，其餘的就複製，而且沒有一個會清楚說明哪些屬於哪一類。模型把可達性明確下來，作為每個數據來源一組確定的方法，說明引擎能即時連到甚麼，以及反過來需要複製甚麼。

剩下的是新鮮度。對於引擎連不到的每個數據來源，它的副本需要多新？實際上答案只有幾種：按需要、按排程、按變更訊號（CDC、水位線、快照），或固定不變。為每個數據來源選一種，就是全部的新鮮度策略。

衍生數據集亦套用同一種形式：衍生資料表、聚合，以及轉換的輸出。它們以同一種形式編寫，所以血緣自然就有了。由每個來源系統到最終輸出的路徑，就是產生它的那個定義，由頭到尾都讀得懂。建構它們時，新鮮度問題會再向前走一步：數據集是按排程重新整理、只在前置條件滿足時重新整理、持續重新整理，還是作為固定的歷史快照？可選項與數據來源副本是同一小組，所以衍生數據集的建構策略用的是同一套詞彙。

維度模型可以直接由此得出。星型模型中的事實及維度，與其他任何衍生數據集一樣。維度是一致化、去重後的投影，事實表是歸約到某個粒度的連接加聚合，各自都有自己的建構及新鮮度策略。緩慢變化維度不需要任何特殊機制：固定快照就是第 2 類歷史，按排程重建就是第 1 類。由於模式定義在模型中，而不是綁定在某個數據倉庫的資料表上，同樣的事實及維度可以在 Oracle 或 Databricks 中建構，亦可以在 MPP 引擎上保持為虛擬的，無需重新建模。

Data Vault 亦以同樣的方式配合，只是早一層。中心表（hub）是去重後的業務鍵數據集，連結（link）是它們之間註冊的關係，衛星（satellite）是只插入、附有時間戳記、保存歷史的屬性數據集。衛星就是採用變更訊號策略的衍生數據集：載入日期加 hashdiff，就是作用在描述性屬性上的變更擷取，而只插入的歷史就是固定快照策略。時間點表及橋接表則是為查詢速度而建的更多衍生數據集。因此，原始保險庫是模型中的一組衍生數據集，星型模型是從它投影出來的，兩者都是產生出來的，都能在引擎之間遷移。方法論仍然由您決定：甚麼成為中心表、衛星的粒度、怎樣拆分。一旦決定，它就存在於模型中，而不是焊死在某個數據倉庫上的 ETL 裏。

這兩種模式都以**兩個快捷宣告**來宣告，而不用手寫檢視。它們是所有星型模型及所有 Data Vault 的構件，不會把您綁定在其中任何一種方法上：

- **`entity`** 是數據來源上附有鍵、去重、可以保留歷史的投影。宣告鍵、屬性及歷史模式，Provisa 就會建構一個實體化檢視。如要求保留歷史，它會改為建構一個**雙時態 MV**（`scd2` → delta，`snapshot` → snapshot）。一個構造就能得到 Kimball 的**維度**（SCD1 或 SCD2）或 Data Vault 的**中心表加衛星**。
- **`fact`** 連接到實體鍵，歸約到宣告的粒度，並聚合量度。Provisa 建構一個聚合 MV，以及到各實體的註冊關係。一個構造就能得到星型的**事實表**或 Data Vault 的**連結**；沒有量度的事實就是純鍵集合連結。

每個 `entity` 或 `fact` 都會準確地變成建模者原本要手寫的實體化檢視、雙時態設定及關係，所以數據倉庫始終留在模型中，可以在引擎之間遷移而無需重新建模。可以在管理介面（**Model** 表單）或透過管理 API（`registerEntity` / `registerFact`）宣告。模型會為您產生 Kimball 星型或 Data Vault，而不會強加其中任何一種。

### 時間旅行 {: #time-travel }

時間旅行的意思是保留一列的每個版本而不是覆寫它，這樣您就能問數據在過去任何時刻是甚麼樣子。不同引擎做這件事的效率差別很大，所以 Provisa 把它作為實體化檢視**定義**的一部分，而不是儲存引擎的一部分。宣告一次，就能在任何能存放實體化檢視的儲存上使用。

令它可以遷移的，是它**只附加**。一個版本一經寫入，就不會再更新或刪除。常見的雙時態技巧是回寫一個「有效至」日期來關閉一列，但這需要 UPDATE，而很多引擎在聯邦儲存上做不到廉價的更新，甚至根本做不到。所以每次重新整理都改為**附加**，某一時刻生效的是哪個版本，在讀取時從日誌中推算出來。附加有兩種方式：

- **Snapshot** 附加整個最新的數據集，並加上今次重新整理的系統時間戳記。不需要比較差異，在每個引擎上都正確，但每次重新整理儲存都會增加一整份副本。
- **Delta** 只附加有變化的內容，再加上已刪除鍵的墓碑。差異由引擎自己以 `INSERT … SELECT` 中的反連接算出來，而不是由 Provisa 逐列合併。它較小，但需要實體鍵。

Provisa 以這種方式管理系統時間，亦即它記錄一個版本的時間。有效時間，亦即一個事實在業務上成立的時間，來自檢視自身的 SELECT，並會保留下來。能做得更好的引擎，例如原生 Iceberg 快照或改動較少列的 MERGE，可以放在同一個宣告背後使用；只附加的路徑是在任何地方都正確的底線。

讀取是透明的。對雙時態 MV 的一般查詢會傳回由日誌重建的**目前**狀態。要回到過去，發送 `X-Provisa-As-Of: <timestamp>` 請求標頭，整項查詢就會按當時的數據狀態作答，在每種儲存上都一樣。再加上 `X-Provisa-As-Recorded`，就能按 Provisa 在某一時刻所記錄的樣子讀取數據，這樣之後記錄的更正就不會改變對過去的重播。可以在管理介面（**Time Travel** 控制項：關閉、snapshot 或 delta，再加一個實體鍵）或透過管理 API 為任何實體化檢視開啟它。

可達性及新鮮度合在一起，構成一個通用的聯邦模型：定義哪些是即時的、哪些是複製的、每份副本保持多新，而不倚賴任何一個引擎能連到甚麼。模型是可遷移的，所以您的數據不會被綁定在今天剛好能連到最多數據來源的那家供應商身上。

## 功能 {: #features }

### 查詢介面 {: #query-interfaces }

這些是你撰寫查詢所用的語言與結構化 API。各有自己的語法與語意；治理（RLS、遮罩、資料行可見性、關係強制執行）在它們之上一致套用，與由哪一種 wire 協定送達無關。

- **GraphQL**——逐角色的結構描述，帶欄位層級可見性、篩選、游標式分頁與彙總查詢（`count`、`sum`、`avg`、`min`、`max`）。受結構描述約束於已註冊關係——靠構造即結構有效，是通往一個正確簡單查詢的最快路徑。內含 Apollo APQ：查詢被雜湊並在伺服器端註冊；後續呼叫經 HTTP GET 只送出雜湊值，使回應可被 CDN 快取而毋須任何用戶端變更。低於可設定資料列門檻的查詢對照表，會以列舉類型呈現。
- **SQL**——在聯邦數據之上的完整 SQL；不受約束，比 GraphQL 更具表達力。寫標準 SQL——相關子查詢等等一應俱全——它就原封不動地跨數據來源執行。單一來源查詢完全繞過聯邦層（低於 100 毫秒）。
- **Cypher**——建於同一份聯邦結構描述之上的圖形查詢語言。把關係當作圖形邊來走訪；聯集數據來源；變長路徑。治理與 GraphQL 和 SQL 完全相同地套用。
- **gRPC 模型 API**——自已註冊結構描述自動產生 `.proto`；在角色結構描述的根部提供逐表的具型別查詢與插入 RPC，串流回應。與 GraphQL 同義地由結構描述驅動——註冊模型就是契約，protobuf 是 wire 編碼。與 Arrow Flight（那是一種資料行式串流傳輸）不同，這是一個完整的逐表查詢介面。
- **JSON:API**——位於 `/data/jsonapi/{table}` 的結構化查詢 API，設計上僅限 HTTP。支援 JSON:API 1.1：稀疏欄位集（`fields[table]=col1,col2`）、篩選運算式（`filter[field][op]=value`）、複合文件（`include=relation`）與排序。它不是一種通用查詢語言——一次查詢一張表，用標準化的篩選語法而非臨時查詢字串。
- **查詢語言探索器**——寫一份 GraphQL 查詢，就在側邊面板看見即時的**語意 SQL** 與 **Cypher** 翻譯；複製其一，或直接跳進 SQL 或圖形編輯器。一種實用的工作流程是先用 GraphQL 勾勒查詢片段，再把產生的 SQL 縫進複雜的檢視或報表。

探索器把一份 GraphQL 查詢與它即時的 SQL 和 Cypher 翻譯並排顯示：

![查詢語言探索器](docs/images/query-explorer.png)

同一份聯邦結構描述可作為一張即時圖形探索——網域與節點標籤、關係類型與變長走訪：

![圖形視覺化](docs/images/graph-view.png)

### 查詢組合工具 {: #query-composition-tools }

這些工具幫助你以上述語言撰寫查詢——它們本身不是查詢語言。

- **自然語言查詢**——由 Claude 驅動的 NL→SQL/Cypher/GraphQL 管線。以日常英文描述你想要什麼；管線以你選定的語言產出一份查詢，並在執行前提供一個互動式驗證迴圈。

![自然語言查詢](docs/images/natural-language.png)

### Wire 協定 {: #wire-protocols }

這些是連線協定。SQL、GraphQL 與 Cypher 都乘載其上——wire 協定的選擇不改變查詢介面或治理行為。

- **pgwire**——任何 PostgreSQL 用戶端（psql、DBeaver、DataGrip、asyncpg、SQLAlchemy、pandas `read_sql`）都在連接埠 5439 上連線，就如同它是一台 Postgres 伺服器。只接受 SQL。完整治理管線照樣套用。`pg_catalog` 與 `information_schema` 自記憶體內目錄作答，因此結構描述瀏覽器毋須聯邦來回即可運作。TLS 可選。
- **Bolt（Neo4j）**——任何 Neo4j 用戶端（Neo4j Browser、Bloom、官方驅動程式）經 Bolt 協定連線，並針對聯邦圖形執行 Cypher。使用者持有的每個角色都呈現為一個 `provisa_<role>` 資料庫。與其他每一種傳輸同樣的治理。TLS 可選。
- **Arrow Flight**——經 gRPC 的高吞吐量資料行式串流；接受 GraphQL 或 SQL 作為查詢輸入。結果集無上限，無伺服器端具體化，毋須另建基礎設施。
- **JDBC**——整合 BI 工具（Tableau、Power BI、DBeaver）。
- **WebSocket / SSE**——訂閱：近即時變更事件；後端：PG 原生、MongoDB 原生、CDC、輪詢。同樣經 Kafka 呈現。

### 數據來源 {: #data-sources }

- **73 個連接器，85 種以上數據來源**——較大的數字分別計入託管的 PostgreSQL 服務（RDS、Aurora、Cloud SQL、AlloyDB、Azure、Supabase、Neon）和 File Crawler 的檔案格式。 PostgreSQL、MySQL、MongoDB、Cassandra、Elasticsearch、Neo4j、SPARQL 三元組儲存、Kafka、Google Sheets 等等，都經單一個 API；圖形與 RDF 數據來源是一級公民，不是轉接器
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

### 安全與治理 {: #security-governance }

- **資料列層級安全**——逐表、逐角色的 WHERE 子句注入
- **資料行遮罩**——逐資料行遮罩（regex、常數、截斷），可依角色略過
- **資料行預設值**——在插入／更新時注入的伺服器端靜態值或工作階段變數值；不在變更操作輸入類型中呈現
- **寫入權限**——逐資料行的變更操作存取控制（`writable_by`）
- **繼承角色**——角色遞迴地自父角色繼承 RLS、可見性與遮罩
- **受追蹤函式與 webhook**——DB 函式與外送 webhook 以具型別回傳形狀呈現為 GraphQL 變更操作
- **ABAC 核准掛鉤**——執行前授權掛鉤；webhook、gRPC 或 unix_socket 傳輸；逐表、逐數據來源或全域範圍；備援政策可設定
- **可插拔驗證**——Firebase、Keycloak、OAuth 2.0、simple（測試用）

![安全角色](docs/images/security-roles.png)

### 交付與效能 {: #delivery-performance }

- **作為已記錄轉換的具體化檢視**——一個 MV 擷取產生它的那次轉換：它的聯結形狀或 SQL、它據以建置的逐數據來源輸入訊號（Iceberg 快照、RDB 水位），以及註冊時的一次決定性檢查。因為轉換被記錄下來，查詢（或子運算式）可被透明地改寫到一個新鮮的 MV 之上——結構式聯結樣式比對並支援部分比對，因此一個只涵蓋部分聯結的 MV 仍然適用，其餘聯結會被保留
- **熱表內嵌**——小型且頻繁聯結的查詢對照表，直接在查詢計劃中以 VALUES CTE 內嵌，消除維度數據的跨來源來回
- **查詢快取**——依角色＋RLS 分割的 Redis 結果快取；內含 APQ 雜湊快取
- **可觀測性即數據**——分散式追蹤、指標與記錄檔經 OpenTelemetry 收集，壓實成 S3 上的 Iceberg，並自動註冊為聯邦結構描述中可查詢的表（`traces`、`metrics`、`logs`、`queries`）；用 SQL、GraphQL 或 Cypher 與你的業務數據並排查詢它們——把一張 `customers` 表聯結到 `queries` 表，就能看出誰跑了什麼、跑了多久

### 管理與整合 {: #administration-integration }

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

## 安全模型 {: #security-model }

這裡正是「就在每次查詢已經要走的那條路徑上」不再只是一句口號之處。Provisa 跨每一種查詢語言（GraphQL、SQL、Cypher）與每一種傳輸（REST、gRPC、Arrow Flight、JDBC、pgwire、Bolt、WebSocket）強制執行一套多層安全模型。治理一致地套用——不存在繞過它的查詢路徑。涵蓋是靠構造而完備，不是靠勤勉：加入一個數據來源、資料行或關係，每一層都自動套用到它，沒有什麼需要記得去註冊。

各層依序套用。一次請求必須通過每一層，下一層才會被評估。

### 第 0 層——自省篩選 {: #layer-0-introspection-filtering }

呈現給某個角色的結構描述與目錄，只包含其 `domain_access` 清單中的表，以及通過逐資料行 `visible_to` 規則的資料行。在角色存取範圍之外的物件，在探索時就不可見——它們無法被查詢、被自動完成，也無法被推斷存在。這適用於 GraphQL 結構描述、SQL 目錄，以及查詢編輯器的結構描述瀏覽器。

### 第 1 層——公開存取 {: #layer-1-public-access }

位於沒有 `domain_access` 限制之網域中的表，對所有已驗證身分可見，毋須額外設定。對真正公開的數據零阻力。

### 第 2 層——網域存取 {: #layer-2-domain-access }

每個角色帶著一份網域 ID 的 `domain_access` 清單。觸及那些網域以外之表的查詢，會在執行前被拒絕。這是粗粒度的所有權邊界——一個 HR 角色無論 SQL 怎麼寫，都觸及不到財務表。

### 第 3 層——資料列層級安全 {: #layer-3-row-level-security }

在網域存取獲確認之後，逐表、逐角色的 `WHERE` 述詞會在執行時注入每一份 `SELECT`。這些述詞針對原始數據求值。一位區域經理查詢一張共用的訂單表，即使下 `SELECT *`，也只看見自己區域的資料列。

### 第 4 層——資料行可見性與遮罩 {: #layer-4-column-visibility-and-masking }

`visible_to` 清單不含請求角色的資料行，會自查詢輸出中剝除。帶遮罩規則的資料行，其值在結果離開伺服器之前被替換——regex 塗黑、常數替換或截斷。遮罩在所有查詢語言與輸出格式中都套用。

### 第 5 層——述詞防護 {: #layer-5-predicate-guard }

被遮罩的資料行不得出現在 `WHERE` 與 `HAVING` 子句中。若無此層，即使輸出被遮罩，呼叫端也能在篩選中二分搜尋而推斷出未遮罩的值。拒絕在查詢剖析時就強制執行，早於執行。

### 關係治理 {: #relationship-governance }

SQL 中的 JOIN 條件，必須符合表與表之間一項已註冊、已核准的關係。未經核准的聯結會被拒絕。每項關係都帶一段供人閱讀的理由與說明——向使用者與自主代理說明某條走訪路徑為何存在的指引。這是治理政策，不是一道硬性安全邊界：無論聯結結構如何，第 2–5 層都成立，因此蓄意規避不會揭露該角色透過兩次分開的查詢也觸及不到的數據。規避嘗試會被記錄且可審計。

無效的聯結永遠不會在你的資料上執行。角色預設受管治；退出是對你信任的角色有意授予 `ignore_relationships` 權限。智能體的 OAuth 令牌對應到一個角色，因此智能體與人獲得相同的層。提供脈絡層的資料目錄告訴智能體它應該做什麼；Provisa 強制它能做什麼。

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

### 第一個查詢 {: #first-query }

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

### JDBC（Tableau、DBeaver、Power BI） {: #jdbc-tableau-dbeaver-power-bi }

下載 [provisa-jdbc.jar](https://provisa.dev/dl/jdbc)（一律為最新版本），並把它加入你的 BI 工具驅動程式路徑。

```text
jdbc:provisa://localhost:8815
```

以你的 Provisa 使用者名稱與密碼驗證——伺服器指派你的角色。

Tableau 與 Power BI 的設定步驟見 [docs/integrations.md](docs/integrations.md)。

### PostgreSQL Wire 協定（pgwire） {: #postgresql-wire-protocol-pgwire }

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

### Bolt（Neo4j Wire 協定） {: #bolt-neo4j-wire-protocol }

Provisa 同樣說 Neo4j 的 **Bolt** 協定，因此圖形原生工具可直接連線，並針對聯邦圖形執行 Cypher——毋須匯出，毋須另一套圖形資料庫。把 **Neo4j Browser** 或 **Bloom** 指向 Provisa，即可在套用同樣治理（網域存取、RLS、遮罩）的前提下跨數據來源走訪關係。

Neo4j Browser 針對 Provisa 執行 Cypher——節點標籤、關係類型與屬性索引鍵，全部直接來自已註冊的結構描述：

![以 Bolt 在 Neo4j Browser 中的 Provisa](docs/images/bolt-neo4j-browser.png)

設定 `PROVISA_BOLT_PORT` 即啟用它（Neo4j 的預設值是 `7687`）。TLS 以 `PROVISA_BOLT_CERT` 與 `PROVISA_BOLT_KEY` 啟用。已驗證使用者持有的每個 Provisa 角色，都呈現為一個可選取的 `provisa_<role>` 資料庫（上圖中的 `provisa_admin` 選擇器）——選定其一會把工作階段收窄到該角色的網域權限；使用者永遠不可能超出他們持有的角色。

### Python 用戶端 {: #python-client }

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

# role URL parameter
engine = create_engine(
    "provisa+http://alice:secret@localhost:8001?role=analyst"
)

# ADBC — Arrow-native streaming via Flight
from provisa_client.adbc import adbc_connect
with adbc_connect("http://localhost:8001", user="alice", password="secret") as conn:
    with conn.cursor() as cur:
        cur.execute("{ orders { id amount } }")
        table = cur.fetch_arrow_table()
```

完整參照見 [docs/python-client.md](docs/python-client.md)。

## 文件 {: #documentation }

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
| 建構測試環境（剖析、偽造數據、合成數據） | [docs/test-data.md](docs/test-data.md) |
| 偽造數據方法參考 | [docs/fake-methods.md](docs/fake-methods.md) |
| 在筆記型電腦上執行及建構示範（售前工程師） | [docs/sales-engineer-demos.md](docs/sales-engineer-demos.md) |
| 支援的數據來源類型 | [docs/sources.md](docs/sources.md) |
| SSE 訂閱 | [docs/subscriptions.md](docs/subscriptions.md) |
| JDBC、BI 工具、Arrow Flight 用戶端、Apollo Federation | [docs/integrations.md](docs/integrations.md) |
| Python 用戶端（`provisa-client`） | [docs/python-client.md](docs/python-client.md) |
| 管理 API | [docs/admin.md](docs/admin.md) |
| 部署（Docker Compose、Kubernetes、macOS） | [docs/deployment.md](docs/deployment.md) |
| Hasura v2 / DDN 匯入 | [docs/import.md](docs/import.md) |
| 發佈流程（alpha/beta/stable 標籤） | [docs/releasing.md](docs/releasing.md) |

## 規模設定 {: #sizing }

Provisa 內含一具供多來源查詢使用的內建聯邦引擎。首次啟動時你選定一個 RAM 預算；Provisa 會自動推導本機聯邦工作處理程序的數量。

| 主機 RAM | 工作處理程序 | 典型工作負載 |
| --- | --- | --- |
| < 24 GB | 0 | 開發、單一來源查詢、小團隊 |
| 24–47 GB | 1 | 小團隊、中等程度的跨來源查詢 |
| 48–95 GB | 2 | 部門級部署、BI 與筆記本混合使用 |
| 96 GB+ | 4 | 大型部門、高併發聯邦 |

工作處理程序數量可隨時變更：編輯 `~/.provisa/config.yaml`（`federation_workers: N`）並執行 `provisa restart`。設為 `0` 即以僅協調模式執行（單節點）。

### 擴展到單機以外 {: #scaling-beyond-a-single-box }

**水平擴展**——在一個負載平衡器後方執行多個 Provisa 執行個體。每個執行個體都是一套功能完整的系統。所有執行個體必須指向同一個設定 DB（在次要機器上設定 `CONFIG_DB_HOST`），並可選擇指向一個共用的 Redis 執行個體（`REDIS_URL`）以取得統一快取。多數查詢會透明地分散；非常大的跨來源聯結可能超出單一執行個體的資源，需要更大的機器或一個外部聯邦叢集。

**共用 Redis**——在每個執行個體上設定 `REDIS_URL` 指向一個外部 Redis。共用 Redis 意味著來自一個執行個體的快取項目對所有執行個體都可用，提升整個叢集的命中率。

**自備聯邦叢集**——把 Provisa 指向一個既有的外部聯邦叢集，取代內嵌工作處理程序。建議用於大規模或雲端部署；設定方式見 [docs/deployment.md](docs/deployment.md)。

## 授權 {: #license }

Business Source License 1.1（未經修改，依 MariaDB 的 Licensor 承諾）。每個
已發佈版本在其公開發佈的第 4 週年轉為 Change License（GPL v2.0 或
更新版本）；當前與近期的程式碼維持在 BSL 之下。
超出 Additional Use Grant 門檻（少於 100 名員工／承包商，且前一年度營收
低於 100 萬美元）的生產使用，需要商業授權。見 [LICENSE](LICENSE)。

Licensor 不同意將本作品用於 AI/ML 訓練。見
[NOTICE](NOTICE)、[ai.txt](ai.txt) 與 [robots.txt](robots.txt)。商業或
AI 訓練授權請洽：<kennethstott@gmail.com>
