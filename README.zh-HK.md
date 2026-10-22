# Provisa

**連接您的資料庫。5 分鐘內即可用 GraphQL、gRPC、SQL 或 MCP 查詢它們，透過任何 API 或協定。**

Provisa 在您的全部資料之上提供一個 API。把它指向您的資料庫及業務應用程式，描述它們如何互相關聯，然後透過 REST、GraphQL、SQL、gRPC、MCP、JSON:API、Arrow Flight、pgwire 或 Bolt 查詢連接後的結果。

這之所以可行，是因為一切都來自同一個模型：您的網域、資料表之間的關係，以及作用在它們之上的政策，涵蓋所有數據來源。Provisa 依據這個模型運作，也依據它實施治理，所以沒有任何需要同步的東西。已註冊的關係是查詢唯一可以進行的連接，存取政策會編譯進每一個查詢計劃。由這一個模型帶來四件事：

- **聯邦。** 73 個連接器可以連到 85 種以上的數據來源，其中包括 Salesforce、ServiceNow 及 Stripe。支撐您業務運作的 SaaS 系統會作為資料表出現在您的資料庫旁邊，就地讀取，不需要先建立管道把數據送進數據倉庫。以 SQL、GraphQL 或 Cypher 查詢它們，可經由九種協定。
- **指標。** 指標是有名稱、受治理的聚合。只需定義一次，任何工具、任何語言來問，得到的都是同一個定義。
- **本體。** 業務詞彙表是一張由概念、概念定義以及概念之間關係構成的圖。Provisa 把它對應到保存這些值的資料表及欄位上，因此能夠回答關於您數據的問題。一個概念在直接或經由其他概念連到數據之前，始終是草稿。詞彙表說明一個概念是甚麼；指標說明一個數字怎樣計算。
- **治理。** 六層檢查會編譯進每個查詢計劃，涵蓋每種語言、每種協定；每項請求都以同樣的方式記錄：誰查詢了甚麼、以甚麼角色、依據甚麼政策。追蹤、指標及日誌亦作為可查詢的資料表，註冊在您的業務數據旁邊。

您可以用 **GraphQL、Cypher 或 SQL** 提問，透過 **pgwire、Bolt、gRPC、REST、Arrow Flight 或 JDBC** 連接。每種查詢語言都會先降為同一種中間表示，治理就在那裏施加，只施加一次，因此政策不會在不同語言之間出現偏差。輸出時，這種形式會被翻譯成各數據來源自己的方言。新增一種查詢語言，就是在同一個核心上新增一個前端。

讀取及寫入都經過它。跨數據來源的查詢透過聯邦層分發。寫入，以及只涉及單一數據來源的讀取，會直接交給該數據來源的驅動程式，治理方式相同，通常在 100 毫秒內返回。內置 Arrow Flight 欄式串流傳輸。

模型的詞彙很少：域、關係、角色及策略。這令它易讀、易稽核，讀一遍策略集，就知道它做甚麼。Provisa 本身是一個輕量的查詢編譯器。它從不作為執行環境擋在您的資料路徑中間。它把請求轉成原生查詢，路由出去，然後讓開，所以整套資料始終保持高速。

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

先說數據來源。每個數據來源都表示為一組建立在同一套共用類型系統上的二維資料表，這是數據來源加入整套數據所要滿足的唯一契約。MySQL 或 PostgreSQL 的資料表本來就是這樣的資料表。GraphQL 查詢結果展平之後亦是。SPARQL 三元組儲存及 Neo4j 看起來完全不像資料表，但您提供一個結果為表格的查詢，這個查詢就成了配接器。無論是甚麼數據來源，系統看到的都是資料列、欄位及類型。接入一種新的數據來源，就是滿足這一個契約，有時需要一點人手幫助。沒有人需要另寫一套度身訂造的整合。

查詢亦同樣處理。SQL 無論有多少方言，都是在資料表上做分析的語言，所以類似 SQL 的形式是天然的共同目標。因此，每項請求無論以甚麼語言到來，都會先降為這種形式。SQL 及 GraphQL 降起來很容易。Cypher 的路徑及圖語義需要真正下工夫，但亦能做到。由於一切在其他處理之前都先經過同一種形式，無論請求用甚麼語言，治理都只在一個地方施加。

有了表格化的數據來源及單一的查詢形式，聯邦就能同時涵蓋即時查詢及數據倉庫：既包括 Trino 這類即時查詢引擎能做的，亦包括這類引擎所倚賴的實體化。**可達性**把兩者連在一起。引擎能就地查詢某個數據來源嗎，還是必須先把它的數據複製到可查詢的地方？答案把整套數據分成即時讀取的部分及複製的部分。

大部分資料庫已經有某種即時連結：DuckDB 的 `ATTACH`、PostgreSQL 的 `postgres_fdw`、Databricks 的外部連結。所以大部分資料庫在一定程度上都能充當聯邦引擎。但沒有一個能涵蓋一切。每個都只能連到特定的一組數據來源，其餘的就複製，而且沒有一個會清楚說明哪些屬於哪一類。模型把可達性明確下來，作為每個數據來源一組確定的方法，說明引擎能即時連到甚麼，以及反過來需要複製甚麼。

剩下的是新鮮度。對於引擎連不到的每個數據來源，它的副本需要多新？實際上答案只有幾種：按需要、按排程、按變更訊號（CDC、水位線、快照），或固定不變。為每個數據來源選一種，就是全部的新鮮度策略。

衍生數據集亦套用同一種形式：衍生資料表、聚合，以及轉換的輸出。它們以同一種形式編寫，所以血緣自然就有了。由每個來源系統到最終輸出的路徑，就是產生它的那個定義，由頭到尾都讀得懂。建構它們時，新鮮度問題會再向前走一步：數據集是按排程重新整理、只在前置條件滿足時重新整理、持續重新整理，還是作為固定的歷史快照？可選項與數據來源副本是同一小組，所以衍生數據集的建構策略用的是同一套詞彙。

維度模型可以直接由此得出。星型模型中的事實及維度，與其他任何衍生數據集一樣。維度是一致化、去重後的投影，事實表是歸約到某個粒度的連接加聚合，各自都有自己的建構及新鮮度策略。緩慢變化維度不需要任何特殊機制：固定快照就是第 2 類歷史，按排程重建就是第 1 類。由於模式定義在模型中，並不綁定在某個數據倉庫的資料表上，同樣的事實及維度可以在 Oracle 或 Databricks 中建構，亦可以在 MPP 引擎上保持為虛擬的，無需重新建模。

Data Vault 亦以同樣的方式配合，只是早一層。中心表（hub）是去重後的業務鍵數據集，連結（link）是它們之間註冊的關係，衛星（satellite）是只插入、附有時間戳記、保存歷史的屬性數據集。衛星就是採用變更訊號策略的衍生數據集：載入日期加 hashdiff，就是作用在描述性屬性上的變更擷取，而只插入的歷史就是固定快照策略。時間點表及橋接表則是為查詢速度而建的更多衍生數據集。因此，原始保險庫是模型中的一組衍生數據集，星型模型是從它投影出來的，兩者都是產生出來的，都能在引擎之間遷移。方法論仍然由您決定：甚麼成為中心表、衛星的粒度、怎樣拆分。一旦決定，它就存在於模型中。沒有任何 ETL 把它焊死在某個數據倉庫上。

這兩種模式都以**兩個快捷宣告**來宣告，而不用手寫檢視。它們是所有星型模型及所有 Data Vault 的構件，不會把您綁定在其中任何一種方法上：

- **`entity`** 是數據來源上附有鍵、去重、可以保留歷史的投影。宣告鍵、屬性及歷史模式，Provisa 就會建構一個實體化檢視。如要求保留歷史，它會改為建構一個**雙時態 MV**（`scd2` → delta，`snapshot` → snapshot）。一個構造就能得到 Kimball 的**維度**（SCD1 或 SCD2）或 Data Vault 的**中心表加衛星**。
- **`fact`** 連接到實體鍵，歸約到宣告的粒度，並聚合量度。Provisa 建構一個聚合 MV，以及到各實體的註冊關係。一個構造就能得到星型的**事實表**或 Data Vault 的**連結**；沒有量度的事實就是純鍵集合連結。

每個 `entity` 或 `fact` 都會準確地變成建模者原本要手寫的實體化檢視、雙時態設定及關係，所以數據倉庫始終留在模型中，可以在引擎之間遷移而無需重新建模。可以在管理介面（**Model** 表單）或透過管理 API（`registerEntity` / `registerFact`）宣告。模型會為您產生 Kimball 星型或 Data Vault，而不會強加其中任何一種。

### 時間旅行 {: #time-travel }

時間旅行的意思是保留一列的每個版本而不是覆寫它，這樣您就能問數據在過去任何時刻是甚麼樣子。不同引擎做這件事的效率差別很大，所以 Provisa 把它作為實體化檢視**定義**的一部分，與儲存引擎無關。宣告一次，就能在任何能存放實體化檢視的儲存上使用。

令它可以遷移的，是它**只附加**。一個版本一經寫入，就不會再更新或刪除。常見的雙時態技巧是回寫一個「有效至」日期來關閉一列，但這需要 UPDATE，而很多引擎在聯邦儲存上做不到廉價的更新，甚至根本做不到。所以每次重新整理都改為**附加**，某一時刻生效的是哪個版本，在讀取時從日誌中推算出來。附加有兩種方式：

- **Snapshot** 附加整個最新的數據集，並加上今次重新整理的系統時間戳記。不需要比較差異，在每個引擎上都正確，但每次重新整理儲存都會增加一整份副本。
- **Delta** 只附加有變化的內容，再加上已刪除鍵的墓碑。差異由引擎自己以 `INSERT … SELECT` 中的反連接算出來。Provisa 從不逐列合併。它較小，但需要實體鍵。

Provisa 以這種方式管理系統時間，亦即它記錄一個版本的時間。有效時間，亦即一個事實在業務上成立的時間，來自檢視自身的 SELECT，並會保留下來。能做得更好的引擎，例如原生 Iceberg 快照或改動較少列的 MERGE，可以放在同一個宣告背後使用；只附加的路徑是在任何地方都正確的底線。

讀取不需要任何特殊語法。對雙時態 MV 的一般查詢會傳回由日誌重建的**目前**狀態。要回到過去，發送 `X-Provisa-As-Of: <timestamp>` 請求標頭，整項查詢就會按當時的數據狀態作答，在每種儲存上都一樣。再加上 `X-Provisa-As-Recorded`，就能按 Provisa 在某一時刻所記錄的樣子讀取數據，這樣之後記錄的更正就不會改變對過去的重播。可以在管理介面（**Time Travel** 控制項：關閉、snapshot 或 delta，再加一個實體鍵）或透過管理 API 為任何實體化檢視開啟它。

可達性及新鮮度合在一起，構成一個通用的聯邦模型：定義哪些是即時的、哪些是複製的、每份副本保持多新，而不倚賴任何一個引擎能連到甚麼。模型是可遷移的，所以您的數據不會被綁定在今天剛好能連到最多數據來源的那家供應商身上。

## 功能 {: #features }

### 查詢介面 {: #query-interfaces }

這些是你撰寫查詢所用的語言與結構化 API。各有自己的語法與語意；治理（RLS、遮罩、資料行可見性、關係強制執行）在它們之上一致套用，與由哪一種 wire 協定送達無關。

- **GraphQL** —— 按角色區分的 schema，具備欄位級可見性、篩選、基於游標的分頁及聚合查詢（`count`、`sum`、`avg`、`min`、`max`）。schema 只提供已登錄的關係，所以每個查詢在構造上就結構有效，GraphQL 亦是寫出正確簡單查詢的最快路徑。內置 Apollo APQ：查詢在伺服器端被雜湊並登錄，後續呼叫只需透過 HTTP GET 傳送雜湊值，回應無須改動用戶端即可被 CDN 快取。列數低於可設定門檻的查找表以列舉類型呈現。
- **SQL** —— 完整的聯邦數據 SQL，不受約束，比 GraphQL 更具表達力。編寫標準 SQL，包括相關子查詢，它都能原樣跨數據來源運行。單源查詢會完全跳過聯邦層（低於 100ms）。
- **Cypher**——建於同一份聯邦結構描述之上的圖形查詢語言。把關係當作圖形邊來走訪；聯集數據來源；變長路徑。治理與 GraphQL 和 SQL 完全相同地套用。
- **gRPC 模型 API** —— 從已登錄的 schema 產生 `.proto`，在角色 schema 的根部為每張表提供帶類型的查詢及插入 RPC，並以串流傳回結果。它與 GraphQL 一樣由 schema 驅動：登錄模型就是契約，protobuf 只是線上編碼。它是完整的逐表查詢介面，而 Arrow Flight 是欄式串流傳輸。
- **JSON:API** —— 位於 `/data/jsonapi/{table}` 的結構化查詢 API，設計上僅支援 HTTP。支援 JSON:API 1.1：稀疏欄位集（`fields[table]=col1,col2`）、篩選運算式（`filter[field][op]=value`）、複合文件（`include=relation`）以及排序。它一次查詢一張表，使用標準化的篩選語法，所以需要通用查詢語言時請使用 SQL 或 GraphQL。
- **查詢語言探索器**——寫一份 GraphQL 查詢，就在側邊面板看見即時的**語意 SQL** 與 **Cypher** 翻譯；複製其一，或直接跳進 SQL 或圖形編輯器。一種實用的工作流程是先用 GraphQL 勾勒查詢片段，再把產生的 SQL 縫進複雜的檢視或報表。

探索器把一份 GraphQL 查詢與它即時的 SQL 和 Cypher 翻譯並排顯示：

![查詢語言探索器](docs/images/query-explorer.png)

同一個聯邦模式亦以即時圖的形式呈現，包含領域及節點標籤、關係類型以及可變長度走訪：

![圖形視覺化](docs/images/graph-view.png)

### 查詢組合工具 {: #query-composition-tools }

這些工具幫助您用上述語言編寫查詢。它們本身不是查詢語言。

- **自然語言查詢** —— 運行在 Claude 上的 NL→SQL/Cypher/GraphQL 流水線。用日常語言描述您想要的內容，流水線會用您選擇的語言寫出查詢，並在執行前提供互動式驗證循環。

![自然語言查詢](docs/images/natural-language.png)

### Wire 協定 {: #wire-protocols }

這些是連線協定。SQL、GraphQL 及 Cypher 都運行在它們之上，選用哪種協定既不改變查詢介面，亦不改變治理行為。

- **pgwire**——任何 PostgreSQL 用戶端（psql、DBeaver、DataGrip、asyncpg、SQLAlchemy、pandas `read_sql`）都在連接埠 5439 上連線，就如同它是一台 Postgres 伺服器。只接受 SQL。完整治理管線照樣套用。`pg_catalog` 與 `information_schema` 自記憶體內目錄作答，因此結構描述瀏覽器毋須聯邦來回即可運作。TLS 可選。
- **Bolt（Neo4j）**——任何 Neo4j 用戶端（Neo4j Browser、Bloom、官方驅動程式）經 Bolt 協定連線，並針對聯邦圖形執行 Cypher。使用者持有的每個角色都呈現為一個 `provisa_<role>` 資料庫。與其他每一種傳輸同樣的治理。TLS 可選。
- **Arrow Flight**——經 gRPC 的高吞吐量資料行式串流；接受 GraphQL 或 SQL 作為查詢輸入。結果集無上限，無伺服器端具體化，毋須另建基礎設施。
- **JDBC**——整合 BI 工具（Tableau、Power BI、DBeaver）。
- **WebSocket / SSE**——訂閱：近即時變更事件；後端：PG 原生、MongoDB 原生、CDC、輪詢。同樣經 Kafka 呈現。

### 數據來源 {: #data-sources }

- **73 個連接器，85 種以上數據來源** —— 較大的數字把託管的 PostgreSQL 服務（RDS、Aurora、Cloud SQL、AlloyDB、Azure、Supabase、Neon）及 File Crawler 的各種檔案格式逐一計算在內。PostgreSQL、MySQL、MongoDB、Cassandra、Elasticsearch、Neo4j、SPARQL 三元組儲存、Kafka、Google Sheets、Kaggle、Salesforce、ServiceNow、Stripe 等都位於同一個 API 之後。圖及 RDF 數據來源與關聯式數據來源地位相同。
- **智慧路由** —— 單源查詢會跳過聯邦（低於 100ms）。多源查詢經由聯邦層執行，可以用您自己的叢集，亦可以用內置的工作節點。
- **API 數據來源** —— 把 REST、GraphQL、gRPC、WebSocket 或 RSS 端點登錄為可查詢的表，並附帶 SPARQL 輔助工具。API 數據來源與關聯式數據來源之間的聯邦連接無須額外設定。
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

- **作為已記錄轉換的具體化檢視** —— MV 會記錄產生它的轉換：它的連接形態或 SQL、建構時所依據的各數據來源輸入訊號（Iceberg 快照、RDB 水位線），以及登錄時的確定性檢查。由於轉換已被記錄，Provisa 可以把查詢或其中一部分改寫到新鮮的 MV 上，無須您修改查詢。比對基於連接模式，並接受部分比對，因此只涵蓋部分連接的 MV 依然適用，其餘連接會被保留。
- **熱表內嵌**——小型且頻繁聯結的查詢對照表，直接在查詢計劃中以 VALUES CTE 內嵌，消除維度數據的跨來源來回
- **查詢快取**——依角色＋RLS 分割的 Redis 結果快取；內含 APQ 雜湊快取
- **可觀測性即數據** —— 分散式追蹤、指標及日誌透過 OpenTelemetry 收集，壓縮存入 S3 上的 Iceberg，並在聯邦模式中登錄為可查詢的表（`traces`、`metrics`、`logs`、`queries`）。您可以用 SQL、GraphQL 或 Cypher 把它們與業務數據放在一起查詢。例如，把 `customers` 表與 `queries` 連接，就能看到誰執行了甚麼、耗時多久。

### 管理與整合 {: #administration-integration }

- **管理 API**——位於 `/admin/graphql` 的 GraphQL；設定上傳／下載、關係編輯、查詢核准
- **報表檢視器**——`/admin/reports` 列出內建的 ops 網域管理檢視，以及任何已註冊的自訂報表；需要 `observability` 能力
- **表預覽**——每張已註冊的表都有一個伺服器分頁的受治理數據檢視器，帶下推篩選、多層群組依據與 CSV 匯出
- **GraphQL Voyager**——以實體關係圖呈現的互動式、依角色設限結構描述視覺化
- **LLM 關係探索** —— 由 Claude 推薦外鍵候選。
- **Python 用戶端**——`pip install provisa-client`；GraphQL/SQL → DataFrame，Arrow Flight → pyarrow Table，SQLAlchemy 方言，ADBC 支援
- **數據擷取**——把 JSON 事件數據推入平台的 HTTP 端點
- **Hasura v2 / DDN 匯入**——把 Hasura v2 中繼數據或 DDN supergraph YAML 轉換為 Provisa 設定
- **Apollo Federation**——把 Provisa 呈現為一個 Apollo Federation v2 子圖

以實體關係圖視覺化的依角色設限結構描述（GraphQL Voyager）：

![結構描述 Voyager](docs/images/schema-voyager.png)

關係被註冊、核准，並作為唯一合法的 JOIN 路徑強制執行：

![關係](docs/images/relationships.png)

## 安全模型 {: #security-model }

「就在每個查詢本來就要走的路徑上」這句話，要在這裏經受考驗。Provisa 對每種查詢語言（GraphQL、SQL、Cypher）及每種傳輸方式（REST、gRPC、Arrow Flight、JDBC、pgwire、Bolt、WebSocket）套用同一套分層安全模型，沒有任何查詢路徑能繞過它。涵蓋範圍來自設計本身，所以沒人需要記住甚麼：加入一個數據來源、一個資料行或一個關係，每一層都會即時作用於它，毋須額外註冊。

各層按次序套用，請求必須通過一層，才會檢查下一層。

### 第 0 層——自省篩選 {: #layer-0-introspection-filtering }

一個角色看到的結構描述及目錄，只包含其 `domain_access` 清單中的資料表，以及通過各自 `visible_to` 規則的資料行。角色存取範圍以外的一切從一開始就不可見：既不能查詢，也不會出現在自動完成中，甚至無法推斷它存在。這適用於 GraphQL 結構描述、SQL 目錄及查詢編輯器的結構描述瀏覽器。

### 第 1 層——公開存取 {: #layer-1-public-access }

沒有 `domain_access` 限制的領域中的資料表，對所有已驗證身分可見，毋須任何額外設定。真正公開的數據不費任何工夫。

### 第 2 層——網域存取 {: #layer-2-domain-access }

每個角色帶有一個由領域 ID 組成的 `domain_access` 清單，觸及這些領域以外資料表的查詢會在執行前被拒絕。這是粗粒度的擁有權邊界：無論 SQL 怎樣寫，人力資源角色都碰不到財務資料表。

### 第 3 層——資料列層級安全 {: #layer-3-row-level-security }

確認領域存取之後，按資料表、按角色的 `WHERE` 述詞會在執行時加到每個 `SELECT` 上，並針對原始數據求值。查詢共用訂單資料表的區域經理只能看到自己區域的資料列，即使用的是 `SELECT *`。

### 第 4 層——資料行可見性與遮罩 {: #layer-4-column-visibility-and-masking }

`visible_to` 清單不包含請求角色的資料行，會從輸出中移除。帶有遮罩規則的資料行，在結果離開伺服器之前，其值會被正規表示式塗黑、常數取代或截斷。遮罩適用於所有查詢語言及所有輸出格式。

### 第 5 層——述詞防護 {: #layer-5-predicate-guard }

已遮罩的資料行在 `WHERE` 及 `HAVING` 子句中會被拒絕。否則，即使輸出已遮罩，呼叫者亦能在篩選條件裏用二分搜尋推出真實值。拒絕發生在剖析查詢時，任何東西執行之前。

### 關係治理 {: #relationship-governance }

在 SQL 中，JOIN 條件必須與資料表之間已註冊並獲批准的關係相符，未獲批准的聯結會被拒絕。每個關係都帶有易讀的原因及描述，向人及自主代理說明這條路徑為何存在。這是治理政策，不是硬性的安全邊界。無論聯結是甚麼形狀，第 2 至第 5 層都照樣生效，所以刻意繞行亦不會暴露角色用兩個獨立查詢拿不到的任何東西，而且每次嘗試都會記入日誌以供稽核。

無效的聯結永遠不會在您的數據上執行。角色預設受治理，要退出，就要有意把 `ignore_relationships` 權限授予您信任的角色。代理的 OAuth 權杖對應到一個角色，所以代理得到的層與人相同。帶情境層的數據目錄告訴代理它應該做甚麼；Provisa 強制規定它能做甚麼。

---

這些層可以疊加。一個擁有領域存取、RLS 及已遮罩資料行的角色，所有層同時生效，因為每一層都獨立設定，並作用於任何觸及受治理物件的查詢。

## 快速開始 {: #quick-start }

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
2. 賦予它執行權限並執行。首次啟動會完成一次性設定（毋須網際網路）：

```bash
chmod +x Provisa-*-linux-x86_64.AppImage
./Provisa-*-linux-x86_64.AppImage
provisa start && provisa open
```

### Windows

1. 下載 [Provisa-windows-x64.exe](https://provisa.dev/dl/windows)（一律為最新版本）
2. 執行安裝程式，它毋須管理員權限。
3. 自「開始」功能表開啟 **Provisa First Launch**，它會完成一次性設定（約 5 分鐘，毋須網際網路）。
4. 打開一個新的終端機：

```bash
provisa start
```

### 第一個查詢 {: #first-query }

在本機開發環境（`PROVISA_MODE=test`）中不需要憑證。在正式環境中，您使用 Bearer 權杖進行認證，Provisa 會從中讀取角色。

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

使用您的 Provisa 使用者名稱及密碼進行認證，伺服器會為您指派角色。

Tableau 與 Power BI 的設定步驟見 [docs/integrations.md](docs/integrations.md)。

### PostgreSQL Wire 協定（pgwire） {: #postgresql-wire-protocol-pgwire }

Provisa 在 5439 埠上使用 PostgreSQL 線協定。任何能連接 Postgres 的用戶端都能連接 Provisa，無須驅動程式、無須轉接器，亦無須改動您現有的工具。

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

每個查詢都經過完整的治理流水線，所以領域存取、RLS、遮罩及謂詞保護的作用方式與 GraphQL 及 REST 完全相同。結構描述瀏覽器（DBeaver、DataGrip、pgAdmin）無須任何設定即可使用。Provisa 從一個按角色領域存取權限限定的記憶體目錄回答它們的 `pg_catalog` 及 `information_schema` 查詢，因此使用者只能看到他們有權查詢的資料表及資料行。

DataGrip 透過 pgwire 瀏覽受治理的結構描述及其外鍵關係圖，無須驅動程式，亦無須轉接器：

![以 pgwire 在 DataGrip 中的 Provisa](docs/images/pgwire-datagrip.png)

設定 `PROVISA_PGWIRE_CERT` 與 `PROVISA_PGWIRE_KEY` 即啟用 TLS。連接埠可經 `PROVISA_PGWIRE_PORT` 設定（預設 `5439`）。

### Bolt（Neo4j Wire 協定） {: #bolt-neo4j-wire-protocol }

Provisa 亦支援 Neo4j 的 **Bolt** 協定，所以圖原生工具可以直接連接，並對聯邦圖執行 Cypher。無須匯出任何資料，亦不需要獨立的圖資料庫。把 **Neo4j Browser** 或 **Bloom** 指向 Provisa，即可在相同的治理（領域存取、RLS、遮罩）下跨數據來源走訪關係。

這裏 Neo4j Browser 正在針對 Provisa 執行 Cypher，節點標籤、關係類型與屬性索引鍵全部直接來自已註冊的結構描述：

![以 Bolt 在 Neo4j Browser 中的 Provisa](docs/images/bolt-neo4j-browser.png)

設定 `PROVISA_BOLT_PORT` 即可啟用它（Neo4j 的預設埠是 `7687`），並透過 `PROVISA_BOLT_CERT` 及 `PROVISA_BOLT_KEY` 開啟 TLS。已認證使用者擁有的每個 Provisa 角色都顯示為一個可選擇的 `provisa_<role>` 資料庫（即上圖中的 `provisa_admin` 選擇器）。選擇其中一個會把工作階段收窄到該角色的領域權限，使用者永遠無法超出自己擁有的角色。

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
| 業務詞彙表與詞條策展 | [docs/ontology.md](docs/ontology.md) |
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

**水平擴展** —— 在負載平衡器後面執行多個 Provisa 實例。每個實例都是完整的系統。所有實例必須指向同一個設定資料庫（在從屬機器上設定 `CONFIG_DB_HOST`），並可共用一個 Redis 實例（`REDIS_URL`）作為統一快取。大多數查詢無須改動即可分散執行。非常大的跨源連接可能超出單一實例的承受範圍，這時需要更大的機器或外部聯邦叢集。

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
