# 複本 {: #replicas }

複本是資料來源表的一份拷貝，Provisa 將它保存在引擎的具體化儲存區中，並讀取它來代替資料來源。本頁介紹複本按甚麼建立索引、複本應存放在何處，以及複本有多新鮮。一張表是否被複製，由 [`replicate`](configuration.md#the-replicate-setting) 以及引擎能夠存取到的內容決定；請參閱 [What an engine changes](engines.md#what-an-engine-changes)。

## 複本帶有索引 {: #replicas-are-indexed }

Provisa 按模型使用其表的方式為複本建立索引。

- **宣告的主索引鍵**，作為索引鍵強制執行。
- **宣告的關係所連接的欄**：
  - 以該表為來源的每個關係的外部索引鍵；
  - 以該表為目標的每個關係的目標欄，除非該欄已經是索引鍵；
  - 關係所經過的連接表的兩個外部索引鍵。

以下面這個關係為例：

```yaml
relationships:
  - id: orders-to-customers
    source_table_id: orders
    target_table_id: customers
    source_column: customer_id
    target_column: id
    cardinality: many-to-one
```

`orders` 的複本得到它的索引鍵，以及 `customer_id` 上的索引。`customers` 的複本得到它的索引鍵，如果 `id` 就是該索引鍵，則無需更多。這樣，在使用索引進行聯結的儲存區中，這兩張表的聯結兩側都會讀取索引（請參閱[各儲存區建構甚麼](#what-each-store-builds)）。建立在複本不含的計算欄上的關係不會得到索引。

在欄上用 `is_primary_key: true` 宣告索引鍵。沒有宣告索引鍵的複本只得到其聯結索引。

### 先可讀，後建索引 {: #readable-first-indexed-after }

落地從不等待索引。複本從換入的一刻起就可讀取；隨後複本執行器在獨立的一輪中，在每個複本的索引鎖之下，逐個建構它的索引。 落地之後最初的幾次查詢會在沒有這些索引的情況下執行。

每次落地都會按名稱記錄它所請求的索引、已建構的索引和失敗的索引，記錄在控制平面中該複本的記錄上（`replica_state.index_state`）。

- **索引失敗絕不會令落地失敗。** 它會連同原因寫入記錄，並在下一次落地時再次請求。記錄行形如 `replica of <source>.<schema>.<table>: index <name> on (<columns>) failed: <cause>`。
- **與已宣告的索引鍵重複的列是獨立的失敗**，代碼為 `replica.index_duplicate_key`，並指明索引鍵的欄。資料來源中存在模型宣告為唯一的欄集合的重複值。重試無法修復：請在資料來源中更正數據，或更正宣告的索引鍵。
- **在建構過程中替換表的落地**會丟下之前的建構；它們會停止且不記錄任何內容，新表則得到自己的建構。在 PostgreSQL 儲存區上，替換會取消仍在被替換的表上執行的建構。
- **計劃在每次落地時依循模型。** 之後新增的關係會在複本的下一次落地時得到其索引。

管理介面不顯示索引記錄。請查看伺服器記錄，或複本所在列的 `index_state` 欄。

### 各儲存區建構甚麼 {: #what-each-store-builds }

| 儲存區 | 建構的內容 | 方式 |
| --- | --- | --- |
| PostgreSQL：`pg` 引擎自己的資料庫、Trino 或 DuckDB 的 PostgreSQL 儲存區、基於 PostgreSQL 的 `sqlalchemy` 引擎 | 索引鍵和聯結索引 | 在其自己的連線上執行 `CREATE INDEX CONCURRENTLY`，因此不會阻塞讀取方和寫入方。索引鍵先建構為唯一索引，再透過 `ADD CONSTRAINT ... PRIMARY KEY USING INDEX` 成為主索引鍵。被取消的建構留下的無效索引會被刪除並重新建構。 |
| 關聯式引擎背後的其他資料庫（`mysql`、`oracle`、`mssql` 等） | 索引鍵和聯結索引 | 該方言自己的 DDL。拒絕該 DDL 的資料庫會把該索引記錄為失敗。 |
| 內嵌 DuckDB 檔案、MotherDuck | 索引鍵和聯結索引 | 透過儲存區代理：`ALTER TABLE ... ADD PRIMARY KEY`、`CREATE INDEX IF NOT EXISTS`。 |
| ClickHouse | 僅排序索引鍵 | ClickHouse 複本按其宣告的索引鍵排序（`ORDER BY`），在落地時設定。ClickHouse 沒有可建構的次要 B-tree 索引。 |
| Snowflake、BigQuery、Databricks、Fabric、Synapse | 無 | 數據倉庫自行組織表。 |

索引只對在索引所在之處執行的聯結有幫助。有兩種引擎讀取其儲存區時不會使用儲存區的索引來做聯結：

- **使用內嵌檔案的 DuckDB。** 引擎從不附加該檔案。每條語句會讀取它所涉及的每張儲存區表的本機拷貝，儲存區每次發生變化後都會重新產生該拷貝。聯結在該拷貝上以雜湊聯結執行，因此聯結索引不會加速它們；檔案的索引負責強制執行索引鍵，並為就地讀取該檔案的任何操作提供服務。
- **使用 PostgreSQL 儲存區的 DuckDB。** DuckDB 透過其 `postgres` 擴充功能讀取儲存區，並關閉了篩選下推（上游錯誤 duckdb/duckdb-postgres#564），因此它會掃描複本，並在 DuckDB 中篩選和聯結。儲存區的索引已建構，但這些讀取不使用它們。

MotherDuck 儲存區是附加並就地讀取的。

## 當引擎在關聯式資料庫中執行 SQL 時，用該資料庫作為複本儲存區 {: #use-a-relational-database-as-the-replica-store-where-the-engine-runs-sql-in-it }

當引擎在關聯式資料庫內部執行聯結時，請把複本保存在那裡。這樣聯結就會讀取上述索引。有三種配置可以做到：

- **`pg` 引擎。** 複本落地到引擎自己的 PostgreSQL 資料庫中，每條語句都在那裡透過它們的索引執行。
- **關聯式引擎**（`mysql`、`oracle`、`mssql`、兜底的 `sqlalchemy` 等）。每個資料來源都落地到該資料庫中，並在那裡查詢。
- **使用其 PostgreSQL 儲存區的 Trino。** PostgreSQL 是 Trino 支援的唯一儲存區；Trino 透過其 PostgreSQL 連接器讀取複本。 Trino 下推到 PostgreSQL 的篩選或聯結會使用這些索引；Trino 自己完成的工作則不會。

用 `PROVISA_MATERIALIZE_URL` 或 `materialize_store_url` 將引擎指向某個儲存區；組織自己登記的儲存區優先於這兩者。

```bash
PROVISA_MATERIALIZE_URL="postgresql://provisa:secret@replicas.internal:5432/replicas"
```

這是一般性建議，而不是規則。是否划算取決於引擎、資料來源、儲存區和表的大小。請權衡以下幾點：

- **每次查詢的存取路徑。** 引擎透過網絡存取的儲存區，會為每次讀取增加一次往返和一次傳輸。內嵌 DuckDB 檔案是本機的、欄式的，掃描很快。
- **落地成本。** 把複本寫入關聯式資料庫再建構其索引，比寫入本機檔案的成本更高。
- **資料庫自身的負載。** 複本讀取、落地和索引建構都在該資料庫上執行。請按它們來決定其規模。
- **大規模掃描。** 對讀取大表大部分內容的查詢，索引沒有任何作用。欄式儲存區掃描它比列式儲存區更快。

## 選擇複本存放的位置 {: #choosing-where-replicas-live }

儲存區只是若干手段之一，而且它們相互重疊。先確定其他手段；儲存區只決定它們留下的部分。

| 手段 | 作用 | 適用場景 |
| --- | --- | --- |
| Hot tables | 小型查找表保存在 Redis 中，並作為內嵌的 `VALUES` CTE（通用資料表運算式）放入每個讀取它的查詢。 | 在任何儲存區上與小型查找表的聯結：資料列隨查詢一起傳遞。多對一關係的目標表，或列數不超過 `hot_tables.auto_threshold`（預設 1,000）的表，會在首次讀取時變為 hot。`hot: true` 強制放入，`hot: false` 將其排除。 |
| 你定義的具體化檢視 | 帶有 `materialize: true` 的檢視會被儲存並刷新。讀取該檢視的查詢，在檢視新鮮且讀取方看到的是完整內容時，由儲存的資料列應答：沒有任何輸入帶有比「全部」更窄的列規則。 | 大表上的彙總。哪些檢視存在由你決定；不會替你建立任何檢視。請參閱 [Views](configuration.md#views-governed-computed-datasets)。 |
| 關係 `materialize: true` | 跨資料來源關係會得到一個儲存的、預先聯結的父子檢視。聯結完全或部分符合該模式的請求，無論以 SQL 形式到達還是由編譯器建構，都會被改寫為讀取它。 | 在不建構聯結索引的儲存區上的聯結。請參閱 [Auto-Materialization](configuration.md#auto-materialization)。 |
| 放置 | 引擎能夠即時讀取的資料來源即時讀取；其餘的則被複製。最終儲存區中有甚麼，取決於你選擇的引擎。 | 選擇引擎決定了儲存區需要容納的大部分內容。 |

關係具體化和帶索引的聯結以兩種方式滿足同一需求。在欄式或數據倉庫儲存區（DuckDB 檔案、ClickHouse、Snowflake、BigQuery、Databricks、Fabric、Synapse）上不建構聯結索引，因此預先聯結的檢視就是讓頻繁聯結變得低成本的辦法。在關聯式儲存區上，聯結欄已經有索引，同樣的聯結無需具體化也能快速執行。

仍取決於儲存區的是：對沒有任何具體化檢視涵蓋、也沒有 hot 表服務的大表做的臨時掃描和聯結。這類操作在關聯式資料庫中依靠儲存區的索引執行，在欄式儲存區中則依靠其掃描速度。

結果因工作負載而異。修改前後請用[容量規劃工具](sizing.md)量度；它的[即時或複本執行](sizing.md#live-or-replica-reads-and-the-ttl)會給出在你的部署上複本讀取與即時讀取的成本對比。

## 新鮮度 {: #freshness }

把新鮮度與速度一併納入決策。答案可以有多舊，以及保持其最新對資料來源意味着多大成本，決定了一張表是即時讀取還是從複本讀取，從而決定儲存區需要容納甚麼。

企業資料來源很少支援即時。數據透過批次載入或夜間 ETL 到達，變更串流存在延遲，許多系統無法承受查詢量級的即時讀取。從每張表所需的新鮮度出發，再選擇滿足它的最便宜的策略。營運方一側的規則見 [Freshness and load: who decides](configuration.md#freshness-and-load-who-decides)。

| 策略 | 設定方式 | 保證甚麼 | 對資料來源的成本 |
| --- | --- | --- | --- |
| 即時讀取 | `replicate: -1` | 截至查詢時是最新的 | 每個查詢都讀取資料來源 |
| TTL | `change_signal: ttl`, `cache_ttl` | 複本不比 `cache_ttl` 更舊 | 每個 `cache_ttl` 重建一次；有 `watermark_column` 時，只重建超過水位線的列 |
| 變更探測 | `change_signal: probe` 或 `ttl_probe`，`cache_ttl` | 僅在探測報告有變更後才重建；探測每個 `cache_ttl` 執行一次 | 每個間隔一次探測，僅在有變更時重建 |
| 推送變更串流 | Kafka 或 WebSocket 資料來源；帶 `cdc` 區塊的資料來源上的 `change_signal: debezium`；PostgreSQL、MongoDB 或 Cosmos DB 上的 `change_signal: native` | 新鮮度在變更串流延遲與防彈跳範圍之內 | Kafka、WebSocket 和 Debezium：首次建置後無，變更串流自帶資料列。PostgreSQL、MongoDB 和 Cosmos DB：每一批變更重建一次 |
| 外部訊號 | `change_signal: signal` | 每當 `POST /data/sources/{source_id}/signal` 表示資料來源已變更時重建；從不按時鐘重建 | 每個訊號重建一次 |
| 按角色 TTL | 表上的 `role_ttl` | 列出的角色接受不超過其自身 TTL 的複本；表的 `cache_ttl` 仍是下限 | 重建更少：寬容角色的讀取從不請求重建 |
| 負載保護 | `load_protected: true`，配合 `cache_ttl`、探測或 `off_peak_window` | 查詢從不讀取資料來源；僅由執行器刷新複本，有非尖峰視窗時僅在視窗開啟期間刷新 | 僅計劃內的刷新 |
| 新鮮度閘門 | 資料來源上的 `freshness_gate: true` | 每次讀取都先對照資料來源的 `change_signal` 和 `cache_ttl` 檢查；判定為過期則在讀取前先刷新 | 過期時在讀取路徑中進行一次刷新 |
| 永不過期 | 資料來源或表上的 `cache_ttl: -1` | 只載入一次；每次讀取都由副本回應，讀取尚未載入的表會被按名拒絕 | 僅此一次載入：所有表都已載入的資料來源不再被連接、探測或讀取，也不需要憑證 |

沒有可比較內容的探測（沒有水位線、哨兵值或探測查詢）會退化為 TTL。

### 來自變更串流的近即時 {: #near-real-time-from-a-change-feed }

當資料來源無法承受即時查詢負載，卻已經發佈其變更時，變更串流可以在查詢時不觸碰資料來源的情況下提供近即時數據。查詢讀取本機複本，既快又不為資料來源帶來負載，而事件迴圈在變更到達時套用它們。數據只延遲於變更串流，通常為幾秒。

僅有變更串流還不夠。Provisa 只會為接入了變更串流的表追蹤它：

- **Kafka 資料來源。** 表指明其主題，每個變更按主索引鍵套用到複本，包括刪除。

  ```yaml
  live:
    strategy: kafka
    kafka:
      topic: orders.cdc
  push_debounce_quiet: 0      # seconds of quiet before a batch lands; 0 lands each message
  push_debounce_max_delay: 5  # a batch never waits longer than this
  ```

- **WebSocket 資料來源。** 其串流以同樣方式套用。
- **帶 `change_signal: debezium` 的表**（在表或其資料來源上），其資料來源的 `cdc` 區塊指明 Kafka 傳輸和連接器的 topic 前綴。首次讀取建置複本；之後 topic `{topic_prefix}.{schema}.{table}` 上的每個變更事件都按主鍵套用，包括刪除，且僅在變更落地後才提交 offset。
- **此類資料來源上帶 `change_signal: kafka` 的表**。其變更從其 `live.kafka.topic` 指定的增量 topic 讀取，使用同一傳輸，並以同樣方式套用；使用此訊號卻未指定 topic 的表，在儲存該表或其資料來源以及載入設定時都會被點名拒絕。
- **帶 `change_signal: native` 的 MongoDB 或 Cosmos DB 表**（在表或其資料來源上）。變更串流上的每一批變更都會請求重建複本。
- **帶 `change_signal: native` 的 PostgreSQL 表**（在表或其資料來源上）。Provisa 從自己的 slot `provisa_<source id>` 讀取資料庫的邏輯複寫，所用的同名發佈列出該資料來源的原生表。不涉及 Kafka。表上的每一批變更都會請求重建其複本。資料庫需要的條件與 Debezium 相同：`wal_level` 設為 `logical`、具有複寫權限且可建立該發佈的角色，以及主要伺服器而非唯讀複本；缺少其中任何一項的資料來源在儲存及載入設定時都會被點名拒絕。第一張表跟隨時建立 slot 和發佈，最後一張停止跟隨或資料來源被刪除時將其刪除。 伺服器啟動時會檢查每個已註冊的 PostgreSQL 資料來源，並刪除沒有任何原生表跟隨的 Provisa slot，因此在沒有伺服器執行時修改設定所遺留的 slot 不會繼續佔用資料來源的 WAL。

被推送的表需要宣告主索引鍵；沒有主索引鍵的表會被跳過並寫入記錄。

帶有推送變更訊號的表在讀取路徑上視為新鮮，因為它的變更串流就是它的新鮮度檢查。 因此，無人接收的推送訊號會被拒絕：除 Kafka、WebSocket、ingest、PostgreSQL、MongoDB 或 Cosmos DB 以外任何類型上的 `change_signal: native`，在儲存資料來源或表以及載入設定時都會被拒絕；沒有 `cdc` 區塊的資料來源上的 `debezium` 同樣如此。
