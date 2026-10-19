# SSE 訂閱 {: #sse-subscriptions }

Provisa 透過 Server-Sent Events（SSE）支援即時推送。客戶端可接收變更事件串流，毋須輪詢。

## 數據來源 {: #sources }

訂閱以**已註冊的資料表**為目標：

| 數據來源 | 可用的 `strategy` 值 |
| -------- | ------------------------- |
| 資料表（PostgreSQL） | `native`（LISTEN/NOTIFY）、`poll` |
| 資料表（非 PG RDBMS，附有數據來源層級的 `cdc` 區塊） | `debezium`、`kafka`、`poll` |
| 資料表（聯邦檢視 / 任何其他數據來源） | 僅限 `poll` |

### PostgreSQL 觸發程序自動安裝 {: #postgresql-trigger-auto-installation }

Provisa 在啟動時，會自動在所有**已預先核准**的 PostgreSQL 資料表上安裝 `AFTER INSERT OR UPDATE OR DELETE` 觸發程序。這些觸發程序會呼叫 `pg_notify('provisa_{table}', ...)`，令原始 DML（不僅限於 Provisa 的變更操作）都能被訂閱捕捉到。

如觸發程序安裝失敗（例如權限不足——資料庫角色必須擁有該資料表的擁有權），Provisa 會退回至該資料表的水位標記輪詢，前提是已設定 `watermark_column`。系統會記錄一則警告。

### 跨數據來源檢視訂閱 {: #cross-datasource-view-subscriptions }

對於透過聯邦引擎合併多個數據來源的檢視，請在資料表註冊時加入 `watermark_column`。該欄位必須存在於檢視的 SQL 中（毋須出現在 GraphQL 結構描述中）：

```sql
-- Example: federated view with derived watermark
CREATE OR REPLACE VIEW orders_with_segments AS
SELECT o.*, s.name AS segment_name,
       GREATEST(o.updated_at, s.updated_at) AS _watermark
FROM postgresql.public.orders o
JOIN mysql.crm.customer_segments s ON o.customer_id = s.customer_id;
```

以 `watermark_column: _watermark` 進行註冊。Provisa 會以 `WHERE _watermark > <last_seen>` 進行輪詢。

### 巢狀關聯訂閱 {: #nested-relationship-subscriptions }

當訂閱欄位選取了已合併資料表（透過已註冊的關聯）的欄位時，Provisa 會同時監察**所有**相關的實體資料表。任何已合併資料表的變更，都會重新觸發該訂閱查詢。

## 端點 {: #endpoint }

訂閱某資料表：

```http
GET /data/subscribe/{table}
Accept: text/event-stream
```

連線會保持開啟，並針對每次變更發送一個 JSON 事件：

```text
data: {"event":"insert","table":"orders","row":{"id":43,"amount":55.00,"region":"east"}}

data: {"event":"update","table":"orders","row":{"id":42,"amount":199.00,"region":"west"}}
```

## 傳送模式 {: #delivery-modes }

傳送方式由資料表設定中的 `live.strategy` 選定：

| `strategy` | 機制 | 適用於 | 需要 |
| ------------ | ----------- | --------------- | --------- |
| `native` | PostgreSQL `LISTEN`/`NOTIFY`、MongoDB Change Streams | PG、MongoDB | 毋須額外項目 |
| `debezium` | 來自 Debezium 連接器的 Kafka 主題 | 非 PG RDBMS 資料表 | 數據來源層級的 `cdc` 區塊（Debezium + Kafka） |
| `kafka` | 任意的 Kafka delta 主題 | 任何以 Kafka 供應數據的資料表 | 數據來源層級的 `cdc` 區塊 |
| `poll` | 以水位標記為基礎的輪詢 | 任何具有水位標記的資料表 | `watermark_column` |

### LISTEN/NOTIFY

Provisa 會在一條持久的 PG 連線上發出 `LISTEN <channel>`。Provisa 的變更操作會自動觸發 `NOTIFY`。外部寫入端必須在寫入後呼叫 `NOTIFY <channel>, '<payload>'`。毋須額外的基礎設施。

### 輪詢 {: #polling }

Provisa 會定期重新執行數據來源查詢，只選取 `watermark_column > last_watermark` 的資料列。差異會以 SSE 事件形式發出。輪詢無法偵測到硬刪除——被移除的資料列不會令水位標記推進。若要令刪除操作可見，請使用軟刪除（例如設定 `deleted_at` 旗標）以推進水位標記欄位；該刪除操作其後會以更新事件形式送達，並附帶軟刪除標記。

資料表輪詢設定（於 `provisa.yaml` 中）：

```yaml
tables:
  - id: federated_orders
    source_id: federated-source
    live:
      strategy: poll
      watermark_column: updated_at
      poll_interval: 30
      outputs:
        - type: sse
```

### Debezium CDC

需要一個正在運行、並將資料寫入 Kafka 的 Debezium 連接器。Provisa 會消費該 Kafka 主題，並將變更事件轉發至已連線的 SSE 客戶端。

CDC 傳輸每個數據來源只須在 `cdc` 區塊中設定一次；主題會依 `{topic_prefix}.{schema}.{table}` 的格式衍生，而不會逐資料表重複設定。之後每個資料表只須選取 `strategy: debezium`：

```yaml
sources:
  - id: sales-mysql
    cdc:
      bootstrap_servers: kafka:9092
      topic_prefix: debezium
      # schema_registry_url: http://schema-registry:8081   # set for Avro; omit for JSON
    tables:
      - id: orders
        live:
          strategy: debezium
```

#### 訊息編碼 {: #message-encodings }

Debezium 主題以兩種編碼之一讀取，每則訊息都會說明是哪一種。

| 編碼 | 主題攜帶的內容 | 數據來源需要的內容 |
|---|---|---|
| JSON | Debezium JSON 轉換器的封裝，帶或不帶 `schema`/`payload` 包裝 | `cdc` 區塊以外不需任何內容 |
| Avro | Confluent schema registry 的線路格式：一個零位元組、四位元組的 schema id、一個 Avro 數據項 | `schema_registry_url` 以及向它驗證身份的方式 |

Avro 訊息以其 id 所指的 schema 解碼，該 schema 從 registry 取得一次並保存。schema 已經演進的主題也能正確讀取，因為每則訊息都指明寫入它所用的 schema。Protobuf 及 JSON Schema 主題不會被讀取。

Avro 訊息絕不會被當作 JSON 讀取。無法讀取它的每一種原因都會被點名拒絕：

| 情況 | 拒絕 |
|---|---|
| Avro 訊息，數據來源沒有指名 registry | `subscribe.avro_topic_without_registry`，附主題 |
| registry 無法連上或沒有及時回應 | `subscribe.schema_registry_unreachable` |
| registry 拒絕數據來源的憑證 | `subscribe.schema_registry_refused_credentials` |
| registry 中不存在的 schema id | `subscribe.schema_id_unknown`，附 id |
| 該 id 指向 Protobuf 或 JSON schema | `subscribe.schema_not_avro` |

無法連上或拒絕憑證的 registry 會在訂閱開始時、串流開啟之前被拒絕。對 registry 的請求最多等待 5 秒建立連線、10 秒等待回應；若請求剩餘的時間較少，則等待較短。

#### Schema registry 身份驗證 {: #schema-registry-authentication }

`schema_registry_auth` 指名方法；該方法所需的欄位會在儲存數據來源時檢查。

| `schema_registry_auth` | 必需欄位 | 傳送給 registry 的內容 |
|---|---|---|
| `none`（預設） | — | 沒有憑證 |
| `basic` | `schema_registry_username`、`schema_registry_password` | HTTP 基本驗證 |
| `bearer` | `schema_registry_token` | `Authorization: Bearer <token>`（靜態權杖） |
| `mtls` | `schema_registry_client_cert`、`schema_registry_client_key` | 用戶端憑證，在 TLS 交握中 |

`schema_registry_ca` 在任何方法下都是可選的：當 registry 的憑證並非由公共機構簽發時所信任的 CA 套件。

密碼及權杖遵循密鑰契約。在數據來源表單中輸入的值存放於組織的密鑰庫，數據來源保存一個 `${secret:NAME}` 參照；在設定檔中寫入的參照（`${env:...}`、`${secret:...}`）按原樣保存。憑證、金鑰及 CA 套件是 Provisa 伺服器程序開啟的檔案，以絕對路徑指定。

```yaml
sources:
  - id: sales-mysql
    cdc:
      bootstrap_servers: kafka:9092
      topic_prefix: debezium
      schema_registry_url: https://schema-registry:8081
      schema_registry_auth: basic
      schema_registry_username: provisa
      schema_registry_password: ${secret:registry_password}
      schema_registry_ca: /etc/provisa/registry-ca.pem
```

## 串流失敗時 {: #when-a-stream-ends-on-a-failure }

已開啟的串流已經回應了 `200`，因此之後的失敗不能再是 HTTP 狀態。訂閱者會收到說明原因的最後一個訊框，然後串流關閉。串流絕不會在沒有這個訊框的情況下因失敗而關閉。

該訊框攜帶的內容，與一般請求對同一失敗的回應相同：

| 失敗 | 主體 |
|---|---|
| 帶代碼的拒絕（schema registry、區域、治理拒絕） | `detail`、`code`、`params` |
| 請求的截止時間 | `detail: "Request timed out"` |
| 任何意料之外的情況 | `detail: "Internal server error"`、`type`（例外類型名稱） |

每個主體亦攜帶 `subscription_id`。發生意料之外的失敗時，訊息及 traceback 位於伺服器記錄中該 id 之下，而不在訊框中。

資料表訂閱（`/data/subscribe/{table}`）把主體作為名為 `stream_error` 的事件數據傳送：

```text
event: stream_error
data: {"detail": "Topic 'debezium.app.orders' carries Avro messages, and its source names no schema registry to read them with: set the source's schema registry URL", "code": "subscribe.avro_topic_without_registry", "params": {"topic": "debezium.app.orders"}, "subscription_id": "5f0c2a9d41b7e386"}
```

該事件不叫 `error`，因為 `EventSource` 在連線中斷時會自行產生一個 `error` 事件，監聽器將無法區分兩者。請按名稱監聽它：

```javascript
source.addEventListener("stream_error", (e) => {
  const { detail, code, params } = JSON.parse(e.data);
  source.close();
});
```

GraphQL 訂閱保持 GraphQL 回應的形態，代碼及 params 位於 `extensions` 之中：

```json
{"errors": [{"message": "…", "extensions": {"code": "subscribe.avro_topic_without_registry", "params": {"topic": "debezium.app.orders"}, "subscription_id": "5f0c2a9d41b7e386"}}]}
```

有一種情況不會結束 GraphQL 訂閱：當某次變更之後的重新讀取失敗時，會以同樣的形態傳送該訊框，然後串流繼續處理下一次變更。初始讀取、變更串流本身或其啟動的失敗是最後一個訊框。

## Kafka 接收器重新導向 {: #kafka-sink-redirect }

任何 GraphQL 訂閱都可以重新導向至 Kafka 主題，而非以串流形式傳回客戶端。請在訂閱請求中加入 `X-Provisa-Sink` 標頭：

```yaml
POST /data/graphql
Authorization: Bearer <token>
Content-Type: application/json
X-Provisa-Sink: kafka://broker:9092/my-topic
```

伺服器會立即回應 `202 Accepted`，並啟動一項背景工作，該工作會：

1. 使用與 SSE 相同的供應者解析方式（LISTEN/NOTIFY → asyncpg 輪詢 → 聯邦輪詢）監察資料表變更
2. 於每次變更時重新執行對應的查詢
3. 將結果以 JSON 訊息形式發佈至指定的 Kafka 主題

該接收器會在伺服器程序的整個生命週期內運行。如要停止，請重新啟動伺服器。

**URI 格式：** `kafka://[broker:port]/topic`

- 如省略 `broker:port`，則會使用 `KAFKA_BOOTSTRAP_SERVERS` 環境變數（預設值：`localhost:9092`）
- `topic` 為必要項目

**範例（curl）：**

```bash
curl -X POST http://localhost:8000/data/graphql \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "X-Provisa-Sink: kafka://kafka:9092/orders-live" \
  -d '{"query": "subscription { orders { id status amount } }"}'
# → 202 {"status":"streaming","sink":"kafka://kafka:9092/orders-live","table":"orders"}
```

### 作為設定層級第二輸出的 Kafka 接收器 {: #kafka-sink-as-a-config-level-second-output }

以輪詢為基礎的資料表訂閱，可透過 `provisa.yaml` 同時發佈至 Kafka 主題。SSE 訂閱與 Kafka 接收器同為同一個 Live Query Engine 的輸出。每個輸出都會獨立追蹤其水位標記。

```yaml
tables:
  - id: active-orders
    live:
      strategy: poll
      watermark_column: updated_at
      poll_interval: 30
      outputs:
        - type: sse
        - type: kafka
          topic: provisa.active-orders
          bootstrap_servers: kafka:9092
          key_column: id
          role: analyst           # required: the rows are published governed as this role
```

Kafka 輸出以其所指名的角色治理後發佈資料表的資料列：該角色的資料列規則、欄位可見性及遮罩，與該角色的讀取一樣適用。沒有 `role` 的輸出會在載入設定時被拒絕。

## 安全性 {: #security }

所有訂閱模式均採用與一般查詢相同的安全管線：

- 行級安全篩選會套用至每一列已發出的資料
- 已遮罩的欄位在事件中會顯示為已遮罩
- 角色授權會於連線時進行驗證

## 客戶端範例 {: #client-example }

```javascript
// Table subscription (LISTEN/NOTIFY)
const source = new EventSource('/data/subscribe/orders', {
  headers: { 'Authorization': 'Bearer <token>' }
});

source.onmessage = (e) => {
  const event = JSON.parse(e.data);
  console.log(event.event, event.row);
};
```

</content>
