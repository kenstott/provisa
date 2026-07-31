# SSE 訂閱

Provisa 透過 Server-Sent Events（SSE）支援即時推送。客戶端可接收變更事件串流，毋須輪詢。（REQ-258）

## 數據來源

訂閱以**已註冊的資料表**為目標：

| 數據來源 | 可用的 `strategy` 值 |
| -------- | ------------------------- |
| 資料表（PostgreSQL） | `native`（LISTEN/NOTIFY）、`poll` |
| 資料表（非 PG RDBMS，附有數據來源層級的 `cdc` 區塊） | `debezium`、`kafka`、`poll` |
| 資料表（聯邦檢視 / 任何其他數據來源） | 僅限 `poll` |

### PostgreSQL 觸發程序自動安裝

Provisa 在啟動時，會自動在所有**已預先核准**的 PostgreSQL 資料表上安裝 `AFTER INSERT OR UPDATE OR DELETE` 觸發程序。（REQ-565）這些觸發程序會呼叫 `pg_notify('provisa_{table}', ...)`，令原始 DML（不僅限於 Provisa 的變更操作）都能被訂閱捕捉到。（REQ-565）

如觸發程序安裝失敗（例如權限不足——資料庫角色必須擁有該資料表的擁有權），Provisa 會退回至該資料表的水位標記輪詢，前提是已設定 `watermark_column`。（REQ-566）系統會記錄一則警告。（REQ-566）

### 跨數據來源檢視訂閱

對於透過聯邦引擎合併多個數據來源的檢視，請在資料表註冊時加入 `watermark_column`。（REQ-260、REQ-283）該欄位必須存在於檢視的 SQL 中（毋須出現在 GraphQL 結構描述中）：

```sql
-- Example: federated view with derived watermark
CREATE OR REPLACE VIEW orders_with_segments AS
SELECT o.*, s.name AS segment_name,
       GREATEST(o.updated_at, s.updated_at) AS _watermark
FROM postgresql.public.orders o
JOIN mysql.crm.customer_segments s ON o.customer_id = s.customer_id;
```

以 `watermark_column: _watermark` 進行註冊。Provisa 會以 `WHERE _watermark > <last_seen>` 進行輪詢。（REQ-260）

### 巢狀關聯訂閱

當訂閱欄位選取了已合併資料表（透過已註冊的關聯）的欄位時，Provisa 會同時監察**所有**相關的實體資料表。（REQ-567）任何已合併資料表的變更，都會重新觸發該訂閱查詢。（REQ-567）

## 端點

訂閱某資料表：

```http
GET /data/subscribe/{table}
Accept: text/event-stream
```

連線會保持開啟，並針對每次變更發送一個 JSON 事件：（REQ-258、REQ-568）

```text
data: {"event":"insert","table":"orders","row":{"id":43,"amount":55.00,"region":"east"}}

data: {"event":"update","table":"orders","row":{"id":42,"amount":199.00,"region":"west"}}
```

## 傳送模式

傳送方式由資料表設定中的 `live.strategy` 選定：（REQ-813、REQ-814）

| `strategy` | 機制 | 適用於 | 需要 |
| ------------ | ----------- | --------------- | --------- |
| `native` | PostgreSQL `LISTEN`/`NOTIFY`、MongoDB Change Streams | PG、MongoDB | 毋須額外項目 |
| `debezium` | 來自 Debezium 連接器的 Kafka 主題 | 非 PG RDBMS 資料表 | 數據來源層級的 `cdc` 區塊（Debezium + Kafka） |
| `kafka` | 任意的 Kafka delta 主題 | 任何以 Kafka 供應數據的資料表 | 數據來源層級的 `cdc` 區塊 |
| `poll` | 以水位標記為基礎的輪詢 | 任何具有水位標記的資料表 | `watermark_column` |

### LISTEN/NOTIFY

Provisa 會在一條持久的 PG 連線上發出 `LISTEN <channel>`。（REQ-258）Provisa 的變更操作會自動觸發 `NOTIFY`。（REQ-565）外部寫入端必須在寫入後呼叫 `NOTIFY <channel>, '<payload>'`。毋須額外的基礎設施。

### 輪詢

Provisa 會定期重新執行數據來源查詢，只選取 `watermark_column > last_watermark` 的資料列。（REQ-260）差異會以 SSE 事件形式發出。輪詢無法偵測到硬刪除——被移除的資料列不會令水位標記推進。若要令刪除操作可見，請使用軟刪除（例如設定 `deleted_at` 旗標）以推進水位標記欄位；該刪除操作其後會以更新事件形式送達，並附帶軟刪除標記。（REQ-260）

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

需要一個正在運行、並將資料寫入 Kafka 的 Debezium 連接器。（REQ-261）Provisa 會消費該 Kafka 主題，並將變更事件轉發至已連線的 SSE 客戶端。（REQ-261）

CDC 傳輸每個數據來源只須在 `cdc` 區塊中設定一次；主題會依 `{topic_prefix}.{schema}.{table}` 的格式衍生，而不會逐資料表重複設定。（REQ-824）之後每個資料表只須選取 `strategy: debezium`：

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

## Kafka 接收器重新導向

任何 GraphQL 訂閱都可以重新導向至 Kafka 主題，而非以串流形式傳回客戶端。（REQ-812）請在訂閱請求中加入 `X-Provisa-Sink` 標頭：

```yaml
POST /data/graphql
Authorization: Bearer <token>
Content-Type: application/json
X-Provisa-Sink: kafka://broker:9092/my-topic
```

伺服器會立即回應 `202 Accepted`，並啟動一項背景工作，該工作會：（REQ-812）

1. 使用與 SSE 相同的供應者解析方式（LISTEN/NOTIFY → asyncpg 輪詢 → 聯邦輪詢）監察資料表變更
2. 於每次變更時重新執行對應的查詢
3. 將結果以 JSON 訊息形式發佈至指定的 Kafka 主題

該接收器會在伺服器程序的整個生命週期內運行。（REQ-812）如要停止，請重新啟動伺服器（透過管理 API 進行持久化接收器註冊的功能已在規劃中）。

**URI 格式：** `kafka://[broker:port]/topic`

- 如省略 `broker:port`，則會使用 `KAFKA_BOOTSTRAP_SERVERS` 環境變數（預設值：`localhost:9092`）（REQ-812）
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

### 作為設定層級第二輸出的 Kafka 接收器

以輪詢為基礎的資料表訂閱，可透過 `provisa.yaml` 同時發佈至 Kafka 主題。（REQ-282、REQ-286）SSE 訂閱與 Kafka 接收器同為同一個 Live Query Engine 的輸出。（REQ-282）每個輸出都會獨立追蹤其水位標記。（REQ-286）

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
```

完整的接收器設定參考，請參閱 [Kafka Sinks](sources.md)。

## 安全性

所有訂閱模式均採用與一般查詢相同的安全管線：（REQ-258、REQ-038）

- 行級安全篩選會套用至每一列已發出的資料（REQ-040）
- 已遮罩的欄位在事件中會顯示為已遮罩（REQ-040）
- 角色授權會於連線時進行驗證（REQ-258）

## 客戶端範例

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
