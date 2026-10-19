# 開發人員快速入門 {: #developer-quick-start }

如想在不從原始碼建構的情況下試用 Provisa，請參閱[快速開始](index.md)：下載 macOS、Windows 或 Linux 安裝程式，然後執行 `provisa start`。

本指南介紹如何**從版本庫執行** Provisa，適用於開發、偵錯或貢獻程式碼。

---

## 先決條件 {: #prerequisites }

- **Docker Desktop**（執行中）
- **Python 3.12+**
- **Node.js 20+**
- **Git**

---

## 1. 複製版本庫並設定 {: #1-clone-and-set-up }

```bash
git clone https://github.com/kenstott/provisa.git
cd provisa
./setup.sh
```

`setup.sh` 會建立 `.venv/`，透過 `pip install -e ".[dev]"` 安裝所有 Python 相依項目，並將 git hooks 設定至 `.githooks/`。

---

## 2. 啟動所有服務 {: #2-start-everything }

```bash
./start-ui.sh
```

啟動完成後，您會看到：

```yaml
Provisa running:
  Backend: http://localhost:8001  (logs: .logs/server.log)
  UI:      http://localhost:3000
```

**啟動內容：**

- Docker Compose 核心服務（`docker-compose.core.yml`）——PostgreSQL、PgBouncer、Trino、Redis
- Docker Compose 開發疊加層（`docker-compose.dev.yml`）——MinIO、Kafka、MongoDB、Elasticsearch、Neo4j、Fuseki、Debezium、Schema Registry
- 執行於連接埠 8001 的後端 API（當 `provisa/` 及 `config/` 有變更時熱重載）
- 執行於連接埠 3000 的 Vite UI 開發伺服器（HMR）
- 於 `http://localhost:3100` 提供的 OpenTelemetry 追蹤及 Grafana。可觀測性堆疊（OTel Collector、Prometheus、Tempo、Grafana）是一個選用的 docker-compose `observability` 設定檔，在平台上預設關閉；`start-ui.sh` 為方便開發會把它開啟，除非您傳遞 `--no-observability`。

**Ctrl+C** 會停止所有項目（後端、UI 及所有 Docker 服務），並還原所有設定修補。

**Ctrl+R** 只會重新啟動後端（在熱重載未能偵測到的設定變更後很有用）。

### 選項 {: #options }

`--no-observability`——停用分散式追蹤。預設情況下，`start-ui.sh` 會下載尚未存在的 OpenTelemetry Java 代理程式，修補 Trino 的 `jvm.config` 以載入該代理程式，並啟動 OTel collector、Prometheus、Tempo 及 Grafana。傳遞 `--no-observability` 即可略過以上所有動作。`jvm.config` 的修補會在按下 Ctrl+C 時還原。

`--seed-data`——在 Docker 服務健康後，以示範數據填入 Kafka。預設不會執行。

`--keep-docker`——在按下 Ctrl+C 後讓 Docker Compose 服務繼續執行，而非呼叫 `docker compose down`。

`--reset-volumes`——清除所有 Docker 磁碟區，並以乾淨狀態重新啟動。適用於 Docker 當機後的復原。

`--demo`——啟動額外的示範數據來源（PostgreSQL pet-store 結構描述、OpenAPI petstore 模擬、SQLite，以及一個遠端 GraphQL）。並會自動填入 petstore 使用者及訂單數據。

`--source=<name>`（僅限 `start-ui-install.sh`，可重複）——與 `--demo` 一併佈建一個可選的數據來源。每個名稱對應 `demo/sources/<name>/`。啟動時會呼叫 `demo/sources/provision.py up`，它會把該數據來源的 `compose.yml` 作為其獨立的 Docker Compose 專案（`provisa-demo-<name>`）啟動，等待其健康檢查，並在數據來源有 `prime.py` 時執行它以植入數據。之後啟動會在 `${PROVISA_HOME:-~/.provisa}/demo/provisa-with-sources.yaml` 寫入一份包裝設定，其中包含基本設定加上每個數據來源的 `fragment.yaml`，並由它啟動。

使用者介面端對端測試套件呼叫的也是同一個 `provision.py`，以便啟動這些數據來源（在自己的連接埠上，使用 `provisa-e2e-<name>` 專案前綴），因此示範所顯示的植入數據，與測試套件所斷言的資料列，只定義一次。

以 Docker 啟動（沒有 `--demo`/`--native`）時，協調器是一個容器，因此每個數據來源都會加入核心堆疊的網絡，並以 `<name>:<container port>` 註冊；以原生方式啟動時，則以 `localhost:<published port>` 註冊。若數據來源的 `demo/sources/<name>/engine` 檔案指名了此次啟動不會執行的引擎，該數據來源會被拒絕。

隨附的數據來源：

| 名稱 | 連接埠 | 說明 |
|------|---------|-------|
| `neo4j` | HTTP 27474, Bolt 27687 | 兩張 Cypher 資料表（`adopter`、`adopter_referral`）；圖由 `seed.cypher` 植入；資料表從片段註冊 |
| `mongodb` | 27117 | 數據來源已註冊；`product_reviews` 集合由 `db/mongo-init.js` 植入；資料表透過 Register Table 手動註冊 |
| `redis` | 26379 | 數據來源已註冊；`support_agent:*` 及 `agent_status:*` 雜湊由 `prime.py` 植入；每個前綴透過 Register Table 註冊為一張資料表 |
| `cassandra` | 29042 | 數據來源已註冊；`shelter_ops.intake_events` 由 `prime.py` 植入（需要 `cassandra` 額外套件）；keyspace 透過 Register Table 註冊為結構描述 |
| `sparql` | 23030 | Apache Jena Fuseki；數據來源及一張以查詢為基礎的資料表（`volunteer`）從片段註冊，圖由 `prime.py` 植入；更多資料表透過 Register Table（查詢 + Preview）註冊 |
| `prometheus` | 29090 | 數據來源已註冊；伺服器抓取自身，因此 `up` 及 `prometheus_*` 指標透過 Register Table 註冊為資料表 |
| `elasticsearch` | 29200 | 數據來源及索引對映已註冊；`support_tickets` 索引由 `prime.py` 植入；由原生引擎經 HTTP 讀取，在 Trino 上經由連接器讀取 |
| `splunk` | mgmt 8089, HEC 8088 | 數據來源以權杖驗證及 `disable_ssl_validation` 註冊（容器的憑證為自簽）；一個索引、七個收容所警示事件及 `shelter_alerts` 數據模型由 `prime.py` 植入，它亦會產生片段以 `PROVISA_DEMO_SPLUNK_TOKEN` 讀取的 API 權杖。數據模型透過 Register Table 註冊為資料表：在 Trino 上經由 `splunk` 目錄，在其他所有引擎上經由引擎掛載的 Splunk 驅動程式 pgwire 端點 |
| `chinook` | 25433 | 存放 Hasura 中繼資料範例所追蹤的 snake_case Chinook 子集的 Postgres，由 `prime.py` 從 `tests/fixtures/hasura_v2_t1_seed.sql` 植入；數據來源從片段註冊，也是匯入 `tests/fixtures/hasura_v2_t1_metadata.json` 的 Hasura v2 匯入所落地的數據來源 |

`--idp=basic|firebase`——啟用用於驗證的身分識別提供者。若無此旗標，後端會在沒有驗證提供者的情況下執行，所有要求都會被視為 `admin`。

---

## 3. 連接數據來源 {: #3-connect-a-data-source }

Provisa 會從 `config/` 讀取設定。新增一個來源檔案，例如 `config/sources/my-db.yaml`：

```yaml
sources:
  - id: my-pg
    type: postgresql
    host: localhost
    port: 5432
    database: mydb
    username: myuser
    password: ${MY_DB_PASSWORD}
    tables:
      - id: orders
        publish: true
        columns:
          - name: id
          - name: amount
          - name: region
          - name: customer_id
```

設定環境變數後，後端會在下次重新載入時套用：

```bash
export MY_DB_PASSWORD=secret
```

如需完整 YAML 參考及所有支援的來源類型，請參閱 [docs/configuration.md](configuration.md)。

---

## 4. 執行您的第一個查詢 {: #4-run-your-first-query }

```bash
# GraphQL
curl -s -X POST http://localhost:8001/data/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "{ orders { id amount region } }"}' | jq

# SQL — use the /data/sql endpoint
curl -s -X POST http://localhost:8001/data/sql \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT id, amount, region FROM orders LIMIT 5"}' | jq
```

當 `config/provisa.yaml` 中沒有 `auth` 區段時（開發環境的預設值），即不需要進行驗證。預設角色為 `admin`。

---

## 5. 開啟 UI {: #5-open-the-ui }

在瀏覽器中開啟 `http://localhost:3000`。

導覽列有四個頂層選單：

- **Explore**——結構描述總管（`/schema`）、GraphQL 編輯器（`/query`）、Cypher 編輯器（`/graph`）、SQL 編輯器（`/sql`）
- **Model**——檢視及 Commands
- **Security**——行級安全及欄遮罩原則
- **Admin**——概覽、網域、快取、排程工作、系統健康狀態、可觀測性、使用者、組織、角色

管理用 GraphQL API 位於 `http://localhost:8001/admin/graphql`。

---

## 疑難排解 {: #troubleshooting }

**後端無法啟動**——請查看 `.logs/server.log`。最常見的原因是缺少環境變數，或連接埠 8001 發生衝突。

**Docker 服務不健康**——執行 `docker compose -f docker-compose.core.yml -f docker-compose.dev.yml ps`，以查看哪個服務卡住。聯邦引擎在首次啟動時大約需時 30 秒。

**連接埠 3000 或 8001 發生衝突**——`start-ui.sh` 會在啟動前終止佔用這些連接埠的過期處理程序。若連接埠被其他項目佔用，請先手動停止該項目。

**全新啟動**——停止指令碼，然後執行 `./start-ui.sh --reset-volumes`，以清除所有磁碟區並重新啟動。

---

## 後續步驟 {: #next-steps }

| 目標 | 文件 |
| ------ | ----- |
| 完整 YAML 設定參考 | [configuration.md](configuration.md) |
| 行級安全、欄遮罩、驗證 | [security.md](security.md) |
| 所有支援的來源類型 | [sources.md](sources.md) |
| 即時訂閱 | [subscriptions.md](subscriptions.md) |
| JDBC、BI 工具、Arrow Flight、Apollo Federation | [integrations.md](integrations.md) |
| Python 用戶端 | [python-client.md](python-client.md) |
| 生產環境部署 | [deployment.md](deployment.md) |
