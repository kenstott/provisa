# 開發人員快速入門

如要在不從原始碼建置的情況下評估 Provisa，請參閱[快速入門](index.md)——下載 macOS、Windows 或 Linux 安裝程式，然後執行 `provisa start`。（REQ-223、REQ-224、REQ-227）

本指南適用於**從版本庫執行** Provisa——即積極開發、偵錯或貢獻程式碼的情況。

---

## 先決條件

- **Docker Desktop**（執行中）
- **Python 3.12+**
- **Node.js 20+**
- **Git**

---

## 1. 複製版本庫並設定

```bash
git clone https://github.com/kenstott/provisa.git
cd provisa
./setup.sh
```

`setup.sh` 會建立 `.venv/`，透過 `pip install -e ".[dev]"` 安裝所有 Python 相依項目，並將 git hooks 設定至 `.githooks/`。[tool-verified: setup.sh lines 5–9]

---

## 2. 啟動所有服務

```bash
./start-ui.sh
```

啟動完成後，您會看到：

```
Provisa running:
  Backend: http://localhost:8001  (logs: .logs/server.log)
  UI:      http://localhost:3000
```

**啟動內容：**[tool-verified: start-ui.sh]

- Docker Compose 核心服務（`docker-compose.core.yml`）——PostgreSQL、PgBouncer、Trino、Redis（REQ-055）
- Docker Compose 開發疊加層（`docker-compose.dev.yml`）——MinIO、Kafka、MongoDB、Elasticsearch、Neo4j、Fuseki、Debezium、Schema Registry（REQ-055）
- 執行於連接埠 8001 的後端 API（當 `provisa/` 及 `config/` 有變更時熱重載）（REQ-618）
- 執行於連接埠 3000 的 Vite UI 開發伺服器（HMR）
- 於 `http://localhost:3100` 提供的 OpenTelemetry 追蹤及 Grafana。可觀測性堆疊是一個選用的 docker-compose `observability` 設定檔（OTel Collector、Prometheus、Tempo、Grafana），在平台層級預設並非啟用；除非您傳遞 `--no-observability`，否則 `start-ui.sh` 會將其作為開發指令碼的便利功能加以啟用。（REQ-302、REQ-303、REQ-330）

**Ctrl+C** 會停止所有項目——後端、UI 及所有 Docker 服務——並還原任何設定修補。（REQ-619）

**Ctrl+R** 只會重新啟動後端（在熱重載未能偵測到的設定變更後很有用）。（REQ-619）

### 選項

`--no-observability`——停用分散式追蹤。預設情況下，`start-ui.sh` 會下載尚未存在的 OpenTelemetry Java 代理程式，修補 Trino 的 `jvm.config` 以載入該代理程式，並啟動 OTel collector、Prometheus、Tempo 及 Grafana。傳遞 `--no-observability` 即可略過以上所有動作。`jvm.config` 的修補會在按下 Ctrl+C 時還原。[tool-verified: start-ui.sh lines 15, 67–82]（REQ-330）

`--seed-data`——在 Docker 服務健康後，以示範數據填入 Kafka。預設不會執行。[tool-verified: start-ui.sh lines 14, 173–178]

`--keep-docker`——在按下 Ctrl+C 後讓 Docker Compose 服務繼續執行，而非呼叫 `docker compose down`。[tool-verified: start-ui.sh lines 16, 301–306]（REQ-619）

`--reset-volumes`——清除所有 Docker 磁碟區，並以乾淨狀態重新啟動。適用於 Docker 當機後的復原。[tool-verified: start-ui.sh line 19]（REQ-170）

`--demo`——啟動額外的示範數據來源（PostgreSQL pet-store 結構描述、OpenAPI petstore 模擬、SQLite，以及一個遠端 GraphQL）。並會自動填入 petstore 使用者及訂單數據。[tool-verified: start-ui.sh lines 17, 55–171]

`--idp=basic|firebase`——啟用用於驗證的身分識別提供者。若無此旗標，後端會在沒有驗證提供者的情況下執行，所有要求都會被視為 `admin`。[tool-verified: start-ui.sh line 18; provisa/auth/wiring.py lines 57–60; provisa/auth/middleware.py lines 57–68]（REQ-120、REQ-124）

---

## 3. 連接數據來源

Provisa 會從 `config/` 讀取設定。請新增一個來源檔案——例如 `config/sources/my-db.yaml`：

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

## 4. 執行您的第一個查詢

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

當 `config/provisa.yaml` 中沒有 `auth` 區段時（開發環境的預設值），即不需要進行驗證。預設角色為 `admin`。[tool-verified: provisa/auth/wiring.py lines 57–60; provisa/auth/middleware.py lines 56–68]（REQ-120、REQ-267）

---

## 5. 開啟 UI

在瀏覽器中開啟 `http://localhost:3000`。

導覽列有四個頂層選單：[tool-verified: provisa-ui/src/components/NavBar.tsx lines 39–80]

- **Explore**——結構描述總管（`/schema`）、GraphQL 編輯器（`/query`）、Cypher 編輯器（`/graph`）、SQL 編輯器（`/sql`）
- **Model**——檢視及 Commands
- **Security**——行級安全及欄遮罩原則（REQ-038、REQ-041）
- **Admin**——概覽、網域、快取、排程工作、系統健康狀態、可觀測性、使用者、組織、角色

管理用 GraphQL API 位於 `http://localhost:8001/admin/graphql`。[tool-verified: provisa/api/app.py line 3389]（REQ-620）

---

## 疑難排解

**後端無法啟動**——請查看 `.logs/server.log`。最常見的原因是缺少環境變數，或連接埠 8001 發生衝突。[tool-verified: start-ui.sh line 202]（REQ-618）

**Docker 服務不健康**——執行 `docker compose -f docker-compose.core.yml -f docker-compose.dev.yml ps`，以查看哪個服務卡住。聯邦引擎在首次啟動時大約需時 30 秒。（REQ-055）

**連接埠 3000 或 8001 發生衝突**——`start-ui.sh` 會在啟動前終止佔用這些連接埠的過期處理程序。若連接埠被其他項目佔用，請先手動停止該項目。[tool-verified: start-ui.sh lines 197–199]（REQ-619）

**全新啟動**——停止指令碼，然後執行 `./start-ui.sh --reset-volumes`，以清除所有磁碟區並重新啟動。[tool-verified: start-ui.sh line 19]（REQ-170）

---

## 後續步驟

| 目標 | 文件 |
|------|-----|
| 完整 YAML 設定參考 | [configuration.md](configuration.md) |
| 行級安全、欄遮罩、驗證 | [security.md](security.md) |
| 所有支援的來源類型 | [sources.md](sources.md) |
| 即時訂閱 | [subscriptions.md](subscriptions.md) |
| JDBC、BI 工具、Arrow Flight、Apollo Federation | [integrations.md](integrations.md) |
| Python 用戶端 | [python-client.md](python-client.md) |
| 生產環境部署 | [deployment.md](deployment.md) |
