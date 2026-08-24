# 部署

## 選擇部署路徑

Provisa 支援六條部署路徑。請依你的受眾與營運脈絡選擇：

| 路徑 | 構件／腳本 | 最適用於 |
| ------ | ------------------- | ---------- |
| **開發** | `start-ui.sh` | 從原始碼開發、附完整示範數據的評估 |
| **macOS 安裝程式** | `Provisa-<version>-macOS.dmg` | 開發者工作站、評估 |
| **Windows 安裝程式** | `Provisa-<version>-windows-x64.exe` | 開發者工作站、評估 |
| **Linux AppImage** | `Provisa.AppImage` | 內部部署伺服器、雲端 VM、氣隙環境 |
| **雲端 VM（AWS）** | `terraform/deploy.sh` | 帶負載平衡器的多節點雲端部署 |
| **Kubernetes** | `helm/provisa/` | 已在營運 K8s 的團隊 |

### VM 對比 Kubernetes

兩者都是企業級的。VM／AppImage 路徑較簡單：毋須佈建叢集、毋須設定 CNI 或 RBAC 政策，且 AppImage 完全自足（REQ-223）。它自然地融入既有的伺服器管理工具（Ansible、Puppet、Datadog agent、Splunk forwarder 等）。

只有在你的團隊已在營運 K8s 叢集，並希望 Provisa 參與該營運模式（滾動部署、HPA、統一可觀測性）時，才選 Kubernetes（REQ-056）。兩者能力相當——Kubernetes 增加的是營運開銷，不是能力。

### 映像取得與安全掃描

所有正式環境路徑，在任何部署可以執行之前，都必須先取得 Provisa 構件。「氣隙」指的是安裝時在目標機器上發生的事——構件必須先取得。

**macOS 與 Windows 安裝程式：** 自 [GitHub 發行頁面](https://github.com/provisa/provisa/releases) 下載。完整打包；下載後毋須網際網路（REQ-227）。用於開發／評估，不用於正式環境——不預期有映像掃描閘門。

**AppImage 路徑：** 自 [GitHub 發行頁面](https://github.com/provisa/provisa/releases) 下載並傳輸到目標機器。AppImage 把所有元件映像以 tarball 形式打包在一個 squashfs 檔案系統內（REQ-294）——多數登錄檔掃描器無法就地檢視它們。請聯絡你的 Provisa 客戶團隊索取元件映像摘要，以便獨立地與你的掃描器比對驗證。

**Terraform 路徑：** 執行 `terraform/deploy.sh` 之前，必須先把 AppImage 上載到 S3。EC2 節點於開機時經 IAM 角色下載它——它們需要對外的 S3 存取（直接或經 VPC 閘道端點）。套用與 AppImage 路徑相同的掃描政策。

**Helm／Kubernetes 路徑：** 各個映像必須推送到叢集可觸及的登錄檔。這條路徑與以登錄檔為基礎的掃描（Prisma Cloud、Aqua、Trivy、AWS Inspector）最相容——映像是掃描器原生理解的一等物件。氣隙叢集請把映像鏡像到內部登錄檔，並在 `values.yaml` 中覆寫參照（REQ-294）。

---

## 開發（從原始碼）

### 建議：`start-ui.sh`

從原始碼執行 Provisa 最簡單的方式。一道命令啟動所有基礎設施、後端 API 與 UI 開發伺服器（REQ-055）。Ctrl+C 會乾淨地關閉一切。

**先決條件：** Docker Desktop、Node.js、位於 `.venv/` 的 Python virtualenv

```bash
./start-ui.sh
```

它做什麼：

- 啟動 `docker-compose.core.yml` + `docker-compose.dev.yml`（所有核心 + 示範服務）並等待其健康（REQ-055）
- 以示範數據播種 Kafka
- 從 `.venv/` 同步 Python 相依項
- 在連接埠 8001 啟動後端 API（記錄至 `.logs/server.log`）（REQ-558）
- 在連接埠 3000 啟動 Vite UI 開發伺服器（REQ-559）
- 印出各 URL 並等待；Ctrl+C 停止一切並拆除 compose

```yaml
Backend: http://localhost:8001
UI:      http://localhost:3000
```

**選項：**

`--reset-volumes` — 啟動前先執行 `docker compose down -v`，銷毀所有 Docker 磁碟區（PostgreSQL 數據、MinIO 物件、Redis 狀態等）（REQ-170）。當你想要一張完全乾淨的白紙時使用——開發期間更改結構描述之後，或 Docker 當掉並留下損毀的磁碟區時。**所有數據都會遺失。**

`--observability` — 加入完整的追蹤與量度檢測。它下載 OpenTelemetry Java agent 並修補 Trino 的 `jvm.config` 以載入它，以 OTLP 匯出檢測 Provisa 後端，並啟動 OTel collector、Prometheus、Tempo 與 Grafana（`http://localhost:3100`）（REQ-330）。該 `jvm.config` 修補會在 Ctrl+C 時自動還原。

### 手動步驟（僅後端，無 UI）

如果你只需要 API：

1. 安裝 [Docker Desktop](https://docs.docker.com/get-docker/)
2. 啟動核心服務：

   ```bash
   docker compose -f docker-compose.core.yml up -d
   ```

3. 啟動 API：

   ```bash
   uvicorn main:app --reload --port 8001
   ```

4. 驗證：`curl http://localhost:8001/health`

### 完整堆疊（Provisa 在容器中）

若要以容器而非在主機上執行 API：

```bash
docker compose -f docker-compose.core.yml -f docker-compose.app.yml up -d
```

### 各項服務

**核心（`docker-compose.core.yml`）——一律必要：**

| 服務 | 連接埠 | 用途 |
| --------- | ------ | --------- |
| PostgreSQL | 5432 | 設定中繼資料 + Iceberg 目錄（REQ-169） |
| PgBouncer | 6432 | 連線池（REQ-053） |
| 聯邦引擎 | 8080 | 查詢聯邦（REQ-028） |
| Redis | 6379 | 查詢結果快取（REQ-371） |
| MinIO | 9000/9001 | 相容 S3 的物件儲存（REQ-029、REQ-171） |

**示範（`docker-compose.dev.yml`）——選用，由 `start-ui.sh` 一併納入：**

| 服務 | 連接埠 | 用途 |
| --------- | ------ | --------- |
| MongoDB | 27017 | 示範 NoSQL 數據來源 |
| Kafka | 9092 | 示範串流數據來源 |
| Schema Registry | 8081 | 示範 Avro／Protobuf 結構描述管理 |
| Debezium | — | 示範 CDC 連接器 |
| Elasticsearch | 9200 | 示範搜尋數據來源 |
| Neo4j | 7474/7687 | 示範圖形數據來源 |
| Fuseki | 3030 | 示範 SPARQL 三元組儲存 |
| OpenTelemetry Collector | — | 追蹤收集（搭配 `--observability`）（REQ-302） |
| Prometheus | 9090 | 量度（搭配 `--observability`）（REQ-330） |
| Tempo | — | 追蹤儲存（搭配 `--observability`）（REQ-330） |
| Grafana | 3100 | 儀表板（搭配 `--observability`）（REQ-330） |

### 遙測後端（`otlp2sql`）

上述的 `--observability` 堆疊（Collector → Tempo／Prometheus／Grafana）是其中一條
遙測路徑。另一條是 `otlp2sql`（`provisa.observability.otlp2sql`）：一個
OTLP/HTTP 接收器，把追蹤、量度與記錄寫入由 SQLAlchemy URL 選定的 SQL
資料庫，並在擷取時抽出 `provisa.*` span 屬性，
因此毋須另跑壓實作業。寫入是批次的
（`OTLP2SQL_BATCH_MAX_ROWS`，預設 1000；`OTLP2SQL_BATCH_MAX_SECS`，預設 2 秒）。

遙測有自己的儲存區，與控制平面資料庫分開。以
`PROVISA_OPS_DB_URL` 選定後端：

| `PROVISA_OPS_DB_URL` | 後端 | 備註 |
| --- | --- | --- |
| *（未設定）* | `~/.provisa/telemetry/` 之下的專用 DuckDB | 預設；無伺服器、無 Docker |
| `clickhouse+native://user@host/otel` | ClickHouse | 高速率擷取，帶自動背景合併 |
| `postgresql+psycopg2://user@host/otel` | PostgreSQL | 中等量 |
| `trino://user@host:8080/otel` | Trino／Iceberg | 技術上可行，**不建議**——見下文 |

**關於 `trino://`：** SQLAlchemy Trino 方言發出有效的 Trino DDL 與
`INSERT`，因此作為 `otlp2sql` 後端在技術上是可行的。除了低擷取速率之外，
都不建議這麼做。每一次批次排清都變成一次分散式 Trino `INSERT`
外加一次 Iceberg 快照，因此高速率遙測會
產生許多小檔案與快照，而且仍需要定期執行
`ALTER TABLE ... EXECUTE optimize`／`expire_snapshots`——而 `otlp2sql` 並
不會跑這些。它同時把查詢引擎放進擷取的熱路徑上。

要把大量遙測送進 Trino／Iceberg，請改用 `otlp2parquet`：它
不經 Trino 就把 parquet 寫入物件儲存，再由排程的
Trino 壓實把原始檔案捲進上線中的 Iceberg 表。若要單一
引擎同時處理高速率擷取與壓實，優先選 ClickHouse。

把應用程式與 Trino 的 OTLP 匯出器（`OTEL_EXPORTER_OTLP_ENDPOINT`）指向
`otlp2sql` 端點，並以同一個 `PROVISA_OPS_DB_URL` 註冊 ops 網域，
好讓它讀到接收器所寫入的內容。

---

## macOS 安裝程式

供開發者工作站與評估使用。完全氣隙——下載後毋須網際網路（REQ-227）。

基礎安裝程式是**原生安裝**：DuckDB 聯邦引擎 + SQLite 控制平面 + 記憶體內（fakeredis）快取，沒有 Docker、VM、Trino、Redis 或 MinIO（REQ-972、REQ-979）。聯邦引擎是精靈中的一項選擇——DuckDB（原生，預設）、Trino-on-Docker，或外部引擎（REQ-973）。可觀測性是一律開啟的自我遙測，可在管理介面中檢視；Docker 的 collector／Prometheus／Grafana 堆疊是選用的外部示範，不是一個開關（REQ-975）。示範數據包是選用的，且預設關閉（REQ-978）。Trino、Docker 可觀測性堆疊與示範，都是採本地優先解析的重量級附加元件（安裝程式鄰近目錄、掛載的磁碟區、`~/Downloads`，然後才是 GitHub 發行），因此企業可為氣隙安裝預先備妥 tarball（REQ-977）。

### 步驟

1. 自 [GitHub 發行頁面](https://github.com/provisa/provisa/releases) 下載 `Provisa-<version>-macOS.dmg`
2. 開啟該 DMG，把 **Provisa.app** 拖到 `/Applications`
3. 按兩下 **Provisa.app**——首次啟動設定執行一次；精靈會提供上述的引擎、可觀測性與示範選項（REQ-1007）
4. 開啟終端機：

   ```bash
   provisa start    # start all services
   provisa status   # confirm all services are running
   provisa open     # open the UI in the browser
   ```

   （REQ-224）

### 數據持續性

所有數據儲存在 `~/.provisa/`（REQ-224）。若要移除一切：`provisa uninstall`。

---

## Windows 安裝程式

供開發者工作站與評估使用。完全氣隙——下載後毋須網際網路（REQ-227）。

與 macOS 一樣，基礎的 Windows 安裝程式是**原生層**：一份獨立的 Python 執行階段 + provisa wheel + DuckDB/pg_duckdb + SQLite 控制平面，不隨附 Docker、不隨附 VM，也不隨附容器映像（REQ-979）。聯邦引擎（Trino）、可觀測性堆疊與示範數據包，稍後經個別的分層安裝程式依序加入：先是 Container 安裝程式（`Provisa-Container-<version>.exe`，它加入 WSL2 + containerd + Trino），然後是 Obs 安裝程式（需要容器層），最後是 Demo 安裝程式（需要 Core + Obs）。首次啟動的指引會說明如何執行 Container 安裝程式來初始化聯邦引擎（REQ-1005）。

### 步驟

1. 自 [GitHub 發行頁面](https://github.com/provisa/provisa/releases) 下載 `Provisa-<version>-windows-x64.exe`
2. 執行該安裝程式——毋須管理員權限；安裝到 `%LOCALAPPDATA%\Programs\Provisa\`
3. 自「開始」功能表開啟 **Provisa First Launch**——原生設定執行一次，並印出分層附加元件的後續步驟指引（REQ-1005）
4. 開啟一個新的終端機：

   ```text
   provisa status
   provisa open
   ```

   （REQ-224）

### 數據持續性

所有數據儲存在 `%USERPROFILE%\.provisa\`。

---

## Linux AppImage——單節點或多節點 VM

### 它是什麼

`Provisa.AppImage` 是單一自足的可執行檔，打包了（REQ-223、REQ-228）：

- 一個無 root 的 Docker 常駐程式（`dockerd-rootless.sh` + `rootlesskit`）——毋須系統 Docker 或 root
- 所有容器映像 tarball（PostgreSQL、PgBouncer、MinIO、Redis、聯邦引擎、Provisa API）（REQ-294）
- Provisa CLI 包裝器與首次啟動設定腳本

Provisa 映像在封裝時已預先建置——絕不含 Python 原始碼。

### 何時使用

- 內部部署的裸機或 VM（單節點或多節點）
- 沒有 K8s 叢集的雲端 VM
- 氣隙環境（REQ-294）
- 當你想要比 Kubernetes 更簡單的營運時

---

### 步驟——單節點

1. 自 [GitHub 發行頁面](https://github.com/provisa/provisa/releases) 下載 `Provisa.AppImage` 並傳輸到目標機器
2. 賦予執行權限：

   ```bash
   chmod +x Provisa.AppImage
   ```

3. 執行首次啟動設定：

   ```bash
   ./Provisa.AppImage
   ```

4. 設定精靈會詢問：
   - **角色** → 選 `primary`
   - **RAM 預算** → 要配置的 RAM 量（0 = 全部可用）；決定 Trino 工作處理程序數量
   - **主機名稱** → 此節點對外通告的位址
   - **API 連接埠** → 預設 `8000`（REQ-560）
5. 設定會載入所有容器映像（約 2–5 分鐘）、寫入設定並啟動各項服務
6. 驗證：

   ```bash
   provisa status
   curl http://localhost:8000/health
   ```

---

### 步驟——多節點（primary）

先在 primary 節點上執行這些步驟。secondary 必須在 primary 執行起來之後才設定。

1. 下載 `Provisa.AppImage` 並傳輸到 primary 機器
2. 開啟必要的防火牆連接埠（secondary 會由此向內連線）：

   | 連接埠 | 服務 |
   | ------ | --------- |
   | 5432 | PostgreSQL |
   | 6379 | Redis |
   | 9000 | MinIO |
   | 8080 | 聯邦引擎協調器 |
   | 8000 | Provisa API |

3. 賦予執行權限並執行：

   ```bash
   chmod +x Provisa.AppImage
   ./Provisa.AppImage
   ```

4. 設定精靈會詢問：
   - **角色** → 選 `primary`
   - **RAM 預算**、**主機名稱**、**API 連接埠** → 與單節點相同作答
5. 設定完成後，記下此機器的**私有 IP**——secondary 需要它
6. 精靈會印出一段 nginx upstream 區塊——請保存下來供你的負載平衡器設定使用
7. 驗證：

   ```bash
   provisa status
   curl http://localhost:8000/health
   ```

---

### 步驟——多節點（每個 secondary）

在 primary 執行起來且可觸及之後，於每個額外節點上重複這些步驟。

1. 下載 `Provisa.AppImage` 並傳輸到 secondary 機器
2. 確認 secondary 可觸及 primary：

   ```bash
   curl http://<primary-ip>:8000/health
   ```

3. 賦予執行權限並執行：

   ```bash
   chmod +x Provisa.AppImage
   ./Provisa.AppImage
   ```

4. 設定精靈會詢問：
   - **角色** → 選 `secondary`
   - **Primary IP** → 輸入 primary 節點的 IP（連通性會即時驗證）
   - **RAM 預算**、**主機名稱**、**API 連接埠** → 同上作答
5. 設定載入縮減後的映像集（沒有 PostgreSQL、PgBouncer、MinIO、Redis——那些只在 primary 上執行）（REQ-561），並啟動 Provisa API 與一個聯邦引擎工作處理程序
6. 驗證：

   ```bash
   provisa status
   curl http://localhost:8000/health
   ```

7. 把此節點加入你的負載平衡器 upstream

---

### primary／secondary 拓撲

**primary 節點**執行所有單例服務：

| 服務 | 為何是單例 |
| --------- | --------------- |
| PostgreSQL | 共用結構描述、應用程式設定、語意模型 |
| Redis | 共用查詢結果快取與訂閱狀態（REQ-371） |
| MinIO | 供重新導向結果與 MV 快照使用的共用物件儲存區（REQ-029） |
| 聯邦引擎協調器 | 所有工作處理程序（primary + secondary）都在此註冊（REQ-028） |

**secondary 節點**只執行：

- Provisa API——無狀態；啟動時從 primary 上的 PostgreSQL 讀取所有設定（REQ-057、REQ-562）
- 聯邦引擎工作處理程序——向 primary 上的協調器自我註冊（REQ-028）

所有應用程式狀態都流經 primary 的 PostgreSQL。毋須手動同步。（REQ-562）

---

### 非互動式（自動化）首次啟動

供 Terraform、cloud-init 或 Ansible 使用——以旗標取代回答提示：

```bash
# Primary
./Provisa.AppImage --non-interactive --role primary --ram-gb 32

# Secondary
./Provisa.AppImage --non-interactive --role secondary --primary-ip 10.0.0.10 --ram-gb 32
```

非互動模式會安裝一個 systemd 單元（`/etc/systemd/system/provisa.service`）以便開機即啟動。（REQ-563）

| 旗標 | 說明 |
| ------ | ------------- |
| `--non-interactive` | 略過所有提示；安裝 systemd 單元 |
| `--role primary\|secondary` | 節點角色 |
| `--primary-ip <ip>` | primary 節點 IP（secondary 必填） |
| `--ram-gb <n>` | 要配置的 RAM（0 = 全部可用） |

---

## 雲端 VM 部署——Terraform（AWS）

以一道互動式命令，在 AWS 上佈建一整套多節點 Provisa 叢集——VPC、安全群組、EC2 執行個體、ALB、NLB。（REQ-564）

### 檔案

| 檔案 | 用途 |
| ------ | --------- |
| `terraform/deploy.sh` | 互動式包裝器——收集參數、驗證憑證、寫入 `terraform.tfvars`、執行 apply |
| `terraform/aws/variables.tf` | 所有變數定義與預設值 |
| `terraform/aws/main.tf` | VPC、子網路、安全群組、IAM、EC2、ALB、NLB |
| `terraform/aws/outputs.tf` | 端點 URL 與節點 IP |

### 步驟

1. 自 [GitHub 發行頁面](https://github.com/provisa/provisa/releases) 下載 `Provisa.AppImage`

2. 把它上載到你 AWS 帳戶中的一個 S3 貯體：

   ```bash
   aws s3 cp Provisa.AppImage s3://<your-bucket>/releases/Provisa.AppImage
   ```

3. 確保 AWS 憑證在你的 shell 中可用（以下任一）：
   - 環境變數：`AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY`
   - 具名設定檔：`export AWS_PROFILE=my-profile`
   - 使用中的 SSO 工作階段：`aws sso login`

4. （選用）若你想要 SSH 存取節點，請在目標區域建立一組 EC2 金鑰配對並記下該金鑰配對名稱

5. 執行部署包裝器：

   ```bash
   bash terraform/deploy.sh
   ```

6. 回答精靈的問題（見下方參照表）。腳本會在繼續之前先驗證 AppImage 存在於 S3，不存在則中止

7. 檢閱部署摘要並確認

8. Terraform 佈建所有基礎設施（約 5–10 分鐘）。apply 之後，腳本會印出：

   ```text
   api_endpoint      = "http://<alb-dns>:8000"
   flight_endpoint   = "<nlb-dns>:8815"
   primary_ip        = "10.0.x.x"
   secondary_ips     = ["10.0.x.x", ...]
   ```

   （REQ-564、REQ-143）

9. （選用）把 DNS 記錄指向 ALB 與 NLB 的 DNS 名稱

10. 驗證：

    ```bash
    curl http://<api_endpoint>/health
    ```

### 精靈問題

| 問題 | 預設 | 備註 |
| ---------- | --------- | ------- |
| 雲端提供者 | — | 目前僅 AWS |
| AWS 憑證 | — | 先檢查使用中的工作階段 |
| 區域 | `us-east-1` | |
| 節點數量 | `2` | 1 = 只有 primary，無 LB；2+ = primary + secondary + ALB/NLB |
| 執行個體類型 | `m7i.2xlarge` | 見下方規格指南 |
| 根磁碟區大小 | `100 GB` | 每節點 |
| RAM 預算 | `0`（全部 RAM） | 決定每節點的 Trino 工作處理程序數量 |
| S3 貯體 | — | 繼續之前即時驗證 |
| S3 索引鍵 | `releases/Provisa.AppImage` | |
| SSH 存取 | 否 | 需要既有的金鑰配對名稱 + 管理 CIDR |
| VPC CIDR | `10.0.0.0/16` | |

### 執行個體規格指南

| 類型 | vCPU | RAM | 每節點 Trino 工作處理程序 | 使用情境 |
| ------ | ------ | ----- | -------------------- | ---------- |
| `m7i.xlarge` | 4 | 16 GB | 0 | 開發／小型數據集 |
| `m7i.2xlarge` | 8 | 32 GB | 1 | 小型正式環境 |
| `m7i.4xlarge` | 16 | 64 GB | 2 | 中型正式環境 |
| `m7i.8xlarge` | 32 | 128 GB | 4 | 大型正式環境 |

所有節點都把工作處理程序貢獻給 primary 上的同一個協調器（REQ-028）。一個 3 節點的 `m7i.4xlarge` 叢集共產生 6 個 Trino 工作處理程序。

### 會佈建什麼

- 跨兩個可用區、含兩個公用子網路的 VPC（REQ-564）
- 安全群組：LB 群組（8000/8815 上的公用輸入）、節點群組（LB → 節點、叢集內部、選用 SSH）
- 對 AppImage 貯體具備 S3 GetObject 的 IAM 角色 + 執行個體設定檔
- primary EC2 執行個體——以 `--non-interactive --role primary` 模式執行首次啟動
- secondary EC2 執行個體（node_count − 1）——以 `--non-interactive --role secondary --primary-ip <primary private IP>` 模式執行首次啟動；相依於 primary 先完成
- 連接埠 8000 上的 ALB——HTTP API，健康檢查 `/health`（REQ-560）
- 連接埠 8815 上的 NLB——Arrow Flight／gRPC（REQ-143）
- 兩個 LB 都附掛到所有節點

### 先決條件檢查清單

- [ ] IAM 權限：EC2 完整、ELB 完整、VPC 完整、IAM 角色建立、AppImage 貯體上的 S3 GetObject
- [ ] `Provisa.AppImage` 已上載到 S3
- [ ] EC2 節點具備對外的 S3 存取（直接網際網路或 S3 VPC 閘道端點）
- [ ] 目標區域中存在 EC2 金鑰配對（如需 SSH）
- [ ] 本機已安裝 Terraform ≥ 1.5
- [ ] 已規劃 ALB／NLB 的 DNS 記錄（選用但建議）
- [ ] 若需要 HTTPS，已備妥 ACM 憑證（基礎 Terraform 未包含）

### 密鑰

Terraform 中沒有內嵌任何密鑰。AppImage 在首次啟動期間生成憑證，並寫入每個節點的 `~/.provisa/config.yaml`（REQ-563）。正式環境請於部署後自 primary 節點取回管理權杖：

```bash
ssh ubuntu@<primary-public-ip> cat ~/.provisa/config.yaml | grep admin_token
```

---

## Kubernetes／Helm

### 何時使用

你的團隊已在營運 Kubernetes 叢集，並希望 Provisa 參與該營運模式（REQ-056）。如果你正在評估 Provisa，或在沒有既有叢集的情況下作內部部署，AppImage 路徑較簡單。

注意：Provisa AppImage 無法在 Kubernetes pod 內執行——它需要 FUSE 與一個無 root 的 Docker 常駐程式，而標準 pod 安全設定檔並不提供這兩者。

### 步驟

1. 確認可存取叢集：

   ```bash
   kubectl cluster-info
   ```

2. 把映像拉取並鏡像到你的內部登錄檔（氣隙或受掃描環境必要；若直接自公用登錄檔拉取則略過）（REQ-294）：

   | 映像 | 用於 |
   | ------- | ---------- |
   | `provisa/provisa:<version>` | Provisa API |
   | `trinodb/trino:480` | 聯邦引擎協調器 + 工作處理程序（REQ-169） |
   | `postgres:16` | 叢集內 PostgreSQL（若 `postgresql.enabled`）（REQ-169） |
   | `edoburu/pgbouncer:latest` | 叢集內 PgBouncer（若 `pgbouncer.enabled`）（REQ-053） |
   | `redis:7.2` | 叢集內 Redis（若 `redis.enabled` 且無 `redis.host`）（REQ-371） |
   | `minio/minio:latest` | 叢集內 MinIO（若 `minio.enabled`）（REQ-029） |

   對於受登錄檔掃描的環境：
   - 把每個映像推送到你的暫存登錄檔
   - 執行你的掃描器（Prisma Cloud、Aqua、Trivy、AWS Inspector）並取得批准
   - 晉升到你的正式環境內部登錄檔

3. 安裝之前先決定：
   - **PostgreSQL** — 叢集內（`postgresql.enabled: true`）或外部託管（`postgresql.host`）？正式環境建議外部
   - **Redis** — 叢集內或外部（`redis.host`）？請更改預設密碼（`redis.password`）
   - **MinIO／S3** — 叢集內 MinIO 或原生 S3？在 AWS 上請以 IAM 角色使用 S3
   - **密鑰** — 評估時經 `--set` 傳入；正式環境請用 External Secrets 或 Vault Agent

4. 安裝 chart：

   ```bash
   helm install provisa helm/provisa/ \
     --set config.pgPassword=<password> \
     --set config.adminToken=<token> \
     --set s3.endpoint=https://s3.amazonaws.com \
     --set s3.bucket=my-provisa-results \
     --namespace provisa --create-namespace
   ```

   若使用內部登錄檔，請加上映像覆寫：

   ```bash
   --set image.repository=harbor.internal.example.com/provisa/provisa \
   --set image.tag=1.2.3 \
   --set trino.image.repository=harbor.internal.example.com/trinodb/trino \
   --set trino.image.tag=480
   ```

5. 驗證 pod 正在執行：

   ```bash
   kubectl get pods -n provisa
   ```

6. 檢查 API：

   ```bash
   kubectl port-forward svc/provisa 8000:8000 -n provisa
   curl http://localhost:8000/health
   ```

7. （選用）啟用 ingress 以供外部存取——設定 `ingress.enabled: true` 並設定你的 ingress 控制器

### 先決條件檢查清單

- [ ] Kubernetes 1.26+、Helm 3.12+
- [ ] 支援 `ReadWriteOnce` PVC 的儲存類別（供叢集內具狀態服務使用）
- [ ] 叢集可取得各映像（公用或內部登錄檔）
- [ ] PostgreSQL 端點 + 憑證（若為外部）
- [ ] Redis 端點 + 憑證（若為外部）
- [ ] S3 貯體 + 憑證或 IAM 角色
- [ ] 已選定管理權杖
- [ ] 已設定 ingress 控制器（若需要外部存取）

### 主要值

| 值 | 預設 | 說明 |
| ------- | --------- | ------------- |
| `replicaCount` | `2` | Provisa API 複本（無狀態）（REQ-057） |
| `config.pgHost` | `postgres` | PostgreSQL 主機 |
| `config.pgPassword` | | PostgreSQL 密碼 |
| `config.adminToken` | | 管理 API bearer 權杖 |
| `redis.enabled` | `true` | 部署叢集內 Redis StatefulSet（REQ-371） |
| `redis.host` | `""` | 設定後即使用外部 Redis |
| `redis.port` | `6379` | |
| `redis.password` | `"provisa"` | 請更改此值 |
| `redis.tls` | `false` | |
| `trino.enabled` | `true` | 部署聯邦引擎（REQ-028） |
| `trino.workers` | `2` | 聯邦引擎工作處理程序複本（REQ-056） |
| `postgresql.enabled` | `true` | 部署叢集內 PostgreSQL（REQ-169） |
| `postgresql.host` | `""` | 設定後即使用外部 PostgreSQL |
| `minio.enabled` | `true` | 部署叢集內 MinIO（REQ-029） |
| `s3.endpoint` | | 相容 S3 的端點 URL |
| `s3.bucket` | `provisa-results` | 大型結果重新導向所用的貯體（REQ-029、REQ-137） |
| `ingress.enabled` | `false` | 啟用 ingress |

### 擴展

```bash
kubectl scale deployment/provisa --replicas=5 --namespace provisa
```

聯邦引擎工作處理程序獨立擴展——工作處理程序愈多，輸送量與並行查詢容量愈高（REQ-056）。（REQ-057）

### 更新設定

```bash
kubectl create configmap provisa-config \
  --from-file=config.yaml=./config.yaml \
  --namespace provisa --dry-run=client -o yaml | kubectl apply -f -
kubectl rollout restart deployment/provisa --namespace provisa
```

---

## 高可用性與復原

Provisa 在所有部署模式上套用一套雙層復原模型（REQ-703）：

- **第 1 層——暫時性錯誤。** 讀取操作在遇到暫時性錯誤時，以帶完整抖動的指數退避重試最多 30 秒。以 `PROVISA_RETRY_BUDGET_SECS` 調校該預算。寫入操作絕不在內部重試，且記憶體錯誤絕不可重試。
- **第 2 層——元件故障。** 一個內部引擎監看器會在 2–3 分鐘內偵測並重新啟動故障的軟體元件。

機器層級與叢集層級的故障仍由營運者負責——請佈建冗餘節點與負載平衡器（上文的 Terraform 與 Helm 路徑）以容忍節點遺失。

## 聯邦引擎相依項

倉庫聯邦引擎需要 Provisa 預設安裝之外的 Python 套件與系統層級元件。此處列出的所有 Python 套件都宣告於 `pyproject.toml`，並作為標準 `pip install provisa` 或 `pip install -e .` 的一部分安裝 [tool-verified: `pyproject.toml` lines 44–52]。

這些 Python 套件隨 Provisa 預設安裝一併出貨——任何倉庫引擎都毋須選用附加項。系統層級的項目（ODBC 驅動程式、雲端 CLI、服務帳戶金鑰）必須另行安裝。

### Python 套件（已在核心相依項中）

[tool-verified: `pyproject.toml` lines 41–52]

| 套件 | 引擎 | 用途 |
| ------- | ------ | ------- |
| `databricks-sql-connector` | Databricks | SQL 倉庫連線；Arrow Cloud Fetch（REQ-987） |
| `snowflake-connector-python[pandas]` | Snowflake | 連線 + Arrow 原生 `fetch_arrow_table`（REQ-988） |
| `google-cloud-bigquery` | BigQuery | 查詢執行 |
| `google-cloud-bigquery-storage` | BigQuery | 供 Arrow 原生讀取的 Storage Read API |
| `google-cloud-storage` | BigQuery | 外部表連結所用的 GCS 暫存 |
| `pyodbc` | Fabric、Synapse | 連往 T-SQL 端點的 ODBC 連線 |
| `azure-identity` | Fabric、Synapse | 經 `DefaultAzureCredential` 取得 Azure AD 權杖 |
| `clickhouse-connect` | ClickHouse | HTTP 資料行式讀取 |
| `protobuf>=6.33.5,<7` | BigQuery、gRPC | 相容性釘選——`google-cloud-*` 與 OTel 共用一份 protobuf 執行階段；`<7` 讓它們保持一致 |
| `grpcio-status<1.82` | gRPC | 與 `protobuf<7` 的釘選對齊 |

### 系統層級需求

這些不是 Python 套件——它們必須安裝在執行 Provisa 的主機或容器上。

**Microsoft Fabric 與 Azure Synapse（ODBC）**

`pyodbc` 經 Microsoft ODBC Driver for SQL Server（`msodbcsql18`）連線。該驅動程式必須安裝在主機上——不能經 pip。[tool-verified: `mssql_warehouse_runtime.py` line 84 `"ODBC Driver 18 for SQL Server"` default]

macOS：

```bash
brew install microsoft/mssql-release/msodbcsql18
```

Linux（Ubuntu／Debian）：

```bash
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql18
```

Provisa 會自動取用該驅動程式。若要覆寫驅動程式名稱（供非標準安裝使用），請設定：

```bash
export PROVISA_MSSQL_ODBC_DRIVER="ODBC Driver 17 for SQL Server"
```

**Azure AD 驗證（Fabric 與 Synapse）**

兩個引擎都經 `azure.identity.DefaultAzureCredential` 驗證 [tool-verified: `mssql_warehouse_runtime.py:79`, `fabric_shortcuts.py:46`]。`DefaultAzureCredential` 依序檢查各憑證來源：環境變數、工作負載身份、受管身份、VS Code、`az login` 等。

本機開發時，`az login` 是最簡單的路徑：

```bash
az login
```

正式環境請用受管身份（在 Azure VM 或 AKS 上）——毋須管理憑證。若要用服務主體驗證，請設定：

```bash
export AZURE_TENANT_ID=<tenant>
export AZURE_CLIENT_ID=<app-id>
export AZURE_CLIENT_SECRET=<secret>
```

**BigQuery（服務帳戶）**

`google-cloud-bigquery` 使用應用程式預設憑證。本機開發時，請指向一份服務帳戶金鑰檔：

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa-key.json
```

在 GCP 上的正式環境（Cloud Run、帶 Workload Identity 的 GKE、Compute Engine），程式庫會自動取用所附掛的服務帳戶——毋須環境變數。

該服務帳戶需要：

- `roles/bigquery.dataViewer` — 讀取數據
- `roles/bigquery.jobUser` — 執行查詢
- `roles/bigquery.dataEditor` — 建立外部表（供 ATTACH 使用）
- `roles/storage.objectViewer` — 為外部表讀取 GCS 物件

**Databricks（開發代理環境中的 CA 憑證）**

若 Provisa 執行在會攔截 TLS 的代理（Charles、mitmproxy、企業代理）之後，Databricks SQL 連接器可能會拒絕該代理的憑證。請傳入自訂的 CA 套組：

```bash
export REQUESTS_CA_BUNDLE=/path/to/your/proxy-ca.pem
```

Databricks 連接器從 `requests` 繼承此設定——毋須 Databricks 專屬的環境變數。

### 逐引擎檢查清單

**Databricks**（REQ-987）

- [ ] 已安裝 `databricks-sql-connector`（預設）
- [ ] 帶 `http_path` 的引擎 URL：`databricks://token:TOKEN@workspace.azuredatabricks.net?http_path=/sql/1.0/warehouses/xxx`
- [ ] 個人存取權杖或服務主體權杖
- [ ] 若在會攔截 TLS 的代理之後，已設定 `REQUESTS_CA_BUNDLE`

**Snowflake**（REQ-988）

- [ ] 已安裝 `snowflake-connector-python[pandas]`（預設）
- [ ] 引擎 URL：`snowflake://user:pass@account.snowflakecomputing.com/database`
- [ ] `PROVISA_ENGINE_URL` 或 `federation_hints` 中有 `account`

**BigQuery**（REQ-989）

- [ ] 已安裝 `google-cloud-bigquery`、`google-cloud-bigquery-storage`、`google-cloud-storage`（預設）
- [ ] 已設定 `GOOGLE_APPLICATION_CREDENTIALS`（開發）或已設定工作負載身份（正式環境）
- [ ] 若無法自服務帳戶推斷專案，已設定 `GOOGLE_CLOUD_PROJECT`
- [ ] 服務帳戶具備 BigQuery Data Viewer + Job User 角色

**Microsoft Fabric**（REQ-989）

- [ ] 已安裝 `pyodbc` + `azure-identity`（預設）
- [ ] 已安裝 `msodbcsql18` 系統驅動程式
- [ ] 已設定 `FABRIC_SQL_SERVER` 與 `FABRIC_DATABASE`
- [ ] Azure AD 驗證：`az login`（開發）或受管身份／服務主體（正式環境）
- [ ] 若使用外部物件儲存連結，已設定 `FABRIC_WORKSPACE_ID`

**Azure Synapse**（REQ-989）

- [ ] 與 Fabric 相同的 Python + 系統需求
- [ ] 已設定 `SYNAPSE_SQL_SERVER` 與 `SYNAPSE_DATABASE`
- [ ] 與 Fabric 相同的 Azure AD 驗證設定

**ClickHouse**（REQ-986）

- [ ] 已安裝 `clickhouse-connect`（預設）
- [ ] 引擎 URL：`clickhouse+http://user:pass@host:8123/database`
- [ ] TLS 時在 `federation_hints` 中設 `secure: "true"`（連接埠 8443）

---

## 環境變數

| 變數 | 預設 | 用途 |
| ---------- | --------- | --------- |
| `PG_PASSWORD` | | PostgreSQL 密碼 |
| `PROVISA_CONFIG` | `config/provisa.yaml` | 設定檔路徑（REQ-528） |
| `PROVISA_REDIRECT_ENABLED` | `false` | 啟用大型結果重新導向至 S3（REQ-029、REQ-137） |
| `PROVISA_REDIRECT_THRESHOLD` | `1000` | 觸發重新導向的行數門檻（REQ-029） |
| `PROVISA_REDIRECT_BUCKET` | `provisa-results` | S3 貯體（REQ-029） |
| `PROVISA_REDIRECT_ENDPOINT` | | 相容 S3 的端點 URL（REQ-029） |
| `PROVISA_REDIRECT_TTL` | `3600` | 預簽署 URL 的 TTL（秒）（REQ-141） |
| `REDIS_HOST` | `localhost` | Redis 主機 |
| `REDIS_PORT` | `6379` | Redis 連接埠 |
| `REDIS_PASSWORD` | | Redis 密碼 |
| `REDIS_TLS` | `false` | 為 Redis 啟用 TLS |
| `TRINO_HOST` | `localhost` | Trino 聯邦引擎協調器主機（REQ-028、REQ-054） |
| `TRINO_PORT` | `8080` | Trino 聯邦引擎協調器 HTTP 連接埠（REQ-028、REQ-054） |
| `PROVISA_ENGINE` | `duckdb` | 啟用中的聯邦引擎索引鍵（REQ-989）；覆寫已保存的設定 |
| `PROVISA_ENGINE_URL` | | URL 驅動之引擎（Databricks、Snowflake、ClickHouse、BigQuery、Fabric、Synapse、SQLAlchemy）的連線 URL |
| `PROVISA_MATERIALIZE_URL` | | 具體化儲存區 URL 覆寫；預設為引擎自己的儲存區 |
| `PROVISA_MSSQL_ODBC_DRIVER` | `ODBC Driver 18 for SQL Server` | 供 Fabric／Synapse 使用的 ODBC 驅動程式名稱 |
| `GOOGLE_APPLICATION_CREDENTIALS` | | GCP 服務帳戶金鑰 JSON 的路徑（BigQuery） |
| `GOOGLE_CLOUD_PROJECT` | | GCP 專案 ID（BigQuery；未設定時自服務帳戶推斷） |
| `FABRIC_SQL_SERVER` | | Microsoft Fabric SQL 分析端點主機名稱 |
| `FABRIC_DATABASE` | | Fabric 資料庫名稱 |
| `FABRIC_WORKSPACE_ID` | | Fabric 工作區 GUID（外部物件儲存捷徑必填） |
| `SYNAPSE_SQL_SERVER` | | Azure Synapse 專用 SQL 集區或無伺服器主機名稱 |
| `SYNAPSE_DATABASE` | | Synapse 資料庫名稱 |
| `AZURE_TENANT_ID` | | Azure AD 租用戶（Fabric／Synapse 的服務主體驗證） |
| `AZURE_CLIENT_ID` | | Azure AD 應用程式用戶端 ID |
| `AZURE_CLIENT_SECRET` | | Azure AD 應用程式用戶端密鑰 |
| `REQUESTS_CA_BUNDLE` | | 自訂 CA 套組路徑（Databricks 連接器、開發 TLS 代理） |

---

## CLI 命令

```bash
provisa start              # Start all services
provisa stop               # Stop all services
provisa restart            # Restart
provisa status             # Show service health
provisa open               # Open the UI in the browser
provisa logs               # Tail service logs
provisa export             # Print current config as YAML to stdout
provisa export FILE        # Write current config as YAML to FILE
provisa import FILE        # Replace running config with YAML from FILE
```

（REQ-224、REQ-164）

### 設定晉升工作流程（dev → test → prod）

所有環境專屬的設定（連線字串、密鑰、連接埠）都屬於環境變數或密鑰管理員——不屬於匯出的設定。匯出的 YAML 擷取的是你的語意模型：數據來源、網域、角色、檢視。（REQ-164）

```bash
# On dev — export after making changes in the UI
provisa export > config.yaml
git add config.yaml && git commit -m "chore: update semantic model"
git push

# On test/prod — pull and import
git pull
provisa import config.yaml
```


另見：[環境](environments.md) 說明如何管理具名、結構描述隔離的受治理模型副本。
