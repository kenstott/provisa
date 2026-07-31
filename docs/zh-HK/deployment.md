# 部署

## 選擇部署路徑

Provisa 支援六種部署路徑。請依您的受眾及作業情境選擇：

| 路徑 | 產出物 / 指令碼 | 最適合 |
| ------ | ------------------- | ---------- |
| **開發** | `start-ui.sh` | 從原始碼開發，具完整示範數據的評估 |
| **macOS 安裝程式** | `Provisa-<version>-macOS.dmg` | 開發人員工作站、評估 |
| **Windows 安裝程式** | `Provisa-<version>-windows-x64.exe` | 開發人員工作站、評估 |
| **Linux AppImage** | `Provisa.AppImage` | 內部部署伺服器、雲端 VM、氣隙環境 |
| **雲端 VM（AWS）** | `terraform/deploy.sh` | 具負載平衡器的多節點雲端部署 |
| **Kubernetes** | `helm/provisa/` | 已在營運 K8s 的團隊 |

### VM 與 Kubernetes 之比較

兩者均具企業級品質。VM/AppImage 路徑較為簡單：無須佈建叢集，無須設定 CNI 或 RBAC 政策，且該 AppImage 完全自足（REQ-223）。它可自然融入現有的伺服器管理工具（Ansible、Puppet、Datadog agent、Splunk forwarder 等）。

僅當您的團隊已在營運 K8s 叢集，且希望 Provisa 參與該作業模型（滾動部署、HPA、統一可觀測性）時，才選擇 Kubernetes（REQ-056）。兩者能力相同——Kubernetes 增加的是作業負擔，而非能力。

### 映像檔取得與安全掃描

所有生產路徑均須先取得 Provisa 產出物，才能執行任何部署。「氣隙」指的是目標機器於安裝時所發生之事——產出物必須先行取得。

**macOS 及 Windows 安裝程式：**自 [GitHub 發行頁面](https://github.com/provisa/provisa/releases) 下載。完全內建；下載後無須網際網路（REQ-227）。適用於開發/評估，非生產用途——不預期有映像掃描關卡。

**AppImage 路徑：**自 [GitHub 發行頁面](https://github.com/provisa/provisa/releases) 下載並傳送至目標機器。該 AppImage 將所有元件映像以 tarball 形式封裝於 squashfs 檔案系統內（REQ-294）——多數登記檔掃描器無法就地檢視這些內容。請聯絡您的 Provisa 客戶團隊取得元件映像摘要，以獨立對照您的掃描器進行驗證。

**Terraform 路徑：**執行 `terraform/deploy.sh` 之前，該 AppImage 必須先上傳至 S3。EC2 節點於開機時經由 IAM 角色下載——它們須具備對外的 S3 存取（直接或經由 VPC 閘道端點）。套用與 AppImage 路徑相同的掃描政策。

**Helm / Kubernetes 路徑：**個別映像必須推送至叢集可存取的登記檔。此路徑與登記檔式掃描（Prisma Cloud、Aqua、Trivy、AWS Inspector）最為相容——映像是掃描器原生可理解的第一級物件。氣隙叢集請將映像鏡射至內部登記檔，並於 `values.yaml` 中覆寫參照（REQ-294）。

---

## 開發（自原始碼）

### 建議：`start-ui.sh`

從原始碼執行 Provisa 最簡便的方式。以單一指令啟動所有基礎架構、後端 API 及 UI 開發伺服器（REQ-055）。Ctrl+C 可乾淨地關閉一切。

**先決條件：**Docker Desktop、Node.js、位於 `.venv/` 的 Python 虛擬環境

```bash
./start-ui.sh
```

其執行內容：

- 啟動 `docker-compose.core.yml` + `docker-compose.dev.yml`（所有核心 + 示範服務）並等待健康（REQ-055）
- 以示範數據填入 Kafka
- 從 `.venv/` 同步 Python 相依套件
- 於連接埠 8001 啟動後端 API（日誌寫入 `.logs/server.log`）（REQ-558）
- 於連接埠 3000 啟動 Vite UI 開發伺服器（REQ-559）
- 印出 URL 並等待；Ctrl+C 會停止一切並拆除 compose

```yaml
Backend: http://localhost:8001
UI:      http://localhost:3000
```

**選項：**

`--reset-volumes`——啟動前先執行 `docker compose down -v`，摧毀所有 Docker 資料卷（PostgreSQL 數據、MinIO 物件、Redis 狀態等）（REQ-170）。當您需要完全乾淨的狀態時使用——例如開發期間結構描述變更後，或 Docker 當機並留下毀損的資料卷時。**所有數據都會遺失。**

`--observability`——加入完整的追蹤與指標檢測。下載 OpenTelemetry Java agent 並修補 Trino 的 `jvm.config` 以載入該 agent，為 Provisa 後端加入 OTLP 匯出檢測，並啟動 OTel collector、Prometheus、Tempo 及 Grafana（`http://localhost:3100`）（REQ-330）。`jvm.config` 修補會於 Ctrl+C 時自動還原。

### 手動步驟（僅後端，無 UI）

若您僅需要 API：

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

### 完整堆疊（Provisa 於容器內）

若要將 API 以容器方式而非於主機上執行：

```bash
docker compose -f docker-compose.core.yml -f docker-compose.app.yml up -d
```

### 服務

**核心（`docker-compose.core.yml`）——恆須具備：**

| 服務 | 連接埠 | 用途 |
| --------- | ------ | --------- |
| PostgreSQL | 5432 | 設定中繼資料 + Iceberg 目錄（REQ-169） |
| PgBouncer | 6432 | 連線池（REQ-053） |
| 聯邦引擎 | 8080 | 查詢聯邦（REQ-028） |
| Redis | 6379 | 查詢結果快取（REQ-371） |
| MinIO | 9000/9001 | S3 相容物件儲存（REQ-029、REQ-171） |

**示範（`docker-compose.dev.yml`）——選用，由 `start-ui.sh` 包含：**

| 服務 | 連接埠 | 用途 |
| --------- | ------ | --------- |
| MongoDB | 27017 | 示範用 NoSQL 數據來源 |
| Kafka | 9092 | 示範用串流數據來源 |
| Schema Registry | 8081 | 示範用 Avro/Protobuf 結構描述管理 |
| Debezium | — | 示範用 CDC 連接器 |
| Elasticsearch | 9200 | 示範用搜尋數據來源 |
| Neo4j | 7474/7687 | 示範用圖形數據來源 |
| Fuseki | 3030 | 示範用 SPARQL 三元組存儲 |
| OpenTelemetry Collector | — | 追蹤收集（搭配 `--observability`）（REQ-302） |
| Prometheus | 9090 | 指標（搭配 `--observability`）（REQ-330） |
| Tempo | — | 追蹤儲存（搭配 `--observability`）（REQ-330） |
| Grafana | 3100 | 儀表板（搭配 `--observability`）（REQ-330） |

### 遙測後端（`otlp2sql`）

上述 `--observability` 堆疊（Collector → Tempo/Prometheus/Grafana）是其中一條遙測路徑。另一條是 `otlp2sql`（`provisa.observability.otlp2sql`）：一個 OTLP/HTTP 接收端，將追蹤、指標及日誌寫入由 SQLAlchemy URL 所選定的 SQL 資料庫，並於擷取時提取 `provisa.*` span 屬性，因此無須另行執行壓縮作業。寫入會被批次處理（`OTLP2SQL_BATCH_MAX_ROWS`，預設 1000；`OTLP2SQL_BATCH_MAX_SECS`，預設 2 秒）。

遙測擁有自己獨立的儲存區，與控制平面資料庫分離。以 `PROVISA_OPS_DB_URL` 選擇後端：

| `PROVISA_OPS_DB_URL` | 後端 | 備註 |
| --- | --- | --- |
| *（未設定）* | 位於 `~/.provisa/telemetry/` 的專屬 DuckDB | 預設；無伺服器，無 Docker |
| `clickhouse+native://user@host/otel` | ClickHouse | 高速率擷取，具自動背景合併 |
| `postgresql+psycopg2://user@host/otel` | PostgreSQL | 中等流量 |
| `trino://user@host:8080/otel` | Trino / Iceberg | 技術上可行，**不建議**——見下文 |

**關於 `trino://`：**SQLAlchemy 的 Trino 方言會產生有效的 Trino DDL 及 `INSERT`，因此技術上可作為 `otlp2sql` 後端。除低擷取速率外並不建議使用。每次批次刷新都會成為一次分散式 Trino `INSERT` 加上一次 Iceberg 快照，因此高速率遙測會產生大量小型檔案及快照，且仍需定期執行 `ALTER TABLE ... EXECUTE optimize` / `expire_snapshots`——而 `otlp2sql` 並不會執行此類作業。此外也會使查詢引擎位於擷取熱路徑上。

對於進入 Trino/Iceberg 的高量遙測，請改用 `otlp2parquet`：它會將 parquet 直接寫入物件儲存而不經過 Trino，並由排程的 Trino 壓縮作業將原始檔案捲入現有的 Iceberg 資料表。若需要單一引擎同時處理高速率擷取與壓縮，則以 ClickHouse 為優先。

請將應用程式及 Trino 的 OTLP 匯出器（`OTEL_EXPORTER_OTLP_ENDPOINT`）指向 `otlp2sql` 端點，並將 ops 領域註冊至相同的 `PROVISA_OPS_DB_URL`，使其可讀取接收端所寫入的內容。

---

## macOS 安裝程式

適用於開發人員工作站及評估。完全氣隙——下載後無須網際網路（REQ-227）。

基礎安裝程式是**原生安裝**：DuckDB 聯邦引擎 + SQLite 控制平面 + 記憶體內（fakeredis）快取，不含 Docker、VM、Trino、Redis 或 MinIO（REQ-972、REQ-979）。聯邦引擎為精靈選項——DuckDB（原生，預設）、Trino-on-Docker，或外部引擎（REQ-973）。可觀測性恆為啟用中的自我遙測，可於 Admin 檢視；Docker collector/Prometheus/Grafana 堆疊為選用的外部示範，而非開關切換（REQ-975）。示範數據包為選用，預設關閉（REQ-978）。Trino、Docker 可觀測性堆疊及示範為重量級附加元件，以本機優先方式解析（安裝程式相鄰目錄、已掛載資料卷、`~/Downloads`，其次為 GitHub 發行版本），使企業可為氣隙安裝預先佈置 tarball（REQ-977）。

### 步驟

1. 自 [GitHub 發行頁面](https://github.com/provisa/provisa/releases) 下載 `Provisa-<version>-macOS.dmg`
2. 開啟該 DMG，並將 **Provisa.app** 拖曳至 `/Applications`
3. 雙擊 **Provisa.app**——首次啟動設定僅執行一次；精靈會提供上述引擎、可觀測性及示範選項（REQ-1007）
4. 開啟終端機：

   ```bash
   provisa start    # start all services
   provisa status   # confirm all services are running
   provisa open     # open the UI in the browser
   ```

   （REQ-224）

### 數據持久性

所有數據均儲存於 `~/.provisa/`（REQ-224）。如需移除全部內容：`provisa uninstall`。

---

## Windows 安裝程式

適用於開發人員工作站及評估。完全氣隙——下載後無須網際網路（REQ-227）。

與 macOS 相同，基礎版 Windows 安裝程式是**原生層**：獨立的 Python 執行階段 + provisa wheel + DuckDB/pg_duckdb + SQLite 控制平面，不含 Docker、不含 VM、不含容器映像（REQ-979）。聯邦引擎（Trino）、可觀測性堆疊及示範數據包，均透過個別分層安裝程式後續加入，順序為：容器安裝程式（`Provisa-Container-<version>.exe`，加入 WSL2 + containerd + Trino）、Obs 安裝程式（需要容器層）、Demo 安裝程式（需要 Core + Obs）。首次啟動指引會說明如何透過執行容器安裝程式來初始化聯邦引擎（REQ-1005）。

### 步驟

1. 自 [GitHub 發行頁面](https://github.com/provisa/provisa/releases) 下載 `Provisa-<version>-windows-x64.exe`
2. 執行該安裝程式——無須系統管理員權限；會安裝至 `%LOCALAPPDATA%\Programs\Provisa\`
3. 自開始功能表開啟 **Provisa First Launch**——原生設定僅執行一次，並印出分層附加元件的後續步驟指引（REQ-1005）
4. 開啟新終端機：

   ```text
   provisa status
   provisa open
   ```

   （REQ-224）

### 數據持久性

所有數據均儲存於 `%USERPROFILE%\.provisa\`。

---

## Linux AppImage——單節點或多節點 VM

### 這是甚麼

`Provisa.AppImage` 是一個單一自足的可執行檔，封裝了以下內容（REQ-223、REQ-228）：

- 一個無須 root 的 Docker daemon（`dockerd-rootless.sh` + `rootlesskit`）——無須系統 Docker 或 root 權限
- 所有容器映像的 tarball（PostgreSQL、PgBouncer、MinIO、Redis、聯邦引擎、Provisa API）（REQ-294）
- Provisa CLI 包裝程式及首次啟動設定指令碼

Provisa 映像於封裝時即已預先建置——Python 原始碼絕不包含在內。

### 何時使用

- 內部部署裸機或 VM（單節點或多節點）
- 無 K8s 叢集的雲端 VM
- 氣隙環境（REQ-294）
- 需要比 Kubernetes 更簡單的作業方式時

---

### 步驟——單節點

1. 自 [GitHub 發行頁面](https://github.com/provisa/provisa/releases) 下載 `Provisa.AppImage` 並傳送至目標機器
2. 使其可執行：

   ```bash
   chmod +x Provisa.AppImage
   ```

3. 執行首次啟動設定：

   ```bash
   ./Provisa.AppImage
   ```

4. 設定精靈會詢問：
   - **角色** → 選擇 `primary`
   - **RAM 預算** → 要配置的 RAM 數量（0 = 全部可用）；決定 Trino worker 數目
   - **主機名稱** → 此節點所廣播的位址
   - **API 連接埠** → 預設 `8000`（REQ-560）
5. 設定會載入所有容器映像（約 2–5 分鐘）、寫入設定並啟動服務
6. 驗證：

   ```bash
   provisa status
   curl http://localhost:8000/health
   ```

---

### 步驟——多節點（主節點）

請先於主節點上執行以下步驟。次要節點必須於主節點運行後才可設定。

1. 下載並傳送 `Provisa.AppImage` 至主機
2. 開啟所需的防火牆連接埠（次要節點會經由這些連接埠建立連入連線）：

   | 連接埠 | 服務 |
   | ------ | --------- |
   | 5432 | PostgreSQL |
   | 6379 | Redis |
   | 9000 | MinIO |
   | 8080 | 聯邦引擎協調器 |
   | 8000 | Provisa API |

3. 使其可執行並執行：

   ```bash
   chmod +x Provisa.AppImage
   ./Provisa.AppImage
   ```

4. 設定精靈會詢問：
   - **角色** → 選擇 `primary`
   - **RAM 預算**、**主機名稱**、**API 連接埠** → 與單節點相同回答
5. 設定完成後，請記下此機器的**私有 IP**——次要節點需要此資訊
6. 精靈會印出一個 nginx upstream 區塊——請保留供負載平衡器設定使用
7. 驗證：

   ```bash
   provisa status
   curl http://localhost:8000/health
   ```

---

### 步驟——多節點（各次要節點）

主節點運行且可連線後，於每個額外節點上重複下列步驟。

1. 下載並傳送 `Provisa.AppImage` 至次要機器
2. 確認次要節點可連線至主節點：

   ```bash
   curl http://<primary-ip>:8000/health
   ```

3. 使其可執行並執行：

   ```bash
   chmod +x Provisa.AppImage
   ./Provisa.AppImage
   ```

4. 設定精靈會詢問：
   - **角色** → 選擇 `secondary`
   - **主節點 IP** → 輸入主節點的 IP（會即時驗證連線狀況）
   - **RAM 預算**、**主機名稱**、**API 連接埠** → 如上回答
5. 設定會載入精簡的映像組合（不含 PostgreSQL、PgBouncer、MinIO、Redis——這些僅於主節點執行）（REQ-561），並啟動 Provisa API 及一個聯邦引擎 worker
6. 驗證：

   ```bash
   provisa status
   curl http://localhost:8000/health
   ```

7. 將此節點加入您的負載平衡器 upstream

---

### 主／次拓撲

**主節點**執行所有單例服務：

| 服務 | 為何為單例 |
| --------- | --------------- |
| PostgreSQL | 共用結構描述、應用程式設定、語意模型 |
| Redis | 共用的查詢結果快取及訂閱狀態（REQ-371） |
| MinIO | 供重導向結果及 MV 快照使用的共用物件儲存（REQ-029） |
| 聯邦引擎協調器 | 所有 worker（主節點 + 次要節點）均於此登記（REQ-028） |

**次要節點**僅執行：

- Provisa API——無狀態；啟動時自主節點的 PostgreSQL 讀取所有設定（REQ-057、REQ-562）
- 聯邦引擎 worker——自主向主節點的協調器登記（REQ-028）

所有應用程式狀態均流經主節點的 PostgreSQL。無須手動同步。（REQ-562）

---

### 非互動式（自動化）首次啟動

供 Terraform、cloud-init 或 Ansible 使用——以旗標取代回答提示：

```bash
# Primary
./Provisa.AppImage --non-interactive --role primary --ram-gb 32

# Secondary
./Provisa.AppImage --non-interactive --role secondary --primary-ip 10.0.0.10 --ram-gb 32
```

非互動模式會安裝一個 systemd 單元（`/etc/systemd/system/provisa.service`）以於開機時啟動。（REQ-563）

| 旗標 | 描述 |
| ------ | ------------- |
| `--non-interactive` | 跳過所有提示；安裝 systemd 單元 |
| `--role primary\|secondary` | 節點角色 |
| `--primary-ip <ip>` | 主節點 IP（次要節點須提供） |
| `--ram-gb <n>` | 要配置的 RAM（0 = 全部可用） |

---

## 雲端 VM 部署——Terraform（AWS）

以單一互動式指令，於 AWS 上佈建完整的多節點 Provisa 叢集——VPC、安全群組、EC2 執行個體、ALB、NLB。（REQ-564）

### 檔案

| 檔案 | 用途 |
| ------ | --------- |
| `terraform/deploy.sh` | 互動式包裝程式——收集參數、驗證憑證、寫入 `terraform.tfvars`、執行 apply |
| `terraform/aws/variables.tf` | 所有變數定義及其預設值 |
| `terraform/aws/main.tf` | VPC、子網路、安全群組、IAM、EC2、ALB、NLB |
| `terraform/aws/outputs.tf` | 端點 URL 及節點 IP |

### 步驟

1. 自 [GitHub 發行頁面](https://github.com/provisa/provisa/releases) 下載 `Provisa.AppImage`

2. 上傳至您 AWS 帳戶中的 S3 儲存桶：

   ```bash
   aws s3 cp Provisa.AppImage s3://<your-bucket>/releases/Provisa.AppImage
   ```

3. 確保您的 shell 中具備 AWS 憑證（任一方式即可）：
   - 環境變數：`AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY`
   - 具名設定檔：`export AWS_PROFILE=my-profile`
   - 有效的 SSO 工作階段：`aws sso login`

4.（選用）若需 SSH 存取節點，請於目標區域建立一組 EC2 金鑰對，並記下該金鑰對名稱

5. 執行部署包裝程式：

   ```bash
   bash terraform/deploy.sh
   ```

6. 回答精靈問題（見下方參考表）。指令碼會於進行前先驗證該 AppImage 是否存在於 S3；若不存在則中止

7. 檢視部署摘要並確認

8. Terraform 佈建所有基礎架構（約 5–10 分鐘）。apply 完成後，指令碼會印出：

   ```text
   api_endpoint      = "http://<alb-dns>:8000"
   flight_endpoint   = "<nlb-dns>:8815"
   primary_ip        = "10.0.x.x"
   secondary_ips     = ["10.0.x.x", ...]
   ```

   （REQ-564、REQ-143）

9.（選用）將 DNS 記錄指向 ALB 及 NLB 的 DNS 名稱

10. 驗證：

    ```bash
    curl http://<api_endpoint>/health
    ```

### 精靈問題

| 問題 | 預設值 | 備註 |
| ---------- | --------- | ------- |
| 雲端供應商 | — | 目前僅支援 AWS |
| AWS 憑證 | — | 先檢查是否有現有的工作階段 |
| 區域 | `us-east-1` | |
| 節點數量 | `2` | 1 = 僅主節點，無 LB；2+ = 主節點 + 次要節點 + ALB/NLB |
| 執行個體型別 | `m7i.2xlarge` | 見下方的容量規劃指南 |
| 根磁碟大小 | `100 GB` | 每節點 |
| RAM 預算 | `0`（全部 RAM） | 決定每節點的 Trino worker 數目 |
| S3 儲存桶 | — | 進行前即時驗證 |
| S3 金鑰 | `releases/Provisa.AppImage` | |
| SSH 存取 | 否 | 需要現有金鑰對名稱 + 管理員 CIDR |
| VPC CIDR | `10.0.0.0/16` | |

### 執行個體容量規劃指南

| 型別 | vCPU | RAM | 每節點 Trino worker | 使用場景 |
| ------ | ------ | ----- | -------------------- | ---------- |
| `m7i.xlarge` | 4 | 16 GB | 0 | 開發／小型數據集 |
| `m7i.2xlarge` | 8 | 32 GB | 1 | 小型生產環境 |
| `m7i.4xlarge` | 16 | 64 GB | 2 | 中型生產環境 |
| `m7i.8xlarge` | 32 | 128 GB | 4 | 大型生產環境 |

所有節點皆將 worker 貢獻至主節點上的一個協調器（REQ-028）。3 節點的 `m7i.4xlarge` 叢集共會產生 6 個 Trino worker。

### 佈建內容

- 橫跨兩個可用區的兩個公有子網路所組成的 VPC（REQ-564）
- 安全群組：LB 群組（8000/8815 上的公有連入）、節點群組（LB → 節點、叢集內部、選用 SSH）
- 具 S3 GetObject 權限（於 AppImage 儲存桶上）的 IAM 角色及執行個體設定檔
- 主節點 EC2 執行個體——以 `--non-interactive --role primary` 模式執行首次啟動
- 次要節點 EC2 執行個體（node_count − 1 個）——以 `--non-interactive --role secondary --primary-ip <primary private IP>` 模式執行首次啟動；相依於主節點先行完成
- 連接埠 8000 上的 ALB——HTTP API，健康檢查 `/health`（REQ-560）
- 連接埠 8815 上的 NLB——Arrow Flight / gRPC（REQ-143）
- 兩個 LB 均連接至所有節點

### 先決條件檢核清單

- [ ] IAM 權限：完整 EC2、完整 ELB、完整 VPC、IAM 角色建立、AppImage 儲存桶上的 S3 GetObject
- [ ] `Provisa.AppImage` 已上傳至 S3
- [ ] EC2 節點具備對外 S3 存取（直接連上網際網路，或經由 S3 VPC 閘道端點）
- [ ] 目標區域已存在 EC2 金鑰對（若需要 SSH）
- [ ] 本機已安裝 Terraform ≥ 1.5
- [ ] 已規劃供 ALB / NLB 使用的 DNS 記錄（選用，但建議）
- [ ] 若需要 HTTPS，已備妥 ACM 憑證（基礎 Terraform 中不含此項）

### 密鑰

Terraform 中不內嵌任何密鑰。該 AppImage 會於首次啟動時產生憑證，並將其寫入各節點上的 `~/.provisa/config.yaml`（REQ-563）。生產環境請於部署後自主節點取得管理員權杖：

```bash
ssh ubuntu@<primary-public-ip> cat ~/.provisa/config.yaml | grep admin_token
```

---

## Kubernetes / Helm

### 何時使用

您的團隊已在營運 Kubernetes 叢集，並希望 Provisa 參與該作業模型（REQ-056）。若您正在評估 Provisa，或在無現有叢集的情況下進行內部部署，AppImage 路徑較為簡單。

注意：Provisa 的 AppImage 無法於 Kubernetes pod 內執行——它需要 FUSE 及一個無須 root 的 Docker daemon，而這些在標準 pod 安全性設定檔中並不可用。

### 步驟

1. 確認叢集存取：

   ```bash
   kubectl cluster-info
   ```

2. 將映像拉取並鏡射至您的內部登記檔（氣隙或有掃描要求的環境須執行；若直接自公有登記檔拉取則可略過）（REQ-294）：

   | 映像 | 用途 |
   | ------- | ---------- |
   | `provisa/provisa:<version>` | Provisa API |
   | `trinodb/trino:480` | 聯邦引擎協調器 + worker（REQ-169） |
   | `postgres:16` | 叢集內 PostgreSQL（若啟用 `postgresql.enabled`）（REQ-169） |
   | `edoburu/pgbouncer:latest` | 叢集內 PgBouncer（若啟用 `pgbouncer.enabled`）（REQ-053） |
   | `redis:7.2` | 叢集內 Redis（若啟用 `redis.enabled` 且無 `redis.host`）（REQ-371） |
   | `minio/minio:latest` | 叢集內 MinIO（若啟用 `minio.enabled`）（REQ-029） |

   針對有登記檔掃描要求的環境：
   - 將各映像推送至您的暫存登記檔
   - 執行您的掃描器（Prisma Cloud、Aqua、Trivy、AWS Inspector）並取得核准
   - 提升至您的生產內部登記檔

3. 安裝前須先決定：
   - **PostgreSQL**——叢集內（`postgresql.enabled: true`）或外部受管（`postgresql.host`）？生產環境建議使用外部
   - **Redis**——叢集內或外部（`redis.host`）？請變更預設密碼（`redis.password`）
   - **MinIO / S3**——叢集內 MinIO 或原生 S3？AWS 環境請搭配 IAM 角色使用 S3
   - **密鑰**——評估時可經 `--set` 傳入；生產環境請使用 External Secrets 或 Vault Agent

4. 安裝該 chart：

   ```bash
   helm install provisa helm/provisa/ \
     --set config.pgPassword=<password> \
     --set config.adminToken=<token> \
     --set s3.endpoint=https://s3.amazonaws.com \
     --set s3.bucket=my-provisa-results \
     --namespace provisa --create-namespace
   ```

   若使用內部登記檔，請加入映像覆寫：

   ```bash
   --set image.repository=harbor.internal.example.com/provisa/provisa \
   --set image.tag=1.2.3 \
   --set trino.image.repository=harbor.internal.example.com/trinodb/trino \
   --set trino.image.tag=480
   ```

5. 驗證 pod 是否運行中：

   ```bash
   kubectl get pods -n provisa
   ```

6. 檢查 API：

   ```bash
   kubectl port-forward svc/provisa 8000:8000 -n provisa
   curl http://localhost:8000/health
   ```

7.（選用）啟用 ingress 以供外部存取——設定 `ingress.enabled: true` 並設定您的 ingress 控制器

### 先決條件檢核清單

- [ ] Kubernetes 1.26+、Helm 3.12+
- [ ] 支援 `ReadWriteOnce` PVC 的儲存類別（供叢集內有狀態服務使用）
- [ ] 叢集可存取的映像（公有或內部登記檔）
- [ ] PostgreSQL 端點 + 憑證（若為外部）
- [ ] Redis 端點 + 憑證（若為外部）
- [ ] S3 儲存桶 + 憑證或 IAM 角色
- [ ] 已選定管理員權杖
- [ ] 已設定 ingress 控制器（若需要外部存取）

### 主要設定值

| 值 | 預設值 | 描述 |
| ------- | --------- | ------------- |
| `replicaCount` | `2` | Provisa API 副本數（無狀態）（REQ-057） |
| `config.pgHost` | `postgres` | PostgreSQL 主機 |
| `config.pgPassword` | | PostgreSQL 密碼 |
| `config.adminToken` | | 管理員 API 持有者權杖 |
| `redis.enabled` | `true` | 部署叢集內 Redis StatefulSet（REQ-371） |
| `redis.host` | `""` | 設定以使用外部 Redis |
| `redis.port` | `6379` | |
| `redis.password` | `"provisa"` | 請變更此值 |
| `redis.tls` | `false` | |
| `trino.enabled` | `true` | 部署聯邦引擎（REQ-028） |
| `trino.workers` | `2` | 聯邦引擎 worker 副本數（REQ-056） |
| `postgresql.enabled` | `true` | 部署叢集內 PostgreSQL（REQ-169） |
| `postgresql.host` | `""` | 設定以使用外部 PostgreSQL |
| `minio.enabled` | `true` | 部署叢集內 MinIO（REQ-029） |
| `s3.endpoint` | | S3 相容端點 URL |
| `s3.bucket` | `provisa-results` | 供大型結果重導向使用的儲存桶（REQ-029、REQ-137） |
| `ingress.enabled` | `false` | 啟用 ingress |

### 擴縮

```bash
kubectl scale deployment/provisa --replicas=5 --namespace provisa
```

聯邦引擎 worker 可獨立擴縮——更多 worker 可提升輸送量及並行查詢容量（REQ-056）。（REQ-057）

### 更新設定

```bash
kubectl create configmap provisa-config \
  --from-file=config.yaml=./config.yaml \
  --namespace provisa --dry-run=client -o yaml | kubectl apply -f -
kubectl rollout restart deployment/provisa --namespace provisa
```

---

## 高可用性與復原

Provisa 於所有部署模式中均套用一套兩層復原模型（REQ-703）：

- **第一層——暫時性錯誤。**讀取作業於遇到暫時性錯誤時，會以全抖動（full jitter）指數退避方式重試最多 30 秒。可透過 `PROVISA_RETRY_BUDGET_SECS` 調整此預算。寫入作業從不於內部重試，記憶體錯誤亦絕不可重試。
- **第二層——元件失效。**內部引擎監看程式會偵測並於 2–3 分鐘內重新啟動失效的軟體元件。

機器層級及叢集層級的失效仍為操作人員的責任——請佈建備援節點及負載平衡器（見上方 Terraform 及 Helm 路徑）以達成節點遺失容忍能力。

## 聯邦引擎相依性

數據倉庫聯邦引擎需要 Provisa 預設安裝之外的 Python 套件及系統層級元件。此處所列所有 Python 套件均宣告於 `pyproject.toml`，並隨標準的 `pip install provisa` 或 `pip install -e .` 一併安裝 [tool-verified: `pyproject.toml` lines 44–52]。

這些 Python 套件隨 Provisa 預設安裝一併提供——任何數據倉庫引擎均無須額外的選用擴充套件。系統層級項目（ODBC 驅動程式、雲端 CLI、服務帳戶金鑰）須另行安裝。

### Python 套件（已列於核心相依套件中）

[tool-verified: `pyproject.toml` lines 41–52]

| 套件 | 引擎 | 用途 |
| ------- | ------ | ------- |
| `databricks-sql-connector` | Databricks | SQL 倉庫連線；Arrow Cloud Fetch（REQ-987） |
| `snowflake-connector-python[pandas]` | Snowflake | 連線 + Arrow 原生的 `fetch_arrow_table`（REQ-988） |
| `google-cloud-bigquery` | BigQuery | 查詢執行 |
| `google-cloud-bigquery-storage` | BigQuery | 供 Arrow 原生讀取用的 Storage Read API |
| `google-cloud-storage` | BigQuery | 供外部資料表連結使用的 GCS 暫存 |
| `pyodbc` | Fabric、Synapse | 連往 T-SQL 端點的 ODBC 連線 |
| `azure-identity` | Fabric、Synapse | 經 `DefaultAzureCredential` 取得的 Azure AD 權杖 |
| `clickhouse-connect` | ClickHouse | HTTP 欄式讀取 |
| `protobuf>=6.33.5,<7` | BigQuery、gRPC | 相容性鎖定版本——`google-cloud-*` 與 OTel 共用同一 protobuf 執行階段；`<7` 使兩者保持一致 |
| `grpcio-status<1.82` | gRPC | 與 `protobuf<7` 鎖定版本保持一致 |

### 系統層級需求

以下並非 Python 套件——必須安裝於執行 Provisa 的主機或容器上。

**Microsoft Fabric 及 Azure Synapse（ODBC）**

`pyodbc` 經 Microsoft ODBC Driver for SQL Server（`msodbcsql18`）連線。該驅動程式必須安裝於主機上——而非經由 pip 安裝。[tool-verified: `mssql_warehouse_runtime.py` line 84 `"ODBC Driver 18 for SQL Server"` default]

macOS：

```bash
brew install microsoft/mssql-release/msodbcsql18
```

Linux（Ubuntu/Debian）：

```bash
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql18
```

Provisa 會自動偵測該驅動程式。若要覆寫驅動程式名稱（供非標準安裝使用），請設定：

```bash
export PROVISA_MSSQL_ODBC_DRIVER="ODBC Driver 17 for SQL Server"
```

**Azure AD 驗證（Fabric 及 Synapse）**

兩個引擎均經由 `azure.identity.DefaultAzureCredential` 進行驗證 [tool-verified: `mssql_warehouse_runtime.py:79`, `fabric_shortcuts.py:46`]。`DefaultAzureCredential` 會依序檢查憑證來源：環境變數、workload identity、managed identity、VS Code、`az login` 及其他方式。

供本機開發使用，`az login` 為最簡便的路徑：

```bash
az login
```

生產環境請使用 managed identity（於 Azure VM 或 AKS 上）——無須管理憑證。若採服務主體驗證，請設定：

```bash
export AZURE_TENANT_ID=<tenant>
export AZURE_CLIENT_ID=<app-id>
export AZURE_CLIENT_SECRET=<secret>
```

**BigQuery（服務帳戶）**

`google-cloud-bigquery` 使用 Application Default Credentials。供本機開發使用，請指向一個服務帳戶金鑰檔：

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa-key.json
```

於 GCP 生產環境（Cloud Run、具 Workload Identity 的 GKE、Compute Engine）中，該程式庫會自動偵測附掛的服務帳戶——無須設定環境變數。

該服務帳戶需要：

- `roles/bigquery.dataViewer`——讀取數據
- `roles/bigquery.jobUser`——執行查詢
- `roles/bigquery.dataEditor`——建立外部資料表（供 ATTACH 使用）
- `roles/storage.objectViewer`——讀取供外部資料表使用的 GCS 物件

**Databricks（開發代理環境中的 CA 憑證）**

若 Provisa 運行於會攔截 TLS 的代理伺服器後方（Charles、mitmproxy、企業代理），Databricks SQL connector 可能會拒絕該代理伺服器的憑證。請傳入自訂 CA 套件：

```bash
export REQUESTS_CA_BUNDLE=/path/to/your/proxy-ca.pem
```

Databricks connector 會自 `requests` 繼承此設定——無須 Databricks 專屬的環境變數。

### 各引擎檢核清單

**Databricks**（REQ-987）

- [ ] 已安裝 `databricks-sql-connector`（預設）
- [ ] 具 `http_path` 的引擎 URL：`databricks://token:TOKEN@workspace.azuredatabricks.net?http_path=/sql/1.0/warehouses/xxx`
- [ ] 個人存取權杖或服務主體權杖
- [ ] 若位於會攔截 TLS 的代理伺服器後方，已設定 `REQUESTS_CA_BUNDLE`

**Snowflake**（REQ-988）

- [ ] 已安裝 `snowflake-connector-python[pandas]`（預設）
- [ ] 引擎 URL：`snowflake://user:pass@account.snowflakecomputing.com/database`
- [ ] `PROVISA_ENGINE_URL` 或 `federation_hints` 中含有 `account`

**BigQuery**（REQ-989）

- [ ] 已安裝 `google-cloud-bigquery`、`google-cloud-bigquery-storage`、`google-cloud-storage`（預設）
- [ ] 已設定 `GOOGLE_APPLICATION_CREDENTIALS`（開發）或已設定 workload identity（生產）
- [ ] 若無法自服務帳戶推斷專案，已設定 `GOOGLE_CLOUD_PROJECT`
- [ ] 服務帳戶具備 BigQuery Data Viewer + Job User 角色

**Microsoft Fabric**（REQ-989）

- [ ] 已安裝 `pyodbc` + `azure-identity`（預設）
- [ ] 已安裝 `msodbcsql18` 系統驅動程式
- [ ] 已設定 `FABRIC_SQL_SERVER` 及 `FABRIC_DATABASE`
- [ ] Azure AD 驗證：`az login`（開發）或 managed identity / 服務主體（生產）
- [ ] 若使用外部物件儲存連結，已設定 `FABRIC_WORKSPACE_ID`

**Azure Synapse**（REQ-989）

- [ ] 與 Fabric 相同的 Python + 系統需求
- [ ] 已設定 `SYNAPSE_SQL_SERVER` 及 `SYNAPSE_DATABASE`
- [ ] 與 Fabric 相同的 Azure AD 驗證設定

**ClickHouse**（REQ-986）

- [ ] 已安裝 `clickhouse-connect`（預設）
- [ ] 引擎 URL：`clickhouse+http://user:pass@host:8123/database`
- [ ] `federation_hints` 中設定 `secure: "true"` 以啟用 TLS（連接埠 8443）

---

## 環境變數

| 變數 | 預設值 | 用途 |
| ---------- | --------- | --------- |
| `PG_PASSWORD` | | PostgreSQL 密碼 |
| `PROVISA_CONFIG` | `config/provisa.yaml` | 設定檔路徑（REQ-528） |
| `PROVISA_REDIRECT_ENABLED` | `false` | 啟用大型結果重導向至 S3（REQ-029、REQ-137） |
| `PROVISA_REDIRECT_THRESHOLD` | `1000` | 觸發重導向的列數門檻（REQ-029） |
| `PROVISA_REDIRECT_BUCKET` | `provisa-results` | S3 儲存桶（REQ-029） |
| `PROVISA_REDIRECT_ENDPOINT` | | S3 相容端點 URL（REQ-029） |
| `PROVISA_REDIRECT_TTL` | `3600` | 預簽署 URL 的 TTL（秒）（REQ-141） |
| `REDIS_HOST` | `localhost` | Redis 主機 |
| `REDIS_PORT` | `6379` | Redis 連接埠 |
| `REDIS_PASSWORD` | | Redis 密碼 |
| `REDIS_TLS` | `false` | 為 Redis 啟用 TLS |
| `TRINO_HOST` | `localhost` | Trino 聯邦引擎協調器主機（REQ-028、REQ-054） |
| `TRINO_PORT` | `8080` | Trino 聯邦引擎協調器 HTTP 連接埠（REQ-028、REQ-054） |
| `PROVISA_ENGINE` | `duckdb` | 使用中的聯邦引擎鍵值（REQ-989）；覆寫已持久化的設定 |
| `PROVISA_ENGINE_URL` | | 供 URL 驅動引擎（Databricks、Snowflake、ClickHouse、BigQuery、Fabric、Synapse、SQLAlchemy）使用的連線 URL |
| `PROVISA_MATERIALIZE_URL` | | 具體化儲存區 URL 覆寫；預設為引擎自身的儲存區 |
| `PROVISA_MSSQL_ODBC_DRIVER` | `ODBC Driver 18 for SQL Server` | 供 Fabric / Synapse 使用的 ODBC 驅動程式名稱 |
| `GOOGLE_APPLICATION_CREDENTIALS` | | GCP 服務帳戶金鑰 JSON 的路徑（BigQuery） |
| `GOOGLE_CLOUD_PROJECT` | | GCP 專案 ID（BigQuery；未設定時自服務帳戶推斷） |
| `FABRIC_SQL_SERVER` | | Microsoft Fabric SQL 分析端點主機名稱 |
| `FABRIC_DATABASE` | | Fabric 資料庫名稱 |
| `FABRIC_WORKSPACE_ID` | | Fabric 工作區 GUID（外部物件儲存捷徑須提供） |
| `SYNAPSE_SQL_SERVER` | | Azure Synapse 專用 SQL 集區或無伺服器主機名稱 |
| `SYNAPSE_DATABASE` | | Synapse 資料庫名稱 |
| `AZURE_TENANT_ID` | | Azure AD 租用戶（供 Fabric/Synapse 服務主體驗證使用） |
| `AZURE_CLIENT_ID` | | Azure AD 應用程式客戶端 ID |
| `AZURE_CLIENT_SECRET` | | Azure AD 應用程式客戶端密鑰 |
| `REQUESTS_CA_BUNDLE` | | 自訂 CA 套件路徑（Databricks connector、開發用 TLS 代理） |

---

## CLI 指令

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

### 設定提升工作流程（開發 → 測試 → 生產）

所有環境專屬設定（連線字串、密鑰、連接埠）均應置於環境變數或密鑰管理器中——而非於匯出的設定內。匯出的 YAML 擷取的是您的語意模型：數據來源、領域、角色、檢視。（REQ-164）

```bash
# On dev — export after making changes in the UI
provisa export > config.yaml
git add config.yaml && git commit -m "chore: update semantic model"
git push

# On test/prod — pull and import
git pull
provisa import config.yaml
```
