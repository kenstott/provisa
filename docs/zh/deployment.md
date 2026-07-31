# 部署

## 选择部署路径

Provisa 支持六种部署路径。请根据受众和运维场景进行选择：

| 路径 | 制品/脚本 | 最适合 |
| ------ | ------------------- | ---------- |
| **开发** | `start-ui.sh` | 源码开发、使用完整演示数据进行评估 |
| **macOS 安装程序** | `Provisa-<version>-macOS.dmg` | 开发者工作站、评估 |
| **Windows 安装程序** | `Provisa-<version>-windows-x64.exe` | 开发者工作站、评估 |
| **Linux AppImage** | `Provisa.AppImage` | 本地服务器、云虚拟机、气隙环境 |
| **云虚拟机（AWS）** | `terraform/deploy.sh` | 带负载均衡器的多节点云部署 |
| **Kubernetes** | `helm/provisa/` | 已在运行 K8s 的团队 |

### 虚拟机 vs Kubernetes

两者均为企业级方案。虚拟机/AppImage 路径更简单：无需预配集群，无需配置 CNI 或 RBAC 策略，AppImage 本身完全自包含（REQ-223）。它可以自然地融入现有的服务器管理工具链（Ansible、Puppet、Datadog agent、Splunk forwarder 等）。

只有当团队已在运行 K8s 集群、并希望 Provisa 参与该运维模型（滚动部署、HPA、统一可观测性）时才选择 Kubernetes（REQ-056）。两者能力等价——Kubernetes 增加的是运维开销，而非能力。

### 镜像获取与安全扫描

所有生产路径都要求先获取 Provisa 制品，之后才能进行部署。“气隙”指的是目标机器上安装时发生的事情——制品必须先被获取。

**macOS 和 Windows 安装程序：** 从 [GitHub releases 页面](https://github.com/provisa/provisa/releases)下载。完全捆绑；下载后无需联网（REQ-227）。仅供开发/评估使用，不用于生产——不预期有镜像扫描门禁。

**AppImage 路径：** 从 [GitHub releases 页面](https://github.com/provisa/provisa/releases)下载并传输到目标机器。AppImage 将所有组件镜像以 tarball 形式打包在一个 squashfs 文件系统中（REQ-294）——大多数镜像仓库扫描器无法就地检查它们。请联系您的 Provisa 客户团队获取组件镜像摘要，以便独立对照您的扫描器进行验证。

**Terraform 路径：** 在运行 `terraform/deploy.sh` 之前，必须先将 AppImage 上传到 S3。EC2 节点在启动时通过 IAM 角色下载它——它们需要出站 S3 访问权限（直接访问或通过 VPC 网关终结点）。适用与 AppImage 路径相同的扫描策略。

**Helm / Kubernetes 路径：** 各个镜像必须推送到集群可访问的镜像仓库。此路径与基于镜像仓库的扫描（Prisma Cloud、Aqua、Trivy、AWS Inspector）兼容性最好——镜像是扫描器原生理解的一等对象。对于气隙集群，将镜像镜像到内部镜像仓库，并在 `values.yaml` 中覆盖引用（REQ-294）。

---

## 开发（从源码）

### 推荐方式：`start-ui.sh`

从源码运行 Provisa 最简单的方式。一条命令即可启动所有基础设施、后端 API 和 UI 开发服务器（REQ-055）。Ctrl+C 可以干净地关闭一切。

**前置条件：** Docker Desktop、Node.js、位于 `.venv/` 的 Python 虚拟环境

```bash
./start-ui.sh
```

该命令的作用：

- 启动 `docker-compose.core.yml` + `docker-compose.dev.yml`（所有核心服务 + 演示服务），并等待其健康就绪（REQ-055）
- 向 Kafka 灌入演示数据
- 从 `.venv/` 同步 Python 依赖
- 在 8001 端口启动后端 API（日志写入 `.logs/server.log`）（REQ-558）
- 在 3000 端口启动 Vite UI 开发服务器（REQ-559）
- 打印 URL 并等待；Ctrl+C 会停止一切并拆除 compose 环境

```yaml
Backend: http://localhost:8001
UI:      http://localhost:3000
```

**选项：**

`--reset-volumes` —— 在启动前运行 `docker compose down -v`，销毁所有 Docker 卷（PostgreSQL 数据、MinIO 对象、Redis 状态等）（REQ-170）。当您需要完全干净的环境时使用——例如开发过程中发生架构变更之后，或者 Docker 崩溃导致卷损坏时。**所有数据都会丢失。**

`--observability` —— 添加完整的追踪和指标插桩。下载 OpenTelemetry Java agent 并修补 Trino 的 `jvm.config` 以加载它，为 Provisa 后端插入 OTLP 导出，并启动 OTel collector、Prometheus、Tempo 和 Grafana（`http://localhost:3100`）（REQ-330）。对 `jvm.config` 的修补会在 Ctrl+C 时自动还原。

### 手动步骤（仅后端，无 UI）

如果您只需要 API：

1. 安装 [Docker Desktop](https://docs.docker.com/get-docker/)
2. 启动核心服务：

   ```bash
   docker compose -f docker-compose.core.yml up -d
   ```

3. 启动 API：

   ```bash
   uvicorn main:app --reload --port 8001
   ```

4. 验证：`curl http://localhost:8001/health`

### 完整堆栈（Provisa 运行在容器中）

要将 API 作为容器而非在主机上运行：

```bash
docker compose -f docker-compose.core.yml -f docker-compose.app.yml up -d
```

### 服务

**核心服务（`docker-compose.core.yml`）—— 始终必需：**

| 服务 | 端口 | 用途 |
| --------- | ------ | --------- |
| PostgreSQL | 5432 | 配置元数据 + Iceberg 目录（REQ-169） |
| PgBouncer | 6432 | 连接池（REQ-053） |
| 联邦查询引擎 | 8080 | 查询联邦（REQ-028） |
| Redis | 6379 | 查询结果缓存（REQ-371） |
| MinIO | 9000/9001 | 兼容 S3 的对象存储（REQ-029, REQ-171） |

**演示服务（`docker-compose.dev.yml`）—— 可选，由 `start-ui.sh` 包含：**

| 服务 | 端口 | 用途 |
| --------- | ------ | --------- |
| MongoDB | 27017 | 演示用 NoSQL 数据源 |
| Kafka | 9092 | 演示用流式数据源 |
| Schema Registry | 8081 | 演示用 Avro/Protobuf 架构管理 |
| Debezium | — | 演示用 CDC 连接器 |
| Elasticsearch | 9200 | 演示用搜索数据源 |
| Neo4j | 7474/7687 | 演示用图数据源 |
| Fuseki | 3030 | 演示用 SPARQL 三元组存储 |
| OpenTelemetry Collector | — | 追踪采集（需 `--observability`）（REQ-302） |
| Prometheus | 9090 | 指标（需 `--observability`）（REQ-330） |
| Tempo | — | 追踪存储（需 `--observability`）（REQ-330） |
| Grafana | 3100 | 仪表板（需 `--observability`）（REQ-330） |

### 遥测后端（`otlp2sql`）

上述 `--observability` 堆栈（Collector → Tempo/Prometheus/Grafana）是一条遥测路径。另一条是 `otlp2sql`（`provisa.observability.otlp2sql`）：一个 OTLP/HTTP 接收器，将追踪、指标和日志写入由 SQLAlchemy URL 指定的 SQL 数据库，并在摄取时提取 `provisa.*` span 属性，因此不需要单独的压缩任务运行。写入是分批进行的（`OTLP2SQL_BATCH_MAX_ROWS`，默认 1000；`OTLP2SQL_BATCH_MAX_SECS`，默认 2 秒）。

遥测数据拥有自己独立的存储，与控制平面数据库分开。通过 `PROVISA_OPS_DB_URL` 选择后端：

| `PROVISA_OPS_DB_URL` | 后端 | 说明 |
| --- | --- | --- |
| *（未设置）* | `~/.provisa/telemetry/` 下的专用 DuckDB | 默认；无服务器，无 Docker |
| `clickhouse+native://user@host/otel` | ClickHouse | 高速摄取，自动后台合并 |
| `postgresql+psycopg2://user@host/otel` | PostgreSQL | 中等数据量 |
| `trino://user@host:8080/otel` | Trino / Iceberg | 技术上可行，**不推荐**——见下文 |

**关于 `trino://`：** SQLAlchemy 的 Trino 方言可以生成有效的 Trino DDL 和 `INSERT` 语句，因此从技术上讲它可以作为 `otlp2sql` 的后端。但除了低摄取速率场景外并不推荐使用。每次批次刷新都会变成一次分布式 Trino `INSERT` 加一次 Iceberg 快照，因此高速率遥测会产生大量小文件和快照，且仍需要定期执行 `ALTER TABLE ... EXECUTE optimize` / `expire_snapshots`——而 `otlp2sql` 并不运行这些操作。它还会让查询引擎处于摄取热路径上。

对于流向 Trino/Iceberg 的高容量遥测，请改用 `otlp2parquet`：它将 parquet 写入对象存储而不经过 Trino，并由一个定时的 Trino 压缩任务将原始文件滚动合并进活动的 Iceberg 表。若希望用单一引擎同时处理高速率摄取和压缩，优先选择 ClickHouse。

将应用和 Trino 的 OTLP 导出器（`OTEL_EXPORTER_OTLP_ENDPOINT`）指向 `otlp2sql` 端点，并将 ops 域注册到相同的 `PROVISA_OPS_DB_URL`，使其读取接收器所写入的数据。

---

## macOS 安装程序

适用于开发者工作站和评估。完全气隙——下载后无需联网（REQ-227）。

基础安装程序是**原生安装**：DuckDB 联邦查询引擎 + SQLite 控制平面 + 内存态（fakeredis）缓存，不含 Docker、虚拟机、Trino、Redis 或 MinIO（REQ-972, REQ-979）。联邦查询引擎是一个向导选项——DuckDB（原生，默认）、Docker 上的 Trino，或外部引擎（REQ-973）。可观测性始终开启，为可在 Admin 中查看的自遥测；Docker collector/Prometheus/Grafana 堆栈是一个可选的外部演示，而非开关（REQ-975）。演示数据包是可选的，默认关闭（REQ-978）。Trino、Docker 可观测性堆栈和演示数据都是重量级附加组件，按本地优先顺序解析（安装程序旁的目录、挂载卷、`~/Downloads`，最后是 GitHub release），因此企业可以为气隙安装预先准备 tarball（REQ-977）。

### 步骤

1. 从 [GitHub releases 页面](https://github.com/provisa/provisa/releases)下载 `Provisa-<version>-macOS.dmg`
2. 打开 DMG，将 **Provisa.app** 拖到 `/Applications`
3. 双击 **Provisa.app**——首次启动的设置只运行一次；向导会提供上述引擎、可观测性和演示数据的选择（REQ-1007）
4. 打开终端：

   ```bash
   provisa start    # start all services
   provisa status   # confirm all services are running
   provisa open     # open the UI in the browser
   ```

   （REQ-224）

### 数据持久化

所有数据存储在 `~/.provisa/` 中（REQ-224）。要移除一切：`provisa uninstall`。

---

## Windows 安装程序

适用于开发者工作站和评估。完全气隙——下载后无需联网（REQ-227）。

与 macOS 类似，基础 Windows 安装程序是**原生层**：独立的 Python 运行时 + provisa wheel 包 + DuckDB/pg_duckdb + SQLite 控制平面，不携带 Docker、虚拟机或容器镜像（REQ-979）。联邦查询引擎（Trino）、可观测性堆栈和演示数据包通过后续独立的分层安装程序添加，顺序为：容器安装程序（`Provisa-Container-<version>.exe`，添加 WSL2 + containerd + Trino），然后是 Obs 安装程序（需要容器层），然后是 Demo 安装程序（需要 Core + Obs）。首次启动指引会说明如何通过运行容器安装程序来初始化联邦查询引擎（REQ-1005）。

### 步骤

1. 从 [GitHub releases 页面](https://github.com/provisa/provisa/releases)下载 `Provisa-<version>-windows-x64.exe`
2. 运行安装程序——无需管理员权限；安装到 `%LOCALAPPDATA%\Programs\Provisa\`
3. 从开始菜单打开 **Provisa First Launch**——原生设置只运行一次，并打印出针对分层附加组件的后续步骤指引（REQ-1005）
4. 打开新终端：

   ```text
   provisa status
   provisa open
   ```

   （REQ-224）

### 数据持久化

所有数据存储在 `%USERPROFILE%\.provisa\` 中。

---

## Linux AppImage —— 单节点或多节点虚拟机

### 它是什么

`Provisa.AppImage` 是一个单一的自包含可执行文件，其中打包了（REQ-223, REQ-228）：

- 一个无根 Docker 守护进程（`dockerd-rootless.sh` + `rootlesskit`）——无需系统级 Docker 或 root 权限
- 所有容器镜像的 tarball（PostgreSQL、PgBouncer、MinIO、Redis、联邦查询引擎、Provisa API）（REQ-294）
- Provisa CLI 包装器和首次启动设置脚本

Provisa 镜像在打包时就已预先构建——绝不包含 Python 源码。

### 何时使用

- 本地裸机或虚拟机（单节点或多节点）
- 没有 K8s 集群的云虚拟机
- 气隙环境（REQ-294）
- 希望运维比 Kubernetes 更简单时

---

### 步骤 —— 单节点

1. 从 [GitHub releases 页面](https://github.com/provisa/provisa/releases)下载 `Provisa.AppImage` 并传输到目标机器
2. 赋予可执行权限：

   ```bash
   chmod +x Provisa.AppImage
   ```

3. 运行首次启动设置：

   ```bash
   ./Provisa.AppImage
   ```

4. 设置向导会询问：
   - **角色** → 选择 `primary`
   - **RAM 预算** → 分配的内存量（0 = 全部可用内存）；决定 Trino worker 数量
   - **主机名** → 该节点对外通告的地址
   - **API 端口** → 默认 `8000`（REQ-560）
5. 设置过程会加载所有容器镜像（约 2–5 分钟），写入配置，并启动服务
6. 验证：

   ```bash
   provisa status
   curl http://localhost:8000/health
   ```

---

### 步骤 —— 多节点（主节点）

先在主节点上执行以下步骤。必须在主节点运行起来之后再设置从节点。

1. 下载并将 `Provisa.AppImage` 传输到主机器
2. 开放所需的防火墙端口（从节点将通过这些端口向内连接）：

   | 端口 | 服务 |
   | ------ | --------- |
   | 5432 | PostgreSQL |
   | 6379 | Redis |
   | 9000 | MinIO |
   | 8080 | 联邦查询引擎协调节点 |
   | 8000 | Provisa API |

3. 赋予可执行权限并运行：

   ```bash
   chmod +x Provisa.AppImage
   ./Provisa.AppImage
   ```

4. 设置向导会询问：
   - **角色** → 选择 `primary`
   - **RAM 预算**、**主机名**、**API 端口** → 与单节点相同的方式回答
5. 设置完成后，记下该机器的**私有 IP**——从节点需要用到它
6. 向导会打印一个 nginx upstream 配置块——请保存它，用于负载均衡器配置
7. 验证：

   ```bash
   provisa status
   curl http://localhost:8000/health
   ```

---

### 步骤 —— 多节点（每个从节点）

在主节点运行且可达之后，在每个额外节点上重复以下步骤。

1. 下载并将 `Provisa.AppImage` 传输到从节点机器
2. 确认从节点可以访问主节点：

   ```bash
   curl http://<primary-ip>:8000/health
   ```

3. 赋予可执行权限并运行：

   ```bash
   chmod +x Provisa.AppImage
   ./Provisa.AppImage
   ```

4. 设置向导会询问：
   - **角色** → 选择 `secondary`
   - **主节点 IP** → 输入主节点的 IP（会实时验证连通性）
   - **RAM 预算**、**主机名**、**API 端口** → 与上文相同方式回答
5. 设置过程会加载一个精简的镜像集合（不含 PostgreSQL、PgBouncer、MinIO、Redis——这些只在主节点运行）（REQ-561），启动 Provisa API 和一个联邦查询引擎 worker
6. 验证：

   ```bash
   provisa status
   curl http://localhost:8000/health
   ```

7. 将该节点加入负载均衡器的 upstream

---

### 主 / 从拓扑

**主节点**运行所有单例服务：

| 服务 | 为何是单例 |
| --------- | --------------- |
| PostgreSQL | 共享架构、应用配置、语义模型 |
| Redis | 共享的查询结果缓存和订阅状态（REQ-371） |
| MinIO | 用于重定向结果和物化视图快照的共享对象存储（REQ-029） |
| 联邦查询引擎协调节点 | 所有 worker（主节点 + 从节点）都注册到这里（REQ-028） |

**从节点**只运行：

- Provisa API——无状态；启动时从主节点的 PostgreSQL 读取全部配置（REQ-057, REQ-562）
- 联邦查询引擎 worker——自行向主节点上的协调节点注册（REQ-028）

所有应用状态都流经主节点的 PostgreSQL。无需手动同步。（REQ-562）

---

### 非交互式（自动化）首次启动

用于 Terraform、cloud-init 或 Ansible——通过传入参数而非回答提示：

```bash
# Primary
./Provisa.AppImage --non-interactive --role primary --ram-gb 32

# Secondary
./Provisa.AppImage --non-interactive --role secondary --primary-ip 10.0.0.10 --ram-gb 32
```

非交互模式会安装一个 systemd 单元（`/etc/systemd/system/provisa.service`）以实现开机自启。（REQ-563）

| 参数 | 说明 |
| ------ | ------------- |
| `--non-interactive` | 跳过所有提示；安装 systemd 单元 |
| `--role primary\|secondary` | 节点角色 |
| `--primary-ip <ip>` | 主节点 IP（从节点必填） |
| `--ram-gb <n>` | 分配的内存（0 = 全部可用） |

---

## 云虚拟机部署 —— Terraform（AWS）

在 AWS 上通过一条交互式命令预配一个完整的多节点 Provisa 集群——VPC、安全组、EC2 实例、ALB、NLB。（REQ-564）

### 文件

| 文件 | 用途 |
| ------ | --------- |
| `terraform/deploy.sh` | 交互式包装脚本——收集参数、校验凭据、写入 `terraform.tfvars`、运行 apply |
| `terraform/aws/variables.tf` | 带默认值的所有变量定义 |
| `terraform/aws/main.tf` | VPC、子网、安全组、IAM、EC2、ALB、NLB |
| `terraform/aws/outputs.tf` | 端点 URL 和节点 IP |

### 步骤

1. 从 [GitHub releases 页面](https://github.com/provisa/provisa/releases)下载 `Provisa.AppImage`

2. 上传到您 AWS 账户中的 S3 存储桶：

   ```bash
   aws s3 cp Provisa.AppImage s3://<your-bucket>/releases/Provisa.AppImage
   ```

3. 确保 shell 中可用 AWS 凭据（以下任一方式）：
   - 环境变量：`AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY`
   - 命名 profile：`export AWS_PROFILE=my-profile`
   - 活动的 SSO 会话：`aws sso login`

4.（可选）如果需要 SSH 访问节点，请在目标区域创建 EC2 密钥对并记下密钥对名称

5. 运行部署包装脚本：

   ```bash
   bash terraform/deploy.sh
   ```

6. 回答向导问题（见下方参考表）。脚本会在继续之前验证 AppImage 是否存在于 S3 中，若不存在则中止

7. 检查部署摘要并确认

8. Terraform 预配所有基础设施（约 5–10 分钟）。apply 完成后，脚本会打印：

   ```text
   api_endpoint      = "http://<alb-dns>:8000"
   flight_endpoint   = "<nlb-dns>:8815"
   primary_ip        = "10.0.x.x"
   secondary_ips     = ["10.0.x.x", ...]
   ```

   （REQ-564, REQ-143）

9.（可选）将 DNS 记录指向 ALB 和 NLB 的 DNS 名称

10. 验证：

    ```bash
    curl http://<api_endpoint>/health
    ```

### 向导问题

| 问题 | 默认值 | 说明 |
| ---------- | --------- | ------- |
| 云提供商 | — | 目前仅支持 AWS |
| AWS 凭据 | — | 先检查是否存在活动会话 |
| 区域 | `us-east-1` | |
| 节点数 | `2` | 1 = 仅主节点，无负载均衡器；2+ = 主节点 + 从节点 + ALB/NLB |
| 实例类型 | `m7i.2xlarge` | 见下方规格指南 |
| 根卷大小 | `100 GB` | 每节点 |
| RAM 预算 | `0`（全部内存） | 决定每节点的 Trino worker 数量 |
| S3 存储桶 | — | 会实时验证 |
| S3 key | `releases/Provisa.AppImage` | |
| SSH 访问 | 否 | 需要已有密钥对名称 + 管理员 CIDR |
| VPC CIDR | `10.0.0.0/16` | |

### 实例规格指南

| 类型 | vCPU | RAM | 每节点 Trino worker 数 | 使用场景 |
| ------ | ------ | ----- | -------------------- | ---------- |
| `m7i.xlarge` | 4 | 16 GB | 0 | 开发/小数据集 |
| `m7i.2xlarge` | 8 | 32 GB | 1 | 小型生产环境 |
| `m7i.4xlarge` | 16 | 64 GB | 2 | 中型生产环境 |
| `m7i.8xlarge` | 32 | 128 GB | 4 | 大型生产环境 |

所有节点都向主节点上的一个协调节点贡献 worker（REQ-028）。一个 3 节点的 `m7i.4xlarge` 集群总共产生 6 个 Trino worker。

### 预配内容

- 跨两个可用区的两个公有子网组成的 VPC（REQ-564）
- 安全组：负载均衡器组（8000/8815 端口的公网入站）、节点组（负载均衡器→节点、集群内部通信、可选 SSH）
- 带 S3 GetObject 权限（针对 AppImage 存储桶）的 IAM 角色 + 实例 profile
- 主 EC2 实例——以 `--non-interactive --role primary` 模式运行首次启动
- 从 EC2 实例（node_count − 1 个）——以 `--non-interactive --role secondary --primary-ip <primary private IP>` 模式运行首次启动；依赖主节点先完成
- 8000 端口的 ALB —— HTTP API，健康检查 `/health`（REQ-560）
- 8815 端口的 NLB —— Arrow Flight / gRPC（REQ-143）
- 两个负载均衡器都挂载到所有节点

### 前置条件清单

- [ ] IAM 权限：EC2 全权限、ELB 全权限、VPC 全权限、IAM 角色创建、针对 AppImage 存储桶的 S3 GetObject
- [ ] `Provisa.AppImage` 已上传到 S3
- [ ] EC2 节点具备出站 S3 访问能力（直接联网或 S3 VPC 网关终结点）
- [ ] 目标区域已存在 EC2 密钥对（如需 SSH）
- [ ] 本地已安装 Terraform ≥ 1.5
- [ ] 已为 ALB / NLB 规划好 DNS 记录（可选但推荐）
- [ ] 如需 HTTPS，已准备好 ACM 证书（基础 Terraform 中不包含）

### 密钥

Terraform 中不嵌入任何密钥。AppImage 在首次启动时生成凭据，并将其写入每个节点上的 `~/.provisa/config.yaml`（REQ-563）。生产环境中，部署完成后从主节点获取管理员令牌：

```bash
ssh ubuntu@<primary-public-ip> cat ~/.provisa/config.yaml | grep admin_token
```

---

## Kubernetes / Helm

### 何时使用

您的团队已在运行 Kubernetes 集群，并希望 Provisa 参与该运维模型（REQ-056）。如果您正在评估 Provisa，或是在没有现成集群的情况下进行本地部署，AppImage 路径更简单。

注意：Provisa AppImage 无法在 Kubernetes pod 内运行——它需要 FUSE 和无根 Docker 守护进程，这些在标准 pod 安全配置文件中不可用。

### 步骤

1. 确认集群访问：

   ```bash
   kubectl cluster-info
   ```

2. 将镜像拉取并镜像到您的内部镜像仓库（气隙或需扫描的环境必需；如果直接从公共镜像仓库拉取则可跳过）（REQ-294）：

   | 镜像 | 用途 |
   | ------- | ---------- |
   | `provisa/provisa:<version>` | Provisa API |
   | `trinodb/trino:480` | 联邦查询引擎协调节点 + worker（REQ-169） |
   | `postgres:16` | 集群内 PostgreSQL（如启用 `postgresql.enabled`）（REQ-169） |
   | `edoburu/pgbouncer:latest` | 集群内 PgBouncer（如启用 `pgbouncer.enabled`）（REQ-053） |
   | `redis:7.2` | 集群内 Redis（如启用 `redis.enabled` 且未设置 `redis.host`）（REQ-371） |
   | `minio/minio:latest` | 集群内 MinIO（如启用 `minio.enabled`）（REQ-029） |

   对于经镜像仓库扫描的环境：
   - 将每个镜像推送到您的暂存镜像仓库
   - 运行您的扫描器（Prisma Cloud、Aqua、Trivy、AWS Inspector）并获得批准
   - 晋升到您的生产内部镜像仓库

3. 安装前需决定：
   - **PostgreSQL** —— 集群内（`postgresql.enabled: true`）还是外部托管（`postgresql.host`）？生产环境建议使用外部
   - **Redis** —— 集群内还是外部（`redis.host`）？请修改默认密码（`redis.password`）
   - **MinIO / S3** —— 集群内 MinIO 还是原生 S3？在 AWS 上，建议使用带 IAM 角色的 S3
   - **密钥** —— 评估阶段可通过 `--set` 传入；生产环境请使用 External Secrets 或 Vault Agent

4. 安装 chart：

   ```bash
   helm install provisa helm/provisa/ \
     --set config.pgPassword=<password> \
     --set config.adminToken=<token> \
     --set s3.endpoint=https://s3.amazonaws.com \
     --set s3.bucket=my-provisa-results \
     --namespace provisa --create-namespace
   ```

   如使用内部镜像仓库，添加镜像覆盖：

   ```bash
   --set image.repository=harbor.internal.example.com/provisa/provisa \
   --set image.tag=1.2.3 \
   --set trino.image.repository=harbor.internal.example.com/trinodb/trino \
   --set trino.image.tag=480
   ```

5. 验证 pod 正在运行：

   ```bash
   kubectl get pods -n provisa
   ```

6. 检查 API：

   ```bash
   kubectl port-forward svc/provisa 8000:8000 -n provisa
   curl http://localhost:8000/health
   ```

7.（可选）为外部访问启用 ingress——设置 `ingress.enabled: true` 并配置您的 ingress controller

### 前置条件清单

- [ ] Kubernetes 1.26+，Helm 3.12+
- [ ] 支持 `ReadWriteOnce` PVC 的存储类（用于集群内有状态服务）
- [ ] 镜像可供集群访问（公共或内部镜像仓库）
- [ ] PostgreSQL 端点 + 凭据（如为外部）
- [ ] Redis 端点 + 凭据（如为外部）
- [ ] S3 存储桶 + 凭据或 IAM 角色
- [ ] 已选定管理员令牌
- [ ] 已配置 ingress controller（如需外部访问）

### 关键值

| 值 | 默认值 | 说明 |
| ------- | --------- | ------------- |
| `replicaCount` | `2` | Provisa API 副本数（无状态）（REQ-057） |
| `config.pgHost` | `postgres` | PostgreSQL 主机 |
| `config.pgPassword` | | PostgreSQL 密码 |
| `config.adminToken` | | 管理员 API Bearer 令牌 |
| `redis.enabled` | `true` | 部署集群内 Redis StatefulSet（REQ-371） |
| `redis.host` | `""` | 设置以使用外部 Redis |
| `redis.port` | `6379` | |
| `redis.password` | `"provisa"` | 请修改此项 |
| `redis.tls` | `false` | |
| `trino.enabled` | `true` | 部署联邦查询引擎（REQ-028） |
| `trino.workers` | `2` | 联邦查询引擎 worker 副本数（REQ-056） |
| `postgresql.enabled` | `true` | 部署集群内 PostgreSQL（REQ-169） |
| `postgresql.host` | `""` | 设置以使用外部 PostgreSQL |
| `minio.enabled` | `true` | 部署集群内 MinIO（REQ-029） |
| `s3.endpoint` | | 兼容 S3 的终结点 URL |
| `s3.bucket` | `provisa-results` | 用于大结果集重定向的存储桶（REQ-029, REQ-137） |
| `ingress.enabled` | `false` | 启用 ingress |

### 扩缩容

```bash
kubectl scale deployment/provisa --replicas=5 --namespace provisa
```

联邦查询引擎 worker 可独立扩缩——更多 worker 可提升吞吐量和并发查询能力（REQ-056）。（REQ-057）

### 更新配置

```bash
kubectl create configmap provisa-config \
  --from-file=config.yaml=./config.yaml \
  --namespace provisa --dry-run=client -o yaml | kubectl apply -f -
kubectl rollout restart deployment/provisa --namespace provisa
```

---

## 高可用与恢复

Provisa 在所有部署模式下都采用两层恢复模型（REQ-703）：

- **第一层——瞬时错误。** 读操作在遇到瞬时错误时，使用带全抖动的指数退避重试，最长可达 30 秒。可通过 `PROVISA_RETRY_BUDGET_SECS` 调整预算。写操作从不在内部重试，内存错误也从不可重试。
- **第二层——组件故障。** 内部引擎监视器会检测失败的软件组件并在 2–3 分钟内重启它们。

机器级和集群级故障仍是运维方的责任——请为节点丢失容错预配冗余节点和负载均衡器（见上文的 Terraform 和 Helm 路径）。

## 联邦查询引擎依赖

数据仓库联邦查询引擎需要 Provisa 默认安装之外的 Python 包和系统级组件。此处列出的所有 Python 包都声明在 `pyproject.toml` 中，并作为标准 `pip install provisa` 或 `pip install -e .` 的一部分安装 [tool-verified: `pyproject.toml` lines 44–52]。

这些 Python 包随 Provisa 默认安装一起提供——任何数据仓库引擎都不需要额外的可选 extras。系统级组件（ODBC 驱动、云 CLI、服务账号密钥）需要单独安装。

### Python 包（已包含在核心依赖中）

[tool-verified: `pyproject.toml` lines 41–52]

| 包 | 引擎 | 用途 |
| ------- | ------ | ------- |
| `databricks-sql-connector` | Databricks | SQL warehouse 连接；Arrow Cloud Fetch（REQ-987） |
| `snowflake-connector-python[pandas]` | Snowflake | 连接 + Arrow 原生的 `fetch_arrow_table`（REQ-988） |
| `google-cloud-bigquery` | BigQuery | 查询执行 |
| `google-cloud-bigquery-storage` | BigQuery | 用于 Arrow 原生读取的 Storage Read API |
| `google-cloud-storage` | BigQuery | 用于外部表关联的 GCS 暂存 |
| `pyodbc` | Fabric、Synapse | 与 T-SQL 终结点的 ODBC 连接 |
| `azure-identity` | Fabric、Synapse | 通过 `DefaultAzureCredential` 获取 Azure AD 令牌 |
| `clickhouse-connect` | ClickHouse | HTTP 列式读取 |
| `protobuf>=6.33.5,<7` | BigQuery、gRPC | 兼容性锁定——`google-cloud-*` 与 OTel 共享一个 protobuf 运行时；`<7` 使两者保持一致 |
| `grpcio-status<1.82` | gRPC | 与 `protobuf<7` 锁定保持一致 |

### 系统级要求

以下不是 Python 包——必须安装在运行 Provisa 的主机或容器上。

**Microsoft Fabric 和 Azure Synapse（ODBC）**

`pyodbc` 通过 Microsoft ODBC Driver for SQL Server（`msodbcsql18`）建立连接。该驱动必须安装在主机上——而非通过 pip。[tool-verified: `mssql_warehouse_runtime.py` line 84 `"ODBC Driver 18 for SQL Server"` default]

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

Provisa 会自动识别该驱动。若要覆盖驱动名称（用于非标准安装），设置：

```bash
export PROVISA_MSSQL_ODBC_DRIVER="ODBC Driver 17 for SQL Server"
```

**Azure AD 认证（Fabric 和 Synapse）**

两个引擎都通过 `azure.identity.DefaultAzureCredential` 进行认证 [tool-verified: `mssql_warehouse_runtime.py:79`, `fabric_shortcuts.py:46`]。`DefaultAzureCredential` 按顺序检查凭据来源：环境变量、workload identity、managed identity、VS Code、`az login` 等等。

对于本地开发，`az login` 是最简单的方式：

```bash
az login
```

对于生产环境，使用 managed identity（在 Azure 虚拟机或 AKS 上）——无需凭据管理。对于服务主体认证，设置：

```bash
export AZURE_TENANT_ID=<tenant>
export AZURE_CLIENT_ID=<app-id>
export AZURE_CLIENT_SECRET=<secret>
```

**BigQuery（服务账号）**

`google-cloud-bigquery` 使用 Application Default Credentials。对于本地开发，指向一个服务账号密钥文件：

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa-key.json
```

对于 GCP 上的生产环境（Cloud Run、带 Workload Identity 的 GKE、Compute Engine），该库会自动获取所附加的服务账号——无需设置环境变量。

该服务账号需要：

- `roles/bigquery.dataViewer` —— 读取数据
- `roles/bigquery.jobUser` —— 运行查询
- `roles/bigquery.dataEditor` —— 创建外部表（用于 ATTACH）
- `roles/storage.objectViewer` —— 为外部表读取 GCS 对象

**Databricks（开发代理环境下的 CA 证书）**

如果 Provisa 运行在进行 TLS 拦截的代理（Charles、mitmproxy、企业代理）之后，Databricks SQL 连接器可能会拒绝该代理的证书。传入自定义 CA bundle：

```bash
export REQUESTS_CA_BUNDLE=/path/to/your/proxy-ca.pem
```

Databricks 连接器从 `requests` 继承此设置——不需要 Databricks 专用的环境变量。

### 各引擎检查清单

**Databricks**（REQ-987）

- [ ] 已安装 `databricks-sql-connector`（默认）
- [ ] 带 `http_path` 的引擎 URL：`databricks://token:TOKEN@workspace.azuredatabricks.net?http_path=/sql/1.0/warehouses/xxx`
- [ ] 个人访问令牌或服务主体令牌
- [ ] 如在 TLS 拦截代理之后，已设置 `REQUESTS_CA_BUNDLE`

**Snowflake**（REQ-988）

- [ ] 已安装 `snowflake-connector-python[pandas]`（默认）
- [ ] 引擎 URL：`snowflake://user:pass@account.snowflakecomputing.com/database`
- [ ] `PROVISA_ENGINE_URL` 或 `federation_hints` 中包含 `account`

**BigQuery**（REQ-989）

- [ ] 已安装 `google-cloud-bigquery`、`google-cloud-bigquery-storage`、`google-cloud-storage`（默认）
- [ ] 已设置 `GOOGLE_APPLICATION_CREDENTIALS`（开发）或已配置 workload identity（生产）
- [ ] 若项目无法从服务账号推断，已设置 `GOOGLE_CLOUD_PROJECT`
- [ ] 服务账号具备 BigQuery Data Viewer + Job User 角色

**Microsoft Fabric**（REQ-989）

- [ ] 已安装 `pyodbc` + `azure-identity`（默认）
- [ ] 已安装 `msodbcsql18` 系统驱动
- [ ] 已设置 `FABRIC_SQL_SERVER` 和 `FABRIC_DATABASE`
- [ ] Azure AD 认证：`az login`（开发）或 managed identity / 服务主体（生产）
- [ ] 如使用外部对象存储链接，已设置 `FABRIC_WORKSPACE_ID`

**Azure Synapse**（REQ-989）

- [ ] 与 Fabric 相同的 Python + 系统要求
- [ ] 已设置 `SYNAPSE_SQL_SERVER` 和 `SYNAPSE_DATABASE`
- [ ] 与 Fabric 相同的 Azure AD 认证设置

**ClickHouse**（REQ-986）

- [ ] 已安装 `clickhouse-connect`（默认）
- [ ] 引擎 URL：`clickhouse+http://user:pass@host:8123/database`
- [ ] `federation_hints` 中设置 `secure: "true"` 以启用 TLS（8443 端口）

---

## 环境变量

| 变量 | 默认值 | 用途 |
| ---------- | --------- | --------- |
| `PG_PASSWORD` | | PostgreSQL 密码 |
| `PROVISA_CONFIG` | `config/provisa.yaml` | 配置文件路径（REQ-528） |
| `PROVISA_REDIRECT_ENABLED` | `false` | 启用大结果集向 S3 重定向（REQ-029, REQ-137） |
| `PROVISA_REDIRECT_THRESHOLD` | `1000` | 触发重定向的行数阈值（REQ-029） |
| `PROVISA_REDIRECT_BUCKET` | `provisa-results` | S3 存储桶（REQ-029） |
| `PROVISA_REDIRECT_ENDPOINT` | | 兼容 S3 的终结点 URL（REQ-029） |
| `PROVISA_REDIRECT_TTL` | `3600` | 预签名 URL TTL（秒）（REQ-141） |
| `REDIS_HOST` | `localhost` | Redis 主机 |
| `REDIS_PORT` | `6379` | Redis 端口 |
| `REDIS_PASSWORD` | | Redis 密码 |
| `REDIS_TLS` | `false` | 为 Redis 启用 TLS |
| `TRINO_HOST` | `localhost` | Trino 联邦查询引擎协调节点主机（REQ-028, REQ-054） |
| `TRINO_PORT` | `8080` | Trino 联邦查询引擎协调节点 HTTP 端口（REQ-028, REQ-054） |
| `PROVISA_ENGINE` | `duckdb` | 活动联邦查询引擎键（REQ-989）；覆盖已持久化的配置 |
| `PROVISA_ENGINE_URL` | | 面向 URL 驱动引擎（Databricks、Snowflake、ClickHouse、BigQuery、Fabric、Synapse、SQLAlchemy）的连接 URL |
| `PROVISA_MATERIALIZE_URL` | | 物化存储 URL 覆盖；默认使用引擎自身的存储 |
| `PROVISA_MSSQL_ODBC_DRIVER` | `ODBC Driver 18 for SQL Server` | Fabric / Synapse 的 ODBC 驱动名称 |
| `GOOGLE_APPLICATION_CREDENTIALS` | | GCP 服务账号密钥 JSON 的路径（BigQuery） |
| `GOOGLE_CLOUD_PROJECT` | | GCP 项目 ID（BigQuery；未设置时从服务账号推断） |
| `FABRIC_SQL_SERVER` | | Microsoft Fabric SQL 分析终结点主机名 |
| `FABRIC_DATABASE` | | Fabric 数据库名称 |
| `FABRIC_WORKSPACE_ID` | | Fabric 工作区 GUID（使用外部对象存储快捷方式时必需） |
| `SYNAPSE_SQL_SERVER` | | Azure Synapse 专用 SQL 池或无服务器主机名 |
| `SYNAPSE_DATABASE` | | Synapse 数据库名称 |
| `AZURE_TENANT_ID` | | Azure AD 租户（Fabric/Synapse 的服务主体认证） |
| `AZURE_CLIENT_ID` | | Azure AD 应用客户端 ID |
| `AZURE_CLIENT_SECRET` | | Azure AD 应用客户端密钥 |
| `REQUESTS_CA_BUNDLE` | | 自定义 CA bundle 路径（Databricks 连接器、开发 TLS 代理） |

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

（REQ-224, REQ-164）

### 配置晋升流程（开发 → 测试 → 生产）

所有环境特定的设置（连接字符串、密钥、端口）应放在环境变量或密钥管理器中——而非导出的配置里。导出的 YAML 捕获的是您的语义模型：数据源、域、角色、视图。（REQ-164）

```bash
# On dev — export after making changes in the UI
provisa export > config.yaml
git add config.yaml && git commit -m "chore: update semantic model"
git push

# On test/prod — pull and import
git pull
provisa import config.yaml
```
