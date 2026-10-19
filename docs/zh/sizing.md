# 容量规划与调优 {: #sizing-and-tuning }

容量规划工具会在你自己的工作负载下，测量你自己的 Provisa 部署。它根据这些测量结果编写一份容量规划与调优指南：工作负载需要多少物理核心和工作进程，以及每项设置能带来什么。指南中的每个数字都来自测量，因此在部署或工作负载发生变化后，需要重新运行该工具并重新生成指南。

该工具位于 Provisa 仓库的 `demo/named/perf/bench/` 下。下面的每条命令都要在该目录中、使用仓库的 Python 环境（`../../../../.venv/bin/python3`）运行。

## 它报告什么 {: #what-it-reports }

一次容量规划运行由针对你的部署的多次基准测试运行组成。每次运行只改变一件事：

| 运行 | 改变的内容 | 得到的结果 |
|---|---|---|
| 基线 | 无：一列、一行、无过滤、无联接、无缓存 | 请求的基础成本 |
| 旋钮运行 | 每次一个请求旋钮：字段、过滤、行数、同数据源联接、跨数据源联接 | 每个旋钮按单位增加的成本 |
| 跨数据源联接运行 | 跨数据源联接的取值，一次两端均实时读取，一次两端均来自副本，一次两端各取其一 | 跨数据源联接的成本（按其两端的读取方式），以及实时端与副本端之间的盈亏平衡点 |
| 缓存运行 | 每个请求都启用响应缓存，然后一个都不启用 | 缓存命中与未命中的成本 |
| 复制运行 | 数据源先实时读取，再从副本读取 | 高于该速率时副本成本更低 |
| 工作进程运行 | 工作进程数，以及每个工作进程的并发处理中请求数 | 每个物理核心的工作进程数、每个工作进程的并发处理中请求数、一个核心能提供的能力 |

旋钮运行、缓存运行和复制运行按传输和数据源进行，并对你指定的每个联邦引擎各进行一次。工作进程运行按传输进行。

这里的成本指 Provisa 处理每个请求的 CPU 时间，单位为毫秒。旋钮运行不使用缓存，因此旋钮的成本从不包含缓存的影响。

根据这些运行，`bench_report.py` 为每个引擎、传输和数据源拟合一个成本模型：基础成本，加上每个字段、过滤、行和联接的成本，其中跨数据源联接按其两端的读取方式计成本。当引擎作为独立服务器运行且其容器的 CPU 被测量时，引擎也会得到一个成本模型。 `sizing_guide.py` 把报告转换为指南。如果给它一份描述你工作负载的契约，它还会为该工作负载做容量规划。

## 编写设置契约 {: #write-a-setup-contract }

设置契约是一个 YAML 文件。它列出你的部署的端点、基准测试可以读取的表、请求的形态以及负载。基准测试在运行前会对照部署检查契约，并拒绝部署未公开的表、列或联接。

表和列使用你的部署注册时的名称。各传输如何称呼它们（SQL 名称、GraphQL 字段、Cypher 标签、REST 路径、gRPC 类型），会在运行开始时从部署中查询。

```yaml
version: 1

deployment:
  name: acme-prod
  endpoints:
    http: {base_url: "https://provisa.acme.internal"}
    pgwire: {host: provisa.acme.internal, port: 5439}
    bolt: {host: provisa.acme.internal, port: 7687}
    flight: {host: provisa.acme.internal, port: 8815}
    grpc: {host: provisa.acme.internal, port: 50051}
  # The credential is read from an environment variable; the secret never appears here.
  # kind: password (the variable holds the user's password) or token (a ready bearer token).
  credentials: {mode: env, kind: password, user: bench-user, env: ACME_BENCH_PASSWORD}
  # The roles requests are sent as, with the share of requests each gets.
  roles:
    - {role: analyst, weight: 0.8}
    - {role: regional_manager, weight: 0.2}

sources:
  sales-pg:
    # What this source's reads are measured under: live, or a replica refreshed every ttl_seconds.
    replication: {setting: live, ttl_seconds: 0}
    # Optional: a Docker container whose CPU is sampled as the source's CPU.
    monitor: {container: sales-postgres}
    tables:
      - schema: public
        table: orders
        weight: 1.0              # share of this source's requests that start at this table
        base_column: order_id    # the one column a baseline request selects
        columns:
          # A column with a domain can be filtered on; the filter's value is drawn from it.
          - {name: order_id, domain: {kind: int_range, min: 1, max: 5000000}}
          - {name: region, domain: {kind: set, values: [NA, EMEA, APAC]}}
          - {name: amount}
      - schema: public
        table: order_lines
        weight: 0.0              # reached only through the join below
        base_column: line_id
        columns:
          - {name: line_id}
          - {name: order_id, domain: {kind: int_range, min: 1, max: 5000000}}
    # Joins within the source, by registered table and column. The relationship must be registered.
    joins:
      - {left: "orders.order_id", right: "order_lines.order_id"}

# Joins across sources: "source:table.column".
cross_source_joins: []

# The rows a request returns when the rows knob does not apply.
base: {rows: 1}

# Each knob is the probability it applies to a request and the distribution its value is drawn
# from when it does. Distributions: constant {value}, uniform {min, max}, zipf {exponent, max},
# geometric {p, max}.
knobs:
  fields: {probability: 1, distribution: {kind: uniform, min: 1, max: 3}}
  filters: {probability: 0.5, distribution: {kind: constant, value: 1}}
  rows: {probability: 1, distribution: {kind: zipf, exponent: 1.2, max: 100}}
  joins_same_source: {probability: 0.2, distribution: {kind: constant, value: 1}}
  joins_cross_source: {probability: 0, distribution: {kind: constant, value: 0}}
  # How often a request repeats one sent shortly before it: the value is how many requests back.
  cache_repetition: {probability: 0.3, distribution: {kind: uniform, min: 1, max: 50}}
  # Share of requests that opt into the response cache. REST and JSON:API have no opt-in.
  cache: {probability: 0.6}
  # How a request's cross-source joins are chosen by how their two ends are read: both_live,
  # both_replica, or mixed (one of each), as the sources' replication settings above make them.
  # Probability 0 chooses joins whatever their class.
  join_replication: {probability: 0, weights: {both_live: 1.0}}
  # Share of requests whose table is in each source.
  source_weights: {sales-pg: 1.0}
  # auto lets the deployment choose the route; leave every source on auto to size a workload.
  route:
    sales-pg: {mode: auto, federated_probability: 0}

# The transports the benchmark runs, one after another. A transport's entry overrides any of the
# knobs above for that transport only; {} means no override.
transports:
  graphql: {}
  pgwire:
    knobs:
      rows: {probability: 1, distribution: {kind: uniform, min: 100, max: 1000}}

load:
  mode: closed_loop
  steps: [1, 16, 64, 256]   # concurrent connections, one step after another
  window_s: 30              # seconds per step
  processes: half_cores     # load-generator processes: an integer, or half the client's cores
  profile_requests: 0
  idle_window_s: 0          # seconds of no load after the ramp; see the replication runs below

seed: 0

# The workload to size: its request rate and each transport's share of it. The knobs above
# describe its requests. A benchmark run does not read this section.
workload:
  rate_per_s: 1500
  transport_weights: {graphql: 0.7, pgwire: 0.3}
```

契约格式由 `setup_contract.py` 和 `contract_parse.py` 定义；无效的契约会被拒绝，并给出指明字段的消息。 `workload` 的权重之和必须为 1，且其中列出的每个传输都必须是契约会运行的传输。

按请求的真实情况来描述它们。指南恰好针对该契约生成的请求做容量规划：规划使用与运行相同的带种子生成器来抽取请求。

## 运行 {: #run-it }

### 1. 一次性解析名称 {: #1-resolve-the-names-once }

先运行一次基线契约。该运行会向部署询问各传输如何称呼你的表，并把 `resolved-names.json` 写在其结果旁边。

```bash
../../../../.venv/bin/python3 run_benchmark.py --engine pg --optimistic \
  --setup acme.yaml --server-pid "$PROVISA_PID" --output-dir results/first
```

`--engine` 指明你的部署所运行的联邦引擎（`duckdb`、`pg` 或 `trino`）。`--server-pid` 是服务器的进程 ID；运行会读取该进程及其子进程（工作进程）的 CPU 时间。它直接读取进程，因此仅当负载生成器运行在服务器所在主机上时才有效。

### 从另一台主机测量服务器的 CPU {: #measuring-the-servers-cpu-from-another-host }

可能时让负载生成器运行在自己的机器上：这样它不会占用服务器的核心。要从那里测量服务器的 CPU，请在服务器所在主机上启动 `server_cpu_agent.py`，并把它的 URL 而不是 `--server-pid` 提供给运行：

```bash
# on the server's host, from demo/named/perf/bench
../../../../.venv/bin/python3 server_cpu_agent.py --pid "$PROVISA_PID" --listen 10.0.0.5:9555

# on the load generator's host
../../../../.venv/bin/python3 run_benchmark.py --engine pg --optimistic \
  --setup acme.yaml --server-cpu-url http://10.0.0.5:9555/ --output-dir results/first
```

代理只回答一个数字，与 `--server-pid` 读取的相同：`GET /` 返回 `{"pid": ..., "cpu_seconds": ...}`，即服务器进程及其工作进程累计的 CPU 秒数。其他所有路径都返回 404，其他所有方法都会被拒绝。 每次运行在 `--server-pid` 与 `--server-cpu-url` 中只给一个，不要同时给两个。

`--listen` 为必填：代理没有默认地址。把它绑定到只有负载生成器能够访问的私有地址，绝不要绑定到公共接口，并在运行结束后停止它（Ctrl+C）。

### 2. 规划运行 {: #2-plan-the-runs }

```bash
../../../../.venv/bin/python3 sweeps.py plan --setup acme.yaml --out results/sizing \
  --resolved-names results/first/optimistic/resolved-names.json \
  --physical-cores 16 --shared-host yes \
  --workers 8,16,32 --request-threads 1,2,4 \
  --replication live,replica --ttl 300 \
  --engines pg,duckdb --engine-container pg=perf-engine-postgresql-1
```

- `--physical-cores`：服务器的物理核心数，而不是硬件线程数。在 Linux 上，`lscpu` 会显示每个插槽的核心数和插槽数；将两者相乘。工具从不猜测这个值。
- `--shared-host`：如果你的数据源或负载生成器运行在服务器所在主机上，则为 `yes`。它们会与 Provisa 争用其核心，指南会指出这一点。
- `--workers` 和 `--request-threads`：要尝试的工作进程数，以及每个工作进程的并发处理中请求数。两者要么都给，要么都不给。
- `--replication` 和 `--ttl`：为每个数据源添加实时运行和副本运行，副本每 `--ttl` 秒刷新一次。从副本读取的跨数据源联接运行也需要 `--ttl`。
- `--engines`：要测量的联邦引擎，默认为 `pg`。每个旋钮运行对每个引擎各规划一次；引擎在服务器启动时设定，因此每个引擎的运行都需要用该引擎重启服务器。
- `--engine-container`：对于在自己的容器中作为服务器运行的引擎，用 `ENGINE=CONTAINER`，之后每次运行都会报告其 CPU。在 Provisa 内部运行的引擎（DuckDB）则省略。

规划会为每次运行写出一份契约，并写出 `manifest.json`。部署无法服务的运行，例如经由 gRPC 的联接，或传输未公开的表，会连同原因列在清单中。它绝不会被悄悄丢弃。

副本运行会在无负载的窗口内测量副本的刷新成本。在规划它们之前，先在契约中设置 `load.idle_window_s`（例如 `120`）。否则报告会以 `no idle.json` 拒绝给出盈亏平衡点。

### 3. 运行每份已规划的契约 {: #3-run-every-planned-contract }

每个清单条目都指明其契约文件。把每一份运行到 `results/sizing/results/<run name>`：

```bash
../../../../.venv/bin/python3 run_benchmark.py --engine pg --optimistic \
  --setup results/sizing/graphql.sales-pg.filters.1.yaml \
  --resolved-names results/first/optimistic/resolved-names.json \
  --server-pid "$PROVISA_PID" --output-dir results/sizing/results/graphql.sales-pg.filters.1
```

从另一台主机运行时，用 `--server-cpu-url` 代替 `--server-pid`。工作进程运行会重启服务器，因此其进程 ID 会变化：也要用新的 `--pid` 重启代理。

对清单条目含有 `"apply_replication": true` 的每次运行（复制运行和跨数据源联接运行），都加上 `--apply-replication`。它会设定该运行的契约所声明的复制，并在运行后恢复原状。

每次运行在清单中都带有一个 `server_env`：服务器必须带着它运行。旋钮运行的是其引擎，`{"PROVISA_ENGINE": "duckdb"}`；先运行同一引擎的所有运行，再用下一个引擎重启服务器。工作进程运行的还要加上其工作进程数和并发处理中上限，例如 `{"PROVISA_WORKERS": "16", "PROVISA_REQUEST_THREADS": "2", "PROVISA_ENGINE": "pg"}`：`PROVISA_WORKERS` 是工作进程数，`PROVISA_REQUEST_THREADS` 设置 `concurrency.request_threads`，即每个工作进程同时处理的请求数。 在开始运行之前，等到每个工作进程都在提供服务（`/health` 报告 `workers.ready` 达到 `workers.expected`）。

### 4. 生成报告 {: #4-build-the-report }

```bash
../../../../.venv/bin/python3 bench_report.py --root results/sizing --out results/sizing/report.json
```

`--tolerance`（默认 `0.05`）设定工作进程设置要多接近最佳实测吞吐量才算足够。

### 5. 生成指南 {: #5-generate-the-guide }

```bash
../../../../.venv/bin/python3 sizing_guide.py --report results/sizing/report.json --out guide.md \
  --setup acme.yaml --resolved-names results/first/optimistic/resolved-names.json
```

没有 `--setup` 时，指南只含调优部分。有了它，指南会用 `--engine` 指定的引擎（默认 `pg`）为契约的 `workload` 做容量规划；运行未测量的引擎会被拒绝。

## 阅读指南 {: #read-the-guide }

### 为此工作负载做容量规划 {: #sizing-this-workload }

指南用一行结束本节，例如：**12 个物理核心、24 个工作进程、每个工作进程 2 个并发处理中请求。** 在它上方，一张表列出每个传输和数据源：请求占比、字段、过滤、行和联接的平均数、缓存命中的占比，以及一个请求的 CPU 成本。

未命中的请求，其成本取成本模型在各旋钮均值处的值；命中的请求，则取实测的命中成本。一个传输每秒的 Provisa CPU 除以一个物理核心的产出，即得到其物理核心数。指南把各传输相加并向上取整。工作进程数是核心数乘以任一传输所需的最大每核心工作进程数。每个工作进程的并发处理中请求数同样取任一传输所需的最大值。一项设置服务所有传输。

工具不会为它没有测量过的内容做容量规划。工作负载中运行没有模型的部分，会让规划停止，并给出指明它的消息：

- 运行未测量的引擎
- 没有工作进程运行的传输
- 在该传输上没有成本模型的数据源
- 运行没有变动过的旋钮（先扫描它，再重新运行）
- 数据源上的路由提示（`direct` 或 `federated`）
- 与所测量的不同的复制设置或 TTL
- 运行没有缓存数据时的缓存命中

### 工作进程与并发处理中请求 {: #worker-processes-and-in-flight-requests }

对每个传输，指南给出峰值吞吐量落在最佳值容差范围内的最少工作进程数。在该工作进程数下，再选出同样达标的每个工作进程最少并发处理中请求数。它用两种形式给出该设置：每个物理核心的工作进程数，以及每个工作进程的并发处理中请求数。两者任何一个再多都不会增加吞吐量；再少则会让核心闲置。表中还给出峰值每秒请求数及其 p99 延迟。其下列出了每个测量点。

容量系数等于一个物理核心每秒提供的请求数，乘以单独一个请求所需的 CPU，再除以 1000。高于 1 表示该核心处理的请求多于一个线程的量，当它的两个硬件线程都在服务时就是如此。

### 一个请求的成本 {: #what-a-request-costs }

对每个引擎、传输和数据源并排给出：无缓存请求的基础成本、每个字段、过滤、行和联接增加的成本，以及模型对测量值的拟合程度（R²，1 表示完美拟合）。跨数据源联接有三种成本，取决于其两端的读取方式：两端实时、两端来自副本，或各取其一。短横线表示运行没有改变该旋钮。在引擎作为独立服务器运行的地方，第二张表以同样的方式给出引擎的 CPU：由引擎服务的请求（例如跨数据源联接）会在那里显示其引擎时间。在修改你的请求之前，可以用这些数字了解修改会带来的成本：每个请求多两个字段，就会增加两倍的每字段成本。

### 响应缓存的启用 {: #the-response-cache-opt-in }

对每个传输和数据源：未命中的 CPU、命中的 CPU，以及命中节省的部分。请求只有在启用缓存并重复了较早的请求时才会从缓存应答，所以节省适用于重复的请求。请把重复的请求启用缓存。

### 实时读取还是副本读取，以及 TTL {: #live-or-replica-reads-and-the-ttl }

对每个有复制运行的引擎、传输和数据源：实时读取的成本、副本读取的成本、一次刷新的 CPU、TTL 以及盈亏平衡速率。高于盈亏平衡速率时，副本消耗的 CPU（Provisa、数据源和引擎合计）比实时读取更少。低于它则实时读取。第二张表按请求中的联接数给出跨数据源联接的同样数据：两端实时读取对比两端来自副本，其中刷新计入切换后改用副本的每个数据源。

盈亏平衡点是每秒刷新的 CPU 除以一次读取所节省的量：

```text
break-even req/s = (refresh CPU s × 1000 / TTL s) / (live ms − replica ms)
```

TTL 加倍会使盈亏平衡速率减半，而数据最多可能老一倍。根据你的用户能接受数据陈旧的程度来选择 TTL，然后将你在该数据源上的请求速率与该 TTL 下的盈亏平衡点比较。

副本读取的成本取决于存储及其索引，这些索引会在每次落地后不久构建。请在索引建好后再测量，更换存储后要重新运行。存储的选择和新鲜度策略见 [Replicas](replicas.md)。
