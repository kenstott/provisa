# 容量規劃與調校 {: #sizing-and-tuning }

容量規劃工具會在你自己的工作負載下，量度你自己的 Provisa 部署。它根據這些量度結果編寫一份容量規劃與調校指南：工作負載需要多少實體核心和工作行程，以及每項設定能帶來甚麼。指南中的每個數字都來自量度，因此在部署或工作負載有所變動後，須重新執行該工具並重新產生指南。

該工具位於 Provisa 儲存庫的 `demo/named/perf/bench/` 之下。下面的每條指令都要在該目錄中、使用儲存庫的 Python 環境（`../../../../.venv/bin/python3`）執行。

## 它報告甚麼 {: #what-it-reports }

一次容量規劃執行由針對你的部署的多次基準測試執行組成。每次執行只改變一件事：

| 執行 | 改變的內容 | 得到的結果 |
|---|---|---|
| 基線 | 無：一欄、一列、無篩選、無聯結、無快取 | 請求的基本成本 |
| 旋鈕執行 | 每次一個請求旋鈕：欄位、篩選、列數、同資料來源聯結、跨資料來源聯結 | 每個旋鈕按單位增加的成本 |
| 跨資料來源聯結執行 | 跨資料來源聯結的取值，一次兩端均即時讀取，一次兩端均來自複本，一次兩端各取其一 | 跨資料來源聯結的成本（按其兩端的讀取方式），以及即時端與複本端之間的損益平衡點 |
| 快取執行 | 每個請求都啟用回應快取，然後一個都不啟用 | 快取命中與未命中的成本 |
| 複製執行 | 資料來源先即時讀取，再從複本讀取 | 高於該速率時複本成本較低 |
| 工作行程執行 | 工作行程數，以及每個工作行程的並行處理中請求數 | 每個實體核心的工作行程數、每個工作行程的並行處理中請求數、一個核心能提供的能力 |

旋鈕執行、快取執行和複製執行按傳輸和資料來源進行，並對你指定的每個聯邦引擎各進行一次。工作行程執行按傳輸進行。[tool-verified: `demo/named/perf/bench/sweeps.py:plan_sweeps,plan_worker_sweep`]

這裡的成本指 Provisa 處理每個請求的 CPU 時間，單位為毫秒。旋鈕執行不使用快取，因此旋鈕的成本從不包含快取的影響。[tool-verified: `demo/named/perf/bench/sweeps.py:_baseline` sets `k["cache"] = {"probability": 0.0}`]

根據這些執行，`bench_report.py` 為每個引擎、傳輸和資料來源擬合一個成本模型：基本成本，加上每個欄位、篩選、列和聯結的成本，其中跨資料來源聯結按其兩端的讀取方式計成本。當引擎作為獨立伺服器執行且其容器的 CPU 被量度時，引擎也會得到一個成本模型。[tool-verified: `demo/named/perf/bench/cost_model.py:fit`] `sizing_guide.py` 把報告轉換為指南。如果給它一份描述你工作負載的合約，它還會為該工作負載做容量規劃。

## 編寫設定合約 {: #write-a-setup-contract }

設定合約是一個 YAML 檔案。它列出你的部署的端點、基準測試可以讀取的表、請求的形態以及負載。基準測試在執行前會對照部署檢查合約，並拒絕部署未公開的表、欄或聯結。[tool-verified: `demo/named/perf/bench/setup_contract.py:setup_from_dict`]

表和欄使用你的部署登記時的名稱。各傳輸如何稱呼它們（SQL 名稱、GraphQL 欄位、Cypher 標籤、REST 路徑、gRPC 類型），會在執行開始時從部署中查詢。

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

合約格式由 `setup_contract.py` 和 `contract_parse.py` 定義；無效的合約會被拒絕，並給出指明欄位的訊息。[tool-verified: `demo/named/perf/bench/contract_parse.py`, `SetupError`] `workload` 的權重總和必須為 1，且其中列出的每個傳輸都必須是合約會執行的傳輸。[tool-verified: `demo/named/perf/bench/contract_parse.py:_workload`]

按請求的真實情況來描述它們。指南恰好針對該合約產生的請求做容量規劃：規劃使用與執行相同的帶種子產生器來抽取請求。[tool-verified: `demo/named/perf/bench/sizing.py:_transport` uses `request_mix.RequestGenerator`]

## 執行 {: #run-it }

### 1. 一次性解析名稱 {: #1-resolve-the-names-once }

先執行一次基線合約。該執行會向部署查詢各傳輸如何稱呼你的表，並把 `resolved-names.json` 寫在其結果旁邊。[tool-verified: `demo/named/perf/bench/optimistic_load.py:resolve_names`]

```bash
../../../../.venv/bin/python3 run_benchmark.py --engine pg --optimistic \
  --setup acme.yaml --server-pid "$PROVISA_PID" --output-dir results/first
```

`--engine` 指明你的部署所執行的聯邦引擎（`duckdb`、`pg` 或 `trino`）。`--server-pid` 是伺服器的行程 ID；執行會讀取該行程及其子行程（工作行程）的 CPU 時間。它直接讀取行程，因此僅當負載產生器執行於伺服器所在主機上時才有效。[tool-verified: `demo/named/perf/bench/optimistic_load.py:_server_cpu_seconds`]

### 從另一部主機量度伺服器的 CPU {: #measuring-the-servers-cpu-from-another-host }

可能時讓負載產生器執行於自己的機器上：這樣它不會佔用伺服器的核心。要從那裡量度伺服器的 CPU，請在伺服器所在主機上啟動 `server_cpu_agent.py`，並把它的 URL 而不是 `--server-pid` 提供給執行：

```bash
# on the server's host, from demo/named/perf/bench
../../../../.venv/bin/python3 server_cpu_agent.py --pid "$PROVISA_PID" --listen 10.0.0.5:9555

# on the load generator's host
../../../../.venv/bin/python3 run_benchmark.py --engine pg --optimistic \
  --setup acme.yaml --server-cpu-url http://10.0.0.5:9555/ --output-dir results/first
```

代理只回答一個數字，與 `--server-pid` 讀取的相同：`GET /` 傳回 `{"pid": ..., "cpu_seconds": ...}`，即伺服器行程及其工作行程累計的 CPU 秒數。其他所有路徑都傳回 404，其他所有方法都會被拒絕。[tool-verified: `demo/named/perf/bench/server_cpu_agent.py:handler`] 每次執行在 `--server-pid` 與 `--server-cpu-url` 中只給一個，切勿同時給兩個。[tool-verified: `demo/named/perf/bench/optimistic_load.py:server_cpu_source`]

`--listen` 為必填：代理沒有預設位址。把它繫結到只有負載產生器能夠存取的私有位址，絕不要繫結到公共介面，並在執行結束後停止它（Ctrl+C）。

### 2. 規劃執行 {: #2-plan-the-runs }

```bash
../../../../.venv/bin/python3 sweeps.py plan --setup acme.yaml --out results/sizing \
  --resolved-names results/first/optimistic/resolved-names.json \
  --physical-cores 16 --shared-host yes \
  --workers 8,16,32 --request-threads 1,2,4 \
  --replication live,replica --ttl 300 \
  --engines pg,duckdb --engine-container pg=perf-engine-postgresql-1
```

- `--physical-cores`：伺服器的實體核心數，而不是硬件執行緒數。在 Linux 上，`lscpu` 會顯示每個插槽的核心數和插槽數；將兩者相乘。工具從不猜測這個值。
- `--shared-host`：如果你的資料來源或負載產生器執行於伺服器所在主機上，則為 `yes`。它們會與 Provisa 爭用其核心，指南會指出這一點。
- `--workers` 和 `--request-threads`：要嘗試的工作行程數，以及每個工作行程的並行處理中請求數。兩者要麼都給，要麼都不給。
- `--replication` 和 `--ttl`：為每個資料來源加入即時執行和複本執行，複本每 `--ttl` 秒刷新一次。從複本讀取的跨資料來源聯結執行也需要 `--ttl`。
- `--engines`：要量度的聯邦引擎，預設為 `pg`。每個旋鈕執行對每個引擎各規劃一次；引擎在伺服器啟動時設定，因此每個引擎的執行都需要以該引擎重新啟動伺服器。
- `--engine-container`：對於在自己的容器中作為伺服器執行的引擎，用 `ENGINE=CONTAINER`，之後每次執行都會報告其 CPU。在 Provisa 內部執行的引擎（DuckDB）則省略。

規劃會為每次執行寫出一份合約，並寫出 `manifest.json`。部署無法服務的執行，例如經由 gRPC 的聯結，或傳輸未公開的表，會連同原因列在清單中。它絕不會被悄悄丟棄。[tool-verified: `demo/named/perf/bench/sweeps.py:_validate`, `write_plan`]

複本執行會在無負載的時段內量度複本的刷新成本。在規劃它們之前，先在合約中設定 `load.idle_window_s`（例如 `120`）。否則報告會以 `no idle.json` 拒絕給出損益平衡點。[tool-verified: `demo/named/perf/bench/bench_report.py:_break_even`]

### 3. 執行每份已規劃的合約 {: #3-run-every-planned-contract }

每個清單項目都指明其合約檔案。把每一份執行到 `results/sizing/results/<run name>`：

```bash
../../../../.venv/bin/python3 run_benchmark.py --engine pg --optimistic \
  --setup results/sizing/graphql.sales-pg.filters.1.yaml \
  --resolved-names results/first/optimistic/resolved-names.json \
  --server-pid "$PROVISA_PID" --output-dir results/sizing/results/graphql.sales-pg.filters.1
```

從另一部主機執行時，用 `--server-cpu-url` 取代 `--server-pid`。工作行程執行會重新啟動伺服器，因此其行程 ID 會變化：也要用新的 `--pid` 重新啟動代理。

對清單項目含有 `"apply_replication": true` 的每次執行（複製執行和跨資料來源聯結執行），都加上 `--apply-replication`。它會設定該執行的合約所宣告的複製，並在執行後還原。[tool-verified: `demo/named/perf/bench/optimistic_load.py:add_arguments`]

每次執行在清單中都帶有一個 `server_env`：伺服器必須帶著它執行。旋鈕執行的是其引擎，`{"PROVISA_ENGINE": "duckdb"}`；先執行同一引擎的所有執行，再以下一個引擎重新啟動伺服器。工作行程執行的還要加上其工作行程數和並行處理中上限，例如 `{"PROVISA_WORKERS": "16", "PROVISA_REQUEST_THREADS": "2", "PROVISA_ENGINE": "pg"}`：`PROVISA_WORKERS` 是工作行程數，`PROVISA_REQUEST_THREADS` 設定 `concurrency.request_threads`，即每個工作行程同時處理的請求數。[tool-verified: `provisa/core/settings_catalog.py` `concurrency.request_threads`, `env="PROVISA_REQUEST_THREADS"`] 在開始執行之前，等到每個工作行程都在提供服務（`/health` 報告 `workers.ready` 達到 `workers.expected`）。

### 4. 建立報告 {: #4-build-the-report }

```bash
../../../../.venv/bin/python3 bench_report.py --root results/sizing --out results/sizing/report.json
```

`--tolerance`（預設 `0.05`）設定工作行程設定要多接近最佳實測吞吐量才算足夠。[tool-verified: `demo/named/perf/bench/bench_report.py:DEFAULT_TOLERANCE`]

### 5. 產生指南 {: #5-generate-the-guide }

```bash
../../../../.venv/bin/python3 sizing_guide.py --report results/sizing/report.json --out guide.md \
  --setup acme.yaml --resolved-names results/first/optimistic/resolved-names.json
```

沒有 `--setup` 時，指南只含調校部分。有了它，指南會用 `--engine` 指定的引擎（預設 `pg`）為合約的 `workload` 做容量規劃；執行未量度的引擎會被拒絕。

## 閱讀指南 {: #read-the-guide }

### 為此工作負載做容量規劃 {: #sizing-this-workload }

指南用一行結束本節，例如：**12 個實體核心、24 個工作行程、每個工作行程 2 個並行處理中請求。** 在它上方，一張表列出每個傳輸和資料來源：請求佔比、欄位、篩選、列和聯結的平均數、快取命中的佔比，以及一個請求的 CPU 成本。

未命中的請求，其成本取成本模型在各旋鈕平均值處的值；命中的請求，則取實測的命中成本。一個傳輸每秒的 Provisa CPU 除以一個實體核心的產出，即得到其實體核心數。指南把各傳輸相加並向上取整。工作行程數是核心數乘以任一傳輸所需的最大每核心工作行程數。每個工作行程的並行處理中請求數同樣取任一傳輸所需的最大值。一項設定服務所有傳輸。[tool-verified: `demo/named/perf/bench/sizing.py:size`]

工具不會為它沒有量度過的內容做容量規劃。工作負載中執行沒有模型的部分，會令規劃停止，並給出指明它的訊息 [tool-verified: `demo/named/perf/bench/sizing.py:SizingRefused`]：

- 執行未量度的引擎
- 沒有工作行程執行的傳輸
- 在該傳輸上沒有成本模型的資料來源
- 執行沒有變動過的旋鈕（先掃描它，再重新執行）
- 資料來源上的路由提示（`direct` 或 `federated`）
- 與所量度的不同的複製設定或 TTL
- 執行沒有快取數據時的快取命中

### 工作行程與並行處理中請求 {: #worker-processes-and-in-flight-requests }

對每個傳輸，指南給出峰值吞吐量落在最佳值容差範圍內的最少工作行程數。在該工作行程數下，再選出同樣達標的每個工作行程最少並行處理中請求數。它用兩種形式給出該設定：每個實體核心的工作行程數，以及每個工作行程的並行處理中請求數。兩者任何一個再多都不會增加吞吐量；再少則會令核心閒置。表中還給出峰值每秒請求數及其 p99 延遲。其下列出了每個量度點。

容量係數等於一個實體核心每秒提供的請求數，乘以單獨一個請求所需的 CPU，再除以 1000。高於 1 表示該核心處理的請求多於一個執行緒的量，當它的兩個硬件執行緒都在服務時就是如此。[tool-verified: `demo/named/perf/bench/bench_report.py:_server`]

### 一個請求的成本 {: #what-a-request-costs }

對每個引擎、傳輸和資料來源並排給出：無快取請求的基本成本、每個欄位、篩選、列和聯結增加的成本，以及模型對量度值的擬合程度（R²，1 表示完美擬合）。跨資料來源聯結有三種成本，取決於其兩端的讀取方式：兩端即時、兩端來自複本，或各取其一。短橫線表示執行沒有改變該旋鈕。在引擎作為獨立伺服器執行的地方，第二張表以同樣的方式給出引擎的 CPU：由引擎服務的請求（例如跨資料來源聯結）會在那裡顯示其引擎時間。在修改你的請求之前，可以用這些數字了解修改會帶來的成本：每個請求多兩個欄位，就會增加兩倍的每欄位成本。

### 回應快取的啟用 {: #the-response-cache-opt-in }

對每個傳輸和資料來源：未命中的 CPU、命中的 CPU，以及命中節省的部分。請求只有在啟用快取並重複了較早的請求時才會從快取應答，所以節省適用於重複的請求。請把重複的請求啟用快取。[tool-verified: `demo/named/perf/bench/bench_report.py:_cache`]

### 即時讀取還是複本讀取，以及 TTL {: #live-or-replica-reads-and-the-ttl }

對每個有複製執行的引擎、傳輸和資料來源：即時讀取的成本、複本讀取的成本、一次刷新的 CPU、TTL 以及損益平衡速率。高於損益平衡速率時，複本消耗的 CPU（Provisa、資料來源和引擎合計）比即時讀取更少。低於它則即時讀取。第二張表按請求中的聯結數給出跨資料來源聯結的同樣數據：兩端即時讀取對比兩端來自複本，其中刷新計入切換後改用複本的每個資料來源。[tool-verified: `demo/named/perf/bench/cost_model.py:break_even`]

損益平衡點是每秒刷新的 CPU 除以一次讀取所節省的量：

```text
break-even req/s = (refresh CPU s × 1000 / TTL s) / (live ms − replica ms)
```

TTL 加倍會令損益平衡速率減半，而數據最多可能老一倍。根據你的用戶能接受數據陳舊的程度來選擇 TTL，然後將你在該資料來源上的請求速率與該 TTL 下的損益平衡點比較。

複本讀取的成本取決於儲存區及其索引，這些索引會在每次落地後不久建構。請在索引建好後再量度，更換儲存區後要重新執行。儲存區的選擇和新鮮度策略見 [Replicas](replicas.md)。
