# Подбор ресурсов и настройка {: #sizing-and-tuning }

Инструмент подбора ресурсов измеряет ваше собственное развёртывание Provisa под вашей собственной нагрузкой. По результатам измерений он составляет руководство по подбору ресурсов и настройке: сколько физических ядер и рабочих процессов нужно нагрузке и что даёт каждая настройка. Каждая цифра в руководстве получена измерением, поэтому после изменения развёртывания или нагрузки инструмент нужно запустить заново и пересоздать руководство.

Инструмент находится в репозитории Provisa в каталоге `demo/named/perf/bench/`. Все команды ниже запускайте из этого каталога в окружении Python репозитория (`../../../../.venv/bin/python3`).

## Что он сообщает {: #what-it-reports }

Один прогон подбора ресурсов — это несколько прогонов бенчмарка против вашего развёртывания. В каждом меняется одна величина:

| Прогон | Что меняется | Что даёт |
|---|---|---|
| Базовый | Ничего: один столбец, одна строка, без фильтра, без соединения, без кэша | Базовая стоимость запроса |
| Прогоны параметров | Один параметр запроса за раз: поля, фильтры, строки, соединения в пределах источника, соединения между источниками | Стоимость, которую каждый параметр добавляет на единицу |
| Прогоны соединений между источниками | Значения соединений между источниками: один раз с обоими концами, читаемыми напрямую, один раз с обоими из реплик, один раз по одному каждого вида | Стоимость соединения между источниками в зависимости от способа чтения его концов и точка безубыточности между прямыми концами и концами из реплик |
| Прогоны кэша | Каждый запрос включён в кэш ответов, затем ни один | Стоимость попадания в кэш и промаха |
| Прогоны репликации | Источник читается напрямую, затем из реплики | Частота, выше которой реплика обходится дешевле |
| Прогоны рабочих процессов | Рабочие процессы и параллельные запросы на рабочий процесс | Рабочих процессов на физическое ядро, параллельных запросов на рабочий процесс, что даёт одно ядро |

Прогоны параметров, кэша и репликации выполняются для каждого транспорта и каждого источника, а также по одному разу для каждого названного вами движка федерации. Прогоны рабочих процессов выполняются для каждого транспорта. [tool-verified: `demo/named/perf/bench/sweeps.py:plan_sweeps,plan_worker_sweep`]

Стоимость здесь — это процессорное время Provisa на запрос в миллисекундах. Прогоны параметров идут без кэша, поэтому в стоимость параметра никогда не входит влияние кэша. [tool-verified: `demo/named/perf/bench/sweeps.py:_baseline` sets `k["cache"] = {"probability": 0.0}`]

По прогонам `bench_report.py` подбирает модель стоимости для каждого движка, транспорта и источника: базовая стоимость плюс стоимость на каждое поле, фильтр, строку и соединение, причём соединение между источниками оценивается по способу чтения двух его концов. Когда движок работает как отдельный сервер и измеряется процессор его контейнера, модель стоимости есть и у движка. [tool-verified: `demo/named/perf/bench/cost_model.py:fit`] `sizing_guide.py` превращает отчёт в руководство. Если дать ему контракт, описывающий вашу нагрузку, он подберёт ресурсы и для неё.

## Напишите контракт настройки {: #write-a-setup-contract }

Контракт настройки — это один файл YAML. В нём названы конечные точки вашего развёртывания, таблицы, которые бенчмарк может читать, форма запросов и нагрузка. Бенчмарк проверяет контракт на соответствие развёртыванию до запуска. Он отклоняет таблицу, столбец или соединение, которых развёртывание не предоставляет. [tool-verified: `demo/named/perf/bench/setup_contract.py:setup_from_dict`]

Таблицы и столбцы называются так, как их регистрирует ваше развёртывание. Как их называет каждый транспорт (имена SQL, поля GraphQL, метки Cypher, пути REST, типы gRPC), выясняется у развёртывания при запуске прогона.

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

Формат контракта определён в `setup_contract.py` и `contract_parse.py`; недопустимый контракт отклоняется сообщением с названием поля. [tool-verified: `demo/named/perf/bench/contract_parse.py`, `SetupError`] Веса в `workload` должны давать в сумме 1, а каждый названный в них транспорт должен быть одним из тех, что запускает контракт. [tool-verified: `demo/named/perf/bench/contract_parse.py:_workload`]

Описывайте запросы такими, какие они на самом деле. Руководство подбирает ресурсы ровно для тех запросов, которые порождает этот контракт: подбор берёт их из того же генератора с начальным значением, что и прогон. [tool-verified: `demo/named/perf/bench/sizing.py:_transport` uses `request_mix.RequestGenerator`]

## Запуск {: #run-it }

### 1. Однократно определите имена {: #1-resolve-the-names-once }

Один раз запустите базовый контракт. Прогон спрашивает у развёртывания, как каждый транспорт называет ваши таблицы, и записывает `resolved-names.json` рядом со своими результатами. [tool-verified: `demo/named/perf/bench/optimistic_load.py:resolve_names`]

```bash
../../../../.venv/bin/python3 run_benchmark.py --engine pg --optimistic \
  --setup acme.yaml --server-pid "$PROVISA_PID" --output-dir results/first
```

`--engine` называет движок федерации, который работает в вашем развёртывании (`duckdb`, `pg` или `trino`). `--server-pid` — идентификатор процесса сервера; прогон считывает процессорное время этого процесса и его дочерних процессов (рабочих процессов). Он читает процесс напрямую, поэтому это работает, только если генератор нагрузки запущен на хосте сервера. [tool-verified: `demo/named/perf/bench/optimistic_load.py:_server_cpu_seconds`]

### Измерение процессора сервера с другого хоста {: #measuring-the-servers-cpu-from-another-host }

По возможности запускайте генератор нагрузки на отдельной машине: тогда он не забирает ядра у сервера. Чтобы измерять процессор сервера оттуда, запустите `server_cpu_agent.py` на хосте сервера и передайте прогону его URL вместо `--server-pid`:

```bash
# on the server's host, from demo/named/perf/bench
../../../../.venv/bin/python3 server_cpu_agent.py --pid "$PROVISA_PID" --listen 10.0.0.5:9555

# on the load generator's host
../../../../.venv/bin/python3 run_benchmark.py --engine pg --optimistic \
  --setup acme.yaml --server-cpu-url http://10.0.0.5:9555/ --output-dir results/first
```

Агент отвечает одной цифрой, той же, что считывает `--server-pid`: `GET /` возвращает `{"pid": ..., "cpu_seconds": ...}` — накопленные секунды процессора серверного процесса и его рабочих процессов. Любой другой путь отвечает 404, а любой другой метод отклоняется. [tool-verified: `demo/named/perf/bench/server_cpu_agent.py:handler`] Передавайте прогону либо `--server-pid`, либо `--server-cpu-url`, но никогда оба. [tool-verified: `demo/named/perf/bench/optimistic_load.py:server_cpu_source`]

`--listen` обязателен: у агента нет адреса по умолчанию. Привяжите его к закрытому адресу, доступному только генератору нагрузки, и никогда к публичному интерфейсу, и остановите его (Ctrl+C), когда прогоны закончены.

### 2. Спланируйте прогоны {: #2-plan-the-runs }

```bash
../../../../.venv/bin/python3 sweeps.py plan --setup acme.yaml --out results/sizing \
  --resolved-names results/first/optimistic/resolved-names.json \
  --physical-cores 16 --shared-host yes \
  --workers 8,16,32 --request-threads 1,2,4 \
  --replication live,replica --ttl 300 \
  --engines pg,duckdb --engine-container pg=perf-engine-postgresql-1
```

- `--physical-cores`: физические ядра сервера, а не его аппаратные потоки. В Linux `lscpu` показывает ядра на сокет и сокеты; перемножьте их. Инструмент никогда не угадывает это значение.
- `--shared-host`: `yes`, если ваши источники или генератор нагрузки работают на хосте сервера. Тогда они конкурируют с Provisa за его ядра, и руководство об этом сообщает.
- `--workers` и `--request-threads`: рабочие процессы и параллельные запросы на рабочий процесс, которые нужно попробовать. Укажите оба или ни одного.
- `--replication` и `--ttl`: добавляют прямые прогоны и прогоны из реплик для каждого источника, причём реплика обновляется каждые `--ttl` секунд. Прогонам соединений между источниками, читающим из реплик, тоже нужен `--ttl`.
- `--engines`: движки федерации для измерения, по умолчанию `pg`. Каждый прогон параметров планируется по одному разу на движок; движок задаётся при запуске сервера, поэтому для прогонов каждого движка сервер нужно перезапускать с ним.
- `--engine-container`: `ENGINE=CONTAINER` для движка, работающего как сервер в собственном контейнере, процессор которого каждый прогон затем сообщает. Не указывайте движок, работающий внутри Provisa (DuckDB).

План записывает по одному контракту на прогон и `manifest.json`. Прогон, который развёртывание обслужить не может, например соединения через gRPC или таблица, которую транспорт не предоставляет, указывается в манифесте с причиной. Он никогда не отбрасывается молча. [tool-verified: `demo/named/perf/bench/sweeps.py:_validate`, `write_plan`]

Прогоны реплик измеряют стоимость обновления реплики в окне без нагрузки. Задайте `load.idle_window_s` в контракте (например, `120`) до того, как планировать их. Без этого отчёт отклоняет точку безубыточности с сообщением `no idle.json`. [tool-verified: `demo/named/perf/bench/bench_report.py:_break_even`]

### 3. Запустите каждый запланированный контракт {: #3-run-every-planned-contract }

Каждая запись манифеста называет свой файл контракта. Запустите каждый в `results/sizing/results/<run name>`:

```bash
../../../../.venv/bin/python3 run_benchmark.py --engine pg --optimistic \
  --setup results/sizing/graphql.sales-pg.filters.1.yaml \
  --resolved-names results/first/optimistic/resolved-names.json \
  --server-pid "$PROVISA_PID" --output-dir results/sizing/results/graphql.sales-pg.filters.1
```

С другого хоста используйте `--server-cpu-url` вместо `--server-pid`. Прогон рабочих процессов перезапускает сервер, поэтому его идентификатор процесса меняется: перезапустите агент и с новым `--pid`.

Добавьте `--apply-replication` к каждому прогону, у записи манифеста которого есть `"apply_replication": true` (прогоны репликации и прогоны соединений между источниками). Он задаёт репликацию, которую объявляет контракт прогона, и восстанавливает прежнюю после него. [tool-verified: `demo/named/perf/bench/optimistic_load.py:add_arguments`]

Каждый прогон несёт в манифесте `server_env`: сервер должен работать с ним. Для прогона параметров это его движок, `{"PROVISA_ENGINE": "duckdb"}`; выполните все прогоны одного движка, затем перезапустите сервер со следующим. К `server_env` прогона рабочих процессов добавляются его рабочие процессы и предел параллельных запросов, например `{"PROVISA_WORKERS": "16", "PROVISA_REQUEST_THREADS": "2", "PROVISA_ENGINE": "pg"}`: `PROVISA_WORKERS` — число рабочих процессов, а `PROVISA_REQUEST_THREADS` задаёт `concurrency.request_threads`, число запросов, которые каждый рабочий процесс выполняет одновременно. [tool-verified: `provisa/core/settings_catalog.py` `concurrency.request_threads`, `env="PROVISA_REQUEST_THREADS"`] Дождитесь, пока обслуживает каждый рабочий процесс (`/health` сообщает `workers.ready` из `workers.expected`), прежде чем начинать прогон.

### 4. Постройте отчёт {: #4-build-the-report }

```bash
../../../../.venv/bin/python3 bench_report.py --root results/sizing --out results/sizing/report.json
```

`--tolerance` (по умолчанию `0.05`) задаёт, насколько близко к лучшей измеренной пропускной способности должна подойти настройка рабочих процессов, чтобы считаться достаточной. [tool-verified: `demo/named/perf/bench/bench_report.py:DEFAULT_TOLERANCE`]

### 5. Создайте руководство {: #5-generate-the-guide }

```bash
../../../../.venv/bin/python3 sizing_guide.py --report results/sizing/report.json --out guide.md \
  --setup acme.yaml --resolved-names results/first/optimistic/resolved-names.json
```

Без `--setup` в руководстве есть только разделы по настройке. С ним руководство подбирает ресурсы для `workload` контракта с движком, названным в `--engine` (по умолчанию `pg`); движок, которого нет среди измеренных прогонами, отклоняется.

## Чтение руководства {: #read-the-guide }

### Подбор ресурсов для этой нагрузки {: #sizing-this-workload }

Руководство завершает этот раздел одной строкой, например: **12 физических ядер, 24 рабочих процесса, 2 параллельных запроса на рабочий процесс.** Над ней таблица показывает каждый транспорт и источник: долю запросов, средние числа полей, фильтров, строк и соединений, долю попаданий в кэш и процессорное время одного запроса.

Запрос стоит столько, сколько даёт модель стоимости при средних значениях его параметров при промахе, и столько, сколько измерено для попадания при попадании. Процессорное время Provisa в секунду для транспорта, делённое на то, что даёт одно физическое ядро, даёт его физические ядра. Руководство складывает их по транспортам и округляет вверх. Рабочие процессы — это ядра, умноженные на наибольшее число рабочих процессов на ядро, которое нужно любому из транспортов. Число параллельных запросов на рабочий процесс — тоже наибольшее из требуемых любым из них. Одна настройка обслуживает все. [tool-verified: `demo/named/perf/bench/sizing.py:size`]

Инструмент не подбирает ресурсы для того, чего не измерял. Часть нагрузки, для которой у прогонов нет модели, останавливает подбор сообщением с её названием [tool-verified: `demo/named/perf/bench/sizing.py:SizingRefused`]:

- движок, которого нет среди измеренных прогонами
- транспорт без прогонов рабочих процессов
- источник без модели стоимости на этом транспорте
- параметр, который прогоны не меняли (прогоните его и запустите заново)
- подсказка маршрута (`direct` или `federated`) на источнике
- настройка репликации или TTL, отличные от измеренных
- попадания в кэш, когда у прогонов нет цифр по кэшу

### Рабочие процессы и параллельные запросы {: #worker-processes-and-in-flight-requests }

Для каждого транспорта руководство показывает наименьшее число рабочих процессов, пиковая пропускная способность которых укладывается в допуск относительно лучшей. При этом числе рабочих процессов оно затем выбирает наименьшее число параллельных запросов на рабочий процесс, дающее то же. Эта настройка показана в двух видах: рабочих процессов на физическое ядро и параллельных запросов на рабочий процесс. Больше любого из них пропускной способности не добавляет. Меньше — ядра простаивают. В таблице также указаны пиковое число запросов в секунду и задержка p99. Под ней перечислены все измеренные точки.

Коэффициент ёмкости равен числу запросов в секунду, которое даёт одно физическое ядро, умноженному на процессорное время, которое запрос стоит сам по себе, делённому на 1000. Выше 1 ядро выполняет больше запросов, чем один поток, как это бывает, когда обслуживают оба его аппаратных потока. [tool-verified: `demo/named/perf/bench/bench_report.py:_server`]

### Во что обходится запрос {: #what-a-request-costs }

Для каждого движка, транспорта и источника рядом показаны: базовая стоимость запроса без кэша, стоимость, добавляемая каждым полем, фильтром, строкой и соединением, и то, насколько хорошо модель соответствует измерениям (R², 1 — идеальное соответствие). У соединения между источниками три стоимости в зависимости от способа чтения двух его концов: оба напрямую, оба из реплик или по одному каждого вида. Прочерк означает, что прогоны этот параметр не меняли. Там, где движок работает как отдельный сервер, вторая таблица так же показывает процессорное время движка: запрос, который обслуживает движок, например соединение между источниками, показывает там своё время движка. Используйте эти цифры, чтобы увидеть, чего стоило бы изменение ваших запросов, ещё до того, как вы его сделаете: ещё два поля в каждом запросе добавляют удвоенную стоимость поля.

### Включение в кэш ответов {: #the-response-cache-opt-in }

Для каждого транспорта и источника: процессорное время промаха, попадания и то, что экономит попадание. Запрос отвечается из кэша, только если он включён в кэш и повторяет более ранний запрос, поэтому экономия относится к повторяющимся запросам. Включайте в кэш повторяющиеся. [tool-verified: `demo/named/perf/bench/bench_report.py:_cache`]

### Прямое чтение или из реплики и TTL {: #live-or-replica-reads-and-the-ttl }

Для каждого движка, транспорта и источника с прогонами репликации: стоимость прямого чтения, чтения из реплики, процессорное время одного обновления, TTL и частота безубыточности. Выше частоты безубыточности реплика обходится по процессору дешевле, чем прямое чтение, считая вместе процессор Provisa, источника и движка. Ниже неё читайте напрямую. Вторая таблица даёт то же для соединений между источниками по числу соединений в запросе: оба конца читаются напрямую против обоих из реплик, причём обновление учитывает каждый источник, который переключение перевело на реплику. [tool-verified: `demo/named/perf/bench/cost_model.py:break_even`]

Безубыточность — это процессорное время обновления в секунду, делённое на то, что экономит одно чтение:

```text
break-even req/s = (refresh CPU s × 1000 / TTL s) / (live ms − replica ms)
```

Удвоение TTL вдвое снижает частоту безубыточности, а данные могут быть вдвое старше. Выберите TTL по тому, насколько устаревшими ваши пользователи готовы принимать данные, затем сравните частоту ваших запросов к этому источнику с безубыточностью при этом TTL.

Стоимость чтения из реплики зависит от хранилища и от его индексов, которые строятся вскоре после каждой загрузки. Измеряйте после того, как индексы построены, и повторяйте прогон после смены хранилища. Выбор хранилища и стратегии актуальности описаны в разделе [Replicas](replicas.md).
