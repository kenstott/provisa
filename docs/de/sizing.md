# Dimensionierung und Tuning {: #sizing-and-tuning }

Das Dimensionierungswerkzeug misst Ihr eigenes Provisa-Deployment unter Ihrer eigenen Last. Aus diesen Messungen erstellt es einen Leitfaden zur Dimensionierung und zum Tuning: wie viele physische Kerne und Worker-Prozesse die Last braucht und was jede Einstellung bringt. Jede Zahl im Leitfaden stammt aus einer Messung; nach einer Änderung am Deployment oder an der Last führen Sie das Werkzeug daher erneut aus und erzeugen den Leitfaden neu.

Das Werkzeug liegt im Provisa-Repository unter `demo/named/perf/bench/`. Führen Sie jeden der folgenden Befehle aus diesem Verzeichnis mit der Python-Umgebung des Repositorys aus (`../../../../.venv/bin/python3`).

## Was es meldet {: #what-it-reports }

Ein einzelner Dimensionierungslauf besteht aus mehreren Benchmark-Läufen gegen Ihr Deployment. Jeder variiert eine Sache:

| Lauf | Was sich ändert | Was er liefert |
|---|---|---|
| Baseline | Nichts: eine Spalte, eine Zeile, kein Filter, kein Join, ungecacht | Die Grundkosten einer Anfrage |
| Stellschrauben-Läufe | Eine Anfrage-Stellschraube nach der anderen: Felder, Filter, Zeilen, Joins innerhalb einer Quelle, Joins über Quellen hinweg | Die Kosten, die jede Stellschraube pro Einheit hinzufügt |
| Läufe mit quellenübergreifenden Joins | Die Werte der quellenübergreifenden Joins, einmal mit beiden Enden live gelesen, einmal mit beiden aus Replikaten, einmal mit je einem von beiden | Die Kosten eines quellenübergreifenden Joins je nachdem, wie seine Enden gelesen werden, und der Break-even zwischen Live- und Replikat-Enden |
| Cache-Läufe | Jede Anfrage nimmt am Antwort-Cache teil, dann keine | Die Kosten eines Cache-Treffers und eines Fehlgriffs |
| Replikationsläufe | Eine Quelle live gelesen, dann aus einem Replikat | Die Rate, ab der ein Replikat weniger kostet |
| Worker-Läufe | Worker-Prozesse und laufende Anfragen pro Worker | Worker pro physischem Kern, laufende Anfragen pro Worker, was ein Kern leistet |

Stellschrauben-, Cache- und Replikationsläufe werden pro Transport und pro Quelle gemacht, sowie einmal pro Föderations-Engine, die Sie nennen. Worker-Läufe werden pro Transport gemacht. [tool-verified: `demo/named/perf/bench/sweeps.py:plan_sweeps,plan_worker_sweep`]

Kosten bedeutet hier die CPU-Zeit von Provisa pro Anfrage, in Millisekunden. Die Stellschrauben-Läufe sind ungecacht, daher enthalten die Kosten einer Stellschraube nie die Wirkung des Caches. [tool-verified: `demo/named/perf/bench/sweeps.py:_baseline` sets `k["cache"] = {"probability": 0.0}`]

Aus den Läufen passt `bench_report.py` pro Engine, Transport und Quelle ein Kostenmodell an: Grundkosten plus Kosten pro Feld, Filter, Zeile und Join, wobei ein quellenübergreifender Join danach bewertet wird, wie seine beiden Enden gelesen werden. Läuft die Engine als eigener Server und wird die CPU ihres Containers gemessen, erhält auch die Engine ein Kostenmodell. [tool-verified: `demo/named/perf/bench/cost_model.py:fit`] `sizing_guide.py` macht aus dem Bericht den Leitfaden. Mit einem Vertrag, der Ihre Last beschreibt, dimensioniert es auch diese Last.

## Einen Setup-Vertrag schreiben {: #write-a-setup-contract }

Der Setup-Vertrag ist eine einzelne YAML-Datei. Er nennt die Endpunkte Ihres Deployments, die Tabellen, die der Benchmark lesen darf, die Form der Anfragen und die Last. Der Benchmark prüft den Vertrag vor dem Lauf gegen das Deployment. Er lehnt eine Tabelle, Spalte oder einen Join ab, den das Deployment nicht bereitstellt. [tool-verified: `demo/named/perf/bench/setup_contract.py:setup_from_dict`]

Tabellen und Spalten werden so benannt, wie Ihr Deployment sie registriert. Wie jeder Transport sie nennt (SQL-Namen, GraphQL-Felder, Cypher-Labels, REST-Pfade, gRPC-Typen), wird beim Start des Laufs vom Deployment abgefragt.

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

Das Vertragsformat ist in `setup_contract.py` und `contract_parse.py` definiert; ein ungültiger Vertrag wird mit einer Meldung abgelehnt, die das Feld nennt. [tool-verified: `demo/named/perf/bench/contract_parse.py`, `SetupError`] Die Gewichte von `workload` müssen sich zu 1 summieren, und jeder darin genannte Transport muss einer sein, den der Vertrag ausführt. [tool-verified: `demo/named/perf/bench/contract_parse.py:_workload`]

Beschreiben Sie die Anfragen so, wie sie wirklich sind. Der Leitfaden dimensioniert genau die Anfragen, die dieser Vertrag erzeugt: Die Dimensionierung zieht sie aus demselben Generator mit festem Seed, den auch ein Lauf verwendet. [tool-verified: `demo/named/perf/bench/sizing.py:_transport` uses `request_mix.RequestGenerator`]

## Ausführen {: #run-it }

### 1. Die Namen einmal auflösen {: #1-resolve-the-names-once }

Führen Sie den Baseline-Vertrag einmal aus. Der Lauf fragt das Deployment, wie jeder Transport Ihre Tabellen nennt, und schreibt `resolved-names.json` neben seine Ergebnisse. [tool-verified: `demo/named/perf/bench/optimistic_load.py:resolve_names`]

```bash
../../../../.venv/bin/python3 run_benchmark.py --engine pg --optimistic \
  --setup acme.yaml --server-pid "$PROVISA_PID" --output-dir results/first
```

`--engine` nennt die Föderations-Engine, die Ihr Deployment ausführt (`duckdb`, `pg` oder `trino`). `--server-pid` ist die Prozess-ID des Servers; der Lauf liest die CPU-Zeit dieses Prozesses und seiner Kindprozesse (der Worker). Er liest den Prozess direkt und funktioniert daher nur, wenn der Lastgenerator auf dem Host des Servers läuft. [tool-verified: `demo/named/perf/bench/optimistic_load.py:_server_cpu_seconds`]

### Die CPU des Servers von einem anderen Host aus messen {: #measuring-the-servers-cpu-from-another-host }

Lassen Sie den Lastgenerator nach Möglichkeit auf einer eigenen Maschine laufen: Dann nimmt er dem Server keine Kerne weg. Um die CPU des Servers von dort aus zu messen, starten Sie `server_cpu_agent.py` auf dem Host des Servers und übergeben dem Lauf seine URL anstelle von `--server-pid`:

```bash
# on the server's host, from demo/named/perf/bench
../../../../.venv/bin/python3 server_cpu_agent.py --pid "$PROVISA_PID" --listen 10.0.0.5:9555

# on the load generator's host
../../../../.venv/bin/python3 run_benchmark.py --engine pg --optimistic \
  --setup acme.yaml --server-cpu-url http://10.0.0.5:9555/ --output-dir results/first
```

Der Agent beantwortet genau eine Zahl, dieselbe, die `--server-pid` liest: `GET /` liefert `{"pid": ..., "cpu_seconds": ...}`, die kumulierten CPU-Sekunden des Serverprozesses und seiner Worker. Jeder andere Pfad antwortet mit 404, und jede andere Methode wird abgelehnt. [tool-verified: `demo/named/perf/bench/server_cpu_agent.py:handler`] Geben Sie einem Lauf entweder `--server-pid` oder `--server-cpu-url`, nie beides. [tool-verified: `demo/named/perf/bench/optimistic_load.py:server_cpu_source`]

`--listen` ist erforderlich: Der Agent hat keine Standardadresse. Binden Sie ihn an eine private Adresse, die nur der Lastgenerator erreichen kann, nie an eine öffentliche Schnittstelle, und beenden Sie ihn (Strg+C), wenn die Läufe abgeschlossen sind.

### 2. Die Läufe planen {: #2-plan-the-runs }

```bash
../../../../.venv/bin/python3 sweeps.py plan --setup acme.yaml --out results/sizing \
  --resolved-names results/first/optimistic/resolved-names.json \
  --physical-cores 16 --shared-host yes \
  --workers 8,16,32 --request-threads 1,2,4 \
  --replication live,replica --ttl 300 \
  --engines pg,duckdb --engine-container pg=perf-engine-postgresql-1
```

- `--physical-cores`: die physischen Kerne des Servers, nicht seine Hardware-Threads. Unter Linux zeigt `lscpu` Kerne pro Sockel und Sockel; multiplizieren Sie beides. Das Werkzeug rät diesen Wert nie.
- `--shared-host`: `yes`, wenn Ihre Quellen oder der Lastgenerator auf dem Host des Servers laufen. Sie konkurrieren dann mit Provisa um dessen Kerne, und der Leitfaden weist darauf hin.
- `--workers` und `--request-threads`: die Worker-Prozesse und die laufenden Anfragen pro Worker, die ausprobiert werden sollen. Geben Sie beide an oder keinen.
- `--replication` und `--ttl`: fügen für jede Quelle die Live- und Replikat-Läufe hinzu, wobei das Replikat alle `--ttl` Sekunden aktualisiert wird. Auch die quellenübergreifenden Join-Läufe, die aus Replikaten lesen, brauchen `--ttl`.
- `--engines`: die Föderations-Engines, die gemessen werden sollen, standardmäßig `pg`. Jeder Stellschrauben-Lauf wird einmal pro Engine geplant; die Engine wird beim Start des Servers festgelegt, daher muss der Server für die Läufe jeder Engine mit ihr neu gestartet werden.
- `--engine-container`: `ENGINE=CONTAINER` für eine Engine, die als Server in einem eigenen Container läuft und deren CPU jeder Lauf dann meldet. Lassen Sie eine Engine weg, die innerhalb von Provisa läuft (DuckDB).

Der Plan schreibt einen Vertrag pro Lauf und eine `manifest.json`. Ein Lauf, den das Deployment nicht bedienen kann, etwa Joins über gRPC oder eine Tabelle, die ein Transport nicht bereitstellt, wird mit dem Grund im Manifest aufgeführt. Er wird nie stillschweigend verworfen. [tool-verified: `demo/named/perf/bench/sweeps.py:_validate`, `write_plan`]

Die Replikat-Läufe messen die Aktualisierungskosten des Replikats in einem Zeitfenster ohne Last. Setzen Sie `load.idle_window_s` in Ihrem Vertrag (zum Beispiel `120`), bevor Sie sie planen. Ohne diesen Wert lehnt der Bericht den Break-even mit `no idle.json` ab. [tool-verified: `demo/named/perf/bench/bench_report.py:_break_even`]

### 3. Jeden geplanten Vertrag ausführen {: #3-run-every-planned-contract }

Jeder Manifest-Eintrag nennt seine Vertragsdatei. Führen Sie jeden in `results/sizing/results/<run name>` aus:

```bash
../../../../.venv/bin/python3 run_benchmark.py --engine pg --optimistic \
  --setup results/sizing/graphql.sales-pg.filters.1.yaml \
  --resolved-names results/first/optimistic/resolved-names.json \
  --server-pid "$PROVISA_PID" --output-dir results/sizing/results/graphql.sales-pg.filters.1
```

Verwenden Sie von einem anderen Host aus `--server-cpu-url` anstelle von `--server-pid`. Ein Worker-Lauf startet den Server neu, daher ändert sich seine Prozess-ID: Starten Sie auch den Agenten mit der neuen `--pid` neu.

Fügen Sie `--apply-replication` zu jedem Lauf hinzu, dessen Manifest-Eintrag `"apply_replication": true` hat (die Replikationsläufe und die quellenübergreifenden Join-Läufe). Es setzt die Replikation, die der Vertrag des Laufs deklariert, und stellt sie danach wieder her. [tool-verified: `demo/named/perf/bench/optimistic_load.py:add_arguments`]

Jeder Lauf trägt im Manifest ein `server_env`: Der Server muss damit laufen. Bei einem Stellschrauben-Lauf ist das seine Engine, `{"PROVISA_ENGINE": "duckdb"}`; führen Sie alle Läufe einer Engine aus und starten Sie den Server dann mit der nächsten neu. Bei einem Worker-Lauf kommen seine Worker und die Obergrenze laufender Anfragen hinzu, zum Beispiel `{"PROVISA_WORKERS": "16", "PROVISA_REQUEST_THREADS": "2", "PROVISA_ENGINE": "pg"}`: `PROVISA_WORKERS` ist die Anzahl der Worker-Prozesse, und `PROVISA_REQUEST_THREADS` setzt `concurrency.request_threads`, die laufenden Anfragen, die jeder Worker gleichzeitig bearbeitet. [tool-verified: `provisa/core/settings_catalog.py` `concurrency.request_threads`, `env="PROVISA_REQUEST_THREADS"`] Warten Sie, bis jeder Worker bedient (`/health` meldet `workers.ready` von `workers.expected`), bevor Sie den Lauf starten.

### 4. Den Bericht erstellen {: #4-build-the-report }

```bash
../../../../.venv/bin/python3 bench_report.py --root results/sizing --out results/sizing/report.json
```

`--tolerance` (Standard `0.05`) legt fest, wie nah eine Worker-Einstellung am besten gemessenen Durchsatz liegen muss, um als ausreichend zu gelten. [tool-verified: `demo/named/perf/bench/bench_report.py:DEFAULT_TOLERANCE`]

### 5. Den Leitfaden erzeugen {: #5-generate-the-guide }

```bash
../../../../.venv/bin/python3 sizing_guide.py --report results/sizing/report.json --out guide.md \
  --setup acme.yaml --resolved-names results/first/optimistic/resolved-names.json
```

Ohne `--setup` enthält der Leitfaden nur die Tuning-Abschnitte. Mit `--setup` dimensioniert der Leitfaden die `workload` des Vertrags mit der Engine, die `--engine` nennt (standardmäßig `pg`); eine Engine, die die Läufe nicht gemessen haben, wird abgelehnt.

## Den Leitfaden lesen {: #read-the-guide }

### Diese Last dimensionieren {: #sizing-this-workload }

Der Leitfaden beendet diesen Abschnitt mit einer Zeile, zum Beispiel: **12 physical cores, 24 worker processes, 2 in-flight requests per worker.** Darüber zeigt eine Tabelle jeden Transport und jede Quelle: den Anteil der Anfragen, die mittleren Felder, Filter, Zeilen und Joins, den Anteil der Cache-Treffer und die CPU, die eine Anfrage kostet.

Eine Anfrage kostet bei einem Fehlgriff das Kostenmodell an den Mittelwerten ihrer Stellschrauben und bei einem Treffer die gemessenen Trefferkosten. Die Provisa-CPU eines Transports pro Sekunde, geteilt durch das, was ein physischer Kern leistet, ergibt seine physischen Kerne. Der Leitfaden addiert diese über die Transporte und rundet auf. Worker sind die Kerne mal die größte Worker-pro-Kern-Zahl, die einer der Transporte braucht. Die laufenden Anfragen pro Worker sind ebenso das Größte, was einer von ihnen braucht. Eine Einstellung bedient alle. [tool-verified: `demo/named/perf/bench/sizing.py:size`]

Das Werkzeug dimensioniert nichts, was es nicht gemessen hat. Ein Teil der Last, für den die Läufe kein Modell haben, stoppt die Dimensionierung mit einer Meldung, die ihn nennt [tool-verified: `demo/named/perf/bench/sizing.py:SizingRefused`]:

- eine Engine, die die Läufe nicht gemessen haben
- ein Transport ohne Worker-Läufe
- eine Quelle ohne Kostenmodell auf diesem Transport
- eine Stellschraube, die die Läufe nicht variiert haben (variieren Sie sie und führen Sie erneut aus)
- ein Routing-Hinweis (`direct` oder `federated`) an einer Quelle
- eine andere Replikationseinstellung oder TTL als die gemessene
- Cache-Treffer, wenn die Läufe keine Cache-Zahlen haben

### Worker-Prozesse und laufende Anfragen {: #worker-processes-and-in-flight-requests }

Für jeden Transport zeigt der Leitfaden die wenigsten Worker, deren Spitzendurchsatz innerhalb der Toleranz des besten liegt. Bei dieser Worker-Zahl wählt er dann die wenigsten laufenden Anfragen pro Worker, die dasselbe leisten. Er zeigt diese Einstellung in zwei Formen: Worker pro physischem Kern und laufende Anfragen pro Worker. Mehr von beidem bringt keinen zusätzlichen Durchsatz. Weniger lässt Kerne ungenutzt. Die Tabelle nennt außerdem die Spitzenzahl an Anfragen pro Sekunde und ihre p99-Latenz. Jeder gemessene Punkt ist darunter aufgeführt.

Der Kapazitätsfaktor sind die Anfragen pro Sekunde, die ein physischer Kern leistet, mal die CPU, die eine Anfrage für sich allein kostet, geteilt durch 1000. Über 1 leistet der Kern mehr als die Anfragen eines Threads, wie es der Fall ist, wenn seine Hardware-Threads beide bedienen. [tool-verified: `demo/named/perf/bench/bench_report.py:_server`]

### Was eine Anfrage kostet {: #what-a-request-costs }

Für jede Engine, jeden Transport und jede Quelle nebeneinander: die Grundkosten einer ungecachten Anfrage, die Kosten, die jedes Feld, jeder Filter, jede Zeile und jeder Join hinzufügt, und wie gut das Modell zu den Messungen passt (R², 1 ist eine perfekte Anpassung). Ein quellenübergreifender Join hat drei Kosten, je nachdem, wie seine beiden Enden gelesen werden: beide live, beide aus Replikaten oder eines von jedem. Ein Strich bedeutet, dass die Läufe diese Stellschraube nicht variiert haben. Läuft die Engine als eigener Server, gibt eine zweite Tabelle die CPU der Engine auf dieselbe Weise an: Eine Anfrage, die die Engine bedient, etwa ein quellenübergreifender Join, zeigt dort ihre Engine-Zeit. Anhand dieser Zahlen sehen Sie, was eine Änderung Ihrer Anfragen kosten würde, bevor Sie sie vornehmen: zwei Felder mehr in jeder Anfrage fügen das Doppelte der Kosten pro Feld hinzu.

### Das Opt-in für den Antwort-Cache {: #the-response-cache-opt-in }

Für jeden Transport und jede Quelle: die CPU eines Fehlgriffs, eines Treffers und was ein Treffer spart. Eine Anfrage wird nur dann aus dem Cache beantwortet, wenn sie sich dafür entscheidet und eine frühere Anfrage wiederholt; die Einsparung gilt also für wiederholte Anfragen. Aktivieren Sie das Opt-in für die wiederholten Anfragen. [tool-verified: `demo/named/perf/bench/bench_report.py:_cache`]

### Live- oder Replikat-Lesezugriffe und die TTL {: #live-or-replica-reads-and-the-ttl }

Für jede Engine, jeden Transport und jede Quelle mit Replikationsläufen: die Kosten eines Live-Lesezugriffs, eines Replikat-Lesezugriffs, die CPU einer Aktualisierung, die TTL und die Break-even-Rate. Oberhalb der Break-even-Rate kostet ein Replikat weniger CPU als das Lesen live, gerechnet als Provisa, Quelle und Engine zusammen. Darunter lesen Sie live. Eine zweite Tabelle gibt dasselbe für quellenübergreifende Joins an, nach der Zahl der Joins in der Anfrage: beide Enden live gelesen gegenüber beiden Enden aus Replikaten, wobei die Aktualisierung jede Quelle zählt, die der Wechsel auf ein Replikat umgestellt hat. [tool-verified: `demo/named/perf/bench/cost_model.py:break_even`]

Der Break-even ist die CPU der Aktualisierung pro Sekunde geteilt durch das, was ein Lesezugriff spart:

```text
break-even req/s = (refresh CPU s × 1000 / TTL s) / (live ms − replica ms)
```

Eine verdoppelte TTL halbiert die Break-even-Rate, und die Daten dürfen bis zu doppelt so alt sein. Wählen Sie die TTL danach, wie veraltet Ihre Benutzer die Daten akzeptieren können, und vergleichen Sie dann Ihre Anfragerate auf dieser Quelle mit dem Break-even bei dieser TTL.

Die Kosten eines Replikat-Lesezugriffs hängen vom Store und von seinen Indizes ab, die kurz nach jedem Landen aufgebaut werden. Messen Sie, nachdem die Indizes vorhanden sind, und führen Sie die Messung nach einem Wechsel des Stores erneut aus. [Replikate](replicas.md) behandelt die Wahl des Stores und die Aktualitätsstrategien.
