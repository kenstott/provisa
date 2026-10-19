# Dimensionamento e tuning {: #sizing-and-tuning }

Lo strumento di dimensionamento misura la tua distribuzione di Provisa sotto il tuo carico di lavoro. Da queste misurazioni scrive una guida al dimensionamento e al tuning: di quanti core fisici e processi worker ha bisogno il carico e che cosa ottiene ciascuna impostazione. Ogni cifra della guida deriva da una misurazione, quindi dopo una modifica alla distribuzione o al carico si riesegue lo strumento e si rigenera la guida.

Lo strumento si trova nel repository di Provisa sotto `demo/named/perf/bench/`. Esegui ogni comando seguente da quella directory con l'ambiente Python del repository (`../../../../.venv/bin/python3`).

## Che cosa riporta {: #what-it-reports }

Una singola esecuzione di dimensionamento è composta da più esecuzioni di benchmark sulla tua distribuzione. Ognuna varia una sola cosa:

| Esecuzione | Che cosa cambia | Che cosa fornisce |
|---|---|---|
| Baseline | Nulla: una colonna, una riga, nessun filtro, nessun join, senza cache | Il costo di base di una richiesta |
| Esecuzioni per parametro | Un parametro di richiesta alla volta: campi, filtri, righe, join sulla stessa origine, join tra origini | Il costo che ciascun parametro aggiunge per unità |
| Esecuzioni di join tra origini | I valori del join tra origini, una volta con entrambi i lati letti dal vivo, una con entrambi da repliche, una con uno per tipo | Il costo di un join tra origini in base a come sono letti i suoi lati e il punto di pareggio tra lati live e lati replica |
| Esecuzioni della cache | Ogni richiesta aderisce alla cache delle risposte, poi nessuna | Il costo di un hit e di un miss della cache |
| Esecuzioni di replicazione | Un'origine letta dal vivo, poi da una replica | La frequenza oltre la quale una replica costa meno |
| Esecuzioni dei worker | Processi worker e richieste in volo per worker | Worker per core fisico, richieste in volo per worker, quanto fornisce un core |

Le esecuzioni per parametro, della cache e di replicazione sono eseguite per trasporto e per origine, e una volta per ogni motore di federazione indicato. Le esecuzioni dei worker sono eseguite per trasporto.

Per costo si intende qui il tempo di CPU di Provisa per richiesta, in millisecondi. Le esecuzioni per parametro non usano la cache, quindi il costo di un parametro non include mai l'effetto della cache.

Dalle esecuzioni, `bench_report.py` adatta un modello di costo per motore, trasporto e origine: un costo di base più un costo per campo, filtro, riga e join, con un join tra origini costato in base a come sono letti i suoi due lati. Quando il motore gira come server a sé e la CPU del suo container è misurata, anche il motore ha un modello di costo. `sizing_guide.py` trasforma il report nella guida. Dato un contratto che descrive il tuo carico, dimensiona anche quel carico.

## Scrivere un contratto di setup {: #write-a-setup-contract }

Il contratto di setup è un unico file YAML. Indica gli endpoint della tua distribuzione, le tabelle che il benchmark può leggere, la forma delle richieste e il carico. Il benchmark verifica il contratto rispetto alla distribuzione prima di partire. Rifiuta una tabella, una colonna o un join che la distribuzione non espone.

Tabelle e colonne sono indicate come la tua distribuzione le registra. Come le chiama ciascun trasporto (nomi SQL, campi GraphQL, etichette Cypher, percorsi REST, tipi gRPC) viene ricavato dalla distribuzione all'avvio dell'esecuzione.

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

Il formato del contratto è definito da `setup_contract.py` e `contract_parse.py`; un contratto non valido viene rifiutato con un messaggio che nomina il campo. I pesi di `workload` devono sommare a 1 e ogni trasporto che nominano deve essere uno di quelli che il contratto esegue.

Descrivi le richieste come sono davvero. La guida dimensiona esattamente le richieste che questo contratto genera: il dimensionamento le estrae dallo stesso generatore con seme che usa un'esecuzione.

## Eseguirlo {: #run-it }

### 1. Risolvere i nomi una volta {: #1-resolve-the-names-once }

Esegui una volta il contratto baseline. L'esecuzione chiede alla distribuzione come ciascun trasporto chiama le tue tabelle e scrive `resolved-names.json` accanto ai suoi risultati.

```bash
../../../../.venv/bin/python3 run_benchmark.py --engine pg --optimistic \
  --setup acme.yaml --server-pid "$PROVISA_PID" --output-dir results/first
```

`--engine` indica il motore di federazione che la tua distribuzione esegue (`duckdb`, `pg` o `trino`). `--server-pid` è l'id di processo del server; l'esecuzione legge il tempo di CPU di quel processo e dei suoi processi figli (i worker). Legge il processo direttamente, quindi funziona solo quando il generatore di carico gira sullo stesso host del server.

### Misurare la CPU del server da un altro host {: #measuring-the-servers-cpu-from-another-host }

Quando puoi, esegui il generatore di carico su una macchina separata: così non sottrae core al server. Per misurare da lì la CPU del server, avvia `server_cpu_agent.py` sull'host del server e passa all'esecuzione il suo URL al posto di `--server-pid`:

```bash
# on the server's host, from demo/named/perf/bench
../../../../.venv/bin/python3 server_cpu_agent.py --pid "$PROVISA_PID" --listen 10.0.0.5:9555

# on the load generator's host
../../../../.venv/bin/python3 run_benchmark.py --engine pg --optimistic \
  --setup acme.yaml --server-cpu-url http://10.0.0.5:9555/ --output-dir results/first
```

L'agente risponde con una sola cifra, la stessa che legge `--server-pid`: `GET /` restituisce `{"pid": ..., "cpu_seconds": ...}`, i secondi di CPU cumulativi del processo server e dei suoi worker. Ogni altro percorso risponde 404 e ogni altro metodo viene rifiutato. Passa a un'esecuzione uno solo tra `--server-pid` e `--server-cpu-url`, mai entrambi.

`--listen` è obbligatorio: l'agente non ha un indirizzo predefinito. Associalo a un indirizzo privato raggiungibile solo dal generatore di carico, mai a un'interfaccia pubblica, e fermalo (Ctrl+C) quando le esecuzioni sono finite.

### 2. Pianificare le esecuzioni {: #2-plan-the-runs }

```bash
../../../../.venv/bin/python3 sweeps.py plan --setup acme.yaml --out results/sizing \
  --resolved-names results/first/optimistic/resolved-names.json \
  --physical-cores 16 --shared-host yes \
  --workers 8,16,32 --request-threads 1,2,4 \
  --replication live,replica --ttl 300 \
  --engines pg,duckdb --engine-container pg=perf-engine-postgresql-1
```

- `--physical-cores`: i core fisici del server, non i suoi thread hardware. Su Linux, `lscpu` mostra i core per socket e i socket; moltiplicali. Lo strumento non lo deduce mai.
- `--shared-host`: `yes` quando le tue origini o il generatore di carico girano sull'host del server. Competono allora con Provisa per i suoi core, e la guida lo segnala.
- `--workers` e `--request-threads`: i processi worker e le richieste in volo per worker da provare. Indica entrambi o nessuno.
- `--replication` e `--ttl`: aggiungono le esecuzioni live e replica per ogni origine, con la replica aggiornata ogni `--ttl` secondi. Anche le esecuzioni di join tra origini che leggono da repliche richiedono `--ttl`.
- `--engines`: i motori di federazione da misurare, `pg` per default. Ogni esecuzione per parametro è pianificata una volta per motore; il motore si imposta all'avvio del server, quindi le esecuzioni di ciascun motore richiedono di riavviare il server con quel motore.
- `--engine-container`: `ENGINE=CONTAINER` per un motore che gira come server nel proprio container, la cui CPU ogni esecuzione riporta. Ometti un motore che gira dentro Provisa (DuckDB).

Il piano scrive un contratto per esecuzione e un `manifest.json`. Un'esecuzione che la distribuzione non può servire, come join su gRPC o una tabella che un trasporto non espone, è elencata nel manifest con il motivo. Non viene mai scartata in silenzio.

Le esecuzioni con replica misurano il costo di aggiornamento della replica durante una finestra senza carico. Imposta `load.idle_window_s` nel tuo contratto (per esempio `120`) prima di pianificarle. Senza, il report rifiuta il punto di pareggio con `no idle.json`.

### 3. Eseguire ogni contratto pianificato {: #3-run-every-planned-contract }

Ogni voce del manifest indica il proprio file di contratto. Esegui ciascuno in `results/sizing/results/<run name>`:

```bash
../../../../.venv/bin/python3 run_benchmark.py --engine pg --optimistic \
  --setup results/sizing/graphql.sales-pg.filters.1.yaml \
  --resolved-names results/first/optimistic/resolved-names.json \
  --server-pid "$PROVISA_PID" --output-dir results/sizing/results/graphql.sales-pg.filters.1
```

Da un altro host, usa `--server-cpu-url` al posto di `--server-pid`. Un'esecuzione dei worker riavvia il server, quindi il suo id di processo cambia: riavvia anche l'agente con il nuovo `--pid`.

Aggiungi `--apply-replication` a ogni esecuzione la cui voce del manifest ha `"apply_replication": true` (le esecuzioni di replicazione e quelle di join tra origini). Imposta la replicazione dichiarata dal contratto dell'esecuzione e la ripristina al termine.

Ogni esecuzione porta un `server_env` nel manifest: il server deve girare con esso. Quello di un'esecuzione per parametro è il suo motore, `{"PROVISA_ENGINE": "duckdb"}`; esegui tutte le esecuzioni di un motore, poi riavvia il server con il successivo. Quello di un'esecuzione dei worker aggiunge i suoi worker e il limite di richieste in volo, per esempio `{"PROVISA_WORKERS": "16", "PROVISA_REQUEST_THREADS": "2", "PROVISA_ENGINE": "pg"}`: `PROVISA_WORKERS` è il numero di processi worker e `PROVISA_REQUEST_THREADS` imposta `concurrency.request_threads`, le richieste in volo che ogni worker esegue contemporaneamente. Attendi che ogni worker sia in servizio (`/health` riporta `workers.ready` su `workers.expected`) prima di avviare l'esecuzione.

### 4. Costruire il report {: #4-build-the-report }

```bash
../../../../.venv/bin/python3 bench_report.py --root results/sizing --out results/sizing/report.json
```

`--tolerance` (default `0.05`) stabilisce quanto vicina alla migliore velocità di elaborazione misurata deve arrivare un'impostazione dei worker per essere considerata sufficiente.

### 5. Generare la guida {: #5-generate-the-guide }

```bash
../../../../.venv/bin/python3 sizing_guide.py --report results/sizing/report.json --out guide.md \
  --setup acme.yaml --resolved-names results/first/optimistic/resolved-names.json
```

Senza `--setup` la guida contiene solo le sezioni di tuning. Con esso, la guida dimensiona il `workload` del contratto con il motore indicato da `--engine` (`pg` per default); un motore che le esecuzioni non hanno misurato viene rifiutato.

## Leggere la guida {: #read-the-guide }

### Dimensionare questo carico {: #sizing-this-workload }

La guida chiude questa sezione con una riga, per esempio: **12 core fisici, 24 processi worker, 2 richieste in volo per worker.** Sopra, una tabella mostra ogni trasporto e origine: la quota di richieste, la media di campi, filtri, righe e join, la quota di cache hit e la CPU che costa una richiesta.

Una richiesta costa il modello di costo alle medie dei suoi parametri in caso di miss e il costo di hit misurato in caso di hit. La CPU di Provisa al secondo di un trasporto, divisa per quanto fornisce un core fisico, dà i suoi core fisici. La guida li somma tra i trasporti e arrotonda per eccesso. I worker sono i core per il maggior numero di worker per core richiesto da uno qualsiasi dei trasporti. Le richieste in volo per worker sono allo stesso modo il massimo richiesto da uno qualsiasi. Un'unica impostazione serve tutti.

Lo strumento non dimensiona nulla che non abbia misurato. Una parte del carico per cui le esecuzioni non hanno un modello interrompe il dimensionamento con un messaggio che la nomina:

- un motore che le esecuzioni non hanno misurato
- un trasporto senza esecuzioni dei worker
- un'origine senza modello di costo su quel trasporto
- un parametro che le esecuzioni non hanno variato (variarlo, poi rieseguire)
- un suggerimento di instradamento (`direct` o `federated`) su un'origine
- un'impostazione di replicazione o un TTL diversi da quelli misurati
- cache hit quando le esecuzioni non hanno cifre sulla cache

### Processi worker e richieste in volo {: #worker-processes-and-in-flight-requests }

Per ogni trasporto, la guida mostra il minor numero di worker il cui picco di velocità di elaborazione rientra nella tolleranza rispetto al migliore. A quel numero di worker sceglie poi il minor numero di richieste in volo per worker che fa lo stesso. Mostra questa impostazione in due forme: worker per core fisico e richieste in volo per worker. Averne di più non porta velocità di elaborazione. Averne meno lascia core inattivi. La tabella riporta anche il picco di richieste al secondo e la sua latenza p99. Ogni punto misurato è elencato sotto.

Il fattore di capacità è il numero di richieste al secondo che un core fisico fornisce, per la CPU che una richiesta costa da sola, diviso 1000. Sopra 1 il core fa più del lavoro di un thread, come accade quando entrambi i suoi thread hardware servono.

### Quanto costa una richiesta {: #what-a-request-costs }

Per ogni motore, trasporto e origine, affiancati: il costo di base di una richiesta senza cache, il costo che ogni campo, filtro, riga e join aggiunge e quanto bene il modello si adatta alle misurazioni (R², dove 1 è un adattamento perfetto). Un join tra origini ha tre costi, in base a come sono letti i suoi due lati: entrambi live, entrambi da repliche o uno per tipo. Un trattino significa che le esecuzioni non hanno variato quel parametro. Dove il motore gira come server a sé, una seconda tabella riporta allo stesso modo la CPU del motore: una richiesta servita dal motore, come un join tra origini, vi mostra il proprio tempo motore. Usa queste cifre per vedere quanto costerebbe una modifica delle tue richieste prima di farla: due campi in più per ogni richiesta aggiungono il doppio del costo per campo.

### L'adesione alla cache delle risposte {: #the-response-cache-opt-in }

Per ogni trasporto e origine: la CPU di un miss, di un hit e ciò che un hit risparmia. Una richiesta è servita dalla cache solo quando aderisce e ripete una richiesta precedente, quindi il risparmio vale per le richieste ripetute. Fai aderire quelle ripetute.

### Letture live o da replica, e il TTL {: #live-or-replica-reads-and-the-ttl }

Per ogni motore, trasporto e origine con esecuzioni di replicazione: il costo di una lettura live, di una lettura da replica, la CPU di un aggiornamento, il TTL e la frequenza di pareggio. Sopra la frequenza di pareggio una replica costa meno CPU, quella di Provisa, della sorgente e del motore insieme, della lettura live. Sotto, leggi live. Una seconda tabella riporta lo stesso per i join tra origini, in base al numero di join nella richiesta: entrambi i lati letti live contro entrambi da repliche, con l'aggiornamento che conta ogni origine che il passaggio ha spostato su una replica.

Il pareggio è la CPU di aggiornamento al secondo divisa per quanto risparmia una lettura:

```text
break-even req/s = (refresh CPU s × 1000 / TTL s) / (live ms − replica ms)
```

Raddoppiare il TTL dimezza la frequenza di pareggio, e i dati possono essere vecchi fino al doppio. Scegli il TTL in base a quanto vecchi gli utenti accettano che siano i dati, poi confronta la frequenza delle tue richieste su quell'origine con il pareggio a quel TTL.

Il costo di una lettura da replica dipende dallo store e dai suoi indici, costruiti poco dopo ogni caricamento. Misura dopo che gli indici sono presenti e riesegui dopo un cambio di store. [Replicas](replicas.md) tratta la scelta dello store e le strategie di freschezza.
