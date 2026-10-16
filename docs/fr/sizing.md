# Dimensionnement et réglage {: #sizing-and-tuning }

L'outil de dimensionnement mesure votre propre déploiement Provisa sous votre propre charge. À partir de ces mesures, il rédige un guide de dimensionnement et de réglage : combien de cœurs physiques et de processus worker la charge exige, et ce que rapporte chaque réglage. Chaque chiffre du guide provient d'une mesure ; après un changement du déploiement ou de la charge, vous relancez donc l'outil et régénérez le guide.

L'outil se trouve dans le dépôt Provisa, sous `demo/named/perf/bench/`. Lancez chaque commande ci-dessous depuis ce répertoire, avec l'environnement Python du dépôt (`../../../../.venv/bin/python3`).

## Ce qu'il rapporte {: #what-it-reports }

Une exécution de dimensionnement est composée de plusieurs exécutions de benchmark contre votre déploiement. Chacune fait varier un seul élément :

| Exécution | Ce qui change | Ce qu'elle donne |
|---|---|---|
| Référence | Rien : une colonne, une ligne, aucun filtre, aucune jointure, sans cache | Le coût de base d'une requête |
| Exécutions de curseurs | Un curseur de requête à la fois : champs, filtres, lignes, jointures au sein d'une même source, jointures entre sources | Le coût que chaque curseur ajoute par unité |
| Exécutions de jointures entre sources | Les valeurs de jointure entre sources, une fois avec les deux extrémités lues en direct, une fois avec les deux depuis des répliques, une fois avec une de chaque | Le coût d'une jointure entre sources selon la façon dont ses extrémités sont lues, et le point d'équilibre entre extrémités en direct et en réplique |
| Exécutions de cache | Chaque requête activée pour le cache de réponses, puis aucune | Le coût d'un succès de cache et d'un échec |
| Exécutions de réplication | Une source lue en direct, puis depuis une réplique | Le débit au-delà duquel une réplique coûte moins |
| Exécutions de workers | Processus worker et requêtes en cours par worker | Workers par cœur physique, requêtes en cours par worker, ce que fournit un cœur |

Les exécutions de curseurs, de cache et de réplication sont faites par transport et par source, et une fois par moteur de fédération que vous nommez. Les exécutions de workers sont faites par transport. [tool-verified: `demo/named/perf/bench/sweeps.py:plan_sweeps,plan_worker_sweep`]

Le coût désigne ici le temps CPU de Provisa par requête, en millisecondes. Les exécutions de curseurs se font sans cache ; le coût d'un curseur n'inclut donc jamais l'effet du cache. [tool-verified: `demo/named/perf/bench/sweeps.py:_baseline` sets `k["cache"] = {"probability": 0.0}`]

À partir des exécutions, `bench_report.py` ajuste un modèle de coût par moteur, transport et source : un coût de base plus un coût par champ, filtre, ligne et jointure, une jointure entre sources étant chiffrée selon la façon dont ses deux extrémités sont lues. Quand le moteur s'exécute comme son propre serveur et que le CPU de son conteneur est mesuré, le moteur reçoit lui aussi un modèle de coût. [tool-verified: `demo/named/perf/bench/cost_model.py:fit`] `sizing_guide.py` transforme le rapport en guide. À partir d'un contrat qui décrit votre charge, il dimensionne aussi cette charge.

## Écrire un contrat de configuration {: #write-a-setup-contract }

Le contrat de configuration est un fichier YAML unique. Il nomme les endpoints de votre déploiement, les tables que le benchmark peut lire, la forme des requêtes et la charge. Le benchmark vérifie le contrat contre le déploiement avant de s'exécuter. Il refuse une table, une colonne ou une jointure que le déploiement n'expose pas. [tool-verified: `demo/named/perf/bench/setup_contract.py:setup_from_dict`]

Les tables et les colonnes sont nommées telles que votre déploiement les enregistre. Le nom que leur donne chaque transport (noms SQL, champs GraphQL, étiquettes Cypher, chemins REST, types gRPC) est demandé au déploiement au démarrage de l'exécution.

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

Le format du contrat est défini par `setup_contract.py` et `contract_parse.py` ; un contrat invalide est refusé avec un message qui nomme le champ. [tool-verified: `demo/named/perf/bench/contract_parse.py`, `SetupError`] Les poids de `workload` doivent totaliser 1, et chaque transport qu'ils nomment doit être l'un de ceux que le contrat exécute. [tool-verified: `demo/named/perf/bench/contract_parse.py:_workload`]

Décrivez les requêtes telles qu'elles sont réellement. Le guide dimensionne exactement les requêtes que ce contrat génère : le dimensionnement les tire du même générateur à graine que celui d'une exécution. [tool-verified: `demo/named/perf/bench/sizing.py:_transport` uses `request_mix.RequestGenerator`]

## L'exécuter {: #run-it }

### 1. Résoudre les noms une fois {: #1-resolve-the-names-once }

Lancez une fois le contrat de référence. L'exécution demande au déploiement comment chaque transport nomme vos tables et écrit `resolved-names.json` à côté de ses résultats. [tool-verified: `demo/named/perf/bench/optimistic_load.py:resolve_names`]

```bash
../../../../.venv/bin/python3 run_benchmark.py --engine pg --optimistic \
  --setup acme.yaml --server-pid "$PROVISA_PID" --output-dir results/first
```

`--engine` nomme le moteur de fédération qu'exécute votre déploiement (`duckdb`, `pg` ou `trino`). `--server-pid` est l'identifiant de processus du serveur ; l'exécution lit le temps CPU de ce processus et de ses processus enfants (les workers). Elle lit le processus directement, ce qui ne fonctionne que lorsque le générateur de charge s'exécute sur l'hôte du serveur. [tool-verified: `demo/named/perf/bench/optimistic_load.py:_server_cpu_seconds`]

### Mesurer le CPU du serveur depuis un autre hôte {: #measuring-the-servers-cpu-from-another-host }

Exécutez si possible le générateur de charge sur sa propre machine : il ne prend alors aucun cœur au serveur. Pour mesurer de là le CPU du serveur, démarrez `server_cpu_agent.py` sur l'hôte du serveur et donnez à l'exécution son URL à la place de `--server-pid` :

```bash
# on the server's host, from demo/named/perf/bench
../../../../.venv/bin/python3 server_cpu_agent.py --pid "$PROVISA_PID" --listen 10.0.0.5:9555

# on the load generator's host
../../../../.venv/bin/python3 run_benchmark.py --engine pg --optimistic \
  --setup acme.yaml --server-cpu-url http://10.0.0.5:9555/ --output-dir results/first
```

L'agent répond un seul chiffre, le même que lit `--server-pid` : `GET /` renvoie `{"pid": ..., "cpu_seconds": ...}`, les secondes CPU cumulées du processus serveur et de ses workers. Tout autre chemin répond 404, et toute autre méthode est refusée. [tool-verified: `demo/named/perf/bench/server_cpu_agent.py:handler`] Donnez à une exécution `--server-pid` ou `--server-cpu-url`, jamais les deux. [tool-verified: `demo/named/perf/bench/optimistic_load.py:server_cpu_source`]

`--listen` est obligatoire : l'agent n'a pas d'adresse par défaut. Liez-le à une adresse privée que seul le générateur de charge peut atteindre, jamais à une interface publique, et arrêtez-le (Ctrl+C) quand les exécutions sont terminées.

### 2. Planifier les exécutions {: #2-plan-the-runs }

```bash
../../../../.venv/bin/python3 sweeps.py plan --setup acme.yaml --out results/sizing \
  --resolved-names results/first/optimistic/resolved-names.json \
  --physical-cores 16 --shared-host yes \
  --workers 8,16,32 --request-threads 1,2,4 \
  --replication live,replica --ttl 300 \
  --engines pg,duckdb --engine-container pg=perf-engine-postgresql-1
```

- `--physical-cores` : les cœurs physiques du serveur, pas ses threads matériels. Sous Linux, `lscpu` indique les cœurs par socket et les sockets ; multipliez-les. L'outil ne devine jamais cette valeur.
- `--shared-host` : `yes` quand vos sources ou le générateur de charge s'exécutent sur l'hôte du serveur. Ils sont alors en concurrence avec Provisa pour ses cœurs, et le guide le signale.
- `--workers` et `--request-threads` : les processus worker et les requêtes en cours par worker à essayer. Donnez les deux ou aucun.
- `--replication` et `--ttl` : ajoutent les exécutions en direct et en réplique pour chaque source, la réplique étant rafraîchie toutes les `--ttl` secondes. Les exécutions de jointures entre sources lues depuis des répliques ont aussi besoin de `--ttl`.
- `--engines` : les moteurs de fédération à mesurer, `pg` par défaut. Chaque exécution de curseurs est planifiée une fois par moteur ; le moteur est fixé au démarrage du serveur, donc les exécutions de chaque moteur exigent de redémarrer le serveur avec lui.
- `--engine-container` : `ENGINE=CONTAINER` pour un moteur qui s'exécute comme serveur dans son propre conteneur, dont chaque exécution rapporte alors le CPU. Omettez un moteur qui s'exécute à l'intérieur de Provisa (DuckDB).

Le plan écrit un contrat par exécution et un `manifest.json`. Une exécution que le déploiement ne peut pas servir, comme des jointures via gRPC ou une table qu'un transport n'expose pas, est listée dans le manifeste avec la raison. Elle n'est jamais écartée silencieusement. [tool-verified: `demo/named/perf/bench/sweeps.py:_validate`, `write_plan`]

Les exécutions de réplique mesurent le coût de rafraîchissement de la réplique pendant une fenêtre sans charge. Définissez `load.idle_window_s` dans votre contrat (par exemple `120`) avant de les planifier. Sans lui, le rapport refuse le point d'équilibre avec `no idle.json`. [tool-verified: `demo/named/perf/bench/bench_report.py:_break_even`]

### 3. Lancer chaque contrat planifié {: #3-run-every-planned-contract }

Chaque entrée du manifeste nomme son fichier de contrat. Lancez chacune dans `results/sizing/results/<run name>` :

```bash
../../../../.venv/bin/python3 run_benchmark.py --engine pg --optimistic \
  --setup results/sizing/graphql.sales-pg.filters.1.yaml \
  --resolved-names results/first/optimistic/resolved-names.json \
  --server-pid "$PROVISA_PID" --output-dir results/sizing/results/graphql.sales-pg.filters.1
```

Depuis un autre hôte, utilisez `--server-cpu-url` à la place de `--server-pid`. Une exécution de workers redémarre le serveur, son identifiant de processus change donc : redémarrez aussi l'agent avec le nouveau `--pid`.

Ajoutez `--apply-replication` à chaque exécution dont l'entrée du manifeste porte `"apply_replication": true` (les exécutions de réplication et les exécutions de jointures entre sources). Il applique la réplication que déclare le contrat de l'exécution et la rétablit ensuite. [tool-verified: `demo/named/perf/bench/optimistic_load.py:add_arguments`]

Chaque exécution porte un `server_env` dans le manifeste : le serveur doit tourner avec lui. Pour une exécution de curseurs, c'est son moteur, `{"PROVISA_ENGINE": "duckdb"}` ; lancez toutes les exécutions d'un moteur, puis redémarrez le serveur avec le suivant. Pour une exécution de workers, il s'y ajoute ses workers et sa borne de requêtes en cours, par exemple `{"PROVISA_WORKERS": "16", "PROVISA_REQUEST_THREADS": "2", "PROVISA_ENGINE": "pg"}` : `PROVISA_WORKERS` est le nombre de processus worker et `PROVISA_REQUEST_THREADS` fixe `concurrency.request_threads`, les requêtes en cours que chaque worker exécute à la fois. [tool-verified: `provisa/core/settings_catalog.py` `concurrency.request_threads`, `env="PROVISA_REQUEST_THREADS"`] Attendez que tous les workers servent (`/health` indique `workers.ready` sur `workers.expected`) avant de démarrer l'exécution.

### 4. Construire le rapport {: #4-build-the-report }

```bash
../../../../.venv/bin/python3 bench_report.py --root results/sizing --out results/sizing/report.json
```

`--tolerance` (par défaut `0.05`) fixe à quelle distance du meilleur débit mesuré un réglage de workers doit se trouver pour être jugé suffisant. [tool-verified: `demo/named/perf/bench/bench_report.py:DEFAULT_TOLERANCE`]

### 5. Générer le guide {: #5-generate-the-guide }

```bash
../../../../.venv/bin/python3 sizing_guide.py --report results/sizing/report.json --out guide.md \
  --setup acme.yaml --resolved-names results/first/optimistic/resolved-names.json
```

Sans `--setup`, le guide ne contient que les sections de réglage. Avec lui, le guide dimensionne la `workload` du contrat avec le moteur que nomme `--engine` (`pg` par défaut) ; un moteur que les exécutions n'ont pas mesuré est refusé.

## Lire le guide {: #read-the-guide }

### Dimensionner cette charge {: #sizing-this-workload }

Le guide termine cette section par une ligne, par exemple : **12 physical cores, 24 worker processes, 2 in-flight requests per worker.** Au-dessus, un tableau présente chaque transport et chaque source : la part des requêtes, la moyenne des champs, filtres, lignes et jointures, la part de succès de cache et le CPU que coûte une requête.

Une requête coûte, lors d'un échec, le modèle de coût aux moyennes de ses curseurs et, lors d'un succès, le coût de succès mesuré. Le CPU Provisa d'un transport par seconde, divisé par ce que fournit un cœur physique, donne ses cœurs physiques. Le guide les additionne entre transports et arrondit à l'entier supérieur. Les workers sont les cœurs multipliés par le plus grand nombre de workers par cœur qu'exige l'un des transports. Les requêtes en cours par worker sont de même le plus grand nombre qu'exige l'un d'eux. Un seul réglage les sert tous. [tool-verified: `demo/named/perf/bench/sizing.py:size`]

L'outil ne dimensionne rien qu'il n'ait mesuré. Une partie de la charge pour laquelle les exécutions n'ont pas de modèle arrête le dimensionnement avec un message qui la nomme [tool-verified: `demo/named/perf/bench/sizing.py:SizingRefused`] :

- un moteur que les exécutions n'ont pas mesuré
- un transport sans exécutions de workers
- une source sans modèle de coût sur ce transport
- un curseur que les exécutions n'ont pas fait varier (faites-le varier, puis relancez)
- une indication de route (`direct` ou `federated`) sur une source
- un réglage de réplication ou un TTL différent de celui qui a été mesuré
- des succès de cache quand les exécutions n'ont pas de chiffres de cache

### Processus worker et requêtes en cours {: #worker-processes-and-in-flight-requests }

Pour chaque transport, le guide indique le plus petit nombre de workers dont le débit maximal reste dans la tolérance du meilleur. À ce nombre de workers, il choisit ensuite le plus petit nombre de requêtes en cours par worker qui fait de même. Il présente ce réglage sous deux formes : workers par cœur physique et requêtes en cours par worker. Davantage de l'un ou l'autre n'apporte pas de débit. Moins laisse des cœurs inactifs. Le tableau donne aussi le pic de requêtes par seconde et sa latence p99. Chaque point mesuré est listé en dessous.

Le facteur de capacité est le nombre de requêtes par seconde que fournit un cœur physique, multiplié par le CPU que coûte une requête isolée, divisé par 1000. Au-dessus de 1, le cœur fait plus que les requêtes d'un seul thread, comme lorsque ses threads matériels servent tous les deux. [tool-verified: `demo/named/perf/bench/bench_report.py:_server`]

### Ce que coûte une requête {: #what-a-request-costs }

Pour chaque moteur, transport et source, côte à côte : le coût de base d'une requête sans cache, le coût qu'ajoutent chaque champ, filtre, ligne et jointure, et la qualité d'ajustement du modèle aux mesures (R², 1 étant un ajustement parfait). Une jointure entre sources a trois coûts, selon la façon dont ses deux extrémités sont lues : toutes deux en direct, toutes deux depuis des répliques, ou une de chaque. Un tiret signifie que les exécutions n'ont pas fait varier ce curseur. Quand le moteur s'exécute comme son propre serveur, un second tableau donne de la même façon le CPU du moteur : une requête que sert le moteur, comme une jointure entre sources, y montre son temps moteur. Servez-vous de ces chiffres pour voir ce que coûterait un changement de vos requêtes avant de l'effectuer : deux champs de plus dans chaque requête ajoutent le double du coût par champ.

### L'activation du cache de réponses {: #the-response-cache-opt-in }

Pour chaque transport et chaque source : le CPU d'un échec, d'un succès et ce qu'économise un succès. Une requête n'est servie depuis le cache que si elle l'active et répète une requête antérieure ; l'économie s'applique donc aux requêtes répétées. Activez le cache pour les requêtes répétées. [tool-verified: `demo/named/perf/bench/bench_report.py:_cache`]

### Lectures en direct ou en réplique, et le TTL {: #live-or-replica-reads-and-the-ttl }

Pour chaque moteur, transport et source ayant des exécutions de réplication : le coût d'une lecture en direct, d'une lecture en réplique, le CPU d'un rafraîchissement, le TTL et le débit du point d'équilibre. Au-dessus du débit du point d'équilibre, une réplique coûte moins de CPU, celui de Provisa, celui de la source et celui du moteur réunis, qu'une lecture en direct. En dessous, lisez en direct. Un second tableau donne la même chose pour les jointures entre sources, selon le nombre de jointures de la requête : les deux extrémités lues en direct contre les deux depuis des répliques, le rafraîchissement comptant chaque source que le basculement a déplacée vers une réplique. [tool-verified: `demo/named/perf/bench/cost_model.py:break_even`]

Le point d'équilibre est le CPU du rafraîchissement par seconde divisé par ce qu'économise une lecture :

```text
break-even req/s = (refresh CPU s × 1000 / TTL s) / (live ms − replica ms)
```

Doubler le TTL divise par deux le débit du point d'équilibre, et les données peuvent être jusqu'à deux fois plus anciennes. Choisissez le TTL d'après l'ancienneté que vos utilisateurs peuvent accepter pour les données, puis comparez votre débit de requêtes sur cette source avec le point d'équilibre à ce TTL.

Le coût d'une lecture en réplique dépend du magasin et de ses index, construits peu après chaque atterrissage. Mesurez une fois les index en place et recommencez après un changement de magasin. [Répliques](replicas.md) traite du choix du magasin et des stratégies de fraîcheur.
