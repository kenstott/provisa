# Riferimento alla configurazione {: #configuration-reference }

Provisa viene configurato tramite un file YAML (default: `config/provisa.yaml`).

## Include {: #includes }

Suddividi una configurazione tra più file con `includes:`. Il file che include elenca i percorsi dei frammenti sotto questa chiave; Provisa li unisce prima della validazione, producendo lo stesso risultato che si otterrebbe scrivendo tutto in un unico file.

```yaml
# provisa-with-sources.yaml — wrapper that adds a Neo4j source to the base config
includes:
  - /path/to/config/provisa-install.yaml
  - /path/to/demo/sources/neo4j/fragment.yaml
```

Un file wrapper che contiene solo `includes:` è valido. `load_control_plane` legge attraverso gli include, quindi la sezione `control_plane:` viene presa dal file incluso che la imposta.

**Regole di unione**

- I percorsi vengono risolti relativamente al file che include. I percorsi assoluti vengono usati così come sono.
- Le sezioni elenco (`sources`, `tables`, `domains`, `relationships`, `roles`, …) vengono accodate, e le voci del frammento seguono quelle del file che include.
- Una chiave scalare o mapping non impostata dal file che include viene presa dal frammento.
- Una chiave impostata da entrambi i file con valori diversi è un conflitto; il caricamento fallisce, nominando la chiave.
- Valori identici in entrambi i file non sono un conflitto.
- Gli include si annidano. Un file che include se stesso — direttamente o tramite un altro frammento — viene rifiutato.
- `includes` viene consumato al momento del caricamento e non compare mai nella configurazione validata.

## Origini {: #sources }

```yaml
sources:
  - id: sales-pg           # unique identifier
    type: postgresql
    host: postgres
    port: 5432
    database: provisa
    username: provisa
    password: ${env:PG_PASSWORD}  # secret resolution
    pool_min: 1
    pool_max: 5
    use_pgbouncer: false
    pgbouncer_port: 6432
```

Tutte le origini condividono un insieme comune di campi.

| Campo | Default | Note |
| ------- | --------- | ------ |
| `id` | obbligatorio | Alfanumerico, trattini, underscore |
| `type` | obbligatorio | Vedi tabella sotto |
| `host` | `""` | Hostname o IP |
| `port` | `0` | `0` significa che ogni connettore fornisce il proprio default — non esiste una mappa centrale di porte di default |
| `database` | `""` | |
| `username` | `""` | |
| `password` | `""` | Supporta i riferimenti a credenziali `${env:VAR}` e `${secret:NAME}` — vedere [Segreti](secrets.md) |
| `path` | `null` | Percorso file o URI per origini basate su file |
| `base_url` | `null` | URL base per origini API |
| `pool_min` / `pool_max` | `1` / `5` | Limiti del connection pool |
| `cache_enabled` | `true` | Attiva/disattiva la cache per tutte le tabelle di questa origine |
| `cache_ttl` | `null` | Secondi; `null` eredita il default globale; `-1` non scade mai |
| `federation_hints` | `{}` | Parametri estesi per connettore (dict[str,str]); vedi riferimento tipi sotto. |
| `mapping` | `{}` | DSL di mapping per redis, elasticsearch, prometheus. |
| `allowed_domains` | `[]` | Restringe questa origine a specifici ID di dominio; vuoto = senza restrizioni |
| `description` | `""` | |

### Tipi di origine supportati {: #supported-source-types }

| Tipo | Stile di connessione | Note |
| ------ | ----------------- | ------- |
| **RDBMS** | | |
| `postgresql` | host/porta | Pool asyncpg; PgBouncer opt-in via `use_pgbouncer` |
| `mysql` | host/porta | |
| `mariadb` | host/porta | |
| `singlestore` | host/porta | |
| `sqlserver` | host/porta | |
| `oracle` | host/porta | |
| `firebird` | host + `path` (file DB) | Estensione community DuckDB firebird |
| `duckdb` | host/porta | |
| `cockroachdb` | host/porta | Riusa il driver/dialetto PostgreSQL |
| `yugabytedb` | host/porta | Riusa il driver/dialetto PostgreSQL |
| `greenplum` | host/porta | Riusa il driver/dialetto PostgreSQL |
| `tidb` | host/porta | Riusa il driver/dialetto MySQL |
| **Cloud DW** | | |
| `snowflake` | host/porta + `federation_hints` | `account` richiesto negli hint |
| `bigquery` | `federation_hints` | `project` richiesto; auth via `GOOGLE_APPLICATION_CREDENTIALS` |
| `databricks` | host + `federation_hints` | `http_path` richiesto negli hint |
| `fabric` | variabili d'ambiente o `PROVISA_ENGINE_URL` | T-SQL su TDS, auth Azure AD |
| `synapse` | variabili d'ambiente o `PROVISA_ENGINE_URL` | T-SQL su TDS, auth Azure AD |
| `redshift` | host/porta | |
| **OLAP** | | |
| `clickhouse` | host/porta + `federation_hints` | L'hint `secure` attiva TLS; porta default 8123/8443 |
| `elasticsearch` | host/porta + DSL `mapping` | |
| `pinot` | host/porta | Endpoint REST del Controller |
| `druid` | host/porta | Endpoint Avatica del Broker |
| `exasol` | host/porta | |
| **Data Lake** | | |
| `delta_lake` | `path` (URI tabella) | `delta_scan` di DuckDB; accesso all'object store via `federation_hints` |
| `iceberg` | `path` (URI tabella) | `iceberg_scan` di DuckDB; accesso all'object store via `federation_hints` |
| `hudi` | `path` (URI tabella) | Motore Hudi di ClickHouse, zero-copy |
| `hive` | host/porta (metastore) + `mapping.storage` | Backend di storage in `mapping["storage"]`: hadoop/hdfs/local/s3/azure/adls |
| `hive_s3` | host/porta (metastore) + chiavi S3 in `mapping` | Tipo distinto; sempre storage S3 |
| **NoSQL** | | |
| `mongodb` | host/porta | Campi di connessione semplici; nessun DSL di mapping |
| `cassandra` | host/porta | Campi di connessione semplici; nessun DSL di mapping |
| `redis` | host/porta + DSL `mapping` | |
| **Streaming** | | |
| `kafka` | solo registrazione | La configurazione reale vive in `kafka_sources[]`; vedi §Kafka sotto |
| `websocket` | host/porta/percorso + `federation_hints` | Feed WebSocket esterno |
| `rss` | host/porta/percorso + `federation_hints` | Feed RSS 2.0 / Atom |
| **Grafo/Semantico** | | |
| `neo4j` | [UNVERIFIED end-to-end mapping] | |
| `sparql` | [UNVERIFIED end-to-end mapping] | |
| **File** | | |
| `sqlite` | `path` | Passa sempre attraverso il motore (nessun pool diretto) |
| `csv` | `path` | |
| `parquet` | `path` | |
| `files` | `path` (directory) | Crawler glob; espone CSV/Parquet/XLSX/JSON come tabelle |
| **API/Remoto** | | |
| `google_sheets` | `federation_hints.spreadsheet_id` | |
| `prometheus` | host/porta o `mapping.url` + DSL `mapping` | |
| `graphql_remote` | `base_url` + `mapping` opzionale | Header, forward-client-headers, timeout in `mapping` |
| `openapi` | `base_url` | |
| `grpc_remote` | [UNVERIFIED end-to-end mapping] | |
| `airport` | `base_url` (location Flight) | Estensione airport di DuckDB |
| `ingest` | ricevitore push | Servizi esterni inviano eventi JSON via POST |
| **SaaS** | | |
| `sharepoint` | `base_url` o `host` + `mapping` | Auth via `mapping.auth_type` |
| `splunk` | `host`/`port` o `base_url` + `mapping` | |
| **GovData** | | |
| `govdata` | subject + `domain_id` | Modello `GovDataSource` separato; vedi §GovData sotto |
| **Data Quality** | | |
| `soda` | host/porta rivolti al pgwire di Provisa | Richiede l'extra `soda`; Elastic License 2.0, solo self-hosted |
| `great_expectations` | host/porta rivolti al pgwire di Provisa | Richiede l'extra `gx`; Apache 2.0 |

### Riferimento ai tipi di origine {: #source-type-reference }

Ogni tipo che richiede una configurazione non ovvia ha una breve voce qui sotto. I tipi RDBMS (postgresql, mysql e così via) usano solo i campi comuni sopra e non hanno bisogno di una sezione propria.

#### GovData

Le origini `govdata` usano un proprio modello di primo livello, `GovDataSource`, al posto del `Source` generico. L'accesso è suddiviso per raggruppamento di argomenti.

```yaml
sources:
  - id: federal-data
    type: govdata
    subject: COMMERCE
    domain_id: federal-analytics
    api_key: ${env:GOVDATA_API_KEY}   # optional
    start_year: 2020                   # optional year filter
    end_year: 2024                     # optional year filter
```

Ogni subject mappa a uno o più schemi GovData. Configurare un'origine `govdata` con un subject espone automaticamente tutti gli schemi per quel subject.

| Subject | Schemi |
| --------- | --------- |
| `COMMERCE` | `sec`, `patents` |
| `ECONOMY` | `econ`, `econ_reference` |
| `EDUCATION` | `census`, `edu` |
| `HEALTH` | `health` |
| `CYBER` | `cyber_threat`, `cyber_vuln` |
| `PUBLIC_SAFETY` | `crime` |
| `ENVIRONMENT` | `lands` |
| `WEATHER` | `weather` |
| `ENERGY` | `energy` |
| `GOVERNMENT` | `fedregister`, `fec` |

Gli schemi `ref` e `geo` sono sempre inclusi come schemi di collegamento. Non sono configurabili e non compaiono nell'elenco sopra. Usate l'argomento `ALL` per concedere l'accesso a tutti gli schemi.

#### Kafka

La riga `kafka` in `sources:` registra soltanto l'origine. Il `details()` del suo connettore restituisce `{}`; la configurazione vera sta nel blocco di primo livello `kafka_sources[]`. Kafka è sempre un VIRTUAL_SOURCE: passa dal motore e non ha un pool diretto.

```yaml
kafka_sources:
  - id: event-stream
    bootstrap_servers: kafka:9092
    schema_registry_url: http://schema-registry:8081  # optional
    topics:
      - id: order-created
        topic: orders.events
        default_window: 1h          # auto-injected time bound
        schema_source: manual       # manual, registry, or sample
        value_format: json
        discriminator:              # filter shared topic by message type
          field: event_type
          value: OrderCreated
        columns:
          - name: event_type
            type: varchar
          - name: order_id
            type: integer
          - name: amount
            type: double
          - name: metadata
            type: varchar           # raw JSON for complex nested data
      - id: order-shipped
        topic: orders.events        # same physical topic
        default_window: 1h
        discriminator:
          field: event_type
          value: OrderShipped
        columns:
          - name: event_type
            type: varchar
          - name: order_id
            type: integer
          - name: shipped_at
            type: timestamp
```

**Time Window** — `default_window` limita ogni query a un periodo di tempo recente, prevenendo letture illimitate da topic ad alto volume. Formato: `1h`, `30m`, `7d`, `60s`. Default `1h`. Iniettato automaticamente come `WHERE _timestamp >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR`. I client possono sovrascriverlo con un proprio filtro `_timestamp` nell'argomento `where` di GraphQL.

**Discriminator** — Più configurazioni di topic possono puntare allo stesso topic Kafka fisico con valori `discriminator` diversi, producendo tipi GraphQL separati. Il discriminator viene iniettato automaticamente come clausola WHERE.

**Schema Source**

| Valore | Comportamento |
| ------- | ---------- |
| `registry` | Recupera lo schema dal Confluent Schema Registry |
| `manual` | Definisce le colonne inline nella configurazione (nessun Schema Registry necessario) |
| `sample` | Scoperta automatica da messaggi di esempio |

#### Snowflake

`account` in `federation_hints` è richiesto. `warehouse`, `role` e `schema` sono opzionali.

```yaml
sources:
  - id: my-snowflake
    type: snowflake
    host: org.snowflakecomputing.com
    username: svc_provisa
    password: ${env:SNOWFLAKE_PASSWORD}
    database: MY_DB
    federation_hints:
      account: myorg-myaccount     # required
      warehouse: COMPUTE_WH
      role: PROVISA_ROLE
      schema: PUBLIC               # remote schema override
```

#### Databricks

`http_path` in `federation_hints` è obbligatorio. `password` contiene il token di accesso personale. `catalog` è facoltativo e va nell'SQL o negli hint; il campo `database` non lo contiene.

```yaml
sources:
  - id: my-databricks
    type: databricks
    host: my-workspace.azuredatabricks.net
    password: ${env:DATABRICKS_TOKEN}
    federation_hints:
      http_path: /sql/1.0/warehouses/xxxx   # required
      catalog: my_unity_catalog              # optional
```

#### BigQuery

`project` in `federation_hints` è richiesto. L'autenticazione usa `GOOGLE_APPLICATION_CREDENTIALS` (percorso a un file di chiave service-account) o le Application Default Credentials nell'ambiente del motore.

```yaml
sources:
  - id: my-bigquery
    type: bigquery
    federation_hints:
      project: my-gcp-project     # required
```

#### Fabric / Synapse

Entrambi usano T-SQL su TDS con autenticazione Azure AD. Accedete con `az login` (sviluppo) o con un'identità gestita (produzione); il motore legge le credenziali tramite `DefaultAzureCredential` di `azure-identity`. I dati di connessione arrivano da variabili d'ambiente, `FABRIC_SQL_SERVER` / `FABRIC_DATABASE` (Fabric) o `SYNAPSE_SQL_SERVER` / `SYNAPSE_DATABASE` (Synapse), oppure da `PROVISA_ENGINE_URL`.

```yaml
sources:
  - id: my-fabric
    type: fabric
    # host/database read from FABRIC_SQL_SERVER / FABRIC_DATABASE when not set here
```

#### ClickHouse

`secure` in `federation_hints` attiva TLS sull'interfaccia HTTP. La porta è di default `8123` (semplice) o `8443` (quando `secure: "true"`). `schema` in `federation_hints` sovrascrive lo schema remoto.

```yaml
sources:
  - id: my-clickhouse
    type: clickhouse
    host: ch.example.com
    password: ${env:CLICKHOUSE_PASSWORD}
    federation_hints:
      secure: "true"    # uses port 8443; omit to use 8123
      schema: analytics
```

#### Delta Lake / Iceberg

`path` è l'URI della tabella (S3, GCS, ADLS o locale). L'accesso all'object store richiede credenziali `federation_hints`. Per Cloudflare R2, aggiungi `account_id`.

```yaml
sources:
  - id: events-delta
    type: delta_lake
    path: s3://my-bucket/data/events
    federation_hints:
      access_key_id: ${env:S3_ACCESS_KEY}
      secret_access_key: ${env:S3_SECRET}

  - id: r2-parquet
    type: parquet
    path: s3://my-bucket/data/events.parquet
    federation_hints:
      access_key_id: ${env:R2_ACCESS_KEY}
      secret_access_key: ${env:R2_SECRET}
      account_id: ${env:R2_ACCOUNT_ID}   # Cloudflare R2 account (S3-compatible)
```

#### Hive / Hive S3

`host` e `port` puntano al metastore Thrift di Hive (porta predefinita 9083). Per `hive`, impostate `mapping["storage"]` per scegliere il backend dell'object store. Una chiave obbligatoria mancante fallisce in modo evidente, senza valore di ripiego.

`hive_s3` è un tipo distinto che dichiara sempre storage S3; non serve `mapping.storage`.

```yaml
sources:
  - id: hive-s3-lake
    type: hive
    host: metastore.internal
    port: 9083
    mapping:
      storage: s3
      endpoint: https://s3.us-east-1.amazonaws.com
      access_key_id: ${env:AWS_ACCESS_KEY_ID}
      secret_access_key: ${env:AWS_SECRET_ACCESS_KEY}
      region: us-east-1
      path_style: true           # required for MinIO and non-AWS S3-compatible endpoints

  - id: hive-adls-lake
    type: hive
    host: metastore.internal
    port: 9083
    mapping:
      storage: adls
      storage_account: mystorageaccount
      access_key: ${env:ADLS_ACCESS_KEY}
      # sas_token: ${env:ADLS_SAS_TOKEN}   # alternative to access_key
```

Valori accettati per `mapping.storage`: `hadoop` (default), `hdfs`, `local`, `s3`, `azure`, `adls`. Chiavi di mapping S3: `endpoint`, `access_key_id`, `secret_access_key`, `region`, `path_style`. Chiavi di mapping ADLS: `storage_account`, `access_key` o `sas_token`.

#### Redis

Usa il DSL `mapping`. `mongodb` e `cassandra` usano campi di connessione semplici e NON usano il DSL di mapping.

```yaml
sources:
  - id: my-redis
    type: redis
    host: redis.internal
    port: 6379
    password: ${env:REDIS_PASSWORD}
    mapping:
      tables:
        - name: sessions
          key_pattern: "sessions:*"
          key_column: key           # default "key"
          value_type: hash          # hash | string | zset | list; default hash
          columns:
            - name: user_id
              data_type: VARCHAR
              field: user_id        # Redis hash field name
            - name: expires_at
              data_type: BIGINT
              field: expires_at
```

#### Elasticsearch

```yaml
sources:
  - id: my-es
    type: elasticsearch
    host: es.internal
    port: 9200
    username: elastic
    password: ${env:ES_PASSWORD}
    mapping:
      tls: true
      tables:
        - name: logs
          index: app-logs-*
          discover: false
          columns:
            - name: timestamp
              data_type: TIMESTAMP
              path: "@timestamp"
            - name: level
              data_type: VARCHAR
              path: level
            - name: message
              data_type: VARCHAR
              path: message
```

#### Prometheus

`mapping.url` sovrascrive `host:port` quando entrambi sono presenti.

```yaml
sources:
  - id: my-prometheus
    type: prometheus
    mapping:
      url: http://prometheus.internal:9090
      tables:
        - name: http_requests
          metric: http_requests_total
          labels_as_columns: [method, status, handler]
          value_column: value      # default "value"
          default_range: 1h        # default "1h"
```

#### Google Sheets

`spreadsheet_id` in `federation_hints` è richiesto. L'autenticazione usa un SECRET DuckDB `gsheet` provisionato al momento dell'attach.

```yaml
sources:
  - id: my-sheet
    type: google_sheets
    federation_hints:
      spreadsheet_id: 1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms
```

#### Origini file (csv / parquet / sqlite / files) {: #file-sources-csv-parquet-sqlite-files }

`path` è richiesto. `files` esegue il crawling di una directory per file CSV, Parquet, XLSX e JSON, esponendo ciascuno come tabella. Tutte le origini basate su file sono VIRTUAL (passano attraverso il motore; nessun pool diretto).

```yaml
sources:
  - id: orders-csv
    type: csv
    path: /data/orders.csv

  - id: data-lake-dir
    type: files
    path: /data/lake/         # directory; each file becomes a table
```

I **dataset Kaggle** non si possono aggiungere con questo file, perché richiedono un token valido e il selettore di dataset nel modulo delle origini (Sources → Subscriptions → Kaggle). Un'origine Kaggle esportata in YAML compare come `type: files` con `kaggle_owner` e `kaggle_ref` in `federation_hints`. Riscaricare da Kaggle richiede la mutation `refreshKaggleSource` o l'aggiornamento nella UI; modificare lo YAML non basta. Vedi [Dataset Kaggle](sources.md#kaggle-datasets) nel riferimento dei tipi di origine.

#### Origini API / Remote {: #api-remote-sources }

**openapi** — imposta `base_url` all'URL base OpenAPI. La scoperta dello schema legge la spec OpenAPI all'avvio.

```yaml
sources:
  - id: payment-api
    type: openapi
    base_url: https://api.payments.example.com/v1
```

**graphql_remote** — imposta `base_url`. Chiavi `mapping` opzionali: `headers` (dict di header statici), `forward_client_headers` (bool), `timeout_seconds` (int).

```yaml
sources:
  - id: orders-gql
    type: graphql_remote
    base_url: https://orders.internal/graphql
    mapping:
      headers:
        X-Api-Key: ${env:ORDERS_API_KEY}
      forward_client_headers: true
      timeout_seconds: 30
```

**airport** — `base_url` è la location del server Arrow Flight. Estensione airport di DuckDB.

```yaml
sources:
  - id: flight-source
    type: airport
    base_url: grpc://flight.internal:8815
```

**websocket / rss** — usa `host`, `port`, `path` e `federation_hints`.

```yaml
sources:
  - id: market-feed
    type: websocket
    host: feed.example.com
    port: 443
    path: /ws/v1
    federation_hints:
      use_ssl: "true"
      subscribe_payload: '{"action":"subscribe","channels":["ticker"]}'
      event_path: data

  - id: news-rss
    type: rss
    host: feeds.example.com
    port: 443
    path: /rss/latest
    federation_hints:
      use_ssl: "true"
      poll_interval: "300"      # seconds
      # feed_url: https://...  # overrides host/port/path when set
```

**sharepoint**

```yaml
sources:
  - id: my-sharepoint
    type: sharepoint
    base_url: https://myorg.sharepoint.com/sites/data
    username: ${env:SP_CLIENT_ID}
    password: ${env:SP_CLIENT_SECRET}
    database: ${env:SP_TENANT_ID}
    mapping:
      auth_type: CLIENT_CREDENTIALS   # default
      # certificate_path: /path/to/cert.pem
      # certificate_password: ${env:CERT_PASSWORD}
```

**splunk**

```yaml
sources:
  - id: my-splunk
    type: splunk
    host: splunk.internal
    port: 8089
    password: ${env:SPLUNK_TOKEN}
    database: search           # Splunk app name (optional)
    mapping:
      use_token: true          # default; false = username/password auth
      datamodel_filter: ""     # optional Splunk Data Model filter
      disable_ssl_validation: false
```

#### Controlli di qualità dei dati (soda / great_expectations) {: #data-quality-checkers-soda-great_expectations }

Un'origine di controllo punta all'endpoint pgwire di Provisa stesso, così un solo driver postgres scansiona la vista federata di una tabella basata su Snowflake o Iceberg. L'identità della scansione la dichiarate voi; non viene mai ereditata. Su quella connessione si applica la policy, e un insieme di righe filtrato non deve far passare un controllo in silenzio. Le chiavi di connessione vengono da `mapping`: `host`, `port`, `database`, `user`, `password`.

```yaml
sources:
  - id: dq
    type: soda                 # or great_expectations
    domain_id: sales-analytics
    mapping:
      host: localhost
      port: 5439               # Provisa's pgwire endpoint
      database: provisa
      user: dq_scanner
      password: ${env:PROVISA_DQ_PASSWORD}
```

Ogni tabella di risultati porta `dq_contract`, lo YAML del contratto Soda o il JSON della suite Great Expectations parola per parola. Colonne, watermark e promozioni ne derivano; vedi come in [Controlli di qualità dei dati](sources.md#data-quality-checkers).

**Scelta all'installazione.** Il checker non viene collegato: la scansione gira in un interprete figlio, e la libreria viene installata solo quando un operatore la nomina. Ogni percorso di installazione (`install.sh`, `packaging/linux/first-launch.sh` e la procedura guidata macOS tramite `PROVISA_DQ_CHECKER`) scrive la scelta in `~/.provisa/config.yaml`:

```yaml
dq_checker: none        # none | soda | gx
```

`scripts/provisa` legge questa chiave ed esporta `PROVISA_EXTRAS`, che `docker-compose.app.yml` passa come build arg al `Dockerfile`'s `ARG PROVISA_EXTRAS`:

| `dq_checker` | `PROVISA_EXTRAS` (tier Docker) | Installazione venv nativa |
| -------------- | -------------------------------- | --------------------- |
| `none` | `firebase,vector` | `provisa[embedded]` |
| `soda` | `firebase,vector,soda` | `provisa[embedded,soda]` |
| `gx` | `firebase,vector,gx` | `provisa[embedded,gx]` |

Installare il dataset demo porta `none` a `gx` e lo segnala, perché la configurazione demo registra una suite Great Expectations su `pet_store.pets` e la sua scorecard di qualità altrimenti non avrebbe nulla da mostrare. Nominare `soda` mantiene `soda`.

Se arrivate alla demo tramite pip invece che con un programma di installazione, saltate quel passo della procedura guidata, quindi l'extra `demo` porta lo stesso checker: `provisa run --demo` richiede `pip install 'provisa[embedded,demo]'` perché la sua scansione giri. Senza, la scansione segnala `data-quality checker 'great_expectations' is not installed` e indica il comando di installazione.

Qualsiasi altro valore ferma il launcher, che quindi non parte mai senza il checker richiesto dall'operatore. L'extra `soda` installa `soda-postgres`, e `gx` installa `great-expectations[postgresql]`. Soda Core è sotto la Elastic License 2.0: `config/capabilities.yaml` marca l'opzione `cloud_eligible: false`, e il piano ospitato la rifiuta.

## Domini {: #domains }

```yaml
domains:
  - id: sales-analytics
    description: Sales operational data
```

## Naming

```yaml
naming:
  convention: apollo_graphql   # snake, hasura_graphql, apollo_graphql (default)
  domain_prefix: true          # prepend domain_id__ to all GraphQL names
  rules:
    - pattern: "^prod_pg_"
      replace: ""
```

### Convenzione di naming {: #naming-convention }

L'autorità di naming è la fonte di verità unica per i nomi rivolti al client; i nomi fisici delle colonne di backend non sono mai esposti ai client. Ogni linguaggio di query deriva il nome di una colonna dal suo `column.alias` se impostato, altrimenti dal nome fisico della colonna tramite la sua convenzione configurata.

La convenzione GraphQL è una di tre enum preimpostate. Le vecchie stringhe libere (`none`, `snake_case`, `camelCase`, `PascalCase`) sono deprecate.

| Preset | Default | Nomi dei tipi | Nomi dei campi | Nomi delle mutation |
| -------- | --------- | ------------ | ------------- | ---------------- |
| `apollo_graphql` | sì | PascalCase | camelCase | camelCase |
| `hasura_graphql` | | PascalCase | camelCase | snake_case |
| `snake` | | PascalCase | snake_case | snake_case |

La convenzione GraphQL di default è `apollo_graphql`, che produce nomi di campo e mutation in camelCase. La convenzione SQL è separata, con default `snake_case`, applicata tramite `apply_sql_name()`; la convenzione GraphQL viene applicata tramite `apply_gql_name()`, e il nome CQL viene derivato dal nome GraphQL.

`domain_prefix: bool` è un'opzione ortogonale che si applica indipendentemente dal preset scelto.

L'`column.alias` esplicito è il nome canonico: SQL lo usa alla lettera senza applicare alcuna convenzione, GraphQL vi applica la propria convenzione, e CQL deriva dal nome GraphQL.

Override per origine:

```yaml
sources:
  - id: legacy-db
    naming_convention: hasura_graphql  # overrides global for this source
```

Override per tabella:

```yaml
tables:
  - source_id: legacy-db
    table: orders
    naming_convention: snake  # overrides source for this table
```

### Prefisso di dominio {: #domain-prefix }

Quando `domain_prefix: true`, tutti i nomi di campo e tipo GraphQL vengono prefissati con l'ID di dominio usando un separatore a doppio underscore:

| Tabella | Dominio | Nome campo |
| ------- | -------- | ----------- |
| `orders` | `sales-analytics` | `sales_analytics__orders` |
| `customer_segments` | `customer-insights` | `customer_insights__customer_segments` |

Questo previene collisioni di nomi quando domini diversi hanno tabelle con lo stesso nome, e rende le query autodocumentanti.

### Regole di naming {: #naming-rules }

Regole regex applicate ai nomi delle tabelle nella generazione dei nomi di campo GraphQL. Applicate in ordine prima della risoluzione dell'unicità.

## Tabelle {: #tables }

```yaml
tables:
  - source_id: sales-pg
    domain_id: sales-analytics
    schema: public
    table: orders
    alias: purchase_orders     # optional: override GraphQL name
    description: "Customer purchase orders"  # optional: GraphQL description
    columns:
      - name: id
        visible_to: [admin, analyst]
      - name: email
        visible_to: [admin, analyst]
        masks:                    # first entry the reader holds applies
          - to: [admin]           # admin sees raw
          - to: ["*"]             # everyone else sees it masked
            mask: {type: regex, pattern: "^(.{2}).*(@.*)$", replace: "$1***$2"}
        alias: email_address      # optional: override GraphQL field name
        description: "Primary email address"  # optional: appears in SDL
      - name: amount
        visible_to: [admin]
        masks:
          - to: [admin]
          - to: ["*"]
            mask: {type: constant, value: 0}
      - name: created_by
        visible_to: [admin, analyst]
      - name: created_at
        visible_to: [admin, analyst]
        masks:
          - to: [admin]
          - to: ["*"]
            mask: {type: truncate, precision: month}
    write_grants:                 # who may write this table, per role and operation
      - role: admin
        operation: insert
        columns: [email, amount]
        check: "TRUE"             # any row may be inserted
        presets:                  # set by the grant; the statement may not supply them
          - {column: created_by, source: session, name: user_id}
          - {column: created_at, source: now}
      - role: admin
        operation: update
        columns: [email, amount]
        rows: "TRUE"              # any row may be updated
        check: "TRUE"
```

Per impostazione predefinita una tabella non ha alcuna autorizzazione di scrittura, quindi nessun ruolo può scriverci. Un'autorizzazione dà a un ruolo un'operazione (`insert`, `update` o `delete`) sulla tabella. Una tabella contiene al massimo un'autorizzazione per ruolo e operazione.

| Campo dell'autorizzazione | Significato |
| ----------- | ------- |
| `columns` | Le colonne che un `INSERT` può elencare o un `UPDATE` può impostare. Un'autorizzazione `delete` non ne nomina nessuna. |
| `rows` | Le righe che un `UPDATE` o un `DELETE` può toccare: un predicato SQL sulle colonne della tabella. Un'autorizzazione `insert` non ne ha. |
| `check` | Ciò che ogni riga scritta da un `INSERT`, o lasciata da un `UPDATE`, deve soddisfare: un predicato SQL. Un'autorizzazione `delete` non ne ha. |
| `presets` | Colonne che l'autorizzazione imposta da sé. L'istruzione non può fornirle, quindi una colonna con preset non compare anche in `columns`. |

Se si omette `rows` o `check`, l'autorizzazione prende il filtro di righe in lettura del ruolo sulla tabella: un ruolo scrive le righe che legge, salvo diversa indicazione dell'autorizzazione. `"TRUE"` significa ogni riga. Le vecchie chiavi `writable_by` della colonna e `column_presets` della tabella vengono rifiutate per nome; la configurazione non si carica finché non sono sostituite da `write_grants`.

Ogni scrittura su ogni superficie viene ammessa in quest'ordine: il ruolo detiene il diritto `write`; il ruolo detiene un'autorizzazione per l'operazione sulla tabella; ogni colonna scritta dall'istruzione è nell'autorizzazione; i preset dell'autorizzazione vengono aggiunti all'istruzione; il predicato `rows` viene combinato con AND nel `WHERE` di un `UPDATE` o `DELETE`; e `check` viene valutato sulle nuove righe prima che venga inviato qualcosa alla sorgente. Un'istruzione che `check` non può decidere viene rifiutata. Le colonne dell'autorizzazione decidono anche quali campi di mutazione GraphQL offre a un ruolo: un campo per ogni operazione per cui ha un'autorizzazione, con esattamente quelle colonne.

Alcune istruzioni richiedono un'autorizzazione più ampia. `TRUNCATE` richiede un'autorizzazione `delete` su ogni riga. `MERGE` richiede un'autorizzazione `insert` e una `update`, entrambe su ogni riga. `INSERT ... ON CONFLICT DO UPDATE` richiede un'autorizzazione `update` su ogni riga senza preset. `COPY ... FROM STDIN` richiede un'autorizzazione `insert` senza preset.

Le autorizzazioni sono ereditate. Un ruolo figlio prende, per tabella e operazione, l'autorizzazione del ruolo più vicino nella sua catena che ne detiene una, la propria per prima. Un meta-ruolo detiene il ripiegamento delle autorizzazioni dei suoi membri: l'unione delle loro colonne, l'OR dei loro predicati `rows` e `check`, e solo i preset che tutti i membri impostano allo stesso modo.

Nell'interfaccia di amministrazione, la sezione **Scritture** di una tabella elenca le autorizzazioni e le modifica insieme.

### Alias {: #aliases }

Gli alias di tabella e colonna sovrascrivono il nome GraphQL di default. Utili per:

- Rinominare nomi di database criptici (es. `tbl_cust_seg` → `customer_segments`)
- Evitare abbreviazioni nel layer API
- Creare un vocabolario pulito, specifico per dominio

### Descrizioni {: #descriptions }

Le descrizioni di tabella e colonna sono incluse nell'SDL GraphQL generato. Appaiono nell'esploratore di documentazione di GraphiQL e nelle query di introspezione. Impostale nella configurazione YAML o tramite la UI di amministrazione.

### Path (estrazione JSON calcolata) {: #path-computed-json-extraction }

Le colonne possono estrarre valori da una colonna sorgente JSON/JSONB usando un `path` in notazione a punti. Utile per dati semi-strutturati in messaggi Kafka, documenti MongoDB o colonne JSONB PostgreSQL.

```yaml
columns:
  - name: payload
    type: varchar
    visible_to: []            # hide the raw JSON column
  - name: order_id
    type: integer
    path: payload.order_id    # extracts from payload column
    visible_to: [admin, analyst]
  - name: customer_name
    type: varchar
    path: payload.customer.name
    visible_to: [admin, analyst]
```

Il formato del path è `source_column.key1.key2...`. Il compilatore genera `json_extract_scalar(source_column, '$.key1.key2')` nell'SQL.

**Impatto sul routing:** Le colonne path usano gli operatori JSON di PostgreSQL (`->>`), che sono supportati nativamente dal routing diretto verso PG. Per origini non PostgreSQL (MySQL, SQL Server, ecc.), le query con colonne path vengono automaticamente instradate attraverso il motore di federazione. Le mutation non sono interessate poiché le colonne path sono campi calcolati in sola lettura.

### Tipi di mascheramento {: #masking-types }

| Tipo | Campi | Descrizione |
| ------ | -------- | ------------- |
| `regex` | `pattern`, `replace` | REGEXP_REPLACE (solo colonne stringa) |
| `constant` | `value` | Sostituzione letterale (NULL, 0, MAX, MIN, personalizzato) |
| `truncate` | `precision` | DATE_TRUNC (solo colonne data/timestamp) |

## Relazioni {: #relationships }

```yaml
relationships:
  - id: orders-to-customers
    source_table_id: orders
    target_table_id: customers
    source_column: customer_id
    target_column: id
    cardinality: many-to-one   # or: one-to-many

  - id: orders-to-reviews
    source_table_id: orders        # sales-pg source
    target_table_id: product_reviews  # reviews-mongo source
    source_column: product_id
    target_column: product_id
    cardinality: one-to-many
    materialize: true              # auto-create MV for this cross-source join
    refresh_interval: 600          # refresh every 10 minutes
```

### Auto-materializzazione {: #auto-materialization }

Imposta `materialize: true` su una relazione per generare automaticamente una vista materializzata per i JOIN cross-source. Questo evita costose query federate pre-calcolando il risultato del JOIN.

- Solo le relazioni cross-source generano MV (i JOIN sulla stessa origine sono già veloci)
- Su una relazione basata su una giunzione la MV copre la traversata a due salti: salto di origine, salto della giunzione, discriminante e le colonne proprie della giunzione come attributi dell'arco. La giunzione conta come una gamba, quindi un arco è cross-source quando una qualsiasi delle tre tabelle sta in un'origine diversa
- La MV parte stale e viene popolata dal loop di refresh in background
- Le mutation su una delle due tabelle sorgente marcano la MV come stale per un nuovo refresh
- `refresh_interval` è di default 300 secondi (5 minuti)

## Ruoli {: #roles }

```yaml
roles:
  - id: admin
    description: Administers sources, tables, relationships and security.
    capabilities:
      - source_registration
      - table_registration
      - relationship_registration
      - security_config
      - query_development
      - full_results
      - admin
    domain_access: ["*"]
  - id: analyst
    description: Queries the sales analytics domain.
    capabilities: [query_development]
    domain_access: [sales-analytics]
  - id: junior_analyst
    description: A new analyst, with the analyst's access and nothing more.
    capabilities: []
    domain_access: [sales-analytics]
    parent_role_id: analyst      # inherits query_development + sales-analytics
```

I ruoli con `parent_role_id` ereditano capability, accesso ai domini, concessioni su colonne e oggetti, e regole RLS dal ruolo padre, con la regola RLS propria del figlio per una tabella che ha la precedenza. La catena viene appiattita all'avvio.

### Capability {: #capabilities }

| Capability | Descrizione |
| ----------- | ------------- |
| `source_registration` | Registra origini dati |
| `table_registration` | Registra tabelle |
| `relationship_registration` | Definisce relazioni |
| `security_config` | Configura RLS, mascheramento |
| `query_development` | Esegue query |
| `full_results` | Bypassa i limiti di campionamento |
| `admin` | Tutte le capability |

## Regole RLS {: #rls-rules }

```yaml
rls_rules:
  - table_id: orders
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"
    description: Analysts read only their own region's orders.
```

## Viste materializzate {: #materialized-views }

```yaml
materialized_views:
  - id: mv-orders-customers
    source_tables: [orders, customers]
    join_pattern:
      left_table: orders
      left_column: customer_id
      right_table: customers
      right_column: id
      join_type: left
      # Add via_table with via_left_column/via_right_column (and
      # via_type_column/via_type_value when the junction is discriminated) to
      # cover a two-hop junction traversal instead of a direct join.
    target_catalog: postgresql
    target_schema: mv_cache
    refresh_interval: 300
    enabled: true
```

## Viste (dataset calcolati governati) {: #views-governed-computed-datasets }

Le viste sono dataset calcolati definiti in SQL con governance completa a livello di colonna. Sono il meccanismo governato per aggiungere aggregazioni, trasformazioni e metriche derivate al modello.

```yaml
views:
  - id: monthly-revenue
    sql: |
      SELECT DATE_TRUNC('month', created_at) AS month,
             region,
             SUM(amount) AS revenue,
             COUNT(*) AS order_count
      FROM orders
      GROUP BY 1, 2
    description: "Monthly revenue by region"
    domain_id: sales-analytics
    materialize: true
    refresh_interval: 3600
    columns:
      - name: month
        visible_to: [admin, analyst]
      - name: region
        visible_to: [admin, analyst]
      - name: revenue
        visible_to: [admin]
      - name: order_count
        visible_to: [admin, analyst]
```

| Campo | Obbligatorio | Descrizione |
| ------- | ---------- | ------------- |
| `id` | Sì | Identificatore univoco della vista |
| `sql` | Sì | Istruzione SQL SELECT che definisce la vista |
| `domain_id` | Sì | Dominio per la visibilità dello schema |
| `materialize` | No | `true` = refresh CTAS periodico, `false` = vista federata dal vivo |
| `refresh_interval` | No | Secondi tra i refresh (solo materializzate, default 300) |
| `description` | No | Appare nell'SDL GraphQL |
| `alias` | No | Sovrascrive il nome GraphQL |
| `columns` | Sì | Definizioni di colonna con visibilità, mascheramento, descrizioni |

### Materializzate vs dal vivo {: #materialized-vs-live }

- **`materialize: true`**: Provisa crea una tabella tramite CTAS e la aggiorna su pianificazione. Query più veloci ma i dati possono essere obsoleti fino a `refresh_interval` secondi.
- **`materialize: false`**: Provisa crea una vista federata. Le query restituiscono sempre dati dal vivo ma possono essere più lente per aggregazioni complesse.

Le viste passano dalla stessa pipeline di governance delle tabelle: RLS, mascheramento, campionamento e visibilità per ruolo. Così niente può aggiungere nuova semantica alla piattaforma senza la supervisione di uno steward.

### Viste con cronologia (viaggio nel tempo) {: #views-with-history-time-travel }

Una tabella o una vista materializzata può conservare ogni versione delle sue righe ed essere letta a un momento passato. Per farlo si mappano tre colonne dell'origine, e si dichiarano tutte e tre o nessuna:

```yaml
views:
  - id: customer-history
    sql: SELECT customer_id, tier, effective_date, updated_at FROM crm.customers
    domain_id: sales
    materialize: true
    business_key: [customer_id]
    valid_time: effective_date
    recorded_time: updated_at
    columns:
      - {name: customer_id, visible_to: [analyst]}
      - {name: tier, visible_to: [analyst]}
      - {name: effective_date, visible_to: [analyst]}
      - {name: updated_at, visible_to: [analyst]}
```

| Campo | Descrizione |
| ------- | ------------- |
| `business_key` | Le colonne che identificano la stessa cosa tra le versioni, ad esempio `transaction_id`. Tutte le versioni di un record condividono questa chiave. |
| `valid_time` | La colonna con la data che l'azienda dà al record: quando è stato concordato, registrato in contabilità o è entrato in vigore. |
| `recorded_time` | La colonna con il momento in cui questa versione del record è stata inserita o modificata, ad esempio `updated_at`. |

Ognuna nomina una colonna della tabella o della vista. Una vista che le dichiara imposta `materialize: true`, e Provisa sceglie come conservare le versioni. Una voce sotto `tables:` accetta le stesse tre chiavi.

I due tempi hanno altri nomi: il tempo di registrazione (recorded time), chiamato tempo di transazione nella letteratura sui database temporali e tempo di sistema in SQL:2011; il tempo di validità (valid time), chiamato tempo applicativo in SQL:2011 o data di efficacia.

Per la stessa versione i due possono differire:

| Esempio | `valid_time` | `recorded_time` |
| ------- | ------------ | --------------- |
| Un pagamento di 100 del 3 ottobre viene corretto a 120 il 5 ottobre. | La correzione mantiene il 3 ottobre. | La correzione riceve il 5 ottobre. |
| Una misura sul campo presa il 3 ottobre alle 14:10 viene inserita il 6 ottobre. Un refuso viene corretto il 7 ottobre. | 3 ottobre 14:10, mantenuto dalla correzione. | 6 ottobre. La correzione riceve il 7 ottobre. |
| Un'operazione viene registrata all'esecuzione con data operazione 3 ottobre. Il 4 ottobre la quantità risulta errata e viene modificata. | Data operazione 3 ottobre, mantenuta dalla modifica. | 3 ottobre 10:02. La modifica riceve il 4 ottobre. |

Letta come registrata il 3 ottobre, l'operazione mostra la quantità errata. Letta secondo quanto si sa ora, mostra quella corretta.

Una versione è in vigore dal suo momento a quello della versione successiva. Una cancellazione ha effetto quando Provisa la registra.

**Lettura.** Una lettura semplice restituisce la versione di ogni chiave in vigore adesso. L'intestazione `X-Provisa-As-Of` legge a un istante, o su un intervallo dato come intervallo ISO 8601 `start/end`:

```
X-Provisa-As-Of: 2026-03-01T00:00:00
X-Provisa-As-Of: 2026-01-01T00:00:00/2026-04-01T00:00:00
```

Un istante restituisce la versione di ogni chiave in vigore in quel momento. Un intervallo restituisce ogni versione in vigore in qualsiasi momento al suo interno, una riga per versione.

**Come registrato.** Una correzione registrata in seguito cambia ciò che una vista dice del passato: la versione in vigore il 30 settembre è quella corretta una volta registrata la correzione. Per leggere la vista come Provisa l'aveva registrata in un momento, prima di qualsiasi correzione successiva, aggiungete `X-Provisa-As-Recorded`, un istante ISO 8601:

```
X-Provisa-As-Of: 2026-09-30T12:00:00
X-Provisa-As-Recorded: 2026-10-01T00:00:00
```

Ogni versione registrata dopo quel momento resta fuori, e l'as-of sceglie poi la versione in vigore al proprio momento. Con `X-Provisa-As-Recorded` da solo, la lettura restituisce la versione di ogni chiave in vigore secondo quanto registrato allora. Accetta un istante, non un intervallo; un intervallo o un valore malformato viene rifiutato con HTTP 400 (`data.invalid_as_recorded`).

Entrambi gli istanti arrivano a Provisa anche senza le intestazioni. L'istante as-of è l'argomento `as_of` di `resolve` (GraphQL, gRPC, MCP), il parametro di query REST `as_of` e l'opzione `as_of` di un ticket Arrow Flight, e un intervallo passato a `resolve` restituisce le `versions` della riga al posto della sua `row`. L'istante as-recorded è l'argomento `as_recorded` di `resolve`, il parametro di query REST `as_recorded` e l'opzione `as_recorded` di un ticket Arrow Flight. Il record di audit di ogni istruzione conserva entrambi gli istanti con cui ha letto, `as_of` e `as_recorded`. Diritti, maschere e regole di riga si applicano come per qualsiasi lettura. Un valore as-of malformato, o un intervallo la cui fine non è dopo l'inizio, viene rifiutato con HTTP 400 (`data.invalid_as_of`).

Ogni vista con cronologia restituisce tre colonne in più, concesse come lo è la sua colonna dell'ora di origine (altrimenti la sua prima colonna chiave):

| Colonna | Significato |
| ------- | ------- |
| `source_from` | Quando questa versione è entrata in vigore: la sua ora di origine, o la sua ora di caricamento in mancanza |
| `source_to` | Quando è entrata in vigore la versione successiva; vuota finché questa è in vigore |
| `recorded_at` | Quando Provisa ha registrato questa versione |

**Ora di origine dalla change data capture.** Una tabella atterrata tramite change data capture porta la sua ora di commit come `source_committed_at`, perché una vista con cronologia la indichi come ora di origine:

- Debezium: l'ora di commit del database che ogni evento di modifica porta (`source.ts_ms`). Una riga copiata da una costruzione completa della replica non ha evento di modifica, quindi il suo `source_committed_at` è vuoto finché la riga non cambia di nuovo, a meno che la sorgente sia PostgreSQL con i timestamp di commit attivi (punto seguente).
- PostgreSQL con il segnale di modifica `native`: l'ora di commit che PostgreSQL conserva per ogni riga (`pg_xact_commit_timestamp(xmin)`), letta dal driver proprio della sorgente quando viene costruita la replica. PostgreSQL la conserva solo con `track_commit_timestamp = on`. Attivatelo per ottenere l'ora di origine con il segnale nativo; senza di esso `source_committed_at` è vuoto. Attivare o disattivare l'impostazione ricostruisce la replica.

I topic Kafka senza involucro Debezium non portano ora di commit.

### Viste su un calendario {: #views-on-a-calendar }

Una vista materializzata può prendere la sua pianificazione da un calendario: viene costruita alla chiusura di ogni finestra che il calendario le dà (un giorno lavorativo, una settimana, un mese, un trimestre o un anno), e una vista che conserva lo storico marca ogni versione con quella chiusura. Una configurazione dichiara i suoi calendari sotto `calendars:`:

```yaml
calendars:
  - name: bank
    version: v1
    tz: America/New_York
    day_close: "18:00"
    holidays: [2026-11-26, 2026-12-25]

views:
  - id: daily-totals
    sql: |
      SELECT store, business_date, SUM(amount) AS total
      FROM fin.payments
      GROUP BY store, business_date
    domain_id: finance
    materialize: true
    calendar: bank
    grain: daily
    business_day_grain: true
    columns:
      - {name: store, visible_to: [analyst]}
      - {name: business_date, visible_to: [analyst]}
      - {name: total, visible_to: [analyst]}
```

| Campo | Obbligatorio | Descrizione |
| ------- | ---------- | ------------- |
| `name` | Sì | Il nome che una vista indica come suo `calendar` |
| `version` | Sì | L'etichetta di versione. Un nome può avere più versioni, ognuna con le proprie festività. |
| `base_system` | No | `gregorian` (predefinito), `fiscal` o `retail_445` |
| `tz` | No | Il fuso orario IANA in cui cadono i giorni del calendario (predefinito `UTC`) |
| `fiscal_anchor_month`, `fiscal_anchor_day` | No | Il mese e il giorno in cui inizia un anno fiscale (predefinito 1 e 1) |
| `retail_anchor` | No | La prima data di un anno 4-4-5 di riferimento; `retail_445` la richiede |
| `week_start` | No | Il giorno in cui inizia una settimana, 0 = lunedì fino a 6 = domenica (predefinito 0) |
| `holidays` | No | Date che non sono giorni lavorativi |
| `weekend` | No | I giorni della settimana che non sono lavorativi, 0 = lunedì (predefinito `[5, 6]`) |
| `day_close` | No | L'ora locale in cui chiude un giorno lavorativo, `"HH:MM"`. Il giorno D va allora dalla chiusura del giorno prima alla chiusura di D. Senza di essa un giorno finisce a mezzanotte. Mettetela tra virgolette: YAML legge un `18:00` senza virgolette come un numero. |

Un calendario viene memorizzato prima delle tabelle, così le viste che lo nominano lo trovano. Una voce che la mutation `createCalendar` rifiuterebbe viene rifiutata con lo stesso messaggio `invalid calendar: ...`, e la configurazione non viene caricata. L'esportazione del modello scrive ogni calendario sotto `calendars:`.

Una vista nomina il suo calendario con queste chiavi. Una voce sotto `tables:` le prende come `mv_calendar`, `mv_grain`, `mv_business_day_grain`, `mv_allowed_lateness` e `mv_expected_events`.

| Campo | Descrizione |
| ------- | ------------- |
| `calendar` | Il `name` del calendario |
| `grain` | `daily`, `weekly`, `monthly`, `quarterly` o `annual`; un n-esimo giorno della settimana come `3WE` (terzo mercoledì) o `LFR` (ultimo venerdì); oppure una regola RFC 5545 come `FREQ=WEEKLY;INTERVAL=2;BYDAY=MO` |
| `business_day_grain` | `true`: una finestra giornaliera esiste solo in un giorno lavorativo, quindi nessuna build viene eseguita nel fine settimana o in un giorno festivo |
| `allowed_lateness` | Secondi dopo la chiusura di una finestra in cui la build attende l'input confermato in ritardo (predefinito 0) |
| `expected_events` | Gli input che devono essere aggiornati fino alla chiusura della finestra. Se omesso, ogni input che la vista legge; `[]`, nessuno. |

Una vista che nomina un calendario che nessuna configurazione né `createCalendar` ha dichiarato viene rifiutata per nome all'avvio del ciclo degli eventi: `unknown calendar 'ledger'; registered: [...]`.

**Costruire un giorno lavorativo.** L'argomento `window` della mutation `refreshMv`, una data, costruisce la vista sigillata alla chiusura di quel giorno lavorativo:

```graphql
mutation {
  refreshMv(mvId: "daily-totals", window: "2026-10-02") { success message code }
}
```

Ciò che la build aggiunge viene marcato con la chiusura della finestra del calendario per quella data, come la marcherebbe la build alla chiusura stessa, quindi una lettura come registrato a quella chiusura la restituisce. Una vista con storico costruita così ricostruisce le date il cui input è cambiato dalla sua ultima build, quindi una correzione registrata prima della build del giorno dopo viene recepita per la sua data. Una vista che non nomina alcun calendario viene rifiutata (`schema.mv_window_needs_calendar`), e così una data in cui il suo calendario non ha una finestra, come un giorno festivo con una granularità a giorni lavorativi.

### Viste di sola lettura {: #query-only-views }

Sia le viste `materialize: true` che `materialize: false` espongono il proprio tipo GraphQL come query-only. Nessuna mutation di insert, upsert, update o delete viene generata per le relazioni supportate da `view_sql`.

## Cache

```yaml
cache:
  enabled: true
  redis_url: ${env:REDIS_URL}
  default_ttl: 300
```

### Gerarchia della cache {: #cache-hierarchy }

Ordine di risoluzione del TTL (il più specifico vince): **tabella** > **origine** > **default globale**. Viene usato il primo valore non nullo.

```yaml
cache:
  enabled: true
  redis_url: ${env:REDIS_URL}
  default_ttl: 300              # global fallback: 5 minutes

sources:
  - id: sales-pg
    cache_enabled: true          # toggle caching for all tables in this source
    cache_ttl: 600               # source override: 10 minutes

tables:
  - source_id: sales-pg
    table: orders
    cache_ttl: 60                # table override: 1 minute (frequently changing)
  - source_id: sales-pg
    table: customers
    # no cache_ttl → inherits source TTL (600s)
```

Impostare `cache_enabled: false` su un'origine disabilita la cache per tutte le tabelle di quell'origine, indipendentemente dal TTL a livello di tabella. Le chiavi di cache includono sempre `role_id` + i valori di contesto RLS per la partizione di sicurezza.

### Attivazione per richiesta {: #per-request-opt-in }

La cache delle risposte è disattivata per ogni richiesta che non la chiede. Una richiesta la attiva con:

| Superficie | Attivazione |
| --- | --- |
| GraphQL (`/data/graphql`, Arrow Flight) | `@cached` sull'operazione, facoltativamente `@cached(ttl: 60)` |
| SQL (pgwire, Flight SQL, `/data/sql`) | una riga di commento `-- @provisa cache=true` o `-- @provisa cache_ttl=60` nell'istruzione |
| Cypher (`/data/cypher`, la Neo4j Query API, Bolt, Arrow Flight) | una riga di commento `// @provisa cache=true` o `// @provisa cache_ttl=60` nella query |
| gRPC (e il proxy gRPC HTTP) | metadati della chiamata (intestazioni sul proxy) `x-provisa-cache: true` o `x-provisa-cache-ttl: 60` |
| REST, JSON:API e, in alternativa all'indicazione nel testo: GraphQL su HTTP, `/data/cypher`, la Neo4j Query API | l'intestazione di richiesta `Cache-Control: max-age=60` (o `max-stale=60`), oppure `x-provisa-cache: true` / `x-provisa-cache-ttl: 60` |

Un `ttl` che non è un intero fa fallire la richiesta. Una richiesta che indica l'attivazione due volte (nel testo e in un'intestazione, o in due intestazioni) con valori diversi fallisce con `400`; nessuno dei due ha la precedenza. `Cache-Control: no-cache` o `no-store` significa nessuna cache, come senza intestazione.

Senza l'indicazione la richiesta non legge né scrive la cache. Con essa decidono comunque le impostazioni sopra: il risultato viene messo in cache solo se ogni origine letta ha `cache_enabled: true` e il TTL risolto di ogni tabella è maggiore di 0. La voce dura il `ttl` della richiesta se ne indica uno, altrimenti il TTL risolto più breve delle tabelle lette.

L'indicazione accetta solo dati più vecchi in cambio di velocità. Non rende mai una lettura più recente di quanto consentano le impostazioni dell'operatore: la freschezza dell'atterraggio e della replica (`cache_ttl`, `change_signal`), `row_materialize`, gli snapshot `load_protected` e i controlli di freschezza valgono per una richiesta con indicazione esattamente come per una senza. Vedere [Freschezza e carico: chi decide](#freshness-and-load-who-decides).

`@noCache` e `-- @provisa no_cache=true` non esistono più: con la cache disattivata per impostazione predefinita non c'è nulla da aggirare.

Una scrittura invalida ogni voce di cache che l'organizzazione che scrive detiene per le tabelle scritte; le voci di un'altra organizzazione per la stessa tabella restano intatte. Se l'invalidazione fallisce, la richiesta fallisce invece di lasciare voci obsolete.

## Freschezza e carico: chi decide {: #freshness-and-load-who-decides }

Tre parti incidono su quanto è recente un risultato e su quanto carico una query impone ai sistemi a monte.

- **La sorgente a monte.** Idealmente gestisce da sé la propria contropressione: limiti di connessione, timeout delle istruzioni, repliche di lettura.
- **L'operatore.** Protegge la piattaforma, Provisa stessa, dalla contropressione. Quando una sorgente non può proteggersi, l'operatore protegge anche lei. Le impostazioni stanno su sorgenti e tabelle: `load_protected`, `replicate`, i `federation_hints` della sorgente, la soglia di reindirizzamento dei risultati grandi, il cluster Kafka su cui scrivono i sink e il watermark registrato di ogni tabella.
- **L'utente finale.** Bilancia velocità e attualità, una richiesta alla volta.

Le impostazioni dell'operatore sono il pavimento. Una richiesta può spostarsi sopra di esso, verso dati più vecchi o meno carico; attivare la cache delle risposte è l'esempio tipico. Non può mai scendere sotto. Un suggerimento di richiesta che lo farebbe viene rifiutato con un errore che nomina l'impostazione dell'operatore, senza essere applicato né ignorato in silenzio.

| Input della richiesta | Cosa può fare | Rifiutato quando |
| --- | --- | --- |
| `@route(engine: DIRECT)`, `-- @provisa route=direct` | Scegliere il driver diretto per una sorgente | una tabella letta dall'istruzione è `load_protected` o `replicate: 0` |
| `@join`, `@reorder`, `@broadcastSize`, `/*+ ... */` | Impostare una proprietà di sessione del motore lasciata libera dai `federation_hints` della sorgente | modifica una proprietà impostata dall'operatore |
| `X-Provisa-Redirect-Threshold`, `@redirect(threshold:)` | Reindirizzare un risultato prima | supera la soglia dell'operatore |
| `@sink(broker:)`, broker di `X-Provisa-Sink` | Ripetere il broker dell'operatore | nomina un altro broker, o `KAFKA_BOOTSTRAP_SERVERS` non è configurato |
| `@watermark` in una sottoscrizione | Ripetere il watermark registrato della tabella | nomina un'altra colonna, o la tabella non ne ha |
| `@cached`, `-- @provisa cache=true` | Servire un risultato più vecchio dalla cache | mai; una voce di cache non è mai più recente della lettura che l'ha salvata |

I rifiuti restituiscono HTTP 403 con il codice `query.operator_floor`, SQLSTATE `42501` su pgwire e `PERMISSION_DENIED` su Flight e gRPC.

Una tabella `load_protected` o con `replicate: 0` non viene mai letta dal vivo da una query, su nessun trasporto. Le query leggono la sua replica. Solo la replicazione e i suoi aggiornamenti leggono la sorgente: un aggiornamento avviene quando `cache_ttl` o un controllo di freschezza lo richiede; senza nessuno dei due la replica viene costruita una volta e poi aggiornata solo tramite un feed di modifiche o lo scheduler. Su una tabella così con una `off_peak_window` (sulla tabella o sulla sua origine, letta in `off_peak_tz`), un aggiornamento gira solo mentre la finestra è aperta: uno che scade o è richiesto fuori di essa attende la prossima apertura della finestra, e una finestra senza `cache_ttl` aggiorna una volta per apertura. La prima costruzione non viene mai trattenuta, perché le letture della tabella la attendono.

### L'impostazione `replicate` {: #the-replicate-setting }

`replicate` è un intero su una tabella. Una tabella senza valore prende quello della sua origine e, se nessuno dei due è impostato, vale la soglia globale. Una replica è una copia di una tabella di origine conservata nello store del motore e aggiornata secondo le impostazioni di tempestività della tabella; non è un flusso continuo di modifiche.

| Valore | Nome | Significato |
| --- | --- | --- |
| non impostato | Default | Vale la soglia globale |
| `-1` | Never | Letta dal vivo ovunque esista un percorso live. Replicata solo su un motore che non può leggere l'origine sul posto |
| `N` maggiore di 0 | Hot-N | Letta dal vivo finché la tabella non supera N istruzioni governate per intervallo, poi servita dalla sua replica |
| `0` | Always | Le letture provengono dalla replica |

Solo Always è una garanzia. Never e i valori Hot sono al meglio delle possibilità. `load_protected` con `replicate: -1` viene rifiutato al caricamento della configurazione e al salvataggio. Un'origine con `replicate: 0` non ha attach dal vivo su nessun motore, quindi ogni sua tabella è servita dalla propria replica.

Il vincolo si valuta sulle tabelle lette da un'istruzione. Un'istruzione che non legge alcuna tabella servita da una replica mantiene il percorso diretto anche quando la sua origine ha altre tabelle replicate.

Una replica è indicizzata sulla chiave primaria dichiarata e sulle colonne su cui si uniscono le relazioni dichiarate, con indici costruiti dopo il caricamento. Dove questi indici accelerano i join, dove conservare le repliche e come scegliere la loro freschezza è in [Replicas](replicas.md).

### Replicazione hot {: #hot-replication }

Una tabella il cui `replicate` non è impostato, o è `N` maggiore di 0, viene letta dal vivo finché non è molto richiesta.

```yaml
replication:
  hot_threshold: 100       # statements per interval for a table left at Default
  hot_interval: 60         # seconds: the window the count is taken over, and how often it is judged
  hot_max_rows: 10000000   # a larger table is not replicated for being busy
```

| Impostazione | Variabile d'ambiente | Default |
| --- | --- | --- |
| `replication.hot_threshold` | `PROVISA_REPLICATION_HOT_THRESHOLD` | 100 |
| `replication.hot_interval` | `PROVISA_REPLICATION_HOT_INTERVAL` | 60 |
| `replication.hot_max_rows` | `PROVISA_REPLICATION_HOT_MAX_ROWS` | 10000000 |

Tutte e tre sono impostazioni dell'operatore: l'interfaccia di amministrazione le mostra sotto Hot Tables e un valore salvato si applica alla valutazione successiva.

Come si conta una tabella:

- Un'istruzione governata conta ogni tabella che legge una sola volta, indipendentemente da quante volte la legge.
- Un'istruzione servita dalla cache delle risposte non conta. Non ha caricato la sorgente.
- Un'istruzione rifiutata dalla governance non conta.

Una tabella viene promossa quando il suo conteggio raggiunge la sua soglia (il proprio `N`, altrimenti quello della sua origine, altrimenti `hot_threshold`) e retrocessa quando il conteggio scende sotto la metà. La promozione richiede la costruzione di una replica; la tabella è letta dal vivo finché la costruzione non termina, poi dalla replica. La retrocessione riporta subito le letture alla sorgente.

Una tabella non viene promossa quando contiene più righe di `hot_max_rows`, quando il suo segnale di modifica è `ttl` o `ttl_probe` e né essa né la sua origine dichiarano un `cache_ttl`, o quando il livello hot di Redis la gestisce già. Il riepilogo della tabella nell'interfaccia di amministrazione indica quale caso si applica.

I conteggi risiedono in Redis. Con un Redis condiviso (`cache.redis_url`) coprono ogni processo della distribuzione. Con il Redis embedded sono per processo, quindi la promozione funziona solo quando un solo worker serve le richieste; con più worker e senza Redis condiviso nulla viene promosso, e il riepilogo lo segnala.

## Autenticazione {: #authentication }

```yaml
auth:
  provider: simple           # none, firebase, keycloak, oauth, simple
  superuser:
    username: admin
    password: ${env:PROVISA_SUPERUSER_PASSWORD}
  simple:
    allow: true
    jwt_secret: ${env:PROVISA_JWT_SECRET}
    users:
      - username: admin
        password_hash: "$2b$12$..."
        roles: [admin]
  role_mapping:
    - claim: groups
      contains: data-analysts
      provisa_role: analyst
    default_role: analyst
```

### Tipi di provider di autenticazione {: #auth-provider-types }

| Provider | Caso d'uso | Validazione del token |
| ---------- | ---------- | ----------------- |
| `simple` | Sviluppo/test locale. Utenti definiti in YAML. | JWT firmato con `PROVISA_JWT_SECRET` |
| `firebase` | Firebase Authentication (tutti i metodi). | `verify_id_token()` dell'SDK `firebase-admin` |
| `keycloak` | Keycloak OIDC. Ruoli di tenant + client mappati. | Validazione JWT basata su JWKS |
| `oauth` | OIDC generico (Okta, Azure AD, Auth0, PingFederate). | JWKS dall'URL di discovery |
| `basic` | Deployment autonomi. Gli account risiedono nell'archivio proprio di Provisa. | Password bcrypt, oppure SCRAM-SHA-256 su pgwire |

Le credenziali superuser (blocco `superuser`) funzionano con qualsiasi provider e si risolvono sempre nel ruolo admin con tutte le capability. Usate per la configurazione iniziale prima che l'autenticazione esterna sia configurata.

### SCRAM-SHA-256 (`auth.scram`)

```yaml
auth:
  provider: basic
  scram: true
```

Fa sì che pgwire annunci SASL con `SCRAM-SHA-256`, così una password viene dimostrata e mai inviata in chiaro. Vale solo per il provider `basic`, perché nessun altro provider conserva i verificatori RFC 5802 di cui SCRAM ha bisogno, e il channel binding non è offerto.

I verificatori non possono essere derivati dagli hash bcrypt esistenti. Ne viene scritto uno ogni volta che una password transita in chiaro, quindi la prima connessione SCRAM di ciascun utente segue la sua prossima registrazione, accesso, modifica della password o reimpostazione da parte dell'amministratore. Fino ad allora le connessioni di quell'utente ripiegano sullo scambio in chiaro su TLS; il canale non rivela chi ha già migrato.

### Limitazione dei tentativi di accesso (`auth.login_throttle`) {: #login-throttling-authlogin_throttle }

```yaml
auth:
  login_throttle:
    max_attempts: 5      # failures within the window before lockout
    window_seconds: 300  # how far back failures are counted
    lockout_seconds: 900 # how long a locked-out subject is refused
```

Attiva per impostazione predefinita con i valori mostrati; il blocco si limita a regolarli. Il contatore sta al livello di validazione delle credenziali, quindi i fallimenti su HTTP, pgwire e Bolt si accumulano sullo stesso soggetto e un blocco vale su ogni interfaccia. È per processo: più worker dell'API consentono ciascuno fino a `max_attempts`.

### Token di accesso personale {: #personal-access-tokens }

I PAT non richiedono un blocco di configurazione. Sono sempre accettati, e il loro archivio viene creato con il resto dello schema del piano di controllo. Ciò che si configura è la scadenza che un utente può chiedere all'emissione: da 1 a 366 giorni, oppure nessuna per un token che non scade. Vedi [Modello di sicurezza](security.md#personal-access-tokens).

### TLS reciproco {: #mutual-tls }

La verifica dei certificati client si configura con variabili d'ambiente, accanto alle impostazioni del certificato TLS che estende, e non ha posto in `provisa.yaml`.

| Variabile | Predefinito | Significato |
| ---------- | --------- | --------- |
| `PROVISA_MTLS_CLIENT_CA` | non impostata | Bundle PEM della o delle CA autorizzate a firmare i certificati client. Impostarla attiva la verifica dei certificati client |
| `PROVISA_MTLS_MODE` | `required` una volta impostata una CA | `required` oppure `optional` |
| `PROVISA_MTLS_BIND_PRINCIPAL` | `false` | Richiede che il common name del certificato coincida con il nome utente con cui la connessione si autentica |

Ognuna accetta un override per protocollo, con gli stessi nomi delle impostazioni TLS. Una modalità impostata senza CA, o una modalità che non è né `required` né `optional`, blocca l'avvio, così il server non serve mai connessioni che l'operatore crede verificate quando non lo sono.

### Indirizzare un'organizzazione su TLS {: #addressing-an-org-over-tls }

Nulla da configurare. In un deployment multi-organizzazione, pgwire e Bolt leggono l'organizzazione dal nome host composto dal client, trasportato nel ClientHello TLS, esattamente come HTTP la legge dall'header `Host`. Un client che si connette a `acme.provisa.dev` richiede l'organizzazione `acme`; la richiesta viene rifiutata a meno che il principal autenticato non ne sia membro. Connettersi tramite indirizzo IP non richiede alcuna organizzazione, ed è il caso di ogni connessione in un deployment a organizzazione singola.

### Esempio completo di configurazione auth (commentato) {: #full-auth-config-example-commented-out }

```yaml
# auth:
#   provider: firebase
#
#   superuser:
#     username: admin
#     password: ${env:PROVISA_SUPERUSER_PASSWORD}
#
#   firebase:
#     project_id: ${env:FIREBASE_PROJECT_ID}
#     service_account_key: ${env:FIREBASE_SERVICE_ACCOUNT}
#
#   # keycloak:
#   #   server_url: https://keycloak.example.com
#   #   # kc-tenant: set to your Keycloak tenant name (e.g. provisa)
#   #   client_id: provisa-app
#   #   client_secret: ${env:KEYCLOAK_CLIENT_SECRET}
#
#   # oauth:
#   #   discovery_url: https://login.example.com/.well-known/openid-configuration
#   #   client_id: provisa
#   #   client_secret: ${env:OAUTH_CLIENT_SECRET}
#   #   role_claim: groups
#   #   audience: provisa-api
#
#   role_mapping:
#     - claim: custom_claims.role
#       value: admin
#       provisa_role: admin
#     - claim: groups
#       contains: data-analysts
#       provisa_role: analyst
#     default_role: analyst
```

## Mutation upsert {: #upsert-mutations }

Per le tabelle con chiave primaria, Provisa genera campi di mutation `upsert_<table>`. Si compilano in un upsert nel dialetto di destinazione: `INSERT ... ON CONFLICT (pk) DO UPDATE SET ...` su PostgreSQL, `ON DUPLICATE KEY UPDATE` su MySQL.

```graphql
mutation {
  upsert_orders(objects: [{id: 1, amount: 150.00, region: "us"}]) {
    affected_rows
  }
}
```

Le colonne di conflitto vengono derivate dai metadati della PK. Si applicano tutte le regole di visibilità delle colonne e di permesso di scrittura.

## Distinct On

L'argomento `distinct_on` seleziona la prima riga per ciascun valore distinto delle colonne specificate. Disponibile sui campi query root.

```graphql
{
  orders(distinct_on: [region], order_by: [{region: asc, created_at: desc}]) {
    region
    amount
    created_at
  }
}
```

Compila in `SELECT DISTINCT ON (region) ...` su PostgreSQL. Per dialetti non PG, viene usato un fallback a window function.

## Preset delle autorizzazioni di scrittura {: #write-grant-presets }

Un'autorizzazione di scrittura imposta da sé alcune colonne in insert e update. I preset sono dichiarati nell'elenco `presets` dell'autorizzazione, per ruolo e operazione.

```yaml
tables:
  - source_id: sales-pg
    domain_id: sales-analytics
    schema: public
    table: orders
    columns:
      - {name: amount, visible_to: [analyst]}
      - {name: created_by, visible_to: [analyst]}
      - {name: created_at, visible_to: [analyst]}
      - {name: source_system, visible_to: [analyst]}
    write_grants:
      - role: analyst
        operation: insert
        columns: [amount]
        presets:
          - {column: created_by, source: session, name: user_id}   # from the request's session
          - {column: created_at, source: now}                      # time of the write
          - {column: source_system, source: literal, value: provisa}
```

| Origine | Comportamento |
| -------- | ---------- |
| `session` | Inietta il valore della variabile di sessione indicata (`name`) della richiesta |
| `now` | Inietta il momento della scrittura |
| `literal` | Inietta una costante (`value`) |

I preset vengono aggiunti all'istruzione durante la compilazione, prima della generazione dell'SQL. L'istruzione non può fornire una colonna con preset, e una colonna con preset non compare nel tipo di input della mutazione.

## Ruoli ereditati {: #inherited-roles }

I ruoli possono ereditare da un solo ruolo padre tramite `parent_role_id`. La catena viene appiattita all'avvio. Un figlio possiede l'unione delle capability e dell'accesso ai domini dei suoi antenati; una colonna, metrica, funzione o webhook concessi a un antenato sono concessi al figlio; e le regole RLS di un antenato si applicano al figlio per tabella, a partire dal ruolo più vicino, con la regola propria del figlio per una tabella che sostituisce quella del padre.

```yaml
roles:
  - id: admin
    description: Administers everything.
    capabilities: [admin]
    domain_access: ["*"]
  - id: analyst
    description: Queries the sales analytics domain.
    capabilities: [query_development]
    domain_access: [sales-analytics]
  - id: junior_analyst
    description: A new analyst, with the analyst's access and nothing more.
    capabilities: []
    domain_access: []
    parent_role_id: analyst      # inherits query_development + sales-analytics
  - id: intern
    description: An intern, with the junior analyst's access.
    capabilities: []
    domain_access: []
    parent_role_id: junior_analyst  # inherits from junior_analyst (and transitively analyst)
```

L'ereditarietà multilivello è supportata. Le capability esplicite e il domain_access del ruolo figlio vengono uniti con quelli del padre. Un padre deve essere un ruolo esistente, non può essere il ruolo stesso e non può chiudere un ciclo; ciascuno di questi casi viene rifiutato al salvataggio.

## Trigger pianificati {: #scheduled-triggers }

Trigger basati su cron che chiamano un URL webhook su pianificazione. Usa APScheduler.

```yaml
scheduled_triggers:
  - name: daily-report
    cron: "0 8 * * *"           # 8:00 AM daily
    webhook_url: https://hooks.example.com/daily-report
    enabled: true
  - name: hourly-sync
    cron: "0 * * * *"           # every hour
    webhook_url: https://hooks.example.com/sync
    enabled: false
```

I task pianificati vengono gestiti tramite la UI di amministrazione (toggle abilita/disabilita) o la mutation admin `toggle_scheduled_task`.

Un trigger può scattare su una tabella registrata invece che su una pianificazione: `on: {table, event}` indica la tabella come `source_id.schema.table` e l'evento, `change`, `error` o `quarantine`. Un trigger `command` esegue un comando registrato come il suo `role`, tramite l'invio governato proprio del comando, e gli passa come argomenti ciò che l'ha fatto scattare. Una modifica passa `table` e, dove il segnale di modifica lo porta (Kafka, Debezium), il lotto caricato come `changes`. Un evento `error` o `quarantine` passa `table`, `event`, `reason` ed `error`. Un trigger il cui comando dichiara un argomento che il trigger non passa viene rifiutato al salvataggio. Una modifica scatta una volta per ogni raffica segnalata dal suo segnale di modifica, o quando arriva una scrittura fatta tramite Provisa; una tabella servita dalla sua replica scatta quando una costruzione carica righe diverse dalla precedente. La scrittura stessa di un comando è una modifica, quindi i trigger si concatenano. Ogni esecuzione è registrata con l'ora, l'esito (`succeeded`, `refused` o `failed`), l'errore e ciò che l'ha fatta scattare, e la query di amministrazione `scheduledTasks` la elenca. Scattano solo i trigger di prod, e nulla viene installato in un database di origine.

```yaml
scheduled_triggers:
  - id: bronze_to_silver
    on: {table: bronze_pg.public.bronze, event: change}
    command: promote_to_silver
    role: loader
  - id: alert_on_quarantine
    on: {table: bronze_pg.public.bronze, event: quarantine}
    command: notify_steward      # declares arguments table, reason
    role: loader
```

## Formato OrderBy {: #orderby-format }

OrderBy usa il formato `{column: direction}` con un enum di direzione a 6 valori:

```graphql
{
  orders(order_by: [{created_at: desc_nulls_last}, {amount: asc}]) {
    id
    created_at
    amount
  }
}
```

| Direzione | SQL |
| ----------- | ----- |
| `asc` | `ASC` |
| `desc` | `DESC` |
| `asc_nulls_first` | `ASC NULLS FIRST` |
| `asc_nulls_last` | `ASC NULLS LAST` |
| `desc_nulls_first` | `DESC NULLS FIRST` |
| `desc_nulls_last` | `DESC NULLS LAST` |

L'ordinamento sulle relazioni è supportato tramite oggetti annidati:

```graphql
{
  orders(order_by: [{customers: {name: asc}}]) {
    id
    customers { name }
  }
}
```

## Osservabilità {: #observability }

```yaml
observability:
  endpoint: "http://localhost:4319"   # OTLP collector; env OTEL_EXPORTER_OTLP_ENDPOINT overrides
  service_name: provisa               # env OTEL_SERVICE_NAME overrides
  sample_rate: 1.0                    # 0.0–1.0; TraceIdRatioBased sampler
  log_level: WARNING                  # env OTEL_LOG_LEVEL overrides
  compact_batch_size: 1000
  telemetry_filter:
    redact_sql_literals: false        # strip literal values from db.statement before export
    redact_attributes: []             # attribute keys dropped entirely before export
  # support_endpoint: ""              # env PROVISA_SUPPORT_OTLP_ENDPOINT; off by default
  support_telemetry_filter:
    redact_sql_literals: true         # default on — strip literals before sending to support
    redact_attributes: []             # additional keys dropped before sending to support
```

### Filtri di telemetria {: #telemetry-filters }

Provisa gestisce due percorsi di esportazione OTLP indipendenti: il vostro collector interno e l'endpoint di supporto Provisa, facoltativo. Ogni percorso ha il proprio filtro. I filtri girano prima che gli span lascino il processo, e gli oggetti span originali non vengono mai modificati.

**`telemetry_filter`** — controlla cosa raggiunge il tuo collector interno.

| Chiave | Tipo | Default | Descrizione |
| ----- | ------ | --------- | ------------- |
| `redact_sql_literals` | bool | `false` | Sostituisce i letterali stringa e numerici in `db.statement` con `?` |
| `redact_attributes` | list[str] | `[]` | Chiavi di attributo eliminate completamente da ogni span |

**`support_telemetry_filter`** — controlla cosa raggiunge l'endpoint di supporto Provisa. La redazione dei letterali SQL è di default `true` su questo percorso, poiché i dati delle query appartengono a te.

| Chiave | Tipo | Default | Descrizione |
| ----- | ------ | --------- | ------------- |
| `redact_sql_literals` | bool | `true` | Sostituisce i letterali stringa e numerici in `db.statement` con `?` |
| `redact_attributes` | list[str] | `[]` | Chiavi di attributo eliminate completamente da ogni span |

Un `db.statement` redatto. Con `redact_sql_literals: true`, questo attributo di span:

```yaml
db.statement: SELECT * FROM orders WHERE region = 'us-west' AND amount > 500
```

diventa:

```yaml
db.statement: SELECT * FROM orders WHERE region = ? AND amount > ?
```

### Endpoint di supporto {: #support-endpoint }

`support_endpoint` (o la variabile d'ambiente `PROVISA_SUPPORT_OTLP_ENDPOINT`) inoltra la telemetria al supporto Provisa per la diagnostica. Se non è impostato, nessun dato lascia la vostra infrastruttura per questa via. Il filtro di supporto funziona indipendentemente da quello interno, quindi potete oscurare i letterali SQL in entrambe le esportazioni e condividere comunque con il supporto i tempi degli span e i dati degli errori.

### Rilevamento del protocollo dell'endpoint {: #endpoint-protocol-detection }

Provisa seleziona OTLP/HTTP o OTLP/gRPC in base allo schema dell'URL dell'endpoint. Gli URL che iniziano con `http://` o `https://` usano OTLP/HTTP, con `/v1/traces`, `/v1/metrics` e `/v1/logs` aggiunti automaticamente. Qualsiasi altro schema usa OTLP/gRPC con `insecure=True`.

## Motore di federazione {: #federation-engine }

Configurare un motore di federazione è facoltativo. Il predefinito è `duckdb`: senza configurazione, nel processo, senza servizi esterni. Scegliete un altro motore quando vi serve la scala MPP o volete riusare un warehouse che avete già.

Precedenza: variabile d'ambiente `PROVISA_ENGINE` → campo di configurazione persistito `federation_engine` della UI di amministrazione → `duckdb`. Le modifiche hanno effetto al riavvio del servizio.

### Panoramica dei motori {: #engine-overview }

| Chiave motore | Etichetta | Dialetto | MPP | Meccanismo di link esterno | Auth |
| ----------- | ------- | --------- | ----- | ------------------------ | ------ |
| `trino` | Provisa Federation Engine | Trino SQL | Sì | Cataloghi Trino (ampio set di connettori) | Credenziali JDBC |
| `trino-byo` | Trino | Trino SQL | Sì | Stesso di `trino`; coordinator non gestito | Credenziali JDBC |
| `pg` | PostgreSQL | PostgreSQL | No | FDW / pg_duckdb | Credenziali PostgreSQL |
| `duckdb` | DuckDB | DuckDB | No | ATTACH nativo dell'estensione | Nessuna (in-process) |
| `clickhouse` | ClickHouse (embedded) | ClickHouse | Sì | Motori tabella S3 / IcebergS3 / DeltaLake | chdb (in-process, nessuna auth) |
| `clickhouse-server` | ClickHouse (Server / Cloud) | ClickHouse | Sì | Motori tabella S3 / IcebergS3 / DeltaLake | Credenziali ClickHouse |
| `snowflake` | Snowflake | Snowflake | Sì | External stage + external table | `PROVISA_ENGINE_URL` |
| `databricks` | Databricks | Databricks SQL | Sì | Tabelle esterne Unity Catalog via REST | `PROVISA_ENGINE_URL` (bearer token + `http_path`) |
| `bigquery` | BigQuery | BigQuery | Sì | Tabelle esterne BigQuery / BigLake | `GOOGLE_APPLICATION_CREDENTIALS` |
| `fabric` | Microsoft Fabric | T-SQL | Sì | Shortcut OneLake → OPENROWSET | Azure AD (`az login` o managed identity) |
| `synapse` | Azure Synapse | T-SQL | Sì | ADLS OPENROWSET / tabelle esterne | Azure AD |
| `mysql` | MySQL | Per dialetto | No | Nessuno (solo land) | Credenziali per dialetto |
| `mariadb` | MariaDB | Per dialetto | No | Nessuno (solo land) | Credenziali per dialetto |
| `oracle` | Oracle Database | Per dialetto | No | Nessuno (solo land) | Credenziali per dialetto |
| `mssql` | Microsoft SQL Server | Per dialetto | No | Nessuno (solo land) | Credenziali per dialetto |
| `db2` | IBM Db2 | Per dialetto | No | Nessuno (solo land) | Credenziali per dialetto |
| `redshift` | Amazon Redshift | Per dialetto | No | Nessuno (solo land) | Credenziali per dialetto |
| `greenplum` | Greenplum | Per dialetto | No | Nessuno (solo land) | Credenziali per dialetto |
| `cockroachdb` | CockroachDB | Per dialetto | No | Nessuno (solo land) | Credenziali per dialetto |
| `yugabytedb` | YugabyteDB | Per dialetto | No | Nessuno (solo land) | Credenziali per dialetto |
| `opengauss` | openGauss | Per dialetto | No | Nessuno (solo land) | Credenziali per dialetto |
| `tidb` | TiDB | Per dialetto | No | Nessuno (solo land) | Credenziali per dialetto |
| `singlestore` | SingleStore | Per dialetto | No | Nessuno (solo land) | Credenziali per dialetto |
| `vertica` | Vertica | Per dialetto | No | Nessuno (solo land) | Credenziali per dialetto |
| `exasol` | Exasol | Per dialetto | No | Nessuno (solo land) | Credenziali per dialetto |
| `teradata` | Teradata Vantage | Per dialetto | No | Nessuno (solo land) | Credenziali per dialetto |
| `saphana` | SAP HANA | Per dialetto | No | Nessuno (solo land) | Credenziali per dialetto |
| `sapase` | SAP ASE (Sybase) | Per dialetto | No | Nessuno (solo land) | Credenziali per dialetto |
| `sqlanywhere` | SAP SQL Anywhere | Per dialetto | No | Nessuno (solo land) | Credenziali per dialetto |
| `monetdb` | MonetDB | Per dialetto | No | Nessuno (solo land) | Credenziali per dialetto |
| `firebird` | Firebird | Per dialetto | No | Nessuno (solo land) | Credenziali per dialetto |
| `sqlalchemy` | Altro database relazionale (tramite URL di connessione) | Per dialetto | No | Nessuno (solo land) | Credenziali per dialetto |

### Riferimento ai motori {: #engine-reference }

#### trino / trino-byo

`trino` è il coordinator Provisa gestito; `trino-byo` si connette al tuo cluster Trino. Entrambi usano Trino SQL e hanno la copertura di tipi di origine più ampia.

```bash
PROVISA_ENGINE=trino
TRINO_HOST=trino.internal
TRINO_PORT=8080
```

Lo store di materializzazione è di default `TENANT_DATABASE_URL` (PostgreSQL).

#### pg

Federa tramite postgres_fdw (SQL/MED) ed estensioni pg_duckdb. Single-node; nessun MPP. Ottimo quando i tuoi dati vivono già in PostgreSQL e vuoi unire poche origini remote.

```bash
PROVISA_ENGINE=pg
# Connection uses the standard PG_* env vars
```

Lo store di materializzazione è di default `TENANT_DATABASE_URL`.

#### duckdb

In-process; nessun servizio esterno. Il motore di default. `PROVISA_DATA_DIR` controlla dove vive lo store embedded (`~/.provisa` di default).

```bash
PROVISA_ENGINE=duckdb   # or omit — this is the default
```

Lo store di materializzazione predefinito è `~/.provisa/materialize.duckdb`. È l'unico motore il cui store predefinito non è PostgreSQL.

#### clickhouse (embedded) / clickhouse-server

`clickhouse` usa chdb (in-process). `clickhouse-server` si connette a un'istanza ClickHouse esterna o ClickHouse Cloud. Entrambi leggono Delta Lake, Iceberg e Hudi direttamente tramite motori tabella ClickHouse nativi.

```bash
# External server
PROVISA_ENGINE=clickhouse-server
PROVISA_ENGINE_URL="clickhouse://user:pass@host:9000/db"
```

Lo store di materializzazione è di default `TENANT_DATABASE_URL`.

#### snowflake

Motore-come-warehouse: Snowflake esegue le query; Provisa spinge i dati dell'origine attraverso external stage.

```bash
PROVISA_ENGINE=snowflake
PROVISA_ENGINE_URL="snowflake://user:pass@account/db/schema?warehouse=WH"
```

Lo store di materializzazione è di default `TENANT_DATABASE_URL`.

#### databricks

Le tabelle esterne Unity Catalog collegano le origini gestite da Provisa a Databricks SQL.

```bash
PROVISA_ENGINE=databricks
PROVISA_ENGINE_URL="databricks://token:TOKEN@my-workspace.azuredatabricks.net?http_path=/sql/1.0/warehouses/xxxx"
```

Lo store di materializzazione è di default `TENANT_DATABASE_URL`.

#### bigquery

Tabelle esterne e BigLake di BigQuery. Il progetto proviene dall'URL o da `GOOGLE_CLOUD_PROJECT`; autenticazione tramite chiave service-account.

```bash
PROVISA_ENGINE=bigquery
PROVISA_ENGINE_URL="bigquery://my-project?location=US"
# GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
```

Lo store di materializzazione è di default `TENANT_DATABASE_URL`.

#### fabric / synapse

Entrambi usano T-SQL su TDS con autenticazione Azure AD (`az login` o managed identity). Ometti `PROVISA_ENGINE_URL` per leggere i dettagli di connessione dalle variabili d'ambiente.

```bash
PROVISA_ENGINE=fabric
# FABRIC_SQL_SERVER=...   FABRIC_DATABASE=...
# or: PROVISA_ENGINE_URL set explicitly

PROVISA_ENGINE=synapse
# SYNAPSE_SQL_SERVER=...  SYNAPSE_DATABASE=...
```

Lo store di materializzazione è di default `TENANT_DATABASE_URL`.

#### Motori di database relazionali (mysql, mariadb, oracle, mssql, db2, redshift, greenplum, cockroachdb, yugabytedb, opengauss, tidb, singlestore, vertica, exasol, teradata, saphana, sapase, sqlanywhere, monetdb, firebird) e `sqlalchemy` {: #relational-database-engines-mysql-mariadb-oracle-mssql-db2-redshift-greenplum-cockroachdb-yugabytedb-opengauss-tidb-singlestore-vertica-exasol-teradata-saphana-sapase-sqlanywhere-monetdb-firebird-and-sqlalchemy }

Una chiave per ogni database relazionale raggiungibile in rete, tutte sullo stesso runtime solo-land (nessuna federazione verso origini esterne): ogni origine atterra nello store e viene interrogata lì. La chiave seleziona il database; `PROVISA_ENGINE_URL` porta il DSN richiesto dal suo dialetto. `sqlalchemy` è il jolly per un database privo di una propria chiave. Gli store file-embedded (SQLite, Access) non sono offerti, perché il server deve essere raggiungibile in rete.

```bash
PROVISA_ENGINE=mysql
PROVISA_ENGINE_URL="mysql+pymysql://user:pass@host:3306/db"
```

Lo store di materializzazione è di default `TENANT_DATABASE_URL`.

### Store di materializzazione {: #materialization-store }

Quando un'origine non può essere collegata dal vivo (nessun connettore ATTACH per il motore selezionato), atterra nello store di materializzazione del motore. Ordine di risoluzione: `PROVISA_MATERIALIZE_URL` esplicito → default dichiarato dal motore → errore esplicito (nessun fallback silenzioso).

DuckDB dichiara il proprio file embedded (`~/.provisa/materialize.duckdb`) come default. Tutti gli altri motori usano di default `TENANT_DATABASE_URL` (PostgreSQL). Sovrascrivi qualsiasi motore con `PROVISA_MATERIALIZE_URL`.

Lo store decide cosa fanno gli indici di una replica per i join: uno store PostgreSQL indicizza le repliche e un motore che esegue SQL al suo interno legge quegli indici, mentre DuckDB esegue i join sulle repliche del suo file embedded su una copia per istruzione. Vedere [Replicas](replicas.md#use-a-relational-database-as-the-replica-store-where-the-engine-runs-sql-in-it).

### Hint di federazione per origine {: #per-source-federation-hints }

I parametri di connessione estesi che i campi standard host/porta/utente/password non possono portare vanno in `federation_hints` sull'origine. Vedi il riferimento ai tipi di origine sopra per le chiavi di hint per tipo. Un esempio consolidato:

```yaml
sources:
  - id: my-databricks
    type: databricks
    host: my-workspace.azuredatabricks.net
    password: ${env:DATABRICKS_TOKEN}
    federation_hints:
      http_path: /sql/1.0/warehouses/xxxx   # required for Databricks sources

  - id: my-snowflake
    type: snowflake
    host: org.snowflakecomputing.com
    username: svc_provisa
    password: ${env:SNOWFLAKE_PASSWORD}
    federation_hints:
      account: myorg-myaccount
      warehouse: COMPUTE_WH

  - id: my-clickhouse
    type: clickhouse
    host: ch.example.com
    port: 8123
    password: ${env:CLICKHOUSE_PASSWORD}
    federation_hints:
      secure: "true"           # enable TLS on the HTTP interface

  - id: r2-parquet
    type: parquet
    path: s3://my-bucket/data/events.parquet
    federation_hints:
      access_key_id: ${env:R2_ACCESS_KEY}
      secret_access_key: ${env:R2_SECRET}
      account_id: ${env:R2_ACCOUNT_ID}   # Cloudflare R2 account (S3-compatible)
```

Per le origini Google Cloud, impostate `GOOGLE_APPLICATION_CREDENTIALS` sul percorso del file di chiave dell'account di servizio. Per Fabric e Synapse, accedete con `az login` (sviluppo) o con un'identità gestita (produzione); il motore legge le credenziali tramite `DefaultAzureCredential` di `azure-identity`.

## Variabili d'ambiente {: #environment-variables }

| Variabile | Default | Descrizione |
| ---------- | --------- | ------------- |
| `PROVISA_CONFIG` | `config/provisa.yaml` | Percorso del file di configurazione |
| `TENANT_DATABASE_URL` | `postgresql+psycopg://provisa:provisa@localhost:5432/provisa` | URI dello store control-plane (SQLAlchemy async); accetta `sqlite+pysqlite://…` / `duckdb://…` per lo store desktop embedded |
| `PLATFORM_DATABASE_URL` | — | URI del registro platform (directory tenant, registro motori); richiesto all'avvio, nessun fallback |
| `PROVISA_REDIS_EMBEDDED` | — | `1`/`true` usa fakeredis embedded invece di un server Redis — nessun Docker |
| `PG_HOST` | `localhost` | Host PostgreSQL |
| `PG_PORT` | `5432` | Porta PostgreSQL |
| `PG_DATABASE` | `provisa` | Database PostgreSQL |
| `PG_USER` | `provisa` | Utente PostgreSQL |
| `PG_PASSWORD` | `provisa` | Password PostgreSQL |
| `PROVISA_ENGINE` | `duckdb` | Chiave del motore di federazione |
| `PROVISA_ENGINE_URL` | — | URL di connessione per motori guidati da URL (Snowflake, Databricks, ClickHouse Server, BigQuery, SQLAlchemy) |
| `PROVISA_MATERIALIZE_URL` | — | Sovrascrive il DSN dello store di materializzazione (di default il default dichiarato dal motore) |
| `PROVISA_DATA_DIR` | `~/.provisa` | Directory dati per lo store DuckDB embedded |
| `TRINO_HOST` | `localhost` | Host del coordinator Trino |
| `TRINO_PORT` | `8080` | Porta HTTP del coordinator Trino |
| `PROVISA_ENGINE_FILES_GID` | `1000` | Gruppo con cui Trino legge i suoi file segreti (le impostazioni SASL di una sorgente Kafka, la chiave client di un host di codice) quando Provisa non gira né come root né come utente di Trino, come un'app nativa accanto al container di Trino. L'utente di Provisa deve farne parte, altrimenti il file viene rifiutato per nome. |
| `PROVISA_PG_ENGINE_FILES_GID` | `999` | Lo stesso, per il server di un motore Postgres che legge la chiave client di un host di codice |
| `GOOGLE_APPLICATION_CREDENTIALS` | — | Percorso al JSON di chiave service-account GCP (motore/origine BigQuery) |
| `GOOGLE_CLOUD_PROJECT` | — | Progetto GCP di default (BigQuery; sovrascritto dall'URL) |
| `FABRIC_SQL_SERVER` | — | Endpoint SQL del Fabric Warehouse (alternativa a `PROVISA_ENGINE_URL`) |
| `FABRIC_DATABASE` | — | Nome del database Fabric Warehouse |
| `SYNAPSE_SQL_SERVER` | — | Endpoint SQL serverless di Synapse |
| `SYNAPSE_DATABASE` | — | Nome del database Synapse |
| `REDIS_URL` | — | URL di connessione Redis |
| `PROVISA_SAMPLE_SIZE` | `10000` | Limite di campionamento di default |
| `PROVISA_DEFAULT_ROW_LIMIT` | `100` | Cap di righe quando una query non fornisce un `LIMIT` esplicito |
| `PROVISA_RETRY_BUDGET_SECS` | `30` | Budget di retry di lettura Tier-1 in secondi; backoff esponenziale con full jitter |
| `ZAYCHIK_PORT` | `8480` | Porta del proxy Zaychik Flight SQL |
| `FLIGHT_PORT` | `8815` | Porta del server Arrow Flight di Provisa |
| `GRPC_PORT` | `50051` | Porta del server gRPC Protobuf di Provisa |
| `PROVISA_REDIRECT_ENABLED` | `false` | Abilita il redirect a soglia lato server |
| `PROVISA_REDIRECT_THRESHOLD` | `1000` | Soglia di default sul conteggio righe |
| `PROVISA_REDIRECT_FORMAT` | `parquet` | Formato di redirect di default |
| `PROVISA_REDIRECT_BUCKET` | `provisa-results` | Bucket S3 per i risultati redirect |
| `PROVISA_REDIRECT_ENDPOINT` | — | URL dell'endpoint compatibile S3 |
| `PROVISA_REDIRECT_ACCESS_KEY` | — | Chiave di accesso S3 |
| `PROVISA_REDIRECT_SECRET_KEY` | — | Chiave segreta S3 |
| `PROVISA_REDIRECT_TTL` | `3600` | TTL dell'URL presigned (secondi) |
| `PROVISA_MTLS_CLIENT_CA` | — | Bundle PEM della o delle CA autorizzate a firmare i certificati client; impostarla attiva la verifica dei certificati client su pgwire, Bolt, gRPC e Flight |
| `PROVISA_MTLS_MODE` | `required` una volta impostata una CA | `required` oppure `optional`; qualsiasi altro valore rifiuta di avviarsi |
| `PROVISA_MTLS_BIND_PRINCIPAL` | `false` | Richiede che il common name del certificato coincida con il nome utente che si autentica |
| `PROVISA_BOLT_ALLOWED_ORIGINS` | — | Siti separati da virgole autorizzati ad aprire un WebSocket Bolt da un browser; se non impostata rifiuta ogni origine browser |
| `PROVISA_EXTRAS` | `firebase,vector` | Extra pyproject incorporati nell'immagine dell'app; `scripts/provisa` li deriva da `dq_checker` in `~/.provisa/config.yaml` |
| `PROVISA_DQ_CHECKER` | `none` | Solo installer: `none`/`soda`/`gx`, letto da `first-launch.sh` in modalità non interattiva e scritto in `config.yaml` come `dq_checker` |
| `ANTHROPIC_API_KEY` | — | Chiave API Claude (discovery) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | Sovrascrive `observability.endpoint` |
| `OTEL_SERVICE_NAME` | `provisa` | Sovrascrive `observability.service_name` |
| `OTEL_LOG_LEVEL` | `WARNING` | Sovrascrive `observability.log_level` |
| `OTEL_COMPACT_BATCH_SIZE` | `10` | Sovrascrive `observability.compact_batch_size` |
| `OTEL_SPAN_EXPORT_DELAY_MILLIS` | `1000` | Ritardo di flush del batch span processor |
| `PROVISA_SUPPORT_OTLP_ENDPOINT` | — | Sovrascrive `observability.support_endpoint` |
