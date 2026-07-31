# Riferimento alla configurazione

Provisa viene configurato tramite un file YAML (default: `config/provisa.yaml`). (REQ-528)

## Origini

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

Tutte le origini condividono un insieme comune di campi. [tool-verified: `provisa/core/models.py:129-212`]

| Campo | Default | Note |
|-------|---------|------|
| `id` | obbligatorio | Alfanumerico, trattini, underscore |
| `type` | obbligatorio | Vedi tabella sotto |
| `host` | `""` | Hostname o IP |
| `port` | `0` | `0` significa che ogni connettore fornisce il proprio default — non esiste una mappa centrale di porte di default |
| `database` | `""` | |
| `username` | `""` | |
| `password` | `""` | Supporta la risoluzione di secret `${env:VAR}` |
| `path` | `null` | Percorso file o URI per origini basate su file |
| `base_url` | `null` | URL base per origini API |
| `pool_min` / `pool_max` | `1` / `5` | Limiti del connection pool |
| `cache_enabled` | `true` | Attiva/disattiva la cache per tutte le tabelle di questa origine |
| `cache_ttl` | `null` | Secondi; `null` eredita il default globale |
| `federation_hints` | `{}` | Parametri estesi per connettore (dict[str,str]); vedi riferimento tipi sotto. REQ-281 |
| `mapping` | `{}` | DSL di mapping per redis, elasticsearch, prometheus. REQ-251 |
| `allowed_domains` | `[]` | Restringe questa origine a specifici ID di dominio; vuoto = senza restrizioni |
| `description` | `""` | |

### Tipi di origine supportati [tool-verified: `provisa/core/models.py:36-101`]

| Tipo | Stile di connessione | Note |
|------|-----------------|-------|
| **RDBMS** | | |
| `postgresql` | host/porta | Pool asyncpg; PgBouncer opt-in via `use_pgbouncer` |
| `mysql` | host/porta | |
| `mariadb` | host/porta | |
| `singlestore` | host/porta | |
| `sqlserver` | host/porta | |
| `oracle` | host/porta | |
| `firebird` | host + `path` (file DB) | Estensione community DuckDB firebird (REQ-899) |
| `duckdb` | host/porta | |
| `cockroachdb` | host/porta | Riusa il driver/dialetto PostgreSQL (REQ-950) |
| `yugabytedb` | host/porta | Riusa il driver/dialetto PostgreSQL (REQ-950) |
| `greenplum` | host/porta | Riusa il driver/dialetto PostgreSQL (REQ-950) |
| `tidb` | host/porta | Riusa il driver/dialetto MySQL (REQ-950) |
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
| `hudi` | `path` (URI tabella) | Motore Hudi di ClickHouse, zero-copy (REQ-1178) |
| `hive` | host/porta (metastore) + `mapping.storage` | Backend di storage in `mapping["storage"]`: hadoop/hdfs/local/s3/azure/adls |
| `hive_s3` | host/porta (metastore) + chiavi S3 in `mapping` | Tipo distinto; sempre storage S3 (REQ-229) |
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
| `airport` | `base_url` (location Flight) | Estensione airport di DuckDB (REQ-899) |
| `ingest` | ricevitore push | Servizi esterni inviano eventi JSON via POST |
| **SaaS** | | |
| `sharepoint` | `base_url` o `host` + `mapping` | Auth via `mapping.auth_type` |
| `splunk` | `host`/`port` o `base_url` + `mapping` | |
| **GovData** | | |
| `govdata` | subject + `domain_id` | Modello `GovDataSource` separato; vedi §GovData sotto |

### Riferimento ai tipi di origine

I tipi che richiedono una configurazione non ovvia hanno ciascuno una breve voce sotto. I tipi RDBMS (postgresql, mysql, ecc.) usano solo i campi comuni sopra — non serve una sezione aggiuntiva.

#### GovData [tool-verified: `provisa/core/models.py:953-983`]

Le origini `govdata` usano un modello top-level separato, `GovDataSource`, non il `Source` generico. (REQ-540) L'accesso è partizionato per raggruppamento subject.

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

Ogni subject mappa a uno o più schemi GovData. Configurare un'origine `govdata` con un subject espone automaticamente tutti gli schemi per quel subject. (REQ-540)

| Subject | Schemi |
|---------|---------|
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

Gli schemi `ref` e `geo` sono sempre inclusi come schemi linker — non configurabili e non elencati sopra. (REQ-541) Usa il subject `ALL` per concedere l'accesso a ogni schema. [tool-verified: `provisa/core/models.py:961-963`]

#### Kafka [tool-verified: `provisa/federation/trino_connectors.py:497-502`, `provisa/api/app_loaders.py:113-118`]

La riga `kafka` in `sources:` è solo di registrazione. Il metodo `details()` del suo connettore restituisce `{}` — la configurazione reale vive nel blocco top-level `kafka_sources[]`, non in una riga `sources:`. Kafka è sempre una VIRTUAL_SOURCE (passa attraverso il motore; nessun pool diretto). [tool-verified: `provisa/transpiler/router.py:44-63`]

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

**Time Window** — `default_window` limita ogni query a un periodo di tempo recente, prevenendo letture illimitate da topic ad alto volume. (REQ-148) Formato: `1h`, `30m`, `7d`, `60s`. Default `1h`. Iniettato automaticamente come `WHERE _timestamp >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR`. I client possono sovrascriverlo con un proprio filtro `_timestamp` nell'argomento `where` di GraphQL.

**Discriminator** — Più configurazioni di topic possono puntare allo stesso topic Kafka fisico con valori `discriminator` diversi, producendo tipi GraphQL separati. (REQ-149) Il discriminator viene iniettato automaticamente come clausola WHERE.

**Schema Source**

| Valore | Comportamento |
|-------|----------|
| `registry` | Recupera lo schema dal Confluent Schema Registry |
| `manual` | Definisce le colonne inline nella configurazione (nessun Schema Registry necessario) |
| `sample` | Scoperta automatica da messaggi di esempio |

#### Snowflake [tool-verified: `provisa/executor/drivers/snowflake.py:48-62`]

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

#### Databricks [tool-verified: `provisa/executor/drivers/databricks.py:34-52`]

`http_path` in `federation_hints` è richiesto. `password` porta il personal access token. `catalog` è opzionale (portato in SQL/hint, non nel campo `database`).

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

#### BigQuery [tool-verified: `provisa/federation/connector_duckdb.py:238`]

`project` in `federation_hints` è richiesto. L'autenticazione usa `GOOGLE_APPLICATION_CREDENTIALS` (percorso a un file di chiave service-account) o le Application Default Credentials nell'ambiente del motore.

```yaml
sources:
  - id: my-bigquery
    type: bigquery
    federation_hints:
      project: my-gcp-project     # required
```

#### Fabric / Synapse [tool-verified: `provisa/core/models.py:56-57`]

Entrambi usano T-SQL su TDS con autenticazione Azure AD. Autenticati con `az login` (sviluppo) o una managed identity (produzione) — il motore legge le credenziali tramite `DefaultAzureCredential` di `azure-identity`. I dettagli di connessione provengono da variabili d'ambiente: `FABRIC_SQL_SERVER` / `FABRIC_DATABASE` (Fabric) o `SYNAPSE_SQL_SERVER` / `SYNAPSE_DATABASE` (Synapse), oppure tramite `PROVISA_ENGINE_URL`.

```yaml
sources:
  - id: my-fabric
    type: fabric
    # host/database read from FABRIC_SQL_SERVER / FABRIC_DATABASE when not set here
```

#### ClickHouse [tool-verified: `provisa/executor/drivers/clickhouse.py:49-59`]

`secure` in `federation_hints` attiva TLS sull'interfaccia HTTP. La porta è di default `8123` (semplice) o `8443` (quando `secure: "true"`). `schema` in `federation_hints` sovrascrive lo schema remoto. [tool-verified: `provisa/federation/connector_duckdb.py:378-379`]

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

#### Delta Lake / Iceberg [tool-verified: `provisa/federation/connector_duckdb.py:291-327`]

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

#### Hive / Hive S3 [tool-verified: `provisa/federation/trino_connectors.py:244-363`]

`host` e `port` puntano al metastore Thrift di Hive (porta default 9083). Per `hive`, imposta `mapping["storage"]` per scegliere il backend dell'object store. Le chiavi obbligatorie mancanti falliscono in modo esplicito — nessun fallback. [tool-verified: `provisa/federation/trino_connectors.py:328-331`]

`hive_s3` è un tipo distinto che dichiara sempre storage S3 (REQ-229); non serve `mapping.storage`.

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

#### Redis [tool-verified: `provisa/core/trino_catalog_files.py:54-75`]

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
          key_pattern: "session:*"
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

#### Elasticsearch [tool-verified: `provisa/core/trino_catalog_files.py:78-104`]

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

#### Prometheus [tool-verified: `provisa/core/trino_catalog_files.py:107-124`]

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

#### Google Sheets [tool-verified: `provisa/federation/connector_duckdb.py:273-275`]

`spreadsheet_id` in `federation_hints` è richiesto. L'autenticazione usa un SECRET DuckDB `gsheet` provisionato al momento dell'attach.

```yaml
sources:
  - id: my-sheet
    type: google_sheets
    federation_hints:
      spreadsheet_id: 1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms
```

#### Origini file (csv / parquet / sqlite / files)

`path` è richiesto. `files` esegue il crawling di una directory per file CSV, Parquet, XLSX e JSON, esponendo ciascuno come tabella. Tutte le origini basate su file sono VIRTUAL (passano attraverso il motore; nessun pool diretto). [tool-verified: `provisa/transpiler/router.py:44-48`]

```yaml
sources:
  - id: orders-csv
    type: csv
    path: /data/orders.csv

  - id: data-lake-dir
    type: files
    path: /data/lake/         # directory; each file becomes a table
```

#### Origini API / Remote

**openapi** — imposta `base_url` all'URL base OpenAPI. La scoperta dello schema legge la spec OpenAPI all'avvio.

```yaml
sources:
  - id: payment-api
    type: openapi
    base_url: https://api.payments.example.com/v1
```

**graphql_remote** — imposta `base_url`. Chiavi `mapping` opzionali: `headers` (dict di header statici), `forward_client_headers` (bool), `timeout_seconds` (int). [tool-verified: `provisa/hasura_v2/mapper.py:129-152`]

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

**airport** — `base_url` è la location del server Arrow Flight. Estensione airport di DuckDB (REQ-899). [tool-verified: `provisa/federation/connector_duckdb.py:285-288`]

```yaml
sources:
  - id: flight-source
    type: airport
    base_url: grpc://flight.internal:8815
```

**websocket / rss** — usa `host`, `port`, `path` e `federation_hints`. [tool-verified: `provisa/api/data/subscribe.py:85-129`]

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

**sharepoint** [tool-verified: `provisa/federation/trino_connectors.py:394-423`]

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

**splunk** [tool-verified: `provisa/federation/trino_connectors.py:426-457`]

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

## Domini

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

### Convenzione di naming

L'autorità di naming è la fonte di verità unica per i nomi rivolti al client; i nomi fisici delle colonne di backend non sono mai esposti ai client. (REQ-194) Ogni linguaggio di query deriva il nome di una colonna dal suo `column.alias` se impostato, altrimenti dal nome fisico della colonna tramite la sua convenzione configurata. (REQ-194)

La convenzione GraphQL è una di tre enum preimpostate. (REQ-416) Le vecchie stringhe libere (`none`, `snake_case`, `camelCase`, `PascalCase`) sono deprecate. (REQ-416)

| Preset | Default | Nomi dei tipi | Nomi dei campi | Nomi delle mutation |
|--------|---------|------------|-------------|----------------|
| `apollo_graphql` | sì | PascalCase | camelCase | camelCase |
| `hasura_graphql` | | PascalCase | camelCase | snake_case |
| `snake` | | PascalCase | snake_case | snake_case |

La convenzione GraphQL di default è `apollo_graphql`, che produce nomi di campo e mutation in camelCase. (REQ-194, REQ-416) La convenzione SQL è separata, con default `snake_case`, applicata tramite `apply_sql_name()`; la convenzione GraphQL viene applicata tramite `apply_gql_name()`, e il nome CQL viene derivato dal nome GraphQL. (REQ-194)

`domain_prefix: bool` è un'opzione ortogonale che si applica indipendentemente dal preset scelto. (REQ-416)

L'`column.alias` esplicito è il nome canonico: SQL lo usa alla lettera senza applicare alcuna convenzione, GraphQL vi applica la propria convenzione, e CQL deriva dal nome GraphQL. (REQ-194)

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

### Prefisso di dominio

Quando `domain_prefix: true`, tutti i nomi di campo e tipo GraphQL vengono prefissati con l'ID di dominio usando un separatore a doppio underscore: (REQ-154)

| Tabella | Dominio | Nome campo |
|-------|--------|-----------|
| `orders` | `sales-analytics` | `sales_analytics__orders` |
| `customer_segments` | `customer-insights` | `customer_insights__customer_segments` |

Questo previene collisioni di nomi quando domini diversi hanno tabelle con lo stesso nome, e rende le query autodocumentanti.

### Regole di naming

Regole regex applicate ai nomi delle tabelle nella generazione dei nomi di campo GraphQL. Applicate in ordine prima della risoluzione dell'unicità. (REQ-542)

## Tabelle

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
        writable_by: []           # read-only (empty = no writes)
      - name: email
        visible_to: [admin, analyst]
        writable_by: [admin]      # only admin can mutate
        unmasked_to: [admin]      # admin sees raw, analyst sees masked
        mask_type: regex
        mask_pattern: "^(.{2}).*(@.*)$"
        mask_replace: "$1***$2"
        alias: email_address      # optional: override GraphQL field name
        description: "Primary email address"  # optional: appears in SDL
      - name: amount
        visible_to: [admin]
        writable_by: [admin]
        unmasked_to: [admin]
        mask_type: constant
        mask_value: "0"
      - name: created_at
        visible_to: [admin, analyst]
        writable_by: []           # nobody can write
        unmasked_to: [admin]
        mask_type: truncate
        mask_precision: month
    column_presets:               # auto-set values on insert/update
      - column: created_by
        source: header            # from request header
        name: X-User-ID
      - column: updated_at
        source: now               # current timestamp
```

### Alias

Gli alias di tabella e colonna sovrascrivono il nome GraphQL di default. (REQ-155) Utili per:
- Rinominare nomi di database criptici (es. `tbl_cust_seg` → `customer_segments`)
- Evitare abbreviazioni nel layer API
- Creare un vocabolario pulito, specifico per dominio

### Descrizioni

Le descrizioni di tabella e colonna sono incluse nell'SDL GraphQL generato. (REQ-156) Appaiono nell'esploratore di documentazione di GraphiQL e nelle query di introspezione. Impostale nella configurazione YAML o tramite la UI di amministrazione.

### Path (estrazione JSON calcolata)

Le colonne possono estrarre valori da una colonna sorgente JSON/JSONB usando un `path` in notazione a punti. (REQ-151) Utile per dati semi-strutturati in messaggi Kafka, documenti MongoDB o colonne JSONB PostgreSQL.

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

Il formato del path è `source_column.key1.key2...`. Il compilatore genera `json_extract_scalar(source_column, '$.key1.key2')` nell'SQL. (REQ-151)

**Impatto sul routing:** Le colonne path usano gli operatori JSON di PostgreSQL (`->>`), che sono supportati nativamente dal routing diretto verso PG. (REQ-152) Per origini non PostgreSQL (MySQL, SQL Server, ecc.), le query con colonne path vengono automaticamente instradate attraverso il motore di federazione. (REQ-152) Le mutation non sono interessate poiché le colonne path sono campi calcolati in sola lettura. (REQ-153)

### Tipi di mascheramento

| Tipo | Campi | Descrizione |
|------|--------|-------------|
| `regex` | `pattern`, `replace` | REGEXP_REPLACE (solo colonne stringa) |
| `constant` | `value` | Sostituzione letterale (NULL, 0, MAX, MIN, personalizzato) |
| `truncate` | `precision` | DATE_TRUNC (solo colonne data/timestamp) |

## Relazioni

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

### Auto-materializzazione

Imposta `materialize: true` su una relazione per generare automaticamente una vista materializzata per i JOIN cross-source. (REQ-158) Questo evita costose query federate pre-calcolando il risultato del JOIN.

- Solo le relazioni cross-source generano MV (i JOIN sulla stessa origine sono già veloci) (REQ-159)
- La MV parte stale e viene popolata dal loop di refresh in background (REQ-160)
- Le mutation su una delle due tabelle sorgente marcano la MV come stale per un nuovo refresh (REQ-543)
- `refresh_interval` è di default 300 secondi (5 minuti) (REQ-543)

## Ruoli

```yaml
roles:
  - id: admin
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
    capabilities: [query_development]
    domain_access: [sales-analytics]
  - id: junior_analyst
    capabilities: []
    domain_access: [sales-analytics]
    parent_role_id: analyst      # inherits query_development + sales-analytics
```

I ruoli con `parent_role_id` ereditano capability e accesso ai domini dal ruolo padre. (REQ-215) La gerarchia viene appiattita all'avvio. (REQ-215)

### Capability

| Capability | Descrizione |
|-----------|-------------|
| `source_registration` | Registra origini dati |
| `table_registration` | Registra tabelle |
| `relationship_registration` | Definisce relazioni |
| `security_config` | Configura RLS, mascheramento |
| `query_development` | Esegue query |
| `full_results` | Bypassa i limiti di campionamento |
| `admin` | Tutte le capability |

## Regole RLS

```yaml
rls_rules:
  - table_id: orders
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"
```

## Viste materializzate

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
    target_catalog: postgresql
    target_schema: mv_cache
    refresh_interval: 300
    enabled: true
```

## Viste (dataset calcolati governati)

Le viste sono dataset calcolati definiti in SQL con governance completa a livello di colonna. (REQ-133) Sono il meccanismo governato per aggiungere aggregazioni, trasformazioni e metriche derivate al layer semantico. (REQ-136)

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
|-------|----------|-------------|
| `id` | Sì | Identificatore univoco della vista |
| `sql` | Sì | Istruzione SQL SELECT che definisce la vista |
| `domain_id` | Sì | Dominio per la visibilità dello schema |
| `materialize` | No | `true` = refresh CTAS periodico, `false` = vista federata dal vivo |
| `refresh_interval` | No | Secondi tra i refresh (solo materializzate, default 300) |
| `description` | No | Appare nell'SDL GraphQL |
| `alias` | No | Sovrascrive il nome GraphQL |
| `columns` | Sì | Definizioni di colonna con visibilità, mascheramento, descrizioni |

### Materializzate vs dal vivo

- **`materialize: true`**: Provisa crea una tabella tramite CTAS e la aggiorna su pianificazione. (REQ-135) Query più veloci ma i dati possono essere obsoleti fino a `refresh_interval` secondi.
- **`materialize: false`**: Provisa crea una vista federata. (REQ-135) Le query restituiscono sempre dati dal vivo ma possono essere più lente per aggregazioni complesse.

Le viste passano attraverso la stessa pipeline di governance delle tabelle — RLS, mascheramento, campionamento e visibilità basata sul ruolo. (REQ-134) Questo garantisce che nessuna nuova semantica possa essere aggiunta alla piattaforma senza supervisione dello steward. (REQ-136)

### Viste di sola lettura

Sia le viste `materialize: true` che `materialize: false` espongono il proprio tipo GraphQL come query-only. Nessuna mutation di insert, upsert, update o delete viene generata per le relazioni supportate da `view_sql`. (REQ-1157) [tool-verified: `provisa/compiler/schema_gen.py:184`, `provisa/compiler/schema_types.py:79`]

## Cache

```yaml
cache:
  enabled: true
  redis_url: ${env:REDIS_URL}
  default_ttl: 300
```

### Gerarchia della cache

Ordine di risoluzione del TTL (il più specifico vince): **tabella** > **origine** > **default globale**. (REQ-544) Viene usato il primo valore non nullo.

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

Impostare `cache_enabled: false` su un'origine disabilita la cache per tutte le tabelle di quell'origine, indipendentemente dal TTL a livello di tabella. (REQ-544) Le chiavi di cache includono sempre `role_id` + i valori di contesto RLS per la partizione di sicurezza. (REQ-544)

## Autenticazione

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

### Tipi di provider di autenticazione

| Provider | Caso d'uso | Validazione del token |
|----------|----------|-----------------|
| `simple` | Sviluppo/test locale. Utenti definiti in YAML. | JWT firmato con `PROVISA_JWT_SECRET` |
| `firebase` | Firebase Authentication (tutti i metodi). | `verify_id_token()` dell'SDK `firebase-admin` |
| `keycloak` | Keycloak OIDC. Ruoli di tenant + client mappati. | Validazione JWT basata su JWKS |
| `oauth` | OIDC generico (Okta, Azure AD, Auth0, PingFederate). | JWKS dall'URL di discovery |

Le credenziali superuser (blocco `superuser`) funzionano con qualsiasi provider e si risolvono sempre nel ruolo admin con tutte le capability. (REQ-125) Usate per la configurazione iniziale prima che l'autenticazione esterna sia configurata.

### Esempio completo di configurazione auth (commentato)

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

## Mutation upsert

Per le tabelle con una chiave primaria, Provisa genera automaticamente campi mutation `upsert_<table>`. (REQ-212) Vengono compilati in un upsert nel dialetto di destinazione — `INSERT ... ON CONFLICT (pk) DO UPDATE SET ...` su PostgreSQL, `ON DUPLICATE KEY UPDATE` su MySQL. (REQ-212)

```graphql
mutation {
  upsert_orders(objects: [{id: 1, amount: 150.00, region: "us"}]) {
    affected_rows
  }
}
```

Le colonne di conflitto vengono derivate dai metadati della PK. (REQ-212) Si applicano tutte le regole di visibilità delle colonne e di permesso di scrittura.

## Distinct On

L'argomento `distinct_on` seleziona la prima riga per ciascun valore distinto delle colonne specificate. (REQ-213) Disponibile sui campi query root.

```graphql
{
  orders(distinct_on: [region], order_by: [{region: asc, created_at: desc}]) {
    region
    amount
    created_at
  }
}
```

Compila in `SELECT DISTINCT ON (region) ...` su PostgreSQL. (REQ-213) Per dialetti non PG, viene usato un fallback a window function. (REQ-213)

## Preset di colonna

Inietta automaticamente valori nelle colonne su insert/update. (REQ-214) Definiti per tabella nella configurazione.

```yaml
tables:
  - source_id: sales-pg
    table: orders
    column_presets:
      - column: created_by
        source: header           # from request header
        name: X-User-ID
      - column: updated_at
        source: now              # current timestamp
      - column: source_system
        source: literal          # constant value
        value: "provisa"
```

| Source | Comportamento |
|--------|--------|
| `header` | Inietta il valore dall'header HTTP della richiesta indicato |
| `now` | Inietta `NOW()` (timestamp corrente) |
| `literal` | Inietta un valore costante |

Le colonne preset vengono iniettate durante la compilazione della mutation prima della generazione dell'SQL. (REQ-214) Non sono visibili nel tipo di input della mutation. (REQ-214)

## Ruoli ereditati

I ruoli possono ereditare capability e accesso ai domini da un ruolo padre tramite `parent_role_id`. (REQ-215) La gerarchia viene appiattita all'avvio. (REQ-215)

```yaml
roles:
  - id: admin
    capabilities: [admin]
    domain_access: ["*"]
  - id: analyst
    capabilities: [query_development]
    domain_access: [sales-analytics]
  - id: junior_analyst
    capabilities: []
    domain_access: []
    parent_role_id: analyst      # inherits query_development + sales-analytics
  - id: intern
    capabilities: []
    domain_access: []
    parent_role_id: junior_analyst  # inherits from junior_analyst (and transitively analyst)
```

L'ereditarietà multilivello è supportata. (REQ-215) Le capability esplicite e il domain_access del ruolo figlio vengono uniti con quelli del padre. (REQ-215)

## Trigger pianificati

Trigger basati su cron che chiamano un URL webhook su pianificazione. (REQ-216) Usa APScheduler. (REQ-216)

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

I task pianificati vengono gestiti tramite la UI di amministrazione (toggle abilita/disabilita) o la mutation admin `toggle_scheduled_task`. (REQ-216)

## Formato OrderBy

OrderBy usa il formato `{column: direction}` con un enum di direzione a 6 valori: (REQ-200, REQ-201)

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
|-----------|-----|
| `asc` | `ASC` |
| `desc` | `DESC` |
| `asc_nulls_first` | `ASC NULLS FIRST` |
| `asc_nulls_last` | `ASC NULLS LAST` |
| `desc_nulls_first` | `DESC NULLS FIRST` |
| `desc_nulls_last` | `DESC NULLS LAST` |

L'ordinamento sulle relazioni è supportato tramite oggetti annidati: (REQ-202)

```graphql
{
  orders(order_by: [{customers: {name: asc}}]) {
    id
    customers { name }
  }
}
```

## Osservabilità

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

### Filtri di telemetria [tool-verified]

Provisa esegue due percorsi di export OTLP indipendenti: il tuo collector interno e l'endpoint di supporto Provisa opzionale. (REQ-545) Ogni percorso ha il proprio filtro. I filtri vengono eseguiti dentro un `_FilteringExporter` che avvolge le span prima che lascino il processo — gli oggetti span originali non vengono mai mutati. (REQ-546) [tool-verified: `provisa/api/otel_setup.py` lines 156–207]

**`telemetry_filter`** — controlla cosa raggiunge il tuo collector interno.

| Chiave | Tipo | Default | Descrizione |
|-----|------|---------|-------------|
| `redact_sql_literals` | bool | `false` | Sostituisce i letterali stringa e numerici in `db.statement` con `?` |
| `redact_attributes` | list[str] | `[]` | Chiavi di attributo eliminate completamente da ogni span |

**`support_telemetry_filter`** — controlla cosa raggiunge l'endpoint di supporto Provisa. La redazione dei letterali SQL è di default `true` su questo percorso, poiché i dati delle query appartengono a te. (REQ-547) [tool-verified: `provisa/api/otel_setup.py` line 240]

| Chiave | Tipo | Default | Descrizione |
|-----|------|---------|-------------|
| `redact_sql_literals` | bool | `true` | Sostituisce i letterali stringa e numerici in `db.statement` con `?` |
| `redact_attributes` | list[str] | `[]` | Chiavi di attributo eliminate completamente da ogni span |

Esempio di `db.statement` redatto — con `redact_sql_literals: true`, questo attributo di span:

```
db.statement: SELECT * FROM orders WHERE region = 'us-west' AND amount > 500
```

diventa:

```
db.statement: SELECT * FROM orders WHERE region = ? AND amount > ?
```

### Endpoint di supporto [tool-verified]

`support_endpoint` (o env `PROVISA_SUPPORT_OTLP_ENDPOINT`) inoltra la telemetria al supporto Provisa per la diagnostica. (REQ-548) Quando non impostato, nessun dato lascia la tua infrastruttura tramite questo percorso. (REQ-548) Il filtro di supporto si applica indipendentemente dal filtro interno — puoi redigere i letterali SQL da entrambi gli export pur condividendo con il supporto i dati di timing e di errore delle span. (REQ-545) [tool-verified: `provisa/api/otel_setup.py` lines 238–288]

### Rilevamento del protocollo dell'endpoint [tool-verified]

Provisa seleziona OTLP/HTTP o OTLP/gRPC in base allo schema dell'URL dell'endpoint. (REQ-549) Gli URL che iniziano con `http://` o `https://` usano OTLP/HTTP, con `/v1/traces`, `/v1/metrics` e `/v1/logs` aggiunti automaticamente. (REQ-549) Qualsiasi altro schema usa OTLP/gRPC con `insecure=True`. (REQ-549) [tool-verified: `provisa/api/otel_setup.py` lines 60–70]

## Motore di federazione

Configurare un motore di federazione è opzionale. Il default è `duckdb` — zero-config, in-process, nessun servizio esterno richiesto (REQ-989). Scegli un altro motore quando hai bisogno di scala MPP o vuoi riusare un warehouse esistente.

Precedenza: variabile d'ambiente `PROVISA_ENGINE` → campo di configurazione persistito `federation_engine` della UI di amministrazione → `duckdb`. Le modifiche hanno effetto al riavvio del servizio. [tool-verified: `engine.py` `build_engine`]

### Panoramica dei motori [tool-verified: `engine.py` `ENGINE_REGISTRY`, `_ENGINE_BUILDERS`]

| Chiave motore | Etichetta | Dialetto | MPP | Meccanismo di link esterno | Auth |
|-----------|-------|---------|-----|------------------------|------|
| `trino` | Provisa Federation Engine | Trino SQL | Sì | Cataloghi Trino (ampio set di connettori) | Credenziali JDBC |
| `trino-byo` | Trino (bring-your-own) | Trino SQL | Sì | Stesso di `trino`; coordinator non gestito | Credenziali JDBC |
| `pg` | PostgreSQL | PostgreSQL | No | FDW / pg_duckdb | Credenziali PostgreSQL |
| `duckdb` | DuckDB | DuckDB | No | ATTACH nativo dell'estensione | Nessuna (in-process) |
| `clickhouse` | ClickHouse (embedded) | ClickHouse | Sì | Motori tabella S3 / IcebergS3 / DeltaLake | chdb (in-process, nessuna auth) |
| `clickhouse-server` | ClickHouse (Server / Cloud) | ClickHouse | Sì | Motori tabella S3 / IcebergS3 / DeltaLake | Credenziali ClickHouse |
| `snowflake` | Snowflake | Snowflake | Sì | External stage + external table | `PROVISA_ENGINE_URL` |
| `databricks` | Databricks | Databricks SQL | Sì | Tabelle esterne Unity Catalog via REST | `PROVISA_ENGINE_URL` (bearer token + `http_path`) |
| `bigquery` | BigQuery | BigQuery | Sì | Tabelle esterne BigQuery / BigLake | `GOOGLE_APPLICATION_CREDENTIALS` |
| `fabric` | Microsoft Fabric | T-SQL | Sì | Shortcut OneLake → OPENROWSET | Azure AD (`az login` o managed identity) |
| `synapse` | Azure Synapse | T-SQL | Sì | ADLS OPENROWSET / tabelle esterne | Azure AD |
| `sqlalchemy` | SQLAlchemy (any RDB) | Per dialetto | No | Nessuno (solo land) | Credenziali per dialetto |

### Riferimento ai motori

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

In-process; nessun servizio esterno. Il motore di default (REQ-989). `PROVISA_DATA_DIR` controlla dove vive lo store embedded (`~/.provisa` di default).

```bash
PROVISA_ENGINE=duckdb   # or omit — this is the default
```

Lo store di materializzazione è di default `~/.provisa/materialize.duckdb` — l'unico motore con uno store di default non PostgreSQL.

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

#### sqlalchemy

Motore RDBMS generico solo-land (nessuna federazione verso origini esterne). Da usare per deployment single-warehouse o test.

```bash
PROVISA_ENGINE=sqlalchemy
PROVISA_ENGINE_URL="postgresql+psycopg2://user:pass@host/db"
```

Lo store di materializzazione è di default `TENANT_DATABASE_URL`.

### Store di materializzazione

Quando un'origine non può essere collegata dal vivo (nessun connettore ATTACH per il motore selezionato), atterra nello store di materializzazione del motore. Ordine di risoluzione: `PROVISA_MATERIALIZE_URL` esplicito → default dichiarato dal motore → errore esplicito (nessun fallback silenzioso). [tool-verified: `engine.py` `materialize_store`]

DuckDB dichiara il proprio file embedded (`~/.provisa/materialize.duckdb`) come default. Tutti gli altri motori usano di default `TENANT_DATABASE_URL` (PostgreSQL). Sovrascrivi qualsiasi motore con `PROVISA_MATERIALIZE_URL`.

### Hint di federazione per origine

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

Per le origini Google Cloud, imposta `GOOGLE_APPLICATION_CREDENTIALS` al percorso del tuo file di chiave service-account. Per Fabric e Synapse, autenticati con `az login` (sviluppo) o una managed identity (produzione) — il motore legge le credenziali tramite `DefaultAzureCredential` di `azure-identity`.

## Variabili d'ambiente

| Variabile | Default | Descrizione |
|----------|---------|-------------|
| `PROVISA_CONFIG` | `config/provisa.yaml` | Percorso del file di configurazione |
| `TENANT_DATABASE_URL` | `postgresql+asyncpg://provisa:provisa@localhost:5432/provisa` | URI dello store control-plane (SQLAlchemy async); accetta `sqlite+aiosqlite://…` / `duckdb://…` per lo store desktop embedded (REQ-828, REQ-850) |
| `PLATFORM_DATABASE_URL` | — | URI del registro platform (directory tenant, registro motori); richiesto all'avvio, nessun fallback (REQ-837) |
| `PROVISA_REDIS_EMBEDDED` | — | `1`/`true` usa fakeredis embedded invece di un server Redis — nessun Docker (REQ-829) |
| `PG_HOST` | `localhost` | Host PostgreSQL |
| `PG_PORT` | `5432` | Porta PostgreSQL |
| `PG_DATABASE` | `provisa` | Database PostgreSQL |
| `PG_USER` | `provisa` | Utente PostgreSQL |
| `PG_PASSWORD` | `provisa` | Password PostgreSQL |
| `PROVISA_ENGINE` | `duckdb` | Chiave del motore di federazione (REQ-989) |
| `PROVISA_ENGINE_URL` | — | URL di connessione per motori guidati da URL (Snowflake, Databricks, ClickHouse Server, BigQuery, SQLAlchemy) |
| `PROVISA_MATERIALIZE_URL` | — | Sovrascrive il DSN dello store di materializzazione (di default il default dichiarato dal motore) |
| `PROVISA_DATA_DIR` | `~/.provisa` | Directory dati per lo store DuckDB embedded (REQ-989) |
| `TRINO_HOST` | `localhost` | Host del coordinator Trino |
| `TRINO_PORT` | `8080` | Porta HTTP del coordinator Trino |
| `GOOGLE_APPLICATION_CREDENTIALS` | — | Percorso al JSON di chiave service-account GCP (motore/origine BigQuery) |
| `GOOGLE_CLOUD_PROJECT` | — | Progetto GCP di default (BigQuery; sovrascritto dall'URL) |
| `FABRIC_SQL_SERVER` | — | Endpoint SQL del Fabric Warehouse (alternativa a `PROVISA_ENGINE_URL`) |
| `FABRIC_DATABASE` | — | Nome del database Fabric Warehouse |
| `SYNAPSE_SQL_SERVER` | — | Endpoint SQL serverless di Synapse |
| `SYNAPSE_DATABASE` | — | Nome del database Synapse |
| `REDIS_URL` | — | URL di connessione Redis |
| `PROVISA_SAMPLE_SIZE` | `10000` | Limite di campionamento di default |
| `PROVISA_DEFAULT_ROW_LIMIT` | `100` | Cap di righe quando una query non fornisce un `LIMIT` esplicito |
| `PROVISA_RETRY_BUDGET_SECS` | `30` | Budget di retry di lettura Tier-1 in secondi; backoff esponenziale con full jitter (REQ-703) |
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
| `ANTHROPIC_API_KEY` | — | Chiave API Claude (discovery) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | Sovrascrive `observability.endpoint` |
| `OTEL_SERVICE_NAME` | `provisa` | Sovrascrive `observability.service_name` |
| `OTEL_LOG_LEVEL` | `WARNING` | Sovrascrive `observability.log_level` |
| `OTEL_COMPACT_BATCH_SIZE` | `10` | Sovrascrive `observability.compact_batch_size` |
| `OTEL_SPAN_EXPORT_DELAY_MILLIS` | `1000` | Ritardo di flush del batch span processor |
| `PROVISA_SUPPORT_OTLP_ENDPOINT` | — | Sovrascrive `observability.support_endpoint` |
