# Konfigurationsreferenz

Provisa wird über eine YAML-Datei konfiguriert (Standard: `config/provisa.yaml`). (REQ-528)

## Quellen

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

Alle Quellen teilen sich einen gemeinsamen Feldsatz. [tool-verified: `provisa/core/models.py:129-212`]

| Feld | Standard | Hinweise |
| ------- | --------- | ------- |
| `id` | erforderlich | Alphanumerisch, Bindestriche, Unterstriche |
| `type` | erforderlich | Siehe Tabelle unten |
| `host` | `""` | Hostname oder IP |
| `port` | `0` | `0` bedeutet, dass jeder Connector seinen eigenen Standard liefert — es gibt keine zentrale Standard-Port-Zuordnung |
| `database` | `""` | |
| `username` | `""` | |
| `password` | `""` | Unterstützt `${env:VAR}`-Secret-Auflösung |
| `path` | `null` | Dateipfad oder URI für dateibasierte Quellen |
| `base_url` | `null` | Basis-URL für API-Quellen |
| `pool_min` / `pool_max` | `1` / `5` | Grenzen des Connection-Pools |
| `cache_enabled` | `true` | Caching für alle Tabellen dieser Quelle umschalten |
| `cache_ttl` | `null` | Sekunden; `null` erbt den globalen Standard |
| `federation_hints` | `{}` | Pro-Connector erweiterte Parameter (dict[str,str]); siehe Typreferenz unten. REQ-281 |
| `mapping` | `{}` | Mapping-DSL für redis, elasticsearch, prometheus. REQ-251 |
| `allowed_domains` | `[]` | Beschränkt diese Quelle auf bestimmte Domänen-IDs; leer = uneingeschränkt |
| `description` | `""` | |

### Unterstützte Quellentypen [tool-verified: `provisa/core/models.py:36-101`]

| Typ | Verbindungsstil | Hinweise |
| ------ | ----------------- | ------- |
| **RDBMS** | | |
| `postgresql` | host/port | Asyncpg-Pool; PgBouncer opt-in über `use_pgbouncer` |
| `mysql` | host/port | |
| `mariadb` | host/port | |
| `singlestore` | host/port | |
| `sqlserver` | host/port | |
| `oracle` | host/port | |
| `firebird` | host + `path` (DB-Datei) | DuckDB-Firebird-Community-Extension (REQ-899) |
| `duckdb` | host/port | |
| `cockroachdb` | host/port | Nutzt den PostgreSQL-Treiber/-Dialekt wieder (REQ-950) |
| `yugabytedb` | host/port | Nutzt den PostgreSQL-Treiber/-Dialekt wieder (REQ-950) |
| `greenplum` | host/port | Nutzt den PostgreSQL-Treiber/-Dialekt wieder (REQ-950) |
| `tidb` | host/port | Nutzt den MySQL-Treiber/-Dialekt wieder (REQ-950) |
| **Cloud DW** | | |
| `snowflake` | host/port + `federation_hints` | `account` in den Hints erforderlich |
| `bigquery` | `federation_hints` | `project` erforderlich; Auth über `GOOGLE_APPLICATION_CREDENTIALS` |
| `databricks` | host + `federation_hints` | `http_path` in den Hints erforderlich |
| `fabric` | env vars oder `PROVISA_ENGINE_URL` | T-SQL über TDS, Azure-AD-Auth |
| `synapse` | env vars oder `PROVISA_ENGINE_URL` | T-SQL über TDS, Azure-AD-Auth |
| `redshift` | host/port | |
| **OLAP** | | |
| `clickhouse` | host/port + `federation_hints` | `secure`-Hint schaltet TLS um; Port-Standard 8123/8443 |
| `elasticsearch` | host/port + `mapping`-DSL | |
| `pinot` | host/port | Controller-REST-Endpunkt |
| `druid` | host/port | Broker-Avatica-Endpunkt |
| `exasol` | host/port | |
| **Data Lake** | | |
| `delta_lake` | `path` (Tabellen-URI) | DuckDB `delta_scan`; Objektspeicherzugriff über `federation_hints` |
| `iceberg` | `path` (Tabellen-URI) | DuckDB `iceberg_scan`; Objektspeicherzugriff über `federation_hints` |
| `hudi` | `path` (Tabellen-URI) | ClickHouse-Hudi-Engine, Zero-Copy (REQ-1178) |
| `hive` | host/port (Metastore) + `mapping.storage` | Storage-Backend in `mapping["storage"]`: hadoop/hdfs/local/s3/azure/adls |
| `hive_s3` | host/port (Metastore) + `mapping` S3-Schlüssel | Eigenständiger Typ; immer S3-Storage (REQ-229) |
| **NoSQL** | | |
| `mongodb` | host/port | Einfache Verbindungsfelder; keine Mapping-DSL |
| `cassandra` | host/port | Einfache Verbindungsfelder; keine Mapping-DSL |
| `redis` | host/port + `mapping`-DSL | |
| **Streaming** | | |
| `kafka` | nur Registrierung | Die tatsächliche Konfiguration liegt in `kafka_sources[]`; siehe §Kafka unten |
| `websocket` | host/port/path + `federation_hints` | Externer WebSocket-Feed |
| `rss` | host/port/path + `federation_hints` | RSS-2.0-/Atom-Feed |
| **Graph/Semantic** | | |
| `neo4j` | [UNVERIFIED end-to-end mapping] | |
| `sparql` | [UNVERIFIED end-to-end mapping] | |
| **File** | | |
| `sqlite` | `path` | Läuft immer über die Engine (kein direkter Pool) |
| `csv` | `path` | |
| `parquet` | `path` | |
| `files` | `path` (Verzeichnis) | Glob-Crawler; stellt CSV/Parquet/XLSX/JSON als Tabellen bereit |
| **API/Remote** | | |
| `google_sheets` | `federation_hints.spreadsheet_id` | |
| `prometheus` | host/port oder `mapping.url` + `mapping`-DSL | |
| `graphql_remote` | `base_url` + optionales `mapping` | Header, forward-client-headers, Timeout in `mapping` |
| `openapi` | `base_url` | |
| `grpc_remote` | [UNVERIFIED end-to-end mapping] | |
| `airport` | `base_url` (Flight-Location) | DuckDB-Airport-Extension (REQ-899) |
| `ingest` | Push-Receiver | Externe Dienste senden JSON-Ereignisse per POST |
| **SaaS** | | |
| `sharepoint` | `base_url` oder `host` + `mapping` | Auth über `mapping.auth_type` |
| `splunk` | `host`/`port` oder `base_url` + `mapping` | |
| **GovData** | | |
| `govdata` | subject + `domain_id` | Separates `GovDataSource`-Modell; siehe §GovData unten |

### Quellentyp-Referenz

Typen, die eine nicht offensichtliche Konfiguration benötigen, haben jeweils einen kurzen Eintrag unten. RDBMS-Typen (postgresql, mysql usw.) verwenden nur die obigen gemeinsamen Felder — kein zusätzlicher Abschnitt nötig.

#### GovData [tool-verified: `provisa/core/models.py:953-983`]

`govdata`-Quellen verwenden ein separates Top-Level-Modell, `GovDataSource`, nicht den generischen `Source`. (REQ-540) Der Zugriff ist nach Subject-Gruppierung partitioniert.

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

Jedes Subject bildet auf ein oder mehrere GovData-Schemas ab. Die Konfiguration einer `govdata`-Quelle mit einem Subject stellt automatisch alle Schemas für dieses Subject bereit. (REQ-540)

| Subject | Schemas |
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

Die Schemas `ref` und `geo` sind immer als Linker-Schemas enthalten — nicht konfigurierbar und oben nicht aufgeführt. (REQ-541) Verwenden Sie das Subject `ALL`, um Zugriff auf jedes Schema zu gewähren. [tool-verified: `provisa/core/models.py:961-963`]

#### Kafka [tool-verified: `provisa/federation/trino_connectors.py:497-502`, `provisa/api/app_loaders.py:113-118`]

Die `kafka`-Zeile in `sources:` dient nur der Registrierung. Ihr `details()` des Connectors gibt `{}` zurück — die tatsächliche Konfiguration liegt im Top-Level-Block `kafka_sources[]`, nicht in einer `sources:`-Zeile. Kafka ist immer eine VIRTUAL_SOURCE (läuft über die Engine; kein direkter Pool). [tool-verified: `provisa/transpiler/router.py:44-63`]

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

**Zeitfenster** — `default_window` begrenzt jede Abfrage auf einen kürzlichen Zeitraum und verhindert unbegrenzte Lesevorgänge aus hochvolumigen Topics. (REQ-148) Format: `1h`, `30m`, `7d`, `60s`. Standard ist `1h`. Automatisch injiziert als `WHERE _timestamp >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR`. Clients können dies mit ihrem eigenen `_timestamp`-Filter im GraphQL-`where`-Argument überschreiben.

**Discriminator** — Mehrere Topic-Konfigurationen können auf dasselbe physische Kafka-Topic mit unterschiedlichen `discriminator`-Werten zeigen und erzeugen so separate GraphQL-Typen. (REQ-149) Der Discriminator wird automatisch als WHERE-Klausel injiziert.

**Schema Source**

| Wert | Verhalten |
| ------- | ---------- |
| `registry` | Schema aus der Confluent Schema Registry abrufen |
| `manual` | Spalten inline in der Konfiguration definieren (keine Schema Registry nötig) |
| `sample` | Automatische Erkennung aus Beispielnachrichten |

#### Snowflake [tool-verified: `provisa/executor/drivers/snowflake.py:48-62`]

`account` in `federation_hints` ist erforderlich. `warehouse`, `role` und `schema` sind optional.

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

`http_path` in `federation_hints` ist erforderlich. `password` trägt das Personal Access Token. `catalog` ist optional (in SQL/Hints geführt, nicht im Feld `database`).

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

`project` in `federation_hints` ist erforderlich. Die Authentifizierung nutzt `GOOGLE_APPLICATION_CREDENTIALS` (Pfad zu einer Service-Account-Schlüsseldatei) oder Application Default Credentials in der Engine-Umgebung.

```yaml
sources:
  - id: my-bigquery
    type: bigquery
    federation_hints:
      project: my-gcp-project     # required
```

#### Fabric / Synapse [tool-verified: `provisa/core/models.py:56-57`]

Beide nutzen T-SQL über TDS mit Azure-AD-Authentifizierung. Authentifizieren Sie sich mit `az login` (Entwicklung) oder einer Managed Identity (Produktion) — die Engine liest Anmeldedaten über die `DefaultAzureCredential` von `azure-identity`. Verbindungsdetails stammen aus Umgebungsvariablen: `FABRIC_SQL_SERVER` / `FABRIC_DATABASE` (Fabric) oder `SYNAPSE_SQL_SERVER` / `SYNAPSE_DATABASE` (Synapse), oder über `PROVISA_ENGINE_URL`.

```yaml
sources:
  - id: my-fabric
    type: fabric
    # host/database read from FABRIC_SQL_SERVER / FABRIC_DATABASE when not set here
```

#### ClickHouse [tool-verified: `provisa/executor/drivers/clickhouse.py:49-59`]

`secure` in `federation_hints` aktiviert TLS auf der HTTP-Schnittstelle. Der Port ist standardmäßig `8123` (unverschlüsselt) oder `8443` (wenn `secure: "true"`). `schema` in `federation_hints` überschreibt das Remote-Schema. [tool-verified: `provisa/federation/connector_duckdb.py:378-379`]

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

`path` ist die Tabellen-URI (S3, GCS, ADLS oder lokal). Objektspeicherzugriff benötigt `federation_hints`-Anmeldedaten. Für Cloudflare R2 fügen Sie `account_id` hinzu.

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

`host` und `port` zeigen auf den Hive-Thrift-Metastore (Standardport 9083). Für `hive` setzen Sie `mapping["storage"]`, um das Objektspeicher-Backend zu wählen. Fehlende erforderliche Schlüssel schlagen laut fehl — kein Fallback. [tool-verified: `provisa/federation/trino_connectors.py:328-331`]

`hive_s3` ist ein eigenständiger Typ, der immer S3-Storage deklariert (REQ-229); kein `mapping.storage` nötig.

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

`mapping.storage` akzeptierte Werte: `hadoop` (Standard), `hdfs`, `local`, `s3`, `azure`, `adls`. S3-Mapping-Schlüssel: `endpoint`, `access_key_id`, `secret_access_key`, `region`, `path_style`. ADLS-Mapping-Schlüssel: `storage_account`, `access_key` oder `sas_token`.

#### Redis [tool-verified: `provisa/core/trino_catalog_files.py:54-75`]

Nutzt die `mapping`-DSL. `mongodb` und `cassandra` verwenden einfache Verbindungsfelder und NICHT die Mapping-DSL.

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

`mapping.url` überschreibt `host:port`, wenn beide vorhanden sind.

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

`spreadsheet_id` in `federation_hints` ist erforderlich. Auth nutzt ein DuckDB-`gsheet`-SECRET, das zum Zeitpunkt des Attach bereitgestellt wird.

```yaml
sources:
  - id: my-sheet
    type: google_sheets
    federation_hints:
      spreadsheet_id: 1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms
```

#### Dateiquellen (csv / parquet / sqlite / files)

`path` ist erforderlich. `files` durchsucht ein Verzeichnis nach CSV-, Parquet-, XLSX- und JSON-Dateien und stellt jede als Tabelle bereit. Alle dateibasierten Quellen sind VIRTUAL (laufen über die Engine; kein direkter Pool). [tool-verified: `provisa/transpiler/router.py:44-48`]

```yaml
sources:
  - id: orders-csv
    type: csv
    path: /data/orders.csv

  - id: data-lake-dir
    type: files
    path: /data/lake/         # directory; each file becomes a table
```

#### API-/Remote-Quellen

**openapi** — setzen Sie `base_url` auf die OpenAPI-Basis-URL. Die Schemaerkennung liest die OpenAPI-Spezifikation beim Start.

```yaml
sources:
  - id: payment-api
    type: openapi
    base_url: https://api.payments.example.com/v1
```

**graphql_remote** — setzen Sie `base_url`. Optionale `mapping`-Schlüssel: `headers` (Dict statischer Header), `forward_client_headers` (bool), `timeout_seconds` (int). [tool-verified: `provisa/hasura_v2/mapper.py:129-152`]

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

**airport** — `base_url` ist der Arrow-Flight-Server-Standort. DuckDB-Airport-Extension (REQ-899). [tool-verified: `provisa/federation/connector_duckdb.py:285-288`]

```yaml
sources:
  - id: flight-source
    type: airport
    base_url: grpc://flight.internal:8815
```

**websocket / rss** — verwenden Sie `host`, `port`, `path` und `federation_hints`. [tool-verified: `provisa/api/data/subscribe.py:85-129`]

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

## Domänen

```yaml
domains:
  - id: sales-analytics
    description: Sales operational data
```

## Namensgebung

```yaml
naming:
  convention: apollo_graphql   # snake, hasura_graphql, apollo_graphql (default)
  domain_prefix: true          # prepend domain_id__ to all GraphQL names
  rules:
    - pattern: "^prod_pg_"
      replace: ""
```

### Namenskonvention

Die Namensautorität ist die einzige Quelle der Wahrheit für clientseitige Namen; physische Backend-Spaltennamen werden Clients nie offengelegt. (REQ-194) Jede Abfragesprache leitet den Namen einer Spalte aus ihrem `column.alias` ab, falls gesetzt, andernfalls aus dem physischen Spaltennamen über die konfigurierte Konvention. (REQ-194)

Die GraphQL-Konvention ist eines von drei voreingestellten Enums. (REQ-416) Alte Freiform-Strings (`none`, `snake_case`, `camelCase`, `PascalCase`) sind veraltet. (REQ-416)

| Preset | Standard | Typnamen | Feldnamen | Mutationsnamen |
| -------- | --------- | ------------ | ------------- | ---------------- |
| `apollo_graphql` | ja | PascalCase | camelCase | camelCase |
| `hasura_graphql` | | PascalCase | camelCase | snake_case |
| `snake` | | PascalCase | snake_case | snake_case |

Die Standard-GraphQL-Konvention ist `apollo_graphql`, was camelCase-Feld- und -Mutationsnamen erzeugt. (REQ-194, REQ-416) Die SQL-Konvention ist separat, mit Standard `snake_case`, angewendet über `apply_sql_name()`; die GraphQL-Konvention wird über `apply_gql_name()` angewendet, und der CQL-Name wird vom GraphQL-Namen abgeleitet. (REQ-194)

`domain_prefix: bool` ist eine orthogonale Option, die unabhängig vom gewählten Preset gilt. (REQ-416)

Ein explizites `column.alias` ist der kanonische Name: SQL verwendet ihn unverändert ohne angewendete Konvention, GraphQL wendet seine Konvention darauf an, und CQL leitet sich vom GraphQL-Namen ab. (REQ-194)

Pro-Quelle-Override:

```yaml
sources:
  - id: legacy-db
    naming_convention: hasura_graphql  # overrides global for this source
```

Pro-Tabelle-Override:

```yaml
tables:
  - source_id: legacy-db
    table: orders
    naming_convention: snake  # overrides source for this table
```

### Domänen-Präfix

Wenn `domain_prefix: true`, werden alle GraphQL-Feld- und Typnamen mit der Domänen-ID unter Verwendung eines doppelten Unterstrichs als Trenner präfixiert: (REQ-154)

| Tabelle | Domäne | Feldname |
| ------- | -------- | ----------- |
| `orders` | `sales-analytics` | `sales_analytics__orders` |
| `customer_segments` | `customer-insights` | `customer_insights__customer_segments` |

Dies verhindert Namenskollisionen, wenn unterschiedliche Domänen Tabellen mit demselben Namen haben, und macht Abfragen selbstdokumentierend.

### Namensregeln

Regex-Regeln, die auf Tabellennamen angewendet werden, wenn GraphQL-Feldnamen generiert werden. Angewendet der Reihe nach vor der Eindeutigkeitsauflösung. (REQ-542)

## Tabellen

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

### Aliase

Tabellen- und Spaltenaliase überschreiben den Standard-GraphQL-Namen. (REQ-155) Nützlich für:

- Umbenennung kryptischer Datenbanknamen (z. B. `tbl_cust_seg` → `customer_segments`)
- Vermeidung von Abkürzungen in der API-Schicht
- Erstellung eines sauberen, domänenspezifischen Vokabulars

### Beschreibungen

Tabellen- und Spaltenbeschreibungen sind im generierten GraphQL-SDL enthalten. (REQ-156) Sie erscheinen im Dokumentations-Explorer von GraphiQL und in Introspektionsabfragen. Setzen Sie sie in der Konfigurations-YAML oder über die Admin-UI.

### Pfad (Berechnete JSON-Extraktion)

Spalten können Werte aus einer JSON-/JSONB-Quellspalte mithilfe eines Punktnotation-`path` extrahieren. (REQ-151) Dies ist nützlich für semistrukturierte Daten in Kafka-Nachrichten, MongoDB-Dokumenten oder PostgreSQL-JSONB-Spalten.

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

Das Pfadformat ist `source_column.key1.key2...`. Der Compiler generiert `json_extract_scalar(source_column, '$.key1.key2')` im SQL. (REQ-151)

**Routing-Auswirkung:** Pfad-Spalten verwenden PostgreSQL-JSON-Operatoren (`->>`), die vom direkten PG-Routing nativ unterstützt werden. (REQ-152) Für Nicht-PostgreSQL-Quellen (MySQL, SQL Server usw.) werden Abfragen mit Pfad-Spalten automatisch über die Föderations-Engine geroutet. (REQ-152) Mutationen sind davon nicht betroffen, da Pfad-Spalten schreibgeschützte berechnete Felder sind. (REQ-153)

### Maskierungstypen

| Typ | Felder | Beschreibung |
| ------ | -------- | -------------- |
| `regex` | `pattern`, `replace` | REGEXP_REPLACE (nur String-Spalten) |
| `constant` | `value` | Literaler Ersatz (NULL, 0, MAX, MIN, benutzerdefiniert) |
| `truncate` | `precision` | DATE_TRUNC (nur Datums-/Zeitstempel-Spalten) |

## Beziehungen

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

### Auto-Materialisierung

Setzen Sie `materialize: true` auf einer Beziehung, um automatisch eine materialisierte Sicht für quellenübergreifende JOINs zu generieren. (REQ-158) Dies vermeidet teure föderierte Abfragen, indem das JOIN-Ergebnis vorab berechnet wird.

- Nur quellenübergreifende Beziehungen generieren MVs (Joins innerhalb derselben Quelle sind bereits schnell) (REQ-159)
- Die MV startet veraltet und wird von der Hintergrund-Refresh-Schleife befüllt (REQ-160)
- Mutationen an einer der beiden Quelltabellen markieren die MV zur erneuten Aktualisierung als veraltet (REQ-543)
- `refresh_interval` ist standardmäßig 300 Sekunden (5 Minuten) (REQ-543)

## Rollen

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

Rollen mit `parent_role_id` erben Capabilities und Domänenzugriff von der übergeordneten Rolle. (REQ-215) Die Hierarchie wird beim Start flach aufgelöst. (REQ-215)

### Capabilities

| Capability | Beschreibung |
| ----------- | -------------- |
| `source_registration` | Datenquellen registrieren |
| `table_registration` | Tabellen registrieren |
| `relationship_registration` | Beziehungen definieren |
| `security_config` | RLS, Maskierung konfigurieren |
| `query_development` | Abfragen ausführen |
| `full_results` | Sampling-Grenzen umgehen |
| `admin` | Alle Capabilities |

## RLS-Regeln

```yaml
rls_rules:
  - table_id: orders
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"
```

## Materialisierte Sichten

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

## Sichten (Regierte berechnete Datasets)

Sichten sind SQL-definierte berechnete Datasets mit vollständiger spaltenweiser Governance. (REQ-133) Sie sind der regierte Mechanismus zum Hinzufügen von Aggregationen, Transformationen und abgeleiteten Kennzahlen zur semantischen Schicht. (REQ-136)

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

| Feld | Erforderlich | Beschreibung |
| ------- | ---------- | -------------- |
| `id` | Ja | Eindeutige Sicht-ID |
| `sql` | Ja | SQL-SELECT-Anweisung, die die Sicht definiert |
| `domain_id` | Ja | Domäne für Schema-Sichtbarkeit |
| `materialize` | Nein | `true` = periodischer CTAS-Refresh, `false` = live-föderierte Sicht |
| `refresh_interval` | Nein | Sekunden zwischen Aktualisierungen (nur materialisiert, Standard 300) |
| `description` | Nein | Erscheint im GraphQL-SDL |
| `alias` | Nein | GraphQL-Namen überschreiben |
| `columns` | Ja | Spaltendefinitionen mit Sichtbarkeit, Maskierung, Beschreibungen |

### Materialisiert vs. Live

- **`materialize: true`**: Provisa erstellt eine Tabelle via CTAS und aktualisiert sie nach Zeitplan. (REQ-135) Schnellere Abfragen, aber die Daten können bis zu `refresh_interval` Sekunden veraltet sein.
- **`materialize: false`**: Provisa erstellt eine föderierte Sicht. (REQ-135) Abfragen liefern immer Live-Daten, können aber bei komplexen Aggregationen langsamer sein.

Sichten durchlaufen dieselbe Governance-Pipeline wie Tabellen — RLS, Maskierung, Sampling und rollenbasierte Sichtbarkeit. (REQ-134) Dies stellt sicher, dass keine neue Semantik ohne Steward-Aufsicht zur Plattform hinzugefügt werden kann. (REQ-136)

### Nur abfragbare Sichten

Sowohl `materialize: true`- als auch `materialize: false`-Sichten exponieren ihren GraphQL-Typ als nur abfragbar. Für `view_sql`-basierte Relationen werden keine Insert-, Upsert-, Update- oder Delete-Mutationen generiert. (REQ-1157) [tool-verified: `provisa/compiler/schema_gen.py:184`, `provisa/compiler/schema_types.py:79`]

## Cache

```yaml
cache:
  enabled: true
  redis_url: ${env:REDIS_URL}
  default_ttl: 300
```

### Cache-Hierarchie

TTL-Auflösungsreihenfolge (spezifischster gewinnt): **Tabelle** > **Quelle** > **globaler Standard**. (REQ-544) Der erste nicht-null-Wert wird verwendet.

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

Das Setzen von `cache_enabled: false` auf einer Quelle deaktiviert das Caching für alle Tabellen dieser Quelle, unabhängig von der Tabellen-TTL. (REQ-544) Cache-Schlüssel enthalten immer `role_id` + RLS-Kontextwerte zur Sicherheitspartitionierung. (REQ-544)

## Authentifizierung

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

### Auth-Provider-Typen

| Provider | Anwendungsfall | Token-Validierung |
| ---------- | ---------- | ----------------- |
| `simple` | Lokale Entwicklung/Tests. Nutzer in YAML definiert. | JWT signiert mit `PROVISA_JWT_SECRET` |
| `firebase` | Firebase Authentication (alle Methoden). | `firebase-admin`-SDK `verify_id_token()` |
| `keycloak` | Keycloak-OIDC. Mandant + Client-Rollen zugeordnet. | JWKS-basierte JWT-Validierung |
| `oauth` | Generisches OIDC (Okta, Azure AD, Auth0, PingFederate). | JWKS von der Discovery-URL |

Superuser-Anmeldedaten (Block `superuser`) funktionieren mit jedem Provider und lösen immer zur Admin-Rolle mit allen Capabilities auf. (REQ-125) Wird für die Ersteinrichtung verwendet, bevor externe Auth konfiguriert ist.

### Vollständiges Auth-Konfigurationsbeispiel (auskommentiert)

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

## Upsert-Mutationen

Für Tabellen mit einem Primärschlüssel generiert Provisa automatisch `upsert_<table>`-Mutationsfelder. (REQ-212) Diese kompilieren zu einem Upsert im Zieldialekt — `INSERT ... ON CONFLICT (pk) DO UPDATE SET ...` bei PostgreSQL, `ON DUPLICATE KEY UPDATE` bei MySQL. (REQ-212)

```graphql
mutation {
  upsert_orders(objects: [{id: 1, amount: 150.00, region: "us"}]) {
    affected_rows
  }
}
```

Konfliktspalten werden aus den PK-Metadaten abgeleitet. (REQ-212) Alle Regeln zur Spaltensichtbarkeit und Schreibberechtigung gelten.

## Distinct On

Das Argument `distinct_on` wählt die erste Zeile für jeden eindeutigen Wert der angegebenen Spalten aus. (REQ-213) Verfügbar auf Root-Abfragefeldern.

```graphql
{
  orders(distinct_on: [region], order_by: [{region: asc, created_at: desc}]) {
    region
    amount
    created_at
  }
}
```

Kompiliert zu `SELECT DISTINCT ON (region) ...` in PostgreSQL. (REQ-213) Für Nicht-PG-Dialekte wird ein Fensterfunktions-Fallback verwendet. (REQ-213)

## Spalten-Presets

Werte automatisch bei Insert/Update in Spalten injizieren. (REQ-214) Pro Tabelle in der Konfiguration definiert.

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

| Source | Verhalten |
| -------- | ---------- |
| `header` | Injiziert den Wert aus dem benannten HTTP-Request-Header |
| `now` | Injiziert `NOW()` (aktueller Zeitstempel) |
| `literal` | Injiziert einen konstanten Wert |

Preset-Spalten werden während der Mutationskompilierung vor der SQL-Generierung injiziert. (REQ-214) Sie sind im Mutations-Eingabetyp nicht sichtbar. (REQ-214)

## Vererbte Rollen

Rollen können Capabilities und Domänenzugriff von einer übergeordneten Rolle über `parent_role_id` erben. (REQ-215) Die Hierarchie wird beim Start flach aufgelöst. (REQ-215)

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

Mehrstufige Vererbung wird unterstützt. (REQ-215) Die expliziten Capabilities und der domain_access der untergeordneten Rolle werden mit denen der übergeordneten Rolle zusammengeführt. (REQ-215)

## Geplante Trigger

Cron-basierte Trigger, die nach Zeitplan eine Webhook-URL aufrufen. (REQ-216) Nutzt APScheduler. (REQ-216)

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

Geplante Aufgaben werden über die Admin-UI (Umschalter zum Aktivieren/Deaktivieren) oder die Admin-Mutation `toggle_scheduled_task` verwaltet. (REQ-216)

## OrderBy-Format

OrderBy verwendet das Format `{column: direction}` mit einem 6-wertigen Richtungs-Enum: (REQ-200, REQ-201)

```graphql
{
  orders(order_by: [{created_at: desc_nulls_last}, {amount: asc}]) {
    id
    created_at
    amount
  }
}
```

| Richtung | SQL |
| ----------- | ----- |
| `asc` | `ASC` |
| `desc` | `DESC` |
| `asc_nulls_first` | `ASC NULLS FIRST` |
| `asc_nulls_last` | `ASC NULLS LAST` |
| `desc_nulls_first` | `DESC NULLS FIRST` |
| `desc_nulls_last` | `DESC NULLS LAST` |

Beziehungssortierung wird über verschachtelte Objekte unterstützt: (REQ-202)

```graphql
{
  orders(order_by: [{customers: {name: asc}}]) {
    id
    customers { name }
  }
}
```

## Observability

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

### Telemetriefilter [tool-verified]

Provisa betreibt zwei unabhängige OTLP-Exportpfade: Ihren internen Collector und den optionalen Provisa-Support-Endpunkt. (REQ-545) Jeder Pfad hat seinen eigenen Filter. Filter laufen innerhalb eines umschließenden `_FilteringExporter`, bevor Spans den Prozess verlassen — ursprüngliche Span-Objekte werden nie mutiert. (REQ-546) [tool-verified: `provisa/api/otel_setup.py` Zeilen 156–207]

**`telemetry_filter`** — steuert, was Ihren internen Collector erreicht.

| Schlüssel | Typ | Standard | Beschreibung |
| ----- | ------ | --------- | -------------- |
| `redact_sql_literals` | bool | `false` | Ersetzt String- und numerische Literale in `db.statement` durch `?` |
| `redact_attributes` | list[str] | `[]` | Attributschlüssel, die vollständig aus jedem Span entfernt werden |

**`support_telemetry_filter`** — steuert, was den Provisa-Support-Endpunkt erreicht. Die SQL-Literal-Schwärzung ist auf diesem Pfad standardmäßig `true`, da Abfragedaten Ihnen gehören. (REQ-547) [tool-verified: `provisa/api/otel_setup.py` Zeile 240]

| Schlüssel | Typ | Standard | Beschreibung |
| ----- | ------ | --------- | -------------- |
| `redact_sql_literals` | bool | `true` | Ersetzt String- und numerische Literale in `db.statement` durch `?` |
| `redact_attributes` | list[str] | `[]` | Attributschlüssel, die vollständig aus jedem Span entfernt werden |

Beispiel für ein geschwärztes `db.statement` — mit `redact_sql_literals: true` wird dieses Span-Attribut:

```yaml
db.statement: SELECT * FROM orders WHERE region = 'us-west' AND amount > 500
```

zu:

```yaml
db.statement: SELECT * FROM orders WHERE region = ? AND amount > ?
```

### Support-Endpunkt [tool-verified]

`support_endpoint` (oder env `PROVISA_SUPPORT_OTLP_ENDPOINT`) leitet Telemetriedaten zu Provisa-Support für Diagnosezwecke weiter. (REQ-548) Wenn nicht gesetzt, verlässt über diesen Pfad keine Daten Ihre Infrastruktur. (REQ-548) Der Support-Filter gilt unabhängig vom internen Filter — Sie können SQL-Literale bei beiden Exports schwärzen und dennoch Span-Timing- und Fehlerdaten mit dem Support teilen. (REQ-545) [tool-verified: `provisa/api/otel_setup.py` Zeilen 238–288]

### Endpunkt-Protokollerkennung [tool-verified]

Provisa wählt OTLP/HTTP oder OTLP/gRPC anhand des URL-Schemas des Endpunkts. (REQ-549) URLs, die mit `http://` oder `https://` beginnen, verwenden OTLP/HTTP, wobei `/v1/traces`, `/v1/metrics` und `/v1/logs` automatisch angehängt werden. (REQ-549) Jedes andere Schema verwendet OTLP/gRPC mit `insecure=True`. (REQ-549) [tool-verified: `provisa/api/otel_setup.py` Zeilen 60–70]

## Föderations-Engine

Die Konfiguration einer Föderations-Engine ist optional. Der Standard ist `duckdb` — keine Konfiguration nötig, In-Process, kein externer Dienst erforderlich (REQ-989). Wählen Sie eine andere Engine, wenn Sie MPP-Skalierung benötigen oder ein bestehendes Warehouse wiederverwenden möchten.

Vorrang: `PROVISA_ENGINE`-Umgebungsvariable → persistiertes Admin-UI-Konfigurationsfeld `federation_engine` → `duckdb`. Änderungen werden beim Neustart des Dienstes wirksam. [tool-verified: `engine.py` `build_engine`]

### Engine-Übersicht [tool-verified: `engine.py` `ENGINE_REGISTRY`, `_ENGINE_BUILDERS`]

| Engine-Schlüssel | Bezeichnung | Dialekt | MPP | Externer-Link-Mechanismus | Auth |
| ----------- | ------- | --------- | ----- | ------------------------ | ------ |
| `trino` | Provisa Federation Engine | Trino SQL | Ja | Trino-Kataloge (breite Connector-Menge) | JDBC-Anmeldedaten |
| `trino-byo` | Trino (bring-your-own) | Trino SQL | Ja | Wie `trino`; unverwalteter Coordinator | JDBC-Anmeldedaten |
| `pg` | PostgreSQL | PostgreSQL | Nein | FDW / pg_duckdb | PostgreSQL-Anmeldedaten |
| `duckdb` | DuckDB | DuckDB | Nein | Extension-natives ATTACH | Keine (In-Process) |
| `clickhouse` | ClickHouse (eingebettet) | ClickHouse | Ja | S3-/IcebergS3-/DeltaLake-Tabellen-Engines | chdb (In-Process, keine Auth) |
| `clickhouse-server` | ClickHouse (Server / Cloud) | ClickHouse | Ja | S3-/IcebergS3-/DeltaLake-Tabellen-Engines | ClickHouse-Anmeldedaten |
| `snowflake` | Snowflake | Snowflake | Ja | External Stage + External Table | `PROVISA_ENGINE_URL` |
| `databricks` | Databricks | Databricks SQL | Ja | Unity-Catalog-External-Tables via REST | `PROVISA_ENGINE_URL` (Bearer-Token + `http_path`) |
| `bigquery` | BigQuery | BigQuery | Ja | BigQuery External-/BigLake-Tables | `GOOGLE_APPLICATION_CREDENTIALS` |
| `fabric` | Microsoft Fabric | T-SQL | Ja | OneLake-Shortcuts → OPENROWSET | Azure AD (`az login` oder Managed Identity) |
| `synapse` | Azure Synapse | T-SQL | Ja | ADLS OPENROWSET / External Tables | Azure AD |
| `sqlalchemy` | SQLAlchemy (jede RDB) | Pro Dialekt | Nein | Keiner (nur Landing) | Anmeldedaten pro Dialekt |

### Engine-Referenz

#### trino / trino-byo

`trino` ist der verwaltete Provisa-Coordinator; `trino-byo` verbindet sich mit Ihrem eigenen Trino-Cluster. Beide nutzen Trino SQL und haben die breiteste Quellentyp-Reichweite.

```bash
PROVISA_ENGINE=trino
TRINO_HOST=trino.internal
TRINO_PORT=8080
```

Der Materialisierungs-Store ist standardmäßig `TENANT_DATABASE_URL` (PostgreSQL).

#### pg

Föderiert über postgres_fdw (SQL/MED) und pg_duckdb-Extensions. Single-Node; kein MPP. Am besten geeignet, wenn Ihre Daten bereits in PostgreSQL liegen und Sie einige wenige Remote-Quellen verknüpfen möchten.

```bash
PROVISA_ENGINE=pg
# Connection uses the standard PG_* env vars
```

Der Materialisierungs-Store ist standardmäßig `TENANT_DATABASE_URL`.

#### duckdb

In-Process; kein externer Dienst. Die Standard-Engine (REQ-989). `PROVISA_DATA_DIR` steuert, wo der eingebettete Store liegt (`~/.provisa` standardmäßig).

```bash
PROVISA_ENGINE=duckdb   # or omit — this is the default
```

Der Materialisierungs-Store ist standardmäßig `~/.provisa/materialize.duckdb` — die einzige Engine mit einem Nicht-PostgreSQL-Standard-Store.

#### clickhouse (eingebettet) / clickhouse-server

`clickhouse` nutzt chdb (In-Process). `clickhouse-server` verbindet sich mit einer externen ClickHouse-Instanz oder ClickHouse Cloud. Beide lesen Delta Lake, Iceberg und Hudi direkt über native ClickHouse-Tabellen-Engines.

```bash
# External server
PROVISA_ENGINE=clickhouse-server
PROVISA_ENGINE_URL="clickhouse://user:pass@host:9000/db"
```

Der Materialisierungs-Store ist standardmäßig `TENANT_DATABASE_URL`.

#### snowflake

Engine-als-Warehouse: Snowflake führt die Abfragen aus; Provisa schiebt Quelldaten durch External Stages.

```bash
PROVISA_ENGINE=snowflake
PROVISA_ENGINE_URL="snowflake://user:pass@account/db/schema?warehouse=WH"
```

Der Materialisierungs-Store ist standardmäßig `TENANT_DATABASE_URL`.

#### databricks

Unity-Catalog-External-Tables verbinden Provisa-verwaltete Quellen mit Databricks SQL.

```bash
PROVISA_ENGINE=databricks
PROVISA_ENGINE_URL="databricks://token:TOKEN@my-workspace.azuredatabricks.net?http_path=/sql/1.0/warehouses/xxxx"
```

Der Materialisierungs-Store ist standardmäßig `TENANT_DATABASE_URL`.

#### bigquery

BigQuery-External- und BigLake-Tables. Das Projekt stammt aus der URL oder `GOOGLE_CLOUD_PROJECT`; Auth über Service-Account-Schlüssel.

```bash
PROVISA_ENGINE=bigquery
PROVISA_ENGINE_URL="bigquery://my-project?location=US"
# GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
```

Der Materialisierungs-Store ist standardmäßig `TENANT_DATABASE_URL`.

#### fabric / synapse

Beide nutzen T-SQL über TDS mit Azure-AD-Auth (`az login` oder Managed Identity). Lassen Sie `PROVISA_ENGINE_URL` weg, um Verbindungsdetails stattdessen aus Umgebungsvariablen zu lesen.

```bash
PROVISA_ENGINE=fabric
# FABRIC_SQL_SERVER=...   FABRIC_DATABASE=...
# or: PROVISA_ENGINE_URL set explicitly

PROVISA_ENGINE=synapse
# SYNAPSE_SQL_SERVER=...  SYNAPSE_DATABASE=...
```

Der Materialisierungs-Store ist standardmäßig `TENANT_DATABASE_URL`.

#### sqlalchemy

Generische RDBMS-Nur-Landing-Engine (keine Föderation zu externen Quellen). Für Single-Warehouse-Deployments oder Tests verwenden.

```bash
PROVISA_ENGINE=sqlalchemy
PROVISA_ENGINE_URL="postgresql+psycopg2://user:pass@host/db"
```

Der Materialisierungs-Store ist standardmäßig `TENANT_DATABASE_URL`.

### Materialisierungs-Store

Wenn eine Quelle nicht live angehängt werden kann (kein ATTACH-Connector für die gewählte Engine), landet sie im Materialisierungs-Store der Engine. Auflösungsreihenfolge: explizites `PROVISA_MATERIALIZE_URL` → deklarierter Standard der Engine → harter Fehler (kein stiller Fallback). [tool-verified: `engine.py` `materialize_store`]

DuckDB deklariert seine eingebettete Datei (`~/.provisa/materialize.duckdb`) als Standard. Alle anderen Engines nutzen standardmäßig `TENANT_DATABASE_URL` (PostgreSQL). Überschreiben Sie jede Engine mit `PROVISA_MATERIALIZE_URL`.

### Pro-Quelle-Föderations-Hints

Erweiterte Verbindungsparameter, die die Standardfelder host/port/user/password nicht tragen können, kommen in `federation_hints` auf der Quelle. Siehe die Quellentyp-Referenz oben für Hint-Schlüssel pro Typ. Ein konsolidiertes Beispiel:

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

Setzen Sie für Google-Cloud-Quellen `GOOGLE_APPLICATION_CREDENTIALS` auf den Pfad Ihrer Service-Account-Schlüsseldatei. Für Fabric und Synapse authentifizieren Sie sich mit `az login` (Entwicklung) oder einer Managed Identity (Produktion) — die Engine liest Anmeldedaten über die `DefaultAzureCredential` von `azure-identity`.

## Umgebungsvariablen

| Variable | Standard | Beschreibung |
| ---------- | --------- | -------------- |
| `PROVISA_CONFIG` | `config/provisa.yaml` | Pfad zur Konfigurationsdatei |
| `TENANT_DATABASE_URL` | `postgresql+asyncpg://provisa:provisa@localhost:5432/provisa` | Control-Plane-Store-URI (SQLAlchemy async); akzeptiert `sqlite+aiosqlite://…` / `duckdb://…` für den eingebetteten Desktop-Store (REQ-828, REQ-850) |
| `PLATFORM_DATABASE_URL` | — | Plattform-Registry-URI (Mandantenverzeichnis, Engine-Registry); beim Start erforderlich, kein Fallback (REQ-837) |
| `PROVISA_REDIS_EMBEDDED` | — | `1`/`true` nutzt eingebettetes fakeredis statt eines Redis-Servers — kein Docker (REQ-829) |
| `PG_HOST` | `localhost` | PostgreSQL-Host |
| `PG_PORT` | `5432` | PostgreSQL-Port |
| `PG_DATABASE` | `provisa` | PostgreSQL-Datenbank |
| `PG_USER` | `provisa` | PostgreSQL-Benutzer |
| `PG_PASSWORD` | `provisa` | PostgreSQL-Passwort |
| `PROVISA_ENGINE` | `duckdb` | Föderations-Engine-Schlüssel (REQ-989) |
| `PROVISA_ENGINE_URL` | — | Verbindungs-URL für URL-gesteuerte Engines (Snowflake, Databricks, ClickHouse Server, BigQuery, SQLAlchemy) |
| `PROVISA_MATERIALIZE_URL` | — | Materialisierungs-Store-DSN überschreiben (Standard ist der deklarierte Standard der Engine) |
| `PROVISA_DATA_DIR` | `~/.provisa` | Datenverzeichnis für den eingebetteten DuckDB-Store (REQ-989) |
| `TRINO_HOST` | `localhost` | Trino-Coordinator-Host |
| `TRINO_PORT` | `8080` | Trino-Coordinator-HTTP-Port |
| `GOOGLE_APPLICATION_CREDENTIALS` | — | Pfad zur GCP-Service-Account-Schlüssel-JSON (BigQuery-Engine/-Quelle) |
| `GOOGLE_CLOUD_PROJECT` | — | Standard-GCP-Projekt (BigQuery; wird von der URL überschrieben) |
| `FABRIC_SQL_SERVER` | — | Fabric-Warehouse-SQL-Endpunkt (Alternative zu `PROVISA_ENGINE_URL`) |
| `FABRIC_DATABASE` | — | Fabric-Warehouse-Datenbankname |
| `SYNAPSE_SQL_SERVER` | — | Synapse-Serverless-SQL-Endpunkt |
| `SYNAPSE_DATABASE` | — | Synapse-Datenbankname |
| `REDIS_URL` | — | Redis-Verbindungs-URL |
| `PROVISA_SAMPLE_SIZE` | `10000` | Standard-Sampling-Grenzwert |
| `PROVISA_DEFAULT_ROW_LIMIT` | `100` | Zeilenobergrenze, wenn eine Abfrage kein explizites `LIMIT` liefert |
| `PROVISA_RETRY_BUDGET_SECS` | `30` | Tier-1-Read-Retry-Budget in Sekunden; exponentielles Backoff mit vollem Jitter (REQ-703) |
| `ZAYCHIK_PORT` | `8480` | Port des Zaychik-Flight-SQL-Proxys |
| `FLIGHT_PORT` | `8815` | Port des Provisa-Arrow-Flight-Servers |
| `GRPC_PORT` | `50051` | Port des Provisa-Protobuf-gRPC-Servers |
| `PROVISA_REDIRECT_ENABLED` | `false` | Serverseitigen Schwellenwert-Redirect aktivieren |
| `PROVISA_REDIRECT_THRESHOLD` | `1000` | Standard-Zeilenanzahl-Schwellenwert |
| `PROVISA_REDIRECT_FORMAT` | `parquet` | Standard-Redirect-Format |
| `PROVISA_REDIRECT_BUCKET` | `provisa-results` | S3-Bucket für weitergeleitete Ergebnisse |
| `PROVISA_REDIRECT_ENDPOINT` | — | S3-kompatible Endpunkt-URL |
| `PROVISA_REDIRECT_ACCESS_KEY` | — | S3-Zugriffsschlüssel |
| `PROVISA_REDIRECT_SECRET_KEY` | — | S3-Geheimschlüssel |
| `PROVISA_REDIRECT_TTL` | `3600` | TTL der Presigned-URL (Sekunden) |
| `ANTHROPIC_API_KEY` | — | Claude-API-Schlüssel (Discovery) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | Überschreibt `observability.endpoint` |
| `OTEL_SERVICE_NAME` | `provisa` | Überschreibt `observability.service_name` |
| `OTEL_LOG_LEVEL` | `WARNING` | Überschreibt `observability.log_level` |
| `OTEL_COMPACT_BATCH_SIZE` | `10` | Überschreibt `observability.compact_batch_size` |
| `OTEL_SPAN_EXPORT_DELAY_MILLIS` | `1000` | Flush-Verzögerung des Batch-Span-Prozessors |
| `PROVISA_SUPPORT_OTLP_ENDPOINT` | — | Überschreibt `observability.support_endpoint` |
