# Konfigurationsreferenz {: #configuration-reference }

Provisa wird über eine YAML-Datei konfiguriert (Standard: `config/provisa.yaml`).

## Includes

Teilen Sie eine Konfiguration mit `includes:` auf mehrere Dateien auf. Die einbindende Datei listet Fragmentpfade unter diesem Schlüssel auf; Provisa führt sie vor der Validierung zusammen, was zum gleichen Ergebnis führt wie das Schreiben von allem in einer einzigen Datei.

```yaml
# provisa-with-sources.yaml — wrapper that adds a Neo4j source to the base config
includes:
  - /path/to/config/provisa-install.yaml
  - /path/to/demo/sources/neo4j/fragment.yaml
```

Eine Wrapper-Datei, die nur `includes:` enthält, ist gültig. `load_control_plane` liest durch Includes hindurch, sodass der Abschnitt `control_plane:` aus derjenigen eingebundenen Datei stammt, die ihn setzt.

**Merge-Regeln**

- Pfade werden relativ zur einbindenden Datei aufgelöst. Absolute Pfade werden unverändert übernommen.
- Listenabschnitte (`sources`, `tables`, `domains`, `relationships`, `roles`, …) werden angehängt, und Fragmenteinträge folgen auf die Einträge der einbindenden Datei.
- Ein skalarer oder Mapping-Schlüssel, den die einbindende Datei nicht setzt, wird aus dem Fragment übernommen.
- Ein Schlüssel, den beide Dateien auf unterschiedliche Werte setzen, ist ein Konflikt; das Laden schlägt fehl und nennt den Schlüssel.
- Identische Werte in beiden Dateien sind kein Konflikt.
- Includes können verschachtelt werden. Eine Datei, die sich selbst einbindet — direkt oder über ein anderes Fragment — wird abgelehnt.
- `includes` wird beim Laden konsumiert und erscheint nie in der validierten Konfiguration.

## Quellen {: #sources }

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

Alle Quellen teilen sich einen gemeinsamen Feldsatz.

| Feld | Standard | Hinweise |
| ------- | --------- | ------- |
| `id` | erforderlich | Alphanumerisch, Bindestriche, Unterstriche |
| `type` | erforderlich | Siehe Tabelle unten |
| `host` | `""` | Hostname oder IP |
| `port` | `0` | `0` bedeutet, dass jeder Connector seinen eigenen Standard liefert — es gibt keine zentrale Standard-Port-Zuordnung |
| `database` | `""` | |
| `username` | `""` | |
| `password` | `""` | Unterstützt `${env:VAR}`- und `${secret:NAME}`-Zugangsdatenreferenzen — siehe [Secrets](secrets.md) |
| `path` | `null` | Dateipfad oder URI für dateibasierte Quellen |
| `base_url` | `null` | Basis-URL für API-Quellen |
| `pool_min` / `pool_max` | `1` / `5` | Grenzen des Connection-Pools |
| `cache_enabled` | `true` | Caching für alle Tabellen dieser Quelle umschalten |
| `cache_ttl` | `null` | Sekunden; `null` erbt den globalen Standard; `-1` läuft nie ab |
| `federation_hints` | `{}` | Pro-Connector erweiterte Parameter (dict[str,str]); siehe Typreferenz unten. |
| `mapping` | `{}` | Mapping-DSL für redis, elasticsearch, prometheus. |
| `allowed_domains` | `[]` | Beschränkt diese Quelle auf bestimmte Domänen-IDs; leer = uneingeschränkt |
| `description` | `""` | |

### Unterstützte Quellentypen {: #supported-source-types }

| Typ | Verbindungsstil | Hinweise |
| ------ | ----------------- | ------- |
| **RDBMS** | | |
| `postgresql` | host/port | Asyncpg-Pool; PgBouncer opt-in über `use_pgbouncer` |
| `mysql` | host/port | |
| `mariadb` | host/port | |
| `singlestore` | host/port | |
| `sqlserver` | host/port | |
| `oracle` | host/port | |
| `firebird` | host + `path` (DB-Datei) | DuckDB-Firebird-Community-Extension |
| `duckdb` | host/port | |
| `cockroachdb` | host/port | Nutzt den PostgreSQL-Treiber/-Dialekt wieder |
| `yugabytedb` | host/port | Nutzt den PostgreSQL-Treiber/-Dialekt wieder |
| `greenplum` | host/port | Nutzt den PostgreSQL-Treiber/-Dialekt wieder |
| `tidb` | host/port | Nutzt den MySQL-Treiber/-Dialekt wieder |
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
| `hudi` | `path` (Tabellen-URI) | ClickHouse-Hudi-Engine, Zero-Copy |
| `hive` | host/port (Metastore) + `mapping.storage` | Storage-Backend in `mapping["storage"]`: hadoop/hdfs/local/s3/azure/adls |
| `hive_s3` | host/port (Metastore) + `mapping` S3-Schlüssel | Eigenständiger Typ; immer S3-Storage |
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
| `airport` | `base_url` (Flight-Location) | DuckDB-Airport-Extension |
| `ingest` | Push-Receiver | Externe Dienste senden JSON-Ereignisse per POST |
| **SaaS** | | |
| `sharepoint` | `base_url` oder `host` + `mapping` | Auth über `mapping.auth_type` |
| `splunk` | `host`/`port` oder `base_url` + `mapping` | |
| **GovData** | | |
| `govdata` | subject + `domain_id` | Separates `GovDataSource`-Modell; siehe §GovData unten |
| **Data Quality** | | |
| `soda` | host/port, zielt auf Provisas pgwire | Benötigt das `soda`-Extra; Elastic License 2.0, nur selbst gehostet |
| `great_expectations` | host/port, zielt auf Provisas pgwire | Benötigt das `gx`-Extra; Apache 2.0 |

### Quellentyp-Referenz {: #source-type-reference }

Jeder Typ, der eine nicht offensichtliche Konfiguration braucht, hat unten einen kurzen Eintrag. RDBMS-Typen (postgresql, mysql usw.) nutzen nur die gemeinsamen Felder oben und brauchen keinen eigenen Abschnitt.

#### GovData

`govdata`-Quellen haben ein eigenes Modell auf oberster Ebene, `GovDataSource`, statt des allgemeinen `Source`. Der Zugriff ist nach Themengruppen aufgeteilt.

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

Jedes Subject bildet auf ein oder mehrere GovData-Schemas ab. Die Konfiguration einer `govdata`-Quelle mit einem Subject stellt automatisch alle Schemas für dieses Subject bereit.

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

Die Schemas `ref` und `geo` sind als Verknüpfungsschemas immer dabei. Sie lassen sich nicht konfigurieren und stehen nicht in der Liste oben. Mit dem Thema `ALL` gewähren Sie Zugriff auf jedes Schema.

#### Kafka

Die `kafka`-Zeile in `sources:` registriert die Quelle nur. Das `details()` ihres Connectors liefert `{}`; die eigentliche Konfiguration steht im Block `kafka_sources[]` auf oberster Ebene. Kafka ist immer eine VIRTUAL_SOURCE: Es läuft über die Engine und hat keinen direkten Pool.

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

**Zeitfenster** — `default_window` begrenzt jede Abfrage auf einen kürzlichen Zeitraum und verhindert unbegrenzte Lesevorgänge aus hochvolumigen Topics. Format: `1h`, `30m`, `7d`, `60s`. Standard ist `1h`. Automatisch injiziert als `WHERE _timestamp >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR`. Clients können dies mit ihrem eigenen `_timestamp`-Filter im GraphQL-`where`-Argument überschreiben.

**Discriminator** — Mehrere Topic-Konfigurationen können auf dasselbe physische Kafka-Topic mit unterschiedlichen `discriminator`-Werten zeigen und erzeugen so separate GraphQL-Typen. Der Discriminator wird automatisch als WHERE-Klausel injiziert.

**Schema Source**

| Wert | Verhalten |
| ------- | ---------- |
| `registry` | Schema aus der Confluent Schema Registry abrufen |
| `manual` | Spalten inline in der Konfiguration definieren (keine Schema Registry nötig) |
| `sample` | Automatische Erkennung aus Beispielnachrichten |

#### Snowflake

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

#### Databricks

`http_path` in `federation_hints` ist Pflicht. `password` trägt das persönliche Zugriffstoken. `catalog` ist optional und steht im SQL oder in den Hints; das Feld `database` enthält es nicht.

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

`project` in `federation_hints` ist erforderlich. Die Authentifizierung nutzt `GOOGLE_APPLICATION_CREDENTIALS` (Pfad zu einer Service-Account-Schlüsseldatei) oder Application Default Credentials in der Engine-Umgebung.

```yaml
sources:
  - id: my-bigquery
    type: bigquery
    federation_hints:
      project: my-gcp-project     # required
```

#### Fabric / Synapse

Beide nutzen T-SQL über TDS mit Azure-AD-Authentifizierung. Melden Sie sich mit `az login` (Entwicklung) oder einer verwalteten Identität (Produktion) an; die Engine liest die Anmeldedaten über `DefaultAzureCredential` aus `azure-identity`. Die Verbindungsdaten kommen aus Umgebungsvariablen, `FABRIC_SQL_SERVER` / `FABRIC_DATABASE` (Fabric) oder `SYNAPSE_SQL_SERVER` / `SYNAPSE_DATABASE` (Synapse), oder aus `PROVISA_ENGINE_URL`.

```yaml
sources:
  - id: my-fabric
    type: fabric
    # host/database read from FABRIC_SQL_SERVER / FABRIC_DATABASE when not set here
```

#### ClickHouse

`secure` in `federation_hints` aktiviert TLS auf der HTTP-Schnittstelle. Der Port ist standardmäßig `8123` (unverschlüsselt) oder `8443` (wenn `secure: "true"`). `schema` in `federation_hints` überschreibt das Remote-Schema.

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

#### Hive / Hive S3

`host` und `port` zeigen auf den Hive-Thrift-Metastore (Standardport 9083). Für `hive` wählen Sie mit `mapping["storage"]` das Backend des Objektspeichers. Ein fehlender Pflichtschlüssel scheitert lautstark, ohne Ausweichwert.

`hive_s3` ist ein eigenständiger Typ, der immer S3-Storage deklariert; kein `mapping.storage` nötig.

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

#### Redis

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

#### Google Sheets

`spreadsheet_id` in `federation_hints` ist erforderlich. Auth nutzt ein DuckDB-`gsheet`-SECRET, das zum Zeitpunkt des Attach bereitgestellt wird.

```yaml
sources:
  - id: my-sheet
    type: google_sheets
    federation_hints:
      spreadsheet_id: 1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms
```

#### Dateiquellen (csv / parquet / sqlite / files) {: #file-sources-csv-parquet-sqlite-files }

`path` ist erforderlich. `files` durchsucht ein Verzeichnis nach CSV-, Parquet-, XLSX- und JSON-Dateien und stellt jede als Tabelle bereit. Alle dateibasierten Quellen sind VIRTUAL (laufen über die Engine; kein direkter Pool).

```yaml
sources:
  - id: orders-csv
    type: csv
    path: /data/orders.csv

  - id: data-lake-dir
    type: files
    path: /data/lake/         # directory; each file becomes a table
```

**Kaggle-Datasets** lassen sich nicht über diese Datei hinzufügen, weil sie ein gültiges Token und die Dataset-Auswahl im Quellenformular brauchen (Quellen → Abonnements → Kaggle). Eine nach YAML exportierte Kaggle-Quelle erscheint als `type: files` mit `kaggle_owner` und `kaggle_ref` in `federation_hints`. Ein erneuter Download von Kaggle geht über die Mutation `refreshKaggleSource` oder die Aktualisierung in der UI; eine Änderung am YAML bewirkt das nicht. Siehe [Kaggle-Datasets](sources.md#kaggle-datasets) in der Referenz der Quelltypen.

#### API-/Remote-Quellen {: #api-remote-sources }

**openapi** — setzen Sie `base_url` auf die OpenAPI-Basis-URL. Die Schemaerkennung liest die OpenAPI-Spezifikation beim Start.

```yaml
sources:
  - id: payment-api
    type: openapi
    base_url: https://api.payments.example.com/v1
```

**graphql_remote** — setzen Sie `base_url`. Optionale `mapping`-Schlüssel: `headers` (Dict statischer Header), `forward_client_headers` (bool), `timeout_seconds` (int).

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

**airport** — `base_url` ist der Arrow-Flight-Server-Standort. DuckDB-Airport-Extension.

```yaml
sources:
  - id: flight-source
    type: airport
    base_url: grpc://flight.internal:8815
```

**websocket / rss** — verwenden Sie `host`, `port`, `path` und `federation_hints`.

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

#### Data-Quality-Checker (soda / great_expectations) {: #data-quality-checkers-soda-great_expectations }

Eine Checker-Quelle zeigt auf Provisas eigenen pgwire-Endpunkt, sodass ein einziger Postgres-Treiber die föderierte Sicht einer Tabelle scannt, hinter der Snowflake oder Iceberg steht. Die Scan-Identität deklarieren Sie; sie wird nie geerbt. Auf dieser Verbindung gilt die Policy, und eine gefilterte Zeilenmenge darf eine Prüfung nicht unbemerkt bestehen lassen. Die Verbindungsschlüssel stammen aus `mapping`: `host`, `port`, `database`, `user`, `password`.

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

Jede Ergebnistabelle trägt `dq_contract`, das Soda-Vertrags-YAML oder das Great-Expectations-Suite-JSON Wort für Wort. Spalten, Watermark und Promotions werden daraus abgeleitet; wie, steht unter [Data Quality Checkers](sources.md#data-quality-checkers).

**Auswahl bei der Installation.** Der Checker wird nicht eingebunden: Der Scan läuft in einem Kind-Interpreter, und die Bibliothek wird nur installiert, wenn ein Betreiber sie nennt. Jeder Installationsweg (`install.sh`, `packaging/linux/first-launch.sh` und der macOS-Assistent über `PROVISA_DQ_CHECKER`) schreibt die Wahl nach `~/.provisa/config.yaml`:

```yaml
dq_checker: none        # none | soda | gx
```

`scripts/provisa` liest diesen Schlüssel und exportiert `PROVISA_EXTRAS`, das `docker-compose.app.yml` als Build-Argument an `ARG PROVISA_EXTRAS` im `Dockerfile` übergibt:

| `dq_checker` | `PROVISA_EXTRAS` (Docker-Tier) | Native venv-Installation |
| -------------- | -------------------------------- | --------------------- |
| `none` | `firebase,vector` | `provisa[embedded]` |
| `soda` | `firebase,vector,soda` | `provisa[embedded,soda]` |
| `gx` | `firebase,vector,gx` | `provisa[embedded,gx]` |

Die Installation des Demo-Datasets hebt `none` auf `gx` an und meldet dies, da die Demo-Konfiguration eine Great-Expectations-Suite über `pet_store.pets` registriert und deren Qualitäts-Scorecard sonst nichts anzuzeigen hätte. Bei Angabe von `soda` bleibt `soda` bestehen.

Wer die Demo über pip statt über ein Installationsprogramm erreicht, überspringt diesen Assistentenschritt, deshalb bringt das Extra `demo` denselben Checker mit: `provisa run --demo` braucht `pip install 'provisa[embedded,demo]'`, damit sein Scan läuft. Ohne es meldet der Scan `data-quality checker 'great_expectations' is not installed` und nennt den Installationsbefehl.

Jeder andere Wert hält den Starter an, sodass er nie ohne den Checker startet, den der Betreiber verlangt hat. Das Extra `soda` zieht `soda-postgres` nach, `gx` zieht `great-expectations[postgresql]` nach. Soda Core steht unter der Elastic License 2.0: `config/capabilities.yaml` markiert die Option mit `cloud_eligible: false`, und das gehostete Plane lehnt sie ab.

## Domänen {: #domains }

```yaml
domains:
  - id: sales-analytics
    description: Sales operational data
```

## Namensgebung {: #naming }

```yaml
naming:
  convention: apollo_graphql   # snake, hasura_graphql, apollo_graphql (default)
  domain_prefix: true          # prepend domain_id__ to all GraphQL names
  rules:
    - pattern: "^prod_pg_"
      replace: ""
```

### Namenskonvention {: #naming-convention }

Die Namensautorität ist die einzige Quelle der Wahrheit für clientseitige Namen; physische Backend-Spaltennamen werden Clients nie offengelegt. Jede Abfragesprache leitet den Namen einer Spalte aus ihrem `column.alias` ab, falls gesetzt, andernfalls aus dem physischen Spaltennamen über die konfigurierte Konvention.

Die GraphQL-Konvention ist eines von drei voreingestellten Enums. Alte Freiform-Strings (`none`, `snake_case`, `camelCase`, `PascalCase`) sind veraltet.

| Preset | Standard | Typnamen | Feldnamen | Mutationsnamen |
| -------- | --------- | ------------ | ------------- | ---------------- |
| `apollo_graphql` | ja | PascalCase | camelCase | camelCase |
| `hasura_graphql` | | PascalCase | camelCase | snake_case |
| `snake` | | PascalCase | snake_case | snake_case |

Die Standard-GraphQL-Konvention ist `apollo_graphql`, was camelCase-Feld- und -Mutationsnamen erzeugt. Die SQL-Konvention ist separat, mit Standard `snake_case`, angewendet über `apply_sql_name()`; die GraphQL-Konvention wird über `apply_gql_name()` angewendet, und der CQL-Name wird vom GraphQL-Namen abgeleitet.

`domain_prefix: bool` ist eine orthogonale Option, die unabhängig vom gewählten Preset gilt.

Ein explizites `column.alias` ist der kanonische Name: SQL verwendet ihn unverändert ohne angewendete Konvention, GraphQL wendet seine Konvention darauf an, und CQL leitet sich vom GraphQL-Namen ab.

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

### Domänen-Präfix {: #domain-prefix }

Wenn `domain_prefix: true`, werden alle GraphQL-Feld- und Typnamen mit der Domänen-ID unter Verwendung eines doppelten Unterstrichs als Trenner präfixiert:

| Tabelle | Domäne | Feldname |
| ------- | -------- | ----------- |
| `orders` | `sales-analytics` | `sales_analytics__orders` |
| `customer_segments` | `customer-insights` | `customer_insights__customer_segments` |

Dies verhindert Namenskollisionen, wenn unterschiedliche Domänen Tabellen mit demselben Namen haben, und macht Abfragen selbstdokumentierend.

### Namensregeln {: #naming-rules }

Regex-Regeln, die auf Tabellennamen angewendet werden, wenn GraphQL-Feldnamen generiert werden. Angewendet der Reihe nach vor der Eindeutigkeitsauflösung.

## Tabellen {: #tables }

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

Eine Tabelle trägt standardmäßig keine Schreibberechtigung, daher kann keine Rolle in sie schreiben. Eine Schreibberechtigung gibt einer Rolle eine Operation (`insert`, `update` oder `delete`) auf der Tabelle. Eine Tabelle enthält höchstens eine Berechtigung je Rolle und Operation.

| Feld der Berechtigung | Bedeutung |
| ----------- | ------- |
| `columns` | Die Spalten, die ein `INSERT` auflisten oder ein `UPDATE` setzen darf. Eine `delete`-Berechtigung nennt keine. |
| `rows` | Die Zeilen, die ein `UPDATE` oder `DELETE` berühren darf: ein SQL-Prädikat über die Spalten der Tabelle. Eine `insert`-Berechtigung hat keines. |
| `check` | Was jede von einem `INSERT` geschriebene oder von einem `UPDATE` hinterlassene Zeile erfüllen muss: ein SQL-Prädikat. Eine `delete`-Berechtigung hat keines. |
| `presets` | Spalten, die die Berechtigung selbst setzt. Die Anweisung darf sie nicht liefern, daher steht eine Preset-Spalte nicht zusätzlich in `columns`. |

Lassen Sie `rows` oder `check` weg, übernimmt die Berechtigung den Lese-Zeilenfilter der Rolle auf der Tabelle: Eine Rolle schreibt die Zeilen, die sie liest, sofern die Berechtigung nichts anderes festlegt. `"TRUE"` steht für jede Zeile. Die älteren Schlüssel `writable_by` an der Spalte und `column_presets` an der Tabelle werden namentlich abgelehnt; die Konfiguration lässt sich erst laden, wenn sie durch `write_grants` ersetzt sind.

Jeder Schreibvorgang auf jeder Oberfläche wird in dieser Reihenfolge zugelassen: Die Rolle besitzt das Recht `write`; die Rolle besitzt eine Berechtigung für die Operation auf der Tabelle; jede Spalte, die die Anweisung schreibt, steht in der Berechtigung; die Presets der Berechtigung werden der Anweisung hinzugefügt; das Prädikat `rows` wird per AND in das `WHERE` eines `UPDATE` oder `DELETE` aufgenommen; und `check` wird auf die neuen Zeilen ausgewertet, bevor etwas an die Quelle gesendet wird. Eine Anweisung, die `check` nicht entscheiden kann, wird abgelehnt. Die Spalten der Berechtigung bestimmen auch, welche Mutationsfelder GraphQL einer Rolle anbietet: ein Feld je Operation, für die sie eine Berechtigung besitzt, mit genau diesen Spalten.

Manche Anweisungen brauchen eine weitergehende Berechtigung. `TRUNCATE` braucht eine `delete`-Berechtigung über jede Zeile. `MERGE` braucht eine `insert`- und eine `update`-Berechtigung, beide über jede Zeile. `INSERT ... ON CONFLICT DO UPDATE` braucht eine `update`-Berechtigung über jede Zeile ohne Presets. `COPY ... FROM STDIN` braucht eine `insert`-Berechtigung ohne Presets.

Berechtigungen werden vererbt. Eine untergeordnete Rolle übernimmt je Tabelle und Operation die Berechtigung der nächstgelegenen Rolle in ihrer Kette, die eine besitzt, wobei ihre eigene Vorrang hat. Eine Meta-Rolle hält die Faltung der Berechtigungen ihrer Mitglieder: die Vereinigung ihrer Spalten, das ODER ihrer `rows`- und `check`-Prädikate und nur die Presets, die alle Mitglieder gleich setzen.

In der Admin-Oberfläche listet der Abschnitt **Schreibrechte** einer Tabelle die Berechtigungen auf und bearbeitet sie gemeinsam.

### Aliase {: #aliases }

Tabellen- und Spaltenaliase überschreiben den Standard-GraphQL-Namen. Nützlich für:

- Umbenennung kryptischer Datenbanknamen (z. B. `tbl_cust_seg` → `customer_segments`)
- Vermeidung von Abkürzungen in der API-Schicht
- Erstellung eines sauberen, domänenspezifischen Vokabulars

### Beschreibungen {: #descriptions }

Tabellen- und Spaltenbeschreibungen sind im generierten GraphQL-SDL enthalten. Sie erscheinen im Dokumentations-Explorer von GraphiQL und in Introspektionsabfragen. Setzen Sie sie in der Konfigurations-YAML oder über die Admin-UI.

### Pfad (Berechnete JSON-Extraktion) {: #path-computed-json-extraction }

Spalten können Werte aus einer JSON-/JSONB-Quellspalte mithilfe eines Punktnotation-`path` extrahieren. Dies ist nützlich für semistrukturierte Daten in Kafka-Nachrichten, MongoDB-Dokumenten oder PostgreSQL-JSONB-Spalten.

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

Das Pfadformat ist `source_column.key1.key2...`. Der Compiler generiert `json_extract_scalar(source_column, '$.key1.key2')` im SQL.

**Routing-Auswirkung:** Pfad-Spalten verwenden PostgreSQL-JSON-Operatoren (`->>`), die vom direkten PG-Routing nativ unterstützt werden. Für Nicht-PostgreSQL-Quellen (MySQL, SQL Server usw.) werden Abfragen mit Pfad-Spalten automatisch über die Föderations-Engine geroutet. Mutationen sind davon nicht betroffen, da Pfad-Spalten schreibgeschützte berechnete Felder sind.

### Maskierungstypen {: #masking-types }

| Typ | Felder | Beschreibung |
| ------ | -------- | -------------- |
| `regex` | `pattern`, `replace` | REGEXP_REPLACE (nur String-Spalten) |
| `constant` | `value` | Literaler Ersatz (NULL, 0, MAX, MIN, benutzerdefiniert) |
| `truncate` | `precision` | DATE_TRUNC (nur Datums-/Zeitstempel-Spalten) |

## Beziehungen {: #relationships }

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

### Auto-Materialisierung {: #auto-materialization }

Setzen Sie `materialize: true` auf einer Beziehung, um automatisch eine materialisierte Sicht für quellenübergreifende JOINs zu generieren. Dies vermeidet teure föderierte Abfragen, indem das JOIN-Ergebnis vorab berechnet wird.

- Nur quellenübergreifende Beziehungen generieren MVs (Joins innerhalb derselben Quelle sind bereits schnell)
- Bei einer Junction-gestützten Beziehung deckt die MV die Zwei-Sprung-Traversierung ab: Quellsprung, Junction-Sprung, Diskriminator und die eigenen Spalten der Junction als Kantenattribute. Die Junction zählt als Bein, eine Kante ist also quellenübergreifend, sobald eine der drei Tabellen in einer anderen Quelle liegt
- Die MV startet veraltet und wird von der Hintergrund-Refresh-Schleife befüllt
- Mutationen an einer der beiden Quelltabellen markieren die MV zur erneuten Aktualisierung als veraltet
- `refresh_interval` ist standardmäßig 300 Sekunden (5 Minuten)

## Rollen {: #roles }

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

Rollen mit `parent_role_id` erben Capabilities, Domänenzugriff, Spalten- und Objektfreigaben sowie RLS-Regeln von der übergeordneten Rolle, wobei die eigene RLS-Regel des Kindes für eine Tabelle Vorrang hat. Die Kette wird beim Start flach aufgelöst.

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

## RLS-Regeln {: #rls-rules }

```yaml
rls_rules:
  - table_id: orders
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"
    description: Analysts read only their own region's orders.
```

## Materialisierte Sichten {: #materialized-views }

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

## Sichten (Regierte berechnete Datasets) {: #views-governed-computed-datasets }

Sichten sind SQL-definierte berechnete Datasets mit vollständiger spaltenweiser Governance. Sie sind der governte Mechanismus zum Hinzufügen von Aggregationen, Transformationen und abgeleiteten Kennzahlen zum Modell.

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

### Materialisiert vs. Live {: #materialized-vs-live }

- **`materialize: true`**: Provisa erstellt eine Tabelle via CTAS und aktualisiert sie nach Zeitplan. Schnellere Abfragen, aber die Daten können bis zu `refresh_interval` Sekunden veraltet sein.
- **`materialize: false`**: Provisa erstellt eine föderierte Sicht. Abfragen liefern immer Live-Daten, können aber bei komplexen Aggregationen langsamer sein.

Sichten durchlaufen dieselbe Governance-Pipeline wie Tabellen: RLS, Maskierung, Stichproben und rollenbasierte Sichtbarkeit. So kann nichts der Plattform neue Semantik hinzufügen, ohne dass ein Steward darüber wacht.

### Sichten mit Historie (Zeitreise) {: #views-with-history-time-travel }

Eine Tabelle oder eine materialisierte Sicht kann jede Version ihrer Zeilen behalten und zu einem vergangenen Zeitpunkt gelesen werden. Sie bilden dafür drei Spalten der Quelle ab, und Sie deklarieren alle drei oder keine:

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

| Feld | Beschreibung |
| ------- | ------------- |
| `business_key` | Die Spalten, die über alle Versionen hinweg dasselbe Ding bezeichnen, etwa `transaction_id`. Jede Version eines Datensatzes hat diesen Schlüssel. |
| `valid_time` | Die Spalte mit dem Datum, das Ihr Unternehmen dem Datensatz gibt: wann er vereinbart, gebucht oder wirksam wurde. |
| `recorded_time` | Die Spalte mit dem Zeitpunkt, zu dem diese Version des Datensatzes erfasst oder geändert wurde, etwa `updated_at`. |

Jede davon nennt eine Spalte der Tabelle oder Sicht. Eine Sicht, die sie deklariert, setzt `materialize: true`, und Provisa wählt, wie die Versionen gespeichert werden. Ein Eintrag unter `tables:` nimmt dieselben drei Schlüssel an.

Die beiden Zeiten heißen anderswo anders: Aufzeichnungszeit (recorded time), in der Literatur zu temporalen Datenbanken Transaktionszeit und in SQL:2011 Systemzeit genannt; Gültigkeitszeit (valid time), in SQL:2011 Anwendungszeit genannt oder Wirksamkeitsdatum (effective date).

Für dieselbe Version können die beiden auseinanderfallen:

| Beispiel | `valid_time` | `recorded_time` |
| ------- | ------------ | --------------- |
| Eine Zahlung von 100 für den 3. Oktober wird am 5. Oktober auf 120 korrigiert. | Die Korrektur behält den 3. Oktober. | Die Korrektur erhält den 5. Oktober. |
| Eine Feldmessung vom 3. Oktober 14:10 wird am 6. Oktober eingegeben. Ein Tippfehler darin wird am 7. Oktober behoben. | 3. Oktober 14:10, von der Korrektur beibehalten. | 6. Oktober. Die Korrektur erhält den 7. Oktober. |
| Ein Handel wird bei Ausführung mit Handelstag 3. Oktober gebucht. Am 4. Oktober erweist sich die Menge als falsch und wird geändert. | Handelstag 3. Oktober, von der Änderung beibehalten. | 3. Oktober 10:02. Die Änderung erhält den 4. Oktober. |

Gelesen wie am 3. Oktober erfasst, zeigt der Handel die falsche Menge. Gelesen nach heutigem Wissen, zeigt er die korrigierte.

Eine Version gilt von ihrem Zeitpunkt bis zu dem der nächsten Version. Ein Löschen wirkt ab dem Zeitpunkt, zu dem Provisa es erfasst.

**Lesen.** Ein einfacher Lesevorgang liefert die jetzt geltende Version jedes Schlüssels. Um eine andere Zeit zu lesen, geben Sie der Relation dort, wo die Anweisung sie nennt, ihr eigenes Argument `as_of`, einen Zeitpunkt oder einen Bereich, angegeben als ISO-8601-Intervall `start/end`:

```sql
SELECT * FROM "banking"."view_eod_totals"(as_of => '2026-03-01T00:00:00') AS e
SELECT * FROM "banking"."view_eod_totals"(as_of => '2026-01-01T00:00:00/2026-04-01T00:00:00') AS e
```

Ein Zeitpunkt liefert die damals geltende Version jedes Schlüssels. Ein Bereich liefert jede Version, die zu irgendeinem Zeitpunkt darin galt, eine Zeile pro Version.

**Wie erfasst.** Eine später erfasste Korrektur ändert, was eine Sicht über die Vergangenheit sagt: Die am 30. September geltende Version ist die korrigierte, sobald die Korrektur erfasst ist. Um die Sicht so zu lesen, wie Provisa sie zu einem Zeitpunkt erfasst hatte, vor jeder späteren Korrektur, fügen Sie `as_recorded` hinzu, einen ISO-8601-Zeitpunkt:

```sql
SELECT * FROM "banking"."view_eod_totals"(as_of => '2026-09-30T12:00:00', as_recorded => '2026-10-01T00:00:00') AS e
```

Jede danach erfasste Version bleibt weg, und das As-of wählt dann die Version, die zu seiner eigenen Zeit galt. Mit `as_recorded` allein liefert der Lesevorgang die Version jedes Schlüssels, die laut damaliger Erfassung galt. Es nimmt einen Zeitpunkt, keinen Bereich.

**Wo die Argumente stehen.** Jede Referenz trägt ihre eigene Zeit, sodass eine Anweisung eine Sicht über zwei Referenzen zu zwei Zeiten lesen kann; eine Referenz ohne Argumente liest den aktuellen Stand. Eine Zeit kann auch ein gebundener Wert sein, `(as_recorded => $1)`, dessen Wert in den `params` der Anfrage mitgesendet wird. Nur eine Relation, die ihre Versionen behält, nimmt die Argumente: eine Tabelle oder Sicht mit Historie und eine Iceberg- oder Delta-Tabelle, die über ihre Snapshots reist und daher nur `as_recorded` nimmt (den damals aktuellen Snapshot). Jede andere Relation, eine fehlerhafte Zeit, ein Bereich für `as_recorded` oder `as_of` auf einer Iceberg- oder Delta-Tabelle wird mit HTTP 400 abgelehnt, unter Nennung der Relation.

Dieselben zwei Namen sind die Argumente des Felds der Relation in GraphQL, auf Root-Feldern und auf einem Beziehungsfeld, dessen Ziel seine Versionen behält, sodass zwei Aliasse eine Sicht zu zwei Zeiten lesen können:

```graphql
{
  reported: view_eod_totals(as_recorded: "2026-10-01T17:00:00") { branch net_movement }
  restated: view_eod_totals(as_recorded: "2026-10-12T17:00:00") { branch net_movement }
}
```

REST nimmt sie als die Abfrageparameter `as_of` und `as_recorded` des Tabellen-Endpunkts, und das MCP-Tool `describe_table` sagt, welche Argumente eine Tabelle nimmt. Das Feld `resolve` der Domäne `meta` (GraphQL, REST und MCP) nimmt ebenfalls `as_of` und `as_recorded`, und ein ihm übergebener Bereich liefert die `versions` der Zeile statt ihrer `row`; ebenso der gRPC-Aufruf `Resolve`. Ein Arrow-Flight-Ticket nimmt sie als seine Optionen `as_of` und `as_recorded`, und eine pgwire-Sitzung setzt mit `SET provisa.as_of` und `SET provisa.as_recorded` einen Standard für die Anweisungen, die sie ausführt, den die eigenen Argumente einer Referenz überschreiben. Die Auswahlfelder „As of“ und „As recorded at“ des SQL-Explorers schreiben die Argumente in die Anweisung. Der Audit-Eintrag jeder Anweisung hält die Zeit fest, zu der sie gelesen hat, `as_of` und `as_recorded`, wenn sie zu einer Zeit gelesen hat. Rechte, Masken und Zeilenregeln gelten wie bei jedem Lesevorgang.

Jede Sicht mit Historie liefert drei weitere Spalten, freigegeben wie ihre Quellzeit-Spalte (sonst ihre erste Schlüsselspalte):

| Spalte | Bedeutung |
| ------- | ------- |
| `source_from` | Wann diese Version wirksam wurde: ihre Quellzeit, ohne eine solche ihre Ladezeit |
| `source_to` | Wann die nächste Version wirksam wurde; leer, solange diese gilt |
| `recorded_at` | Wann Provisa diese Version erfasst hat |

**Quellzeit aus Change Data Capture.** Eine per Change Data Capture gelandete Tabelle trägt ihre Commit-Zeit als `source_committed_at`, damit eine Sicht mit Historie sie als Quellzeit benennen kann:

- Debezium: die Commit-Zeit der Datenbank, die jedes Änderungsereignis trägt (`source.ts_ms`). Eine durch einen vollständigen Build des Replikats kopierte Zeile hat kein Änderungsereignis, daher ist ihr `source_committed_at` leer, bis sich die Zeile das nächste Mal ändert, es sei denn, die Quelle ist PostgreSQL mit eingeschalteten Commit-Zeitstempeln (nächster Punkt).
- PostgreSQL mit dem Änderungssignal `native`: die Commit-Zeit, die PostgreSQL für jede Zeile hält (`pg_xact_commit_timestamp(xmin)`), vom eigenen Treiber der Quelle gelesen, wenn das Replikat gebaut wird. PostgreSQL hält sie nur mit `track_commit_timestamp = on`. Schalten Sie es ein, um mit dem nativen Signal Quellzeit zu erhalten; ohne es ist `source_committed_at` leer. Das Ein- oder Ausschalten der Einstellung baut das Replikat neu.

Kafka-Topics ohne Debezium-Hülle tragen keine Commit-Zeit.

### Sichten nach einem Kalender {: #views-on-a-calendar }

Eine materialisierte Sicht kann ihren Zeitplan aus einem Kalender nehmen: Sie wird zum Schluss jedes Fensters gebaut, das der Kalender ihr gibt (ein Geschäftstag, eine Woche, ein Monat, ein Quartal oder ein Jahr), und eine Sicht mit Historie versieht jede Version mit diesem Schlusszeitpunkt. Eine Konfiguration deklariert ihre Kalender unter `calendars:`:

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

| Feld | Erforderlich | Beschreibung |
| ------- | ---------- | ------------- |
| `name` | Ja | Der Name, den eine Sicht als ihren `calendar` angibt |
| `version` | Ja | Die Versionsbezeichnung. Ein Name kann mehrere Versionen haben, jede mit eigenen Feiertagen. |
| `base_system` | Nein | `gregorian` (Standard), `fiscal` oder `retail_445` |
| `tz` | Nein | Die IANA-Zeitzone, in der die Tage des Kalenders liegen (Standard `UTC`) |
| `fiscal_anchor_month`, `fiscal_anchor_day` | Nein | Monat und Tag, an dem ein Geschäftsjahr beginnt (Standard 1 und 1) |
| `retail_anchor` | Nein | Das erste Datum eines Referenzjahres nach 4-4-5; `retail_445` braucht es |
| `week_start` | Nein | Der Tag, an dem eine Woche beginnt, 0 = Montag bis 6 = Sonntag (Standard 0) |
| `holidays` | Nein | Daten, die keine Geschäftstage sind |
| `weekend` | Nein | Die Wochentage, die keine Geschäftstage sind, 0 = Montag (Standard `[5, 6]`) |
| `day_close` | Nein | Die lokale Uhrzeit, zu der ein Geschäftstag schließt, `"HH:MM"`. Tag D läuft dann vom Schluss des Vortags bis zum Schluss von D. Ohne sie endet ein Tag um Mitternacht. Setzen Sie sie in Anführungszeichen: YAML liest ein `18:00` ohne Anführungszeichen als Zahl. |

Ein Kalender wird vor den Tabellen gespeichert, sodass die Sichten, die ihn nennen, ihn finden. Ein Eintrag, den die Mutation `createCalendar` ablehnen würde, wird mit derselben Meldung `invalid calendar: ...` abgelehnt, und die Konfiguration wird nicht geladen. Der Modellexport schreibt jeden Kalender unter `calendars:`.

Eine Sicht nennt ihren Kalender mit diesen Schlüsseln. Ein Eintrag unter `tables:` nimmt sie als `mv_calendar`, `mv_grain`, `mv_business_day_grain`, `mv_allowed_lateness` und `mv_expected_events`.

| Feld | Beschreibung |
| ------- | ------------- |
| `calendar` | Der `name` des Kalenders |
| `grain` | `daily`, `weekly`, `monthly`, `quarterly` oder `annual`; ein n-ter Wochentag wie `3WE` (dritter Mittwoch) oder `LFR` (letzter Freitag); oder eine RFC-5545-Regel wie `FREQ=WEEKLY;INTERVAL=2;BYDAY=MO` |
| `business_day_grain` | `true`: Ein tägliches Fenster gibt es nur an einem Geschäftstag, sodass an einem Wochenende oder Feiertag kein Build läuft |
| `allowed_lateness` | Sekunden nach dem Schluss eines Fensters, die der Build auf verspätet festgeschriebene Eingaben wartet (Standard 0) |
| `expected_events` | Die Eingaben, die bis zum Schluss des Fensters aktuell sein müssen. Fehlt der Schlüssel, jede Eingabe, die die Sicht liest; `[]`, keine. |

Eine Sicht, die einen Kalender nennt, den weder eine Konfiguration noch `createCalendar` deklariert hat, wird beim Start der Event-Schleife mit Namen abgelehnt: `unknown calendar 'ledger'; registered: [...]`.

**Einen Geschäftstag bauen.** Das Argument `window` der Mutation `refreshMv`, ein Datum, baut die Sicht versiegelt zum Schluss dieses Geschäftstags:

```graphql
mutation {
  refreshMv(mvId: "daily-totals", window: "2026-10-02") { success message code }
}
```

Was der Build anhängt, erhält als Zeitstempel den Schluss des Kalenderfensters für dieses Datum, so wie der Build zum Schluss selbst ihn setzen würde; ein Lesen wie aufgezeichnet zu diesem Schluss liefert ihn also. Eine so gebaute Sicht mit Historie baut die Daten neu, deren Eingaben sich seit ihrem letzten Build geändert haben, sodass eine vor dem Build des nächsten Tages aufgezeichnete Korrektur für ihr eigenes Datum übernommen wird. Eine Sicht, die keinen Kalender nennt, wird abgelehnt (`schema.mv_window_needs_calendar`), ebenso ein Datum, an dem ihr Kalender kein Fenster hat, etwa ein Feiertag bei einer Geschäftstags-Granularität.

### Nur abfragbare Sichten {: #query-only-views }

Sowohl `materialize: true`- als auch `materialize: false`-Sichten exponieren ihren GraphQL-Typ als nur abfragbar. Für `view_sql`-basierte Relationen werden keine Insert-, Upsert-, Update- oder Delete-Mutationen generiert.

## Cache

```yaml
cache:
  enabled: true
  redis_url: ${env:REDIS_URL}
  default_ttl: 300
```

### Cache-Hierarchie {: #cache-hierarchy }

TTL-Auflösungsreihenfolge (spezifischster gewinnt): **Tabelle** > **Quelle** > **globaler Standard**. Der erste nicht-null-Wert wird verwendet.

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

Das Setzen von `cache_enabled: false` auf einer Quelle deaktiviert das Caching für alle Tabellen dieser Quelle, unabhängig von der Tabellen-TTL. Cache-Schlüssel enthalten immer `role_id` + RLS-Kontextwerte zur Sicherheitspartitionierung.

### Opt-in pro Anfrage {: #per-request-opt-in }

Der Antwort-Cache ist für jede Anfrage aus, die ihn nicht anfordert. Eine Anfrage meldet sich so an:

| Oberfläche | Opt-in |
| --- | --- |
| GraphQL (`/data/graphql`, Arrow Flight) | `@cached` an der Operation, optional `@cached(ttl: 60)` |
| SQL (pgwire, Flight SQL, `/data/sql`) | eine Kommentarzeile `-- @provisa cache=true` oder `-- @provisa cache_ttl=60` in der Anweisung |
| Cypher (`/data/cypher`, die Neo4j Query API, Bolt, Arrow Flight) | eine Kommentarzeile `// @provisa cache=true` oder `// @provisa cache_ttl=60` in der Abfrage |
| gRPC (und der HTTP-gRPC-Proxy) | Aufruf-Metadaten (beim Proxy Header) `x-provisa-cache: true` oder `x-provisa-cache-ttl: 60` |
| REST, JSON:API und, alternativ zum Hinweis im Text: GraphQL über HTTP, `/data/cypher`, die Neo4j Query API | der Request-Header `Cache-Control: max-age=60` (oder `max-stale=60`) oder `x-provisa-cache: true` / `x-provisa-cache-ttl: 60` |

Ein `ttl`, der keine Ganzzahl ist, lässt die Anfrage fehlschlagen. Eine Anfrage, die das Opt-in zweimal angibt (in ihrem Text und in einem Header oder in zwei Headern) und dabei verschiedene Werte nennt, schlägt mit `400` fehl; keiner der Werte wird bevorzugt. `Cache-Control: no-cache` oder `no-store` bedeutet kein Cache, wie ohne Header.

Ohne den Hinweis liest und schreibt die Anfrage den Cache nicht. Mit ihm entscheiden weiterhin die obigen Einstellungen: Das Ergebnis wird nur gecacht, wenn jede gelesene Quelle `cache_enabled: true` hat und die aufgelöste TTL jeder Tabelle über 0 liegt. Der Eintrag lebt so lange wie die `ttl` der Anfrage, falls sie eine angibt, sonst so lange wie die kürzeste aufgelöste TTL der gelesenen Tabellen.

Der Hinweis nimmt nur ältere Daten im Tausch gegen Geschwindigkeit in Kauf. Er macht einen Lesevorgang nie aktueller, als die Einstellungen des Betreibers erlauben: Landungs- und Replikat-Aktualität (`cache_ttl`, `change_signal`), `row_materialize`, `load_protected`-Snapshots und Aktualitätsprüfungen gelten für eine Anfrage mit Hinweis genauso wie für eine ohne. Siehe [Aktualität und Last: wer entscheidet](#freshness-and-load-who-decides).

`@noCache` und `-- @provisa no_cache=true` gibt es nicht mehr: Da Caching standardmäßig aus ist, gibt es nichts zu umgehen.

Ein Schreibvorgang invalidiert jeden Cache-Eintrag, den die schreibende Organisation für die geschriebenen Tabellen hält; die Einträge einer anderen Organisation für dieselbe Tabelle bleiben unberührt. Schlägt die Invalidierung fehl, schlägt die Anfrage fehl, statt veraltete Einträge zurückzulassen.

## Aktualität und Last: wer entscheidet {: #freshness-and-load-who-decides }

Drei Parteien bestimmen mit, wie aktuell ein Ergebnis ist und wie viel Last eine Abfrage auf den Systemen dahinter erzeugt.

- **Die Upstream-Quelle.** Idealerweise regelt sie ihren Rückstau selbst: Verbindungslimits, Statement-Timeouts, Lesereplikate.
- **Der Betreiber.** Schützt die Plattform, also Provisa selbst, vor Rückstau. Kann sich eine Quelle nicht selbst schützen, schützt der Betreiber auch sie. Die Einstellungen dafür liegen an Quellen und Tabellen: `load_protected`, `replicate`, `federation_hints` der Quelle, der Schwellenwert für die Umleitung großer Ergebnisse, der Kafka-Cluster, in den Sinks schreiben, und das registrierte Watermark jeder Tabelle.
- **Der Endnutzer.** Wägt Geschwindigkeit gegen Aktualität ab, Anfrage für Anfrage.

Die Einstellungen des Betreibers sind die Untergrenze. Eine Anfrage kann darüber hinausgehen, hin zu älteren Daten oder weniger Last; das Opt-in in den Antwort-Cache ist das übliche Beispiel. Darunter kann sie nie gehen. Ein Anfrage-Hinweis, der das täte, wird mit einem Fehler abgelehnt, der die Betreibereinstellung nennt, und weder stillschweigend angewendet noch stillschweigend verworfen.

| Anfrage-Eingabe | Was sie darf | Abgelehnt, wenn |
| --- | --- | --- |
| `@route(engine: DIRECT)`, `-- @provisa route=direct` | Den direkten Treiber für eine Quelle wählen | eine Tabelle, die die Anweisung liest, `load_protected` oder `replicate: 0` ist |
| `@join`, `@reorder`, `@broadcastSize`, `/*+ ... */` | Eine Engine-Session-Eigenschaft setzen, die die `federation_hints` der Quelle offen ließen | sie eine vom Betreiber gesetzte Eigenschaft ändert |
| `X-Provisa-Redirect-Threshold`, `@redirect(threshold:)` | Ein Ergebnis früher umleiten | der Wert über dem Schwellenwert des Betreibers liegt |
| `@sink(broker:)`, Broker in `X-Provisa-Sink` | Den Broker des Betreibers wiederholen | ein anderer Broker genannt wird oder `KAFKA_BOOTSTRAP_SERVERS` fehlt |
| `@watermark` in einer Subscription | Das registrierte Watermark der Tabelle wiederholen | eine andere Spalte genannt wird oder die Tabelle keines hat |
| `@cached`, `-- @provisa cache=true` | Ein älteres Ergebnis aus dem Cache liefern | nie; ein Cache-Eintrag ist nie aktueller als der Lesevorgang, der ihn gespeichert hat |

Ablehnungen liefern HTTP 403 mit dem Code `query.operator_floor`, SQLSTATE `42501` über pgwire und `PERMISSION_DENIED` über Flight und gRPC.

Eine Tabelle mit `load_protected` oder `replicate: 0` wird von keiner Abfrage über irgendeinen Transport live gelesen. Abfragen lesen ihr Replikat. Nur die Replikation und ihre Refreshes lesen die Quelle: Ein Refresh läuft, wenn `cache_ttl` oder eine Aktualitätsprüfung es verlangt; ohne beides wird das Replikat einmal aufgebaut und danach nur über einen Change-Feed oder den Scheduler aktualisiert. Hat eine solche Tabelle ein `off_peak_window` (an der Tabelle oder ihrer Quelle, gelesen in `off_peak_tz`), läuft eine Aktualisierung nur, solange das Fenster offen ist: Eine, die außerhalb fällig wird oder angefordert wird, wartet auf die nächste Öffnung des Fensters, und ein Fenster ohne `cache_ttl` aktualisiert einmal pro Öffnung. Der erste Aufbau wird nie zurückgehalten, weil Lesevorgänge der Tabelle auf ihn warten.

### Die Einstellung `replicate` {: #the-replicate-setting }

`replicate` ist eine Ganzzahl an einer Tabelle. Eine Tabelle ohne Wert übernimmt den ihrer Quelle; ist keiner von beiden gesetzt, gilt der globale Schwellenwert. Ein Replikat ist eine Kopie einer Quelltabelle, die im Store der Engine liegt und nach den Aktualitätseinstellungen der Tabelle aktualisiert wird; es ist kein kontinuierlicher Änderungsstrom.

| Wert | Name | Bedeutung |
| --- | --- | --- |
| nicht gesetzt | Standard | Der globale Schwellenwert gilt |
| `-1` | Nie | Live gelesen, wo immer ein Live-Pfad existiert. Nur repliziert auf einer Engine, die die Quelle nicht an Ort und Stelle lesen kann |
| `N` über 0 | Hot-N | Live gelesen, bis die Tabelle N governte Anweisungen pro Intervall überschreitet; danach aus ihrem Replikat bedient |
| `0` | Immer | Lesezugriffe kommen aus dem Replikat |

Nur „Immer“ ist eine Garantie. „Nie“ und die Hot-Werte sind Best Effort. `load_protected` mit `replicate: -1` wird beim Laden und beim Speichern der Konfiguration abgelehnt. Eine Quelle mit `replicate: 0` hat auf keiner Engine ein Live-Attach, daher wird jede ihrer Tabellen aus ihrem Replikat bedient.

Der Floor wird anhand der Tabellen beurteilt, die eine Anweisung liest. Eine Anweisung, die keine aus einem Replikat bediente Tabelle liest, behält ihre direkte Route, auch wenn die Quelle andere replizierte Tabellen hat.

Ein Replikat wird auf seinem deklarierten Primärschlüssel und auf den Spalten indiziert, über die seine deklarierten Beziehungen joinen; die Indizes werden nach dem Landen aufgebaut. Wo diese Indizes Joins beschleunigen, wo Replikate abzulegen sind und wie ihre Aktualität zu wählen ist, steht unter [Replikate](replicas.md).

### Hot-Replikation {: #hot-replication }

Eine Tabelle, deren `replicate` nicht gesetzt ist oder `N` über 0 beträgt, wird live gelesen, bis sie stark beansprucht wird.

```yaml
replication:
  hot_threshold: 100       # statements per interval for a table left at Default
  hot_interval: 60         # seconds: the window the count is taken over, and how often it is judged
  hot_max_rows: 10000000   # a larger table is not replicated for being busy
```

| Einstellung | Umgebungsvariable | Standard |
| --- | --- | --- |
| `replication.hot_threshold` | `PROVISA_REPLICATION_HOT_THRESHOLD` | 100 |
| `replication.hot_interval` | `PROVISA_REPLICATION_HOT_INTERVAL` | 60 |
| `replication.hot_max_rows` | `PROVISA_REPLICATION_HOT_MAX_ROWS` | 10000000 |

Alle drei sind Betreibereinstellungen: Die Admin-UI zeigt sie unter „Hot Tables“, und ein gespeicherter Wert gilt ab der nächsten Auswertung.

So wird eine Tabelle gezählt:

- Eine governte Anweisung zählt jede Tabelle, die sie liest, einmal, egal wie oft sie sie liest.
- Eine Anweisung, die aus dem Antwort-Cache beantwortet wird, zählt nicht. Sie hat die Quelle nicht belastet.
- Eine Anweisung, die die Governance abgelehnt hat, zählt nicht.

Eine Tabelle wird befördert, wenn ihr Zähler ihren Schwellenwert erreicht (ihr eigenes `N`, sonst das ihrer Quelle, sonst `hot_threshold`), und zurückgestuft, wenn der Zähler unter die Hälfte davon fällt. Die Beförderung fordert einen Replikat-Aufbau an; die Tabelle wird live gelesen, bis dieser Aufbau abgeschlossen ist, danach aus dem Replikat. Die Rückstufung lenkt Lesezugriffe sofort wieder auf die Quelle.

Eine Tabelle wird nicht befördert, wenn sie mehr Zeilen enthält als `hot_max_rows`, wenn ihr Änderungssignal `ttl` oder `ttl_probe` ist und weder sie noch ihre Quelle ein `cache_ttl` deklariert, oder wenn die Redis-Hot-Stufe sie bereits verwaltet. Die Zusammenfassung der Tabelle in der Admin-UI nennt, welcher Fall zutrifft.

Die Zähler liegen in Redis. Mit einem gemeinsamen Redis (`cache.redis_url`) umfassen sie jeden Prozess des Deployments. Mit dem eingebetteten Redis gelten sie pro Prozess, daher läuft die Beförderung nur, wenn ein einziger Worker Anfragen bedient; bei mehreren Workern und ohne gemeinsames Redis wird nichts befördert, und die Zusammenfassung weist darauf hin.

## Authentifizierung {: #authentication }

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

### Auth-Provider-Typen {: #auth-provider-types }

| Provider | Anwendungsfall | Token-Validierung |
| ---------- | ---------- | ----------------- |
| `simple` | Lokale Entwicklung/Tests. Nutzer in YAML definiert. | JWT signiert mit `PROVISA_JWT_SECRET` |
| `firebase` | Firebase Authentication (alle Methoden). | `firebase-admin`-SDK `verify_id_token()` |
| `keycloak` | Keycloak-OIDC. Mandant + Client-Rollen zugeordnet. | JWKS-basierte JWT-Validierung |
| `oauth` | Generisches OIDC (Okta, Azure AD, Auth0, PingFederate). | JWKS von der Discovery-URL |
| `basic` | Eigenständige Deployments. Konten liegen in Provisas eigenem Speicher. | bcrypt-Passwort oder SCRAM-SHA-256 auf pgwire |

Superuser-Anmeldedaten (Block `superuser`) funktionieren mit jedem Provider und lösen immer zur Admin-Rolle mit allen Capabilities auf. Wird für die Ersteinrichtung verwendet, bevor externe Auth konfiguriert ist.

### SCRAM-SHA-256 (`auth.scram`)

```yaml
auth:
  provider: basic
  scram: true
```

Lässt pgwire SASL mit `SCRAM-SHA-256` anbieten, sodass ein Passwort nachgewiesen und nie im Klartext gesendet wird. Es gilt nur für den Provider `basic`, weil kein anderer Provider die RFC-5802-Verifier hält, die SCRAM braucht, und Channel Binding wird nicht angeboten.

Verifier lassen sich nicht aus vorhandenen bcrypt-Hashes ableiten. Einer wird immer dann geschrieben, wenn ein Passwort im Klartext durchläuft, sodass die erste SCRAM-Verbindung jedes Nutzers auf dessen nächste Registrierung, Anmeldung, Passwortänderung oder Admin-Zurücksetzung folgt. Bis dahin fallen die Verbindungen dieses Nutzers auf den Klartext-Austausch über TLS zurück; die Leitung verrät nicht, wer bereits migriert ist.

### Anmelde-Drosselung (`auth.login_throttle`) {: #login-throttling-authlogin_throttle }

```yaml
auth:
  login_throttle:
    max_attempts: 5      # failures within the window before lockout
    window_seconds: 300  # how far back failures are counted
    lockout_seconds: 900 # how long a locked-out subject is refused
```

Standardmäßig aktiv mit den gezeigten Werten; der Block justiert sie lediglich. Der Zähler sitzt auf der Ebene der Anmeldedatenprüfung, sodass Fehlversuche über HTTP, pgwire und Bolt sich auf dasselbe Subjekt summieren und eine Sperre auf jeder Schnittstelle greift. Er gilt pro Prozess: mehrere API-Worker erlauben jeweils bis zu `max_attempts`.

### Persönliche Zugriffstoken {: #personal-access-tokens }

PATs brauchen keinen Konfigurationsblock. Sie werden immer angenommen, und ihr Speicher wird mit dem übrigen Schema der Steuerungsebene angelegt. Einstellen lässt sich die Laufzeit, die ein Benutzer beim Ausstellen verlangen darf: 1 bis 366 Tage oder keine, für ein Token ohne Ablauf. Siehe [Sicherheitsmodell](security.md#personal-access-tokens).

### Gegenseitiges TLS {: #mutual-tls }

Die Prüfung von Client-Zertifikaten wird über Umgebungsvariablen eingestellt, neben den TLS-Zertifikatseinstellungen, die sie erweitert, und hat keinen Platz in `provisa.yaml`.

| Variable | Standard | Bedeutung |
| ---------- | --------- | --------- |
| `PROVISA_MTLS_CLIENT_CA` | nicht gesetzt | PEM-Bundle der CA(s), die Client-Zertifikate signieren dürfen. Setzen aktiviert die Client-Zertifikatsprüfung |
| `PROVISA_MTLS_MODE` | `required`, sobald eine CA gesetzt ist | `required` oder `optional` |
| `PROVISA_MTLS_BIND_PRINCIPAL` | `false` | Verlangt, dass der Common Name des Zertifikats dem Benutzernamen entspricht, mit dem sich die Verbindung authentifiziert |

Jede nimmt eine protokollspezifische Überschreibung an, benannt wie die TLS-Einstellungen. Ein Modus ohne CA oder ein Modus, der weder `required` noch `optional` ist, verhindert den Start, sodass der Server nie Verbindungen bedient, die der Betreiber für geprüft hält, obwohl sie es nicht sind.

### Eine Org über TLS adressieren {: #addressing-an-org-over-tls }

Nichts zu konfigurieren. In einem Multi-Org-Deployment lesen pgwire und Bolt die Org aus dem Hostnamen, den der Client gewählt hat, übertragen im TLS ClientHello, genau so, wie HTTP sie aus dem `Host`-Header liest. Ein Client, der sich mit `acme.provisa.dev` verbindet, fordert die Org `acme` an; die Anfrage wird abgelehnt, sofern das authentifizierte Principal kein Mitglied ist. Eine Verbindung über die IP-Adresse fordert keine Org an. Das ist jede Verbindung in einem Single-Org-Deployment.

### Vollständiges Auth-Konfigurationsbeispiel (auskommentiert) {: #full-auth-config-example-commented-out }

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

## Upsert-Mutationen {: #upsert-mutations }

Für Tabellen mit Primärschlüssel erzeugt Provisa Mutationsfelder `upsert_<table>`. Sie werden zu einem Upsert im Zieldialekt kompiliert: `INSERT ... ON CONFLICT (pk) DO UPDATE SET ...` auf PostgreSQL, `ON DUPLICATE KEY UPDATE` auf MySQL.

```graphql
mutation {
  upsert_orders(objects: [{id: 1, amount: 150.00, region: "us"}]) {
    affected_rows
  }
}
```

Konfliktspalten werden aus den PK-Metadaten abgeleitet. Alle Regeln zur Spaltensichtbarkeit und Schreibberechtigung gelten.

## Distinct On

Das Argument `distinct_on` wählt die erste Zeile für jeden eindeutigen Wert der angegebenen Spalten aus. Verfügbar auf Root-Abfragefeldern.

```graphql
{
  orders(distinct_on: [region], order_by: [{region: asc, created_at: desc}]) {
    region
    amount
    created_at
  }
}
```

Kompiliert zu `SELECT DISTINCT ON (region) ...` in PostgreSQL. Für Nicht-PG-Dialekte wird ein Fensterfunktions-Fallback verwendet.

## Presets der Schreibberechtigung {: #write-grant-presets }

Eine Schreibberechtigung setzt Spalten bei Insert und Update selbst. Presets werden in der Liste `presets` der Berechtigung deklariert, je Rolle und Operation.

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

| Quelle | Verhalten |
| -------- | ---------- |
| `session` | Fügt den Wert der benannten Sitzungsvariable (`name`) der Anfrage ein |
| `now` | Fügt den Zeitpunkt des Schreibvorgangs ein |
| `literal` | Fügt eine Konstante (`value`) ein |

Presets werden bei der Kompilierung vor der SQL-Generierung zur Anweisung hinzugefügt. Die Anweisung darf eine Preset-Spalte nicht liefern, und eine Preset-Spalte steht nicht im Mutations-Eingabetyp.

## Vererbte Rollen {: #inherited-roles }

Rollen können über `parent_role_id` von einer übergeordneten Rolle erben. Die Kette wird beim Start flach aufgelöst. Ein Kind hält die Vereinigung der Capabilities und des Domänenzugriffs seiner Vorfahren; eine Spalte, Metrik, Funktion oder ein Webhook, die einem Vorfahren freigegeben sind, sind dem Kind freigegeben; und die RLS-Regeln eines Vorfahren gelten für das Kind pro Tabelle, die nächstgelegene Rolle zuerst, wobei die eigene Regel des Kindes für eine Tabelle die des Elternteils ersetzt.

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

Mehrstufige Vererbung wird unterstützt. Die expliziten Capabilities und der domain_access der untergeordneten Rolle werden mit denen der übergeordneten Rolle zusammengeführt. Eine übergeordnete Rolle muss eine bestehende Rolle sein, darf nicht die Rolle selbst sein und keinen Zyklus schließen; jeder dieser Fälle wird beim Speichern abgelehnt.

## Geplante Trigger {: #scheduled-triggers }

Cron-basierte Trigger, die nach Zeitplan eine Webhook-URL aufrufen. Nutzt APScheduler.

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

Geplante Aufgaben werden über die Admin-UI (Umschalter zum Aktivieren/Deaktivieren) oder die Admin-Mutation `toggle_scheduled_task` verwaltet.

Ein Trigger kann statt nach Zeitplan bei einer registrierten Tabelle auslösen: `on: {table, event}` nennt die Tabelle als `source_id.schema.table` und das Ereignis, `change`, `error` oder `quarantine`. Ein `command`-Trigger führt einen registrierten Befehl als seine `role` aus, über die eigene gesteuerte Ausführung des Befehls, und übergibt ihm als Argumente, was ihn ausgelöst hat. Eine Änderung übergibt `table` und, wo das Änderungssignal ihn mitführt (Kafka, Debezium), den geladenen Stapel als `changes`. Ein `error`- oder `quarantine`-Ereignis übergibt `table`, `event`, `reason` und `error`. Ein Trigger, dessen Befehl ein Argument deklariert, das der Trigger nicht übergibt, wird beim Speichern abgelehnt. Eine Änderung löst einmal pro Schub aus, den ihr Änderungssignal meldet, oder sobald ein über Provisa geschriebener Schreibvorgang ankommt; eine aus ihrem Replikat bediente Tabelle löst aus, wenn ein Aufbau Zeilen lädt, die sich vom letzten unterscheiden. Der eigene Schreibvorgang eines Befehls ist eine Änderung, daher verketten sich Trigger. Jeder Lauf wird mit Zeit, Ergebnis (`succeeded`, `refused` oder `failed`), Fehler und Auslöser aufgezeichnet, und die Admin-Abfrage `scheduledTasks` listet ihn. Nur die Trigger von prod lösen aus, und in einer Quelldatenbank wird nichts installiert.

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

## OrderBy-Format

OrderBy verwendet das Format `{column: direction}` mit einem 6-wertigen Richtungs-Enum:

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

Beziehungssortierung wird über verschachtelte Objekte unterstützt:

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

### Telemetriefilter {: #telemetry-filters }

Provisa betreibt zwei unabhängige OTLP-Exportwege: Ihren internen Collector und den optionalen Support-Endpunkt von Provisa. Jeder Weg hat seinen eigenen Filter. Die Filter laufen, bevor Spans den Prozess verlassen, und die ursprünglichen Span-Objekte werden nie verändert.

**`telemetry_filter`** — steuert, was Ihren internen Collector erreicht.

| Schlüssel | Typ | Standard | Beschreibung |
| ----- | ------ | --------- | -------------- |
| `redact_sql_literals` | bool | `false` | Ersetzt String- und numerische Literale in `db.statement` durch `?` |
| `redact_attributes` | list[str] | `[]` | Attributschlüssel, die vollständig aus jedem Span entfernt werden |

**`support_telemetry_filter`** — steuert, was den Provisa-Support-Endpunkt erreicht. Die SQL-Literal-Schwärzung ist auf diesem Pfad standardmäßig `true`, da Abfragedaten Ihnen gehören.

| Schlüssel | Typ | Standard | Beschreibung |
| ----- | ------ | --------- | -------------- |
| `redact_sql_literals` | bool | `true` | Ersetzt String- und numerische Literale in `db.statement` durch `?` |
| `redact_attributes` | list[str] | `[]` | Attributschlüssel, die vollständig aus jedem Span entfernt werden |

Ein geschwärztes `db.statement`. Mit `redact_sql_literals: true` wird dieses Span-Attribut:

```yaml
db.statement: SELECT * FROM orders WHERE region = 'us-west' AND amount > 500
```

zu:

```yaml
db.statement: SELECT * FROM orders WHERE region = ? AND amount > ?
```

### Support-Endpunkt {: #support-endpoint }

`support_endpoint` (oder die Umgebungsvariable `PROVISA_SUPPORT_OTLP_ENDPOINT`) leitet Telemetrie zur Diagnose an den Provisa-Support weiter. Ist es nicht gesetzt, verlassen auf diesem Weg keine Daten Ihre Infrastruktur. Der Support-Filter arbeitet unabhängig vom internen, sodass Sie SQL-Literale aus beiden Exporten entfernen und dem Support trotzdem Span-Zeiten und Fehlerdaten geben können.

### Endpunkt-Protokollerkennung {: #endpoint-protocol-detection }

Provisa wählt OTLP/HTTP oder OTLP/gRPC anhand des URL-Schemas des Endpunkts. URLs, die mit `http://` oder `https://` beginnen, verwenden OTLP/HTTP, wobei `/v1/traces`, `/v1/metrics` und `/v1/logs` automatisch angehängt werden. Jedes andere Schema verwendet OTLP/gRPC mit `insecure=True`.

## Föderations-Engine {: #federation-engine }

Eine Federation-Engine zu konfigurieren ist optional. Standard ist `duckdb`: ohne Konfiguration, im Prozess, ohne externen Dienst. Wählen Sie eine andere Engine, wenn Sie MPP-Skalierung brauchen oder ein vorhandenes Warehouse weiterverwenden wollen.

Vorrang: `PROVISA_ENGINE`-Umgebungsvariable → persistiertes Admin-UI-Konfigurationsfeld `federation_engine` → `duckdb`. Änderungen werden beim Neustart des Dienstes wirksam.

### Engine-Übersicht {: #engine-overview }

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
| `mysql` | MySQL | Pro Dialekt | Nein | Keiner (nur Landing) | Anmeldedaten pro Dialekt |
| `mariadb` | MariaDB | Pro Dialekt | Nein | Keiner (nur Landing) | Anmeldedaten pro Dialekt |
| `oracle` | Oracle Database | Pro Dialekt | Nein | Keiner (nur Landing) | Anmeldedaten pro Dialekt |
| `mssql` | Microsoft SQL Server | Pro Dialekt | Nein | Keiner (nur Landing) | Anmeldedaten pro Dialekt |
| `db2` | IBM Db2 | Pro Dialekt | Nein | Keiner (nur Landing) | Anmeldedaten pro Dialekt |
| `redshift` | Amazon Redshift | Pro Dialekt | Nein | Keiner (nur Landing) | Anmeldedaten pro Dialekt |
| `greenplum` | Greenplum | Pro Dialekt | Nein | Keiner (nur Landing) | Anmeldedaten pro Dialekt |
| `cockroachdb` | CockroachDB | Pro Dialekt | Nein | Keiner (nur Landing) | Anmeldedaten pro Dialekt |
| `yugabytedb` | YugabyteDB | Pro Dialekt | Nein | Keiner (nur Landing) | Anmeldedaten pro Dialekt |
| `opengauss` | openGauss | Pro Dialekt | Nein | Keiner (nur Landing) | Anmeldedaten pro Dialekt |
| `tidb` | TiDB | Pro Dialekt | Nein | Keiner (nur Landing) | Anmeldedaten pro Dialekt |
| `singlestore` | SingleStore | Pro Dialekt | Nein | Keiner (nur Landing) | Anmeldedaten pro Dialekt |
| `vertica` | Vertica | Pro Dialekt | Nein | Keiner (nur Landing) | Anmeldedaten pro Dialekt |
| `exasol` | Exasol | Pro Dialekt | Nein | Keiner (nur Landing) | Anmeldedaten pro Dialekt |
| `teradata` | Teradata Vantage | Pro Dialekt | Nein | Keiner (nur Landing) | Anmeldedaten pro Dialekt |
| `saphana` | SAP HANA | Pro Dialekt | Nein | Keiner (nur Landing) | Anmeldedaten pro Dialekt |
| `sapase` | SAP ASE (Sybase) | Pro Dialekt | Nein | Keiner (nur Landing) | Anmeldedaten pro Dialekt |
| `sqlanywhere` | SAP SQL Anywhere | Pro Dialekt | Nein | Keiner (nur Landing) | Anmeldedaten pro Dialekt |
| `monetdb` | MonetDB | Pro Dialekt | Nein | Keiner (nur Landing) | Anmeldedaten pro Dialekt |
| `firebird` | Firebird | Pro Dialekt | Nein | Keiner (nur Landing) | Anmeldedaten pro Dialekt |
| `sqlalchemy` | Andere relationale Datenbank (per Connection-URL) | Pro Dialekt | Nein | Keiner (nur Landing) | Anmeldedaten pro Dialekt |

### Engine-Referenz {: #engine-reference }

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

In-Process; kein externer Dienst. Die Standard-Engine. `PROVISA_DATA_DIR` steuert, wo der eingebettete Store liegt (`~/.provisa` standardmäßig).

```bash
PROVISA_ENGINE=duckdb   # or omit — this is the default
```

Der Materialisierungs-Store ist standardmäßig `~/.provisa/materialize.duckdb`. Es ist die einzige Engine, deren Standard-Store nicht PostgreSQL ist.

#### clickhouse (eingebettet) / clickhouse-server {: #clickhouse-embedded-clickhouse-server }

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

#### Relationale-Datenbank-Engines (mysql, mariadb, oracle, mssql, db2, redshift, greenplum, cockroachdb, yugabytedb, opengauss, tidb, singlestore, vertica, exasol, teradata, saphana, sapase, sqlanywhere, monetdb, firebird) und `sqlalchemy` {: #relational-database-engines-mysql-mariadb-oracle-mssql-db2-redshift-greenplum-cockroachdb-yugabytedb-opengauss-tidb-singlestore-vertica-exasol-teradata-saphana-sapase-sqlanywhere-monetdb-firebird-and-sqlalchemy }

Ein Schlüssel pro netzwerkadressierbarer relationaler Datenbank, alle auf derselben Nur-Landing-Laufzeit (keine Föderation zu externen Quellen): Jede Quelle landet im Store und wird dort abgefragt. Der Schlüssel wählt die Datenbank; `PROVISA_ENGINE_URL` trägt den DSN, den ihr Dialekt erwartet. `sqlalchemy` ist der Catch-all für eine Datenbank ohne eigenen Schlüssel. Dateibasierte Stores (SQLite, Access) werden nicht angeboten, denn der Server muss über das Netzwerk erreichbar sein.

```bash
PROVISA_ENGINE=mysql
PROVISA_ENGINE_URL="mysql+pymysql://user:pass@host:3306/db"
```

Der Materialisierungs-Store ist standardmäßig `TENANT_DATABASE_URL`.

### Materialisierungs-Store {: #materialization-store }

Wenn eine Quelle nicht live angehängt werden kann (kein ATTACH-Connector für die gewählte Engine), landet sie im Materialisierungs-Store der Engine. Auflösungsreihenfolge: explizites `PROVISA_MATERIALIZE_URL` → deklarierter Standard der Engine → harter Fehler (kein stiller Fallback).

DuckDB deklariert seine eingebettete Datei (`~/.provisa/materialize.duckdb`) als Standard. Alle anderen Engines nutzen standardmäßig `TENANT_DATABASE_URL` (PostgreSQL). Überschreiben Sie jede Engine mit `PROVISA_MATERIALIZE_URL`.

Der Store bestimmt, was die Indizes eines Replikats für Joins bewirken: Ein PostgreSQL-Store indiziert Replikate, und eine Engine, die SQL darin ausführt, liest diese Indizes, während DuckDB die Replikate seiner eingebetteten Datei über eine Kopie pro Anweisung joint. Siehe [Replikate](replicas.md#use-a-relational-database-as-the-replica-store-where-the-engine-runs-sql-in-it).

### Pro-Quelle-Föderations-Hints {: #per-source-federation-hints }

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

Für Google-Cloud-Quellen setzen Sie `GOOGLE_APPLICATION_CREDENTIALS` auf den Pfad Ihrer Dienstkonto-Schlüsseldatei. Für Fabric und Synapse melden Sie sich mit `az login` (Entwicklung) oder einer verwalteten Identität (Produktion) an; die Engine liest die Anmeldedaten über `DefaultAzureCredential` aus `azure-identity`.

## Umgebungsvariablen {: #environment-variables }

| Variable | Standard | Beschreibung |
| ---------- | --------- | -------------- |
| `PROVISA_CONFIG` | `config/provisa.yaml` | Pfad zur Konfigurationsdatei |
| `TENANT_DATABASE_URL` | `postgresql+psycopg://provisa:provisa@localhost:5432/provisa` | Control-Plane-Store-URI (SQLAlchemy async); akzeptiert `sqlite+pysqlite://…` / `duckdb://…` für den eingebetteten Desktop-Store |
| `PLATFORM_DATABASE_URL` | — | Plattform-Registry-URI (Mandantenverzeichnis, Engine-Registry); beim Start erforderlich, kein Fallback |
| `PROVISA_REDIS_EMBEDDED` | — | `1`/`true` nutzt eingebettetes fakeredis statt eines Redis-Servers — kein Docker |
| `PG_HOST` | `localhost` | PostgreSQL-Host |
| `PG_PORT` | `5432` | PostgreSQL-Port |
| `PG_DATABASE` | `provisa` | PostgreSQL-Datenbank |
| `PG_USER` | `provisa` | PostgreSQL-Benutzer |
| `PG_PASSWORD` | `provisa` | PostgreSQL-Passwort |
| `PROVISA_ENGINE` | `duckdb` | Föderations-Engine-Schlüssel |
| `PROVISA_ENGINE_URL` | — | Verbindungs-URL für URL-gesteuerte Engines (Snowflake, Databricks, ClickHouse Server, BigQuery, SQLAlchemy) |
| `PROVISA_MATERIALIZE_URL` | — | Materialisierungs-Store-DSN überschreiben (Standard ist der deklarierte Standard der Engine) |
| `PROVISA_DATA_DIR` | `~/.provisa` | Datenverzeichnis für den eingebetteten DuckDB-Store |
| `TRINO_HOST` | `localhost` | Trino-Coordinator-Host |
| `TRINO_PORT` | `8080` | Trino-Coordinator-HTTP-Port |
| `PROVISA_ENGINE_FILES_GID` | `1000` | Gruppe, als die Trino seine geheimen Dateien liest (die SASL-Einstellungen einer Kafka-Quelle, den Client-Schlüssel eines Code-Hosts), wenn Provisa weder als root noch als Benutzer von Trino läuft, wie eine native App neben dem Container von Trino. Der Benutzer von Provisa muss ihr angehören, sonst wird die Datei mit Namen abgelehnt. |
| `PROVISA_PG_ENGINE_FILES_GID` | `999` | Dasselbe für den Server einer Postgres-Engine, der den Client-Schlüssel eines Code-Hosts liest |
| `GOOGLE_APPLICATION_CREDENTIALS` | — | Pfad zur GCP-Service-Account-Schlüssel-JSON (BigQuery-Engine/-Quelle) |
| `GOOGLE_CLOUD_PROJECT` | — | Standard-GCP-Projekt (BigQuery; wird von der URL überschrieben) |
| `FABRIC_SQL_SERVER` | — | Fabric-Warehouse-SQL-Endpunkt (Alternative zu `PROVISA_ENGINE_URL`) |
| `FABRIC_DATABASE` | — | Fabric-Warehouse-Datenbankname |
| `SYNAPSE_SQL_SERVER` | — | Synapse-Serverless-SQL-Endpunkt |
| `SYNAPSE_DATABASE` | — | Synapse-Datenbankname |
| `REDIS_URL` | — | Redis-Verbindungs-URL |
| `PROVISA_SAMPLE_SIZE` | `10000` | Standard-Sampling-Grenzwert |
| `PROVISA_DEFAULT_ROW_LIMIT` | `100` | Zeilenobergrenze, wenn eine Abfrage kein explizites `LIMIT` liefert |
| `PROVISA_RETRY_BUDGET_SECS` | `30` | Tier-1-Read-Retry-Budget in Sekunden; exponentielles Backoff mit vollem Jitter |
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
| `PROVISA_MTLS_CLIENT_CA` | — | PEM-Bundle der CA(s), die Client-Zertifikate signieren dürfen; Setzen aktiviert die Client-Zertifikatsprüfung auf pgwire, Bolt, gRPC und Flight |
| `PROVISA_MTLS_MODE` | `required`, sobald eine CA gesetzt ist | `required` oder `optional`; jeder andere Wert verweigert den Start |
| `PROVISA_MTLS_BIND_PRINCIPAL` | `false` | Verlangt, dass der Common Name des Zertifikats dem authentifizierenden Benutzernamen entspricht |
| `PROVISA_BOLT_ALLOWED_ORIGINS` | — | Kommagetrennte Sites, die einen Bolt-WebSocket aus einem Browser öffnen dürfen; nicht gesetzt weist jede Browser-Origin ab |
| `PROVISA_EXTRAS` | `firebase,vector` | Pyproject-Extras, die ins App-Image eingebacken werden; `scripts/provisa` leitet den Wert aus `dq_checker` in `~/.provisa/config.yaml` ab |
| `PROVISA_DQ_CHECKER` | `none` | Nur Installer: `none`/`soda`/`gx`, gelesen von `first-launch.sh` im nicht-interaktiven Modus und als `dq_checker` in `config.yaml` geschrieben |
| `ANTHROPIC_API_KEY` | — | Claude-API-Schlüssel (Discovery) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | Überschreibt `observability.endpoint` |
| `OTEL_SERVICE_NAME` | `provisa` | Überschreibt `observability.service_name` |
| `OTEL_LOG_LEVEL` | `WARNING` | Überschreibt `observability.log_level` |
| `OTEL_COMPACT_BATCH_SIZE` | `10` | Überschreibt `observability.compact_batch_size` |
| `OTEL_SPAN_EXPORT_DELAY_MILLIS` | `1000` | Flush-Verzögerung des Batch-Span-Prozessors |
| `PROVISA_SUPPORT_OTLP_ENDPOINT` | — | Überschreibt `observability.support_endpoint` |
