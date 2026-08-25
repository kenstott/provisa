# Referencia de configuración

Provisa se configura mediante un archivo YAML (por defecto: `config/provisa.yaml`). (REQ-528)

## Orígenes

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

Todos los orígenes comparten un conjunto común de campos. [tool-verified: `provisa/core/models.py:129-212`]

| Campo | Por defecto | Notas |
| ------- | --------- | ------- |
| `id` | requerido | Alfanumérico, guiones, guiones bajos |
| `type` | requerido | Ver tabla abajo |
| `host` | `""` | Nombre de host o IP |
| `port` | `0` | `0` significa que cada conector suministra su propio valor por defecto — no hay un mapa central de puertos por defecto |
| `database` | `""` | |
| `username` | `""` | |
| `password` | `""` | Admite referencias de credenciales `${env:VAR}` y `${secret:NAME}` — consulte [Secretos](secrets.md) |
| `path` | `null` | Ruta de archivo o URI para orígenes basados en archivo |
| `base_url` | `null` | URL base para orígenes de API |
| `pool_min` / `pool_max` | `1` / `5` | Límites del pool de conexiones |
| `cache_enabled` | `true` | Activa/desactiva la caché para todas las tablas de este origen |
| `cache_ttl` | `null` | Segundos; `null` hereda el valor global por defecto |
| `federation_hints` | `{}` | Parámetros extendidos por conector (dict[str,str]); ver referencia de tipos abajo. REQ-281 |
| `mapping` | `{}` | DSL de mapeo para redis, elasticsearch, prometheus. REQ-251 |
| `allowed_domains` | `[]` | Restringe este origen a IDs de dominio específicos; vacío = sin restricción |
| `description` | `""` | |

### Tipos de origen soportados [tool-verified: `provisa/core/models.py:36-101`]

| Tipo | Estilo de conexión | Notas |
| ------ | ----------------- | ------- |
| **RDBMS** | | |
| `postgresql` | host/port | Pool asyncpg; PgBouncer opcional vía `use_pgbouncer` |
| `mysql` | host/port | |
| `mariadb` | host/port | |
| `singlestore` | host/port | |
| `sqlserver` | host/port | |
| `oracle` | host/port | |
| `firebird` | host + `path` (archivo de BD) | Extensión comunitaria firebird de DuckDB (REQ-899) |
| `duckdb` | host/port | |
| `cockroachdb` | host/port | Reutiliza el driver/dialecto de PostgreSQL (REQ-950) |
| `yugabytedb` | host/port | Reutiliza el driver/dialecto de PostgreSQL (REQ-950) |
| `greenplum` | host/port | Reutiliza el driver/dialecto de PostgreSQL (REQ-950) |
| `tidb` | host/port | Reutiliza el driver/dialecto de MySQL (REQ-950) |
| **Cloud DW** | | |
| `snowflake` | host/port + `federation_hints` | `account` requerido en hints |
| `bigquery` | `federation_hints` | `project` requerido; autenticación vía `GOOGLE_APPLICATION_CREDENTIALS` |
| `databricks` | host + `federation_hints` | `http_path` requerido en hints |
| `fabric` | variables de entorno o `PROVISA_ENGINE_URL` | T-SQL sobre TDS, autenticación Azure AD |
| `synapse` | variables de entorno o `PROVISA_ENGINE_URL` | T-SQL sobre TDS, autenticación Azure AD |
| `redshift` | host/port | |
| **OLAP** | | |
| `clickhouse` | host/port + `federation_hints` | El hint `secure` activa TLS; el puerto por defecto es 8123/8443 |
| `elasticsearch` | host/port + DSL `mapping` | |
| `pinot` | host/port | Endpoint REST del controlador |
| `druid` | host/port | Endpoint Avatica del broker |
| `exasol` | host/port | |
| **Data Lake** | | |
| `delta_lake` | `path` (URI de tabla) | `delta_scan` de DuckDB; acceso a almacenamiento de objetos vía `federation_hints` |
| `iceberg` | `path` (URI de tabla) | `iceberg_scan` de DuckDB; acceso a almacenamiento de objetos vía `federation_hints` |
| `hudi` | `path` (URI de tabla) | Motor Hudi de ClickHouse, sin copia (REQ-1178) |
| `hive` | host/port (metastore) + `mapping.storage` | Backend de almacenamiento en `mapping["storage"]`: hadoop/hdfs/local/s3/azure/adls |
| `hive_s3` | host/port (metastore) + claves S3 de `mapping` | Tipo distinto; siempre almacenamiento S3 (REQ-229) |
| **NoSQL** | | |
| `mongodb` | host/port | Campos de conexión simples; sin DSL de mapeo |
| `cassandra` | host/port | Campos de conexión simples; sin DSL de mapeo |
| `redis` | host/port + DSL `mapping` | |
| **Streaming** | | |
| `kafka` | solo registro | La configuración real vive en `kafka_sources[]`; ver §Kafka abajo |
| `websocket` | host/port/path + `federation_hints` | Feed externo de WebSocket |
| `rss` | host/port/path + `federation_hints` | Feed RSS 2.0 / Atom |
| **Grafo/Semántico** | | |
| `neo4j` | [UNVERIFIED end-to-end mapping] | |
| `sparql` | [UNVERIFIED end-to-end mapping] | |
| **Archivo** | | |
| `sqlite` | `path` | Siempre enruta a través del motor (sin pool directo) |
| `csv` | `path` | |
| `parquet` | `path` | |
| `files` | `path` (directorio) | Rastreador glob; expone CSV/Parquet/XLSX/JSON como tablas |
| **API/Remoto** | | |
| `google_sheets` | `federation_hints.spreadsheet_id` | |
| `prometheus` | host/port o `mapping.url` + DSL `mapping` | |
| `graphql_remote` | `base_url` + `mapping` opcional | Encabezados, forward-client-headers, timeout en `mapping` |
| `openapi` | `base_url` | |
| `grpc_remote` | [UNVERIFIED end-to-end mapping] | |
| `airport` | `base_url` (ubicación Flight) | Extensión airport de DuckDB (REQ-899) |
| `ingest` | receptor push | Servicios externos hacen POST de eventos JSON |
| **SaaS** | | |
| `sharepoint` | `base_url` o `host` + `mapping` | Autenticación vía `mapping.auth_type` |
| `splunk` | `host`/`port` o `base_url` + `mapping` | |
| **GovData** | | |
| `govdata` | subject + `domain_id` | Modelo `GovDataSource` separado; ver §GovData abajo |
| **Calidad de datos** | | |
| `soda` | host/port apuntando al pgwire de Provisa | Necesita el extra `soda`; Elastic License 2.0, solo autoalojado (REQ-1443) |
| `great_expectations` | host/port apuntando al pgwire de Provisa | Necesita el extra `gx`; Apache 2.0 (REQ-1443) |

### Referencia de tipos de origen

Los tipos que necesitan configuración no obvia tienen una entrada breve a continuación. Los tipos RDBMS (postgresql, mysql, etc.) usan solo los campos comunes de arriba — no necesitan sección adicional.

#### GovData [tool-verified: `provisa/core/models.py:953-983`]

Los orígenes `govdata` usan un modelo de nivel superior separado, `GovDataSource`, no el `Source` genérico. (REQ-540) El acceso se particiona por agrupación de subject.

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

Cada subject se asigna a uno o más esquemas de GovData. Configurar un origen `govdata` con un subject expone automáticamente todos los esquemas de ese subject. (REQ-540)

| Subject | Esquemas |
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

Los esquemas `ref` y `geo` siempre se incluyen como esquemas de enlace — no son configurables y no se listan arriba. (REQ-541) Use el subject `ALL` para conceder acceso a todos los esquemas. [tool-verified: `provisa/core/models.py:961-963`]

#### Kafka [tool-verified: `provisa/federation/trino_connectors.py:497-502`, `provisa/api/app_loaders.py:113-118`]

La fila `kafka` en `sources:` es solo de registro. El `details()` de su conector devuelve `{}` — la configuración real vive en el bloque de nivel superior `kafka_sources[]`, no en una fila de `sources:`. Kafka siempre es un VIRTUAL_SOURCE (enruta a través del motor; sin pool directo). [tool-verified: `provisa/transpiler/router.py:44-63`]

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

**Ventana de tiempo** — `default_window` acota cada consulta a un período reciente, evitando lecturas no acotadas sobre topics de alto volumen. (REQ-148) Formato: `1h`, `30m`, `7d`, `60s`. Por defecto `1h`. Se inyecta automáticamente como `WHERE _timestamp >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR`. Los clientes pueden anularlo con su propio filtro `_timestamp` en el argumento `where` de GraphQL.

**Discriminador** — Varias configuraciones de topic pueden apuntar al mismo topic físico de Kafka con distintos valores de `discriminator`, produciendo tipos GraphQL separados. (REQ-149) El discriminador se inyecta automáticamente como cláusula WHERE.

**Origen del esquema**

| Valor | Comportamiento |
| ------- | ---------- |
| `registry` | Obtiene el esquema desde el Confluent Schema Registry |
| `manual` | Define las columnas en línea en la configuración (no necesita Schema Registry) |
| `sample` | Descubre automáticamente a partir de mensajes de muestra |

#### Snowflake [tool-verified: `provisa/executor/drivers/snowflake.py:48-62`]

`account` en `federation_hints` es requerido. `warehouse`, `role` y `schema` son opcionales.

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

`http_path` en `federation_hints` es requerido. `password` lleva el token de acceso personal. `catalog` es opcional (se transporta en SQL/hints, no en el campo `database`).

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

`project` en `federation_hints` es requerido. La autenticación usa `GOOGLE_APPLICATION_CREDENTIALS` (ruta a un archivo de clave de cuenta de servicio) o las Credenciales Predeterminadas de la Aplicación en el entorno del motor.

```yaml
sources:
  - id: my-bigquery
    type: bigquery
    federation_hints:
      project: my-gcp-project     # required
```

#### Fabric / Synapse [tool-verified: `provisa/core/models.py:56-57`]

Ambos usan T-SQL sobre TDS con autenticación Azure AD. Autentíquese con `az login` (desarrollo) o una identidad administrada (producción) — el motor lee las credenciales mediante `DefaultAzureCredential` de `azure-identity`. Los detalles de conexión provienen de variables de entorno: `FABRIC_SQL_SERVER` / `FABRIC_DATABASE` (Fabric) o `SYNAPSE_SQL_SERVER` / `SYNAPSE_DATABASE` (Synapse), o mediante `PROVISA_ENGINE_URL`.

```yaml
sources:
  - id: my-fabric
    type: fabric
    # host/database read from FABRIC_SQL_SERVER / FABRIC_DATABASE when not set here
```

#### ClickHouse [tool-verified: `provisa/executor/drivers/clickhouse.py:49-59`]

`secure` en `federation_hints` habilita TLS en la interfaz HTTP. El puerto por defecto es `8123` (plano) u `8443` (cuando `secure: "true"`). `schema` en `federation_hints` anula el esquema remoto. [tool-verified: `provisa/federation/connector_duckdb.py:378-379`]

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

`path` es la URI de la tabla (S3, GCS, ADLS o local). El acceso al almacenamiento de objetos necesita credenciales en `federation_hints`. Para Cloudflare R2, añada `account_id`.

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

`host` y `port` apuntan al metastore Thrift de Hive (puerto por defecto 9083). Para `hive`, configure `mapping["storage"]` para elegir el backend de almacenamiento de objetos. Las claves requeridas faltantes fallan de forma ruidosa — sin reserva silenciosa. [tool-verified: `provisa/federation/trino_connectors.py:328-331`]

`hive_s3` es un tipo distinto que siempre declara almacenamiento S3 (REQ-229); no necesita `mapping.storage`.

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

Valores aceptados de `mapping.storage`: `hadoop` (por defecto), `hdfs`, `local`, `s3`, `azure`, `adls`. Claves de mapeo S3: `endpoint`, `access_key_id`, `secret_access_key`, `region`, `path_style`. Claves de mapeo ADLS: `storage_account`, `access_key` o `sas_token`.

#### Redis [tool-verified: `provisa/core/trino_catalog_files.py:54-75`]

Usa el DSL de `mapping`. `mongodb` y `cassandra` usan campos de conexión simples y NO usan el DSL de mapeo.

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

`mapping.url` anula `host:port` cuando ambos están presentes.

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

`spreadsheet_id` en `federation_hints` es requerido. La autenticación usa un SECRET `gsheet` de DuckDB aprovisionado en el momento del attach.

```yaml
sources:
  - id: my-sheet
    type: google_sheets
    federation_hints:
      spreadsheet_id: 1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms
```

#### Orígenes de archivo (csv / parquet / sqlite / files)

`path` es requerido. `files` rastrea un directorio en busca de archivos CSV, Parquet, XLSX y JSON, exponiendo cada uno como una tabla. Todos los orígenes basados en archivo son VIRTUAL (enrutan a través del motor; sin pool directo). [tool-verified: `provisa/transpiler/router.py:44-48`]

```yaml
sources:
  - id: orders-csv
    type: csv
    path: /data/orders.csv

  - id: data-lake-dir
    type: files
    path: /data/lake/         # directory; each file becomes a table
```

#### Orígenes de API / remotos

**openapi** — configure `base_url` con la URL base de OpenAPI. El descubrimiento de esquema lee la especificación OpenAPI al inicio.

```yaml
sources:
  - id: payment-api
    type: openapi
    base_url: https://api.payments.example.com/v1
```

**graphql_remote** — configure `base_url`. Claves `mapping` opcionales: `headers` (dict de encabezados estáticos), `forward_client_headers` (bool), `timeout_seconds` (int). [tool-verified: `provisa/hasura_v2/mapper.py:129-152`]

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

**airport** — `base_url` es la ubicación del servidor Arrow Flight. Extensión airport de DuckDB (REQ-899). [tool-verified: `provisa/federation/connector_duckdb.py:285-288`]

```yaml
sources:
  - id: flight-source
    type: airport
    base_url: grpc://flight.internal:8815
```

**websocket / rss** — use `host`, `port`, `path` y `federation_hints`. [tool-verified: `provisa/api/data/subscribe.py:85-129`]

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

#### Verificadores de calidad de datos (soda / great_expectations)

[tool-verified: `provisa/dq/registration.py`, `provisa/events/source_loader.py` `make_dq_loader`]

Un origen verificador apunta al propio endpoint pgwire de Provisa, de modo que un driver de postgres explora la vista federada de una tabla respaldada por Snowflake o Iceberg. La identidad del escaneo se declara, nunca se hereda — la política se aplica a esa conexión, y un conjunto de filas filtrado no debe producir un chequeo que pase silenciosamente. Las claves de conexión provienen de `mapping`: `host`, `port`, `database`, `user`, `password`.

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

Cada tabla de resultados lleva `dq_contract` — YAML de contrato Soda o JSON de un suite de Great Expectations, textual. Las columnas, la marca de agua y las promociones se derivan de él; ver [Verificadores de calidad de datos](sources.md#verificadores-de-calidad-de-datos-req-1443) para la derivación completa.

**Selección en tiempo de instalación.** El verificador no se enlaza de forma estática — el escaneo se ejecuta en un intérprete hijo, y la biblioteca se instala solo cuando un operador la nombra. Cada ruta de instalador (`install.sh`, `packaging/linux/first-launch.sh` y el asistente de macOS mediante `PROVISA_DQ_CHECKER`) escribe la elección en `~/.provisa/config.yaml`:

```yaml
dq_checker: none        # none | soda | gx
```

`scripts/provisa` lee esa clave y exporta `PROVISA_EXTRAS`, que `docker-compose.app.yml` pasa como argumento de build al `ARG PROVISA_EXTRAS` del `Dockerfile`: [tool-verified: `scripts/provisa:69-79`]

| `dq_checker` | `PROVISA_EXTRAS` (nivel Docker) | Instalación nativa en venv |
| -------------- | -------------------------------- | --------------------- |
| `none` | `firebase,vector` | `provisa[embedded]` |
| `soda` | `firebase,vector,soda` | `provisa[embedded,soda]` |
| `gx` | `firebase,vector,gx` | `provisa[embedded,gx]` |

Instalar el conjunto de datos de demostración eleva `none` a `gx` y lo indica, porque la configuración de demo registra un suite de Great Expectations sobre `pet_store.pets` y, de lo contrario, su tablero de calidad no tendría nada que mostrar. Nombrar `soda` mantiene `soda`.

Llegar a la demo mediante pip en lugar de un instalador omite ese paso del asistente, así que el extra `demo` lleva el mismo verificador: `pip install 'provisa[embedded,demo]'` es lo que `provisa run --demo` necesita para que su escaneo se ejecute. Sin él, el escaneo informa `data-quality checker 'great_expectations' is not installed`, nombrando el comando de instalación.

Cualquier otro valor detiene el lanzador en lugar de iniciar sin el verificador que el operador pidió. El extra `soda` trae `soda-postgres`; `gx` trae `great-expectations[postgresql]`. Soda Core está bajo Elastic License 2.0 — `config/capabilities.yaml` marca la opción como `cloud_eligible: false`, y el plano alojado la rechaza.

## Dominios

```yaml
domains:
  - id: sales-analytics
    description: Sales operational data
```

## Nomenclatura

```yaml
naming:
  convention: apollo_graphql   # snake, hasura_graphql, apollo_graphql (default)
  domain_prefix: true          # prepend domain_id__ to all GraphQL names
  rules:
    - pattern: "^prod_pg_"
      replace: ""
```

### Convención de nomenclatura

La autoridad de nomenclatura es la única fuente de verdad para los nombres orientados al cliente; los nombres físicos de columna del backend nunca se exponen a los clientes. (REQ-194) Cada lenguaje de consulta deriva el nombre de una columna a partir de su `column.alias` si está establecido, o si no, a partir del nombre físico de la columna vía su convención configurada. (REQ-194)

La convención GraphQL es uno de tres enums preestablecidos. (REQ-416) Las cadenas de forma libre antiguas (`none`, `snake_case`, `camelCase`, `PascalCase`) están obsoletas. (REQ-416)

| Preajuste | Por defecto | Nombres de tipo | Nombres de campo | Nombres de mutación |
| -------- | --------- | ------------ | ------------- | ---------------- |
| `apollo_graphql` | sí | PascalCase | camelCase | camelCase |
| `hasura_graphql` | | PascalCase | camelCase | snake_case |
| `snake` | | PascalCase | snake_case | snake_case |

La convención GraphQL por defecto es `apollo_graphql`, que produce nombres de campo y mutación en camelCase. (REQ-194, REQ-416) La convención SQL es separada, con `snake_case` por defecto, aplicada mediante `apply_sql_name()`; la convención GraphQL se aplica mediante `apply_gql_name()`, y el nombre CQL se deriva del nombre GraphQL. (REQ-194)

`domain_prefix: bool` es una opción ortogonal que se aplica sin importar el preajuste elegido. (REQ-416)

El `column.alias` explícito es el nombre canónico: SQL lo usa textualmente sin aplicar convención, GraphQL le aplica su convención, y CQL se deriva del nombre GraphQL. (REQ-194)

Anulación por origen:

```yaml
sources:
  - id: legacy-db
    naming_convention: hasura_graphql  # overrides global for this source
```

Anulación por tabla:

```yaml
tables:
  - source_id: legacy-db
    table: orders
    naming_convention: snake  # overrides source for this table
```

### Prefijo de dominio

Cuando `domain_prefix: true`, todos los nombres de campo y tipo de GraphQL se prefijan con el ID de dominio usando un separador de doble guion bajo: (REQ-154)

| Tabla | Dominio | Nombre de campo |
| ------- | -------- | ----------- |
| `orders` | `sales-analytics` | `sales_analytics__orders` |
| `customer_segments` | `customer-insights` | `customer_insights__customer_segments` |

Esto evita colisiones de nombres cuando distintos dominios tienen tablas con el mismo nombre, y hace que las consultas sean autodescriptivas.

### Reglas de nomenclatura

Reglas de expresión regular aplicadas a los nombres de tabla al generar nombres de campo GraphQL. Se aplican en orden antes de la resolución de unicidad. (REQ-542)

## Tablas

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

Los alias de tabla y columna anulan el nombre GraphQL por defecto. (REQ-155) Útiles para:

- Renombrar nombres de base de datos crípticos (p. ej., `tbl_cust_seg` → `customer_segments`)
- Evitar abreviaturas en la capa de API
- Crear un vocabulario limpio, específico del dominio

### Descripciones

Las descripciones de tabla y columna se incluyen en el SDL de GraphQL generado. (REQ-156) Aparecen en el explorador de documentación de GraphiQL y en las consultas de introspección. Configúrelas en el YAML de configuración o mediante la interfaz de administración.

### Path (extracción computada de JSON)

Las columnas pueden extraer valores de una columna de origen JSON/JSONB usando un `path` de notación de punto. (REQ-151) Es útil para datos semiestructurados en mensajes de Kafka, documentos de MongoDB o columnas JSONB de PostgreSQL.

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

El formato del path es `source_column.key1.key2...`. El compilador genera `json_extract_scalar(source_column, '$.key1.key2')` en el SQL. (REQ-151)

**Impacto en el enrutamiento:** Las columnas path usan operadores JSON de PostgreSQL (`->>`), que son soportados de forma nativa por el enrutamiento directo a PG. (REQ-152) Para orígenes que no son PostgreSQL (MySQL, SQL Server, etc.), las consultas con columnas path se enrutan automáticamente a través del motor de federación. (REQ-152) Las mutaciones no se ven afectadas ya que las columnas path son campos computados de solo lectura. (REQ-153)

### Tipos de enmascaramiento

| Tipo | Campos | Descripción |
| ------ | -------- | ------------- |
| `regex` | `pattern`, `replace` | REGEXP_REPLACE (solo columnas de tipo cadena) |
| `constant` | `value` | Reemplazo literal (NULL, 0, MAX, MIN, personalizado) |
| `truncate` | `precision` | DATE_TRUNC (solo columnas de fecha/timestamp) |

## Relaciones

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

### Automaterialización

Configure `materialize: true` en una relación para generar automáticamente una vista materializada para JOINs entre orígenes. (REQ-158) Esto evita consultas federadas costosas precalculando el resultado del JOIN.

- Solo las relaciones entre orígenes generan MVs (los JOINs del mismo origen ya son rápidos) (REQ-159)
- En una relación respaldada por una tabla de unión, la MV cubre el recorrido de dos saltos — salto de origen, salto de la tabla de unión, discriminador y las columnas propias de la tabla de unión como atributos de la arista. La tabla de unión cuenta como una pata, así que una arista es entre orígenes cuando cualquiera de las tres tablas está en un origen distinto (REQ-1586)
- La MV comienza obsoleta y se puebla mediante el bucle de actualización en segundo plano (REQ-160)
- Las mutaciones sobre cualquiera de las tablas de origen marcan la MV como obsoleta para su reactualización (REQ-543)
- `refresh_interval` por defecto es de 300 segundos (5 minutos) (REQ-543)

## Roles

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

Los roles con `parent_role_id` heredan capacidades y acceso a dominios del padre. (REQ-215) La jerarquía se aplana al inicio. (REQ-215)

### Capacidades

| Capacidad | Descripción |
| ----------- | ------------- |
| `source_registration` | Registrar orígenes de datos |
| `table_registration` | Registrar tablas |
| `relationship_registration` | Definir relaciones |
| `security_config` | Configurar RLS, enmascaramiento |
| `query_development` | Ejecutar consultas |
| `full_results` | Omitir los límites de muestreo |
| `admin` | Todas las capacidades |

## Reglas RLS

```yaml
rls_rules:
  - table_id: orders
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"
```

## Vistas materializadas

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
      # REQ-1586: add via_table with via_left_column/via_right_column (and
      # via_type_column/via_type_value when the junction is discriminated) to
      # cover a two-hop junction traversal instead of a direct join.
    target_catalog: postgresql
    target_schema: mv_cache
    refresh_interval: 300
    enabled: true
```

## Vistas (conjuntos de datos computados gobernados)

Las vistas son conjuntos de datos computados definidos en SQL con gobierno completo a nivel de columna. (REQ-133) Son el mecanismo gobernado para añadir agregaciones, transformaciones y métricas derivadas a la capa semántica. (REQ-136)

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

| Campo | Requerido | Descripción |
| ------- | ---------- | ------------- |
| `id` | Sí | Identificador único de vista |
| `sql` | Sí | Sentencia SQL SELECT que define la vista |
| `domain_id` | Sí | Dominio para la visibilidad del esquema |
| `materialize` | No | `true` = actualización CTAS periódica, `false` = vista federada en vivo |
| `refresh_interval` | No | Segundos entre actualizaciones (solo materializadas, por defecto 300) |
| `description` | No | Aparece en el SDL de GraphQL |
| `alias` | No | Anula el nombre GraphQL |
| `columns` | Sí | Definiciones de columna con visibilidad, enmascaramiento, descripciones |

### Materializada vs. en vivo

- **`materialize: true`**: Provisa crea una tabla mediante CTAS y la actualiza según un horario. (REQ-135) Consultas más rápidas, pero los datos pueden estar obsoletos hasta `refresh_interval` segundos.
- **`materialize: false`**: Provisa crea una vista federada. (REQ-135) Las consultas siempre devuelven datos en vivo, pero pueden ser más lentas para agregaciones complejas.

Las vistas pasan por el mismo pipeline de gobierno que las tablas — RLS, enmascaramiento, muestreo y visibilidad basada en roles. (REQ-134) Esto asegura que no pueda añadirse ninguna semántica nueva a la plataforma sin supervisión de un steward. (REQ-136)

### Vistas de solo consulta

Tanto las vistas `materialize: true` como `materialize: false` exponen su tipo GraphQL como de solo consulta. No se generan mutaciones de insert, upsert, update ni delete para relaciones respaldadas por `view_sql`. (REQ-1157) [tool-verified: `provisa/compiler/schema_gen.py:184`, `provisa/compiler/schema_types.py:79`]

## Caché

```yaml
cache:
  enabled: true
  redis_url: ${env:REDIS_URL}
  default_ttl: 300
```

### Jerarquía de caché

Orden de resolución de TTL (el más específico gana): **tabla** > **origen** > **valor global por defecto**. (REQ-544) Se usa el primer valor no nulo.

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

Configurar `cache_enabled: false` en un origen desactiva la caché para todas las tablas de ese origen, sin importar el TTL a nivel de tabla. (REQ-544) Las claves de caché siempre incluyen `role_id` + valores de contexto RLS para la partición de seguridad. (REQ-544)

## Autenticación

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

### Tipos de proveedor de autenticación

| Proveedor | Caso de uso | Validación de token |
| ---------- | ---------- | ----------------- |
| `simple` | Desarrollo/pruebas locales. Usuarios definidos en YAML. | JWT firmado con `PROVISA_JWT_SECRET` |
| `firebase` | Firebase Authentication (todos los métodos). | `verify_id_token()` del SDK `firebase-admin` |
| `keycloak` | Keycloak OIDC. Tenant + roles de cliente mapeados. | Validación de JWT basada en JWKS |
| `oauth` | OIDC genérico (Okta, Azure AD, Auth0, PingFederate). | JWKS desde la URL de descubrimiento |
| `basic` | Despliegues autocontenidos. Las cuentas viven en el propio almacén de Provisa. | Contraseña bcrypt, o SCRAM-SHA-256 sobre pgwire |

Las credenciales de superusuario (bloque `superuser`) funcionan con cualquier proveedor y siempre se resuelven al rol admin con todas las capacidades. (REQ-125) Se usan para la configuración inicial antes de configurar autenticación externa.

### SCRAM-SHA-256 (`auth.scram`)

```yaml
auth:
  provider: basic
  scram: true
```

Hace que pgwire anuncie SASL con `SCRAM-SHA-256`, de modo que una contraseña se demuestra en lugar de enviarse en texto claro. (REQ-1394) Se aplica solo al proveedor `basic` — ningún otro proveedor mantiene los verificadores RFC 5802 que SCRAM necesita — y no se ofrece channel binding.

Los verificadores no pueden derivarse de hashes bcrypt existentes. Se escribe uno cada vez que una contraseña pasa en texto plano, así que la primera conexión SCRAM de cada usuario sigue a su siguiente registro, inicio de sesión, cambio de contraseña o restablecimiento por un administrador. Hasta entonces, las conexiones de ese usuario recurren al intercambio en texto claro sobre TLS; el cable no revela quién ha migrado.

### Limitación de intentos de inicio de sesión (`auth.login_throttle`)

```yaml
auth:
  login_throttle:
    max_attempts: 5      # failures within the window before lockout
    window_seconds: 300  # how far back failures are counted
    lockout_seconds: 900 # how long a locked-out subject is refused
```

Activado por defecto con los valores mostrados; el bloque solo los ajusta. (REQ-1393) El contador se sitúa en la capa de validación de credenciales, así que los fallos por HTTP, pgwire y Bolt se acumulan contra el mismo sujeto y un bloqueo se mantiene en todas las superficies. Es por proceso: varios workers de API permiten cada uno hasta `max_attempts`.

### Tokens de acceso personal

Los PAT no necesitan ningún bloque de configuración — siempre se aceptan, y el almacén se crea junto con el resto del esquema del plano de control. (REQ-1263) Lo configurable es la expiración que un usuario puede solicitar al emitirlo: de 1 a 366 días, o ninguna para un token que no expira. Ver [Modelo de seguridad](security.md#tokens-de-acceso-personal).

### TLS mutuo

La verificación de certificado de cliente se configura mediante variable de entorno en lugar de en `provisa.yaml`, junto a la configuración de certificado TLS que extiende. (REQ-1228)

| Variable | Por defecto | Significado |
| ---------- | --------- | --------- |
| `PROVISA_MTLS_CLIENT_CA` | sin definir | Paquete PEM de las CA permitidas para firmar certificados de cliente. Configurarla activa la verificación de certificado de cliente |
| `PROVISA_MTLS_MODE` | `required` una vez configurada una CA | `required` u `optional` |
| `PROVISA_MTLS_BIND_PRINCIPAL` | `false` | Requiere que el nombre común del certificado sea igual al nombre de usuario con el que se autentica la conexión |

Cada una admite una anulación por protocolo bajo el mismo esquema de nombres que la configuración TLS. Un modo configurado sin una CA, o un modo que no sea ninguno de los dos valores, se niega a iniciar en lugar de servir conexiones que el operador cree verificadas.

### Dirigirse a una org sobre TLS

Nada que configurar. En un despliegue multiorg, pgwire y Bolt leen la org a partir del nombre de host al que marcó el cliente, transportado en el ClientHello de TLS, exactamente igual que HTTP lo lee del encabezado `Host`. (REQ-1234) Un cliente que se conecta a `acme.provisa.dev` solicita la org `acme`; la solicitud se rechaza a menos que el principal autenticado sea miembro. Conectarse por dirección IP no solicita ninguna org, que es el caso de toda conexión en un despliegue de una sola org.

### Ejemplo completo de configuración de autenticación (comentado)

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

## Mutaciones Upsert

Para tablas con clave primaria, Provisa autogenera campos de mutación `upsert_<table>`. (REQ-212) Estos se compilan a un upsert en el dialecto destino — `INSERT ... ON CONFLICT (pk) DO UPDATE SET ...` en PostgreSQL, `ON DUPLICATE KEY UPDATE` en MySQL. (REQ-212)

```graphql
mutation {
  upsert_orders(objects: [{id: 1, amount: 150.00, region: "us"}]) {
    affected_rows
  }
}
```

Las columnas de conflicto se derivan de los metadatos de la PK. (REQ-212) Se aplican todas las reglas de visibilidad de columna y permiso de escritura.

## Distinct On

El argumento `distinct_on` selecciona la primera fila para cada valor distinto de las columnas especificadas. (REQ-213) Disponible en los campos de consulta raíz.

```graphql
{
  orders(distinct_on: [region], order_by: [{region: asc, created_at: desc}]) {
    region
    amount
    created_at
  }
}
```

Se compila a `SELECT DISTINCT ON (region) ...` en PostgreSQL. (REQ-213) Para dialectos que no son PG, se usa una reserva mediante función de ventana. (REQ-213)

## Preajustes de columna

Inyecta automáticamente valores en columnas al insertar/actualizar. (REQ-214) Se definen por tabla en la configuración.

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

| Origen | Comportamiento |
| -------- | ---------- |
| `header` | Inyecta el valor del encabezado de solicitud HTTP nombrado |
| `now` | Inyecta `NOW()` (timestamp actual) |
| `literal` | Inyecta un valor constante |

Las columnas de preajuste se inyectan durante la compilación de la mutación, antes de la generación de SQL. (REQ-214) No son visibles en el tipo de entrada de la mutación. (REQ-214)

## Roles heredados

Los roles pueden heredar capacidades y acceso a dominios de un rol padre mediante `parent_role_id`. (REQ-215) La jerarquía se aplana al inicio. (REQ-215)

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

Se admite herencia multinivel. (REQ-215) Las capacidades y domain_access explícitos del rol hijo se combinan con los del padre. (REQ-215)

## Disparadores programados

Disparadores basados en cron que llaman a una URL de webhook según un horario. (REQ-216) Usa APScheduler. (REQ-216)

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

Las tareas programadas se gestionan mediante la interfaz de administración (activar/desactivar) o la mutación de administración `toggle_scheduled_task`. (REQ-216)

## Formato OrderBy

OrderBy usa el formato `{column: direction}` con un enum de dirección de 6 valores: (REQ-200, REQ-201)

```graphql
{
  orders(order_by: [{created_at: desc_nulls_last}, {amount: asc}]) {
    id
    created_at
    amount
  }
}
```

| Dirección | SQL |
| ----------- | ----- |
| `asc` | `ASC` |
| `desc` | `DESC` |
| `asc_nulls_first` | `ASC NULLS FIRST` |
| `asc_nulls_last` | `ASC NULLS LAST` |
| `desc_nulls_first` | `DESC NULLS FIRST` |
| `desc_nulls_last` | `DESC NULLS LAST` |

El ordenamiento por relación se admite mediante objetos anidados: (REQ-202)

```graphql
{
  orders(order_by: [{customers: {name: asc}}]) {
    id
    customers { name }
  }
}
```

## Observabilidad

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

### Filtros de telemetría [tool-verified]

Provisa ejecuta dos rutas de exportación OTLP independientes: su colector interno y el endpoint opcional de soporte de Provisa. (REQ-545) Cada ruta tiene su propio filtro. Los filtros se ejecutan dentro de un `_FilteringExporter` envolvente antes de que los spans salgan del proceso — los objetos span originales nunca se mutan. (REQ-546) [tool-verified: `provisa/api/otel_setup.py` lines 156–207]

**`telemetry_filter`** — controla qué llega a su colector interno.

| Clave | Tipo | Por defecto | Descripción |
| ----- | ------ | --------- | ------------- |
| `redact_sql_literals` | bool | `false` | Reemplaza los literales de cadena y numéricos en `db.statement` con `?` |
| `redact_attributes` | list[str] | `[]` | Claves de atributo eliminadas por completo de cada span |

**`support_telemetry_filter`** — controla qué llega al endpoint de soporte de Provisa. La redacción de literales SQL está activada por defecto en esta ruta, ya que los datos de consulta le pertenecen a usted. (REQ-547) [tool-verified: `provisa/api/otel_setup.py` line 240]

| Clave | Tipo | Por defecto | Descripción |
| ----- | ------ | --------- | ------------- |
| `redact_sql_literals` | bool | `true` | Reemplaza los literales de cadena y numéricos en `db.statement` con `?` |
| `redact_attributes` | list[str] | `[]` | Claves de atributo eliminadas por completo de cada span |

Ejemplo de `db.statement` redactado — con `redact_sql_literals: true`, este atributo de span:

```yaml
db.statement: SELECT * FROM orders WHERE region = 'us-west' AND amount > 500
```

se convierte en:

```yaml
db.statement: SELECT * FROM orders WHERE region = ? AND amount > ?
```

### Endpoint de soporte [tool-verified]

`support_endpoint` (o la variable de entorno `PROVISA_SUPPORT_OTLP_ENDPOINT`) reenvía la telemetría al soporte de Provisa para diagnóstico. (REQ-548) Cuando no está configurada, ningún dato sale de su infraestructura por esta ruta. (REQ-548) El filtro de soporte se aplica de forma independiente al filtro interno — puede redactar literales SQL de ambas exportaciones y aun así compartir el tiempo de span y los datos de error con soporte. (REQ-545) [tool-verified: `provisa/api/otel_setup.py` lines 238–288]

### Detección de protocolo de endpoint [tool-verified]

Provisa selecciona OTLP/HTTP u OTLP/gRPC a partir del esquema de la URL del endpoint. (REQ-549) Las URL que comienzan con `http://` o `https://` usan OTLP/HTTP, con `/v1/traces`, `/v1/metrics` y `/v1/logs` añadidos automáticamente. (REQ-549) Cualquier otro esquema usa OTLP/gRPC con `insecure=True`. (REQ-549) [tool-verified: `provisa/api/otel_setup.py` lines 60–70]

## Motor de federación

Configurar un motor de federación es opcional. El valor por defecto es `duckdb` — sin configuración, en proceso, sin servicio externo requerido (REQ-989). Elija otro motor cuando necesite escala MPP o quiera reutilizar un almacén existente.

Precedencia: variable de entorno `PROVISA_ENGINE` → campo de configuración `federation_engine` persistido en la interfaz de administración → `duckdb`. Los cambios surten efecto al reiniciar el servicio. [tool-verified: `engine.py` `build_engine`]

### Resumen de motores [tool-verified: `engine.py` `ENGINE_REGISTRY`, `_ENGINE_BUILDERS`]

| Clave de motor | Etiqueta | Dialecto | MPP | Mecanismo de enlace externo | Autenticación |
| ----------- | ------- | --------- | ----- | ------------------------ | ------ |
| `trino` | Motor de federación de Provisa | Trino SQL | Sí | Catálogos de Trino (amplio conjunto de conectores) | Credenciales JDBC |
| `trino-byo` | Trino | Trino SQL | Sí | Igual que `trino`; coordinador no gestionado | Credenciales JDBC |
| `pg` | PostgreSQL | PostgreSQL | No | FDW / pg_duckdb | Credenciales PostgreSQL |
| `duckdb` | DuckDB | DuckDB | No | ATTACH nativo de extensión | Ninguna (en proceso) |
| `clickhouse` | ClickHouse (integrado) | ClickHouse | Sí | Motores de tabla S3 / IcebergS3 / DeltaLake | chdb (en proceso, sin autenticación) |
| `clickhouse-server` | ClickHouse (Server / Cloud) | ClickHouse | Sí | Motores de tabla S3 / IcebergS3 / DeltaLake | Credenciales ClickHouse |
| `snowflake` | Snowflake | Snowflake | Sí | External stage + tabla externa | `PROVISA_ENGINE_URL` |
| `databricks` | Databricks | Databricks SQL | Sí | Tablas externas de Unity Catalog vía REST | `PROVISA_ENGINE_URL` (token bearer + `http_path`) |
| `bigquery` | BigQuery | BigQuery | Sí | Tablas externas / BigLake de BigQuery | `GOOGLE_APPLICATION_CREDENTIALS` |
| `fabric` | Microsoft Fabric | T-SQL | Sí | Accesos directos OneLake → OPENROWSET | Azure AD (`az login` o identidad administrada) |
| `synapse` | Azure Synapse | T-SQL | Sí | ADLS OPENROWSET / tablas externas | Azure AD |
| `mysql` | MySQL | Por dialecto | No | Ninguno (solo aterrizaje) | Credenciales por dialecto |
| `mariadb` | MariaDB | Por dialecto | No | Ninguno (solo aterrizaje) | Credenciales por dialecto |
| `oracle` | Oracle Database | Por dialecto | No | Ninguno (solo aterrizaje) | Credenciales por dialecto |
| `mssql` | Microsoft SQL Server | Por dialecto | No | Ninguno (solo aterrizaje) | Credenciales por dialecto |
| `db2` | IBM Db2 | Por dialecto | No | Ninguno (solo aterrizaje) | Credenciales por dialecto |
| `redshift` | Amazon Redshift | Por dialecto | No | Ninguno (solo aterrizaje) | Credenciales por dialecto |
| `greenplum` | Greenplum | Por dialecto | No | Ninguno (solo aterrizaje) | Credenciales por dialecto |
| `cockroachdb` | CockroachDB | Por dialecto | No | Ninguno (solo aterrizaje) | Credenciales por dialecto |
| `yugabytedb` | YugabyteDB | Por dialecto | No | Ninguno (solo aterrizaje) | Credenciales por dialecto |
| `opengauss` | openGauss | Por dialecto | No | Ninguno (solo aterrizaje) | Credenciales por dialecto |
| `tidb` | TiDB | Por dialecto | No | Ninguno (solo aterrizaje) | Credenciales por dialecto |
| `singlestore` | SingleStore | Por dialecto | No | Ninguno (solo aterrizaje) | Credenciales por dialecto |
| `vertica` | Vertica | Por dialecto | No | Ninguno (solo aterrizaje) | Credenciales por dialecto |
| `exasol` | Exasol | Por dialecto | No | Ninguno (solo aterrizaje) | Credenciales por dialecto |
| `teradata` | Teradata Vantage | Por dialecto | No | Ninguno (solo aterrizaje) | Credenciales por dialecto |
| `saphana` | SAP HANA | Por dialecto | No | Ninguno (solo aterrizaje) | Credenciales por dialecto |
| `sapase` | SAP ASE (Sybase) | Por dialecto | No | Ninguno (solo aterrizaje) | Credenciales por dialecto |
| `sqlanywhere` | SAP SQL Anywhere | Por dialecto | No | Ninguno (solo aterrizaje) | Credenciales por dialecto |
| `monetdb` | MonetDB | Por dialecto | No | Ninguno (solo aterrizaje) | Credenciales por dialecto |
| `firebird` | Firebird | Por dialecto | No | Ninguno (solo aterrizaje) | Credenciales por dialecto |
| `sqlalchemy` | Otra base de datos relacional (por URL de conexión) | Por dialecto | No | Ninguno (solo aterrizaje) | Credenciales por dialecto |

### Referencia de motores

#### trino / trino-byo

`trino` es el coordinador gestionado por Provisa; `trino-byo` se conecta a su propio clúster de Trino. Ambos usan Trino SQL y tienen el alcance más amplio de tipos de origen.

```bash
PROVISA_ENGINE=trino
TRINO_HOST=trino.internal
TRINO_PORT=8080
```

El almacén de materialización usa por defecto `TENANT_DATABASE_URL` (PostgreSQL).

#### pg

Federa mediante extensiones postgres_fdw (SQL/MED) y pg_duckdb. Nodo único; sin MPP. Óptimo cuando sus datos ya viven en PostgreSQL y desea unir unos pocos orígenes remotos.

```bash
PROVISA_ENGINE=pg
# Connection uses the standard PG_* env vars
```

El almacén de materialización usa por defecto `TENANT_DATABASE_URL`.

#### duckdb

En proceso; sin servicio externo. El motor por defecto (REQ-989). `PROVISA_DATA_DIR` controla dónde vive el almacén integrado (`~/.provisa` por defecto).

```bash
PROVISA_ENGINE=duckdb   # or omit — this is the default
```

El almacén de materialización usa por defecto `~/.provisa/materialize.duckdb` — el único motor con un almacén por defecto que no es PostgreSQL.

#### clickhouse (integrado) / clickhouse-server

`clickhouse` usa chdb (en proceso). `clickhouse-server` se conecta a una instancia externa de ClickHouse o a ClickHouse Cloud. Ambos leen Delta Lake, Iceberg y Hudi directamente mediante motores de tabla nativos de ClickHouse.

```bash
# External server
PROVISA_ENGINE=clickhouse-server
PROVISA_ENGINE_URL="clickhouse://user:pass@host:9000/db"
```

El almacén de materialización usa por defecto `TENANT_DATABASE_URL`.

#### snowflake

Motor como almacén: Snowflake ejecuta las consultas; Provisa empuja los datos de origen a través de external stages.

```bash
PROVISA_ENGINE=snowflake
PROVISA_ENGINE_URL="snowflake://user:pass@account/db/schema?warehouse=WH"
```

El almacén de materialización usa por defecto `TENANT_DATABASE_URL`.

#### databricks

Las tablas externas de Unity Catalog conectan los orígenes gestionados por Provisa con Databricks SQL.

```bash
PROVISA_ENGINE=databricks
PROVISA_ENGINE_URL="databricks://token:TOKEN@my-workspace.azuredatabricks.net?http_path=/sql/1.0/warehouses/xxxx"
```

El almacén de materialización usa por defecto `TENANT_DATABASE_URL`.

#### bigquery

Tablas externas y BigLake de BigQuery. El proyecto proviene de la URL o de `GOOGLE_CLOUD_PROJECT`; la autenticación es mediante clave de cuenta de servicio.

```bash
PROVISA_ENGINE=bigquery
PROVISA_ENGINE_URL="bigquery://my-project?location=US"
# GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
```

El almacén de materialización usa por defecto `TENANT_DATABASE_URL`.

#### fabric / synapse

Ambos usan T-SQL sobre TDS con autenticación Azure AD (`az login` o identidad administrada). Omita `PROVISA_ENGINE_URL` para leer los detalles de conexión desde variables de entorno.

```bash
PROVISA_ENGINE=fabric
# FABRIC_SQL_SERVER=...   FABRIC_DATABASE=...
# or: PROVISA_ENGINE_URL set explicitly

PROVISA_ENGINE=synapse
# SYNAPSE_SQL_SERVER=...  SYNAPSE_DATABASE=...
```

El almacén de materialización usa por defecto `TENANT_DATABASE_URL`.

#### Motores de bases de datos relacionales (mysql, mariadb, oracle, mssql, db2, redshift, greenplum, cockroachdb, yugabytedb, opengauss, tidb, singlestore, vertica, exasol, teradata, saphana, sapase, sqlanywhere, monetdb, firebird) y `sqlalchemy`

Una clave por cada base de datos relacional accesible por red, todas sobre el mismo runtime de solo aterrizaje (sin federación a orígenes externos): cada origen aterriza en el almacén y se consulta ahí. La clave selecciona la base de datos; `PROVISA_ENGINE_URL` lleva el DSN que su dialecto admite. `sqlalchemy` es el comodín para una base de datos sin clave propia. No se ofrecen almacenes embebidos en archivo (SQLite, Access) — el servidor debe ser accesible por red.

```bash
PROVISA_ENGINE=mysql
PROVISA_ENGINE_URL="mysql+pymysql://user:pass@host:3306/db"
```

El almacén de materialización usa por defecto `TENANT_DATABASE_URL`.

### Almacén de materialización

Cuando un origen no puede conectarse (attach) en vivo (no hay conector ATTACH para el motor seleccionado), aterriza en el almacén de materialización del motor. Orden de resolución: `PROVISA_MATERIALIZE_URL` explícito → valor por defecto declarado por el motor → error explícito (sin reserva silenciosa). [tool-verified: `engine.py` `materialize_store`]

DuckDB declara su archivo integrado (`~/.provisa/materialize.duckdb`) como su valor por defecto. Todos los demás motores usan por defecto `TENANT_DATABASE_URL` (PostgreSQL). Anule cualquier motor con `PROVISA_MATERIALIZE_URL`.

### Hints de federación por origen

Los parámetros de conexión extendidos que los campos estándar host/port/user/password no pueden transportar van en `federation_hints` del origen. Ver la referencia de tipos de origen arriba para las claves de hint por tipo. Un ejemplo consolidado:

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

Para orígenes de Google Cloud, configure `GOOGLE_APPLICATION_CREDENTIALS` con la ruta de su archivo de clave de cuenta de servicio. Para Fabric y Synapse, autentíquese con `az login` (desarrollo) o una identidad administrada (producción) — el motor lee las credenciales mediante `DefaultAzureCredential` de `azure-identity`.

## Variables de entorno

| Variable | Por defecto | Descripción |
| ---------- | --------- | ------------- |
| `PROVISA_CONFIG` | `config/provisa.yaml` | Ruta del archivo de configuración |
| `TENANT_DATABASE_URL` | `postgresql+asyncpg://provisa:provisa@localhost:5432/provisa` | URI del almacén del plano de control (SQLAlchemy async); admite `sqlite+aiosqlite://…` / `duckdb://…` para el almacén de escritorio integrado (REQ-828, REQ-850) |
| `PLATFORM_DATABASE_URL` | — | URI del registro de plataforma (directorio de tenants, registro de motores); requerida al inicio, sin reserva (REQ-837) |
| `PROVISA_REDIS_EMBEDDED` | — | `1`/`true` usa fakeredis integrado en lugar de un servidor Redis — sin Docker (REQ-829) |
| `PG_HOST` | `localhost` | Host de PostgreSQL |
| `PG_PORT` | `5432` | Puerto de PostgreSQL |
| `PG_DATABASE` | `provisa` | Base de datos de PostgreSQL |
| `PG_USER` | `provisa` | Usuario de PostgreSQL |
| `PG_PASSWORD` | `provisa` | Contraseña de PostgreSQL |
| `PROVISA_ENGINE` | `duckdb` | Clave del motor de federación (REQ-989, REQ-916) |
| `PROVISA_ENGINE_URL` | — | URL de conexión para motores dirigidos por URL (Snowflake, Databricks, ClickHouse Server, BigQuery, SQLAlchemy) |
| `PROVISA_MATERIALIZE_URL` | — | Anula el DSN del almacén de materialización (por defecto, el valor declarado por el motor) |
| `PROVISA_DATA_DIR` | `~/.provisa` | Directorio de datos para el almacén DuckDB integrado (REQ-989) |
| `TRINO_HOST` | `localhost` | Host del coordinador Trino |
| `TRINO_PORT` | `8080` | Puerto HTTP del coordinador Trino |
| `GOOGLE_APPLICATION_CREDENTIALS` | — | Ruta al JSON de clave de cuenta de servicio de GCP (motor/origen BigQuery) |
| `GOOGLE_CLOUD_PROJECT` | — | Proyecto de GCP por defecto (BigQuery; anulado por la URL) |
| `FABRIC_SQL_SERVER` | — | Endpoint SQL del Fabric Warehouse (alternativa a `PROVISA_ENGINE_URL`) |
| `FABRIC_DATABASE` | — | Nombre de base de datos del Fabric Warehouse |
| `SYNAPSE_SQL_SERVER` | — | Endpoint SQL serverless de Synapse |
| `SYNAPSE_DATABASE` | — | Nombre de base de datos de Synapse |
| `REDIS_URL` | — | URL de conexión a Redis |
| `PROVISA_SAMPLE_SIZE` | `10000` | Límite de muestreo por defecto |
| `PROVISA_DEFAULT_ROW_LIMIT` | `100` | Tope de filas cuando una consulta no suministra un `LIMIT` explícito |
| `PROVISA_RETRY_BUDGET_SECS` | `30` | Presupuesto de reintento de lectura de nivel 1 en segundos; backoff exponencial con jitter completo (REQ-703) |
| `ZAYCHIK_PORT` | `8480` | Puerto del proxy Flight SQL de Zaychik |
| `FLIGHT_PORT` | `8815` | Puerto del servidor Arrow Flight de Provisa |
| `GRPC_PORT` | `50051` | Puerto del servidor gRPC Protobuf de Provisa |
| `PROVISA_REDIRECT_ENABLED` | `false` | Habilita la redirección por umbral del lado del servidor |
| `PROVISA_REDIRECT_THRESHOLD` | `1000` | Umbral de cantidad de filas por defecto |
| `PROVISA_REDIRECT_FORMAT` | `parquet` | Formato de redirección por defecto |
| `PROVISA_REDIRECT_BUCKET` | `provisa-results` | Bucket S3 para resultados redirigidos |
| `PROVISA_REDIRECT_ENDPOINT` | — | URL de endpoint compatible con S3 |
| `PROVISA_REDIRECT_ACCESS_KEY` | — | Clave de acceso S3 |
| `PROVISA_REDIRECT_SECRET_KEY` | — | Clave secreta S3 |
| `PROVISA_REDIRECT_TTL` | `3600` | TTL de la URL prefirmada (segundos) |
| `PROVISA_MTLS_CLIENT_CA` | — | Paquete PEM de las CA permitidas para firmar certificados de cliente; configurarla activa la verificación de certificado de cliente en pgwire, Bolt, gRPC y Flight (REQ-1228) |
| `PROVISA_MTLS_MODE` | `required` una vez configurada una CA | `required` u `optional`; cualquier otro valor se niega a iniciar (REQ-1228) |
| `PROVISA_MTLS_BIND_PRINCIPAL` | `false` | Requiere que el nombre común del certificado sea igual al nombre de usuario autenticante (REQ-1228) |
| `PROVISA_BOLT_ALLOWED_ORIGINS` | — | Sitios separados por comas con permiso para abrir un WebSocket Bolt desde un navegador; sin definir rechaza todo origen de navegador (REQ-802) |
| `PROVISA_EXTRAS` | `firebase,vector` | Extras de pyproject integrados en la imagen de la app; `scripts/provisa` lo deriva de `dq_checker` en `~/.provisa/config.yaml` (REQ-1443) |
| `PROVISA_DQ_CHECKER` | `none` | Solo instalador: `none`/`soda`/`gx`, leído por `first-launch.sh` en modo no interactivo y escrito en `config.yaml` como `dq_checker` (REQ-1443) |
| `ANTHROPIC_API_KEY` | — | Clave de API de Claude (descubrimiento) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | Anula `observability.endpoint` |
| `OTEL_SERVICE_NAME` | `provisa` | Anula `observability.service_name` |
| `OTEL_LOG_LEVEL` | `WARNING` | Anula `observability.log_level` |
| `OTEL_COMPACT_BATCH_SIZE` | `10` | Anula `observability.compact_batch_size` |
| `OTEL_SPAN_EXPORT_DELAY_MILLIS` | `1000` | Retardo de vaciado del procesador de spans por lote |
| `PROVISA_SUPPORT_OTLP_ENDPOINT` | — | Anula `observability.support_endpoint` |
