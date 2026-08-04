# Referencia de configuración

Provisa se configura mediante un archivo YAML (predeterminado: `config/provisa.yaml`). (REQ-528)

## Sources

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

| Field | Default | Notes |
| ------- | --------- | ------- |
| `id` | required | Alfanumérico, guiones, guiones bajos |
| `type` | required | Ver la tabla siguiente |
| `host` | `""` | Nombre de host o IP |
| `port` | `0` | `0` significa que cada conector proporciona su propio valor predeterminado; no existe un mapa central de puertos predeterminados |
| `database` | `""` | |
| `username` | `""` | |
| `password` | `""` | Admite la resolución de secretos `${env:VAR}` |
| `path` | `null` | Ruta de archivo o URI para orígenes basados en archivos |
| `base_url` | `null` | URL base para orígenes de tipo API |
| `pool_min` / `pool_max` | `1` / `5` | Límites del pool de conexiones |
| `cache_enabled` | `true` | Activa o desactiva la caché para todas las tablas de este origen |
| `cache_ttl` | `null` | Segundos; `null` hereda el valor global predeterminado |
| `federation_hints` | `{}` | Parámetros extendidos por conector (dict[str,str]); ver la referencia de tipos más abajo. REQ-281 |
| `mapping` | `{}` | DSL de mapeo para redis, elasticsearch, prometheus. REQ-251 |
| `allowed_domains` | `[]` | Restringe este origen a dominios (`domain`) específicos; vacío = sin restricción |
| `description` | `""` | |

### Tipos de origen admitidos [tool-verified: `provisa/core/models.py:36-101`]

| Type | Connection style | Notes |
| ------ | ----------------- | ------- |
| **RDBMS** | | |
| `postgresql` | host/port | Pool asyncpg; PgBouncer opcional mediante `use_pgbouncer` |
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
| `bigquery` | `federation_hints` | `project` requerido; autenticación mediante `GOOGLE_APPLICATION_CREDENTIALS` |
| `databricks` | host + `federation_hints` | `http_path` requerido en hints |
| `fabric` | variables de entorno o `PROVISA_ENGINE_URL` | T-SQL sobre TDS, autenticación con Azure AD |
| `synapse` | variables de entorno o `PROVISA_ENGINE_URL` | T-SQL sobre TDS, autenticación con Azure AD |
| `redshift` | host/port | |
| **OLAP** | | |
| `clickhouse` | host/port + `federation_hints` | El hint `secure` activa TLS; puerto predeterminado 8123/8443 |
| `elasticsearch` | host/port + DSL `mapping` | |
| `pinot` | host/port | Endpoint REST del controlador |
| `druid` | host/port | Endpoint Avatica del broker |
| `exasol` | host/port | |
| **Data Lake** | | |
| `delta_lake` | `path` (URI de tabla) | `delta_scan` de DuckDB; acceso al almacenamiento de objetos mediante `federation_hints` |
| `iceberg` | `path` (URI de tabla) | `iceberg_scan` de DuckDB; acceso al almacenamiento de objetos mediante `federation_hints` |
| `hudi` | `path` (URI de tabla) | Motor Hudi de ClickHouse, sin copia (REQ-1178) |
| `hive` | host/port (metastore) + `mapping.storage` | Backend de almacenamiento en `mapping["storage"]`: hadoop/hdfs/local/s3/azure/adls |
| `hive_s3` | host/port (metastore) + claves S3 en `mapping` | Tipo distinto; siempre almacenamiento S3 (REQ-229) |
| **NoSQL** | | |
| `mongodb` | host/port | Campos de conexión simples; sin DSL de mapeo |
| `cassandra` | host/port | Campos de conexión simples; sin DSL de mapeo |
| `redis` | host/port + DSL `mapping` | |
| **Streaming** | | |
| `kafka` | solo registro | La configuración real vive en `kafka_sources[]`; ver §Kafka más abajo |
| `websocket` | host/port/path + `federation_hints` | Feed externo por WebSocket |
| `rss` | host/port/path + `federation_hints` | Feed RSS 2.0 / Atom |
| **Graph/Semantic** | | |
| `neo4j` | [UNVERIFIED end-to-end mapping] | |
| `sparql` | [UNVERIFIED end-to-end mapping] | |
| **File** | | |
| `sqlite` | `path` | Siempre se enruta a través del motor (sin pool directo) |
| `csv` | `path` | |
| `parquet` | `path` | |
| `files` | `path` (directorio) | Rastreador por glob; expone CSV/Parquet/XLSX/JSON como tablas |
| **API/Remote** | | |
| `google_sheets` | `federation_hints.spreadsheet_id` | |
| `prometheus` | host/port o `mapping.url` + DSL `mapping` | |
| `graphql_remote` | `base_url` + `mapping` opcional | Headers, forward-client-headers, timeout en `mapping` |
| `openapi` | `base_url` | |
| `grpc_remote` | [UNVERIFIED end-to-end mapping] | |
| `airport` | `base_url` (ubicación Flight) | Extensión airport de DuckDB (REQ-899) |
| `ingest` | receptor push | Servicios externos envían eventos JSON mediante POST |
| **SaaS** | | |
| `sharepoint` | `base_url` o `host` + `mapping` | Autenticación mediante `mapping.auth_type` |
| `splunk` | `host`/`port` o `base_url` + `mapping` | |
| **GovData** | | |
| `govdata` | subject + `domain_id` | Modelo separado `GovDataSource`; ver §GovData más abajo |

### Referencia de tipos de origen

Los tipos que requieren configuración no evidente tienen una entrada breve más abajo. Los tipos RDBMS (postgresql, mysql, etc.) usan únicamente los campos comunes anteriores; no necesitan una sección adicional.

#### GovData [tool-verified: `provisa/core/models.py:953-983`]

Los orígenes `govdata` usan un modelo de nivel superior separado, `GovDataSource`, no el `Source` genérico. (REQ-540) El acceso está particionado por agrupación de subject.

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

Cada subject se asocia a uno o más esquemas de GovData. Configurar un origen `govdata` con un subject expone automáticamente todos los esquemas de ese subject. (REQ-540)

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

Los esquemas `ref` y `geo` siempre se incluyen como esquemas de enlace (linker); no son configurables y no aparecen en la lista anterior. (REQ-541) Use el subject `ALL` para otorgar acceso a todos los esquemas. [tool-verified: `provisa/core/models.py:961-963`]

#### Kafka [tool-verified: `provisa/federation/trino_connectors.py:497-502`, `provisa/api/app_loaders.py:113-118`]

La fila `kafka` en `sources:` es solo de registro. El método `details()` de su conector devuelve `{}`; la configuración real vive en el bloque de nivel superior `kafka_sources[]`, no en una fila de `sources:`. Kafka es siempre un VIRTUAL_SOURCE (se enruta a través del motor; sin pool directo). [tool-verified: `provisa/transpiler/router.py:44-63`]

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

**Time Window** — `default_window` acota cada consulta a un período de tiempo reciente, evitando lecturas sin límite en topics de alto volumen. (REQ-148) Formato: `1h`, `30m`, `7d`, `60s`. Predeterminado: `1h`. Se inyecta automáticamente como `WHERE _timestamp >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR`. Los clientes pueden anularlo con su propio filtro `_timestamp` en el argumento `where` de GraphQL.

**Discriminator** — Varias configuraciones de topic pueden apuntar al mismo topic físico de Kafka con distintos valores de `discriminator`, generando tipos GraphQL separados. (REQ-149) El discriminator se inyecta automáticamente como una cláusula WHERE.

**Schema Source**

| Value | Behavior |
| ------- | ---------- |
| `registry` | Obtiene el esquema desde Confluent Schema Registry |
| `manual` | Define las columnas en línea en la configuración (no requiere Schema Registry) |
| `sample` | Descubre automáticamente a partir de mensajes de muestra |

#### Snowflake [tool-verified: `provisa/executor/drivers/snowflake.py:48-62`]

`account` en `federation_hints` es obligatorio. `warehouse`, `role` y `schema` son opcionales.

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

`http_path` en `federation_hints` es obligatorio. `password` transporta el token de acceso personal. `catalog` es opcional (se transporta en SQL/hints, no en el campo `database`).

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

`project` en `federation_hints` es obligatorio. La autenticación usa `GOOGLE_APPLICATION_CREDENTIALS` (ruta a un archivo de clave de cuenta de servicio) o las Application Default Credentials en el entorno del motor.

```yaml
sources:
  - id: my-bigquery
    type: bigquery
    federation_hints:
      project: my-gcp-project     # required
```

#### Fabric / Synapse [tool-verified: `provisa/core/models.py:56-57`]

Ambos usan T-SQL sobre TDS con autenticación Azure AD. Autentique con `az login` (desarrollo) o una identidad administrada (producción); el motor lee las credenciales mediante `DefaultAzureCredential` de `azure-identity`. Los detalles de conexión provienen de variables de entorno: `FABRIC_SQL_SERVER` / `FABRIC_DATABASE` (Fabric) o `SYNAPSE_SQL_SERVER` / `SYNAPSE_DATABASE` (Synapse), o mediante `PROVISA_ENGINE_URL`.

```yaml
sources:
  - id: my-fabric
    type: fabric
    # host/database read from FABRIC_SQL_SERVER / FABRIC_DATABASE when not set here
```

#### ClickHouse [tool-verified: `provisa/executor/drivers/clickhouse.py:49-59`]

`secure` en `federation_hints` activa TLS en la interfaz HTTP. El puerto predeterminado es `8123` (plano) u `8443` (cuando `secure: "true"`). `schema` en `federation_hints` anula el esquema remoto. [tool-verified: `provisa/federation/connector_duckdb.py:378-379`]

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

`path` es la URI de la tabla (S3, GCS, ADLS o local). El acceso al almacenamiento de objetos requiere credenciales en `federation_hints`. Para Cloudflare R2, agregue `account_id`.

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

`host` y `port` apuntan al metastore Thrift de Hive (puerto predeterminado 9083). Para `hive`, configure `mapping["storage"]` para elegir el backend de almacenamiento de objetos. Las claves requeridas faltantes fallan de forma explícita; no hay fallback. [tool-verified: `provisa/federation/trino_connectors.py:328-331`]

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

Valores aceptados de `mapping.storage`: `hadoop` (predeterminado), `hdfs`, `local`, `s3`, `azure`, `adls`. Claves de mapeo S3: `endpoint`, `access_key_id`, `secret_access_key`, `region`, `path_style`. Claves de mapeo ADLS: `storage_account`, `access_key` o `sas_token`.

#### Redis [tool-verified: `provisa/core/trino_catalog_files.py:54-75`]

Usa el DSL `mapping`. `mongodb` y `cassandra` usan campos de conexión simples y NO usan el DSL de mapeo.

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

`spreadsheet_id` en `federation_hints` es obligatorio. La autenticación usa un SECRET `gsheet` de DuckDB aprovisionado en el momento del attach.

```yaml
sources:
  - id: my-sheet
    type: google_sheets
    federation_hints:
      spreadsheet_id: 1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms
```

#### Orígenes de archivo (csv / parquet / sqlite / files)

`path` es obligatorio. `files` rastrea un directorio en busca de archivos CSV, Parquet, XLSX y JSON, exponiendo cada uno como una tabla. Todos los orígenes basados en archivos son VIRTUAL (se enrutan a través del motor; sin pool directo). [tool-verified: `provisa/transpiler/router.py:44-48`]

```yaml
sources:
  - id: orders-csv
    type: csv
    path: /data/orders.csv

  - id: data-lake-dir
    type: files
    path: /data/lake/         # directory; each file becomes a table
```

#### Orígenes de tipo API / Remote

**openapi** — configure `base_url` con la URL base de OpenAPI. El descubrimiento de esquema lee la especificación OpenAPI al inicio.

```yaml
sources:
  - id: payment-api
    type: openapi
    base_url: https://api.payments.example.com/v1
```

**graphql_remote** — configure `base_url`. Claves `mapping` opcionales: `headers` (dict de headers estáticos), `forward_client_headers` (bool), `timeout_seconds` (int). [tool-verified: `provisa/hasura_v2/mapper.py:129-152`]

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

## Domains

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

### Convención de nomenclatura

La autoridad de nomenclatura es la fuente única de verdad para los nombres orientados al cliente; los nombres físicos de columnas del backend nunca se exponen a los clientes. (REQ-194) Cada lenguaje de consulta deriva el nombre de una columna a partir de su `column.alias` si está definido, o si no, del nombre físico de la columna según su convención configurada. (REQ-194)

La convención de GraphQL es uno de tres enums predefinidos. (REQ-416) Las cadenas de formato libre antiguas (`none`, `snake_case`, `camelCase`, `PascalCase`) están obsoletas. (REQ-416)

| Preset | Default | Type names | Field names | Mutation names |
| -------- | --------- | ------------ | ------------- | ---------------- |
| `apollo_graphql` | yes | PascalCase | camelCase | camelCase |
| `hasura_graphql` | | PascalCase | camelCase | snake_case |
| `snake` | | PascalCase | snake_case | snake_case |

La convención GraphQL predeterminada es `apollo_graphql`, que produce nombres de campo y de mutación en camelCase. (REQ-194, REQ-416) La convención SQL es independiente, con `snake_case` como predeterminada, aplicada mediante `apply_sql_name()`; la convención GraphQL se aplica mediante `apply_gql_name()`, y el nombre CQL se deriva del nombre GraphQL. (REQ-194)

`domain_prefix: bool` es una opción ortogonal que se aplica independientemente del preset elegido. (REQ-416)

El `column.alias` explícito es el nombre canónico: SQL lo usa tal cual sin aplicar ninguna convención, GraphQL le aplica su convención, y CQL se deriva del nombre GraphQL. (REQ-194)

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

Cuando `domain_prefix: true`, todos los nombres de campo y de tipo GraphQL se prefijan con el ID del dominio usando un separador de doble guión bajo: (REQ-154)

| Table | Domain | Field Name |
| ------- | -------- | ----------- |
| `orders` | `sales-analytics` | `sales_analytics__orders` |
| `customer_segments` | `customer-insights` | `customer_insights__customer_segments` |

Esto evita colisiones de nombres cuando distintos dominios tienen tablas con el mismo nombre, y hace que las consultas sean autoexplicativas.

### Reglas de nomenclatura

Reglas de expresión regular aplicadas a los nombres de tabla al generar los nombres de campo GraphQL. Se aplican en orden antes de la resolución de unicidad. (REQ-542)

## Tables

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

Los alias de tabla y de columna anulan el nombre GraphQL predeterminado. (REQ-155) Útil para:

- Renombrar nombres crípticos de base de datos (p. ej., `tbl_cust_seg` → `customer_segments`)
- Evitar abreviaturas en la capa de API
- Crear un vocabulario limpio y específico del dominio

### Descripciones

Las descripciones de tabla y columna se incluyen en el SDL de GraphQL generado. (REQ-156) Aparecen en el explorador de documentación de GraphiQL y en las consultas de introspección. Se definen en el YAML de configuración o mediante la UI de administración.

### Path (extracción computada de JSON)

Las columnas pueden extraer valores de una columna origen JSON/JSONB usando un `path` en notación de puntos. (REQ-151) Esto es útil para datos semiestructurados en mensajes de Kafka, documentos MongoDB o columnas JSONB de PostgreSQL.

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

**Impacto en el enrutamiento:** las columnas de tipo path usan operadores JSON de PostgreSQL (`->>`), que son compatibles de forma nativa con el enrutamiento directo a PG. (REQ-152) Para orígenes que no son PostgreSQL (MySQL, SQL Server, etc.), las consultas con columnas de tipo path se enrutan automáticamente a través del motor de federación. (REQ-152) Las mutaciones no se ven afectadas, ya que las columnas de tipo path son campos computados de solo lectura. (REQ-153)

### Tipos de enmascaramiento

| Type | Fields | Description |
| ------ | -------- | ------------- |
| `regex` | `pattern`, `replace` | REGEXP_REPLACE (solo columnas de tipo cadena) |
| `constant` | `value` | Reemplazo literal (NULL, 0, MAX, MIN, personalizado) |
| `truncate` | `precision` | DATE_TRUNC (solo columnas de fecha/timestamp) |

## Relationships

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

### Auto-materialización

Configure `materialize: true` en una relación para generar automáticamente una vista materializada para JOINs entre orígenes distintos. (REQ-158) Esto evita consultas federadas costosas al precomputar el resultado del JOIN.

- Solo las relaciones entre orígenes distintos generan MVs (los JOINs dentro del mismo origen ya son rápidos) (REQ-159)
- La MV comienza obsoleta (stale) y se completa mediante el bucle de actualización en segundo plano (REQ-160)
- Las mutaciones sobre cualquiera de las tablas origen marcan la MV como obsoleta para su reactualización (REQ-543)
- `refresh_interval` es de 300 segundos (5 minutos) por defecto (REQ-543)

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

Los roles con `parent_role_id` heredan capacidades y acceso a dominios del rol padre. (REQ-215) La jerarquía se aplana al inicio. (REQ-215)

### Capabilities

| Capability | Description |
| ----------- | ------------- |
| `source_registration` | Registrar orígenes de datos |
| `table_registration` | Registrar tablas |
| `relationship_registration` | Definir relaciones |
| `security_config` | Configurar RLS, enmascaramiento |
| `query_development` | Ejecutar consultas |
| `full_results` | Omitir los límites de muestreo |
| `admin` | Todas las capacidades |

## RLS Rules

```yaml
rls_rules:
  - table_id: orders
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"
```

## Materialized Views

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

## Views (conjuntos de datos computados gobernados)

Las views son conjuntos de datos computados definidos en SQL con gobierno a nivel de columna completo. (REQ-133) Son el mecanismo gobernado para agregar agregaciones, transformaciones y métricas derivadas a la capa semántica. (REQ-136)

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

| Field | Required | Description |
| ------- | ---------- | ------------- |
| `id` | Yes | Identificador único de la view |
| `sql` | Yes | Sentencia SQL SELECT que define la view |
| `domain_id` | Yes | Dominio para la visibilidad del esquema |
| `materialize` | No | `true` = actualización periódica por CTAS, `false` = view federada en vivo |
| `refresh_interval` | No | Segundos entre actualizaciones (solo materializadas, 300 por defecto) |
| `description` | No | Aparece en el SDL de GraphQL |
| `alias` | No | Anula el nombre GraphQL |
| `columns` | Yes | Definiciones de columna con visibilidad, enmascaramiento y descripciones |

### Materializada vs. en vivo

- **`materialize: true`**: Provisa crea una tabla mediante CTAS y la actualiza según un cronograma. (REQ-135) Consultas más rápidas, pero los datos pueden estar obsoletos hasta `refresh_interval` segundos.
- **`materialize: false`**: Provisa crea una view federada. (REQ-135) Las consultas siempre devuelven datos en vivo, pero pueden ser más lentas en agregaciones complejas.

Las views pasan por el mismo pipeline de gobierno que las tablas: RLS, enmascaramiento, muestreo y visibilidad basada en roles. (REQ-134) Esto garantiza que no se pueda agregar ninguna semántica nueva a la plataforma sin supervisión de un data steward. (REQ-136)

### Views de solo consulta

Tanto las views con `materialize: true` como las de `materialize: false` exponen su tipo GraphQL como de solo consulta. No se genera ninguna mutación de tipo insert, upsert, update ni delete para relaciones respaldadas por `view_sql`. (REQ-1157) [tool-verified: `provisa/compiler/schema_gen.py:184`, `provisa/compiler/schema_types.py:79`]

## Cache

```yaml
cache:
  enabled: true
  redis_url: ${env:REDIS_URL}
  default_ttl: 300
```

### Jerarquía de caché

Orden de resolución del TTL (el más específico prevalece): **tabla** > **origen** > **valor global predeterminado**. (REQ-544) Se usa el primer valor no nulo.

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

Configurar `cache_enabled: false` en un origen desactiva la caché para todas las tablas de ese origen, independientemente del TTL a nivel de tabla. (REQ-544) Las claves de caché siempre incluyen `role_id` y los valores de contexto RLS para el particionamiento de seguridad. (REQ-544)

## Authentication

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

| Provider | Use Case | Token Validation |
| ---------- | ---------- | ----------------- |
| `simple` | Desarrollo/pruebas locales. Usuarios definidos en YAML. | JWT firmado con `PROVISA_JWT_SECRET` |
| `firebase` | Firebase Authentication (todos los métodos). | `verify_id_token()` del SDK `firebase-admin` |
| `keycloak` | Keycloak OIDC. Roles de tenant y de cliente mapeados. | Validación de JWT basada en JWKS |
| `oauth` | OIDC genérico (Okta, Azure AD, Auth0, PingFederate). | JWKS desde la URL de descubrimiento |

Las credenciales de superusuario (bloque `superuser`) funcionan con cualquier proveedor y siempre se resuelven al rol admin con todas las capacidades. (REQ-125) Se usan para la configuración inicial antes de configurar la autenticación externa.

### Ejemplo completo de configuración de auth (comentado)

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

## Upsert Mutations

Para tablas con clave primaria, Provisa genera automáticamente campos de mutación `upsert_<table>`. (REQ-212) Estos compilan a un upsert en el dialecto de destino: `INSERT ... ON CONFLICT (pk) DO UPDATE SET ...` en PostgreSQL, `ON DUPLICATE KEY UPDATE` en MySQL. (REQ-212)

```graphql
mutation {
  upsert_orders(objects: [{id: 1, amount: 150.00, region: "us"}]) {
    affected_rows
  }
}
```

Las columnas de conflicto se derivan de los metadatos de la PK. (REQ-212) Se aplican todas las reglas de visibilidad de columna y permisos de escritura.

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

Compila a `SELECT DISTINCT ON (region) ...` en PostgreSQL. (REQ-213) Para dialectos que no son PG, se usa un fallback basado en función de ventana. (REQ-213)

## Column Presets

Inyecta valores automáticamente en columnas durante insert/update. (REQ-214) Se definen por tabla en la configuración.

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

| Source | Behavior |
| -------- | ---------- |
| `header` | Inyecta el valor del header HTTP de la solicitud indicado |
| `now` | Inyecta `NOW()` (marca de tiempo actual) |
| `literal` | Inyecta un valor constante |

Las columnas preset se inyectan durante la compilación de la mutación, antes de la generación de SQL. (REQ-214) No son visibles en el tipo de entrada de la mutación. (REQ-214)

## Inherited Roles

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

Se admite herencia multinivel. (REQ-215) Las capacidades y el domain_access explícitos del rol hijo se combinan con los del padre. (REQ-215)

## Scheduled Triggers

Triggers basados en cron que invocan una URL de webhook según un cronograma. (REQ-216) Usa APScheduler. (REQ-216)

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

Las tareas programadas se gestionan mediante la UI de administración (interruptor de activar/desactivar) o la mutación de administración `toggle_scheduled_task`. (REQ-216)

## OrderBy Format

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

| Direction | SQL |
| ----------- | ----- |
| `asc` | `ASC` |
| `desc` | `DESC` |
| `asc_nulls_first` | `ASC NULLS FIRST` |
| `asc_nulls_last` | `ASC NULLS LAST` |
| `desc_nulls_first` | `DESC NULLS FIRST` |
| `desc_nulls_last` | `DESC NULLS LAST` |

El ordenamiento de relaciones se admite mediante objetos anidados: (REQ-202)

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

### Filtros de telemetría [tool-verified]

Provisa ejecuta dos rutas de exportación OTLP independientes: su colector interno y el endpoint de soporte opcional de Provisa. (REQ-545) Cada ruta tiene su propio filtro. Los filtros se ejecutan dentro de un `_FilteringExporter` envolvente antes de que los spans salgan del proceso; los objetos span originales nunca se modifican. (REQ-546) [tool-verified: `provisa/api/otel_setup.py` lines 156–207]

**`telemetry_filter`** — controla lo que llega a su colector interno.

| Key | Type | Default | Description |
| ----- | ------ | --------- | ------------- |
| `redact_sql_literals` | bool | `false` | Reemplaza los literales de cadena y numéricos en `db.statement` con `?` |
| `redact_attributes` | list[str] | `[]` | Claves de atributo eliminadas por completo de cada span |

**`support_telemetry_filter`** — controla lo que llega al endpoint de soporte de Provisa. La redacción de literales SQL está activada por defecto (`true`) en esta ruta, ya que los datos de consulta le pertenecen a usted. (REQ-547) [tool-verified: `provisa/api/otel_setup.py` line 240]

| Key | Type | Default | Description |
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

`support_endpoint` (o la variable de entorno `PROVISA_SUPPORT_OTLP_ENDPOINT`) reenvía telemetría al soporte de Provisa para diagnósticos. (REQ-548) Si no está configurado, ningún dato sale de su infraestructura por esta ruta. (REQ-548) El filtro de soporte se aplica de forma independiente del filtro interno; puede redactar los literales SQL de ambas exportaciones y aun así compartir con el soporte los datos de tiempos y errores de los spans. (REQ-545) [tool-verified: `provisa/api/otel_setup.py` lines 238–288]

### Detección del protocolo del endpoint [tool-verified]

Provisa selecciona OTLP/HTTP u OTLP/gRPC según el esquema de la URL del endpoint. (REQ-549) Las URLs que comienzan con `http://` o `https://` usan OTLP/HTTP, con `/v1/traces`, `/v1/metrics` y `/v1/logs` añadidos automáticamente. (REQ-549) Cualquier otro esquema usa OTLP/gRPC con `insecure=True`. (REQ-549) [tool-verified: `provisa/api/otel_setup.py` lines 60–70]

## Federation Engine

Configurar un motor de federación es opcional. El predeterminado es `duckdb`: sin configuración, en proceso, sin necesidad de un servicio externo (REQ-989). Elija otro motor cuando necesite escala MPP o quiera reutilizar un almacén de datos existente.

Precedencia: variable de entorno `PROVISA_ENGINE` → campo de configuración `federation_engine` persistido en la UI de administración → `duckdb`. Los cambios surten efecto al reiniciar el servicio. [tool-verified: `engine.py` `build_engine`]

### Resumen de motores [tool-verified: `engine.py` `ENGINE_REGISTRY`, `_ENGINE_BUILDERS`]

| Engine key | Label | Dialect | MPP | External-link mechanism | Auth |
| ----------- | ------- | --------- | ----- | ------------------------ | ------ |
| `trino` | Provisa Federation Engine | Trino SQL | Yes | Catálogos Trino (amplio conjunto de conectores) | Credenciales JDBC |
| `trino-byo` | Trino (bring-your-own) | Trino SQL | Yes | Igual que `trino`; coordinador no administrado | Credenciales JDBC |
| `pg` | PostgreSQL | PostgreSQL | No | FDW / pg_duckdb | Credenciales PostgreSQL |
| `duckdb` | DuckDB | DuckDB | No | ATTACH nativo por extensión | Ninguna (en proceso) |
| `clickhouse` | ClickHouse (embedded) | ClickHouse | Yes | Motores de tabla S3 / IcebergS3 / DeltaLake | chdb (en proceso, sin autenticación) |
| `clickhouse-server` | ClickHouse (Server / Cloud) | ClickHouse | Yes | Motores de tabla S3 / IcebergS3 / DeltaLake | Credenciales ClickHouse |
| `snowflake` | Snowflake | Snowflake | Yes | Stage externo + tabla externa | `PROVISA_ENGINE_URL` |
| `databricks` | Databricks | Databricks SQL | Yes | Tablas externas de Unity Catalog mediante REST | `PROVISA_ENGINE_URL` (token bearer + `http_path`) |
| `bigquery` | BigQuery | BigQuery | Yes | Tablas externas de BigQuery / BigLake | `GOOGLE_APPLICATION_CREDENTIALS` |
| `fabric` | Microsoft Fabric | T-SQL | Yes | Accesos directos de OneLake → OPENROWSET | Azure AD (`az login` o identidad administrada) |
| `synapse` | Azure Synapse | T-SQL | Yes | ADLS OPENROWSET / tablas externas | Azure AD |
| `sqlalchemy` | SQLAlchemy (any RDB) | Per-dialect | No | Ninguno (solo aterrizaje de datos) | Credenciales por dialecto |

### Referencia de motores

#### trino / trino-byo

`trino` es el coordinador administrado de Provisa; `trino-byo` se conecta a su propio clúster Trino. Ambos usan Trino SQL y tienen el mayor alcance de tipos de origen.

```bash
PROVISA_ENGINE=trino
TRINO_HOST=trino.internal
TRINO_PORT=8080
```

El almacén de materialización usa por defecto `TENANT_DATABASE_URL` (PostgreSQL).

#### pg

Federa mediante extensiones postgres_fdw (SQL/MED) y pg_duckdb. Un solo nodo; sin MPP. Óptimo cuando sus datos ya residen en PostgreSQL y desea unir algunos orígenes remotos.

```bash
PROVISA_ENGINE=pg
# Connection uses the standard PG_* env vars
```

El almacén de materialización usa por defecto `TENANT_DATABASE_URL`.

#### duckdb

En proceso; sin servicio externo. El motor predeterminado (REQ-989). `PROVISA_DATA_DIR` controla dónde vive el almacén embebido (`~/.provisa` por defecto).

```bash
PROVISA_ENGINE=duckdb   # or omit — this is the default
```

El almacén de materialización usa por defecto `~/.provisa/materialize.duckdb`: el único motor con un almacén predeterminado que no es PostgreSQL.

#### clickhouse (embedded) / clickhouse-server

`clickhouse` usa chdb (en proceso). `clickhouse-server` se conecta a una instancia externa de ClickHouse o a ClickHouse Cloud. Ambos leen Delta Lake, Iceberg y Hudi directamente mediante motores de tabla nativos de ClickHouse.

```bash
# External server
PROVISA_ENGINE=clickhouse-server
PROVISA_ENGINE_URL="clickhouse://user:pass@host:9000/db"
```

El almacén de materialización usa por defecto `TENANT_DATABASE_URL`.

#### snowflake

Motor como almacén de datos: Snowflake ejecuta las consultas; Provisa envía los datos de origen a través de stages externos.

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

Tablas externas y BigLake de BigQuery. El proyecto proviene de la URL o de `GOOGLE_CLOUD_PROJECT`; la autenticación se realiza mediante clave de cuenta de servicio.

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

#### sqlalchemy

Motor RDBMS genérico de solo aterrizaje de datos (sin federación a orígenes externos). Úselo para despliegues de un solo almacén de datos o para pruebas.

```bash
PROVISA_ENGINE=sqlalchemy
PROVISA_ENGINE_URL="postgresql+psycopg2://user:pass@host/db"
```

El almacén de materialización usa por defecto `TENANT_DATABASE_URL`.

### Almacén de materialización

Cuando un origen no puede conectarse en vivo (sin conector ATTACH para el motor seleccionado), sus datos aterrizan en el almacén de materialización del motor. Orden de resolución: `PROVISA_MATERIALIZE_URL` explícito → valor predeterminado declarado por el motor → error explícito (sin fallback silencioso). [tool-verified: `engine.py` `materialize_store`]

DuckDB declara su archivo embebido (`~/.provisa/materialize.duckdb`) como su valor predeterminado. Todos los demás motores usan por defecto `TENANT_DATABASE_URL` (PostgreSQL). Anule cualquier motor con `PROVISA_MATERIALIZE_URL`.

### Hints de federación por origen

Los parámetros de conexión extendidos que los campos estándar host/port/user/password no pueden transportar se colocan en `federation_hints` del origen. Ver la referencia de tipos de origen anterior para las claves de hint por tipo. Un ejemplo consolidado:

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

Para orígenes de Google Cloud, configure `GOOGLE_APPLICATION_CREDENTIALS` con la ruta al archivo de clave de cuenta de servicio. Para Fabric y Synapse, autentique con `az login` (desarrollo) o una identidad administrada (producción); el motor lee las credenciales mediante `DefaultAzureCredential` de `azure-identity`.

## Environment Variables

| Variable | Default | Description |
| ---------- | --------- | ------------- |
| `PROVISA_CONFIG` | `config/provisa.yaml` | Ruta del archivo de configuración |
| `TENANT_DATABASE_URL` | `postgresql+asyncpg://provisa:provisa@localhost:5432/provisa` | URI del almacén del plano de control (SQLAlchemy async); admite `sqlite+aiosqlite://…` / `duckdb://…` para el almacén de escritorio embebido (REQ-828, REQ-850) |
| `PLATFORM_DATABASE_URL` | — | URI del registro de plataforma (directorio de tenants, registro de motores); obligatorio al inicio, sin fallback (REQ-837) |
| `PROVISA_REDIS_EMBEDDED` | — | `1`/`true` usa fakeredis embebido en lugar de un servidor Redis; sin Docker (REQ-829) |
| `PG_HOST` | `localhost` | Host de PostgreSQL |
| `PG_PORT` | `5432` | Puerto de PostgreSQL |
| `PG_DATABASE` | `provisa` | Base de datos de PostgreSQL |
| `PG_USER` | `provisa` | Usuario de PostgreSQL |
| `PG_PASSWORD` | `provisa` | Contraseña de PostgreSQL |
| `PROVISA_ENGINE` | `duckdb` | Clave del motor de federación (REQ-989) |
| `PROVISA_ENGINE_URL` | — | URL de conexión para motores guiados por URL (Snowflake, Databricks, ClickHouse Server, BigQuery, SQLAlchemy) |
| `PROVISA_MATERIALIZE_URL` | — | Anula el DSN del almacén de materialización (por defecto, el declarado por el motor) |
| `PROVISA_DATA_DIR` | `~/.provisa` | Directorio de datos para el almacén DuckDB embebido (REQ-989) |
| `TRINO_HOST` | `localhost` | Host del coordinador Trino |
| `TRINO_PORT` | `8080` | Puerto HTTP del coordinador Trino |
| `GOOGLE_APPLICATION_CREDENTIALS` | — | Ruta al JSON de clave de cuenta de servicio de GCP (motor/origen BigQuery) |
| `GOOGLE_CLOUD_PROJECT` | — | Proyecto GCP predeterminado (BigQuery; anulado por la URL) |
| `FABRIC_SQL_SERVER` | — | Endpoint SQL de Fabric Warehouse (alternativa a `PROVISA_ENGINE_URL`) |
| `FABRIC_DATABASE` | — | Nombre de la base de datos de Fabric Warehouse |
| `SYNAPSE_SQL_SERVER` | — | Endpoint SQL serverless de Synapse |
| `SYNAPSE_DATABASE` | — | Nombre de la base de datos de Synapse |
| `REDIS_URL` | — | URL de conexión de Redis |
| `PROVISA_SAMPLE_SIZE` | `10000` | Límite de muestreo predeterminado |
| `PROVISA_DEFAULT_ROW_LIMIT` | `100` | Límite de filas cuando una consulta no incluye un `LIMIT` explícito |
| `PROVISA_RETRY_BUDGET_SECS` | `30` | Presupuesto de reintento de lectura de nivel 1, en segundos; backoff exponencial con jitter completo (REQ-703) |
| `ZAYCHIK_PORT` | `8480` | Puerto del proxy Zaychik Flight SQL |
| `FLIGHT_PORT` | `8815` | Puerto del servidor Arrow Flight de Provisa |
| `GRPC_PORT` | `50051` | Puerto del servidor Protobuf gRPC de Provisa |
| `PROVISA_REDIRECT_ENABLED` | `false` | Activa la redirección por umbral en el servidor |
| `PROVISA_REDIRECT_THRESHOLD` | `1000` | Umbral de cantidad de filas predeterminado |
| `PROVISA_REDIRECT_FORMAT` | `parquet` | Formato de redirección predeterminado |
| `PROVISA_REDIRECT_BUCKET` | `provisa-results` | Bucket S3 para resultados redirigidos |
| `PROVISA_REDIRECT_ENDPOINT` | — | URL de endpoint compatible con S3 |
| `PROVISA_REDIRECT_ACCESS_KEY` | — | Clave de acceso S3 |
| `PROVISA_REDIRECT_SECRET_KEY` | — | Clave secreta S3 |
| `PROVISA_REDIRECT_TTL` | `3600` | TTL de la URL prefirmada (segundos) |
| `ANTHROPIC_API_KEY` | — | Clave de API de Claude (descubrimiento) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | Anula `observability.endpoint` |
| `OTEL_SERVICE_NAME` | `provisa` | Anula `observability.service_name` |
| `OTEL_LOG_LEVEL` | `WARNING` | Anula `observability.log_level` |
| `OTEL_COMPACT_BATCH_SIZE` | `10` | Anula `observability.compact_batch_size` |
| `OTEL_SPAN_EXPORT_DELAY_MILLIS` | `1000` | Retraso de descarga del procesador de spans por lotes |
| `PROVISA_SUPPORT_OTLP_ENDPOINT` | — | Anula `observability.support_endpoint` |
