# Referencia de configuración {: #configuration-reference }

Provisa se configura mediante un archivo YAML (por defecto: `config/provisa.yaml`).

## Includes

Divida una configuración entre varios archivos con `includes:`. El archivo que incluye enumera rutas de fragmentos bajo esta clave; Provisa las combina antes de la validación, produciendo el mismo resultado que escribir todo en un solo archivo.

```yaml
# provisa-with-sources.yaml — wrapper that adds a Neo4j source to the base config
includes:
  - /path/to/config/provisa-install.yaml
  - /path/to/demo/sources/neo4j/fragment.yaml
```

Un archivo wrapper que solo contiene `includes:` es válido. `load_control_plane` lee a través de los includes, así que la sección `control_plane:` se toma del archivo incluido que la establezca.

**Reglas de combinación**

- Las rutas se resuelven en relación con el archivo que incluye. Las rutas absolutas se usan tal cual.
- Las secciones de tipo lista (`sources`, `tables`, `domains`, `relationships`, `roles`, …) se anexan, y las entradas del fragmento van después de las entradas del archivo que incluye.
- Una clave escalar o de mapeo que el archivo que incluye no establece se toma del fragmento.
- Una clave que ambos archivos establecen con valores distintos es un conflicto; la carga falla, nombrando la clave.
- Valores idénticos en ambos archivos no son un conflicto.
- Los includes se anidan. Un archivo que se incluye a sí mismo — directamente o a través de otro fragmento — se rechaza.
- `includes` se consume en el momento de la carga y nunca aparece en la configuración validada.

## Orígenes {: #sources }

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

Todos los orígenes comparten un conjunto común de campos.

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
| `cache_ttl` | `null` | Segundos; `null` hereda el valor global por defecto; `-1` nunca caduca |
| `federation_hints` | `{}` | Parámetros extendidos por conector (dict[str,str]); ver referencia de tipos abajo. |
| `mapping` | `{}` | DSL de mapeo para redis, elasticsearch, prometheus. |
| `allowed_domains` | `[]` | Restringe este origen a IDs de dominio específicos; vacío = sin restricción |
| `description` | `""` | |

### Tipos de origen soportados {: #supported-source-types }

| Tipo | Estilo de conexión | Notas |
| ------ | ----------------- | ------- |
| **RDBMS** | | |
| `postgresql` | host/port | Pool asyncpg; PgBouncer opcional vía `use_pgbouncer` |
| `mysql` | host/port | |
| `mariadb` | host/port | |
| `singlestore` | host/port | |
| `sqlserver` | host/port | |
| `oracle` | host/port | |
| `firebird` | host + `path` (archivo de BD) | Extensión comunitaria firebird de DuckDB |
| `duckdb` | host/port | |
| `cockroachdb` | host/port | Reutiliza el driver/dialecto de PostgreSQL |
| `yugabytedb` | host/port | Reutiliza el driver/dialecto de PostgreSQL |
| `greenplum` | host/port | Reutiliza el driver/dialecto de PostgreSQL |
| `tidb` | host/port | Reutiliza el driver/dialecto de MySQL |
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
| `hudi` | `path` (URI de tabla) | Motor Hudi de ClickHouse, sin copia |
| `hive` | host/port (metastore) + `mapping.storage` | Backend de almacenamiento en `mapping["storage"]`: hadoop/hdfs/local/s3/azure/adls |
| `hive_s3` | host/port (metastore) + claves S3 de `mapping` | Tipo distinto; siempre almacenamiento S3 |
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
| `airport` | `base_url` (ubicación Flight) | Extensión airport de DuckDB |
| `ingest` | receptor push | Servicios externos hacen POST de eventos JSON |
| **SaaS** | | |
| `sharepoint` | `base_url` o `host` + `mapping` | Autenticación vía `mapping.auth_type` |
| `splunk` | `host`/`port` o `base_url` + `mapping` | |
| **GovData** | | |
| `govdata` | subject + `domain_id` | Modelo `GovDataSource` separado; ver §GovData abajo |
| **Calidad de datos** | | |
| `soda` | host/port apuntando al pgwire de Provisa | Necesita el extra `soda`; Elastic License 2.0, solo autoalojado |
| `great_expectations` | host/port apuntando al pgwire de Provisa | Necesita el extra `gx`; Apache 2.0 |

### Referencia de tipos de origen {: #source-type-reference }

Cada tipo que necesita una configuración poco evidente tiene una entrada breve abajo. Los tipos RDBMS (postgresql, mysql, etc.) usan solo los campos comunes de arriba y no necesitan sección propia.

#### GovData

Los orígenes `govdata` usan un modelo de nivel superior propio, `GovDataSource`, en lugar del `Source` genérico. El acceso se reparte por agrupación de temas.

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

Cada subject se asigna a uno o más esquemas de GovData. Configurar un origen `govdata` con un subject expone automáticamente todos los esquemas de ese subject.

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

Los esquemas `ref` y `geo` se incluyen siempre como esquemas de enlace. No se pueden configurar y no aparecen en la lista de arriba. Use el tema `ALL` para conceder acceso a todos los esquemas.

#### Kafka

La fila `kafka` de `sources:` solo registra el origen. El `details()` de su conector devuelve `{}`; la configuración real está en el bloque de nivel superior `kafka_sources[]`. Kafka es siempre un VIRTUAL_SOURCE: pasa por el motor y no tiene pool directo.

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

**Ventana de tiempo** — `default_window` acota cada consulta a un período reciente, evitando lecturas no acotadas sobre topics de alto volumen. Formato: `1h`, `30m`, `7d`, `60s`. Por defecto `1h`. Se inyecta automáticamente como `WHERE _timestamp >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR`. Los clientes pueden anularlo con su propio filtro `_timestamp` en el argumento `where` de GraphQL.

**Discriminador** — Varias configuraciones de topic pueden apuntar al mismo topic físico de Kafka con distintos valores de `discriminator`, produciendo tipos GraphQL separados. El discriminador se inyecta automáticamente como cláusula WHERE.

**Origen del esquema**

| Valor | Comportamiento |
| ------- | ---------- |
| `registry` | Obtiene el esquema desde el Confluent Schema Registry |
| `manual` | Define las columnas en línea en la configuración (no necesita Schema Registry) |
| `sample` | Descubre automáticamente a partir de mensajes de muestra |

#### Snowflake

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

#### Databricks

`http_path` en `federation_hints` es obligatorio. `password` lleva el token de acceso personal. `catalog` es opcional y va en el SQL o en los hints; el campo `database` no lo contiene.

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

`project` en `federation_hints` es requerido. La autenticación usa `GOOGLE_APPLICATION_CREDENTIALS` (ruta a un archivo de clave de cuenta de servicio) o las Credenciales Predeterminadas de la Aplicación en el entorno del motor.

```yaml
sources:
  - id: my-bigquery
    type: bigquery
    federation_hints:
      project: my-gcp-project     # required
```

#### Fabric / Synapse

Ambos usan T-SQL sobre TDS con autenticación de Azure AD. Inicie sesión con `az login` (desarrollo) o con una identidad administrada (producción); el motor lee las credenciales mediante `DefaultAzureCredential` de `azure-identity`. Los datos de conexión vienen de variables de entorno, `FABRIC_SQL_SERVER` / `FABRIC_DATABASE` (Fabric) o `SYNAPSE_SQL_SERVER` / `SYNAPSE_DATABASE` (Synapse), o de `PROVISA_ENGINE_URL`.

```yaml
sources:
  - id: my-fabric
    type: fabric
    # host/database read from FABRIC_SQL_SERVER / FABRIC_DATABASE when not set here
```

#### ClickHouse

`secure` en `federation_hints` habilita TLS en la interfaz HTTP. El puerto por defecto es `8123` (plano) u `8443` (cuando `secure: "true"`). `schema` en `federation_hints` anula el esquema remoto.

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

#### Hive / Hive S3

`host` y `port` apuntan al metastore Thrift de Hive (puerto predeterminado 9083). Para `hive`, defina `mapping["storage"]` para elegir el backend de almacenamiento de objetos. Una clave obligatoria que falte falla de forma visible, sin valor de reserva.

`hive_s3` es un tipo distinto que siempre declara almacenamiento S3; no necesita `mapping.storage`.

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

#### Redis

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

#### Google Sheets

`spreadsheet_id` en `federation_hints` es requerido. La autenticación usa un SECRET `gsheet` de DuckDB aprovisionado en el momento del attach.

```yaml
sources:
  - id: my-sheet
    type: google_sheets
    federation_hints:
      spreadsheet_id: 1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms
```

#### Orígenes de archivo (csv / parquet / sqlite / files) {: #file-sources-csv-parquet-sqlite-files }

`path` es requerido. `files` rastrea un directorio en busca de archivos CSV, Parquet, XLSX y JSON, exponiendo cada uno como una tabla. Todos los orígenes basados en archivo son VIRTUAL (enrutan a través del motor; sin pool directo).

```yaml
sources:
  - id: orders-csv
    type: csv
    path: /data/orders.csv

  - id: data-lake-dir
    type: files
    path: /data/lake/         # directory; each file becomes a table
```

Los **conjuntos de datos de Kaggle** no se pueden añadir con este archivo, porque necesitan un token válido y el selector de conjuntos de datos del formulario de orígenes (Orígenes → Suscripciones → Kaggle). Un origen de Kaggle exportado a YAML aparece como `type: files` con `kaggle_owner` y `kaggle_ref` en `federation_hints`. Volver a descargar de Kaggle requiere la mutación `refreshKaggleSource` o la actualización en la UI; editar el YAML no lo hace. Consulte [Conjuntos de datos de Kaggle](sources.md#kaggle-datasets) en la referencia de tipos de origen.

#### Orígenes de API / remotos {: #api-remote-sources }

**openapi** — configure `base_url` con la URL base de OpenAPI. El descubrimiento de esquema lee la especificación OpenAPI al inicio.

```yaml
sources:
  - id: payment-api
    type: openapi
    base_url: https://api.payments.example.com/v1
```

**graphql_remote** — configure `base_url`. Claves `mapping` opcionales: `headers` (dict de encabezados estáticos), `forward_client_headers` (bool), `timeout_seconds` (int).

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

**airport** — `base_url` es la ubicación del servidor Arrow Flight. Extensión airport de DuckDB.

```yaml
sources:
  - id: flight-source
    type: airport
    base_url: grpc://flight.internal:8815
```

**websocket / rss** — use `host`, `port`, `path` y `federation_hints`.

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

#### Verificadores de calidad de datos (soda / great_expectations) {: #data-quality-checkers-soda-great_expectations }

Un origen verificador apunta al propio endpoint pgwire de Provisa, así que un solo driver de postgres escanea la vista federada de una tabla respaldada por Snowflake o Iceberg. La identidad del escaneo se declara; nunca se hereda. La política se aplica a esa conexión, y un conjunto de filas filtrado no debe dejar pasar una comprobación sin que nadie lo note. Las claves de conexión salen de `mapping`: `host`, `port`, `database`, `user`, `password`.

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

Cada tabla de resultados lleva `dq_contract`, el YAML del contrato de Soda o el JSON de la suite de Great Expectations palabra por palabra. Las columnas, la marca de agua y las promociones se derivan de él; consulte cómo en [Verificadores de calidad de datos](sources.md#data-quality-checkers).

**Selección al instalar.** El verificador no se enlaza: el escaneo se ejecuta en un intérprete hijo, y la biblioteca solo se instala cuando un operador la nombra. Cada ruta de instalación (`install.sh`, `packaging/linux/first-launch.sh` y el asistente de macOS mediante `PROVISA_DQ_CHECKER`) escribe la elección en `~/.provisa/config.yaml`:

```yaml
dq_checker: none        # none | soda | gx
```

`scripts/provisa` lee esa clave y exporta `PROVISA_EXTRAS`, que `docker-compose.app.yml` pasa como argumento de build al `ARG PROVISA_EXTRAS` del `Dockerfile`:

| `dq_checker` | `PROVISA_EXTRAS` (nivel Docker) | Instalación nativa en venv |
| -------------- | -------------------------------- | --------------------- |
| `none` | `firebase,vector` | `provisa[embedded]` |
| `soda` | `firebase,vector,soda` | `provisa[embedded,soda]` |
| `gx` | `firebase,vector,gx` | `provisa[embedded,gx]` |

Instalar el conjunto de datos de demostración eleva `none` a `gx` y lo indica, porque la configuración de demo registra un suite de Great Expectations sobre `pet_store.pets` y, de lo contrario, su tablero de calidad no tendría nada que mostrar. Nombrar `soda` mantiene `soda`.

Si llega a la demo con pip en lugar de con un instalador, se salta ese paso del asistente, así que el extra `demo` lleva el mismo verificador: `provisa run --demo` necesita `pip install 'provisa[embedded,demo]'` para que su escaneo se ejecute. Sin él, el escaneo informa `data-quality checker 'great_expectations' is not installed` e indica el comando de instalación.

Cualquier otro valor detiene el lanzador, de modo que nunca arranca sin el verificador que pidió el operador. El extra `soda` trae `soda-postgres` y `gx` trae `great-expectations[postgresql]`. Soda Core está bajo la Elastic License 2.0: `config/capabilities.yaml` marca la opción como `cloud_eligible: false`, y el plano alojado la rechaza.

## Dominios {: #domains }

```yaml
domains:
  - id: sales-analytics
    description: Sales operational data
```

## Nomenclatura {: #naming }

```yaml
naming:
  convention: apollo_graphql   # snake, hasura_graphql, apollo_graphql (default)
  domain_prefix: true          # prepend domain_id__ to all GraphQL names
  rules:
    - pattern: "^prod_pg_"
      replace: ""
```

### Convención de nomenclatura {: #naming-convention }

La autoridad de nomenclatura es la única fuente de verdad para los nombres orientados al cliente; los nombres físicos de columna del backend nunca se exponen a los clientes. Cada lenguaje de consulta deriva el nombre de una columna a partir de su `column.alias` si está establecido, o si no, a partir del nombre físico de la columna vía su convención configurada.

La convención GraphQL es uno de tres enums preestablecidos. Las cadenas de forma libre antiguas (`none`, `snake_case`, `camelCase`, `PascalCase`) están obsoletas.

| Preajuste | Por defecto | Nombres de tipo | Nombres de campo | Nombres de mutación |
| -------- | --------- | ------------ | ------------- | ---------------- |
| `apollo_graphql` | sí | PascalCase | camelCase | camelCase |
| `hasura_graphql` | | PascalCase | camelCase | snake_case |
| `snake` | | PascalCase | snake_case | snake_case |

La convención GraphQL por defecto es `apollo_graphql`, que produce nombres de campo y mutación en camelCase. La convención SQL es separada, con `snake_case` por defecto, aplicada mediante `apply_sql_name()`; la convención GraphQL se aplica mediante `apply_gql_name()`, y el nombre CQL se deriva del nombre GraphQL.

`domain_prefix: bool` es una opción ortogonal que se aplica sin importar el preajuste elegido.

El `column.alias` explícito es el nombre canónico: SQL lo usa textualmente sin aplicar convención, GraphQL le aplica su convención, y CQL se deriva del nombre GraphQL.

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

### Prefijo de dominio {: #domain-prefix }

Cuando `domain_prefix: true`, todos los nombres de campo y tipo de GraphQL se prefijan con el ID de dominio usando un separador de doble guion bajo:

| Tabla | Dominio | Nombre de campo |
| ------- | -------- | ----------- |
| `orders` | `sales-analytics` | `sales_analytics__orders` |
| `customer_segments` | `customer-insights` | `customer_insights__customer_segments` |

Esto evita colisiones de nombres cuando distintos dominios tienen tablas con el mismo nombre, y hace que las consultas sean autodescriptivas.

### Reglas de nomenclatura {: #naming-rules }

Reglas de expresión regular aplicadas a los nombres de tabla al generar nombres de campo GraphQL. Se aplican en orden antes de la resolución de unicidad.

## Tablas {: #tables }

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

Una tabla no tiene ningún permiso de escritura por defecto, así que ningún rol puede escribir en ella. Un permiso otorga a un rol una operación (`insert`, `update` o `delete`) sobre la tabla. Una tabla contiene como máximo un permiso por rol y operación.

| Campo del permiso | Significado |
| ----------- | ------- |
| `columns` | Las columnas que un `INSERT` puede listar o un `UPDATE` puede establecer. Un permiso `delete` no nombra ninguna. |
| `rows` | Las filas que un `UPDATE` o `DELETE` puede tocar: un predicado SQL sobre las columnas de la tabla. Un permiso `insert` no tiene. |
| `check` | Lo que debe cumplir cada fila que escribe un `INSERT`, o que deja un `UPDATE`: un predicado SQL. Un permiso `delete` no tiene. |
| `presets` | Columnas que el permiso establece por sí mismo. La sentencia no puede suministrarlas, así que una columna con preset no está también en `columns`. |

Si omite `rows` o `check`, el permiso toma el filtro de filas de lectura del rol sobre la tabla: un rol escribe las filas que lee, salvo que el permiso diga otra cosa. `"TRUE"` significa todas las filas. Las claves antiguas `writable_by` de columna y `column_presets` de tabla se rechazan por su nombre; la configuración no carga hasta que se sustituyan por `write_grants`.

Toda escritura en toda superficie se admite en este orden: el rol tiene el derecho `write`; el rol tiene un permiso para la operación sobre la tabla; cada columna que escribe la sentencia está en el permiso; los presets del permiso se añaden a la sentencia; el predicado `rows` se combina con AND en el `WHERE` de un `UPDATE` o `DELETE`; y `check` se evalúa sobre las filas nuevas antes de enviar nada al origen. Se rechaza una sentencia que `check` no puede decidir. Las columnas del permiso también deciden qué campos de mutación ofrece GraphQL a un rol: un campo por cada operación para la que tiene un permiso, con exactamente esas columnas.

Algunas sentencias necesitan un permiso más amplio. `TRUNCATE` necesita un permiso `delete` sobre todas las filas. `MERGE` necesita un permiso `insert` y uno `update`, ambos sobre todas las filas. `INSERT ... ON CONFLICT DO UPDATE` necesita un permiso `update` sobre todas las filas sin presets. `COPY ... FROM STDIN` necesita un permiso `insert` sin presets.

Los permisos se heredan. Un rol hijo toma, por tabla y operación, el permiso del rol más cercano de su cadena que tenga uno, con el propio primero. Un metarrol mantiene el pliegue de los permisos de sus miembros: la unión de sus columnas, el OR de sus predicados `rows` y `check`, y solo los presets que todos los miembros establecen igual.

En la interfaz de administración, la sección **Escrituras** de una tabla lista los permisos y los edita en conjunto.

### Alias {: #aliases }

Los alias de tabla y columna anulan el nombre GraphQL por defecto. Útiles para:

- Renombrar nombres de base de datos crípticos (p. ej., `tbl_cust_seg` → `customer_segments`)
- Evitar abreviaturas en la capa de API
- Crear un vocabulario limpio, específico del dominio

### Descripciones {: #descriptions }

Las descripciones de tabla y columna se incluyen en el SDL de GraphQL generado. Aparecen en el explorador de documentación de GraphiQL y en las consultas de introspección. Configúrelas en el YAML de configuración o mediante la interfaz de administración.

### Path (extracción computada de JSON) {: #path-computed-json-extraction }

Las columnas pueden extraer valores de una columna de origen JSON/JSONB usando un `path` de notación de punto. Es útil para datos semiestructurados en mensajes de Kafka, documentos de MongoDB o columnas JSONB de PostgreSQL.

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

El formato del path es `source_column.key1.key2...`. El compilador genera `json_extract_scalar(source_column, '$.key1.key2')` en el SQL.

**Impacto en el enrutamiento:** Las columnas path usan operadores JSON de PostgreSQL (`->>`), que son soportados de forma nativa por el enrutamiento directo a PG. Para orígenes que no son PostgreSQL (MySQL, SQL Server, etc.), las consultas con columnas path se enrutan automáticamente a través del motor de federación. Las mutaciones no se ven afectadas ya que las columnas path son campos computados de solo lectura.

### Tipos de enmascaramiento {: #masking-types }

| Tipo | Campos | Descripción |
| ------ | -------- | ------------- |
| `regex` | `pattern`, `replace` | REGEXP_REPLACE (solo columnas de tipo cadena) |
| `constant` | `value` | Reemplazo literal (NULL, 0, MAX, MIN, personalizado) |
| `truncate` | `precision` | DATE_TRUNC (solo columnas de fecha/timestamp) |

## Relaciones {: #relationships }

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

### Automaterialización {: #auto-materialization }

Configure `materialize: true` en una relación para generar automáticamente una vista materializada para JOINs entre orígenes. Esto evita consultas federadas costosas precalculando el resultado del JOIN.

- Solo las relaciones entre orígenes generan MVs (los JOINs del mismo origen ya son rápidos)
- En una relación respaldada por una tabla de unión, la MV cubre el recorrido de dos saltos: salto de origen, salto de la tabla de unión, discriminador y las columnas propias de la tabla de unión como atributos de la arista. La tabla de unión cuenta como una pata, así que una arista es entre orígenes cuando cualquiera de las tres tablas está en un origen distinto
- La MV comienza obsoleta y se puebla mediante el bucle de actualización en segundo plano
- Las mutaciones sobre cualquiera de las tablas de origen marcan la MV como obsoleta para su reactualización
- `refresh_interval` por defecto es de 300 segundos (5 minutos)

## Roles

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

Los roles con `parent_role_id` heredan capacidades, acceso a dominios, concesiones de columnas y objetos, y reglas de RLS del padre, con la regla de RLS propia del hijo para una tabla teniendo precedencia. La cadena se aplana al inicio.

### Capacidades {: #capabilities }

| Capacidad | Descripción |
| ----------- | ------------- |
| `source_registration` | Registrar orígenes de datos |
| `table_registration` | Registrar tablas |
| `relationship_registration` | Definir relaciones |
| `security_config` | Configurar RLS, enmascaramiento |
| `query_development` | Ejecutar consultas |
| `full_results` | Omitir los límites de muestreo |
| `admin` | Todas las capacidades |

## Reglas RLS {: #rls-rules }

```yaml
rls_rules:
  - table_id: orders
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"
    description: Analysts read only their own region's orders.
```

## Vistas materializadas {: #materialized-views }

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

## Vistas (conjuntos de datos computados gobernados) {: #views-governed-computed-datasets }

Las vistas son conjuntos de datos computados definidos en SQL con gobierno completo a nivel de columna. Son el mecanismo gobernado para añadir agregaciones, transformaciones y métricas derivadas al modelo.

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

### Materializada vs. en vivo {: #materialized-vs-live }

- **`materialize: true`**: Provisa crea una tabla mediante CTAS y la actualiza según un horario. Consultas más rápidas, pero los datos pueden estar obsoletos hasta `refresh_interval` segundos.
- **`materialize: false`**: Provisa crea una vista federada. Las consultas siempre devuelven datos en vivo, pero pueden ser más lentas para agregaciones complejas.

Las vistas pasan por el mismo pipeline de gobernanza que las tablas: RLS, enmascaramiento, muestreo y visibilidad por rol. Así nada puede añadir semántica nueva a la plataforma sin la supervisión de un steward.

### Vistas con historial (viaje en el tiempo) {: #views-with-history-time-travel }

Una vista materializada puede conservar cada versión de sus filas y leerse en un momento pasado. Declara cómo las conserva y a qué clave pertenece una versión:

```yaml
views:
  - id: customer-history
    sql: SELECT customer_id, tier, updated_at FROM crm.customers
    domain_id: sales
    materialize: true
    bitemporal_mode: delta            # snapshot | delta
    bitemporal_key: [customer_id]     # required
    bitemporal_source_time: updated_at  # optional: the source's change time
    columns:
      - {name: customer_id, visible_to: [analyst]}
      - {name: tier, visible_to: [analyst]}
      - {name: updated_at, visible_to: [analyst]}
```

| Campo | Obligatorio | Descripción |
| ------- | ---------- | ------------- |
| `bitemporal_mode` | Sí | `snapshot` añade el resultado completo en cada actualización; `delta` añade solo lo que cambió |
| `bitemporal_key` | Sí | Las columnas a las que pertenece una versión |
| `bitemporal_source_time` | No | La columna que indica cuándo cambió cada fila en el origen: el reloj con el que lee el viaje en el tiempo |

**Elegir el reloj.** Con `bitemporal_source_time`, las versiones se ordenan y leen por la hora del propio origen: "a fecha del martes pasado" significa el martes pasado en el origen. Sin él, se ordenan y leen por el momento en que Provisa aterrizó cada cambio, que sigue la cadencia de actualización y no el momento en que la fila cambió en el origen. Las dos opciones son válidas. La hora de carga va bien con una cadencia de actualización frecuente, con datos cuya hora en el origen no importa y con lecturas que deben referirse a la hora de carga, como auditar qué tenía Provisa y cuándo.

Una versión rige desde su momento hasta el de la versión siguiente. Un borrado no tiene hora de cambio en el origen y surte efecto cuando Provisa lo registró.

**Lectura.** Una lectura simple responde la versión de cada clave vigente ahora. La cabecera `X-Provisa-As-Of` lee en un momento, o sobre un rango dado como intervalo ISO 8601 `start/end`:

```
X-Provisa-As-Of: 2026-03-01T00:00:00
X-Provisa-As-Of: 2026-01-01T00:00:00/2026-04-01T00:00:00
```

Un momento responde la versión de cada clave vigente entonces. Un rango responde cada versión vigente en algún momento dentro de él, una fila por versión.

**Según lo registrado.** Una corrección registrada después cambia lo que una vista dice del pasado: la versión vigente el 30 de septiembre es la corregida una vez registrada la corrección. Para leer la vista tal como Provisa la tenía registrada en un momento, antes de cualquier corrección posterior, añada `X-Provisa-As-Recorded`, un instante ISO 8601:

```
X-Provisa-As-Of: 2026-09-30T12:00:00
X-Provisa-As-Recorded: 2026-10-01T00:00:00
```

Toda versión registrada después de ese momento queda fuera, y el as-of elige entonces la versión vigente en su propio momento. Con `X-Provisa-As-Recorded` solo, la lectura responde con la versión de cada clave vigente según lo registrado entonces. Admite un momento, no un rango; un rango o un valor mal formado se rechaza con HTTP 400 (`data.invalid_as_recorded`).

Los dos momentos llegan también a Provisa sin las cabeceras. El momento as-of es el argumento `as_of` de `resolve` (GraphQL, gRPC, MCP), el parámetro de consulta REST `as_of` y la opción `as_of` de un ticket de Arrow Flight, y un rango pasado a `resolve` responde con las `versions` de la fila en lugar de su `row`. El momento as-recorded es el argumento `as_recorded` de `resolve`, el parámetro de consulta REST `as_recorded` y la opción `as_recorded` de un ticket de Arrow Flight. El registro de auditoría de cada instrucción guarda los dos momentos con que leyó, `as_of` y `as_recorded`. Los derechos, las máscaras y las reglas de fila se aplican como en cualquier lectura. Un valor as-of mal formado, o un rango cuyo final no es posterior a su inicio, se rechaza con HTTP 400 (`data.invalid_as_of`).

Toda vista con historial responde tres columnas más, concedidas como lo está su columna de hora de origen (si no, su primera columna de clave):

| Columna | Significado |
| ------- | ------- |
| `source_from` | Cuándo entró en vigor esta versión: su hora de origen, o su hora de carga si no tiene |
| `source_to` | Cuándo entró en vigor la versión siguiente; vacía mientras esta rige |
| `recorded_at` | Cuándo registró Provisa esta versión |

**Hora de origen desde la captura de datos de cambio.** Una tabla aterrizada por captura de datos de cambio lleva su hora de confirmación como `source_committed_at`, para que una vista con historial la nombre como su hora de origen:

- Debezium: la hora de confirmación de la base de datos que lleva cada evento de cambio (`source.ts_ms`). Una fila copiada por una construcción completa de la réplica no tiene evento de cambio, así que su `source_committed_at` está vacío hasta que la fila vuelva a cambiar, salvo que el origen sea PostgreSQL con las marcas de tiempo de confirmación activadas (siguiente punto).
- PostgreSQL con la señal de cambio `native`: la hora de confirmación que PostgreSQL guarda para cada fila (`pg_xact_commit_timestamp(xmin)`), leída por el propio controlador del origen cuando se construye la réplica. PostgreSQL solo la guarda con `track_commit_timestamp = on`. Actívelo para obtener la hora de origen con la señal nativa; sin ello, `source_committed_at` está vacío. Activar o desactivar la opción reconstruye la réplica.

Los temas de Kafka sin sobre de Debezium no llevan hora de confirmación.

### Vistas de solo consulta {: #query-only-views }

Tanto las vistas `materialize: true` como `materialize: false` exponen su tipo GraphQL como de solo consulta. No se generan mutaciones de insert, upsert, update ni delete para relaciones respaldadas por `view_sql`.

## Caché {: #cache }

```yaml
cache:
  enabled: true
  redis_url: ${env:REDIS_URL}
  default_ttl: 300
```

### Jerarquía de caché {: #cache-hierarchy }

Orden de resolución de TTL (el más específico gana): **tabla** > **origen** > **valor global por defecto**. Se usa el primer valor no nulo.

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

Configurar `cache_enabled: false` en un origen desactiva la caché para todas las tablas de ese origen, sin importar el TTL a nivel de tabla. Las claves de caché siempre incluyen `role_id` + valores de contexto RLS para la partición de seguridad.

### Activación por solicitud {: #per-request-opt-in }

La caché de respuestas está desactivada para toda solicitud que no la pida. Una solicitud la activa con:

| Superficie | Activación |
| --- | --- |
| GraphQL (`/data/graphql`, Arrow Flight) | `@cached` en la operación, opcionalmente `@cached(ttl: 60)` |
| SQL (pgwire, Flight SQL, `/data/sql`) | una línea de comentario `-- @provisa cache=true` o `-- @provisa cache_ttl=60` en la sentencia |
| Cypher (`/data/cypher`, la Neo4j Query API, Bolt, Arrow Flight) | una línea de comentario `// @provisa cache=true` o `// @provisa cache_ttl=60` en la consulta |
| gRPC (y el proxy gRPC HTTP) | metadatos de la llamada (encabezados en el proxy) `x-provisa-cache: true` o `x-provisa-cache-ttl: 60` |
| REST, JSON:API y, como alternativa a la indicación en el texto: GraphQL sobre HTTP, `/data/cypher`, la Neo4j Query API | el encabezado de solicitud `Cache-Control: max-age=60` (o `max-stale=60`), o `x-provisa-cache: true` / `x-provisa-cache-ttl: 60` |

Un `ttl` que no sea un entero hace fallar la solicitud. Una solicitud que indica la activación dos veces (en su texto y en un encabezado, o en dos encabezados) con valores distintos falla con `400`; no se prefiere ninguno. `Cache-Control: no-cache` o `no-store` significa sin caché, igual que sin encabezado.

Sin la indicación, la solicitud no lee ni escribe la caché. Con ella, siguen decidiendo los ajustes anteriores: el resultado se guarda en caché solo si cada origen que lee tiene `cache_enabled: true` y el TTL resuelto de cada tabla es mayor que 0. La entrada dura el `ttl` de la solicitud si lo indica; si no, el TTL resuelto más corto de las tablas leídas.

La indicación solo acepta datos más antiguos a cambio de velocidad. Nunca hace una lectura más reciente de lo que permiten los ajustes del operador: la frescura de la carga y de la réplica (`cache_ttl`, `change_signal`), `row_materialize`, las instantáneas `load_protected` y las comprobaciones de frescura se aplican a una solicitud con indicación exactamente igual que a una sin ella. Consulte [Frescura y carga: quién decide](#freshness-and-load-who-decides).

`@noCache` y `-- @provisa no_cache=true` ya no existen: con la caché desactivada por defecto no hay nada que omitir.

Una escritura invalida toda entrada de caché que la organización que escribe tiene para las tablas que escribió; las entradas de otra organización para la misma tabla no se tocan. Si la invalidación falla, la solicitud falla en lugar de dejar entradas obsoletas.

## Frescura y carga: quién decide {: #freshness-and-load-who-decides }

Tres partes influyen en lo reciente que es un resultado y en cuánta carga pone una consulta sobre los sistemas que hay detrás.

- **La fuente de origen.** Lo ideal es que gestione su propia contrapresión: límites de conexiones, tiempos de espera de sentencias, réplicas de lectura.
- **El operador.** Protege la plataforma, Provisa misma, de la contrapresión. Cuando una fuente no puede protegerse, el operador la protege también. Los ajustes para ello están en fuentes y tablas: `load_protected`, `replicate`, los `federation_hints` de la fuente, el umbral de redirección de resultados grandes, el clúster de Kafka al que escriben los sinks y el watermark registrado de cada tabla.
- **El usuario final.** Equilibra velocidad y actualidad, petición a petición.

Los ajustes del operador son el suelo. Una petición puede moverse por encima de él, hacia datos más antiguos o menos carga; activar la caché de respuestas es el ejemplo habitual. Nunca puede bajar de él. Una pista de petición que lo haría se rechaza con un error que nombra el ajuste del operador, sin aplicarse ni descartarse en silencio.

| Entrada de la petición | Qué puede hacer | Se rechaza cuando |
| --- | --- | --- |
| `@route(engine: DIRECT)`, `-- @provisa route=direct` | Elegir el driver directo para una fuente | una tabla que lee la instrucción es `load_protected` o `replicate: 0` |
| `@join`, `@reorder`, `@broadcastSize`, `/*+ ... */` | Fijar una propiedad de sesión del motor que los `federation_hints` de la fuente dejaron libre | cambia una propiedad que fijó el operador |
| `X-Provisa-Redirect-Threshold`, `@redirect(threshold:)` | Redirigir un resultado antes | supera el umbral del operador |
| `@sink(broker:)`, broker de `X-Provisa-Sink` | Repetir el broker del operador | nombra otro broker o no hay `KAFKA_BOOTSTRAP_SERVERS` configurado |
| `@watermark` en una suscripción | Repetir el watermark registrado de la tabla | nombra otra columna o la tabla no tiene ninguno |
| `@cached`, `-- @provisa cache=true` | Servir un resultado en caché más antiguo | nunca; una entrada de caché nunca es más reciente que la lectura que la guardó |

Los rechazos devuelven HTTP 403 con el código `query.operator_floor`, SQLSTATE `42501` por pgwire y `PERMISSION_DENIED` por Flight y gRPC.

Una tabla `load_protected` o con `replicate: 0` nunca se lee en vivo desde una consulta en ningún transporte. Las consultas leen su réplica. Solo la replicación y sus refrescos leen el origen: un refresco ocurre cuando `cache_ttl` o una comprobación de frescura lo pide; sin ninguno, la réplica se construye una vez y luego solo se refresca mediante un feed de cambios o el planificador. En una tabla así con una `off_peak_window` (en la tabla o en su origen, leída en `off_peak_tz`), un refresco solo se ejecuta mientras la ventana está abierta: uno que vence o se solicita fuera de ella espera a la siguiente apertura de la ventana, y una ventana sin `cache_ttl` refresca una vez por apertura. La primera construcción nunca se retiene, porque las lecturas de la tabla la esperan.

### El ajuste `replicate` {: #the-replicate-setting }

`replicate` es un entero en una tabla. Una tabla sin valor toma el de su origen y, cuando ninguno está definido, se aplica el umbral global. Una réplica es una copia de una tabla de origen que se mantiene en el almacén del motor y se refresca según los ajustes de puntualidad de la tabla; no es un flujo continuo de cambios.

| Valor | Nombre | Significado |
| --- | --- | --- |
| sin definir | Predeterminado | Se aplica el umbral global |
| `-1` | Nunca | Se lee en vivo dondequiera que exista una ruta en vivo. Solo se replica en un motor que no puede leer el origen in situ |
| `N` mayor que 0 | Hot-N | Se lee en vivo hasta que la tabla supera N instrucciones gobernadas por intervalo; entonces se sirve desde su réplica |
| `0` | Siempre | Las lecturas provienen de la réplica |

Solo Siempre es una garantía. Nunca y los valores Hot son de mejor esfuerzo. `load_protected` con `replicate: -1` se rechaza al cargar y al guardar la configuración. Un origen con `replicate: 0` no tiene attach en vivo en ningún motor, de modo que cada una de sus tablas se sirve desde su réplica.

El piso se juzga por las tablas que lee una instrucción. Una instrucción que no lee ninguna tabla servida desde una réplica conserva su ruta directa aunque su origen tenga otras tablas replicadas.

Una réplica se indexa por su clave primaria declarada y por las columnas sobre las que se unen sus relaciones declaradas; los índices se construyen después de aterrizar. Dónde esos índices aceleran las uniones, dónde conservar las réplicas y cómo elegir su frescura se explica en [Réplicas](replicas.md).

### Replicación hot {: #hot-replication }

Una tabla cuyo `replicate` no está definido, o es `N` mayor que 0, se lee en vivo hasta que tiene mucha actividad.

```yaml
replication:
  hot_threshold: 100       # statements per interval for a table left at Default
  hot_interval: 60         # seconds: the window the count is taken over, and how often it is judged
  hot_max_rows: 10000000   # a larger table is not replicated for being busy
```

| Ajuste | Variable de entorno | Por defecto |
| --- | --- | --- |
| `replication.hot_threshold` | `PROVISA_REPLICATION_HOT_THRESHOLD` | 100 |
| `replication.hot_interval` | `PROVISA_REPLICATION_HOT_INTERVAL` | 60 |
| `replication.hot_max_rows` | `PROVISA_REPLICATION_HOT_MAX_ROWS` | 10000000 |

Los tres son ajustes del operador: la interfaz de administración los muestra en Hot Tables, y un valor guardado se aplica en la siguiente evaluación.

Cómo se cuenta una tabla:

- Una instrucción gobernada cuenta cada tabla que lee una sola vez, sin importar cuántas veces la lea.
- Una instrucción respondida desde la caché de respuestas no cuenta. No puso carga en el origen.
- Una instrucción que el gobierno rechazó no cuenta.

Una tabla se promueve cuando su recuento alcanza su umbral (su propio `N`, si no el de su origen, si no `hot_threshold`) y se degrada cuando el recuento cae por debajo de la mitad. La promoción solicita la construcción de una réplica; la tabla se lee en vivo hasta que esa construcción termina y después desde la réplica. La degradación devuelve las lecturas al origen de inmediato.

Una tabla no se promueve cuando contiene más filas que `hot_max_rows`, cuando su señal de cambio es `ttl` o `ttl_probe` y ni ella ni su origen declaran un `cache_ttl`, o cuando el nivel hot de Redis ya la gestiona. El resumen de la tabla en la interfaz de administración indica cuál es el caso.

Los recuentos viven en Redis. Con un Redis compartido (`cache.redis_url`) abarcan todos los procesos del despliegue. Con el Redis integrado son por proceso, de modo que la promoción solo se ejecuta cuando un único worker atiende solicitudes; con varios workers y sin Redis compartido no se promueve nada, y el resumen lo indica.

## Autenticación {: #authentication }

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

### Tipos de proveedor de autenticación {: #auth-provider-types }

| Proveedor | Caso de uso | Validación de token |
| ---------- | ---------- | ----------------- |
| `simple` | Desarrollo/pruebas locales. Usuarios definidos en YAML. | JWT firmado con `PROVISA_JWT_SECRET` |
| `firebase` | Firebase Authentication (todos los métodos). | `verify_id_token()` del SDK `firebase-admin` |
| `keycloak` | Keycloak OIDC. Tenant + roles de cliente mapeados. | Validación de JWT basada en JWKS |
| `oauth` | OIDC genérico (Okta, Azure AD, Auth0, PingFederate). | JWKS desde la URL de descubrimiento |
| `basic` | Despliegues autocontenidos. Las cuentas viven en el propio almacén de Provisa. | Contraseña bcrypt, o SCRAM-SHA-256 sobre pgwire |

Las credenciales de superusuario (bloque `superuser`) funcionan con cualquier proveedor y siempre se resuelven al rol admin con todas las capacidades. Se usan para la configuración inicial antes de configurar autenticación externa.

### SCRAM-SHA-256 (`auth.scram`)

```yaml
auth:
  provider: basic
  scram: true
```

Hace que pgwire anuncie SASL con `SCRAM-SHA-256`, de modo que la contraseña se demuestra y nunca se envía en claro. Solo se aplica al proveedor `basic`, porque ningún otro proveedor guarda los verificadores RFC 5802 que SCRAM necesita, y no se ofrece channel binding.

Los verificadores no pueden derivarse de hashes bcrypt existentes. Se escribe uno cada vez que una contraseña pasa en texto plano, así que la primera conexión SCRAM de cada usuario sigue a su siguiente registro, inicio de sesión, cambio de contraseña o restablecimiento por un administrador. Hasta entonces, las conexiones de ese usuario recurren al intercambio en texto claro sobre TLS; el cable no revela quién ha migrado.

### Limitación de intentos de inicio de sesión (`auth.login_throttle`) {: #login-throttling-authlogin_throttle }

```yaml
auth:
  login_throttle:
    max_attempts: 5      # failures within the window before lockout
    window_seconds: 300  # how far back failures are counted
    lockout_seconds: 900 # how long a locked-out subject is refused
```

Activado por defecto con los valores mostrados; el bloque solo los ajusta. El contador se sitúa en la capa de validación de credenciales, así que los fallos por HTTP, pgwire y Bolt se acumulan contra el mismo sujeto y un bloqueo se mantiene en todas las superficies. Es por proceso: varios workers de API permiten cada uno hasta `max_attempts`.

### Tokens de acceso personal {: #personal-access-tokens }

Los PAT no necesitan bloque de configuración. Se aceptan siempre, y su almacén se crea con el resto del esquema del plano de control. Lo configurable es la caducidad que un usuario puede pedir al emitirlo: de 1 a 366 días, o ninguna para un token que no caduca. Consulte [Modelo de seguridad](security.md#personal-access-tokens).

### TLS mutuo {: #mutual-tls }

La verificación de certificados de cliente se configura con variables de entorno, junto a los ajustes de certificado TLS que amplía, y no tiene sitio en `provisa.yaml`.

| Variable | Por defecto | Significado |
| ---------- | --------- | --------- |
| `PROVISA_MTLS_CLIENT_CA` | sin definir | Paquete PEM de las CA permitidas para firmar certificados de cliente. Configurarla activa la verificación de certificado de cliente |
| `PROVISA_MTLS_MODE` | `required` una vez configurada una CA | `required` u `optional` |
| `PROVISA_MTLS_BIND_PRINCIPAL` | `false` | Requiere que el nombre común del certificado sea igual al nombre de usuario con el que se autentica la conexión |

Cada una admite una sobrescritura por protocolo, con los mismos nombres que los ajustes de TLS. Un modo sin CA, o un modo que no es ni `required` ni `optional`, impide el arranque, de modo que el servidor nunca atiende conexiones que el operador cree verificadas sin que lo estén.

### Dirigirse a una org sobre TLS {: #addressing-an-org-over-tls }

Nada que configurar. En un despliegue multiorg, pgwire y Bolt leen la org a partir del nombre de host al que marcó el cliente, transportado en el ClientHello de TLS, exactamente igual que HTTP lo lee del encabezado `Host`. Un cliente que se conecta a `acme.provisa.dev` solicita la org `acme`; la solicitud se rechaza a menos que el principal autenticado sea miembro. Conectarse por dirección IP no solicita ninguna org, que es el caso de toda conexión en un despliegue de una sola org.

### Ejemplo completo de configuración de autenticación (comentado) {: #full-auth-config-example-commented-out }

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

## Mutaciones Upsert {: #upsert-mutations }

Para tablas con clave primaria, Provisa genera campos de mutación `upsert_<table>`. Se compilan a un upsert en el dialecto de destino: `INSERT ... ON CONFLICT (pk) DO UPDATE SET ...` en PostgreSQL, `ON DUPLICATE KEY UPDATE` en MySQL.

```graphql
mutation {
  upsert_orders(objects: [{id: 1, amount: 150.00, region: "us"}]) {
    affected_rows
  }
}
```

Las columnas de conflicto se derivan de los metadatos de la PK. Se aplican todas las reglas de visibilidad de columna y permiso de escritura.

## Distinct On

El argumento `distinct_on` selecciona la primera fila para cada valor distinto de las columnas especificadas. Disponible en los campos de consulta raíz.

```graphql
{
  orders(distinct_on: [region], order_by: [{region: asc, created_at: desc}]) {
    region
    amount
    created_at
  }
}
```

Se compila a `SELECT DISTINCT ON (region) ...` en PostgreSQL. Para dialectos que no son PG, se usa una reserva mediante función de ventana.

## Presets de los permisos de escritura {: #write-grant-presets }

Un permiso de escritura establece columnas por sí mismo en insert y update. Los presets se declaran en la lista `presets` del permiso, por rol y operación.

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

| Origen | Comportamiento |
| -------- | ---------- |
| `session` | Inyecta el valor de la variable de sesión indicada (`name`) de la solicitud |
| `now` | Inyecta el momento de la escritura |
| `literal` | Inyecta una constante (`value`) |

Los presets se añaden a la sentencia durante la compilación, antes de generar el SQL. La sentencia no puede suministrar una columna con preset, y una columna con preset no está en el tipo de entrada de la mutación.

## Roles heredados {: #inherited-roles }

Los roles pueden heredar de un único rol padre mediante `parent_role_id`. La cadena se aplana al inicio. Un hijo tiene la unión de las capacidades y el acceso a dominios de sus ancestros; una columna, métrica, función o webhook concedido a un ancestro queda concedido al hijo; y las reglas de RLS de un ancestro se aplican al hijo por tabla, empezando por el rol más cercano, con la regla propia del hijo para una tabla reemplazando la del padre.

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

Se admite herencia multinivel. Las capacidades y domain_access explícitos del rol hijo se combinan con los del padre. Un padre debe ser un rol existente, no puede ser el propio rol y no puede cerrar un ciclo; cualquiera de estos casos se rechaza al guardar.

## Disparadores programados {: #scheduled-triggers }

Disparadores basados en cron que llaman a una URL de webhook según un horario. Usa APScheduler.

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

Las tareas programadas se gestionan mediante la interfaz de administración (activar/desactivar) o la mutación de administración `toggle_scheduled_task`.

Un disparador puede activarse sobre una tabla registrada en lugar de una programación: `on: {table, event}` nombra la tabla como `source_id.schema.table` y el evento, `change`, `error` o `quarantine`. Un disparador `command` ejecuta un comando registrado como su `role`, mediante el despacho gobernado del propio comando, y le pasa como argumentos lo que lo activó. Un cambio pasa `table` y, donde la señal de cambio lo lleva (Kafka, Debezium), el lote cargado como `changes`. Un evento `error` o `quarantine` pasa `table`, `event`, `reason` y `error`. Un disparador cuyo comando declara un argumento que el disparador no pasa se rechaza al guardar. Un cambio se activa una vez por ráfaga que informa su señal de cambio, o cuando llega una escritura hecha a través de Provisa; una tabla servida desde su réplica se activa cuando una construcción carga filas distintas de la anterior. La propia escritura de un comando es un cambio, así que los disparadores se encadenan. Cada ejecución se registra con su hora, su resultado (`succeeded`, `refused` o `failed`), su error y lo que la activó, y la consulta de administración `scheduledTasks` la lista. Solo se activan los disparadores de prod, y no se instala nada en ninguna base de datos de origen.

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

OrderBy usa el formato `{column: direction}` con un enum de dirección de 6 valores:

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

El ordenamiento por relación se admite mediante objetos anidados:

```graphql
{
  orders(order_by: [{customers: {name: asc}}]) {
    id
    customers { name }
  }
}
```

## Observabilidad {: #observability }

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

### Filtros de telemetría {: #telemetry-filters }

Provisa tiene dos vías de exportación OTLP independientes: su collector interno y el endpoint de soporte opcional de Provisa. Cada vía tiene su propio filtro. Los filtros se ejecutan antes de que los spans salgan del proceso, y los objetos span originales nunca se modifican.

**`telemetry_filter`** — controla qué llega a su colector interno.

| Clave | Tipo | Por defecto | Descripción |
| ----- | ------ | --------- | ------------- |
| `redact_sql_literals` | bool | `false` | Reemplaza los literales de cadena y numéricos en `db.statement` con `?` |
| `redact_attributes` | list[str] | `[]` | Claves de atributo eliminadas por completo de cada span |

**`support_telemetry_filter`** — controla qué llega al endpoint de soporte de Provisa. La redacción de literales SQL está activada por defecto en esta ruta, ya que los datos de consulta le pertenecen a usted.

| Clave | Tipo | Por defecto | Descripción |
| ----- | ------ | --------- | ------------- |
| `redact_sql_literals` | bool | `true` | Reemplaza los literales de cadena y numéricos en `db.statement` con `?` |
| `redact_attributes` | list[str] | `[]` | Claves de atributo eliminadas por completo de cada span |

Un `db.statement` redactado. Con `redact_sql_literals: true`, este atributo de span:

```yaml
db.statement: SELECT * FROM orders WHERE region = 'us-west' AND amount > 500
```

se convierte en:

```yaml
db.statement: SELECT * FROM orders WHERE region = ? AND amount > ?
```

### Endpoint de soporte {: #support-endpoint }

`support_endpoint` (o la variable de entorno `PROVISA_SUPPORT_OTLP_ENDPOINT`) reenvía telemetría al soporte de Provisa para diagnóstico. Si no está definido, ningún dato sale de su infraestructura por esta vía. El filtro de soporte funciona con independencia del interno, así que puede ocultar los literales SQL en ambas exportaciones y seguir compartiendo con soporte los tiempos de los spans y los datos de errores.

### Detección de protocolo de endpoint {: #endpoint-protocol-detection }

Provisa selecciona OTLP/HTTP u OTLP/gRPC a partir del esquema de la URL del endpoint. Las URL que comienzan con `http://` o `https://` usan OTLP/HTTP, con `/v1/traces`, `/v1/metrics` y `/v1/logs` añadidos automáticamente. Cualquier otro esquema usa OTLP/gRPC con `insecure=True`.

## Motor de federación {: #federation-engine }

Configurar un motor de federación es opcional. El predeterminado es `duckdb`: sin configuración, dentro del proceso, sin ningún servicio externo. Elija otro motor cuando necesite escala MPP o quiera reutilizar un almacén de datos que ya tiene.

Precedencia: variable de entorno `PROVISA_ENGINE` → campo de configuración `federation_engine` persistido en la interfaz de administración → `duckdb`. Los cambios surten efecto al reiniciar el servicio.

### Resumen de motores {: #engine-overview }

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

### Referencia de motores {: #engine-reference }

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

En proceso; sin servicio externo. El motor por defecto. `PROVISA_DATA_DIR` controla dónde vive el almacén integrado (`~/.provisa` por defecto).

```bash
PROVISA_ENGINE=duckdb   # or omit — this is the default
```

El almacén de materialización es por defecto `~/.provisa/materialize.duckdb`. Es el único motor cuyo almacén predeterminado no es PostgreSQL.

#### clickhouse (integrado) / clickhouse-server {: #clickhouse-embedded-clickhouse-server }

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

#### Motores de bases de datos relacionales (mysql, mariadb, oracle, mssql, db2, redshift, greenplum, cockroachdb, yugabytedb, opengauss, tidb, singlestore, vertica, exasol, teradata, saphana, sapase, sqlanywhere, monetdb, firebird) y `sqlalchemy` {: #relational-database-engines-mysql-mariadb-oracle-mssql-db2-redshift-greenplum-cockroachdb-yugabytedb-opengauss-tidb-singlestore-vertica-exasol-teradata-saphana-sapase-sqlanywhere-monetdb-firebird-and-sqlalchemy }

Una clave por cada base de datos relacional accesible por red, todas sobre el mismo runtime de solo aterrizaje (sin federación a orígenes externos): cada origen aterriza en el almacén y se consulta ahí. La clave selecciona la base de datos; `PROVISA_ENGINE_URL` lleva el DSN que su dialecto admite. `sqlalchemy` es el comodín para una base de datos sin clave propia. No se ofrecen almacenes embebidos en archivo (SQLite, Access), porque el servidor debe ser accesible por red.

```bash
PROVISA_ENGINE=mysql
PROVISA_ENGINE_URL="mysql+pymysql://user:pass@host:3306/db"
```

El almacén de materialización usa por defecto `TENANT_DATABASE_URL`.

### Almacén de materialización {: #materialization-store }

Cuando un origen no puede conectarse (attach) en vivo (no hay conector ATTACH para el motor seleccionado), aterriza en el almacén de materialización del motor. Orden de resolución: `PROVISA_MATERIALIZE_URL` explícito → valor por defecto declarado por el motor → error explícito (sin reserva silenciosa).

DuckDB declara su archivo integrado (`~/.provisa/materialize.duckdb`) como su valor por defecto. Todos los demás motores usan por defecto `TENANT_DATABASE_URL` (PostgreSQL). Anule cualquier motor con `PROVISA_MATERIALIZE_URL`.

El almacén decide lo que hacen los índices de una réplica en las uniones: un almacén PostgreSQL indexa las réplicas y un motor que ejecuta SQL en él lee esos índices, mientras que DuckDB une las réplicas de su archivo integrado sobre una copia por instrucción. Consulte [Réplicas](replicas.md#use-a-relational-database-as-the-replica-store-where-the-engine-runs-sql-in-it).

### Hints de federación por origen {: #per-source-federation-hints }

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

Para orígenes de Google Cloud, defina `GOOGLE_APPLICATION_CREDENTIALS` con la ruta del archivo de clave de su cuenta de servicio. Para Fabric y Synapse, inicie sesión con `az login` (desarrollo) o una identidad administrada (producción); el motor lee las credenciales mediante `DefaultAzureCredential` de `azure-identity`.

## Variables de entorno {: #environment-variables }

| Variable | Por defecto | Descripción |
| ---------- | --------- | ------------- |
| `PROVISA_CONFIG` | `config/provisa.yaml` | Ruta del archivo de configuración |
| `TENANT_DATABASE_URL` | `postgresql+psycopg://provisa:provisa@localhost:5432/provisa` | URI del almacén del plano de control (SQLAlchemy async); admite `sqlite+pysqlite://…` / `duckdb://…` para el almacén de escritorio integrado |
| `PLATFORM_DATABASE_URL` | — | URI del registro de plataforma (directorio de tenants, registro de motores); requerida al inicio, sin reserva |
| `PROVISA_REDIS_EMBEDDED` | — | `1`/`true` usa fakeredis integrado en lugar de un servidor Redis — sin Docker |
| `PG_HOST` | `localhost` | Host de PostgreSQL |
| `PG_PORT` | `5432` | Puerto de PostgreSQL |
| `PG_DATABASE` | `provisa` | Base de datos de PostgreSQL |
| `PG_USER` | `provisa` | Usuario de PostgreSQL |
| `PG_PASSWORD` | `provisa` | Contraseña de PostgreSQL |
| `PROVISA_ENGINE` | `duckdb` | Clave del motor de federación |
| `PROVISA_ENGINE_URL` | — | URL de conexión para motores dirigidos por URL (Snowflake, Databricks, ClickHouse Server, BigQuery, SQLAlchemy) |
| `PROVISA_MATERIALIZE_URL` | — | Anula el DSN del almacén de materialización (por defecto, el valor declarado por el motor) |
| `PROVISA_DATA_DIR` | `~/.provisa` | Directorio de datos para el almacén DuckDB integrado |
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
| `PROVISA_RETRY_BUDGET_SECS` | `30` | Presupuesto de reintento de lectura de nivel 1 en segundos; backoff exponencial con jitter completo |
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
| `PROVISA_MTLS_CLIENT_CA` | — | Paquete PEM de las CA permitidas para firmar certificados de cliente; configurarla activa la verificación de certificado de cliente en pgwire, Bolt, gRPC y Flight |
| `PROVISA_MTLS_MODE` | `required` una vez configurada una CA | `required` u `optional`; cualquier otro valor se niega a iniciar |
| `PROVISA_MTLS_BIND_PRINCIPAL` | `false` | Requiere que el nombre común del certificado sea igual al nombre de usuario autenticante |
| `PROVISA_BOLT_ALLOWED_ORIGINS` | — | Sitios separados por comas con permiso para abrir un WebSocket Bolt desde un navegador; sin definir rechaza todo origen de navegador |
| `PROVISA_EXTRAS` | `firebase,vector` | Extras de pyproject integrados en la imagen de la app; `scripts/provisa` lo deriva de `dq_checker` en `~/.provisa/config.yaml` |
| `PROVISA_DQ_CHECKER` | `none` | Solo instalador: `none`/`soda`/`gx`, leído por `first-launch.sh` en modo no interactivo y escrito en `config.yaml` como `dq_checker` |
| `ANTHROPIC_API_KEY` | — | Clave de API de Claude (descubrimiento) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | Anula `observability.endpoint` |
| `OTEL_SERVICE_NAME` | `provisa` | Anula `observability.service_name` |
| `OTEL_LOG_LEVEL` | `WARNING` | Anula `observability.log_level` |
| `OTEL_COMPACT_BATCH_SIZE` | `10` | Anula `observability.compact_batch_size` |
| `OTEL_SPAN_EXPORT_DELAY_MILLIS` | `1000` | Retardo de vaciado del procesador de spans por lote |
| `PROVISA_SUPPORT_OTLP_ENDPOINT` | — | Anula `observability.support_endpoint` |
