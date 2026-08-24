# Tipos de origen
## Modelo de ejecución
Toda consulta se ejecuta en última instancia a través del motor de federación, que provee federación entre todos los orígenes. Los orígenes se dividen en tres categorías según su conectividad. [tool-verified: `provisa/core/models.py` lines 84–132] (REQ-550)

| Categoría | Tiene controlador directo | Tiene conector federado | Ejemplos |
| --- | --- | --- | --- |
| **Con capacidad directa** | Sí | Sí | PostgreSQL, MySQL, MariaDB, SingleStore, SQL Server, Oracle, DuckDB |
| **Solo federación** | No | Sí | Redshift, Druid, Exasol, Hive, Iceberg, Delta Lake, Hive (respaldado por S3) |
| **Lectura directa (réplica)** | Sí | Sí | Snowflake, Databricks, ClickHouse — el controlador lee los datos y crea una réplica; las consultas se ejecutan contra la réplica en el motor activo |
| **Materializar → Federación** | No | No | REST/OpenAPI, GraphQL remoto, gRPC, Neo4j Cypher, SPARQL, WebSocket, RSS, CSV, SQLite, Parquet, Ingest (receptor push), GovData, SharePoint, Splunk |

Los orígenes **con capacidad directa** ejecutan consultas de un solo origen mediante su controlador nativo (menos de 100 ms), evitando el motor de federación (REQ-027, REQ-229). Conservan el soporte completo del conector y participan en la federación cuando se combinan con otros orígenes (REQ-028).

Los orígenes **solo federación** siempre se consultan a través de la capa de federación. No existe un controlador directo (REQ-229).

Los orígenes de **lectura directa (réplica)** tienen un DirectDriver que lee del almacén de datos de forma nativa (nativo en Arrow cuando está disponible), crea una réplica en el almacén de materialización del motor activo, y luego las consultas se ejecutan contra esa réplica. Consulte [Almacenes de datos como orígenes con nombre](#almacenes-de-datos-como-origenes-con-nombre).

Los orígenes de **materialización** no tienen conector federado. Provisa obtiene sus datos (al inicio o en el momento de la consulta) y los almacena en caché como Parquet en S3 o en PostgreSQL, haciéndolos accesibles para el motor de federación en consultas entre orígenes (REQ-309).

---

## Todos los orígenes
Provisa registra **53** tipos de origen. Las tablas siguientes cubren los 53; el índice es el recuento. [tool-verified: `provisa/core/models.py` `SourceType`]

| # | Grupo | Tipos de origen |
| --- | --- | --- |
| 1–13 | [RDBMS](#rdbms) | `postgresql`, `mysql`, `mariadb`, `singlestore`, `sqlserver`, `oracle`, `duckdb`, `cockroachdb`, `yugabytedb`, `greenplum`, `tidb`, `firebird`, `airport` |
| 14–20 | [Almacenes de datos en la nube](#cloud-data-warehouses) | `snowflake`, `bigquery`, `databricks`, `redshift`, `fabric`, `synapse`, `trino` |
| 21–25 | [Analítica / OLAP](#analytics-olap) | `clickhouse`, `druid`, `exasol`, `elasticsearch`, `pinot` |
| 26–30 | [Data lake / Formatos de tabla abiertos](#data-lake-open-table-formats) | `iceberg`, `delta_lake`, `hudi`, `hive`, `hive_s3` |
| 31–33 | [NoSQL](#nosql) | `mongodb`, `cassandra`, `redis` |
| 34–36 | [Streaming](#streaming) | `kafka`, `websocket`, `rss` |
| 37 | [Receptor push](#push-receiver) | `ingest` |
| 38–39 | [Grafo y semántica](#graph-semantic) | `neo4j`, `sparql` |
| 40–43 | [Basados en archivos](#file-based) | `sqlite`, `csv`, `parquet`, `files` |
| 44–45 | [Observabilidad y otros](#observability-other) | `google_sheets`, `prometheus` |
| 46–47 | [SaaS empresarial](#enterprise-saas-connectors) | `sharepoint`, `splunk` |
| 48–50 | [Orígenes de API](#api-sources) | `openapi`, `graphql_remote`, `grpc_remote` |
| 51 | [GovData](#govdata) | `govdata` |
| 52–53 | [Verificadores de calidad de datos](#data-quality-checkers-req-1443) | `soda`, `great_expectations` |

Referencia de todos los tipos de origen que soporta Provisa. "Controlador directo" significa que las consultas de un solo origen se ejecutan de forma nativa contra el origen (menos de 100 ms) (REQ-027). "Nombre del conector" es el conector federado que se usa cuando el origen participa en JOIN entre varios orígenes (REQ-028). [tool-verified: `provisa/core/source_registry.py` `SOURCE_TO_DIALECT`; `provisa/federation/trino_connectors.py` `trino_connector_name`]

### RDBMS

| Tipo de origen | Controlador directo | Nombre del conector | Dialecto | Mutaciones |
| ------------ | -------------- | ----------------- | ----------------- | ----------- |
| `postgresql` | asyncpg | postgresql | postgres | Sí |
| `mysql` | aiomysql | mysql | mysql | Sí |
| `mariadb` | aiomysql | mariadb | mysql | Sí |
| `singlestore` | — | singlestore | singlestore | Federada |
| `sqlserver` | aioodbc | sqlserver | tsql | Sí |
| `oracle` | oracledb | oracle | oracle | Sí |
| `duckdb` | duckdb | memory | duckdb | Sí |
| `cockroachdb` | asyncpg (pg wire) | postgresql | postgres | Sí |
| `yugabytedb` | asyncpg (pg wire) | postgresql | postgres | Sí |
| `greenplum` | asyncpg (pg wire) | postgresql | postgres | Sí |
| `tidb` | aiomysql (mysql wire) | mysql | mysql | Sí |
| `firebird` | — | — (extensión de DuckDB) | — | No |
| `airport` | — | — (extensión de DuckDB) | — | No |

Las bases de datos compatibles a nivel de wire reutilizan el controlador JDBC, el controlador nativo asíncrono y el dialecto de un wire base — CockroachDB, YugabyteDB y Greenplum usan el wire de PostgreSQL; TiDB usa el wire de MySQL. Solo necesitan entradas de registro, sin código de conector nuevo. [tool-verified: `provisa/core/source_registry.py` `_PG_WIRE_TYPES`, `_MYSQL_WIRE_TYPES`] (REQ-950)

`firebird` (Firebird 3/4/5) y `airport` (servidor Arrow Flight) son tipos de origen registrados a los que se accede en el lugar a través de extensiones de la comunidad de DuckDB cuando DuckDB es el motor activo — sin controlador directo, sin conector federado. [tool-verified: `provisa/core/models.py` lines 44, 93] (REQ-899)

### Almacenes de datos en la nube {#cloud-data-warehouses}
[tool-verified: `executor/drivers/snowflake.py`, `executor/drivers/databricks.py`, `executor/drivers/registry.py`]

| Tipo de origen | Controlador directo | Nombre del conector | Dialecto | Mutaciones | Notas |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `snowflake` | SnowflakeDriver | snowflake | snowflake | Federada | Lee mediante snowflake-connector-python; crea réplica; `account`/`warehouse`/`role` en `federation_hints` (REQ-988) |
| `bigquery` | — | bigquery | bigquery | Federada | Sin DirectDriver; se accede mediante el motor de federación o BigQuery engine ATTACH |
| `databricks` | DatabricksDriver | delta_lake | databricks | Federada | Lee mediante databricks-sql-connector (Cloud Fetch, Arrow); crea réplica; `http_path` obligatorio en `federation_hints` (REQ-987) |
| `redshift` | — | redshift | redshift | Federada | — |
| `fabric` | MssqlWarehouseDriver | — | tsql | Federada | Microsoft Fabric Warehouse; T-SQL sobre TDS, autenticación Azure AD; crea réplica (REQ-995) |
| `synapse` | MssqlWarehouseDriver | — | tsql | Federada | Azure Synapse SQL; T-SQL sobre TDS, autenticación Azure AD; crea réplica (REQ-995) |
| `trino` | SQLAlchemyDriver | — | — | Federada | Coordinador remoto Trino/Presto leído mediante el dialecto trino de SQLAlchemy; crea réplica en cualquier motor (REQ-994) |

### Analítica / OLAP {#analytics-olap}
[tool-verified: `executor/drivers/clickhouse.py`]

| Tipo de origen | Controlador directo | Nombre del conector | Dialecto | Mutaciones | Notas |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `clickhouse` | ClickHouseDriver | clickhouse | clickhouse | Federada | Lee mediante clickhouse-connect (HTTP); `secure: "true"` en `federation_hints` para TLS (REQ-986) |
| `druid` | — | druid | druid | No | — |
| `exasol` | — | exasol | exasol | No | — |
| `elasticsearch` | — | elasticsearch | — | No | Las propiedades del conector provienen del DSL de mapeo del tipo [tool-verified: `trino_connectors.py:309`] |
| `pinot` | — | pinot | — | No | Conector `pinot` de Trino; `pinot.controller-urls` = host:port del controlador de Pinot [tool-verified: `trino_connectors.py:199`] |

### Data lake / Formatos de tabla abiertos {#data-lake-open-table-formats}
Estos tipos de origen son solo de federación — sin controlador directo, sin dialecto. [tool-verified: `LAKE_ONLY_SOURCES` in `provisa/core/source_registry.py`] (REQ-229)

| Tipo de origen | Nombre del conector | Viaje en el tiempo | Notas |
| ------------ | ----------------- | ------------- | ------- |
| `iceberg` | iceberg | Sí (argumento `as_of`, REQ-372) | — |
| `delta_lake` | delta_lake | Sí (argumento `as_of`, REQ-372) | — |
| `hive` | hive | No | — |
| `hudi` | — (motor `Hudi` de ClickHouse, sin copia — REQ-1178) | No | Sin conector federado; se alcanza en su sitio cuando ClickHouse es el motor activo |
| `hive_s3` | hive | No | Hive respaldado por S3 |

### NoSQL

`mongodb`, `cassandra` y `redis` tienen conectores de Trino (`redis` construye sus propiedades a partir del DSL de mapeo del tipo). [tool-verified: `provisa/federation/trino_connectors.py`; `provisa/core/models.py`] (REQ-017, REQ-1097)

| Tipo de origen | Nombre del conector | Mutaciones |
| ------------ | ----------------- | ----------- |
| `mongodb` | mongodb | No |
| `cassandra` | cassandra | No |
| `redis` | redis | No |

### Streaming

| Tipo de origen | Mecanismo | Mutaciones |
| ------------ | ----------- | ----------- |
| `kafka` | Conector Kafka federado; esquema mediante Confluent Schema Registry (Avro, Protobuf, JSON Schema), definición manual o inferencia por muestreo (REQ-147, REQ-150) | Solo sink (REQ-176) |
| `websocket` | Feed WebSocket externo — conecta, se suscribe, recibe eventos; los resultados se materializan (REQ-338) | No |
| `rss` | Feed RSS 2.0 / Atom — sondea, marca de agua por pubDate/updated; los resultados se materializan (REQ-342, REQ-343) | No |

### Receptor push {#push-receiver}
| Tipo de origen | Mecanismo | Mutaciones |
| ------------ | ----------- | ----------- |
| `ingest` | Servicios externos envían eventos JSON mediante POST; los resultados se materializan (REQ-331, REQ-335) | No |

### Grafo y semántica {#graph-semantic}
| Tipo de origen | Mecanismo | Mutaciones |
| ------------ | ----------- | ----------- |
| `neo4j` | Cypher mediante API HTTP, resultados almacenados en caché en PostgreSQL (REQ-295) | No |
| `sparql` | SPARQL 1.1 POST, resultados almacenados en caché en PostgreSQL (REQ-297) | No |

### Basados en archivos {#file-based}
Dos mecanismos cubren los archivos. Ambos usan el campo `path` en lugar de `host`/`port`. [tool-verified: `provisa/core/models.py`] (REQ-553)

**Orígenes de archivo único** — `sqlite`, `csv`, `parquet` apuntan `path` a un solo archivo.

| Tipo de origen | Transportes | Mutaciones |
| --- | --- | --- |
| `sqlite` | local | Sí |
| `csv` | local | No |
| `parquet` | local, `s3://` | No |

Los buckets privados necesitan credenciales (región y claves de AWS desde el entorno). Para CSV sobre `s3://` o `http(s)://`, o para registrar muchos archivos a la vez, use el origen `files`. [tool-verified: `provisa/file_source/source.py`]

**Origen `files`** — apunta `path` a un glob, lo recorre de forma recursiva y registra el directorio como un catálogo federado de tablas. Lee muchos formatos sobre muchos transportes; los conjuntos siguientes provienen del conector de archivos (kenstott/calcite fork). [tool-verified: `provisa/core/catalog.py` `files` branch and `provisa/core/models.py` `SOURCE_TO_CONNECTOR`; format and transport lists from the calcite `file` adapter — `FileSchema.java`, `storage/StorageProviderFactory.java`]

| Formatos | Transportes |
| --- | --- |
| CSV, TSV, JSON, YAML, Excel (XLS/XLSX), Parquet, Arrow, y documentos convertidos a tablas — HTML, Markdown, DOCX, PPTX | Sistema de archivos local, HTTP(S), `s3://`, `hdfs://`, `ftp://`/`ftps://`, `sftp://`, `iceberg://`, SharePoint (REST y Microsoft Graph) |

```yaml
- id: sales_files
  type: files
  path: s3://bucket/sales/**/*.csv   # glob; local and http(s):// also supported
```

### Observabilidad y otros {#observability-other}
`prometheus` tiene un conector de Trino (propiedades construidas a partir del DSL de mapeo del tipo). `google_sheets` es un tipo de origen registrado sin conector de Trino y se materializa a través del pipeline de caché de API. [tool-verified: `provisa/federation/trino_connectors.py:314`; `provisa/core/models.py` lines 87–88]

| Tipo de origen | Nombre del conector | Mutaciones |
| ------------ | ----------------- | ----------- |
| `google_sheets` | — (materializado) | No |
| `prometheus` | prometheus | No |

### Conectores SaaS empresariales {#enterprise-saas-connectors}
SharePoint y Splunk se registran mediante conectores de Apache Calcite (kenstott/calcite fork). Ninguno tiene controlador directo — Provisa materializa sus filas lanzando el servidor pgwire de Calcite incluido en el conector (`pgwire-sharepoint`, `pgwire-splunk`), conectándose a él como un endpoint genérico de PostgreSQL, y llevando las filas al almacén de materialización para la federación (REQ-954). Ambos conectores siempre habilitan la coincidencia de nombres sin distinción de mayúsculas y minúsculas, coincidiendo con la semántica propia de cada producto en ese aspecto (REQ-725, REQ-730). [tool-verified: `provisa/core/models.py` lines 99–100; `provisa/federation/trino_connectors.py` lines 223–286]

#### `sharepoint`

Las listas de SharePoint se enumeran como esquemas y se exponen como tablas consultables (REQ-726, REQ-731). Dos métodos de autenticación: `CLIENT_CREDENTIALS` (predeterminado) y basado en certificado mediante un certificado PFX (REQ-727). Los valores secretos en `mapping` se resuelven a través del motor de secretos antes de llegar al conector (REQ-729). [tool-verified: `provisa/federation/trino_connectors.py` lines 230–252]

| Campo de origen | Propiedad del conector | Notas |
| --- | --- | --- |
| `base_url` o `host` | `site-url` | URL del sitio de SharePoint |
| `username` | `client-id` | ID de cliente de la app de Azure |
| `password` | `client-secret` | Secreto de cliente de la app de Azure |
| `database` | `tenant-id` | UUID del inquilino de Azure |
| `mapping.auth_type` | `auth-type` | `CLIENT_CREDENTIALS` (predeterminado) o `CERTIFICATE` |
| `mapping.certificate_path` | `certificate-path` | Ruta del PFX cuando `auth_type: CERTIFICATE` |
| `mapping.certificate_password` | `certificate-password` | Contraseña del PFX |

Cuando el conector no expone `information_schema.columns`, registre la tabla con definiciones de columna explícitas (obtenidas de la API de Microsoft Graph) mediante la mutación `registerTable` (REQ-732).

```yaml
- id: hr-sharepoint
  type: sharepoint
  base_url: https://kenstott.sharepoint.com
  username: ${env:SP_CLIENT_ID}
  password: ${env:SP_CLIENT_SECRET}
  database: ${env:SP_TENANT_ID}
  mapping:
    auth_type: CLIENT_CREDENTIALS
```

#### `splunk`

Los resultados de búsqueda de Splunk son consultables como tablas (por ejemplo, `internal_server`) (REQ-721). La URL del conector proviene de `base_url`, o se construye como `https://{host}:{port}` con un puerto predeterminado de `8089` (REQ-722). Autenticación: cuando `mapping.use_token` es `true` (el predeterminado), `password` se pasa como el token de la API; cuando es `false`, `username` y `password` se pasan como credenciales separadas (REQ-723). [tool-verified: `provisa/federation/trino_connectors.py` lines 262–286]

| Campo de origen | Propiedad del conector | Notas |
| --- | --- | --- |
| `base_url` / `host` + `port` | `url` | `base_url`, o en su defecto `https://host:port` (puerto predeterminado 8089) |
| `password` | `token` o `password` | token cuando `use_token: true` |
| `username` | `user` | solo cuando `use_token: false` |
| `database` | `app` | restringe a una app de Splunk |
| `mapping.datamodel_filter` | `datamodel-filter` | filtra a un modelo de datos |
| `mapping.disable_ssl_validation` | `disable-ssl-validation` | para certificados autofirmados (REQ-724) |

```yaml
- id: ops-splunk
  type: splunk
  host: splunk
  port: 8089
  password: ${env:SPLUNK_TOKEN}
  mapping:
    use_token: true
    disable_ssl_validation: true
```

### Orígenes de API {#api-sources}
Registre cualquier endpoint HTTP como una tabla consultable. [tool-verified: `provisa/core/models.py` `SourceType` enum] (REQ-314, REQ-307, REQ-322)

| Tipo de API | Detección | Inferencia de columnas |
| --------- | ----------- | ----------------- |
| `openapi` | Análisis de especificación OpenAPI (REQ-314, REQ-316) | Primitivos → nativo, objetos → JSONB |
| `graphql_remote` | Introspección de esquema (REQ-307, REQ-308) | Primitivos → nativo, objetos → JSONB |
| `grpc_remote` | Reflexión del servidor (REQ-322, REQ-325) | Primitivos → nativo, objetos → JSONB |

Las respuestas de la API se obtienen, se almacenan en caché en PostgreSQL (TTL configurable) y se exponen como tipos GraphQL (REQ-309, REQ-318, REQ-327). Las tablas en caché participan en consultas federadas como cualquier otro origen (REQ-313).

**Reglas de JSONB**: Las columnas complejas (objetos, arreglos) almacenadas como JSONB no son filtrables (REQ-119). El acceso a subcampos usa la extracción `->>` en SQL (REQ-151). Las relaciones se declaran entre tablas usando columnas de clave foránea escalares — las columnas de blob JSONB no son destinos de join. Use la promoción de JSONB para convertir campos anidados en columnas escalares nativas cuando se necesite filtrar o hacer join sobre ellos (REQ-119).

### GovData

Datos abiertos del gobierno de EE. UU. El acceso está particionado por agrupación temática. [tool-verified: `provisa/core/models.py` lines 543–609]

Cada origen `govdata` selecciona un tema. Ese tema determina qué esquemas de GovData se exponen. Los esquemas `ref` y `geo` siempre se incluyen como esquemas de enlace — no se listan por tema, pero siempre están presentes. [tool-verified: `provisa/core/models.py` line 562–563 comment]

| Tema | Esquemas expuestos |
| --------- | ----------------- |
| `COMMERCE` | `sec`, `patents` |
| `ECONOMY` | `econ` |
| `EDUCATION` | `census`, `edu` |
| `HEALTH` | `health` |
| `CYBER` | `cyber_threat`, `cyber_vuln` |
| `PUBLIC_SAFETY` | `crime` |
| `ENVIRONMENT` | `lands` |
| `WEATHER` | `weather` |
| `GOVERNMENT` | `fedregister`, `fec` |
| `ALL` | Todos los esquemas anteriores |

```yaml
sources:

  - id: federal-commerce
    type: govdata
    subject: COMMERCE
    domain_id: federal-analytics
    description: U.S. commerce and securities data
```

| Campo | Obligatorio | Predeterminado | Descripción |
| ------- | ---------- | --------- | ------------- |
| `id` | Sí | — | Identificador único |
| `subject` | Sí | — | Uno de los valores de tema anteriores |
| `domain_id` | Sí | — | Dominio al que pertenece este origen |
| `description` | No | `""` | Descripción legible por humanos |

### Verificadores de calidad de datos (REQ-1443) {#data-quality-checkers-req-1443}
Un verificador de calidad de datos es un tipo de origen, no un subsistema. Su resultado de escaneo es dato: un resultado de verificación es una observación, así que ingresa por la ruta ordinaria de origen y hereda cadencia, frescura, eventos, linaje, gobierno, RLS, grid y exportación de cualquier otro origen. [tool-verified: `provisa/core/models.py` lines 110–116 `SourceType.soda`, `SourceType.great_expectations`; `provisa/events/source_loader.py` `make_dq_loader`]

Se soportan dos, y la elección es tanto una elección de licencia como una elección de funcionalidad.

| Tipo de origen | Dialecto del contrato | Extra | Licencia | Plano cloud alojado |
| ------------ | ----------------- | ------- | --------- | -------------------- |
| `soda` | YAML de contrato Soda | `pip install .[soda]` (`soda-postgres`) | Elastic License 2.0 | Rechazado — ver más abajo |
| `great_expectations` | JSON de suite de expectativas | `pip install .[gx]` (`great-expectations[postgresql]`) | Apache 2.0 | Permitido |

Elastic License 2.0 prohíbe ofrecer el software a terceros como servicio alojado o gestionado, y ejecutar Soda dentro del plano SaaS en nombre de un inquilino es exactamente eso. `config/capabilities.yaml` lleva esa división como `cloud_eligible: false` en la opción `soda`, y el plano alojado lee ese indicador. Un despliegue alojado que quiera Soda accede a un endpoint Soda provisto por el operador, que el operador mismo ejecuta. [tool-verified: `config/capabilities.yaml` lines 197–203]

Provisa no empaqueta ni enlaza nada. El escaneo se ejecuta en un intérprete hijo (`python -m provisa.dq.worker`), que es el único lugar donde se importa `soda_core` o `great_expectations`, de modo que un verificador source-available nunca llega al proceso del servidor y un fallo del verificador mata un subproceso en lugar del bucle de eventos. [tool-verified: `provisa/dq/runner.py` `build_command`, `run_contract`]

**El origen apunta al propio endpoint pgwire de Provisa.** Eso es lo que permite que un solo controlador de postgres verifique una tabla respaldada por Snowflake o Iceberg: el verificador escanea la vista federada, no el sistema subyacente. Como la política se aplica a esa conexión, la identidad de escaneo se declara en lugar de heredarse — un conjunto de filas filtrado nunca debe producir una verificación que pase en silencio.

```yaml
sources:

  - id: dq
    type: soda
    domain_id: sales-analytics
    description: Soda contract scans over the governed estate
    mapping:
      host: localhost
      port: 5439          # Provisa's pgwire endpoint
      database: provisa
      user: dq_scanner    # the scan identity, declared explicitly
      password: ${env:PROVISA_DQ_PASSWORD}
```

**Una tabla de resultados por contrato, y el contrato es todo el registro.** La tabla lleva `dq_contract` — el texto del contrato tal cual — y nada más sobre su forma. Las columnas, la marca de agua y las promociones son todas derivadas. [tool-verified: `provisa/dq/registration.py` `derive_checker_table`]

```yaml
tables:

  - source_id: dq
    schema_name: quality
    table_name: orders_scan
    domain_id: sales-analytics
    change_signal: ttl_probe
    cache_ttl: 3600
    columns:
      - name: scan_id          # declared only to carry visible_to; replaced at parse
        visible_to: [analyst, admin]
    dq_contract: |
      dataset: provisa/sales/orders
      columns:
        - name: customer_id
          checks:
            - missing:
                threshold:
                  metric: percent
                  must_be_less_than: 1
      checks:
        - row_count:
            must_be_greater_than: 0
```

Lo que el registro deriva de ese texto:

- **Linaje.** El contrato ya nombra su conjunto de datos destino, así que el registro lo analiza de la misma forma en que `extract_inputs` analiza SQL (REQ-939) y lo resuelve a la tabla gobernada. Una sola definición, sin una segunda copia que pueda divergir. Un contrato que nombra un conjunto de datos no gobernado falla de forma ruidosa en el registro en lugar de dejar filas que nadie pidió.
- **Columnas.** El sobre de resultados es del verificador, no del operador — 16 columnas incluidas, desde `scan_id` hasta `diagnostics`. Las columnas declaradas se leen solo por su `visible_to`, que debe ser unánime, y luego se reemplazan. [tool-verified: `provisa/dq/results.py` `_ENVELOPE`, `results_columns`]
- **Marca de agua.** `scan_time` se convierte en la marca de agua, lo que hace que el aterrizaje sea un append (REQ-982). El historial de escaneos se acumula sin ningún subsistema de historial.
- **Promociones.** `freshness_max_timestamp` y `dataset_rows_tested` se promueven desde el jsonb `diagnostics` como columnas tipadas (REQ-119). Agregue más de la misma forma en que lo haría en cualquier otra columna jsonb. [tool-verified: `provisa/dq/results.py` `DQ_PROMOTIONS`]

La temporización no introduce campos nuevos. `change_signal` junto con `cache_ttl` dan la cadencia de sondeo; `mv_debounce_quiet` y `mv_debounce_max_delay` colapsan una ráfaga ascendente en un solo escaneo (REQ-963); un grano de calendario lo vuelve periódico (REQ-962); `expected_events` retiene el escaneo hasta que sus entradas estén frescas dentro de la ventana (REQ-961). El bucle de sondeo es el programador de escaneos.

`outcome` es uno de `pass`, `fail`, `warn`, `error`, `skipped`. Ninguno de ellos es un veredicto — la aplicación (enforcement), si se desea, es una declaración separada posterior: un preflight o una vista materializada sobre los resultados aterrizados. Como una observación aterrizada no lleva ninguna obligación de determinismo (REQ-964), aquí son admisibles verificaciones no deterministas que nunca podrían estar en una puerta de preflight — puntuación de anomalía, cambio de ventana móvil, frescura contra el momento actual.

El contrato se redacta en la UI, en el panel de calidad de datos de la superficie de edición de tabla, y el texto crudo del contrato ahí es siempre la fuente de verdad. Una ejecución en seco (dry run) ejecuta el contrato contra la tabla en vivo y muestra los resultados sin aterrizarlos — así es como se detecta un contrato cuyo nombre de conjunto de datos se resolvió en un lugar inesperado y que de otro modo no aterrizaría más que filas que pasan.

---

## Conectores personalizados (REQ-1177)
Los motores de federación nativos — Postgres, DuckDB y ClickHouse — obtienen accesibilidad a un nuevo tipo de origen cuando un operador declara un conector para él en `config/custom_connectors.yaml`. No se requiere código. [tool-verified: `provisa/federation/custom_connectors.py` `load_custom_connectors`; `provisa/federation/engine.py` `build_pg_engine`, `build_duckdb_engine`, `build_clickhouse_engine`]

La extensibilidad de conectores en sí misma es anterior a esto. El motor Trino lleva mucho tiempo siendo extensible en su propia capa — un conector JDBC genérico parametrizado por tipo de origen, un cuerpo `.properties` de catálogo por tipo, y los propios plugins de conector Trino personalizados de Provisa (Splunk, SharePoint, Calcite). [tool-verified: `provisa/federation/trino_connectors.py` `_TrinoJdbcConnector`, `_TRINO_JDBC_TYPES`; `trino/plugins/trino-splunk`, `trino/plugins/trino-sharepoint`, `trino/plugins/trino-calcite`] REQ-1177 lleva esa misma extensibilidad basada en configuración a los dos motores nativos sin clúster, que antes tenían un conjunto de conectores fijo.

La configuración se distribuye vacía. Los conectores integrados cubren el alcance listo para usar; todo lo que hay en este archivo lo escribe el operador. [tool-verified: `config/custom_connectors.yaml` line 52: `connectors: []`] Defina `PROVISA_CUSTOM_CONNECTORS` para apuntar a una ruta distinta (útil para pruebas).

### Tipos de descriptor
| Motor | Tipo | Mecanismo | Qué proporciona el descriptor |
| --- | --- | --- | --- |
| `postgres` | `pg_fdw` | SQL/MED (estándar ISO) | `extension`, `server_options`, `user_mapping`, `supports_import`, `table_options`, `remote_schema` |
| `duckdb` | `duckdb_attach` | INSTALL/LOAD + ATTACH | `extension`, `probe_symbol`, `attach_template`, `remote_schema` |
| `duckdb` | `duckdb_scan` | INSTALL/LOAD + vista de escaneo | `extension`, `probe_symbol`, `scan_template` |
| `clickhouse` | `clickhouse_database` | `CREATE DATABASE ENGINE=…` (expone automáticamente cada tabla remota) | `ch_engine`, `engine_template` |
| `clickhouse` | `clickhouse_table` | `CREATE TABLE ENGINE=…` por tabla (columnas desde el registro) | `ch_engine`, `engine_template` (puede llevar `{table}`) |
| `clickhouse` | `clickhouse_scan` | `CREATE TABLE ENGINE=…`, ClickHouse infiere el esquema | `ch_engine`, `engine_template` |

**Postgres es genérico.** SQL/MED es un estándar ISO, por lo que todo FDW conforme comparte la misma forma de DDL: `CREATE SERVER … FOREIGN DATA WRAPPER <fdw> OPTIONS(…)`, opcionalmente `CREATE USER MAPPING`, y luego `IMPORT FOREIGN SCHEMA` (cuando `supports_import: true`) o una `CREATE FOREIGN TABLE` explícita por tabla (cuando es `false`). Un descriptor `pg_fdw` solo proporciona la variación específica del FDW — nombre de la extensión, claves de opciones del servidor, claves de mapeo de usuario, indicador de importación, opciones de tabla. Cualquier FDW conforme al estándar es, por lo tanto, manejable solo desde la configuración. [tool-verified: `provisa/federation/custom_connectors.py` `GenericPgFdwConnector.details` lines 98–125]

**DuckDB soporta dos mecanismos.** Una extensión que expone un catálogo mediante ATTACH usa `duckdb_attach`; una que expone una función de tabla de lectura usa `duckdb_scan`. Una extensión que no encaja en ninguno de los dos patrones no es compatible. [tool-verified: `provisa/federation/custom_connectors.py` `GenericDuckDbAttachConnector`, `GenericDuckDbScanConnector`]

**ClickHouse soporta tres mecanismos**, uno por cada forma de motor de integración: un motor DATABASE relacional que expone automáticamente cada tabla remota (`clickhouse_database`, por ejemplo Redis/MySQL), un motor por tabla cuyas columnas provee el registro (`clickhouse_table`, por ejemplo el puente JDBC/ODBC — el `engine_template` puede llevar un marcador `{table}` que el runtime vincula), y un motor de archivo/lake/URL cuyo esquema infiere ClickHouse (`clickhouse_scan`, por ejemplo HDFS/URL). SQLite (motor DATABASE, archivo, sin servidor) y Hudi (lakehouse, sin copia) vienen listos para usar. [tool-verified: `provisa/federation/custom_connectors.py` `GenericClickHouseDatabaseConnector`, `GenericClickHouseTableConnector`, `GenericClickHouseScanConnector`; `provisa/federation/clickhouse_connectors.py` `ClickHouseSqliteConnector`, `ClickHouseHudiConnector`] (REQ-1178)

Un valor `kind` desconocido falla de forma ruidosa al inicio — un error tipográfico en el descriptor no debe dejar un tipo de origen inalcanzable en silencio. [tool-verified: `provisa/federation/custom_connectors.py` `load_custom_connectors` lines 178–197]

### Verificación de disponibilidad
La disponibilidad se verifica en el momento del attach contra el catálogo de detección estándar de cada motor:

- **Postgres** — verifica `pg_extension`, luego `pg_available_extensions`. [tool-verified: `provisa/federation/connector_duckdb.py` `_probe_pg_extension` lines 333–344]
- **DuckDB** — ejecuta `INSTALL`/`LOAD` y verifica `duckdb_functions()` para el `probe_symbol` declarado. [tool-verified: `provisa/federation/connector_duckdb.py` `_DuckDBExtensionConnector.probe` lines 160–180]
- **ClickHouse** — verifica `system.table_engines` para el `ch_engine` declarado; su ausencia en el build falla de forma ruidosa. [tool-verified: `provisa/federation/custom_connectors.py` `_probe_clickhouse_engine`]

Una extensión declarada que no se puede instalar falla de forma ruidosa. Sin omisión silenciosa, sin valor de respaldo. Un conector cuya verificación falla simplemente no está activo para ese despliegue.

### Variables de plantilla
Todo valor de `server_options`, valor de `user_mapping`, `attach_template` y `scan_template` puede usar marcadores `{field}`. Campos disponibles: [tool-verified: `provisa/federation/custom_connectors.py` `_source_fields` lines 53–63]

`{id}`, `{host}`, `{port}`, `{database}`, `{username}`, `{password}`, `{path}`, `{schema_name}`, `{table_name}`, además de cualquier clave de `federation_hints`. Las plantillas de attach de DuckDB también reciben `{alias}` — el alias de catálogo interno que Provisa asigna a la base de datos adjunta.

Una plantilla que referencia un campo desconocido falla de forma ruidosa en el momento del attach, exponiendo un desajuste entre descriptor y origen antes de que un DDL roto llegue al motor.

### Ejemplos
**Postgres — MongoDB mediante `mongo_fdw` (sin importación de esquema; columnas provistas por tabla)**

```yaml
# config/custom_connectors.yaml
connectors:
  - engine: postgres
    source_type: mongodb
    kind: pg_fdw
    extension: mongo_fdw
    mechanism: attach_r
    server_options:
      address: "{host}"
      port: "{port}"
    user_mapping:
      username: "{username}"
      password: "{password}"
    supports_import: false
    table_options:
      database: "{database}"
      collection: "{table_name}"
```

**DuckDB — Archivos Excel mediante `read_xlsx` (función de tabla de escaneo)**

```yaml
  - engine: duckdb
    source_type: xlsx
    kind: duckdb_scan
    extension: excel
    install_from_community: false
    probe_symbol: read_xlsx
    scan_template: "read_xlsx('{path}')"
```

[tool-verified: `config/custom_connectors.yaml` commented examples, lines 26–50]

Con cualquiera de los dos descriptores en su lugar, registrar un origen con el `source_type` declarado lo enruta a través del conector personalizado, sujeto a una verificación exitosa. No se necesita ningún otro cambio de configuración.

---

## Almacenes de datos como orígenes con nombre
Snowflake, Databricks y ClickHouse se pueden registrar como orígenes con nombre independientemente de cuál motor de federación esté activo. [tool-verified: `executor/drivers/snowflake.py` (REQ-988), `executor/drivers/databricks.py` (REQ-987), `executor/drivers/clickhouse.py` (REQ-986)]

Una vez registrado, Provisa lee el almacén de datos mediante el DirectDriver del origen y crea una réplica en el almacén de materialización del motor activo. La consulta se ejecuta luego contra esa réplica. Esto difiere de la ruta tradicional con capacidad directa (asyncpg, aiomysql), donde el motor se evita por completo — aquí el motor sigue ejecutando la consulta, pero contra una réplica local en lugar de por cable hacia el almacén de datos en cada solicitud.

Las lecturas son nativas en Arrow cuando el almacén de datos lo soporta: Databricks usa Cloud Fetch, Snowflake usa `fetch_arrow_table`, y ClickHouse usa la interfaz HTTP columnar nativa.

Los parámetros de conexión extendidos que los campos estándar `host`/`port`/`username`/`password` no pueden llevar van en `federation_hints`:

```yaml
sources:
  - id: my-databricks
    type: databricks
    host: my-workspace.azuredatabricks.net
    password: ${env:DATABRICKS_TOKEN}
    federation_hints:
      http_path: /sql/1.0/warehouses/xxxx   # required — the SQL Warehouse connection detail

  - id: my-snowflake
    type: snowflake
    host: org.snowflakecomputing.com
    username: svc_provisa
    password: ${env:SNOWFLAKE_PASSWORD}
    federation_hints:
      account: myorg-myaccount    # required — Snowflake account identifier
      warehouse: COMPUTE_WH       # optional — virtual warehouse to use
      role: PROVISA_ROLE          # optional — Snowflake role

  - id: my-clickhouse
    type: clickhouse
    host: ch.example.com
    port: 8123
    database: analytics
    username: default
    password: ${env:CLICKHOUSE_PASSWORD}
    federation_hints:
      secure: "true"              # optional — enables TLS on the HTTP interface
```

El registro como origen con nombre es independiente de seleccionar ese mismo almacén de datos como motor de federación. Un origen Snowflake sobre un motor DuckDB crea una réplica en DuckDB, no en Snowflake.

Los datos de objeto/lake en la nube (archivos parquet, csv, iceberg, delta_lake en S3 / GCS / R2) son un tipo de origen independiente que se adjunta en el lugar cuando el motor activo tiene un conector ATTACH para ese tipo. No se crea réplica alguna — el motor escanea el almacenamiento de objetos directamente. Las credenciales de esos orígenes también van en `federation_hints`:

```yaml
sources:
  - id: r2-events
    type: parquet
    path: s3://my-bucket/events/2026/*.parquet
    federation_hints:
      access_key_id: ${env:R2_ACCESS_KEY}
      secret_access_key: ${env:R2_SECRET}
      account_id: ${env:R2_ACCOUNT_ID}     # Cloudflare R2 account (S3-compatible)
```

---

## Campos de configuración de origen
Todos los orígenes comparten un conjunto común de campos. [tool-verified: `provisa/core/models.py` `Source` class, lines 138–204]

| Campo | Obligatorio | Predeterminado | Descripción |
| ------- | ---------- | --------- | ------------- |
| `id` | Sí | — | Identificador único; alfanumérico con guiones/guiones bajos |
| `type` | Sí | — | Tipo de origen (ver tablas anteriores) |
| `host` | No | `""` | Nombre de host o IP |
| `port` | No | `0` | Número de puerto |
| `database` | No | `""` | Nombre de la base de datos |
| `username` | No | `""` | Nombre de usuario |
| `password` | No | `""` | Contraseña; use `${env:VAR}` para resolución de secretos |
| `path` | No | `null` | Ruta de archivo o URI en la nube para orígenes basados en archivo y de objeto/lake |
| `base_url` | No | `null` | URL base para orígenes OpenAPI |
| `pool_min` | No | `1` | Tamaño mínimo del pool de conexiones (REQ-052) |
| `pool_max` | No | `5` | Tamaño máximo del pool de conexiones (REQ-052) |
| `use_pgbouncer` | No | `false` | Enruta las conexiones a través de PgBouncer (REQ-053) |
| `pgbouncer_port` | No | `6432` | Puerto de PgBouncer (REQ-053) |
| `cache_enabled` | No | `true` | Habilita el almacenamiento en caché de respuestas de API |
| `cache_ttl` | No | `null` | TTL de la caché en segundos; hereda el predeterminado global cuando es null |
| `cache_catalog` | No | `null` | Catálogo federado para la caché de API; por defecto usa el catálogo propio del origen |
| `cache_schema` | No | `api_cache` | Esquema dentro del catálogo de caché |
| `naming_convention` | No | `null` | Anula la convención de nombres global para este origen (REQ-194) |
| `federation_hints` | No | `{}` | Propiedades de sesión pasadas al motor de federación, y parámetros de conexión extendidos para orígenes de almacén de datos (REQ-278, REQ-281) |
| `mapping` | No | `{}` | Configuración de conector específica del tipo para orígenes NoSQL y SaaS (por ejemplo, `auth_type` de SharePoint, `use_token` de Splunk) (REQ-251) |
| `allowed_domains` | No | `[]` | Restringe el origen a dominios específicos; vacío = sin restricción |
| `description` | No | `""` | Descripción legible por humanos |

---

## Orígenes Kafka
Los tópicos de Kafka se configuran por separado bajo `kafka_sources`, indexados por el `id` de origen de un origen `kafka` registrado. [tool-verified: `config/provisa.yaml` lines 138–151] (REQ-147)

```yaml
kafka_sources:

  - id: kafka-support
    topics:

      - id: tickets
        topic: support.tickets
        domain_id: sales-analytics
        description: "Inbound support tickets"
        default_window: 1h
        columns:

          - name: id
          - name: subject
          - name: status
          - name: created_at
```

| Campo | Descripción |
| ------- | ------------- |
| `id` | Debe coincidir con el `id` de un origen con `type: kafka` |
| `topics[].id` | Nombre lógico de este tópico dentro de Provisa |
| `topics[].topic` | Nombre del tópico de Kafka |
| `topics[].domain_id` | Dominio al que pertenece este tópico |
| `topics[].description` | Descripción legible por humanos |
| `topics[].default_window` | Ventana de tiempo predeterminada para consultas con ventana (por ejemplo, `1h`) (REQ-148) |
| `topics[].columns` | Definiciones de columna para el esquema del tópico (REQ-150) |

---

## Visibilidad de columnas
El campo `visible_to` en cada columna es una lista de ID de rol que pueden ver esa columna. [tool-verified: `provisa/core/models.py` `Column` class line 248; `config/provisa.yaml` lines 39–51]

```yaml
columns:

  - name: email
    visible_to: [admin]        # only admin role sees this column

  - name: region
    visible_to: [admin, analyst]  # both roles see this column
```

Las columnas omitidas de la lista `visible_to` de un rol no aparecen en el esquema GraphQL de ese rol y no se pueden consultar ni referenciar en filtros (REQ-039).

---

## Relaciones
Las relaciones conectan dos tablas registradas y aparecen como campos anidados en GraphQL. [tool-verified: `provisa/core/models.py` `Relationship` class lines 323–343; `config/provisa.yaml` lines 103–110] (REQ-019)

```yaml
relationships:

  - id: orders-to-customers
    source_table_id: orders
    target_table_id: customers
    source_column: customer_id
    target_column: id
    cardinality: many-to-one
```

| Campo | Obligatorio | Descripción |
| ------- | ---------- | ------------- |
| `id` | Sí | Identificador único para esta relación |
| `source_table_id` | Sí | Tabla que contiene la clave foránea |
| `target_table_id` | Sí | Tabla referenciada; vacío para relaciones calculadas |
| `source_column` | Sí | Columna en la tabla de origen |
| `target_column` | Sí | Columna en la tabla de destino; vacío para relaciones calculadas |
| `cardinality` | Sí | `many-to-one` o `one-to-many` (REQ-019) |
| `materialize` | No | Crea automáticamente una vista materializada para joins entre orígenes (REQ-158) |
| `refresh_interval` | No | Intervalo de actualización de la vista materializada en segundos (predeterminado: 300) |
| `target_function_name` | No | Nombre de función de base de datos para relaciones calculadas |
| `function_arg` | No | Qué argumento de la función recibe el valor de la columna de origen |
| `alias` | No | Tipo de relación legible por humanos (por ejemplo, `WORKS_FOR`) |
| `graphql_alias` | No | Nombra el campo SDL que esta relación expone en el tipo padre. Cuando está ausente, el nombre se deriva del `field_name` de la tabla destino y la cardinalidad de la relación. [tool-verified: `provisa/compiler/schema_gen.py:1050`] |
| `disable_cypher` | No | Cuando es `true`, excluye esta relación de las aristas del grafo Cypher |
| `source_json_key` | No | Extrae esta clave de la columna de origen como un objeto JSON antes del JOIN |

Valores de cardinalidad [tool-verified: `provisa/core/models.py` `Cardinality` enum, lines 79–81]:

- `many-to-one` — cada fila de origen se mapea a una fila de destino (FK a PK)
- `one-to-many` — cada fila de origen se mapea a varias filas de destino (inverso de la anterior)

---

## Reglas de seguridad de nivel de fila
Las reglas RLS inyectan cláusulas `WHERE` en el momento de la consulta, con alcance a un rol y opcionalmente a una tabla o dominio. [tool-verified: `provisa/core/models.py` `RLSRule` class lines 391–395; `config/provisa.yaml` lines 128–131] (REQ-041)

```yaml
rls_rules:

  - table_id: orders          # applies to orders table only
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"

  - domain_id: sales-analytics  # applies to every table in domain (REQ-402)
    role_id: analyst
    filter: "tenant_id = current_setting('provisa.tenant_id')"
```

Cuando existen tanto una regla de nivel de dominio como una de nivel de tabla para el mismo rol, la regla de nivel de tabla tiene prioridad (REQ-403).

| Campo | Obligatorio | Descripción |
| ------- | ---------- | ------------- |
| `table_id` | Condicional | Tabla a la que se aplica la regla; mutuamente excluyente con `domain_id` |
| `domain_id` | Condicional | Dominio al que se aplica la regla; se aplica a todas las tablas del dominio (REQ-402) |
| `role_id` | Sí | Rol al que se aplica esta regla |
| `filter` | Sí | Predicado SQL inyectado en `WHERE`; puede referenciar variables de sesión (REQ-041) |

---

## Funciones y webhooks
### Funciones de base de datos
Registra una función de base de datos y la expone como consulta o mutación GraphQL. [tool-verified: `provisa/core/models.py` `Function` class lines 423–438; `config/provisa.yaml` lines 152–164] (REQ-205)

Los orígenes de base de datos también pueden autodescubrir sus procedimientos almacenados y funciones a partir del catálogo del proveedor (`pg_proc`, `information_schema.routines`, o equivalentes del proveedor), eliminando la necesidad de registrar cada uno manualmente. La detección lee `prokind` y `provolatile`: las funciones inmutables/estables se registran como relaciones parametrizadas (los argumentos del procedimiento se convierten en parámetros de consulta, con la misma forma que las tablas OpenAPI GET), y los procedimientos volátiles se registran como mutaciones/funciones rastreadas. Las rutinas descubiertas pasan por el gobierno de Etapa 2 de forma idéntica a las registradas manualmente. [tool-verified: `provisa/api/admin/introspect.py:541`, `provisa/api/admin/introspect.py:593`] (REQ-887)

```yaml
functions:

  - name: get_customers_by_region
    source_id: sales-pg
    schema: public
    function_name: get_customers_by_region
    returns: customers
    domain_id: sales-analytics
    description: "Returns customers filtered by region"
    visible_to: [admin, analyst]
    kind: query
    arguments:

      - name: p_region
        type: String
```

| Campo | Obligatorio | Predeterminado | Descripción |
| ------- | ---------- | --------- | ------------- |
| `name` | Sí | — | Nombre del campo GraphQL |
| `source_id` | Sí | — | Origen que contiene la función |
| `schema` | No | `public` | Esquema de base de datos |
| `function_name` | Sí | — | Nombre real de la función de base de datos |
| `returns` | Sí | — | ID de la tabla registrada que devuelve la función (REQ-207) |
| `arguments` | No | `[]` | Lista de definiciones de argumento `{name, type}` (REQ-211) |
| `visible_to` | No | `[]` | Roles que pueden llamar a esta función |
| `writable_by` | No | `[]` | Roles que pueden llamarla como mutación |
| `domain_id` | No | `""` | Dominio al que pertenece esta función |
| `description` | No | `null` | Descripción del campo GraphQL |
| `kind` | No | `mutation` | `"query"` o `"mutation"` (REQ-205) |

### Webhooks

Expone un endpoint HTTP externo como consulta o mutación GraphQL. [tool-verified: `provisa/core/models.py` `Webhook` class lines 441–455; `config/provisa.yaml` lines 166–178] (REQ-209)

```yaml
webhooks:

  - name: notify_support
    url: http://localhost:9999/notify
    method: POST
    timeout_ms: 3000
    domain_id: sales-analytics
    description: "Send a support notification"
    visible_to: [admin]
    kind: mutation
    arguments:

      - name: message
        type: String
```

| Campo | Obligatorio | Predeterminado | Descripción |
| ------- | ---------- | --------- | ------------- |
| `name` | Sí | — | Nombre del campo GraphQL |
| `url` | Sí | — | URL del endpoint del webhook |
| `method` | No | `POST` | Método HTTP |
| `timeout_ms` | No | `5000` | Tiempo límite de la solicitud en milisegundos |
| `returns` | No | `null` | ID de tabla registrada, o null para tipo en línea |
| `inline_return_type` | No | `[]` | Lista de campos `{name, type}` para formas de retorno personalizadas (REQ-210) |
| `arguments` | No | `[]` | Lista de definiciones de argumento `{name, type}` |
| `visible_to` | No | `[]` | Roles que pueden llamar a este webhook |
| `domain_id` | No | `""` | Dominio al que pertenece este webhook |
| `description` | No | `null` | Descripción del campo GraphQL |
| `kind` | No | `mutation` | `"query"` o `"mutation"` |

---

## Autenticación
La autenticación se configura bajo la clave `auth`. [tool-verified: `provisa/core/models.py` `AuthConfig` class lines 467–477] (REQ-120)

| Proveedor | Descripción |
| ---------- | ------------- |
| `none` | Sin autenticación; todas las solicitudes se tratan como el `default_role` |
| `firebase` | Firebase Authentication; requiere `project_id` y `service_account_key` (REQ-121) |
| `keycloak` | Keycloak OIDC (REQ-122) |
| `oauth` | OAuth 2.0 genérico (REQ-123) |
| `simple` | Usuario/contraseña sin proveedor externo (REQ-124) |

```yaml
auth:
  provider: firebase
  assignments_source: provisa   # "claims" or "provisa"
  default_role: analyst
  default_assignments:

    - role_id: analyst
      domain_id: "*"
  firebase:
    project_id: ${env:FIREBASE_PROJECT_ID}
    service_account_key: ${env:FIREBASE_SERVICE_ACCOUNT_KEY}
```

`assignments_source: claims` lee las asignaciones de rol desde los claims del JWT. `assignments_source: provisa` las lee desde el almacén de asignaciones propio de Provisa. [tool-verified: `provisa/core/models.py` line 476] (REQ-551)

---

## Enrutamiento de ejecución
**Ejecución directa** — Las consultas RDBMS de un solo origen se enrutan al controlador nativo para lograr una latencia menor a 100 ms (REQ-027). Los orígenes requieren tanto una entrada en `SOURCE_TO_DIALECT` como una en `SOURCE_TO_CONNECTOR` para soportar esta ruta (REQ-229).

**Ejecución federada** — Las consultas de varios orígenes y los orígenes sin controlador directo se enrutan a través del motor de federación (REQ-028). Provisa incluye un motor de federación embebido; apunte a su propio clúster compatible para despliegues a gran escala (REQ-226).

**Estadísticas** — Al registrar, Provisa ejecuta `ANALYZE` contra cada tabla publicada para preparar el optimizador basado en costos (conteo de filas, fracción de nulos, valores distintos, mínimo/máximo). Los fallos se registran en el log y no bloquean el registro (REQ-275).

---

## Orígenes de grafo y semántica
### Neo4j

Registre una base de datos de grafos Neo4j como un origen consultable. Los stewards escriben consultas Cypher que proyectan valores escalares; Provisa almacena en caché los resultados y los expone como tipos GraphQL (REQ-295).

Las consultas Cypher deben usar accesores de propiedad en la cláusula `RETURN` (`RETURN n.id AS id, n.name AS name`) — devolver objetos de nodo se rechaza en el momento del registro (REQ-296).

```bash
# Register via admin API (no YAML config required)
POST /admin/sources/neo4j
{
  "source_id": "graph",
  "host": "neo4j",
  "port": 7474,
  "database": "neo4j"
}

# Register a table (preview + validate before persisting)
POST /admin/sources/neo4j/graph/tables
{
  "table_name": "person_skills",
  "cypher": "MATCH (p:Person)-[:HAS_SKILL]->(s:Skill) RETURN p.name AS name, s.skill AS skill, p.experience AS years",
  "ttl": 300
}
```

El endpoint de vista previa (`POST /admin/sources/neo4j/{id}/preview`) devuelve filas de muestra y bloquea el registro si el Cypher devuelve objetos de nodo (REQ-296).

### SPARQL

Registre cualquier triplestore compatible con SPARQL 1.1 (Apache Jena Fuseki, Virtuoso, Stardog, etc.) como un origen consultable (REQ-297).

Las consultas deben ser consultas `SELECT`. Los nombres de variable en la cláusula `SELECT` se convierten automáticamente en nombres de columna (REQ-297).

```bash
# Register via admin API
POST /admin/sources/sparql
{
  "source_id": "knowledge-graph",
  "endpoint_url": "http://fuseki:3030/ds/sparql",
  "default_graph_uri": "http://example.org/graph"
}

# Register a table (executes LIMIT 5 probe to validate and infer columns)
POST /admin/sources/sparql/knowledge-graph/tables
{
  "table_name": "product_categories",
  "sparql_query": "SELECT ?product ?label ?category WHERE { ?product a :Product ; rdfs:label ?label ; :hasCategory ?category . }",
  "ttl": 600
}
```

Ambos conectores usan el pipeline de caché de origen de API — los resultados se almacenan en PostgreSQL con TTL configurable, lo que los hace disponibles para JOIN federados entre orígenes (REQ-295, REQ-297, REQ-299).

---

## Ejemplos de conexión
### PostgreSQL

```yaml
- id: sales-pg
  type: postgresql
  host: postgres
  port: 5432
  database: provisa
  username: provisa
  password: ${env:PG_PASSWORD}
```

### Snowflake

```yaml
- id: analytics-sf
  type: snowflake
  host: org.snowflakecomputing.com
  port: 443
  database: ANALYTICS
  username: svc_provisa
  password: ${env:SNOWFLAKE_PASSWORD}
  federation_hints:
    account: myorg-myaccount
    warehouse: COMPUTE_WH
```

### Databricks

```yaml
- id: lakehouse-db
  type: databricks
  host: my-workspace.azuredatabricks.net
  password: ${env:DATABRICKS_TOKEN}
  federation_hints:
    http_path: /sql/1.0/warehouses/xxxx
```

### MongoDB

```yaml
- id: reviews-mongo
  type: mongodb
  host: mongodb
  port: 27017
  database: provisa
  username: ""
  password: ""
```

### Consulta entre orígenes
```graphql
{
  orders(where: {region: {eq: "us"}}) {
    id
    amount
    customers {       # PostgreSQL
      name
      email
    }
    productReviews {  # MongoDB (federated)
      rating
      comment
    }
  }
}
```

Las porciones de un solo origen se enrutan directamente (REQ-027). Los JOIN entre orígenes se federan con coerción de tipos automática (REQ-028, REQ-552).
