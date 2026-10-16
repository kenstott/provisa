# Tipos de origen {: #source-types }
## Modelo de ejecución {: #execution-model }
Toda consulta se ejecuta en última instancia a través del motor de federación, que provee federación entre todos los orígenes. Los orígenes se dividen en tres categorías según su conectividad. [tool-verified: `provisa/core/models.py` lines 84–132] (REQ-550)

| Categoría | Tiene controlador directo | Tiene conector federado | Ejemplos |
| --- | --- | --- | --- |
| **Con capacidad directa** | Sí | Sí | PostgreSQL, MySQL, MariaDB, SingleStore, SQL Server, Oracle, DuckDB |
| **Solo federación** | No | Sí | Redshift, Druid, Exasol, Hive, Iceberg, Delta Lake, Hive (respaldado por S3) |
| **Lectura directa (réplica)** | Sí | Sí | Snowflake, Databricks, ClickHouse — el controlador lee los datos y crea una réplica; las consultas se ejecutan contra la réplica en el motor activo |
| **Directo (sin conector entre orígenes)** | Sí | No | Athena — las instrucciones de un solo origen se ejecutan íntegras en Athena; las uniones entre orígenes crean una réplica completa |
| **Materializar → Federación** | No | No | REST/OpenAPI, GraphQL remoto, gRPC, Neo4j Cypher, SPARQL, WebSocket, RSS, CSV, SQLite, Parquet, Ingest (receptor push), GovData, SharePoint, Salesforce, ServiceNow, inventario de nube, Splunk, Qdrant, Azure Cosmos DB, Logs, Google Workspace, Microsoft 365 |

Los orígenes **con capacidad directa** ejecutan consultas de un solo origen mediante su controlador nativo (menos de 100 ms), evitando el motor de federación (REQ-027, REQ-229). Conservan el soporte completo del conector y participan en la federación cuando se combinan con otros orígenes (REQ-028).

Los orígenes **solo federación** siempre se consultan a través de la capa de federación. No existe un controlador directo (REQ-229).

Los orígenes de **lectura directa (réplica)** tienen un DirectDriver que lee del almacén de datos de forma nativa (nativo en Arrow cuando está disponible), crea una réplica en el almacén de materialización del motor activo, y luego las consultas se ejecutan contra esa réplica. Consulte [Almacenes de datos como orígenes con nombre](#almacenes-de-datos-como-origenes-con-nombre).

Los orígenes **directos (sin conector entre orígenes)** tienen un DirectDriver que ejecuta las instrucciones de un solo origen íntegras en el origen — cada filtro, columna y límite se traslada. Cuando una instrucción une el origen con cualquier otro, el controlador lee los datos y crea una réplica; la unión se ejecuta contra esa réplica en el motor activo. No existe ningún conector de Trino para estos orígenes.

Los orígenes de **materialización** no tienen conector federado. Provisa obtiene sus datos (al inicio o en el momento de la consulta) y los almacena en caché como Parquet en S3 o en PostgreSQL, haciéndolos accesibles para el motor de federación en consultas entre orígenes (REQ-309).

---

## Todos los orígenes {: #all-sources }
Provisa registra **72** tipos de origen, los que ofrece el formulario de Orígenes. Las tablas siguientes cubren los 72; el índice es el recuento. [tool-verified: `provisa-ui/src/pages/sources/constants.ts` `SOURCE_TYPES`, donde `graphql` y `grpc` son `graphql_remote` y `grpc_remote`; Kaggle, Wikipedia, GitHub, GitLab y Stripe se cuentan como orígenes distintos aunque internamente se registran mediante los conectores `files`, `graphql_remote` y `openapi`]

| # | Grupo | Tipos de origen |
| --- | --- | --- |
| 1–15 | [RDBMS](#rdbms) | `postgresql`, `mysql`, `mariadb`, `singlestore`, `sqlserver`, `oracle`, `duckdb`, `cockroachdb`, `yugabytedb`, `greenplum`, `tidb`, `firebird`, `airport`, `saphana`, `hiveserver2` |
| 16–24 | [Almacenes de datos en la nube](#cloud-data-warehouses) | `snowflake`, `bigquery`, `databricks`, `redshift`, `athena`, `fabric`, `synapse`, `trino`, [`motherduck`](#motherduck) |
| 25–29 | [Analítica / OLAP](#analytics-olap) | `clickhouse`, `druid`, `exasol`, `elasticsearch`, `pinot` |
| 30–33 | [Data lake / Formatos de tabla abiertos](#data-lake-open-table-formats) | `iceberg`, `delta_lake`, `hive`, `hive_s3` |
| 34–38 | [NoSQL](#nosql) | `mongodb`, `cosmosdb`, `qdrant`, `cassandra`, `redis` |
| 39–40 | [Fuentes de código](arch/code-sources.md) | `node`, `python` |
| 41–43 | [Streaming](#streaming) | `kafka`, `websocket`, `rss` |
| 44 | [Receptor push](#push-receiver) | `ingest` |
| 45–46 | [Grafo y semántica](#graph-semantic) | `neo4j`, `sparql` |
| 47–51 | [Basados en archivos](#file-based) | `sqlite`, `csv`, `parquet`, `files`, `logs` |
| 52–53 | [Observabilidad y otros](#observability-other) | `google_sheets`, `prometheus` |
| 54–60 | [SaaS empresarial](#enterprise-saas-connectors) | `sharepoint`, `salesforce`, `servicenow`, `cloudops`, `splunk`, `google_workspace`, `microsoft_365` |
| 61–66 | [Orígenes de API](#api-sources) | `openapi`, `graphql_remote`, `grpc_remote`, GitHub, GitLab, Stripe |
| 67 | [GovData](#govdata) | `govdata` |
| 68–69 | [Verificadores de calidad de datos](#data-quality-checkers-req-1443) | `soda`, `great_expectations` |
| 70 | [Perfilador de datos](#data-profiler) | `data_profiler` |
| 71 | [Conjuntos de datos de Kaggle](#kaggle-datasets) | Kaggle (preparado mediante el formulario de Orígenes; se registra como un origen `files` — véase [Conjuntos de datos de Kaggle](#kaggle-datasets)) |
| 72 | [Wikipedia](#wikipedia) | Wikipedia (se registra como un origen `files` que rastrea las páginas indicadas — véase [Wikipedia](#wikipedia)) |

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
| `saphana` | SQLAlchemy (`hana+hdbcli`) | — | — | No |
| `hiveserver2` | impyla (HiveServer2 sobre Thrift) | — | hive | No |

Las bases de datos compatibles a nivel de wire reutilizan el controlador JDBC, el controlador nativo asíncrono y el dialecto de un wire base — CockroachDB, YugabyteDB y Greenplum usan el wire de PostgreSQL; TiDB usa el wire de MySQL. Solo necesitan entradas de registro, sin código de conector nuevo. [tool-verified: `provisa/core/source_registry.py` `_PG_WIRE_TYPES`, `_MYSQL_WIRE_TYPES`] (REQ-950)

`firebird` (Firebird 3/4/5) y `airport` (servidor Arrow Flight) son tipos de origen registrados a los que se accede en el lugar a través de extensiones de la comunidad de DuckDB cuando DuckDB es el motor activo — sin controlador directo, sin conector federado. [tool-verified: `provisa/core/models.py` lines 44, 93] (REQ-899)

#### `saphana`

SAP HANA se lee con su propio controlador y se replica en el almacén del motor activo, de modo que es accesible en todos los motores. El formulario pide host, puerto (predeterminado 39015), usuario, contraseña y base de datos; en un sistema multi-tenant la base de datos nombra el tenant. Register Table lista los esquemas que el usuario puede leer, sin los esquemas de sistema de HANA. [tool-verified: `provisa/executor/drivers/registry.py` `_SQLALCHEMY_FALLBACK`; `provisa/api/admin/introspect.py` `_SAPHANA_SYSTEM_SCHEMAS`] (REQ-1753)

#### `hiveserver2`

Un endpoint HiveServer2 leído directamente sobre Thrift, para un despliegue de Hive donde el endpoint es lo único accesible. Es distinto de `hive` y `hive_s3`, que leen los archivos y el metastore del almacén y nunca pasan por HiveServer2. Las filas se leen a través del endpoint y se replican en el almacén del motor activo. El formulario pide host, puerto (predeterminado 10000), usuario, contraseña y base de datos; `federation_hints.auth_mechanism` selecciona la autenticación (predeterminada `PLAIN`, por ejemplo `GSSAPI` para Kerberos). [tool-verified: `provisa/executor/drivers/hive.py` `HiveDriver`; `provisa/core/models.py` `SourceType.hiveserver2`] (REQ-1731)

### Almacenes de datos en la nube {#cloud-data-warehouses} {: #cloud-data-warehouses }
[tool-verified: `executor/drivers/snowflake.py`, `executor/drivers/databricks.py`, `executor/drivers/registry.py`]

| Tipo de origen | Controlador directo | Nombre del conector | Dialecto | Mutaciones | Notas |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `snowflake` | SnowflakeDriver | snowflake | snowflake | Federada | Lee mediante snowflake-connector-python; crea réplica; `account`/`warehouse`/`role` en `federation_hints` (REQ-988) |
| `bigquery` | — | bigquery | bigquery | Federada | Sin DirectDriver; se accede mediante el motor de federación o BigQuery engine ATTACH |
| `databricks` | DatabricksDriver | delta_lake | databricks | Federada | Lee mediante databricks-sql-connector (Cloud Fetch, Arrow); crea réplica; `http_path` obligatorio en `federation_hints` (REQ-987) |
| `redshift` | — | redshift | redshift | Federada | — |
| `athena` | AthenaDriver | — | athena | Parcial (INSERT siempre; UPDATE/DELETE/MERGE solo en Iceberg) | Consulta serverless sobre S3; las instrucciones de una sola fuente se ejecutan íntegramente en Athena; las consultas de fuentes cruzadas aterrizan una réplica; `region` requerida en `federation_hints` (REQ-1981) |
| `fabric` | MssqlWarehouseDriver | — | tsql | Federada | Microsoft Fabric Warehouse; T-SQL sobre TDS, autenticación Azure AD; crea réplica (REQ-995) |
| `synapse` | MssqlWarehouseDriver | — | tsql | Federada | Azure Synapse SQL; T-SQL sobre TDS, autenticación Azure AD; crea réplica (REQ-995) |
| `trino` | SQLAlchemyDriver | — | — | Federada | Coordinador remoto Trino/Presto leído mediante el dialecto trino de SQLAlchemy; crea réplica en cualquier motor (REQ-994) |

#### `athena`

Amazon Athena es un servicio de consultas sin servidor sobre datos en S3, facturado por byte analizado. Una instrucción únicamente sobre tablas de Athena se ejecuta directamente en Athena: los filtros, listas de columnas y `LIMIT` se trasladan íntegramente, de modo que solo se analizan y facturan los bytes que Athena necesita para responder esa instrucción. Una instrucción que une una tabla de Athena con otra fuente primero aterriza una réplica completa desde Athena — se lee toda la tabla y Athena la factura — y luego la unión se ejecuta contra esa réplica. Los ajustes de Replicate y protección de carga de la fuente rigen la frecuencia de aterrizajes; el aviso en la documentación es la única regla. (REQ-1981)

**Descubrimiento.** Register Table llama a la API de metadatos de Athena — `ListDatabases`, `ListTableMetadata`, `GetTableMetadata`. Estas llamadas no ejecutan ninguna consulta, por lo que listar bases de datos y tablas y leer las columnas de una tabla no cuesta nada por byte. Las columnas de una tabla se leen solo cuando se selecciona esa tabla. Las claves de partición siguen a las columnas regulares. Los tipos se asignan a través de la entrada `athena` en `provisa/core/ir_types.py`: `array<…>`, `map<…>` y `struct<…>` se convierten en `text`; un tipo desconocido es rechazado indicando su nombre. [tool-verified: `provisa/federation/athena.py` `databases`, `tables`, `columns`; `provisa/core/ir_types.py` entrada `"athena"`]

**Campos de la fuente.**

| Campo | Dónde | Requerido | Predeterminado | Notas |
| --- | --- | --- | --- | --- |
| `database` | fuente | Sí | — | La base de datos de Glue consultada por defecto |
| `username` | fuente | No | — | ID de clave de acceso de AWS; puede ser `${secret:…}` o `${env:…}` |
| `password` | fuente | No | — | Clave de acceso secreta de AWS; puede ser `${secret:…}` o `${env:…}` |
| `region` | `federation_hints` | Sí | — | Región de AWS, p. ej. `us-east-1` |
| `workgroup` | `federation_hints` | No | `primary` | Grupo de trabajo de Athena |
| `s3_staging_dir` | `federation_hints` | No | predeterminado del grupo | Dónde Athena escribe los resultados; requerido si el grupo de trabajo no establece uno |
| `catalog` | `federation_hints` | No | `AwsDataCatalog` | Catálogo de datos de Athena |
| `role_arn` | `federation_hints` | No | — | ARN del rol de IAM asumido en cada llamada |

Ambos `username` y `password` vacíos significa que se usa la cadena de credenciales de boto3: rol de instancia, variables de entorno (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`) o un perfil con nombre. Uno sin el otro es rechazado. [tool-verified: `provisa/executor/drivers/athena.py` `AthenaDriver.connect`; `provisa/federation/athena.py` `athena_client`]

**Escrituras.** Cada tabla registrada acepta `INSERT`. `UPDATE`, `DELETE` y `MERGE` solo están disponibles en una tabla Iceberg; el formato se registra al momento del registro (`registered_tables.table_format`). Una escritura en una tabla Hive que solicite `UPDATE`, `DELETE` o `MERGE` es rechazada indicando la tabla y la operación. Athena no devuelve las filas escritas — se rechaza una cláusula `RETURNING` y GraphQL no ofrece campos de mutación para estas tablas; `INSERT`, `UPDATE`, `DELETE` y `COPY` son los caminos de escritura. `INSERT … ON CONFLICT`, `UPDATE … FROM`, `DELETE … USING` y `TRUNCATE` son rechazados indicando la forma. [tool-verified: `provisa/executor/write_capability.py` `table_write_ops`; `provisa/executor/writable.py` `_TYPE_RETURNS_NO_ROWS`, `_TYPE_REFUSED_FORMS`]

```yaml
- id: analytics-athena
  type: athena
  database: my_glue_database
  username: ${env:ATHENA_ACCESS_KEY_ID}
  password: ${env:ATHENA_SECRET_ACCESS_KEY}
  federation_hints:
    region: us-east-1
    workgroup: primary
    s3_staging_dir: s3://my-bucket/athena-results/
```

Con un rol asumido por llamada (cadena de credenciales para la sesión base):

```yaml
- id: analytics-athena
  type: athena
  database: my_glue_database
  federation_hints:
    region: us-east-1
    role_arn: arn:aws:iam::123456789012:role/AthenaQueryRole
    s3_staging_dir: s3://my-bucket/athena-results/
```


### MotherDuck

`motherduck` (REQ-1998) es una base de datos DuckDB alojada en MotherDuck. En el motor DuckDB se adjunta en su lugar; en el motor Postgres sigue siendo el conector del foreign data wrapper `pg_duckdb` (REQ-1868). [tool-verified: `provisa/federation/connector_duckdb.py` `DuckDBMotherDuckConnector`, `PgDuckdbMotherDuckConnector`]

**Conexión.** `database` nombra la base de datos de MotherDuck, o un recurso compartido por su dirección de recurso compartido. `password` es el token de MotherDuck, que se resuelve como cualquier secreto de origen. [tool-verified: `DuckDBMotherDuckConnector.details`]

```yaml
- id: reference
  type: motherduck
  database: analytics            # attached as md:analytics
  password: ${secret:<name>}     # the MotherDuck token, or a reference to it
```

El motor DuckDB emite `ATTACH 'md:<database>'`; para un recurso compartido, `database` es la dirección del recurso compartido (`_share/...`), así que el adjunto es `md:_share/...`. [tool-verified: `details` builds `"md:" + database`; `tests/integration/test_motherduck_duckdb_e2e.py` attaches MotherDuck's public sample-data share this way]

- **Manejo del token.** El motor inicia sesión con `SET GLOBAL motherduck_token = ?`, con el token pasado como valor enlazado. No aparece en el texto de ninguna instrucción, por lo que ningún log, traza o error lo contiene. [tool-verified: `provisa/federation/duckdb_runtime.py` `_sign_in_motherduck`]
- **Sin token.** Un origen sin `password` se rechaza por nombre antes de adjuntar nada (`a MotherDuck source signs in with its token (the password field); none is set`). Sin esta comprobación, la extensión iniciaría un inicio de sesión interactivo y esperaría alrededor de un minuto. Un origen sin `database` también se rechaza. [tool-verified: `details`]
- **Una cuenta por motor.** Un motor inicia sesión en una cuenta de MotherDuck. Un segundo origen MotherDuck con un token distinto se rechaza por nombre: "one DuckDB engine reads one MotherDuck account". [tool-verified: `_sign_in_motherduck` compares a SHA-256 of the token]
- **Las escrituras** pasan por el motor, como en el motor Postgres. [tool-verified: `DuckDBMotherDuckConnector.capability` `write=True`]

**La ejecución híbrida (dual)** no necesita ninguna configuración. En el motor DuckDB todos los orígenes se adjuntan en una sola conexión, así que una instrucción que une una tabla de un origen de archivo DuckDB con una tabla de MotherDuck se ejecuta como una sola instrucción DuckDB, y MotherDuck decide dónde se ejecuta cada parte. [tool-verified: `DuckDBMotherDuckConnector` docstring; REQ-1998 notes record a live join showing dual-execution operators]

> **Movimiento de datos.** Con la ejecución dual, MotherDuck puede mover filas locales a su lado para las partes que ejecuta. Por eso, una instrucción así puede enviar datos del archivo local a la cuenta de MotherDuck de la organización. No una un archivo con datos que deben permanecer en el nodo con un origen MotherDuck salvo que enviarlos a esa cuenta sea aceptable. [tool-verified: `DuckDBMotherDuckConnector` docstring: "which may move local rows to its side"]

Un origen MotherDuck solo lo sirven los motores DuckDB y Postgres. Ningún otro motor tiene un conector para él, y nada aterriza allí sus tablas como réplicas. [tool-verified: `provisa/federation/engine.py` registers `DuckDBMotherDuckConnector` and `PgDuckdbMotherDuckConnector` and no other MotherDuck connector]

### Analítica / OLAP {#analytics-olap} {: #analytics-olap }
[tool-verified: `executor/drivers/clickhouse.py`]

| Tipo de origen | Controlador directo | Nombre del conector | Dialecto | Mutaciones | Notas |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `clickhouse` | ClickHouseDriver | clickhouse | clickhouse | Federada | Lee mediante clickhouse-connect (HTTP); `secure: "true"` en `federation_hints` para TLS (REQ-986) |
| `druid` | — | druid | druid | No | — |
| `exasol` | — | exasol | exasol | No | — |
| `elasticsearch` | HTTP (motores nativos) | elasticsearch (Trino) | — | No | En Trino el conector lo lee, con sus propiedades provenientes del DSL de mapeo del tipo [tool-verified: `trino_connectors.py:309`]; en cualquier otro motor Provisa lee el índice mediante HTTP (índices y mapeo para Register Table, una lectura por scroll para el aterrizaje) y aterriza las filas [tool-verified: `provisa/elasticsearch/fetch.py`, `provisa/events/source_loader.py` `make_elasticsearch_loader`] (REQ-1672) |
| `pinot` | — | pinot | — | No | Conector `pinot` de Trino; `pinot.controller-urls` = host:port del controlador de Pinot [tool-verified: `trino_connectors.py:199`] |

### Data lake / Formatos de tabla abiertos {#data-lake-open-table-formats} {: #data-lake-open-table-formats }
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
| `cassandra` | cassandra (Trino); lectura CQL mediante cassandra-driver en cualquier otro motor | No | Los keyspaces son esquemas; Register Table lista las tablas de un keyspace y tipa las columnas a partir de los metadatos de esquema del clúster (las claves de partición como claves primarias); el extra `cassandra` instala el controlador [tool-verified: `provisa/cassandra/fetch.py`] (REQ-1676) |
| `redis` | redis (Trino); lectura sin HTTP mediante redis-py en cualquier otro motor | No | Un prefijo de clave `<table>:*` es una tabla y un hash es una fila; Register Table lista los prefijos presentes y tipa las columnas de un prefijo a partir de sus hashes (una entrada de `mapping.tables` anula el patrón, la columna de clave, el tipo de valor y las columnas) [tool-verified: `provisa/redis/fetch.py`] (REQ-1675) |

### Qdrant

`qdrant` (REQ-1983, REQ-2000) se lee y se escribe con el cliente propio de Qdrant, `qdrant-client`. Qdrant es **solo en vivo**: una propiedad del tipo de origen, no un ajuste. Responde con su índice vectorial y su Query API, y ninguna réplica puede responder lo mismo, así que sus tablas nunca se replican. No hay opción Replicate para un origen Qdrant ni para sus tablas; un valor `replicate` o `row_materialize` en uno se rechaza. [tool-verified: `provisa/core/source_registry.py` `LIVE_ONLY_TYPES`; `provisa/api/admin/_landing_ttl.py` `_live_only_refusal`]

**Conexión.** `host` es la URL de la instancia con su puerto (`https://<cluster>.cloud.qdrant.io:6333`, o un `http://qdrant:6333` autoalojado); `password` es su clave de API, una referencia a un secreto resuelta cuando se lee el origen, vacía para una instancia sin clave. `federation_hints.prefer_grpc: true` lee por el puerto gRPC de Qdrant en lugar de REST. El origen tiene un esquema, `default`. [tool-verified: `provisa/qdrant/fetch.py` `QdrantConnection.from_source`; `provisa/qdrant/introspect.py` `SCHEMA`]

**Tablas de colección.** Una colección es una tabla. Register Table ofrece las columnas de la colección: `id` (el id del punto, su clave, ruta `@id`), los campos del payload (un campo indexado se tipa por su índice, cualquier otro según sugieren los puntos de muestra; los campos anidados se aplanan con su ruta) y una columna `json` por vector (ruta `@vector`, o `@vector.<name>` para un vector con nombre), ofrecida sin seleccionar. Una columna de vector nombra el modelo de embeddings registrado del que contiene embeddings, elegido entre los modelos cuyas dimensiones son el tamaño del vector (en Register Table y en la pestaña Tipo y claves del editor); elegir un modelo registra el vector. [tool-verified: `provisa/qdrant/introspect.py` `columns`; `provisa-ui/src/pages/tables/QdrantVectorColumns.tsx`]

**Instrucciones sobre un origen Qdrant.** Una instrucción que lee una sola colección se ejecuta en Qdrant cuando la API de Qdrant puede expresarla con exactitud:

- las condiciones sobre el payload se convierten en un filtro de Qdrant (`=`, `<>`, `IN`, rangos sobre números y fechas, `IS NULL`, `AND`, `OR`, `NOT`); se conserva la semántica de NULL de SQL, de modo que `lang <> 'en'` omite los puntos sin `lang`;
- una condición solo sobre `id` (`=` o `IN`) es un `retrieve`; cualquier otra lectura es un `scroll` filtrado;
- `ORDER BY cosine_similarity(<vector>, <query>) DESC LIMIT k` es un `query_points` sobre un vector cuya distancia es Cosine, con el filtro aplicado antes del top-k. Un texto de consulta lo incrusta el modelo del vector. La respuesta lleva una advertencia de que el top-k es aproximado, elegido mediante el índice de Qdrant.

Todo lo demás (un agregado, un `GROUP BY`, una columna calculada o enmascarada, un orden distinto del más similar primero) lo responde el motor, como se describe a continuación. [tool-verified: `provisa/qdrant/direct.py` `analyse`, `condition`, `QdrantDriver`]

**Instrucciones que responde el motor.** Cuando una instrucción une una colección con otro origen, o calcula sobre sus columnas, la parte de la colección se lee en vivo desde Qdrant para esa instrucción y se entrega al motor como filas. La parte son las columnas de la colección, las condiciones de la instrucción sobre la colección sola (entre ellas sus reglas de fila) y, cuando la colección se lee sola en su `SELECT`, el orden por similitud y el `LIMIT` de ese `SELECT`. Una parte que Qdrant no puede expresar con exactitud se rechaza, nombrando la tabla y el motivo. Una parte sin condición, sin clave y sin top-k leería toda la colección y también se rechaza. Una condición sobre un parámetro (`$1`) se deja al motor. [tool-verified: `provisa/federation/live_only.py` `cut`, `read`; `provisa/pgwire/_pipeline.py` `_read_live_parts`]

**Escrituras.** `INSERT`, `UPDATE` y `DELETE` sobre una colección se ejecutan en Qdrant:

- `INSERT … VALUES` nombra la columna `id` y escribe puntos nuevos. Un `id` que la colección ya contiene se rechaza por su nombre. La comprobación lee primero los ids, por lo que no es atómica con la escritura.
- `INSERT … ON CONFLICT (id) DO UPDATE SET c = EXCLUDED.c, …` es un upsert. Las columnas que la instrucción no establece conservan sus valores.
- `UPDATE` y `DELETE` actúan sobre los puntos que selecciona su `WHERE`, como filtro de Qdrant; una condición sin forma de filtro se rechaza.
- Un texto escrito en una columna de vector lo incrusta el modelo de la columna. Un vector escrito en ella debe tener las dimensiones del modelo.

[tool-verified: `provisa/qdrant/direct.py` `_insert`, `_upsert_clause`, `_write_points`, `_update`, `_delete`, `_vector_value`]

**Tablas de búsqueda.** La búsqueda vectorial es una tabla de búsqueda: su `query_template` contiene una petición de la Query API.

```json
{"collection": "docs", "query": "@query_vector", "using": "text",
 "filter": {"must": [{"key": "lang", "match": {"value": "@lang"}}]},
 "limit": "@k", "score_threshold": 0.5, "model": "text-embedding-3-small"}
```

- `query` es siempre un argumento; `using`, `filter`, `limit`, `score_threshold` y `model` son opcionales. Cualquier valor de `using`, `filter`, `limit` o `score_threshold` que sea exactamente `"@name"` también es un argumento.
- Cada argumento es un parámetro de la tabla (`query_arg`, REQ-1986), convertido estrictamente a su tipo declarado e insertado en la petición como valor JSON, nunca en su texto. El argumento de `query` es `array(double)` y obligatorio.
- Quien llama pasa los argumentos como cualquier parámetro: `SELECT id, score, title FROM docs_search WHERE _nf_query_vector = '[0.1, 0.2, …]' AND _nf_lang = 'en'` en SQL, argumentos de campo en GraphQL.
- Un argumento opcional no dado no filtra nada: su condición se omite del filtro. Sin `limit`, una búsqueda devuelve el valor por defecto de Qdrant, 10 puntos.
- Qdrant aplica el filtro antes de elegir sus puntos más cercanos, así que es exacto. Cualquier otra condición de la instrucción estrecha después los puntos que devolvió la búsqueda; no obtiene más.
- `score` es la métrica propia de la colección para el vector buscado: para una colección Cosine, la similitud del coseno. Register Table lista los vectores de la colección con su tamaño y distancia.
- `model` nombra un modelo de embeddings registrado (`vector_models`, admin → AI Models). Un texto dado como consulta se incrusta entonces con ese modelo, y un vector dado debe tener las dimensiones de ese modelo (REQ-429, REQ-430). Sin `model`, la consulta debe ser un vector.

Una búsqueda se lee por instrucción con sus argumentos, y sus filas se conservan bajo esos argumentos durante el TTL de la tabla. Una tabla de búsqueda es de solo lectura. [tool-verified: `provisa/qdrant/table.py` `check_table`, `plan`, `read`; `provisa/api/data/materialization.py` `_is_native_read`]

### Streaming

| Tipo de origen | Mecanismo | Mutaciones |
| ------------ | ----------- | ----------- |
| `kafka` | Conector Kafka federado; esquema mediante Confluent Schema Registry (Avro, Protobuf, JSON Schema), definición manual o inferencia por muestreo (REQ-147, REQ-150) | No; la salida en vivo de una tabla publica en Kafka (REQ-565) |
| `websocket` | Feed WebSocket externo — conecta, se suscribe, recibe eventos; los resultados se materializan (REQ-338) | No |
| `rss` | Feed RSS 2.0 / Atom — sondea, marca de agua por pubDate/updated; los resultados se materializan (REQ-342, REQ-343) | No |

### Receptor push {#push-receiver} {: #push-receiver }
| Tipo de origen | Mecanismo | Mutaciones |
| ------------ | ----------- | ----------- |
| `ingest` | Servicios externos envían eventos JSON mediante POST; los resultados se materializan (REQ-331, REQ-335) | No |

### Grafo y semántica {#graph-semantic} {: #graph-semantic }
| Tipo de origen | Mecanismo | Mutaciones |
| ------------ | ----------- | ----------- |
| `neo4j` | Cypher mediante API HTTP, resultados almacenados en caché en PostgreSQL (REQ-295) | No |
| `sparql` | SPARQL 1.1 POST, resultados almacenados en caché en PostgreSQL (REQ-297) | No |

### Basados en archivos {#file-based} {: #file-based }
Tres mecanismos cubren los archivos. Los orígenes de un solo archivo y el rastreador de archivos usan el campo `path` en lugar de `host`/`port`. [tool-verified: `provisa/core/models.py`] (REQ-553) El tercero, [Logs](#logs), nombra sus archivos mediante un glob en su `mapping` y lee solo archivos del servidor de Provisa. [tool-verified: `provisa/logs/settings.py`]

**Orígenes de base de datos en archivo y de archivo único** — `sqlite`, `duckdb`, `csv`, `parquet` apuntan `path` a un solo archivo. (Un origen `duckdb` guarda su ubicación en el campo `database` del formulario.) [tool-verified: `provisa-ui/src/pages/sources/FileLocationFields.tsx`]

| Tipo de origen | Ubicaciones | Mutaciones |
| --- | --- | --- |
| `sqlite` | ruta local, `https://`, `s3://`, `gs://`, `az://` | Sí en un archivo local; ninguna en uno remoto |
| `duckdb` | ruta local, `https://`, `s3://`, `gs://`, `az://` | Sí en un archivo local; ninguna en uno remoto |
| `csv` | ruta local, `https://`, `s3://`, `gs://`, `az://` (motor DuckDB) | No |
| `parquet` | ruta local, `https://`, `s3://`, `gs://`, `az://` (motor DuckDB) | No |

[tool-verified: `provisa/federation/connector_duckdb.py` `location_kind`, `file_location_details`; `provisa/executor/write_capability.py` `table_write_ops`. The table describes the DuckDB engine, where the connectors above take every location. On other engines, CSV and Parquet over `s3://` or `http(s)://` are better registered through the `files` source. [inferred]] (REQ-1997)

Una ruta con cualquier otro esquema (`ftp://`, por ejemplo) se rechaza: "DuckDB reads no file at a `ftp://` location". [tool-verified: `location_kind` raises `ValueError`]

**Credenciales para una ubicación remota.** Póngalas en los `federation_hints` del origen. Cada una se limita al bucket o contenedor de la propia ruta, así que las credenciales de un origen nunca se usan para la ubicación de otro origen. Un origen que no da ninguna se lee como una ubicación pública. [tool-verified: `object_store_secret_ddl`]

| Ubicación | Claves de `federation_hints` |
| --- | --- |
| `s3://` | `access_key_id`, `secret_access_key`; opcionales `region`, `endpoint` (un almacén compatible con S3 como R2 o MinIO) |
| `gs://` | `gcs_access_id`, `gcs_secret_key` (una clave HMAC) |
| `az://` | `azure_account_name`, `azure_account_key` |

**Los archivos SQLite y DuckDB remotos son de solo lectura.** El motor DuckDB los adjunta en su lugar con `READ_ONLY`, y una tabla sobre uno de ellos no ofrece insert, update ni delete. En un motor que no adjunta SQLite, un archivo SQLite remoto se lee a través de una conexión DuckDB temporal que lo adjunta en solo lectura, y sus filas aterrizan como de costumbre. [tool-verified: `_attach_options`; `table_write_ops`; `provisa/events/source_loader.py` `make_sqlite_loader` `_remote`]

**Subir un archivo.** En el formulario de un origen `sqlite`, `duckdb`, `csv` o `parquet`, suba el archivo en lugar de escribir una ruta. El archivo se transmite al disco, nunca se guarda en memoria, a la carpeta de la organización en `files/<source id>/<file name>`, y la ruta del origen pasa a apuntar a él. [tool-verified: `provisa/core/source_uploads.py` `receive`, `upload_dir`; `provisa/api/admin/source_files_router.py` `upload_file`]

- **Reemplazar.** Subir de nuevo ocupa el lugar del archivo anterior, sea cual sea su nombre, y el origen se aprovisiona de nuevo para que la siguiente lectura vea el archivo nuevo. [tool-verified: `receive` removes other files in the directory; `upload_file` calls `reprovision_source_after_registration`]
- **Eliminar.** Eliminar el origen elimina su archivo subido. Nada más lo elimina. [tool-verified: `provisa/api/admin/schema_mutation.py` `source_uploads.remove(id)`]
- **Quién.** Solo los roles con la capacidad `source_registration` pueden subir archivos. [tool-verified: `upload_file` `require_capability_request(request, "source_registration")`]
- **Límite de tamaño.** La configuración `sources.max_upload_bytes` (entorno `PROVISA_SOURCES_MAX_UPLOAD_BYTES`) limita el archivo, 1 GiB (1073741824 bytes) por defecto, mínimo 1, y se lee al empezar una subida. Un archivo mayor se rechaza con HTTP 413 indicando el límite, y no se conserva nada de él. [tool-verified: `provisa/core/settings_catalog.py`; `UploadTooLarge`]
- **Ruta.** Mientras la ruta sea el archivo subido del origen, el campo de ruta del formulario es de solo lectura. [tool-verified: `FileLocationFields.tsx` `readOnly={uploaded}`]
- **Nombres.** El id del origen y el nombre del archivo solo pueden usar letras, dígitos, `.`, `_` y `-`. [tool-verified: `source_uploads._SEGMENT`]
- **Almacenamiento.** [inferred from the `source_uploads` docstring] Un archivo subido se guarda como cualquier archivo de la organización: en la carpeta de la organización, cifrado cuando esa carpeta lo está, y visible para todos los nodos en un montaje compartido.

**Escrituras en un archivo SQLite o DuckDB local, y bloqueos.** Una escritura toma el bloqueo del archivo y nunca lo espera. Cuando otro proceso o nodo tiene el archivo, la escritura se rechaza, indicando el origen y que su archivo está bloqueado. [tool-verified: `provisa/federation/file_locks.py` `FileLocked`; REQ-1997 notes quote the maintainer: "writes back to file databases in a multi-node env - just error them out when they can't a lock on it."]

- **SQLite.** La escritura establece `PRAGMA busy_timeout = 0`, así que un archivo bloqueado falla de inmediato. [tool-verified: `provisa/executor/drivers/sqlalchemy_driver.py` `_write_sqlite`]
- **DuckDB.** DuckDB comparte un archivo solo entre aperturas de solo lectura. Por eso el adjunto del motor y la conexión propia del origen mantienen el archivo en solo lectura, y todos los nodos lo leen a la vez. Una escritura pausa a los poseedores de este proceso, abre el archivo en lectura y escritura solo para esa instrucción y lo cierra. Si la apertura no puede tomar el bloqueo, la escritura se rechaza. [tool-verified: `provisa/executor/drivers/duckdb_driver.py` `_write`: `with file_locks.exclusive(self._database):` then `duckdb.connect(self._database)`; `connector_duckdb._attach_options`]

**Montajes compartidos.** En una carpeta de organización cifrada cuyo montaje comparten varios nodos (configuración `org_files.shared_mount`), una escritura en un archivo SQLite o DuckDB de esa carpeta se rechaza directamente, indicando el origen. La capa de cifrado de carpetas (gocryptfs) no transmite los bloqueos de archivo a los demás nodos, así que nada impediría que dos nodos escribieran a la vez. Las lecturas siguen funcionando. [tool-verified: `file_locks.refuse_unprotected`, `UnprotectedWrite`; both drivers call it before a write]

Una carpeta compartida sin cifrado debe montarse con bloqueos que lleguen a todos los nodos: un montaje NFS con el bloqueo activado, no `nolock` ni `local_lock`. Ninguna comprobación lo verifica; es un requisito del despliegue. [tool-verified: REQ-1997 description in `docs/arch/requirements.yaml`; no check exists in source [inferred]]

**Rastreador de archivos (origen `files`)** — apunta `path` a un glob, lo recorre de forma recursiva y registra el directorio como un catálogo federado de tablas. Lee muchos formatos sobre muchos transportes; los conjuntos siguientes provienen del conector de archivos (kenstott/calcite fork). [tool-verified: `provisa/core/catalog.py` `files` branch and `provisa/core/models.py` `SOURCE_TO_CONNECTOR`; format and transport lists from the calcite `file` adapter — `FileSchema.java`, `storage/StorageProviderFactory.java`]

| Formatos | Transportes |
| --- | --- |
| CSV, TSV, JSON, YAML, Excel (XLS/XLSX), Parquet, Arrow, y documentos convertidos a tablas — HTML, Markdown, DOCX, PPTX | Sistema de archivos local, HTTP(S), `s3://`, `hdfs://`, `ftp://`/`ftps://`, `sftp://`, `iceberg://`, SharePoint (REST y Microsoft Graph) |

```yaml
- id: sales_files
  type: files
  path: s3://bucket/sales/**/*.csv   # glob; local and http(s):// also supported
```

Cualquier origen `files` también puede declarar un `crawl` en su mapping, con los mismos ajustes que se listan en [Wikipedia](#wikipedia): el origen lee entonces primero páginas web y deja cada tabla de datos HTML y cada archivo de datos enlazado en su directorio como una tabla. [tool-verified: `provisa/file_source/crawl.py` `crawl_operand`]

En el motor DuckDB, `files` se lee de forma nativa — una vista de escaneo `read_csv_auto` por cada `<table>.csv` bajo el directorio resuelto (REQ-229) [tool-verified: `provisa/federation/connector_duckdb.py` `DuckDBFilesConnector`]. En un motor sin conector `files` propio, las filas aterrizan a través del mismo servidor pgwire de Calcite incluido en el conector (`pgwire-file`) que usan sharepoint/splunk (REQ-954) — véase [Conectores SaaS empresariales](#enterprise-saas-connectors) más abajo. La cobertura de extremo a extremo de la UI (formulario de Orígenes → Register Table → consulta SQL) y la ruta de aterrizaje por pgwire están probadas en REQ-1694.

#### Logs (REQ-1996) {#logs} {: #logs }

Un origen `logs` convierte los archivos de registro del servidor de Provisa en una tabla. Usted indica los archivos con una ruta completa o un glob, y elige el formato en que están escritos los registros. Cada registro se convierte en una fila. Un registro que el formato no puede leer se conserva, con su texto sin procesar y el motivo, y nunca se descarta. [tool-verified: `provisa/logs/__init__.py`; `provisa/logs/files.py` `rows_of`]

```yaml
sources:
  - id: mac-install-log
    type: logs
    mapping:
      glob: /var/log/install.log*   # the live file and its rotated, compressed copies
      format: macos_install
```

El mapping admite cuatro claves y rechaza cualquier otra por su nombre: `glob` y `format` (ambas obligatorias), y `pattern` y `record_start`, que solo admite el formato `regex`. [tool-verified: `provisa/logs/settings.py` `parse`; `provisa/logparse/__init__.py` `format_for`]

```yaml
    mapping:
      glob: /var/log/myapp/*.log
      format: regex
      pattern: '(?P<ts>\S+ \S+) (?P<level>[A-Z]+) (?P<message>.*)'
      record_start: '\d{4}-\d{2}-\d{2} '   # a stack trace joins the line it follows
```

En el formulario de Orígenes, un origen Logs aparece como "Log files" bajo File. El botón **Preview** muestra, antes de guardar nada, los archivos con los que coincide el glob, las columnas que da el formato y las primeras 20 filas del archivo más reciente. [tool-verified: `provisa-ui/src/pages/sources/constants.ts`; `provisa-ui/src/pages/sources/LogsFields.tsx`; `provisa/api/admin/logs_router.py` `PREVIEW_ROWS`]

**El glob.** Debe ser una ruta completa; `~` se expande, y una ruta relativa se rechaza ("name the files by their full path"). Se evalúa con `glob` de Python en modo no recursivo, así que `**` se comporta como `*` y coincide dentro de un solo nivel de directorio. Solo se toman archivos regulares, en orden de nombre. Un archivo vacío se omite hasta que tiene bytes. [tool-verified: `provisa/logs/roots.py` `checked_glob`; `provisa/logs/files.py` `matched`, `identify`] El glob puede nombrar una variable de entorno, como en `${env:LOG_DIR}/*.log`. La fuente guarda la referencia tal como se escribió y la resuelve cada vez que lee los archivos; una variable sin definir se rechaza por su nombre. [tool-verified: `provisa/logs/settings.py` `parse`; `tests/unit/test_logs_source.py` `test_a_glob_naming_a_variable_reads_where_it_points`]

**Las columnas propias de cada fila.** Van antes de las columnas del formato:

| Columna | Tipo | Contiene |
| --- | --- | --- |
| `_file_id` | text | La identidad del archivo: una suma de verificación de sus primeros 4096 bytes, descomprimidos |
| `_file_name` | text | La ruta bajo la que se leyó el registro |
| `_line_offset` | bigint | Desplazamiento en bytes de la primera línea del registro en el texto descomprimido |
| `_entry` | bigint | Qué entrada del registro es la fila: 0, salvo en un registro por lotes como el `Records` de un archivo de CloudTrail |
| `_raw` | text | El registro tal como se escribió |
| `_parse_error` | text | Por qué no se pudo leer el registro o uno de sus valores; NULL cuando se leyó |
| `_landed_at` | timestamp | Cuándo aterrizó la fila |

`_file_id`, `_line_offset` y `_entry` son la clave de la fila, de modo que un registro leído dos veces es una sola fila. [tool-verified: `provisa/logs/__init__.py` `META_COLUMNS`, `KEY`; `provisa/logs/preview.py` `columns`]

**Formatos y presets.** El selector de formato agrupa los formatos por familia; la última opción, `regex`, es su propio patrón. [tool-verified: `provisa/logparse/__init__.py` `BUILT_IN`, `catalog`; `provisa/logparse/presets/__init__.py` `presets`; family labels from `provisa-ui/src/i18n/locales/en/logsFields.json`]

| Familia (etiqueta del formulario) | Nombres de formato |
| --- | --- |
| Common formats | `rfc3164`, `rfc5424`, `macos_install`, `clf`, `combined`, `jsonl`, `logfmt`, `w3c`, `cef`, `gelf` |
| Amazon Web Services | `aws_alb`, `aws_nlb`, `aws_elb`, `aws_cloudfront`, `aws_s3_access`, `aws_cloudtrail`, `aws_vpc_flow`, `aws_waf`, `aws_route53_resolver` |
| Databases and runtimes | `postgresql_stderr`, `postgresql_csvlog`, `postgresql_jsonlog`, `mysql_error`, `mysql_slow`, `mongodb`, `redis`, `log4j2_default`, `logback_default`, `python_logging` |
| Google | `gcp_cloud_logging`, `android_logcat`, `chrome_net_export` |
| Containers and Linux | `kubernetes_cri`, `journald_json`, `linux_auditd`, `linux_dmesg` |
| Microsoft | `ms_iis`, `ms_windows_firewall`, `ms_dns_debug`, `ms_dhcp_audit`, `ms_exchange_tracking`, `ms_sqlserver_errorlog`, `azure_storage_analytics`, `azure_monitor`, `windows_evtx` |
| Network security monitoring | `zeek_tsv`, `zeek_json`, `suricata_eve`, `snort_fast` |
| Web servers and proxies | `nginx_error`, `apache_error`, `haproxy_http`, `squid_access`, `envoy_access`, `caddy_access`, `traefik_access` |
| Your own | `regex` |

Son 56 formatos integrados y `regex`. [tool-verified: `BUILT_IN` enumerated with the project interpreter]

**Cómo obtiene columnas un formato.** Los formatos obtienen sus columnas de cuatro maneras. [tool-verified: `provisa/logparse/formats.py`; `provisa/logparse/presets/_shared.py`; `provisa/logparse/coltypes.py`]

- **Columnas fijas.** La mayoría de los formatos declaran sus columnas y tipos. `rfc3164`, por ejemplo, da `timestamp`, `facility`, `severity`, `host`, `process`, `pid` y `message`; `facility` y `severity` se calculan a partir del valor `<PRI>`. Una línea de syslog BSD no lleva año, así que el año es el de la última escritura del archivo, o el anterior cuando el mes de la línea es posterior al de esa escritura.
- **Campos nombrados por el registro.** `jsonl` y `logfmt` toman sus columnas de los campos vistos en las primeras líneas de los archivos (ajuste `logs.preview_lines`, por defecto 1000). Cada columna recibe el tipo más estrecho en el que caben todos los valores vistos: boolean, luego integer, float, timestamp y json, y si no text. Un campo que lleva un registro posterior y que ninguna columna tiene va a una columna json `extra`.
- **Campos nombrados por una cabecera.** `w3c` y los presets construidos sobre él leen la línea `#Fields:` de cada archivo; `date` y `time` se unen en una sola columna `timestamp`. `aws_cloudfront`, `ms_iis`, `ms_windows_firewall`, `ms_exchange_tracking` y `zeek_tsv` leen las líneas de cabecera `#` del mismo modo, y `aws_vpc_flow` lee una línea de cabecera que empieza por `version`. El lugar donde se leyó una línea de cabecera se queda con el archivo, de modo que un archivo que se sigue leyendo después conserva sus campos.
- **`regex`.** Cada grupo con nombre, como `(?P<level>…)`, es una columna de tipo text.

La pantalla Register Table lista estas columnas con sus tipos y marca la clave. [tool-verified: `provisa/api/admin/schema_query.py` `_logs_columns`] Al leer las filas, cada valor se convierte al tipo que su columna tiene en la tabla registrada, de modo que un tipo cambiado allí es el que se aplica. Un valor que su columna no puede contener pasa a NULL y su motivo va a `_parse_error`. [tool-verified: `provisa/logs/loader.py` `_load`; `provisa/logs/files.py` `_typed`] [inferred: that the screen lets a steward change a suggested type, as REQ-1996 states]

Un registro más largo que `logs.max_line_bytes` (por defecto 65536) conserva sus primeros bytes en `_raw` y no se analiza; `_parse_error` da su longitud. Un archivo que empieza con una marca de orden de bytes UTF-8 o UTF-16 se lee con esa codificación, y cualquier otro como UTF-8. [tool-verified: `provisa/core/settings_catalog.py`; `provisa/logs/files.py` `rows_of`, `text_encoding`]

**Registros que ocupan varias líneas.** Un formato con patrón de inicio de registro forma un registro con una línea que coincide más todas las líneas siguientes que no coinciden. Un stack trace se queda con su excepción, y una entrada de consulta lenta se queda entera. Las líneas anteriores a la primera coincidencia son un registro propio; el formato suele rechazarlo, y la fila se conserva. Los formatos integrados con inicio de registro son `rfc3164`, `macos_install`, `aws_cloudtrail`, `postgresql_stderr`, `postgresql_csvlog`, `mysql_error`, `mysql_slow`, `log4j2_default`, `logback_default`, `python_logging`, `chrome_net_export`, `ms_sqlserver_errorlog` y `azure_monitor`. Con `regex`, defina usted `record_start`. [tool-verified: `provisa/logparse/framing.py` `records`; `record_start` of each format enumerated with the project interpreter]

Dos formatos leen sus registros por sí mismos en lugar de por líneas. `kubernetes_cri` une en un registro las líneas parciales (`P`) que el runtime dividió, y `windows_evtx` lee el archivo binario `.evtx`. [tool-verified: `provisa/logparse/presets/linux.py` `KubernetesCri`; `frames_bytes` enumerated with the project interpreter]

Un archivo en el que aún se está escribiendo se lee hasta su última línea completa. En un formato multilínea se lee hasta el inicio de su último registro, que la línea siguiente podría continuar. Cuando el archivo pasa 2 segundos sin escrituras, ese último registro se toma como completo. [tool-verified: `provisa/logs/files.py` `QUIET_SECONDS`, `read_file`]

**Rotación.** Un archivo se identifica por una suma de verificación de sus primeros 4096 bytes, descomprimidos, no por su nombre. Cuando `system.log` rota a `system.log.0` y luego se comprime a `system.log.0.gz`, cada copia es el mismo archivo. La lectura continúa desde el byte al que llegó la última, así que ninguna línea se lee dos veces. [tool-verified: `provisa/logs/files.py` module docstring, `identify`; `tests/unit/test_logs_source.py` `test_a_rotated_and_compressed_file_is_read_on_and_no_line_twice`]

- La compresión se reconoce por los primeros bytes del archivo, nunca por su nombre: gzip y bzip2. Un archivo comprimido se trata como completo.
- Una copia rotada y su gemela comprimida, ambas coincidentes, se leen una vez, desde la comprimida.
- Un archivo que resulta más corto que el desplazamiento ya leído fue truncado: es un archivo nuevo y se lee desde su inicio. Las filas del antiguo permanecen.
- Los archivos de menos de 4096 bytes que empiezan igual se distinguen por el inodo, así que siguen siendo dos archivos.
- Un archivo con el que el glob ya no coincide se olvida; sus filas permanecen como historial.

[tool-verified: `provisa/logs/files.py` `compression`, `Found.complete`, `identify`; `provisa/logs/loader.py` module docstring; `tests/unit/test_logs_source.py`]

**Actualización y entrega en vivo.** Una tabla Logs es una réplica. Nada vigila los archivos. En cambio, cuando la señal de cambio del origen es `probe` o `ttl_probe`, su probe calcula un digest de la ruta, el inodo, el tamaño y la última escritura de cada archivo coincidente; cuando el digest difiere, se pide una actualización. Los tipos de probe que admite un origen Logs son `hash` y `none`. [tool-verified: `provisa/logs/loader.py` `files_token`, `_freshness_probe`; `provisa/events/probes.py` `_LOADER_PROBED`, `probe_capabilities`]

Una actualización lee solo lo nuevo. Cada archivo se lee desde donde llegó la última pasada, y las filas se escriben por su clave, de 5000 en 5000, antes de guardar la nueva posición. Una pasada que se detiene entre ambos pasos vuelve a leer esos registros, lo que deja las mismas filas. Cada pasada sella sus filas en `_landed_at`, la columna que la entrega en vivo usa como marca de agua. Una definición de tabla cambiada es una reconstrucción completa que lee cada archivo desde su inicio. [tool-verified: `provisa/logs/loader.py` `_UnitsRead`, `_WholeRead`, `BATCH_ROWS`, `replica_stamp_column`; `provisa/federation/replica_builds.py` `_kept_in_place`] [inferred: that subscriptions deliver a Logs table's new rows by `_landed_at`, as REQ-1996 states; the subscription side was not traced]

**Raíces permitidas.** El ajuste de operador `logs.allowed_roots` lista los directorios en los que puede resolverse un glob. Su valor por defecto es `["/var/log"]`; es una lista en la tarjeta Security de Admin → Configuración, que también se fija con `PROVISA_LOGS_ALLOWED_ROOTS`, y un cambio pide confirmación. Cada raíz se resuelve tras expandir `~` y seguir los enlaces simbólicos. [tool-verified: `provisa/core/settings_catalog.py` `logs.allowed_roots`; `provisa/logs/roots.py` `allowed_roots`]

Se aplican dos comprobaciones. La parte fija del glob, hasta su primer comodín, debe resolverse dentro de una raíz; esto se comprueba al guardar el origen y cada vez que se previsualiza o se lee. Cada archivo coincidente debe además resolverse dentro de una raíz una vez seguidos sus enlaces simbólicos, de modo que un enlace simbólico que salga de `/var/log` se rechaza. El rechazo nombra la ruta, a qué se resolvió y las raíces permitidas:

```text
/home/me/app.log resolves to /home/me/app.log, outside the directories log files may be read from: /var/log
```

Al guardar, el código es `schema.logs_path_refused`; en la vista previa es un 403 con `logs.path_refused`. [tool-verified: `provisa/logs/roots.py` `checked_glob`, `checked_file`; `provisa/api/admin/schema_common.py` `logs_refusal`; `provisa/api/admin/logs_router.py`; `tests/unit/test_logs_source.py` `test_a_symlink_escaping_the_root_is_refused_by_name`]

**Rechazo en entorno alojado (SaaS).** Un despliegue que aloja a muchas organizaciones no lee ningún archivo de registro propio. Toda comprobación de ruta rechaza primero, al guardar, al previsualizar y al leer, con "`<path>`: a server that holds many organizations reads no log file of its own". Los archivos son del servidor, no de una organización concreta, así que ningún inquilino puede leerlos. El despliegue sabe si aloja a muchas organizaciones por su estado de multitenencia. [tool-verified: `provisa/logs/roots.py` `_refuse_on_saas`; `provisa/core/secrets_providers.py` `many_organisations`; `provisa/api/app.py` `set_many_organisations_resolver(lambda: state.multitenancy)`; `tests/unit/test_logs_source.py` `test_a_server_of_many_organizations_reads_no_log_file_of_its_own`]

#### Conjuntos de datos de Kaggle (REQ-1780, REQ-1781, REQ-1782, REQ-1783) {#kaggle-datasets} {: #kaggle-datasets }

Kaggle es una plataforma de descarga de archivos. Un conjunto de datos de Kaggle preparado se registra como un origen de tipo `files` y se consulta a través del mismo conector pgwire-file que usa cualquier otro origen `files` — no existe un `SourceType` `kaggle` en el enum. [tool-verified: `provisa/kaggle/downloader.py`; `provisa/core/models.py` `SourceType` — sin literal `kaggle`]

**Agregar un conjunto de datos.** Abra Orígenes → Suscripciones → Kaggle. El formulario ejecuta dos pasos secuenciales, porque el propio endpoint de búsqueda de conjuntos de datos de Kaggle exige autenticación — el selector no puede aparecer antes de que el token se valide. [tool-verified: `provisa-ui/src/pages/sources/KaggleFormSection.tsx`] (REQ-1783)

1. **Token** — ingrese un token de API de Kaggle y haga clic en "Validate". La validación llama a `POST https://www.kaggle.com/api/v1/datasets/create/new` con un cuerpo vacío. Un `401` significa inválido; cualquier otra respuesta significa válido (la propia validación de payload de Kaggle se dispara antes de crear ningún conjunto de datos — no se persiste nada). [tool-verified: `provisa/kaggle/client.py` `validate_token`] (REQ-1782)
2. **Selector** — un campo de búsqueda en vivo mientras escribe consulta `GET /api/v1/datasets/list`. Cada resultado muestra el título y la descripción del conjunto de datos. Seleccione uno y haga clic en "Add Dataset".

Al hacer clic en "Add Dataset" se descarga el paquete desde `GET /api/v1/datasets/download/{owner}/{ref}`, se descomprimen los miembros CSV y Parquet en `<PROVISA_DATA_DIR>/kaggle/<owner>/<ref>/<file-stem>/<file-name>`, y se crea **un** origen de tipo `files` cuyo `path` es ese directorio raíz. El token nunca se almacena del lado del servidor. [tool-verified: `provisa/kaggle/downloader.py` `stage_dataset`; `provisa-ui/src/pages/sources/KaggleFormSection.tsx` `handleConfirmDataset`] (REQ-1780, REQ-1781)

Después de agregar el origen, registre sus tablas mediante la pantalla normal de Register Table. El descubrimiento recursivo de directorios del conector pgwire-file lista cada archivo como su propia tabla — el mismo mecanismo que usa cualquier otro origen `files` (REQ-1690). (REQ-1783)

**Limitación en v1.** Un paquete que contenga un archivo `.sqlite` o `.db` se rechaza directamente con un error claro. Solo se preparan archivos CSV y Parquet. [tool-verified: `provisa/kaggle/downloader.py` `UnsupportedKaggleDataset`, `_UNSUPPORTED_EXTENSIONS`]

**Nomenclatura de tablas.** Una tabla rastreada se llama `<page>__<name>`: el título de la página y luego el pie de la tabla o, si no tiene, el último encabezado sobre la tabla, ambos con la grafía de nombres de columna del origen. Por ejemplo, la tabla bajo el encabezado "Tallest buildings in the world" en la página `List_of_tallest_buildings` es `list_of_tallest_buildings__tallest_buildings_in_the_world`. Un archivo de datos enlazado conserva su propio nombre. [tool-verified: a live crawl of that page through the 0.109.0 file bundle]

**Actualización.** Para volver a obtener un conjunto de datos después de que Kaggle publique una nueva versión, llame a la mutación GraphQL `refreshKaggleSource` con el ID del origen y un token válido. Esto vuelve a preparar los archivos en el mismo lugar y desaloja la caché del endpoint pgwire-file para que el conector detecte cualquier cambio de esquema en su siguiente consulta. Los valores `kaggle_owner` y `kaggle_ref` almacenados en `federation_hints` al momento de la creación identifican qué conjunto de datos volver a obtener. [tool-verified: `provisa/api/admin/schema_mutation.py` `refresh_kaggle_source`] (REQ-1780)

**Sin ruta de configuración YAML estática.** Los orígenes de Kaggle se crean solo mediante el formulario de Orígenes. Un origen de Kaggle exportado a YAML aparece como `type: files` con `kaggle_owner` y `kaggle_ref` en `federation_hints`. Volver a descargar desde Kaggle requiere el flujo de actualización de la UI o la mutación `refreshKaggleSource` — apuntar el `path` del YAML a un directorio ya preparado es la alternativa para entornos aislados de la red (air-gapped).

#### Wikipedia (REQ-1960) {#wikipedia} {: #wikipedia }

Wikipedia rastrea las páginas que usted indica. Cada tabla de datos de una página, y cada archivo CSV, TSV, Excel, JSON o Parquet enlazado, se convierte en una tabla del origen. Como Kaggle, se registra como un origen `files` con un `crawl` en su mapping. Agregarlo no registra nada: un steward registra las tablas deseadas mediante la pantalla normal de Register Table. [tool-verified: `provisa/file_source/wikipedia.py` `crawl_settings`, `DATA_FILE_EXTENSIONS`; `provisa-ui/src/pages/sources/constants.ts` `wikipedia: "files"`] (REQ-316)

**Campos de configuración.**

| Campo | Predeterminado | Significado |
| --- | --- | --- |
| Pages to start from | ninguno (obligatorio) | Uno por línea: el título de una página, o la dirección de una página de la edición elegida |
| Language edition | `en` | El código de la edición, por ejemplo `en`, `de` o `ja` |
| Link depth | `1` | A cuántos enlaces de distancia de una página inicial se lee; `0` lee solo las páginas iniciales |
| Page limit | `25` | El máximo de páginas que lee un rastreo |
| Contact | vacío | Se envía con cada solicitud para que Wikipedia pueda localizarle, como pide su política |
| Follow "See also" links | desactivado | Desactivado deja esa sección fuera del artículo |
| What this edition calls its "See also" section | vacío | Solo se pregunta cuando la casilla anterior está desactivada y Provisa no puede nombrar la sección de la edición elegida. El título de la sección tal como lo muestra una página, por ejemplo `Voir aussi` |
| Directory for the crawl's files | vacío | Dónde se escriben las tablas y los archivos de datos. Vacío: `<PROVISA_DATA_DIR>/crawl/<source id>` |

[tool-verified: `provisa-ui/src/pages/sources/WikipediaFields.tsx`; `provisa-ui/src/i18n/locales/en/sourceFormFieldsExtended.json` `wp*`; `provisa/file_source/wikipedia.py` `DEFAULTS`; `provisa/file_source/crawl.py` `crawl_landing_directory`]

Provisa conoce el encabezado "See also" de estas ediciones: `en`, `de`, `es`, `it`, `pt`, `nl`, `pl`, `sv`, `ja`, `zh`, `ru`. Para cualquier otra edición con "See also" desactivado, el formulario pregunta cómo llama la edición a esa sección; escriba su título tal como lo muestra una página (por ejemplo `Voir aussi`). Un origen guardado sin él se rechaza (`wikipedia.see_also_heading_needed`). [tool-verified: `provisa/file_source/wikipedia.py` `SEE_ALSO_HEADING`, `crawl_settings`]

**Qué enlaces se siguen.** Un enlace se elige por lo que dice el propio marcado de la página sobre él, nunca por un nombre en su dirección: esos nombres difieren en cada edición de idioma y también aparecen en títulos corrientes. [tool-verified: `provisa/file_source/wikipedia.py` module docstring]

- Se siguen: enlaces a otros artículos que están en el texto, las listas y las tablas de datos del propio artículo. La marca de marcado es `a[rel='mw:WikiLink']`.
- No se siguen: páginas aún no escritas, la propia página, búsquedas de fuentes de libros, páginas de archivos, otros wikis y sitios externos.
- No se leen en absoluto (se quitan antes de reunir enlaces y tablas): cuadros de navegación, cuadros de información, notas y citas, categorías, el índice, imágenes y avisos de mantenimiento.
- No se leen salvo que active la opción: la sección "See also".

Solo las tablas que los editores marcan como tablas de datos (`table.wikitable`) se convierten en tablas, y una tabla con menos de 2 filas no se ofrece. [tool-verified: `provisa/file_source/wikipedia.py` `LINK_SELECTOR`, `REMOVE_SELECTORS`, `TABLE_SELECTOR`, `DEFAULTS`]

**Ajustes avanzados de rastreo.** El formulario los mantiene plegados bajo "Crawl settings" y muestra cada uno con su valor predeterminado; deje un campo vacío para conservarlo. En un mapping de `files` los mismos nombres van bajo `crawl:`, y un nombre fuera de esta lista se rechaza. [tool-verified: `provisa/file_source/crawl.py` `CRAWL_SETTINGS`, `crawl_operand`; `provisa/file_source/wikipedia.py` `crawl_settings`]

| Ajuste | Predeterminado en Wikipedia | Significado |
| --- | --- | --- |
| `start_urls` | las páginas que usted indicó | Páginas desde las que empieza el rastreo |
| `max_depth` | `1` | Profundidad de enlaces |
| `max_pages` | `25` | Límite de páginas |
| `request_delay` | `1 seconds` | Pausa entre solicitudes |
| `user_agent` | `Provisa/<version> (+https://provisa.dev) file-crawler`, con su contacto añadido | El nombre con que se presenta el rastreo |
| `content_selector` | `#mw-content-text .mw-parser-output` | Selector CSS: los enlaces y las tablas se leen solo dentro de él |
| `remove_selectors` | la lista de partes descrita arriba | Selectores CSS de las partes que se dejan fuera. Indicarlo reemplaza toda la lista, incluida la regla de "See also" |
| `link_selector` | `a[rel='mw:WikiLink']` con exclusiones | Selector CSS que un enlace debe cumplir para seguirse |
| `link_exclude_patterns` | vacío | Expresiones regulares; un enlace cuya dirección coincide con una no se sigue |
| `follow_external_links` | `false` | Seguir enlaces a otros sitios |
| `allowed_domains` | sin definir | Dominios que el rastreo puede visitar |
| `table_selector` | `table.wikitable` | Selector CSS que una tabla debe cumplir |
| `generate_tables_from_html` | sin definir | Si las tablas HTML se convierten en tablas |
| `html_table_min_rows` | `2` | Mínimo de filas de una tabla |
| `html_table_max_rows` | sin definir | Máximo de filas de una tabla |
| `allowed_file_extensions` | `csv`, `tsv`, `xlsx`, `xls`, `json`, `parquet` | Tipos de archivo enlazados que se convierten en tablas |
| `html_cache_ttl` | `1 days` | Cuánto tiempo se conserva un rastreo |
| `max_html_size` | sin definir | Página más grande que se lee, por ejemplo `10MB` |
| `max_data_file_size` | sin definir | Archivo de datos más grande que se lee, por ejemplo `100MB` |

**Nomenclatura de tablas.** Una tabla rastreada se llama `<page>__<name>`. El nombre es el `id` propio de la tabla si lo tiene; si no, su pie; si no, el encabezado más cercano por encima. Por ejemplo, una tabla bajo el encabezado "Tallest buildings" en la página `List_of_tallest_buildings` pasa a ser `list_of_tallest_buildings__tallest_buildings` tras la normalización del nombre. [tool-verified: calcite `file` adapter `HtmlToJsonConverter.java` (`baseFileName + "__" + tableName`), `HtmlTableScanner.java` `getTableName`] [inferred: the exact normalized spelling in the example]

**Actualización.** Un rastreo se conserva durante el período de caché, `1 days` de forma predeterminada (`html_cache_ttl`). Las páginas solo se obtienen de nuevo cuando ha pasado ese tiempo. [tool-verified: `provisa/file_source/wikipedia.py` `DEFAULTS`]


### Observabilidad y otros {#observability-other} {: #observability-other }
`prometheus` tiene un conector de Trino (propiedades construidas a partir del DSL de mapeo del tipo). `google_sheets` es un tipo de origen registrado sin conector de Trino y se materializa a través del pipeline de caché de API. [tool-verified: `provisa/federation/trino_connectors.py:314`; `provisa/core/models.py` lines 87–88]

| Tipo de origen | Nombre del conector | Mutaciones |
| ------------ | ----------------- | ----------- |
| `google_sheets` | — (materializado) | No |
| `prometheus` | prometheus | No | Una métrica es una tabla y una muestra es una fila (`timestamp`, `value`, una columna por etiqueta); en cualquier motor sin conector en vivo Provisa lee la API HTTP — nombres de métricas y etiquetas para Register Table, `query_range` sobre el rango de la tabla para el aterrizaje [tool-verified: `provisa/prometheus/fetch.py`] (REQ-1689) |

### Conectores SaaS empresariales {#enterprise-saas-connectors} {: #enterprise-saas-connectors }
Google Workspace y Microsoft 365 los leen cargadores propios; véase [Google Workspace y Microsoft 365](#google-workspace-and-microsoft-365). SharePoint, Salesforce, ServiceNow, el inventario de nube y Splunk se registran mediante conectores de Apache Calcite (kenstott/calcite fork). Ninguno tiene controlador directo — Provisa lanza el servidor pgwire de Calcite incluido en el conector (`pgwire-sharepoint`, `pgwire-salesforce`, `pgwire-servicenow`, `pgwire-cloudops`, `pgwire-splunk`) y lo alcanza como un endpoint genérico de PostgreSQL. En el motor DuckDB ese endpoint se conecta en vivo mediante la extensión postgres: Register Table lista las tablas del conector desde el catálogo conectado, las consultas leen el conector en su sitio, y los filtros y proyecciones se empujan hacia abajo hasta Calcite (REQ-1690) [tool-verified: `provisa/federation/connector_duckdb.py` `_DuckDBPgwireConnector`]. Cualquier otro motor lleva las filas al almacén de materialización para la federación (REQ-954). Los paquetes se obtienen por SO/arquitectura desde la versión fijada de `kenstott/calcite` (`pgwire-<connector>-<version>-<os>-<arch>.tar.gz`; macOS arm64, Linux x86_64, Windows x86_64) [tool-verified: `provisa/runtime_deps/pgwire_bundles.py`] Un operador puede en cambio nombrar una carpeta de paquete construida para este host, por conector, en Admin → Configuración → Servidores de conectores (`connectors.<connector>.bundle_dir`); consulte [Admin](admin.md).. Los cinco conectores siempre habilitan la coincidencia de nombres sin distinción de mayúsculas y minúsculas, coincidiendo con la semántica propia de cada producto en ese aspecto (REQ-725, REQ-730). [tool-verified: `provisa/core/models.py` lines 99–100; `provisa/federation/trino_connectors.py` lines 223–286]

#### `sharepoint`

Las listas de SharePoint se enumeran como esquemas y se exponen como tablas consultables (REQ-726, REQ-731). Dos métodos de autenticación: `CLIENT_CREDENTIALS` (predeterminado) y basado en certificado mediante un certificado PFX (REQ-727). Los valores secretos en `mapping` se resuelven a través del motor de secretos antes de llegar al conector (REQ-729). [tool-verified: `provisa/federation/trino_connectors.py` lines 230–252]

| Campo de origen | Propiedad del conector | Notas |
| --- | --- | --- |
| `base_url` o `host` | `site-url` | URL del sitio de SharePoint |
| `username` | `client-id` | ID de cliente de la app de Azure |
| `password` | `client-secret` | Secreto de cliente de la app de Azure |
| `database` | `tenant-id` | UUID del inquilino de Azure |
| `mapping.auth_type` | `auth-type` | `CLIENT_CREDENTIALS` (predeterminado) o `CERTIFICATE` |
| `mapping.certificate_path` | `certificate-path` | Ruta del PFX cuando `auth_type: CERTIFICATE` — debe ser ABSOLUTA |
| `mapping.certificate_password` | `certificate-password` | Contraseña del PFX — la clave debe estar presente, cadena vacía para un PFX sin contraseña |

La autenticación por certificado en los motores que no son Trino conlleva dos reglas adicionales, ambas aplicadas al construir el operando `model.json` del servidor pgwire de Calcite (REQ-1693). `certificate_path` debe ser absoluta: el servidor se ejecuta con su directorio de paquete como directorio de trabajo, de modo que una ruta relativa se resuelve dentro de la caché de runtime-deps y el PFX no se encuentra. `certificate_password` debe estar presente en `mapping` incluso cuando el PFX no tiene contraseña, en cuyo caso es la cadena vacía — el adaptador de Calcite rechaza una contraseña nula de plano, y una clave ausente se trata como un error de configuración en lugar de leerse silenciosamente como una contraseña vacía. Un valor ausente o relativo lanza `MissingConnectorConfig` nombrando el campo. [tool-verified: `provisa/federation/pgwire_replica.py` `_sharepoint_operand`]

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

Autenticación por certificado, con la ruta absoluta y la contraseña siempre presente:

```yaml
- id: hr-sharepoint
  type: sharepoint
  base_url: https://kenstott.sharepoint.com
  username: ${env:SP_CLIENT_ID}
  database: ${env:SP_TENANT_ID}
  mapping:
    auth_type: CERTIFICATE
    certificate_path: /etc/provisa/certs/sharepoint.pfx
    certificate_password: ${env:SP_CERT_PASSWORD}
```

#### `salesforce`

Cada sObject que la credencial puede consultar es una tabla que el steward puede registrar (`Account`, `Opportunity`, objetos personalizados `*__c`); nada se registra automáticamente (REQ-1946). Las columnas y los tipos provienen del describe del sObject. Los filtros, las proyecciones, las ordenaciones y los límites se delegan a Salesforce como SOQL. [tool-verified: `provisa/federation/pgwire_replica.py` `_salesforce_operand`; `provisa/federation/trino_connectors.py` `TrinoSalesforceConnector`]

Una fuente Salesforce indica la URL de inicio de sesión de Mi dominio de la organización y un conjunto completo de credenciales, elegido con `mapping.auth_type`. La URL de inicio de sesión es la URL completa `https://…` (por ejemplo `https://acme.my.salesforce.com`); un host simple o cualquier otro esquema se rechaza, nombrando el campo. Un conjunto incompleto se rechaza cuando se usa la fuente, indicando el valor que falta. Cada secreto puede ser una referencia `${secret:…}` o `${env:…}`. Un secreto escrito como literal se guarda en la bóveda de la organización y la fuente conserva su referencia.

| Campo de origen | Propiedad del conector | Notas |
| --- | --- | --- |
| `base_url` o `host` | `login-url` | La URL de Mi dominio de la organización (Configuración, luego Mi dominio). `login.salesforce.com` no funciona con el flujo de credenciales de cliente |
| `username` | `client-id` | Clave de consumidor de la aplicación conectada |
| `password` | `client-secret` | Secreto de consumidor de la aplicación conectada |
| `mapping.auth_type` | — | `CLIENT_CREDENTIALS` (predeterminado), `USERNAME_PASSWORD` o `ACCESS_TOKEN` |
| `mapping.sf_username`, `mapping.sf_password` | `username`, `password` | Solo `USERNAME_PASSWORD`, junto con la clave y el secreto de consumidor |
| `mapping.security_token` | `security-token` | Solo `USERNAME_PASSWORD`; opcional |
| `mapping.access_token`, `mapping.instance_url` | `access-token`, `instance-url` | Solo `ACCESS_TOKEN`; sin clave ni secreto de consumidor |
| `mapping.api_version` | `api-version` | Opcional, por ejemplo `v61.0` |

Para `CLIENT_CREDENTIALS`, la aplicación conectada necesita *Enable Client Credentials Flow* y un usuario *Run As*; cada lectura y escritura se ejecuta con los permisos de ese usuario. Las organizaciones creadas desde Summer '23 bloquean de forma predeterminada el flujo de usuario y contraseña.

**Lecturas.** En Trino la fuente se lee a través del catálogo `salesforce`, que solo lee. En cualquier otro motor, el motor adjunta el servidor pgwire de la fuente y la lee en su lugar.

**Escrituras.** `INSERT`, `UPDATE` y `DELETE` sobre un sObject registrado son mutaciones: gobernadas, auditadas y tratadas por entorno como cualquier otra mutación. En todos los motores, Trino incluido, una escritura se ejecuta en el servidor pgwire de la fuente; en Trino ese servidor se ejecuta solo para las escrituras. Cada sentencia se envía a Salesforce y se confirma al ejecutarse, por lo que no puede revertirse. El servidor no devuelve las filas que escribe: una sentencia con cláusula `RETURNING` se rechaza y GraphQL no ofrece campos de mutación para estas tablas; `INSERT`, `UPDATE`, `DELETE` y `COPY` de SQL son los caminos de escritura. Una escritura es una sola sentencia sobre una tabla: `INSERT … ON CONFLICT`, `UPDATE … FROM`, `DELETE … USING`, `MERGE` y `TRUNCATE` se rechazan, nombrando la forma. Una fuente SharePoint se escribe de la misma manera. [tool-verified: `provisa/executor/writable.py` `PGWIRE_SERVER_WRITTEN`; `provisa/api/data/pgwire_write.py`]

**Arranque.** Antes de aceptar conexiones, el servidor de la fuente lee las columnas de cada sObject, unos cuatro minutos para una organización con 1.200. Se paga una vez: los resultados de describe se guardan en el directorio de estado propio de la fuente, bajo el directorio de datos de la instancia. El servidor se inicia cuando la fuente se registra o se carga. En Trino el catálogo `salesforce` conserva sus propios resultados de describe, un directorio por fuente bajo `/data/trino/cache/salesforce-describe` en el coordinador, de modo que el coste también se paga una sola vez allí cuando esa ruta es un volumen que sobrevive al contenedor: el volumen `trino_cache` de la pila compose, o el volumen de caché del coordinador del chart de Helm (`trino.coordinator.cache.persistence`). Hasta que acepta conexiones, Registrar tabla muestra el conector como iniciándose y una escritura recibe el mismo mensaje en lugar de esperar. [tool-verified: `provisa/federation/pgwire_replica.py` `build_model_json`, `start_endpoint`]

```yaml
- id: sales-salesforce
  type: salesforce
  base_url: https://acme.my.salesforce.com
  username: ${env:SF_CONSUMER_KEY}
  password: ${env:SF_CONSUMER_SECRET}
  mapping:
    api_version: v61.0
```

Flujo de usuario y contraseña:

```yaml
- id: sales-salesforce
  type: salesforce
  base_url: https://acme.my.salesforce.com
  username: ${env:SF_CONSUMER_KEY}
  password: ${env:SF_CONSUMER_SECRET}
  mapping:
    auth_type: USERNAME_PASSWORD
    sf_username: ops@acme.com
    sf_password: ${env:SF_PASSWORD}
    security_token: ${env:SF_SECURITY_TOKEN}
```

#### `servicenow`

Cada tabla de la instancia está disponible para su registro; nada se registra automáticamente (REQ-1954). Una instancia completa puede tener miles de tablas; Register Table las agrupa por tema — cada tema es el nombre para mostrar de un paquete de ServiceNow, o "(sin paquete)" cuando una tabla no pertenece a ninguno. Cada tabla aparece como `name — label`. Al seleccionar una tabla se leen sus columnas de la instancia; listar las tablas no lo hace. [tool-verified: `provisa/federation/pgwire_replica.py` `servicenow_table_list`, `servicenow_columns`; `provisa/federation/servicenow.py` `TABLE_LIST`, `COLUMN_LIST`]

Una fuente ServiceNow nombra la URL `https://…` completa de la instancia junto con un nombre de usuario y contraseña. La URL se toma de `base_url` o `host`; un host simple o cualquier otro esquema se rechaza, nombrando el campo (`instanceUrl`). Un nombre de usuario o contraseña ausente se rechaza, nombrando el campo. Cada valor puede ser una referencia `${secret:…}` o `${env:…}`. Un secreto escrito como literal se guarda en la bóveda de la organización y la fuente conserva su referencia.

| Campo fuente | Propiedad del adaptador | Notas |
| --- | --- | --- |
| `base_url` o `host` | `instanceUrl` | URL `https://…` completa de la instancia (p. ej. `https://acme.service-now.com`). Un host simple u otro esquema se rechaza |
| `username` | `username` | Nombre de usuario de autenticación básica |
| `password` | `password` | Contraseña de autenticación básica |

[tool-verified: `provisa/federation/servicenow.py` `servicenow_instance_url`, `servicenow_settings`]

**Lecturas.** En Trino la fuente se lee a través del catálogo `servicenow`, que solo lee. En cualquier otro motor, el motor adjunta el servidor pgwire de la fuente y la lee en su sitio. Solo las columnas seleccionadas se solicitan a la instancia (`sysparm_fields`). Un filtro se envía a la instancia (`sysparm_query`) solo en una forma que el adaptador ha verificado contra una instancia en vivo; todos los demás filtros, y cada ordenación y límite, se aplican a las filas leídas. [inferred: the adapter's requirements ledger (kenstott/calcite `docs/requirements/servicenow.yaml`, SNOW-022, SNOW-023, SNOW-030)]

Un campo de referencia aparece como dos columnas: `<field>` contiene el sys_id del registro referenciado y `<field>__display` contiene el valor de visualización. Un campo vacío es NULL. Un campo cuyo tipo no puede mapear el adaptador genera un error que nombra la tabla, el campo y el tipo. [inferred from adapter behavior; no verificado directamente en el código fuente de Provisa en esta sesión]

La búsqueda de tablas (lenguaje natural) coincide con el nombre, la etiqueta y el tema de cada tabla. Una vez registrada una tabla, sus columnas se incorporan a la búsqueda. [tool-verified: `provisa/discovery/catalog_cache.py` docstring de `COLUMNS_PER_TABLE_KINDS`]

**Ciclo de vida del servidor.** El servidor sirve solo las tablas registradas desde él. Registrar o eliminar una tabla reinicia el servidor y vuelve a aprovisionar la fuente en el motor; una sentencia que llegue durante ese reinicio falla. [tool-verified: `provisa/federation/pgwire_replica.py` `SERVES_REGISTERED_TABLES`; `provisa/api/app_rebuild.py` `serve_registered_tables`]

**Escrituras.** `INSERT`, `UPDATE` y `DELETE` sobre una tabla registrada son mutaciones: gobernadas, auditadas y tratadas por entorno como cualquier otra mutación. En todos los motores, Trino incluido, una escritura se ejecuta en el servidor pgwire de la fuente. El servidor devuelve la fila que la instancia tiene tras cada escritura, por lo que una cláusula `RETURNING` se acepta y GraphQL ofrece campos de mutación para estas tablas. Una escritura es una sola sentencia sobre una tabla: `INSERT … ON CONFLICT`, `UPDATE … FROM`, `DELETE … USING`, `MERGE` y `TRUNCATE` se rechazan, nombrando la forma. [tool-verified: `provisa/executor/writable.py` `PGWIRE_SERVER_WRITTEN`, `PGWIRE_SERVER_RETURNS_ROWS`]

**Import sets.** Cuando una tabla tiene un mapa de transformación activo hacia ella, Register Table ofrece sus import sets, y el steward puede vincular la tabla a uno (una tabla con varios mapas nombra cada uno). Las inserciones en una tabla vinculada pasan por la Import Set API de la instancia y la transformación de ese mapa; las inserciones en una tabla no vinculada, y cada actualización y eliminación, se escriben directamente en la tabla. Una fila que la transformación rechaza se rehúsa, nombrando la fila; las filas ya escritas permanecen escritas. La vinculación se comprueba cuando la tabla se guarda: debe ser el import set de un mapa de transformación activo hacia la tabla, en una fuente ServiceNow. [tool-verified: `provisa/api/admin/_import_set.py` `table_import_set_refusal`; `provisa/federation/servicenow.py` `servicenow_settings`; `provisa/federation/pgwire_replica.py` `servicenow_transform_maps`]

```yaml
- id: helpdesk-sn
  type: servicenow
  base_url: https://acme.service-now.com
  username: ${env:SN_USERNAME}
  password: ${env:SN_PASSWORD}
```

#### `cloudops`

El inventario de nube presenta los recursos de Azure, AWS y GCP como un único conjunto de tablas: `compute_resources`, `storage_resources`, `kubernetes_clusters`, `database_resources`, `network_resources`, `iam_resources`, `container_registries` y `compute_security_groups`, que vincula un recurso de cómputo con sus grupos de seguridad. Cada tabla tiene un único conjunto de columnas en las tres nubes, y su columna `cloud_provider` indica la nube de la que procede cada fila. La fuente solo lee, y sus tablas las registra un steward como cualquier otra (REQ-1947). [tool-verified: `provisa/federation/cloudops.py` `cloudops_settings`; `provisa/federation/trino_connectors.py` `TrinoCloudopsConnector`]

Una fuente indica una o varias nubes. Cada nube es todo o nada: una nube con solo parte de sus valores obligatorios se rechaza, indicando lo que falta, igual que una fuente que no indica ninguna nube. La fuente lee exactamente las nubes que indica. Todos los valores están en `mapping`, y cada secreto puede ser una referencia `${secret:…}` o `${env:…}`. Un secreto escrito como literal se guarda en la bóveda de la organización y la fuente conserva su referencia.

| Nube | Clave de `mapping` | Propiedad del conector | Notas |
| --- | --- | --- | --- |
| Azure | `azure_tenant_id` | `azure.tenant-id` | Obligatorio |
| Azure | `azure_client_id` | `azure.client-id` | Obligatorio; el ID de cliente del registro de la aplicación |
| Azure | `azure_client_secret` | `azure.client-secret` | Obligatorio; secreto |
| Azure | `azure_subscription_ids` | `azure.subscription-ids` | Obligatorio; separados por comas |
| AWS | `aws_access_key_id` | `aws.access-key-id` | Obligatorio |
| AWS | `aws_secret_access_key` | `aws.secret-access-key` | Obligatorio; secreto |
| AWS | `aws_region` | `aws.region` | Obligatorio, por ejemplo `us-east-1` |
| AWS | `aws_account_ids` | `aws.account-ids` | Obligatorio; separados por comas |
| AWS | `aws_role_arn` | `aws.role-arn` | Opcional; un rol que asumir para el acceso entre cuentas |
| GCP | `gcp_credentials_path` | `gcp.credentials-path` | Obligatorio; ruta ABSOLUTA a un archivo de clave JSON de una cuenta de servicio |
| GCP | `gcp_project_ids` | `gcp.project-ids` | Obligatorio; separados por comas |
| — | `cache_ttl_minutes` | `cache.ttl-minutes` | Opcional; cuánto tiempo se reutiliza un resultado antes de volver a consultar las nubes (predeterminado del adaptador 5) |

El archivo de clave de GCP lo abre quien lee la fuente. En Trino es el servidor Trino, así que la ruta debe existir allí. En cualquier otro motor es el servidor pgwire de la fuente en el host de Provisa, cuyo directorio de trabajo es su propio directorio de estado, por lo que una ruta relativa se rechaza.

En Trino la fuente se lee a través del catálogo `cloudops`. En cualquier otro motor, el motor adjunta el servidor pgwire de la fuente y la lee en su lugar, como una fuente SharePoint.

```yaml
- id: cloud-estate
  type: cloudops
  mapping:
    aws_access_key_id: ${env:CLOUDOPS_AWS_ACCESS_KEY_ID}
    aws_secret_access_key: ${env:CLOUDOPS_AWS_SECRET_ACCESS_KEY}
    aws_region: us-east-1
    aws_account_ids: "111111111111,222222222222"
    azure_tenant_id: ${env:CLOUDOPS_AZURE_TENANT_ID}
    azure_client_id: ${env:CLOUDOPS_AZURE_CLIENT_ID}
    azure_client_secret: ${env:CLOUDOPS_AZURE_CLIENT_SECRET}
    azure_subscription_ids: "sub-1,sub-2"
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

En la ruta pgwire-replica (cualquier motor salvo Trino) los mismos cuatro ajustes opcionales se convierten en las claves del operando `model.json` de Calcite `app`, `token`/`username`+`password`, `datamodelFilter` y `disableSslValidation` — las dos últimas con los tipos a los que `SplunkSchemaFactory` las convierte, una cadena y un booleano (REQ-1694). [tool-verified: `provisa/federation/pgwire_replica.py` `_splunk_operand`]

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

#### Google Workspace y Microsoft 365 (REQ-1923, REQ-1994) {#google-workspace-and-microsoft-365} {: #google-workspace-and-microsoft-365 }

`google_workspace` y `microsoft_365` leen el correo, los calendarios y las tareas de las cuentas que nombra un origen, y los llevan como réplicas. No son conectores de Calcite: cada uno tiene un cargador propio que llama a la API del proveedor, Gmail, Google Calendar y Google Tasks por un lado y Microsoft Graph por el otro. El formulario los lista en Enterprise como "Google Workspace (Gmail)" y "Microsoft 365 (Outlook, Exchange)". [tool-verified: `provisa/core/models.py` `SourceType`; `provisa/federation/strategy.py` `_MATERIALIZE_ONLY`; `provisa-ui/src/pages/sources/constants.ts`]

**Tablas.** Ambos tipos producen las mismas tablas canónicas: los mismos nombres, tipos de columna y orden. Una unión de los dos no necesita conversión ni cambio de nombre; una columna para la que un proveedor no tiene valor es NULL. Las tablas se listan bajo el esquema `default`, y cada fila empieza con la cuenta de la que se leyó. Un origen ofrece las tablas de los recursos que lee: [tool-verified: `provisa/core/canonical_mail.py` module docstring, `RESOURCE_TABLES`, `tables_for`; `provisa/google_workspace/loader.py` `SCHEMA`]

| Recurso | Tablas | Escritas por sentencias |
| --- | --- | --- |
| `mail` | `messages`, `message_recipients`, `folders`, `message_folders`, `threads`, `attachments` | No |
| `calendar` | `calendars`, `events`, `event_attendees` | `events`, `event_attendees` |
| `tasks` | `task_lists`, `tasks` | `tasks` |

Las columnas se listan en [el esquema canónico de correo](arch/canonical-mail-schema.md). `events` contiene eventos únicos, maestros de serie con su recurrencia como líneas RFC 5545, y excepciones; nunca contiene las ocurrencias expandidas de una serie. [tool-verified: `provisa/google_workspace/calendar_read.py` module docstring] (REQ-1994)

**Registro.** El administrador de una organización introduce primero el cliente de la organización para la plataforma en Admin → Email: un ID de cliente y un secreto, más el ID de directorio (tenant) o el dominio en Microsoft 365. Quien agrega un origen nunca lo ve ni lo escribe. Un origen toma entonces una de dos formas: [tool-verified: `provisa/core/mail_platforms.py` module docstring; `provisa/microsoft365/__init__.py`; `provisa-ui/src/i18n/locales/en/mailPlatforms.json`]

- **Un buzón.** El origen nombra una dirección en `accounts`. Su propietario hace clic en **Connect** en el formulario y aprueba el cliente de la organización. La aprobación resultante se guarda en el almacén de estado de la organización, no en los ajustes del origen. En Google Workspace esta forma declara además `sign_in: google_account`.
- **Los buzones de la organización.** El origen declara `mailboxes` como exactamente uno de `everyone: true`, `group: <address or id>` o `list: [addresses]`. No guarda ninguna credencial y nadie lo conecta. La organización lee con su propio acceso, y solo una vez que el administrador ha activado "Read the organization's mailboxes". En Google ese acceso es una clave de cuenta de servicio con delegación en todo el dominio, más una dirección de administrador para las llamadas al directorio. En Microsoft es la concesión de credenciales de cliente del registro de la aplicación.

[tool-verified: `provisa/google_workspace/settings.py` `parse`, `_mailboxes`; `provisa/microsoft365/settings.py` `parse`; `provisa/core/mail_platforms.py` `organisation_mailboxes`]

Con la segunda forma, una tabla contiene las filas de todos los buzones. Un rol que puede leer la tabla lee todos los buzones, salvo que una regla de fila sobre la columna `account` lo limite. [tool-verified: `provisa-ui/src/i18n/locales/en/googleWorkspaceFields.json` `everyMailbox`]

```yaml
sources:
  - id: team-mail
    type: microsoft_365
    mapping:
      resources: [mail, calendar, tasks]
      mailboxes:
        group: support@acme.com
      mail_since: "2026-01-01"
      calendar_check_days: 30        # required when calendar is read
      calendar_send_updates: false
```

**Ajustes.** Cada módulo rechaza por su nombre una clave que no lee. [tool-verified: `provisa/google_workspace/settings.py` `_KEYS`; `provisa/microsoft365/settings.py` `_KEYS`]

| Clave de `mapping` | Tipo | Significado |
| --- | --- | --- |
| `resources` | ambos | Cuáles de `mail`, `calendar`, `tasks` lee el origen; al menos uno |
| `accounts` | ambos | La dirección del buzón único (forma de un buzón) |
| `mailboxes` | ambos | `everyone`, `group` o `list` (forma de organización) |
| `sign_in` | `google_workspace` | `google_account` (forma de un buzón) |
| `mail_since` | ambos | `YYYY-MM-DD`; no se conserva nada recibido antes |
| `mail_content` | `google_workspace` | `full`, o `headers` solo para cabeceras y etiquetas (el alcance `gmail.metadata` de Gmail). Obligatorio cuando se lee correo |
| `mail_search` | `google_workspace` | Una búsqueda tal como se escribe en el cuadro de búsqueda de Gmail |
| `mail_labels` | `google_workspace` | Nombres de etiqueta; solo se lee el correo que lleva todas |
| `mail_include_spam_trash` | `google_workspace` | `false` por defecto |
| `calendar_send_updates` | ambos | Si una escritura avisa a los asistentes de un evento; desactivado por defecto |
| `calendar_check_days` | `microsoft_365` | Días a cada lado de hoy que mira la comprobación de cambios; un entero, 1 o más, obligatorio cuando se lee el calendario |

Google rechaza una búsqueda bajo `gmail.metadata`, así que `mail_content: headers` con `mail_search` o `mail_since` se rechaza. [tool-verified: `provisa/google_workspace/settings.py` `_mail`]

**Alcances.** Un inicio de sesión pide solo los alcances de los recursos marcados: [tool-verified: `provisa/google_workspace/settings.py` `MAIL_SCOPES`, `RESOURCE_SCOPES`; `provisa/google_workspace/sign_in.py` `EMAIL_SCOPE`; `provisa/microsoft365/settings.py` `RESOURCE_SCOPES`, `BASE_SCOPES`; `provisa/microsoft365/directory.py`]

| Recurso | Google Workspace | Microsoft 365 (delegado) |
| --- | --- | --- |
| `mail` | `gmail.readonly`, o `gmail.metadata` solo para cabeceras | `Mail.Read` |
| `calendar` | `calendar.calendarlist.readonly`, `calendar.events` | `Calendars.ReadWrite` |
| `tasks` | `tasks` | `Tasks.ReadWrite` |
| todo origen | `userinfo.email` | `offline_access`, `User.Read` |

Para la forma de organización, Microsoft necesita los permisos de aplicación `Mail.Read` y `User.Read.All`, más `Calendars.ReadWrite` y `Tasks.ReadWrite.All` para calendarios y tareas. La cuenta de servicio de Google está autorizada para los alcances de `DELEGATED_SCOPES`, que incluyen también la lectura de usuarios y miembros de grupo del directorio. [tool-verified: `provisa/microsoft365/directory.py`; `provisa/google_workspace/__init__.py` `DELEGATED_SCOPES`; `provisa-ui/src/i18n/locales/en/mailPlatforms.json` `organisationGrant`]

**Actualización y frescura.** Los tipos de probe que admiten estos orígenes son `hash` y `none`, y su cargador aporta el token de cambio: [tool-verified: `provisa/events/probes.py` `_MAIL_TYPES`, `probe_capabilities`]

- **Gmail:** un digest del history id actual de cada buzón, una llamada a `users.getProfile` por buzón. [tool-verified: `provisa/google_workspace/loader.py` `mailbox_token`]
- **Correo de Microsoft:** cada enlace delta conservado, seguido para un elemento. [tool-verified: `provisa/microsoft365/loader.py` `unchanged`]
- **Calendarios y tareas de Google:** la etiqueta y la hora del último cambio de cada calendario; la etiqueta de cada lista de tareas, y el id y la hora de actualización de cada tarea. [tool-verified: `provisa/google_workspace/calendar_read.py` `change_token`]
- **Calendarios y tareas de Microsoft:** las líneas de cambio de cada buzón, durante `calendar_check_days` a cada lado de hoy. [tool-verified: `provisa/microsoft365/loader.py` `_calendar_token`] [inferred: that a line is an event's id and change key, or a task's update time, from REQ-1994]

Una comprobación que no puede hacerse pide una lectura; nunca informa "sin cambios". Las tablas de calendario y tareas se leen enteras en cada actualización. Las réplicas de correo de un origen Microsoft 365 o Google Workspace se mantienen en su sitio: cada parte de un buzón se escribe a medida que llega y puede consultarse de inmediato. [tool-verified: `provisa/core/canonical_mail.py` `RESOURCE_TABLES` comment; `provisa/federation/replica_builds.py` `GROUP_READ_TYPES`; `provisa/federation/mail_units.py`] Un origen Google Workspace que acota con `mail_search` se vuelve a leer entero en cada actualización, porque solo Google puede decir qué coincide con una búsqueda. [tool-verified: `provisa/google_workspace/units.py` module docstring]

**Escrituras.** `INSERT`, `UPDATE` y `DELETE` sobre `events`, `event_attendees` y `tasks` van al proveedor por una sola vía de escritura para ambos tipos. Un `UPDATE` o `DELETE` nombra sus filas por `provider`, `account` e `id`; un asistente se nombra por `event_id` y `address`. Una ocurrencia de una serie se nombra por `series_id` y `original_start_at`. Una escritura que fija una columna fuera del conjunto escribible de la tabla se rechaza por su nombre. Google escribe con `sendUpdates=all` cuando `calendar_send_updates` está activado y con `sendUpdates=none` cuando está desactivado. Microsoft Graph no puede hacer ese cambio en silencio, así que se rechaza un cambio en los asistentes de una reunión, nombrando el ajuste. [tool-verified: `provisa/executor/writable.py` `API_WRITTEN`; `provisa/core/canonical_mail.py` `WRITE_KEYS`, `WRITABLE_COLUMNS`; `provisa-ui/src/i18n/locales/en/mailResources.json` `sendUpdatesHelp`] [tool-verified: `provisa/google_workspace/calendar_write.py`]

**Exclusiones.** El correo es de solo lectura; ninguna sentencia cambia un buzón. `calendars`, `task_lists` y todas las tablas de correo no admiten escrituras. Spam y Papelera de Gmail se dejan fuera salvo que `mail_include_spam_trash` esté activado. La API Tasks de Google guarda solo el día en que vence una tarea y no tiene recordatorio. [tool-verified: `provisa/microsoft365/settings.py` `RESOURCE_SCOPES`; `provisa/core/canonical_mail.py` `WRITABLE_COLUMNS`; `provisa/google_workspace/settings.py` `_mail`] [inferred: the Tasks API limit, from REQ-1994]

### Orígenes de API {#api-sources} {: #api-sources }
Registre cualquier endpoint HTTP como una tabla consultable. [tool-verified: `provisa/core/models.py` `SourceType` enum] (REQ-314, REQ-307, REQ-322)

| Tipo de API | Detección | Inferencia de columnas |
| --------- | ----------- | ----------------- |
| `openapi` | Análisis de especificación OpenAPI (REQ-314, REQ-316) | Primitivos → nativo, objetos → JSONB |
| `graphql_remote` | Introspección de esquema (REQ-307, REQ-308) | Primitivos → nativo, objetos → JSONB |
| `grpc_remote` | Reflexión del servidor (REQ-322, REQ-325) | Primitivos → nativo, objetos → JSONB |

Las respuestas de la API se obtienen, se almacenan en caché en PostgreSQL (TTL configurable) y se exponen como tipos GraphQL (REQ-309, REQ-318, REQ-327). Las tablas en caché participan en consultas federadas como cualquier otro origen (REQ-313).

**Reglas de JSONB**: Las columnas complejas (objetos, arreglos) almacenadas como JSONB no son filtrables (REQ-119). El acceso a subcampos usa la extracción `->>` en SQL (REQ-151). Las relaciones se declaran entre tablas usando columnas de clave foránea escalares — las columnas de blob JSONB no son destinos de join. Use la promoción de JSONB para convertir campos anidados en columnas escalares nativas cuando se necesite filtrar o hacer join sobre ellos (REQ-119).

#### GitHub y GitLab {: #github-and-gitlab }

GitHub y GitLab son orígenes de marca que transporta `graphql_remote`: cada uno aparece en el formulario de Orígenes con su propio nombre y se registra como un origen GraphQL remoto que anota su marca. El formulario pide un token de acceso personal y nada más. El endpoint (`https://api.github.com/graphql`, `https://gitlab.com/api/graphql`) y el esquema se distribuyen con Provisa, por lo que el esquema no se introspecciona al registrarlo. El token se comprueba al agregar el origen. Agregar el origen no registra ninguna tabla: cada tabla que ofrece el esquema está disponible bajo el espacio de nombres del origen (`gh`, `gl`), y el steward registra las deseadas. Un campo que el token no puede leer se deja fuera de una tabla cuando esta se registra. [tool-verified: `provisa/graphql_remote/brands.py` `BRANDS`] (REQ-1923)

#### Stripe (REQ-1923) {#stripe} {: #stripe }

Stripe es un origen de marca que transporta `openapi`, construido como GitHub y GitLab sobre `graphql_remote`. Aparece en el formulario de Orígenes como Stripe, bajo Enterprise. Se registra como un origen OpenAPI cuya fila anota la marca en `federation_hints` y cuya ruta de especificación es `brand:stripe`. [tool-verified: `provisa-ui/src/pages/sources/constants.ts` `BRAND_CARRIER`; `provisa/openapi/brands.py` `BRANDS`, `SPEC_SCHEME`; `provisa/api/admin/openapi_router.py` `_branded`]

**A qué llega.** A la API REST de Stripe, mediante la especificación OpenAPI publicada por Stripe (`stripe/openapi`, `spec3.json`). La especificación se incluye con Provisa, fijada y comprimida con gzip, y no se obtiene al registrar. La incluida es la versión de API `2026-09-30.endive` con 425 rutas. Cada llamada lleva `Stripe-Version` con esa versión, de modo que cada respuesta tiene la forma que describe la especificación, sea cual sea la versión por defecto de la cuenta. Los campos que Stripe devuelve y la especificación omite se añaden desde `stripe.additions.json`. [tool-verified: `provisa/openapi/brands.py` `brand_spec`, `Brand.headers`; `provisa/openapi/_known_specs/`; version and path count read with the project interpreter]

Como en cualquier origen OpenAPI, cada operación `GET` es una tabla ofrecida y cada otra operación es un comando ofrecido. Agregar el origen no registra ninguna: el steward registra las que quiere. [tool-verified: `provisa/api/admin/openapi_router.py` `register_openapi_source`; `provisa/openapi/brands.py` module docstring]

**Autenticación.** El formulario pide una sola cosa, un token de acceso (`BRAND_AUTH`: `token`). Use una clave de API secreta o restringida de Stripe. El token se envía como token bearer y se comprueba una vez con `GET /v1/balance` antes de agregar el origen. Un token rechazado hace fallar el registro con `openapi.credential_rejected`; uno ausente, con `openapi.credential_required`. [tool-verified: `provisa-ui/src/pages/sources/constants.ts` `BRAND_AUTH`; `provisa/openapi/brands.py` `Brand.auth`, `verify_path`; `provisa/api/admin/openapi_router.py` `_branded`] [inferred: that a restricted key works, provided it may read `/v1/balance`]

**Actualización y frescura.** Una tabla de Stripe es una réplica, como toda tabla OpenAPI. Sus respuestas en caché duran el `cache_ttl` del origen, que una solicitud de registro fija en 300 segundos salvo que diga otra cosa. Se aplican los tipos de probe de OpenAPI. [tool-verified: `provisa/api/admin/openapi_router.py` `OpenAPIRegisterRequest.cache_ttl`; `provisa/events/probes.py` `_HTTP_API_TYPES`] REQ-1923 prevé que un preset nombre el parámetro que una actualización usa para obtener solo lo que cambió. La marca Stripe no nombra ninguno, así que cada actualización lee sus tablas enteras. [tool-verified: `provisa/openapi/brands.py` `Brand` has no such field]

**Exclusiones.** La integración de pagos obsoleta de tokens y Sources de Stripe queda fuera de la especificación incluida. La lista está en [Exclusiones en un origen construido desde una especificación](#exclusions-on-a-specification-built-source). Las `exclusions` propias de un operador se suman a las de la marca. [tool-verified: `provisa/openapi/_known_specs/stripe.exclusions.json`; `provisa/openapi/brands.py` `brand_spec`] (REQ-1957)

`stripe` aparece también en `SourceType` entre los tipos `wrappers` de Supabase con stub. Un origen Stripe agregado por el formulario es un origen `openapi`, no ese stub. [tool-verified: `provisa/core/models.py` `SourceType`]

#### Exclusiones en un origen construido desde una especificación {#exclusions-on-a-specification-built-source} {: #exclusions-on-a-specification-built-source }

Una especificación OpenAPI publicada describe todo lo que acepta la API remota, incluido lo que su proveedor ha marcado como obsoleto y lo que un despliegue nunca debe enviar. Un origen OpenAPI puede llevar exclusiones: cosas de la especificación que el origen no ofrece. (REQ-1957)

| Excluido | Se nombra con | Efecto |
|---|---|---|
| Una operación | `method` y `path` | No se lista para registrarla como tabla ni como comando |
| Un argumento de solicitud | `name`, en una operación (`method` y `path`) o, sin ninguno de los dos, en cada operación que lo tenga | No figura entre los argumentos de la operación, no se muestra, nunca se envía |

Las exclusiones se definen bajo `exclusions` en el `mapping` del origen:

```yaml
sources:
  - id: shop-api
    type: openapi
    path: https://shop.example.com/openapi.json
    mapping:
      exclusions:
        operations:
          - {method: POST, path: /v1/tokens}
        arguments:
          - {name: source, method: POST, path: /v1/customers}
          - {name: legacy_token}          # on every operation that has it
```

Se aplican a la especificación cuando se carga, de modo que la lista de registro, los comandos generados y la solicitud que se envía ven una especificación en la que las cosas excluidas no existen. Se puede excluir una propiedad del cuerpo y un parámetro de consulta, de cabecera o de cookie; un parámetro de ruta no, porque forma parte de la dirección de la operación.

Una exclusión que nombra una operación o un argumento que la especificación no tiene hace fallar la carga de la especificación de ese origen y nombra cada entrada obsoleta, de modo que se advierte una lista que ya no coincide con una especificación más reciente.

Un origen de marca distribuye sus propias exclusiones como parte de su curaduría, y las de un operador se suman a ellas. El origen de Stripe excluye la integración de pagos obsoleta de Stripe basada en tokens y sources: crear un cargo, las API Tokens y Sources, añadir una tarjeta o cuenta bancaria a un cliente a partir de un token, y los argumentos `source`, `card`, `bank_account` y `default_card` de las escrituras de clientes y pagos de facturas. Los pagos se cobran mediante Payment Intents, Payment Methods y Setup Intents, que el origen ofrece. La lectura y eliminación de las fuentes de pago que una cuenta ya tiene sigue disponible.


### GovData

Datos abiertos del gobierno de EE. UU. El acceso está particionado por agrupación temática. [tool-verified: `provisa/core/models.py` lines 543–609]

A un origen se le dan uno o más temas, y cada tema aporta los esquemas del adaptador que se listan abajo. Un esquema puede pertenecer a más de un tema (`census`). Los esquemas `ref` y `geo` son esquemas de enlace: datos de referencia y geografía a través de los cuales se unen las tablas de todos los temas, por lo que todo origen los sirve sean cuales sean los temas que se le den. La lista se declara una vez en el servidor y una prueba la mantiene igual a los esquemas que sirve el paquete del adaptador fijado. [tool-verified: `provisa/core/models.py` `GOVDATA_SUBJECT_SCHEMAS`, `GOVDATA_LINKER_SCHEMAS`; `tests/unit/test_govdata_subject_schemas.py`]

| Tema | Esquemas expuestos |
| --------- | ----------------- |
| `COMMERCE` | `sec`, `patents`, `cftc` |
| `ECONOMY` | `econ`, `econ_reference`, `ag`, `banking`, `fiscal`, `housing`, `transport` |
| `EDUCATION` | `census`, `edu`, `research` |
| `HEALTH` | `health` |
| `CYBER` | `cyber_threat`, `cyber_vuln` |
| `PUBLIC_SAFETY` | `crime`, `disasters` |
| `ENVIRONMENT` | `lands`, `environment` |
| `WEATHER` | `weather` |
| `ENERGY` | `energy` |
| `GOVERNMENT` | `fedregister`, `fec`, `officials`, `law` |
| `DEMOGRAPHICS` | `census` |
| todo origen | `ref`, `geo` |

`ALL` es la abreviatura de una suscripción para todos los temas; no es una opción del formulario de Orígenes.


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

Un origen AskAmerica creado en la administración recibe una credencial, la clave de API. Provisa presenta la clave a la API de AskAmerica y recibe las credenciales de almacenamiento de corta duración y solo lectura con las que se leen los datos; la clave se rechaza allí por su nombre si es desconocida o está revocada. [tool-verified: `provisa/federation/askamerica.py`]

El origen es entonces un origen de protocolo Postgres como los de SharePoint, Salesforce e inventario de nube: Provisa inicia para él el servidor `pgwire-govdata` incluido, con la clave y esas credenciales en el entorno del servidor, y cada motor llega a ese servidor como llega al de un hermano. DuckDB, Postgres y ClickHouse lo adjuntan y leen sus tablas en su lugar, de modo que una tabla de AskAmerica se une con tablas de otros orígenes, queda bajo vistas y se entrega como cualquier otra. Trino no tiene conector para él y lee una réplica creada a través del servidor: una copia de la tabla guardada en el almacén de materialización, tan fresca como su última actualización bajo los ajustes de caché y replicación de la tabla, en lugar de leerse en el momento de la consulta. Una tabla se lee del esquema del adaptador que nombra (`sec`, `econ`, etc.). [tool-verified: `provisa/federation/pgwire_replica.py`, `provisa/federation/connector_duckdb.py`, `provisa/federation/clickhouse_connectors.py`]

El servidor monta sus esquemas desde el almacenamiento de objetos antes de aceptar una conexión, lo que tarda minutos en un primer arranque. Se inicia cuando el origen se guarda; hasta que está listo, el formulario de Register Table informa de que el origen aún se está iniciando. [tool-verified: `provisa/federation/pgwire_replica.py` `start_when_saved`]

### Dónde vive la contraseña de un origen {: #where-a-sources-password-lives }

La contraseña de un origen nunca se almacena junto al resto de sus ajustes de conexión. La fila `sources` del plano de control lleva una columna `password_ref` que contiene lo que se introdujo para la contraseña: el valor mismo, o una referencia como `${env:PG_PASSWORD}` o `${secret:SNOWFLAKE_KEY}`. Se resuelve en el momento en que se marca el origen, dentro de la organización bajo la que se ejecuta la solicitud. [tool-verified: `provisa/core/schema_org.py`, `provisa/core/repositories/source.py`]

Un valor sin `${…}` se usa tal cual. `${env:VAR}` lee el entorno de proceso del despliegue y no necesita nada vinculado. `${secret:NAME}` nombra un secreto que posee una organización, de modo que solo se resuelve dentro de las propias operaciones de esa organización: las costuras de introspección de administración y la terminal de consulta a las que llega cada superficie establecen ese vínculo. [tool-verified: `provisa/core/secrets.py` `resolve_secrets`; `provisa/pgwire/_pipeline.py` `_execute_plan`]

El campo guarda lo que usted escribe en él, sea cual sea la forma en que se registró el origen:

- **Desde la configuración.** El archivo es el registro: Provisa copia el valor o la referencia en `password_ref` de forma literal.
- **Desde el formulario de Orígenes.** Lo que se escribe en el campo de contraseña se almacena de forma literal, sea un valor o una referencia. Provisa no escribe nada en la bóveda de la organización por ello ni inventa ningún nombre de secreto. Para mantener una credencial fuera de la fila del plano de control, cree usted mismo el secreto (véase [Secretos](secrets.md)) y escriba `${secret:NAME}` en el campo. [tool-verified: `provisa/api/admin/schema_mutation.py` `create_source`]

Un valor escrito como valor se almacena en la base de datos del plano de control tal como se escribió. Volver a escribir la contraseña reemplaza el texto del campo. Eliminar el origen quita su fila y nada de la bóveda: un secreto que usted creó es suyo. [tool-verified: `provisa/api/admin/schema_mutation.py` `delete_source`; `tests/unit/test_no_product_written_secrets.py`]


`password_ref` no viaja entre entornos (REQ-1491). Una rama o un entorno copiado suministra sus propios valores de conexión, y la bóveda que nombra una referencia pertenece a cualquiera que sea el entorno que la suministró. [tool-verified: `provisa/core/env_classes.py` `BINDING_COLUMNS`]

### Perfilador de datos {#data-profiler} {: #data-profiler }

Un perfilador de datos es un origen que guarda un nombre, un horario y valores predeterminados de ejecución, y nada más. Nunca se introspecciona. Se le agregan tablas de otros orígenes, y cada ejecución las mide y escribe sus filas en el plano de control de la organización, donde se leen como las tablas de cualquier otro origen. Para crear un perfilador y agregarle tablas, véase [Datos de prueba](test-data.md). [tool-verified: `provisa/core/models.py` `SourceType.data_profiler`; `provisa/federation/strategy.py` `_MATERIALIZE_ONLY`] (REQ-1934)


### Verificadores de calidad de datos (REQ-1443) {#data-quality-checkers-req-1443} {: #data-quality-checkers-req-1443 }
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

## Conectores personalizados (REQ-1177) {: #custom-connectors-req-1177 }
Los motores de federación nativos — Postgres, DuckDB y ClickHouse — obtienen accesibilidad a un nuevo tipo de origen cuando un operador declara un conector para él en `config/custom_connectors.yaml`. No se requiere código. [tool-verified: `provisa/federation/custom_connectors.py` `load_custom_connectors`; `provisa/federation/engine.py` `build_pg_engine`, `build_duckdb_engine`, `build_clickhouse_engine`]

La extensibilidad de conectores en sí misma es anterior a esto. El motor Trino lleva mucho tiempo siendo extensible en su propia capa — un conector JDBC genérico parametrizado por tipo de origen, un cuerpo `.properties` de catálogo por tipo, y los propios plugins de conector Trino personalizados de Provisa (Splunk, SharePoint, Calcite). [tool-verified: `provisa/federation/trino_connectors.py` `_TrinoJdbcConnector`, `_TRINO_JDBC_TYPES`; `trino/plugins/trino-splunk`, `trino/plugins/trino-sharepoint`, `trino/plugins/trino-calcite`] REQ-1177 lleva esa misma extensibilidad basada en configuración a los dos motores nativos sin clúster, que antes tenían un conjunto de conectores fijo.

La configuración se distribuye vacía. Los conectores integrados cubren el alcance listo para usar; todo lo que hay en este archivo lo escribe el operador. [tool-verified: `config/custom_connectors.yaml` line 52: `connectors: []`] Defina `PROVISA_CUSTOM_CONNECTORS` para apuntar a una ruta distinta (útil para pruebas).

### Tipos de descriptor {: #descriptor-kinds }
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

### Verificación de disponibilidad {: #probe-gating }
La disponibilidad se verifica en el momento del attach contra el catálogo de detección estándar de cada motor:

- **Postgres** — verifica `pg_extension`, luego `pg_available_extensions`. [tool-verified: `provisa/federation/connector_duckdb.py` `_probe_pg_extension` lines 333–344]
- **DuckDB** — ejecuta `INSTALL`/`LOAD` y verifica `duckdb_functions()` para el `probe_symbol` declarado. [tool-verified: `provisa/federation/connector_duckdb.py` `_DuckDBExtensionConnector.probe` lines 160–180]
- **ClickHouse** — verifica `system.table_engines` para el `ch_engine` declarado; su ausencia en el build falla de forma ruidosa. [tool-verified: `provisa/federation/custom_connectors.py` `_probe_clickhouse_engine`]

Una extensión declarada que no se puede instalar falla de forma ruidosa. Sin omisión silenciosa, sin valor de respaldo. Un conector cuya verificación falla simplemente no está activo para ese despliegue.

### Variables de plantilla {: #template-variables }
Todo valor de `server_options`, valor de `user_mapping`, `attach_template` y `scan_template` puede usar marcadores `{field}`. Campos disponibles: [tool-verified: `provisa/federation/custom_connectors.py` `_source_fields` lines 53–63]

`{id}`, `{host}`, `{port}`, `{database}`, `{username}`, `{password}`, `{path}`, `{schema_name}`, `{table_name}`, además de cualquier clave de `federation_hints`. Las plantillas de attach de DuckDB también reciben `{alias}` — el alias de catálogo interno que Provisa asigna a la base de datos adjunta.

Una plantilla que referencia un campo desconocido falla de forma ruidosa en el momento del attach, exponiendo un desajuste entre descriptor y origen antes de que un DDL roto llegue al motor.

### Ejemplos {: #examples }
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

## Almacenes de datos como orígenes con nombre {: #warehouses-as-named-sources }
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

## Campos de configuración de origen {: #source-configuration-fields }
Todos los orígenes comparten un conjunto común de campos. [tool-verified: `provisa/core/models.py` `Source` class, lines 138–204]

| Campo | Obligatorio | Predeterminado | Descripción |
| ------- | ---------- | --------- | ------------- |
| `id` | Sí | — | Identificador único; alfanumérico con guiones/guiones bajos |
| `type` | Sí | — | Tipo de origen (ver tablas anteriores) |
| `host` | No | `""` | Nombre de host o IP |
| `port` | No | `0` | Número de puerto |
| `database` | No | `""` | Nombre de la base de datos |
| `username` | No | `""` | Nombre de usuario |
| `password` | No | `""` | Contraseña; use `${env:VAR}` o `${secret:NAME}` en lugar de un valor literal (véase más abajo) |
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
| `mapping` | No | `{}` | Configuración de conector específica del tipo para orígenes NoSQL y SaaS (por ejemplo, `auth_type` de SharePoint, `auth_type` de Salesforce, `use_token` de Splunk) (REQ-251) |
| `allowed_domains` | No | `[]` | Restringe el origen a dominios específicos; vacío = sin restricción |
| `description` | No | `""` | Descripción legible por humanos |

---

## Orígenes Kafka {: #kafka-sources }
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

## Visibilidad de columnas {: #column-visibility }
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

## Relaciones {: #relationships }
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
| `materialize` | No | Crea automáticamente una vista materializada para joins entre orígenes (REQ-158). En una arista respaldada por una tabla de unión, la vista cubre el recorrido de dos saltos, no un join directo (REQ-1586) |
| `refresh_interval` | No | Intervalo de actualización de la vista materializada en segundos (predeterminado: 300) |
| `target_function_name` | No | Nombre de función de base de datos para relaciones calculadas |
| `function_arg` | No | Qué argumento de la función recibe el valor de la columna de origen |
| `alias` | No | Tipo de relación legible por humanos (por ejemplo, `WORKS_FOR`) |
| `graphql_alias` | No | Nombra el campo SDL que esta relación expone en el tipo padre. Cuando está ausente, el nombre se deriva del `field_name` de la tabla destino y la cardinalidad de la relación. [tool-verified: `provisa/compiler/schema_gen.py:1050`] |
| `disable_cypher` | No | Cuando es `true`, excluye esta relación de las aristas del grafo Cypher |
| `source_json_key` | No | Extrae esta clave de la columna de origen como un objeto JSON antes del JOIN |
| `via_table` | No | Nombre de tabla registrada de la tabla de unión que recorre esta arista. Fijarlo hace que la arista sea respaldada por una tabla de unión; dejarlo vacío la deja como arista de clave foránea (REQ-1586) |
| `via_source_column` | No | Columna de la tabla de unión que se empareja con `source_column`. Separada por comas y posicional para una clave compuesta |
| `via_target_column` | No | Columna de la tabla de unión que se empareja con `target_column` |
| `via_type_column` | No | Columna discriminadora, cuando una misma tabla de unión lleva varios tipos de relación |
| `via_type_value` | No | El valor de discriminador al que queda fijada esta arista |
| `via_label_source` | No | Qué designación nombra el tipo Cypher: `column` (el valor del discriminador), `table` (el nombre de la tabla de unión) o `fixed` (el alias declarado). Todas se pasan a UPPER_SNAKE_CASE |

### Relaciones respaldadas por una tabla de unión {: #junction-backed-relationships }

Una tabla asociativa puede declararse como relación Cypher de primera clase en lugar de como nodo, de modo que sus propias columnas pasan a ser los atributos de esa relación: (REQ-1586)

```yaml
relationships:

  - id: pets-bonded-pair
    source_table_id: pets
    target_table_id: pets
    source_column: id
    target_column: id
    cardinality: one-to-many
    via_table: pet_companions
    via_source_column: pet_id
    via_target_column: companion_pet_id
    via_type_column: relation_type
    via_type_value: bonded pair
    via_label_source: column
```

La tabla de unión es una tabla registrada como cualquier otra y debe estar registrada antes de que una relación pueda nombrarla. Declárela una vez por valor de discriminador: tres filas sobre `pet_companions` producen `BONDED_PAIR`, `LITTERMATE` y `SHARES_ENCLOSURE` como tres tipos Cypher distintos, cada uno llevando las columnas restantes de la fila de unión como propiedades de la arista. La configuración de demo incluida declara exactamente esto.

Una arista de unión es una relación Cypher, no un campo de join de GraphQL: el emisor de joins de GraphQL construye su cláusula `ON` para un único par de columnas y no tiene sitio para el segundo salto, así que las aristas de unión quedan excluidas del SDL generado y de `pg_constraint`. [tool-verified: `provisa/compiler/schema_gen.py:304`] La tabla de unión sigue siendo consultable como su propio campo raíz, y desaparece del lado de los nodos del esquema del grafo Cypher, por lo que nunca aparece como etiqueta de nodo.

`materialize: true` funciona en una arista de unión, y lo que materializa es el recorrido en vez de un join directo `pets`-a-`pets`: la vista guarda el salto de origen, el salto de la tabla de unión, el discriminador y las columnas propias de la tabla de unión junto a las del destino. Como la tabla de unión es una tercera pata del join, que la arista cruce orígenes se juzga sobre las tres tablas — una tabla de unión en un origen distinto al de las dos que enlaza se materializa aunque esas dos coincidan. Una declaración materializa un tipo de arista, así que una vista construida para `bonded pair` nunca responde a un recorrido `littermate`.

Valores de cardinalidad [tool-verified: `provisa/core/models.py` `Cardinality` enum, lines 79–81]:

- `many-to-one` — cada fila de origen se mapea a una fila de destino (FK a PK)
- `one-to-many` — cada fila de origen se mapea a varias filas de destino (inverso de la anterior)

---

## Reglas de seguridad de nivel de fila {: #row-level-security-rules }
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

## Funciones y webhooks {: #functions-and-webhooks }
### Funciones de base de datos {: #db-functions }
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

## Autenticación {: #authentication }
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

## Enrutamiento de ejecución {: #execution-routing }
**Ejecución directa** — Las consultas RDBMS de un solo origen se enrutan al controlador nativo para lograr una latencia menor a 100 ms (REQ-027). Los orígenes requieren tanto una entrada en `SOURCE_TO_DIALECT` como una en `SOURCE_TO_CONNECTOR` para soportar esta ruta (REQ-229).

**Ejecución federada** — Las consultas de varios orígenes y los orígenes sin controlador directo se enrutan a través del motor de federación (REQ-028). Provisa incluye un motor de federación embebido; apunte a su propio clúster compatible para despliegues a gran escala (REQ-226).

**Estadísticas** — Al registrar, Provisa ejecuta `ANALYZE` contra cada tabla publicada para preparar el optimizador basado en costos (conteo de filas, fracción de nulos, valores distintos, mínimo/máximo). Los fallos se registran en el log y no bloquean el registro (REQ-275).

---

## Orígenes de grafo y semántica {: #graph-semantic-sources }
### Neo4j

Registre una base de datos de grafos Neo4j como un origen consultable. Los stewards escriben consultas Cypher que proyectan valores escalares; Provisa almacena en caché los resultados y los expone como tipos GraphQL (REQ-295).

Las consultas Cypher deben usar accesores de propiedad en la cláusula `RETURN` (`RETURN n.id AS id, n.name AS name`) — devolver objetos de nodo se rechaza en el momento del registro (REQ-296).

#### Registro por archivo de configuración (REQ-1668) {: #config-file-registration-req-1668 }

Declare un origen `neo4j` y sus tablas en YAML. Cada tabla requiere un `query_template` (el Cypher que produce sus filas) y columnas tipadas. La clave `query_template` no es válida bajo ningún otro tipo de origen. [tool-verified: `provisa/core/config_loader.py:456-511`]

Un origen requiere `host`, `port` y `database`. [tool-verified: `provisa/core/config_loader.py:456-468`] Las filas se obtienen enviando un POST con `{"statements": [{"statement": <cypher>}]}` a `/db/<database>/tx/commit` (la API de transacciones HTTP de Neo4j). Una respuesta con una lista `errors` no vacía se trata como una consulta fallida, no como un resultado vacío. [tool-verified: `provisa/neo4j/source.py:71-82`, `provisa/api_source/caller.py:344-347`, `provisa/api_source/normalizers.py:49-74`]

Cada columna requiere `data_type`. El cargador convierte los tipos de configuración al tipo de columna de la API usado en el momento de la consulta [tool-verified: `provisa/neo4j/persist.py:31-64`]:

| `data_type` de configuración | Tipo de la API |
|---|---|
| `varchar`, `text`, `string`, `char` | string |
| `integer`, `int`, `bigint`, `smallint` | integer |
| `float`, `double`, `real`, `decimal`, `numeric`, `number` | number |
| `boolean`, `bool` | boolean |
| `json`, `jsonb` | jsonb |

Se aceptan `varchar(N)` y `decimal(10,2)` — se usa el tipo base anterior al paréntesis.

El registro persiste una fila `api_sources` y una fila `api_endpoints` por tabla, de modo que las tablas sobreviven a un reinicio sin volver a leer el archivo. Los endpoints REST de administración bajo `/admin/sources/neo4j` escriben las mismas filas. [tool-verified: `provisa/neo4j/persist.py:77-120`]

```yaml
sources:
  - id: graph
    type: neo4j
    host: neo4j
    port: 7474
    database: neo4j
    cache_ttl: 300

tables:
  - source_id: graph
    schema: neo4j
    table: person_skills
    query_template: >-
      MATCH (p:Person)-[:HAS_SKILL]->(s:Skill)
      RETURN p.name AS name, s.skill AS skill, p.experience AS years
    columns:
      - name: name
        data_type: varchar
      - name: skill
        data_type: varchar
      - name: years
        data_type: integer
```

#### Register Table en la UI (REQ-1670) {: #register-table-in-the-ui-req-1670 }

Un origen neo4j no tiene tablas que listar, así que el formulario Register Table pide la tabla en lugar de ofrecer una. [tool-verified: `provisa-ui/src/pages/tables/RegisterTableForm.tsx` (`isNeo4j`)]

1. Elija el origen neo4j y un dominio. Los selectores de esquema y tabla, la casilla de descubrimiento y el selector de marca de agua no aparecen; el origen nunca se introspecciona.
2. Escriba un nombre de tabla y el Cypher. El Cypher debe proyectar escalares (`RETURN a.name AS name`); una proyección que devuelva un nodo o una lista se reporta como error.
3. Pulse Preview. El formulario ejecuta el Cypher con `LIMIT 5` a través de la consulta GraphQL `neo4jPreview` y completa la lista de columnas a partir de las filas que regresan, tipadas como `text`, `integer`, `double`, `boolean` o `json`. [tool-verified: `provisa/api/admin/_neo4j_registration.py` `preview_neo4j`] Una vista previa fallida deja el Cypher en el editor y muestra el mensaje.
4. Ajuste visibilidad, alias o enmascaramiento como para cualquier tabla y luego registre. El formulario se niega a enviar hasta que una vista previa haya tipado las columnas, y el servidor rechaza una tabla neo4j que no lleve Cypher (`schema.neo4j_query_required`). [tool-verified: `provisa/api/admin/schema_mutation_ops.py` `persist_neo4j_registration`]

El Cypher se almacena con la tabla como `queryTemplate`, se muestra en la vista de lectura de la tabla, y persiste exactamente igual que un registro por archivo de configuración: una fila `api_sources` y una fila `api_endpoints` que el siguiente inicio hidrata. Editar la tabla vuelve a persistir un Cypher editado.

#### Registro por REST de administración {: #admin-rest-registration }

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

#### Registro por archivo de configuración y por UI (REQ-1683) {: #config-file-and-ui-registration-req-1683 }

El `host` de un origen `sparql` es la URL de su endpoint SPARQL (el formulario de Orígenes lo almacena de la misma manera). Cada tabla bajo él lleva `query_template`, un SELECT cuyas variables son las columnas; cada binding es `text`. [tool-verified: `provisa/core/config_loader.py` `_validate_neo4j_sources`, `_handle_sparql_table`]

```yaml
sources:
- id: sparql-demo
  type: sparql
  host: http://localhost:23030/provisa/query
tables:
- source_id: sparql-demo
  domain_id: shelter
  schema: sparql
  table: volunteer
  query_template: >-
    PREFIX s: <http://provisa.dev/shelter#>
    SELECT ?volunteer_id ?name WHERE { ?v a s:Volunteer ; s:id ?volunteer_id ; s:name ?name }
  columns:
  - { name: volunteer_id, data_type: text, visible_to: [org_admin] }
  - { name: name, data_type: text, visible_to: [org_admin] }
```

Register Table funciona de la misma manera que para Neo4j: elija el origen, escriba un nombre de tabla y el SELECT, pulse Preview (la consulta `sparqlPreview` lo ejecuta con `LIMIT 5` y completa la lista de columnas), luego registre. El registro persiste una fila `api_sources` y una fila `api_endpoints` (POST codificado como formulario a la ruta del endpoint, normalizador `sparql_bindings`), las mismas filas que escribe un registro por archivo de configuración, y el motor nativo aterriza las filas a través de la misma cadena de obtención que Neo4j. [tool-verified: `provisa/api/admin/_query_api_registration.py`, `provisa/sparql/persist.py`]

---

## Ejemplos de conexión {: #connection-examples }
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

### Consulta entre orígenes {: #cross-source-query }
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
