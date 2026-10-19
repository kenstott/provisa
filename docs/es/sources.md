# Tipos de origen {: #source-types }
## Modelo de ejecución {: #execution-model }

Toda consulta acaba ejecutándose a través del motor de federación, que es lo que permite unir cualquier origen con cualquier otro. Cómo llega un origen hasta ahí depende de cómo lo alcanza Provisa, y eso coloca cada origen en uno de cinco grupos.

| Categoría | Tiene controlador directo | Tiene conector federado | Ejemplos |
| --- | --- | --- | --- |
| **Con capacidad directa** | Sí | Sí | PostgreSQL, MySQL, MariaDB, SingleStore, SQL Server, Oracle, DuckDB |
| **Solo federación** | No | Sí | Redshift, Druid, Exasol, Hive, Iceberg, Delta Lake, Hive (respaldado por S3) |
| **Lectura directa (réplica)** | Sí | Sí | Snowflake, Databricks, ClickHouse — el controlador lee los datos y crea una réplica; las consultas se ejecutan contra la réplica en el motor activo |
| **Directo (sin conector entre orígenes)** | Sí | No | Athena — las instrucciones de un solo origen se ejecutan íntegras en Athena; las uniones entre orígenes crean una réplica completa |
| **Materializar → Federación** | No | No | REST/OpenAPI, GraphQL remoto, gRPC, Neo4j Cypher, SPARQL, WebSocket, RSS, CSV, SQLite, Parquet, Ingest (receptor push), GovData, SharePoint, Salesforce, ServiceNow, inventario de nube, Splunk, Qdrant, Azure Cosmos DB, Logs, Google Workspace, Microsoft 365 |

Los orígenes **con capacidad directa** responden una consulta que solo los toca a ellos mediante su propio controlador, en menos de 100 ms y sin pasar por el motor de federación. Conservan su conector completo, así que se siguen uniendo con otros orígenes a través del motor.

Los orígenes **solo federación** no tienen controlador directo. Toda consulta les llega a través de la capa de federación.

Los orígenes de **lectura directa (réplica)** tienen un DirectDriver que lee del almacén de datos de forma nativa (nativo en Arrow cuando se puede) y crea una réplica en el almacén de materialización del motor activo. Las consultas se ejecutan luego contra esa réplica. Consulte [Almacenes de datos como orígenes con nombre](#warehouses-as-named-sources).

Los orígenes **directos (sin conector entre orígenes)** tienen un DirectDriver que ejecuta una instrucción de un solo origen íntegra en el origen, con cada filtro, columna y límite trasladado. No hay conector de Trino para ellos, así que cuando una instrucción une uno de ellos con otro origen, el controlador lee los datos y crea una réplica, y la unión se ejecuta contra esa réplica en el motor activo.

Los orígenes de **materialización** tampoco tienen conector federado. Provisa obtiene sus datos, al inicio o en el momento de la consulta, y los guarda en caché como Parquet en S3 o en PostgreSQL. Desde ahí el motor de federación los alcanza para consultas entre orígenes.

---

## Todos los orígenes {: #all-sources }
Provisa registra **73** tipos de origen, los que ofrece el formulario de Orígenes. Las tablas siguientes cubren los 73; el índice es el recuento.

| # | Grupo | Tipos de origen |
| --- | --- | --- |
| 1–15 | [RDBMS](#rdbms) | `postgresql`, `mysql`, `mariadb`, `singlestore`, `sqlserver`, `oracle`, `duckdb`, `cockroachdb`, `yugabytedb`, `greenplum`, `tidb`, `firebird`, `airport`, `saphana`, `hiveserver2` |
| 16–24 | [Almacenes de datos en la nube](#cloud-data-warehouses) | `snowflake`, `bigquery`, `databricks`, `redshift`, `athena`, `fabric`, `synapse`, `trino`, [`motherduck`](#motherduck) |
| 25–29 | [Analítica / OLAP](#analytics-olap) | `clickhouse`, `druid`, `exasol`, `elasticsearch`, `pinot` |
| 30–33 | [Data lake / Formatos de tabla abiertos](#data-lake-open-table-formats) | `iceberg`, `delta_lake`, `hive`, `hive_s3` |
| 34–38 | [NoSQL](#nosql) | `mongodb`, `cosmosdb`, `qdrant`, `cassandra`, `redis` |
| 39–40 | [Fuentes de código](code-sources.md) | `node`, `python` |
| 41–43 | [Streaming](#streaming) | `kafka`, `websocket`, `rss` |
| 44 | [Receptor push](#push-receiver) | `ingest` |
| 45–46 | [Grafo y semántica](#graph-semantic) | `neo4j`, `sparql` |
| 47–51 | [Basados en archivos](#file-based) | `sqlite`, `csv`, `parquet`, `files`, `logs` |
| 52–53 | [Observabilidad y otros](#observability-other) | `google_sheets`, `prometheus` |
| 54–61 | [SaaS empresarial](#enterprise-saas-connectors) | `sharepoint`, `salesforce`, `servicenow`, `jira`, `cloudops`, `splunk`, `google_workspace`, `microsoft_365` |
| 62–67 | [Orígenes de API](#api-sources) | `openapi`, `graphql_remote`, `grpc_remote`, GitHub, GitLab, Stripe |
| 68 | [GovData](#govdata) | `govdata` |
| 69–70 | [Verificadores de calidad de datos](#data-quality-checkers) | `soda`, `great_expectations` |
| 71 | [Perfilador de datos](#data-profiler) | `data_profiler` |
| 72 | [Conjuntos de datos de Kaggle](#kaggle-datasets) | Kaggle (preparado mediante el formulario de Orígenes; se registra como un origen `files` — véase [Conjuntos de datos de Kaggle](#kaggle-datasets)) |
| 73 | [Wikipedia](#wikipedia) | Wikipedia (se registra como un origen `files` que rastrea las páginas indicadas — véase [Wikipedia](#wikipedia)) |

Referencia de todos los tipos de origen que soporta Provisa. "Controlador directo" significa que las consultas de un solo origen se ejecutan de forma nativa contra el origen (menos de 100 ms). "Nombre del conector" es el conector federado que se usa cuando el origen participa en JOIN entre varios orígenes.

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

Las bases de datos compatibles a nivel de protocolo reutilizan el driver JDBC, el driver asíncrono nativo y el dialecto de un protocolo base. CockroachDB, YugabyteDB y Greenplum usan el protocolo de PostgreSQL, y TiDB el de MySQL. Solo necesitan entradas en el registro y ningún código de conector nuevo.

`firebird` (Firebird 3/4/5) y `airport` (servidor Arrow Flight) son tipos de origen registrados a los que las extensiones de la comunidad de DuckDB llegan en su sitio cuando DuckDB es el motor activo. No tienen driver directo ni conector federado.
#### `saphana`

SAP HANA se lee con su propio controlador y se replica en el almacén del motor activo, de modo que es accesible en todos los motores. El formulario pide host, puerto (predeterminado 39015), usuario, contraseña y base de datos; en un sistema multi-tenant la base de datos nombra el tenant. Register Table lista los esquemas que el usuario puede leer, sin los esquemas de sistema de HANA.

#### `hiveserver2`

Un endpoint HiveServer2 leído directamente sobre Thrift, para un despliegue de Hive donde el endpoint es lo único accesible. Es distinto de `hive` y `hive_s3`, que leen los archivos y el metastore del almacén y nunca pasan por HiveServer2. Las filas se leen a través del endpoint y se replican en el almacén del motor activo. El formulario pide host, puerto (predeterminado 10000), usuario, contraseña y base de datos; `federation_hints.auth_mechanism` selecciona la autenticación (predeterminada `PLAIN`, por ejemplo `GSSAPI` para Kerberos).

### Almacenes de datos en la nube {: #cloud-data-warehouses }

| Tipo de origen | Controlador directo | Nombre del conector | Dialecto | Mutaciones | Notas |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `snowflake` | SnowflakeDriver | snowflake | snowflake | Federada | Lee mediante snowflake-connector-python; crea réplica; `account`/`warehouse`/`role` en `federation_hints` |
| `bigquery` | — | bigquery | bigquery | Federada | Sin DirectDriver; se accede mediante el motor de federación o BigQuery engine ATTACH |
| `databricks` | DatabricksDriver | delta_lake | databricks | Federada | Lee mediante databricks-sql-connector (Cloud Fetch, Arrow); crea réplica; `http_path` obligatorio en `federation_hints` |
| `redshift` | — | redshift | redshift | Federada | — |
| `athena` | AthenaDriver | — | athena | Parcial (INSERT siempre; UPDATE/DELETE/MERGE solo en Iceberg) | Consulta serverless sobre S3; las instrucciones de una sola fuente se ejecutan íntegramente en Athena; las consultas de fuentes cruzadas aterrizan una réplica; `region` requerida en `federation_hints` |
| `fabric` | MssqlWarehouseDriver | — | tsql | Federada | Microsoft Fabric Warehouse; T-SQL sobre TDS, autenticación Azure AD; crea réplica |
| `synapse` | MssqlWarehouseDriver | — | tsql | Federada | Azure Synapse SQL; T-SQL sobre TDS, autenticación Azure AD; crea réplica |
| `trino` | SQLAlchemyDriver | — | — | Federada | Coordinador remoto Trino/Presto leído mediante el dialecto trino de SQLAlchemy; crea réplica en cualquier motor |

#### `athena`

Amazon Athena es un servicio de consultas sin servidor sobre datos en S3, que se factura por byte escaneado. Una instrucción solo sobre tablas de Athena va directa a Athena, con filtros, listas de columnas y `LIMIT` trasladados, así que Athena escanea y factura solo los bytes que esa instrucción necesita. Una instrucción que une una tabla de Athena con cualquier otro origen primero crea una réplica completa desde Athena. Eso lee la tabla entera, y Athena lo factura; después el join se ejecuta contra la réplica. Los ajustes Replicate y de protección de carga del origen deciden cada cuánto se crea, y esta nota es el único aviso sobre ese coste.

**Descubrimiento.** Register Table llama a la API de metadatos de Athena: `ListDatabases`, `ListTableMetadata` y `GetTableMetadata`. Estas llamadas no ejecutan ninguna consulta, así que listar bases de datos y tablas y leer las columnas de una tabla no cuesta nada por byte. Las columnas de una tabla se obtienen solo cuando usted selecciona esa tabla, con las claves de partición después de las columnas normales. `array<…>`, `map<…>` y `struct<…>` pasan a `text`, y un tipo desconocido se rechaza por su nombre.

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

Ambos `username` y `password` vacíos significa que se usa la cadena de credenciales de boto3: rol de instancia, variables de entorno (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`) o un perfil con nombre. Uno sin el otro es rechazado.

**Escrituras.** Toda tabla registrada acepta `INSERT`. `UPDATE`, `DELETE` y `MERGE` solo funcionan en una tabla Iceberg, y el formato se registra al dar de alta la tabla (`registered_tables.table_format`). En una tabla Hive, una escritura que pide `UPDATE`, `DELETE` o `MERGE` se rechaza indicando la tabla y la operación. Athena no devuelve filas escritas, así que una cláusula `RETURNING` se rechaza y GraphQL no ofrece campos de mutación para estas tablas. Las vías de escritura son `INSERT`, `UPDATE`, `DELETE` y `COPY`. `INSERT … ON CONFLICT`, `UPDATE … FROM`, `DELETE … USING` y `TRUNCATE` se rechazan indicando la forma.

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

`motherduck` es una base de datos DuckDB alojada en MotherDuck. En el motor DuckDB se adjunta en su lugar; en el motor Postgres sigue siendo el conector del foreign data wrapper `pg_duckdb`.

**Conexión.** `database` nombra la base de datos de MotherDuck, o un recurso compartido por su dirección de recurso compartido. `password` es el token de MotherDuck, que se resuelve como cualquier secreto de origen.

```yaml
- id: reference
  type: motherduck
  database: analytics            # attached as md:analytics
  password: ${secret:<name>}     # the MotherDuck token, or a reference to it
```

El motor DuckDB emite `ATTACH 'md:<database>'`; para un recurso compartido, `database` es la dirección del recurso compartido (`_share/...`), así que el adjunto es `md:_share/...`.

- **Manejo del token.** El motor inicia sesión con `SET GLOBAL motherduck_token = ?`, con el token pasado como valor enlazado. No aparece en el texto de ninguna instrucción, por lo que ningún log, traza o error lo contiene.
- **Sin token.** Un origen sin `password` se rechaza por nombre antes de adjuntar nada (`a MotherDuck source signs in with its token (the password field); none is set`). Sin esta comprobación, la extensión iniciaría un inicio de sesión interactivo y esperaría alrededor de un minuto. Un origen sin `database` también se rechaza.
- **Una cuenta por motor.** Un motor inicia sesión en una cuenta de MotherDuck. Un segundo origen MotherDuck con un token distinto se rechaza por nombre: "one DuckDB engine reads one MotherDuck account".
- **Las escrituras** pasan por el motor, como en el motor Postgres.

**La ejecución híbrida (dual)** no necesita ninguna configuración. En el motor DuckDB todos los orígenes se adjuntan en una sola conexión, así que una instrucción que une una tabla de un origen de archivo DuckDB con una tabla de MotherDuck se ejecuta como una sola instrucción DuckDB, y MotherDuck decide dónde se ejecuta cada parte.

> **Movimiento de datos.** Con la ejecución dual, MotherDuck puede mover filas locales a su lado para las partes que ejecuta. Por eso, una instrucción así puede enviar datos del archivo local a la cuenta de MotherDuck de la organización. No una un archivo con datos que deben permanecer en el nodo con un origen MotherDuck salvo que enviarlos a esa cuenta sea aceptable.

Un origen MotherDuck solo lo sirven los motores DuckDB y Postgres. Ningún otro motor tiene un conector para él, y nada aterriza allí sus tablas como réplicas.

### Analítica / OLAP {: #analytics-olap }

| Tipo de origen | Controlador directo | Nombre del conector | Dialecto | Mutaciones | Notas |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `clickhouse` | ClickHouseDriver | clickhouse | clickhouse | Federada | Lee mediante clickhouse-connect (HTTP); `secure: "true"` en `federation_hints` para TLS |
| `druid` | — | druid | druid | No | — |
| `exasol` | — | exasol | exasol | No | — |
| `elasticsearch` | HTTP (motores nativos) | elasticsearch (Trino) | — | No | En Trino el conector lo lee, con sus propiedades provenientes del DSL de mapeo del tipo; en cualquier otro motor Provisa lee el índice mediante HTTP (índices y mapeo para Register Table, una lectura por scroll para el aterrizaje) y aterriza las filas |
| `pinot` | — | pinot | — | No | Conector `pinot` de Trino; `pinot.controller-urls` = host:port del controlador de Pinot |

### Data lake / Formatos de tabla abiertos {: #data-lake-open-table-formats }
Estos tipos de origen son solo de federación — sin controlador directo, sin dialecto.

| Tipo de origen | Nombre del conector | Viaje en el tiempo | Notas |
| ------------ | ----------------- | ------------- | ------- |
| `iceberg` | iceberg | Sí (argumento `as_of`) | — |
| `delta_lake` | delta_lake | Sí (argumento `as_of`) | — |
| `hive` | hive | No | — |
| `hive_s3` | hive | No | Hive respaldado por S3 |

### NoSQL

`mongodb`, `cassandra` y `redis` tienen conectores de Trino (`redis` construye sus propiedades a partir del DSL de mapeo del tipo).

| Tipo de origen | Nombre del conector | Mutaciones |
| ------------ | ----------------- | ----------- |
| `mongodb` | mongodb | No |
| `cassandra` | cassandra (Trino); lectura CQL mediante cassandra-driver en cualquier otro motor | No | Los keyspaces son esquemas; Register Table lista las tablas de un keyspace y tipa las columnas a partir de los metadatos de esquema del clúster (las claves de partición como claves primarias); el extra `cassandra` instala el controlador |
| `redis` | redis (Trino); lectura sin HTTP mediante redis-py en cualquier otro motor | No | Un prefijo de clave `<table>:*` es una tabla y un hash es una fila; Register Table lista los prefijos presentes y tipa las columnas de un prefijo a partir de sus hashes (una entrada de `mapping.tables` anula el patrón, la columna de clave, el tipo de valor y las columnas) |

**Redis en los motores alojados.** Redis no está disponible cuando el motor de la organización es uno de los motores aprovisionados de la plataforma alojada. Trino lee las tablas de un catálogo Redis de archivos de descripción en un directorio propio del catálogo, y a un motor alojado no se le puede dar uno mientras se ejecuta. Guardar o aplicar allí una fuente Redis se rechaza por nombre (HTTP 422, `sources.redis_needs_engine_files`); un despliegue autoalojado lee Redis como se describe arriba.

### Qdrant

`qdrant` se lee y se escribe con el cliente propio de Qdrant, `qdrant-client`. Qdrant es **solo en vivo**: una propiedad del tipo de origen, no un ajuste. Responde con su índice vectorial y su Query API, y ninguna réplica puede responder lo mismo, así que sus tablas nunca se replican. No hay opción Replicate para un origen Qdrant ni para sus tablas; un valor `replicate` o `row_materialize` en uno se rechaza.

**Conexión.** `host` es la URL de la instancia con su puerto (`https://<cluster>.cloud.qdrant.io:6333`, o un `http://qdrant:6333` autoalojado); `password` es su clave de API, una referencia a un secreto resuelta cuando se lee el origen, vacía para una instancia sin clave. `federation_hints.prefer_grpc: true` lee por el puerto gRPC de Qdrant en lugar de REST. El origen tiene un esquema, `default`.

**Tablas de colección.** Una colección es una tabla. Register Table ofrece las columnas de la colección: `id` (el id del punto, su clave, ruta `@id`), los campos del payload (un campo indexado se tipa por su índice, cualquier otro según sugieren los puntos de muestra; los campos anidados se aplanan con su ruta) y una columna `json` por vector (ruta `@vector`, o `@vector.<name>` para un vector con nombre), ofrecida sin seleccionar. Una columna de vector nombra el modelo de embeddings registrado del que contiene embeddings, elegido entre los modelos cuyas dimensiones son el tamaño del vector (en Register Table y en la pestaña Tipo y claves del editor); elegir un modelo registra el vector.

**Instrucciones sobre un origen Qdrant.** Una instrucción que lee una sola colección se ejecuta en Qdrant cuando la API de Qdrant puede expresarla con exactitud:

- las condiciones sobre el payload se convierten en un filtro de Qdrant (`=`, `<>`, `IN`, rangos sobre números y fechas, `IS NULL`, `AND`, `OR`, `NOT`); se conserva la semántica de NULL de SQL, de modo que `lang <> 'en'` omite los puntos sin `lang`;
- una condición solo sobre `id` (`=` o `IN`) es un `retrieve`; cualquier otra lectura es un `scroll` filtrado;
- `ORDER BY cosine_similarity(<vector>, <query>) DESC LIMIT k` es un `query_points` sobre un vector cuya distancia es Cosine, con el filtro aplicado antes del top-k. Un texto de consulta lo incrusta el modelo del vector. La respuesta lleva una advertencia de que el top-k es aproximado, elegido mediante el índice de Qdrant.

Todo lo demás (un agregado, un `GROUP BY`, una columna calculada o enmascarada, un orden distinto del más similar primero) lo responde el motor, como se describe a continuación.

**Instrucciones que responde el motor.** Cuando una instrucción une una colección con otro origen, o calcula sobre sus columnas, la parte de la colección se lee en vivo desde Qdrant para esa instrucción y se entrega al motor como filas. La parte son las columnas de la colección, las condiciones de la instrucción sobre la colección sola (entre ellas sus reglas de fila) y, cuando la colección se lee sola en su `SELECT`, el orden por similitud y el `LIMIT` de ese `SELECT`. Una parte que Qdrant no puede expresar con exactitud se rechaza, nombrando la tabla y el motivo. Una parte sin condición, sin clave y sin top-k leería toda la colección y también se rechaza. Una condición sobre un parámetro (`$1`) se deja al motor.

**Escrituras.** `INSERT`, `UPDATE` y `DELETE` sobre una colección se ejecutan en Qdrant:

- `INSERT … VALUES` nombra la columna `id` y escribe puntos nuevos. Un `id` que la colección ya contiene se rechaza por su nombre. La comprobación lee primero los ids, por lo que no es atómica con la escritura.
- `INSERT … ON CONFLICT (id) DO UPDATE SET c = EXCLUDED.c, …` es un upsert. Las columnas que la instrucción no establece conservan sus valores.
- `UPDATE` y `DELETE` actúan sobre los puntos que selecciona su `WHERE`, como filtro de Qdrant; una condición sin forma de filtro se rechaza.
- Un texto escrito en una columna de vector lo incrusta el modelo de la columna. Un vector escrito en ella debe tener las dimensiones del modelo.

**Tablas de búsqueda.** La búsqueda vectorial es una tabla de búsqueda: su `query_template` contiene una petición de la Query API.

```json
{"collection": "docs", "query": "@query_vector", "using": "text",
 "filter": {"must": [{"key": "lang", "match": {"value": "@lang"}}]},
 "limit": "@k", "score_threshold": 0.5, "model": "text-embedding-3-small"}
```

- `query` es siempre un argumento; `using`, `filter`, `limit`, `score_threshold` y `model` son opcionales. Cualquier valor de `using`, `filter`, `limit` o `score_threshold` que sea exactamente `"@name"` también es un argumento.
- Cada argumento es un parámetro de la tabla (`query_arg`), convertido estrictamente a su tipo declarado e insertado en la petición como valor JSON, nunca en su texto. El argumento de `query` es `array(double)` y obligatorio.
- Quien llama pasa los argumentos como cualquier parámetro: `SELECT id, score, title FROM docs_search WHERE _nf_query_vector = '[0.1, 0.2, …]' AND _nf_lang = 'en'` en SQL, argumentos de campo en GraphQL.
- Un argumento opcional no dado no filtra nada: su condición se omite del filtro. Sin `limit`, una búsqueda devuelve el valor por defecto de Qdrant, 10 puntos.
- Qdrant aplica el filtro antes de elegir sus puntos más cercanos, así que es exacto. Cualquier otra condición de la instrucción estrecha después los puntos que devolvió la búsqueda; no obtiene más.
- `score` es la métrica propia de la colección para el vector buscado: para una colección Cosine, la similitud del coseno. Register Table lista los vectores de la colección con su tamaño y distancia.
- `model` nombra un modelo de embeddings registrado (`vector_models`, admin → AI Models). Un texto dado como consulta se incrusta entonces con ese modelo, y un vector dado debe tener las dimensiones de ese modelo. Sin `model`, la consulta debe ser un vector.

Una búsqueda se lee por instrucción con sus argumentos, y sus filas se conservan bajo esos argumentos durante el TTL de la tabla. Una tabla de búsqueda es de solo lectura.

### Streaming

| Tipo de origen | Mecanismo | Mutaciones |
| ------------ | ----------- | ----------- |
| `kafka` | Conector Kafka federado; esquema mediante Confluent Schema Registry (Avro, Protobuf, JSON Schema), definición manual o inferencia por muestreo | No; la salida en vivo de una tabla publica en Kafka |
| `websocket` | Feed WebSocket externo — conecta, se suscribe, recibe eventos; los resultados se materializan | No |
| `rss` | Feed RSS 2.0 / Atom — sondea, marca de agua por pubDate/updated; los resultados se materializan | No |

**Kafka en los motores alojados.** En los motores aprovisionados de la plataforma alojada, una fuente Kafka debe nombrar un registro de esquemas (`schema_registry_url`). Una cuyos tópicos tienen columnas que solo declara el modelo se rechaza por nombre al guardarla o aplicarla (HTTP 422, `sources.kafka_needs_schema_registry`), ya que sus descripciones de tabla necesitarían un directorio que no se puede dar al motor en ejecución. Las credenciales SASL llegan a esos motores en memoria (consulte [Seguridad](security.md#the-federation-engine-trino)).

### Receptor push {: #push-receiver }
| Tipo de origen | Mecanismo | Mutaciones |
| ------------ | ----------- | ----------- |
| `ingest` | Servicios externos envían eventos JSON mediante POST; los resultados se materializan | No |

### Grafo y semántica {: #graph-semantic }
| Tipo de origen | Mecanismo | Mutaciones |
| ------------ | ----------- | ----------- |
| `neo4j` | Cypher mediante API HTTP, resultados almacenados en caché en PostgreSQL | No |
| `sparql` | SPARQL 1.1 POST, resultados almacenados en caché en PostgreSQL | No |

### Basados en archivos {: #file-based }
Tres mecanismos cubren los archivos. Los orígenes de un solo archivo y el rastreador de archivos usan el campo `path` en lugar de `host`/`port`. El tercero, [Logs](#logs), nombra sus archivos mediante un glob en su `mapping` y lee solo archivos del servidor de Provisa.

**Orígenes de base de datos en archivo y de un solo archivo.** Un origen `sqlite`, `duckdb`, `csv` o `parquet` apunta `path` a un único archivo. (Un origen `duckdb` guarda su ubicación en el campo `database` del formulario.)

| Tipo de origen | Ubicaciones | Mutaciones |
| --- | --- | --- |
| `sqlite` | ruta local, `https://`, `s3://`, `gs://`, `az://` | Sí en un archivo local; ninguna en uno remoto |
| `duckdb` | ruta local, `https://`, `s3://`, `gs://`, `az://` | Sí en un archivo local; ninguna en uno remoto |
| `csv` | ruta local, `https://`, `s3://`, `gs://`, `az://` (motor DuckDB) | No |
| `parquet` | ruta local, `https://`, `s3://`, `gs://`, `az://` (motor DuckDB) | No |

Una ruta con cualquier otro esquema (`ftp://`, por ejemplo) se rechaza: "DuckDB reads no file at a `ftp://` location".

**Credenciales para una ubicación remota.** Póngalas en los `federation_hints` del origen. Cada una se limita al bucket o contenedor de la propia ruta, así que las credenciales de un origen nunca se usan para la ubicación de otro origen. Un origen que no da ninguna se lee como una ubicación pública.

| Ubicación | Claves de `federation_hints` |
| --- | --- |
| `s3://` | `access_key_id`, `secret_access_key`; opcionales `region`, `endpoint` (un almacén compatible con S3 como R2 o MinIO) |
| `gs://` | `gcs_access_id`, `gcs_secret_key` (una clave HMAC) |
| `az://` | `azure_account_name`, `azure_account_key` |

**Los archivos SQLite y DuckDB remotos son de solo lectura.** El motor DuckDB los adjunta en su lugar con `READ_ONLY`, y una tabla sobre uno de ellos no ofrece insert, update ni delete. En un motor que no adjunta SQLite, un archivo SQLite remoto se lee a través de una conexión DuckDB temporal que lo adjunta en solo lectura, y sus filas aterrizan como de costumbre.

**Subir un archivo.** En el formulario de un origen `sqlite`, `duckdb`, `csv` o `parquet`, suba el archivo en lugar de escribir una ruta. El archivo se transmite al disco, nunca se guarda en memoria, a la carpeta de la organización en `files/<source id>/<file name>`, y la ruta del origen pasa a apuntar a él.

- **Reemplazar.** Subir de nuevo ocupa el lugar del archivo anterior, sea cual sea su nombre, y el origen se aprovisiona de nuevo para que la siguiente lectura vea el archivo nuevo.
- **Eliminar.** Eliminar el origen elimina su archivo subido. Nada más lo elimina.
- **Quién.** Solo los roles con la capacidad `source_registration` pueden subir archivos.
- **Límite de tamaño.** La configuración `sources.max_upload_bytes` (entorno `PROVISA_SOURCES_MAX_UPLOAD_BYTES`) limita el archivo, 1 GiB (1073741824 bytes) por defecto, mínimo 1, y se lee al empezar una subida. Un archivo mayor se rechaza con HTTP 413 indicando el límite, y no se conserva nada de él.
- **Ruta.** Mientras la ruta sea el archivo subido del origen, el campo de ruta del formulario es de solo lectura.
- **Nombres.** El id del origen y el nombre del archivo solo pueden usar letras, dígitos, `.`, `_` y `-`.
- **Almacenamiento.** Un archivo subido se guarda como cualquier archivo de la organización: en la carpeta de la organización, cifrado cuando esa carpeta lo está, y visible para todos los nodos en un montaje compartido.

**Escrituras en un archivo SQLite o DuckDB local, y bloqueos.** Una escritura toma el bloqueo del archivo y nunca lo espera. Cuando otro proceso o nodo tiene el archivo, la escritura se rechaza, indicando el origen y que su archivo está bloqueado.

- **SQLite.** La escritura establece `PRAGMA busy_timeout = 0`, así que un archivo bloqueado falla de inmediato.
- **DuckDB.** DuckDB comparte un archivo solo entre aperturas de solo lectura. Por eso el adjunto del motor y la conexión propia del origen mantienen el archivo en solo lectura, y todos los nodos lo leen a la vez. Una escritura pausa a los poseedores de este proceso, abre el archivo en lectura y escritura solo para esa instrucción y lo cierra. Si la apertura no puede tomar el bloqueo, la escritura se rechaza.

**Montajes compartidos.** En una carpeta de organización cifrada cuyo montaje comparten varios nodos (configuración `org_files.shared_mount`), una escritura en un archivo SQLite o DuckDB de esa carpeta se rechaza directamente, indicando el origen. La capa de cifrado de carpetas (gocryptfs) no transmite los bloqueos de archivo a los demás nodos, así que nada impediría que dos nodos escribieran a la vez. Las lecturas siguen funcionando.

Una carpeta compartida sin cifrado debe montarse con bloqueos que lleguen a todos los nodos: un montaje NFS con el bloqueo activado, no `nolock` ni `local_lock`. Ninguna comprobación lo verifica; es un requisito del despliegue.

**Rastreador de archivos (origen `files`).** Apunta `path` a un glob, lo recorre de forma recursiva y registra el directorio como un catálogo federado de tablas. Lee muchos formatos por muchos transportes:

| Formatos | Transportes |
| --- | --- |
| CSV, TSV, JSON, YAML, Excel (XLS/XLSX), Parquet, Arrow, y documentos convertidos a tablas — HTML, Markdown, DOCX, PPTX | Sistema de archivos local, HTTP(S), `s3://`, `hdfs://`, `ftp://`/`ftps://`, `sftp://`, `iceberg://`, SharePoint (REST y Microsoft Graph) |

```yaml
- id: sales_files
  type: files
  path: s3://bucket/sales/**/*.csv   # glob; local and http(s):// also supported
```

Cualquier origen `files` también puede declarar un `crawl` en su mapping, con los mismos ajustes que se listan en [Wikipedia](#wikipedia): el origen lee entonces primero páginas web y deja cada tabla de datos HTML y cada archivo de datos enlazado en su directorio como una tabla.

En el motor DuckDB, `files` se lee de forma nativa, con una vista de escaneo `read_csv_auto` por cada `<table>.csv` bajo el directorio resuelto. En un motor sin conector `files` propio, las filas se aterrizan a través del mismo servidor pgwire de Calcite incluido con el conector (`pgwire-file`) que usan sharepoint y splunk. Consulte [Conectores SaaS empresariales](#enterprise-saas-connectors) más abajo.
#### Logs {: #logs }

Un origen `logs` convierte los archivos de registro del servidor de Provisa en una tabla. Usted indica los archivos con una ruta completa o un glob, y elige el formato en que están escritos los registros. Cada registro se convierte en una fila. Un registro que el formato no puede leer se conserva, con su texto sin procesar y el motivo, y nunca se descarta.

```yaml
sources:
  - id: mac-install-log
    type: logs
    mapping:
      glob: /var/log/install.log*   # the live file and its rotated, compressed copies
      format: macos_install
```

El mapping admite cuatro claves y rechaza cualquier otra por su nombre: `glob` y `format` (ambas obligatorias), y `pattern` y `record_start`, que solo admite el formato `regex`.

```yaml
    mapping:
      glob: /var/log/myapp/*.log
      format: regex
      pattern: '(?P<ts>\S+ \S+) (?P<level>[A-Z]+) (?P<message>.*)'
      record_start: '\d{4}-\d{2}-\d{2} '   # a stack trace joins the line it follows
```

En el formulario de Orígenes, un origen Logs aparece como "Log files" bajo File. El botón **Preview** muestra, antes de guardar nada, los archivos con los que coincide el glob, las columnas que da el formato y las primeras 20 filas del archivo más reciente.

**El glob.** Debe ser una ruta completa; `~` se expande, y una ruta relativa se rechaza ("name the files by their full path"). Se evalúa con `glob` de Python en modo no recursivo, así que `**` se comporta como `*` y coincide dentro de un solo nivel de directorio. Solo se toman archivos regulares, en orden de nombre. Un archivo vacío se omite hasta que tiene bytes. El glob puede nombrar una variable de entorno, como en `${env:LOG_DIR}/*.log`. La fuente guarda la referencia tal como se escribió y la resuelve cada vez que lee los archivos; una variable sin definir se rechaza por su nombre.

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

`_file_id`, `_line_offset` y `_entry` son la clave de la fila, de modo que un registro leído dos veces es una sola fila.

**Formatos y presets.** El selector de formato agrupa los formatos por familia; la última opción, `regex`, es su propio patrón.

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

Son 56 formatos integrados y `regex`.

**Cómo obtiene columnas un formato.** Los formatos obtienen sus columnas de cuatro maneras.

- **Columnas fijas.** La mayoría de los formatos declaran sus columnas y tipos. `rfc3164`, por ejemplo, da `timestamp`, `facility`, `severity`, `host`, `process`, `pid` y `message`; `facility` y `severity` se calculan a partir del valor `<PRI>`. Una línea de syslog BSD no lleva año, así que el año es el de la última escritura del archivo, o el anterior cuando el mes de la línea es posterior al de esa escritura.
- **Campos nombrados por el registro.** `jsonl` y `logfmt` toman sus columnas de los campos vistos en las primeras líneas de los archivos (ajuste `logs.preview_lines`, por defecto 1000). Cada columna recibe el tipo más estrecho en el que caben todos los valores vistos: boolean, luego integer, float, timestamp y json, y si no text. Un campo que lleva un registro posterior y que ninguna columna tiene va a una columna json `extra`.
- **Campos nombrados por una cabecera.** `w3c` y los presets construidos sobre él leen la línea `#Fields:` de cada archivo; `date` y `time` se unen en una sola columna `timestamp`. `aws_cloudfront`, `ms_iis`, `ms_windows_firewall`, `ms_exchange_tracking` y `zeek_tsv` leen las líneas de cabecera `#` del mismo modo, y `aws_vpc_flow` lee una línea de cabecera que empieza por `version`. El lugar donde se leyó una línea de cabecera se queda con el archivo, de modo que un archivo que se sigue leyendo después conserva sus campos.
- **`regex`.** Cada grupo con nombre, como `(?P<level>…)`, es una columna de tipo text.

La pantalla Register Table lista estas columnas con sus tipos y marca la clave. Al leer las filas, cada valor se convierte al tipo que su columna tiene en la tabla registrada, de modo que un tipo cambiado allí es el que se aplica. Un valor que su columna no puede contener pasa a NULL y su motivo va a `_parse_error`.

Un registro más largo que `logs.max_line_bytes` (por defecto 65536) conserva sus primeros bytes en `_raw` y no se analiza; `_parse_error` da su longitud. Un archivo que empieza con una marca de orden de bytes UTF-8 o UTF-16 se lee con esa codificación, y cualquier otro como UTF-8.

**Registros que ocupan varias líneas.** Un formato con patrón de inicio de registro forma un registro con una línea que coincide más todas las líneas siguientes que no coinciden. Un stack trace se queda con su excepción, y una entrada de consulta lenta se queda entera. Las líneas anteriores a la primera coincidencia son un registro propio; el formato suele rechazarlo, y la fila se conserva. Los formatos integrados con inicio de registro son `rfc3164`, `macos_install`, `aws_cloudtrail`, `postgresql_stderr`, `postgresql_csvlog`, `mysql_error`, `mysql_slow`, `log4j2_default`, `logback_default`, `python_logging`, `chrome_net_export`, `ms_sqlserver_errorlog` y `azure_monitor`. Con `regex`, defina usted `record_start`.

Dos formatos leen sus registros por sí mismos en lugar de por líneas. `kubernetes_cri` une en un registro las líneas parciales (`P`) que el runtime dividió, y `windows_evtx` lee el archivo binario `.evtx`.

Un archivo en el que aún se está escribiendo se lee hasta su última línea completa. En un formato multilínea se lee hasta el inicio de su último registro, que la línea siguiente podría continuar. Cuando el archivo pasa 2 segundos sin escrituras, ese último registro se toma como completo.

**Rotación.** Un archivo se identifica por una suma de verificación de sus primeros 4096 bytes, descomprimidos, no por su nombre. Cuando `system.log` rota a `system.log.0` y luego se comprime a `system.log.0.gz`, cada copia es el mismo archivo. La lectura continúa desde el byte al que llegó la última, así que ninguna línea se lee dos veces.

- La compresión se reconoce por los primeros bytes del archivo, nunca por su nombre: gzip y bzip2. Un archivo comprimido se trata como completo.
- Una copia rotada y su gemela comprimida, ambas coincidentes, se leen una vez, desde la comprimida.
- Un archivo que resulta más corto que el desplazamiento ya leído fue truncado: es un archivo nuevo y se lee desde su inicio. Las filas del antiguo permanecen.
- Los archivos de menos de 4096 bytes que empiezan igual se distinguen por el inodo, así que siguen siendo dos archivos.
- Un archivo con el que el glob ya no coincide se olvida; sus filas permanecen como historial.

**Actualización y entrega en vivo.** Una tabla Logs es una réplica. Nada vigila los archivos. En cambio, cuando la señal de cambio del origen es `probe` o `ttl_probe`, su probe calcula un digest de la ruta, el inodo, el tamaño y la última escritura de cada archivo coincidente; cuando el digest difiere, se pide una actualización. Los tipos de probe que admite un origen Logs son `hash` y `none`.

Una actualización lee solo lo nuevo. Cada archivo se lee desde donde llegó la última pasada, y las filas se escriben por su clave, de 5000 en 5000, antes de guardar la nueva posición. Una pasada que se detiene entre ambos pasos vuelve a leer esos registros, lo que deja las mismas filas. Cada pasada sella sus filas en `_landed_at`, la columna que la entrega en vivo usa como marca de agua. Una definición de tabla cambiada es una reconstrucción completa que lee cada archivo desde su inicio.

**Raíces permitidas.** El ajuste de operador `logs.allowed_roots` lista los directorios en los que puede resolverse un glob. Su valor por defecto es `["/var/log"]`; es una lista en la tarjeta Security de Admin → Configuración, que también se fija con `PROVISA_LOGS_ALLOWED_ROOTS`, y un cambio pide confirmación. Cada raíz se resuelve tras expandir `~` y seguir los enlaces simbólicos.

Se aplican dos comprobaciones. La parte fija del glob, hasta su primer comodín, debe resolverse dentro de una raíz; esto se comprueba al guardar el origen y cada vez que se previsualiza o se lee. Cada archivo coincidente debe además resolverse dentro de una raíz una vez seguidos sus enlaces simbólicos, de modo que un enlace simbólico que salga de `/var/log` se rechaza. El rechazo nombra la ruta, a qué se resolvió y las raíces permitidas:

```text
/home/me/app.log resolves to /home/me/app.log, outside the directories log files may be read from: /var/log
```

Al guardar, el código es `schema.logs_path_refused`; en la vista previa es un 403 con `logs.path_refused`.

**Rechazo en entorno alojado (SaaS).** Un despliegue que aloja a muchas organizaciones no lee ningún archivo de registro propio. Toda comprobación de ruta rechaza primero, al guardar, al previsualizar y al leer, con "`<path>`: a server that holds many organizations reads no log file of its own". Los archivos son del servidor, no de una organización concreta, así que ningún inquilino puede leerlos. El despliegue sabe si aloja a muchas organizaciones por su estado de multitenencia.

#### Conjuntos de datos de Kaggle {: #kaggle-datasets }

Kaggle es un sitio para descargar archivos de datos. Un conjunto de datos de Kaggle que usted añade se registra como origen `files`, y se consulta igual que cualquier otro origen `files`.

**Añadir un conjunto de datos.** Abra Orígenes → Suscripciones → Kaggle. El formulario funciona en dos pasos, porque la búsqueda de conjuntos de datos de Kaggle necesita un token, así que el selector no aparece hasta que el suyo se valida.

1. **Token.** Introduzca un token de la API de Kaggle y haga clic en "Validate". La validación llama a `POST https://www.kaggle.com/api/v1/datasets/create/new` con el cuerpo vacío. Un `401` significa que el token no es válido, y cualquier otra respuesta significa que sí lo es. Kaggle revisa el cuerpo vacío antes de crear nada, así que no se guarda nada.
2. **Selector.** Un campo de búsqueda consulta `GET /api/v1/datasets/list` mientras escribe, y cada resultado muestra el título y la descripción del conjunto de datos. Elija uno y haga clic en "Add Dataset".

"Add Dataset" descarga el paquete desde `GET /api/v1/datasets/download/{owner}/{ref}`, descomprime sus archivos CSV y Parquet en `<PROVISA_DATA_DIR>/kaggle/<owner>/<ref>/<file-stem>/<file-name>` y crea **un** origen `files` cuyo `path` es ese directorio raíz. El servidor nunca guarda el token.

Después registre las tablas que quiera en la pantalla normal Register Table. Cada archivo aparece como su propia tabla, como en cualquier otro origen `files`.

**Solo CSV y Parquet.** Un paquete que contiene un archivo `.sqlite` o `.db` se rechaza con un error que lo indica. Solo se preparan archivos CSV y Parquet.

**Actualizar.** Cuando Kaggle publica una nueva versión de un conjunto de datos, llame a la mutación GraphQL `refreshKaggleSource` con el ID del origen y un token válido. Vuelve a preparar los archivos en su sitio y limpia la caché del endpoint de archivos, para que la siguiente consulta recoja cualquier cambio de esquema. Los `kaggle_owner` y `kaggle_ref` guardados en `federation_hints` al crear el origen indican qué conjunto de datos traer.

**Sin YAML propio.** Un origen de Kaggle solo se crea desde el formulario de orígenes. Exportado a YAML, aparece como `type: files` con `kaggle_owner` y `kaggle_ref` en `federation_hints`. Para volver a traerlo de Kaggle hace falta la actualización en la UI o la mutación `refreshKaggleSource`. En un entorno aislado, apunte el `path` del YAML a un directorio que haya preparado usted.

#### Wikipedia {: #wikipedia }

Wikipedia rastrea las páginas que usted indica. Cada tabla de datos de una página, y cada archivo CSV, TSV, Excel, JSON o Parquet enlazado, se convierte en una tabla del origen. Como Kaggle, se registra como un origen `files` con un `crawl` en su mapping. Agregarlo no registra nada: un steward registra las tablas deseadas mediante la pantalla normal de Register Table.

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

Provisa conoce el encabezado "See also" de estas ediciones: `en`, `de`, `es`, `it`, `pt`, `nl`, `pl`, `sv`, `ja`, `zh`, `ru`. Para cualquier otra edición con "See also" desactivado, el formulario pregunta cómo llama la edición a esa sección; escriba su título tal como lo muestra una página (por ejemplo `Voir aussi`). Un origen guardado sin él se rechaza (`wikipedia.see_also_heading_needed`).

**Qué enlaces se siguen.** Un enlace se elige por lo que dice el propio marcado de la página sobre él, nunca por un nombre en su dirección: esos nombres difieren en cada edición de idioma y también aparecen en títulos corrientes.

- Se siguen: enlaces a otros artículos que están en el texto, las listas y las tablas de datos del propio artículo. La marca de marcado es `a[rel='mw:WikiLink']`.
- No se siguen: páginas aún no escritas, la propia página, búsquedas de fuentes de libros, páginas de archivos, otros wikis y sitios externos.
- No se leen en absoluto (se quitan antes de reunir enlaces y tablas): cuadros de navegación, cuadros de información, notas y citas, categorías, el índice, imágenes y avisos de mantenimiento.
- No se leen salvo que active la opción: la sección "See also".

Solo las tablas que los editores marcan como tablas de datos (`table.wikitable`) se convierten en tablas, y una tabla con menos de 2 filas no se ofrece.

**Ajustes avanzados de rastreo.** El formulario los mantiene plegados bajo "Crawl settings" y muestra cada uno con su valor predeterminado; deje un campo vacío para conservarlo. En un mapping de `files` los mismos nombres van bajo `crawl:`, y un nombre fuera de esta lista se rechaza.

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

**Nombres de tabla.** Una tabla rastreada se llama `<page>__<name>`. El nombre es el título (caption) de la tabla o, si no tiene, el último encabezado por encima de ella; una tabla sin ninguno de los dos es `table_1`, `table_2` y así sucesivamente. Ambas partes pasan a minúsculas, y cada tramo de otros caracteres se convierte en `_`. Por ejemplo, una tabla bajo el encabezado "Tallest buildings" en la página `List_of_tallest_buildings` pasa a ser `list_of_tallest_buildings__tallest_buildings`. Un archivo de datos enlazado conserva su propio nombre.

**Actualización.** Un rastreo se conserva durante el período de caché, `1 days` de forma predeterminada (`html_cache_ttl`). Las páginas solo se obtienen de nuevo cuando ha pasado ese tiempo.

### Observabilidad y otros {: #observability-other }
`prometheus` tiene un conector de Trino (propiedades construidas a partir del DSL de mapeo del tipo). `google_sheets` es un tipo de origen registrado sin conector de Trino y se materializa a través del pipeline de caché de API.

| Tipo de origen | Nombre del conector | Mutaciones |
| ------------ | ----------------- | ----------- |
| `google_sheets` | — (materializado) | No |
| `prometheus` | prometheus | No | Una métrica es una tabla y una muestra es una fila (`timestamp`, `value`, una columna por etiqueta); en cualquier motor sin conector en vivo Provisa lee la API HTTP — nombres de métricas y etiquetas para Register Table, `query_range` sobre el rango de la tabla para el aterrizaje |

### Conectores SaaS empresariales {: #enterprise-saas-connectors }

Google Workspace y Microsoft 365 se leen con cargadores propios; consulte [Google Workspace y Microsoft 365](#google-workspace-and-microsoft-365). SharePoint, Salesforce, ServiceNow, Jira, el inventario de nube y Splunk se leen a través de drivers que se ejecutan como proceso propio y hablan el protocolo de PostgreSQL (`pgwire-sharepoint`, `pgwire-salesforce`, `pgwire-servicenow`, `pgwire-jira`, `pgwire-cloudops`, `pgwire-splunk`), así que ninguno tiene driver directo. En el motor DuckDB el endpoint del driver se adjunta en vivo mediante la extensión postgres: Register Table lista las tablas del conector desde el catálogo adjunto, las consultas leen el conector en su sitio, y los filtros y proyecciones se le trasladan. Todos los demás motores aterrizan las filas en el almacén de materialización para la federación. Los paquetes de drivers se descargan por sistema operativo y arquitectura de una versión fijada (`pgwire-<connector>-<version>-<os>-<arch>.tar.gz`; macOS arm64, Linux x86_64, Windows x86_64). Un operador puede indicar en su lugar, por conector, una carpeta de paquete compilada para este host en Admin → Configuración → Servidores de conectores (`connectors.<connector>.bundle_dir`); consulte [Admin](admin.md). Los cinco conectores de Trino (Jira no tiene) siempre comparan nombres sin distinguir mayúsculas, igual que cada producto.
#### `sharepoint`

Las listas de SharePoint se enumeran como esquemas y se exponen como tablas consultables. Dos métodos de autenticación: `CLIENT_CREDENTIALS` (predeterminado) y basado en certificado mediante un certificado PFX. Los valores secretos en `mapping` se resuelven a través del motor de secretos antes de llegar al conector.

| Campo de origen | Propiedad del conector | Notas |
| --- | --- | --- |
| `base_url` o `host` | `site-url` | URL del sitio de SharePoint |
| `username` | `client-id` | ID de cliente de la app de Azure |
| `password` | `client-secret` | Secreto de cliente de la app de Azure |
| `database` | `tenant-id` | UUID del inquilino de Azure |
| `mapping.auth_type` | `auth-type` | `CLIENT_CREDENTIALS` (predeterminado) o `CERTIFICATE` |
| `mapping.certificate_path` | `certificate-path` | Ruta del PFX cuando `auth_type: CERTIFICATE` — debe ser ABSOLUTA |
| `mapping.certificate_password` | `certificate-password` | Contraseña del PFX — la clave debe estar presente, cadena vacía para un PFX sin contraseña |

La autenticación por certificado en los motores que no son Trino conlleva dos reglas adicionales, ambas aplicadas al construir el operando `model.json` del servidor pgwire de Calcite. `certificate_path` debe ser absoluta: el servidor se ejecuta con su directorio de paquete como directorio de trabajo, de modo que una ruta relativa se resuelve dentro de la caché de runtime-deps y el PFX no se encuentra. `certificate_password` debe estar presente en `mapping` incluso cuando el PFX no tiene contraseña, en cuyo caso es la cadena vacía — el adaptador de Calcite rechaza una contraseña nula de plano, y una clave ausente se trata como un error de configuración en lugar de leerse silenciosamente como una contraseña vacía. Un valor ausente o relativo lanza `MissingConnectorConfig` nombrando el campo.

Cuando el conector no expone `information_schema.columns`, registre la tabla con definiciones de columna explícitas (obtenidas de la API de Microsoft Graph) mediante la mutación `registerTable`.

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

Cada sObject que la credencial puede consultar es una tabla que el steward puede registrar (`Account`, `Opportunity`, objetos personalizados `*__c`); nada se registra automáticamente. Las columnas y los tipos provienen del describe del sObject. Los filtros, las proyecciones, las ordenaciones y los límites se delegan a Salesforce como SOQL.

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

**Escrituras.** `INSERT`, `UPDATE` y `DELETE` sobre un sObject registrado son mutaciones: gobernadas, auditadas y tratadas por entorno como cualquier otra mutación. En todos los motores, Trino incluido, una escritura se ejecuta en el servidor pgwire de la fuente; en Trino ese servidor se ejecuta solo para las escrituras. Cada sentencia se envía a Salesforce y se confirma al ejecutarse, por lo que no puede revertirse. El servidor no devuelve las filas que escribe: una sentencia con cláusula `RETURNING` se rechaza y GraphQL no ofrece campos de mutación para estas tablas; `INSERT`, `UPDATE`, `DELETE` y `COPY` de SQL son los caminos de escritura. Una escritura es una sola sentencia sobre una tabla: `INSERT … ON CONFLICT`, `UPDATE … FROM`, `DELETE … USING`, `MERGE` y `TRUNCATE` se rechazan, nombrando la forma. Una fuente SharePoint se escribe de la misma manera.

**Arranque.** Antes de aceptar conexiones, el servidor de la fuente lee las columnas de cada sObject, unos cuatro minutos para una organización con 1.200. Se paga una vez: los resultados de describe se guardan en el directorio de estado propio de la fuente, bajo el directorio de datos de la instancia. El servidor se inicia cuando la fuente se registra o se carga. En Trino el catálogo `salesforce` conserva sus propios resultados de describe, un directorio por fuente bajo `/data/trino/cache/salesforce-describe` en el coordinador, de modo que el coste también se paga una sola vez allí cuando esa ruta es un volumen que sobrevive al contenedor: el volumen `trino_cache` de la pila compose, o el volumen de caché del coordinador del chart de Helm (`trino.coordinator.cache.persistence`). Hasta que acepta conexiones, Registrar tabla muestra el conector como iniciándose y una escritura recibe el mismo mensaje en lugar de esperar.

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

Todas las tablas de la instancia están disponibles para registrarse, y nada se registra automáticamente. Una instancia con mucho uso tiene miles de tablas, así que Register Table las agrupa por tema. Cada tema es el nombre visible de un paquete de ServiceNow, o "(no package)" para una tabla que no pertenece a ninguno. Cada tabla aparece como `name — label`. Seleccionar una tabla lee sus columnas de la instancia; listar las tablas no.

Una fuente ServiceNow nombra la URL `https://…` completa de la instancia junto con un nombre de usuario y contraseña. La URL se toma de `base_url` o `host`; un host simple o cualquier otro esquema se rechaza, nombrando el campo (`instanceUrl`). Un nombre de usuario o contraseña ausente se rechaza, nombrando el campo. Cada valor puede ser una referencia `${secret:…}` o `${env:…}`. Un secreto escrito como literal se guarda en la bóveda de la organización y la fuente conserva su referencia.

| Campo fuente | Propiedad del adaptador | Notas |
| --- | --- | --- |
| `base_url` o `host` | `instanceUrl` | URL `https://…` completa de la instancia (p. ej. `https://acme.service-now.com`). Un host simple u otro esquema se rechaza |
| `username` | `username` | Nombre de usuario de autenticación básica |
| `password` | `password` | Contraseña de autenticación básica |

**Lecturas.** En Trino la fuente se lee a través del catálogo `servicenow`, que solo lee. En cualquier otro motor, el motor adjunta el servidor pgwire de la fuente y la lee en su sitio. Solo las columnas seleccionadas se solicitan a la instancia (`sysparm_fields`). Un filtro se envía a la instancia (`sysparm_query`) solo en una forma que el adaptador ha verificado contra una instancia en vivo; todos los demás filtros, y cada ordenación y límite, se aplican a las filas leídas.

Un campo de referencia aparece como dos columnas: `<field>` contiene el sys_id del registro referenciado y `<field>__display` contiene el valor de visualización. Un campo vacío es NULL. Un campo cuyo tipo no puede mapear el adaptador genera un error que nombra la tabla, el campo y el tipo.

La búsqueda de tablas (lenguaje natural) coincide con el nombre, la etiqueta y el tema de cada tabla. Una vez registrada una tabla, sus columnas se incorporan a la búsqueda.

**Ciclo de vida del servidor.** El servidor sirve solo las tablas registradas desde él. Registrar o eliminar una tabla reinicia el servidor y vuelve a aprovisionar la fuente en el motor; una sentencia que llegue durante ese reinicio falla.

**Escrituras.** `INSERT`, `UPDATE` y `DELETE` sobre una tabla registrada son mutaciones: gobernadas, auditadas y tratadas por entorno como cualquier otra mutación. En todos los motores, Trino incluido, una escritura se ejecuta en el servidor pgwire de la fuente. El servidor devuelve la fila que la instancia tiene tras cada escritura, por lo que una cláusula `RETURNING` se acepta y GraphQL ofrece campos de mutación para estas tablas. Una escritura es una sola sentencia sobre una tabla: `INSERT … ON CONFLICT`, `UPDATE … FROM`, `DELETE … USING`, `MERGE` y `TRUNCATE` se rechazan, nombrando la forma.

**Import sets.** Cuando una tabla tiene un mapa de transformación activo hacia ella, Register Table ofrece sus import sets, y el steward puede vincular la tabla a uno (una tabla con varios mapas nombra cada uno). Las inserciones en una tabla vinculada pasan por la Import Set API de la instancia y la transformación de ese mapa; las inserciones en una tabla no vinculada, y cada actualización y eliminación, se escriben directamente en la tabla. Una fila que la transformación rechaza se rehúsa, nombrando la fila; las filas ya escritas permanecen escritas. La vinculación se comprueba cuando la tabla se guarda: debe ser el import set de un mapa de transformación activo hacia la tabla, en una fuente ServiceNow.

```yaml
- id: helpdesk-sn
  type: servicenow
  base_url: https://acme.service-now.com
  username: ${env:SN_USERNAME}
  password: ${env:SN_PASSWORD}
```

#### `jira`

Un sitio de Jira Cloud se lee y se escribe mediante su propio adaptador de Calcite. Una tabla de una fuente Jira es un **formulario**: un proyecto y un tipo de incidencia, y los campos que leen sus columnas. Register Table lista cada formulario del que la cuenta puede crear una incidencia, agrupados por proyecto; al seleccionar uno se leen sus campos de Jira, y listar los formularios no lo hace. Cada formulario tiene las columnas `key` (su clave primaria), `id`, `summary`, `status`, `created` y `updated`, y luego una columna por cada campo elegido, con el nombre que le da el adaptador: el nombre del campo en snake case (`Story Points` es `story_points`). Un campo cuyo nombre ya es el de una columna se nombra por su id de campo (`customfield_10099`), de modo que todo campo puede elegirse. Un campo de usuario contiene el id de cuenta; su nombre para mostrar es un campo aparte (`assignee.displayName`, columna `assignee_name`). Las columnas que escribe el propio Jira son de solo lectura: `key`, `id`, `created`, `updated`, el nombre para mostrar de un campo de usuario y un campo de una forma que el adaptador no conoce, que sirve como texto JSON. Al registrarla se marca con «Read only: Jira sets it» en su descripción cuando no tiene ninguna, y el servidor rechaza una escritura que la establezca, nombrándola.

La tabla de un formulario nombra su proyecto y su tipo de incidencia en sus `arguments` (`project`, `issue_type`), y cada columna lee el campo que nombra su `path` (un id de campo o el nombre de un campo), o el campo de su mismo nombre. Una tabla registrada desde el admin obtiene ambos de Jira; una configuración los declara. Una tabla Jira que fije cualquier otra cosa, o que carezca de alguno, se rechaza, nombrándola.

| Campo fuente | Propiedad del adaptador | Notas |
| --- | --- | --- |
| `base_url` o `host` | `baseUrl` | Dirección `https://…` completa del sitio (p. ej. `https://acme.atlassian.net`). Un host simple u otro esquema se rechaza |
| `username` | `email` | Correo electrónico de la cuenta de Atlassian |
| `password` | `apiToken` | Un token de API de esa cuenta, creado en id.atlassian.com en Security, API tokens |

**Vínculos.** La tabla `issue_links` de la fuente contiene los vínculos entre las incidencias de sus formularios: `link_id`, `link_type`, `outward_key` e `inward_key`, cada vínculo una vez. A través de ella se declara como junction una relación entre dos formularios, con `link_type` como discriminador, de modo que GraphQL y Cypher recorren de una incidencia a la incidencia vinculada por tipo de vínculo.

**Lecturas.** Una búsqueda por clave (`key =`, `key IN`) se obtiene por clave, de modo que una clave que no nombra ninguna incidencia no es una fila. Una comparación de fecha o de número se envía a Jira como JQL. Todo otro filtro, y toda ordenación, se aplica a las filas leídas: una igualdad sobre texto, una opción, un estado o un usuario no se envía, ya que JQL rechaza un valor que no nombra nada donde SQL devuelve cero filas. Un campo multivalor (etiquetas, selección múltiple, componentes, versiones) es texto que contiene un array JSON, y se escribe de la misma manera.

En DuckDB, PostgreSQL y ClickHouse el motor adjunta el servidor pgwire de la fuente y la lee en su sitio. En Trino la fuente no tiene catálogo propio y se carga a través de su servidor.

**Ciclo de vida del servidor.** El servidor sirve solo los formularios registrados desde él, junto con su lista de formularios, su lista de campos y los vínculos. Registrar, cambiar o eliminar un formulario reinicia el servidor y vuelve a aprovisionar la fuente en el motor.

**Escrituras.** `INSERT`, `UPDATE` y `DELETE` sobre un formulario son mutaciones, escritas en el servidor pgwire de la fuente en todos los motores. Las filas se insertan en lotes de 50; una actualización o eliminación se hace incidencia por incidencia, por clave. Establecer `status` ejecuta la transición de flujo de trabajo a ese estado, y se rechaza, nombrando los estados alcanzables, cuando el flujo de trabajo de la incidencia no tiene ninguno. Una incidencia con subtareas no se elimina. El servidor devuelve cada fila escrita por su clave, por lo que se acepta `RETURNING` y GraphQL ofrece campos de mutación. En `issue_links`, `INSERT` crea un vínculo y `DELETE` elimina uno; `UPDATE` se rechaza.

```yaml
- id: support-jira
  type: jira
  base_url: https://acme.atlassian.net
  username: ${env:JIRA_EMAIL}
  password: ${env:JIRA_API_TOKEN}
tables:
- source_id: support-jira
  table: incident
  arguments: {project: KAN, issue_type: Incident}
  columns:
  - {name: key, data_type: varchar, is_primary_key: true}
  - {name: summary, data_type: varchar}
  - {name: status, data_type: varchar}
  - {name: priority, data_type: varchar}
  - {name: flagged, data_type: varchar, path: customfield_10021}
```

#### `cloudops`

El inventario de nube presenta los recursos de Azure, AWS y GCP como un único conjunto de tablas: `compute_resources`, `storage_resources`, `kubernetes_clusters`, `database_resources`, `network_resources`, `iam_resources`, `container_registries` y `compute_security_groups`, que vincula un recurso de cómputo con sus grupos de seguridad. Cada tabla tiene un único conjunto de columnas en las tres nubes, y su columna `cloud_provider` indica la nube de la que procede cada fila. La fuente solo lee, y sus tablas las registra un steward como cualquier otra.

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

Los resultados de búsqueda de Splunk son consultables como tablas (por ejemplo, `internal_server`). La URL del conector proviene de `base_url`, o se construye como `https://{host}:{port}` con un puerto predeterminado de `8089`. Autenticación: cuando `mapping.use_token` es `true` (el predeterminado), `password` se pasa como el token de la API; cuando es `false`, `username` y `password` se pasan como credenciales separadas.

| Campo de origen | Propiedad del conector | Notas |
| --- | --- | --- |
| `base_url` / `host` + `port` | `url` | `base_url`, o en su defecto `https://host:port` (puerto predeterminado 8089) |
| `password` | `token` o `password` | token cuando `use_token: true` |
| `username` | `user` | solo cuando `use_token: false` |
| `database` | `app` | restringe a una app de Splunk |
| `mapping.datamodel_filter` | `datamodel-filter` | filtra a un modelo de datos |
| `mapping.disable_ssl_validation` | `disable-ssl-validation` | para certificados autofirmados |

En la ruta de réplica pgwire (todos los motores salvo Trino) los mismos cuatro ajustes opcionales se convierten en las claves de operando de Calcite `model.json` `app`, `token`/`username`+`password`, `datamodelFilter` y `disableSslValidation`. Las dos últimas toman los tipos a los que `SplunkSchemaFactory` las convierte: una cadena y un booleano.

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

#### Google Workspace y Microsoft 365 {: #google-workspace-and-microsoft-365 }

`google_workspace` y `microsoft_365` leen el correo, los calendarios y las tareas de las cuentas que nombra un origen, y los llevan como réplicas. No son conectores de Calcite: cada uno tiene un cargador propio que llama a la API del proveedor, Gmail, Google Calendar y Google Tasks por un lado y Microsoft Graph por el otro. El formulario los lista en Enterprise como "Google Workspace (Gmail)" y "Microsoft 365 (Outlook, Exchange)".

**Tablas.** Ambos tipos producen las mismas tablas canónicas: los mismos nombres, tipos de columna y orden. Una unión de los dos no necesita conversión ni cambio de nombre; una columna para la que un proveedor no tiene valor es NULL. Las tablas se listan bajo el esquema `default`, y cada fila empieza con la cuenta de la que se leyó. Un origen ofrece las tablas de los recursos que lee:

| Recurso | Tablas | Escritas por sentencias |
| --- | --- | --- |
| `mail` | `messages`, `message_recipients`, `folders`, `message_folders`, `threads`, `attachments` | No |
| `calendar` | `calendars`, `events`, `event_attendees` | `events`, `event_attendees` |
| `tasks` | `task_lists`, `tasks` | `tasks` |

Las columnas se listan en [el esquema canónico de correo](arch/canonical-mail-schema.md). `events` contiene eventos únicos, maestros de serie con su recurrencia como líneas RFC 5545, y excepciones; nunca contiene las ocurrencias expandidas de una serie.

**Registro.** El administrador de una organización introduce primero el cliente de la organización para la plataforma en Admin → Email: un ID de cliente y un secreto, más el ID de directorio (tenant) o el dominio en Microsoft 365. Quien agrega un origen nunca lo ve ni lo escribe. Un origen toma entonces una de dos formas:

- **Un buzón.** El origen nombra una dirección en `accounts`. Su propietario hace clic en **Connect** en el formulario y aprueba el cliente de la organización. La aprobación resultante se guarda en el almacén de estado de la organización, no en los ajustes del origen. En Google Workspace esta forma declara además `sign_in: google_account`.
- **Los buzones de la organización.** El origen declara `mailboxes` como exactamente uno de `everyone: true`, `group: <address or id>` o `list: [addresses]`. No guarda ninguna credencial y nadie lo conecta. La organización lee con su propio acceso, y solo una vez que el administrador ha activado "Read the organization's mailboxes". En Google ese acceso es una clave de cuenta de servicio con delegación en todo el dominio, más una dirección de administrador para las llamadas al directorio. En Microsoft es la concesión de credenciales de cliente del registro de la aplicación.

Con la segunda forma, una tabla contiene las filas de todos los buzones. Un rol que puede leer la tabla lee todos los buzones, salvo que una regla de fila sobre la columna `account` lo limite.

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

**Ajustes.** Cada módulo rechaza por su nombre una clave que no lee.

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

Google rechaza una búsqueda bajo `gmail.metadata`, así que `mail_content: headers` con `mail_search` o `mail_since` se rechaza.

**Alcances.** Un inicio de sesión pide solo los alcances de los recursos marcados:

| Recurso | Google Workspace | Microsoft 365 (delegado) |
| --- | --- | --- |
| `mail` | `gmail.readonly`, o `gmail.metadata` solo para cabeceras | `Mail.Read` |
| `calendar` | `calendar.calendarlist.readonly`, `calendar.events` | `Calendars.ReadWrite` |
| `tasks` | `tasks` | `Tasks.ReadWrite` |
| todo origen | `userinfo.email` | `offline_access`, `User.Read` |

Para la forma de organización, Microsoft necesita los permisos de aplicación `Mail.Read` y `User.Read.All`, más `Calendars.ReadWrite` y `Tasks.ReadWrite.All` para calendarios y tareas. La cuenta de servicio de Google está autorizada para los alcances de `DELEGATED_SCOPES`, que incluyen también la lectura de usuarios y miembros de grupo del directorio.

**Actualización y frescura.** Los tipos de probe que admiten estos orígenes son `hash` y `none`, y su cargador aporta el token de cambio:

- **Gmail:** un digest del history id actual de cada buzón, una llamada a `users.getProfile` por buzón.
- **Correo de Microsoft:** cada enlace delta conservado, seguido para un elemento.
- **Calendarios y tareas de Google:** la etiqueta y la hora del último cambio de cada calendario; la etiqueta de cada lista de tareas, y el id y la hora de actualización de cada tarea.
- **Calendarios y tareas de Microsoft:** las líneas de cambio de cada buzón, durante `calendar_check_days` a cada lado de hoy.

Una comprobación que no puede hacerse pide una lectura; nunca informa "sin cambios". Las tablas de calendario y tareas se leen enteras en cada actualización. Las réplicas de correo de un origen Microsoft 365 o Google Workspace se mantienen en su sitio: cada parte de un buzón se escribe a medida que llega y puede consultarse de inmediato. Un origen Google Workspace que acota con `mail_search` se vuelve a leer entero en cada actualización, porque solo Google puede decir qué coincide con una búsqueda.

**Escrituras.** `INSERT`, `UPDATE` y `DELETE` sobre `events`, `event_attendees` y `tasks` van al proveedor por una sola vía de escritura para ambos tipos. Un `UPDATE` o `DELETE` nombra sus filas por `provider`, `account` e `id`; un asistente se nombra por `event_id` y `address`. Una ocurrencia de una serie se nombra por `series_id` y `original_start_at`. Una escritura que fija una columna fuera del conjunto escribible de la tabla se rechaza por su nombre. Google escribe con `sendUpdates=all` cuando `calendar_send_updates` está activado y con `sendUpdates=none` cuando está desactivado. Microsoft Graph no puede hacer ese cambio en silencio, así que se rechaza un cambio en los asistentes de una reunión, nombrando el ajuste.

**Exclusiones.** El correo es de solo lectura; ninguna sentencia cambia un buzón. `calendars`, `task_lists` y todas las tablas de correo no admiten escrituras. Spam y Papelera de Gmail se dejan fuera salvo que `mail_include_spam_trash` esté activado. La API Tasks de Google guarda solo el día en que vence una tarea y no tiene recordatorio.

### Orígenes de API {: #api-sources }
Registre cualquier endpoint HTTP como una tabla consultable.

| Tipo de API | Detección | Inferencia de columnas |
| --------- | ----------- | ----------------- |
| `openapi` | Análisis de especificación OpenAPI | Primitivos → nativo, objetos → JSONB |
| `graphql_remote` | Introspección de esquema | Primitivos → nativo, objetos → JSONB |
| `grpc_remote` | Reflexión del servidor | Primitivos → nativo, objetos → JSONB |

Las respuestas de la API se obtienen, se almacenan en caché en PostgreSQL (TTL configurable) y se exponen como tipos GraphQL. Las tablas en caché participan en consultas federadas como cualquier otro origen.

**Reglas de JSONB**: Las columnas complejas (objetos, arreglos) almacenadas como JSONB no son filtrables. El acceso a subcampos usa la extracción `->>` en SQL. Las relaciones se declaran entre tablas usando columnas de clave foránea escalares — las columnas de blob JSONB no son destinos de join. Use la promoción de JSONB para convertir campos anidados en columnas escalares nativas cuando se necesite filtrar o hacer join sobre ellos.

#### GitHub y GitLab {: #github-and-gitlab }

GitHub y GitLab son orígenes de marca que transporta `graphql_remote`: cada uno aparece en el formulario de Orígenes con su propio nombre y se registra como un origen GraphQL remoto que anota su marca. El formulario pide un token de acceso personal y nada más. El endpoint (`https://api.github.com/graphql`, `https://gitlab.com/api/graphql`) y el esquema se distribuyen con Provisa, por lo que el esquema no se introspecciona al registrarlo. El token se comprueba al agregar el origen. Agregar el origen no registra ninguna tabla: cada tabla que ofrece el esquema está disponible bajo el espacio de nombres del origen (`gh`, `gl`), y el steward registra las deseadas. Un campo que el token no puede leer se deja fuera de una tabla cuando esta se registra.

#### Stripe {: #stripe }

Stripe es un origen de marca que transporta `openapi`, construido como GitHub y GitLab sobre `graphql_remote`. Aparece en el formulario de Orígenes como Stripe, bajo Enterprise. Se registra como un origen OpenAPI cuya fila anota la marca en `federation_hints` y cuya ruta de especificación es `brand:stripe`.

**A qué llega.** A la API REST de Stripe, mediante la especificación OpenAPI publicada por Stripe (`stripe/openapi`, `spec3.json`). La especificación se incluye con Provisa, fijada y comprimida con gzip, y no se obtiene al registrar. La incluida es la versión de API `2026-09-30.endive` con 425 rutas. Cada llamada lleva `Stripe-Version` con esa versión, de modo que cada respuesta tiene la forma que describe la especificación, sea cual sea la versión por defecto de la cuenta. Los campos que Stripe devuelve y la especificación omite se añaden desde `stripe.additions.json`.

Como en cualquier origen OpenAPI, cada operación `GET` es una tabla ofrecida y cada otra operación es un comando ofrecido. Agregar el origen no registra ninguna: el steward registra las que quiere.

**Autenticación.** El formulario pide una sola cosa, un token de acceso (`BRAND_AUTH`: `token`). Use una clave de API secreta o restringida de Stripe. El token se envía como token bearer y se comprueba una vez con `GET /v1/balance` antes de agregar el origen. Un token rechazado hace fallar el registro con `openapi.credential_rejected`; uno ausente, con `openapi.credential_required`.

**Actualización y frescura.** Una tabla de Stripe es una réplica, como toda tabla OpenAPI. Sus respuestas en caché duran el `cache_ttl` del origen, que una solicitud de registro fija en 300 segundos salvo que diga otra cosa. Se aplican los tipos de probe de OpenAPI. Un preset puede nombrar el parámetro que una actualización usa para obtener solo lo que cambió. La marca Stripe no nombra ninguno, así que cada actualización lee sus tablas enteras.

**Exclusiones.** La integración de pagos obsoleta de tokens y Sources de Stripe queda fuera de la especificación incluida. La lista está en [Exclusiones en un origen construido desde una especificación](#exclusions-on-a-specification-built-source). Las `exclusions` propias de un operador se suman a las de la marca.

`stripe` aparece también en `SourceType` entre los tipos `wrappers` de Supabase con stub. Un origen Stripe agregado por el formulario es un origen `openapi`, no ese stub.

#### Exclusiones en un origen construido desde una especificación {: #exclusions-on-a-specification-built-source }

Una especificación OpenAPI publicada describe todo lo que acepta la API remota, incluido lo que su proveedor ha marcado como obsoleto y lo que un despliegue nunca debe enviar. Un origen OpenAPI puede llevar exclusiones: cosas de la especificación que el origen no ofrece.

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

Datos abiertos del gobierno de EE. UU. El acceso está particionado por agrupación temática.

A un origen se le dan uno o más temas, y cada tema aporta los esquemas del adaptador que se listan abajo. Un esquema puede pertenecer a más de un tema (`census`). Los esquemas `ref` y `geo` son esquemas de enlace: datos de referencia y geografía a través de los cuales se unen las tablas de todos los temas, por lo que todo origen los sirve sean cuales sean los temas que se le den. La lista se declara una vez en el servidor y una prueba la mantiene igual a los esquemas que sirve el paquete del adaptador fijado.

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

Un origen AskAmerica creado en la administración recibe una credencial, la clave de API. Provisa presenta la clave a la API de AskAmerica y recibe las credenciales de almacenamiento de corta duración y solo lectura con las que se leen los datos; la clave se rechaza allí por su nombre si es desconocida o está revocada.

El origen es entonces un origen de protocolo Postgres como los de SharePoint, Salesforce e inventario de nube: Provisa inicia para él el servidor `pgwire-govdata` incluido, con la clave y esas credenciales en el entorno del servidor, y cada motor llega a ese servidor como llega al de un hermano. DuckDB, Postgres y ClickHouse lo adjuntan y leen sus tablas en su lugar, de modo que una tabla de AskAmerica se une con tablas de otros orígenes, queda bajo vistas y se entrega como cualquier otra. Trino no tiene conector para él y lee una réplica creada a través del servidor: una copia de la tabla guardada en el almacén de materialización, tan fresca como su última actualización bajo los ajustes de caché y replicación de la tabla, en lugar de leerse en el momento de la consulta. Una tabla se lee del esquema del adaptador que nombra (`sec`, `econ`, etc.).

El servidor monta sus esquemas desde el almacenamiento de objetos antes de aceptar una conexión, lo que tarda minutos en un primer arranque. Se inicia cuando el origen se guarda; hasta que está listo, el formulario de Register Table informa de que el origen aún se está iniciando.

### Dónde vive la contraseña de un origen {: #where-a-sources-password-lives }

La contraseña de un origen nunca se almacena junto al resto de sus ajustes de conexión. La fila `sources` del plano de control lleva una columna `password_ref` que contiene lo que se introdujo para la contraseña: el valor mismo, o una referencia como `${env:PG_PASSWORD}` o `${secret:SNOWFLAKE_KEY}`. Se resuelve en el momento en que se marca el origen, dentro de la organización bajo la que se ejecuta la solicitud.

Un valor sin `${…}` se usa tal cual. `${env:VAR}` lee el entorno de proceso del despliegue y no necesita nada vinculado. `${secret:NAME}` nombra un secreto que posee una organización, de modo que solo se resuelve dentro de las propias operaciones de esa organización: las costuras de introspección de administración y la terminal de consulta a las que llega cada superficie establecen ese vínculo.

El campo guarda lo que usted escribe en él, sea cual sea la forma en que se registró el origen:

- **Desde la configuración.** El archivo es el registro: Provisa copia el valor o la referencia en `password_ref` de forma literal.
- **Desde el formulario de Orígenes.** Lo que se escribe en el campo de contraseña se almacena de forma literal, sea un valor o una referencia. Provisa no escribe nada en la bóveda de la organización por ello ni inventa ningún nombre de secreto. Para mantener una credencial fuera de la fila del plano de control, cree usted mismo el secreto (véase [Secretos](secrets.md)) y escriba `${secret:NAME}` en el campo.

Un valor escrito como valor se almacena en la base de datos del plano de control tal como se escribió. Volver a escribir la contraseña reemplaza el texto del campo. Eliminar el origen quita su fila y nada de la bóveda: un secreto que usted creó es suyo.

`password_ref` no viaja entre entornos. Una rama o un entorno copiado suministra sus propios valores de conexión, y la bóveda que nombra una referencia pertenece a cualquiera que sea el entorno que la suministró.

### Perfilador de datos {: #data-profiler }

Un perfilador de datos es un origen que guarda un nombre, un horario y valores predeterminados de ejecución, y nada más. Nunca se introspecciona. Se le agregan tablas de otros orígenes, y cada ejecución las mide y escribe sus filas en el plano de control de la organización, donde se leen como las tablas de cualquier otro origen. Para crear un perfilador y agregarle tablas, véase [Datos de prueba](test-data.md).

### Verificadores de calidad de datos {: #data-quality-checkers }

Un verificador de calidad de datos es simplemente otro tipo de origen. Su salida de escaneo son datos: cada resultado de comprobación es una observación, así que se aterriza por la ruta normal de los orígenes y recibe lo mismo que cualquier otro origen, desde cadencia, frescura, eventos y linaje hasta gobernanza, RLS, la cuadrícula y la exportación.

Se admiten dos verificadores, y elegir entre ellos es tanto una cuestión de licencia como de funciones.

| Tipo de origen | Dialecto del contrato | Extra | Licencia | Plano cloud alojado |
| ------------ | ----------------- | ------- | --------- | -------------------- |
| `soda` | YAML de contrato Soda | `pip install .[soda]` (`soda-postgres`) | Elastic License 2.0 | Rechazado — ver más abajo |
| `great_expectations` | JSON de suite de expectativas | `pip install .[gx]` (`great-expectations[postgresql]`) | Apache 2.0 | Permitido |

La Elastic License 2.0 prohíbe ofrecer el software a terceros como servicio alojado o gestionado, y ejecutar Soda dentro del plano SaaS en nombre de un inquilino sería exactamente eso. `config/capabilities.yaml` lo registra como `cloud_eligible: false` en la opción `soda`, y el plano alojado lee ese indicador. Un despliegue alojado que quiera Soda apunta a un endpoint de Soda que el propio operador ejecuta.

Provisa no incluye ni enlaza nada. El escaneo se ejecuta en un intérprete hijo (`python -m provisa.dq.worker`), el único lugar donde se importa `soda_core` o `great_expectations`. Por eso un verificador de código disponible nunca llega al proceso del servidor, y si un verificador falla, cae un subproceso y el bucle de eventos sigue intacto.

**El origen apunta al propio endpoint pgwire de Provisa.** Así un solo driver de postgres puede comprobar una tabla respaldada por Snowflake o Iceberg: el verificador escanea la vista federada, nunca el sistema subyacente. La política se aplica a esa conexión, así que la identidad del escaneo se declara de forma explícita. Si se heredara, un conjunto de filas filtrado podría hacer que una comprobación pasara sin que nadie lo notara.

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

**Una tabla de resultados por contrato, y el contrato es todo el registro.** La tabla lleva `dq_contract`, el texto del contrato palabra por palabra, y nada más sobre su forma. Las columnas, la marca de agua y las promociones se derivan de él.

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

El registro deriva cuatro cosas de ese texto:

- **Linaje.** El contrato ya nombra su conjunto de datos destino, así que el registro lo analiza como `extract_inputs` analiza SQL y lo resuelve a la tabla gobernada. Hay una sola definición y ninguna segunda copia que pueda divergir. Un contrato que nombra un conjunto de datos no gobernado falla de forma visible en el registro, de modo que nunca aterriza filas que nadie pidió.
- **Columnas.** El sobre de resultados pertenece al verificador: 16 columnas incluidas, de `scan_id` a `diagnostics`. De las columnas declaradas solo se lee su `visible_to`, que debe ser igual en todas, y luego se reemplazan.
- **Marca de agua.** `scan_time` se convierte en la marca de agua, lo que hace del aterrizaje un append. El historial de escaneos crece sin un subsistema de historial.
- **Promociones.** `freshness_max_timestamp` y `dataset_rows_tested` se promueven desde el jsonb `diagnostics` como columnas con tipo. Puede añadir más como en cualquier otra columna jsonb.

El calendario no necesita campos nuevos. `change_signal` más `cache_ttl` fijan cada cuánto se consulta, `mv_debounce_quiet` y `mv_debounce_max_delay` agrupan una ráfaga de cambios previos en un solo escaneo, un grano de calendario lo hace periódico y `expected_events` retiene el escaneo hasta que sus entradas estén frescas en toda la ventana. El bucle de sondeo es el planificador de escaneos.

`outcome` es uno de `pass`, `fail`, `warn`, `error` o `skipped`. Ninguno impone nada por sí solo. Si quiere imponer algo, declárelo aparte: un preflight o una MV sobre los resultados aterrizados. Una observación aterrizada no tiene que ser determinista, así que aquí caben comprobaciones que nunca podrían controlar un preflight, como una puntuación de anomalía, un cambio en una ventana móvil o la frescura medida respecto a ahora.

El contrato se escribe en la UI, en el panel de calidad de datos de la página de edición de la tabla, y el texto del contrato que hay ahí es siempre la fuente de verdad. Una ejecución de prueba aplica el contrato a la tabla en vivo y muestra los resultados sin aterrizarlos. Úsela para detectar un contrato cuyo nombre de conjunto de datos se resolvió en un sitio inesperado y que, de otro modo, solo aterrizaría filas que pasan.

---

## Conectores personalizados {: #custom-connectors }

Un operador puede dar a los motores de federación nativos (Postgres, DuckDB y ClickHouse) alcance a un nuevo tipo de origen declarando un conector para él en `config/custom_connectors.yaml`. No hace falta código.

Así los motores nativos, que funcionan sin clúster, obtienen la misma extensibilidad por configuración que el motor Trino tiene en su propia capa. Allí, un conector JDBC genérico se parametriza por tipo de origen, cada tipo recibe un cuerpo `.properties` de catálogo, y Provisa añade sus propios plugins de conector de Trino (Splunk, SharePoint, Calcite).

El archivo viene vacío. Los conectores incorporados cubren lo que funciona de serie, y todo lo que hay en este archivo lo escribe el operador. Para usar otra ruta, por ejemplo en pruebas, defina `PROVISA_CUSTOM_CONNECTORS`.

### Tipos de descriptor {: #descriptor-kinds }
| Motor | Tipo | Mecanismo | Qué proporciona el descriptor |
| --- | --- | --- | --- |
| `postgres` | `pg_fdw` | SQL/MED (estándar ISO) | `extension`, `server_options`, `user_mapping`, `supports_import`, `table_options`, `remote_schema` |
| `duckdb` | `duckdb_attach` | INSTALL/LOAD + ATTACH | `extension`, `probe_symbol`, `attach_template`, `remote_schema` |
| `duckdb` | `duckdb_scan` | INSTALL/LOAD + vista de escaneo | `extension`, `probe_symbol`, `scan_template` |
| `clickhouse` | `clickhouse_database` | `CREATE DATABASE ENGINE=…` (expone automáticamente cada tabla remota) | `ch_engine`, `engine_template` |
| `clickhouse` | `clickhouse_table` | `CREATE TABLE ENGINE=…` por tabla (columnas desde el registro) | `ch_engine`, `engine_template` (puede llevar `{table}`) |
| `clickhouse` | `clickhouse_scan` | `CREATE TABLE ENGINE=…`, ClickHouse infiere el esquema | `ch_engine`, `engine_template` |

**Postgres es genérico.** SQL/MED es un estándar ISO, por lo que todo FDW conforme comparte la misma forma de DDL: `CREATE SERVER … FOREIGN DATA WRAPPER <fdw> OPTIONS(…)`, opcionalmente `CREATE USER MAPPING`, y luego `IMPORT FOREIGN SCHEMA` (cuando `supports_import: true`) o una `CREATE FOREIGN TABLE` explícita por tabla (cuando es `false`). Un descriptor `pg_fdw` solo proporciona la variación específica del FDW — nombre de la extensión, claves de opciones del servidor, claves de mapeo de usuario, indicador de importación, opciones de tabla. Cualquier FDW conforme al estándar es, por lo tanto, manejable solo desde la configuración.

**DuckDB soporta dos mecanismos.** Una extensión que expone un catálogo mediante ATTACH usa `duckdb_attach`; una que expone una función de tabla de lectura usa `duckdb_scan`. Una extensión que no encaja en ninguno de los dos patrones no es compatible.

**ClickHouse soporta tres mecanismos**, uno por cada forma de motor de integración: un motor DATABASE relacional que expone automáticamente cada tabla remota (`clickhouse_database`, por ejemplo Redis/MySQL), un motor por tabla cuyas columnas provee el registro (`clickhouse_table`, por ejemplo el puente JDBC/ODBC — el `engine_template` puede llevar un marcador `{table}` que el runtime vincula), y un motor de archivo/lake/URL cuyo esquema infiere ClickHouse (`clickhouse_scan`, por ejemplo HDFS/URL). SQLite (motor DATABASE, archivo, sin servidor) y Hudi (lakehouse, sin copia) vienen listos para usar.

Un valor `kind` desconocido falla de forma visible al arrancar, así que una errata en un descriptor nunca deja un tipo de origen inalcanzable sin avisar.
### Verificación de disponibilidad {: #probe-gating }
La disponibilidad se verifica en el momento del attach contra el catálogo de detección estándar de cada motor:

- **Postgres** — verifica `pg_extension`, luego `pg_available_extensions`.
- **DuckDB** — ejecuta `INSTALL`/`LOAD` y verifica `duckdb_functions()` para el `probe_symbol` declarado.
- **ClickHouse** — verifica `system.table_engines` para el `ch_engine` declarado; su ausencia en el build falla de forma ruidosa.

Una extensión declarada que no se puede instalar falla de forma ruidosa. Sin omisión silenciosa, sin valor de respaldo. Un conector cuya verificación falla simplemente no está activo para ese despliegue.

### Variables de plantilla {: #template-variables }
Todo valor de `server_options`, valor de `user_mapping`, `attach_template` y `scan_template` puede usar marcadores `{field}`. Campos disponibles:

`{id}`, `{host}`, `{port}`, `{database}`, `{username}`, `{password}`, `{path}`, `{schema_name}`, `{table_name}`, más cualquier clave de `federation_hints`. Las plantillas de attach de DuckDB reciben además `{alias}`, el alias de catálogo interno que Provisa da a la base de datos adjunta.

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

Con cualquiera de los dos descriptores en su lugar, registrar un origen con el `source_type` declarado lo enruta a través del conector personalizado, sujeto a una verificación exitosa. No se necesita ningún otro cambio de configuración.

---

## Almacenes de datos como orígenes con nombre {: #warehouses-as-named-sources }
Snowflake, Databricks y ClickHouse se pueden registrar como orígenes con nombre independientemente de cuál motor de federación esté activo.

Una vez registrado, el almacén de datos se lee a través del DirectDriver del origen, que crea una réplica en el almacén de materialización del motor activo, y la consulta se ejecuta contra esa réplica. En la ruta directa habitual (asyncpg, aiomysql) el motor se salta por completo. Aquí el motor sigue ejecutando la consulta, pero contra una réplica local en lugar de ir por la red al almacén de datos en cada petición.

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

Los datos de objetos en la nube y de lago (archivos parquet, csv, iceberg y delta_lake en S3, GCS o R2) son un tipo de origen aparte. Se adjuntan en su sitio cuando el motor activo tiene un conector ATTACH para ese tipo, así que no se crea ninguna réplica y el motor escanea el almacenamiento de objetos directamente. Las credenciales de estos orígenes también van en `federation_hints`:

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
Todos los orígenes comparten un conjunto común de campos.

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
| `pool_min` | No | `1` | Tamaño mínimo del pool de conexiones |
| `pool_max` | No | `5` | Tamaño máximo del pool de conexiones |
| `use_pgbouncer` | No | `false` | Enruta las conexiones a través de PgBouncer |
| `pgbouncer_port` | No | `6432` | Puerto de PgBouncer |
| `cache_enabled` | No | `true` | Habilita el almacenamiento en caché de respuestas de API |
| `cache_ttl` | No | `null` | TTL de la caché en segundos; hereda el predeterminado global cuando es null |
| `cache_catalog` | No | `null` | Catálogo federado para la caché de API; por defecto usa el catálogo propio del origen |
| `cache_schema` | No | `api_cache` | Esquema dentro del catálogo de caché |
| `naming_convention` | No | `null` | Anula la convención de nombres global para este origen |
| `federation_hints` | No | `{}` | Propiedades de sesión pasadas al motor de federación, y parámetros de conexión extendidos para orígenes de almacén de datos |
| `mapping` | No | `{}` | Configuración de conector específica del tipo para orígenes NoSQL y SaaS (por ejemplo, `auth_type` de SharePoint, `auth_type` de Salesforce, `use_token` de Splunk) |
| `allowed_domains` | No | `[]` | Restringe el origen a dominios específicos; vacío = sin restricción |
| `description` | No | `""` | Descripción legible por humanos |

---

## Orígenes Kafka {: #kafka-sources }
Los tópicos de Kafka se configuran por separado bajo `kafka_sources`, indexados por el `id` de origen de un origen `kafka` registrado.

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
| `topics[].default_window` | Ventana de tiempo predeterminada para consultas con ventana (por ejemplo, `1h`) |
| `topics[].columns` | Definiciones de columna para el esquema del tópico |

---

## Visibilidad de columnas {: #column-visibility }
El campo `visible_to` en cada columna es una lista de ID de rol que pueden ver esa columna.

```yaml
columns:

  - name: email
    visible_to: [admin]        # only admin role sees this column

  - name: region
    visible_to: [admin, analyst]  # both roles see this column
```

Las columnas omitidas de la lista `visible_to` de un rol no aparecen en el esquema GraphQL de ese rol y no se pueden consultar ni referenciar en filtros.

---

## Relaciones {: #relationships }
Las relaciones conectan dos tablas registradas y aparecen como campos anidados en GraphQL.

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
| `cardinality` | Sí | `many-to-one` o `one-to-many` |
| `materialize` | No | Crea automáticamente una vista materializada para joins entre orígenes. En una arista respaldada por una tabla de unión, la vista cubre el recorrido de dos saltos, no un join directo |
| `refresh_interval` | No | Intervalo de actualización de la vista materializada en segundos (predeterminado: 300) |
| `target_function_name` | No | Nombre de función de base de datos para relaciones calculadas |
| `function_arg` | No | Qué argumento de la función recibe el valor de la columna de origen |
| `alias` | No | Tipo de relación legible por humanos (por ejemplo, `WORKS_FOR`) |
| `graphql_alias` | No | Nombra el campo SDL que esta relación expone en el tipo padre. Cuando está ausente, el nombre se deriva del `field_name` de la tabla destino y la cardinalidad de la relación. |
| `disable_cypher` | No | Cuando es `true`, excluye esta relación de las aristas del grafo Cypher |
| `source_json_key` | No | Extrae esta clave de la columna de origen como un objeto JSON antes del JOIN |
| `via_table` | No | Nombre de tabla registrada de la tabla de unión que recorre esta arista. Fijarlo hace que la arista sea respaldada por una tabla de unión; dejarlo vacío la deja como arista de clave foránea |
| `via_source_column` | No | Columna de la tabla de unión que se empareja con `source_column`. Separada por comas y posicional para una clave compuesta |
| `via_target_column` | No | Columna de la tabla de unión que se empareja con `target_column` |
| `via_type_column` | No | Columna discriminadora, cuando una misma tabla de unión lleva varios tipos de relación |
| `via_type_value` | No | El valor de discriminador al que queda fijada esta arista |
| `via_label_source` | No | Qué designación nombra el tipo Cypher: `column` (el valor del discriminador), `table` (el nombre de la tabla de unión) o `fixed` (el alias declarado). Todas se pasan a UPPER_SNAKE_CASE |

### Relaciones respaldadas por una tabla de unión {: #junction-backed-relationships }

Puede declarar una tabla asociativa como relación de Cypher por derecho propio, en lugar de como nodo. Sus columnas pasan a ser los atributos de la relación:

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

La tabla de unión es una tabla registrada como cualquier otra, y tiene que estar registrada antes de que una relación pueda nombrarla. Declare una relación por cada valor del discriminador. Tres filas sobre `pet_companions` dan `BONDED_PAIR`, `LITTERMATE` y `SHARES_ENCLOSURE` como tres tipos de Cypher distintos, cada uno con las demás columnas de la fila de unión como propiedades de arista. La configuración de demostración incluida declara exactamente esto.

Una arista de unión solo existe en Cypher. El emisor de joins de GraphQL construye su cláusula `ON` para un único par de columnas y no tiene dónde poner el segundo salto, así que las aristas de unión quedan fuera del SDL generado y de `pg_constraint`. La tabla de unión sigue pudiéndose consultar como su propio campo raíz. Se retira del lado de nodos del esquema de grafo de Cypher, así que nunca aparece como etiqueta de nodo.

`materialize: true` también funciona sobre una arista de unión. Lo que materializa es el recorrido completo: la vista contiene el salto de origen, el salto de unión, el discriminador y las columnas propias de la unión junto a las del destino. La unión es una tercera pata del join, así que si la arista cruza orígenes se decide mirando las tres tablas. Una tabla de unión que está en un origen distinto del de las dos tablas que enlaza se materializa aunque esas dos compartan origen. Cada declaración materializa un tipo de arista, así que una vista construida para `bonded pair` nunca responde a un recorrido `littermate`.

Valores de cardinalidad:

- `many-to-one`: cada fila de origen corresponde a una fila de destino (FK a PK)
- `one-to-many`: cada fila de origen corresponde a varias filas de destino (lo inverso)

---

## Reglas de seguridad de nivel de fila {: #row-level-security-rules }
Las reglas RLS inyectan cláusulas `WHERE` en el momento de la consulta, con alcance a un rol y opcionalmente a una tabla o dominio.

```yaml
rls_rules:

  - table_id: orders          # applies to orders table only
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"
    description: Analysts read only their own region's orders.

  - domain_id: sales-analytics  # applies to every table in domain
    role_id: analyst
    filter: "tenant_id = current_setting('provisa.tenant_id')"
    description: Analysts read only their own tenant's rows.
```

Cuando existen tanto una regla de nivel de dominio como una de nivel de tabla para el mismo rol, la regla de nivel de tabla tiene prioridad.

| Campo | Obligatorio | Descripción |
| ------- | ---------- | ------------- |
| `table_id` | Condicional | Tabla a la que se aplica la regla; mutuamente excluyente con `domain_id` |
| `domain_id` | Condicional | Dominio al que se aplica la regla; se aplica a todas las tablas del dominio |
| `role_id` | Sí | Rol al que se aplica esta regla |
| `filter` | Sí | Predicado SQL inyectado en `WHERE`; puede referenciar variables de sesión |
| `description` | Sí | Para qué sirve la regla, tal como la lee una revisión de acceso |

---

## Funciones y webhooks {: #functions-and-webhooks }
### Funciones de base de datos {: #db-functions }
Registra una función de base de datos y la expone como consulta o mutación GraphQL.

Los orígenes de base de datos también pueden autodescubrir sus procedimientos almacenados y funciones a partir del catálogo del proveedor (`pg_proc`, `information_schema.routines`, o equivalentes del proveedor), eliminando la necesidad de registrar cada uno manualmente. La detección lee `prokind` y `provolatile`: las funciones inmutables/estables se registran como relaciones parametrizadas (los argumentos del procedimiento se convierten en parámetros de consulta, con la misma forma que las tablas OpenAPI GET), y los procedimientos volátiles se registran como mutaciones/funciones rastreadas. Las rutinas descubiertas pasan por el gobierno de Etapa 2 de forma idéntica a las registradas manualmente.

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
| `returns` | Sí | — | ID de la tabla registrada que devuelve la función |
| `arguments` | No | `[]` | Lista de definiciones de argumento `{name, type}` |
| `visible_to` | No | `[]` | Roles a los que está asignada la función (vacío: todos los roles). Una `mutation` además necesita el derecho `write` |
| `domain_id` | No | `""` | Dominio al que pertenece esta función |
| `description` | No | `null` | Descripción del campo GraphQL |
| `kind` | No | `mutation` | `"query"` o `"mutation"` |

### Webhooks

Expone un endpoint HTTP externo como consulta o mutación GraphQL.

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
| `inline_return_type` | No | `[]` | Lista de campos `{name, type}` para formas de retorno personalizadas |
| `arguments` | No | `[]` | Lista de definiciones de argumento `{name, type}` |
| `visible_to` | No | `[]` | Roles que pueden llamar a este webhook |
| `domain_id` | No | `""` | Dominio al que pertenece este webhook |
| `description` | No | `null` | Descripción del campo GraphQL |
| `kind` | No | `mutation` | `"query"` o `"mutation"` |

---

## Autenticación {: #authentication }
La autenticación se configura bajo la clave `auth`.

| Proveedor | Descripción |
| ---------- | ------------- |
| `none` | Sin autenticación; todas las solicitudes se tratan como el `default_role` |
| `firebase` | Firebase Authentication; requiere `project_id` y `service_account_key` |
| `keycloak` | Keycloak OIDC |
| `oauth` | OAuth 2.0 genérico |
| `simple` | Usuario/contraseña sin proveedor externo |

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

`assignments_source: claims` lee las asignaciones de rol desde los claims del JWT. `assignments_source: provisa` las lee desde el almacén de asignaciones propio de Provisa.

---

## Enrutamiento de ejecución {: #execution-routing }

**Ejecución directa.** Una consulta que toca un solo origen relacional va directa al driver propio de ese origen, en menos de 100 ms. Un tipo de origen toma este camino cuando Provisa conoce tanto su dialecto SQL como su conector.

**Ejecución federada.** Una consulta entre orígenes, o sobre un origen sin driver directo, pasa por el motor de federación. Provisa incluye uno embebido; para despliegues grandes, apúntelo a su propio clúster compatible.

**Estadísticas.** Cuando se registra una tabla, Provisa ejecuta `ANALYZE` sobre cada tabla publicada para que el optimizador basado en costes tenga recuentos de filas, fracciones de nulos, valores distintos y mín/máx. Si falla, el fallo se registra en el log y el registro sigue adelante.

---

## Orígenes de grafo y semántica {: #graph-semantic-sources }
### Neo4j

Registre una base de datos de grafos Neo4j como un origen consultable. Los stewards escriben consultas Cypher que proyectan valores escalares; Provisa almacena en caché los resultados y los expone como tipos GraphQL.

Las consultas Cypher deben usar accesos a propiedades en la cláusula `RETURN` (`RETURN n.id AS id, n.name AS name`). El registro rechaza una consulta que devuelve objetos nodo.
#### Registro por archivo de configuración {: #config-file-registration }

Declare un origen `neo4j` y sus tablas en YAML. Cada tabla requiere un `query_template` (el Cypher que produce sus filas) y columnas tipadas. La clave `query_template` no es válida bajo ningún otro tipo de origen.

Un origen requiere `host`, `port` y `database`. Las filas se obtienen enviando un POST con `{"statements": [{"statement": <cypher>}]}` a `/db/<database>/tx/commit` (la API de transacciones HTTP de Neo4j). Una respuesta con una lista `errors` no vacía se trata como una consulta fallida, no como un resultado vacío.

Cada columna requiere `data_type`. El cargador convierte los tipos de configuración al tipo de columna de la API usado en el momento de la consulta:

| `data_type` de configuración | Tipo de la API |
|---|---|
| `varchar`, `text`, `string`, `char` | string |
| `integer`, `int`, `bigint`, `smallint` | integer |
| `float`, `double`, `real`, `decimal`, `numeric`, `number` | number |
| `boolean`, `bool` | boolean |
| `json`, `jsonb` | jsonb |

Se aceptan `varchar(N)` y `decimal(10,2)`, y se usa el tipo base anterior al paréntesis.

El registro persiste una fila `api_sources` y una fila `api_endpoints` por tabla, de modo que las tablas sobreviven a un reinicio sin volver a leer el archivo. Los endpoints REST de administración bajo `/admin/sources/neo4j` escriben las mismas filas.

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

#### Register Table en la UI {: #register-table-in-the-ui }

Un origen neo4j no tiene tablas que listar, así que el formulario Register Table pide la tabla en lugar de ofrecer una.

1. Elija el origen neo4j y un dominio. Los selectores de esquema y tabla, la casilla de descubrimiento y el selector de marca de agua no aparecen; el origen nunca se introspecciona.
2. Escriba un nombre de tabla y el Cypher. El Cypher debe proyectar escalares (`RETURN a.name AS name`); una proyección que devuelva un nodo o una lista se reporta como error.
3. Pulse Preview. El formulario ejecuta el Cypher con `LIMIT 5` a través de la consulta GraphQL `neo4jPreview` y completa la lista de columnas a partir de las filas que regresan, tipadas como `text`, `integer`, `double`, `boolean` o `json`. Una vista previa fallida deja el Cypher en el editor y muestra el mensaje.
4. Ajuste visibilidad, alias o enmascaramiento como para cualquier tabla y luego registre. El formulario se niega a enviar hasta que una vista previa haya tipado las columnas, y el servidor rechaza una tabla neo4j que no lleve Cypher (`schema.neo4j_query_required`).

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

El endpoint de vista previa (`POST /admin/sources/neo4j/{id}/preview`) devuelve filas de muestra y bloquea el registro si el Cypher devuelve objetos de nodo.

### SPARQL

Registre cualquier triplestore compatible con SPARQL 1.1 (Apache Jena Fuseki, Virtuoso, Stardog, etc.) como un origen consultable.

Las consultas deben ser consultas `SELECT`. Los nombres de variable en la cláusula `SELECT` se convierten automáticamente en nombres de columna.

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

Ambos conectores usan el pipeline de caché de origen de API — los resultados se almacenan en PostgreSQL con TTL configurable, lo que los hace disponibles para JOIN federados entre orígenes.

#### Registro por archivo de configuración y por UI {: #config-file-and-ui-registration }

El `host` de un origen `sparql` es la URL de su endpoint SPARQL (el formulario de Orígenes lo almacena de la misma manera). Cada tabla bajo él lleva `query_template`, un SELECT cuyas variables son las columnas; cada binding es `text`.

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

Register Table funciona de la misma manera que para Neo4j: elija el origen, escriba un nombre de tabla y el SELECT, pulse Preview (la consulta `sparqlPreview` lo ejecuta con `LIMIT 5` y completa la lista de columnas), luego registre. El registro persiste una fila `api_sources` y una fila `api_endpoints` (POST codificado como formulario a la ruta del endpoint, normalizador `sparql_bindings`), las mismas filas que escribe un registro por archivo de configuración, y el motor nativo aterriza las filas a través de la misma cadena de obtención que Neo4j.

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

Las porciones de un solo origen se enrutan directamente. Los JOIN entre orígenes se federan con coerción de tipos automática.
