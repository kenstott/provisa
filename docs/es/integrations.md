# Integraciones {: #integrations }

## Elegir una vía de conexión {: #choosing-a-connection-path }

| Tipo de cliente | Vía recomendada | Por qué |
| ------------- | ----------------- | ----- |
| Herramientas de BI (Tableau, Power BI, Looker) | JDBC | Streaming columnar de Arrow Flight sobre el cable; las herramientas de BI tienen un asistente JDBC integrado y se benefician de la entrega columnar de alto rendimiento para conjuntos de resultados grandes |
| psql, DBeaver, cualquier herramienta compatible con PG | pgwire (driver PG nativo) | Opción predeterminada sin fricción — no se necesita un driver personalizado; use lo que ya tiene |
| Stack de datos de Python (pandas, pyarrow) | `provisa-client` o ADBC directo | Lotes de Arrow en streaming; sin sobrecarga de serialización por fila |
| Spark, DuckDB, pipelines de alto rendimiento | Arrow Flight (ADBC) | Streaming columnar sin límite directo a memoria Arrow |
| Servicio a servicio (contratos tipados) | Protobuf gRPC | Proto generado por rol; filas en streaming; seguridad de tipos |
| Aplicaciones web, scripting | HTTP (`/data/graphql`, `/data/sql`) | Sin driver; HTTP estándar; elección completa de lenguaje de consulta |
| Clientes REST (estándar JSON:API) | `GET /data/jsonapi/{table}` | Envoltorio JSON:API v1.0; conjuntos de campos dispersos, paginación, filtrado mediante parámetros de consulta; sin driver |

---

## pgwire — Driver nativo de PostgreSQL {: #pgwire-native-postgresql-driver }

Provisa implementa el protocolo de cable de PostgreSQL (versión de protocolo 3.0). Cualquier cliente que hable PostgreSQL se conecta sin un driver personalizado.

Actívelo estableciendo `PROVISA_PGWIRE_PORT` (por ejemplo, `5433`) antes de iniciar Provisa. Está deshabilitado cuando no se establece o es `0`.

### ¿Por qué pgwire en lugar de JDBC? {: #why-pgwire-instead-of-jdbc }

El driver JDBC usa Arrow Flight como transporte y requiere desplegar `provisa-jdbc.jar`. pgwire no requiere nada — si ya tiene `psql`, DBeaver, SQLAlchemy o un driver JDBC de PG, ya está listo. Es la vía de menor fricción para cargas de trabajo solo de SQL.

JDBC es la opción correcta para herramientas de BI que tienen un asistente de conexión JDBC integrado y se benefician del streaming columnar de Arrow Flight para conjuntos de resultados grandes. pgwire acepta SQL libre contra el esquema publicado completo — las mismas consultas, menor costo de configuración.

### psql

```bash
psql -h localhost -p 5433 -U alice
```

### DBeaver

1. Nueva conexión → PostgreSQL
2. Host: `localhost`, Puerto: `5433`
3. Nombre de usuario / contraseña según lo configurado en Provisa
4. No se requiere descargar ningún driver adicional

### SQLAlchemy (Python)

```python
from sqlalchemy import create_engine

engine = create_engine("postgresql+psycopg2://alice:secret@localhost:5433/provisa")
df = pd.read_sql("SELECT * FROM sales.orders", engine)
```

O con `asyncpg`:

```python
engine = create_engine("postgresql+asyncpg://alice:secret@localhost:5433/provisa")
```

### Autenticación {: #authentication }

El campo `password` del paquete de inicio transporta la credencial, y *qué* es la credencial determina el método: un token de acceso personal, un token bearer OIDC o una contraseña contra el proveedor configurado. Bajo el proveedor `basic` con `auth.scram: true`, la contraseña se demuestra mediante SCRAM-SHA-256 en lugar de enviarse. Se admiten certificados de cliente. En modo de confianza (`none`), el nombre de usuario se asigna directamente a un rol y la contraseña se ignora.

La tabla completa de interfaz × método está en el [Modelo de seguridad](security.md#surfaces-and-credentials). MD5 no es compatible; active TLS (`PROVISA_PGWIRE_CERT` / `PROVISA_PGWIRE_KEY`) cuando opere sobre una red no confiable.

### Limitaciones {: #limitations }

- Solo SQL. GraphQL y Cypher no se aceptan sobre pgwire.
- No es de solo lectura. Las filas de las tablas registradas se escriben con `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `TRUNCATE` y `COPY ... FROM STDIN`, cada una admitida por el derecho `write` del rol, sus concesiones de columnas y su filtro de filas; consulte [Escrituras de datos](pgwire.md#data-writes-insert-update-delete-merge-copy-from-stdin).
- Sin definiciones: `CREATE`, `ALTER` y `DROP` se rechazan con SQLSTATE 0A000, salvo la tabla temporal propia de una sesión; consulte [Definiciones](pgwire.md#definitions-create-alter-drop). `COPY ... TO STDOUT` exporta en `text`, `csv` y `binary`; `COPY ... FROM STDIN` lee `text` y `csv`.
- Las consultas a `information_schema` y `pg_catalog` se interceptan y se responden desde un shim de catálogo DuckDB — las herramientas de descubrimiento de esquemas funcionan correctamente.

---

## Driver JDBC {: #jdbc-driver }

El driver JDBC de Provisa usa Arrow Flight como transporte subyacente. Es la vía recomendada para herramientas de BI con un asistente de conexión JDBC.

Las consultas se ejecutan sobre Arrow Flight siempre que el puerto Flight responda al abrir la conexión, y los resultados llegan por lotes con sus tipos de columna. Se toma como puerto Flight el que está junto al puerto HTTP (8815 junto a 8001); defina la propiedad `flight_port` cuando no sea así. Si el puerto Flight no es accesible al conectar, la conexión usa la API HTTP para sus consultas y lo registra en INFO. Una vez que una conexión está en Flight, un error del servidor (una credencial rechazada, un rol que el usuario no tiene, una sentencia fallida) se lanza a quien llama con el mensaje del servidor; no se reintenta por HTTP.

La exploración del esquema (`getTables`, `getColumns`, claves primarias y foráneas) se lee del mismo puerto Flight, con la credencial con la que se inició sesión: el controlador lista el catálogo del rol con el que actúa, así que no aparece una tabla o columna que no se conceda a ese rol. Tampoco se lista una clave que remite a una tabla o columna no concedida al rol. En una conexión cuyo puerto Flight no era accesible al conectar, el controlador lee el mismo catálogo por HTTP (`GET /data/catalog`): el servidor construye ambos listados con una sola función, así que las tablas, los nombres de columna, los tipos y las claves no dependen de qué puerto respondió.

### Conexión {: #connection }

Descargue [provisa-jdbc.jar](https://provisa.dev/dl/jdbc) (siempre la última versión) y agréguelo a la ruta de drivers de su herramienta.

URL JDBC:

```yaml
jdbc:provisa://<host>:8815
```

La autenticación usa las propiedades JDBC estándar `user` / `password`. El controlador las cambia por un token de sesión y lo envía en cada solicitud; un inicio de sesión rechazado hace fallar la conexión con el estado y el motivo del servidor. Provisa deriva el rol de la identidad con la que se inició sesión. La propiedad opcional `role` (o `?role=` en la URL) pide actuar como un rol que tiene el usuario, o un conjunto separado por comas de ellos; un rol que el usuario no tiene se rechaza. En un despliegue sin proveedor de autenticación no hay inicio de sesión, y el nombre de usuario se toma como el rol solicitado.

### Configuración de herramientas de BI {: #bi-tool-setup }

**Tableau**

1. Administrar → Drivers → Instalar Provisa JDBC
2. Conectar → Otras bases de datos (JDBC)
3. URL: `jdbc:provisa://localhost:8815`
4. Ingrese su nombre de usuario y contraseña cuando se le solicite

**DBeaver** (vía JDBC — para la vía pgwire, ver arriba)

1. Base de datos → Nueva conexión → JDBC
2. Driver: agregue `provisa-jdbc.jar`
3. URL: `jdbc:provisa://localhost:8815`
4. Ingrese su nombre de usuario y contraseña en la pestaña de autenticación

**Power BI** — use la puerta de enlace ODBC con el puente Provisa JDBC-ODBC (incluido en el instalador).

---

## Clientes Arrow Flight {: #arrow-flight-clients }

Arrow Flight (puerto 8815) es la vía recomendada para herramientas de datos que lo admiten. Los resultados fluyen en streaming como RecordBatches de Arrow sin materializarse en la memoria de Provisa.

### Python (`provisa-client`)

La vía de Python recomendada — envuelve tanto GraphQL como Arrow Flight:

```bash
pip install provisa-client
```

```python
from provisa_client import ProvisaClient

client = ProvisaClient("http://localhost:8001", username="alice", password="secret")

# Arrow Flight → pyarrow Table (high-throughput, streaming)
table = client.flight("SELECT id, amount FROM sales.orders")

# Arrow Flight → pandas DataFrame
df = client.flight_df("SELECT id, amount FROM sales.orders")

# GraphQL → DataFrame
df = client.query_df("{ orders { id amount } }")
```

Consulte [docs/python-client.md](python-client.md) para la referencia completa, incluidos DB-API 2.0, el dialecto de SQLAlchemy y ADBC.

### Python (PyArrow puro) {: #python-raw-pyarrow }

```python
import pyarrow.flight as flight

client = flight.connect("grpc://localhost:8815")
ticket = flight.Ticket(b'{"query": "SELECT id, amount FROM sales.orders"}')
df = client.do_get(ticket).read_all().to_pandas()
```

Flight transporta su credencial en la carga JSON, como un campo `token`: un token bearer del proveedor o un token de acceso personal. Tanto el handshake como cada ticket lo aceptan, y ambos lo validan igual, de modo que un cliente que se autenticó en el handshake sigue presentando el token en cada `do_get`. Un campo `role` junto a él *solicita* un rol; el servidor deriva los roles permitidos de la identidad y sustituye el valor autorizado, así que una cadena de rol en un ticket nunca es la identidad. Consulte el [Modelo de seguridad](security.md#surfaces-and-credentials).

```python
ticket = flight.Ticket(json.dumps({
    "query": "SELECT id, amount FROM sales.orders",
    "token": "provisa_pat_...",
    "role": "analyst",
}).encode())
```

Explorar también necesita la credencial. `list_flights`, `get_flight_info` y `get_schema` no llevan ticket, así que leen la credencial de la cabecera `authorization: Bearer <token>` de la llamada y el rol solicitado de `x-provisa-role` (un rol, o un conjunto de roles que se tienen separados por comas). Con la autenticación activada, una llamada de exploración sin credencial se rechaza, y el catálogo que devuelve es el del rol autorizado: las tablas, columnas y comandos que se conceden a ese rol, y nada más. Un ticket de catálogo (un ticket sin `query`) se responde igual a partir de su propio `token` y `role`. Por eso un cliente debe enviar su credencial antes de explorar; `provisa-client` lo hace en `list_tables()`. En un despliegue sin proveedor de autenticación estas llamadas listan el catálogo completo y no toman credencial.

```python
options = flight.FlightCallOptions(headers=[
    (b"authorization", b"Bearer provisa_pat_..."),
    (b"x-provisa-role", b"analyst"),
])
for info in client.list_flights(b"", options):
    print(info.descriptor.path)
```

### ADBC

```python
import adbc_driver_flightsql.dbapi as adbc

conn = adbc.connect("grpc://localhost:8815", db_kwargs={"username": "alice", "password": "secret"})
cursor = conn.cursor()
cursor.execute("SELECT id, amount FROM sales.orders")
table = cursor.fetch_arrow_table()
```

### DuckDB

```python
import duckdb, pyarrow.flight as flight

client = flight.connect("grpc://localhost:8815")
ticket = flight.Ticket(b'{"query": "SELECT * FROM sales.orders"}')
arrow_table = client.do_get(ticket).read_all()

conn = duckdb.connect()
result = conn.execute("SELECT region, sum(amount) FROM arrow_table GROUP BY 1").df()
```

### Spark (PySpark)

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .config("spark.jars.packages", "org.apache.arrow:flight-core:14.0.0") \
    .getOrCreate()

# Use ADBC Flight connector or load via pandas → Spark
```

---

## Protobuf gRPC (puerto 50051) {: #protobuf-grpc-port-50051 }

Ruta de servicio a servicio. Provisa genera un `.proto` por rol y lo regenera cada vez que cambia el modelo o el rol — cada rol ve solo las tablas y columnas a las que tiene acceso. El servicio tiene una RPC por cada tabla en la raíz del esquema del rol; las tablas relacionadas se alcanzan a través de sus relaciones, como en GraphQL.

Descargue el proto de su rol:

```bash
curl http://localhost:8001/data/proto/analyst > provisa_analyst.proto
```

Use `grpc_server_reflection` para descubrir el esquema de forma programática. La reflexión describe el servicio del rol con el que actúa la llamada, el mismo archivo que la descarga anterior. Con la autenticación desactivada, una llamada de reflexión indica su rol como cualquier otra llamada:

```bash
grpcurl -plaintext -H 'x-provisa-role: analyst' localhost:50051 list
```

Cada RPC debe llevar una credencial en la clave de metadatos `authorization`: un token del proveedor o un token de acceso personal. `x-provisa-role` solicita un rol del conjunto permitido de la identidad; no es una credencial ni lo fue nunca. Se admiten certificados de cliente. Consulte el [Modelo de seguridad](security.md#surfaces-and-credentials).

Las consultas en streaming emiten un mensaje por fila; las mutaciones son unarias.

---

## Invocar comandos entre protocolos {: #invoking-commands-across-protocols }

Un **comando** es una función registrada rastreada o un webhook — un elemento invocable registrado en el modelo de Provisa con un `kind` (`query` o `mutation`) y un `impl_kind` que describe cómo se ejecuta. Toda superficie enruta las invocaciones a través de un único ejecutor gobernado (`invoke_tracked_function`) que aplica la asignación del comando (`visible_to`), el derecho `write` para una mutación y el gobierno de manera uniforme.

| `impl_kind` | Qué se ejecuta | Campos de enlace |
| ------------ | ----------- | --------------- |
| `source_procedure` | Procedimiento almacenado en un origen registrado (predeterminado) | `sourceId`, `schemaName`, `functionName` |
| `script` | Script del lado del servidor | `script` |
| `http` | Llamada HTTP saliente | `url`, `method` |
| `grpc` | Llamada gRPC saliente a un servidor externo | `target`, `method` |
| `python` | Elemento invocable de Python alojado por Provisa | `callable` (por ejemplo, `demo.py_functions:random_dataset`) |

Cuando un comando declara un `return_schema` (JSON Schema con `type: array, items: object`), es de retorno de conjunto — toda superficie lo proyecta como un conjunto de filas tipado. Los comandos de demostración `random_python_set` (impl_kind `python`) y `random_grpc_set` (impl_kind `grpc`) ilustran tanto un elemento invocable alojado como un puente gRPC externo que devuelve filas de valor aleatorio; ambos están registrados en `config/provisa-install.yaml`.

### Matriz de protocolos {: #protocol-matrix }

| Superficie | Sintaxis | Ejemplo |
| --------- | -------- | --------- |
| GraphQL | `kind=query` → campo Query; `kind=mutation` → campo Mutation; con prefijo de dominio cuando `domain_prefix: true` | `{ ps__random_python_set(rows: 5, seed: 42) { id region amount } }` |
| pgwire / Arrow Flight / MCP `run_sql` | `SELECT * FROM fn(args)` o `SELECT fn(args)` | `SELECT * FROM random_python_set(5, 42)` |
| Cypher HTTP (`POST /data/cypher`) | `CALL fn(args) YIELD cols` | `CALL random_python_set(5, 42) YIELD id, region, amount` |
| Bolt (Neo4j Browser / driver) | `CALL fn(args)` — los argumentos posicionales se asignan a los nombres de argumento declarados | `CALL random_python_set(3, 7)` |
| Provisa gRPC (puerto 50051) | Unario `CallCommand(CommandRequest{name, args_json})` → `CommandResponse{rows_json}` | `grpcurl -d '{"name":"random_python_set","args_json":"{\"rows\":5}"}' ... ProvisaService/CallCommand` |

El campo `kind` controla únicamente la ubicación en GraphQL — las superficies SQL, Cypher, Bolt y gRPC aceptan por igual comandos `query` y `mutation`.

---

## Apollo Federation

Provisa puede actuar como un subgrafo de Federation v2, exponiendo su esquema publicado a un Apollo Router o Apollo Gateway.

### Configuración {: #setup }

Active la federación en `config.yaml`:

```yaml
federation:
  enabled: true
  subgraph_name: provisa-data
```

Provisa genera directivas `@key` en las columnas de clave primaria y `@external`/`@provides` en las relaciones entre subgrafos automáticamente.

### Registro con Apollo Router {: #register-with-apollo-router }

En su `supergraph.yaml`:

```yaml
subgraphs:
  provisa-data:
    routing_url: http://provisa:8001/data/graphql
    schema:
      subgraph_url: http://provisa:8001/data/graphql
```

Ejecute `rover supergraph compose --config supergraph.yaml` para generar el esquema del supergrafo.

### Entidades {: #entities }

Provisa responde a las consultas `_entities` para uniones entre subgrafos. Cualquier tabla con una clave primaria es automáticamente resoluble como una entidad de Federation.

---

## Importación de Hasura v2 / DDN {: #hasura-v2-ddn-import }

Consulte [docs/import.md](import.md) para la migración de Hasura a Provisa.

---

## Kafka

Consulte [docs/sources.md](sources.md#kafka-sources) para la configuración de temas de Kafka como tablas de solo lectura y receptores de resultados de consulta.

---

## Verificadores de calidad de datos {: #data-quality-checkers }

Soda Core y Great Expectations se conectan a Provisa de la misma manera que cualquier otro cliente de postgres — a través de pgwire. Esa es toda la integración: el verificador mantiene un único driver de postgres y analiza la vista federada, de modo que una tabla de Snowflake, una tabla de Iceberg y una colección de Mongo se verifican con el mismo dialecto de contrato, sin un verificador por sistema.

El análisis se ejecuta en un intérprete hijo — `python -m provisa.dq.worker` — que es el único lugar donde se importa `soda_core` o `great_expectations`. Nada se enlaza en el proceso del servidor, y un fallo del verificador derriba un subproceso en lugar del bucle de eventos.

Los resultados del análisis llegan como filas de origen ordinarias, de modo que la cadencia, la actualidad, los eventos, el linaje, el gobierno, la RLS, la cuadrícula y la exportación se aplican todos sin un segundo mecanismo. La redacción de contratos, el sobre de resultados y el registro derivado se cubren en [docs/sources.md](sources.md#data-quality-checkers).

### Instalación de un verificador {: #installing-a-checker }

Ninguna de las dos bibliotecas se incluye de manera predeterminada. El instalador pregunta cuál desea, y la respuesta se convierte en `dq_checker: none|soda|gx` en `~/.provisa/config.yaml`. En el nivel Docker, `scripts/provisa` convierte eso en el argumento de compilación `PROVISA_EXTRAS`; en el nivel nativo, `first-launch.sh` instala el extra de pyproject correspondiente en el venv.

| `dq_checker` | Biblioteca | Licencia | Plano de nube alojado |
| -------------- | --------- | --------- | -------------------- |
| `soda` | `soda-postgres` | Elastic License 2.0 | Rechazado (`cloud_eligible: false`) |
| `gx` | `great-expectations[postgresql]` | Apache 2.0 | Permitido |

Elastic License 2.0 prohíbe ofrecer el software a terceros como un servicio alojado, que es justamente lo que sería ejecutar Soda dentro del plano SaaS en nombre de un inquilino. Una implementación alojada que desee usar Soda apunta a un endpoint de Soda que el operador ejecuta por su cuenta. Consulte [docs/configuration.md](configuration.md#data-quality-checkers-soda-great_expectations) para las claves de conexión.

---

## Intercambio semántico Apache Ossie {: #apache-ossie-semantic-interchange }

Provisa intercambia modelos semánticos con Apache Ossie (especificación 0.2.0.dev0, en incubación; antes Open
Semantic Interchange) mediante un adaptador de frontera. El vocabulario interno de Provisa nunca se renombra
al de Ossie: la especificación declara probables los cambios incompatibles, así que el acoplamiento se limita al adaptador.

La especificación en borrador ha cambiado de forma sin cambiar su cadena de versión. El adaptador sigue la
especificación tal como está en el commit `642cb894` de `apache/ossie` (2026-10-10): un modelo por documento,
con `version`, `name`, `datasets`, `relationships` y `metrics` en la raíz del documento, y
un `datatype` enumerado. La exportación se comprueba en las pruebas unitarias contra el
`core-spec/ossie-schema.json` de ese commit.

### Exportación {: #export }

La superficie canónica de exportación es un endpoint HTTP en vivo. Deriva el documento Ossie del estado en vivo
en cada solicitud: sin caché ni paso de generación.

```http
GET /admin/ossie
```
La respuesta es un documento YAML con `Content-Disposition: attachment; filename=provisa.ossie.yaml`.

La página Métricas también ofrece un botón **Descargar** y una URL de endpoint copiable en el panel Ossie
Interchange, ambos apuntando al mismo endpoint.

#### Qué se exporta {: #what-is-exported }

El adaptador asigna los objetos de Provisa a objetos de Ossie así:

| Objeto de Provisa | Objeto de Ossie | Notas |
| --- | --- | --- |
| `Table` | `dataset` | `source` = `catalog.schema.table`; claves primarias/únicas a partir de la configuración de columnas y `UniqueConstraint` |
| `Column` | `field` | `expression` = referencia de columna (dialecto ANSI_SQL); las columnas de tiempo obtienen `dimension.is_time: true` |
| `Relationship` | `relationship` | Se usa el alias como nombre cuando existe; las relaciones calculadas (con destino una función) se omiten |
| `Metric` | `metric` | `name`, `expression` (ANSI_SQL), `datatype` (asignado como el de una columna), `description`, `ai_context` |
| `modeling_role` / `modeling_history` | `custom_extensions[].vendor_name="provisa"` | Solo para el viaje de ida y vuelta; otras herramientas pueden ignorarlo |

Una relación sobre varias columnas enumera cada columna en `from_columns` y `to_columns`. Un
modelo sin ninguna tabla se rechaza con `422`: un documento Ossie tiene al menos un dataset.

El gobierno, la RLS, el linaje y la semántica de grafos no se exportan. Pueden viajar en la ranura opcional
`provisa` de custom_extensions para la fidelidad del viaje de ida y vuelta, pero el intercambio nunca depende de que otras
herramientas la lean.

Un tipo de columna fuera del vocabulario de Ossie se escribe como `Opaque`, con el nombre propio del tipo en la
extensión `provisa` del campo, que es lo que la especificación indica para un tipo conocido para el que no tiene
miembro. Una columna sin tipo registrado no indica `datatype`. El adaptador nunca asigna un tipo que la
columna no tiene.

#### Correspondencia de tipos {: #type-mapping }

| Tipo de Provisa / origen | `datatype` de Ossie |
| --- | --- |
| `varchar`, `text`, `char`, `uuid`, `string` | `String` |
| `int`, `integer`, `bigint`, `smallint`, `int4`, `int8`, `tinyint` | `Integer` |
| `numeric`, `decimal` | `Decimal` |
| `float`, `double`, `real` | `Float` |
| `bool`, `boolean` | `Boolean` |
| `date` | `Date` |
| `time` | `Time` |
| `timestamp`, `datetime` | `DateTime` |
| `timestamptz`, `timestamp with time zone` | `DateTimeTz` |
| cualquier otro | `Opaque`, con el nombre del tipo en la extensión `provisa` |

El `Decimal` de Ossie no lleva precisión ni escala, así que `numeric(10,2)` se exporta como `Decimal`
y vuelve de una importación como `decimal`.

### Importación {: #import }

La importación acepta un documento Ossie (YAML o JSON) y devuelve propuestas de registro. Nada se
registra automáticamente: las definiciones importadas nunca se saltan el paso de revisión.

```http
POST /admin/ossie/import
Content-Type: text/yaml   (or application/json)

<ossie document>
```
El servidor analiza el documento con `parse_ossie_model`, que valida la estructura y devuelve una
dataclass `OssieImport` con las tablas, relaciones y métricas propuestas como diccionarios simples.
Cualquier problema estructural es un `400` con un error nombrado por ruta, p. ej.
`ossie import: missing $.datasets[1].source`.

El importador solo lee la forma plana del documento. Tres cosas se rechazan por su nombre:

- Un documento que envuelve su modelo en `semantic_model`, la forma que tenía la especificación en borrador antes de
  septiembre de 2026. El error dice que el documento es anterior a la forma plana y cómo convertirlo.
- Una `version` distinta de `0.2.0.dev0`.
- Un `datatype` que no es miembro del vocabulario de Ossie; el error nombra la ruta y enumera
  los miembros.

Una columna o métrica propuesta lleva el tipo que representa su datatype de Ossie (`Integer` propone
`bigint`, `DateTime` propone `timestamp`). `Opaque` propone el tipo que nombra la extensión `provisa`
cuando el documento vino de Provisa, y ningún tipo en otro caso. Un `ai_context` dado como
objeto se lee por sus `instructions`.

Un campo se propone como columna solo cuando su expresión es una columna del origen. Un campo
cuya expresión es un cálculo no se propone; la pantalla de revisión lo nombra bajo su tabla
como no importado. Un campo que renombra su columna se propone como esa columna, con el nombre del
campo como alias de la columna. El `source` de un dataset debe tener tres partes,
`source.schema.table`; una consulta o un nombre más corto se rechaza, y el error lo dice.

#### Desde dbt, Cube, Snowflake y otras capas semánticas {: #coming-from-dbt-cube-snowflake-and-other-semantic-layers }

El proyecto Apache Ossie mantiene conversores entre Ossie y los formatos de modelo de otras herramientas. Ejecute
el conversor de su herramienta y luego envíe su salida al endpoint de importación o elija el archivo en la
pantalla de revisión de abajo. Provisa lee el documento Ossie; no lee los archivos de la otra herramienta.

| Desde | Comando del conversor | Qué lee |
|---|---|---|
| dbt (capa semántica) | `ossie-dbt msi-to-ossie -i target/semantic_manifest.json -o semantic_model.yaml` | El `semantic_manifest.json` que escribe dbt para MetricFlow |
| Cube | `ossie-cube import -i model/ -o model.yaml` | Un directorio de modelo de Cube, o archivos individuales de cubos y vistas |

El mismo repositorio contiene conversores para Snowflake, Databricks, Microsoft, Salesforce,
ThoughtSpot, Sigma, Omni, Hex, GoodData y otros; cada uno tiene su propio README.

Dos límites que conviene conocer antes de convertir:

- El conversor de dbt solo lee el manifiesto de la capa semántica. Un proyecto dbt sin modelos
  semánticos no tiene nada que convertir. Descarta las métricas de conversión, las métricas privadas y las
  entidades de clave natural, e imprime una advertencia por cada una.

- La salida de un conversor sigue la especificación tal como la tiene el propio commit de ese conversor. Provisa lee
  la forma plana del commit `642cb894` de `apache/ossie`; un conversor anterior a septiembre de 2026
  escribe la envoltura `semantic_model`, que Provisa rechaza por su nombre.

Ambas vías se ejecutaron el 2026-10-10 con los conversores instalados desde el commit `642cb894` de `apache/ossie`
(no se publican como paquetes; se instalan desde los directorios `converters/dbt` y
`converters/cube` del repositorio y necesitan Python 3.11 o posterior). Un modelo de
dos tablas, una relación y una métrica se exportó desde Provisa, se convirtió a un manifiesto semántico de dbt
y a un modelo de Cube, se volvió a convertir a Ossie con `ossie-dbt msi-to-ossie` y
`ossie-cube import`, y lo leyó `parse_ossie_model`:

| Vía | Resultado |
|---|---|
| Modelo de Cube → Ossie → Provisa | Leído. Volvieron las tablas, las claves, los tipos de columna, la relación y la métrica; el nombre de la relación era el del conversor (`orders_to_customers`). |
| Manifiesto semántico de dbt → Ossie → Provisa | Leído. Volvieron las tablas, las claves primarias y la métrica. Los tipos de columna no (la salida del conversor no indica ninguno), y la relación volvió tal como el conversor la derivó de las entidades de dbt, que no era la registrada. Revise las relaciones antes de aplicar. |

El conversor de dbt rechaza un modelo en el que alguna tabla tiene una restricción única sobre varias
columnas ("MetricFlow entities cannot represent composite keys losslessly").

#### La pantalla de revisión {: #the-review-screen }

En la interfaz, el botón **Importar** (página Métricas → panel Ossie Interchange) abre un selector de archivos.
Una vez enviado y analizado el documento, se abre un modal de revisión con cada tabla,
relación y métrica propuesta listada como elemento marcado. El modelador puede desmarcar cualquier cosa para excluirla.
Al pulsar **Aplicar** se registran los elementos marcados mediante las mutaciones de registro existentes: primero las
tablas, luego las relaciones (que hacen referencia a tablas) y luego las métricas.

El rol de modelado y el historial guardados en un documento Ossie exportado por Provisa sobreviven correctamente a la
importación.

### Archivo de sources de dbt {: #dbt-sources-file }

Para un proyecto dbt que no usa la capa semántica, Provisa escribe directamente un `sources.yml` de dbt.
Se deriva del modelo en vivo en cada solicitud.

```http
GET /admin/dbt/sources?role=<role id>
```
La respuesta es YAML con `Content-Disposition: attachment; filename=provisa.sources.yml`. Quien
llama necesita `org_settings`, y `role` debe ser un rol que tenga; quien tiene
`access_config` puede nombrar cualquier rol. El panel de intercambio de la página Métricas ofrece la misma descarga
para el rol en uso.

El archivo lista cada tabla que se sirve al rol, bajo un source de dbt por origen y esquema (nombrado
por el id del origen, o `<source>_<schema>` cuando un origen tiene tablas en más de un esquema),
con la descripción de la tabla, las descripciones de las columnas servidas y estas pruebas:

| El modelo indica | El archivo escribe |
|---|---|
| Una clave primaria de una columna | `unique` y `not_null` en esa columna |
| Una clave primaria de varias columnas | `not_null` en cada columna |
| Una restricción única de una columna | `unique` en esa columna |
| Una relación entre dos tablas que se sirven ambas al rol | `relationships` en la columna de origen, hacia `source('<source>', '<table>')` |

Una columna que no se sirve al rol no aparece, como tampoco una tabla sin ninguna columna servida. Una clave sobre
varias columnas no tiene prueba estándar de dbt; el comentario inicial del archivo nombra cada una y no se escribe
ninguna prueba para ella. El modelo no registra la nulabilidad, así que `not_null` aparece solo en columnas de clave.
El archivo no lleva métricas, seguridad de filas, enmascaramiento, roles ni linaje, y su comentario inicial
lo dice.

Un archivo generado a partir de un modelo de dos tablas y una relación lo analizó dbt-core 1.12.5
con dbt-duckdb 1.11.0: dos sources y cinco pruebas, sin advertencias. dbt-core 1.9.11 no lee
sus pruebas `relationships`, que anidan sus argumentos bajo `arguments`; no se probaron versiones intermedias.

---

## Métricas entre protocolos {: #metrics-across-protocols }

La definición de una métrica gobernada — su expresión, descripción y `ai_context` — viaja con el
valor a cada superficie de consulta a través de una única expansión del compilador. No hay copias.
El compilador reserva el esquema `metrics` para el acceso SQL; cada protocolo agrega luego su
propio canal de metadatos.

### SQL / pgwire

Direccione cualquier métrica como una relación virtual en el esquema `metrics`. Las columnas de
dimensión que seleccione se convierten en el GROUP BY:

```sql
-- Grand total
SELECT value FROM metrics.net_revenue;

-- By region
SELECT region, value FROM metrics.net_revenue GROUP BY region;

-- By region and month, filtered
SELECT region, month, value
FROM metrics.net_revenue
WHERE net_revenue.status = 'completed'
GROUP BY region, month;
```

El compilador expande la forma `metrics.<name>` en el agregado agrupado real antes de que se
ejecute el gobierno. Las descripciones de columnas se exponen como entradas `pg_description`, por
lo que DBeaver y `\d+` de psql las muestran.

`SELECT *` se rechaza — nombre las columnas explícitamente.

### GraphQL

Las métricas se proyectan dentro del campo raíz `_aggregate` como un bloque `metrics`.

El texto de la definición (`description`, `ai_context`) aparece en la documentación de introspección
de GraphQL, por lo que las herramientas conscientes del esquema y la generación de código lo captan
automáticamente.

### MCP (agentes de IA) {: #mcp-ai-agents }

Dos herramientas exponen métricas a los clientes MCP:

- **`list_metrics`** — devuelve todas las métricas gobernadas visibles para la sesión, con `name`,
  `description` y `ai_context`.
- **`query_metric`** — acepta un nombre de métrica más una lista de dimensiones y llama a la vía
  SQL semántica del compilador, devolviendo el resultado agregado.

Los agentes que llaman a `list_metrics` antes de construir una consulta seleccionan una métrica
gobernada por nombre en lugar de escribir SQL de agregación a mano. El campo `ai_context` es el
lugar para colocar el texto de definición que guía la selección correcta.

### Arrow Flight

Las métricas son direccionables como descriptores de vuelo de métrica que devuelven tablas Arrow.

Use la misma forma SQL `metrics.<name>` a través de la vía estándar de ticket Flight SQL.

### Bolt / Cypher (Neo4j Browser)

Llame a una métrica usando el procedimiento `provisa.metric()`:

```cypher
CALL provisa.metric('net_revenue', ['region']) YIELD region, value
```

Las tablas de hechos y dimensiones llevan las etiquetas de nodo `:Fact` y `:Dimension` en el grafo
federado, por lo que Bloom representa la forma de estrella automáticamente.

### Consultas en lenguaje natural {: #natural-language-queries }

El comparador de esquemas de LN resuelve el vocabulario de métricas en preguntas en lenguaje
natural directamente a una métrica más dimensiones, y luego genera SQL semántico.

Las tablas de hechos se etiquetan como `[fact]` en el prompt de LN; las tablas de dimensión se
etiquetan como `[dimension]`. El comparador sesga las rutas de unión de hecho a dimensión al
resolver preguntas.

### Streaming

Combine `view_metrics` con `materialize` para mantener una vista de métricas al día a medida que
cambian sus entradas, usando la maquinaria de materialización existente. No se requiere ningún
pipeline nuevo.

### Observabilidad (OTel) {: #observability-otel }

Las evaluaciones de métricas se trazan y se pueden exportar como métricas de OpenTelemetry.
