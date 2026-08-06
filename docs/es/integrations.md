# Integraciones

## Elegir una vía de conexión

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

## pgwire — Driver nativo de PostgreSQL

Provisa implementa el protocolo de cable de PostgreSQL (versión de protocolo 3.0). Cualquier cliente que hable PostgreSQL se conecta sin un driver personalizado.

Actívelo estableciendo `PROVISA_PGWIRE_PORT` (por ejemplo, `5433`) antes de iniciar Provisa. Está deshabilitado cuando no se establece o es `0`.

### ¿Por qué pgwire en lugar de JDBC?

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

### Autenticación

El campo `password` del paquete de inicio transporta la credencial, y *qué* es la credencial determina el método: un token de acceso personal, un token bearer OIDC o una contraseña contra el proveedor configurado. Bajo el proveedor `basic` con `auth.scram: true`, la contraseña se demuestra mediante SCRAM-SHA-256 en lugar de enviarse. Se admiten certificados de cliente. En modo de confianza (`none`), el nombre de usuario se asigna directamente a un rol y la contraseña se ignora.

La tabla completa de interfaz × método está en el [Modelo de seguridad](security.md#surfaces-and-credentials). MD5 no es compatible; active TLS (`PROVISA_PGWIRE_CERT` / `PROVISA_PGWIRE_KEY`) cuando opere sobre una red no confiable.

### Limitaciones

- Solo SQL. GraphQL y Cypher no se aceptan sobre pgwire.
- No es de solo lectura. `COPY ... FROM STDIN` inserta filas en orígenes `postgresql`, `mysql`, `sqlite` y `mariadb`, y se admite DDL (ver abajo).
- DDL (`CREATE`, `ALTER`, `DROP`) es compatible y se despacha a la vía de Trino o directa; la nueva tabla se registra en el contexto de compilación y queda consultable de inmediato. `COPY ... TO STDOUT` (exportación) y `COPY ... FROM STDIN` (importación) son compatibles en formatos `text` y `csv`.
- Las consultas a `information_schema` y `pg_catalog` se interceptan y se responden desde un shim de catálogo DuckDB — las herramientas de descubrimiento de esquemas funcionan correctamente.

---

## Driver JDBC

El driver JDBC de Provisa usa Arrow Flight como transporte subyacente. Es la vía recomendada para herramientas de BI con un asistente de conexión JDBC.

### Conexión

Descargue [provisa-jdbc.jar](https://provisa.dev/dl/jdbc) (siempre la última versión) y agréguelo a la ruta de drivers de su herramienta.

URL JDBC:

```yaml
jdbc:provisa://<host>:8815
```

La autenticación usa las propiedades JDBC estándar `user` / `password`. Provisa autentica las credenciales contra el proveedor de autenticación configurado y asigna el rol — el cliente no elige su propio rol.

### Configuración de herramientas de BI

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

## Clientes Arrow Flight

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

### Python (PyArrow puro)

```python
import pyarrow.flight as flight

client = flight.connect("grpc://localhost:8815")
ticket = flight.Ticket(b'{"query": "SELECT id, amount FROM sales.orders"}')
df = client.do_get(ticket).read_all().to_pandas()
```

Flight transporta su credencial en la carga JSON, como un campo `token`: un token bearer del proveedor o un token de acceso personal. Tanto el handshake como cada ticket lo aceptan, y ambos lo validan igual, de modo que un cliente que se autenticó en el handshake sigue presentando el token en cada `do_get`. Un campo `role` junto a él *solicita* un rol; el servidor deriva los roles permitidos de la identidad y sustituye el valor autorizado, así que una cadena de rol en un ticket nunca es la identidad. (REQ-1263) Consulte el [Modelo de seguridad](security.md#surfaces-and-credentials).

```python
ticket = flight.Ticket(json.dumps({
    "query": "SELECT id, amount FROM sales.orders",
    "token": "provisa_pat_...",
    "role": "analyst",
}).encode())
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

## Protobuf gRPC (puerto 50051)

Vía de servicio a servicio. Provisa genera un `.proto` por rol al iniciar — cada rol ve solo las tablas y columnas a las que tiene acceso.

Descargue el proto de su rol:

```bash
curl http://localhost:8001/proto/analyst > provisa_analyst.proto
```

Use `grpc_server_reflection` para descubrir el esquema mediante programación.

Cada RPC debe llevar una credencial en la clave de metadatos `authorization`: un token del proveedor o un token de acceso personal. `x-provisa-role` solicita un rol del conjunto permitido de la identidad; no es una credencial ni lo fue nunca. Se admiten certificados de cliente. Consulte el [Modelo de seguridad](security.md#surfaces-and-credentials).

Las consultas en streaming emiten un mensaje por fila; las mutaciones son unarias.

---

## Invocar comandos entre protocolos

Un **comando** es una función registrada rastreada o un webhook — un elemento invocable registrado en la capa semántica de Provisa con un `kind` (`query` o `mutation`) y un `impl_kind` que describe cómo se ejecuta. Toda superficie enruta las invocaciones a través de un único ejecutor gobernado (`invoke_tracked_function`) que aplica `writable_by` y el gobierno de manera uniforme (REQ-1156). [tool-verified: `provisa/api/data/action_exec.py`, `provisa/bolt/session.py:786-791`, `provisa/grpc/server.py:107-135`, `provisa/pgwire/function_call.py:80-88`, `provisa/api/flight/server.py:542-554`]

| `impl_kind` | Qué se ejecuta | Campos de enlace |
| ------------ | ----------- | --------------- |
| `source_procedure` | Procedimiento almacenado en un origen registrado (predeterminado) | `sourceId`, `schemaName`, `functionName` |
| `script` | Script del lado del servidor | `script` |
| `http` | Llamada HTTP saliente | `url`, `method` |
| `grpc` | Llamada gRPC saliente a un servidor externo | `target`, `method` |
| `python` | Elemento invocable de Python alojado por Provisa (REQ-885) | `callable` (por ejemplo, `demo.py_functions:random_dataset`) |

Cuando un comando declara un `return_schema` (JSON Schema con `type: array, items: object`), es de retorno de conjunto — toda superficie lo proyecta como un conjunto de filas tipado. Los comandos de demostración `random_python_set` (impl_kind `python`) y `random_grpc_set` (impl_kind `grpc`) ilustran tanto un elemento invocable alojado como un puente gRPC externo que devuelve filas de valor aleatorio; ambos están registrados en `config/provisa-install.yaml`. [tool-verified: `config/provisa-install.yaml:809-856`]

### Matriz de protocolos

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

### Configuración

Active la federación en `config.yaml`:

```yaml
federation:
  enabled: true
  subgraph_name: provisa-data
```

Provisa genera directivas `@key` en las columnas de clave primaria y `@external`/`@provides` en las relaciones entre subgrafos automáticamente.

### Registro con Apollo Router

En su `supergraph.yaml`:

```yaml
subgraphs:
  provisa-data:
    routing_url: http://provisa:8001/data/graphql
    schema:
      subgraph_url: http://provisa:8001/data/graphql
```

Ejecute `rover supergraph compose --config supergraph.yaml` para generar el esquema del supergrafo.

### Entidades

Provisa responde a las consultas `_entities` para uniones entre subgrafos. Cualquier tabla con una clave primaria es automáticamente resoluble como una entidad de Federation.

---

## Importación de Hasura v2 / DDN

Consulte [docs/import.md](import.md) para la migración de Hasura a Provisa.

---

## Kafka

Consulte [docs/sources.md](sources.md#origenes-kafka) para la configuración de temas de Kafka como tablas de solo lectura y receptores de resultados de consulta.

---

## Intercambio semántico Apache Ossie (REQ-1316)

Provisa intercambia modelos semánticos con Apache Ossie (especificación 0.2.0.dev0, en incubación;
antes Open Semantic Interchange) a través de un adaptador de frontera. El vocabulario interno de
Provisa nunca se renombra al de Ossie — la especificación declara probables cambios disruptivos,
por lo que el acoplamiento se confina al adaptador.
[tool-verified: `provisa/ossie/convert.py` docstring lines 7–16; `OSSIE_VERSION = "0.2.0.dev0"`,
`provisa/ossie/convert.py` line 29]

### Exportación

La superficie de exportación canónica es un endpoint HTTP en vivo. Deriva el documento Ossie del
estado en vivo en cada solicitud — sin caché, sin paso de generación.

```http
GET /admin/ossie
```

La respuesta es un documento YAML con `Content-Disposition: attachment; filename=provisa.ossie.yaml`.
[tool-verified: `ossie_router.py` lines 20–33: "THE canonical live Ossie endpoint: the semantic
model derived from live state on every read — no caching, no regeneration step"]

La página de Métricas también ofrece un botón de **Descargar** y una URL de endpoint copiable en el
panel de Intercambio Ossie, ambos apuntando al mismo endpoint.
[tool-verified: `OssieInterchangePanel.tsx` lines 64–79: `endpointUrl = window.location.origin + OSSIE_ENDPOINT_PATH`]

#### Qué se exporta

El adaptador asigna los objetos de Provisa a objetos de Ossie de la siguiente manera:

| Objeto de Provisa | Objeto de Ossie | Notas |
| --- | --- | --- |
| `Table` | `dataset` | `source` = `catalog.schema.table`; claves primarias/únicas desde la configuración de columnas y `UniqueConstraint` |
| `Column` | `field` | `expression` = referencia de columna (dialecto ANSI_SQL); las columnas de tiempo obtienen `dimension.is_time: true` |
| `Relationship` | `relationship` | Se usa el alias como nombre cuando está establecido; las relaciones calculadas (destino de función) se omiten |
| `Metric` | `metric` | `name`, `expression` (ANSI_SQL), `datatype`, `description`, `ai_context` — sin pérdida por diseño |
| `modeling_role` / `modeling_history` | `custom_extensions[].vendor_name="provisa"` | Solo para ida y vuelta; otras herramientas pueden ignorarlo |

[tool-verified: `_table_to_dataset`, `build_ossie_model`, `provisa/ossie/convert.py` lines 90–198;
`_table_to_dataset` comment at line 153: "Computed (function-target) relationships have no dataset
target — not representable in Ossie; skipping is the defined export boundary"]

El gobierno, la seguridad de nivel de fila, el linaje y la semántica de grafo no se exportan. Pueden
viajar en la ranura opcional `provisa` de custom_extensions para fidelidad de ida y vuelta, pero el
intercambio nunca depende de que otras herramientas la lean. [tool-verified: `provisa/ossie/convert.py` docstring lines 13–15]

Los tipos de columna de Provisa desconocidos pasan tal cual; el adaptador nunca asigna
silenciosamente a un tipo incorrecto. [tool-verified: `_map_datatype`, `provisa/ossie/convert.py` lines 70–77: "Unknown types
pass through verbatim — mapping silently to a wrong type would corrupt the model"]

#### Asignación de tipos

[tool-verified: `_DATATYPE_MAP`, `provisa/ossie/convert.py` lines 35–65]

| Tipo de Provisa / origen | `datatype` de Ossie |
| --- | --- |
| `varchar`, `text`, `char`, `uuid`, `string` | `string` |
| `int`, `integer`, `bigint`, `smallint`, `int4`, `int8`, `tinyint` | `integer` |
| `numeric`, `decimal`, `float`, `double`, `real` | `number` |
| `bool`, `boolean` | `boolean` |
| `date` | `date` |
| `time` | `time` |
| `timestamp`, `timestamptz`, `datetime` | `timestamp` |
| cualquier otro | pasa tal cual |

### Importación

La importación acepta un documento Ossie (YAML o JSON) y devuelve propuestas de registro. Nada se
registra automáticamente — las definiciones importadas nunca omiten el paso de revisión.

```http
POST /admin/ossie/import
Content-Type: text/yaml   (or application/json)

<ossie document>
```

El servidor analiza el documento con `parse_ossie_model`, que valida la estructura y devuelve una
clase de datos `OssieImport` que contiene las tablas, relaciones y métricas propuestas como
diccionarios planos. Cualquier problema estructural es un `400` con un error con ruta nombrada, por
ejemplo, `ossie import: missing semantic_model[0].datasets[1].source`.
[tool-verified: `import_ossie`, `provisa/api/admin/ossie_router.py` lines 36–52:
"Nothing is registered here — imported definitions never bypass registration review"]

#### La pantalla de revisión

En la interfaz, el botón **Importar** (página Métricas → panel de Intercambio Ossie) abre un selector
de archivos. Después de que el documento se publica y se analiza, se abre un modal de revisión con
cada tabla, relación y métrica propuesta listada como un elemento marcado. El modelador puede
desmarcar cualquier elemento para excluirlo. Al hacer clic en **Aplicar** se registran los elementos
marcados a través de las mutaciones de registro existentes — primero las tablas, luego las
relaciones (que hacen referencia a tablas), y luego las métricas.
[tool-verified: `OssieInterchangePanel.tsx` lines 88–165: "Review screen opens with everything
checked; trimming = unchecking"; "Tables first, then relationships... then metrics — each through
the EXISTING registration mutations (REQ-1316)"]

El rol de modelado y el historial almacenados en un documento Ossie exportado por Provisa hacen el
recorrido de ida y vuelta correctamente a través de la importación. [tool-verified: `_parse_dataset` custom_extensions handling,
`provisa/ossie/convert.py` lines 287–300: "REQ-1320: round-trip the provisa modeling metadata slot"]

---

## Métricas entre protocolos (REQ-1319)

La definición de una métrica gobernada — su expresión, descripción y `ai_context` — viaja con el
valor a cada superficie de consulta a través de una única expansión del compilador. No hay copias.
El compilador reserva el esquema `metrics` para el acceso SQL; cada protocolo agrega luego su
propio canal de metadatos.

[tool-verified: `METRICS_SCHEMA = "metrics"`, `provisa/compiler/metric_expand.py` line 43;
REQ-1319 requirement text: "the definition (description, ai_context) travels with the value
everywhere, with no copies"]

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
lo que DBeaver y `\d+` de psql las muestran. [tool-verified: `metric_semantic_sql`, `provisa/compiler/metric_expand.py` lines 52–70;
REQ-1319: "description surfaced via pg_description"]

`SELECT *` se rechaza — nombre las columnas explícitamente.
[tool-verified: `expand_metric_query`, `provisa/compiler/metric_expand.py` lines 302–306]

### GraphQL

Las métricas se proyectan dentro del campo raíz `_aggregate` como un bloque `metrics`.
[inferred: per REQ-1319; aggregate_gen.py not read in this session]

El texto de la definición (`description`, `ai_context`) aparece en la documentación de introspección
de GraphQL, por lo que las herramientas conscientes del esquema y la generación de código lo captan
automáticamente.
[inferred: per REQ-1319: "definition in introspection docs"]

### MCP (agentes de IA)

Dos herramientas exponen métricas a los clientes MCP:

- **`list_metrics`** — devuelve todas las métricas gobernadas visibles para la sesión, con `name`,
  `description` y `ai_context`.
- **`query_metric`** — acepta un nombre de métrica más una lista de dimensiones y llama a la vía
  SQL semántica del compilador, devolviendo el resultado agregado.

[inferred: per REQ-1319: "MCP: list_metrics and query_metric tools carrying ai_context, so agents
select governed meanings instead of composing aggregation SQL"; `provisa/api/mcp/tools.py` not
read in this session]

Los agentes que llaman a `list_metrics` antes de construir una consulta seleccionan una métrica
gobernada por nombre en lugar de escribir SQL de agregación a mano. El campo `ai_context` es el
lugar para colocar el texto de definición que guía la selección correcta.

### Arrow Flight

Las métricas son direccionables como descriptores de vuelo de métrica que devuelven tablas Arrow.
[inferred: per REQ-1319: "Arrow Flight: metric flight descriptors returning Arrow tables";
`provisa/api/flight/catalog.py` not read in this session]

Use la misma forma SQL `metrics.<name>` a través de la vía estándar de ticket Flight SQL.

### Bolt / Cypher (Neo4j Browser)

Llame a una métrica usando el procedimiento `provisa.metric()`:

```cypher
CALL provisa.metric('net_revenue', ['region']) YIELD region, value
```

[inferred: per REQ-1319: "Bolt/Cypher: a provisa.metric() procedure"; the procedure signature
is inferred from the REQ text and not verified against provisa/bolt/session.py in this session]

Las tablas de hechos y dimensiones llevan las etiquetas de nodo `:Fact` y `:Dimension` en el grafo
federado, por lo que Bloom representa la forma de estrella automáticamente.
[inferred: per REQ-1319 and REQ-1320: "federated graph labels nodes :Fact/:Dimension so Bloom
renders the star"; provisa/cypher/label_map.py not read in this session]

### Consultas en lenguaje natural

El comparador de esquemas de LN resuelve el vocabulario de métricas en preguntas en lenguaje
natural directamente a una métrica más dimensiones, y luego genera SQL semántico. [tool-verified: `resolve_metric`,
`provisa/nl/schema_matcher.py` is exercised in `test_nl_metrics.py` lines 76–78:
`sql = matcher.resolve_metric("What is the total revenue by region?")` →
`"SELECT region, value FROM metrics.total_revenue GROUP BY region"`]

Las tablas de hechos se etiquetan como `[fact]` en el prompt de LN; las tablas de dimensión se
etiquetan como `[dimension]`. El comparador sesga las rutas de unión de hecho a dimensión al
resolver preguntas.
[tool-verified: `test_format_entities_tags_star_roles`, `tests/unit/test_nl_metrics.py` lines 129–132:
`assert "table: orders [fact]  fields: amount" in block`]

### Streaming

Combine `view_metrics` con `materialize` y un receptor de Kafka para producir una salida de métrica
de tipo push-on-change usando la maquinaria de materialización existente. No se requiere ningún
pipeline nuevo.
[inferred: per REQ-1319: "Streaming: view_metrics + materialize + Kafka sink yields push-on-change
metrics from existing machinery"; implementation not verified beyond the requirement text]

### Observabilidad (OTel)

Las evaluaciones de métricas se trazan y se pueden exportar como métricas de OpenTelemetry.
[inferred: per REQ-1319: "Observability: metric evaluations traced and exportable as OTel metrics";
OTel integration code not read in this session]
