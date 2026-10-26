# Linaje a nivel de columna {: #column-level-lineage }

Provisa sigue el linaje de datos a nivel de columna de forma estática. Lo calcula a partir de las definiciones SQL y los contratos de los comandos, así que no hace falta ejecutar nada. Tiene dos vistas: un DAG por sentencia y un grafo de procedencia de toda la federación que abarca cada vista y vista materializada (MV) registrada.

## El explorador de linaje {: #the-lineage-explorer }

Abra **Lineage** en la interfaz (`/lineage`). Pegue una sentencia SQL y haga clic en **Build
statement graph** para ver su DAG a nivel de columna. Haga clic en **Federation graph** para cargar
el grafo de procedencia sobre cada MV del registro.

## DAG a nivel de sentencia {: #statement-level-dag }

Cada columna de salida nombrada en su SQL se convierte en un nodo. El constructor la rastrea hacia
atrás a través de cada CTE, subconsulta, join y llamada de comando en línea hasta sus columnas de
origen, construyendo un grafo dirigido desde las entradas de origen hasta las salidas finales.

### Ejemplo resuelto {: #worked-example }

```sql
SELECT o.id, e.embedding, upper(e.geo) AS geo_u
FROM   orders o
JOIN   enrich_grpc_set('main.public.orders') e ON o.id = e.id
```

Esta sentencia produce tres columnas de salida. El grafo para `geo_u` se ve así:

```text
orders.geo  ──[enrich_grpc_set(...)]──►  e.geo  ──[UPPER]──►  geo_u
orders.id   ─╮                                              (taint closure)
orders.region ─╯
```

- `orders.id`, `orders.region` y `orders.geo` son nodos **origen** (el contrato de entrada estrecho
  de `enrich_grpc_set` declara `id` y `region`; el cierre de contaminación (taint closure) completo
  conecta todas las entradas declaradas con todas las salidas).
- `e.embedding` y `e.geo` son nodos **command** en el límite de `enrich_grpc_set`.
- `geo_u` es un nodo **derived** producido por la función SQL `UPPER`.

El límite del comando **no es opaco**. Dado que `enrich_grpc_set` declara sus columnas de entrada
(`id`, `region`) y columnas de salida (`id`, `embedding`, `geo`), el motor de linaje empalma el
cierre de contaminación de forma continua desde las columnas declaradas de la relación de origen
hasta cada salida.

### Tipos de nodo y señales visuales {: #node-kinds-and-visual-cues }

| Tipo de nodo | Color | Significado |
| --- | --- | --- |
| `source` | Verde | Una columna de una tabla base |
| `derived` | Azul | Producida por una expresión SQL (función, operador, CTE) |
| `command` | Púrpura | Una columna de salida de un comando registrado |
| `native` | Naranja | El campo en bruto que lee una fuente, debajo de la columna de una tabla registrada |

Anillos adicionales en un nodo:

- **Anillo naranja** — una columna de salida final de la sentencia.
- **Borde doble** — la relación de la columna es una vista materializada (instantánea de MV/CTAS).
- **Anillo rojo** — miembro de un ciclo clasificado como error.
- **Anillo amarillo** — miembro de un ciclo clasificado como bucle de retroalimentación.

### Transformaciones nombradas en las aristas {: #named-transforms-on-edges }

Cada arista lleva la expresión SQL cruda que produce la columna de destino, además de una lista de
operaciones nombradas: funciones SQL (`sql_function`), operadores aritméticos/lógicos
(`operator`), comandos registrados (`command`), referencias de columna simples (`identity`) y
literales (`constant`).

Una arista proveniente de una llamada de comando se representa como una línea púrpura discontinua
en la interfaz.

### Debajo de la tabla registrada

Un usuario con registro de fuentes ve un salto más debajo de cada columna de origen: el campo tal como lo lee la fuente, en un recuadro para el lugar del que se lee. El recuadro lleva el logotipo y la etiqueta de tipo de la fuente, como los muestra la página Sources, y la dirección según el tipo de fuente:

| Fuente | Dirección |
| --- | --- |
| OpenAPI | La operación (`GET /v2/customers`), sus parámetros fijos y la ubicación de las filas |
| Remote GraphQL | El campo raíz, sus argumentos obligatorios y la ubicación de las filas |
| MongoDB, Cosmos DB | La base de datos y la colección o el contenedor |
| Elasticsearch, Qdrant | El índice o la colección |
| CSV, Parquet, archivos, logs | La ruta, y el patrón de archivos de una tabla que lee muchos archivos |
| Kafka | El topic |
| Fuente de código | La función |
| Cualquier otra | El esquema y la tabla nativos |

El campo en bruto es la ruta de campo de la columna cuando la tiene, si no su propio nombre. La arista de él a la columna registrada nombra el cambio de nombre: `path`, `alias` o `naming` (la convención de nombres). El recuadro de la tabla registrada dice cómo se sirve: `virtual`, leída en cada sentencia, o `replica`, con su TTL y cuándo se cargó por última vez. Nunca se muestran credenciales, referencias a secretos ni el usuario, la contraseña o la query de una URL. Un usuario sin registro de fuentes ve el linaje desde la tabla registrada hacia arriba.

### Enlaces

Cada elemento se abre donde está definido, cuando los permisos del usuario abren esa página. Un clic en un campo abre su tabla, vista o fuente; un doble clic en un recuadro abre su relación; un clic en la arista de un comando abre el comando. Una función SQL integrada no tiene página ni enlace.

## Grafo a escala de la federación {: #federation-wide-graph }

El grafo de la federación fusiona el linaje por sentencia de cada MV registrada en un único grafo de procedencia. La identidad de un nodo es `relation.column`, así que la columna de salida de una vista y la referencia de entrada de otra vista a esa columna se funden en un nodo. El resultado es un único DAG desde las columnas de las fuentes base hasta cada conjunto de datos derivado de la plataforma.

Use `focus`, `direction` y `depth` para acotar la vista a escala de federación sin recalcular el
grafo.

## Ciclos {: #cycles }

El motor de linaje detecta cada ciclo dirigido y lo **clasifica**. Un ciclo se describe y nunca se rechaza.

| Clasificación | Color de borde | Significado |
| --- | --- | --- |
| `feedback` | Amarillo | El ciclo atraviesa un nodo materializado — un bucle de retroalimentación legítimo y desfasado en el tiempo. La instantánea de la MV es el límite de versión que lo hace bien definido. |
| `error` | Rojo | No hay límite de materialización en el bucle — una definición circular sin un orden de evaluación estable. Probablemente un error de diseño. |

Un ciclo `feedback` es un patrón válido. Una MV de enriquecimiento que devuelve una columna derivada a su propia relación de origen funciona mientras un nodo del bucle esté materializado, porque la instantánea separa las dos mitades en el tiempo. Un ciclo `error` necesita el criterio de un operador. Suele significar que dos vistas se referencian entre sí sin una instantánea en medio.

## API

Ambos endpoints son **estáticos**. Leen definiciones y contratos, nunca datos.

### POST /admin/lineage/graph

Devuelve el DAG a nivel de columna para una única sentencia SQL.

```http
POST /admin/lineage/graph
Content-Type: application/json

{
  "sql": "SELECT o.id, e.embedding FROM orders o JOIN enrich_grpc_set('main.public.orders') e ON o.id = e.id",
  "dialect": "postgres"
}
```

Forma de la respuesta:

```json
{
  "nodes": [
    {"id": "orders.id", "column": "id", "relation": "orders", "kind": "source", "materialized": false}
  ],
  "edges": [
    {
      "source": "orders.id",
      "target": "e.id",
      "transform": "enrich_grpc_set(...)",
      "ops": [{"name": "enrich_grpc_set", "kind": "command"}]
    }
  ],
  "outputs": ["id", "embedding"]
}
```

Para quien llama con registro de fuentes, ambos endpoints añaden el salto nativo: nodos de tipo `native` y un objeto `natives` con clave por la relación de cada recuadro nativo (`native:<domain>.<table>`):

```json
"natives": {
  "native:sales.customers": {
    "relation": "sales.customers",
    "source": "crm",
    "type": "openapi",
    "hints": {"brand": "stripe"},
    "address": [
      {"what": "operation", "value": "GET /v2/customers"},
      {"what": "rows", "value": "data.items"}
    ],
    "served": {"mode": "virtual"}
  }
}
```

El `served` de una réplica es `{"mode": "replica", "cache_ttl": 3600, "last_load": "...", "next_load": "..."}`. Para cualquier otro llamante, `natives` está vacío.

Devuelve HTTP 422 cuando el SQL no se puede analizar (parsear).

### GET /admin/lineage/federation

Devuelve el grafo de procedencia combinado sobre todas las MV del registro.

```http
GET /admin/lineage/federation
GET /admin/lineage/federation?focus=orders.id&direction=downstream&depth=3
```

Parámetros de consulta:

| Parámetro | Valores | Predeterminado | Efecto |
| --- | --- | --- | --- |
| `focus` | Un id de nodo | — | Acota la respuesta al subgrafo alrededor de este nodo |
| `direction` | `upstream` \| `downstream` \| `both` | `both` | Dirección de recorrido a partir de `focus` |
| `depth` | entero | sin límite | Distancia máxima de saltos desde `focus` |

La respuesta tiene la misma forma que el grafo de sentencia, con un campo `cycles` añadido
:

```json
{
  "nodes": [...],
  "edges": [...],
  "outputs": [...],
  "cycles": [
    {
      "nodes": ["orders.region", "enriched_orders.region"],
      "has_materialization_boundary": true,
      "classification": "feedback"
    }
  ]
}
```

## De qué se construyó cada refresco {: #what-each-refresh-was-built-from }

Cada refresco de una vista materializada registra de qué se construyó: la versión de la definición de la vista, la versión de sus entradas y cómo se estableció esa versión, y las columnas de origen y la expresión de cada columna de salida. La vista operativa `"ops"."mv_refresh_lineage"` devuelve una fila por refresco:

| Columna | Contiene |
|---|---|
| `mv_id` | La vista materializada refrescada |
| `target_table` | La tabla que escribió el refresco |
| `definition_version` | La versión de la definición de la vista |
| `input_version`, `input_version_kind` | La versión de sus entradas: un snapshot de Iceberg, una marca de agua, un token de frescura o la hora del refresco |
| `trace_id` | La traza del refresco en `"ops"."traces"` |
| `refreshed_at` | Cuándo se ejecutó |
| `column_derivations` | Cada columna de salida con sus columnas de origen y su expresión, como JSON |

```sql
SELECT refreshed_at, definition_version, input_version, input_version_kind
FROM "ops"."mv_refresh_lineage"
WHERE mv_id = 'view-view_exposure'
ORDER BY refreshed_at
```

La vista lee los registros de refresco de `"ops"."traces"` y responde en todo motor que los conserve. En Trino son las tablas Iceberg del catálogo `otel`. En DuckDB el servidor escribe sus propios registros de traza como archivos Parquet en su carpeta de telemetría (`PROVISA_TELEMETRY_DIR`, `~/.provisa/telemetry` por defecto), un archivo por lote exportado en una carpeta por día. La compactación programada de telemetría (`otel.compact_cron`) une los archivos pequeños de cada día en archivos de hasta 64 MB. Cada organización lee solo sus propios refrescos, y los registros se conservan indefinidamente. Los spans solo se registran mientras el SDK de OpenTelemetry está activado (`OTEL_SDK_DISABLED` sin definir o false).

## Una vista tal como estaba y tal como se registró {: #a-view-as-it-stood-and-as-it-was-recorded }

Una lectura simple responde el estado actual. Dos argumentos de la vista, escritos donde la sentencia la nombra, leen su pasado:

- `as_of` lee la vista en un momento, o sobre un rango: `"reproduce"."view_exposure"(as_of => '2026-09-30T12:00:00')`.
- `as_recorded` la lee tal como Provisa la había registrado en un momento. Las versiones registradas después quedan fuera, así que una corrección registrada más tarde no cambia una reproducción del pasado.

A 30 de septiembre y registrada el 1 de octubre da la cifra original; registrada el 5 de octubre da la corrección. El registro de auditoría de cada sentencia guarda ambos tiempos (`as_of`, `as_recorded`), de modo que un informe se puede reproducir tal como se produjo por primera vez. [Vistas con historial](configuration.md#views-with-history-time-travel) explica las tres declaraciones, con ejemplos, y cada superficie que acepta los dos valores.

## Qué rompería un renombrado o una eliminación de columna {: #what-a-column-rename-or-drop-would-break }

Una columna lleva dos nombres, y cada uno lo almacena un conjunto distinto de artefactos.

El **nombre expuesto** es lo que muestran las superficies SQL y GraphQL: `table_columns.alias`, con
respaldo en el valor predeterminado snake_case cuando no hay alias definido. Las vistas, vistas materializadas, expresiones de
métricas, predicados de RLS, contratos de DQ, granularidades de vistas de métricas y claves de fila
de MV se escriben todos contra ese nombre, así que **renombrar un alias los rompe con la misma
certeza que eliminar la columna**.

El **nombre físico** es `table_columns.column_name`, la identidad que sobrevive al reemplazo
completo de columnas del upsert de la tabla. Las relaciones, los enlaces de la [ontología](ontology.md), las
asignaciones de etiquetas, la columna de marca de agua (watermark) y los ajustes preestablecidos de
columna almacenan este, así que solo se rompen cuando la columna se **elimina**.

`columnDependents` reporta ambos casos. Las vistas y MV descendientes provienen de recortar el
grafo de federación en el nombre expuesto de la columna; los artefactos que ese grafo no cubre
provienen de un escaneo directo del registro.

```graphql
query {
  columnDependents(tableId: "42", renamed: ["order_total"], removed: ["legacy_code"]) {
    columnName
    dependents { kind name detail breaksOn }
  }
}
```

`breaksOn` es `rename` para una referencia por nombre expuesto y `remove` para una por nombre
físico, de modo que quien llama pueda saber a cuál de las dos mitades de la edición está
reaccionando cada artefacto.

Consulte esto **antes** de guardar. Una columna renombrada se localiza por el nombre expuesto que
aún conserva en el registro; una vez que el alias se ha aplicado, el nombre anterior desaparece y
la consulta no encuentra nada.

La página de tablas ejecuta la consulta automáticamente cuando una edición pendiente cambia un alias o reduce el conjunto de columnas, y enumera lo que encuentra. El aviso es orientativo. Nombra los artefactos afectados, y el administrador decide. Nunca bloquea el guardado, porque no se puede llegar a todos los consumidores del patrimonio: un panel externo o una aplicación cliente que consulta la columna por su nombre queda fuera del conocimiento del registro. Por el mismo motivo, los análisis sobre texto SQL libre buscan la columna como token identificador sin resolver el ámbito, así que pueden nombrar un artefacto que al final no usa la columna. En un aviso, informar de más es la dirección segura.

## Usar el linaje para gobernar contratos de comandos {: #using-lineage-to-govern-command-contracts }

Dado que el cierre de contaminación conecta cada columna de entrada declarada con cada columna de
salida declarada, la amplitud de ese cierre depende enteramente de lo que usted declare.

Piense en un comando que recibe una tabla de pedidos completa (`id`, `region`, `amount`, `customer_id`, `discount`, `notes`, ...) y devuelve un `embedding`. Si el contrato de entrada enumera todas esas columnas, cada columna posterior que use el embedding muestra linaje desde todas ellas. Es exacto, pero no ayuda, porque no puede saber qué importó de verdad.

Declare solo `id` y `text` (las columnas que el modelo de embedding realmente lee), y el cono de
linaje se reduce a esas dos columnas de origen. La derivación es a la vez sólida y precisa.

Consulte [Commands](commands.md) para conocer la mecánica de declarar un contrato de entrada
estrecho.
