# Linaje a nivel de columna

Provisa rastrea el linaje de datos a nivel de columna de forma estática — calculado a partir de las
definiciones SQL y los contratos de comandos, sin necesidad de ejecución. Hay dos vistas
disponibles: un DAG por sentencia y un grafo de procedencia a escala de la federación que abarca
todas las vistas y vistas materializadas (MV) registradas.

## El explorador de linaje

Navegue a **Lineage** en la interfaz (`/lineage`). Pegue una sentencia SQL y haga clic en **Build
statement graph** para ver su DAG a nivel de columna. Haga clic en **Federation graph** para cargar
el grafo de procedencia sobre cada MV del registro. [tool-verified: LineagePage.tsx:28-119]

## DAG a nivel de sentencia (REQ-1160)

Cada columna de salida nombrada en su SQL se convierte en un nodo. El constructor la rastrea hacia
atrás a través de cada CTE, subconsulta, join y llamada de comando en línea hasta sus columnas de
origen, construyendo un grafo dirigido desde las entradas de origen hasta las salidas finales.

### Ejemplo resuelto

```sql
SELECT o.id, e.embedding, upper(e.geo) AS geo_u
FROM   orders o
JOIN   enrich_grpc_set('main.public.orders') e ON o.id = e.id
```

Esta sentencia produce tres columnas de salida. El grafo para `geo_u` se ve así:

```
orders.geo  ──[enrich_grpc_set(...)]──►  e.geo  ──[UPPER]──►  geo_u
orders.id   ─╮                                              (taint closure)
orders.region ─╯
```

- `orders.id`, `orders.region` y `orders.geo` son nodos **origen** (el contrato de entrada estrecho
  de `enrich_grpc_set` declara `id` y `region`; el cierre de contaminación (taint closure) completo
  conecta todas las entradas declaradas con todas las salidas). [tool-verified: `_splice_commands`
  en graph.py:223-242]
- `e.embedding` y `e.geo` son nodos **command** — el límite de `enrich_grpc_set`.
- `geo_u` es un nodo **derived** producido por la función SQL `UPPER`.

El límite del comando **no es opaco**. Dado que `enrich_grpc_set` declara sus columnas de entrada
(`id`, `region`) y columnas de salida (`id`, `embedding`, `geo`), el motor de linaje empalma el
cierre de contaminación de forma continua desde las columnas declaradas de la relación de origen
hasta cada salida. [tool-verified: `_splice_commands` y `_input_relation` en graph.py:245-271]

### Tipos de nodo y señales visuales

[tool-verified: LineageDag.tsx:25-29, KIND_COLOR constants; LineagePage.tsx:21-26 LEGEND]

| Tipo de nodo | Color | Significado |
|---|---|---|
| `source` | Verde | Una columna de una tabla base |
| `derived` | Azul | Producida por una expresión SQL (función, operador, CTE) |
| `command` | Púrpura | Una columna de salida de un comando registrado |

Anillos adicionales en un nodo:

- **Anillo naranja** — una columna de salida final de la sentencia.
- **Borde doble** — la relación de la columna es una vista materializada (instantánea de MV/CTAS).
- **Anillo rojo** — miembro de un ciclo clasificado como error.
- **Anillo amarillo** — miembro de un ciclo clasificado como bucle de retroalimentación.

[tool-verified: LineageDag.tsx:88-103 Cytoscape style selectors]

### Transformaciones nombradas en las aristas

Cada arista lleva la expresión SQL cruda que produce la columna de destino, además de una lista de
operaciones nombradas: funciones SQL (`sql_function`), operadores aritméticos/lógicos
(`operator`), comandos registrados (`command`), referencias de columna simples (`identity`) y
literales (`constant`). [tool-verified: TransformOp and name_transform en graph.py:36-145]

Una arista proveniente de una llamada de comando se representa como una línea púrpura discontinua
en la interfaz. [tool-verified: LineageDag.tsx:122-124]

## Grafo a escala de la federación (REQ-1161)

El grafo de federación combina el linaje por sentencia de cada MV registrada en un único grafo de
procedencia. La identidad del nodo es `relation.column` — la columna de salida de una vista y la
referencia de entrada de otra vista a la misma columna colapsan en un solo nodo. El resultado es un
único DAG desde las columnas de origen base hasta cada conjunto de datos derivado en la plataforma.
[tool-verified: `build_federation_graph` en merge.py:205-229 y `qualify_outputs` en
graph.py:275-299]

Use `focus`, `direction` y `depth` para acotar la vista a escala de federación sin recalcular el
grafo. [tool-verified: `slice_graph` en merge.py:160-189]

## Ciclos (REQ-1161)

Los ciclos se describen, no se rechazan. El motor de linaje detecta cada ciclo dirigido y lo
**clasifica**. [tool-verified: `Cycle.classification` property en merge.py:43-46]

| Clasificación | Color de borde | Significado |
|---|---|---|
| `feedback` | Amarillo | El ciclo atraviesa un nodo materializado — un bucle de retroalimentación legítimo y desfasado en el tiempo. La instantánea de la MV es el límite de versión que lo hace bien definido. |
| `error` | Rojo | No hay límite de materialización en el bucle — una definición circular sin un orden de evaluación estable. Probablemente un error de diseño. |

[tool-verified: LineagePage.tsx:83-98 cycle alert rendering; merge.py:38-48]

Un ciclo `feedback` no es un fallo. Una MV de enriquecimiento que retroalimenta una columna
derivada hacia su propia relación de origen es un patrón válido siempre que un nodo del bucle esté
materializado — la instantánea aísla las dos mitades temporalmente. Un ciclo `error` requiere
criterio del operador: normalmente significa que dos vistas se referencian mutuamente sin ninguna
instantánea de por medio.

## API

Ambos endpoints son **estáticos** — leen definiciones y contratos, no datos.

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

[tool-verified: `lineage_graph` endpoint at lineage_router.py:45-54, LineageGraphRequest model at
lineage_router.py:29-31]

Forma de la respuesta [tool-verified: `LineageGraph.to_dict` en graph.py:82-105]:

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

Devuelve HTTP 422 cuando el SQL no se puede analizar (parsear).
[tool-verified: lineage_router.py:51-54]

### GET /admin/lineage/federation

Devuelve el grafo de procedencia combinado sobre todas las MV del registro.

```
GET /admin/lineage/federation
GET /admin/lineage/federation?focus=orders.id&direction=downstream&depth=3
```

[tool-verified: `federation_graph` endpoint at lineage_router.py:73-98]

Parámetros de consulta [tool-verified: function signature at lineage_router.py:73-76]:

| Parámetro | Valores | Predeterminado | Efecto |
|---|---|---|---|
| `focus` | Un id de nodo | — | Acota la respuesta al subgrafo alrededor de este nodo |
| `direction` | `upstream` \| `downstream` \| `both` | `both` | Dirección de recorrido a partir de `focus` |
| `depth` | entero | sin límite | Distancia máxima de saltos desde `focus` |

La respuesta tiene la misma forma que el grafo de sentencia, con un campo `cycles` añadido
[tool-verified: `MergedGraph.to_dict` en merge.py:60-64]:

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

## Usar el linaje para gobernar contratos de comandos

Dado que el cierre de contaminación conecta cada columna de entrada declarada con cada columna de
salida declarada, la amplitud de ese cierre depende enteramente de lo que usted declare.

Considere un comando que toma una tabla `orders` completa (`id`, `region`, `amount`,
`customer_id`, `discount`, `notes`, ...) y devuelve un `embedding`. Si el contrato de entrada
enumera todas esas columnas, cada columna posterior que use el embedding mostrará linaje de todas
ellas. Eso es exacto pero no útil — es difícil saber qué fue lo que realmente importó.

Declare solo `id` y `text` (las columnas que el modelo de embedding realmente lee), y el cono de
linaje se reduce a esas dos columnas de origen. La derivación es a la vez sólida y precisa.

Consulte [Commands](commands.md) para conocer la mecánica de declarar un contrato de entrada
estrecho.
