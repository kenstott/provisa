# Compatibilidad con consultas Cypher {: #cypher-query-support }

Provisa traduce un subconjunto de openCypher a SQL mediante el módulo `provisa/cypher/`. Las consultas se analizan con un analizador de descenso recursivo personalizado (sin biblioteca externa de Cypher), se resuelven contra el esquema del modelo y se emiten como SQL, que luego se enruta al motor de ejecución de destino.

## Funcionalidades implementadas {: #implemented-features }

### Cláusulas {: #clauses }

| Cláusula | Estado | Notas |
| -------- | -------- | ------- |
| `MATCH (n:Label)` | ✓ | Patrones de nodo con etiquetas, variables, propiedades en línea |
| `OPTIONAL MATCH` | ✓ | Emite LEFT JOIN |
| `WHERE` | ✓ | Compatibilidad completa de expresiones; se aplica después de MATCH |
| `RETURN` | ✓ | Comodín, acceso a propiedades, expresiones, alias |
| `RETURN DISTINCT` | ✓ | Emite SELECT DISTINCT |
| `WITH` | ✓ | Emite una CTE con nombre (`_w0`, `_w1`, …); admite `WITH … WHERE` |
| `ORDER BY` | ✓ | ASC / DESC |
| `SKIP` / `LIMIT` | ✓ | Se asigna a SQL OFFSET / LIMIT |
| `UNION` / `UNION ALL` | ✓ | Unión recursiva entre sub-AST |
| `CALL { … }` | ✓ | Descomposición de subconsultas CALL de nivel superior mediante `cypher_calls_to_sql_list` |
| `CALL { WITH x … }` | ✓ | Subconsulta correlacionada → `CROSS JOIN LATERAL`; véase §CALL correlacionado |
| `CALL db.labels()` | ✓ | Devuelve las etiquetas de nodo del modelo; sin traducción a SQL |
| `CALL db.relationshipTypes()` | ✓ | Devuelve los tipos de relación del modelo |
| `CALL db.propertyKeys()` | ✓ | Devuelve todos los nombres de claves de propiedad de todos los tipos de nodo |
| `UNWIND` | ✓ | Expansión de arreglo a filas; el primer elemento se convierte en FROM, los siguientes en CROSS JOIN UNNEST |

### Patrones de coincidencia {: #match-patterns }

| Patrón | Estado | Notas |
| --------- | -------- | ------- |
| `(n)` — nodo sin etiqueta | ✓ | UNION ALL sobre todos los tipos conocidos |
| `(n:Label)` | ✓ | Se asigna a la tabla registrada para ese tipo GraphQL |
| `(n:Label {prop: val})` | ✓ | El filtro de propiedad en línea se convierte en WHERE |
| `(a)-[:TYPE]->(b)` | ✓ | Dirigido, un solo salto |
| `(a)<-[:TYPE]-(b)` | ✓ | Recorrido inverso; columnas de unión invertidas |
| `(a)-[]->(b)` | ✓ | Cualquier relación dirigida a→b; UNION ALL si coinciden varios tipos |
| `(a)-[]-(b)` | ✓ | Bidireccional; se expande a UNION ALL de todas las relaciones directas e inversas |
| `(a)-[:TYPE*..N]->(b)` | ✓ | Longitud variable con límite superior; CTE recursiva para casos autorreferenciales, JOIN plano en el resto |
| `(a)-[]->(b)-[]->(c)` | ✓ | JOIN encadenados de múltiples saltos |
| `(n:DomainLabel)` | ✓ | Etiqueta de dominio → subconsulta UNION ALL sobre todos los tipos del dominio |
| `(n:A\|B)` | ✓ | Alternancia de etiquetas → dominio ad hoc inyectado en el mapa de etiquetas; UNION ALL sobre los tipos coincidentes |
| `shortestPath(…)` | ✓ | JOIN plano para extremos heterogéneos; CTE WITH RECURSIVE para casos del mismo tipo/autorreferenciales |
| `allShortestPaths(…)` | ✓ | Igual que shortestPath pero sin LIMIT 1 |

### Expresiones y predicados {: #expressions-and-predicates }

| Funcionalidad | Estado | Asignación SQL |
| --------- | -------- | ------------ |
| Acceso a propiedad `n.prop` | ✓ | `n."prop"` |
| Parámetros `$name` | ✓ | Posicional `$N` |
| Parámetros heredados `{name}` | ✓ | Normalizado a `$name` en el momento del análisis |
| Comparación `=`, `<>`, `<`, `>`, `<=`, `>=` | ✓ | Directa |
| `AND`, `OR`, `NOT` | ✓ | Directa |
| `IS NULL` / `IS NOT NULL` | ✓ | Directa |
| `IN [list]` | ✓ | SQL IN; la sintaxis de corchetes `[...]` de Cypher se reescribe como `(...)` |
| `STARTS WITH` | ✓ | `starts_with(col, val)` |
| `ENDS WITH` | ✓ | `col LIKE CONCAT('%', val)` |
| `CONTAINS` | ✓ | `strpos(col, val) > 0` |
| `=~` regex | ✓ | `regexp_like(col, pattern)` |
| `exists(n.prop)` | ✓ | `(n.prop) IS NOT NULL` |
| `EXISTS { MATCH … }` | ✓ | Subconsulta correlacionada `EXISTS (SELECT 1 FROM …)` |
| `COUNT { MATCH … }` | ✓ | Subconsulta correlacionada `(SELECT count(*) FROM …)` |
| `COLLECT { MATCH … RETURN x }` | ✓ | Subconsulta correlacionada `ARRAY(SELECT x FROM …)` |
| `id(n)` | ✓ | Se resuelve a la columna de ID configurada del nodo |
| `labels(n)` | ✓ | `ARRAY['Label']` |
| `keys(n)` | ✓ | `ARRAY['prop1', 'prop2', …]` |
| `type(r)` | ✓ | Se resuelve en tiempo de compilación al literal de cadena `'REL_TYPE'`; sin columna en tiempo de ejecución |
| `length(p)` | ✓ | `_t.hops` para rutas de CTE recursiva; `1` para rutas de JOIN plano |
| `CASE WHEN … THEN … ELSE … END` | ✓ | Directa (formas buscada y simple) |
| GROUP BY implícito | ✓ | Los elementos de RETURN no agregados se convierten en claves de GROUP BY cuando algún elemento tiene un agregado |

### Proyecciones de mapa {: #map-projections }

| Sintaxis | Asignación SQL |
| -------- | ------------ |
| `n { .prop1, .prop2 }` | `MAP(ARRAY['prop1','prop2'], ARRAY[n."prop1",n."prop2"])` |
| `n { .* }` | `MAP(ARRAY[all props...], ARRAY[n."col",...])` — expandido desde el esquema |
| `n { .*, extra: expr }` | Todas las propiedades del esquema más la clave nombrada; MAP combinado |
| `n { key: expr }` | `MAP(ARRAY['key'], ARRAY[expr])` |

**Propiedades de objeto.** Una columna que contiene un objeto declarado (el objeto incrustado de un tipo GraphQL remoto, o cualquier columna registrada con campos de objeto) es una propiedad de mapa: `RETURN n.address` la devuelve como mapa, sea cual sea la forma en que la devuelva el motor, y el nodo `n` la lleva entre sus propiedades. Un campo GraphQL remoto que apunta a un tipo registrado, un objeto o una lista, no es una propiedad; se alcanza por su relación.

### Funciones de agregación {: #aggregation-functions }

| Cypher | SQL |
| -------- | ----- |
| `count(*)`, `count(x)` | directa |
| `count(DISTINCT x)` | `count(DISTINCT x)` |
| `collect(x)` | `array_agg(x)` |
| `avg`, `sum`, `min`, `max` | directa |
| `stDev(x)` | `stddev_samp(x)` |
| `stDevP(x)` | `stddev_pop(x)` |
| `percentileCont(x, p)` | `approx_percentile(x, p)` |
| `percentileDisc(x, p)` | `approx_percentile(x, p)` |

### Funciones de cadena {: #string-functions }

| Cypher | SQL |
| -------- | ----- |
| `toLower(x)` | `lower(x)` |
| `toUpper(x)` | `upper(x)` |
| `ltrim(x)`, `rtrim(x)`, `trim(x)` | directa |
| `replace(x, a, b)` | directa |
| `reverse(x)` | directa |
| `split(x, d)` | directa |
| `left(x, n)` | `left(x, n)` |
| `right(x, n)` | `right(x, n)` |
| `substring(x, start, len)` | `substr(x, start+1, len)` (índice 0→1) |
| `size(string)` | `char_length(string)` |
| `size(list)` | `cardinality(list)` |

### Funciones de conversión de tipos {: #type-conversion-functions }

| Cypher | SQL |
| -------- | ----- |
| `toString(x)` | `CAST(x AS VARCHAR)` |
| `toInteger(x)` | `TRY_CAST(x AS BIGINT)` |
| `toFloat(x)` | `TRY_CAST(x AS DOUBLE)` |
| `toBoolean(x)` | `TRY_CAST(x AS BOOLEAN)` |
| `toStringOrNull`, `toIntegerOrNull`, `toFloatOrNull`, `toBooleanOrNull` | variantes de `TRY_CAST` |

### Funciones matemáticas {: #math-functions }

| Cypher | SQL |
| -------- | ----- |
| `log(x)` | `ln(x)` (logaritmo natural) |
| `log2(x)` | `log2(x)` |
| `range(start, end)` | `sequence(start, end)` |
| `abs`, `sqrt`, `ceil`, `floor`, `round`, `sign` | se pasan directamente |

### Funciones de lista {: #list-functions }

| Cypher | SQL |
| -------- | ----- |
| `head(list)` | `element_at(list, 1)` |
| `last(list)` | `element_at(list, -1)` |
| `tail(list)` | `slice(list, 2, cardinality(list))` |
| `isEmpty(list)` | `cardinality(list) = 0` |

### Comprensiones de lista {: #list-comprehensions }

| Sintaxis | Asignación SQL |
| -------- | ------------ |
| `[x IN list \| f(x)]` | `transform(list, x -> f(x))` |
| `[x IN list WHERE p(x)]` | `filter(list, x -> p(x))` |
| `[x IN list WHERE p(x) \| f(x)]` | `transform(filter(list, x -> p(x)), x -> f(x))` |
| `any(x IN list WHERE p(x))` | `any_match(list, x -> p(x))` |
| `all(x IN list WHERE p(x))` | `all_match(list, x -> p(x))` |
| `none(x IN list WHERE p(x))` | `none_match(list, x -> p(x))` |
| `single(x IN list WHERE p(x))` | `cardinality(filter(list, x -> p(x))) = 1` |
| `reduce(acc = init, x IN list \| expr)` | `reduce(list, init, (acc, x) -> expr, acc -> acc)` |

### Comprensiones de patrón {: #pattern-comprehensions }

| Sintaxis | Asignación SQL |
| -------- | ------------ |
| `[(a)-[:R]->(b) \| b.prop]` | `ARRAY(SELECT b."prop" FROM ... WHERE a.fk = b.pk)` |
| `[(a)-[]->(b:Label) \| b.prop]` | tipo inferido a partir del modelo; misma forma de subconsulta ARRAY |

### Subconsultas CALL correlacionadas {: #correlated-call-subqueries }

`CALL { WITH x MATCH (x)-[:R]->(n) RETURN n.prop AS alias }` se traduce a `CROSS JOIN LATERAL (SELECT n."prop" AS alias FROM ... WHERE x."pk" = n."fk")`. Reglas:

- La variable de ámbito externo (`x`) debe aparecer en `WITH`
- Se admiten varias variables importadas (`WITH a, b`)
- La primera relación en el MATCH interno cuyo origen es una variable ligada por lateral determina el `FROM` interno y la condición de unión
- Los bloques `CALL { ... }` de nivel superior no correlacionados (sin `WITH`) se gestionan mediante `cypher_calls_to_sql_list`

---

## Escrituras {: #writes }

Cypher admite tres patrones de escritura a través del endpoint `/data/cypher`, ejecutados por `provisa/cypher/write_translator.py`.

| Cypher | SQL |
|---|---|
| `CREATE (n:Label {props})` | `INSERT INTO catalog.schema.table (cols) VALUES (vals)` |
| `MATCH (n:Label) WHERE … DELETE n` | `DELETE FROM catalog.schema.table WHERE …` |
| `MATCH (n:Label) WHERE … SET n.prop = val, …` | `UPDATE catalog.schema.table SET col = val, … WHERE …` |

Los nombres de propiedad se asignan a columnas mediante la eliminación del prefijo de dominio y la resolución de alias; los valores escalares de Cypher se convierten al tipo de columna de destino. El cuerpo de la respuesta incluye un recuento `affected_rows`.

La propiedad de un nodo es el nombre publicado de su columna: el alias de la columna cuando lo tiene, si no el nombre según la convención de nomenclatura —el nombre con el que la leen GraphQL y SQL— en `MATCH`, `RETURN`, `CREATE`, `SET` y `MERGE` por igual.

Reglas:

- La etiqueta debe resolverse a exactamente una tabla registrada. Las etiquetas ambiguas o desconocidas son errores irrecuperables; no hay coincidencia aproximada. No se pueden crear nuevas etiquetas ni tipos a través de Cypher.
- Cada escritura está condicionada a los permisos de escritura de la tabla de destino: el rol necesita el derecho `write` y un permiso para la operación, y las columnas escritas deben estar en el permiso. Un rol que no los tiene se rechaza en tiempo de compilación.
- El conector del origen de datos subyacente debe admitir DML. Los orígenes de solo lectura (federados con Trino, Iceberg sin conector Delta) rechazan las escrituras en tiempo de traducción.
- No se pueden escribir relaciones: se derivan de las uniones declaradas en el modelo, no se almacenan como aristas. Apuntar a una relación es un error irrecuperable. Una arista respaldada por una tabla de unión (junction) no es una excepción: la tabla asociativa que hay detrás es a su vez una tabla registrada, y las filas se escriben en esa tabla, no en la arista.
- Las escrituras se ejecutan a través del pipeline de escritura completo: inyección de RLS y hooks posteriores a la mutación (invalidación de caché de respuesta, marcado de vista materializada como obsoleta, eventos de cambio de Kafka, recarga de tabla activa).
- `MERGE`, `DETACH DELETE` y `REMOVE` no son compatibles y se rechazan en tiempo de análisis.

---

## Acceso por protocolo {: #protocol-access }

Cypher llega al mismo pipeline gobernado a través de dos transportes:

- **HTTP** — `POST /data/cypher` con un cuerpo JSON (`{"query": "...", "params": {...}}`). Devuelve filas tipadas, o `affected_rows` para escrituras. Las variables de grafo en la cláusula `RETURN` se serializan como JSON: los nodos llevan `id`, `label`, `tableLabel` y `properties`; las aristas llevan `identity`, `start`, `end`, `type`, `properties`, `startNode` y `endNode`; las rutas llevan `nodes`, `edges` y `length`/`hops`. Los comandos registrados también se pueden invocar aquí mediante `CALL fn(args) YIELD col1, col2`, y los argumentos posicionales se asignan a los nombres de argumento declarados del comando, en orden.
- **Bolt** — un servidor de protocolo binario compatible con Neo4j (códec PackStream, framing por fragmentos) que permite a Neo4j Browser, Bloom y los controladores Bolt ejecutar Cypher sobre el grafo federado. Se inicia cuando `PROVISA_BOLT_PORT` se establece con un valor distinto de cero y está deshabilitado de forma predeterminada; configure `PROVISA_BOLT_CERT` / `PROVISA_BOLT_KEY` para TLS. La autenticación de Bolt asigna el principal a un usuario y la base de datos a un rol: `SHOW DATABASES` lista una entrada por cada par (vista × rol), nombrada `provisa_<role>` (dominios de negocio) o `provisa_ops_<role>` (con dominios system/meta/ops); `:use` selecciona el rol y la vista activos. Las relaciones reciben ID enteros duraderos mediante una tabla `rel_ids`, siguiendo el diseño de `node_ids`. Los comandos registrados se pueden invocar con `CALL command(args)`, y los argumentos posicionales se asignan a los nombres de argumento declarados, en orden; los procedimientos `CALL dbms.*` / `CALL db.*` tienen prioridad.

### Analítica de grafos {: #graph-analytics }

`POST /data/graph-analytics` ejecuta una consulta Cypher, construye un grafo NetworkX en memoria a partir de los nodos y aristas resultantes, ejecuta un algoritmo con nombre y combina un diccionario `_analytics` en cada nodo y arista antes de devolverlos como JSON con un campo `elapsed_ms`. Las claves de `_analytics` varían según el algoritmo: la centralidad produce `score`; la detección de comunidades produce `cluster`; k-core produce `core_number`; la centralidad de grado añade `in_degree` y `out_degree`. El endpoint rechaza grafos por encima de un tamaño configurable (10.000 nodos / 50.000 aristas de forma predeterminada) con HTTP 413; Girvan-Newman está limitado a 500 nodos salvo que quien llama pase `force=true`.

---

## Limitaciones {: #limitations }

### Restricciones de diseño {: #design-constraints }

1. **Las escrituras se limitan a `CREATE`, `SET` y `DELETE`.** Se ejecutan como escrituras directas de tabla a través del mismo pipeline que las mutaciones de GraphQL y SQL. Véase §Escrituras más arriba. `MERGE`, `DETACH DELETE` y `REMOVE` se rechazan en tiempo de análisis. Los procedimientos APOC también se rechazan.

2. **Las propiedades de relación solo existen en aristas respaldadas por una tabla de unión.** Una arista declarada sobre un par de columnas de clave foránea existe solo como metadatos de join en el modelo y no guarda atributos, así que `WHERE r.since > 2020` o `RETURN r.weight` no significan nada sobre ella. Una arista declarada sobre una tabla de unión sí los lleva. Las columnas restantes de la tabla asociativa son las propiedades de la relación, y `RETURN r` las devuelve. Un `WHERE` sobre una de ellas se compila como un predicado sobre el alias de la unión, así que restringe el propio recorrido en lugar de filtrar filas ya ensambladas. La tabla de unión se quita del lado de los nodos del esquema del grafo: aquí es una arista y en todo lo demás una tabla.

3. **El recorrido bidireccional** `(a)-[]-(b)` se reescribe como la UNION ALL hacia delante y hacia atrás de cada relación dirigida coincidente del modelo. Toda relación del modelo tiene dirección, y la sintaxis bidireccional es azúcar que se expande a ambas. Las ramas adicionales se emiten en el nivel más externo de la consulta. Un patrón MATCH posterior de la misma consulta no se duplica entre las ramas, lo que limita los patrones bidireccionales en una consulta con varias cláusulas MATCH.

4. **Las rutas recursivas requieren un límite.** Los patrones de longitud variable (`[*]`) deben incluir un límite superior (por ejemplo, `[*..10]`). El recorrido sin límite se rechaza en tiempo de análisis para evitar CTE recursivas descontroladas.

### Notas de comportamiento {: #behaviour-notes }

5. **`shortestPath` en rutas no autorreferenciales usa un JOIN plano sin orden por saltos.** Cuando los tipos de inicio y fin difieren y el esquema no tiene ninguna relación autorreferencial, el traductor emite una cadena de JOIN plana a lo largo de la ruta de esquema más corta. Esa vía de código no sigue los saltos, así que no emite `ORDER BY hops`. El resultado es la ruta de esquema estructuralmente más corta. Puede diferir de la ruta más corta a través de los datos en varias filas.

6. **Varias rutas de esquema producen `UNION ALL`.** Cuando dos rutas de esquema con el mismo número de saltos conectan los mismos tipos de inicio y fin (por ejemplo, `Person -[WORKS_AT]-> Company` y `Person -[MANAGES]-> Company`), ambas se emiten como ramas `UNION ALL`. No se realiza deduplicación de filas que aparecen en ambas ramas.

7. **Un `RelationshipMapping` por combinación de par origen→destino y rel\_type.** Si dos campos GraphQL en el mismo tipo de origen producen la misma cadena `rel_type` (tras convertirla a mayúsculas) hacia el mismo tipo de destino, el segundo registro sobrescribe al primero en `CypherLabelMap.relationships`. La clave de la relación incluye los nombres de los tipos de origen y destino, por lo que pares origen/destino distintos con el mismo nombre de tipo obtienen cada uno su propia entrada y no se ven afectados.

8. **Las CTE de la cláusula `WITH` se nombran `_w0`, `_w1`, …** Los nombres se asignan posicionalmente dentro de una única llamada de traducción. Componer varias consultas traducidas (por ejemplo, en un lote) puede producir colisiones de nombres de CTE si se concatenan sin cuidado.

### Cobertura de expresiones y patrones {: #expression-and-pattern-coverage }

Las expresiones Cypher se analizan en un AST y se reducen nodo a nodo a SQL (`provisa/cypher/expr_parser.py`, `provisa/gql/expr_visitor.py`). La gramática sigue la torre de precedencia `oC_Expression` de openCypher. Compatible: literales, parámetros, acceso a propiedades, `n.prop`, índice y segmento (slice), aritmética (`+ - * / % ^`), comparación, `IN`, `STARTS WITH` / `ENDS WITH` / `CONTAINS` / `=~`, `IS [NOT] NULL`, booleanos `AND` / `OR` / `XOR` / `NOT`, `CASE`, literales de lista y mapa, comprensiones de lista y patrón (incluyendo el enlace de ruta `p = (…)`), proyección de mapa, `reduce`, los cuantificadores `all` / `any` / `none` / `single`, subconsultas existenciales y llamadas a función.

9. **Las etiquetas son fijas; no se pueden crear tipos de objeto a través de Cypher.** Una etiqueta se resuelve en un dominio conocido, un tipo de objeto conocido o un `domain:object_type` cualificado, que es el conjunto cerrado que define el esquema registrado. Cypher nunca introduce una etiqueta ni un tipo nuevos. Solo puede crear instancias de tipos ya definidos en una fuente de datos con escritura: `CREATE` escribe filas en esa tabla (vea §Writes) y no puede definir una etiqueta ni un tipo nuevos. Se aceptan las dos formas de etiqueta y significan la misma prueba, la postfija `n:Label` y la extensa `n IS :Label` (con su negación `n IS NOT :Label`). Una etiqueta cualificada se escribe `n:domain:object_type`.

10. **`shortestPath` y `allShortestPaths` solo funcionan dentro de `MATCH`.** En un patrón (`MATCH p = shortestPath((a:Person)-[:KNOWS*..5]->(b:Person))`) se traducen a una CTE `WITH RECURSIVE` y necesitan nodos de origen y destino con etiqueta. No se admiten en posición de expresión, por ejemplo `RETURN shortestPath((a)-[*]->(b))` o `WHERE length(shortestPath((a)-[*]->(b))) < 5`. La reescritura recursiva parte de la cláusula `MATCH`, y no hay una subconsulta correlacionada que la lleve.

11. **Las comprensiones de lista, `REDUCE` y los cuantificadores operan sobre valores de lista; las comprensiones de patrón recorren el grafo.** `reduce(...)`, `all/any/none/single(...)` y la comprensión de lista `[x IN list | …]` trabajan sobre una expresión de lista y se reducen a las funciones de lista de orden superior del motor. Por sí mismas no recorren el grafo. La comprensión de **patrón** `[(a)-[:R]->(b) WHERE p | e]` sí recorre: su patrón de grafo se trata como una subconsulta correlacionada, así que su origen es un recorrido. Para alimentar las formas de lista con resultados de recorrido, use `nodes(p)` / `relationships(p)` / `collect(...)`, o use directamente una comprensión de patrón.
