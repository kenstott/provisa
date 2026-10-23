# Referencia de la API {: #api-reference }

## Descripción general {: #overview }

Provisa expone endpoints REST bajo dos prefijos: `/data` para la ejecución de consultas y la introspección de esquemas, y `/admin` para la gestión de configuración. La mayoría de los endpoints de datos requieren un identificador de rol. Las operaciones de configuración de administración usan una API de Strawberry GraphQL en `/admin/graphql`.

---

## Autenticación {: #authentication }

Cuando `auth.provider` está configurado en `provisa.yaml`, todos los endpoints excepto `/health` y `/setup/status` requieren un encabezado `Authorization: Bearer <token>`.

Sin autenticación configurada, el servidor se ejecuta en modo de desarrollo. Cualquier solicitud se trata como la identidad `anonymous`, que se asigna a todos los roles configurados con acceso de dominio comodín.

**Inicio de sesión (`POST /auth/login`)** lo proporciona el proveedor de autenticación activo cuando `provider: basic` está configurado. El formato de credenciales y la respuesta dependen del proveedor.

**Introspección de identidad:**

```http
GET /auth/me
```

Devuelve el id, correo electrónico, nombre para mostrar, membresías de organización y asignaciones de rol del usuario autenticado. En modo de desarrollo devuelve `dev_mode: true` con todos los IDs de rol listados.

```http
GET /auth/provider-type
```

Devuelve `{"provider": "<name>"}` o `{"provider": null}` cuando la autenticación no está configurada.

---

## Endpoints de datos {: #data-endpoints }

### `POST /data/graphql`

Ejecuta una consulta o mutación GraphQL.

**Cuerpo de la solicitud:**

```json
{
  "query": "{ orders(where: {region: {eq: \"us\"}}) { id amount } }",
  "variables": {},
  "role": "org_admin",
  "extensions": {}
}
```

La solicitud se ejecuta con su rol actuante: el rol del usuario autenticado o, en modo de desarrollo (sin autenticación), la cabecera `X-Provisa-Role`, `org_admin` cuando falta la cabecera. El campo `role` del cuerpo no selecciona un rol. Si está presente debe ser igual al rol actuante; un valor distinto devuelve `400` con el código `data.role_mismatch`, nombrando ambos roles. La misma regla se aplica a `POST /data/sql`, `POST /data/query` y `POST /query/nl`.

El campo `extensions` admite el protocolo Automatic Persisted Query (APQ):

```json
{
  "extensions": {"persistedQuery": {"sha256Hash": "<sha256-of-query>"}}
}
```

**Encabezados:**

- `X-Provisa-Role` — el rol con el que ejecutar (modo de desarrollo); con autenticación, una petición de rol que se atiende solo si está asignado al usuario
- `Accept` — formato de respuesta (ver Negociación de contenido)
- `Authorization` — `Bearer <token>` cuando la autenticación está habilitada
- `X-Provisa-Redirect-Format` — tipo MIME para la salida de redirección a S3
- `X-Provisa-Redirect-Threshold` — cantidad de filas por encima de la cual se activa la redirección
- `X-Provisa-Redirect` — `true` para forzar la redirección incondicionalmente

**Caché de respuestas:** se activa por solicitud con la directiva de operación `@cached` (`query @cached(ttl: 60) { ... }`, `ttl` opcional); la misma directiva se aplica a GraphQL enviado por Arrow Flight. Cypher se activa con una línea `// @provisa cache=true` o `// @provisa cache_ttl=N`, y gRPC con los metadatos de llamada `x-provisa-cache: true` o `x-provisa-cache-ttl: N`. Sin ella, la solicitud no lee ni escribe la caché de respuestas. El encabezado de respuesta `X-Provisa-Cache` indica `HIT` o `MISS`. Consulte [Caché](configuration.md#per-request-opt-in).

#### Encabezados de solicitud de la caché de respuestas {: #response-cache-request-headers }

`/data/graphql`, `/data/cypher`, la Neo4j Query API (`/db/{database}/query/v2`), `/data/rest` y `/data/jsonapi` también aceptan la activación como encabezado de solicitud. Cada forma siguiente se comporta como `@cached(ttl: N)`:

| Encabezado | Equivale a |
| --- | --- |
| `Cache-Control: max-age=N` / `Cache-Control: max-stale=N` | `@cached(ttl: N)` |
| `Cache-Control: max-stale` (sin valor) | `@cached` (el TTL del operador) |
| `Cache-Control: no-cache` / `no-store` | sin caché, igual que no enviar nada |
| `x-provisa-cache: true` | `@cached` |
| `x-provisa-cache-ttl: N` | `@cached(ttl: N)` |

Una solicitud sin uno de estos encabezados y sin indicación en su texto no se almacena en caché, aunque las cachés HTTP simples lo hagan por defecto. Los ajustes de caché del operador siguen aplicándose como con `@cached`: un encabezado nunca hace una lectura más reciente de lo que ellos permiten, y nunca se rechaza por pedir un `ttl` mayor. Las demás directivas de `Cache-Control` (`no-transform`, `min-fresh`, ...) se ignoran, como exige RFC 9111 a una caché.

Una solicitud puede indicar la activación más de una vez: en su texto y en un encabezado, o en dos encabezados. Si coinciden, se almacena en caché una vez según lo pedido. Si no coinciden (por ejemplo `@cached(ttl: 60)` con `Cache-Control: max-age=5`, o `x-provisa-cache-ttl: 10` con `Cache-Control: max-age=20`), la solicitud falla con `400` y el código `data.invalid_cache_control`, y nombra cada forma enviada. Un valor `N` que no sea un número entero de segundos falla del mismo modo. Ninguna forma se prefiere a la otra.

```bash
curl -H 'Cache-Control: max-age=120' -H 'X-Provisa-Role: analyst' \
  'http://localhost:8001/data/rest/sales/orders?limit=10'
```

**Respuesta (JSON en línea):**

```json
{
  "data": {
    "orders": [
      {"id": 1, "amount": 99.99}
    ]
  }
}
```

**Respuesta (redirección):**

```json
{
  "data": {"orders": null},
  "redirect": {
    "redirect_url": "https://...",
    "row_count": 50000,
    "expires_in": 3600,
    "content_type": "application/vnd.apache.parquet"
  }
}
```

**Respuesta (múltiples raíces con contenido mixto en línea/redirección):**

```json
{
  "data": {
    "orders": [{"id": 1}],
    "customers": null
  },
  "redirects": {
    "customers": {
      "redirect_url": "https://...",
      "row_count": 10000,
      "expires_in": 3600,
      "content_type": "application/vnd.apache.parquet"
    }
  }
}
```

Las consultas con múltiples raíces ejecutan cada campo raíz de forma independiente. Los campos por debajo del umbral de redirección se devuelven en línea; los que están por encima se redirigen. La clave `redirects` (plural) asigna nombres de campo a información de redirección.

**Encabezados de caché:**

- `X-Provisa-Cache: HIT|MISS`
- `X-Provisa-Cache-Age: <seconds>` (en HIT)

**Capacidades requeridas:** `QUERY_DEVELOPMENT` para todas las solicitudes, incluida la introspección.

---

### Negociación de contenido {: #content-negotiation }

| Encabezado Accept | Formato |
| --- | --- |
| `application/json` | JSON (predeterminado) |
| `application/x-ndjson` | JSON delimitado por saltos de línea |
| `text/csv` | CSV |
| `application/vnd.apache.parquet` | Parquet |
| `application/vnd.apache.arrow.stream` | Arrow IPC |

---

### Redirección {: #redirect }

Los resultados que superan un umbral de filas configurado (o cuando `X-Provisa-Redirect: true`) se escriben en S3 y se devuelve una URL prefirmada.

| Formato de redirección | Escrito por | Memoria |
| --- | --- | --- |
| `application/vnd.apache.parquet` | CTAS federado | Ninguna — los datos nunca pasan por Provisa |
| `application/x-orc` | CTAS federado | Ninguna — los datos nunca pasan por Provisa |
| `application/json` | Provisa | Limitado por memoria |
| `application/x-ndjson` | Provisa | Limitado por memoria |
| `text/csv` | Provisa | Limitado por memoria |
| `application/vnd.apache.arrow.stream` | Provisa | Limitado por memoria |

Para exportaciones analíticas grandes, use la redirección a Parquet u ORC. El motor de federación escribe directamente en S3 en paralelo y ningún dato pasa por Provisa.

```yaml
X-Provisa-Redirect-Format: application/vnd.apache.parquet
X-Provisa-Redirect-Threshold: 1000
```

---

### `POST /data/sql`

Ejecuta SQL sin procesar a través del pipeline de gobierno de la Etapa 2.

**Cuerpo de la solicitud:**

```json
{
  "sql": "SELECT id, amount FROM orders WHERE region = 'us'",
  "role": "org_admin"
}
```

`params` (opcional) lleva los valores de los marcadores `$1…$n` de la sentencia, en orden, aparte de su texto:

```json
{
  "sql": "SELECT id, amount FROM orders WHERE region = $1 AND amount > $2",
  "params": ["us", 100]
}
```

Los valores se vinculan cuando se ejecuta la instrucción, por el motor o por el driver del origen, y nunca se escriben en la instrucción, así que un valor no puede cambiar lo que es la instrucción. Van con exactamente una instrucción cuyos marcadores los numeran exactamente. Un lote de instrucciones, un marcador sin valor o un valor que ningún marcador recibe se rechaza con `400` y el código `data.sql_parameters_do_not_fit` antes de gobernar o ejecutar la instrucción. Una instrucción sin marcadores se envía sin `params`, como hasta ahora.

**Capacidades requeridas:** `QUERY_DEVELOPMENT`.

Las violaciones de gobierno en `POST /data/sql` devuelven HTTP 403.

**Respuesta:** Mismo formato que `/data/graphql` (filas JSON de forma predeterminada, negociadas por contenido mediante `Accept`).

---

### `POST /data/query`

Un solo endpoint para todos los lenguajes de consulta. Acepta GraphQL, SQL o Cypher y detecta cuál ha recibido.

Las consultas Cypher también pueden enviarse al endpoint exclusivo de Cypher `POST /query/cypher`.

**Cuerpo de la solicitud:**

```json
{
  "query": "{ orders { id } }",
  "params": {},
  "variables": {},
  "role": "org_admin"
}
```

Devuelve `{"data": ...}` para GraphQL, `{"columns": [...], "rows": [...]}` para SQL y Cypher.

---

### `POST /data/sql/explain`

Explica o analiza una sentencia SQL a través del pipeline gobernado.

El endpoint envuelve el SQL **gobernado** en la sintaxis EXPLAIN del dialecto: la instrucción que de verdad se ejecuta bajo el rol de quien llama, después de RLS y enmascaramiento. Así que el plan que ve corresponde a la versión autorizada de la consulta, no a la entrada sin procesar.

**Cuerpo de la solicitud:**

```json
{
  "sql": "SELECT id, amount FROM orders",
  "role": "org_admin",
  "analyze": false
}
```

Establezca `analyze: true` para ejecutar EXPLAIN ANALYZE. La consulta se ejecuta y el plan incluye recuentos de filas y tiempos reales. No todos los dialectos admiten ANALYZE; consulte la tabla en [Planes de consulta y estadísticas](engines.md#query-plans-and-statistics).

**Respuesta:** `{"plan": "<plan text or JSON>", "dialect": "trino", "analyzed": false}`

`400` cuando el dialecto no admite EXPLAIN, o cuando se solicita `analyze: true` en un dialecto que no lo admite (por ejemplo, SQLite).

---

### `GET /data/engine/state`

Devuelve el estado actual del shard del motor sin despertarlo.

La UI consulta este endpoint para mostrar un aviso de arranque mientras el motor arranca en frío. Nunca despierta el motor, así que consultarlo es seguro y no cuenta como actividad para el apagado por inactividad.

**Respuesta:**

```json
{"state": "ready"}
```

Valores posibles:

| Estado | Significado |
| --- | --- |
| `always-on` | Escritorio, autoalojado o coordinador propio — sin gestión de ciclo de vida |
| `ready` | El shard está activo y acepta consultas |
| `starting` | Arranque en frío en curso |
| `stopped` | El shard está escalado a cero |

---

### `POST /data/engine/prewarm`

Provoca el despertar del motor sin ejecutar una consulta.

Devuelve `202 Accepted` de inmediato, y el despertar se ejecuta en segundo plano. Úselo cuando quiera el motor listo antes de que llegue la primera consulta, por ejemplo desde un planificador que lanza consultas unos minutos después.

**Respuesta:** `202 Accepted`, cuerpo `{"started": true}`

---

### `GET /data/rest/{domain_id}/{table_name}`

Endpoint REST plano generado automáticamente para cada tabla registrada. La cadena de consulta se asigna a argumentos GraphQL y la solicitud se compila y ejecuta a través del mismo pipeline (RLS, enmascaramiento, enrutamiento) que GraphQL.

**Parámetros de consulta:**

- `limit` — máximo de filas (≥ 1)
- `offset` — filas a omitir (≥ 0)
- `fields` — nombres de campo separados por comas tal como los escribe el esquema GraphQL, en camelCase con la convención de nombres predeterminada, p. ej. `?fields=orderId,amount` (por defecto, todos los campos escalares). Los nombres de campo de `filter` y `orderBy` siguen la misma escritura. JSON:API toma en su lugar el nombre físico de la columna (`fields[orders]=order_id`).
- `filter` — arreglo JSON de objetos de filtro `{"field", "comparator", "value"}`. Un valor se lee como el tipo de su columna, así que `"100"` en una columna numérica se compara como 100; un valor que el tipo de la columna no admite devuelve `400`.
- `orderBy` — arreglo JSON de objetos de ordenamiento `{"field", "direction"}`
- `params` — los parámetros de la tabla como un objeto JSON por nombre de parámetro, p. ej. `?params={"region":"EU"}`. Una tabla registrada sobre una API, o una tabla de consulta, tiene un parámetro por cada entrada que toma su origen. Una lectura sin un parámetro obligatorio devuelve `400` con su nombre, igual que un nombre para el que la tabla no tiene parámetro. Cada valor se comprueba contra el tipo de su parámetro.

Los valores de filtro y de parámetro viajan como variables junto al texto GraphQL que construye el endpoint, así que un valor con comillas se compara tal como está escrito.

Se requiere el rol autenticado; las solicitudes no autenticadas devuelven `401`. Se sirve una especificación OpenAPI para estas rutas en `GET /data/rest/openapi.json`, con Swagger UI en `GET /data/rest/docs`.

**Caché de respuestas:** se activa con un encabezado de solicitud, `Cache-Control: max-age=N` o `x-provisa-cache: true` (consulte [Encabezados de solicitud de la caché de respuestas](#response-cache-request-headers)). Toda respuesta lleva `X-Provisa-Cache: HIT|MISS`, y `X-Provisa-Cache-Age` en un acierto.

#### Explorador OpenAPI / Swagger UI {: #openapi-swagger-ui-explorer }

La página del explorador OpenAPI (`/app/openapi`) incrusta Swagger UI en un iframe aislado. La especificación está limitada al rol, así que solo aparecen las tablas y columnas que el rol actual puede ver, y el selector de dominio puede acotarla más. La UI cambia sola entre los temas claro y oscuro.

La página carga el HTML de la especificación con `fetch()` en lugar de un `src` directo del iframe. Así la petición lleva el token bearer de la sesión y las peticiones relativas de Swagger UI se resuelven contra el mismo origen.

Abierta desde un enlace NL "Abrir en OpenAPI", la página expande el endpoint de destino, rellena los parámetros de consulta desde la URL generada por NL (p. ej. `aggregate`, `groupBy`) y pulsa Execute, sondeando el DOM para que cada paso termine antes de empezar el siguiente.

---

### `GET /data/jsonapi/{domain_id}/{table_name}`

Endpoint compatible con [JSON:API](https://jsonapi.org) generado automáticamente para cada tabla registrada. Mismo RLS, enmascaramiento y enrutamiento que GraphQL.

**Encabezado `Accept`:** debe incluir `application/vnd.api+json` (el tipo de medio JSON:API) o la solicitud devuelve `406`.

**Parámetros de consulta:**

- `fields[<type>]` — conjuntos de campos dispersos, p. ej. `?fields[orders]=amount`. Los nombres son los nombres físicos de las columnas, p. ej. `fields[orders]=order_id`, igual que en `filter[...]`, `sort` y las claves de atributo emitidas. REST toma en su lugar la escritura de GraphQL (`fields=orderId`).
- `filter[<col>]` / `filter[<col>][<op>]` — p. ej. `?filter[region]=US`, `?filter[amount][gt]=100`. Un valor se lee como el tipo de su columna; un valor que el tipo no admite devuelve `400`.
- `params` — los parámetros de la tabla como un objeto JSON por nombre de parámetro, p. ej. `?params={"region":"EU"}`, como en el endpoint REST. Un parámetro obligatorio ausente devuelve `400` con su nombre; un nombre desconocido o un valor de tipo incorrecto devuelve `400` con `source.parameter` igual a `params`. Los enlaces de paginación conservan el parámetro.
- `sort` — separado por comas, prefijo `-` para orden descendente, p. ej. `?sort=-created_at,amount`
- `page[number]` / `page[size]` — paginación
- `page[total]` — `true` cuenta las filas que coinciden con el filtro y devuelve `meta.total` y `links.last`. Sin él, la respuesta lleva solo `links.self`, `links.first`, `links.prev` y `links.next`, y la base de datos no ejecuta ningún recuento. Cualquier valor distinto de `true` o `false` devuelve `400`. Los enlaces de una respuesta conservan el parámetro.
- `aggregate` — funciones agregadas separadas por comas que se ejecutan en lugar de la recuperación de filas: `count`, `sum`, `avg`, `stddev`, `variance`, `min`, `max`. Use `?aggregate=count,sum` para solicitar un subconjunto. Las respuestas agregadas devuelven `data: null` con los resultados en `meta.aggregate`.
- `groupBy` — nombres de columnas separados por comas; se usa con `?aggregate=` para agrupar resultados. Solo son válidas las columnas presentes en la enumeración `DistinctOnColumn` de la tabla; el servidor devuelve `400` para cualquier columna que el rol no pueda ver.
- `includeNodes` — `true` para incluir las columnas escalares de la tabla base (y los escalares de dimensión incluidos nombrados en `include=`) dentro del arreglo `nodes` de cada fila de grupo. Necesario cuando una consulta de agrupamiento de NL también solicita detalles de dimensión.

- `include` — nombres de relación separados por comas, con la grafía física, p. ej. `?include=order_items,shipment`: cada recurso relacionado se devuelve una vez en `included`, y cada recurso primario enlaza lo que incluye en `relationships.<name>.data` (una lista de identificadores de recurso para una relación a-muchos, un identificador o `null` para una a-uno).

Las respuestas son objetos de recurso con `type`/`id`/`attributes`. El `id` de un recurso es la clave primaria de su tabla: el valor de la columna clave o, para una clave compuesta, los valores de sus columnas en el orden de la clave unidos por `:` (la línea 2 del pedido 7 de una tabla con clave `(order_id, line)` es `7:2`). Un `:` o una `\` dentro de un valor se escapa con `\`. Una tabla sin clave primaria declarada usa su columna `id`; una tabla sin ninguna de las dos devuelve `400`, igual que un `include` que llegue a una. Los errores siguen la forma del objeto de error de JSON:API.

**Caché de respuestas:** se activa con un encabezado de solicitud, `Cache-Control: max-age=N` o `x-provisa-cache: true` (consulte [Encabezados de solicitud de la caché de respuestas](#response-cache-request-headers)). Toda respuesta lleva `X-Provisa-Cache: HIT|MISS`, y `X-Provisa-Cache-Age` en un acierto. Dos formas que no coinciden devuelven `400` como objeto de error de JSON:API.

#### Explorador JSON:API {: #jsonapi-explorer }

La página del explorador JSON:API (`/app/jsonapi`) es una interfaz de navegador sobre estos endpoints. Seleccione una tabla de la lista agrupada por dominio y luego configure:

- **Campos** — elija qué columnas incluir (conjunto de campos disperso); deje todas sin marcar para solicitar todas las columnas
- **Relaciones** — seleccione nombres de relaciones derivadas de FK para incluir con `?include=`
- **Filtro** — campo, operador (`eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `like`) y valor
- **Ordenar** — un campo, ascendente o descendente
- **Agregar** — elija columnas de agrupamiento de la lista validada por el servidor y luego marque una o más funciones agregadas; cuando se seleccionan columnas de agrupamiento, una casilla "Incluir nodos" agrega las columnas escalares de la tabla base a cada fila
- **Tamaño de página** — recursos por página, con navegación primera/anterior/siguiente/última

Los resultados se muestran en una vista de resumen con formato (tarjetas de recurso con anclas de relación en las que se puede hacer clic) o en una pestaña de JSON sin procesar. Se muestra la URL de la solicitud activa y puede copiarse. La selección de tabla y el tamaño de página persisten entre sesiones en `localStorage`.

Cuando se navega desde un enlace de NL "Abrir en JSON:API", el explorador preselecciona la tabla y siembra el selector de agregación a partir de los parámetros de consulta generados por NL, y luego ejecuta automáticamente la solicitud.

---

### `POST /query/nl`

Envía una pregunta en lenguaje natural. El servicio inicia un trabajo asíncrono y devuelve `202 Accepted` con un `job_id` de inmediato. Requiere un proveedor de LLM configurado en la sección de configuración `ai_models`.

**Cuerpo de la solicitud:**

```json
{"q": "How many orders were placed last month?", "role": "org_admin"}
```

Devuelve `{"job_id": "<id>"}`. Superar el límite de frecuencia de NL por rol devuelve `429` con un encabezado `Retry-After`.

**Obtener el resultado:**

- `GET /query/nl/{job_id}` — sondeo (polling). Devuelve el documento del trabajo.
- `GET /query/nl/{job_id}/stream` — SSE. Un evento `branch` por cada objetivo de generación a medida que se completa, seguido de un evento `done`.

Tres bucles de generación (Cypher, GraphQL, SQL) se ejecutan en paralelo, cada uno validado mediante el compilador y refinado ante errores. El prompt se limita al esquema visible del rol. El documento de resultado clasifica cada rama por objetivo:

```json
{
  "job_id": "<id>",
  "state": "complete",
  "branches": {
    "cypher":  {"query": "MATCH ...", "result": [...], "error": null},
    "graphql": {"query": "{ ... }",   "result": {...}, "error": null},
    "sql":     {"query": "SELECT ...", "result": [...], "error": null}
  }
}
```

Una rama que agota sus iteraciones devuelve `query: null`, `result: null` y una cadena `error`. Cada consulta generada se ejecuta con los derechos del consumidor y la gobernanza de la etapa 2 aplicada; el servicio nunca esquiva la gobernanza.

#### Agrupamiento de NL con detalles de dimensión {: #nl-group-by-with-dimension-details }

Tome una consulta NL con group-by que además proyecta columnas de una tabla de dimensión unida, como "número de consultas por usuario con nombre de usuario y correo". El ejecutor deriva rutas con puntos por campo (`dim_paths`) de las columnas de dimensión del SELECT. Esas rutas rellenan el parámetro `includeNodes=` en las URL generadas para los paneles JSON:API y OpenAPI, de modo que esos paneles piden los mismos campos de dimensión unida que resolvieron las ramas SQL y GraphQL. Sin ellas, `includeNodes=true` devolvería solo los campos escalares propios de la tabla agregada base.

En el panel de gRPC, la `{Type}GroupByRequest` generada lleva `include_nodes` (booleano) e `include` (cadena repetida de nombres de campo de relación). La `{Type}GroupByRow` devuelta incluye un campo `nodes` tipado con las filas de detalle de dimensión.

---

### `GET /data/sdl`

Devuelve el SDL de GraphQL para el esquema de un rol.

**Encabezados:** `X-Provisa-Role: <role_id>` (obligatorio)

**Parámetros de consulta:**

- `domain` — IDs de dominio separados por comas. Cuando se establece, la respuesta se filtra al dominio (o dominios) indicado y a las tablas accesibles desde ellos.

**Respuesta:** SDL de GraphQL en `text/plain`.

---

### `GET /data/introspection`

Devuelve el JSON de introspección de GraphQL, opcionalmente filtrado por dominio.

**Encabezados:** `X-Provisa-Role: <role_id>` (obligatorio)

**Parámetros de consulta:** `domain` — IDs de dominio separados por comas.

**Respuesta:** resultado de introspección en `application/json`.

---

### `GET /data/graph-schema`

Devuelve la vista de grafo del esquema del rol: etiquetas de nodo y sus tipos de relación, para clientes Cypher/grafo. Incluye `pk_columns` por etiqueta de nodo para que los llamadores puedan determinar las columnas de clave primaria.

**Respuesta:** `application/json` con `node_labels` (cada una con `pk`/`pk_columns`) y `relationship_types`.

Cada tipo de relación lleva además `junction_table_name` y `properties`. En una arista de unión, el primero nombra la tabla asociativa que recorre y el segundo enumera sus columnas, legibles como `r.attr` y filtrables en `WHERE`. En una arista basada en clave foránea el nombre es `null` y la lista está vacía, y así distingue un cliente las dos. La tabla de unión nunca es una etiqueta de nodo. Es la arista, así que no tiene etiqueta en un cliente de grafos ni fila en `node_labels`.

---

### `GET /data/domains`

Devuelve los IDs de dominio accesibles para el rol solicitante.

**Encabezados:** `X-Provisa-Role: <role_id>` (obligatorio)

**Respuesta:** `["sales", "support", ...]`

---

### `GET /data/schema-version`

Devuelve la cadena de versión del esquema actual. Combina un nonce por arranque con un contador de reconstrucción. Los clientes lo usan para invalidar cachés de esquema tras reinicios del servidor.

**Respuesta:** `{"version": "<boot-id>-<counter>"}`

---

### `GET /data/catalog`

Devuelve el catálogo del rol con el que se ejecuta la solicitud: las tablas que se sirven a ese rol, tal como las da el listado de Arrow Flight.

**Respuesta:** `{"tables": [{"path": ["<domain>", "<table>"], "schema": "<base64>"}]}`. `schema` es el esquema Arrow de la tabla en la codificación Arrow IPC: cada columna con el nombre con el que la lee SQL, con su tipo registrado, su descripción y sus metadatos de clave (`primary_key`, `references`) en los metadatos del campo. No se listan comandos ni métricas.

El rol es el que estableció la capa de autenticación (`X-Provisa-Role` puede pedir un rol que se tiene, o un conjunto separado por comas). Un despliegue sin proveedor de autenticación lista el catálogo completo, como el listado de Flight. El controlador JDBC lee esta ruta cuando el puerto Flight no es accesible.

---

### `GET /data/proto/{role_id}`

Devuelve el archivo `.proto` generado automáticamente para un rol.

**Respuesta:** esquema protobuf en `text/plain`.

`{role_id}` es un rol que el solicitante posee, o un conjunto de roles poseídos separados por comas (`analyst,org_admin`), que se sirve como el metarrol del conjunto: la unión de lo concedido a sus miembros. Un rol que el solicitante no posee se rechaza con `403 auth.role_not_assigned`, y un id de metarrol nombrado directamente con `403 auth.meta_role_named`. `?domains=a,b` limita el proto a esos dominios; un dominio que el rol no alcanza se rechaza con `403 data.domain_not_accessible`. Un rol sin superficie de datos responde `404 data.no_proto_for_role`, un rol desconocido `404 data.no_role` y un modelo aún no construido `503 data.schema_cache_not_ready`.

Cada tabla del esquema del rol produce un `message` proto. Las relaciones producen campos de mensaje anidados. El servicio lista RPC solo para las tablas en la raíz del esquema del rol, las mismas que lista su tipo `Query` de GraphQL; una tabla relacionada tiene su mensaje y se alcanza a través de su relación.

Añada `?domains=a,b` para restringir el archivo a esos dominios. El servicio lista entonces RPC para las tablas propias de los dominios elegidos; las tablas relacionadas con ellas aparecen como mensajes.

El archivo sigue al modelo. Se regenera con el esquema del rol cada vez que cambia el modelo o el rol, también el de un metarrol. Asignación de tipos: `integer → int32`, `bigint → int64`, `varchar → string`, `decimal → double`, `boolean → bool`, `timestamp → google.protobuf.Timestamp`.

---

### `GET /data/subscribe/{table}`

Flujo de Server-Sent Events para notificaciones de cambios en tiempo real de una tabla.

La entrega de notificaciones usa un proveedor conectable elegido según el tipo de origen: los orígenes PostgreSQL usan `LISTEN/NOTIFY` (a través de asyncpg), los orígenes MongoDB usan Change Streams (`collection.watch()`), y los orígenes Kafka usan grupos de consumidores. Cada proveedor implementa una interfaz de observación asíncrona común. El filtrado RLS y la validación de esquema se aplican independientemente del proveedor. También se admiten orígenes WebSocket y RSS.

**Encabezado — `X-Provisa-Sink`:** Establézcalo en un destino Kafka (p. ej. `kafka://broker:9092/topic`) para redirigir los eventos de cambio a un sink de Kafka en lugar de la respuesta SSE. El servidor inicia un consumidor de sink y devuelve `202 Accepted` en lugar de un flujo abierto.

### `POST /data/sources/{source_id}/signal`

Indica a Provisa que un origen cambió, sin filas: el receptor de la señal de cambio `signal`. Cada tabla del origen cuya señal de cambio es `signal` (la suya propia, o si no la de su origen) se trata como escrita, como una escritura a través de Provisa: se descartan sus respuestas en caché, las vistas materializadas sobre ella quedan obsoletas y se pide una construcción de su réplica. La construcción de una tabla protegida contra carga espera a su ventana fuera de horas punta. `?table=schema.table` señala una tabla.

El rol de quien llama necesita el derecho de escritura y debe alcanzar el dominio de cada tabla que señala. La respuesta es `202 Accepted` con las tablas señaladas:

```json
{"signalled": ["public.orders"]}
```

Un origen que no existe, o una `table` que no tiene, responde `404`; un origen sin ninguna tabla con la señal de cambio `signal` responde `409`, porque no se actualizaría nada.

---

## Endpoints REST de administración {: #admin-rest-endpoints }

### Config

#### `GET /admin/config`

Descarga el `provisa.yaml` actual como `application/x-yaml` con un encabezado `Content-Disposition: attachment`.

#### `PUT /admin/config`

Sube un YAML de configuración revisado. El servidor escribe una copia de seguridad `.bak`, guarda el nuevo archivo, vuelve a cargar desde él los ajustes del despliegue y lo aplica al almacén del modelo como una siembra explícita y única: lo que declara el archivo se añade o actualiza mediante el almacén del modelo, y no se quita nada. Un reinicio posterior lee los ajustes del archivo y nunca vuelve a aplicar su modelo; tras el primer arranque solo el almacén del modelo es dueño del modelo.

**Cuerpo de la solicitud:** Contenido YAML sin procesar.

**Respuesta:**

```json
{"success": true, "message": "Config uploaded and applied"}
```

En caso de error de recarga: `{"success": false, "message": "<error>"}`.

#### `GET /admin/config/live`

Descarga el modelo tal como está, escrito como archivo de configuración. Contiene los ajustes del archivo de despliegue, con cada sección del modelo leída del almacén del modelo: almacenes, regiones, orígenes, dominios, roles, productos de datos, términos del glosario, tablas y sus columnas, relaciones, métricas, etiquetas y sus asignaciones, filtros de filas, commands, webhooks, disparadores programados y reglas de nombres. Aplicado a un almacén del modelo vacío, el archivo construye el mismo modelo. Lo que el despliegue siembra en todo almacén (los orígenes integrados y sus tablas, los dominios del sistema, `org_admin` y `platform_admin`) queda fuera.

**Parámetro de consulta:** `sections` (opcional): una lista de secciones del modelo separadas por comas, p. ej. `sources,tables`. Solo se escriben esas secciones; un nombre desconocido responde 400 `settings.export_unknown_section`.

El endpoint está abierto en todo despliegue para quien tiene el derecho sobre los ajustes del despliegue; la sección Archivo de configuración de la página de administración lo ofrece como **Exportar modelo**.

Devuelve `application/x-yaml` con `Content-Disposition: attachment; filename=provisa.live.yaml`.

#### `GET /admin/config/diff`

Devuelve los dos lados de la diferencia de configuración, `original` (la base del arranque) y `current` (el estado en vivo), normalizados igual, de modo que la comparación muestra solo cambios reales e ignora reordenaciones y cambios en comentarios.

**Respuesta:**

```json
{"original": "<yaml>", "current": "<yaml>"}
```

#### `POST /admin/config/patch`

Genera un parche en formato unified diff desde la línea base hasta la configuración enviada.

Envíe el YAML revisado como cuerpo de la petición. La respuesta es un archivo `text/x-patch` (`provisa.config.patch`) que `git apply` o `patch` pueden aplicar tal cual, algo útil para confirmar a través de un pipeline de CI/CD los cambios de configuración hechos en la UI.

---

### Configuración (Settings) {: #settings }

#### `GET /admin/settings`

Devuelve la configuración actual de la plataforma como JSON.

**Respuesta:**

```json
{
  "redirect": {
    "enabled": true,
    "threshold": 10000,
    "default_format": "application/vnd.apache.parquet",
    "ttl": 3600
  },
  "sampling": {
    "default_sample_size": 1000
  },
  "cache": {
    "default_ttl": 300
  },
  "naming": {
    "domain_prefix": false,
    "convention": "apollo_graphql"
  },
  "relationships": {
    "auto_track_fk": true
  },
  "otel": {
    "endpoint": "http://otel-collector:4318",
    "service_name": "provisa",
    "sample_rate": 1.0,
    "support_endpoint": "",
    "support_redact_sql_literals": true,
    "support_redact_attributes": []
  }
}
```

#### `PUT /admin/settings`

Actualiza los ajustes de la plataforma en tiempo de ejecución. Todos los campos son opcionales y solo cambian las claves presentes en el cuerpo.

**Cuerpo de la solicitud (ejemplo parcial):**

```json
{
  "otel": {
    "support_endpoint": "https://telemetry.vendor.com/v1/traces",
    "support_redact_sql_literals": true,
    "support_redact_attributes": ["db.statement", "user.email"]
  },
  "cache": {"default_ttl": 600}
}
```

Campos actualizables por sección:

- `redirect`: `enabled`, `threshold`, `default_format`, `ttl`
- `sampling`: `default_sample_size`
- `cache`: `default_ttl`
- `naming`: `domain_prefix`, `convention` — escribe en el archivo de configuración y activa la recarga de esquema
- `relationships`: `auto_track_fk` — rige únicamente el seguimiento de claves foráneas. Una relación respaldada por una tabla de unión se declara en el registro de la tabla y nunca se infiere, así que este ajuste no la alcanza.
- `otel`: `endpoint`, `service_name`, `sample_rate`, `support_endpoint`, `support_redact_sql_literals`, `support_redact_attributes`

**Respuesta:**

```json
{"success": true, "updated": ["otel.support_endpoint", "cache.default_ttl"]}
```

---

### Modelos de IA {: #ai-models }

#### `GET /admin/ai-models`

Devuelve las asignaciones de modelos de IA de la organización activa, el registro de modelos vectoriales y el límite de tasa de NL.

**Respuesta:**

```json
{
  "ai_models": {
    "nl": "claude-3-5-sonnet-20241022",
    "embedding": "text-embedding-3-small"
  },
  "vector_models": [...],
  "nl": {"rate_limit": 20},
  "api_keys_set": {"anthropic": true, "openai": false}
}
```

Las claves de API nunca se devuelven: `api_keys_set` solo indica si cada proveedor tiene una clave configurada. Los cambios se aplican desde la siguiente petición, sin reiniciar.

#### `PUT /admin/ai-models`

Actualiza las asignaciones de modelos de IA de la organización, el registro de modelos vectoriales o el límite de tasa de NL. Surte efecto en la siguiente solicitud.

#### `GET /admin/ai-models/vendors/{vendor}/models`

Devuelve los nombres de modelo que sirve actualmente un proveedor, para el selector de modelos.

La lista se lee en vivo de la API de listado de modelos del propio proveedor, con la clave configurada de la organización, o con la credencial del despliegue cuando la organización no tiene clave. Un modelo publicado después de esta versión se puede elegir el mismo día que el proveedor empieza a servirlo.

Devuelve `400` cuando el proveedor no publica una API de listado de modelos (en ese caso, escriba el nombre del modelo directamente) o cuando no hay ninguna clave disponible.

---

### Motor de federación {: #federation-engine }

#### `GET /admin/federation-engine`

Devuelve la selección actual del motor de federación, su configuración de conexión y el registro completo de motores seleccionables.

**Respuesta:**

```json
{
  "current": "trino",
  "persisted": "trino",
  "registry": [
    {"key": "trino", "label": "Trino (embedded)", "fields": [...]},
    {"key": "duckdb", "label": "DuckDB", "fields": []}
  ],
  "note": "Changing the federation engine takes effect after the service is restarted."
}
```

La clave `current` es el motor en ejecución en este momento; `persisted` es lo que se escribe en el archivo de configuración y se cargará en el próximo reinicio. Divergen cuando la configuración se cambió pero el servicio aún no se ha reiniciado.

#### `PUT /admin/federation-engine`

Guarda una selección de motor de federación.

**Cuerpo de la solicitud:**

```json
{"engine": "trino", "federation_engine_url": "http://trino-coordinator:8080"}
```

La selección se escribe en la configuración de la plataforma y surte efecto tras el siguiente reinicio del servicio, porque el motor se elige una vez al arrancar.

---

### Política de dominios {: #domain-policy }

#### `POST /admin/domain-policy`

Cambia la política de dominios de la organización activa (`use_domains` / `default_domain`).

El cambio no quita nada. Se rechaza con `409` y el código `settings.domain_policy_catalog_exists` mientras la organización que actúa tenga un catálogo: una tabla registrada, un origen o un dominio que no sembró el despliegue. Los `params` del rechazo cuentan cada tipo (`tables`, `sources`, `domains`). Elimine antes esos objetos, cada uno con su propia acción, y luego cambie. Cuando un archivo de configuración declara parte de ese catálogo, el código es `settings.domain_policy_catalog_in_config`: la siguiente carga lo restauraría, así que la política se fija en el archivo de configuración.

**Cuerpo de la solicitud:**

```json
{
  "use_domains": true,
  "default_domain": "default"
}
```

`use_domains: null` elimina la anulación de la organización y vuelve al ajuste del despliegue. `use_domains: false` requiere `default_domain` (el nombre del dominio único en el que aterrizan todas las tablas). La reconstrucción del catálogo es síncrona; la respuesta se devuelve cuando los esquemas están listos.

---

### Observabilidad {: #observability }

#### `GET /admin/traces/recent`

Devuelve hasta N spans completados recientes del búfer de spans en memoria.

**Parámetros de consulta:** `limit` (predeterminado 50, máximo 200)

**Respuesta:** `{"traces": [...]}`

#### `POST /admin/query-engine/reload-catalog`

Recarga en caliente un catálogo con nombre en el coordinador del motor de federación a través de su API REST. Reconecta la conexión interna de Provisa y vuelve a ejecutar el DDL de OTel.

**Parámetros de consulta:** `catalog` (predeterminado `"otel"`)

**Respuesta:**

```json
{"success": true, "errors": []}
```

#### `POST /admin/query-engine/restart`

Reinicia el contenedor del motor de federación (solo para desarrollo de un solo nodo).

**Parámetros de consulta:** `container` (por defecto la variable de entorno `QUERY_ENGINE_CONTAINER`, luego `"trino"`)

---

### Descubrimiento {: #discovery }

#### `POST /admin/discover/relationships`

Activa el descubrimiento de relaciones. Siempre ejecuta la introspección de claves foráneas desde el motor de federación. Ejecuta inferencia por LLM si `ANTHROPIC_API_KEY` está configurada.

**Cuerpo de la solicitud:**

```json
{
  "scope": "domain",
  "domain_id": "sales"
}
```

`scope` debe ser uno de `"table"`, `"domain"`, `"cross-domain"`. Para el ámbito `"table"`, se requiere `table_id` (entero). Para el ámbito `"domain"`, se requiere `domain_id`.

**Respuesta:** `{"candidates_found": 12, "stored_ids": [1, 2, 3, ...]}`

#### `GET /admin/discover/candidates`

Lista los candidatos de relación pendientes.

#### `POST /admin/discover/candidates/{candidate_id}/accept`

Acepta un candidato y lo registra como relación.

**Cuerpo de la solicitud (opcional):** `{"name": "custom-relationship-name"}`

#### `POST /admin/discover/candidates/{candidate_id}/reject`

Rechaza un candidato.

**Cuerpo de la solicitud:** `{"reason": "Not a real join"}`

#### `GET /admin/discover/candidates/rejected/count`

Devuelve el recuento de candidatos rechazados.

#### `DELETE /admin/discover/candidates/rejected`

Elimina todos los candidatos rechazados.

---

### Rastreo de orígenes (Source Crawl) {: #source-crawl }

#### `POST /admin/sources/crawl`

Rastrea un origen de datos para hacer introspección de su esquema y registrar tablas.

---

### Búsqueda de tablas de origen {: #source-table-search }

#### `GET /admin/sources/{source_id}/tables/search`

Busca por nombre tablas disponibles (aún no registradas) en un origen.

---

### Perfilado de tablas {: #table-profiling }

#### `POST /admin/tables/{table_id}/profile`

Ejecuta un perfil de columnas sobre una tabla registrada: cardinalidad, mín/máx y tasas de nulos.

---

### Descripciones de origen {: #source-descriptions }

#### `POST /admin/source-meta/db-description`

Genera descripciones asistidas por LLM para las tablas y columnas de un origen.

---

### Almacenamiento de objetos {: #object-storage }

#### `GET /admin/org-storage`

Informa de la huella de almacenamiento de la organización activa frente a su asignación de plataforma, y de si la organización ha registrado su propio almacén.

Cuando la organización ha registrado su propio DSN, sus materializaciones van allí y dejan de contar contra la asignación. El DSN nunca se devuelve.

#### `PUT /admin/org-storage`

Registra (o elimina) el almacén de materialización propio de la organización.

**Cuerpo de la solicitud:**

```json
{"storage_url": "s3://my-bucket/provisa?region=us-east-1&access_key=..."}
```

El DSN se comprueba contra el motor de federación antes de aceptarlo, así que un DSN inservible falla al registrarse y no horas después durante una actualización. El valor se cifra en reposo y GET nunca lo devuelve.

Envíe `storage_url: null` para eliminar el almacén propio de la organización y devolver sus materializaciones al almacén de la plataforma (y a la asignación). El runtime de la organización se reconstruye en la misma llamada, de modo que el nuevo almacén es efectivo de inmediato.

---

### Cifrado de la organización {: #org-encryption }

#### `GET /admin/org-encryption`

Devuelve el estado actual de la clave de la organización: huella, id y procedencia. Nunca devuelve material de clave.

Cuando la organización no ha establecido ninguna clave, devuelve `{"configured": false}`. Toda organización empieza en este estado y hereda la clave del despliegue.

#### `PUT /admin/org-encryption`

Establece o rota la clave de cifrado en reposo de la organización.

**Cuerpo de la solicitud:**

```json
{"key_b64": "<32 raw bytes, base64-encoded>"}
```

Omita `key_b64` para que Provisa genere una clave. Es la opción más segura, porque la clave nunca aparece en un portapapeles ni en un registro de peticiones. Indicar `key_b64` aporta su propia clave.

La rotación añade una nueva entrada activa al anillo de claves y conserva la anterior, de modo que los datos escritos con la clave previa siguen siendo legibles. La rotación no es un recifrado. No hay endpoint de borrado: retirar la última clave haría ilegible toda carga envuelta.

El anillo activo se revincula en la misma llamada, de modo que la siguiente escritura cifrada usa la nueva clave de inmediato.

---

### Importación de Hasura / DDN {: #hasura-ddn-import }

#### `POST /admin/import/hasura/preview`

Convierte un archivo comprimido de un proyecto Hasura v2 o DDN en configuración propuesta de Provisa sin escribir nada.

**Cuerpo de la solicitud:**

```json
{
  "filename": "my-project.zip",
  "content_b64": "<base64-encoded archive>",
  "flavor": "auto",
  "domain_map": {"public": "sales"},
  "source_overrides": {}
}
```

`flavor` es `"auto"` (detectado a partir de la estructura del archivo comprimido), `"hasura_v2"` o `"ddn"`.

**Respuesta:**

```json
{
  "config_yaml": "...",
  "warnings": ["..."],
  "summary": {
    "sources": 1, "domains": 2, "tables": 40,
    "columns": 180, "roles": 3, "relationships": 15, "rls_rules": 6
  }
}
```

No se guarda nada. La vista previa no se almacena en caché en el servidor; `apply` toma el YAML que usted envía, así que lo que se aplica es exactamente lo que se revisó (y, si acaso, se editó).

#### `POST /admin/import/hasura/apply`

Carga en la organización activa una configuración previamente vista.

**Cuerpo de la solicitud:**

```json
{"config_yaml": "<yaml string>"}
```

Una importación es una siembra explícita y única del almacén del modelo de la organización que actúa, la misma aplicación que `PUT /admin/config`: añade y actualiza mediante el almacén del modelo lo que declara la configuración importada, con sus protecciones y versiones, y no quita nada que no mencione. No hay campo `replace`. Los catálogos del motor de los orígenes que declara, los pools de la organización y sus esquemas se reconstruyen antes de que vuelva la respuesta.

**Respuesta:** `{"summary": {...}}` — los recuentos de lo que declara la configuración importada.

---

### Intercambio con Apache Ossie {: #apache-ossie-interchange }

#### `GET /admin/ossie`

Exporta el modelo gobernado de la organización como un documento YAML de Apache Ossie (en incubación).

El documento se construye a partir del estado en vivo en cada petición y nunca se guarda en caché, así que no puede quedar desactualizado. Las tablas pasan a objetos `dataset`, las columnas a objetos `field` y las relaciones se asignan a objetos `relationship` de Ossie.

Devuelve `text/yaml` con `Content-Disposition: attachment; filename=provisa.ossie.yaml`.

#### `GET /admin/dbt/sources`

Descarga el catálogo gobernado como un `sources.yml` de dbt, tal como se sirve a un rol.

**Parámetro de consulta:** `role` — un rol que tiene quien llama, o cualquier rol para quien tiene `access_config`.

Devuelve `text/yaml` con `Content-Disposition: attachment; filename=provisa.sources.yml`. Un rol desconocido es un `404` con el código `dbt_sources.unknown_role`. Consulte [integraciones](integrations.md#dbt-sources-file) para saber qué contiene el archivo.

#### `POST /admin/ossie/import`

Analiza un documento Ossie en YAML o JSON y devuelve registros propuestos de tablas y relaciones.

**Cuerpo de la solicitud:** YAML o JSON de Ossie sin procesar. El formato se detecta automáticamente.

**Respuesta:**

```json
{
  "proposals": {
    "tables": [...],
    "relationships": [...]
  }
}
```

No se registra nada. Use la pantalla de revisión de la interfaz de administración para aceptar o recortar las propuestas antes de que se dispare cualquier mutación.

---

### Acciones (funciones y webhooks) {: #actions-functions-and-webhooks }

Todos los endpoints están bajo el prefijo `/admin/actions`.

Cada invocación, venga de GraphQL, SQL, Cypher, Bolt, Arrow Flight, MCP `run_sql` o Provisa gRPC, pasa por un único ejecutor gobernado. Este impone la asignación de commands (`visible_to`), el derecho `write` para una mutación y la gobernanza, siempre de la misma manera. Consulte [docs/integrations.md](integrations.md#invoking-commands-across-protocols) para la sintaxis de llamada de cada protocolo.

#### `GET /admin/actions`

Devuelve todas las funciones de BD y webhooks rastreados.

**Respuesta:**

```json
{
  "functions": [
    {
      "name": "random_python_set",
      "implKind": "python",
      "binding": {"callable": "demo.py_functions:random_dataset"},
      "returns": "",
      "returnSchema": {
        "type": "array",
        "items": {"type": "object", "properties": {"id": {"type": "integer"}, "region": {"type": "string"}}}
      },
      "arguments": [{"name": "rows", "type": "Int"}, {"name": "seed", "type": "Int"}],
      "visibleTo": ["admin"],
      "domainId": "pet-store",
      "description": "Demo Python command returning random rows",
      "kind": "query"
    }
  ],
  "webhooks": [
    {
      "name": "add-pet",
      "url": "https://petstore.example.com/pets",
      "method": "POST",
      "kind": "mutation",
      "approved": true
    }
  ]
}
```

Cada objeto de webhook lleva un booleano `approved`. Un webhook queda aprobado en cuanto un steward ejecuta su solicitud de creación; los webhooks declarados en la configuración se aprueban automáticamente. Un webhook no aprobado queda registrado pero no se expone en ninguna superficie.

#### `POST /admin/actions/functions`

Registra una función rastreada (comando).

**Campos clave:**

| Campo | Obligatorio | Descripción |
| --- | --- | --- |
| `name` | Sí | Nombre único del comando |
| `kind` | Sí | `"query"` → campo de Query GraphQL; `"mutation"` → campo de Mutation |
| `implKind` | No | Cómo se ejecuta el comando — ver tabla siguiente (predeterminado `source_procedure`) |
| `binding` | No | Detalles de conexión específicos de `implKind` (objeto JSON) |
| `returnSchema` | No | JSON Schema `{type:"array", items:{type:"object", properties:{...}}}` — hace que el comando devuelva un conjunto en cada superficie |
| `arguments` | No | Definiciones de argumento `[{name, type}]`; el orden posicional importa para llamadores SQL y Bolt |
| `visibleTo` | No | IDs de rol a los que está asignado el comando (vacío: todos los roles). Una `mutation` además necesita el derecho `write` |
| `domainId` | No | Dominio para la ubicación en GraphQL y el control de acceso |

**Valores de `implKind`:**

| `implKind` | Qué se ejecuta | Campos de `binding` |
| --- | --- | --- |
| `source_procedure` | Procedimiento almacenado en un origen registrado (predeterminado) | `sourceId`, `schemaName`, `functionName` |
| `script` | Script del lado del servidor | `script` |
| `http` | Llamada HTTP saliente | `url`, `method` |
| `grpc` | Llamada gRPC saliente a un servidor externo | `target`, `method` |
| `python` | Callable de Python alojado por Provisa | `callable` (p. ej. `"demo.py_functions:random_dataset"`) |

Los comandos de demostración `random_python_set` (`implKind: python`) y `random_grpc_set` (`implKind: grpc`) muestran en la práctica comandos que devuelven conjuntos con `returnSchema`; ambos están en `config/provisa-install.yaml`.

#### `PUT /admin/actions/functions/{name}`

Actualiza una función rastreada por nombre.

#### `DELETE /admin/actions/functions/{name}`

Elimina una función rastreada por nombre.

#### `POST /admin/actions/webhooks`

Registra un webhook rastreado. Registrar o actualizar un webhook pone en cola una solicitud de aprobación para el steward, y el webhook solo se activa en todas las superficies cuando un steward lo aprueba. Los webhooks declarados en la configuración se aprueban automáticamente. **Campos del cuerpo de la petición:** `name`, `url`, `method`, `timeoutMs`, `returns`, `inlineReturnType`, `arguments`, `visibleTo`, `domainId`, `description`, `kind`.

#### `PUT /admin/actions/webhooks/{name}`

Actualiza un webhook rastreado por nombre. Cualquier edición reinicia la aprobación a pendiente hasta que se vuelva a aprobar.

#### `DELETE /admin/actions/webhooks/{name}`

Elimina un webhook rastreado por nombre.

#### `POST /admin/actions/test`

Prueba una acción (función o webhook) por nombre.

---

### Roles

Todos los endpoints están bajo el prefijo `/admin/roles`.

| Método | Ruta | Descripción |
| --- | --- | --- |
| `GET` | `/admin/roles/` | Lista todos los roles |
| `POST` | `/admin/roles/` | Crea un rol |
| `PUT` | `/admin/roles/{role_id}` | Actualiza un rol |
| `DELETE` | `/admin/roles/{role_id}` | Elimina un rol |

---

### Usuarios {: #users }

Todos los endpoints están bajo el prefijo `/admin/users`.

| Método | Ruta | Descripción |
| --- | --- | --- |
| `POST` | `/admin/users/` | Crea un usuario local |
| `GET` | `/admin/users/` | Lista usuarios locales |
| `GET` | `/admin/users/{user_id}` | Obtiene un usuario |
| `PUT` | `/admin/users/{user_id}` | Actualiza un usuario |
| `PATCH` | `/admin/users/{user_id}/password` | Cambia la contraseña |
| `DELETE` | `/admin/users/{user_id}` | Elimina un usuario |
| `GET` | `/admin/users/{user_id}/assignments` | Lista las asignaciones de rol |
| `POST` | `/admin/users/{user_id}/assignments` | Agrega una asignación de rol |
| `DELETE` | `/admin/users/{user_id}/assignments/{assignment_id}` | Elimina una asignación de rol |

---

### Organizaciones {: #organizations }

Todos los endpoints están bajo `/admin/orgs`.

| Método | Ruta | Descripción |
| --- | --- | --- |
| `GET` | `/admin/orgs/` | Lista organizaciones |
| `POST` | `/admin/orgs/` | Crea una organización |
| `PUT` | `/admin/orgs/{org_id}` | Actualiza una organización |
| `DELETE` | `/admin/orgs/{org_id}` | Elimina una organización |
| `GET` | `/admin/orgs/{org_id}/members` | Lista miembros |
| `POST` | `/admin/orgs/{org_id}/members` | Agrega un miembro |
| `DELETE` | `/admin/orgs/{org_id}/members/{user_id}` | Elimina un miembro |

---

### Invitaciones {: #invites }

Todos los endpoints están bajo `/admin/invites`.

| Método | Ruta | Descripción |
| --- | --- | --- |
| `POST` | `/admin/invites/` | Crea una invitación |
| `GET` | `/admin/invites/` | Lista las invitaciones pendientes |
| `DELETE` | `/admin/invites/{token}` | Revoca una invitación |

---

### GraphQL de administración {: #admin-graphql }

#### `POST /admin/graphql`

Endpoint de Strawberry GraphQL para todas las operaciones de administración: CRUD de orígenes y tablas, gestión de relaciones, configuración de dominios, reglas de RLS, control de caché, convenciones de nomenclatura, gestión de tareas programadas y compilación de consultas.

Para la referencia completa del esquema (cada campo de Query, campo de Mutation y tipo de entrada/salida) véase [Referencia de la API GraphQL de administración](admin-graphql.md).

**Mutaciones clave:**

```graphql
# Cache
mutation { update_source_cache(source_id: "sales-pg", enabled: true, ttl: 600) { success } }
mutation { update_table_cache(table_id: 1, ttl: 60) { success } }

# Naming conventions
mutation { update_source_naming(source_id: "legacy-db", convention: "camelCase") { success } }
mutation { update_table_naming(table_id: 1, convention: "PascalCase") { success } }

# Scheduled tasks
mutation { toggle_scheduled_task(name: "daily-report", enabled: false) { success } }

# Compile a query (returns enforcement metadata and routed SQL)
mutation {
  compile_query(input: {role: "admin", query: "{ orders { id } }"}) {
    sql semantic_sql trino_sql direct_sql route route_reason sources root_field
    enforcement { rls_filters_applied columns_excluded masking_applied }
  }
}
```

---

### Configuración inicial (Setup) {: #setup }

#### `GET /setup/status`

Devuelve el estado de la configuración de primer arranque. Siempre sin autenticación.

#### `POST /setup/`

Completa la configuración de primer arranque.

---

## Verificación de estado (Health Check) {: #health-check }

#### `GET /health` o `HEAD /health` {: #get-health-or-head-health }

Devuelve `{"status": "ok"}`. Siempre sin autenticación.

---

## Respuestas de error {: #error-responses }

| Estado | Significado |
| --- | --- |
| 400 | Consulta inválida, error de validación o error de análisis SQL |
| 401 | Token de autenticación ausente o inválido |
| 403 | Capacidades insuficientes; violación de gobierno |
| 404 | Rol, recurso o archivo de configuración no encontrado |
| 422 | Falta un encabezado obligatorio (p. ej. `X-Provisa-Role`) |
| 503 | Base de datos u origen no conectado; dependencia no disponible |
| 504 | La solicitud agotó el tiempo de espera |

Las violaciones de gobierno en `POST /data/sql` devuelven HTTP 403 con un cuerpo estructurado:

```json
{
  "detail": {
    "violations": [
      {"code": "V000", "message": "Table 'orders' is not accessible for role 'analyst'"}
    ]
  }
}
```

Todos los demás errores usan: `{"detail": "<message>"}`.

---

## Endpoint de Arrow Flight {: #arrow-flight-endpoint }

Puerto `8815`. Transporte columnar Arrow nativo sobre gRPC.

Las consultas y el descubrimiento de catálogo están disponibles en la misma conexión. El pipeline de gobierno completo (RLS, enmascaramiento, muestreo) se aplica a cada consulta.

El descubrimiento del catálogo (`list_flights`, `get_flight_info`, `get_schema` y un ticket sin `query`) lista lo que se concede al rol de quien llama. Con la autenticación activada, las tres llamadas sin ticket toman la credencial de la cabecera de llamada `authorization: Bearer` y el rol de `x-provisa-role`, y se rechazan sin credencial; consulte [Integraciones](integrations.md).

**Formato de ticket** (JSON):

```json
{"query": "{ customers { name email } }", "role": "analyst", "variables": {}}
```

**Uso (Python):**

```python
import pyarrow.flight as flight

client = flight.FlightClient("grpc://localhost:8815")
ticket = flight.Ticket(b'{"query": "{ orders { id amount } }", "role": "admin"}')
# Stream batch-by-batch
for batch in client.do_get(ticket):
    process(batch.data)
# Or read all at once
table = client.do_get(ticket).read_all()
```

Cuando el proxy Zaychik Flight SQL está disponible (puerto 8480), los lotes de registros se transmiten de extremo a extremo sin materialización completa. Si Zaychik no está disponible, recurre a la materialización a través de la capa de consulta federada.

**Advertencias.** Lo que una respuesta dice de sí misma viaja en el flujo como el `app_metadata` de un lote sin filas: `{"provisa_warnings": [{"code", "params", "message"}]}`. Las advertencias conocidas antes de leer las filas llegan en un lote sin filas previo a ellas. Una advertencia conocida solo tras leer las filas llega en un segundo lote sin filas, el último del flujo: `statement.rows_cut` dice que la respuesta se cortó en un límite de filas y que hay más filas que coinciden. Lea `app_metadata` en cada lote, incluido el último.

---

## Endpoint gRPC de Protobuf {: #protobuf-grpc-endpoint }

Puerto `50051` (anúlelo con la variable de entorno `GRPC_PORT` o la configuración `server.grpc_port`).

Pase el rol en la clave de metadatos gRPC `x-provisa-role`. Si está ausente, el servidor aborta con `UNAUTHENTICATED`.

Descargue el proto específico de un rol desde `GET /data/proto/{role_id}`. Solo aparecen las tablas y columnas visibles para ese rol.

```proto
service ProvisaService {
  rpc QueryOrders (QueryOrdersRequest) returns (stream Orders);
  rpc InsertOrders (InsertOrdersRequest) returns (InsertOrdersResponse);
}
```

Cada tabla en la raíz del esquema del rol produce una RPC de streaming `Query{TypeName}`. Son las tablas que lista el tipo `Query` de GraphQL del rol. Una tabla a la que el rol solo llega a través de una relación no tiene RPC propia, igual que no tiene campo raíz en GraphQL. Las RPC `Insert{TypeName}` existen por simetría del esquema, pero abortan con `UNIMPLEMENTED`.

**Parámetros.** Una tabla con parámetros (una tabla registrada sobre una API o una tabla de consulta) tiene un mensaje `{TypeName}Params`, un campo opcional por parámetro, nombrado sin su prefijo `_nf_`, y `Query{TypeName}Request` lo lleva como `params`. Cada valor fijado allí llega al origen como el parámetro, vinculado en la sentencia; un nombre que no es un parámetro de la tabla se rechaza con `INVALID_ARGUMENT`, y una lectura sin un parámetro obligatorio se rechaza nombrándolo. El proxy HTTP de gRPC toma los mismos valores como el objeto `params` del cuerpo (HTTP 400, código `data.invalid_parameter`, ante un nombre desconocido o un valor de tipo JSON incorrecto).

Un método que no forma parte del servicio del rol del llamante responde `UNIMPLEMENTED`.

`grpc_reflection.v1alpha` está habilitado para el descubrimiento de servicios sin un proto precompilado. La reflexión describe a cada llamante el servicio del rol con el que actúa: el mismo `.proto` que ese rol descarga de `GET /data/proto/{role_id}`. Lista por nombre el servicio de Provisa y el servicio de reflexión, y describe el archivo del servicio de Provisa.

Una llamada de reflexión se identifica como cualquier otra llamada gRPC. Con la autenticación activada, el rol proviene de la credencial del llamante. Con la autenticación desactivada, indique el rol en `x-provisa-role`; en un despliegue que sirve a más de una organización, indique también la organización en `x-provisa-org`. Una llamada sin ellos se rechaza con `UNAUTHENTICATED`. Con la autenticación desactivada, la reflexión permanece deshabilitada salvo que se establezca `server.grpc_allow_unsecured_reflection`.

```bash
grpcurl -plaintext -H 'x-provisa-role: analyst' localhost:50051 list
grpcurl -plaintext -H 'x-provisa-role: analyst' \
  -d '{}' localhost:50051 provisa.v1.ProvisaService/QueryOrders
```

El servidor gRPC arranca cuando el modelo cargado al inicio define un servicio gRPC. A partir de ahí, cada llamada se sirve desde el modelo tal como está: una tabla registrada o modificada mientras el servidor está en marcha es servida y descrita por la siguiente llamada, sin reiniciar.

#### RPC de agregación y agrupamiento {: #aggregate-and-group-by-rpcs }

Cuando una tabla tiene `enable_aggregates` establecido, el proto generado incluye dos RPC adicionales junto a `Query{TypeName}`:

- **`Query{TypeName}Aggregate`** — devuelve escalares agregados para la tabla (`count`; `sum`, `avg`, `stddev`, `variance` por columna numérica; `min`, `max` por columna comparable)
- **`Query{TypeName}GroupBy`** — devuelve una fila por clave de grupo con subcampos agregados y, opcionalmente, escalares de la tabla base y filas de dimensión incluida en un campo `nodes`

Ambos pasan por el mismo pipeline de agregados del compilador que los campos raíz `{field}_aggregate` y `{field}_group_by` de GraphQL, así que no hay una implementación de agregados aparte.

**Campo `funcs`.** El mensaje de solicitud acepta un campo `funcs` de cadena repetida. Los valores válidos son `count`, `sum`, `avg`, `stddev`, `variance`, `min` y `max`. Cuando se omite `funcs`, se solicita toda función que el esquema exponga para esa tabla. Cuando se establece, solo aparecen las funciones nombradas. Si ninguna de las funciones nombradas aplica a los tipos de columna de la tabla, la consulta recurre a `count`.

**Campos `include_nodes` e `include`.** Las solicitudes de `Query{TypeName}GroupBy` pueden establecer `include_nodes: true` para incluir las columnas escalares de la tabla base en el campo `nodes` de cada fila. El campo de cadena repetida `include` nombra campos de relación de muchos a uno cuyas columnas escalares también se anidan dentro de `nodes`. Esto coincide con el comportamiento `?includeNodes=` / `?include=` de JSON:API.

---

## Controlador JDBC {: #jdbc-driver }

El controlador JDBC de Provisa (`provisa-jdbc-<release>.jar`) expone el catálogo semántico a herramientas de BI (Tableau, PowerBI, DBeaver).

**URL de conexión:** `jdbc:provisa://host:port`

Los dominios se asignan a esquemas JDBC. Las tablas usan sus alias registrados. Las columnas usan alias y muestran las descripciones como `REMARKS`. Los métodos de metadatos estándar (`getPrimaryKeys`, `getImportedKeys`, `getExportedKeys`) exponen las relaciones semánticas como metadatos de clave primaria/clave foránea.

**Soporte SQL:** `SELECT * FROM <alias> [WHERE col = 'value']`.

El controlador solicita redirección Arrow IPC de forma predeterminada. Los resultados se transmiten lote por lote mediante `ArrowStreamReader`, acotados a un lote de registros en memoria.

---

## Formato del argumento `orderBy` {: #orderby-argument-format }

El argumento `order_by` usa objetos `{column: direction}` con una enumeración de dirección de 6 valores:

```json
{
  "query": "{ orders(order_by: [{created_at: desc_nulls_last}]) { id created_at } }",
  "role": "admin"
}
```

Direcciones admitidas: `asc`, `desc`, `asc_nulls_first`, `asc_nulls_last`, `desc_nulls_first`, `desc_nulls_last`.

---

## Suscripciones {: #subscriptions }

Las suscripciones SSE están disponibles en `GET /data/subscribe/{table}`. La entrega de notificaciones usa un proveedor conectable seleccionado según el tipo de origen: los orígenes PostgreSQL usan `LISTEN/NOTIFY`, los orígenes MongoDB usan Change Streams, y los orígenes Kafka usan grupos de consumidores. El filtrado RLS y la validación de esquema se aplican independientemente del proveedor. También se admiten orígenes WebSocket y RSS a través del mismo endpoint.

---

## Glosario de negocio {: #business-glossary }

El glosario de negocio asigna los nombres físicos de los campos, tal como aparecen en las bases de datos de origen, a un vocabulario humano compartido. Cada columna registrada en el modelo recibe un término automáticamente, así que nadie tiene que introducir nada para llenar el glosario. Los curadores añaden definiciones, relaciones y expertos sobre lo que el sistema deriva.

### Cómo se derivan los términos {: #how-terms-are-derived }

Cuando Provisa registra o actualiza las columnas de una tabla, `normalize_term` (`provisa/core/glossary.py`) se ejecuta sobre cada nombre de columna y produce una frase canónica.

La normalización aplica cinco reglas en secuencia:

1. Dividir en los límites de camelCase y los caracteres separadores (`_`, `-`, `.`, `/`, espacio en blanco).
2. Convertir el resultado a minúsculas.
3. Expandir una tabla fija de abreviaturas (p. ej. `cust` → `customer`, `amt` → `amount`, `dt` → `date`, `id` → `identifier`, `key` → `identifier`, `guid` → `identifier`).
4. Eliminar un **token proxy** final (`identifier`, `code`, `index` o `reference`). Una columna nombrada por su clave o código apunta al concepto subyacente a través de un valor sustituto, por lo que el término debe ser el concepto mismo. El último token restante nunca se elimina.
5. Calificar una **frase demasiado genérica** con el concepto de la tabla. Cuando la frase normalizada completa es una palabra de atributo desnuda (`name`, `identifier`, `date`, `location`, `message`, `first name`, `last name` y similares), el término se convierte en `<table concept> <phrase>`: `employees.first_name` → `employee first name`, `orders.id` → `order identifier`. Un término compartido `name` entre tablas no relacionadas fusionaría significados distintos; la calificación conecta cada columna con su concepto contenedor en su lugar. El concepto de tabla es el nombre de negocio de la tabla, normalizado con un sustantivo núcleo singular (`order_lines` → `order line`).

Las pseudo-columnas de filtro nativo (con prefijo `_nf_`, o cualquier columna que lleve `native_filter_type`) son mecanismos de parámetros de consulta, no campos de negocio, y no derivan términos.

Debido a que `id`, `key`, `pk` y `sk` se expanden todos a `identifier` antes de la verificación de proxy, tres nombres de columna físicamente distintos terminan en exactamente el mismo término:

| Nombre físico | Después de la normalización |
| --- | --- |
| `cust_id` | `customer` |
| `customerId` | `customer` |
| `CUSTOMER_KEY` | `customer` |
| `txn_amt` | `transaction amount` |

Los tres primeros se reducen a un solo término. `transaction amount` conserva ambos tokens porque `amount` no es un sustituto. Una columna `id` sola, sin tokens delante, no se puede recortar, así que se normaliza a `identifier` y el término no queda vacío.

### Ciclo de vida {: #lifecycle }

Los términos se **derivan de la pertenencia a la capa semántica**; los usuarios no los crean a demanda. El repositorio de tablas es la única vía de escritura: `sync_table_refs` se ejecuta dentro de cada upsert de un conjunto de columnas, y `sweep_refless_terms` se ejecuta tras cualquier vía de borrado.

**Cuando se agrega una columna:** Provisa busca el término normalizado por nombre. Si ya existe, la columna obtiene una referencia a él (y si el término estaba obsoleto, se revive, y `deprecated` se restablece a `False`). Si aún no existe ningún término, se crea uno.

**Cuando una columna desaparece** (un cambio de esquema o la retirada de una tabla), su referencia se borra y el término se **resuelve** con una regla de eliminar o depreciar. Un término raíz sin referencias restantes se elimina directamente, junto con sus aristas y asignaciones de expertos, salvo que eliminarlo dejara un término abstracto sin camino en el grafo de términos hasta ningún término raíz. En ese caso el término se **deprecia** (se marca `deprecated=True`) en lugar de borrarse, para que el término abstracto conserve su ancla en el grafo.

Los términos abstractos nunca se eliminan automáticamente; existen fuera del ciclo de vida físico y solo se eliminan explícitamente mediante la API de administración.

**Revivificación:** si el nombre normalizado de un término obsoleto reaparece (se vuelve a registrar una columna), el término se desmarca y sus referencias reanudan su acumulación.

### Endpoints de curación {: #curation-endpoints }

Todos los endpoints están bajo `/admin/glossary`. Requieren acceso `org_admin` y una organización configurada. Cada mutación activa una publicación de metadatos.

| Método | Ruta | Descripción |
| --- | --- | --- |
| `GET` | `/admin/glossary/terms` | Lista términos. Parámetros de consulta: `q` (búsqueda por nombre/definición), `include_deprecated` (predeterminado `true`) |
| `GET` | `/admin/glossary/terms/{term_id}` | Obtiene el detalle de un término: definición, referencias físicas, aristas tipadas, expertos |
| `POST` | `/admin/glossary/terms` | Crea un término abstracto — vocabulario de usuario sin referencias físicas |
| `PATCH` | `/admin/glossary/terms/{term_id}` | Renombra, establece la definición o alterna la exclusión de exportación |
| `DELETE` | `/admin/glossary/terms/{term_id}` | Elimina un término que no tiene referencias físicas |
| `POST` | `/admin/glossary/refs/move` | Mueve una referencia física a otro término (consolidación) |
| `POST` | `/admin/glossary/terms/{term_id}/edges` | Agrega una arista de relación tipada entre dos términos |
| `DELETE` | `/admin/glossary/terms/{term_id}/edges` | Elimina una arista (parámetros de consulta: `to_term_id`, `rel_type`) |
| `POST` | `/admin/glossary/terms/{term_id}/experts` | Etiqueta a un usuario como experto o autor de un término |
| `DELETE` | `/admin/glossary/terms/{term_id}/experts/{user_id}` | Elimina la designación de experto/autor de un usuario |
| `POST` | `/admin/glossary/terms/{term_id}/definition/generate` | Redacta una definición para un término usando el modelo de IA de la organización — devuelve solo texto, nada se persiste hasta guardarlo |
| `POST` | `/admin/glossary/definitions/generate` | Genera y persiste definiciones para cada término que no tenga ninguna — nunca sobrescribe texto redactado por humanos |
| `POST` | `/admin/glossary/relationships/generate` | Propone y persiste aristas tipadas en todo el glosario usando el modelo de IA de la organización |

**Cuerpo de `POST /admin/glossary/terms`:**

```json
{"name": "revenue", "definition": "Recognized net revenue after returns and discounts."}
```

**Cuerpo de `POST /admin/glossary/terms/{term_id}/edges`:**

```json
{"to_term_id": 42, "rel_type": "KIND_OF"}
```

Valores válidos de `rel_type`: `KIND_OF`, `RELATED_TO`, `PART_OF`, `SYNONYM_OF`.

**Cuerpo de `POST /admin/glossary/terms/{term_id}/experts`:**

```json
{"user_id": "alice@example.com", "kind": "author"}
```

Valores válidos de `kind`: `expert`, `author`.

**Cuerpo de `POST /admin/glossary/refs/move`:**

```json
{"table_id": 7, "column_name": "cust_id", "to_term_id": 12}
```

Mover una referencia resuelve el término que la pierde con la regla de eliminar o depreciar. Úselo para fusionar dos términos que la normalización mantuvo separados, por ejemplo cuando un origen usa una abreviatura no estándar que la tabla de expansión no cubre.

Eliminar un término enraizado (uno con referencias físicas) devuelve `400 glossary.invalid`. Elimine o mueva primero todas las referencias.

**`PATCH /admin/glossary/terms/{term_id}` — campo `export_excluded`:**

```json
{"export_excluded": true}
```

Establecer `export_excluded` en `true` retiene el término de todos los snapshots de exportación de metadatos, sin importar sus referencias físicas o su estado abstracto. Establecerlo de nuevo en `false` restaura el término al snapshot en la siguiente publicación. Los datos de curación (definición, aristas, expertos) no se ven afectados.

### Curación asistida por IA {: #ai-assisted-curation }

El modelo de IA configurado de la organización puede redactar definiciones y proponer aristas de relación en todo el glosario en una sola operación. Ambas acciones masivas requieren acceso `org_admin` y una organización configurada.

**`POST /admin/glossary/definitions/generate`**

Recorre todos los términos del glosario, omite los que ya tienen definición y pide al modelo de IA de la organización que redacte una para cada uno de los demás. Cada borrador se guarda al momento, sin paso de edición, a diferencia del endpoint de borrador por término (`POST /admin/glossary/terms/{term_id}/definition/generate`). Una definición escrita por una persona nunca se sobrescribe, porque un término que ya la tiene se omite antes de cualquier llamada al modelo. Una sola notificación de publicación cubre todo el lote.

Respuesta:

```json
{"generated": 12}
```

`generated` es el recuento de términos que recibieron una nueva definición. Es cero cuando cada término ya tiene una.

**`POST /admin/glossary/relationships/generate`**

Envía la lista completa de términos al modelo de IA de la organización con un prompt que especifica los diez tipos de arista permitidos (`KIND_OF`, `PART_OF`, `SYNONYM_OF`, `RELATED_TO`, `VALID_VALUE_OF`, `DERIVED_FROM`, `REPLACES`, `PREFERRED_TERM_FOR`, `TRANSLATION_OF`, `ANTONYM_OF`) y solicita solo propuestas seguras. El modelo responde con un arreglo JSON; cada entrada se valida antes de cualquier escritura: los nombres de término desconocidos, las auto-aristas y los tipos de arista fuera de la enumeración cerrada se descartan silenciosamente. Las propuestas válidas se insertan/actualizan (upsert) de forma idempotente. Volver a ejecutar la acción no duplica aristas. Una sola notificación de publicación cubre el lote. El endpoint devuelve `{"added": 0}` de inmediato cuando el glosario contiene menos de dos términos no obsoletos.

Respuesta:

```json
{"added": 5}
```

`added` es el número de aristas escritas. Una arista que ya existía también cuenta: el upsert tiene éxito, pero los datos de la arista no cambian.

### Herramienta MCP `search_terms` {: #mcp-search_terms-tool }

```
search_terms(query, role=None, limit=25)
```

Busca en nombres y definiciones de términos con una coincidencia de subcadena sin distinción entre mayúsculas y minúsculas, hasta `limit` resultados. Cada resultado es el detalle completo del término: `name`, `definition`, `is_abstract`, `deprecated`, referencias físicas (con `source_id`, `schema_name`, `table_name`, `column_name`), aristas tipadas y asignaciones de expertos.

Use `search_terms` antes de escribir SQL para encontrar cada campo físico que representa un concepto por nombre. Por ejemplo, buscar `"order date"` devuelve el término y todas las columnas `order_dt`, `orderDate`, `ORDER_DATE` en cada tabla registrada.

### Herramientas de estadística MCP {: #mcp-statistics-tools }

```
describe(sql, role=None)
correlate(sql, columns, method="pearson", role=None)
regress(sql, target, features, kind="linear", role=None)
compare(sql, value, group, role=None)
forecast(sql, time, value, horizon, seasonal_periods=None, role=None)
```

Cada herramienta toma un conjunto de datos definido por `sql` y lo ejecuta por la misma canalización gobernada que `run_sql`, con el rol del llamante: se aplican las reglas de filas, el enmascaramiento y la redacción, el SQL que la canalización rechaza falla igual que en `run_sql`, y un rol que el llamante no tiene se rechaza. Cada una devuelve resultados, nunca las filas, con el método usado y el `n` contado.

| Herramienta | Devuelve |
| --- | --- |
| `describe` | Por columna: `count`, `missing`, `distinct`; para un número `mean`, `sd`, `min`, `max` y `quantiles` (p0, p5, p25, p50, p75, p95, p100); `top_values` (los cinco más frecuentes) |
| `correlate` | La `matrix` de correlación `pearson` o `spearman` de las `columns` numéricas, sobre las filas completas en todas ellas, y ese `n` |
| `regress` | `kind="linear"`: `coefficients` de mínimos cuadrados (`estimate`, `se`, `p_value`), `r_squared`, `adj_r_squared`. `kind="logistic"`: un `target` de dos valores, `coefficients` de máxima verosimilitud, `pseudo_r_squared`, `auc` y el `positive_value`. Una variable de texto se convierte en un indicador por valor salvo el primero |
| `compare` | El `n`, `mean` y `sd` de cada grupo; para dos grupos, la prueba t de Welch (`t`, `df`, `p_value`) y la d de Cohen; para más, ANOVA de un factor (`f`, `df_between`, `df_within`, `p_value`) y eta cuadrado |
| `forecast` | `horizon` puntos tras el último `time`, cada uno con `value`, `lower` y `upper` (intervalo de predicción del 95 %), por suavizado exponencial (ETS) con error aditivo, tendencia aditiva a partir de 10 puntos y estacionalidad aditiva cuando se indica `seasonal_periods` |

El motor calcula lo que puede sobre cada fila que lee el rol: los recuentos, recuentos distintos, medias, desviaciones estándar, mínimos y máximos de `describe`, y el `n`, la media y la varianza de cada grupo para `compare`. Un modelo ajustado y los cuantiles y valores más frecuentes de `describe` leen como máximo `mcp.stats_max_rows` filas (Admin → Settings → Limits; 100.000 por defecto). Cada respuesta que lee filas indica `rows_read`, `row_cap` y `truncated`, y el límite de filas propio de un rol se informa en `warnings`, como en `run_sql`.

Las herramientas de estadística devuelven solo resultados, así que las filas detrás de un análisis nunca entran en el contexto del modelo: minimización de datos por defecto. El control de acceso es lo que limita lo que un agente puede saber. Para mantener las identidades fuera del alcance de un agente, conceda a su rol solo las columnas que no identifican a nadie; un rol que puede leer una columna puede agregarla, y los agregados sobre grupos pequeños aún pueden identificar a personas. Consulte [Seguridad](security.md#statistics-over-mcp).

### Exportación de metadatos {: #metadata-export }

El grafo de términos del glosario se incluye en cada `MetadataSnapshot` construido por `build_snapshot`.

La exportación aplica los mismos filtros que el resto del snapshot:

- Un término marcado como `export_excluded` se retiene por completo, sin importar sus referencias físicas, su estado abstracto o si el catálogo de la organización está configurado.
- Un término enraizado se publica solo cuando al menos una de sus referencias físicas pertenece a una columna que pasa tanto el filtro de **Data Product** (el indicador `data_product` de la tabla debe ser `true`) como el filtro de columna **técnica** (las columnas etiquetadas `technical` se retienen).
- Un término enraizado cuyas referencias están todas retenidas por esos filtros se retiene junto con ellas.
- Los términos abstractos se publican incondicionalmente: son vocabulario de usuario, no están vinculados a columnas físicas.
- Una arista entre dos términos se publica solo cuando ambos términos extremos se publican.

Cada adaptador de proveedor publica el grafo de términos de forma nativa, en un contenedor de glosario que es de Provisa y que crea de forma idempotente, nunca en un glosario de catálogo existente:

| Proveedor | Contenedor | Términos | Relaciones | Obsolescencia |
| --- | --- | --- | --- | --- |
| Apache Atlas | "Provisa Glossary" (API de glosario) | términos de glosario, definición en `longDescription` | KIND_OF → `isA`, SYNONYM_OF → `synonyms`, RELATED_TO/PART_OF → `seeAlso` | marcador `[DEPRECATED]` en shortDescription |
| Atlan | Glosario de Provisa por qualifiedName estable | `longDescription` (nunca el `userDescription` editado por humanos) | misma asignación de Atlas | `certificateStatus = DEPRECATED` |
| DataHub | `urn:li:glossaryNode:provisa.<org>` | aspecto `glossaryTermInfo` por término | KIND_OF → Inherits, PART_OF → Contains (invertido), RELATED_TO/SYNONYM_OF → términos relacionados | aspecto de obsolescencia; los renombrados siguen la sucesión de URN |
| OpenMetadata | Glosario de Provisa vía `/v1/glossaries` | PUT indexado por fqn, los renombrados hacen PATCH-rebind por UUID almacenado | KIND_OF → jerarquía padre nativa, SYNONYM_OF → `synonyms`, otros → `relatedTerms` | `entityStatus` |
| Collibra | Dominio de tipo Glosario "Provisa Glossary" | activos Business Term vía la Import API | tipos de relación Business Term nativos | estado del activo |

Lo que cuenta es el vínculo, no el nombre. El id de proveedor de cada término publicado se guarda en `catalog_bindings` bajo el URN del término (`provisa://<org>/terms/<name>`), y Provisa modifica o borra un elemento de glosario del lado del proveedor solo cuando tiene ese vínculo, o cuando el elemento está en el contenedor que Provisa creó. Un elemento de glosario sin vínculo de Provisa vino del sistema externo y nunca se toca. Las actualizaciones leen y combinan, así que los campos que un steward añadió a los términos propios de Provisa se conservan, y nada se borra cuando un término sale de la instantánea. Cada término lleva su rol (clase, propiedad o concepto) como propiedad del término: `provisaRole` en las propiedades personalizadas de DataHub, una propiedad personalizada en OpenMetadata, los atributos adicionales del término en Atlas, Purview y Atlan, y el atributo `Provisa Role` en Collibra. En DataHub el nodo de Provisa de la organización tiene un nodo hijo por rol, llamados Classes, Properties y Concepts. OpenMetadata no tiene más nodo de glosario que un término, así que cada rol es un término de grupo que contiene los términos de nivel superior de ese rol, y un término con un padre `KIND_OF` se queda bajo ese padre. Provisa también asigna cada término a los datos que significa. Un término de clase se asigna a las tablas publicadas que fundamenta, un término de propiedad a sus columnas publicadas y un concepto a nada. Cada asignación se escribe como la asignación automática propia del catálogo y se atribuye a Provisa: una etiqueta `Automated` en OpenMetadata, una asociación de término atribuida al actor de Provisa en DataHub, una asignación semántica cuya fuente es Provisa en Atlas y Atlan, y una relación creada por el usuario de Provisa en Collibra. Provisa solo añade y retira esas, así que una asignación hecha por un steward nunca se elimina ni se duplica. Una asignación no se retira porque haya fallado una petición. En Collibra las asignaciones pasan por la API de relaciones y no por la Import API, porque la importación reemplazaría las relaciones del mismo tipo de un steward. En Collibra en concreto, la Import API reemplaza lo que recibe, y la seguridad se basa en la contención: la carga solo nombra activos dentro del dominio de glosario de Provisa y relaciones solo entre términos de Provisa, así que los glosarios de los stewards y sus relaciones quedan fuera de alcance.

---

## Productos de datos {: #data-products }

Un producto de datos agrupa tablas publicadas juntas para su consumo, propiedad de exactamente un dominio. Los campos siguen el vocabulario de ODPS (Open Data Product Standard) donde Provisa ya tiene la fuente de verdad. La interfaz de administración expone los productos de datos en **Admin → Data Products**.

### Capacidades {: #capabilities }

| Capacidad | Otorga |
| --- | --- |
| `data_product_read` | Acceso de lectura al campo de consulta `data_products` y a la página de administración de productos de datos. Asignado por defecto a `org_admin`, `analyst`, `developer` y `modeler`. |
| `data_product_rw` | Mutaciones de creación y eliminación. Habilita los controles de Nuevo / Editar / Eliminar en la interfaz. |

### Admin GraphQL {: #admin-graphql_1 }

Todas las operaciones de productos de datos pasan por `POST /admin/graphql`.

**Consulta:**

```graphql
query {
  data_products {
    id
    domain_id
    name
    owner_role
    team_role
    purpose
    limitations
    usage
    version
    status
    sla
    support
    custom_properties
  }
}
```

Requiere `data_product_read`.

**Crear o actualizar:**

```graphql
mutation {
  create_data_product(input: {
    id: "customer_360"
    domain_id: "sales"
    name: "Customer 360"
    owner_role: "data-product-owner"
    team_role: "sales-analytics"
    purpose: "Single view of a customer across all touchpoints."
    status: "active"
    version: "1.0.0"
  }) {
    success
    message
  }
}
```

`create_data_product` hace upsert: llamarlo con un `id` existente actualiza el registro. Requiere `data_product_rw`.

**Eliminar:**

```graphql
mutation {
  delete_data_product(id: "customer_360") {
    success
    message
  }
}
```

Eliminar un producto limpia `product_id` de cada tabla miembro, eliminando su pertenencia. Requiere `data_product_rw`.

### Esquema de campos {: #field-schema }

| Campo | Tipo | Obligatorio | Notas |
| --- | --- | --- | --- |
| `id` | `String` | Sí | Identificador estable legible por máquina, p. ej. `customer_360` |
| `domain_id` | `String` | Sí | Dominio propietario. Las tablas miembro deben compartir este `domain_id` — las discrepancias se rechazan en el momento de guardar |
| `name` | `String` | Sí | Nombre para mostrar |
| `owner_role` | `String` | No | Rol responsable de este producto; distinto del steward del dominio |
| `team_role` | `String` | No | Rol cuyos miembros mantienen este producto día a día; se resuelve en individuos |
| `purpose` | `String` | No | Qué publica este producto y por qué |
| `limitations` | `String` | No | Restricciones, advertencias o exclusiones conocidas |
| `usage` | `String` | No | Cómo consumir este producto |
| `version` | `String` | No | p. ej. `1.2.0` |
| `status` | `String` | No | p. ej. `proposed`, `active`, `deprecated`, `retired` |
| `sla` | `String` | No | Compromisos de nivel de servicio; texto — un producto abarca varias tablas y un SLA estructurado no puede nombrar sin ambigüedad qué miembro describe |
| `support` | `String` | No | Orientación de soporte en texto libre |
| `custom_properties` | `JSON` | No | Metadatos clave-valor arbitrarios no cubiertos por los campos estándar |

Hay dos campos más en el modelo que no se exponen en `DataProductType` / `DataProductInput`. Son propios del Snowflake Horizon Catalog:

| Campo | Notas |
| --- | --- |
| `support_contact` | Correo electrónico o URL; requerido por los manifiestos de ficha de organización del Catálogo Horizon |
| `publish` | `true` para publicar fichas de Horizon de inmediato; las nuevas fichas son DRAFT por defecto |

### Pertenencia de tablas {: #table-membership }

Una tabla se une a un producto de datos cuando se establece su campo `product_id` en el formulario de edición de la tabla. El selector solo ofrece productos cuyo `domain_id` coincide con el dominio de la tabla, así que a una tabla del dominio `marketing` nunca se le ofrece un producto del dominio `sales`.

Los comandos del mismo dominio también pueden asignarse como miembros.

### Filtro de exportación de metadatos {: #metadata-export-filter }

`build_snapshot` aplica `data_products_only=True` en cada publicación de catálogo. Las tablas sin `product_id` se retienen del snapshot, junto con sus aristas de relación, aristas de linaje y etiquetas de gobierno. Los orígenes y dominios siempre se publican. Los términos del glosario se publican solo cuando al menos una de sus referencias físicas pertenece a una tabla exportada (miembro de un producto).

Un producto sin miembros exportados no recibe entrada en la instantánea, porque un listado sin miembros daría al catálogo una imagen falsa del producto.

### Soporte de productos de datos por destino de catálogo {: #data-product-support-by-catalog-target }

`MetadataSnapshot.data_products` llega a cada adaptador, pero solo los adaptadores cuya plataforma tiene un concepto nativo de producto de datos lo publican como entidad propia; el resto publica las tablas miembro (ya filtradas arriba) sin agrupación de producto.

| Destino | Representación del producto de datos |
| --- | --- |
| Snowflake Horizon | Cada producto se convierte en un `SHARE` sobre las direcciones físicas de sus tablas miembro, envuelto en un `CREATE ORGANIZATION LISTING` interno — un Data Product nativo del Catálogo Horizon. `publish=true` pone la ficha en activo de inmediato; de lo contrario queda como DRAFT. |
| BigQuery Dataplex | Cada producto se convierte en una ficha de Analytics Hub a través de `/v1/dataProducts`. |
| OpenMetadata | Cada producto se convierte en una entidad `DataProduct` nativa (`/api/v1/dataProducts`), con propiedad derivada del dominio. |
| DataHub | Cada producto se convierte en una entidad `dataProduct` nativa (`urn:li:dataProduct:...`) con sus propios aspectos `dataProductProperties`/propiedad. |
| Collibra | Cada producto se convierte en un activo de tipo de comunidad `Data Product`, relacionado con sus tablas miembro mediante una relación `Data Product groups Table`. |
| Atlan | Publicado como una suposición de typedef personalizado `DataProduct` — Atlan no tiene un nombre de tipo estable documentado para este concepto, por lo que la asignación es con mejor esfuerzo. |
| Apache Atlas | Publicado como un typedef personalizado `provisa_data_product` con una relación `provisa_data_product_members` — Atlas no tiene tipo de entidad nativo para productos de datos. |
| OpenLineage | Sin entidad propia; las tablas miembro llevan una faceta personalizada `provisa_data_product` que nombra el producto propietario. |
