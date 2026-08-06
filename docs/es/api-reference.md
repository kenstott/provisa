# Referencia de la API

## Descripción general

Provisa expone endpoints REST bajo dos prefijos: `/data` para la ejecución de consultas y la introspección de esquemas, y `/admin` para la gestión de configuración. (REQ-043) La mayoría de los endpoints de datos requieren un identificador de rol. Las operaciones de configuración de administración usan una API de Strawberry GraphQL en `/admin/graphql`. (REQ-164)

---

## Autenticación

Cuando `auth.provider` está configurado en `provisa.yaml`, todos los endpoints excepto `/health` y `/setup/status` requieren un encabezado `Authorization: Bearer <token>`. (REQ-120) [tool-verified: `provisa/api/app.py`, `provisa/auth/wiring.py`]

Sin autenticación configurada, el servidor se ejecuta en modo de desarrollo. Cualquier solicitud se trata como la identidad `anonymous`, que se asigna a todos los roles configurados con acceso de dominio comodín. (REQ-535)

**Inicio de sesión (`POST /auth/login`)** lo proporciona el proveedor de autenticación activo cuando `provider: basic` está configurado. (REQ-124) El formato de credenciales y la respuesta dependen del proveedor.

**Introspección de identidad:**

```http
GET /auth/me
```

Devuelve el id, correo electrónico, nombre para mostrar, membresías de organización y asignaciones de rol del usuario autenticado. En modo de desarrollo devuelve `dev_mode: true` con todos los IDs de rol listados. [tool-verified: `provisa/api/auth_router.py`]

```http
GET /auth/provider-type
```

Devuelve `{"provider": "<name>"}` o `{"provider": null}` cuando la autenticación no está configurada. [tool-verified: `provisa/api/auth_router.py`]

---

## Endpoints de datos

### `POST /data/graphql`

Ejecuta una consulta o mutación GraphQL. (REQ-043) [tool-verified: `provisa/api/data/endpoint.py:151`]

**Cuerpo de la solicitud:**

```json
{
  "query": "{ orders(where: {region: {eq: \"us\"}}) { id amount } }",
  "variables": {},
  "role": "admin",
  "extensions": {}
}
```

El campo `role` se usa solo en modo de desarrollo (sin autenticación). Cuando la autenticación está activa, se usa el rol del usuario autenticado y el `role` del cuerpo se ignora.

El campo `extensions` admite el protocolo Automatic Persisted Query (APQ): (REQ-288)

```json
{
  "extensions": {"persistedQuery": {"sha256Hash": "<sha256-of-query>"}}
}
```

**Encabezados:**

- `X-Provisa-Role` — anula el rol (modo de desarrollo)
- `Accept` — formato de respuesta (ver Negociación de contenido)
- `Authorization` — `Bearer <token>` cuando la autenticación está habilitada
- `X-Provisa-Redirect-Format` — tipo MIME para la salida de redirección a S3 (REQ-137)
- `X-Provisa-Redirect-Threshold` — cantidad de filas por encima de la cual se activa la redirección (REQ-137)
- `X-Provisa-Redirect` — `true` para forzar la redirección incondicionalmente (REQ-029)

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

Las consultas con múltiples raíces ejecutan cada campo raíz de forma independiente. Los campos por debajo del umbral de redirección se devuelven en línea; los que están por encima se redirigen. La clave `redirects` (plural) asigna nombres de campo a información de redirección. (REQ-029) [tool-verified: `provisa/api/data/endpoint.py`]

**Encabezados de caché:**

- `X-Provisa-Cache: HIT|MISS` (REQ-536)
- `X-Provisa-Cache-Age: <seconds>` (en HIT) (REQ-536)

**Capacidades requeridas:** `QUERY_DEVELOPMENT` para todas las solicitudes, incluida la introspección. [tool-verified: `provisa/api/data/endpoint.py:186-283`]

---

### Negociación de contenido

| Encabezado Accept | Formato |
| --- | --- |
| `application/json` | JSON (predeterminado) |
| `application/x-ndjson` | JSON delimitado por saltos de línea |
| `text/csv` | CSV |
| `application/vnd.apache.parquet` | Parquet |
| `application/vnd.apache.arrow.stream` | Arrow IPC |

(REQ-047, REQ-048, REQ-049, REQ-050) [tool-verified: `provisa/api/data/endpoint.py:84-90`]

---

### Redirección

Los resultados que superan un umbral de filas configurado (o cuando `X-Provisa-Redirect: true`) se escriben en S3 y se devuelve una URL prefirmada. (REQ-029, REQ-044)

| Formato de redirección | Escrito por | Memoria |
| --- | --- | --- |
| `application/vnd.apache.parquet` | CTAS federado | Ninguna — los datos nunca pasan por Provisa |
| `application/x-orc` | CTAS federado | Ninguna — los datos nunca pasan por Provisa |
| `application/json` | Provisa | Limitado por memoria |
| `application/x-ndjson` | Provisa | Limitado por memoria |
| `text/csv` | Provisa | Limitado por memoria |
| `application/vnd.apache.arrow.stream` | Provisa | Limitado por memoria |

Para exportaciones analíticas grandes, use redirección Parquet u ORC. El motor de federación escribe directamente en S3 en paralelo — ningún dato pasa por Provisa. (REQ-138)

```yaml
X-Provisa-Redirect-Format: application/vnd.apache.parquet
X-Provisa-Redirect-Threshold: 1000
```

---

### `POST /data/sql`

Ejecuta SQL sin procesar a través del pipeline de gobierno de la Etapa 2. (REQ-267) [tool-verified: `provisa/api/data/endpoint_dev.py:62`]

**Cuerpo de la solicitud:**

```json
{
  "sql": "SELECT id, amount FROM orders WHERE region = 'us'",
  "role": "admin"
}
```

**Capacidades requeridas:** `QUERY_DEVELOPMENT`.

Las violaciones de gobierno en `POST /data/sql` devuelven HTTP 403. (REQ-002, REQ-266)

**Respuesta:** Mismo formato que `/data/graphql` (filas JSON de forma predeterminada, negociadas por contenido mediante `Accept`).

---

### `POST /data/query`

Endpoint de consulta unificado. Acepta GraphQL, SQL o Cypher — la sintaxis se detecta automáticamente. (REQ-267) [tool-verified: `provisa/api/data/endpoint_dev.py:509`]

Las consultas Cypher también pueden enviarse al endpoint exclusivo de Cypher `POST /query/cypher`. (REQ-345)

**Cuerpo de la solicitud:**

```json
{
  "query": "{ orders { id } }",
  "params": {},
  "variables": {},
  "role": "admin"
}
```

Devuelve `{"data": ...}` para GraphQL, `{"columns": [...], "rows": [...]}` para SQL y Cypher.

---

### `GET /data/rest/{domain_id}/{table_name}`

Endpoint REST plano generado automáticamente para cada tabla registrada. La cadena de consulta se asigna a argumentos GraphQL y la solicitud se compila y ejecuta a través del mismo pipeline (RLS, enmascaramiento, enrutamiento) que GraphQL. (REQ-256) [tool-verified: `provisa/api/rest/generator.py:153`]

**Parámetros de consulta:**

- `limit` — máximo de filas (≥ 1)
- `offset` — filas a omitir (≥ 0)
- `fields` — nombres de columnas separados por comas (por defecto, todos los campos escalares)
- `filter` — arreglo JSON de objetos de filtro `{"field", "comparator", "value"}`
- `orderBy` — arreglo JSON de objetos de ordenamiento `{"field", "direction"}`

Se requiere el rol autenticado; las solicitudes no autenticadas devuelven `401`. Se sirve una especificación OpenAPI para estas rutas en `GET /data/rest/openapi.json`, con Swagger UI en `GET /data/rest/docs`.

---

### `GET /data/jsonapi/{domain_id}/{table_name}`

Endpoint compatible con [JSON:API](https://jsonapi.org) generado automáticamente para cada tabla registrada. Mismo RLS, enmascaramiento y enrutamiento que GraphQL. (REQ-257) [tool-verified: `provisa/api/jsonapi/generator.py:284`]

**Encabezado `Accept`:** debe incluir `application/vnd.api+json` (el tipo de medio JSON:API) o la solicitud devuelve `406`.

**Parámetros de consulta:**

- `fields[<type>]` — conjuntos de campos dispersos (sparse fieldsets), p. ej. `?fields[orders]=amount`
- `filter[<col>]` / `filter[<col>][<op>]` — p. ej. `?filter[region]=US`, `?filter[amount][gt]=100`
- `sort` — separado por comas, prefijo `-` para orden descendente, p. ej. `?sort=-created_at,amount`
- `page[number]` / `page[size]` — paginación

Las respuestas son objetos de recurso con `type`/`id`/`attributes`. Los errores siguen la forma del objeto de error de JSON:API.

---

### `POST /query/nl`

Envía una pregunta en lenguaje natural. El servicio inicia un trabajo asíncrono y devuelve `202 Accepted` con un `job_id` de inmediato. Requiere un proveedor de LLM configurado en la sección de configuración `ai_models`. (REQ-354) [tool-verified: `provisa/api/rest/nl_router.py:50`]

**Cuerpo de la solicitud:**

```json
{"q": "How many orders were placed last month?", "role": "admin"}
```

Devuelve `{"job_id": "<id>"}`. Superar el límite de frecuencia de NL por rol devuelve `429` con un encabezado `Retry-After`. (REQ-370)

**Obtener el resultado:**

- `GET /query/nl/{job_id}` — sondeo (polling). Devuelve el documento del trabajo.
- `GET /query/nl/{job_id}/stream` — SSE. Un evento `branch` por cada objetivo de generación a medida que se completa, seguido de un evento `done`. (REQ-357, REQ-358)

Tres bucles de generación (Cypher, GraphQL, SQL) se ejecutan en paralelo, cada uno validado mediante el compilador y refinado ante errores. (REQ-355) El prompt se limita al esquema visible del rol. (REQ-356) El documento de resultado clasifica cada rama por objetivo: (REQ-357) [tool-verified: `provisa/nl/job.py:69`]

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

Una rama que agota su límite de iteraciones devuelve `query: null`, `result: null` y una cadena `error`. Cada consulta generada se ejecuta bajo los derechos del consumidor con el gobierno de la Etapa 2 aplicado — el servicio nunca omite el gobierno. (REQ-359)

---

### `GET /data/sdl`

Devuelve el SDL de GraphQL para el esquema de un rol. (REQ-008) [tool-verified: `provisa/api/data/sdl.py:137`]

**Encabezados:** `X-Role: <role_id>` (obligatorio)

**Parámetros de consulta:**

- `domain` — IDs de dominio separados por comas. Cuando se establece, la respuesta se filtra al dominio (o dominios) indicado y a las tablas accesibles desde ellos.

**Respuesta:** SDL de GraphQL en `text/plain`.

---

### `GET /data/introspection`

Devuelve el JSON de introspección de GraphQL, opcionalmente filtrado por dominio. [tool-verified: `provisa/api/data/sdl.py:200`]

**Encabezados:** `X-Provisa-Role: <role_id>` (obligatorio)

**Parámetros de consulta:** `domain` — IDs de dominio separados por comas.

**Respuesta:** resultado de introspección en `application/json`.

---

### `GET /data/graph-schema`

Devuelve la vista de grafo del esquema del rol: etiquetas de nodo y sus tipos de relación, para clientes Cypher/grafo. Incluye `pk_columns` por etiqueta de nodo para que los llamadores puedan determinar las columnas de clave primaria. (REQ-398) [tool-verified: `provisa/api/rest/cypher_router.py:689`]

**Respuesta:** `application/json` con `node_labels` (cada una con `pk`/`pk_columns`) y `relationship_types`.

---

### `GET /data/domains`

Devuelve los IDs de dominio accesibles para el rol solicitante. [tool-verified: `provisa/api/data/sdl.py:116`]

**Encabezados:** `X-Role: <role_id>` (obligatorio)

**Respuesta:** `["sales", "support", ...]`

---

### `GET /data/schema-version`

Devuelve la cadena de versión del esquema actual. Combina un nonce por arranque con un contador de reconstrucción. Los clientes lo usan para invalidar cachés de esquema tras reinicios del servidor. (REQ-537) [tool-verified: `provisa/api/data/sdl.py:102`]

**Respuesta:** `{"version": "<boot-id>-<counter>"}`

---

### `GET /data/proto/{role_id}`

Devuelve el archivo `.proto` generado automáticamente para un rol. [tool-verified: `provisa/api/data/endpoint_dev.py:49`]

**Respuesta:** esquema protobuf en `text/plain`.

Cada tabla registrada produce un `message` proto. Las relaciones producen campos de mensaje anidados. Asignación de tipos: `integer → int32`, `bigint → int64`, `varchar → string`, `decimal → double`, `boolean → bool`, `timestamp → google.protobuf.Timestamp`. (REQ-538)

---

### `GET /data/subscribe/{table}`

Flujo de Server-Sent Events para notificaciones de cambios en tiempo real de una tabla. (REQ-219, REQ-258) [tool-verified: `provisa/api/data/subscribe.py:239`]

La entrega de notificaciones usa un proveedor conectable elegido según el tipo de origen: los orígenes PostgreSQL usan `LISTEN/NOTIFY` (a través de asyncpg), los orígenes MongoDB usan Change Streams (`collection.watch()`), y los orígenes Kafka usan grupos de consumidores. Cada proveedor implementa una interfaz de observación asíncrona común. El filtrado RLS y la validación de esquema se aplican independientemente del proveedor. (REQ-258) También se admiten orígenes WebSocket y RSS. (REQ-338, REQ-342)

**Encabezado — `X-Provisa-Sink`:** Establézcalo en un destino Kafka (p. ej. `kafka://broker:9092/topic`) para redirigir los eventos de cambio a un sink de Kafka en lugar de la respuesta SSE. El servidor inicia un consumidor de sink y devuelve `202 Accepted` en lugar de un flujo abierto. (REQ-812) [tool-verified: `provisa/api/data/subscription_sse.py:137`]

---

## Endpoints REST de administración

### Config

#### `GET /admin/config`

Descarga el `provisa.yaml` actual como `application/x-yaml` con un encabezado `Content-Disposition: attachment`. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:19`]

#### `PUT /admin/config`

Sube un YAML de configuración revisado. El servidor escribe una copia de seguridad `.bak`, guarda el nuevo archivo y recarga todos los esquemas, orígenes y vistas materializadas. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:32`]

**Cuerpo de la solicitud:** Contenido YAML sin procesar.

**Respuesta:**

```json
{"success": true, "message": "Config uploaded and reloaded"}
```

En caso de error de recarga: `{"success": false, "message": "<error>"}`.

---

### Configuración (Settings)

#### `GET /admin/settings`

Devuelve la configuración actual de la plataforma como JSON. (REQ-165) [tool-verified: `provisa/api/admin/settings_router.py:50`]

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

Actualiza la configuración de la plataforma en tiempo de ejecución. Todos los campos son opcionales — solo se actualizan las claves presentes en el cuerpo. (REQ-165) [tool-verified: `provisa/api/admin/settings_router.py:100`]

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
- `naming`: `domain_prefix`, `convention` — escribe en el archivo de configuración y activa la recarga de esquema (REQ-253)
- `relationships`: `auto_track_fk`
- `otel`: `endpoint`, `service_name`, `sample_rate`, `support_endpoint`, `support_redact_sql_literals`, `support_redact_attributes`

**Respuesta:**

```json
{"success": true, "updated": ["otel.support_endpoint", "cache.default_ttl"]}
```

---

### Observabilidad

#### `GET /admin/traces/recent`

Devuelve hasta N spans completados recientes del búfer de spans en memoria. (REQ-302) [tool-verified: `provisa/api/admin/settings_router.py:317`]

**Parámetros de consulta:** `limit` (predeterminado 50, máximo 200)

**Respuesta:** `{"traces": [...]}`

#### `POST /admin/query-engine/reload-catalog`

Recarga en caliente un catálogo con nombre en el coordinador del motor de federación a través de su API REST. Reconecta la conexión interna de Provisa y vuelve a ejecutar el DDL de OTel. [tool-verified: `provisa/api/admin/settings_router.py:208`]

**Parámetros de consulta:** `catalog` (predeterminado `"otel"`)

**Respuesta:**

```json
{"success": true, "errors": []}
```

#### `POST /admin/query-engine/restart`

Reinicia el contenedor del motor de federación (solo para desarrollo de un solo nodo). [tool-verified: `provisa/api/admin/settings_router.py:287`]

**Parámetros de consulta:** `container` (por defecto la variable de entorno `QUERY_ENGINE_CONTAINER`, luego `"trino"`)

---

### Descubrimiento

#### `POST /admin/discover/relationships`

Activa el descubrimiento de relaciones. Siempre ejecuta la introspección de claves foráneas desde el motor de federación. (REQ-018) Ejecuta inferencia por LLM si `ANTHROPIC_API_KEY` está configurada. (REQ-167) [tool-verified: `provisa/api/admin/discovery.py:55`]

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

Lista los candidatos de relación pendientes. [tool-verified: `provisa/api/admin/discovery.py:96`]

#### `POST /admin/discover/candidates/{candidate_id}/accept`

Acepta un candidato y lo registra como relación. [tool-verified: `provisa/api/admin/discovery.py:103`]

**Cuerpo de la solicitud (opcional):** `{"name": "custom-relationship-name"}`

#### `POST /admin/discover/candidates/{candidate_id}/reject`

Rechaza un candidato. [tool-verified: `provisa/api/admin/discovery.py:110`]

**Cuerpo de la solicitud:** `{"reason": "Not a real join"}`

#### `GET /admin/discover/candidates/rejected/count`

Devuelve el recuento de candidatos rechazados. [tool-verified: `provisa/api/admin/discovery.py:118`]

#### `DELETE /admin/discover/candidates/rejected`

Elimina todos los candidatos rechazados. [tool-verified: `provisa/api/admin/discovery.py:128`]

---

### Rastreo de orígenes (Source Crawl)

#### `POST /admin/sources/crawl`

Rastrea un origen de datos para hacer introspección de su esquema y registrar tablas. (REQ-012) [tool-verified: `provisa/api/admin/crawl_router.py:36`]

---

### Búsqueda de tablas de origen

#### `GET /admin/sources/{source_id}/tables/search`

Busca por nombre tablas disponibles (aún no registradas) en un origen. [tool-verified: `provisa/api/admin/table_search_router.py:103`]

---

### Perfilado de tablas

#### `POST /admin/tables/{table_id}/profile`

Ejecuta un perfil de columnas en una tabla registrada — cardinalidad, mínimo/máximo, tasas de nulos. [tool-verified: `provisa/api/admin/table_profile_router.py:28`]

---

### Descripciones de origen

#### `POST /admin/source-meta/db-description`

Genera descripciones asistidas por LLM para las tablas y columnas de un origen. [tool-verified: `provisa/api/admin/source_meta_router.py:48`]

---

### Acciones (funciones y webhooks)

Todos los endpoints están bajo el prefijo `/admin/actions`. (REQ-205) [tool-verified: `provisa/api/admin/actions_router.py:24`]

Cada invocación — desde GraphQL, SQL, Cypher, Bolt, Arrow Flight, MCP `run_sql` y Provisa gRPC — se enruta a través de un único ejecutor gobernado que aplica `writable_by` y el gobierno de manera uniforme. (REQ-1156) [tool-verified: `provisa/api/data/action_exec.py`] Vea [docs/integrations.md](integrations.md#invocar-comandos-entre-protocolos) para la sintaxis de llamada por protocolo.

#### `GET /admin/actions`

Devuelve todas las funciones de BD y webhooks rastreados. (REQ-242) [tool-verified: `provisa/api/admin/actions_router.py:104`]

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
      "writableBy": [],
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

Cada objeto de webhook lleva un booleano `approved`. Un webhook queda aprobado en cuanto un steward ejecuta su solicitud de creación (REQ-209); los webhooks declarados en la configuración se aprueban automáticamente. Un webhook no aprobado queda registrado pero no se expone en ninguna superficie. [tool-verified: `provisa/api/admin/actions_router.py:124-131`]

#### `POST /admin/actions/functions`

Registra una función rastreada (comando). (REQ-205) [tool-verified: `provisa/api/admin/actions_router.py:117`]

**Campos clave:**

| Campo | Obligatorio | Descripción |
| --- | --- | --- |
| `name` | Sí | Nombre único del comando |
| `kind` | Sí | `"query"` → campo de Query GraphQL; `"mutation"` → campo de Mutation |
| `implKind` | No | Cómo se ejecuta el comando — ver tabla siguiente (predeterminado `source_procedure`) |
| `binding` | No | Detalles de conexión específicos de `implKind` (objeto JSON) |
| `returnSchema` | No | JSON Schema `{type:"array", items:{type:"object", properties:{...}}}` — hace que el comando devuelva un conjunto en cada superficie |
| `arguments` | No | Definiciones de argumento `[{name, type}]`; el orden posicional importa para llamadores SQL y Bolt |
| `visibleTo` | No | IDs de rol que pueden llamar al comando |
| `writableBy` | No | IDs de rol autorizados a invocarlo como mutación |
| `domainId` | No | Dominio para la ubicación en GraphQL y el control de acceso |

**Valores de `implKind`:**

| `implKind` | Qué se ejecuta | Campos de `binding` |
| --- | --- | --- |
| `source_procedure` | Procedimiento almacenado en un origen registrado (predeterminado) | `sourceId`, `schemaName`, `functionName` |
| `script` | Script del lado del servidor | `script` |
| `http` | Llamada HTTP saliente | `url`, `method` |
| `grpc` | Llamada gRPC saliente a un servidor externo | `target`, `method` |
| `python` | Callable de Python alojado por Provisa (REQ-885) | `callable` (p. ej. `"demo.py_functions:random_dataset"`) |

Los comandos de demostración `random_python_set` (`implKind: python`) y `random_grpc_set` (`implKind: grpc`) muestran en la práctica comandos que devuelven conjuntos con `returnSchema`; ambos están en `config/provisa-install.yaml`. [tool-verified: `config/provisa-install.yaml:809-856`]

#### `PUT /admin/actions/functions/{name}`

Actualiza una función rastreada por nombre. [tool-verified: `provisa/api/admin/actions_router.py:182`]

#### `DELETE /admin/actions/functions/{name}`

Elimina una función rastreada por nombre. [tool-verified: `provisa/api/admin/actions_router.py:233`]

#### `POST /admin/actions/webhooks`

Registra un webhook rastreado. (REQ-209) Registrar o actualizar un webhook encola una solicitud de aprobación del steward — el webhook queda activo en todas las superficies solo después de que un steward lo aprueba. Los webhooks declarados en la configuración se aprueban automáticamente. **Campos del cuerpo de la solicitud:** `name`, `url`, `method`, `timeoutMs`, `returns`, `inlineReturnType`, `arguments`, `visibleTo`, `domainId`, `description`, `kind`. [tool-verified: `provisa/api/admin/actions_router.py:132`, `provisa/api/admin/actions_router.py:325-331`]

#### `PUT /admin/actions/webhooks/{name}`

Actualiza un webhook rastreado por nombre. Cualquier edición reinicia la aprobación a pendiente hasta que se vuelva a aprobar. [tool-verified: `provisa/api/admin/actions_router.py:306`]

#### `DELETE /admin/actions/webhooks/{name}`

Elimina un webhook rastreado por nombre. [tool-verified: `provisa/api/admin/actions_router.py:355`]

#### `POST /admin/actions/test`

Prueba una acción (función o webhook) por nombre. (REQ-245) [tool-verified: `provisa/api/admin/actions_router.py:384`]

---

### Roles

Todos los endpoints están bajo el prefijo `/admin/roles`. [tool-verified: `provisa/api/admin/roles_router.py:18`]

| Método | Ruta | Descripción |
| --- | --- | --- |
| `GET` | `/admin/roles/` | Lista todos los roles |
| `POST` | `/admin/roles/` | Crea un rol |
| `PUT` | `/admin/roles/{role_id}` | Actualiza un rol |
| `DELETE` | `/admin/roles/{role_id}` | Elimina un rol |

[tool-verified: `provisa/api/admin/roles_router.py`]

---

### Usuarios

Todos los endpoints están bajo el prefijo `/admin/users`. [tool-verified: `provisa/api/admin/local_users_router.py:21`]

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

### Organizaciones

Todos los endpoints están bajo `/admin/orgs`. [tool-verified: `provisa/api/admin/orgs_router.py:18`]

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

### Invitaciones

Todos los endpoints están bajo `/admin/invites`. [tool-verified: `provisa/api/admin/invites_router.py:18`]

| Método | Ruta | Descripción |
| --- | --- | --- |
| `POST` | `/admin/invites/` | Crea una invitación |
| `GET` | `/admin/invites/` | Lista las invitaciones pendientes |
| `DELETE` | `/admin/invites/{token}` | Revoca una invitación |

---

### GraphQL de administración

#### `POST /admin/graphql`

Endpoint de Strawberry GraphQL para todas las operaciones de administración: CRUD de orígenes y tablas, gestión de relaciones, configuración de dominios, reglas de RLS, control de caché, convenciones de nomenclatura, gestión de tareas programadas y compilación de consultas. (REQ-164) [tool-verified: `provisa/api/app.py:2171`]

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

[tool-verified: `provisa/api/admin/schema.py`, `provisa/api/admin/actions_router.py`]

---

### Configuración inicial (Setup)

#### `GET /setup/status`

Devuelve el estado de la configuración de primer arranque. Siempre sin autenticación. (REQ-539) [tool-verified: `provisa/api/setup_router.py:100`]

#### `POST /setup/`

Completa la configuración de primer arranque. [tool-verified: `provisa/api/setup_router.py:142`]

---

## Verificación de estado (Health Check)

#### `GET /health` o `HEAD /health`

Devuelve `{"status": "ok"}`. Siempre sin autenticación. (REQ-539) [tool-verified: `provisa/api/app.py:2258`]

---

## Respuestas de error

| Estado | Significado |
| --- | --- |
| 400 | Consulta inválida, error de validación o error de análisis SQL |
| 401 | Token de autenticación ausente o inválido |
| 403 | Capacidades insuficientes; violación de gobierno |
| 404 | Rol, recurso o archivo de configuración no encontrado |
| 422 | Falta un encabezado obligatorio (p. ej. `X-Role`) |
| 503 | Base de datos u origen no conectado; dependencia no disponible |
| 504 | La solicitud agotó el tiempo de espera |

Las violaciones de gobierno en `POST /data/sql` devuelven HTTP 403 con un cuerpo estructurado: (REQ-002) [tool-verified: `provisa/api/data/endpoint_dev.py:184-190`]

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

## Endpoint de Arrow Flight

Puerto `8815`. Transporte columnar Arrow nativo sobre gRPC. (REQ-143, REQ-045) [tool-verified: `provisa/api/flight/server.py`]

Las consultas y el descubrimiento de catálogo están disponibles en la misma conexión. El pipeline de gobierno completo (RLS, enmascaramiento, muestreo) se aplica a cada consulta. (REQ-130, REQ-143)

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

Cuando el proxy Zaychik Flight SQL está disponible (puerto 8480), los lotes de registros se transmiten de extremo a extremo sin materialización completa. (REQ-144) Si Zaychik no está disponible, recurre a la materialización a través de la capa de consulta federada. (REQ-146)

---

## Endpoint gRPC de Protobuf

Puerto `50051` (anúlelo con la variable de entorno `GRPC_PORT` o la configuración `server.grpc_port`). (REQ-529) [tool-verified: `provisa/grpc/server.py`, `provisa/api/app.py`]

Pase el rol en la clave de metadatos gRPC `x-provisa-role`. Si está ausente, el servidor aborta con `UNAUTHENTICATED`. [tool-verified: `provisa/grpc/server.py`]

Descargue el proto específico de un rol desde `GET /data/proto/{role_id}`. Solo aparecen las tablas y columnas visibles para ese rol. (REQ-039)

```proto
service ProvisaService {
  rpc QueryOrders (QueryOrdersRequest) returns (stream Orders);
  rpc InsertOrders (InsertOrdersRequest) returns (InsertOrdersResponse);
}
```

Cada tabla produce un RPC de streaming `Query{TypeName}`. Los RPC `Insert{TypeName}` existen por simetría de esquema pero abortan con `UNIMPLEMENTED`. [tool-verified: `provisa/grpc/server.py`]

`grpc_reflection.v1alpha` está habilitado para el descubrimiento de servicios sin un proto precompilado. (REQ-529) [tool-verified: `provisa/grpc/reflection.py`]

```bash
grpcurl -plaintext localhost:50051 list
grpcurl -plaintext -H 'x-provisa-role: analyst' \
  -d '{}' localhost:50051 ProvisaService/QueryOrders
```

El servidor gRPC solo se inicia cuando se puede compilar un proto válido en el arranque. Si la construcción del esquema falla, el servidor gRPC no se inicia. (REQ-529)

---

## Controlador JDBC

El controlador JDBC de Provisa (`provisa-jdbc-0.1.0.jar`) expone el catálogo semántico a herramientas de BI (Tableau, PowerBI, DBeaver). (REQ-126)

**URL de conexión:** `jdbc:provisa://host:port` (REQ-131)

Los dominios se asignan a esquemas JDBC. (REQ-127) Las tablas usan sus alias registrados. Las columnas usan alias y muestran las descripciones como `REMARKS`. (REQ-128) Los métodos de metadatos estándar (`getPrimaryKeys`, `getImportedKeys`, `getExportedKeys`) exponen las relaciones semánticas como metadatos de clave primaria/clave foránea.

**Soporte SQL:** `SELECT * FROM <alias> [WHERE col = 'value']`. (REQ-129)

El controlador solicita redirección Arrow IPC de forma predeterminada. Los resultados se transmiten lote por lote mediante `ArrowStreamReader`, acotados a un lote de registros en memoria. (REQ-293)

---

## Formato del argumento `orderBy`

El argumento `order_by` usa objetos `{column: direction}` con una enumeración de dirección de 6 valores: (REQ-200)

```json
{
  "query": "{ orders(order_by: [{created_at: desc_nulls_last}]) { id created_at } }",
  "role": "admin"
}
```

Direcciones admitidas: `asc`, `desc`, `asc_nulls_first`, `asc_nulls_last`, `desc_nulls_first`, `desc_nulls_last`. (REQ-201)

---

## Suscripciones

Las suscripciones SSE están disponibles en `GET /data/subscribe/{table}`. (REQ-219, REQ-258) La entrega de notificaciones usa un proveedor conectable seleccionado según el tipo de origen: los orígenes PostgreSQL usan `LISTEN/NOTIFY`, los orígenes MongoDB usan Change Streams, y los orígenes Kafka usan grupos de consumidores. El filtrado RLS y la validación de esquema se aplican independientemente del proveedor. También se admiten orígenes WebSocket y RSS a través del mismo endpoint. (REQ-338, REQ-342) [tool-verified: `provisa/api/data/subscribe.py:239`, `provisa/subscriptions/registry.py`, `provisa/api/app.py` `_rebuild_schemas`]
</content>
