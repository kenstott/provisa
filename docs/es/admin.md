# Admin API

La Admin API es un endpoint de Strawberry GraphQL en `POST /admin/graphql` (REQ-533). Requiere un rol de superusuario o admin (REQ-125, REQ-060) y es independiente del endpoint de GraphQL de datos (REQ-533).

## Autenticación

Pase sus credenciales en el encabezado `Authorization` usando el proveedor de autenticación estándar de Provisa (REQ-120):

```yaml
Authorization: Bearer <token>
```

El acceso admin se rige por la capacidad `admin` asignada a un rol (REQ-060, REQ-042).

### Tokens de acceso personal

Un token de acceso personal se acepta en todos los sitios donde se acepta un token bearer, incluido este endpoint. Emitirlo y revocarlo es autoservicio: es la credencial propia de quien lo posee, así que vive en el perfil del usuario dentro de la interfaz de administración y no bajo una página de administrador, junto a abandonar una organización y eliminar la cuenta. Un administrador no acuña tokens en nombre de otra persona. (REQ-1263)

| Ruta | Efecto |
| ------- | -------- |
| `POST /auth/tokens` | Acuña un token para quien llama. Cuerpo: `name` y, opcionalmente, `role_id`, `scopes`, `expires_in_days` (1–366). La respuesta es el único lugar donde el secreto aparece alguna vez |
| `GET /auth/tokens` | Los tokens activos de quien llama en esta organización: prefijo de visualización, nombre, marcas de tiempo del ciclo de vida y el hash que identifica un token para revocarlo. Nunca una credencial utilizable |
| `DELETE /auth/tokens/{token_hash}` | Revoca uno de los tokens de quien llama. 404 cuando no es suyo o ya está revocado |

Omitir `role_id` deja que el token se resuelva al rol que tenga su propietario; nombrar uno estrecha el token por debajo de su propietario. La revocación también ocurre de forma implícita: retirar la pertenencia de un usuario a una organización revoca sus tokens para esa organización. Para la credencial en sí, consulte [Modelo de seguridad](security.md#tokens-de-acceso-personal).

## Capacidades

### Gestión de Configuración

Descargue la configuración en ejecución actual (REQ-164):

```http
GET /admin/config
```

Devuelve el `config.yaml` completo como archivo YAML. Suba una nueva configuración (REQ-164):

```http
PUT /admin/config
```

Provisa valida el YAML, recarga los catálogos y regenera los esquemas (REQ-012, REQ-253). No requiere reinicio.

### Configuración en Tiempo de Ejecución

Lea y escriba la configuración de la plataforma en tiempo de ejecución sin editar el archivo de configuración (REQ-165):

```http
GET  /admin/settings
PUT  /admin/settings
```

La superficie de configuración abarca la redirección de resultados grandes, el muestreo predeterminado y el límite de filas, el TTL de la caché de respuestas, la convención de nomenclatura, el auto-rastreo de FK de relaciones, el DSN del almacén de materialización, la memoria del motor de federación (`jvm_heap_gb`, `query_max_memory`, `query_max_memory_per_node`, `query_max_total_memory`, `fault_tolerant_execution`, `fault_tolerant_task_memory`, `exchange_spool_dir`) y toda la superficie de ajuste del pipeline de trazas de OpenTelemetry (REQ-1082). Los límites de recorrido de GraphQL remoto y la configuración de nivel cálido/caché de lectura también se exponen (REQ-1081, REQ-1083).

Postura de seguridad — `security.mode` (`standard` | `high`) — aplicada al reiniciar (REQ-1079):

```http
GET  /admin/security
PUT  /admin/security
```

Asignaciones de modelos de IA, el registro de modelos de embeddings/vectores, y el límite de tasa de NL — aplicados al reiniciar (REQ-1080):

```http
GET  /admin/ai-models
PUT  /admin/ai-models
```

La pestaña de cifrado del admin deriva su lista de proveedores en vivo desde el registro de cifrado; los proveedores no disponibles aparecen pero no son seleccionables (REQ-1091).

`GET`/`HEAD /health` y `GET /setup/status` siempre están sin autenticar — eluden el requisito de `Authorization: Bearer` incluso cuando hay un proveedor de autenticación configurado (REQ-539).

### Editor de Relaciones

Liste las relaciones (REQ-166):

```graphql
query {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceColumn
    targetColumn
    cardinality
    materialize
  }
}
```

Cree una relación (REQ-019):

```graphql
mutation {
  upsertRelationship(input: {
    id: "orders-to-customers"
    sourceTableId: "orders"
    targetTableId: "customers"
    sourceColumn: "customer_id"
    targetColumn: "id"
    cardinality: "many_to_one"
  }) {
    success
  }
}
```

### Descubrimiento de Relaciones con IA

Active el análisis de FK impulsado por Claude vía REST (REQ-167, REQ-018):

```bash
curl -X POST http://localhost:8001/admin/discover/relationships \
  -H "Content-Type: application/json" \
  -d '{"scope": "domain", "domain_id": "sales"}'
```

Devuelve candidatos de FK clasificados por confianza. Acepte un candidato:

```bash
curl -X POST http://localhost:8001/admin/discover/candidates/{id}/accept \
  -H "Content-Type: application/json" \
  -d '{"name": "orders_to_customers"}'
```

### Introspección de Esquemas

Explore las tablas publicadas en todos los orígenes (REQ-008):

```graphql
query {
  tables {
    id
    sourceId
    columns {
      columnName
      unmaskedTo
      writableBy
    }
  }
}
```

### Verificación de dependencias de columnas (REQ-1484)

Antes de guardar una edición de tabla que renombra el alias SQL de una columna o elimina una
columna, pregunte qué más la referencia:

```graphql
query {
  columnDependents(tableId: "42", renamed: ["order_total"], removed: ["legacy_code"]) {
    columnName
    dependents { kind name detail breaksOn }
  }
}
```

Renombrar un alias rompe todo artefacto creado a partir del nombre expuesto: vistas, vistas
materializadas, expresiones de métricas, predicados de RLS, contratos de DQ. Eliminar una columna
rompe esos más los artefactos que almacenan el `column_name` físico: relaciones, vínculos de
glosario, asignaciones de etiquetas. `breaksOn` indica cuáles. La página Tablas ejecuta esta
verificación al guardar y muestra el resultado como un diálogo consultivo. Consulte
[Linaje](lineage.md) para saber qué cubre la consulta y qué no puede cubrir.

### Gestión de Vistas

Registre una vista materializada (REQ-133, REQ-135):

```graphql
mutation {
  registerTable(input: {
    viewSql: "SELECT o.id, o.amount, c.name FROM orders o JOIN customers c ON o.customer_id = c.id"
    mvRefreshInterval: 300
    materialize: true
  }) {
    success
  }
}
```

Active una actualización manual (REQ-135):

```graphql
mutation {
  refreshMv(mvId: "orders-with-customers") {
    success
  }
}
```

### Registro de Orígenes de Grafo

Los orígenes Neo4j y SPARQL se registran mediante endpoints REST (no la Admin API de GraphQL) (REQ-295, REQ-297):

**Neo4j:**

```bash
# 1. Register the Neo4j source
curl -X POST http://localhost:8001/admin/sources/neo4j \
  -H "Content-Type: application/json" \
  -d '{"source_id": "graph", "host": "neo4j", "port": 7474, "database": "neo4j"}'

# 2. Preview a Cypher query (validates scalar projections)
curl -X POST http://localhost:8001/admin/sources/neo4j/graph/preview \
  -H "Content-Type: application/json" \
  -d '{"cypher": "MATCH (p:Person) RETURN p.name AS name, p.age AS age"}'

# 3. Register a table (runs preview+validate automatically)
curl -X POST http://localhost:8001/admin/sources/neo4j/graph/tables \
  -H "Content-Type: application/json" \
  -d '{"table_name": "people", "cypher": "MATCH (p:Person) RETURN p.name AS name, p.age AS age", "ttl": 300}'
```

**SPARQL:**

```bash
# 1. Register the SPARQL source
curl -X POST http://localhost:8001/admin/sources/sparql \
  -H "Content-Type: application/json" \
  -d '{"source_id": "kg", "endpoint_url": "http://fuseki:3030/ds/sparql"}'

# 2. Register a table (probes endpoint and infers columns)
curl -X POST http://localhost:8001/admin/sources/sparql/kg/tables \
  -H "Content-Type: application/json" \
  -d '{"table_name": "products", "sparql_query": "SELECT ?name ?category WHERE { ?p a :Product ; :name ?name ; :category ?category . }", "ttl": 600}'
```

Una vez registradas, las tablas aparecen en el esquema de GraphQL y son consultables como cualquier otro origen (REQ-016).

## GraphiQL

La Admin API incluye GraphiQL en `GET /admin/graphql` en el navegador (REQ-622). Úselo para explorar el esquema admin completo de forma interactiva.

## Vistas de gestión del dominio ops (REQ-1386)

En cada instalación se siembran ocho vistas SQL en el dominio integrado `ops`. [tool-verified: `provisa/api/startup_seed.py:225-331` `_seed_ops_domain`] Exponen el registro de auditoría de consultas como tablas gobernadas, consultables mediante SQL (pgwire), GraphQL y Cypher bajo las mismas reglas de acceso de dominio, RLS y enmascaramiento que cualquier tabla de negocio.

`org_admin` queda designado como responsable del dominio ops en el momento de la siembra, de modo que el dominio nunca aparece como una laguna de gobernanza en `stale_metadata`. [tool-verified: `startup_seed.py:326-331`]

| Vista | A qué responde |
| --- | --- |
| `usage_ranking` | Número de consultas y usuarios distintos por tabla registrada; las tablas sin ningún acceso emergen como candidatas a desaprobación |
| `deprecated_usage` | Cada acceso a una tabla o columna con la etiqueta `deprecated`: los consumidores activos que impiden una retirada segura |
| `pii_access` | Cada acceso a una tabla o columna con la etiqueta `pii`: quién consultó, bajo qué rol y por qué superficie |
| `policy_denials` | Todos los intentos de acceso que la gobernanza rechazó (HTTP 401/403) |
| `surface_mix` | Número diario de consultas y usuarios distintos por superficie de protocolo (SQL, GraphQL, Cypher, gRPC, etc.) |
| `query_health` | Número diario de errores y latencia media/máxima por superficie |
| `stale_metadata` | Tablas y columnas sin descripción; dominios sin responsable |
| `join_hotspots` | Pares de tablas consultadas juntas con más frecuencia: candidatas a materialización o caché |

Hoy se aplican dos límites. La granularidad es de tabla: el registro de auditoría anota `table_ids`, no las columnas concretas a las que se accede. El texto de la consulta está cifrado (REQ-689) y queda excluido de todas estas vistas; solo es accesible por la vía de descifrado administrativa autorizada. [tool-verified: `_meta_views.py:148-162` — comment notes `query_text_enc` exclusion]

Un rol necesita acceso al dominio `ops` para que estas vistas sean visibles. Concédalo igual que concede el acceso a cualquier otro dominio.

```sql
-- Which tables have never been queried?
SELECT table_name, domain_id
FROM ops.usage_ranking
WHERE query_count = 0;

-- Who accessed PII-tagged data in the last 7 days?
SELECT user_id, role_id, source, pii_column, logged_at
FROM ops.pii_access
WHERE logged_at >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY logged_at DESC;

-- Where does traffic originate by protocol?
SELECT source, day, query_count, distinct_users
FROM ops.surface_mix
ORDER BY day DESC, query_count DESC;
```

Las mismas consultas se ejecutan como GraphQL o Cypher sobre cualquier transporte gobernado: pgwire, Arrow Flight o Bolt. [inferred from governed-surface design]

## Visor de informes (REQ-1390)

El visor de informes está en `/admin/reports`. Los roles sin la capacidad `observability` no pueden llegar a él.

El panel izquierdo lista cada tabla registrada del dominio `ops`, ordenada por alias. [tool-verified: `ReportsTab.tsx:46-52` — filters `tables` to `domainId === "ops"`] Las ocho vistas de gestión sembradas aparecen ahí automáticamente. Haga clic en cualquier informe para cargarlo en el visor de datos gobernado de la derecha.

**Añadir un informe propio.** El botón «Añadir informe» abre un diálogo. Indique un nombre, una descripción opcional y una sentencia SELECT. Al guardar, la vista se registra como tabla derivada gobernada en el dominio `ops`: catalogada, con control de acceso y consultable por todas las superficies junto a las vistas sembradas. [tool-verified: `ReportsTab.tsx:70-96` — `registerTable` called with `sourceId: DERIVED_SOURCE_ID, domainId: "ops"`]

**Eliminar.** El icono de papelera solo aparece en los informes propios. Las vistas de gestión sembradas no se pueden eliminar desde esta interfaz. [tool-verified: `ReportsTab.tsx:151` — `const custom = report.sourceId === DERIVED_SOURCE_ID` gates the delete button]

## Vista previa de tabla (REQ-1392)

Despliegue cualquier fila de tabla en la página Tablas. El botón **Vista previa** abre un modal al 90 % de ancho con los datos gobernados en vivo de la tabla. [tool-verified: `TablePreviewModal.tsx:24` — `size="90%"`; `GovernedTableViewer.tsx` is the underlying viewer]

Las tablas respaldadas por APIs con parámetros de ruta obligatorios bloquean la vista previa hasta que se suministran esos valores. Un formulario en línea recoge cada parámetro obligatorio antes de que se ejecute la primera consulta; los parámetros de consulta opcionales aparecen en el mismo formulario. [tool-verified: `GovernedTableViewer.tsx:51-55, 153-155` — `requiredParamColumns` check; "paramsRequired" message shown when `activeParams == null`]

## Visor de datos gobernado (REQ-1391)

El mismo componente de visor impulsa el modal de vista previa y el visor de informes. Su comportamiento es idéntico en ambos contextos.

**Paginación en el servidor.** Cada página es su propio `SELECT *` gobernado con `LIMIT 101 OFFSET n`. Aparecen 100 filas por página; la número 101 indica si hay más. El conjunto de datos completo nunca se carga en el navegador. [tool-verified: `nativeParams.ts:72` — `LIMIT ${pageSize + 1} OFFSET ${page * pageSize}`; `types.ts:74` — `PAGE_SIZE = 100`]

**Filtros y ordenaciones empujados a la fuente.** Cada cabecera de columna tiene un campo de filtro. Los términos de filtro se convierten en predicados `WHERE LOWER(CAST(col AS VARCHAR)) LIKE LOWER('%term%')`; los clics de ordenación producen cláusulas `ORDER BY`. Ambos van a la base de datos: un filtro sobre una tabla de mil millones de filas recorre la fuente, no las 100 filas que tiene delante. [tool-verified: `nativeParams.ts:53-70`]

**Agrupación de varios niveles.** El icono de capas de cualquier cabecera de columna incorpora esa columna a la agrupación. Las columnas de agrupación encabezan el `ORDER BY`, de modo que los miembros de un grupo caen en la misma página que su cabecera al cruzar los límites de página. Las columnas de clave primaria se añaden al final como desempate estable. [tool-verified: `nativeParams.ts:61-70` — group columns first, then explicit sorts, then PKs] Las filas de cabecera de grupo se pueden contraer; contraerlas oculta los miembros sin lanzar una nueva consulta. [tool-verified: `useResultsGrid.ts:150-171` — `collapsedGroups` set gates the `build()` recursion]

**Preferencias persistentes.** Los ajustes de filtro, orden y agrupación se guardan en `localStorage` bajo `provisa.grid.table:<domain>.<table>` y se restauran en la siguiente visita. [tool-verified: `useResultsGrid.ts:95-98`, `GovernedTableViewer.tsx:66`]

**Exportación.** Descargue la página actual como CSV o cópiela al portapapeles como texto separado por tabuladores. La exportación abarca solo la página visible. [tool-verified: `useResultsGrid.ts:247-274` — both handlers iterate `displayRows`, which in server-paged mode is the current page]
