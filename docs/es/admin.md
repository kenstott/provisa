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

Asignaciones de modelos de IA, el registro de modelos de embeddings/vectores, y el límite de tasa de NL — surten efecto en la siguiente solicitud, sin necesidad de reiniciar (REQ-1349): [tool-verified: `provisa/api/admin/ai_models_router.py:38-39`]

```http
GET  /admin/ai-models
PUT  /admin/ai-models
```

La pestaña de cifrado del admin deriva su lista de proveedores en vivo desde el registro de cifrado; los proveedores no disponibles aparecen pero no son seleccionables (REQ-1091).

`GET`/`HEAD /health` y `GET /setup/status` siempre están sin autenticar — eluden el requisito de `Authorization: Bearer` incluso cuando hay un proveedor de autenticación configurado (REQ-539).

### Motor de federación

Lea o cambie qué motor usa el despliegue (REQ-916):

```http
GET  /admin/federation-engine
PUT  /admin/federation-engine
```

`GET` devuelve la clave del motor activo y los campos de configuración que necesita. `PUT` acepta un cuerpo con `engine` (la clave) y cualquier campo específico del motor; la selección se persiste en la configuración de la plataforma y se vincula en el siguiente reinicio del servicio. [tool-verified: `provisa/api/admin/settings_router.py:730-829`]

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

Declare una relación respaldada por una tabla de unión (REQ-1586):

```graphql
mutation {
  upsertRelationship(input: {
    id: "pets-bonded-pair"
    sourceTableId: "pets"
    targetTableId: "pets"
    sourceColumn: "id"
    targetColumn: "id"
    cardinality: "one-to-many"
    viaTable: "pet_companions"
    viaSourceColumn: "pet_id"
    viaTargetColumn: "companion_pet_id"
    viaTypeColumn: "companion_type"
    viaTypeValue: "bonded pair"
    viaLabelSource: "column"
  }) {
    success
  }
}
```

Una tabla asociativa se declara como arista, nunca se descubre. `viaTable` nombra una tabla registrada; sus dos columnas clave llevan la arista, y cada columna restante pasa a ser un atributo de la relación, filtrable como cualquier otro campo. `viaTypeColumn` / `viaTypeValue` dividen una misma tabla de unión en varios tipos de arista — tres filas de `pet_companions` con `companion_type` igual a `bonded pair`, `littermate` y `shares enclosure` son tres relaciones distintas sobre el mismo par de tablas.

`viaLabelSource` designa de dónde procede el nombre expuesto, y las tres formas se pasan a UPPER_SNAKE_CASE para Cypher: `column` usa `viaTypeValue` (`BONDED_PAIR`), `table` usa el nombre propio de la tabla de unión (`PET_COMPANIONS`), `fixed` usa el `alias` declarado. Una tabla de unión declarada así es una arista y no una entidad — desaparece de las etiquetas de nodo, por lo que nunca aparece como píldora de nodo en la interfaz del grafo. [tool-verified: `provisa/api/admin/types.py:606-611`, `provisa/api/admin/db_queries.py:47-82`]

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

### Importación de Hasura / DDN (REQ-1483)

Convierta un proyecto existente de Hasura v2 o de Hasura DDN en configuración de Provisa desde la interfaz de administración o la API, sin que nada aterrice hasta que usted lo apruebe.

```http
POST /admin/import/hasura/preview
POST /admin/import/hasura/apply
```

**La vista previa** convierte el archivo cargado y devuelve el `config_yaml` propuesto, una lista de advertencias y un resumen de lo encontrado (recuentos de orígenes, dominios, tablas, columnas, roles, relaciones y RLS). No se escribe nada en la base de datos del inquilino. Cuerpo de la solicitud:

```json
{
  "filename": "my-hasura-project.zip",
  "content_b64": "<base64-encoded archive>",
  "flavor": "auto",
  "domain_map": {"public": "sales"},
  "source_overrides": {}
}
```

`flavor` es `"auto"` (detectado a partir de la estructura del archivo), `"hasura_v2"` o `"ddn"`.

**La aplicación** toma el YAML que usted revisó (y editó, si procede) y lo carga en la organización que actúa, por la misma ruta de recarga en caliente que `PUT /admin/config`. Cuerpo de la solicitud: `{"config_yaml": "<yaml string>"}`.

La vista previa nunca almacena en caché el YAML convertido en el servidor; la aplicación toma el YAML que usted proporciona, así que lo que se aplica es exactamente lo que se revisó. [tool-verified: `provisa/api/admin/import_router.py`]

### Intercambio con Apache Ossie (REQ-1316, REQ-1321)

Provisa interopera con Apache Ossie (en incubación) como frontera de importación y exportación.

```http
GET  /admin/ossie
POST /admin/ossie/import
```

**La exportación** (`GET /admin/ossie`) deriva el documento YAML de Ossie del modelo gobernado en vivo en cada solicitud: nunca se almacena en caché, así que no puede quedar obsoleto. La respuesta es `text/yaml` con una cabecera `Content-Disposition: attachment`. Las tablas se convierten en objetos `dataset`, las columnas en objetos `field`, y las relaciones se asignan a objetos `relationship` de Ossie. (REQ-1321) [tool-verified: `provisa/api/admin/ossie_router.py:download_ossie`]

**La importación** (`POST /admin/ossie/import`) acepta un documento YAML o JSON de Ossie (el formato se detecta automáticamente). Analiza el documento y devuelve los registros propuestos de tablas y relaciones como un objeto JSON; no se registra nada. La pantalla de revisión de la interfaz de administración le permite aceptar o recortar las propuestas antes de que se dispare ninguna mutación. (REQ-1316) [tool-verified: `provisa/api/admin/ossie_router.py:import_ossie`]

### Almacenamiento de objetos (REQ-1046, REQ-1048, REQ-1049)

Lea o configure el almacenamiento de materialización de la organización:

```http
GET  /admin/org-storage
PUT  /admin/org-storage
```

`GET` informa de cuánta asignación de almacenamiento de la plataforma usa la organización. `PUT` registra el DSN de almacenamiento propio de la organización (cifrado en reposo; nunca lo devuelve GET). Una vez establecido, las materializaciones de la organización aterrizan en su propio bucket y dejan de contar contra la asignación de la plataforma. Enviar `storage_url: null` lo borra y devuelve la organización al almacén de la plataforma. [tool-verified: `provisa/api/admin/org_storage_router.py`]

### Cifrado de la organización (REQ-1574)

Establezca o rote la clave de cifrado en reposo de la organización:

```http
GET  /admin/org-encryption
PUT  /admin/org-encryption
```

`GET` devuelve la huella, el id y la procedencia de la clave; nunca el material de la clave. `PUT` establece o rota la clave. Proporcione `key_b64` (32 bytes en bruto, codificados en base64) para aportar su propia clave, u omítalo para que Provisa genere una. No hay eliminación: retirar la última clave dejaría ilegible toda carga que hubiera envuelto. [tool-verified: `provisa/api/admin/org_encryption_router.py`]

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
