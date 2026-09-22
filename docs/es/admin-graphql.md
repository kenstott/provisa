# Referencia de la API GraphQL de administración

La API GraphQL de administración es el plano de configuración de Provisa. Es la API que la aplicación web de administración llama para toda operación de gestión — crear orígenes, registrar tablas, definir relaciones, configurar reglas de RLS, y todo lo demás que da forma al modelo.

**Punto de montaje:** `POST /admin/graphql`

Esta no es la misma API que el plano de datos en `/data/graphql`. El plano de datos sirve las consultas de los usuarios finales sobre los dominios registrados y se describe mediante el SDL en `/data/sdl`. La API de administración configura cómo se ve ese esquema y quién puede ver qué.

---

## Cómo habla la UI con esta API

La aplicación web de administración usa Apollo Client, apuntando a `${API_BASE}/admin/graphql`. [tool-verified: `provisa-ui/src/apolloClient.ts:19`]

Cada solicitud lleva un token bearer (obtenido de nuevo del proveedor de autenticación en cada llamada), un encabezado `X-Org-Id` cuando es multiinquilino, y un encabezado `X-Env` cuando sirve un entorno de rama. [tool-verified: `provisa-ui/src/apolloClient.ts:24-42`]

El esquema se ensambla a partir de dos clases `@strawberry.type` — `Query` de `schema_query.py` y `Mutation` de `schema_mutation.py` — y se envuelve en una `ModelCommitExtension` que registra cada mutación contra la rama de entorno actual (REQ-1524). [tool-verified: `provisa/api/admin/schema.py:44`]

---

## Autorización

**Modo de desarrollo:** cuando no hay autenticación configurada y toda solicitud llega como un principal anónimo, se omiten todas las verificaciones de capacidad. Esto mantiene una instalación local funcional sin configurar autenticación. [tool-verified: `provisa/api/admin/capabilities.py:98-99`]

**Compuertas de capacidad:** los despliegues de producción aplican capacidades nombradas. El derecho específico requerido por cada campo se indica en línea. Llamar a una mutación sin la capacidad requerida lanza un `PermissionError`. El rol de administrador de la plataforma omite todas las verificaciones de capacidad (REQ-1297). [tool-verified: `provisa/api/admin/capabilities.py:80-110`]

**Compuertas de dominio:** varias mutaciones también verifican el dominio al que pertenece el objeto. Un llamador con alcance en `sales` no puede registrar una tabla en `finance`, encolar una regla de RLS para ella, ni crear una relación cuya tabla de origen viva en un dominio que no posee (REQ-1530, REQ-1531). Las vistas están además restringidas: cada tabla que el SQL de la vista lee debe estar dentro de los dominios del llamador, porque de lo contrario el SQL de mano libre le daría a un miembro acceso a datos fuera de su alcance. [tool-verified: `provisa/api/admin/domain_guard.py:1-133`]

**Herencia de roles:** las capacidades de un rol padre son heredadas por los roles hijos (REQ-1677). `createRole` y `deleteRole` rechazan ciclos e impiden eliminar un rol que tenga herederos.

---

## Tipo de retorno común

La mayoría de las mutaciones devuelven `MutationResult`. [tool-verified: `provisa/api/admin/types.py:1181-1188`]

```graphql
type MutationResult {
  success: Boolean!
  message: String!
  code: String          # stable i18n key, e.g. "schema.source_created"
  params: JSON          # key/value pairs for client-side localization (REQ-1350)
}
```

Cuando una mutación falla, `success` es `false` y `message` lleva la razón en inglés. `code` es un identificador estable que la UI usa para renderizar un mensaje localizado.

---

## Consultas

### Orígenes

#### `sources → [SourceType!]!`

Todos los orígenes de datos registrados. [tool-verified: `provisa/api/admin/schema_query.py:327-331`]

```graphql
query {
  sources {
    id type host port database username dialect
    cacheEnabled cacheTtl preferMaterialized
    loadProtected offPeakWindow offPeakTz
    gqlNamingConvention path allowedDomains
    description mappingJson federationHintsJson
    changeSignal passwordRef
    cdc { bootstrapServers topicPrefix schemaRegistryUrl consumerGroupId }
  }
}
```

`passwordRef` es una referencia `${secret:NAME}` a la bóveda de la organización — nunca la credencial literal. [tool-verified: `provisa/api/admin/types.py:105`]

#### `source(id: String!) → SourceType`

Un solo origen por ID. Devuelve `null` cuando no se encuentra. [tool-verified: `provisa/api/admin/schema_query.py:334-339`]

#### `availableSchemas(sourceId: String!) → [String!]!`

Esquemas visibles en un origen, filtrados para excluir los internos de Provisa. Usa primero la introspección nativa; recae en el catálogo del motor cuando el tipo de origen no tiene pool directo. [tool-verified: `provisa/api/admin/schema_query.py:628-667`]

#### `availableTables(sourceId: String!, schemaName: String = "public") → [AvailableTableType!]!`

Tablas de un esquema de un origen, con sus comentarios. Para orígenes OpenAPI, devuelve operaciones GET cuya respuesta es un arreglo o un envoltorio de paginación. Para orígenes GraphQL, devuelve campos de consulta que devuelven una lista. Para gRPC, devuelve RPC de streaming del servidor. [tool-verified: `provisa/api/admin/schema_query.py:669-731`]

#### `availableColumns(sourceId: String!, schemaName: String!, tableName: String!) → [String!]!`

Nombres de columna de una tabla en el catálogo del motor. Para orígenes govdata, usa un resolutor aparte. [tool-verified: `provisa/api/admin/schema_query.py:845-866`]

#### `availableColumnsMetadata(sourceId: String!, schemaName: String!, tableName: String!) → [AvailableColumnType!]!`

Nombres de columna con tipos de datos, comentarios, tipos de filtro nativos e indicadores de clave primaria. Para orígenes OpenAPI, deriva la forma a partir del esquema de respuesta y los parámetros de la operación. [tool-verified: `provisa/api/admin/schema_query.py:869-876`]

#### `availableFunctions(sourceId: String!, schemaName: String = "openapi") → [AvailableTableType!]!`

Operaciones que no son GET para un origen OpenAPI (POST, PUT, PATCH, DELETE). Devuelve una lista vacía para orígenes que no son OpenAPI. [tool-verified: `provisa/api/admin/schema_query.py:822-843`]

#### `crawlSource(path, depth, pattern, recursive, simpleLinks, sameDomain, excludePattern) → CrawlResultType`

Vista previa de lo que descubriría un rastreo del conector de archivos — archivos, tablas y columnas — antes de crear un origen. Los ajustes exclusivos de HTTP (`simpleLinks`, `sameDomain`, `excludePattern`) se ignoran para raíces locales, S3, FTP y SFTP. (REQ-1785) [tool-verified: `provisa/api/admin/schema_query.py:733-790`]

#### `suggestTableAlias(tableName: String!, domainId: String!, sourceId: String!) → String!`

Devuelve el alias a usar al registrar `tableName` en `domainId` desde `sourceId`. Devuelve un alias simple en snake-case cuando no hay conflicto, o un alias prefijado con el origen (`sqlite_b_orders`) cuando el nombre efectivo ya está tomado por un origen distinto en el mismo dominio. [tool-verified: `provisa/api/admin/schema_query.py:879-922`]

---

### Tablas

#### `tables → [RegisteredTableType!]!`

Todas las tablas registradas, cada una con su lista completa de columnas. La visibilidad de columnas en la respuesta respeta la capacidad `table_registration` del llamador — `canDeployToDb` depende de si el llamador posee ese derecho. (REQ-016, REQ-021, REQ-042) [tool-verified: `provisa/api/admin/schema_query.py:502-536`]

Cada `RegisteredTableType` expone subcampos calculados:

- **`refreshPolicySummary → RefreshPolicySummaryType`** — la política efectiva de actualización/servicio como texto plano, derivada del lado del servidor a partir de la misma resolución del planificador que usa el motor. Devuelve `null` durante el arranque. (REQ-1143) [tool-verified: `provisa/api/admin/types.py:319-327`]
- **`graphqlFieldName → String`** — el nombre de campo que esta tabla tiene en el esquema compilado del plano de datos, para que el panel de Data Product pueda construir un ejemplo ejecutable sin replicar el algoritmo de nomenclatura. (REQ-1634) [tool-verified: `provisa/api/admin/types.py:330-339`]
- **`dqDataset → String`** — esta tabla como un conjunto de datos de contrato de calidad de datos, en la forma que el verificador escanea. (REQ-1443) [tool-verified: `provisa/api/admin/types.py:374-387`]
- **`productId → String`** — el producto de datos al que pertenece esta tabla. Una tabla de verificador de DQ hereda el producto de la tabla cuyo contrato escanea. (REQ-1634) [tool-verified: `provisa/api/admin/types.py:342-372`]

#### `refreshPolicyPreview(...) → RefreshPolicySummaryType`

Vista previa del resumen efectivo de actualización/servicio para valores de tabla *borrador* (sin guardar), de modo que el resumen en la parte superior del formulario se actualice al cambiar los campos sin persistir nada. Misma derivación que `refreshPolicySummary` arriba. (REQ-1143) [tool-verified: `provisa/api/admin/schema_query.py:947-979`]

Argumentos: `sourceId`, `domainId`, `schemaName`, `tableName`, `cacheTtl`, `preferMaterialized`, `loadProtected`, `offPeakWindow`, `offPeakTz`, `changeSignal`.

#### `columnDependents(tableId: String!, renamed: [String!], removed: [String!]) → [ColumnDependentsType!]!`

Artefactos que un cambio de alias o una eliminación de columna pendientes romperían. Es informativo — la UI de administración lo muestra antes de guardar y el administrador decide. Debe llamarse *antes* de guardar, porque los dependientes fueron escritos contra el nombre expuesto que la columna lleva actualmente. (REQ-1484) [tool-verified: `provisa/api/admin/schema_query.py:1301-1331`]

---

### Relaciones

#### `relationships → [RelationshipType!]!`

Todas las relaciones definidas por el usuario (excluye las entradas `gql_auto__` autogeneradas y las entradas sintéticas `meta:%` usadas por el ERD). [tool-verified: `provisa/api/admin/schema_query.py:539-569`]

#### `allRelationships → [RelationshipType!]!`

Igual que `relationships`, pero incluye las entradas sintéticas `meta:%`. Usado por el ERD de grafo, que necesita mostrar cada arista, incluyendo los enlaces implícitos `HAS_TABLE` entre las tablas de datos y el registro de metadatos. [tool-verified: `provisa/api/admin/schema_query.py:572-601`]

Cada `RelationshipType` expone:

- **`autoSuggested → Boolean`** — si la relación fue sugerida por el análisis de claves foráneas (el `id` comienza con `fk__`). [tool-verified: `provisa/api/admin/types.py:523-525`]
- **`physicalName → String`** — el nombre de la relación en los planos de SQL y gRPC (el parámetro `?include=`). Derivado del lado del servidor; los clientes no deben transliterar el alias de GraphQL. (REQ-471, REQ-1417) [tool-verified: `provisa/api/admin/types.py:527-536`]

---

### Dominios, roles y usuarios

#### `domains → [DomainType!]!`

Todos los dominios en la base de datos del tenant de la organización activa. La base de datos del tenant está aislada a nivel de esquema, de modo que la lista de dominios de un org-admin contiene solo las filas de su organización. (REQ-021, REQ-042, REQ-1293) [tool-verified: `provisa/api/admin/schema_query.py:342-357`]

#### `roles → [RoleType!]!`

Roles visibles para el llamador. Un administrador ve cada rol; un no administrador ve solo los roles sin `org_id` o los roles que pertenecen a su organización. (REQ-042, REQ-059, REQ-060, REQ-215) [tool-verified: `provisa/api/admin/schema_query.py:603-618`]

#### `resolveOwners(refs: [String!]!) → [UserSummaryType!]!`

Resuelve IDs de rol o de usuario a usuarios individuales. Se usa para expandir `DataProduct.ownerRole`, `Domain.steward` y `Column.visibleTo` en una lista legible por humanos. Las referencias desconocidas se devuelven tal cual, para que la UI muestre el ID en bruto en lugar de nada. [tool-verified: `provisa/api/admin/schema_query.py:388-444`]

---

### Reglas de RLS

#### `rlsRules → [RLSRuleType!]!`

Todas las reglas de seguridad de nivel de fila. El repositorio subyacente descifra `filterExpr` en el límite. (REQ-041, REQ-402, REQ-686) [tool-verified: `provisa/api/admin/schema_query.py:621-625`]

---

### Productos de datos

#### `dataProducts → [DataProductType!]!`

Todos los productos de datos. Requiere la capacidad `data_product_read`. (REQ-1634) [tool-verified: `provisa/api/admin/schema_query.py:360-386`]

---

### Etiquetas

#### `tags → [TagType!]!`

Todas las definiciones de etiquetas, incluyendo sus valores de parámetro permitidos por etiqueta. (REQ-1373, REQ-1467) [tool-verified: `provisa/api/admin/schema_query.py:447-475`]

#### `tagAssignments → [TagAssignmentType!]!`

Todas las asignaciones de etiquetas entre orígenes, tablas, columnas y relaciones. (REQ-1377) [tool-verified: `provisa/api/admin/schema_query.py:478-499`]

---

### Vistas materializadas

#### `mvList → [MVType!]!`

Todas las vistas materializadas con su estado en tiempo de ejecución: habilitada/deshabilitada, marca de tiempo de la última actualización, conteo de filas y último error. [tool-verified: `provisa/api/admin/schema_query.py:927-944`]

---

### Caché

#### `cacheStats → CacheStatsType`

Estadísticas de caché. Devuelve `storeType: "redis"` con métricas operativas completas cuando Redis está configurado, `storeType: "memory"` para el almacén fakeredis embebido, y `storeType: "noop"` cuando no hay caché configurada. [tool-verified: `provisa/api/admin/schema_query.py:1106-1140`]

#### `cacheTableStats → [CacheTableStatType!]!`

Conteos de entradas cacheadas por tabla. Vacío cuando no hay un almacén de caché configurado. [tool-verified: `provisa/api/admin/schema_query.py:1143-1148`]

#### `hotTables → [HotTableStatType!]!`

Tablas de las que Provisa mantiene una copia, en dos niveles: `hot` (reflejada en el almacén de respuesta para el inlining de JOIN) y `warm` (aterrizada como una copia Iceberg). Una tabla está en como máximo un nivel (REQ-241). [tool-verified: `provisa/api/admin/schema_query.py:1151-1175`]

#### `materializeStoreInfo → MaterializeStoreInfoType`

Identidad del almacén de materialización durable: nombre del motor, referencia de DSN del almacén, conteo de MV, y si el almacén es local a la instancia (un archivo local como DuckDB o SQLite, lo que significa que cada instancia detrás de un balanceador de carga mantiene su propia copia). [tool-verified: `provisa/api/admin/schema_query.py:1178-1189`]

---

### Salud del sistema

#### `systemHealth → SystemHealthType`

Estado de conexión del motor, conteos del pool de workers, estado del pool de la BD de metadatos, modo de caché, y disponibilidad de cada listener de protocolo (pgwire, gRPC, Arrow Flight, Bolt). [tool-verified: `provisa/api/admin/schema_query.py:1194-1198`]

```graphql
query {
  systemHealth {
    engineConnected engineWorkerCount engineActiveWorkers
    metadataPoolSize metadataPoolFree metadataDialect
    cacheMode cacheConnected mvRefreshLoopRunning
    protocols { name status port }
  }
}
```

---

### Tareas programadas

#### `scheduledTasks → [ScheduledTaskType!]!`

Disparadores programados desde la configuración con estado en tiempo de ejecución. Cada entrada lleva su expresión cron, `kind` (`webhook` o `sql`), si está actualmente habilitada, la marca de tiempo de la última ejecución (siempre `null` en esta versión — rastreada por el planificador), y la próxima hora de ejecución programada desde APScheduler. [tool-verified: `provisa/api/admin/schema_query.py:1203-1245`]

---

### Calidad de datos

#### `dqContractParse(checker: String!, contractText: String!) → DqContractType`

Analiza el texto de un contrato en bruto en las filas editables del panel constructor. Se llama en cada edición; un fallo de análisis regresa como `error` en lugar de como un error de GraphQL, porque el texto a medio escribir es normal mientras el operador está escribiendo. (REQ-1443 cláusula 7) [tool-verified: `provisa/api/admin/schema_query.py:1007-1022`]

#### `dqCheckCatalog(checker: String!, dataset: String!) → DqCheckCatalogType`

Las verificaciones que ofrece `checker`, con alcance a las columnas de `dataset`. El dataset es el objetivo observado del contrato, resuelto de la misma manera que lo resuelve el escáner — de modo que las verificaciones ofrecidas coincidan con las columnas que el verificador realmente verá. (REQ-1443 cláusula 7) [tool-verified: `provisa/api/admin/schema_query.py:1025-1050`]

#### `dqCheckDefinition(checker: String!, check: DqCheckBuildInput!) → DqCheckDefinitionType`

El texto de una verificación desde los editores del panel. Se hace del lado del servidor porque el dialecto tiene una única implementación; una verificación construida en el panel y una escrita a mano deben ser indistinguibles. (REQ-1443 cláusula 7) [tool-verified: `provisa/api/admin/schema_query.py:1053-1075`]

#### `dqContractBuild(checker: String!, dataset: String!, checks: [DqCheckInput!]!) → DqContractTextType`

Serializa las filas de verificación editadas de vuelta a texto de contrato. La inversa de `dqContractParse`. Del lado del servidor por la misma razón: el panel no puede emitir texto que el verificador rechazaría. (REQ-1443 cláusula 7) [tool-verified: `provisa/api/admin/schema_query.py:1078-1101`]

---

### Vistas previas de origen

#### `neo4jPreview(sourceId: String!, cypher: String!) → QueryPreviewType`

Vista previa de una proyección Cypher en un origen Neo4j: hasta cinco filas y los tipos de columna que el registro llevará. Los fallos regresan como `error`. (REQ-1670) [tool-verified: `provisa/api/admin/schema_query.py:984-992`]

#### `sparqlPreview(sourceId: String!, query: String!) → QueryPreviewType`

Vista previa de un SELECT SPARQL en un origen SPARQL: hasta cinco filas, todas las columnas como texto. (REQ-1683) [tool-verified: `provisa/api/admin/schema_query.py:995-1002`]

---

### Kaggle

#### `kaggleTokenValid(token: String!) → Boolean!`

Verificación en vivo contra la API de Kaggle. Devuelve `true` solo cuando el token se autentica. Respalda el paso de compuerta de token en el formulario de origen de Kaggle. (REQ-1783) [tool-verified: `provisa/api/admin/schema_query.py:793-799`]

#### `kaggleDatasets(token: String!, query: String = "", page: Int = 1) → [KaggleDatasetType!]!`

Busca en el catálogo público completo de conjuntos de datos de Kaggle. Sujeto a token. (REQ-1783) [tool-verified: `provisa/api/admin/schema_query.py:802-819`]

---

### Calendarios

#### `calendars → [CalendarType!]!`

Todas las versiones de calendario de límites de instantánea registradas. Alimenta el selector de configuración de programación de instantáneas y confirma qué calendarios puede referenciar una MV periódica. (REQ-962) [tool-verified: `provisa/api/admin/schema_query.py:218-242`]

---

### Métricas

#### `metrics → [MetricType!]!`

Todas las definiciones de métricas gobernadas. Las métricas derivadas de un fact llevan `fromFact`. (REQ-1317, REQ-1320) [tool-verified: `provisa/api/admin/schema_query.py:245-264`]

---

### Versión del esquema

#### `schemaVersion → String!`

Hash SHA-256 del estado actual del esquema (dominios, IDs de tabla, IDs de relación). El cliente Apollo lee esto del encabezado de respuesta `X-Schema-Version` y vuelve a obtener todas las consultas activas cuando avanza. [tool-verified: `provisa/api/admin/schema_query.py:291-324`]

---

### Ayudantes de IA

#### `generateTableDescription(tableId: String!) → String!`

Usa el LLM configurado para generar una descripción de una o dos oraciones para una tabla registrada. Guarde la tabla primero; llamar a esto en una tabla no guardada devuelve un mensaje instructivo. [tool-verified: `provisa/api/admin/schema_query.py:1250-1298`]

#### `generateColumnDescription(tableId: String!, columnName: String!) → String!`

Usa el LLM configurado para generar una descripción de una oración para una sola columna. [tool-verified: `provisa/api/admin/schema_query.py:1334-1383`]

---

### Solicitudes de creación

#### `creationRequests → [CreationRequestType!]!`

Solicitudes de creación pendientes, visibles para los llamadores que posean la capacidad de creación relevante. Se usa cuando un miembro sin `create_relationship` o `create_view` envía una solicitud que un titular de derechos debe aprobar. (REQ-434, REQ-063) [tool-verified: `provisa/api/admin/schema_query.py:267-289`]

---

## Mutaciones

### Orígenes

#### `createSource(input: SourceInput!) → MutationResult`

Registra un nuevo origen de datos. Valida la conexión antes de persistir — un origen rechazado no deja ninguna entrada en la bóveda. Almacena las credenciales en la bóveda de la organización y registra la referencia; el texto plano nunca llega a la base de datos. (REQ-012, REQ-013) Requiere la capacidad `source_registration`. [tool-verified: `provisa/api/admin/schema_mutation.py:616-781`]

#### `updateSource(input: SourceInput!) → MutationResult`

Actualiza los detalles de conexión, la descripción y la configuración de un origen existente. Desmonta y vuelve a conectar el endpoint pgwire para orígenes de archivo/SharePoint, de modo que un cambio de ruta surta efecto de inmediato. Requiere `source_registration`. [tool-verified: `provisa/api/admin/schema_mutation.py:922-1073`]

#### `deleteSource(id: String!) → MutationResult`

Elimina un origen y su entrada en la bóveda. Descarta el catálogo del motor y reconstruye los esquemas. [tool-verified: `provisa/api/admin/schema_mutation.py:1101-1132`]

#### `renameSource(oldId: String!, newId: String!) → MutationResult`

Renombra el ID de un origen. [tool-verified: `provisa/api/admin/schema_mutation.py:1076-1098`]

#### `updateSourceCache(sourceId: String!, cacheEnabled: Boolean!, cacheTtl: Int) → MutationResult`

Habilita o deshabilita el almacenamiento en caché de resultados de consulta para un origen, y establece el TTL en segundos. [tool-verified: `provisa/api/admin/schema_mutation.py:2327-2350`]

#### `updateSourcePreferMaterialized(sourceId: String!, preferMaterialized: Boolean!) → MutationResult`

Fuerza (o libera) la federación materializada para todas las tablas de un origen. (REQ-826) [tool-verified: `provisa/api/admin/schema_mutation.py:2379-2402`]

#### `updateSourceLoadProtection(sourceId, loadProtected, offPeakWindow, offPeakTz) → MutationResult`

Marca un origen como protegido de carga (solo actualización programada). Requiere al menos una compuerta — ventana fuera de horario pico, cadencia de TTL de caché, o una señal de cambio por sondeo — o la llamada falla. (REQ-1141) [tool-verified: `provisa/api/admin/schema_mutation.py:2431-2480`]

#### `updateSourceNaming(sourceId: String!, gqlNamingConvention: String) → MutationResult`

Establece la convención de nomenclatura de GraphQL por origen. [tool-verified: `provisa/api/admin/schema_mutation.py:2595-2619`]

#### `updateSourceAllowedDomains(sourceId: String!, allowedDomains: [String!]!) → MutationResult`

Establece qué dominios pueden usar un origen (lista vacía = sin restricción). Requiere `source_registration`. (REQ-1531) [tool-verified: `provisa/api/admin/schema_mutation.py:2622-2658`]

#### `stageKaggleDataset(token, owner, ref, idPrefix) → KaggleStageResultType`

Descarga y descomprime un conjunto de datos de Kaggle en el disco local. Devuelve la ruta del directorio preparado; el llamador luego crea un origen de tipo `files` que apunta a él. Los paquetes que contienen SQLite se rechazan por completo. Requiere `source_registration`. (REQ-1780–1782) [tool-verified: `provisa/api/admin/schema_mutation.py:784-824`]

#### `refreshKaggleSource(sourceId: String!, token: String!) → MutationResult`

Vuelve a obtener el conjunto de datos de un origen derivado de Kaggle en el mismo lugar. Omite la descarga si Kaggle no tiene nada más nuevo que lo que hay en disco. Requiere `source_registration`. (REQ-1787) [tool-verified: `provisa/api/admin/schema_mutation.py:827-920`]

#### `refreshSourceStatistics(sourceId: String!) → MutationResult`

Ejecuta `ANALYZE` en todas las tablas registradas de un origen. Mejora las decisiones de orden de unión y de broadcast para las consultas federadas. (REQ-276) [tool-verified: `provisa/api/admin/schema_mutation.py:2944-3008`]

---

### Tablas

#### `registerTable(input: TableInput!) → MutationResult`

Registra una nueva tabla (o vista) en un dominio. Requiere la capacidad `table_registration` y pertenencia al dominio destino. Un llamador sin `create_relationship` que envía una vista se encola como una solicitud de creación para que un titular de derechos la apruebe. (REQ-013, REQ-016, REQ-252, REQ-434) [tool-verified: `provisa/api/admin/schema_mutation.py:1714-1718`]

#### `updateTable(input: TableInput!) → MutationResult`

Actualiza el alias, la descripción, los metadatos de columna, los ajustes de MV y la configuración de entrega en vivo de una tabla existente. (REQ-016, REQ-020) Requiere `table_registration`. [tool-verified: `provisa/api/admin/schema_mutation.py:1858-1995`]

#### `deleteTable(id: Int!) → MutationResult`

Elimina una tabla registrada. Busca el dominio de la tabla para la compuerta de dominio antes de eliminar. (REQ-1531) [tool-verified: `provisa/api/admin/schema_mutation.py:1998-2029`]

#### `updateTableCache(tableId: Int!, cacheTtl: Int) → MutationResult`

Anula el TTL de caché para una tabla. [tool-verified: `provisa/api/admin/schema_mutation.py:2353-2376`]

#### `updateTablePreferMaterialized(tableId: Int!, preferMaterialized: Boolean) → MutationResult`

Anula la federación materializada para una tabla. `null` = hereda el valor predeterminado del origen. (REQ-826) [tool-verified: `provisa/api/admin/schema_mutation.py:2405-2428`]

#### `updateTableLoadProtection(tableId, loadProtected, offPeakWindow, offPeakTz) → MutationResult`

Anula la protección de carga para una tabla. `null` para `loadProtected` hereda el valor predeterminado del origen. Valida la combinación efectiva de compuertas (tabla → origen). (REQ-1141) [tool-verified: `provisa/api/admin/schema_mutation.py:2483-2566`]

#### `updateTableNaming(tableId: Int!, gqlNamingConvention: String) → MutationResult`

Establece la convención de nomenclatura de GraphQL por tabla. [tool-verified: `provisa/api/admin/schema_mutation.py:2661-2685`]

#### `deployViewToDb(tableId: Int!) → MutationResult`

Promueve una vista virtual de Provisa a una vista de base de datos real en su origen nativo subyacente. [tool-verified: `provisa/api/admin/schema_mutation.py:3058-3060`]

#### `forceRegen(tableId: Int!, reason: String!) → MutationResult`

Recalcula las filas aterrizadas de una tabla a demanda, sin pasar por la compuerta de cambio normal. `reason` es una anotación de auditoría obligatoria. Se rechaza para tablas federadas en vivo (sin filas aterrizadas). (REQ-968) [tool-verified: `provisa/api/admin/schema_mutation.py:2689-2782`]

#### `invalidateFileSource(tableId: Int!) → MutationResult`

Fuerza que el próximo acceso de una tabla del conector de archivos SQLite vuelva a sincronizarse desde el disco. [tool-verified: `provisa/api/admin/schema_mutation.py:2877-2880`]

#### `registerEntity(input: EntityInput!) → MutationResult`

Azúcar sintáctico para registrar una entidad de dimensión/hub. Se reduce a una MV (bitemporal, cuando se historiza) y llama a `registerTable`. (REQ-1164) [tool-verified: `provisa/api/admin/schema_mutation.py:1721-1725`]

#### `registerFact(input: FactInput!) → MutationResult`

Azúcar sintáctico para registrar un fact de esquema en estrella. Se reduce a una MV agregada, crea relaciones de dimensión, y registra automáticamente las medidas del fact como métricas gobernadas. (REQ-1164, REQ-1320) [tool-verified: `provisa/api/admin/schema_mutation.py:1728-1776`]

---

### Relaciones

#### `upsertRelationship(input: RelationshipInput!) → MutationResult`

Crea o actualiza una relación. La compuerta verifica el dominio de la tabla de origen (no el del destino). Una arista entre dominios se almacena con `needsReview: true`. Un llamador sin `create_relationship` se encola como una solicitud de creación. Las aristas de unión (muchos a muchos) requieren longitudes de lista de claves coincidentes. (REQ-019, REQ-020, REQ-366, REQ-434) [tool-verified: `provisa/api/admin/schema_mutation.py:2297-2300`]

#### `deleteRelationship(id: String!) → MutationResult`

Elimina una relación por ID y reconstruye los esquemas. [tool-verified: `provisa/api/admin/schema_mutation.py:2303-2322`]

---

### Dominios

#### `createDomain(input: DomainInput!) → MutationResult`

Crea un dominio. Se rechazan las palabras de segmento reservadas (`tables`, `relationships` y otros segmentos de ruta URI), así como el literal comodín `*`. Requiere la capacidad `org_settings`. (REQ-021, REQ-1531) [tool-verified: `provisa/api/admin/schema_mutation.py:1135-1189`]

#### `deleteDomain(id: String!) → MutationResult`

Elimina un dominio. Requiere `org_settings`. (REQ-1531) [tool-verified: `provisa/api/admin/schema_mutation.py:1192-1213`]

#### `updateGqlNamingConvention(convention: String!) → MutationResult`

Establece la convención de nomenclatura global de GraphQL y reconstruye los esquemas para todos los roles. Solo se aceptan nombres de convención reconocidos. (REQ-253, REQ-416) [tool-verified: `provisa/api/admin/schema_mutation.py:2571-2592`]

---

### Roles

#### `createRole(input: RoleInput!) → MutationResult`

Crea o reemplaza un rol con capacidades, acceso a dominios, límites de tasa opcionales y un rol padre opcional. Valida que el padre exista y que la cadena de padres no tenga ciclos. Requiere `user_management`. (REQ-042, REQ-1531, REQ-1677) [tool-verified: `provisa/api/admin/schema_mutation.py:1657-1712`]

#### `deleteRole(id: String!) → MutationResult`

Elimina un rol. Falla si otros roles heredan de él — reasígneles un nuevo padre primero. Requiere `user_management`. (REQ-1531, REQ-1677) [tool-verified: `provisa/api/admin/schema_mutation.py:2032-2063`]

---

### Reglas de RLS

#### `upsertRlsRule(input: RLSRuleInput!) → MutationResult`

Crea o actualiza una regla de seguridad de nivel de fila. La expresión de filtro se valida en el momento de guardar contra las columnas de la tabla o el dominio destino, de modo que una regla que el administrador no puede consultar se rechaza con la razón en lugar de fallar silenciosamente en el momento de la consulta. Requiere `masking_config`. (REQ-041, REQ-402, REQ-1531, REQ-1676) [tool-verified: `provisa/api/admin/schema_mutation.py:2066-2136`]

Los destinos son mutuamente excluyentes: establezca `tableId` para una regla a nivel de tabla, `domainId` para una regla a nivel de dominio, o `actionName` para una función o webhook rastreados. (REQ-1679)

#### `deleteRlsRule(roleId, tableId, domainId, actionName) → MutationResult`

Elimina una regla de RLS. Requiere `masking_config`. (REQ-1531) [tool-verified: `provisa/api/admin/schema_mutation.py:2139-2178`]

---

### Productos de datos

#### `createDataProduct(input: DataProductInput!) → MutationResult`

Crea o reemplaza un producto de datos. Requiere la capacidad `data_product_rw`. (REQ-1634) [tool-verified: `provisa/api/admin/schema_mutation.py:1216-1259`]

#### `deleteDataProduct(id: String!) → MutationResult`

Elimina un producto de datos. Requiere `data_product_rw`. (REQ-1634) [tool-verified: `provisa/api/admin/schema_mutation.py:1262-1285`]

---

### Etiquetas

#### `upsertTag(input: TagInput!) → MutationResult`

Crea o actualiza una definición de etiqueta. Las etiquetas del sistema y las derivadas no pueden redefinirse. `appliesTo` debe ser un subconjunto no vacío de `["source", "table", "column", "relationship", "command"]`. (REQ-1373, REQ-1375) [tool-verified: `provisa/api/admin/schema_mutation.py:1288-1361`]

#### `deleteTag(id: String!) → MutationResult`

Elimina una etiqueta. Rechaza las etiquetas del sistema y las derivadas. (REQ-1373) [tool-verified: `provisa/api/admin/schema_mutation.py:1364-1393`]

#### `assignTag(input: TagAssignmentInput!) → MutationResult`

Asigna una etiqueta a un origen, tabla, columna, relación o comando. Aplica las políticas de campo de la etiqueta (`reason_policy`, `expires_policy`) y — para etiquetas parametrizadas — valida el valor del parámetro contra la lista permitida de la etiqueta. (REQ-1376, REQ-1377) [tool-verified: `provisa/api/admin/schema_mutation.py:1396-1527`]

#### `unassignTag(input: TagAssignmentInput!) → MutationResult`

Elimina una asignación de etiqueta. (REQ-1377) [tool-verified: `provisa/api/admin/schema_mutation.py:1530-1564`]

#### `upsertTagParamValue(input: TagParamValueInput!) → MutationResult`

Agrega o vuelve a describir un valor de parámetro permitido para una etiqueta parametrizada. La lista de valores permitidos es cerrada: cada asignación debe nombrar un valor de ella. (REQ-1467) [tool-verified: `provisa/api/admin/schema_mutation.py:1567-1616`]

#### `deleteTagParamValue(tagId: String!, value: String!) → MutationResult`

Elimina un valor permitido. Se rechaza mientras alguna asignación aún lo lleve, porque esas asignaciones nombrarían un tipo que la lista ya no admite. (REQ-1467) [tool-verified: `provisa/api/admin/schema_mutation.py:1619-1655`]

---

### Métricas

#### `upsertMetric(input: MetricInput!) → MutationResult`

Crea o reemplaza una definición de métrica gobernada. La expresión debe analizarse correctamente bajo sqlglot y contener al menos una función de agregación. Regenera todas las vistas compuestas por métricas que referencian esta métrica. Requiere `table_registration`. (REQ-1317, REQ-1318) [tool-verified: `provisa/api/admin/schema_mutation.py:1779-1831`]

#### `deleteMetric(name: String!) → MutationResult`

Elimina una métrica gobernada. Reconstruye los esquemas. Requiere `table_registration`. (REQ-1317) [tool-verified: `provisa/api/admin/schema_mutation.py:1834-1856`]

---

### Calendarios

#### `createCalendar(input: CalendarInput!) → MutationResult`

Crea o reemplaza un calendario versionado de límites de instantánea. Se valida construyendo el `Calendar` en memoria antes de persistir — falla ante un sistema base desconocido, una zona horaria incorrecta o un anclaje fiscal incorrecto. (REQ-962) [tool-verified: `provisa/api/admin/schema_mutation.py:525-579`]

#### `deleteCalendar(name: String!) → MutationResult`

Elimina un calendario (todas las versiones). Se rechaza cuando alguna vista materializada lo referencia. (REQ-962) [tool-verified: `provisa/api/admin/schema_mutation.py:582-613`]

---

### Vistas materializadas

#### `refreshMv(mvId: String!) → MutationResult`

Dispara una actualización manual de una vista materializada. Coordina entre la flota cuando el modo de consistencia de la MV es `shared`. (REQ-133, REQ-158, REQ-879) [tool-verified: `provisa/api/admin/schema_mutation.py:2787-2813`]

#### `toggleMv(mvId: String!, enabled: Boolean!) → MutationResult`

Habilita o deshabilita una vista materializada. [tool-verified: `provisa/api/admin/schema_mutation.py:2816-2839`]

---

### Caché

#### `purgeCache → MutationResult`

Purga todos los resultados de consulta cacheados. [tool-verified: `provisa/api/admin/schema_mutation.py:2844-2858`]

#### `purgeCacheByTable(tableId: Int!) → MutationResult`

Purga los resultados cacheados de una tabla. [tool-verified: `provisa/api/admin/schema_mutation.py:2861-2875`]

---

### Tareas programadas

#### `createScheduledTask(id, name, cron, kind, webhookName, argsJson, sql) → MutationResult`

Crea un disparador programado — ya sea una llamada webhook o una sentencia SQL — y lo registra en vivo en APScheduler. `kind` es `"webhook"` o `"sql"`. (REQ-1003, REQ-1004) [tool-verified: `provisa/api/admin/schema_mutation.py:2923-2936`]

#### `deleteScheduledTask(taskId: String!) → MutationResult`

Elimina un disparador programado de la configuración y del planificador en vivo. (REQ-1003) [tool-verified: `provisa/api/admin/schema_mutation.py:2939-2941`]

#### `toggleScheduledTask(taskId: String!, enabled: Boolean!) → MutationResult`

Habilita o deshabilita una tarea programada en el archivo de configuración. [tool-verified: `provisa/api/admin/schema_mutation.py:2885-2920`]

---

### Calidad de datos

#### `dryRunDqContract(sourceId: String!, contractText: String!) → DqDryRunType`

Ejecuta un contrato contra la tabla en vivo y devuelve los resultados sin aterrizar nada. Es una mutación en lugar de una consulta porque cuesta un escaneo real. Lo que demuestra es si el identificador del dataset resuelve a la tabla gobernada que el operador pretende. (REQ-1443 cláusula 7) [tool-verified: `provisa/api/admin/schema_mutation.py:463-487`]

#### `runDqCheckNow(schemaName: String!, tableName: String!) → MutationResult`

Dispara de inmediato el trabajo de sondeo de una tabla de verificador. Aterriza filas de la manera normal, de modo que los resultados persisten y el historial de DQ muestra el nuevo escaneo. (REQ-1443) [tool-verified: `provisa/api/admin/schema_mutation.py:490-522`]

---

### Mantenimiento de esquema

#### `rebuildSchemas → MutationResult`

Reconstruye el esquema en memoria a partir del estado de la base de datos. Útil después de cambios externos en la base de datos. [tool-verified: `provisa/api/admin/schema_mutation.py:456-461`]

---

### Solicitudes de creación

#### `executeCreationRequest(requestId: Int!) → MutationResult`

Un titular de derechos ejecuta una solicitud de creación encolada — relación, vista o webhook. Requiere la capacidad que la solicitud está esperando. (REQ-434, REQ-063) [tool-verified: `provisa/api/admin/schema_mutation.py:2181-2255`]

#### `rejectCreationRequest(requestId: Int!, reason: String!) → MutationResult`

Rechaza una solicitud encolada con una razón accionable. `reason` es obligatorio. (REQ-434, REQ-063) [tool-verified: `provisa/api/admin/schema_mutation.py:2258-2294`]

---

### Compilación de consultas

#### `compileQuery(input: CompileQueryInput!) → [CompileQueryResult!]!`

Compila una consulta GraphQL del plano de datos contra el esquema de un rol y devuelve la decisión de enrutamiento completa: SQL semántico, SQL de motor, SQL directo, ruta, metadatos de aplicación (filtros de RLS aplicados, columnas excluidas, enmascaramiento aplicado), y Cypher compilado. Devuelve un resultado por cada campo raíz en la consulta. (REQ-161) [tool-verified: `provisa/api/admin/schema_mutation.py:3011-3055`]

```graphql
mutation {
  compileQuery(input: {
    role: "analyst"
    query: "{ orders { id customer_id total } }"
  }) {
    sql
    semanticSql
    engineSql
    route
    routeReason
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      maskingApplied
    }
  }
}
```

Campos de `CompileQueryInput`:

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `query` | `String!` | Consulta GraphQL del plano de datos a compilar |
| `role` | `String!` | Rol contra cuyo esquema compilar |
| `variables` | `JSON` | Vínculos de variables |
| `flatSql` | `Boolean` | Devuelve una sola cadena SQL aplanada en lugar de un par semántico/de motor |
| `flatCypher` | `Boolean` | Aplana la salida de Cypher |
| `nodeOnlyCypher` | `Boolean` | Emite Cypher solo de nodos (sin patrones de arista) |

---

## Tipos de entrada clave

### `SourceInput`

[tool-verified: `provisa/api/admin/types.py:590-611`]

| Campo | Tipo | Notas |
|-------|------|-------|
| `id` | `String!` | Identificador del origen |
| `type` | `String!` | Tipo de conector (p. ej. `postgres`, `files`, `openapi`) |
| `host` | `String` | |
| `port` | `Int` | |
| `database` | `String` | |
| `username` | `String` | |
| `password` | `String` | Texto plano o referencia `${secret:NAME}` |
| `path` | `String` | Ruta del sistema de archivos para orígenes de archivo/CSV |
| `federationHintsJson` | `String` | Objeto JSON para extras de almacén de datos (warehouse/role de Snowflake, http_path de Databricks) |
| `changeSignal` | `String` | `ttl` \| `probe` \| `ttl_probe` (REQ-929) |
| `loadProtected` | `Boolean` | Solo actualización programada (REQ-1141) |
| `offPeakWindow` | `String` | Ventana de mantenimiento `HH:MM-HH:MM` |
| `offPeakTz` | `String` | Zona horaria IANA |
| `cdc` | `SourceCdcConfigInput` | Configuración de transporte CDC por Kafka (REQ-824) |

### `TableInput`

[tool-verified: `provisa/api/admin/types.py:745-800`]

La entrada central de registro de tablas. Campos clave más allá de lo básico:

| Campo | Notas |
|-------|-------|
| `materialize` | Aterriza una copia en el almacén de materialización |
| `mvRefreshInterval` | Segundos entre actualizaciones |
| `mvPersist` | `replace` \| `append` \| `upsert` (REQ-965) |
| `mvIncremental` | Mantenimiento incremental (REQ-969) |
| `mvBitemporalMode` | `snapshot` \| `delta` para tablas bitemporales (REQ-1162) |
| `mvCalendar` | Nombre del calendario de instantáneas (REQ-962) |
| `mvGrain` | Grano de instantánea: `daily`, `weekly`, `monthly`, `annual`, o personalizado `3WE` / `LFR` (REQ-962) |
| `viewSql` | SQL para una vista derivada |
| `viewMetrics` | Especificación declarativa de vista compuesta por métricas — mutuamente excluyente con `viewSql` (REQ-1318) |
| `dqContract` | Texto de contrato de calidad de datos en YAML/JSON (REQ-1443) |
| `queryTemplate` | Cypher para una tabla Neo4j (REQ-1670) |
| `live` | Configuración de entrega en vivo para push por SSE/Kafka (REQ-565, REQ-813) |
| `discover` | Infiere columnas desde el origen en vivo en el momento del registro (REQ-252) |

### `RelationshipInput`

[tool-verified: `provisa/api/admin/types.py:804-826`]

| Campo | Notas |
|-------|-------|
| `id` | Identificador de la relación |
| `sourceTableId` | Nombre de tabla virtual (alias si está establecido, si no el nombre de tabla) |
| `targetTableId` | Nombre de tabla virtual; vacío para relaciones calculadas |
| `sourceColumn` | Columna de unión en el lado del origen |
| `targetColumn` | Columna de unión en el lado del destino |
| `cardinality` | `one-to-one` \| `one-to-many` \| `many-to-one` \| `many-to-many` |
| `alias` | Etiqueta de arista Cypher (p. ej. `WORKS_FOR`) |
| `graphqlAlias` | Nombre de campo GraphQL en el tipo de origen |
| `viaTable` | Nombre de la tabla de unión para aristas de muchos a muchos (REQ-1586) |
| `recordCandidate` | También escribe una fila `accepted` en relationship_candidates |

---

## Ejemplo: registrar una tabla

```graphql
mutation {
  registerTable(input: {
    sourceId: "sales-pg"
    domainId: "sales"
    schemaName: "public"
    tableName: "orders"
    alias: "orders"
    columns: [
      { name: "id", visibleTo: ["public"], isPrimaryKey: true }
      { name: "customer_id", visibleTo: ["public"] }
      { name: "total", visibleTo: ["public"] }
    ]
  }) {
    success
    message
    code
  }
}
```

## Ejemplo: crear una regla de RLS

```graphql
mutation {
  upsertRlsRule(input: {
    tableId: "orders"
    roleId: "regional-analyst"
    filterExpr: "region = '{{user.region}}'"
  }) {
    success
    message
  }
}
```
