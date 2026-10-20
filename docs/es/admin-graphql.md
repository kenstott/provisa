# Referencia de la API GraphQL de administración {: #admin-graphql-api-reference }

La API GraphQL de administración es el plano de configuración de Provisa. Es la API que la aplicación web de administración llama para toda operación de gestión: crear orígenes, registrar tablas, definir relaciones, configurar reglas de RLS, y todo lo demás que da forma al modelo.

**Punto de montaje:** `POST /admin/graphql`

Esta no es la misma API que el plano de datos en `/data/graphql`. El plano de datos sirve las consultas de los usuarios finales sobre los dominios registrados y se describe mediante el SDL en `/data/sdl`. La API de administración configura cómo se ve ese esquema y quién puede ver qué.

---

## Cómo habla la UI con esta API {: #how-the-ui-talks-to-this-api }

La aplicación web de administración usa Apollo Client, apuntando a `${API_BASE}/admin/graphql`.

Cada solicitud lleva un token bearer (obtenido de nuevo del proveedor de autenticación en cada llamada), un encabezado `X-Org-Id` cuando es multiinquilino, y un encabezado `X-Env` cuando sirve un entorno de rama.

El esquema se ensambla a partir de dos clases `@strawberry.type` — `Query` de `schema_query.py` y `Mutation` de `schema_mutation.py` — y se envuelve en una `ModelCommitExtension` que registra cada mutación contra la rama de entorno actual.

---

## Autorización {: #authorization }

**Modo de desarrollo:** cuando no hay autenticación configurada y toda solicitud llega como un principal anónimo, se omiten todas las verificaciones de capacidad. Esto mantiene una instalación local funcional sin configurar autenticación.

**Compuertas de capacidad:** los despliegues de producción aplican capacidades nombradas. El derecho específico requerido por cada campo se indica en línea. Llamar a una mutación sin la capacidad requerida lanza un `PermissionError`. El rol de administrador de la plataforma omite todas las verificaciones de capacidad.

**Compuertas de dominio:** varias mutaciones también verifican el dominio al que pertenece el objeto. Un llamador con alcance en `sales` no puede registrar una tabla en `finance`, encolar una regla de RLS para ella, ni crear una relación cuya tabla de origen viva en un dominio que no posee. Las vistas están además restringidas: cada tabla que el SQL de la vista lee debe estar dentro de los dominios del llamador, porque de lo contrario el SQL de mano libre le daría a un miembro acceso a datos fuera de su alcance.

**Herencia de roles:** las capacidades de un rol padre son heredadas por los roles hijos. `createRole` y `deleteRole` rechazan ciclos e impiden eliminar un rol que tenga herederos.

---

## Tipo de retorno común {: #common-return-type }

La mayoría de las mutaciones devuelven `MutationResult`.

```graphql
type MutationResult {
  success: Boolean!
  message: String!
  code: String          # stable i18n key, e.g. "schema.source_created"
  params: JSON          # key/value pairs for client-side localization
}
```

Cuando una mutación falla, `success` es `false` y `message` lleva la razón en inglés. `code` es un identificador estable que la UI usa para renderizar un mensaje localizado.

---

## Consultas {: #queries }

### Orígenes {: #sources }

#### `sources → [SourceType!]!`

Todos los orígenes de datos registrados.

```graphql
query {
  sources {
    id type host port database username dialect
    cacheEnabled cacheTtl replicate
    loadProtected offPeakWindow offPeakTz
    gqlNamingConvention path allowedDomains
    description mappingJson federationHintsJson
    changeSignal passwordRef
    cdc {
      bootstrapServers topicPrefix consumerGroupId
      schemaRegistryUrl schemaRegistryAuth schemaRegistryUsername schemaRegistryPassword
      schemaRegistryToken schemaRegistryClientCert schemaRegistryClientKey schemaRegistryCa
    }
  }
}
```

`passwordRef` es una referencia `${secret:NAME}` a la bóveda de la organización, nunca la credencial literal.

#### `source(id: String!) → SourceType`

Un solo origen por ID. Devuelve `null` cuando no se encuentra.

#### `availableSchemas(sourceId: String!) → [String!]!`

Esquemas visibles en un origen, filtrados para excluir los internos de Provisa. Usa primero la introspección nativa; recae en el catálogo del motor cuando el tipo de origen no tiene pool directo.

#### `availableTables(sourceId: String!, schemaName: String = "public") → [AvailableTableType!]!`

Tablas de un esquema de un origen, con sus comentarios. Para orígenes OpenAPI, devuelve operaciones GET cuya respuesta es un arreglo o un envoltorio de paginación. Para orígenes GraphQL, devuelve campos de consulta que devuelven una lista. Para gRPC, devuelve RPC de streaming del servidor.

#### `availableColumns(sourceId: String!, schemaName: String!, tableName: String!) → [String!]!`

Nombres de columna de una tabla en el catálogo del motor. Para orígenes govdata, usa un resolutor aparte.

#### `availableColumnsMetadata(sourceId: String!, schemaName: String!, tableName: String!) → [AvailableColumnType!]!`

Nombres de columna con tipos de datos, comentarios, tipos de filtro nativos e indicadores de clave primaria. Para orígenes OpenAPI, deriva la forma a partir del esquema de respuesta y los parámetros de la operación.

#### `availableFunctions(sourceId: String!, schemaName: String = "openapi") → [AvailableTableType!]!`

Operaciones que no son GET para un origen OpenAPI (POST, PUT, PATCH, DELETE). Devuelve una lista vacía para orígenes que no son OpenAPI.

#### `crawlSource(path, depth, pattern, recursive, simpleLinks, sameDomain, excludePattern) → CrawlResultType`

Vista previa de lo que descubriría un rastreo del conector de archivos — archivos, tablas y columnas — antes de crear un origen. Los ajustes exclusivos de HTTP (`simpleLinks`, `sameDomain`, `excludePattern`) se ignoran para raíces locales, S3, FTP y SFTP.

#### `suggestTableAlias(tableName: String!, domainId: String!, sourceId: String!) → String!`

Devuelve el alias a usar al registrar `tableName` en `domainId` desde `sourceId`. Devuelve un alias simple en snake-case cuando no hay conflicto, o un alias prefijado con el origen (`sqlite_b_orders`) cuando el nombre efectivo ya está tomado por un origen distinto en el mismo dominio.

---

### Tablas {: #tables }

#### `tables → [RegisteredTableType!]!`

Todas las tablas registradas, cada una con su lista completa de columnas. La visibilidad de columnas en la respuesta respeta la capacidad `table_registration` del llamador: `canDeployToDb` depende de si el llamador posee ese derecho.

Cada `RegisteredTableType` expone subcampos calculados:

- **`refreshPolicySummary → RefreshPolicySummaryType`** — la política efectiva de actualización/servicio como texto plano, derivada del lado del servidor a partir de la misma resolución del planificador que usa el motor. Devuelve `null` durante el arranque.
- **`graphqlFieldName → String`** — el nombre de campo que esta tabla tiene en el esquema compilado del plano de datos, para que el panel de Data Product pueda construir un ejemplo ejecutable sin replicar el algoritmo de nomenclatura.
- **`dqDataset → String`** — esta tabla como un conjunto de datos de contrato de calidad de datos, en la forma que el verificador escanea.
- **`productId → String`** — el producto de datos al que pertenece esta tabla. Una tabla de verificador de DQ hereda el producto de la tabla cuyo contrato escanea.

#### `refreshPolicyPreview(...) → RefreshPolicySummaryType`

Vista previa del resumen efectivo de actualización/servicio para valores de tabla *borrador* (sin guardar), de modo que el resumen en la parte superior del formulario se actualice al cambiar los campos sin persistir nada. Misma derivación que `refreshPolicySummary` arriba.

Argumentos: `sourceId`, `domainId`, `schemaName`, `tableName`, `cacheTtl`, `replicate`, `loadProtected`, `offPeakWindow`, `offPeakTz`, `changeSignal`.

#### `columnDependents(tableId: String!, renamed: [String!], removed: [String!]) → [ColumnDependentsType!]!`

Artefactos que un cambio de alias o una eliminación de columna pendientes romperían. Es informativo: la UI de administración lo muestra antes de guardar y el administrador decide. Debe llamarse *antes* de guardar, porque los dependientes fueron escritos contra el nombre expuesto que la columna lleva actualmente.

---

### Relaciones {: #relationships }

#### `relationships → [RelationshipType!]!`

Todas las relaciones definidas por el usuario (excluye las entradas `gql_auto__` autogeneradas y las entradas sintéticas `meta:%` usadas por el ERD).

#### `allRelationships → [RelationshipType!]!`

Igual que `relationships`, pero incluye las entradas sintéticas `meta:%`. Usado por el ERD de grafo, que necesita mostrar cada arista, incluyendo los enlaces implícitos `HAS_TABLE` entre las tablas de datos y el registro de metadatos.

Cada `RelationshipType` expone:

- **`autoSuggested → Boolean`** — si la relación fue sugerida por el análisis de claves foráneas (el `id` comienza con `fk__`).
- **`physicalName → String`** — el nombre de la relación en los planos de SQL y gRPC (el parámetro `?include=`). Derivado del lado del servidor; los clientes no deben transliterar el alias de GraphQL.

---

### Dominios, roles y usuarios {: #domains-roles-users }

#### `domains → [DomainType!]!`

Todos los dominios en la base de datos del tenant de la organización activa. La base de datos del tenant está aislada a nivel de esquema, de modo que la lista de dominios de un org-admin contiene solo las filas de su organización.

#### `roles → [RoleType!]!`

Roles visibles para el llamador. Un administrador ve cada rol; un no administrador ve solo los roles sin `org_id` o los roles que pertenecen a su organización.

#### `resolveOwners(refs: [String!]!) → [UserSummaryType!]!`

Resuelve IDs de rol o de usuario a usuarios individuales. Se usa para expandir `DataProduct.ownerRole`, `Domain.steward` y `Column.visibleTo` en una lista legible por humanos. Las referencias desconocidas se devuelven tal cual, para que la UI muestre el ID en bruto en lugar de nada.

---

### Reglas de RLS {: #rls-rules }

#### `rlsRules → [RLSRuleType!]!`

Todas las reglas de seguridad de nivel de fila. El repositorio subyacente descifra `filterExpr` en el límite.

---

### Productos de datos {: #data-products }

#### `dataProducts → [DataProductType!]!`

Todos los productos de datos. Requiere la capacidad `data_product_read`.

---

### Etiquetas {: #tags }

#### `tags → [TagType!]!`

Todas las definiciones de etiquetas, incluyendo sus valores de parámetro permitidos por etiqueta.

#### `tagAssignments → [TagAssignmentType!]!`

Todas las asignaciones de etiquetas entre orígenes, tablas, columnas y relaciones.

---

### Vistas materializadas {: #materialized-views }

#### `mvList → [MVType!]!`

Todas las vistas materializadas con su estado en tiempo de ejecución: habilitada/deshabilitada, marca de tiempo de la última actualización, conteo de filas y último error.

---

### Caché {: #cache }

#### `cacheStats → CacheStatsType`

Estadísticas de caché. Devuelve `storeType: "redis"` con métricas operativas completas cuando Redis está configurado, `storeType: "memory"` para el almacén fakeredis embebido, y `storeType: "noop"` cuando no hay caché configurada.

#### `cacheTableStats → [CacheTableStatType!]!`

Conteos de entradas cacheadas por tabla. Vacío cuando no hay un almacén de caché configurado.

#### `hotTables → [HotTableStatType!]!`

Tablas de las que Provisa mantiene una copia, en dos niveles: `hot` (reflejada en el almacén de respuesta para el inlining de JOIN) y `warm` (aterrizada como una copia Iceberg). Una tabla está en como máximo un nivel.

#### `materializeStoreInfo → MaterializeStoreInfoType`

Identidad del almacén de materialización durable: nombre del motor, referencia de DSN del almacén, conteo de MV, y si el almacén es local a la instancia (un archivo local como DuckDB o SQLite, lo que significa que cada instancia detrás de un balanceador de carga mantiene su propia copia).

---

### Salud del sistema {: #system-health }

#### `systemHealth → SystemHealthType`

Estado de conexión del motor, conteos del pool de workers, estado del pool de la BD de metadatos, modo de caché, y disponibilidad de cada listener de protocolo (pgwire, gRPC, Arrow Flight, Bolt).

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

### Tareas programadas {: #scheduled-tasks }

#### `scheduledTasks → [ScheduledTaskType!]!`

Disparadores programados desde la configuración con estado en tiempo de ejecución. Cada entrada lleva su expresión cron, `kind` (`webhook` o `sql`), si está actualmente habilitada, la marca de tiempo de la última ejecución (siempre `null`; la sigue el planificador), y la próxima hora de ejecución programada desde APScheduler.

---

### Calidad de datos {: #data-quality }

#### `dqContractParse(checker: String!, contractText: String!) → DqContractType`

Analiza el texto de un contrato en bruto en las filas editables del panel constructor. Se llama en cada edición; un fallo de análisis regresa como `error` en lugar de como un error de GraphQL, porque el texto a medio escribir es normal mientras el operador está escribiendo.

#### `dqCheckCatalog(checker: String!, dataset: String!) → DqCheckCatalogType`

Las verificaciones que ofrece `checker`, con alcance a las columnas de `dataset`. El dataset es el objetivo observado del contrato, resuelto de la misma manera que lo resuelve el escáner, de modo que las verificaciones ofrecidas coincidan con las columnas que el verificador realmente verá.

#### `dqCheckDefinition(checker: String!, check: DqCheckBuildInput!) → DqCheckDefinitionType`

El texto de una verificación desde los editores del panel. Se hace del lado del servidor porque el dialecto tiene una única implementación; una verificación construida en el panel y una escrita a mano deben ser indistinguibles.

#### `dqContractBuild(checker: String!, dataset: String!, checks: [DqCheckInput!]!) → DqContractTextType`

Serializa las filas de verificación editadas de vuelta a texto de contrato. La inversa de `dqContractParse`. Del lado del servidor por la misma razón: el panel no puede emitir texto que el verificador rechazaría.

---

### Vistas previas de origen {: #source-previews }

#### `neo4jPreview(sourceId: String!, cypher: String!) → QueryPreviewType`

Vista previa de una proyección Cypher en un origen Neo4j: hasta cinco filas y los tipos de columna que el registro llevará. Los fallos regresan como `error`.

#### `sparqlPreview(sourceId: String!, query: String!) → QueryPreviewType`

Vista previa de un SELECT SPARQL en un origen SPARQL: hasta cinco filas, todas las columnas como texto.

---

### Kaggle

#### `kaggleTokenValid(token: String!) → Boolean!`

Verificación en vivo contra la API de Kaggle. Devuelve `true` solo cuando el token se autentica. Respalda el paso de compuerta de token en el formulario de origen de Kaggle.

#### `kaggleDatasets(token: String!, query: String = "", page: Int = 1) → [KaggleDatasetType!]!`

Busca en el catálogo público completo de conjuntos de datos de Kaggle. Sujeto a token.

---

### Calendarios {: #calendars }

#### `calendars → [CalendarType!]!`

Todas las versiones de calendario de límites de instantánea registradas. Alimenta el selector de configuración de programación de instantáneas y confirma qué calendarios puede referenciar una MV periódica.

---

### Métricas {: #metrics }

#### `metrics → [MetricType!]!`

Todas las definiciones de métricas gobernadas. Las métricas derivadas de un fact llevan `fromFact`.

---

### Versión del esquema {: #schema-version }

#### `schemaVersion → String!`

Hash SHA-256 del estado actual del esquema (dominios, IDs de tabla, IDs de relación). El cliente Apollo lee esto del encabezado de respuesta `X-Schema-Version` y vuelve a obtener todas las consultas activas cuando avanza.

---

### Ayudantes de IA {: #ai-helpers }

#### `generateTableDescription(tableId: String!) → String!`

Usa el LLM configurado para generar una descripción de una o dos oraciones para una tabla registrada. Guarde la tabla primero; llamar a esto en una tabla no guardada devuelve un mensaje instructivo.

#### `generateColumnDescription(tableId: String!, columnName: String!) → String!`

Usa el LLM configurado para generar una descripción de una oración para una sola columna.

---

### Solicitudes de creación {: #creation-requests }

#### `creationRequests → [CreationRequestType!]!`

Solicitudes de creación pendientes, visibles para quienes tienen la capacidad de creación correspondiente. Se usa cuando un miembro sin `create_relationship` ni `create_view` envía una solicitud que debe aprobar quien tiene el derecho. Una solicitud de relación se muestra a los usuarios cuyo derecho `create_relationship` alcanza el dominio de su tabla de origen o de destino, y al usuario que la hizo.

---

## Mutaciones {: #mutations }

### Orígenes {: #sources_1 }

#### `createSource(input: SourceInput!) → MutationResult`

Registra un nuevo origen de datos. Un id que ya existe se rechaza (`schema.source_id_taken`) después de las comprobaciones de la definición y antes de preparar o escribir nada; un origen se cambia con `updateSource`. Valida la conexión antes de persistir: un origen rechazado no deja ninguna entrada en la bóveda. Guarda las credenciales en la bóveda de la organización y registra la referencia; el texto plano nunca llega a la base de datos. Requiere la capacidad `source_registration`.

#### `updateSource(input: SourceInput!) → MutationResult`

Actualiza los datos de conexión, la descripción y la configuración de un origen existente, después lo aprovisiona de nuevo en el motor vinculado, reconstruye los esquemas y concilia sus tablas aterrizadas, de modo que un origen guardado con un motor lo sirva el motor al que se cambió después. Desmonta y vuelve a montar el endpoint pgwire de los orígenes de archivos/SharePoint para que un cambio de ruta surta efecto de inmediato. Requiere `source_registration`.

#### `deleteSource(id: String!) → MutationResult`

Elimina un origen y su entrada en la bóveda. Descarta el catálogo del motor y reconstruye los esquemas.

#### `renameSource(oldId: String!, newId: String!) → MutationResult`

Cambia el ID de un origen. Un ID nuevo que ya tiene otro origen se rechaza (`schema.source_id_taken`) y no se escribe nada; renombrar un origen a su propio ID no cambia nada.

#### `updateSourceCache(sourceId: String!, cacheEnabled: Boolean!, cacheTtl: Int) → MutationResult`

Habilita o deshabilita el almacenamiento en caché de resultados de consulta para un origen, y establece el TTL en segundos.

#### `updateSourceReplicate(sourceId: String!, replicate: Int) → MutationResult`

Fija cuándo las tablas de un origen se sirven desde sus réplicas: `null` Default, `-1` Never, `N` mayor que 0 en cuanto una tabla supera N sentencias gobernadas por intervalo, `0` Always. Una tabla sin valor propio lo hereda. Se rechaza cuando emparejaría `load_protected` con `-1`, o dejaría sin `cache_ttl` una tabla `ttl` que replica. Requiere `source_registration`.

#### `updateSourceLoadProtection(sourceId, loadProtected, offPeakWindow, offPeakTz) → MutationResult`

Marca un origen como protegido de carga (solo actualización programada). Requiere al menos una compuerta — ventana fuera de horario pico, cadencia de TTL de caché, o una señal de cambio por sondeo — o la llamada falla.

#### `updateSourceNaming(sourceId: String!, gqlNamingConvention: String) → MutationResult`

Establece la convención de nomenclatura de GraphQL por origen.

#### `updateSourceAllowedDomains(sourceId: String!, allowedDomains: [String!]!) → MutationResult`

Establece qué dominios pueden usar un origen (lista vacía = sin restricción). Requiere `source_registration`.

#### `stageKaggleDataset(token, owner, ref, idPrefix) → KaggleStageResultType`

Descarga y descomprime un conjunto de datos de Kaggle en el disco local. Devuelve la ruta del directorio preparado; el llamador luego crea un origen de tipo `files` que apunta a él. Los paquetes que contienen SQLite se rechazan por completo. Requiere `source_registration`.

#### `refreshKaggleSource(sourceId: String!, token: String!) → MutationResult`

Vuelve a obtener el conjunto de datos de un origen derivado de Kaggle en el mismo lugar. Omite la descarga si Kaggle no tiene nada más nuevo que lo que hay en disco. Requiere `source_registration`.

#### `refreshSourceStatistics(sourceId: String!) → MutationResult`

Ejecuta `ANALYZE` en todas las tablas registradas de un origen. Mejora las decisiones de orden de unión y de broadcast para las consultas federadas.

---

### Tablas {: #tables_1 }

#### `registerTable(input: TableInput!) → MutationResult`

Registra una nueva tabla (o vista) en un dominio. Requiere la capacidad `table_registration` y pertenencia al dominio destino. Un llamador sin `create_relationship` que envía una vista se encola como una solicitud de creación para que un titular de derechos la apruebe.

#### `updateTable(input: TableInput!) → MutationResult`

Actualiza el alias, la descripción, los metadatos de columna, los ajustes de MV y la configuración de entrega en vivo de una tabla existente. Requiere `table_registration`.

#### `deleteTable(id: Int!) → MutationResult`

Elimina una tabla registrada. Busca el dominio de la tabla para la compuerta de dominio antes de eliminar.

#### `updateTableCache(tableId: Int!, cacheTtl: Int) → MutationResult`

Anula el TTL de caché para una tabla.

#### `updateTableReplicate(tableId: Int!, replicate: Int) → MutationResult`

Fija cuándo una tabla se sirve desde su réplica. `null` = heredar el valor del origen. Mismos valores y rechazos que `updateSourceReplicate`. Requiere `table_registration`.

#### `updateTableLoadProtection(tableId, loadProtected, offPeakWindow, offPeakTz) → MutationResult`

Anula la protección de carga para una tabla. `null` para `loadProtected` hereda el valor predeterminado del origen. Valida la combinación efectiva de compuertas (tabla → origen).

#### `updateTableNaming(tableId: Int!, gqlNamingConvention: String) → MutationResult`

Establece la convención de nomenclatura de GraphQL por tabla.

#### `deployViewToDb(tableId: Int!) → MutationResult`

Promueve una vista virtual de Provisa a una vista de base de datos real en su origen nativo subyacente.

#### `forceRegen(tableId: Int!, reason: String!) → MutationResult`

Recalcula las filas aterrizadas de una tabla a demanda, sin pasar por la compuerta de cambio normal. `reason` es una anotación de auditoría obligatoria. Se rechaza para tablas federadas en vivo (sin filas aterrizadas).

#### `invalidateFileSource(tableId: Int!) → MutationResult`

Fuerza que el próximo acceso de una tabla del conector de archivos SQLite vuelva a sincronizarse desde el disco.

#### `registerEntity(input: EntityInput!) → MutationResult`

Azúcar sintáctico para registrar una entidad de dimensión/hub. Se reduce a una MV (bitemporal, cuando se historiza) y llama a `registerTable`.

#### `registerFact(input: FactInput!) → MutationResult`

Azúcar sintáctico para registrar un fact de esquema en estrella. Se reduce a una MV agregada, crea relaciones de dimensión, y registra automáticamente las medidas del fact como métricas gobernadas.

---

### Relaciones {: #relationships_1 }

#### `createRelationship(input: RelationshipInput!) → MutationResult`

Crea una relación. La puerta comprueba el dominio de la tabla de origen (no el del destino). Una arista entre dominios se guarda con `needsReview: true`. A quien no tiene `create_relationship` se le encola como solicitud de creación. Las aristas de unión (muchos a muchos) requieren listas de claves de la misma longitud. Se rechaza por su nombre, antes de escribir nada: un id que ya existe (`schema.relationship_id_taken`); un nombre de tabla que no resuelve a ninguna tabla registrada o a más de una (`schema.relationship_unknown_source`, `_target`, `_junction`, `schema.relationship_ambiguous_table`); un alias Cypher o un alias GraphQL que ya tiene otra relación de la misma tabla de origen (`schema.relationship_alias_taken`, `schema.relationship_graphql_alias_taken`). Una tabla se nombra por su nombre virtual: su alias cuando lo tiene.

#### `updateRelationship(input: RelationshipInput!) → MutationResult`

Cambia una relación existente, con las mismas comprobaciones que `createRelationship`. Un id que no tiene ninguna relación se rechaza (`schema.relationship_not_found`). Un cambio necesita el derecho en sí; nunca se encola como solicitud.

#### `deleteRelationship(id: String!) → MutationResult`

Elimina una relación por ID y reconstruye los esquemas.

---

### Dominios {: #domains }

#### `createDomain(input: DomainInput!) → MutationResult`

Crea un dominio. Un id que ya existe se rechaza (`schema.domain_id_taken`). Las palabras reservadas de segmento (`tables`, `relationships` y otros segmentos de ruta URI) se rechazan, igual que el literal comodín `*`. Requiere la capacidad `org_settings`.

#### `deleteDomain(id: String!) → MutationResult`

Elimina un dominio. Requiere `org_settings`.

#### `updateGqlNamingConvention(convention: String!) → MutationResult`

Establece la convención de nomenclatura global de GraphQL y reconstruye los esquemas para todos los roles. Solo se aceptan nombres de convención reconocidos.

---

### Roles

#### `createRole(input: RoleInput!) → MutationResult`

Crea un rol con capacidades, acceso a dominios, límites de tasa opcionales y un rol padre opcional. Valida que el padre exista y que la cadena de padres no tenga ciclos. Tras las comprobaciones de la definición, un id que ya existe se rechaza (`schema.role_id_taken`). Requiere `user_management`.

#### `updateRole(input: RoleInput!) → MutationResult`

Cambia un rol existente, con las mismas comprobaciones que `createRole`. Un id que no tiene ningún rol se rechaza (`schema.role_not_found`). Redefinir un rol es un acto en cada dominio que alcanza.

#### `deleteRole(id: String!) → MutationResult`

Elimina un rol. Falla si otros roles heredan de él; reasígneles un nuevo padre primero. Requiere `user_management`.

---

### Reglas de RLS {: #rls-rules_1 }

#### `upsertRlsRule(input: RLSRuleInput!) → MutationResult`

Crea o actualiza una regla de seguridad de nivel de fila. La expresión de filtro se valida en el momento de guardar contra las columnas de la tabla o el dominio destino, de modo que una regla que el administrador no puede consultar se rechaza con la razón en lugar de fallar silenciosamente en el momento de la consulta. Requiere `masking_config`.

Los destinos son mutuamente excluyentes: establezca `tableId` para una regla a nivel de tabla, `domainId` para una regla a nivel de dominio, o `actionName` para una función o webhook rastreados.

#### `deleteRlsRule(roleId, tableId, domainId, actionName) → MutationResult`

Elimina una regla de RLS. Requiere `masking_config`.

---

### Productos de datos {: #data-products_1 }

#### `createDataProduct(input: DataProductInput!) → MutationResult`

Crea un producto de datos. Un id que ya existe se rechaza (`schema.data_product_id_taken`). Requiere la capacidad `data_product_rw`.

#### `updateDataProduct(input: DataProductInput!) → MutationResult`

Cambia un producto de datos existente. Un id que no tiene ningún producto se rechaza (`schema.data_product_not_found`). Requiere `data_product_rw`.

#### `deleteDataProduct(id: String!) → MutationResult`

Elimina un producto de datos. Requiere `data_product_rw`.

---

### Etiquetas {: #tags_1 }

#### `createTag(input: TagInput!) → MutationResult`

Crea una definición de etiqueta. Un id que ya existe se rechaza (`schema.tag_id_taken`). Las etiquetas de sistema y las derivadas no se pueden redefinir. `appliesTo` debe ser un subconjunto no vacío de `["source", "table", "column", "relationship", "command"]`.

#### `updateTag(input: TagInput!) → MutationResult`

Cambia una definición de etiqueta existente, con las mismas comprobaciones que `createTag`. Un id que no tiene ninguna etiqueta se rechaza (`schema.tag_not_found`).

#### `deleteTag(id: String!) → MutationResult`

Elimina una etiqueta. Rechaza las etiquetas del sistema y las derivadas.

#### `assignTag(input: TagAssignmentInput!) → MutationResult`

Asigna una etiqueta a un origen, tabla, columna, relación o comando. Aplica las políticas de campo de la etiqueta (`reason_policy`, `expires_policy`) y — para etiquetas parametrizadas — valida el valor del parámetro contra la lista permitida de la etiqueta.

#### `unassignTag(input: TagAssignmentInput!) → MutationResult`

Elimina una asignación de etiqueta.

#### `upsertTagParamValue(input: TagParamValueInput!) → MutationResult`

Agrega o vuelve a describir un valor de parámetro permitido para una etiqueta parametrizada. La lista de valores permitidos es cerrada: cada asignación debe nombrar un valor de ella.

#### `deleteTagParamValue(tagId: String!, value: String!) → MutationResult`

Elimina un valor permitido. Se rechaza mientras alguna asignación aún lo lleve, porque esas asignaciones nombrarían un tipo que la lista ya no admite.

---

### Métricas {: #metrics_1 }

#### `createMetric(input: MetricInput!) → MutationResult`

Crea una definición de métrica gobernada. Un nombre que ya existe se rechaza (`schema.metric_name_taken`). La expresión debe analizarse con sqlglot y contener al menos una función de agregación. Regenera todas las vistas compuestas por métricas que hacen referencia a esta métrica. Requiere `table_registration`.

#### `updateMetric(input: MetricInput!) → MutationResult`

Sustituye la definición de una métrica existente, con las mismas comprobaciones que `createMetric`. Un nombre que no tiene ninguna métrica se rechaza (`schema.metric_not_found`). Guardarla necesita los dominios de la nueva expresión y de la definición que sustituye.

#### `deleteMetric(name: String!) → MutationResult`

Elimina una métrica gobernada. Reconstruye los esquemas. Requiere `table_registration`.

---

### Calendarios {: #calendars_1 }

#### `createCalendar(input: CalendarInput!) → MutationResult`

Crea o reemplaza un calendario versionado de límites de instantánea. Se valida construyendo el `Calendar` en memoria antes de persistir. Falla ante un sistema base desconocido, una zona horaria incorrecta o un anclaje fiscal incorrecto.

#### `deleteCalendar(name: String!) → MutationResult`

Elimina un calendario (todas las versiones). Se rechaza cuando alguna vista materializada lo referencia.

---

### Vistas materializadas {: #materialized-views_1 }

#### `refreshMv(mvId: String!) → MutationResult`

Dispara una actualización manual de una vista materializada. Coordina entre la flota cuando el modo de consistencia de la MV es `shared`.

#### `toggleMv(mvId: String!, enabled: Boolean!) → MutationResult`

Habilita o deshabilita una vista materializada.

---

### Caché {: #cache_1 }

#### `purgeCache → MutationResult`

Purga todos los resultados de consulta cacheados.

#### `purgeCacheByTable(tableId: Int!) → MutationResult`

Purga los resultados cacheados de una tabla.

---

### Tareas programadas {: #scheduled-tasks_1 }

#### `createScheduledTask(id, name, cron, kind, webhookName, argsJson, sql) → MutationResult`

Crea un disparador programado — ya sea una llamada webhook o una sentencia SQL — y lo registra en vivo en APScheduler. `kind` es `"webhook"` o `"sql"`.

#### `deleteScheduledTask(taskId: String!) → MutationResult`

Elimina un disparador programado de la configuración y del planificador en vivo.

#### `toggleScheduledTask(taskId: String!, enabled: Boolean!) → MutationResult`

Habilita o deshabilita una tarea programada en el archivo de configuración.

---

### Calidad de datos {: #data-quality_1 }

#### `dryRunDqContract(sourceId: String!, contractText: String!) → DqDryRunType`

Ejecuta un contrato contra la tabla en vivo y devuelve los resultados sin aterrizar nada. Es una mutación en lugar de una consulta porque cuesta un escaneo real. Lo que demuestra es si el identificador del dataset resuelve a la tabla gobernada que el operador pretende.

#### `runDqCheckNow(schemaName: String!, tableName: String!) → MutationResult`

Dispara de inmediato el trabajo de sondeo de una tabla de verificador. Aterriza filas de la manera normal, de modo que los resultados persisten y el historial de DQ muestra el nuevo escaneo.

---

### Mantenimiento de esquema {: #schema-maintenance }

#### `rebuildSchemas → MutationResult`

Reconstruye el esquema en memoria a partir del estado de la base de datos. Útil después de cambios externos en la base de datos.

---

### Solicitudes de creación {: #creation-requests_1 }

#### `approveCreationRequest(requestId: Int!) → MutationResult`

Aprueba una solicitud de creación: la misma regla, los mismos rechazos y las mismas entradas del registro de administración que `POST /admin/creation-requests/{id}/approve`. El solicitante no puede aprobar su propia solicitud y la aprobación de un usuario cuenta una vez. La aprobación que completa el recuento de la solicitud crea lo que pide; `params.status` es `executed` cuando lo hizo esta, si no `pending`, y `params.waitingOn` enumera los dominios que una solicitud de relación aún espera.

#### `executeCreationRequest(requestId: Int!) → MutationResult`

Ejecuta una solicitud de creación que ya tiene sus aprobaciones — relación, vista, tabla, origen o webhook — mediante el mismo código que usa la mutación directa. Se rechaza con `requests.approvals_incomplete` hasta que se cumplen las aprobaciones requeridas de la solicitud (una, o dos para una relación) por usuarios distintos del solicitante; una solicitud de relación también se rechaza con `requests.waiting_on_domains` mientras un dominio que toca no tenga aprobador. Las aprobaciones se dan mediante `POST /admin/creation-requests/{id}/approve`, y la aprobación que completa el recuento realiza la creación, así que esta mutación sirve para reintentar una creación que falló. Una creación fallida deja la solicitud pendiente y responde con el código propio del fallo.

#### `rejectCreationRequest(requestId: Int!, reason: String!) → MutationResult`

Rechaza una solicitud encolada con un motivo accionable. `reason` es obligatorio. Una solicitud de relación puede rechazarla cualquier usuario que pudiera aprobarla. El autor de una solicitud de cualquier otro tipo puede retirarla; eso queda en el registro de administración como una retirada.

---

### Compilación de consultas {: #query-compilation }

#### `compileQuery(input: CompileQueryInput!) → [CompileQueryResult!]!`

Compila una consulta GraphQL del plano de datos contra el esquema de un rol y devuelve la decisión de enrutamiento completa: SQL semántico, SQL de motor, SQL directo, ruta, metadatos de aplicación (filtros de RLS aplicados, columnas excluidas, enmascaramiento aplicado), y Cypher compilado. Devuelve un resultado por cada campo raíz en la consulta.

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

## Tipos de entrada clave {: #key-input-types }

### `SourceInput`

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
| `changeSignal` | `String` | `ttl` \| `probe` \| `ttl_probe` |
| `loadProtected` | `Boolean` | Solo actualización programada |
| `offPeakWindow` | `String` | Ventana de mantenimiento `HH:MM-HH:MM` |
| `offPeakTz` | `String` | Zona horaria IANA |
| `cdc` | `SourceCdcConfigInput` | Configuración del transporte Kafka CDC, incluido cómo se llega a su schema registry (consulte [Autenticación del schema registry](subscriptions.md#schema-registry-authentication)). `schemaRegistryPassword` y `schemaRegistryToken` se devuelven como referencias de la bóveda, nunca como credenciales. |

### `TableInput`

La entrada central de registro de tablas. Campos clave más allá de lo básico:

| Campo | Notas |
|-------|-------|
| `materialize` | Aterriza una copia en el almacén de materialización |
| `mvRefreshInterval` | Segundos entre actualizaciones |
| `mvPersist` | `replace` \| `append` \| `upsert` |
| `mvIncremental` | Mantenimiento incremental |
| `businessKey` | Columnas que identifican la misma cosa entre versiones; se declara con `validTime` y `recordedTime`, las tres o ninguna |
| `validTime` | Columna con la fecha que el negocio da al registro |
| `recordedTime` | Columna con el momento en que se introdujo o modificó esta versión |
| `mvCalendar` | Nombre del calendario de instantáneas |
| `mvGrain` | Grano de instantánea: `daily`, `weekly`, `monthly`, `annual`, o personalizado `3WE` / `LFR` |
| `viewSql` | SQL para una vista derivada |
| `viewMetrics` | Especificación declarativa de vista compuesta por métricas — mutuamente excluyente con `viewSql` |
| `dqContract` | Texto de contrato de calidad de datos en YAML/JSON |
| `queryTemplate` | Cypher para una tabla Neo4j |
| `live` | Configuración de entrega en vivo para push por SSE/Kafka |
| `discover` | Infiere columnas desde el origen en vivo en el momento del registro |

### `RelationshipInput`

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
| `viaTable` | Nombre de la tabla de unión para aristas de muchos a muchos |
| `recordCandidate` | También escribe una fila `accepted` en relationship_candidates |

---

## Ejemplo: registrar una tabla {: #example-registering-a-table }

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

## Ejemplo: crear una regla de RLS {: #example-creating-an-rls-rule }

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
