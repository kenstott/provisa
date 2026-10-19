# Admin API

La Admin API es un endpoint de Strawberry GraphQL en `POST /admin/graphql`. Requiere un rol de superusuario o admin y es independiente del endpoint de GraphQL de datos.

## Autenticación {: #authentication }

Pase sus credenciales en el encabezado `Authorization` usando el proveedor de autenticación estándar de Provisa:

```yaml
Authorization: Bearer <token>
```

El acceso admin se rige por la capacidad `admin` asignada a un rol.

### Tokens de acceso personal {: #personal-access-tokens }

Un token de acceso personal se acepta en cualquier lugar donde se acepte un token bearer, incluido este endpoint. Usted emite y revoca los suyos, porque un token es la credencial propia de quien lo tiene. Por eso vive en su perfil en la UI de administración, junto a salir de una organización y eliminar su cuenta, y no en una página de administración. Un administrador nunca emite un token en nombre de otra persona.

| Ruta | Efecto |
| ------- | -------- |
| `POST /auth/tokens` | Acuña un token para quien llama. Cuerpo: `name` y, opcionalmente, `role_id`, `scopes`, `expires_in_days` (1–366). La respuesta es el único lugar donde el secreto aparece alguna vez |
| `GET /auth/tokens` | Los tokens activos de quien llama en esta organización: prefijo de visualización, nombre, marcas de tiempo del ciclo de vida y el hash que identifica un token para revocarlo. Nunca una credencial utilizable |
| `DELETE /auth/tokens/{token_hash}` | Revoca uno de los tokens de quien llama. 404 cuando no es suyo o ya está revocado |

Omitir `role_id` deja que el token se resuelva al rol que tenga su propietario; nombrar uno estrecha el token por debajo de su propietario. La revocación también ocurre de forma implícita: retirar la pertenencia de un usuario a una organización revoca sus tokens para esa organización. Para la credencial en sí, consulte [Modelo de seguridad](security.md#personal-access-tokens).

## Capacidades {: #capabilities }

### Gestión de Configuración {: #config-management }

Descargue la configuración en ejecución actual:

```http
GET /admin/config
```

Devuelve el `config.yaml` completo como archivo YAML. Suba una nueva configuración:

```http
PUT /admin/config
```

Provisa valida el YAML, recarga los catálogos y regenera los esquemas. No requiere reinicio.

### Configuración en Tiempo de Ejecución {: #runtime-settings }

Lea y escriba la configuración de la plataforma en tiempo de ejecución sin editar el archivo de configuración:

```http
GET  /admin/settings
PUT  /admin/settings
```

La superficie de configuración abarca la redirección de resultados grandes, el muestreo predeterminado y el límite de filas, el TTL de la caché de respuestas, la convención de nomenclatura, el auto-rastreo de FK de relaciones, el DSN del almacén de materialización, la memoria del motor de federación (`jvm_heap_gb`, `query_max_memory`, `query_max_memory_per_node`, `query_max_total_memory`, `fault_tolerant_execution`, `fault_tolerant_task_memory`, `exchange_spool_dir`) y toda la superficie de ajuste del pipeline de trazas de OpenTelemetry. Los límites de recorrido de GraphQL remoto y la configuración de nivel cálido/caché de lectura también se exponen.

La tarjeta **Servidores de conectores** nombra, para cada conector con un servidor incluido (File Crawler, SharePoint, Salesforce, ServiceNow, Cloud Inventory, Splunk, AskAmerica), una carpeta que contiene un paquete de servidor construido para este host (`connectors.<connector>.bundle_dir`, variable de entorno `PROVISA_PGWIRE_BUNDLE_<CONNECTOR>`). Vacía, el servidor del conector se obtiene desde la versión fijada de `kenstott/calcite`. Establecida, la carpeta se usa tal como está: su lanzador debe estar en `bin/pgwire-<connector>`, y una carpeta sin él impide que el servidor del conector arranque con un error que nombra la configuración; la versión nunca se obtiene en su lugar. La configuración se lee cada vez que el servidor de un conector arranca.

La postura de seguridad, `security.mode` (`standard` | `high`), se aplica al reiniciar:

```http
GET  /admin/security
PUT  /admin/security
```

Las asignaciones de modelos de IA, el registro de modelos de embeddings y vectores y el límite de frecuencia de NL surten efecto en la siguiente petición, sin reiniciar:

```http
GET  /admin/ai-models
PUT  /admin/ai-models
```

La pestaña de cifrado del admin deriva su lista de proveedores en vivo desde el registro de cifrado; los proveedores no disponibles aparecen pero no son seleccionables.

`GET`/`HEAD /health` y `GET /setup/status` nunca exigen autenticación. Se saltan el requisito de `Authorization: Bearer` aunque haya un proveedor de autenticación configurado.

### Motor de federación {: #federation-engine }

Lea o cambie qué motor usa el despliegue:

```http
GET  /admin/federation-engine
PUT  /admin/federation-engine
```

`GET` devuelve la clave del motor activo y los campos de configuración que necesita. `PUT` acepta un cuerpo con `engine` (la clave) y cualquier campo específico del motor; la selección se persiste en la configuración de la plataforma y se vincula en el siguiente reinicio del servicio.

### Editor de Relaciones {: #relationship-editor }

Liste las relaciones:

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

Cree una relación. Un id que ya existe se rechaza; una relación se cambia con `updateRelationship`, que toma la misma entrada:

```graphql
mutation {
  createRelationship(input: {
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

Declare una relación respaldada por una tabla de unión:

```graphql
mutation {
  createRelationship(input: {
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

Una tabla asociativa se declara como arista; Provisa nunca la descubre. `viaTable` nombra una tabla registrada. Sus dos columnas clave llevan la arista, y cada columna restante pasa a ser un atributo de la relación, filtrable como cualquier campo. `viaTypeColumn` / `viaTypeValue` dividen una tabla de unión en varios tipos de arista. Por ejemplo, las filas de `pet_companions` cuyo `companion_type` es `bonded pair`, `littermate` o `shares enclosure` dan tres relaciones distintas sobre el mismo par de tablas.

`viaLabelSource` indica de dónde sale el nombre expuesto, y Cypher recibe cada forma en mayúsculas con guiones bajos. `column` usa `viaTypeValue` (`BONDED_PAIR`), `table` usa el nombre propio de la tabla de unión (`PET_COMPANIONS`) y `fixed` usa el `alias` declarado. Una tabla de unión declarada así es una arista. Provisa la quita de las etiquetas de nodo, así que nunca aparece como píldora de nodo en la UI del grafo.

### Descubrimiento de Relaciones con IA {: #ai-relationship-discovery }

Active el análisis de FK impulsado por Claude vía REST:

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

### Introspección de Esquemas {: #schema-introspection }

Explore las tablas publicadas en todos los orígenes:

```graphql
query {
  tables {
    id
    sourceId
    columns {
      columnName
      unmaskedTo
    }
    writeGrants {
      role
      operation
      columns
      rows
      check
      presets { column source name value }
    }
  }
}
```

### Verificación de dependencias de columnas {: #column-dependency-check }

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

Renombrar un alias rompe todo artefacto escrito contra el nombre expuesto: vistas, MVs, expresiones de métricas, predicados RLS y contratos de DQ. Quitar una columna rompe también esos, más los artefactos que guardan el `column_name` físico, que son relaciones, vínculos del glosario y asignaciones de etiquetas. `breaksOn` dice cuál. La página de tablas ejecuta esta comprobación al guardar y muestra el resultado como un diálogo de aviso. [Linaje](lineage.md) explica qué cubre la consulta y qué no.

### Gestión de Vistas {: #view-management }

Registre una vista materializada:

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

Active una actualización manual:

```graphql
mutation {
  refreshMv(mvId: "orders-with-customers") {
    success
  }
}
```

### Registro de Orígenes de Grafo {: #graph-source-registration }

Los orígenes Neo4j y SPARQL se registran mediante endpoints REST (no la Admin API de GraphQL):

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

Una vez registradas, las tablas aparecen en el esquema de GraphQL y son consultables como cualquier otro origen.

### Importación de Hasura / DDN {: #hasura-ddn-import }

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

**La aplicación** carga el YAML que revisó, y editó si quiso, en la organización que actúa. Usa la misma vía de recarga en caliente que `PUT /admin/config`. Cuerpo de la petición: `{"config_yaml": "<yaml string>"}`.

La vista previa nunca almacena en caché el YAML convertido en el servidor; la aplicación toma el YAML que usted proporciona, así que lo que se aplica es exactamente lo que se revisó.

### Intercambio con Apache Ossie {: #apache-ossie-interchange }

Provisa interopera con Apache Ossie (en incubación) como frontera de importación y exportación.

```http
GET  /admin/ossie
POST /admin/ossie/import
```

**La exportación** (`GET /admin/ossie`) construye el documento YAML de Ossie a partir del modelo gobernado vivo en cada petición. No se guarda nada en caché, así que el documento nunca está desfasado. La respuesta es `text/yaml` con una cabecera `Content-Disposition: attachment`. El documento es un modelo con `version`, `name`, `datasets`, `relationships` y `metrics` en su raíz. Las tablas pasan a ser objetos `dataset`, las columnas objetos `field`, y las relaciones se corresponden con objetos `relationship` de Ossie. Un modelo sin tablas se rechaza con un `422`.

**La importación** (`POST /admin/ossie/import`) acepta un documento de Ossie en YAML o JSON y detecta el formato por sí sola. Analiza el documento y devuelve como objeto JSON los registros propuestos de tablas, relaciones y métricas. No se registra nada. Solo lee la forma plana del documento, y un documento que envuelve su modelo en `semantic_model` se rechaza por su nombre. En la UI de administración, la pantalla de revisión le permite aceptar o recortar propuestas antes de que cambie nada.

### Almacenamiento de objetos {: #object-storage }

Lea o configure el almacenamiento de materialización de la organización:

```http
GET  /admin/org-storage
PUT  /admin/org-storage
```

`GET` informa de cuánta asignación de almacenamiento de la plataforma usa la organización. `PUT` registra el DSN de almacenamiento propio de la organización (cifrado en reposo; nunca lo devuelve GET). Una vez establecido, las materializaciones de la organización aterrizan en su propio bucket y dejan de contar contra la asignación de la plataforma. Enviar `storage_url: null` lo borra y devuelve la organización al almacén de la plataforma.

### Cifrado de la organización {: #org-encryption }

Establezca o rote la clave de cifrado en reposo de la organización:

```http
GET  /admin/org-encryption
PUT  /admin/org-encryption
```

`GET` devuelve la huella, el id y la procedencia de la clave, y nunca material de clave. `PUT` fija o rota la clave. Indique `key_b64` (32 bytes en bruto, codificados en base64) para traer su propia clave, u omítalo y Provisa genera una. No hay borrado, porque retirar la última clave dejaría ilegible cada carga que envolvió.

## GraphiQL

La Admin API incluye GraphiQL en `GET /admin/graphql` en el navegador. Úselo para explorar el esquema admin completo de forma interactiva.

## Vistas de gestión del dominio ops {: #ops-domain-management-views }

Cada instalación siembra ocho vistas SQL en el dominio integrado `ops`. Exponen el registro de auditoría de consultas como tablas gobernadas. Usted las consulta por SQL (pgwire), GraphQL y Cypher, con las mismas reglas de acceso al dominio, RLS y enmascaramiento que cualquier tabla de negocio.

`org_admin` queda designado como responsable del dominio ops en el momento de la siembra, de modo que el dominio nunca aparece como una laguna de gobernanza en `stale_metadata`.

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

La granularidad llega hasta la tabla. El registro de auditoría guarda `table_ids`, y no las columnas concretas que leyó una consulta. El texto de las consultas está cifrado y queda fuera de todas estas vistas; solo la vía autorizada de descifrado de administración puede leerlo.

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

Como las vistas son tablas gobernadas, puede escribir las mismas consultas en GraphQL o Cypher.

## Visor de informes {: #reports-viewer }

El visor de informes está en `/admin/reports`. Los roles sin la capacidad `observability` no pueden llegar a él.

El panel izquierdo lista cada tabla registrada del dominio `ops`, ordenada por alias. Las ocho vistas de gestión sembradas aparecen ahí automáticamente. Haga clic en cualquier informe para cargarlo en el visor de datos gobernado de la derecha.

**Añadir un informe propio.** El botón "Add report" abre un diálogo donde indica un nombre, una descripción opcional y una sentencia SELECT. Al guardar, la vista se registra como tabla derivada gobernada en el dominio `ops`. Queda catalogada, con control de acceso y consultable desde cualquier superficie, junto a las vistas sembradas.

**Eliminar.** El icono de papelera solo aparece en los informes propios. Las vistas de gestión sembradas no se pueden eliminar desde esta interfaz.

## Vista previa de tabla {: #table-preview }

Despliegue cualquier fila de tabla en la página Tablas. El botón **Vista previa** abre un modal al 90 % de ancho con los datos gobernados en vivo de la tabla.

Las tablas respaldadas por APIs con parámetros de ruta obligatorios bloquean la vista previa hasta que se suministran esos valores. Un formulario en línea recoge cada parámetro obligatorio antes de que se ejecute la primera consulta; los parámetros de consulta opcionales aparecen en el mismo formulario.

## Visor de datos gobernado {: #governed-data-viewer }

El mismo componente de visor impulsa el modal de vista previa y el visor de informes. Su comportamiento es idéntico en ambos contextos.

**Paginación en el servidor.** Cada página es su propio `SELECT *` gobernado con `LIMIT 101 OFFSET n`. Aparecen 100 filas por página; la número 101 indica si hay más. El conjunto de datos completo nunca se carga en el navegador.

**Filtros y ordenaciones empujados a la fuente.** Cada cabecera de columna tiene un campo de filtro. Un término de filtro se convierte en un predicado `WHERE LOWER(CAST(col AS VARCHAR)) LIKE LOWER('%term%')`, y un clic de ordenación produce una cláusula `ORDER BY`. Ambos se ejecutan en la base de datos. Un filtro sobre una tabla de mil millones de filas recorre la fuente, y su alcance nunca se limita a la página de 100 filas que tiene delante.

**Agrupación de varios niveles.** El icono de capas de cualquier cabecera de columna incorpora esa columna a la agrupación. Las columnas de agrupación encabezan el `ORDER BY`, de modo que los miembros de un grupo caen en la misma página que su cabecera al cruzar los límites de página. Las columnas de clave primaria se añaden al final como desempate estable. Las filas de cabecera de grupo se pueden contraer; contraerlas oculta los miembros sin lanzar una nueva consulta.

**Preferencias persistentes.** Los ajustes de filtro, orden y agrupación se guardan en `localStorage` bajo `provisa.grid.table:<domain>.<table>` y se restauran en la siguiente visita.

**Exportación.** Descargue la página actual como CSV o cópiela al portapapeles como texto separado por tabuladores. La exportación abarca solo la página visible.
