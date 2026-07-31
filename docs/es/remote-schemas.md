# Esquemas remotos

Un origen de esquema remoto conecta una API externa —GraphQL, gRPC o REST (OpenAPI)— a la capa semántica de Provisa. Una vez registrada, las operaciones de la API externa se convierten en tablas y funciones de Provisa de primera clase. (REQ-308, REQ-316, REQ-325) Toda regla de gobierno, interfaz de consulta y capa de seguridad se aplica automáticamente. (REQ-310, REQ-319, REQ-328) El servicio remoto nunca ve las reglas de gobierno de Provisa. (REQ-310, REQ-319, REQ-328)

---

## Tres tipos de origen

### Esquema remoto GraphQL (REQ-307–313)

**Cómo registrar.** Enviar un POST a `/admin/sources/graphql-remote` con la URL del endpoint, un namespace y autenticación opcional. Provisa dispara una consulta de introspección `__schema` estándar contra el endpoint remoto. (REQ-307) [tool-verified: `provisa/graphql_remote/introspect.py:47–59`]

```json
{
  "source_id": "petstore-gql",
  "url": "https://api.example.com/graphql",
  "namespace": "petstore",
  "domain_id": "veterinary",
  "auth": { "type": "bearer", "token": "..." },
  "cache_ttl": 300,
  "field_overrides": { "createPet": "query" },
  "relationships": [
    { "source_table": "petstore__pets", "source_column": "owner_id",
      "target_table": "owners__users", "target_column": "id" }
  ]
}
```

Opciones de autenticación: `none`, `bearer` (encabezado Authorization), `basic` (usuario:contraseña en Base64). (REQ-307) [tool-verified: `provisa/graphql_remote/introspect.py:36–45`]

**Overrides de campo.** `field_overrides` es un mapa `{fieldName: "query" | "mutation"}` que se aplica después de la introspección. Tiene prioridad sobre la clasificación estructural. Solo los campos de tipo query pueden reclasificarse como mutation; los campos de tipo mutation no tienen ruta de override en GraphQL. (REQ-531) [tool-verified: `provisa/graphql_remote/mapper.py`]

**Relaciones al momento del registro.** `relationships` declara rutas de unión FK/PK entre tablas al momento del registro. Se almacenan como relaciones declaradas manualmente (sin el flag `remote_managed`). En cada actualización (refresh), las relaciones detectadas automáticamente (aquellas con `remote_managed: True`) se vuelven a ejecutar y pueden cambiar; las relaciones declaradas manualmente no se modifican. (REQ-554) [tool-verified: `provisa/api/admin/graphql_remote_router.py`]

**Qué se descubre automáticamente.** Todo campo del tipo `Query` remoto que devuelve un OBJECT se convierte en una tabla virtual. Todo campo del tipo `Mutation` remoto se convierte en una función rastreada. (REQ-308) [tool-verified: `provisa/graphql_remote/mapper.py:243–278`]

**Nomenclatura de tablas.** Las tablas se nombran `{namespace}__{field_name}`. Con el namespace `petstore` y un campo de consulta `pets`: el nombre de la tabla es `petstore__pets`. (REQ-312) [tool-verified: `provisa/graphql_remote/mapper.py:250`]

**Mapeo de tipos (REQ-308).** Los campos escalares se mapean directamente a tipos de Provisa. Los campos OBJECT se dividen en dos casos según si el tipo destino está gobernado (ver "Tablas gobernadas" más abajo). [tool-verified: `provisa/graphql_remote/mapper.py:14–36`, `provisa/api/data/endpoint.py:655–671`, `provisa/compiler/schema_gen.py:481–485`]

| Tipo GraphQL | Tipo Provisa |
| --- | --- |
| `String` | `text` |
| `ID` | `text` |
| `Int` | `integer` |
| `Float` | `numeric` |
| `Boolean` | `boolean` |
| OBJECT (tipo inline no gobernado, p. ej. `ContactInfo`) | columna blob `jsonb` |
| OBJECT (tipo destino gobernado) | excluido por completo de la SDL y de la obtención de datos |
| Cualquier ENUM | `jsonb` |
| Escalar personalizado | `text` (valor de reserva) |

**Tablas gobernadas.** Un tipo GQL está gobernado cuando aparece como campo raíz de `Query` en el esquema remoto. `_collect_queryable_types` recopila estos tipos durante el registro, dando preferencia a los campos sin argumentos obligatorios para que puedan obtenerse en bloque como destinos de unión (join). [tool-verified: `provisa/graphql_remote/mapper.py:395–413`]

Cuando una columna de tipo OBJECT en una tabla gobernada apunta a otro tipo gobernado, esa columna queda sujeta a tres reglas simultáneamente [tool-verified: `provisa/api/data/endpoint.py:655–671`, `provisa/compiler/schema_gen.py:481–485`]:

1. **Excluida de la obtención GQL** — el campo no se solicita al obtener las filas de la tabla padre.
2. **Excluida de la SDL** — el campo no aparece en el tipo padre dentro del esquema generado.
3. **Accesible solo mediante una relación declarada** — un steward debe registrar un JOIN entre las dos tablas gobernadas materializadas. Sin esa relación, el campo simplemente está ausente; no hay un blob de reserva.

Los tipos OBJECT que NO son alcanzables como campos raíz de Query (tipos inline como `ContactInfo` o `Address`) siguen reglas distintas: se obtienen como columnas blob `jsonb` y aparecen en la SDL como campos de objeto anidado. Los subcampos son accesibles mediante extracción `-->>` en SQL.

**Argumentos obligatorios.** Cuando un campo raíz de query tiene argumentos non-null sin valor por defecto, estos se convierten en columnas `native_filter_type: query_param` en la tabla (con el prefijo `_nf_` al momento de la inyección). El ejecutor las pasa como variables GraphQL. (REQ-555) [tool-verified: `provisa/graphql_remote/mapper.py:110–120`, `provisa/api/app.py:1280–1303`]

**Relaciones detectadas automáticamente.** Provisa examina las columnas de tipo OBJECT de cada tabla. Cuando el tipo GQL referenciado también está registrado como tabla en el mismo origen, se emite una relación. Las relaciones de muchos a uno infieren las columnas de origen y destino a partir de convenciones de nombres (`breedName` en el tipo de origen → `name` en el tipo destino `Breed`). Los campos uno a muchos (LIST) emiten relaciones con referencias de columna vacías — la clave foránea reside en el lado destino. (REQ-554) [tool-verified: `provisa/graphql_remote/mapper.py:162–202`]

**Mutaciones.** Los campos de mutation producen funciones rastreadas con tipos de argumento mapeados a partir de los argumentos de la mutation y un `return_schema` derivado del tipo de retorno de la mutation. (REQ-308) [tool-verified: `provisa/graphql_remote/mapper.py:261–278`]

**Actualización (refresh).** Enviar un POST a `/admin/sources/graphql-remote/{id}/refresh`. Vuelve a introspeccionar el esquema remoto y actualiza los registros de tablas y funciones. Las reglas de gobierno existentes (RLS, enmascaramiento) se preservan. (REQ-311) [tool-verified: `provisa/api/admin/graphql_remote_router.py:217–257`]

**Limitaciones.**

- Los campos raíz de query de tipo escalar y ENUM (cuando el tipo de retorno no es OBJECT) se convierten en funciones rastreadas, no en tablas virtuales. Su `return_schema` es una única columna `value` del tipo escalar mapeado. [tool-verified: `provisa/graphql_remote/mapper.py:254–279`]
- El anidamiento de objetos se resuelve al momento del registro hasta `graphql_remote.max_object_depth` (por defecto: 5). Tanto la selección de obtención remota como los metadatos de subcampos se construyen hasta esa profundidad; los campos más allá del límite no se obtienen ni están disponibles para extracción SQL. (REQ-556) [tool-verified: `provisa/graphql_remote/mapper.py:38–52`]
- Los campos OBJECT anidados de tipo LIST (p. ej. `breed.awards: [Award]`) se incluyen en la selección de obtención hasta `graphql_remote.max_list_depth` niveles de anidamiento (por defecto: 2). Dentro de ese límite, la lista se obtiene como un array `jsonb` en la columna padre, y la selección GQL inyecta `first: N`, donde N es `graphql_remote.max_list_items` (por defecto: 100), para acotar el tamaño del array. Más allá de `max_list_depth`, el campo LIST se excluye por completo para evitar una expansión de datos sin límite. En SQL, el array se accede mediante `json_array_elements(column_name)` o extracción por índice `->>`. Si el tipo de elemento de la lista tiene su propia query raíz, regístrelo como una tabla separada y cree una relación en su lugar — la ruta de unión es más eficiente y evita el blob. (REQ-556) [tool-verified: `provisa/graphql_remote/mapper.py:43–70`]
- Para consultas SQL, las columnas de tipo OBJECT no gobernadas se obtienen por completo desde el origen remoto (todos los subcampos hasta la profundidad configurada) y se almacenan en caché como `jsonb`. El acceso a subcampos en SQL se maneja mediante extracción `->>` contra el blob; la solicitud remota no se acota únicamente a los campos que selecciona la consulta SQL. Cuando el tipo de elemento de la lista no tiene query raíz y la representación en blob resulta insuficiente, escriba la consulta directamente en SDL de GraphQL — Provisa reproduce fielmente la selección de campos GQL, de modo que el origen remoto ve exactamente los campos solicitados. [tool-verified: `provisa/compiler/sql_gen.py:1332–1368`]
- Si el servidor remoto rechaza un campo de tipo OBJECT porque requiere selección de subcampos (lo cual no debería ocurrir cuando `gql_selection` está disponible), el ejecutor reintenta una vez con esos campos eliminados para que las columnas escalares se sigan devolviendo. [tool-verified: `provisa/graphql_remote/executor.py:76–80`]

---

### Esquema remoto gRPC (REQ-322–329)

**Cómo registrar.** Enviar un POST a `/admin/grpc-remote/register` con la dirección del servidor, una ruta o URL a un archivo `.proto`, y configuración TLS opcional.

```json
{
  "source_id": "orders-grpc",
  "proto_path": "https://api.example.com/orders.proto",
  "server_address": "grpc.example.com:443",
  "namespace": "orders",
  "domain_id": "commerce",
  "tls": true,
  "cache_ttl": 300,
  "method_overrides": { "CreateOrder": "query" },
  "relationships": [
    { "source_table": "orders__OrderService__ListOrders", "source_column": "customer_id",
      "target_table": "customers__CustomerService__GetCustomer", "target_column": "id" }
  ]
}
```

Provisa obtiene el proto, lo analiza con un parser de texto puro (sin dependencias externas de proto al momento del análisis), compila los stubs de Python vía `grpc_tools.protoc`, y abre un `grpc.aio.Channel` persistente. (REQ-322) [tool-verified: `provisa/grpc_remote/loader.py:99–128`, `provisa/grpc_remote/loader.py:166–214`, `provisa/api/admin/grpc_remote_router.py:80–104`]

Los archivos proto también pueden ser rutas locales. Las rutas de importación para tipos bien conocidos (`google/protobuf/timestamp.proto`) se almacenan al momento del registro y se reutilizan en la actualización (refresh). (REQ-329) [tool-verified: `provisa/grpc_remote/loader.py:135–159`]

**Qué se descubre automáticamente.** Todo método `rpc` del proto se clasifica como query o mutation usando tres señales en orden de prioridad: (REQ-323) [tool-verified: `provisa/grpc_remote/mapper.py`]

1. **`method_overrides`** en el payload de registro — `{"MethodName": "query"}` o `{"MethodName": "mutation"}` tiene prioridad sobre todo lo demás.
2. **`server_streaming: true`** — el servidor envía un stream de mensajes; siempre se convierte en tabla virtual (a menos que la salida sea un escalar).
3. **El mensaje de salida tiene un campo repetido de tipo mensaje** — p. ej. `ListOrdersResponse { repeated Order items; }` se trata como un envoltorio de lista (list-wrapper) y se convierte en tabla virtual. Los campos escalares repetidos (p. ej. `repeated string tags`) no activan esta regla — son propiedades de array de una sola entidad, no orígenes de filas.

Los métodos que no coinciden con ninguna de estas señales (RPC unario que devuelve un único mensaje de entidad, o cualquier salida escalar) se convierten en funciones rastreadas.

**Nomenclatura de tablas.** El nombre por defecto es `{namespace}__{ServiceName}__{MethodName}`. Sin namespace, los nombres de servicio y método se unen directamente. A cualquier tabla registrada se le puede asignar un `alias`; cuando se establece, el alias es el nombre usado en todas partes (consultas, SDL, relaciones). El nombre autogenerado es la clave de registro y nunca cambia. (REQ-322) [tool-verified: `provisa/core/repositories/table.py:129–134`]

**Mapeo de tipos (REQ-324).** Los tipos escalares de proto se mapean a tipos SQL de la siguiente manera. [tool-verified: `provisa/grpc_remote/mapper.py:31–47`]

| Tipo Proto | Tipo SQL |
| --- | --- |
| `string`, `bytes` | `text` |
| `int32` / `uint32` / `sint32` / `fixed32` / `sfixed32` | `integer` |
| `int64` / `uint64` / `sint64` / `fixed64` / `sfixed64` | `bigint` |
| `float` | `real` |
| `double` | `numeric` |
| `bool` | `boolean` |
| `repeated <T>` | `jsonb` |
| Mensaje anidado | `jsonb` |
| Enum | `text` |

**Relaciones al momento del registro.** `relationships` funciona igual que en el adaptador GQL — declara rutas de unión FK/PK almacenadas como relaciones declaradas manualmente (sin el flag `remote_managed`). En cada actualización (refresh), estas se preservan sin cambios. (REQ-554) [tool-verified: `provisa/api/admin/grpc_remote_router.py:93–109`]

**Métodos de query (REQ-325).** Los campos del mensaje de salida se convierten en columnas de la tabla. Los campos del mensaje de entrada se convierten a la vez en argumentos GraphQL pasados a la llamada remota *y* se registran como columnas con prefijo `_nf_` y `native_filter_type: "grpc_input"` — el mismo mecanismo que usan GQL y OpenAPI para la inyección de filtros nativos. (REQ-555) [tool-verified: `provisa/api/admin/grpc_remote_router.py:207–213`]

**Subcampos de mensajes anidados.** Para los métodos de query, los campos de tipo mensaje no repetidos en profundidad 0 (columnas de salida directas) tienen sus subcampos resueltos un nivel más y se almacenan como `object_fields` en el `ColumnDef`. Estos metadatos se usan para la extracción de subcampos `jsonb` en SQL y para la documentación del esquema. Los campos anidados más allá de la profundidad 1 no se expanden recursivamente. (REQ-556) [tool-verified: `provisa/grpc_remote/mapper.py:111–128`]

Los métodos de server-streaming recopilan todos los mensajes transmitidos en una lista antes de devolver las filas. (REQ-325) [tool-verified: `provisa/grpc_remote/executor.py:86–119`]

**Métodos de mutation (REQ-326).** Los campos del mensaje de entrada se convierten en argumentos de entrada de la mutation. El esquema del mensaje de salida se convierte en el `return_schema`. [tool-verified: `provisa/grpc_remote/executor.py:122–143`]

**Gestión de canales.** Se almacena un `grpc.aio.Channel` por origen registrado en el estado de la aplicación y se reutiliza entre solicitudes. El canal antiguo se cierra antes de que se abra uno nuevo en la actualización (refresh). (REQ-327) [tool-verified: `provisa/api/admin/grpc_remote_router.py:107–117`]

**Actualización (refresh).** Enviar un POST a `/admin/grpc-remote/refresh/{source_id}`. Vuelve a cargar el proto desde la ruta almacenada, recompila los stubs y vuelve a registrar tablas y funciones. Alternativamente, enviar un PUT a `/admin/grpc-remote/{source_id}/proto` con un nuevo `proto_text` para actualizar el proto en línea. (REQ-329) [tool-verified: `provisa/api/admin/grpc_remote_router.py:241–268`, `provisa/api/admin/grpc_remote_router.py:300–358`]

**Limitaciones.**

- La extracción de subcampos de objeto tiene un nivel de profundidad. Los campos de mensaje anidados más allá de la profundidad 1 no se expanden recursivamente. (REQ-556) [tool-verified: `provisa/grpc_remote/mapper.py:111–128`]

---

### OpenAPI / REST (REQ-314–321)

**Cómo registrar.** Llamar a `auto_register_openapi_source` con un ID de origen, una especificación analizada y metadatos de conexión. La especificación se carga desde un archivo local o una URL. (REQ-314) [tool-verified: `provisa/openapi/loader.py:30–55`, `provisa/openapi/register.py:249–264`]

**Payload de registro.** El endpoint `/admin/openapi/register` acepta dos campos adicionales junto con `source_id`, `spec_path`, etc.:

```json
{
  "operation_overrides": { "createPet": "query", "listOrders": "mutation" },
  "relationships": [
    { "source_table": "pets__listPets", "source_column": "owner_id",
      "target_table": "owners__listOwners", "target_column": "id" }
  ]
}
```

**Qué se descubre automáticamente.** Toda operación GET en la especificación se convierte en tabla virtual, a menos que su esquema de respuesta sea un tipo escalar (`string`, `number`, `boolean`, `integer`) — los GET que devuelven escalares se convierten en funciones rastreadas con una única columna `value`. Toda operación distinta de GET (POST, PUT, PATCH, DELETE) se convierte en función rastreada. (REQ-316, REQ-317)

Prioridad de clasificación: `operation_overrides` (payload) tiene prioridad sobre `x-provisa-kind` (extensión de la especificación), que a su vez tiene prioridad sobre la heurística de GET. `operation_overrides` es la ruta de override recomendada; `x-provisa-kind` es para cuando la propia especificación debe llevar la clasificación. (REQ-408) [tool-verified: `provisa/openapi/mapper.py:192–203`]

**Relaciones al momento del registro.** `relationships` funciona igual que en los demás adaptadores — se almacena como relaciones declaradas manualmente, preservadas en la actualización (refresh). (REQ-554) [tool-verified: `provisa/api/admin/openapi_router.py:103–108`]

**Nomenclatura de tablas.** Las tablas usan el `operationId` de la operación. Si no hay `operationId` definido, Provisa convierte a slug `{method}_{path}`. Se deriva un alias eliminando el segmento verbal inicial y singularizando el sustantivo (`findPetsByStatus` → `pet_by_status`). (REQ-557) [tool-verified: `provisa/openapi/register.py:39–56`]

**Mapeo de tipos.** Los tipos de JSON Schema se mapean a tipos de Provisa de la siguiente manera. [tool-verified: `provisa/openapi/register.py:59–70`]

| Tipo JSON Schema | Tipo Provisa |
| --- | --- |
| `string` | `string` |
| `integer` | `integer` |
| `number` | `number` |
| `boolean` | `boolean` |
| `array` | `jsonb` |
| `object` | `jsonb` |

**Parámetros como columnas de filtro nativo.** Los parámetros de ruta y de query que no son ya campos de respuesta se convierten en columnas con `native_filter_type` establecido en `path_param` o `query_param`, con prefijo `_nf_`. Cuando el nombre de un parámetro coincide con el nombre de un campo de respuesta, los metadatos del parámetro se fusionan en la entrada de columna existente en lugar de crear un duplicado. (REQ-555) [tool-verified: `provisa/openapi/register.py:116–122`, `provisa/openapi/register.py:172–196`]

**Resolución del esquema de respuesta.** El mapper verifica `responses.200`, luego `responses.2xx`, luego `responses.default`. Las respuestas de tipo array se desenvuelven a su esquema de elemento. Las referencias `$ref` se resuelven un nivel de profundidad. (REQ-316) [tool-verified: `provisa/openapi/mapper.py:83–101`]

**Subcampos de objeto.** Las propiedades de respuesta con `type: object` y sus propias `properties` se almacenan como `object_fields` en la columna. Estos subcampos son visibles en la SDL y se usan para la extracción `jsonb` en las consultas. (REQ-556) [tool-verified: `provisa/openapi/register.py:87–96`]

**Caché de respuestas (REQ-318).** Los resultados de las operaciones GET se almacenan en caché en PostgreSQL mediante `pg_cache.py`. Cada combinación de parámetros de solicitud obtiene su propio grupo `_params_hash`. Las filas de un hash determinado se reemplazan cuando expira el TTL. Los endpoints con parámetro de ruta (`/pets/{id}`) omiten la obtención masiva inicial — la tabla de caché se crea vacía para la introspección de esquema, y luego se puebla por clave primaria a medida que llegan las solicitudes. [tool-verified: `provisa/openapi/pg_cache.py:181–234`, `provisa/openapi/pg_cache.py:307–360`]

**Actualización (REQ-321).** Volver a analizar la especificación y llamar de nuevo a `auto_register_openapi_source`. Las reglas de gobierno existentes se preservan; los registros se actualizan mediante upsert ON CONFLICT. [tool-verified: `provisa/openapi/register.py:249–264`]

**Limitaciones.**

- La extracción de subcampos de objeto tiene un nivel de profundidad. Las propiedades anidadas dentro de `object_fields` no se expanden recursivamente. (REQ-556) [tool-verified: `provisa/openapi/register.py:87–96`]
- Los parámetros de encabezado y de cookie se ignoran; solo se registran los parámetros `path` y `query`. (REQ-555) [tool-verified: `provisa/openapi/mapper.py:144–158`]
- La resolución de `$ref` a nivel de especificación tiene un nivel de profundidad para los esquemas de propiedades; las referencias de componentes anidadas en profundidad pueden no resolverse. [tool-verified: `provisa/openapi/mapper.py:51–60`]

---

## Impacto de registrar una tabla remota

Una tabla registrada desde cualquier origen de esquema remoto es una tabla de Provisa de primera clase. Nada en ella se trata de forma diferente a una tabla relacional conectada localmente en tiempo de ejecución. (REQ-308, REQ-313)

**Interfaces de consulta.** La tabla es consultable de inmediato vía GraphQL, SQL (pgwire o directo), Cypher (GQL), JSON:API y Arrow Flight. (REQ-001, REQ-267, REQ-345, REQ-257, REQ-051) La generación de esquema sintetiza `ColumnMetadata` para las tablas remotas, ya que no tienen catálogo — el mapeo de tipos se aplica al momento de construir el esquema. (REQ-602) [tool-verified: `provisa/api/app.py:1367–1386`]

**Modelo de seguridad.** Se aplican las cinco capas de gobierno:

1. Control de acceso por dominio — el `domain_id` de la tabla determina qué roles pueden verla. (REQ-039) [tool-verified: `provisa/compiler/schema_gen.py:1064–1076`]
2. Seguridad de nivel de fila (RLS) — los filtros de fila configurados en la tabla se inyectan en cada consulta, sin importar la interfaz. (REQ-040, REQ-041)
3. Visibilidad de columnas — la lista `visible_to` de cada columna controla la exposición de campos por rol. (REQ-039)
4. Enmascaramiento de columnas — las reglas de enmascaramiento se aplican en la Etapa 2 del pipeline de gobierno. (REQ-040, REQ-263)
5. Guardia de predicados — las columnas enmascaradas se rechazan en las cláusulas WHERE y HAVING. (REQ-603)

Las consultas ad-hoc contra tablas remotas se permiten únicamente bajo los derechos del usuario — el acceso se basa uniformemente en derechos (derechos de tabla/columna + relaciones aprobadas), sin un modo de gobierno por tabla. (REQ-001, REQ-003)

**Gobierno de relaciones (V002).** Las condiciones JOIN contra tablas remotas —cuando se consultan vía SQL o Cypher— deben coincidir con una relación registrada y aprobada. (REQ-604) La verificación V002 se omite para las consultas GraphQL porque las relaciones definidas en la SDL están preaprobadas por diseño. Ver [docs/security.md](security.md#gobierno-de-relaciones-v002).

**Columnas de tipo OBJECT.** Cuando una columna se mapea a un tipo OBJECT de GQL inline no gobernado o a un tipo de objeto de OpenAPI, su tipo Provisa es `jsonb`. La columna almacena el blob JSON anidado completo. Cuando se declaran subcampos (`gql_object_fields` u `object_fields`), el mapa `gql_object_columns` se puebla al momento de construir el esquema. El generador de SQL usa este mapa para emitir expresiones de extracción `->>` para los subcampos cuando una consulta los selecciona. [tool-verified: `provisa/api/app.py:1305–1315`, `provisa/compiler/schema_gen.py:80–82`]

**Argumentos obligatorios como parámetros de filtro nativo.** Los campos raíz de query con argumentos non-null y sin valor por defecto inyectan columnas adicionales en la tabla registrada. Estas columnas llevan `native_filter_type: query_param`. El traductor de Cypher reescribe `WHERE n.id = $val` como `WHERE n._nf_id = $val`, y el ejecutor de GraphQL las recoge como variables para pasar al endpoint remoto. (REQ-555) [tool-verified: `provisa/api/app.py:1280–1303`]

---

## Impacto de crear una relación de cobertura

Cuando un steward registra una relación entre dos tablas remotas (o entre una tabla remota y una tabla local), la relación se convierte en la ruta de unión usada en tiempo de consulta.

**Cómo prevalece la unión.** Al compilar la consulta, Provisa resuelve la ruta de unión a través de la relación registrada. `source_column` y `target_column` de la relación se convierten en la condición de unión en el SQL generado. La unión reemplaza cualquier llamada remota por tabla que de otro modo se necesitaría para el tipo conectado.

**El blob crudo nunca se expone en SQL.** La columna `breed` en `petstore__pets` no es seleccionable como un valor jsonb crudo en consultas SQL. Cuando se registra una relación entre `petstore__pets` y `petstore__breeds`, las consultas SQL recorren la unión — `SELECT breed.name FROM petstore__pets` se resuelve vía la unión FK, no mediante un blob. Cuando no hay una relación registrada pero la columna tiene subcampos declarados (`gql_object_fields`), las referencias a subcampos en SQL se reescriben como extracción `->>` contra el blob almacenado. Esta ruta solo está disponible para tipos inline no gobernados — los campos de destino gobernado se excluyen por completo de la SDL y no tienen blob del cual extraer. El blob crudo en sí nunca se emite como valor de columna simple. [tool-verified: `provisa/compiler/sql_gen.py:1156`, `tests/unit/test_sql_gen.py:TestGqlJsonBlobExtraction`]

En la SDL de GraphQL, un campo OBJECT inline no gobernado se tipa como el tipo de objeto anidado. Que se sirva mediante una unión o mediante extracción de blob en tiempo de ejecución es un detalle de implementación — la forma de la SDL es idéntica en ambos casos. Cuando el tipo hijo está registrado como su propia tabla (y se vuelve gobernado), las cinco capas de gobierno se aplican a él de forma independiente: sus propias reglas RLS, visibilidad de columnas, reglas de enmascaramiento, guardias de predicados y control de acceso por dominio. (REQ-039, REQ-040, REQ-041, REQ-263) La extracción de blob evita esto — los datos del hijo llegan preincrustados en la fila padre y se gobiernan únicamente por las reglas de la tabla padre. Registrar el hijo como tabla y crear una relación es la vía hacia un gobierno de grano fino en el tipo hijo.

**`graphql_alias` en la relación.** El campo `graphql_alias` nombra el campo de la SDL que la relación expone en el tipo padre. Cuando está ausente, el nombre se deriva del `field_name` de la tabla destino y la cardinalidad de la relación vía `rel_field_name(target.field_name, cardinality)`. (REQ-605) [tool-verified: `provisa/compiler/schema_gen.py:1050`]

**V002 en la ruta de unión.** Las consultas SQL y Cypher que recorren la relación están sujetas al gobierno de relaciones V002. La relación debe estar registrada y aprobada para que se permita la unión. (REQ-604) El recorrido de GraphQL vía el campo de relación de la SDL siempre está preaprobado. [tool-verified: `docs/security.md:41–54`]

**Flag remote-managed.** Las relaciones detectadas automáticamente durante el registro de un esquema remoto GraphQL se almacenan con `remote_managed: True`. (REQ-554) [tool-verified: `provisa/graphql_remote/mapper.py:199`] Este es un marcador de metadatos; no altera el comportamiento de gobierno.

---

## Comportamiento de solo definición de tipo

No todos los tipos de un esquema remoto necesitan ser una tabla consultable.

Cuando se establece `root_table_ids` en un `SchemaInput`, las tablas cuyos ID están ausentes de ese conjunto se excluyen de los campos raíz de query en la SDL generada. Siguen presentes como tipos GraphQL y son accesibles mediante campos de relación en tablas que sí tienen entradas raíz. (REQ-601) [tool-verified: `provisa/compiler/schema_gen.py:1062–1069`]

El mismo mecanismo se aplica a las construcciones de esquema filtradas por dominio: las tablas en dominios a los que el rol no puede acceder son solo definiciones de tipo — su definición de tipo existe en la SDL para el recorrido de relaciones, pero no se genera ningún campo raíz de query para ellas. (REQ-039) [tool-verified: `provisa/compiler/schema_gen.py:1068–1076`]

Una tabla de solo definición de tipo:

- No tiene campo raíz de query — los clientes no pueden consultarla directamente por nombre.
- Es accesible mediante campos de relación en tablas que sí tienen entradas raíz.
- Sigue apareciendo en la introspección de esquema como un tipo con nombre.
- Sigue teniendo todas las reglas de gobierno aplicadas cuando se accede a los datos a través de una relación. (REQ-039, REQ-040)

La eliminación completa del esquema —incluida la definición de tipo— solo ocurre cuando el registro de la tabla se elimina por completo. Marcar una tabla como solo definición de tipo (eliminando su ID de `root_table_ids` o filtrando por acceso de dominio) no elimina el tipo.

Este diseño permite a los stewards exponer grafos de objetos navegables donde algunos tipos son alcanzables solo por recorrido, no por consulta independiente.
