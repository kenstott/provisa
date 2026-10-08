# Esquemas remotos

Un origen de esquema remoto conecta una API externa —GraphQL (incluido GitHub), gRPC o REST (OpenAPI)— a el modelo de Provisa. Añadir un origen no registra ninguna tabla. El origen ofrece tablas, y un steward registra cada tabla que desea mediante el selector «Register Table»; ese registro es el paso de curación. (REQ-308, REQ-316, REQ-322) Una tabla registrada es una tabla de Provisa de primera clase. (REQ-308, REQ-316, REQ-325) Toda regla de gobierno, interfaz de consulta y capa de seguridad se aplica automáticamente. (REQ-310, REQ-319, REQ-328) El servicio remoto nunca ve las reglas de gobierno de Provisa. (REQ-310, REQ-319, REQ-328)

---

## Tres tipos de origen

### Esquema remoto GraphQL (REQ-307–313)

**Cómo añadir el origen.** Enviar un POST a `/admin/sources/graphql-remote` con la URL del endpoint, un namespace y autenticación opcional. Provisa dispara una consulta de introspección `__schema` estándar contra el endpoint remoto para confirmar el endpoint y la credencial. (REQ-307) [tool-verified: `provisa/graphql_remote/introspect.py:47–59`]

Añadir el origen no registra ninguna tabla ni ningún command. Todo tipo de origen remoto responde al añadir y al actualizar con los mismos contadores: `tables` (tablas registradas o puestas al día; 0 al añadir), `available_tables` (tablas ofrecidas), `mutations` (siempre 0) y `available_mutations` (commands ofrecidos). [tool-verified: `provisa/api/admin/schema_common.py` `remote_source_counts`; `provisa/api/admin/graphql_remote_router.py` `register_graphql_remote_source`]

**Registrar tablas.** Abrir Tables, luego Register Table, elegir el origen y el esquema `graphql`, y seleccionar las tablas y columnas deseadas. Mediante la API GraphQL de administración: `availableTables(sourceId, schemaName)` lista las tablas ofrecidas, `availableColumns` lista las columnas de una tabla y `registerTable(input: TableInput)` registra una con las columnas elegidas. Una tabla registrada queda entonces gobernada. (REQ-308) [tool-verified: `provisa/api/admin/introspect.py` `_native_tables_graphql` (`if schema_name != "graphql": return []`), `provisa/api/admin/_graphql_table_registration.py` `offered_tables`, `offered_columns`, `columns_to_register`] [tool-verified: `provisa/api/admin/schema_query.py` `available_tables`, `available_columns`; `registerTable` is from the task brief, not read]

Cómo se lee una tabla registrada (campo raíz, ruta de filas, argumentos obligatorios, argumentos de paginación) se guarda en `sources.mapping["tables"]`, de modo que un proceso reiniciado la lee sin pedir su esquema al remoto. [tool-verified: `_graphql_table_registration.py` `TABLE_SPECS_KEY = "tables"`, `remember_table`]

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

**Qué ofrece el origen.** Todo campo del tipo `Query` remoto que devuelve un objeto o una lista de objetos se ofrece como tabla, y también cada conexión Relay bajo un campo de objeto único (véase más abajo). Registrar una tabla ofrecida la convierte en tabla. Cada campo del tipo `Mutation` remoto es un comando ofrecido, contado en `available_mutations`; añadir el origen no registra ninguno. Registre los que desee como comandos; véase [Operación de escritura de un origen remoto](commands.md#operacion-de-escritura-de-un-origen-remoto-req-1924). (REQ-308, REQ-1924) [tool-verified: `provisa/graphql_remote/mapper.py:243–278`, `graphql_remote_router.py` `register_graphql_remote_source` (`"functions": 0`)]

**Nomenclatura de tablas.** Las tablas se nombran `{namespace}__{field_name}`. Con el namespace `petstore` y un campo de consulta `pets`: el nombre de la tabla es `petstore__pets`. (REQ-312) [tool-verified: `provisa/graphql_remote/mapper.py:250`]

**Conexiones Relay.** Muchas API devuelven las listas como conexiones Relay: un objeto con `nodes` (o `edges { node }`) junto a `pageInfo`. Provisa asigna una conexión a una tabla de sus nodos y la lee página por página. (REQ-308, REQ-309) [tool-verified: `provisa/graphql_remote/mapper.py` `_is_connection`, `_map_connection_table`]

- Un campo raíz que devuelve una conexión (`securityAdvisories`) se convierte en una tabla de sus nodos.
- Una conexión en el objeto único que devuelve un campo raíz se convierte en su propia tabla. La tabla toma los argumentos obligatorios del campo raíz. Con `repository(owner, name)` y una conexión `issues` en `Repository`, la tabla es `repositoryIssues`, con nombre SQL `gh__repository_issues` bajo el namespace `gh`. Fíltrela mediante las columnas `_nf_owner` y `_nf_name`: `WHERE _nf_owner = 'acme' AND _nf_name = 'widgets'`.
- Una conexión nunca es una columna. De lo contrario, una fila llevaría una lectura que el remoto calcula por fila, por cada conexión que tenga su tipo.
- Una conexión es tabla solo si su campo admite `first` y `after`, de modo que pueda leerse página por página. Una que necesita un argumento propio no es tabla. Tampoco lo es una conexión de una unión, ni cualquier conexión bajo un campo raíz que devuelve una lista.

[tool-verified: `provisa/graphql_remote/mapper.py` `_map_connection_table`, `_map_child_connection_tables`; `tests/unit/test_graphql_remote_relay.py` `test_child_connection_table_takes_the_root_fields_arguments`]

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

**Los campos que necesitan un argumento no son columnas.** Un campo con un argumento obligatorio no puede seleccionarse sin más, por lo que se deja fuera de las columnas de la tabla y de las selecciones anidadas. [tool-verified: `provisa/graphql_remote/mapper.py` `_build_columns`, `_build_gql_field_selection`]

**Argumentos obligatorios.** Cuando un campo raíz de query tiene argumentos non-null sin valor por defecto, estos se convierten en columnas `native_filter_type: query_param` en la tabla (con el prefijo `_nf_` al momento de la inyección). El ejecutor las pasa como variables GraphQL. (REQ-555) [tool-verified: `provisa/graphql_remote/mapper.py:110–120`, `provisa/api/app.py:1280–1303`]

**Relaciones detectadas automáticamente.** Provisa examina las columnas de tipo OBJECT de cada tabla registrada. Cuando el tipo GQL referenciado también es una tabla registrada en el mismo origen, y la columna sobre la que se apoya la relación figura entre las columnas registradas, la relación se almacena. Una tabla aún no registrada no obtiene ninguna. [tool-verified: `_graphql_table_registration.py` `sync_detected_relationships`] Las relaciones many-to-one infieren las columnas de origen y destino a partir de convenciones de nomenclatura (`breedName` en el tipo de origen → `name` en el tipo de destino `Breed`). Los campos one-to-many (LIST) emiten relaciones con referencias de columna vacías: la FK reside en el lado del destino. (REQ-554) [tool-verified: `provisa/graphql_remote/mapper.py:162–202`]

**Mutaciones.** Un campo de mutación se registra, uno por uno, como un comando de tipo `source_operation`. Sus argumentos son cada uno de tipo `json` y se pasan al servicio remoto como variables tipadas; la respuesta es el JSON que devuelve el servicio remoto, sin `return_schema`. Véase [Operación de escritura de un origen remoto](commands.md#operacion-de-escritura-de-un-origen-remoto-req-1924). (REQ-1924) [tool-verified: `provisa/api/admin/actions_router.py` `_as_source_operation` (`body.returns = ""`, arguments typed `json`); `provisa/executor/source_operation.py` `_call_graphql`]

**Actualización (refresh).** Enviar un POST a `/admin/sources/graphql-remote/{id}/refresh`. Vuelve a introspeccionar el esquema remoto y pone al día las tablas ya registradas con él. No añade ninguna tabla ni columna: una tabla o columna que el esquema haya ganado sigue ofrecida, y una columna que el esquema haya perdido se elimina. Las reglas de gobierno existentes (RLS, enmascaramiento) se preservan. (REQ-311) [tool-verified: `provisa/api/admin/graphql_remote_router.py` `refresh_graphql_remote_source`; `_graphql_table_registration.py` `refreshed_registered_tables`: "a column the schema has lost is gone; one it has gained is on offer and is not added"]

**Limitaciones.**

- Los campos raíz de query de tipo escalar y ENUM (cuando el tipo de retorno no es OBJECT) se convierten en funciones rastreadas, no en tablas virtuales. Su `return_schema` es una única columna `value` del tipo escalar mapeado. [tool-verified: `provisa/graphql_remote/mapper.py:254–279`]
- El anidamiento de objetos se resuelve al momento del registro hasta `graphql_remote.max_object_depth` (por defecto: 5). Tanto la selección de la obtención remota como los metadatos de los subcampos se construyen hasta esa profundidad; los campos más allá del límite no se obtienen y no están disponibles para la extracción en SQL. Un tipo se visita una sola vez a lo largo de cada ruta: un campo cuyo tipo ya está en el camino hacia abajo se omite, de modo que un esquema cuyos tipos se refieren entre sí se recorre una vez por tipo, no una vez por nivel de profundidad. (REQ-556) [tool-verified: `provisa/graphql_remote/mapper.py` `_build_gql_field_selection`, `tests/unit/test_graphql_remote_relay.py` `test_a_type_is_entered_once_along_a_path`]
- Los campos OBJECT anidados de tipo LIST (p. ej. `breed.awards: [Award]`) se incluyen en la selección de obtención hasta `graphql_remote.max_list_depth` niveles de anidamiento (por defecto: 2). Dentro de ese límite, la lista se obtiene como un arreglo `jsonb` en la columna padre. Cuando el campo de lista declara un argumento `first` (Relay, PostGraphile, pg_graphql) o un argumento `limit` (Hasura), la selección lo pasa como `first: N` o `limit: N`, donde N es `graphql_remote.max_list_items` (por defecto: 100). Un campo de lista que no declara ninguno de los dos no recibe argumento, porque un remoto rechaza un argumento que el campo no declara. Más allá de `max_list_depth`, el campo LIST se excluye por completo para evitar una expansión ilimitada de datos. En SQL, el arreglo se accede mediante `json_array_elements(column_name)` o extracción por índice con `->>`. Si el tipo de elemento de la lista tiene su propia consulta raíz, regístrelo en su lugar como tabla independiente y cree una relación: la ruta de join es más eficiente y evita el blob. (REQ-556) [tool-verified: `provisa/graphql_remote/mapper.py` `_list_limit_arg`, `_build_gql_field_selection`; `tests/unit/test_graphql_remote_relay.py` `test_a_plain_list_takes_no_first_and_a_list_that_declares_first_gets_it`]
- Para consultas SQL, las columnas de tipo OBJECT no gobernadas se obtienen por completo desde el origen remoto (todos los subcampos hasta la profundidad configurada) y se almacenan en caché como `jsonb`. El acceso a subcampos en SQL se maneja mediante extracción `->>` contra el blob; la solicitud remota no se acota únicamente a los campos que selecciona la consulta SQL. Cuando el tipo de elemento de la lista no tiene query raíz y la representación en blob resulta insuficiente, escriba la consulta directamente en SDL de GraphQL — Provisa reproduce fielmente la selección de campos GQL, de modo que el origen remoto ve exactamente los campos solicitados. [tool-verified: `provisa/compiler/sql_gen.py:1332–1368`]
- Si el servidor remoto rechaza un campo de tipo OBJECT porque requiere selección de subcampos (lo cual no debería ocurrir cuando `gql_selection` está disponible), el ejecutor reintenta una vez con esos campos eliminados para que las columnas escalares se sigan devolviendo. Esto se aplica a las tablas leídas desde un campo raíz. Una tabla de conexión no sigue esta ruta. [tool-verified: `provisa/graphql_remote/executor.py` `execute_remote` (`for attempt in range(2)`), `_execute_connection`]

**Lecturas paginadas.** Una tabla de conexión se lee por cursor. Cada página pide `first: N, after: $pageCursor` con `pageInfo { hasNextPage endCursor }`, y la lectura sigue `endCursor` hasta que el remoto indica que no hay página siguiente. (REQ-309) [tool-verified: `provisa/graphql_remote/executor.py` `_connection_query`, `_execute_connection`]

| Ajuste | Valor por defecto | Efecto |
| --- | --- | --- |
| `graphql_remote.max_list_items` | `100` | Filas por página. [tool-verified: `provisa/api/data/materialization.py` passes `limit=max_items` to `execute_remote`] |
| `graphql_remote.max_rows` | `10000` | El máximo de filas que toma una lectura de una tabla de conexión. Una lectura que lo alcanza se detiene y registra una advertencia. [tool-verified: `provisa/core/models.py` `GraphQLRemoteConfig`] |

```yaml
graphql_remote:
  max_list_items: 100
  max_rows: 10000
```

Dos respuestas hacen que el ejecutor reintente:

- **Página demasiado pesada.** Cuando el remoto responde 502 o 504, se pide de nuevo la misma página a la mitad de tamaño, hasta una fila. [tool-verified: `_PAGE_TOO_HEAVY = (502, 504)`, `page_size = max(1, page_size // 2)`]
- **Límite de tasa con tiempo de espera.** Cuando el remoto responde 403 o 429 con un `Retry-After` de 120 segundos o menos, el ejecutor espera ese tiempo y vuelve a enviar la solicitud, hasta tres intentos. Un rechazo sin `Retry-After`, o que pide una espera más larga, se lanza como error. Esto se aplica a toda lectura, sea de conexión o no. [tool-verified: `_post`, `_RETRY_AFTER_STATUSES`, `_RETRY_AFTER_ATTEMPTS`, `_RETRY_AFTER_MAX_SECONDS`]

Cualquier otro error en la respuesta hace fallar la lectura, salvo que el tipo de origen indique lo contrario (véase GitHub más abajo). Una conexión cuyo padre llegó nulo no tiene filas. [tool-verified: `_accept_row_field_errors`, `_execute_connection`]

---

### GitHub (REQ-1923)

GitHub es un tipo de origen ordinario. Su API es GraphQL, por lo que sus tablas se comportan como se describe arriba, incluidas las tablas de conexión como `gh__repository_issues`. [tool-verified: `provisa/graphql_remote/brands.py` `BRANDS["github"]`]

**Añadir el origen.**

1. Abrir Sources y añadir un origen de tipo **GitHub**.
2. Introducir un token de acceso de GitHub. Opcionalmente, introducir un namespace, el prefijo de los nombres de tabla; el valor por defecto es `gh`.
3. Guardar. Provisa comprueba el token contra GitHub. Un token que GitHub rechaza hace fallar el alta con el mensaje de GitHub.

Añadir el origen no registra ninguna tabla. [tool-verified: `provisa/api/admin/graphql_remote_router.py` `_register_branded_source` (`"tables": 0`, `verify_query="query { viewer { login } }"`)]

**Registrar tablas.** Abrir Tables, luego Register Table. Elegir el origen GitHub, elegir el esquema `graphql` y luego las tablas deseadas. Se lista toda tabla que GitHub ofrece; el registro es su decisión sobre cuáles exponer. [tool-verified: `provisa/api/admin/_graphql_table_registration.py` `offered_tables`] [inferred: picker labels and the `graphql` schema name from the task brief; the UI strings were not read]

**Alcances (scopes) del token.** Al registrar una tabla, Provisa la comprueba una vez contra GitHub con su token.

- Un campo que los alcances del token no cubren se deja fuera de la tabla. El resultado nombra cada campo omitido: `Left out, because the source's credential may not read them: projectsV2`. [tool-verified: `provisa/api/admin/schema_mutation_ops.py`]
- Una tabla que el token no puede leer en absoluto se rechaza, con el motivo de GitHub: `GitHub does not let this source's credential read gh__repository_issues: ...`. [tool-verified: `provisa/api/admin/_table_ops.py` `_branded_columns_for_input`]

**Filas que el token no puede ver.** GitHub responde `FORBIDDEN` para un campo que el token no puede ver en una fila concreta, como los colaboradores de un repositorio sin acceso de escritura, y `NOT_ORG_OWNED_REPO` para un campo que existe solo en repositorios propiedad de una organización. Ese campo es nulo en esa fila, el resto de la lectura continúa y Provisa registra una advertencia. Un error contra la propia tabla hace fallar la lectura. [tool-verified: `provisa/graphql_remote/brands.py` `error_policy`, `provisa/graphql_remote/executor.py` `_accept_row_field_errors`]

**Páginas pesadas.** Cuando GitHub responde `RESOURCE_LIMITS_EXCEEDED` porque calcular una página cuesta demasiado, la página se pide de nuevo a la mitad de tamaño. [tool-verified: `brands.py` `overload`, `executor.py` `_execute_connection`]

**Objetos anidados.** Las tablas de GitHub usan su propia profundidad de anidamiento de 0 (`max_object_depth=0` para este tipo de origen), no `graphql_remote.max_object_depth`. Una columna de objeto anidado se selecciona solo con sus propios campos escalares; los objetos dentro de ella aparecen como `__typename`. [tool-verified: `brands.py`]

**Almacenamiento del token.** El token va a la bóveda de secretos y la fila del origen conserva una referencia, de modo que un reinicio vuelve a leer el origen sin volver a introducir el token. [tool-verified: `provisa/api/admin/graphql_remote_router.py` `_persist_source` docstring: "The credential goes to the org's vault and the row carries the reference"]

**Cómo funciona (operadores).** El esquema de GitHub se distribuye con Provisa, por lo que añadir el origen no hace ninguna llamada de introspección y un esquema grande no cuesta nada en el registro. Las tablas se mapean desde él una a una a medida que se registran. El endpoint de refresh rechaza este tipo de origen; un nuevo esquema de GitHub llega con una versión de Provisa. [tool-verified: `brands.py` module docstring, `brand_schema`; REQ-1923 "there is no refresh" in `docs/arch/requirements.yaml` REQ-1875 supersession note] [tool-verified: refresh handler returns code `graphql_remote.branded_source_not_refreshed`]

---

### GitLab (REQ-1923)

GitLab es un tipo de origen ordinario y se añade y registra igual que GitHub: añadir un origen de tipo **GitLab** con un token de acceso y luego registrar las tablas deseadas del esquema `graphql`. El prefijo por defecto de los nombres de tabla es `gl`. El origen alcanza `gitlab.com`. [tool-verified: `provisa/graphql_remote/brands.py` `BRANDS["gitlab"]`]

**Elegir columnas.** GitLab pone precio a cada consulta y rechaza la que cuesta demasiado: 200 puntos para un llamador anónimo, 250 con token. Una tabla ancha con todas las columnas seleccionadas supera ese precio, así que registre una tabla de GitLab con las columnas que desea. [tool-verified: live against gitlab.com 2026-10-02, `project.issues` with all 63 columns answered "Query has complexity of 1733, which exceeds max complexity of 200"; with 14 chosen columns it registered and read]

- Al registrar una tabla, Provisa pregunta una vez a GitLab si atenderá la selección con el tamaño de página que usan las lecturas. Si GitLab responde que la consulta es demasiado compleja o demasiado grande, la tabla no se registra y el resultado incluye el mensaje de GitLab: `Table 'gl__project_issues' was not registered with the columns selected: Query has complexity of 1733, which exceeds max complexity of 200. Choose fewer columns.` [tool-verified: `provisa/graphql_remote/probe.py` `QueryTooComplex`; `provisa/api/admin/_table_ops.py` code `schema.table_too_complex`]
- Lo que cuesta una columna depende de su tipo. Un valor simple cuesta alrededor de un punto; una columna de objeto anidado cuesta muchas veces más. Descartar columnas de objeto anidado es lo que más libera. [tool-verified: live, five scalar columns scored 26 at 100 rows a page; two small object columns added 18]
- El tamaño de página forma parte del precio. Es `graphql_remote.max_list_items`. [tool-verified: live, the same five columns scored 15 at 5 rows a page and 26 at 100]

**Comprobación del token.** GitLab responde a un token no reconocido con un resultado vacío, no con un error. Provisa lo trata como un token rechazado y no añade el origen. [tool-verified: `provisa/api/admin/graphql_remote_router.py` `_verify_live_auth`]

---

### Esquema remoto gRPC (REQ-322–329)

**Cómo añadir el origen.** Enviar un POST a `/admin/grpc-remote/register` con la dirección del servidor, una ruta o URL a un archivo `.proto` y configuración TLS opcional. Añadir el origen no registra ninguna tabla.

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

**Qué ofrece el origen.** Cada método `rpc` del proto se clasifica como query o mutation mediante tres señales, en orden de prioridad: (REQ-323) [tool-verified: `provisa/grpc_remote/mapper.py`]

1. **`method_overrides`** en el payload de registro — `{"MethodName": "query"}` o `{"MethodName": "mutation"}` tiene prioridad sobre todo lo demás.
2. **`server_streaming: true`** — el servidor envía un stream de mensajes; siempre se convierte en tabla virtual (a menos que la salida sea un escalar).
3. **El mensaje de salida tiene un campo repetido de tipo mensaje** — p. ej. `ListOrdersResponse { repeated Order items; }` se trata como un envoltorio de lista (list-wrapper) y se convierte en tabla virtual. Los campos escalares repetidos (p. ej. `repeated string tags`) no activan esta regla — son propiedades de array de una sola entidad, no orígenes de filas.

Los métodos que no coinciden con ninguna de estas señales (RPC unario que devuelve un único mensaje de entidad, o cualquier salida escalar) se convierten en funciones rastreadas.

**Registrar tablas.** Cada método de query se ofrece como una tabla, con nombre `{namespace}__{Service}__{Method}`, bajo el esquema de selector `grpc_remote`. Registre las que desee con el selector Register Table (`availableTables`, `availableColumns`, `registerTable`, como en los orígenes GraphQL), eligiendo las columnas de respuesta. Los campos de la solicitud se convierten en columnas de filtro nativo `_nf_*`, y estas siempre se incluyen. (REQ-322) [tool-verified: `provisa/api/admin/introspect.py` `_native_tables_grpc` (`if schema_name != "grpc_remote": return []`), `provisa/api/admin/grpc_remote_router.py` `query_table_name`, `query_columns`, `_register_schema` (`if table_name not in registered: continue`), `provisa/api/admin/_table_ops.py` `_grpc_columns_for_input`]

Los métodos de mutación son comandos ofrecidos, contados en `available_mutations`; añadir el origen no registra ninguno. Una mutación gRPC se registra en la página Comandos eligiendo el origen y después el método, llamado `Service.Method`; el tipo del comando es `source_operation`. Véase [Operación de escritura de un origen remoto](commands.md#operacion-de-escritura-de-un-origen-remoto-req-1924). (REQ-1924) [tool-verified: `provisa/executor/source_operation.py` `grpc_operation_name`, `_grpc_operations`; `provisa/api/admin/actions_router.py` `_as_source_operation`]

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

**Métodos de mutación (REQ-326).** Un método de mutación registrado es un comando cuyos argumentos son los campos del mensaje de entrada, cada uno de tipo `json` y pasado sin cambios. La respuesta del servicio remoto vuelve como filas; una llamada rechazada es un 422, `functions.remote_refused`. Véase [Operación de escritura de un origen remoto](commands.md#operacion-de-escritura-de-un-origen-remoto-req-1924). (REQ-1924) [tool-verified: `provisa/executor/source_operation.py` `_grpc_operations`, `_call_grpc`, `_refused`]

**Gestión de canales.** Se almacena un `grpc.aio.Channel` por origen registrado en el estado de la aplicación y se reutiliza entre solicitudes. El canal antiguo se cierra antes de que se abra uno nuevo en la actualización (refresh). (REQ-327) [tool-verified: `provisa/api/admin/grpc_remote_router.py:107–117`]

**Actualización (refresh).** Enviar un POST a `/admin/grpc-remote/refresh/{source_id}`. Vuelve a cargar el proto desde la ruta almacenada, recompila los stubs y pone al día las tablas ya registradas con el proto, con las columnas con las que se registró cada una. No registra ninguna tabla nueva; un método de query añadido al proto sigue ofrecido. Como alternativa, enviar un PUT a `/admin/grpc-remote/{source_id}/proto` con un nuevo `proto_text` para actualizar el proto en línea. (REQ-329) [tool-verified: `provisa/api/admin/grpc_remote_router.py` `refresh_grpc_remote_source`, `_load_and_register` and `put_grpc_proto` (both pass `registered=await registered_query_tables(conn, source_id)`)]

**Limitaciones.**

- La extracción de subcampos de objeto tiene un nivel de profundidad. Los campos de mensaje anidados más allá de la profundidad 1 no se expanden recursivamente. (REQ-556) [tool-verified: `provisa/grpc_remote/mapper.py:111–128`]

---

### OpenAPI / REST (REQ-314–321)

**Cómo añadir el origen.** Enviar un POST a `/admin/openapi/register` con un ID de origen y una especificación, cargada desde un archivo local o una URL. La especificación se analiza y se conserva con el origen; no se registra ninguna tabla ni ningún command. La respuesta informa `tables: 0` y `mutations: 0`, con los contadores ofrecidos en `available_tables` y `available_mutations`. (REQ-314) [tool-verified: `provisa/openapi/loader.py:30–55`, `provisa/api/admin/openapi_router.py` `_load_and_register` docstring: "Tables and functions are NOT auto-registered here. Users register them individually via the Register Table / Register Action UI."]

**Registrar tablas.** Registre cada operación GET deseada mediante el selector Register Table (`availableTables`, `availableColumns`, `registerTable`), eligiendo las columnas. Registre cada operación que no sea GET de forma individual como comando en la página Comandos, listadas por `availableFunctions`; véase [Operación de escritura de un origen remoto](commands.md#operacion-de-escritura-de-un-origen-remoto-req-1924). `PUT /admin/openapi/spec/{source_id}` almacena una especificación editada a mano, no registra nada y devuelve `available_tables` y `available_mutations`. (REQ-316) [tool-verified: `provisa/api/admin/openapi_router.py` `put_openapi_spec`; `provisa/api/admin/schema_query.py` `available_functions` ("returns non-GET operations")] [tool-verified: `provisa/api/admin/_table_ops.py` `_build_columns_for_input`; a registered OpenAPI table is read through the operation in the stored spec, `provisa/api/data/materialization.py` (`state.openapi_specs`)]

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

**Qué ofrece el origen.** Toda operación GET de la especificación se ofrece como tabla, salvo que su esquema de respuesta sea un tipo escalar (`string`, `number`, `boolean`, `integer`) — las operaciones GET que devuelven un escalar son funciones con una sola columna `value`. Toda operación que no sea GET (POST, PUT, PATCH, DELETE) se ofrece como comando, con el nombre de su `operationId`. Una vez registrada, recibe los parámetros de ruta de la operación y un argumento `body` para el cuerpo de la solicitud, cada uno de tipo `json`; cualquier otro argumento va en la cadena de consulta. Véase [Operación de escritura de un origen remoto](commands.md#operacion-de-escritura-de-un-origen-remoto-req-1924). (REQ-316, REQ-317, REQ-1924) [tool-verified: `provisa/executor/source_operation.py` `_openapi_operations`, `_call_openapi`]

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

**Actualización (REQ-321).** Enviar un POST a `/admin/openapi/refresh/{source_id}`. Vuelve a analizar la especificación mediante `_load_and_register`, que no registra nada: no añade ninguna tabla ni columna. Las reglas de gobierno existentes se preservan. [tool-verified: `provisa/api/admin/openapi_router.py` `refresh_openapi_source`, `_load_and_register`] Una tabla registrada conserva sus columnas; se lee mediante la operación de la spec actualizada. [tool-verified: `provisa/api/admin/openapi_router.py` `_load_and_register` (replaces `state.openapi_specs[source_id]`), `provisa/api/data/materialization.py`]

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
