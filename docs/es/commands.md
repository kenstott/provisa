# Comandos

Un comando es una función registrada y gobernada que trae la computación externa bajo el sistema
de gobierno, auditoría y linaje de Provisa. Donde el motor de federación maneja SQL de forma
nativa, un comando es la costura para la computación que este no puede expresar: un microservicio
de enriquecimiento, un modelo de Python, un script de shell, un procedimiento almacenado nativo de
base de datos. Regístrelo una vez; cada superficie cliente — GraphQL, SQL por pgwire, REST, Arrow
Flight, gRPC, Bolt/Cypher — puede invocarlo con el mismo gobierno (REQ-885, REQ-1156).
[tool-verified: function_dispatch.py module docstring + REQ-885 in requirements.md]

La distinción clave: un comando es un **RPC gobernado**, no un ETL ad hoc. Sus entradas y salidas
están declaradas, tipadas, validadas, trazadas y conectadas al linaje. Una llamada curl no
gobernada o un subproceso no son nada de eso.

## Tipos de implementación

Se admiten seis valores de `impl_kind` [tool-verified: `_EXECUTORS` dict in `provisa/executor/function_dispatch.py`]:

| `impl_kind` | Transporte |
| --- | --- |
| `source_procedure` | Procedimiento almacenado nativo en un origen registrado |
| `source_operation` | Una operación de escritura de un origen OpenAPI, GraphQL remoto o gRPC, pasada tal cual (véase [Operación de escritura de un origen remoto](#operacion-de-escritura-de-un-origen-remoto-req-1924)) |
| `script` | Subproceso local alimentado con JSON por stdin, lee JSON desde stdout |
| `http` | Endpoint HTTP/S; cuerpo de solicitud JSON, respuesta JSON |
| `grpc` | gRPC unario; puente JSON sin proto |
| `python` | Invocable Python en proceso (`module:attr`) |

El direccionamiento (el `name` del catálogo y `function_name`) está desacoplado del `binding`
(transporte y ubicación). Cambie el binding y el gobierno, el linaje y los contratos del llamador
del comando permanecen sin cambios. [tool-verified: Function model in models.py:710-750]

## Tipos de argumento

Cada argumento declara un `arg_kind` [tool-verified: FunctionArgument.arg_kind in
models.py:691-700]:

| `arg_kind` | Comportamiento |
| --- | --- |
| `column_value` | Escalar; se pasa directamente en la carga de la solicitud |
| `table_ref` | Perezoso; Provisa pasa la referencia de la relación tal cual; el servicio obtiene los datos |
| `result_set` | Eager; Provisa materializa la relación referenciada y envía sus filas |

Los comandos `http` y `grpc` **deben** declarar al menos un argumento `table_ref` o `result_set`.
Un comando externo que solo reciba argumentos escalares se invocaría una vez por fila, lo que
anula el batching. El dispatcher rechaza esta configuración en el momento de la llamada (422).
[tool-verified: `_reject_rowwise_external` in function_dispatch.py:322-344]

Un comando que devuelve un conjunto (declarado mediante `output_columns` y `return_schema`) es una
función de valor tabular. Úselo en una cláusula `FROM` o en un `JOIN`. [inferred from
models.py:744-748 and command_localize.py:52-63]

## El contrato del conjunto de datos (REQ-1159)

Cada argumento `table_ref` o `result_set` puede declarar un **contrato de columnas de entrada**:
una lista ordenada y tipada por IR de columnas en `FunctionArgument.columns`. El propio comando
declara un **contrato de columnas de salida** en `Function.output_columns`. [tool-verified:
DatasetColumn model in models.py:675-683, Function.output_columns in models.py:748]

Ambos contratos se validan de forma estricta (fail-loud) en cada invocación:

- **Entrada (solo `result_set`):** tras la materialización, Provisa valida las filas contra las
  columnas declaradas. Los campos adicionales, los campos faltantes y los tipos incorrectos
  generan un HTTP 422. [tool-verified: `_validate_against` called in `_prepare_args` at
  function_dispatch.py:243-248]
- **Salida:** las filas devueltas por el comando se validan contra `output_columns` antes de
  llegar al llamador. [tool-verified: function_dispatch.py:488-490]
- **Proyección estrecha:** cuando se declara un contrato de entrada, la consulta de materialización
  proyecta **solo esas columnas** (`SELECT "id", "region" FROM ...`) en lugar de `SELECT *`.
  [tool-verified: `_materialize_relation` at function_dispatch.py:155-177, col_names passed
  to projection at line 171]

### El vocabulario de tipos IR

Los tipos de columna del contrato usan el sistema canónico de tipos IR (REQ-846), no los
escalares de GraphQL ni las grafías nativas del origen. Los nombres válidos son [tool-verified:
`_IR_TO_SA` keys in ir_types.py:45-63]:

`smallint` `integer` `bigint` `text` `boolean` `float` `double` `numeric`
`date` `timestamp` `time` `uuid` `bytea` `json`

Los alias comunes se resuelven automáticamente (`varchar` → `text`, `int4` → `integer`,
`jsonb` → `json`, etc.). [tool-verified: `_ALIASES` dict in ir_types.py:67-90]

`return_schema` es la **proyección GraphQL** de `output_columns`, no la fuente de verdad. Declare
`output_columns` para la validación y el linaje; agregue `return_schema` para la generación de
tipos GraphQL. [tool-verified: models.py:744-748, comment "return_schema is its GraphQL
projection"]

## Cómo crear un comando

### Archivo de configuración

```yaml
functions:
  - name: enrich_orders
    description: Enrich orders inline — deterministic score + region label
    domain_id: sales-analytics
    kind: query
    impl_kind: python
    source_id: ""
    function_name: enrich_orders
    returns: ""
    binding:
      callable: demo.py_functions:enrich_orders
    arguments:
      - name: input
        type: String
        arg_kind: result_set
        columns:
          - {name: id, type: integer}   # narrow input contract
          - {name: region, type: text}
    visible_to: [admin]
    output_columns:
      - {name: id, type: integer}
      - {name: score, type: double}
      - {name: region_label, type: text}
    return_schema:
      type: array
      items:
        type: object
        properties:
          id: {type: integer}
          score: {type: number}
          region_label: {type: string}
```

[tool-verified: sample_config.yaml enrich_orders block]

La variante gRPC (`enrich_grpc_set`) sigue el mismo patrón pero especifica `impl_kind: grpc` y un
`binding` con las claves `target` y `method` en lugar de `callable`:

```yaml
  - name: enrich_grpc_set
    impl_kind: grpc
    binding:
      target: ${env:DEMO_GRPC_TARGET:-localhost:50071}
      method: /provisa.demo.Enrich/EnrichRows
    arguments:
      - name: input
        type: String
        arg_kind: result_set
        columns:
          - {name: id, type: integer}
          - {name: region, type: text}
    output_columns:
      - {name: id, type: integer}
      - {name: embedding, type: text}
      - {name: geo, type: text}
```

[tool-verified: config/provisa.yaml enrich_grpc_set block]

### UI de administración

El formulario de comandos en **Configuración → Comandos** incluye un editor de columnas de
entrada por conjunto de datos (una fila por columna declarada, con un selector de tipo IR) y un
editor de columnas de salida. Guarde el formulario para registrar o actualizar el comando sin
recargar la configuración. [inferred from CommandFormFields.tsx]

## Composición inline (REQ-1159)

Los comandos pueden aparecer **dentro** de una instrucción SQL más amplia — unidos, en
subconsultas o proyectados. No está limitado a `SELECT * FROM fn(args)`. La excepción es la operación de escritura de un origen remoto, que se invoca por sí sola (véase [Por qué no se puede componer](#por-que-no-se-puede-componer)).

```sql
-- Enrich the orders relation and join the result back inline.
SELECT o.id, o.amount, e.score, e.region_label
FROM   orders o
JOIN   enrich_orders('main.public.orders') e ON o.id = e.id
WHERE  e.score > 0.8;
```

Antes de que se ejecute el gobierno, la validación o el enrutamiento, el pipeline detecta las
llamadas a comandos registrados, ejecuta cada una a través del ejecutor gobernado compartido (de
modo que el contrato de E/S y el modelo de identidad se aplican exactamente igual que en una
llamada directa) y reescribe el sitio de la llamada como una relación local tipada. [tool-verified:
`_localize_inline_commands` in _pipeline.py:145-163 and localize_commands in
command_localize.py:178-222]

La sustitución se adapta al tamaño: hasta 1000 filas, el resultado se incorpora inline como una
lista `VALUES` tipada; por encima de ese umbral, se registra como una relación local con nombre en
el motor. [tool-verified: `_DEFAULT_VALUES_MAX_ROWS = 1000` in command_localize.py:49, path at
lines 211-216]

Una instrucción localizada se enruta con normalidad. Las consultas de un solo origen permanecen en
el origen; solo las consultas genuinamente entre orígenes van al motor de federación.
[tool-verified: _pipeline.py:304 comment "REQ-1159: a localized statement carries an inline local
relation..."]

## Operación de escritura de un origen remoto (REQ-1924)

Un origen OpenAPI, GraphQL remoto o gRPC ofrece operaciones de escritura. Registrar una como comando la hace invocable, gobernada y auditada desde todas las superficies. Añadir el origen no registra ninguna; usted registra las que desea, una por una, igual que registra tablas. El registro es la curación. [tool-verified: `provisa/executor/source_operation.py` module docstring; `provisa/api/admin/schema_common.py` `remote_source_counts`, `"mutations": 0`]

Qué ofrece un origen [tool-verified: `provisa/executor/source_operation.py` `offered_operations`]:

| Tipo de origen | Operaciones ofrecidas | Nombre de la operación |
| --- | --- | --- |
| `openapi` | Toda operación que no sea GET de la especificación | El `operationId` |
| `graphql_remote` | Todo campo del tipo `Mutation` remoto | El nombre del campo |
| `grpc_remote` | Todo método clasificado como mutación | `Service.Method` |

### Registrar una

1. Abra **Modelo → Comandos** y añada un comando.
2. Elija el origen remoto. El formulario cambia a un selector de operaciones que lista lo que ofrece el origen.
3. Elija la operación, un dominio y los roles que pueden invocarla.
4. Opcionalmente active **Requiere aprobación** y rellene el campo **Escribe en la tabla**.

[tool-verified: `provisa-ui/src/components/navGroups.ts` (`/commands` in the Model group); `provisa-ui/src/pages/commands/CommandFormFields.tsx` `isSourceOperation`, `command-requires-approval-switch`, `writesTable`; `provisa/api/admin/schema_query.py` `available_functions` ("Listing them registers none")]

A través de la API GraphQL de administración, `availableFunctions(sourceId, schemaName)` lista las operaciones. El nombre del esquema es `openapi`, `graphql` o `grpc_remote`, según el tipo de origen. [tool-verified: `OPERATION_SCHEMA` in `source_operation.py`; `available_functions` returns `[]` when the schema name does not match the source type]

Todo lo demás se deriva de la operación, no de lo que envía el formulario. `_as_source_operation` sobrescribe estos campos:

```python
body.implKind = "source_operation"
body.kind = "mutation"
body.schemaName = OPERATION_SCHEMA[source_type]
body.returns = ""
body.binding = {}
body.materialize = False
body.arguments = [{"name": a, "type": "json"} for a in operation.arguments]
```

[tool-verified: `provisa/api/admin/actions_router.py` `_as_source_operation`]

Cada argumento es de tipo `json`. Los argumentos de la operación son sus parámetros de ruta de OpenAPI (más `body` cuando la operación admite un cuerpo de solicitud), sus argumentos de mutación de GraphQL o sus campos de solicitud de gRPC. [tool-verified: `_openapi_operations`, `_graphql_operations`, `_grpc_operations` in `source_operation.py`] Una operación que el origen no ofrece se rechaza con 422, `functions.operation_not_offered`. [tool-verified: `offered_operation`]

### Invocación

Provisa no da forma, tipo ni valida la entrada. Cada argumento va al servicio remoto sin cambios, con la credencial del origen, y la respuesta del servicio remoto vuelve sin cambios. Provisa gobierna quién puede invocar, en qué dominio, si se necesita aprobación, y registra la llamada. [tool-verified: `source_operation.py` module docstring]

Cómo recibe el servicio remoto los argumentos:

- **OpenAPI.** Los parámetros de ruta rellenan la plantilla de la ruta. `body` es el cuerpo JSON de la solicitud. Cualquier otro argumento va en la cadena de consulta. [tool-verified: `_call_openapi`]
- **GraphQL.** Los argumentos se envían como variables tipadas, cada una declarada con el tipo que le da el esquema remoto. La mutación pide de vuelta los campos escalares y de enumeración de la respuesta, y los de los objetos que contiene hasta dos niveles de profundidad. [tool-verified: `mutation_document`, `_selection`, `_ANSWER_DEPTH = 2`]
- **gRPC.** Los argumentos pasan a ser el mensaje de solicitud de `Service.Method`. [tool-verified: `_call_grpc`]

La respuesta son las filas del comando: un objeto es una fila, una lista de objetos son sus filas, cualquier otra cosa es una fila `{"result": ...}`. [tool-verified: `_rows`]

En GraphQL el comando es un campo de mutación y su respuesta es el escalar JSON. [tool-verified: `provisa/compiler/actions_schema.py` (`gql_return = JSONScalar` for `source_operation`; `kind` defaults to `"mutation"`)]

```graphql
mutation {
  createIssue(input: {repositoryId: "R_kgDO...", title: "Crash on save"})
}
```

[inferred: argument names are those of the remote operation; the example call was not run]

En las superficies SQL (pgwire y las demás que pasan SQL) escriba cada argumento como un literal JSON dentro de una cadena. `'{"title": "x"}'` es un objeto, `'"text"'` una cadena, `'3'` un número. Un literal que no es JSON válido falla con 422, `functions.json_argument_invalid`. [tool-verified: `_json_arguments_from_sql` in `function_dispatch.py`]

```sql
SELECT * FROM create_issue('{"repositoryId": "R_kgDO...", "title": "Crash on save"}');
```

[inferred: the first-argument shape follows `_json_arguments_from_sql`; the statement was not run, and the command's argument list is the operation's]

En REST, envíe por POST un objeto JSON de argumentos a `/data/rest/{domain}/commands/{command}`. Un argumento `json` se documenta en la especificación generada como cualquier valor. [tool-verified: `provisa/api/rest/openapi_spec.py` `cmd_path = f"/{cmd_domain}/commands/{cmd_name}"`, `_arg_type_to_openapi` (`"json"` returns `{}`)]

```bash
curl -X POST https://acme.provisa.org/data/rest/engineering/commands/create_issue \
  -H "Content-Type: application/json" \
  -d '{"input": {"repositoryId": "R_kgDO...", "title": "Crash on save"}}'
```

[inferred: host, domain and command name are placeholders; not run]

### Rechazos

El rechazo del servicio remoto vuelve tal como lo formuló. [tool-verified: `_refused` in `source_operation.py`]

| Servicio remoto | Provisa responde |
| --- | --- |
| Rechaza la llamada (HTTP 4xx, `errors` de GraphQL, una llamada gRPC rechazada) | 422, `functions.remote_refused`, con `remote_status` y `answer` |
| Falla (HTTP 5xx) | 502, `functions.remote_refused` |

Si la credencial del origen puede ejecutar la operación lo decide el servicio remoto, cuando se invoca la operación. Provisa no puede probar una escritura al registrar sin ejecutarla. [tool-verified: REQ-1924 CREDENTIAL AT CALL amendment in `docs/arch/requirements.yaml`; no credential check in `_as_source_operation`]

### Aprobación

Active **Requiere aprobación** y cada llamada se somete al hook de aprobación del despliegue antes de ejecutarse. Solo se ejecuta si el hook la aprueba. [tool-verified: `provisa/api/data/action_exec.py` `_require_approval`]

- Sin hook configurado: 403, `functions.approval_unavailable`.
- El hook deniega: 403, `functions.approval_denied`, con el motivo del hook.

El hook recibe al invocador, el rol, el nombre del comando y sus argumentos. Véase [Hook de aprobación ABAC](security.md#hook-de-aprobacion-abac). La marca se almacena como `Function.requires_approval`, y la comprobación se aplica a cualquier comando que la active. [tool-verified: `action_exec.py` `if fn.get("requires_approval")`]

### Escribe en la tabla

Indique en el campo **Escribe en la tabla** la tabla en la que escribe la operación, como `schema.table`. Debe ser una tabla registrada del propio origen del comando; de lo contrario, el guardado se rechaza con 422, `actions.written_table_not_registered`. El ajuste es opcional. [tool-verified: `_check_written_table` in `actions_router.py`; `written_table` in `source_operation.py`]

Tras cada llamada que el servicio remoto acepta, Provisa la trata como una escritura en esa tabla. Descarta las respuestas en caché de la tabla, marca como obsoletas las vistas materializadas sobre ella, emite el evento de cambio, ejecuta los sinks de la tabla y recarga la tabla cuando se mantiene en caliente. [tool-verified: `provisa/api/data/table_written.py` `after_table_written`]

La llamada no actualiza las réplicas. Eso espera a una forma de pedir la actualización de una réplica, que no está construida; hasta entonces una réplica se actualiza según su propia programación. [tool-verified: REQ-1924 WRITTEN TABLE amendment; no replica call in `after_table_written`]

### Por qué no se puede componer

Una operación de escritura es una acción, no una transformación de datos. Una vista o vista materializada que contuviera una ejecutaría la escritura cada vez que se leyera o actualizara. Por eso la llamada va sola: `SELECT * FROM create_issue(...)` por sí sola la ejecuta, y la misma llamada dentro de una sentencia mayor se rechaza, dondequiera que esté -- en un join, una subconsulta o una proyección. [tool-verified: `provisa/pgwire/_pipeline.py` `_refuse_composed_mutators`; `provisa/executor/source_operation.py` `writes_called_in`]

Una vista o vista materializada cuya definición invoca una se rechaza al guardarla. Una definición que no se puede analizar también se rechaza mientras haya alguna operación de escritura registrada, ya que no se puede demostrar que no invoca ninguna. [tool-verified: `refuse_writes_in_definition` in `source_operation.py`, called from `provisa/api/admin/_table_ops.py` `_build_columns_for_input` (views) and `provisa/api/admin/schema_common.py` (materialized views)]

```text
command 'create_issue' writes to its source and is called on its own: it cannot be composed in a query, a view or a materialized view (REQ-1924)
```

Una operación de origen tampoco es un nodo de linaje: el linaje se lee del SQL de vistas y consultas, donde un comando aparece como nodo, y ninguna definición guardada puede invocar una operación de origen. [tool-verified: `provisa/lineage/graph.py` (`kind="command"` for a call in the SQL); `refuse_writes_in_definition`]

## Comandos y linaje

Dado que cada comando declara sus columnas de entrada y salida, el linaje a nivel de columna
**se cierra a través del límite opaco del comando**. El motor de linaje aplica un cierre de
contaminación (taint closure): cada columna de salida declarada se deriva de cada columna de
entrada declarada. [tool-verified: `_splice_commands` in graph.py:223-242]

**La consecuencia práctica:** el ancho de su contrato de entrada determina la precisión de ese
cierre. Una entrada estrecha — solo las columnas que el comando realmente necesita — produce un
cono de linaje ajustado y legible. Declarar cada columna de la relación de origen se propaga
ampliamente a través de cada salida, lo cual sigue siendo correcto (no se pierde ningún linaje)
pero difumina la trazabilidad.

**Regla general:** pase la proyección mínima que el comando necesita y devuelva solo columnas
derivadas (no las de entrada repetidas sin cambios). Esto mantiene preciso el cono de
contaminación. [inferred from _splice_commands behavior in graph.py and _materialize_relation
narrow-projection in function_dispatch.py:161]

Consulte [Linaje](lineage.md) para saber cómo aparecen los nodos de comando en el DAG y cómo
interpretarlos.

## Lista de permitidos de salida (egress)

Los comandos `http` y `grpc` llaman a endpoints externos. Cada host de destino debe figurar en el
`udf_egress_allowlist` de la implementación. El loopback (`localhost`, `127.0.0.1`, `::1`) siempre
está permitido. Una lista de permitidos ausente deniega toda salida externa con HTTP 403 — no hay
un valor predeterminado silencioso. [tool-verified: `_check_egress` in function_dispatch.py:292-311]

## Trazado de invocaciones (REQ-886)

Cada invocación emite una traza sin importar el resultado. La traza incluye el nombre del comando,
el tipo de transporte, el modelo de identidad (DEFINER o INVOKER), las referencias de relación de
entrada, el id de rol y la cardinalidad de salida. El dispatcher emite la traza — ningún
`impl_kind` puede omitirla. [tool-verified: `udf_invocation_trace` context in
dispatch_function:475-492]

## CLI: provisa metadata export

`provisa metadata export` es un trabajo de nivel shell, no un RPC gobernado. Dispara la publicación
de metadatos bajo demanda del servidor en ejecución (REQ-1072/REQ-1074) enviando un POST a
`/admin/metadata-export/publish` — el mismo endpoint que invoca el botón **Publicar ahora** de la
pestaña de administración. [tool-verified: `_cmd_metadata_export` in provisa/cli.py:272-310]

Úselo para lanzar exportaciones programadas desde cron o CI cuando la planificación configurada en
`reconcile_cron` no tenga suficiente granularidad:

```bash
provisa metadata export --api https://acme.provisa.org --token "$PROVISA_API_TOKEN"
```

Salida 0 = publicación completa. Salida 1 = publicación parcial o fallo de conexión.

Para la referencia completa de flags, las opciones de autenticación, el nombrado de hosts en
multiinquilino y un ejemplo de cron, consulte [Exportación de metadatos — Desde la línea de comandos](metadata-export.md#from-the-command-line).


Los comandos aparecen en la proyección git de cada entorno. Consulte [Entornos](environments.md) para saber cómo un comando y sus asignaciones de etiquetas sobreviven a un merge y a un pull.
