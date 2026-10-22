# Comandos {: #commands }

Un comando es una función registrada y gobernada que pone el cómputo externo bajo la gobernanza, la auditoría y el linaje de Provisa. El motor de federación maneja SQL de forma nativa. Un comando cubre el cómputo que SQL no puede expresar, como un microservicio de enriquecimiento, un modelo de Python, un script de shell o un procedimiento almacenado nativo de la base de datos. Lo registra una vez, y cada superficie de cliente (GraphQL, SQL por pgwire, REST, Arrow Flight, gRPC, Bolt/Cypher) puede invocarlo con idéntica gobernanza.

Los comandos son una de las formas de escribir lógica de negocio en Provisa. Vea [La lógica de negocio en Provisa](business-logic.md).

Un comando es un **RPC gobernado**. Sus entradas y salidas están declaradas, tipadas, validadas, trazadas y conectadas al linaje. Un paso de ETL improvisado, una llamada curl sin gobierno o un subproceso no tienen nada de eso.

## Tipos de implementación {: #implementation-kinds }

Se admiten seis valores de `impl_kind`:

| `impl_kind` | Transporte |
| --- | --- |
| `source_procedure` | Procedimiento almacenado nativo en un origen registrado |
| `source_operation` | Una operación de escritura de un origen OpenAPI, GraphQL remoto o gRPC, pasada tal cual (véase [Operación de escritura de un origen remoto](#a-remote-sources-write-operation)) |
| `script` | Subproceso local alimentado con JSON por stdin, lee JSON desde stdout |
| `http` | Endpoint HTTP/S; cuerpo de solicitud JSON, respuesta JSON |
| `grpc` | gRPC unario; puente JSON sin proto |
| `python` | Invocable Python en proceso (`module:attr`) |

El direccionamiento (el `name` del catálogo y `function_name`) está desacoplado del `binding`
(transporte y ubicación). Cambie el binding y el gobierno, el linaje y los contratos del llamador
del comando permanecen sin cambios.

## Tipos de argumento {: #argument-kinds }

Cada argumento declara un `arg_kind`:

| `arg_kind` | Comportamiento |
| --- | --- |
| `column_value` | Escalar; se pasa directamente en la carga de la solicitud |
| `table_ref` | Perezoso; Provisa pasa la referencia de la relación tal cual; el servicio obtiene los datos |
| `result_set` | Eager; Provisa materializa la relación referenciada y envía sus filas |

Los comandos `http` y `grpc` **deben** declarar al menos un argumento `table_ref` o `result_set`.
Un comando externo que solo reciba argumentos escalares se invocaría una vez por fila, lo que
anula el batching. El dispatcher rechaza esta configuración en el momento de la llamada (422).

Un comando que devuelve un conjunto (declarado mediante `output_columns` y `return_schema`) es una
función de valor tabular. Úselo en una cláusula `FROM` o en un `JOIN`.

## El contrato del conjunto de datos {: #the-dataset-contract }

Cada argumento `table_ref` o `result_set` puede declarar un **contrato de columnas de entrada**:
una lista ordenada y tipada por IR de columnas en `FunctionArgument.columns`. El propio comando
declara un **contrato de columnas de salida** en `Function.output_columns`.

Ambos contratos se validan de forma estricta (fail-loud) en cada invocación:

- **Entrada (solo `result_set`):** tras la materialización, Provisa valida las filas contra las
  columnas declaradas. Los campos adicionales, los campos faltantes y los tipos incorrectos
  generan un HTTP 422.

- **Salida:** las filas devueltas por el comando se validan contra `output_columns` antes de
  llegar al llamador.
- **Proyección estrecha:** cuando se declara un contrato de entrada, la consulta de materialización
  proyecta **solo esas columnas** (`SELECT "id", "region" FROM ...`) en lugar de `SELECT *`.

### El vocabulario de tipos IR {: #the-ir-type-vocabulary }

Los tipos de columna de los contratos usan los nombres de tipo IR canónicos:

`smallint` `integer` `bigint` `text` `boolean` `float` `double` `numeric`
`date` `timestamp` `time` `uuid` `bytea` `json`

Los alias comunes se resuelven automáticamente (`varchar` → `text`, `int4` → `integer`,
`jsonb` → `json`, etc.).

`return_schema` es la **proyección GraphQL** de `output_columns`. La fuente de verdad es `output_columns`, así que declárelo para la validación y el linaje, y añada `return_schema` para generar los tipos GraphQL.

## Cómo crear un comando {: #authoring-a-command }

### Archivo de configuración {: #config-file }

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

### UI de administración {: #admin-ui }

El formulario de comandos en **Configuración → Comandos** incluye un editor de columnas de
entrada por conjunto de datos (una fila por columna declarada, con un selector de tipo IR) y un
editor de columnas de salida. Guarde el formulario para registrar o actualizar el comando sin
recargar la configuración.

## Composición inline {: #inline-composition }

Los comandos pueden aparecer **dentro** de una sentencia SQL mayor, donde los une, los usa en subconsultas o los proyecta; `SELECT * FROM fn(args)` es solo la forma más simple. La excepción es la operación de escritura de una fuente remota, porque se invoca sola (vea [Por qué no se puede componer](#why-it-cannot-be-composed)).

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
llamada directa) y reescribe el sitio de la llamada como una relación local tipada.

La sustitución se adapta al tamaño: hasta 1000 filas, el resultado se incorpora inline como una
lista `VALUES` tipada; por encima de ese umbral, se registra como una relación local con nombre en
el motor.

Una instrucción localizada se enruta con normalidad. Las consultas de un solo origen permanecen en
el origen; solo las consultas genuinamente entre orígenes van al motor de federación.

## Operación de escritura de un origen remoto {: #a-remote-sources-write-operation }

Un origen OpenAPI, GraphQL remoto o gRPC ofrece operaciones de escritura. Registrar una como comando la hace invocable, gobernada y auditada desde todas las superficies. Añadir el origen no registra ninguna; usted registra las que desea, una por una, igual que registra tablas. El registro es la curación.

Qué ofrece un origen:

| Tipo de origen | Operaciones ofrecidas | Nombre de la operación |
| --- | --- | --- |
| `openapi` | Toda operación que no sea GET de la especificación | El `operationId` |
| `graphql_remote` | Todo campo del tipo `Mutation` remoto | El nombre del campo |
| `grpc_remote` | Todo método clasificado como mutación | `Service.Method` |

### Registrar una {: #register-one }

1. Abra **Modelo → Comandos** y añada un comando.
2. Elija el origen remoto. El formulario cambia a un selector de operaciones que lista lo que ofrece el origen.
3. Elija la operación, un dominio y los roles que pueden invocarla.
4. Opcionalmente active **Requiere aprobación** y rellene el campo **Escribe en la tabla**.

A través de la API GraphQL de administración, `availableFunctions(sourceId, schemaName)` lista las operaciones. El nombre del esquema es `openapi`, `graphql` o `grpc_remote`, según el tipo de origen.

Todo lo demás se deriva de la operación, envíe lo que envíe el formulario. Provisa sobrescribe estos campos:

```python
body.implKind = "source_operation"
body.kind = "mutation"
body.schemaName = OPERATION_SCHEMA[source_type]
body.returns = ""
body.binding = {}
body.materialize = False
body.arguments = [{"name": a, "type": "json"} for a in operation.arguments]
```

Cada argumento es de tipo `json`. Los argumentos de la operación son sus parámetros de ruta de OpenAPI (más `body` cuando la operación admite un cuerpo de solicitud), sus argumentos de mutación de GraphQL o sus campos de solicitud de gRPC. Una operación que el origen no ofrece se rechaza con 422, `functions.operation_not_offered`.

### Invocación {: #calling-it }

Provisa no da forma, tipo ni valida la entrada. Cada argumento va al servicio remoto sin cambios, con la credencial del origen, y la respuesta del servicio remoto vuelve sin cambios. Provisa gobierna quién puede invocar, en qué dominio, si se necesita aprobación, y registra la llamada.

Cómo recibe el servicio remoto los argumentos:

- **OpenAPI.** Los parámetros de ruta rellenan la plantilla de la ruta. `body` es el cuerpo JSON de la solicitud. Cualquier otro argumento va en la cadena de consulta.
- **GraphQL.** Los argumentos se envían como variables tipadas, cada una declarada con el tipo que le da el esquema remoto. La mutación pide de vuelta los campos escalares y de enumeración de la respuesta, y los de los objetos que contiene hasta dos niveles de profundidad.
- **gRPC.** Los argumentos pasan a ser el mensaje de solicitud de `Service.Method`.

La respuesta son las filas del comando: un objeto es una fila, una lista de objetos son sus filas, cualquier otra cosa es una fila `{"result": ...}`.

En GraphQL el comando es un campo de mutación y su respuesta es el escalar JSON.

```graphql
mutation {
  createIssue(input: {repositoryId: "R_kgDO...", title: "Crash on save"})
}
```

En las superficies SQL (pgwire y las demás que pasan SQL) escriba cada argumento como un literal JSON dentro de una cadena. `'{"title": "x"}'` es un objeto, `'"text"'` una cadena, `'3'` un número. Un literal que no es JSON válido falla con 422, `functions.json_argument_invalid`.

```sql
SELECT * FROM create_issue('{"repositoryId": "R_kgDO...", "title": "Crash on save"}');
```

En REST, envíe por POST un objeto JSON de argumentos a `/data/rest/{domain}/commands/{command}`. Un argumento `json` se documenta en la especificación generada como cualquier valor.

```bash
curl -X POST https://acme.provisa.org/data/rest/engineering/commands/create_issue \
  -H "Content-Type: application/json" \
  -d '{"input": {"repositoryId": "R_kgDO...", "title": "Crash on save"}}'
```

### Rechazos {: #refusals }

El rechazo del servicio remoto vuelve tal como lo formuló.

| Servicio remoto | Provisa responde |
| --- | --- |
| Rechaza la llamada (HTTP 4xx, `errors` de GraphQL, una llamada gRPC rechazada) | 422, `functions.remote_refused`, con `remote_status` y `answer` |
| Falla (HTTP 5xx) | 502, `functions.remote_refused` |

Si la credencial del origen puede ejecutar la operación lo decide el servicio remoto, cuando se invoca la operación. Provisa no puede probar una escritura al registrar sin ejecutarla.

### Aprobación {: #approval }

Active **Requiere aprobación** y cada llamada se somete al hook de aprobación del despliegue antes de ejecutarse. Solo se ejecuta si el hook la aprueba.

- Sin hook configurado: 403, `functions.approval_unavailable`.
- El hook deniega: 403, `functions.approval_denied`, con el motivo del hook.

El hook recibe al invocador, el rol, el nombre del comando y sus argumentos. Véase [Hook de aprobación ABAC](security.md#abac-approval-hook). La marca se almacena como `Function.requires_approval`, y la comprobación se aplica a cualquier comando que la active.

### Escribe en la tabla {: #writes-table }

Indique en el campo **Escribe en la tabla** la tabla en la que escribe la operación, como `schema.table`. Debe ser una tabla registrada del propio origen del comando; de lo contrario, el guardado se rechaza con 422, `actions.written_table_not_registered`. El ajuste es opcional.

Tras cada llamada que el servicio remoto acepta, Provisa la trata como una escritura en esa tabla. Descarta las respuestas en caché de la tabla, marca como obsoletas las vistas materializadas sobre ella, emite el evento de cambio, ejecuta los comandos que corren cuando cambia y recarga la tabla cuando se mantiene en caliente.

Cuando la tabla se lee de su réplica completa (la configuración del operador la pone allí, o el motor no puede leer su origen en su sitio), tras la llamada se pide una construcción de la réplica, con el motivo `write`. Los lectores conservan la réplica antigua hasta que la nueva la sustituye. Una tabla replicada fila a fila, o una con una columna de parámetro, no tiene una réplica completa que reconstruir.

### Por qué no se puede componer {: #why-it-cannot-be-composed }

Una operación de escritura es una acción. No transforma datos, y una vista o vista materializada que contuviera una ejecutaría la escritura cada vez que se leyera o refrescara. Por eso la llamada va sola. `SELECT * FROM create_issue(...)` por sí sola la ejecuta, y la misma llamada dentro de una sentencia mayor se rechaza esté donde esté, ya sea en un join, una subconsulta o una proyección.

Una vista o vista materializada cuya definición invoca una se rechaza al guardarla. Una definición que no se puede analizar también se rechaza mientras haya alguna operación de escritura registrada, ya que no se puede demostrar que no invoca ninguna.

```text
command 'create_issue' writes to its source and is called on its own: it cannot be composed in a query, a view or a materialized view
```

Una operación de fuente tampoco es un nodo de linaje. El linaje se lee del SQL de vistas y consultas, donde un comando aparece como nodo, y ninguna definición guardada puede llamar a una operación de fuente.

## Comandos y linaje {: #commands-and-lineage }

Dado que cada comando declara sus columnas de entrada y salida, el linaje a nivel de columna
**se cierra a través del límite opaco del comando**. El motor de linaje aplica un cierre de
contaminación (taint closure): cada columna de salida declarada se deriva de cada columna de
entrada declarada.

**La consecuencia práctica:** la anchura de su contrato de entrada fija la precisión de ese cierre. Una entrada estrecha, con solo las columnas que el comando necesita, produce un cono de linaje ajustado y legible. Declarar cada columna de la relación de origen abre el abanico sobre cada salida. Sigue siendo correcto, porque no se pierde linaje, pero difumina la trazabilidad.

**Regla general:** pase la proyección mínima que el comando necesita y devuelva solo columnas derivadas. Las entradas devueltas sin cambios ensanchan el cono de contaminación, así que déjelas fuera.

Consulte [Linaje](lineage.md) para saber cómo aparecen los nodos de comando en el DAG y cómo
interpretarlos.

## Lista de permitidos de salida (egress) {: #egress-allowlist }

Los comandos `http` y `grpc` llaman a endpoints externos. Cada host de destino debe figurar en la `udf_egress_allowlist` del despliegue. El loopback (`localhost`, `127.0.0.1`, `::1`) siempre está permitido. Sin lista de permitidos, toda salida externa se deniega con HTTP 403, y no hay ningún valor por defecto silencioso.

## Trazado de invocaciones {: #invocation-tracing }

Cada invocación emite una traza, sea cual sea el resultado. La traza contiene el nombre del comando, el tipo de transporte, el modelo de identidad (DEFINER o INVOKER), las referencias a relaciones de entrada, el id de rol y la cardinalidad de salida. La emite el despachador, así que ningún `impl_kind` puede saltársela.

## CLI: provisa metadata export

`provisa metadata export` es un trabajo del nivel de shell y se ejecuta fuera de la vía de RPC gobernado. Lanza la publicación de metadatos bajo demanda del servidor en ejecución enviando a `/admin/metadata-export/publish`, el endpoint que llama el botón **Publicar ahora** de la pestaña Admin.

Úselo para lanzar exportaciones programadas desde cron o CI cuando la planificación configurada en
`reconcile_cron` no tenga suficiente granularidad:

```bash
provisa metadata export --api https://acme.provisa.org --token "$PROVISA_API_TOKEN"
```

Salida 0 = publicación completa. Salida 1 = publicación parcial o fallo de conexión.

Para la referencia completa de flags, las opciones de autenticación, el nombrado de hosts en
multiinquilino y un ejemplo de cron, consulte [Exportación de metadatos — Desde la línea de comandos](metadata-export.md#from-the-command-line).

Los comandos aparecen en la proyección git de cada entorno. Consulte [Entornos](environments.md) para saber cómo un comando y sus asignaciones de etiquetas sobreviven a un merge y a un pull.
