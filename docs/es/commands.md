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

Se admiten cinco valores de `impl_kind` [tool-verified: `_EXECUTORS` dict in
function_dispatch.py:420-426]:

| `impl_kind` | Transporte |
|---|---|
| `source_procedure` | Procedimiento almacenado nativo en un origen registrado |
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
|---|---|
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
subconsultas o proyectados. No está limitado a `SELECT * FROM fn(args)`.

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
