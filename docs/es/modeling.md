<!-- markdownlint-disable MD046 -->
<!-- MD046 off: mkdocs-material `===` content-tab bodies are indented, which the linter
     misreads as indented code blocks; the fenced code blocks below are required for rendering. -->

# Modelado de datos (Entities y Facts)

Provisa ofrece dos primitivas declarativas — `entity` y `fact` — que cubren los bloques de
construcción con los que se ensambla todo star schema y Data Vault. Declare la especificación;
Provisa la reduce exactamente a las definiciones de vista materializada, bitemporales y de
relación que de otro modo tendría que escribir a mano (REQ-1164). [tool-verified: modeling.py
module docstring lines 11-28]

## Qué son las entities y los facts

Una **entity** es una proyección con clave, deduplicada y opcionalmente historizada de una
relación de origen. Se le asigna un nombre, se apunta a un origen, se declara la clave de la
entity y los atributos que se desean conservar, y se elige un modo de historial. Provisa escribe
el SQL de la vista y registra la MV. Cuando el historial está habilitado, la MV es bitemporal.
[tool-verified: `Entity` dataclass, modeling.py lines 53-69; `entity_registration` function,
modeling.py lines 105-120]

Un **fact** es una unión (join) a claves de entity, reducida a un grano declarado, con medidas
agregadas. Provisa escribe una consulta de MV agregada (`GROUP BY` de grano + columnas FK) y
registra una relación por cada vínculo de dimensión declarado. Un fact sin medidas es un puro
conjunto de claves — el patrón de link de Data Vault. [tool-verified: `Fact` dataclass,
modeling.py lines 91-102; `fact_registration` function, modeling.py lines 123-141; comment at
line 130 "a measureless fact is a pure key-set (DV link)"]

Ambas construcciones son IR (representación intermedia). Las definiciones generadas se
reorientan entre motores — materializadas en Oracle, Databricks, o dejadas virtuales sobre un
motor MPP — sin necesidad de remodelar. [tool-verified: modeling.py docstring lines 25-28]

## Modos de historial

Hay tres modos disponibles en una entity [tool-verified: `_HISTORY` constant at modeling.py
line 38, `_HISTORY_MODE` dict at modeling.py line 40]:

| Modo | Significado | Modo bitemporal |
| --- | --- | --- |
| `none` | Solo actual. Sin historial. | — |
| `scd2` | Registra cada cambio. Agrega solo las filas modificadas (delta) con clave en la clave de la entity. | `delta` |
| `snapshot` | Registra cada actualización. Agrega el conjunto de resultados completo en cada actualización, con marca de tiempo del sistema. | `snapshot` |

`scd2` necesita una clave de entity para calcular el delta. `snapshot` funciona en cualquier
motor, pero el almacenamiento crece en una copia completa por actualización. Elija `scd2` para
orígenes grandes y de cambio lento; elija `snapshot` cuando necesite historial completo y el
origen no pueda proporcionar una clave.

Los facts no tienen modo de historial — la cobertura temporal proviene del historial de la
entity subyacente.

## Medidas y agregaciones

Las medidas se declaran como pares `column:agg`. Agregaciones admitidas [tool-verified:
`_AGGS` at modeling.py line 41]:

`sum` &nbsp;`avg` &nbsp;`min` &nbsp;`max` &nbsp;`count`

La agregación predeterminada es `sum` [tool-verified: `Measure.agg` default at modeling.py
line 75].

## Ejemplo práctico: entity Customer + fact Sales

### Las tablas de origen

- `raw.customers` — id, name, region, tier
- `raw.orders` — order_id, customer_id, amount, quantity

### Registrar la entity Customer

=== "Admin UI"

    1. Abra **Tables** y haga clic en **+ Model**.
    2. Elija **Entity (dimension)**.
    3. Complete el formulario:
       - **Name:** `Customer`
       - **Source relation:** `raw.customers`
       - **Domain:** *(su dominio)*
       - **Entity key:** `id`
       - **Attributes:** `name, region, tier`
       - **History:** `SCD2 (track changes — delta bitemporal)`
    4. Haga clic en **Create**.

=== "GraphQL API"

    ```graphql
    mutation {
      registerEntity(input: {
        name: "Customer"
        source: "raw.customers"
        domainId: "sales"
        key: ["id"]
        attributes: ["name", "region", "tier"]
        history: "scd2"
      }) {
        success
        message
      }
    }
    ```

Provisa genera y registra esta MV bitemporal [tool-verified: `entity_registration` in
modeling.py lines 105-120]:

```sql
SELECT "id", "name", "region", "tier" FROM "raw"."customers"
-- registered as a bitemporal delta MV, entity key: ["id"]
```

### Registrar el fact Sales

=== "Admin UI"

    1. Haga clic de nuevo en **+ Model**.
    2. Elija **Fact**.
    3. Complete el formulario:
       - **Name:** `Sales`
       - **Source relation:** `raw.orders`
       - **Domain:** *(su dominio)*
       - **Grain:** `order_id`
       - **Measures:** `amount:sum, quantity:sum`
       - **Dimensions:** `Customer:customer_id`
    4. Haga clic en **Create**.

=== "GraphQL API"

    ```graphql
    mutation {
      registerFact(input: {
        name: "Sales"
        source: "raw.orders"
        domainId: "sales"
        grain: ["order_id"]
        measures: [
          { column: "amount", agg: "sum" }
          { column: "quantity", agg: "sum" }
        ]
        dimensions: [
          { entity: "Customer", via: "customer_id" }
        ]
      }) {
        success
        message
      }
    }
    ```

Provisa genera y registra [tool-verified: `fact_registration` in modeling.py lines 123-141]:

```sql
SELECT "order_id", "customer_id",
       SUM("amount") AS "amount",
       SUM("quantity") AS "quantity"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id"
```

Además, se registra una relación: `Sales.customer_id → Customer` (cardinalidad: many-to-one).
[tool-verified: `fact_table_input` in modeling_register.py lines 89-98, cardinality at line 95]

## El formulario Model (admin UI)

El botón **+ Model** aparece en la página **Tables** (tooltip: "Model an entity or fact (star
schema / Data Vault)"). [tool-verified: tablesPage.json line 13; TablesPage.tsx lines 441-450]

Un control segmentado en la parte superior del modal alterna entre **Entity (dimension)** y
**Fact**. [tool-verified: ModelingForm.tsx lines 102-110]

### Campos de Entity

[tool-verified: ModelingForm.tsx lines 141-171; modelingForm.json]

| Campo | Obligatorio | Notas |
| --- | --- | --- |
| Name | sí | El nombre de la MV en el catálogo |
| Source relation | sí | Relación con notación de puntos, p. ej. `raw.customers` |
| Domain | sí | Dominio al que pertenece la MV |
| Entity key | sí | Columna(s) clave separadas por comas, p. ej. `id` |
| Attributes | no | Columnas de atributos separadas por comas, p. ej. `name, region, tier` |
| History | no | `none` / `scd2` / `snapshot`; el valor predeterminado es `none` |

### Campos de Fact

[tool-verified: ModelingForm.tsx lines 172-196; modelingForm.json]

| Campo | Obligatorio | Notas |
| --- | --- | --- |
| Name | sí | El nombre de la MV en el catálogo |
| Source relation | sí | Relación con notación de puntos, p. ej. `raw.orders` |
| Domain | sí | Dominio al que pertenece la MV |
| Grain | sí | Columna(s) de grano separadas por comas, p. ej. `order_id` |
| Measures | no | Pares `col:agg` separados por comas, p. ej. `amount:sum, quantity:sum` |
| Dimensions | no | Pares `Entity:fk_column` separados por comas, p. ej. `Customer:customer_id` |

Cuando se omite `agg` en una medida (`amount` en lugar de `amount:sum`), el valor
predeterminado es `sum`. [tool-verified: ModelingForm.tsx line 73 `agg: agg || "sum"`]

## La API de GraphQL

Ambas mutations residen en el esquema de administración. [tool-verified: schema_mutation.py
lines 449-472]

### `registerEntity`

```graphql
mutation RegisterEntity($input: EntityInput!) {
  registerEntity(input: $input) {
    success
    message
  }
}
```

Campos de `EntityInput` [tool-verified: types.py lines 449-456]:

| Campo | Tipo | Predeterminado | Descripción |
| --- | --- | --- | --- |
| `name` | String | — | Nombre de catálogo para la MV de la entity |
| `source` | String | — | Relación de origen (`schema.table` o entre comillas) |
| `domainId` | String | — | Id del dominio |
| `key` | [String] | — | Columna(s) de clave de la entity |
| `attributes` | [String] | `[]` | Columnas de atributos a proyectar |
| `history` | String | `"none"` | `"none"` \| `"scd2"` \| `"snapshot"` |
| `visibleTo` | [String] | `["public"]` | Lista de visibilidad por rol |

### `registerFact`

```graphql
mutation RegisterFact($input: FactInput!) {
  registerFact(input: $input) {
    success
    message
  }
}
```

Campos de `FactInput` [tool-verified: types.py lines 472-479]:

| Campo | Tipo | Predeterminado | Descripción |
| --- | --- | --- | --- |
| `name` | String | — | Nombre de catálogo para la MV del fact |
| `source` | String | — | Relación de origen |
| `domainId` | String | — | Id del dominio |
| `grain` | [String] | — | Columna(s) de grano para el GROUP BY |
| `measures` | [MeasureInput] | `[]` | Pares `{ column, agg }` |
| `dimensions` | [DimRefInput] | `[]` | Pares `{ entity, via }` |
| `visibleTo` | [String] | `["public"]` | Lista de visibilidad por rol |

`MeasureInput`: `{ column: String, agg: String }` — agg toma `"sum"` por defecto.
[tool-verified: types.py lines 460-462]

`DimRefInput`: `{ entity: String, via: String }` — `entity` es el nombre de la entity
referenciada; `via` es la columna FK en el origen del fact.
[tool-verified: types.py lines 465-468]

Al tener éxito, `registerFact` devuelve un mensaje con esta forma:
`Fact 'Sales' registered with 1 dimension link(s)`.
[tool-verified: schema_mutation.py line 471]

## Star schema de Kimball y Data Vault

Ningún patrón requiere herramientas independientes. Las mismas dos primitivas se componen en
ambos.

### Star schema de Kimball

Este recorrido construye una estrella de tres dimensiones. Se agregan dos tablas de origen
nuevas:

- `raw.products` — `product_id`, `name`, `category`, `list_price` [inferred: introduced for
  this example]
- `raw.date_spine` — `date_key`, `year`, `quarter`, `month` [inferred: introduced for this
  example]

`raw.orders` también incorpora aquí las columnas `product_id` y `order_date`. [inferred]

#### Elegir el tipo de SCD

El modo de historial es el único parámetro que distingue entre SCD Type 1 y Type 2:

| Tipo SCD | Modo de historial | Efecto |
| --- | --- | --- |
| Type 1 (solo actual) | `none` | La MV se reconstruye en cada actualización; sin historial de filas |
| Type 2 (versionado) | `scd2` | MV delta bitemporal; cada cambio agrega una fila nueva con clave en la clave de la entity |

[tool-verified: `_HISTORY_MODE` at modeling.py line 40; `entity_registration` history branch at
lines 115-119]

Use `scd2` cuando las consultas posteriores necesiten unir una dimensión tal como existía en el
momento de la transacción — el tier de un cliente en el momento de la compra, no su tier
actual. Use `none` para búsquedas estables. Un date spine nunca cambia. Un catálogo de
productos donde solo se necesita el precio actual puede reconstruirse en cada actualización.

#### Decisión de grano

El grano es el nivel de detalle más bajo que responde el fact. `order_id` da una fila por
pedido, preservando la capacidad de contar pedidos distintos y unir con cualquier dimensión a
la granularidad de pedido. Un grano más grueso — por ejemplo `["customer_id", "order_date"]` —
preagrega entre pedidos y descarta ese detalle de forma permanente. Declare el grano más
estrecho que necesite el negocio; los agregados más gruesos son económicos de derivar después.

#### Registrar las dimensiones

**Customer** (SCD Type 2 — los cambios de tier deben preservarse):

```graphql
mutation {
  registerEntity(input: {
    name: "Customer"
    source: "raw.customers"
    domainId: "sales"
    key: ["id"]
    attributes: ["name", "region", "tier"]
    history: "scd2"
  }) { success message }
}
```

Genera una MV delta bitemporal con clave en `id` [tool-verified: entity_registration
modeling.py lines 105-120]:

```sql
SELECT "id", "name", "region", "tier" FROM "raw"."customers"
-- bitemporal delta MV, entity key: ["id"]
```

**Product** (SCD Type 1 — catálogo actual, sin necesidad de historial de versiones):

```graphql
mutation {
  registerEntity(input: {
    name: "Product"
    source: "raw.products"
    domainId: "sales"
    key: ["product_id"]
    attributes: ["name", "category", "list_price"]
    history: "none"
  }) { success message }
}
```

Genera una MV ordinaria que se reconstruye en cada actualización [tool-verified:
entity_registration modeling.py lines 105-114; `mv_bitemporal_mode` is only added when
`history != "none"`, line 115]:

```sql
SELECT "product_id", "name", "category", "list_price" FROM "raw"."products"
```

**DateDim** (sin historial — una fecha es inmutable):

```graphql
mutation {
  registerEntity(input: {
    name: "DateDim"
    source: "raw.date_spine"
    domainId: "sales"
    key: ["date_key"]
    attributes: ["year", "quarter", "month"]
    history: "none"
  }) { success message }
}
```

Genera:

```sql
SELECT "date_key", "year", "quarter", "month" FROM "raw"."date_spine"
```

#### Registrar el fact Sales en tres dimensiones

Grano: `order_id`. Tres referencias de dimensión — una columna FK cada una. Ambas medidas son
sumas aditivas.

```graphql
mutation {
  registerFact(input: {
    name: "Sales"
    source: "raw.orders"
    domainId: "sales"
    grain: ["order_id"]
    measures: [
      { column: "amount",   agg: "sum" }
      { column: "quantity", agg: "sum" }
    ]
    dimensions: [
      { entity: "Customer", via: "customer_id" }
      { entity: "Product",  via: "product_id"  }
      { entity: "DateDim",  via: "order_date"  }
    ]
  }) { success message }
}
```

Provisa calcula `group_cols = dedup([grain] + [dim FKs])`
= `["order_id", "customer_id", "product_id", "order_date"]` y genera
[tool-verified: fact_registration modeling.py lines 125-131]:

```sql
SELECT "order_id", "customer_id", "product_id", "order_date",
       SUM("amount")   AS "amount",
       SUM("quantity") AS "quantity"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id", "product_id", "order_date"
```

Se registran automáticamente tres relaciones [tool-verified: modeling_register.py lines 89-98,
cardinality `"many_to_one"` at line 95]:

| Relación | Cardinalidad |
| --- | --- |
| `Sales.customer_id → Customer` | many-to-one |
| `Sales.product_id → Product` | many-to-one |
| `Sales.order_date → DateDim` | many-to-one |

#### Dimensiones conformadas

Una dimensión conformada se registra una vez y se referencia por nombre desde cualquier
cantidad de facts. Suponga que `raw.returns` contiene `return_id`, `customer_id`,
`product_id` y `amount`. El fact Returns reutiliza Customer y Product sin volver a
registrarlos:

```graphql
mutation {
  registerFact(input: {
    name: "Returns"
    source: "raw.returns"
    domainId: "sales"
    grain: ["return_id"]
    measures: [{ column: "amount", agg: "sum" }]
    dimensions: [
      { entity: "Customer", via: "customer_id" }
      { entity: "Product",  via: "product_id"  }
    ]
  }) { success message }
}
```

Tanto `Sales` como `Returns` apuntan a las mismas entities `Customer` y `Product`. Las rutas de
unión de Provisa garantizan que las consultas a través de cualquiera de los dos facts
recorran la misma definición de dimensión [tool-verified: fact_registration uses entity name
as `target_table` at modeling.py lines 138-140; fact_table_input wires `target_table_id` from
that name at modeling_register.py lines 91-93].

---

### Data Vault

Las mismas primitivas se mapean directamente al vocabulario de Data Vault:

| Artefacto DV | Primitiva | Historial |
| --- | --- | --- |
| Hub | `entity` | `none` — solo claves de entity |
| Satellite | `entity` | `scd2` o `snapshot` — historial de atributos junto a la clave del hub |
| Link | `fact` sin medidas | — |
| Bridge / link agregado | `fact` con medidas | — |

El ejemplo construye un vault mínimo sobre `raw.customers` y `raw.orders`.

#### Hubs

Un hub contiene la clave de la entity y nada más. `attributes: []` con `history: "none"`
produce un conjunto de claves actuales deduplicado; el historial de atributos vive por
completo en el satellite.

```graphql
mutation {
  registerEntity(input: {
    name: "CustomerHub"
    source: "raw.customers"
    domainId: "vault"
    key: ["id"]
    attributes: []
    history: "none"
  }) { success message }
}
```

Genera [tool-verified: entity_registration modeling.py lines 107-108;
`cols = dedup([*key, *attributes])` = `["id"]` when `attributes=[]`]:

```sql
SELECT "id" FROM "raw"."customers"
```

```graphql
mutation {
  registerEntity(input: {
    name: "OrderHub"
    source: "raw.orders"
    domainId: "vault"
    key: ["order_id"]
    attributes: []
    history: "none"
  }) { success message }
}
```

Genera:

```sql
SELECT "order_id" FROM "raw"."orders"
```

#### Satellite

El satellite se ubica junto a la clave del hub y transporta el historial completo de
atributos. Use `scd2` para agregar solo las filas modificadas; use `snapshot` para marcar cada
actualización completa.

```graphql
mutation {
  registerEntity(input: {
    name: "CustomerSat"
    source: "raw.customers"
    domainId: "vault"
    key: ["id"]
    attributes: ["name", "region", "tier"]
    history: "scd2"
  }) { success message }
}
```

Genera [tool-verified: entity_registration modeling.py lines 115-119;
`_HISTORY_MODE["scd2"]` = `"delta"` at modeling.py line 40]:

```sql
SELECT "id", "name", "region", "tier" FROM "raw"."customers"
-- bitemporal delta MV, entity key: ["id"]
```

`CustomerSat` y `CustomerHub` tienen ambos clave en `id`. El hub es el destino de unión
estable; el satellite proporciona acceso a atributos en un punto en el tiempo a través de la
capa bitemporal.

#### Link (fact sin medidas)

Un link registra qué claves de hub coocurrieron — solo claves, sin medidas. Provisa omite el
`GROUP BY` cuando `measures` está vacío [tool-verified: modeling.py lines 130-131:
`if f.measures: view_sql += " GROUP BY ..."`].

```graphql
mutation {
  registerFact(input: {
    name: "OrderCustomerLink"
    source: "raw.orders"
    domainId: "vault"
    grain: ["order_id"]
    measures: []
    dimensions: [
      { entity: "CustomerHub", via: "customer_id" }
      { entity: "OrderHub",    via: "order_id"    }
    ]
  }) { success message }
}
```

`group_cols = dedup(["order_id"] + ["customer_id", "order_id"])` = `["order_id",
"customer_id"]`. Sin medidas, por lo que no hay `GROUP BY`. Genera [tool-verified:
fact_registration modeling.py lines 125-131]:

```sql
SELECT "order_id", "customer_id" FROM "raw"."orders"
```

Se registran dos relaciones: `OrderCustomerLink.customer_id → CustomerHub` y
`OrderCustomerLink.order_id → OrderHub`, ambas many-to-one
[tool-verified: modeling_register.py lines 89-98].

#### Bridge / link agregado

Agregue medidas al link y Provisa emite el `GROUP BY`, produciendo un bridge preagregado. Al
grano `order_id` con un cliente por pedido, el resultado es una fila agregada por pedido:

```graphql
mutation {
  registerFact(input: {
    name: "OrderSummary"
    source: "raw.orders"
    domainId: "vault"
    grain: ["order_id"]
    measures: [{ column: "amount", agg: "sum" }]
    dimensions: [
      { entity: "CustomerHub", via: "customer_id" }
      { entity: "OrderHub",    via: "order_id"    }
    ]
  }) { success message }
}
```

`group_cols = dedup(["order_id"] + ["customer_id", "order_id"])` = `["order_id",
"customer_id"]` (el `order_id` duplicado de la lista de dimensiones se descarta mediante
`_dedup`). Genera [tool-verified: fact_registration modeling.py lines 125-131]:

```sql
SELECT "order_id", "customer_id", SUM("amount") AS "amount"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id"
```

El modelo no decide la metodología. El grano, la conformidad, la elección de SCD y la
división hub/satellite siguen siendo decisiones del modelador. Provisa las ejecuta.
[tool-verified: modeling.py docstring lines 25-26]

## Métricas (REQ-1317, REQ-1318, REQ-1320)

Una **métrica** (metric) es una definición agregada, con nombre y gobernada, sin grano propio.
El grano — las dimensiones por las que se desglosa el agregado — lo vincula quien llama en el
momento de la consulta, no en el momento de la definición. Eso es lo que distingue una métrica
de una vista: una vista fija el grano al crearse; una métrica permanece abierta hasta que se
consulta. [tool-verified: `Metric` class comment, `provisa/core/models.py` lines 452–455:
"A named, governed aggregate definition with no grain of its own... grain is bound at
query time by the requested dimension set"]

### El objeto Metric

[tool-verified: `Metric` class, `provisa/core/models.py` lines 451–476]

| Campo | Obligatorio | Notas |
| --- | --- | --- |
| `name` | sí | snake_case, p. ej. `net_revenue`. Validado: `[a-z][a-z0-9_]*` |
| `expression` | sí | ANSI-SQL agregado; debe incluir al menos una función de agregación |
| `datatype` | no | Sugerencia del tipo de resultado, p. ej. `number`, `integer` |
| `description` | no | Definición de negocio legible por humanos |
| `ai_context` | no | Texto para consumidores de IA — se proyecta a herramientas MCP, pg_description, documentación de GraphQL y exportación a Ossie |
| `visible_to` | no | Lista de roles; el valor predeterminado es `["*"]` (todos los roles) |
| `from_fact` | — | Se establece automáticamente cuando la métrica se generó a partir de una medida de un fact |

Las referencias a columnas dentro de la expresión deben estar calificadas con la tabla
(`orders.amount`, no `amount`). Una columna no calificada es un error irrecuperable en el
momento de la expansión, no una advertencia.
[tool-verified: `_expression_tables`, `provisa/compiler/metric_expand.py` lines 83–96]

El repositorio de métricas valida la expresión en cada escritura. Una expresión que no analiza
correctamente o que no contiene ninguna función de agregación se rechaza; nunca se almacena.
[tool-verified: `validate_expression`, `provisa/core/repositories/metric.py` lines 34–43]

Ejemplo de entrada de configuración:

```yaml
metrics:
  - name: net_revenue
    expression: "SUM(orders.amount) - SUM(orders.refunds)"
    datatype: number
    description: "Order revenue after refunds"
    ai_context: "Net revenue: total order amounts minus approved refunds. Use for P&L."
```

### Consultar una métrica

El compilador reserva el esquema `metrics`. [tool-verified: `METRICS_SCHEMA = "metrics"`,
`provisa/compiler/metric_expand.py` line 43] Cada métrica es direccionable como una relación
virtual dentro de ese esquema. Consúltela como una tabla — las columnas que seleccione se
convierten en el conjunto de dimensiones y en el GROUP BY:

```sql
-- Scalar total (no dimension)
SELECT value FROM metrics.net_revenue;

-- Broken out by region and month
SELECT region, month, value FROM metrics.net_revenue GROUP BY region, month;
```

El compilador reescribe esto en un agregado agrupado real sobre las tablas semánticas
subyacentes antes de que se ejecute el gobierno, de modo que RLS y el enmascaramiento se
aplican a las columnas reales.
[tool-verified: `expand_metric_query` docstring, `provisa/compiler/metric_expand.py` lines
263–276: "BEFORE governance, so RLS/masking apply to the real columns (REQ-1317)"]

`SELECT *` contra una relación de métrica se rechaza — indique explícitamente las columnas de
dimensión y `value`. [tool-verified: `expand_metric_query`, `provisa/compiler/metric_expand.py`
lines 302–306]

Cuando la expresión de una métrica abarca varias tablas, el compilador las une a través de
las relaciones registradas. Una dimensión que es una columna de una tabla referenciada
directamente se resuelve a esa tabla. Una dimensión a un salto de relación de distancia se une
automáticamente. Dos saltos o una dimensión ambigua son un error irrecuperable que nombra al
responsable.
[tool-verified: `_JoinPlan.resolve_dimension`, `provisa/compiler/metric_expand.py` lines
190–228]

### Métricas a partir de especificaciones de fact (REQ-1320)

Al registrar un fact, cada medida declarada registra automáticamente un objeto Metric
correspondiente. El campo `from_fact` de la métrica registra el nombre de la tabla de fact de
origen, y las dimensiones de agrupación válidas son los atributos de entity alcanzables a
través de las relaciones FK del fact.
[tool-verified: `Metric.from_fact` comment, `provisa/core/models.py` line 466–467:
"set when this metric was auto-registered from a fact spec's measure";
`from_fact` stored in `provisa/core/repositories/metric.py` line 57]

Las métricas registradas automáticamente aparecen en la página Metrics con una insignia
**fact**. Se pueden editar como cualquier otra métrica. [tool-verified: `MetricsPage.tsx`
lines 405–408: `{m.fromFact && <Badge ... data-testid={`metrics-from-fact-${m.name}`}>...
</Badge>}`]

### Vistas compuestas por métricas (view_metrics, REQ-1318)

Una vista `view_metrics` fija el grano de una métrica en el momento de la definición. Declare
los nombres de las métricas, las columnas de dimensión y los filtros opcionales; el compilador
genera el SELECT.

[tool-verified: `ViewMetricsSpec`, `provisa/core/models.py` lines 479–492]

```yaml
tables:
  - source_id: pg1
    domain_id: sales
    schema: public
    table: monthly_revenue
    view_metrics:
      metrics: [net_revenue]
      dimensions: [region, month]
      filters: ["orders.status = 'completed'"]
```

El compilador genera (para este ejemplo):

```sql
SELECT orders.region AS region, orders.month AS month,
       SUM(orders.amount) - SUM(orders.refunds) AS net_revenue
FROM orders
WHERE orders.status = 'completed'
GROUP BY orders.region, orders.month
```

`view_metrics` y `view_sql` se excluyen mutuamente en la misma tabla.
[tool-verified: `Table` model validator, `provisa/core/models.py` lines 614–617:
`if self.view_sql is not None and self.view_metrics is not None: raise ValueError(...)`]

**Regeneración automática al cambiar una métrica.** Cuando se actualiza la expresión de una
métrica, cada vista `view_metrics` que la referencia se recompila y el SQL nuevo se persiste
de inmediato. La vista no puede desviarse de la definición de la métrica por construcción.
[tool-verified: `regenerate_metric_views`, `provisa/api/admin/_metric_views.py` lines 79–117:
"each dependent view_metrics spec recompiles against the UPDATED metric set and the fresh SQL
is persisted"]

**Llamadas `metric()` en línea dentro de SQL de vista escrito a mano.** El `view_sql` escrito a
mano también puede referenciar métricas mediante `metric('name')`. El compilador reemplaza
cada llamada con la expresión de la métrica y registra un enlace de linaje. Esto le da a las
vistas escritas a mano la misma propiedad de recompilación al cambio cuando referencian una
métrica en lugar de copiar su fórmula.
[tool-verified: `expand_metric_calls_in_sql`, `provisa/compiler/metric_expand.py` lines
393–429]

Nota: las vistas de la ruta de configuración que usan llamadas `metric()` en línea se
regeneran al recargar la configuración, no al hacer upsert de la métrica. [tool-verified:
`regenerate_metric_views` docstring, `_metric_views.py` lines 84–86: "Free-hand view_sql born
from inline metric() calls carries no stored provenance, so it is not regenerated here
(config-path views regenerate on config reload)"]

### La página de administración de Metrics (REQ-1323, REQ-1324)

Abra el elemento de navegación **Metrics** para gestionar métricas gobernadas. Haga clic en
una fila para expandir un panel de detalle de solo lectura; haga clic en **Edit** dentro de
él para pasar a edición en línea (sin modal). **New Metric** abre una tarjeta de creación en
línea sobre la tabla. La confirmación de eliminación es el único modal de la página.
[tool-verified: `MetricsPage.tsx` lines 214–216 comment: "REQ-1317: registered-metrics
management page (list / create / edit / delete). REQ-1323: detail-then-edit"]

El formulario de creación/edición ofrece un constructor de tres selectores para métricas
originadas en facts: elija la tabla de fact de origen (filtrada a `modelingRole=fact`), una
columna de medida y una función de agregación (`SUM`, `AVG`, `COUNT`, `MIN`, `MAX`). El
datatype se deriva automáticamente: `COUNT → bigint`, `AVG → numeric`, `SUM/MIN/MAX → el tipo
de la columna de medida`. El área de texto de expresión sigue siendo la vía de escape para
expresiones arbitrarias.
[tool-verified: `deriveDatatype` function, `MetricsPage.tsx` lines 66–70;
`applyBuilder`, `MetricsPage.tsx` lines 273–285]

## El beneficio de la IR

Cada llamada de registro pasa por la misma ruta que una MV escrita a mano. La especificación
de entity/fact es una representación intermedia — no una plantilla, no una macro. El almacén
al que apunta es una propiedad del despliegue, no del modelo. Cambie el motor de destino y las
mismas declaraciones `entity` / `fact` se materializan allí, porque el SQL generado y los
modos bitemporales son neutrales respecto al motor por construcción. [tool-verified:
modeling.py docstring lines 25-28; modeling_register.py lines 56-66, 80-88]
