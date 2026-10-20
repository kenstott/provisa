<!-- markdownlint-disable MD046 -->
<!-- MD046 off: mkdocs-material `===` content-tab bodies are indented, which the linter
     misreads as indented code blocks; the fenced code blocks below are required for rendering. -->

# Modelado de datos (Entities y Facts) {: #data-modeling-entities-facts }

Provisa le ofrece dos primitivas declarativas, `entity` y `fact`, que cubren las piezas con las que se arma todo esquema en estrella y todo Data Vault. Declare la especificación, y Provisa la convierte exactamente en las definiciones de vistas materializadas, bitemporales y de relaciones que de otro modo escribiría a mano.

## Qué son las entities y los facts {: #what-entities-and-facts-are }

Una **entity** es una proyección con clave, deduplicada y opcionalmente historizada de una
relación de origen. Se le asigna un nombre, se apunta a un origen, se declara la clave de la
entity y los atributos que se desean conservar, y se elige un modo de historial. Provisa escribe
el SQL de la vista y registra la MV. Cuando el historial está habilitado, la MV es bitemporal.
Declara entonces la clave de la entity como `business_key` junto con las columnas de tiempo válido y tiempo de registro que usted indique.

Un **hecho** es un join a claves de entidad, reducido a un grano declarado, con medidas agregadas. Provisa escribe una consulta de MV agregada (`GROUP BY` grano + columnas FK) y registra una relación por cada enlace de dimensión declarado. Un hecho sin medidas es un conjunto puro de claves, que es el patrón link de Data Vault.

Ambas construcciones son IR. Las definiciones generadas pasan de un motor a otro sin remodelar: materializadas en Oracle o Databricks, o virtuales sobre un motor MPP.

## Modos de historial {: #history-modes }

Hay tres modos disponibles en una entity:

| Modo | Significado |
| --- | --- |
| `none` | Solo el estado actual. Sin historial. |
| `scd2` | Registra cada cambio, con clave en la clave de la entity. |
| `snapshot` | Registra cada actualización, con clave en la clave de la entity. |

`scd2` y `snapshot` conservan cada versión, y Provisa elige cómo guardarlas. Ambos necesitan una columna de tiempo válido y una de tiempo de registro (`validTime`, `recordedTime`): la fecha que su negocio da al registro y cuándo se introdujo o modificó esta versión. [Vistas con historial](configuration.md#views-with-history-time-travel) explica las dos horas.

Los hechos no tienen modo de historial propio; su cobertura en el tiempo viene del historial de las entidades subyacentes.

## Medidas y agregaciones {: #measures-and-aggregations }

Las medidas se declaran como pares `column:agg`. Agregaciones admitidas:

`sum` &nbsp;`avg` &nbsp;`min` &nbsp;`max` &nbsp;`count`

La agregación predeterminada es `sum`.

## Ejemplo práctico: entity Customer + fact Sales {: #worked-example-customer-entity-sales-fact }

### Las tablas de origen {: #the-source-tables }

- `raw.customers` — id, name, region, tier, effective_date, updated_at
- `raw.orders` — order_id, customer_id, amount, quantity

### Registrar la entity Customer {: #register-the-customer-entity }

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
       - **Valid time:** `effective_date`
       - **Recorded time:** `updated_at`
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
        validTime: "effective_date"
        recordedTime: "updated_at"
      }) {
        success
        message
      }
    }
    ```

Provisa genera y registra esta MV bitemporal:

```sql
SELECT "id", "name", "region", "tier", "effective_date", "updated_at" FROM "raw"."customers"
-- registered as a bitemporal MV: business_key ["id"], valid_time effective_date,
-- recorded_time updated_at
```

### Registrar el fact Sales {: #register-the-sales-fact }

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

Provisa genera y registra:

```sql
SELECT "order_id", "customer_id",
       SUM("amount") AS "amount",
       SUM("quantity") AS "quantity"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id"
```

Además, se registra una relación: `Sales.customer_id → Customer` (cardinalidad: many-to-one).

## El formulario Model (admin UI) {: #the-model-form-admin-ui }

El botón **+ Model** aparece en la página **Tables** (tooltip: "Model an entity or fact (star
schema / Data Vault)").

Un control segmentado en la parte superior del modal alterna entre **Entity (dimension)** y
**Fact**.

### Campos de Entity {: #entity-fields }

| Campo | Obligatorio | Notas |
| --- | --- | --- |
| Name | sí | El nombre de la MV en el catálogo |
| Source relation | sí | Relación con notación de puntos, p. ej. `raw.customers` |
| Domain | sí | Dominio al que pertenece la MV |
| Entity key | sí | Columna(s) clave separadas por comas, p. ej. `id` |
| Attributes | no | Columnas de atributos separadas por comas, p. ej. `name, region, tier` |
| History | no | `none` / `scd2` / `snapshot`; el valor predeterminado es `none` |
| Valid time | con historial | La columna con la fecha que su negocio da al registro |
| Recorded time | con historial | La columna con el momento en que se introdujo o modificó esta versión |

### Campos de Fact {: #fact-fields }

| Campo | Obligatorio | Notas |
| --- | --- | --- |
| Name | sí | El nombre de la MV en el catálogo |
| Source relation | sí | Relación con notación de puntos, p. ej. `raw.orders` |
| Domain | sí | Dominio al que pertenece la MV |
| Grain | sí | Columna(s) de grano separadas por comas, p. ej. `order_id` |
| Measures | no | Pares `col:agg` separados por comas, p. ej. `amount:sum, quantity:sum` |
| Dimensions | no | Pares `Entity:fk_column` separados por comas, p. ej. `Customer:customer_id` |

Cuando se omite `agg` en una medida (`amount` en lugar de `amount:sum`), el valor
predeterminado es `sum`.

## La API de GraphQL {: #the-graphql-api }

Ambas mutations residen en el esquema de administración.

### `registerEntity`

```graphql
mutation RegisterEntity($input: EntityInput!) {
  registerEntity(input: $input) {
    success
    message
  }
}
```

Campos de `EntityInput`:

| Campo | Tipo | Predeterminado | Descripción |
| --- | --- | --- | --- |
| `name` | String | — | Nombre de catálogo para la MV de la entity |
| `source` | String | — | Relación de origen (`schema.table` o entre comillas) |
| `domainId` | String | — | Id del dominio |
| `key` | [String] | — | Columna(s) de clave de la entity |
| `attributes` | [String] | `[]` | Columnas de atributos a proyectar |
| `history` | String | `"none"` | `"none"` \| `"scd2"` \| `"snapshot"` |
| `validTime` | String | `null` | Columna de tiempo válido; obligatoria cuando `history` no es `"none"` |
| `recordedTime` | String | `null` | Columna de tiempo de registro; obligatoria cuando `history` no es `"none"` |
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

Campos de `FactInput`:

| Campo | Tipo | Predeterminado | Descripción |
| --- | --- | --- | --- |
| `name` | String | — | Nombre de catálogo para la MV del fact |
| `source` | String | — | Relación de origen |
| `domainId` | String | — | Id del dominio |
| `grain` | [String] | — | Columna(s) de grano para el GROUP BY |
| `measures` | [MeasureInput] | `[]` | Pares `{ column, agg }` |
| `dimensions` | [DimRefInput] | `[]` | Pares `{ entity, via }` |
| `visibleTo` | [String] | `["public"]` | Lista de visibilidad por rol |

`MeasureInput`: `{ column: String, agg: String }`; agg toma `"sum"` por defecto.

`DimRefInput`: `{ entity: String, via: String }`; `entity` es el nombre de la entity referenciada; `via` es la columna FK en el origen del fact.

Al tener éxito, `registerFact` devuelve un mensaje con esta forma:
`Fact 'Sales' registered with 1 dimension link(s)`.

## Star schema de Kimball y Data Vault {: #kimball-star-schema-and-data-vault }

Ningún patrón requiere herramientas independientes. Las mismas dos primitivas se componen en
ambos.

### Star schema de Kimball {: #kimball-star-schema }

Este recorrido construye una estrella de tres dimensiones. Se agregan dos tablas de origen
nuevas:

- `raw.products` — `product_id`, `name`, `category`, `list_price`
- `raw.date_spine` — `date_key`, `year`, `quarter`, `month`

`raw.orders` también incorpora aquí las columnas `product_id` y `order_date`.

#### Elegir el tipo de SCD {: #choosing-scd-type }

El modo de historial es el único parámetro que distingue entre SCD Type 1 y Type 2:

| Tipo SCD | Modo de historial | Efecto |
| --- | --- | --- |
| Type 1 (solo actual) | `none` | La MV se reconstruye en cada actualización; sin historial de filas |
| Type 2 (versionado) | `scd2` | MV bitemporal; cada cambio agrega una versión nueva con clave en la clave de la entity |

Use `scd2` cuando las consultas posteriores necesiten unir una dimensión tal como era en el momento de la transacción, como el nivel de un cliente en el momento de la compra en vez de su nivel actual. Use `none` para tablas de consulta estables. Un eje de fechas nunca cambia, y un catálogo de productos en el que solo importa el precio actual puede reconstruirse en cada actualización.

#### Decisión de grano {: #grain-decision }

El grano es el nivel de detalle más bajo que responde el hecho. `order_id` da una fila por pedido y conserva la posibilidad de contar pedidos distintos y unir con cualquier dimensión a nivel de pedido. Un grano más grueso, por ejemplo `["customer_id", "order_date"]`, agrega por adelantado entre pedidos y pierde ese detalle para siempre. Declare el grano más fino que necesite el negocio; los resúmenes más gruesos son baratos de derivar después.

#### Registrar las dimensiones {: #register-the-dimensions }

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
    validTime: "effective_date"
    recordedTime: "updated_at"
  }) { success message }
}
```

Genera una MV bitemporal con clave en `id`:

```sql
SELECT "id", "name", "region", "tier", "effective_date", "updated_at" FROM "raw"."customers"
-- bitemporal MV: business_key ["id"], valid_time effective_date, recorded_time updated_at
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

Genera una MV ordinaria que se reconstruye en cada actualización:

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

#### Registrar el fact Sales en tres dimensiones {: #register-the-sales-fact-across-three-dimensions }

Grano: `order_id`. Tres referencias a dimensiones, con una columna FK cada una. Ambas medidas son sumas aditivas.

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
:

```sql
SELECT "order_id", "customer_id", "product_id", "order_date",
       SUM("amount")   AS "amount",
       SUM("quantity") AS "quantity"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id", "product_id", "order_date"
```

Se registran automáticamente tres relaciones:

| Relación | Cardinalidad |
| --- | --- |
| `Sales.customer_id → Customer` | many-to-one |
| `Sales.product_id → Product` | many-to-one |
| `Sales.order_date → DateDim` | many-to-one |

#### Dimensiones conformadas {: #conformed-dimensions }

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
recorran la misma definición de dimensión.

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

Genera:

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

El satellite se ubica junto a la clave del hub y transporta el historial completo de atributos. Use `scd2` para registrar cada cambio, o `snapshot` para registrar cada actualización.

```graphql
mutation {
  registerEntity(input: {
    name: "CustomerSat"
    source: "raw.customers"
    domainId: "vault"
    key: ["id"]
    attributes: ["name", "region", "tier"]
    history: "scd2"
    validTime: "effective_date"
    recordedTime: "updated_at"
  }) { success message }
}
```

Genera:

```sql
SELECT "id", "name", "region", "tier", "effective_date", "updated_at" FROM "raw"."customers"
-- bitemporal MV: business_key ["id"], valid_time effective_date, recorded_time updated_at
```

`CustomerSat` y `CustomerHub` tienen ambos clave en `id`. El hub es el destino de unión
estable; el satellite proporciona acceso a atributos en un punto en el tiempo a través de la
capa bitemporal.

#### Link (fact sin medidas) {: #link-measureless-fact }

Un link registra qué claves de hub aparecieron juntas. Solo contiene claves, sin medidas, y Provisa omite el `GROUP BY` cuando `measures` está vacío.

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

`group_cols = dedup(["order_id"] + ["customer_id", "order_id"])` = `["order_id", "customer_id"]`.
Sin medidas, por lo que no hay `GROUP BY`. Genera:

```sql
SELECT "order_id", "customer_id" FROM "raw"."orders"
```

Se registran dos relaciones: `OrderCustomerLink.customer_id → CustomerHub` y
`OrderCustomerLink.order_id → OrderHub`, ambas many-to-one
.

#### Bridge / link agregado {: #bridge-aggregate-link }

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

`group_cols = dedup(["order_id"] + ["customer_id", "order_id"])` = `["order_id", "customer_id"]`
(el `order_id` duplicado de la lista de dimensiones se descarta mediante
`_dedup`). Genera:

```sql
SELECT "order_id", "customer_id", SUM("amount") AS "amount"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id"
```

El modelo no decide la metodología. El grano, la conformidad, la elección de SCD y la
división hub/satellite siguen siendo decisiones del modelador. Provisa las ejecuta.

## Métricas {: #metrics }

Una **métrica** es una definición de agregado con nombre y gobernada, sin grano propio. El grano, es decir, las dimensiones por las que se desglosa el agregado, lo fija quien consulta en el momento de la consulta y no al definirla. Eso es lo que distingue una métrica de una vista: una vista fija el grano al crearse, y una métrica queda abierta hasta que se consulta.

### El objeto Metric {: #the-metric-object }

| Campo | Obligatorio | Notas |
| --- | --- | --- |
| `name` | sí | snake_case, p. ej. `net_revenue`. Validado: `[a-z][a-z0-9_]*` |
| `expression` | sí | ANSI-SQL agregado; debe incluir al menos una función de agregación |
| `datatype` | no | Sugerencia del tipo de resultado, p. ej. `number`, `integer` |
| `description` | no | Definición de negocio legible por humanos |
| `ai_context` | no | Texto para consumidores de IA — se proyecta a herramientas MCP, pg_description, documentación de GraphQL y exportación a Ossie |
| `visible_to` | no | Lista de roles; el valor predeterminado es `["*"]` (todos los roles) |
| `from_fact` | — | Se establece automáticamente cuando la métrica se generó a partir de una medida de un fact |

Las referencias a columnas dentro de la expresión deben ir calificadas con su tabla (`orders.amount`, no solo `amount`). Una columna sin calificar es un error grave al expandir la métrica, y nada la deja pasar con un aviso.

El repositorio de métricas valida la expresión en cada escritura. Una expresión que no analiza
correctamente o que no contiene ninguna función de agregación se rechaza; nunca se almacena.

Ejemplo de entrada de configuración:

```yaml
metrics:
  - name: net_revenue
    expression: "SUM(orders.amount) - SUM(orders.refunds)"
    datatype: number
    description: "Order revenue after refunds"
    ai_context: "Net revenue: total order amounts minus approved refunds. Use for P&L."
```

### Consultar una métrica {: #querying-a-metric }

El compilador reserva el esquema `metrics`, y cada métrica se puede direccionar como una relación virtual dentro de él. Consúltela como una tabla, y las columnas que seleccione pasan a ser el conjunto de dimensiones y el GROUP BY:

```sql
-- Scalar total (no dimension)
SELECT value FROM metrics.net_revenue;

-- Broken out by region and month
SELECT region, month, value FROM metrics.net_revenue GROUP BY region, month;
```

El compilador reescribe esto en un agregado agrupado real sobre las tablas semánticas
subyacentes antes de que se ejecute el gobierno, de modo que RLS y el enmascaramiento se
aplican a las columnas reales.

`SELECT *` sobre una relación de métrica se rechaza. Nombre de forma explícita las columnas de dimensión y `value`.

Cuando la expresión de una métrica abarca varias tablas, el compilador las une a través de
las relaciones registradas. Una dimensión que es una columna de una tabla referenciada
directamente se resuelve a esa tabla. Una dimensión a un salto de relación de distancia se une
automáticamente. Dos saltos o una dimensión ambigua son un error irrecuperable que nombra al
responsable.

### Métricas a partir de especificaciones de fact {: #metrics-from-fact-specs }

Al registrar un fact, cada medida declarada registra automáticamente un objeto Metric
correspondiente. El campo `from_fact` de la métrica registra el nombre de la tabla de fact de
origen, y las dimensiones de agrupación válidas son los atributos de entity alcanzables a
través de las relaciones FK del fact.

Las métricas registradas automáticamente aparecen en la página Metrics con una insignia
**fact**. Se pueden editar como cualquier otra métrica.

### Vistas compuestas por métricas (view_metrics) {: #metric-composed-views-view_metrics }

Una vista `view_metrics` fija el grano de una métrica en el momento de la definición. Declare
los nombres de las métricas, las columnas de dimensión y los filtros opcionales; el compilador
genera el SELECT.

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

**Regeneración automática al cambiar una métrica.** Cuando se actualiza la expresión de una
métrica, cada vista `view_metrics` que la referencia se recompila y el SQL nuevo se persiste
de inmediato. La vista no puede desviarse de la definición de la métrica por construcción.

**Llamadas `metric()` en línea dentro de SQL de vista escrito a mano.** El `view_sql` escrito a
mano también puede referenciar métricas mediante `metric('name')`. El compilador reemplaza
cada llamada con la expresión de la métrica y registra un enlace de linaje. Esto le da a las
vistas escritas a mano la misma propiedad de recompilación al cambio cuando referencian una
métrica en lugar de copiar su fórmula.

Nota: las vistas del archivo de configuración que usan llamadas `metric()` en línea se regeneran cuando se recarga la configuración, y un upsert de la métrica no las regenera.

### La página de administración de Metrics {: #the-metrics-admin-page }

Abra el elemento de navegación **Metrics** para gestionar métricas gobernadas. Haga clic en
una fila para expandir un panel de detalle de solo lectura; haga clic en **Edit** dentro de
él para pasar a edición en línea (sin modal). **New Metric** abre una tarjeta de creación en
línea sobre la tabla. La confirmación de eliminación es el único modal de la página.

El formulario de creación/edición ofrece un constructor de tres selectores para métricas
originadas en facts: elija la tabla de fact de origen (filtrada a `modelingRole=fact`), una
columna de medida y una función de agregación (`SUM`, `AVG`, `COUNT`, `MIN`, `MAX`). El
datatype se deriva automáticamente: `COUNT → bigint`, `AVG → numeric`, `SUM/MIN/MAX → the measure column's type`
(el tipo de la columna de medida). El área de texto de expresión sigue siendo la vía de escape para
expresiones arbitrarias.

## El beneficio de la IR {: #the-ir-payoff }

Cada llamada de registro sigue la misma vía que una MV escrita a mano. La especificación de entity y fact es una representación intermedia; no es una plantilla ni una macro. El almacén de destino es una propiedad del despliegue, y el modelo no lo nombra. Cambie el motor de destino, y las mismas declaraciones `entity` / `fact` se materializan allí, porque el SQL generado y el historial bitemporal es neutral respecto al motor por diseño.
