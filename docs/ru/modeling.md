<!-- markdownlint-disable MD046 -->
<!-- MD046 off: mkdocs-material `===` content-tab bodies are indented, which the linter
     misreads as indented code blocks; the fenced code blocks below are required for rendering. -->

# Моделирование данных (сущности и факты)

Provisa предоставляет два декларативных примитива — `entity` и `fact` — которые покрывают строительные
блоки, из которых собирается любая звёздная схема (star schema) и Data Vault. Объявите спецификацию; Provisa
понижает её ровно до тех определений материализованного представления, битемпоральности и связей, которые вы
иначе писали бы вручную (REQ-1164). [tool-verified: modeling.py module docstring lines 11-28]

## Что такое сущности и факты

**Сущность (entity)** — это ключевая, дедуплицированная, опционально историзированная проекция исходного отношения. Вы
даёте ей имя, указываете источник, объявляете ключ сущности и атрибуты, которые хотите перенести,
и выбираете режим истории. Provisa пишет SQL представления и регистрирует MV. Когда история
включена, MV становится битемпоральным. [tool-verified: `Entity` dataclass, modeling.py lines 53-69;
`entity_registration` function, modeling.py lines 105-120]

**Факт (fact)** — это соединение с ключами сущностей, приведённое к объявленной степени детализации (grain), с агрегированными мерами. Provisa
пишет запрос агрегатного MV (`GROUP BY` grain + столбцы FK) и регистрирует связь для каждого
объявленного измерения. Факт без мер — это чистый набор ключей — паттерн Data Vault link.
[tool-verified: `Fact` dataclass, modeling.py lines 91-102; `fact_registration` function, modeling.py
lines 123-141; comment at line 130 "a measureless fact is a pure key-set (DV link)"]

Обе конструкции — это IR. Сгенерированные определения перенацеливаются между движками — материализованные в Oracle,
Databricks или оставленные виртуальными поверх MPP-движка — без переделки модели. [tool-verified: modeling.py
docstring lines 25-28]

## Режимы истории

Для сущности доступны три режима [tool-verified: `_HISTORY` constant at modeling.py line 38,
`_HISTORY_MODE` dict at modeling.py line 40]:

| Режим | Значение | Битемпоральный режим |
| --- | --- | --- |
| `none` | Только текущее состояние. Без истории. | — |
| `scd2` | Отслеживать каждое изменение. Добавлять только изменённые строки (дельта) по ключу сущности. | `delta` |
| `snapshot` | Отслеживать каждое обновление. Добавлять полный результирующий набор при каждом обновлении, помеченный системным временем. | `snapshot` |

`scd2` требует ключ сущности для вычисления дельты. `snapshot` работает на любом движке, но хранилище растёт
на полную копию при каждом обновлении. Выбирайте `scd2` для больших, медленно меняющихся источников; выбирайте `snapshot`, когда
нужна полная история, а источник не может предоставить ключ.

У фактов нет режима истории — временное покрытие идёт от истории базовой сущности.

## Меры и агрегации

Меры объявляются как пары `column:agg`. Поддерживаемые агрегации [tool-verified: `_AGGS`
at modeling.py line 41]:

`sum` &nbsp;`avg` &nbsp;`min` &nbsp;`max` &nbsp;`count`

Агрегация по умолчанию — `sum` [tool-verified: `Measure.agg` default at modeling.py line 75].

## Разобранный пример: сущность Customer + факт Sales

### Исходные таблицы

- `raw.customers` — id, name, region, tier
- `raw.orders` — order_id, customer_id, amount, quantity

### Регистрация сущности Customer

=== "Admin UI"

    1. Откройте **Tables** и нажмите **+ Model**.
    2. Выберите **Entity (dimension)**.
    3. Заполните форму:
       - **Name:** `Customer`
       - **Source relation:** `raw.customers`
       - **Domain:** *(ваш домен)*
       - **Entity key:** `id`
       - **Attributes:** `name, region, tier`
       - **History:** `SCD2 (track changes — delta bitemporal)`
    4. Нажмите **Create**.

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

Provisa генерирует и регистрирует этот битемпоральный MV [tool-verified: `entity_registration` in
modeling.py lines 105-120]:

```sql
SELECT "id", "name", "region", "tier" FROM "raw"."customers"
-- registered as a bitemporal delta MV, entity key: ["id"]
```

### Регистрация факта Sales

=== "Admin UI"

    1. Снова нажмите **+ Model**.
    2. Выберите **Fact**.
    3. Заполните форму:
       - **Name:** `Sales`
       - **Source relation:** `raw.orders`
       - **Domain:** *(ваш домен)*
       - **Grain:** `order_id`
       - **Measures:** `amount:sum, quantity:sum`
       - **Dimensions:** `Customer:customer_id`
    4. Нажмите **Create**.

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

Provisa генерирует и регистрирует [tool-verified: `fact_registration` in modeling.py lines 123-141]:

```sql
SELECT "order_id", "customer_id",
       SUM("amount") AS "amount",
       SUM("quantity") AS "quantity"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id"
```

Плюс одна зарегистрированная связь: `Sales.customer_id → Customer` (кардинальность: many-to-one).
[tool-verified: `fact_table_input` in modeling_register.py lines 89-98, cardinality at line 95]

## Форма Model (admin UI)

Кнопка **+ Model** появляется на странице **Tables** (подсказка: "Model an entity or fact (star
schema / Data Vault)"). [tool-verified: tablesPage.json line 13; TablesPage.tsx lines 441-450]

Сегментированный переключатель в верхней части модального окна переключается между **Entity (dimension)** и **Fact**.
[tool-verified: ModelingForm.tsx lines 102-110]

### Поля сущности

[tool-verified: ModelingForm.tsx lines 141-171; modelingForm.json]

| Поле | Обязательно | Примечания |
| --- | --- | --- |
| Name | да | Имя MV в каталоге |
| Source relation | да | Отношение через точку, например `raw.customers` |
| Domain | да | Домен, к которому принадлежит MV |
| Entity key | да | Столбец(ы) ключа через запятую, например `id` |
| Attributes | нет | Столбцы атрибутов через запятую, например `name, region, tier` |
| History | нет | `none` / `scd2` / `snapshot`; по умолчанию `none` |

### Поля факта

[tool-verified: ModelingForm.tsx lines 172-196; modelingForm.json]

| Поле | Обязательно | Примечания |
| --- | --- | --- |
| Name | да | Имя MV в каталоге |
| Source relation | да | Отношение через точку, например `raw.orders` |
| Domain | да | Домен, к которому принадлежит MV |
| Grain | да | Столбец(ы) степени детализации через запятую, например `order_id` |
| Measures | нет | Пары `col:agg` через запятую, например `amount:sum, quantity:sum` |
| Dimensions | нет | Пары `Entity:fk_column` через запятую, например `Customer:customer_id` |

Если `agg` опущен в мере (`amount` вместо `amount:sum`), по умолчанию используется `sum`.
[tool-verified: ModelingForm.tsx line 73 `agg: agg || "sum"`]

## GraphQL API

Обе мутации находятся в схеме admin. [tool-verified: schema_mutation.py lines 449-472]

### `registerEntity`

```graphql
mutation RegisterEntity($input: EntityInput!) {
  registerEntity(input: $input) {
    success
    message
  }
}
```

Поля `EntityInput` [tool-verified: types.py lines 449-456]:

| Поле | Тип | По умолчанию | Описание |
| --- | --- | --- | --- |
| `name` | String | — | Имя в каталоге для MV сущности |
| `source` | String | — | Исходное отношение (`schema.table` или в кавычках) |
| `domainId` | String | — | Id домена |
| `key` | [String] | — | Столбец(ы) ключа сущности |
| `attributes` | [String] | `[]` | Столбцы атрибутов для проекции |
| `history` | String | `"none"` | `"none"` \| `"scd2"` \| `"snapshot"` |
| `visibleTo` | [String] | `["public"]` | Список видимости по ролям |

### `registerFact`

```graphql
mutation RegisterFact($input: FactInput!) {
  registerFact(input: $input) {
    success
    message
  }
}
```

Поля `FactInput` [tool-verified: types.py lines 472-479]:

| Поле | Тип | По умолчанию | Описание |
| --- | --- | --- | --- |
| `name` | String | — | Имя в каталоге для MV факта |
| `source` | String | — | Исходное отношение |
| `domainId` | String | — | Id домена |
| `grain` | [String] | — | Столбец(ы) степени детализации для GROUP BY |
| `measures` | [MeasureInput] | `[]` | Пары `{ column, agg }` |
| `dimensions` | [DimRefInput] | `[]` | Пары `{ entity, via }` |
| `visibleTo` | [String] | `["public"]` | Список видимости по ролям |

`MeasureInput`: `{ column: String, agg: String }` — agg по умолчанию `"sum"`.
[tool-verified: types.py lines 460-462]

`DimRefInput`: `{ entity: String, via: String }` — `entity` — это имя ссылаемой сущности;
`via` — столбец FK в источнике факта.
[tool-verified: types.py lines 465-468]

При успехе `registerFact` возвращает сообщение вида:
`Fact 'Sales' registered with 1 dimension link(s)`.
[tool-verified: schema_mutation.py line 471]

## Звёздная схема Kimball и Data Vault

Ни один из паттернов не требует отдельного инструментария. Одни и те же два примитива компонуются в оба.

### Звёздная схема Kimball

Этот разбор строит звезду с тремя измерениями. Две исходные таблицы новые:

- `raw.products` — `product_id`, `name`, `category`, `list_price` [inferred: introduced for this example]
- `raw.date_spine` — `date_key`, `year`, `quarter`, `month` [inferred: introduced for this example]

`raw.orders` здесь также получает столбцы `product_id` и `order_date`. [inferred]

#### Выбор типа SCD

Режим истории — это единственный переключатель между SCD Type 1 и Type 2:

| Тип SCD | Режим истории | Эффект |
| --- | --- | --- |
| Type 1 (только текущее) | `none` | MV пересобирается при обновлении; без истории строк |
| Type 2 (версионированный) | `scd2` | Битемпоральный дельта-MV; каждое изменение добавляет новую строку по ключу сущности |

[tool-verified: `_HISTORY_MODE` at modeling.py line 40; `entity_registration` history branch at
lines 115-119]

Используйте `scd2`, когда нисходящим запросам нужно соединяться с измерением таким, каким оно было на момент транзакции — уровень
клиента в момент покупки, а не его текущий уровень. Используйте `none` для стабильных справочников.
Ось дат никогда не меняется. Каталог продуктов, где нужна только текущая цена, можно пересобирать при
каждом обновлении.

#### Решение о grain

Grain — это самый низкий уровень детализации, на который отвечает факт. `order_id` даёт одну строку на заказ,
сохраняя возможность подсчитывать уникальные заказы и соединяться с любым измерением на уровне заказа.
Более грубый grain — скажем, `["customer_id", "order_date"]` — предварительно агрегирует по заказам и безвозвратно отбрасывает
эту детализацию. Объявляйте самый узкий grain, который нужен бизнесу; более грубые свёртки дёшево
вывести впоследствии.

#### Регистрация измерений

**Customer** (SCD Type 2 — изменения уровня должны сохраняться):

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

Генерирует битемпоральный дельта-MV по ключу `id` [tool-verified: entity_registration modeling.py
lines 105-120]:

```sql
SELECT "id", "name", "region", "tier" FROM "raw"."customers"
-- bitemporal delta MV, entity key: ["id"]
```

**Product** (SCD Type 1 — текущий каталог, история версий не нужна):

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

Генерирует обычный MV, пересобираемый при обновлении [tool-verified: entity_registration modeling.py
lines 105-114; `mv_bitemporal_mode` is only added when `history != "none"`, line 115]:

```sql
SELECT "product_id", "name", "category", "list_price" FROM "raw"."products"
```

**DateDim** (без истории — дата неизменна):

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

Генерирует:

```sql
SELECT "date_key", "year", "quarter", "month" FROM "raw"."date_spine"
```

#### Регистрация факта Sales по трём измерениям

Grain: `order_id`. Три ссылки на измерения — по одному столбцу FK каждая. Обе меры — аддитивные суммы.

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

Provisa вычисляет `group_cols = dedup([grain] + [dim FKs])`
= `["order_id", "customer_id", "product_id", "order_date"]` и генерирует
[tool-verified: fact_registration modeling.py lines 125-131]:

```sql
SELECT "order_id", "customer_id", "product_id", "order_date",
       SUM("amount")   AS "amount",
       SUM("quantity") AS "quantity"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id", "product_id", "order_date"
```

Автоматически регистрируются три связи [tool-verified: modeling_register.py lines 89-98,
cardinality `"many_to_one"` at line 95]:

| Связь | Кардинальность |
| --- | --- |
| `Sales.customer_id → Customer` | many-to-one |
| `Sales.product_id → Product` | many-to-one |
| `Sales.order_date → DateDim` | many-to-one |

#### Согласованные измерения (conformed dimensions)

Согласованное измерение регистрируется один раз и ссылается по имени из любого числа фактов. Допустим,
`raw.returns` содержит `return_id`, `customer_id`, `product_id` и `amount`. Факт Returns переиспользует
Customer и Product без повторной регистрации:

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

И `Sales`, и `Returns` указывают на одни и те же сущности `Customer` и `Product`. Пути соединений Provisa
гарантируют, что запросы через любой из фактов проходят через одно и то же определение измерения
[tool-verified: fact_registration uses entity name as `target_table` at modeling.py lines 138-140;
fact_table_input wires `target_table_id` from that name at modeling_register.py lines 91-93].

---

### Data Vault

Те же примитивы напрямую отображаются на терминологию Data Vault:

| Артефакт DV | Примитив | История |
| --- | --- | --- |
| Hub | `entity` | `none` — только ключи сущности |
| Satellite | `entity` | `scd2` или `snapshot` — история атрибутов рядом с ключом hub |
| Link | `fact` без мер | — |
| Bridge / агрегатная связь | `fact` с мерами | — |

Пример строит минимальное хранилище (vault) поверх `raw.customers` и `raw.orders`.

#### Hubs

Hub хранит ключ сущности и ничего больше. `attributes: []` с `history: "none"` производит
дедуплицированный текущий набор ключей; история атрибутов целиком живёт в satellite.

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

Генерирует [tool-verified: entity_registration modeling.py lines 107-108;
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

Генерирует:

```sql
SELECT "order_id" FROM "raw"."orders"
```

#### Satellite

Satellite находится рядом с ключом hub и несёт полную историю атрибутов. Используйте `scd2`, чтобы добавлять
только изменённые строки; используйте `snapshot`, чтобы фиксировать каждое полное обновление.

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

Генерирует [tool-verified: entity_registration modeling.py lines 115-119;
`_HISTORY_MODE["scd2"]` = `"delta"` at modeling.py line 40]:

```sql
SELECT "id", "name", "region", "tier" FROM "raw"."customers"
-- bitemporal delta MV, entity key: ["id"]
```

`CustomerSat` и `CustomerHub` оба ключуются по `id`. Hub — это стабильная цель соединения; satellite
предоставляет доступ к атрибутам на определённый момент времени через битемпоральный слой.

#### Link (факт без мер)

Link фиксирует, какие ключи hub встречались вместе — только ключи, без мер. Provisa опускает `GROUP BY`,
когда `measures` пуст [tool-verified: modeling.py lines 130-131:
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

`group_cols = dedup(["order_id"] + ["customer_id", "order_id"])` = `["order_id", "customer_id"]`.
Мер нет, поэтому нет `GROUP BY`. Генерирует [tool-verified: fact_registration modeling.py lines
125-131]:

```sql
SELECT "order_id", "customer_id" FROM "raw"."orders"
```

Регистрируются две связи: `OrderCustomerLink.customer_id → CustomerHub` и
`OrderCustomerLink.order_id → OrderHub`, обе many-to-one
[tool-verified: modeling_register.py lines 89-98].

#### Bridge / агрегатная связь

Добавьте меры к link, и Provisa выдаст `GROUP BY`, произведя предварительно агрегированный bridge. При
grain `order_id` с одним клиентом на заказ результат — одна агрегированная строка на заказ:

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
(дублирующийся `order_id` из списка измерений удаляется `_dedup`). Генерирует
[tool-verified: fact_registration modeling.py lines 125-131]:

```sql
SELECT "order_id", "customer_id", SUM("amount") AS "amount"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id"
```

Модель не решает методологию. Grain, согласованность, выбор SCD и разделение hub/satellite
остаются решениями моделировщика. Provisa их исполняет. [tool-verified: modeling.py
docstring lines 25-26]

## Метрики (REQ-1317, REQ-1318, REQ-1320)

**Метрика (metric)** — это именованное, управляемое определение агрегата без собственного grain. Grain — измерения,
по которым разбивается агрегат, — привязывается во время запроса вызывающей стороной, а не во время
определения. Именно это отличает метрику от представления: представление фиксирует grain при создании; метрика
остаётся открытой до момента запроса. [tool-verified: `Metric` class comment, `provisa/core/models.py` lines
452–455: "A named, governed aggregate definition with no grain of its own... grain is bound at
query time by the requested dimension set"]

### Объект Metric

[tool-verified: `Metric` class, `provisa/core/models.py` lines 451–476]

| Поле | Обязательно | Примечания |
| --- | --- | --- |
| `name` | да | snake_case, например `net_revenue`. Проверяется: `[a-z][a-z0-9_]*` |
| `expression` | да | Агрегатный ANSI-SQL; должен включать хотя бы одну агрегатную функцию |
| `datatype` | нет | Подсказка типа результата, например `number`, `integer` |
| `description` | нет | Человекочитаемое бизнес-определение |
| `ai_context` | нет | Текст для потребителей ИИ — проецируется в инструменты MCP, pg_description, документацию GraphQL и экспорт Ossie |
| `visible_to` | нет | Список ролей; по умолчанию `["*"]` (все роли) |
| `from_fact` | — | Устанавливается автоматически, когда метрика сгенерирована из меры факта |

Ссылки на столбцы внутри выражения должны быть квалифицированы таблицей (`orders.amount`, а не `amount`).
Неквалифицированный столбец — жёсткая ошибка на этапе раскрытия, а не предупреждение.
[tool-verified: `_expression_tables`, `provisa/compiler/metric_expand.py` lines 83–96]

Репозиторий метрик проверяет выражение при каждой записи. Выражение, которое не разбирается или
не содержит агрегатной функции, отклоняется; оно никогда не сохраняется.
[tool-verified: `validate_expression`, `provisa/core/repositories/metric.py` lines 34–43]

Пример записи конфигурации:

```yaml
metrics:
  - name: net_revenue
    expression: "SUM(orders.amount) - SUM(orders.refunds)"
    datatype: number
    description: "Order revenue after refunds"
    ai_context: "Net revenue: total order amounts minus approved refunds. Use for P&L."
```

### Запрос метрики

Компилятор резервирует схему `metrics`. [tool-verified: `METRICS_SCHEMA = "metrics"`,
`provisa/compiler/metric_expand.py` line 43] Каждая метрика адресуема как виртуальное отношение
внутри этой схемы. Запрашивайте её как таблицу — столбцы, которые вы выбираете, становятся набором измерений и
GROUP BY:

```sql
-- Scalar total (no dimension)
SELECT value FROM metrics.net_revenue;

-- Broken out by region and month
SELECT region, month, value FROM metrics.net_revenue GROUP BY region, month;
```

Компилятор переписывает это в реальный сгруппированный агрегат по базовым семантическим таблицам до
выполнения governance, так что RLS и маскирование применяются к реальным столбцам.
[tool-verified: `expand_metric_query` docstring, `provisa/compiler/metric_expand.py` lines 263–276:
"BEFORE governance, so RLS/masking apply to the real columns (REQ-1317)"]

`SELECT *` для отношения метрики отклоняется — называйте столбцы измерений и `value`
явно. [tool-verified: `expand_metric_query`, `provisa/compiler/metric_expand.py` lines 302–306]

Когда выражение метрики охватывает несколько таблиц, компилятор соединяет их через зарегистрированные
связи. Измерение, являющееся столбцом напрямую ссылаемой таблицы, разрешается в эту таблицу.
Измерение на один переход связи дальше соединяется автоматически. Два перехода или неоднозначное
измерение — жёсткая ошибка с указанием виновника.
[tool-verified: `_JoinPlan.resolve_dimension`, `provisa/compiler/metric_expand.py` lines 190–228]

### Метрики из спецификаций фактов (REQ-1320)

Когда вы регистрируете факт, каждая объявленная мера автоматически регистрирует соответствующий объект Metric.
Поле `from_fact` метрики фиксирует имя таблицы-источника факта, а допустимые измерения группировки —
это атрибуты сущности, достижимые через связи FK факта.
[tool-verified: `Metric.from_fact` comment, `provisa/core/models.py` line 466–467:
"set when this metric was auto-registered from a fact spec's measure";
`from_fact` stored in `provisa/core/repositories/metric.py` line 57]

Автоматически зарегистрированные метрики появляются на странице Metrics со значком **fact**. Вы можете редактировать их как
любую другую метрику. [tool-verified: `MetricsPage.tsx` lines 405–408:
`{m.fromFact && <Badge ... data-testid={`metrics-from-fact-${m.name}`}>...</Badge>}`]

### Представления, составленные из метрик (view_metrics, REQ-1318)

Представление `view_metrics` фиксирует grain метрики во время определения. Объявите имена метрик,
столбцы измерений и опциональные фильтры; компилятор генерирует SELECT.

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

Компилятор генерирует (для этого примера):

```sql
SELECT orders.region AS region, orders.month AS month,
       SUM(orders.amount) - SUM(orders.refunds) AS net_revenue
FROM orders
WHERE orders.status = 'completed'
GROUP BY orders.region, orders.month
```

`view_metrics` и `view_sql` взаимно исключают друг друга в одной и той же таблице.
[tool-verified: `Table` model validator, `provisa/core/models.py` lines 614–617:
`if self.view_sql is not None and self.view_metrics is not None: raise ValueError(...)`]

**Автоматическая регенерация при изменении метрики.** Когда выражение метрики обновляется, каждое
представление `view_metrics`, ссылающееся на неё, перекомпилируется, и новый SQL немедленно сохраняется.
Представление по конструкции не может разойтись с определением метрики.
[tool-verified: `regenerate_metric_views`, `provisa/api/admin/_metric_views.py` lines 79–117:
"each dependent view_metrics spec recompiles against the UPDATED metric set and the fresh SQL
is persisted"]

**Встроенные вызовы `metric()` в свободном SQL представления.** Написанный вручную `view_sql` также может
ссылаться на метрики через `metric('name')`. Компилятор заменяет каждый вызов выражением метрики и
записывает ребро происхождения. Это даёт свободным представлениям то же свойство перекомпиляции при изменении, когда
они ссылаются на метрику, а не копируют её формулу.
[tool-verified: `expand_metric_calls_in_sql`, `provisa/compiler/metric_expand.py` lines 393–429]

Примечание: представления из конфигурации, использующие встроенные вызовы `metric()`, регенерируются при перезагрузке конфигурации, а не при
upsert метрики. [tool-verified: `regenerate_metric_views` docstring, `_metric_views.py` lines 84–86:
"Free-hand view_sql born from inline metric() calls carries no stored provenance, so it is not
regenerated here (config-path views regenerate on config reload)"]

### Административная страница Metrics (REQ-1323, REQ-1324)

Откройте пункт навигации **Metrics** для управления управляемыми метриками. Нажмите на строку, чтобы развернуть
доступную только для чтения панель деталей; нажмите **Edit** внутри неё, чтобы переключиться на встроенное редактирование (без модального окна). **New Metric** открывает
встроенную карточку создания над таблицей. Подтверждение удаления — единственное модальное окно на странице.
[tool-verified: `MetricsPage.tsx` lines 214–216 comment: "REQ-1317: registered-metrics management
page (list / create / edit / delete). REQ-1323: detail-then-edit"]

Форма создания/редактирования предлагает конструктор из трёх выпадающих списков для метрик на основе факта: выберите
исходную таблицу факта (отфильтрованную по `modelingRole=fact`), столбец меры и агрегатную функцию
(`SUM`, `AVG`, `COUNT`, `MIN`, `MAX`). Тип данных выводится автоматически:
`COUNT → bigint`, `AVG → numeric`, `SUM/MIN/MAX → тип столбца меры`.
Текстовое поле выражения остаётся аварийным выходом для произвольных выражений.
[tool-verified: `deriveDatatype` function, `MetricsPage.tsx` lines 66–70;
`applyBuilder`, `MetricsPage.tsx` lines 273–285]

## Выигрыш от IR

Каждый вызов регистрации проходит тем же путём, что и написанный вручную MV. Спецификация entity/fact
— это промежуточное представление (intermediate representation) — не шаблон, не макрос. Хранилище, на которое она нацелена, — это
свойство развёртывания, а не модели. Смените целевой движок, и те же объявления `entity` /
`fact` материализуются там же, потому что сгенерированный SQL и битемпоральные режимы
нейтральны к движку по построению. [tool-verified: modeling.py docstring lines 25-28;
modeling_register.py lines 56-66, 80-88]
