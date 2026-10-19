<!-- markdownlint-disable MD046 -->
<!-- MD046 off: mkdocs-material `===` content-tab bodies are indented, which the linter
     misreads as indented code blocks; the fenced code blocks below are required for rendering. -->

# Моделирование данных (сущности и факты) {: #data-modeling-entities-facts }

Provisa предоставляет два декларативных примитива — `entity` и `fact` — которые покрывают строительные
блоки, из которых собирается любая звёздная схема (star schema) и Data Vault. Объявите спецификацию; Provisa
понижает её ровно до тех определений материализованного представления, битемпоральности и связей, которые вы
иначе писали бы вручную.

## Что такое сущности и факты {: #what-entities-and-facts-are }

**Сущность (entity)** — это ключевая, дедуплицированная, опционально историзированная проекция исходного отношения. Вы
даёте ей имя, указываете источник, объявляете ключ сущности и атрибуты, которые хотите перенести,
и выбираете режим истории. Provisa пишет SQL представления и регистрирует MV. Когда история
включена, MV становится битемпоральным.

**Факт (fact)** — это соединение с ключами сущностей, приведённое к объявленной степени детализации (grain), с агрегированными мерами. Provisa
пишет запрос агрегатного MV (`GROUP BY` grain + столбцы FK) и регистрирует связь для каждого
объявленного измерения. Факт без мер — это чистый набор ключей — паттерн Data Vault link.

Обе конструкции — это IR. Сгенерированные определения перенацеливаются между движками — материализованные в Oracle,
Databricks или оставленные виртуальными поверх MPP-движка — без переделки модели.

## Режимы истории {: #history-modes }

Для сущности доступны три режима:

| Режим | Значение | Битемпоральный режим |
| --- | --- | --- |
| `none` | Только текущее состояние. Без истории. | — |
| `scd2` | Отслеживать каждое изменение. Добавлять только изменённые строки (дельта) по ключу сущности. | `delta` |
| `snapshot` | Отслеживать каждое обновление. Добавлять полный результирующий набор при каждом обновлении, помеченный системным временем. | `snapshot` |

`scd2` требует ключ сущности для вычисления дельты. `snapshot` работает на любом движке, но хранилище растёт
на полную копию при каждом обновлении. Выбирайте `scd2` для больших, медленно меняющихся источников; выбирайте `snapshot`, когда
нужна полная история, а источник не может предоставить ключ.

У фактов нет режима истории — временное покрытие идёт от истории базовой сущности.

## Меры и агрегации {: #measures-and-aggregations }

Меры объявляются как пары `column:agg`. Поддерживаемые агрегации:

`sum` &nbsp;`avg` &nbsp;`min` &nbsp;`max` &nbsp;`count`

Агрегация по умолчанию — `sum`.

## Разобранный пример: сущность Customer + факт Sales {: #worked-example-customer-entity-sales-fact }

### Исходные таблицы {: #the-source-tables }

- `raw.customers` — id, name, region, tier
- `raw.orders` — order_id, customer_id, amount, quantity

### Регистрация сущности Customer {: #register-the-customer-entity }

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

Provisa генерирует и регистрирует этот битемпоральный MV:

```sql
SELECT "id", "name", "region", "tier" FROM "raw"."customers"
-- registered as a bitemporal delta MV, entity key: ["id"]
```

### Регистрация факта Sales {: #register-the-sales-fact }

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

Provisa генерирует и регистрирует:

```sql
SELECT "order_id", "customer_id",
       SUM("amount") AS "amount",
       SUM("quantity") AS "quantity"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id"
```

Плюс одна зарегистрированная связь: `Sales.customer_id → Customer` (кардинальность: many-to-one).

## Форма Model (admin UI) {: #the-model-form-admin-ui }

Кнопка **+ Model** появляется на странице **Tables** (подсказка: "Model an entity or fact (star
schema / Data Vault)").

Сегментированный переключатель в верхней части модального окна переключается между **Entity (dimension)** и **Fact**.

### Поля сущности {: #entity-fields }

| Поле | Обязательно | Примечания |
| --- | --- | --- |
| Name | да | Имя MV в каталоге |
| Source relation | да | Отношение через точку, например `raw.customers` |
| Domain | да | Домен, к которому принадлежит MV |
| Entity key | да | Столбец(ы) ключа через запятую, например `id` |
| Attributes | нет | Столбцы атрибутов через запятую, например `name, region, tier` |
| History | нет | `none` / `scd2` / `snapshot`; по умолчанию `none` |

### Поля факта {: #fact-fields }

| Поле | Обязательно | Примечания |
| --- | --- | --- |
| Name | да | Имя MV в каталоге |
| Source relation | да | Отношение через точку, например `raw.orders` |
| Domain | да | Домен, к которому принадлежит MV |
| Grain | да | Столбец(ы) степени детализации через запятую, например `order_id` |
| Measures | нет | Пары `col:agg` через запятую, например `amount:sum, quantity:sum` |
| Dimensions | нет | Пары `Entity:fk_column` через запятую, например `Customer:customer_id` |

Если `agg` опущен в мере (`amount` вместо `amount:sum`), по умолчанию используется `sum`.

## GraphQL API {: #the-graphql-api }

Обе мутации находятся в схеме admin.

### `registerEntity`

```graphql
mutation RegisterEntity($input: EntityInput!) {
  registerEntity(input: $input) {
    success
    message
  }
}
```

Поля `EntityInput`:

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

Поля `FactInput`:

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

`DimRefInput`: `{ entity: String, via: String }` — `entity` — это имя ссылаемой сущности;
`via` — столбец FK в источнике факта.

При успехе `registerFact` возвращает сообщение вида:
`Fact 'Sales' registered with 1 dimension link(s)`.

## Звёздная схема Kimball и Data Vault {: #kimball-star-schema-and-data-vault }

Ни один из паттернов не требует отдельного инструментария. Одни и те же два примитива компонуются в оба.

### Звёздная схема Kimball {: #kimball-star-schema }

Этот разбор строит звезду с тремя измерениями. Две исходные таблицы новые:

- `raw.products` — `product_id`, `name`, `category`, `list_price`
- `raw.date_spine` — `date_key`, `year`, `quarter`, `month`

`raw.orders` здесь также получает столбцы `product_id` и `order_date`.

#### Выбор типа SCD {: #choosing-scd-type }

Режим истории — это единственный переключатель между SCD Type 1 и Type 2:

| Тип SCD | Режим истории | Эффект |
| --- | --- | --- |
| Type 1 (только текущее) | `none` | MV пересобирается при обновлении; без истории строк |
| Type 2 (версионированный) | `scd2` | Битемпоральный дельта-MV; каждое изменение добавляет новую строку по ключу сущности |

Используйте `scd2`, когда нисходящим запросам нужно соединяться с измерением таким, каким оно было на момент транзакции — уровень
клиента в момент покупки, а не его текущий уровень. Используйте `none` для стабильных справочников.
Ось дат никогда не меняется. Каталог продуктов, где нужна только текущая цена, можно пересобирать при
каждом обновлении.

#### Решение о grain {: #grain-decision }

Grain — это самый низкий уровень детализации, на который отвечает факт. `order_id` даёт одну строку на заказ,
сохраняя возможность подсчитывать уникальные заказы и соединяться с любым измерением на уровне заказа.
Более грубый grain — скажем, `["customer_id", "order_date"]` — предварительно агрегирует по заказам и безвозвратно отбрасывает
эту детализацию. Объявляйте самый узкий grain, который нужен бизнесу; более грубые свёртки дёшево
вывести впоследствии.

#### Регистрация измерений {: #register-the-dimensions }

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

Генерирует битемпоральный дельта-MV по ключу `id`:

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

Генерирует обычный MV, пересобираемый при обновлении:

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

#### Регистрация факта Sales по трём измерениям {: #register-the-sales-fact-across-three-dimensions }

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
:

```sql
SELECT "order_id", "customer_id", "product_id", "order_date",
       SUM("amount")   AS "amount",
       SUM("quantity") AS "quantity"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id", "product_id", "order_date"
```

Автоматически регистрируются три связи:

| Связь | Кардинальность |
| --- | --- |
| `Sales.customer_id → Customer` | many-to-one |
| `Sales.product_id → Product` | many-to-one |
| `Sales.order_date → DateDim` | many-to-one |

#### Согласованные измерения (conformed dimensions) {: #conformed-dimensions }

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
.

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

Генерирует:

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

Генерирует:

```sql
SELECT "id", "name", "region", "tier" FROM "raw"."customers"
-- bitemporal delta MV, entity key: ["id"]
```

`CustomerSat` и `CustomerHub` оба ключуются по `id`. Hub — это стабильная цель соединения; satellite
предоставляет доступ к атрибутам на определённый момент времени через битемпоральный слой.

#### Link (факт без мер) {: #link-measureless-fact }

Link фиксирует, какие ключи hub встречались вместе — только ключи, без мер. Provisa опускает `GROUP BY`,
когда `measures` пуст.

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
Мер нет, поэтому нет `GROUP BY`. Генерирует:

```sql
SELECT "order_id", "customer_id" FROM "raw"."orders"
```

Регистрируются две связи: `OrderCustomerLink.customer_id → CustomerHub` и
`OrderCustomerLink.order_id → OrderHub`, обе many-to-one
.

#### Bridge / агрегатная связь {: #bridge-aggregate-link }

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
:

```sql
SELECT "order_id", "customer_id", SUM("amount") AS "amount"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id"
```

Модель не решает методологию. Grain, согласованность, выбор SCD и разделение hub/satellite
остаются решениями моделировщика. Provisa их исполняет.

## Метрики {: #metrics }

**Метрика (metric)** — это именованное, управляемое определение агрегата без собственного grain. Grain — измерения,
по которым разбивается агрегат, — привязывается во время запроса вызывающей стороной, а не во время
определения. Именно это отличает метрику от представления: представление фиксирует grain при создании; метрика
остаётся открытой до момента запроса.

### Объект Metric {: #the-metric-object }

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

Репозиторий метрик проверяет выражение при каждой записи. Выражение, которое не разбирается или
не содержит агрегатной функции, отклоняется; оно никогда не сохраняется.

Пример записи конфигурации:

```yaml
metrics:
  - name: net_revenue
    expression: "SUM(orders.amount) - SUM(orders.refunds)"
    datatype: number
    description: "Order revenue after refunds"
    ai_context: "Net revenue: total order amounts minus approved refunds. Use for P&L."
```

### Запрос метрики {: #querying-a-metric }

Компилятор резервирует схему `metrics`. Каждая метрика адресуема как виртуальное отношение
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

`SELECT *` для отношения метрики отклоняется — называйте столбцы измерений и `value`
явно.

Когда выражение метрики охватывает несколько таблиц, компилятор соединяет их через зарегистрированные
связи. Измерение, являющееся столбцом напрямую ссылаемой таблицы, разрешается в эту таблицу.
Измерение на один переход связи дальше соединяется автоматически. Два перехода или неоднозначное
измерение — жёсткая ошибка с указанием виновника.

### Метрики из спецификаций фактов {: #metrics-from-fact-specs }

Когда вы регистрируете факт, каждая объявленная мера автоматически регистрирует соответствующий объект Metric.
Поле `from_fact` метрики фиксирует имя таблицы-источника факта, а допустимые измерения группировки —
это атрибуты сущности, достижимые через связи FK факта.

Автоматически зарегистрированные метрики появляются на странице Metrics со значком **fact**. Вы можете редактировать их как
любую другую метрику.

### Представления, составленные из метрик (view_metrics) {: #metric-composed-views-view_metrics }

Представление `view_metrics` фиксирует grain метрики во время определения. Объявите имена метрик,
столбцы измерений и опциональные фильтры; компилятор генерирует SELECT.

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

**Автоматическая регенерация при изменении метрики.** Когда выражение метрики обновляется, каждое
представление `view_metrics`, ссылающееся на неё, перекомпилируется, и новый SQL немедленно сохраняется.
Представление по конструкции не может разойтись с определением метрики.

**Встроенные вызовы `metric()` в свободном SQL представления.** Написанный вручную `view_sql` также может
ссылаться на метрики через `metric('name')`. Компилятор заменяет каждый вызов выражением метрики и
записывает ребро происхождения. Это даёт свободным представлениям то же свойство перекомпиляции при изменении, когда
они ссылаются на метрику, а не копируют её формулу.

Примечание: представления из конфигурации, использующие встроенные вызовы `metric()`, регенерируются при перезагрузке конфигурации, а не при
upsert метрики.

### Административная страница Metrics {: #the-metrics-admin-page }

Откройте пункт навигации **Metrics** для управления управляемыми метриками. Нажмите на строку, чтобы развернуть
доступную только для чтения панель деталей; нажмите **Edit** внутри неё, чтобы переключиться на встроенное редактирование (без модального окна). **New Metric** открывает
встроенную карточку создания над таблицей. Подтверждение удаления — единственное модальное окно на странице.

Форма создания/редактирования предлагает конструктор из трёх выпадающих списков для метрик на основе факта: выберите
исходную таблицу факта (отфильтрованную по `modelingRole=fact`), столбец меры и агрегатную функцию
(`SUM`, `AVG`, `COUNT`, `MIN`, `MAX`). Тип данных выводится автоматически:
`COUNT → bigint`, `AVG → numeric`, `SUM/MIN/MAX → the measure column's type` (тип столбца меры).
Текстовое поле выражения остаётся аварийным выходом для произвольных выражений.

## Выигрыш от IR {: #the-ir-payoff }

Каждый вызов регистрации проходит тем же путём, что и написанный вручную MV. Спецификация entity/fact
— это промежуточное представление (intermediate representation) — не шаблон, не макрос. Хранилище, на которое она нацелена, — это
свойство развёртывания, а не модели. Смените целевой движок, и те же объявления `entity` /
`fact` материализуются там же, потому что сгенерированный SQL и битемпоральные режимы
нейтральны к движку по построению.
