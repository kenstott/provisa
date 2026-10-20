<!-- markdownlint-disable MD046 -->
<!-- MD046 off: mkdocs-material `===` content-tab bodies are indented, which the linter
     misreads as indented code blocks; the fenced code blocks below are required for rendering. -->

# Моделирование данных (сущности и факты) {: #data-modeling-entities-facts }

Provisa даёт вам два декларативных примитива, `entity` и `fact`, которые покрывают строительные блоки любой схемы «звезда» и любого Data Vault. Объявите спецификацию, и Provisa превратит её ровно в те определения материализованных представлений, битемпоральности и связей, которые иначе пришлось бы писать вручную.

## Что такое сущности и факты {: #what-entities-and-facts-are }

**Сущность (entity)** — это ключевая, дедуплицированная, опционально историзированная проекция исходного отношения. Вы
даёте ей имя, указываете источник, объявляете ключ сущности и атрибуты, которые хотите перенести,
и выбираете режим истории. Provisa пишет SQL представления и регистрирует MV. Когда история
включена, MV становится битемпоральным.
Тогда MV объявляет ключ сущности как `business_key` вместе со столбцами действительного времени и времени записи, которые вы указали.

**Факт** представляет собой соединение с ключами сущностей, сведённое к объявленному зерну, с агрегированными показателями. Provisa пишет запрос агрегатного MV (`GROUP BY` зерно + столбцы FK) и регистрирует связь для каждой объявленной ссылки на измерение. Факт без показателей является чистым набором ключей, то есть шаблоном link из Data Vault.

Обе конструкции являются IR. Сгенерированные определения переходят между движками без перемоделирования: материализуются в Oracle или Databricks либо остаются виртуальными поверх MPP-движка.

## Режимы истории {: #history-modes }

Для сущности доступны три режима:

| Режим | Значение |
| --- | --- |
| `none` | Только текущее состояние. Без истории. |
| `scd2` | Отслеживает каждое изменение по ключу сущности. |
| `snapshot` | Отслеживает каждое обновление по ключу сущности. |

`scd2` и `snapshot` оба хранят каждую версию, а Provisa выбирает, как их хранить. Каждому нужен столбец действительного времени и столбец времени записи (`validTime`, `recordedTime`): дата, которую ваш бизнес даёт записи, и момент, когда эта версия была введена или изменена. [Представления с историей](configuration.md#views-with-history-time-travel) объясняет эти два времени.

У фактов нет собственного режима истории; их покрытие во времени берётся из истории сущностей под ними.

## Меры и агрегации {: #measures-and-aggregations }

Меры объявляются как пары `column:agg`. Поддерживаемые агрегации:

`sum` &nbsp;`avg` &nbsp;`min` &nbsp;`max` &nbsp;`count`

Агрегация по умолчанию — `sum`.

## Разобранный пример: сущность Customer + факт Sales {: #worked-example-customer-entity-sales-fact }

### Исходные таблицы {: #the-source-tables }

- `raw.customers` — id, name, region, tier, effective_date, updated_at
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
       - **Valid time:** `effective_date`
       - **Recorded time:** `updated_at`
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
        validTime: "effective_date"
        recordedTime: "updated_at"
      }) {
        success
        message
      }
    }
    ```

Provisa генерирует и регистрирует этот битемпоральный MV:

```sql
SELECT "id", "name", "region", "tier", "effective_date", "updated_at" FROM "raw"."customers"
-- registered as a bitemporal MV: business_key ["id"], valid_time effective_date,
-- recorded_time updated_at
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
| Valid time | с историей | Столбец с датой, которую ваш бизнес даёт записи |
| Recorded time | с историей | Столбец с моментом, когда эта версия была введена или изменена |

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
| `validTime` | String | `null` | Столбец действительного времени; обязателен, если `history` не `"none"` |
| `recordedTime` | String | `null` | Столбец времени записи; обязателен, если `history` не `"none"` |
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

`MeasureInput`: `{ column: String, agg: String }`; agg по умолчанию `"sum"`.

`DimRefInput`: `{ entity: String, via: String }`; `entity` — это имя ссылаемой сущности; `via` — столбец FK в источнике факта.

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
| Type 2 (версионированный) | `scd2` | Битемпоральный MV; каждое изменение добавляет новую версию по ключу сущности |

Используйте `scd2`, когда последующим запросам нужно соединять измерение таким, каким оно было в момент транзакции, например уровень клиента на момент покупки, а не текущий. Используйте `none` для стабильных справочников. Шкала дат никогда не меняется, а каталог товаров, где важна только текущая цена, можно перестраивать при каждом обновлении.

#### Решение о grain {: #grain-decision }

Зерно задаёт самый мелкий уровень детализации, на который отвечает факт. `order_id` даёт одну строку на заказ и сохраняет возможность считать уникальные заказы и соединять с любым измерением на уровне заказа. Более крупное зерно, скажем `["customer_id", "order_date"]`, заранее агрегирует по заказам и навсегда теряет эту детализацию. Объявляйте самое мелкое зерно, нужное бизнесу; более крупные свёртки потом дёшево вывести.

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
    validTime: "effective_date"
    recordedTime: "updated_at"
  }) { success message }
}
```

Генерирует битемпоральный MV по ключу `id`:

```sql
SELECT "id", "name", "region", "tier", "effective_date", "updated_at" FROM "raw"."customers"
-- bitemporal MV: business_key ["id"], valid_time effective_date, recorded_time updated_at
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

Зерно: `order_id`. Три ссылки на измерения, по одному столбцу FK на каждую. Оба показателя являются аддитивными суммами.

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

Satellite находится рядом с ключом hub и несёт полную историю атрибутов. Используйте `scd2`, чтобы отслеживать каждое изменение, или `snapshot`, чтобы отслеживать каждое обновление.

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

Генерирует:

```sql
SELECT "id", "name", "region", "tier", "effective_date", "updated_at" FROM "raw"."customers"
-- bitemporal MV: business_key ["id"], valid_time effective_date, recorded_time updated_at
```

`CustomerSat` и `CustomerHub` оба ключуются по `id`. Hub — это стабильная цель соединения; satellite
предоставляет доступ к атрибутам на определённый момент времени через битемпоральный слой.

#### Link (факт без мер) {: #link-measureless-fact }

Link фиксирует, какие ключи хабов встречались вместе. Он содержит только ключи без показателей, и Provisa опускает `GROUP BY`, когда `measures` пуст.

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

**Метрика** представляет собой именованное управляемое определение агрегата без собственного зерна. Зерно, то есть измерения, по которым раскладывается агрегат, задаёт вызывающий в момент запроса, а не при определении. Этим метрика и отличается от представления: представление фиксирует зерно при создании, а метрика остаётся открытой до запроса.

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

Ссылки на столбцы внутри выражения должны быть квалифицированы таблицей (`orders.amount`, а не просто `amount`). Неквалифицированный столбец при раскрытии метрики даёт жёсткую ошибку, и ничто не пропускает его с одним лишь предупреждением.

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

Компилятор резервирует схему `metrics`, и к каждой метрике можно обратиться как к виртуальному отношению внутри неё. Запрашивайте её как таблицу, и выбранные столбцы станут набором измерений и GROUP BY:

```sql
-- Scalar total (no dimension)
SELECT value FROM metrics.net_revenue;

-- Broken out by region and month
SELECT region, month, value FROM metrics.net_revenue GROUP BY region, month;
```

Компилятор переписывает это в реальный сгруппированный агрегат по базовым семантическим таблицам до
выполнения governance, так что RLS и маскирование применяются к реальным столбцам.

`SELECT *` к отношению метрики отклоняется. Явно перечислите столбцы измерений и `value`.

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

Примечание: представления из файла конфигурации, использующие встроенные вызовы `metric()`, пересоздаются при перезагрузке конфигурации, а upsert метрики их не пересоздаёт.

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

Каждый вызов регистрации проходит тот же путь, что и написанное вручную MV. Спецификация entity и fact является промежуточным представлением, а не шаблоном или макросом. Целевое хранилище определяется развёртыванием, и модель его не называет. Смените целевой движок, и те же объявления `entity` / `fact` материализуются там, потому что сгенерированный SQL и битемпоральная история по своему устройству не зависят от движка.
