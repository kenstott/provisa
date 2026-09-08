# API Reference

## Обзор

Provisa предоставляет REST-эндпоинты под двумя префиксами: `/data` — для выполнения запросов и интроспекции схемы, `/admin` — для управления конфигурацией. (REQ-043) Большинству эндпоинтов данных требуется идентификатор роли. Административные операции с конфигурацией используют Strawberry GraphQL API по адресу `/admin/graphql`. (REQ-164)

---

## Аутентификация

Когда в `provisa.yaml` настроен `auth.provider`, всем эндпоинтам, кроме `/health` и `/setup/status`, требуется заголовок `Authorization: Bearer <token>`. (REQ-120) [tool-verified: `provisa/api/app.py`, `provisa/auth/wiring.py`]

Без настроенной аутентификации сервер работает в режиме разработки. Любой запрос обрабатывается как идентичность `anonymous`, которая сопоставляется со всеми настроенными ролями с доступом ко всем доменам (wildcard). (REQ-535)

**Вход (`POST /auth/login`)** предоставляется активным провайдером аутентификации, когда настроено `provider: basic`. (REQ-124) Формат учётных данных и ответ зависят от провайдера.

**Интроспекция идентичности:**

```http
GET /auth/me
```

Возвращает id аутентифицированного пользователя, email, отображаемое имя, членство в организациях и назначения ролей. В режиме разработки возвращает `dev_mode: true` со списком всех id ролей. [tool-verified: `provisa/api/auth_router.py`]

```http
GET /auth/provider-type
```

Возвращает `{"provider": "<name>"}` или `{"provider": null}`, если аутентификация не настроена. [tool-verified: `provisa/api/auth_router.py`]

---

## Эндпоинты данных

### `POST /data/graphql`

Выполнить GraphQL-запрос или мутацию. (REQ-043) [tool-verified: `provisa/api/data/endpoint.py:151`]

**Тело запроса:**

```json
{
  "query": "{ orders(where: {region: {eq: \"us\"}}) { id amount } }",
  "variables": {},
  "role": "admin",
  "extensions": {}
}
```

Поле `role` используется только в режиме разработки (без аутентификации). Когда аутентификация активна, используется роль аутентифицированного пользователя, а `role` в теле запроса игнорируется.

Поле `extensions` поддерживает протокол Automatic Persisted Query (APQ): (REQ-288)

```json
{
  "extensions": {"persistedQuery": {"sha256Hash": "<sha256-of-query>"}}
}
```

**Заголовки:**

- `X-Provisa-Role` — переопределить роль (режим разработки)
- `Accept` — формат ответа (см. раздел Согласование содержимого)
- `Authorization` — `Bearer <token>`, когда аутентификация включена
- `X-Provisa-Redirect-Format` — MIME-тип для вывода в формате S3-редиректа (REQ-137)
- `X-Provisa-Redirect-Threshold` — количество строк, выше которого срабатывает редирект (REQ-137)
- `X-Provisa-Redirect` — `true`, чтобы принудительно включить редирект безусловно (REQ-029)

**Ответ (JSON inline):**

```json
{
  "data": {
    "orders": [
      {"id": 1, "amount": 99.99}
    ]
  }
}
```

**Ответ (редирект):**

```json
{
  "data": {"orders": null},
  "redirect": {
    "redirect_url": "https://...",
    "row_count": 50000,
    "expires_in": 3600,
    "content_type": "application/vnd.apache.parquet"
  }
}
```

**Ответ (несколько корней со смешанным inline/редиректом):**

```json
{
  "data": {
    "orders": [{"id": 1}],
    "customers": null
  },
  "redirects": {
    "customers": {
      "redirect_url": "https://...",
      "row_count": 10000,
      "expires_in": 3600,
      "content_type": "application/vnd.apache.parquet"
    }
  }
}
```

Запросы с несколькими корневыми полями выполняют каждое корневое поле независимо. Поля ниже порога редиректа возвращаются inline; поля выше — редиректятся. Ключ `redirects` (во множественном числе) сопоставляет имена полей с информацией о редиректе. (REQ-029) [tool-verified: `provisa/api/data/endpoint.py`]

**Заголовки кеша:**

- `X-Provisa-Cache: HIT|MISS` (REQ-536)
- `X-Provisa-Cache-Age: <seconds>` (при HIT) (REQ-536)

**Требуемые возможности:** `QUERY_DEVELOPMENT` для всех запросов, включая интроспекцию. [tool-verified: `provisa/api/data/endpoint.py:186-283`]

---

### Согласование содержимого

| Заголовок Accept | Формат |
| --- | --- |
| `application/json` | JSON (по умолчанию) |
| `application/x-ndjson` | JSON с разделением строками |
| `text/csv` | CSV |
| `application/vnd.apache.parquet` | Parquet |
| `application/vnd.apache.arrow.stream` | Arrow IPC |

(REQ-047, REQ-048, REQ-049, REQ-050) [tool-verified: `provisa/api/data/endpoint.py:84-90`]

---

### Редирект

Результаты, превышающие настроенный порог строк (или когда `X-Provisa-Redirect: true`), записываются в S3, и возвращается предварительно подписанный URL. (REQ-029, REQ-044)

| Формат редиректа | Кем записывается | Память |
| --- | --- | --- |
| `application/vnd.apache.parquet` | федеративный CTAS | Не используется — данные никогда не проходят через Provisa |
| `application/x-orc` | федеративный CTAS | Не используется — данные никогда не проходят через Provisa |
| `application/json` | Provisa | Ограничено памятью |
| `application/x-ndjson` | Provisa | Ограничено памятью |
| `text/csv` | Provisa | Ограничено памятью |
| `application/vnd.apache.arrow.stream` | Provisa | Ограничено памятью |

Для крупных аналитических выгрузок используйте редирект в Parquet или ORC. Движок федерации пишет напрямую в S3 параллельно — данные не проходят через Provisa. (REQ-138)

```yaml
X-Provisa-Redirect-Format: application/vnd.apache.parquet
X-Provisa-Redirect-Threshold: 1000
```

---

### `POST /data/sql`

Выполнить необработанный SQL через конвейер governance Stage 2. (REQ-267) [tool-verified: `provisa/api/data/endpoint_dev.py:62`]

**Тело запроса:**

```json
{
  "sql": "SELECT id, amount FROM orders WHERE region = 'us'",
  "role": "admin"
}
```

**Требуемые возможности:** `QUERY_DEVELOPMENT`.

Нарушения governance на `POST /data/sql` возвращают HTTP 403. (REQ-002, REQ-266)

**Ответ:** тот же формат, что и `/data/graphql` (JSON-строки по умолчанию, согласование содержимого через `Accept`).

---

### `POST /data/query`

Унифицированный эндпоинт запросов. Принимает GraphQL, SQL или Cypher — синтаксис определяется автоматически. (REQ-267) [tool-verified: `provisa/api/data/endpoint_dev.py:509`]

Cypher-запросы также можно отправлять на выделенный только-для-Cypher эндпоинт `POST /query/cypher`. (REQ-345)

**Тело запроса:**

```json
{
  "query": "{ orders { id } }",
  "params": {},
  "variables": {},
  "role": "admin"
}
```

Возвращает `{"data": ...}` для GraphQL, `{"columns": [...], "rows": [...]}` для SQL и Cypher.

---

### `POST /data/sql/explain`

Выполнить explain или analyze для SQL-выражения через управляемый конвейер. (REQ-1519) [tool-verified: `provisa/api/data/endpoint_dev.py:328`]

Эндпоинт оборачивает **управляемый (governed)** SQL — выражение, которое фактически выполняется под ролью вызывающего, после применения RLS и маскирования — в синтаксис EXPLAIN соответствующего диалекта. То, что показывает план, — это авторизованная версия запроса, а не исходный ввод.

**Тело запроса:**

```json
{
  "sql": "SELECT id, amount FROM orders",
  "role": "admin",
  "analyze": false
}
```

Установите `analyze: true`, чтобы выполнить EXPLAIN ANALYZE. Запрос выполняется, и план содержит реальные количества строк и тайминги. Не все диалекты поддерживают ANALYZE; см. таблицу в разделе [Query plans and statistics](engines.md#query-plans-and-statistics).

**Ответ:** `{"plan": "<plan text or JSON>", "dialect": "trino", "analyzed": false}`

`400`, если диалект не поддерживает EXPLAIN, или запрошен `analyze: true` для диалекта, который его не поддерживает (например, SQLite). [tool-verified: `provisa/executor/explain.py:wrap_explain`, `analyze_sql`]

---

### `GET /data/engine/state`

Вернуть текущее состояние шарда движка, не пробуждая его. (REQ-1516) [tool-verified: `provisa/api/data/endpoint_dev.py:892`]

UI опрашивает этот эндпоинт, чтобы показать баннер запуска во время холодного старта движка. Он никогда не вызывает пробуждение — опрос безопасен и не засчитывается как активность для «сборщика простоя» (idle reaper).

**Ответ:**

```json
{"state": "ready"}
```

Возможные значения:

| Состояние | Значение |
| --- | --- |
| `always-on` | Desktop, self-hosted или собственный (BYO) координатор — без управления жизненным циклом |
| `ready` | Шард поднят и принимает запросы |
| `starting` | Идёт холодный старт |
| `stopped` | Шард масштабирован до нуля |

[tool-verified: `provisa/federation/engine_wake.py:engine_state`]

---

### `POST /data/engine/prewarm`

Инициировать пробуждение движка без выполнения запроса. (REQ-1516) [tool-verified: `provisa/api/data/endpoint_dev.py:913`]

Немедленно возвращает `202 Accepted`. Пробуждение выполняется в фоне. Используйте это, если хотите, чтобы движок был готов до прихода первого запроса — например, из планировщика, который выполнит запросы через несколько минут.

**Ответ:** `202 Accepted`, тело `{"started": true}`

[tool-verified: `provisa/federation/engine_wake.py:prewarm_engine`]

---

### `GET /data/rest/{domain_id}/{table_name}`

Автоматически сгенерированный обычный REST-эндпоинт для каждой зарегистрированной таблицы. Строка запроса сопоставляется с аргументами GraphQL, и запрос компилируется и выполняется через тот же конвейер (RLS, маскирование, маршрутизация), что и GraphQL. (REQ-256) [tool-verified: `provisa/api/rest/generator.py:153`]

**Параметры запроса:**

- `limit` — максимум строк (≥ 1)
- `offset` — пропустить строки (≥ 0)
- `fields` — имена колонок через запятую (по умолчанию — все скалярные поля)
- `filter` — JSON-массив объектов фильтра `{"field", "comparator", "value"}`
- `orderBy` — JSON-массив объектов сортировки `{"field", "direction"}`

Требуется аутентифицированная роль; неаутентифицированные запросы возвращают `401`. Спецификация OpenAPI для этих маршрутов доступна по адресу `GET /data/rest/openapi.json`, с Swagger UI по адресу `GET /data/rest/docs`.

#### OpenAPI / Swagger UI Explorer

Страница OpenAPI explorer (`/app/openapi`) встраивает Swagger UI в изолированный (sandboxed) iframe. Спецификация ограничена ролью — отображаются только таблицы и колонки, видимые текущей роли, — и опционально фильтруется по домену через селектор домена. UI автоматически переключается между светлой и тёмной темами. [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:20-34`]

Страница загружает HTML спецификации через `fetch()`, а не через прямой iframe `src`, поэтому запрос несёт bearer-токен сессии, а собственные относительные запросы Swagger UI корректно разрешаются относительно того же источника (origin). [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:44-69`]

При переходе по ссылке NL «Open in OpenAPI» страница автоматически разворачивает целевой эндпоинт, заполняет параметры запроса из URL, сгенерированного NL (например, `aggregate`, `groupBy`), и нажимает Execute — используя опрос DOM, чтобы каждый шаг завершался до срабатывания следующего. (REQ-1359) [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:94-171`]

---

### `GET /data/jsonapi/{domain_id}/{table_name}`

Автоматически сгенерированный эндпоинт, совместимый с [JSON:API](https://jsonapi.org), для каждой зарегистрированной таблицы. Те же RLS, маскирование и маршрутизация, что и в GraphQL. (REQ-257) [tool-verified: `provisa/api/jsonapi/generator.py:284`]

**Заголовок `Accept`:** должен включать `application/vnd.api+json` (медиатип JSON:API), иначе запрос вернёт `406`.

**Параметры запроса:**

- `fields[<type>]` — разреженные наборы полей (sparse fieldsets), например `?fields[orders]=amount`
- `filter[<col>]` / `filter[<col>][<op>]` — например `?filter[region]=US`, `?filter[amount][gt]=100`
- `sort` — через запятую, префикс `-` для убывания, например `?sort=-created_at,amount`
- `page[number]` / `page[size]` — пагинация
- `aggregate` — агрегатные функции через запятую, выполняемые вместо получения строк: `count`, `sum`, `avg`, `stddev`, `variance`, `min`, `max`. Используйте `?aggregate=count,sum`, чтобы запросить подмножество. Ответы с агрегатами возвращают `data: null`, а результаты — в `meta.aggregate`. (REQ-1359) [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:238`]
- `groupBy` — имена колонок через запятую; используется вместе с `?aggregate=` для группировки результатов. Допустимы только колонки из перечисления `DistinctOnColumn` таблицы; сервер возвращает `400` для любой колонки, которую роль не может видеть. (REQ-1361) [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:447`]
- `includeNodes` — `true`, чтобы включить скалярные колонки базовой таблицы (и скалярные поля присоединённых измерений, названных в `include=`) внутрь массива `nodes` каждой сгруппированной строки. Требуется, когда NL-запрос с группировкой также запрашивает детали измерений. (REQ-1405)

Ответы представляют собой объекты ресурсов с `type`/`id`/`attributes`. Ошибки следуют форме объекта ошибки JSON:API.

#### JSON:API Explorer

Страница JSON:API explorer (`/app/jsonapi`) — это браузерный UI поверх этих эндпоинтов. Выберите таблицу из списка, сгруппированного по доменам, затем настройте:

- **Fields** — выберите, какие колонки включить (разреженный набор полей); оставьте всё неотмеченным, чтобы запросить все колонки
- **Relationships** — выберите имена связей, выведенных из FK, для подгрузки (sideload) через `?include=`
- **Filter** — поле, оператор (`eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `like`) и значение
- **Sort** — одно поле, по возрастанию или убыванию
- **Aggregate** — выберите колонки группировки из списка, проверенного сервером, затем отметьте одну или несколько агрегатных функций; при выборе колонок группировки появляется флажок «Include nodes», добавляющий скалярные колонки базовой таблицы к каждой строке
- **Page size** — количество ресурсов на страницу с навигацией first/prev/next/last

Результаты отображаются либо в форматированном сводном виде (карточки ресурсов с кликабельными якорями связей), либо на вкладке необработанного JSON. Показывается живой URL запроса, который можно скопировать. Выбор таблицы и размер страницы сохраняются между сессиями в `localStorage`. [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx`]

При переходе по ссылке NL «Open in JSON:API» explorer предварительно выбирает таблицу и заполняет средство выбора агрегатов из параметров запроса, сгенерированных NL, затем автоматически выполняет запрос. [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:460-479`]

---

### `POST /query/nl`

Отправить вопрос на естественном языке. Сервис запускает асинхронную задачу и немедленно возвращает `202 Accepted` с `job_id`. Требует настроенного LLM-провайдера в разделе конфигурации `ai_models`. (REQ-354) [tool-verified: `provisa/api/rest/nl_router.py:50`]

**Тело запроса:**

```json
{"q": "How many orders were placed last month?", "role": "admin"}
```

Возвращает `{"job_id": "<id>"}`. Превышение лимита частоты NL-запросов для роли возвращает `429` с заголовком `Retry-After`. (REQ-370)

**Получить результат:**

- `GET /query/nl/{job_id}` — опрос. Возвращает документ задачи.
- `GET /query/nl/{job_id}/stream` — SSE. Одно событие `branch` на каждую цель генерации по мере её завершения, затем событие `done`. (REQ-357, REQ-358)

Три цикла генерации (Cypher, GraphQL, SQL) выполняются параллельно, каждый проверяется компилятором и уточняется при ошибке. (REQ-355) Промпт ограничен видимой схемой роли. (REQ-356) Итоговый документ ключует каждую ветвь по цели: (REQ-357) [tool-verified: `provisa/nl/job.py:69`]

```json
{
  "job_id": "<id>",
  "state": "complete",
  "branches": {
    "cypher":  {"query": "MATCH ...", "result": [...], "error": null},
    "graphql": {"query": "{ ... }",   "result": {...}, "error": null},
    "sql":     {"query": "SELECT ...", "result": [...], "error": null}
  }
}
```

Ветвь, исчерпавшая лимит итераций, возвращает `query: null`, `result: null` и строку `error`. Каждый сгенерированный запрос выполняется под правами потребителя с применением governance Stage 2 — сервис никогда не обходит governance. (REQ-359)

#### NL Group-By с деталями измерений (REQ-1405)

Когда NL-запрос с группировкой также проецирует колонки из присоединённой таблицы измерения — например, «количество обращений по пользователю с именем пользователя и email» — исполнитель выводит поэлевые dot-пути (`dim_paths`) из колонок измерения, спроецированных в SELECT. Эти пути заполняют параметр `includeNodes=` в сгенерированных URL панелей JSON:API и OpenAPI, так что эти панели запрашивают те же поля присоединённого измерения, которые разрешили ветви SQL и GraphQL. Без этого `includeNodes=true` возвращал бы только собственные скалярные поля базовой агрегатной таблицы. (REQ-1405) [tool-verified: `docs/arch/requirements.md:REQ-1405`]

На панели gRPC сгенерированный `{Type}GroupByRequest` несёт `include_nodes` (bool) и `include` (повторяющуюся строку с именами полей связей). Возвращаемый `{Type}GroupByRow` включает типизированное поле `nodes` со строками деталей измерения. [tool-verified: `provisa/grpc/query_ir.py:168-196`]

---

### `GET /data/sdl`

Вернуть GraphQL SDL для схемы роли. (REQ-008) [tool-verified: `provisa/api/data/sdl.py:137`]

**Заголовки:** `X-Role: <role_id>` (обязателен)

**Параметры запроса:**

- `domain` — id доменов через запятую. При указании ответ фильтруется по названным домену(ам) и таблицам, достижимым из них.

**Ответ:** GraphQL SDL в `text/plain`.

---

### `GET /data/introspection`

Вернуть JSON интроспекции GraphQL, опционально отфильтрованный по домену. [tool-verified: `provisa/api/data/sdl.py:200`]

**Заголовки:** `X-Provisa-Role: <role_id>` (обязателен)

**Параметры запроса:** `domain` — id доменов через запятую.

**Ответ:** результат интроспекции в `application/json`.

---

### `GET /data/graph-schema`

Вернуть графовое представление схемы роли: метки узлов и их типы связей, для клиентов Cypher/graph. Включает `pk_columns` для каждой метки узла, чтобы вызывающие могли определить колонки первичного ключа. (REQ-398) [tool-verified: `provisa/api/rest/cypher_router.py:689`]

**Ответ:** `application/json` с `node_labels` (каждый несёт `pk`/`pk_columns`) и `relationship_types`.

Каждый тип связи также несёт `junction_table_name` и `properties` (REQ-1586). На ребре, опирающемся на junction-таблицу, первое поле называет ассоциативную таблицу, по которой проходит обход, а второе перечисляет колонки этой таблицы, доступные для чтения как `r.attr` и для фильтрации в `WHERE`; на ребре, опирающемся на внешний ключ, имя равно `null`, а список свойств пуст — именно так клиент отличает одно от другого. Сама junction-таблица никогда не является меткой узла — она является ребром, поэтому у неё нет «пилюли» в graph-клиенте и нет строки в `node_labels`. [tool-verified: `provisa/api/rest/cypher_router.py:797-805`, `provisa/cypher/label_map.py:378-397`]

---

### `GET /data/domains`

Вернуть id доменов, доступных запрашивающей роли. [tool-verified: `provisa/api/data/sdl.py:116`]

**Заголовки:** `X-Role: <role_id>` (обязателен)

**Ответ:** `["sales", "support", ...]`

---

### `GET /data/schema-version`

Вернуть строку текущей версии схемы. Объединяет одноразовый nonce на загрузку со счётчиком пересборок. Клиенты используют это для инвалидации кешей схемы после перезапусков сервера. (REQ-537) [tool-verified: `provisa/api/data/sdl.py:102`]

**Ответ:** `{"version": "<boot-id>-<counter>"}`

---

### `GET /data/proto/{role_id}`

Вернуть автоматически сгенерированный файл `.proto` для роли. [tool-verified: `provisa/api/data/endpoint_dev.py:49`]

**Ответ:** protobuf-схема в `text/plain`.

Каждая зарегистрированная таблица порождает `message` в proto. Связи порождают вложенные поля-сообщения. Сопоставление типов: `integer → int32`, `bigint → int64`, `varchar → string`, `decimal → double`, `boolean → bool`, `timestamp → google.protobuf.Timestamp`. (REQ-538)

---

### `GET /data/subscribe/{table}`

Поток Server-Sent Events для уведомлений об изменениях в реальном времени от таблицы. (REQ-219, REQ-258) [tool-verified: `provisa/api/data/subscribe.py:239`]

Доставка уведомлений использует подключаемого провайдера, выбираемого по типу источника: источники PostgreSQL используют `LISTEN/NOTIFY` (через asyncpg), источники MongoDB используют Change Streams (`collection.watch()`), а источники Kafka используют consumer group. Каждый провайдер реализует общий асинхронный интерфейс наблюдения (watch). Фильтрация RLS и проверка схемы применяются независимо от провайдера. (REQ-258) Источники WebSocket и RSS также поддерживаются. (REQ-338, REQ-342)

**Заголовок — `X-Provisa-Sink`:** установите Kafka-цель (например, `kafka://broker:9092/topic`), чтобы перенаправить события изменений в приёмник (sink) Kafka вместо SSE-ответа. Сервер запускает потребитель-приёмник и возвращает `202 Accepted` вместо открытого потока. (REQ-812) [tool-verified: `provisa/api/data/subscription_sse.py:137`]

---

## Административные REST-эндпоинты

### Config

#### `GET /admin/config`

Скачать текущий `provisa.yaml` как `application/x-yaml` с заголовком `Content-Disposition: attachment`. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:19`]

#### `PUT /admin/config`

Загрузить обновлённый YAML конфигурации. Сервер записывает резервную копию `.bak`, сохраняет новый файл и перезагружает все схемы, источники и материализованные представления. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:32`]

**Тело запроса:** необработанное содержимое YAML.

**Ответ:**

```json
{"success": true, "message": "Config uploaded and reloaded"}
```

При ошибке перезагрузки: `{"success": false, "message": "<error>"}`.

#### `GET /admin/config/live`

Скачать **текущую живую конфигурацию** — конфигурацию, которую Provisa записала бы сегодня, отражающую каждую таблицу, связь, домен, роль и правило RLS, созданные администратором и накопившиеся с момента запуска. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:67`]

Файл на диске может отставать от живого состояния, если изменения вносились через admin API без последующей загрузки. Этот эндпоинт закрывает этот разрыв: его вывод — это то, что `PUT /admin/config` должен получить, чтобы файл на диске совпал с живым состоянием.

Возвращает `application/x-yaml` с `Content-Disposition: attachment; filename=provisa.live.yaml`.

#### `GET /admin/config/diff`

Вернуть обе стороны диффа конфигурации — `original` (базовую линию при запуске) и `current` (живое состояние) — нормализованные одинаково, так что сравнение показывает только реальные изменения, а не переупорядочивание или дрейф комментариев. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:82`]

**Ответ:**

```json
{"original": "<yaml>", "current": "<yaml>"}
```

#### `POST /admin/config/patch`

Сгенерировать unified-diff-патч от базовой линии к отправленной конфигурации. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:93`]

Отправьте обновлённый YAML в теле запроса. Ответ — файл `text/x-patch` (`provisa.config.patch`), который `git apply` или `patch` могут применить напрямую — полезно для коммита изменений конфигурации, сделанных через UI, в CI/CD-конвейер.

---

### Settings

#### `GET /admin/settings`

Вернуть текущие настройки платформы в виде JSON. (REQ-165) [tool-verified: `provisa/api/admin/settings_router.py:50`]

**Ответ:**

```json
{
  "redirect": {
    "enabled": true,
    "threshold": 10000,
    "default_format": "application/vnd.apache.parquet",
    "ttl": 3600
  },
  "sampling": {
    "default_sample_size": 1000
  },
  "cache": {
    "default_ttl": 300
  },
  "naming": {
    "domain_prefix": false,
    "convention": "apollo_graphql"
  },
  "relationships": {
    "auto_track_fk": true
  },
  "otel": {
    "endpoint": "http://otel-collector:4318",
    "service_name": "provisa",
    "sample_rate": 1.0,
    "support_endpoint": "",
    "support_redact_sql_literals": true,
    "support_redact_attributes": []
  }
}
```

#### `PUT /admin/settings`

Обновить настройки платформы во время выполнения. Все поля необязательны — обновляются только ключи, присутствующие в теле запроса. (REQ-165) [tool-verified: `provisa/api/admin/settings_router.py:100`]

**Тело запроса (частичный пример):**

```json
{
  "otel": {
    "support_endpoint": "https://telemetry.vendor.com/v1/traces",
    "support_redact_sql_literals": true,
    "support_redact_attributes": ["db.statement", "user.email"]
  },
  "cache": {"default_ttl": 600}
}
```

Обновляемые поля по разделам:

- `redirect`: `enabled`, `threshold`, `default_format`, `ttl`
- `sampling`: `default_sample_size`
- `cache`: `default_ttl`
- `naming`: `domain_prefix`, `convention` — записывается в файл конфигурации и запускает перезагрузку схемы (REQ-253)
- `relationships`: `auto_track_fk` — управляет только отслеживанием внешних ключей. Связь, опирающаяся на junction-таблицу, объявляется при регистрации таблицы и никогда не выводится автоматически, поэтому эта настройка на неё не влияет. (REQ-1586)
- `otel`: `endpoint`, `service_name`, `sample_rate`, `support_endpoint`, `support_redact_sql_literals`, `support_redact_attributes`

**Ответ:**

```json
{"success": true, "updated": ["otel.support_endpoint", "cache.default_ttl"]}
```

---

### AI Models

#### `GET /admin/ai-models`

Вернуть назначения AI-моделей действующей организации, реестр векторных моделей и лимит частоты NL-запросов. (REQ-464, REQ-1349) [tool-verified: `provisa/api/admin/ai_models_router.py:58`]

**Ответ:**

```json
{
  "ai_models": {
    "nl": "claude-3-5-sonnet-20241022",
    "embedding": "text-embedding-3-small"
  },
  "vector_models": [...],
  "nl": {"rate_limit": 20},
  "api_keys_set": {"anthropic": true, "openai": false}
}
```

API-ключи никогда не возвращаются обратно — `api_keys_set` сообщает только о том, настроен ли ключ для каждого поставщика. Изменения вступают в силу при следующем запросе; перезапуск не требуется. (REQ-1349)

#### `PUT /admin/ai-models`

Обновить назначения AI-моделей организации, реестр векторных моделей или лимит частоты NL-запросов. Вступает в силу при следующем запросе. [tool-verified: `provisa/api/admin/ai_models_router.py:148`]

#### `GET /admin/ai-models/vendors/{vendor}/models`

Вернуть имена моделей, которые в настоящее время предоставляет поставщик, для средства выбора модели. (REQ-1395, REQ-1398, REQ-1409) [tool-verified: `provisa/api/admin/ai_models_router.py:89`]

Список читается вживую из собственного API list-models поставщика с использованием ключа, настроенного организацией, — или учётных данных развёртывания, если ключ организации не задан. Модель, выпущенная после сборки этой версии, доступна для выбора в тот же день, когда её начинает предоставлять поставщик.

Возвращает `400`, если поставщик не публикует API list-models (в этом случае введите имя модели напрямую) или если нет доступного ключа. [tool-verified: `provisa/api/admin/ai_models_router.py:109-128`]

---

### Federation Engine

#### `GET /admin/federation-engine`

Вернуть текущий выбор движка федерации, его конфигурацию подключения и полный реестр доступных для выбора движков. (REQ-916) [tool-verified: `provisa/api/admin/settings_router.py:730`]

**Ответ:**

```json
{
  "current": "trino",
  "persisted": "trino",
  "registry": [
    {"key": "trino", "label": "Trino (embedded)", "fields": [...]},
    {"key": "duckdb", "label": "DuckDB", "fields": []}
  ],
  "note": "Changing the federation engine takes effect after the service is restarted."
}
```

Ключ `current` — это движок, работающий прямо сейчас; `persisted` — то, что записано в файл конфигурации и будет загружено при следующем перезапуске. Они расходятся, когда конфигурация была изменена, но сервис ещё не перезапускался.

#### `PUT /admin/federation-engine`

Сохранить выбор движка федерации. (REQ-916) [tool-verified: `provisa/api/admin/settings_router.py:774`]

**Тело запроса:**

```json
{"engine": "trino", "federation_engine_url": "http://trino-coordinator:8080"}
```

Выбор записывается в конфигурацию платформы. Вступает в силу после следующего перезапуска сервиса — движок выбирается один раз при загрузке.

---

### Domain Policy

#### `POST /admin/domain-policy`

Изменить политику доменов действующей организации (`use_domains` / `default_domain`). (REQ-165, REQ-1266, REQ-1349) [tool-verified: `provisa/api/admin/settings_router.py:632`]

Это деструктивная операция, ограниченная действующей организацией. Каждый зарегистрированный источник, таблица, домен и связь очищаются и перестраиваются под новую политику. Используйте это при переключении организации между namespaced-по-доменам и плоской (или наоборот) структурами.

**Тело запроса:**

```json
{
  "use_domains": true,
  "default_domain": "default"
}
```

`use_domains: null` сбрасывает переопределение организации и возвращается к настройке уровня развёртывания. `use_domains: false` требует `default_domain` (единственное имя домена, куда попадают все таблицы). Пересборка каталога синхронна; ответ возвращается, когда схемы готовы.

---

### Observability

#### `GET /admin/traces/recent`

Вернуть до N последних завершённых спанов из буфера спанов в памяти. (REQ-302) [tool-verified: `provisa/api/admin/settings_router.py:317`]

**Параметры запроса:** `limit` (по умолчанию 50, максимум 200)

**Ответ:** `{"traces": [...]}`

#### `POST /admin/query-engine/reload-catalog`

Горячая перезагрузка именованного каталога в координаторе движка федерации через его REST API. Переподключает внутреннее соединение Provisa и заново выполняет DDL OTel. [tool-verified: `provisa/api/admin/settings_router.py:208`]

**Параметры запроса:** `catalog` (по умолчанию `"otel"`)

**Ответ:**

```json
{"success": true, "errors": []}
```

#### `POST /admin/query-engine/restart`

Перезапустить контейнер движка федерации (только для однонодовой разработки). [tool-verified: `provisa/api/admin/settings_router.py:287`]

**Параметры запроса:** `container` (по умолчанию берётся из переменной окружения `QUERY_ENGINE_CONTAINER`, затем `"trino"`)

---

### Discovery

#### `POST /admin/discover/relationships`

Запустить обнаружение связей. Всегда выполняет интроспекцию FK из движка федерации. (REQ-018) Запускает вывод LLM, если задан `ANTHROPIC_API_KEY`. (REQ-167) [tool-verified: `provisa/api/admin/discovery.py:55`]

**Тело запроса:**

```json
{
  "scope": "domain",
  "domain_id": "sales"
}
```

`scope` должен быть одним из `"table"`, `"domain"`, `"cross-domain"`. Для области `"table"` требуется `table_id` (целое число). Для области `"domain"` требуется `domain_id`.

**Ответ:** `{"candidates_found": 12, "stored_ids": [1, 2, 3, ...]}`

#### `GET /admin/discover/candidates`

Список ожидающих кандидатов на связи. [tool-verified: `provisa/api/admin/discovery.py:96`]

#### `POST /admin/discover/candidates/{candidate_id}/accept`

Принять кандидата и зарегистрировать его как связь. [tool-verified: `provisa/api/admin/discovery.py:103`]

**Тело запроса (необязательно):** `{"name": "custom-relationship-name"}`

#### `POST /admin/discover/candidates/{candidate_id}/reject`

Отклонить кандидата. [tool-verified: `provisa/api/admin/discovery.py:110`]

**Тело запроса:** `{"reason": "Not a real join"}`

#### `GET /admin/discover/candidates/rejected/count`

Вернуть количество отклонённых кандидатов. [tool-verified: `provisa/api/admin/discovery.py:118`]

#### `DELETE /admin/discover/candidates/rejected`

Удалить всех отклонённых кандидатов. [tool-verified: `provisa/api/admin/discovery.py:128`]

---

### Source Crawl

#### `POST /admin/sources/crawl`

Просканировать источник данных для интроспекции его схемы и регистрации таблиц. (REQ-012) [tool-verified: `provisa/api/admin/crawl_router.py:36`]

---

### Source Table Search

#### `GET /admin/sources/{source_id}/tables/search`

Найти доступные (ещё не зарегистрированные) таблицы в источнике по имени. [tool-verified: `provisa/api/admin/table_search_router.py:103`]

---

### Table Profiling

#### `POST /admin/tables/{table_id}/profile`

Выполнить профилирование колонок для зарегистрированной таблицы — кардинальность, min/max, доли null. [tool-verified: `provisa/api/admin/table_profile_router.py:28`]

---

### Source Descriptions

#### `POST /admin/source-meta/db-description`

Сгенерировать описания таблиц и колонок источника с помощью LLM. [tool-verified: `provisa/api/admin/source_meta_router.py:48`]

---

### Object Storage (REQ-1046, REQ-1048, REQ-1049)

#### `GET /admin/org-storage`

Сообщить о занимаемом объёме хранилища действующей организации относительно её платформенной квоты, а также о том, зарегистрировала ли организация собственное хранилище. [tool-verified: `provisa/api/admin/org_storage_router.py:69`]

Когда организация зарегистрировала собственную DSN, её материализации направляются туда и больше не учитываются в квоте. Сама DSN никогда не возвращается.

#### `PUT /admin/org-storage`

Зарегистрировать (или очистить) собственное хранилище материализаций организации. [tool-verified: `provisa/api/admin/org_storage_router.py:81`]

**Тело запроса:**

```json
{"storage_url": "s3://my-bucket/provisa?region=us-east-1&access_key=..."}
```

DSN проверяется движком федерации перед принятием — непригодная DSN не пройдёт при регистрации, а не спустя часы, во время обновления. Значение шифруется при хранении и никогда не возвращается через GET.

Отправьте `storage_url: null`, чтобы очистить собственное хранилище организации и вернуть её материализации в платформенное хранилище (и квоту). Рантайм организации пересобирается в рамках того же вызова, поэтому новое хранилище становится действующим немедленно. [tool-verified: `provisa/api/admin/org_storage_router.py:123-138`]

---

### Org Encryption (REQ-1574)

#### `GET /admin/org-encryption`

Вернуть текущий статус ключа организации: отпечаток, id и происхождение. Никогда не возвращает материал ключа. [tool-verified: `provisa/api/admin/org_encryption_router.py:53`]

Когда организация не задала ключ, возвращает `{"configured": false}`. Каждая организация начинает в этом состоянии и наследует ключ развёртывания.

#### `PUT /admin/org-encryption`

Задать или ротировать ключ шифрования организации для данных в состоянии покоя. [tool-verified: `provisa/api/admin/org_encryption_router.py:68`]

**Тело запроса:**

```json
{"key_b64": "<32 raw bytes, base64-encoded>"}
```

Опустите `key_b64`, чтобы Provisa сгенерировала ключ, — самый безопасный путь, поскольку ключ никогда не появляется в буфере обмена или журнале запросов. Указание `key_b64` означает использование собственного ключа.

Ротация добавляет новую активную запись в кольцо ключей и сохраняет старую, поэтому данные, записанные под предыдущим ключом, остаются читаемыми. Ротация — это не перешифрование. Эндпоинта удаления не существует: удаление последнего ключа сделало бы каждую упакованную (wrapped) полезную нагрузку нечитаемой. [tool-verified: `provisa/api/admin/org_encryption_router.py:75`]

Живое кольцо ключей перепривязывается в рамках того же вызова, поэтому следующая зашифрованная запись немедленно использует новый ключ.

---

### Hasura / DDN Import (REQ-1483)

#### `POST /admin/import/hasura/preview`

Преобразовать архив проекта Hasura v2 или DDN в предлагаемую конфигурацию Provisa, ничего не записывая. [tool-verified: `provisa/api/admin/import_router.py`]

**Тело запроса:**

```json
{
  "filename": "my-project.zip",
  "content_b64": "<base64-encoded archive>",
  "flavor": "auto",
  "domain_map": {"public": "sales"},
  "source_overrides": {}
}
```

`flavor` принимает значения `"auto"` (определяется по структуре архива), `"hasura_v2"` или `"ddn"`.

**Ответ:**

```json
{
  "config_yaml": "...",
  "warnings": ["..."],
  "summary": {
    "sources": 1, "domains": 2, "tables": 40,
    "columns": 180, "roles": 3, "relationships": 15, "rls_rules": 6
  }
}
```

Ничего не сохраняется. Предпросмотр не кешируется на стороне сервера; `apply` берёт YAML, который вы передаёте, поэтому применяется именно то, что было проверено (и, возможно, отредактировано).

#### `POST /admin/import/hasura/apply`

Загрузить ранее предпросмотренную конфигурацию в действующую организацию. [tool-verified: `provisa/api/admin/import_router.py`]

**Тело запроса:**

```json
{"config_yaml": "<yaml string>"}
```

Использует тот же путь горячей перезагрузки, что и `PUT /admin/config`. Каталог, схемы и пулы организации пересобираются до возврата ответа.

---

### Apache Ossie Interchange (REQ-1316, REQ-1321)

#### `GET /admin/ossie`

Экспортировать управляемую модель организации в виде документа Apache Ossie (incubating) YAML. (REQ-1321) [tool-verified: `provisa/api/admin/ossie_router.py`]

Документ формируется из живого состояния при каждом запросе — никогда не кешируется, — поэтому не может быть устаревшим. Таблицы становятся объектами `dataset`, колонки становятся объектами `field`, а связи сопоставляются с объектами `relationship` Ossie.

Возвращает `text/yaml` с `Content-Disposition: attachment; filename=provisa-ossie.yaml`.

#### `POST /admin/ossie/import`

Разобрать документ Ossie в формате YAML или JSON и вернуть предлагаемые регистрации таблиц и связей. (REQ-1316) [tool-verified: `provisa/api/admin/ossie_router.py`]

**Тело запроса:** необработанный Ossie YAML или JSON. Формат определяется автоматически.

**Ответ:**

```json
{
  "proposals": {
    "tables": [...],
    "relationships": [...]
  }
}
```

Ничего не регистрируется. Используйте экран проверки административного UI, чтобы принять или урезать предложения до срабатывания какой-либо мутации.

---

### Actions (Functions and Webhooks)

Все эндпоинты находятся под префиксом `/admin/actions`. (REQ-205) [tool-verified: `provisa/api/admin/actions_router.py:24`]

Каждый вызов — из GraphQL, SQL, Cypher, Bolt, Arrow Flight, MCP `run_sql` и Provisa gRPC — проходит через единый управляемый исполнитель, который единообразно применяет `writable_by` и governance. (REQ-1156) [tool-verified: `provisa/api/data/action_exec.py`] См. [docs/integrations.md](integrations.md#invoking-commands-across-protocols) для синтаксиса вызова по каждому протоколу.

#### `GET /admin/actions`

Вернуть все отслеживаемые функции БД и вебхуки. (REQ-242) [tool-verified: `provisa/api/admin/actions_router.py:104`]

**Ответ:**

```json
{
  "functions": [
    {
      "name": "random_python_set",
      "implKind": "python",
      "binding": {"callable": "demo.py_functions:random_dataset"},
      "returns": "",
      "returnSchema": {
        "type": "array",
        "items": {"type": "object", "properties": {"id": {"type": "integer"}, "region": {"type": "string"}}}
      },
      "arguments": [{"name": "rows", "type": "Int"}, {"name": "seed", "type": "Int"}],
      "visibleTo": ["admin"],
      "writableBy": [],
      "domainId": "pet-store",
      "description": "Demo Python command returning random rows",
      "kind": "query"
    }
  ],
  "webhooks": [
    {
      "name": "add-pet",
      "url": "https://petstore.example.com/pets",
      "method": "POST",
      "kind": "mutation",
      "approved": true
    }
  ]
}
```

Каждый объект вебхука несёт булево поле `approved`. Вебхук становится одобренным, как только стюард выполняет запрос на его создание (REQ-209); вебхуки, объявленные в конфигурации, одобряются автоматически. Неодобренный вебхук регистрируется, но не раскрывается ни на одной поверхности. [tool-verified: `provisa/api/admin/actions_router.py:124-131`]

#### `POST /admin/actions/functions`

Зарегистрировать отслеживаемую функцию (команду). (REQ-205) [tool-verified: `provisa/api/admin/actions_router.py:117`]

**Ключевые поля:**

| Поле | Обязательно | Описание |
| --- | --- | --- |
| `name` | Да | Уникальное имя команды |
| `kind` | Да | `"query"` → поле GraphQL Query; `"mutation"` → поле Mutation |
| `implKind` | Нет | Как выполняется команда — см. таблицу ниже (по умолчанию `source_procedure`) |
| `binding` | Нет | Специфичные для `implKind` детали подключения (JSON-объект) |
| `returnSchema` | Нет | JSON Schema `{type:"array", items:{type:"object", properties:{...}}}` — делает команду возвращающей множество на любой поверхности |
| `arguments` | Нет | Определения аргументов `[{name, type}]`; порядок по позиции важен для вызывающих SQL и Bolt |
| `visibleTo` | Нет | Id ролей, которые могут вызывать команду |
| `writableBy` | Нет | Id ролей, которым разрешено вызывать её как мутацию |
| `domainId` | Нет | Домен для размещения в GraphQL и контроля доступа |

**Значения `implKind`:**

| `implKind` | Что выполняется | Поля `binding` |
| --- | --- | --- |
| `source_procedure` | Хранимая процедура на зарегистрированном источнике (по умолчанию) | `sourceId`, `schemaName`, `functionName` |
| `script` | Скрипт на стороне сервера | `script` |
| `http` | Исходящий HTTP-вызов | `url`, `method` |
| `grpc` | Исходящий gRPC-вызов на внешний сервер | `target`, `method` |
| `python` | Python-вызываемый объект, размещённый в Provisa (REQ-885) | `callable` (например, `"demo.py_functions:random_dataset"`) |

Демонстрационные команды `random_python_set` (`implKind: python`) и `random_grpc_set` (`implKind: grpc`) на практике показывают команды, возвращающие множество, с `returnSchema`; обе находятся в `config/provisa-install.yaml`. [tool-verified: `config/provisa-install.yaml:809-856`]

#### `PUT /admin/actions/functions/{name}`

Обновить отслеживаемую функцию по имени. [tool-verified: `provisa/api/admin/actions_router.py:182`]

#### `DELETE /admin/actions/functions/{name}`

Удалить отслеживаемую функцию по имени. [tool-verified: `provisa/api/admin/actions_router.py:233`]

#### `POST /admin/actions/webhooks`

Зарегистрировать отслеживаемый вебхук. (REQ-209) Регистрация или обновление вебхука ставит в очередь запрос на одобрение стюардом — вебхук становится активным на всех поверхностях только после одобрения стюардом. Вебхуки, объявленные в конфигурации, одобряются автоматически. **Поля тела запроса:** `name`, `url`, `method`, `timeoutMs`, `returns`, `inlineReturnType`, `arguments`, `visibleTo`, `domainId`, `description`, `kind`. [tool-verified: `provisa/api/admin/actions_router.py:132`, `provisa/api/admin/actions_router.py:325-331`]

#### `PUT /admin/actions/webhooks/{name}`

Обновить отслеживаемый вебхук по имени. Любое изменение сбрасывает одобрение обратно в статус ожидания до повторного одобрения. [tool-verified: `provisa/api/admin/actions_router.py:306`]

#### `DELETE /admin/actions/webhooks/{name}`

Удалить отслеживаемый вебхук по имени. [tool-verified: `provisa/api/admin/actions_router.py:355`]

#### `POST /admin/actions/test`

Протестировать действие (функцию или вебхук) по имени. (REQ-245) [tool-verified: `provisa/api/admin/actions_router.py:384`]

---

### Roles

Все эндпоинты находятся под префиксом `/admin/roles`. [tool-verified: `provisa/api/admin/roles_router.py:18`]

| Метод | Путь | Описание |
| --- | --- | --- |
| `GET` | `/admin/roles/` | Список всех ролей |
| `POST` | `/admin/roles/` | Создать роль |
| `PUT` | `/admin/roles/{role_id}` | Обновить роль |
| `DELETE` | `/admin/roles/{role_id}` | Удалить роль |

[tool-verified: `provisa/api/admin/roles_router.py`]

---

### Users

Все эндпоинты находятся под префиксом `/admin/users`. [tool-verified: `provisa/api/admin/local_users_router.py:21`]

| Метод | Путь | Описание |
| --- | --- | --- |
| `POST` | `/admin/users/` | Создать локального пользователя |
| `GET` | `/admin/users/` | Список локальных пользователей |
| `GET` | `/admin/users/{user_id}` | Получить пользователя |
| `PUT` | `/admin/users/{user_id}` | Обновить пользователя |
| `PATCH` | `/admin/users/{user_id}/password` | Изменить пароль |
| `DELETE` | `/admin/users/{user_id}` | Удалить пользователя |
| `GET` | `/admin/users/{user_id}/assignments` | Список назначений ролей |
| `POST` | `/admin/users/{user_id}/assignments` | Добавить назначение роли |
| `DELETE` | `/admin/users/{user_id}/assignments/{assignment_id}` | Удалить назначение роли |

---

### Organizations

Все эндпоинты находятся под `/admin/orgs`. [tool-verified: `provisa/api/admin/orgs_router.py:18`]

| Метод | Путь | Описание |
| --- | --- | --- |
| `GET` | `/admin/orgs/` | Список организаций |
| `POST` | `/admin/orgs/` | Создать организацию |
| `PUT` | `/admin/orgs/{org_id}` | Обновить организацию |
| `DELETE` | `/admin/orgs/{org_id}` | Удалить организацию |
| `GET` | `/admin/orgs/{org_id}/members` | Список участников |
| `POST` | `/admin/orgs/{org_id}/members` | Добавить участника |
| `DELETE` | `/admin/orgs/{org_id}/members/{user_id}` | Удалить участника |

---

### Invites

Все эндпоинты находятся под `/admin/invites`. [tool-verified: `provisa/api/admin/invites_router.py:18`]

| Метод | Путь | Описание |
| --- | --- | --- |
| `POST` | `/admin/invites/` | Создать приглашение |
| `GET` | `/admin/invites/` | Список ожидающих приглашений |
| `DELETE` | `/admin/invites/{token}` | Отозвать приглашение |

---

### Admin GraphQL

#### `POST /admin/graphql`

Эндпоинт Strawberry GraphQL для всех административных операций: CRUD источников и таблиц, управление связями, конфигурация доменов, правила RLS, управление кешем, соглашения об именовании, управление плановыми задачами и компиляция запросов. (REQ-164) [tool-verified: `provisa/api/app.py:2171`]

**Ключевые мутации:**

```graphql
# Cache
mutation { update_source_cache(source_id: "sales-pg", enabled: true, ttl: 600) { success } }
mutation { update_table_cache(table_id: 1, ttl: 60) { success } }

# Naming conventions
mutation { update_source_naming(source_id: "legacy-db", convention: "camelCase") { success } }
mutation { update_table_naming(table_id: 1, convention: "PascalCase") { success } }

# Scheduled tasks
mutation { toggle_scheduled_task(name: "daily-report", enabled: false) { success } }

# Compile a query (returns enforcement metadata and routed SQL)
mutation {
  compile_query(input: {role: "admin", query: "{ orders { id } }"}) {
    sql semantic_sql trino_sql direct_sql route route_reason sources root_field
    enforcement { rls_filters_applied columns_excluded masking_applied }
  }
}
```

[tool-verified: `provisa/api/admin/schema.py`, `provisa/api/admin/actions_router.py`]

---

### Setup

#### `GET /setup/status`

Вернуть статус первоначальной настройки. Всегда неаутентифицирован. (REQ-539) [tool-verified: `provisa/api/setup_router.py:100`]

#### `POST /setup/`

Завершить первоначальную настройку. [tool-verified: `provisa/api/setup_router.py:142`]

---

## Health Check

#### `GET /health` или `HEAD /health`

Возвращает `{"status": "ok"}`. Всегда неаутентифицирован. (REQ-539) [tool-verified: `provisa/api/app.py:2258`]

---

## Ответы с ошибками

| Статус | Значение |
| --- | --- |
| 400 | Некорректный запрос, ошибка валидации или ошибка разбора SQL |
| 401 | Отсутствующий или недействительный токен аутентификации |
| 403 | Недостаточно возможностей; нарушение governance |
| 404 | Роль, ресурс или файл конфигурации не найдены |
| 422 | Отсутствует обязательный заголовок (например, `X-Role`) |
| 503 | База данных или источник не подключены; зависимость недоступна |
| 504 | Истекло время ожидания запроса |

Нарушения governance на `POST /data/sql` возвращают HTTP 403 со структурированным телом: (REQ-002) [tool-verified: `provisa/api/data/endpoint_dev.py:184-190`]

```json
{
  "detail": {
    "violations": [
      {"code": "V000", "message": "Table 'orders' is not accessible for role 'analyst'"}
    ]
  }
}
```

Все прочие ошибки используют: `{"detail": "<message>"}`.

---

## Эндпоинт Arrow Flight

Порт `8815`. Нативный колоночный транспорт Arrow поверх gRPC. (REQ-143, REQ-045) [tool-verified: `provisa/api/flight/server.py`]

Запросы и обнаружение каталога доступны на одном и том же соединении. Полный конвейер governance (RLS, маскирование, сэмплирование) применяется к каждому запросу. (REQ-130, REQ-143)

**Формат тикета** (JSON):

```json
{"query": "{ customers { name email } }", "role": "analyst", "variables": {}}
```

**Использование (Python):**

```python
import pyarrow.flight as flight

client = flight.FlightClient("grpc://localhost:8815")
ticket = flight.Ticket(b'{"query": "{ orders { id amount } }", "role": "admin"}')
# Stream batch-by-batch
for batch in client.do_get(ticket):
    process(batch.data)
# Or read all at once
table = client.do_get(ticket).read_all()
```

Когда доступен прокси Zaychik Flight SQL (порт 8480), пакеты записей передаются потоком от начала до конца без полной материализации. (REQ-144) При недоступности Zaychik происходит откат к материализации через слой федеративных запросов. (REQ-146)

---

## Protobuf gRPC Endpoint

Порт `50051` (переопределяется переменной окружения `GRPC_PORT` или конфигурацией `server.grpc_port`). (REQ-529) [tool-verified: `provisa/grpc/server.py`, `provisa/api/app.py`]

Передавайте роль в ключе gRPC-метаданных `x-provisa-role`. Если он отсутствует, сервер прерывает соединение с `UNAUTHENTICATED`. [tool-verified: `provisa/grpc/server.py`]

Скачайте proto для конкретной роли по адресу `GET /data/proto/{role_id}`. Отображаются только таблицы и колонки, видимые этой роли. (REQ-039)

```proto
service ProvisaService {
  rpc QueryOrders (QueryOrdersRequest) returns (stream Orders);
  rpc InsertOrders (InsertOrdersRequest) returns (InsertOrdersResponse);
}
```

Каждая таблица порождает потоковый RPC `Query{TypeName}`. RPC `Insert{TypeName}` существуют для симметрии схемы, но прерываются с `UNIMPLEMENTED`. [tool-verified: `provisa/grpc/server.py`]

`grpc_reflection.v1alpha` включён для обнаружения сервиса без предварительно скомпилированного proto. (REQ-529) [tool-verified: `provisa/grpc/reflection.py`]

```bash
grpcurl -plaintext localhost:50051 list
grpcurl -plaintext -H 'x-provisa-role: analyst' \
  -d '{}' localhost:50051 ProvisaService/QueryOrders
```

Сервер gRPC запускается только тогда, когда при старте удаётся скомпилировать корректный proto. Если сборка схемы завершается неудачей, сервер gRPC не запускается. (REQ-529)

#### Aggregate and Group-By RPCs (REQ-1359, REQ-1361, REQ-1405)

Когда для таблицы установлен `enable_aggregates`, сгенерированный proto включает два дополнительных RPC наряду с `Query{TypeName}`:

- **`Query{TypeName}Aggregate`** — возвращает агрегатные скаляры для таблицы (`count`; `sum`, `avg`, `stddev`, `variance` по каждой числовой колонке; `min`, `max` по каждой сравнимой колонке)
- **`Query{TypeName}GroupBy`** — возвращает одну строку на ключ группировки с агрегатными подполями и, опционально, скалярами базовой таблицы и строками присоединённых измерений в поле `nodes`

Оба проходят через тот же конвейер компиляции агрегатов, что и корневые поля GraphQL `{field}_aggregate` и `{field}_group_by` — отдельной реализации агрегатов не существует. (REQ-1359) [tool-verified: `provisa/grpc/query_ir.py:133-196`]

**Поле `funcs` (REQ-1361).** Сообщение запроса принимает повторяющееся строковое поле `funcs`. Допустимые значения: `count`, `sum`, `avg`, `stddev`, `variance`, `min` и `max`. Когда `funcs` опущено, запрашивается каждая функция, которую схема предоставляет для этой таблицы. Когда поле задано, отображаются только названные функции. Если ни одна из названных функций не применима к типам колонок таблицы, запрос откатывается к `count`. [tool-verified: `provisa/grpc/query_ir.py:66`, `provisa/grpc/query_ir.py:75-97`]

**Поля `include_nodes` и `include` (REQ-1405).** Запросы `Query{TypeName}GroupBy` могут устанавливать `include_nodes: true`, чтобы включить скалярные колонки базовой таблицы в поле `nodes` каждой строки. Повторяющееся строковое поле `include` называет поля связей многие-к-одному, чьи скалярные колонки также вкладываются внутрь `nodes`. Это соответствует поведению `?includeNodes=` / `?include=` в JSON:API. [tool-verified: `provisa/grpc/query_ir.py:168-195`]

---

## Драйвер JDBC

Драйвер Provisa JDBC (`provisa-jdbc-0.1.0.jar`) раскрывает семантический каталог для BI-инструментов (Tableau, PowerBI, DBeaver). (REQ-126)

**URL подключения:** `jdbc:provisa://host:port` (REQ-131)

Домены сопоставляются со схемами JDBC. (REQ-127) Таблицы используют свои зарегистрированные псевдонимы. Колонки используют псевдонимы и раскрывают описания как `REMARKS`. (REQ-128) Стандартные методы метаданных (`getPrimaryKeys`, `getImportedKeys`, `getExportedKeys`) раскрывают семантические связи как метаданные PK/FK.

**Поддержка SQL:** `SELECT * FROM <alias> [WHERE col = 'value']`. (REQ-129)

Драйвер по умолчанию запрашивает редирект в формате Arrow IPC. Результаты передаются потоком пакет за пакетом через `ArrowStreamReader`, ограниченные одним пакетом записей в памяти. (REQ-293)

---

## Формат аргумента `orderBy`

Аргумент `order_by` использует объекты `{column: direction}` с перечислением из 6 значений направления: (REQ-200)

```json
{
  "query": "{ orders(order_by: [{created_at: desc_nulls_last}]) { id created_at } }",
  "role": "admin"
}
```

Поддерживаемые направления: `asc`, `desc`, `asc_nulls_first`, `asc_nulls_last`, `desc_nulls_first`, `desc_nulls_last`. (REQ-201)

---

## Subscriptions

SSE-подписки доступны по адресу `GET /data/subscribe/{table}`. (REQ-219, REQ-258) Доставка уведомлений использует подключаемого провайдера, выбираемого по типу источника: источники PostgreSQL используют `LISTEN/NOTIFY`, источники MongoDB используют Change Streams, а источники Kafka используют consumer group. Фильтрация RLS и проверка схемы применяются независимо от провайдера. Источники WebSocket и RSS также поддерживаются через тот же эндпоинт. (REQ-338, REQ-342) [tool-verified: `provisa/api/data/subscribe.py:239`, `provisa/subscriptions/registry.py`, `provisa/api/app.py` `_rebuild_schemas`]

---

## Бизнес-глоссарий (REQ-1387)

Бизнес-глоссарий сопоставляет физические имена полей — как они существуют в исходных базах данных — с общей человекочитаемой терминологией. Каждая колонка, зарегистрированная в семантическом слое, автоматически получает термин. Ручной ввод не требуется для наполнения глоссария; кураторы добавляют определения, связи и экспертов поверх того, что система выводит автоматически.

### Как выводятся термины

Когда Provisa регистрирует или обновляет колонки таблицы, `normalize_term` (`provisa/core/glossary.py`) запускается для каждого имени колонки и формирует каноническую фразу. [tool-verified: `provisa/core/repositories/glossary.py:sync_table_refs`]

Нормализация последовательно применяет пять правил:

1. Разбить по границам camelCase и символам-разделителям (`_`, `-`, `.`, `/`, пробел).
2. Привести результат к нижнему регистру.
3. Раскрыть по фиксированной таблице сокращений (например, `cust` → `customer`, `amt` → `amount`, `dt` → `date`, `id` → `identifier`, `key` → `identifier`, `guid` → `identifier`).
4. Отсечь замыкающий **токен-заместитель (proxy token)** (`identifier`, `code`, `index` или `reference`) — колонка, названная по своему ключу или коду, указывает на базовое понятие через значение-заместитель, поэтому термином должно быть само понятие. Последний оставшийся токен никогда не отсекается.
5. Уточнить **слишком общую фразу** понятием таблицы. Когда полная нормализованная фраза является голым словом-атрибутом (`name`, `identifier`, `date`, `location`, `message`, `first name`, `last name` и подобными), термин становится `<понятие таблицы> <фраза>` — `employees.first_name` → `employee first name`, `orders.id` → `order identifier`. Один общий термин `name`, разделяемый несвязанными таблицами, объединил бы разные значения; уточнение вместо этого связывает каждую колонку с её объемлющим понятием. Понятие таблицы — это бизнес-имя таблицы, нормализованное с единственным числом стержневого существительного (`order_lines` → `order line`).

Псевдоколонки нативных фильтров (с префиксом `_nf_`, или любая колонка, несущая `native_filter_type`) — это машинерия параметров запроса, а не бизнес-поля, и термины для них не выводятся.

Поскольку `id`, `key`, `pk` и `sk` — все раскрываются в `identifier` до проверки на токен-заместитель, три физически разных имени колонок попадают в точно один и тот же термин:

| Физическое имя | После нормализации |
| --- | --- |
| `cust_id` | `customer` |
| `customerId` | `customer` |
| `CUSTOMER_KEY` | `customer` |
| `txn_amt` | `transaction amount` |

Первые три схлопываются в один термин. `transaction amount` сохраняет оба токена, потому что `amount` не является заместителем. Голая колонка `id` — без предшествующих токенов — не может быть отсечена; она нормализуется в `identifier`, поэтому термин не пуст. [tool-verified: `provisa/core/glossary.py:normalize_term`]

### Жизненный цикл

Термины **выводятся из членства в семантическом слое**, а не создаются пользователями по требованию. Репозиторий таблиц — единственный путь записи: `sync_table_refs` выполняется в рамках каждого upsert набора колонок, а `sweep_refless_terms` выполняется после любого пути удаления. [tool-verified: `provisa/core/repositories/glossary.py`]

**Когда колонка добавляется:** Provisa ищет нормализованный термин по имени. Если он уже существует, колонка получает ссылку на него (и если термин был устаревшим (deprecated), он оживляется — `deprecated` возвращается в `False`). Если термина ещё нет, он создаётся.

**Когда колонка уходит** (изменение схемы или удаление таблицы): её ссылка удаляется, и термин **улаживается (settled)** по правилу «удалить или пометить устаревшим». Корневой термин без оставшихся ссылок удаляется полностью — вместе со своими рёбрами и назначениями экспертов — если только его удаление не оставило бы абстрактный термин отсоединённым от всех корневых терминов (без пути через граф терминов). В этом случае термин помечается **устаревшим** (`deprecated=True`) вместо удаления, чтобы якорь абстрактного термина в графе сохранился.

Абстрактные термины никогда не удаляются автоматически; они существуют вне физического жизненного цикла и удаляются только явно через admin API.

**Оживление:** если нормализованное имя устаревшего термина появляется снова (колонка регистрируется заново), пометка термина снимается, и его ссылки снова начинают накапливаться.

### Эндпоинты курирования

Все эндпоинты находятся под `/admin/glossary`. Они требуют доступа `org_admin` и настроенной организации. Каждая мутация запускает публикацию метаданных. [tool-verified: `provisa/api/admin/glossary_router.py`]

| Метод | Путь | Описание |
| --- | --- | --- |
| `GET` | `/admin/glossary/terms` | Список терминов. Параметры запроса: `q` (поиск по имени/определению), `include_deprecated` (по умолчанию `true`) |
| `GET` | `/admin/glossary/terms/{term_id}` | Детали термина: определение, физические ссылки, типизированные рёбра, эксперты |
| `POST` | `/admin/glossary/terms` | Создать абстрактный термин — пользовательская терминология без физических ссылок |
| `PATCH` | `/admin/glossary/terms/{term_id}` | Переименовать, задать определение или переключить исключение из экспорта |
| `DELETE` | `/admin/glossary/terms/{term_id}` | Удалить термин, не имеющий физических ссылок |
| `POST` | `/admin/glossary/refs/move` | Переместить одну физическую ссылку на другой термин (консолидация) |
| `POST` | `/admin/glossary/terms/{term_id}/edges` | Добавить типизированное ребро связи между двумя терминами |
| `DELETE` | `/admin/glossary/terms/{term_id}/edges` | Удалить ребро (параметры запроса: `to_term_id`, `rel_type`) |
| `POST` | `/admin/glossary/terms/{term_id}/experts` | Пометить пользователя как эксперта или автора термина |
| `DELETE` | `/admin/glossary/terms/{term_id}/experts/{user_id}` | Удалить назначение эксперта/автора у пользователя |
| `POST` | `/admin/glossary/terms/{term_id}/definition/generate` | Сгенерировать черновик определения для одного термина с помощью AI-модели организации — возвращает только текст, ничего не сохраняется до сохранения |
| `POST` | `/admin/glossary/definitions/generate` | Сгенерировать и сохранить определения для каждого термина, у которого их нет — никогда не перезаписывает текст, написанный человеком |
| `POST` | `/admin/glossary/relationships/generate` | Предложить и сохранить типизированные рёбра по всему глоссарию с помощью AI-модели организации |

**Тело запроса `POST /admin/glossary/terms`:**

```json
{"name": "revenue", "definition": "Recognized net revenue after returns and discounts."}
```

**Тело запроса `POST /admin/glossary/terms/{term_id}/edges`:**

```json
{"to_term_id": 42, "rel_type": "KIND_OF"}
```

Допустимые значения `rel_type`: `KIND_OF`, `RELATED_TO`, `PART_OF`, `SYNONYM_OF`. [tool-verified: `provisa/core/glossary.py:TERM_EDGE_TYPES`]

**Тело запроса `POST /admin/glossary/terms/{term_id}/experts`:**

```json
{"user_id": "alice@example.com", "kind": "author"}
```

Допустимые значения `kind`: `expert`, `author`. [tool-verified: `provisa/core/repositories/glossary.py:add_expert`]

**Тело запроса `POST /admin/glossary/refs/move`:**

```json
{"table_id": 7, "column_name": "cust_id", "to_term_id": 12}
```

Перемещение ссылки улаживает теряющий термин по правилу «удалить или пометить устаревшим». Используйте это, чтобы консолидировать два термина, которые нормализация оставила раздельными, — например, после того как источник использовал нестандартное сокращение, выпавшее за пределы таблицы раскрытия.

Удаление корневого термина (с физическими ссылками) возвращает `400 glossary.invalid`. Сначала удалите или переместите все ссылки.

**`PATCH /admin/glossary/terms/{term_id}` — поле `export_excluded`:**

```json
{"export_excluded": true}
```

Установка `export_excluded` в `true` исключает термин из всех снимков экспорта метаданных, независимо от его физических ссылок или абстрактного статуса. Возврат в `false` восстанавливает термин в снимке при следующей публикации. Данные курирования (определение, рёбра, эксперты) при этом не затрагиваются. [tool-verified: `provisa/core/repositories/glossary.py:set_export_excluded`, `provisa/api/admin/glossary_router.py:update_term`]

### Курирование с помощью AI

Настроенная AI-модель организации может составлять черновики определений и предлагать рёбра связей по всему глоссарию за одну операцию. Оба массовых действия требуют доступа `org_admin` и настроенной организации.

**`POST /admin/glossary/definitions/generate`**

Проходит по каждому термину в глоссарии, пропускает те, у которых уже есть определение, и вызывает AI-модель организации, чтобы составить черновик для каждого оставшегося термина. Черновик сохраняется немедленно — в отличие от эндпоинта черновика для одного термина (`POST /admin/glossary/terms/{term_id}/definition/generate`), здесь нет этапа редактирования. Определения, написанные человеком, никогда не перезаписываются: защита — это `if summary["definition"]: continue` перед любым вызовом модели. Одно уведомление о публикации покрывает весь пакет. [tool-verified: `provisa/api/admin/glossary_router.py:generate_all_definitions`]

Ответ:

```json
{"generated": 12}
```

`generated` — количество терминов, получивших новое определение. Равно нулю, когда у каждого термина уже есть определение.

**`POST /admin/glossary/relationships/generate`**

Отправляет полный список терминов AI-модели организации с промптом, указывающим десять допустимых типов рёбер (`KIND_OF`, `PART_OF`, `SYNONYM_OF`, `RELATED_TO`, `VALID_VALUE_OF`, `DERIVED_FROM`, `REPLACES`, `PREFERRED_TERM_FOR`, `TRANSLATION_OF`, `ANTONYM_OF`), и запрашивает только уверенные предложения. Модель отвечает JSON-массивом; каждая запись проверяется перед любой записью: неизвестные имена терминов, самоссылающиеся рёбра и типы рёбер вне закрытого перечисления молча отбрасываются. Валидные предложения записываются идемпотентно (upsert) — повторный запуск действия не дублирует рёбра. Одно уведомление о публикации покрывает пакет. Эндпоинт немедленно возвращает `{"added": 0}`, когда в глоссарии меньше двух неустаревших терминов. [tool-verified: `provisa/api/admin/glossary_router.py:generate_relationships`]

Ответ:

```json
{"added": 5}
```

`added` — количество записанных рёбер. Ребро, которое уже существовало, всё равно засчитывается — upsert выполняется успешно, но данные ребра не меняются.

### Инструмент MCP `search_terms`

```
search_terms(query, role=None, limit=25)
```

Ищет по именам и определениям терминов с сопоставлением подстроки без учёта регистра, до `limit` результатов. Каждый результат — это полная деталь термина: `name`, `definition`, `is_abstract`, `deprecated`, физические ссылки (с `source_id`, `schema_name`, `table_name`, `column_name`), типизированные рёбра и назначения экспертов. [tool-verified: `provisa/api/mcp/server.py:236-244`, `provisa/core/repositories/glossary.py:search_terms`]

Используйте `search_terms` перед написанием SQL, чтобы найти каждое физическое поле, представляющее понятие по имени. Например, поиск `"order date"` возвращает термин и все колонки `order_dt`, `orderDate`, `ORDER_DATE` во всех зарегистрированных таблицах.

### Экспорт метаданных

Граф терминов глоссария включён в каждый `MetadataSnapshot`, формируемый `build_snapshot`. [tool-verified: `provisa/api/metadata_export/builder.py:_glossary_assets`]

Экспорт применяет те же фильтры, что и остальная часть снимка:

- Термин, помеченный `export_excluded`, исключается полностью — независимо от его физических ссылок, абстрактного статуса или того, настроен ли каталог организации. [tool-verified: `provisa/api/metadata_export/builder.py:_glossary_assets`]
- Корневой термин публикуется только тогда, когда хотя бы одна из его физических ссылок принадлежит колонке, проходящей и фильтр **Data Product** (флаг `data_product` таблицы должен быть `true`), и фильтр **технических** колонок (колонки, помеченные как `technical`, исключаются).
- Корневой термин, все ссылки которого отсекаются этими фильтрами, исключается вместе с ними.
- Абстрактные термины публикуются безусловно — они являются пользовательской терминологией, не привязанной к физическим колонкам.
- Ребро между двумя терминами публикуется только тогда, когда публикуются оба конечных термина.

Каждый адаптер поставщика публикует граф терминов нативно, в контейнер глоссария, принадлежащий Provisa, который он создаёт идемпотентно, — никогда в существующий глоссарий каталога:

| Поставщик | Контейнер | Термины | Отношения | Устаревание |
| --- | --- | --- | --- | --- |
| Apache Atlas | «Provisa Glossary» (glossary API) | термины глоссария, определение в `longDescription` | KIND_OF → `isA`, SYNONYM_OF → `synonyms`, RELATED_TO/PART_OF → `seeAlso` | маркер shortDescription `[DEPRECATED]` |
| Atlan | глоссарий Provisa по стабильному qualifiedName | `longDescription` (никогда не человекочитаемый `userDescription`) | то же сопоставление Atlas | `certificateStatus = DEPRECATED` |
| DataHub | `urn:li:glossaryNode:provisa.<org>` | аспект `glossaryTermInfo` для каждого термина | KIND_OF → Inherits, PART_OF → Contains (инвертировано), RELATED_TO/SYNONYM_OF → related terms | аспект deprecation; переименования следуют преемственности URN |
| OpenMetadata | глоссарий Provisa через `/v1/glossaries` | PUT с ключом fqn, переименования PATCH-переподвязывают по сохранённому UUID | KIND_OF → нативная родительская иерархия, SYNONYM_OF → `synonyms`, остальные → `relatedTerms` | `entityStatus` |
| Collibra | домен типа Glossary «Provisa Glossary» | активы Business Term через Import API | нативные типы отношений Business Term | статус актива |

Владение — это привязка, а не имя: id поставщика каждого опубликованного термина фиксируется в `catalog_bindings` под URN термина (`provisa://<org>/terms/<name>`), и Provisa изменяет или удаляет элемент глоссария на стороне поставщика только тогда, когда владеет этой привязкой (или элемент находится в принадлежащем Provisa контейнере, который она создала). Элемент глоссария без привязки Provisa возник во внешней системе и никогда не затрагивается; обновления выполняются по схеме read-merge, так что поля, добавленные стюардом к собственным терминам Provisa, сохраняются; ничего не удаляется, когда термин покидает снимок. Назначения стюардом термина активу остаются во внешнем владении — ни один адаптер не записывает назначения термина активу (публикация назначений, авторизованных Provisa, — явное последующее развитие). Конкретно в Collibra безопасность в рамках семантики REPLACE Import API опирается на изоляцию: полезная нагрузка упоминает только активы внутри домена глоссария Provisa и экземпляры отношений только между терминами Provisa, так что глоссарии стюардов и их отношения никогда не оказываются достижимыми. [tool-verified: `provisa/api/metadata_export/atlan.py`, `provisa/api/metadata_export/datahub.py`, `provisa/api/metadata_export/atlas.py`, `provisa/api/metadata_export/openmetadata.py`]

---

## Data Products (REQ-1634)

Продукт данных группирует таблицы, публикуемые вместе для потребления, принадлежащие ровно одному домену. Поля следуют словарю ODPS (Open Data Product Standard) там, где Provisa уже является источником истины. Административный UI раскрывает продукты данных в разделе **Admin → Data Products**. [tool-verified: `provisa/core/models.py:318-342`, `provisa/api/admin/schema_mutation.py:949-1017`, `provisa/api/admin/schema_query.py:352-362`]

### Возможности

| Возможность | Предоставляет |
| --- | --- |
| `data_product_read` | Доступ на чтение к полю запроса `data_products` и странице администрирования Data Products. По умолчанию предустановлено для `org_admin`, `analyst`, `developer` и `modeler`. |
| `data_product_rw` | Мутации создания и удаления. Включает элементы управления New / Edit / Delete в UI. |

[tool-verified: `provisa/api/admin/schema_mutation.py:959,1001`, `provisa/api/admin/schema_query.py:357`]

### Admin GraphQL

Все операции с продуктами данных проходят через `POST /admin/graphql`.

**Запрос:**

```graphql
query {
  data_products {
    id
    domain_id
    name
    owner_role
    team_role
    purpose
    limitations
    usage
    version
    status
    sla
    support
    custom_properties
  }
}
```

Требует `data_product_read`.

**Создание или обновление:**

```graphql
mutation {
  create_data_product(input: {
    id: "customer_360"
    domain_id: "sales"
    name: "Customer 360"
    owner_role: "data-product-owner"
    team_role: "sales-analytics"
    purpose: "Single view of a customer across all touchpoints."
    status: "active"
    version: "1.0.0"
  }) {
    success
    message
  }
}
```

`create_data_product` выполняет upsert — вызов с существующим `id` обновляет запись. Требует `data_product_rw`.

**Удаление:**

```graphql
mutation {
  delete_data_product(id: "customer_360") {
    success
    message
  }
}
```

Удаление продукта очищает `product_id` у каждой таблицы-участника, снимая её членство. Требует `data_product_rw`. [tool-verified: `provisa/api/admin/schema_mutation.py:995-1017`]

### Схема полей

| Поле | Тип | Обязательно | Примечания |
| --- | --- | --- | --- |
| `id` | `String` | Да | Машиночитаемый стабильный идентификатор, например `customer_360` |
| `domain_id` | `String` | Да | Владеющий домен. Таблицы-участники должны разделять этот `domain_id` — несовпадения отклоняются при сохранении |
| `name` | `String` | Да | Отображаемое имя |
| `owner_role` | `String` | Нет | Роль, ответственная за этот продукт; отлична от стюарда домена |
| `team_role` | `String` | Нет | Роль, чьи держатели поддерживают этот продукт день за днём; разрешается до отдельных лиц |
| `purpose` | `String` | Нет | Что публикует этот продукт и зачем |
| `limitations` | `String` | Нет | Известные ограничения, оговорки или исключения |
| `usage` | `String` | Нет | Как потреблять этот продукт |
| `version` | `String` | Нет | например, `1.2.0` |
| `status` | `String` | Нет | например, `proposed`, `active`, `deprecated`, `retired` |
| `sla` | `String` | Нет | Обязательства по уровню обслуживания; текст — продукт охватывает несколько таблиц, и структурированный SLA не может однозначно указать, какого участника он описывает |
| `support` | `String` | Нет | Произвольный текст с рекомендациями по поддержке |
| `custom_properties` | `JSON` | Нет | Произвольные метаданные ключ-значение, не покрытые стандартными полями |

На модели существуют два дополнительных поля, но они не раскрыты в Strawberry `DataProductType` / `DataProductInput` — они специфичны для Snowflake Horizon Catalog (REQ-1635):

| Поле | Примечания |
| --- | --- |
| `support_contact` | Email или URL; требуется манифестами организационного листинга Horizon Catalog |
| `publish` | `true` для немедленной публикации листингов Horizon; новые листинги по умолчанию находятся в DRAFT |

[tool-verified: `provisa/core/models.py:338-341`, `provisa/api/admin/types.py:104-118,538-551`]

### Членство таблиц

Таблица входит в продукт данных, устанавливая своё поле `product_id` в форме редактирования таблицы. Средство выбора ограничено продуктами, чей `domain_id` совпадает с собственным доменом таблицы, — таблица в домене `marketing` никогда не предлагается для продукта в домене `sales`. [tool-verified: `provisa/api/admin/actions_router.py:244-260`, `docs/arch/requirements.yaml:54585-54586`]

Команды в том же домене также могут быть назначены участниками. [tool-verified: `provisa-ui/src/i18n/locales/en/dataProductsTab.json:commandsLabel`]

### Фильтр экспорта метаданных

`build_snapshot` применяет `data_products_only=True` для каждой публикации в каталог. Таблицы без `product_id` исключаются из снимка вместе с их рёбрами связей, рёбрами lineage и тегами governance. Источники и домены публикуются всегда. Термины глоссария публикуются только тогда, когда хотя бы одна из их физических ссылок принадлежит экспортированной (входящей в продукт) таблице. [tool-verified: `provisa/api/metadata_export/builder.py:594,609,641`]

Продукт без экспортированных участников не формирует запись в снимке — листинг без участников искажал бы представление продукта в каталоге. [tool-verified: `provisa/api/metadata_export/model.py:106-113`]

### Поддержка продуктов данных по целевому каталогу

`MetadataSnapshot.data_products` доходит до каждого адаптера, но только адаптеры, чья платформа имеет нативное понятие продукта данных, публикуют его как самостоятельную сущность; остальные публикуют таблицы-участники (уже отфильтрованные выше) без группировки в продукт.

| Цель | Представление продукта данных |
| --- | --- |
| Snowflake Horizon | Каждый продукт становится `SHARE` над физическими адресами таблиц-участников, обёрнутым во внутренний `CREATE ORGANIZATION LISTING` — нативный продукт данных Horizon Catalog. `publish=true` немедленно публикует листинг вживую; иначе он попадает как DRAFT. [tool-verified: `provisa/api/metadata_export/snowflake_horizon.py:21-34,389-418`] |
| BigQuery Dataplex | Каждый продукт становится листингом Analytics Hub через `/v1/dataProducts`. [tool-verified: `provisa/api/metadata_export/bigquery_dataplex.py:100,136,159`] |
| OpenMetadata | Каждый продукт становится нативной сущностью `DataProduct` (`/api/v1/dataProducts`) с владением, производным от домена. [tool-verified: `provisa/api/metadata_export/openmetadata.py:326-344,635`] |
| DataHub | Каждый продукт становится нативной сущностью `dataProduct` (`urn:li:dataProduct:...`) с собственными аспектами `dataProductProperties`/ownership. [tool-verified: `provisa/api/metadata_export/datahub.py:133-136,443-483`] |
| Collibra | Каждый продукт становится активом типа сообщества `Data Product`, связанным с таблицами-участниками через отношение `Data Product groups Table`. [tool-verified: `provisa/api/metadata_export/collibra.py:129-133,371-388`] |
| Atlan | Публикуется как пользовательский typedef-догадка `DataProduct` — у Atlan нет документированного стабильного имени типа для этого понятия, поэтому сопоставление выполняется по принципу «лучшее из возможного». [tool-verified: `provisa/api/metadata_export/atlan.py:60`] |
| Apache Atlas | Публикуется как пользовательский typedef `provisa_data_product` с отношением `provisa_data_product_members` — у Atlas нет нативного типа сущности «продукт данных». [tool-verified: `provisa/api/metadata_export/atlas.py:134-147,191,256-260`] |
| OpenLineage | Не самостоятельная сущность — таблицы-участники несут пользовательский фасет `provisa_data_product`, называющий владеющий продукт. [tool-verified: `provisa/api/metadata_export/openlineage.py:243,348`] |
