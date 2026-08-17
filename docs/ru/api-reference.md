# Справочник API

## Обзор

Provisa предоставляет REST-эндпоинты под двумя префиксами: `/data` для выполнения запросов и интроспекции схемы, и `/admin` для управления конфигурацией. (REQ-043) Большинство эндпоинтов данных требуют идентификатор роли. Операции конфигурации admin используют Strawberry GraphQL API по адресу `/admin/graphql`. (REQ-164)

---

## Аутентификация

Когда `auth.provider` настроен в `provisa.yaml`, все эндпоинты, кроме `/health` и `/setup/status`, требуют заголовок `Authorization: Bearer <token>`. (REQ-120) [tool-verified: `provisa/api/app.py`, `provisa/auth/wiring.py`]

Без настроенной аутентификации сервер работает в dev-режиме. Любой запрос обрабатывается как identity `anonymous`, которая сопоставляется со всеми настроенными ролями с доступом ко всем доменам через wildcard. (REQ-535)

**Вход (`POST /auth/login`)** предоставляется активным провайдером аутентификации, когда настроено `provider: basic`. (REQ-124) Формат учётных данных и ответа зависит от провайдера.

**Интроспекция identity:**

```http
GET /auth/me
```

Возвращает id, email, отображаемое имя, членства в организациях и назначения ролей аутентифицированного пользователя. В dev-режиме возвращает `dev_mode: true` со списком всех идентификаторов ролей. [tool-verified: `provisa/api/auth_router.py`]

```http
GET /auth/provider-type
```

Возвращает `{"provider": "<name>"}` или `{"provider": null}`, когда аутентификация не настроена. [tool-verified: `provisa/api/auth_router.py`]

---

## Эндпоинты данных

### `POST /data/graphql`

Выполнить запрос или мутацию GraphQL. (REQ-043) [tool-verified: `provisa/api/data/endpoint.py:151`]

**Тело запроса:**

```json
{
  "query": "{ orders(where: {region: {eq: \"us\"}}) { id amount } }",
  "variables": {},
  "role": "admin",
  "extensions": {}
}
```

Поле `role` используется только в dev-режиме (без аутентификации). Когда аутентификация активна, используется роль аутентифицированного пользователя, а `role` в теле игнорируется.

Поле `extensions` поддерживает протокол Automatic Persisted Query (APQ): (REQ-288)

```json
{
  "extensions": {"persistedQuery": {"sha256Hash": "<sha256-of-query>"}}
}
```

**Заголовки:**

- `X-Provisa-Role` — переопределение роли (dev-режим)
- `Accept` — формат ответа (см. «Согласование содержимого»)
- `Authorization` — `Bearer <token>`, когда включена аутентификация
- `X-Provisa-Redirect-Format` — MIME-тип для вывода перенаправления S3 (REQ-137)
- `X-Provisa-Redirect-Threshold` — количество строк, выше которого срабатывает перенаправление (REQ-137)
- `X-Provisa-Redirect` — `true` для безусловного принудительного перенаправления (REQ-029)

**Ответ (JSON инлайн):**

```json
{
  "data": {
    "orders": [
      {"id": 1, "amount": 99.99}
    ]
  }
}
```

**Ответ (перенаправление):**

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

**Ответ (несколько корней со смешанным инлайн/перенаправлением):**

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

Запросы с несколькими корневыми полями выполняют каждое корневое поле независимо. Поля ниже порога перенаправления возвращаются инлайн; поля выше — перенаправляются. Ключ `redirects` (во множественном числе) сопоставляет имена полей с информацией о перенаправлении. (REQ-029) [tool-verified: `provisa/api/data/endpoint.py`]

**Заголовки кеша:**

- `X-Provisa-Cache: HIT|MISS` (REQ-536)
- `X-Provisa-Cache-Age: <seconds>` (при HIT) (REQ-536)

**Необходимые возможности:** `QUERY_DEVELOPMENT` для всех запросов, включая интроспекцию. [tool-verified: `provisa/api/data/endpoint.py:186-283`]

---

### Согласование содержимого

| Заголовок Accept | Формат |
| --- | --- |
| `application/json` | JSON (по умолчанию) |
| `application/x-ndjson` | JSON с разделением строк |
| `text/csv` | CSV |
| `application/vnd.apache.parquet` | Parquet |
| `application/vnd.apache.arrow.stream` | Arrow IPC |

(REQ-047, REQ-048, REQ-049, REQ-050) [tool-verified: `provisa/api/data/endpoint.py:84-90`]

---

### Перенаправление

Результаты выше настроенного порога строк (или когда `X-Provisa-Redirect: true`) записываются в S3, и возвращается подписанный URL. (REQ-029, REQ-044)

| Формат перенаправления | Записывается | Память |
| --- | --- | --- |
| `application/vnd.apache.parquet` | федеративный CTAS | Нет — данные никогда не проходят через Provisa |
| `application/x-orc` | федеративный CTAS | Нет — данные никогда не проходят через Provisa |
| `application/json` | Provisa | Ограничено памятью |
| `application/x-ndjson` | Provisa | Ограничено памятью |
| `text/csv` | Provisa | Ограничено памятью |
| `application/vnd.apache.arrow.stream` | Provisa | Ограничено памятью |

Для крупных аналитических экспортов используйте перенаправление Parquet или ORC. Движок федерации пишет напрямую в S3 параллельно — данные не проходят через Provisa. (REQ-138)

```yaml
X-Provisa-Redirect-Format: application/vnd.apache.parquet
X-Provisa-Redirect-Threshold: 1000
```

---

### `POST /data/sql`

Выполнить сырой SQL через конвейер governance этапа 2. (REQ-267) [tool-verified: `provisa/api/data/endpoint_dev.py:62`]

**Тело запроса:**

```json
{
  "sql": "SELECT id, amount FROM orders WHERE region = 'us'",
  "role": "admin"
}
```

**Необходимые возможности:** `QUERY_DEVELOPMENT`.

Нарушения governance на `POST /data/sql` возвращают HTTP 403. (REQ-002, REQ-266)

**Ответ:** Тот же формат, что и `/data/graphql` (строки JSON по умолчанию, с согласованием содержимого через `Accept`).

---

### `POST /data/query`

Унифицированный эндпоинт запросов. Принимает GraphQL, SQL или Cypher — синтаксис определяется автоматически. (REQ-267) [tool-verified: `provisa/api/data/endpoint_dev.py:509`]

Запросы Cypher также можно отправлять на эндпоинт, предназначенный только для Cypher, `POST /query/cypher`. (REQ-345)

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

### `GET /data/rest/{domain_id}/{table_name}`

Автоматически сгенерированный обычный REST-эндпоинт для каждой зарегистрированной таблицы. Строка запроса сопоставляется с аргументами GraphQL, и запрос компилируется и выполняется через тот же конвейер (RLS, маскирование, маршрутизация), что и GraphQL. (REQ-256) [tool-verified: `provisa/api/rest/generator.py:153`]

**Параметры запроса:**

- `limit` — максимум строк (≥ 1)
- `offset` — пропуск строк (≥ 0)
- `fields` — имена столбцов через запятую (по умолчанию все скалярные поля)
- `filter` — JSON-массив объектов фильтра `{"field", "comparator", "value"}`
- `orderBy` — JSON-массив объектов сортировки `{"field", "direction"}`

Требуется аутентифицированная роль; неаутентифицированные запросы возвращают `401`. Спецификация OpenAPI для этих маршрутов предоставляется по адресу `GET /data/rest/openapi.json` со Swagger UI по адресу `GET /data/rest/docs`.

#### Проводник OpenAPI / Swagger UI

Страница проводника OpenAPI (`/app/openapi`) встраивает Swagger UI в изолированный iframe. Спецификация ограничена ролью — отображаются только таблицы и столбцы, видимые текущей роли, — и опционально фильтруется по домену через селектор домена. UI автоматически переключается между светлой и тёмной темами. [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:20-34`]

Страница загружает HTML спецификации через `fetch()`, а не через прямой `src` у iframe, поэтому запрос несёт токен-носитель сессии, а собственные относительные запросы Swagger UI корректно разрешаются относительно того же источника. [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:44-69`]

При переходе по ссылке NL «Открыть в OpenAPI» страница автоматически разворачивает целевой эндпоинт, заполняет параметры запроса из URL, сгенерированного NL (например, `aggregate`, `groupBy`), и нажимает Execute — используя опрос DOM, чтобы гарантировать завершение каждого шага перед запуском следующего. (REQ-1359) [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:94-171`]

---

### `GET /data/jsonapi/{domain_id}/{table_name}`

Автоматически сгенерированный эндпоинт, совместимый с [JSON:API](https://jsonapi.org), для каждой зарегистрированной таблицы. Те же RLS, маскирование и маршрутизация, что и в GraphQL. (REQ-257) [tool-verified: `provisa/api/jsonapi/generator.py:284`]

**Заголовок `Accept`:** должен включать `application/vnd.api+json` (медиа-тип JSON:API), иначе запрос возвращает `406`.

**Параметры запроса:**

- `fields[<type>]` — разреженные наборы полей, например `?fields[orders]=amount`
- `filter[<col>]` / `filter[<col>][<op>]` — например `?filter[region]=US`, `?filter[amount][gt]=100`
- `sort` — через запятую, префикс `-` для убывания, например `?sort=-created_at,amount`
- `page[number]` / `page[size]` — пагинация
- `aggregate` — агрегатные функции через запятую, выполняемые вместо выборки строк: `count`, `sum`, `avg`, `stddev`, `variance`, `min`, `max`. Используйте `?aggregate=count,sum`, чтобы запросить подмножество. Ответы агрегации возвращают `data: null` с результатами в `meta.aggregate`. (REQ-1359) [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:238`]
- `groupBy` — имена столбцов через запятую; используется вместе с `?aggregate=` для группировки результатов. Допустимы только столбцы из перечисления `DistinctOnColumn` таблицы; сервер возвращает `400` для любого столбца, который роль не может видеть. (REQ-1361) [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:447`]
- `includeNodes` — `true`, чтобы включить скалярные столбцы базовой таблицы (и скалярные поля присоединённых измерений, указанные в `include=`) внутрь массива `nodes` каждой строки группы. Требуется, когда запрос группировки NL также запрашивает детали измерения. (REQ-1405)

Ответы — это объекты ресурсов с `type`/`id`/`attributes`. Ошибки следуют форме объекта ошибки JSON:API.

#### Проводник JSON:API

Страница проводника JSON:API (`/app/jsonapi`) — это браузерный интерфейс поверх этих эндпоинтов. Выберите таблицу из списка, сгруппированного по доменам, затем настройте:

- **Поля** — выберите, какие столбцы включить (разреженный набор полей); оставьте все не отмеченными, чтобы запросить каждый столбец
- **Связи** — выберите имена связей, производных от FK, для подгрузки через `?include=`
- **Фильтр** — поле, оператор (`eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `like`) и значение
- **Сортировка** — одно поле, по возрастанию или убыванию
- **Агрегация** — выберите столбцы группировки из списка, проверенного сервером, затем отметьте одну или несколько агрегатных функций; когда выбраны столбцы группировки, флажок «Включить узлы» добавляет скалярные столбцы базовой таблицы к каждой строке
- **Размер страницы** — ресурсов на страницу, с навигацией первая/предыдущая/следующая/последняя

Результаты отображаются в форматированном сводном виде (карточки ресурсов с кликабельными якорями связей) или на вкладке необработанного JSON. Показан URL текущего запроса, который можно скопировать. Выбор таблицы и размер страницы сохраняются между сессиями в `localStorage`. [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx`]

При переходе по ссылке NL «Открыть в JSON:API» проводник заранее выбирает таблицу и заполняет селектор агрегации параметрами запроса, сгенерированными NL, затем автоматически выполняет запрос. [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:460-479`]

---

### `POST /query/nl`

Отправить вопрос на естественном языке. Сервис запускает асинхронную задачу и немедленно возвращает `202 Accepted` с `job_id`. Требует настроенного провайдера LLM в секции конфигурации `ai_models`. (REQ-354) [tool-verified: `provisa/api/rest/nl_router.py:50`]

**Тело запроса:**

```json
{"q": "How many orders were placed last month?", "role": "admin"}
```

Возвращает `{"job_id": "<id>"}`. Превышение лимита частоты NL для роли возвращает `429` с заголовком `Retry-After`. (REQ-370)

**Получение результата:**

- `GET /query/nl/{job_id}` — опрос. Возвращает документ задачи.
- `GET /query/nl/{job_id}/stream` — SSE. Одно событие `branch` на каждую цель генерации по мере завершения, затем событие `done`. (REQ-357, REQ-358)

Три цикла генерации (Cypher, GraphQL, SQL) выполняются параллельно, каждый проверяется компилятором и уточняется при ошибке. (REQ-355) Промпт ограничен видимой схемой роли. (REQ-356) Итоговый документ ключирует каждую ветвь по цели: (REQ-357) [tool-verified: `provisa/nl/job.py:69`]

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

Ветвь, исчерпавшая лимит итераций, возвращает `query: null`, `result: null` и строку `error`. Каждый сгенерированный запрос выполняется от имени прав потребителя с применённым governance этапа 2 — сервис никогда не обходит governance. (REQ-359)

#### Группировка NL с деталями измерений (REQ-1405)

Когда запрос группировки NL также проецирует столбцы из присоединённой таблицы измерений — например, «количество обращений по пользователю с именем пользователя и email», — исполнитель выводит пофайловые точечные пути (`dim_paths`) из столбцов измерений, спроецированных в SELECT. Эти пути заполняют параметр `includeNodes=` в сгенерированных URL панелей JSON:API и OpenAPI, поэтому эти панели запрашивают те же поля присоединённых измерений, которые разрешили ветви SQL и GraphQL. Без этого `includeNodes=true` вернул бы только собственные скалярные поля базовой агрегатной таблицы. (REQ-1405) [tool-verified: `docs/arch/requirements.md:REQ-1405`]

На панели gRPC сгенерированный `{Type}GroupByRequest` несёт `include_nodes` (bool) и `include` (повторяющаяся строка имён полей связей). Возвращаемый `{Type}GroupByRow` включает типизированное поле `nodes` со строками деталей измерения. [tool-verified: `provisa/grpc/query_ir.py:168-196`]

---

### `GET /data/sdl`

Вернуть GraphQL SDL для схемы роли. (REQ-008) [tool-verified: `provisa/api/data/sdl.py:137`]

**Заголовки:** `X-Role: <role_id>` (обязателен)

**Параметры запроса:**

- `domain` — идентификаторы доменов через запятую. Если задано, ответ фильтруется по указанному домену(ам) и таблицам, достижимым из них.

**Ответ:** GraphQL SDL в `text/plain`.

---

### `GET /data/introspection`

Вернуть JSON интроспекции GraphQL, опционально отфильтрованный по домену. [tool-verified: `provisa/api/data/sdl.py:200`]

**Заголовки:** `X-Provisa-Role: <role_id>` (обязателен)

**Параметры запроса:** `domain` — идентификаторы доменов через запятую.

**Ответ:** результат интроспекции в формате `application/json`.

---

### `GET /data/graph-schema`

Вернуть графовое представление схемы роли: метки узлов и их типы связей для клиентов Cypher/графов. Включает `pk_columns` для каждой метки узла, чтобы вызывающие могли определить столбцы первичного ключа. (REQ-398) [tool-verified: `provisa/api/rest/cypher_router.py:689`]

**Ответ:** `application/json` с `node_labels` (каждая несёт `pk`/`pk_columns`) и `relationship_types`.

---

### `GET /data/domains`

Вернуть идентификаторы доменов, доступные запрашивающей роли. [tool-verified: `provisa/api/data/sdl.py:116`]

**Заголовки:** `X-Role: <role_id>` (обязателен)

**Ответ:** `["sales", "support", ...]`

---

### `GET /data/schema-version`

Вернуть строку текущей версии схемы. Объединяет одноразовый идентификатор загрузки со счётчиком пересборок. Клиенты используют это для сброса кешей схемы после перезапусков сервера. (REQ-537) [tool-verified: `provisa/api/data/sdl.py:102`]

**Ответ:** `{"version": "<boot-id>-<counter>"}`

---

### `GET /data/proto/{role_id}`

Вернуть автоматически сгенерированный файл `.proto` для роли. [tool-verified: `provisa/api/data/endpoint_dev.py:49`]

**Ответ:** схема protobuf в `text/plain`.

Каждая зарегистрированная таблица производит proto `message`. Связи производят вложенные поля сообщений. Сопоставление типов: `integer → int32`, `bigint → int64`, `varchar → string`, `decimal → double`, `boolean → bool`, `timestamp → google.protobuf.Timestamp`. (REQ-538)

---

### `GET /data/subscribe/{table}`

Поток Server-Sent Events для уведомлений об изменениях в реальном времени из таблицы. (REQ-219, REQ-258) [tool-verified: `provisa/api/data/subscribe.py:239`]

Доставка уведомлений использует подключаемого провайдера, выбираемого по типу источника: источники PostgreSQL используют `LISTEN/NOTIFY` (через asyncpg), источники MongoDB используют Change Streams (`collection.watch()`), а источники Kafka используют группы потребителей. Каждый провайдер реализует общий асинхронный интерфейс наблюдения. Фильтрация RLS и проверка схемы применяются независимо от провайдера. (REQ-258) Источники WebSocket и RSS также поддерживаются. (REQ-338, REQ-342)

**Заголовок — `X-Provisa-Sink`:** Установите на цель Kafka (например, `kafka://broker:9092/topic`), чтобы перенаправить события изменений в приёмник Kafka вместо ответа SSE. Сервер запускает потребителя-приёмник и возвращает `202 Accepted` вместо открытого потока. (REQ-812) [tool-verified: `provisa/api/data/subscription_sse.py:137`]

---

## Административные REST-эндпоинты

### Конфигурация

#### `GET /admin/config`

Скачать текущий `provisa.yaml` как `application/x-yaml` с заголовком `Content-Disposition: attachment`. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:19`]

#### `PUT /admin/config`

Загрузить пересмотренный YAML конфигурации. Сервер записывает резервную копию `.bak`, сохраняет новый файл и перезагружает все схемы, источники и материализованные представления. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:32`]

**Тело запроса:** Сырое содержимое YAML.

**Ответ:**

```json
{"success": true, "message": "Config uploaded and reloaded"}
```

При сбое перезагрузки: `{"success": false, "message": "<error>"}`.

---

### Настройки

#### `GET /admin/settings`

Вернуть текущие настройки платформы в формате JSON. (REQ-165) [tool-verified: `provisa/api/admin/settings_router.py:50`]

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

Обновить настройки платформы во время выполнения. Все поля опциональны — обновляются только ключи, присутствующие в теле. (REQ-165) [tool-verified: `provisa/api/admin/settings_router.py:100`]

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

Обновляемые поля по секциям:

- `redirect`: `enabled`, `threshold`, `default_format`, `ttl`
- `sampling`: `default_sample_size`
- `cache`: `default_ttl`
- `naming`: `domain_prefix`, `convention` — записывает в файл конфигурации и запускает перезагрузку схемы (REQ-253)
- `relationships`: `auto_track_fk`
- `otel`: `endpoint`, `service_name`, `sample_rate`, `support_endpoint`, `support_redact_sql_literals`, `support_redact_attributes`

**Ответ:**

```json
{"success": true, "updated": ["otel.support_endpoint", "cache.default_ttl"]}
```

---

### Наблюдаемость

#### `GET /admin/traces/recent`

Вернуть до N недавних завершённых спанов из буфера спанов в памяти. (REQ-302) [tool-verified: `provisa/api/admin/settings_router.py:317`]

**Параметры запроса:** `limit` (по умолчанию 50, максимум 200)

**Ответ:** `{"traces": [...]}`

#### `POST /admin/query-engine/reload-catalog`

Горячая перезагрузка именованного каталога в координаторе движка федерации через его REST API. Переподключает внутреннее соединение Provisa и повторно выполняет DDL OTel. [tool-verified: `provisa/api/admin/settings_router.py:208`]

**Параметры запроса:** `catalog` (по умолчанию `"otel"`)

**Ответ:**

```json
{"success": true, "errors": []}
```

#### `POST /admin/query-engine/restart`

Перезапустить контейнер движка федерации (только для однонодовой разработки). [tool-verified: `provisa/api/admin/settings_router.py:287`]

**Параметры запроса:** `container` (по умолчанию из переменной окружения `QUERY_ENGINE_CONTAINER`, затем `"trino"`)

---

### Обнаружение

#### `POST /admin/discover/relationships`

Запустить обнаружение связей. Всегда выполняет интроспекцию FK из движка федерации. (REQ-018) Выполняет вывод LLM, если установлен `ANTHROPIC_API_KEY`. (REQ-167) [tool-verified: `provisa/api/admin/discovery.py:55`]

**Тело запроса:**

```json
{
  "scope": "domain",
  "domain_id": "sales"
}
```

`scope` должен быть одним из `"table"`, `"domain"`, `"cross-domain"`. Для области `"table"` требуется `table_id` (integer). Для области `"domain"` требуется `domain_id`.

**Ответ:** `{"candidates_found": 12, "stored_ids": [1, 2, 3, ...]}`

#### `GET /admin/discover/candidates`

Список ожидающих кандидатов связей. [tool-verified: `provisa/api/admin/discovery.py:96`]

#### `POST /admin/discover/candidates/{candidate_id}/accept`

Принять кандидата и зарегистрировать его как связь. [tool-verified: `provisa/api/admin/discovery.py:103`]

**Тело запроса (опционально):** `{"name": "custom-relationship-name"}`

#### `POST /admin/discover/candidates/{candidate_id}/reject`

Отклонить кандидата. [tool-verified: `provisa/api/admin/discovery.py:110`]

**Тело запроса:** `{"reason": "Not a real join"}`

#### `GET /admin/discover/candidates/rejected/count`

Вернуть количество отклонённых кандидатов. [tool-verified: `provisa/api/admin/discovery.py:118`]

#### `DELETE /admin/discover/candidates/rejected`

Удалить всех отклонённых кандидатов. [tool-verified: `provisa/api/admin/discovery.py:128`]

---

### Обход источника

#### `POST /admin/sources/crawl`

Обойти источник данных, чтобы выполнить интроспекцию его схемы и зарегистрировать таблицы. (REQ-012) [tool-verified: `provisa/api/admin/crawl_router.py:36`]

---

### Поиск таблиц источника

#### `GET /admin/sources/{source_id}/tables/search`

Искать доступные (ещё не зарегистрированные) таблицы в источнике по имени. [tool-verified: `provisa/api/admin/table_search_router.py:103`]

---

### Профилирование таблиц

#### `POST /admin/tables/{table_id}/profile`

Выполнить профиль столбцов зарегистрированной таблицы — кардинальность, min/max, доли null-значений. [tool-verified: `provisa/api/admin/table_profile_router.py:28`]

---

### Описания источников

#### `POST /admin/source-meta/db-description`

Сгенерировать описания таблиц и столбцов источника с помощью LLM. [tool-verified: `provisa/api/admin/source_meta_router.py:48`]

---

### Действия (функции и вебхуки)

Все эндпоинты находятся под префиксом `/admin/actions`. (REQ-205) [tool-verified: `provisa/api/admin/actions_router.py:24`]

Каждый вызов — из GraphQL, SQL, Cypher, Bolt, Arrow Flight, MCP `run_sql` и Provisa gRPC — маршрутизируется через единый управляемый исполнитель, который единообразно применяет `writable_by` и governance. (REQ-1156) [tool-verified: `provisa/api/data/action_exec.py`] См. [docs/integrations.md](integrations.md#_6) для синтаксиса вызова по каждому протоколу.

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

Каждый объект вебхука несёт булево поле `approved`. Вебхук считается одобренным, как только дата-стюард выполняет запрос на его создание (REQ-209); объявленные в конфигурации вебхуки одобряются автоматически. Неодобренный вебхук зарегистрирован, но не выставлен ни на одном интерфейсе. [tool-verified: `provisa/api/admin/actions_router.py:124-131`]

#### `POST /admin/actions/functions`

Зарегистрировать отслеживаемую функцию (команду). (REQ-205) [tool-verified: `provisa/api/admin/actions_router.py:117`]

**Ключевые поля:**

| Поле | Обязательно | Описание |
| --- | --- | --- |
| `name` | Да | Уникальное имя команды |
| `kind` | Да | `"query"` → поле GraphQL Query; `"mutation"` → поле Mutation |
| `implKind` | Нет | Как выполняется команда — см. таблицу ниже (по умолчанию `source_procedure`) |
| `binding` | Нет | Детали соединения, специфичные для `implKind` (JSON-объект) |
| `returnSchema` | Нет | JSON Schema `{type:"array", items:{type:"object", properties:{...}}}` — делает команду возвращающей набор строк на каждом интерфейсе |
| `arguments` | Нет | Определения аргументов `[{name, type}]`; позиционный порядок важен для вызывающих через SQL и Bolt |
| `visibleTo` | Нет | Идентификаторы ролей, которые могут вызывать команду |
| `writableBy` | Нет | Идентификаторы ролей, которым разрешено вызывать её как мутацию |
| `domainId` | Нет | Домен для размещения в GraphQL и контроля доступа |

**Значения `implKind`:**

| `implKind` | Что выполняется | Поля `binding` |
| --- | --- | --- |
| `source_procedure` | Хранимая процедура на зарегистрированном источнике (по умолчанию) | `sourceId`, `schemaName`, `functionName` |
| `script` | Серверный скрипт | `script` |
| `http` | Исходящий вызов HTTP | `url`, `method` |
| `grpc` | Исходящий вызов gRPC к внешнему серверу | `target`, `method` |
| `python` | Python-вызываемый объект, размещённый Provisa (REQ-885) | `callable` (например, `"demo.py_functions:random_dataset"`) |

Демонстрационные команды `random_python_set` (`implKind: python`) и `random_grpc_set` (`implKind: grpc`) показывают на практике команды, возвращающие набор строк, с `returnSchema`; обе находятся в `config/provisa-install.yaml`. [tool-verified: `config/provisa-install.yaml:809-856`]

#### `PUT /admin/actions/functions/{name}`

Обновить отслеживаемую функцию по имени. [tool-verified: `provisa/api/admin/actions_router.py:182`]

#### `DELETE /admin/actions/functions/{name}`

Удалить отслеживаемую функцию по имени. [tool-verified: `provisa/api/admin/actions_router.py:233`]

#### `POST /admin/actions/webhooks`

Зарегистрировать отслеживаемый вебхук. (REQ-209) Регистрация или обновление вебхука ставит в очередь запрос на одобрение дата-стюардом — вебхук становится активным на всех интерфейсах только после одобрения дата-стюардом. Объявленные в конфигурации вебхуки одобряются автоматически. **Поля тела запроса:** `name`, `url`, `method`, `timeoutMs`, `returns`, `inlineReturnType`, `arguments`, `visibleTo`, `domainId`, `description`, `kind`. [tool-verified: `provisa/api/admin/actions_router.py:132`, `provisa/api/admin/actions_router.py:325-331`]

#### `PUT /admin/actions/webhooks/{name}`

Обновить отслеживаемый вебхук по имени. Любое изменение сбрасывает одобрение до статуса ожидания, пока не будет одобрено повторно. [tool-verified: `provisa/api/admin/actions_router.py:306`]

#### `DELETE /admin/actions/webhooks/{name}`

Удалить отслеживаемый вебхук по имени. [tool-verified: `provisa/api/admin/actions_router.py:355`]

#### `POST /admin/actions/test`

Протестировать действие (функцию или вебхук) по имени. (REQ-245) [tool-verified: `provisa/api/admin/actions_router.py:384`]

---

### Роли

Все эндпоинты находятся под префиксом `/admin/roles`. [tool-verified: `provisa/api/admin/roles_router.py:18`]

| Метод | Путь | Описание |
| --- | --- | --- |
| `GET` | `/admin/roles/` | Список всех ролей |
| `POST` | `/admin/roles/` | Создать роль |
| `PUT` | `/admin/roles/{role_id}` | Обновить роль |
| `DELETE` | `/admin/roles/{role_id}` | Удалить роль |

[tool-verified: `provisa/api/admin/roles_router.py`]

---

### Пользователи

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

### Организации

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

### Приглашения

Все эндпоинты находятся под `/admin/invites`. [tool-verified: `provisa/api/admin/invites_router.py:18`]

| Метод | Путь | Описание |
| --- | --- | --- |
| `POST` | `/admin/invites/` | Создать приглашение |
| `GET` | `/admin/invites/` | Список ожидающих приглашений |
| `DELETE` | `/admin/invites/{token}` | Отозвать приглашение |

---

### Admin GraphQL

#### `POST /admin/graphql`

Эндпоинт Strawberry GraphQL для всех административных операций: CRUD источников и таблиц, управление связями, настройка доменов, правила RLS, управление кешем, соглашения об именовании, управление плановыми задачами и компиляция запросов. (REQ-164) [tool-verified: `provisa/api/app.py:2171`]

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

### Настройка

#### `GET /setup/status`

Вернуть статус первоначальной настройки. Всегда без аутентификации. (REQ-539) [tool-verified: `provisa/api/setup_router.py:100`]

#### `POST /setup/`

Завершить первоначальную настройку. [tool-verified: `provisa/api/setup_router.py:142`]

---

## Проверка состояния

#### `GET /health` или `HEAD /health`

Возвращает `{"status": "ok"}`. Всегда без аутентификации. (REQ-539) [tool-verified: `provisa/api/app.py:2258`]

---

## Ответы с ошибками

| Статус | Значение |
| --- | --- |
| 400 | Неверный запрос, ошибка валидации или ошибка разбора SQL |
| 401 | Отсутствующий или недействительный токен аутентификации |
| 403 | Недостаточно возможностей; нарушение governance |
| 404 | Роль, ресурс или файл конфигурации не найден |
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

Все остальные ошибки используют: `{"detail": "<message>"}`.

---

## Эндпоинт Arrow Flight

Порт `8815`. Нативный колоночный транспорт Arrow через gRPC. (REQ-143, REQ-045) [tool-verified: `provisa/api/flight/server.py`]

Запросы и обнаружение каталога доступны на одном и том же соединении. Полный конвейер governance (RLS, маскирование, выборка) применяется к каждому запросу. (REQ-130, REQ-143)

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

Когда доступен прокси Zaychik Flight SQL (порт 8480), пакеты записей передаются потоком от начала до конца без полной материализации. (REQ-144) Если Zaychik недоступен, используется запасной вариант с материализацией через слой федеративных запросов. (REQ-146)

---

## Эндпоинт Protobuf gRPC

Порт `50051` (переопределяется переменной окружения `GRPC_PORT` или конфигурацией `server.grpc_port`). (REQ-529) [tool-verified: `provisa/grpc/server.py`, `provisa/api/app.py`]

Передайте роль в ключе метаданных gRPC `x-provisa-role`. Если отсутствует, сервер прерывает соединение с `UNAUTHENTICATED`. [tool-verified: `provisa/grpc/server.py`]

Скачайте proto для конкретной роли с `GET /data/proto/{role_id}`. Отображаются только таблицы и столбцы, видимые этой роли. (REQ-039)

```proto
service ProvisaService {
  rpc QueryOrders (QueryOrdersRequest) returns (stream Orders);
  rpc InsertOrders (InsertOrdersRequest) returns (InsertOrdersResponse);
}
```

Каждая таблица производит потоковый RPC `Query{TypeName}`. RPC `Insert{TypeName}` существуют для симметрии схемы, но прерываются с `UNIMPLEMENTED`. [tool-verified: `provisa/grpc/server.py`]

`grpc_reflection.v1alpha` включён для обнаружения сервисов без предварительно скомпилированного proto. (REQ-529) [tool-verified: `provisa/grpc/reflection.py`]

```bash
grpcurl -plaintext localhost:50051 list
grpcurl -plaintext -H 'x-provisa-role: analyst' \
  -d '{}' localhost:50051 ProvisaService/QueryOrders
```

Сервер gRPC запускается только тогда, когда действительный proto может быть скомпилирован при запуске. Если сборка схемы завершается сбоем, сервер gRPC не запускается. (REQ-529)

#### Агрегатные и группирующие RPC (REQ-1359, REQ-1361, REQ-1405)

Когда у таблицы установлено `enable_aggregates`, сгенерированный proto включает два дополнительных RPC наряду с `Query{TypeName}`:

- **`Query{TypeName}Aggregate`** — возвращает агрегатные скаляры для таблицы (`count`; `sum`, `avg`, `stddev`, `variance` для каждого числового столбца; `min`, `max` для каждого сравнимого столбца)
- **`Query{TypeName}GroupBy`** — возвращает одну строку на ключ группы с агрегатными подполями и, опционально, скалярами базовой таблицы и строками присоединённых измерений в поле `nodes`

Оба маршрутизируются через тот же конвейер компилятора агрегации, что и корневые поля GraphQL `{field}_aggregate` и `{field}_group_by` — отдельной реализации агрегации нет. (REQ-1359) [tool-verified: `provisa/grpc/query_ir.py:133-196`]

**Поле `funcs` (REQ-1361).** Сообщение запроса принимает поле `funcs` — повторяющуюся строку. Допустимые значения: `count`, `sum`, `avg`, `stddev`, `variance`, `min` и `max`. Когда `funcs` опущено, запрашивается каждая функция, которую схема предоставляет для этой таблицы. Когда оно задано, отображаются только названные функции. Если ни одна из названных функций не применима к типам столбцов таблицы, запрос откатывается к `count`. [tool-verified: `provisa/grpc/query_ir.py:66`, `provisa/grpc/query_ir.py:75-97`]

**Поля `include_nodes` и `include` (REQ-1405).** Запросы `Query{TypeName}GroupBy` могут установить `include_nodes: true`, чтобы включить скалярные столбцы базовой таблицы в поле `nodes` каждой строки. Повторяющееся строковое поле `include` называет поля связей «многие-к-одному», чьи скалярные столбцы также вложены внутрь `nodes`. Это соответствует поведению JSON:API `?includeNodes=` / `?include=`. [tool-verified: `provisa/grpc/query_ir.py:168-195`]

---

## Драйвер JDBC

Драйвер JDBC Provisa (`provisa-jdbc-0.1.0.jar`) предоставляет семантический каталог инструментам BI (Tableau, PowerBI, DBeaver). (REQ-126)

**URL соединения:** `jdbc:provisa://host:port` (REQ-131)

Домены сопоставляются со схемами JDBC. (REQ-127) Таблицы используют свои зарегистрированные алиасы. Столбцы используют алиасы и выставляют описания как `REMARKS`. (REQ-128) Стандартные методы метаданных (`getPrimaryKeys`, `getImportedKeys`, `getExportedKeys`) выставляют семантические связи как метаданные PK/FK.

**Поддержка SQL:** `SELECT * FROM <alias> [WHERE col = 'value']`. (REQ-129)

Драйвер по умолчанию запрашивает перенаправление Arrow IPC. Результаты передаются потоком пакет за пакетом через `ArrowStreamReader`, ограничены одним пакетом записей в памяти. (REQ-293)

---

## Формат аргумента `orderBy`

Аргумент `order_by` использует объекты `{column: direction}` с перечислением из 6 направлений: (REQ-200)

```json
{
  "query": "{ orders(order_by: [{created_at: desc_nulls_last}]) { id created_at } }",
  "role": "admin"
}
```

Поддерживаемые направления: `asc`, `desc`, `asc_nulls_first`, `asc_nulls_last`, `desc_nulls_first`, `desc_nulls_last`. (REQ-201)

---

## Подписки

Подписки SSE доступны по адресу `GET /data/subscribe/{table}`. (REQ-219, REQ-258) Доставка уведомлений использует подключаемого провайдера, выбираемого по типу источника: источники PostgreSQL используют `LISTEN/NOTIFY`, источники MongoDB используют Change Streams, а источники Kafka используют группы потребителей. Фильтрация RLS и проверка схемы применяются независимо от провайдера. Источники WebSocket и RSS также поддерживаются через тот же эндпоинт. (REQ-338, REQ-342) [tool-verified: `provisa/api/data/subscribe.py:239`, `provisa/subscriptions/registry.py`, `provisa/api/app.py` `_rebuild_schemas`]

---

## Бизнес-глоссарий (REQ-1387)

Бизнес-глоссарий сопоставляет физические имена полей — в том виде, в каком они существуют в исходных базах данных, — с общим человекочитаемым словарём. Каждый столбец, зарегистрированный в семантическом слое, автоматически получает термин. Для наполнения глоссария не требуется ручной ввод; кураторы добавляют определения, связи и экспертов поверх того, что система выводит сама.

### Как выводятся термины

Когда Provisa регистрирует или обновляет столбцы таблицы, `normalize_term` (`provisa/core/glossary.py`) выполняется для каждого имени столбца и производит каноническую фразу. [tool-verified: `provisa/core/repositories/glossary.py:sync_table_refs`]

Нормализация применяет пять правил последовательно:

1. Разделение по границам camelCase и разделительным символам (`_`, `-`, `.`, `/`, пробелы).
2. Приведение результата к нижнему регистру.
3. Расширение фиксированной таблицы сокращений (например, `cust` → `customer`, `amt` → `amount`, `dt` → `date`, `id` → `identifier`, `key` → `identifier`, `guid` → `identifier`).
4. Удаление завершающего **токена-заместителя** (`identifier`, `code`, `index` или `reference`) — столбец, названный по своему ключу или коду, указывает на лежащее в основе понятие через значение-заместитель, поэтому термином должно быть само понятие. Последний оставшийся токен никогда не удаляется.
5. Уточнение **слишком общей фразы** концепцией таблицы. Когда полностью нормализованная фраза представляет собой голое слово-атрибут (`name`, `identifier`, `date`, `location`, `message`, `first name`, `last name` и подобные), термин становится `<концепция таблицы> <фраза>` — `employees.first_name` → `employee first name`, `orders.id` → `order identifier`. Один общий термин `name` для разных таблиц объединил бы разные значения; уточнение вместо этого связывает каждый столбец с его охватывающей концепцией. Концепция таблицы — это бизнес-имя таблицы, нормализованное с единственным числом главного существительного (`order_lines` → `order line`).

Псевдо-столбцы нативных фильтров (с префиксом `_nf_` или любой столбец, несущий `native_filter_type`) — это служебные параметры запроса, а не бизнес-поля, и термины для них не выводятся.

Поскольку `id`, `key`, `pk` и `sk` все расширяются в `identifier` перед проверкой на заместитель, три физически разных имени столбца попадают ровно на один и тот же термин:

| Физическое имя | После нормализации |
| --- | --- |
| `cust_id` | `customer` |
| `customerId` | `customer` |
| `CUSTOMER_KEY` | `customer` |
| `txn_amt` | `transaction amount` |

Первые три схлопываются в один термин. `transaction amount` сохраняет оба токена, потому что `amount` не является заместителем. Голый столбец `id` — без предшествующих токенов — не может быть усечён; он нормализуется в `identifier`, чтобы термин не был пустым. [tool-verified: `provisa/core/glossary.py:normalize_term`]

### Жизненный цикл

Термины **выводятся из членства в семантическом слое**, а не создаются пользователями по запросу. Репозиторий таблиц — единственный путь записи: `sync_table_refs` выполняется внутри каждого upsert набора столбцов, а `sweep_refless_terms` выполняется после любого пути удаления. [tool-verified: `provisa/core/repositories/glossary.py`]

**Когда столбец добавляется:** Provisa ищет нормализованный термин по имени. Если он уже существует, столбец получает ссылку на него (и если термин был устаревшим, он восстанавливается — `deprecated` сбрасывается обратно в `False`). Если термина ещё не существует, он создаётся.

**Когда столбец исчезает** (изменение схемы или удаление таблицы): его ссылка удаляется, и термин **улаживается** по правилу «удалить или пометить устаревшим». Корневой термин без оставшихся ссылок удаляется полностью — вместе со своими рёбрами и назначениями экспертов, — если только его удаление не оставит абстрактный термин отсоединённым от всех корневых терминов (без пути через граф терминов). В этом случае термин помечается **устаревшим** (`deprecated=True`) вместо удаления, чтобы якорь графа абстрактного термина сохранился.

Абстрактные термины никогда не удаляются автоматически; они существуют вне физического жизненного цикла и удаляются только явно через admin API.

**Восстановление:** если нормализованное имя устаревшего термина появляется снова (столбец повторно регистрируется), пометка термина снимается, и его ссылки снова начинают накапливаться.

### Эндпоинты курирования

Все эндпоинты находятся под `/admin/glossary`. Они требуют доступа `org_admin` и настроенной организации. Каждая мутация запускает публикацию метаданных. [tool-verified: `provisa/api/admin/glossary_router.py`]

| Метод | Путь | Описание |
| --- | --- | --- |
| `GET` | `/admin/glossary/terms` | Список терминов. Параметры запроса: `q` (поиск по имени/определению), `include_deprecated` (по умолчанию `true`) |
| `GET` | `/admin/glossary/terms/{term_id}` | Получить детали термина: определение, физические ссылки, типизированные рёбра, эксперты |
| `POST` | `/admin/glossary/terms` | Создать абстрактный термин — пользовательский словарь без физических ссылок |
| `PATCH` | `/admin/glossary/terms/{term_id}` | Переименовать, задать определение или переключить исключение из экспорта |
| `DELETE` | `/admin/glossary/terms/{term_id}` | Удалить термин, у которого нет физических ссылок |
| `POST` | `/admin/glossary/refs/move` | Переместить одну физическую ссылку на другой термин (консолидация) |
| `POST` | `/admin/glossary/terms/{term_id}/edges` | Добавить типизированное ребро связи между двумя терминами |
| `DELETE` | `/admin/glossary/terms/{term_id}/edges` | Удалить ребро (параметры запроса: `to_term_id`, `rel_type`) |
| `POST` | `/admin/glossary/terms/{term_id}/experts` | Пометить пользователя как эксперта или автора термина |
| `DELETE` | `/admin/glossary/terms/{term_id}/experts/{user_id}` | Удалить назначение эксперта/автора у пользователя |
| `POST` | `/admin/glossary/terms/{term_id}/definition/generate` | Составить черновик определения для одного термина с помощью AI-модели организации — возвращает только текст, ничего не сохраняется до подтверждения |
| `POST` | `/admin/glossary/definitions/generate` | Сгенерировать и сохранить определения для каждого термина, у которого их ещё нет — никогда не перезаписывает текст, написанный человеком |
| `POST` | `/admin/glossary/relationships/generate` | Предложить и сохранить типизированные рёбра по всему глоссарию с помощью AI-модели организации |

**Тело `POST /admin/glossary/terms`:**

```json
{"name": "revenue", "definition": "Recognized net revenue after returns and discounts."}
```

**Тело `POST /admin/glossary/terms/{term_id}/edges`:**

```json
{"to_term_id": 42, "rel_type": "KIND_OF"}
```

Допустимые значения `rel_type`: `KIND_OF`, `RELATED_TO`, `PART_OF`, `SYNONYM_OF`. [tool-verified: `provisa/core/glossary.py:TERM_EDGE_TYPES`]

**Тело `POST /admin/glossary/terms/{term_id}/experts`:**

```json
{"user_id": "alice@example.com", "kind": "author"}
```

Допустимые значения `kind`: `expert`, `author`. [tool-verified: `provisa/core/repositories/glossary.py:add_expert`]

**Тело `POST /admin/glossary/refs/move`:**

```json
{"table_id": 7, "column_name": "cust_id", "to_term_id": 12}
```

Перемещение ссылки улаживает теряющий термин по правилу «удалить или пометить устаревшим». Используйте это для консолидации двух терминов, которые нормализация оставила раздельными, — например, после того как источник использовал нестандартное сокращение, выпавшее за рамки таблицы расширений.

Удаление корневого термина (у которого есть физические ссылки) возвращает `400 glossary.invalid`. Сначала удалите или переместите все ссылки.

**`PATCH /admin/glossary/terms/{term_id}` — поле `export_excluded`:**

```json
{"export_excluded": true}
```

Установка `export_excluded` в `true` исключает термин из всех снимков экспорта метаданных, независимо от его физических ссылок или абстрактного статуса. Возврат в `false` восстанавливает термин в снимке при следующей публикации. Данные курирования (определение, рёбра, эксперты) не затрагиваются. [tool-verified: `provisa/core/repositories/glossary.py:set_export_excluded`, `provisa/api/admin/glossary_router.py:update_term`]

### Курирование с помощью AI

Настроенная AI-модель организации может составлять черновики определений и предлагать рёбра связей по всему глоссарию за одну операцию. Оба массовых действия требуют доступа `org_admin` и настроенной организации.

**`POST /admin/glossary/definitions/generate`**

Проходит по каждому термину глоссария, пропускает те, у которых уже есть определение, и вызывает AI-модель организации, чтобы составить черновик для каждого оставшегося термина. Черновик сохраняется немедленно — в отличие от пооконечного эндпоинта черновика (`POST /admin/glossary/terms/{term_id}/definition/generate`), здесь нет шага редактирования. Определения, написанные человеком, никогда не перезаписываются: защита — `if summary["definition"]: continue` перед любым вызовом модели. Одно уведомление о публикации покрывает всю партию. [tool-verified: `provisa/api/admin/glossary_router.py:generate_all_definitions`]

Ответ:

```json
{"generated": 12}
```

`generated` — это количество терминов, получивших новое определение. Значение равно нулю, когда у каждого термина уже есть определение.

**`POST /admin/glossary/relationships/generate`**

Отправляет полный список терминов AI-модели организации с промптом, который задаёт десять допустимых типов рёбер (`KIND_OF`, `PART_OF`, `SYNONYM_OF`, `RELATED_TO`, `VALID_VALUE_OF`, `DERIVED_FROM`, `REPLACES`, `PREFERRED_TERM_FOR`, `TRANSLATION_OF`, `ANTONYM_OF`) и запрашивает только уверенные предложения. Модель отвечает массивом JSON; каждая запись проверяется перед любой записью: неизвестные имена терминов, самопетли и типы рёбер вне закрытого перечисления бесшумно отбрасываются. Валидные предложения записываются идемпотентно через upsert — повторный запуск действия не дублирует рёбра. Одно уведомление о публикации покрывает партию. Эндпоинт немедленно возвращает `{"added": 0}`, когда в глоссарии менее двух неустаревших терминов. [tool-verified: `provisa/api/admin/glossary_router.py:generate_relationships`]

Ответ:

```json
{"added": 5}
```

`added` — это количество записанных рёбер. Ребро, которое уже существовало, всё равно засчитывается — upsert выполняется успешно, но данные ребра не меняются.

### Инструмент MCP `search_terms`

```
search_terms(query, role=None, limit=25)
```

Ищет по именам и определениям терминов без учёта регистра как подстроку, до `limit` результатов. Каждый результат — это полная деталь термина: `name`, `definition`, `is_abstract`, `deprecated`, физические ссылки (с `source_id`, `schema_name`, `table_name`, `column_name`), типизированные рёбра и назначения экспертов. [tool-verified: `provisa/api/mcp/server.py:236-244`, `provisa/core/repositories/glossary.py:search_terms`]

Используйте `search_terms` перед написанием SQL, чтобы найти каждое физическое поле, представляющее концепцию по имени. Например, поиск `"order date"` возвращает термин и все столбцы `order_dt`, `orderDate`, `ORDER_DATE` во всех зарегистрированных таблицах.

### Экспорт метаданных

Граф терминов глоссария включён в каждый `MetadataSnapshot`, построенный `build_snapshot`. [tool-verified: `provisa/api/metadata_export/builder.py:_glossary_assets`]

Экспорт применяет те же фильтры, что и остальная часть снимка:

- Термин, помеченный `export_excluded`, полностью исключается — независимо от его физических ссылок, абстрактного статуса или того, настроен ли каталог организации. [tool-verified: `provisa/api/metadata_export/builder.py:_glossary_assets`]
- Корневой термин публикуется только тогда, когда хотя бы одна из его физических ссылок принадлежит столбцу, который проходит как фильтр **Data Product** (флаг `data_product` таблицы должен быть `true`), так и фильтр **технических** столбцов (столбцы, помеченные `technical`, исключаются).
- Корневой термин, все ссылки которого исключены этими фильтрами, исключается вместе с ними.
- Абстрактные термины публикуются безусловно — они являются пользовательским словарём, не привязанным к физическим столбцам.
- Ребро между двумя терминами публикуется только тогда, когда публикуются оба конечных термина.

Каждый вендорский адаптер публикует граф терминов нативно, в собственный контейнер глоссария Provisa, который он создаёт идемпотентно — никогда в существующий глоссарий каталога:

| Провайдер | Контейнер | Термины | Связи | Устаревание |
| --- | --- | --- | --- | --- |
| Apache Atlas | «Provisa Glossary» (API глоссария) | термины глоссария, определение в `longDescription` | KIND_OF → `isA`, SYNONYM_OF → `synonyms`, RELATED_TO/PART_OF → `seeAlso` | маркер `[DEPRECATED]` в shortDescription |
| Atlan | Глоссарий Provisa по стабильному qualifiedName | `longDescription` (никогда написанное человеком `userDescription`) | то же сопоставление Atlas | `certificateStatus = DEPRECATED` |
| DataHub | `urn:li:glossaryNode:provisa.<org>` | аспект `glossaryTermInfo` на термин | KIND_OF → Inherits, PART_OF → Contains (инвертировано), RELATED_TO/SYNONYM_OF → related terms | аспект deprecation; переименования следуют преемственности URN |
| OpenMetadata | Глоссарий Provisa через `/v1/glossaries` | PUT по fqn-ключу, переименования PATCH-перепривязка по сохранённому UUID | KIND_OF → нативная родительская иерархия, SYNONYM_OF → `synonyms`, остальные → `relatedTerms` | `entityStatus` |
| Collibra | Домен типа «Глоссарий» «Provisa Glossary» | активы Business Term через Import API | нативные типы связей Business Term | статус актива |

Привязка, а не имя, служит владением: vendor-идентификатор каждого опубликованного термина фиксируется в `catalog_bindings` под URN термина (`provisa://<org>/terms/<name>`), и Provisa изменяет или удаляет элемент глоссария на стороне вендора только тогда, когда владеет этой привязкой (или элемент находится в собственном контейнере Provisa, который она создала). Элемент глоссария без привязки Provisa возник во внешней системе и никогда не затрагивается; обновления выполняют чтение-слияние, чтобы поля, добавленные дата-стюардом к собственным терминам Provisa, сохранялись; ничего не удаляется, когда термин покидает снимок. Назначения терминов активам, сделанные дата-стюардом, остаются во владении внешней системы — ни один адаптер не записывает назначения термин-актив (публикация назначений, авторизованных Provisa, — явное последующее направление работы). Именно в Collibra безопасность в рамках семантики REPLACE Import API опирается на изоляцию: полезная нагрузка упоминает только активы внутри домена глоссария Provisa и экземпляры связей только между терминами Provisa, поэтому глоссарии дата-стюардов и их связи никогда не оказываются достижимыми. [tool-verified: `provisa/api/metadata_export/atlan.py`, `provisa/api/metadata_export/datahub.py`, `provisa/api/metadata_export/atlas.py`, `provisa/api/metadata_export/openmetadata.py`]
