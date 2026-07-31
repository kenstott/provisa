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
  "role": "admin",
  "discovery_mode": false
}
```

Флаг `discovery_mode` расширяет проверку видимости таблиц, включая все таблицы из всех контекстов. Только для внутренних инструментов. [tool-verified: `provisa/api/data/endpoint_dev.py:148-152`]

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

---

### `GET /data/jsonapi/{domain_id}/{table_name}`

Автоматически сгенерированный эндпоинт, совместимый с [JSON:API](https://jsonapi.org), для каждой зарегистрированной таблицы. Те же RLS, маскирование и маршрутизация, что и в GraphQL. (REQ-257) [tool-verified: `provisa/api/jsonapi/generator.py:284`]

**Заголовок `Accept`:** должен включать `application/vnd.api+json` (медиа-тип JSON:API), иначе запрос возвращает `406`.

**Параметры запроса:**

- `fields[<type>]` — разреженные наборы полей, например `?fields[orders]=amount`
- `filter[<col>]` / `filter[<col>][<op>]` — например `?filter[region]=US`, `?filter[amount][gt]=100`
- `sort` — через запятую, префикс `-` для убывания, например `?sort=-created_at,amount`
- `page[number]` / `page[size]` — пагинация

Ответы — это объекты ресурсов с `type`/`id`/`attributes`. Ошибки следуют форме объекта ошибки JSON:API.

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
