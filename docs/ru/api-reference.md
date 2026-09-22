# Справочник API

## Обзор

Provisa предоставляет REST-эндпоинты под двумя префиксами: `/data` для выполнения запросов и интроспекции схемы, и `/admin` для управления конфигурацией. (REQ-043) Большинство эндпоинтов данных требуют идентификатор роли. Операции конфигурации admin используют Strawberry GraphQL API по адресу `/admin/graphql`. (REQ-164)

---

## Аутентификация

Когда `auth.provider` настроен в `provisa.yaml`, все эндпоинты, кроме `/health` и `/setup/status`, требуют заголовок `Authorization: Bearer <token>`. (REQ-120) [tool-verified: `provisa/api/app.py`, `provisa/auth/wiring.py`]

Без настроенной аутентификации сервер работает в режиме разработки. Любой запрос обрабатывается как identity `anonymous`, которая сопоставляется со всеми настроенными ролями с доступом к доменам по маске (wildcard). (REQ-535)

**Вход (`POST /auth/login`)** предоставляется активным провайдером аутентификации, когда настроено `provider: basic`. (REQ-124) Формат учётных данных и ответа зависит от провайдера.

**Интроспекция identity:**

```http
GET /auth/me
```

Возвращает id, email, отображаемое имя, членства в организациях и назначенные роли аутентифицированного пользователя. В режиме разработки возвращает `dev_mode: true` со списком всех id ролей. [tool-verified: `provisa/api/auth_router.py`]

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

Поле `role` используется только в режиме разработки (без аутентификации). Когда аутентификация активна, используется роль аутентифицированного пользователя, а `role` в теле игнорируется.

Поле `extensions` поддерживает протокол Automatic Persisted Query (APQ): (REQ-288)

```json
{
  "extensions": {"persistedQuery": {"sha256Hash": "<sha256-of-query>"}}
}
```

**Заголовки:**

- `X-Provisa-Role` — переопределение роли (режим разработки)
- `Accept` — формат ответа (см. раздел «Согласование содержимого»)
- `Authorization` — `Bearer <token>`, когда включена аутентификация
- `X-Provisa-Redirect-Format` — MIME-тип для вывода перенаправления в S3 (REQ-137)
- `X-Provisa-Redirect-Threshold` — количество строк, выше которого срабатывает перенаправление (REQ-137)
- `X-Provisa-Redirect` — `true` для безусловного принудительного перенаправления (REQ-029)

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

**Ответ (несколько корневых полей со смешанным inline/redirect):**

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

Запросы с несколькими корневыми полями выполняют каждое корневое поле независимо. Поля ниже порога перенаправления возвращаются inline; поля выше — перенаправляются. Ключ `redirects` (во множественном числе) сопоставляет имена полей с информацией о перенаправлении. (REQ-029) [tool-verified: `provisa/api/data/endpoint.py`]

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

### Перенаправление

Результаты, превышающие настроенный порог по количеству строк (или при `X-Provisa-Redirect: true`), записываются в S3, и возвращается предподписанный (presigned) URL. (REQ-029, REQ-044)

| Формат перенаправления | Кем записывается | Память |
| --- | --- | --- |
| `application/vnd.apache.parquet` | федеративный CTAS | Не используется — данные никогда не проходят через Provisa |
| `application/x-orc` | федеративный CTAS | Не используется — данные никогда не проходят через Provisa |
| `application/json` | Provisa | Ограничено памятью |
| `application/x-ndjson` | Provisa | Ограничено памятью |
| `text/csv` | Provisa | Ограничено памятью |
| `application/vnd.apache.arrow.stream` | Provisa | Ограничено памятью |

Для крупных аналитических экспортов используйте перенаправление в Parquet или ORC. Движок федерации пишет напрямую в S3 параллельно — данные не проходят через Provisa. (REQ-138)

```yaml
X-Provisa-Redirect-Format: application/vnd.apache.parquet
X-Provisa-Redirect-Threshold: 1000
```

---

### `POST /data/sql`

Выполнить необработанный SQL через конвейер управления Stage 2. (REQ-267) [tool-verified: `provisa/api/data/endpoint_dev.py:62`]

**Тело запроса:**

```json
{
  "sql": "SELECT id, amount FROM orders WHERE region = 'us'",
  "role": "admin"
}
```

**Требуемые возможности:** `QUERY_DEVELOPMENT`.

Нарушения управления на `POST /data/sql` возвращают HTTP 403. (REQ-002, REQ-266)

**Ответ:** тот же формат, что и `/data/graphql` (строки JSON по умолчанию, согласование содержимого через `Accept`).

---

### `POST /data/query`

Единый эндпоинт запросов. Принимает GraphQL, SQL или Cypher — синтаксис определяется автоматически. (REQ-267) [tool-verified: `provisa/api/data/endpoint_dev.py:509`]

Запросы Cypher также можно отправлять на выделенный эндпоинт `POST /query/cypher`. (REQ-345)

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

Установите `analyze: true`, чтобы выполнить EXPLAIN ANALYZE. Запрос выполняется, и план содержит реальные количества строк и тайминги. Не каждый диалект поддерживает ANALYZE; см. таблицу в разделе [Планы запросов и статистика](engines.md#query-plans-and-statistics).

**Ответ:** `{"plan": "<plan text or JSON>", "dialect": "trino", "analyzed": false}`

`400`, если диалект не поддерживает EXPLAIN, или если запрошен `analyze: true` для диалекта, который его не поддерживает (например, SQLite). [tool-verified: `provisa/executor/explain.py:wrap_explain`, `analyze_sql`]

---

### `GET /data/engine/state`

Возвращает текущее состояние шарда движка без его пробуждения. (REQ-1516) [tool-verified: `provisa/api/data/endpoint_dev.py:892`]

UI опрашивает этот эндпоинт, чтобы показать баннер запуска, пока движок выполняет холодный старт. Он никогда не вызывает пробуждение — опрос безопасен и не считается активностью для «жнеца простоя» (idle reaper).

**Ответ:**

```json
{"state": "ready"}
```

Возможные значения:

| Состояние | Значение |
| --- | --- |
| `always-on` | Настольная версия, self-hosted или собственный (BYO) координатор — без управления жизненным циклом |
| `ready` | Шард запущен и принимает запросы |
| `starting` | Выполняется холодный старт |
| `stopped` | Шард масштабирован до нуля |

[tool-verified: `provisa/federation/engine_wake.py:engine_state`]

---

### `POST /data/engine/prewarm`

Инициировать пробуждение движка без выполнения запроса. (REQ-1516) [tool-verified: `provisa/api/data/endpoint_dev.py:913`]

Немедленно возвращает `202 Accepted`. Пробуждение выполняется в фоновом режиме. Используйте это, если хотите, чтобы движок был готов до прихода первого запроса — например, из планировщика, который выполнит запросы через несколько минут.

**Ответ:** `202 Accepted`, тело `{"started": true}`

[tool-verified: `provisa/federation/engine_wake.py:prewarm_engine`]

---

### `GET /data/rest/{domain_id}/{table_name}`

Автоматически сгенерированный обычный REST-эндпоинт для каждой зарегистрированной таблицы. Строка запроса сопоставляется с аргументами GraphQL, и запрос компилируется и выполняется через тот же конвейер (RLS, маскирование, маршрутизация), что и GraphQL. (REQ-256) [tool-verified: `provisa/api/rest/generator.py:153`]

**Параметры запроса:**

- `limit` — максимальное число строк (≥ 1)
- `offset` — пропустить строки (≥ 0)
- `fields` — имена столбцов через запятую (по умолчанию все скалярные поля)
- `filter` — JSON-массив объектов фильтра `{"field", "comparator", "value"}`
- `orderBy` — JSON-массив объектов сортировки `{"field", "direction"}`

Аутентифицированная роль обязательна; неаутентифицированные запросы возвращают `401`. Спецификация OpenAPI для этих маршрутов предоставляется по адресу `GET /data/rest/openapi.json`, со Swagger UI по адресу `GET /data/rest/docs`.

#### Обозреватель OpenAPI / Swagger UI

Страница обозревателя OpenAPI (`/app/openapi`) встраивает Swagger UI в изолированный (sandboxed) iframe. Спецификация ограничена ролью — отображаются только таблицы и столбцы, видимые текущей роли, — и опционально фильтруется по домену через селектор домена. UI автоматически переключается между светлой и тёмной темами. [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:20-34`]

Страница загружает HTML спецификации через `fetch()`, а не напрямую через `src` iframe, поэтому запрос несёт bearer-токен сессии, а собственные относительные запросы Swagger UI корректно разрешаются относительно того же origin. [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:44-69`]

При переходе по ссылке NL «Open in OpenAPI» страница автоматически разворачивает целевой эндпоинт, заполняет параметры запроса из URL, сгенерированного NL (например, `aggregate`, `groupBy`), и нажимает Execute — используя опрос DOM, чтобы гарантировать завершение каждого шага перед выполнением следующего. (REQ-1359) [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:94-171`]

---

### `GET /data/jsonapi/{domain_id}/{table_name}`

Автоматически сгенерированный эндпоинт, совместимый с [JSON:API](https://jsonapi.org), для каждой зарегистрированной таблицы. Те же RLS, маскирование и маршрутизация, что и в GraphQL. (REQ-257) [tool-verified: `provisa/api/jsonapi/generator.py:284`]

**Заголовок `Accept`:** должен включать `application/vnd.api+json` (медиа-тип JSON:API), иначе запрос вернёт `406`.

**Параметры запроса:**

- `fields[<type>]` — разреженные наборы полей (sparse fieldsets), например `?fields[orders]=amount`
- `filter[<col>]` / `filter[<col>][<op>]` — например `?filter[region]=US`, `?filter[amount][gt]=100`
- `sort` — через запятую, префикс `-` для убывания, например `?sort=-created_at,amount`
- `page[number]` / `page[size]` — пагинация
- `aggregate` — агрегатные функции через запятую, выполняемые вместо получения строк: `count`, `sum`, `avg`, `stddev`, `variance`, `min`, `max`. Используйте `?aggregate=count,sum`, чтобы запросить подмножество. Ответы агрегации возвращают `data: null` с результатами в `meta.aggregate`. (REQ-1359) [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:238`]
- `groupBy` — имена столбцов через запятую; используется вместе с `?aggregate=` для группировки результатов. Допустимы только столбцы из перечисления `DistinctOnColumn` таблицы; сервер возвращает `400` для любого столбца, который роль не может видеть. (REQ-1361) [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:447`]
- `includeNodes` — `true`, чтобы включить скалярные столбцы базовой таблицы (и скалярные поля присоединённых измерений, названных в `include=`) внутрь массива `nodes` каждой строки группы. Требуется, когда NL-запрос с группировкой также запрашивает детали измерения. (REQ-1405)

Ответы представляют собой объекты-ресурсы с `type`/`id`/`attributes`. Ошибки следуют форме объекта ошибки JSON:API.

#### Обозреватель JSON:API

Страница обозревателя JSON:API (`/app/jsonapi`) — это браузерный UI поверх этих эндпоинтов. Выберите таблицу из списка, сгруппированного по доменам, затем настройте:

- **Поля (Fields)** — выберите, какие столбцы включить (разреженный набор полей); оставьте все невыбранными, чтобы запросить каждый столбец
- **Связи (Relationships)** — выберите имена связей, производных от внешних ключей, для подгрузки через `?include=`
- **Фильтр (Filter)** — поле, оператор (`eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `like`) и значение
- **Сортировка (Sort)** — одно поле, по возрастанию или убыванию
- **Агрегация (Aggregate)** — выберите столбцы группировки из проверенного сервером списка, затем отметьте одну или несколько агрегатных функций; при выбранных столбцах группировки флажок «Include nodes» добавляет скалярные столбцы базовой таблицы к каждой строке
- **Размер страницы (Page size)** — количество ресурсов на страницу, с навигацией первая/предыдущая/следующая/последняя

Результаты отображаются в отформатированном сводном представлении (карточки ресурсов с кликабельными якорями связей) или на вкладке необработанного JSON. Показывается актуальный URL запроса, который можно скопировать. Выбор таблицы и размер страницы сохраняются между сессиями в `localStorage`. [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx`]

При переходе по ссылке NL «Open in JSON:API» обозреватель заранее выбирает таблицу и заполняет средство выбора агрегации из параметров запроса, сгенерированных NL, затем автоматически выполняет запрос. [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:460-479`]

---

### `POST /query/nl`

Отправить вопрос на естественном языке. Сервис запускает асинхронное задание и немедленно возвращает `202 Accepted` с `job_id`. Требует настроенного провайдера LLM в разделе конфигурации `ai_models`. (REQ-354) [tool-verified: `provisa/api/rest/nl_router.py:50`]

**Тело запроса:**

```json
{"q": "How many orders were placed last month?", "role": "admin"}
```

Возвращает `{"job_id": "<id>"}`. Превышение ограничения частоты запросов NL для роли возвращает `429` с заголовком `Retry-After`. (REQ-370)

**Получение результата:**

- `GET /query/nl/{job_id}` — опрос. Возвращает документ задания.
- `GET /query/nl/{job_id}/stream` — SSE. Одно событие `branch` на каждую цель генерации по мере завершения, затем событие `done`. (REQ-357, REQ-358)

Три цикла генерации (Cypher, GraphQL, SQL) выполняются параллельно, каждый проверяется компилятором и уточняется при ошибке. (REQ-355) Подсказка (prompt) ограничена видимой схемой роли. (REQ-356) Итоговый документ индексирует каждую ветвь по цели: (REQ-357) [tool-verified: `provisa/nl/job.py:69`]

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

Ветвь, исчерпавшая лимит итераций, возвращает `query: null`, `result: null` и строку `error`. Каждый сгенерированный запрос выполняется в рамках прав потребителя с применением управления Stage 2 — сервис никогда не обходит управление. (REQ-359)

#### NL-группировка с деталями измерения (REQ-1405)

Когда NL-запрос с группировкой также проецирует столбцы из присоединённой таблицы измерения — например, «количество обращений по пользователю с именем и email пользователя», — исполнитель выводит пути через точку (`dim_paths`) для каждого поля на основе столбцов измерения, спроецированных в SELECT. Эти пути заполняют параметр `includeNodes=` в сгенерированных URL панелей JSON:API и OpenAPI, так что эти панели запрашивают те же поля присоединённого измерения, которые разрешили ветви SQL и GraphQL. Без этого `includeNodes=true` вернул бы только собственные скалярные поля базовой агрегатной таблицы. (REQ-1405) [tool-verified: `docs/arch/requirements.md:REQ-1405`]

На панели gRPC сгенерированный `{Type}GroupByRequest` несёт `include_nodes` (bool) и `include` (повторяющаяся строка имён полей связи). Возвращаемый `{Type}GroupByRow` включает типизированное поле `nodes` со строками деталей измерения. [tool-verified: `provisa/grpc/query_ir.py:168-196`]

---

### `GET /data/sdl`

Возвращает GraphQL SDL для схемы роли. (REQ-008) [tool-verified: `provisa/api/data/sdl.py:137`]

**Заголовки:** `X-Role: <role_id>` (обязателен)

**Параметры запроса:**

- `domain` — id доменов через запятую. Если задан, ответ фильтруется по указанным доменам и таблицам, достижимым из них.

**Ответ:** GraphQL SDL в формате `text/plain`.

---

### `GET /data/introspection`

Возвращает JSON интроспекции GraphQL, опционально отфильтрованный по домену. [tool-verified: `provisa/api/data/sdl.py:200`]

**Заголовки:** `X-Provisa-Role: <role_id>` (обязателен)

**Параметры запроса:** `domain` — id доменов через запятую.

**Ответ:** результат интроспекции в формате `application/json`.

---

### `GET /data/graph-schema`

Возвращает представление схемы роли в виде графа: метки узлов и типы их связей, для клиентов Cypher/графа. Включает `pk_columns` для каждой метки узла, чтобы вызывающие могли определить столбцы первичного ключа. (REQ-398) [tool-verified: `provisa/api/rest/cypher_router.py:689`]

**Ответ:** `application/json` с `node_labels` (каждая несёт `pk`/`pk_columns`) и `relationship_types`.

Каждый тип связи также несёт `junction_table_name` и `properties` (REQ-1586). Для рёбер на основе связующей таблицы (junction) первое поле называет ассоциативную таблицу, через которую проходит обход, а второе перечисляет столбцы этой таблицы, доступные для чтения как `r.attr` и для фильтрации в `WHERE`; для рёбер на основе внешнего ключа имя равно `null`, а список свойств пуст — так клиент отличает одно от другого. Сама связующая таблица никогда не является меткой узла — она представляет ребро, поэтому у неё нет «пилюли» в графовом клиенте и строки в `node_labels`. [tool-verified: `provisa/api/rest/cypher_router.py:797-805`, `provisa/cypher/label_map.py:378-397`]

---

### `GET /data/domains`

Возвращает id доменов, доступных запрашивающей роли. [tool-verified: `provisa/api/data/sdl.py:116`]

**Заголовки:** `X-Role: <role_id>` (обязателен)

**Ответ:** `["sales", "support", ...]`

---

### `GET /data/schema-version`

Возвращает строку текущей версии схемы. Объединяет одноразовый идентификатор запуска (nonce) со счётчиком пересборок. Клиенты используют это, чтобы инвалидировать кеши схемы после перезапуска сервера. (REQ-537) [tool-verified: `provisa/api/data/sdl.py:102`]

**Ответ:** `{"version": "<boot-id>-<counter>"}`

---

### `GET /data/proto/{role_id}`

Возвращает автоматически сгенерированный файл `.proto` для роли. [tool-verified: `provisa/api/data/endpoint_dev.py:49`]

**Ответ:** схема protobuf в формате `text/plain`.

Каждая зарегистрированная таблица порождает `message` proto. Связи порождают вложенные поля сообщений. Сопоставление типов: `integer → int32`, `bigint → int64`, `varchar → string`, `decimal → double`, `boolean → bool`, `timestamp → google.protobuf.Timestamp`. (REQ-538)

---

### `GET /data/subscribe/{table}`

Поток Server-Sent Events для уведомлений об изменениях в реальном времени из таблицы. (REQ-219, REQ-258) [tool-verified: `provisa/api/data/subscribe.py:239`]

Доставка уведомлений использует подключаемого (pluggable) провайдера, выбираемого по типу источника: источники PostgreSQL используют `LISTEN/NOTIFY` (через asyncpg), источники MongoDB используют Change Streams (`collection.watch()`), а источники Kafka используют группы потребителей. Каждый провайдер реализует общий асинхронный интерфейс наблюдения (watch). Фильтрация RLS и валидация схемы применяются независимо от провайдера. (REQ-258) Источники WebSocket и RSS также поддерживаются. (REQ-338, REQ-342)

**Заголовок — `X-Provisa-Sink`:** установите на цель Kafka (например, `kafka://broker:9092/topic`), чтобы перенаправить события изменений в приёмник (sink) Kafka вместо ответа SSE. Сервер запускает потребителя приёмника и возвращает `202 Accepted` вместо открытого потока. (REQ-812) [tool-verified: `provisa/api/data/subscription_sse.py:137`]

---

## Административные REST-эндпоинты

### Конфигурация

#### `GET /admin/config`

Скачать текущий `provisa.yaml` как `application/x-yaml` с заголовком `Content-Disposition: attachment`. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:19`]

#### `PUT /admin/config`

Загрузить пересмотренный YAML конфигурации. Сервер создаёт резервную копию `.bak`, сохраняет новый файл и перезагружает все схемы, источники и материализованные представления. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:32`]

**Тело запроса:** необработанное содержимое YAML.

**Ответ:**

```json
{"success": true, "message": "Config uploaded and reloaded"}
```

При сбое перезагрузки: `{"success": false, "message": "<error>"}`.

#### `GET /admin/config/live`

Скачать **текущую действующую конфигурацию** — конфигурацию, которую Provisa записала бы сегодня, отражающую каждую созданную через admin таблицу, связь, домен, роль и правило RLS, накопленные с момента запуска. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:67`]

Файл на диске может отставать от действующего состояния, если изменения были внесены через admin API без последующей загрузки. Этот эндпоинт закрывает этот разрыв: его вывод — это то, что должен получить `PUT /admin/config`, чтобы файл на диске совпал с действующим состоянием.

Возвращает `application/x-yaml` с `Content-Disposition: attachment; filename=provisa.live.yaml`.

#### `GET /admin/config/diff`

Возвращает обе стороны разницы конфигурации — `original` (базовая версия при запуске) и `current` (действующее состояние) — нормализованные одинаково, так что сравнение показывает только реальные изменения, а не переупорядочивание или дрейф комментариев. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:82`]

**Ответ:**

```json
{"original": "<yaml>", "current": "<yaml>"}
```

#### `POST /admin/config/patch`

Сгенерировать унифицированный diff-патч от базовой версии к отправленной конфигурации. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:93`]

Отправьте пересмотренный YAML как тело запроса. Ответ — это файл `text/x-patch` (`provisa.config.patch`), который `git apply` или `patch` могут применить напрямую — полезно для фиксации изменений конфигурации, сделанных через UI, через CI/CD-конвейер.

---

### Настройки

#### `GET /admin/settings`

Возвращает текущие настройки платформы в формате JSON. (REQ-165) [tool-verified: `provisa/api/admin/settings_router.py:50`]

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

Обновляемые поля по разделам:

- `redirect`: `enabled`, `threshold`, `default_format`, `ttl`
- `sampling`: `default_sample_size`
- `cache`: `default_ttl`
- `naming`: `domain_prefix`, `convention` — записывает в файл конфигурации и вызывает согласование схемы (REQ-253)
- `relationships`: `auto_track_fk` — управляет только отслеживанием внешних ключей. Связь на основе связующей таблицы (junction) объявляется при регистрации таблицы и никогда не выводится автоматически, поэтому эта настройка на неё не влияет. (REQ-1586)
- `otel`: `endpoint`, `service_name`, `sample_rate`, `support_endpoint`, `support_redact_sql_literals`, `support_redact_attributes`

**Ответ:**

```json
{"success": true, "updated": ["otel.support_endpoint", "cache.default_ttl"]}
```

---

### Модели ИИ

#### `GET /admin/ai-models`

Возвращает назначения ИИ-моделей действующей организации, реестр векторных моделей и ограничение частоты запросов NL. (REQ-464, REQ-1349) [tool-verified: `provisa/api/admin/ai_models_router.py:58`]

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

Ключи API никогда не возвращаются обратно — `api_keys_set` сообщает только, настроен ли ключ для каждого вендора. Изменения вступают в силу со следующего запроса; перезапуск не требуется. (REQ-1349)

#### `PUT /admin/ai-models`

Обновить назначения ИИ-моделей организации, реестр векторных моделей или ограничение частоты запросов NL. Вступает в силу со следующего запроса. [tool-verified: `provisa/api/admin/ai_models_router.py:148`]

#### `GET /admin/ai-models/vendors/{vendor}/models`

Возвращает имена моделей, которые вендор в данный момент предоставляет, для средства выбора модели. (REQ-1395, REQ-1398, REQ-1409) [tool-verified: `provisa/api/admin/ai_models_router.py:89`]

Список читается напрямую из собственного API list-models вендора с использованием ключа организации — или учётных данных развёртывания, если ключ организации не задан. Модель, выпущенная после сборки этой версии, становится доступной для выбора в тот же день, когда её начинает предоставлять вендор.

Возвращает `400`, если вендор не публикует API list-models (в этом случае введите имя модели напрямую) или если нет доступного ключа. [tool-verified: `provisa/api/admin/ai_models_router.py:109-128`]

---

### Движок федерации

#### `GET /admin/federation-engine`

Возвращает текущий выбор движка федерации, его конфигурацию подключения и полный реестр доступных для выбора движков. (REQ-916) [tool-verified: `provisa/api/admin/settings_router.py:730`]

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

Ключ `current` — это движок, работающий прямо сейчас; `persisted` — то, что записано в файл конфигурации и будет загружено при следующем перезапуске. Они расходятся, если конфигурация была изменена, а сервис ещё не перезапущен.

#### `PUT /admin/federation-engine`

Сохранить выбор движка федерации. (REQ-916) [tool-verified: `provisa/api/admin/settings_router.py:774`]

**Тело запроса:**

```json
{"engine": "trino", "federation_engine_url": "http://trino-coordinator:8080"}
```

Выбор записывается в конфигурацию платформы. Вступает в силу после следующего перезапуска сервиса — движок выбирается один раз при загрузке.

---

### Политика домена

#### `POST /admin/domain-policy`

Изменить политику доменов действующей организации (`use_domains` / `default_domain`). (REQ-165, REQ-1266, REQ-1349) [tool-verified: `provisa/api/admin/settings_router.py:632`]

Это деструктивная операция, ограниченная действующей организацией. Каждый зарегистрированный источник, таблица, домен и связь очищаются и пересобираются под новую политику. Используйте это при переключении организации с доменного пространства имён на плоское (или наоборот).

**Тело запроса:**

```json
{
  "use_domains": true,
  "default_domain": "default"
}
```

`use_domains: null` очищает переопределение организации и возвращается к настройке уровня развёртывания. `use_domains: false` требует `default_domain` (единственное имя домена, в который попадают все таблицы). Пересборка каталога синхронна; ответ возвращается, когда схемы готовы.

---

### Наблюдаемость

#### `GET /admin/traces/recent`

Возвращает до N последних завершённых спанов из буфера спанов в памяти. (REQ-302) [tool-verified: `provisa/api/admin/settings_router.py:317`]

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

### Обнаружение

#### `POST /admin/discover/relationships`

Инициировать обнаружение связей. Всегда выполняет интроспекцию внешних ключей из движка федерации. (REQ-018) Выполняет вывод LLM, если задан `ANTHROPIC_API_KEY`. (REQ-167) [tool-verified: `provisa/api/admin/discovery.py:55`]

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

Список ожидающих кандидатов связей. [tool-verified: `provisa/api/admin/discovery.py:96`]

#### `POST /admin/discover/candidates/{candidate_id}/accept`

Принять кандидата и зарегистрировать его как связь. [tool-verified: `provisa/api/admin/discovery.py:103`]

**Тело запроса (опционально):** `{"name": "custom-relationship-name"}`

#### `POST /admin/discover/candidates/{candidate_id}/reject`

Отклонить кандидата. [tool-verified: `provisa/api/admin/discovery.py:110`]

**Тело запроса:** `{"reason": "Not a real join"}`

#### `GET /admin/discover/candidates/rejected/count`

Возвращает количество отклонённых кандидатов. [tool-verified: `provisa/api/admin/discovery.py:118`]

#### `DELETE /admin/discover/candidates/rejected`

Удалить всех отклонённых кандидатов. [tool-verified: `provisa/api/admin/discovery.py:128`]

---

### Обход источника (Crawl)

#### `POST /admin/sources/crawl`

Обойти источник данных, чтобы выполнить интроспекцию его схемы и зарегистрировать таблицы. (REQ-012) [tool-verified: `provisa/api/admin/crawl_router.py:36`]

---

### Поиск таблиц источника

#### `GET /admin/sources/{source_id}/tables/search`

Найти доступные (ещё не зарегистрированные) таблицы в источнике по имени. [tool-verified: `provisa/api/admin/table_search_router.py:103`]

---

### Профилирование таблиц

#### `POST /admin/tables/{table_id}/profile`

Выполнить профилирование столбцов зарегистрированной таблицы — кардинальность, мин/макс, доля null-значений. [tool-verified: `provisa/api/admin/table_profile_router.py:28`]

---

### Описания источников

#### `POST /admin/source-meta/db-description`

Сгенерировать описания таблиц и столбцов источника с помощью LLM. [tool-verified: `provisa/api/admin/source_meta_router.py:48`]

---

### Объектное хранилище (REQ-1046, REQ-1048, REQ-1049)

#### `GET /admin/org-storage`

Сообщает объём хранилища действующей организации относительно её платформенной квоты и указывает, зарегистрировала ли организация собственное хранилище. [tool-verified: `provisa/api/admin/org_storage_router.py:69`]

Когда у организации зарегистрирован собственный DSN, её материализации попадают туда и больше не учитываются в квоте. Сам DSN никогда не возвращается.

#### `PUT /admin/org-storage`

Зарегистрировать (или очистить) собственное хранилище материализаций организации. [tool-verified: `provisa/api/admin/org_storage_router.py:81`]

**Тело запроса:**

```json
{"storage_url": "s3://my-bucket/provisa?region=us-east-1&access_key=..."}
```

DSN проверяется движком федерации перед принятием — непригодный DSN отклоняется при регистрации, а не спустя часы при обновлении. Значение шифруется в состоянии покоя и никогда не возвращается через GET.

Отправьте `storage_url: null`, чтобы очистить собственное хранилище организации и вернуть её материализации в платформенное хранилище (и квоту). Среда выполнения организации пересобирается в том же вызове, так что новое хранилище действует немедленно. [tool-verified: `provisa/api/admin/org_storage_router.py:123-138`]

---

### Шифрование организации (REQ-1574)

#### `GET /admin/org-encryption`

Возвращает текущий статус ключа организации: отпечаток, id и происхождение. Никогда не возвращает материал ключа. [tool-verified: `provisa/api/admin/org_encryption_router.py:53`]

Когда организация не задала ключ, возвращает `{"configured": false}`. Каждая организация начинает в этом состоянии и наследует ключ развёртывания.

#### `PUT /admin/org-encryption`

Задать или сменить ключ шифрования организации в состоянии покоя. [tool-verified: `provisa/api/admin/org_encryption_router.py:68`]

**Тело запроса:**

```json
{"key_b64": "<32 raw bytes, base64-encoded>"}
```

Опустите `key_b64`, чтобы Provisa сгенерировала ключ — самый безопасный путь, так как ключ никогда не появляется в буфере обмена или журнале запросов. Указание `key_b64` означает использование собственного ключа.

Смена ключа добавляет новую активную запись в кольцо ключей и сохраняет старую, так что данные, записанные под предыдущим ключом, остаются читаемыми. Смена ключа — это не перешифрование. Эндпоинта удаления нет: удаление последнего ключа сделало бы каждую обёрнутую полезную нагрузку нечитаемой. [tool-verified: `provisa/api/admin/org_encryption_router.py:75`]

Действующее кольцо ключей переподключается в том же вызове, так что следующая зашифрованная запись немедленно использует новый ключ.

---

### Импорт Hasura / DDN (REQ-1483)

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

`flavor` — это `"auto"` (определяется по структуре архива), `"hasura_v2"` или `"ddn"`.

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

Ничего не сохраняется. Предпросмотр не кешируется на стороне сервера; `apply` принимает предоставленный вами YAML, так что применяется именно то, что было проверено (и, возможно, отредактировано).

#### `POST /admin/import/hasura/apply`

Загрузить ранее предпросмотренную конфигурацию в действующую организацию. [tool-verified: `provisa/api/admin/import_router.py`]

**Тело запроса:**

```json
{"config_yaml": "<yaml string>"}
```

Использует тот же путь горячей перезагрузки, что и `PUT /admin/config`. Каталог, схемы и пулы организации пересобираются до возврата ответа.

---

### Обмен Apache Ossie (REQ-1316, REQ-1321)

#### `GET /admin/ossie`

Экспортировать управляемую модель организации в виде YAML-документа Apache Ossie (incubating). (REQ-1321) [tool-verified: `provisa/api/admin/ossie_router.py`]

Документ формируется из действующего состояния при каждом запросе — никогда не кешируется, — поэтому не может устареть. Таблицы становятся объектами `dataset`, столбцы становятся объектами `field`, а связи сопоставляются с объектами `relationship` Ossie.

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

Ничего не регистрируется. Используйте экран проверки в admin UI, чтобы принять или урезать предложения до срабатывания любой мутации.

---

### Действия (Функции и вебхуки)

Все эндпоинты находятся под префиксом `/admin/actions`. (REQ-205) [tool-verified: `provisa/api/admin/actions_router.py:24`]

Каждый вызов — из GraphQL, SQL, Cypher, Bolt, Arrow Flight, MCP `run_sql` и Provisa gRPC — проходит через единый управляемый исполнитель, который единообразно применяет `writable_by` и управление. (REQ-1156) [tool-verified: `provisa/api/data/action_exec.py`] См. [docs/integrations.md](integrations.md#invoking-commands-across-protocols) для синтаксиса вызова по каждому протоколу.

#### `GET /admin/actions`

Возвращает все отслеживаемые функции БД и вебхуки. (REQ-242) [tool-verified: `provisa/api/admin/actions_router.py:104`]

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

Каждый объект вебхука несёт булево поле `approved`. Вебхук становится одобренным, когда стюард выполняет его запрос на создание (REQ-209); объявленные в конфигурации вебхуки одобряются автоматически. Неодобренный вебхук зарегистрирован, но не предоставляется ни на одной поверхности. [tool-verified: `provisa/api/admin/actions_router.py:124-131`]

#### `POST /admin/actions/functions`

Зарегистрировать отслеживаемую функцию (команду). (REQ-205) [tool-verified: `provisa/api/admin/actions_router.py:117`]

**Ключевые поля:**

| Поле | Обязательно | Описание |
| --- | --- | --- |
| `name` | Да | Уникальное имя команды |
| `kind` | Да | `"query"` → поле GraphQL Query; `"mutation"` → поле Mutation |
| `implKind` | Нет | Способ выполнения команды — см. таблицу ниже (по умолчанию `source_procedure`) |
| `binding` | Нет | Детали подключения, специфичные для `implKind` (объект JSON) |
| `returnSchema` | Нет | JSON Schema `{type:"array", items:{type:"object", properties:{...}}}` — делает команду возвращающей набор строк на каждой поверхности |
| `arguments` | Нет | Определения аргументов `[{name, type}]`; порядок позиций важен для вызывающих через SQL и Bolt |
| `visibleTo` | Нет | Id ролей, которые могут вызывать команду |
| `writableBy` | Нет | Id ролей, которым разрешено вызывать её как мутацию |
| `domainId` | Нет | Домен для размещения в GraphQL и контроля доступа |

**Значения `implKind`:**

| `implKind` | Что выполняется | Поля `binding` |
| --- | --- | --- |
| `source_procedure` | Хранимая процедура в зарегистрированном источнике (по умолчанию) | `sourceId`, `schemaName`, `functionName` |
| `script` | Скрипт на стороне сервера | `script` |
| `http` | Исходящий HTTP-вызов | `url`, `method` |
| `grpc` | Исходящий gRPC-вызов к внешнему серверу | `target`, `method` |
| `python` | Python-функция, размещённая в Provisa (REQ-885) | `callable` (например, `"demo.py_functions:random_dataset"`) |

Демонстрационные команды `random_python_set` (`implKind: python`) и `random_grpc_set` (`implKind: grpc`) показывают на практике команды, возвращающие набор строк, с `returnSchema`; обе находятся в `config/provisa-install.yaml`. [tool-verified: `config/provisa-install.yaml:809-856`]

#### `PUT /admin/actions/functions/{name}`

Обновить отслеживаемую функцию по имени. [tool-verified: `provisa/api/admin/actions_router.py:182`]

#### `DELETE /admin/actions/functions/{name}`

Удалить отслеживаемую функцию по имени. [tool-verified: `provisa/api/admin/actions_router.py:233`]

#### `POST /admin/actions/webhooks`

Зарегистрировать отслеживаемый вебхук. (REQ-209) Регистрация или обновление вебхука ставит в очередь запрос на одобрение стюардом — вебхук становится активным на всех поверхностях только после одобрения стюардом. Объявленные в конфигурации вебхуки одобряются автоматически. **Поля тела запроса:** `name`, `url`, `method`, `timeoutMs`, `returns`, `inlineReturnType`, `arguments`, `visibleTo`, `domainId`, `description`, `kind`. [tool-verified: `provisa/api/admin/actions_router.py:132`, `provisa/api/admin/actions_router.py:325-331`]

#### `PUT /admin/actions/webhooks/{name}`

Обновить отслеживаемый вебхук по имени. Любое редактирование сбрасывает одобрение в статус ожидания до повторного одобрения. [tool-verified: `provisa/api/admin/actions_router.py:306`]

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
| `PATCH` | `/admin/users/{user_id}/password` | Сменить пароль |
| `DELETE` | `/admin/users/{user_id}` | Удалить пользователя |
| `GET` | `/admin/users/{user_id}/assignments` | Список назначенных ролей |
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

Эндпоинт Strawberry GraphQL для всех операций admin: CRUD источников и таблиц, управление связями, конфигурация доменов, правила RLS, контроль кеша, соглашения об именовании, управление запланированными задачами и компиляция запросов. (REQ-164) [tool-verified: `provisa/api/app.py:2171`]

Полный справочник схемы — каждое поле Query, поле Mutation и тип ввода/вывода — см. в разделе [Справочник Admin GraphQL API](admin-graphql.md).

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

Возвращает статус первоначальной настройки. Всегда без аутентификации. (REQ-539) [tool-verified: `provisa/api/setup_router.py:100`]

#### `POST /setup/`

Завершить первоначальную настройку. [tool-verified: `provisa/api/setup_router.py:142`]

---

## Проверка работоспособности

#### `GET /health` или `HEAD /health`

Возвращает `{"status": "ok"}`. Всегда без аутентификации. (REQ-539) [tool-verified: `provisa/api/app.py:2258`]

---

## Ответы об ошибках

| Статус | Значение |
| --- | --- |
| 400 | Некорректный запрос, ошибка валидации или ошибка разбора SQL |
| 401 | Отсутствующий или недействительный токен аутентификации |
| 403 | Недостаточно возможностей; нарушение управления |
| 404 | Роль, ресурс или файл конфигурации не найдены |
| 422 | Отсутствует обязательный заголовок (например, `X-Role`) |
| 503 | База данных или источник не подключены; зависимость недоступна |
| 504 | Истекло время ожидания запроса |

Нарушения управления на `POST /data/sql` возвращают HTTP 403 со структурированным телом: (REQ-002) [tool-verified: `provisa/api/data/endpoint_dev.py:184-190`]

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

Порт `8815`. Нативный колоночный транспорт Arrow поверх gRPC. (REQ-143, REQ-045) [tool-verified: `provisa/api/flight/server.py`]

Запросы и обнаружение каталога доступны на одном и том же соединении. Полный конвейер управления (RLS, маскирование, сэмплирование) применяется к каждому запросу. (REQ-130, REQ-143)

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

## Эндпоинт Protobuf gRPC

Порт `50051` (переопределяется переменной окружения `GRPC_PORT` или конфигурацией `server.grpc_port`). (REQ-529) [tool-verified: `provisa/grpc/server.py`, `provisa/api/app.py`]

Передайте роль в ключе метаданных gRPC `x-provisa-role`. Если он отсутствует, сервер прерывает выполнение с `UNAUTHENTICATED`. [tool-verified: `provisa/grpc/server.py`]

Скачайте специфичный для роли proto по адресу `GET /data/proto/{role_id}`. Отображаются только таблицы и столбцы, видимые этой роли. (REQ-039)

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

gRPC-сервер запускается только тогда, когда действительный proto может быть скомпилирован при старте. Если сборка схемы завершается неудачей, сервер gRPC не запускается. (REQ-529)

#### RPC агрегации и группировки (REQ-1359, REQ-1361, REQ-1405)

Когда у таблицы установлен `enable_aggregates`, сгенерированный proto включает два дополнительных RPC наряду с `Query{TypeName}`:

- **`Query{TypeName}Aggregate`** — возвращает агрегатные скаляры для таблицы (`count`; `sum`, `avg`, `stddev`, `variance` для каждого числового столбца; `min`, `max` для каждого сравнимого столбца)
- **`Query{TypeName}GroupBy`** — возвращает одну строку на ключ группы с агрегатными подполями и, опционально, скалярами базовой таблицы и строками присоединённого измерения в поле `nodes`

Оба используют тот же конвейер компилятора для агрегации, что и корневые поля GraphQL `{field}_aggregate` и `{field}_group_by` — отдельной реализации агрегации нет. (REQ-1359) [tool-verified: `provisa/grpc/query_ir.py:133-196`]

**Поле `funcs` (REQ-1361).** Сообщение запроса принимает повторяющееся строковое поле `funcs`. Допустимые значения: `count`, `sum`, `avg`, `stddev`, `variance`, `min` и `max`. Когда `funcs` опущено, запрашивается каждая функция, которую схема предоставляет для этой таблицы. Когда оно задано, отображаются только названные функции. Если ни одна из названных функций не применима к типам столбцов таблицы, запрос откатывается к `count`. [tool-verified: `provisa/grpc/query_ir.py:66`, `provisa/grpc/query_ir.py:75-97`]

**Поля `include_nodes` и `include` (REQ-1405).** Запросы `Query{TypeName}GroupBy` могут установить `include_nodes: true`, чтобы включить скалярные столбцы базовой таблицы в поле `nodes` каждой строки. Повторяющееся строковое поле `include` называет поля связей «многие к одному», чьи скалярные столбцы также вкладываются внутрь `nodes`. Это соответствует поведению `?includeNodes=` / `?include=` в JSON:API. [tool-verified: `provisa/grpc/query_ir.py:168-195`]

---

## Драйвер JDBC

Драйвер JDBC Provisa (`provisa-jdbc-0.1.0.jar`) предоставляет семантический каталог инструментам BI (Tableau, PowerBI, DBeaver). (REQ-126)

**URL подключения:** `jdbc:provisa://host:port` (REQ-131)

Домены сопоставляются со схемами JDBC. (REQ-127) Таблицы используют свои зарегистрированные псевдонимы. Столбцы используют псевдонимы и отображают описания как `REMARKS`. (REQ-128) Стандартные методы метаданных (`getPrimaryKeys`, `getImportedKeys`, `getExportedKeys`) предоставляют семантические связи как метаданные PK/FK.

**Поддержка SQL:** `SELECT * FROM <alias> [WHERE col = 'value']`. (REQ-129)

По умолчанию драйвер запрашивает перенаправление Arrow IPC. Результаты передаются потоком пакет за пакетом через `ArrowStreamReader`, ограничены одним пакетом записей в памяти. (REQ-293)

---

## Формат аргумента `orderBy`

Аргумент `order_by` использует объекты `{column: direction}` с перечислением направления из 6 значений: (REQ-200)

```json
{
  "query": "{ orders(order_by: [{created_at: desc_nulls_last}]) { id created_at } }",
  "role": "admin"
}
```

Поддерживаемые направления: `asc`, `desc`, `asc_nulls_first`, `asc_nulls_last`, `desc_nulls_first`, `desc_nulls_last`. (REQ-201)

---

## Подписки

Подписки SSE доступны по адресу `GET /data/subscribe/{table}`. (REQ-219, REQ-258) Доставка уведомлений использует подключаемого провайдера, выбираемого по типу источника: источники PostgreSQL используют `LISTEN/NOTIFY`, источники MongoDB используют Change Streams, а источники Kafka используют группы потребителей. Фильтрация RLS и валидация схемы применяются независимо от провайдера. Источники WebSocket и RSS также поддерживаются через тот же эндпоинт. (REQ-338, REQ-342) [tool-verified: `provisa/api/data/subscribe.py:239`, `provisa/subscriptions/registry.py`, `provisa/api/app.py` `_rebuild_schemas`]

---

## Бизнес-глоссарий (REQ-1387)

Бизнес-глоссарий сопоставляет физические имена полей — такими, какие они есть в исходных базах данных, — с общим человеко-понятным словарём. Каждый столбец, зарегистрированный в семантическом слое, автоматически получает термин. Ручной ввод для заполнения глоссария не требуется; кураторы добавляют определения, связи и экспертов поверх того, что выводит система.

### Как выводятся термины

Когда Provisa регистрирует или обновляет столбцы таблицы, `normalize_term` (`provisa/core/glossary.py`) выполняется для каждого имени столбца и формирует каноническую фразу. [tool-verified: `provisa/core/repositories/glossary.py:sync_table_refs`]

Нормализация применяет пять правил последовательно:

1. Разбиение по границам camelCase и символам-разделителям (`_`, `-`, `.`, `/`, пробел).
2. Приведение результата к нижнему регистру.
3. Расширение фиксированной таблицы сокращений (например, `cust` → `customer`, `amt` → `amount`, `dt` → `date`, `id` → `identifier`, `key` → `identifier`, `guid` → `identifier`).
4. Удаление конечного **токена-заместителя (proxy token)** (`identifier`, `code`, `index` или `reference`) — столбец, названный по своему ключу или коду, указывает на лежащее в основе понятие через значение-заместитель, поэтому термином должно быть само понятие. Последний оставшийся токен никогда не удаляется.
5. Уточнение **слишком общей фразы** концептом таблицы. Когда полная нормализованная фраза представляет собой голое слово-атрибут (`name`, `identifier`, `date`, `location`, `message`, `first name`, `last name` и подобные), термин становится `<концепт таблицы> <фраза>` — `employees.first_name` → `employee first name`, `orders.id` → `order identifier`. Один общий термин `name` для не связанных между собой таблиц объединил бы разные значения; уточнение вместо этого связывает каждый столбец с его охватывающим концептом. Концепт таблицы — это бизнес-имя таблицы, нормализованное с единственным числом главного существительного (`order_lines` → `order line`).

Псевдостолбцы нативных фильтров (с префиксом `_nf_`, или любой столбец, несущий `native_filter_type`) являются механизмом параметров запроса, а не бизнес-полями, и не порождают термины.

Поскольку `id`, `key`, `pk` и `sk` — все раскрываются в `identifier` до проверки на заместитель, три физически разных имени столбца попадают точно на один и тот же термин:

| Физическое имя | После нормализации |
| --- | --- |
| `cust_id` | `customer` |
| `customerId` | `customer` |
| `CUSTOMER_KEY` | `customer` |
| `txn_amt` | `transaction amount` |

Первые три схлопываются в один термин. `transaction amount` сохраняет оба токена, потому что `amount` не является заместителем. Голый столбец `id` — без предшествующих токенов — не может быть удалён; он нормализуется в `identifier`, так что термин остаётся непустым. [tool-verified: `provisa/core/glossary.py:normalize_term`]

### Жизненный цикл

Термины **выводятся из членства в семантическом слое**, а не создаются пользователями по запросу. Репозиторий таблиц — единственный путь записи: `sync_table_refs` выполняется внутри каждого upsert набора столбцов, а `sweep_refless_terms` выполняется после любого пути удаления. [tool-verified: `provisa/core/repositories/glossary.py`]

**Когда добавляется столбец:** Provisa ищет нормализованный термин по имени. Если он уже существует, столбец получает на него ссылку (ref) (и если термин был устаревшим (deprecated), он восстанавливается — `deprecated` сбрасывается обратно в `False`). Если термина ещё не существует, он создаётся.

**Когда столбец уходит** (изменение схемы или удаление таблицы): его ссылка удаляется, а термин **улаживается (settled)** по правилу «удалить или пометить устаревшим». Корневой (rooted) термин без оставшихся ссылок удаляется полностью — вместе со своими рёбрами и назначениями экспертов — если только удаление не оставит абстрактный термин отсоединённым от всех корневых терминов (нет пути через граф терминов). В этом случае термин **помечается устаревшим** (`deprecated=True`), а не удаляется, чтобы якорь абстрактного термина в графе сохранился.

Абстрактные термины никогда не удаляются автоматически; они существуют вне физического жизненного цикла и удаляются только явно через admin API.

**Восстановление:** если нормализованное имя устаревшего термина появляется снова (столбец перерегистрируется), пометка с термина снимается, и его ссылки снова начинают накапливаться.

### Эндпоинты курирования

Все эндпоинты находятся под `/admin/glossary`. Они требуют доступа `org_admin` и настроенной организации. Каждая мутация вызывает публикацию метаданных. [tool-verified: `provisa/api/admin/glossary_router.py`]

| Метод | Путь | Описание |
| --- | --- | --- |
| `GET` | `/admin/glossary/terms` | Список терминов. Параметры запроса: `q` (поиск по имени/определению), `include_deprecated` (по умолчанию `true`) |
| `GET` | `/admin/glossary/terms/{term_id}` | Получить детали термина: определение, физические ссылки, типизированные рёбра, эксперты |
| `POST` | `/admin/glossary/terms` | Создать абстрактный термин — пользовательский словарь без физических ссылок |
| `PATCH` | `/admin/glossary/terms/{term_id}` | Переименовать, задать определение или переключить исключение из экспорта |
| `DELETE` | `/admin/glossary/terms/{term_id}` | Удалить термин без физических ссылок |
| `POST` | `/admin/glossary/refs/move` | Переместить одну физическую ссылку на другой термин (консолидация) |
| `POST` | `/admin/glossary/terms/{term_id}/edges` | Добавить типизированное ребро связи между двумя терминами |
| `DELETE` | `/admin/glossary/terms/{term_id}/edges` | Удалить ребро (параметры запроса: `to_term_id`, `rel_type`) |
| `POST` | `/admin/glossary/terms/{term_id}/experts` | Отметить пользователя как эксперта или автора для термина |
| `DELETE` | `/admin/glossary/terms/{term_id}/experts/{user_id}` | Удалить назначение эксперта/автора пользователя |
| `POST` | `/admin/glossary/terms/{term_id}/definition/generate` | Составить черновик определения для одного термина с использованием ИИ-модели организации — возвращает только текст, ничего не сохраняется до сохранения |
| `POST` | `/admin/glossary/definitions/generate` | Сгенерировать и сохранить определения для каждого термина, у которого их нет — никогда не перезаписывает текст, написанный человеком |
| `POST` | `/admin/glossary/relationships/generate` | Предложить и сохранить типизированные рёбра по всему глоссарию с использованием ИИ-модели организации |

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

Перемещение ссылки улаживает теряющий термин по правилу «удалить или пометить устаревшим». Используйте это для консолидации двух терминов, которые нормализация оставила раздельными — например, после того как источник использует нестандартное сокращение, выпадающее из таблицы расширений.

Удаление корневого термина (с физическими ссылками) возвращает `400 glossary.invalid`. Сначала удалите или переместите все ссылки.

**Поле `export_excluded` в `PATCH /admin/glossary/terms/{term_id}`:**

```json
{"export_excluded": true}
```

Установка `export_excluded` в `true` исключает термин из всех снимков экспорта метаданных, независимо от его физических ссылок или абстрактного статуса. Возврат в `false` восстанавливает термин в снимке при следующей публикации. Данные курирования (определение, рёбра, эксперты) не затрагиваются. [tool-verified: `provisa/core/repositories/glossary.py:set_export_excluded`, `provisa/api/admin/glossary_router.py:update_term`]

### Курирование с помощью ИИ

Настроенная ИИ-модель организации может составлять черновики определений и предлагать рёбра связей по всему глоссарию за одну операцию. Оба массовых действия требуют доступа `org_admin` и настроенной организации.

**`POST /admin/glossary/definitions/generate`**

Перебирает каждый термин в глоссарии, пропускает те, у которых уже есть определение, и вызывает ИИ-модель организации, чтобы составить черновик для каждого оставшегося термина. Черновик сохраняется немедленно — в отличие от эндпоинта черновика для одного термина (`POST /admin/glossary/terms/{term_id}/definition/generate`), здесь нет шага редактирования. Определения, написанные человеком, никогда не перезаписываются: защита — это `if summary["definition"]: continue` перед любым вызовом модели. Одно уведомление о публикации покрывает весь пакет. [tool-verified: `provisa/api/admin/glossary_router.py:generate_all_definitions`]

Ответ:

```json
{"generated": 12}
```

`generated` — это количество терминов, получивших новое определение. Равно нулю, когда у каждого термина уже есть определение.

**`POST /admin/glossary/relationships/generate`**

Отправляет полный список терминов ИИ-модели организации с подсказкой, которая указывает десять допустимых типов рёбер (`KIND_OF`, `PART_OF`, `SYNONYM_OF`, `RELATED_TO`, `VALID_VALUE_OF`, `DERIVED_FROM`, `REPLACES`, `PREFERRED_TERM_FOR`, `TRANSLATION_OF`, `ANTONYM_OF`) и просит только уверенные предложения. Модель отвечает JSON-массивом; каждая запись проверяется перед любой записью: неизвестные имена терминов, само-рёбра и типы рёбер вне закрытого перечисления молча отбрасываются. Валидные предложения записываются идемпотентно через upsert — повторный запуск действия не дублирует рёбра. Одно уведомление о публикации покрывает пакет. Эндпоинт немедленно возвращает `{"added": 0}`, когда глоссарий содержит менее двух неустаревших терминов. [tool-verified: `provisa/api/admin/glossary_router.py:generate_relationships`]

Ответ:

```json
{"added": 5}
```

`added` — это количество записанных рёбер. Уже существовавшее ребро всё равно засчитывается — upsert проходит успешно, но данные ребра не меняются.

### Инструмент MCP `search_terms`

```
search_terms(query, role=None, limit=25)
```

Ищет по именам и определениям терминов с сопоставлением подстроки без учёта регистра, до `limit` результатов. Каждый результат — полные детали термина: `name`, `definition`, `is_abstract`, `deprecated`, физические ссылки (с `source_id`, `schema_name`, `table_name`, `column_name`), типизированные рёбра и назначения экспертов. [tool-verified: `provisa/api/mcp/server.py:236-244`, `provisa/core/repositories/glossary.py:search_terms`]

Используйте `search_terms` перед написанием SQL, чтобы найти каждое физическое поле, представляющее понятие по имени. Например, поиск `"order date"` возвращает термин и все столбцы `order_dt`, `orderDate`, `ORDER_DATE` во всех зарегистрированных таблицах.

### Экспорт метаданных

Граф терминов глоссария включён в каждый `MetadataSnapshot`, построенный `build_snapshot`. [tool-verified: `provisa/api/metadata_export/builder.py:_glossary_assets`]

Экспорт применяет те же фильтры, что и остальная часть снимка:

- Термин, помеченный `export_excluded`, исключается полностью — независимо от его физических ссылок, абстрактного статуса или того, настроен ли каталог организации. [tool-verified: `provisa/api/metadata_export/builder.py:_glossary_assets`]
- Корневой термин публикуется только тогда, когда хотя бы одна из его физических ссылок принадлежит столбцу, проходящему как фильтр **Data Product** (флаг `data_product` таблицы должен быть `true`), так и фильтр **технических** столбцов (столбцы с тегом `technical` исключаются).
- Корневой термин, чьи ссылки все исключены этими фильтрами, исключается вместе с ними.
- Абстрактные термины публикуются безусловно — это пользовательский словарь, не привязанный к физическим столбцам.
- Ребро между двумя терминами публикуется только тогда, когда публикуются оба конечных термина.

Каждый адаптер вендора публикует граф терминов нативно, в собственный контейнер глоссария Provisa, который он создаёт идемпотентно — никогда в существующий глоссарий каталога:

| Провайдер | Контейнер | Термины | Отношения | Устаревание |
| --- | --- | --- | --- | --- |
| Apache Atlas | «Provisa Glossary» (API глоссария) | термины глоссария, определение в `longDescription` | KIND_OF → `isA`, SYNONYM_OF → `synonyms`, RELATED_TO/PART_OF → `seeAlso` | маркер shortDescription `[DEPRECATED]` |
| Atlan | глоссарий Provisa по стабильному qualifiedName | `longDescription` (никогда не человеко-редактируемый `userDescription`) | то же сопоставление Atlas | `certificateStatus = DEPRECATED` |
| DataHub | `urn:li:glossaryNode:provisa.<org>` | аспект `glossaryTermInfo` на термин | KIND_OF → Inherits, PART_OF → Contains (инвертировано), RELATED_TO/SYNONYM_OF → related terms | аспект устаревания; переименования следуют преемственности URN |
| OpenMetadata | глоссарий Provisa через `/v1/glossaries` | PUT с ключом fqn, переименования — PATCH-перепривязка по хранимому UUID | KIND_OF → нативная родительская иерархия, SYNONYM_OF → `synonyms`, остальные → `relatedTerms` | `entityStatus` |
| Collibra | домен типа «Глоссарий» «Provisa Glossary» | активы Business Term через Import API | нативные типы отношений Business Term | статус актива |

Владение — это привязка, а не имя: id вендора каждого опубликованного термина фиксируется в `catalog_bindings` под URN термина (`provisa://<org>/terms/<name>`), и Provisa изменяет или удаляет элемент глоссария на стороне вендора только тогда, когда владеет этой привязкой (или элемент находится в собственном контейнере Provisa, который она создала). Элемент глоссария без привязки Provisa возник во внешней системе и никогда не затрагивается; обновления выполняют read-merge, так что поля, добавленные стюардом к собственным терминам Provisa, сохраняются; ничего не удаляется, когда термин покидает снимок. Назначения «термин-актив» стюарда остаются во внешнем владении — ни один адаптер не записывает назначения «термин-актив» (публикация назначений, авторства Provisa, — это явное последующее развитие). Конкретно в Collibra безопасность в рамках семантики REPLACE Import API опирается на изоляцию: полезная нагрузка упоминает только активы внутри домена глоссария Provisa и экземпляры отношений только между терминами Provisa, так что глоссарии стюардов и их отношения никогда не достижимы. [tool-verified: `provisa/api/metadata_export/atlan.py`, `provisa/api/metadata_export/datahub.py`, `provisa/api/metadata_export/atlas.py`, `provisa/api/metadata_export/openmetadata.py`]

---

## Продукты данных (REQ-1634)

Продукт данных группирует таблицы, публикуемые вместе для потребления, принадлежащие ровно одному домену. Поля следуют словарю ODPS (Open Data Product Standard) там, где у Provisa уже есть источник истины. Admin UI предоставляет продукты данных в разделе **Admin → Data Products**. [tool-verified: `provisa/core/models.py:318-342`, `provisa/api/admin/schema_mutation.py:949-1017`, `provisa/api/admin/schema_query.py:352-362`]

### Возможности

| Возможность | Предоставляет |
| --- | --- |
| `data_product_read` | Доступ на чтение к полю запроса `data_products` и странице admin «Data Products». По умолчанию предоставлена `org_admin`, `analyst`, `developer` и `modeler`. |
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

Удаление продукта очищает `product_id` у каждой входящей в него таблицы, снимая её членство. Требует `data_product_rw`. [tool-verified: `provisa/api/admin/schema_mutation.py:995-1017`]

### Схема полей

| Поле | Тип | Обязательно | Примечания |
| --- | --- | --- | --- |
| `id` | `String` | Да | Машиночитаемый стабильный идентификатор, например `customer_360` |
| `domain_id` | `String` | Да | Владеющий домен. Входящие таблицы должны иметь тот же `domain_id` — несовпадения отклоняются при сохранении |
| `name` | `String` | Да | Отображаемое имя |
| `owner_role` | `String` | Нет | Роль, ответственная за этот продукт; отличается от стюарда домена |
| `team_role` | `String` | Нет | Роль, чьи носители сопровождают этот продукт день за днём; разрешается до конкретных людей |
| `purpose` | `String` | Нет | Что публикует этот продукт и зачем |
| `limitations` | `String` | Нет | Известные ограничения, оговорки или исключения |
| `usage` | `String` | Нет | Как потреблять этот продукт |
| `version` | `String` | Нет | например, `1.2.0` |
| `status` | `String` | Нет | например, `proposed`, `active`, `deprecated`, `retired` |
| `sla` | `String` | Нет | Обязательства по уровню обслуживания; проза — продукт охватывает несколько таблиц, и структурированный SLA не может однозначно назвать, к какому именно участнику он относится |
| `support` | `String` | Нет | Произвольный текст с рекомендациями по поддержке |
| `custom_properties` | `JSON` | Нет | Произвольные метаданные ключ-значение, не покрытые стандартными полями |

На модели существуют ещё два поля, но они не предоставлены в Strawberry `DataProductType` / `DataProductInput` — они специфичны для Snowflake Horizon Catalog (REQ-1635):

| Поле | Примечания |
| --- | --- |
| `support_contact` | Email или URL; обязателен для манифестов листинга организации Horizon Catalog |
| `publish` | `true` для немедленной публикации листингов Horizon; новые листинги по умолчанию имеют статус DRAFT |

[tool-verified: `provisa/core/models.py:338-341`, `provisa/api/admin/types.py:104-118,538-551`]

### Членство таблиц

Таблица присоединяется к продукту данных путём установки поля `product_id` в форме редактирования таблицы. Средство выбора ограничено продуктами, чей `domain_id` совпадает с собственным доменом таблицы — таблице в домене `marketing` никогда не предлагается продукт в домене `sales`. [tool-verified: `provisa/api/admin/actions_router.py:244-260`, `docs/arch/requirements.yaml:54585-54586`]

Команды в том же домене также могут быть назначены участниками. [tool-verified: `provisa-ui/src/i18n/locales/en/dataProductsTab.json:commandsLabel`]

### Фильтр экспорта метаданных

`build_snapshot` применяет `data_products_only=True` при каждой публикации каталога. Таблицы без `product_id` исключаются из снимка вместе с их рёбрами связей, рёбрами происхождения и тегами управления. Источники и домены публикуются всегда. Термины глоссария публикуются только тогда, когда хотя бы одна из их физических ссылок принадлежит экспортированной (входящей в продукт) таблице. [tool-verified: `provisa/api/metadata_export/builder.py:594,609,641`]

Продукт без экспортированных участников не формирует запись снимка — листинг без участников искажал бы представление продукта в каталоге. [tool-verified: `provisa/api/metadata_export/model.py:106-113`]

### Поддержка продуктов данных по целевому каталогу

`MetadataSnapshot.data_products` достигает каждого адаптера, но только адаптеры, чья платформа имеет нативное понятие продукта данных, публикуют его как полноценную сущность; остальные публикуют входящие таблицы (уже отфильтрованные выше) без группировки по продукту.

| Цель | Представление продукта данных |
| --- | --- |
| Snowflake Horizon | Каждый продукт становится `SHARE` над физическими адресами входящих в него таблиц, обёрнутым во внутренний `CREATE ORGANIZATION LISTING` — нативный продукт данных Horizon Catalog. `publish=true` немедленно публикует листинг вживую; иначе он попадает в статус DRAFT. [tool-verified: `provisa/api/metadata_export/snowflake_horizon.py:21-34,389-418`] |
| BigQuery Dataplex | Каждый продукт становится листингом Analytics Hub через `/v1/dataProducts`. [tool-verified: `provisa/api/metadata_export/bigquery_dataplex.py:100,136,159`] |
| OpenMetadata | Каждый продукт становится нативной сущностью `DataProduct` (`/api/v1/dataProducts`) с владением, производным от домена. [tool-verified: `provisa/api/metadata_export/openmetadata.py:326-344,635`] |
| DataHub | Каждый продукт становится нативной сущностью `dataProduct` (`urn:li:dataProduct:...`) с собственными аспектами `dataProductProperties`/владения. [tool-verified: `provisa/api/metadata_export/datahub.py:133-136,443-483`] |
| Collibra | Каждый продукт становится активом типа community «Data Product», связанным со своими входящими таблицами через отношение `Data Product groups Table`. [tool-verified: `provisa/api/metadata_export/collibra.py:129-133,371-388`] |
| Atlan | Публикуется как предполагаемый (guess) пользовательский typedef `DataProduct` — у Atlan нет документированного стабильного имени типа для этого понятия, поэтому сопоставление выполняется по best-effort. [tool-verified: `provisa/api/metadata_export/atlan.py:60`] |
| Apache Atlas | Публикуется как пользовательский typedef `provisa_data_product` с отношением `provisa_data_product_members` — у Atlas нет нативного типа сущности продукта данных. [tool-verified: `provisa/api/metadata_export/atlas.py:134-147,191,256-260`] |
| OpenLineage | Не является полноценной сущностью — входящие таблицы несут пользовательскую фасету `provisa_data_product`, называющую владеющий продукт. [tool-verified: `provisa/api/metadata_export/openlineage.py:243,348`] |
