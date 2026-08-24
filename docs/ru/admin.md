# Административный API

Административный API — это конечная точка Strawberry GraphQL по адресу `POST /admin/graphql` (REQ-533). Она требует роли суперпользователя или администратора (REQ-125, REQ-060) и отделена от конечной точки GraphQL для данных (REQ-533).

## Аутентификация

Передавайте учётные данные в заголовке `Authorization`, используя штатный провайдер аутентификации Provisa (REQ-120):

```yaml
Authorization: Bearer <token>
```

Административный доступ управляется возможностью `admin`, назначенной роли (REQ-060, REQ-042).

### Персональные токены доступа

Персональный токен доступа принимается везде, где принимается bearer-токен, включая эту конечную точку. Выпуск и отзыв выполняются самостоятельно — это собственные учётные данные владельца токена, поэтому они находятся в профиле пользователя в административном интерфейсе, а не на административной странице, рядом с выходом из организации и удалением учётной записи. Администратор не выпускает токены от чужого имени. (REQ-1263)

| Маршрут | Действие |
| ------- | -------- |
| `POST /auth/tokens` | Выпускает токен для вызывающей стороны. Тело: `name`, необязательные `role_id`, `scopes`, `expires_in_days` (1–366). Ответ — единственное место, где вообще появляется секрет |
| `GET /auth/tokens` | Активные токены вызывающей стороны в этой организации — отображаемый префикс, имя, отметки времени жизненного цикла и хеш, идентифицирующий токен при отзыве. Никогда не рабочие учётные данные |
| `DELETE /auth/tokens/{token_hash}` | Отзывает один из токенов вызывающей стороны. 404, если токен не принадлежит ей или уже отозван |

Если `role_id` не указан, токен разрешается в ту роль, которой владеет его владелец; указание роли сужает токен относительно его владельца. Отзыв происходит и неявно: удаление членства пользователя в организации отзывает его токены для этой организации. О самих учётных данных см. [Модель безопасности](security.md#personal-access-tokens).

## Возможности

### Управление конфигурацией

Скачать текущую работающую конфигурацию (REQ-164):

```http
GET /admin/config
```

Возвращает полный `config.yaml` в виде YAML-файла. Загрузить новую конфигурацию (REQ-164):

```http
PUT /admin/config
```

Provisa проверяет YAML, перезагружает каталоги и заново генерирует схемы (REQ-012, REQ-253). Перезапуск не требуется.

### Настройки времени выполнения

Чтение и запись настроек платформы времени выполнения без правки файла конфигурации (REQ-165):

```http
GET  /admin/settings
PUT  /admin/settings
```

Поверхность настроек охватывает перенаправление больших результатов, выборку и лимит строк по умолчанию, TTL кеша ответов, соглашение об именовании, автоотслеживание FK для связей, DSN хранилища материализации, память федеративного движка (`jvm_heap_gb`, `query_max_memory`, `query_max_memory_per_node`, `query_max_total_memory`, `fault_tolerant_execution`, `fault_tolerant_task_memory`, `exchange_spool_dir`) и всю поверхность тонкой настройки конвейера трассировки OpenTelemetry (REQ-1082). Также доступны лимиты обхода удалённого GraphQL и настройки тёплого уровня и кеша чтения (REQ-1081, REQ-1083).

Профиль безопасности — `security.mode` (`standard` | `high`) — применяется при перезапуске (REQ-1079):

```http
GET  /admin/security
PUT  /admin/security
```

Назначения моделей ИИ, реестр моделей эмбеддингов и векторных моделей, а также лимит частоты запросов на естественном языке — вступают в силу со следующим запросом, перезапуск не требуется (REQ-1349): [tool-verified: `provisa/api/admin/ai_models_router.py:38-39`]

```http
GET  /admin/ai-models
PUT  /admin/ai-models
```

Вкладка шифрования в административном интерфейсе получает список провайдеров вживую из реестра шифрования; недоступные провайдеры отображаются, но выбрать их нельзя (REQ-1091).

`GET`/`HEAD /health` и `GET /setup/status` всегда не требуют аутентификации — они обходят требование `Authorization: Bearer` даже при настроенном провайдере аутентификации (REQ-539).

### Федеративный движок

Прочитать или изменить, какой движок использует развёртывание (REQ-916):

```http
GET  /admin/federation-engine
PUT  /admin/federation-engine
```

`GET` возвращает ключ активного движка и нужные ему поля конфигурации. `PUT` принимает тело с `engine` (ключ) и любыми полями, специфичными для движка; выбор сохраняется в конфигурации платформы и вступает в силу при следующем перезапуске службы. [tool-verified: `provisa/api/admin/settings_router.py:730-829`]

### Редактор связей

Получить список связей (REQ-166):

```graphql
query {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceColumn
    targetColumn
    cardinality
    materialize
  }
}
```

Создать связь (REQ-019):

```graphql
mutation {
  upsertRelationship(input: {
    id: "orders-to-customers"
    sourceTableId: "orders"
    targetTableId: "customers"
    sourceColumn: "customer_id"
    targetColumn: "id"
    cardinality: "many_to_one"
  }) {
    success
  }
}
```

### Обнаружение связей с помощью ИИ

Запустить анализ внешних ключей на базе Claude через REST (REQ-167, REQ-018):

```bash
curl -X POST http://localhost:8001/admin/discover/relationships \
  -H "Content-Type: application/json" \
  -d '{"scope": "domain", "domain_id": "sales"}'
```

Возвращает кандидатов на внешние ключи, ранжированных по уверенности. Принять кандидата:

```bash
curl -X POST http://localhost:8001/admin/discover/candidates/{id}/accept \
  -H "Content-Type: application/json" \
  -d '{"name": "orders_to_customers"}'
```

### Интроспекция схемы

Просмотр опубликованных таблиц по всем источникам (REQ-008):

```graphql
query {
  tables {
    id
    sourceId
    columns {
      columnName
      unmaskedTo
      writableBy
    }
  }
}
```

### Проверка зависимостей колонки (REQ-1484)

Прежде чем сохранить правку таблицы, которая переименовывает SQL-псевдоним колонки или удаляет колонку, спросите, что ещё
на неё ссылается:

```graphql
query {
  columnDependents(tableId: "42", renamed: ["order_total"], removed: ["legacy_code"]) {
    columnName
    dependents { kind name detail breaksOn }
  }
}
```

Переименование псевдонима ломает каждый артефакт, написанный по внешнему имени, — представления, материализованные представления, выражения
метрик, предикаты RLS, контракты качества данных. Удаление колонки ломает всё перечисленное плюс артефакты, которые
хранят физическое `column_name`: связи, привязки глоссария, назначения тегов. `breaksOn`
говорит, что именно. Страница таблиц выполняет этот запрос при сохранении и показывает результат как предупреждающий диалог. О том, что запрос охватывает, а что нет, см.
[Происхождение данных](lineage.md).

### Управление представлениями

Зарегистрировать материализованное представление (REQ-133, REQ-135):

```graphql
mutation {
  registerTable(input: {
    viewSql: "SELECT o.id, o.amount, c.name FROM orders o JOIN customers c ON o.customer_id = c.id"
    mvRefreshInterval: 300
    materialize: true
  }) {
    success
  }
}
```

Запустить обновление вручную (REQ-135):

```graphql
mutation {
  refreshMv(mvId: "orders-with-customers") {
    success
  }
}
```

### Регистрация графовых источников

Источники Neo4j и SPARQL регистрируются через REST-эндпоинты (не через административный API GraphQL) (REQ-295, REQ-297):

**Neo4j:**

```bash
# 1. Register the Neo4j source
curl -X POST http://localhost:8001/admin/sources/neo4j \
  -H "Content-Type: application/json" \
  -d '{"source_id": "graph", "host": "neo4j", "port": 7474, "database": "neo4j"}'

# 2. Preview a Cypher query (validates scalar projections)
curl -X POST http://localhost:8001/admin/sources/neo4j/graph/preview \
  -H "Content-Type: application/json" \
  -d '{"cypher": "MATCH (p:Person) RETURN p.name AS name, p.age AS age"}'

# 3. Register a table (runs preview+validate automatically)
curl -X POST http://localhost:8001/admin/sources/neo4j/graph/tables \
  -H "Content-Type: application/json" \
  -d '{"table_name": "people", "cypher": "MATCH (p:Person) RETURN p.name AS name, p.age AS age", "ttl": 300}'
```

**SPARQL:**

```bash
# 1. Register the SPARQL source
curl -X POST http://localhost:8001/admin/sources/sparql \
  -H "Content-Type: application/json" \
  -d '{"source_id": "kg", "endpoint_url": "http://fuseki:3030/ds/sparql"}'

# 2. Register a table (probes endpoint and infers columns)
curl -X POST http://localhost:8001/admin/sources/sparql/kg/tables \
  -H "Content-Type: application/json" \
  -d '{"table_name": "products", "sparql_query": "SELECT ?name ?category WHERE { ?p a :Product ; :name ?name ; :category ?category . }", "ttl": 600}'
```

После регистрации таблицы появляются в схеме GraphQL и доступны для запросов, как любой другой источник (REQ-016).

### Импорт из Hasura / DDN (REQ-1483)

Преобразуйте существующий проект Hasura v2 или Hasura DDN в конфигурацию Provisa через административный интерфейс или API — ничего не будет записано, пока вы это не утвердите.

```http
POST /admin/import/hasura/preview
POST /admin/import/hasura/apply
```

**Предпросмотр** преобразует загруженный архив и возвращает предлагаемый `config_yaml`, список предупреждений и сводку найденного (количество источников, доменов, таблиц, колонок, ролей, связей и правил RLS). В базу данных арендатора ничего не записывается. Тело запроса:

```json
{
  "filename": "my-hasura-project.zip",
  "content_b64": "<base64-encoded archive>",
  "flavor": "auto",
  "domain_map": {"public": "sales"},
  "source_overrides": {}
}
```

`flavor` принимает значения `"auto"` (определяется по структуре архива), `"hasura_v2"` или `"ddn"`.

**Применение** берёт проверенный вами (и при необходимости отредактированный) YAML и загружает его в действующую организацию — по тому же пути горячей перезагрузки, что и `PUT /admin/config`. Тело запроса: `{"config_yaml": "<yaml string>"}`.

Предпросмотр никогда не кеширует преобразованный YAML на сервере; применение берёт тот YAML, который вы передали, поэтому применяется ровно то, что было проверено. [tool-verified: `provisa/api/admin/import_router.py`]

### Обмен через Apache Ossie (REQ-1316, REQ-1321)

Provisa взаимодействует с Apache Ossie (incubating) как граница импорта и экспорта.

```http
GET  /admin/ossie
POST /admin/ossie/import
```

**Экспорт** (`GET /admin/ossie`) выводит YAML-документ Ossie из живой управляемой модели при каждом запросе — он никогда не кешируется, поэтому не может устареть. Ответ имеет тип `text/yaml` с заголовком `Content-Disposition: attachment`. Таблицы становятся объектами `dataset`, колонки — объектами `field`, а связи отображаются в объекты `relationship` Ossie. (REQ-1321) [tool-verified: `provisa/api/admin/ossie_router.py:download_ossie`]

**Импорт** (`POST /admin/ossie/import`) принимает документ Ossie в формате YAML или JSON (формат определяется автоматически). Он разбирает документ и возвращает предлагаемые регистрации таблиц и связей в виде объекта JSON — ничего не регистрируется. Экран проверки в административном интерфейсе позволяет принять или урезать предложения до того, как сработает любая мутация. (REQ-1316) [tool-verified: `provisa/api/admin/ossie_router.py:import_ossie`]

### Объектное хранилище (REQ-1046, REQ-1048, REQ-1049)

Прочитать или настроить хранилище материализации организации:

```http
GET  /admin/org-storage
PUT  /admin/org-storage
```

`GET` сообщает, какую часть выделенного платформой объёма хранилища использует организация. `PUT` регистрирует собственный DSN хранилища организации (шифруется при хранении; никогда не возвращается методом GET). После настройки материализации организации попадают в её собственный бакет и больше не учитываются в платформенной квоте. Отправка `storage_url: null` очищает настройку и возвращает организацию в платформенное хранилище. [tool-verified: `provisa/api/admin/org_storage_router.py`]

### Шифрование организации (REQ-1574)

Задать или сменить ключ шифрования организации при хранении:

```http
GET  /admin/org-encryption
PUT  /admin/org-encryption
```

`GET` возвращает отпечаток ключа, его идентификатор и происхождение — никогда сам ключевой материал. `PUT` задаёт или сменяет ключ. Передайте `key_b64` (32 необработанных байта в кодировке base64), чтобы использовать собственный ключ, или опустите его, чтобы Provisa сгенерировала ключ сама. Удаления нет: вывод последнего ключа сделал бы нечитаемыми все данные, которые он защищал. [tool-verified: `provisa/api/admin/org_encryption_router.py`]

## GraphiQL

Административный API поставляется с GraphiQL по адресу `GET /admin/graphql` в браузере (REQ-622). Используйте его для интерактивного изучения полной административной схемы.

## Управленческие представления домена ops (REQ-1386)

Восемь SQL-представлений создаются во встроенном домене `ops` при каждой установке. [tool-verified: `provisa/api/startup_seed.py:225-331` `_seed_ops_domain`] Они предоставляют журнал аудита запросов в виде управляемых таблиц — доступных через SQL (pgwire), GraphQL и Cypher с теми же правилами доступа к домену, RLS и маскирования, что и у любой бизнес-таблицы.

`org_admin` назначается ответственным за домен ops при создании, поэтому домен никогда не появляется как пробел в управлении в `stale_metadata`. [tool-verified: `startup_seed.py:326-331`]

| Представление | На какой вопрос отвечает |
| --- | --- |
| `usage_ranking` | Количество запросов и число уникальных пользователей по каждой зарегистрированной таблице; таблицы с нулём обращений всплывают как кандидаты на вывод из эксплуатации |
| `deprecated_usage` | Каждое обращение к таблице или колонке с тегом `deprecated` — активные потребители, мешающие безопасному удалению |
| `pii_access` | Каждое обращение к таблице или колонке с тегом `pii`: кто запрашивал, под какой ролью, через какую поверхность |
| `policy_denials` | Все попытки доступа, отклонённые управлением (HTTP 401/403) |
| `surface_mix` | Ежедневное количество запросов и число уникальных пользователей по каждой протокольной поверхности (SQL, GraphQL, Cypher, gRPC и т. д.) |
| `query_health` | Ежедневное количество ошибок и средняя и максимальная задержка по каждой поверхности |
| `stale_metadata` | Таблицы и колонки без описаний; домены без ответственного |
| `join_hotspots` | Пары таблиц, которые запрашиваются вместе чаще всего, — кандидаты на материализацию или кеширование |

Сегодня действуют два ограничения. Детализация — на уровне таблицы: журнал аудита фиксирует `table_ids`, а не отдельные затронутые колонки. Текст запроса зашифрован (REQ-689) и исключён из всех перечисленных представлений; он доступен только через авторизованный административный путь расшифровки. [tool-verified: `_meta_views.py:148-162` — comment notes `query_text_enc` exclusion]

Роли нужен доступ к домену `ops`, чтобы эти представления были видны. Выдайте его так же, как доступ к любому другому домену.

```sql
-- Which tables have never been queried?
SELECT table_name, domain_id
FROM ops.usage_ranking
WHERE query_count = 0;

-- Who accessed PII-tagged data in the last 7 days?
SELECT user_id, role_id, source, pii_column, logged_at
FROM ops.pii_access
WHERE logged_at >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY logged_at DESC;

-- Where does traffic originate by protocol?
SELECT source, day, query_count, distinct_users
FROM ops.surface_mix
ORDER BY day DESC, query_count DESC;
```

Те же запросы выполняются как GraphQL или Cypher через любой управляемый транспорт — pgwire, Arrow Flight или Bolt. [inferred from governed-surface design]

## Просмотр отчётов (REQ-1390)

Просмотр отчётов находится по адресу `/admin/reports`. Роли без возможности `observability` не могут его открыть.

Левая панель перечисляет каждую зарегистрированную таблицу домена `ops`, отсортированную по псевдониму. [tool-verified: `ReportsTab.tsx:46-52` — filters `tables` to `domainId === "ops"`] Восемь предустановленных управленческих представлений появляются там автоматически. Щёлкните любой отчёт, чтобы загрузить его в средстве просмотра управляемых данных справа.

**Добавление собственного отчёта.** Кнопка «Add report» открывает диалог. Укажите имя, необязательное описание и оператор SELECT. При сохранении представление регистрируется как управляемая производная таблица в домене `ops` — каталогизированная, с контролем доступа и доступная для запросов через все поверхности наравне с предустановленными представлениями. [tool-verified: `ReportsTab.tsx:70-96` — `registerTable` called with `sourceId: DERIVED_SOURCE_ID, domainId: "ops"`]

**Удаление.** Значок корзины появляется только у собственных отчётов. Предустановленные управленческие представления нельзя удалить из этого интерфейса. [tool-verified: `ReportsTab.tsx:151` — `const custom = report.sourceId === DERIVED_SOURCE_ID` gates the delete button]

## Предпросмотр таблицы (REQ-1392)

Разверните любую строку таблицы на странице таблиц. Кнопка **Preview** открывает модальное окно шириной 90% с живыми управляемыми данными таблицы. [tool-verified: `TablePreviewModal.tsx:24` — `size="90%"`; `GovernedTableViewer.tsx` is the underlying viewer]

Таблицы, основанные на API с обязательными параметрами пути, блокируют предпросмотр, пока эти значения не заданы. Встроенная форма собирает каждый обязательный параметр до первого запроса; необязательные параметры запроса появляются в той же форме. [tool-verified: `GovernedTableViewer.tsx:51-55, 153-155` — `requiredParamColumns` check; "paramsRequired" message shown when `activeParams == null`]

## Средство просмотра управляемых данных (REQ-1391)

Один и тот же компонент просмотра работает и в модальном окне предпросмотра, и в просмотре отчётов. Его поведение в обоих случаях одинаково.

**Постраничная навигация на сервере.** Каждая страница — это отдельный управляемый `SELECT *` с `LIMIT 101 OFFSET n`. На странице отображается 100 строк; 101-я показывает, есть ли ещё. Полный набор данных никогда не загружается в браузер. [tool-verified: `nativeParams.ts:72` — `LIMIT ${pageSize + 1} OFFSET ${page * pageSize}`; `types.ts:74` — `PAGE_SIZE = 100`]

**Проталкиваемые фильтры и сортировки.** У каждого заголовка колонки есть поле фильтра. Условия фильтра превращаются в предикаты `WHERE LOWER(CAST(col AS VARCHAR)) LIKE LOWER('%term%')`; щелчки по сортировке дают выражения `ORDER BY`. И то и другое уходит в базу данных — фильтр по таблице с миллиардом строк сканирует источник, а не те 100 строк, что перед вами. [tool-verified: `nativeParams.ts:53-70`]

**Многоуровневая группировка.** Значок «Layers» в заголовке любой колонки включает эту колонку в группировку. Колонки группировки идут первыми в `ORDER BY`, чтобы члены группы попадали на ту же страницу, что и их заголовок, через границы страниц. Колонки первичного ключа добавляются в конец как устойчивый разделитель. [tool-verified: `nativeParams.ts:61-70` — group columns first, then explicit sorts, then PKs] Строки-заголовки групп сворачиваются; сворачивание скрывает члены группы без нового запроса. [tool-verified: `useResultsGrid.ts:150-171` — `collapsedGroups` set gates the `build()` recursion]

**Сохранение выбора.** Настройки фильтра, сортировки и группировки сохраняются в `localStorage` под ключом `provisa.grid.table:<domain>.<table>` и восстанавливаются при следующем посещении. [tool-verified: `useResultsGrid.ts:95-98`, `GovernedTableViewer.tsx:66`]

**Экспорт.** Скачайте текущую страницу как CSV или скопируйте её в буфер обмена как текст с разделителями-табуляциями. Экспорт охватывает только видимую страницу. [tool-verified: `useResultsGrid.ts:247-274` — both handlers iterate `displayRows`, which in server-paged mode is the current page]
