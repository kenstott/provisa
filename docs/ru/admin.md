# Admin API

Admin API — это эндпоинт Strawberry GraphQL по адресу `POST /admin/graphql` (REQ-533). Он требует роли суперпользователя или администратора (REQ-125, REQ-060) и отделён от эндпоинта данных GraphQL (REQ-533).

## Аутентификация

Передавайте свои учётные данные в заголовке `Authorization`, используя стандартный провайдер аутентификации Provisa (REQ-120):

```yaml
Authorization: Bearer <token>
```

Доступ администратора управляется возможностью (capability) `admin`, назначенной роли (REQ-060, REQ-042).

## Возможности

### Управление конфигурацией

Скачать текущую работающую конфигурацию (REQ-164):

```http
GET /admin/config
```

Возвращает полный `config.yaml` как YAML-файл. Загрузить новую конфигурацию (REQ-164):

```http
PUT /admin/config
```

Provisa проверяет YAML, перезагружает каталоги и регенерирует схемы (REQ-012, REQ-253). Перезапуск не требуется.

### Настройки времени выполнения

Читать и записывать настройки платформы времени выполнения без редактирования файла конфигурации (REQ-165):

```http
GET  /admin/settings
PUT  /admin/settings
```

Поверхность настроек охватывает перенаправление больших результатов, выборку и лимит строк по умолчанию, TTL кеша ответов, соглашение об именовании, автоотслеживание FK связей, DSN хранилища материализации, память движка федерации (`jvm_heap_gb`, `query_max_memory`, `query_max_memory_per_node`, `query_max_total_memory`, `fault_tolerant_execution`, `fault_tolerant_task_memory`, `exchange_spool_dir`) и полную поверхность настройки конвейера трассировки OpenTelemetry (REQ-1082). Лимиты обхода удалённого GraphQL и настройки тёплого уровня/кеша чтения также раскрыты (REQ-1081, REQ-1083).

Режим безопасности — `security.mode` (`standard` | `high`) — применяется при перезапуске (REQ-1079):

```http
GET  /admin/security
PUT  /admin/security
```

Назначения AI-моделей, реестр моделей эмбеддингов/векторов и лимит скорости NL — применяются при перезапуске (REQ-1080):

```http
GET  /admin/ai-models
PUT  /admin/ai-models
```

Вкладка шифрования администратора выводит свой список провайдеров динамически из реестра шифрования; недоступные провайдеры отображаются, но не выбираемы (REQ-1091).

`GET`/`HEAD /health` и `GET /setup/status` всегда без аутентификации — они обходят требование `Authorization: Bearer`, даже когда настроен провайдер аутентификации (REQ-539).

### Редактор связей

Список связей (REQ-166):

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

### Обнаружение связей с помощью AI

Запустить анализ FK на базе Claude через REST (REQ-167, REQ-018):

```bash
curl -X POST http://localhost:8001/admin/discover/relationships \
  -H "Content-Type: application/json" \
  -d '{"scope": "domain", "domain_id": "sales"}'
```

Возвращает кандидатов FK, ранжированных по уверенности. Принять кандидата:

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

Источники Neo4j и SPARQL регистрируются через эндпоинты REST (а не через GraphQL admin API) (REQ-295, REQ-297):

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

После регистрации таблицы появляются в схеме GraphQL и доступны для запросов, как и любой другой источник (REQ-016).

## GraphiQL

Admin API поставляется с GraphiQL по адресу `GET /admin/graphql` в браузере (REQ-622). Используйте его для интерактивного изучения полной схемы администратора.
