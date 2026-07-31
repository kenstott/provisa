# Подписки SSE

Provisa поддерживает push-уведомления в реальном времени через Server-Sent Events (SSE). Клиенты получают поток событий изменений без опроса. (REQ-258)

## Источники

Подписки нацелены на **зарегистрированную таблицу**:

| Источник | Доступные значения `strategy` |
|--------|-------------------------|
| Таблица (PostgreSQL) | `native` (LISTEN/NOTIFY), `poll` |
| Таблица (РСУБД, отличная от PG, с блоком источника `cdc`) | `debezium`, `kafka`, `poll` |
| Таблица (федеративное представление / любой другой источник) | только `poll` |

### Автоматическая установка триггеров PostgreSQL

Provisa автоматически устанавливает триггеры `AFTER INSERT OR UPDATE OR DELETE` на всех **предварительно утверждённых** таблицах PostgreSQL при запуске. (REQ-565) Эти триггеры вызывают `pg_notify('provisa_{table}', ...)`, чтобы сырой DML (а не только мутации Provisa) подхватывался подписками. (REQ-565)

Если установка триггера не удаётся (например, недостаточно прав — роль базы данных должна владеть таблицей), Provisa переключается на опрос по водяному знаку для этой таблицы, при условии что настроен `watermark_column`. (REQ-566) Регистрируется предупреждение. (REQ-566)

### Подписки на кросс-источниковые представления

Для представлений, соединяющих несколько источников данных через движок федерации, добавьте `watermark_column` в регистрацию таблицы. (REQ-260, REQ-283) Столбец должен существовать в SQL представления (он не обязан присутствовать в схеме GraphQL):

```sql
-- Example: federated view with derived watermark
CREATE OR REPLACE VIEW orders_with_segments AS
SELECT o.*, s.name AS segment_name,
       GREATEST(o.updated_at, s.updated_at) AS _watermark
FROM postgresql.public.orders o
JOIN mysql.crm.customer_segments s ON o.customer_id = s.customer_id;
```

Регистрируйте с `watermark_column: _watermark`. Provisa опрашивает, используя `WHERE _watermark > <last_seen>`. (REQ-260)

### Подписки на вложенные связи

Когда поле подписки выбирает поля из соединённых таблиц (через зарегистрированные связи), Provisa отслеживает **все** вовлечённые физические таблицы одновременно. (REQ-567) Изменение любой соединённой таблицы повторно запускает запрос подписки. (REQ-567)

## Эндпоинт

Подписка на таблицу:
```
GET /data/subscribe/{table}
Accept: text/event-stream
```

Соединение остаётся открытым и выдаёт одно JSON-событие на каждое изменение: (REQ-258, REQ-568)
```
data: {"event":"insert","table":"orders","row":{"id":43,"amount":55.00,"region":"east"}}

data: {"event":"update","table":"orders","row":{"id":42,"amount":199.00,"region":"west"}}
```

## Режимы доставки

Доставка выбирается через `live.strategy` в конфигурации таблицы: (REQ-813, REQ-814)

| `strategy` | Механизм | Доступно для | Требует |
|------------|-----------|---------------|---------|
| `native` | `LISTEN`/`NOTIFY` PostgreSQL, Change Streams MongoDB | PG, MongoDB | Ничего дополнительного |
| `debezium` | Топик Kafka от коннектора Debezium | Таблицы РСУБД, отличных от PG | Блок `cdc` уровня источника (Debezium + Kafka) |
| `kafka` | Произвольный дельта-топик Kafka | Любая таблица, питаемая Kafka | Блок `cdc` уровня источника |
| `poll` | Опрос на основе водяного знака | Любая таблица с водяным знаком | `watermark_column` |

### LISTEN/NOTIFY

Provisa выполняет `LISTEN <channel>` на постоянном соединении PG. (REQ-258) Мутации Provisa автоматически выдают `NOTIFY`. (REQ-565) Внешние писатели должны вызывать `NOTIFY <channel>, '<payload>'` после записей. Дополнительная инфраструктура не требуется.

### Опрос

Provisa периодически повторно выполняет запрос источника, выбирая только строки, где `watermark_column > last_watermark`. (REQ-260) Разницы выдаются как события SSE. Опрос не видит жёсткие удаления — удалённая строка не оставляет продвигающегося водяного знака. Чтобы сделать удаление видимым, используйте мягкое удаление (например, установите флаг `deleted_at`), которое продвигает столбец водяного знака; удаление тогда приходит как событие обновления, несущее маркер мягкого удаления. (REQ-260)

Конфигурация опроса таблицы (в `provisa.yaml`):
```yaml
tables:
  - id: federated_orders
    source_id: federated-source
    live:
      strategy: poll
      watermark_column: updated_at
      poll_interval: 30
      outputs:
        - type: sse
```

### Debezium CDC

Требует запущенного коннектора Debezium, пишущего в Kafka. (REQ-261) Provisa потребляет топик Kafka и пересылает события изменений подключённым клиентам SSE. (REQ-261)

Транспорт CDC настраивается один раз для каждого источника в блоке `cdc`; топики выводятся как `{topic_prefix}.{schema}.{table}` и никогда не повторяются для каждой таблицы. (REQ-824) Каждая таблица затем выбирает `strategy: debezium`:
```yaml
sources:
  - id: sales-mysql
    cdc:
      bootstrap_servers: kafka:9092
      topic_prefix: debezium
      # schema_registry_url: http://schema-registry:8081   # set for Avro; omit for JSON
    tables:
      - id: orders
        live:
          strategy: debezium
```

## Перенаправление приёмника Kafka

Любая подписка GraphQL может быть перенаправлена в топик Kafka вместо потоковой передачи обратно клиенту. (REQ-812) Добавьте заголовок `X-Provisa-Sink` к запросу подписки:

```
POST /data/graphql
Authorization: Bearer <token>
Content-Type: application/json
X-Provisa-Sink: kafka://broker:9092/my-topic
```

Сервер немедленно отвечает `202 Accepted` и запускает фоновую задачу, которая: (REQ-812)
1. Отслеживает изменения таблицы, используя то же разрешение провайдера, что и SSE (LISTEN/NOTIFY → опрос asyncpg → федеративный опрос)
2. Повторно выполняет эквивалентный запрос при каждом изменении
3. Публикует результат как JSON-сообщение в именованный топик Kafka

Приёмник работает в течение жизни процесса сервера. (REQ-812) Перезапустите сервер, чтобы остановить его (постоянная регистрация приёмника через admin API запланирована).

**Формат URI:** `kafka://[broker:port]/topic`

- Если `broker:port` опущен, используется переменная окружения `KAFKA_BOOTSTRAP_SERVERS` (по умолчанию: `localhost:9092`) (REQ-812)
- `topic` обязателен

**Пример (curl):**
```bash
curl -X POST http://localhost:8000/data/graphql \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "X-Provisa-Sink: kafka://kafka:9092/orders-live" \
  -d '{"query": "subscription { orders { id status amount } }"}'
# → 202 {"status":"streaming","sink":"kafka://kafka:9092/orders-live","table":"orders"}
```

### Приёмник Kafka как второй вывод на уровне конфигурации

Подписка на таблицу на основе опроса может одновременно публиковать в топик Kafka через `provisa.yaml`. (REQ-282, REQ-286) Подписка SSE и приёмник Kafka — оба являются выводами одного и того же движка живых запросов (Live Query Engine). (REQ-282) Каждый вывод отслеживает свой водяной знак независимо. (REQ-286)

```yaml
tables:
  - id: active-orders
    live:
      strategy: poll
      watermark_column: updated_at
      poll_interval: 30
      outputs:
        - type: sse
        - type: kafka
          topic: provisa.active-orders
          bootstrap_servers: kafka:9092
          key_column: id
```

Полный справочник конфигурации приёмника см. в [Приёмники Kafka](sources.md).

## Безопасность

Все режимы подписки применяют тот же конвейер безопасности, что и обычные запросы: (REQ-258, REQ-038)

- Фильтры RLS применяются к каждой выданной строке (REQ-040)
- Замаскированные столбцы отображаются замаскированными в событиях (REQ-040)
- Авторизация роли проверяется во время подключения (REQ-258)

## Пример клиента

```javascript
// Table subscription (LISTEN/NOTIFY)
const source = new EventSource('/data/subscribe/orders', {
  headers: { 'Authorization': 'Bearer <token>' }
});

source.onmessage = (e) => {
  const event = JSON.parse(e.data);
  console.log(event.event, event.row);
};
```
