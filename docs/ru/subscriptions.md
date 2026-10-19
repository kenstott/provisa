# Подписки SSE {: #sse-subscriptions }

Provisa поддерживает push-уведомления в реальном времени через Server-Sent Events (SSE). Клиенты получают поток событий изменений без опроса.

## Источники {: #sources }

Подписки нацелены на **зарегистрированную таблицу**:

| Источник | Доступные значения `strategy` |
| -------- | ------------------------- |
| Таблица (PostgreSQL) | `native` (LISTEN/NOTIFY), `poll` |
| Таблица (РСУБД, отличная от PG, с блоком источника `cdc`) | `debezium`, `kafka`, `poll` |
| Таблица (федеративное представление / любой другой источник) | только `poll` |

### Автоматическая установка триггеров PostgreSQL {: #postgresql-trigger-auto-installation }

Provisa автоматически устанавливает триггеры `AFTER INSERT OR UPDATE OR DELETE` на всех **предварительно утверждённых** таблицах PostgreSQL при запуске. Эти триггеры вызывают `pg_notify('provisa_{table}', ...)`, чтобы сырой DML (а не только мутации Provisa) подхватывался подписками.

Если установка триггера не удаётся (например, недостаточно прав — роль базы данных должна владеть таблицей), Provisa переключается на опрос по водяному знаку для этой таблицы, при условии что настроен `watermark_column`. Регистрируется предупреждение.

### Подписки на кросс-источниковые представления {: #cross-datasource-view-subscriptions }

Для представлений, соединяющих несколько источников данных через движок федерации, добавьте `watermark_column` в регистрацию таблицы. Столбец должен существовать в SQL представления (он не обязан присутствовать в схеме GraphQL):

```sql
-- Example: federated view with derived watermark
CREATE OR REPLACE VIEW orders_with_segments AS
SELECT o.*, s.name AS segment_name,
       GREATEST(o.updated_at, s.updated_at) AS _watermark
FROM postgresql.public.orders o
JOIN mysql.crm.customer_segments s ON o.customer_id = s.customer_id;
```

Регистрируйте с `watermark_column: _watermark`. Provisa опрашивает, используя `WHERE _watermark > <last_seen>`.

### Подписки на вложенные связи {: #nested-relationship-subscriptions }

Когда поле подписки выбирает поля из соединённых таблиц (через зарегистрированные связи), Provisa отслеживает **все** вовлечённые физические таблицы одновременно. Изменение любой соединённой таблицы повторно запускает запрос подписки.

## Эндпоинт {: #endpoint }

Подписка на таблицу:

```http
GET /data/subscribe/{table}
Accept: text/event-stream
```

Соединение остаётся открытым и выдаёт одно JSON-событие на каждое изменение:

```text
data: {"event":"insert","table":"orders","row":{"id":43,"amount":55.00,"region":"east"}}

data: {"event":"update","table":"orders","row":{"id":42,"amount":199.00,"region":"west"}}
```

## Режимы доставки {: #delivery-modes }

Доставка выбирается через `live.strategy` в конфигурации таблицы:

| `strategy` | Механизм | Доступно для | Требует |
| ------------ | ----------- | --------------- | --------- |
| `native` | `LISTEN`/`NOTIFY` PostgreSQL, Change Streams MongoDB | PG, MongoDB | Ничего дополнительного |
| `debezium` | Топик Kafka от коннектора Debezium | Таблицы РСУБД, отличных от PG | Блок `cdc` уровня источника (Debezium + Kafka) |
| `kafka` | Произвольный дельта-топик Kafka | Любая таблица, питаемая Kafka | Блок `cdc` уровня источника |
| `poll` | Опрос на основе водяного знака | Любая таблица с водяным знаком | `watermark_column` |

### LISTEN/NOTIFY

Provisa выполняет `LISTEN <channel>` на постоянном соединении PG. Мутации Provisa автоматически выдают `NOTIFY`. Внешние писатели должны вызывать `NOTIFY <channel>, '<payload>'` после записей. Дополнительная инфраструктура не требуется.

### Опрос {: #polling }

Provisa периодически повторно выполняет запрос источника, выбирая только строки, где `watermark_column > last_watermark`. Разницы выдаются как события SSE. Опрос не видит жёсткие удаления — удалённая строка не оставляет продвигающегося водяного знака. Чтобы сделать удаление видимым, используйте мягкое удаление (например, установите флаг `deleted_at`), которое продвигает столбец водяного знака; удаление тогда приходит как событие обновления, несущее маркер мягкого удаления.

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

Требует запущенного коннектора Debezium, пишущего в Kafka. Provisa потребляет топик Kafka и пересылает события изменений подключённым клиентам SSE.

Транспорт CDC настраивается один раз для каждого источника в блоке `cdc`; топики выводятся как `{topic_prefix}.{schema}.{table}` и никогда не повторяются для каждой таблицы. Каждая таблица затем выбирает `strategy: debezium`:

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

#### Кодировки сообщений {: #message-encodings }

Тема Debezium читается в одной из двух кодировок, и каждое сообщение говорит, в какой.

| Кодировка | Что несёт тема | Что нужно источнику |
|---|---|---|
| JSON | Конверт JSON-конвертера Debezium, с обёрткой `schema`/`payload` или без неё | Ничего сверх блока `cdc` |
| Avro | Формат передачи schema registry Confluent: нулевой байт, четырёхбайтовый id схемы, один элемент Avro | `schema_registry_url` и способ аутентификации в нём |

Сообщения Avro декодируются схемой, которую называет их id, получаемой из registry один раз и сохраняемой. Тема, схема которой эволюционировала, читается правильно, потому что каждое сообщение называет схему, которой оно записано. Темы Protobuf и JSON Schema не читаются.

Сообщение Avro никогда не читается как JSON. Каждая причина, по которой его не удаётся прочитать, отклоняется по имени:

| Ситуация | Отказ |
|---|---|
| Сообщение Avro, источник не называет registry | `subscribe.avro_topic_without_registry`, с темой |
| Registry недоступен или не отвечает вовремя | `subscribe.schema_registry_unreachable` |
| Registry отвергает учётные данные источника | `subscribe.schema_registry_refused_credentials` |
| Id схемы, которого нет в registry | `subscribe.schema_id_unknown`, с id |
| Id называет схему Protobuf или JSON | `subscribe.schema_not_avro` |

Registry, который недоступен или отвергает учётные данные, отклоняется при запуске подписки, до открытия потока. Запрос к registry ждёт подключения не более 5 секунд и ответа не более 10 секунд, и меньше, если у запроса осталось меньше времени.

#### Аутентификация schema registry {: #schema-registry-authentication }

`schema_registry_auth` называет метод; поля, нужные этому методу, проверяются при сохранении источника.

| `schema_registry_auth` | Обязательные поля | Отправляется в registry |
|---|---|---|
| `none` (по умолчанию) | — | Без учётных данных |
| `basic` | `schema_registry_username`, `schema_registry_password` | Базовая аутентификация HTTP |
| `bearer` | `schema_registry_token` | `Authorization: Bearer <token>` (статический токен) |
| `mtls` | `schema_registry_client_cert`, `schema_registry_client_key` | Клиентский сертификат, в рукопожатии TLS |

`schema_registry_ca` необязателен при любом методе: пакет CA, которому доверять, когда сертификат registry не подписан публичным центром.

Пароль и токен следуют контракту секретов. Значение, введённое в форме источника, хранится в хранилище организации, и источник сохраняет ссылку `${secret:NAME}`; ссылка, записанная в файле конфигурации (`${env:...}`, `${secret:...}`), сохраняется как записана. Сертификат, ключ и пакет CA — это файлы, которые открывает серверный процесс Provisa, указываемые абсолютным путём.

```yaml
sources:
  - id: sales-mysql
    cdc:
      bootstrap_servers: kafka:9092
      topic_prefix: debezium
      schema_registry_url: https://schema-registry:8081
      schema_registry_auth: basic
      schema_registry_username: provisa
      schema_registry_password: ${secret:registry_password}
      schema_registry_ca: /etc/provisa/registry-ca.pem
```

## Когда поток завершается ошибкой {: #when-a-stream-ends-on-a-failure }

Открывшийся поток уже ответил `200`, поэтому ошибка после этого не может быть статусом HTTP. Подписчик получает последний кадр, в котором сказано почему, и затем поток закрывается. Поток никогда не закрывается из-за ошибки без этого кадра.

Кадр несёт то, чем на ту же ошибку ответил бы обычный запрос:

| Ошибка | Тело |
|---|---|
| Отказ с кодом (schema registry, региона, отказ governance) | `detail`, `code`, `params` |
| Крайний срок запроса | `detail: "Request timed out"` |
| Что-либо неожиданное | `detail: "Internal server error"`, `type` (имя типа исключения) |

Каждое тело несёт также `subscription_id`. При неожиданной ошибке сообщение и трассировка находятся в журнале сервера под этим id, а не в кадре.

Подписка на таблицу (`/data/subscribe/{table}`) отправляет тело как данные события с именем `stream_error`:

```text
event: stream_error
data: {"detail": "Topic 'debezium.app.orders' carries Avro messages, and its source names no schema registry to read them with: set the source's schema registry URL", "code": "subscribe.avro_topic_without_registry", "params": {"topic": "debezium.app.orders"}, "subscription_id": "5f0c2a9d41b7e386"}
```

Событие не называется `error`, потому что `EventSource` сам порождает событие `error` при обрыве соединения, и слушатель не смог бы их различить. Слушайте его по имени:

```javascript
source.addEventListener("stream_error", (e) => {
  const { detail, code, params } = JSON.parse(e.data);
  source.close();
});
```

Подписка GraphQL сохраняет форму ответа GraphQL, с кодом и params в `extensions`:

```json
{"errors": [{"message": "…", "extensions": {"code": "subscribe.avro_topic_without_registry", "params": {"topic": "debezium.app.orders"}, "subscription_id": "5f0c2a9d41b7e386"}}]}
```

Один случай не завершает подписку GraphQL: когда повторное чтение после одного изменения не удаётся, этот кадр отправляется в той же форме, и поток переходит к следующему изменению. Ошибка начального чтения, самого потока изменений или его запуска — последний кадр.

## Перенаправление приёмника Kafka {: #kafka-sink-redirect }

Любая подписка GraphQL может быть перенаправлена в топик Kafka вместо потоковой передачи обратно клиенту. Добавьте заголовок `X-Provisa-Sink` к запросу подписки:

```yaml
POST /data/graphql
Authorization: Bearer <token>
Content-Type: application/json
X-Provisa-Sink: kafka://broker:9092/my-topic
```

Сервер немедленно отвечает `202 Accepted` и запускает фоновую задачу, которая:

1. Отслеживает изменения таблицы, используя то же разрешение провайдера, что и SSE (LISTEN/NOTIFY → опрос asyncpg → федеративный опрос)
2. Повторно выполняет эквивалентный запрос при каждом изменении
3. Публикует результат как JSON-сообщение в именованный топик Kafka

Приёмник работает в течение жизни процесса сервера. Перезапустите сервер, чтобы остановить его.

**Формат URI:** `kafka://[broker:port]/topic`

- Если `broker:port` опущен, используется переменная окружения `KAFKA_BOOTSTRAP_SERVERS` (по умолчанию: `localhost:9092`)
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

### Приёмник Kafka как второй вывод на уровне конфигурации {: #kafka-sink-as-a-config-level-second-output }

Подписка на таблицу на основе опроса может одновременно публиковать в топик Kafka через `provisa.yaml`. Подписка SSE и приёмник Kafka — оба являются выводами одного и того же движка живых запросов (Live Query Engine). Каждый вывод отслеживает свой водяной знак независимо.

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
          role: analyst           # required: the rows are published governed as this role
```

Вывод Kafka публикует строки таблицы, регулируемые как роль, которую он называет: её правила строк, видимость колонок и маски применяются как к чтению этой ролью. Вывод без `role` отклоняется при загрузке конфигурации.

## Безопасность {: #security }

Все режимы подписки применяют тот же конвейер безопасности, что и обычные запросы:

- Фильтры RLS применяются к каждой выданной строке
- Замаскированные столбцы отображаются замаскированными в событиях
- Авторизация роли проверяется во время подключения

## Пример клиента {: #client-example }

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
