# Реплики {: #replicas }

Реплика — это копия таблицы источника, которую Provisa хранит в хранилище материализации движка и читает вместо источника. На этой странице описано, по чему индексируется реплика, где хранить реплики и насколько они свежие. Реплицируется ли таблица вообще, определяют [`replicate`](configuration.md#the-replicate-setting) и то, до чего может дотянуться движок; см. [What an engine changes](engines.md#what-an-engine-changes). (REQ-826)

## Реплики индексируются {: #replicas-are-indexed }

Provisa индексирует реплику так, как модель использует её таблицу. (REQ-2015) [tool-verified: `provisa/federation/replica_indexes.py` `plan`, `join_columns`]

- **Объявленный первичный ключ**, обеспечиваемый как ключ.
- **Столбцы, по которым соединяются объявленные связи**:
  - внешний ключ каждой связи, источником которой является эта таблица;
  - целевой столбец каждой связи, целью которой является эта таблица, если этот столбец уже не является ключом;
  - оба внешних ключа таблицы-связки, через которую проходит связь.

Возьмём такую связь:

```yaml
relationships:
  - id: orders-to-customers
    source_table_id: orders
    target_table_id: customers
    source_column: customer_id
    target_column: id
    cardinality: many-to-one
```

Реплика `orders` получает свой ключ и индекс по `customer_id`. Реплика `customers` получает свой ключ, а если `id` и есть этот ключ, больше ничего не нужно. Соединение двух таблиц тогда читает индекс с обеих сторон — в хранилище, которое использует индексы для соединений (см. [Что строит каждое хранилище](#what-each-store-builds)). Связь по вычисляемому столбцу, которого нет в реплике, индекса не получает. [tool-verified: `provisa/federation/replica_indexes.py` `plan`]

Объявите ключ через `is_primary_key: true` на столбце. Реплика без объявленного ключа получает только индексы соединений. [tool-verified: `provisa/federation/replica_builds.py` `plan_indexes`]

### Сначала читаемая, потом индексированная {: #readable-first-indexed-after }

Загрузка никогда не ждёт индексов. Реплика читается с момента подмены; затем исполнитель реплик строит её индексы по одному, отдельным проходом, под блокировкой индексов каждой реплики. [tool-verified: `provisa/federation/replica_runner.py` `_planned`, `_start_index_builds`] Первые запросы после загрузки выполняются без них.

Каждая загрузка записывает индексы, которые она запросила, построенные и завершившиеся неудачей — по именам — в запись реплики в плоскости управления (`replica_state.index_state`). [tool-verified: `provisa/federation/replica_state.py` `record_index_plan`, `settle_index_state`]

- **Неудавшийся индекс никогда не проваливает загрузку.** Он записывается в журнал с причиной, и следующая загрузка запрашивает его снова. Строка журнала выглядит так: `replica of <source>.<schema>.<table>: index <name> on (<columns>) failed: <cause>`. [tool-verified: `provisa/federation/replica_indexes.py` `build_due`]
- **Строки, повторяющие объявленный ключ, — это отдельная ошибка** с кодом `replica.index_duplicate_key`, в которой названы столбцы ключа. В источнике есть дубликаты набора столбцов, который модель объявляет уникальным. Повтор этого не исправит: исправьте данные в источнике или исправьте объявленный ключ. [tool-verified: `provisa/federation/replica_indexes.py` `DuplicateKey`, `DUPLICATE_KEY`]
- **Загрузка, заменяющая таблицу посреди построения,** оставляет предыдущие построения позади; они останавливаются и ничего не записывают, а новая таблица получает свои собственные. В хранилище PostgreSQL подмена отменяет построение, ещё выполняющееся на заменяемой таблице. [tool-verified: `provisa/federation/replica_target.py` `_pg_swap`; `provisa/federation/replica_indexes.py` `build_due`]
- **План следует модели при каждой загрузке.** Связь, добавленная позже, получает свой индекс при следующей загрузке реплики. [tool-verified: `provisa/federation/replica_builds.py` `plan_indexes`]

Admin UI не показывает запись об индексах. Смотрите журнал сервера или столбец `index_state` в строке реплики. [tool-verified: no reader of `index_state` outside `provisa/federation/replica_state.py` and `replica_indexes.py`]

### Что строит каждое хранилище {: #what-each-store-builds }

| Хранилище | Что строится | Как |
| --- | --- | --- |
| PostgreSQL: собственная база движка `pg`, хранилище PostgreSQL у Trino или DuckDB, движок `sqlalchemy` поверх PostgreSQL | Ключ и индексы соединений | `CREATE INDEX CONCURRENTLY` на собственном подключении, поэтому читатели и писатели не блокируются. Ключ строится как уникальный индекс, затем становится первичным ключом через `ADD CONSTRAINT ... PRIMARY KEY USING INDEX`. Недействительный индекс, оставленный отменённым построением, удаляется и строится заново. |
| Другие базы данных за реляционным движком (`mysql`, `oracle`, `mssql` и остальные) | Ключ и индексы соединений | Собственный DDL диалекта. База данных, отклонившая DDL, фиксирует индекс как неудавшийся. |
| Встроенный файл DuckDB, MotherDuck | Ключ и индексы соединений | Через брокер хранилища: `ALTER TABLE ... ADD PRIMARY KEY`, `CREATE INDEX IF NOT EXISTS`. |
| ClickHouse | Только ключ сортировки | Реплика ClickHouse упорядочена по объявленному ключу (`ORDER BY`), заданному при загрузке. Вторичного B-tree индекса в ClickHouse для построения нет. |
| Snowflake, BigQuery, Databricks, Fabric, Synapse | Ничего | Хранилище данных само организует таблицу. |

[tool-verified: `provisa/federation/replica_target.py` `PostgresIndexer`, `SqlAlchemyIndexer`, `DuckDBStoreIndexer`; `provisa/federation/materialize_broker.py` `_SyncedStore.replica_index`; `provisa/federation/clickhouse_store.py` `create_ddl`; `replica_indexer` in `backend.py`, `pg_backend.py`, `duckdb_backend.py`, `sqlalchemy_backend.py`, `clickhouse_backend.py`, `snowflake_backend.py`, `bigquery_backend.py`, `databricks_backend.py`, `mssql_warehouse_backend.py`]

Индекс помогает только соединению, которое выполняется там же, где индекс. Два движка читают своё хранилище, не используя его индексы для соединений:

- **DuckDB со встроенным файлом.** Движок никогда не подключает файл. Каждая инструкция читает собственную локальную копию каждой названной ею таблицы хранилища, создаваемую заново при каждом изменении хранилища. Соединения выполняются как хеш-соединения над этой копией, поэтому индексы соединений их не ускоряют; индексы файла обеспечивают ключ и обслуживают всё, что читает файл на месте. [tool-verified: `provisa/federation/duckdb_runtime.py` `_store_is_file`, `_copy_store_table`; `provisa/federation/replica_target.py` `DuckDBStoreIndexer`]
- **DuckDB с хранилищем PostgreSQL.** DuckDB читает хранилище через расширение `postgres` с отключённой передачей фильтров (ошибка в upstream duckdb/duckdb-postgres#564), поэтому он сканирует реплику, а фильтрацию и соединения выполняет в DuckDB. Индексы хранилища построены, но эти чтения их не используют. [tool-verified: `provisa/federation/duckdb_extensions.py` `_EXTENSION_SETTINGS`]

Хранилище MotherDuck подключается и читается на месте. [tool-verified: `provisa/federation/duckdb_runtime.py` `_store_is_file`]

## Используйте реляционную базу данных как хранилище реплик там, где движок выполняет в ней SQL {: #use-a-relational-database-as-the-replica-store-where-the-engine-runs-sql-in-it }

Когда движок выполняет соединения внутри реляционной базы данных, держите реплики там. Тогда соединения читают описанные выше индексы. Так работают три конфигурации:

- **Движок `pg`.** Реплики загружаются в собственную базу PostgreSQL движка, и каждая инструкция выполняется там, через их индексы. [tool-verified: `provisa/federation/pg_backend.py` `replica_target`, `replica_indexer`]
- **Реляционный движок** (`mysql`, `oracle`, `mssql`, универсальный `sqlalchemy` и остальные). Каждый источник загружается в эту базу данных и запрашивается там. [tool-verified: `provisa/federation/sqlalchemy_backend.py` `replica_indexer`; [Engine reference](configuration.md#engine-reference)]
- **Trino с хранилищем PostgreSQL.** PostgreSQL — единственное хранилище, которое поддерживает Trino; Trino читает реплики через свой коннектор PostgreSQL. [tool-verified: `provisa/federation/engine.py` `build_trino_engine` `supported_materialize_stores`; `provisa/federation/backend.py` `TrinoBackend.materialize_store_target`] Фильтр или соединение, которые Trino передаёт в PostgreSQL, используют индексы; работа, которую Trino выполняет сам, — нет. [inferred]

Направьте движок на хранилище с помощью `PROVISA_MATERIALIZE_URL` или `materialize_store_url`; собственное зарегистрированное хранилище организации имеет приоритет над обоими. [tool-verified: `provisa/federation/engine.py` `materialize_store`, `configured_materialize_url`]

```bash
PROVISA_MATERIALIZE_URL="postgresql://provisa:secret@replicas.internal:5432/replicas"
```

Это общая рекомендация, а не правило. Окупится ли она, зависит от движка, источников, хранилища и размеров таблиц. Учитывайте следующее:

- **Доступ при каждом запросе.** Хранилище, до которого движок достаёт по сети, добавляет к каждому чтению обращение и передачу данных. Встроенный файл DuckDB локален и столбцовый, он сканируется быстро. [inferred]
- **Стоимость загрузки.** Запись реплики в реляционную базу данных с последующим построением её индексов обходится дороже записи локального файла. [inferred]
- **Собственная нагрузка базы данных.** Чтения реплик, загрузки и построения индексов выполняются на этой базе данных. Рассчитывайте её размер с их учётом. [inferred]
- **Большие сканирования.** Индекс бесполезен для запроса, читающего большую часть крупной таблицы. Столбцовое хранилище сканирует её быстрее строкового. [inferred]

## Выбор места для реплик {: #choosing-where-replicas-live }

Хранилище — один из нескольких рычагов, и они пересекаются. Сначала определитесь с остальными; хранилище решает лишь то, что они оставляют.

| Рычаг | Что делает | Где помогает |
| --- | --- | --- |
| Hot tables | Небольшая справочная таблица хранится в Redis и подставляется в каждый запрос, который её читает, как встроенное CTE (общее табличное выражение) `VALUES`. [tool-verified: `provisa/compiler/sql_gen.py`, `provisa/cache/hot_tables.py`] | Соединения с небольшими справочниками в любом хранилище: строки передаются внутри запроса. Цель связи «многие к одному» или таблица размером не более `hot_tables.auto_threshold` строк (по умолчанию 1000) становится горячей при первом чтении. `hot: true` принудительно включает таблицу, а `hot: false` не пускает её. [tool-verified: `provisa/cache/hot_tables.py` `promote_on_read` and the loader that registers candidates; `provisa/core/models.py` `HotTablesConfig`, `Table.hot`] |
| Материализованные представления, которые вы определяете | Представление с `materialize: true` сохраняется и обновляется. Запрос, читающий представление, обслуживается сохранёнными строками, когда они свежие и читатель видит их целиком: ни один вход не несёт правило строк, более узкое, чем «всё». [tool-verified: `provisa/mv/view_read.py` module docstring] | Агрегаты над большими таблицами. Вы решаете, какие из них существуют; ни одно не создаётся за вас. См. [Views](configuration.md#views-governed-computed-datasets). |
| Связь с `materialize: true` | Межисточниковая связь получает сохранённое, заранее соединённое представление «родитель — потомок». Запрос, соединение в котором полностью или частично совпадает с шаблоном, переписывается на его чтение, пришёл ли он как SQL или его построил компилятор. [tool-verified: `provisa/api/app_loaders.py` `_load_mv_and_views_config`; `provisa/mv/view_reinject.py` `reinject`; `provisa/pgwire/_pipeline.py` `_view_answering`] | Соединения в хранилище, не строящем индексов соединений. См. [Auto-Materialization](configuration.md#auto-materialization). |
| Размещение | Источники, которые движок может читать напрямую, читаются напрямую; остальные реплицируются. Что окажется в хранилище, зависит от выбранного движка. [tool-verified: `provisa/federation/replica_routing.py`] | Выбор движка определяет большую часть того, что должно вмещать хранилище. |

Материализация связей и индексированные соединения отвечают на одну потребность двумя способами. В столбцовом хранилище или хранилище данных (файл DuckDB, ClickHouse, Snowflake, BigQuery, Databricks, Fabric, Synapse) индексы соединений не строятся, поэтому заранее соединённое представление — способ удешевить частое соединение. В реляционном хранилище столбцы соединений уже проиндексированы, и те же соединения могут выполняться быстро без материализации. [inferred]

От хранилища всё ещё зависит следующее: разовые сканирования и соединения над большими таблицами, которые не покрыты ни одним материализованным представлением и не обслуживаются горячей таблицей. Они выполняются на индексах хранилища в реляционной базе данных или на скорости его сканирования в столбцовой.

Результаты зависят от нагрузки. Измеряйте до и после изменения с помощью [инструмента подбора ресурсов](sizing.md); его [прогоны «напрямую или реплика»](sizing.md#live-or-replica-reads-and-the-ttl) показывают стоимость чтения из реплики в сравнении с прямым чтением в вашем развёртывании.

## Актуальность {: #freshness }

Включите актуальность в решение наравне со скоростью. Насколько старым может быть ответ и во что обходится источнику поддержание его свежести — от этого зависит, читается ли таблица напрямую или из реплики, а значит, и что должно вмещать хранилище.

Корпоративные источники редко поддерживают реальное время. Данные поступают пакетной загрузкой или ночным ETL, потоки изменений отстают, а многие системы не выдерживают прямых чтений при объёме запросов. Начните с актуальности, нужной каждой таблице, затем выберите самую дешёвую стратегию, которая её обеспечивает. Правила на стороне оператора описаны в разделе [Freshness and load: who decides](configuration.md#freshness-and-load-who-decides).

| Стратегия | Задаётся | Что гарантирует | Во что обходится источнику |
| --- | --- | --- | --- |
| Прямое чтение | `replicate: -1` | Актуально на момент запроса | Каждый запрос читает источник |
| TTL | `change_signal: ttl`, `cache_ttl` | Реплика не старше `cache_ttl` | Одно перестроение за `cache_ttl`; при `watermark_column` — только строки после watermark |
| Проба изменений | `change_signal: probe` или `ttl_probe`, `cache_ttl` | Перестраивается только после того, как проба сообщила об изменении; проба выполняется каждые `cache_ttl` | Одна проба за интервал и перестроение только при изменении |
| Поток изменений с отправкой | Источник Kafka или WebSocket; `change_signal: debezium` для источника с блоком `cdc`; `change_signal: native` для PostgreSQL, MongoDB или Cosmos DB | Актуально с точностью до отставания потока и сглаживания (debounce) | Kafka, WebSocket и Debezium: ничего после первого построения, строки несёт сам поток. PostgreSQL, MongoDB и Cosmos DB: перестроение на каждую серию изменений |
| Внешний сигнал | `change_signal: signal` | Перестраивается каждый раз, когда `POST /data/sources/{source_id}/signal` сообщает, что источник изменился; никогда по часам | Одно перестроение на сигнал |
| TTL по ролям | `role_ttl` на таблице | Перечисленная роль принимает реплику не старше её собственного TTL; `cache_ttl` таблицы остаётся нижней границей | Меньше перестроений: чтение терпимой роли никогда их не запрашивает |
| Защита нагрузки | `load_protected: true`, с `cache_ttl`, пробой или `off_peak_window` | Запросы никогда не читают источник; реплику обновляет только исполнитель, а при окне вне пиковых часов только пока оно открыто | Только плановые обновления |
| Шлюз актуальности | `freshness_gate: true` на источнике | Каждое чтение сначала проверяется по `change_signal` и `cache_ttl` источника; вердикт «устарело» обновляет данные перед чтением | Обновление на пути чтения, когда данные устарели |
| Не истекает | `cache_ttl: -1` на источнике или таблице | Загружается один раз; каждое чтение обслуживается из реплики, а чтение ещё не загруженной таблицы отклоняется с указанием её имени | Только эта загрузка: источник, все таблицы которого загружены, больше не подключается, не опрашивается и не читается, и учётные данные ему не нужны |

[tool-verified: `provisa/core/replicate.py`; `provisa/federation/replica_builds.py` `_next_refresh_at`, `_probe_decides`; `provisa/federation/query_residency.py` `freshness_verdict`, `is_stale_of`; `provisa/federation/role_ttl.py` `effective_ttl`; `provisa/freshness/source_gate.py` `gate_source`; `provisa/events/push_wiring.py` `wire_push_listeners`, `_run_change_stream`]

Проба, которой нечего сравнивать (нет watermark, контрольного значения или запроса пробы), вырождается в TTL. [tool-verified: `provisa/federation/replica_builds.py` `_probe_decides`]

### Почти реальное время из потока изменений {: #near-real-time-from-a-change-feed }

Когда источник не выдерживает нагрузки живых запросов, но уже публикует свои изменения, поток изменений даёт данные почти в реальном времени, не трогая источник во время запроса. Запросы читают локальную реплику, что быстро и не нагружает источник, а цикл событий применяет изменения по мере их поступления. Отставание данных определяется только потоком — обычно это секунды. [inferred: the lag figure]

Наличия потока недостаточно. Provisa следит за ним только для таблицы, подключённой к нему:

- **Источник Kafka.** Таблица называет свой топик, и каждое изменение применяется к реплике по первичному ключу, включая удаления. [tool-verified: `provisa/events/push_wiring.py` `_build_provider`, `_run_listener`; `provisa/subscriptions/cdc_landing.py`]

  ```yaml
  live:
    strategy: kafka
    kafka:
      topic: orders.cdc
  push_debounce_quiet: 0      # seconds of quiet before a batch lands; 0 lands each message
  push_debounce_max_delay: 5  # a batch never waits longer than this
  ```

- **Источник WebSocket.** Его поток применяется так же. [tool-verified: `provisa/events/push_wiring.py` `_build_provider`]
- **Таблица с `change_signal: debezium`** (у таблицы или её источника) на источнике, чей блок `cdc` задаёт транспорт Kafka и префикс топика коннектора. Первое чтение строит реплику; затем каждое событие изменения в топике `{topic_prefix}.{schema}.{table}` применяется по первичному ключу, включая удаления, а смещение фиксируется только после того, как изменение загружено. [tool-verified: `provisa/events/push_wiring.py` `_captured_by_cdc`, `_cdc_provider`; `provisa/subscriptions/debezium_provider.py` `ack`]
- **Таблица такого источника с `change_signal: kafka`**. Её изменения читаются из топика дельт, указанного в её `live.kafka.topic`, через тот же транспорт и применяются так же; таблица с этим сигналом, не указывающая топик, отклоняется по имени при сохранении таблицы или её источника и при загрузке конфигурации. [tool-verified: `provisa/events/push_wiring.py` `_cdc_provider`; `provisa/core/change_signal.py` `kafka_topic_refusal`]
- **Таблица MongoDB или Cosmos DB с `change_signal: native`** (на таблице или на её источнике). Каждая серия изменений в потоке изменений запрашивает перестроение реплики. [tool-verified: `provisa/events/push_wiring.py` `_change_feed_opted_in`, `_run_change_stream`]
- **Таблица PostgreSQL с `change_signal: native`** (у таблицы или её источника). Provisa читает логическую репликацию базы данных из собственного слота `provisa_<source id>` через публикацию с тем же именем, в которой перечислены нативные таблицы источника. Kafka не участвует. Каждая серия изменений в таблице запрашивает перестроение её реплики. Базе данных нужно то же, что и Debezium: `wal_level` со значением `logical`, роль с правом репликации, которой разрешено создать публикацию, и основной сервер, а не реплика для чтения; источник, которому чего-то из этого не хватает, отклоняется по имени при сохранении и при загрузке конфигурации. Слот и публикация создаются, когда за ними начинает следить первая таблица, и удаляются, когда последняя перестаёт или источник удаляется. При запуске сервер проверяет каждый зарегистрированный источник PostgreSQL и удаляет слот Provisa, за которым не следит ни одна нативная таблица, так что слот, оставшийся после изменения конфигурации, сделанного при остановленных серверах, больше не удерживает WAL источника. [tool-verified: `provisa/subscriptions/pg_logical.py`; `provisa/events/push_wiring.py` `_run_logical_stream`, `_drop_unfollowed_slots`]

Таблице с отправляемыми изменениями нужен объявленный первичный ключ; таблица без него пропускается и записывается в журнал. [tool-verified: `provisa/events/push_wiring.py` `wire_push_listeners`]

Таблица с сигналом изменений типа push считается свежей на пути чтения, потому что её поток и есть её проверка актуальности. [tool-verified: `provisa/federation/query_residency.py` `freshness_verdict`] Поэтому сигнал push, который ничто не получит, отклоняется: `change_signal: native` для любого типа, кроме Kafka, WebSocket, ingest, PostgreSQL, MongoDB и Cosmos DB, отклоняется при сохранении источника или таблицы и при загрузке конфигурации, как и `debezium` для источника без блока `cdc`. [tool-verified: `provisa/core/change_signal.py` `native_signal_refusal`; `provisa/core/config_loader.py` `_validate_change_signal`; `provisa/api/admin/_change_feed.py` `table_change_feed_refusal`]
