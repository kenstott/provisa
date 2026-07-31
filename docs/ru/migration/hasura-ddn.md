# Миграция с Hasura DDN (v3) на Provisa

## Предварительные требования

1. Проект Hasura DDN с файлами HML (расширение `.hml`).
   Проекты DDN обычно имеют структуру каталогов вида:

   ```text
   my-ddn-project/
     app/
       subgraph1/
         models/
           MyModel.hml
         commands/
           MyCommand.hml
       subgraph2/
         ...
     globals/
       ...
   ```

2. Python 3.11+ с установленным пакетом `provisa`.

## Использование CLI

```bash
python -m provisa.ddn <hml-dir> -o provisa.yaml
```

### Аргументы

| Аргумент | Обязателен | Описание |
| ---------- | ---------- | ------------- |
| `hml_dir` | Да | Путь к каталогу проекта DDN HML (рекурсивно сканируется на предмет файлов `.hml`) |

### Опции

| Опция | По умолчанию | Описание |
| -------- | --------- | ------------- |
| `-o, --output FILE` | stdout | Путь к выходному файлу YAML |
| `--source-overrides FILE` | None | Файл YAML с переопределениями подключения по источникам |
| `--domain-map KEY=VAL ...` | None | Отображения подграф-в-домен (например, `app=core analytics=reporting`) |
| `--dry-run` | off | Разбор и валидация без записи вывода |

### Файл переопределений источников

Файл YAML, ключами которого являются имена коннекторов (после санитизации ID: пробелы, точки, слэши
становятся подчёркиваниями), со свойствами подключения:

```yaml
my_pg_connector:
  host: prod-db.example.com
  port: 5432
  database: chinook
  username: provisa_user
  password: "${env:PROD_DB_PASSWORD}"
```

## Матрица паритета функций

| Тип DDN | Эквивалент Provisa | Примечания |
| --- | --- | --- |
| **DataConnectorLink** | `sources[]` | Тип источника выводится из URL коннектора (postgres, mysql, mssql, mongo, clickhouse, snowflake, bigquery). Детали подключения по умолчанию — заполнители; используйте `--source-overrides` для задания фактических значений. |
| **ObjectType** | Определения столбцов на `tables[]` | Поля становятся столбцами. `dataConnectorTypeMapping.fieldMapping` разрешает имена полей GraphQL в имена физических столбцов. |
| **Model** | `tables[]` | Каждая Model производит одну таблицу. `source_id` из коннектора, `table_name` из коллекции. `graphql_type_name` становится `alias`. Подграф (и, следовательно, `domain_id`) выводится из каталога файла: первый компонент каталога под корнем проекта. |
| **Relationship** | `relationships[]` | Тип object -> `many-to-one`, тип array -> `one-to-many`. Отображение полей разрешается через поиск физических столбцов. |
| **TypePermissions** | `columns[].visible_to[]` | `allowedFields` определяет, какие роли могут видеть каждый столбец. |
| **ModelPermissions** | `rls_rules[]` | Предикаты фильтра преобразуются в предложения SQL WHERE. Поддерживаются `_eq`, `_neq`, `_gt`, `_lt`, `_gte`, `_lte`, `_in`, `_nin`, `_like`, `_is_null`, `_and`, `_or`, `_not`. Ссылки на переменные сессии сохраняются как `${x-hasura-...}`. |
| **Command** | `functions[]` | Отображаются как функции, так и процедуры. Аргументы, тип возврата и имя корневого поля GraphQL сохраняются. `domain_id` устанавливается из подграфа. |
| **AggregateExpression** | Побочный файл `provisa-aggregates.yaml` | Count, count_distinct и агрегатные функции по полям сохраняются в побочном файле и преобразуются в конфигурацию агрегатов Provisa. |
| **BooleanExpressionType** | Пропускается (молча) | Используется внутри DDN для фильтрации; прямого эквивалента в Provisa не требуется. |
| **AuthConfig** | Пропускается (молча) | Конфигурация авторизации DDN не отображается; настройте авторизацию Provisa отдельно. |
| **ScalarType** | Пропускается | Выдаётся предупреждение со счётчиком. |
| **GraphqlConfig** | Пропускается | Выдаётся предупреждение со счётчиком. |
| **CompatibilityConfig** | Пропускается | Выдаётся предупреждение со счётчиком. |
| **Прочие нераспознанные типы (Kinds)** | Пропускаются | Выдаётся предупреждение со счётчиком по каждому типу. |

## Ключевая концепция: разрешение поля GraphQL в физический столбец

DDN отделяет схему GraphQL (имена полей) от физической схемы базы данных
(имена столбцов) через `dataConnectorTypeMapping` на ObjectTypes. Конвертер:

1. Читает записи `fieldMapping` из отображений типов каждого ObjectType.
2. Строит справочник: `{graphql_field_name -> physical_column_name}`.
3. Для полей без явного отображения предполагает, что имя поля равно имени столбца.
4. Использует этот справочник при построении столбцов, связей и выражений фильтра RLS.

Это означает, что выходной `provisa.yaml` использует **имена физических столбцов** для `columns[].name`
и устанавливает `columns[].alias` в имя поля GraphQL, когда они отличаются.

## Шаги после конвертации

1. **Проверьте выходной YAML.** Проверьте источники, таблицы и отображения столбцов.
2. **Настройте подключения источников.** Коннекторы предоставляют только подсказку URL для определения
   типа. Фактические хост/порт/база данных/учётные данные должны быть заданы через
   `--source-overrides` или путём редактирования вывода.
3. **Проверьте назначения доменов.** Имена подграфов выводятся из структуры каталогов
   (первый компонент каталога под корнем проекта). Без `--domain-map` каждое
   имя подграфа напрямую становится ID домена. Используйте `--domain-map` для их переименования.
4. **Проверьте правила RLS.** Предикаты фильтра DDN преобразуются в SQL-приближения.
   Вложенная булева логика (`_and`/`_or`/`_not`) поддерживается, но сложные
   фильтры, проходящие через связи, могут потребовать ручной проверки.
5. **Проверьте конфигурацию агрегатов.** Агрегатные выражения записываются в побочный файл
   `provisa-aggregates.yaml` и преобразуются в конфигурацию агрегатов Provisa.
6. **Просмотрите предупреждения.** Конвертер выводит сводку в stderr, перечисляя пропущенные
   типы (Kinds) DDN и любые модели, ссылающиеся на неизвестные ObjectTypes.
7. **Протестируйте.** Запустите сервер Provisa и проверьте запросы к вашим источникам данных.

## Распространённые проблемы и устранение неполадок

### Определение типа источника не удаётся

URL коннектора используется эвристически (проверка на ключевые слова вроде «postgres»,
«mysql», «mongo»). Если URL не содержит распознаваемого ключевого слова, источник
по умолчанию становится `postgresql`. Переопределите через `--source-overrides`.

### Отсутствует ObjectType для Model

Если Model ссылается на имя ObjectType, которое не найдено ни в одном файле `.hml`,
таблица пропускается и выдаётся предупреждение. Убедитесь, что все файлы HML включены
в сканируемый каталог.

### Обнаружение подграфов

Подграфы выводятся из структуры каталогов: первый компонент каталога
под корнем проекта берётся как имя подграфа. Поле `subgraph` внутри
документов HML не используется. Файлы в каталоге `globals/` присваиваются
подграфу `globals` и исключаются из обнаружения доменов.

### Разрешение источника связи

Связи ссылаются на `source_type` (имя ObjectType) и `target_model` (имя
Model). Если ни одна Model не использует данный ObjectType, связь молча пропускается.

### Алиасы столбцов повсюду

Если ваш проект DDN широко использует `fieldMapping`, ожидайте, что у большинства столбцов будет
`alias` в выводе. Это корректное поведение — `name` является физическим столбцом,
`alias` — это имя GraphQL, которое использовало ваше приложение.

### Агрегатные выражения

Агрегатные выражения сохраняются в побочном файле `provisa-aggregates.yaml`, записываемом
рядом с выводом, и преобразуются в конфигурацию агрегатов Provisa. Они не хранятся в
`description` таблицы.

## Пример: конвертация проекта DDN Chinook

```bash
# Convert the DDN project
python -m provisa.ddn ./chinook-ddn/ \
  -o provisa.yaml \
  --domain-map app=music \
  --source-overrides overrides.yaml

# Dry run to check warnings first
python -m provisa.ddn ./chinook-ddn/ --dry-run
```

Структура вывода:

```yaml
sources:
  - id: chinook_pg
    type: postgresql
    host: prod-db.example.com
    port: 5432
    database: chinook
    ...
domains:
  - id: music
tables:
  - source_id: chinook_pg
    domain_id: music
    schema_name: public
    table_name: Album
    columns:
      - name: AlbumId
        visible_to: [admin, user]
      - name: Title
        visible_to: [admin, user]
      - name: ArtistId
        visible_to: [admin, user]
    alias: Albums
  - source_id: chinook_pg
    domain_id: music
    schema_name: public
    table_name: Artist
    columns:
      - name: artist_id
        visible_to: [admin, user]
        alias: ArtistId
      - name: artist_name
        visible_to: [admin, user]
        alias: Name
    alias: Artists
roles:
  - id: admin
    capabilities: [read]
    domain_access: ["*"]
  - id: user
    capabilities: [read]
    domain_access: ["*"]
relationships:
  - id: chinook_pg.public.Album.Artist
    source_table_id: chinook_pg.public.Album
    target_table_id: chinook_pg.public.Artist
    source_column: ArtistId
    target_column: artist_id
    cardinality: many-to-one
functions:
  - name: GetTopTracks
    source_id: chinook_pg
    schema_name: public
    function_name: get_top_tracks
    returns: Track
    domain_id: music
    description: "DDN function"
```
