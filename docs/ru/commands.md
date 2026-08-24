# Команды

Команда — это зарегистрированная управляемая функция, которая вводит внешние вычисления в систему
управления, аудита и происхождения данных Provisa. Там, где движок федерации обрабатывает SQL
нативно, команда — это шов для вычислений, которые он выразить не может: микросервис обогащения,
модель на Python, shell-скрипт, нативная хранимая процедура базы данных. Зарегистрируйте её один
раз; любая клиентская поверхность — GraphQL, pgwire SQL, REST, Arrow Flight, gRPC, Bolt/Cypher —
сможет вызвать её с одним и тем же управлением (REQ-885, REQ-1156). [tool-verified:
function_dispatch.py module docstring + REQ-885 in requirements.md]

Ключевое различие: команда — это **управляемый RPC**, а не ad-hoc ETL. Её входы и выходы объявлены,
типизированы, валидированы, трассируются и подключены к происхождению данных. Неуправляемый вызов
curl или подпроцесс не обладают ничем из этого.

## Виды реализации

Поддерживаются пять значений `impl_kind` [tool-verified: `_EXECUTORS` dict in
function_dispatch.py:420-426]:

| `impl_kind` | Транспорт |
| --- | --- |
| `source_procedure` | Нативная хранимая процедура на зарегистрированном источнике |
| `script` | Локальный подпроцесс, получающий JSON на stdin и читающий JSON из stdout |
| `http` | HTTP/S-эндпоинт; тело запроса JSON, ответ JSON |
| `grpc` | Унарный gRPC; JSON-мост без proto |
| `python` | Вызываемый объект Python в процессе (`module:attr`) |

Адресация (`name` каталога и `function_name`) отделена от `binding` (транспорт и расположение).
Замените привязку — и управление командой, её происхождение данных и контракты вызывающих сторон
останутся неизменными. [tool-verified: Function model in models.py:710-750]

## Виды аргументов

Каждый аргумент объявляет `arg_kind` [tool-verified: FunctionArgument.arg_kind in
models.py:691-700]:

| `arg_kind` | Поведение |
| --- | --- |
| `column_value` | Скаляр; передаётся напрямую в полезной нагрузке запроса |
| `table_ref` | Ленивый; Provisa передаёт ссылку на отношение как есть, данные забирает сама служба |
| `result_set` | Энергичный; Provisa материализует указанное отношение и отправляет его строки |

Команды `http` и `grpc` **обязаны** объявить хотя бы один аргумент `table_ref` или `result_set`.
Внешняя команда, получающая только скалярные аргументы, вызывалась бы по разу на строку, что сводит
на нет пакетирование. Диспетчер отклоняет такую конфигурацию во время вызова (422). [tool-verified:
`_reject_rowwise_external` in function_dispatch.py:322-344]

Команда, возвращающая множество (объявленное через `output_columns` и `return_schema`), — это
табличная функция. Используйте её в предложении `FROM` или в `JOIN`. [inferred from
models.py:744-748 and command_localize.py:52-63]

## Контракт набора данных (REQ-1159)

Каждый аргумент `table_ref` или `result_set` может объявить **входной контракт колонок**:
упорядоченный, типизированный в IR список колонок в `FunctionArgument.columns`. Сама команда
объявляет **выходной контракт колонок** в `Function.output_columns`. [tool-verified: DatasetColumn
model in models.py:675-683, Function.output_columns in models.py:748]

Оба контракта валидируются с явным отказом при каждом вызове:

- **Вход (только result_set):** после материализации Provisa валидирует строки по объявленным
  колонкам. Лишние поля, отсутствующие поля и неверные типы — всё вызывает HTTP 422.
  [tool-verified: `_validate_against` called in `_prepare_args` at function_dispatch.py:243-248]
- **Выход:** строки, возвращённые командой, валидируются по `output_columns` до того, как дойдут
  до вызывающей стороны. [tool-verified: function_dispatch.py:488-490]
- **Узкая проекция:** когда входной контракт объявлен, запрос материализации проецирует
  **только эти колонки** (`SELECT "id", "region" FROM ...`), а не `SELECT *`.
  [tool-verified: `_materialize_relation` at function_dispatch.py:155-177, col_names passed
  to projection at line 171]

### Словарь типов IR

Типы колонок контракта используют каноническую систему типов IR (REQ-846), а не скаляры GraphQL и
не написания, нативные для источника. Допустимые имена [tool-verified: `_IR_TO_SA` keys in
ir_types.py:45-63]:

`smallint` `integer` `bigint` `text` `boolean` `float` `double` `numeric`
`date` `timestamp` `time` `uuid` `bytea` `json`

Распространённые псевдонимы разрешаются автоматически (`varchar` → `text`, `int4` → `integer`,
`jsonb` → `json` и так далее). [tool-verified: `_ALIASES` dict in ir_types.py:67-90]

`return_schema` — это **проекция в GraphQL** для `output_columns`, а не источник истины. Объявляйте
`output_columns` для валидации и происхождения данных; добавляйте `return_schema` для генерации
типов GraphQL. [tool-verified: models.py:744-748, comment "return_schema is its GraphQL projection"]

## Написание команды

### Файл конфигурации

```yaml
functions:
  - name: enrich_orders
    description: Enrich orders inline — deterministic score + region label
    domain_id: sales-analytics
    kind: query
    impl_kind: python
    source_id: ""
    function_name: enrich_orders
    returns: ""
    binding:
      callable: demo.py_functions:enrich_orders
    arguments:
      - name: input
        type: String
        arg_kind: result_set
        columns:
          - {name: id, type: integer}   # narrow input contract
          - {name: region, type: text}
    visible_to: [admin]
    output_columns:
      - {name: id, type: integer}
      - {name: score, type: double}
      - {name: region_label, type: text}
    return_schema:
      type: array
      items:
        type: object
        properties:
          id: {type: integer}
          score: {type: number}
          region_label: {type: string}
```

[tool-verified: sample_config.yaml enrich_orders block]

Вариант на gRPC (`enrich_grpc_set`) следует тому же образцу, но задаёт `impl_kind: grpc` и
`binding` с ключами `target` и `method` вместо `callable`:

```yaml
  - name: enrich_grpc_set
    impl_kind: grpc
    binding:
      target: ${env:DEMO_GRPC_TARGET:-localhost:50071}
      method: /provisa.demo.Enrich/EnrichRows
    arguments:
      - name: input
        type: String
        arg_kind: result_set
        columns:
          - {name: id, type: integer}
          - {name: region, type: text}
    output_columns:
      - {name: id, type: integer}
      - {name: embedding, type: text}
      - {name: geo, type: text}
```

[tool-verified: config/provisa.yaml enrich_grpc_set block]

### Админский UI

Форма команды в **Settings → Commands** включает редактор входных колонок для каждого набора данных
(одна строка на объявленную колонку, с селектором IR-типа) и редактор выходных колонок. Сохраните
форму, чтобы зарегистрировать или обновить команду без перезагрузки конфигурации. [inferred from
CommandFormFields.tsx]

## Встроенная композиция (REQ-1159)

Команды могут появляться **внутри** более крупного SQL-выражения — в join, в подзапросе или в
проекции. Вы не ограничены формой `SELECT * FROM fn(args)`.

```sql
-- Enrich the orders relation and join the result back inline.
SELECT o.id, o.amount, e.score, e.region_label
FROM   orders o
JOIN   enrich_orders('main.public.orders') e ON o.id = e.id
WHERE  e.score > 0.8;
```

До того как отработают управление, валидация или маршрутизация, конвейер обнаруживает вызовы
зарегистрированных команд, выполняет каждый через общий управляемый исполнитель (так что контракт
ввода-вывода и модель личности применяются ровно так же, как при прямом вызове) и переписывает место
вызова в типизированное локальное отношение. [tool-verified: `_localize_inline_commands` in
_pipeline.py:145-163 and localize_commands in command_localize.py:178-222]

Подстановка адаптируется к размеру: до 1 000 строк результат встраивается как типизированный список
`VALUES`; выше этого порога он регистрируется в движке как именованное локальное отношение.
[tool-verified: `_DEFAULT_VALUES_MAX_ROWS = 1000` in command_localize.py:49, path at lines 211-216]

Локализованное выражение маршрутизируется обычным образом. Запросы к одному источнику остаются на
источнике; в движок федерации уходят только по-настоящему межисточниковые запросы. [tool-verified:
_pipeline.py:304 comment "REQ-1159: a localized statement carries an inline local relation..."]

## Команды и происхождение данных

Поскольку каждая команда объявляет свои входные и выходные колонки, происхождение на уровне колонок
**замыкается через непрозрачную границу команды**. Движок происхождения применяет taint-замыкание:
каждая объявленная выходная колонка выводится из каждой объявленной входной колонки. [tool-verified:
`_splice_commands` in graph.py:223-242]

**Практическое следствие:** ширина вашего входного контракта определяет точность этого замыкания.
Узкий вход — только те колонки, которые команде действительно нужны, — даёт плотный, читаемый конус
происхождения. Объявление каждой колонки исходного отношения широко сводит их во все выходы; это
по-прежнему корректно (ничего из происхождения не теряется), но размывает прослеживаемость.

**Практическое правило:** передавайте минимальную проекцию, нужную команде, и возвращайте только
производные колонки (а не входы, прокинутые без изменений). Так конус taint остаётся точным.
[inferred from _splice_commands behavior in graph.py and _materialize_relation narrow-projection in
function_dispatch.py:161]

О том, как узлы команд выглядят в DAG и как их читать, см. [Происхождение данных](lineage.md).

## Разрешительный список исходящего трафика

Команды `http` и `grpc` вызывают внешние эндпоинты. Каждый целевой хост должен присутствовать в
`udf_egress_allowlist` развёртывания. Петлевой адрес (`localhost`, `127.0.0.1`, `::1`) разрешён
всегда. Отсутствующий разрешительный список запрещает весь внешний исходящий трафик с HTTP 403 —
никакого молчаливого значения по умолчанию нет. [tool-verified: `_check_egress` in
function_dispatch.py:292-311]

## Трассировка вызовов (REQ-886)

Каждый вызов порождает трассу независимо от исхода. Трасса включает имя команды, вид транспорта,
модель личности (DEFINER или INVOKER), ссылки на входные отношения, идентификатор роли и мощность
выхода. Трассу порождает диспетчер — обойти её не может ни один `impl_kind`. [tool-verified:
`udf_invocation_trace` context in dispatch_function:475-492]

## CLI: provisa metadata export

`provisa metadata export` — это задача уровня оболочки, а не управляемый RPC. Она запускает
публикацию метаданных по требованию у работающего сервера (REQ-1072/REQ-1074), отправляя POST на
`/admin/metadata-export/publish` — тот же эндпоинт, который вызывает кнопка **Publish now** во
вкладке Admin. [tool-verified: `_cmd_metadata_export` in provisa/cli.py:272-310]

Используйте её, чтобы запускать экспорты по расписанию из cron или CI, когда настроенного
расписания `reconcile_cron` недостаточно по гранулярности:

```bash
provisa metadata export --api https://acme.provisa.org --token "$PROVISA_API_TOKEN"
```

Код выхода 0 = полная публикация. Код выхода 1 = частичная публикация или сбой подключения.

Полный справочник флагов, варианты аутентификации, именование хостов при мультиарендности и пример
для cron см. в [Экспорт метаданных — из командной строки](metadata-export.md#from-the-command-line).


Команды присутствуют в git-проекции каждого окружения. О том, как команда и назначения её тегов
переживают слияние и pull, см. [Окружения](environments.md).
