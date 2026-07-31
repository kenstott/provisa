# Команды

Команда — это зарегистрированная, управляемая функция, которая приводит внешние вычисления под систему
governance, аудита и происхождения (lineage) Provisa. Там, где движок федерации обрабатывает SQL нативно, команда
является швом для вычислений, которые он не может выразить: сервис обогащения, модель Python, shell-скрипт,
нативная хранимая процедура базы данных. Зарегистрируйте её один раз; каждая клиентская поверхность — GraphQL,
pgwire SQL, REST, Arrow Flight, gRPC, Bolt/Cypher — может вызывать её с идентичным governance
(REQ-885, REQ-1156). [tool-verified: function_dispatch.py module docstring + REQ-885 in requirements.md]

Ключевое отличие: команда — это **управляемый RPC**, а не разовый ETL. Её входы и выходы
объявлены, типизированы, проверены, трассируются и вплетены в происхождение (lineage). Неуправляемый вызов curl или
subprocess — ничего из этого не имеет.

## Виды реализации

Поддерживаются пять значений `impl_kind` [tool-verified: `_EXECUTORS` dict in function_dispatch.py:420-426]:

| `impl_kind` | Транспорт |
|---|---|
| `source_procedure` | Нативная хранимая процедура на зарегистрированном источнике |
| `script` | Локальный subprocess, получающий JSON на stdin, читает JSON из stdout |
| `http` | Эндпоинт HTTP/S; тело запроса JSON, ответ JSON |
| `grpc` | Унарный gRPC; мост JSON без proto |
| `python` | Вызываемый объект Python в процессе (`module:attr`) |

Адресация (каталожное `name` и `function_name`) отделена от `binding` (транспорт и
местоположение). Замена binding не изменяет governance, происхождение (lineage) и контракты вызывающей стороны
команды. [tool-verified: Function model in models.py:710-750]

## Виды аргументов

Каждый аргумент объявляет `arg_kind` [tool-verified: FunctionArgument.arg_kind in models.py:691-700]:

| `arg_kind` | Поведение |
|---|---|
| `column_value` | Скаляр; передаётся напрямую в полезной нагрузке запроса |
| `table_ref` | Ленивый; Provisa передаёт ссылку на отношение как есть; сервис сам получает данные |
| `result_set` | Активный; Provisa материализует ссылаемое отношение и отправляет его строки |

Команды `http` и `grpc` **обязаны** объявлять хотя бы один аргумент `table_ref` или `result_set`.
Внешняя команда, получающая только скалярные аргументы, вызывалась бы по разу на строку, что сводит на нет
батчинг. Диспетчер отклоняет такую конфигурацию во время вызова (422). [tool-verified:
`_reject_rowwise_external` in function_dispatch.py:322-344]

Команда, возвращающая набор (объявленный через `output_columns` и `return_schema`), является
табличной функцией. Используйте её в предложении `FROM` или `JOIN`. [inferred from models.py:744-748
and command_localize.py:52-63]

## Контракт набора данных (REQ-1159)

Каждый аргумент `table_ref` или `result_set` может объявлять **контракт входных столбцов**: упорядоченный,
типизированный по IR список столбцов в `FunctionArgument.columns`. Сама команда объявляет
**контракт выходных столбцов** в `Function.output_columns`. [tool-verified: DatasetColumn model in
models.py:675-683, Function.output_columns in models.py:748]

Оба контракта проверяются с явным отказом при каждом вызове:

- **Вход (только result_set):** после материализации Provisa проверяет строки на соответствие
  объявленным столбцам. Лишние поля, отсутствующие поля и неверные типы вызывают HTTP 422.
  [tool-verified: `_validate_against` called in `_prepare_args` at function_dispatch.py:243-248]
- **Выход:** строки, возвращённые командой, проверяются на соответствие `output_columns` до того, как они
  достигнут вызывающей стороны. [tool-verified: function_dispatch.py:488-490]
- **Узкая проекция:** когда объявлен входной контракт, запрос материализации проецирует
  **только эти столбцы** (`SELECT "id", "region" FROM ...`), а не `SELECT *`.
  [tool-verified: `_materialize_relation` at function_dispatch.py:155-177, col_names passed
  to projection at line 171]

### Словарь типов IR

Типы столбцов контракта используют каноническую систему типов IR (REQ-846), а не скаляры GraphQL или
нативные для источника обозначения. Допустимые имена [tool-verified: `_IR_TO_SA` keys in ir_types.py:45-63]:

`smallint` `integer` `bigint` `text` `boolean` `float` `double` `numeric`
`date` `timestamp` `time` `uuid` `bytea` `json`

Распространённые псевдонимы разрешаются автоматически (`varchar` → `text`, `int4` → `integer`, `jsonb` → `json`
и т. д.). [tool-verified: `_ALIASES` dict in ir_types.py:67-90]

`return_schema` — это **проекция GraphQL** для `output_columns`, а не источник истины.
Объявляйте `output_columns` для валидации и происхождения (lineage); добавляйте `return_schema` для генерации
типов GraphQL. [tool-verified: models.py:744-748, comment "return_schema is its GraphQL projection"]

## Создание команды

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

Вариант gRPC (`enrich_grpc_set`) следует тому же шаблону, но указывает `impl_kind: grpc`
и `binding` с ключами `target` и `method` вместо `callable`:

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

### Admin UI

Форма команды в **Settings → Commands** включает редактор входных столбцов для каждого набора данных (одна строка
на объявленный столбец с селектором типа IR) и редактор выходных столбцов. Сохранение формы
регистрирует или обновляет команду без перезагрузки конфигурации. [inferred from CommandFormFields.tsx]

## Встроенная композиция (REQ-1159)

Команды могут появляться **внутри** более крупного оператора SQL — соединёнными, в подзапросе или проецируемыми. Вы
не ограничены `SELECT * FROM fn(args)`.

```sql
-- Enrich the orders relation and join the result back inline.
SELECT o.id, o.amount, e.score, e.region_label
FROM   orders o
JOIN   enrich_orders('main.public.orders') e ON o.id = e.id
WHERE  e.score > 0.8;
```

Перед запуском governance, валидации или маршрутизации конвейер обнаруживает зарегистрированные вызовы команд,
выполняет каждый через общий управляемый исполнитель (так что контракт ввода-вывода и модель идентичности применяются
точно так же, как при прямом вызове), и переписывает место вызова в типизированное локальное отношение.
[tool-verified: `_localize_inline_commands` in _pipeline.py:145-163 and localize_commands in
command_localize.py:178-222]

Подстановка адаптивна по размеру: до 1000 строк результат встраивается как типизированный список `VALUES`;
выше этого порога он регистрируется как именованное локальное отношение в движке.
[tool-verified: `_DEFAULT_VALUES_MAX_ROWS = 1000` in command_localize.py:49, path at lines 211-216]

Локализованный оператор маршрутизируется как обычно. Запросы к одному источнику остаются на источнике; только по-настоящему
межисточниковые запросы идут в движок федерации. [tool-verified: _pipeline.py:304 comment
"REQ-1159: a localized statement carries an inline local relation..."]

## Команды и происхождение (lineage)

Поскольку каждая команда объявляет свои входные и выходные столбцы, происхождение на уровне столбцов **замыкается через
непрозрачную границу команды**. Движок происхождения применяет замыкание заражения (taint closure): каждый объявленный выходной
столбец происходит от каждого объявленного входного столбца. [tool-verified: `_splice_commands` in graph.py:223-242]

**Практическое следствие:** ширина вашего входного контракта определяет точность этого
замыкания. Узкий вход — только столбцы, которые команде действительно нужны — производит компактный,
читаемый конус происхождения. Объявление каждого столбца в исходном отношении широко разветвляется по каждому
выходу, что всё ещё корректно (происхождение не теряется), но размывает прослеживаемость.

**Правило: передавайте минимальную проекцию, необходимую команде, и возвращайте только производные столбцы**
(а не отражённые без изменений входы). Это сохраняет конус заражения точным. [inferred from
_splice_commands behavior in graph.py and _materialize_relation narrow-projection in function_dispatch.py:161]

О том, как узлы команд появляются в DAG и как их читать, см. [Происхождение](lineage.md).

## Allowlist исходящего трафика

Команды `http` и `grpc` вызывают внешние эндпоинты. Каждый целевой хост должен присутствовать в
`udf_egress_allowlist` развёртывания. Loopback (`localhost`, `127.0.0.1`, `::1`) всегда
разрешён. Отсутствующий allowlist запрещает весь внешний исходящий трафик с HTTP 403 — молчаливого
значения по умолчанию не существует. [tool-verified: `_check_egress` in function_dispatch.py:292-311]

## Трассировка вызовов (REQ-886)

Каждый вызов выдаёт трассировку независимо от результата. Трассировка включает имя команды,
вид транспорта, модель идентичности (DEFINER или INVOKER), ссылки на входные отношения, id роли и
выходную мощность (cardinality). Диспетчер выдаёт трассировку — никакой `impl_kind` не может её обойти.
[tool-verified: `udf_invocation_trace` context in dispatch_function:475-492]
