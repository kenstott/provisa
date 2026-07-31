# Commands

command הוא פונקציה רשומה ומנוהלת המביאה חישוב חיצוני תחת מערכת הממשל, הביקורת (audit), והמוצא (lineage) של Provisa. במקום שמנוע הפדרציה מטפל ב-SQL באופן ילידי, command הוא התפר עבור חישוב שהוא אינו יכול לבטא: מיקרו-שירות העשרה, מודל Python, סקריפט shell, פרוצדורה מאוחסנת ילידית של מסד נתונים. רשמו אותו פעם אחת; כל surface לקוח — GraphQL,
SQL‏ pgwire, REST, Arrow Flight, gRPC, Bolt/Cypher — יכול להפעיל אותו עם ממשל זהה
(REQ-885, REQ-1156). [tool-verified: function_dispatch.py module docstring + REQ-885 in requirements.md]

ההבחנה המרכזית: command הוא **RPC מנוהל**, לא ETL אד-הוק. הקלטים והפלטים שלו
מוצהרים, מוקלדים (typed), מאומתים, נעקבים (traced), ומחווטים לתוך lineage. קריאת curl או subprocess לא-מנוהלים
אינם אף אחד מאלה.

## סוגי מימוש (Implementation kinds)

חמישה ערכי `impl_kind` נתמכים [tool-verified: `_EXECUTORS` dict in function_dispatch.py:420-426]:

| `impl_kind` | תעבורה |
|---|---|
| `source_procedure` | פרוצדורה מאוחסנת ילידית על מקור רשום |
| `script` | subprocess מקומי המוזן JSON על stdin, קורא JSON מ-stdout |
| `http` | נקודת קצה HTTP/S; גוף בקשה JSON, תגובת JSON |
| `grpc` | gRPC unary; גשר JSON חסר-proto |
| `python` | callable‏ Python בתוך-תהליך (`module:attr`) |

הכתובת (ה-`name` בקטלוג ו-`function_name`) מנותקת מ-`binding` (תעבורה ו-
מיקום). החליפו את ה-binding וממשל ה-command, ה-lineage, וחוזי הקורא נשארים ללא שינוי. [tool-verified: Function model in models.py:710-750]

## סוגי ארגומנטים (Argument kinds)

כל ארגומנט מצהיר `arg_kind` [tool-verified: FunctionArgument.arg_kind in models.py:691-700]:

| `arg_kind` | התנהגות |
|---|---|
| `column_value` | סקלרי; מועבר ישירות ב-payload הבקשה |
| `table_ref` | עצל (lazy); Provisa מעבירה את הפניית ה-relation כפי-שהיא; השירות שולף את הנתונים |
| `result_set` | חמדני (eager); Provisa ממשת את ה-relation המופנה ושולחת את שורותיו |

command-ים מסוג `http` ו-`grpc` **חייבים** להצהיר לפחות ארגומנט אחד מסוג `table_ref` או `result_set`.
command חיצוני המקבל רק ארגומנטים סקלריים היה מופעל פעם אחת לכל שורה, מה שמסכל
batching. ה-dispatcher דוחה תצורה זו בזמן הקריאה (422). [tool-verified:
`_reject_rowwise_external` in function_dispatch.py:322-344]

command המחזיר סט (מוצהר דרך `output_columns` ו-`return_schema`) הוא
פונקציה table-valued. השתמשו בו במשפט `FROM` או ב-`JOIN`. [inferred from models.py:744-748
and command_localize.py:52-63]

## חוזה מערך הנתונים (REQ-1159)

כל ארגומנט מסוג `table_ref` או `result_set` יכול להצהיר **חוזה עמודות קלט**: רשימה מסודרת,
מוקלדת-IR, של עמודות ב-`FunctionArgument.columns`. ה-command עצמו מצהיר
**חוזה עמודות פלט** ב-`Function.output_columns`. [tool-verified: DatasetColumn model in
models.py:675-683, Function.output_columns in models.py:748]

שני החוזים מאומתים fail-loud בכל הפעלה:

- **קלט (‏result_set בלבד):** לאחר מימוש, Provisa מאמתת את השורות מול העמודות
  המוצהרות. שדות נוספים, שדות חסרים, וטיפוסים שגויים כולם מעלים HTTP 422.
  [tool-verified: `_validate_against` called in `_prepare_args` at function_dispatch.py:243-248]
- **פלט:** שורות המוחזרות על ידי ה-command מאומתות מול `output_columns` לפני שהן
  מגיעות לקורא. [tool-verified: function_dispatch.py:488-490]
- **הקרנה צרה (Narrow projection):** כאשר חוזה קלט מוצהר, שאילתת המימוש מקרינה
  **רק את העמודות הללו** (`SELECT "id", "region" FROM ...`) במקום `SELECT *`.
  [tool-verified: `_materialize_relation` at function_dispatch.py:155-177, col_names passed
  to projection at line 171]

### אוצר המילים של טיפוסי IR

טיפוסי עמודות בחוזה משתמשים במערכת טיפוסי ה-IR הקנונית (REQ-846), לא בסקלרי GraphQL או
באיות ילידי-מקור. השמות התקפים הם [tool-verified: `_IR_TO_SA` keys in ir_types.py:45-63]:

`smallint` `integer` `bigint` `text` `boolean` `float` `double` `numeric`
`date` `timestamp` `time` `uuid` `bytea` `json`

כינויים (aliases) נפוצים נפתרים אוטומטית (`varchar` → `text`, `int4` → `integer`, `jsonb` → `json`,
וכו'). [tool-verified: `_ALIASES` dict in ir_types.py:67-90]

`return_schema` הוא **הקרנת ה-GraphQL** של `output_columns`, לא מקור האמת.
הצהירו `output_columns` עבור אימות ו-lineage; הוסיפו `return_schema` עבור יצירת
טיפוס GraphQL. [tool-verified: models.py:744-748, comment "return_schema is its GraphQL projection"]

## כתיבת command

### קובץ תצורה

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

הוריאנט של gRPC (‏`enrich_grpc_set`) עוקב אחר אותו דפוס אך מציין `impl_kind: grpc`
ו-`binding` עם מפתחות `target` ו-`method` במקום `callable`:

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

טופס ה-command תחת **Settings → Commands** כולל עורך עמודות-קלט לכל-מערך-נתונים (שורה אחת
לכל עמודה מוצהרת, עם בורר טיפוס IR) ועורך עמודות-פלט. שמרו את הטופס כדי
לרשום או לעדכן את ה-command ללא טעינה מחדש של תצורה. [inferred from CommandFormFields.tsx]

## הרכבה inline (REQ-1159)

Commands יכולים להופיע **בתוך** משפט SQL גדול יותר — מצורפים, בתת-שאילתה, או מוקרנים. אינכם
מוגבלים ל-`SELECT * FROM fn(args)`.

```sql
-- Enrich the orders relation and join the result back inline.
SELECT o.id, o.amount, e.score, e.region_label
FROM   orders o
JOIN   enrich_orders('main.public.orders') e ON o.id = e.id
WHERE  e.score > 0.8;
```

לפני שממשל, אימות, או ניתוב רצים, הצינור מזהה קריאות command רשומות,
מבצע כל אחת דרך ה-executor המנוהל המשותף (כך שחוזה ה-I/O ומודל הזהות חלים
בדיוק כמו עבור קריאה ישירה), וכותב מחדש את אתר הקריאה ל-relation מקומי מוקלד.
[tool-verified: `_localize_inline_commands` in _pipeline.py:145-163 and localize_commands in
command_localize.py:178-222]

ההחלפה (substitution) מתאימה-גודל (size-adaptive): עד 1,000 שורות התוצאה מוטמעת inline כרשימת `VALUES` מוקלדת;
מעבר לסף זה היא נרשמת כ-relation מקומי בעל-שם במנוע.
[tool-verified: `_DEFAULT_VALUES_MAX_ROWS = 1000` in command_localize.py:49, path at lines 211-216]

משפט מלוקלז (localized) מנותב כרגיל. שאילתות מקור-יחיד נשארות על המקור; רק שאילתות שהן באמת
חוצות-מקורות הולכות למנוע הפדרציה. [tool-verified: _pipeline.py:304 comment
"REQ-1159: a localized statement carries an inline local relation..."]

## Commands ו-Lineage

מכיוון שכל command מצהיר את עמודות הקלט והפלט שלו, lineage ברמת-עמודה **נסגר על פני
גבול ה-command האטום**. מנוע ה-lineage מחיל closure של taint: כל עמודת פלט מוצהרת
נגזרת מכל עמודת קלט מוצהרת. [tool-verified: `_splice_commands` in graph.py:223-242]

**ההשלכה בת-הפעולה:** רוחב חוזה הקלט שלכם קובע את הדיוק של אותו
closure. קלט צר — רק העמודות שה-command באמת צריך — מייצר קונוס lineage
הדוק וקריא. הצהרת כל עמודה ב-relation המקור מתפזרת (fans in) באופן רחב על פני כל
פלט, מה שעדיין תקין (אין lineage שאובד) אך מטשטש את יכולת-המעקב.

**כלל אצבע:** העבירו את ההקרנה המינימלית שה-command צריך, והחזירו רק עמודות נגזרות
(לא קלטים המוחזרים ללא שינוי). זה שומר על קונוס ה-taint מדויק. [inferred from
_splice_commands behavior in graph.py and _materialize_relation narrow-projection in function_dispatch.py:161]

ראו [Lineage](lineage.md) לגבי איך צמתי command מופיעים ב-DAG וכיצד לקרוא אותם.

## רשימת אישור Egress (Egress allowlist)

commands מסוג `http` ו-`grpc` קוראים לנקודות קצה חיצוניות. כל host יעד חייב להופיע ב-
`udf_egress_allowlist` של הפריסה. Loopback (‏`localhost`, `127.0.0.1`, `::1`) תמיד
מותר. רשימת אישור חסרה דוחה את כל ה-egress החיצוני עם HTTP 403 — אין ברירת מחדל
שקטה. [tool-verified: `_check_egress` in function_dispatch.py:292-311]

## מעקב הפעלה (Invocation tracing) (REQ-886)

כל הפעלה פולטת trace ללא קשר לתוצאה. ה-trace כולל את שם ה-command, סוג
התעבורה, מודל הזהות (DEFINER או INVOKER), הפניות relation קלט, מזהה תפקיד, ו-
עוצמת פלט (cardinality). ה-dispatcher פולט את ה-trace — אף `impl_kind` אינו יכול לעקוף אותו.
[tool-verified: `udf_invocation_trace` context in dispatch_function:475-492]
