# פקודות

פקודה היא פונקציה רשומה וממושלת המכניסה חישוב חיצוני תחת מערכת הממשל, הביקורת
וה-Data Lineage של Provisa. במקום שמנוע הפדרציה מטפל ב-SQL באופן טבעי, פקודה
היא התפר עבור חישוב שאין ביכולתו לבטא: מיקרו-שירות העשרה, מודל Python, סקריפט
מעטפת, פרוצדורה מאוחסנת טבעית של מסד נתונים. רשמו אותה פעם אחת; כל משטח לקוח — GraphQL,
‏pgwire SQL, ‏REST, ‏Arrow Flight, ‏gRPC, ‏Bolt/Cypher — יכול להפעיל אותה עם ממשל זהה
(REQ-885, REQ-1156). [tool-verified: function_dispatch.py module docstring + REQ-885 in requirements.md]

ההבחנה המרכזית: פקודה היא **RPC ממושל**, לא ETL אד-הוק. הקלטים והפלטים שלה
מוצהרים, מוקלדים, מאומתים, נמעקבים ומחווטים אל ה-Data Lineage. קריאת curl או תת-תהליך לא ממושלים
אינם אף אחד מהדברים האלה.

## סוגי מימוש

חמישה ערכי `impl_kind` נתמכים [tool-verified: `_EXECUTORS` dict in function_dispatch.py:420-426]:

| `impl_kind` | תעבורה |
| --- | --- |
| `source_procedure` | פרוצדורה מאוחסנת טבעית על מקור רשום |
| `script` | תת-תהליך מקומי המוזן JSON ב-stdin, קורא JSON מ-stdout |
| `http` | נקודת קצה HTTP/S; ‏גוף בקשה JSON, תגובה JSON |
| `grpc` | ‏gRPC unary; גשר JSON נטול proto |
| `python` | קריאה של Python בתוך התהליך (`module:attr`) |

המיעון (ה-`name` וה-`function_name` בקטלוג) מנותק מה-`binding` (תעבורה
ומיקום). החליפו את ה-binding והממשל, ה-Data Lineage וחוזי הקורא של הפקודה יישארו
ללא שינוי. [tool-verified: Function model in models.py:710-750]

## סוגי ארגומנטים

כל ארגומנט מצהיר על `arg_kind` [tool-verified: FunctionArgument.arg_kind in models.py:691-700]:

| `arg_kind` | התנהגות |
| --- | --- |
| `column_value` | סקלר; מועבר ישירות במטען הבקשה |
| `table_ref` | עצל; Provisa מעבירה את הפניית היחס כמות שהיא; השירות מביא את הנתונים |
| `result_set` | להוט; Provisa ממטריאלת את היחס המופנה ושולחת את שורותיו |

פקודות `http` ו-`grpc` **חייבות** להצהיר על ארגומנט `table_ref` או `result_set` אחד לפחות.
פקודה חיצונית המקבלת ארגומנטים סקלריים בלבד הייתה מופעלת פעם אחת לכל שורה, מה שמסכל
איגוד לאצוות. המשגר דוחה תצורה זו בזמן הקריאה (422). [tool-verified:
`_reject_rowwise_external` in function_dispatch.py:322-344]

פקודה המחזירה קבוצה (מוצהרת דרך `output_columns` ו-`return_schema`) היא
פונקציה מוערכת-טבלה. השתמשו בה בסעיף `FROM` או ב-`JOIN`. [inferred from models.py:744-748
and command_localize.py:52-63]

## חוזה ערכת הנתונים (REQ-1159)

כל ארגומנט `table_ref` או `result_set` רשאי להצהיר על **חוזה עמודות קלט**: רשימה מסודרת
של עמודות מוקלדות-IR ב-`FunctionArgument.columns`. הפקודה עצמה מצהירה על
**חוזה עמודות פלט** ב-`Function.output_columns`. [tool-verified: DatasetColumn model in
models.py:675-683, Function.output_columns in models.py:748]

שני החוזים מאומתים fail-loud בכל הפעלה:

- **קלט (result_set בלבד):** לאחר המטריאליזציה, Provisa מאמתת את השורות מול
  העמודות המוצהרות. שדות עודפים, שדות חסרים וסוגים שגויים — כולם מעלים HTTP 422.
  [tool-verified: `_validate_against` called in `_prepare_args` at function_dispatch.py:243-248]
- **פלט:** שורות המוחזרות על ידי הפקודה מאומתות מול `output_columns` לפני שהן
  מגיעות לקורא. [tool-verified: function_dispatch.py:488-490]
- **הטלה צרה:** כשחוזה קלט מוצהר, שאילתת המטריאליזציה מטילה
  **רק את העמודות ההן** (`SELECT "id", "region" FROM ...`) במקום `SELECT *`.
  [tool-verified: `_materialize_relation` at function_dispatch.py:155-177, col_names passed
  to projection at line 171]

### אוצר מילות הסוגים של IR

סוגי עמודות בחוזה משתמשים במערכת סוגי ה-IR הקנונית (REQ-846), לא בסקלרים של GraphQL או
באיותים טבעיים של מקורות. השמות התקפים הם [tool-verified: `_IR_TO_SA` keys in ir_types.py:45-63]:

`smallint` `integer` `bigint` `text` `boolean` `float` `double` `numeric`
`date` `timestamp` `time` `uuid` `bytea` `json`

כינויים נפוצים מתפענחים אוטומטית (`varchar` → `text`, `int4` → `integer`, `jsonb` → `json`,
וכן הלאה). [tool-verified: `_ALIASES` dict in ir_types.py:67-90]

‏`return_schema` הוא **ההטלה ל-GraphQL** של `output_columns`, לא מקור האמת.
הצהירו על `output_columns` לצורך אימות ו-Data Lineage; הוסיפו `return_schema` לצורך יצירת
סוגי GraphQL. [tool-verified: models.py:744-748, comment "return_schema is its GraphQL projection"]

## כתיבת פקודה

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

הווריאנט של gRPC ‏(`enrich_grpc_set`) עוקב אחר אותה תבנית אך מציין `impl_kind: grpc`
ו-`binding` עם המפתחות `target` ו-`method` במקום `callable`:

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

### ממשק הניהול

טופס הפקודה ב-**Settings ← Commands** כולל עורך עמודות-קלט לכל ערכת נתונים (שורה אחת
לכל עמודה מוצהרת, עם בורר סוג IR) ועורך עמודות-פלט. שמרו את הטופס כדי
לרשום או לעדכן את הפקודה ללא טעינת תצורה מחדש. [inferred from CommandFormFields.tsx]

## הרכבה מוטבעת (REQ-1159)

פקודות רשאיות להופיע **בתוך** משפט SQL גדול יותר — מצורפות ב-join, בתת-שאילתה או מוטלות. אינכם
מוגבלים ל-`SELECT * FROM fn(args)`.

```sql
-- Enrich the orders relation and join the result back inline.
SELECT o.id, o.amount, e.score, e.region_label
FROM   orders o
JOIN   enrich_orders('main.public.orders') e ON o.id = e.id
WHERE  e.score > 0.8;
```

לפני שממשל, אימות או ניתוב רצים, הצינור מזהה קריאות לפקודות רשומות,
מבצע כל אחת דרך המבצע הממושל המשותף (כך שחוזה הקלט/פלט ומודל הזהות חלים
בדיוק כמו בקריאה ישירה), וכותב מחדש את אתר הקריאה ליחס מקומי מוקלד.
[tool-verified: `_localize_inline_commands` in _pipeline.py:145-163 and localize_commands in
command_localize.py:178-222]

ההצבה מסתגלת לגודל: עד 1,000 שורות התוצאה מוטבעת כרשימת `VALUES` מוקלדת;
מעל לסף הזה היא נרשמת כיחס מקומי בעל שם במנוע.
[tool-verified: `_DEFAULT_VALUES_MAX_ROWS = 1000` in command_localize.py:49, path at lines 211-216]

משפט שעבר לוקליזציה מנותב כרגיל. שאילתות חד-מקוריות נשארות על המקור; רק שאילתות
חוצות-מקורות באמת הולכות למנוע הפדרציה. [tool-verified: _pipeline.py:304 comment
"REQ-1159: a localized statement carries an inline local relation..."]

## פקודות ו-Data Lineage

משום שכל פקודה מצהירה על עמודות הקלט והפלט שלה, Data Lineage ברמת העמודה **נסגר על פני
גבול הפקודה האטום**. מנוע ה-Data Lineage מיישם סגור זיהום: כל עמודת פלט מוצהרת
נגזרת מכל עמודת קלט מוצהרת. [tool-verified: `_splice_commands` in graph.py:223-242]

**ההשלכה המעשית:** רוחב חוזה הקלט שלכם קובע את דיוקו של אותו
סגור. קלט צר — רק העמודות שהפקודה באמת צריכה — מייצר חרוט Data Lineage הדוק
וקריא. הצהרה על כל עמודה ביחס המקור מתפרשׂת רחב על פני כל
פלט, מה שעדיין תקין (שום Data Lineage אינו אובד) אך מטשטש את יכולת המעקב.

**כלל אצבע:** העבירו את ההטלה המינימלית שהפקודה צריכה, והחזירו רק עמודות נגזרות
(לא קלטים שהוחזרו כהד ללא שינוי). זה שומר על חרוט הזיהום מדויק. [inferred from
_splice_commands behavior in graph.py and _materialize_relation narrow-projection in function_dispatch.py:161]

ראו [Data Lineage](lineage.md) לאופן שבו צמתי פקודה מופיעים ב-DAG וכיצד לקרוא אותם.

## רשימת היתר ליציאה

פקודות `http` ו-`grpc` קוראות לנקודות קצה חיצוניות. כל מארח יעד חייב להופיע ב-
`udf_egress_allowlist` של הפריסה. ‏Loopback (`localhost`, `127.0.0.1`, `::1`) מותר
תמיד. רשימת היתר נעדרת דוחה כל יציאה חיצונית עם HTTP 403 — אין ברירת מחדל
שקטה. [tool-verified: `_check_egress` in function_dispatch.py:292-311]

## מעקב הפעלות (REQ-886)

כל הפעלה פולטת מעקב ללא קשר לתוצאה. המעקב כולל את שם הפקודה,
סוג התעבורה, מודל הזהות (DEFINER או INVOKER), הפניות ליחסי הקלט, מזהה התפקיד,
ועוצמת הפלט. המשגר פולט את המעקב — שום `impl_kind` אינו יכול לעקוף אותו.
[tool-verified: `udf_invocation_trace` context in dispatch_function:475-492]

## CLI: provisa metadata export

‏`provisa metadata export` היא משימה בשכבת המעטפת, לא RPC ממושל. היא מפעילה את פרסום
המטא-דאטה לפי דרישה של השרת הרץ (REQ-1072/REQ-1074) על ידי שליחת POST אל
`/admin/metadata-export/publish` — אותה נקודת קצה שכפתור **Publish now** בלשונית הניהול
קורא לה. [tool-verified: `_cmd_metadata_export` in provisa/cli.py:272-310]

השתמשו בה כדי להניע ייצואים מתוזמנים מ-cron או מ-CI כשלוח הזמנים המוגדר ב-`reconcile_cron`
אינו גרעיני מספיק:

```bash
provisa metadata export --api https://acme.provisa.org --token "$PROVISA_API_TOKEN"
```

יציאה 0 = פרסום מלא. יציאה 1 = פרסום חלקי או כשל בחיבור.

למדריך הדגלים המלא, אפשרויות האימות, מתן שמות מארחים בריבוי-דיירים ודוגמת cron, ראו
[ייצוא מטא-דאטה — משורת הפקודה](metadata-export.md#from-the-command-line).


פקודות מופיעות בהטלת ה-git של כל סביבה. ראו [סביבות](environments.md) לאופן שבו פקודה והקצאות התגיות שלה שורדות מיזוג ומשיכה.
