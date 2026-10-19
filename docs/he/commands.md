# פקודות {: #commands }

פקודה היא פונקציה רשומה וממושלת המכניסה חישוב חיצוני תחת מערכת הממשל, הביקורת
וה-Data Lineage של Provisa. במקום שמנוע הפדרציה מטפל ב-SQL באופן טבעי, פקודה
היא התפר עבור חישוב שאין ביכולתו לבטא: מיקרו-שירות העשרה, מודל Python, סקריפט
מעטפת, פרוצדורה מאוחסנת טבעית של מסד נתונים. רשמו אותה פעם אחת; כל משטח לקוח — GraphQL,
‏pgwire SQL, ‏REST, ‏Arrow Flight, ‏gRPC, ‏Bolt/Cypher — יכול להפעיל אותה עם ממשל זהה
.

ההבחנה המרכזית: פקודה היא **RPC ממושל**, לא ETL אד-הוק. הקלטים והפלטים שלה
מוצהרים, מוקלדים, מאומתים, נמעקבים ומחווטים אל ה-Data Lineage. קריאת curl או תת-תהליך לא ממושלים
אינם אף אחד מהדברים האלה.

## סוגי מימוש {: #implementation-kinds }

שישה ערכי `impl_kind` נתמכים:

| `impl_kind` | תעבורה |
| --- | --- |
| `source_procedure` | פרוצדורה מאוחסנת טבעית על מקור רשום |
| `source_operation` | פעולת כתיבה של מקור OpenAPI, GraphQL מרוחק או gRPC, המועברת כפי שהיא (ראו [פעולת כתיבה של מקור מרוחק](#a-remote-sources-write-operation)) |
| `script` | תת-תהליך מקומי המוזן JSON ב-stdin, קורא JSON מ-stdout |
| `http` | נקודת קצה HTTP/S; ‏גוף בקשה JSON, תגובה JSON |
| `grpc` | ‏gRPC unary; גשר JSON נטול proto |
| `python` | קריאה של Python בתוך התהליך (`module:attr`) |

המיעון (ה-`name` וה-`function_name` בקטלוג) מנותק מה-`binding` (תעבורה
ומיקום). החליפו את ה-binding והממשל, ה-Data Lineage וחוזי הקורא של הפקודה יישארו
ללא שינוי.

## סוגי ארגומנטים {: #argument-kinds }

כל ארגומנט מצהיר על `arg_kind`:

| `arg_kind` | התנהגות |
| --- | --- |
| `column_value` | סקלר; מועבר ישירות במטען הבקשה |
| `table_ref` | עצל; Provisa מעבירה את הפניית היחס כמות שהיא; השירות מביא את הנתונים |
| `result_set` | להוט; Provisa ממטריאלת את היחס המופנה ושולחת את שורותיו |

פקודות `http` ו-`grpc` **חייבות** להצהיר על ארגומנט `table_ref` או `result_set` אחד לפחות.
פקודה חיצונית המקבלת ארגומנטים סקלריים בלבד הייתה מופעלת פעם אחת לכל שורה, מה שמסכל
איגוד לאצוות. המשגר דוחה תצורה זו בזמן הקריאה (422).

פקודה המחזירה קבוצה (מוצהרת דרך `output_columns` ו-`return_schema`) היא
פונקציה מוערכת-טבלה. השתמשו בה בסעיף `FROM` או ב-`JOIN`.

## חוזה ערכת הנתונים {: #the-dataset-contract }

כל ארגומנט `table_ref` או `result_set` רשאי להצהיר על **חוזה עמודות קלט**: רשימה מסודרת
של עמודות מוקלדות-IR ב-`FunctionArgument.columns`. הפקודה עצמה מצהירה על
**חוזה עמודות פלט** ב-`Function.output_columns`.

שני החוזים מאומתים fail-loud בכל הפעלה:

- **קלט (result_set בלבד):** לאחר המטריאליזציה, Provisa מאמתת את השורות מול
  העמודות המוצהרות. שדות עודפים, שדות חסרים וסוגים שגויים — כולם מעלים HTTP 422.

- **פלט:** שורות המוחזרות על ידי הפקודה מאומתות מול `output_columns` לפני שהן
  מגיעות לקורא.
- **הטלה צרה:** כשחוזה קלט מוצהר, שאילתת המטריאליזציה מטילה
  **רק את העמודות ההן** (`SELECT "id", "region" FROM ...`) במקום `SELECT *`.

### אוצר מילות הסוגים של IR {: #the-ir-type-vocabulary }

סוגי עמודות בחוזה משתמשים במערכת סוגי ה-IR הקנונית, לא בסקלרים של GraphQL או
באיותים טבעיים של מקורות. השמות התקפים הם:

`smallint` `integer` `bigint` `text` `boolean` `float` `double` `numeric`
`date` `timestamp` `time` `uuid` `bytea` `json`

כינויים נפוצים מתפענחים אוטומטית (`varchar` → `text`, `int4` → `integer`, `jsonb` → `json`,
וכן הלאה).

‏`return_schema` הוא **ההטלה ל-GraphQL** של `output_columns`, לא מקור האמת.
הצהירו על `output_columns` לצורך אימות ו-Data Lineage; הוסיפו `return_schema` לצורך יצירת
סוגי GraphQL.

## כתיבת פקודה {: #authoring-a-command }

### קובץ תצורה {: #config-file }

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

### ממשק הניהול {: #admin-ui }

טופס הפקודה ב-**Settings ← Commands** כולל עורך עמודות-קלט לכל ערכת נתונים (שורה אחת
לכל עמודה מוצהרת, עם בורר סוג IR) ועורך עמודות-פלט. שמרו את הטופס כדי
לרשום או לעדכן את הפקודה ללא טעינת תצורה מחדש.

## הרכבה מוטבעת {: #inline-composition }

פקודות רשאיות להופיע **בתוך** משפט SQL גדול יותר — מצורפות ב-join, בתת-שאילתה או מוטלות. אינכם
מוגבלים ל-`SELECT * FROM fn(args)`. היוצא מן הכלל הוא פעולת כתיבה של מקור מרוחק, הנקראת לבדה (ראו [מדוע אי אפשר להרכיב אותה](#why-it-cannot-be-composed)).

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

ההצבה מסתגלת לגודל: עד 1,000 שורות התוצאה מוטבעת כרשימת `VALUES` מוקלדת;
מעל לסף הזה היא נרשמת כיחס מקומי בעל שם במנוע.

משפט שעבר לוקליזציה מנותב כרגיל. שאילתות חד-מקוריות נשארות על המקור; רק שאילתות
חוצות-מקורות באמת הולכות למנוע הפדרציה.

## פעולת כתיבה של מקור מרוחק {: #a-remote-sources-write-operation }

מקור OpenAPI, GraphQL מרוחק או gRPC מציע פעולות כתיבה. רישום אחת מהן כפקודה הופך אותה לניתנת לקריאה, מנוהלת ומבוקרת מכל משטח. הוספת המקור אינה רושמת אף אחת מהן; אתם רושמים את הרצויות, אחת אחת, כפי שאתם רושמים טבלאות. הרישום הוא הקיורציה.

מה מקור מציע:

| סוג מקור | פעולות מוצעות | שם הפעולה |
| --- | --- | --- |
| `openapi` | כל פעולה שאינה GET במפרט | ה-`operationId` |
| `graphql_remote` | כל שדה מסוג `Mutation` המרוחק | שם השדה |
| `grpc_remote` | כל מתודה המסווגת כ-mutation | `Service.Method` |

### רישום אחת {: #register-one }

1. פתחו את **מודל → פקודות** והוסיפו פקודה.
2. בחרו את המקור המרוחק. הטופס עובר לבורר פעולות המפרט מה שהמקור מציע.
3. בחרו את הפעולה, תחום ואת התפקידים שרשאים לקרוא לה.
4. אפשר להפעיל את **דורש אישור** ולמלא את השדה **כותב לטבלה**.

דרך ה-API של GraphQL לניהול, `availableFunctions(sourceId, schemaName)` מפרט את הפעולות. שם הסכמה הוא `openapi`, `graphql` או `grpc_remote`, לפי סוג המקור.

כל השאר נגזר מהפעולה, ולא ממה שהטופס שולח. `_as_source_operation` דורס את השדות האלה:

```python
body.implKind = "source_operation"
body.kind = "mutation"
body.schemaName = OPERATION_SCHEMA[source_type]
body.returns = ""
body.binding = {}
body.materialize = False
body.arguments = [{"name": a, "type": "json"} for a in operation.arguments]
```

כל ארגומנט מוגדר כ-`json`. הארגומנטים של הפעולה הם פרמטרי הנתיב שלה ב-OpenAPI (ועוד `body` כשהפעולה מקבלת גוף בקשה), ארגומנטי ה-mutation שלה ב-GraphQL או שדות הבקשה שלה ב-gRPC. פעולה שהמקור אינו מציע נדחית ב-422, `functions.operation_not_offered`.

### קריאה לפעולה {: #calling-it }

Provisa אינו מעצב, מגדיר סוג או בודק את הקלט. כל ארגומנט עובר לשירות המרוחק ללא שינוי, עם האישור של המקור, והתשובה של השירות המרוחק חוזרת ללא שינוי. Provisa שולט במי רשאי לקרוא, באיזה תחום, האם נדרש אישור, ורושם את הקריאה.

כך השירות המרוחק מקבל את הארגומנטים:

- **OpenAPI.** פרמטרי נתיב ממלאים את תבנית הנתיב. `body` הוא גוף הבקשה ב-JSON. כל ארגומנט אחר עובר במחרוזת השאילתה.
- **GraphQL.** ארגומנטים נשלחים כמשתנים מוגדרי סוג, כל אחד מוצהר עם הסוג שהסכמה המרוחקת נותנת לו. ה-mutation מבקש בחזרה את שדות הסקלר וה-enum של התשובה, ואת אלה של אובייקטים שבתוכה עד שתי רמות עומק.
- **gRPC.** הארגומנטים הופכים להודעת הבקשה של `Service.Method`.

התשובה היא שורות הפקודה: אובייקט הוא שורה אחת, רשימת אובייקטים היא השורות שלה, וכל דבר אחר הוא שורה אחת `{"result": ...}`.

ב-GraphQL הפקודה היא שדה mutation והתשובה שלה היא הסקלר JSON.

```graphql
mutation {
  createIssue(input: {repositoryId: "R_kgDO...", title: "Crash on save"})
}
```

במשטחי ה-SQL (pgwire ואחרים שמעבירים SQL) כתבו כל ארגומנט כליטרל JSON בתוך מחרוזת. `'{"title": "x"}'` הוא אובייקט, `'"text"'` מחרוזת, `'3'` מספר. ליטרל שאינו JSON תקין נכשל ב-422, `functions.json_argument_invalid`.

```sql
SELECT * FROM create_issue('{"repositoryId": "R_kgDO...", "title": "Crash on save"}');
```

ב-REST שלחו ב-POST אובייקט JSON של ארגומנטים אל `/data/rest/{domain}/commands/{command}`. ארגומנט `json` מתועד במפרט שנוצר כערך כלשהו.

```bash
curl -X POST https://acme.provisa.org/data/rest/engineering/commands/create_issue \
  -H "Content-Type: application/json" \
  -d '{"input": {"repositoryId": "R_kgDO...", "title": "Crash on save"}}'
```

### דחיות {: #refusals }

הדחייה של השירות המרוחק חוזרת כפי שהשירות ניסח אותה.

| השירות המרוחק | Provisa משיב |
| --- | --- |
| דוחה את הקריאה (HTTP 4xx, `errors` של GraphQL, קריאת gRPC שנדחתה) | 422, `functions.remote_refused`, עם `remote_status` ו-`answer` |
| נכשל (HTTP 5xx) | 502, `functions.remote_refused` |

האם האישור של המקור רשאי לבצע את הפעולה נתון להכרעת השירות המרוחק, כשקוראים לפעולה. Provisa אינו יכול לנסות כתיבה ברישום בלי לבצע אותה.

### אישור {: #approval }

הפעילו **דורש אישור** וכל קריאה מוצגת ל-hook האישור של הפריסה לפני שהיא רצה. היא רצה רק אם ה-hook מאשר.

- לא הוגדר hook: 403, `functions.approval_unavailable`.
- ה-hook דוחה: 403, `functions.approval_denied`, עם הנימוק של ה-hook.

ה-hook מקבל את הקורא, התפקיד, שם הפקודה והארגומנטים שלה. ראו [Hook אישור ABAC](security.md#abac-approval-hook). הדגל נשמר כ-`Function.requires_approval`, והבדיקה חלה על כל פקודה שמגדירה אותו.

### כותב לטבלה {: #writes-table }

ציינו בשדה **כותב לטבלה** את הטבלה שהפעולה כותבת אליה, בצורת `schema.table`. היא חייבת להיות טבלה רשומה של המקור של הפקודה עצמה, אחרת השמירה נדחית ב-422, `actions.written_table_not_registered`. ההגדרה אופציונלית.

אחרי כל קריאה שהשירות המרוחק מקבל, Provisa מתייחס אליה ככתיבה לטבלה זו. הוא משליך את התשובות השמורות במטמון של הטבלה, מסמן כמיושנות את התצוגות הממומשות שמעליה, פולט את אירוע השינוי, מריץ את הפקודות שרצות כשהיא משתנה וטוען מחדש את הטבלה כשהיא מוחזקת במצב hot.

כאשר הטבלה נקראת מההעתק המלא שלה (הגדרות המפעיל מציבות אותה שם, או שהמנוע אינו יכול לקרוא את המקור שלה במקומו), לאחר הקריאה מתבקשת בנייה של ההעתק, עם הסיבה `write`. הקוראים שומרים על ההעתק הישן עד שהחדש מחליף אותו. לטבלה המשוכפלת שורה אחר שורה, או לטבלה עם עמודת פרמטר, אין העתק מלא לבנות מחדש.

### מדוע אי אפשר להרכיב אותה {: #why-it-cannot-be-composed }

פעולת כתיבה היא פעולה, לא טרנספורמציה של נתונים. תצוגה או תצוגה ממומשת שהחזיקה אחת כזו הייתה מבצעת את הכתיבה בכל קריאה או רענון שלה. לכן הקריאה עומדת לבדה: `SELECT * FROM create_issue(...)` לבדה מריצה אותה, והקריאה עצמה בתוך הצהרה גדולה יותר נדחית, בכל מקום שבו היא נמצאת -- ב-join, בשאילתת משנה או בהקרנה.

תצוגה או תצוגה ממומשת שהגדרתה קוראת לאחת כזו נדחית בעת שמירתה. הגדרה שאינה ניתנת לניתוח נדחית אף היא כל עוד רשומה פעולת כתיבה כלשהי, כי אי אפשר להראות שאינה קוראת לאף אחת.

```text
command 'create_issue' writes to its source and is called on its own: it cannot be composed in a query, a view or a materialized view
```

פעולת מקור אינה גם צומת lineage: ה-lineage נקרא מה-SQL של תצוגות ושאילתות, שבו פקודה מופיעה כצומת, ושום הגדרה שמורה אינה יכולה לקרוא לפעולת מקור.

## פקודות ו-Data Lineage {: #commands-and-lineage }

משום שכל פקודה מצהירה על עמודות הקלט והפלט שלה, Data Lineage ברמת העמודה **נסגר על פני
גבול הפקודה האטום**. מנוע ה-Data Lineage מיישם סגור זיהום: כל עמודת פלט מוצהרת
נגזרת מכל עמודת קלט מוצהרת.

**ההשלכה המעשית:** רוחב חוזה הקלט שלכם קובע את דיוקו של אותו
סגור. קלט צר — רק העמודות שהפקודה באמת צריכה — מייצר חרוט Data Lineage הדוק
וקריא. הצהרה על כל עמודה ביחס המקור מתפרשׂת רחב על פני כל
פלט, מה שעדיין תקין (שום Data Lineage אינו אובד) אך מטשטש את יכולת המעקב.

**כלל אצבע:** העבירו את ההטלה המינימלית שהפקודה צריכה, והחזירו רק עמודות נגזרות
(לא קלטים שהוחזרו כהד ללא שינוי). זה שומר על חרוט הזיהום מדויק.

ראו [Data Lineage](lineage.md) לאופן שבו צמתי פקודה מופיעים ב-DAG וכיצד לקרוא אותם.

## רשימת היתר ליציאה {: #egress-allowlist }

פקודות `http` ו-`grpc` קוראות לנקודות קצה חיצוניות. כל מארח יעד חייב להופיע ב-
`udf_egress_allowlist` של הפריסה. ‏Loopback (`localhost`, `127.0.0.1`, `::1`) מותר
תמיד. רשימת היתר נעדרת דוחה כל יציאה חיצונית עם HTTP 403 — אין ברירת מחדל
שקטה.

## מעקב הפעלות {: #invocation-tracing }

כל הפעלה פולטת מעקב ללא קשר לתוצאה. המעקב כולל את שם הפקודה,
סוג התעבורה, מודל הזהות (DEFINER או INVOKER), הפניות ליחסי הקלט, מזהה התפקיד,
ועוצמת הפלט. המשגר פולט את המעקב — שום `impl_kind` אינו יכול לעקוף אותו.

## CLI: provisa metadata export

‏`provisa metadata export` היא משימה בשכבת המעטפת, לא RPC ממושל. היא מפעילה את פרסום
המטא-דאטה לפי דרישה של השרת הרץ על ידי שליחת POST אל
`/admin/metadata-export/publish` — אותה נקודת קצה שכפתור **Publish now** בלשונית הניהול
קורא לה.

השתמשו בה כדי להניע ייצואים מתוזמנים מ-cron או מ-CI כשלוח הזמנים המוגדר ב-`reconcile_cron`
אינו גרעיני מספיק:

```bash
provisa metadata export --api https://acme.provisa.org --token "$PROVISA_API_TOKEN"
```

יציאה 0 = פרסום מלא. יציאה 1 = פרסום חלקי או כשל בחיבור.

למדריך הדגלים המלא, אפשרויות האימות, מתן שמות מארחים בריבוי-דיירים ודוגמת cron, ראו
[ייצוא מטא-דאטה — משורת הפקודה](metadata-export.md#from-the-command-line).

פקודות מופיעות בהטלת ה-git של כל סביבה. ראו [סביבות](environments.md) לאופן שבו פקודה והקצאות התגיות שלה שורדות מיזוג ומשיכה.
