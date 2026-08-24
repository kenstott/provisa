# Column-Level Lineage

Provisa עוקבת אחר Data Lineage של נתונים ברמת העמודה באופן סטטי — מחושב מהגדרות SQL ומחוזי
פקודות, ללא צורך בביצוע. שתי תצוגות זמינות: DAG לכל משפט וגרף מקור
ברוחב הפדרציה הפרוש על פני כל התצוגות ו-Materialized Views ‏(MV) הרשומות.

## סייר ה-Data Lineage

נווטו אל **Lineage** בממשק המשתמש (`/lineage`). הדביקו משפט SQL ולחצו על **Build statement
graph** כדי לראות את ה-DAG שלו ברמת העמודה. לחצו על **Federation graph** כדי לטעון את גרף המקור על פני
כל MV ברישום. [tool-verified: LineagePage.tsx:28-119]

## DAG ברמת המשפט (REQ-1160)

כל עמודת פלט בעלת שם ב-SQL שלכם הופכת לצומת. הבונה עוקב אחריה לאחור דרך כל
CTE, תת-שאילתה, join וקריאת פקודה מוטבעת אל עמודות המקור שלה, ובונה גרף מכוון
מקלטי המקור אל פלטי הסיום.

### דוגמה מעובדת

```sql
SELECT o.id, e.embedding, upper(e.geo) AS geo_u
FROM   orders o
JOIN   enrich_grpc_set('main.public.orders') e ON o.id = e.id
```

המשפט הזה מייצר שלוש עמודות פלט. הגרף עבור `geo_u` נראה כך:

```text
orders.geo  ──[enrich_grpc_set(...)]──►  e.geo  ──[UPPER]──►  geo_u
orders.id   ─╮                                              (taint closure)
orders.region ─╯
```

- ‏`orders.id`, `orders.region` ו-`orders.geo` הם צמתי **source** ‏(חוזה הקלט הצר
  של `enrich_grpc_set` מצהיר על `id` ועל `region`; סגור הזיהום המלא מחבר את כל הקלטים
  המוצהרים לכל הפלטים). [tool-verified: `_splice_commands` in graph.py:223-242]
- ‏`e.embedding` ו-`e.geo` הם צמתי **command** — גבול ה-`enrich_grpc_set`.
- ‏`geo_u` הוא צומת **derived** המיוצר על ידי פונקציית ה-SQL `UPPER`.

גבול הפקודה **אינו אטום**. משום ש-`enrich_grpc_set` מצהיר על עמודות הקלט שלו
(`id`, `region`) ועל עמודות הפלט שלו (`id`, `embedding`, `geo`), מנוע ה-Data Lineage משחיל את
סגור הזיהום ברציפות מהעמודות המוצהרות של יחס המקור אל כל פלט.
[tool-verified: `_splice_commands` and `_input_relation` in graph.py:245-271]

### סוגי צמתים ורמזים חזותיים

[tool-verified: LineageDag.tsx:25-29, KIND_COLOR constants; LineagePage.tsx:21-26 LEGEND]

| סוג צומת | צבע | משמעות |
| --- | --- | --- |
| `source` | ירוק | עמודה בטבלת בסיס |
| `derived` | כחול | מיוצרת על ידי ביטוי SQL (פונקציה, אופרטור, CTE) |
| `command` | סגול | עמודת פלט מפקודה רשומה |

טבעות נוספות על צומת:

- **טבעת כתומה** — עמודת פלט סופית של המשפט.
- **מסגרת כפולה** — היחס של העמודה הוא Materialized View ‏(תצלום MV/CTAS).
- **טבעת אדומה** — חבר במעגל המסווג כשגיאה.
- **טבעת צהובה** — חבר במעגל המסווג כלולאת משוב.

[tool-verified: LineageDag.tsx:88-103 Cytoscape style selectors]

### טרנספורמציות בעלות שם על קשתות

כל קשת נושאת את ביטוי ה-SQL הגולמי המייצר את עמודת היעד, בתוספת רשימת פעולות
בעלות שם: פונקציות SQL ‏(`sql_function`), אופרטורים אריתמטיים/לוגיים (`operator`), פקודות
רשומות (`command`), הפניות עמודה חשופות (`identity`) וליטרלים (`constant`).
[tool-verified: TransformOp and name_transform in graph.py:36-145]

קשת מקריאת פקודה מוצגת כקו סגול מקווקו בממשק המשתמש.
[tool-verified: LineageDag.tsx:122-124]

## גרף ברוחב הפדרציה (REQ-1161)

גרף הפדרציה ממזג את ה-Data Lineage לכל משפט של כל MV רשום לגרף מקור אחד.
זהות הצומת היא `relation.column` — עמודת פלט של תצוגה אחת והפניית קלט של תצוגה אחרת
לאותה עמודה מתקרסות לצומת אחד. התוצאה היא DAG יחיד מעמודות מקור בסיסיות אל
כל ערכת נתונים נגזרת בפלטפורמה. [tool-verified: `build_federation_graph` in merge.py:205-229
and `qualify_outputs` in graph.py:275-299]

השתמשו ב-`focus`, `direction` ו-`depth` כדי לתחום את התצוגה בקנה מידה של פדרציה מבלי לחשב מחדש
את הגרף. [tool-verified: `slice_graph` in merge.py:160-189]

## מעגלים (REQ-1161)

מעגלים מתוארים, לא נדחים. מנוע ה-Data Lineage מזהה כל מעגל מכוון
ו**מסווג** אותו. [tool-verified: `Cycle.classification` property in merge.py:43-46]

| סיווג | צבע מסגרת | משמעות |
| --- | --- | --- |
| `feedback` | צהוב | המעגל חוצה צומת ממוטריאל — לולאת משוב חוקית ומושהית בזמן. תצלום ה-MV הוא גבול הגרסה שהופך אותה למוגדרת היטב. |
| `error` | אדום | אין גבול מטריאליזציה על הלולאה — הגדרה מעגלית ללא סדר הערכה יציב. סביר להניח ששגיאת תכנון. |

[tool-verified: LineagePage.tsx:83-98 cycle alert rendering; merge.py:38-48]

מעגל `feedback` אינו כשל. ‏MV של העשרה המזין בחזרה עמודה נגזרת אל יחס המקור
שלו עצמו הוא דפוס תקף כל עוד צומת אחד על הלולאה ממוטריאל —
התצלום מבודד את שני החצאים בזמן. מעגל `error` דורש שיפוט של מפעיל: הוא בדרך כלל
פירושו ששתי תצוגות מפנות זו לזו ללא תצלום ביניהן.

## API

שתי נקודות הקצה **סטטיות** — הן קוראות הגדרות וחוזים, לא נתונים.

### POST /admin/lineage/graph

מחזירה את ה-DAG ברמת העמודה עבור משפט SQL יחיד.

```http
POST /admin/lineage/graph
Content-Type: application/json

{
  "sql": "SELECT o.id, e.embedding FROM orders o JOIN enrich_grpc_set('main.public.orders') e ON o.id = e.id",
  "dialect": "postgres"
}
```

[tool-verified: `lineage_graph` endpoint at lineage_router.py:45-54, LineageGraphRequest model at
lineage_router.py:29-31]

צורת התגובה [tool-verified: `LineageGraph.to_dict` in graph.py:82-105]:

```json
{
  "nodes": [
    {"id": "orders.id", "column": "id", "relation": "orders", "kind": "source", "materialized": false}
  ],
  "edges": [
    {
      "source": "orders.id",
      "target": "e.id",
      "transform": "enrich_grpc_set(...)",
      "ops": [{"name": "enrich_grpc_set", "kind": "command"}]
    }
  ],
  "outputs": ["id", "embedding"]
}
```

מחזירה HTTP 422 כשלא ניתן לנתח את ה-SQL.
[tool-verified: lineage_router.py:51-54]

### GET /admin/lineage/federation

מחזירה את גרף המקור הממוזג על פני כל ה-MV ברישום.

```http
GET /admin/lineage/federation
GET /admin/lineage/federation?focus=orders.id&direction=downstream&depth=3
```

[tool-verified: `federation_graph` endpoint at lineage_router.py:73-98]

פרמטרי שאילתה [tool-verified: function signature at lineage_router.py:73-76]:

| פרמטר | ערכים | ברירת מחדל | השפעה |
| --- | --- | --- | --- |
| `focus` | מזהה צומת | — | תוחם את התגובה לתת-הגרף סביב הצומת הזה |
| `direction` | `upstream` \| `downstream` \| `both` | `both` | לאיזה כיוון לסרוק מ-`focus` |
| `depth` | מספר שלם | ללא הגבלה | מרחק הקפיצות המרבי מ-`focus` |

התגובה היא באותה צורה כמו גרף המשפט, עם שדה `cycles` נוסף
[tool-verified: `MergedGraph.to_dict` in merge.py:60-64]:

```json
{
  "nodes": [...],
  "edges": [...],
  "outputs": [...],
  "cycles": [
    {
      "nodes": ["orders.region", "enriched_orders.region"],
      "has_materialization_boundary": true,
      "classification": "feedback"
    }
  ]
}
```

## מה שינוי שם או מחיקה של עמודה היו שוברים (REQ-1484)

עמודה נושאת שני שמות, וכל אחד מהם מאוחסן על ידי קבוצת ארטיפקטים אחרת.

**השם החשוף** הוא מה שמשטחי ה-SQL וה-GraphQL מציגים: ‏`table_columns.alias`, עם נפילה
לברירת המחדל ב-snake_case כשלא נקבע כינוי [tool-verified: `computed_sql_alias` at
`schema_helpers.py:317`]. תצוגות, Materialized Views, ביטויי מדדים, פרדיקטים של RLS, חוזי
DQ, גרעיני metric-view ומפתחות שורה של MV נכתבים כולם מול השם ההוא, ולכן **שינוי שם של
כינוי שובר אותם בדיוק כפי שמחיקת העמודה עושה**.

**השם הפיזי** הוא `table_columns.column_name`, הזהות ששורדת את ההחלפה הגורפת של עמודות
בעת upsert של הטבלה. קשרים, קישורי [מילון](glossary.md), הקצאות תגיות, עמודת ה-watermark
וקבועות מראש של עמודות מאחסנים את זה, ולכן הם נשברים רק כשהעמודה **מוסרת**.

‏`columnDependents` מדווח על שניהם. תצוגות ו-MV במורד הזרם מגיעות מפריסת גרף הפדרציה
בשם החשוף של העמודה; הארטיפקטים שהגרף אינו מכסה מגיעים מסריקה ישירה של
הרישום [tool-verified: `graph_dependents` in `provisa/lineage/dependents.py`, registry scans in
`provisa/api/admin/column_dependents.py`].

```graphql
query {
  columnDependents(tableId: "42", renamed: ["order_total"], removed: ["legacy_code"]) {
    columnName
    dependents { kind name detail breaksOn }
  }
}
```

‏`breaksOn` הוא `rename` עבור הפניה לשם חשוף ו-`remove` עבור הפניה לשם פיזי, כך
שקורא יכול לדעת לאיזה חצי של העריכה כל ארטיפקט מגיב.

שאלו זאת **לפני** השמירה. עמודה ששמה שונה מאותרת לפי השם החשוף שהיא עדיין נושאת
ברישום; ברגע שהכינוי נחת, השם הישן נעלם והשאילתה אינה מוצאת דבר.

עמוד הטבלאות מריץ את השאילתה אוטומטית כשעריכה ממתינה משנה כינוי או מכווצת את
קבוצת העמודות, ומונה את מה שהוא מוצא [tool-verified: `diffEditedColumns` in
`provisa-ui/src/pages/tables/columnDiff.ts`, dialog in `TablesPage.tsx`]. האזהרה היא ייעוצית:
היא נוקבת בשמות הארטיפקטים המושפעים והמנהל מחליט. היא אינה חוסמת את השמירה, משום
שלא ניתן להגיע לכל צרכני האחוזה — לוח מחוונים חיצוני או יישום לקוח
השואל את העמודה בשמה נמצאים מעבר לידיעת הרישום. מאותה סיבה, סריקות על טקסט
SQL חופשי מתאימות את העמודה כאסימון מזהה במקום לפענח היקף, מה שעלול לנקוב בשם
ארטיפקט שמסתבר שאינו משתמש בעמודה. דיווח יתר הוא הכיוון הבטוח עבור אזהרה.

## שימוש ב-Data Lineage לממשל חוזי פקודות

משום שסגור הזיהום מחבר כל עמודת קלט מוצהרת לכל עמודת פלט מוצהרת,
רוחבו של אותו סגור תלוי לחלוטין במה שאתם מצהירים.

שקלו פקודה המקבלת טבלת orders מלאה (`id`, `region`, `amount`, `customer_id`,
‏`discount`, `notes`, ...) ומחזירה `embedding`. אם חוזה הקלט מונה את כל העמודות האלה,
כל עמודה במורד הזרם המשתמשת ב-embedding תציג Data Lineage מכולן.
זה מדויק אך לא שימושי — קשה לדעת מה באמת היה חשוב.

הצהירו רק על `id` ועל `text` (העמודות שמודל ה-embedding באמת קורא), וחרוט
ה-Data Lineage מתהדק לשתי עמודות המקור ההן. הגזירה תקינה ומדויקת כאחת.

ראו [פקודות](commands.md) למכניקה של הצהרה על חוזה קלט צר.
