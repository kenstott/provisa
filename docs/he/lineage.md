# Data Lineage ברמת עמודה

Provisa עוקבת אחר Data Lineage ברמת עמודה באופן סטטי — מחושב מהגדרות SQL וחוזי (contracts) command,
ללא צורך בהרצה. שתי תצוגות זמינות: DAG פר-statement וגרף מקור (provenance graph) רחב-פדרציה
הפרוש על כל התצוגות (views) והתצוגות המומחשות (materialized views, MVs) הרשומות.

## סייר ה-lineage

נווטו אל **Lineage** ב-UI (`/lineage`). הדביקו statement של SQL ולחצו על **Build statement
graph** כדי לראות את ה-DAG שלו ברמת עמודה. לחצו על **Federation graph** כדי לטעון את גרף המקור על
פני כל MV ברישום. [tool-verified: LineagePage.tsx:28-119]

## DAG ברמת statement (REQ-1160)

כל עמודת פלט בעלת שם ב-SQL שלכם הופכת לצומת (node). הבונה עוקב אחריה אחורה דרך כל CTE, תת-שאילתה
(subquery), join, וקריאת command מוטבעת עד לעמודות המקור שלה, ובונה גרף מכוון ממקורות הקלט אל
פלטי הסיום.

### דוגמה מפורטת

```sql
SELECT o.id, e.embedding, upper(e.geo) AS geo_u
FROM   orders o
JOIN   enrich_grpc_set('main.public.orders') e ON o.id = e.id
```

ה-statement הזה מפיק שלוש עמודות פלט. הגרף עבור `geo_u` נראה כך:

```text
orders.geo  ──[enrich_grpc_set(...)]──►  e.geo  ──[UPPER]──►  geo_u
orders.id   ─╮                                              (taint closure)
orders.region ─╯
```

- `orders.id`, `orders.region`, ו-`orders.geo` הן צמתי **source** (חוזה הקלט הצר של
  `enrich_grpc_set` מצהיר על `id` ו-`region`; ה-taint closure המלא מחבר את כל קלטי הקלט המוצהרים
  אל כל הפלטים). [tool-verified: `_splice_commands` in graph.py:223-242]
- `e.embedding` ו-`e.geo` הן צמתי **command** — הגבול (boundary) של `enrich_grpc_set`.
- `geo_u` הוא צומת **derived** המופק על ידי הפונקציה `UPPER` של SQL.

גבול ה-command **אינו אטום**. מכיוון ש-`enrich_grpc_set` מצהיר על עמודות הקלט שלו
(`id`, `region`) ועמודות הפלט שלו (`id`, `embedding`, `geo`), מנוע ה-lineage מחבר את
ה-taint closure ברציפות מהעמודות המוצהרות של יחס המקור אל כל פלט.
[tool-verified: `_splice_commands` and `_input_relation` in graph.py:245-271]

### סוגי צמתים ורמזים חזותיים

[tool-verified: LineageDag.tsx:25-29, KIND_COLOR constants; LineagePage.tsx:21-26 LEGEND]

| סוג צומת | צבע | משמעות |
| --- | --- | --- |
| `source` | ירוק | עמודת טבלת בסיס |
| `derived` | כחול | מופק על ידי ביטוי SQL (פונקציה, אופרטור, CTE) |
| `command` | סגול | עמודת פלט מ-command רשום |

טבעות נוספות על צומת:

- **טבעת כתומה** — עמודת פלט סופית של ה-statement.
- **מסגרת כפולה** — היחס של העמודה הוא תצוגה מומחשת (MV/CTAS snapshot).
- **טבעת אדומה** — חבר במעגל (cycle) המסווג כשגיאה.
- **טבעת צהובה** — חבר במעגל המסווג כלולאת משוב (feedback loop).

[tool-verified: LineageDag.tsx:88-103 Cytoscape style selectors]

### טרנספורמציות בעלות שם על קשתות (edges)

כל קשת נושאת את ביטוי ה-SQL הגולמי שמפיק את עמודת היעד, בתוספת רשימת פעולות בעלות שם: פונקציות
SQL (`sql_function`), אופרטורים אריתמטיים/לוגיים (`operator`), commands רשומים (`command`),
הפניות עמודה חשופות (`identity`), וליטרלים (`constant`).
[tool-verified: TransformOp and name_transform in graph.py:36-145]

קשת מקריאת command מוצגת כקו סגול מקווקו ב-UI.
[tool-verified: LineageDag.tsx:122-124]

## גרף רחב-פדרציה (REQ-1161)

גרף הפדרציה ממזג את ה-lineage הפר-statement של כל MV רשום לגרף מקור אחד. זהות הצומת היא
`relation.column` — עמודת פלט של תצוגה והפניית קלט של תצוגה אחרת לאותה עמודה מתמזגות לצומת אחד.
התוצאה היא DAG יחיד מעמודות מקור בסיסיות ועד לכל ערכת נתונים נגזרת בפלטפורמה.
[tool-verified: `build_federation_graph` in merge.py:205-229 and `qualify_outputs` in graph.py:275-299]

השתמשו ב-`focus`, `direction`, ו-`depth` כדי לצמצם את התצוגה בקנה מידה של הפדרציה מבלי לחשב מחדש
את הגרף. [tool-verified: `slice_graph` in merge.py:160-189]

## מעגלים (Cycles) (REQ-1161)

מעגלים מתוארים, לא נדחים. מנוע ה-lineage מזהה כל מעגל מכוון (directed cycle) **ומסווג** אותו.
[tool-verified: `Cycle.classification` property in merge.py:43-46]

| סיווג | צבע מסגרת | משמעות |
| --- | --- | --- |
| `feedback` | צהוב | המעגל חוצה צומת מומחש — לולאת משוב חוקית, עם פיגור זמן (time-lagged). ה-snapshot של ה-MV הוא גבול הגרסה שהופך אותה לחד-משמעית. |
| `error` | אדום | אין גבול המחשה על הלולאה — הגדרה מעגלית ללא סדר הערכה יציב. ככל הנראה שגיאת עיצוב. |

[tool-verified: LineagePage.tsx:83-98 cycle alert rendering; merge.py:38-48]

מעגל מסוג `feedback` אינו כשל. MV להעשרה שמזין בחזרה עמודה נגזרת אל יחס המקור שלו עצמו הוא דפוס
תקף כל עוד צומת אחד בלולאה מומחש — ה-snapshot מבודד את שני החצאים באופן זמני (temporally). מעגל
מסוג `error` דורש שיקול דעת של המפעיל: בדרך כלל משמעותו ששתי תצוגות מפנות זו לזו ללא snapshot
ביניהן.

## API

שני נקודות הקצה **סטטיות** — הן קוראות הגדרות וחוזים, לא נתונים.

### POST /admin/lineage/graph

מחזיר את ה-DAG ברמת עמודה עבור statement יחיד של SQL.

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

מחזיר HTTP 422 כאשר לא ניתן לנתח (parse) את ה-SQL.
[tool-verified: lineage_router.py:51-54]

### GET /admin/lineage/federation

מחזיר את גרף המקור הממוזג על פני כל ה-MVs ברישום.

```http
GET /admin/lineage/federation
GET /admin/lineage/federation?focus=orders.id&direction=downstream&depth=3
```

[tool-verified: `federation_graph` endpoint at lineage_router.py:73-98]

פרמטרי שאילתה [tool-verified: function signature at lineage_router.py:73-76]:

| פרמטר | ערכים | ברירת מחדל | אפקט |
| --- | --- | --- | --- |
| `focus` | מזהה צומת | — | צמצום התגובה לתת-הגרף סביב צומת זה |
| `direction` | `upstream` \| `downstream` \| `both` | `both` | באיזה כיוון לעבור מ-`focus` |
| `depth` | מספר שלם | ללא הגבלה | מרחק קפיצות (hops) מרבי מ-`focus` |

צורת התגובה זהה לזו של גרף ה-statement, עם הוספת שדה `cycles`
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

## מה שינוי שם או מחיקה של עמודה עלולים לשבור (REQ-1484)

עמודה נושאת שני שמות, וכל אחד מהם נשמר על ידי קבוצת artifacts שונה.

**השם החשוף (exposed name)** הוא מה שמשטחי ה-SQL וה-GraphQL מציגים: `table_columns.alias`, ונופל
חזרה לברירת המחדל ב-snake_case כאשר לא הוגדר alias [tool-verified: `computed_sql_alias` at
`schema_helpers.py:317`]. תצוגות, תצוגות מומחשות, ביטויי metric, פרדיקטים של RLS, חוזי DQ,
גרנולריות (grains) של metric-view ומפתחות שורה (row keys) של MV — כולם נכתבים כנגד שם זה, כך
ש**שינוי שם alias שובר אותם בדיוק כמו מחיקת העמודה**.

**השם הפיזי (physical name)** הוא `table_columns.column_name`, הזהות ששורדת את ההחלפה המלאה של
עמודות ב-upsert של הטבלה. קשרים (relationships), קשרי glossary, שיוכי תגיות (tag assignments),
עמודת ה-watermark ופריסטים (presets) של עמודות שומרים דווקא את זה, ולכן הם נשברים רק כאשר העמודה
**מוסרת**.

`columnDependents` מדווח על שניהם. תצוגות ו-MVs נגזרים (downstream) מגיעים מחיתוך גרף הפדרציה
בשם החשוף של העמודה; ה-artifacts שהגרף הזה אינו מכסה מגיעים מסריקה ישירה של הרישום
[tool-verified: `graph_dependents` in `provisa/lineage/dependents.py`, registry scans in
`provisa/api/admin/column_dependents.py`].

```graphql
query {
  columnDependents(tableId: "42", renamed: ["order_total"], removed: ["legacy_code"]) {
    columnName
    dependents { kind name detail breaksOn }
  }
}
```

`breaksOn` הוא `rename` עבור הפניה בשם חשוף ו-`remove` עבור הפניה בשם פיזי, כך שקורא (caller)
יכול לדעת לאיזה חצי של העריכה כל artifact מגיב.

שאלו את זה **לפני** השמירה. עמודה ששונה שמה מאותרת לפי השם החשוף שהיא עדיין נושאת ברישום; ברגע
שה-alias נחת, השם הישן נעלם והשאילתה לא מוצאת דבר.

דף ה-Tables מריץ את השאילתה אוטומטית כאשר עריכה ממתינה (pending edit) משנה alias או מצמצמת את
קבוצת העמודות, ומציג את מה שהוא מוצא [tool-verified: `diffEditedColumns` in
`provisa-ui/src/pages/tables/columnDiff.ts`, dialog in `TablesPage.tsx`]. האזהרה היא מייעצת בלבד:
היא מציינת בשם את ה-artifacts המושפעים והמנהל מחליט. היא אינה חוסמת את השמירה, מכיוון שלא ניתן
להגיע לכל הצרכנים של האחוזה (estate) — לוח מחוונים חיצוני או אפליקציית לקוח ששואלים את העמודה לפי
שם הם מעבר לידיעת הרישום. מאותה סיבה, סריקות על טקסט SQL חופשי מתאימות את העמודה כטוקן מזהה במקום
לפתור scope, מה שעלול לציין artifact שבפועל אינו משתמש בעמודה. דיווח-יתר הוא הכיוון הבטוח עבור
אזהרה.

## שימוש ב-lineage לממשל חוזי command

מכיוון שה-taint closure מחבר כל עמודת קלט מוצהרת לכל עמודת פלט מוצהרת, רוחב ה-closure הזה תלוי
לחלוטין במה שאתם מצהירים.

התבוננו ב-command שלוקח טבלת orders מלאה (`id`, `region`, `amount`, `customer_id`,
`discount`, `notes`, ...) ומחזיר `embedding`. אם חוזה הקלט מפרט את כל אותן עמודות, כל עמודה נגזרת
שמשתמשת ב-embedding תציג lineage מכולן. זה מדויק אך לא שימושי — קשה לדעת מה באמת היה משמעותי.

הצהירו רק על `id` ו-`text` (העמודות שמודל ה-embedding קורא בפועל), וחרוט ה-lineage מצטמצם לשתי
עמודות המקור הללו. הנגזרת גם קבילה (sound) וגם מדויקת.

ראו [Commands](commands.md) למכניקה של הצהרה על חוזה קלט צר.
