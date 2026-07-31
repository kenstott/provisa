# Lineage ברמת-עמודה

Provisa עוקבת אחר lineage נתונים ברמת-עמודה באופן סטטי — מחושב מהגדרות SQL וחוזי
command, ללא צורך בביצוע. שתי תצוגות זמינות: DAG לכל-משפט וגרף
provenance רחב-פדרציה החוצה על פני כל התצוגות הרשומות ותצוגות ה-materialized (MVs).

## סייר ה-Lineage

נווטו ל-**Lineage** ב-UI (`/lineage`). הדביקו משפט SQL ולחצו **Build statement
graph** כדי לראות את ה-DAG ברמת-העמודה שלו. לחצו **Federation graph** כדי לטעון את גרף ה-provenance על פני
כל MV ברישום. [tool-verified: LineagePage.tsx:28-119]

## DAG ברמת-משפט (REQ-1160)

כל עמודת פלט בעלת-שם ב-SQL שלכם הופכת ל-node. ה-builder עוקב אחריה חזרה דרך כל
CTE, תת-שאילתה, join, וקריאת command inline עד לעמודות המקור שלה, בונה גרף מכוון
מקלטי מקור לפלטים סופיים.

### דוגמה מפורטת

```sql
SELECT o.id, e.embedding, upper(e.geo) AS geo_u
FROM   orders o
JOIN   enrich_grpc_set('main.public.orders') e ON o.id = e.id
```

משפט זה מייצר שלוש עמודות פלט. הגרף עבור `geo_u` נראה כך:

```text
orders.geo  ──[enrich_grpc_set(...)]──►  e.geo  ──[UPPER]──►  geo_u
orders.id   ─╮                                              (taint closure)
orders.region ─╯
```

- `orders.id`, `orders.region`, ו-`orders.geo` הם nodes מסוג **source** (החוזה
  הצר של קלט `enrich_grpc_set` מצהיר `id` ו-`region`; ה-taint-closure המלא מחבר את כל
  הקלטים המוצהרים לכל הפלטים). [tool-verified: `_splice_commands` in graph.py:223-242]
- `e.embedding` ו-`e.geo` הם nodes מסוג **command** — גבול `enrich_grpc_set`.
- `geo_u` הוא node מסוג **derived** המיוצר על ידי הפונקציה `UPPER` של SQL.

גבול ה-command **אינו אטום**. מכיוון ש-`enrich_grpc_set` מצהיר את עמודות הקלט שלו
(`id`, `region`) ועמודות הפלט (`id`, `embedding`, `geo`), מנוע ה-lineage מחבר (splices) את
ה-taint closure ברציפות מהעמודות המוצהרות של relation המקור לכל פלט.
[tool-verified: `_splice_commands` and `_input_relation` in graph.py:245-271]

### סוגי node ורמזים ויזואליים

[tool-verified: LineageDag.tsx:25-29, KIND_COLOR constants; LineagePage.tsx:21-26 LEGEND]

| סוג Node | צבע | משמעות |
| --- | --- | --- |
| `source` | ירוק | עמודת טבלת בסיס |
| `derived` | כחול | מיוצר על ידי ביטוי SQL (פונקציה, אופרטור, CTE) |
| `command` | סגול | עמודת פלט מ-command רשום |

טבעות נוספות על node:

- **טבעת כתומה** — עמודת פלט סופית של המשפט.
- **מסגרת כפולה** — ה-relation של העמודה הוא materialized view (snapshot של MV/CTAS).
- **טבעת אדומה** — חבר במעגל המסווג כשגיאה.
- **טבעת צהובה** — חבר במעגל המסווג כלולאת משוב (feedback loop).

[tool-verified: LineageDag.tsx:88-103 Cytoscape style selectors]

### שינויים בעלי-שם (Named transforms) על קשתות

כל קשת נושאת את ביטוי ה-SQL הגולמי המייצר את עמודת היעד, בתוספת רשימת
פעולות בעלות-שם: פונקציות SQL (`sql_function`), אופרטורים אריתמטיים/לוגיים (`operator`), commands
רשומים (`command`), הפניות עמודה עירומות (`identity`), וליטרלים (`constant`).
[tool-verified: TransformOp and name_transform in graph.py:36-145]

קשת מקריאת command מוצגת כקו סגול מקווקו ב-UI.
[tool-verified: LineageDag.tsx:122-124]

## גרף רחב-פדרציה (REQ-1161)

גרף הפדרציה ממזג את ה-lineage לכל-משפט של כל MV רשום לתוך גרף provenance אחד.
זהות ה-node היא `relation.column` — עמודת הפלט של תצוגה והפניית הקלט של תצוגה אחרת
לאותה עמודה מתמזגות ל-node אחד. התוצאה היא DAG יחיד מעמודות מקור בסיסיות עד
כל מערך נתונים נגזר בפלטפורמה. [tool-verified: `build_federation_graph` in merge.py:205-229
and `qualify_outputs` in graph.py:275-299]

השתמשו ב-`focus`, `direction`, ו-`depth` כדי לתחום את התצוגה בקנה-מידה פדרטיבי ללא חישוב מחדש
של הגרף. [tool-verified: `slice_graph` in merge.py:160-189]

## מעגלים (REQ-1161)

מעגלים מתוארים, לא נדחים. מנוע ה-lineage מזהה כל מעגל מכוון
ו**מסווג** אותו. [tool-verified: `Cycle.classification` property in merge.py:43-46]

| סיווג | צבע מסגרת | משמעות |
| --- | --- | --- |
| `feedback` | צהוב | המעגל חוצה node ממומש (materialized) — לולאת משוב חוקית, מושהית-בזמן. ה-snapshot של ה-MV הוא גבול הגרסה המבהיר אותה. |
| `error` | אדום | ללא גבול מימוש בלולאה — הגדרה מעגלית ללא סדר הערכה יציב. ככל הנראה שגיאת עיצוב. |

[tool-verified: LineagePage.tsx:83-98 cycle alert rendering; merge.py:38-48]

מעגל `feedback` אינו כשל. MV העשרה המזין בחזרה עמודה נגזרת ל-
relation המקור שלו עצמו הוא דפוס תקף כל עוד node אחד בלולאה ממומש — ה-
snapshot מבודד את שני החצאים באופן זמני. מעגל `error` דורש שיקול-דעת מפעיל: הוא בדרך-כלל
אומר ששתי תצוגות מפנות זו לזו ללא snapshot ביניהן.

## API

שתי נקודות הקצה הן **סטטיות** — הן קוראות הגדרות וחוזים, לא נתונים.

### POST /admin/lineage/graph

מחזיר את ה-DAG ברמת-עמודה עבור משפט SQL יחיד.

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

מחזיר HTTP 422 כאשר ה-SQL אינו ניתן לפענוח.
[tool-verified: lineage_router.py:51-54]

### GET /admin/lineage/federation

מחזיר את גרף ה-provenance הממוזג על פני כל ה-MVs ברישום.

```http
GET /admin/lineage/federation
GET /admin/lineage/federation?focus=orders.id&direction=downstream&depth=3
```

[tool-verified: `federation_graph` endpoint at lineage_router.py:73-98]

פרמטרי שאילתה [tool-verified: function signature at lineage_router.py:73-76]:

| פרמטר | ערכים | ברירת מחדל | אפקט |
| --- | --- | --- | --- |
| `focus` | מזהה node | — | תיחום התגובה לתת-הגרף סביב node זה |
| `direction` | `upstream` \| `downstream` \| `both` | `both` | לאיזה כיוון לחצות מ-`focus` |
| `depth` | מספר שלם | ללא-גבול | מרחק hop מקסימלי מ-`focus` |

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

## שימוש ב-lineage לממשל חוזי command

מכיוון שה-taint closure מחבר כל עמודת קלט מוצהרת לכל עמודת פלט מוצהרת,
רוחב אותו closure תלוי לחלוטין במה שאתם מצהירים.

שקלו command הלוקח טבלת orders מלאה (`id`, `region`, `amount`, `customer_id`,
`discount`, `notes`, ...) ומחזיר `embedding`. אם חוזה הקלט מפרט את כל אותן
עמודות, כל עמודה downstream המשתמשת ב-embedding תראה lineage מכולן.
זה מדויק אך לא שימושי — קשה לדעת מה באמת היה חשוב.

הצהירו רק `id` ו-`text` (העמודות שמודל ה-embedding באמת קורא), וקונוס ה-lineage
מתהדק לשתי עמודות המקור הללו. הגזירה גם תקינה וגם מדויקת.

ראו [Commands](commands.md) עבור המכניקה של הצהרת חוזה קלט צר.
