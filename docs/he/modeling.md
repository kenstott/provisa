<!-- markdownlint-disable MD046 -->
<!-- MD046 off: mkdocs-material `===` content-tab bodies are indented, which the linter
     misreads as indented code blocks; the fenced code blocks below are required for rendering. -->

# מידול נתונים (Entities & Facts)

Provisa נותנת לכם שני פרימיטיבים דקלרטיביים — `entity` ו-`fact` — המכסים את אבני-
הבניין שכל star schema ו-Data Vault מורכבים מהם. הצהירו את ה-spec; Provisa מורידה אותו
בדיוק להגדרות ה-materialized-view, בי-טמפורליות (bitemporal), והקשר שהייתם אחרת
כותבים ידנית (REQ-1164). [tool-verified: modeling.py module docstring lines 11-28]

## מה הם entities ו-facts

**entity** הוא הקרנה מוקלדת (keyed), נטולת-כפילויות, אופציונלית-היסטורית של relation מקור. אתם
נותנים לו שם, מכוונים אותו למקור, מצהירים את מפתח ה-entity והתכונות שברצונכם לשאת,
ובוחרים מצב היסטוריה. Provisa כותבת את ה-SQL של התצוגה ורושמת את ה-MV. כאשר היסטוריה
מופעלת, ה-MV הוא בי-טמפורלי. [tool-verified: `Entity` dataclass, modeling.py lines 53-69;
`entity_registration` function, modeling.py lines 105-120]

**fact** הוא join למפתחות entity, מצומצם לגרעין (grain) מוצהר, עם מדדים (measures) מצוברים. Provisa
כותבת שאילתת MV מצטברת (`GROUP BY` grain + עמודות FK) ורושמת קשר עבור כל
קישור-ממד מוצהר. fact ללא מדדים הוא סט-מפתחות טהור — דפוס ה-link של Data Vault.
[tool-verified: `Fact` dataclass, modeling.py lines 91-102; `fact_registration` function, modeling.py
lines 123-141; comment at line 130 "a measureless fact is a pure key-set (DV link)"]

שני המבנים הם IR. ההגדרות שנוצרות מתאימות מחדש (retarget) על פני מנועים — ממומשות ב-Oracle,
Databricks, או נשארות וירטואליות מעל מנוע MPP — ללא מידול-מחדש. [tool-verified: modeling.py
docstring lines 25-28]

## מצבי היסטוריה

שלושה מצבים זמינים על entity [tool-verified: `_HISTORY` constant at modeling.py line 38,
`_HISTORY_MODE` dict at modeling.py line 40]:

| מצב | משמעות | מצב בי-טמפורלי |
| --- | --- | --- |
| `none` | נוכחי-בלבד. ללא היסטוריה. | — |
| `scd2` | מעקב אחר כל שינוי. הוספת שורות שהשתנו בלבד (delta) מוקלדות על מפתח ה-entity. | `delta` |
| `snapshot` | מעקב אחר כל רענון. הוספת סט התוצאות המלא בכל רענון, מוחתם בזמן מערכת. | `snapshot` |

`scd2` דורש מפתח entity כדי לחשב את ה-delta. `snapshot` עובד על כל מנוע אך האחסון גדל
בעותק מלא לכל רענון. בחרו `scd2` עבור מקורות גדולים, איטיים-בשינוי; בחרו `snapshot` כאשר
אתם זקוקים להיסטוריה מלאה והמקור אינו יכול לספק מפתח.

ל-facts אין מצב היסטוריה — כיסוי טמפורלי מגיע מהיסטוריית ה-entity הבסיסית.

## מדדים (Measures) ואגרגציות

מדדים מוצהרים כזוגות `column:agg`. אגרגציות נתמכות [tool-verified: `_AGGS`
at modeling.py line 41]:

`sum` &nbsp;`avg` &nbsp;`min` &nbsp;`max` &nbsp;`count`

האגרגציה ברירת המחדל היא `sum` [tool-verified: `Measure.agg` default at modeling.py line 75].

## דוגמה מפורטת: entity לקוח + fact מכירות

### טבלאות המקור

- `raw.customers` — id, name, region, tier
- `raw.orders` — order_id, customer_id, amount, quantity

### רישום ה-entity לקוח

=== "Admin UI"

    1. פתחו **Tables** ולחצו **+ Model**.
    2. בחרו **Entity (dimension)**.
    3. מלאו את הטופס:
       - **Name:** `Customer`
       - **Source relation:** `raw.customers`
       - **Domain:** *(הדומיין שלכם)*
       - **Entity key:** `id`
       - **Attributes:** `name, region, tier`
       - **History:** `SCD2 (track changes — delta bitemporal)`
    4. לחצו **Create**.

=== "GraphQL API"

    ```graphql
    mutation {
      registerEntity(input: {
        name: "Customer"
        source: "raw.customers"
        domainId: "sales"
        key: ["id"]
        attributes: ["name", "region", "tier"]
        history: "scd2"
      }) {
        success
        message
      }
    }
    ```

Provisa יוצרת ורושמת MV בי-טמפורלי זה [tool-verified: `entity_registration` in
modeling.py lines 105-120]:

```sql
SELECT "id", "name", "region", "tier" FROM "raw"."customers"
-- registered as a bitemporal delta MV, entity key: ["id"]
```

### רישום ה-fact מכירות

=== "Admin UI"

    1. לחצו **+ Model** שוב.
    2. בחרו **Fact**.
    3. מלאו את הטופס:
       - **Name:** `Sales`
       - **Source relation:** `raw.orders`
       - **Domain:** *(הדומיין שלכם)*
       - **Grain:** `order_id`
       - **Measures:** `amount:sum, quantity:sum`
       - **Dimensions:** `Customer:customer_id`
    4. לחצו **Create**.

=== "GraphQL API"

    ```graphql
    mutation {
      registerFact(input: {
        name: "Sales"
        source: "raw.orders"
        domainId: "sales"
        grain: ["order_id"]
        measures: [
          { column: "amount", agg: "sum" }
          { column: "quantity", agg: "sum" }
        ]
        dimensions: [
          { entity: "Customer", via: "customer_id" }
        ]
      }) {
        success
        message
      }
    }
    ```

Provisa יוצרת ורושמת [tool-verified: `fact_registration` in modeling.py lines 123-141]:

```sql
SELECT "order_id", "customer_id",
       SUM("amount") AS "amount",
       SUM("quantity") AS "quantity"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id"
```

בתוספת קשר רשום אחד: `Sales.customer_id → Customer` (עוצמה (cardinality): many-to-one).
[tool-verified: `fact_table_input` in modeling_register.py lines 89-98, cardinality at line 95]

## טופס ה-Model (Admin UI)

הכפתור **+ Model** מופיע בעמוד **Tables** (tooltip: "Model an entity or fact (star
schema / Data Vault)"). [tool-verified: tablesPage.json line 13; TablesPage.tsx lines 441-450]

בקרה מקוטעת (segmented control) בראש המודל עוברת בין **Entity (dimension)** ו-**Fact**.
[tool-verified: ModelingForm.tsx lines 102-110]

### שדות Entity

[tool-verified: ModelingForm.tsx lines 141-171; modelingForm.json]

| שדה | נדרש | הערות |
| --- | --- | --- |
| Name | כן | שם ה-MV בקטלוג |
| Source relation | כן | relation מנוקד, למשל `raw.customers` |
| Domain | כן | הדומיין שאליו שייך ה-MV |
| Entity key | כן | עמודת/עמודות מפתח מופרדות-פסיק, למשל `id` |
| Attributes | לא | עמודות תכונה מופרדות-פסיק, למשל `name, region, tier` |
| History | לא | `none` / `scd2` / `snapshot`; ברירת מחדל `none` |

### שדות Fact

[tool-verified: ModelingForm.tsx lines 172-196; modelingForm.json]

| שדה | נדרש | הערות |
| --- | --- | --- |
| Name | כן | שם ה-MV בקטלוג |
| Source relation | כן | relation מנוקד, למשל `raw.orders` |
| Domain | כן | הדומיין שאליו שייך ה-MV |
| Grain | כן | עמודת/עמודות grain מופרדות-פסיק, למשל `order_id` |
| Measures | לא | זוגות `col:agg` מופרדים-פסיק, למשל `amount:sum, quantity:sum` |
| Dimensions | לא | זוגות `Entity:fk_column` מופרדים-פסיק, למשל `Customer:customer_id` |

כאשר `agg` מושמט במדד (`amount` במקום `amount:sum`), ברירת המחדל היא `sum`.
[tool-verified: ModelingForm.tsx line 73 `agg: agg || "sum"`]

## ה-GraphQL API

שתי המוטציות חיות בסכמת ה-admin. [tool-verified: schema_mutation.py lines 449-472]

### `registerEntity`

```graphql
mutation RegisterEntity($input: EntityInput!) {
  registerEntity(input: $input) {
    success
    message
  }
}
```

שדות `EntityInput` [tool-verified: types.py lines 449-456]:

| שדה | טיפוס | ברירת מחדל | תיאור |
| --- | --- | --- | --- |
| `name` | String | — | שם קטלוג עבור ה-MV entity |
| `source` | String | — | relation מקור (`schema.table` או במרכאות) |
| `domainId` | String | — | מזהה דומיין |
| `key` | [String] | — | עמודת/עמודות מפתח entity |
| `attributes` | [String] | `[]` | עמודות תכונה להקרנה |
| `history` | String | `"none"` | `"none"` \| `"scd2"` \| `"snapshot"` |
| `visibleTo` | [String] | `["public"]` | רשימת נראות תפקידים |

### `registerFact`

```graphql
mutation RegisterFact($input: FactInput!) {
  registerFact(input: $input) {
    success
    message
  }
}
```

שדות `FactInput` [tool-verified: types.py lines 472-479]:

| שדה | טיפוס | ברירת מחדל | תיאור |
| --- | --- | --- | --- |
| `name` | String | — | שם קטלוג עבור ה-MV fact |
| `source` | String | — | relation מקור |
| `domainId` | String | — | מזהה דומיין |
| `grain` | [String] | — | עמודת/עמודות grain עבור ה-GROUP BY |
| `measures` | [MeasureInput] | `[]` | זוגות `{ column, agg }` |
| `dimensions` | [DimRefInput] | `[]` | זוגות `{ entity, via }` |
| `visibleTo` | [String] | `["public"]` | רשימת נראות תפקידים |

`MeasureInput`: `{ column: String, agg: String }` — ברירת המחדל של agg היא `"sum"`.
[tool-verified: types.py lines 460-462]

`DimRefInput`: `{ entity: String, via: String }` — `entity` הוא שם ה-entity המופנה;
`via` היא עמודת ה-FK ב-source ה-fact.
[tool-verified: types.py lines 465-468]

בהצלחה, `registerFact` מחזיר הודעה בצורה:
`Fact 'Sales' registered with 1 dimension link(s)`.
[tool-verified: schema_mutation.py line 471]

## Star schema של Kimball ו-Data Vault

אף דפוס אינו דורש כלים נפרדים. אותם שני פרימיטיבים מרכיבים את שניהם.

### Star schema של Kimball

הליכה מודרכת זו בונה star בעל שלושה ממדים. שתי טבלאות מקור חדשות:

- `raw.products` — `product_id`, `name`, `category`, `list_price` [inferred: introduced for this example]
- `raw.date_spine` — `date_key`, `year`, `quarter`, `month` [inferred: introduced for this example]

`raw.orders` מקבלת גם עמודות `product_id` ו-`order_date` כאן. [inferred]

#### בחירת סוג SCD

מצב היסטוריה הוא החוגה היחידה בין SCD Type 1 ל-Type 2:

| סוג SCD | מצב היסטוריה | אפקט |
| --- | --- | --- |
| Type 1 (נוכחי-בלבד) | `none` | MV נבנה-מחדש ברענון; ללא היסטוריית שורה |
| Type 2 (עם גרסאות) | `scd2` | MV‏ delta בי-טמפורלי; כל שינוי מוסיף שורה חדשה מוקלדת על מפתח ה-entity |

[tool-verified: `_HISTORY_MODE` at modeling.py line 40; `entity_registration` history branch at
lines 115-119]

השתמשו ב-`scd2` כאשר שאילתות downstream צריכות לצרף ממד כפי-שהתקיים בזמן העסקה — ה-
tier של לקוח ברגע הרכישה, לא ה-tier הנוכחי שלו. השתמשו ב-`none` עבור בדיקות-מול-מקור יציבות.
date spine לעולם אינו משתנה. קטלוג מוצרים שבו אתם זקוקים רק למחיר הנוכחי יכול להיבנות-מחדש בכל
רענון.

#### החלטת Grain

ה-grain הוא רמת הפירוט הנמוכה ביותר שה-fact עונה עליה. `order_id` נותן שורה אחת לכל הזמנה,
שומר על היכולת לספור הזמנות ייחודיות ולצרף לכל ממד ברמת הזמנה. grain
גס יותר — נניח `["customer_id", "order_date"]` — מצבר-מראש (pre-aggregates) על פני הזמנות ומשליך
פרטים אלה לצמיתות. הצהירו את ה-grain הצר-ביותר שהעסק צריך; rollups גסים יותר זולים
לגזור אחר-כך.

#### רישום הממדים

**Customer** (SCD Type 2 — שינויי tier חייבים להישמר):

```graphql
mutation {
  registerEntity(input: {
    name: "Customer"
    source: "raw.customers"
    domainId: "sales"
    key: ["id"]
    attributes: ["name", "region", "tier"]
    history: "scd2"
  }) { success message }
}
```

יוצר MV‏ delta בי-טמפורלי מוקלד על `id` [tool-verified: entity_registration modeling.py
lines 105-120]:

```sql
SELECT "id", "name", "region", "tier" FROM "raw"."customers"
-- bitemporal delta MV, entity key: ["id"]
```

**Product** (SCD Type 1 — קטלוג נוכחי, אין צורך בהיסטוריית גרסאות):

```graphql
mutation {
  registerEntity(input: {
    name: "Product"
    source: "raw.products"
    domainId: "sales"
    key: ["product_id"]
    attributes: ["name", "category", "list_price"]
    history: "none"
  }) { success message }
}
```

יוצר MV רגיל הנבנה-מחדש ברענון [tool-verified: entity_registration modeling.py
lines 105-114; `mv_bitemporal_mode` is only added when `history != "none"`, line 115]:

```sql
SELECT "product_id", "name", "category", "list_price" FROM "raw"."products"
```

**DateDim** (ללא היסטוריה — תאריך הוא בלתי-משתנה):

```graphql
mutation {
  registerEntity(input: {
    name: "DateDim"
    source: "raw.date_spine"
    domainId: "sales"
    key: ["date_key"]
    attributes: ["year", "quarter", "month"]
    history: "none"
  }) { success message }
}
```

יוצר:

```sql
SELECT "date_key", "year", "quarter", "month" FROM "raw"."date_spine"
```

#### רישום ה-fact מכירות על פני שלושה ממדים

Grain: `order_id`. שלוש הפניות ממד — עמודת FK אחת לכל אחת. שני המדדים הם sums אדיטיביים.

```graphql
mutation {
  registerFact(input: {
    name: "Sales"
    source: "raw.orders"
    domainId: "sales"
    grain: ["order_id"]
    measures: [
      { column: "amount",   agg: "sum" }
      { column: "quantity", agg: "sum" }
    ]
    dimensions: [
      { entity: "Customer", via: "customer_id" }
      { entity: "Product",  via: "product_id"  }
      { entity: "DateDim",  via: "order_date"  }
    ]
  }) { success message }
}
```

Provisa מחשבת `group_cols = dedup([grain] + [dim FKs])`
= `["order_id", "customer_id", "product_id", "order_date"]` ויוצרת
[tool-verified: fact_registration modeling.py lines 125-131]:

```sql
SELECT "order_id", "customer_id", "product_id", "order_date",
       SUM("amount")   AS "amount",
       SUM("quantity") AS "quantity"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id", "product_id", "order_date"
```

שלושה קשרים נרשמים אוטומטית [tool-verified: modeling_register.py lines 89-98,
cardinality `"many_to_one"` at line 95]:

| קשר | עוצמה (Cardinality) |
| --- | --- |
| `Sales.customer_id → Customer` | many-to-one |
| `Sales.product_id → Product` | many-to-one |
| `Sales.order_date → DateDim` | many-to-one |

#### ממדים תואמים (Conformed dimensions)

ממד תואם נרשם פעם אחת ומופנה לפי שם ממספר בלתי-מוגבל של facts. נניח
ש-`raw.returns` מחזיק `return_id`, `customer_id`, `product_id`, ו-`amount`. ה-fact Returns עושה שימוש חוזר
ב-Customer ו-Product מבלי לרשום אותם מחדש:

```graphql
mutation {
  registerFact(input: {
    name: "Returns"
    source: "raw.returns"
    domainId: "sales"
    grain: ["return_id"]
    measures: [{ column: "amount", agg: "sum" }]
    dimensions: [
      { entity: "Customer", via: "customer_id" }
      { entity: "Product",  via: "product_id"  }
    ]
  }) { success message }
}
```

הן `Sales` והן `Returns` מפנים לאותם entities `Customer` ו-`Product`. נתיבי ה-join של Provisa
אוכפים ששאילתות דרך כל אחד מה-facts יחצו את אותה הגדרת ממד
[tool-verified: fact_registration uses entity name as `target_table` at modeling.py lines 138-140;
fact_table_input wires `target_table_id` from that name at modeling_register.py lines 91-93].

---

### Data Vault

אותם פרימיטיבים ממופים ישירות לאוצר המילים של Data Vault:

| Artifact‏ DV | פרימיטיב | היסטוריה |
| --- | --- | --- |
| Hub | `entity` | `none` — מפתחות entity בלבד |
| Satellite | `entity` | `scd2` או `snapshot` — היסטוריית תכונה לצד מפתח ה-hub |
| Link | `fact` ללא מדדים | — |
| Bridge / aggregate link | `fact` עם מדדים | — |

הדוגמה בונה vault מינימלי מעל `raw.customers` ו-`raw.orders`.

#### Hubs

hub מחזיק את מפתח ה-entity ולא-כלום מלבד זאת. `attributes: []` עם `history: "none"` מייצר
סט מפתחות נוכחי נטול-כפילויות; היסטוריית תכונה חיה כולה ב-satellite.

```graphql
mutation {
  registerEntity(input: {
    name: "CustomerHub"
    source: "raw.customers"
    domainId: "vault"
    key: ["id"]
    attributes: []
    history: "none"
  }) { success message }
}
```

יוצר [tool-verified: entity_registration modeling.py lines 107-108;
`cols = dedup([*key, *attributes])` = `["id"]` when `attributes=[]`]:

```sql
SELECT "id" FROM "raw"."customers"
```

```graphql
mutation {
  registerEntity(input: {
    name: "OrderHub"
    source: "raw.orders"
    domainId: "vault"
    key: ["order_id"]
    attributes: []
    history: "none"
  }) { success message }
}
```

יוצר:

```sql
SELECT "order_id" FROM "raw"."orders"
```

#### Satellite

ה-satellite יושב לצד מפתח ה-hub ונושא היסטוריית תכונה מלאה. השתמשו ב-`scd2` כדי להוסיף
רק שורות שהשתנו; השתמשו ב-`snapshot` כדי להחתים כל רענון מלא.

```graphql
mutation {
  registerEntity(input: {
    name: "CustomerSat"
    source: "raw.customers"
    domainId: "vault"
    key: ["id"]
    attributes: ["name", "region", "tier"]
    history: "scd2"
  }) { success message }
}
```

יוצר [tool-verified: entity_registration modeling.py lines 115-119;
`_HISTORY_MODE["scd2"]` = `"delta"` at modeling.py line 40]:

```sql
SELECT "id", "name", "region", "tier" FROM "raw"."customers"
-- bitemporal delta MV, entity key: ["id"]
```

`CustomerSat` ו-`CustomerHub` שניהם מוקלדים על `id`. ה-hub הוא יעד ה-join היציב; ה-satellite
מספק גישת-תכונה נקודתית-בזמן דרך השכבה הבי-טמפורלית.

#### Link (fact נטול-מדדים)

link מתעד אילו מפתחות hub הופיעו יחד — מפתחות בלבד, ללא מדדים. Provisa משמיטה את ה-`GROUP BY`
כאשר `measures` ריק [tool-verified: modeling.py lines 130-131:
`if f.measures: view_sql += " GROUP BY ..."`].

```graphql
mutation {
  registerFact(input: {
    name: "OrderCustomerLink"
    source: "raw.orders"
    domainId: "vault"
    grain: ["order_id"]
    measures: []
    dimensions: [
      { entity: "CustomerHub", via: "customer_id" }
      { entity: "OrderHub",    via: "order_id"    }
    ]
  }) { success message }
}
```

`group_cols = dedup(["order_id"] + ["customer_id", "order_id"])` = `["order_id", "customer_id"]`.
ללא מדדים, לכן ללא `GROUP BY`. יוצר [tool-verified: fact_registration modeling.py lines
125-131]:

```sql
SELECT "order_id", "customer_id" FROM "raw"."orders"
```

שני קשרים נרשמים: `OrderCustomerLink.customer_id → CustomerHub` ו-
`OrderCustomerLink.order_id → OrderHub`, שניהם many-to-one
[tool-verified: modeling_register.py lines 89-98].

#### Bridge / aggregate link

הוסיפו מדדים ל-link ו-Provisa פולטת את ה-`GROUP BY`, ומייצרת bridge מצובר-מראש. ב-
grain‏ `order_id` עם לקוח אחד לכל הזמנה, התוצאה היא שורה מצוברת אחת לכל הזמנה:

```graphql
mutation {
  registerFact(input: {
    name: "OrderSummary"
    source: "raw.orders"
    domainId: "vault"
    grain: ["order_id"]
    measures: [{ column: "amount", agg: "sum" }]
    dimensions: [
      { entity: "CustomerHub", via: "customer_id" }
      { entity: "OrderHub",    via: "order_id"    }
    ]
  }) { success message }
}
```

`group_cols = dedup(["order_id"] + ["customer_id", "order_id"])` = `["order_id", "customer_id"]`
(ה-`order_id` הכפול מרשימת הממדים מושמט על ידי `_dedup`). יוצר
[tool-verified: fact_registration modeling.py lines 125-131]:

```sql
SELECT "order_id", "customer_id", SUM("amount") AS "amount"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id"
```

המודל אינו מחליט את המתודולוגיה. Grain, תאימות (conformance), בחירת SCD, ופיצול
hub/satellite נשארים החלטות המעצב. Provisa מבצעת אותן. [tool-verified: modeling.py
docstring lines 25-26]

## מטריקות (Metrics) (REQ-1317, REQ-1318, REQ-1320)

**מטריקה (metric)** היא הגדרת אגרגט בעלת-שם, מנוהלת, ללא grain משלה. ה-grain — ה-
ממדים שהאגרגט מפורק לפיהם — נקשר בזמן שאילתה על ידי הקורא, לא בזמן ההגדרה.
זה ההבדל בין metric לתצוגה: תצוגה נועלת את ה-grain ביצירה; metric
נשארת פתוחה עד שנשאלת. [tool-verified: `Metric` class comment, `provisa/core/models.py` lines
452–455: "A named, governed aggregate definition with no grain of its own... grain is bound at
query time by the requested dimension set"]

### אובייקט ה-Metric

[tool-verified: `Metric` class, `provisa/core/models.py` lines 451–476]

| שדה | נדרש | הערות |
| --- | --- | --- |
| `name` | כן | snake_case, למשל `net_revenue`. מאומת: `[a-z][a-z0-9_]*` |
| `expression` | כן | SQL‏ ANSI מצטבר; חייב לכלול לפחות פונקציית אגרגציה אחת |
| `datatype` | לא | רמז טיפוס תוצאה, למשל `number`, `integer` |
| `description` | לא | הגדרה עסקית קריאה-לאדם |
| `ai_context` | לא | טקסט עבור צרכני AI — מוקרן ל-כלי MCP, pg_description, תיעוד GraphQL, וייצוא Ossie |
| `visible_to` | לא | רשימת תפקידים; ברירת מחדל `["*"]` (כל התפקידים) |
| `from_fact` | — | מוגדר אוטומטית כאשר ה-metric נוצר ממדד fact |

הפניות עמודה בתוך הביטוי חייבות להיות מוסמכות-טבלה (`orders.amount`, לא `amount`).
עמודה לא-מוסמכת היא שגיאה קשה בזמן הרחבה (expansion), לא אזהרה.
[tool-verified: `_expression_tables`, `provisa/compiler/metric_expand.py` lines 83–96]

מאגר ה-metric מאמת את הביטוי בכל כתיבה. ביטוי שאינו נפענח או
אינו מכיל פונקציית אגרגציה נדחה; הוא לעולם אינו נשמר.
[tool-verified: `validate_expression`, `provisa/core/repositories/metric.py` lines 34–43]

דוגמת רשומת תצורה:

```yaml
metrics:
  - name: net_revenue
    expression: "SUM(orders.amount) - SUM(orders.refunds)"
    datatype: number
    description: "Order revenue after refunds"
    ai_context: "Net revenue: total order amounts minus approved refunds. Use for P&L."
```

### שאילתת metric

הקומפיילר שומר את סכמת ה-`metrics`. [tool-verified: `METRICS_SCHEMA = "metrics"`,
`provisa/compiler/metric_expand.py` line 43] כל metric ניתן לכתובת כ-relation וירטואלי
בתוך אותה סכמה. שאלו אותה כמו טבלה — העמודות שאתם בוחרים הופכות לסט הממדים ו-
ה-GROUP BY:

```sql
-- Scalar total (no dimension)
SELECT value FROM metrics.net_revenue;

-- Broken out by region and month
SELECT region, month, value FROM metrics.net_revenue GROUP BY region, month;
```

הקומפיילר כותב זאת מחדש לאגרגט מקובץ אמיתי על פני טבלאות סמנטיות בסיסיות לפני שממשל
רץ, כך ש-RLS ומיסוך מוחלים על העמודות הממשיות.
[tool-verified: `expand_metric_query` docstring, `provisa/compiler/metric_expand.py` lines 263–276:
"BEFORE governance, so RLS/masking apply to the real columns (REQ-1317)"]

`SELECT *` מול relation מטריקה נדחה — ציינו את עמודות הממד ו-`value`
במפורש. [tool-verified: `expand_metric_query`, `provisa/compiler/metric_expand.py` lines 302–306]

כאשר ביטוי של metric חוצה מספר טבלאות, הקומפיילר מצרף אותן דרך קשרים רשומים. ממד
שהוא עמודה של טבלה מופנית-ישירות נפתר לאותה טבלה. ממד במרחק hop קשר
אחד מצורף אוטומטית. שני hops או ממד דו-משמעי הם שגיאה קשה המציינת בשם את העבריין.
[tool-verified: `_JoinPlan.resolve_dimension`, `provisa/compiler/metric_expand.py` lines 190–228]

### מטריקות מ-fact specs (REQ-1320)

כאשר אתם רושמים fact, כל מדד מוצהר רושם-אוטומטית אובייקט Metric תואם.
שדה `from_fact` של ה-metric רושם את שם טבלת ה-fact המקור, וממדי הקיבוץ
התקפים הם תכונות ה-entity הישיגות דרך קשרי ה-FK של ה-fact.
[tool-verified: `Metric.from_fact` comment, `provisa/core/models.py` line 466–467:
"set when this metric was auto-registered from a fact spec's measure";
`from_fact` stored in `provisa/core/repositories/metric.py` line 57]

מטריקות רשומות-אוטומטית מופיעות בעמוד Metrics עם badge של **fact**. תוכלו לערוך אותן כמו
כל metric אחרת. [tool-verified: `MetricsPage.tsx` lines 405–408:
`{m.fromFact && <Badge ... data-testid={`metrics-from-fact-${m.name}`}>...</Badge>}`]

### תצוגות מורכבות-מטריקה (view_metrics, REQ-1318)

תצוגת `view_metrics` נועלת את ה-grain של metric בזמן ההגדרה. הצהירו את שמות ה-metric,
עמודות הממד, ופילטרים אופציונליים; הקומפיילר יוצר את ה-SELECT.

[tool-verified: `ViewMetricsSpec`, `provisa/core/models.py` lines 479–492]

```yaml
tables:
  - source_id: pg1
    domain_id: sales
    schema: public
    table: monthly_revenue
    view_metrics:
      metrics: [net_revenue]
      dimensions: [region, month]
      filters: ["orders.status = 'completed'"]
```

הקומפיילר יוצר (עבור דוגמה זו):

```sql
SELECT orders.region AS region, orders.month AS month,
       SUM(orders.amount) - SUM(orders.refunds) AS net_revenue
FROM orders
WHERE orders.status = 'completed'
GROUP BY orders.region, orders.month
```

`view_metrics` ו-`view_sql` הם בלעדיים-הדדית (mutually exclusive) על אותה טבלה.
[tool-verified: `Table` model validator, `provisa/core/models.py` lines 614–617:
`if self.view_sql is not None and self.view_metrics is not None: raise ValueError(...)`]

**יצירה-מחדש אוטומטית בשינוי metric.** כאשר ביטוי של metric מעודכן, כל
תצוגת `view_metrics` המפנה אליה מתקמפלת-מחדש וה-SQL החדש נשמר מיידית.
התצוגה אינה יכולה לסטות מהגדרת ה-metric מבחינה מבנית.
[tool-verified: `regenerate_metric_views`, `provisa/api/admin/_metric_views.py` lines 79–117:
"each dependent view_metrics spec recompiles against the UPDATED metric set and the fresh SQL
is persisted"]

**קריאות `metric()` inline ב-SQL תצוגה חופשי.** `view_sql` כתוב-ידנית יכול גם להפנות
מטריקות דרך `metric('name')`. הקומפיילר מחליף כל קריאה בביטוי ה-metric ו-
רושם קשת lineage. זה נותן לתצוגות חופשיות את אותה תכונת קימפול-מחדש-בשינוי כאשר
הן מפנות ל-metric ולא מעתיקות את הנוסחה שלה.
[tool-verified: `expand_metric_calls_in_sql`, `provisa/compiler/metric_expand.py` lines 393–429]

הערה: תצוגות בנתיב-תצורה המשתמשות בקריאות `metric()` inline נוצרות-מחדש בטעינה מחדש של תצורה, לא ב-
upsert של metric. [tool-verified: `regenerate_metric_views` docstring, `_metric_views.py` lines 84–86:
"Free-hand view_sql born from inline metric() calls carries no stored provenance, so it is not
regenerated here (config-path views regenerate on config reload)"]

### עמוד ה-admin של Metrics (REQ-1323, REQ-1324)

פתחו את פריט הניווט **Metrics** כדי לנהל metrics מנוהלות. לחצו על שורה כדי להרחיב פאנל
פרטים read-only; לחצו **Edit** בתוכו כדי לעבור לעריכה inline (ללא modal). **New Metric** פותח
כרטיס יצירה inline מעל הטבלה. אישור המחיקה הוא ה-modal היחיד בעמוד.
[tool-verified: `MetricsPage.tsx` lines 214–216 comment: "REQ-1317: registered-metrics management
page (list / create / edit / delete). REQ-1323: detail-then-edit"]

טופס היצירה/עריכה מציע builder תלת-בורר עבור metrics המקורן ב-fact: בחרו את טבלת ה-
fact המקור (מסוננת ל-`modelingRole=fact`), עמודת מדד, ופונקציית אגרגציה
(`SUM`, `AVG`, `COUNT`, `MIN`, `MAX`). ה-datatype נגזר אוטומטית:
`COUNT → bigint`, `AVG → numeric`, `SUM/MIN/MAX → the measure column's type`.
תיבת הטקסט של הביטוי נשארת פתח-המילוט (escape hatch) עבור ביטויים שרירותיים.
[tool-verified: `deriveDatatype` function, `MetricsPage.tsx` lines 66–70;
`applyBuilder`, `MetricsPage.tsx` lines 273–285]

## התועלת של ה-IR

כל קריאת רישום עוברת דרך אותו נתיב כמו MV כתוב-ידנית. ה-spec entity/fact
הוא ייצוג ביניים (intermediate representation) — לא template, לא macro. ה-warehouse שאליו הוא מכוון הוא
תכונה של הפריסה, לא של המודל. שנו את מנוע היעד ואותן הצהרות `entity` /
`fact` ימומשו שם, מכיוון שה-SQL הנוצר והמצבים הבי-טמפורליים הם
נייטרליים-מנוע מבחינה מבנית. [tool-verified: modeling.py docstring lines 25-28;
modeling_register.py lines 56-66, 80-88]
