<!-- markdownlint-disable MD046 -->
<!-- MD046 off: mkdocs-material `===` content-tab bodies are indented, which the linter
     misreads as indented code blocks; the fenced code blocks below are required for rendering. -->

# מידול נתונים (Entities & Facts) {: #data-modeling-entities-facts }

Provisa נותנת לכם שני פרימיטיבים דקלרטיביים — `entity` ו-`fact` — המכסים את אבני-
הבניין שכל star schema ו-Data Vault מורכבים מהם. הצהירו את ה-spec; Provisa מורידה אותו
בדיוק להגדרות ה-materialized-view, בי-טמפורליות (bitemporal), והקשר שהייתם אחרת
כותבים ידנית.

## מה הם entities ו-facts {: #what-entities-and-facts-are }

**entity** הוא הקרנה מוקלדת (keyed), נטולת-כפילויות, אופציונלית-היסטורית של relation מקור. אתם
נותנים לו שם, מכוונים אותו למקור, מצהירים את מפתח ה-entity והתכונות שברצונכם לשאת,
ובוחרים מצב היסטוריה. Provisa כותבת את ה-SQL של התצוגה ורושמת את ה-MV. כאשר היסטוריה
מופעלת, ה-MV הוא בי-טמפורלי.

**fact** הוא join למפתחות entity, מצומצם לגרעין (grain) מוצהר, עם מדדים (measures) מצוברים. Provisa
כותבת שאילתת MV מצטברת (`GROUP BY` grain + עמודות FK) ורושמת קשר עבור כל
קישור-ממד מוצהר. fact ללא מדדים הוא סט-מפתחות טהור — דפוס ה-link של Data Vault.

שני המבנים הם IR. ההגדרות שנוצרות מתאימות מחדש (retarget) על פני מנועים — ממומשות ב-Oracle,
Databricks, או נשארות וירטואליות מעל מנוע MPP — ללא מידול-מחדש.

## מצבי היסטוריה {: #history-modes }

שלושה מצבים זמינים על entity:

| מצב | משמעות | מצב בי-טמפורלי |
| --- | --- | --- |
| `none` | נוכחי-בלבד. ללא היסטוריה. | — |
| `scd2` | מעקב אחר כל שינוי. הוספת שורות שהשתנו בלבד (delta) מוקלדות על מפתח ה-entity. | `delta` |
| `snapshot` | מעקב אחר כל רענון. הוספת סט התוצאות המלא בכל רענון, מוחתם בזמן מערכת. | `snapshot` |

`scd2` דורש מפתח entity כדי לחשב את ה-delta. `snapshot` עובד על כל מנוע אך האחסון גדל
בעותק מלא לכל רענון. בחרו `scd2` עבור מקורות גדולים, איטיים-בשינוי; בחרו `snapshot` כאשר
אתם זקוקים להיסטוריה מלאה והמקור אינו יכול לספק מפתח.

ל-facts אין מצב היסטוריה — כיסוי טמפורלי מגיע מהיסטוריית ה-entity הבסיסית.

## מדדים (Measures) ואגרגציות {: #measures-and-aggregations }

מדדים מוצהרים כזוגות `column:agg`. אגרגציות נתמכות:

`sum` &nbsp;`avg` &nbsp;`min` &nbsp;`max` &nbsp;`count`

האגרגציה ברירת המחדל היא `sum`.

## דוגמה מפורטת: entity לקוח + fact מכירות {: #worked-example-customer-entity-sales-fact }

### טבלאות המקור {: #the-source-tables }

- `raw.customers` — id, name, region, tier
- `raw.orders` — order_id, customer_id, amount, quantity

### רישום ה-entity לקוח {: #register-the-customer-entity }

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

Provisa יוצרת ורושמת MV בי-טמפורלי זה:

```sql
SELECT "id", "name", "region", "tier" FROM "raw"."customers"
-- registered as a bitemporal delta MV, entity key: ["id"]
```

### רישום ה-fact מכירות {: #register-the-sales-fact }

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

Provisa יוצרת ורושמת:

```sql
SELECT "order_id", "customer_id",
       SUM("amount") AS "amount",
       SUM("quantity") AS "quantity"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id"
```

בתוספת קשר רשום אחד: `Sales.customer_id → Customer` (עוצמה (cardinality): many-to-one).

## טופס ה-Model (Admin UI) {: #the-model-form-admin-ui }

הכפתור **+ Model** מופיע בעמוד **Tables** (tooltip: "Model an entity or fact (star
schema / Data Vault)").

בקרה מקוטעת (segmented control) בראש המודל עוברת בין **Entity (dimension)** ו-**Fact**.

### שדות Entity {: #entity-fields }

| שדה | נדרש | הערות |
| --- | --- | --- |
| Name | כן | שם ה-MV בקטלוג |
| Source relation | כן | relation מנוקד, למשל `raw.customers` |
| Domain | כן | הדומיין שאליו שייך ה-MV |
| Entity key | כן | עמודת/עמודות מפתח מופרדות-פסיק, למשל `id` |
| Attributes | לא | עמודות תכונה מופרדות-פסיק, למשל `name, region, tier` |
| History | לא | `none` / `scd2` / `snapshot`; ברירת מחדל `none` |

### שדות Fact {: #fact-fields }

| שדה | נדרש | הערות |
| --- | --- | --- |
| Name | כן | שם ה-MV בקטלוג |
| Source relation | כן | relation מנוקד, למשל `raw.orders` |
| Domain | כן | הדומיין שאליו שייך ה-MV |
| Grain | כן | עמודת/עמודות grain מופרדות-פסיק, למשל `order_id` |
| Measures | לא | זוגות `col:agg` מופרדים-פסיק, למשל `amount:sum, quantity:sum` |
| Dimensions | לא | זוגות `Entity:fk_column` מופרדים-פסיק, למשל `Customer:customer_id` |

כאשר `agg` מושמט במדד (`amount` במקום `amount:sum`), ברירת המחדל היא `sum`.

## ה-GraphQL API {: #the-graphql-api }

שתי המוטציות חיות בסכמת ה-admin.

### `registerEntity`

```graphql
mutation RegisterEntity($input: EntityInput!) {
  registerEntity(input: $input) {
    success
    message
  }
}
```

שדות `EntityInput`:

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

שדות `FactInput`:

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

`DimRefInput`: `{ entity: String, via: String }` — `entity` הוא שם ה-entity המופנה;
`via` היא עמודת ה-FK ב-source ה-fact.

בהצלחה, `registerFact` מחזיר הודעה בצורה:
`Fact 'Sales' registered with 1 dimension link(s)`.

## Star schema של Kimball ו-Data Vault {: #kimball-star-schema-and-data-vault }

אף דפוס אינו דורש כלים נפרדים. אותם שני פרימיטיבים מרכיבים את שניהם.

### Star schema של Kimball {: #kimball-star-schema }

הליכה מודרכת זו בונה star בעל שלושה ממדים. שתי טבלאות מקור חדשות:

- `raw.products` — `product_id`, `name`, `category`, `list_price`
- `raw.date_spine` — `date_key`, `year`, `quarter`, `month`

`raw.orders` מקבלת גם עמודות `product_id` ו-`order_date` כאן.

#### בחירת סוג SCD {: #choosing-scd-type }

מצב היסטוריה הוא החוגה היחידה בין SCD Type 1 ל-Type 2:

| סוג SCD | מצב היסטוריה | אפקט |
| --- | --- | --- |
| Type 1 (נוכחי-בלבד) | `none` | MV נבנה-מחדש ברענון; ללא היסטוריית שורה |
| Type 2 (עם גרסאות) | `scd2` | MV‏ delta בי-טמפורלי; כל שינוי מוסיף שורה חדשה מוקלדת על מפתח ה-entity |

השתמשו ב-`scd2` כאשר שאילתות downstream צריכות לצרף ממד כפי-שהתקיים בזמן העסקה — ה-
tier של לקוח ברגע הרכישה, לא ה-tier הנוכחי שלו. השתמשו ב-`none` עבור בדיקות-מול-מקור יציבות.
date spine לעולם אינו משתנה. קטלוג מוצרים שבו אתם זקוקים רק למחיר הנוכחי יכול להיבנות-מחדש בכל
רענון.

#### החלטת Grain {: #grain-decision }

ה-grain הוא רמת הפירוט הנמוכה ביותר שה-fact עונה עליה. `order_id` נותן שורה אחת לכל הזמנה,
שומר על היכולת לספור הזמנות ייחודיות ולצרף לכל ממד ברמת הזמנה. grain
גס יותר — נניח `["customer_id", "order_date"]` — מצבר-מראש (pre-aggregates) על פני הזמנות ומשליך
פרטים אלה לצמיתות. הצהירו את ה-grain הצר-ביותר שהעסק צריך; rollups גסים יותר זולים
לגזור אחר-כך.

#### רישום הממדים {: #register-the-dimensions }

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

יוצר MV‏ delta בי-טמפורלי מוקלד על `id`:

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

יוצר MV רגיל הנבנה-מחדש ברענון:

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

#### רישום ה-fact מכירות על פני שלושה ממדים {: #register-the-sales-fact-across-three-dimensions }

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
:

```sql
SELECT "order_id", "customer_id", "product_id", "order_date",
       SUM("amount")   AS "amount",
       SUM("quantity") AS "quantity"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id", "product_id", "order_date"
```

שלושה קשרים נרשמים אוטומטית:

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
.

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

יוצר:

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

יוצר:

```sql
SELECT "id", "name", "region", "tier" FROM "raw"."customers"
-- bitemporal delta MV, entity key: ["id"]
```

`CustomerSat` ו-`CustomerHub` שניהם מוקלדים על `id`. ה-hub הוא יעד ה-join היציב; ה-satellite
מספק גישת-תכונה נקודתית-בזמן דרך השכבה הבי-טמפורלית.

#### Link (fact נטול-מדדים) {: #link-measureless-fact }

link מתעד אילו מפתחות hub הופיעו יחד — מפתחות בלבד, ללא מדדים. Provisa משמיטה את ה-`GROUP BY`
כאשר `measures` ריק.

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
ללא מדדים, לכן ללא `GROUP BY`. יוצר:

```sql
SELECT "order_id", "customer_id" FROM "raw"."orders"
```

שני קשרים נרשמים: `OrderCustomerLink.customer_id → CustomerHub` ו-
`OrderCustomerLink.order_id → OrderHub`, שניהם many-to-one
.

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
:

```sql
SELECT "order_id", "customer_id", SUM("amount") AS "amount"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id"
```

המודל אינו מחליט את המתודולוגיה. Grain, תאימות (conformance), בחירת SCD, ופיצול
hub/satellite נשארים החלטות המעצב. Provisa מבצעת אותן.

## מטריקות (Metrics)

**מטריקה (metric)** היא הגדרת אגרגט בעלת-שם, מנוהלת, ללא grain משלה. ה-grain — ה-
ממדים שהאגרגט מפורק לפיהם — נקשר בזמן שאילתה על ידי הקורא, לא בזמן ההגדרה.
זה ההבדל בין metric לתצוגה: תצוגה נועלת את ה-grain ביצירה; metric
נשארת פתוחה עד שנשאלת.

### אובייקט ה-Metric {: #the-metric-object }

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

מאגר ה-metric מאמת את הביטוי בכל כתיבה. ביטוי שאינו נפענח או
אינו מכיל פונקציית אגרגציה נדחה; הוא לעולם אינו נשמר.

דוגמת רשומת תצורה:

```yaml
metrics:
  - name: net_revenue
    expression: "SUM(orders.amount) - SUM(orders.refunds)"
    datatype: number
    description: "Order revenue after refunds"
    ai_context: "Net revenue: total order amounts minus approved refunds. Use for P&L."
```

### שאילתת metric {: #querying-a-metric }

הקומפיילר שומר את סכמת ה-`metrics`. כל metric ניתן לכתובת כ-relation וירטואלי
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

`SELECT *` מול relation מטריקה נדחה — ציינו את עמודות הממד ו-`value`
במפורש.

כאשר ביטוי של metric חוצה מספר טבלאות, הקומפיילר מצרף אותן דרך קשרים רשומים. ממד
שהוא עמודה של טבלה מופנית-ישירות נפתר לאותה טבלה. ממד במרחק hop קשר
אחד מצורף אוטומטית. שני hops או ממד דו-משמעי הם שגיאה קשה המציינת בשם את העבריין.

### מטריקות מ-fact specs {: #metrics-from-fact-specs }

כאשר אתם רושמים fact, כל מדד מוצהר רושם-אוטומטית אובייקט Metric תואם.
שדה `from_fact` של ה-metric רושם את שם טבלת ה-fact המקור, וממדי הקיבוץ
התקפים הם תכונות ה-entity הישיגות דרך קשרי ה-FK של ה-fact.

מטריקות רשומות-אוטומטית מופיעות בעמוד Metrics עם badge של **fact**. תוכלו לערוך אותן כמו
כל metric אחרת.

### תצוגות מורכבות-מטריקה (view_metrics) {: #metric-composed-views-view_metrics-req-1318 }

תצוגת `view_metrics` נועלת את ה-grain של metric בזמן ההגדרה. הצהירו את שמות ה-metric,
עמודות הממד, ופילטרים אופציונליים; הקומפיילר יוצר את ה-SELECT.

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

**יצירה-מחדש אוטומטית בשינוי metric.** כאשר ביטוי של metric מעודכן, כל
תצוגת `view_metrics` המפנה אליה מתקמפלת-מחדש וה-SQL החדש נשמר מיידית.
התצוגה אינה יכולה לסטות מהגדרת ה-metric מבחינה מבנית.

**קריאות `metric()` inline ב-SQL תצוגה חופשי.** `view_sql` כתוב-ידנית יכול גם להפנות
מטריקות דרך `metric('name')`. הקומפיילר מחליף כל קריאה בביטוי ה-metric ו-
רושם קשת lineage. זה נותן לתצוגות חופשיות את אותה תכונת קימפול-מחדש-בשינוי כאשר
הן מפנות ל-metric ולא מעתיקות את הנוסחה שלה.

הערה: תצוגות בנתיב-תצורה המשתמשות בקריאות `metric()` inline נוצרות-מחדש בטעינה מחדש של תצורה, לא ב-
upsert של metric.

### עמוד ה-admin של Metrics {: #the-metrics-admin-page }

פתחו את פריט הניווט **Metrics** כדי לנהל metrics מנוהלות. לחצו על שורה כדי להרחיב פאנל
פרטים read-only; לחצו **Edit** בתוכו כדי לעבור לעריכה inline (ללא modal). **New Metric** פותח
כרטיס יצירה inline מעל הטבלה. אישור המחיקה הוא ה-modal היחיד בעמוד.

טופס היצירה/עריכה מציע builder תלת-בורר עבור metrics המקורן ב-fact: בחרו את טבלת ה-
fact המקור (מסוננת ל-`modelingRole=fact`), עמודת מדד, ופונקציית אגרגציה
(`SUM`, `AVG`, `COUNT`, `MIN`, `MAX`). ה-datatype נגזר אוטומטית:
`COUNT → bigint`, `AVG → numeric`, `SUM/MIN/MAX → the measure column's type`.
תיבת הטקסט של הביטוי נשארת פתח-המילוט (escape hatch) עבור ביטויים שרירותיים.

## התועלת של ה-IR {: #the-ir-payoff }

כל קריאת רישום עוברת דרך אותו נתיב כמו MV כתוב-ידנית. ה-spec entity/fact
הוא ייצוג ביניים (intermediate representation) — לא template, לא macro. ה-warehouse שאליו הוא מכוון הוא
תכונה של הפריסה, לא של המודל. שנו את מנוע היעד ואותן הצהרות `entity` /
`fact` ימומשו שם, מכיוון שה-SQL הנוצר והמצבים הבי-טמפורליים הם
נייטרליים-מנוע מבחינה מבנית.
