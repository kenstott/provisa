# מעבר מ-Hasura DDN‏ (v3) ל-Provisa

## דרישות מוקדמות

1. פרויקט Hasura DDN עם קבצי HML (סיומת `.hml`).
   לפרויקטי DDN בדרך-כלל יש מבנה ספריות כמו:

   ```text
   my-ddn-project/
     app/
       subgraph1/
         models/
           MyModel.hml
         commands/
           MyCommand.hml
       subgraph2/
         ...
     globals/
       ...
   ```

2. Python 3.11+ עם חבילת `provisa` מותקנת.

## שימוש ב-CLI

```bash
python -m provisa.ddn <hml-dir> -o provisa.yaml
```

### ארגומנטים

| ארגומנט | נדרש | תיאור |
| ---------- | ---------- | ------------- |
| `hml_dir` | כן | נתיב לספריית פרויקט DDN HML (נסרקת רקורסיבית עבור קבצי `.hml`) |

### אפשרויות

| אפשרות | ברירת מחדל | תיאור |
| -------- | --------- | ------------- |
| `-o, --output FILE` | stdout | נתיב קובץ YAML של פלט |
| `--source-overrides FILE` | None | קובץ YAML עם דריסות חיבור לכל-מקור |
| `--domain-map KEY=VAL ...` | None | מיפויי subgraph-לדומיין (למשל `app=core analytics=reporting`) |
| `--dry-run` | כבוי | פענוח ואימות ללא כתיבת פלט |

### קובץ דריסות מקור

קובץ YAML עם מפתחות לפי שם מחבר (לאחר סניטציה של מזהה: רווחים, נקודות, לוכסנים
הופכים לקווים-תחתיים) עם תכונות חיבור:

```yaml
my_pg_connector:
  host: prod-db.example.com
  port: 5432
  database: chinook
  username: provisa_user
  password: "${env:PROD_DB_PASSWORD}"
```

## מטריצת שוויון תכונות

| סוג DDN | שווה-ערך Provisa | הערות |
| --- | --- | --- |
| **DataConnectorLink** | `sources[]` | סוג מקור נגזר מ-URL המחבר (postgres, mysql, mssql, mongo, clickhouse, snowflake, bigquery). פרטי חיבור ברירת-מחדל ל-placeholders; השתמשו ב-`--source-overrides` כדי להגדיר ערכים ממשיים. |
| **ObjectType** | הגדרות עמודה על `tables[]` | שדות הופכים לעמודות. `dataConnectorTypeMapping.fieldMapping` פותר שמות שדה GraphQL לשמות עמודה פיזיים. |
| **Model** | `tables[]` | כל Model מייצר טבלה אחת. `source_id` מהמחבר, `table_name` מה-collection. `graphql_type_name` הופך ל-`alias`. Subgraph (וכך `domain_id`) נגזר ממבנה הספריה של הקובץ: רכיב הספרייה הראשון תחת שורש הפרויקט. |
| **Relationship** | `relationships[]` | טיפוס Object -> `many-to-one`, טיפוס Array -> `one-to-many`. שיוך שדה נפתר דרך חיפוש עמודה פיזי. |
| **TypePermissions** | `columns[].visible_to[]` | `allowedFields` קובע אילו תפקידים יכולים לראות כל עמודה. |
| **ModelPermissions** | `rls_rules[]` | פרדיקטי פילטר מומרים לסעיפי WHERE של SQL. תומך ב-`_eq`, `_neq`, `_gt`, `_lt`, `_gte`, `_lte`, `_in`, `_nin`, `_like`, `_is_null`, `_and`, `_or`, `_not`. הפניות משתנה session נשמרות כ-`${x-hasura-...}`. |
| **Command** | `functions[]` | הן פונקציות והן פרוצדורות ממופות. ארגומנטים, טיפוס החזרה, ושם שדה שורש GraphQL נשמרים. `domain_id` מוגדר מה-subgraph. |
| **AggregateExpression** | קובץ צד `provisa-aggregates.yaml` | count, count_distinct, ופונקציות אגרגט לכל-שדה נשמרות בקובץ צד ומומרות לתצורת אגרגט Provisa. |
| **BooleanExpressionType** | מדולג (בשקט) | משמש פנימית על ידי DDN לפילטור; אין צורך בשווה-ערך Provisa ישיר. |
| **AuthConfig** | מדולג (בשקט) | תצורת אימות DDN אינה ממופה; הגדירו אימות Provisa בנפרד. |
| **ScalarType** | מדולג | אזהרה נפלטת עם ספירה. |
| **GraphqlConfig** | מדולג | אזהרה נפלטת עם ספירה. |
| **CompatibilityConfig** | מדולג | אזהרה נפלטת עם ספירה. |
| **סוגים לא-מזוהים אחרים** | מדולגים | אזהרה נפלטת עם ספירה לכל סוג. |

## מושג מפתח: פתירת שדה GraphQL לעמודה פיזית

DDN מפריד את סכמת ה-GraphQL (שמות שדה) מסכמת מסד הנתונים הפיזית
(שמות עמודה) דרך `dataConnectorTypeMapping` על ObjectTypes. הממיר:

1. קורא רשומות `fieldMapping` משיוכי הטיפוס של כל ObjectType.
2. בונה חיפוש: `{graphql_field_name -> physical_column_name}`.
3. עבור שדות ללא שיוך מפורש, מניח ששם השדה שווה לשם העמודה.
4. משתמש בחיפוש זה בעת בניית עמודות, קשרים, וביטויי פילטר RLS.

משמעות הדבר שה-`provisa.yaml` הפלט משתמש ב-**שמות עמודה פיזיים** עבור `columns[].name`
ומגדיר `columns[].alias` לשם שדה ה-GraphQL כשהם שונים.

## שלבים לאחר-המרה

1. **סקרו את ה-YAML הפלט.** אמתו מקורות, טבלאות, ושיוכי עמודה.
2. **הגדירו חיבורי מקור.** מחברים מספקים רק רמז URL עבור זיהוי
   טיפוס. host/port/database/credentials ממשיים חייבים להיות מסופקים דרך
   `--source-overrides` או על ידי עריכת הפלט.
3. **אמתו שיוכי דומיין.** שמות Subgraph נגזרים ממבנה ספריה
   (רכיב הספרייה הראשון תחת שורש הפרויקט). ללא `--domain-map`, כל
   שם subgraph הופך למזהה דומיין ישירות. השתמשו ב-`--domain-map` כדי לשנות את שמם.
4. **בדקו כללי RLS.** פרדיקטי פילטר DDN מומרים לקירובי SQL.
   לוגיקה בוליאנית מקוננת (`_and`/`_or`/`_not`) נתמכת אך פילטרים
   חוצי-קשר מורכבים עשויים לדרוש סקירה ידנית.
5. **סקרו תצורת אגרגט.** ביטויי אגרגט נכתבים לקובץ צד
   `provisa-aggregates.yaml` ומומרים לתצורת אגרגט Provisa.
6. **סקרו אזהרות.** הממיר מדפיס תקציר ל-stderr המפרט
   סוגי DDN שדולגו וכל model המפנה ל-ObjectTypes לא-ידועים.
7. **בדקו.** הפעילו את שרת Provisa ואמתו שאילתות מול מקורות הנתונים שלכם.

## בעיות נפוצות ופתרון תקלות

### זיהוי סוג מקור נכשל

ה-URL של המחבר משמש היוריסטית (בודק מילות מפתח כמו "postgres",
"mysql", "mongo"). אם ה-URL אינו מכיל מילת מפתח מזוהה, המקור
ברירת-מחדל ל-`postgresql`. דרסו עם `--source-overrides`.

### ObjectType חסר עבור Model

אם Model מפנה לשם ObjectType שלא נמצא באף קובץ `.hml`,
הטבלה מדולגת ואזהרה נפלטת. ודאו שכל קבצי ה-HML כלולים
בספרייה הנסרקת.

### גילוי Subgraph

Subgraphs נגזרים ממבנה הספרייה: רכיב הספרייה הראשון
תחת שורש הפרויקט נלקח כשם ה-subgraph. שדה ה-`subgraph` בתוך
מסמכי HML אינו בשימוש. קבצים תחת ספריית `globals/` משוייכים ל-
subgraph‏ `globals` ומוחרגים מגילוי דומיין.

### פתירת מקור קשר

קשרים מפנים ל-`source_type` (שם ObjectType) ו-`target_model` (שם
Model). אם אף Model אינו משתמש ב-ObjectType הנתון, הקשר מדולג בשקט.

### aliases עמודה בכל מקום

אם פרויקט ה-DDN שלכם משתמש נרחבות ב-`fieldMapping`, ציפו שלרוב העמודות יהיה
`alias` בפלט. זו התנהגות נכונה -- `name` הוא העמודה הפיזית,
`alias` הוא שם ה-GraphQL שהאפליקציה שלכם השתמשה בו.

### ביטויי אגרגט

ביטויי אגרגט נשמרים בקובץ צד `provisa-aggregates.yaml` הנכתב
לצד הפלט ומומרים לתצורת אגרגט Provisa. הם אינם נשמרים על
ה-`description` של הטבלה.

## דוגמה: המרת פרויקט DDN‏ Chinook

```bash
# Convert the DDN project
python -m provisa.ddn ./chinook-ddn/ \
  -o provisa.yaml \
  --domain-map app=music \
  --source-overrides overrides.yaml

# Dry run to check warnings first
python -m provisa.ddn ./chinook-ddn/ --dry-run
```

מבנה פלט:

```yaml
sources:
  - id: chinook_pg
    type: postgresql
    host: prod-db.example.com
    port: 5432
    database: chinook
    ...
domains:
  - id: music
tables:
  - source_id: chinook_pg
    domain_id: music
    schema_name: public
    table_name: Album
    columns:
      - name: AlbumId
        visible_to: [admin, user]
      - name: Title
        visible_to: [admin, user]
      - name: ArtistId
        visible_to: [admin, user]
    alias: Albums
  - source_id: chinook_pg
    domain_id: music
    schema_name: public
    table_name: Artist
    columns:
      - name: artist_id
        visible_to: [admin, user]
        alias: ArtistId
      - name: artist_name
        visible_to: [admin, user]
        alias: Name
    alias: Artists
roles:
  - id: admin
    capabilities: [read]
    domain_access: ["*"]
  - id: user
    capabilities: [read]
    domain_access: ["*"]
relationships:
  - id: chinook_pg.public.Album.Artist
    source_table_id: chinook_pg.public.Album
    target_table_id: chinook_pg.public.Artist
    source_column: ArtistId
    target_column: artist_id
    cardinality: many-to-one
functions:
  - name: GetTopTracks
    source_id: chinook_pg
    schema_name: public
    function_name: get_top_tracks
    returns: Track
    domain_id: music
    description: "DDN function"
```
