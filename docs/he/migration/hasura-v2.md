# מעבר מ-Hasura v2 ל-Provisa

## דרישות מוקדמות

1. מופע Hasura v2 פועל (v2.x) עם מטא-דאטה מיוצא.
2. יצוא מטא-דאטה באמצעות ה-CLI של Hasura:

   ```bash
   hasura metadata export --endpoint http://localhost:8080
   ```

   זה יוצר ספריית `metadata/` המכילה `sources.yaml`, `actions.yaml`,
   `cron_triggers.yaml`, `inherited_roles.yaml`, `remote_schemas.yaml`, וכו'.
3. Python 3.11+ עם חבילת `provisa` מותקנת.

## שימוש ב-CLI

```bash
python -m provisa.hasura_v2 <metadata-dir> -o provisa.yaml
```

### ארגומנטים

| ארגומנט | נדרש | תיאור |
| ---------- | ---------- | ------------- |
| `metadata_dir` | כן | נתיב לספריית המטא-דאטה המיוצאת של Hasura v2 |

### אפשרויות

| אפשרות | ברירת מחדל | תיאור |
| -------- | --------- | ------------- |
| `-o, --output FILE` | stdout | נתיב קובץ YAML של פלט |
| `--source-overrides FILE` | None | קובץ YAML עם דריסות חיבור לכל-מקור |
| `--domain-map KEY=VAL ...` | None | מיפויי סכמה-לדומיין (למשל `public=core hr=people`) |
| `--auth-env-file FILE` | None | נתיב לקובץ `.env` עם תצורת אימות JWT/admin-secret |
| `--dry-run` | כבוי | פענוח ואימות ללא כתיבת פלט |

### קובץ דריסות מקור

קובץ YAML עם מפתחות לפי שם מקור עם תכונות חיבור לדריסה:

```yaml
default:
  host: prod-db.example.com
  port: 5432
  database: myapp
  username: provisa_user
  password: "${env:PROD_DB_PASSWORD}"
```

### קובץ סביבת אימות

קובץ בסגנון `.env` המחזיק את תצורת אימות ה-Hasura להמרה. הממיר
ממפה:

- JWT עם `jwk_url` -> Provisa `provider: oauth`.
- JWT `claims_map` -> Provisa `role_mapping[]`.
- Admin secret -> Provisa `superuser`.
- אימות webhook -> אזהרה נפלטת (אין שווה-ערך ב-Provisa).

## מטריצת שוויון תכונות

| תכונת Hasura v2 | שווה-ערך Provisa | הערות |
| --- | --- | --- |
| **מקורות** (postgres, mysql, mssql, bigquery, citus) | `sources[]` | סוג ממופה: pg/postgres -> postgresql, mssql -> sqlserver. כתובת URL של החיבור מפוענחת ל-host/port/database/username/password. הגדרות pool נשמרות. |
| **טבלאות** (טבלאות עוקבות) | `tables[]` | שם סכמה + טבלה נשמר. `source_id` מקשר למקור. |
| **שמות טבלה מותאמים** (`custom_name`, `custom_root_fields.select`) | `tables[].alias` | הראשון שאינו null מבין `select`, `select_by_pk`, `custom_name`. |
| **שמות עמודה מותאמים** | `columns[].alias` | ממפה מילון `custom_column_names` לשיוכי עמודה. |
| **הרשאות בחירה** (עמודות, פילטר) | `columns[].visible_to[]`, `rls_rules[]` | רשימות עמודות הופכות ל-`visible_to`. עמודות תו-כללי (`*`) נתמכות. פילטרים מומרים ל-SQL דרך `bool_expr_to_sql`. |
| **הרשאות הוספה/עדכון** (עמודות) | `columns[].writable_by[]` | רשימות עמודות הופכות ל-`writable_by`. תפקידים משודרגים ליכולת `write`. |
| **הרשאות מחיקה** | שדרוג יכולת תפקיד | התפקיד מקבל יכולת `write`. אין מיפוי מחיקה לכל-טבלה. |
| **קשרי אובייקט** | `relationships[]` עם `cardinality: many-to-one` | שיוך עמודה נשמר. |
| **קשרי מערך** | `relationships[]` עם `cardinality: one-to-many` | שיוך עמודה נשמר. |
| **שדות מחושבים** | `functions[]` | ממופים ל-Function עם `returns` המצביע לטבלת האב. |
| **פונקציות עוקבות** | `functions[]` | `exposed_as` ברירת מחדל ל-mutation. סכמה נשמרת. |
| **Actions** (handler של פרוצדורה מאוחסנת) | `functions[]` | מומר ל-Function config כשמגובה על ידי פרוצדורה מאוחסנת. |
| **Actions** (handler של webhook) | לא מומר | אזהרה נפלטת, כולל כתובת ה-handler. |
| **Cron triggers** | לא מומר | אזהרה נפלטת. (טריגרים מתוזמנים בזמן-ריצה קיימים, אך הממיר אינו ממפה אותם.) |
| **Event triggers** | לא מומר | אזהרה נפלטת. (טריגרי אירועים בזמן-ריצה קיימים, אך הממיר אינו ממפה אותם.) |
| **תפקידים בירושה** | `roles[].parent_role_id` | התפקיד הראשון ב-`role_set` הופך להורה. כל התפקידים הצאצאים נוצרים. |
| **סכמות מרוחקות** | `sources[]` (`graphql_remote`) | נרשם כמקור `graphql_remote`. שם, URL, כותרות, ותצורת אימות נשמרים. |
| **טבלאות enum** | טבלה נוצרת | דגל `is_enum` אינו מועבר (אין שווה-ערך ב-Provisa). |
| **רשימות allow** | מדולג | לא קיים במודל המטא-דאטה. |

## שלבים לאחר-המרה

1. **סקרו את ה-YAML הפלט.** בדקו שמקורות, טבלאות, ותפקידים נראים נכונים.
2. **הגדירו חיבורי מקור.** הממיר מפענח כתובות URL של חיבור אך ברירת-מחדל
   ל-`localhost` בכשל פענוח. השתמשו ב-`--source-overrides` או ערכו את הפלט ישירות.
3. **אמתו שיוכי דומיין.** ללא `--domain-map`, כל הטבלאות נופלות ל-`default`.
   שייכו סכמות לדומיינים עם `--domain-map public=core analytics=reporting`.
4. **בדקו כללי RLS.** פילטרים מומרים לקירובי SQL. ביטויים
   בוליאניים מורכבים (`_and`/`_or`/`_exists` מקוננים) צריכים להיסקר ידנית.
5. **סקרו אזהרות.** הממיר מדפיס תקציר אזהרות ל-stderr עבור תכונות
   שהממיר אינו ממפה (event triggers, cron triggers, actions מגובות webhook).
6. **הגדירו אימות.** אם מופע ה-Hasura שלכם משתמש באימות JWT/webhook, צרו קובץ סביבת
   אימות והריצו מחדש עם `--auth-env-file`.
7. **בדקו.** הפעילו את שרת Provisa ואמתו שאילתות מול מקורות הנתונים שלכם.

## בעיות נפוצות ופתרון תקלות

### כתובת URL של חיבור לא מפוענחת

אם `database_url` של המקור הוא הפניה למשתנה סביבה (`{"from_env": "PG_URL"}`),
הממיר אינו יכול לפתור אותה בזמן ההמרה. למקור יהיו ערכי placeholder
(`host: localhost`, `database: default`). תקנו עם `--source-overrides`.

### עמודות תו-כללי

כאשר הרשאה מעניקה `columns: "*"`, הממיר יוצר רשומת עמודת תו-כללי
אחת. לאחר ההמרה, ייתכן שתרצו להחליף אותה ברשימות עמודה מפורשות
על ידי בדיקת סכמת מסד הנתונים בפועל.

### נאמנות טריגר אירוע

טריגרי אירועים מומרים עם `operations` ו-`webhook_url` אך ערבויות
משלוח ספציפיות ל-Hasura (exactly-once, משלוח-מחדש) אין להן שווה-ערך ישיר ב-Provisa.
סקרו את סעיף `event_triggers` והגדירו את תשתית ה-webhook שלכם בהתאם.

### תפקידים חסרים

תפקידים נאספים רק מרשומות הרשאה. אם תפקיד קיים ב-Hasura אך אין לו
הרשאות על אף טבלה או action, הוא לא יופיע בפלט.

### שדות שורש מותאמים

רק שדות שורש `select` ו-`select_by_pk` משמשים לשיוך הטבלה. שדות שורש
מותאמים אחרים (`select_aggregate`, `insert`, `update`, `delete`) אינם ממופים.

## דוגמה

המרת פרויקט Hasura v2 טיפוסי עם שתי סכמות ממופות לדומיינים:

```bash
# Export metadata from Hasura
hasura metadata export --endpoint http://localhost:8080

# Convert with domain mapping and source overrides
python -m provisa.hasura_v2 metadata/ \
  -o provisa.yaml \
  --domain-map public=core hr=people \
  --source-overrides overrides.yaml \
  --auth-env-file auth.env

# Dry run first to check for warnings
python -m provisa.hasura_v2 metadata/ --dry-run
```

מבנה פלט:

```yaml
sources:
  - id: default
    type: postgresql
    host: prod-db.example.com
    port: 5432
    database: myapp
    ...
domains:
  - id: core
  - id: people
tables:
  - source_id: default
    domain_id: core
    schema_name: public
    table_name: users
    columns:
      - name: id
        visible_to: [user, admin]
      - name: email
        visible_to: [admin]
        writable_by: [admin]
    alias: Users
roles:
  - id: admin
    capabilities: [read, write]
    domain_access: ["*"]
  - id: user
    capabilities: [read]
    domain_access: ["*"]
rls_rules:
  - table_id: default.public.users
    role_id: user
    filter: "id = x-hasura-user-id"
relationships:
  - id: default.public.orders.user
    source_table_id: default.public.orders
    target_table_id: default.public.users
    source_column: user_id
    target_column: id
    cardinality: many-to-one
```
