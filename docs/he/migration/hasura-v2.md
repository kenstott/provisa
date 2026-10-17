# מעבר מ-Hasura v2 ל-Provisa {: #migrating-from-hasura-v2-to-provisa }

## דרישות מוקדמות {: #prerequisites }

1. מופע Hasura v2 (v2.x) פעיל עם מטא-דאטה מיוצא.
2. ייצאו את המטא-דאטה באחד משני המבנים:

   **ספריית המטא-דאטה של ה-CLI:**

   ```bash
   hasura metadata export --endpoint http://localhost:8080
   ```

   פעולה זו יוצרת ספריית `metadata/`. `databases/databases.yaml` מפרט כל מקור עם סוגו
   וחיבורו ומפנה לטבלאותיו ולפונקציותיו דרך אינדקסי `!include`;
   `actions.yaml`, `cron_triggers.yaml`, `inherited_roles.yaml`, `remote_schemas.yaml` ושאר
   הקבצים נמצאים לצדו. גם המבנה השטוח הישן (`tables.yaml` בשורש)
   מומר.

   **מסמך ה-API היחיד של המטא-דאטה:**

   ```bash
   curl -X POST http://localhost:8080/v1/metadata \
     -H "X-Hasura-Admin-Secret: <secret>" \
     -d '{"type":"export_metadata","args":{}}' > metadata.json
   ```

   גם האובייקט החשוף וגם המעטפת `{resource_version, metadata}` פועלים. ייבוא הניהול
   (Admin, Import Hasura Config) מקבל את הצורה הזו; ממיר שורת הפקודה שלהלן
   קורא ספריות.
3. Python 3.11+ עם חבילת `provisa` מותקנת.

## שימוש ב-CLI {: #cli-usage }

```bash
python -m provisa.hasura_v2 <metadata-dir> -o provisa.yaml
```

### ארגומנטים {: #arguments }

| ארגומנט | נדרש | תיאור |
| ---------- | ---------- | ------------- |
| `metadata_dir` | כן | נתיב לספריית המטא-דאטה המיוצאת של Hasura v2 |

### אפשרויות {: #options }

| אפשרות | ברירת מחדל | תיאור |
| -------- | --------- | ------------- |
| `-o, --output FILE` | stdout | נתיב קובץ YAML של פלט |
| `--source-overrides FILE` | None | קובץ YAML עם דריסות חיבור לכל-מקור |
| `--domain-map KEY=VAL ...` | None | מיפויי סכמה-לדומיין (למשל `public=core hr=people`) |
| `--auth-env-file FILE` | None | נתיב לקובץ `.env` עם תצורת אימות JWT/admin-secret |
| `--dry-run` | כבוי | פענוח ואימות ללא כתיבת פלט |

### קובץ דריסות מקור {: #source-overrides-file }

קובץ YAML עם מפתחות לפי שם מקור עם תכונות חיבור לדריסה:

```yaml
default:
  host: prod-db.example.com
  port: 5432
  database: myapp
  username: provisa_user
  password: "${env:PROD_DB_PASSWORD}"
```

### קובץ סביבת אימות {: #auth-env-file }

קובץ בסגנון `.env` המחזיק את תצורת אימות ה-Hasura להמרה. הממיר
ממפה:

- JWT עם `jwk_url` -> Provisa `provider: oauth`.
- JWT `claims_map` -> Provisa `role_mapping[]`.
- Admin secret -> Provisa `superuser`.
- אימות webhook -> אזהרה נפלטת (אין שווה-ערך ב-Provisa).

## הדוח {: #the-report }

ההמרה מסתיימת בדוח בשלושה חלקים:

1. **הועבר כמות שהוא.** לאובייקט יש מקבילה ב-Provisa והוא מומר בלי שינוי משמעות.
2. **מופה ליכולת טובה יותר של Provisa.** כל פריט אומר מדוע היכולת של Provisa עושה את העבודה טוב יותר.
3. **לעשות מחדש ידנית.** כל פריט אומר מה לעשות. אלה האזהרות.

ממיר שורת הפקודה מדפיס את הדוח ל-stderr בשם `Carried as`, `Mapped to` ו-`Redo by hand`, כל אחד עם ספירה, כשנשאר משהו לעשות מחדש. ייבוא הניהול מחזיר את שלוש הרשימות עם התצוגה המקדימה.

## מטריצת שוויון יכולות {: #feature-parity-matrix }

| יכולת ב-Hasura v2 | חלק | מקבילה ב-Provisa | הערות |
| --- | --- | --- | --- |
| **מקורות** (postgres, mysql, mssql, bigquery, citus) | הועבר | `sources[]` | הסוג ממופה: pg/postgres -> postgresql, mssql -> sqlserver. URL חיבור שכתוב במפורש מפוענח ל-host/port/database/username/password. גודל ה-pool נשמר. URL שנקרא ממשתנה סביבה אינו מנוחש (ראו להלן). |
| **טבלאות** (טבלאות עוקבות) | הועבר | `tables[]` | הסכמה ושם הטבלה נשמרים. `source_id` מקשר למקור. |
| **שמות טבלה מותאמים** (`custom_name`, `custom_root_fields.select`) | הועבר | `tables[].alias` | הערך הראשון שאינו null מבין `select`, `select_by_pk`, `custom_name`. |
| **שמות עמודה מותאמים** (`column_config`, `custom_column_names` הישן) | הועבר | `columns[].alias` | השם המותאם של כל עמודה הופך לכינוי שלה. |
| **שמות שדות שורש נוספים** (`select_by_pk`, `select_aggregate`, `insert`, ...) | מופה | השדה האחד של הטבלה | מדווחים בשמם. קריאה לפי מפתח היא מסנן על הטבלה, אגרגט הוא השדה `_aggregate` שלה, כתיבה היא המוטציה שלה. |
| **הרשאות select** (עמודות, מסנן) | הועבר | `columns[].visible_to[]`, `rls_rules[]` | רשימות עמודות הופכות ל-`visible_to`; `*` נתמך. מסננים הופכים ל-SQL; משתני הפעלה הופכים ל-`current_setting('provisa.<name>')`. |
| **מסננים דרך relationships** | הועבר | תת-שאילתות `EXISTS` ב-`rls_rules[]` | הכלל עוקב אחר ה-relationship עד הטבלה שלה. |
| **הרשאות insert, update ו-delete** | הועבר | `tables[].write_grants[]` | הרשאה אחת לכל תפקיד ופעולה: עמודות מ-`columns`, `rows` מ-`filter`, `check` מ-`check`, presets מ-`set`. תפקידים מקבלים את הזכות `write`. |
| **תפקיד admin** | הועבר | `org_admin` | מחזיק בכל הרשאה על כל עמודה ושורה, ושמו מופיע בכל עמודה מיובאת. |
| **Object relationships** | הועבר | `relationships[]` עם `cardinality: many-to-one` | מאושרות. שדה ה-GraphQL הוא שם ה-relationship של Hasura. |
| **Array relationships** | הועבר | `relationships[]` עם `cardinality: one-to-many` | מאושרות. שדה ה-GraphQL הוא שם ה-relationship של Hasura. |
| **תפקידים בירושה** (תפקיד אחד) | הועבר | `roles[].parent_role_id` | התפקיד הופך להורה. |
| **תפקידים בירושה** (כמה תפקידים) | מופה | תפקיד שמחזיק את איחוד הרשאותיהם | לתפקידים ב-Provisa הורה אחד, ולכן האיחוד נכתב במפורש על התפקיד. |
| **Computed fields** | לעשות מחדש | עמודת view | הייצוא נוקב בפונקציה, לא בגוף שלה. הדוח נותן את ה-`SELECT` להוספה. |
| **פונקציות עוקבות** | הועבר, אחרת לעשות מחדש | commands של `functions[]` | התצוגה המקדימה קוראת את החתימה ממסד הנתונים. בלעדיה, הוסיפו את הפונקציה בדף Commands. |
| **Actions** | מופה | commands שקוראים ל-handler | `{{VAR}}` ב-URL של ה-handler נכתב `${env:VAR}` ונפתר כשה-command נקרא. |
| **Cron triggers** | הועבר | `scheduled_triggers[]` | ביטוי cron, URL, payload והדגל enabled. ניסיונות חוזרים אינם מועברים (אזהרה). |
| **Event triggers** | מופה | `scheduled_triggers[]` על שינוי הטבלה | נורה לכל אצוות שינויים בכל שינוי. ניסיונות חוזרים אינם מועברים (אזהרה). |
| **Remote schemas** | מופה | `sources[]` (`graphql_remote`) שנוחת כטבלאות | שם, URL, כותרות ואימות נשמרים; שדות שורש הופכים לטבלאות מנוהלות. |
| **טבלאות enum** | הועבר | טבלה נוצרה | הדגל `is_enum` אינו מועבר (אין מקבילה ב-Provisa). |
| **Allow lists, query collections, נקודות קצה REST** | לעשות מחדש | אין | מדווחים כשהם קיימים בייצוא מאוחד; אינם מומרים. |

### Relationships {: #relationships }

- שדה ה-GraphQL של כל relationship הוא שם Hasura. כשכבר יש לטבלה שדה בשם הזה, ה-relationship לוקחת את השם ש-Provisa נותנת לה והדוח מזהיר.
- שתי relationships שמצהירות על אותו join ממוזגות לאחת. הדוח אומר איזו מוזגה לאיזו.
- relationship שמצטרפת על כמה זוגות עמודות מקבלת אזהרה ואינה נוצרת, כי קשר ב-Provisa מצטרף על זוג אחד. כללי שורות דרכה מומרים בכל זאת.
- object relationship שהוצהרה רק על ידי עמודת מפתח זר נפתרת דרך ה-array relationship ההפוכה; בלעדיה היא מקבלת אזהרה.

### הרשאות כתיבה {: #write-grants }

הרשאה מציינת מה תפקיד רשאי לכתוב לטבלה בפעולה אחת. `filter` או `check` ריק פירושו כל שורה (`TRUE`). ערך `set` של `X-Hasura-User-Id` הופך ל-preset מסוג `session` של `hasura_user_id`; כל ערך אחר הופך ל-preset מסוג `literal`. עמודת preset אינה מופיעה גם בעמודות ההרשאה.

### Actions, פונקציות עוקבות ו-triggers {: #actions-tracked-functions-and-triggers }

- **Action** הופך ל-command שקורא ל-handler שלו, גלוי לתפקידים שהרשאותיו נוקבות בהם (ל-`org_admin` בלבד כשאינן נוקבות באף אחד). action אסינכרוני מקבל אזהרה: ה-command ממתין לתשובת ה-handler.
- **פונקציה עוקבת** הופכת ל-command כשהתצוגה המקדימה יכולה לקרוא את הארגומנטים שלה ואת טבלת ההחזרה שלה ממקור PostgreSQL. במצב לא מקוון, ולכל פונקציה שחסרה, עמוסה או מחזירה טבלה שהייבוא אינו רושם, היא עוברת ל"לעשות מחדש".
- **Event trigger** נורה בכל שינוי בטבלה שלו, פעם אחת לכל אצווה, ושולח את הטבלה ואת השורות ששונו במקום אירוע לכל שורה עם ערכים ישנים וחדשים. trigger שהאזין לפחות משלוש הפעולות מקבל אזהרה.

## שלבים לאחר ההמרה {: #post-conversion-steps }

1. **עברו על הדוח.** התחילו ב-`Redo by hand`.
2. **הגדירו את חיבורי המקורות.** חיבור שהייצוא כותב במפורש מומר כפי שהוא. חיבור שנקרא ממשתנה סביבה מגיע ריק. השתמשו ב-`--source-overrides` או ערכו את הפלט ישירות.
3. **אמתו את שיוכי התחומים.** ללא `--domain-map`, כל הטבלאות נוחתות ב-`default`.
   שייכו סכמות לתחומים עם `--domain-map public=core analytics=reporting`.
4. **בדקו את כללי השורות.** מסננים מומרים ל-SQL. עברו על ביטויים בוליאניים
   מורכבים (`_and`/`_or`/`_exists` מקוננים) ועל מסננים דרך relationships.
5. **עשו מחדש את מה שמפורט.** Computed fields, פונקציות עוקבות שהתצוגה המקדימה לא יכלה לקרוא,
   ניסיונות חוזרים ב-triggers ואימות webhook אין להם המרה אוטומטית.
6. **הגדירו אימות.** אם מופע ה-Hasura שלכם משתמש באימות JWT/webhook, צרו קובץ
   סביבת אימות והריצו שוב עם `--auth-env-file`.
7. **שלחו את משתני ההפעלה.** הגדירו את ספק הזהות כך שהטוקן יישא כל משתנה הפעלה שהדוח מפרט תחת "הועבר אל" (למשל `x-hasura-user-id`, ברמה העליונה או בתוך אובייקט ה-claims של Hasura), או תעדו אותו לכל חבר כתכונה.
8. **בדקו.** הפעילו את שרת Provisa ואמתו שאילתות מול מקורות הנתונים שלכם.

## בעיות נפוצות ופתרון תקלות {: #common-issues-and-troubleshooting }

### החיבור מגיע ממשתנה סביבה {: #connection-comes-from-an-environment-variable }

אם ה-`database_url` של המקור הוא הפניה למשתנה סביבה (`{"from_env": "PG_URL"}`),
ערכו אינו בייצוא. הממיר אינו מנחש אחד: למקור יש host, מסד נתונים,
משתמש וסיסמה ריקים, והדוח מפרט אותו תחת `Redo by hand`.
הזינו את החיבור בהגדרות המקורות של הייבוא (או עם `--source-overrides`) והמירו
שוב.

### עמודות תו-כללי {: #wildcard-columns }

כאשר הרשאה מעניקה `columns: "*"`, הממיר יוצר רשומת עמודת תו-כללי
אחת. לאחר ההמרה, ייתכן שתרצו להחליף אותה ברשימות עמודה מפורשות
על ידי בדיקת סכמת מסד הנתונים בפועל.

### נאמנות של event triggers {: #event-trigger-fidelity }

Event trigger הופך ל-trigger על שינויי הטבלה שלו, שקורא לאותו webhook. הוא נורה
לכל אצוות שינויים בכל שינוי, בין שהשינוי הגיע דרך Provisa ובין שהגיע ישירות למסד הנתונים, ושולח את הטבלה ואת
השורות ששונו. הערכים הישנים והחדשים לכל שורה, הסינון לפי פעולה והניסיונות החוזרים של Hasura אינם מועברים;
הדוח מזהיר על כל אחד מהם שרלוונטי.
דאגו שה-handler יפעל על השורות שאליהן נועד, ונסו שוב בתוכו אם קריאה שהוחמצה חשובה.

### תפקידים חסרים {: #missing-roles }

תפקידים נאספים מהרשאות טבלה, מהרשאות action ומתפקידים בירושה. אם תפקיד קיים ב-Hasura
אך אינו מופיע באף אחד מהם, הוא לא יופיע בפלט.

### שדות שורש מותאמים {: #custom-root-fields }

שמות שדות השורש `select` ו-`select_by_pk` נותנים את כינוי הטבלה. שאר שדות השורש המותאמים
(`select_aggregate`, `insert`, `update`, `delete`) מדווחים בשמם: Provisa מגישה
טבלה כשדה אחד, עם אגרגטים וכתיבות כשדה `_aggregate` שלה וכמוטציות שלה.

## דוגמה {: #example }

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

מבנה הפלט (מקוצר):

```yaml
sources:
  - id: default
    type: postgresql
    host: prod-db.example.com
    port: 5432
    database: myapp
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
        visible_to: [org_admin, user]
      - name: email
        visible_to: [org_admin, user]
    write_grants:
      - role: user
        operation: update
        columns: [email]
        rows: "id = current_setting('provisa.hasura_user_id')"
        check: "TRUE"
      - role: org_admin
        operation: insert
        columns: [id, email]
        check: "TRUE"
      - role: org_admin
        operation: update
        columns: [id, email]
        rows: "TRUE"
        check: "TRUE"
      - role: org_admin
        operation: delete
        rows: "TRUE"
roles:
  - id: user
    capabilities: [query_development, write]
    domain_access: ["*"]
rls_rules:
  - table_id: users
    role_id: user
    filter: "id = current_setting('provisa.hasura_user_id')"
relationships:
  - id: orders.user
    source_table_id: orders
    target_table_id: users
    source_column: user_id
    target_column: id
    cardinality: many-to-one
    graphql_alias: user
```
