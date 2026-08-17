# ייבוא מ-Hasura

Provisa יכולה להמיר metadata קיים של Hasura ל-`config.yaml` של Provisa, תוך שימור טבלאות עקובות, קשרים (relationships), הרשאות, וסכמות מרוחקות (remote schemas).

## ייבוא אינטראקטיבי (Admin → Import Hasura Config)

משטח הניהול (admin surface) מריץ את אותם ממירים (converters), כך שייבוא לא דורש גישת shell ולא דורש round-trip של קובץ config. דורש את היכולת (capability) `org_settings`; הייבוא נוחת בארגון שבו הסשן פועל.

1. **העלאה.** בחרו תיקיית metadata מגובזת (zipped) של Hasura v2, פרויקט DDN מגובז, ייצוא metadata מאוחד (`.yaml`/`.json`, כולל המעטפה (envelope) `{resource_version, metadata}` שמחזיר ה-metadata API), או קובץ `.hml` בודד. השאירו את הפורמט על *זיהוי אוטומטי* אלא אם ההעלאה דו-משמעית.
2. **מיפוי דומיינים** (אופציונלי). כל זוג ממפה סכמת v2 או subgraph של DDN לדומיין ב-Provisa; כל דבר שלא ממופה שומר על שמו המקורי.
3. **המרה ותצוגה מקדימה.** השרת ממיר ומחזיר ספירות, אזהרות ממיר, ואת הקונפיגורציה שנוצרה. שום דבר לא נכתב בשלב הזה.
4. **סקירה ועריכה.** הקונפיגורציה ניתנת לעריכה במקום — פרטי חיבור, שמות דומיינים, שמות תפקידים (roles). מה שמיישמים הוא מה שמוצג.
5. **החלה.** *Replace the existing semantic layer* מוחקת כל מקור, טבלה, תפקיד וכלל שנעדרים מהקונפיגורציה; ללא הפעלה, הייבוא ממוזג לתוך מה שקיים בארגון. ההחלה טוענת את הקונפיגורציה ובונה מחדש את הסכמות של הארגון.

נקודות קצה: `POST /admin/import/hasura/preview` ו-`POST /admin/import/hasura/apply`.

---

## Hasura v2

### ייצוא Metadata

מהקונסולה או ה-CLI של Hasura:

```bash
hasura metadata export --output metadata.yaml
```

או השתמשו ב-API של Hasura:

```bash
curl -X POST http://localhost:8080/v1/metadata \
  -H "X-Hasura-Admin-Secret: <secret>" \
  -d '{"type":"export_metadata","args":{}}' \
  > metadata.json
```

### המרה

ממיר ה-v2 קורא **תיקיית** metadata של Hasura (המבנה שמפיק `hasura metadata export`, או המבנה השטוח `tables.yaml` / `actions.yaml`) וכותב קונפיגורציית Provisa:

```bash
python -m provisa.hasura_v2 ./metadata -o config.yaml
```

השמיטו את `-o` כדי לכתוב את הקונפיגורציה ל-stdout.

דגלים (flags):

| דגל | מטרה |
| ------ | --------- |
| `-o`, `--output` | נתיב פלט YAML (ברירת מחדל: stdout) |
| `--source-overrides` | קובץ YAML עם דריסות חיבור פר-מקור (host, port, credentials) |
| `--domain-map` | מיפויי סכמה-לדומיין כזוגות `SCHEMA=DOMAIN` |
| `--auth-env-file` | קובץ `.env` עם קונפיגורציית אימות; ממיר JWT/JWK, admin secret, ומיפוי claims |
| `--dry-run` | ניתוח (parse) ואימות ללא כתיבת פלט |

### מה מומר

| מושג ב-Hasura | מקביל ב-Provisa |
| --------------- | ------------------- |
| טבלה עקובה (Tracked table) | `tables[]` עם `publish: true` |
| קשר אובייקט (Object relationship) | `relationships[]` עם `cardinality: many-to-one` |
| קשר מערך (Array relationship) | `relationships[]` עם `cardinality: one-to-many` |
| הרשאת Select | נראות תפקיד (role) + מסנן RLS |
| הרשאת עמודה | `visible_to` / `writable_by` |
| הרשאת Insert/update/delete | Mutation `writable_by` + RLS |
| סכמה מרוחקת (Remote schema) | רישום מקור `graphql_remote` |
| שדה מחושב (Computed field) | ערך `functions[]` עם `kind: query` |

### מגבלות

- **Actions** מומרות אוטומטית: actions עם handler מסוג HTTP הופכות ל-mutations תחת `webhooks[]`; actions עם handler שאינו HTTP (מסד נתונים) הופכות ל-placeholder תחת `functions[]` ומוציאות אזהרה לסקירת ה-handler
- **Event triggers** מומרות לקונפיגורציית `event_triggers` פר-טבלה (פעולות, כתובת webhook, מדיניות retry) ומוציאות אזהרה המציינת נאמנות (fidelity) מוגבלת
- **סכמות מרוחקות** מומרות לערכי מקור `graphql_remote`
- **פונקציות SQL מותאמות אישית** דורשות סקירה — מקרים פשוטים מומרים לערכי `functions[]`, מורכבים דורשים עבודה ידנית
- **Cron triggers** מומרים לערכי קונפיגורציית `scheduler`, תוך שימור ביטוי ה-cron ודגל ההפעלה (enabled flag)

---

## Hasura DDN (v3)

### איתור פרויקט ה-HML

ממיר ה-DDN קורא ישירות את **תיקיית** פרויקט ה-DDN של קבצי `.hml` — אין צורך בשלב build של supergraph. רכיב התיקייה הראשון תחת שורש הפרויקט נלקח כשם ה-subgraph; קבצים תחת `globals/` משויכים ל-subgraph בשם `globals`.

### המרה

```bash
python -m provisa.ddn ./my-ddn-project -o config.yaml
```

השמיטו את `-o` כדי לכתוב את הקונפיגורציה ל-stdout.

דגלים:

| דגל | מטרה |
| ------ | --------- |
| `-o`, `--output` | נתיב פלט YAML (ברירת מחדל: stdout) |
| `--source-overrides` | קובץ YAML עם דריסות חיבור פר-מקור |
| `--domain-map` | מיפויי subgraph-לדומיין כזוגות `SUBGRAPH=DOMAIN` |
| `--aggregates-output` | נתיב פלט לקובץ הצדדי (sidecar) של ביטויי aggregate (ברירת מחדל: `<output>-aggregates.yaml`) |
| `--dry-run` | ניתוח ואימות ללא כתיבת פלט |

metadata מסוג `AggregateExpression` נשמר בקובץ צדדי `*-aggregates.yaml`.

### מה מומר

| מושג ב-DDN | מקביל ב-Provisa |
| ------------ | ------------------- |
| מודל Subgraph | `tables[]` תחת מקור |
| קשר (Relationship) | `relationships[]` |
| כלל הרשאה | מסנן RLS |
| Command | Mutation מסוג webhook או תצוגה (view) |
| מחבר (Connector) | ערך מקור עם פרטי חיבור |

### מגבלות

- **מחברי Lambda** (פונקציות TypeScript/Python) דורשים הגדרת webhook ידנית
- **Lifecycle plugins** אין להם מקביל ישיר
- **מצבי אימות DDN** ממופים לספקי אימות של Provisa אך נתיבי claim של JWT עשויים לדרוש התאמה

---

## אחרי הייבוא

1. סקרו את `config.yaml` שנוצר — שימו לב ל-`warnings` מהממיר
2. אמתו את פרטי ההזדהות לחיבור (הממיר משתמש בערכי placeholder)
3. הפעילו את Provisa וודאו שהטבלאות מופיעות ב-Explorer
4. הריצו את שאילתות ה-GraphQL הקיימות שלכם — הסכמה תואמת לדפוסים נפוצים
5. הגישו שאילתות לאישור דרך Admin API או ה-UI לפני הפעלת ממשל production
