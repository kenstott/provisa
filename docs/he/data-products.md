# מוצרי נתונים (REQ-1634)

מוצר נתונים (Data Product) הוא חבילת טבלאות בעלת שם ובעלים, המתפרסמת יחד לצריכה. זוהי היחידה שהקטלוג חושף לצרכנים — לא טבלאות בודדות, אלא משטח אצור (Curated) שתחום מצהיר במפורש שהוא מוכן. שדות עוקבים אחר אוצר המילים של ODPS (Open Data Product Standard) במקום שבו כבר קיים ל-Provisa מקור האמת. [tool-verified: `provisa/core/models.py:318-342`, `provisa-ui/src/i18n/locales/en/dataProductsTab.json`]

## כלל בעלות תחום

כל מוצר נתונים הוא בבעלות תחום אחד בלבד (`domain_id` הוא שדה חובה). טבלה יכולה להצטרף למוצר נתונים רק כאשר לשניהם אותו `domain_id`. ה-UI מגביל את בורר הטבלאות לתחום המוצר; ה-Backend דוחה שיוך `product_id` שהתחום שלו אינו תואם לתחום המוצר בעת השמירה. [tool-verified: `provisa/core/models.py:320`, `docs/arch/requirements.yaml:54573-54574`]

מוצר שזקוק לנתונים מתחום אחר חייב להביא נתונים אלה תחילה כ-View של התחום, ואז לכלול את ה-View כחבר.

## Output Ports

הטבלאות והפקודות המשויכות למוצר נתונים הן ה-**Output Ports** שלו — משטח בר-שאילתה (Queryable) שהצרכנים רואים. שיוך טבלה קובע את `Table.product_id`; ביטול השיוך מסיר את החברות. טבלה שייכת למוצר אחד לכל היותר. פקודות (Commands) באותו תחום יכולות גם הן להיות משויכות כחברות. [tool-verified: `provisa/core/models.py:941`, `provisa-ui/src/i18n/locales/en/dataProductsTab.json:tablesLabel,commandsLabel`]

## קטעי חלונית פרטים

פתיחת מוצר נתונים ב-UI המנהל מציגה את החלוניות הבאות:

| חלונית | מה היא מציגה |
| --- | --- |
| Output Ports | טבלאות חבר ועמודותיהן; פקודות חבר; שאילתות דוגמה (GraphQL, SQL, Cypher, gRPC, JSON:API, REST) |
| מונחים קשורים | מונחי מילון הקשורים לטבלאות החבר של המוצר |
| טבלאות קשורות | טבלאות הניתנות להגעה מטבלאות חבר דרך קשרים מאושרים אך עדיין לא חלק מהמוצר |
| קשרים | קשרים מאושרים בין טבלאות החבר של מוצר זה |
| Lineage | גרף Lineage ברמת עמודה המציג את טבלאות החבר כנקודת הקצה המתפרסמת בתוספת כל טבלה במעלה הזרם. דורש את יכולת ה-`view_governance` |
| Input Ports | קלטים בקפיצה אחת → טרנספורמציה → פלטים, נגזרים מ-Lineage. דורש `view_governance` |
| איכות נתונים | טבלאות בודקות (Checker) שהחוזים שלהן סורקים את ה-Output Ports של מוצר זה; שורה אחת לכל בדיקה לכל הרצה. כולל חלונית כללים ותצוגת תגי PII |

[tool-verified: `provisa-ui/src/i18n/locales/en/dataProductsTab.json:detail`]

## ייצוא מטא-דאטה {: #metadata-export }

רק טבלאות המשויכות למוצר מתפרסמות לקטלוגים חיצוניים כברירת מחדל. `build_snapshot` מפעיל מסנן `data_products_only`: טבלאות לא-משויכות נשמרות בחוץ, יחד עם קשתות הקשר (Relationship), קשתות ה-Lineage, ותגי הממשל שלהן. מקורות ותחומים מתפרסמים תמיד ללא תלות בכך. [tool-verified: `provisa/api/metadata_export/builder.py:594,609,641`]

מוצר ללא חברים מיוצאים אינו מתפרסם — רשימה ריקה תטען שהמוצר קיים ללא דבר מאחוריו. [tool-verified: `provisa/api/metadata_export/model.py:106-113`]

רק קטלוגים בעלי מושג מוצר-נתונים ילידי מפרסמים אותו כישות מדרגה ראשונה; השאר מפרסמים את טבלאות החבר (שכבר סוננו) ללא קיבוץ מוצר:

| קטלוג | מתפרסם כ- |
| --- | --- |
| Snowflake Horizon | SHARE + רשימת ארגון (Data Product ילידי); `publish=false` משאיר אותו כ-DRAFT, `publish=true` מפעיל אותו |
| BigQuery Analytics Hub | רשימת Analytics Hub (ילידי) |
| OpenMetadata | ישות `DataProduct` (ילידית) |
| DataHub | ישות `dataProduct` URN ילידית עם היבטי properties/ownership משלה |
| Collibra | נכס מסוג קהילת `Data Product`, מקושר לטבלאות חבר |
| Apache Atlas | typedef מותאם `provisa_data_product` במיטב המאמץ — ל-Atlas אין סוג מוצר-נתונים ילידי |
| Atlan | ניחוש typedef מותאם `DataProduct` במיטב המאמץ — ל-Atlan אין סוג יציב מתועד לכך |
| OpenLineage | לא רשימה — טבלאות חבר נושאות Facet מותאם `provisa_data_product` הנוקב בשם המוצר |

[tool-verified: `provisa/api/metadata_export/snowflake_horizon.py:389-418`, `provisa/api/metadata_export/bigquery_dataplex.py:112-136`, `provisa/api/metadata_export/openmetadata.py:326-344`, `provisa/api/metadata_export/datahub.py:133-136,443-483`, `provisa/api/metadata_export/collibra.py:129-133,371-388`, `provisa/api/metadata_export/atlas.py:134-147`, `provisa/api/metadata_export/atlan.py:60`, `provisa/api/metadata_export/openlineage.py:243,348`]

## שדות

| שדה | חובה | הערות |
| --- | --- | --- |
| `id` | כן | מזהה יציב קריא-מכונה, למשל `customer_360` |
| `domain_id` | כן | התחום הבעלים; כלל החברות נאכף כנגד שדה זה |
| `name` | כן | שם תצוגה |
| `owner_role` | לא | תפקיד האחראי למוצר זה; נבדל מ-Steward התחום |
| `team_role` | לא | תפקיד שבעליו מהווים את צוות העבודה היום-יומי; מתפענח לפרטים (Individuals) |
| `purpose` | לא | מה מוצר זה מפרסם ומדוע |
| `limitations` | לא | אילוצים, הסתייגויות או החרגות ידועים |
| `usage` | לא | כיצד לצרוך מוצר זה |
| `version` | לא | למשל `1.2.0` |
| `status` | לא | למשל `proposed`, `active`, `deprecated`, `retired` |
| `sla` | לא | התחייבויות רמת שירות; פרוזה בלבד — מוצר משתרע על פני מספר טבלאות חבר וניתן SLA מובנה לא יכול לנקוב באופן חד-משמעי איזה חבר הוא מתאר |
| `support` | לא | הנחיית תמיכה בטקסט חופשי |
| `support_contact` | לא | אימייל או כתובת URL; נדרש עבור קובצי רשימת ארגון של Snowflake Horizon Catalog (REQ-1635) |
| `publish` | לא | `true` לפרסום רשימות Horizon Catalog מיידית; רשימות חדשות ברירת מחדל DRAFT (REQ-1635) |
| `custom_properties` | לא | מטא-דאטה במפתח-ערך שרירותי שאינו מכוסה על-ידי השדות הסטנדרטיים |

[tool-verified: `provisa/core/models.py:318-342`, `provisa/api/admin/types.py:104-118,538-551`]
