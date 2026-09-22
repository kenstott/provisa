# הפניית Admin GraphQL API

ה-API של Admin GraphQL הוא מישור ההגדרות של Provisa. זהו ה-API שאפליקציית ה-Web של הניהול קוראת לו בכל פעולת ניהול — יצירת מקורות, רישום טבלאות, הגדרת קשרים, הגדרת כללי RLS, וכל דבר אחר שמעצב את המודל.

**נקודת עגינה (Mount point):** `POST /admin/graphql`

זהו API שונה מזה של מישור הנתונים ב-`/data/graphql`. מישור הנתונים משרת שאילתות משתמש-קצה מעל תחומים רשומים ומתואר על ידי ה-SDL תחת `/data/sdl`. ה-API של הניהול מגדיר כיצד נראית אותה סכמה ומי רשאי לראות מה.

---

## כיצד ה-UI מדבר עם API זה

אפליקציית ה-Web של הניהול משתמשת ב-Apollo Client, המצביע על `${API_BASE}/admin/graphql`. [tool-verified: `provisa-ui/src/apolloClient.ts:19`]

כל בקשה נושאת אסימון bearer (הנשלף מחדש מספק האימות בכל קריאה), כותרת `X-Org-Id` בסביבת ריבוי-דיירים, וכותרת `X-Env` בעת שירות סביבת ענף. [tool-verified: `provisa-ui/src/apolloClient.ts:24-42`]

הסכמה מורכבת משתי מחלקות `@strawberry.type` — `Query` מ-`schema_query.py` ו-`Mutation` מ-`schema_mutation.py` — ועטופה ב-`ModelCommitExtension` הרושם כל mutation כנגד ענף הסביבה הנוכחי (REQ-1524). [tool-verified: `provisa/api/admin/schema.py:44`]

---

## הרשאה

**מצב פיתוח:** כאשר אין אימות מוגדר וכל בקשה מגיעה כעקרון אנונימי, כל בדיקות היכולת מדולגות. זה שומר על התקנה מקומית פונקציונלית ללא הגדרת אימות. [tool-verified: `provisa/api/admin/capabilities.py:98-99`]

**שערי יכולת (Capability gates):** פריסות ייצור אוכפות יכולות בעלות שם. הזכות הספציפית הנדרשת לכל שדה מצוינת בתוך הטקסט. קריאה ל-mutation ללא היכולת הנדרשת מעלה `PermissionError`. תפקיד מנהל הפלטפורמה עוקף את כל בדיקות היכולת (REQ-1297). [tool-verified: `provisa/api/admin/capabilities.py:80-110`]

**שערי תחום (Domain gates):** מספר mutations בודקות גם את התחום שהאובייקט שייך אליו. קורא המוגבל לתחום `sales` אינו יכול לרשום טבלה לתוך `finance`, להעמיד בתור כלל RLS עבורה, או ליצור קשר שהטבלת המקור שלו נמצאת בתחום שאין הוא מחזיק (REQ-1530, REQ-1531). Views מוגבלים עוד יותר: כל טבלה ש-SQL של ה-view קורא ממנה חייבת להיות בתוך התחומים של הקורא, מכיוון ש-SQL חופשי אחרת מעניק לחבר גישה לנתונים מחוץ להיקפו. [tool-verified: `provisa/api/admin/domain_guard.py:1-133`]

**הורשת תפקיד:** יכולות של תפקיד הורה נורשות על ידי תפקידי הצאצא (REQ-1677). `createRole` ו-`deleteRole` דוחים מעגלים ומונעים מחיקת תפקיד שיש לו יורשים.

---

## סוג החזרה משותף

רוב ה-mutations מחזירות `MutationResult`. [tool-verified: `provisa/api/admin/types.py:1181-1188`]

```graphql
type MutationResult {
  success: Boolean!
  message: String!
  code: String          # stable i18n key, e.g. "schema.source_created"
  params: JSON          # key/value pairs for client-side localization (REQ-1350)
}
```

כאשר mutation נכשלת, `success` הוא `false` ו-`message` נושא את הסיבה באנגלית. `code` הוא מזהה יציב שה-UI משתמש בו לצורך הצגת הודעה מתורגמת.

---

## Queries

### מקורות

#### `sources → [SourceType!]!`

כל מקורות הנתונים הרשומים. [tool-verified: `provisa/api/admin/schema_query.py:327-331`]

```graphql
query {
  sources {
    id type host port database username dialect
    cacheEnabled cacheTtl preferMaterialized
    loadProtected offPeakWindow offPeakTz
    gqlNamingConvention path allowedDomains
    description mappingJson federationHintsJson
    changeSignal passwordRef
    cdc { bootstrapServers topicPrefix schemaRegistryUrl consumerGroupId }
  }
}
```

`passwordRef` הוא הפניית `${secret:NAME}` לתוך כספת הארגון — לעולם לא האישור המילולי. [tool-verified: `provisa/api/admin/types.py:105`]

#### `source(id: String!) → SourceType`

מקור בודד לפי מזהה. מחזיר `null` כאשר לא נמצא. [tool-verified: `provisa/api/admin/schema_query.py:334-339`]

#### `availableSchemas(sourceId: String!) → [String!]!`

סכמות גלויות במקור, מסוננות להחריג את אלה הפנימיות של Provisa. משתמש בבדיקה עצמית (introspection) ילידית תחילה; נופל חזרה לקטלוג המנוע כאשר לסוג המקור אין pool ישיר. [tool-verified: `provisa/api/admin/schema_query.py:628-667`]

#### `availableTables(sourceId: String!, schemaName: String = "public") → [AvailableTableType!]!`

טבלאות בסכמה אחת של מקור, עם הערותיהן. עבור מקורות OpenAPI, מחזיר פעולות GET שהתגובה שלהן היא מערך או עוטף עימוד. עבור מקורות GraphQL, מחזיר שדות query המחזירים רשימה. עבור gRPC, מחזיר RPC-ים מסוג server-streaming. [tool-verified: `provisa/api/admin/schema_query.py:669-731`]

#### `availableColumns(sourceId: String!, schemaName: String!, tableName: String!) → [String!]!`

שמות עמודות לטבלה בקטלוג המנוע. עבור מקורות govdata, משתמש ב-resolver נפרד. [tool-verified: `provisa/api/admin/schema_query.py:845-866`]

#### `availableColumnsMetadata(sourceId: String!, schemaName: String!, tableName: String!) → [AvailableColumnType!]!`

שמות עמודות עם סוגי נתונים, הערות, סוגי מסנן ילידי, ודגלי מפתח ראשי. עבור מקורות OpenAPI, נגזר מהצורה של סכמת התגובה והפרמטרים של הפעולה. [tool-verified: `provisa/api/admin/schema_query.py:869-876`]

#### `availableFunctions(sourceId: String!, schemaName: String = "openapi") → [AvailableTableType!]!`

פעולות שאינן GET עבור מקור OpenAPI (POST, PUT, PATCH, DELETE). מחזיר רשימה ריקה עבור מקורות שאינם OpenAPI. [tool-verified: `provisa/api/admin/schema_query.py:822-843`]

#### `crawlSource(path, depth, pattern, recursive, simpleLinks, sameDomain, excludePattern) → CrawlResultType`

תצוגה מקדימה של מה שסריקת מחבר-קובץ (file-connector) תגלה — קבצים, טבלאות ועמודות — לפני יצירת מקור. ההגדרות שהן HTTP-בלבד (`simpleLinks`, `sameDomain`, `excludePattern`) מתעלמות עבור שורשי local, S3, FTP ו-SFTP. (REQ-1785) [tool-verified: `provisa/api/admin/schema_query.py:733-790`]

#### `suggestTableAlias(tableName: String!, domainId: String!, sourceId: String!) → String!`

מחזיר את הכינוי (alias) לשימוש בעת רישום `tableName` בתוך `domainId` מ-`sourceId`. מחזיר כינוי snake-case פשוט כשאין התנגשות, או כינוי בעל קידומת מקור (`sqlite_b_orders`) כאשר השם האפקטיבי כבר תפוס על ידי מקור אחר באותו תחום. [tool-verified: `provisa/api/admin/schema_query.py:879-922`]

---

### טבלאות

#### `tables → [RegisteredTableType!]!`

כל הטבלאות הרשומות, כל אחת עם רשימת העמודות המלאה שלה. נראות עמודה בתגובה מכבדת את היכולת `table_registration` של הקורא — `canDeployToDb` מותנה בכך שהקורא מחזיק בזכות זו. (REQ-016, REQ-021, REQ-042) [tool-verified: `provisa/api/admin/schema_query.py:502-536`]

כל `RegisteredTableType` חושף תת-שדות מחושבים:

- **`refreshPolicySummary → RefreshPolicySummaryType`** — מדיניות הרענון/השירות האפקטיבית כטקסט רגיל, נגזרת בצד השרת מאותה פתרון מתכנן (planner) שהמנוע משתמש בו. מחזיר `null` בזמן ההפעלה. (REQ-1143) [tool-verified: `provisa/api/admin/types.py:319-327`]
- **`graphqlFieldName → String`** — שם השדה שיש לטבלה זו בסכמת מישור הנתונים המהודרת, כך שפאנל מוצר הנתונים יכול לבנות דוגמה הניתנת להרצה מבלי לשכפל את אלגוריתם השמות. (REQ-1634) [tool-verified: `provisa/api/admin/types.py:330-339`]
- **`dqDataset → String`** — טבלה זו כערכת נתונים של חוזה איכות-נתונים, בצורה שהבודק (checker) סורק. (REQ-1443) [tool-verified: `provisa/api/admin/types.py:374-387`]
- **`productId → String`** — מוצר הנתונים שאליו טבלה זו שייכת. טבלת בודק DQ יורשת את המוצר של הטבלה שהחוזה שלה סורק. (REQ-1634) [tool-verified: `provisa/api/admin/types.py:342-372`]

#### `refreshPolicyPreview(...) → RefreshPolicySummaryType`

תצוגה מקדימה של סיכום הרענון/השירות האפקטיבי עבור ידיות טבלה *טיוטה* (שאינן שמורות), כך שסיכום ראש הטופס מתעדכן ככל שהשדות משתנים ללא שמירה של דבר. אותה גזירה כמו `refreshPolicySummary` לעיל. (REQ-1143) [tool-verified: `provisa/api/admin/schema_query.py:947-979`]

ארגומנטים: `sourceId`, `domainId`, `schemaName`, `tableName`, `cacheTtl`, `preferMaterialized`, `loadProtected`, `offPeakWindow`, `offPeakTz`, `changeSignal`.

#### `columnDependents(tableId: String!, renamed: [String!], removed: [String!]) → [ColumnDependentsType!]!`

מוצרים שהמרה ממתינה של כינוי או מחיקת עמודה תשבור. ייעוצי בלבד — ה-UI של הניהול מציג זאת לפני השמירה והמנהל מחליט. חייב להיקרא *לפני* השמירה, מכיוון שהתלויות נכתבו כנגד השם החשוף שהעמודה נושאת כרגע. (REQ-1484) [tool-verified: `provisa/api/admin/schema_query.py:1301-1331`]

---

### קשרים

#### `relationships → [RelationshipType!]!`

כל הקשרים המוגדרים על ידי משתמש (מחריג רשומות `gql_auto__` שנוצרות אוטומטית ורשומות `meta:%` הסינתטיות המשמשות את ה-ERD). [tool-verified: `provisa/api/admin/schema_query.py:539-569`]

#### `allRelationships → [RelationshipType!]!`

זהה ל-`relationships`, אך כולל רשומות `meta:%` סינתטיות. משמש את ה-ERD הגרפי, הזקוק להציג כל קשת כולל הקישורים המשתמעים `HAS_TABLE` בין טבלאות נתונים לבין רשם המטא-דאטה. [tool-verified: `provisa/api/admin/schema_query.py:572-601`]

כל `RelationshipType` חושף:

- **`autoSuggested → Boolean`** — האם הקשר הוצע על ידי ניתוח FK (`id` מתחיל ב-`fk__`). [tool-verified: `provisa/api/admin/types.py:523-525`]
- **`physicalName → String`** — שם הקשר במישורי SQL ו-gRPC (הפרמטר `?include=`). נגזר בצד השרת; לקוחות אסור להם לתעתק את הכינוי של GraphQL. (REQ-471, REQ-1417) [tool-verified: `provisa/api/admin/types.py:527-536`]

---

### תחומים, תפקידים ומשתמשים

#### `domains → [DomainType!]!`

כל התחומים במסד הנתונים של דייר הארגון הפעיל. מסד הנתונים של הדייר מבודד ברמת הסכמה, כך שרשימת התחומים של org-admin מכילה רק את שורות הארגון שלו. (REQ-021, REQ-042, REQ-1293) [tool-verified: `provisa/api/admin/schema_query.py:342-357`]

#### `roles → [RoleType!]!`

תפקידים גלויים לקורא. מנהל רואה כל תפקיד; מי שאינו מנהל רואה רק תפקידים ללא `org_id` או תפקידים השייכים לארגונו. (REQ-042, REQ-059, REQ-060, REQ-215) [tool-verified: `provisa/api/admin/schema_query.py:603-618`]

#### `resolveOwners(refs: [String!]!) → [UserSummaryType!]!`

פותר מזהי תפקיד או מזהי משתמש למשתמשים בודדים. משמש להרחבת `DataProduct.ownerRole`, `Domain.steward`, ו-`Column.visibleTo` לרשימה קריאה לבני אדם. הפניות לא ידועות מוחזרות כלשונן כך שה-UI מציג את המזהה הגולמי ולא כלום. [tool-verified: `provisa/api/admin/schema_query.py:388-444`]

---

### כללי RLS

#### `rlsRules → [RLSRuleType!]!`

כל כללי האבטחה ברמת השורה. המאגר הבסיסי מפענח את `filterExpr` בגבול. (REQ-041, REQ-402, REQ-686) [tool-verified: `provisa/api/admin/schema_query.py:621-625`]

---

### מוצרי נתונים

#### `dataProducts → [DataProductType!]!`

כל מוצרי הנתונים. דורש את היכולת `data_product_read`. (REQ-1634) [tool-verified: `provisa/api/admin/schema_query.py:360-386`]

---

### תגים

#### `tags → [TagType!]!`

כל הגדרות התגים, כולל ערכי הפרמטרים המותרים לכל תג. (REQ-1373, REQ-1467) [tool-verified: `provisa/api/admin/schema_query.py:447-475`]

#### `tagAssignments → [TagAssignmentType!]!`

כל שיוכי התגים על פני מקורות, טבלאות, עמודות וקשרים. (REQ-1377) [tool-verified: `provisa/api/admin/schema_query.py:478-499`]

---

### Materialized Views

#### `mvList → [MVType!]!`

כל ה-Materialized Views עם מצב הריצה שלהם: מופעל/מושבת, חותמת זמן רענון אחרונה, מספר שורות, ושגיאה אחרונה. [tool-verified: `provisa/api/admin/schema_query.py:927-944`]

---

### מטמון

#### `cacheStats → CacheStatsType`

סטטיסטיקות מטמון. מחזיר `storeType: "redis"` עם מדדים תפעוליים מלאים כאשר Redis מוגדר, `storeType: "memory"` עבור חנות ה-fakeredis המוטבעת, ו-`storeType: "noop"` כאשר לא מוגדר מטמון. [tool-verified: `provisa/api/admin/schema_query.py:1106-1140`]

#### `cacheTableStats → [CacheTableStatType!]!`

ספירות רשומות שמורות במטמון לכל טבלה. ריק כאשר אין חנות מטמון מוגדרת. [tool-verified: `provisa/api/admin/schema_query.py:1143-1148`]

#### `hotTables → [HotTableStatType!]!`

טבלאות ש-Provisa שומרת עותק שלהן, על פני שתי דרגות: `hot` (משוקפת אל חנות התגובה לצורך הטמעת JOIN) ו-`warm` (נוחתת כעותק Iceberg). טבלה נמצאת בדרגה אחת לכל היותר (REQ-241). [tool-verified: `provisa/api/admin/schema_query.py:1151-1175`]

#### `materializeStoreInfo → MaterializeStoreInfoType`

זהות חנות המטריאליזציה העמידה: שם מנוע, הפניית DSN של החנות, ספירת MV, והאם החנות מקומית-למופע (קובץ מקומי כמו DuckDB או SQLite, שמשמעו שכל מופע מאחורי מאזן עומסים שומר עותק משלו). [tool-verified: `provisa/api/admin/schema_query.py:1178-1189`]

---

### תקינות המערכת

#### `systemHealth → SystemHealthType`

מצב חיבור המנוע, ספירות מאגר עובדים, מצב מאגר מסד המטא-דאטה, מצב מטמון, ותקינות של כל מאזין פרוטוקול (pgwire, gRPC, Arrow Flight, Bolt). [tool-verified: `provisa/api/admin/schema_query.py:1194-1198`]

```graphql
query {
  systemHealth {
    engineConnected engineWorkerCount engineActiveWorkers
    metadataPoolSize metadataPoolFree metadataDialect
    cacheMode cacheConnected mvRefreshLoopRunning
    protocols { name status port }
  }
}
```

---

### משימות מתוזמנות

#### `scheduledTasks → [ScheduledTaskType!]!`

טריגרים מתוזמנים מקובץ ההגדרות עם מצב ריצה. כל רשומה נושאת את ביטוי ה-cron שלה, `kind` (`webhook` או `sql`), האם היא מופעלת כרגע, חותמת הזמן של ההרצה האחרונה (תמיד `null` בגרסה זו — נעקב על ידי המתזמן), וזמן ההרצה המתוזמן הבא מ-APScheduler. [tool-verified: `provisa/api/admin/schema_query.py:1203-1245`]

---

### איכות נתונים

#### `dqContractParse(checker: String!, contractText: String!) → DqContractType`

פענח טקסט חוזה גולמי לשורות הניתנות לעריכה של פאנל הבנייה. נקרא בכל עריכה; כשל פענוח חוזר כ-`error` ולא כשגיאת GraphQL, מכיוון שטקסט כתוב-למחצה נורמלי בעת שהמפעיל מקליד. (REQ-1443 clause 7) [tool-verified: `provisa/api/admin/schema_query.py:1007-1022`]

#### `dqCheckCatalog(checker: String!, dataset: String!) → DqCheckCatalogType`

הבדיקות ש-`checker` מציע, מוגבלות לעמודות של `dataset`. ערכת הנתונים היא היעד הנצפה של החוזה, נפתר באותה דרך שהסורק פותר אותו — כך שהבדיקות המוצעות תואמות לעמודות שהבודק יראה באמת. (REQ-1443 clause 7) [tool-verified: `provisa/api/admin/schema_query.py:1025-1050`]

#### `dqCheckDefinition(checker: String!, check: DqCheckBuildInput!) → DqCheckDefinitionType`

הטקסט של בדיקה אחת מעורכי הפאנל. בצד השרת מכיוון שלדיאלקט יש מימוש אחד; בדיקה שנבנתה בבונה ובדיקה שהוקלדה ידנית חייבות להיות בלתי-ניתנות להבחנה. (REQ-1443 clause 7) [tool-verified: `provisa/api/admin/schema_query.py:1053-1075`]

#### `dqContractBuild(checker: String!, dataset: String!, checks: [DqCheckInput!]!) → DqContractTextType`

סריאליזציה של שורות בדיקה שנערכו חזרה לטקסט חוזה. ההופכי של `dqContractParse`. בצד השרת מאותה סיבה: הפאנל אינו יכול להפיק טקסט שהבודק היה דוחה. (REQ-1443 clause 7) [tool-verified: `provisa/api/admin/schema_query.py:1078-1101`]

---

### תצוגות מקדימות של מקורות

#### `neo4jPreview(sourceId: String!, cypher: String!) → QueryPreviewType`

תצוגה מקדימה של הטלת Cypher על מקור Neo4j: עד חמש שורות וסוגי העמודות שהרישום ישא. כשלים חוזרים כ-`error`. (REQ-1670) [tool-verified: `provisa/api/admin/schema_query.py:984-992`]

#### `sparqlPreview(sourceId: String!, query: String!) → QueryPreviewType`

תצוגה מקדימה של SPARQL SELECT על מקור SPARQL: עד חמש שורות, כל העמודות כטקסט. (REQ-1683) [tool-verified: `provisa/api/admin/schema_query.py:995-1002`]

---

### Kaggle

#### `kaggleTokenValid(token: String!) → Boolean!`

בדיקה חיה כנגד ה-API של Kaggle. מחזיר `true` רק כאשר האסימון מאמת. תומך בשלב שער-האסימון בטופס מקור Kaggle. (REQ-1783) [tool-verified: `provisa/api/admin/schema_query.py:793-799`]

#### `kaggleDatasets(token: String!, query: String = "", page: Int = 1) → [KaggleDatasetType!]!`

חיפוש בקטלוג ערכות הנתונים הציבורי המלא של Kaggle. מוגבל-אסימון. (REQ-1783) [tool-verified: `provisa/api/admin/schema_query.py:802-819`]

---

### לוחות שנה

#### `calendars → [CalendarType!]!`

כל גרסאות לוח השנה של גבול תמונת המצב הרשומות. מזין את בורר הגדרות לוח הזמנים של תמונת המצב ומאשר לאילו לוחות שנה MV תקופתי רשאי להפנות. (REQ-962) [tool-verified: `provisa/api/admin/schema_query.py:218-242`]

---

### מדדים

#### `metrics → [MetricType!]!`

כל הגדרות המדד המנוהלות. מדדים הנגזרים מעובדה (fact) נושאים `fromFact`. (REQ-1317, REQ-1320) [tool-verified: `provisa/api/admin/schema_query.py:245-264`]

---

### גרסת סכמה

#### `schemaVersion → String!`

hash מסוג SHA-256 של מצב הסכמה הנוכחי (תחומים, מזהי טבלה, מזהי קשר). לקוח Apollo קורא ערך זה מכותרת התגובה `X-Schema-Version` ומבצע fetch מחדש לכל השאילתות הפעילות כשהוא מתקדם. [tool-verified: `provisa/api/admin/schema_query.py:291-324`]

---

### עוזרי AI

#### `generateTableDescription(tableId: String!) → String!`

השתמש ב-LLM המוגדר ליצירת תיאור של משפט אחד עד שניים עבור טבלה רשומה. שמור את הטבלה תחילה; קריאה לפעולה זו על טבלה לא-שמורה מחזירה הודעה הנחייתית. [tool-verified: `provisa/api/admin/schema_query.py:1250-1298`]

#### `generateColumnDescription(tableId: String!, columnName: String!) → String!`

השתמש ב-LLM המוגדר ליצירת תיאור של משפט אחד עבור עמודה בודדת. [tool-verified: `provisa/api/admin/schema_query.py:1334-1383`]

---

### בקשות יצירה

#### `creationRequests → [CreationRequestType!]!`

בקשות יצירה ממתינות, גלויות לקוראים המחזיקים ביכולת היצירה הרלוונטית. משמש כאשר חבר ללא `create_relationship` או `create_view` מגיש בקשה שמחזיק זכות חייב לאשר. (REQ-434, REQ-063) [tool-verified: `provisa/api/admin/schema_query.py:267-289`]

---

## Mutations

### מקורות

#### `createSource(input: SourceInput!) → MutationResult`

רישום מקור נתונים חדש. מאמת את החיבור לפני שמירה — מקור שנדחה אינו משאיר רשומת כספת מאחוריו. שומר אישורים בכספת הארגון ומתעד את ההפניה; הטקסט הגלוי לעולם אינו נוחת במסד הנתונים. (REQ-012, REQ-013) דורש את היכולת `source_registration`. [tool-verified: `provisa/api/admin/schema_mutation.py:616-781`]

#### `updateSource(input: SourceInput!) → MutationResult`

עדכון פרטי חיבור, תיאור והגדרות של מקור קיים. מפרק ומחבר מחדש את נקודת הקצה של pgwire עבור מקורות file/SharePoint כך ששינוי נתיב נכנס לתוקף מיידית. דורש `source_registration`. [tool-verified: `provisa/api/admin/schema_mutation.py:922-1073`]

#### `deleteSource(id: String!) → MutationResult`

הסרת מקור ורשומת הכספת שלו. מפיל את קטלוג המנוע ובונה מחדש סכמות. [tool-verified: `provisa/api/admin/schema_mutation.py:1101-1132`]

#### `renameSource(oldId: String!, newId: String!) → MutationResult`

שינוי שם מזהה מקור. [tool-verified: `provisa/api/admin/schema_mutation.py:1076-1098`]

#### `updateSourceCache(sourceId: String!, cacheEnabled: Boolean!, cacheTtl: Int) → MutationResult`

הפעלה או השבתה של מטמון תוצאות שאילתה עבור מקור, וקביעת ה-TTL בשניות. [tool-verified: `provisa/api/admin/schema_mutation.py:2327-2350`]

#### `updateSourcePreferMaterialized(sourceId: String!, preferMaterialized: Boolean!) → MutationResult`

כפיה (או שחרור) של פדרציה מטריאליזית עבור כל הטבלאות במקור. (REQ-826) [tool-verified: `provisa/api/admin/schema_mutation.py:2379-2402`]

#### `updateSourceLoadProtection(sourceId, loadProtected, offPeakWindow, offPeakTz) → MutationResult`

סימון מקור כמוגן-עומס (רענון מתוזמן בלבד). דורש לפחות שער אחד — חלון שעות שפל, קדנס TTL של מטמון, או אות שינוי בבדיקה — אחרת הקריאה נכשלת. (REQ-1141) [tool-verified: `provisa/api/admin/schema_mutation.py:2431-2480`]

#### `updateSourceNaming(sourceId: String!, gqlNamingConvention: String) → MutationResult`

קביעת מוסכמת השמות של GraphQL לכל מקור. [tool-verified: `provisa/api/admin/schema_mutation.py:2595-2619`]

#### `updateSourceAllowedDomains(sourceId: String!, allowedDomains: [String!]!) → MutationResult`

קביעת אילו תחומים רשאים להשתמש במקור (רשימה ריקה = ללא הגבלה). דורש `source_registration`. (REQ-1531) [tool-verified: `provisa/api/admin/schema_mutation.py:2622-2658`]

#### `stageKaggleDataset(token, owner, ref, idPrefix) → KaggleStageResultType`

הורדה ופתיחת ערכת נתונים של Kaggle לדיסק המקומי. מחזיר את נתיב התיקייה שהוכנה; הקורא לאחר מכן יוצר מקור מסוג `files` המצביע אליה. חבילות הנושאות SQLite נדחות במלואן. דורש `source_registration`. (REQ-1780–1782) [tool-verified: `provisa/api/admin/schema_mutation.py:784-824`]

#### `refreshKaggleSource(sourceId: String!, token: String!) → MutationResult`

משיכה מחדש של ערכת הנתונים של מקור נגזר-Kaggle במקום. מדלג על ההורדה אם ל-Kaggle אין דבר חדש יותר ממה שיש בדיסק. דורש `source_registration`. (REQ-1787) [tool-verified: `provisa/api/admin/schema_mutation.py:827-920`]

#### `refreshSourceStatistics(sourceId: String!) → MutationResult`

הרצת `ANALYZE` על כל הטבלאות הרשומות עבור מקור. משפר החלטות סדר-join ושידור עבור שאילתות פדרטיביות. (REQ-276) [tool-verified: `provisa/api/admin/schema_mutation.py:2944-3008`]

---

### טבלאות

#### `registerTable(input: TableInput!) → MutationResult`

רישום טבלה (או view) חדשה לתוך תחום. דורש את היכולת `table_registration` וחברות בתחום היעד. קורא שאין לו `create_relationship` המגיש view מועמד בתור כבקשת יצירה שמחזיק זכות חייב לאשר. (REQ-013, REQ-016, REQ-252, REQ-434) [tool-verified: `provisa/api/admin/schema_mutation.py:1714-1718`]

#### `updateTable(input: TableInput!) → MutationResult`

עדכון כינוי, תיאור, מטא-דאטה של עמודות, הגדרות MV, והגדרות אספקה חיה של טבלה קיימת. (REQ-016, REQ-020) דורש `table_registration`. [tool-verified: `provisa/api/admin/schema_mutation.py:1858-1995`]

#### `deleteTable(id: Int!) → MutationResult`

מחיקת טבלה רשומה. מחפש את תחום הטבלה לצורך שער התחום לפני המחיקה. (REQ-1531) [tool-verified: `provisa/api/admin/schema_mutation.py:1998-2029`]

#### `updateTableCache(tableId: Int!, cacheTtl: Int) → MutationResult`

דריסת ה-TTL של המטמון עבור טבלה אחת. [tool-verified: `provisa/api/admin/schema_mutation.py:2353-2376`]

#### `updateTablePreferMaterialized(tableId: Int!, preferMaterialized: Boolean) → MutationResult`

דריסת פדרציה מטריאליזית עבור טבלה אחת. `null` = ירושה מברירת המחדל של המקור. (REQ-826) [tool-verified: `provisa/api/admin/schema_mutation.py:2405-2428`]

#### `updateTableLoadProtection(tableId, loadProtected, offPeakWindow, offPeakTz) → MutationResult`

דריסת הגנת עומס עבור טבלה אחת. `null` עבור `loadProtected` יורש את ברירת המחדל של המקור. מאמת את שילוב השערים האפקטיבי (טבלה ← מקור). (REQ-1141) [tool-verified: `provisa/api/admin/schema_mutation.py:2483-2566`]

#### `updateTableNaming(tableId: Int!, gqlNamingConvention: String) → MutationResult`

קביעת מוסכמת השמות של GraphQL לכל טבלה. [tool-verified: `provisa/api/admin/schema_mutation.py:2661-2685`]

#### `deployViewToDb(tableId: Int!) → MutationResult`

קידום view וירטואלי של Provisa ל-view אמיתי במסד הנתונים על המקור הילידי הבסיסי שלו. [tool-verified: `provisa/api/admin/schema_mutation.py:3058-3060`]

#### `forceRegen(tableId: Int!, reason: String!) → MutationResult`

חישוב מחדש של שורות נחותות של טבלה על-פי דרישה, תוך עקיפת שער השינוי הרגיל. `reason` הוא הערת ביקורת נדרשת. נדחה עבור טבלאות מפודררות-חי (ללא שורות נחותות). (REQ-968) [tool-verified: `provisa/api/admin/schema_mutation.py:2689-2782`]

#### `invalidateFileSource(tableId: Int!) → MutationResult`

כפיית סנכרון מחדש מהדיסק בגישה הבאה של טבלת מחבר-קובץ SQLite. [tool-verified: `provisa/api/admin/schema_mutation.py:2877-2880`]

#### `registerEntity(input: EntityInput!) → MutationResult`

תחביר נוח (Sugar) לרישום ישות ממד/hub. יורד ל-MV (דו-זמני, כאשר הוסטר) וקורא ל-`registerTable`. (REQ-1164) [tool-verified: `provisa/api/admin/schema_mutation.py:1721-1725`]

#### `registerFact(input: FactInput!) → MutationResult`

תחביר נוח (Sugar) לרישום עובדה (fact) בסכמת כוכב (star-schema). יורד ל-MV צובר, יוצר קשרי ממד, ורושם אוטומטית מדדי עובדה כמדדים מנוהלים. (REQ-1164, REQ-1320) [tool-verified: `provisa/api/admin/schema_mutation.py:1728-1776`]

---

### קשרים

#### `upsertRelationship(input: RelationshipInput!) → MutationResult`

יצירה או עדכון קשר. השער בודק את תחום טבלת המקור (לא זה של היעד). קשת חוצת-תחומים נשמרת עם `needsReview: true`. קורא שאין לו `create_relationship` מועמד בתור כבקשת יצירה. קשתות צומת (many-to-many) דורשות אורכי רשימת-מפתח תואמים. (REQ-019, REQ-020, REQ-366, REQ-434) [tool-verified: `provisa/api/admin/schema_mutation.py:2297-2300`]

#### `deleteRelationship(id: String!) → MutationResult`

מחיקת קשר לפי מזהה ובניה מחדש של סכמות. [tool-verified: `provisa/api/admin/schema_mutation.py:2303-2322`]

---

### תחומים

#### `createDomain(input: DomainInput!) → MutationResult`

יצירת תחום. מילות מקטע שמורות (`tables`, `relationships`, ומקטעי נתיב URI אחרים) נדחות, וכך גם ה-wildcard המילולי `*`. דורש את היכולת `org_settings`. (REQ-021, REQ-1531) [tool-verified: `provisa/api/admin/schema_mutation.py:1135-1189`]

#### `deleteDomain(id: String!) → MutationResult`

מחיקת תחום. דורש `org_settings`. (REQ-1531) [tool-verified: `provisa/api/admin/schema_mutation.py:1192-1213`]

#### `updateGqlNamingConvention(convention: String!) → MutationResult`

קביעת מוסכמת השמות הגלובלית של GraphQL ובניה מחדש של סכמות עבור כל התפקידים. רק שמות מוסכמה מוכרים מתקבלים. (REQ-253, REQ-416) [tool-verified: `provisa/api/admin/schema_mutation.py:2571-2592`]

---

### תפקידים

#### `createRole(input: RoleInput!) → MutationResult`

יצירה או החלפה של תפקיד עם יכולות, גישת תחום, מגבלות קצב אופציונליות, ותפקיד הורה אופציונלי. מאמת שההורה קיים ושרשרת ההורים חופשית ממעגלים. דורש `user_management`. (REQ-042, REQ-1531, REQ-1677) [tool-verified: `provisa/api/admin/schema_mutation.py:1657-1712`]

#### `deleteRole(id: String!) → MutationResult`

מחיקת תפקיד. נכשל אם תפקידים אחרים יורשים ממנו — יש להורות אותם מחדש (reparent) תחילה. דורש `user_management`. (REQ-1531, REQ-1677) [tool-verified: `provisa/api/admin/schema_mutation.py:2032-2063`]

---

### כללי RLS

#### `upsertRlsRule(input: RLSRuleInput!) → MutationResult`

יצירה או עדכון של כלל אבטחה ברמת השורה. ביטוי המסנן מאומת בזמן השמירה כנגד עמודות הטבלה או התחום היעד, כך שכלל שהמנהל אינו יכול לשאול נדחה עם הסיבה במקום להיכשל בשקט בזמן שאילתה. דורש `masking_config`. (REQ-041, REQ-402, REQ-1531, REQ-1676) [tool-verified: `provisa/api/admin/schema_mutation.py:2066-2136`]

היעדים בלעדיים הדדית: קבע `tableId` עבור כלל ברמת-טבלה, `domainId` עבור כלל ברמת-תחום, או `actionName` עבור פונקציה/webhook עקובים. (REQ-1679)

#### `deleteRlsRule(roleId, tableId, domainId, actionName) → MutationResult`

מחיקת כלל RLS. דורש `masking_config`. (REQ-1531) [tool-verified: `provisa/api/admin/schema_mutation.py:2139-2178`]

---

### מוצרי נתונים

#### `createDataProduct(input: DataProductInput!) → MutationResult`

יצירה או החלפה של מוצר נתונים. דורש את היכולת `data_product_rw`. (REQ-1634) [tool-verified: `provisa/api/admin/schema_mutation.py:1216-1259`]

#### `deleteDataProduct(id: String!) → MutationResult`

מחיקת מוצר נתונים. דורש `data_product_rw`. (REQ-1634) [tool-verified: `provisa/api/admin/schema_mutation.py:1262-1285`]

---

### תגים

#### `upsertTag(input: TagInput!) → MutationResult`

יצירה או עדכון של הגדרת תג. תגי מערכת ותגים נגזרים אינם ניתנים להגדרה מחדש. `appliesTo` חייב להיות תת-קבוצה לא-ריקה של `["source", "table", "column", "relationship", "command"]`. (REQ-1373, REQ-1375) [tool-verified: `provisa/api/admin/schema_mutation.py:1288-1361`]

#### `deleteTag(id: String!) → MutationResult`

מחיקת תג. דוחה תגי מערכת ותגים נגזרים. (REQ-1373) [tool-verified: `provisa/api/admin/schema_mutation.py:1364-1393`]

#### `assignTag(input: TagAssignmentInput!) → MutationResult`

שיוך תג למקור, טבלה, עמודה, קשר, או פקודה. אוכף את מדיניויות השדה של התג (`reason_policy`, `expires_policy`) — ועבור תגים בעלי פרמטרים — מאמת את ערך הפרמטר כנגד הרשימה המותרת של התג. (REQ-1376, REQ-1377) [tool-verified: `provisa/api/admin/schema_mutation.py:1396-1527`]

#### `unassignTag(input: TagAssignmentInput!) → MutationResult`

הסרת שיוך תג. (REQ-1377) [tool-verified: `provisa/api/admin/schema_mutation.py:1530-1564`]

#### `upsertTagParamValue(input: TagParamValueInput!) → MutationResult`

הוספה או תיאור מחדש של ערך פרמטר מותר עבור תג בעל פרמטרים. רשימת הערכים המותרים סגורה: כל שיוך חייב לנקוב בערך ממנה. (REQ-1467) [tool-verified: `provisa/api/admin/schema_mutation.py:1567-1616`]

#### `deleteTagParamValue(tagId: String!, value: String!) → MutationResult`

הסרת ערך מותר. נדחה כל עוד שיוך כלשהו עדיין נושא אותו, מכיוון ששיוכים אלה ינקבו בטיפוס שהרשימה כבר אינה מכילה. (REQ-1467) [tool-verified: `provisa/api/admin/schema_mutation.py:1619-1655`]

---

### מדדים

#### `upsertMetric(input: MetricInput!) → MutationResult`

יצירה או החלפה של הגדרת מדד מנוהל. הביטוי חייב להתפענח תחת sqlglot ולהכיל לפחות פונקציית צבירה אחת. יוצר מחדש את כל ה-views המורכבים-מדד המפנים למדד זה. דורש `table_registration`. (REQ-1317, REQ-1318) [tool-verified: `provisa/api/admin/schema_mutation.py:1779-1831`]

#### `deleteMetric(name: String!) → MutationResult`

מחיקת מדד מנוהל. בונה מחדש סכמות. דורש `table_registration`. (REQ-1317) [tool-verified: `provisa/api/admin/schema_mutation.py:1834-1856`]

---

### לוחות שנה

#### `createCalendar(input: CalendarInput!) → MutationResult`

יצירה או החלפה של לוח שנה גרסתי של גבול תמונת מצב. מאומת על ידי בניית ה-`Calendar` בזיכרון לפני שמירה — נכשל על מערכת בסיס לא ידועה, אזור זמן שגוי, או עוגן פיסקלי שגוי. (REQ-962) [tool-verified: `provisa/api/admin/schema_mutation.py:525-579`]

#### `deleteCalendar(name: String!) → MutationResult`

מחיקת לוח שנה (כל הגרסאות). נדחה כאשר MV כלשהו מפנה אליו. (REQ-962) [tool-verified: `provisa/api/admin/schema_mutation.py:582-613`]

---

### Materialized Views

#### `refreshMv(mvId: String!) → MutationResult`

הפעלת רענון ידני של Materialized View. מתאם על פני הצי כאשר מצב העקביות של ה-MV הוא `shared`. (REQ-133, REQ-158, REQ-879) [tool-verified: `provisa/api/admin/schema_mutation.py:2787-2813`]

#### `toggleMv(mvId: String!, enabled: Boolean!) → MutationResult`

הפעלה או השבתה של Materialized View. [tool-verified: `provisa/api/admin/schema_mutation.py:2816-2839`]

---

### מטמון

#### `purgeCache → MutationResult`

ניקוי כל תוצאות השאילתה השמורות במטמון. [tool-verified: `provisa/api/admin/schema_mutation.py:2844-2858`]

#### `purgeCacheByTable(tableId: Int!) → MutationResult`

ניקוי תוצאות שמורות במטמון עבור טבלה אחת. [tool-verified: `provisa/api/admin/schema_mutation.py:2861-2875`]

---

### משימות מתוזמנות

#### `createScheduledTask(id, name, cron, kind, webhookName, argsJson, sql) → MutationResult`

יצירת טריגר מתוזמן — קריאת webhook או הצהרת SQL — ורישומו חי ב-APScheduler. `kind` הוא `"webhook"` או `"sql"`. (REQ-1003, REQ-1004) [tool-verified: `provisa/api/admin/schema_mutation.py:2923-2936`]

#### `deleteScheduledTask(taskId: String!) → MutationResult`

הסרת טריגר מתוזמן מקובץ ההגדרות ומהמתזמן החי. (REQ-1003) [tool-verified: `provisa/api/admin/schema_mutation.py:2939-2941`]

#### `toggleScheduledTask(taskId: String!, enabled: Boolean!) → MutationResult`

הפעלה או השבתה של משימה מתוזמנת בקובץ ההגדרות. [tool-verified: `provisa/api/admin/schema_mutation.py:2885-2920`]

---

### איכות נתונים

#### `dryRunDqContract(sourceId: String!, contractText: String!) → DqDryRunType`

הרצת חוזה כנגד הטבלה החיה והחזרת תוצאות מבלי להנחית דבר. mutation ולא query מכיוון שהיא עולה סריקה אמיתית. מה שהיא מוכיחה הוא האם מזהה ערכת הנתונים נפתר לטבלה המנוהלת שהמפעיל מתכוון אליה. (REQ-1443 clause 7) [tool-verified: `provisa/api/admin/schema_mutation.py:463-487`]

#### `runDqCheckNow(schemaName: String!, tableName: String!) → MutationResult`

הפעלת עבודת הסקר (poll) של טבלת בודק מיידית. נוחתת שורות בדרך הרגילה, כך שתוצאות נשמרות והיסטוריית ה-DQ מציגה את הסריקה החדשה. (REQ-1443) [tool-verified: `provisa/api/admin/schema_mutation.py:490-522`]

---

### תחזוקת סכמה

#### `rebuildSchemas → MutationResult`

בניית הסכמה בזיכרון מחדש ממצב מסד הנתונים. שימושי לאחר שינויי מסד נתונים חיצוניים. [tool-verified: `provisa/api/admin/schema_mutation.py:456-461`]

---

### בקשות יצירה

#### `executeCreationRequest(requestId: Int!) → MutationResult`

מחזיק זכות מבצע בקשת יצירה שהועמדה בתור — קשר, view, או webhook. דורש את היכולת שהבקשה ממתינה לה. (REQ-434, REQ-063) [tool-verified: `provisa/api/admin/schema_mutation.py:2181-2255`]

#### `rejectCreationRequest(requestId: Int!, reason: String!) → MutationResult`

דחיית בקשה שהועמדה בתור עם סיבה הניתנת לפעולה. `reason` נדרש. (REQ-434, REQ-063) [tool-verified: `provisa/api/admin/schema_mutation.py:2258-2294`]

---

### הידור שאילתה

#### `compileQuery(input: CompileQueryInput!) → [CompileQueryResult!]!`

הידור שאילתת GraphQL של מישור-נתונים כנגד סכמה של תפקיד והחזרת החלטת הניתוב המלאה: SQL סמנטי, SQL מנוע, SQL ישיר, מסלול, מטא-דאטה של אכיפה (מסנני RLS שהוחלו, עמודות שהוחרגו, מיסוך שהוחל), ו-Cypher מהודר. מחזיר תוצאה אחת לכל שדה שורש בשאילתה. (REQ-161) [tool-verified: `provisa/api/admin/schema_mutation.py:3011-3055`]

```graphql
mutation {
  compileQuery(input: {
    role: "analyst"
    query: "{ orders { id customer_id total } }"
  }) {
    sql
    semanticSql
    engineSql
    route
    routeReason
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      maskingApplied
    }
  }
}
```

שדות `CompileQueryInput`:

| שדה | סוג | תיאור |
|-------|------|-------------|
| `query` | `String!` | שאילתת GraphQL של מישור-נתונים להידור |
| `role` | `String!` | תפקיד שכנגד סכמתו יש להדר |
| `variables` | `JSON` | כריכות משתנה |
| `flatSql` | `Boolean` | החזר מחרוזת SQL שטוחה יחידה במקום זוג סמנטי/מנוע |
| `flatCypher` | `Boolean` | השטח את פלט ה-Cypher |
| `nodeOnlyCypher` | `Boolean` | הפק Cypher בעל-צמתים-בלבד (ללא תבניות קשת) |

---

## טיפוסי קלט מפתח

### `SourceInput`

[tool-verified: `provisa/api/admin/types.py:590-611`]

| שדה | סוג | הערות |
|-------|------|-------|
| `id` | `String!` | מזהה מקור |
| `type` | `String!` | סוג מחבר (למשל `postgres`, `files`, `openapi`) |
| `host` | `String` | |
| `port` | `Int` | |
| `database` | `String` | |
| `username` | `String` | |
| `password` | `String` | טקסט גלוי או הפניית `${secret:NAME}` |
| `path` | `String` | נתיב מערכת-קבצים עבור מקורות קובץ/CSV |
| `federationHintsJson` | `String` | אובייקט JSON עבור תוספות מחסן (warehouse/role של Snowflake, http_path של Databricks) |
| `changeSignal` | `String` | `ttl` \| `probe` \| `ttl_probe` (REQ-929) |
| `loadProtected` | `Boolean` | רענון מתוזמן בלבד (REQ-1141) |
| `offPeakWindow` | `String` | חלון תחזוקה `HH:MM-HH:MM` |
| `offPeakTz` | `String` | אזור זמן IANA |
| `cdc` | `SourceCdcConfigInput` | הגדרת תעבורת Kafka CDC (REQ-824) |

### `TableInput`

[tool-verified: `provisa/api/admin/types.py:745-800`]

קלט רישום הטבלה המרכזי. שדות מפתח מעבר ליסודות:

| שדה | הערות |
|-------|------|
| `materialize` | הנחתת עותק בחנות המטריאליזציה |
| `mvRefreshInterval` | שניות בין רענונים |
| `mvPersist` | `replace` \| `append` \| `upsert` (REQ-965) |
| `mvIncremental` | תחזוקה מצטברת (REQ-969) |
| `mvBitemporalMode` | `snapshot` \| `delta` עבור טבלאות דו-זמניות (REQ-1162) |
| `mvCalendar` | שם לוח שנה של תמונת מצב (REQ-962) |
| `mvGrain` | גרעיניות תמונת מצב: `daily`, `weekly`, `monthly`, `annual`, או מותאם `3WE` / `LFR` (REQ-962) |
| `viewSql` | SQL עבור view נגזר |
| `viewMetrics` | מפרט view הצהרתי מורכב-מדד — בלעדי הדדית עם `viewSql` (REQ-1318) |
| `dqContract` | טקסט חוזה איכות-נתונים YAML/JSON (REQ-1443) |
| `queryTemplate` | Cypher עבור טבלת Neo4j (REQ-1670) |
| `live` | הגדרת אספקה חיה עבור דחיפת SSE/Kafka (REQ-565, REQ-813) |
| `discover` | הסק עמודות מהמקור החי בעת הרישום (REQ-252) |

### `RelationshipInput`

[tool-verified: `provisa/api/admin/types.py:804-826`]

| שדה | הערות |
|-------|------|
| `id` | מזהה קשר |
| `sourceTableId` | שם טבלה וירטואלית (כינוי אם הוגדר, אחרת שם טבלה) |
| `targetTableId` | שם טבלה וירטואלית; ריק עבור קשרים מחושבים |
| `sourceColumn` | עמודת join בצד המקור |
| `targetColumn` | עמודת join בצד היעד |
| `cardinality` | `one-to-one` \| `one-to-many` \| `many-to-one` \| `many-to-many` |
| `alias` | תווית קשת Cypher (למשל `WORKS_FOR`) |
| `graphqlAlias` | שם שדה GraphQL בטיפוס המקור |
| `viaTable` | שם טבלת צומת עבור קשתות many-to-many (REQ-1586) |
| `recordCandidate` | כתוב גם שורת relationship_candidates מסוג `accepted` |

---

## דוגמה: רישום טבלה

```graphql
mutation {
  registerTable(input: {
    sourceId: "sales-pg"
    domainId: "sales"
    schemaName: "public"
    tableName: "orders"
    alias: "orders"
    columns: [
      { name: "id", visibleTo: ["public"], isPrimaryKey: true }
      { name: "customer_id", visibleTo: ["public"] }
      { name: "total", visibleTo: ["public"] }
    ]
  }) {
    success
    message
    code
  }
}
```

## דוגמה: יצירת כלל RLS

```graphql
mutation {
  upsertRlsRule(input: {
    tableId: "orders"
    roleId: "regional-analyst"
    filterExpr: "region = '{{user.region}}'"
  }) {
    success
    message
  }
}
```
