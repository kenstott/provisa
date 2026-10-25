# הפניית Admin GraphQL API {: #admin-graphql-api-reference }

ה-API של Admin GraphQL הוא מישור ההגדרות של Provisa. זהו ה-API שאפליקציית ה-Web של הניהול קוראת לו בכל פעולת ניהול: יצירת מקורות, רישום טבלאות, הגדרת קשרים, הגדרת כללי RLS, וכל דבר אחר שמעצב את המודל.

**נקודת עגינה (Mount point):** `POST /admin/graphql`

זהו API שונה מזה של מישור הנתונים ב-`/data/graphql`. מישור הנתונים משרת שאילתות משתמש-קצה מעל תחומים רשומים ומתואר על ידי ה-SDL תחת `/data/sdl`. ה-API של הניהול מגדיר כיצד נראית אותה סכמה ומי רשאי לראות מה.

---

## כיצד ה-UI מדבר עם API זה {: #how-the-ui-talks-to-this-api }

אפליקציית ה-Web של הניהול משתמשת ב-Apollo Client, המצביע על `${API_BASE}/admin/graphql`.

כל בקשה נושאת אסימון bearer (הנשלף מחדש מספק האימות בכל קריאה), כותרת `X-Org-Id` בסביבת ריבוי-דיירים, וכותרת `X-Env` בעת שירות סביבת ענף.

הסכמה מורכבת משתי מחלקות `@strawberry.type` — `Query` מ-`schema_query.py` ו-`Mutation` מ-`schema_mutation.py` — ועטופה ב-`ModelCommitExtension` הרושם כל mutation כנגד ענף הסביבה הנוכחי.

---

## הרשאה {: #authorization }

**מצב פיתוח:** כאשר אין אימות מוגדר וכל בקשה מגיעה כעקרון אנונימי, כל בדיקות היכולת מדולגות. זה שומר על התקנה מקומית פונקציונלית ללא הגדרת אימות.

**שערי יכולת (Capability gates):** פריסות ייצור אוכפות יכולות בעלות שם. הזכות הספציפית הנדרשת לכל שדה מצוינת בתוך הטקסט. קריאה ל-mutation ללא היכולת הנדרשת מעלה `PermissionError`. תפקיד מנהל הפלטפורמה עוקף את כל בדיקות היכולת.

**שערי תחום (Domain gates):** מספר mutations בודקות גם את התחום שהאובייקט שייך אליו. קורא המוגבל לתחום `sales` אינו יכול לרשום טבלה לתוך `finance`, להעמיד בתור כלל RLS עבורה, או ליצור קשר שהטבלת המקור שלו נמצאת בתחום שאין הוא מחזיק. Views מוגבלים עוד יותר: כל טבלה ש-SQL של ה-view קורא ממנה חייבת להיות בתוך התחומים של הקורא, מכיוון ש-SQL חופשי אחרת מעניק לחבר גישה לנתונים מחוץ להיקפו.

**הורשת תפקיד:** יכולות של תפקיד הורה נורשות על ידי תפקידי הצאצא. `createRole` ו-`deleteRole` דוחים מעגלים ומונעים מחיקת תפקיד שיש לו יורשים.

---

## סוג החזרה משותף {: #common-return-type }

רוב ה-mutations מחזירות `MutationResult`.

```graphql
type MutationResult {
  success: Boolean!
  message: String!
  code: String          # stable i18n key, e.g. "schema.source_created"
  params: JSON          # key/value pairs for client-side localization
}
```

כאשר mutation נכשלת, `success` הוא `false` ו-`message` נושא את הסיבה באנגלית. `code` הוא מזהה יציב שה-UI משתמש בו לצורך הצגת הודעה מתורגמת.

---

## Queries

### מקורות {: #sources }

#### `sources → [SourceType!]!`

כל מקורות הנתונים הרשומים.

```graphql
query {
  sources {
    id type host port database username dialect
    cacheEnabled cacheTtl replicate
    loadProtected offPeakWindow offPeakTz
    gqlNamingConvention path allowedDomains
    description mappingJson federationHintsJson
    changeSignal passwordRef
    cdc {
      bootstrapServers topicPrefix consumerGroupId
      schemaRegistryUrl schemaRegistryAuth schemaRegistryUsername schemaRegistryPassword
      schemaRegistryToken schemaRegistryClientCert schemaRegistryClientKey schemaRegistryCa
    }
  }
}
```

`passwordRef` הוא הפניית `${secret:NAME}` לתוך כספת הארגון, ולעולם לא האישור המילולי.

#### `source(id: String!) → SourceType`

מקור בודד לפי מזהה. מחזיר `null` כאשר לא נמצא.

#### `availableSchemas(sourceId: String!) → [String!]!`

סכמות גלויות במקור, מסוננות להחריג את אלה הפנימיות של Provisa. משתמש בבדיקה עצמית (introspection) ילידית תחילה; נופל חזרה לקטלוג המנוע כאשר לסוג המקור אין pool ישיר.

#### `availableTables(sourceId: String!, schemaName: String = "public") → [AvailableTableType!]!`

טבלאות בסכמה אחת של מקור, עם הערותיהן. עבור מקורות OpenAPI, מחזיר פעולות GET שהתגובה שלהן היא מערך או עוטף עימוד. עבור מקורות GraphQL, מחזיר שדות query המחזירים רשימה. עבור gRPC, מחזיר RPC-ים מסוג server-streaming.

#### `availableColumns(sourceId: String!, schemaName: String!, tableName: String!) → [String!]!`

שמות עמודות לטבלה בקטלוג המנוע. עבור מקורות govdata, משתמש ב-resolver נפרד.

#### `availableColumnsMetadata(sourceId: String!, schemaName: String!, tableName: String!) → [AvailableColumnType!]!`

שמות עמודות עם סוגי נתונים, הערות, סוגי מסנן ילידי, ודגלי מפתח ראשי. עבור מקורות OpenAPI, נגזר מהצורה של סכמת התגובה והפרמטרים של הפעולה.

#### `availableFunctions(sourceId: String!, schemaName: String = "openapi") → [AvailableTableType!]!`

פעולות שאינן GET עבור מקור OpenAPI (POST, PUT, PATCH, DELETE). מחזיר רשימה ריקה עבור מקורות שאינם OpenAPI.

#### `crawlSource(path, depth, pattern, recursive, simpleLinks, sameDomain, excludePattern) → CrawlResultType`

תצוגה מקדימה של מה שסריקת מחבר-קובץ (file-connector) תגלה — קבצים, טבלאות ועמודות — לפני יצירת מקור. ההגדרות שהן HTTP-בלבד (`simpleLinks`, `sameDomain`, `excludePattern`) מתעלמות עבור שורשי local, S3, FTP ו-SFTP.

#### `suggestTableAlias(tableName: String!, domainId: String!, sourceId: String!) → String!`

מחזיר את הכינוי (alias) לשימוש בעת רישום `tableName` בתוך `domainId` מ-`sourceId`. מחזיר כינוי snake-case פשוט כשאין התנגשות, או כינוי בעל קידומת מקור (`sqlite_b_orders`) כאשר השם האפקטיבי כבר תפוס על ידי מקור אחר באותו תחום.

---

### טבלאות {: #tables }

#### `tables → [RegisteredTableType!]!`

כל הטבלאות הרשומות, כל אחת עם רשימת העמודות המלאה שלה. נראות עמודה בתגובה מכבדת את היכולת `table_registration` של הקורא: `canDeployToDb` מותנה בכך שהקורא מחזיק בזכות זו.

כל `RegisteredTableType` חושף תת-שדות מחושבים:

- **`refreshPolicySummary → RefreshPolicySummaryType`** — מדיניות הרענון/השירות האפקטיבית כטקסט רגיל, נגזרת בצד השרת מאותה פתרון מתכנן (planner) שהמנוע משתמש בו. מחזיר `null` בזמן ההפעלה.
- **`graphqlFieldName → String`** — שם השדה שיש לטבלה זו בסכמת מישור הנתונים המהודרת, כך שפאנל מוצר הנתונים יכול לבנות דוגמה הניתנת להרצה מבלי לשכפל את אלגוריתם השמות.
- **`dqDataset → String`** — טבלה זו כערכת נתונים של חוזה איכות-נתונים, בצורה שהבודק (checker) סורק.
- **`productId → String`** — מוצר הנתונים שאליו טבלה זו שייכת. טבלת בודק DQ יורשת את המוצר של הטבלה שהחוזה שלה סורק.

#### `refreshPolicyPreview(...) → RefreshPolicySummaryType`

תצוגה מקדימה של סיכום הרענון/השירות האפקטיבי עבור ידיות טבלה *טיוטה* (שאינן שמורות), כך שסיכום ראש הטופס מתעדכן ככל שהשדות משתנים ללא שמירה של דבר. אותה גזירה כמו `refreshPolicySummary` לעיל.

ארגומנטים: `sourceId`, `domainId`, `schemaName`, `tableName`, `cacheTtl`, `replicate`, `loadProtected`, `offPeakWindow`, `offPeakTz`, `changeSignal`.

#### `columnDependents(tableId: String!, renamed: [String!], removed: [String!]) → [ColumnDependentsType!]!`

מוצרים שהמרה ממתינה של כינוי או מחיקת עמודה תשבור. ייעוצי בלבד: ה-UI של הניהול מציג זאת לפני השמירה והמנהל מחליט. חייב להיקרא *לפני* השמירה, מכיוון שהתלויות נכתבו כנגד השם החשוף שהעמודה נושאת כרגע.

---

### קשרים {: #relationships }

#### `relationships → [RelationshipType!]!`

כל הקשרים המוגדרים על ידי משתמש (מחריג רשומות `gql_auto__` שנוצרות אוטומטית ורשומות `meta:%` הסינתטיות המשמשות את ה-ERD).

#### `allRelationships → [RelationshipType!]!`

זהה ל-`relationships`, אך כולל רשומות `meta:%` סינתטיות. משמש את ה-ERD הגרפי, הזקוק להציג כל קשת כולל הקישורים המשתמעים `HAS_TABLE` בין טבלאות נתונים לבין רשם המטא-דאטה.

כל `RelationshipType` חושף:

- **`autoSuggested → Boolean`** — האם הקשר הוצע על ידי ניתוח FK (`id` מתחיל ב-`fk__`).
- **`physicalName → String`** — שם הקשר במישורי SQL ו-gRPC (הפרמטר `?include=`). נגזר בצד השרת; לקוחות אסור להם לתעתק את הכינוי של GraphQL.

---

### תחומים, תפקידים ומשתמשים {: #domains-roles-users }

#### `domains → [DomainType!]!`

כל התחומים במסד הנתונים של דייר הארגון הפעיל. מסד הנתונים של הדייר מבודד ברמת הסכמה, כך שרשימת התחומים של org-admin מכילה רק את שורות הארגון שלו.

#### `roles → [RoleType!]!`

תפקידים גלויים לקורא. מנהל רואה כל תפקיד; מי שאינו מנהל רואה רק תפקידים ללא `org_id` או תפקידים השייכים לארגונו.

#### `resolveOwners(refs: [String!]!) → [UserSummaryType!]!`

פותר מזהי תפקיד או מזהי משתמש למשתמשים בודדים. משמש להרחבת `DataProduct.ownerRole`, `Domain.steward`, ו-`Column.visibleTo` לרשימה קריאה לבני אדם. הפניות לא ידועות מוחזרות כלשונן כך שה-UI מציג את המזהה הגולמי ולא כלום.

---

### כללי RLS {: #rls-rules }

#### `rlsRules → [RLSRuleType!]!`

כל כללי האבטחה ברמת השורה. המאגר הבסיסי מפענח את `filterExpr` בגבול.

---

### מוצרי נתונים {: #data-products }

#### `dataProducts → [DataProductType!]!`

כל מוצרי הנתונים. דורש את היכולת `data_product_read`.

---

### תגים {: #tags }

#### `tags → [TagType!]!`

כל הגדרות התגים, כולל ערכי הפרמטרים המותרים לכל תג.

#### `tagAssignments → [TagAssignmentType!]!`

כל שיוכי התגים על פני מקורות, טבלאות, עמודות וקשרים.

---

### Materialized Views

#### `mvList → [MVType!]!`

כל ה-Materialized Views עם מצב הריצה שלהם: מופעל/מושבת, חותמת זמן רענון אחרונה, מספר שורות, ושגיאה אחרונה.

---

### מטמון {: #cache }

#### `cacheStats → CacheStatsType`

סטטיסטיקות מטמון. מחזיר `storeType: "redis"` עם מדדים תפעוליים מלאים כאשר Redis מוגדר, `storeType: "memory"` עבור חנות ה-fakeredis המוטבעת, ו-`storeType: "noop"` כאשר לא מוגדר מטמון.

#### `cacheTableStats → [CacheTableStatType!]!`

ספירות רשומות שמורות במטמון לכל טבלה. ריק כאשר אין חנות מטמון מוגדרת.

#### `hotTables → [HotTableStatType!]!`

טבלאות ש-Provisa שומרת עותק שלהן, על פני שתי דרגות: `hot` (משוקפת אל חנות התגובה לצורך הטמעת JOIN) ו-`warm` (נוחתת כעותק Iceberg). טבלה נמצאת בדרגה אחת לכל היותר.

#### `materializeStoreInfo → MaterializeStoreInfoType`

זהות חנות המטריאליזציה העמידה: שם מנוע, הפניית DSN של החנות, ספירת MV, והאם החנות מקומית-למופע (קובץ מקומי כמו DuckDB או SQLite, שמשמעו שכל מופע מאחורי מאזן עומסים שומר עותק משלו).

---

### תקינות המערכת {: #system-health }

#### `systemHealth → SystemHealthType`

מצב חיבור המנוע, ספירות מאגר עובדים, מצב מאגר מסד המטא-דאטה, מצב מטמון, ותקינות של כל מאזין פרוטוקול (pgwire, gRPC, Arrow Flight, Bolt).

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

### משימות מתוזמנות {: #scheduled-tasks }

#### `scheduledTasks → [ScheduledTaskType!]!`

טריגרים מתוזמנים מקובץ ההגדרות עם מצב ריצה. כל רשומה נושאת את ביטוי ה-cron שלה, `kind` (`webhook` או `sql`), האם היא מופעלת כרגע, חותמת הזמן של ההרצה האחרונה (תמיד `null`; המתזמן עוקב אחריו), וזמן ההרצה המתוזמן הבא מ-APScheduler.

---

### איכות נתונים {: #data-quality }

#### `dqContractParse(checker: String!, contractText: String!) → DqContractType`

פענח טקסט חוזה גולמי לשורות הניתנות לעריכה של פאנל הבנייה. נקרא בכל עריכה; כשל פענוח חוזר כ-`error` ולא כשגיאת GraphQL, מכיוון שטקסט כתוב-למחצה נורמלי בעת שהמפעיל מקליד.

#### `dqCheckCatalog(checker: String!, dataset: String!) → DqCheckCatalogType`

הבדיקות ש-`checker` מציע, מוגבלות לעמודות של `dataset`. ערכת הנתונים היא היעד הנצפה של החוזה, נפתר באותה דרך שהסורק פותר אותו, כך שהבדיקות המוצעות תואמות לעמודות שהבודק יראה באמת.

#### `dqCheckDefinition(checker: String!, check: DqCheckBuildInput!) → DqCheckDefinitionType`

הטקסט של בדיקה אחת מעורכי הפאנל. בצד השרת מכיוון שלדיאלקט יש מימוש אחד; בדיקה שנבנתה בבונה ובדיקה שהוקלדה ידנית חייבות להיות בלתי-ניתנות להבחנה.

#### `dqContractBuild(checker: String!, dataset: String!, checks: [DqCheckInput!]!) → DqContractTextType`

סריאליזציה של שורות בדיקה שנערכו חזרה לטקסט חוזה. ההופכי של `dqContractParse`. בצד השרת מאותה סיבה: הפאנל אינו יכול להפיק טקסט שהבודק היה דוחה.

---

### תצוגות מקדימות של מקורות {: #source-previews }

#### `neo4jPreview(sourceId: String!, cypher: String!) → QueryPreviewType`

תצוגה מקדימה של הטלת Cypher על מקור Neo4j: עד חמש שורות וסוגי העמודות שהרישום ישא. כשלים חוזרים כ-`error`.

#### `sparqlPreview(sourceId: String!, query: String!) → QueryPreviewType`

תצוגה מקדימה של SPARQL SELECT על מקור SPARQL: עד חמש שורות, כל העמודות כטקסט.

---

### Kaggle

#### `kaggleTokenValid(token: String!) → Boolean!`

בדיקה חיה כנגד ה-API של Kaggle. מחזיר `true` רק כאשר האסימון מאמת. תומך בשלב שער-האסימון בטופס מקור Kaggle.

#### `kaggleDatasets(token: String!, query: String = "", page: Int = 1) → [KaggleDatasetType!]!`

חיפוש בקטלוג ערכות הנתונים הציבורי המלא של Kaggle. מוגבל-אסימון.

---

### לוחות שנה {: #calendars }

#### `calendars → [CalendarType!]!`

כל גרסאות לוח השנה של גבול תמונת המצב הרשומות. מזין את בורר הגדרות לוח הזמנים של תמונת המצב ומאשר לאילו לוחות שנה MV תקופתי רשאי להפנות.

---

### מדדים {: #metrics }

#### `metrics → [MetricType!]!`

כל הגדרות המדד המנוהלות. מדדים הנגזרים מעובדה (fact) נושאים `fromFact`.

---

### גרסת סכמה {: #schema-version }

#### `schemaVersion → String!`

hash מסוג SHA-256 של מצב הסכמה הנוכחי (תחומים, מזהי טבלה, מזהי קשר). לקוח Apollo קורא ערך זה מכותרת התגובה `X-Schema-Version` ומבצע fetch מחדש לכל השאילתות הפעילות כשהוא מתקדם.

---

### עוזרי AI {: #ai-helpers }

#### `generateTableDescription(tableId: String!) → String!`

השתמש ב-LLM המוגדר ליצירת תיאור של משפט אחד עד שניים עבור טבלה רשומה. שמור את הטבלה תחילה; קריאה לפעולה זו על טבלה לא-שמורה מחזירה הודעה הנחייתית.

#### `generateColumnDescription(tableId: String!, columnName: String!) → String!`

השתמש ב-LLM המוגדר ליצירת תיאור של משפט אחד עבור עמודה בודדת.

---

### בקשות יצירה {: #creation-requests }

#### `creationRequests → [CreationRequestType!]!`

בקשות יצירה ממתינות, הגלויות לקוראים שיש להם יכולת היצירה המתאימה. משמש כשחבר ללא `create_relationship` או `create_view` שולח בקשה שמחזיק הזכות צריך לאשר. בקשת קשר מוצגת למשתמשים שזכות ה-`create_relationship` שלהם מגיעה לתחום של טבלת המקור או היעד שלה, ולמשתמש שהגיש אותה.

---

## Mutations

### מקורות {: #sources_1 }

#### `createSource(input: SourceInput!) → MutationResult`

רושם מקור נתונים חדש. מזהה שכבר קיים נדחה (`schema.source_id_taken`) אחרי בדיקות ההגדרה ולפני שדבר מוכן או נכתב; מקור משתנה עם `updateSource`. בודק את החיבור לפני השמירה. מקור שנדחה אינו משאיר רשומה במאגר. שומר אישורים בכספת של הארגון ורושם הפניה; טקסט גלוי לעולם אינו מגיע למסד הנתונים. דורש את היכולת `source_registration`.

#### `updateSource(input: SourceInput!) → MutationResult`

מעדכן את פרמטרי החיבור, התיאור והקונפיגורציה של מקור קיים, ואז מכין אותו מחדש על המנוע הקשור, בונה מחדש את הסכמות ומיישב את הטבלאות הטעונות שלו, כך שמקור שנשמר תחת מנוע אחד מוגש על ידי מנוע שעברו אליו מאוחר יותר. מנתק ומחבר מחדש את נקודת הקצה של pgwire למקורות קבצים/SharePoint כדי ששינוי נתיב ייכנס לתוקף מיד. דורש `source_registration`.

#### `deleteSource(id: String!) → MutationResult`

הסרת מקור ורשומת הכספת שלו. מפיל את קטלוג המנוע ובונה מחדש סכמות.

#### `renameSource(oldId: String!, newId: String!) → MutationResult`

משנה את מזהה המקור. מזהה חדש שבבעלות מקור אחר נדחה (`schema.source_id_taken`) ודבר אינו נכתב; שינוי שם של מקור למזהה שלו עצמו אינו משנה דבר.

#### `updateSourceCache(sourceId: String!, cacheEnabled: Boolean!, cacheTtl: Int) → MutationResult`

הפעלה או השבתה של מטמון תוצאות שאילתה עבור מקור, וקביעת ה-TTL בשניות.

#### `updateSourceReplicate(sourceId: String!, replicate: Int) → MutationResult`

קובע מתי טבלאות המקור מוגשות מהעותקים שלהן: `null` Default, `-1` Never, `N` גדול מ-0 — ברגע שטבלה חוצה N הצהרות מנוהלות במרווח, `0` Always. טבלה ללא ערך משלה יורשת אותו. נדחה אם היה משלב `load_protected` עם `-1` או משאיר בלי `cache_ttl` טבלת `ttl` שהוא משכפל. דורש `source_registration`.

#### `updateSourceLoadProtection(sourceId, loadProtected, offPeakWindow, offPeakTz) → MutationResult`

סימון מקור כמוגן-עומס (רענון מתוזמן בלבד). דורש לפחות שער אחד — חלון שעות שפל, קדנס TTL של מטמון, או אות שינוי בבדיקה — אחרת הקריאה נכשלת.

#### `updateSourceNaming(sourceId: String!, gqlNamingConvention: String) → MutationResult`

קביעת מוסכמת השמות של GraphQL לכל מקור.

#### `updateSourceAllowedDomains(sourceId: String!, allowedDomains: [String!]!) → MutationResult`

קביעת אילו תחומים רשאים להשתמש במקור (רשימה ריקה = ללא הגבלה). דורש `source_registration`.

#### `stageKaggleDataset(token, owner, ref, idPrefix) → KaggleStageResultType`

הורדה ופתיחת ערכת נתונים של Kaggle לדיסק המקומי. מחזיר את נתיב התיקייה שהוכנה; הקורא לאחר מכן יוצר מקור מסוג `files` המצביע אליה. חבילות הנושאות SQLite נדחות במלואן. דורש `source_registration`.

#### `refreshKaggleSource(sourceId: String!, token: String!) → MutationResult`

משיכה מחדש של ערכת הנתונים של מקור נגזר-Kaggle במקום. מדלג על ההורדה אם ל-Kaggle אין דבר חדש יותר ממה שיש בדיסק. דורש `source_registration`.

#### `refreshSourceStatistics(sourceId: String!) → MutationResult`

הרצת `ANALYZE` על כל הטבלאות הרשומות עבור מקור. משפר החלטות סדר-join ושידור עבור שאילתות פדרטיביות.

---

### טבלאות {: #tables_1 }

#### `registerTable(input: TableInput!) → MutationResult`

רושם טבלה (או view) חדשה בתוך domain, עם כל הגדרה שהקלט נושא, ובהן `gqlNamingConvention`, `roleTtl`, `replicate`, `writeGrants`, `cacheTtl`, `pagination`, `region`, `rowRules` ו-`draft`, בטרנזקציה אחת: אם אחת מהן נדחית, שום דבר לא נרשם. דורש את היכולת `table_registration` וחברות ב-domain היעד; נשיאת `writeGrants` דורשת שם גם `masking_config`. קורא בלי `create_relationship` שמגיש view נכנס לתור כבקשת יצירה שבעל ההרשאות יאשר.

#### `updateTable(input: TableInput!) → MutationResult`

מעדכן את הכינוי, התיאור, מטא-נתוני העמודות, הגדרות ה-MV ותצורת המסירה החיה של טבלה קיימת, ואת ההגדרות שהקלט נושא (`gqlNamingConvention`, `roleTtl`, `replicate`, `writeGrants`, `cacheTtl`, `pagination`, `region`, `rowRules`, `draft`), בטרנזקציה אחת: אם אחת מהן נדחית, שום דבר לא נשמר. הגדרה שהושמטה שומרת את הערך השמור שלה. דורש `table_registration`.

#### `deleteTable(id: Int!) → MutationResult`

מחיקת טבלה רשומה. מחפש את תחום הטבלה לצורך שער התחום לפני המחיקה.

#### `updateTableCache(tableId: Int!, cacheTtl: Int) → MutationResult`

דריסת ה-TTL של המטמון עבור טבלה אחת.

#### `updateTableReplicate(tableId: Int!, replicate: Int) → MutationResult`

קובע מתי טבלה אחת מוגשת מהעותק שלה. `null` = ירושת הערך של המקור. אותם ערכים ודחיות כמו `updateSourceReplicate`. דורש `table_registration`.

#### `updateTableLoadProtection(tableId, loadProtected, offPeakWindow, offPeakTz) → MutationResult`

דריסת הגנת עומס עבור טבלה אחת. `null` עבור `loadProtected` יורש את ברירת המחדל של המקור. מאמת את שילוב השערים האפקטיבי (טבלה ← מקור).

#### `updateTableNaming(tableId: Int!, gqlNamingConvention: String) → MutationResult`

קביעת מוסכמת השמות של GraphQL לכל טבלה.

#### `deployViewToDb(tableId: Int!) → MutationResult`

קידום view וירטואלי של Provisa ל-view אמיתי במסד הנתונים על המקור הילידי הבסיסי שלו.

#### `forceRegen(tableId: Int!, reason: String!) → MutationResult`

חישוב מחדש של שורות נחותות של טבלה על-פי דרישה, תוך עקיפת שער השינוי הרגיל. `reason` הוא הערת ביקורת נדרשת. נדחה עבור טבלאות מפודררות-חי (ללא שורות נחותות).

#### `invalidateFileSource(tableId: Int!) → MutationResult`

כפיית סנכרון מחדש מהדיסק בגישה הבאה של טבלת מחבר-קובץ SQLite.

#### `registerEntity(input: EntityInput!) → MutationResult`

תחביר נוח (Sugar) לרישום ישות ממד/hub. יורד ל-MV (דו-זמני, כאשר הוסטר) וקורא ל-`registerTable`.

#### `registerFact(input: FactInput!) → MutationResult`

תחביר נוח (Sugar) לרישום עובדה (fact) בסכמת כוכב (star-schema). יורד ל-MV צובר, יוצר קשרי ממד, ורושם אוטומטית מדדי עובדה כמדדים מנוהלים.

---

### קשרים {: #relationships_1 }

#### `createRelationship(input: RelationshipInput!) → MutationResult`

יוצר קשר. הבדיקה מסתכלת על התחום של טבלת המקור (לא היעד). קשת בין תחומים נשמרת עם `needsReview: true`. קורא ללא `create_relationship` נכנס לתור כבקשת יצירה. קשתות junction (רבים-לרבים) דורשות רשימות מפתחות באורך שווה. נדחה בשמו לפני כל כתיבה: מזהה שכבר קיים (`schema.relationship_id_taken`); שם טבלה שאינו מתפענח לאף טבלה רשומה או מתפענח לכמה (`schema.relationship_unknown_source`, `_target`, `_junction`, `schema.relationship_ambiguous_table`); כינוי Cypher או GraphQL שכבר תפוס על ידי קשר אחר של אותה טבלת מקור (`schema.relationship_alias_taken`, `schema.relationship_graphql_alias_taken`). טבלה נקובה בשמה הווירטואלי: הכינוי שלה, כשיש לה.

#### `updateRelationship(input: RelationshipInput!) → MutationResult`

משנה קשר קיים עם אותן בדיקות כמו `createRelationship`. מזהה שאינו בבעלות אף קשר נדחה (`schema.relationship_not_found`). שינוי צריך את הזכות עצמה; הוא לעולם אינו נכנס לתור כבקשה.

שמירה שמעבירה עמודות מפתח בין תכונה למחלקה עלולה להסיר מהן את `pii`. היא נדחית אז עם `tag.move_loses`, ו-`params.columns` מפרט כל עמודה עם התגים שהיא מאבדת (`loses`) ומקבלת (`gains`). שמרו שוב עם `confirmTagLoss` שמונה כל תג שאבד, או עם `carryTags: true` כדי לשים אותם על המונח שאליו עוברת כל עמודה.

#### `deleteRelationship(id: String!, confirmTagLoss: [String!], carryTags: Boolean) → MutationResult`

מחיקת קשר לפי מזהה ובניה מחדש של סכמות. מחיקה שמעבירה עמודות מפתח מקבלת את אותם `confirmTagLoss` ו-`carryTags` כמו שמירה.

---

### תחומים {: #domains }

#### `createDomain(input: DomainInput!) → MutationResult`

יוצר תחום. מזהה שכבר קיים נדחה (`schema.domain_id_taken`). מילות מקטע שמורות (`tables`, `relationships` ושאר מקטעי נתיב ה-URI) נדחות, וכך גם ליטרל התו הכללי `*`. דורש את היכולת `org_settings`.

#### `deleteDomain(id: String!) → MutationResult`

מחיקת תחום. דורש `org_settings`.

#### `updateGqlNamingConvention(convention: String!) → MutationResult`

קביעת מוסכמת השמות הגלובלית של GraphQL ובניה מחדש של סכמות עבור כל התפקידים. רק שמות מוסכמה מוכרים מתקבלים.

---

### תפקידים {: #roles }

#### `createRole(input: RoleInput!) → MutationResult`

יוצר תפקיד עם יכולות, גישה לתחומים, מגבלות בקשות אופציונליות ותפקיד הורה אופציונלי. בודק שההורה קיים ושרשרת ההורים אינה מכילה מעגלים. אחרי בדיקות ההגדרה, מזהה שכבר קיים נדחה (`schema.role_id_taken`). דורש `user_management`.

#### `updateRole(input: RoleInput!) → MutationResult`

משנה תפקיד קיים עם אותן בדיקות כמו `createRole`. מזהה שאינו בבעלות אף תפקיד נדחה (`schema.role_not_found`). הגדרה מחדש של תפקיד היא פעולה בכל תחום שהוא מגיע אליו.

#### `deleteRole(id: String!) → MutationResult`

מחיקת תפקיד. נכשל אם תפקידים אחרים יורשים ממנו; יש להורות אותם מחדש (reparent) תחילה. דורש `user_management`.

---

### כללי RLS {: #rls-rules_1 }

#### `upsertRlsRule(input: RLSRuleInput!) → MutationResult`

יצירה או עדכון של כלל אבטחה ברמת השורה. ביטוי המסנן מאומת בזמן השמירה כנגד עמודות הטבלה או התחום היעד, כך שכלל שהמנהל אינו יכול לשאול נדחה עם הסיבה במקום להיכשל בשקט בזמן שאילתה. דורש `masking_config`.

היעדים בלעדיים הדדית: קבע `tableId` עבור כלל ברמת-טבלה, `domainId` עבור כלל ברמת-תחום, או `actionName` עבור פונקציה/webhook עקובים.

#### `deleteRlsRule(roleId, tableId, domainId, actionName) → MutationResult`

מחיקת כלל RLS. דורש `masking_config`.

---

### מוצרי נתונים {: #data-products_1 }

#### `createDataProduct(input: DataProductInput!) → MutationResult`

יוצר מוצר נתונים. מזהה שכבר קיים נדחה (`schema.data_product_id_taken`). דורש את היכולת `data_product_rw`.

#### `updateDataProduct(input: DataProductInput!) → MutationResult`

משנה מוצר נתונים קיים. מזהה שאינו בבעלות אף מוצר נדחה (`schema.data_product_not_found`). דורש `data_product_rw`.

#### `deleteDataProduct(id: String!) → MutationResult`

מחיקת מוצר נתונים. דורש `data_product_rw`.

---

### תגים {: #tags_1 }

#### `createTag(input: TagInput!) → MutationResult`

יוצר הגדרת תג. מזהה שכבר קיים נדחה (`schema.tag_id_taken`). תגי מערכת ותגים נגזרים אינם ניתנים להגדרה מחדש. `appliesTo` חייב להיות תת-קבוצה לא ריקה של `["source", "table", "column", "relationship", "command"]`.

#### `updateTag(input: TagInput!) → MutationResult`

משנה הגדרת תג קיימת עם אותן בדיקות כמו `createTag`. מזהה שאינו בבעלות אף תג נדחה (`schema.tag_not_found`).

#### `deleteTag(id: String!) → MutationResult`

מחיקת תג. דוחה תגי מערכת ותגים נגזרים.

#### `assignTag(input: TagAssignmentInput!) → MutationResult`

שיוך תג למקור, טבלה, עמודה, קשר, או פקודה. אוכף את מדיניויות השדה של התג (`reason_policy`, `expires_policy`), ועבור תג בעל פרמטר, בודק את ערך הפרמטר כנגד הרשימה המותרת של התג.

אפשר לשים `pii` גם על מונח במילון, כך שכל עמודה בעלת המשמעות הזו נושאת את התג, כולל עמודות שיירשמו בהמשך. קבעו `objectType: "term"` ו-`termId`:

```graphql
mutation { assignTag(input: {tagId: "pii", objectType: "term", termId: 12}) { success code params } }
```

על עמודה, `alsoTerm: true` שם את התג במקום זאת על המונח של העמודה. `params.covers` בתוצאה אומר לכמה עמודות התג מגיע: עבור עמודה, כמה עמודות אחרות מחזיק המונח שלה; עבור מונח, כמה הוא מחזיק. נדחים בשמם: תג שאינו `pii` על מונח (`tag.not_on_terms`), מונח שאינו תכונה (`tag.term_not_property`), מונח בטיוטה (`tag.term_not_live`, "הגדירו את *מונח* כדי לסמן אותו") ועם `alsoTerm`, עמודת מפתח (`tag.key_column`) או עמודה ללא מונח (`tag.column_has_no_term`). עמודה שמסומנת ישירות צריכה סיבה.

#### `unassignTag(input: TagAssignmentInput!) → MutationResult`

הסרת שיוך תג, גם ממונח (`objectType: "term"`, `termId`). הסרת `pii` מעמודה שנושאת אותו דרך המונח שלה נדחית (`tag.inherited_from_term`): בטלו את הסימון של המונח, או העבירו את העמודה למונח שמתאים למה שהיא מחזיקה.

#### `upsertTagParamValue(input: TagParamValueInput!) → MutationResult`

הוספה או תיאור מחדש של ערך פרמטר מותר עבור תג בעל פרמטרים. רשימת הערכים המותרים סגורה: כל שיוך חייב לנקוב בערך ממנה.

#### `deleteTagParamValue(tagId: String!, value: String!) → MutationResult`

הסרת ערך מותר. נדחה כל עוד שיוך כלשהו עדיין נושא אותו, מכיוון ששיוכים אלה ינקבו בטיפוס שהרשימה כבר אינה מכילה.

---

### מדדים {: #metrics_1 }

#### `createMetric(input: MetricInput!) → MutationResult`

יוצר הגדרת מדד מנוהל. שם שכבר קיים נדחה (`schema.metric_name_taken`). הביטוי חייב להיות ניתן לניתוח על ידי sqlglot ולהכיל לפחות פונקציית צבירה אחת. יוצר מחדש כל view המורכב ממדדים שמפנה למדד זה. דורש `table_registration`.

#### `updateMetric(input: MetricInput!) → MutationResult`

מחליף את ההגדרה של מדד קיים עם אותן בדיקות כמו `createMetric`. שם שאינו בבעלות אף מדד נדחה (`schema.metric_not_found`). השמירה צריכה את התחומים של הביטוי החדש ושל ההגדרה המוחלפת.

#### `deleteMetric(name: String!) → MutationResult`

מחיקת מדד מנוהל. בונה מחדש סכמות. דורש `table_registration`.

---

### לוחות שנה {: #calendars_1 }

#### `createCalendar(input: CalendarInput!) → MutationResult`

יצירה או החלפה של לוח שנה גרסתי של גבול תמונת מצב. מאומת על ידי בניית ה-`Calendar` בזיכרון לפני שמירה. הוא נכשל על מערכת בסיס לא ידועה, אזור זמן שגוי, או עוגן פיסקלי שגוי.

#### `deleteCalendar(name: String!) → MutationResult`

מחיקת לוח שנה (כל הגרסאות). נדחה כאשר MV כלשהו מפנה אליו.

---

### Materialized Views

#### `refreshMv(mvId: String!) → MutationResult`

הפעלת רענון ידני של Materialized View. מתאם על פני הצי כאשר מצב העקביות של ה-MV הוא `shared`.

#### `toggleMv(mvId: String!, enabled: Boolean!) → MutationResult`

הפעלה או השבתה של Materialized View.

---

### מטמון {: #cache_1 }

#### `purgeCache → MutationResult`

ניקוי כל תוצאות השאילתה השמורות במטמון.

#### `purgeCacheByTable(tableId: Int!) → MutationResult`

ניקוי תוצאות שמורות במטמון עבור טבלה אחת.

---

### משימות מתוזמנות {: #scheduled-tasks_1 }

#### `createScheduledTask(id, name, cron, kind, webhookName, argsJson, sql) → MutationResult`

יצירת טריגר מתוזמן — קריאת webhook או הצהרת SQL — ורישומו חי ב-APScheduler. `kind` הוא `"webhook"` או `"sql"`.

#### `deleteScheduledTask(taskId: String!) → MutationResult`

הסרת טריגר מתוזמן מקובץ ההגדרות ומהמתזמן החי.

#### `toggleScheduledTask(taskId: String!, enabled: Boolean!) → MutationResult`

הפעלה או השבתה של משימה מתוזמנת בקובץ ההגדרות.

---

### איכות נתונים {: #data-quality_1 }

#### `dryRunDqContract(sourceId: String!, contractText: String!) → DqDryRunType`

הרצת חוזה כנגד הטבלה החיה והחזרת תוצאות מבלי להנחית דבר. mutation ולא query מכיוון שהיא עולה סריקה אמיתית. מה שהיא מוכיחה הוא האם מזהה ערכת הנתונים נפתר לטבלה המנוהלת שהמפעיל מתכוון אליה.

#### `runDqCheckNow(schemaName: String!, tableName: String!) → MutationResult`

הפעלת עבודת הסקר (poll) של טבלת בודק מיידית. נוחתת שורות בדרך הרגילה, כך שתוצאות נשמרות והיסטוריית ה-DQ מציגה את הסריקה החדשה.

---

### תחזוקת סכמה {: #schema-maintenance }

#### `rebuildSchemas → MutationResult`

בניית הסכמה בזיכרון מחדש ממצב מסד הנתונים. שימושי לאחר שינויי מסד נתונים חיצוניים.

---

### בקשות יצירה {: #creation-requests_1 }

#### `approveCreationRequest(requestId: Int!) → MutationResult`

מאשר בקשת יצירה: אותו כלל, אותן דחיות ואותן רשומות ביומן הניהול כמו `POST /admin/creation-requests/{id}/approve`. מגיש הבקשה אינו יכול לאשר את הבקשה שלו, ואישור של משתמש אחד נספר פעם אחת. אישור שמשלים את הספירה של הבקשה יוצר את מה שהיא מבקשת; `params.status` הוא `executed` כשהוא עשה זאת, אחרת `pending`, ו-`params.waitingOn` מפרט את התחומים שבקשת קשר עדיין ממתינה להם.

#### `executeCreationRequest(requestId: Int!) → MutationResult`

מבצע בקשת יצירה שקיבלה את האישורים שלה — קשר, view, טבלה, מקור או webhook — דרך אותו קוד שהמוטציה הישירה משתמשת בו. נדחה עם `requests.approvals_incomplete` עד שהאישורים הנדרשים של הבקשה (אחד, או שניים לקשר) התקבלו ממשתמשים שאינם המגיש; בקשת קשר נדחית גם עם `requests.waiting_on_domains` עד שלכל תחום שהיא נוגעת בו יש מאשר. אישורים ניתנים דרך `POST /admin/creation-requests/{id}/approve`, והאישור שמשלים את הספירה מבצע את היצירה, ולכן מוטציה זו משמשת לניסיון חוזר של יצירה שנכשלה. יצירה שנכשלה משאירה את הבקשה ממתינה ועונה עם קוד השגיאה שלה.

#### `rejectCreationRequest(requestId: Int!, reason: String!) → MutationResult`

דוחה בקשה מהתור עם סיבה מעשית. `reason` נדרש. בקשת קשר יכולה להידחות על ידי כל משתמש שהיה יכול לאשר אותה. המגיש של בקשה מכל סוג אחר יכול לבטל אותה; הדבר נרשם ביומן הניהול כביטול.

---

### הידור שאילתה {: #query-compilation }

#### `compileQuery(input: CompileQueryInput!) → [CompileQueryResult!]!`

הידור שאילתת GraphQL של מישור-נתונים כנגד סכמה של תפקיד והחזרת החלטת הניתוב המלאה: SQL סמנטי, SQL מנוע, SQL ישיר, מסלול, מטא-דאטה של אכיפה (מסנני RLS שהוחלו, עמודות שהוחרגו, מיסוך שהוחל), ו-Cypher מהודר. מחזיר תוצאה אחת לכל שדה שורש בשאילתה.

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

## טיפוסי קלט מפתח {: #key-input-types }

### `SourceInput`

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
| `changeSignal` | `String` | `ttl` \| `probe` \| `ttl_probe` |
| `loadProtected` | `Boolean` | רענון מתוזמן בלבד |
| `offPeakWindow` | `String` | חלון תחזוקה `HH:MM-HH:MM` |
| `offPeakTz` | `String` | אזור זמן IANA |
| `cdc` | `SourceCdcConfigInput` | קונפיגורציית תעבורת Kafka CDC, כולל הדרך לפנות ל-schema registry שלה (ראו [אימות מול schema registry](subscriptions.md#schema-registry-authentication)). `schemaRegistryPassword` ו-`schemaRegistryToken` מוחזרים כהפניות לכספת, לעולם לא כאישורים. |

### `TableInput`

קלט רישום הטבלה המרכזי. שדות מפתח מעבר ליסודות:

| שדה | הערות |
|-------|------|
| `materialize` | הנחתת עותק בחנות המטריאליזציה |
| `mvRefreshInterval` | שניות בין רענונים |
| `mvPersist` | `replace` \| `append` \| `upsert` |
| `mvIncremental` | תחזוקה מצטברת |
| `businessKey` | עמודות שמזהות את אותו הדבר לאורך הגרסאות; מוצהר יחד עם `validTime` ו-`recordedTime`, שלושתם או אף אחד |
| `validTime` | עמודה שמחזיקה את התאריך שהעסק נותן לרשומה |
| `recordedTime` | עמודה שמחזיקה את המועד שבו גרסה זו הוזנה או שונתה |
| `mvCalendar` | שם לוח שנה של תמונת מצב |
| `mvGrain` | גרעיניות תמונת מצב: `daily`, `weekly`, `monthly`, `annual`, או מותאם `3WE` / `LFR` |
| `viewSql` | SQL עבור view נגזר |
| `viewMetrics` | מפרט view הצהרתי מורכב-מדד — בלעדי הדדית עם `viewSql` |
| `dqContract` | טקסט חוזה איכות-נתונים YAML/JSON |
| `queryTemplate` | Cypher עבור טבלת Neo4j |
| `live` | הגדרת אספקה חיה עבור דחיפת SSE/Kafka |
| `discover` | הסק עמודות מהמקור החי בעת הרישום |
| `gqlNamingConvention` | מוסכמת השמות של GraphQL לטבלה; `null` יורש. אם הושמטה, טבלה חדשה יורשת ועדכון שומר את השמורה |
| `roleTtl` | כל רשימת התפקיד ל-TTL, `[{ role, ttl }]` |
| `replicate` | מתי הטבלה מוגשת מהעותק שלה; `null` יורש את ערך המקור |
| `writeGrants` | כל קבוצת הרשאות הכתיבה של הטבלה; דורש `masking_config` |
| `draft` | `false` משחרר את הטבלה; אחרת רישום מתחיל כטיוטה |
| `cacheTtl` | ה-Cache TTL של הטבלה עצמה; `null` – אין לה משלה |
| `pagination` | העימוד המוצהר של הטבלה; `null` לא מצהיר על עימוד |
| `region` | איפה נמצאים נתוני הטבלה; בעדכון, אזור אחר מעביר אותה, ונבדק כמו ב-`setTableRegion` |
| `rowRules` | כללי השורות של הטבלה (`RLSRuleInput`), נשמרים איתה; דורש `masking_config` |

### `RelationshipInput`

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
| `viaTable` | שם טבלת צומת עבור קשתות many-to-many |
| `recordCandidate` | כתוב גם שורת relationship_candidates מסוג `accepted` |

---

## דוגמה: רישום טבלה {: #example-registering-a-table }

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

## דוגמה: יצירת כלל RLS {: #example-creating-an-rls-rule }

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
