# רפליקות {: #replicas }

רפליקה היא עותק של טבלת מקור ש-Provisa מחזיקה במאגר המימוש של המנוע וקוראת במקום המקור. עמוד זה מכסה על מה רפליקה מאונדקסת, היכן לשמור רפליקות וכמה הן עדכניות. אם טבלה משוכפלת בכלל נקבע על ידי [`replicate`](configuration.md#the-replicate-setting) ועל ידי מה שהמנוע יכול להגיע אליו; ראו [What an engine changes](engines.md#what-an-engine-changes). (REQ-826)

## רפליקות מאונדקסות {: #replicas-are-indexed }

Provisa מאנדקסת רפליקה כפי שהמודל משתמש בטבלה שלה. (REQ-2015) [tool-verified: `provisa/federation/replica_indexes.py` `plan`, `join_columns`]

- **המפתח הראשי המוצהר**, נאכף כמפתח.
- **העמודות שהקשרים המוצהרים מצטרפים עליהן**:
  - המפתח הזר של כל קשר שהמקור שלו הוא טבלה זו;
  - עמודת היעד של כל קשר שהיעד שלו הוא טבלה זו, אלא אם עמודה זו כבר המפתח;
  - שני המפתחות הזרים של טבלת צומת (junction) שקשר עובר דרכה.

קחו את הקשר הזה:

```yaml
relationships:
  - id: orders-to-customers
    source_table_id: orders
    target_table_id: customers
    source_column: customer_id
    target_column: id
    cardinality: many-to-one
```

הרפליקה של `orders` מקבלת את המפתח שלה ואינדקס על `customer_id`. הרפליקה של `customers` מקבלת את המפתח שלה, וכש-`id` הוא אותו מפתח היא אינה צריכה דבר נוסף. צירוף של השתיים קורא אז אינדקס משני הצדדים, במאגר שמשתמש באינדקסים לצירופים (ראו [What each store builds](#what-each-store-builds)). קשר על עמודה מחושבת שהרפליקה אינה מחזיקה אינו מקבל אינדקס. [tool-verified: `provisa/federation/replica_indexes.py` `plan`]

הצהירו על המפתח עם `is_primary_key: true` על העמודה. רפליקה ללא מפתח מוצהר מקבלת רק את אינדקסי הצירוף שלה. [tool-verified: `provisa/federation/replica_builds.py` `plan_indexes`]

### קריאה קודם, אינדקס אחר כך {: #readable-first-indexed-after }

טעינה לעולם אינה ממתינה לאינדקסים. הרפליקה נקראת מהרגע שהיא מוחלפת פנימה; לאחר מכן רץ הרפליקות (replica runner) בונה את האינדקסים שלה בזה אחר זה, במעבר נפרד, תחת מנעול אינדקס לכל רפליקה. [tool-verified: `provisa/federation/replica_runner.py` `_planned`, `_start_index_builds`] צפו שהשאילתות הראשונות אחרי טעינה ירוצו בלעדיהם.

כל טעינה רושמת את האינדקסים שביקשה, את אלה שנבנו ואת אלה שנכשלו, בשמם, ברשומת הרפליקה במישור הבקרה (`replica_state.index_state`). [tool-verified: `provisa/federation/replica_state.py` `record_index_plan`, `settle_index_state`]

- **אינדקס שנכשל לעולם אינו מכשיל את הטעינה.** הוא נרשם ביומן עם סיבתו ומבוקש שוב בטעינה הבאה. שורת היומן היא `replica of <source>.<schema>.<table>: index <name> on (<columns>) failed: <cause>`. [tool-verified: `provisa/federation/replica_indexes.py` `build_due`]
- **שורות החוזרות על מפתח מוצהר הן כשל בפני עצמן**, בקוד `replica.index_duplicate_key`, המציין את עמודות המפתח. המקור מחזיק כפילויות של קבוצת עמודות שהמודל מצהיר עליה כייחודית. ניסיון חוזר אינו מתקן זאת: תקנו את הנתונים במקור, או תקנו את המפתח המוצהר. [tool-verified: `provisa/federation/replica_indexes.py` `DuplicateKey`, `DUPLICATE_KEY`]
- **טעינה שמחליפה את הטבלה באמצע בנייה** משאירה מאחור את הבניות הקודמות; הן נעצרות ואינן רושמות דבר, והטבלה החדשה מקבלת בניות משלה. במאגר PostgreSQL ההחלפה מבטלת בנייה שעדיין רצה על הטבלה שהיא מחליפה. [tool-verified: `provisa/federation/replica_target.py` `_pg_swap`; `provisa/federation/replica_indexes.py` `build_due`]
- **התוכנית עוקבת אחר המודל בכל טעינה.** קשר שנוסף מאוחר יותר מקבל את האינדקס שלו בטעינה הבאה של הרפליקה. [tool-verified: `provisa/federation/replica_builds.py` `plan_indexes`]

ממשק הניהול אינו מציג את רשומת האינדקסים. קראו את יומן השרת, או את העמודה `index_state` בשורת הרפליקה. [tool-verified: no reader of `index_state` outside `provisa/federation/replica_state.py` and `replica_indexes.py`]

### מה כל מאגר בונה {: #what-each-store-builds }

| מאגר | מה נבנה | כיצד |
| --- | --- | --- |
| PostgreSQL: מסד הנתונים של מנוע `pg` עצמו, מאגר ה-PostgreSQL של Trino או DuckDB, מנוע `sqlalchemy` על PostgreSQL | אינדקסי מפתח וצירוף | `CREATE INDEX CONCURRENTLY` בחיבור משלו, כך שקוראים וכותבים אינם נחסמים. המפתח נבנה כאינדקס ייחודי, ואז הופך למפתח הראשי עם `ADD CONSTRAINT ... PRIMARY KEY USING INDEX`. אינדקס פגום שבנייה שבוטלה השאירה נמחק ונבנה מחדש. |
| מסדי נתונים אחרים מאחורי מנוע רלציוני (`mysql`, `oracle`, `mssql` ושאר) | אינדקסי מפתח וצירוף | ה-DDL של הדיאלקט עצמו. מסד נתונים שדוחה את ה-DDL רושם את האינדקס ככושל. |
| קובץ DuckDB מובנה, MotherDuck | אינדקסי מפתח וצירוף | דרך מתווך המאגר (store broker): `ALTER TABLE ... ADD PRIMARY KEY`, `CREATE INDEX IF NOT EXISTS`. |
| ClickHouse | מפתח המיון בלבד | רפליקה ב-ClickHouse מסודרת לפי המפתח המוצהר שלה (`ORDER BY`), הנקבע כשהיא נטענת. ל-ClickHouse אין אינדקס B-tree משני לבנות. |
| Snowflake, BigQuery, Databricks, Fabric, Synapse | דבר | המחסן מארגן את הטבלה בעצמו. |

[tool-verified: `provisa/federation/replica_target.py` `PostgresIndexer`, `SqlAlchemyIndexer`, `DuckDBStoreIndexer`; `provisa/federation/materialize_broker.py` `_SyncedStore.replica_index`; `provisa/federation/clickhouse_store.py` `create_ddl`; `replica_indexer` in `backend.py`, `pg_backend.py`, `duckdb_backend.py`, `sqlalchemy_backend.py`, `clickhouse_backend.py`, `snowflake_backend.py`, `bigquery_backend.py`, `databricks_backend.py`, `mssql_warehouse_backend.py`]

אינדקס מועיל רק לצירוף שרץ היכן שהאינדקס נמצא. שני מנועים קוראים את המאגר שלהם בלי להשתמש באינדקסים שלו לצירופים:

- **DuckDB עם הקובץ המובנה שלו.** המנוע לעולם אינו מחבר (attach) את הקובץ. כל הצהרה קוראת עותק מקומי משלה של כל טבלת מאגר שהיא מציינת, הנעשה מחדש בכל פעם שהמאגר השתנה. צירופים רצים כצירופי hash על העותק הזה, ולכן אינדקסי צירוף אינם מאיצים אותם; אינדקסי הקובץ אוכפים את המפתח ומשרתים כל מה שקורא את הקובץ במקומו. [tool-verified: `provisa/federation/duckdb_runtime.py` `_store_is_file`, `_copy_store_table`; `provisa/federation/replica_target.py` `DuckDBStoreIndexer`]
- **DuckDB עם מאגר PostgreSQL.** DuckDB קורא את המאגר דרך הרחבת `postgres` שלו כשדחיפת המסננים (filter pushdown) כבויה (באג במעלה הזרם duckdb/duckdb-postgres#564), ולכן הוא סורק את הרפליקה ומסנן ומצרף ב-DuckDB. אינדקסי המאגר נבנים אך הקריאות האלה אינן משתמשות בהם. [tool-verified: `provisa/federation/duckdb_extensions.py` `_EXTENSION_SETTINGS`]

מאגר MotherDuck מחובר (attach) ונקרא במקומו. [tool-verified: `provisa/federation/duckdb_runtime.py` `_store_is_file`]

## השתמשו במסד נתונים רלציוני כמאגר הרפליקות כשהמנוע מריץ בו SQL {: #use-a-relational-database-as-the-replica-store-where-the-engine-runs-sql-in-it }

כשהמנוע מריץ את הצירופים שלו בתוך מסד נתונים רלציוני, שמרו שם את הרפליקות. הצירופים קוראים אז את האינדקסים שלמעלה. שלוש תצורות עושות זאת:

- **מנוע `pg`.** רפליקות נוחתות במסד ה-PostgreSQL של המנוע עצמו וכל הצהרה רצה שם, דרך האינדקסים שלהן. [tool-verified: `provisa/federation/pg_backend.py` `replica_target`, `replica_indexer`]
- **מנוע רלציוני** (`mysql`, `oracle`, `mssql`, ה-`sqlalchemy` הכללי ושאר). כל מקור נוחת לתוך מסד הנתונים הזה ונשאל שם. [tool-verified: `provisa/federation/sqlalchemy_backend.py` `replica_indexer`; [Engine reference](configuration.md#engine-reference)]
- **Trino עם מאגר ה-PostgreSQL שלו.** PostgreSQL הוא המאגר היחיד ש-Trino תומך בו; Trino קורא רפליקות דרך מחבר ה-PostgreSQL שלו. [tool-verified: `provisa/federation/engine.py` `build_trino_engine` `supported_materialize_stores`; `provisa/federation/backend.py` `TrinoBackend.materialize_store_target`] מסנן או צירוף ש-Trino דוחף ל-PostgreSQL משתמש באינדקסים; עבודה ש-Trino עושה בעצמו אינה משתמשת. [inferred]

כוונו מנוע למאגר עם `PROVISA_MATERIALIZE_URL` או `materialize_store_url`; מאגר שארגון רשם בעצמו קודם לשניהם. [tool-verified: `provisa/federation/engine.py` `materialize_store`, `configured_materialize_url`]

```bash
PROVISA_MATERIALIZE_URL="postgresql://provisa:secret@replicas.internal:5432/replicas"
```

זו המלצה כללית, לא כלל. אם היא משתלמת תלוי במנוע, במקורות, במאגר ובגדלי הטבלאות. שקלו את אלה:

- **טווח הגעה לכל שאילתה.** מאגר שהמנוע מגיע אליו דרך הרשת מוסיף הלוך-חזור והעברה לכל קריאה. קובץ ה-DuckDB המובנה מקומי ועמודתי, וסורק מהר. [inferred]
- **עלות טעינה.** כתיבת רפליקה למסד נתונים רלציוני ואז בניית האינדקסים שלה עולה יותר מכתיבת קובץ מקומי. [inferred]
- **העומס של מסד הנתונים עצמו.** קריאות רפליקה, טעינות ובניית אינדקסים רצות כולן על אותו מסד נתונים. קבעו את גודלו בהתאם. [inferred]
- **סריקות גדולות.** אינדקס אינו עושה דבר לשאילתה שקוראת את רוב הטבלה הגדולה. מאגר עמודתי סורק אותה מהר יותר ממאגר שורות. [inferred]

## בחירת מקום מגורי הרפליקות {: #choosing-where-replicas-live }

המאגר הוא מנוף אחד מכמה, והם חופפים. הכריעו קודם את האחרים; המאגר מכריע רק במה שהם משאירים.

| מנוף | מה הוא עושה | היכן הוא עוזר |
| --- | --- | --- |
| טבלאות חמות (Hot tables) | טבלת חיפוש קטנה מוחזקת ב-Redis ומוכנסת לכל שאילתה שקוראת אותה כ-CTE (ביטוי טבלה משותף) מוטבע מסוג `VALUES`. [tool-verified: `provisa/compiler/sql_gen.py`, `provisa/cache/hot_tables.py`] | צירופים לטבלאות חיפוש קטנות, בכל מאגר: השורות נוסעות בתוך השאילתה. היעד של קשר רבים-לאחד, או טבלה עם `hot_tables.auto_threshold` שורות או פחות (1,000 כברירת מחדל), הופכים לחמים בקריאה הראשונה. `hot: true` מכריח טבלה פנימה ו-`hot: false` מרחיק אותה. [tool-verified: `provisa/cache/hot_tables.py` `promote_on_read` and the loader that registers candidates; `provisa/core/models.py` `HotTablesConfig`, `Table.hot`] |
| תצוגות ממומשות שאתם מגדירים | תצוגה עם `materialize: true` נשמרת ומתרעננת. שאילתה שקוראת את התצוגה נענית מהשורות השמורות כשהן עדכניות והקורא רואה אותה במלואה: אף קלט אינו נושא כלל שורות צר מכל השורות. [tool-verified: `provisa/mv/view_read.py` module docstring] | אגרגציות על טבלאות גדולות. אתם מחליטים אילו קיימות; אף אחת אינה נוצרת עבורכם. ראו [Views](configuration.md#views-governed-computed-datasets). |
| קשר עם `materialize: true` | קשר בין מקורות מקבל תצוגת הורה-ילד שמורה וממוזגת מראש. בקשה שהצירוף שלה תואם את התבנית, במלואה או בחלקה, נכתבת מחדש כך שתקרא אותה, בין שהיא מגיעה כ-SQL ובין שהמהדר בונה אותה. [tool-verified: `provisa/api/app_loaders.py` `_load_mv_and_views_config`; `provisa/mv/view_reinject.py` `reinject`; `provisa/pgwire/_pipeline.py` `_view_answering`] | צירופים במאגר שאינו בונה אינדקסי צירוף. ראו [Auto-Materialization](configuration.md#auto-materialization). |
| מיקום (Placement) | מקורות שהמנוע יכול לקרוא חי נקראים חי; השאר משוכפלים. מה שמגיע למאגר נובע מהמנוע שבחרתם. [tool-verified: `provisa/federation/replica_routing.py`] | בחירת המנוע קובעת את רוב מה שהמאגר צריך להחזיק. |

מימוש קשרים ואינדקסי צירוף עונים על אותו צורך בשתי דרכים. במאגר עמודתי או מחסן (קובץ DuckDB, ClickHouse, Snowflake, BigQuery, Databricks, Fabric, Synapse) לא נבנה אינדקס צירוף, ולכן תצוגה ממוזגת מראש היא הדרך להוזיל צירוף תדיר. במאגר רלציוני עמודות הצירוף כבר מאונדקסות, ואותם צירופים יכולים לרוץ מהר גם בלי לממש אותם. [inferred]

מה שעדיין תלוי במאגר: סריקות וצירופים אד-הוק על טבלאות גדולות שאף תצוגה ממומשת אינה מכסה ואף טבלה חמה אינה משרתת. אלה רצים על אינדקסי המאגר במסד נתונים רלציוני, או על מהירות הסריקה שלו במאגר עמודתי.

התוצאות משתנות לפי עומס העבודה. מדדו לפני שינוי ואחריו עם [כלי קביעת הגודל](sizing.md); [ריצות החי-או-רפליקה](sizing.md#live-or-replica-reads-and-the-ttl) שלו נותנות את עלות קריאת רפליקה מול קריאה חיה בפריסה שלכם.

## עדכניות {: #freshness }

הפכו את העדכניות לחלק מההחלטה, לצד המהירות. כמה ישנה יכולה להיות תשובה, ומה עולה לשמור אותה עדכנית למקור, קובעים אם טבלה נקראת חי או מרפליקה, ובכך מה המאגר צריך להחזיק.

מקורות ארגוניים נדיר שתומכים בזמן אמת. נתונים מגיעים בטעינת אצווה או ב-ETL לילי, הזנות שינויים מתעכבות, ומערכות רבות אינן יכולות לקבל קריאות חיות בנפח שאילתות. התחילו מהעדכניות שכל טבלה צריכה, ואז בחרו את האסטרטגיה הזולה ביותר שעומדת בה. הכללים בצד המפעיל נמצאים ב-[Freshness and load: who decides](configuration.md#freshness-and-load-who-decides).

| אסטרטגיה | מוגדרת עם | מה היא מבטיחה | מה היא עולה למקור |
| --- | --- | --- | --- |
| קריאה חיה | `replicate: -1` | עדכנית נכון לשאילתה | כל שאילתה קוראת את המקור |
| TTL | `change_signal: ttl`, `cache_ttl` | רפליקה שאינה ישנה מ-`cache_ttl` | בנייה מחדש אחת לכל `cache_ttl`; עם `watermark_column`, רק שורות מעבר ל-watermark |
| בדיקת שינוי | `change_signal: probe` או `ttl_probe`, `cache_ttl` | נבנית מחדש רק אחרי שהבדיקה מדווחת על שינוי; הבדיקה רצה כל `cache_ttl` | בדיקה אחת לכל מרווח, ובנייה מחדש רק בשינוי |
| הזנת שינויים בדחיפה | מקור Kafka או WebSocket; `change_signal: debezium` במקור עם בלוק `cdc`; `change_signal: native` ב-PostgreSQL‏, MongoDB או Cosmos DB | עדכנית עד כדי השהיית ההזנה וה-debounce | Kafka‏, WebSocket ו-Debezium: דבר לאחר הבנייה הראשונה, ההזנה נושאת את השורות. PostgreSQL‏, MongoDB ו-Cosmos DB: בנייה מחדש לכל התפרצות שינויים |
| אות חיצוני | `change_signal: signal` | נבנה מחדש בכל פעם ש-`POST /data/sources/{source_id}/signal` מודיע שהמקור השתנה; לעולם לא לפי שעון | בנייה מחדש אחת לכל אות |
| TTL לפי תפקיד | `role_ttl` על טבלה | תפקיד ברשימה מקבל רפליקה עד ה-TTL שלו; `cache_ttl` של הטבלה נשאר הרצפה | פחות בניות מחדש: קריאה של תפקיד סובלני אינה מבקשת אחת |
| הגנת עומס | `load_protected: true`, עם `cache_ttl`, בדיקה או `off_peak_window` | שאילתות לעולם אינן קוראות את המקור; הרצן לבדו מרענן את הרפליקה, ועם חלון שעות שפל רק כשהוא פתוח | רק הרענונים המתוזמנים |
| שער עדכניות | `freshness_gate: true` על מקור | כל קריאה נבדקת מול `change_signal` ו-`cache_ttl` של המקור קודם; פסק דין "מיושן" מרענן לפני הקריאה | רענון בנתיב הקריאה כשמיושן |
| אינו פג לעולם | `cache_ttl: -1` על מקור או טבלה | נטען פעם אחת; כל קריאה נענית מהרפליקה, וקריאה של טבלה שעוד לא נטענה נדחית בציון שמה | רק הטעינה האחת: מקור שכל הטבלאות שלו נטענו אינו מחובר, נבדק או נקרא שוב, ואינו צריך פרטי גישה |

[tool-verified: `provisa/core/replicate.py`; `provisa/federation/replica_builds.py` `_next_refresh_at`, `_probe_decides`; `provisa/federation/query_residency.py` `freshness_verdict`, `is_stale_of`; `provisa/federation/role_ttl.py` `effective_ttl`; `provisa/freshness/source_gate.py` `gate_source`; `provisa/events/push_wiring.py` `wire_push_listeners`, `_run_change_stream`]

בדיקה שאין לה מה להשוות (אין watermark, sentinel או שאילתת בדיקה) מתנוונת ל-TTL. [tool-verified: `provisa/federation/replica_builds.py` `_probe_decides`]

### כמעט זמן אמת מהזנת שינויים {: #near-real-time-from-a-change-feed }

כשמקור אינו יכול לקבל עומס שאילתות חי אך כבר מפרסם את שינוייו, הזנת שינויים נותנת נתונים כמעט בזמן אמת בלי לגעת במקור בזמן השאילתה. שאילתות קוראות את הרפליקה המקומית, שמהירה ואינה מעמיסה על המקור, בעוד לולאת האירועים מיישמת שינויים עם הגעתם. הנתונים מפגרים רק בהשהיית ההזנה, בדרך כלל שניות. [inferred: the lag figure]

עצם קיומה של ההזנה אינו מספיק. Provisa עוקבת אחריה רק עבור טבלה שמחוברת אליה:

- **מקור Kafka.** הטבלה מציינת את הנושא (topic) שלה, וכל שינוי מיושם על הרפליקה לפי המפתח הראשי, כולל מחיקות. [tool-verified: `provisa/events/push_wiring.py` `_build_provider`, `_run_listener`; `provisa/subscriptions/cdc_landing.py`]

    ```yaml
  live:
    strategy: kafka
    kafka:
      topic: orders.cdc
  push_debounce_quiet: 0      # seconds of quiet before a batch lands; 0 lands each message
  push_debounce_max_delay: 5  # a batch never waits longer than this
  ```

- **מקור WebSocket.** הזרם שלו מיושם באותו אופן. [tool-verified: `provisa/events/push_wiring.py` `_build_provider`]
- **טבלה עם `change_signal: debezium`** (בטבלה או במקור שלה) במקור שבלוק ה-`cdc` שלו מציין את תעבורת Kafka ואת קידומת ה-topic של המחבר. הקריאה הראשונה בונה את הרפליקה; לאחר מכן כל אירוע שינוי ב-topic `{topic_prefix}.{schema}.{table}` מיושם לפי מפתח ראשי, כולל מחיקות, וה-offset נרשם רק לאחר שהשינוי נטען. [tool-verified: `provisa/events/push_wiring.py` `_captured_by_cdc`, `_cdc_provider`; `provisa/subscriptions/debezium_provider.py` `ack`]
- **טבלה של מקור כזה עם `change_signal: kafka`**. השינויים שלה נקראים מ-topic הדלתא ש-`live.kafka.topic` שלה מציין, באותה תעבורה, ומיושמים באותו אופן; טבלה עם אות זה שאינה מציינת topic נדחית בשמה בשמירת הטבלה או המקור שלה ובטעינת התצורה. [tool-verified: `provisa/events/push_wiring.py` `_cdc_provider`; `provisa/core/change_signal.py` `kafka_topic_refusal`]
- **טבלת MongoDB או Cosmos DB עם `change_signal: native`** (על הטבלה או על המקור שלה). כל התפרצות שינויים בזרם השינויים מבקשת בנייה מחדש של הרפליקה. [tool-verified: `provisa/events/push_wiring.py` `_change_feed_opted_in`, `_run_change_stream`]
- **טבלת PostgreSQL עם `change_signal: native`** (בטבלה או במקור שלה). Provisa קוראת את השכפול הלוגי של מסד הנתונים מ-slot משלה, `provisa_<source id>`, דרך פרסום באותו שם המונה את הטבלאות המקוריות של המקור. Kafka אינה מעורבת. כל התפרצות שינויים בטבלה מבקשת בנייה מחדש של הרפליקה שלה. מסד הנתונים צריך את מה ש-Debezium צריך: `wal_level` שמוגדר ל-`logical`, תפקיד עם הרשאת שכפול שרשאי ליצור את הפרסום, ושרת ראשי ולא רפליקת קריאה; מקור שחסר לו אחד מאלה נדחה בשמו בשמירה ובטעינת התצורה. ה-slot והפרסום נוצרים כשהטבלה הראשונה עוקבת אחריהם ונמחקים כשהאחרונה מפסיקה או כשהמקור נמחק. בעת ההפעלה שרת בודק כל מקור PostgreSQL רשום ומוחק slot של Provisa שאף טבלה מקורית אינה עוקבת אחריו, כך ש-slot שנותר משינוי תצורה שנעשה כשאף שרת לא רץ אינו ממשיך להחזיק את ה-WAL של המקור. [tool-verified: `provisa/subscriptions/pg_logical.py`; `provisa/events/push_wiring.py` `_run_logical_stream`, `_drop_unfollowed_slots`]

טבלה בדחיפה צריכה מפתח ראשי מוצהר; טבלה בלעדיו מדולגת ונרשמת ביומן. [tool-verified: `provisa/events/push_wiring.py` `wire_push_listeners`]

טבלה עם אות שינוי בדחיפה נחשבת עדכנית בנתיב הקריאה, כי ההזנה שלה היא בדיקת העדכניות שלה. [tool-verified: `provisa/federation/query_residency.py` `freshness_verdict`] לכן אות דחיפה ששום דבר לא יקבל נדחה: `change_signal: native` בכל סוג שאינו Kafka‏, WebSocket‏, ingest‏, PostgreSQL‏, MongoDB או Cosmos DB נדחה בשמירת המקור או הטבלה ובטעינת התצורה, וכך גם `debezium` במקור ללא בלוק `cdc`. [tool-verified: `provisa/core/change_signal.py` `native_signal_refusal`; `provisa/core/config_loader.py` `_validate_change_signal`; `provisa/api/admin/_change_feed.py` `table_change_feed_refusal`]
