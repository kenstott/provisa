# תמיכה בשאילתות Cypher

Provisa מתרגמת תת-קבוצה של openCypher ל-SQL דרך המודול `provisa/cypher/`. (REQ-345, REQ-347) שאילתות מפוענחות על ידי parser‏ recursive-descent מותאם אישית (ללא ספריית Cypher חיצונית) (REQ-571), נפתרות-סכמה (schema-resolved) מול השכבה הסמנטית (REQ-351), ונפלטות כ-SQL, ואז מנותבות למנוע הביצוע היעד. (REQ-066, REQ-067, REQ-347)

## תכונות ממומשות

### Clauses

| Clause | סטטוס | הערות |
| -------- | -------- | ------- |
| `MATCH (n:Label)` | ✓ | דפוסי node עם תוויות, משתנים, מאפיינים inline |
| `OPTIONAL MATCH` | ✓ | פולט LEFT JOIN |
| `WHERE` | ✓ | תמיכה מלאה בביטויים; מוחל אחרי MATCH |
| `RETURN` | ✓ | Star, גישת מאפיין, ביטויים, aliases |
| `RETURN DISTINCT` | ✓ | פולט SELECT DISTINCT |
| `WITH` | ✓ | פולט CTE בעל-שם (`_w0`, `_w1`, …); תומך ב-`WITH … WHERE` |
| `ORDER BY` | ✓ | ASC / DESC |
| `SKIP` / `LIMIT` | ✓ | ממופה ל-SQL OFFSET / LIMIT |
| `UNION` / `UNION ALL` | ✓ | union רקורסיבי על פני תתי-AST |
| `CALL { … }` | ✓ | פירוק תת-שאילתת call ברמה-עליונה דרך `cypher_calls_to_sql_list` |
| `CALL { WITH x … }` | ✓ | תת-שאילתה מתואמת (correlated) → `CROSS JOIN LATERAL`; ראו §CALL מתואם |
| `CALL db.labels()` | ✓ | מחזיר תוויות node מהשכבה הסמנטית; ללא תרגום SQL (REQ-572) |
| `CALL db.relationshipTypes()` | ✓ | מחזיר סוגי קשר מהשכבה הסמנטית (REQ-572) |
| `CALL db.propertyKeys()` | ✓ | מחזיר את כל שמות מפתחות המאפיינים על פני כל סוגי ה-node (REQ-572) |
| `UNWIND` | ✓ | הרחבת array-לשורות; הפריט הראשון הופך ל-FROM, הבאים הופכים ל-CROSS JOIN UNNEST |

### דפוסי Match

| דפוס | סטטוס | הערות |
| --------- | -------- | ------- |
| `(n)` — node ללא תווית | ✓ | UNION ALL על פני כל הטיפוסים הידועים |
| `(n:Label)` | ✓ | ממופה לטבלה הרשומה עבור אותו טיפוס GraphQL |
| `(n:Label {prop: val})` | ✓ | פילטר מאפיין inline הופך ל-WHERE |
| `(a)-[:TYPE]->(b)` | ✓ | מכוון, hop יחיד |
| `(a)<-[:TYPE]-(b)` | ✓ | מעבר אחורה; עמודות join הפוכות |
| `(a)-[]->(b)` | ✓ | כל קשר מכוון a→b; UNION ALL אם מספר טיפוסים תואמים |
| `(a)-[]-(b)` | ✓ | דו-כיווני; מתרחב ל-UNION ALL של כל הקשרים הקדימה והאחורה |
| `(a)-[:TYPE*..N]->(b)` | ✓ | אורך-משתנה עם גבול עליון; CTE רקורסיבי עבור self-referential, JOIN שטוח אחרת |
| `(a)-[]->(b)-[]->(c)` | ✓ | JOIN-ים מרובי-hop משורשרים |
| `(n:DomainLabel)` | ✓ | תווית דומיין → תת-שאילתת UNION ALL על פני כל הטיפוסים בדומיין |
| `(n:A\|B)` | ✓ | חלופת תווית → דומיין אד-הוק מוזרק למפת התוויות; UNION ALL על פני טיפוסים תואמים |
| `shortestPath(…)` | ✓ | JOIN שטוח עבור endpoints הטרוגניים; CTE‏ WITH RECURSIVE עבור אותו-טיפוס/self-referential |
| `allShortestPaths(…)` | ✓ | כמו shortestPath ללא LIMIT 1 |

### ביטויים ופרדיקטים

| תכונה | סטטוס | מיפוי SQL |
| --------- | -------- | ------------ |
| גישת מאפיין `n.prop` | ✓ | `n."prop"` |
| פרמטרים `$name` | ✓ | פוזיציוני `$N` |
| פרמטרים ישנים `{name}` | ✓ | מנורמל ל-`$name` בזמן פענוח |
| השוואה `=`, `<>`, `<`, `>`, `<=`, `>=` | ✓ | ישיר |
| `AND`, `OR`, `NOT` | ✓ | ישיר |
| `IS NULL` / `IS NOT NULL` | ✓ | ישיר |
| `IN [list]` | ✓ | SQL IN; תחביר סוגריים `[...]` של Cypher נכתב מחדש ל-`(...)` |
| `STARTS WITH` | ✓ | `starts_with(col, val)` |
| `ENDS WITH` | ✓ | `col LIKE CONCAT('%', val)` |
| `CONTAINS` | ✓ | `strpos(col, val) > 0` |
| `=~` regex | ✓ | `regexp_like(col, pattern)` |
| `exists(n.prop)` | ✓ | `(n.prop) IS NOT NULL` |
| `EXISTS { MATCH … }` | ✓ | תת-שאילתת `EXISTS (SELECT 1 FROM …)` מתואמת |
| `COUNT { MATCH … }` | ✓ | תת-שאילתת `(SELECT count(*) FROM …)` מתואמת |
| `COLLECT { MATCH … RETURN x }` | ✓ | תת-שאילתת `ARRAY(SELECT x FROM …)` מתואמת |
| `id(n)` | ✓ | נפתר לעמודת ה-ID המוגדרת של ה-node |
| `labels(n)` | ✓ | `ARRAY['Label']` |
| `keys(n)` | ✓ | `ARRAY['prop1', 'prop2', …]` |
| `type(r)` | ✓ | נפתר בזמן קומפילציה למחרוזת-ליטרל `'REL_TYPE'`; ללא עמודת runtime |
| `length(p)` | ✓ | `_t.hops` עבור נתיבי CTE רקורסיביים; `1` עבור נתיבי JOIN שטוחים |
| `CASE WHEN … THEN … ELSE … END` | ✓ | ישיר (צורות searched ו-simple) |
| GROUP BY משתמע | ✓ | פריטי RETURN לא-מצוברים הופכים למפתחות GROUP BY כאשר לפריט כלשהו יש אגרגט |

### הקרנות מפה (Map Projections)

| תחביר | מיפוי SQL |
| -------- | ------------ |
| `n { .prop1, .prop2 }` | `MAP(ARRAY['prop1','prop2'], ARRAY[n."prop1",n."prop2"])` |
| `n { .* }` | `MAP(ARRAY[all props...], ARRAY[n."col",...])` — מורחב מהסכמה |
| `n { .*, extra: expr }` | כל מאפייני הסכמה בתוספת מפתח בעל-שם; MAP משולב |
| `n { key: expr }` | `MAP(ARRAY['key'], ARRAY[expr])` |

### פונקציות אגרגציה

| Cypher | SQL |
| -------- | ----- |
| `count(*)`, `count(x)` | ישיר |
| `count(DISTINCT x)` | `count(DISTINCT x)` |
| `collect(x)` | `array_agg(x)` |
| `avg`, `sum`, `min`, `max` | ישיר |
| `stDev(x)` | `stddev_samp(x)` |
| `stDevP(x)` | `stddev_pop(x)` |
| `percentileCont(x, p)` | `approx_percentile(x, p)` |
| `percentileDisc(x, p)` | `approx_percentile(x, p)` |

### פונקציות מחרוזת

| Cypher | SQL |
| -------- | ----- |
| `toLower(x)` | `lower(x)` |
| `toUpper(x)` | `upper(x)` |
| `ltrim(x)`, `rtrim(x)`, `trim(x)` | ישיר |
| `replace(x, a, b)` | ישיר |
| `reverse(x)` | ישיר |
| `split(x, d)` | ישיר |
| `left(x, n)` | `left(x, n)` |
| `right(x, n)` | `right(x, n)` |
| `substring(x, start, len)` | `substr(x, start+1, len)` (אינדקס 0→1) |
| `size(string)` | `char_length(string)` |
| `size(list)` | `cardinality(list)` |

### פונקציות המרת טיפוס

| Cypher | SQL |
| -------- | ----- |
| `toString(x)` | `CAST(x AS VARCHAR)` |
| `toInteger(x)` | `TRY_CAST(x AS BIGINT)` |
| `toFloat(x)` | `TRY_CAST(x AS DOUBLE)` |
| `toBoolean(x)` | `TRY_CAST(x AS BOOLEAN)` |
| `toStringOrNull`, `toIntegerOrNull`, `toFloatOrNull`, `toBooleanOrNull` | וריאנטים של `TRY_CAST` |

### פונקציות מתמטיות

| Cypher | SQL |
| -------- | ----- |
| `log(x)` | `ln(x)` (לוג טבעי) |
| `log2(x)` | `log2(x)` |
| `range(start, end)` | `sequence(start, end)` |
| `abs`, `sqrt`, `ceil`, `floor`, `round`, `sign` | מועברים כמו-שהם |

### פונקציות רשימה

| Cypher | SQL |
| -------- | ----- |
| `head(list)` | `element_at(list, 1)` |
| `last(list)` | `element_at(list, -1)` |
| `tail(list)` | `slice(list, 2, cardinality(list))` |
| `isEmpty(list)` | `cardinality(list) = 0` |

### List Comprehensions

| תחביר | מיפוי SQL |
| -------- | ------------ |
| `[x IN list \| f(x)]` | `transform(list, x -> f(x))` |
| `[x IN list WHERE p(x)]` | `filter(list, x -> p(x))` |
| `[x IN list WHERE p(x) \| f(x)]` | `transform(filter(list, x -> p(x)), x -> f(x))` |
| `any(x IN list WHERE p(x))` | `any_match(list, x -> p(x))` |
| `all(x IN list WHERE p(x))` | `all_match(list, x -> p(x))` |
| `none(x IN list WHERE p(x))` | `none_match(list, x -> p(x))` |
| `single(x IN list WHERE p(x))` | `cardinality(filter(list, x -> p(x))) = 1` |
| `reduce(acc = init, x IN list \| expr)` | `reduce(list, init, (acc, x) -> expr, acc -> acc)` |

### Pattern Comprehensions

| תחביר | מיפוי SQL |
| -------- | ------------ |
| `[(a)-[:R]->(b) \| b.prop]` | `ARRAY(SELECT b."prop" FROM ... WHERE a.fk = b.pk)` |
| `[(a)-[]->(b:Label) \| b.prop]` | טיפוס-מוסק מהשכבה הסמנטית; אותה צורת תת-שאילתת ARRAY |

### תתי-שאילתות CALL מתואמות (Correlated)

`CALL { WITH x MATCH (x)-[:R]->(n) RETURN n.prop AS alias }` מתורגם ל-`CROSS JOIN LATERAL (SELECT n."prop" AS alias FROM ... WHERE x."pk" = n."fk")`. (REQ-573) כללים:

- המשתנה מהיקף-חיצוני (`x`) חייב להופיע ב-`WITH`
- משתנים מיובאים מרובים (`WITH a, b`) נתמכים
- הקשר הראשון ב-MATCH הפנימי שמקורו הוא משתנה lateral-bound קובע את ה-`FROM` הפנימי ותנאי ה-join
- בלוקי `CALL { ... }` ברמה-עליונה שאינם-מתואמים (ללא `WITH`) מטופלים על ידי `cypher_calls_to_sql_list`

---

## כתיבות (Writes)

Cypher תומכת בשלושה דפוסי כתיבה דרך נקודת הקצה `/data/cypher`, המבוצעים על ידי `provisa/cypher/write_translator.py`. (REQ-818) [tool-verified: `provisa/api/rest/cypher_router.py:415-545`]

| Cypher | SQL | Req |
| -------- | ----- | ----- |
| `CREATE (n:Label {props})` | `INSERT INTO catalog.schema.table (cols) VALUES (vals)` | REQ-666 |
| `MATCH (n:Label) WHERE … DELETE n` | `DELETE FROM catalog.schema.table WHERE …` | REQ-667 |
| `MATCH (n:Label) WHERE … SET n.prop = val, …` | `UPDATE catalog.schema.table SET col = val, … WHERE …` | REQ-668 |

שמות מאפיינים ממופים לעמודות דרך הסרת קידומת-דומיין ופתרון alias; ערכים סקלריים של Cypher נכפים (coerced) לטיפוס העמודה היעד. (REQ-666, REQ-668) גוף התגובה נושא ספירת `affected_rows`. (REQ-670)

כללים:

- התווית חייבת להיפתר בדיוק לטבלה רשומה אחת. תוויות דו-משמעיות או לא-ידועות הן שגיאות קשות; ללא התאמה מטושטשת (fuzzy). (REQ-661) תוויות או טיפוסים חדשים לא ניתנים ליצירה דרך Cypher. (REQ-662)
- כל כתיבה נשערת (gated) על ACL‏ `writable_by` של הטבלה היעד; תפקיד ללא זכויות כתיבה נדחה בזמן קומפילציה. (REQ-663)
- מחבר המקור התומך חייב לתמוך ב-DML. מקורות read-only (פדרטיביים-Trino, Iceberg ללא מחבר Delta) דוחים כתיבות בזמן תרגום. (REQ-664)
- קשרים אינם ניתנים לכתיבה — הם נגזרים מ-joins של מפתח-זר, לא קצוות מאוחסנים. כוונון (targeting) קשר הוא שגיאה קשה. (REQ-665)
- כתיבות רצות דרך צינור הכתיבה המלא: הזרקת RLS ו-hooks לאחר-מוטציה (ביטול תוקף מטמון-תגובה, סימון-מיושן של materialized-view, אירועי שינוי Kafka, טעינה מחדש של טבלה חמה). (REQ-798)
- `MERGE`, `DETACH DELETE`, ו-`REMOVE` אינם נתמכים ונדחים בזמן פענוח. (REQ-671)

---

## גישת פרוטוקול

Cypher מגיעה לאותו צינור מנוהל על פני שתי תעבורות:

- **HTTP** — `POST /data/cypher` עם גוף JSON (`{"query": "...", "params": {...}}`). מחזיר שורות מוקלדות, או `affected_rows` עבור כתיבות. משתני גרף בסעיף `RETURN` מסודרים כ-JSON: nodes נושאים `id`, `label`, `tableLabel`, ו-`properties`; edges נושאים `identity`, `start`, `end`, `type`, `properties`, `startNode`, ו-`endNode`; paths נושאים `nodes`, `edges`, ו-`length`/`hops`. (REQ-750) commands רשומים ניתנים לקריאה גם כאן דרך `CALL fn(args) YIELD col1, col2` — ארגומנטים פוזיציוניים ממופים לשמות הארגומנטים המוצהרים של ה-command לפי סדר. (REQ-1156) [tool-verified: `provisa/api/rest/registered_call.py:113-143`]
- **Bolt** — שרת פרוטוקול בינארי תואם-Neo4j (קודק PackStream, framing מפוצל-chunk) המאפשר ל-Neo4j Browser, Bloom, ולדרייברי Bolt להריץ Cypher על פני הגרף הפדרטיבי. (REQ-802) הוא מתחיל כאשר `PROVISA_BOLT_PORT` מוגדר לערך שאינו-אפס וכבוי כברירת מחדל; הגדירו `PROVISA_BOLT_CERT` / `PROVISA_BOLT_KEY` עבור TLS. [tool-verified: `provisa/api/app_startup.py:317-338`] אימות Bolt ממפה principal למשתמש ומסד-נתונים לתפקיד: `SHOW DATABASES` מציג רשומה אחת לכל זוג (תצוגה × תפקיד), בשם `provisa_<role>` (דומייני עסקים) או `provisa_ops_<role>` (עם דומייני system/meta/ops); `:use` בוחר את התפקיד והתצוגה הפעילים. (REQ-807) קשרים מקבלים מזהי integer מתמידים דרך טבלת `rel_ids`, המשקפת את עיצוב `node_ids`. (REQ-806) commands רשומים ניתנים לקריאה עם `CALL command(args)` — ארגומנטים פוזיציוניים ממופים לשמות הארגומנטים המוצהרים לפי סדר; פרוצדורות `CALL dbms.*` / `CALL db.*` מקבלות עדיפות. (REQ-1156) [tool-verified: `provisa/bolt/session.py:722-749`]

### אנליטיקת גרף

`POST /data/graph-analytics` מריצה שאילתת Cypher, בונה גרף NetworkX בזיכרון מה-nodes וה-edges המתקבלים, מריצה אלגוריתם בעל-שם, וממזגת מילון `_analytics` לתוך כל node ו-edge לפני החזרתם כ-JSON עם שדה `elapsed_ms`. (REQ-642) מפתחות ה-`_analytics` משתנים לפי אלגוריתם: centrality מניב `score`; זיהוי קהילה מניב `cluster`; k-core מניב `core_number`; degree centrality מוסיף `in_degree` ו-`out_degree`. (REQ-643) נקודת הקצה דוחה גרפים מעל גודל הניתן-לתצורה (ברירת מחדל 10,000 nodes / 50,000 edges) עם HTTP 413; Girvan-Newman מוגבל ל-500 nodes אלא אם הקורא מעביר `force=true`. (REQ-650, REQ-651)

---

## מגבלות

### אילוצי עיצוב

1. **כתיבות מוגבלות ל-`CREATE`, `SET`, ו-`DELETE`.** אלה מבוצעות ככתיבות טבלה ישירות דרך אותו צינור כמו מוטציות GraphQL ו-SQL. (REQ-818, REQ-666, REQ-667, REQ-668) ראו §כתיבות למעלה. `MERGE`, `DETACH DELETE`, ו-`REMOVE` נדחים בזמן פענוח. (REQ-671, REQ-818) פרוצדורות APOC נדחות אף הן.

2. **ללא מאפייני קשר.** קשרים (`-[r:TYPE]->`) קיימים אך ורק כמטא-דאטת join בשכבה הסמנטית. (REQ-574) הם אינם נושאים תכונות מאוחסנות, כך של-`WHERE r.since > 2020` או `RETURN r.weight` אין משמעות ואינם נתמכים.

3. **מעבר דו-כיווני** `(a)-[]-(b)` נכתב מחדש ל-UNION ALL קדימה+אחורה של כל הקשרים המכוונים התואמים מהשכבה הסמנטית. (REQ-575) כל קשר בשכבה הסמנטית הוא כיווני; תחביר דו-כיווני הוא סוכר תחבירי המתרחב לשני הכיוונים. ענפים נוספים נפלטים ברמת השאילתה החיצונית-ביותר — דפוסי MATCH נוספים באותה שאילתה אינם משוכפלים על פני ענפים (מגבלה עבור דו-כיווני מרובה-MATCH).

4. **נתיבים רקורסיביים דורשים גבול.** דפוסי אורך-משתנה (`[*]`) חייבים לכלול גבול עליון (למשל `[*..10]`). (REQ-348) מעבר לא-חסום נדחה בזמן פענוח כדי למנוע CTE-ים רקורסיביים חסרי-שליטה.

### הערות התנהגות

5. **`shortestPath` על נתיבים שאינם-self-referential משתמש ב-JOIN שטוח, לא בסידור hops.** כאשר טיפוסי ההתחלה והסיום שונים ולא קיים קשר self-referential בסכמה, המתרגם פולט שרשרת JOIN שטוחה (נתיב הסכמה הקצר-ביותר). (REQ-576) הוא אינו פולט `ORDER BY hops` כי hops אינם נעקבים באותו נתיב קוד. התוצאה היא נתיב הסכמה הקצר-ביותר מבחינה מבנית, לא הנתיב הקצר-ביותר-בנתונים על פני מספר שורות.

6. **נתיבי סכמה מרובים מייצרים `UNION ALL`.** כאשר שני נתיבי סכמה בעלי אותה ספירת-hop מחברים את אותם טיפוסי התחלה וסיום (למשל `Person -[WORKS_AT]-> Company` ו-`Person -[MANAGES]-> Company`), שניהם נפלטים כענפי `UNION ALL`. (REQ-577) הסרת כפילויות של שורות המופיעות בשני הענפים אינה מבוצעת.

7. **`RelationshipMapping` אחד לכל צירוף מקור→יעד וצירוף rel_type.** אם שני שדות GraphQL על אותו טיפוס מקור מייצרים את אותה מחרוזת `rel_type` (לאחר uppercasing) לאותו טיפוס יעד, הרישום השני דורס את הראשון ב-`CypherLabelMap.relationships`. מפתח הקשר כולל שמות טיפוס מקור ויעד, כך שצירופי מקור/יעד שונים עם אותו שם טיפוס מקבלים כל אחד רשומה משלו ואינם מושפעים.

8. **CTE-ים בסעיף `WITH` נקראים `_w0`, `_w1`, …** (REQ-578) שמות מוקצים פוזיציונית בתוך קריאת תרגום יחידה. הרכבת מספר שאילתות מתורגמות (למשל ב-batch) יכולה לייצר שמות CTE מתנגשים אם הן משורשרות באופן נאיבי.

### כיסוי ביטויים ודפוסים (REQ-913)

ביטויי Cypher מפוענחים ל-AST ומורדים node-ל-node ל-SQL (`provisa/cypher/expr_parser.py`, `provisa/cypher/expr_visitor.py`). הדקדוק עוקב אחר מגדל העדיפויות `oC_Expression` של openCypher. נתמך: ליטרלים, פרמטרים, גישת מאפיין, `n.prop`, אינדקס ו-slice, אריתמטיקה (`+ - * / % ^`), השוואה, `IN`, `STARTS WITH` / `ENDS WITH` / `CONTAINS` / `=~`, `IS [NOT] NULL`, בוליאני `AND` / `OR` / `XOR` / `NOT`, `CASE`, ליטרלי רשימה ומפה, comprehensions של רשימה ודפוס (כולל קשירת נתיב `p = (…)`), הקרנת מפה, `reduce`, המכמתים (quantifiers) `all` / `any` / `none` / `single`, תתי-שאילתות אקזיסטנציאליות, וקריאות פונקציה.

9. **תוויות קבועות; אינכם יכולים ליצור טיפוסי אובייקט דרך Cypher.** תווית נפתרת לדומיין ידוע, טיפוס אובייקט ידוע, או `domain:object_type` מוסמך — הסט הסגור המוגדר על ידי הסכמה הרשומה. Cypher לעולם אינה מציגה תווית או טיפוס חדשים. יצירת מופע אפשרית רק עבור טיפוסים המוגדרים כבר בתוך מקור נתונים ניתן-לכתיבה; `CREATE` כותב שורות לתוך טבלה כזו (ראו §כתיבות) אך אינו יכול להגדיר תווית או טיפוס חדשים. (REQ-662) שתי צורות התווית מתקבלות ופירושן אותו מבחן: הפוסטפיקס `n:Label` והצורה המפורטת `n IS :Label` (ושלילתן `n IS NOT :Label`). תווית מוסמכת נכתבת `n:domain:object_type`.

10. **`shortestPath` ו-`allShortestPaths` נתמכים רק בתוך `MATCH`, לא כביטויים.** בדפוס (`MATCH p = shortestPath((a:Person)-[:KNOWS*..5]->(b:Person))`) הם מתורגמים ל-CTE `WITH RECURSIVE` ודורשים nodes מקור ויעד מתויגים. בשימוש בעמדת ביטוי — למשל `RETURN shortestPath((a)-[*]->(b))` או `WHERE length(shortestPath((a)-[*]->(b))) < 5` — הם אינם נתמכים, כי הכתיבה-מחדש הרקורסיבית מונעת מסעיף ה-`MATCH` ולא מתת-שאילתה מתואמת.

11. **List comprehensions, `REDUCE`, ומכמתים פועלים על ערכי רשימה; pattern comprehensions חוצות (traverse).** `reduce(...)`, `all/any/none/single(...)`, ו-list comprehension `[x IN list | …]` פועלים על ביטוי רשימה ויורדים לפונקציות הרשימה מסדר-גבוה של המנוע — הם עצמם אינם הולכים על הגרף. ה-**pattern** comprehension `[(a)-[:R]->(b) WHERE p | e]` כן חוצה: דפוס הגרף שלו מטופל כתת-שאילתה מתואמת, כך שזהו comprehension שמקורו הוא מעבר (traversal). הזינו תוצאות מעבר לתוך צורות הרשימה עם `nodes(p)` / `relationships(p)` / `collect(...)`, או השתמשו ב-pattern comprehension ישירות.
