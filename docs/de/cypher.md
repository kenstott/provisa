# Cypher-Abfrageunterstützung {: #cypher-query-support }

Provisa übersetzt eine Teilmenge von openCypher nach SQL über das Modul `provisa/cypher/`. Abfragen werden von einem eigenen rekursiven Abstiegs-Parser geparst (keine externe Cypher-Bibliothek), gegen das Modell schema-aufgelöst und als SQL emittiert, dann zur Ziel-Ausführungs-Engine geroutet.

## Implementierte Funktionen {: #implemented-features }

### Klauseln {: #clauses }

| Klausel | Status | Hinweise |
| -------- | -------- | ------- |
| `MATCH (n:Label)` | ✓ | Knotenmuster mit Labels, Variablen, Inline-Eigenschaften |
| `OPTIONAL MATCH` | ✓ | Emittiert LEFT JOIN |
| `WHERE` | ✓ | Vollständige Ausdrucksunterstützung; nach MATCH angewendet |
| `RETURN` | ✓ | Stern, Eigenschaftszugriff, Ausdrücke, Aliase |
| `RETURN DISTINCT` | ✓ | Emittiert SELECT DISTINCT |
| `WITH` | ✓ | Emittiert eine benannte CTE (`_w0`, `_w1`, …); unterstützt `WITH … WHERE` |
| `ORDER BY` | ✓ | ASC / DESC |
| `SKIP` / `LIMIT` | ✓ | Bildet auf SQL OFFSET / LIMIT ab |
| `UNION` / `UNION ALL` | ✓ | Rekursive Union über Sub-ASTs |
| `CALL { … }` | ✓ | Top-Level-Call-Subquery-Zerlegung über `cypher_calls_to_sql_list` |
| `CALL { WITH x … }` | ✓ | Korrelierte Subquery → `CROSS JOIN LATERAL`; siehe §Korrelierte CALL |
| `CALL db.labels()` | ✓ | Gibt Knoten-Labels aus dem Modell zurück; keine SQL-Übersetzung |
| `CALL db.relationshipTypes()` | ✓ | Gibt Beziehungstypen aus dem Modell zurück |
| `CALL db.propertyKeys()` | ✓ | Gibt alle Eigenschaftsschlüssel-Namen über alle Knotentypen hinweg zurück |
| `UNWIND` | ✓ | Array-zu-Zeilen-Expansion; erstes Element wird zu FROM, weitere zu CROSS JOIN UNNEST |

### Match-Muster {: #match-patterns }

| Muster | Status | Hinweise |
| --------- | -------- | ------- |
| `(n)` — unbeschrifteter Knoten | ✓ | UNION ALL über alle bekannten Typen |
| `(n:Label)` | ✓ | Bildet auf die registrierte Tabelle für diesen GraphQL-Typ ab |
| `(n:Label {prop: val})` | ✓ | Inline-Eigenschaftsfilter wird zu WHERE |
| `(a)-[:TYPE]->(b)` | ✓ | Gerichtet, ein Hop |
| `(a)<-[:TYPE]-(b)` | ✓ | Rückwärts-Traversierung; Join-Spalten vertauscht |
| `(a)-[]->(b)` | ✓ | Beliebige gerichtete Beziehung a→b; UNION ALL bei mehreren passenden Typen |
| `(a)-[]-(b)` | ✓ | Bidirektional; expandiert zu UNION ALL aller Vorwärts- und Rückwärtsbeziehungen |
| `(a)-[:TYPE*..N]->(b)` | ✓ | Variable Länge mit Obergrenze; rekursive CTE bei Selbstreferenz, sonst flacher JOIN |
| `(a)-[]->(b)-[]->(c)` | ✓ | Mehrstufige verkettete JOINs |
| `(n:DomainLabel)` | ✓ | Domänen-Label → UNION-ALL-Subquery über alle Typen in der Domäne |
| `(n:A\|B)` | ✓ | Label-Alternation → Ad-hoc-Domäne in die Label-Map injiziert; UNION ALL über passende Typen |
| `shortestPath(…)` | ✓ | Flacher JOIN bei heterogenen Endpunkten; WITH-RECURSIVE-CTE bei gleichem Typ/Selbstreferenz |
| `allShortestPaths(…)` | ✓ | Wie shortestPath, aber ohne LIMIT 1 |

### Ausdrücke und Prädikate {: #expressions-and-predicates }

| Funktion | Status | SQL-Abbildung |
| --------- | -------- | ------------ |
| Eigenschaftszugriff `n.prop` | ✓ | `n."prop"` |
| Parameter `$name` | ✓ | Positional `$N` |
| Legacy-Parameter `{name}` | ✓ | Zur Parse-Zeit auf `$name` normalisiert |
| Vergleich `=`, `<>`, `<`, `>`, `<=`, `>=` | ✓ | Direkt |
| `AND`, `OR`, `NOT` | ✓ | Direkt |
| `IS NULL` / `IS NOT NULL` | ✓ | Direkt |
| `IN [list]` | ✓ | SQL IN; Cypher-`[...]`-Klammersyntax zu `(...)` umgeschrieben |
| `STARTS WITH` | ✓ | `starts_with(col, val)` |
| `ENDS WITH` | ✓ | `col LIKE CONCAT('%', val)` |
| `CONTAINS` | ✓ | `strpos(col, val) > 0` |
| `=~` Regex | ✓ | `regexp_like(col, pattern)` |
| `exists(n.prop)` | ✓ | `(n.prop) IS NOT NULL` |
| `EXISTS { MATCH … }` | ✓ | Korrelierte `EXISTS (SELECT 1 FROM …)`-Subquery |
| `COUNT { MATCH … }` | ✓ | Korrelierte `(SELECT count(*) FROM …)`-Subquery |
| `COLLECT { MATCH … RETURN x }` | ✓ | Korrelierte `ARRAY(SELECT x FROM …)`-Subquery |
| `id(n)` | ✓ | Aufgelöst zur konfigurierten ID-Spalte des Knotens |
| `labels(n)` | ✓ | `ARRAY['Label']` |
| `keys(n)` | ✓ | `ARRAY['prop1', 'prop2', …]` |
| `type(r)` | ✓ | Zur Kompilierzeit zum String-Literal `'REL_TYPE'` aufgelöst; keine Laufzeitspalte |
| `length(p)` | ✓ | `_t.hops` bei rekursiven CTE-Pfaden; `1` bei flachen JOIN-Pfaden |
| `CASE WHEN … THEN … ELSE … END` | ✓ | Direkt (gesuchte und einfache Formen) |
| Implizites GROUP BY | ✓ | Nicht aggregierte RETURN-Elemente werden zu GROUP-BY-Schlüsseln, wenn ein Element ein Aggregat enthält |

### Map-Projektionen {: #map-projections }

| Syntax | SQL-Abbildung |
| -------- | ------------ |
| `n { .prop1, .prop2 }` | `MAP(ARRAY['prop1','prop2'], ARRAY[n."prop1",n."prop2"])` |
| `n { .* }` | `MAP(ARRAY[all props...], ARRAY[n."col",...])` — aus dem Schema expandiert |
| `n { .*, extra: expr }` | Alle Schema-Eigenschaften plus benannter Schlüssel; kombinierte MAP |
| `n { key: expr }` | `MAP(ARRAY['key'], ARRAY[expr])` |

**Objekteigenschaften.** Eine Spalte mit einem deklarierten Objekt (dem eingebetteten Objekt eines Remote-GraphQL-Typs oder jeder mit Objektfeldern registrierten Spalte) ist eine Map-Eigenschaft: `RETURN n.address` liefert sie als Map, in welcher Form die Engine sie auch zurückgibt, und der Knoten `n` trägt sie unter seinen Eigenschaften. Ein Remote-GraphQL-Feld, das auf einen registrierten Typ zeigt, ein Objekt oder eine Liste, ist keine Eigenschaft; es wird über seine Beziehung erreicht.

### Aggregationsfunktionen {: #aggregation-functions }

| Cypher | SQL |
| -------- | ----- |
| `count(*)`, `count(x)` | direkt |
| `count(DISTINCT x)` | `count(DISTINCT x)` |
| `collect(x)` | `array_agg(x)` |
| `avg`, `sum`, `min`, `max` | direkt |
| `stDev(x)` | `stddev_samp(x)` |
| `stDevP(x)` | `stddev_pop(x)` |
| `percentileCont(x, p)` | `approx_percentile(x, p)` |
| `percentileDisc(x, p)` | `approx_percentile(x, p)` |

### String-Funktionen {: #string-functions }

| Cypher | SQL |
| -------- | ----- |
| `toLower(x)` | `lower(x)` |
| `toUpper(x)` | `upper(x)` |
| `ltrim(x)`, `rtrim(x)`, `trim(x)` | direkt |
| `replace(x, a, b)` | direkt |
| `reverse(x)` | direkt |
| `split(x, d)` | direkt |
| `left(x, n)` | `left(x, n)` |
| `right(x, n)` | `right(x, n)` |
| `substring(x, start, len)` | `substr(x, start+1, len)` (0→1-Index) |
| `size(string)` | `char_length(string)` |
| `size(list)` | `cardinality(list)` |

### Typkonvertierungsfunktionen {: #type-conversion-functions }

| Cypher | SQL |
| -------- | ----- |
| `toString(x)` | `CAST(x AS VARCHAR)` |
| `toInteger(x)` | `TRY_CAST(x AS BIGINT)` |
| `toFloat(x)` | `TRY_CAST(x AS DOUBLE)` |
| `toBoolean(x)` | `TRY_CAST(x AS BOOLEAN)` |
| `toStringOrNull`, `toIntegerOrNull`, `toFloatOrNull`, `toBooleanOrNull` | `TRY_CAST`-Varianten |

### Mathematikfunktionen {: #math-functions }

| Cypher | SQL |
| -------- | ----- |
| `log(x)` | `ln(x)` (natürlicher Logarithmus) |
| `log2(x)` | `log2(x)` |
| `range(start, end)` | `sequence(start, end)` |
| `abs`, `sqrt`, `ceil`, `floor`, `round`, `sign` | unverändert durchgereicht |

### Listenfunktionen {: #list-functions }

| Cypher | SQL |
| -------- | ----- |
| `head(list)` | `element_at(list, 1)` |
| `last(list)` | `element_at(list, -1)` |
| `tail(list)` | `slice(list, 2, cardinality(list))` |
| `isEmpty(list)` | `cardinality(list) = 0` |

### Listen-Comprehensions {: #list-comprehensions }

| Syntax | SQL-Abbildung |
| -------- | ------------ |
| `[x IN list \| f(x)]` | `transform(list, x -> f(x))` |
| `[x IN list WHERE p(x)]` | `filter(list, x -> p(x))` |
| `[x IN list WHERE p(x) \| f(x)]` | `transform(filter(list, x -> p(x)), x -> f(x))` |
| `any(x IN list WHERE p(x))` | `any_match(list, x -> p(x))` |
| `all(x IN list WHERE p(x))` | `all_match(list, x -> p(x))` |
| `none(x IN list WHERE p(x))` | `none_match(list, x -> p(x))` |
| `single(x IN list WHERE p(x))` | `cardinality(filter(list, x -> p(x))) = 1` |
| `reduce(acc = init, x IN list \| expr)` | `reduce(list, init, (acc, x) -> expr, acc -> acc)` |

### Muster-Comprehensions {: #pattern-comprehensions }

| Syntax | SQL-Abbildung |
| -------- | ------------ |
| `[(a)-[:R]->(b) \| b.prop]` | `ARRAY(SELECT b."prop" FROM ... WHERE a.fk = b.pk)` |
| `[(a)-[]->(b:Label) \| b.prop]` | Typ aus dem Modell abgeleitet; gleiche ARRAY-Subquery-Form |

### Korrelierte CALL-Subqueries {: #correlated-call-subqueries }

`CALL { WITH x MATCH (x)-[:R]->(n) RETURN n.prop AS alias }` übersetzt sich zu `CROSS JOIN LATERAL (SELECT n."prop" AS alias FROM ... WHERE x."pk" = n."fk")`. Regeln:

- Die Variable aus dem äußeren Gültigkeitsbereich (`x`) muss in `WITH` erscheinen
- Mehrere importierte Variablen (`WITH a, b`) werden unterstützt
- Die erste Beziehung im inneren MATCH, deren Quelle eine lateral gebundene Variable ist, bestimmt das innere `FROM` und die Join-Bedingung
- Nicht korrelierte Top-Level-`CALL { ... }`-Blöcke (ohne `WITH`) werden von `cypher_calls_to_sql_list` behandelt

---

## Writes

Cypher unterstützt drei Schreibmuster über den `/data/cypher`-Endpunkt, ausgeführt von `provisa/cypher/write_translator.py`.

| Cypher | SQL |
|---|---|
| `CREATE (n:Label {props})` | `INSERT INTO catalog.schema.table (cols) VALUES (vals)` |
| `MATCH (n:Label) WHERE … DELETE n` | `DELETE FROM catalog.schema.table WHERE …` |
| `MATCH (n:Label) WHERE … SET n.prop = val, …` | `UPDATE catalog.schema.table SET col = val, … WHERE …` |

Eigenschaftsnamen werden über Domänenpräfix-Entfernung und Alias-Auflösung auf Spalten abgebildet; Cypher-Skalarwerte werden auf den Zielspaltentyp konvertiert. Der Antwortkörper enthält eine `affected_rows`-Zählung.

Die Eigenschaft eines Knotens ist der veröffentlichte Name seiner Spalte: der Alias der Spalte, wenn sie einen hat, sonst der Name nach der Namenskonvention – der Name, unter dem GraphQL und SQL sie lesen – in `MATCH`, `RETURN`, `CREATE`, `SET` und `MERGE` gleichermaßen.

Regeln:

- Das Label muss auf genau eine registrierte Tabelle aufgelöst werden. Mehrdeutige oder unbekannte Labels sind harte Fehler; kein Fuzzy-Matching. Neue Labels oder Typen können nicht über Cypher erstellt werden.
- Jeder Write ist an die Schreibberechtigungen der Zieltabelle gebunden: Die Rolle braucht das Recht `write` und eine Berechtigung für die Operation, und die geschriebenen Spalten müssen in der Berechtigung stehen. Eine Rolle ohne diese wird zur Kompilierzeit abgelehnt.
- Der zugrunde liegende Quellconnector muss DML unterstützen. Nur-Lese-Quellen (Trino-föderiert, Iceberg ohne Delta-Connector) lehnen Writes zur Übersetzungszeit ab.
- Beziehungen können nicht geschrieben werden: Sie werden aus den deklarierten Joins des Modells abgeleitet, nicht als gespeicherte Kanten. Eine Beziehung als Ziel zu adressieren ist ein harter Fehler. Eine Junction-gestützte Kante ist keine Ausnahme: Die dahinterliegende Zuordnungstabelle ist selbst eine registrierte Tabelle, und Zeilen werden in diese Tabelle geschrieben, nicht in die Kante.
- Writes durchlaufen die vollständige Write-Pipeline: RLS-Injektion und Post-Mutation-Hooks (Response-Cache-Invalidierung, Markierung materialisierter Sichten als veraltet, Kafka-Change-Events, Hot-Table-Reload).
- `MERGE`, `DETACH DELETE` und `REMOVE` werden nicht unterstützt und zur Parse-Zeit abgelehnt.

---

## Protokollzugriff {: #protocol-access }

Cypher erreicht dieselbe geregelte Pipeline über zwei Transporte:

- **HTTP** — `POST /data/cypher` mit einem JSON-Body (`{"query": "...", "params": {...}}`). Gibt typisierte Zeilen zurück, oder `affected_rows` bei Writes. Graph-Variablen in der `RETURN`-Klausel serialisieren als JSON: Knoten tragen `id`, `label`, `tableLabel` und `properties`; Kanten tragen `identity`, `start`, `end`, `type`, `properties`, `startNode` und `endNode`; Pfade tragen `nodes`, `edges` und `length`/`hops`. Registrierte Commands sind hier ebenfalls aufrufbar über `CALL fn(args) YIELD col1, col2`, und positionale Argumente werden der Reihe nach auf die deklarierten Argumentnamen des Commands abgebildet.
- **Bolt** — ein Neo4j-kompatibler Binärprotokoll-Server (PackStream-Codec, Chunked Framing), der es Neo4j Browser, Bloom und Bolt-Treibern ermöglicht, Cypher über den föderierten Graphen auszuführen. Er startet, wenn `PROVISA_BOLT_PORT` auf einen von null verschiedenen Wert gesetzt ist, und ist standardmäßig deaktiviert; setzen Sie `PROVISA_BOLT_CERT` / `PROVISA_BOLT_KEY` für TLS. Bolt-Auth bildet Principal auf Benutzer und Datenbank auf Rolle ab: `SHOW DATABASES` listet einen Eintrag pro (Sicht × Rolle)-Paar auf, benannt `provisa_<role>` (Geschäftsdomänen) oder `provisa_ops_<role>` (mit System-/Meta-/Ops-Domänen); `:use` wählt die aktive Rolle und Sicht aus. Beziehungen erhalten dauerhafte Integer-IDs über eine `rel_ids`-Tabelle, die das `node_ids`-Design widerspiegelt. Registrierte Commands sind mit `CALL command(args)` aufrufbar, und positionale Argumente werden der Reihe nach auf deklarierte Argumentnamen abgebildet; `CALL dbms.*` / `CALL db.*`-Prozeduren haben Vorrang.

### Graph-Analytics

`POST /data/graph-analytics` führt eine Cypher-Abfrage aus, baut einen In-Memory-NetworkX-Graphen aus den resultierenden Knoten und Kanten, führt einen benannten Algorithmus aus und fügt jedem Knoten und jeder Kante ein `_analytics`-Dict hinzu, bevor sie als JSON mit einem `elapsed_ms`-Feld zurückgegeben werden. Die `_analytics`-Schlüssel variieren je nach Algorithmus: Zentralität liefert `score`; Community-Erkennung liefert `cluster`; K-Core liefert `core_number`; Grad-Zentralität fügt `in_degree` und `out_degree` hinzu. Der Endpunkt lehnt Graphen oberhalb einer konfigurierbaren Größe (Standard 10.000 Knoten / 50.000 Kanten) mit HTTP 413 ab; Girvan-Newman ist auf 500 Knoten begrenzt, sofern der Aufrufer nicht `force=true` übergibt.

---

## Einschränkungen {: #limitations }

### Designbeschränkungen {: #design-constraints }

1. **Writes sind auf `CREATE`, `SET` und `DELETE` beschränkt.** Diese werden als direkte Tabellen-Writes über dieselbe Pipeline wie GraphQL- und SQL-Mutationen ausgeführt. Siehe §Writes oben. `MERGE`, `DETACH DELETE` und `REMOVE` werden zur Parse-Zeit abgelehnt. APOC-Prozeduren werden ebenfalls abgelehnt.

2. **Beziehungseigenschaften gibt es nur auf Junction-gestützten Kanten.** Eine über ein Fremdschlüssel-Spaltenpaar deklarierte Kante existiert nur als Join-Metadaten im Modell und speichert keine Attribute, also bedeutet `WHERE r.since > 2020` oder `RETURN r.weight` auf ihr nichts. Eine über eine Verknüpfungstabelle deklarierte Kante trägt sie dagegen. Die übrigen Spalten der Assoziationstabelle sind die Eigenschaften der Beziehung, und `RETURN r` gibt sie zurück. Ein `WHERE` auf einer davon wird zu einem Prädikat auf dem Junction-Alias kompiliert und schränkt damit die Traversierung selbst ein, statt Zeilen nach dem Zusammenbau zu filtern. Die Verknüpfungstabelle fällt aus der Knotenseite des Graphschemas: Hier ist sie eine Kante, überall sonst eine Tabelle.

3. **Bidirektionale Traversierung** `(a)-[]-(b)` wird zum UNION ALL aus Vorwärts- und Rückwärtsrichtung jeder passenden gerichteten Beziehung im Modell umgeschrieben. Jede Beziehung im Modell hat eine Richtung, und die bidirektionale Syntax ist Zucker, der zu beiden aufgelöst wird. Die zusätzlichen Zweige entstehen auf der äußersten Abfrageebene. Ein späteres MATCH-Muster derselben Abfrage wird nicht über die Zweige dupliziert, was bidirektionale Muster in einer Abfrage mit mehreren MATCH-Klauseln einschränkt.

4. **Rekursive Pfade erfordern eine Obergrenze.** Muster variabler Länge (`[*]`) müssen eine Obergrenze enthalten (z. B. `[*..10]`). Unbegrenzte Traversierung wird zur Parse-Zeit abgelehnt, um ausufernde rekursive CTEs zu verhindern.

### Verhaltenshinweise {: #behaviour-notes }

5. **`shortestPath` bei nicht selbstreferentiellen Pfaden verwendet einen flachen JOIN ohne Hops-Sortierung.** Unterscheiden sich Start- und Endtyp und hat das Schema keine selbstreferentielle Beziehung, erzeugt der Übersetzer eine flache JOIN-Kette entlang des kürzesten Schemapfads. Dieser Codepfad verfolgt keine Hops, also erzeugt er kein `ORDER BY hops`. Das Ergebnis ist der strukturell kürzeste Schemapfad. Er kann vom kürzesten Pfad durch die Daten über mehrere Zeilen abweichen.

6. **Mehrere Schemapfade erzeugen `UNION ALL`.** Wenn zwei Schemapfade gleicher Hop-Anzahl denselben Start- und Endtyp verbinden (z. B. `Person -[WORKS_AT]-> Company` und `Person -[MANAGES]-> Company`), werden beide als `UNION ALL`-Zweige emittiert. Eine Deduplizierung von Zeilen, die in beiden Zweigen erscheinen, erfolgt nicht.

7. **Ein `RelationshipMapping` pro Quelle→Ziel-Paar und rel_type-Kombination.** Wenn zwei GraphQL-Felder auf demselben Quelltyp denselben `rel_type`-String (nach Großschreibung) zum selben Zieltyp erzeugen, überschreibt die zweite Registrierung die erste in `CypherLabelMap.relationships`. Der Beziehungsschlüssel enthält Quell- und Zieltyp-Namen, daher erhalten unterschiedliche Quelle/Ziel-Paare mit demselben Typnamen jeweils einen eigenen Eintrag und sind nicht betroffen.

8. **`WITH`-Klausel-CTEs werden `_w0`, `_w1`, … benannt.** Namen werden positional innerhalb eines einzelnen Übersetzungsaufrufs vergeben. Das Zusammensetzen mehrerer übersetzter Abfragen (z. B. in einem Batch) kann bei naiver Verkettung zu kollidierenden CTE-Namen führen.

### Ausdrucks- und Musterabdeckung {: #expression-and-pattern-coverage }

Cypher-Ausdrücke werden in einen AST geparst und Knoten für Knoten zu SQL abgesenkt (`provisa/cypher/expr_parser.py`, `provisa/cypher/expr_visitor.py`). Die Grammatik folgt dem Präzedenzturm `oC_Expression` von openCypher. Unterstützt: Literale, Parameter, Eigenschaftszugriff, `n.prop`, Index und Slice, Arithmetik (`+ - * / % ^`), Vergleich, `IN`, `STARTS WITH` / `ENDS WITH` / `CONTAINS` / `=~`, `IS [NOT] NULL`, boolesches `AND` / `OR` / `XOR` / `NOT`, `CASE`, Listen- und Map-Literale, Listen- und Muster-Comprehensions (einschließlich der `p = (…)`-Pfadbindung), Map-Projektion, `reduce`, die Quantoren `all` / `any` / `none` / `single`, existenzielle Subqueries und Funktionsaufrufe.

9. **Labels sind fest; Sie können keine Objekttypen über Cypher erstellen.** Ein Label wird zu einer bekannten Domäne, einem bekannten Objekttyp oder einem qualifizierten `domain:object_type` aufgelöst, der geschlossenen Menge, die das registrierte Schema definiert. Cypher führt nie ein neues Label oder einen neuen Typ ein. Instanzen können Sie nur von Typen anlegen, die in einer beschreibbaren Datenquelle bereits definiert sind: `CREATE` schreibt Zeilen in eine solche Tabelle (siehe §Writes) und kann kein neues Label und keinen neuen Typ definieren. Beide Label-Formen werden akzeptiert und bedeuten denselben Test, das Postfix `n:Label` und das ausführliche `n IS :Label` (mit der Verneinung `n IS NOT :Label`). Ein qualifiziertes Label schreibt man `n:domain:object_type`.

10. **`shortestPath` und `allShortestPaths` funktionieren nur innerhalb von `MATCH`.** In einem Muster (`MATCH p = shortestPath((a:Person)-[:KNOWS*..5]->(b:Person))`) werden sie zu einer `WITH RECURSIVE`-CTE übersetzt und brauchen gelabelte Start- und Zielknoten. In Ausdrucksposition werden sie nicht unterstützt, zum Beispiel `RETURN shortestPath((a)-[*]->(b))` oder `WHERE length(shortestPath((a)-[*]->(b))) < 5`. Die rekursive Umschreibung geht von der `MATCH`-Klausel aus, und es gibt keine korrelierte Unterabfrage, die sie tragen könnte.

11. **Listen-Comprehensions, `REDUCE` und Quantoren arbeiten mit Listenwerten; Muster-Comprehensions traversieren.** `reduce(...)`, `all/any/none/single(...)` und die Listen-Comprehension `[x IN list | …]` arbeiten über einem Listenausdruck und werden auf die Listenfunktionen höherer Ordnung der Engine abgesenkt. Selbst durchlaufen sie den Graphen nicht. Die **Muster**-Comprehension `[(a)-[:R]->(b) WHERE p | e]` traversiert dagegen: Ihr Graphmuster wird als korrelierte Unterabfrage angesprochen, also ist ihre Quelle eine Traversierung. Um Traversierungsergebnisse in die Listenformen zu speisen, nutzen Sie `nodes(p)` / `relationships(p)` / `collect(...)` oder direkt eine Muster-Comprehension.
