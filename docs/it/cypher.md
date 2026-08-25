# Supporto query Cypher

Provisa traduce un sottoinsieme di openCypher in SQL tramite il modulo `provisa/cypher/`. (REQ-345, REQ-347) Le query vengono analizzate da un parser custom recursive-descent (nessuna libreria Cypher esterna) (REQ-571), risolte contro il layer semantico (REQ-351), ed emesse come SQL, poi instradate al motore di esecuzione target. (REQ-066, REQ-067, REQ-347)

## Funzionalità implementate

### Clausole

| Clausola | Stato | Note |
| -------- | -------- | ------- |
| `MATCH (n:Label)` | ✓ | Pattern nodo con etichette, variabili, proprietà inline |
| `OPTIONAL MATCH` | ✓ | Emette LEFT JOIN |
| `WHERE` | ✓ | Supporto completo di espressioni; applicato dopo MATCH |
| `RETURN` | ✓ | Star, accesso proprietà, espressioni, alias |
| `RETURN DISTINCT` | ✓ | Emette SELECT DISTINCT |
| `WITH` | ✓ | Emette una CTE nominata (`_w0`, `_w1`, …); supporta `WITH … WHERE` |
| `ORDER BY` | ✓ | ASC / DESC |
| `SKIP` / `LIMIT` | ✓ | Mappa a SQL OFFSET / LIMIT |
| `UNION` / `UNION ALL` | ✓ | Union ricorsiva tra sotto-AST |
| `CALL { … }` | ✓ | Decomposizione della subquery call top-level via `cypher_calls_to_sql_list` |
| `CALL { WITH x … }` | ✓ | Subquery correlata → `CROSS JOIN LATERAL`; vedi §CALL correlata |
| `CALL db.labels()` | ✓ | Restituisce le etichette dei nodi dal layer semantico; nessuna traduzione SQL (REQ-572) |
| `CALL db.relationshipTypes()` | ✓ | Restituisce i tipi di relazione dal layer semantico (REQ-572) |
| `CALL db.propertyKeys()` | ✓ | Restituisce tutti i nomi delle chiavi di proprietà su tutti i tipi nodo (REQ-572) |
| `UNWIND` | ✓ | Espansione array-a-righe; il primo elemento diventa FROM, i successivi diventano CROSS JOIN UNNEST |

### Pattern Match

| Pattern | Stato | Note |
| --------- | -------- | ------- |
| `(n)` — nodo senza etichetta | ✓ | UNION ALL su tutti i tipi conosciuti |
| `(n:Label)` | ✓ | Mappa alla tabella registrata per quel tipo GraphQL |
| `(n:Label {prop: val})` | ✓ | Filtro proprietà inline diventa WHERE |
| `(a)-[:TYPE]->(b)` | ✓ | Diretto, singolo hop |
| `(a)<-[:TYPE]-(b)` | ✓ | Attraversamento all'indietro; colonne di join invertite |
| `(a)-[]->(b)` | ✓ | Qualsiasi relazione diretta a→b; UNION ALL se più tipi corrispondono |
| `(a)-[]-(b)` | ✓ | Bidirezionale; si espande a UNION ALL di tutte le relazioni forward e backward |
| `(a)-[:TYPE*..N]->(b)` | ✓ | Lunghezza variabile con limite superiore; CTE ricorsiva per self-referenziale, JOIN piatto altrimenti |
| `(a)-[]->(b)-[]->(c)` | ✓ | JOIN concatenati multi-hop |
| `(n:DomainLabel)` | ✓ | Etichetta di dominio → subquery UNION ALL su tutti i tipi nel dominio |
| `(n:A\|B)` | ✓ | Alternanza di etichette → dominio ad-hoc iniettato nella mappa etichette; UNION ALL sui tipi corrispondenti |
| `shortestPath(…)` | ✓ | JOIN piatto per endpoint eterogenei; CTE WITH RECURSIVE per stesso-tipo/self-referenziale |
| `allShortestPaths(…)` | ✓ | Come shortestPath senza LIMIT 1 |

### Espressioni e predicati

| Funzionalità | Stato | Mapping SQL |
| --------- | -------- | ------------ |
| Accesso proprietà `n.prop` | ✓ | `n."prop"` |
| Parametri `$name` | ✓ | Posizionale `$N` |
| Parametri legacy `{name}` | ✓ | Normalizzato a `$name` al momento del parsing |
| Confronto `=`, `<>`, `<`, `>`, `<=`, `>=` | ✓ | Diretto |
| `AND`, `OR`, `NOT` | ✓ | Diretto |
| `IS NULL` / `IS NOT NULL` | ✓ | Diretto |
| `IN [list]` | ✓ | SQL IN; sintassi a parentesi quadre `[...]` di Cypher riscritta come `(...)` |
| `STARTS WITH` | ✓ | `starts_with(col, val)` |
| `ENDS WITH` | ✓ | `col LIKE CONCAT('%', val)` |
| `CONTAINS` | ✓ | `strpos(col, val) > 0` |
| `=~` regex | ✓ | `regexp_like(col, pattern)` |
| `exists(n.prop)` | ✓ | `(n.prop) IS NOT NULL` |
| `EXISTS { MATCH … }` | ✓ | Subquery correlata `EXISTS (SELECT 1 FROM …)` |
| `COUNT { MATCH … }` | ✓ | Subquery correlata `(SELECT count(*) FROM …)` |
| `COLLECT { MATCH … RETURN x }` | ✓ | Subquery correlata `ARRAY(SELECT x FROM …)` |
| `id(n)` | ✓ | Risolto alla colonna ID configurata del nodo |
| `labels(n)` | ✓ | `ARRAY['Label']` |
| `keys(n)` | ✓ | `ARRAY['prop1', 'prop2', …]` |
| `type(r)` | ✓ | Risolto a tempo di compilazione a un literal stringa `'REL_TYPE'`; nessuna colonna runtime |
| `length(p)` | ✓ | `_t.hops` per path CTE ricorsivi; `1` per path JOIN piatto |
| `CASE WHEN … THEN … ELSE … END` | ✓ | Diretto (forme searched e simple) |
| GROUP BY implicito | ✓ | Gli elementi RETURN non aggregati diventano chiavi GROUP BY quando un elemento qualsiasi ha un aggregato |

### Proiezioni map

| Sintassi | Mapping SQL |
| -------- | ------------ |
| `n { .prop1, .prop2 }` | `MAP(ARRAY['prop1','prop2'], ARRAY[n."prop1",n."prop2"])` |
| `n { .* }` | `MAP(ARRAY[all props...], ARRAY[n."col",...])` — espanso dallo schema |
| `n { .*, extra: expr }` | Tutte le proprietà dello schema più chiave nominata; MAP combinata |
| `n { key: expr }` | `MAP(ARRAY['key'], ARRAY[expr])` |

### Funzioni di aggregazione

| Cypher | SQL |
| -------- | ----- |
| `count(*)`, `count(x)` | diretto |
| `count(DISTINCT x)` | `count(DISTINCT x)` |
| `collect(x)` | `array_agg(x)` |
| `avg`, `sum`, `min`, `max` | diretto |
| `stDev(x)` | `stddev_samp(x)` |
| `stDevP(x)` | `stddev_pop(x)` |
| `percentileCont(x, p)` | `approx_percentile(x, p)` |
| `percentileDisc(x, p)` | `approx_percentile(x, p)` |

### Funzioni stringa

| Cypher | SQL |
| -------- | ----- |
| `toLower(x)` | `lower(x)` |
| `toUpper(x)` | `upper(x)` |
| `ltrim(x)`, `rtrim(x)`, `trim(x)` | diretto |
| `replace(x, a, b)` | diretto |
| `reverse(x)` | diretto |
| `split(x, d)` | diretto |
| `left(x, n)` | `left(x, n)` |
| `right(x, n)` | `right(x, n)` |
| `substring(x, start, len)` | `substr(x, start+1, len)` (indice 0→1) |
| `size(string)` | `char_length(string)` |
| `size(list)` | `cardinality(list)` |

### Funzioni di conversione tipo

| Cypher | SQL |
| -------- | ----- |
| `toString(x)` | `CAST(x AS VARCHAR)` |
| `toInteger(x)` | `TRY_CAST(x AS BIGINT)` |
| `toFloat(x)` | `TRY_CAST(x AS DOUBLE)` |
| `toBoolean(x)` | `TRY_CAST(x AS BOOLEAN)` |
| `toStringOrNull`, `toIntegerOrNull`, `toFloatOrNull`, `toBooleanOrNull` | varianti `TRY_CAST` |

### Funzioni matematiche

| Cypher | SQL |
| -------- | ----- |
| `log(x)` | `ln(x)` (logaritmo naturale) |
| `log2(x)` | `log2(x)` |
| `range(start, end)` | `sequence(start, end)` |
| `abs`, `sqrt`, `ceil`, `floor`, `round`, `sign` | passate direttamente |

### Funzioni lista

| Cypher | SQL |
| -------- | ----- |
| `head(list)` | `element_at(list, 1)` |
| `last(list)` | `element_at(list, -1)` |
| `tail(list)` | `slice(list, 2, cardinality(list))` |
| `isEmpty(list)` | `cardinality(list) = 0` |

### List Comprehension

| Sintassi | Mapping SQL |
| -------- | ------------ |
| `[x IN list \| f(x)]` | `transform(list, x -> f(x))` |
| `[x IN list WHERE p(x)]` | `filter(list, x -> p(x))` |
| `[x IN list WHERE p(x) \| f(x)]` | `transform(filter(list, x -> p(x)), x -> f(x))` |
| `any(x IN list WHERE p(x))` | `any_match(list, x -> p(x))` |
| `all(x IN list WHERE p(x))` | `all_match(list, x -> p(x))` |
| `none(x IN list WHERE p(x))` | `none_match(list, x -> p(x))` |
| `single(x IN list WHERE p(x))` | `cardinality(filter(list, x -> p(x))) = 1` |
| `reduce(acc = init, x IN list \| expr)` | `reduce(list, init, (acc, x) -> expr, acc -> acc)` |

### Pattern Comprehension

| Sintassi | Mapping SQL |
| -------- | ------------ |
| `[(a)-[:R]->(b) \| b.prop]` | `ARRAY(SELECT b."prop" FROM ... WHERE a.fk = b.pk)` |
| `[(a)-[]->(b:Label) \| b.prop]` | tipo inferito dal layer semantico; stessa forma subquery ARRAY |

### Subquery CALL correlate

`CALL { WITH x MATCH (x)-[:R]->(n) RETURN n.prop AS alias }` si traduce in `CROSS JOIN LATERAL (SELECT n."prop" AS alias FROM ... WHERE x."pk" = n."fk")`. (REQ-573) Regole:

- La variabile di scope esterno (`x`) deve comparire in `WITH`
- Sono supportate più variabili importate (`WITH a, b`)
- La prima relazione nel MATCH interno la cui sorgente è una variabile lateral-bound determina il `FROM` interno e la condizione di join
- I blocchi `CALL { ... }` top-level non correlati (senza `WITH`) sono gestiti da `cypher_calls_to_sql_list`

---

## Scritture

Cypher supporta tre pattern di scrittura tramite l'endpoint `/data/cypher`, eseguiti da `provisa/cypher/write_translator.py`. (REQ-818) [tool-verified: `provisa/api/rest/cypher_router.py:415-545`]

| Cypher | SQL | Req |
| -------- | ----- | ----- |
| `CREATE (n:Label {props})` | `INSERT INTO catalog.schema.table (cols) VALUES (vals)` | REQ-666 |
| `MATCH (n:Label) WHERE … DELETE n` | `DELETE FROM catalog.schema.table WHERE …` | REQ-667 |
| `MATCH (n:Label) WHERE … SET n.prop = val, …` | `UPDATE catalog.schema.table SET col = val, … WHERE …` | REQ-668 |

I nomi delle proprietà mappano a colonne tramite lo strip del prefisso di dominio e la risoluzione degli alias; i valori scalari Cypher vengono convertiti al tipo della colonna target. (REQ-666, REQ-668) Il corpo della risposta porta un conteggio `affected_rows`. (REQ-670)

Regole:

- L'etichetta deve risolvere esattamente a una tabella registrata. Etichette ambigue o sconosciute sono errori bloccanti; nessun matching fuzzy. (REQ-661) Non è possibile creare nuove etichette o tipi tramite Cypher. (REQ-662)
- Ogni scrittura è vincolata alla ACL `writable_by` della tabella target; un ruolo senza diritti di scrittura viene rifiutato a tempo di compilazione. (REQ-663)
- Il connettore dell'origine sottostante deve supportare DML. Le origini di sola lettura (federate via Trino, Iceberg senza connettore Delta) rifiutano le scritture a tempo di traduzione. (REQ-664)
- Le relazioni non possono essere scritte — sono derivate dai join dichiarati nel layer semantico, non archi memorizzati. Targettare una relazione è un errore bloccante. (REQ-665) Un arco basato su una tabella di giunzione non fa eccezione: la tabella associativa che lo sostiene è essa stessa una tabella registrata, e le righe si scrivono in quella tabella, non nell'arco. (REQ-1586)
- Le scritture passano attraverso l'intera pipeline di scrittura: iniezione RLS e hook post-mutation (invalidazione della cache di risposta, marcatura stale delle viste materializzate, eventi di modifica Kafka, ricarica hot-table). (REQ-798)
- `MERGE`, `DETACH DELETE`, e `REMOVE` non sono supportati e vengono rifiutati a tempo di parsing. (REQ-671)

---

## Accesso al protocollo

Cypher raggiunge la stessa pipeline governata su due trasporti:

- **HTTP** — `POST /data/cypher` con un corpo JSON (`{"query": "...", "params": {...}}`). Restituisce righe tipizzate, o `affected_rows` per le scritture. Le variabili grafo nella clausola `RETURN` vengono serializzate come JSON: i nodi portano `id`, `label`, `tableLabel`, e `properties`; gli archi portano `identity`, `start`, `end`, `type`, `properties`, `startNode`, e `endNode`; i path portano `nodes`, `edges`, e `length`/`hops`. (REQ-750) I comandi registrati sono anche invocabili qui via `CALL fn(args) YIELD col1, col2` — gli argomenti posizionali mappano ai nomi degli argomenti dichiarati del comando in ordine. (REQ-1156) [tool-verified: `provisa/api/rest/registered_call.py:113-143`]
- **Bolt** — un server protocollo binario compatibile Neo4j (codec PackStream, framing chunked) che permette a Neo4j Browser, Bloom, e driver Bolt di eseguire Cypher sul grafo federato. (REQ-802) Si avvia quando `PROVISA_BOLT_PORT` è impostato a un valore diverso da zero ed è disabilitato per default; imposta `PROVISA_BOLT_CERT` / `PROVISA_BOLT_KEY` per TLS. [tool-verified: `provisa/api/app_startup.py:317-338`] L'autenticazione Bolt mappa il principal a un utente e il database a un ruolo: `SHOW DATABASES` elenca una voce per ogni coppia (vista × ruolo), nominata `provisa_<role>` (domini business) o `provisa_ops_<role>` (con domini system/meta/ops); `:use` seleziona il ruolo e la vista attivi. (REQ-807) Le relazioni ricevono ID interi durevoli tramite una tabella `rel_ids`, rispecchiando il design di `node_ids`. (REQ-806) I comandi registrati sono invocabili con `CALL command(args)` — gli argomenti posizionali mappano ai nomi degli argomenti dichiarati in ordine; le procedure `CALL dbms.*` / `CALL db.*` hanno la precedenza. (REQ-1156) [tool-verified: `provisa/bolt/session.py:722-749`]

### Graph Analytics

`POST /data/graph-analytics` esegue una query Cypher, costruisce un grafo NetworkX in-memory dai nodi e archi risultanti, esegue un algoritmo nominato, e unisce un dict `_analytics` in ogni nodo e arco prima di restituirli come JSON con un campo `elapsed_ms`. (REQ-642) Le chiavi `_analytics` variano per algoritmo: la centralità produce `score`; il rilevamento di community produce `cluster`; il k-core produce `core_number`; la centralità di grado aggiunge `in_degree` e `out_degree`. (REQ-643) L'endpoint rifiuta grafi sopra una dimensione configurabile (default 10.000 nodi / 50.000 archi) con HTTP 413; Girvan-Newman è limitato a 500 nodi a meno che il chiamante non passi `force=true`. (REQ-650, REQ-651)

---

## Limitazioni

### Vincoli di design

1. **Le scritture sono limitate a `CREATE`, `SET`, e `DELETE`.** Queste vengono eseguite come scritture dirette su tabella attraverso la stessa pipeline delle mutation GraphQL e SQL. (REQ-818, REQ-666, REQ-667, REQ-668) Vedi §Scritture sopra. `MERGE`, `DETACH DELETE`, e `REMOVE` vengono rifiutati a tempo di parsing. (REQ-671, REQ-818) Anche le procedure APOC vengono rifiutate.

2. **Le proprietà di relazione esistono solo sugli archi basati su una giunzione.** Un arco dichiarato su una coppia di colonne di chiave esterna esiste solo come metadati di join nel layer semantico (REQ-574) e non porta attributi memorizzati, quindi `WHERE r.since > 2020` o `RETURN r.weight` non hanno significato su di esso. Un arco dichiarato su una tabella di giunzione li porta eccome: le restanti colonne della tabella associativa sono le proprietà della relazione, `RETURN r` le restituisce e un `WHERE` su una di esse compila in un predicato sull'alias della giunzione — quindi restringe la traversata invece di filtrare righe già assemblate. (REQ-1586) La tabella di giunzione stessa scompare dal lato nodi dello schema del grafo; qui è un arco e ovunque altrove è una tabella.

3. **L'attraversamento bidirezionale** `(a)-[]-(b)` si riscrive nella UNION ALL forward+backward di tutte le relazioni dirette corrispondenti dal layer semantico. (REQ-575) Ogni relazione nel layer semantico è direzionale; la sintassi bidirezionale è zucchero sintattico che si espande in entrambe le direzioni. I rami extra vengono emessi al livello di query più esterno — i pattern MATCH successivi nella stessa query non vengono duplicati tra i rami (limitazione per bidirezionale multi-MATCH).

4. **I path ricorsivi richiedono un limite.** I pattern a lunghezza variabile (`[*]`) devono includere un limite superiore (es. `[*..10]`). (REQ-348) L'attraversamento illimitato viene rifiutato a tempo di parsing per prevenire CTE ricorsive fuori controllo.

### Note sul comportamento

5. **`shortestPath` su path non self-referenziali usa JOIN piatto, non ordinamento per hop.** Quando i tipi di inizio e fine differiscono e non esiste alcuna relazione self-referenziale nello schema, il traduttore emette una catena di JOIN piatta (il path schema più breve). (REQ-576) Non emette `ORDER BY hops` perché gli hop non sono tracciati in quel percorso di codice. Il risultato è il path schema strutturalmente più breve, non il path data-più-breve su più righe.

6. **Path schema multipli producono `UNION ALL`.** Quando due path schema con lo stesso numero di hop collegano gli stessi tipi di inizio e fine (es. `Person -[WORKS_AT]-> Company` e `Person -[MANAGES]-> Company`), entrambi vengono emessi come rami `UNION ALL`. (REQ-577) La deduplicazione delle righe che compaiono in entrambi i rami non viene eseguita.

7. **Un `RelationshipMapping` per ogni combinazione coppia sorgente→target e rel\_type.** Se due campi GraphQL sullo stesso tipo sorgente producono la stessa stringa `rel_type` (dopo l'uppercasing) verso lo stesso tipo target, la seconda registrazione sovrascrive la prima in `CypherLabelMap.relationships`. La chiave di relazione include i nomi dei tipi sorgente e target, quindi coppie sorgente/target distinte con lo stesso nome di tipo ottengono ciascuna la propria voce e non ne sono influenzate.

8. **Le CTE della clausola `WITH` sono nominate `_w0`, `_w1`, …** (REQ-578) I nomi vengono assegnati posizionalmente all'interno di una singola chiamata di traduzione. Comporre più query tradotte (es. in un batch) può produrre nomi CTE in collisione se vengono concatenate in modo ingenuo.

### Copertura di espressioni e pattern (REQ-913)

Le espressioni Cypher vengono analizzate in un AST e abbassate nodo-a-nodo a SQL (`provisa/cypher/expr_parser.py`, `provisa/cypher/expr_visitor.py`). La grammatica segue la torre di precedenza `oC_Expression` di openCypher. Supportati: literal, parametri, accesso proprietà, `n.prop`, indice e slice, aritmetica (`+ - * / % ^`), confronto, `IN`, `STARTS WITH` / `ENDS WITH` / `CONTAINS` / `=~`, `IS [NOT] NULL`, booleani `AND` / `OR` / `XOR` / `NOT`, `CASE`, literal lista e map, list e pattern comprehension (incluso il binding di path `p = (…)`), proiezione map, `reduce`, i quantificatori `all` / `any` / `none` / `single`, subquery esistenziali, e chiamate a funzione.

9. **Le etichette sono fisse; non è possibile creare tipi oggetto tramite Cypher.** Un'etichetta risolve a un dominio conosciuto, un tipo oggetto conosciuto, o un `domain:object_type` qualificato — l'insieme chiuso definito dallo schema registrato. Cypher non introduce mai una nuova etichetta o tipo. La creazione di istanze è possibile solo per tipi già definiti all'interno di un'origine dati scrivibile; `CREATE` scrive righe in tale tabella (vedi §Scritture) ma non può definire una nuova etichetta o tipo. (REQ-662) Entrambe le forme di etichetta sono accettate e significano lo stesso test: la forma postfissa `n:Label` e quella verbosa `n IS :Label` (e la loro negazione `n IS NOT :Label`). Un'etichetta qualificata si scrive `n:domain:object_type`.

10. **`shortestPath` e `allShortestPaths` sono supportati solo dentro `MATCH`, non come espressioni.** In un pattern (`MATCH p = shortestPath((a:Person)-[:KNOWS*..5]->(b:Person))`) si traducono in una CTE `WITH RECURSIVE` e richiedono nodi sorgente e target etichettati. Usati in posizione di espressione — per esempio `RETURN shortestPath((a)-[*]->(b))` o `WHERE length(shortestPath((a)-[*]->(b))) < 5` — non sono supportati, perché la riscrittura ricorsiva è guidata dalla clausola `MATCH` piuttosto che da una subquery correlata.

11. **List comprehension, `REDUCE`, e quantificatori operano su valori lista; le pattern comprehension attraversano.** `reduce(...)`, `all/any/none/single(...)`, e la list comprehension `[x IN list | …]` operano su un'espressione lista e si abbassano alle funzioni lista higher-order del motore — non attraversano esse stesse il grafo. La comprehension di **pattern** `[(a)-[:R]->(b) WHERE p | e]` attraversa invece: il suo pattern grafo viene indirizzato come subquery correlata, quindi è una comprehension la cui sorgente è un attraversamento. Alimenta i risultati di attraversamento nelle forme lista con `nodes(p)` / `relationships(p)` / `collect(...)`, oppure usa direttamente una pattern comprehension.
