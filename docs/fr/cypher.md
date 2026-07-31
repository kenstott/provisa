# Prise en charge des requêtes Cypher

Provisa traduit un sous-ensemble d'openCypher en SQL via le module `provisa/cypher/`. (REQ-345, REQ-347) Les requêtes sont analysées par un analyseur syntaxique à descente récursive maison (sans bibliothèque Cypher externe) (REQ-571), résolues au niveau du schéma par rapport à la couche sémantique (REQ-351), puis émises en SQL avant d'être acheminées vers le moteur d'exécution cible. (REQ-066, REQ-067, REQ-347)

## Fonctionnalités implémentées

### Clauses

| Clause | État | Remarques |
| -------- | -------- | ------- |
| `MATCH (n:Label)` | ✓ | Modèles de nœud avec étiquettes, variables, propriétés en ligne |
| `OPTIONAL MATCH` | ✓ | Émet un LEFT JOIN |
| `WHERE` | ✓ | Prise en charge complète des expressions ; appliquée après MATCH |
| `RETURN` | ✓ | Astérisque, accès aux propriétés, expressions, alias |
| `RETURN DISTINCT` | ✓ | Émet un SELECT DISTINCT |
| `WITH` | ✓ | Émet une CTE nommée (`_w0`, `_w1`, …) ; prend en charge `WITH … WHERE` |
| `ORDER BY` | ✓ | ASC / DESC |
| `SKIP` / `LIMIT` | ✓ | Correspond à SQL OFFSET / LIMIT |
| `UNION` / `UNION ALL` | ✓ | Union récursive entre sous-AST |
| `CALL { … }` | ✓ | Décomposition de sous-requête CALL de premier niveau via `cypher_calls_to_sql_list` |
| `CALL { WITH x … }` | ✓ | Sous-requête corrélée → `CROSS JOIN LATERAL` ; voir §CALL corrélé |
| `CALL db.labels()` | ✓ | Renvoie les étiquettes de nœud de la couche sémantique ; aucune traduction SQL (REQ-572) |
| `CALL db.relationshipTypes()` | ✓ | Renvoie les types de relation de la couche sémantique (REQ-572) |
| `CALL db.propertyKeys()` | ✓ | Renvoie tous les noms de clés de propriété pour tous les types de nœud (REQ-572) |
| `UNWIND` | ✓ | Expansion d'un tableau en lignes ; le premier élément devient le FROM, les suivants deviennent des CROSS JOIN UNNEST |

### Modèles de correspondance

| Modèle | État | Remarques |
| --------- | -------- | ------- |
| `(n)` — nœud sans étiquette | ✓ | UNION ALL sur tous les types connus |
| `(n:Label)` | ✓ | Correspond à la table enregistrée pour ce type GraphQL |
| `(n:Label {prop: val})` | ✓ | Le filtre de propriété en ligne devient un WHERE |
| `(a)-[:TYPE]->(b)` | ✓ | Dirigé, à un seul saut |
| `(a)<-[:TYPE]-(b)` | ✓ | Parcours inverse ; colonnes de jointure inversées |
| `(a)-[]->(b)` | ✓ | Toute relation dirigée a→b ; UNION ALL si plusieurs types correspondent |
| `(a)-[]-(b)` | ✓ | Bidirectionnel ; se transforme en UNION ALL de toutes les relations directes et inverses |
| `(a)-[:TYPE*..N]->(b)` | ✓ | Longueur variable avec borne supérieure ; CTE récursive pour les cas autoréférentiels, JOIN simple sinon |
| `(a)-[]->(b)-[]->(c)` | ✓ | JOIN chaînés multi-sauts |
| `(n:DomainLabel)` | ✓ | Étiquette de domaine → sous-requête UNION ALL sur tous les types du domaine |
| `(n:A\|B)` | ✓ | Alternance d'étiquettes → domaine ad hoc injecté dans le mappage d'étiquettes ; UNION ALL sur les types correspondants |
| `shortestPath(…)` | ✓ | JOIN simple pour les extrémités hétérogènes ; CTE WITH RECURSIVE pour les cas de même type/autoréférentiels |
| `allShortestPaths(…)` | ✓ | Identique à shortestPath mais sans LIMIT 1 |

### Expressions et prédicats

| Fonctionnalité | État | Correspondance SQL |
| --------- | -------- | ------------ |
| Accès aux propriétés `n.prop` | ✓ | `n."prop"` |
| Paramètres `$name` | ✓ | Positionnel `$N` |
| Paramètres hérités `{name}` | ✓ | Normalisé en `$name` au moment de l'analyse |
| Comparaison `=`, `<>`, `<`, `>`, `<=`, `>=` | ✓ | Directe |
| `AND`, `OR`, `NOT` | ✓ | Directe |
| `IS NULL` / `IS NOT NULL` | ✓ | Directe |
| `IN [list]` | ✓ | SQL IN ; la syntaxe entre crochets `[...]` de Cypher est réécrite en `(...)` |
| `STARTS WITH` | ✓ | `starts_with(col, val)` |
| `ENDS WITH` | ✓ | `col LIKE CONCAT('%', val)` |
| `CONTAINS` | ✓ | `strpos(col, val) > 0` |
| `=~` regex | ✓ | `regexp_like(col, pattern)` |
| `exists(n.prop)` | ✓ | `(n.prop) IS NOT NULL` |
| `EXISTS { MATCH … }` | ✓ | Sous-requête corrélée `EXISTS (SELECT 1 FROM …)` |
| `COUNT { MATCH … }` | ✓ | Sous-requête corrélée `(SELECT count(*) FROM …)` |
| `COLLECT { MATCH … RETURN x }` | ✓ | Sous-requête corrélée `ARRAY(SELECT x FROM …)` |
| `id(n)` | ✓ | Résolu vers la colonne d'ID configurée du nœud |
| `labels(n)` | ✓ | `ARRAY['Label']` |
| `keys(n)` | ✓ | `ARRAY['prop1', 'prop2', …]` |
| `type(r)` | ✓ | Résolu au moment de la compilation vers le littéral de chaîne `'REL_TYPE'` ; aucune colonne à l'exécution |
| `length(p)` | ✓ | `_t.hops` pour les chemins de CTE récursive ; `1` pour les chemins de JOIN simple |
| `CASE WHEN … THEN … ELSE … END` | ✓ | Directe (formes recherchée et simple) |
| GROUP BY implicite | ✓ | Les éléments RETURN non agrégés deviennent des clés de GROUP BY dès qu'un élément comporte un agrégat |

### Projections de carte (map)

| Syntaxe | Correspondance SQL |
| -------- | ------------ |
| `n { .prop1, .prop2 }` | `MAP(ARRAY['prop1','prop2'], ARRAY[n."prop1",n."prop2"])` |
| `n { .* }` | `MAP(ARRAY[all props...], ARRAY[n."col",...])` — développé à partir du schéma |
| `n { .*, extra: expr }` | Toutes les propriétés du schéma plus la clé nommée ; MAP combinée |
| `n { key: expr }` | `MAP(ARRAY['key'], ARRAY[expr])` |

### Fonctions d'agrégation

| Cypher | SQL |
| -------- | ----- |
| `count(*)`, `count(x)` | directe |
| `count(DISTINCT x)` | `count(DISTINCT x)` |
| `collect(x)` | `array_agg(x)` |
| `avg`, `sum`, `min`, `max` | directe |
| `stDev(x)` | `stddev_samp(x)` |
| `stDevP(x)` | `stddev_pop(x)` |
| `percentileCont(x, p)` | `approx_percentile(x, p)` |
| `percentileDisc(x, p)` | `approx_percentile(x, p)` |

### Fonctions de chaîne

| Cypher | SQL |
| -------- | ----- |
| `toLower(x)` | `lower(x)` |
| `toUpper(x)` | `upper(x)` |
| `ltrim(x)`, `rtrim(x)`, `trim(x)` | directe |
| `replace(x, a, b)` | directe |
| `reverse(x)` | directe |
| `split(x, d)` | directe |
| `left(x, n)` | `left(x, n)` |
| `right(x, n)` | `right(x, n)` |
| `substring(x, start, len)` | `substr(x, start+1, len)` (index 0→1) |
| `size(string)` | `char_length(string)` |
| `size(list)` | `cardinality(list)` |

### Fonctions de conversion de type

| Cypher | SQL |
| -------- | ----- |
| `toString(x)` | `CAST(x AS VARCHAR)` |
| `toInteger(x)` | `TRY_CAST(x AS BIGINT)` |
| `toFloat(x)` | `TRY_CAST(x AS DOUBLE)` |
| `toBoolean(x)` | `TRY_CAST(x AS BOOLEAN)` |
| `toStringOrNull`, `toIntegerOrNull`, `toFloatOrNull`, `toBooleanOrNull` | variantes `TRY_CAST` |

### Fonctions mathématiques

| Cypher | SQL |
| -------- | ----- |
| `log(x)` | `ln(x)` (logarithme naturel) |
| `log2(x)` | `log2(x)` |
| `range(start, end)` | `sequence(start, end)` |
| `abs`, `sqrt`, `ceil`, `floor`, `round`, `sign` | transmises telles quelles |

### Fonctions de liste

| Cypher | SQL |
| -------- | ----- |
| `head(list)` | `element_at(list, 1)` |
| `last(list)` | `element_at(list, -1)` |
| `tail(list)` | `slice(list, 2, cardinality(list))` |
| `isEmpty(list)` | `cardinality(list) = 0` |

### Compréhensions de liste

| Syntaxe | Correspondance SQL |
| -------- | ------------ |
| `[x IN list \| f(x)]` | `transform(list, x -> f(x))` |
| `[x IN list WHERE p(x)]` | `filter(list, x -> p(x))` |
| `[x IN list WHERE p(x) \| f(x)]` | `transform(filter(list, x -> p(x)), x -> f(x))` |
| `any(x IN list WHERE p(x))` | `any_match(list, x -> p(x))` |
| `all(x IN list WHERE p(x))` | `all_match(list, x -> p(x))` |
| `none(x IN list WHERE p(x))` | `none_match(list, x -> p(x))` |
| `single(x IN list WHERE p(x))` | `cardinality(filter(list, x -> p(x))) = 1` |
| `reduce(acc = init, x IN list \| expr)` | `reduce(list, init, (acc, x) -> expr, acc -> acc)` |

### Compréhensions de modèle

| Syntaxe | Correspondance SQL |
| -------- | ------------ |
| `[(a)-[:R]->(b) \| b.prop]` | `ARRAY(SELECT b."prop" FROM ... WHERE a.fk = b.pk)` |
| `[(a)-[]->(b:Label) \| b.prop]` | type inféré à partir de la couche sémantique ; même forme de sous-requête ARRAY |

### Sous-requêtes CALL corrélées

`CALL { WITH x MATCH (x)-[:R]->(n) RETURN n.prop AS alias }` se traduit par `CROSS JOIN LATERAL (SELECT n."prop" AS alias FROM ... WHERE x."pk" = n."fk")`. (REQ-573) Règles :

- La variable de portée externe (`x`) doit apparaître dans `WITH`
- Plusieurs variables importées (`WITH a, b`) sont prises en charge
- La première relation du MATCH interne dont la source est une variable liée par lateral détermine le `FROM` interne et la condition de jointure
- Les blocs `CALL { ... }` de premier niveau non corrélés (sans `WITH`) sont pris en charge par `cypher_calls_to_sql_list`

---

## Écritures

Cypher prend en charge trois modèles d'écriture via le endpoint `/data/cypher`, exécutés par `provisa/cypher/write_translator.py`. (REQ-818) [tool-verified : `provisa/api/rest/cypher_router.py:415-545`]

| Cypher | SQL | Exigence |
| -------- | ----- | ----- |
| `CREATE (n:Label {props})` | `INSERT INTO catalog.schema.table (cols) VALUES (vals)` | REQ-666 |
| `MATCH (n:Label) WHERE … DELETE n` | `DELETE FROM catalog.schema.table WHERE …` | REQ-667 |
| `MATCH (n:Label) WHERE … SET n.prop = val, …` | `UPDATE catalog.schema.table SET col = val, … WHERE …` | REQ-668 |

Les noms de propriété sont mappés aux colonnes par suppression du préfixe de domaine et résolution d'alias ; les valeurs scalaires Cypher sont converties vers le type de colonne cible. (REQ-666, REQ-668) Le corps de la réponse contient un compteur `affected_rows`. (REQ-670)

Règles :

- L'étiquette doit correspondre à exactement une table enregistrée. Les étiquettes ambiguës ou inconnues provoquent des erreurs bloquantes ; aucune correspondance approximative. (REQ-661) Aucune nouvelle étiquette ni aucun nouveau type ne peut être créé via Cypher. (REQ-662)
- Chaque écriture est conditionnée par la liste de contrôle d'accès `writable_by` de la table cible ; un rôle sans droit d'écriture est rejeté au moment de la compilation. (REQ-663)
- Le connecteur de la source sous-jacente doit prendre en charge le DML. Les sources en lecture seule (fédérées via Trino, Iceberg sans connecteur Delta) rejettent les écritures au moment de la traduction. (REQ-664)
- Les relations ne peuvent pas être écrites — elles sont dérivées de jointures par clé étrangère, et non stockées comme des arêtes. Cibler une relation constitue une erreur bloquante. (REQ-665)
- Les écritures traversent l'intégralité du pipeline d'écriture : injection RLS et hooks post-mutation (invalidation du cache de réponse, marquage des vues matérialisées comme obsolètes, événements de changement Kafka, rechargement des tables actives). (REQ-798)
- `MERGE`, `DETACH DELETE` et `REMOVE` ne sont pas pris en charge et sont rejetés au moment de l'analyse. (REQ-671)

---

## Accès par protocole

Cypher atteint le même pipeline gouverné via deux transports :

- **HTTP** — `POST /data/cypher` avec un corps JSON (`{"query": "...", "params": {...}}`). Renvoie des lignes typées, ou `affected_rows` pour les écritures. Les variables de graphe de la clause `RETURN` sont sérialisées en JSON : les nœuds portent `id`, `label`, `tableLabel` et `properties` ; les arêtes portent `identity`, `start`, `end`, `type`, `properties`, `startNode` et `endNode` ; les chemins portent `nodes`, `edges` et `length`/`hops`. (REQ-750) Les commandes enregistrées sont également appelables ici via `CALL fn(args) YIELD col1, col2` — les arguments positionnels correspondent aux noms d'arguments déclarés de la commande, dans l'ordre. (REQ-1156) [tool-verified : `provisa/api/rest/registered_call.py:113-143`]
- **Bolt** — un serveur de protocole binaire compatible Neo4j (codec PackStream, découpage en trames) qui permet à Neo4j Browser, Bloom et aux pilotes Bolt d'exécuter des requêtes Cypher sur le graphe fédéré. (REQ-802) Il démarre lorsque `PROVISA_BOLT_PORT` est défini avec une valeur non nulle et est désactivé par défaut ; définissez `PROVISA_BOLT_CERT` / `PROVISA_BOLT_KEY` pour le TLS. [tool-verified : `provisa/api/app_startup.py:317-338`] L'authentification Bolt fait correspondre le principal à un utilisateur et la base de données à un rôle : `SHOW DATABASES` liste une entrée par paire (vue × rôle), nommée `provisa_<role>` (domaines métier) ou `provisa_ops_<role>` (avec les domaines system/meta/ops) ; `:use` sélectionne le rôle et la vue actifs. (REQ-807) Les relations reçoivent des ID entiers durables via une table `rel_ids`, sur le modèle de `node_ids`. (REQ-806) Les commandes enregistrées sont appelables avec `CALL command(args)` — les arguments positionnels correspondent aux noms d'arguments déclarés, dans l'ordre ; les procédures `CALL dbms.*` / `CALL db.*` sont prioritaires. (REQ-1156) [tool-verified : `provisa/bolt/session.py:722-749`]

### Analytique de graphe

`POST /data/graph-analytics` exécute une requête Cypher, construit un graphe NetworkX en mémoire à partir des nœuds et arêtes obtenus, exécute un algorithme nommé, puis fusionne un dictionnaire `_analytics` dans chaque nœud et arête avant de les renvoyer en JSON avec un champ `elapsed_ms`. (REQ-642) Les clés de `_analytics` varient selon l'algorithme : la centralité produit `score` ; la détection de communautés produit `cluster` ; le k-core produit `core_number` ; la centralité de degré ajoute `in_degree` et `out_degree`. (REQ-643) Le endpoint rejette les graphes dépassant une taille configurable (10 000 nœuds / 50 000 arêtes par défaut) avec un code HTTP 413 ; Girvan-Newman est plafonné à 500 nœuds sauf si l'appelant transmet `force=true`. (REQ-650, REQ-651)

---

## Limitations

### Contraintes de conception

1. **Les écritures se limitent à `CREATE`, `SET` et `DELETE`.** Elles s'exécutent comme des écritures directes en table via le même pipeline que les mutations GraphQL et SQL. (REQ-818, REQ-666, REQ-667, REQ-668) Voir §Écritures ci-dessus. `MERGE`, `DETACH DELETE` et `REMOVE` sont rejetés au moment de l'analyse. (REQ-671, REQ-818) Les procédures APOC sont également rejetées.

2. **Aucune propriété de relation.** Les relations (`-[r:TYPE]->`) existent uniquement comme métadonnées de jointure dans la couche sémantique. (REQ-574) Elles ne portent aucun attribut stocké, si bien que `WHERE r.since > 2020` ou `RETURN r.weight` n'ont aucun sens et ne sont pas pris en charge.

3. **Le parcours bidirectionnel** `(a)-[]-(b)` est réécrit sous forme d'UNION ALL direct + inverse de toutes les relations dirigées correspondantes de la couche sémantique. (REQ-575) Chaque relation de la couche sémantique est directionnelle ; la syntaxe bidirectionnelle est un sucre syntaxique qui se développe dans les deux directions. Les branches supplémentaires sont émises au niveau le plus externe de la requête — les modèles MATCH suivants dans la même requête ne sont pas dupliqués entre les branches (limitation pour le cas bidirectionnel multi-MATCH).

4. **Les chemins récursifs nécessitent une borne.** Les modèles de longueur variable (`[*]`) doivent inclure une borne supérieure (par exemple `[*..10]`). (REQ-348) Le parcours non borné est rejeté au moment de l'analyse afin d'éviter des CTE récursives incontrôlées.

### Notes de comportement

5. **`shortestPath` sur des chemins non autoréférentiels utilise un JOIN simple, pas un tri par nombre de sauts.** Lorsque les types de départ et d'arrivée diffèrent et qu'aucune relation autoréférentielle n'existe dans le schéma, le traducteur émet une chaîne de JOIN simple (le chemin de schéma le plus court). (REQ-576) Il n'émet pas de `ORDER BY hops`, car les sauts ne sont pas suivis dans ce chemin de code. Le résultat est le chemin de schéma structurellement le plus court, et non le chemin le plus court en données parmi plusieurs lignes.

6. **Plusieurs chemins de schéma produisent un `UNION ALL`.** Lorsque deux chemins de schéma comportant le même nombre de sauts relient les mêmes types de départ et d'arrivée (par exemple `Person -[WORKS_AT]-> Company` et `Person -[MANAGES]-> Company`), les deux sont émis sous forme de branches `UNION ALL`. (REQ-577) La déduplication des lignes apparaissant dans les deux branches n'est pas effectuée.

7. **Un seul `RelationshipMapping` par combinaison de paire source→cible et de rel\_type.** Si deux champs GraphQL du même type source produisent la même chaîne `rel_type` (après mise en majuscules) vers le même type cible, le second enregistrement écrase le premier dans `CypherLabelMap.relationships`. La clé de la relation inclut les noms des types source et cible ; des paires source/cible distinctes portant le même nom de type obtiennent donc chacune leur propre entrée et ne sont pas affectées.

8. **Les CTE de la clause `WITH` sont nommées `_w0`, `_w1`, …** (REQ-578) Les noms sont attribués de manière positionnelle au sein d'un seul appel de traduction. La composition de plusieurs requêtes traduites (par exemple dans un lot) peut produire des collisions de noms de CTE si elles sont concaténées sans précaution.

### Couverture des expressions et des modèles (REQ-913)

Les expressions Cypher sont analysées en un AST puis réduites nœud par nœud en SQL (`provisa/cypher/expr_parser.py`, `provisa/cypher/expr_visitor.py`). La grammaire suit la tour de précédence `oC_Expression` d'openCypher. Pris en charge : littéraux, paramètres, accès aux propriétés, `n.prop`, index et découpe (slice), arithmétique (`+ - * / % ^`), comparaison, `IN`, `STARTS WITH` / `ENDS WITH` / `CONTAINS` / `=~`, `IS [NOT] NULL`, booléens `AND` / `OR` / `XOR` / `NOT`, `CASE`, littéraux de liste et de carte, compréhensions de liste et de modèle (y compris la liaison de chemin `p = (…)`), projection de carte, `reduce`, les quantificateurs `all` / `any` / `none` / `single`, les sous-requêtes existentielles et les appels de fonction.

9. **Les étiquettes sont fixes ; il est impossible de créer des types d'objet via Cypher.** Une étiquette se résout vers un domaine connu, un type d'objet connu, ou un `domain:object_type` qualifié — l'ensemble fermé défini par le schéma enregistré. Cypher n'introduit jamais de nouvelle étiquette ni de nouveau type. La création d'instances n'est possible que pour des types déjà définis dans une source de données inscriptible ; `CREATE` écrit des lignes dans une telle table (voir §Écritures) mais ne peut pas définir de nouvelle étiquette ni de nouveau type. (REQ-662) Les deux formes d'étiquette sont acceptées et représentent le même test : la forme postfixée `n:Label` et la forme longue `n IS :Label` (ainsi que leur négation `n IS NOT :Label`). Une étiquette qualifiée s'écrit `n:domain:object_type`.

10. **`shortestPath` et `allShortestPaths` ne sont pris en charge qu'à l'intérieur de `MATCH`, pas en tant qu'expressions.** Dans un modèle (`MATCH p = shortestPath((a:Person)-[:KNOWS*..5]->(b:Person))`), ils se traduisent par une CTE `WITH RECURSIVE` et exigent des nœuds source et cible étiquetés. Utilisés en position d'expression — par exemple `RETURN shortestPath((a)-[*]->(b))` ou `WHERE length(shortestPath((a)-[*]->(b))) < 5` — ils ne sont pas pris en charge, car la réécriture récursive est pilotée par la clause `MATCH` plutôt que par une sous-requête corrélée.

11. **Les compréhensions de liste, `REDUCE` et les quantificateurs opèrent sur des valeurs de liste ; les compréhensions de modèle parcourent le graphe.** `reduce(...)`, `all/any/none/single(...)` et la compréhension de liste `[x IN list | …]` opèrent sur une expression de liste et se réduisent vers les fonctions de liste d'ordre supérieur du moteur — elles ne parcourent pas elles-mêmes le graphe. La compréhension de **modèle** `[(a)-[:R]->(b) WHERE p | e]` parcourt bien le graphe : son modèle de graphe est résolu comme une sous-requête corrélée ; il s'agit donc d'une compréhension dont la source est un parcours. Injectez les résultats du parcours dans les formes de liste avec `nodes(p)` / `relationships(p)` / `collect(...)`, ou utilisez directement une compréhension de modèle.
