# Prise en charge des requêtes Cypher {: #cypher-query-support }

Provisa traduit un sous-ensemble d'openCypher en SQL via le module `provisa/cypher/`. Les requêtes sont analysées par un analyseur syntaxique à descente récursive maison (sans bibliothèque Cypher externe), résolues au niveau du schéma par rapport au modèle, puis émises en SQL avant d'être acheminées vers le moteur d'exécution cible.

## Fonctionnalités implémentées {: #implemented-features }

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
| `CALL db.labels()` | ✓ | Renvoie les étiquettes de nœud du modèle ; aucune traduction SQL |
| `CALL db.relationshipTypes()` | ✓ | Renvoie les types de relation du modèle |
| `CALL db.propertyKeys()` | ✓ | Renvoie tous les noms de clés de propriété pour tous les types de nœud |
| `UNWIND` | ✓ | Expansion d'un tableau en lignes ; le premier élément devient le FROM, les suivants deviennent des CROSS JOIN UNNEST |

### Modèles de correspondance {: #match-patterns }

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

### Expressions et prédicats {: #expressions-and-predicates }

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

### Projections de carte (map) {: #map-projections }

| Syntaxe | Correspondance SQL |
| -------- | ------------ |
| `n { .prop1, .prop2 }` | `MAP(ARRAY['prop1','prop2'], ARRAY[n."prop1",n."prop2"])` |
| `n { .* }` | `MAP(ARRAY[all props...], ARRAY[n."col",...])` — développé à partir du schéma |
| `n { .*, extra: expr }` | Toutes les propriétés du schéma plus la clé nommée ; MAP combinée |
| `n { key: expr }` | `MAP(ARRAY['key'], ARRAY[expr])` |

**Propriétés objet.** Une colonne contenant un objet déclaré (l'objet imbriqué d'un type GraphQL distant, ou toute colonne enregistrée avec des champs d'objet) est une propriété de type map : `RETURN n.address` la donne comme une map, quelle que soit la forme sous laquelle le moteur la renvoie, et le nœud `n` la porte parmi ses propriétés. Un champ GraphQL distant qui pointe vers un type enregistré, un objet ou une liste, n'est pas une propriété ; on l'atteint par sa relation.

### Fonctions d'agrégation {: #aggregation-functions }

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

### Fonctions de chaîne {: #string-functions }

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

### Fonctions de conversion de type {: #type-conversion-functions }

| Cypher | SQL |
| -------- | ----- |
| `toString(x)` | `CAST(x AS VARCHAR)` |
| `toInteger(x)` | `TRY_CAST(x AS BIGINT)` |
| `toFloat(x)` | `TRY_CAST(x AS DOUBLE)` |
| `toBoolean(x)` | `TRY_CAST(x AS BOOLEAN)` |
| `toStringOrNull`, `toIntegerOrNull`, `toFloatOrNull`, `toBooleanOrNull` | variantes `TRY_CAST` |

### Fonctions mathématiques {: #math-functions }

| Cypher | SQL |
| -------- | ----- |
| `log(x)` | `ln(x)` (logarithme naturel) |
| `log2(x)` | `log2(x)` |
| `range(start, end)` | `sequence(start, end)` |
| `abs`, `sqrt`, `ceil`, `floor`, `round`, `sign` | transmises telles quelles |

### Fonctions de liste {: #list-functions }

| Cypher | SQL |
| -------- | ----- |
| `head(list)` | `element_at(list, 1)` |
| `last(list)` | `element_at(list, -1)` |
| `tail(list)` | `slice(list, 2, cardinality(list))` |
| `isEmpty(list)` | `cardinality(list) = 0` |

### Compréhensions de liste {: #list-comprehensions }

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

### Compréhensions de modèle {: #pattern-comprehensions }

| Syntaxe | Correspondance SQL |
| -------- | ------------ |
| `[(a)-[:R]->(b) \| b.prop]` | `ARRAY(SELECT b."prop" FROM ... WHERE a.fk = b.pk)` |
| `[(a)-[]->(b:Label) \| b.prop]` | type inféré à partir du modèle ; même forme de sous-requête ARRAY |

### Sous-requêtes CALL corrélées {: #correlated-call-subqueries }

`CALL { WITH x MATCH (x)-[:R]->(n) RETURN n.prop AS alias }` se traduit par `CROSS JOIN LATERAL (SELECT n."prop" AS alias FROM ... WHERE x."pk" = n."fk")`. Règles :

- La variable de portée externe (`x`) doit apparaître dans `WITH`
- Plusieurs variables importées (`WITH a, b`) sont prises en charge
- La première relation du MATCH interne dont la source est une variable liée par lateral détermine le `FROM` interne et la condition de jointure
- Les blocs `CALL { ... }` de premier niveau non corrélés (sans `WITH`) sont pris en charge par `cypher_calls_to_sql_list`

---

## Écritures {: #writes }

Cypher prend en charge trois modèles d'écriture via le endpoint `/data/cypher`, exécutés par `provisa/cypher/write_translator.py`.

| Cypher | SQL |
|---|---|
| `CREATE (n:Label {props})` | `INSERT INTO catalog.schema.table (cols) VALUES (vals)` |
| `MATCH (n:Label) WHERE … DELETE n` | `DELETE FROM catalog.schema.table WHERE …` |
| `MATCH (n:Label) WHERE … SET n.prop = val, …` | `UPDATE catalog.schema.table SET col = val, … WHERE …` |

Les noms de propriété sont mappés aux colonnes par suppression du préfixe de domaine et résolution d'alias ; les valeurs scalaires Cypher sont converties vers le type de colonne cible. Le corps de la réponse contient un compteur `affected_rows`.

La propriété d'un nœud est le nom publié de sa colonne : l'alias de la colonne quand elle en a un, sinon le nom selon la convention de nommage — le nom sous lequel GraphQL et SQL la lisent — dans `MATCH`, `RETURN`, `CREATE`, `SET` et `MERGE` de la même façon.

Règles :

- L'étiquette doit correspondre à exactement une table enregistrée. Les étiquettes ambiguës ou inconnues provoquent des erreurs bloquantes ; aucune correspondance approximative. Aucune nouvelle étiquette ni aucun nouveau type ne peut être créé via Cypher.
- Chaque écriture est conditionnée par les autorisations d'écriture de la table cible : le rôle a besoin du droit `write` et d'une autorisation pour l'opération, et les colonnes écrites doivent figurer dans l'autorisation. Un rôle qui ne les a pas est rejeté au moment de la compilation.
- Le connecteur de la source sous-jacente doit prendre en charge le DML. Les sources en lecture seule (fédérées via Trino, Iceberg sans connecteur Delta) rejettent les écritures au moment de la traduction.
- Les relations ne peuvent pas être écrites : elles sont dérivées des jointures déclarées dans le modèle, et non stockées comme des arêtes. Cibler une relation constitue une erreur bloquante. Une arête adossée à une jonction ne fait pas exception : la table associative qui la porte est elle-même une table enregistrée, et les lignes sont écrites dans cette table, non dans l'arête.
- Les écritures traversent l'intégralité du pipeline d'écriture : injection RLS et hooks post-mutation (invalidation du cache de réponse, marquage des vues matérialisées comme obsolètes, événements de changement Kafka, rechargement des tables actives).
- `MERGE`, `DETACH DELETE` et `REMOVE` ne sont pas pris en charge et sont rejetés au moment de l'analyse.

---

## Accès par protocole {: #protocol-access }

Cypher atteint le même pipeline gouverné via deux transports :

- **HTTP** — `POST /data/cypher` avec un corps JSON (`{"query": "...", "params": {...}}`). Renvoie des lignes typées, ou `affected_rows` pour les écritures. Les variables de graphe de la clause `RETURN` sont sérialisées en JSON : les nœuds portent `id`, `label`, `tableLabel` et `properties` ; les arêtes portent `identity`, `start`, `end`, `type`, `properties`, `startNode` et `endNode` ; les chemins portent `nodes`, `edges` et `length`/`hops`. Les commandes enregistrées sont également appelables ici via `CALL fn(args) YIELD col1, col2`, et les arguments positionnels correspondent aux noms d'arguments déclarés de la commande, dans l'ordre.
- **Bolt** — un serveur de protocole binaire compatible Neo4j (codec PackStream, découpage en trames) qui permet à Neo4j Browser, Bloom et aux pilotes Bolt d'exécuter des requêtes Cypher sur le graphe fédéré. Il démarre lorsque `PROVISA_BOLT_PORT` est défini avec une valeur non nulle et est désactivé par défaut ; définissez `PROVISA_BOLT_CERT` / `PROVISA_BOLT_KEY` pour le TLS. L'authentification Bolt fait correspondre le principal à un utilisateur et la base de données à un rôle : `SHOW DATABASES` liste une entrée par paire (vue × rôle), nommée `provisa_<role>` (domaines métier) ou `provisa_ops_<role>` (avec les domaines system/meta/ops) ; `:use` sélectionne le rôle et la vue actifs. Les relations reçoivent des ID entiers durables via une table `rel_ids`, sur le modèle de `node_ids`. Les commandes enregistrées sont appelables avec `CALL command(args)`, et les arguments positionnels correspondent aux noms d'arguments déclarés, dans l'ordre ; les procédures `CALL dbms.*` / `CALL db.*` sont prioritaires.

### Analytique de graphe {: #graph-analytics }

`POST /data/graph-analytics` exécute une requête Cypher, construit un graphe NetworkX en mémoire à partir des nœuds et arêtes obtenus, exécute un algorithme nommé, puis fusionne un dictionnaire `_analytics` dans chaque nœud et arête avant de les renvoyer en JSON avec un champ `elapsed_ms`. Les clés de `_analytics` varient selon l'algorithme : la centralité produit `score` ; la détection de communautés produit `cluster` ; le k-core produit `core_number` ; la centralité de degré ajoute `in_degree` et `out_degree`. Le endpoint rejette les graphes dépassant une taille configurable (10 000 nœuds / 50 000 arêtes par défaut) avec un code HTTP 413 ; Girvan-Newman est plafonné à 500 nœuds sauf si l'appelant transmet `force=true`.

---

## Limitations

### Contraintes de conception {: #design-constraints }

1. **Les écritures se limitent à `CREATE`, `SET` et `DELETE`.** Elles s'exécutent comme des écritures directes en table via le même pipeline que les mutations GraphQL et SQL. Voir §Écritures ci-dessus. `MERGE`, `DETACH DELETE` et `REMOVE` sont rejetés au moment de l'analyse. Les procédures APOC sont également rejetées.

2. **Les propriétés de relation n'existent que sur les arêtes adossées à une jonction.** Une arête déclarée sur une paire de colonnes de clé étrangère n'existe que comme métadonnée de jointure dans le modèle et ne stocke aucun attribut ; `WHERE r.since > 2020` ou `RETURN r.weight` n'a donc aucun sens sur elle. Une arête déclarée sur une table de jonction en porte. Les colonnes restantes de la table associative sont les propriétés de la relation, et `RETURN r` les renvoie. Un `WHERE` sur l'une d'elles est compilé en prédicat sur l'alias de la jonction ; il restreint donc le parcours lui-même au lieu de filtrer des lignes déjà assemblées. La table de jonction est retirée du côté nœuds du schéma du graphe : ici c'est une arête, partout ailleurs une table.

3. **Le parcours bidirectionnel** `(a)-[]-(b)` est réécrit en UNION ALL avant et arrière de chaque relation orientée correspondante du modèle. Chaque relation du modèle a une direction, et la syntaxe bidirectionnelle est un sucre qui se développe dans les deux sens. Les branches supplémentaires sont émises au niveau le plus externe de la requête. Un motif MATCH ultérieur de la même requête n'est pas dupliqué entre les branches, ce qui limite les motifs bidirectionnels dans une requête à plusieurs clauses MATCH.

4. **Les chemins récursifs nécessitent une borne.** Les modèles de longueur variable (`[*]`) doivent inclure une borne supérieure (par exemple `[*..10]`). Le parcours non borné est rejeté au moment de l'analyse afin d'éviter des CTE récursives incontrôlées.

### Notes de comportement {: #behaviour-notes }

5. **`shortestPath` sur des chemins non autoréférentiels utilise un JOIN simple, sans tri par nombre de sauts.** Quand les types de départ et d'arrivée diffèrent et que le schéma n'a aucune relation autoréférentielle, le traducteur émet une chaîne de JOIN simple le long du plus court chemin du schéma. Ce chemin de code ne suit pas les sauts, il n'émet donc pas d'`ORDER BY hops`. Le résultat est le chemin de schéma structurellement le plus court. Il peut différer du plus court chemin à travers les données sur plusieurs lignes.

6. **Plusieurs chemins de schéma produisent un `UNION ALL`.** Lorsque deux chemins de schéma comportant le même nombre de sauts relient les mêmes types de départ et d'arrivée (par exemple `Person -[WORKS_AT]-> Company` et `Person -[MANAGES]-> Company`), les deux sont émis sous forme de branches `UNION ALL`. La déduplication des lignes apparaissant dans les deux branches n'est pas effectuée.

7. **Un seul `RelationshipMapping` par combinaison de paire source→cible et de rel\_type.** Si deux champs GraphQL du même type source produisent la même chaîne `rel_type` (après mise en majuscules) vers le même type cible, le second enregistrement écrase le premier dans `CypherLabelMap.relationships`. La clé de la relation inclut les noms des types source et cible ; des paires source/cible distinctes portant le même nom de type obtiennent donc chacune leur propre entrée et ne sont pas affectées.

8. **Les CTE de la clause `WITH` sont nommées `_w0`, `_w1`, …** Les noms sont attribués de manière positionnelle au sein d'un seul appel de traduction. La composition de plusieurs requêtes traduites (par exemple dans un lot) peut produire des collisions de noms de CTE si elles sont concaténées sans précaution.

### Couverture des expressions et des modèles {: #expression-and-pattern-coverage }

Les expressions Cypher sont analysées en un AST puis réduites nœud par nœud en SQL (`provisa/cypher/expr_parser.py`, `provisa/gql/expr_visitor.py`). La grammaire suit la tour de précédence `oC_Expression` d'openCypher. Pris en charge : littéraux, paramètres, accès aux propriétés, `n.prop`, index et découpe (slice), arithmétique (`+ - * / % ^`), comparaison, `IN`, `STARTS WITH` / `ENDS WITH` / `CONTAINS` / `=~`, `IS [NOT] NULL`, booléens `AND` / `OR` / `XOR` / `NOT`, `CASE`, littéraux de liste et de carte, compréhensions de liste et de modèle (y compris la liaison de chemin `p = (…)`), projection de carte, `reduce`, les quantificateurs `all` / `any` / `none` / `single`, les sous-requêtes existentielles et les appels de fonction.

9. **Les étiquettes sont fixes ; il est impossible de créer des types d'objet via Cypher.** Une étiquette se résout en un domaine connu, un type d'objet connu ou un `domain:object_type` qualifié, l'ensemble fermé que définit le schéma enregistré. Cypher n'introduit jamais d'étiquette ni de type nouveau. Vous ne pouvez créer des instances que de types déjà définis dans une source de données accessible en écriture : `CREATE` écrit des lignes dans une telle table (voir §Writes) et ne peut pas définir de nouvelle étiquette ni de nouveau type. Les deux formes d'étiquette sont acceptées et expriment le même test, la forme postfixe `n:Label` et la forme longue `n IS :Label` (avec sa négation `n IS NOT :Label`). Une étiquette qualifiée s'écrit `n:domain:object_type`.

10. **`shortestPath` et `allShortestPaths` ne fonctionnent qu'à l'intérieur de `MATCH`.** Dans un motif (`MATCH p = shortestPath((a:Person)-[:KNOWS*..5]->(b:Person))`), ils se traduisent en CTE `WITH RECURSIVE` et exigent des nœuds source et cible étiquetés. Ils ne sont pas pris en charge en position d'expression, par exemple `RETURN shortestPath((a)-[*]->(b))` ou `WHERE length(shortestPath((a)-[*]->(b))) < 5`. La réécriture récursive part de la clause `MATCH`, et aucune sous-requête corrélée ne peut la porter.

11. **Les compréhensions de liste, `REDUCE` et les quantificateurs opèrent sur des valeurs de liste ; les compréhensions de modèle parcourent le graphe.** `reduce(...)`, `all/any/none/single(...)` et la compréhension de liste `[x IN list | …]` travaillent sur une expression de liste et se ramènent aux fonctions de liste d'ordre supérieur du moteur. Elles ne parcourent pas le graphe elles-mêmes. La compréhension de **modèle** `[(a)-[:R]->(b) WHERE p | e]` parcourt : son motif de graphe est traité comme une sous-requête corrélée, sa source est donc un parcours. Pour alimenter les formes de liste avec des résultats de parcours, utilisez `nodes(p)` / `relationships(p)` / `collect(...)`, ou directement une compréhension de modèle.
