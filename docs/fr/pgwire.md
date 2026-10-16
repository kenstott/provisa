# Serveur pgwire de Provisa {: #provisa-pgwire-server }

Provisa expose un endpoint du protocole filaire de PostgreSQL (pgwire). Tout outil qui parle le protocole client de PostgreSQL — psycopg2, asyncpg, DBeaver, Tableau, JDBC — peut se connecter et interroger les données de Provisa via le même pipeline de gouvernance que celui qui gouverne l'API HTTP. (REQ-266)

Les requêtes traversent l'ensemble du pipeline de gouvernance : application de la sécurité au niveau des lignes, règles de masquage, garde-fous de relation, vérifications d'accès au domaine. (REQ-001, REQ-002, REQ-263) L'interface pgwire n'est pas un moyen de contournement. (REQ-002, REQ-266)

---

## Détails de connexion {: #connection-details }

Le serveur démarre lorsque `PROVISA_PGWIRE_PORT` est défini avec un entier non nul. Il est désactivé par défaut. (REQ-527) [tool-verified: `app.py:1739`]

```yaml
Host: 0.0.0.0  (all interfaces)
Port: $PROVISA_PGWIRE_PORT
```

**TLS.** Définissez `PROVISA_PGWIRE_CERT` et `PROVISA_PGWIRE_KEY` avec les chemins d'un certificat et d'une clé PEM. Lorsque les deux sont présents, le serveur enveloppe les connexions entrantes dans TLS. En leur absence, TLS est désactivé et le serveur répond `N` aux demandes de négociation SSL. (REQ-530) [tool-verified: `server.py:1746-1750`]

**Version de serveur signalée.** Les clients voient `14.0.provisa`. Les outils qui conditionnent des fonctionnalités au numéro de version peuvent se comporter comme s'ils étaient connectés à PostgreSQL 14. (REQ-579) [tool-verified: `server.py:208`]

---

## Authentification {: #authentication }

Le paquet de démarrage transporte un nom d'utilisateur et un unique champ secret, sans aucun schéma indiquant ce qu'est ce secret. Provisa tranche à partir du secret lui-même : un client n'a donc besoin d'aucune configuration au-delà de `user` et `password` :

| Le secret est | Reconnu à | Se résout en |
| --------------- | --------------- | ------------- |
| Un jeton d'accès personnel | son préfixe `provisa_pat_` | le propriétaire et le rôle du jeton (REQ-1263) |
| Un jeton bearer OIDC / du fournisseur | au fait que le fournisseur configuré est un fournisseur de jetons | l'identité affirmée par le jeton (REQ-890) |
| Un mot de passe | tout le reste | le compte dans le fournisseur configuré (`basic` ou `simple`) |

La décision est prise une seule fois. Une credential refusée par le validateur retenu n'est pas réessayée auprès d'un autre : un refus ne devient donc pas une seconde tentative.

Le mode trust (`provider: none`, ou middleware d'authentification inactif) fait exception : le nom d'utilisateur sert directement de `role_id` et le secret est ignoré. Ne l'utilisez pas sur une connexion non chiffrée.

**SCRAM-SHA-256.** Avec `provider: basic` et `auth.scram: true`, le serveur annonce SASL (code d'authentification 10) avec `SCRAM-SHA-256`, et le mot de passe est prouvé plutôt qu'envoyé. (REQ-1394) `SCRAM-SHA-256-PLUS` n'est pas proposé. Un utilisateur dont le vérificateur n'a pas encore été écrit — les vérificateurs ne peuvent pas être dérivés d'empreintes bcrypt — reçoit un échange factice, afin que le réseau ne révèle pas qui a migré ; cet utilisateur s'authentifie par mot de passe en clair sur TLS jusqu'à ce que sa prochaine saisie de mot de passe en écrive un. Avec `auth.scram` désactivé, le serveur utilise le type d'authentification PG 3 (mot de passe en texte clair). MD5 n'est pris en charge dans aucun des deux cas.

**Certificats client.** Définissez `PROVISA_MTLS_CLIENT_CA` et le serveur vérifie un certificat client pendant le handshake, avant d'examiner la moindre credential. (REQ-1228) Avec `PROVISA_MTLS_BIND_PRINCIPAL`, le common name du certificat doit être égal au `user` sous lequel la connexion s'authentifie ensuite. Voir [Configuration](configuration.md#mutual-tls).

**Les échecs sont comptés.** Cinq échecs en cinq minutes verrouillent le compte pendant quinze minutes, et le compteur est partagé avec HTTP et Bolt : un verrouillage obtenu sur une surface vaut sur toutes. (REQ-1393)

**Choisir une organisation.** Sur un déploiement multi-organisations, connectez-vous à `<org>.<your-domain>` et pgwire lit l'organisation depuis le nom d'hôte du ClientHello TLS, comme HTTP la lit depuis l'en-tête `Host`. (REQ-1234) Le nom d'hôte demande une organisation ; il ne l'accorde pas, et un principal qui n'y est pas membre est refusé. Une connexion par adresse IP ne demande aucune organisation.

---

## Ce qui fonctionne {: #what-works }

### SELECT

Toutes les instructions SELECT traversent le pipeline de gouvernance (`_pipeline.py`). (REQ-001, REQ-262, REQ-266) Le pipeline :

1. Réécrit le SQL sémantique en SQL physique (`rewrite_semantic_to_physical`)
2. Applique la gouvernance (sécurité au niveau des lignes, masquage, accès au domaine) (REQ-263)
3. Valide par rapport au schéma enregistré (REQ-011)
4. Route vers Trino ou vers le pool direct de la source (REQ-027, REQ-028)

Les requêtes simples multi-instructions sont prises en charge. Les instructions séparées par des points-virgules sont découpées et exécutées dans l'ordre. (REQ-580) [tool-verified: `server.py:318-381`]

Les requêtes paramétrées (`$1`, `$2`, ...) sont prises en charge à la fois en mode requête simple et en mode requête étendue (Bind/Execute). Les paramètres sont substitués sous forme de littéraux avant l'exécution. (REQ-581) [tool-verified: `server.py:78-85`]

`SELECT * FROM fn(args)` et `SELECT fn(args)` — où `fn` désigne une fonction suivie et enregistrée — sont interceptés avant le pipeline de gouvernance et routés via l'unique exécuteur gouverné (`invoke_tracked_function`). Le résultat est un ensemble de lignes typé identique à celui que renvoie toute autre surface pour cette commande. `writable_by` et les règles de gouvernance sont appliqués à l'intérieur de l'exécuteur. (REQ-1156) [tool-verified: `provisa/pgwire/function_call.py:74-88`]

### Curseurs (DECLARE / FETCH / MOVE / CLOSE) {: #cursors-declare-fetch-move-close }

Provisa gère la syntaxe textuelle des curseurs SQL sur le protocole de requête simple. (REQ-1862) Les clients qui envoient `DECLARE`/`FETCH`/`MOVE`/`CLOSE` comme de simples instructions SQL obtiennent un vrai streaming côté serveur plutôt qu'une erreur de protocole ou un tampon contenant le résultat complet chargé dans la mémoire du client.

**C'est distinct du streaming par portails.** Le protocole de requête étendue — Bind/Execute avec un champ de limite de lignes — est un mécanisme différent du protocole filaire. asyncpg et la plupart des pilotes PostgreSQL modernes l'utilisent automatiquement, sans SQL de curseur. Si votre pilote utilise déjà des portails, vous n'avez pas besoin de cette syntaxe. `DECLARE CURSOR` sert aux clients qui envoient littéralement du SQL de curseur : les curseurs nommés/côté serveur de psycopg2 (`conn.cursor(name="...")`), certains outils BI et quelques anciens pilotes JDBC.

**DECLARE**

```sql
DECLARE c1 CURSOR FOR SELECT id, amount FROM orders WHERE region = 'us-west';
DECLARE c2 SCROLL CURSOR FOR SELECT * FROM events ORDER BY ts;
DECLARE "CaseSensitive" NO SCROLL CURSOR FOR SELECT * FROM large_table;
```
Le SELECT interne passe par le pipeline gouverné normal de Provisa — la même application de la RLS, le même masquage et les mêmes contrôles d'accès aux domaines que pour tout SELECT autonome. [tool-verified: `server.py:1367` — `ctx.execute_sql(inner_sql)`, the same call path as a plain SELECT] Déclarer un curseur ne contourne pas la gouvernance.

Le mot-clé `SCROLL` contrôle à la fois la navigation autorisée et la mémoire qu'utilise le curseur :

| Déclaration | Navigation arrière | Coût mémoire |
|---|---|---|
| `DECLARE c CURSOR FOR ...` (par défaut) | Non autorisée | Limité à un lot de FETCH |
| `DECLARE c NO SCROLL CURSOR FOR ...` | Non autorisée | Limité à un lot de FETCH |
| `DECLARE c SCROLL CURSOR FOR ...` | Autorisée | Résultat complet mis en tampon dans la mémoire Python |

Lorsque ni `SCROLL` ni `NO SCROLL` n'est écrit, la valeur par défaut est `NO SCROLL` — comme la valeur par défaut de PostgreSQL. [tool-verified: `server.py:1361-1362`] Un curseur `NO SCROLL` retire de son tampon interne les lignes consommées après chaque pas en avant, de sorte qu'un parcours `FETCH FORWARD` d'un grand résultat ne garde jamais plus d'environ un lot en mémoire Python à la fois. [tool-verified: `server.py:443-447`] Un curseur `SCROLL` conserve chaque ligne qu'il a jamais récupérée — c'est le prix de la prise en charge de `FETCH BACKWARD`, `ABSOLUTE`, `FIRST` et `LAST`. Avant de déclarer un curseur `SCROLL` sur un grand résultat, prévoyez la taille complète du résultat en tas Python. [tool-verified: `server.py:383-389`]

Redéclarer un nom de curseur déjà ouvert ferme d'abord le curseur précédent. [tool-verified: `server.py:1369-1371`] `WITH HOLD` et `WITHOUT HOLD` sont acceptés dans la syntaxe ; voir la durée de vie du curseur ci-dessous.

**FETCH et MOVE**

```sql
FETCH 100 FROM c1            -- next 100 rows
FETCH FORWARD 100 FROM c1    -- same
FETCH ALL FROM c1            -- all remaining rows
FETCH BACKWARD 10 FROM c1    -- previous 10 rows (SCROLL cursors only)
FETCH ABSOLUTE 50 FROM c1    -- the row at absolute position 50 (SCROLL cursors only)
FETCH RELATIVE -5 FROM c1    -- 5 positions back (SCROLL cursors only)
FETCH FIRST FROM c1          -- row 1 (SCROLL cursors only)
FETCH LAST FROM c1           -- the final row (SCROLL cursors only)
MOVE 100 FROM c1             -- advance position without returning rows
```
`FETCH` renvoie des lignes et envoie l'étiquette de commande `FETCH <n>`, où `n` est le nombre réellement renvoyé — qui peut être inférieur à celui demandé quand le curseur atteint la fin. `MOVE` avance la position sans renvoyer de lignes et envoie `MOVE <n>`. [tool-verified: `server.py:1417`, `server.py:1419`]

`FROM` et `IN` sont interchangeables pour introduire le nom du curseur. Les noms de curseur sans guillemets sont mis en minuscules ; les noms entre guillemets (`"CaseSensitive"`) conservent leur casse. [tool-verified: `test_cursor.py:74-79`]

La navigation arrière (`BACKWARD`, `ABSOLUTE`, `FIRST`, `LAST`, `RELATIVE` négatif) sur un curseur `NO SCROLL` renvoie SQLSTATE 55000 (`object_not_in_prerequisite_state`), comme le refus de PostgreSQL lui-même. [tool-verified: `server.py:1402-1406`] Un FETCH ou MOVE nommant un curseur jamais déclaré renvoie SQLSTATE 34000 (`invalid_cursor_name`). [tool-verified: `server.py:1398-1400`]

**CLOSE**

```sql
CLOSE c1;    -- release one cursor and free its result stream
CLOSE ALL;   -- release every open cursor on this connection
```
`CLOSE` libère le flux de résultats sous-jacent et retire le curseur de la session. [tool-verified: `server.py:1421-1436`] Se déconnecter sans émettre `CLOSE` est sûr : `ProvisaSession.close()` libère automatiquement tous les curseurs encore ouverts à la déconnexion. [tool-verified: `test_cursor.py:335-346`]

**Durée de vie du curseur.** Le serveur pgwire de Provisa n'a pas de machine à états de transaction (`ProvisaSession.in_transaction()` est figé à `False`). Chaque curseur se comporte donc comme s'il était déclaré `WITH HOLD` : il vit pendant toute la connexion, pas jusqu'à `COMMIT`. [tool-verified: `server.py:200` — `in_transaction()` always returns `False`] Les mots-clés `WITH HOLD` et `WITHOUT HOLD` sont acceptés et ignorés sans avertissement — il n'y a pas de cycle de vie des curseurs par transaction à appliquer.

### Définitions (CREATE, ALTER, DROP, TRUNCATE)

Rien n'est défini par pgwire. Une instruction qui crée, modifie ou supprime un objet (`CREATE TABLE`, `CREATE TABLE … AS SELECT`, `CREATE VIEW`, `ALTER`, `DROP`, un index, une séquence ou un schéma) est refusée avec SQLSTATE 0A000, quoi que détienne le rôle ; l'exception est une table temporaire propre à la session (voir [Tables temporaires](#temporary-tables)). Une table ou une vue est un objet du modèle : créez-la dans le modèle (pages d'administration, API d'administration ou configuration), ou créez-la dans la source de données et admettez-la dans le modèle. `TRUNCATE` est refusé de la même façon ; `DELETE` est le moyen gouverné de retirer des lignes. [tool-verified: `provisa/compiler/definitions.py`]

Le refus est fait une fois, dans le pipeline que traverse chaque surface SQL, de sorte que SQL sur HTTP, Flight et MCP refusent les mêmes instructions avec le même message.

### Tables temporaires {: #temporary-tables }

Une session peut créer, écrire, lire et supprimer ses propres tables temporaires :

```sql
CREATE TEMP TABLE busy AS SELECT customer_id, COUNT(*) AS n FROM sales.orders GROUP BY customer_id;
INSERT INTO busy VALUES (0, 0);
UPDATE busy SET n = n + 1 WHERE customer_id = 0;
DELETE FROM busy WHERE n < 5;
SELECT c.name, b.n FROM sales.customers c JOIN busy b ON b.customer_id = c.id;
DROP TABLE busy;
```
- **Créer** avec `CREATE TEMP TABLE name (column type, ...)` ou `CREATE TEMP TABLE name AS SELECT ...`. Le nom ne prend pas de schéma. Un `SELECT` à partir duquel elle est créée est une lecture comme une autre : les règles de lignes, les masques, les données factices et la limite de lignes du rôle s'appliquent aux lignes qu'il prend.
- **Types de colonnes** : `smallint`, `integer`, `bigint`, `real`, `double precision`, `boolean`, `date`, `timestamp`, `text`, `varchar` et `DECIMAL(precision, scale)`. Un décimal conserve sa précision et son échelle. Tout autre type est refusé, en nommant la colonne et le type : une valeur n'est jamais stockée sous un autre type que le sien. Une colonne créée à partir d'un `SELECT` prend le type de ses valeurs ; une colonne sans valeur permettant d'en déduire le type est refusée, appliquez donc un `CAST` dans le `SELECT`.
- **Limite de lignes.** Les lignes que prend un `CREATE ... AS SELECT` ou un `INSERT ... SELECT` sont lues comme le rôle lit toute chose, donc la limite de lignes du rôle s'applique. Une table temporaire remplie à partir d'une lecture que la limite a tronquée porte le même avertissement d'instruction que toute lecture tronquée (`statement.rows_cut`), avec la limite.
- **Écrire** avec `INSERT ... VALUES`, `INSERT ... SELECT`, `UPDATE` et `DELETE`, sans `RETURNING` et sans `FROM`. Une écriture dans une table temporaire n'est pas une mutation : elle n'atteint aucune source ni aucune donnée de l'environnement, elle fonctionne donc quel que soit le traitement des mutations de l'environnement et n'a besoin d'aucun droit d'écriture.
- **Lire** par son nom, seule ou jointe à des tables du modèle. Le moteur calcule la lecture.
- **Durée de vie.** Une table temporaire n'est pas un objet du modèle, aucune autre session ne la voit, et elle se termine avec la session. Sur pgwire, une session est la connexion. Sur une surface sans connexion propre (SQL sur HTTP, Flight, MCP), une session est une requête, donc une table temporaire vit pour les instructions envoyées ensemble dans cette requête.
- **Stockage.** Les lignes sont conservées dans un schéma propre à la session dans le magasin du moteur et supprimées à la fin de la session. Un `INSERT`, `UPDATE` ou `DELETE` réécrit toute la table via le moteur, donc une table temporaire convient aux données de travail, pas aux grandes tables écrites ligne par ligne. Un processus qui s'arrête sans terminer ses sessions laisse leurs schémas dans le magasin ; ils sont supprimés lorsque l'environnement est retiré.

Les mêmes instructions fonctionnent sur chaque surface SQL : elles sont traitées dans le pipeline que toutes partagent. [inferred: verified by `tests/integration/test_session_temp_tables.py` over HTTP; the pgwire connection-long session is not yet covered by a test]

### Écritures de données (INSERT, UPDATE, DELETE, MERGE, COPY FROM STDIN) {: #data-writes-insert-update-delete-merge-copy-from-stdin }

Les lignes d'une table enregistrée existante s'écrivent par pgwire comme sur toute autre surface SQL, via l'unique admission des écritures (`provisa/compiler/write_admission.py`) :

1. **Le droit.** Le rôle détient `write`. (REQ-868)
2. **Les colonnes.** Chaque colonne qu'écrit l'instruction nomme le rôle dans son `writable_by` ; une colonne qui n'en déclare aucun n'est inscriptible par personne. (REQ-663)
3. **Les lignes qu'il peut toucher.** Le filtre de lignes du rôle, le même prédicat que portent ses lectures, est ajouté au `WHERE` d'un `UPDATE` ou d'un `DELETE`.
4. **Les lignes qu'il laisse.** Un rôle filtré ne peut écrire que des lignes qu'il pourrait lire ensuite : les lignes d'un `INSERT`, et les nouvelles valeurs d'un `UPDATE` qui fixe une colonne lue par le filtre, sont vérifiées contre le filtre avant que quoi que ce soit ne soit envoyé à la source.

Ce qui ne peut pas être décidé est refusé nommément plutôt qu'admis dans le doute (un `INSERT … SELECT`, une valeur qui n'est ni un littéral ni un paramètre lié, un filtre qui lit une colonne que l'instruction ne fournit pas). [tool-verified: `provisa/compiler/write_admission.py`, `provisa/pgwire/copy_handler.py`]

### COPY

`COPY ... TO STDOUT` et `COPY ... FROM STDIN` sont tous deux pris en charge. (REQ-585)

**COPY TO STDOUT** — exporte les résultats de requête au format filaire COPY de PG. Deux formes fonctionnent :

```sql
-- Table reference
COPY my_table TO STDOUT WITH (FORMAT csv)

-- Arbitrary query
COPY (SELECT col1, col2 FROM my_table WHERE ...) TO STDOUT WITH (FORMAT text)
```
Formats pris en charge : `text` (délimité par des tabulations, par défaut) et `csv`. Le format binaire n'est pas pris en charge en sortie de COPY. [tool-verified: `copy_handler.py:36-52`]

**COPY FROM STDIN** est une écriture de données dans une table enregistrée existante : elle est admise par la même admission des écritures qu'un `INSERT` des mêmes lignes (voir [Écritures de données](#data-writes-insert-update-delete-merge-copy-from-stdin)).

### Transactions et commandes de session {: #transactions-and-session-commands }

SET, BEGIN, COMMIT, ROLLBACK, SAVEPOINT, RELEASE, DISCARD, RESET et DEALLOCATE sont interceptés et renvoient une réponse de succès vide. (REQ-587) Le serveur est sans état vis-à-vis des transactions — il n'y a ni isolation transactionnelle ni prise en charge de l'annulation. (REQ-587) [tool-verified: `catalog.py:27-31`, `catalog.py:1129-1132`]

---

## Interception de catalogue {: #catalog-intercept }

Les requêtes sur `information_schema` et `pg_catalog` sont traitées localement sans aller-retour vers Trino. (REQ-532) La couche d'interception construit une base de données DuckDB en mémoire par requête, alimentée à partir du contexte de compilation du rôle. (REQ-532) [tool-verified: `catalog.py:210-213`]

Tables interceptées :

**information_schema :** `schemata`, `tables`, `columns`, `views`, `table_constraints`, `key_column_usage`, `referential_constraints`

**pg_catalog :** `pg_namespace`, `pg_class`, `pg_attribute`, `pg_type`, `pg_attrdef`, `pg_description`, `pg_index`, `pg_constraint`, `pg_proc`, `pg_roles`, `pg_auth_members`, `pg_database`, `pg_settings`, `pg_tables`, `pg_stat_user_tables`, `pg_statio_user_tables`, `pg_am`, `pg_extension`, `pg_enum`, `pg_stat_activity`

[tool-verified: `catalog.py:39-67`]

`pg_constraint` est alimentée avec des données réelles de clé primaire et de clé étrangère dérivées des champs `pk_columns` et `joins` du modèle de domaine. (REQ-392, REQ-399) Les outils de BI qui inspectent les relations de clé étrangère (Tableau, DBeaver, etc.) verront le graphe de jointures que Provisa connaît. [tool-verified: `catalog.py:551-632`] Les jointures à colonne unique entre la même paire source/cible dont les colonnes cibles forment ensemble la clé primaire composite de la cible sont regroupées en une seule ligne de clé étrangère avec des tableaux `conkey`/`confkey` à plusieurs éléments. (REQ-1094) [tool-verified: `catalog_constraints.py`]

Une relation adossée à une jonction (REQ-1586) ne produit aucune ligne FK. C'est une arête passant par une table associative, non une paire de colonnes, et `pg_constraint` n'a pas de forme pour deux sauts — le modèle de domaine la laisse donc hors de `joins`, et la table de jonction apparaît comme une table ordinaire dotée de ses propres clés étrangères vers chaque extrémité. Les clients SQL l'atteignent en joignant cette table ; les clients Cypher la traversent comme une relation unique. [tool-verified: `provisa/compiler/schema_gen.py:302-306`]

`pg_index` est alimentée avec une ligne par contrainte de clé primaire et UNIQUE (`indrelid` = oid de la table, `indkey` = attnums de clé ordonnés, `indisprimary`/`indisunique` définis). Les clients qui résolvent les colonnes clés via `pg_index.indkey` plutôt que via `pg_constraint` — DataGrip, par exemple — découvrent les bonnes colonnes via la jointure standard `pg_index` → `pg_attribute`. (REQ-1095) [tool-verified: `catalog_constraints.py:340-384`]

Les expressions scalaires suivantes sont également interceptées : (REQ-588)

- `current_user`, `session_user` → le `role_id` authentifié
- `current_database()` → `"provisa"`
- `current_schema()` → `"public"`
- `version()` → `"PostgreSQL 14.0 on Provisa"`
- `pg_backend_pid()` → `0`
- `current_setting(...)` → renvoie une valeur d'une table de paramètres fixe
- `SHOW <setting>` → renvoie une valeur de la même table de paramètres

[tool-verified: `catalog.py:168-207`, `catalog.py:1076-1120`]

---

## Encodage binaire des paramètres {: #binary-parameter-encoding }

Le protocole de requête étendue (Bind/Execute) prend en charge les paramètres encodés en binaire. (REQ-589) Les OID de type suivants sont décodés depuis le binaire : [tool-verified: `postgres.py:69-97`]

| OID | Type PG | Type Python |
| ----- | --------- | ------------- |
| 16 | bool | bool |
| 17 | bytea | bytes |
| 20 | int8 | int |
| 21 | int2 | int |
| 23 | int4 | int |
| 25 | text | str |
| 700 | float4 | float |
| 701 | float8 | float |
| 1043 | varchar | str |
| 1082 | date | datetime.date |
| 1114 | timestamp | datetime.datetime |
| 1184 | timestamptz | datetime.datetime (UTC) |
| 1700 | numeric | decimal.Decimal |
| 2950 | uuid | str |

Tout OID absent de cette table déclenche `"Unsupported binary parameter type: <oid>"`. (REQ-589) [tool-verified: `postgres.py:579`]

Les colonnes de résultat sont également envoyées en binaire lorsque le client le demande, pour le même ensemble de types plus ARRAY, JSON, INTERVAL et BIGINT. (REQ-589) [tool-verified: `postgres.py:191-244`]

---

## Recommandations de pilote {: #driver-recommendations }

**Pilotes Python natifs (psycopg2, asyncpg).** Ceux-ci négocient le protocole de requête étendue par défaut et utilisent l'encodage binaire pour la plupart des types. La fidélité des types est maximale ici — les colonnes `NUMERIC` arrivent en tant que `Decimal`, `TIMESTAMP` en tant que `datetime`, et ainsi de suite. Utilisez-les pour l'ETL basé sur Python, les scripts ou l'intégration directe.

**JDBC (pilote JDBC PostgreSQL).** Utilisez-le pour les outils de l'écosystème Java : DBeaver, Tableau, Power BI, Metabase, opérateurs JDBC d'Airflow. JDBC utilise par défaut le protocole de requête simple, ce qui évite les complications liées à l'encodage binaire. Chaîne de connexion :

```yaml
jdbc:postgresql://<host>:<PROVISA_PGWIRE_PORT>/provisa?user=<role_id>&password=<password>
```

Certains outils de BI basés sur JDBC envoient, à la connexion, une rafale de requêtes vers `information_schema` et `pg_catalog` pour alimenter leur explorateur de schéma. Toutes sont traitées par la couche d'interception de catalogue — aucun trafic vers Trino n'est généré lors de l'inspection du schéma. (REQ-532)

**Quand préférer l'un à l'autre.** Si le client est en Python, utilisez psycopg2 ou asyncpg pour une meilleure gestion des types. Si le client est un outil de BI ou toute application JVM, utilisez JDBC. Évitez de mélanger les attentes de protocole binaire et texte sur la même connexion si vous observez des anomalies de conversion de type — le comportement en mode texte de JDBC est plus simple à raisonner.

---

## Mises en garde et contraintes {: #caveats-and-constraints }

**SQL uniquement.** L'écouteur pgwire analyse et exécute uniquement du SQL — les chaînes GraphQL et Cypher ne sont pas acceptées. (REQ-614) Les écritures de données dans des tables existantes sont admises selon les droits du rôle, ses autorisations de colonnes et son filtre de lignes ; voir [Écritures de données](#data-writes-insert-update-delete-merge-copy-from-stdin).

**Aucune définition.** `CREATE`, `ALTER`, `DROP` et `TRUNCATE` sont refusés avec SQLSTATE 0A000 quoi que détienne le rôle, sauf une table temporaire propre à la session ; voir [Tables temporaires](#temporary-tables). (REQ-615, REQ-616)

**Pas de véritable prise en charge des transactions.** BEGIN/COMMIT/ROLLBACK sont acceptés et ignorés silencieusement. Chaque instruction s'exécute de manière indépendante. (REQ-587) [tool-verified: `server.py:146-158` — `in_transaction()` renvoie toujours `False`]

**Délai de requête.** Une instruction peut prendre le délai de requête propre à pgwire, `limits.request_timeouts.pgwire`, ou le délai commun `limits.request_timeout` lorsque le premier n'est pas défini ; les deux sont des réglages de l'opérateur. (REQ-590, REQ-1905) [tool-verified: `provisa/core/limits.py`, `server.py`]

**Le format de sortie de COPY est text ou csv.** Le format binaire COPY de PG (`FORMAT binary`) n'est pas implémenté. [inferred: seules les branches `text` et `csv` existent dans `_rows_to_copy_text` / `_rows_to_copy_csv`]

**La substitution de paramètres est littérale.** Les paramètres `$1`, `$2`, ... sont substitués sous forme de littéraux SQL avant l'exécution, et non envoyés comme paramètres liés au moteur sous-jacent. Cela signifie que le moteur sous-jacent ne voit jamais d'instruction préparée. Pour Trino, cela n'a aucun impact pratique ; pour les sources à pool direct, cela contourne la mise en cache des instructions préparées. (REQ-581) [tool-verified: `server.py:78-85`]

**DECLARE SCROLL met en tampon le résultat complet.** Un curseur `SCROLL` accumule dans le tas Python chaque ligne qu'il récupère du moteur afin de pouvoir servir `FETCH BACKWARD`, `ABSOLUTE`, `FIRST` et `LAST` à la demande. Sur un grand résultat, le résultat entier vit en mémoire tant que le curseur est ouvert. Utilisez la valeur par défaut `NO SCROLL` pour un streaming uniquement vers l'avant. (REQ-1862) [tool-verified: `server.py:383-389`]

**`pg_stat_activity`, `pg_stat_user_tables`, `pg_extension`, `pg_enum`, `pg_attrdef`, `pg_proc`.** Ces tables existent dans la couche de catalogue mais sont des stubs vides. Les outils de surveillance qui les interrogent recevront zéro ligne plutôt que des erreurs. (REQ-532) [tool-verified: `catalog.py:519-535`, `catalog.py:639-934`] (`pg_index` est alimentée — voir Interception de catalogue.)
