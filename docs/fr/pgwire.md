# Serveur pgwire de Provisa

Provisa expose un endpoint du protocole filaire de PostgreSQL (pgwire). Tout outil qui parle le protocole client de PostgreSQL — psycopg2, asyncpg, DBeaver, Tableau, JDBC — peut se connecter et interroger les données de Provisa via le même pipeline de gouvernance que celui qui gouverne l'API HTTP. (REQ-266)

Les requêtes traversent l'ensemble du pipeline de gouvernance : application de la sécurité au niveau des lignes, règles de masquage, garde-fous de relation, vérifications d'accès au domaine. (REQ-001, REQ-002, REQ-263) L'interface pgwire n'est pas un moyen de contournement. (REQ-002, REQ-266)

---

## Détails de connexion

Le serveur démarre lorsque `PROVISA_PGWIRE_PORT` est défini avec un entier non nul. Il est désactivé par défaut. (REQ-527) [tool-verified: `app.py:1739`]

```yaml
Host: 0.0.0.0  (all interfaces)
Port: $PROVISA_PGWIRE_PORT
```

**TLS.** Définissez `PROVISA_PGWIRE_CERT` et `PROVISA_PGWIRE_KEY` avec les chemins d'un certificat et d'une clé PEM. Lorsque les deux sont présents, le serveur enveloppe les connexions entrantes dans TLS. En leur absence, TLS est désactivé et le serveur répond `N` aux demandes de négociation SSL. (REQ-530) [tool-verified: `server.py:1746-1750`]

**Version de serveur signalée.** Les clients voient `14.0.provisa`. Les outils qui conditionnent des fonctionnalités au numéro de version peuvent se comporter comme s'ils étaient connectés à PostgreSQL 14. (REQ-579) [tool-verified: `server.py:208`]

---

## Authentification

Le paquet de démarrage transporte un nom d'utilisateur et un unique champ secret, sans aucun schéma indiquant ce qu'est ce secret. Provisa tranche à partir du secret lui-même : un client n'a donc besoin d'aucune configuration au-delà de `user` et `password` :

| Le secret est | Reconnu à | Se résout en |
| --------------- | --------------- | ------------- |
| Un jeton d'accès personnel | son préfixe `provisa_pat_` | le propriétaire et le rôle du jeton (REQ-1263) |
| Un jeton bearer OIDC / du fournisseur | au fait que le fournisseur configuré est un fournisseur de jetons | l'identité affirmée par le jeton (REQ-890) |
| Un mot de passe | tout le reste | le compte dans le fournisseur configuré (`basic` ou `simple`) |

La décision est prise une seule fois. Une credential refusée par le validateur retenu n'est pas réessayée auprès d'un autre : un refus ne devient donc pas une seconde tentative.

Le mode trust (`provider: none`, ou middleware d'authentification inactif) fait exception : le nom d'utilisateur sert directement de `role_id` et le secret est ignoré. Ne l'utilisez pas sur une connexion non chiffrée.

**SCRAM-SHA-256.** Avec `provider: basic` et `auth.scram: true`, le serveur annonce SASL (code d'authentification 10) avec `SCRAM-SHA-256`, et le mot de passe est prouvé plutôt qu'envoyé. (REQ-1394) `SCRAM-SHA-256-PLUS` n'est pas proposé. Un utilisateur dont le vérificateur n'a pas encore été écrit — les vérificateurs ne peuvent pas être dérivés d'empreintes bcrypt — reçoit un échange factice, afin que le réseau ne révèle pas qui a migré ; cet utilisateur s'authentifie par mot de passe en clair sur TLS jusqu'à ce que sa prochaine saisie de mot de passe en écrive un. Avec `auth.scram` désactivé, le serveur utilise le type d'authentification PG 3 (mot de passe en texte clair). MD5 n'est pris en charge dans aucun des deux cas.

**Certificats client.** Définissez `PROVISA_MTLS_CLIENT_CA` et le serveur vérifie un certificat client pendant le handshake, avant d'examiner la moindre credential. (REQ-1228) Avec `PROVISA_MTLS_BIND_PRINCIPAL`, le common name du certificat doit être égal au `user` sous lequel la connexion s'authentifie ensuite. Voir [Configuration](configuration.md#tls-mutuel).

**Les échecs sont comptés.** Cinq échecs en cinq minutes verrouillent le compte pendant quinze minutes, et le compteur est partagé avec HTTP et Bolt : un verrouillage obtenu sur une surface vaut sur toutes. (REQ-1393)

**Choisir une organisation.** Sur un déploiement multi-organisations, connectez-vous à `<org>.<votre-domaine>` et pgwire lit l'organisation depuis le nom d'hôte du ClientHello TLS, comme HTTP la lit depuis l'en-tête `Host`. (REQ-1234) Le nom d'hôte demande une organisation ; il ne l'accorde pas, et un principal qui n'y est pas membre est refusé. Une connexion par adresse IP ne demande aucune organisation.

---

## Ce qui fonctionne

### SELECT

Toutes les instructions SELECT traversent le pipeline de gouvernance (`_pipeline.py`). (REQ-001, REQ-262, REQ-266) Le pipeline :

1. Réécrit le SQL sémantique en SQL physique (`rewrite_semantic_to_physical`)
2. Applique la gouvernance (sécurité au niveau des lignes, masquage, accès au domaine) (REQ-263)
3. Valide par rapport au schéma enregistré (REQ-011)
4. Route vers Trino ou vers le pool direct de la source (REQ-027, REQ-028)

Les requêtes simples multi-instructions sont prises en charge. Les instructions séparées par des points-virgules sont découpées et exécutées dans l'ordre. (REQ-580) [tool-verified: `server.py:318-381`]

Les requêtes paramétrées (`$1`, `$2`, ...) sont prises en charge à la fois en mode requête simple et en mode requête étendue (Bind/Execute). Les paramètres sont substitués sous forme de littéraux avant l'exécution. (REQ-581) [tool-verified: `server.py:78-85`]

`SELECT * FROM fn(args)` et `SELECT fn(args)` — où `fn` désigne une fonction suivie et enregistrée — sont interceptés avant le pipeline de gouvernance et routés via l'unique exécuteur gouverné (`invoke_tracked_function`). Le résultat est un ensemble de lignes typé identique à celui que renvoie toute autre surface pour cette commande. `writable_by` et les règles de gouvernance sont appliqués à l'intérieur de l'exécuteur. (REQ-1156) [tool-verified: `provisa/pgwire/function_call.py:74-88`]

### DDL

Les instructions DDL sont détectées par l'expression régulière de `server.py` et transmises à `DdlHandler`. Le rôle doit posséder la capacité `"ddl"`. (REQ-042) Sans elle, l'instruction est rejetée avec SQLSTATE 42501. [tool-verified: `ddl_handler.py:82-83`]

Les formes de DDL reconnues sont :

```sql
CREATE TABLE / VIEW / INDEX / UNIQUE INDEX / SEQUENCE / SCHEMA
ALTER TABLE / INDEX / SEQUENCE / VIEW
DROP TABLE / VIEW / INDEX / SEQUENCE / SCHEMA
```

[tool-verified: `server.py:56-61`]

Deux chemins d'exécution existent selon `ddl_catalog` : (REQ-582)

**Chemin Trino** — utilisé lorsque `ddl_catalog` est un catalogue Trino Iceberg, Hive ou tout autre catalogue non enregistré (par exemple `iceberg`, `hive`, `otel`, `results`). Seuls `CREATE TABLE` et `CREATE VIEW` sont pris en charge sur ce chemin. Toute tentative de `ALTER`, `DROP` ou `CREATE INDEX` déclenche une erreur. Le nom de table est pleinement qualifié sous la forme `catalog.schema.table`. [tool-verified: `ddl_handler.py:92-100`]

**Chemin direct** — utilisé lorsque `ddl_catalog` correspond à un identifiant de source enregistré. Le DDL complet est pris en charge : CREATE, ALTER, DROP, index, séquences. `CREATE TABLE` et `CREATE VIEW` sont qualifiés par schéma sous la forme `schema.table`. Tout autre DDL (ALTER, DROP, CREATE INDEX) est transmis tel quel après établissement du contexte de schéma. Pour les sources PostgreSQL et SQLite, le contexte est établi avec `SET search_path TO schema`. Pour MySQL et MariaDB, le contexte est établi avec `USE schema`. [tool-verified: `ddl_handler.py:139-170`, `ddl_handler.py:207-213`]

Après le DDL sur l'un ou l'autre chemin, la nouvelle table est enregistrée dans le contexte de compilation du rôle afin d'être immédiatement interrogeable. (REQ-583) [tool-verified: `ddl_handler.py:216-250`]

**Résolution de la cible d'écriture.** Le catalogue et le schéma DDL proviennent des champs `ddl_catalog` et `ddl_schema` du domaine. Si `ddl_catalog` n'est pas défini, le système utilise par défaut le catalogue Iceberg. Si `ddl_schema` n'est pas défini, il utilise par défaut l'identifiant du domaine. Le domaine est résolu via la liste `domain_access` du rôle. (REQ-584) [tool-verified: `app.py:804-811`, `ddl_handler.py:104-115`]

### COPY

`COPY ... TO STDOUT` et `COPY ... FROM STDIN` sont tous deux pris en charge. (REQ-585) [tool-verified: `copy_handler.py:231-257`]

**COPY TO STDOUT** — exporte les résultats de requête au format filaire COPY de PG. Deux formes fonctionnent :

```sql
-- Table reference
COPY my_table TO STDOUT WITH (FORMAT csv)

-- Arbitrary query
COPY (SELECT col1, col2 FROM my_table WHERE ...) TO STDOUT WITH (FORMAT text)
```

Formats pris en charge : `text` (délimité par tabulations, par défaut) et `csv`. Le format binaire n'est pas pris en charge en sortie de COPY. [tool-verified: `copy_handler.py:36-52`]

**COPY FROM STDIN** — insère des lignes dans une table cible. Limité aux sources de type `postgresql`, `mysql`, `sqlite` ou `mariadb`. (REQ-586) Toute tentative de COPY FROM sur une source exclusivement Trino (par exemple Iceberg) déclenche une erreur de permission. [tool-verified: `copy_handler.py:65`, `copy_handler.py:351-356`]

```sql
COPY my_table (col1, col2) FROM STDIN WITH (FORMAT text)
```

Si aucune liste de colonnes n'est fournie, les colonnes sont déduites du schéma enregistré. [tool-verified: `copy_handler.py:357`]

### Transactions et commandes de session

SET, BEGIN, COMMIT, ROLLBACK, SAVEPOINT, RELEASE, DISCARD, RESET et DEALLOCATE sont interceptés et renvoient une réponse de succès vide. (REQ-587) Le serveur est sans état vis-à-vis des transactions — il n'y a ni isolation transactionnelle ni prise en charge de l'annulation. (REQ-587) [tool-verified: `catalog.py:27-31`, `catalog.py:1129-1132`]

---

## Interception de catalogue

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

## Encodage binaire des paramètres

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

## Recommandations de pilote

**Pilotes Python natifs (psycopg2, asyncpg).** Ceux-ci négocient le protocole de requête étendue par défaut et utilisent l'encodage binaire pour la plupart des types. La fidélité des types est maximale ici — les colonnes `NUMERIC` arrivent en tant que `Decimal`, `TIMESTAMP` en tant que `datetime`, et ainsi de suite. Utilisez-les pour l'ETL basé sur Python, les scripts ou l'intégration directe.

**JDBC (pilote JDBC PostgreSQL).** Utilisez-le pour les outils de l'écosystème Java : DBeaver, Tableau, Power BI, Metabase, opérateurs JDBC d'Airflow. JDBC utilise par défaut le protocole de requête simple, ce qui évite les complications liées à l'encodage binaire. Chaîne de connexion :

```yaml
jdbc:postgresql://<host>:<PROVISA_PGWIRE_PORT>/provisa?user=<role_id>&password=<password>
```

Certains outils de BI basés sur JDBC envoient, à la connexion, une rafale de requêtes vers `information_schema` et `pg_catalog` pour alimenter leur explorateur de schéma. Toutes sont traitées par la couche d'interception de catalogue — aucun trafic vers Trino n'est généré lors de l'inspection du schéma. (REQ-532)

**Quand préférer l'un à l'autre.** Si le client est en Python, utilisez psycopg2 ou asyncpg pour une meilleure gestion des types. Si le client est un outil de BI ou toute application JVM, utilisez JDBC. Évitez de mélanger les attentes de protocole binaire et texte sur la même connexion si vous observez des anomalies de conversion de type — le comportement en mode texte de JDBC est plus simple à raisonner.

---

## Mises en garde et contraintes

**SQL uniquement ; pas de mutations DML.** Le listener pgwire analyse et exécute uniquement du SQL — les chaînes GraphQL et Cypher ne sont pas acceptées. (REQ-614) Les instructions `INSERT`, `UPDATE` et `DELETE` simples ne sont pas routées vers un chemin d'écriture. (REQ-615) Écrivez des données via `COPY FROM STDIN` (sources accessibles en écriture) ou `CREATE TABLE AS` ; les mutations au niveau des lignes doivent en revanche passer par les chemins d'écriture GraphQL, Cypher ou Trino.

**COPY et DDL nécessitent la capacité `ddl`.** `COPY` (dans les deux sens) et le DDL sont tous deux conditionnés par la capacité `ddl` du rôle ; les rôles qui ne la possèdent pas reçoivent SQLSTATE 42501. (REQ-616)

**Pas de véritable prise en charge des transactions.** BEGIN/COMMIT/ROLLBACK sont acceptés et ignorés silencieusement. Chaque instruction s'exécute de manière indépendante. (REQ-587) [tool-verified: `server.py:146-158` — `in_transaction()` renvoie toujours `False`]

**Délai d'expiration de 60 secondes pour le DDL, 120 secondes pour les requêtes.** Ces valeurs sont codées en dur dans les threads du gestionnaire. (REQ-590) Un DDL de longue durée sur des sources distantes (modifications de schéma sur de grandes tables) peut expirer. [tool-verified: `ddl_handler.py:136`, `server.py:186`]

**COPY FROM fonctionne uniquement avec les sources accessibles en écriture.** Iceberg, Hive, les sources exclusivement Trino et les types de source en lecture seule n'acceptent pas COPY FROM. L'erreur est SQLSTATE 42501. (REQ-586) [tool-verified: `copy_handler.py:65`]

**Le format de sortie de COPY est text ou csv.** Le format binaire COPY de PG (`FORMAT binary`) n'est pas implémenté. [inferred: seules les branches `text` et `csv` existent dans `_rows_to_copy_text` / `_rows_to_copy_csv`]

**Le DDL sur le chemin Trino est limité à CREATE.** ALTER, DROP et CREATE INDEX contre les catalogues Iceberg ou Hive ne sont pas pris en charge. Utilisez une source SQL enregistrée comme `ddl_catalog` si vous avez besoin du DDL complet. (REQ-582) [tool-verified: `ddl_handler.py:92-100`]

**La substitution de paramètres est littérale.** Les paramètres `$1`, `$2`, ... sont substitués sous forme de littéraux SQL avant l'exécution, et non envoyés comme paramètres liés au moteur sous-jacent. Cela signifie que le moteur sous-jacent ne voit jamais d'instruction préparée. Pour Trino, cela n'a aucun impact pratique ; pour les sources à pool direct, cela contourne la mise en cache des instructions préparées. (REQ-581) [tool-verified: `server.py:78-85`]

**`pg_stat_activity`, `pg_stat_user_tables`, `pg_extension`, `pg_enum`, `pg_attrdef`, `pg_proc`.** Ces tables existent dans la couche de catalogue mais sont des stubs vides. Les outils de surveillance qui les interrogent recevront zéro ligne plutôt que des erreurs. (REQ-532) [tool-verified: `catalog.py:519-535`, `catalog.py:639-934`] (`pg_index` est alimentée — voir Interception de catalogue.)
