# Intégrations

## Choisir un chemin de connexion {: #choosing-a-connection-path }

| Type de client | Chemin recommandé | Pourquoi |
| ------------- | ----------------- | ----- |
| Outils BI (Tableau, Power BI, Looker) | JDBC | Diffusion columnaire Arrow Flight sur le fil ; les outils BI ont un assistant JDBC intégré et bénéficient de la livraison columnaire à haut débit pour les grands jeux de résultats |
| psql, DBeaver, tout outil compatible PG | pgwire (pilote PG natif) | Défaut sans friction — aucun pilote personnalisé nécessaire ; utilisez ce que vous avez déjà |
| Pile de données Python (pandas, pyarrow) | `provisa-client` ou ADBC brut | Lots Arrow en streaming ; aucun surcoût de sérialisation de lignes |
| Spark, DuckDB, pipelines à haut débit | Arrow Flight (ADBC) | Streaming columnaire non borné directement vers la mémoire Arrow |
| Service à service (contrats typés) | Protobuf gRPC | Proto généré par rôle ; lignes en streaming ; sécurité de type |
| Applications web, scripting | HTTP (`/data/graphql`, `/data/sql`) | Pas de pilote ; HTTP standard ; choix complet de langage de requête |
| Clients REST (standard JSON:API) | `GET /data/jsonapi/{table}` | Enveloppe JSON:API v1.0 ; jeux de champs partiels, pagination, filtrage via paramètres de requête ; pas de pilote |

---

## pgwire — Pilote PostgreSQL natif {: #pgwire-native-postgresql-driver }

Provisa implémente le protocole filaire PostgreSQL (protocole version 3.0). Tout client qui parle PostgreSQL se connecte sans pilote personnalisé.

Activez-le en définissant `PROVISA_PGWIRE_PORT` (p. ex. `5433`) avant de démarrer Provisa. Désactivé quand non défini ou à `0`.

### Pourquoi pgwire plutôt que JDBC ? {: #why-pgwire-instead-of-jdbc }

Le pilote JDBC fait passer ses requêtes par Arrow Flight, et il faut déployer `provisa-jdbc.jar` pour s'en servir. pgwire n'a besoin de rien de plus. Si vous avez déjà `psql`, DBeaver, SQLAlchemy ou un pilote PG JDBC, vous êtes prêt : c'est le chemin le plus simple quand vous n'envoyez que du SQL.

Choisissez JDBC pour les outils BI qui ont un assistant de connexion JDBC intégré et ramènent de grands jeux de résultats, là où le streaming columnaire d'Arrow Flight est rentable. pgwire accepte le même SQL libre sur tout le schéma publié, avec moins à configurer.

### psql

```bash
psql -h localhost -p 5433 -U alice
```

### DBeaver

1. New Connection → PostgreSQL
2. Host : `localhost`, Port : `5433`
3. Nom d'utilisateur / mot de passe tels que configurés dans Provisa
4. Aucun téléchargement de pilote supplémentaire requis

### SQLAlchemy (Python)

```python
from sqlalchemy import create_engine

engine = create_engine("postgresql+psycopg2://alice:secret@localhost:5433/provisa")
df = pd.read_sql("SELECT * FROM sales.orders", engine)
```

Ou avec `asyncpg` :

```python
engine = create_engine("postgresql+asyncpg://alice:secret@localhost:5433/provisa")
```

### Authentification {: #authentication }

Le champ `password` du paquet de démarrage porte l'identifiant, et ce que l'identifiant *est* détermine la méthode : un jeton d'accès personnel, un jeton porteur OIDC, ou un mot de passe contre le fournisseur configuré. Sous le fournisseur `basic` avec `auth.scram: true`, le mot de passe est prouvé via SCRAM-SHA-256 plutôt qu'envoyé. Les certificats client sont pris en charge. En mode de confiance (`none`) le nom d'utilisateur se mappe directement à un rôle et le mot de passe est ignoré.

Le tableau complet surface × méthode se trouve dans [Security Model](security.md#surfaces-and-credentials). MD5 n'est pas pris en charge ; activez TLS (`PROVISA_PGWIRE_CERT` / `PROVISA_PGWIRE_KEY`) en cas d'exécution sur un réseau non fiable.

### Limitations

- SQL uniquement. GraphQL et Cypher ne sont pas acceptés sur pgwire.
- Pas en lecture seule. Les lignes des tables enregistrées sont écrites avec `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `TRUNCATE` et `COPY ... FROM STDIN`, chacune admise par le droit `write` du rôle, ses octrois de colonnes et son filtre de lignes ; voir [Écritures de données](pgwire.md#data-writes-insert-update-delete-merge-copy-from-stdin).
- Aucune définition : `CREATE`, `ALTER` et `DROP` sont refusés avec SQLSTATE 0A000, sauf la table temporaire propre à une session ; voir [Définitions](pgwire.md#definitions-create-alter-drop). `COPY ... TO STDOUT` exporte en `text`, `csv` et `binary` ; `COPY ... FROM STDIN` lit `text` et `csv`.
- Les requêtes `information_schema` et `pg_catalog` sont interceptées et répondues depuis un shim de catalogue DuckDB — les outils de découverte de schéma fonctionnent correctement.

---

## Pilote JDBC {: #jdbc-driver }

Le pilote JDBC de Provisa utilise Arrow Flight comme transport sous-jacent. C'est le chemin recommandé pour les outils BI dotés d'un assistant de connexion JDBC.

Les requêtes passent par Arrow Flight dès que le port Flight répond à l'ouverture de la connexion, et les résultats arrivent par lots avec leurs types de colonnes. Le port Flight est supposé être celui situé à côté du port HTTP (8815 à côté de 8001) ; définissez la propriété `flight_port` lorsque ce n'est pas le cas. Si le port Flight est injoignable à la connexion, la connexion utilise l'API HTTP pour ses requêtes et le journalise en INFO. Une fois une connexion sur Flight, une erreur du serveur — un identifiant refusé, un rôle que l'utilisateur ne détient pas, une instruction en échec — est remontée à l'appelant avec le message du serveur ; elle n'est pas réessayée en HTTP.

La navigation dans le schéma (`getTables`, `getColumns`, clés primaires et étrangères) est lue sur le même port Flight, avec l'identifiant de connexion : le pilote liste le catalogue du rôle sous lequel il agit, donc une table ou une colonne non accordée à ce rôle n'apparaît pas. Une clé qui fait référence à une table ou une colonne non accordée au rôle n'est pas listée non plus. Sur une connexion dont le port Flight était injoignable à la connexion, le pilote lit le même catalogue en HTTP (`GET /data/catalog`) : le serveur construit les deux listes avec une seule fonction, donc les tables, les noms de colonnes, les types et les clés ne dépendent pas du port qui a répondu.

### Connexion {: #connection }

Téléchargez [provisa-jdbc.jar](https://provisa.dev/dl/jdbc) (toujours la dernière version) et ajoutez-le au chemin de pilote de votre outil.

URL JDBC :

```yaml
jdbc:provisa://<host>:8815
```

L'authentification utilise les propriétés JDBC standard `user` / `password`. Le pilote les échange contre un jeton de session qu'il envoie à chaque requête ; une connexion refusée fait échouer la connexion avec le statut et le motif du serveur. Provisa dérive le rôle de l'identité connectée. La propriété facultative `role` (ou `?role=` dans l'URL) demande à agir sous un rôle que détient l'utilisateur, ou un ensemble séparé par des virgules de tels rôles ; un rôle que l'utilisateur ne détient pas est refusé. Sur un déploiement sans fournisseur d'authentification, il n'y a pas de connexion, et le nom d'utilisateur est pris comme rôle demandé.

### Configuration des outils BI {: #bi-tool-setup }

**Tableau**

1. Manage → Drivers → Install Provisa JDBC
2. Connect → Other Databases (JDBC)
3. URL : `jdbc:provisa://localhost:8815`
4. Saisissez votre nom d'utilisateur et mot de passe lorsque demandé

**DBeaver** (chemin JDBC — pour le chemin pgwire voir ci-dessus)

1. Database → New Connection → JDBC
2. Pilote : ajoutez `provisa-jdbc.jar`
3. URL : `jdbc:provisa://localhost:8815`
4. Saisissez votre nom d'utilisateur et mot de passe dans l'onglet Authentication

**Power BI** — utilisez la passerelle ODBC avec le pont Provisa JDBC-ODBC (inclus dans l'installateur).

---

## Clients Arrow Flight {: #arrow-flight-clients }

Arrow Flight (port 8815) est le chemin recommandé pour les outils de données qui le prennent en charge. Les résultats sont diffusés sous forme de RecordBatches Arrow sans se matérialiser dans la mémoire de Provisa.

### Python (`provisa-client`)

Le chemin Python recommandé — enveloppe à la fois GraphQL et Arrow Flight :

```bash
pip install provisa-client
```

```python
from provisa_client import ProvisaClient

client = ProvisaClient("http://localhost:8001", username="alice", password="secret")

# Arrow Flight → pyarrow Table (high-throughput, streaming)
table = client.flight("SELECT id, amount FROM sales.orders")

# Arrow Flight → pandas DataFrame
df = client.flight_df("SELECT id, amount FROM sales.orders")

# GraphQL → DataFrame
df = client.query_df("{ orders { id amount } }")
```

Voir [docs/python-client.md](python-client.md) pour la référence complète incluant DB-API 2.0, le dialecte SQLAlchemy, et ADBC.

### Python (PyArrow brut) {: #python-raw-pyarrow }

```python
import pyarrow.flight as flight

client = flight.connect("grpc://localhost:8815")
ticket = flight.Ticket(b'{"query": "SELECT id, amount FROM sales.orders"}')
df = client.do_get(ticket).read_all().to_pandas()
```

Flight porte son identifiant dans la charge utile JSON, comme champ `token` — un jeton porteur de fournisseur ou un jeton d'accès personnel. La poignée de main et chaque ticket l'acceptent tous deux, et les deux le valident de la même manière, de sorte qu'un client authentifié à la poignée de main présente toujours le jeton à chaque `do_get`. Un champ `role` à ses côtés *demande* un rôle ; le serveur dérive les rôles autorisés de l'identité et substitue la valeur autorisée, de sorte qu'une chaîne de rôle dans un ticket n'est jamais l'identité elle-même. Voir [Security Model](security.md#surfaces-and-credentials).

```python
ticket = flight.Ticket(json.dumps({
    "query": "SELECT id, amount FROM sales.orders",
    "token": "provisa_pat_...",
    "role": "analyst",
}).encode())
```

La navigation a aussi besoin de l'identifiant. `list_flights`, `get_flight_info` et `get_schema` ne portent pas de ticket, ils lisent donc l'identifiant dans l'en-tête d'appel `authorization: Bearer <token>` et le rôle demandé dans `x-provisa-role` (un rôle, ou un ensemble de rôles détenus séparés par des virgules). Avec l'authentification activée, un appel de navigation sans identifiant est refusé, et le catalogue renvoyé est celui du rôle autorisé : les tables, colonnes et commandes accordées à ce rôle, et rien d'autre. Un ticket de catalogue (un ticket sans `query`) reçoit la même réponse à partir de ses propres `token` et `role`. Un client doit donc envoyer son identifiant avant de naviguer ; `provisa-client` le fait dans `list_tables()`. Sur un déploiement sans fournisseur d'authentification, ces appels listent tout le catalogue et ne prennent pas d'identifiant.

```python
options = flight.FlightCallOptions(headers=[
    (b"authorization", b"Bearer provisa_pat_..."),
    (b"x-provisa-role", b"analyst"),
])
for info in client.list_flights(b"", options):
    print(info.descriptor.path)
```

### ADBC

```python
import adbc_driver_flightsql.dbapi as adbc

conn = adbc.connect("grpc://localhost:8815", db_kwargs={"username": "alice", "password": "secret"})
cursor = conn.cursor()
cursor.execute("SELECT id, amount FROM sales.orders")
table = cursor.fetch_arrow_table()
```

### DuckDB

```python
import duckdb, pyarrow.flight as flight

client = flight.connect("grpc://localhost:8815")
ticket = flight.Ticket(b'{"query": "SELECT * FROM sales.orders"}')
arrow_table = client.do_get(ticket).read_all()

conn = duckdb.connect()
result = conn.execute("SELECT region, sum(amount) FROM arrow_table GROUP BY 1").df()
```

### Spark (PySpark)

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .config("spark.jars.packages", "org.apache.arrow:flight-core:14.0.0") \
    .getOrCreate()

# Use ADBC Flight connector or load via pandas → Spark
```

---

## Protobuf gRPC (port 50051)

Chemin de service à service. Provisa génère un `.proto` par rôle et le régénère chaque fois que le modèle ou le rôle change — chaque rôle ne voit que les tables et colonnes auxquelles il a accès. Le service a une RPC pour chaque table à la racine du schéma du rôle ; les tables liées s'atteignent par leurs relations, comme dans GraphQL.

Téléchargez le proto pour votre rôle :

```bash
curl http://localhost:8001/data/proto/analyst > provisa_analyst.proto
```

Utilisez `grpc_server_reflection` pour découvrir le schéma par programme. La réflexion décrit le service du rôle sous lequel l'appel agit, le même fichier que le téléchargement ci-dessus. Avec l'authentification désactivée, un appel de réflexion indique son rôle comme tout autre appel :

```bash
grpcurl -plaintext -H 'x-provisa-role: analyst' localhost:50051 list
```

Chaque RPC doit porter un identifiant dans la clé de métadonnées `authorization` — un jeton de fournisseur ou un jeton d'accès personnel. `x-provisa-role` demande un rôle parmi l'ensemble autorisé de l'identité ; ce n'est pas un identifiant et ça ne l'a jamais été. Les certificats client sont pris en charge. Voir [Security Model](security.md#surfaces-and-credentials).

Les requêtes en streaming émettent un message par ligne ; les mutations sont unaires.

---

## Invoquer des commandes à travers les protocoles {: #invoking-commands-across-protocols }

Une **commande** est une fonction suivie ou un webhook enregistré — un élément appelable enregistré dans le modèle de Provisa avec un `kind` (`query` ou `mutation`) et un `impl_kind` qui décrit sa façon de s'exécuter. Chaque surface achemine les invocations à travers un seul exécuteur gouverné (`invoke_tracked_function`) qui applique uniformément l'affectation de la commande (`visible_to`), le droit `write` pour une mutation et la gouvernance.

| `impl_kind` | Ce qui s'exécute | Champs de liaison |
| ------------ | ----------- | --------------- |
| `source_procedure` | Procédure stockée sur une source enregistrée (par défaut) | `sourceId`, `schemaName`, `functionName` |
| `script` | Script côté serveur | `script` |
| `http` | Appel HTTP sortant | `url`, `method` |
| `grpc` | Appel gRPC sortant vers un serveur externe | `target`, `method` |
| `python` | Élément appelable Python hébergé par Provisa | `callable` (p. ex. `demo.py_functions:random_dataset`) |

Quand une commande déclare un `return_schema` (JSON Schema avec `type: array, items: object`), elle retourne un ensemble — chaque surface la projette comme un jeu de lignes typé. Les commandes de démonstration `random_python_set` (impl_kind `python`) et `random_grpc_set` (impl_kind `grpc`) illustrent à la fois un appelable hébergé et un pont gRPC externe retournant des lignes à valeurs aléatoires ; les deux sont enregistrées dans `config/provisa-install.yaml`.

### Matrice de protocole {: #protocol-matrix }

| Surface | Syntaxe | Exemple |
| --------- | -------- | --------- |
| GraphQL | `kind=query` → champ Query ; `kind=mutation` → champ Mutation ; préfixé par domaine quand `domain_prefix: true` | `{ ps__random_python_set(rows: 5, seed: 42) { id region amount } }` |
| pgwire / Arrow Flight / MCP `run_sql` | `SELECT * FROM fn(args)` ou `SELECT fn(args)` | `SELECT * FROM random_python_set(5, 42)` |
| Cypher HTTP (`POST /data/cypher`) | `CALL fn(args) YIELD cols` | `CALL random_python_set(5, 42) YIELD id, region, amount` |
| Bolt (Neo4j Browser / driver) | `CALL fn(args)` — les arguments positionnels se mappent aux noms d'arguments déclarés | `CALL random_python_set(3, 7)` |
| Provisa gRPC (port 50051) | Unaire `CallCommand(CommandRequest{name, args_json})` → `CommandResponse{rows_json}` | `grpcurl -d '{"name":"random_python_set","args_json":"{\"rows\":5}"}' ... ProvisaService/CallCommand` |

Le champ `kind` ne contrôle que le placement GraphQL — les surfaces SQL, Cypher, Bolt, et gRPC acceptent les commandes `query` et `mutation` de manière identique.

---

## Apollo Federation

Provisa peut agir comme un sous-graphe Federation v2, exposant son schéma publié à un Apollo Router ou Apollo Gateway.

### Configuration {: #setup }

Activez la fédération dans `config.yaml` :

```yaml
federation:
  enabled: true
  subgraph_name: provisa-data
```

Provisa génère automatiquement des directives `@key` sur les colonnes de clé primaire et `@external`/`@provides` sur les relations inter-sous-graphes.

### Enregistrement avec Apollo Router {: #register-with-apollo-router }

Dans votre `supergraph.yaml` :

```yaml
subgraphs:
  provisa-data:
    routing_url: http://provisa:8001/data/graphql
    schema:
      subgraph_url: http://provisa:8001/data/graphql
```

Exécutez `rover supergraph compose --config supergraph.yaml` pour générer le schéma du supergraphe.

### Entités {: #entities }

Provisa répond aux requêtes `_entities` pour les jointures inter-sous-graphes. Toute table avec une clé primaire est automatiquement résolvable comme une entité Federation.

---

## Import Hasura v2 / DDN {: #hasura-v2-ddn-import }

Voir [docs/import.md](import.md) pour la migration depuis Hasura vers Provisa.

---

## Kafka

Voir [docs/sources.md](sources.md#kafka-sources) pour la configuration des topics Kafka comme tables en lecture seule et receveurs de résultats de requête.

---

## Vérificateurs de qualité des données {: #data-quality-checkers }

Soda Core et Great Expectations se connectent à Provisa de la même manière que tout autre client postgres — via pgwire. C'est l'intégration entière : le vérificateur détient un seul pilote postgres et balaie la vue fédérée, de sorte qu'une table Snowflake, une table Iceberg et une collection Mongo sont toutes vérifiées par le même dialecte de contrat sans vérificateur par système.

Le balayage s'exécute dans un interpréteur enfant — `python -m provisa.dq.worker` — qui est le seul endroit où `soda_core` ou `great_expectations` est importé. Rien n'est lié au processus serveur, et un plantage de vérificateur fait tomber un sous-processus plutôt que la boucle d'événements.

Les résultats de balayage atterrissent comme des lignes source ordinaires, de sorte que la cadence, la fraîcheur, les événements, la traçabilité, la gouvernance, RLS, la grille et l'export s'appliquent tous sans mécanisme secondaire. La rédaction de contrats, l'enveloppe de résultat et l'enregistrement dérivé sont couverts dans [docs/sources.md](sources.md#data-quality-checkers).

### Installer un vérificateur {: #installing-a-checker }

Aucune des deux bibliothèques n'est livrée par défaut. L'installateur demande laquelle vous voulez, et la réponse devient `dq_checker: none|soda|gx` dans `~/.provisa/config.yaml`. Sur le palier Docker, `scripts/provisa` transforme cela en l'argument de build `PROVISA_EXTRAS` ; sur le palier natif, `first-launch.sh` installe l'extra pyproject correspondant dans le venv.

| `dq_checker` | Bibliothèque | Licence | Plan cloud hébergé |
| -------------- | --------- | --------- | -------------------- |
| `soda` | `soda-postgres` | Elastic License 2.0 | Refusé (`cloud_eligible: false`) |
| `gx` | `great-expectations[postgresql]` | Apache 2.0 | Autorisé |

La Elastic License 2.0 interdit de fournir le logiciel à des tiers en tant que service hébergé, ce que serait l'exécution de Soda à l'intérieur du plan SaaS pour le compte d'un locataire. Un déploiement hébergé qui souhaite Soda pointe vers un endpoint Soda que l'opérateur exécute lui-même. Voir [docs/configuration.md](configuration.md#data-quality-checkers-soda-great_expectations) pour les clés de connexion.

---

## Échange sémantique Apache Ossie {: #apache-ossie-semantic-interchange }

Provisa échange des modèles sémantiques avec Apache Ossie (spécification 0.2.0.dev0, en incubation ; anciennement Open
Semantic Interchange) au moyen d'un adaptateur de frontière. Le vocabulaire interne de Provisa n'est jamais renommé
en celui d'Ossie — la spécification déclare probables des changements incompatibles, le couplage est donc confiné à l'adaptateur.

La spécification en brouillon a changé de forme sans changer sa chaîne de version. L'adaptateur suit la
spécification telle qu'elle est au commit `642cb894` d'`apache/ossie` (2026-10-10) : un modèle par document,
avec `version`, `name`, `datasets`, `relationships` et `metrics` à la racine du document, et
un `datatype` énuméré. L'export est vérifié dans les tests unitaires contre le
`core-spec/ossie-schema.json` de ce commit.

### Export

La surface d'export canonique est un endpoint HTTP en direct. Il dérive le document Ossie de l'état en direct
à chaque requête — sans cache ni étape de génération.

```http
GET /admin/ossie
```
La réponse est un document YAML avec `Content-Disposition: attachment; filename=provisa.ossie.yaml`.

La page Métriques propose aussi un bouton **Télécharger** et une URL d'endpoint copiable dans le panneau Ossie
Interchange, qui pointent tous deux vers le même endpoint.

#### Ce qui est exporté {: #what-is-exported }

L'adaptateur fait correspondre les objets Provisa aux objets Ossie comme suit :

| Objet Provisa | Objet Ossie | Notes |
| --- | --- | --- |
| `Table` | `dataset` | `source` = `catalog.schema.table` ; clés primaires/uniques issues de la configuration des colonnes et de `UniqueConstraint` |
| `Column` | `field` | `expression` = référence de colonne (dialecte ANSI_SQL) ; les colonnes temporelles reçoivent `dimension.is_time: true` |
| `Relationship` | `relationship` | L'alias sert de nom lorsqu'il est défini ; les relations calculées (cible fonction) sont ignorées |
| `Metric` | `metric` | `name`, `expression` (ANSI_SQL), `datatype` (mis en correspondance comme celui d'une colonne), `description`, `ai_context` |
| `modeling_role` / `modeling_history` | `custom_extensions[].vendor_name="provisa"` | Aller-retour uniquement ; d'autres outils peuvent l'ignorer |

Une relation sur plusieurs colonnes liste chaque colonne dans `from_columns` et `to_columns`. Un
modèle sans table est refusé avec un `422` : un document Ossie a au moins un dataset.

La gouvernance, la RLS, le lignage et la sémantique de graphe ne sont pas exportés. Ils peuvent voyager dans l'emplacement facultatif
`provisa` de custom_extensions pour la fidélité de l'aller-retour, mais l'échange ne dépend jamais de la lecture qu'en font d'autres
outils.

Un type de colonne hors du vocabulaire d'Ossie est écrit `Opaque`, avec le nom propre du type dans
l'extension `provisa` du champ, ce que prescrit la spécification pour un type connu dont elle n'a pas de
membre. Une colonne sans type enregistré n'indique pas de `datatype`. L'adaptateur ne fait jamais correspondre un type que la
colonne n'a pas.

#### Correspondance des types {: #type-mapping }

| Type Provisa / source | `datatype` Ossie |
| --- | --- |
| `varchar`, `text`, `char`, `uuid`, `string` | `String` |
| `int`, `integer`, `bigint`, `smallint`, `int4`, `int8`, `tinyint` | `Integer` |
| `numeric`, `decimal` | `Decimal` |
| `float`, `double`, `real` | `Float` |
| `bool`, `boolean` | `Boolean` |
| `date` | `Date` |
| `time` | `Time` |
| `timestamp`, `datetime` | `DateTime` |
| `timestamptz`, `timestamp with time zone` | `DateTimeTz` |
| tout autre | `Opaque`, avec le nom du type dans l'extension `provisa` |

Le `Decimal` d'Ossie ne porte ni précision ni échelle, donc `numeric(10,2)` est exporté en `Decimal`
et revient d'un import en `decimal`.

### Import

L'import accepte un document Ossie (YAML ou JSON) et renvoie des propositions d'enregistrement. Rien n'est
enregistré automatiquement — les définitions importées ne contournent jamais l'étape de revue.

```http
POST /admin/ossie/import
Content-Type: text/yaml   (or application/json)

<ossie document>
```
Le serveur analyse le document avec `parse_ossie_model`, qui valide la structure et renvoie une
dataclass `OssieImport` contenant les tables, relations et métriques proposées sous forme de dictionnaires simples.
Tout problème structurel est un `400` avec une erreur nommée par chemin, par ex.
`ossie import: missing $.datasets[1].source`.

L'importateur ne lit que la forme plate du document. Trois choses sont refusées nommément :

- Un document qui enveloppe son modèle dans `semantic_model`, la forme qu'avait la spécification en brouillon avant
  septembre 2026. L'erreur indique que le document est antérieur à la forme plate et comment le convertir.
- Une `version` autre que `0.2.0.dev0`.
- Un `datatype` qui n'est pas membre du vocabulaire d'Ossie ; l'erreur nomme le chemin et liste
  les membres.

Une colonne ou une métrique proposée porte le type que représente son datatype Ossie (`Integer` propose
`bigint`, `DateTime` propose `timestamp`). `Opaque` propose le type que nomme l'extension `provisa`
lorsque le document vient de Provisa, et aucun type sinon. Un `ai_context` donné comme
objet est lu par ses `instructions`.

Un champ n'est proposé comme colonne que si son expression est une colonne de la source. Un champ
dont l'expression est un calcul n'est pas proposé ; l'écran de revue le nomme sous sa table
comme non importé. Un champ qui renomme sa colonne est proposé comme cette colonne, avec le nom du
champ comme alias de la colonne. La `source` d'un dataset doit avoir trois parties,
`source.schema.table` ; une requête ou un nom plus court est refusé, et l'erreur le dit.

#### Depuis dbt, Cube, Snowflake et d'autres couches sémantiques {: #coming-from-dbt-cube-snowflake-and-other-semantic-layers }

Le projet Apache Ossie maintient des convertisseurs entre Ossie et les formats de modèle d'autres outils. Exécutez
le convertisseur de votre outil, puis envoyez sa sortie à l'endpoint d'import ou choisissez le fichier dans
l'écran de revue ci-dessous. Provisa lit le document Ossie ; il ne lit pas les fichiers de l'autre outil.

| Depuis | Commande du convertisseur | Ce qu'il lit |
|---|---|---|
| dbt (couche sémantique) | `ossie-dbt msi-to-ossie -i target/semantic_manifest.json -o semantic_model.yaml` | Le `semantic_manifest.json` que dbt écrit pour MetricFlow |
| Cube | `ossie-cube import -i model/ -o model.yaml` | Un répertoire de modèle Cube, ou des fichiers de cubes et de vues individuels |

Le même dépôt contient des convertisseurs pour Snowflake, Databricks, Microsoft, Salesforce,
ThoughtSpot, Sigma, Omni, Hex, GoodData et d'autres ; chacun a son propre README.

Deux limites à connaître avant de convertir :

- Le convertisseur dbt ne lit que le manifeste de la couche sémantique. Un projet dbt sans modèles
  sémantiques n'a rien à convertir. Il abandonne les métriques de conversion, les métriques privées et les
  entités à clé naturelle, et affiche un avertissement pour chacune.

- La sortie d'un convertisseur suit la spécification telle que l'a le propre commit de ce convertisseur. Provisa lit
  la forme plate du commit `642cb894` d'`apache/ossie` ; un convertisseur antérieur à septembre 2026
  écrit l'enveloppe `semantic_model`, que Provisa refuse nommément.

Les deux voies ont été exécutées le 2026-10-10 avec les convertisseurs installés depuis le commit `642cb894` d'`apache/ossie`
(ils ne sont pas publiés comme paquets ; ils s'installent depuis les répertoires `converters/dbt` et
`converters/cube` du dépôt et nécessitent Python 3.11 ou plus récent). Un modèle de
deux tables, une relation et une métrique a été exporté de Provisa, converti en manifeste sémantique dbt
et en modèle Cube, reconverti en Ossie par `ossie-dbt msi-to-ossie` et
`ossie-cube import`, puis lu par `parse_ossie_model` :

| Voie | Résultat |
|---|---|
| Modèle Cube → Ossie → Provisa | Lu. Les tables, les clés, les types de colonnes, la relation et la métrique sont revenus ; le nom de la relation était celui du convertisseur (`orders_to_customers`). |
| Manifeste sémantique dbt → Ossie → Provisa | Lu. Les tables, les clés primaires et la métrique sont revenues. Les types de colonnes non (la sortie du convertisseur n'en indique aucun), et la relation est revenue telle que le convertisseur l'a dérivée des entités dbt, ce qui n'était pas celle enregistrée. Vérifiez les relations avant d'appliquer. |

Le convertisseur dbt refuse un modèle dans lequel une table a une contrainte d'unicité sur plusieurs
colonnes (« MetricFlow entities cannot represent composite keys losslessly »).

#### L'écran de revue {: #the-review-screen }

Dans l'interface, le bouton **Importer** (page Métriques → panneau Ossie Interchange) ouvre un sélecteur de fichier.
Une fois le document envoyé et analysé, une fenêtre de revue s'ouvre avec chaque table,
relation et métrique proposée listée comme élément coché. Le modélisateur peut décocher n'importe quoi pour l'exclure.
Cliquer sur **Appliquer** enregistre les éléments cochés via les mutations d'enregistrement existantes — d'abord les
tables, puis les relations (qui font référence aux tables), puis les métriques.

Le rôle de modélisation et l'historique stockés dans un document Ossie exporté par Provisa font correctement l'aller-retour
à l'import.

### Fichier de sources dbt {: #dbt-sources-file }

Pour un projet dbt qui n'utilise pas la couche sémantique, Provisa écrit directement un `sources.yml` dbt.
Il est dérivé du modèle en direct à chaque requête.

```http
GET /admin/dbt/sources?role=<role id>
```
La réponse est du YAML avec `Content-Disposition: attachment; filename=provisa.sources.yml`. L'appelant
a besoin de `org_settings`, et `role` doit être un rôle qu'il détient ; le détenteur de
`access_config` peut nommer n'importe quel rôle. Le panneau d'échange de la page Métriques propose le même téléchargement
pour le rôle utilisé.

Le fichier liste chaque table servie au rôle, sous une source dbt par source et schéma (nommée
d'après l'identifiant de la source, ou `<source>_<schema>` lorsqu'une source a des tables dans plusieurs schémas),
avec la description de la table, les descriptions des colonnes servies et ces tests :

| Le modèle indique | Le fichier écrit |
|---|---|
| Une clé primaire d'une colonne | `unique` et `not_null` sur cette colonne |
| Une clé primaire de plusieurs colonnes | `not_null` sur chaque colonne |
| Une contrainte d'unicité d'une colonne | `unique` sur cette colonne |
| Une relation entre deux tables toutes deux servies au rôle | `relationships` sur la colonne source, vers `source('<source>', '<table>')` |

Une colonne non servie au rôle est absente, tout comme une table sans colonne servie. Une clé sur
plusieurs colonnes n'a pas de test dbt standard ; le commentaire d'ouverture du fichier nomme chacune, et aucun
test n'est écrit pour elle. Le modèle n'enregistre pas la nullabilité, donc `not_null` n'apparaît que sur les colonnes de clé.
Le fichier ne porte ni métriques, ni sécurité de lignes, ni masquage, ni rôles, ni lignage, et son commentaire d'ouverture
le dit.

Un fichier généré à partir d'un modèle de deux tables et une relation a été analysé par dbt-core 1.12.5
avec dbt-duckdb 1.11.0 : deux sources et cinq tests, aucun avertissement. dbt-core 1.9.11 ne lit pas
ses tests `relationships`, qui imbriquent leurs arguments sous `arguments` ; les versions intermédiaires
n'ont pas été essayées.

---

## Métriques à travers les protocoles {: #metrics-across-protocols }

La définition d'une métrique gouvernée — son expression, description, et `ai_context` — voyage avec
la valeur dans chaque surface de requête à travers une seule expansion du compilateur. Il n'y a
aucune copie. Le compilateur réserve le schéma `metrics` pour l'accès SQL ; chaque protocole ajoute
ensuite son propre canal de métadonnées.

### SQL / pgwire

Adressez toute métrique comme une relation virtuelle dans le schéma `metrics`. Les colonnes de
dimension que vous sélectionnez deviennent le GROUP BY :

```sql
-- Grand total
SELECT value FROM metrics.net_revenue;

-- By region
SELECT region, value FROM metrics.net_revenue GROUP BY region;

-- By region and month, filtered
SELECT region, month, value
FROM metrics.net_revenue
WHERE net_revenue.status = 'completed'
GROUP BY region, month;
```

Le compilateur étend la forme `metrics.<name>` vers l'agrégat groupé réel avant que la gouvernance
ne s'exécute. Les descriptions de colonnes sont exposées comme des entrées `pg_description`, de
sorte que DBeaver et `\d+` de psql les affichent.

`SELECT *` est rejeté — nommez les colonnes explicitement.

### GraphQL

Les métriques se projettent à l'intérieur du champ racine `_aggregate` comme un bloc `metrics`.

Le texte de la définition (`description`, `ai_context`) apparaît dans les documents d'introspection
GraphQL, de sorte que les outils sensibles au schéma et la génération de code le récupèrent
automatiquement.

### MCP (agents IA) {: #mcp-ai-agents }

Deux outils exposent les métriques aux clients MCP :

- **`list_metrics`** — retourne toutes les métriques gouvernées visibles pour la session, avec
  `name`, `description`, et `ai_context`.
- **`query_metric`** — accepte un nom de métrique plus une liste de dimensions et appelle le chemin
  SQL sémantique du compilateur, retournant le résultat agrégé.

Les agents qui appellent `list_metrics` avant de construire une requête sélectionnent une métrique
gouvernée par nom plutôt que d'écrire du SQL d'agrégation à la main. Le champ `ai_context` est
l'endroit où placer le texte de définition qui guide une sélection correcte.

### Arrow Flight

Les métriques sont adressables comme des descripteurs de vol de métrique retournant des tables
Arrow.

Utilisez la même forme SQL `metrics.<name>` via le chemin de ticket Flight SQL standard.

### Bolt / Cypher (Neo4j Browser)

Appelez une métrique en utilisant la procédure `provisa.metric()` :

```cypher
CALL provisa.metric('net_revenue', ['region']) YIELD region, value
```

Les tables Fact et Dimension portent des labels de nœud `:Fact` et `:Dimension` dans le graphe
fédéré, de sorte que Bloom rend automatiquement la forme en étoile.

### Requêtes en langage naturel {: #natural-language-queries }

Le matcher de schéma NL résout le vocabulaire de métrique dans les questions en langage naturel
directement vers une métrique plus des dimensions, puis génère du SQL sémantique.

Les tables de fait sont taguées `[fact]` dans le prompt NL ; les tables de dimension sont taguées
`[dimension]`. Le matcher favorise les chemins de jointure fait-vers-dimension lors de la
résolution des questions.

### Streaming

Combinez `view_metrics` avec `materialize` pour tenir une vue de métriques à jour à mesure que ses
entrées changent, avec la machinerie de matérialisation existante. Aucun nouveau pipeline n'est
requis.

### Observabilité (OTel) {: #observability-otel }

Les évaluations de métriques sont tracées et exportables comme métriques OpenTelemetry.
