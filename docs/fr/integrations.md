# Intégrations

## Choisir un chemin de connexion

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

## pgwire — Pilote PostgreSQL natif

Provisa implémente le protocole filaire PostgreSQL (protocole version 3.0). Tout client qui parle PostgreSQL se connecte sans pilote personnalisé.

Activez-le en définissant `PROVISA_PGWIRE_PORT` (p. ex. `5433`) avant de démarrer Provisa. Désactivé quand non défini ou à `0`.

### Pourquoi pgwire plutôt que JDBC ?

Le pilote JDBC utilise Arrow Flight comme transport et nécessite le déploiement du `provisa-jdbc.jar`. pgwire ne nécessite rien — si vous avez déjà `psql`, DBeaver, SQLAlchemy, ou un pilote PG JDBC, c'est terminé. C'est le chemin à moindre friction pour les charges SQL uniquement.

JDBC est le bon choix pour les outils BI qui ont un assistant de connexion JDBC intégré et bénéficient du streaming columnaire d'Arrow Flight pour les grands jeux de résultats. pgwire accepte du SQL libre sur l'intégralité du schéma publié — les mêmes requêtes, un coût de mise en place moindre.

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

### Authentification

Le champ `password` du paquet de démarrage porte l'identifiant, et ce que l'identifiant *est* détermine la méthode : un jeton d'accès personnel, un jeton porteur OIDC, ou un mot de passe contre le fournisseur configuré. Sous le fournisseur `basic` avec `auth.scram: true`, le mot de passe est prouvé via SCRAM-SHA-256 plutôt qu'envoyé. Les certificats client sont pris en charge. En mode de confiance (`none`) le nom d'utilisateur se mappe directement à un rôle et le mot de passe est ignoré.

Le tableau complet surface × méthode se trouve dans [Security Model](security.md#surfaces-et-identifiants). MD5 n'est pas pris en charge ; activez TLS (`PROVISA_PGWIRE_CERT` / `PROVISA_PGWIRE_KEY`) en cas d'exécution sur un réseau non fiable.

### Limitations

- SQL uniquement. GraphQL et Cypher ne sont pas acceptés sur pgwire.
- Non en lecture seule. `COPY ... FROM STDIN` insère des lignes dans les sources `postgresql`, `mysql`, `sqlite`, et `mariadb`, et le DDL est pris en charge (voir ci-dessous).
- Le DDL (`CREATE`, `ALTER`, `DROP`) est pris en charge et distribué vers le chemin Trino ou direct ; la nouvelle table est enregistrée dans le contexte de compilation et est immédiatement interrogeable. `COPY ... TO STDOUT` (export) et `COPY ... FROM STDIN` (import) sont pris en charge dans les formats `text` et `csv`.
- Les requêtes `information_schema` et `pg_catalog` sont interceptées et répondues depuis un shim de catalogue DuckDB — les outils de découverte de schéma fonctionnent correctement.

---

## Pilote JDBC

Le pilote JDBC de Provisa utilise Arrow Flight comme transport sous-jacent. C'est le chemin recommandé pour les outils BI dotés d'un assistant de connexion JDBC.

### Connexion

Téléchargez [provisa-jdbc.jar](https://provisa.dev/dl/jdbc) (toujours la dernière version) et ajoutez-le au chemin de pilote de votre outil.

URL JDBC :

```yaml
jdbc:provisa://<host>:8815
```

L'authentification utilise les propriétés JDBC standard `user` / `password`. Provisa authentifie les identifiants contre le fournisseur d'authentification configuré et assigne le rôle — le client ne choisit pas son propre rôle.

### Configuration des outils BI

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

## Clients Arrow Flight

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

### Python (PyArrow brut)

```python
import pyarrow.flight as flight

client = flight.connect("grpc://localhost:8815")
ticket = flight.Ticket(b'{"query": "SELECT id, amount FROM sales.orders"}')
df = client.do_get(ticket).read_all().to_pandas()
```

Flight porte son identifiant dans la charge utile JSON, comme champ `token` — un jeton porteur de fournisseur ou un jeton d'accès personnel. La poignée de main et chaque ticket l'acceptent tous deux, et les deux le valident de la même manière, de sorte qu'un client authentifié à la poignée de main présente toujours le jeton à chaque `do_get`. Un champ `role` à ses côtés *demande* un rôle ; le serveur dérive les rôles autorisés de l'identité et substitue la valeur autorisée, de sorte qu'une chaîne de rôle dans un ticket n'est jamais l'identité elle-même. (REQ-1263) Voir [Security Model](security.md#surfaces-et-identifiants).

```python
ticket = flight.Ticket(json.dumps({
    "query": "SELECT id, amount FROM sales.orders",
    "token": "provisa_pat_...",
    "role": "analyst",
}).encode())
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

Chemin service à service. Provisa génère un `.proto` par rôle au démarrage — chaque rôle ne voit que les tables et colonnes auxquelles il a accès.

Téléchargez le proto pour votre rôle :

```bash
curl http://localhost:8001/proto/analyst > provisa_analyst.proto
```

Utilisez `grpc_server_reflection` pour découvrir le schéma par programmation.

Chaque RPC doit porter un identifiant dans la clé de métadonnées `authorization` — un jeton de fournisseur ou un jeton d'accès personnel. `x-provisa-role` demande un rôle parmi l'ensemble autorisé de l'identité ; ce n'est pas un identifiant et ça ne l'a jamais été. Les certificats client sont pris en charge. Voir [Security Model](security.md#surfaces-et-identifiants).

Les requêtes en streaming émettent un message par ligne ; les mutations sont unaires.

---

## Invoquer des commandes à travers les protocoles

Une **commande** est une fonction suivie ou un webhook enregistré — un élément appelable enregistré dans la couche sémantique de Provisa avec un `kind` (`query` ou `mutation`) et un `impl_kind` qui décrit sa façon de s'exécuter. Chaque surface achemine les invocations à travers un seul exécuteur gouverné (`invoke_tracked_function`) qui applique uniformément `writable_by` et la gouvernance (REQ-1156). [tool-verified: `provisa/api/data/action_exec.py`, `provisa/bolt/session.py:786-791`, `provisa/grpc/server.py:107-135`, `provisa/pgwire/function_call.py:80-88`, `provisa/api/flight/server.py:542-554`]

| `impl_kind` | Ce qui s'exécute | Champs de liaison |
| ------------ | ----------- | --------------- |
| `source_procedure` | Procédure stockée sur une source enregistrée (par défaut) | `sourceId`, `schemaName`, `functionName` |
| `script` | Script côté serveur | `script` |
| `http` | Appel HTTP sortant | `url`, `method` |
| `grpc` | Appel gRPC sortant vers un serveur externe | `target`, `method` |
| `python` | Élément appelable Python hébergé par Provisa (REQ-885) | `callable` (p. ex. `demo.py_functions:random_dataset`) |

Quand une commande déclare un `return_schema` (JSON Schema avec `type: array, items: object`), elle retourne un ensemble — chaque surface la projette comme un jeu de lignes typé. Les commandes de démonstration `random_python_set` (impl_kind `python`) et `random_grpc_set` (impl_kind `grpc`) illustrent à la fois un appelable hébergé et un pont gRPC externe retournant des lignes à valeurs aléatoires ; les deux sont enregistrées dans `config/provisa-install.yaml`. [tool-verified: `config/provisa-install.yaml:809-856`]

### Matrice de protocole

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

### Configuration

Activez la fédération dans `config.yaml` :

```yaml
federation:
  enabled: true
  subgraph_name: provisa-data
```

Provisa génère automatiquement des directives `@key` sur les colonnes de clé primaire et `@external`/`@provides` sur les relations inter-sous-graphes.

### Enregistrement avec Apollo Router

Dans votre `supergraph.yaml` :

```yaml
subgraphs:
  provisa-data:
    routing_url: http://provisa:8001/data/graphql
    schema:
      subgraph_url: http://provisa:8001/data/graphql
```

Exécutez `rover supergraph compose --config supergraph.yaml` pour générer le schéma du supergraphe.

### Entités

Provisa répond aux requêtes `_entities` pour les jointures inter-sous-graphes. Toute table avec une clé primaire est automatiquement résolvable comme une entité Federation.

---

## Import Hasura v2 / DDN

Voir [docs/import.md](import.md) pour la migration depuis Hasura vers Provisa.

---

## Kafka

Voir [docs/sources.md](sources.md#sources-kafka) pour la configuration des topics Kafka comme tables en lecture seule et receveurs de résultats de requête.

---

## Vérificateurs de qualité des données (REQ-1443)

Soda Core et Great Expectations se connectent à Provisa de la même manière que tout autre client postgres — via pgwire. C'est l'intégration entière : le vérificateur détient un seul pilote postgres et balaie la vue fédérée, de sorte qu'une table Snowflake, une table Iceberg et une collection Mongo sont toutes vérifiées par le même dialecte de contrat sans vérificateur par système. [tool-verified: `provisa/events/source_loader.py` `make_dq_loader`]

Le balayage s'exécute dans un interpréteur enfant — `python -m provisa.dq.worker` — qui est le seul endroit où `soda_core` ou `great_expectations` est importé. Rien n'est lié au processus serveur, et un plantage de vérificateur fait tomber un sous-processus plutôt que la boucle d'événements. [tool-verified: `provisa/dq/runner.py` `build_command`]

Les résultats de balayage atterrissent comme des lignes source ordinaires, de sorte que la cadence, la fraîcheur, les événements, la traçabilité, la gouvernance, RLS, la grille et l'export s'appliquent tous sans mécanisme secondaire. La rédaction de contrats, l'enveloppe de résultat et l'enregistrement dérivé sont couverts dans [docs/sources.md](sources.md#verificateurs-de-qualite-des-donnees-req-1443).

### Installer un vérificateur

Aucune des deux bibliothèques n'est livrée par défaut. L'installateur demande laquelle vous voulez, et la réponse devient `dq_checker: none|soda|gx` dans `~/.provisa/config.yaml`. Sur le palier Docker, `scripts/provisa` transforme cela en l'argument de build `PROVISA_EXTRAS` ; sur le palier natif, `first-launch.sh` installe l'extra pyproject correspondant dans le venv. [tool-verified: `scripts/provisa:69-79`, `packaging/linux/first-launch.sh` `_native_extras`]

| `dq_checker` | Bibliothèque | Licence | Plan cloud hébergé |
| -------------- | --------- | --------- | -------------------- |
| `soda` | `soda-postgres` | Elastic License 2.0 | Refusé (`cloud_eligible: false`) |
| `gx` | `great-expectations[postgresql]` | Apache 2.0 | Autorisé |

La Elastic License 2.0 interdit de fournir le logiciel à des tiers en tant que service hébergé, ce que serait l'exécution de Soda à l'intérieur du plan SaaS pour le compte d'un locataire. Un déploiement hébergé qui souhaite Soda pointe vers un endpoint Soda que l'opérateur exécute lui-même. Voir [docs/configuration.md](configuration.md#verificateurs-de-qualite-des-donnees-soda-great_expectations) pour les clés de connexion.

---

## Échange sémantique Apache Ossie (REQ-1316)

Provisa échange des modèles sémantiques avec Apache Ossie (spec 0.2.0.dev0, incubating ; anciennement
Open Semantic Interchange) via un adaptateur de frontière. Le vocabulaire interne de Provisa n'est
jamais renommé vers celui d'Ossie — la spec déclare des changements cassants comme probables, donc
le couplage est confiné à l'adaptateur.
[tool-verified: `provisa/ossie/convert.py` docstring lines 7–16; `OSSIE_VERSION = "0.2.0.dev0"`,
`provisa/ossie/convert.py` line 29]

### Export

La surface d'export canonique est un endpoint HTTP en direct. Elle dérive le document Ossie de
l'état en direct à chaque requête — pas de mise en cache, pas d'étape de génération.

```http
GET /admin/ossie
```

La réponse est un document YAML avec `Content-Disposition: attachment; filename=provisa.ossie.yaml`.
[tool-verified: `ossie_router.py` lines 20–33: "THE canonical live Ossie endpoint: the semantic
model derived from live state on every read — no caching, no regeneration step"]

La page Metrics offre aussi un bouton **Download** et une URL d'endpoint copiable dans le panneau
Ossie Interchange, tous deux pointant vers le même endpoint.
[tool-verified: `OssieInterchangePanel.tsx` lines 64–79: `endpointUrl = window.location.origin + OSSIE_ENDPOINT_PATH`]

#### Ce qui est exporté

L'adaptateur mappe les objets Provisa vers les objets Ossie comme suit :

| Objet Provisa | Objet Ossie | Notes |
| --- | --- | --- |
| `Table` | `dataset` | `source` = `catalog.schema.table` ; clés primaires/uniques depuis la config de colonnes et `UniqueConstraint` |
| `Column` | `field` | `expression` = référence de colonne (dialecte ANSI_SQL) ; les colonnes temporelles gagnent `dimension.is_time: true` |
| `Relationship` | `relationship` | Alias utilisé comme nom quand défini ; les relations calculées (cible-fonction) sont ignorées |
| `Metric` | `metric` | `name`, `expression` (ANSI_SQL), `datatype`, `description`, `ai_context` — sans perte par conception |
| `modeling_role` / `modeling_history` | `custom_extensions[].vendor_name="provisa"` | Aller-retour uniquement ; d'autres outils peuvent l'ignorer |

[tool-verified: `_table_to_dataset`, `build_ossie_model`, `provisa/ossie/convert.py` lines 90–198;
`_table_to_dataset` comment at line 153: "Computed (function-target) relationships have no dataset
target — not representable in Ossie; skipping is the defined export boundary"]

La gouvernance, RLS, la traçabilité, et la sémantique de graphe ne sont pas exportées. Elles peuvent
voyager dans l'emplacement optionnel `provisa` de custom_extensions pour la fidélité d'aller-retour,
mais l'échange ne dépend jamais du fait que d'autres outils le lisent. [tool-verified:
`provisa/ossie/convert.py` docstring lines 13–15]

Les types de colonnes Provisa inconnus passent tels quels ; l'adaptateur ne mappe jamais
silencieusement vers un type erroné. [tool-verified: `_map_datatype`, `provisa/ossie/convert.py`
lines 70–77: "Unknown types pass through verbatim — mapping silently to a wrong type would corrupt
the model"]

#### Correspondance de types

[tool-verified: `_DATATYPE_MAP`, `provisa/ossie/convert.py` lines 35–65]

| Type Provisa / source | `datatype` Ossie |
| --- | --- |
| `varchar`, `text`, `char`, `uuid`, `string` | `string` |
| `int`, `integer`, `bigint`, `smallint`, `int4`, `int8`, `tinyint` | `integer` |
| `numeric`, `decimal`, `float`, `double`, `real` | `number` |
| `bool`, `boolean` | `boolean` |
| `date` | `date` |
| `time` | `time` |
| `timestamp`, `timestamptz`, `datetime` | `timestamp` |
| tout le reste | passé tel quel |

### Import

L'import accepte un document Ossie (YAML ou JSON) et retourne des propositions d'enregistrement.
Rien n'est enregistré automatiquement — les définitions importées ne contournent jamais l'étape de
révision.

```http
POST /admin/ossie/import
Content-Type: text/yaml   (or application/json)

<ossie document>
```

Le serveur analyse le document avec `parse_ossie_model`, qui valide la structure et retourne une
dataclass `OssieImport` contenant les tables, relations, et métriques proposées comme dicts bruts.
Tout problème structurel est un `400` avec une erreur nommant le chemin, p. ex.
`ossie import: missing semantic_model[0].datasets[1].source`.
[tool-verified: `import_ossie`, `provisa/api/admin/ossie_router.py` lines 36–52:
"Nothing is registered here — imported definitions never bypass registration review"]

#### L'écran de révision

Dans l'UI, le bouton **Import** (page Metrics → panneau Ossie Interchange) ouvre un sélecteur de
fichier. Une fois le document soumis et analysé, une fenêtre modale de révision s'ouvre avec chaque
table, relation, et métrique proposée listée comme un élément coché. Le modélisateur peut décocher
n'importe quoi pour l'exclure. Cliquer sur **Apply** enregistre les éléments cochés via les mutations
d'enregistrement existantes — les tables d'abord, puis les relations (qui référencent les tables),
puis les métriques.
[tool-verified: `OssieInterchangePanel.tsx` lines 88–165: "Review screen opens with everything
checked; trimming = unchecking"; "Tables first, then relationships... then metrics — each through
the EXISTING registration mutations (REQ-1316)"]

Le rôle de modélisation et l'historique stockés dans un document Ossie exporté par Provisa font
l'aller-retour correctement à travers l'import. [tool-verified: `_parse_dataset` custom_extensions
handling, `provisa/ossie/convert.py` lines 287–300: "REQ-1320: round-trip the provisa modeling
metadata slot"]

---

## Métriques à travers les protocoles (REQ-1319)

La définition d'une métrique gouvernée — son expression, description, et `ai_context` — voyage avec
la valeur dans chaque surface de requête à travers une seule expansion du compilateur. Il n'y a
aucune copie. Le compilateur réserve le schéma `metrics` pour l'accès SQL ; chaque protocole ajoute
ensuite son propre canal de métadonnées.

[tool-verified: `METRICS_SCHEMA = "metrics"`, `provisa/compiler/metric_expand.py` line 43;
REQ-1319 requirement text: "the definition (description, ai_context) travels with the value
everywhere, with no copies"]

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
sorte que DBeaver et `\d+` de psql les affichent. [tool-verified: `metric_semantic_sql`,
`provisa/compiler/metric_expand.py` lines 52–70; REQ-1319: "description surfaced via pg_description"]

`SELECT *` est rejeté — nommez les colonnes explicitement.
[tool-verified: `expand_metric_query`, `provisa/compiler/metric_expand.py` lines 302–306]

### GraphQL

Les métriques se projettent à l'intérieur du champ racine `_aggregate` comme un bloc `metrics`.
[inferred: per REQ-1319; aggregate_gen.py not read in this session]

Le texte de la définition (`description`, `ai_context`) apparaît dans les documents d'introspection
GraphQL, de sorte que les outils sensibles au schéma et la génération de code le récupèrent
automatiquement.
[inferred: per REQ-1319: "definition in introspection docs"]

### MCP (agents IA)

Deux outils exposent les métriques aux clients MCP :

- **`list_metrics`** — retourne toutes les métriques gouvernées visibles pour la session, avec
  `name`, `description`, et `ai_context`.
- **`query_metric`** — accepte un nom de métrique plus une liste de dimensions et appelle le chemin
  SQL sémantique du compilateur, retournant le résultat agrégé.

[inferred: per REQ-1319: "MCP: list_metrics and query_metric tools carrying ai_context, so agents
select governed meanings instead of composing aggregation SQL"; `provisa/api/mcp/tools.py` not
read in this session]

Les agents qui appellent `list_metrics` avant de construire une requête sélectionnent une métrique
gouvernée par nom plutôt que d'écrire du SQL d'agrégation à la main. Le champ `ai_context` est
l'endroit où placer le texte de définition qui guide une sélection correcte.

### Arrow Flight

Les métriques sont adressables comme des descripteurs de vol de métrique retournant des tables
Arrow.
[inferred: per REQ-1319: "Arrow Flight: metric flight descriptors returning Arrow tables";
`provisa/api/flight/catalog.py` not read in this session]

Utilisez la même forme SQL `metrics.<name>` via le chemin de ticket Flight SQL standard.

### Bolt / Cypher (Neo4j Browser)

Appelez une métrique en utilisant la procédure `provisa.metric()` :

```cypher
CALL provisa.metric('net_revenue', ['region']) YIELD region, value
```

[inferred: per REQ-1319: "Bolt/Cypher: a provisa.metric() procedure"; the procedure signature
is inferred from the REQ text and not verified against provisa/bolt/session.py in this session]

Les tables Fact et Dimension portent des labels de nœud `:Fact` et `:Dimension` dans le graphe
fédéré, de sorte que Bloom rend automatiquement la forme en étoile.
[inferred: per REQ-1319 and REQ-1320: "federated graph labels nodes :Fact/:Dimension so Bloom
renders the star"; provisa/cypher/label_map.py not read in this session]

### Requêtes en langage naturel

Le matcher de schéma NL résout le vocabulaire de métrique dans les questions en langage naturel
directement vers une métrique plus des dimensions, puis génère du SQL sémantique. [tool-verified:
`resolve_metric`, `provisa/nl/schema_matcher.py` is exercised in `test_nl_metrics.py` lines 76–78:
`sql = matcher.resolve_metric("What is the total revenue by region?")` →
`"SELECT region, value FROM metrics.total_revenue GROUP BY region"`]

Les tables de fait sont taguées `[fact]` dans le prompt NL ; les tables de dimension sont taguées
`[dimension]`. Le matcher favorise les chemins de jointure fait-vers-dimension lors de la
résolution des questions.
[tool-verified: `test_format_entities_tags_star_roles`, `tests/unit/test_nl_metrics.py` lines 129–132:
`assert "table: orders [fact]  fields: amount" in block`]

### Streaming

Combinez `view_metrics` avec `materialize` et un receveur Kafka pour produire une sortie de métrique
push-on-change en utilisant la machinerie de matérialisation existante. Aucun nouveau pipeline
n'est requis.
[inferred: per REQ-1319: "Streaming: view_metrics + materialize + Kafka sink yields push-on-change
metrics from existing machinery"; implementation not verified beyond the requirement text]

### Observabilité (OTel)

Les évaluations de métriques sont tracées et exportables comme métriques OpenTelemetry.
[inferred: per REQ-1319: "Observability: metric evaluations traced and exportable as OTel metrics";
OTel integration code not read in this session]
