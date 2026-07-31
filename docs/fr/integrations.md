# Intégrations

## Choisir une voie de connexion

| Type de client | Voie recommandée | Pourquoi |
|-------------|-----------------|-----|
| Outils de BI (Tableau, Power BI, Looker) | JDBC | Streaming en colonnes Arrow Flight sur le réseau ; les outils de BI disposent d'un assistant JDBC intégré et profitent de la livraison en colonnes à haut débit pour les grands ensembles de résultats |
| psql, DBeaver, tout outil compatible PG | pgwire (driver PG natif) | Option par défaut sans friction — aucun driver personnalisé requis ; utilisez ce que vous avez déjà |
| Pile de données Python (pandas, pyarrow) | `provisa-client` ou ADBC brut | Lots Arrow en streaming ; aucune surcharge de sérialisation ligne par ligne |
| Spark, DuckDB, pipelines à haut débit | Arrow Flight (ADBC) | Streaming en colonnes non borné directement vers la mémoire Arrow |
| Service à service (contrats typés) | Protobuf gRPC | Proto généré par rôle ; lignes en streaming ; sécurité des types |
| Applications web, scripting | HTTP (`/data/graphql`, `/data/sql`) | Aucun driver ; HTTP standard ; choix complet du langage de requête |
| Clients REST (standard JSON:API) | `GET /data/jsonapi/{table}` | Enveloppe JSON:API v1.0 ; ensembles de champs partiels, pagination, filtrage via des paramètres de requête ; aucun driver |

---

## pgwire — Driver PostgreSQL natif

Provisa implémente le protocole réseau de PostgreSQL (version de protocole 3.0). Tout client qui parle PostgreSQL se connecte sans driver personnalisé.

Activez-le en définissant `PROVISA_PGWIRE_PORT` (par exemple `5433`) avant de démarrer Provisa. Désactivé si non défini ou égal à `0`.

### Pourquoi pgwire plutôt que JDBC ?

Le driver JDBC utilise Arrow Flight comme transport et nécessite le déploiement de `provisa-jdbc.jar`. pgwire ne nécessite rien — si vous disposez déjà de `psql`, DBeaver, SQLAlchemy ou d'un driver JDBC PG, c'est prêt. C'est la voie la moins contraignante pour les charges de travail purement SQL.

JDBC est le bon choix pour les outils de BI dotés d'un assistant de connexion JDBC intégré et qui profitent du streaming en colonnes d'Arrow Flight pour les grands ensembles de résultats. pgwire accepte du SQL libre sur l'ensemble du schéma publié — les mêmes requêtes, un coût de configuration plus faible.

### psql

```bash
psql -h localhost -p 5433 -U alice
```

### DBeaver

1. Nouvelle connexion → PostgreSQL
2. Hôte : `localhost`, Port : `5433`
3. Nom d'utilisateur / mot de passe tels que configurés dans Provisa
4. Aucun téléchargement de driver supplémentaire requis

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

pgwire utilise une authentification par mot de passe en clair reliée au fournisseur d'authentification configuré de Provisa (`none` ou `simple`). En mode de confiance (`none`), le nom d'utilisateur est directement mappé à un rôle — le mot de passe est ignoré. MD5 n'est pas pris en charge ; activez TLS (`PROVISA_PGWIRE_CERT` / `PROVISA_PGWIRE_KEY`) lors d'une utilisation sur un réseau non fiable.

### Limitations

- SQL uniquement. GraphQL et Cypher ne sont pas acceptés via pgwire.
- Ce n'est pas en lecture seule. `COPY ... FROM STDIN` insère des lignes dans les sources `postgresql`, `mysql`, `sqlite` et `mariadb`, et le DDL est pris en charge (voir ci-dessous).
- Le DDL (`CREATE`, `ALTER`, `DROP`) est pris en charge et redirigé vers la voie Trino ou directe ; la nouvelle table est enregistrée dans le contexte de compilation et devient immédiatement interrogeable. `COPY ... TO STDOUT` (export) et `COPY ... FROM STDIN` (import) sont pris en charge dans les formats `text` et `csv`.
- Les requêtes `information_schema` et `pg_catalog` sont interceptées et servies depuis une couche d'émulation de catalogue DuckDB — les outils de découverte de schéma fonctionnent correctement.

---

## Driver JDBC

Le driver JDBC de Provisa utilise Arrow Flight comme transport sous-jacent. C'est la voie recommandée pour les outils de BI disposant d'un assistant de connexion JDBC.

### Connexion

Téléchargez [provisa-jdbc.jar](https://provisa.dev/dl/jdbc) (toujours la dernière version) et ajoutez-le au chemin de drivers de votre outil.

URL JDBC :
```
jdbc:provisa://<host>:8815
```

L'authentification utilise les propriétés JDBC standard `user` / `password`. Provisa authentifie les identifiants auprès du fournisseur d'authentification configuré et attribue le rôle — le client ne choisit pas son propre rôle.

### Configuration des outils de BI

**Tableau**
1. Gérer → Drivers → Installer Provisa JDBC
2. Connexion → Autres bases de données (JDBC)
3. URL : `jdbc:provisa://localhost:8815`
4. Saisissez votre nom d'utilisateur et votre mot de passe lorsque vous y êtes invité

**DBeaver** (voie JDBC — pour la voie pgwire, voir ci-dessus)
1. Base de données → Nouvelle connexion → JDBC
2. Driver : ajoutez `provisa-jdbc.jar`
3. URL : `jdbc:provisa://localhost:8815`
4. Saisissez votre nom d'utilisateur et votre mot de passe dans l'onglet Authentification

**Power BI** — utilisez la passerelle ODBC avec le pont Provisa JDBC-ODBC (inclus dans le programme d'installation).

---

## Clients Arrow Flight

Arrow Flight (port 8815) est la voie recommandée pour les outils de données qui le prennent en charge. Les résultats circulent en streaming sous forme de RecordBatches Arrow sans être matérialisés dans la mémoire de Provisa.

### Python (`provisa-client`)

La voie Python recommandée — encapsule à la fois GraphQL et Arrow Flight :

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

Consultez [docs/python-client.md](python-client.md) pour la référence complète, y compris DB-API 2.0, le dialecte SQLAlchemy et ADBC.

### Python (PyArrow brut)

```python
import pyarrow.flight as flight

client = flight.connect("grpc://localhost:8815")
ticket = flight.Ticket(b'{"query": "SELECT id, amount FROM sales.orders"}')
df = client.do_get(ticket).read_all().to_pandas()
```

Le ticket ne transporte aucun rôle. Le serveur attribue le rôle à partir du fournisseur d'authentification configuré. Lorsque la sélection de rôle est autorisée, transmettez-la dans les métadonnées de l'appel gRPC sous la clé `x-provisa-role` (par exemple `flight.FlightCallOptions(headers=[(b"x-provisa-role", b"analyst")])`), et non dans le JSON du ticket.

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

Voie de service à service. Provisa génère un `.proto` par rôle au démarrage — chaque rôle ne voit que les tables et colonnes auxquelles il a accès.

Téléchargez le proto de votre rôle :

```bash
curl http://localhost:8001/proto/analyst > provisa_analyst.proto
```

Utilisez `grpc_server_reflection` pour découvrir le schéma par programmation.

Le rôle est transmis via la clé de métadonnées `x-provisa-role` sur chaque RPC. Les requêtes en streaming émettent un message par ligne ; les mutations sont unaires.

---

## Invoquer des commandes entre protocoles

Une **commande** est une fonction suivie enregistrée ou un webhook — un élément invocable enregistré dans la couche sémantique de Provisa avec un `kind` (`query` ou `mutation`) et un `impl_kind` qui décrit son mode d'exécution. Chaque surface achemine les invocations via un unique exécuteur gouverné (`invoke_tracked_function`) qui applique `writable_by` et la gouvernance de manière uniforme (REQ-1156). [tool-verified: `provisa/api/data/action_exec.py`, `provisa/bolt/session.py:786-791`, `provisa/grpc/server.py:107-135`, `provisa/pgwire/function_call.py:80-88`, `provisa/api/flight/server.py:542-554`]

| `impl_kind` | Ce qui s'exécute | Champs de liaison |
|------------|-----------|---------------|
| `source_procedure` | Procédure stockée sur une source enregistrée (par défaut) | `sourceId`, `schemaName`, `functionName` |
| `script` | Script côté serveur | `script` |
| `http` | Appel HTTP sortant | `url`, `method` |
| `grpc` | Appel gRPC sortant vers un serveur externe | `target`, `method` |
| `python` | Élément invocable Python hébergé par Provisa (REQ-885) | `callable` (par exemple `demo.py_functions:random_dataset`) |

Lorsqu'une commande déclare un `return_schema` (JSON Schema avec `type: array, items: object`), elle retourne un ensemble — chaque surface la projette alors comme un ensemble de lignes typé. Les commandes de démonstration `random_python_set` (impl_kind `python`) et `random_grpc_set` (impl_kind `grpc`) illustrent à la fois un élément invocable hébergé et un pont gRPC externe renvoyant des lignes à valeurs aléatoires ; les deux sont enregistrées dans `config/provisa-install.yaml`. [tool-verified: `config/provisa-install.yaml:809-856`]

### Matrice des protocoles

| Surface | Syntaxe | Exemple |
|---------|--------|---------|
| GraphQL | `kind=query` → champ Query ; `kind=mutation` → champ Mutation ; préfixé par domaine lorsque `domain_prefix: true` | `{ ps__random_python_set(rows: 5, seed: 42) { id region amount } }` |
| pgwire / Arrow Flight / MCP `run_sql` | `SELECT * FROM fn(args)` ou `SELECT fn(args)` | `SELECT * FROM random_python_set(5, 42)` |
| Cypher HTTP (`POST /data/cypher`) | `CALL fn(args) YIELD cols` | `CALL random_python_set(5, 42) YIELD id, region, amount` |
| Bolt (Neo4j Browser / driver) | `CALL fn(args)` — les arguments positionnels correspondent aux noms d'arguments déclarés | `CALL random_python_set(3, 7)` |
| Provisa gRPC (port 50051) | Unaire `CallCommand(CommandRequest{name, args_json})` → `CommandResponse{rows_json}` | `grpcurl -d '{"name":"random_python_set","args_json":"{\"rows\":5}"}' ... ProvisaService/CallCommand` |

Le champ `kind` contrôle uniquement le placement dans GraphQL — les surfaces SQL, Cypher, Bolt et gRPC acceptent les commandes `query` et `mutation` de manière identique.

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

Provisa génère automatiquement des directives `@key` sur les colonnes de clé primaire et `@external`/`@provides` sur les relations entre sous-graphes.

### Enregistrement auprès d'Apollo Router

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

Provisa répond aux requêtes `_entities` pour les jointures entre sous-graphes. Toute table dotée d'une clé primaire est automatiquement résoluble en tant qu'entité Federation.

---

## Import Hasura v2 / DDN

Consultez [docs/import.md](import.md) pour la migration de Hasura vers Provisa.

---

## Kafka

Consultez [docs/sources.md](sources.md#sources-kafka) pour la configuration des topics Kafka en tant que tables en lecture seule et récepteurs de résultats de requête.

---

## Interopérabilité sémantique Apache Ossie (REQ-1316)

Provisa échange des modèles sémantiques avec Apache Ossie (spécification 0.2.0.dev0, en incubation ;
anciennement Open Semantic Interchange) via un adaptateur de frontière. Le vocabulaire interne de
Provisa n'est jamais renommé selon celui d'Ossie — la spécification indique que des changements
incompatibles sont probables, le couplage est donc confiné à l'adaptateur.
[tool-verified: `provisa/ossie/convert.py` docstring lines 7–16; `OSSIE_VERSION = "0.2.0.dev0"`,
`provisa/ossie/convert.py` line 29]

### Export

La surface d'export canonique est un endpoint HTTP en direct. Elle dérive le document Ossie de
l'état en direct à chaque requête — sans cache, sans étape de génération.

```
GET /admin/ossie
```

La réponse est un document YAML avec `Content-Disposition: attachment; filename=provisa.ossie.yaml`.
[tool-verified: `ossie_router.py` lines 20–33: "THE canonical live Ossie endpoint: the semantic
model derived from live state on every read — no caching, no regeneration step"]

La page Métriques propose également un bouton **Télécharger** et une URL d'endpoint copiable dans le
panneau d'interopérabilité Ossie, tous deux pointant vers le même endpoint.
[tool-verified: `OssieInterchangePanel.tsx` lines 64–79: `endpointUrl = window.location.origin + OSSIE_ENDPOINT_PATH`]

#### Ce qui est exporté

L'adaptateur mappe les objets Provisa vers les objets Ossie de la manière suivante :

| Objet Provisa | Objet Ossie | Notes |
| --- | --- | --- |
| `Table` | `dataset` | `source` = `catalog.schema.table` ; clés primaires/uniques issues de la configuration des colonnes et de `UniqueConstraint` |
| `Column` | `field` | `expression` = référence de colonne (dialecte ANSI_SQL) ; les colonnes temporelles obtiennent `dimension.is_time: true` |
| `Relationship` | `relationship` | L'alias est utilisé comme nom lorsqu'il est défini ; les relations calculées (cible-fonction) sont ignorées |
| `Metric` | `metric` | `name`, `expression` (ANSI_SQL), `datatype`, `description`, `ai_context` — sans perte par conception |
| `modeling_role` / `modeling_history` | `custom_extensions[].vendor_name="provisa"` | Aller-retour uniquement ; d'autres outils peuvent l'ignorer |

[tool-verified: `_table_to_dataset`, `build_ossie_model`, `provisa/ossie/convert.py` lines 90–198;
`_table_to_dataset` comment at line 153: "Computed (function-target) relationships have no dataset
target — not representable in Ossie; skipping is the defined export boundary"]

La gouvernance, la sécurité au niveau des lignes, la traçabilité et la sémantique de graphe ne sont
pas exportées. Elles peuvent transiter par l'emplacement optionnel `provisa` de custom_extensions
pour la fidélité de l'aller-retour, mais l'interopérabilité ne dépend jamais du fait que d'autres
outils la lisent. [tool-verified: `provisa/ossie/convert.py` docstring lines 13–15]

Les types de colonnes Provisa inconnus passent tels quels ; l'adaptateur ne mappe jamais
silencieusement vers un type incorrect. [tool-verified: `_map_datatype`, `provisa/ossie/convert.py` lines 70–77: "Unknown types
pass through verbatim — mapping silently to a wrong type would corrupt the model"]

#### Correspondance des types

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
| tout autre | passé tel quel |

### Import

L'import accepte un document Ossie (YAML ou JSON) et renvoie des propositions d'enregistrement. Rien
n'est enregistré automatiquement — les définitions importées ne contournent jamais l'étape de révision.

```http
POST /admin/ossie/import
Content-Type: text/yaml   (or application/json)

<ossie document>
```

Le serveur analyse le document avec `parse_ossie_model`, qui valide la structure et renvoie une
classe de données `OssieImport` contenant les tables, relations et métriques proposées sous forme de
dictionnaires simples. Tout problème structurel entraîne une erreur `400` avec un chemin nommé, par
exemple `ossie import: missing semantic_model[0].datasets[1].source`.
[tool-verified: `import_ossie`, `provisa/api/admin/ossie_router.py` lines 36–52:
"Nothing is registered here — imported definitions never bypass registration review"]

#### L'écran de révision

Dans l'interface, le bouton **Importer** (page Métriques → panneau d'interopérabilité Ossie) ouvre un
sélecteur de fichiers. Une fois le document publié et analysé, une fenêtre modale de révision s'ouvre
avec chaque table, relation et métrique proposée listée comme élément coché. Le modélisateur peut
décocher n'importe quel élément pour l'exclure. En cliquant sur **Appliquer**, les éléments cochés
sont enregistrés via les mutations d'enregistrement existantes — d'abord les tables, puis les
relations (qui référencent les tables), puis les métriques.
[tool-verified: `OssieInterchangePanel.tsx` lines 88–165: "Review screen opens with everything
checked; trimming = unchecking"; "Tables first, then relationships... then metrics — each through
the EXISTING registration mutations (REQ-1316)"]

Le rôle de modélisation et l'historique stockés dans un document Ossie exporté par Provisa font
correctement l'aller-retour lors de l'import. [tool-verified: `_parse_dataset` custom_extensions handling,
`provisa/ossie/convert.py` lines 287–300: "REQ-1320: round-trip the provisa modeling metadata slot"]

---

## Métriques entre protocoles (REQ-1319)

La définition d'une métrique gouvernée — son expression, sa description et son `ai_context` —
accompagne la valeur vers chaque surface de requête via une unique expansion du compilateur. Il n'y
a aucune copie. Le compilateur réserve le schéma `metrics` pour l'accès SQL ; chaque protocole ajoute
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

Le compilateur développe la forme `metrics.<name>` en l'agrégat groupé réel avant l'exécution de la
gouvernance. Les descriptions de colonnes apparaissent sous forme d'entrées `pg_description`, de
sorte que DBeaver et `\d+` de psql les affichent. [tool-verified: `metric_semantic_sql`, `provisa/compiler/metric_expand.py` lines 52–70;
REQ-1319: "description surfaced via pg_description"]

`SELECT *` est rejeté — nommez les colonnes explicitement.
[tool-verified: `expand_metric_query`, `provisa/compiler/metric_expand.py` lines 302–306]

### GraphQL

Les métriques sont projetées à l'intérieur du champ racine `_aggregate` comme un bloc `metrics`.
[inferred: per REQ-1319; aggregate_gen.py not read in this session]

Le texte de la définition (`description`, `ai_context`) apparaît dans la documentation
d'introspection GraphQL, de sorte que les outils sensibles au schéma et la génération de code le
récupèrent automatiquement.
[inferred: per REQ-1319: "definition in introspection docs"]

### MCP (agents IA)

Deux outils exposent les métriques aux clients MCP :

- **`list_metrics`** — renvoie toutes les métriques gouvernées visibles pour la session, avec `name`,
  `description` et `ai_context`.
- **`query_metric`** — accepte un nom de métrique ainsi qu'une liste de dimensions et appelle la
  voie SQL sémantique du compilateur, renvoyant le résultat agrégé.

[inferred: per REQ-1319: "MCP: list_metrics and query_metric tools carrying ai_context, so agents
select governed meanings instead of composing aggregation SQL"; `provisa/api/mcp/tools.py` not
read in this session]

Les agents qui appellent `list_metrics` avant de construire une requête sélectionnent une métrique
gouvernée par son nom plutôt que d'écrire du SQL d'agrégation à la main. Le champ `ai_context` est
l'endroit où placer le texte de définition qui guide une sélection correcte.

### Arrow Flight

Les métriques sont adressables sous forme de descripteurs de vol de métrique renvoyant des tables
Arrow.
[inferred: per REQ-1319: "Arrow Flight: metric flight descriptors returning Arrow tables";
`provisa/api/flight/catalog.py` not read in this session]

Utilisez la même forme SQL `metrics.<name>` via la voie standard de ticket Flight SQL.

### Bolt / Cypher (Neo4j Browser)

Appelez une métrique à l'aide de la procédure `provisa.metric()` :

```cypher
CALL provisa.metric('net_revenue', ['region']) YIELD region, value
```

[inferred: per REQ-1319: "Bolt/Cypher: a provisa.metric() procedure"; the procedure signature
is inferred from the REQ text and not verified against provisa/bolt/session.py in this session]

Les tables de faits et de dimensions portent les étiquettes de nœud `:Fact` et `:Dimension` dans le
graphe fédéré, de sorte que Bloom affiche automatiquement la forme en étoile.
[inferred: per REQ-1319 and REQ-1320: "federated graph labels nodes :Fact/:Dimension so Bloom
renders the star"; provisa/cypher/label_map.py not read in this session]

### Requêtes en langage naturel

Le comparateur de schéma en langage naturel résout le vocabulaire des métriques dans les questions en
langage naturel directement vers une métrique et des dimensions, puis génère du SQL sémantique. [tool-verified: `resolve_metric`,
`provisa/nl/schema_matcher.py` is exercised in `test_nl_metrics.py` lines 76–78:
`sql = matcher.resolve_metric("What is the total revenue by region?")` →
`"SELECT region, value FROM metrics.total_revenue GROUP BY region"`]

Les tables de faits sont étiquetées `[fact]` dans l'invite en langage naturel ; les tables de
dimension sont étiquetées `[dimension]`. Le comparateur privilégie les chemins de jointure de fait
vers dimension lors de la résolution des questions.
[tool-verified: `test_format_entities_tags_star_roles`, `tests/unit/test_nl_metrics.py` lines 129–132:
`assert "table: orders [fact]  fields: amount" in block`]

### Streaming

Combinez `view_metrics` avec `materialize` et un récepteur Kafka pour produire une sortie de métrique
de type push-on-change en utilisant la machinerie de matérialisation existante. Aucun nouveau
pipeline n'est requis.
[inferred: per REQ-1319: "Streaming: view_metrics + materialize + Kafka sink yields push-on-change
metrics from existing machinery"; implementation not verified beyond the requirement text]

### Observabilité (OTel)

Les évaluations de métriques sont tracées et exportables sous forme de métriques OpenTelemetry.
[inferred: per REQ-1319: "Observability: metric evaluations traced and exportable as OTel metrics";
OTel integration code not read in this session]
