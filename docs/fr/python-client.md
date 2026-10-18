# Client Python (`provisa-client`) {: #python-client-provisa-client }

Client Python pour Provisa. Fournit quatre interfaces :

| Interface | Cas d'usage |
| ----------- | ---------- |
| `ProvisaClient` | Requêtes GraphQL, Arrow Flight, sortie DataFrame |
| DB-API 2.0 (`connect`) | Interface de base de données Python standard (PEP 249) (REQ-268) |
| Dialecte SQLAlchemy | Outils BI, ORM, `read_sql` Pandas (REQ-270) |
| ADBC | Streaming columnaire natif Arrow via Flight (REQ-271) |

## Installation {: #install }

```bash
pip install provisa-client                      # core (ProvisaClient + DB-API)
pip install "provisa-client[pandas]"            # adds pandas
pip install "provisa-client[sqlalchemy]"        # adds SQLAlchemy dialect
pip install "provisa-client[adbc]"              # adds ADBC over Arrow Flight
```

---

## ProvisaClient

### Démarrage rapide {: #quick-start }

```python
from provisa_client import ProvisaClient

client = ProvisaClient(
    "http://localhost:8001",
    token="provisa_pat_...",   # personal access token, or a provider bearer token
    role="analyst",
)
```

`ProvisaClient` prend une credential, pas un nom d'utilisateur et un mot de passe : il ne comporte aucune étape de connexion propre. Un jeton d'accès personnel est la credential à privilégier lorsqu'un script doit s'exécuter sans surveillance — il est émis depuis le profil de l'utilisateur lui-même, porte une expiration et se révoque sans toucher au compte. (REQ-1263) Un jeton bearer du fournisseur fonctionne à l'identique. L'un comme l'autre va dans `token`, et le client le présente aussi bien sur le chemin HTTP que sur celui d'Arrow Flight.

Pour échanger un mot de passe contre un jeton, faites un POST sur `/auth/login` et lisez `access_token` :

```python
import httpx

body = httpx.post(
    "http://localhost:8001/auth/login",
    json={"username": "alice", "password": "secret"},
).json()
client = ProvisaClient("http://localhost:8001", token=body["access_token"])
```

Les points d'entrée DB-API et ADBC effectuent cet échange pour vous — voir plus bas.

### Requêtes GraphQL {: #graphql-queries }

```python
# Raw response dict
result = client.query("{ orders { id amount region } }")

# With variables
result = client.query(
    "query Q($region: String!) { orders(region: $region) { id amount } }",
    variables={"region": "west"},
)

# pandas DataFrame (first root field is flattened)
df = client.query_df("{ orders { id amount region } }")
```

### Asynchrone {: #async }

```python
result = await client.aquery("{ orders { id amount } }")
```

### Arrow Flight (columnaire haut débit) {: #arrow-flight-high-throughput-columnar }

Utilisez Flight pour les grands ensembles de résultats — les données sont diffusées en flux sous
forme de lots d'enregistrements Arrow sans être matérialisées côté serveur. (REQ-143, REQ-145)

```python
import pyarrow as pa

table: pa.Table = client.flight("{ orders { id amount region } }")
df = client.flight_df("{ orders { id amount region } }")
```

Flight se connecte au port 8815 par défaut. (REQ-143) Remplacez avec `flight_port=` :

```python
client = ProvisaClient("http://prod.example.com", flight_port=8815)
```

### Exploration du catalogue {: #catalog-exploration }

```python
tables_df = client.list_tables()
```

### Référence de connexion {: #connection-reference }

| Paramètre | Défaut | Description |
| ----------- | --------- | ------------- |
| `url` | `http://localhost:8001` | URL de base du serveur Provisa |
| `token` | `None` | Credential bearer — un jeton du fournisseur ou un jeton d'accès personnel ; à omettre pour l'authentification par mot de passe (REQ-606, REQ-1263) |
| `role` | `None` | Rôle sous lequel agir, envoyé dans `X-Provisa-Role` et honoré seulement s'il est attribué à l'utilisateur ; s'il est omis, la requête s'exécute sous le rôle propre de l'identité (REQ-273) |
| `flight_port` | `8815` | Port gRPC d'Arrow Flight (REQ-143) |

### Gestion des erreurs {: #error-handling }

`query()` lève `httpx.HTTPStatusError` en cas d'erreur HTTP. (REQ-607)
`query_df()` lève `RuntimeError` si la réponse contient des erreurs GraphQL. (REQ-607)

---

## Utiliser Provisa dans Jupyter {: #using-provisa-in-jupyter }

Installez dans le noyau en cours d'exécution avant d'importer. L'extra `pandas` couvre la sortie en DataFrame ; ajoutez `sqlalchemy` si vous avez aussi besoin de `pandas.read_sql` via le dialecte.

```python
%pip install "provisa-client[pandas]"
```

### Identifiants {: #credentials }

Les notebooks enregistrent la sortie des cellules dans le fichier `.ipynb`. Un jeton saisi dans une cellule sera enregistré sur le disque avec votre code. Ne le faites pas.

`ProvisaClient` ne lit aucun environnement : transmettez-lui ses identifiants explicitement, à partir de variables d'environnement. (`provisa_client.query()`, ci-dessous, les lit pour vous.)

```python
import os
from provisa_client import ProvisaClient

client = ProvisaClient(
    os.environ["PROVISA_URL"],           # choose your own variable name
    token=os.environ["PROVISA_TOKEN"],   # personal access token
    role=os.environ.get("PROVISA_ROLE"), # optional; omit to run as your own role
    org=os.environ.get("PROVISA_ORG"),   # required on a multi-tenant deployment; None otherwise
)
```

Définissez ces variables dans votre shell avant de lancer Jupyter, ou chargez un fichier `.env` avec `python-dotenv`. Dans les deux cas, gardez-les hors du notebook.

### Un seul appel : `query()` {: #one-call-query }

`provisa_client.query()` prend une instruction SQL, GraphQL ou Cypher et renvoie un DataFrame pandas. Le serveur détermine de quel langage il s'agit. Rien n'est construit au préalable : la connexion vient de l'environnement ou d'un fichier de profil.

```python
import provisa_client

df = provisa_client.query("SELECT region, amount FROM shop.orders")
admin_view = provisa_client.query("SELECT region, amount FROM shop.orders", role="org_admin")
```

`role=` agit sous ce rôle pour une lecture, envoyé comme `X-Provisa-Role`. Le serveur ne l'honore que pour un rôle que vous détenez. La lecture passe par Arrow Flight ; mettez `transport` à `http` pour l'envoyer à `/data/query`. `await provisa_client.aquery(...)` en est la forme attendable.

La connexion est lue au premier endroit qui nomme chaque réglage :

1. les mots-clés de `provisa_client.sign_in(...)`, qui résout la connexion une fois pour toutes les lectures suivantes ;
2. les variables d'environnement : `PROVISA_URL`, `PROVISA_TOKEN`, `PROVISA_USERNAME`, `PROVISA_PASSWORD`, `PROVISA_ROLE`, `PROVISA_ORG`, `PROVISA_FLIGHT_PORT`, `PROVISA_TRANSPORT` ;
3. une section de `~/.provisa/credentials` : `[default]`, ou celle que nomme `PROVISA_PROFILE` (ou `sign_in(profile=...)`).

```ini
[default]
url = https://provisa.example.com
token = provisa_pat_...
role = analyst

[staging]
url = https://staging.provisa.example.com
username = ana
```

Un `token` est un jeton d'accès personnel ou un jeton de session. Donnez plutôt un `username` et un `password`, et le client les échange contre un jeton de session à `POST /auth/login`. Cela fonctionne sur un déploiement dont le fournisseur de connexion accepte un mot de passe (basic, simple, LDAP). Gardez le mot de passe dans `PROVISA_PASSWORD` plutôt que dans le fichier. Une connexion refusée, une URL absente ou un profil nommé absent du fichier lève une erreur avec sa raison. Provisa n'a aucun parcours de connexion qu'un notebook puisse piloter dans un navigateur ; pour les déploiements à authentification unique, utilisez donc un jeton d'accès personnel.

### Magics de cellule {: #cell-magics }

```python
%pip install "provisa-client[notebook]"
%load_ext provisa_client.magics
```

```text
%%sql eu --role analyst_eu
SELECT region, customer_email FROM shop.orders
```

`%%sql`, `%%graphql` et `%%cypher` répondent à la cellule avec `query()`. Le DataFrame est le résultat de la cellule. Un nom après le magic le lie aussi dans le notebook (`eu` ci-dessus) ; `--role` agit sous ce rôle pour la cellule.

JupySQL (`%load_ext sql`) définit aussi un `%%sql`. Chargez les deux et `%%sql` appartient à l'extension chargée en dernier. Celle de Provisa accepte `--role` et lit via Arrow Flight. JupySQL fonctionne aussi, via le dialecte SQLAlchemy : `%sql provisa+http://user:password@host:port` se connecte, `%sql`/`%%sql` exécutent des instructions gouvernées, et `%sqlcmd tables -s <domain>` et `%sqlcmd columns -t <table> -s <domain>` listent le catalogue.

### Dans le navigateur (Pyodide) {: #in-the-browser-pyodide }

Sous Pyodide, `query()` lit sur le serveur qui a servi la page, en tant qu'utilisateur connecté de la page. La page hôte garde cet utilisateur dans la portée globale du noyau :

```javascript
self.provisaAuth = { baseUrl: "", role: "analyst_eu", bearer: "<token>", org: null };
```

`baseUrl` est vide pour l'origine même de la page. Le client lit `provisaAuth` à chaque requête, de sorte qu'un jeton renouvelé prend effet sans redémarrage. Les résultats arrivent en Arrow quand `pyarrow` est chargé ; sans lui, ils arrivent en JSON. Une instruction Cypher revient toujours en JSON. `query()` est bloquant, ce qu'un Web Worker (où tourne le noyau d'un notebook) peut faire ; sur le thread principal de la page, il lève une erreur et c'est `await provisa_client.aquery(...)` qu'il faut appeler.

### DataFrames depuis GraphQL, SQL et Cypher {: #dataframes-from-graphql-sql-and-cypher }

`query_df()` accepte les trois langages de requête et renvoie un DataFrame pandas.

```python
# GraphQL — first root field is extracted automatically
df = client.query_df("{ orders { id amount region } }")

# SQL
df = client.query_df("SELECT id, amount, region FROM orders WHERE region = 'west'")

# Cypher
df = client.query_df("MATCH (o:Order) RETURN o.id, o.amount, o.region")
```

Les réponses GraphQL portent les données sous un nom de champ racine ; `query_df()` les extrait. Les réponses SQL et Cypher portent des noms de colonne explicites venant du serveur et sont mappées directement.

### Arrow Flight pour les gros volumes {: #arrow-flight-for-large-pulls }

`flight_df()` transmet les données en lots d'enregistrements Arrow au lieu de désérialiser du JSON. Pour des tables larges ou des résultats de plus de quelques centaines de milliers de lignes, c'est plus rapide et moins gourmand en mémoire.

```python
df = client.flight_df("SELECT * FROM events")
```

Arrow préserve les types de colonne : les entiers restent des entiers, les horodatages restent des horodatages. Pas JSON. Cela suffit à préférer Flight quand le schéma compte.

Flight se connecte par défaut au port 8815. Passez `flight_port=` au constructeur de `ProvisaClient` si votre serveur utilise un autre port.

### `pandas.read_sql` via le dialecte SQLAlchemy {: #pandasread_sql-through-the-sqlalchemy-dialect }

Installez l'extra `sqlalchemy` dans le noyau :

```python
%pip install "provisa-client[sqlalchemy]"
```

Créez ensuite un engine avec une URL `provisa+http://`. Mettez les identifiants dans des variables d'environnement, pas dans l'URL.

```python
import os
import pandas as pd
from sqlalchemy import create_engine

host = os.environ["PROVISA_HOST"]       # e.g. "localhost:8001"
user = os.environ["PROVISA_USER"]
pwd  = os.environ["PROVISA_PASSWORD"]

engine = create_engine(f"provisa+http://{user}:{pwd}@{host}")

df = pd.read_sql("SELECT id, amount FROM orders", engine)
```

Le dialecte envoie le nom d'utilisateur et le mot de passe à `/auth/login` et garde le jeton obtenu pendant la durée de vie de l'engine : les identifiants ne sont pas retransmis à chaque requête.

---

## DB-API 2.0

Interface [PEP 249](https://peps.python.org/pep-0249/) standard. (REQ-268) Fonctionne avec tout
outil acceptant une connexion DB-API.

```python
from provisa_client import connect

conn = connect(
    "http://localhost:8001",
    username="alice",
    password="secret",
    role="analyst",     # optional; omit to run as the role the login returns
)
```

`connect` envoie le nom d'utilisateur et le mot de passe en POST à `/auth/login` et conserve l'`access_token` renvoyé : la connexion porte donc une vraie credential plutôt qu'un nom. `role` *demande* un rôle et le serveur ne l'honore que si l'identité le possède (REQ-273) ; omis, la connexion s'exécute avec le rôle résolu par la connexion.

### Exécuter des requêtes {: #executing-queries }

Le curseur accepte GraphQL ou SQL — détecté automatiquement. (REQ-268, REQ-274)

```python
cur = conn.cursor()

# GraphQL
cur.execute("{ orders { id amount region } }")
rows = cur.fetchall()           # list of tuples
one  = cur.fetchone()           # single tuple or None
many = cur.fetchmany(size=50)   # up to N tuples

# SQL (routed through Stage 2 governance)
cur.execute("SELECT id, amount FROM orders WHERE region = 'west'")
rows = cur.fetchall()
```

### Métadonnées de colonnes {: #column-metadata }

```python
cur.execute("{ orders { id amount } }")
print(cur.description)
# [('id', None, ...), ('amount', None, ...)]
print(cur.rowcount)
```

### Paramètres nommés {: #named-parameters }

```python
cur.execute(
    "SELECT * FROM orders WHERE region = :region",
    {"region": "west"},
)
```

### Gestionnaires de contexte {: #context-managers }

```python
with connect("http://localhost:8001", username="alice", password="secret") as conn:
    with conn.cursor() as cur:
        cur.execute("{ orders { id amount } }")
        print(cur.fetchall())
```

---

## Dialecte SQLAlchemy {: #sqlalchemy-dialect }

```bash
pip install "provisa-client[sqlalchemy]"
```

Schéma d'URL : `provisa+http://` ou `provisa+https://` (REQ-270)

```python
from sqlalchemy import create_engine, text

engine = create_engine("provisa+http://alice:secret@localhost:8001")

with engine.connect() as conn:
    result = conn.execute(text("{ orders { id amount region } }"))
    for row in result:
        print(row)
```

### Avec pandas {: #with-pandas }

```python
import pandas as pd

df = pd.read_sql("{ orders { id amount } }", engine)
```

### Paramètres d'URL {: #url-parameters }

| Paramètre | Description | Défaut |
| ----------- | ------------- | --------- |
| `role` | Rôle à demander ; validé côté serveur (REQ-273) | le rôle résolu par la connexion |

```python
engine = create_engine(
    "provisa+http://alice:secret@localhost:8001?role=analyst"
)
```

### Introspection de schéma {: #schema-introspection }

Le dialecte reflète le catalogue servi à votre rôle, lu depuis `GET /data/catalog` en tant qu'utilisateur, rôle et organisation de la connexion. Chaque domaine est un schéma, et chaque table vit dans l'un d'eux : `get_schema_names()` liste les domaines, `get_table_names(schema="shop")` liste les tables de ce domaine par leurs noms SQL, et `get_columns("orders", schema="shop")` donne le type, la nullabilité et la description de chaque colonne. Sans schéma nommé, `get_table_names()` n'en liste aucune. Une table inconnue lève `NoSuchTableError` ; un catalogue que le serveur refuse lève une erreur. (REQ-363, REQ-270, REQ-2070)

---

## ADBC

Arrow Database Connectivity adossé à Arrow Flight. (REQ-271) Retourne directement une
`pyarrow.Table` — sans désérialisation JSON. (REQ-271)

```bash
pip install "provisa-client[adbc]"
```

```python
from provisa_client.adbc import adbc_connect

conn = adbc_connect(
    "http://localhost:8001",
    user="alice",
    password="secret",
    role="analyst",   # optional; server validates the requested role
    port=8815,        # Arrow Flight port (REQ-711)
)
```

`adbc_connect` se connecte d'abord en HTTP et place le jeton obtenu dans chaque ticket Flight, de sorte que le serveur Flight authentifie la connexion exactement comme le fait la surface REST. (REQ-1263) L'argument `role` est une demande, validée côté serveur au regard des attributions de l'identité — il ne devient jamais l'identité. (REQ-273)

### Récupération sous forme de table Arrow {: #fetch-as-arrow-table }

```python
with conn.cursor() as cur:
    cur.execute("{ orders { id amount region } }")
    table = cur.fetch_arrow_table()   # pyarrow.Table
    df = table.to_pandas()
```

### Récupération sous forme de tuples {: #fetch-as-tuples }

```python
with conn.cursor() as cur:
    cur.execute("{ orders { id amount } }")
    rows = cur.fetchall()    # list of tuples
    one  = cur.fetchone()    # single tuple or None
```

### Métadonnées de colonnes {: #column-metadata_1 }

```python
cur.execute("{ orders { id amount } }")
print(cur.description)
# [('id', None, ...), ('amount', None, ...)]
```

### Gestionnaire de contexte {: #context-manager }

```python
with adbc_connect("http://localhost:8001", user="alice", password="secret") as conn:
    with conn.cursor() as cur:
        cur.execute("{ orders { id amount } }")
        table = cur.fetch_arrow_table()
```

ADBC se connecte au serveur Flight sur le port 8815 par défaut. (REQ-143) Transmettez `port=` pour
atteindre un serveur Flight lié à un port non standard. (REQ-711)
