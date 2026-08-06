# Client Python (`provisa-client`)

Client Python pour Provisa. Fournit quatre interfaces :

| Interface | Cas d'usage |
| ----------- | ---------- |
| `ProvisaClient` | Requêtes GraphQL, Arrow Flight, sortie DataFrame |
| DB-API 2.0 (`connect`) | Interface de base de données Python standard (PEP 249) (REQ-268) |
| Dialecte SQLAlchemy | Outils BI, ORM, `read_sql` Pandas (REQ-270) |
| ADBC | Streaming columnaire natif Arrow via Flight (REQ-271) |

## Installation

```bash
pip install provisa-client                      # core (ProvisaClient + DB-API)
pip install "provisa-client[pandas]"            # adds pandas
pip install "provisa-client[sqlalchemy]"        # adds SQLAlchemy dialect
pip install "provisa-client[adbc]"              # adds ADBC over Arrow Flight
```

---

## ProvisaClient

### Démarrage rapide

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

### Requêtes GraphQL

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

### Asynchrone

```python
result = await client.aquery("{ orders { id amount } }")
```

### Arrow Flight (columnaire haut débit)

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

### Exploration du catalogue

```python
tables_df = client.list_tables()
```

### Référence de connexion

| Paramètre | Défaut | Description |
| ----------- | --------- | ------------- |
| `url` | `http://localhost:8001` | URL de base du serveur Provisa |
| `token` | `None` | Credential bearer — un jeton du fournisseur ou un jeton d'accès personnel ; à omettre pour l'authentification par mot de passe (REQ-606, REQ-1263) |
| `role` | `"admin"` | Rôle transmis à chaque requête (REQ-273) |
| `flight_port` | `8815` | Port gRPC d'Arrow Flight (REQ-143) |

### Gestion des erreurs

`query()` lève `httpx.HTTPStatusError` en cas d'erreur HTTP. (REQ-607)
`query_df()` lève `RuntimeError` si la réponse contient des erreurs GraphQL. (REQ-607)

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

### Exécuter des requêtes

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

### Métadonnées de colonnes

```python
cur.execute("{ orders { id amount } }")
print(cur.description)
# [('id', None, ...), ('amount', None, ...)]
print(cur.rowcount)
```

### Paramètres nommés

```python
cur.execute(
    "SELECT * FROM orders WHERE region = :region",
    {"region": "west"},
)
```

### Gestionnaires de contexte

```python
with connect("http://localhost:8001", username="alice", password="secret") as conn:
    with conn.cursor() as cur:
        cur.execute("{ orders { id amount } }")
        print(cur.fetchall())
```

---

## Dialecte SQLAlchemy

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

### Avec pandas

```python
import pandas as pd

df = pd.read_sql("{ orders { id amount } }", engine)
```

### Paramètres d'URL

| Paramètre | Description | Défaut |
| ----------- | ------------- | --------- |
| `role` | Rôle à demander ; validé côté serveur (REQ-273) | le rôle résolu par la connexion |

```python
engine = create_engine(
    "provisa+http://alice:secret@localhost:8001?role=analyst"
)
```

### Introspection de schéma

Le dialecte implémente `get_table_names()`, `get_columns()`, et `has_table()` — les outils de
catalogue (DBeaver, SQLAlchemy automap) peuvent inspecter le schéma. (REQ-363, REQ-270)

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

### Récupération sous forme de table Arrow

```python
with conn.cursor() as cur:
    cur.execute("{ orders { id amount region } }")
    table = cur.fetch_arrow_table()   # pyarrow.Table
    df = table.to_pandas()
```

### Récupération sous forme de tuples

```python
with conn.cursor() as cur:
    cur.execute("{ orders { id amount } }")
    rows = cur.fetchall()    # list of tuples
    one  = cur.fetchone()    # single tuple or None
```

### Métadonnées de colonnes

```python
cur.execute("{ orders { id amount } }")
print(cur.description)
# [('id', None, ...), ('amount', None, ...)]
```

### Gestionnaire de contexte

```python
with adbc_connect("http://localhost:8001", user="alice", password="secret") as conn:
    with conn.cursor() as cur:
        cur.execute("{ orders { id amount } }")
        table = cur.fetch_arrow_table()
```

ADBC se connecte au serveur Flight sur le port 8815 par défaut. (REQ-143) Transmettez `port=` pour
atteindre un serveur Flight lié à un port non standard. (REQ-711)
