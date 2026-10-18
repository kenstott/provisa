# Python Client (`provisa-client`)

Cliente de Python para Provisa. Ofrece cuatro interfaces:

| Interfaz | Caso de uso |
| ----------- | ---------- |
| `ProvisaClient` | Consultas GraphQL, Arrow Flight, salida en DataFrame |
| DB-API 2.0 (`connect`) | Interfaz estándar de base de datos de Python (PEP 249) (REQ-268) |
| Dialecto de SQLAlchemy | Herramientas de BI, ORM, `read_sql` de Pandas (REQ-270) |
| ADBC | Transmisión columnar nativa de Arrow mediante Flight (REQ-271) |

## Instalación {: #install }

```bash
pip install provisa-client                      # core (ProvisaClient + DB-API)
pip install "provisa-client[pandas]"            # adds pandas
pip install "provisa-client[sqlalchemy]"        # adds SQLAlchemy dialect
pip install "provisa-client[adbc]"              # adds ADBC over Arrow Flight
```

---

## ProvisaClient

### Inicio rápido {: #quick-start }

```python
from provisa_client import ProvisaClient

client = ProvisaClient(
    "http://localhost:8001",
    token="provisa_pat_...",   # personal access token, or a provider bearer token
    role="analyst",
)
```

`ProvisaClient` recibe una credencial, no un nombre de usuario y una contraseña: no tiene ningún paso de inicio de sesión propio. Un token de acceso personal es la credencial a la que recurrir cuando un script debe ejecutarse sin supervisión — se emite desde el perfil del propio usuario, lleva una caducidad y es revocable sin tocar la cuenta. (REQ-1263) Un token bearer del proveedor funciona igual. Cualquiera de los dos va en `token`, y el cliente lo presenta tanto en la ruta HTTP como en la de Arrow Flight.

Para canjear una contraseña por un token, haga POST a `/auth/login` y lea `access_token`:

```python
import httpx

body = httpx.post(
    "http://localhost:8001/auth/login",
    json={"username": "alice", "password": "secret"},
).json()
client = ProvisaClient("http://localhost:8001", token=body["access_token"])
```

Los puntos de entrada de DB-API y ADBC hacen este canje por usted — véase más abajo.

### Consultas de GraphQL {: #graphql-queries }

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

### Asíncrono {: #async }

```python
result = await client.aquery("{ orders { id amount } }")
```

### Arrow Flight (columnar de alto rendimiento) {: #arrow-flight-high-throughput-columnar }

Use Flight para conjuntos de resultados grandes: los datos se transmiten como lotes de registros Arrow sin materializarse en el servidor. (REQ-143, REQ-145)

```python
import pyarrow as pa

table: pa.Table = client.flight("{ orders { id amount region } }")
df = client.flight_df("{ orders { id amount region } }")
```

Flight se conecta al puerto 8815 de forma predeterminada. (REQ-143) Sobrescríbalo con `flight_port=`:

```python
client = ProvisaClient("http://prod.example.com", flight_port=8815)
```

### Exploración del catálogo {: #catalog-exploration }

```python
tables_df = client.list_tables()
```

### Referencia de conexión {: #connection-reference }

| Parámetro | Valor predeterminado | Descripción |
| ----------- | --------- | ------------- |
| `url` | `http://localhost:8001` | URL base del servidor Provisa |
| `token` | `None` | Credencial bearer — un token del proveedor o un token de acceso personal; omítalo para autenticación con contraseña (REQ-606, REQ-1263) |
| `role` | `None` | Rol con el que actuar, enviado como `X-Provisa-Role` y atendido solo si está asignado al usuario; si se omite, la solicitud se ejecuta con el rol propio de la identidad (REQ-273) |
| `flight_port` | `8815` | Puerto gRPC de Arrow Flight (REQ-143) |

### Manejo de errores {: #error-handling }

`query()` lanza `httpx.HTTPStatusError` en errores HTTP. (REQ-607)  
`query_df()` lanza `RuntimeError` si la respuesta contiene errores de GraphQL. (REQ-607)

---

## Usar Provisa en Jupyter {: #using-provisa-in-jupyter }

Instale en el kernel en ejecución antes de importar. El extra `pandas` cubre la salida como DataFrame; añada `sqlalchemy` si también necesita `pandas.read_sql` a través del dialecto.

```python
%pip install "provisa-client[pandas]"
```

### Credenciales {: #credentials }

Los notebooks guardan la salida de las celdas en el archivo `.ipynb`. Si escribe un token en una celda, se guardará en disco junto con su código. No lo haga.

`ProvisaClient` no lee el entorno: pásele las credenciales explícitamente, desde variables de entorno. (`provisa_client.query()`, más abajo, las lee por usted.)

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

Defina esas variables en su shell antes de iniciar Jupyter, o cargue un archivo `.env` con `python-dotenv`. En cualquier caso, manténgalas fuera del notebook.

### Una sola llamada: `query()` {: #one-call-query }

`provisa_client.query()` recibe una sentencia SQL, GraphQL o Cypher y devuelve un DataFrame de pandas. El servidor determina de qué lenguaje se trata. No hay que construir nada antes: la conexión sale del entorno o de un archivo de perfil.

```python
import provisa_client

df = provisa_client.query("SELECT region, amount FROM shop.orders")
admin_view = provisa_client.query("SELECT region, amount FROM shop.orders", role="org_admin")
```

`role=` actúa con ese rol durante una lectura, enviado como `X-Provisa-Role`. El servidor solo lo respeta para un rol que usted tenga. La lectura va por Arrow Flight; ponga `transport` en `http` para enviarla a `/data/query`. `await provisa_client.aquery(...)` es la forma awaitable.

La conexión se lee del primer lugar que nombra cada parámetro:

1. argumentos de `provisa_client.sign_in(...)`, que resuelve la conexión una vez para todas las lecturas posteriores;
2. variables de entorno: `PROVISA_URL`, `PROVISA_TOKEN`, `PROVISA_USERNAME`, `PROVISA_PASSWORD`, `PROVISA_ROLE`, `PROVISA_ORG`, `PROVISA_FLIGHT_PORT`, `PROVISA_TRANSPORT`;
3. una sección de `~/.provisa/credentials`: `[default]`, o la que nombre `PROVISA_PROFILE` (o `sign_in(profile=...)`).

```ini
[default]
url = https://provisa.example.com
token = provisa_pat_...
role = analyst

[staging]
url = https://staging.provisa.example.com
username = ana
```

Un `token` es un token de acceso personal o un token de sesión. Si en su lugar da `username` y `password`, el cliente los cambia por un token de sesión en `POST /auth/login`. Esto funciona en un despliegue cuyo proveedor de inicio de sesión acepta contraseña (basic, simple, LDAP). Guarde la contraseña en `PROVISA_PASSWORD` y no en el archivo. Un inicio de sesión rechazado, una URL ausente o un perfil nombrado que no está en el archivo lanza un error con el motivo. Provisa no tiene un flujo de inicio de sesión que un notebook pueda conducir desde un navegador, así que en despliegues con inicio de sesión único use un token de acceso personal.

### Magics de celda {: #cell-magics }

```python
%pip install "provisa-client[notebook]"
%load_ext provisa_client.magics
```

```text
%%sql eu --role analyst_eu
SELECT region, customer_email FROM shop.orders
```

`%%sql`, `%%graphql` y `%%cypher` responden la celda con `query()`. El DataFrame es el resultado de la celda. Un nombre tras el magic también lo enlaza en el notebook (`eu` arriba); `--role` actúa con ese rol para la celda.

JupySQL (`%load_ext sql`) también define un `%%sql`. Si carga ambos, `%%sql` pertenece a la extensión cargada en último lugar. La de Provisa acepta `--role` y lee por Arrow Flight. JupySQL también funciona, mediante el dialecto de SQLAlchemy: `%sql provisa+http://user:password@host:port` conecta, `%sql`/`%%sql` ejecutan sentencias gobernadas, y `%sqlcmd tables -s <domain>` y `%sqlcmd columns -t <table> -s <domain>` listan el catálogo.

### En el navegador (Pyodide) {: #in-the-browser-pyodide }

Con Pyodide, `query()` lee del servidor que sirvió la página, como el usuario con sesión iniciada en la página. La página anfitriona mantiene ese usuario en el ámbito global del kernel:

```javascript
self.provisaAuth = { baseUrl: "", role: "analyst_eu", bearer: "<token>", org: null };
```

`baseUrl` está vacío para el propio origen de la página. El cliente lee `provisaAuth` en cada solicitud, así que un token rotado surte efecto sin reiniciar. Los resultados llegan como Arrow cuando `pyarrow` está cargado; sin él llegan como JSON. Una sentencia Cypher siempre vuelve como JSON. `query()` bloquea, algo que un Web Worker (donde corre el kernel de un notebook) puede hacer; en el hilo principal de la página lanza un error y la llamada a usar es `await provisa_client.aquery(...)`.

### DataFrames desde GraphQL, SQL y Cypher {: #dataframes-from-graphql-sql-and-cypher }

`query_df()` acepta los tres lenguajes de consulta y devuelve un DataFrame de pandas.

```python
# GraphQL — first root field is extracted automatically
df = client.query_df("{ orders { id amount region } }")

# SQL
df = client.query_df("SELECT id, amount, region FROM orders WHERE region = 'west'")

# Cypher
df = client.query_df("MATCH (o:Order) RETURN o.id, o.amount, o.region")
```

Las respuestas GraphQL llevan los datos bajo el nombre de un campo raíz; `query_df()` los extrae. Las respuestas SQL y Cypher llevan nombres de columna explícitos del servidor y se mapean directamente.

### Arrow Flight para extracciones grandes {: #arrow-flight-for-large-pulls }

`flight_df()` transmite los datos como lotes de registros Arrow en lugar de deserializar JSON. Para tablas anchas o conjuntos de resultados de más de unos cientos de miles de filas, es más rápido y usa menos memoria.

```python
df = client.flight_df("SELECT * FROM events")
```

Arrow conserva los tipos de columna: los enteros siguen siendo enteros y las marcas de tiempo siguen siendo marcas de tiempo. JSON no. Eso basta para preferir Flight cuando el esquema importa.

Flight se conecta por defecto al puerto 8815. Pase `flight_port=` al constructor de `ProvisaClient` si su servidor usa otro puerto.

### `pandas.read_sql` mediante el dialecto de SQLAlchemy {: #pandasread_sql-through-the-sqlalchemy-dialect }

Instale el extra `sqlalchemy` en el kernel:

```python
%pip install "provisa-client[sqlalchemy]"
```

Después cree un engine con una URL `provisa+http://`. Ponga las credenciales en variables de entorno, no en la URL.

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

El dialecto envía el usuario y la contraseña a `/auth/login` y conserva el token resultante durante la vida del engine: las credenciales no se retransmiten en cada consulta.

---

## DB-API 2.0

Interfaz estándar [PEP 249](https://peps.python.org/pep-0249/). (REQ-268) Funciona con cualquier herramienta que acepte una conexión DB-API.

```python
from provisa_client import connect

conn = connect(
    "http://localhost:8001",
    username="alice",
    password="secret",
    role="analyst",     # optional; omit to run as the role the login returns
)
```

`connect` envía el nombre de usuario y la contraseña por POST a `/auth/login` y conserva el `access_token` que recibe, de modo que la conexión lleva una credencial real en vez de un nombre. `role` *solicita* un rol y el servidor lo concede solo si la identidad lo tiene asignado (REQ-273); si se omite, la conexión se ejecuta con el rol que resolvió el inicio de sesión.

### Ejecución de consultas {: #executing-queries }

El cursor acepta GraphQL o SQL: se detecta automáticamente. (REQ-268, REQ-274)

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

### Metadatos de columnas {: #column-metadata }

```python
cur.execute("{ orders { id amount } }")
print(cur.description)
# [('id', None, ...), ('amount', None, ...)]
print(cur.rowcount)
```

### Parámetros con nombre {: #named-parameters }

```python
cur.execute(
    "SELECT * FROM orders WHERE region = :region",
    {"region": "west"},
)
```

### Gestores de contexto {: #context-managers }

```python
with connect("http://localhost:8001", username="alice", password="secret") as conn:
    with conn.cursor() as cur:
        cur.execute("{ orders { id amount } }")
        print(cur.fetchall())
```

---

## Dialecto de SQLAlchemy {: #sqlalchemy-dialect }

```bash
pip install "provisa-client[sqlalchemy]"
```

Esquema de URL: `provisa+http://` o `provisa+https://` (REQ-270)

```python
from sqlalchemy import create_engine, text

engine = create_engine("provisa+http://alice:secret@localhost:8001")

with engine.connect() as conn:
    result = conn.execute(text("{ orders { id amount region } }"))
    for row in result:
        print(row)
```

### Con pandas {: #with-pandas }

```python
import pandas as pd

df = pd.read_sql("{ orders { id amount } }", engine)
```

### Parámetros de URL {: #url-parameters }

| Parámetro | Descripción | Valor predeterminado |
| ----------- | ------------- | --------- |
| `role` | Rol a solicitar; validado en el servidor (REQ-273) | el rol que resuelve el inicio de sesión |

```python
engine = create_engine(
    "provisa+http://alice:secret@localhost:8001?role=analyst"
)
```

### Introspección del esquema {: #schema-introspection }

El dialecto refleja el catálogo que se sirve a su rol, leído de `GET /data/catalog` como el usuario, el rol y la organización de la conexión. Cada dominio es un esquema, y cada tabla vive en uno: `get_schema_names()` lista los dominios, `get_table_names(schema="shop")` lista las tablas de ese dominio por sus nombres SQL, y `get_columns("orders", schema="shop")` da el tipo, la nulabilidad y la descripción de cada columna. Sin esquema, `get_table_names()` no lista ninguna. Una tabla desconocida lanza `NoSuchTableError`; un catálogo que el servidor rechaza lanza un error. (REQ-363, REQ-270, REQ-2070)

---

## ADBC

Arrow Database Connectivity respaldado por Arrow Flight. (REQ-271) Devuelve `pyarrow.Table` directamente, sin deserialización JSON. (REQ-271)

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

`adbc_connect` inicia sesión por HTTP primero y coloca el token resultante en cada ticket de Flight, de modo que el servidor Flight autentica la conexión igual que lo hace la superficie REST. (REQ-1263) El argumento `role` es una solicitud, validada en el servidor contra las asignaciones de la identidad: nunca se convierte en la identidad. (REQ-273)

### Obtención como tabla Arrow {: #fetch-as-arrow-table }

```python
with conn.cursor() as cur:
    cur.execute("{ orders { id amount region } }")
    table = cur.fetch_arrow_table()   # pyarrow.Table
    df = table.to_pandas()
```

### Obtención como tuplas {: #fetch-as-tuples }

```python
with conn.cursor() as cur:
    cur.execute("{ orders { id amount } }")
    rows = cur.fetchall()    # list of tuples
    one  = cur.fetchone()    # single tuple or None
```

### Metadatos de columnas {: #column-metadata_1 }

```python
cur.execute("{ orders { id amount } }")
print(cur.description)
# [('id', None, ...), ('amount', None, ...)]
```

### Gestor de contexto {: #context-manager }

```python
with adbc_connect("http://localhost:8001", user="alice", password="secret") as conn:
    with conn.cursor() as cur:
        cur.execute("{ orders { id amount } }")
        table = cur.fetch_arrow_table()
```

ADBC se conecta al servidor Flight en el puerto 8815 de forma predeterminada. (REQ-143) Pase `port=` para alcanzar un servidor Flight vinculado a un puerto no predeterminado. (REQ-711)
