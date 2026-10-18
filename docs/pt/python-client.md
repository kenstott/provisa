# Cliente Python (`provisa-client`) {: #python-client-provisa-client }

Cliente Python para o Provisa. Fornece quatro interfaces:

| Interface | Caso de uso |
| ----------- | ---------- |
| `ProvisaClient` | Consultas GraphQL, Arrow Flight, saída DataFrame |
| DB-API 2.0 (`connect`) | Interface padrão de banco de dados Python (PEP 249) (REQ-268) |
| Dialeto SQLAlchemy | Ferramentas de BI, ORM, `read_sql` do Pandas (REQ-270) |
| ADBC | Streaming colunar Arrow-nativo via Flight (REQ-271) |

## Instalação {: #install }

```bash
pip install provisa-client                      # core (ProvisaClient + DB-API)
pip install "provisa-client[pandas]"            # adds pandas
pip install "provisa-client[sqlalchemy]"        # adds SQLAlchemy dialect
pip install "provisa-client[adbc]"              # adds ADBC over Arrow Flight
```

---

## ProvisaClient

### Início Rápido {: #quick-start }

```python
from provisa_client import ProvisaClient

client = ProvisaClient(
    "http://localhost:8001",
    token="provisa_pat_...",   # personal access token, or a provider bearer token
    role="analyst",
)
```

`ProvisaClient` recebe uma credencial, não um nome de usuário e uma senha: ele não tem nenhuma etapa de login própria. Um token de acesso pessoal é a credencial indicada quando um script precisa rodar sem supervisão — é emitido a partir do perfil do próprio usuário, carrega uma expiração e é revogável sem tocar na conta. (REQ-1263) Um token bearer do provedor funciona de forma idêntica. Qualquer um dos dois vai em `token`, e o cliente o apresenta tanto no caminho HTTP quanto no do Arrow Flight.

Para trocar uma senha por um token, faça POST em `/auth/login` e leia `access_token`:

```python
import httpx

body = httpx.post(
    "http://localhost:8001/auth/login",
    json={"username": "alice", "password": "secret"},
).json()
client = ProvisaClient("http://localhost:8001", token=body["access_token"])
```

Os pontos de entrada de DB-API e ADBC fazem essa troca por você — veja abaixo.

### Consultas GraphQL {: #graphql-queries }

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

### Assíncrono {: #async }

```python
result = await client.aquery("{ orders { id amount } }")
```

### Arrow Flight (colunar de alta vazão) {: #arrow-flight-high-throughput-columnar }

Use Flight para grandes conjuntos de resultados — os dados fazem streaming como batches de registro Arrow sem materializar no servidor. (REQ-143, REQ-145)

```python
import pyarrow as pa

table: pa.Table = client.flight("{ orders { id amount region } }")
df = client.flight_df("{ orders { id amount region } }")
```

O Flight se conecta à porta 8815 por padrão. (REQ-143) Sobreponha com `flight_port=`:

```python
client = ProvisaClient("http://prod.example.com", flight_port=8815)
```

### Exploração de Catálogo {: #catalog-exploration }

```python
tables_df = client.list_tables()
```

### Referência de Conexão {: #connection-reference }

| Parâmetro | Padrão | Descrição |
| ----------- | --------- | ------------- |
| `url` | `http://localhost:8001` | URL base do servidor Provisa |
| `token` | `None` | Credencial bearer — um token do provedor ou um token de acesso pessoal; omita para autenticação por senha (REQ-606, REQ-1263) |
| `role` | `None` | Função com a qual agir, enviada como `X-Provisa-Role` e atendida apenas se atribuída ao usuário; se omitida, a requisição é executada com a função própria da identidade (REQ-273) |
| `flight_port` | `8815` | Porta gRPC do Arrow Flight (REQ-143) |

### Tratamento de Erros {: #error-handling }

`query()` lança `httpx.HTTPStatusError` em erros HTTP. (REQ-607)
`query_df()` lança `RuntimeError` se a resposta contiver erros GraphQL. (REQ-607)

---

## Usar o Provisa no Jupyter {: #using-provisa-in-jupyter }

Instale no kernel em execução antes de importar. O extra `pandas` cobre a saída como DataFrame; adicione `sqlalchemy` se também precisar de `pandas.read_sql` pelo dialeto.

```python
%pip install "provisa-client[pandas]"
```

### Credenciais {: #credentials }

Os notebooks gravam a saída das células no arquivo `.ipynb`. Um token digitado numa célula é salvo em disco junto com o seu código. Não faça isso.

`ProvisaClient` não lê o ambiente: passe as credenciais explicitamente, a partir de variáveis de ambiente. (`provisa_client.query()`, abaixo, as lê por você.)

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

Defina essas variáveis no shell antes de iniciar o Jupyter, ou carregue um arquivo `.env` com `python-dotenv`. De qualquer forma, mantenha-as fora do notebook.

### Uma chamada: `query()` {: #one-call-query }

`provisa_client.query()` recebe uma instrução SQL, GraphQL ou Cypher e retorna um DataFrame do pandas. O servidor descobre qual linguagem é. Nada é construído antes: a conexão vem do ambiente ou de um arquivo de perfil.

```python
import provisa_client

df = provisa_client.query("SELECT region, amount FROM shop.orders")
admin_view = provisa_client.query("SELECT region, amount FROM shop.orders", role="org_admin")
```

`role=` atua com esse papel por uma leitura, enviado como `X-Provisa-Role`. O servidor só o respeita para um papel que você tem. A leitura vai pelo Arrow Flight; defina `transport` como `http` para enviá-la a `/data/query`. `await provisa_client.aquery(...)` é a forma aguardável.

A conexão é lida do primeiro lugar que nomeia cada configuração:

1. palavras-chave de `provisa_client.sign_in(...)`, que resolve a conexão uma vez para todas as leituras seguintes;
2. variáveis de ambiente: `PROVISA_URL`, `PROVISA_TOKEN`, `PROVISA_USERNAME`, `PROVISA_PASSWORD`, `PROVISA_ROLE`, `PROVISA_ORG`, `PROVISA_FLIGHT_PORT`, `PROVISA_TRANSPORT`;
3. uma seção de `~/.provisa/credentials`: `[default]`, ou a que `PROVISA_PROFILE` (ou `sign_in(profile=...)`) nomeia.

```ini
[default]
url = https://provisa.example.com
token = provisa_pat_...
role = analyst

[staging]
url = https://staging.provisa.example.com
username = ana
```

Um `token` é um token de acesso pessoal ou um token de sessão. Informe `username` e `password` em vez disso e o cliente os troca por um token de sessão em `POST /auth/login`. Isso funciona numa implantação cujo provedor de login aceita senha (basic, simple, LDAP). Guarde a senha em `PROVISA_PASSWORD` em vez do arquivo. Um login recusado, uma URL ausente ou um perfil nomeado que não está no arquivo gera um erro com o motivo. O Provisa não tem um fluxo de login que um notebook possa conduzir por um navegador; por isso, em implantações com single sign-on, use um token de acesso pessoal.

### Magics de célula {: #cell-magics }

```python
%pip install "provisa-client[notebook]"
%load_ext provisa_client.magics
```

```text
%%sql eu --role analyst_eu
SELECT region, customer_email FROM shop.orders
```

`%%sql`, `%%graphql` e `%%cypher` respondem à célula com `query()`. O DataFrame é o resultado da célula. Um nome após o magic também o vincula no notebook (`eu` acima); `--role` atua com esse papel na célula.

O JupySQL (`%load_ext sql`) também define um `%%sql`. Carregue os dois e `%%sql` pertence à extensão carregada por último. A do Provisa aceita `--role` e lê pelo Arrow Flight. O JupySQL também funciona, pelo dialeto SQLAlchemy: `%sql provisa+http://user:password@host:port` conecta, `%sql`/`%%sql` executam instruções governadas, e `%sqlcmd tables -s <domain>` e `%sqlcmd columns -t <table> -s <domain>` listam o catálogo.

### No navegador (Pyodide) {: #in-the-browser-pyodide }

No Pyodide, `query()` lê do servidor que serviu a página, como o usuário conectado na página. A página hospedeira mantém esse usuário no escopo global do kernel:

```javascript
self.provisaAuth = { baseUrl: "", role: "analyst_eu", bearer: "<token>", org: null };
```

`baseUrl` fica vazio para a própria origem da página. O cliente lê `provisaAuth` a cada requisição, então um token rotacionado vale sem reiniciar. Os resultados chegam como Arrow quando `pyarrow` está carregado; sem ele, chegam como JSON. Uma instrução Cypher sempre volta como JSON. `query()` bloqueia, o que um Web Worker (onde roda o kernel de um notebook) pode fazer; na thread principal da página ela gera um erro, e a chamada a usar é `await provisa_client.aquery(...)`.

### DataFrames de GraphQL, SQL e Cypher {: #dataframes-from-graphql-sql-and-cypher }

`query_df()` aceita as três linguagens de consulta e retorna um DataFrame do pandas.

```python
# GraphQL — first root field is extracted automatically
df = client.query_df("{ orders { id amount region } }")

# SQL
df = client.query_df("SELECT id, amount, region FROM orders WHERE region = 'west'")

# Cypher
df = client.query_df("MATCH (o:Order) RETURN o.id, o.amount, o.region")
```

As respostas GraphQL trazem os dados sob o nome de um campo raiz; `query_df()` os extrai. As respostas SQL e Cypher trazem nomes de coluna explícitos do servidor e são mapeadas diretamente.

### Arrow Flight para extrações grandes {: #arrow-flight-for-large-pulls }

`flight_df()` transmite os dados como lotes de registros Arrow em vez de desserializar JSON. Para tabelas largas ou resultados acima de algumas centenas de milhares de linhas, é mais rápido e usa menos memória.

```python
df = client.flight_df("SELECT * FROM events")
```

O Arrow preserva os tipos de coluna: inteiros continuam inteiros, timestamps continuam timestamps. O JSON não. Só isso já é motivo para preferir o Flight quando o esquema importa.

O Flight conecta por padrão na porta 8815. Passe `flight_port=` ao construtor de `ProvisaClient` se o seu servidor usa outra porta.

### `pandas.read_sql` pelo dialeto SQLAlchemy {: #pandasread_sql-through-the-sqlalchemy-dialect }

Instale o extra `sqlalchemy` no kernel:

```python
%pip install "provisa-client[sqlalchemy]"
```

Depois crie um engine com uma URL `provisa+http://`. Coloque as credenciais em variáveis de ambiente, não na URL.

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

O dialeto envia o usuário e a senha para `/auth/login` e mantém o token obtido durante a vida do engine: as credenciais não são retransmitidas a cada consulta.

---

## DB-API 2.0

Interface padrão [PEP 249](https://peps.python.org/pep-0249/). (REQ-268) Funciona com qualquer ferramenta que aceite uma conexão DB-API.

```python
from provisa_client import connect

conn = connect(
    "http://localhost:8001",
    username="alice",
    password="secret",
    role="analyst",     # optional; omit to run as the role the login returns
)
```

`connect` envia o nome de usuário e a senha por POST para `/auth/login` e guarda o `access_token` que recebe, de modo que a conexão carrega uma credencial de verdade em vez de um nome. `role` *solicita* uma função e o servidor a atende apenas se a identidade a tiver atribuída (REQ-273); se omitida, a conexão roda com a função que o login resolveu.

### Executando consultas {: #executing-queries }

O cursor aceita GraphQL ou SQL — detectado automaticamente. (REQ-268, REQ-274)

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

### Metadados de coluna {: #column-metadata }

```python
cur.execute("{ orders { id amount } }")
print(cur.description)
# [('id', None, ...), ('amount', None, ...)]
print(cur.rowcount)
```

### Parâmetros nomeados {: #named-parameters }

```python
cur.execute(
    "SELECT * FROM orders WHERE region = :region",
    {"region": "west"},
)
```

### Gerenciadores de contexto {: #context-managers }

```python
with connect("http://localhost:8001", username="alice", password="secret") as conn:
    with conn.cursor() as cur:
        cur.execute("{ orders { id amount } }")
        print(cur.fetchall())
```

---

## Dialeto SQLAlchemy {: #sqlalchemy-dialect }

```bash
pip install "provisa-client[sqlalchemy]"
```

Esquema de URL: `provisa+http://` ou `provisa+https://` (REQ-270)

```python
from sqlalchemy import create_engine, text

engine = create_engine("provisa+http://alice:secret@localhost:8001")

with engine.connect() as conn:
    result = conn.execute(text("{ orders { id amount region } }"))
    for row in result:
        print(row)
```

### Com pandas {: #with-pandas }

```python
import pandas as pd

df = pd.read_sql("{ orders { id amount } }", engine)
```

### Parâmetros de URL {: #url-parameters }

| Parâmetro | Descrição | Padrão |
| ----------- | ------------- | --------- |
| `role` | Função a solicitar; validada no servidor (REQ-273) | a função que o login resolve |

```python
engine = create_engine(
    "provisa+http://alice:secret@localhost:8001?role=analyst"
)
```

### Introspecção de esquema {: #schema-introspection }

O dialeto reflete o catálogo servido ao seu papel, lido de `GET /data/catalog` como o usuário, o papel e a organização da conexão. Cada domínio é um esquema, e cada tabela vive em um: `get_schema_names()` lista os domínios, `get_table_names(schema="shop")` lista as tabelas desse domínio pelos seus nomes SQL, e `get_columns("orders", schema="shop")` dá o tipo, a nulidade e a descrição de cada coluna. Sem esquema nomeado, `get_table_names()` não lista nenhuma. Uma tabela desconhecida gera `NoSuchTableError`; um catálogo que o servidor recusa gera um erro. (REQ-363, REQ-270, REQ-2070)

---

## ADBC

Arrow Database Connectivity apoiado por Arrow Flight. (REQ-271) Retorna `pyarrow.Table` diretamente — sem desserialização JSON. (REQ-271)

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

`adbc_connect` faz login por HTTP primeiro e coloca o token resultante em cada ticket do Flight, de modo que o servidor Flight autentica a conexão do mesmo jeito que a superfície REST. (REQ-1263) O argumento `role` é uma solicitação, validada no servidor contra as atribuições da identidade — ele nunca se torna a identidade. (REQ-273)

### Buscar como Arrow Table {: #fetch-as-arrow-table }

```python
with conn.cursor() as cur:
    cur.execute("{ orders { id amount region } }")
    table = cur.fetch_arrow_table()   # pyarrow.Table
    df = table.to_pandas()
```

### Buscar como tuplas {: #fetch-as-tuples }

```python
with conn.cursor() as cur:
    cur.execute("{ orders { id amount } }")
    rows = cur.fetchall()    # list of tuples
    one  = cur.fetchone()    # single tuple or None
```

### Metadados de coluna {: #column-metadata_1 }

```python
cur.execute("{ orders { id amount } }")
print(cur.description)
# [('id', None, ...), ('amount', None, ...)]
```

### Gerenciador de contexto {: #context-manager }

```python
with adbc_connect("http://localhost:8001", user="alice", password="secret") as conn:
    with conn.cursor() as cur:
        cur.execute("{ orders { id amount } }")
        table = cur.fetch_arrow_table()
```

O ADBC se conecta ao servidor Flight na porta 8815 por padrão. (REQ-143) Passe `port=` para alcançar um servidor Flight vinculado a uma porta não padrão. (REQ-711)
