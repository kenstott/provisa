# Integrações

## Escolhendo um Caminho de Conexão

| Tipo de cliente | Caminho recomendado | Por quê |
| ------------- | ----------------- | ----- |
| Ferramentas de BI (Tableau, Power BI, Looker) | JDBC | Streaming colunar Arrow Flight pelo fio; ferramentas de BI têm um assistente JDBC embutido e se beneficiam da entrega colunar de alta vazão para grandes conjuntos de resultados |
| psql, DBeaver, qualquer ferramenta compatível com PG | pgwire (driver PG nativo) | Padrão de fricção zero — nenhum driver personalizado necessário; use o que você já tem |
| Stack de dados Python (pandas, pyarrow) | `provisa-client` ou ADBC bruto | Batches Arrow em streaming; sem overhead de serialização de linha |
| Spark, DuckDB, pipelines de alta vazão | Arrow Flight (ADBC) | Streaming colunar ilimitado direto para memória Arrow |
| Serviço a serviço (contratos tipados) | Protobuf gRPC | Proto gerado por função; linhas em streaming; segurança de tipo |
| Aplicações web, scripting | HTTP (`/data/graphql`, `/data/sql`) | Sem driver; HTTP padrão; escolha completa de linguagem de consulta |
| Clientes REST (padrão JSON:API) | `GET /data/jsonapi/{table}` | Envelope JSON:API v1.0; sparse fieldsets, paginação, filtragem via parâmetros de consulta; sem driver |

---

## pgwire — Driver PostgreSQL Nativo

O Provisa implementa o protocolo de fio PostgreSQL (versão de protocolo 3.0). Qualquer cliente que fale PostgreSQL se conecta sem um driver personalizado.

Habilite definindo `PROVISA_PGWIRE_PORT` (ex.: `5433`) antes de iniciar o Provisa. Desabilitado quando não definido ou `0`.

### Por que pgwire em vez de JDBC?

O driver JDBC usa Arrow Flight como seu transporte e exige implantar o `provisa-jdbc.jar`. O pgwire não exige nada — se você já tem `psql`, DBeaver, SQLAlchemy, ou um driver JDBC PG, você já está pronto. É o caminho de menor fricção para cargas de trabalho somente SQL.

JDBC é a escolha certa para ferramentas de BI que têm um assistente de conexão JDBC embutido e se beneficiam do streaming colunar do Arrow Flight para grandes conjuntos de resultados. O pgwire aceita SQL livre contra o esquema publicado completo — as mesmas consultas, custo de configuração menor.

### psql

```bash
psql -h localhost -p 5433 -U alice
```

### DBeaver

1. New Connection → PostgreSQL
2. Host: `localhost`, Port: `5433`
3. Usuário / senha conforme configurado no Provisa
4. Nenhum download de driver extra necessário

### SQLAlchemy (Python)

```python
from sqlalchemy import create_engine

engine = create_engine("postgresql+psycopg2://alice:secret@localhost:5433/provisa")
df = pd.read_sql("SELECT * FROM sales.orders", engine)
```

Ou com `asyncpg`:

```python
engine = create_engine("postgresql+asyncpg://alice:secret@localhost:5433/provisa")
```

### Autenticação

O pgwire usa autenticação por senha em texto claro conectada ao provedor de autenticação configurado do Provisa (`none` ou `simple`). No modo trust (`none`), o nome de usuário mapeia diretamente para uma função — a senha é ignorada. MD5 não é suportado; habilite TLS (`PROVISA_PGWIRE_CERT` / `PROVISA_PGWIRE_KEY`) ao rodar sobre uma rede não confiável.

### Limitações

- Somente SQL. GraphQL e Cypher não são aceitos via pgwire.
- Não é somente leitura. `COPY ... FROM STDIN` insere linhas em fontes `postgresql`, `mysql`, `sqlite`, e `mariadb`, e DDL é suportado (veja abaixo).
- DDL (`CREATE`, `ALTER`, `DROP`) é suportado e despachado para o caminho Trino ou direto; a nova tabela é registrada no contexto de compilação e é imediatamente consultável. `COPY ... TO STDOUT` (exportação) e `COPY ... FROM STDIN` (importação) são suportados nos formatos `text` e `csv`.
- Consultas a `information_schema` e `pg_catalog` são interceptadas e respondidas a partir de um shim de catálogo DuckDB — ferramentas de descoberta de esquema funcionam corretamente.

---

## Driver JDBC

O driver JDBC do Provisa usa Arrow Flight como seu transporte subjacente. É o caminho recomendado para ferramentas de BI com um assistente de conexão JDBC.

### Conexão

Baixe [provisa-jdbc.jar](https://provisa.dev/dl/jdbc) (sempre a versão mais recente) e adicione-o ao caminho de driver da sua ferramenta.

URL JDBC:

```yaml
jdbc:provisa://<host>:8815
```

A autenticação usa propriedades JDBC padrão `user` / `password`. O Provisa autentica as credenciais contra o provedor de autenticação configurado e atribui a função — o cliente não escolhe sua própria função.

### Configuração de Ferramenta de BI

**Tableau**

1. Manage → Drivers → Install Provisa JDBC
2. Connect → Other Databases (JDBC)
3. URL: `jdbc:provisa://localhost:8815`
4. Digite seu usuário e senha quando solicitado

**DBeaver** (caminho JDBC — para o caminho pgwire veja acima)

1. Database → New Connection → JDBC
2. Driver: adicione `provisa-jdbc.jar`
3. URL: `jdbc:provisa://localhost:8815`
4. Digite seu usuário e senha na aba Authentication

**Power BI** — use o gateway ODBC com a ponte Provisa JDBC-ODBC (incluída no instalador).

---

## Clientes Arrow Flight

O Arrow Flight (porta 8815) é o caminho recomendado para ferramentas de dados que o suportam. Os resultados fazem streaming como Arrow RecordBatches sem materializar na memória do Provisa.

### Python (`provisa-client`)

O caminho Python recomendado — envolve tanto GraphQL quanto Arrow Flight:

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

Veja [docs/python-client.md](python-client.md) para a referência completa incluindo DB-API 2.0, dialeto SQLAlchemy, e ADBC.

### Python (PyArrow bruto)

```python
import pyarrow.flight as flight

client = flight.connect("grpc://localhost:8815")
ticket = flight.Ticket(b'{"query": "SELECT id, amount FROM sales.orders"}')
df = client.do_get(ticket).read_all().to_pandas()
```

O ticket não carrega função. O servidor atribui a função a partir do provedor de autenticação configurado. Quando a seleção de função é permitida, passe-a nos metadados da chamada gRPC sob a chave `x-provisa-role` (por exemplo `flight.FlightCallOptions(headers=[(b"x-provisa-role", b"analyst")])`), não no JSON do ticket.

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

## Protobuf gRPC (porta 50051)

Caminho serviço a serviço. O Provisa gera um `.proto` por função na inicialização — cada função vê apenas as tabelas e colunas às quais tem acesso.

Baixe o proto para sua função:

```bash
curl http://localhost:8001/proto/analyst > provisa_analyst.proto
```

Use `grpc_server_reflection` para descobrir o esquema programaticamente.

A função é passada via a chave de metadados `x-provisa-role` em cada RPC. Consultas em streaming emitem uma mensagem por linha; mutações são unárias.

---

## Invocando Comandos Entre Protocolos

Um **comando** é uma função rastreada registrada ou webhook — um chamável registrado na camada semântica do Provisa com um `kind` (`query` ou `mutation`) e um `impl_kind` que descreve como ele roda. Toda superfície roteia invocações através de um único executor governado (`invoke_tracked_function`) que aplica `writable_by` e governança uniformemente (REQ-1156). [tool-verified: `provisa/api/data/action_exec.py`, `provisa/bolt/session.py:786-791`, `provisa/grpc/server.py:107-135`, `provisa/pgwire/function_call.py:80-88`, `provisa/api/flight/server.py:542-554`]

| `impl_kind` | O que roda | Campos de vinculação |
| ------------ | ----------- | --------------- |
| `source_procedure` | Procedimento armazenado em uma fonte registrada (padrão) | `sourceId`, `schemaName`, `functionName` |
| `script` | Script no lado do servidor | `script` |
| `http` | Chamada HTTP de saída | `url`, `method` |
| `grpc` | Chamada gRPC de saída para um servidor externo | `target`, `method` |
| `python` | Chamável Python hospedado pelo Provisa (REQ-885) | `callable` (ex.: `demo.py_functions:random_dataset`) |

Quando um comando declara um `return_schema` (JSON Schema com `type: array, items: object`), ele retorna conjunto — toda superfície o projeta como um conjunto de linhas tipado. Os comandos de demonstração `random_python_set` (impl_kind `python`) e `random_grpc_set` (impl_kind `grpc`) ilustram tanto um chamável hospedado quanto uma ponte gRPC externa retornando linhas com valores aleatórios; ambos são registrados em `config/provisa-install.yaml`. [tool-verified: `config/provisa-install.yaml:809-856`]

### Matriz de protocolo

| Superfície | Sintaxe | Exemplo |
| --------- | -------- | --------- |
| GraphQL | `kind=query` → campo Query; `kind=mutation` → campo Mutation; prefixado por domínio quando `domain_prefix: true` | `{ ps__random_python_set(rows: 5, seed: 42) { id region amount } }` |
| pgwire / Arrow Flight / MCP `run_sql` | `SELECT * FROM fn(args)` ou `SELECT fn(args)` | `SELECT * FROM random_python_set(5, 42)` |
| Cypher HTTP (`POST /data/cypher`) | `CALL fn(args) YIELD cols` | `CALL random_python_set(5, 42) YIELD id, region, amount` |
| Bolt (Neo4j Browser / driver) | `CALL fn(args)` — argumentos posicionais mapeiam para nomes de argumento declarados | `CALL random_python_set(3, 7)` |
| Provisa gRPC (porta 50051) | Unário `CallCommand(CommandRequest{name, args_json})` → `CommandResponse{rows_json}` | `grpcurl -d '{"name":"random_python_set","args_json":"{\"rows\":5}"}' ... ProvisaService/CallCommand` |

O campo `kind` controla apenas o posicionamento no GraphQL — as superfícies SQL, Cypher, Bolt, e gRPC aceitam comandos `query` e `mutation` de forma idêntica.

---

## Apollo Federation

O Provisa pode atuar como um subgraph Federation v2, expondo seu esquema publicado a um Apollo Router ou Apollo Gateway.

### Configuração

Habilite federation em `config.yaml`:

```yaml
federation:
  enabled: true
  subgraph_name: provisa-data
```

O Provisa gera diretivas `@key` em colunas de chave primária e `@external`/`@provides` em relacionamentos entre subgraphs automaticamente.

### Registro com Apollo Router

No seu `supergraph.yaml`:

```yaml
subgraphs:
  provisa-data:
    routing_url: http://provisa:8001/data/graphql
    schema:
      subgraph_url: http://provisa:8001/data/graphql
```

Rode `rover supergraph compose --config supergraph.yaml` para gerar o esquema do supergraph.

### Entidades

O Provisa responde a consultas `_entities` para joins entre subgraphs. Qualquer tabela com uma chave primária é automaticamente resolvível como uma entidade Federation.

---

## Importação Hasura v2 / DDN

Veja [docs/import.md](import.md) para migração do Hasura para o Provisa.

---

## Kafka

Veja [docs/sources.md](sources.md#fontes-kafka) para configuração de tópico Kafka como tabelas somente leitura e sinks de resultado de consulta.

---

## Interoperabilidade Semântica Apache Ossie (REQ-1316)

O Provisa troca modelos semânticos com o Apache Ossie (spec 0.2.0.dev0, incubating; anteriormente Open
Semantic Interchange) através de um adaptador de fronteira. O vocabulário interno do Provisa nunca é renomeado
para o do Ossie — a spec declara mudanças que quebram compatibilidade como prováveis, então o acoplamento é confinado ao adaptador.
[tool-verified: `provisa/ossie/convert.py` docstring lines 7–16; `OSSIE_VERSION = "0.2.0.dev0"`,
`provisa/ossie/convert.py` line 29]

### Exportação

A superfície de exportação canônica é um endpoint HTTP ao vivo. Ela deriva o documento Ossie do estado ao vivo
em cada requisição — sem cache, sem etapa de geração.

```http
GET /admin/ossie
```

A resposta é um documento YAML com `Content-Disposition: attachment; filename=provisa.ossie.yaml`.
[tool-verified: `ossie_router.py` lines 20–33: "THE canonical live Ossie endpoint: the semantic
model derived from live state on every read — no caching, no regeneration step"]

A página de Métricas também oferece um botão **Download** e uma URL de endpoint copiável no painel Ossie
Interchange, ambos apontando para o mesmo endpoint.
[tool-verified: `OssieInterchangePanel.tsx` lines 64–79: `endpointUrl = window.location.origin + OSSIE_ENDPOINT_PATH`]

#### O que é exportado

O adaptador mapeia objetos do Provisa para objetos do Ossie da seguinte forma:

| Objeto Provisa | Objeto Ossie | Notas |
| --- | --- | --- |
| `Table` | `dataset` | `source` = `catalog.schema.table`; chaves primárias/únicas da config de coluna e `UniqueConstraint` |
| `Column` | `field` | `expression` = referência de coluna (dialeto ANSI_SQL); colunas de tempo ganham `dimension.is_time: true` |
| `Relationship` | `relationship` | Alias usado como nome quando definido; relacionamentos computados (alvo de função) são pulados |
| `Metric` | `metric` | `name`, `expression` (ANSI_SQL), `datatype`, `description`, `ai_context` — sem perdas por design |
| `modeling_role` / `modeling_history` | `custom_extensions[].vendor_name="provisa"` | Somente round-trip; outras ferramentas podem ignorar |

[tool-verified: `_table_to_dataset`, `build_ossie_model`, `provisa/ossie/convert.py` lines 90–198;
`_table_to_dataset` comment at line 153: "Computed (function-target) relationships have no dataset
target — not representable in Ossie; skipping is the defined export boundary"]

Governança, RLS, linhagem, e semântica de grafo não são exportadas. Elas podem viajar no slot opcional
`provisa` custom_extensions para fidelidade de round-trip, mas a interoperabilidade nunca depende de outras
ferramentas o lendo. [tool-verified: `provisa/ossie/convert.py` docstring lines 13–15]

Tipos de coluna do Provisa desconhecidos passam verbatim; o adaptador nunca mapeia silenciosamente para um tipo
errado. [tool-verified: `_map_datatype`, `provisa/ossie/convert.py` lines 70–77: "Unknown types
pass through verbatim — mapping silently to a wrong type would corrupt the model"]

#### Mapeamento de tipo

[tool-verified: `_DATATYPE_MAP`, `provisa/ossie/convert.py` lines 35–65]

| Tipo Provisa / fonte | `datatype` Ossie |
| --- | --- |
| `varchar`, `text`, `char`, `uuid`, `string` | `string` |
| `int`, `integer`, `bigint`, `smallint`, `int4`, `int8`, `tinyint` | `integer` |
| `numeric`, `decimal`, `float`, `double`, `real` | `number` |
| `bool`, `boolean` | `boolean` |
| `date` | `date` |
| `time` | `time` |
| `timestamp`, `timestamptz`, `datetime` | `timestamp` |
| qualquer outro | passa verbatim |

### Importação

A importação aceita um documento Ossie (YAML ou JSON) e retorna propostas de registro. Nada é
registrado automaticamente — definições importadas nunca ignoram a etapa de revisão.

```http
POST /admin/ossie/import
Content-Type: text/yaml   (or application/json)

<ossie document>
```

O servidor faz o parse do documento com `parse_ossie_model`, que valida a estrutura e retorna uma
dataclass `OssieImport` contendo tabelas propostas, relacionamentos, e métricas como dicts simples.
Qualquer problema estrutural é um `400` com um erro nomeado por caminho, ex.:
`ossie import: missing semantic_model[0].datasets[1].source`.
[tool-verified: `import_ossie`, `provisa/api/admin/ossie_router.py` lines 36–52:
"Nothing is registered here — imported definitions never bypass registration review"]

#### A tela de revisão

Na UI, o botão **Import** (página Metrics → painel Ossie Interchange) abre um seletor de arquivo.
Depois que o documento é enviado e analisado, um modal de revisão abre com cada tabela, relacionamento, e
métrica proposta listados como um item marcado. O modelador pode desmarcar qualquer coisa para excluí-la.
Clicar em **Apply** registra os itens marcados através das mutações de registro existentes — tabelas
primeiro, depois relacionamentos (que referenciam tabelas), depois métricas.
[tool-verified: `OssieInterchangePanel.tsx` lines 88–165: "Review screen opens with everything
checked; trimming = unchecking"; "Tables first, then relationships... then metrics — each through
the EXISTING registration mutations (REQ-1316)"]

O papel de modelagem e o histórico armazenados em um documento Ossie exportado pelo Provisa fazem round-trip corretamente
através da importação. [tool-verified: `_parse_dataset` custom_extensions handling,
`provisa/ossie/convert.py` lines 287–300: "REQ-1320: round-trip the provisa modeling metadata slot"]

---

## Métricas Entre Protocolos (REQ-1319)

A definição de uma métrica governada — sua expressão, descrição, e `ai_context` — viaja com o
valor para toda superfície de consulta através de uma única expansão do compilador. Não há cópias. O compilador
reserva o esquema `metrics` para acesso SQL; cada protocolo então adiciona seu próprio canal de metadados.

[tool-verified: `METRICS_SCHEMA = "metrics"`, `provisa/compiler/metric_expand.py` line 43;
REQ-1319 requirement text: "the definition (description, ai_context) travels with the value
everywhere, with no copies"]

### SQL / pgwire

Endereça qualquer métrica como uma relação virtual no esquema `metrics`. As colunas de dimensão que você seleciona
tornam-se o GROUP BY:

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

O compilador expande a forma `metrics.<name>` para o agregado agrupado real antes da governança
rodar. Descrições de coluna são exibidas como entradas `pg_description`, então o DBeaver e o `\d+` do psql
as mostram. [tool-verified: `metric_semantic_sql`, `provisa/compiler/metric_expand.py` lines 52–70;
REQ-1319: "description surfaced via pg_description"]

`SELECT *` é rejeitado — nomeie as colunas explicitamente.
[tool-verified: `expand_metric_query`, `provisa/compiler/metric_expand.py` lines 302–306]

### GraphQL

Métricas se projetam dentro do campo raiz `_aggregate` como um bloco `metrics`.
[inferred: per REQ-1319; aggregate_gen.py not read in this session]

O texto da definição (`description`, `ai_context`) aparece na documentação de introspecção do GraphQL, então
ferramentas cientes de esquema e geração de código o captam automaticamente.
[inferred: per REQ-1319: "definition in introspection docs"]

### MCP (agentes de IA)

Duas ferramentas expõem métricas a clientes MCP:

- **`list_metrics`** — retorna todas as métricas governadas visíveis à sessão, com `name`,
  `description`, e `ai_context`.
- **`query_metric`** — aceita um nome de métrica mais uma lista de dimensões e chama o caminho de
  SQL semântico do compilador, retornando o resultado agregado.

[inferred: per REQ-1319: "MCP: list_metrics and query_metric tools carrying ai_context, so agents
select governed meanings instead of composing aggregation SQL"; `provisa/api/mcp/tools.py` not
read in this session]

Agentes que chamam `list_metrics` antes de construir uma consulta selecionam uma métrica governada pelo nome
em vez de escrever SQL de agregação manualmente. O campo `ai_context` é o lugar para colocar o
texto de definição que orienta a seleção correta.

### Arrow Flight

Métricas são endereçáveis como descritores de flight de métrica retornando tabelas Arrow.
[inferred: per REQ-1319: "Arrow Flight: metric flight descriptors returning Arrow tables";
`provisa/api/flight/catalog.py` not read in this session]

Use a mesma forma SQL `metrics.<name>` via o caminho de ticket Flight SQL padrão.

### Bolt / Cypher (Neo4j Browser)

Chame uma métrica usando o procedimento `provisa.metric()`:

```cypher
CALL provisa.metric('net_revenue', ['region']) YIELD region, value
```

[inferred: per REQ-1319: "Bolt/Cypher: a provisa.metric() procedure"; the procedure signature
is inferred from the REQ text and not verified against provisa/bolt/session.py in this session]

Tabelas Fact e Dimension carregam rótulos de nó `:Fact` e `:Dimension` no grafo federado, então
o Bloom renderiza a forma de estrela automaticamente.
[inferred: per REQ-1319 and REQ-1320: "federated graph labels nodes :Fact/:Dimension so Bloom
renders the star"; provisa/cypher/label_map.py not read in this session]

### Consultas em linguagem natural

O matcher de esquema NL resolve vocabulário de métrica em perguntas em linguagem natural diretamente para uma métrica
mais dimensões, depois gera SQL semântico. [tool-verified: `resolve_metric`,
`provisa/nl/schema_matcher.py` is exercised in `test_nl_metrics.py` lines 76–78:
`sql = matcher.resolve_metric("What is the total revenue by region?")` →
`"SELECT region, value FROM metrics.total_revenue GROUP BY region"`]

Tabelas fact são marcadas `[fact]` no prompt NL; tabelas de dimensão são marcadas `[dimension]`. O
matcher favorece caminhos de join fact-para-dimensão ao resolver perguntas.
[tool-verified: `test_format_entities_tags_star_roles`, `tests/unit/test_nl_metrics.py` lines 129–132:
`assert "table: orders [fact]  fields: amount" in block`]

### Streaming

Combine `view_metrics` com `materialize` e um sink Kafka para produzir saída de métrica push-on-change
usando a maquinaria de materialização existente. Nenhum pipeline novo é exigido.
[inferred: per REQ-1319: "Streaming: view_metrics + materialize + Kafka sink yields push-on-change
metrics from existing machinery"; implementation not verified beyond the requirement text]

### Observabilidade (OTel)

Avaliações de métrica são rastreadas e exportáveis como métricas OpenTelemetry.
[inferred: per REQ-1319: "Observability: metric evaluations traced and exportable as OTel metrics";
OTel integration code not read in this session]
