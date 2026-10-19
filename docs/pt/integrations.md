# Integrações {: #integrations }

## Escolhendo um Caminho de Conexão {: #choosing-a-connection-path }

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

## pgwire — Driver PostgreSQL Nativo {: #pgwire-native-postgresql-driver }

O Provisa implementa o protocolo de fio PostgreSQL (versão de protocolo 3.0). Qualquer cliente que fale PostgreSQL se conecta sem um driver personalizado.

Habilite definindo `PROVISA_PGWIRE_PORT` (ex.: `5433`) antes de iniciar o Provisa. Desabilitado quando não definido ou `0`.

### Por que pgwire em vez de JDBC? {: #why-pgwire-instead-of-jdbc }

O driver JDBC leva as consultas pelo Arrow Flight, e para usá-lo você precisa implantar o `provisa-jdbc.jar`. O pgwire não precisa de mais nada. Se você já tem `psql`, DBeaver, SQLAlchemy ou um driver JDBC PG, está pronto, e por isso esse é o caminho mais simples quando você só envia SQL.

Escolha JDBC para ferramentas de BI com assistente de conexão JDBC embutido que puxam grandes conjuntos de resultados, onde o streaming colunar do Arrow Flight compensa. O pgwire aceita o mesmo SQL livre contra todo o esquema publicado, com menos para configurar.

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

### Autenticação {: #authentication }

O campo `password` do pacote de inicialização carrega a credencial, e *o que* a credencial é determina o método: um token de acesso pessoal, um token bearer OIDC ou uma senha contra o provedor configurado. Sob o provedor `basic` com `auth.scram: true`, a senha é provada por SCRAM-SHA-256 em vez de enviada. Certificados de cliente são suportados. No modo trust (`none`), o nome de usuário mapeia diretamente para uma função e a senha é ignorada.

A tabela completa de interface × método está no [Modelo de segurança](security.md#surfaces-and-credentials). MD5 não é suportado; habilite TLS (`PROVISA_PGWIRE_CERT` / `PROVISA_PGWIRE_KEY`) ao rodar sobre uma rede não confiável.

### Limitações {: #limitations }

- Somente SQL. GraphQL e Cypher não são aceitos via pgwire.
- Não é somente leitura. As linhas das tabelas registradas são gravadas com `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `TRUNCATE` e `COPY ... FROM STDIN`, cada uma admitida pelo direito `write` da função, suas concessões de colunas e seu filtro de linhas; veja [Gravações de dados](pgwire.md#data-writes-insert-update-delete-merge-copy-from-stdin).
- Sem definições: `CREATE`, `ALTER` e `DROP` são recusados com SQLSTATE 0A000, exceto a tabela temporária própria de uma sessão; veja [Definições](pgwire.md#definitions-create-alter-drop). `COPY ... TO STDOUT` exporta em `text`, `csv` e `binary`; `COPY ... FROM STDIN` lê `text` e `csv`.
- Consultas a `information_schema` e `pg_catalog` são interceptadas e respondidas a partir de um shim de catálogo DuckDB, então as ferramentas de descoberta de esquema funcionam corretamente.

---

## Driver JDBC {: #jdbc-driver }

O driver JDBC do Provisa usa Arrow Flight como seu transporte subjacente. É o caminho recomendado para ferramentas de BI com um assistente de conexão JDBC.

As consultas passam pelo Arrow Flight sempre que a porta Flight responde quando a conexão é aberta, e os resultados chegam um lote por vez com seus tipos de coluna. A porta Flight é considerada a que fica ao lado da porta HTTP (8815 ao lado de 8001); defina a propriedade `flight_port` quando não for assim. Se a porta Flight não estiver acessível ao conectar, a conexão usa a API HTTP para suas consultas e registra isso em INFO. Uma vez que a conexão está no Flight, um erro do servidor — uma credencial recusada, uma função que o usuário não tem, uma instrução que falhou — é repassado ao chamador com a mensagem do servidor; não é tentado de novo por HTTP.

A navegação no esquema (`getTables`, `getColumns`, chaves primárias e estrangeiras) é lida da mesma porta Flight, com a credencial de login: o driver lista o catálogo da função com que age, então uma tabela ou coluna não concedida a essa função não aparece. Uma chave que referencia uma tabela ou coluna não concedida à função também não é listada. Em uma conexão cuja porta Flight não estava acessível ao conectar, o driver lê o mesmo catálogo por HTTP (`GET /data/catalog`): o servidor monta as duas listagens com uma única função, então tabelas, nomes de coluna, tipos e chaves não dependem da porta que respondeu.

### Conexão {: #connection }

Baixe [provisa-jdbc.jar](https://provisa.dev/dl/jdbc) (sempre a versão mais recente) e adicione-o ao caminho de driver da sua ferramenta.

URL JDBC:

```yaml
jdbc:provisa://<host>:8815
```

A autenticação usa as propriedades JDBC padrão `user` / `password`. O driver as troca por um token de sessão e o envia em cada requisição; um login recusado faz a conexão falhar com o status e o motivo do servidor. O Provisa deriva a função da identidade que fez login. A propriedade opcional `role` (ou `?role=` na URL) pede para agir como uma função que o usuário tem, ou um conjunto delas separado por vírgulas; uma função que o usuário não tem é recusada. Em uma implantação sem provedor de autenticação não há login, e o nome de usuário é tomado como a função solicitada.

### Configuração de Ferramenta de BI {: #bi-tool-setup }

**Tableau**

1. Manage → Drivers → Install Provisa JDBC
2. Connect → Other Databases (JDBC)
3. URL: `jdbc:provisa://localhost:8815`
4. Digite seu usuário e senha quando solicitado

**DBeaver** (o caminho JDBC; o caminho pgwire está acima)

1. Database → New Connection → JDBC
2. Driver: adicione `provisa-jdbc.jar`
3. URL: `jdbc:provisa://localhost:8815`
4. Digite seu usuário e senha na aba Authentication

**Power BI** — use o gateway ODBC com a ponte Provisa JDBC-ODBC (incluída no instalador).

---

## Clientes Arrow Flight {: #arrow-flight-clients }

O Arrow Flight (porta 8815) é o caminho recomendado para ferramentas de dados que o suportam. Os resultados fazem streaming como Arrow RecordBatches sem materializar na memória do Provisa.

### Python (`provisa-client`)

O caminho Python recomendado envolve tanto GraphQL quanto Arrow Flight:

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

### Python (PyArrow bruto) {: #python-raw-pyarrow }

```python
import pyarrow.flight as flight

client = flight.connect("grpc://localhost:8815")
ticket = flight.Ticket(b'{"query": "SELECT id, amount FROM sales.orders"}')
df = client.do_get(ticket).read_all().to_pandas()
```

O Flight carrega sua credencial no payload JSON, num campo `token`, que contém um token bearer do provedor ou um token de acesso pessoal. Tanto o handshake quanto cada ticket o aceitam, e ambos o validam do mesmo modo, então um cliente que se autenticou no handshake ainda apresenta o token em cada `do_get`. Um campo `role` ao lado *solicita* uma função; o servidor deriva as funções permitidas da identidade e substitui pelo valor autorizado, de modo que uma string de função em um ticket nunca é a identidade. Veja [Modelo de segurança](security.md#surfaces-and-credentials).

```python
ticket = flight.Ticket(json.dumps({
    "query": "SELECT id, amount FROM sales.orders",
    "token": "provisa_pat_...",
    "role": "analyst",
}).encode())
```

A navegação também precisa da credencial. `list_flights`, `get_flight_info` e `get_schema` não levam ticket, então leem a credencial do cabeçalho de chamada `authorization: Bearer <token>` e a função solicitada de `x-provisa-role` (uma função, ou um conjunto de funções detidas separado por vírgulas). Com a autenticação ativada, uma chamada de navegação sem credencial é recusada, e o catálogo que ela retorna é o da função autorizada: as tabelas, colunas e comandos concedidos a essa função, e nada mais. Um ticket de catálogo (um ticket sem `query`) é respondido da mesma forma a partir do seu próprio `token` e `role`. Um cliente precisa, portanto, enviar sua credencial antes de navegar; o `provisa-client` faz isso em `list_tables()`. Em uma implantação sem provedor de autenticação essas chamadas listam o catálogo inteiro e não pedem credencial.

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

## Protobuf gRPC (porta 50051) {: #protobuf-grpc-port-50051 }

Caminho de serviço para serviço. O Provisa gera um `.proto` por função e o regenera sempre que o modelo ou a função muda, e cada função vê apenas as tabelas e colunas às quais tem acesso. O serviço tem uma RPC para cada tabela na raiz do esquema da função; as tabelas relacionadas são alcançadas pelos seus relacionamentos, como no GraphQL.

Baixe o proto para sua função:

```bash
curl http://localhost:8001/data/proto/analyst > provisa_analyst.proto
```

Use `grpc_server_reflection` para descobrir o esquema programaticamente. A reflexão descreve o serviço da função com que a chamada atua, o mesmo arquivo do download acima. Com a autenticação desativada, uma chamada de reflexão informa sua função como qualquer outra chamada:

```bash
grpcurl -plaintext -H 'x-provisa-role: analyst' localhost:50051 list
```

Todo RPC deve carregar uma credencial na chave de metadados `authorization`, seja um token do provedor, seja um token de acesso pessoal. `x-provisa-role` solicita uma função do conjunto permitido da identidade; não é uma credencial e nunca foi. Certificados de cliente são suportados. Veja [Modelo de segurança](security.md#surfaces-and-credentials).

Consultas em streaming emitem uma mensagem por linha; mutações são unárias.

---

## Invocando Comandos Entre Protocolos {: #invoking-commands-across-protocols }

Um **comando** é uma função rastreada registrada ou webhook: um chamável registrado no modelo do Provisa com um `kind` (`query` ou `mutation`) e um `impl_kind` que descreve como ele roda. Toda superfície roteia invocações através de um único executor governado (`invoke_tracked_function`) que aplica a atribuição do comando (`visible_to`), o direito `write` para uma mutação e governança uniformemente.

| `impl_kind` | O que roda | Campos de vinculação |
| ------------ | ----------- | --------------- |
| `source_procedure` | Procedimento armazenado em uma fonte registrada (padrão) | `sourceId`, `schemaName`, `functionName` |
| `script` | Script no lado do servidor | `script` |
| `http` | Chamada HTTP de saída | `url`, `method` |
| `grpc` | Chamada gRPC de saída para um servidor externo | `target`, `method` |
| `python` | Chamável Python hospedado pelo Provisa | `callable` (ex.: `demo.py_functions:random_dataset`) |

Quando um comando declara um `return_schema` (JSON Schema com `type: array, items: object`), ele retorna conjunto, e toda superfície o projeta como um conjunto de linhas tipado. Os comandos de demonstração `random_python_set` (impl_kind `python`) e `random_grpc_set` (impl_kind `grpc`) ilustram tanto um chamável hospedado quanto uma ponte gRPC externa retornando linhas com valores aleatórios; ambos são registrados em `config/provisa-install.yaml`.

### Matriz de protocolo {: #protocol-matrix }

| Superfície | Sintaxe | Exemplo |
| --------- | -------- | --------- |
| GraphQL | `kind=query` → campo Query; `kind=mutation` → campo Mutation; prefixado por domínio quando `domain_prefix: true` | `{ ps__random_python_set(rows: 5, seed: 42) { id region amount } }` |
| pgwire / Arrow Flight / MCP `run_sql` | `SELECT * FROM fn(args)` ou `SELECT fn(args)` | `SELECT * FROM random_python_set(5, 42)` |
| Cypher HTTP (`POST /data/cypher`) | `CALL fn(args) YIELD cols` | `CALL random_python_set(5, 42) YIELD id, region, amount` |
| Bolt (Neo4j Browser / driver) | `CALL fn(args)` — argumentos posicionais mapeiam para nomes de argumento declarados | `CALL random_python_set(3, 7)` |
| Provisa gRPC (porta 50051) | Unário `CallCommand(CommandRequest{name, args_json})` → `CommandResponse{rows_json}` | `grpcurl -d '{"name":"random_python_set","args_json":"{\"rows\":5}"}' ... ProvisaService/CallCommand` |

O campo `kind` controla apenas o posicionamento no GraphQL. As superfícies SQL, Cypher, Bolt e gRPC aceitam comandos `query` e `mutation` de forma idêntica.

---

## Apollo Federation

O Provisa pode atuar como um subgraph Federation v2, expondo seu esquema publicado a um Apollo Router ou Apollo Gateway.

### Configuração {: #setup }

Habilite federation em `config.yaml`:

```yaml
federation:
  enabled: true
  subgraph_name: provisa-data
```

O Provisa gera diretivas `@key` em colunas de chave primária e `@external`/`@provides` em relacionamentos entre subgraphs automaticamente.

### Registro com Apollo Router {: #register-with-apollo-router }

No seu `supergraph.yaml`:

```yaml
subgraphs:
  provisa-data:
    routing_url: http://provisa:8001/data/graphql
    schema:
      subgraph_url: http://provisa:8001/data/graphql
```

Rode `rover supergraph compose --config supergraph.yaml` para gerar o esquema do supergraph.

### Entidades {: #entities }

O Provisa responde a consultas `_entities` para joins entre subgraphs. Qualquer tabela com uma chave primária é automaticamente resolvível como uma entidade Federation.

---

## Importação Hasura v2 / DDN {: #hasura-v2-ddn-import }

Veja [docs/import.md](import.md) para migração do Hasura para o Provisa.

---

## Kafka

Veja [docs/sources.md](sources.md#kafka-sources) para configuração de tópico Kafka como tabelas somente leitura e sinks de resultado de consulta.

---

## Verificadores de Qualidade de Dados {: #data-quality-checkers }

Soda Core e Great Expectations se conectam ao Provisa pelo pgwire, como qualquer outro cliente postgres. Essa é toda a integração: o verificador mantém um driver postgres e varre a exibição federada, de modo que uma tabela Snowflake, uma tabela Iceberg e uma coleção Mongo são todas verificadas pelo mesmo dialeto de contrato, sem verificador por sistema.

A varredura roda em um interpretador filho — `python -m provisa.dq.worker` — que é o único lugar onde `soda_core` ou `great_expectations` é importado. Nada é vinculado ao processo do servidor, e uma falha do verificador derruba um subprocesso em vez do loop de eventos.

Os resultados da varredura chegam como linhas de fonte comuns, então cadência, atualidade, eventos, linhagem, governança, RLS, a grade e exportação se aplicam todos sem um segundo mecanismo. A autoria de contrato, o envelope de resultado e o registro derivado são cobertos em [docs/sources.md](sources.md#data-quality-checkers).

### Instalando um verificador {: #installing-a-checker }

Nenhuma das bibliotecas vem instalada por padrão. O instalador pergunta qual você quer, e a resposta se torna `dq_checker: none|soda|gx` em `~/.provisa/config.yaml`. No nível Docker, `scripts/provisa` transforma isso no argumento de build `PROVISA_EXTRAS`; no nível nativo, `first-launch.sh` instala o extra pyproject correspondente no venv.

| `dq_checker` | Biblioteca | Licença | Plano cloud hospedado |
| -------------- | --------- | --------- | -------------------- |
| `soda` | `soda-postgres` | Elastic License 2.0 | Recusado (`cloud_eligible: false`) |
| `gx` | `great-expectations[postgresql]` | Apache 2.0 | Permitido |

O Elastic License 2.0 proíbe fornecer o software a terceiros como um serviço hospedado, que é o que rodar o Soda dentro do plano SaaS em nome de um locatário seria. Uma implantação hospedada que deseja Soda aponta para um endpoint Soda que o operador roda por conta própria. Veja [docs/configuration.md](configuration.md#data-quality-checkers-soda-great_expectations) para as chaves de conexão.

---

## Intercâmbio semântico Apache Ossie {: #apache-ossie-semantic-interchange }

O Provisa troca modelos semânticos com o Apache Ossie (especificação 0.2.0.dev0, em incubação; antes Open Semantic Interchange) por meio de um adaptador de fronteira. O vocabulário interno do Provisa nunca é renomeado para o do Ossie. A especificação declara prováveis mudanças incompatíveis, então o acoplamento fica restrito ao adaptador.

A especificação em rascunho mudou de forma sem mudar sua string de versão. O adaptador segue a
especificação como está no commit `642cb894` do `apache/ossie` (2026-10-10): um modelo por documento,
com `version`, `name`, `datasets`, `relationships` e `metrics` na raiz do documento, e
um `datatype` enumerado. A exportação é verificada nos testes unitários contra o
`core-spec/ossie-schema.json` desse commit.

### Exportação {: #export }

A superfície canônica de exportação é um endpoint HTTP ativo. Ele deriva o documento Ossie do estado ativo a cada requisição, sem cache e sem etapa de geração.

```http
GET /admin/ossie
```

A resposta é um documento YAML com `Content-Disposition: attachment; filename=provisa.ossie.yaml`.

A página Métricas também oferece um botão **Baixar** e uma URL de endpoint copiável no painel Ossie
Interchange, ambos apontando para o mesmo endpoint.

#### O que é exportado {: #what-is-exported }

O adaptador mapeia objetos do Provisa para objetos do Ossie assim:

| Objeto do Provisa | Objeto do Ossie | Notas |
| --- | --- | --- |
| `Table` | `dataset` | `source` = `catalog.schema.table`; chaves primárias/únicas a partir da configuração das colunas e de `UniqueConstraint` |
| `Column` | `field` | `expression` = referência de coluna (dialeto ANSI_SQL); colunas de tempo recebem `dimension.is_time: true` |
| `Relationship` | `relationship` | O alias é usado como nome quando definido; relacionamentos calculados (com destino função) são ignorados |
| `Metric` | `metric` | `name`, `expression` (ANSI_SQL), `datatype` (mapeado como o de uma coluna), `description`, `ai_context` |
| `modeling_role` / `modeling_history` | `custom_extensions[].vendor_name="provisa"` | Apenas ida e volta; outras ferramentas podem ignorar |

Um relacionamento sobre várias colunas lista cada coluna em `from_columns` e `to_columns`. Um
modelo sem nenhuma tabela é recusado com `422`: um documento Ossie tem pelo menos um dataset.

Governança, RLS, linhagem e semântica de grafos não são exportadas. Elas podem viajar no slot opcional
`provisa` de custom_extensions para a fidelidade da ida e volta, mas o intercâmbio nunca depende de outras
ferramentas o lerem.

Um tipo de coluna fora do vocabulário do Ossie é escrito como `Opaque`, com o nome próprio do tipo na
extensão `provisa` do campo, que é o que a especificação manda para um tipo conhecido para o qual não tem
membro. Uma coluna sem tipo registrado não informa `datatype`. O adaptador nunca mapeia para um tipo que a
coluna não tem.

#### Mapeamento de tipos {: #type-mapping }

| Tipo do Provisa / da fonte | `datatype` do Ossie |
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
| qualquer outro | `Opaque`, com o nome do tipo na extensão `provisa` |

O `Decimal` do Ossie não traz precisão nem escala, então `numeric(10,2)` é exportado como `Decimal`
e volta de uma importação como `decimal`.

### Importação {: #import }

A importação aceita um documento Ossie (YAML ou JSON) e retorna propostas de registro. Nada é registrado automaticamente, e as definições importadas nunca pulam a etapa de revisão.

```http
POST /admin/ossie/import
Content-Type: text/yaml   (or application/json)

<ossie document>
```

O servidor analisa o documento com `parse_ossie_model`, que valida a estrutura e retorna uma
dataclass `OssieImport` contendo as tabelas, os relacionamentos e as métricas propostos como dicionários simples.
Qualquer problema estrutural é um `400` com um erro nomeado pelo caminho, por exemplo
`ossie import: missing $.datasets[1].source`.

O importador lê apenas a forma plana do documento. Três coisas são recusadas pelo nome:

- Um documento que envolve seu modelo em `semantic_model`, a forma que a especificação em rascunho tinha antes de
  setembro de 2026. O erro diz que o documento é anterior à forma plana e como convertê-lo.
- Uma `version` diferente de `0.2.0.dev0`.
- Um `datatype` que não é membro do vocabulário do Ossie; o erro nomeia o caminho e lista
  os membros.

Uma coluna ou métrica proposta traz o tipo que o seu datatype Ossie representa (`Integer` propõe
`bigint`, `DateTime` propõe `timestamp`). `Opaque` propõe o tipo que a extensão `provisa` nomeia
quando o documento veio do Provisa, e nenhum tipo caso contrário. Um `ai_context` dado como
objeto é lido pelas suas `instructions`.

Um campo só é proposto como coluna quando sua expressão é uma coluna da fonte. Um campo
cuja expressão é um cálculo não é proposto; a tela de revisão o nomeia sob sua tabela
como não importado. Um campo que renomeia sua coluna é proposto como essa coluna, com o nome do
campo como alias da coluna. A `source` de um dataset deve ter três partes,
`source.schema.table`; uma consulta ou um nome mais curto é recusado, e o erro diz isso.

#### Vindo do dbt, Cube, Snowflake e outras camadas semânticas {: #coming-from-dbt-cube-snowflake-and-other-semantic-layers }

O projeto Apache Ossie mantém conversores entre o Ossie e os formatos de modelo de outras ferramentas. Execute
o conversor da sua ferramenta e depois envie sua saída ao endpoint de importação ou escolha o arquivo na
tela de revisão abaixo. O Provisa lê o documento Ossie; ele não lê os arquivos da outra ferramenta.

| Origem | Comando do conversor | O que ele lê |
|---|---|---|
| dbt (camada semântica) | `ossie-dbt msi-to-ossie -i target/semantic_manifest.json -o semantic_model.yaml` | O `semantic_manifest.json` que o dbt escreve para o MetricFlow |
| Cube | `ossie-cube import -i model/ -o model.yaml` | Um diretório de modelo do Cube, ou arquivos individuais de cubos e views |

O mesmo repositório tem conversores para Snowflake, Databricks, Microsoft, Salesforce,
ThoughtSpot, Sigma, Omni, Hex, GoodData e outros; cada um tem seu próprio README.

Dois limites a conhecer antes de converter:

- O conversor do dbt lê apenas o manifesto da camada semântica. Um projeto dbt sem modelos
  semânticos não tem nada para ele converter. Ele descarta métricas de conversão, métricas privadas e
  entidades de chave natural, e imprime um aviso para cada uma.

- A saída de um conversor segue a especificação como o próprio commit desse conversor a tem. O Provisa lê
  a forma plana do commit `642cb894` do `apache/ossie`; um conversor anterior a setembro de 2026
  escreve o invólucro `semantic_model`, que o Provisa recusa pelo nome.

Os dois caminhos foram executados em 2026-10-10 com os conversores instalados a partir do commit `642cb894` do `apache/ossie`
(eles não são publicados como pacotes; instalam-se a partir dos diretórios `converters/dbt` e
`converters/cube` do repositório e precisam de Python 3.11 ou posterior). Um modelo de
duas tabelas, um relacionamento e uma métrica foi exportado do Provisa, convertido para um manifesto semântico do dbt
e para um modelo Cube, convertido de volta para Ossie por `ossie-dbt msi-to-ossie` e
`ossie-cube import`, e lido por `parse_ossie_model`:

| Caminho | Resultado |
|---|---|
| Modelo Cube → Ossie → Provisa | Lido. Tabelas, chaves, tipos de coluna, o relacionamento e a métrica voltaram; o nome do relacionamento era o do conversor (`orders_to_customers`). |
| Manifesto semântico do dbt → Ossie → Provisa | Lido. Tabelas, chaves primárias e a métrica voltaram. Os tipos de coluna não (a saída do conversor não informa nenhum), e o relacionamento voltou como o conversor o derivou das entidades do dbt, que não era o registrado. Revise os relacionamentos antes de aplicar. |

O conversor do dbt recusa um modelo em que alguma tabela tem uma restrição de unicidade sobre várias
colunas ("MetricFlow entities cannot represent composite keys losslessly").

#### A tela de revisão {: #the-review-screen }

Na interface, o botão **Importar** (página Métricas → painel Ossie Interchange) abre um seletor de arquivos. Depois que o documento é enviado e analisado, abre-se uma janela de revisão com cada tabela, relacionamento e métrica proposta listada como item marcado. O modelador pode desmarcar qualquer coisa para excluí-la. Clicar em **Aplicar** registra os itens marcados pelas mutações de registro existentes: primeiro as tabelas, depois os relacionamentos (que referenciam tabelas), depois as métricas.

A função de modelagem e o histórico guardados em um documento Ossie exportado pelo Provisa fazem a ida e volta corretamente
na importação.

### Arquivo de sources do dbt {: #dbt-sources-file }

Para um projeto dbt que não usa a camada semântica, o Provisa escreve diretamente um `sources.yml` do dbt.
Ele é derivado do modelo ativo a cada requisição.

```http
GET /admin/dbt/sources?role=<role id>
```

A resposta é YAML com `Content-Disposition: attachment; filename=provisa.sources.yml`. O
chamador precisa de `org_settings`, e `role` deve ser uma função que o chamador tem; o detentor de
`access_config` pode nomear qualquer função. O painel de intercâmbio da página Métricas oferece o mesmo download
para a função em uso.

O arquivo lista cada tabela servida à função, sob uma source do dbt por fonte e esquema (nomeada
pelo id da fonte, ou `<source>_<schema>` quando uma fonte tem tabelas em mais de um esquema),
com a descrição da tabela, as descrições das colunas servidas e estes testes:

| O modelo informa | O arquivo escreve |
|---|---|
| Uma chave primária de uma coluna | `unique` e `not_null` nessa coluna |
| Uma chave primária de várias colunas | `not_null` em cada coluna |
| Uma restrição de unicidade de uma coluna | `unique` nessa coluna |
| Um relacionamento entre duas tabelas ambas servidas à função | `relationships` na coluna de origem, para `source('<source>', '<table>')` |

Uma coluna não servida à função fica ausente, assim como uma tabela sem nenhuma coluna servida. Uma chave sobre
várias colunas não tem teste padrão do dbt; o comentário inicial do arquivo nomeia cada uma, e nenhum
teste é escrito para ela. O modelo não registra a nulabilidade, então `not_null` aparece só em colunas de chave.
O arquivo não traz métricas, segurança de linha, mascaramento, funções nem linhagem, e seu comentário inicial
diz isso.

Um arquivo gerado a partir de um modelo de duas tabelas e um relacionamento foi analisado pelo dbt-core 1.12.5
com dbt-duckdb 1.11.0: duas sources e cinco testes, sem avisos. O dbt-core 1.9.11 não lê
seus testes `relationships`, que aninham seus argumentos sob `arguments`; versões intermediárias
não foram testadas.

---

## Métricas Entre Protocolos {: #metrics-across-protocols }

A definição de uma métrica governada — sua expressão, descrição, e `ai_context` — viaja com o
valor para toda superfície de consulta através de uma única expansão do compilador. Não há cópias. O compilador
reserva o esquema `metrics` para acesso SQL; cada protocolo então adiciona seu próprio canal de metadados.

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
as mostram.

`SELECT *` é rejeitado, então nomeie as colunas que você quer.

### GraphQL

Métricas se projetam dentro do campo raiz `_aggregate` como um bloco `metrics`.

O texto da definição (`description`, `ai_context`) aparece na documentação de introspecção do GraphQL, então
ferramentas cientes de esquema e geração de código o captam automaticamente.

### MCP (agentes de IA) {: #mcp-ai-agents }

Duas ferramentas expõem métricas a clientes MCP:

- **`list_metrics`** — retorna todas as métricas governadas visíveis à sessão, com `name`,
  `description`, e `ai_context`.
- **`query_metric`** — aceita um nome de métrica mais uma lista de dimensões e chama o caminho de
  SQL semântico do compilador, retornando o resultado agregado.

Agentes que chamam `list_metrics` antes de construir uma consulta selecionam uma métrica governada pelo nome
em vez de escrever SQL de agregação manualmente. O campo `ai_context` é o lugar para colocar o
texto de definição que orienta a seleção correta.

### Arrow Flight

Métricas são endereçáveis como descritores de flight de métrica retornando tabelas Arrow.

Use a mesma forma SQL `metrics.<name>` via o caminho de ticket Flight SQL padrão.

### Bolt / Cypher (Neo4j Browser)

Chame uma métrica usando o procedimento `provisa.metric()`:

```cypher
CALL provisa.metric('net_revenue', ['region']) YIELD region, value
```

Tabelas Fact e Dimension carregam rótulos de nó `:Fact` e `:Dimension` no grafo federado, então
o Bloom renderiza a forma de estrela automaticamente.

### Consultas em linguagem natural {: #natural-language-queries }

O matcher de esquema NL resolve vocabulário de métrica em perguntas em linguagem natural diretamente para uma métrica
mais dimensões, depois gera SQL semântico.

Tabelas fact são marcadas `[fact]` no prompt NL; tabelas de dimensão são marcadas `[dimension]`. O
matcher favorece caminhos de join fact-para-dimensão ao resolver perguntas.

### Streaming

Combine `view_metrics` com `materialize` para manter uma view de métricas atualizada conforme suas
entradas mudam, usando a maquinaria de materialização existente. Nenhum pipeline novo é exigido.

### Observabilidade (OTel) {: #observability-otel }

Avaliações de métrica são rastreadas e exportáveis como métricas OpenTelemetry.
