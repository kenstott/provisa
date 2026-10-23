# Tipos de Fonte {: #source-types }

## Modelo de Execução {: #execution-model }

Toda consulta acaba passando pelo motor de federação, e é isso que permite unir qualquer fonte a qualquer outra. Como uma fonte chega lá depende de como o Provisa a alcança, e isso coloca cada fonte em um de cinco grupos.

| Categoria | Tem Driver Direto | Tem Conector Federado | Exemplos |
| --- | --- | --- | --- |
| **Direta-capaz** | Sim | Sim | PostgreSQL, MySQL, MariaDB, SingleStore, SQL Server, Oracle, DuckDB |
| **Somente federação** | Não | Sim | Redshift, Druid, Exasol, Hive, Iceberg, Delta Lake, Hive (apoiado em S3) |
| **Leitura direta (réplica)** | Sim | Sim | Snowflake, Databricks, ClickHouse — o driver lê os dados e pousa uma réplica; consultas rodam contra a réplica no motor ativo |
| **Direta (sem conector entre fontes)** | Sim | Não | Athena — instruções de fonte única rodam inteiras no Athena; joins entre fontes pousam uma réplica completa |
| **Materializar → Federação** | Não | Não | REST/OpenAPI, GraphQL remoto, gRPC, Neo4j Cypher, SPARQL, WebSocket, RSS, CSV, SQLite, Parquet, Ingest (receptor push), GovData, SharePoint, Salesforce, ServiceNow, Cloud inventory, Splunk, Qdrant, Azure Cosmos DB, Logs, Google Workspace, Microsoft 365 |

Fontes **direta-capazes** respondem a uma consulta que só toca nelas pelo próprio driver, em menos de 100 ms, sem passar pelo motor de federação. Elas mantêm o conector completo, então continuam se unindo a outras fontes pelo motor.

Fontes **somente federação** não têm driver direto. Toda consulta chega a elas pela camada de federação.

Fontes de **leitura direta (réplica)** têm um DirectDriver que lê do warehouse nativamente (Arrow-nativo quando possível) e pousa uma réplica no armazenamento de materialização do motor ativo. As consultas então rodam contra essa réplica. Veja [Warehouses como Fontes Nomeadas](#warehouses-as-named-sources).

Fontes **diretas (sem conector entre fontes)** têm um DirectDriver que executa uma instrução de fonte única inteira na própria fonte, com todo filtro, coluna e limite empurrado até ela. Não existe conector Trino para elas, então quando uma instrução une uma delas a outra fonte, o driver lê os dados e pousa uma réplica, e o join roda contra essa réplica no motor ativo.

Fontes de **materialização** também não têm conector federado. O Provisa busca os dados delas, na inicialização ou no momento da consulta, e os guarda em cache como Parquet no S3 ou no PostgreSQL. Dali o motor de federação os alcança para consultas entre fontes.

---

## Todas as Fontes {: #all-sources }

O Provisa registra **73** tipos de fonte, os que o formulário de Fontes oferece. As tabelas abaixo cobrem os 73; o índice é a contagem.

| # | Grupo | Tipos de fonte |
| --- | --- | --- |
| 1–15 | [RDBMS](#rdbms) | `postgresql`, `mysql`, `mariadb`, `singlestore`, `sqlserver`, `oracle`, `duckdb`, `cockroachdb`, `yugabytedb`, `greenplum`, `tidb`, `firebird`, `airport`, `saphana`, `hiveserver2` |
| 16–24 | [Data warehouses em nuvem](#cloud-data-warehouses) | `snowflake`, `bigquery`, `databricks`, `redshift`, `athena`, `fabric`, `synapse`, `trino`, [`motherduck`](#motherduck) |
| 25–29 | [Analytics / OLAP](#analytics-olap) | `clickhouse`, `druid`, `exasol`, `elasticsearch`, `pinot` |
| 30–33 | [Data lake / formatos de tabela aberta](#data-lake-open-table-formats) | `iceberg`, `delta_lake`, `hive`, `hive_s3` |
| 34–38 | [NoSQL](#nosql) | `mongodb`, `cosmosdb`, `qdrant`, `cassandra`, `redis` |
| 39–40 | [Fontes de código](code-sources.md) | `node`, `python` |
| 41–43 | [Streaming](#streaming) | `kafka`, `websocket`, `rss` |
| 44 | [Receptor push](#push-receiver) | `ingest` |
| 45–46 | [Grafo e semântico](#graph-semantic) | `neo4j`, `sparql` |
| 47–51 | [Baseado em arquivo](#file-based) | `sqlite`, `csv`, `parquet`, `files`, `logs` |
| 52–53 | [Observabilidade e outros](#observability-other) | `google_sheets`, `prometheus` |
| 54–61 | [SaaS empresarial](#enterprise-saas-connectors) | `sharepoint`, `salesforce`, `servicenow`, `jira`, `cloudops`, `splunk`, `google_workspace`, `microsoft_365` |
| 62–67 | [Fontes de API](#api-sources) | `openapi`, `graphql_remote`, `grpc_remote`, GitHub, GitLab, Stripe |
| 68 | [GovData](#govdata) | `govdata` |
| 69–70 | [Verificadores de qualidade de dados](#data-quality-checkers) | `soda`, `great_expectations` |
| 71 | [Data profiler](#data-profiler) | `data_profiler` |
| 72 | [Conjuntos de dados Kaggle](#kaggle-datasets) | Kaggle (preparado pelo formulário de Fontes; registra-se como fonte `files` — veja [Conjuntos de dados Kaggle](#kaggle-datasets)) |
| 73 | [Wikipedia](#wikipedia) | Wikipedia (registra-se como fonte `files` que percorre as páginas indicadas — veja [Wikipedia](#wikipedia)) |

Referência para todo tipo de fonte que o Provisa suporta. "Driver direto" significa que consultas de fonte única são executadas contra a fonte nativamente (sub-100ms). "Nome do Conector" é o conector federado usado quando a fonte participa de JOINs multi-fonte.

### RDBMS

| Tipo de Fonte | Driver Direto | Nome do Conector | Dialeto | Mutações |
| ------------ | -------------- | ----------------- | ----------------- | ----------- |
| `postgresql` | asyncpg | postgresql | postgres | Sim |
| `mysql` | aiomysql | mysql | mysql | Sim |
| `mariadb` | aiomysql | mariadb | mysql | Sim |
| `singlestore` | — | singlestore | singlestore | Federada |
| `sqlserver` | aioodbc | sqlserver | tsql | Sim |
| `oracle` | oracledb | oracle | oracle | Sim |
| `duckdb` | duckdb | memory | duckdb | Sim |
| `cockroachdb` | asyncpg (pg wire) | postgresql | postgres | Sim |
| `yugabytedb` | asyncpg (pg wire) | postgresql | postgres | Sim |
| `greenplum` | asyncpg (pg wire) | postgresql | postgres | Sim |
| `tidb` | aiomysql (mysql wire) | mysql | mysql | Sim |
| `firebird` | — | — (extensão DuckDB) | — | Não |
| `airport` | — | — (extensão DuckDB) | — | Não |
| `saphana` | SQLAlchemy (`hana+hdbcli`) | — | — | Não |
| `hiveserver2` | impyla (HiveServer2 over Thrift) | — | hive | Não |

Bancos compatíveis no protocolo reaproveitam o driver JDBC, o driver assíncrono nativo e o dialeto de um protocolo base. CockroachDB, YugabyteDB e Greenplum usam o protocolo do PostgreSQL, e o TiDB o do MySQL. Eles só precisam de entradas no registro, sem código de conector novo.

`firebird` (Firebird 3/4/5) e `airport` (servidor Arrow Flight) são tipos de fonte registrados que as extensões da comunidade do DuckDB alcançam no lugar quando o DuckDB é o motor ativo. Eles não têm driver direto nem conector federado.

#### `saphana`

O SAP HANA é lido por seu próprio driver e replicado no armazenamento do motor ativo, então é alcançável em todo motor. O formulário pede host, porta (padrão 39015), nome de usuário, senha e banco de dados; em um sistema multi-tenant o banco de dados indica o tenant. Register Table lista os esquemas que o usuário pode ler, deixando de fora os esquemas de sistema do HANA.

#### `hiveserver2`

Um endpoint HiveServer2 lido diretamente por Thrift, para uma implantação Hive em que o endpoint é a única coisa alcançável. Ele é distinto de `hive` e `hive_s3`, que leem os arquivos e o metastore do warehouse e nunca passam pelo HiveServer2. As linhas são lidas pelo endpoint e replicadas no armazenamento do motor ativo. O formulário pede host, porta (padrão 10000), nome de usuário, senha e banco de dados; `federation_hints.auth_mechanism` seleciona a autenticação (padrão `PLAIN`, por exemplo `GSSAPI` para Kerberos).

### Data Warehouses em Nuvem {: #cloud-data-warehouses }

| Tipo de Fonte | Driver Direto | Nome do Conector | Dialeto | Mutações | Notas |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `snowflake` | SnowflakeDriver | snowflake | snowflake | Federada | Lê via snowflake-connector-python; pousa réplica; `account`/`warehouse`/`role` em `federation_hints` |
| `bigquery` | — | bigquery | bigquery | Federada | Sem DirectDriver; alcançado via motor de federação ou ATTACH do motor BigQuery |
| `databricks` | DatabricksDriver | delta_lake | databricks | Federada | Lê via databricks-sql-connector (Cloud Fetch, Arrow); pousa réplica; `http_path` exigido em `federation_hints` |
| `redshift` | — | redshift | redshift | Federada | — |
| `athena` | AthenaDriver | — | athena | Parcial (INSERT sempre; UPDATE/DELETE/MERGE apenas em Iceberg) | Consulta serverless sobre S3; instruções de fonte única são executadas inteiramente no Athena; junções entre fontes aterram uma réplica; `region` obrigatória em `federation_hints` |
| `fabric` | MssqlWarehouseDriver | — | tsql | Federada | Microsoft Fabric Warehouse; T-SQL sobre TDS, autenticação Azure AD; pousa réplica |
| `synapse` | MssqlWarehouseDriver | — | tsql | Federada | Azure Synapse SQL; T-SQL sobre TDS, autenticação Azure AD; pousa réplica |
| `trino` | SQLAlchemyDriver | — | — | Federada | Coordenador Trino/Presto remoto lido via o dialeto SQLAlchemy trino; pousa réplica em qualquer motor |

#### `athena`

O Amazon Athena é um serviço de consulta serverless sobre dados no S3, cobrado por byte varrido. Uma instrução só sobre tabelas do Athena vai direto ao Athena, com filtros, listas de colunas e `LIMIT` empurrados até ele, então o Athena varre e cobra só os bytes de que essa instrução precisa. Uma instrução que une uma tabela do Athena a qualquer outra fonte primeiro pousa uma réplica completa vinda do Athena. Isso lê a tabela inteira, e o Athena cobra por isso; depois o join roda contra a réplica. As configurações Replicate e de proteção de carga da fonte decidem com que frequência um pouso acontece, e esta nota é o único aviso sobre esse custo.

**Descoberta.** O Register Table chama a API de metadados do Athena: `ListDatabases`, `ListTableMetadata` e `GetTableMetadata`. Essas chamadas não executam consulta nenhuma, então listar bancos e tabelas e ler as colunas de uma tabela não custa nada por byte. As colunas de uma tabela só são buscadas quando você seleciona essa tabela, com as chaves de partição depois das colunas normais. `array<…>`, `map<…>` e `struct<…>` viram `text`, e um tipo desconhecido é recusado pelo nome.

**Campos da fonte.**

| Campo | Onde | Obrigatório | Padrão | Notas |
| --- | --- | --- | --- | --- |
| `database` | fonte | Sim | — | O banco de dados Glue consultado por padrão |
| `username` | fonte | Não | — | ID da chave de acesso AWS; pode ser `${secret:…}` ou `${env:…}` |
| `password` | fonte | Não | — | Chave de acesso secreta AWS; pode ser `${secret:…}` ou `${env:…}` |
| `region` | `federation_hints` | Sim | — | Região AWS, ex. `us-east-1` |
| `workgroup` | `federation_hints` | Não | `primary` | Grupo de trabalho do Athena |
| `s3_staging_dir` | `federation_hints` | Não | padrão do grupo | Onde o Athena grava os resultados; obrigatório quando o grupo de trabalho não define um |
| `catalog` | `federation_hints` | Não | `AwsDataCatalog` | Catálogo de dados do Athena |
| `role_arn` | `federation_hints` | Não | — | ARN do perfil IAM assumido a cada chamada |

Ambos `username` e `password` vazios significa que a cadeia de credenciais do boto3 é usada: perfil de instância, variáveis de ambiente (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`) ou um perfil nomeado. Um sem o outro é recusado.

**Escritas.** Toda tabela registrada aceita `INSERT`. `UPDATE`, `DELETE` e `MERGE` só funcionam numa tabela Iceberg, e o formato é anotado no registro (`registered_tables.table_format`). Numa tabela Hive, uma escrita que pede `UPDATE`, `DELETE` ou `MERGE` é recusada, nomeando a tabela e a operação. O Athena não devolve linhas escritas, então uma cláusula `RETURNING` é recusada e o GraphQL não oferece campos de mutation para essas tabelas. Os caminhos de escrita são `INSERT`, `UPDATE`, `DELETE` e `COPY`. `INSERT … ON CONFLICT`, `UPDATE … FROM`, `DELETE … USING` e `TRUNCATE` são recusados, nomeando a forma.

```yaml
- id: analytics-athena
  type: athena
  database: my_glue_database
  username: ${env:ATHENA_ACCESS_KEY_ID}
  password: ${env:ATHENA_SECRET_ACCESS_KEY}
  federation_hints:
    region: us-east-1
    workgroup: primary
    s3_staging_dir: s3://my-bucket/athena-results/
```

Com um perfil assumido a cada chamada (cadeia de credenciais para a sessão base):

```yaml
- id: analytics-athena
  type: athena
  database: my_glue_database
  federation_hints:
    region: us-east-1
    role_arn: arn:aws:iam::123456789012:role/AthenaQueryRole
    s3_staging_dir: s3://my-bucket/athena-results/
```

### MotherDuck

`motherduck` é um banco de dados DuckDB hospedado no MotherDuck. No motor DuckDB ele é anexado no lugar; no motor Postgres continua sendo o conector do foreign data wrapper `pg_duckdb`.

**Conexão.** `database` nomeia o banco de dados do MotherDuck, ou um compartilhamento pelo seu endereço de compartilhamento. `password` é o token do MotherDuck, resolvido como qualquer segredo de fonte.

```yaml
- id: reference
  type: motherduck
  database: analytics            # attached as md:analytics
  password: ${secret:<name>}     # the MotherDuck token, or a reference to it
```

O motor DuckDB emite `ATTACH 'md:<database>'`; para um compartilhamento, `database` é o endereço do compartilhamento (`_share/...`), então o anexo é `md:_share/...`.

- **Tratamento do token.** O motor faz login com `SET GLOBAL motherduck_token = ?`, com o token passado como valor vinculado. Ele não está no texto de nenhuma instrução, então nenhum log, trace ou erro o contém.
- **Sem token.** Uma fonte sem `password` é recusada pelo nome antes que qualquer coisa seja anexada (`a MotherDuck source signs in with its token (the password field); none is set`). Sem essa verificação, a extensão iniciaria um login interativo e esperaria cerca de um minuto. Uma fonte sem `database` também é recusada.
- **Uma conta por motor.** Um motor faz login em uma conta do MotherDuck. Uma segunda fonte MotherDuck com um token diferente é recusada pelo nome: "one DuckDB engine reads one MotherDuck account".
- **Escritas** passam pelo motor, como no motor Postgres.

**A execução híbrida (dual)** não precisa de configuração. No motor DuckDB toda fonte é anexada em uma só conexão, então uma instrução que faz join de uma tabela de uma fonte de arquivo DuckDB com uma tabela do MotherDuck roda como uma única instrução DuckDB, e o MotherDuck decide onde cada parte roda.

> **Movimentação de dados.** Na execução dual, o MotherDuck pode mover linhas locais para o seu lado nas partes que executa. Uma instrução assim pode, portanto, enviar dados do arquivo local para a conta do MotherDuck da organização. Não faça join de um arquivo com dados que precisam ficar no nó com uma fonte MotherDuck, a menos que enviá-los para essa conta seja aceitável.

Uma fonte MotherDuck é servida apenas pelos motores DuckDB e Postgres. Nenhum outro motor tem um conector para ela, e nada pousa suas tabelas como réplicas ali.

### Analytics / OLAP

| Tipo de Fonte | Driver Direto | Nome do Conector | Dialeto | Mutações | Notas |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `clickhouse` | ClickHouseDriver | clickhouse | clickhouse | Federada | Lê via clickhouse-connect (HTTP); `secure: "true"` em `federation_hints` para TLS |
| `druid` | — | druid | druid | Não | — |
| `exasol` | — | exasol | exasol | Não | — |
| `elasticsearch` | HTTP (motores nativos) | elasticsearch (Trino) | — | Não | No Trino o conector o lê, suas propriedades vêm da DSL de mapeamento do tipo; em todo outro motor o Provisa lê o índice via HTTP (índices e mapeamento para Register Table, uma leitura por scroll para pouso) e pousa as linhas |
| `pinot` | — | pinot | — | Não | Conector Trino `pinot`; `pinot.controller-urls` = host:porta do controlador Pinot |

### Data Lake / Formatos de Tabela Aberta {: #data-lake-open-table-formats }

Esses tipos de fonte são somente federação. Eles não têm driver direto nem dialeto.

| Tipo de Fonte | Nome do Conector | Time Travel | Notas |
| ------------ | ----------------- | ------------- | ------- |
| `iceberg` | iceberg | Sim (argumento `as_of`) | — |
| `delta_lake` | delta_lake | Sim (argumento `as_of`) | — |
| `hive` | hive | Não | — |
| `hive_s3` | hive | Não | Hive apoiado em S3 |

### NoSQL

`mongodb`, `cassandra`, e `redis` têm conectores Trino (`redis` constrói suas propriedades a partir da DSL de mapeamento do tipo).

| Tipo de Fonte | Nome do Conector | Mutações |
| ------------ | ----------------- | ----------- |
| `mongodb` | mongodb | Não |
| `cassandra` | cassandra (Trino); leitura CQL via cassandra-driver em todo outro motor | Não | Keyspaces são esquemas; Register Table lista as tabelas de um keyspace e tipa as colunas a partir dos metadados de esquema do cluster (chaves de partição como chaves primárias); o extra `cassandra` instala o driver |
| `redis` | redis (Trino); leitura redis-py sem HTTP em todo outro motor | Não | Um prefixo de chave `<table>:*` é uma tabela e um hash é uma linha; Register Table lista os prefixos presentes e tipa as colunas de um prefixo a partir de seus hashes (uma entrada de `mapping.tables` sobrepõe padrão, coluna de chave, tipo de valor e colunas) |

**Redis nos motores hospedados.** O Redis não está disponível quando o motor da organização é um dos motores provisionados da plataforma hospedada. O Trino lê as tabelas de um catálogo Redis de arquivos de descrição num diretório próprio do catálogo, e não é possível dar um a um motor hospedado enquanto ele roda. Salvar ou aplicar ali uma fonte Redis é recusado pelo nome (HTTP 422, `sources.redis_needs_engine_files`); uma implantação auto-hospedada lê o Redis como descrito acima.

### Qdrant

`qdrant` é lido e escrito pelo cliente próprio do Qdrant, `qdrant-client`. O Qdrant é **somente ao vivo**: uma propriedade do tipo de fonte, não uma configuração. Ele responde com seu índice vetorial e sua Query API, e nenhuma réplica pode responder do mesmo modo, então suas tabelas nunca são replicadas. Não há opção Replicate para uma fonte Qdrant nem para suas tabelas; um valor `replicate` ou `row_materialize` em uma delas é recusado.

**Conexão.** `host` é a URL da instância com sua porta (`https://<cluster>.cloud.qdrant.io:6333`, ou um `http://qdrant:6333` auto-hospedado); `password` é sua chave de API, uma referência a um segredo resolvida quando a fonte é lida, vazia para uma instância sem chave. `federation_hints.prefer_grpc: true` lê pela porta gRPC do Qdrant em vez de REST. A fonte tem um esquema, `default`.

**Tabelas de coleção.** Uma coleção é uma tabela. Register Table oferece as colunas da coleção: `id` (o id do ponto, sua chave, caminho `@id`), os campos do payload (um campo indexado tipado por seu índice, qualquer outro como os pontos amostrados sugerem; campos aninhados achatados com seu caminho) e uma coluna `json` por vetor (caminho `@vector`, ou `@vector.<name>` para um vetor nomeado), oferecida sem seleção. Uma coluna de vetor nomeia o modelo de embedding registrado cujos embeddings ela contém, escolhido entre os modelos cujas dimensões são o tamanho do vetor (em Register Table e na aba Tipo e chaves do editor); escolher um modelo registra o vetor.

**Instruções sobre uma fonte Qdrant.** Uma instrução que lê apenas uma coleção é executada no Qdrant quando a API do Qdrant a expressa exatamente:

- as condições sobre o payload viram um filtro do Qdrant (`=`, `<>`, `IN`, intervalos sobre números e datas, `IS NULL`, `AND`, `OR`, `NOT`); a semântica de NULL do SQL é mantida, de modo que `lang <> 'en'` ignora pontos sem `lang`;
- uma condição apenas sobre `id` (`=` ou `IN`) é um `retrieve`; qualquer outra leitura é um `scroll` filtrado;
- `ORDER BY cosine_similarity(<vector>, <query>) DESC LIMIT k` é um `query_points` sobre um vetor cuja distância é Cosine, com o filtro aplicado antes do top-k. Um texto de consulta é convertido em embedding pelo modelo do vetor. A resposta traz um aviso de que o top-k é aproximado, escolhido pelo índice do Qdrant.

Tudo o mais (um agregado, um `GROUP BY`, uma coluna calculada ou mascarada, uma ordenação diferente da mais semelhante primeiro) é respondido pelo motor, como abaixo.

**Instruções que o motor responde.** Quando uma instrução une uma coleção a outra fonte, ou calcula sobre suas colunas, a parte da coleção é lida ao vivo do Qdrant para essa instrução e entregue ao motor como linhas. A parte são as colunas da coleção, as condições da instrução sobre a coleção sozinha (incluindo suas regras de linha) e, quando a coleção é lida sozinha em seu `SELECT`, a ordenação por similaridade e o `LIMIT` desse `SELECT`. Uma parte que o Qdrant não consegue expressar exatamente é recusada, nomeando a tabela e o motivo. Uma parte sem condição, sem chave e sem top-k leria a coleção inteira e também é recusada. Uma condição sobre um parâmetro (`$1`) fica a cargo do motor.

**Gravações.** `INSERT`, `UPDATE` e `DELETE` em uma coleção são executados no Qdrant:

- `INSERT … VALUES` nomeia a coluna `id` e grava pontos novos. Um `id` que a coleção já contém é recusado pelo nome. A verificação lê os ids primeiro, portanto não é atômica com a gravação.
- `INSERT … ON CONFLICT (id) DO UPDATE SET c = EXCLUDED.c, …` é um upsert. As colunas que a instrução não define mantêm seus valores.
- `UPDATE` e `DELETE` atuam sobre os pontos que seu `WHERE` seleciona, como filtro do Qdrant; uma condição sem forma de filtro é recusada.
- Um texto gravado em uma coluna de vetor é convertido em embedding pelo modelo da coluna. Um vetor gravado nela deve ter as dimensões do modelo.

**Tabelas de busca.** A busca vetorial é uma tabela de busca: seu `query_template` contém uma requisição da Query API.

```json
{"collection": "docs", "query": "@query_vector", "using": "text",
 "filter": {"must": [{"key": "lang", "match": {"value": "@lang"}}]},
 "limit": "@k", "score_threshold": 0.5, "model": "text-embedding-3-small"}
```

- `query` é sempre um argumento; `using`, `filter`, `limit`, `score_threshold` e `model` são opcionais. Qualquer valor em `using`, `filter`, `limit` ou `score_threshold` que seja exatamente `"@name"` também é um argumento.
- Cada argumento é um parâmetro da tabela (`query_arg`), convertido estritamente para seu tipo declarado e colocado na requisição como valor JSON, nunca em seu texto. O argumento de `query` é `array(double)` e obrigatório.
- Quem chama passa os argumentos como qualquer parâmetro: `SELECT id, score, title FROM docs_search WHERE _nf_query_vector = '[0.1, 0.2, …]' AND _nf_lang = 'en'` em SQL, argumentos de campo em GraphQL.
- Um argumento opcional não informado não filtra nada: sua condição é omitida do filtro. Sem `limit`, uma busca devolve o padrão do Qdrant, 10 pontos.
- O filtro é aplicado pelo Qdrant antes de escolher seus pontos mais próximos, portanto é exato. Qualquer outra condição da instrução restringe depois os pontos que a busca devolveu; não busca mais.
- `score` é a métrica própria da coleção para o vetor buscado: para uma coleção Cosine, a similaridade de cosseno. Register Table lista os vetores da coleção com tamanho e distância.
- `model` nomeia um modelo de embedding registrado (`vector_models`, admin → AI Models). Um texto dado para a consulta é então incorporado por esse modelo, e um vetor dado deve ter as dimensões desse modelo. Sem `model`, a consulta deve ser um vetor.

Uma busca é lida por instrução sob seus argumentos, suas linhas mantidas sob esses argumentos durante o TTL da tabela. Uma tabela de busca é somente leitura.

**Uma busca alimentada por um join.** Um relacionamento pode alimentar os argumentos de uma busca com as colunas de outra tabela, do mesmo jeito que alimenta os de uma tabela de código: seu mapa `parameters` nomeia cada argumento e a coluna de origem que ele recebe. Uma instrução que junta a busca a essa tabela roda então uma consulta Qdrant para cada conjunto distinto de valores da origem, como o papel da instrução os lê, com os próprios argumentos da instrução para os que o relacionamento não alimenta. Os pontos de uma busca não guardam nada da consulta que os encontrou, então o relacionamento junta pela coluna de parâmetro da consulta: cada ponto carrega, nessa coluna, o valor pelo qual foi buscado.

```yaml
relationships:
- {id: line-searches, cardinality: one-to-many, source_table_id: line, source_column: text,
   target_table_id: lines_like, target_column: _nf_query,
   parameters: {query: text, sonnet: sonnet_number}}
```

```sql
SELECT l.id, s.id AS near_id, s.score
FROM line AS l JOIN lines_like AS s ON s._nf_query = l.text
WHERE s._nf_k = 3
```

As palavras de cada verso são buscadas, com os pontos do seu próprio soneto deixados de fora pelo filtro `"@sonnet"` da busca, e cada verso recebe os três pontos encontrados para ele. Uma instrução que fornece ela mesma um dos argumentos alimentados, como `WHERE s._nf_query = 'the ravages of time'`, lê a busca com os próprios argumentos e não é alimentada.

### Streaming

| Tipo de Fonte | Mecanismo | Mutações |
| ------------ | ----------- | ----------- |
| `kafka` | Conector Kafka federado; esquema via Confluent Schema Registry (Avro, Protobuf, JSON Schema), definição manual, ou inferência de amostra | Não; a saída ao vivo de uma tabela publica no Kafka |
| `websocket` | Feed WebSocket externo — conecta, subscreve, recebe eventos; resultados materializados | Não |
| `rss` | Feed RSS 2.0 / Atom — polling, marca d'água por pubDate/updated; resultados materializados | Não |

**Kafka nos motores hospedados.** Nos motores provisionados da plataforma hospedada, uma fonte Kafka deve nomear um registro de esquemas (`schema_registry_url`). Uma fonte cujas colunas dos tópicos só o modelo declara é recusada pelo nome ao ser salva ou aplicada (HTTP 422, `sources.kafka_needs_schema_registry`), pois suas descrições de tabela exigiriam um diretório que não pode ser dado ao motor em execução. As credenciais SASL chegam a esses motores em memória (veja [Segurança](security.md#the-federation-engine-trino)).

### Receptor Push {: #push-receiver }

| Tipo de Fonte | Mecanismo | Mutações |
| ------------ | ----------- | ----------- |
| `ingest` | Serviços externos fazem POST de eventos JSON; resultados materializados | Não |

### Grafo e Semântico {: #graph-semantic }

| Tipo de Fonte | Mecanismo | Mutações |
| ------------ | ----------- | ----------- |
| `neo4j` | Cypher via API HTTP, resultados em cache no PostgreSQL | Não |
| `sparql` | SPARQL 1.1 POST, resultados em cache no PostgreSQL | Não |

### Baseado em Arquivo {: #file-based }

Três mecanismos cobrem arquivos. As fontes de arquivo único e o rastreador de arquivos usam o campo `path` em vez de `host`/`port`. O terceiro, [Logs](#logs), indica seus arquivos por um glob em seu `mapping` e lê apenas arquivos que estão no servidor Provisa.

**Fontes de banco de dados em arquivo e de arquivo único.** Uma fonte `sqlite`, `duckdb`, `csv` ou `parquet` aponta `path` para um único arquivo. (Uma fonte `duckdb` guarda sua localização no campo `database` do formulário.)

| Tipo de Fonte | Locais | Mutações |
| --- | --- | --- |
| `sqlite` | caminho local, `https://`, `s3://`, `gs://`, `az://` | Sim em um arquivo local; nenhuma em um remoto |
| `duckdb` | caminho local, `https://`, `s3://`, `gs://`, `az://` | Sim em um arquivo local; nenhuma em um remoto |
| `csv` | caminho local, `https://`, `s3://`, `gs://`, `az://` (motor DuckDB) | Não |
| `parquet` | caminho local, `https://`, `s3://`, `gs://`, `az://` (motor DuckDB) | Não |

Um caminho com qualquer outro esquema (`ftp://`, por exemplo) é recusado: "DuckDB reads no file at a `ftp://` location".

**Credenciais para um local remoto.** Coloque-as nos `federation_hints` da fonte. Cada uma vale apenas para o bucket ou contêiner do próprio caminho, então as credenciais de uma fonte nunca são usadas para o local de outra fonte. Uma fonte que não fornece nenhuma é lida como um local público.

| Local | Chaves de `federation_hints` |
| --- | --- |
| `s3://` | `access_key_id`, `secret_access_key`; opcionais `region`, `endpoint` (um armazenamento compatível com S3 como R2 ou MinIO) |
| `gs://` | `gcs_access_id`, `gcs_secret_key` (uma chave HMAC) |
| `az://` | `azure_account_name`, `azure_account_key` |

**Arquivos SQLite e DuckDB remotos são somente leitura.** O motor DuckDB os anexa no lugar com `READ_ONLY`, e uma tabela sobre um deles não oferece insert, update ou delete. Em um motor que não anexa SQLite, um arquivo SQLite remoto é lido por uma conexão DuckDB temporária que o anexa como somente leitura, e suas linhas pousam como de costume.

**Enviando um arquivo.** No formulário de uma fonte `sqlite`, `duckdb`, `csv` ou `parquet`, envie o arquivo em vez de digitar um caminho. O arquivo é gravado em disco por streaming, nunca mantido em memória, na pasta da organização em `files/<source id>/<file name>`, e o caminho da fonte passa a apontar para ele.

- **Substituir.** Enviar de novo toma o lugar do arquivo anterior, qualquer que seja o nome, e a fonte é provisionada de novo para que a próxima leitura veja o arquivo novo.
- **Remover.** Remover a fonte remove o arquivo enviado. Nada mais o remove.
- **Quem.** Somente papéis com a capacidade `source_registration` podem enviar.
- **Limite de tamanho.** A configuração `sources.max_upload_bytes` (ambiente `PROVISA_SOURCES_MAX_UPLOAD_BYTES`) limita o arquivo, 1 GiB (1073741824 bytes) por padrão, mínimo 1, lida quando um envio começa. Um arquivo maior é recusado com HTTP 413 indicando o limite, e nada dele é mantido.
- **Caminho.** Enquanto o caminho for o arquivo enviado da fonte, o campo de caminho do formulário é somente leitura.
- **Nomes.** O id da fonte e o nome do arquivo só podem usar letras, dígitos, `.`, `_` e `-`.
- **Armazenamento.** Um arquivo enviado é armazenado como qualquer arquivo da organização: na pasta da organização, criptografado quando essa pasta é, e visto por todos os nós em um mount compartilhado.

**Baixar um arquivo.** Uma fonte `csv` ou `parquet` pode receber o seu arquivo por download. Escolha **Download a copy** no formulário da fonte e indique um arquivo: um caminho no servidor, um endereço `https://` ou um endereço `s3://` lido com as credenciais que o formulário guarda para o armazenamento. O Provisa transmite o arquivo pelo caminho de upload para `files/<source id>/<file name>` na pasta da organização, com o mesmo limite de tamanho, armazenamento e substituição de um upload, e a fonte lê essa cópia.

- **Quando é baixado de novo.** Quem decide é o TTL de cache da fonte. Uma tabela construída a partir da fonte baixa o arquivo de novo primeiro quando o TTL dela venceu desde que a cópia foi feita. Com um TTL de -1, o arquivo é baixado uma vez e mantido.
- **Caminhos no servidor.** Um caminho do servidor precisa ser um arquivo sob um diretório listado na configuração `sources.download_roots` (ambiente `PROVISA_SOURCES_DOWNLOAD_ROOTS`), depois de seguidos os links simbólicos. Nenhum é listado por padrão. Um caminho fora deles é recusado pelo nome e nada é copiado.
- **Muitas organizações.** Num servidor de muitas organizações (`multitenancy: true`), um caminho do servidor é recusado pelo nome seja o que for que a configuração tenha, e só `https://` e `s3://` são oferecidos. Um endereço cujo host resolve dentro da própria rede do servidor (privada, loopback ou link-local) também é recusado pelo nome, em cada redirecionamento.
- **A tabela dela, como rascunho.** Quando a fonte é criada, o formulário oferece configurar a tabela dela na hora, como rascunho, no domínio que você escolher. A tabela recebe o nome da fonte e é tipada a partir do arquivo como o Register Table a proporia, e fica fora de serviço até que um steward a tire do rascunho. A opção vem ligada por padrão.
- **Pela Polly.** Dê à Polly um caminho totalmente resolvido para um único arquivo CSV ou Parquet e ela propõe a fonte com o download e a tabela em rascunho, e a cria quando você confirma. Um diretório, um glob ou vários arquivos viram em vez disso uma fonte de arquivos.

**Escritas em um arquivo SQLite ou DuckDB local, e bloqueios.** Uma escrita pega o bloqueio do arquivo e nunca espera por ele. Quando outro processo ou nó detém o arquivo, a escrita é recusada, indicando a fonte e que seu arquivo está bloqueado.

- **SQLite.** A escrita define `PRAGMA busy_timeout = 0`, então um arquivo bloqueado falha na hora.
- **DuckDB.** O DuckDB compartilha um arquivo apenas entre aberturas somente leitura. Por isso o anexo do motor e a conexão própria da fonte mantêm o arquivo como somente leitura, e todos os nós o leem ao mesmo tempo. Uma escrita pausa os detentores deste processo, abre o arquivo para leitura e escrita apenas para aquela instrução e o fecha. Se a abertura não conseguir pegar o bloqueio, a escrita é recusada.

**Mounts compartilhados.** Em uma pasta de organização criptografada cujo mount é compartilhado por vários nós (configuração `org_files.shared_mount`), uma escrita em um arquivo SQLite ou DuckDB nela é recusada de imediato, indicando a fonte. A camada de criptografia de pastas (gocryptfs) não leva os bloqueios de arquivo aos outros nós, então nada impediria dois nós de escreverem ao mesmo tempo. Leituras continuam funcionando.

Uma pasta compartilhada sem criptografia deve ser montada com bloqueios que alcancem todos os nós: um mount NFS com bloqueio ativado, não `nolock` e não `local_lock`. Nenhuma verificação confere isso; é um requisito de implantação.

**Rastreador de arquivos (fonte `files`).** Ele aponta `path` para um glob, percorre-o recursivamente e registra o diretório como um catálogo federado de tabelas. Lê muitos formatos por muitos transportes:

| Formatos | Transportes |
| --- | --- |
| CSV, TSV, JSON, YAML, Excel (XLS/XLSX), Parquet, Arrow, e documentos convertidos em tabelas — HTML, Markdown, DOCX, PPTX | Sistema de arquivos local, HTTP(S), `s3://`, `hdfs://`, `ftp://`/`ftps://`, `sftp://`, `iceberg://`, SharePoint (REST e Microsoft Graph) |

```yaml
- id: sales_files
  type: files
  path: s3://bucket/sales/**/*.csv   # glob; local and http(s):// also supported
```

Qualquer fonte `files` também pode declarar um `crawl` em seu mapping, com as mesmas configurações listadas em [Wikipedia](#wikipedia): a fonte então lê páginas web primeiro e pousa cada tabela de dados HTML e cada arquivo de dados vinculado em seu diretório como uma tabela.

No motor DuckDB, `files` é lido de forma nativa, com uma visão de varredura `read_csv_auto` por `<table>.csv` sob o diretório resolvido. Num motor sem conector `files` próprio, as linhas pousam pelo mesmo servidor pgwire do Calcite que vem com o conector (`pgwire-file`) e que sharepoint e splunk usam. Veja [Conectores SaaS Empresariais](#enterprise-saas-connectors) abaixo.

#### Logs {: #logs }

Uma fonte `logs` transforma arquivos de log do servidor Provisa em uma tabela. Você indica os arquivos por um caminho completo ou por um glob, e escolhe o formato em que seus registros são escritos. Cada registro vira uma linha. Um registro que o formato não consegue ler é mantido, com seu texto bruto e o motivo, nunca descartado.

```yaml
sources:
  - id: mac-install-log
    type: logs
    mapping:
      glob: /var/log/install.log*   # the live file and its rotated, compressed copies
      format: macos_install
```

O mapping aceita quatro chaves e recusa qualquer outra pelo nome: `glob` e `format` (ambas obrigatórias), e `pattern` e `record_start`, que só o formato `regex` aceita.

```yaml
    mapping:
      glob: /var/log/myapp/*.log
      format: regex
      pattern: '(?P<ts>\S+ \S+) (?P<level>[A-Z]+) (?P<message>.*)'
      record_start: '\d{4}-\d{2}-\d{2} '   # a stack trace joins the line it follows
```

No formulário de Fontes, uma fonte Logs aparece como "Log files" em File. O botão **Preview** mostra os arquivos que o glob encontra, as colunas que o formato dá e as 20 primeiras linhas do arquivo mais recente, antes de qualquer coisa ser salva.

**O glob.** Deve ser um caminho completo; `~` é expandido e um caminho relativo é recusado ("name the files by their full path"). A correspondência usa o `glob` do Python em modo não recursivo, então `**` se comporta como `*` e corresponde dentro de um único nível de diretório. Só arquivos regulares são tomados, em ordem de nome. Um arquivo vazio é ignorado até ter bytes. O glob pode nomear uma variável de ambiente, como em `${env:LOG_DIR}/*.log`. A fonte guarda a referência como foi escrita e a resolve a cada leitura dos arquivos; uma variável não definida é recusada pelo nome.

**As colunas próprias de cada linha.** Vêm antes das colunas do formato:

| Coluna | Tipo | Conteúdo |
| --- | --- | --- |
| `_file_id` | text | A identidade do arquivo: um checksum dos seus primeiros 4096 bytes, descomprimidos |
| `_file_name` | text | O caminho sob o qual o registro foi lido |
| `_line_offset` | bigint | Deslocamento em bytes da primeira linha do registro no texto descomprimido |
| `_entry` | bigint | Qual entrada do registro a linha é: 0, exceto para um registro em lote como o `Records` de um arquivo CloudTrail |
| `_raw` | text | O registro como foi escrito |
| `_parse_error` | text | Por que o registro, ou um de seus valores, não pôde ser lido; NULL quando foi |
| `_landed_at` | timestamp | Quando a linha pousou |

`_file_id`, `_line_offset` e `_entry` são a chave da linha, então um registro lido duas vezes é uma só linha.

**Formatos e presets.** A seleção de formato os agrupa por família; a última opção, `regex`, é o seu próprio padrão.

| Família (rótulo no formulário) | Nomes de formato |
| --- | --- |
| Formatos comuns | `rfc3164`, `rfc5424`, `macos_install`, `clf`, `combined`, `jsonl`, `logfmt`, `w3c`, `cef`, `gelf` |
| Amazon Web Services | `aws_alb`, `aws_nlb`, `aws_elb`, `aws_cloudfront`, `aws_s3_access`, `aws_cloudtrail`, `aws_vpc_flow`, `aws_waf`, `aws_route53_resolver` |
| Bancos de dados e runtimes | `postgresql_stderr`, `postgresql_csvlog`, `postgresql_jsonlog`, `mysql_error`, `mysql_slow`, `mongodb`, `redis`, `log4j2_default`, `logback_default`, `python_logging` |
| Google | `gcp_cloud_logging`, `android_logcat`, `chrome_net_export` |
| Contêineres e Linux | `kubernetes_cri`, `journald_json`, `linux_auditd`, `linux_dmesg` |
| Microsoft | `ms_iis`, `ms_windows_firewall`, `ms_dns_debug`, `ms_dhcp_audit`, `ms_exchange_tracking`, `ms_sqlserver_errorlog`, `azure_storage_analytics`, `azure_monitor`, `windows_evtx` |
| Monitoramento de segurança de rede | `zeek_tsv`, `zeek_json`, `suricata_eve`, `snort_fast` |
| Servidores web e proxies | `nginx_error`, `apache_error`, `haproxy_http`, `squid_access`, `envoy_access`, `caddy_access`, `traefik_access` |
| Próprio | `regex` |

São 56 formatos embutidos e `regex`.

**Como um formato produz colunas.** Os formatos obtêm suas colunas de uma de quatro maneiras.

- **Colunas fixas.** A maioria dos formatos declara suas colunas e tipos. `rfc3164`, por exemplo, dá `timestamp`, `facility`, `severity`, `host`, `process`, `pid` e `message`; `facility` e `severity` são calculadas a partir do valor `<PRI>`. Uma linha de syslog BSD não traz o ano, então o ano é o da última escrita do arquivo, ou o ano anterior quando o mês da linha é posterior ao da escrita.
- **Campos nomeados pelo registro.** `jsonl` e `logfmt` tiram suas colunas dos campos vistos nas primeiras linhas dos arquivos (configuração `logs.preview_lines`, padrão 1000). Cada coluna recebe o tipo mais estreito em que cabem todos os valores vistos: boolean, depois integer, float, timestamp e json, senão text. Um campo que um registro posterior traz e que nenhuma coluna tem vai para uma coluna json `extra`.
- **Campos nomeados por um cabeçalho.** `w3c` e os presets construídos sobre ele leem a linha `#Fields:` de cada arquivo; `date` e `time` se juntam em uma única coluna `timestamp`. `aws_cloudfront`, `ms_iis`, `ms_windows_firewall`, `ms_exchange_tracking` e `zeek_tsv` leem do mesmo modo as linhas de cabeçalho que começam com `#`, e `aws_vpc_flow` lê uma linha de cabeçalho que começa com `version`. O ponto em que uma linha de cabeçalho foi lida fica com o arquivo, então um arquivo lido mais adiante mantém seus campos.
- **`regex`.** Cada grupo nomeado, como `(?P<level>…)`, é uma coluna do tipo text.

A tela Register Table lista essas colunas com seus tipos e marca a chave. Quando as linhas são lidas, cada valor é convertido para o tipo que sua coluna tem na tabela registrada, então um tipo alterado ali é o aplicado. Um valor que a coluna não comporta vira NULL e seu motivo vai para `_parse_error`.

Um registro maior que `logs.max_line_bytes` (padrão 65536) guarda seus primeiros bytes em `_raw` e não é analisado; `_parse_error` informa seu comprimento. Um arquivo que começa com uma marca de ordem de bytes (BOM) UTF-8 ou UTF-16 é lido nessa codificação, e qualquer outro arquivo como UTF-8.

**Registros que abrangem várias linhas.** Um formato com um padrão de início de registro forma um registro de uma linha que o satisfaz mais todas as linhas seguintes que não o satisfazem. Um stack trace fica com sua exceção, e uma entrada de consulta lenta fica inteira. As linhas antes da primeira correspondência são um registro à parte; em geral o formato o recusa, e a linha é mantida. Os formatos embutidos com início de registro são `rfc3164`, `macos_install`, `aws_cloudtrail`, `postgresql_stderr`, `postgresql_csvlog`, `mysql_error`, `mysql_slow`, `log4j2_default`, `logback_default`, `python_logging`, `chrome_net_export`, `ms_sqlserver_errorlog` e `azure_monitor`. Com `regex`, defina `record_start` você mesmo.

Dois formatos leem seus registros por conta própria em vez de por linha. `kubernetes_cri` junta em um registro as linhas parciais (`P`) que o runtime dividiu, e `windows_evtx` lê o arquivo binário `.evtx`.

Um arquivo ainda em escrita é lido até sua última linha completa. Para um formato de várias linhas, é lido até o início do seu último registro, que a linha seguinte pode continuar. Quando o arquivo passa 2 segundos sem escrita, esse último registro é tomado como inteiro.

**Rotação.** Um arquivo é reconhecido por um checksum dos seus primeiros 4096 bytes, descomprimidos, não pelo nome. Quando `system.log` é rotacionado para `system.log.0` e depois comprimido para `system.log.0.gz`, toda cópia é o mesmo arquivo. A leitura continua do byte que a última leitura alcançou, então nenhuma linha é lida duas vezes.

- A compressão é reconhecida pelos primeiros bytes do arquivo, nunca pelo nome: gzip e bzip2. Um arquivo comprimido é tratado como completo.
- Uma cópia rotacionada e sua gêmea comprimida, ambas encontradas, são lidas uma vez, a partir da comprimida.
- Um arquivo encontrado menor que o deslocamento já lido foi truncado: é um arquivo novo, lido desde o início. As linhas do antigo permanecem.
- Arquivos menores que 4096 bytes que começam igual são distinguidos por inode, então continuam sendo dois arquivos.
- Um arquivo que o glob não encontra mais é esquecido; suas linhas permanecem como histórico.

**Atualização e entrega ao vivo.** Uma tabela Logs é uma réplica. Nada observa os arquivos. Em vez disso, quando o sinal de mudança da fonte é `probe` ou `ttl_probe`, sua sondagem calcula um digest do caminho, inode, tamanho e última escrita de cada arquivo encontrado; quando o digest difere, uma atualização é pedida. Os tipos de sondagem que uma fonte Logs aceita são `hash` e `none`.

Uma atualização lê apenas o que é novo. Cada arquivo é lido de onde a última passagem chegou, e as linhas são escritas pela chave, 5000 por vez, antes de a nova posição ser guardada. Uma passagem que para entre os dois passos relê esses registros, o que deixa as mesmas linhas. Cada passagem carimba suas linhas em `_landed_at`, a coluna que a entrega ao vivo usa como watermark. Uma definição de tabela alterada é uma reconstrução completa que lê cada arquivo desde o início.

**Raízes permitidas.** A configuração de operador `logs.allowed_roots` lista os diretórios em que um glob pode se resolver. O padrão é `["/var/log"]`; é uma lista no cartão Security de Admin → Settings, também definível por `PROVISA_LOGS_ALLOWED_ROOTS`, e uma alteração pede confirmação. Cada raiz é resolvida após a expansão de `~` e dos symlinks.

Aplicam-se duas verificações. A parte fixa do glob, até o primeiro curinga, deve se resolver dentro de uma raiz; isso é verificado quando a fonte é salva e a cada pré-visualização ou leitura. Cada arquivo encontrado também deve se resolver dentro de uma raiz depois de seguidos os symlinks, então um symlink que sai de `/var/log` é recusado. A recusa informa o caminho, o que ele resolveu e as raízes permitidas:

```text
/home/me/app.log resolves to /home/me/app.log, outside the directories log files may be read from: /var/log
```

Ao salvar, o código é `schema.logs_path_refused`; na pré-visualização é um 403 com `logs.path_refused`.

**Recusa em ambiente hospedado (SaaS).** Uma implantação que abriga muitas organizações não lê nenhum arquivo de log próprio. Toda verificação de caminho recusa primeiro, ao salvar, na pré-visualização e na leitura, com "`<path>`: a server that holds many organizations reads no log file of its own". Os arquivos são do servidor, não de uma organização em particular, então nenhum tenant pode lê-los. A implantação sabe se abriga muitas organizações pelo seu estado de multitenancy.

#### Conjuntos de dados Kaggle {: #kaggle-datasets }

O Kaggle é um site para baixar arquivos de dados. Um conjunto de dados do Kaggle que você adiciona é registrado como fonte `files`, e você o consulta como qualquer outra fonte `files`.

**Adicionando um conjunto de dados.** Abra Fontes → Subscriptions → Kaggle. O formulário funciona em dois passos, porque a busca de conjuntos de dados do Kaggle exige um token, então o seletor só aparece depois que o seu é validado.

1. **Token.** Digite um token da API do Kaggle e clique em "Validate". A validação chama `POST https://www.kaggle.com/api/v1/datasets/create/new` com corpo vazio. Um `401` significa que o token é inválido, e qualquer outra resposta significa que é válido. O Kaggle confere o corpo vazio antes de criar qualquer coisa, então nada é salvo.
2. **Seletor.** Um campo de busca consulta `GET /api/v1/datasets/list` enquanto você digita, e cada resultado mostra o título e a descrição do conjunto de dados. Escolha um e clique em "Add Dataset".

"Add Dataset" baixa o pacote de `GET /api/v1/datasets/download/{owner}/{ref}`, descompacta os arquivos CSV e Parquet em `<PROVISA_DATA_DIR>/kaggle/<owner>/<ref>/<file-stem>/<file-name>` e cria **uma** fonte `files` cujo `path` é esse diretório raiz. O servidor nunca guarda o token.

Depois registre as tabelas que quiser na tela normal Register Table. Cada arquivo aparece como uma tabela própria, como em qualquer outra fonte `files`.

**Só CSV e Parquet.** Um pacote com um arquivo `.sqlite` ou `.db` é recusado com um erro que diz isso. Só arquivos CSV e Parquet são preparados.

**Atualizando.** Quando o Kaggle publicar uma nova versão de um conjunto de dados, chame a mutation GraphQL `refreshKaggleSource` com o ID da fonte e um token válido. Ela prepara os arquivos de novo no lugar e limpa o cache do endpoint de arquivos, para que a próxima consulta pegue qualquer mudança de esquema. Os `kaggle_owner` e `kaggle_ref` guardados em `federation_hints` na criação da fonte dizem qual conjunto de dados buscar.

**Sem YAML próprio.** Uma fonte do Kaggle só é criada pelo formulário de fontes. Exportada para YAML, aparece como `type: files` com `kaggle_owner` e `kaggle_ref` em `federation_hints`. Buscar de novo no Kaggle exige a atualização na UI ou a mutation `refreshKaggleSource`. Num ambiente isolado, aponte o `path` do YAML para um diretório que você mesmo preparou.

#### Wikipedia {: #wikipedia }

A Wikipedia percorre as páginas que você indicar. Cada tabela de dados de uma página, e cada arquivo CSV, TSV, Excel, JSON ou Parquet vinculado, vira uma tabela da fonte. Como o Kaggle, ela se registra como fonte `files` com um `crawl` em seu mapping. Adicioná-la não registra nada: um steward registra as tabelas desejadas pela tela normal de Register Table.

**Campos de configuração.**

| Campo | Padrão | Significado |
| --- | --- | --- |
| Pages to start from | nenhum (obrigatório) | Um por linha: o título de uma página, ou o endereço de uma página na edição escolhida |
| Language edition | `en` | O código da edição, por exemplo `en`, `de` ou `ja` |
| Link depth | `1` | A quantos links de distância de uma página inicial ler; `0` lê apenas as páginas iniciais |
| Page limit | `25` | O máximo de páginas que um rastreamento lê |
| Contact | vazio | Enviado com cada requisição para que a Wikipedia possa falar com você, como sua política pede |
| Follow "See also" links | desligado | Desligado deixa essa seção fora do artigo |
| What this edition calls its "See also" section | vazio | Perguntado apenas quando a caixa acima está desligada e o Provisa não sabe nomear a seção na edição escolhida. O título da seção como uma página o mostra, por exemplo `Voir aussi` |
| Directory for the crawl's files | vazio | Onde tabelas e arquivos de dados são gravados. Vazio: `<PROVISA_DATA_DIR>/crawl/<source id>` |

O Provisa conhece o cabeçalho "See also" destas edições: `en`, `de`, `es`, `it`, `pt`, `nl`, `pl`, `sv`, `ja`, `zh`, `ru`. Para qualquer outra edição com "See also" desligado, o formulário pergunta como a edição chama essa seção; digite o título como uma página o mostra (por exemplo `Voir aussi`). Uma fonte salva sem ele é recusada (`wikipedia.see_also_heading_needed`).

**Quais links são seguidos.** Um link é escolhido pelo que a própria marcação da página diz que ele é, nunca por um nome em seu endereço: tais nomes diferem em cada edição de idioma e também aparecem em títulos comuns.

- Seguidos: links para outros artigos que ficam no texto, nas listas e nas tabelas de dados do próprio artigo. A marca de marcação é `a[rel='mw:WikiLink']`.
- Não seguidos: páginas ainda não escritas, a própria página, buscas de livros, páginas de arquivos, outras wikis e sites externos.
- Não lidos de forma alguma (removidos antes de coletar links e tabelas): caixas de navegação, caixas de informação, notas e citações, categorias, o índice, imagens e mensagens de manutenção.
- Não lida a menos que você ative a opção: a seção "See also".

Apenas tabelas que os editores marcam como tabelas de dados (`table.wikitable`) viram tabelas, e uma tabela com menos de 2 linhas não é oferecida.

**Configurações avançadas de rastreamento.** O formulário as mantém recolhidas em "Crawl settings" e mostra cada uma com seu padrão; deixe um campo vazio para mantê-lo. Em um mapping `files` os mesmos nomes vão sob `crawl:`, e um nome fora desta lista é recusado.

| Configuração | Padrão na Wikipedia | Significado |
| --- | --- | --- |
| `start_urls` | as páginas que você indicou | Páginas de onde o rastreamento começa |
| `max_depth` | `1` | Profundidade de links |
| `max_pages` | `25` | Limite de páginas |
| `request_delay` | `1 seconds` | Pausa entre requisições |
| `user_agent` | `Provisa/<version> (+https://provisa.dev) file-crawler`, com o seu contato anexado | O nome com que o rastreamento se apresenta |
| `content_selector` | `#mw-content-text .mw-parser-output` | Seletor CSS: links e tabelas são lidos apenas dentro dele |
| `remove_selectors` | a lista de partes descrita acima | Seletores CSS das partes deixadas de fora. Indicá-lo substitui a lista inteira, inclusive a regra de "See also" |
| `link_selector` | `a[rel='mw:WikiLink']` com exclusões | Seletor CSS que um link deve satisfazer para ser seguido |
| `link_exclude_patterns` | vazio | Expressões regulares; um link cujo endereço contém uma delas não é seguido |
| `follow_external_links` | `false` | Seguir links para outros sites |
| `allowed_domains` | não definido | Domínios que o rastreamento pode visitar |
| `table_selector` | `table.wikitable` | Seletor CSS que uma tabela deve satisfazer |
| `generate_tables_from_html` | não definido | Se tabelas HTML viram tabelas |
| `html_table_min_rows` | `2` | Menor número de linhas em uma tabela |
| `html_table_max_rows` | não definido | Maior número de linhas em uma tabela |
| `allowed_file_extensions` | `csv`, `tsv`, `xlsx`, `xls`, `json`, `parquet` | Tipos de arquivo vinculados que viram tabelas |
| `html_cache_ttl` | `1 days` | Por quanto tempo um rastreamento é mantido |
| `max_html_size` | não definido | A maior página lida, por exemplo `10MB` |
| `max_data_file_size` | não definido | O maior arquivo de dados lido, por exemplo `100MB` |

**Nomes das tabelas.** Uma tabela rastreada se chama `<page>__<name>`. O nome é a legenda (caption) da tabela ou, quando ela não tem, o último título acima dela; uma tabela sem nenhum dos dois vira `table_1`, `table_2` e assim por diante. As duas partes vão para minúsculas, e cada sequência de outros caracteres vira `_`. Por exemplo, uma tabela sob o título "Tallest buildings" na página `List_of_tallest_buildings` vira `list_of_tallest_buildings__tallest_buildings`. Um arquivo de dados vinculado mantém o próprio nome.

**Atualizando.** Um rastreamento é mantido pelo período de cache, `1 days` por padrão (`html_cache_ttl`). As páginas são buscadas de novo apenas depois que esse tempo passou.

### Observabilidade e Outros {: #observability-other }

`prometheus` tem um conector Trino (propriedades construídas a partir da DSL de mapeamento do tipo). `google_sheets` é um tipo de fonte registrado sem conector Trino e materializa através do pipeline de cache de API.

| Tipo de Fonte | Nome do Conector | Mutações |
| ------------ | ----------------- | ----------- |
| `google_sheets` | — (materializada) | Não |
| `prometheus` | prometheus | Não | Uma métrica é uma tabela e uma amostra é uma linha (`timestamp`, `value`, uma coluna por label); em todo motor sem conector ao vivo o Provisa lê a API HTTP — nomes de métrica e labels para Register Table, `query_range` sobre o intervalo da tabela para pouso |

### Conectores SaaS Empresariais {: #enterprise-saas-connectors }

Google Workspace e Microsoft 365 são lidos por carregadores próprios; veja [Google Workspace e Microsoft 365](#google-workspace-and-microsoft-365). SharePoint, Salesforce, ServiceNow, Jira, o inventário de nuvem e o Splunk são lidos por drivers que rodam como processo próprio e falam o protocolo do PostgreSQL (`pgwire-sharepoint`, `pgwire-salesforce`, `pgwire-servicenow`, `pgwire-jira`, `pgwire-cloudops`, `pgwire-splunk`), então nenhum deles tem driver direto. No motor DuckDB o endpoint do driver é anexado ao vivo pela extensão postgres: o Register Table lista as tabelas do conector a partir do catálogo anexado, as consultas leem o conector no lugar, e filtros e projeções são empurrados até ele. Todo outro motor pousa as linhas no armazenamento de materialização para a federação. Os pacotes de drivers são baixados por sistema operacional e arquitetura de uma release fixada (`pgwire-<connector>-<version>-<os>-<arch>.tar.gz`; macOS arm64, Linux x86_64, Windows x86_64). Um operador pode, em vez disso, indicar por conector uma pasta de pacote compilada para este host em Admin → Configurações → Servidores de conectores (`connectors.<connector>.bundle_dir`); veja [Admin](admin.md). Os cinco conectores Trino (o Jira não tem) sempre comparam nomes sem diferenciar maiúsculas e minúsculas, como cada produto faz.

#### `sharepoint`

Listas do SharePoint são enumeradas como esquemas e expostas como tabelas consultáveis. Dois métodos de autenticação: `CLIENT_CREDENTIALS` (padrão) e baseado em certificado via um certificado PFX. Valores secretos em `mapping` são resolvidos através do motor de segredos antes de alcançar o conector.

| Campo da fonte | Propriedade do conector | Notas |
| --- | --- | --- |
| `base_url` ou `host` | `site-url` | URL do site SharePoint |
| `username` | `client-id` | ID do cliente do app Azure |
| `password` | `client-secret` | Segredo do cliente do app Azure |
| `database` | `tenant-id` | UUID do tenant Azure |
| `mapping.auth_type` | `auth-type` | `CLIENT_CREDENTIALS` (padrão) ou `CERTIFICATE` |
| `mapping.certificate_path` | `certificate-path` | Caminho PFX quando `auth_type: CERTIFICATE` — precisa ser ABSOLUTO |
| `mapping.certificate_password` | `certificate-password` | Senha PFX — a chave precisa estar presente, string vazia para um PFX sem senha |

Autenticação por certificado nos motores que não são Trino carrega duas regras extras, ambas aplicadas quando o operando `model.json` do servidor pgwire Calcite é construído. `certificate_path` precisa ser absoluto: o servidor roda com seu diretório de pacote como diretório de trabalho, então um caminho relativo se resolve dentro do cache de runtime-deps e o PFX não é encontrado. `certificate_password` precisa estar presente em `mapping` mesmo quando o PFX não tem senha, caso em que é a string vazia. O adaptador Calcite rejeita uma senha nula por completo, e uma chave ausente é tratada como erro de configuração em vez de lida silenciosamente como uma senha vazia. Um valor ausente ou relativo levanta `MissingConnectorConfig` nomeando o campo.

Quando o conector não expõe `information_schema.columns`, registre a tabela com definições explícitas de coluna (obtidas da API Microsoft Graph) via a mutação `registerTable`.

```yaml
- id: hr-sharepoint
  type: sharepoint
  base_url: https://kenstott.sharepoint.com
  username: ${env:SP_CLIENT_ID}
  password: ${env:SP_CLIENT_SECRET}
  database: ${env:SP_TENANT_ID}
  mapping:
    auth_type: CLIENT_CREDENTIALS
```

Autenticação por certificado, com o caminho absoluto e a senha sempre presente:

```yaml
- id: hr-sharepoint
  type: sharepoint
  base_url: https://kenstott.sharepoint.com
  username: ${env:SP_CLIENT_ID}
  database: ${env:SP_TENANT_ID}
  mapping:
    auth_type: CERTIFICATE
    certificate_path: /etc/provisa/certs/sharepoint.pfx
    certificate_password: ${env:SP_CERT_PASSWORD}
```

#### `salesforce`

Cada sObject que a credencial pode consultar é uma tabela que o steward pode registrar (`Account`, `Opportunity`, objetos personalizados `*__c`); nada é registrado automaticamente. As colunas e os tipos vêm do describe do sObject. Filtros, projeções, ordenações e limites são delegados ao Salesforce como SOQL.

Uma fonte Salesforce informa a URL de login Meu domínio da organização e um conjunto completo de credenciais, escolhido por `mapping.auth_type`. A URL de login é a URL `https://…` completa (por exemplo `https://acme.my.salesforce.com`); um host sem esquema ou qualquer outro esquema é recusado, indicando o campo. Um conjunto incompleto é recusado quando a fonte é usada, indicando o valor que falta. Cada segredo pode ser uma referência `${secret:…}` ou `${env:…}`. Um segredo digitado como valor literal é guardado no cofre da organização e a fonte mantém a referência.

| Campo da fonte | Propriedade do conector | Notas |
| --- | --- | --- |
| `base_url` ou `host` | `login-url` | URL Meu domínio da organização (Configuração, depois Meu domínio). `login.salesforce.com` não funciona com o fluxo de credenciais do cliente |
| `username` | `client-id` | Chave do consumidor do aplicativo conectado |
| `password` | `client-secret` | Segredo do consumidor do aplicativo conectado |
| `mapping.auth_type` | — | `CLIENT_CREDENTIALS` (padrão), `USERNAME_PASSWORD` ou `ACCESS_TOKEN` |
| `mapping.sf_username`, `mapping.sf_password` | `username`, `password` | Somente `USERNAME_PASSWORD`, junto com a chave e o segredo do consumidor |
| `mapping.security_token` | `security-token` | Somente `USERNAME_PASSWORD`; opcional |
| `mapping.access_token`, `mapping.instance_url` | `access-token`, `instance-url` | Somente `ACCESS_TOKEN`; sem chave nem segredo do consumidor |
| `mapping.api_version` | `api-version` | Opcional, por exemplo `v61.0` |

Para `CLIENT_CREDENTIALS`, o aplicativo conectado precisa de *Enable Client Credentials Flow* e de um usuário *Run As*; toda leitura e escrita é executada com as permissões desse usuário. Organizações criadas desde o Summer '23 bloqueiam por padrão o fluxo de nome de usuário e senha.

**Leituras.** No Trino a fonte é lida pelo catálogo `salesforce`, que apenas lê. Em qualquer outro mecanismo, o mecanismo anexa o servidor pgwire da fonte e a lê no local.

**Escritas.** `INSERT`, `UPDATE` e `DELETE` em um sObject registrado são mutações: governadas, auditadas e tratadas por ambiente como qualquer outra mutação. Em todos os mecanismos, inclusive o Trino, uma escrita é executada no servidor pgwire da fonte; no Trino esse servidor roda apenas para as escritas. Cada instrução é enviada ao Salesforce e confirmada quando executa, então não pode ser desfeita. O servidor não devolve as linhas que escreve: uma instrução com cláusula `RETURNING` é recusada, e o GraphQL não oferece campos de mutação para essas tabelas; `INSERT`, `UPDATE`, `DELETE` e `COPY` em SQL são os caminhos de escrita. Uma escrita é uma instrução simples em uma tabela: `INSERT … ON CONFLICT`, `UPDATE … FROM`, `DELETE … USING`, `MERGE` e `TRUNCATE` são recusados, indicando a forma. Uma fonte SharePoint é escrita da mesma maneira.

**Inicialização.** Antes de aceitar conexões, o servidor da fonte lê as colunas de cada sObject, cerca de quatro minutos para uma organização com 1.200 deles. Isso é pago uma vez: os resultados de describe ficam no diretório de estado da própria fonte, sob o diretório de dados da instância. O servidor é iniciado quando a fonte é registrada ou carregada. No Trino o catálogo `salesforce` guarda seus próprios resultados de describe, um diretório por fonte em `/data/trino/cache/salesforce-describe` no coordinator, então o custo é pago uma vez ali também quando esse caminho é um volume que sobrevive ao contêiner: o volume `trino_cache` da stack compose, ou o volume de cache do coordinator do chart Helm (`trino.coordinator.cache.persistence`). Até que ele aceite conexões, Register Table mostra o conector como iniciando e uma escrita recebe a mesma mensagem em vez de esperar.

```yaml
- id: sales-salesforce
  type: salesforce
  base_url: https://acme.my.salesforce.com
  username: ${env:SF_CONSUMER_KEY}
  password: ${env:SF_CONSUMER_SECRET}
  mapping:
    api_version: v61.0
```

Fluxo de nome de usuário e senha:

```yaml
- id: sales-salesforce
  type: salesforce
  base_url: https://acme.my.salesforce.com
  username: ${env:SF_CONSUMER_KEY}
  password: ${env:SF_CONSUMER_SECRET}
  mapping:
    auth_type: USERNAME_PASSWORD
    sf_username: ops@acme.com
    sf_password: ${env:SF_PASSWORD}
    security_token: ${env:SF_SECURITY_TOKEN}
```

#### `servicenow`

Toda tabela da instância está disponível para registro, e nada é registrado automaticamente. Uma instância movimentada tem milhares de tabelas, então o Register Table as agrupa por tema. Cada tema é o nome de exibição de um pacote do ServiceNow, ou "(no package)" para uma tabela que não pertence a nenhum. Cada tabela aparece como `name — label`. Selecionar uma tabela lê as colunas dela na instância; listar as tabelas não.

Uma fonte ServiceNow nomeia a URL `https://…` completa da instância junto com um nome de usuário e senha. A URL é retirada de `base_url` ou `host`; um host simples ou qualquer outro esquema é recusado, nomeando o campo (`instanceUrl`). Um nome de usuário ou senha ausente é recusado, nomeando o campo. Cada valor pode ser uma referência `${secret:…}` ou `${env:…}`.

| Campo da fonte | Propriedade do adaptador | Notas |
| --- | --- | --- |
| `base_url` ou `host` | `instanceUrl` | URL `https://…` completa da instância (por exemplo, `https://acme.service-now.com`). Um host simples ou outro esquema é recusado |
| `username` | `username` | Nome de usuário de autenticação básica |
| `password` | `password` | Senha de autenticação básica |

**Leituras.** No Trino a fonte é lida pelo catálogo `servicenow`, que apenas lê. Em qualquer outro mecanismo, o mecanismo anexa o servidor pgwire da fonte e a lê no local. Somente as colunas selecionadas são solicitadas à instância (`sysparm_fields`). Um filtro é enviado à instância (`sysparm_query`) apenas em uma forma que o adaptador verificou em uma instância real; todos os outros filtros, e cada ordenação e limite, são aplicados às linhas lidas.

Um campo de referência aparece como duas colunas: `<field>` contém o sys_id do registro referenciado e `<field>__display` contém o valor de exibição. Um campo vazio é NULL. Um campo cujo tipo o adaptador não consegue mapear gera um erro nomeando a tabela, o campo e o tipo.

A pesquisa de tabelas (linguagem natural) corresponde ao nome, rótulo e tema de cada tabela. Uma vez que uma tabela é registrada, suas colunas entram na pesquisa.

**Ciclo de vida do servidor.** O servidor serve apenas as tabelas registradas nele. Registrar ou remover uma tabela reinicia o servidor e reprovision a fonte no mecanismo; uma instrução que chega durante esse reinício falha.

**Escritas.** `INSERT`, `UPDATE` e `DELETE` em uma tabela registrada são mutações: governadas, auditadas e tratadas por ambiente como qualquer outra mutação. Em todos os mecanismos, inclusive o Trino, uma escrita é executada no servidor pgwire da fonte. O servidor retorna a linha que a instância contém após cada escrita; uma cláusula `RETURNING` é portanto aceita e o GraphQL oferece campos de mutação para essas tabelas. Uma escrita é uma única instrução em uma tabela: `INSERT … ON CONFLICT`, `UPDATE … FROM`, `DELETE … USING`, `MERGE` e `TRUNCATE` são recusados, nomeando a forma.

**Import sets.** Quando uma tabela tem um transform map ativo para ela, Register Table oferece seus import sets, e o steward pode vincular a tabela a um deles (uma tabela com vários maps nomeia cada um). As inserções em uma tabela vinculada passam pela Import Set API da instância e pela transformação daquele map; as inserções em uma tabela não vinculada, e cada atualização e exclusão, são gravadas diretamente na tabela. Uma linha que a transformação rejeita é recusada, nomeando a linha; linhas já gravadas permanecem gravadas. A vinculação é verificada quando a tabela é salva: deve ser o import set de um transform map ativo para a tabela, em uma fonte ServiceNow.

```yaml
- id: helpdesk-sn
  type: servicenow
  base_url: https://acme.service-now.com
  username: ${env:SN_USERNAME}
  password: ${env:SN_PASSWORD}
```

#### `jira`

Um site Jira Cloud é lido e escrito por um adaptador Calcite próprio. Uma tabela de uma fonte Jira é um **formulário**: um projeto e um tipo de issue, mais os campos que suas colunas leem. Register Table lista cada formulário do qual a conta pode criar uma issue, agrupado por projeto; selecionar um lê os campos desse formulário do Jira, e listar os formulários não faz isso. Todo formulário tem as colunas `key` (sua chave primária), `id`, `summary`, `status`, `created` e `updated`, depois uma coluna por campo escolhido, nomeada como o adaptador a nomeia: o nome do campo em snake case (`Story Points` vira `story_points`). Um campo cujo nome já é o de uma coluna é nomeado pelo seu id de campo (`customfield_10099`), de modo que todo campo possa ser escolhido. Um campo de usuário contém o id da conta; seu nome de exibição é um campo à parte (`assignee.displayName`, coluna `assignee_name`). As colunas que o próprio Jira escreve são somente leitura: `key`, `id`, `created`, `updated`, o nome de exibição de um campo de usuário e um campo de uma forma que o adaptador não conhece, que ele serve como texto JSON. Registrá-la a marca com «Read only: Jira sets it» na sua descrição, quando ela não tem nenhuma, e uma gravação que a define é recusada pelo servidor, nomeando-a.

A tabela de um formulário nomeia seu projeto e seu tipo de issue em seus `arguments` (`project`, `issue_type`), e cada coluna lê o campo que seu `path` nomeia (um id de campo ou o nome de um campo), ou o campo de seu próprio nome. Uma tabela registrada pelo admin obtém ambos do Jira; uma configuração os declara. Uma tabela Jira que fixe qualquer outra coisa, ou que não tenha um dos dois, é recusada, nomeando-a.

| Campo da fonte | Propriedade do adaptador | Notas |
| --- | --- | --- |
| `base_url` ou `host` | `baseUrl` | Endereço `https://…` completo do site (por exemplo, `https://acme.atlassian.net`). Um host simples ou outro esquema é recusado |
| `username` | `email` | E-mail da conta Atlassian |
| `password` | `apiToken` | Um token de API dessa conta, criado em id.atlassian.com em Security, API tokens |

**Vínculos.** A tabela `issue_links` da fonte contém os vínculos entre as issues de seus formulários: `link_id`, `link_type`, `outward_key` e `inward_key`, cada vínculo uma vez. Uma relação entre dois formulários é declarada através dela como junction, com `link_type` como discriminador, de modo que GraphQL e Cypher percorram de uma issue à issue vinculada pelo tipo de vínculo.

**Leituras.** Uma consulta por chave (`key =`, `key IN`) é buscada por chave, de modo que uma chave que não nomeia nenhuma issue não é uma linha. Uma comparação de data ou número é enviada ao Jira como JQL. Todo outro filtro, e toda ordenação, é aplicado às linhas lidas: uma igualdade sobre texto, opção, status ou usuário não é enviada, pois o JQL recusa um valor que não nomeia nada onde o SQL retorna zero linhas. Um campo de múltiplos valores (labels, seleção múltipla, componentes, versões) é texto contendo um array JSON, e é escrito da mesma forma.

No DuckDB, PostgreSQL e ClickHouse o mecanismo anexa o servidor pgwire da fonte e a lê no local. No Trino a fonte não tem catálogo próprio e é carregada pelo seu servidor.

**Ciclo de vida do servidor.** O servidor serve apenas os formulários registrados nele, ao lado de sua lista de formulários, sua lista de campos e os vínculos. Registrar, alterar ou remover um formulário reinicia o servidor e reprovisiona a fonte no mecanismo.

**Escritas.** `INSERT`, `UPDATE` e `DELETE` em um formulário são mutações, gravadas no servidor pgwire da fonte em todos os mecanismos. As linhas são inseridas em lotes de 50; uma atualização ou exclusão é feita issue por issue, por chave. Definir `status` executa a transição de workflow para esse status, e é recusado, nomeando os status alcançáveis, quando o workflow da issue não tem nenhum. Uma issue com subtarefas não é excluída. O servidor retorna cada linha gravada por sua chave, portanto `RETURNING` é aceito e o GraphQL oferece campos de mutação. Em `issue_links`, `INSERT` cria um vínculo e `DELETE` remove um; `UPDATE` é recusado.

```yaml
- id: support-jira
  type: jira
  base_url: https://acme.atlassian.net
  username: ${env:JIRA_EMAIL}
  password: ${env:JIRA_API_TOKEN}
tables:
- source_id: support-jira
  table: incident
  arguments: {project: KAN, issue_type: Incident}
  columns:
  - {name: key, data_type: varchar, is_primary_key: true}
  - {name: summary, data_type: varchar}
  - {name: status, data_type: varchar}
  - {name: priority, data_type: varchar}
  - {name: flagged, data_type: varchar, path: customfield_10021}
```

#### `cloudops`

O inventário de nuvem apresenta os recursos do Azure, da AWS e do GCP como um único conjunto de tabelas: `compute_resources`, `storage_resources`, `kubernetes_clusters`, `database_resources`, `network_resources`, `iam_resources`, `container_registries` e `compute_security_groups`, que associa um recurso de computação aos seus grupos de segurança. Cada tabela tem as mesmas colunas nas três nuvens, e sua coluna `cloud_provider` indica de qual nuvem veio cada linha. A fonte apenas lê, e suas tabelas são registradas por um steward como quaisquer outras.

Uma fonte informa uma ou mais nuvens. Cada nuvem é tudo ou nada: uma nuvem com apenas parte dos valores obrigatórios é recusada, indicando o que falta, assim como uma fonte que não informa nenhuma nuvem. A fonte lê exatamente as nuvens que informa. Todos os valores ficam em `mapping`; cada segredo pode ser uma referência `${secret:…}` ou `${env:…}`, e um segredo digitado como valor literal é guardado no cofre da organização, e a fonte mantém a referência.

| Nuvem | Chave de `mapping` | Propriedade do conector | Notas |
| --- | --- | --- | --- |
| Azure | `azure_tenant_id` | `azure.tenant-id` | Obrigatório |
| Azure | `azure_client_id` | `azure.client-id` | Obrigatório; o ID do cliente do registro do aplicativo |
| Azure | `azure_client_secret` | `azure.client-secret` | Obrigatório; segredo |
| Azure | `azure_subscription_ids` | `azure.subscription-ids` | Obrigatório; separados por vírgulas |
| AWS | `aws_access_key_id` | `aws.access-key-id` | Obrigatório |
| AWS | `aws_secret_access_key` | `aws.secret-access-key` | Obrigatório; segredo |
| AWS | `aws_region` | `aws.region` | Obrigatório, por exemplo `us-east-1` |
| AWS | `aws_account_ids` | `aws.account-ids` | Obrigatório; separados por vírgulas |
| AWS | `aws_role_arn` | `aws.role-arn` | Opcional; um papel a assumir para acesso entre contas |
| GCP | `gcp_credentials_path` | `gcp.credentials-path` | Obrigatório; caminho ABSOLUTO de um arquivo de chave JSON de conta de serviço |
| GCP | `gcp_project_ids` | `gcp.project-ids` | Obrigatório; separados por vírgulas |
| — | `cache_ttl_minutes` | `cache.ttl-minutes` | Opcional; por quanto tempo um resultado é reutilizado antes de as nuvens serem consultadas de novo (padrão do adaptador 5) |

O arquivo de chave do GCP é aberto por quem lê a fonte. No Trino é o servidor Trino, então o caminho precisa existir lá. Em qualquer outro mecanismo é o servidor pgwire da fonte no host do Provisa, cujo diretório de trabalho é o seu próprio diretório de estado, então um caminho relativo é recusado.

No Trino a fonte é lida pelo catálogo `cloudops`. Em qualquer outro mecanismo, o mecanismo anexa o servidor pgwire da fonte e a lê no local, como uma fonte SharePoint.

```yaml
- id: cloud-estate
  type: cloudops
  mapping:
    aws_access_key_id: ${env:CLOUDOPS_AWS_ACCESS_KEY_ID}
    aws_secret_access_key: ${env:CLOUDOPS_AWS_SECRET_ACCESS_KEY}
    aws_region: us-east-1
    aws_account_ids: "111111111111,222222222222"
    azure_tenant_id: ${env:CLOUDOPS_AZURE_TENANT_ID}
    azure_client_id: ${env:CLOUDOPS_AZURE_CLIENT_ID}
    azure_client_secret: ${env:CLOUDOPS_AZURE_CLIENT_SECRET}
    azure_subscription_ids: "sub-1,sub-2"
```

#### `splunk`

Resultados de busca do Splunk são consultáveis como tabelas (ex.: `internal_server`). A URL do conector vem de `base_url`, ou é construída como `https://{host}:{port}` com uma porta padrão de `8089`. Autenticação: quando `mapping.use_token` é `true` (o padrão), `password` é passada como o token de API; quando `false`, `username` e `password` são passadas como credenciais separadas.

| Campo da fonte | Propriedade do conector | Notas |
| --- | --- | --- |
| `base_url` / `host` + `port` | `url` | `base_url`, senão `https://host:port` (porta padrão 8089) |
| `password` | `token` ou `password` | token quando `use_token: true` |
| `username` | `user` | apenas quando `use_token: false` |
| `database` | `app` | restringe a um app Splunk |
| `mapping.datamodel_filter` | `datamodel-filter` | filtra para um data model |
| `mapping.disable_ssl_validation` | `disable-ssl-validation` | para certificados autoassinados |

No caminho de réplica pgwire (todo motor exceto o Trino) as mesmas quatro configurações opcionais viram as chaves de operando do Calcite `model.json` `app`, `token`/`username`+`password`, `datamodelFilter` e `disableSslValidation`. As duas últimas assumem os tipos para os quais o `SplunkSchemaFactory` as converte, uma string e um booleano.

```yaml
- id: ops-splunk
  type: splunk
  host: splunk
  port: 8089
  password: ${env:SPLUNK_TOKEN}
  mapping:
    use_token: true
    disable_ssl_validation: true
```

#### Google Workspace e Microsoft 365 {: #google-workspace-and-microsoft-365 }

`google_workspace` e `microsoft_365` leem o e-mail, os calendários e as tarefas das contas que uma fonte indica, e os pousam como réplicas. Não são conectores Calcite: cada um tem um loader próprio que chama a API do provedor, Gmail, Google Calendar e Google Tasks de um lado e Microsoft Graph do outro. O formulário os lista em Enterprise como "Google Workspace (Gmail)" e "Microsoft 365 (Outlook, Exchange)".

**Tabelas.** Os dois tipos produzem as mesmas tabelas canônicas: mesmos nomes, tipos de coluna e ordem. Uma união dos dois não precisa de cast nem de renomeação; uma coluna para a qual um provedor não tem valor é NULL. As tabelas são listadas no esquema `default`, e toda linha começa com a conta de onde foi lida. Uma fonte oferece as tabelas dos recursos que lê:

| Recurso | Tabelas | Escritas por instruções |
| --- | --- | --- |
| `mail` | `messages`, `message_recipients`, `folders`, `message_folders`, `threads`, `attachments` | Não |
| `calendar` | `calendars`, `events`, `event_attendees` | `events`, `event_attendees` |
| `tasks` | `task_lists`, `tasks` | `tasks` |

As colunas estão listadas no [esquema canônico de e-mail](arch/canonical-mail-schema.md). `events` guarda eventos únicos, eventos mestre de séries com a recorrência como linhas RFC 5545, e exceções; nunca guarda as ocorrências expandidas de uma série.

**Registro.** O administrador de uma organização primeiro informa o cliente da organização para a plataforma em Admin → Email: um client ID e um secret, mais o ID do diretório (tenant) ou o domínio para o Microsoft 365. Quem adiciona uma fonte não o vê nem o digita. Uma fonte assume então uma de duas formas:

- **Uma caixa de correio.** A fonte indica um endereço em `accounts`. Seu dono clica em **Connect** no formulário e aprova o cliente da organização. A aprovação resultante é guardada no armazenamento de estado da organização, não nas configurações da fonte. No Google Workspace, essa forma também declara `sign_in: google_account`.
- **As caixas de correio da organização.** A fonte declara `mailboxes` como exatamente um entre `everyone: true`, `group: <address or id>` ou `list: [addresses]`. Não guarda credencial e ninguém a conecta. A organização lê com acesso próprio, e só depois que o administrador ativou "Read the organization's mailboxes". No Google, esse acesso é uma chave de service account com delegação em todo o domínio, mais um endereço de administrador para chamadas ao diretório. No Microsoft, é o grant client-credentials do registro do aplicativo.

Na segunda forma, uma tabela guarda as linhas de todas as caixas. Um papel que pode ler a tabela lê todas as caixas, a menos que uma regra de linha na coluna `account` o limite.

```yaml
sources:
  - id: team-mail
    type: microsoft_365
    mapping:
      resources: [mail, calendar, tasks]
      mailboxes:
        group: support@acme.com
      mail_since: "2026-01-01"
      calendar_check_days: 30        # required when calendar is read
      calendar_send_updates: false
```

**Configurações.** Cada módulo recusa pelo nome uma chave que não lê.

| Chave de `mapping` | Tipo | Significado |
| --- | --- | --- |
| `resources` | ambos | Quais entre `mail`, `calendar`, `tasks` a fonte lê; pelo menos um |
| `accounts` | ambos | O endereço da única caixa (forma de uma caixa) |
| `mailboxes` | ambos | `everyone`, `group` ou `list` (forma de organização) |
| `sign_in` | `google_workspace` | `google_account` (forma de uma caixa) |
| `mail_since` | ambos | `YYYY-MM-DD`; nada recebido antes é guardado |
| `mail_content` | `google_workspace` | `full`, ou `headers` para somente cabeçalhos e rótulos (escopo `gmail.metadata` do Gmail). Obrigatório quando o e-mail é lido |
| `mail_search` | `google_workspace` | Uma busca como digitada na caixa de busca do Gmail |
| `mail_labels` | `google_workspace` | Nomes de rótulos; só o e-mail que traz todos é lido |
| `mail_include_spam_trash` | `google_workspace` | `false` por padrão |
| `calendar_send_updates` | ambos | Se uma escrita avisa os participantes de um evento; desativado por padrão |
| `calendar_check_days` | `microsoft_365` | Dias antes e depois de hoje que a verificação de mudanças examina; um número inteiro, 1 ou mais, obrigatório quando o calendário é lido |

O Google recusa uma busca sob `gmail.metadata`, então `mail_content: headers` com `mail_search` ou `mail_since` é recusado.

**Escopos.** Um login pede apenas os escopos dos recursos marcados:

| Recurso | Google Workspace | Microsoft 365 (delegated) |
| --- | --- | --- |
| `mail` | `gmail.readonly`, ou `gmail.metadata` somente para cabeçalhos | `Mail.Read` |
| `calendar` | `calendar.calendarlist.readonly`, `calendar.events` | `Calendars.ReadWrite` |
| `tasks` | `tasks` | `Tasks.ReadWrite` |
| toda fonte | `userinfo.email` | `offline_access`, `User.Read` |

Para a forma de organização, o Microsoft exige as permissões de aplicativo `Mail.Read` e `User.Read.All`, mais `Calendars.ReadWrite` e `Tasks.ReadWrite.All` para calendários e tarefas. A service account do Google é autorizada para os escopos em `DELEGATED_SCOPES`, que também incluem a leitura de usuários e membros de grupos do diretório.

**Atualização e frescor.** Os tipos de sondagem que essas fontes aceitam são `hash` e `none`, e o loader delas fornece o token de mudança:

- **Gmail:** um digest do history id atual de cada caixa, uma chamada `users.getProfile` por caixa.
- **E-mail do Microsoft:** cada delta link guardado, seguido por um item.
- **Calendários e tarefas do Google:** a tag e a hora da última mudança de cada calendário; a tag de cada lista de tarefas, e o id e a hora de atualização de cada tarefa.
- **Calendários e tarefas do Microsoft:** as linhas de mudança de cada caixa, dentro de `calendar_check_days` antes e depois de hoje.

Uma verificação que não pode ser feita pede uma leitura; nunca informa "sem mudança". As tabelas de calendário e tarefas são lidas inteiras a cada atualização. As réplicas de e-mail de uma fonte Microsoft 365 ou Google Workspace são mantidas no lugar: cada parte de uma caixa é escrita conforme chega e pode ser consultada na hora. Uma fonte Google Workspace que restringe por `mail_search` é relida inteira a cada atualização, porque só o Google pode dizer o que uma busca encontra.

**Escritas.** `INSERT`, `UPDATE` e `DELETE` em `events`, `event_attendees` e `tasks` vão ao provedor por um único caminho de escrita para os dois tipos. Um `UPDATE` ou `DELETE` indica suas linhas por `provider`, `account` e `id`; um participante é indicado por `event_id` e `address`. Uma ocorrência de uma série é indicada por `series_id` e `original_start_at`. Uma escrita que define uma coluna fora do conjunto gravável da tabela é recusada pelo nome. O Google escreve com `sendUpdates=all` quando `calendar_send_updates` está ativo e com `sendUpdates=none` quando está desativado. O Microsoft Graph não consegue fazer uma mudança dessas em silêncio, então uma mudança nos participantes de uma reunião é recusada, indicando a configuração.

**Exclusões.** O e-mail é somente leitura; nenhuma instrução altera uma caixa. `calendars`, `task_lists` e toda tabela de e-mail não aceitam escritas. Spam e Lixeira do Gmail ficam de fora a menos que `mail_include_spam_trash` esteja ativo. A API Tasks do Google guarda apenas o dia em que uma tarefa vence, e não tem lembrete.

### Fontes de API {: #api-sources }

Registre qualquer endpoint HTTP como uma tabela consultável.

| Tipo de API | Descoberta | Inferência de Coluna |
| --------- | ----------- | ----------------- |
| `openapi` | Parse de especificação OpenAPI | Primitivos → nativo, objetos → JSONB |
| `graphql_remote` | Introspecção de esquema | Primitivos → nativo, objetos → JSONB |
| `grpc_remote` | Reflexão de servidor | Primitivos → nativo, objetos → JSONB |

Respostas de API são buscadas, armazenadas em cache no PostgreSQL (TTL configurável), e expostas como tipos GraphQL. Tabelas em cache participam de consultas federadas como qualquer outra fonte.

**Regras JSONB**: Colunas complexas (objetos, arrays) armazenadas como JSONB não são filtráveis. O acesso a subcampo usa extração `->>` em SQL. Relacionamentos são declarados entre tabelas usando colunas FK escalares. Colunas de blob JSONB não são alvos de join. Use a promoção de JSONB para converter campos aninhados em colunas escalares nativas quando filtrar ou unir sobre eles for necessário.

#### GitHub e GitLab {: #github-and-gitlab }

GitHub e GitLab são fontes de marca conduzidas por `graphql_remote`: cada uma aparece no formulário de Fontes com o próprio nome e se registra como uma fonte GraphQL remota que guarda sua marca. O formulário pede um personal access token e nada mais. O endpoint (`https://api.github.com/graphql`, `https://gitlab.com/api/graphql`) e o esquema acompanham o Provisa, então o esquema não passa por introspecção no registro. O token é verificado quando a fonte é adicionada. Adicionar a fonte não registra tabelas: toda tabela que o esquema oferece fica disponível sob o namespace da fonte (`gh`, `gl`), e o steward registra as desejadas. Um campo que o token não pode ler fica de fora de uma tabela quando ela é registrada.

#### Stripe {: #stripe }

O Stripe é uma fonte com marca carregada por `openapi`, construída como GitHub e GitLab são construídos sobre `graphql_remote`. Aparece no formulário de Fontes como Stripe, em Enterprise. Registra-se como uma fonte OpenAPI cuja linha grava a marca em `federation_hints` e cujo caminho de especificação é `brand:stripe`.

**O que alcança.** A API REST do Stripe, pela especificação OpenAPI que ele publica (`stripe/openapi`, `spec3.json`). A especificação acompanha o Provisa, fixada em uma versão e comprimida com gzip, e não é baixada no registro. A que acompanha é a versão de API `2026-09-30.endive` com 425 caminhos. Toda chamada leva `Stripe-Version` definido para essa versão, então cada resposta tem a forma que a especificação descreve, seja qual for a versão padrão da conta. Os campos que o Stripe devolve mas a especificação omite são acrescentados de `stripe.additions.json`.

Como em qualquer fonte OpenAPI, cada operação `GET` é uma tabela oferecida e cada outra operação é um comando oferecido. Adicionar a fonte não registra nenhuma: o steward registra as desejadas.

**Autenticação.** O formulário pede uma só coisa, um token de acesso (`BRAND_AUTH`: `token`). Use uma chave de API secreta ou restrita do Stripe. O token é enviado como bearer token e verificado uma vez com `GET /v1/balance` antes de a fonte ser adicionada. Um token rejeitado faz o registro falhar com `openapi.credential_rejected`; a ausência dele o faz falhar com `openapi.credential_required`.

**Atualização e frescor.** Uma tabela Stripe é uma réplica, como toda tabela OpenAPI. Suas respostas em cache duram o `cache_ttl` da fonte, que uma requisição de registro define como 300 segundos, a menos que diga outra coisa. Aplicam-se os tipos de sondagem do OpenAPI. Um preset pode indicar o parâmetro que uma atualização usa para buscar só o que mudou. A marca Stripe não indica nenhum, então cada atualização lê suas tabelas inteiras.

**Exclusões.** A integração de pagamento descontinuada do Stripe baseada em token e Sources fica de fora da especificação que acompanha o produto. A lista está em [Exclusões em uma fonte construída a partir de uma especificação](#exclusions-on-a-specification-built-source). As `exclusions` do próprio operador são somadas às da marca.

`stripe` também aparece em `SourceType` entre os tipos stub do Supabase `wrappers`. Uma fonte Stripe adicionada pelo formulário é uma fonte `openapi`, não esse stub.

#### Exclusões em uma fonte construída a partir de uma especificação {: #exclusions-on-a-specification-built-source }

Uma especificação OpenAPI publicada descreve tudo o que a API remota aceita, inclusive o que seu fornecedor descontinuou e o que uma implantação nunca deve enviar. Uma fonte OpenAPI pode ter exclusões: coisas da especificação que a fonte não oferece.

| Excluído | Indicado por | Efeito |
|---|---|---|
| Uma operação | `method` e `path` | Não listada para registro como tabela ou como comando |
| Um argumento de requisição | `name`, em uma operação (`method` e `path`) ou, sem eles, em toda operação que o tenha | Fora dos argumentos da operação, não exibido, nunca enviado |

As exclusões são definidas sob `exclusions` no `mapping` da fonte:

```yaml
sources:
  - id: shop-api
    type: openapi
    path: https://shop.example.com/openapi.json
    mapping:
      exclusions:
        operations:
          - {method: POST, path: /v1/tokens}
        arguments:
          - {name: source, method: POST, path: /v1/customers}
          - {name: legacy_token}          # on every operation that has it
```

Elas são aplicadas à especificação quando ela é carregada, de modo que a lista de registro, os comandos gerados e a requisição enviada veem todos uma especificação em que as coisas excluídas não existem. Uma propriedade de body e um parâmetro de query, header ou cookie podem ser excluídos; um parâmetro de caminho (path) não pode, porque faz parte do endereço da operação.

Uma exclusão que nomeia uma operação ou um argumento que a especificação não tem faz o carregamento da especificação dessa fonte falhar e nomeia cada entrada obsoleta, de modo que uma lista que já não corresponde a uma especificação mais nova é notada.

Uma fonte de marca traz suas próprias exclusões como parte de sua curadoria, e as do operador são somadas a elas. A fonte Stripe exclui a integração de pagamento descontinuada da Stripe baseada em token e source: criar uma cobrança (charge), as APIs Tokens e Sources, adicionar um cartão ou conta bancária a um cliente a partir de um token, e os argumentos `source`, `card`, `bank_account` e `default_card` das escritas de cliente e de pagamento de fatura. Os pagamentos são feitos por Payment Intents, Payment Methods e Setup Intents, que a fonte oferece. Ler e excluir as formas de pagamento que uma conta já tem continua disponível.

### GovData

Dados abertos do governo dos EUA. O acesso é particionado por agrupamento de assunto.

Uma fonte recebe um ou mais assuntos, e cada assunto traz os esquemas do adaptador listados abaixo. Um esquema pode pertencer a mais de um assunto (`census`). Os esquemas `ref` e `geo` são esquemas de ligação: dados de referência e geografia pelos quais as tabelas de todo assunto se unem, então toda fonte os serve, quaisquer que sejam os assuntos que recebeu. A lista é declarada uma única vez no servidor e é mantida, por um teste, igual aos esquemas que o pacote do adaptador fixado serve.

| Assunto | Esquemas Expostos |
| --------- | ----------------- |
| `COMMERCE` | `sec`, `patents`, `cftc` |
| `ECONOMY` | `econ`, `econ_reference`, `ag`, `banking`, `fiscal`, `housing`, `transport` |
| `EDUCATION` | `census`, `edu`, `research` |
| `HEALTH` | `health` |
| `CYBER` | `cyber_threat`, `cyber_vuln` |
| `PUBLIC_SAFETY` | `crime`, `disasters` |
| `ENVIRONMENT` | `lands`, `environment` |
| `WEATHER` | `weather` |
| `ENERGY` | `energy` |
| `GOVERNMENT` | `fedregister`, `fec`, `officials`, `law` |
| `DEMOGRAPHICS` | `census` |
| toda fonte | `ref`, `geo` |

`ALL` é o atalho de uma assinatura para todos os assuntos; não é uma escolha no formulário de Fontes.

```yaml
sources:

  - id: federal-commerce
    type: govdata
    subject: COMMERCE
    domain_id: federal-analytics
    description: U.S. commerce and securities data
```

| Campo | Obrigatório | Padrão | Descrição |
| ------- | ---------- | --------- | ------------- |
| `id` | Sim | — | Identificador único |
| `subject` | Sim | — | Um dos valores de assunto acima |
| `domain_id` | Sim | — | Domínio ao qual esta fonte pertence |
| `description` | Não | `""` | Descrição legível por humanos |

Uma fonte AskAmerica criada no admin recebe uma credencial, a chave de API. O Provisa apresenta a chave à API da AskAmerica e recebe as credenciais de armazenamento, de curta duração e somente leitura, com as quais os dados são lidos; a chave é recusada ali, pelo nome, se for desconhecida ou revogada.

A fonte é então uma fonte Postgres-wire como as fontes SharePoint, Salesforce e inventário de nuvem: o Provisa inicia para ela o servidor `pgwire-govdata` embutido, com a chave e essas credenciais no ambiente do servidor, e cada motor alcança esse servidor como alcança o de uma fonte irmã. DuckDB, Postgres e ClickHouse o anexam e leem suas tabelas no local, então uma tabela da AskAmerica se une a tabelas de outras fontes, fica sob views e é entregue como qualquer outra. O Trino não tem conector para ela e lê uma réplica pousada pelo servidor: uma cópia da tabela mantida no armazenamento de materialização, tão atual quanto seu último refresh sob as configurações de cache e replicação da tabela, e não lida no momento da consulta. Uma tabela é lida do esquema do adaptador que ela nomeia (`sec`, `econ` e assim por diante).

O servidor monta seus esquemas a partir do armazenamento de objetos antes de aceitar uma conexão, o que leva minutos na primeira inicialização. Ele é iniciado quando a fonte é salva; até que esteja pronto, o formulário de Register Table informa que a fonte ainda está iniciando.

### Onde vive a senha de uma fonte {: #where-a-sources-password-lives }

A senha de uma fonte nunca é armazenada ao lado do restante de suas configurações de conexão. A
linha `sources` do plano de controle carrega uma coluna `password_ref` contendo o que foi digitado
como senha: o próprio valor, ou uma referência como `${env:PG_PASSWORD}` ou
`${secret:SNOWFLAKE_KEY}`. Ela é resolvida no momento em que a fonte é discada, dentro da
organização em que a requisição está atuando.

Um valor sem `${…}` é usado como está. `${env:VAR}` lê o ambiente de processo da implantação e não
precisa de nada vinculado. `${secret:NAME}` nomeia um segredo que uma organização possui, então ele
se resolve apenas dentro das próprias operações dessa organização: as costuras de introspecção do
admin e o terminal de consulta que toda interface alcança estabelecem essa vinculação.

O campo guarda o que você digitou, seja qual for a forma como a fonte foi registrada:

- **A partir da config.** O arquivo é o registro: o Provisa copia o valor ou a referência para
  `password_ref` literalmente.
- **A partir do formulário de Fontes.** O que é digitado no campo de senha é armazenado
  literalmente, seja um valor ou uma referência. O Provisa não escreve nada no cofre da organização
  para ele e não inventa nenhum nome de segredo. Para manter uma credencial fora da linha do plano
  de controle, crie o segredo você mesmo (veja [Segredos](secrets.md)) e digite `${secret:NAME}` no
  campo.

Um valor digitado como valor é armazenado no banco de dados do plano de controle como foi digitado.
Redigitar a senha substitui o texto do campo. Excluir a fonte remove sua linha e nada do cofre: um
segredo que você criou é seu.

`password_ref` não viaja entre ambientes. Um branch ou um ambiente copiado fornece
seus próprios valores de conexão, e o cofre que uma referência nomeia pertence a qualquer que seja
o ambiente que a forneceu.

### Data profiler {: #data-profiler }

Um data profiler é uma fonte que guarda um nome, uma agenda e os padrões de execução, e nada mais. Ele nunca passa por introspecção. Tabelas de outras fontes são adicionadas a ele, e cada execução as mede e grava suas linhas no plano de controle da organização, onde são lidas como as tabelas de qualquer outra fonte. Veja [Test data](test-data.md) para criar um profiler e adicionar tabelas a ele.

### Verificadores de Qualidade de Dados {: #data-quality-checkers }

Um verificador de qualidade de dados é só mais um tipo de fonte. A saída da varredura são dados: cada resultado de verificação é uma observação, então ela é pousada pelo caminho comum das fontes e recebe o que qualquer outra fonte recebe, de cadência, atualidade, eventos e linhagem até governança, RLS, a grade e a exportação.

Dois verificadores são suportados, e escolher entre eles é tanto uma questão de licença quanto de recursos.

| Tipo de Fonte | Dialeto do Contrato | Extra | Licença | Plano de nuvem hospedado |
| ------------ | ----------------- | ------- | --------- | -------------------- |
| `soda` | YAML de contrato Soda | `pip install .[soda]` (`soda-postgres`) | Elastic License 2.0 | Recusado — veja abaixo |
| `great_expectations` | JSON de suíte de expectativas | `pip install .[gx]` (`great-expectations[postgresql]`) | Apache 2.0 | Permitido |

A Elastic License 2.0 proíbe oferecer o software a terceiros como serviço hospedado ou gerenciado, e rodar o Soda dentro do plano SaaS em nome de um inquilino seria exatamente isso. O `config/capabilities.yaml` registra isso como `cloud_eligible: false` na opção `soda`, e o plano hospedado lê essa flag. Um deployment hospedado que queira Soda aponta para um endpoint Soda que o próprio operador roda.

O Provisa não embute nem vincula nada. A varredura roda num interpretador filho (`python -m provisa.dq.worker`), o único lugar onde `soda_core` ou `great_expectations` é importado. Assim, um verificador source-available nunca chega ao processo do servidor, e se um verificador falhar, ele derruba um subprocesso e deixa o loop de eventos em paz.

**A fonte aponta para o próprio endpoint pgwire do Provisa.** É assim que um único driver postgres consegue verificar uma tabela apoiada em Snowflake ou Iceberg: o verificador varre a visão federada, nunca o sistema por baixo. A política se aplica a essa conexão, então você declara a identidade da varredura de forma explícita. Se ela fosse herdada, um conjunto filtrado de linhas poderia fazer uma verificação passar sem ninguém notar.

```yaml
sources:

  - id: dq
    type: soda
    domain_id: sales-analytics
    description: Soda contract scans over the governed estate
    mapping:
      host: localhost
      port: 5439          # Provisa's pgwire endpoint
      database: provisa
      user: dq_scanner    # the scan identity, declared explicitly
      password: ${env:PROVISA_DQ_PASSWORD}
```

**Uma tabela de resultados por contrato, e o contrato é todo o registro.** A tabela carrega `dq_contract`, o texto do contrato palavra por palavra, e nada mais sobre sua forma. Colunas, marca d'água e promoções são todas derivadas dele.

```yaml
tables:

  - source_id: dq
    schema_name: quality
    table_name: orders_scan
    domain_id: sales-analytics
    change_signal: ttl_probe
    cache_ttl: 3600
    columns:
      - name: scan_id          # declared only to carry visible_to; replaced at parse
        visible_to: [analyst, admin]
    dq_contract: |
      dataset: provisa/sales/orders
      columns:
        - name: customer_id
          checks:
            - missing:
                threshold:
                  metric: percent
                  must_be_less_than: 1
      checks:
        - row_count:
            must_be_greater_than: 0
```

O registro deriva quatro coisas desse texto:

- **Linhagem.** O contrato já nomeia seu conjunto de dados alvo, então o registro o interpreta como `extract_inputs` interpreta SQL e o resolve para a tabela governada. Há uma definição só e nenhuma segunda cópia que possa divergir. Um contrato que nomeia um conjunto de dados não governado falha de forma visível no registro, então nunca pousa linhas que ninguém pediu.
- **Colunas.** O envelope de resultado pertence ao verificador: 16 colunas incluídas, de `scan_id` a `diagnostics`. Das colunas declaradas só se lê o `visible_to`, que deve ser igual em todas, e depois elas são substituídas.
- **Marca d'água.** `scan_time` se torna a marca d'água, o que faz do pouso um append. O histórico de varreduras cresce sem um subsistema de histórico.
- **Promoções.** `freshness_max_timestamp` e `dataset_rows_tested` são promovidos para fora do jsonb `diagnostics` como colunas tipadas. Você pode adicionar outras como faria em qualquer outra coluna jsonb.

O agendamento não pede campos novos. `change_signal` mais `cache_ttl` definem a frequência de consulta, `mv_debounce_quiet` e `mv_debounce_max_delay` juntam uma rajada de mudanças a montante numa só varredura, uma granularidade de calendário a torna periódica e `expected_events` segura a varredura até que suas entradas estejam atualizadas em toda a janela. O loop de consulta é o agendador de varreduras.

`outcome` é um de `pass`, `fail`, `warn`, `error` ou `skipped`. Nenhum deles impõe nada por si só. Se você quiser imposição, declare-a à parte: um preflight, ou uma MV sobre os resultados pousados. Uma observação pousada não precisa ser determinística, então aqui cabem verificações que nunca poderiam controlar um preflight, como uma pontuação de anomalia, uma variação numa janela móvel ou a atualidade medida em relação a agora.

Você escreve o contrato na UI, no painel de qualidade de dados da página de edição da tabela, e o texto bruto do contrato ali é sempre a fonte da verdade. Uma execução de teste aplica o contrato à tabela ao vivo e mostra os resultados sem pousá-los. Use-a para pegar um contrato cujo nome de conjunto de dados foi resolvido num lugar inesperado e que, de outra forma, só pousaria linhas aprovadas.

---

## Conectores Personalizados {: #custom-connectors }

Um operador pode dar aos motores de federação nativos (Postgres, DuckDB e ClickHouse) alcance a um novo tipo de fonte declarando um conector para ele em `config/custom_connectors.yaml`. Não é preciso código.

Assim os motores nativos, que rodam sem cluster, ganham a mesma extensibilidade por configuração que o motor Trino tem na própria camada. Lá, um conector JDBC genérico é parametrizado por tipo de fonte, cada tipo recebe um corpo `.properties` de catálogo, e o Provisa acrescenta seus próprios plugins de conector Trino (Splunk, SharePoint, Calcite).

O arquivo vem vazio. Os conectores embutidos cobrem as fontes que funcionam sem configuração extra, e tudo neste arquivo é escrito pelo operador. Para usar outro caminho, por exemplo em testes, defina `PROVISA_CUSTOM_CONNECTORS`.

### Tipos de descritor {: #descriptor-kinds }

| Motor | Tipo | Mecanismo | O que o descritor fornece |
| --- | --- | --- | --- |
| `postgres` | `pg_fdw` | SQL/MED (padrão ISO) | `extension`, `server_options`, `user_mapping`, `supports_import`, `table_options`, `remote_schema` |
| `duckdb` | `duckdb_attach` | INSTALL/LOAD + ATTACH | `extension`, `probe_symbol`, `attach_template`, `remote_schema` |
| `duckdb` | `duckdb_scan` | INSTALL/LOAD + view de scanner | `extension`, `probe_symbol`, `scan_template` |
| `clickhouse` | `clickhouse_database` | `CREATE DATABASE ENGINE=…` (expõe automaticamente toda tabela remota) | `ch_engine`, `engine_template` |
| `clickhouse` | `clickhouse_table` | `CREATE TABLE ENGINE=…` por tabela (colunas do registro) | `ch_engine`, `engine_template` (pode carregar `{table}`) |
| `clickhouse` | `clickhouse_scan` | `CREATE TABLE ENGINE=…`, ClickHouse infere o esquema | `ch_engine`, `engine_template` |

**Postgres é genérico.** SQL/MED é um padrão ISO, então todo FDW conforme compartilha a mesma forma de DDL: `CREATE SERVER … FOREIGN DATA WRAPPER <fdw> OPTIONS(…)`, `CREATE USER MAPPING` opcional, depois ou `IMPORT FOREIGN SCHEMA` (quando `supports_import: true`) ou um `CREATE FOREIGN TABLE` explícito por tabela (quando `false`). Um descritor `pg_fdw` fornece apenas a variação por FDW: nome da extensão, chaves de opção de servidor, chaves de mapeamento de usuário, flag de importação, opções de tabela. Qualquer FDW conforme ao padrão é, portanto, controlável apenas pela config.

**DuckDB suporta dois mecanismos.** Uma extensão que expõe um catálogo via ATTACH usa `duckdb_attach`; uma que expõe uma table-function de leitura usa `duckdb_scan`. Uma extensão que não se encaixa em nenhum dos padrões não é suportada.

**ClickHouse suporta três mecanismos**, um por forma de motor de integração: um motor DATABASE relacional que expõe automaticamente toda tabela remota (`clickhouse_database`, ex.: Redis/MySQL), um motor por tabela cujas colunas o registro fornece (`clickhouse_table`, ex.: a ponte JDBC/ODBC; o `engine_template` pode carregar um placeholder `{table}` que o runtime vincula), e um motor de arquivo/lake/URL cujo esquema o ClickHouse infere (`clickhouse_scan`, ex.: HDFS/URL). SQLite (motor DATABASE, arquivo, sem servidor) e Hudi (lakehouse, zero-cópia) são entregues prontos para uso.

Um valor `kind` desconhecido falha de forma visível na inicialização, então um erro de digitação num descritor nunca deixa um tipo de fonte inalcançável em silêncio.

### Gate de sondagem {: #probe-gating }

A disponibilidade é verificada no momento do attach contra o catálogo de descoberta padrão de cada motor:

- **Postgres** — verifica `pg_extension`, depois `pg_available_extensions`.
- **DuckDB** — executa `INSTALL`/`LOAD` e verifica `duckdb_functions()` pelo `probe_symbol` declarado.
- **ClickHouse** — verifica `system.table_engines` pelo `ch_engine` declarado; ausente da build falha alto.

Uma extensão declarada que não é instalável falha alto. Sem skip silencioso, sem fallback. Um conector cuja sondagem falha simplesmente não fica ativo para aquela implantação.

### Variáveis de template {: #template-variables }

Todo valor `server_options`, valor `user_mapping`, `attach_template`, e `scan_template` pode usar placeholders `{field}`. Campos disponíveis:

`{id}`, `{host}`, `{port}`, `{database}`, `{username}`, `{password}`, `{path}`, `{schema_name}`, `{table_name}`, mais qualquer chave de `federation_hints`. Os templates de attach do DuckDB também recebem `{alias}`, o alias de catálogo interno que o Provisa dá ao banco anexado.

Um template que referencia um campo desconhecido falha alto no momento do attach, expondo uma incompatibilidade descritor/fonte antes que um DDL quebrado alcance o motor.

### Exemplos {: #examples }

**Postgres — MongoDB via `mongo_fdw` (sem importação de esquema; colunas fornecidas por tabela)**

```yaml
# config/custom_connectors.yaml
connectors:
  - engine: postgres
    source_type: mongodb
    kind: pg_fdw
    extension: mongo_fdw
    mechanism: attach_r
    server_options:
      address: "{host}"
      port: "{port}"
    user_mapping:
      username: "{username}"
      password: "{password}"
    supports_import: false
    table_options:
      database: "{database}"
      collection: "{table_name}"
```

**DuckDB — arquivos Excel via `read_xlsx` (table-function de scan)**

```yaml
  - engine: duckdb
    source_type: xlsx
    kind: duckdb_scan
    extension: excel
    install_from_community: false
    probe_symbol: read_xlsx
    scan_template: "read_xlsx('{path}')"
```

Com qualquer um dos descritores em vigor, registrar uma fonte com o `source_type` declarado é roteado através do conector personalizado, sujeito a uma sondagem bem-sucedida. Nenhuma outra mudança de configuração é necessária.

---

## Warehouses como Fontes Nomeadas {: #warehouses-as-named-sources }

Snowflake, Databricks, e ClickHouse podem ser registrados como fontes nomeadas independentemente de qual motor de federação está ativo.

Depois de registrado, o warehouse é lido pelo DirectDriver da fonte, que pousa uma réplica no armazenamento de materialização do motor ativo, e a consulta roda contra essa réplica. No caminho direto usual (asyncpg, aiomysql) o motor é pulado por completo. Aqui o motor ainda executa a consulta, mas contra uma réplica local em vez de ir pela rede até o warehouse a cada requisição.

As leituras são Arrow-nativas onde o warehouse suporta: Databricks usa Cloud Fetch, Snowflake usa `fetch_arrow_table`, e ClickHouse usa a interface HTTP colunar nativa.

Parâmetros de conexão estendidos que os campos padrão `host`/`port`/`username`/`password` não conseguem carregar vão em `federation_hints`:

```yaml
sources:
  - id: my-databricks
    type: databricks
    host: my-workspace.azuredatabricks.net
    password: ${env:DATABRICKS_TOKEN}
    federation_hints:
      http_path: /sql/1.0/warehouses/xxxx   # required — the SQL Warehouse connection detail

  - id: my-snowflake
    type: snowflake
    host: org.snowflakecomputing.com
    username: svc_provisa
    password: ${env:SNOWFLAKE_PASSWORD}
    federation_hints:
      account: myorg-myaccount    # required — Snowflake account identifier
      warehouse: COMPUTE_WH       # optional — virtual warehouse to use
      role: PROVISA_ROLE          # optional — Snowflake role

  - id: my-clickhouse
    type: clickhouse
    host: ch.example.com
    port: 8123
    database: analytics
    username: default
    password: ${env:CLICKHOUSE_PASSWORD}
    federation_hints:
      secure: "true"              # optional — enables TLS on the HTTP interface
```

O registro como fonte nomeada é independente de selecionar o mesmo warehouse como o motor de federação. Uma fonte Snowflake em um motor DuckDB pousa uma réplica no DuckDB, não no Snowflake.

Dados de objetos e de lake na nuvem (arquivos parquet, csv, iceberg e delta_lake no S3, GCS ou R2) são um tipo de fonte à parte. Eles são anexados no lugar quando o motor ativo tem um conector ATTACH para esse tipo, então nenhuma réplica é pousada e o motor varre o armazenamento de objetos diretamente. As credenciais dessas fontes também vão em `federation_hints`:

```yaml
sources:
  - id: r2-events
    type: parquet
    path: s3://my-bucket/events/2026/*.parquet
    federation_hints:
      access_key_id: ${env:R2_ACCESS_KEY}
      secret_access_key: ${env:R2_SECRET}
      account_id: ${env:R2_ACCOUNT_ID}     # Cloudflare R2 account (S3-compatible)
```

---

## Campos de Configuração de Fonte {: #source-configuration-fields }

Todas as fontes compartilham um conjunto comum de campos.

| Campo | Obrigatório | Padrão | Descrição |
| ------- | ---------- | --------- | ------------- |
| `id` | Sim | — | Identificador único; alfanumérico com hífens/underscores |
| `type` | Sim | — | Tipo de fonte (veja tabelas acima) |
| `host` | Não | `""` | Hostname ou IP |
| `port` | Não | `0` | Número da porta |
| `database` | Não | `""` | Nome do banco de dados |
| `username` | Não | `""` | Usuário |
| `password` | Não | `""` | Senha; use `${env:VAR}` ou `${secret:NAME}` em vez de um literal (veja abaixo) |
| `path` | Não | `null` | Caminho de arquivo ou URI em nuvem para fontes baseadas em arquivo e objeto/lake |
| `base_url` | Não | `null` | URL base para fontes OpenAPI |
| `pool_min` | Não | `1` | Tamanho mínimo do pool de conexão |
| `pool_max` | Não | `5` | Tamanho máximo do pool de conexão |
| `use_pgbouncer` | Não | `false` | Roteia conexões através do PgBouncer |
| `pgbouncer_port` | Não | `6432` | Porta do PgBouncer |
| `cache_enabled` | Não | `true` | Habilita cache de resposta de API |
| `cache_ttl` | Não | `null` | TTL de cache em segundos; herda o padrão global quando nulo |
| `cache_catalog` | Não | `null` | Catálogo federado para cache de API; padrão é o próprio catálogo da fonte |
| `cache_schema` | Não | `api_cache` | Esquema dentro do catálogo de cache |
| `naming_convention` | Não | `null` | Sobrepõe a convenção de nomenclatura global para esta fonte |
| `federation_hints` | Não | `{}` | Propriedades de sessão passadas ao motor de federação, e parâmetros de conexão estendidos para fontes de warehouse |
| `mapping` | Não | `{}` | Configurações de conector específicas de tipo para fontes NoSQL e SaaS (ex.: `auth_type` do SharePoint, `auth_type` do Salesforce, `use_token` do Splunk) |
| `allowed_domains` | Não | `[]` | Restringe a fonte a domínios específicos; vazio = irrestrito |
| `description` | Não | `""` | Descrição legível por humanos |

---

## Fontes Kafka {: #kafka-sources }

Tópicos Kafka são configurados separadamente sob `kafka_sources`, indexados pelo `id` da fonte de uma fonte `kafka` registrada.

```yaml
kafka_sources:

  - id: kafka-support
    topics:

      - id: tickets
        topic: support.tickets
        domain_id: sales-analytics
        description: "Inbound support tickets"
        default_window: 1h
        columns:

          - name: id
          - name: subject
          - name: status
          - name: created_at
```

| Campo | Descrição |
| ------- | ------------- |
| `id` | Deve corresponder ao `id` de uma fonte com `type: kafka` |
| `topics[].id` | Nome lógico para este tópico dentro do Provisa |
| `topics[].topic` | Nome do tópico Kafka |
| `topics[].domain_id` | Domínio ao qual este tópico pertence |
| `topics[].description` | Descrição legível por humanos |
| `topics[].default_window` | Janela de tempo padrão para consultas em janela (ex.: `1h`) |
| `topics[].columns` | Definições de coluna para o esquema do tópico |

---

## Visibilidade de Coluna {: #column-visibility }

O campo `visible_to` em cada coluna é uma lista de IDs de função que podem ver aquela coluna.

```yaml
columns:

  - name: email
    visible_to: [admin]        # only admin role sees this column

  - name: region
    visible_to: [admin, analyst]  # both roles see this column
```

Colunas omitidas da lista `visible_to` de uma função não aparecem no esquema GraphQL dessa função e não podem ser consultadas ou referenciadas em filtros.

---

## Relacionamentos {: #relationships }

Relacionamentos conectam duas tabelas registradas e aparecem como campos aninhados no GraphQL.

```yaml
relationships:

  - id: orders-to-customers
    source_table_id: orders
    target_table_id: customers
    source_column: customer_id
    target_column: id
    cardinality: many-to-one
```

| Campo | Obrigatório | Descrição |
| ------- | ---------- | ------------- |
| `id` | Sim | Identificador único para este relacionamento |
| `source_table_id` | Sim | Tabela que contém a chave estrangeira |
| `target_table_id` | Sim | Tabela sendo referenciada; vazio para relacionamentos computados |
| `source_column` | Sim | Coluna na tabela de origem |
| `target_column` | Sim | Coluna na tabela alvo; vazio para relacionamentos computados |
| `cardinality` | Sim | `many-to-one` ou `one-to-many` |
| `materialize` | Não | Cria automaticamente uma view materializada para joins entre fontes. Em uma aresta apoiada em junção, a view cobre a travessia de dois saltos, não um join direto |
| `refresh_interval` | Não | Intervalo de atualização de MV em segundos (padrão: 300) |
| `target_function_name` | Não | Nome da função do BD para relacionamentos computados |
| `function_arg` | Não | Qual argumento da função recebe o valor da coluna de origem |
| `alias` | Não | Tipo de relacionamento legível por humanos (ex.: `WORKS_FOR`) |
| `graphql_alias` | Não | Nomeia o campo SDL que este relacionamento expõe no tipo pai. Quando ausente, o nome é derivado do `field_name` da tabela alvo e da cardinalidade do relacionamento. |
| `disable_cypher` | Não | Quando `true`, exclui este relacionamento das arestas do grafo Cypher |
| `source_json_key` | Não | Extrai esta chave da coluna de origem como um objeto JSON antes do JOIN |
| `via_table` | Não | Nome da tabela registrada da junção que esta aresta percorre. Defini-lo torna a aresta apoiada em junção; deixá-lo vazio a mantém uma aresta de chave estrangeira |
| `via_source_column` | Não | Coluna da junção que pareia com `source_column`. Separada por vírgulas e posicional para uma chave composta |
| `via_target_column` | Não | Coluna da junção que pareia com `target_column` |
| `via_type_column` | Não | Coluna discriminadora, quando uma mesma junção carrega vários tipos de relacionamento |
| `via_type_value` | Não | O valor de discriminador ao qual esta aresta está fixada |
| `via_label_source` | Não | Qual indicação nomeia o tipo Cypher: `column` (o valor do discriminador), `table` (o nome da tabela de junção) ou `fixed` (o alias declarado). Todos são convertidos para UPPER_SNAKE_CASE |

### Relacionamentos apoiados em junção {: #junction-backed-relationships }

Você pode declarar uma tabela associativa como um relacionamento Cypher por si só, em vez de um nó. As colunas dela passam a ser os atributos do relacionamento:

```yaml
relationships:

  - id: pets-bonded-pair
    source_table_id: pets
    target_table_id: pets
    source_column: id
    target_column: id
    cardinality: one-to-many
    via_table: pet_companions
    via_source_column: pet_id
    via_target_column: companion_pet_id
    via_type_column: relation_type
    via_type_value: bonded pair
    via_label_source: column
```

A junção é uma tabela registrada como qualquer outra, e precisa estar registrada antes que um relacionamento possa nomeá-la. Declare um relacionamento por valor do discriminador. Três linhas sobre `pet_companions` dão `BONDED_PAIR`, `LITTERMATE` e `SHARES_ENCLOSURE` como três tipos Cypher distintos, cada um levando as outras colunas da linha de junção como propriedades da aresta. A configuração de demonstração incluída declara exatamente isso.

Uma aresta de junção só existe no Cypher. O emissor de joins do GraphQL monta sua cláusula `ON` para um único par de colunas e não tem onde pôr o segundo salto, então as arestas de junção ficam fora do SDL gerado e do `pg_constraint`. A própria tabela de junção continua consultável como seu campo raiz. Ela sai do lado de nós do esquema de grafo do Cypher, então nunca aparece como rótulo de nó.

`materialize: true` também funciona numa aresta de junção. O que se materializa é a travessia inteira: a visão guarda o salto de origem, o salto de junção, o discriminador e as colunas próprias da junção ao lado das do destino. A junção é uma terceira perna do join, então se a aresta cruza fontes é julgado pelas três tabelas. Uma junção que fica numa fonte diferente das duas tabelas que liga é materializada mesmo quando essas duas compartilham a fonte. Cada declaração materializa um tipo de aresta, então uma visão construída para `bonded pair` nunca responde a uma travessia `littermate`.

Valores de cardinalidade:

- `many-to-one`: cada linha de origem corresponde a uma linha de destino (FK para PK)
- `one-to-many`: cada linha de origem corresponde a várias linhas de destino (o inverso)

---

## Regras de Segurança em Nível de Linha {: #row-level-security-rules }

Regras de RLS injetam cláusulas `WHERE` no momento da consulta, escopadas a uma função e opcionalmente a uma tabela ou domínio.

```yaml
rls_rules:

  - table_id: orders          # applies to orders table only
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"
    description: Analysts read only their own region's orders.

  - domain_id: sales-analytics  # applies to every table in domain
    role_id: analyst
    filter: "tenant_id = current_setting('provisa.tenant_id')"
    description: Analysts read only their own tenant's rows.
```

Quando existem tanto uma regra em nível de domínio quanto uma regra em nível de tabela para a mesma função, a regra em nível de tabela tem precedência.

| Campo | Obrigatório | Descrição |
| ------- | ---------- | ------------- |
| `table_id` | Condicional | Tabela à qual a regra se aplica; mutuamente exclusiva com `domain_id` |
| `domain_id` | Condicional | Domínio ao qual a regra se aplica; aplica-se a todas as tabelas no domínio |
| `role_id` | Sim | Função à qual esta regra se aplica |
| `filter` | Sim | Predicado SQL injetado em `WHERE`; pode referenciar variáveis de sessão |
| `description` | Sim | Para que serve a regra, como uma revisão de acesso a lê |

---

## Funções e Webhooks {: #functions-and-webhooks }

### Funções de BD {: #db-functions }

Rastreie uma função de banco de dados e a exponha como uma consulta ou mutação GraphQL.

Fontes de banco de dados também podem auto-descobrir seus procedimentos armazenados e funções a partir do catálogo do fornecedor (`pg_proc`, `information_schema.routines`, ou equivalentes do fornecedor), removendo a necessidade de registrar cada uma manualmente. A descoberta lê `prokind` e `provolatile`: funções imutáveis/estáveis se registram como relações parametrizadas (argumentos do procedimento tornam-se parâmetros de consulta, a mesma forma das tabelas GET do OpenAPI), e procedimentos voláteis se registram como mutações/funções rastreadas. Rotinas descobertas fluem pela governança de Estágio 2 identicamente às registradas manualmente.

```yaml
functions:

  - name: get_customers_by_region
    source_id: sales-pg
    schema: public
    function_name: get_customers_by_region
    returns: customers
    domain_id: sales-analytics
    description: "Returns customers filtered by region"
    visible_to: [admin, analyst]
    kind: query
    arguments:

      - name: p_region
        type: String
```

| Campo | Obrigatório | Padrão | Descrição |
| ------- | ---------- | --------- | ------------- |
| `name` | Sim | — | Nome do campo GraphQL |
| `source_id` | Sim | — | Fonte contendo a função |
| `schema` | Não | `public` | Esquema do banco de dados |
| `function_name` | Sim | — | Nome real da função no banco de dados |
| `returns` | Sim | — | ID da tabela registrada que a função retorna |
| `arguments` | Não | `[]` | Lista de definições de argumento `{name, type}` |
| `visible_to` | Não | `[]` | Funções às quais a função está atribuída (vazio: todas as funções). Uma `mutation` exige também o direito `write` |
| `domain_id` | Não | `""` | Domínio ao qual esta função pertence |
| `description` | Não | `null` | Descrição do campo GraphQL |
| `kind` | Não | `mutation` | `"query"` ou `"mutation"` |

### Webhooks

Exponha um endpoint HTTP externo como uma consulta ou mutação GraphQL.

```yaml
webhooks:

  - name: notify_support
    url: http://localhost:9999/notify
    method: POST
    timeout_ms: 3000
    domain_id: sales-analytics
    description: "Send a support notification"
    visible_to: [admin]
    kind: mutation
    arguments:

      - name: message
        type: String
```

| Campo | Obrigatório | Padrão | Descrição |
| ------- | ---------- | --------- | ------------- |
| `name` | Sim | — | Nome do campo GraphQL |
| `url` | Sim | — | URL do endpoint do webhook |
| `method` | Não | `POST` | Método HTTP |
| `timeout_ms` | Não | `5000` | Timeout da requisição em milissegundos |
| `returns` | Não | `null` | ID de tabela registrada, ou null para tipo inline |
| `inline_return_type` | Não | `[]` | Lista de campos `{name, type}` para formatos de retorno personalizados |
| `arguments` | Não | `[]` | Lista de definições de argumento `{name, type}` |
| `visible_to` | Não | `[]` | Funções que podem chamar este webhook |
| `domain_id` | Não | `""` | Domínio ao qual este webhook pertence |
| `description` | Não | `null` | Descrição do campo GraphQL |
| `kind` | Não | `mutation` | `"query"` ou `"mutation"` |

---

## Autenticação {: #authentication }

A autenticação é configurada sob a chave `auth`.

| Provedor | Descrição |
| ---------- | ------------- |
| `none` | Sem autenticação; todas as requisições tratadas como o `default_role` |
| `firebase` | Firebase Authentication; exige `project_id` e `service_account_key` |
| `keycloak` | Keycloak OIDC |
| `oauth` | OAuth 2.0 genérico |
| `simple` | Usuário/senha sem um provedor externo |

```yaml
auth:
  provider: firebase
  assignments_source: provisa   # "claims" or "provisa"
  default_role: analyst
  default_assignments:

    - role_id: analyst
      domain_id: "*"
  firebase:
    project_id: ${env:FIREBASE_PROJECT_ID}
    service_account_key: ${env:FIREBASE_SERVICE_ACCOUNT_KEY}
```

`assignments_source: claims` lê atribuições de função das claims JWT. `assignments_source: provisa` as lê do próprio armazenamento de atribuições do Provisa.

---

## Roteamento de Execução {: #execution-routing }

**Execução direta.** Uma consulta que toca uma só fonte relacional vai direto ao driver próprio dessa fonte, em menos de 100 ms. Um tipo de fonte segue esse caminho quando o Provisa conhece tanto o dialeto SQL quanto o conector dele.

**Execução federada.** Uma consulta entre fontes, ou sobre uma fonte sem driver direto, passa pelo motor de federação. O Provisa traz um embutido; em deployments grandes, aponte-o para o seu próprio cluster compatível.

**Estatísticas.** Quando uma tabela é registrada, o Provisa roda `ANALYZE` em cada tabela publicada para que o otimizador baseado em custo tenha contagens de linhas, frações de nulos, valores distintos e mín/máx. Se falhar, a falha é registrada no log e o registro segue em frente.

---

## Fontes de Grafo e Semânticas {: #graph-semantic-sources }

### Neo4j

Registre um banco de dados de grafo Neo4j como uma fonte consultável. Stewards escrevem consultas Cypher que projetam valores escalares; o Provisa armazena os resultados em cache e os expõe como tipos GraphQL.

Consultas Cypher precisam usar acessos a propriedades na cláusula `RETURN` (`RETURN n.id AS id, n.name AS name`). O registro recusa uma consulta que devolve objetos de nó.

#### Registro via arquivo de config {: #config-file-registration }

Declare uma fonte `neo4j` e suas tabelas em YAML. Cada tabela exige um `query_template` (o Cypher que produz suas linhas) e colunas tipadas. A chave `query_template` é inválida sob qualquer outro tipo de fonte.

Uma fonte exige `host`, `port` e `database`. Linhas são buscadas fazendo POST de `{"statements": [{"statement": <cypher>}]}` para `/db/<database>/tx/commit` (a API de transação HTTP do Neo4j). Uma resposta com uma lista `errors` não vazia é tratada como uma consulta falha, não como um resultado vazio.

Cada coluna exige `data_type`. O loader mapeia tipos de config para o tipo de coluna de API usado no momento da consulta:

| `data_type` da config | Tipo de API |
|---|---|
| `varchar`, `text`, `string`, `char` | string |
| `integer`, `int`, `bigint`, `smallint` | integer |
| `float`, `double`, `real`, `decimal`, `numeric`, `number` | number |
| `boolean`, `bool` | boolean |
| `json`, `jsonb` | jsonb |

`varchar(N)` e `decimal(10,2)` são aceitos, e usa-se o tipo base antes do parêntese.

O registro persiste uma linha `api_sources` e uma linha `api_endpoints` por tabela, então as tabelas sobrevivem a um restart sem reler o arquivo. Os endpoints REST de admin sob `/admin/sources/neo4j` escrevem as mesmas linhas.

```yaml
sources:
  - id: graph
    type: neo4j
    host: neo4j
    port: 7474
    database: neo4j
    cache_ttl: 300

tables:
  - source_id: graph
    schema: neo4j
    table: person_skills
    query_template: >-
      MATCH (p:Person)-[:HAS_SKILL]->(s:Skill)
      RETURN p.name AS name, s.skill AS skill, p.experience AS years
    columns:
      - name: name
        data_type: varchar
      - name: skill
        data_type: varchar
      - name: years
        data_type: integer
```

#### Register Table na UI {: #register-table-in-the-ui }

Uma fonte neo4j não tem tabelas para listar, então o formulário Register Table pede a tabela em vez de oferecer uma.

1. Escolha a fonte neo4j e um domínio. Os seletores de esquema e tabela, a checkbox discover e o seletor de watermark não aparecem; a fonte nunca é introspectada.
2. Digite um nome de tabela e o Cypher. O Cypher precisa projetar escalares (`RETURN a.name AS name`); uma projeção que retorna um nó ou uma lista é reportada como erro.
3. Pressione Preview. O formulário roda o Cypher com `LIMIT 5` através da consulta GraphQL `neo4jPreview` e preenche a lista de colunas a partir das linhas retornadas, tipadas como `text`, `integer`, `double`, `boolean` ou `json`. Um preview falho mantém o Cypher no editor e mostra a mensagem.
4. Ajuste visibilidade, aliases ou mascaramento como para qualquer tabela, então registre. O formulário se recusa a enviar até que um preview tenha tipado as colunas, e o servidor recusa uma tabela neo4j que não carrega Cypher (`schema.neo4j_query_required`).

O Cypher é armazenado com a tabela como `queryTemplate`, aparece na visão de leitura da tabela, e persiste exatamente como um registro via arquivo de config: uma linha `api_sources` e uma linha `api_endpoints` que o próximo start hidrata. Editar a tabela regrava um Cypher editado.

#### Registro REST de admin {: #admin-rest-registration }

```bash
# Register via admin API (no YAML config required)
POST /admin/sources/neo4j
{
  "source_id": "graph",
  "host": "neo4j",
  "port": 7474,
  "database": "neo4j"
}

# Register a table (preview + validate before persisting)
POST /admin/sources/neo4j/graph/tables
{
  "table_name": "person_skills",
  "cypher": "MATCH (p:Person)-[:HAS_SKILL]->(s:Skill) RETURN p.name AS name, s.skill AS skill, p.experience AS years",
  "ttl": 300
}
```

O endpoint de preview (`POST /admin/sources/neo4j/{id}/preview`) retorna linhas de amostra e bloqueia o registro se o Cypher retornar objetos de nó.

### SPARQL

Registre qualquer triplestore compatível com SPARQL 1.1 (Apache Jena Fuseki, Virtuoso, Stardog, etc.) como uma fonte consultável.

As consultas devem ser consultas `SELECT`. Nomes de variável na cláusula `SELECT` tornam-se nomes de coluna automaticamente.

```bash
# Register via admin API
POST /admin/sources/sparql
{
  "source_id": "knowledge-graph",
  "endpoint_url": "http://fuseki:3030/ds/sparql",
  "default_graph_uri": "http://example.org/graph"
}

# Register a table (executes LIMIT 5 probe to validate and infer columns)
POST /admin/sources/sparql/knowledge-graph/tables
{
  "table_name": "product_categories",
  "sparql_query": "SELECT ?product ?label ?category WHERE { ?product a :Product ; rdfs:label ?label ; :hasCategory ?category . }",
  "ttl": 600
}
```

Ambos os conectores usam o pipeline de cache de fonte de API. Os resultados são armazenados no PostgreSQL com TTL configurável, tornando-os disponíveis para JOINs federados entre fontes.

---

#### Registro via arquivo de config e UI {: #config-file-and-ui-registration }

O `host` de uma fonte `sparql` é a URL do seu endpoint SPARQL (o formulário de Fontes o armazena da mesma forma). Cada tabela sob ela carrega `query_template`, um SELECT cujas variáveis são as colunas. A pré-visualização tipa cada coluna pelo tipo de dado XSD que seus bindings compartilham: os tipos inteiros como `integer`, `xsd:decimal`, `xsd:double` e `xsd:float` como `double`, `xsd:boolean` como `boolean`, `xsd:date` como `date` e `xsd:dateTime` como `timestamp`. Um IRI, um nó em branco, uma string simples ou com etiqueta de idioma, ou uma variável cujos bindings diferem no tipo, é `text`.

```yaml
sources:
- id: sparql-demo
  type: sparql
  host: http://localhost:23030/provisa/query
tables:
- source_id: sparql-demo
  domain_id: shelter
  schema: sparql
  table: volunteer
  query_template: >-
    PREFIX s: <http://provisa.dev/shelter#>
    SELECT ?volunteer_id ?name WHERE { ?v a s:Volunteer ; s:id ?volunteer_id ; s:name ?name }
  columns:
  - { name: volunteer_id, data_type: text, visible_to: [org_admin] }
  - { name: name, data_type: text, visible_to: [org_admin] }
```

Register Table funciona da mesma forma que para Neo4j: escolha a fonte, digite um nome de tabela e o SELECT, pressione Preview (a consulta `sparqlPreview` a roda com `LIMIT 5` e preenche a lista de colunas), então registre. O registro persiste uma linha `api_sources` e uma linha `api_endpoints` (POST form-encoded para o caminho do endpoint, normalizador `sparql_bindings`), as mesmas linhas que um registro via arquivo de config escreve, e o motor nativo pousa as linhas através da mesma cadeia de busca que o Neo4j.

**Pushdown.** Quando uma instrução lê uma tabela SPARQL sozinha (sem join), suas condições sobre as colunas da tabela vão para a consulta enviada ao endpoint. O SELECT registrado vira uma subconsulta, `SELECT * WHERE { { <the SELECT> } FILTER(...) }`, com suas linhas `PREFIX` e `BASE` mantidas na frente.
- Empurradas: igualdade e `IN` em uma coluna `text` (comparadas como `STR(?var)`, de modo que um IRI corresponde ao seu texto); qualquer comparação em uma coluna `integer` ou `double`; igualdade em um `boolean`.
- Não empurradas: outras condições, como uma comparação de ordem sobre texto, uma data, uma função de uma coluna ou um parâmetro vinculado. Elas são aplicadas às linhas que o Provisa recebe.
- Toda condição é aplicada de novo às linhas, então empurrar uma só reduz o que é buscado.
- O `LIMIT` da instrução só é empurrado quando todas as condições foram empurradas e a instrução não agrupa, não agrega nem usa `DISTINCT`, `ORDER BY` ou `OFFSET`.
- Uma resposta reduzida é cacheada sob a própria consulta, separada da da tabela inteira.

#### Reasoner {: #reasoner }

Uma fonte `sparql` pode nomear um reasoner e um documento de axiomas OWL. Uma tabela cuja consulta é um único IRI entre colchetes angulares serve então essa classe ou propriedade como os axiomas a inferem:
- uma classe: uma coluna, seus membros;
- uma propriedade de objeto ou de dados: duas colunas, cada sujeito e seu valor.

```yaml
sources:
- id: kyc-graph
  type: sparql
  host: http://localhost:23030/kyc/query
  cache_ttl: 3600
  mapping:
    reasoner: hermit
    axioms: |
      @prefix : <http://example.org/kyc#> .
      @prefix owl: <http://www.w3.org/2002/07/owl#> .
      :controls a owl:ObjectProperty, owl:TransitiveProperty .
      :SanctionedParty a owl:Class .
      :SanctionsExposed owl:equivalentClass [ a owl:Restriction ;
        owl:onProperty [ owl:inverseOf :controls ] ; owl:someValuesFrom :SanctionedParty ] .
tables:
- source_id: kyc-graph
  domain_id: kyc
  schema: kyc
  table: sanctions_exposed
  query_template: <http://example.org/kyc#SanctionsExposed>
  columns:
  - { name: party, data_type: text, visible_to: [compliance] }
- source_id: kyc-graph
  domain_id: kyc
  schema: kyc
  table: controls
  query_template: <http://example.org/kyc#controls>
  columns:
  - { name: controller, data_type: text, visible_to: [compliance] }
  - { name: controlled, data_type: text, visible_to: [compliance] }
```

Os axiomas podem ser escritos em qualquer sintaxe que a OWL API leia: Turtle, RDF/XML, sintaxe funcional OWL ou sintaxe Manchester. O HermiT é o único reasoner.

Quando uma tabela inferida é lida, o reasoner:
1. lê as classes e propriedades que os axiomas nomeiam;
2. envia ao endpoint um `CONSTRUCT` para cada tripla dessas propriedades e cada pertinência a essas classes, pedindo N-Triples;
3. classifica essa fatia sob os axiomas;
4. devolve os fatos que decorrem disso.

Todas as tabelas inferidas de uma fonte compartilham uma classificação, reaproveitada pelo TTL de cache da tabela. A declaração de atualidade da fonte é, portanto, a frequência com que ela raciocina: `cache_ttl` para uma tabela ao vivo, a atualização da réplica para uma réplica, e uma única vez para um TTL de `-1`. O que uma tabela inferida serve é governado para cada leitor como qualquer outra tabela.

O HermiT roda como um programa separado, nunca dentro do servidor. Ele roda em um JDK 21 Eclipse Temurin fixado, com o HermiT, a OWL API e suas bibliotecas no class path. Cada peça é baixada no primeiro uso, verificada contra o SHA-256 que o Provisa fixa e cacheada sob a raiz de cache das dependências de execução (`$PROVISA_RUNTIME_DEPS_CACHE`, padrão `~/.cache/provisa/runtime_deps`). Um download que falha, ou um arquivo cuja soma difere da fixada, faz a leitura falhar indicando-o pelo nome.

## Exemplos de Conexão {: #connection-examples }

### PostgreSQL

```yaml
- id: sales-pg
  type: postgresql
  host: postgres
  port: 5432
  database: provisa
  username: provisa
  password: ${env:PG_PASSWORD}
```

### Snowflake

```yaml
- id: analytics-sf
  type: snowflake
  host: org.snowflakecomputing.com
  port: 443
  database: ANALYTICS
  username: svc_provisa
  password: ${env:SNOWFLAKE_PASSWORD}
  federation_hints:
    account: myorg-myaccount
    warehouse: COMPUTE_WH
```

### Databricks

```yaml
- id: lakehouse-db
  type: databricks
  host: my-workspace.azuredatabricks.net
  password: ${env:DATABRICKS_TOKEN}
  federation_hints:
    http_path: /sql/1.0/warehouses/xxxx
```

### MongoDB

```yaml
- id: reviews-mongo
  type: mongodb
  host: mongodb
  port: 27017
  database: provisa
  username: ""
  password: ""
```

### Consulta Entre Fontes {: #cross-source-query }

```graphql
{
  orders(where: {region: {eq: "us"}}) {
    id
    amount
    customers {       # PostgreSQL
      name
      email
    }
    productReviews {  # MongoDB (federated)
      rating
      comment
    }
  }
}
```

Porções de fonte única roteiam diretamente. JOINs entre fontes federam com coerção automática de tipo.
