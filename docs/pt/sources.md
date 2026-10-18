# Tipos de Fonte {: #source-types }

## Modelo de Execução {: #execution-model }

Toda consulta em última análise é executada através do motor de federação, que fornece federação entre todas as fontes. Fontes se dividem em três categorias com base em sua conectividade. [tool-verified: `provisa/core/models.py` lines 84–132] (REQ-550)

| Categoria | Tem Driver Direto | Tem Conector Federado | Exemplos |
| --- | --- | --- | --- |
| **Direta-capaz** | Sim | Sim | PostgreSQL, MySQL, MariaDB, SingleStore, SQL Server, Oracle, DuckDB |
| **Somente federação** | Não | Sim | Redshift, Druid, Exasol, Hive, Iceberg, Delta Lake, Hive (apoiado em S3) |
| **Leitura direta (réplica)** | Sim | Sim | Snowflake, Databricks, ClickHouse — o driver lê os dados e pousa uma réplica; consultas rodam contra a réplica no motor ativo |
| **Direta (sem conector entre fontes)** | Sim | Não | Athena — instruções de fonte única rodam inteiras no Athena; joins entre fontes pousam uma réplica completa |
| **Materializar → Federação** | Não | Não | REST/OpenAPI, GraphQL remoto, gRPC, Neo4j Cypher, SPARQL, WebSocket, RSS, CSV, SQLite, Parquet, Ingest (receptor push), GovData, SharePoint, Salesforce, ServiceNow, Cloud inventory, Splunk, Qdrant, Azure Cosmos DB, Logs, Google Workspace, Microsoft 365 |

Fontes **direta-capazes** executam consultas de fonte única via seu driver nativo (sub-100ms), contornando o motor de federação (REQ-027, REQ-229). Elas mantêm suporte completo a conectores e participam de federação quando unidas com outras fontes (REQ-028).

Fontes **somente federação** são sempre consultadas através da camada de federação. Nenhum driver direto existe (REQ-229).

Fontes de **leitura direta (réplica)** têm um DirectDriver que lê do warehouse nativamente (transporte Arrow-nativo quando disponível), pousa uma réplica no armazenamento de materialização do motor ativo, e então as consultas rodam contra essa réplica. Veja [Warehouses como Fontes Nomeadas](#warehouses-as-named-sources).

Fontes **diretas (sem conector entre fontes)** têm um DirectDriver que executa instruções de fonte única inteiras na própria fonte — todo filtro, coluna e limite é empurrado até ela. Quando uma instrução une a fonte a qualquer outra, o driver lê os dados e pousa uma réplica; o join roda contra essa réplica no motor ativo. Não existe conector Trino para essas fontes.

Fontes de **materialização** não têm conector federado. O Provisa busca seus dados (na inicialização ou no momento da consulta) e os armazena em cache como Parquet no S3 ou no PostgreSQL, tornando-os alcançáveis pelo motor de federação para consultas entre fontes (REQ-309).

---

## Todas as Fontes {: #all-sources }

O Provisa registra **72** tipos de fonte, os que o formulário de Fontes oferece. As tabelas abaixo cobrem os 72; o índice é a contagem. [tool-verified: `provisa-ui/src/pages/sources/constants.ts` `SOURCE_TYPES`, where `graphql` and `grpc` are `graphql_remote` and `grpc_remote`; Kaggle, Wikipedia, GitHub, GitLab and Stripe are counted as distinct sources even though they register through the `files`, `graphql_remote` and `openapi` connectors internally]

| # | Grupo | Tipos de fonte |
| --- | --- | --- |
| 1–15 | [RDBMS](#rdbms) | `postgresql`, `mysql`, `mariadb`, `singlestore`, `sqlserver`, `oracle`, `duckdb`, `cockroachdb`, `yugabytedb`, `greenplum`, `tidb`, `firebird`, `airport`, `saphana`, `hiveserver2` |
| 16–24 | [Data warehouses em nuvem](#cloud-data-warehouses) | `snowflake`, `bigquery`, `databricks`, `redshift`, `athena`, `fabric`, `synapse`, `trino`, [`motherduck`](#motherduck) |
| 25–29 | [Analytics / OLAP](#analytics-olap) | `clickhouse`, `druid`, `exasol`, `elasticsearch`, `pinot` |
| 30–33 | [Data lake / formatos de tabela aberta](#data-lake-open-table-formats) | `iceberg`, `delta_lake`, `hive`, `hive_s3` |
| 34–38 | [NoSQL](#nosql) | `mongodb`, `cosmosdb`, `qdrant`, `cassandra`, `redis` |
| 39–40 | [Fontes de código](arch/code-sources.md) | `node`, `python` |
| 41–43 | [Streaming](#streaming) | `kafka`, `websocket`, `rss` |
| 44 | [Receptor push](#push-receiver) | `ingest` |
| 45–46 | [Grafo e semântico](#graph-semantic) | `neo4j`, `sparql` |
| 47–51 | [Baseado em arquivo](#file-based) | `sqlite`, `csv`, `parquet`, `files`, `logs` |
| 52–53 | [Observabilidade e outros](#observability-other) | `google_sheets`, `prometheus` |
| 54–60 | [SaaS empresarial](#enterprise-saas-connectors) | `sharepoint`, `salesforce`, `servicenow`, `cloudops`, `splunk`, `google_workspace`, `microsoft_365` |
| 61–66 | [Fontes de API](#api-sources) | `openapi`, `graphql_remote`, `grpc_remote`, GitHub, GitLab, Stripe |
| 67 | [GovData](#govdata) | `govdata` |
| 68–69 | [Verificadores de qualidade de dados](#data-quality-checkers-req-1443) | `soda`, `great_expectations` |
| 70 | [Data profiler](#data-profiler) | `data_profiler` |
| 71 | [Conjuntos de dados Kaggle](#kaggle-datasets) | Kaggle (preparado pelo formulário de Fontes; registra-se como fonte `files` — veja [Conjuntos de dados Kaggle](#kaggle-datasets)) |
| 72 | [Wikipedia](#wikipedia) | Wikipedia (registra-se como fonte `files` que percorre as páginas indicadas — veja [Wikipedia](#wikipedia)) |

Referência para todo tipo de fonte que o Provisa suporta. "Driver direto" significa que consultas de fonte única são executadas contra a fonte nativamente (sub-100ms) (REQ-027). "Nome do Conector" é o conector federado usado quando a fonte participa de JOINs multi-fonte (REQ-028). [tool-verified: `provisa/core/source_registry.py` `SOURCE_TO_DIALECT`; `provisa/federation/trino_connectors.py` `trino_connector_name`]

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

Bancos de dados compatíveis com o protocolo de fio reutilizam o driver JDBC, o driver assíncrono nativo, e o dialeto de um fio base — CockroachDB, YugabyteDB, e Greenplum usam o fio PostgreSQL; TiDB usa o fio MySQL. Eles precisam apenas de entradas no registro, sem novo código de conector. [tool-verified: `provisa/core/source_registry.py` `_PG_WIRE_TYPES`, `_MYSQL_WIRE_TYPES`] (REQ-950)

`firebird` (Firebird 3/4/5) e `airport` (servidor Arrow Flight) são tipos de fonte registrados alcançados no local via extensões da comunidade DuckDB quando o DuckDB é o motor ativo — sem driver direto, sem conector federado. [tool-verified: `provisa/core/models.py` lines 44, 93] (REQ-899)

#### `saphana`

O SAP HANA é lido por seu próprio driver e replicado no armazenamento do motor ativo, então é alcançável em todo motor. O formulário pede host, porta (padrão 39015), nome de usuário, senha e banco de dados; em um sistema multi-tenant o banco de dados indica o tenant. Register Table lista os esquemas que o usuário pode ler, deixando de fora os esquemas de sistema do HANA. [tool-verified: `provisa/executor/drivers/registry.py` `_SQLALCHEMY_FALLBACK`; `provisa/api/admin/introspect.py` `_SAPHANA_SYSTEM_SCHEMAS`] (REQ-1753)

#### `hiveserver2`

Um endpoint HiveServer2 lido diretamente por Thrift, para uma implantação Hive em que o endpoint é a única coisa alcançável. Ele é distinto de `hive` e `hive_s3`, que leem os arquivos e o metastore do warehouse e nunca passam pelo HiveServer2. As linhas são lidas pelo endpoint e replicadas no armazenamento do motor ativo. O formulário pede host, porta (padrão 10000), nome de usuário, senha e banco de dados; `federation_hints.auth_mechanism` seleciona a autenticação (padrão `PLAIN`, por exemplo `GSSAPI` para Kerberos). [tool-verified: `provisa/executor/drivers/hive.py` `HiveDriver`; `provisa/core/models.py` `SourceType.hiveserver2`] (REQ-1731)

### Data Warehouses em Nuvem {: #cloud-data-warehouses }

[tool-verified: `executor/drivers/snowflake.py`, `executor/drivers/databricks.py`, `executor/drivers/registry.py`]

| Tipo de Fonte | Driver Direto | Nome do Conector | Dialeto | Mutações | Notas |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `snowflake` | SnowflakeDriver | snowflake | snowflake | Federada | Lê via snowflake-connector-python; pousa réplica; `account`/`warehouse`/`role` em `federation_hints` (REQ-988) |
| `bigquery` | — | bigquery | bigquery | Federada | Sem DirectDriver; alcançado via motor de federação ou ATTACH do motor BigQuery |
| `databricks` | DatabricksDriver | delta_lake | databricks | Federada | Lê via databricks-sql-connector (Cloud Fetch, Arrow); pousa réplica; `http_path` exigido em `federation_hints` (REQ-987) |
| `redshift` | — | redshift | redshift | Federada | — |
| `athena` | AthenaDriver | — | athena | Parcial (INSERT sempre; UPDATE/DELETE/MERGE apenas em Iceberg) | Consulta serverless sobre S3; instruções de fonte única são executadas inteiramente no Athena; junções entre fontes aterram uma réplica; `region` obrigatória em `federation_hints` (REQ-1981) |
| `fabric` | MssqlWarehouseDriver | — | tsql | Federada | Microsoft Fabric Warehouse; T-SQL sobre TDS, autenticação Azure AD; pousa réplica (REQ-995) |
| `synapse` | MssqlWarehouseDriver | — | tsql | Federada | Azure Synapse SQL; T-SQL sobre TDS, autenticação Azure AD; pousa réplica (REQ-995) |
| `trino` | SQLAlchemyDriver | — | — | Federada | Coordenador Trino/Presto remoto lido via o dialeto SQLAlchemy trino; pousa réplica em qualquer motor (REQ-994) |

#### `athena`

Amazon Athena é um serviço de consulta sem servidor sobre dados no S3, cobrado por byte analisado. Uma instrução apenas sobre tabelas Athena é roteada diretamente para o Athena: filtros, listas de colunas e `LIMIT` são transmitidos integralmente, de modo que apenas os bytes necessários ao Athena para responder àquela instrução são analisados e cobrados. Uma instrução que une uma tabela Athena com outra fonte primeiro aterra uma réplica completa do Athena — toda a tabela é lida e o Athena a cobra — e então a junção é executada contra essa réplica. As configurações Replicate e proteção de carga da fonte regem a frequência dos aterramentos; a nota na documentação é a única regra. (REQ-1981)

**Descoberta.** Register Table chama a API de metadados do Athena — `ListDatabases`, `ListTableMetadata`, `GetTableMetadata`. Essas chamadas não executam nenhuma consulta, portanto listar bancos de dados e tabelas e ler as colunas de uma tabela não tem custo por byte. As colunas de uma tabela são lidas apenas quando essa tabela é selecionada. Chaves de partição seguem as colunas regulares. Os tipos são mapeados através da entrada `athena` em `provisa/core/ir_types.py`: `array<…>`, `map<…>` e `struct<…>` tornam-se `text`; um tipo desconhecido é recusado, nomeando-o. [tool-verified: `provisa/federation/athena.py` `databases`, `tables`, `columns`; `provisa/core/ir_types.py` entrada `"athena"`]

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

Ambos `username` e `password` vazios significa que a cadeia de credenciais do boto3 é usada: perfil de instância, variáveis de ambiente (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`) ou um perfil nomeado. Um sem o outro é recusado. [tool-verified: `provisa/executor/drivers/athena.py` `AthenaDriver.connect`; `provisa/federation/athena.py` `athena_client`]

**Gravações.** Cada tabela registrada aceita `INSERT`. `UPDATE`, `DELETE` e `MERGE` estão disponíveis apenas em uma tabela Iceberg; o formato é registrado no momento do registro (`registered_tables.table_format`). Uma gravação em uma tabela Hive que solicite `UPDATE`, `DELETE` ou `MERGE` é recusada, nomeando a tabela e a operação. O Athena não retorna as linhas gravadas — uma cláusula `RETURNING` é recusada e o GraphQL não oferece campos de mutação para essas tabelas; `INSERT`, `UPDATE`, `DELETE` e `COPY` são os caminhos de gravação. `INSERT … ON CONFLICT`, `UPDATE … FROM`, `DELETE … USING` e `TRUNCATE` são recusados, nomeando a forma. [tool-verified: `provisa/executor/write_capability.py` `table_write_ops`; `provisa/executor/writable.py` `_TYPE_RETURNS_NO_ROWS`, `_TYPE_REFUSED_FORMS`]

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

`motherduck` (REQ-1998) é um banco de dados DuckDB hospedado no MotherDuck. No motor DuckDB ele é anexado no lugar; no motor Postgres continua sendo o conector do foreign data wrapper `pg_duckdb` (REQ-1868). [tool-verified: `provisa/federation/connector_duckdb.py` `DuckDBMotherDuckConnector`, `PgDuckdbMotherDuckConnector`]

**Conexão.** `database` nomeia o banco de dados do MotherDuck, ou um compartilhamento pelo seu endereço de compartilhamento. `password` é o token do MotherDuck, resolvido como qualquer segredo de fonte. [tool-verified: `DuckDBMotherDuckConnector.details`]

```yaml
- id: reference
  type: motherduck
  database: analytics            # attached as md:analytics
  password: ${secret:<name>}     # the MotherDuck token, or a reference to it
```

O motor DuckDB emite `ATTACH 'md:<database>'`; para um compartilhamento, `database` é o endereço do compartilhamento (`_share/...`), então o anexo é `md:_share/...`. [tool-verified: `details` builds `"md:" + database`; `tests/integration/test_motherduck_duckdb_e2e.py` attaches MotherDuck's public sample-data share this way]

- **Tratamento do token.** O motor faz login com `SET GLOBAL motherduck_token = ?`, com o token passado como valor vinculado. Ele não está no texto de nenhuma instrução, então nenhum log, trace ou erro o contém. [tool-verified: `provisa/federation/duckdb_runtime.py` `_sign_in_motherduck`]
- **Sem token.** Uma fonte sem `password` é recusada pelo nome antes que qualquer coisa seja anexada (`a MotherDuck source signs in with its token (the password field); none is set`). Sem essa verificação, a extensão iniciaria um login interativo e esperaria cerca de um minuto. Uma fonte sem `database` também é recusada. [tool-verified: `details`]
- **Uma conta por motor.** Um motor faz login em uma conta do MotherDuck. Uma segunda fonte MotherDuck com um token diferente é recusada pelo nome: "one DuckDB engine reads one MotherDuck account". [tool-verified: `_sign_in_motherduck` compares a SHA-256 of the token]
- **Escritas** passam pelo motor, como no motor Postgres. [tool-verified: `DuckDBMotherDuckConnector.capability` `write=True`]

**A execução híbrida (dual)** não precisa de configuração. No motor DuckDB toda fonte é anexada em uma só conexão, então uma instrução que faz join de uma tabela de uma fonte de arquivo DuckDB com uma tabela do MotherDuck roda como uma única instrução DuckDB, e o MotherDuck decide onde cada parte roda. [tool-verified: `DuckDBMotherDuckConnector` docstring; REQ-1998 notes record a live join showing dual-execution operators]

> **Movimentação de dados.** Na execução dual, o MotherDuck pode mover linhas locais para o seu lado nas partes que executa. Uma instrução assim pode, portanto, enviar dados do arquivo local para a conta do MotherDuck da organização. Não faça join de um arquivo com dados que precisam ficar no nó com uma fonte MotherDuck, a menos que enviá-los para essa conta seja aceitável. [tool-verified: `DuckDBMotherDuckConnector` docstring: "which may move local rows to its side"]

Uma fonte MotherDuck é servida apenas pelos motores DuckDB e Postgres. Nenhum outro motor tem um conector para ela, e nada pousa suas tabelas como réplicas ali. [tool-verified: `provisa/federation/engine.py` registers `DuckDBMotherDuckConnector` and `PgDuckdbMotherDuckConnector` and no other MotherDuck connector]

### Analytics / OLAP

[tool-verified: `executor/drivers/clickhouse.py`]

| Tipo de Fonte | Driver Direto | Nome do Conector | Dialeto | Mutações | Notas |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `clickhouse` | ClickHouseDriver | clickhouse | clickhouse | Federada | Lê via clickhouse-connect (HTTP); `secure: "true"` em `federation_hints` para TLS (REQ-986) |
| `druid` | — | druid | druid | Não | — |
| `exasol` | — | exasol | exasol | Não | — |
| `elasticsearch` | HTTP (motores nativos) | elasticsearch (Trino) | — | Não | No Trino o conector o lê, suas propriedades vêm da DSL de mapeamento do tipo [tool-verified: `trino_connectors.py:309`]; em todo outro motor o Provisa lê o índice via HTTP (índices e mapeamento para Register Table, uma leitura por scroll para pouso) e pousa as linhas [tool-verified: `provisa/elasticsearch/fetch.py`, `provisa/events/source_loader.py` `make_elasticsearch_loader`] (REQ-1672) |
| `pinot` | — | pinot | — | Não | Conector Trino `pinot`; `pinot.controller-urls` = host:porta do controlador Pinot [tool-verified: `trino_connectors.py:199`] |

### Data Lake / Formatos de Tabela Aberta {: #data-lake-open-table-formats }

Esses tipos de fonte são somente federação — sem driver direto, sem dialeto. [tool-verified: `LAKE_ONLY_SOURCES` em `provisa/core/source_registry.py`] (REQ-229)

| Tipo de Fonte | Nome do Conector | Time Travel | Notas |
| ------------ | ----------------- | ------------- | ------- |
| `iceberg` | iceberg | Sim (argumento `as_of`, REQ-372) | — |
| `delta_lake` | delta_lake | Sim (argumento `as_of`, REQ-372) | — |
| `hive` | hive | Não | — |
| `hudi` | — (motor `Hudi` do ClickHouse, cópia zero — REQ-1178) | Não | Sem conector federado; alcançado no local quando o ClickHouse é o motor ativo |
| `hive_s3` | hive | Não | Hive apoiado em S3 |

### NoSQL

`mongodb`, `cassandra`, e `redis` têm conectores Trino (`redis` constrói suas propriedades a partir da DSL de mapeamento do tipo). [tool-verified: `provisa/federation/trino_connectors.py`; `provisa/core/models.py`] (REQ-017, REQ-1097)

| Tipo de Fonte | Nome do Conector | Mutações |
| ------------ | ----------------- | ----------- |
| `mongodb` | mongodb | Não |
| `cassandra` | cassandra (Trino); leitura CQL via cassandra-driver em todo outro motor | Não | Keyspaces são esquemas; Register Table lista as tabelas de um keyspace e tipa as colunas a partir dos metadados de esquema do cluster (chaves de partição como chaves primárias); o extra `cassandra` instala o driver [tool-verified: `provisa/cassandra/fetch.py`] (REQ-1676) |
| `redis` | redis (Trino); leitura redis-py sem HTTP em todo outro motor | Não | Um prefixo de chave `<table>:*` é uma tabela e um hash é uma linha; Register Table lista os prefixos presentes e tipa as colunas de um prefixo a partir de seus hashes (uma entrada de `mapping.tables` sobrepõe padrão, coluna de chave, tipo de valor e colunas) [tool-verified: `provisa/redis/fetch.py`] (REQ-1675) |

**Redis nos motores hospedados.** O Redis não está disponível quando o motor da organização é um dos motores provisionados da plataforma hospedada. O Trino lê as tabelas de um catálogo Redis de arquivos de descrição num diretório próprio do catálogo, e não é possível dar um a um motor hospedado enquanto ele roda. Salvar ou aplicar ali uma fonte Redis é recusado pelo nome (HTTP 422, `sources.redis_needs_engine_files`); uma implantação auto-hospedada lê o Redis como descrito acima. [tool-verified: `provisa/core/repositories/source.py` `refuse_on_engine_shard`] (REQ-2073)

### Qdrant

`qdrant` (REQ-1983, REQ-2000) é lido e escrito pelo cliente próprio do Qdrant, `qdrant-client`. O Qdrant é **somente ao vivo**: uma propriedade do tipo de fonte, não uma configuração. Ele responde com seu índice vetorial e sua Query API, e nenhuma réplica pode responder do mesmo modo, então suas tabelas nunca são replicadas. Não há opção Replicate para uma fonte Qdrant nem para suas tabelas; um valor `replicate` ou `row_materialize` em uma delas é recusado. [tool-verified: `provisa/core/source_registry.py` `LIVE_ONLY_TYPES`; `provisa/api/admin/_landing_ttl.py` `_live_only_refusal`]

**Conexão.** `host` é a URL da instância com sua porta (`https://<cluster>.cloud.qdrant.io:6333`, ou um `http://qdrant:6333` auto-hospedado); `password` é sua chave de API, uma referência a um segredo resolvida quando a fonte é lida, vazia para uma instância sem chave. `federation_hints.prefer_grpc: true` lê pela porta gRPC do Qdrant em vez de REST. A fonte tem um esquema, `default`. [tool-verified: `provisa/qdrant/fetch.py` `QdrantConnection.from_source`; `provisa/qdrant/introspect.py` `SCHEMA`]

**Tabelas de coleção.** Uma coleção é uma tabela. Register Table oferece as colunas da coleção: `id` (o id do ponto, sua chave, caminho `@id`), os campos do payload (um campo indexado tipado por seu índice, qualquer outro como os pontos amostrados sugerem; campos aninhados achatados com seu caminho) e uma coluna `json` por vetor (caminho `@vector`, ou `@vector.<name>` para um vetor nomeado), oferecida sem seleção. Uma coluna de vetor nomeia o modelo de embedding registrado cujos embeddings ela contém, escolhido entre os modelos cujas dimensões são o tamanho do vetor (em Register Table e na aba Tipo e chaves do editor); escolher um modelo registra o vetor. [tool-verified: `provisa/qdrant/introspect.py` `columns`; `provisa-ui/src/pages/tables/QdrantVectorColumns.tsx`]

**Instruções sobre uma fonte Qdrant.** Uma instrução que lê apenas uma coleção é executada no Qdrant quando a API do Qdrant a expressa exatamente:

- as condições sobre o payload viram um filtro do Qdrant (`=`, `<>`, `IN`, intervalos sobre números e datas, `IS NULL`, `AND`, `OR`, `NOT`); a semântica de NULL do SQL é mantida, de modo que `lang <> 'en'` ignora pontos sem `lang`;
- uma condição apenas sobre `id` (`=` ou `IN`) é um `retrieve`; qualquer outra leitura é um `scroll` filtrado;
- `ORDER BY cosine_similarity(<vector>, <query>) DESC LIMIT k` é um `query_points` sobre um vetor cuja distância é Cosine, com o filtro aplicado antes do top-k. Um texto de consulta é convertido em embedding pelo modelo do vetor. A resposta traz um aviso de que o top-k é aproximado, escolhido pelo índice do Qdrant.

Tudo o mais (um agregado, um `GROUP BY`, uma coluna calculada ou mascarada, uma ordenação diferente da mais semelhante primeiro) é respondido pelo motor, como abaixo. [tool-verified: `provisa/qdrant/direct.py` `analyse`, `condition`, `QdrantDriver`]

**Instruções que o motor responde.** Quando uma instrução une uma coleção a outra fonte, ou calcula sobre suas colunas, a parte da coleção é lida ao vivo do Qdrant para essa instrução e entregue ao motor como linhas. A parte são as colunas da coleção, as condições da instrução sobre a coleção sozinha (incluindo suas regras de linha) e, quando a coleção é lida sozinha em seu `SELECT`, a ordenação por similaridade e o `LIMIT` desse `SELECT`. Uma parte que o Qdrant não consegue expressar exatamente é recusada, nomeando a tabela e o motivo. Uma parte sem condição, sem chave e sem top-k leria a coleção inteira e também é recusada. Uma condição sobre um parâmetro (`$1`) fica a cargo do motor. [tool-verified: `provisa/federation/live_only.py` `cut`, `read`; `provisa/pgwire/_pipeline.py` `_read_live_parts`]

**Gravações.** `INSERT`, `UPDATE` e `DELETE` em uma coleção são executados no Qdrant:

- `INSERT … VALUES` nomeia a coluna `id` e grava pontos novos. Um `id` que a coleção já contém é recusado pelo nome. A verificação lê os ids primeiro, portanto não é atômica com a gravação.
- `INSERT … ON CONFLICT (id) DO UPDATE SET c = EXCLUDED.c, …` é um upsert. As colunas que a instrução não define mantêm seus valores.
- `UPDATE` e `DELETE` atuam sobre os pontos que seu `WHERE` seleciona, como filtro do Qdrant; uma condição sem forma de filtro é recusada.
- Um texto gravado em uma coluna de vetor é convertido em embedding pelo modelo da coluna. Um vetor gravado nela deve ter as dimensões do modelo.

[tool-verified: `provisa/qdrant/direct.py` `_insert`, `_upsert_clause`, `_write_points`, `_update`, `_delete`, `_vector_value`]

**Tabelas de busca.** A busca vetorial é uma tabela de busca: seu `query_template` contém uma requisição da Query API.

```json
{"collection": "docs", "query": "@query_vector", "using": "text",
 "filter": {"must": [{"key": "lang", "match": {"value": "@lang"}}]},
 "limit": "@k", "score_threshold": 0.5, "model": "text-embedding-3-small"}
```

- `query` é sempre um argumento; `using`, `filter`, `limit`, `score_threshold` e `model` são opcionais. Qualquer valor em `using`, `filter`, `limit` ou `score_threshold` que seja exatamente `"@name"` também é um argumento.
- Cada argumento é um parâmetro da tabela (`query_arg`, REQ-1986), convertido estritamente para seu tipo declarado e colocado na requisição como valor JSON, nunca em seu texto. O argumento de `query` é `array(double)` e obrigatório.
- Quem chama passa os argumentos como qualquer parâmetro: `SELECT id, score, title FROM docs_search WHERE _nf_query_vector = '[0.1, 0.2, …]' AND _nf_lang = 'en'` em SQL, argumentos de campo em GraphQL.
- Um argumento opcional não informado não filtra nada: sua condição é omitida do filtro. Sem `limit`, uma busca devolve o padrão do Qdrant, 10 pontos.
- O filtro é aplicado pelo Qdrant antes de escolher seus pontos mais próximos, portanto é exato. Qualquer outra condição da instrução restringe depois os pontos que a busca devolveu; não busca mais.
- `score` é a métrica própria da coleção para o vetor buscado: para uma coleção Cosine, a similaridade de cosseno. Register Table lista os vetores da coleção com tamanho e distância.
- `model` nomeia um modelo de embedding registrado (`vector_models`, admin → AI Models). Um texto dado para a consulta é então incorporado por esse modelo, e um vetor dado deve ter as dimensões desse modelo (REQ-429, REQ-430). Sem `model`, a consulta deve ser um vetor.

Uma busca é lida por instrução sob seus argumentos, suas linhas mantidas sob esses argumentos durante o TTL da tabela. Uma tabela de busca é somente leitura. [tool-verified: `provisa/qdrant/table.py` `check_table`, `plan`, `read`; `provisa/api/data/materialization.py` `_is_native_read`]

### Streaming

| Tipo de Fonte | Mecanismo | Mutações |
| ------------ | ----------- | ----------- |
| `kafka` | Conector Kafka federado; esquema via Confluent Schema Registry (Avro, Protobuf, JSON Schema), definição manual, ou inferência de amostra (REQ-147, REQ-150) | Não; a saída ao vivo de uma tabela publica no Kafka (REQ-565) |
| `websocket` | Feed WebSocket externo — conecta, subscreve, recebe eventos; resultados materializados (REQ-338) | Não |
| `rss` | Feed RSS 2.0 / Atom — polling, marca d'água por pubDate/updated; resultados materializados (REQ-342, REQ-343) | Não |

**Kafka nos motores hospedados.** Nos motores provisionados da plataforma hospedada, uma fonte Kafka deve nomear um registro de esquemas (`schema_registry_url`). Uma fonte cujas colunas dos tópicos só o modelo declara é recusada pelo nome ao ser salva ou aplicada (HTTP 422, `sources.kafka_needs_schema_registry`), pois suas descrições de tabela exigiriam um diretório que não pode ser dado ao motor em execução. As credenciais SASL chegam a esses motores em memória (veja [Segurança](security.md#the-federation-engine-trino)). [tool-verified: `provisa/core/repositories/kafka_source.py` `refuse_on_engine_shard`] (REQ-2073)

### Receptor Push {: #push-receiver }

| Tipo de Fonte | Mecanismo | Mutações |
| ------------ | ----------- | ----------- |
| `ingest` | Serviços externos fazem POST de eventos JSON; resultados materializados (REQ-331, REQ-335) | Não |

### Grafo e Semântico {: #graph-semantic }

| Tipo de Fonte | Mecanismo | Mutações |
| ------------ | ----------- | ----------- |
| `neo4j` | Cypher via API HTTP, resultados em cache no PostgreSQL (REQ-295) | Não |
| `sparql` | SPARQL 1.1 POST, resultados em cache no PostgreSQL (REQ-297) | Não |

### Baseado em Arquivo {: #file-based }

Três mecanismos cobrem arquivos. As fontes de arquivo único e o rastreador de arquivos usam o campo `path` em vez de `host`/`port`. [tool-verified: `provisa/core/models.py`] (REQ-553) O terceiro, [Logs](#logs), indica seus arquivos por um glob em seu `mapping` e lê apenas arquivos que estão no servidor Provisa. [tool-verified: `provisa/logs/settings.py`]

**Fontes de banco de dados em arquivo e de arquivo único** — `sqlite`, `duckdb`, `csv`, `parquet` apontam `path` para um arquivo. (Uma fonte `duckdb` guarda sua localização no campo `database` do formulário.) [tool-verified: `provisa-ui/src/pages/sources/FileLocationFields.tsx`]

| Tipo de Fonte | Locais | Mutações |
| --- | --- | --- |
| `sqlite` | caminho local, `https://`, `s3://`, `gs://`, `az://` | Sim em um arquivo local; nenhuma em um remoto |
| `duckdb` | caminho local, `https://`, `s3://`, `gs://`, `az://` | Sim em um arquivo local; nenhuma em um remoto |
| `csv` | caminho local, `https://`, `s3://`, `gs://`, `az://` (motor DuckDB) | Não |
| `parquet` | caminho local, `https://`, `s3://`, `gs://`, `az://` (motor DuckDB) | Não |

[tool-verified: `provisa/federation/connector_duckdb.py` `location_kind`, `file_location_details`; `provisa/executor/write_capability.py` `table_write_ops`. The table describes the DuckDB engine, where the connectors above take every location. On other engines, CSV and Parquet over `s3://` or `http(s)://` are better registered through the `files` source. [inferred]] (REQ-1997)

Um caminho com qualquer outro esquema (`ftp://`, por exemplo) é recusado: "DuckDB reads no file at a `ftp://` location". [tool-verified: `location_kind` raises `ValueError`]

**Credenciais para um local remoto.** Coloque-as nos `federation_hints` da fonte. Cada uma vale apenas para o bucket ou contêiner do próprio caminho, então as credenciais de uma fonte nunca são usadas para o local de outra fonte. Uma fonte que não fornece nenhuma é lida como um local público. [tool-verified: `object_store_secret_ddl`]

| Local | Chaves de `federation_hints` |
| --- | --- |
| `s3://` | `access_key_id`, `secret_access_key`; opcionais `region`, `endpoint` (um armazenamento compatível com S3 como R2 ou MinIO) |
| `gs://` | `gcs_access_id`, `gcs_secret_key` (uma chave HMAC) |
| `az://` | `azure_account_name`, `azure_account_key` |

**Arquivos SQLite e DuckDB remotos são somente leitura.** O motor DuckDB os anexa no lugar com `READ_ONLY`, e uma tabela sobre um deles não oferece insert, update ou delete. Em um motor que não anexa SQLite, um arquivo SQLite remoto é lido por uma conexão DuckDB temporária que o anexa como somente leitura, e suas linhas pousam como de costume. [tool-verified: `_attach_options`; `table_write_ops`; `provisa/events/source_loader.py` `make_sqlite_loader` `_remote`]

**Enviando um arquivo.** No formulário de uma fonte `sqlite`, `duckdb`, `csv` ou `parquet`, envie o arquivo em vez de digitar um caminho. O arquivo é gravado em disco por streaming, nunca mantido em memória, na pasta da organização em `files/<source id>/<file name>`, e o caminho da fonte passa a apontar para ele. [tool-verified: `provisa/core/source_uploads.py` `receive`, `upload_dir`; `provisa/api/admin/source_files_router.py` `upload_file`]

- **Substituir.** Enviar de novo toma o lugar do arquivo anterior, qualquer que seja o nome, e a fonte é provisionada de novo para que a próxima leitura veja o arquivo novo. [tool-verified: `receive` removes other files in the directory; `upload_file` calls `reprovision_source_after_registration`]
- **Remover.** Remover a fonte remove o arquivo enviado. Nada mais o remove. [tool-verified: `provisa/api/admin/schema_mutation.py` `source_uploads.remove(id)`]
- **Quem.** Somente papéis com a capacidade `source_registration` podem enviar. [tool-verified: `upload_file` `require_capability_request(request, "source_registration")`]
- **Limite de tamanho.** A configuração `sources.max_upload_bytes` (ambiente `PROVISA_SOURCES_MAX_UPLOAD_BYTES`) limita o arquivo, 1 GiB (1073741824 bytes) por padrão, mínimo 1, lida quando um envio começa. Um arquivo maior é recusado com HTTP 413 indicando o limite, e nada dele é mantido. [tool-verified: `provisa/core/settings_catalog.py`; `UploadTooLarge`]
- **Caminho.** Enquanto o caminho for o arquivo enviado da fonte, o campo de caminho do formulário é somente leitura. [tool-verified: `FileLocationFields.tsx` `readOnly={uploaded}`]
- **Nomes.** O id da fonte e o nome do arquivo só podem usar letras, dígitos, `.`, `_` e `-`. [tool-verified: `source_uploads._SEGMENT`]
- **Armazenamento.** [inferred from the `source_uploads` docstring] Um arquivo enviado é armazenado como qualquer arquivo da organização: na pasta da organização, criptografado quando essa pasta é, e visto por todos os nós em um mount compartilhado.

**Escritas em um arquivo SQLite ou DuckDB local, e bloqueios.** Uma escrita pega o bloqueio do arquivo e nunca espera por ele. Quando outro processo ou nó detém o arquivo, a escrita é recusada, indicando a fonte e que seu arquivo está bloqueado. [tool-verified: `provisa/federation/file_locks.py` `FileLocked`; REQ-1997 notes quote the maintainer: "writes back to file databases in a multi-node env - just error them out when they can't a lock on it."]

- **SQLite.** A escrita define `PRAGMA busy_timeout = 0`, então um arquivo bloqueado falha na hora. [tool-verified: `provisa/executor/drivers/sqlalchemy_driver.py` `_write_sqlite`]
- **DuckDB.** O DuckDB compartilha um arquivo apenas entre aberturas somente leitura. Por isso o anexo do motor e a conexão própria da fonte mantêm o arquivo como somente leitura, e todos os nós o leem ao mesmo tempo. Uma escrita pausa os detentores deste processo, abre o arquivo para leitura e escrita apenas para aquela instrução e o fecha. Se a abertura não conseguir pegar o bloqueio, a escrita é recusada. [tool-verified: `provisa/executor/drivers/duckdb_driver.py` `_write`: `with file_locks.exclusive(self._database):` then `duckdb.connect(self._database)`; `connector_duckdb._attach_options`]

**Mounts compartilhados.** Em uma pasta de organização criptografada cujo mount é compartilhado por vários nós (configuração `org_files.shared_mount`), uma escrita em um arquivo SQLite ou DuckDB nela é recusada de imediato, indicando a fonte. A camada de criptografia de pastas (gocryptfs) não leva os bloqueios de arquivo aos outros nós, então nada impediria dois nós de escreverem ao mesmo tempo. Leituras continuam funcionando. [tool-verified: `file_locks.refuse_unprotected`, `UnprotectedWrite`; both drivers call it before a write]

Uma pasta compartilhada sem criptografia deve ser montada com bloqueios que alcancem todos os nós: um mount NFS com bloqueio ativado, não `nolock` e não `local_lock`. Nenhuma verificação confere isso; é um requisito de implantação. [tool-verified: REQ-1997 description in `docs/arch/requirements.yaml`; no check exists in source [inferred]]

**Crawler de arquivos (fonte `files`)** — aponta `path` para um glob, o percorre recursivamente, e registra o diretório como um catálogo federado de tabelas. Ele lê muitos formatos por muitos transportes; os conjuntos abaixo vêm do conector de arquivo (fork kenstott/calcite). [tool-verified: `provisa/core/catalog.py` branch `files` e `provisa/core/models.py` `SOURCE_TO_CONNECTOR`; listas de formato e transporte do adaptador calcite `file` — `FileSchema.java`, `storage/StorageProviderFactory.java`]

| Formatos | Transportes |
| --- | --- |
| CSV, TSV, JSON, YAML, Excel (XLS/XLSX), Parquet, Arrow, e documentos convertidos em tabelas — HTML, Markdown, DOCX, PPTX | Sistema de arquivos local, HTTP(S), `s3://`, `hdfs://`, `ftp://`/`ftps://`, `sftp://`, `iceberg://`, SharePoint (REST e Microsoft Graph) |

```yaml
- id: sales_files
  type: files
  path: s3://bucket/sales/**/*.csv   # glob; local and http(s):// also supported
```

Qualquer fonte `files` também pode declarar um `crawl` em seu mapping, com as mesmas configurações listadas em [Wikipedia](#wikipedia): a fonte então lê páginas web primeiro e pousa cada tabela de dados HTML e cada arquivo de dados vinculado em seu diretório como uma tabela. [tool-verified: `provisa/file_source/crawl.py` `crawl_operand`]

No motor DuckDB, `files` é lido nativamente — uma view scanner `read_csv_auto` por `<table>.csv` sob o diretório resolvido (REQ-229) [tool-verified: `provisa/federation/connector_duckdb.py` `DuckDBFilesConnector`]. Em um motor sem conector `files` próprio, as linhas pousam através do mesmo servidor pgwire Calcite empacotado pelo conector (`pgwire-file`) que sharepoint/splunk usam (REQ-954) — veja [Conectores SaaS Empresariais](#enterprise-saas-connectors) abaixo. A cobertura end-to-end da UI (formulário de Fontes → Register Table → consulta SQL) e o caminho de pouso pgwire são comprovados na REQ-1694.

#### Logs (REQ-1996) {: #logs }

Uma fonte `logs` transforma arquivos de log do servidor Provisa em uma tabela. Você indica os arquivos por um caminho completo ou por um glob, e escolhe o formato em que seus registros são escritos. Cada registro vira uma linha. Um registro que o formato não consegue ler é mantido, com seu texto bruto e o motivo, nunca descartado. [tool-verified: `provisa/logs/__init__.py`; `provisa/logs/files.py` `rows_of`]

```yaml
sources:
  - id: mac-install-log
    type: logs
    mapping:
      glob: /var/log/install.log*   # the live file and its rotated, compressed copies
      format: macos_install
```

O mapping aceita quatro chaves e recusa qualquer outra pelo nome: `glob` e `format` (ambas obrigatórias), e `pattern` e `record_start`, que só o formato `regex` aceita. [tool-verified: `provisa/logs/settings.py` `parse`; `provisa/logparse/__init__.py` `format_for`]

```yaml
    mapping:
      glob: /var/log/myapp/*.log
      format: regex
      pattern: '(?P<ts>\S+ \S+) (?P<level>[A-Z]+) (?P<message>.*)'
      record_start: '\d{4}-\d{2}-\d{2} '   # a stack trace joins the line it follows
```

No formulário de Fontes, uma fonte Logs aparece como "Log files" em File. O botão **Preview** mostra os arquivos que o glob encontra, as colunas que o formato dá e as 20 primeiras linhas do arquivo mais recente, antes de qualquer coisa ser salva. [tool-verified: `provisa-ui/src/pages/sources/constants.ts`; `provisa-ui/src/pages/sources/LogsFields.tsx`; `provisa/api/admin/logs_router.py` `PREVIEW_ROWS`]

**O glob.** Deve ser um caminho completo; `~` é expandido e um caminho relativo é recusado ("name the files by their full path"). A correspondência usa o `glob` do Python em modo não recursivo, então `**` se comporta como `*` e corresponde dentro de um único nível de diretório. Só arquivos regulares são tomados, em ordem de nome. Um arquivo vazio é ignorado até ter bytes. [tool-verified: `provisa/logs/roots.py` `checked_glob`; `provisa/logs/files.py` `matched`, `identify`] O glob pode nomear uma variável de ambiente, como em `${env:LOG_DIR}/*.log`. A fonte guarda a referência como foi escrita e a resolve a cada leitura dos arquivos; uma variável não definida é recusada pelo nome. [tool-verified: `provisa/logs/settings.py` `parse`; `tests/unit/test_logs_source.py` `test_a_glob_naming_a_variable_reads_where_it_points`]

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

`_file_id`, `_line_offset` e `_entry` são a chave da linha, então um registro lido duas vezes é uma só linha. [tool-verified: `provisa/logs/__init__.py` `META_COLUMNS`, `KEY`; `provisa/logs/preview.py` `columns`]

**Formatos e presets.** A seleção de formato os agrupa por família; a última opção, `regex`, é o seu próprio padrão. [tool-verified: `provisa/logparse/__init__.py` `BUILT_IN`, `catalog`; `provisa/logparse/presets/__init__.py` `presets`; family labels from `provisa-ui/src/i18n/locales/en/logsFields.json`]

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

São 56 formatos embutidos e `regex`. [tool-verified: `BUILT_IN` enumerated with the project interpreter]

**Como um formato produz colunas.** Os formatos obtêm suas colunas de uma de quatro maneiras. [tool-verified: `provisa/logparse/formats.py`; `provisa/logparse/presets/_shared.py`; `provisa/logparse/coltypes.py`]

- **Colunas fixas.** A maioria dos formatos declara suas colunas e tipos. `rfc3164`, por exemplo, dá `timestamp`, `facility`, `severity`, `host`, `process`, `pid` e `message`; `facility` e `severity` são calculadas a partir do valor `<PRI>`. Uma linha de syslog BSD não traz o ano, então o ano é o da última escrita do arquivo, ou o ano anterior quando o mês da linha é posterior ao da escrita.
- **Campos nomeados pelo registro.** `jsonl` e `logfmt` tiram suas colunas dos campos vistos nas primeiras linhas dos arquivos (configuração `logs.preview_lines`, padrão 1000). Cada coluna recebe o tipo mais estreito em que cabem todos os valores vistos: boolean, depois integer, float, timestamp e json, senão text. Um campo que um registro posterior traz e que nenhuma coluna tem vai para uma coluna json `extra`.
- **Campos nomeados por um cabeçalho.** `w3c` e os presets construídos sobre ele leem a linha `#Fields:` de cada arquivo; `date` e `time` se juntam em uma única coluna `timestamp`. `aws_cloudfront`, `ms_iis`, `ms_windows_firewall`, `ms_exchange_tracking` e `zeek_tsv` leem do mesmo modo as linhas de cabeçalho que começam com `#`, e `aws_vpc_flow` lê uma linha de cabeçalho que começa com `version`. O ponto em que uma linha de cabeçalho foi lida fica com o arquivo, então um arquivo lido mais adiante mantém seus campos.
- **`regex`.** Cada grupo nomeado, como `(?P<level>…)`, é uma coluna do tipo text.

A tela Register Table lista essas colunas com seus tipos e marca a chave. [tool-verified: `provisa/api/admin/schema_query.py` `_logs_columns`] Quando as linhas são lidas, cada valor é convertido para o tipo que sua coluna tem na tabela registrada, então um tipo alterado ali é o aplicado. Um valor que a coluna não comporta vira NULL e seu motivo vai para `_parse_error`. [tool-verified: `provisa/logs/loader.py` `_load`; `provisa/logs/files.py` `_typed`] [inferred: that the screen lets a steward change a suggested type, as REQ-1996 states]

Um registro maior que `logs.max_line_bytes` (padrão 65536) guarda seus primeiros bytes em `_raw` e não é analisado; `_parse_error` informa seu comprimento. Um arquivo que começa com uma marca de ordem de bytes (BOM) UTF-8 ou UTF-16 é lido nessa codificação, e qualquer outro arquivo como UTF-8. [tool-verified: `provisa/core/settings_catalog.py`; `provisa/logs/files.py` `rows_of`, `text_encoding`]

**Registros que abrangem várias linhas.** Um formato com um padrão de início de registro forma um registro de uma linha que o satisfaz mais todas as linhas seguintes que não o satisfazem. Um stack trace fica com sua exceção, e uma entrada de consulta lenta fica inteira. As linhas antes da primeira correspondência são um registro à parte; em geral o formato o recusa, e a linha é mantida. Os formatos embutidos com início de registro são `rfc3164`, `macos_install`, `aws_cloudtrail`, `postgresql_stderr`, `postgresql_csvlog`, `mysql_error`, `mysql_slow`, `log4j2_default`, `logback_default`, `python_logging`, `chrome_net_export`, `ms_sqlserver_errorlog` e `azure_monitor`. Com `regex`, defina `record_start` você mesmo. [tool-verified: `provisa/logparse/framing.py` `records`; `record_start` of each format enumerated with the project interpreter]

Dois formatos leem seus registros por conta própria em vez de por linha. `kubernetes_cri` junta em um registro as linhas parciais (`P`) que o runtime dividiu, e `windows_evtx` lê o arquivo binário `.evtx`. [tool-verified: `provisa/logparse/presets/linux.py` `KubernetesCri`; `frames_bytes` enumerated with the project interpreter]

Um arquivo ainda em escrita é lido até sua última linha completa. Para um formato de várias linhas, é lido até o início do seu último registro, que a linha seguinte pode continuar. Quando o arquivo passa 2 segundos sem escrita, esse último registro é tomado como inteiro. [tool-verified: `provisa/logs/files.py` `QUIET_SECONDS`, `read_file`]

**Rotação.** Um arquivo é reconhecido por um checksum dos seus primeiros 4096 bytes, descomprimidos, não pelo nome. Quando `system.log` é rotacionado para `system.log.0` e depois comprimido para `system.log.0.gz`, toda cópia é o mesmo arquivo. A leitura continua do byte que a última leitura alcançou, então nenhuma linha é lida duas vezes. [tool-verified: `provisa/logs/files.py` module docstring, `identify`; `tests/unit/test_logs_source.py` `test_a_rotated_and_compressed_file_is_read_on_and_no_line_twice`]

- A compressão é reconhecida pelos primeiros bytes do arquivo, nunca pelo nome: gzip e bzip2. Um arquivo comprimido é tratado como completo.
- Uma cópia rotacionada e sua gêmea comprimida, ambas encontradas, são lidas uma vez, a partir da comprimida.
- Um arquivo encontrado menor que o deslocamento já lido foi truncado: é um arquivo novo, lido desde o início. As linhas do antigo permanecem.
- Arquivos menores que 4096 bytes que começam igual são distinguidos por inode, então continuam sendo dois arquivos.
- Um arquivo que o glob não encontra mais é esquecido; suas linhas permanecem como histórico.

[tool-verified: `provisa/logs/files.py` `compression`, `Found.complete`, `identify`; `provisa/logs/loader.py` module docstring; `tests/unit/test_logs_source.py`]

**Atualização e entrega ao vivo.** Uma tabela Logs é uma réplica. Nada observa os arquivos. Em vez disso, quando o sinal de mudança da fonte é `probe` ou `ttl_probe`, sua sondagem calcula um digest do caminho, inode, tamanho e última escrita de cada arquivo encontrado; quando o digest difere, uma atualização é pedida. Os tipos de sondagem que uma fonte Logs aceita são `hash` e `none`. [tool-verified: `provisa/logs/loader.py` `files_token`, `_freshness_probe`; `provisa/events/probes.py` `_LOADER_PROBED`, `probe_capabilities`]

Uma atualização lê apenas o que é novo. Cada arquivo é lido de onde a última passagem chegou, e as linhas são escritas pela chave, 5000 por vez, antes de a nova posição ser guardada. Uma passagem que para entre os dois passos relê esses registros, o que deixa as mesmas linhas. Cada passagem carimba suas linhas em `_landed_at`, a coluna que a entrega ao vivo usa como watermark. Uma definição de tabela alterada é uma reconstrução completa que lê cada arquivo desde o início. [tool-verified: `provisa/logs/loader.py` `_UnitsRead`, `_WholeRead`, `BATCH_ROWS`, `replica_stamp_column`; `provisa/federation/replica_builds.py` `_kept_in_place`] [inferred: that subscriptions deliver a Logs table's new rows by `_landed_at`, as REQ-1996 states; the subscription side was not traced]

**Raízes permitidas.** A configuração de operador `logs.allowed_roots` lista os diretórios em que um glob pode se resolver. O padrão é `["/var/log"]`; é uma lista no cartão Security de Admin → Settings, também definível por `PROVISA_LOGS_ALLOWED_ROOTS`, e uma alteração pede confirmação. Cada raiz é resolvida após a expansão de `~` e dos symlinks. [tool-verified: `provisa/core/settings_catalog.py` `logs.allowed_roots`; `provisa/logs/roots.py` `allowed_roots`]

Aplicam-se duas verificações. A parte fixa do glob, até o primeiro curinga, deve se resolver dentro de uma raiz; isso é verificado quando a fonte é salva e a cada pré-visualização ou leitura. Cada arquivo encontrado também deve se resolver dentro de uma raiz depois de seguidos os symlinks, então um symlink que sai de `/var/log` é recusado. A recusa informa o caminho, o que ele resolveu e as raízes permitidas:

```text
/home/me/app.log resolves to /home/me/app.log, outside the directories log files may be read from: /var/log
```

Ao salvar, o código é `schema.logs_path_refused`; na pré-visualização é um 403 com `logs.path_refused`. [tool-verified: `provisa/logs/roots.py` `checked_glob`, `checked_file`; `provisa/api/admin/schema_common.py` `logs_refusal`; `provisa/api/admin/logs_router.py`; `tests/unit/test_logs_source.py` `test_a_symlink_escaping_the_root_is_refused_by_name`]

**Recusa em ambiente hospedado (SaaS).** Uma implantação que abriga muitas organizações não lê nenhum arquivo de log próprio. Toda verificação de caminho recusa primeiro, ao salvar, na pré-visualização e na leitura, com "`<path>`: a server that holds many organizations reads no log file of its own". Os arquivos são do servidor, não de uma organização em particular, então nenhum tenant pode lê-los. A implantação sabe se abriga muitas organizações pelo seu estado de multitenancy. [tool-verified: `provisa/logs/roots.py` `_refuse_on_saas`; `provisa/core/secrets_providers.py` `many_organisations`; `provisa/api/app.py` `set_many_organisations_resolver(lambda: state.multitenancy)`; `tests/unit/test_logs_source.py` `test_a_server_of_many_organizations_reads_no_log_file_of_its_own`]

#### Conjuntos de dados Kaggle (REQ-1780, REQ-1781, REQ-1782, REQ-1783) {: #kaggle-datasets }

O Kaggle é uma plataforma de download de arquivos. Um conjunto de dados Kaggle preparado se registra como uma Fonte do tipo `files` e é consultado através do mesmo conector pgwire-file que qualquer outra fonte `files` usa — não há um `SourceType` `kaggle` no enum. [tool-verified: `provisa/kaggle/downloader.py`; `provisa/core/models.py` `SourceType` — sem literal `kaggle`]

**Adicionando um conjunto de dados.** Abra Fontes → Subscriptions → Kaggle. O formulário executa duas etapas sequenciais, porque o próprio endpoint de busca de conjuntos de dados do Kaggle exige autenticação — o seletor não pode aparecer antes que o token seja validado. [tool-verified: `provisa-ui/src/pages/sources/KaggleFormSection.tsx`] (REQ-1783)

1. **Token** — insira um token de API do Kaggle e clique em "Validate". A validação chama `POST https://www.kaggle.com/api/v1/datasets/create/new` com um corpo vazio. Um `401` significa inválido; qualquer outra resposta significa válido (a própria validação de payload do Kaggle dispara antes que qualquer conjunto de dados seja criado — nada é persistido). [tool-verified: `provisa/kaggle/client.py` `validate_token`] (REQ-1782)
2. **Seletor** — um campo de busca ao vivo conforme você digita consulta `GET /api/v1/datasets/list`. Cada resultado mostra o título e a descrição do conjunto de dados. Selecione um, então clique em "Add Dataset".

Clicar em "Add Dataset" baixa o pacote de `GET /api/v1/datasets/download/{owner}/{ref}`, descompacta os membros CSV e Parquet em `<PROVISA_DATA_DIR>/kaggle/<owner>/<ref>/<file-stem>/<file-name>`, e cria **uma** Fonte do tipo `files` cujo `path` é esse diretório raiz. O token nunca é armazenado no lado do servidor. [tool-verified: `provisa/kaggle/downloader.py` `stage_dataset`; `provisa-ui/src/pages/sources/KaggleFormSection.tsx` `handleConfirmDataset`] (REQ-1780, REQ-1781)

Depois de adicionar a fonte, registre suas tabelas através da tela normal de Register Table. A descoberta recursiva de diretório do conector pgwire-file lista cada arquivo como sua própria tabela — o mesmo mecanismo que qualquer outra fonte `files` usa (REQ-1690). (REQ-1783)

**Limitação da v1.** Um pacote contendo um arquivo `.sqlite` ou `.db` é rejeitado imediatamente com um erro claro. Somente arquivos CSV e Parquet são preparados. [tool-verified: `provisa/kaggle/downloader.py` `UnsupportedKaggleDataset`, `_UNSUPPORTED_EXTENSIONS`]

**Nomenclatura de tabela.** Uma tabela rastreada se chama `<page>__<name>`: o título da página, depois a legenda da tabela, ou o último cabeçalho acima da tabela quando ela não tem legenda, ambos na grafia de nomes de coluna da fonte. Por exemplo, a tabela sob o cabeçalho "Tallest buildings in the world" na página `List_of_tallest_buildings` é `list_of_tallest_buildings__tallest_buildings_in_the_world`. Um arquivo de dados vinculado mantém seu próprio nome. [tool-verified: a live crawl of that page through the 0.109.0 file bundle]

**Atualizando.** Para buscar novamente um conjunto de dados depois que o Kaggle publica uma nova versão, chame a mutação GraphQL `refreshKaggleSource` com o ID da fonte e um token válido. Isso prepara novamente os arquivos no lugar e esvazia o cache do endpoint pgwire-file, para que o conector capte quaisquer mudanças de esquema na próxima consulta. `kaggle_owner` e `kaggle_ref`, armazenados em `federation_hints` no momento da criação, identificam qual conjunto de dados buscar novamente. [tool-verified: `provisa/api/admin/schema_mutation.py` `refresh_kaggle_source`] (REQ-1780)

**Sem caminho de config YAML estático.** Fontes Kaggle são criadas apenas através do formulário de Fontes. Uma fonte Kaggle exportada para YAML aparece como `type: files` com `kaggle_owner` e `kaggle_ref` em `federation_hints`. Baixar novamente do Kaggle exige o fluxo de atualização da UI ou a mutação `refreshKaggleSource` — apontar o `path` do YAML para um diretório já preparado é a alternativa para ambientes air-gapped.


#### Wikipedia (REQ-1960) {: #wikipedia }

A Wikipedia percorre as páginas que você indicar. Cada tabela de dados de uma página, e cada arquivo CSV, TSV, Excel, JSON ou Parquet vinculado, vira uma tabela da fonte. Como o Kaggle, ela se registra como fonte `files` com um `crawl` em seu mapping. Adicioná-la não registra nada: um steward registra as tabelas desejadas pela tela normal de Register Table. [tool-verified: `provisa/file_source/wikipedia.py` `crawl_settings`, `DATA_FILE_EXTENSIONS`; `provisa-ui/src/pages/sources/constants.ts` `wikipedia: "files"`] (REQ-316)

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

[tool-verified: `provisa-ui/src/pages/sources/WikipediaFields.tsx`; `provisa-ui/src/i18n/locales/en/sourceFormFieldsExtended.json` `wp*`; `provisa/file_source/wikipedia.py` `DEFAULTS`; `provisa/file_source/crawl.py` `crawl_landing_directory`]

O Provisa conhece o cabeçalho "See also" destas edições: `en`, `de`, `es`, `it`, `pt`, `nl`, `pl`, `sv`, `ja`, `zh`, `ru`. Para qualquer outra edição com "See also" desligado, o formulário pergunta como a edição chama essa seção; digite o título como uma página o mostra (por exemplo `Voir aussi`). Uma fonte salva sem ele é recusada (`wikipedia.see_also_heading_needed`). [tool-verified: `provisa/file_source/wikipedia.py` `SEE_ALSO_HEADING`, `crawl_settings`]

**Quais links são seguidos.** Um link é escolhido pelo que a própria marcação da página diz que ele é, nunca por um nome em seu endereço: tais nomes diferem em cada edição de idioma e também aparecem em títulos comuns. [tool-verified: `provisa/file_source/wikipedia.py` module docstring]

- Seguidos: links para outros artigos que ficam no texto, nas listas e nas tabelas de dados do próprio artigo. A marca de marcação é `a[rel='mw:WikiLink']`.
- Não seguidos: páginas ainda não escritas, a própria página, buscas de livros, páginas de arquivos, outras wikis e sites externos.
- Não lidos de forma alguma (removidos antes de coletar links e tabelas): caixas de navegação, caixas de informação, notas e citações, categorias, o índice, imagens e mensagens de manutenção.
- Não lida a menos que você ative a opção: a seção "See also".

Apenas tabelas que os editores marcam como tabelas de dados (`table.wikitable`) viram tabelas, e uma tabela com menos de 2 linhas não é oferecida. [tool-verified: `provisa/file_source/wikipedia.py` `LINK_SELECTOR`, `REMOVE_SELECTORS`, `TABLE_SELECTOR`, `DEFAULTS`]

**Configurações avançadas de rastreamento.** O formulário as mantém recolhidas em "Crawl settings" e mostra cada uma com seu padrão; deixe um campo vazio para mantê-lo. Em um mapping `files` os mesmos nomes vão sob `crawl:`, e um nome fora desta lista é recusado. [tool-verified: `provisa/file_source/crawl.py` `CRAWL_SETTINGS`, `crawl_operand`; `provisa/file_source/wikipedia.py` `crawl_settings`]

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

**Nomenclatura de tabela.** Uma tabela rastreada se chama `<page>__<name>`. O nome é o próprio `id` da tabela quando ela tem um, caso contrário sua legenda, caso contrário o cabeçalho mais próximo acima dela. Por exemplo, uma tabela sob o cabeçalho "Tallest buildings" na página `List_of_tallest_buildings` vira `list_of_tallest_buildings__tallest_buildings` após a normalização do nome. [tool-verified: calcite `file` adapter `HtmlToJsonConverter.java` (`baseFileName + "__" + tableName`), `HtmlTableScanner.java` `getTableName`] [inferred: the exact normalized spelling in the example]

**Atualizando.** Um rastreamento é mantido pelo período de cache, `1 days` por padrão (`html_cache_ttl`). As páginas são buscadas de novo apenas depois que esse tempo passou. [tool-verified: `provisa/file_source/wikipedia.py` `DEFAULTS`]

### Observabilidade e Outros {: #observability-other }

`prometheus` tem um conector Trino (propriedades construídas a partir da DSL de mapeamento do tipo). `google_sheets` é um tipo de fonte registrado sem conector Trino e materializa através do pipeline de cache de API. [tool-verified: `provisa/federation/trino_connectors.py:314`; `provisa/core/models.py` lines 87–88]

| Tipo de Fonte | Nome do Conector | Mutações |
| ------------ | ----------------- | ----------- |
| `google_sheets` | — (materializada) | Não |
| `prometheus` | prometheus | Não | Uma métrica é uma tabela e uma amostra é uma linha (`timestamp`, `value`, uma coluna por label); em todo motor sem conector ao vivo o Provisa lê a API HTTP — nomes de métrica e labels para Register Table, `query_range` sobre o intervalo da tabela para pouso [tool-verified: `provisa/prometheus/fetch.py`] (REQ-1689) |

### Conectores SaaS Empresariais {: #enterprise-saas-connectors }

Google Workspace e Microsoft 365 são lidos por loaders próprios; veja [Google Workspace e Microsoft 365](#google-workspace-and-microsoft-365). SharePoint, Salesforce, ServiceNow, o inventário de nuvem e o Splunk se registram através de conectores Apache Calcite (fork kenstott/calcite). Nenhum tem driver direto — o Provisa lança o servidor pgwire Calcite embutido do conector (`pgwire-sharepoint`, `pgwire-salesforce`, `pgwire-servicenow`, `pgwire-cloudops`, `pgwire-splunk`) e o alcança como um endpoint PostgreSQL genérico. No motor DuckDB esse endpoint é conectado (attach) ao vivo através da extensão postgres: Register Table lista as tabelas do conector a partir do catálogo conectado, consultas leem o conector no local, e filtros e projeções são empurrados para o Calcite (REQ-1690) [tool-verified: `provisa/federation/connector_duckdb.py` `_DuckDBPgwireConnector`]. Todo outro motor pousa as linhas no armazenamento de materialização para federação (REQ-954). Os pacotes são obtidos por SO/arquitetura a partir da release fixada `kenstott/calcite` (`pgwire-<connector>-<version>-<os>-<arch>.tar.gz`; macOS arm64, Linux x86_64, Windows x86_64) [tool-verified: `provisa/runtime_deps/pgwire_bundles.py`] Um operador pode em vez disso nomear uma pasta de pacote construída para este host, por conector, em Admin → Configurações → Servidores de conectores (`connectors.<connector>.bundle_dir`); consulte [Admin](admin.md). Os cinco conectores sempre habilitam correspondência de nome insensível a maiúsculas/minúsculas, correspondendo à semântica insensível a maiúsculas/minúsculas própria de cada produto (REQ-725, REQ-730). [tool-verified: `provisa/core/models.py` lines 99–100; `provisa/federation/trino_connectors.py` lines 223–286]

#### `sharepoint`

Listas do SharePoint são enumeradas como esquemas e expostas como tabelas consultáveis (REQ-726, REQ-731). Dois métodos de autenticação: `CLIENT_CREDENTIALS` (padrão) e baseado em certificado via um certificado PFX (REQ-727). Valores secretos em `mapping` são resolvidos através do motor de segredos antes de alcançar o conector (REQ-729). [tool-verified: `provisa/federation/trino_connectors.py` lines 230–252]

| Campo da fonte | Propriedade do conector | Notas |
| --- | --- | --- |
| `base_url` ou `host` | `site-url` | URL do site SharePoint |
| `username` | `client-id` | ID do cliente do app Azure |
| `password` | `client-secret` | Segredo do cliente do app Azure |
| `database` | `tenant-id` | UUID do tenant Azure |
| `mapping.auth_type` | `auth-type` | `CLIENT_CREDENTIALS` (padrão) ou `CERTIFICATE` |
| `mapping.certificate_path` | `certificate-path` | Caminho PFX quando `auth_type: CERTIFICATE` — precisa ser ABSOLUTO |
| `mapping.certificate_password` | `certificate-password` | Senha PFX — a chave precisa estar presente, string vazia para um PFX sem senha |

Autenticação por certificado nos motores que não são Trino carrega duas regras extras, ambas aplicadas quando o operando `model.json` do servidor pgwire Calcite é construído (REQ-1693). `certificate_path` precisa ser absoluto: o servidor roda com seu diretório de pacote como diretório de trabalho, então um caminho relativo se resolve dentro do cache de runtime-deps e o PFX não é encontrado. `certificate_password` precisa estar presente em `mapping` mesmo quando o PFX não tem senha, caso em que é a string vazia — o adaptador Calcite rejeita uma senha nula por completo, e uma chave ausente é tratada como erro de configuração em vez de lida silenciosamente como uma senha vazia. Um valor ausente ou relativo levanta `MissingConnectorConfig` nomeando o campo. [tool-verified: `provisa/federation/pgwire_replica.py` `_sharepoint_operand`]

Quando o conector não expõe `information_schema.columns`, registre a tabela com definições explícitas de coluna (obtidas da API Microsoft Graph) via a mutação `registerTable` (REQ-732).

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

Cada sObject que a credencial pode consultar é uma tabela que o steward pode registrar (`Account`, `Opportunity`, objetos personalizados `*__c`); nada é registrado automaticamente (REQ-1946). As colunas e os tipos vêm do describe do sObject. Filtros, projeções, ordenações e limites são delegados ao Salesforce como SOQL. [tool-verified: `provisa/federation/pgwire_replica.py` `_salesforce_operand`; `provisa/federation/trino_connectors.py` `TrinoSalesforceConnector`]

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

**Escritas.** `INSERT`, `UPDATE` e `DELETE` em um sObject registrado são mutações: governadas, auditadas e tratadas por ambiente como qualquer outra mutação. Em todos os mecanismos, inclusive o Trino, uma escrita é executada no servidor pgwire da fonte; no Trino esse servidor roda apenas para as escritas. Cada instrução é enviada ao Salesforce e confirmada quando executa, então não pode ser desfeita. O servidor não devolve as linhas que escreve: uma instrução com cláusula `RETURNING` é recusada, e o GraphQL não oferece campos de mutação para essas tabelas; `INSERT`, `UPDATE`, `DELETE` e `COPY` em SQL são os caminhos de escrita. Uma escrita é uma instrução simples em uma tabela: `INSERT … ON CONFLICT`, `UPDATE … FROM`, `DELETE … USING`, `MERGE` e `TRUNCATE` são recusados, indicando a forma. Uma fonte SharePoint é escrita da mesma maneira. [tool-verified: `provisa/executor/writable.py` `PGWIRE_SERVER_WRITTEN`; `provisa/api/data/pgwire_write.py`]

**Inicialização.** Antes de aceitar conexões, o servidor da fonte lê as colunas de cada sObject, cerca de quatro minutos para uma organização com 1.200 deles. Isso é pago uma vez: os resultados de describe ficam no diretório de estado da própria fonte, sob o diretório de dados da instância. O servidor é iniciado quando a fonte é registrada ou carregada. No Trino o catálogo `salesforce` guarda seus próprios resultados de describe, um diretório por fonte em `/data/trino/cache/salesforce-describe` no coordinator, então o custo é pago uma vez ali também quando esse caminho é um volume que sobrevive ao contêiner: o volume `trino_cache` da stack compose, ou o volume de cache do coordinator do chart Helm (`trino.coordinator.cache.persistence`). Até que ele aceite conexões, Register Table mostra o conector como iniciando e uma escrita recebe a mesma mensagem em vez de esperar. [tool-verified: `provisa/federation/pgwire_replica.py` `build_model_json`, `start_endpoint`]

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

Cada tabela da instância está disponível para registro; nada é registrado automaticamente (REQ-1954). Uma instância completa pode ter milhares de tabelas; Register Table as agrupa por tema — cada tema é o nome de exibição de um pacote ServiceNow, ou "(sem pacote)" quando uma tabela não pertence a nenhum. Cada tabela é exibida como `name — label`. Selecionar uma tabela lê suas colunas da instância; listar as tabelas não faz isso. [tool-verified: `provisa/federation/pgwire_replica.py` `servicenow_table_list`, `servicenow_columns`; `provisa/federation/servicenow.py` `TABLE_LIST`, `COLUMN_LIST`]

Uma fonte ServiceNow nomeia a URL `https://…` completa da instância junto com um nome de usuário e senha. A URL é retirada de `base_url` ou `host`; um host simples ou qualquer outro esquema é recusado, nomeando o campo (`instanceUrl`). Um nome de usuário ou senha ausente é recusado, nomeando o campo. Cada valor pode ser uma referência `${secret:…}` ou `${env:…}`.

| Campo da fonte | Propriedade do adaptador | Notas |
| --- | --- | --- |
| `base_url` ou `host` | `instanceUrl` | URL `https://…` completa da instância (por exemplo, `https://acme.service-now.com`). Um host simples ou outro esquema é recusado |
| `username` | `username` | Nome de usuário de autenticação básica |
| `password` | `password` | Senha de autenticação básica |

[tool-verified: `provisa/federation/servicenow.py` `servicenow_instance_url`, `servicenow_settings`]

**Leituras.** No Trino a fonte é lida pelo catálogo `servicenow`, que apenas lê. Em qualquer outro mecanismo, o mecanismo anexa o servidor pgwire da fonte e a lê no local. Somente as colunas selecionadas são solicitadas à instância (`sysparm_fields`). Um filtro é enviado à instância (`sysparm_query`) apenas em uma forma que o adaptador verificou em uma instância real; todos os outros filtros, e cada ordenação e limite, são aplicados às linhas lidas. [inferred: the adapter's requirements ledger (kenstott/calcite `docs/requirements/servicenow.yaml`, SNOW-022, SNOW-023, SNOW-030)]

Um campo de referência aparece como duas colunas: `<field>` contém o sys_id do registro referenciado e `<field>__display` contém o valor de exibição. Um campo vazio é NULL. Um campo cujo tipo o adaptador não consegue mapear gera um erro nomeando a tabela, o campo e o tipo. [inferred from adapter behavior; não verificado diretamente no código-fonte do Provisa nesta sessão]

A pesquisa de tabelas (linguagem natural) corresponde ao nome, rótulo e tema de cada tabela. Uma vez que uma tabela é registrada, suas colunas entram na pesquisa. [tool-verified: `provisa/discovery/catalog_cache.py` docstring de `COLUMNS_PER_TABLE_KINDS`]

**Ciclo de vida do servidor.** O servidor serve apenas as tabelas registradas nele. Registrar ou remover uma tabela reinicia o servidor e reprovision a fonte no mecanismo; uma instrução que chega durante esse reinício falha. [tool-verified: `provisa/federation/pgwire_replica.py` `SERVES_REGISTERED_TABLES`; `provisa/api/app_rebuild.py` `serve_registered_tables`]

**Escritas.** `INSERT`, `UPDATE` e `DELETE` em uma tabela registrada são mutações: governadas, auditadas e tratadas por ambiente como qualquer outra mutação. Em todos os mecanismos, inclusive o Trino, uma escrita é executada no servidor pgwire da fonte. O servidor retorna a linha que a instância contém após cada escrita; uma cláusula `RETURNING` é portanto aceita e o GraphQL oferece campos de mutação para essas tabelas. Uma escrita é uma única instrução em uma tabela: `INSERT … ON CONFLICT`, `UPDATE … FROM`, `DELETE … USING`, `MERGE` e `TRUNCATE` são recusados, nomeando a forma. [tool-verified: `provisa/executor/writable.py` `PGWIRE_SERVER_WRITTEN`, `PGWIRE_SERVER_RETURNS_ROWS`]

**Import sets.** Quando uma tabela tem um transform map ativo para ela, Register Table oferece seus import sets, e o steward pode vincular a tabela a um deles (uma tabela com vários maps nomeia cada um). As inserções em uma tabela vinculada passam pela Import Set API da instância e pela transformação daquele map; as inserções em uma tabela não vinculada, e cada atualização e exclusão, são gravadas diretamente na tabela. Uma linha que a transformação rejeita é recusada, nomeando a linha; linhas já gravadas permanecem gravadas. A vinculação é verificada quando a tabela é salva: deve ser o import set de um transform map ativo para a tabela, em uma fonte ServiceNow. [tool-verified: `provisa/api/admin/_import_set.py` `table_import_set_refusal`; `provisa/federation/servicenow.py` `servicenow_settings`; `provisa/federation/pgwire_replica.py` `servicenow_transform_maps`]

```yaml
- id: helpdesk-sn
  type: servicenow
  base_url: https://acme.service-now.com
  username: ${env:SN_USERNAME}
  password: ${env:SN_PASSWORD}
```

#### `cloudops`

O inventário de nuvem apresenta os recursos do Azure, da AWS e do GCP como um único conjunto de tabelas: `compute_resources`, `storage_resources`, `kubernetes_clusters`, `database_resources`, `network_resources`, `iam_resources`, `container_registries` e `compute_security_groups`, que associa um recurso de computação aos seus grupos de segurança. Cada tabela tem as mesmas colunas nas três nuvens, e sua coluna `cloud_provider` indica de qual nuvem veio cada linha. A fonte apenas lê, e suas tabelas são registradas por um steward como quaisquer outras (REQ-1947). [tool-verified: `provisa/federation/cloudops.py` `cloudops_settings`; `provisa/federation/trino_connectors.py` `TrinoCloudopsConnector`]

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

Resultados de busca do Splunk são consultáveis como tabelas (ex.: `internal_server`) (REQ-721). A URL do conector vem de `base_url`, ou é construída como `https://{host}:{port}` com uma porta padrão de `8089` (REQ-722). Autenticação: quando `mapping.use_token` é `true` (o padrão), `password` é passada como o token de API; quando `false`, `username` e `password` são passadas como credenciais separadas (REQ-723). [tool-verified: `provisa/federation/trino_connectors.py` lines 262–286]

| Campo da fonte | Propriedade do conector | Notas |
| --- | --- | --- |
| `base_url` / `host` + `port` | `url` | `base_url`, senão `https://host:port` (porta padrão 8089) |
| `password` | `token` ou `password` | token quando `use_token: true` |
| `username` | `user` | apenas quando `use_token: false` |
| `database` | `app` | restringe a um app Splunk |
| `mapping.datamodel_filter` | `datamodel-filter` | filtra para um data model |
| `mapping.disable_ssl_validation` | `disable-ssl-validation` | para certificados autoassinados (REQ-724) |

No caminho pgwire-replica (todo motor exceto o Trino) as mesmas quatro configurações opcionais se tornam as chaves do operando Calcite `model.json` `app`, `token`/`username`+`password`, `datamodelFilter` e `disableSslValidation` — as duas últimas nos tipos para os quais o `SplunkSchemaFactory` as converte, uma string e um booleano (REQ-1694). [tool-verified: `provisa/federation/pgwire_replica.py` `_splunk_operand`]

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

#### Google Workspace e Microsoft 365 (REQ-1923, REQ-1994) {: #google-workspace-and-microsoft-365 }

`google_workspace` e `microsoft_365` leem o e-mail, os calendários e as tarefas das contas que uma fonte indica, e os pousam como réplicas. Não são conectores Calcite: cada um tem um loader próprio que chama a API do provedor, Gmail, Google Calendar e Google Tasks de um lado e Microsoft Graph do outro. O formulário os lista em Enterprise como "Google Workspace (Gmail)" e "Microsoft 365 (Outlook, Exchange)". [tool-verified: `provisa/core/models.py` `SourceType`; `provisa/federation/strategy.py` `_MATERIALIZE_ONLY`; `provisa-ui/src/pages/sources/constants.ts`]

**Tabelas.** Os dois tipos produzem as mesmas tabelas canônicas: mesmos nomes, tipos de coluna e ordem. Uma união dos dois não precisa de cast nem de renomeação; uma coluna para a qual um provedor não tem valor é NULL. As tabelas são listadas no esquema `default`, e toda linha começa com a conta de onde foi lida. Uma fonte oferece as tabelas dos recursos que lê: [tool-verified: `provisa/core/canonical_mail.py` module docstring, `RESOURCE_TABLES`, `tables_for`; `provisa/google_workspace/loader.py` `SCHEMA`]

| Recurso | Tabelas | Escritas por instruções |
| --- | --- | --- |
| `mail` | `messages`, `message_recipients`, `folders`, `message_folders`, `threads`, `attachments` | Não |
| `calendar` | `calendars`, `events`, `event_attendees` | `events`, `event_attendees` |
| `tasks` | `task_lists`, `tasks` | `tasks` |

As colunas estão listadas no [esquema canônico de e-mail](arch/canonical-mail-schema.md). `events` guarda eventos únicos, eventos mestre de séries com a recorrência como linhas RFC 5545, e exceções; nunca guarda as ocorrências expandidas de uma série. [tool-verified: `provisa/google_workspace/calendar_read.py` module docstring] (REQ-1994)

**Registro.** O administrador de uma organização primeiro informa o cliente da organização para a plataforma em Admin → Email: um client ID e um secret, mais o ID do diretório (tenant) ou o domínio para o Microsoft 365. Quem adiciona uma fonte não o vê nem o digita. Uma fonte assume então uma de duas formas: [tool-verified: `provisa/core/mail_platforms.py` module docstring; `provisa/microsoft365/__init__.py`; `provisa-ui/src/i18n/locales/en/mailPlatforms.json`]

- **Uma caixa de correio.** A fonte indica um endereço em `accounts`. Seu dono clica em **Connect** no formulário e aprova o cliente da organização. A aprovação resultante é guardada no armazenamento de estado da organização, não nas configurações da fonte. No Google Workspace, essa forma também declara `sign_in: google_account`.
- **As caixas de correio da organização.** A fonte declara `mailboxes` como exatamente um entre `everyone: true`, `group: <address or id>` ou `list: [addresses]`. Não guarda credencial e ninguém a conecta. A organização lê com acesso próprio, e só depois que o administrador ativou "Read the organization's mailboxes". No Google, esse acesso é uma chave de service account com delegação em todo o domínio, mais um endereço de administrador para chamadas ao diretório. No Microsoft, é o grant client-credentials do registro do aplicativo.

[tool-verified: `provisa/google_workspace/settings.py` `parse`, `_mailboxes`; `provisa/microsoft365/settings.py` `parse`; `provisa/core/mail_platforms.py` `organisation_mailboxes`]

Na segunda forma, uma tabela guarda as linhas de todas as caixas. Um papel que pode ler a tabela lê todas as caixas, a menos que uma regra de linha na coluna `account` o limite. [tool-verified: `provisa-ui/src/i18n/locales/en/googleWorkspaceFields.json` `everyMailbox`]

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

**Configurações.** Cada módulo recusa pelo nome uma chave que não lê. [tool-verified: `provisa/google_workspace/settings.py` `_KEYS`; `provisa/microsoft365/settings.py` `_KEYS`]

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

O Google recusa uma busca sob `gmail.metadata`, então `mail_content: headers` com `mail_search` ou `mail_since` é recusado. [tool-verified: `provisa/google_workspace/settings.py` `_mail`]

**Escopos.** Um login pede apenas os escopos dos recursos marcados: [tool-verified: `provisa/google_workspace/settings.py` `MAIL_SCOPES`, `RESOURCE_SCOPES`; `provisa/google_workspace/sign_in.py` `EMAIL_SCOPE`; `provisa/microsoft365/settings.py` `RESOURCE_SCOPES`, `BASE_SCOPES`; `provisa/microsoft365/directory.py`]

| Recurso | Google Workspace | Microsoft 365 (delegated) |
| --- | --- | --- |
| `mail` | `gmail.readonly`, ou `gmail.metadata` somente para cabeçalhos | `Mail.Read` |
| `calendar` | `calendar.calendarlist.readonly`, `calendar.events` | `Calendars.ReadWrite` |
| `tasks` | `tasks` | `Tasks.ReadWrite` |
| toda fonte | `userinfo.email` | `offline_access`, `User.Read` |

Para a forma de organização, o Microsoft exige as permissões de aplicativo `Mail.Read` e `User.Read.All`, mais `Calendars.ReadWrite` e `Tasks.ReadWrite.All` para calendários e tarefas. A service account do Google é autorizada para os escopos em `DELEGATED_SCOPES`, que também incluem a leitura de usuários e membros de grupos do diretório. [tool-verified: `provisa/microsoft365/directory.py`; `provisa/google_workspace/__init__.py` `DELEGATED_SCOPES`; `provisa-ui/src/i18n/locales/en/mailPlatforms.json` `organisationGrant`]

**Atualização e frescor.** Os tipos de sondagem que essas fontes aceitam são `hash` e `none`, e o loader delas fornece o token de mudança: [tool-verified: `provisa/events/probes.py` `_MAIL_TYPES`, `probe_capabilities`]

- **Gmail:** um digest do history id atual de cada caixa, uma chamada `users.getProfile` por caixa. [tool-verified: `provisa/google_workspace/loader.py` `mailbox_token`]
- **E-mail do Microsoft:** cada delta link guardado, seguido por um item. [tool-verified: `provisa/microsoft365/loader.py` `unchanged`]
- **Calendários e tarefas do Google:** a tag e a hora da última mudança de cada calendário; a tag de cada lista de tarefas, e o id e a hora de atualização de cada tarefa. [tool-verified: `provisa/google_workspace/calendar_read.py` `change_token`]
- **Calendários e tarefas do Microsoft:** as linhas de mudança de cada caixa, dentro de `calendar_check_days` antes e depois de hoje. [tool-verified: `provisa/microsoft365/loader.py` `_calendar_token`] [inferred: that a line is an event's id and change key, or a task's update time, from REQ-1994]

Uma verificação que não pode ser feita pede uma leitura; nunca informa "sem mudança". As tabelas de calendário e tarefas são lidas inteiras a cada atualização. As réplicas de e-mail de uma fonte Microsoft 365 ou Google Workspace são mantidas no lugar: cada parte de uma caixa é escrita conforme chega e pode ser consultada na hora. [tool-verified: `provisa/core/canonical_mail.py` `RESOURCE_TABLES` comment; `provisa/federation/replica_builds.py` `GROUP_READ_TYPES`; `provisa/federation/mail_units.py`] Uma fonte Google Workspace que restringe por `mail_search` é relida inteira a cada atualização, porque só o Google pode dizer o que uma busca encontra. [tool-verified: `provisa/google_workspace/units.py` module docstring]

**Escritas.** `INSERT`, `UPDATE` e `DELETE` em `events`, `event_attendees` e `tasks` vão ao provedor por um único caminho de escrita para os dois tipos. Um `UPDATE` ou `DELETE` indica suas linhas por `provider`, `account` e `id`; um participante é indicado por `event_id` e `address`. Uma ocorrência de uma série é indicada por `series_id` e `original_start_at`. Uma escrita que define uma coluna fora do conjunto gravável da tabela é recusada pelo nome. O Google escreve com `sendUpdates=all` quando `calendar_send_updates` está ativo e com `sendUpdates=none` quando está desativado. O Microsoft Graph não consegue fazer uma mudança dessas em silêncio, então uma mudança nos participantes de uma reunião é recusada, indicando a configuração. [tool-verified: `provisa/executor/writable.py` `API_WRITTEN`; `provisa/core/canonical_mail.py` `WRITE_KEYS`, `WRITABLE_COLUMNS`; `provisa-ui/src/i18n/locales/en/mailResources.json` `sendUpdatesHelp`] [tool-verified: `provisa/google_workspace/calendar_write.py`]

**Exclusões.** O e-mail é somente leitura; nenhuma instrução altera uma caixa. `calendars`, `task_lists` e toda tabela de e-mail não aceitam escritas. Spam e Lixeira do Gmail ficam de fora a menos que `mail_include_spam_trash` esteja ativo. A API Tasks do Google guarda apenas o dia em que uma tarefa vence, e não tem lembrete. [tool-verified: `provisa/microsoft365/settings.py` `RESOURCE_SCOPES`; `provisa/core/canonical_mail.py` `WRITABLE_COLUMNS`; `provisa/google_workspace/settings.py` `_mail`] [inferred: the Tasks API limit, from REQ-1994]

### Fontes de API {: #api-sources }

Registre qualquer endpoint HTTP como uma tabela consultável. [tool-verified: `provisa/core/models.py` `SourceType` enum] (REQ-314, REQ-307, REQ-322)

| Tipo de API | Descoberta | Inferência de Coluna |
| --------- | ----------- | ----------------- |
| `openapi` | Parse de especificação OpenAPI (REQ-314, REQ-316) | Primitivos → nativo, objetos → JSONB |
| `graphql_remote` | Introspecção de esquema (REQ-307, REQ-308) | Primitivos → nativo, objetos → JSONB |
| `grpc_remote` | Reflexão de servidor (REQ-322, REQ-325) | Primitivos → nativo, objetos → JSONB |

Respostas de API são buscadas, armazenadas em cache no PostgreSQL (TTL configurável), e expostas como tipos GraphQL (REQ-309, REQ-318, REQ-327). Tabelas em cache participam de consultas federadas como qualquer outra fonte (REQ-313).

**Regras JSONB**: Colunas complexas (objetos, arrays) armazenadas como JSONB não são filtráveis (REQ-119). O acesso a subcampo usa extração `->>` em SQL (REQ-151). Relacionamentos são declarados entre tabelas usando colunas FK escalares — colunas de blob JSONB não são alvos de join. Use a promoção de JSONB para converter campos aninhados em colunas escalares nativas quando filtrar ou unir sobre eles for necessário (REQ-119).

#### GitHub e GitLab {: #github-and-gitlab }

GitHub e GitLab são fontes de marca conduzidas por `graphql_remote`: cada uma aparece no formulário de Fontes com o próprio nome e se registra como uma fonte GraphQL remota que guarda sua marca. O formulário pede um personal access token e nada mais. O endpoint (`https://api.github.com/graphql`, `https://gitlab.com/api/graphql`) e o esquema acompanham o Provisa, então o esquema não passa por introspecção no registro. O token é verificado quando a fonte é adicionada. Adicionar a fonte não registra tabelas: toda tabela que o esquema oferece fica disponível sob o namespace da fonte (`gh`, `gl`), e o steward registra as desejadas. Um campo que o token não pode ler fica de fora de uma tabela quando ela é registrada. [tool-verified: `provisa/graphql_remote/brands.py` `BRANDS`] (REQ-1923)

#### Stripe (REQ-1923) {: #stripe }

O Stripe é uma fonte com marca carregada por `openapi`, construída como GitHub e GitLab são construídos sobre `graphql_remote`. Aparece no formulário de Fontes como Stripe, em Enterprise. Registra-se como uma fonte OpenAPI cuja linha grava a marca em `federation_hints` e cujo caminho de especificação é `brand:stripe`. [tool-verified: `provisa-ui/src/pages/sources/constants.ts` `BRAND_CARRIER`; `provisa/openapi/brands.py` `BRANDS`, `SPEC_SCHEME`; `provisa/api/admin/openapi_router.py` `_branded`]

**O que alcança.** A API REST do Stripe, pela especificação OpenAPI que ele publica (`stripe/openapi`, `spec3.json`). A especificação acompanha o Provisa, fixada em uma versão e comprimida com gzip, e não é baixada no registro. A que acompanha é a versão de API `2026-09-30.endive` com 425 caminhos. Toda chamada leva `Stripe-Version` definido para essa versão, então cada resposta tem a forma que a especificação descreve, seja qual for a versão padrão da conta. Os campos que o Stripe devolve mas a especificação omite são acrescentados de `stripe.additions.json`. [tool-verified: `provisa/openapi/brands.py` `brand_spec`, `Brand.headers`; `provisa/openapi/_known_specs/`; version and path count read with the project interpreter]

Como em qualquer fonte OpenAPI, cada operação `GET` é uma tabela oferecida e cada outra operação é um comando oferecido. Adicionar a fonte não registra nenhuma: o steward registra as desejadas. [tool-verified: `provisa/api/admin/openapi_router.py` `register_openapi_source`; `provisa/openapi/brands.py` module docstring]

**Autenticação.** O formulário pede uma só coisa, um token de acesso (`BRAND_AUTH`: `token`). Use uma chave de API secreta ou restrita do Stripe. O token é enviado como bearer token e verificado uma vez com `GET /v1/balance` antes de a fonte ser adicionada. Um token rejeitado faz o registro falhar com `openapi.credential_rejected`; a ausência dele o faz falhar com `openapi.credential_required`. [tool-verified: `provisa-ui/src/pages/sources/constants.ts` `BRAND_AUTH`; `provisa/openapi/brands.py` `Brand.auth`, `verify_path`; `provisa/api/admin/openapi_router.py` `_branded`] [inferred: that a restricted key works, provided it may read `/v1/balance`]

**Atualização e frescor.** Uma tabela Stripe é uma réplica, como toda tabela OpenAPI. Suas respostas em cache duram o `cache_ttl` da fonte, que uma requisição de registro define como 300 segundos, a menos que diga outra coisa. Aplicam-se os tipos de sondagem do OpenAPI. [tool-verified: `provisa/api/admin/openapi_router.py` `OpenAPIRegisterRequest.cache_ttl`; `provisa/events/probes.py` `_HTTP_API_TYPES`] O REQ-1923 prevê que um preset possa indicar o parâmetro que uma atualização usa para buscar só o que mudou. A marca Stripe não indica nenhum, então cada atualização lê suas tabelas inteiras. [tool-verified: `provisa/openapi/brands.py` `Brand` has no such field]

**Exclusões.** A integração de pagamento descontinuada do Stripe baseada em token e Sources fica de fora da especificação que acompanha o produto. A lista está em [Exclusões em uma fonte construída a partir de uma especificação](#exclusions-on-a-specification-built-source). As `exclusions` do próprio operador são somadas às da marca. [tool-verified: `provisa/openapi/_known_specs/stripe.exclusions.json`; `provisa/openapi/brands.py` `brand_spec`] (REQ-1957)

`stripe` também aparece em `SourceType` entre os tipos stub do Supabase `wrappers`. Uma fonte Stripe adicionada pelo formulário é uma fonte `openapi`, não esse stub. [tool-verified: `provisa/core/models.py` `SourceType`]

#### Exclusões em uma fonte construída a partir de uma especificação {: #exclusions-on-a-specification-built-source }

Uma especificação OpenAPI publicada descreve tudo o que a API remota aceita, inclusive o que seu fornecedor descontinuou e o que uma implantação nunca deve enviar. Uma fonte OpenAPI pode ter exclusões: coisas da especificação que a fonte não oferece. (REQ-1957)

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

Dados abertos do governo dos EUA. O acesso é particionado por agrupamento de assunto. [tool-verified: `provisa/core/models.py` lines 543–609]

Uma fonte recebe um ou mais assuntos, e cada assunto traz os esquemas do adaptador listados abaixo. Um esquema pode pertencer a mais de um assunto (`census`). Os esquemas `ref` e `geo` são esquemas de ligação: dados de referência e geografia pelos quais as tabelas de todo assunto se unem, então toda fonte os serve, quaisquer que sejam os assuntos que recebeu. A lista é declarada uma única vez no servidor e é mantida, por um teste, igual aos esquemas que o pacote do adaptador fixado serve. [tool-verified: `provisa/core/models.py` `GOVDATA_SUBJECT_SCHEMAS`, `GOVDATA_LINKER_SCHEMAS`; `tests/unit/test_govdata_subject_schemas.py`]

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

Uma fonte AskAmerica criada no admin recebe uma credencial, a chave de API. O Provisa apresenta a chave à API da AskAmerica e recebe as credenciais de armazenamento, de curta duração e somente leitura, com as quais os dados são lidos; a chave é recusada ali, pelo nome, se for desconhecida ou revogada. [tool-verified: `provisa/federation/askamerica.py`]

A fonte é então uma fonte Postgres-wire como as fontes SharePoint, Salesforce e inventário de nuvem: o Provisa inicia para ela o servidor `pgwire-govdata` embutido, com a chave e essas credenciais no ambiente do servidor, e cada motor alcança esse servidor como alcança o de uma fonte irmã. DuckDB, Postgres e ClickHouse o anexam e leem suas tabelas no local, então uma tabela da AskAmerica se une a tabelas de outras fontes, fica sob views e é entregue como qualquer outra. O Trino não tem conector para ela e lê uma réplica pousada pelo servidor: uma cópia da tabela mantida no armazenamento de materialização, tão atual quanto seu último refresh sob as configurações de cache e replicação da tabela, e não lida no momento da consulta. Uma tabela é lida do esquema do adaptador que ela nomeia (`sec`, `econ` e assim por diante). [tool-verified: `provisa/federation/pgwire_replica.py`, `provisa/federation/connector_duckdb.py`, `provisa/federation/clickhouse_connectors.py`]

O servidor monta seus esquemas a partir do armazenamento de objetos antes de aceitar uma conexão, o que leva minutos na primeira inicialização. Ele é iniciado quando a fonte é salva; até que esteja pronto, o formulário de Register Table informa que a fonte ainda está iniciando. [tool-verified: `provisa/federation/pgwire_replica.py` `start_when_saved`]

### Onde vive a senha de uma fonte {: #where-a-sources-password-lives }

A senha de uma fonte nunca é armazenada ao lado do restante de suas configurações de conexão. A
linha `sources` do plano de controle carrega uma coluna `password_ref` contendo o que foi digitado
como senha: o próprio valor, ou uma referência como `${env:PG_PASSWORD}` ou
`${secret:SNOWFLAKE_KEY}`. Ela é resolvida no momento em que a fonte é discada, dentro da
organização em que a requisição está atuando. [tool-verified: `provisa/core/schema_org.py`,
`provisa/core/repositories/source.py`]

Um valor sem `${…}` é usado como está. `${env:VAR}` lê o ambiente de processo da implantação e não
precisa de nada vinculado. `${secret:NAME}` nomeia um segredo que uma organização possui, então ele
se resolve apenas dentro das próprias operações dessa organização: as costuras de introspecção do
admin e o terminal de consulta que toda interface alcança estabelecem essa vinculação.
[tool-verified: `provisa/core/secrets.py` `resolve_secrets`; `provisa/pgwire/_pipeline.py` `_execute_plan`]

O campo guarda o que você digitou, seja qual for a forma como a fonte foi registrada:

- **A partir da config.** O arquivo é o registro: o Provisa copia o valor ou a referência para
  `password_ref` literalmente.
- **A partir do formulário de Fontes.** O que é digitado no campo de senha é armazenado
  literalmente, seja um valor ou uma referência. O Provisa não escreve nada no cofre da organização
  para ele e não inventa nenhum nome de segredo. Para manter uma credencial fora da linha do plano
  de controle, crie o segredo você mesmo (veja [Segredos](secrets.md)) e digite `${secret:NAME}` no
  campo. [tool-verified: `provisa/api/admin/schema_mutation.py` `create_source`]

Um valor digitado como valor é armazenado no banco de dados do plano de controle como foi digitado.
Redigitar a senha substitui o texto do campo. Excluir a fonte remove sua linha e nada do cofre: um
segredo que você criou é seu. [tool-verified: `provisa/api/admin/schema_mutation.py`
`delete_source`; `tests/unit/test_no_product_written_secrets.py`]

`password_ref` não viaja entre ambientes (REQ-1491). Um branch ou um ambiente copiado fornece
seus próprios valores de conexão, e o cofre que uma referência nomeia pertence a qualquer que seja
o ambiente que a forneceu. [tool-verified: `provisa/core/env_classes.py` `BINDING_COLUMNS`]

### Data profiler {: #data-profiler }

Um data profiler é uma fonte que guarda um nome, uma agenda e os padrões de execução, e nada mais. Ele nunca passa por introspecção. Tabelas de outras fontes são adicionadas a ele, e cada execução as mede e grava suas linhas no plano de controle da organização, onde são lidas como as tabelas de qualquer outra fonte. Veja [Test data](test-data.md) para criar um profiler e adicionar tabelas a ele. [tool-verified: `provisa/core/models.py` `SourceType.data_profiler`; `provisa/federation/strategy.py` `_MATERIALIZE_ONLY`] (REQ-1934)

### Verificadores de Qualidade de Dados (REQ-1443) {: #data-quality-checkers-req-1443 }

Um verificador de qualidade de dados é um tipo de fonte, não um subsistema. Sua saída de scan é dado: um resultado de verificação é uma observação, então ela chega pelo caminho de fonte comum e herda cadência, frescor, eventos, linhagem, governança, RLS, grid e exportação de qualquer outra fonte. [tool-verified: `provisa/core/models.py` lines 110–116 `SourceType.soda`, `SourceType.great_expectations`; `provisa/events/source_loader.py` `make_dq_loader`]

Dois são suportados, e a escolha é tanto uma escolha de licença quanto de funcionalidade.

| Tipo de Fonte | Dialeto do Contrato | Extra | Licença | Plano de nuvem hospedado |
| ------------ | ----------------- | ------- | --------- | -------------------- |
| `soda` | YAML de contrato Soda | `pip install .[soda]` (`soda-postgres`) | Elastic License 2.0 | Recusado — veja abaixo |
| `great_expectations` | JSON de suíte de expectativas | `pip install .[gx]` (`great-expectations[postgresql]`) | Apache 2.0 | Permitido |

A Elastic License 2.0 proíbe fornecer o software a terceiros como serviço hospedado ou gerenciado, e executar o Soda dentro do plano SaaS em nome de um locatário é exatamente isso. `config/capabilities.yaml` carrega essa divisão como `cloud_eligible: false` na opção `soda`, e o plano hospedado lê essa flag. Uma implantação hospedada que quer usar o Soda alcança um endpoint Soda fornecido pelo operador, que o próprio operador executa. [tool-verified: `config/capabilities.yaml` lines 197–203]

O Provisa não empacota nem vincula nada. O scan roda em um interpretador filho (`python -m provisa.dq.worker`), que é o único lugar onde `soda_core` ou `great_expectations` é importado, então um verificador source-available nunca alcança o processo do servidor e uma falha do verificador mata um subprocesso em vez do event loop. [tool-verified: `provisa/dq/runner.py` `build_command`, `run_contract`]

**A fonte aponta para o próprio endpoint pgwire do Provisa.** É isso que permite que um único driver postgres verifique uma tabela apoiada em Snowflake ou Iceberg: o verificador escaneia a exibição federada, não o sistema subjacente. Como a política se aplica a essa conexão, a identidade do scan é declarada em vez de herdada — um conjunto de linhas filtrado nunca deve produzir uma verificação que passe silenciosamente.

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

**Uma tabela de resultados por contrato, e o contrato é todo o registro.** A tabela carrega `dq_contract` — o texto do contrato ao pé da letra — e nada mais sobre sua forma. Colunas, marca d'água e promoções são todas derivadas. [tool-verified: `provisa/dq/registration.py` `derive_checker_table`]

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

O que o registro deriva desse texto:

- **Linhagem.** O contrato já nomeia seu conjunto de dados alvo, então o registro o interpreta da mesma forma que `extract_inputs` interpreta SQL (REQ-939) e o resolve para a tabela governada. Uma única definição, sem segunda cópia que possa divergir. Um contrato que nomeia um conjunto de dados não governado falha alto no registro em vez de pousar linhas que ninguém pediu.
- **Colunas.** O envelope de resultado é do verificador, não do operador — 16 colunas entregues, de `scan_id` até `diagnostics`. Colunas declaradas são lidas apenas por seu `visible_to`, que deve ser unânime, e então são substituídas. [tool-verified: `provisa/dq/results.py` `_ENVELOPE`, `results_columns`]
- **Marca d'água.** `scan_time` se torna a marca d'água, o que torna o pouso um append (REQ-982). O histórico de scans se acumula sem um subsistema de histórico.
- **Promoções.** `freshness_max_timestamp` e `dataset_rows_tested` são promovidos para fora do jsonb `diagnostics` como colunas tipadas (REQ-119). Adicione mais da mesma forma que faria em qualquer outra coluna jsonb. [tool-verified: `provisa/dq/results.py` `DQ_PROMOTIONS`]

A temporização não introduz novos campos. `change_signal` somado a `cache_ttl` fornece a cadência de polling; `mv_debounce_quiet` e `mv_debounce_max_delay` colapsam uma rajada upstream em um único scan (REQ-963); um grão de calendário o torna periódico (REQ-962); `expected_events` retém o scan até que suas entradas estejam frescas ao longo da janela (REQ-961). O loop de polling é o agendador do scan.

`outcome` é um de `pass`, `fail`, `warn`, `error`, `skipped`. Nenhum deles é um veredito — a aplicação, se desejada, é uma declaração separada posterior: um preflight ou uma MV sobre os resultados pousados. Como uma observação pousada não carrega nenhuma obrigação de determinismo (REQ-964), verificações não determinísticas são admissíveis aqui que nunca poderiam estar em um gate de preflight — pontuação de anomalia, mudança de janela móvel, frescor em relação a agora.

O contrato é escrito na UI, no painel de qualidade de dados da superfície de edição de tabela, e o texto bruto do contrato ali é sempre a fonte da verdade. Um dry run executa o contrato contra a tabela ao vivo e mostra os resultados sem pousá-los — assim é como você detecta um contrato cujo nome de conjunto de dados foi resolvido para algo inesperado e que, de outra forma, pousaria apenas linhas aprovadas.

---

## Conectores Personalizados (REQ-1177) {: #custom-connectors-req-1177 }

Os motores de federação nativos — Postgres, DuckDB, e ClickHouse — ganham alcance a um novo tipo de fonte quando um operador declara um conector para ele em `config/custom_connectors.yaml`. Nenhum código é exigido. [tool-verified: `provisa/federation/custom_connectors.py` `load_custom_connectors`; `provisa/federation/engine.py` `build_pg_engine`, `build_duckdb_engine`, `build_clickhouse_engine`]

A extensibilidade de conector em si já existia antes disso. O motor Trino há muito é extensível em sua própria camada — um conector JDBC genérico parametrizado por tipo de fonte, um corpo `.properties` de catálogo por tipo, e os próprios plugins de conector Trino personalizados do Provisa (Splunk, SharePoint, Calcite). [tool-verified: `provisa/federation/trino_connectors.py` `_TrinoJdbcConnector`, `_TRINO_JDBC_TYPES`; `trino/plugins/trino-splunk`, `trino/plugins/trino-sharepoint`, `trino/plugins/trino-calcite`] O REQ-1177 traz essa mesma extensibilidade orientada por config aos dois motores nativos sem cluster, que anteriormente carregavam um conjunto fixo de conectores.

A config é entregue vazia. Conectores embutidos cobrem o alcance pronto para uso; tudo neste arquivo é escrito pelo operador. [tool-verified: `config/custom_connectors.yaml` line 52: `connectors: []`] Defina `PROVISA_CUSTOM_CONNECTORS` para apontar para um caminho diferente (útil para testes).

### Tipos de descritor {: #descriptor-kinds }

| Motor | Tipo | Mecanismo | O que o descritor fornece |
| --- | --- | --- | --- |
| `postgres` | `pg_fdw` | SQL/MED (padrão ISO) | `extension`, `server_options`, `user_mapping`, `supports_import`, `table_options`, `remote_schema` |
| `duckdb` | `duckdb_attach` | INSTALL/LOAD + ATTACH | `extension`, `probe_symbol`, `attach_template`, `remote_schema` |
| `duckdb` | `duckdb_scan` | INSTALL/LOAD + view de scanner | `extension`, `probe_symbol`, `scan_template` |
| `clickhouse` | `clickhouse_database` | `CREATE DATABASE ENGINE=…` (expõe automaticamente toda tabela remota) | `ch_engine`, `engine_template` |
| `clickhouse` | `clickhouse_table` | `CREATE TABLE ENGINE=…` por tabela (colunas do registro) | `ch_engine`, `engine_template` (pode carregar `{table}`) |
| `clickhouse` | `clickhouse_scan` | `CREATE TABLE ENGINE=…`, ClickHouse infere o esquema | `ch_engine`, `engine_template` |

**Postgres é genérico.** SQL/MED é um padrão ISO, então todo FDW conforme compartilha a mesma forma de DDL: `CREATE SERVER … FOREIGN DATA WRAPPER <fdw> OPTIONS(…)`, `CREATE USER MAPPING` opcional, depois ou `IMPORT FOREIGN SCHEMA` (quando `supports_import: true`) ou um `CREATE FOREIGN TABLE` explícito por tabela (quando `false`). Um descritor `pg_fdw` fornece apenas a variação por FDW — nome da extensão, chaves de opção de servidor, chaves de mapeamento de usuário, flag de importação, opções de tabela. Qualquer FDW conforme ao padrão é, portanto, controlável apenas pela config. [tool-verified: `provisa/federation/custom_connectors.py` `GenericPgFdwConnector.details` lines 98–125]

**DuckDB suporta dois mecanismos.** Uma extensão que expõe um catálogo via ATTACH usa `duckdb_attach`; uma que expõe uma table-function de leitura usa `duckdb_scan`. Uma extensão que não se encaixa em nenhum dos padrões não é suportada. [tool-verified: `provisa/federation/custom_connectors.py` `GenericDuckDbAttachConnector`, `GenericDuckDbScanConnector`]

**ClickHouse suporta três mecanismos**, um por forma de motor de integração: um motor DATABASE relacional que expõe automaticamente toda tabela remota (`clickhouse_database`, ex.: Redis/MySQL), um motor por tabela cujas colunas o registro fornece (`clickhouse_table`, ex.: a ponte JDBC/ODBC — o `engine_template` pode carregar um placeholder `{table}` que o runtime vincula), e um motor de arquivo/lake/URL cujo esquema o ClickHouse infere (`clickhouse_scan`, ex.: HDFS/URL). SQLite (motor DATABASE, arquivo, sem servidor) e Hudi (lakehouse, zero-cópia) são entregues prontos para uso. [tool-verified: `provisa/federation/custom_connectors.py` `GenericClickHouseDatabaseConnector`, `GenericClickHouseTableConnector`, `GenericClickHouseScanConnector`; `provisa/federation/clickhouse_connectors.py` `ClickHouseSqliteConnector`, `ClickHouseHudiConnector`] (REQ-1178)

Um valor `kind` desconhecido falha alto na inicialização — um erro de digitação no descritor não deve deixar silenciosamente um tipo de fonte inalcançável. [tool-verified: `provisa/federation/custom_connectors.py` `load_custom_connectors` lines 178–197]

### Gate de sondagem {: #probe-gating }

A disponibilidade é verificada no momento do attach contra o catálogo de descoberta padrão de cada motor:

- **Postgres** — verifica `pg_extension`, depois `pg_available_extensions`. [tool-verified: `provisa/federation/connector_duckdb.py` `_probe_pg_extension` lines 333–344]
- **DuckDB** — executa `INSTALL`/`LOAD` e verifica `duckdb_functions()` pelo `probe_symbol` declarado. [tool-verified: `provisa/federation/connector_duckdb.py` `_DuckDBExtensionConnector.probe` lines 160–180]
- **ClickHouse** — verifica `system.table_engines` pelo `ch_engine` declarado; ausente da build falha alto. [tool-verified: `provisa/federation/custom_connectors.py` `_probe_clickhouse_engine`]

Uma extensão declarada que não é instalável falha alto. Sem skip silencioso, sem fallback. Um conector cuja sondagem falha simplesmente não fica ativo para aquela implantação.

### Variáveis de template {: #template-variables }

Todo valor `server_options`, valor `user_mapping`, `attach_template`, e `scan_template` pode usar placeholders `{field}`. Campos disponíveis: [tool-verified: `provisa/federation/custom_connectors.py` `_source_fields` lines 53–63]

`{id}`, `{host}`, `{port}`, `{database}`, `{username}`, `{password}`, `{path}`, `{schema_name}`, `{table_name}`, mais qualquer chave de `federation_hints`. Templates de attach do DuckDB também recebem `{alias}` — o alias interno de catálogo que o Provisa atribui ao banco de dados anexado.

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

[tool-verified: `config/custom_connectors.yaml` commented examples, lines 26–50]

Com qualquer um dos descritores em vigor, registrar uma fonte com o `source_type` declarado é roteado através do conector personalizado, sujeito a uma sondagem bem-sucedida. Nenhuma outra mudança de configuração é necessária.

---

## Warehouses como Fontes Nomeadas {: #warehouses-as-named-sources }

Snowflake, Databricks, e ClickHouse podem ser registrados como fontes nomeadas independentemente de qual motor de federação está ativo. [tool-verified: `executor/drivers/snowflake.py` (REQ-988), `executor/drivers/databricks.py` (REQ-987), `executor/drivers/clickhouse.py` (REQ-986)]

Quando registrado, o Provisa lê o warehouse via o DirectDriver da fonte e pousa uma réplica no armazenamento de materialização do motor ativo. A consulta então roda contra essa réplica. Isso difere do caminho tradicional direta-capaz (asyncpg, aiomysql), onde o motor é totalmente contornado — aqui o motor ainda executa a consulta, mas contra uma réplica local em vez de pela rede até o warehouse em cada requisição.

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

Dados de objeto/lake em nuvem (arquivos parquet, csv, iceberg, delta_lake em S3 / GCS / R2) são um tipo de fonte separado que faz attach no local quando o motor ativo tem um conector ATTACH para esse tipo. Nenhuma réplica é pousada — o motor escaneia o armazenamento de objeto diretamente. Credenciais para essas fontes também vão em `federation_hints`:

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

Todas as fontes compartilham um conjunto comum de campos. [tool-verified: `provisa/core/models.py` `Source` class, lines 138–204]

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
| `pool_min` | Não | `1` | Tamanho mínimo do pool de conexão (REQ-052) |
| `pool_max` | Não | `5` | Tamanho máximo do pool de conexão (REQ-052) |
| `use_pgbouncer` | Não | `false` | Roteia conexões através do PgBouncer (REQ-053) |
| `pgbouncer_port` | Não | `6432` | Porta do PgBouncer (REQ-053) |
| `cache_enabled` | Não | `true` | Habilita cache de resposta de API |
| `cache_ttl` | Não | `null` | TTL de cache em segundos; herda o padrão global quando nulo |
| `cache_catalog` | Não | `null` | Catálogo federado para cache de API; padrão é o próprio catálogo da fonte |
| `cache_schema` | Não | `api_cache` | Esquema dentro do catálogo de cache |
| `naming_convention` | Não | `null` | Sobrepõe a convenção de nomenclatura global para esta fonte (REQ-194) |
| `federation_hints` | Não | `{}` | Propriedades de sessão passadas ao motor de federação, e parâmetros de conexão estendidos para fontes de warehouse (REQ-278, REQ-281) |
| `mapping` | Não | `{}` | Configurações de conector específicas de tipo para fontes NoSQL e SaaS (ex.: `auth_type` do SharePoint, `auth_type` do Salesforce, `use_token` do Splunk) (REQ-251) |
| `allowed_domains` | Não | `[]` | Restringe a fonte a domínios específicos; vazio = irrestrito |
| `description` | Não | `""` | Descrição legível por humanos |

---

## Fontes Kafka {: #kafka-sources }

Tópicos Kafka são configurados separadamente sob `kafka_sources`, indexados pelo `id` da fonte de uma fonte `kafka` registrada. [tool-verified: `config/provisa.yaml` lines 138–151] (REQ-147)

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
| `topics[].default_window` | Janela de tempo padrão para consultas em janela (ex.: `1h`) (REQ-148) |
| `topics[].columns` | Definições de coluna para o esquema do tópico (REQ-150) |

---

## Visibilidade de Coluna {: #column-visibility }

O campo `visible_to` em cada coluna é uma lista de IDs de função que podem ver aquela coluna. [tool-verified: `provisa/core/models.py` `Column` class line 248; `config/provisa.yaml` lines 39–51]

```yaml
columns:

  - name: email
    visible_to: [admin]        # only admin role sees this column

  - name: region
    visible_to: [admin, analyst]  # both roles see this column
```

Colunas omitidas da lista `visible_to` de uma função não aparecem no esquema GraphQL dessa função e não podem ser consultadas ou referenciadas em filtros (REQ-039).

---

## Relacionamentos {: #relationships }

Relacionamentos conectam duas tabelas registradas e aparecem como campos aninhados no GraphQL. [tool-verified: `provisa/core/models.py` `Relationship` class lines 323–343; `config/provisa.yaml` lines 103–110] (REQ-019)

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
| `cardinality` | Sim | `many-to-one` ou `one-to-many` (REQ-019) |
| `materialize` | Não | Cria automaticamente uma view materializada para joins entre fontes (REQ-158). Em uma aresta apoiada em junção, a view cobre a travessia de dois saltos, não um join direto (REQ-1586) |
| `refresh_interval` | Não | Intervalo de atualização de MV em segundos (padrão: 300) |
| `target_function_name` | Não | Nome da função do BD para relacionamentos computados |
| `function_arg` | Não | Qual argumento da função recebe o valor da coluna de origem |
| `alias` | Não | Tipo de relacionamento legível por humanos (ex.: `WORKS_FOR`) |
| `graphql_alias` | Não | Nomeia o campo SDL que este relacionamento expõe no tipo pai. Quando ausente, o nome é derivado do `field_name` da tabela alvo e da cardinalidade do relacionamento. [tool-verified: `provisa/compiler/schema_gen.py:1050`] |
| `disable_cypher` | Não | Quando `true`, exclui este relacionamento das arestas do grafo Cypher |
| `source_json_key` | Não | Extrai esta chave da coluna de origem como um objeto JSON antes do JOIN |
| `via_table` | Não | Nome da tabela registrada da junção que esta aresta percorre. Defini-lo torna a aresta apoiada em junção; deixá-lo vazio a mantém uma aresta de chave estrangeira (REQ-1586) |
| `via_source_column` | Não | Coluna da junção que pareia com `source_column`. Separada por vírgulas e posicional para uma chave composta |
| `via_target_column` | Não | Coluna da junção que pareia com `target_column` |
| `via_type_column` | Não | Coluna discriminadora, quando uma mesma junção carrega vários tipos de relacionamento |
| `via_type_value` | Não | O valor de discriminador ao qual esta aresta está fixada |
| `via_label_source` | Não | Qual indicação nomeia o tipo Cypher: `column` (o valor do discriminador), `table` (o nome da tabela de junção) ou `fixed` (o alias declarado). Todos são convertidos para UPPER_SNAKE_CASE |

### Relacionamentos apoiados em junção {: #junction-backed-relationships }

Uma tabela associativa pode ser declarada como relacionamento Cypher de primeira classe em vez de nó, de modo que suas próprias colunas se tornem os atributos desse relacionamento: (REQ-1586)

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

A junção é uma tabela registrada como qualquer outra e precisa estar registrada antes que um relacionamento possa nomeá-la. Declare-a uma vez por valor de discriminador: três linhas sobre `pet_companions` produzem `BONDED_PAIR`, `LITTERMATE` e `SHARES_ENCLOSURE` como três tipos Cypher distintos, cada um carregando as demais colunas da linha de junção como propriedades da aresta. A configuração de demonstração distribuída declara exatamente isso.

Uma aresta de junção é um relacionamento Cypher, não um campo de join GraphQL: o emissor de joins do GraphQL monta sua cláusula `ON` para um único par de colunas e não tem lugar para o segundo salto, então arestas de junção ficam excluídas do SDL gerado e de `pg_constraint`. [tool-verified: `provisa/compiler/schema_gen.py:304`] A tabela de junção continua consultável como seu próprio campo raiz e sai do lado dos nós do esquema do grafo Cypher, de modo que nunca aparece como rótulo de nó.

`materialize: true` funciona em uma aresta de junção, e o que ele materializa é a travessia em vez de um join direto `pets`-para-`pets`: a view guarda o salto de origem, o salto da junção, o discriminador e as colunas próprias da junção ao lado das do destino. Como a junção é uma terceira perna do join, se a aresta cruza fontes é julgado sobre as três tabelas — uma junção em uma fonte diferente das duas que ela liga é materializada mesmo quando essas duas coincidem. Uma declaração materializa um tipo de aresta, então uma view construída para `bonded pair` nunca responde a uma travessia `littermate`.

Valores de cardinalidade [tool-verified: `provisa/core/models.py` `Cardinality` enum, lines 79–81]:

- `many-to-one` — cada linha de origem mapeia para uma linha alvo (FK para PK)
- `one-to-many` — cada linha de origem mapeia para múltiplas linhas alvo (inverso do acima)

---

## Regras de Segurança em Nível de Linha {: #row-level-security-rules }

Regras de RLS injetam cláusulas `WHERE` no momento da consulta, escopadas a uma função e opcionalmente a uma tabela ou domínio. [tool-verified: `provisa/core/models.py` `RLSRule` class lines 391–395; `config/provisa.yaml` lines 128–131] (REQ-041)

```yaml
rls_rules:

  - table_id: orders          # applies to orders table only
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"

  - domain_id: sales-analytics  # applies to every table in domain (REQ-402)
    role_id: analyst
    filter: "tenant_id = current_setting('provisa.tenant_id')"
```

Quando existem tanto uma regra em nível de domínio quanto uma regra em nível de tabela para a mesma função, a regra em nível de tabela tem precedência (REQ-403).

| Campo | Obrigatório | Descrição |
| ------- | ---------- | ------------- |
| `table_id` | Condicional | Tabela à qual a regra se aplica; mutuamente exclusiva com `domain_id` |
| `domain_id` | Condicional | Domínio ao qual a regra se aplica; aplica-se a todas as tabelas no domínio (REQ-402) |
| `role_id` | Sim | Função à qual esta regra se aplica |
| `filter` | Sim | Predicado SQL injetado em `WHERE`; pode referenciar variáveis de sessão (REQ-041) |

---

## Funções e Webhooks {: #functions-and-webhooks }

### Funções de BD {: #db-functions }

Rastreie uma função de banco de dados e a exponha como uma consulta ou mutação GraphQL. [tool-verified: `provisa/core/models.py` `Function` class lines 423–438; `config/provisa.yaml` lines 152–164] (REQ-205)

Fontes de banco de dados também podem auto-descobrir seus procedimentos armazenados e funções a partir do catálogo do fornecedor (`pg_proc`, `information_schema.routines`, ou equivalentes do fornecedor), removendo a necessidade de registrar cada uma manualmente. A descoberta lê `prokind` e `provolatile`: funções imutáveis/estáveis se registram como relações parametrizadas (argumentos do procedimento tornam-se parâmetros de consulta, a mesma forma das tabelas GET do OpenAPI), e procedimentos voláteis se registram como mutações/funções rastreadas. Rotinas descobertas fluem pela governança de Estágio 2 identicamente às registradas manualmente. [tool-verified: `provisa/api/admin/introspect.py:541`, `provisa/api/admin/introspect.py:593`] (REQ-887)

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
| `returns` | Sim | — | ID da tabela registrada que a função retorna (REQ-207) |
| `arguments` | Não | `[]` | Lista de definições de argumento `{name, type}` (REQ-211) |
| `visible_to` | Não | `[]` | Funções às quais a função está atribuída (vazio: todas as funções). Uma `mutation` exige também o direito `write` |
| `domain_id` | Não | `""` | Domínio ao qual esta função pertence |
| `description` | Não | `null` | Descrição do campo GraphQL |
| `kind` | Não | `mutation` | `"query"` ou `"mutation"` (REQ-205) |

### Webhooks

Exponha um endpoint HTTP externo como uma consulta ou mutação GraphQL. [tool-verified: `provisa/core/models.py` `Webhook` class lines 441–455; `config/provisa.yaml` lines 166–178] (REQ-209)

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
| `inline_return_type` | Não | `[]` | Lista de campos `{name, type}` para formatos de retorno personalizados (REQ-210) |
| `arguments` | Não | `[]` | Lista de definições de argumento `{name, type}` |
| `visible_to` | Não | `[]` | Funções que podem chamar este webhook |
| `domain_id` | Não | `""` | Domínio ao qual este webhook pertence |
| `description` | Não | `null` | Descrição do campo GraphQL |
| `kind` | Não | `mutation` | `"query"` ou `"mutation"` |

---

## Autenticação {: #authentication }

A autenticação é configurada sob a chave `auth`. [tool-verified: `provisa/core/models.py` `AuthConfig` class lines 467–477] (REQ-120)

| Provedor | Descrição |
| ---------- | ------------- |
| `none` | Sem autenticação; todas as requisições tratadas como o `default_role` |
| `firebase` | Firebase Authentication; exige `project_id` e `service_account_key` (REQ-121) |
| `keycloak` | Keycloak OIDC (REQ-122) |
| `oauth` | OAuth 2.0 genérico (REQ-123) |
| `simple` | Usuário/senha sem um provedor externo (REQ-124) |

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

`assignments_source: claims` lê atribuições de função das claims JWT. `assignments_source: provisa` as lê do próprio armazenamento de atribuições do Provisa. [tool-verified: `provisa/core/models.py` line 476] (REQ-551)

---

## Roteamento de Execução {: #execution-routing }

**Execução direta** — Consultas RDBMS de fonte única roteiam para o driver nativo para latência sub-100ms (REQ-027). Fontes exigem tanto uma entrada `SOURCE_TO_DIALECT` quanto uma entrada `SOURCE_TO_CONNECTOR` para suportar este caminho (REQ-229).

**Execução federada** — Consultas multi-fonte e fontes sem driver direto roteiam através do motor de federação (REQ-028). O Provisa inclui um motor de federação embutido; aponte para seu próprio cluster compatível para implantações em grande escala (REQ-226).

**Estatísticas** — No registro, o Provisa executa `ANALYZE` contra cada tabela publicada para preparar o otimizador baseado em custo (contagens de linha, fração de nulos, valores distintos, mín/máx). Falhas são registradas e não bloqueiam o registro (REQ-275).

---

## Fontes de Grafo e Semânticas {: #graph-semantic-sources }

### Neo4j

Registre um banco de dados de grafo Neo4j como uma fonte consultável. Stewards escrevem consultas Cypher que projetam valores escalares; o Provisa armazena os resultados em cache e os expõe como tipos GraphQL (REQ-295).

Consultas Cypher devem usar acessadores de propriedade na cláusula `RETURN` (`RETURN n.id AS id, n.name AS name`) — retornar objetos de nó é rejeitado no momento do registro (REQ-296).

#### Registro via arquivo de config (REQ-1668) {: #config-file-registration-req-1668 }

Declare uma fonte `neo4j` e suas tabelas em YAML. Cada tabela exige um `query_template` (o Cypher que produz suas linhas) e colunas tipadas. A chave `query_template` é inválida sob qualquer outro tipo de fonte. [tool-verified: `provisa/core/config_loader.py:456-511`]

Uma fonte exige `host`, `port` e `database`. [tool-verified: `provisa/core/config_loader.py:456-468`] Linhas são buscadas fazendo POST de `{"statements": [{"statement": <cypher>}]}` para `/db/<database>/tx/commit` (a API de transação HTTP do Neo4j). Uma resposta com uma lista `errors` não vazia é tratada como uma consulta falha, não como um resultado vazio. [tool-verified: `provisa/neo4j/source.py:71-82`, `provisa/api_source/caller.py:344-347`, `provisa/api_source/normalizers.py:49-74`]

Cada coluna exige `data_type`. O loader mapeia tipos de config para o tipo de coluna de API usado no momento da consulta [tool-verified: `provisa/neo4j/persist.py:31-64`]:

| `data_type` da config | Tipo de API |
|---|---|
| `varchar`, `text`, `string`, `char` | string |
| `integer`, `int`, `bigint`, `smallint` | integer |
| `float`, `double`, `real`, `decimal`, `numeric`, `number` | number |
| `boolean`, `bool` | boolean |
| `json`, `jsonb` | jsonb |

`varchar(N)` e `decimal(10,2)` são aceitos — o tipo base antes do parêntese é usado.

O registro persiste uma linha `api_sources` e uma linha `api_endpoints` por tabela, então as tabelas sobrevivem a um restart sem reler o arquivo. Os endpoints REST de admin sob `/admin/sources/neo4j` escrevem as mesmas linhas. [tool-verified: `provisa/neo4j/persist.py:77-120`]

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

#### Register Table na UI (REQ-1670) {: #register-table-in-the-ui-req-1670 }

Uma fonte neo4j não tem tabelas para listar, então o formulário Register Table pede a tabela em vez de oferecer uma. [tool-verified: `provisa-ui/src/pages/tables/RegisterTableForm.tsx` (`isNeo4j`)]

1. Escolha a fonte neo4j e um domínio. Os seletores de esquema e tabela, a checkbox discover e o seletor de watermark não aparecem; a fonte nunca é introspectada.
2. Digite um nome de tabela e o Cypher. O Cypher precisa projetar escalares (`RETURN a.name AS name`); uma projeção que retorna um nó ou uma lista é reportada como erro.
3. Pressione Preview. O formulário roda o Cypher com `LIMIT 5` através da consulta GraphQL `neo4jPreview` e preenche a lista de colunas a partir das linhas retornadas, tipadas como `text`, `integer`, `double`, `boolean` ou `json`. [tool-verified: `provisa/api/admin/_neo4j_registration.py` `preview_neo4j`] Um preview falho mantém o Cypher no editor e mostra a mensagem.
4. Ajuste visibilidade, aliases ou mascaramento como para qualquer tabela, então registre. O formulário se recusa a enviar até que um preview tenha tipado as colunas, e o servidor recusa uma tabela neo4j que não carrega Cypher (`schema.neo4j_query_required`). [tool-verified: `provisa/api/admin/schema_mutation_ops.py` `persist_neo4j_registration`]

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

O endpoint de preview (`POST /admin/sources/neo4j/{id}/preview`) retorna linhas de amostra e bloqueia o registro se o Cypher retornar objetos de nó (REQ-296).

### SPARQL

Registre qualquer triplestore compatível com SPARQL 1.1 (Apache Jena Fuseki, Virtuoso, Stardog, etc.) como uma fonte consultável (REQ-297).

As consultas devem ser consultas `SELECT`. Nomes de variável na cláusula `SELECT` tornam-se nomes de coluna automaticamente (REQ-297).

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

Ambos os conectores usam o pipeline de cache de fonte de API — os resultados são armazenados no PostgreSQL com TTL configurável, tornando-os disponíveis para JOINs federados entre fontes (REQ-295, REQ-297, REQ-299).

---

#### Registro via arquivo de config e UI (REQ-1683) {: #config-file-and-ui-registration-req-1683 }

O `host` de uma fonte `sparql` é a URL do seu endpoint SPARQL (o formulário de Fontes o armazena da mesma forma). Cada tabela sob ela carrega `query_template`, um SELECT cujas variáveis são as colunas; todo binding é `text`. [tool-verified: `provisa/core/config_loader.py` `_validate_neo4j_sources`, `_handle_sparql_table`]

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

Register Table funciona da mesma forma que para Neo4j: escolha a fonte, digite um nome de tabela e o SELECT, pressione Preview (a consulta `sparqlPreview` a roda com `LIMIT 5` e preenche a lista de colunas), então registre. O registro persiste uma linha `api_sources` e uma linha `api_endpoints` (POST form-encoded para o caminho do endpoint, normalizador `sparql_bindings`), as mesmas linhas que um registro via arquivo de config escreve, e o motor nativo pousa as linhas através da mesma cadeia de busca que o Neo4j. [tool-verified: `provisa/api/admin/_query_api_registration.py`, `provisa/sparql/persist.py`]

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

Porções de fonte única roteiam diretamente (REQ-027). JOINs entre fontes federam com coerção automática de tipo (REQ-028, REQ-552).
