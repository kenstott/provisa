# Tipos de Fonte

## Modelo de Execução

Toda consulta em última análise é executada através do motor de federação, que fornece federação entre todas as fontes. Fontes se dividem em três categorias com base em sua conectividade. [tool-verified: `provisa/core/models.py` lines 84–132] (REQ-550)

| Categoria | Tem Driver Direto | Tem Conector Federado | Exemplos |
| --- | --- | --- | --- |
| **Direta-capaz** | Sim | Sim | PostgreSQL, MySQL, MariaDB, SingleStore, SQL Server, Oracle, DuckDB |
| **Somente federação** | Não | Sim | Redshift, Druid, Exasol, Hive, Iceberg, Delta Lake, Hive (apoiado em S3) |
| **Leitura direta (réplica)** | Sim | Sim | Snowflake, Databricks, ClickHouse — o driver lê os dados e pousa uma réplica; consultas rodam contra a réplica no motor ativo |
| **Materializar → Federação** | Não | Não | REST/OpenAPI, GraphQL remoto, gRPC, Neo4j Cypher, SPARQL, WebSocket, RSS, CSV, SQLite, Parquet, Ingest (receptor push), GovData, SharePoint, Splunk |

Fontes **direta-capazes** executam consultas de fonte única via seu driver nativo (sub-100ms), contornando o motor de federação (REQ-027, REQ-229). Elas mantêm suporte completo a conectores e participam de federação quando unidas com outras fontes (REQ-028).

Fontes **somente federação** são sempre consultadas através da camada de federação. Nenhum driver direto existe (REQ-229).

Fontes de **leitura direta (réplica)** têm um DirectDriver que lê do warehouse nativamente (transporte Arrow-nativo quando disponível), pousa uma réplica no armazenamento de materialização do motor ativo, e então as consultas rodam contra essa réplica. Veja [Warehouses como Fontes Nomeadas](#warehouses-como-fontes-nomeadas).

Fontes de **materialização** não têm conector federado. O Provisa busca seus dados (na inicialização ou no momento da consulta) e os armazena em cache como Parquet no S3 ou no PostgreSQL, tornando-os alcançáveis pelo motor de federação para consultas entre fontes (REQ-309).

---

## Todas as Fontes

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

Bancos de dados compatíveis com o protocolo de fio reutilizam o driver JDBC, o driver assíncrono nativo, e o dialeto de um fio base — CockroachDB, YugabyteDB, e Greenplum usam o fio PostgreSQL; TiDB usa o fio MySQL. Eles precisam apenas de entradas no registro, sem novo código de conector. [tool-verified: `provisa/core/source_registry.py` `_PG_WIRE_TYPES`, `_MYSQL_WIRE_TYPES`] (REQ-950)

`firebird` (Firebird 3/4/5) e `airport` (servidor Arrow Flight) são tipos de fonte registrados alcançados no local via extensões da comunidade DuckDB quando o DuckDB é o motor ativo — sem driver direto, sem conector federado. [tool-verified: `provisa/core/models.py` lines 44, 93] (REQ-899)

### Data Warehouses em Nuvem

[tool-verified: `executor/drivers/snowflake.py`, `executor/drivers/databricks.py`, `executor/drivers/registry.py`]

| Tipo de Fonte | Driver Direto | Nome do Conector | Dialeto | Mutações | Notas |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `snowflake` | SnowflakeDriver | snowflake | snowflake | Federada | Lê via snowflake-connector-python; pousa réplica; `account`/`warehouse`/`role` em `federation_hints` (REQ-988) |
| `bigquery` | — | bigquery | bigquery | Federada | Sem DirectDriver; alcançado via motor de federação ou ATTACH do motor BigQuery |
| `databricks` | DatabricksDriver | delta_lake | databricks | Federada | Lê via databricks-sql-connector (Cloud Fetch, Arrow); pousa réplica; `http_path` exigido em `federation_hints` (REQ-987) |
| `redshift` | — | redshift | redshift | Federada | — |
| `fabric` | MssqlWarehouseDriver | — | tsql | Federada | Microsoft Fabric Warehouse; T-SQL sobre TDS, autenticação Azure AD; pousa réplica (REQ-995) |
| `synapse` | MssqlWarehouseDriver | — | tsql | Federada | Azure Synapse SQL; T-SQL sobre TDS, autenticação Azure AD; pousa réplica (REQ-995) |
| `trino` | SQLAlchemyDriver | — | — | Federada | Coordenador Trino/Presto remoto lido via o dialeto SQLAlchemy trino; pousa réplica em qualquer motor (REQ-994) |

### Analytics / OLAP

[tool-verified: `executor/drivers/clickhouse.py`]

| Tipo de Fonte | Driver Direto | Nome do Conector | Dialeto | Mutações | Notas |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `clickhouse` | ClickHouseDriver | clickhouse | clickhouse | Federada | Lê via clickhouse-connect (HTTP); `secure: "true"` em `federation_hints` para TLS (REQ-986) |
| `druid` | — | druid | druid | Não | — |
| `exasol` | — | exasol | exasol | Não | — |
| `elasticsearch` | — | elasticsearch | — | Não | Propriedades do conector vêm da DSL de mapeamento do tipo [tool-verified: `trino_connectors.py:309`] |
| `pinot` | — | pinot | — | Não | Conector Trino `pinot`; `pinot.controller-urls` = host:porta do controlador Pinot [tool-verified: `trino_connectors.py:199`] |

### Data Lake / Formatos de Tabela Aberta

Esses tipos de fonte são somente federação — sem driver direto, sem dialeto. [tool-verified: `LAKE_ONLY_SOURCES` em `provisa/core/source_registry.py`] (REQ-229)

| Tipo de Fonte | Nome do Conector | Time Travel | Notas |
| ------------ | ----------------- | ------------- | ------- |
| `iceberg` | iceberg | Sim (argumento `as_of`, REQ-372) | — |
| `delta_lake` | delta_lake | Sim (argumento `as_of`, REQ-372) | — |
| `hive` | hive | Não | — |
| `hive_s3` | hive | Não | Hive apoiado em S3 |

### NoSQL

`mongodb`, `cassandra`, e `redis` têm conectores Trino (`redis` constrói suas propriedades a partir da DSL de mapeamento do tipo). [tool-verified: `provisa/federation/trino_connectors.py`; `provisa/core/models.py`] (REQ-017, REQ-1097)

| Tipo de Fonte | Nome do Conector | Mutações |
| ------------ | ----------------- | ----------- |
| `mongodb` | mongodb | Não |
| `cassandra` | cassandra | Não |
| `redis` | redis | Não |

### Streaming

| Tipo de Fonte | Mecanismo | Mutações |
| ------------ | ----------- | ----------- |
| `kafka` | Conector Kafka federado; esquema via Confluent Schema Registry (Avro, Protobuf, JSON Schema), definição manual, ou inferência de amostra (REQ-147, REQ-150) | Somente sink (REQ-176) |
| `websocket` | Feed WebSocket externo — conecta, subscreve, recebe eventos; resultados materializados (REQ-338) | Não |
| `rss` | Feed RSS 2.0 / Atom — polling, marca d'água por pubDate/updated; resultados materializados (REQ-342, REQ-343) | Não |

### Receptor Push

| Tipo de Fonte | Mecanismo | Mutações |
| ------------ | ----------- | ----------- |
| `ingest` | Serviços externos fazem POST de eventos JSON; resultados materializados (REQ-331, REQ-335) | Não |

### Grafo e Semântico

| Tipo de Fonte | Mecanismo | Mutações |
| ------------ | ----------- | ----------- |
| `neo4j` | Cypher via API HTTP, resultados em cache no PostgreSQL (REQ-295) | Não |
| `sparql` | SPARQL 1.1 POST, resultados em cache no PostgreSQL (REQ-297) | Não |

### Baseado em Arquivo

Dois mecanismos cobrem arquivos. Ambos usam o campo `path` em vez de `host`/`port`. [tool-verified: `provisa/core/models.py`] (REQ-553)

**Fontes de arquivo único** — `sqlite`, `csv`, `parquet` apontam `path` para um arquivo.

| Tipo de Fonte | Transportes | Mutações |
| --- | --- | --- |
| `sqlite` | local | Sim |
| `csv` | local | Não |
| `parquet` | local, `s3://` | Não |

Buckets privados precisam de credenciais (região e chaves AWS do ambiente). Para CSV via `s3://` ou `http(s)://`, ou para registrar muitos arquivos de uma vez, use a fonte `files`. [tool-verified: `provisa/file_source/source.py`]

**Fonte `files`** — aponta `path` para um glob, o percorre recursivamente, e registra o diretório como um catálogo federado de tabelas. Ele lê muitos formatos por muitos transportes; os conjuntos abaixo vêm do conector de arquivo (fork kenstott/calcite). [tool-verified: `provisa/core/catalog.py` branch `files` e `provisa/core/models.py` `SOURCE_TO_CONNECTOR`; listas de formato e transporte do adaptador calcite `file` — `FileSchema.java`, `storage/StorageProviderFactory.java`]

| Formatos | Transportes |
| --- | --- |
| CSV, TSV, JSON, YAML, Excel (XLS/XLSX), Parquet, Arrow, e documentos convertidos em tabelas — HTML, Markdown, DOCX, PPTX | Sistema de arquivos local, HTTP(S), `s3://`, `hdfs://`, `ftp://`/`ftps://`, `sftp://`, `iceberg://`, SharePoint (REST e Microsoft Graph) |

```yaml
- id: sales_files
  type: files
  path: s3://bucket/sales/**/*.csv   # glob; local and http(s):// also supported
```

### Observabilidade e Outros

`prometheus` tem um conector Trino (propriedades construídas a partir da DSL de mapeamento do tipo). `google_sheets` é um tipo de fonte registrado sem conector Trino e materializa através do pipeline de cache de API. [tool-verified: `provisa/federation/trino_connectors.py:314`; `provisa/core/models.py` lines 87–88]

| Tipo de Fonte | Nome do Conector | Mutações |
| ------------ | ----------------- | ----------- |
| `google_sheets` | — (materializada) | Não |
| `prometheus` | prometheus | Não |

### Conectores SaaS Empresariais

SharePoint e Splunk se registram através de conectores Apache Calcite (fork kenstott/calcite). Nenhum tem driver direto — o Provisa materializa suas linhas lançando o servidor pgwire Calcite embutido do conector (`pgwire-sharepoint`, `pgwire-splunk`), conectando-se a ele como um endpoint PostgreSQL genérico, e pousando as linhas no armazenamento de materialização para federação (REQ-954). Ambos os conectores sempre habilitam correspondência de nome insensível a maiúsculas/minúsculas, correspondendo à semântica insensível a maiúsculas/minúsculas própria de cada produto (REQ-725, REQ-730). [tool-verified: `provisa/core/models.py` lines 99–100; `provisa/federation/trino_connectors.py` lines 223–286]

#### `sharepoint`

Listas do SharePoint são enumeradas como esquemas e expostas como tabelas consultáveis (REQ-726, REQ-731). Dois métodos de autenticação: `CLIENT_CREDENTIALS` (padrão) e baseado em certificado via um certificado PFX (REQ-727). Valores secretos em `mapping` são resolvidos através do motor de segredos antes de alcançar o conector (REQ-729). [tool-verified: `provisa/federation/trino_connectors.py` lines 230–252]

| Campo da fonte | Propriedade do conector | Notas |
| --- | --- | --- |
| `base_url` ou `host` | `site-url` | URL do site SharePoint |
| `username` | `client-id` | ID do cliente do app Azure |
| `password` | `client-secret` | Segredo do cliente do app Azure |
| `database` | `tenant-id` | UUID do tenant Azure |
| `mapping.auth_type` | `auth-type` | `CLIENT_CREDENTIALS` (padrão) ou `CERTIFICATE` |
| `mapping.certificate_path` | `certificate-path` | Caminho PFX quando `auth_type: CERTIFICATE` |
| `mapping.certificate_password` | `certificate-password` | Senha PFX |

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

### Fontes de API

Registre qualquer endpoint HTTP como uma tabela consultável. [tool-verified: `provisa/core/models.py` `SourceType` enum] (REQ-314, REQ-307, REQ-322)

| Tipo de API | Descoberta | Inferência de Coluna |
| --------- | ----------- | ----------------- |
| `openapi` | Parse de especificação OpenAPI (REQ-314, REQ-316) | Primitivos → nativo, objetos → JSONB |
| `graphql_remote` | Introspecção de esquema (REQ-307, REQ-308) | Primitivos → nativo, objetos → JSONB |
| `grpc_remote` | Reflexão de servidor (REQ-322, REQ-325) | Primitivos → nativo, objetos → JSONB |

Respostas de API são buscadas, armazenadas em cache no PostgreSQL (TTL configurável), e expostas como tipos GraphQL (REQ-309, REQ-318, REQ-327). Tabelas em cache participam de consultas federadas como qualquer outra fonte (REQ-313).

**Regras JSONB**: Colunas complexas (objetos, arrays) armazenadas como JSONB não são filtráveis (REQ-119). O acesso a subcampo usa extração `->>` em SQL (REQ-151). Relacionamentos são declarados entre tabelas usando colunas FK escalares — colunas de blob JSONB não são alvos de join. Use a promoção de JSONB para converter campos aninhados em colunas escalares nativas quando filtrar ou unir sobre eles for necessário (REQ-119).

### GovData

Dados abertos do governo dos EUA. O acesso é particionado por agrupamento de assunto. [tool-verified: `provisa/core/models.py` lines 543–609]

Cada fonte `govdata` seleciona um assunto. Esse assunto determina quais esquemas GovData são expostos. Os esquemas `ref` e `geo` são sempre incluídos como esquemas de ligação — eles não são listados por assunto mas estão sempre presentes. [tool-verified: `provisa/core/models.py` line 562–563 comment]

| Assunto | Esquemas Expostos |
| --------- | ----------------- |
| `COMMERCE` | `sec`, `patents` |
| `ECONOMY` | `econ` |
| `EDUCATION` | `census`, `edu` |
| `HEALTH` | `health` |
| `CYBER` | `cyber_threat`, `cyber_vuln` |
| `PUBLIC_SAFETY` | `crime` |
| `ENVIRONMENT` | `lands` |
| `WEATHER` | `weather` |
| `GOVERNMENT` | `fedregister`, `fec` |
| `ALL` | Todos os esquemas acima |

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

### Verificadores de Qualidade de Dados (REQ-1443)

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

## Conectores Personalizados (REQ-1177)

Os motores de federação nativos — Postgres, DuckDB, e ClickHouse — ganham alcance a um novo tipo de fonte quando um operador declara um conector para ele em `config/custom_connectors.yaml`. Nenhum código é exigido. [tool-verified: `provisa/federation/custom_connectors.py` `load_custom_connectors`; `provisa/federation/engine.py` `build_pg_engine`, `build_duckdb_engine`, `build_clickhouse_engine`]

A extensibilidade de conector em si já existia antes disso. O motor Trino há muito é extensível em sua própria camada — um conector JDBC genérico parametrizado por tipo de fonte, um corpo `.properties` de catálogo por tipo, e os próprios plugins de conector Trino personalizados do Provisa (Splunk, SharePoint, Calcite). [tool-verified: `provisa/federation/trino_connectors.py` `_TrinoJdbcConnector`, `_TRINO_JDBC_TYPES`; `trino/plugins/trino-splunk`, `trino/plugins/trino-sharepoint`, `trino/plugins/trino-calcite`] O REQ-1177 traz essa mesma extensibilidade orientada por config aos dois motores nativos sem cluster, que anteriormente carregavam um conjunto fixo de conectores.

A config é entregue vazia. Conectores embutidos cobrem o alcance pronto para uso; tudo neste arquivo é escrito pelo operador. [tool-verified: `config/custom_connectors.yaml` line 52: `connectors: []`] Defina `PROVISA_CUSTOM_CONNECTORS` para apontar para um caminho diferente (útil para testes).

### Tipos de descritor

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

### Gate de sondagem

A disponibilidade é verificada no momento do attach contra o catálogo de descoberta padrão de cada motor:

- **Postgres** — verifica `pg_extension`, depois `pg_available_extensions`. [tool-verified: `provisa/federation/connector_duckdb.py` `_probe_pg_extension` lines 333–344]
- **DuckDB** — executa `INSTALL`/`LOAD` e verifica `duckdb_functions()` pelo `probe_symbol` declarado. [tool-verified: `provisa/federation/connector_duckdb.py` `_DuckDBExtensionConnector.probe` lines 160–180]
- **ClickHouse** — verifica `system.table_engines` pelo `ch_engine` declarado; ausente da build falha alto. [tool-verified: `provisa/federation/custom_connectors.py` `_probe_clickhouse_engine`]

Uma extensão declarada que não é instalável falha alto. Sem skip silencioso, sem fallback. Um conector cuja sondagem falha simplesmente não fica ativo para aquela implantação.

### Variáveis de template

Todo valor `server_options`, valor `user_mapping`, `attach_template`, e `scan_template` pode usar placeholders `{field}`. Campos disponíveis: [tool-verified: `provisa/federation/custom_connectors.py` `_source_fields` lines 53–63]

`{id}`, `{host}`, `{port}`, `{database}`, `{username}`, `{password}`, `{path}`, `{schema_name}`, `{table_name}`, mais qualquer chave de `federation_hints`. Templates de attach do DuckDB também recebem `{alias}` — o alias interno de catálogo que o Provisa atribui ao banco de dados anexado.

Um template que referencia um campo desconhecido falha alto no momento do attach, expondo uma incompatibilidade descritor/fonte antes que um DDL quebrado alcance o motor.

### Exemplos

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

## Warehouses como Fontes Nomeadas

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

## Campos de Configuração de Fonte

Todas as fontes compartilham um conjunto comum de campos. [tool-verified: `provisa/core/models.py` `Source` class, lines 138–204]

| Campo | Obrigatório | Padrão | Descrição |
| ------- | ---------- | --------- | ------------- |
| `id` | Sim | — | Identificador único; alfanumérico com hífens/underscores |
| `type` | Sim | — | Tipo de fonte (veja tabelas acima) |
| `host` | Não | `""` | Hostname ou IP |
| `port` | Não | `0` | Número da porta |
| `database` | Não | `""` | Nome do banco de dados |
| `username` | Não | `""` | Usuário |
| `password` | Não | `""` | Senha; use `${env:VAR}` para resolução de segredo |
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
| `mapping` | Não | `{}` | Configurações de conector específicas de tipo para fontes NoSQL e SaaS (ex.: `auth_type` do SharePoint, `use_token` do Splunk) (REQ-251) |
| `allowed_domains` | Não | `[]` | Restringe a fonte a domínios específicos; vazio = irrestrito |
| `description` | Não | `""` | Descrição legível por humanos |

---

## Fontes Kafka

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

## Visibilidade de Coluna

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

## Relacionamentos

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
| `materialize` | Não | Cria automaticamente uma view materializada para joins entre fontes (REQ-158) |
| `refresh_interval` | Não | Intervalo de atualização de MV em segundos (padrão: 300) |
| `target_function_name` | Não | Nome da função do BD para relacionamentos computados |
| `function_arg` | Não | Qual argumento da função recebe o valor da coluna de origem |
| `alias` | Não | Tipo de relacionamento legível por humanos (ex.: `WORKS_FOR`) |
| `graphql_alias` | Não | Nomeia o campo SDL que este relacionamento expõe no tipo pai. Quando ausente, o nome é derivado do `field_name` da tabela alvo e da cardinalidade do relacionamento. [tool-verified: `provisa/compiler/schema_gen.py:1050`] |
| `disable_cypher` | Não | Quando `true`, exclui este relacionamento das arestas do grafo Cypher |
| `source_json_key` | Não | Extrai esta chave da coluna de origem como um objeto JSON antes do JOIN |

Valores de cardinalidade [tool-verified: `provisa/core/models.py` `Cardinality` enum, lines 79–81]:

- `many-to-one` — cada linha de origem mapeia para uma linha alvo (FK para PK)
- `one-to-many` — cada linha de origem mapeia para múltiplas linhas alvo (inverso do acima)

---

## Regras de Segurança em Nível de Linha

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

## Funções e Webhooks

### Funções de BD

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
| `visible_to` | Não | `[]` | Funções que podem chamar esta função |
| `writable_by` | Não | `[]` | Funções que podem chamar isto como uma mutação |
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

## Autenticação

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

## Roteamento de Execução

**Execução direta** — Consultas RDBMS de fonte única roteiam para o driver nativo para latência sub-100ms (REQ-027). Fontes exigem tanto uma entrada `SOURCE_TO_DIALECT` quanto uma entrada `SOURCE_TO_CONNECTOR` para suportar este caminho (REQ-229).

**Execução federada** — Consultas multi-fonte e fontes sem driver direto roteiam através do motor de federação (REQ-028). O Provisa inclui um motor de federação embutido; aponte para seu próprio cluster compatível para implantações em grande escala (REQ-226).

**Estatísticas** — No registro, o Provisa executa `ANALYZE` contra cada tabela publicada para preparar o otimizador baseado em custo (contagens de linha, fração de nulos, valores distintos, mín/máx). Falhas são registradas e não bloqueiam o registro (REQ-275).

---

## Fontes de Grafo e Semânticas

### Neo4j

Registre um banco de dados de grafo Neo4j como uma fonte consultável. Stewards escrevem consultas Cypher que projetam valores escalares; o Provisa armazena os resultados em cache e os expõe como tipos GraphQL (REQ-295).

Consultas Cypher devem usar acessadores de propriedade na cláusula `RETURN` (`RETURN n.id AS id, n.name AS name`) — retornar objetos de nó é rejeitado no momento do registro (REQ-296).

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

## Exemplos de Conexão

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

### Consulta Entre Fontes

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
