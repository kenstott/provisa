# Arquitetura do Provisa {: #provisa-architecture }

## Visão geral {: #overview }

O Provisa é uma plataforma de virtualização de dados orientada por configuração: federação, métricas, ontologia e governança a partir de um único modelo, para pequenas equipes e grandes empresas. Ele fornece uma API unificada sobre fontes de dados heterogêneas com governança, segurança e otimização de desempenho. Clientes consultam via SQL, GraphQL ou Cypher; todas as três são interfaces de primeira classe com a mesma governança aplicada.

O modelo é o único lugar onde se acrescenta significado. Para adicionar ao modelo, você registra novas fontes de dados ou agregações dentro da camada de virtualização de dados. Isso cria uma separação limpa — nenhuma nova adição à semântica pode ser feita fora da plataforma, permitindo governança de dados verdadeira. A aplicação ocorre no nível do compilador: o catálogo de relacionamentos aprovado é a fonte da verdade independentemente de qual linguagem de consulta é usada.

O Provisa é projetado para ser altamente performático para necessidades operacionais e altamente escalável para necessidades analíticas empresariais. Uma única plataforma atende a ambas sem sacrificar velocidade ou escalabilidade.

```text
Config YAML → PG Metadata → Federation Catalogs
                               ↓
         Federation engine metadata → Schema Generator → SDL / SQL catalog / Cypher labels / gRPC proto (per role)
                                     ↓
                     Query → Parser → SQL Compiler → Transpiler
                                     ↓
                             Router (Smart Dispatch)
                         /           |            \
                    Federation  Direct PG      Direct MySQL/etc.
                         \           |            /
                              Executor Pool
                                     ↓
                         ┌───── Inline ─────┐     ┌──── Redirect ────┐
                         │  JSON (HTTP)     │     │  CTAS → S3       │
                         │  Arrow (Flight)  │     │  (Parquet, ORC)  │
                         │  Protobuf (gRPC) │     │  Provisa → S3    │
                         └─────────────────-┘     │  (JSON, CSV, …)  │
                                                  └─────────────────-┘
```

## Interfaces de Consulta {: #query-interfaces }

Cada interface é um transporte distinto. Todas as quatro aplicam o mesmo pipeline de segurança (RLS, mascaramento, amostragem, verificações de função). Clientes nunca falam diretamente com o motor de federação. A "linguagem de consulta" (SQL / GraphQL / Cypher) é ortogonal ao transporte — múltiplas linguagens podem chegar pelo mesmo transporte.

| Porta | Transporte | Linguagens de consulta aceitas | Caso de uso |
| ------ | ----------- | -------------------------- | ---------- |
| 8001 | HTTP | GraphQL, SQL, Cypher | Clientes web, ferramentas de BI, curl, consumidores REST |
| 8815 | Arrow Flight (gRPC) | SQL (via Arrow Flight SQL) | Ferramentas de dados (Pandas, DuckDB, Spark, ADBC) |
| 50051 | Protobuf gRPC | RPCs proto gerados por função | Serviço-a-serviço com contratos tipados |
| configurável¹ | Protocolo de fio PostgreSQL (pgwire) | SQL | psql, DBeaver, SQLAlchemy, qualquer cliente compatível com PG |

¹ Defina `PROVISA_PGWIRE_PORT` (ex.: 5433). Desabilitado quando não definido ou `0`.

### HTTP (porta 8001) {: #http-port-8001 }

Múltiplos endpoints sob a mesma porta, distinguidos por caminho:

| Caminho | Linguagem | Notas |
| ------ | ---------- | ------- |
| `POST /data/graphql` | GraphQL | Leituras e mutações; hash APQ aceito via `extensions.persistedQuery` |
| `POST /data/sql` | SQL | Somente leitura; sem gate de capacidade — governado por visibilidade de objeto + RLS + mascaramento |
| `POST /data/query` | Cypher | Somente leitura; função padrão |
| `GET /data/nl` | Linguagem natural | Traduz para SQL/GraphQL/Cypher com base no tipo de fonte |
| `GET /data/subscribe/{table}` | GraphQL | Stream de subscription SSE |
| `GET /neo4j/...` | Cypher (compat Neo4j) | Shim de compatibilidade com a API HTTP do Neo4j |
| `POST /admin/graphql` | GraphQL | API de administração (função superuser/admin exigida) |

Todos os caminhos retornam JSON por padrão. `Accept: text/csv`, `application/vnd.apache.parquet`, `application/vnd.apache.arrow.stream`, e `application/octet-stream` (binário bruto) são suportados via negociação de conteúdo. Resultados que excedem o limite de tamanho configurado são automaticamente redirecionados para uma URL S3 assinada.

### Arrow Flight (porta 8815) {: #arrow-flight-port-8815 }

Transporte colunar Arrow nativo sobre gRPC. Clientes enviam um ticket JSON:

```json
{"query": "SELECT name, email FROM customers", "role": "analyst"}
```

e recebem RecordBatches Arrow em stream de forma preguiçosa. Quando o proxy Zaychik Flight SQL está disponível, os dados fluem como um stream de record batches Arrow de ponta a ponta:

```text
Client ←(Arrow batches)← Provisa Flight Server ←(Arrow batches)← Zaychik ←(JDBC)← Federation Engine
```

O resultado completo nunca é materializado na memória do Provisa — os batches são encaminhados conforme chegam. Isso torna o Arrow Flight um caminho ilimitado, adequado para resultados arbitrariamente grandes.

### Protobuf gRPC (porta 50051) {: #protobuf-grpc-port-50051 }

`.proto` auto-gerado a partir do esquema de dados, gerado por função. Consultas em streaming (uma mensagem por linha), mutações unárias. Reflexão de servidor habilitada. Função via chave de metadados `x-provisa-role`. A reflexão descreve a cada chamador o serviço da sua própria função.

### Protocolo de fio PostgreSQL / pgwire (porta configurável) {: #postgresql-wire-protocol-pgwire-configurable-port }

Implementa o protocolo de fio frontend/backend do PostgreSQL usando a biblioteca `buenavista`. Qualquer cliente compatível com PostgreSQL — `psql`, DBeaver, SQLAlchemy com `psycopg2`, JDBC — pode se conectar sem modificação. Aceita apenas SQL. O pipeline de governança completo (RLS, mascaramento, permissões de domínio) se aplica identicamente às conexões pgwire. Habilitado definindo `PROVISA_PGWIRE_PORT` para uma porta diferente de zero.

## Pipeline de Requisição {: #request-pipeline }

Três linguagens de consulta são aceitas. Todas convergem na governança após suas respectivas etapas de parse/compilação. Apenas GraphQL suporta escritas. Não há gate de capacidade sobre a consulta em si — qualquer identidade autenticada pode consultar em qualquer linguagem, e os dados são governados exclusivamente por visibilidade de objeto, RLS e mascaramento.

| Interface | Leituras | Escritas | Gate de consulta |
| --- | --- | --- | --- |
| GraphQL (`/data/graphql`) | Sim | Sim (mutações) | Nenhum — apenas governança da camada de dados |
| SQL (`/data/sql`) | Sim | Não | Nenhum — apenas governança da camada de dados |
| Cypher (`/data/query`) | Sim | Não | Nenhum — apenas governança da camada de dados |

```mermaid
flowchart TD
    A[GraphQL Request] --> B[Auth / Role Resolution]
    A2[SQL Request] --> B
    A3[Cypher Request] --> B
    B --> E[APQ Hash Check]
    E --> F[Parse & Validate]
    F --> G[Extract Directives / Hints]
    G --> H{Cache Hit?}
    H -- yes --> R
    H -- no --> I{Input Type}
    I -- GraphQL --> I1[Compile → Semantic SQL]
    I -- SQL --> I2[Parse & Validate SQL\nApply Namespace / Source Binding]
    I -- Cypher --> I3[Translate Cypher → SQL\nResolve Node / Rel Mappings]
    I1 --> J[Governance: RLS + Masking + Visibility + Sampling]
    I2 --> J
    I3 --> J
    J --> K[MV Rewrite]
    K --> L{Route}
    L -- Direct --> M[Transpile → Source Dialect\nExecute via Driver]
    L -- Federation --> N[Transpile → Federation SQL\nInject Session Hints\nExecute via Federation Engine / Flight]
    L -- Materialize --> O[Fetch from REST / GraphQL / gRPC\nMaterialize → S3 Parquet\nPost-filter via Federation Engine]
    L -- Mutation --> P[RLS Injection\nTranspile → Source Dialect\nExecute via Driver\nInvalidate Cache + MV\nEmit Change Event]
    M --> Q{Redirect?}
    N --> Q
    O --> Q
    Q -- yes --> S[Upload to S3\nReturn Signed URL]
    Q -- no --> R[Serialize: JSON / CSV / Parquet / Arrow]
    R --> T[Store in Cache]
    T --> U[Return to Client]
    P --> U
```

**Decisões de rota:**

| Rota | Quando |
| --- | --- |
| **Cache** | Hit no cache de resultado — avaliado primeiro, serve o resultado armazenado sem execução |
| **Contagem barata** | Consulta em formato `count(*)` sobre uma fonte não materializada que expõe uma contagem nativa exata — roteada para a chamada de contagem nativa em vez de materializar para contar |
| **Direta** | Fonte única + tem driver nativo + tem conector de federação |
| **Federação** | Federação multi-fonte, ou a fonte tem conector mas não driver |
| **Materializar** | A fonte não tem conector de federação — busca e armazena em cache em S3/PG primeiro |
| **Mutação** | Uma escrita: o driver próprio da fonte quando ela tem um; senão o motor, por meio de um conector que escreve na fonte |

O roteamento consome a saída do estágio de otimização pós-governança, nunca o SQL governado pré-otimização. A governança pode ADICIONAR fontes (predicados de subconsulta RLS); o estágio de otimização pode REMOVÊ-las (inlining de VALUES-CTE de hot-table, reescritas de cache de API, poda de ramos de union). Uma consulta federada que colapsa para uma única fonte ao vivo após o inlining é, portanto, reroteada como direta.

### Consultas Multi-Raiz {: #multi-root-queries }

Consultas GraphQL com múltiplos campos raiz (ex.: `{ orders { id } customers { name } }`) são compiladas em consultas SQL separadas e executadas independentemente. Requisições SQL e Cypher são de raiz única por definição. Os resultados são mesclados em uma única resposta:

- Campos abaixo do limite de redirecionamento são retornados inline em `data`
- Campos acima do limite são redirecionados, com entradas por campo em `redirects`
- Formatos binários (Parquet, Arrow) são suportados apenas para consultas de raiz única

## Caminhos de Execução de Federação {: #federation-execution-paths }

| Caminho | Transporte | Via | Quando usado |
| ------ | ----------- | ----- | ----------- |
| REST | cliente do motor de federação (HTTP :8080) | Consulta direta | Padrão, sempre disponível |
| Flight SQL | `adbc-driver-flightsql` (gRPC :8480) | Proxy Zaychik → JDBC | Quando o Zaychik está rodando |
| CTAS | cliente do motor de federação (HTTP :8080) | Escrita direta, Iceberg para S3 | Redirecionamento Parquet/ORC |

### Proxy Zaychik Arrow Flight SQL {: #zaychik-arrow-flight-sql-proxy }

O motor de federação não suporta nativamente o protocolo Arrow Flight SQL. O [Zaychik](https://github.com/Raiffeisen-DGTL/zaychik-trino-proxy) é um proxy Java que implementa a interface gRPC Arrow Flight SQL, traduz requisições para consultas JDBC, e faz streaming dos resultados de volta como record batches Arrow.

```text
ADBC client → gRPC :8480 → Zaychik → JDBC :8080 → Federation Engine → results → Arrow batches → client
```

O servidor Flight do Provisa (porta 8815) se conecta ao Zaychik como um cliente ADBC, permitindo streaming Arrow de ponta a ponta sem materializar resultados.

### Catálogo de Resultados Iceberg {: #iceberg-results-catalog }

O redirecionamento CTAS usa um conector Iceberg (catálogo `results`) apoiado por um catálogo JDBC na instância PostgreSQL existente. O Iceberg grava arquivos Parquet/ORC diretamente no MinIO/S3 via o sistema de arquivos S3 nativo (`fs.native-s3.enabled=true`).

## Motores de Federação {: #federation-engines }

O Provisa seleciona um motor de federação na inicialização via a variável de ambiente `PROVISA_ENGINE`, a configuração persistida na UI de administração, ou o padrão. Quando nada está definido, o DuckDB é o padrão — totalmente em processo, sem serviço externo. Veja [Configuração](configuration.md#federation-engine) para detalhes de seleção.

Cada motor é uma instância de `FederationEngine` definida em `provisa/federation/engine.py`. A instância possui uma coleção de conectores que determina quais tipos de fonte o motor pode ler ao vivo (ATTACH) versus quais devem pousar no armazenamento de materialização do motor primeiro.

### Classes de driver {: #driver-classes-tool-verified-enginepy-driverclass }

| Classe | Significado | Exemplos |
| ------- | --------- | --------- |
| `BROAD` | Alcança muitos tipos de fonte externa via conectores nativos | Trino |
| `PARTIAL` | Alcança um subconjunto (relacional, arquivos, objeto/lake em nuvem) além de pousar tudo o mais | DuckDB, PostgreSQL, ClickHouse, Databricks, Snowflake, BigQuery, Fabric, Synapse |
| `SELF_ONLY` | Alcança apenas seu próprio armazenamento; toda outra fonte pousa nele | SQLAlchemy |

### Motores disponíveis {: #available-engines-tool-verified-enginepy-engine_builders }

| Chave do motor | Dialeto | MPP | Mecanismo de link externo | Autenticação |
| ----------- | --------- | ----- | ------------------------ | ------ |
| `trino` / `trino-byo` | Trino SQL | Sim | Catálogos Trino (amplo conjunto de conectores) | Credenciais JDBC |
| `pg` | PostgreSQL | Não | FDW / pg_duckdb | Credenciais PostgreSQL |
| `duckdb` | DuckDB | Não | ATTACH nativo de extensão | Nenhuma (em processo) |
| `clickhouse` / `clickhouse-server` | ClickHouse | Sim (shards) | Motores de tabela S3 / IcebergS3 / DeltaLake | Credenciais ClickHouse |
| `snowflake` | Snowflake | Sim | External stage + tabela externa | `PROVISA_ENGINE_URL` |
| `databricks` | Databricks SQL | Sim | Tabelas externas Unity Catalog via REST | Bearer token (`http_path` em `federation_hints`) |
| `bigquery` | BigQuery | Sim (Dremel) | Tabelas externas BigQuery / BigLake | Chave de service account `GOOGLE_APPLICATION_CREDENTIALS` |
| `fabric` | T-SQL | Sim | Atalhos OneLake → OPENROWSET | Azure AD (`az login` / identidade gerenciada) |
| `synapse` | T-SQL | Sim | ADLS OPENROWSET / tabelas externas | Azure AD |
| `sqlalchemy` | Qualquer dialeto SQLAlchemy | Não | Nenhum (apenas pouso) | Credenciais por dialeto |

### Padrão zero-config: DuckDB {: #zero-config-default-duckdb-req-989-tool-verified-enginepy-build_duckdb_engine-embedded_duckdb_materialize_default }

Quando `PROVISA_ENGINE` não está definido, o Provisa usa o motor DuckDB totalmente embutido, em processo. O armazenamento de materialização do DuckDB é um arquivo DuckDB embutido em `$PROVISA_DATA_DIR/materialize.duckdb` (padrão `~/.provisa/materialize.duckdb`). Nenhum banco de dados ou serviço externo é necessário.

Como o DuckDB impõe um único gravador por arquivo, `store_connection.py` grava no armazenamento embutido através da própria conexão do motor — nunca uma segunda conexão independente. Este é o único caso em que o motor e o armazenamento de materialização compartilham um handle de arquivo por design.

### Transporte de leitura nativo Arrow {: #arrow-native-read-transport-tool-verified-enginepy-build__engine-capabilities }

ClickHouse, DuckDB, Snowflake, Databricks, BigQuery, Fabric, e Synapse todos anunciam `EngineCapability.ARROW` e `EngineCapability.ARROW_STREAM`. Consultas contra esses motores retornam RecordBatches Arrow diretamente — o caminho de serialização por linha é completamente contornado. O servidor Flight faz streaming desses batches para clientes sem materializar o resultado completo na memória do processo do Provisa. Para o Trino, o streaming Arrow depende do proxy Zaychik; para os motores de warehouse, a própria API Arrow-nativa do motor (Cloud Fetch para Databricks, Storage Read API para BigQuery, `fetch_arrow_table` para DuckDB e Snowflake) alimenta o stream Flight.

### Links de dados externos (ATTACH) {: #external-data-links-attach-tool-verified-enginepy-warehouse_connectors }

Todo motor de warehouse pode escanear dados de objeto/lake em nuvem no lugar, sem pousar uma cópia. Arquivos Parquet, CSV, Iceberg e Delta Lake em S3, GCS, ou OneLake se conectam (attach) diretamente ao motor como se fossem tabelas nativas. A estratégia — ATTACH (escanear no lugar) ou LAND (copiar para o armazenamento) — é determinada pelo `Mechanism` declarado do conector; não existe ramificação específica de motor no planejador. Um conector `Mechanism.ATTACH_R` aciona escaneamento zero-cópia; um conector `Mechanism.DIRECT` ou ausente aciona um pouso.

O attach auto-provisiona todos os pré-requisitos no momento do attach:

| Motor | Formatos objeto/lake | Mecanismo | Auto-provisionamento |
| -------- | ------------------- | ---------- | ---------------------------------- |
| Databricks | parquet, csv, iceberg, delta_lake | Tabela externa UC (`ATTACH_R`) | REST instala credencial de armazenamento Unity Catalog + local externo, depois `CREATE TABLE … USING <format> LOCATION …` — verificado ao vivo sobre Cloudflare R2 |
| BigQuery | parquet, csv, json, iceberg, delta_lake | Tabela externa BigQuery / BigLake (`ATTACH_R`) | `CREATE OR REPLACE EXTERNAL TABLE … OPTIONS(format=…, uris=[…])` — verificado ao vivo |
| ClickHouse | csv, parquet, iceberg, delta_lake | Motor de tabela S3 / IcebergS3 / DeltaLake (`ATTACH_R`) | Sonda de validação executada no momento do attach — verificada ao vivo sobre Cloudflare R2 |
| Fabric | parquet, csv, iceberg, delta_lake | Atalho OneLake → OPENROWSET (`ATTACH_R`) | REST cria uma conexão `AmazonS3Compatible` + lakehouse + atalho; retorna o caminho `BULK` do OneLake — verificado ao vivo lendo R2 através do Fabric |
| Snowflake | parquet, csv, json, iceberg, delta_lake | External stage + tabela externa (`ATTACH_R`) | `CREATE STAGE … URL=… CREDENTIALS=…`, depois `CREATE OR REPLACE EXTERNAL TABLE … LOCATION=@stage FILE_FORMAT=(TYPE=…)` — implementado; não testado ao vivo (nenhuma conta disponível) |

Credenciais para armazenamento em nuvem viajam em `federation_hints` da fonte (veja [Fontes](sources.md#warehouses-as-named-sources)). Qualquer tipo de fonte que não pode fazer ATTACH pousa primeiro no armazenamento de materialização do motor.

### Escritas de materialização colunar {: #columnar-materialization-writes-tool-verified-coredatabasepy436-store_connectionpy99 }

`Connection.bulk_copy` em `provisa/core/database.py` escolhe o caminho de ingestão em massa mais rápido por dialeto de armazenamento: `COPY` binário (`copy_records_to_table` do asyncpg) para armazenamentos PostgreSQL, e uma única declaração preparada `executemany` para todos os outros armazenamentos relacionais. O armazenamento embutido DuckDB pousa através de `land_duckdb_native` em `store_connection.py` — uma única chamada `executemany` para o lote inteiro, nunca um laço linha a linha.

## Redirecionamento de Resultado Grande {: #large-result-redirect }

Resultados que excedem um limite de linhas são redirecionados para armazenamento compatível com S3 (MinIO) em vez de serem retornados inline.

### Modos de Redirecionamento {: #redirect-modes }

| Modo | Como funciona | Os dados tocam o Provisa? |
| ------ | ------------- | ---------------------- |
| **CTAS** (Parquet, ORC) | O motor de federação grava diretamente no S3 via `CREATE TABLE AS SELECT` | Não |
| **Upload Provisa** (JSON, NDJSON, CSV, Arrow IPC) | O Provisa serializa e envia via boto3 | Sim |

Para formatos CTAS-nativos, o Provisa nunca manipula os dados — o motor de federação grava os arquivos diretamente no MinIO/S3. Este é o caminho preferido para grandes exportações analíticas.

### Sem object store {: #with-no-object-store }

Um resultado vai para o object store quando há um configurado (um endpoint, ou credenciais para AWS S3) e o motor grava nele o formato solicitado por conta própria. Caso contrário, ele é gravado em um arquivo na pasta da organização, `<data directory>/orgs/<org>/results/`. Um store configurado no qual o motor grava mas que não consegue alcançar faz a requisição falhar pelo nome. O resultado não é gravado na pasta em seu lugar.

| Motor | Grava o arquivo por conta própria | Formatos |
| ------ | ---------------------- | ------- |
| DuckDB | `COPY (query) TO '<file>'` | parquet, csv, json, ndjson |
| PostgreSQL | `COPY (query) TO STDOUT`, transmitido para o arquivo no host do Provisa | csv |
| Qualquer outro formato ou motor | O Provisa grava a partir das linhas | parquet, orc, csv, arrow, json, ndjson |

O handle tem as mesmas chaves do object store, com `"sink": "local"` e um `redirect_url` igual a `/data/redirect-file/<name>`, um caminho no servidor que respondeu. Só o usuário que pediu o resultado pode buscá-lo. Cada busca compara a identidade própria da requisição com o proprietário registrado ao lado do arquivo, e responde 403 a qualquer outro e 401 a uma requisição sem identidade.

Um arquivo mais antigo que `redirect.ttl` recebe 404 e é excluído. Um temporizador o exclui ao expirar, como são excluídos os resultados do object store. Cada novo resultado também varre a pasta de resultados da organização, removendo arquivos cujo temporizador foi encerrado por um reinício.

### Cabeçalhos de Redirecionamento {: #redirect-headers }

| Cabeçalho | Efeito |
| -------- | -------- |
| `X-Provisa-Redirect-Format: <mime>` | Redireciona neste formato (implica força a menos que um limite seja definido) |
| `X-Provisa-Redirect-Threshold: N` | Redireciona apenas se o resultado exceder N linhas |
| `X-Provisa-Redirect: true` | Força o redirecionamento usando o formato padrão |

Esses cabeçalhos implementam redirecionamento controlado pelo cliente.

**Resposta:**

```json
{
  "data": {"orders": null},
  "redirect": {
    "redirect_url": "https://minio:9000/provisa-results/results/abc.parquet?...",
    "row_count": 50000,
    "expires_in": 3600,
    "content_type": "application/vnd.apache.parquet"
  }
}
```

### Configuração do Servidor {: #server-configuration }

| Variável de ambiente | Padrão | Propósito |
| --------- | --------- | --------- |
| `PROVISA_REDIRECT_ENABLED` | `false` | Habilita redirecionamento por limite no lado do servidor |
| `PROVISA_REDIRECT_THRESHOLD` | `1000` | Limite padrão de contagem de linhas |
| `PROVISA_REDIRECT_FORMAT` | `parquet` | Formato de redirecionamento padrão |
| `PROVISA_REDIRECT_BUCKET` | `provisa-results` | Nome do bucket S3 |
| `PROVISA_REDIRECT_ENDPOINT` | | URL de endpoint compatível com S3 |
| `PROVISA_REDIRECT_TTL` | `3600` | TTL da URL pré-assinada (segundos) |

## Árvore de Decisão de Roteamento {: #routing-decision-tree }

```text
Multi-source query? → Federation engine
NoSQL source (MongoDB, Cassandra)? → Federation engine
Uses path columns on non-PG source? → Federation engine
Single RDBMS with driver? → Direct (sub-100ms target)
Single RDBMS without driver? → Federation engine
Steward hint "federated"? → Federation engine (override)
Steward hint "direct"? → Direct (if possible)
Redirect to Parquet/ORC? → Federation engine (CTAS, regardless of source count)
```

## Otimização de Consulta de Federação {: #federation-query-optimization }

O Provisa alimenta automaticamente o otimizador baseado em custo do motor de federação para que os planos de consulta entre fontes se baseiem na distribuição real dos dados, não em padrões fixos.

### Estatísticas Automáticas (`ANALYZE`) {: #automatic-statistics-analyze }

No registro de uma fonte, o Provisa executa `ANALYZE catalog.schema.table` para cada tabela publicada. Isso coleta:

- Contagem de linhas
- Por coluna: fração de nulos, contagem de valores distintos, mín/máx, histogramas (dependente do conector)

O otimizador usa esses dados para estimar a seletividade de consultas filtradas. Sem estatísticas, ele recorre a padrões fixos (ex.: 10% de seletividade para predicados de igualdade), o que produz planos de join ruins em dados enviesados ou de alta cardinalidade. Com estatísticas, as estimativas são precisas o suficiente para tomar decisões corretas de join broadcast vs. particionado para a maioria das cargas de trabalho.

**Cobertura**: o suporte a estatísticas varia por conector. PostgreSQL, MySQL, Hive, Iceberg e Delta Lake suportam totalmente `ANALYZE`. Os conectores MongoDB e Cassandra têm suporte parcial ou nenhum. O Provisa engole falhas de `ANALYZE` silenciosamente — o registro nunca é bloqueado.

**Limites de seletividade**: as estatísticas fornecem estimativas por coluna. Para predicados correlacionados (`WHERE region = 'US' AND city = 'Seattle'`), o otimizador assume independência entre colunas, o que pode subestimar a contagem de linhas. Esta é uma limitação conhecida das estatísticas em nível de coluna em todos os otimizadores baseados em custo.

**Fontes de API**: tabelas `api_cache_{table_name}` no PostgreSQL são analisadas automaticamente após cada ciclo de atualização de cache, para que o otimizador tenha estimativas de linha atuais ao unir fontes apoiadas em API com fontes relacionais.

### Administração: Atualizar Estatísticas {: #admin-refresh-statistics }

Reexecute a coleta de estatísticas sob demanda via a API de administração:

```graphql
mutation {
  refreshSourceStatistics(sourceId: "sales-pg") {
    tablesAnalyzed
    failures { table message }
  }
}
```

Útil quando uma fonte recebeu dados novos significativos desde o registro.

## Views Materializadas {: #materialized-views }

As MVs otimizam de forma transparente consultas custosas pré-computando e armazenando resultados em cache.

### Relacionamentos como Hints de MV {: #relationships-as-mv-hints }

Uma declaração de relacionamento não é apenas um artefato de governança — é também a descrição estrutural de um formato de join. Esse formato é exatamente o que o otimizador de MV precisa: duas tabelas, duas colunas, um tipo de join. Isso significa que um relacionamento pode diretamente conduzir a materialização.

Para **relacionamentos entre fontes**, isso acontece automaticamente na inicialização: todo relacionamento com `materialize: true` cujas pernas caem em mais de uma fonte gera uma MV `JoinPattern` (`auto-mv-<rel_id>`). Nenhuma configuração de MV separada é necessária. Quando o compilador vê esse join em uma consulta, o reescritor substitui o resultado pré-materializado de forma transparente. Relacionamentos dentro da mesma fonte não geram nada — esses JOINs já são rápidos via execução direta.

Um **relacionamento apoiado em junção** materializa sua travessia em vez de um join direto: a tabela associativa é uma terceira perna, então o padrão carrega o salto de origem, o salto da junção e o discriminador que fixa o conjunto de linhas a um único tipo de aresta, com as colunas próprias da junção aterrissando na visão ao lado das do destino. Como a junção conta como perna, uma aresta cuja junção fica em uma fonte diferente das duas tabelas que ela liga é entre fontes mesmo quando essas duas coincidem. O reescritor casa os dois saltos como uma cadeia — o segundo precisa partir do alias que o primeiro introduziu — de modo que uma consulta que alcança essas mesmas duas tabelas sem passar pela junção lê as tabelas base, e uma visão construída para um valor de discriminador nunca responde a uma travessia filtrada por outro.

A consequência prática: stewards que aprovam um relacionamento estão implicitamente decidindo se o join é um bom candidato para materialização. O ato de governança e o hint de otimização são a mesma declaração.

### Modos {: #modes }

| Modo | Config | Comportamento |
| ------ | -------- | ---------- |
| **Join-pattern** | `join_pattern` na config de MV | Reescreve JOINs correspondentes para ler da tabela MV |
| **SQL personalizado** | `sql` na config de MV | SELECT arbitrário, opcionalmente exposto no SDL |
| **Relacionamento auto-materializado** | relacionamento entre fontes (automático) | Auto-gera uma MV join-pattern; nenhuma config necessária |
| **Relacionamento materializado pelo steward** | `materialize: true` em relacionamento na mesma fonte | Opt-in explícito para caminhos de join frequentes na mesma fonte |

### Auto-Materialização {: #auto-materialization }

JOINs entre fontes são as consultas mais custosas (sempre federadas). Relacionamentos entre fontes geram automaticamente definições de MV na inicialização:

```yaml
relationships:
  - id: orders-to-reviews
    source_table_id: orders        # sales-pg
    target_table_id: product_reviews  # reviews-mongo
    source_column: product_id
    target_column: product_id
    cardinality: one-to-many
    materialize: true              # auto-create MV
    refresh_interval: 600          # refresh every 10 minutes
```

Apenas relacionamentos entre fontes geram MVs (JOINs na mesma fonte já são rápidos via execução direta). A MV começa com status `STALE` e é atualizada pelo laço de atualização em segundo plano antes de ser usada pelo otimizador de consultas.

### Ciclo de Vida de Atualização {: #refresh-lifecycle }

```text
STALE → (refresh loop picks up) → REFRESHING → FRESH
  ↑                                                |
  └──── mutation hits source table ────────────────┘
```

O laço de atualização roda a cada 30 segundos, verifica `get_due_for_refresh()`, e executa `CREATE TABLE AS SELECT` (primeira execução) ou `DELETE + INSERT` (execuções subsequentes) contra a tabela alvo da MV via o motor de federação.

## Mapa de Módulos {: #module-map }

| Módulo | Propósito |
| -------- | ------------- |
| `api/` | App FastAPI, roteadores, middleware, gerenciamento de lifespan |
| `api/flight/` | Servidor Arrow Flight (gRPC, porta 8815) |
| `api/admin/` | API GraphQL de administração Strawberry — config, descoberta, views |
| `api/rest/` | Endpoints REST auto-gerados a partir de tabelas registradas |
| `api/jsonapi/` | Endpoints JSON:API auto-gerados com paginação e tratamento de erros |
| `api/data/subscribe.py` | Subscriptions SSE — LISTEN/NOTIFY, polling, Debezium CDC |
| `compiler/` | Parsers GraphQL/SQL, gerador de SQL semântico, RLS, mascaramento, amostragem, governança em dois estágios (`stage2.py`) |
| `cypher/` | Tradutor Cypher → SQL, parser, mapa de rótulos, tradutor de escrita para mutações Cypher |
| `pgwire/` | Servidor de protocolo de fio PostgreSQL; `catalog.py` intercepta pg_catalog/information_schema para visibilidade de objeto por função |
| `vector/` | Busca vetorial — registro de modelos, provedores de embedding (openai/ollama/huggingface), tradução de `cosine_similarity()`, índice HNSW na coluna de embedding de uma réplica, geração declarativa de embeddings |
| `compiler/federation.py` | Suporte a subgraph Apollo Federation v2 |
| `transpiler/` | Transpilação de dialeto, lógica de roteamento |
| `executor/` | Execução federada/direta, serialização, formatos de saída |
| `executor/drivers/` | Drivers de fonte diretos (PostgreSQL, MySQL, DuckDB, Snowflake, Databricks, ClickHouse, …) |
| `executor/trino_flight.py` | Cliente ADBC Flight SQL para o motor de federação |
| `executor/ctas_write.py` | Redirecionamento baseado em CTAS (o motor de federação grava no S3) |
| `executor/redirect.py` | Lógica de redirecionamento S3, upload do lado do Provisa |
| `federation/engine.py` | `FederationEngine`, `DriverClass`, `_ENGINE_BUILDERS`, `ENGINE_REGISTRY`, `build_engine` |
| `federation/connector.py` | Abstrações de conector — Trino, ClickHouse; `Mechanism`, `WarehouseNativeConnector` |
| `federation/connector_duckdb.py` | Definições de conector DuckDB e PostgreSQL FDW |
| `federation/snowflake_connectors.py` | Conectores ATTACH de external stage + tabela externa Snowflake |
| `federation/databricks_connectors.py` | Conectores ATTACH de tabela externa UC Databricks |
| `federation/bigquery_connectors.py` | Conectores ATTACH externo BigQuery / BigLake |
| `federation/databricks_uc.py` | Auto-provisionamento de credencial + local externo Unity Catalog |
| `federation/databricks_backend.py` | Backend de execução Databricks SQL warehouse |
| `federation/snowflake_backend.py` | Backend de execução Snowflake |
| `federation/bigquery_backend.py` | Backend de execução BigQuery (transporte Arrow Storage Read API) |
| `federation/mssql_warehouse_backend.py` | Backends de execução Fabric Warehouse + Synapse (T-SQL sobre ODBC) |
| `federation/mssql_warehouse_connectors.py` | Conectores ATTACH OPENROWSET para Fabric / Synapse |
| `federation/fabric_shortcuts.py` | Auto-provisionamento de atalho OneLake (conexão → lakehouse → atalho) |
| `federation/clickhouse_backend.py` | Backend de execução ClickHouse |
| `federation/duckdb_backend.py` | Backend de execução DuckDB em processo |
| `federation/pg_backend.py` | Backend de execução PostgreSQL |
| `federation/store_connection.py` | Face de escrita do armazenamento de materialização DuckDB-nativo |
| `registry/` | Registro de consultas persistidas, governança |
| `security/` | Visibilidade, direitos, mascaramento de coluna |
| `cache/` | Cache de resultado de consulta apoiado em Redis (camada quente) |
| `mv/` | Registro de views materializadas, atualização, reescritor SQL |
| `events/` | Eventos de mudança de dataset e despacho de gatilhos |
| `webhooks/` | Execução de webhook de saída para mutações e eventos |
| `scheduler/` | Gerenciamento de jobs em segundo plano baseado em APScheduler — gatilhos cron e de intervalo que disparam webhooks e mutações, e as consultas das saídas Kafka ao vivo |
| `apq/` | Protocolo de fio Apollo APQ — cache de hash de consulta apoiado em Redis; separado do cache de resultado |
| `compiler/cursor.py` | Paginação por cursor estilo Relay — argumentos `first`/`after`/`last`/`before` e geração de `pageInfo` em todas as consultas de lista |
| `compiler/aggregate_gen.py` | Tipos de consulta `{table}_aggregate` auto-gerados com subcampos `count`, `sum`, `avg`, `min`, `max` e acesso a `nodes` filtrado |
| `compiler/enum_detect.py` | Auto-detecção de tipo enum — tipos enum nativos do PostgreSQL (`pg_enum`) expostos como tipos enum GraphQL em vez de scalars de string |
| `compiler/hints.py` | Hints de desempenho de federação — diretivas de roteamento em nível de consulta embutidas como comentários SQL (`/* @provisa route=federated */`) que sobrepõem o roteamento automático |
| `compiler/mutation_gen.py` | Compilador de mutação; presets de coluna — valores estáticos do lado do servidor ou de variável de sessão aplicados em insert/update, não expostos no tipo de entrada da mutação |
| `auth/approval_hook.py` | Hook de aprovação ABAC — autorização externa plugável chamada antes da execução da consulta; transportes webhook, gRPC, e unix_socket; escopo por tabela/fonte/global; política de fallback configurável |
| `subscriptions/` | Estado e entrega de subscription SSE |
| `discovery/` | Descoberta de relacionamento por LLM (API Claude) |
| `grpc/` | Geração de proto, servidor gRPC, reflexão |
| `api_source/` | Fontes de API REST/GraphQL/gRPC com cache PG |
| `kafka/` | Fontes de tópico Kafka, sink, Schema Registry |
| `auth/` | Provedores de autenticação plugáveis, middleware, mapeamento de função |
| `core/` | Config, modelos, BD, repositórios, segredos; o modelo de função suporta `parent_role_id` e `flatten_roles()` para herança recursiva de função |
| `hasura_v2/` | Conversor de metadados Hasura v2 → config Provisa |
| `ddn/` | Conversor de supergraph Hasura DDN → config Provisa |
| `mongodb/` | Conector de fonte MongoDB |
| `elasticsearch/` | Conector de fonte Elasticsearch |
| `cassandra/` | Conector de fonte Cassandra |
| `prometheus/` | Conector de fonte de métricas Prometheus |
| `source_adapters/` | Camada de adaptador genérico para conexões de fonte |

## API de Administração {: #admin-api }

A API GraphQL Strawberry de administração é montada em `/admin/graphql` (porta HTTP 8001). É separada do endpoint GraphQL de dados e exige função superuser ou admin.

| Capacidade | Descrição |
| ----------- | ------------- |
| Download/upload de config | Exporta ou substitui a configuração YAML completa do Provisa |
| Editor de relacionamentos | Cria, atualiza, exclui definições de relacionamento |
| Descoberta de FK por IA | Aciona análise de candidatos a FK potencializada por Claude |
| Introspecção de esquema | Navega tabelas, colunas e funções publicadas |
| Gerenciamento de views | Registra e gerencia definições de view materializada |

## Configuração de Modelos de IA {: #ai-models-configuration }

`GET /admin/ai-models` e `PUT /admin/ai-models` configuram o pipeline de LLM para cada organização.

As configurações têm **escopo por organização**: as escolhas de cada organização se sobrepõem à config de implantação e entram em vigor na próxima requisição — sem necessidade de reinício.

**Atribuições de modelo por operação.** Seis operações têm cada uma um fornecedor e uma string de modelo configuráveis:

| Operação | O que ela controla |
| --------- | -------------- |
| `table_description` | Descrições de tabela geradas por LLM |
| `column_description` | Descrições de coluna geradas por LLM |
| `relationship_inference` | Descoberta de candidatos a FK |
| `sql_generation` | Geração de NL → SQL |
| `table_selection` | Escolha de quais tabelas incluir no prompt de NL |
| `mcp_chat` | O loop de uso de ferramentas do painel do assistente de chat |

O campo de fornecedor aceita qualquer fornecedor compatível com `aisuite` (`anthropic`, `openai`, `groq`, `mistral`, `cohere`, entre outros) ou um endpoint local (`ollama`, `lmstudio`). Uma string de modelo em branco remove o override da organização e reverte para o padrão da implantação.

**Limite de taxa de NL.** Um limite opcional de requisições por período aplicado por função. Requisições em excesso retornam `429` com `Retry-After`.

**Registro de modelos vetoriais.** Uma lista de modelos de embedding (campos: `id`, `provider`, `dimensions`, opcionais `api_key_env` e `base_url`, flag `enabled`). Substituição de lista completa: toda entrada deve ter `id`, `provider`, e `dimensions`, ou a escrita é rejeitada com `400`.

**Chaves de API.** Chaves de API de LLM por fornecedor são armazenadas de forma criptografada via `provisa.core.org_secrets` (veja abaixo). A resposta do `GET` reporta apenas se uma chave está definida para cada fornecedor — o valor nunca é retornado. Enviar uma string em branco para um fornecedor limpa aquela chave, revertendo as chamadas de LLM para aquele fornecedor para a credencial de variável de ambiente da implantação.

## Segredos Criptografados por Organização {: #per-org-encrypted-secrets }

`provisa/core/org_secrets.py` armazena credenciais que nunca devem aparecer como texto plano no banco de dados. Ele guarda as chaves de API dos fornecedores de IA (`{vendor}_api_key`) e a chave do Jev (`jev_api_key`).

Os valores são criptografados via o `encryption_service` de escopo de processo de `provisa.encryption.runtime` — o mesmo mecanismo usado por `api_sources.auth`.

Doze fornecedores compatíveis com `aisuite` são suportados: `anthropic`, `openai`, `cohere`, `groq`, `mistral`, `xai`, `deepseek`, `together`, `fireworks`, `nebius`, `sambanova`, e `inception`. Google, AWS e Azure são excluídos porque exigem configuração além de uma simples chave de API (IDs de projeto, funções IAM, região). Fornecedores de endpoint local (`ollama`, `lmstudio`) não têm chave e são excluídos pelo mesmo motivo.

Passar `value=None` para `write_org_secret` exclui a linha. Chamadores que leem um segredo o consomem imediatamente (ex.: para construir um cliente LLM) e não devem ecoá-lo em nenhuma resposta de API.

## Endpoints REST e JSON:API Auto-Gerados {: #auto-generated-rest-jsonapi-endpoints }

Tabelas registradas são expostas como endpoints REST e JSON:API junto com a interface GraphQL.

| Interface | Caminho de montagem | Especificação |
| ----------- | ----------- | ------ |
| REST | `/rest/<table-id>` | GET/POST simples com parâmetros de consulta |
| JSON:API | `/jsonapi/<table-id>` | Compatível com [jsonapi.org](https://jsonapi.org) — paginação, relacionamentos, objetos de erro |

Esses endpoints aplicam o mesmo pipeline de segurança (RLS, mascaramento, verificações de função) que o endpoint GraphQL.

## Subscriptions

Subscriptions SSE são servidas em `GET /data/subscribe/{table}`. Três modos de entrega:

| Modo | Mecanismo | Quando usado |
| ------ | ----------- | ----------- |
| **LISTEN/NOTIFY** | `LISTEN` do PostgreSQL em um canal | Fontes PG com atividade de mutação |
| **Polling** | Reexecuta a consulta em intervalo | Fontes não-PG, ou quando CDC indisponível |
| **Debezium CDC** | Tópico Kafka do conector Debezium | Streams de mudança de alta frequência |

O cliente recebe `text/event-stream` com um evento JSON por linha alterada ou diff.

## Sistema de Eventos e Webhooks {: #event-webhook-system }

Mutações de banco de dados (INSERT/UPDATE/DELETE) podem acionar eventos de saída via os módulos `events/` e `webhooks/`.

```text
Mutation executed → EventDispatcher → match event trigger rules
                                          ↓
                               WebhookExecutor → HTTP POST to configured URL
```

Gatilhos de evento são definidos na config e correspondidos por tabela, tipo de operação, e filtro de linha opcional. Payloads de webhook incluem o tipo de operação, linha alterada, e contexto de função.

## Serviços em Segundo Plano {: #background-services }

Quatro laços em segundo plano iniciam durante o lifespan da app (`api/app.py`):

| Serviço | Intervalo | Propósito |
| --------- | ---------- | --------- |
| Laço de atualização de MV | 30 s | Consulta `get_due_for_refresh()`, executa CTAS ou DELETE+INSERT em MVs desatualizadas |
| Avaliação da replicação quente | `replication.hot_interval` | Promove uma tabela cuja contagem de instruções passou do seu limite e solicita sua réplica; rebaixa uma que caiu abaixo da metade. Roda no único processo que detém o bloqueio do agendador |
| Carregador de tabelas quentes | Configurável | Carrega pequenas tabelas de referência em cache em memória para acesso sub-milissegundo |
| Poller de fonte API | Intervalo por fonte | Rebusca e recacheia fontes remotas REST/GraphQL/gRPC |

### Tabelas quentes e tabelas replicadas sob carga {: #hot-tables-and-replicated-when-busy-tables }

| Camada | Armazenamento | Critério de promoção | Latência de acesso |
| ------ | --------- | ------------------- | ---------------- |
| Quente | Memória em processo | Contagem de linhas < limite, ou é alvo de relacionamento | <1 ms |
| Replicada sob carga | Réplica no store do motor | `replicate` é Default ou Hot-N e as instruções governadas que leem a tabela passaram do seu limite | A leitura própria do motor no seu store |
| Fria | Fonte remota | Padrão | 50–500 ms |

Uma tabela fica em no máximo um dos dois: uma tabela gerida pela camada quente não é replicada por estar sob carga.

## Importação de Metadados (Hasura v2 / DDN) {: #metadata-import-hasura-v2-ddn }

Implantações Hasura existentes podem ser convertidas para config Provisa sem reescrita manual.

| Módulo | Entrada | Saída |
| -------- | ------- | -------- |
| `hasura_v2/` | `metadata.yaml` do Hasura v2 | `config.yaml` do Provisa |
| `ddn/` | JSON de supergraph Hasura DDN | `config.yaml` do Provisa |

Ambos os conversores mapeiam tabelas rastreadas, relacionamentos, permissões, e esquemas remotos. O resultado é uma config Provisa completa pronta para implantação.

## Apollo Federation

`compiler/federation.py` expõe o Provisa como um subgraph Apollo Federation v2. O SDL do subgraph é auto-gerado a partir do esquema publicado com diretivas `@key` em colunas de chave primária e anotações `@external`/`@provides` em relacionamentos entre subgraphs. O Provisa responde a consultas `_entities` e `_service` exigidas pelo gateway de federação.

## Paginação Baseada em Cursor {: #cursor-based-pagination }

Todas as consultas de lista suportam paginação por cursor estilo Relay via `compiler/cursor.py`. Clientes passam argumentos `first`/`after` (para frente) ou `last`/`before` (para trás). O compilador codifica a posição da linha como um cursor base64 opaco e injeta as cláusulas `WHERE`/`LIMIT` apropriadas. Toda consulta de lista retorna um objeto `pageInfo`:

| Campo | Tipo | Descrição |
| ------- | ------ | ------------- |
| `hasNextPage` | Boolean | Verdadeiro se mais resultados existem após esta página |
| `hasPreviousPage` | Boolean | Verdadeiro se resultados existem antes desta página |
| `startCursor` | String | Cursor do primeiro nó nesta página |
| `endCursor` | String | Cursor do último nó nesta página |

## Consultas Agregadas {: #aggregate-queries }

Toda tabela registrada recebe um campo raiz auto-gerado `{table}_aggregate` (`compiler/aggregate_gen.py`). O tipo agregado expõe `count`, `sum`, `avg`, `min`, `max` por coluna numérica, e `nodes` para acesso a linha filtrado com seleção de campo completa (mesmo RLS/mascaramento que a consulta base). Um agregado sobre uma única tabela, de qualquer superfície (SQL ou compilada), é respondido por uma view materializada atualizada que o pré-agrega, sem nada declarado: uma view cujo SQL é um `GROUP BY` de uma única tabela com `SUM`, `COUNT`, `MIN` e `MAX` nomeados atende os agregados que agrupam por um subconjunto das suas colunas de agrupamento e filtram só por elas. Cada agregado é reagregado sobre os armazenados (`COUNT` como a soma das contagens armazenadas, `AVG` a partir de uma soma e uma contagem armazenadas da mesma coluna); o que a view não consegue responder exatamente fica para a tabela. A reescrita roda depois da governança, então um filtro de linha sobre uma das colunas de agrupamento da view é levado para a view, e um filtro de linha sobre qualquer outra coluna, ou uma máscara sobre uma coluna somada, deixa a leitura para a tabela.

## Automatic Persisted Queries (APQ)

`apq/cache.py` implementa o protocolo de fio Apollo APQ. Quando um cliente envia apenas um hash de consulta (`extensions.persistedQuery`), o Provisa o busca no Redis. Em caso de miss, ele retorna um erro `PersistedQueryNotFound`; o cliente tenta novamente com o corpo completo da consulta, que o Provisa armazena. Isso é separado do cache de resultado (`cache/`).

## Funções Herdadas {: #inherited-roles }

Uma função nomeia no máximo um pai através de `parent_role_id`. Na construção em tempo de execução, `security/inheritance.py` percorre a cadeia de cada função do filho para cima e a incorpora às próprias cópias dos dados carregados da construção, de modo que toda busca posterior permanece indexada pelo id da função atuante. Capacidades e acesso a domínio são a união da cadeia. O `visible_to` de uma coluna, cada entrada da sua lista de máscaras, e o `visible_to` de uma métrica, função ou webhook valem para uma função quando a nomeiam ou nomeiam um ancestral. As permissões de escrita de uma tabela se resolvem por operação do mesmo modo: uma função sem permissão para uma operação assume a do ancestral mais próximo, a sua própria primeiro. O RLS se resolve por tabela com a filha tendo precedência: a função mais próxima na cadeia que tem uma regra para a tabela — sua própria regra de tabela antes da sua regra de domínio — fornece o único predicado que roda; a regra de um ancestral nunca é combinada com ela. Uma função herda o limite de taxa do pai quando não define um próprio. Um pai precisa existir, não pode ser a própria função e não pode fechar um ciclo; uma função da qual outras funções herdam não pode ser excluída até ser reatribuída a outro pai.

## Hook de Aprovação ABAC {: #abac-approval-hook }

`auth/approval_hook.py` é um hook de autorização plugável invocado antes da execução da consulta, após RLS e mascaramento. Ele se integra com motores de política externos (OPA, serviços ABAC personalizados).

| Configuração | Descrição |
| --------- | ------------- |
| Transporte | `webhook` (HTTP POST), `grpc`, ou `unix_socket` |
| Escopo | Por tabela, por fonte, ou global |
| Política de fallback | `allow` ou `deny` quando o endpoint do hook está inacessível |

## Auto-Detecção de Tipo Enum {: #enum-type-auto-detection }

`compiler/enum_detect.py` introspecta tipos enum nativos do PostgreSQL (`pg_enum`) no momento da geração de esquema. Colunas usando um tipo enum definido pelo usuário no PostgreSQL são promovidas a tipos enum GraphQL — seus valores tornam-se membros do enum em vez de scalars de string.

## Gatilhos Programados {: #scheduled-triggers }

`scheduler/jobs.py` usa o APScheduler para executar jobs em segundo plano definidos como gatilhos cron ou de intervalo. Cada job pode fazer POST para uma URL de webhook ou executar uma mutação contra o endpoint de dados; a saída Kafka ao vivo de uma tabela (`live.outputs`) é consultada no mesmo agendador. Gatilhos são configurados via a API de administração (mutações `scheduledTrigger`) ou a chave `scheduled_triggers` na config YAML.

## Hints de Desempenho de Federação {: #federation-performance-hints }

`compiler/hints.py` analisa hints de steward embutidos em consultas como comentários usando a sintaxe de comentário do Provisa. O formato do hint varia por linguagem de consulta:

```graphql
# @provisa route=federated
{ orders { id amount } }
```

```sql
/* @provisa route=federated */
SELECT id, amount FROM orders
```

```cypher
// @provisa route=federated
MATCH (o:Order) RETURN o.id, o.amount
```

| Hint | Efeito |
| ------ | -------- |
| `route=federated` | Força a federação através do motor de federação, contornando o roteamento de driver direto |
| `route=direct` | Força a execução de driver direto |
| `cache=true` | Aderir esta consulta ao cache de respostas (forma de comentário SQL: `-- @provisa cache=true`) |
| `cache_ttl=N` | Aderir e manter o resultado em cache por N segundos (`-- @provisa cache_ttl=N`) |

O cache de respostas é por adesão a cada requisição: GraphQL usa a diretiva `@cached(ttl: Int)`, SQL e Cypher o comentário `cache`/`cache_ttl`, gRPC os metadados de chamada `x-provisa-cache` / `x-provisa-cache-ttl`. A dica vale em todo transporte que carrega a linguagem (Cypher sobre HTTP, Bolt e Arrow Flight; GraphQL sobre HTTP e Arrow Flight). Uma requisição sem dica nunca lê nem grava o cache, e nenhuma forma lê dados mais recentes do que as configurações de fonte e tabela do operador permitem. Veja [Cache](configuration.md#per-request-opt-in).

## Permissões de escrita e presets em mutações {: #write-grants-and-presets-in-mutations }

Os `write_grants` de uma tabela indicam, por função e operação, quais colunas uma escrita pode nomear, quais linhas pode tocar e quais valores a própria permissão define. `compiler/write_admission.py` admite cada escrita na etapa de governança, de modo que GraphQL, SQL, Cypher e as demais superfícies recebem a mesma resposta: o direito `write`, a permissão da função para a operação, as colunas, os presets adicionados à instrução, o predicado `rows` combinado com AND no `WHERE` e o `check` avaliado sobre as novas linhas antes de qualquer coisa chegar a uma fonte. Os presets vêm de uma variável de sessão, de um literal ou do momento da escrita. A instrução não pode fornecer uma coluna com preset, então os presets não estão no tipo de entrada de mutação GraphQL gerado.

## Explorador de Esquema GraphQL Voyager {: #graphql-voyager-schema-explorer }

A UI de administração (`provisa-ui/src/pages/SchemaExplorer.tsx`) embute o GraphQL Voyager como uma ferramenta interativa de visualização de esquema. Ela renderiza o esquema escopado por função como um diagrama de relacionamento de entidade navegável — tabelas como nós, relacionamentos como arestas. O esquema mostrado é sempre filtrado para a função atualmente selecionada.

## Ordem de Aplicação de Segurança {: #security-enforcement-order }

Não há gate de capacidade sobre a consulta — a governança é expressa inteiramente através de controles da camada de dados. Uma requisição de SQL bruto rejeita (HTTP 403) qualquer tabela fora do escopo de objeto da função antes que a governança execute.

1. **Visibilidade de Objeto**: O esquema por função oculta tabelas/colunas não autorizadas; tabelas fora de escopo em SQL bruto são rejeitadas
2. **Aplicação de relacionamento**: Travessias devem existir no catálogo de relacionamentos aprovado, a menos que a função possua `ignore_relationships` — entre as funções de sistema pré-carregadas, apenas `modeler` a possui. No modo de alta segurança a capacidade é ignorada e nenhuma travessia escapa do catálogo
3. **RLS**: Injeção de cláusula WHERE por tabela por função
4. **Mascaramento de Coluna**: Transformação de dados por coluna por função
5. **Limite de linhas (LIMIT)**: Limite de contagem de linhas para funções sem `full_results`; amostragem estatística aleatória é um recurso de consulta de usuário separado

Todas as quatro interfaces de consulta (HTTP, Flight, gRPC, pgwire) aplicam o mesmo pipeline de governança de Estágio 2; nenhum caminho de cliente pode contorná-lo sem contornar o servidor.

## Limites de Escalabilidade {: #scalability-limits }

O Provisa é uma camada fina de compilação e roteamento — adiciona latência de milissegundos de dígito único à consulta. No entanto, caminhos onde o Provisa serializa dados de resultado são limitados pela memória do processo. Dois caminhos são verdadeiramente ilimitados:

| Caminho | Limitado por memória? | Adequado para |
| ------ | -------------- | ------------- |
| JSON inline (HTTP) | Sim | Resultados pequenos-médios |
| **Streaming Arrow Flight (gRPC :8815)** | **Não** | **Ilimitado — streaming via Zaychik ou API Arrow do warehouse** |
| Protobuf gRPC inline (:50051) | Sim | Resultados médios, serviço-a-serviço |
| Redirecionamento: upload Provisa (JSON, CSV, NDJSON, Arrow IPC) | Sim | Resultados médios, download de arquivo |
| **Redirecionamento: CTAS (Parquet, ORC)** | **Não** | **Ilimitado — o motor de federação grava no S3** |

### Sondagem por Limite {: #threshold-probing }

Para redirecionamento baseado em limite, o Provisa injeta `LIMIT threshold + 1` na consulta como sonda. Se o resultado tiver menos linhas, ele retorna inline (resultado completo, sem trabalho desperdiçado). Se o resultado atingir o limite, a sonda é descartada e a consulta completa é reexecutada via CTAS ou upload Provisa. Isso evita `SELECT COUNT(*)` (que algumas fontes não otimizam) e funciona em qualquer fonte.

Para grandes cargas de trabalho analíticas, use:

- **Arrow Flight** (porta 8815) para streaming para ferramentas de dados — os batches fluem pelo Provisa sem materializar
- **Redirecionamento Parquet/ORC** para exportações baseadas em arquivo — o motor de federação grava diretamente no S3, o Provisa retorna uma URL pré-assinada

## Infraestrutura {: #infrastructure }

| Serviço | Imagem | Porta | Propósito |
| --------- | ------- | ------ | --------- |
| Provisa API | (processo host) | 8001 | Endpoint HTTP/REST |
| Provisa Flight | (processo host) | 8815 | Servidor gRPC Arrow Flight |
| Provisa gRPC | (processo host) | 50051 | Servidor gRPC Protobuf |
| Motor de Federação | `trinodb/trino` (padrão) ou warehouse externo | 8080 / varia | Motor de federação de consulta — Trino para a stack embutida; Snowflake/Databricks/BigQuery/Fabric/Synapse/DuckDB para alvos de warehouse |
| Zaychik | `provisa-zaychik` (construído a partir do código-fonte) | 8480 | Proxy Arrow Flight SQL para Trino; não exigido para motores de warehouse |
| PostgreSQL | `postgres:16` | 5432 | Metadados de config + catálogo Iceberg |
| MongoDB | `mongo:7` | 27017 | Fonte de dados NoSQL de demonstração |
| MinIO | `minio/minio` | 9000/9001 | Armazenamento de objetos compatível com S3 |
| Redis | `redis:7-alpine` | 6379 | Cache de resultado de consulta |
| PgBouncer | `edoburu/pgbouncer` | 6432 | Pooling de conexão para PG |
| Kafka | `confluentinc/cp-kafka:7.6.0` | 9092 | Fontes de dados em streaming |
| Schema Registry | `confluentinc/cp-schema-registry:7.6.0` | 8081 | Gerenciamento de esquema Avro/Protobuf |
