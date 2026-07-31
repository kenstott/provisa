# Referência de Configuração

O Provisa é configurado via um arquivo YAML (padrão: `config/provisa.yaml`). (REQ-528)

## Fontes

```yaml
sources:
  - id: sales-pg           # unique identifier
    type: postgresql
    host: postgres
    port: 5432
    database: provisa
    username: provisa
    password: ${env:PG_PASSWORD}  # secret resolution
    pool_min: 1
    pool_max: 5
    use_pgbouncer: false
    pgbouncer_port: 6432
```

Todas as fontes compartilham um conjunto comum de campos. [tool-verified: `provisa/core/models.py:129-212`]

| Campo | Padrão | Notas |
|-------|---------|-------|
| `id` | obrigatório | Alfanumérico, hífens, sublinhados |
| `type` | obrigatório | Ver tabela abaixo |
| `host` | `""` | Nome de host ou IP |
| `port` | `0` | `0` significa que cada conector fornece seu próprio padrão — não há um mapa central de portas padrão |
| `database` | `""` | |
| `username` | `""` | |
| `password` | `""` | Suporta resolução de segredo `${env:VAR}` |
| `path` | `null` | Caminho de arquivo ou URI para fontes baseadas em arquivo |
| `base_url` | `null` | URL base para fontes de API |
| `pool_min` / `pool_max` | `1` / `5` | Limites do pool de conexão |
| `cache_enabled` | `true` | Alterna o cache para todas as tabelas desta fonte |
| `cache_ttl` | `null` | Segundos; `null` herda o padrão global |
| `federation_hints` | `{}` | Parâmetros estendidos por conector (dict[str,str]); ver referência de tipos abaixo. REQ-281 |
| `mapping` | `{}` | DSL de mapeamento para redis, elasticsearch, prometheus. REQ-251 |
| `allowed_domains` | `[]` | Restringe esta fonte a IDs de domínio específicos; vazio = irrestrito |
| `description` | `""` | |

### Tipos de fonte suportados [tool-verified: `provisa/core/models.py:36-101`]

| Tipo | Estilo de conexão | Notas |
|------|-----------------|-------|
| **RDBMS** | | |
| `postgresql` | host/port | Pool asyncpg; PgBouncer opcional via `use_pgbouncer` |
| `mysql` | host/port | |
| `mariadb` | host/port | |
| `singlestore` | host/port | |
| `sqlserver` | host/port | |
| `oracle` | host/port | |
| `firebird` | host + `path` (arquivo do BD) | Extensão comunitária firebird do DuckDB (REQ-899) |
| `duckdb` | host/port | |
| `cockroachdb` | host/port | Reaproveita o driver/dialeto PostgreSQL (REQ-950) |
| `yugabytedb` | host/port | Reaproveita o driver/dialeto PostgreSQL (REQ-950) |
| `greenplum` | host/port | Reaproveita o driver/dialeto PostgreSQL (REQ-950) |
| `tidb` | host/port | Reaproveita o driver/dialeto MySQL (REQ-950) |
| **DW em nuvem** | | |
| `snowflake` | host/port + `federation_hints` | `account` obrigatório em hints |
| `bigquery` | `federation_hints` | `project` obrigatório; autenticação via `GOOGLE_APPLICATION_CREDENTIALS` |
| `databricks` | host + `federation_hints` | `http_path` obrigatório em hints |
| `fabric` | variáveis de ambiente ou `PROVISA_ENGINE_URL` | T-SQL sobre TDS, autenticação Azure AD |
| `synapse` | variáveis de ambiente ou `PROVISA_ENGINE_URL` | T-SQL sobre TDS, autenticação Azure AD |
| `redshift` | host/port | |
| **OLAP** | | |
| `clickhouse` | host/port + `federation_hints` | Hint `secure` alterna TLS; porta padrão 8123/8443 |
| `elasticsearch` | host/port + DSL `mapping` | |
| `pinot` | host/port | Endpoint REST do Controller |
| `druid` | host/port | Endpoint Avatica do Broker |
| `exasol` | host/port | |
| **Data Lake** | | |
| `delta_lake` | `path` (URI da tabela) | `delta_scan` do DuckDB; acesso a armazenamento de objetos via `federation_hints` |
| `iceberg` | `path` (URI da tabela) | `iceberg_scan` do DuckDB; acesso a armazenamento de objetos via `federation_hints` |
| `hudi` | `path` (URI da tabela) | Motor Hudi do ClickHouse, zero-copy (REQ-1178) |
| `hive` | host/port (metastore) + `mapping.storage` | Backend de armazenamento em `mapping["storage"]`: hadoop/hdfs/local/s3/azure/adls |
| `hive_s3` | host/port (metastore) + chaves S3 em `mapping` | Tipo distinto; sempre armazenamento S3 (REQ-229) |
| **NoSQL** | | |
| `mongodb` | host/port | Campos de conexão simples; sem DSL de mapeamento |
| `cassandra` | host/port | Campos de conexão simples; sem DSL de mapeamento |
| `redis` | host/port + DSL `mapping` | |
| **Streaming** | | |
| `kafka` | apenas registro | Configuração real vive em `kafka_sources[]`; ver §Kafka abaixo |
| `websocket` | host/port/path + `federation_hints` | Feed WebSocket externo |
| `rss` | host/port/path + `federation_hints` | Feed RSS 2.0 / Atom |
| **Grafo/Semântico** | | |
| `neo4j` | [UNVERIFIED end-to-end mapping] | |
| `sparql` | [UNVERIFIED end-to-end mapping] | |
| **Arquivo** | | |
| `sqlite` | `path` | Sempre roteia pelo motor (sem pool direto) |
| `csv` | `path` | |
| `parquet` | `path` | |
| `files` | `path` (diretório) | Crawler por glob; expõe CSV/Parquet/XLSX/JSON como tabelas |
| **API/Remoto** | | |
| `google_sheets` | `federation_hints.spreadsheet_id` | |
| `prometheus` | host/port ou `mapping.url` + DSL `mapping` | |
| `graphql_remote` | `base_url` + `mapping` opcional | Headers, forward-client-headers, timeout em `mapping` |
| `openapi` | `base_url` | |
| `grpc_remote` | [UNVERIFIED end-to-end mapping] | |
| `airport` | `base_url` (localização Flight) | Extensão airport do DuckDB (REQ-899) |
| `ingest` | receptor push | Serviços externos enviam POST de eventos JSON |
| **SaaS** | | |
| `sharepoint` | `base_url` ou `host` + `mapping` | Autenticação via `mapping.auth_type` |
| `splunk` | `host`/`port` ou `base_url` + `mapping` | |
| **GovData** | | |
| `govdata` | subject + `domain_id` | Modelo separado `GovDataSource`; ver §GovData abaixo |

### Referência de tipos de fonte

Tipos que precisam de configuração não óbvia têm uma entrada curta abaixo. Tipos RDBMS (postgresql, mysql, etc.) usam apenas os campos comuns acima — nenhuma seção adicional é necessária.

#### GovData [tool-verified: `provisa/core/models.py:953-983`]

Fontes `govdata` usam um modelo separado de nível superior, `GovDataSource`, não o `Source` genérico. (REQ-540) O acesso é particionado por agrupamento de subject.

```yaml
sources:
  - id: federal-data
    type: govdata
    subject: COMMERCE
    domain_id: federal-analytics
    api_key: ${env:GOVDATA_API_KEY}   # optional
    start_year: 2020                   # optional year filter
    end_year: 2024                     # optional year filter
```

Cada subject mapeia para um ou mais esquemas GovData. Configurar uma fonte `govdata` com um subject expõe todos os esquemas para esse subject automaticamente. (REQ-540)

| Subject | Esquemas |
|---------|---------|
| `COMMERCE` | `sec`, `patents` |
| `ECONOMY` | `econ`, `econ_reference` |
| `EDUCATION` | `census`, `edu` |
| `HEALTH` | `health` |
| `CYBER` | `cyber_threat`, `cyber_vuln` |
| `PUBLIC_SAFETY` | `crime` |
| `ENVIRONMENT` | `lands` |
| `WEATHER` | `weather` |
| `ENERGY` | `energy` |
| `GOVERNMENT` | `fedregister`, `fec` |

Os esquemas `ref` e `geo` são sempre incluídos como esquemas de ligação — não configuráveis e não listados acima. (REQ-541) Use o subject `ALL` para conceder acesso a todos os esquemas. [tool-verified: `provisa/core/models.py:961-963`]

#### Kafka [tool-verified: `provisa/federation/trino_connectors.py:497-502`, `provisa/api/app_loaders.py:113-118`]

A linha `kafka` em `sources:` é apenas registro. O `details()` de seu conector retorna `{}` — a configuração real vive no bloco de nível superior `kafka_sources[]`, não em uma linha de `sources:`. Kafka é sempre um VIRTUAL_SOURCE (roteia pelo motor; sem pool direto). [tool-verified: `provisa/transpiler/router.py:44-63`]

```yaml
kafka_sources:
  - id: event-stream
    bootstrap_servers: kafka:9092
    schema_registry_url: http://schema-registry:8081  # optional
    topics:
      - id: order-created
        topic: orders.events
        default_window: 1h          # auto-injected time bound
        schema_source: manual       # manual, registry, or sample
        value_format: json
        discriminator:              # filter shared topic by message type
          field: event_type
          value: OrderCreated
        columns:
          - name: event_type
            type: varchar
          - name: order_id
            type: integer
          - name: amount
            type: double
          - name: metadata
            type: varchar           # raw JSON for complex nested data
      - id: order-shipped
        topic: orders.events        # same physical topic
        default_window: 1h
        discriminator:
          field: event_type
          value: OrderShipped
        columns:
          - name: event_type
            type: varchar
          - name: order_id
            type: integer
          - name: shipped_at
            type: timestamp
```

**Janela de tempo** — `default_window` limita toda consulta a um período recente, evitando leituras ilimitadas de tópicos de alto volume. (REQ-148) Formato: `1h`, `30m`, `7d`, `60s`. Padrão `1h`. Auto-injetado como `WHERE _timestamp >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR`. Clientes podem sobrepor com seu próprio filtro `_timestamp` no argumento `where` do GraphQL.

**Discriminador** — Múltiplas configurações de tópico podem apontar para o mesmo tópico Kafka físico com valores de `discriminator` diferentes, produzindo tipos GraphQL separados. (REQ-149) O discriminador é auto-injetado como uma cláusula WHERE.

**Origem do esquema**

| Valor | Comportamento |
|-------|-------|
| `registry` | Busca o esquema no Confluent Schema Registry |
| `manual` | Define colunas inline na configuração (sem necessidade de Schema Registry) |
| `sample` | Descobre automaticamente a partir de mensagens de amostra |

#### Snowflake [tool-verified: `provisa/executor/drivers/snowflake.py:48-62`]

`account` em `federation_hints` é obrigatório. `warehouse`, `role` e `schema` são opcionais.

```yaml
sources:
  - id: my-snowflake
    type: snowflake
    host: org.snowflakecomputing.com
    username: svc_provisa
    password: ${env:SNOWFLAKE_PASSWORD}
    database: MY_DB
    federation_hints:
      account: myorg-myaccount     # required
      warehouse: COMPUTE_WH
      role: PROVISA_ROLE
      schema: PUBLIC               # remote schema override
```

#### Databricks [tool-verified: `provisa/executor/drivers/databricks.py:34-52`]

`http_path` em `federation_hints` é obrigatório. `password` carrega o personal access token. `catalog` é opcional (carregado em SQL/hints, não no campo `database`).

```yaml
sources:
  - id: my-databricks
    type: databricks
    host: my-workspace.azuredatabricks.net
    password: ${env:DATABRICKS_TOKEN}
    federation_hints:
      http_path: /sql/1.0/warehouses/xxxx   # required
      catalog: my_unity_catalog              # optional
```

#### BigQuery [tool-verified: `provisa/federation/connector_duckdb.py:238`]

`project` em `federation_hints` é obrigatório. A autenticação usa `GOOGLE_APPLICATION_CREDENTIALS` (caminho para um arquivo de chave de service account) ou Application Default Credentials no ambiente do motor.

```yaml
sources:
  - id: my-bigquery
    type: bigquery
    federation_hints:
      project: my-gcp-project     # required
```

#### Fabric / Synapse [tool-verified: `provisa/core/models.py:56-57`]

Ambos usam T-SQL sobre TDS com autenticação Azure AD. Autentique-se com `az login` (desenvolvedor) ou uma identidade gerenciada (produção) — o motor lê as credenciais via `DefaultAzureCredential` do `azure-identity`. Os detalhes de conexão vêm de variáveis de ambiente: `FABRIC_SQL_SERVER` / `FABRIC_DATABASE` (Fabric) ou `SYNAPSE_SQL_SERVER` / `SYNAPSE_DATABASE` (Synapse), ou via `PROVISA_ENGINE_URL`.

```yaml
sources:
  - id: my-fabric
    type: fabric
    # host/database read from FABRIC_SQL_SERVER / FABRIC_DATABASE when not set here
```

#### ClickHouse [tool-verified: `provisa/executor/drivers/clickhouse.py:49-59`]

`secure` em `federation_hints` habilita TLS na interface HTTP. A porta padrão é `8123` (simples) ou `8443` (quando `secure: "true"`). `schema` em `federation_hints` sobrepõe o esquema remoto. [tool-verified: `provisa/federation/connector_duckdb.py:378-379`]

```yaml
sources:
  - id: my-clickhouse
    type: clickhouse
    host: ch.example.com
    password: ${env:CLICKHOUSE_PASSWORD}
    federation_hints:
      secure: "true"    # uses port 8443; omit to use 8123
      schema: analytics
```

#### Delta Lake / Iceberg [tool-verified: `provisa/federation/connector_duckdb.py:291-327`]

`path` é o URI da tabela (S3, GCS, ADLS ou local). O acesso a armazenamento de objetos precisa de credenciais em `federation_hints`. Para Cloudflare R2, adicione `account_id`.

```yaml
sources:
  - id: events-delta
    type: delta_lake
    path: s3://my-bucket/data/events
    federation_hints:
      access_key_id: ${env:S3_ACCESS_KEY}
      secret_access_key: ${env:S3_SECRET}

  - id: r2-parquet
    type: parquet
    path: s3://my-bucket/data/events.parquet
    federation_hints:
      access_key_id: ${env:R2_ACCESS_KEY}
      secret_access_key: ${env:R2_SECRET}
      account_id: ${env:R2_ACCOUNT_ID}   # Cloudflare R2 account (S3-compatible)
```

#### Hive / Hive S3 [tool-verified: `provisa/federation/trino_connectors.py:244-363`]

`host` e `port` apontam para o metastore Thrift do Hive (porta padrão 9083). Para `hive`, defina `mapping["storage"]` para escolher o backend de armazenamento de objetos. Chaves obrigatórias ausentes falham de forma explícita — sem fallback. [tool-verified: `provisa/federation/trino_connectors.py:328-331`]

`hive_s3` é um tipo distinto que sempre declara armazenamento S3 (REQ-229); não precisa de `mapping.storage`.

```yaml
sources:
  - id: hive-s3-lake
    type: hive
    host: metastore.internal
    port: 9083
    mapping:
      storage: s3
      endpoint: https://s3.us-east-1.amazonaws.com
      access_key_id: ${env:AWS_ACCESS_KEY_ID}
      secret_access_key: ${env:AWS_SECRET_ACCESS_KEY}
      region: us-east-1
      path_style: true           # required for MinIO and non-AWS S3-compatible endpoints

  - id: hive-adls-lake
    type: hive
    host: metastore.internal
    port: 9083
    mapping:
      storage: adls
      storage_account: mystorageaccount
      access_key: ${env:ADLS_ACCESS_KEY}
      # sas_token: ${env:ADLS_SAS_TOKEN}   # alternative to access_key
```

Valores aceitos por `mapping.storage`: `hadoop` (padrão), `hdfs`, `local`, `s3`, `azure`, `adls`. Chaves de mapeamento S3: `endpoint`, `access_key_id`, `secret_access_key`, `region`, `path_style`. Chaves de mapeamento ADLS: `storage_account`, `access_key` ou `sas_token`.

#### Redis [tool-verified: `provisa/core/trino_catalog_files.py:54-75`]

Usa a DSL `mapping`. `mongodb` e `cassandra` usam campos de conexão simples e NÃO usam a DSL de mapeamento.

```yaml
sources:
  - id: my-redis
    type: redis
    host: redis.internal
    port: 6379
    password: ${env:REDIS_PASSWORD}
    mapping:
      tables:
        - name: sessions
          key_pattern: "session:*"
          key_column: key           # default "key"
          value_type: hash          # hash | string | zset | list; default hash
          columns:
            - name: user_id
              data_type: VARCHAR
              field: user_id        # Redis hash field name
            - name: expires_at
              data_type: BIGINT
              field: expires_at
```

#### Elasticsearch [tool-verified: `provisa/core/trino_catalog_files.py:78-104`]

```yaml
sources:
  - id: my-es
    type: elasticsearch
    host: es.internal
    port: 9200
    username: elastic
    password: ${env:ES_PASSWORD}
    mapping:
      tls: true
      tables:
        - name: logs
          index: app-logs-*
          discover: false
          columns:
            - name: timestamp
              data_type: TIMESTAMP
              path: "@timestamp"
            - name: level
              data_type: VARCHAR
              path: level
            - name: message
              data_type: VARCHAR
              path: message
```

#### Prometheus [tool-verified: `provisa/core/trino_catalog_files.py:107-124`]

`mapping.url` sobrepõe `host:port` quando ambos estão presentes.

```yaml
sources:
  - id: my-prometheus
    type: prometheus
    mapping:
      url: http://prometheus.internal:9090
      tables:
        - name: http_requests
          metric: http_requests_total
          labels_as_columns: [method, status, handler]
          value_column: value      # default "value"
          default_range: 1h        # default "1h"
```

#### Google Sheets [tool-verified: `provisa/federation/connector_duckdb.py:273-275`]

`spreadsheet_id` em `federation_hints` é obrigatório. A autenticação usa um SECRET `gsheet` do DuckDB provisionado no momento do attach.

```yaml
sources:
  - id: my-sheet
    type: google_sheets
    federation_hints:
      spreadsheet_id: 1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms
```

#### Fontes de arquivo (csv / parquet / sqlite / files)

`path` é obrigatório. `files` percorre um diretório em busca de arquivos CSV, Parquet, XLSX e JSON, expondo cada um como uma tabela. Todas as fontes baseadas em arquivo são VIRTUAL (roteiam pelo motor; sem pool direto). [tool-verified: `provisa/transpiler/router.py:44-48`]

```yaml
sources:
  - id: orders-csv
    type: csv
    path: /data/orders.csv

  - id: data-lake-dir
    type: files
    path: /data/lake/         # directory; each file becomes a table
```

#### Fontes de API / Remotas

**openapi** — defina `base_url` para a URL base do OpenAPI. A descoberta de esquema lê a especificação OpenAPI na inicialização.

```yaml
sources:
  - id: payment-api
    type: openapi
    base_url: https://api.payments.example.com/v1
```

**graphql_remote** — defina `base_url`. Chaves opcionais de `mapping`: `headers` (dict de cabeçalhos estáticos), `forward_client_headers` (bool), `timeout_seconds` (int). [tool-verified: `provisa/hasura_v2/mapper.py:129-152`]

```yaml
sources:
  - id: orders-gql
    type: graphql_remote
    base_url: https://orders.internal/graphql
    mapping:
      headers:
        X-Api-Key: ${env:ORDERS_API_KEY}
      forward_client_headers: true
      timeout_seconds: 30
```

**airport** — `base_url` é a localização do servidor Arrow Flight. Extensão airport do DuckDB (REQ-899). [tool-verified: `provisa/federation/connector_duckdb.py:285-288`]

```yaml
sources:
  - id: flight-source
    type: airport
    base_url: grpc://flight.internal:8815
```

**websocket / rss** — use `host`, `port`, `path` e `federation_hints`. [tool-verified: `provisa/api/data/subscribe.py:85-129`]

```yaml
sources:
  - id: market-feed
    type: websocket
    host: feed.example.com
    port: 443
    path: /ws/v1
    federation_hints:
      use_ssl: "true"
      subscribe_payload: '{"action":"subscribe","channels":["ticker"]}'
      event_path: data

  - id: news-rss
    type: rss
    host: feeds.example.com
    port: 443
    path: /rss/latest
    federation_hints:
      use_ssl: "true"
      poll_interval: "300"      # seconds
      # feed_url: https://...  # overrides host/port/path when set
```

**sharepoint** [tool-verified: `provisa/federation/trino_connectors.py:394-423`]

```yaml
sources:
  - id: my-sharepoint
    type: sharepoint
    base_url: https://myorg.sharepoint.com/sites/data
    username: ${env:SP_CLIENT_ID}
    password: ${env:SP_CLIENT_SECRET}
    database: ${env:SP_TENANT_ID}
    mapping:
      auth_type: CLIENT_CREDENTIALS   # default
      # certificate_path: /path/to/cert.pem
      # certificate_password: ${env:CERT_PASSWORD}
```

**splunk** [tool-verified: `provisa/federation/trino_connectors.py:426-457`]

```yaml
sources:
  - id: my-splunk
    type: splunk
    host: splunk.internal
    port: 8089
    password: ${env:SPLUNK_TOKEN}
    database: search           # Splunk app name (optional)
    mapping:
      use_token: true          # default; false = username/password auth
      datamodel_filter: ""     # optional Splunk Data Model filter
      disable_ssl_validation: false
```

## Domínios

```yaml
domains:
  - id: sales-analytics
    description: Sales operational data
```

## Nomenclatura

```yaml
naming:
  convention: apollo_graphql   # snake, hasura_graphql, apollo_graphql (default)
  domain_prefix: true          # prepend domain_id__ to all GraphQL names
  rules:
    - pattern: "^prod_pg_"
      replace: ""
```

### Convenção de nomenclatura

A autoridade de nomenclatura é a única fonte de verdade para nomes voltados ao cliente; nomes físicos de colunas do backend nunca são expostos aos clientes. (REQ-194) Cada linguagem de consulta deriva o nome de uma coluna a partir de seu `column.alias`, se definido, senão a partir do nome físico da coluna via sua convenção configurada. (REQ-194)

A convenção GraphQL é uma de três enums predefinidos. (REQ-416) Strings de formato livre antigas (`none`, `snake_case`, `camelCase`, `PascalCase`) estão descontinuadas. (REQ-416)

| Preset | Padrão | Nomes de tipo | Nomes de campo | Nomes de mutação |
|--------|---------|------------|-------------|----------------|
| `apollo_graphql` | sim | PascalCase | camelCase | camelCase |
| `hasura_graphql` | | PascalCase | camelCase | snake_case |
| `snake` | | PascalCase | snake_case | snake_case |

A convenção GraphQL padrão é `apollo_graphql`, que produz nomes de campo e mutação em camelCase. (REQ-194, REQ-416) A convenção SQL é separada, com padrão `snake_case`, aplicada via `apply_sql_name()`; a convenção GraphQL é aplicada via `apply_gql_name()`, e o nome CQL é derivado do nome GraphQL. (REQ-194)

`domain_prefix: bool` é uma opção ortogonal que se aplica independentemente do preset escolhido. (REQ-416)

Um `column.alias` explícito é o nome canônico: SQL o usa literalmente sem aplicar convenção, GraphQL aplica sua convenção a ele, e CQL deriva do nome GraphQL. (REQ-194)

Sobreposição por fonte:
```yaml
sources:
  - id: legacy-db
    naming_convention: hasura_graphql  # overrides global for this source
```

Sobreposição por tabela:
```yaml
tables:
  - source_id: legacy-db
    table: orders
    naming_convention: snake  # overrides source for this table
```

### Prefixo de domínio

Quando `domain_prefix: true`, todos os nomes de campo e tipo GraphQL recebem o prefixo do ID de domínio usando um separador de sublinhado duplo: (REQ-154)

| Tabela | Domínio | Nome do campo |
|-------|--------|-----------|
| `orders` | `sales-analytics` | `sales_analytics__orders` |
| `customer_segments` | `customer-insights` | `customer_insights__customer_segments` |

Isso evita colisões de nome quando domínios diferentes têm tabelas com o mesmo nome, e torna as consultas autoexplicativas.

### Regras de nomenclatura

Regras de regex aplicadas a nomes de tabela ao gerar nomes de campo GraphQL. Aplicadas em ordem antes da resolução de unicidade. (REQ-542)

## Tabelas

```yaml
tables:
  - source_id: sales-pg
    domain_id: sales-analytics
    schema: public
    table: orders
    alias: purchase_orders     # optional: override GraphQL name
    description: "Customer purchase orders"  # optional: GraphQL description
    columns:
      - name: id
        visible_to: [admin, analyst]
        writable_by: []           # read-only (empty = no writes)
      - name: email
        visible_to: [admin, analyst]
        writable_by: [admin]      # only admin can mutate
        unmasked_to: [admin]      # admin sees raw, analyst sees masked
        mask_type: regex
        mask_pattern: "^(.{2}).*(@.*)$"
        mask_replace: "$1***$2"
        alias: email_address      # optional: override GraphQL field name
        description: "Primary email address"  # optional: appears in SDL
      - name: amount
        visible_to: [admin]
        writable_by: [admin]
        unmasked_to: [admin]
        mask_type: constant
        mask_value: "0"
      - name: created_at
        visible_to: [admin, analyst]
        writable_by: []           # nobody can write
        unmasked_to: [admin]
        mask_type: truncate
        mask_precision: month
    column_presets:               # auto-set values on insert/update
      - column: created_by
        source: header            # from request header
        name: X-User-ID
      - column: updated_at
        source: now               # current timestamp
```

### Aliases

Aliases de tabela e coluna sobrepõem o nome GraphQL padrão. (REQ-155) Úteis para:
- Renomear nomes de banco de dados crípticos (ex.: `tbl_cust_seg` → `customer_segments`)
- Evitar abreviações na camada de API
- Criar um vocabulário limpo e específico do domínio

### Descrições

Descrições de tabela e coluna são incluídas no SDL GraphQL gerado. (REQ-156) Elas aparecem no explorador de documentação do GraphiQL e em consultas de introspecção. Defina-as no YAML de configuração ou via a UI de administração.

### Path (Extração JSON computada)

Colunas podem extrair valores de uma coluna de origem JSON/JSONB usando um `path` em notação de ponto. (REQ-151) Útil para dados semiestruturados em mensagens Kafka, documentos MongoDB ou colunas JSONB do PostgreSQL.

```yaml
columns:
  - name: payload
    type: varchar
    visible_to: []            # hide the raw JSON column
  - name: order_id
    type: integer
    path: payload.order_id    # extracts from payload column
    visible_to: [admin, analyst]
  - name: customer_name
    type: varchar
    path: payload.customer.name
    visible_to: [admin, analyst]
```

O formato do path é `source_column.key1.key2...`. O compilador gera `json_extract_scalar(source_column, '$.key1.key2')` no SQL. (REQ-151)

**Impacto no roteamento:** Colunas com path usam operadores JSON do PostgreSQL (`->>`), suportados nativamente pelo roteamento direto ao PG. (REQ-152) Para fontes não-PostgreSQL (MySQL, SQL Server, etc.), consultas com colunas de path são automaticamente roteadas pelo motor de federação. (REQ-152) Mutações não são afetadas, já que colunas de path são campos computados somente-leitura. (REQ-153)

### Tipos de mascaramento

| Tipo | Campos | Descrição |
|------|--------|-------------|
| `regex` | `pattern`, `replace` | REGEXP_REPLACE (apenas colunas string) |
| `constant` | `value` | Substituição literal (NULL, 0, MAX, MIN, personalizado) |
| `truncate` | `precision` | DATE_TRUNC (apenas colunas date/timestamp) |

## Relacionamentos

```yaml
relationships:
  - id: orders-to-customers
    source_table_id: orders
    target_table_id: customers
    source_column: customer_id
    target_column: id
    cardinality: many-to-one   # or: one-to-many

  - id: orders-to-reviews
    source_table_id: orders        # sales-pg source
    target_table_id: product_reviews  # reviews-mongo source
    source_column: product_id
    target_column: product_id
    cardinality: one-to-many
    materialize: true              # auto-create MV for this cross-source join
    refresh_interval: 600          # refresh every 10 minutes
```

### Auto-materialização

Defina `materialize: true` em um relacionamento para gerar automaticamente uma view materializada para JOINs entre fontes. (REQ-158) Isso evita consultas federadas custosas pré-computando o resultado do JOIN.

- Apenas relacionamentos entre fontes geram MVs (JOINs dentro da mesma fonte já são rápidos) (REQ-159)
- A MV começa desatualizada e é populada pelo laço de atualização em segundo plano (REQ-160)
- Mutações em qualquer uma das tabelas de origem marcam a MV como desatualizada para nova atualização (REQ-543)
- `refresh_interval` tem padrão de 300 segundos (5 minutos) (REQ-543)

## Funções

```yaml
roles:
  - id: admin
    capabilities:
      - source_registration
      - table_registration
      - relationship_registration
      - security_config
      - query_development
      - full_results
      - admin
    domain_access: ["*"]
  - id: analyst
    capabilities: [query_development]
    domain_access: [sales-analytics]
  - id: junior_analyst
    capabilities: []
    domain_access: [sales-analytics]
    parent_role_id: analyst      # inherits query_development + sales-analytics
```

Funções com `parent_role_id` herdam capacidades e acesso a domínio da função pai. (REQ-215) A hierarquia é achatada na inicialização. (REQ-215)

### Capacidades

| Capacidade | Descrição |
|-----------|-------------|
| `source_registration` | Registrar fontes de dados |
| `table_registration` | Registrar tabelas |
| `relationship_registration` | Definir relacionamentos |
| `security_config` | Configurar RLS, mascaramento |
| `query_development` | Executar consultas |
| `full_results` | Ignorar limites de amostragem |
| `admin` | Todas as capacidades |

## Regras de RLS

```yaml
rls_rules:
  - table_id: orders
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"
```

## Views Materializadas

```yaml
materialized_views:
  - id: mv-orders-customers
    source_tables: [orders, customers]
    join_pattern:
      left_table: orders
      left_column: customer_id
      right_table: customers
      right_column: id
      join_type: left
    target_catalog: postgresql
    target_schema: mv_cache
    refresh_interval: 300
    enabled: true
```

## Views (conjuntos de dados computados governados)

Views são conjuntos de dados computados definidos por SQL com governança completa em nível de coluna. (REQ-133) São o mecanismo governado para adicionar agregações, transformações e métricas derivadas à camada semântica. (REQ-136)

```yaml
views:
  - id: monthly-revenue
    sql: |
      SELECT DATE_TRUNC('month', created_at) AS month,
             region,
             SUM(amount) AS revenue,
             COUNT(*) AS order_count
      FROM orders
      GROUP BY 1, 2
    description: "Monthly revenue by region"
    domain_id: sales-analytics
    materialize: true
    refresh_interval: 3600
    columns:
      - name: month
        visible_to: [admin, analyst]
      - name: region
        visible_to: [admin, analyst]
      - name: revenue
        visible_to: [admin]
      - name: order_count
        visible_to: [admin, analyst]
```

| Campo | Obrigatório | Descrição |
|-------|----------|-------------|
| `id` | Sim | Identificador único da view |
| `sql` | Sim | Instrução SQL SELECT que define a view |
| `domain_id` | Sim | Domínio para visibilidade de esquema |
| `materialize` | Não | `true` = atualização periódica via CTAS, `false` = view federada ao vivo |
| `refresh_interval` | Não | Segundos entre atualizações (apenas materializadas, padrão 300) |
| `description` | Não | Aparece no SDL GraphQL |
| `alias` | Não | Sobrepõe o nome GraphQL |
| `columns` | Sim | Definições de coluna com visibilidade, mascaramento, descrições |

### Materializada vs ao vivo

- **`materialize: true`**: O Provisa cria uma tabela via CTAS e a atualiza em uma programação. (REQ-135) Consultas mais rápidas, mas os dados podem estar desatualizados em até `refresh_interval` segundos.
- **`materialize: false`**: O Provisa cria uma view federada. (REQ-135) Consultas sempre retornam dados ao vivo, mas podem ser mais lentas para agregações complexas.

Views passam pelo mesmo pipeline de governança que tabelas — RLS, mascaramento, amostragem e visibilidade baseada em função. (REQ-134) Isso garante que nenhuma semântica nova possa ser adicionada à plataforma sem supervisão de um steward. (REQ-136)

### Views somente-consulta

Views com `materialize: true` e `materialize: false` expõem seu tipo GraphQL como somente-consulta. Nenhuma mutação de insert, upsert, update ou delete é gerada para relações apoiadas em `view_sql`. (REQ-1157) [tool-verified: `provisa/compiler/schema_gen.py:184`, `provisa/compiler/schema_types.py:79`]

## Cache

```yaml
cache:
  enabled: true
  redis_url: ${env:REDIS_URL}
  default_ttl: 300
```

### Hierarquia de cache

Ordem de resolução de TTL (mais específico vence): **tabela** > **fonte** > **padrão global**. (REQ-544) O primeiro valor não nulo é usado.

```yaml
cache:
  enabled: true
  redis_url: ${env:REDIS_URL}
  default_ttl: 300              # global fallback: 5 minutes

sources:
  - id: sales-pg
    cache_enabled: true          # toggle caching for all tables in this source
    cache_ttl: 600               # source override: 10 minutes

tables:
  - source_id: sales-pg
    table: orders
    cache_ttl: 60                # table override: 1 minute (frequently changing)
  - source_id: sales-pg
    table: customers
    # no cache_ttl → inherits source TTL (600s)
```

Definir `cache_enabled: false` em uma fonte desabilita o cache para todas as tabelas dessa fonte, independentemente do TTL em nível de tabela. (REQ-544) As chaves de cache sempre incluem `role_id` + valores de contexto RLS para particionamento de segurança. (REQ-544)

## Autenticação

```yaml
auth:
  provider: simple           # none, firebase, keycloak, oauth, simple
  superuser:
    username: admin
    password: ${env:PROVISA_SUPERUSER_PASSWORD}
  simple:
    allow: true
    jwt_secret: ${env:PROVISA_JWT_SECRET}
    users:
      - username: admin
        password_hash: "$2b$12$..."
        roles: [admin]
  role_mapping:
    - claim: groups
      contains: data-analysts
      provisa_role: analyst
    default_role: analyst
```

### Tipos de provedor de autenticação

| Provedor | Caso de uso | Validação de token |
|----------|----------|-----------------|
| `simple` | Desenvolvimento/testes locais. Usuários definidos em YAML. | JWT assinado com `PROVISA_JWT_SECRET` |
| `firebase` | Firebase Authentication (todos os métodos). | `verify_id_token()` do SDK `firebase-admin` |
| `keycloak` | Keycloak OIDC. Tenant + funções de cliente mapeadas. | Validação de JWT baseada em JWKS |
| `oauth` | OIDC genérico (Okta, Azure AD, Auth0, PingFederate). | JWKS a partir da URL de descoberta |

Credenciais de superusuário (bloco `superuser`) funcionam com qualquer provedor e sempre resolvem para a função admin com todas as capacidades. (REQ-125) Usadas para configuração inicial antes que a autenticação externa seja configurada.

### Exemplo de configuração completa de autenticação (comentado)

```yaml
# auth:
#   provider: firebase
#
#   superuser:
#     username: admin
#     password: ${env:PROVISA_SUPERUSER_PASSWORD}
#
#   firebase:
#     project_id: ${env:FIREBASE_PROJECT_ID}
#     service_account_key: ${env:FIREBASE_SERVICE_ACCOUNT}
#
#   # keycloak:
#   #   server_url: https://keycloak.example.com
#   #   # kc-tenant: set to your Keycloak tenant name (e.g. provisa)
#   #   client_id: provisa-app
#   #   client_secret: ${env:KEYCLOAK_CLIENT_SECRET}
#
#   # oauth:
#   #   discovery_url: https://login.example.com/.well-known/openid-configuration
#   #   client_id: provisa
#   #   client_secret: ${env:OAUTH_CLIENT_SECRET}
#   #   role_claim: groups
#   #   audience: provisa-api
#
#   role_mapping:
#     - claim: custom_claims.role
#       value: admin
#       provisa_role: admin
#     - claim: groups
#       contains: data-analysts
#       provisa_role: analyst
#     default_role: analyst
```

## Mutações Upsert

Para tabelas com uma chave primária, o Provisa gera automaticamente campos de mutação `upsert_<table>`. (REQ-212) Eles compilam para um upsert no dialeto de destino — `INSERT ... ON CONFLICT (pk) DO UPDATE SET ...` no PostgreSQL, `ON DUPLICATE KEY UPDATE` no MySQL. (REQ-212)

```graphql
mutation {
  upsert_orders(objects: [{id: 1, amount: 150.00, region: "us"}]) {
    affected_rows
  }
}
```

Colunas de conflito são derivadas dos metadados de PK. (REQ-212) Todas as regras de visibilidade de coluna e permissão de escrita se aplicam.

## Distinct On

O argumento `distinct_on` seleciona a primeira linha para cada valor distinto das colunas especificadas. (REQ-213) Disponível nos campos de consulta raiz.

```graphql
{
  orders(distinct_on: [region], order_by: [{region: asc, created_at: desc}]) {
    region
    amount
    created_at
  }
}
```

Compila para `SELECT DISTINCT ON (region) ...` no PostgreSQL. (REQ-213) Para dialetos não-PG, um fallback com função de janela é usado. (REQ-213)

## Presets de Coluna

Injeta automaticamente valores em colunas no insert/update. (REQ-214) Definidos por tabela na configuração.

```yaml
tables:
  - source_id: sales-pg
    table: orders
    column_presets:
      - column: created_by
        source: header           # from request header
        name: X-User-ID
      - column: updated_at
        source: now              # current timestamp
      - column: source_system
        source: literal          # constant value
        value: "provisa"
```

| Origem | Comportamento |
|--------|-------|
| `header` | Injeta o valor do cabeçalho de requisição HTTP nomeado |
| `now` | Injeta `NOW()` (timestamp atual) |
| `literal` | Injeta um valor constante |

Colunas preset são injetadas durante a compilação da mutação, antes da geração de SQL. (REQ-214) Não são visíveis no tipo de entrada da mutação. (REQ-214)

## Funções Herdadas

Funções podem herdar capacidades e acesso a domínio de uma função pai via `parent_role_id`. (REQ-215) A hierarquia é achatada na inicialização. (REQ-215)

```yaml
roles:
  - id: admin
    capabilities: [admin]
    domain_access: ["*"]
  - id: analyst
    capabilities: [query_development]
    domain_access: [sales-analytics]
  - id: junior_analyst
    capabilities: []
    domain_access: []
    parent_role_id: analyst      # inherits query_development + sales-analytics
  - id: intern
    capabilities: []
    domain_access: []
    parent_role_id: junior_analyst  # inherits from junior_analyst (and transitively analyst)
```

Herança em múltiplos níveis é suportada. (REQ-215) As capacidades e domain_access explícitos da função filha são mesclados com os da função pai. (REQ-215)

## Gatilhos Programados

Gatilhos baseados em cron que chamam uma URL de webhook em uma programação. (REQ-216) Usa o APScheduler. (REQ-216)

```yaml
scheduled_triggers:
  - name: daily-report
    cron: "0 8 * * *"           # 8:00 AM daily
    webhook_url: https://hooks.example.com/daily-report
    enabled: true
  - name: hourly-sync
    cron: "0 * * * *"           # every hour
    webhook_url: https://hooks.example.com/sync
    enabled: false
```

Tarefas programadas são gerenciadas via a UI de administração (alternância habilitar/desabilitar) ou a mutação de administração `toggle_scheduled_task`. (REQ-216)

## Formato OrderBy

OrderBy usa o formato `{column: direction}` com um enum de direção de 6 valores: (REQ-200, REQ-201)

```graphql
{
  orders(order_by: [{created_at: desc_nulls_last}, {amount: asc}]) {
    id
    created_at
    amount
  }
}
```

| Direção | SQL |
|-----------|-----|
| `asc` | `ASC` |
| `desc` | `DESC` |
| `asc_nulls_first` | `ASC NULLS FIRST` |
| `asc_nulls_last` | `ASC NULLS LAST` |
| `desc_nulls_first` | `DESC NULLS FIRST` |
| `desc_nulls_last` | `DESC NULLS LAST` |

Ordenação por relacionamento é suportada via objetos aninhados: (REQ-202)

```graphql
{
  orders(order_by: [{customers: {name: asc}}]) {
    id
    customers { name }
  }
}
```

## Observabilidade

```yaml
observability:
  endpoint: "http://localhost:4319"   # OTLP collector; env OTEL_EXPORTER_OTLP_ENDPOINT overrides
  service_name: provisa               # env OTEL_SERVICE_NAME overrides
  sample_rate: 1.0                    # 0.0–1.0; TraceIdRatioBased sampler
  log_level: WARNING                  # env OTEL_LOG_LEVEL overrides
  compact_batch_size: 1000
  telemetry_filter:
    redact_sql_literals: false        # strip literal values from db.statement before export
    redact_attributes: []             # attribute keys dropped entirely before export
  # support_endpoint: ""              # env PROVISA_SUPPORT_OTLP_ENDPOINT; off by default
  support_telemetry_filter:
    redact_sql_literals: true         # default on — strip literals before sending to support
    redact_attributes: []             # additional keys dropped before sending to support
```

### Filtros de Telemetria [tool-verified]

O Provisa mantém dois caminhos de exportação OTLP independentes: seu coletor interno e o endpoint de suporte Provisa opcional. (REQ-545) Cada caminho tem seu próprio filtro. Os filtros rodam dentro de um `_FilteringExporter` envolvente antes que os spans deixem o processo — os objetos de span originais nunca são alterados. (REQ-546) [tool-verified: `provisa/api/otel_setup.py` lines 156–207]

**`telemetry_filter`** — controla o que chega ao seu coletor interno.

| Chave | Tipo | Padrão | Descrição |
|-----|------|---------|-------------|
| `redact_sql_literals` | bool | `false` | Substitui literais string e numéricos em `db.statement` por `?` |
| `redact_attributes` | list[str] | `[]` | Chaves de atributo removidas completamente de todo span |

**`support_telemetry_filter`** — controla o que chega ao endpoint de suporte Provisa. A redação de literais SQL tem padrão `true` neste caminho, já que os dados de consulta pertencem a você. (REQ-547) [tool-verified: `provisa/api/otel_setup.py` line 240]

| Chave | Tipo | Padrão | Descrição |
|-----|------|---------|-------------|
| `redact_sql_literals` | bool | `true` | Substitui literais string e numéricos em `db.statement` por `?` |
| `redact_attributes` | list[str] | `[]` | Chaves de atributo removidas completamente de todo span |

Exemplo de `db.statement` com redação — com `redact_sql_literals: true`, este atributo de span:

```
db.statement: SELECT * FROM orders WHERE region = 'us-west' AND amount > 500
```

torna-se:

```
db.statement: SELECT * FROM orders WHERE region = ? AND amount > ?
```

### Endpoint de Suporte [tool-verified]

`support_endpoint` (ou env `PROVISA_SUPPORT_OTLP_ENDPOINT`) encaminha telemetria ao suporte Provisa para diagnóstico. (REQ-548) Quando não definido, nenhum dado sai da sua infraestrutura por este caminho. (REQ-548) O filtro de suporte se aplica independentemente do filtro interno — você pode redigir literais SQL de ambas as exportações e ainda compartilhar dados de tempo de span e erro com o suporte. (REQ-545) [tool-verified: `provisa/api/otel_setup.py` lines 238–288]

### Detecção de Protocolo do Endpoint [tool-verified]

O Provisa seleciona OTLP/HTTP ou OTLP/gRPC a partir do esquema da URL do endpoint. (REQ-549) URLs que começam com `http://` ou `https://` usam OTLP/HTTP, com `/v1/traces`, `/v1/metrics` e `/v1/logs` anexados automaticamente. (REQ-549) Qualquer outro esquema usa OTLP/gRPC com `insecure=True`. (REQ-549) [tool-verified: `provisa/api/otel_setup.py` lines 60–70]

## Motor de Federação

Configurar um motor de federação é opcional. O padrão é `duckdb` — zero configuração, em processo, sem serviço externo necessário (REQ-989). Escolha outro motor quando precisar de escala MPP ou quiser reaproveitar um warehouse existente.

Precedência: variável de ambiente `PROVISA_ENGINE` → campo de configuração `federation_engine` persistido na UI de administração → `duckdb`. As mudanças têm efeito na reinicialização do serviço. [tool-verified: `engine.py` `build_engine`]

### Visão geral dos motores [tool-verified: `engine.py` `ENGINE_REGISTRY`, `_ENGINE_BUILDERS`]

| Chave do motor | Rótulo | Dialeto | MPP | Mecanismo de link externo | Autenticação |
|-----------|-------|---------|-----|------------------------|------|
| `trino` | Provisa Federation Engine | Trino SQL | Sim | Catálogos Trino (amplo conjunto de conectores) | Credenciais JDBC |
| `trino-byo` | Trino (traga o seu) | Trino SQL | Sim | Mesmo que `trino`; coordenador não gerenciado | Credenciais JDBC |
| `pg` | PostgreSQL | PostgreSQL | Não | FDW / pg_duckdb | Credenciais PostgreSQL |
| `duckdb` | DuckDB | DuckDB | Não | ATTACH nativo de extensão | Nenhuma (em processo) |
| `clickhouse` | ClickHouse (embutido) | ClickHouse | Sim | Motores de tabela S3 / IcebergS3 / DeltaLake | chdb (em processo, sem autenticação) |
| `clickhouse-server` | ClickHouse (Server / Cloud) | ClickHouse | Sim | Motores de tabela S3 / IcebergS3 / DeltaLake | Credenciais ClickHouse |
| `snowflake` | Snowflake | Snowflake | Sim | External stage + tabela externa | `PROVISA_ENGINE_URL` |
| `databricks` | Databricks | Databricks SQL | Sim | Tabelas externas Unity Catalog via REST | `PROVISA_ENGINE_URL` (bearer token + `http_path`) |
| `bigquery` | BigQuery | BigQuery | Sim | Tabelas externas BigQuery / BigLake | `GOOGLE_APPLICATION_CREDENTIALS` |
| `fabric` | Microsoft Fabric | T-SQL | Sim | Atalhos OneLake → OPENROWSET | Azure AD (`az login` ou identidade gerenciada) |
| `synapse` | Azure Synapse | T-SQL | Sim | ADLS OPENROWSET / tabelas externas | Azure AD |
| `sqlalchemy` | SQLAlchemy (qualquer RDB) | Por dialeto | Não | Nenhum (apenas pouso) | Credenciais por dialeto |

### Referência de motores

#### trino / trino-byo

`trino` é o coordenador Provisa gerenciado; `trino-byo` conecta ao seu próprio cluster Trino. Ambos usam Trino SQL e têm o maior alcance de tipos de fonte.

```bash
PROVISA_ENGINE=trino
TRINO_HOST=trino.internal
TRINO_PORT=8080
```

O armazenamento de materialização usa `TENANT_DATABASE_URL` (PostgreSQL) como padrão.

#### pg

Federa via extensões postgres_fdw (SQL/MED) e pg_duckdb. Nó único; sem MPP. Melhor quando seus dados já vivem no PostgreSQL e você quer unir algumas fontes remotas.

```bash
PROVISA_ENGINE=pg
# Connection uses the standard PG_* env vars
```

O armazenamento de materialização usa `TENANT_DATABASE_URL` como padrão.

#### duckdb

Em processo; sem serviço externo. O motor padrão (REQ-989). `PROVISA_DATA_DIR` controla onde o armazenamento embutido vive (`~/.provisa` por padrão).

```bash
PROVISA_ENGINE=duckdb   # or omit — this is the default
```

O armazenamento de materialização usa `~/.provisa/materialize.duckdb` como padrão — o único motor com um armazenamento padrão não-PostgreSQL.

#### clickhouse (embutido) / clickhouse-server

`clickhouse` usa chdb (em processo). `clickhouse-server` conecta a uma instância ClickHouse externa ou ClickHouse Cloud. Ambos leem Delta Lake, Iceberg e Hudi diretamente via motores de tabela nativos do ClickHouse.

```bash
# External server
PROVISA_ENGINE=clickhouse-server
PROVISA_ENGINE_URL="clickhouse://user:pass@host:9000/db"
```

O armazenamento de materialização usa `TENANT_DATABASE_URL` como padrão.

#### snowflake

Motor-como-warehouse: o Snowflake executa as consultas; o Provisa envia dados de origem via external stages.

```bash
PROVISA_ENGINE=snowflake
PROVISA_ENGINE_URL="snowflake://user:pass@account/db/schema?warehouse=WH"
```

O armazenamento de materialização usa `TENANT_DATABASE_URL` como padrão.

#### databricks

Tabelas externas do Unity Catalog conectam fontes gerenciadas pelo Provisa ao Databricks SQL.

```bash
PROVISA_ENGINE=databricks
PROVISA_ENGINE_URL="databricks://token:TOKEN@my-workspace.azuredatabricks.net?http_path=/sql/1.0/warehouses/xxxx"
```

O armazenamento de materialização usa `TENANT_DATABASE_URL` como padrão.

#### bigquery

Tabelas externas e BigLake do BigQuery. O projeto vem da URL ou de `GOOGLE_CLOUD_PROJECT`; autenticação via chave de service account.

```bash
PROVISA_ENGINE=bigquery
PROVISA_ENGINE_URL="bigquery://my-project?location=US"
# GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
```

O armazenamento de materialização usa `TENANT_DATABASE_URL` como padrão.

#### fabric / synapse

Ambos usam T-SQL sobre TDS com autenticação Azure AD (`az login` ou identidade gerenciada). Omita `PROVISA_ENGINE_URL` para ler os detalhes de conexão de variáveis de ambiente.

```bash
PROVISA_ENGINE=fabric
# FABRIC_SQL_SERVER=...   FABRIC_DATABASE=...
# or: PROVISA_ENGINE_URL set explicitly

PROVISA_ENGINE=synapse
# SYNAPSE_SQL_SERVER=...  SYNAPSE_DATABASE=...
```

O armazenamento de materialização usa `TENANT_DATABASE_URL` como padrão.

#### sqlalchemy

Motor RDBMS genérico apenas para pouso (sem federação para fontes externas). Use para implantações de warehouse único ou testes.

```bash
PROVISA_ENGINE=sqlalchemy
PROVISA_ENGINE_URL="postgresql+psycopg2://user:pass@host/db"
```

O armazenamento de materialização usa `TENANT_DATABASE_URL` como padrão.

### Armazenamento de materialização

Quando uma fonte não pode conectar (attach) ao vivo (sem conector ATTACH para o motor selecionado), ela pousa no armazenamento de materialização do motor. Ordem de resolução: `PROVISA_MATERIALIZE_URL` explícito → padrão declarado do motor → erro explícito (sem fallback silencioso). [tool-verified: `engine.py` `materialize_store`]

O DuckDB declara seu arquivo embutido (`~/.provisa/materialize.duckdb`) como padrão. Todos os outros motores usam `TENANT_DATABASE_URL` (PostgreSQL) como padrão. Sobreponha qualquer motor com `PROVISA_MATERIALIZE_URL`.

### Hints de federação por fonte

Parâmetros de conexão estendidos que os campos padrão host/port/user/password não conseguem carregar vão em `federation_hints` na fonte. Veja a referência de tipos de fonte acima para as chaves de hint por tipo. Um exemplo consolidado:

```yaml
sources:
  - id: my-databricks
    type: databricks
    host: my-workspace.azuredatabricks.net
    password: ${env:DATABRICKS_TOKEN}
    federation_hints:
      http_path: /sql/1.0/warehouses/xxxx   # required for Databricks sources

  - id: my-snowflake
    type: snowflake
    host: org.snowflakecomputing.com
    username: svc_provisa
    password: ${env:SNOWFLAKE_PASSWORD}
    federation_hints:
      account: myorg-myaccount
      warehouse: COMPUTE_WH

  - id: my-clickhouse
    type: clickhouse
    host: ch.example.com
    port: 8123
    password: ${env:CLICKHOUSE_PASSWORD}
    federation_hints:
      secure: "true"           # enable TLS on the HTTP interface

  - id: r2-parquet
    type: parquet
    path: s3://my-bucket/data/events.parquet
    federation_hints:
      access_key_id: ${env:R2_ACCESS_KEY}
      secret_access_key: ${env:R2_SECRET}
      account_id: ${env:R2_ACCOUNT_ID}   # Cloudflare R2 account (S3-compatible)
```

Para fontes Google Cloud, defina `GOOGLE_APPLICATION_CREDENTIALS` para o caminho do arquivo de chave da sua service account. Para Fabric e Synapse, autentique-se com `az login` (desenvolvedor) ou uma identidade gerenciada (produção) — o motor lê as credenciais via `DefaultAzureCredential` do `azure-identity`.

## Variáveis de Ambiente

| Variável | Padrão | Descrição |
|----------|---------|-------------|
| `PROVISA_CONFIG` | `config/provisa.yaml` | Caminho do arquivo de configuração |
| `TENANT_DATABASE_URL` | `postgresql+asyncpg://provisa:provisa@localhost:5432/provisa` | URI do armazenamento do plano de controle (SQLAlchemy async); aceita `sqlite+aiosqlite://…` / `duckdb://…` para o armazenamento desktop embutido (REQ-828, REQ-850) |
| `PLATFORM_DATABASE_URL` | — | URI do registro de plataforma (diretório de tenants, registro de motores); obrigatório na inicialização, sem fallback (REQ-837) |
| `PROVISA_REDIS_EMBEDDED` | — | `1`/`true` usa fakeredis embutido em vez de um servidor Redis — sem Docker (REQ-829) |
| `PG_HOST` | `localhost` | Host PostgreSQL |
| `PG_PORT` | `5432` | Porta PostgreSQL |
| `PG_DATABASE` | `provisa` | Banco de dados PostgreSQL |
| `PG_USER` | `provisa` | Usuário PostgreSQL |
| `PG_PASSWORD` | `provisa` | Senha PostgreSQL |
| `PROVISA_ENGINE` | `duckdb` | Chave do motor de federação (REQ-989) |
| `PROVISA_ENGINE_URL` | — | URL de conexão para motores orientados a URL (Snowflake, Databricks, ClickHouse Server, BigQuery, SQLAlchemy) |
| `PROVISA_MATERIALIZE_URL` | — | Sobrepõe o DSN do armazenamento de materialização (padrão é o declarado pelo motor) |
| `PROVISA_DATA_DIR` | `~/.provisa` | Diretório de dados para o armazenamento DuckDB embutido (REQ-989) |
| `TRINO_HOST` | `localhost` | Host do coordenador Trino |
| `TRINO_PORT` | `8080` | Porta HTTP do coordenador Trino |
| `GOOGLE_APPLICATION_CREDENTIALS` | — | Caminho para o JSON de chave de service account GCP (motor/fonte BigQuery) |
| `GOOGLE_CLOUD_PROJECT` | — | Projeto GCP padrão (BigQuery; sobreposto pela URL) |
| `FABRIC_SQL_SERVER` | — | Endpoint SQL do Fabric Warehouse (alternativa a `PROVISA_ENGINE_URL`) |
| `FABRIC_DATABASE` | — | Nome do banco de dados Fabric Warehouse |
| `SYNAPSE_SQL_SERVER` | — | Endpoint SQL serverless do Synapse |
| `SYNAPSE_DATABASE` | — | Nome do banco de dados Synapse |
| `REDIS_URL` | — | URL de conexão Redis |
| `PROVISA_SAMPLE_SIZE` | `10000` | Limite de amostragem padrão |
| `PROVISA_DEFAULT_ROW_LIMIT` | `100` | Limite de linhas quando uma consulta não fornece um `LIMIT` explícito |
| `PROVISA_RETRY_BUDGET_SECS` | `30` | Orçamento de retry de leitura de Tier-1 em segundos; backoff exponencial com jitter completo (REQ-703) |
| `ZAYCHIK_PORT` | `8480` | Porta do proxy Zaychik Flight SQL |
| `FLIGHT_PORT` | `8815` | Porta do servidor Arrow Flight do Provisa |
| `GRPC_PORT` | `50051` | Porta do servidor gRPC Protobuf do Provisa |
| `PROVISA_REDIRECT_ENABLED` | `false` | Habilita redirecionamento por limite do lado do servidor |
| `PROVISA_REDIRECT_THRESHOLD` | `1000` | Limite padrão de contagem de linhas |
| `PROVISA_REDIRECT_FORMAT` | `parquet` | Formato de redirecionamento padrão |
| `PROVISA_REDIRECT_BUCKET` | `provisa-results` | Bucket S3 para resultados redirecionados |
| `PROVISA_REDIRECT_ENDPOINT` | — | URL de endpoint compatível com S3 |
| `PROVISA_REDIRECT_ACCESS_KEY` | — | Chave de acesso S3 |
| `PROVISA_REDIRECT_SECRET_KEY` | — | Chave secreta S3 |
| `PROVISA_REDIRECT_TTL` | `3600` | TTL da URL pré-assinada (segundos) |
| `ANTHROPIC_API_KEY` | — | Chave de API Claude (descoberta) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | Sobrepõe `observability.endpoint` |
| `OTEL_SERVICE_NAME` | `provisa` | Sobrepõe `observability.service_name` |
| `OTEL_LOG_LEVEL` | `WARNING` | Sobrepõe `observability.log_level` |
| `OTEL_COMPACT_BATCH_SIZE` | `10` | Sobrepõe `observability.compact_batch_size` |
| `OTEL_SPAN_EXPORT_DELAY_MILLIS` | `1000` | Atraso de flush do processador de spans em lote |
| `PROVISA_SUPPORT_OTLP_ENDPOINT` | — | Sobrepõe `observability.support_endpoint` |
